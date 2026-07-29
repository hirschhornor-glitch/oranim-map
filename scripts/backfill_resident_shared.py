"""Backfill resident_shared / resident_shared_prg from cached Table 5 XLSX.

Parses every "זכויות והוראות בניה XLS" (Table 5) file in a cache dir (default
C:\\ORANIM\\temp_xlsx), extracts the shared-tenant ("חדר דיירים") area via
parse_table5_xlsx._shared_tenant_spaces, matches each file to a plan in
plans.geojson by taba number, and writes the value to BOTH:

  - the Oranim_Taba Google Sheet (authoritative — so the live status-change
    pipeline in table5_status_check.py will not later blank it), columns
    resident_shared / resident_shared_prg
  - data/plans.geojson (so the map + the resident-shared report see it now)

Only plans with resident_shared_sqm > 0 are written. Idempotent — re-running
with the same cache produces no new writes.

Shared-tenant spaces are PRIVATE to a building's residents (shared leisure
floors, shared balconies, a tenants' club room) and are deliberately tracked
apart from public program (שב"צ/שצ"פ) — see the memory
reference_program_vs_shared_tenant_spaces.

Usage:
    python backfill_resident_shared.py               # write GS + geojson
    python backfill_resident_shared.py --dry-run      # preview only
    python backfill_resident_shared.py --no-gs        # geojson only
    python backfill_resident_shared.py --xlsx-dir DIR
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from parse_table5_xlsx import parse_table5_xlsx, result_to_dict

REPO_ROOT = Path(__file__).resolve().parent.parent
GEOJSON = REPO_ROOT / "data" / "plans.geojson"
DEFAULT_XLSX_DIR = Path(r"C:\ORANIM\temp_xlsx")

# GS (same sheet as update_table5_gs.py)
CREDS_FILE = r"C:\ORANIM\oranim-490018-ceaf784afe61.json"
SHEET_ID = "1_AcuuA1CNPh6jXc_lZKNghfpEF1aDPV8Zci8QPz2WVE"


def _digits(s) -> str:
    return re.sub(r"\D", "", str(s or ""))


def collect(xlsx_dir: Path) -> dict:
    """Parse every cached XLSX → {taba_digits: {sqm, notes}} for hits > 0."""
    out: dict = {}
    for p in sorted(xlsx_dir.glob("*.xlsx")):
        r = parse_table5_xlsx(p)
        if not r or r.error:
            continue
        d = result_to_dict(r)
        if d["resident_shared_sqm"] > 0:
            out[_digits(p.stem)] = {
                "sqm": d["resident_shared_sqm"],
                "notes": d["shared_tenant_notes"],
            }
    return out


def match_features(geo: dict, hits: dict) -> list:
    """Return [(feature, sqm, notes)] for geojson plans that match a hit by
    taba or plan_name digits."""
    matched = []
    for f in geo["features"]:
        p = f["properties"]
        for key in (_digits(p.get("taba")), _digits(p.get("plan_name"))):
            if key and key in hits:
                matched.append((f, hits[key]["sqm"], hits[key]["notes"]))
                break
    return matched


def main() -> None:
    if sys.platform.startswith("win"):
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx-dir", default=str(DEFAULT_XLSX_DIR))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-gs", action="store_true")
    args = ap.parse_args()

    hits = collect(Path(args.xlsx_dir))
    print(f"{len(hits)} cached plans with shared-tenant area > 0")

    geo = json.loads(GEOJSON.read_text(encoding="utf-8"))
    matched = match_features(geo, hits)
    print(f"{len(matched)} matched to plans.geojson")

    # want[plan_name] = (val, notes) for every matched plan (authoritative target).
    want = {}
    geo_changed = 0
    for f, sqm, notes in matched:
        p = f["properties"]
        val = int(sqm) if float(sqm).is_integer() else round(sqm, 1)
        want[str(p.get("plan_name", ""))] = (val, notes)
        print(f"  {str(p.get('plan_name','')):16} {val:>6}  units={p.get('units_total')}  | {notes}")
        if str(p.get("resident_shared", "")) != str(val) or p.get("resident_shared_prg", "") != notes:
            p["resident_shared"] = val
            p["resident_shared_prg"] = notes
            geo_changed += 1

    if args.dry_run:
        print(f"\n[DRY RUN] would update {geo_changed} geojson feature(s); no sheet write.")
        return

    if geo_changed:
        GEOJSON.write_text(json.dumps(geo, ensure_ascii=False), encoding="utf-8")
        print(f"✓ updated {geo_changed} plan(s) in {GEOJSON.relative_to(REPO_ROOT)}")
    else:
        print("geojson already current.")

    if args.no_gs or not want:
        return

    # Mirror the authoritative values into the sheet so the status-change
    # pipeline keeps them (compared against current cells, independent of geojson).
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    client = gspread.authorize(Credentials.from_service_account_file(CREDS_FILE, scopes=scopes))
    sheet = client.open_by_key(SHEET_ID).sheet1
    all_data = sheet.get_all_values()
    hdr = {h.strip(): i for i, h in enumerate(all_data[0])}
    if "resident_shared" not in hdr or "resident_shared_prg" not in hdr:
        print("ERROR: sheet is missing resident_shared / resident_shared_prg columns.",
              file=sys.stderr)
        sys.exit(1)
    c_sqm, c_prg = hdr["resident_shared"], hdr["resident_shared_prg"]
    name_col = hdr.get("plan_name", 5)
    batch = []
    for row_idx, row in enumerate(all_data[1:], start=2):
        pn = row[name_col].strip() if name_col < len(row) else ""
        if pn not in want:
            continue
        val, notes = want[pn]
        cur_sqm = row[c_sqm].strip() if c_sqm < len(row) else ""
        cur_prg = row[c_prg].strip() if c_prg < len(row) else ""
        if cur_sqm != str(val):
            batch.append({"range": gspread.utils.rowcol_to_a1(row_idx, c_sqm + 1),
                          "values": [[str(val)]]})
        if cur_prg != notes:
            batch.append({"range": gspread.utils.rowcol_to_a1(row_idx, c_prg + 1),
                          "values": [[notes]]})
    if batch:
        sheet.spreadsheet.values_batch_update({"valueInputOption": "RAW", "data": batch})
        print(f"✓ wrote {len(batch)} cell(s) to Google Sheets")
    else:
        print("Sheet already current — no cells written.")


if __name__ == "__main__":
    main()
