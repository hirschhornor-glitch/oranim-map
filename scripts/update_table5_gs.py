"""
Update Google Sheets with Table 5 data from all_table5_v2_results.json.

Columns updated (resolved at runtime by header name — NOT hardcoded index;
hardcoding caused floors_max to overwrite the `authority` column when new
columns were inserted, see _archive notes / project_authority_col_fix memory):
    commerce_out      ← totals.commerce_requested_sqm
    employment        ← totals.employment_requested_sqm
    shavatz_out_sqm   ← totals.public_requested_sqm_standalone
    shavatz_out_prog  ← public uses description (standalone)
    hafrash_sqm       ← totals.public_requested_sqm_hafrash
    hafrash_prg       ← public uses description (hafrash)
    level_num         ← totals.max_floors
    High              ← totals.max_height_m
    last_modified     ← ISO timestamp (touched on any change)

Usage:
    python update_table5_gs.py            # actually write
    python update_table5_gs.py --dry-run  # preview only
"""
from __future__ import annotations

import json
import sys
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials


CREDS_FILE = r"C:\ORANIM\oranim-490018-ceaf784afe61.json"
SHEET_ID = "1_AcuuA1CNPh6jXc_lZKNghfpEF1aDPV8Zci8QPz2WVE"
RESULTS_FILE = r"C:\ORANIM\all_table5_v2_results.json"

# field_key (used in build_plan_updates) → GS header name
FIELD_TO_HEADER = {
    "plan_name":    "plan_name",
    "commerce":     "commerce_out",
    "employment":   "employment",
    "shavatz_out":  "shavatz_out_sqm",
    "shavatz_prog": "shavatz_out_prog",
    "hafrash":      "hafrash_sqm",
    "hafrash_prg":  "hafrash_prg",
    "floors_max":   "level_num",
    "height_max":   "High",
    "last_mod":     "last_modified",
}


def fmt_num(val: float) -> str:
    """Format a number for a cell. Returns empty string if value is 0 or missing."""
    if not val or val <= 0:
        return ""
    return str(int(val)) if float(val).is_integer() else f"{val:.1f}"


def build_plan_updates(results: list) -> dict:
    """Aggregate per-plan fields from v2 results for GS update.

    Returns: {plan_number: {commerce, employment, shavatz_out, shavatz_prog,
                            hafrash, hafrash_prg, floors_max, height_max}}
    """
    plan_updates: dict = {}
    for r in results:
        if r.get("status") != "success":
            continue
        totals = r.get("totals", {}) or {}
        rows = r.get("rows", []) or []

        # Descriptions: collect per-row "use — sqm" strings for standalone vs hafrash
        standalone_uses: list = []
        hafrash_uses: list = []
        for row in rows:
            if row.get("category") != "public":
                continue
            use_name = (row.get("use") or row.get("designation") or "").strip()
            if not use_name or len(use_name) < 2:
                continue
            req = row.get("requested_sqm", 0)
            try:
                req_int = int(round(float(req)))
            except (TypeError, ValueError):
                req_int = 0
            desc = f"{use_name} ({req_int})" if req_int > 0 else use_name
            subtype = row.get("public_subtype", "")
            if subtype == "standalone":
                standalone_uses.append(desc)
            elif subtype == "hafrash":
                hafrash_uses.append(desc)

        plan_updates[r["plan_number"]] = {
            "commerce":       totals.get("commerce_requested_sqm", 0),
            "employment":     totals.get("employment_requested_sqm", 0),
            "shavatz_out":    totals.get("public_requested_sqm_standalone", 0),
            "shavatz_prog":   "; ".join(standalone_uses),
            "hafrash":        totals.get("public_requested_sqm_hafrash", 0),
            "hafrash_prg":    "; ".join(hafrash_uses),
            "floors_max":     totals.get("max_floors", 0),
            "height_max":     totals.get("max_height_m", 0),
        }
    return plan_updates


def main() -> None:
    if sys.platform.startswith("win"):
        sys.stdout.reconfigure(encoding="utf-8")
    dry_run = "--dry-run" in sys.argv

    print(f"Loading results from {RESULTS_FILE}...", flush=True)
    with open(RESULTS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    results = data.get("results", [])
    print(f"Results: {data.get('total_processed', len(results))} processed, "
          f"{data.get('success', 0)} success", flush=True)

    plan_updates = build_plan_updates(results)
    print(f"{len(plan_updates)} plans with Table 5 data ready to write", flush=True)
    if not plan_updates:
        print("Nothing to update.")
        return

    print("\nConnecting to Google Sheets...", flush=True)
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID).sheet1

    all_data = sheet.get_all_values()
    print(f"Sheet has {len(all_data) - 1} data rows\n", flush=True)

    # Resolve all column positions by header name (1-indexed for rowcol_to_a1).
    headers = all_data[0]
    header_idx = {h.strip(): i for i, h in enumerate(headers)}
    missing = [hdr for hdr in FIELD_TO_HEADER.values() if hdr not in header_idx]
    if missing:
        print(f"ERROR: missing headers in sheet: {missing}", file=sys.stderr)
        sys.exit(1)
    col = {fk: header_idx[hdr] + 1 for fk, hdr in FIELD_TO_HEADER.items()}

    now_iso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    batch: list = []
    changes: list = []

    for row_idx, row in enumerate(all_data[1:], start=2):
        if len(row) < col["plan_name"]:
            continue
        plan_name = row[col["plan_name"] - 1].strip()
        if plan_name not in plan_updates:
            continue
        pu = plan_updates[plan_name]

        cell_changes: dict = {}

        def maybe_set(c: int, new_val: str) -> None:
            """Stage a cell update only if the value is non-empty."""
            if new_val == "":
                return
            existing = row[c - 1].strip() if c - 1 < len(row) else ""
            if existing == new_val:
                return
            cell_changes[c] = new_val

        maybe_set(col["commerce"], fmt_num(pu["commerce"]))
        maybe_set(col["employment"], fmt_num(pu["employment"]))
        maybe_set(col["shavatz_out"], fmt_num(pu["shavatz_out"]))
        maybe_set(col["shavatz_prog"], pu["shavatz_prog"])
        maybe_set(col["hafrash"], fmt_num(pu["hafrash"]))
        maybe_set(col["hafrash_prg"], pu["hafrash_prg"])
        maybe_set(col["floors_max"], fmt_num(pu["floors_max"]))
        maybe_set(col["height_max"], fmt_num(pu["height_max"]))

        if not cell_changes:
            continue

        for c, val in cell_changes.items():
            batch.append({
                "range": gspread.utils.rowcol_to_a1(row_idx, c),
                "values": [[val]],
            })
        # Touch last_modified whenever anything else changes
        batch.append({
            "range": gspread.utils.rowcol_to_a1(row_idx, col["last_mod"]),
            "values": [[now_iso]],
        })
        changes.append({
            "row": row_idx,
            "plan": plan_name,
            "fields": cell_changes,
        })

    if not changes:
        print("All target cells already match computed values — nothing to write.")
        return

    print(f"{'Plan':20} | {'Cout':>6} | {'Emp':>6} | {'SOut':>6} | {'Hafr':>6} | {'Lvl':>6} | {'High':>6}")
    print("-" * 78)
    for c in changes[:30]:
        def g(field_key: str) -> str:
            return c["fields"].get(col[field_key], "")
        print(
            f"{c['plan']:20} | "
            f"{g('commerce'):>6} | {g('employment'):>6} | "
            f"{g('shavatz_out'):>6} | {g('hafrash'):>6} | "
            f"{g('floors_max'):>6} | {g('height_max'):>6}"
        )
    if len(changes) > 30:
        print(f"... and {len(changes) - 30} more")

    print(f"\n{len(changes)} plans to update, {len(batch)} cells total.")

    if dry_run:
        print("\n[DRY RUN] No changes written.")
        return

    print(f"Writing {len(batch)} cells to Google Sheets...", flush=True)
    sheet.spreadsheet.values_batch_update(
        {"valueInputOption": "RAW", "data": batch}
    )
    print(f"Done at {now_iso}.")


if __name__ == "__main__":
    main()
