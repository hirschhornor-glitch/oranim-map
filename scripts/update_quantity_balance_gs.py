"""Fill-only write of the quantity-balance scrape (existing/נכנס state) to GS.

Reads quantity_balance_results.json (from scrape_quantity_balance.py) and writes,
ONLY into empty cells (header-name lookup):
    units_in   <- units_in    (existing residential יח"ד, מצב מאושר)
    units_add  <- units_add   (תוספת = proposed - existing)
    Machpil    <- multiplier  (מכפיל = proposed / existing)
    commerce_in<- commerce_in (existing מסחר מ"ר)

Usage:
    python update_quantity_balance_gs.py --dry-run
    python update_quantity_balance_gs.py
"""
import json
import sys
from datetime import datetime
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

if sys.platform.startswith("win"):
    sys.stdout.reconfigure(encoding="utf-8")

CREDS_FILE = r"C:\ORANIM\oranim-490018-ceaf784afe61.json"
SHEET_ID = "1_AcuuA1CNPh6jXc_lZKNghfpEF1aDPV8Zci8QPz2WVE"
RESULTS_FILE = Path(r"C:\ORANIM\quantity_balance_results.json")

# result key -> GS header. Only write residential figures when the plan actually
# has residential units (units_total>0); commerce_in only when >0.
def build_updates(dry_run):
    res = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))["results"]
    byplan = {r["plan_number"]: r for r in res
              if r.get("status") == "success" and r.get("plan_number")}
    print(f"Loaded {len(byplan)} successful balance results")

    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    sh = gspread.authorize(creds).open_by_key(SHEET_ID).sheet1
    vals = sh.get_all_values()
    hdr = vals[0]
    h = {n: i for i, n in enumerate(hdr)}
    for req in ("plan_name", "units_total", "units_in", "units_add", "Machpil", "commerce_in", "last_modified"):
        if req not in h:
            print(f"ERROR: missing GS header {req!r}")
            return
    PN, LM = h["plan_name"], h["last_modified"]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _num(s):
        s = str(s).replace(",", "").strip()
        try:
            return float(s)
        except ValueError:
            return None

    per = {"units_in": 0, "units_add": 0, "Machpil": 0, "commerce_in": 0}
    updates, touched = [], set()
    for ridx, r in enumerate(vals[1:], start=2):
        pn = r[PN].strip() if len(r) > PN else ""
        if pn not in byplan:
            continue
        d = byplan[pn]
        has_resid = (d.get("units_total") or 0) > 0 or (d.get("units_in") or 0) > 0
        plan_fields = []
        if has_resid:
            ui = d.get("units_in", 0)
            plan_fields.append(("units_in", ui))
            # תוספת/מכפיל derive from the AUTHORITATIVE "out" = Table 5 (GS units_total),
            # falling back to the accordion's proposed only if GS has no value. Skip
            # when out < in (sources disagree) rather than write a negative/nonsensical add.
            gs_total = _num(r[h["units_total"]]) if h["units_total"] < len(r) and r[h["units_total"]].strip() else None
            out_total = gs_total if gs_total is not None else d.get("units_total")
            if out_total is not None and out_total >= ui:
                plan_fields.append(("units_add", int(round(out_total - ui))))
                if ui:
                    plan_fields.append(("Machpil", round(out_total / ui, 2)))
        if (d.get("commerce_in") or 0) > 0:
            plan_fields.append(("commerce_in", d["commerce_in"]))
        for field, val in plan_fields:
            ci = h[field]
            cur = r[ci].strip() if ci < len(r) else ""
            if cur:
                continue  # fill-only
            sval = str(int(val)) if isinstance(val, (int, float)) and float(val) == int(val) else str(val)
            updates.append({"range": gspread.utils.rowcol_to_a1(ridx, ci + 1), "values": [[sval]]})
            per[field] += 1
            touched.add(ridx)
    for ridx in touched:
        updates.append({"range": gspread.utils.rowcol_to_a1(ridx, LM + 1), "values": [[now]]})

    print(f"Plans matched & filling: {len(touched)} | cells (excl last_modified): {sum(per.values())}")
    for f, c in per.items():
        print(f"  {f:14s} {c}")
    if dry_run:
        print("DRY-RUN: no write.")
        return
    if updates:
        sh.spreadsheet.values_batch_update({"valueInputOption": "RAW", "data": updates})
        print(f"WROTE {len(updates)} cells (incl {len(touched)} last_modified).")


if __name__ == "__main__":
    build_updates("--dry-run" in sys.argv)
