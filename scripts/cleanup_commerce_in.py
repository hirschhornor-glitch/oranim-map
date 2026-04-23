"""
One-time cleanup: remove wrongly-written commerce_in values.

Context:
    update_table5_gs.py previously wrote commerce_requested_sqm to COL_COMMERCE_IN (V)
    instead of COL_COMMERCE_OUT (W). This inflated commerce_in for plans where the
    Mavat DOM had no "מצב מאושר" value (it should be 0 for those plans).

What this script does:
    For every plan in all_table5_v2_results.json with commerce_requested_sqm > 0:
      - Read current commerce_in (V) from sheet
      - If it matches the wrongly-written Table-5 value (or is within 1 unit), CLEAR it.
      - Otherwise leave it alone (probably a legitimate value from enrich_mavat.py).

After running this, users should:
    1. Run `update_table5_gs.py` — writes the correct proposed value to commerce_out (W)
    2. Re-run `enrich_mavat.py` at a later time to populate commerce_in from Mavat DOM
       (this now sets 0 when "מצב מאושר" is empty — see fix in enrich_mavat.py)

Usage:
    python cleanup_commerce_in.py --dry-run   # preview
    python cleanup_commerce_in.py             # actually clear
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

COL_PLAN_NAME = 6         # F
COL_COMMERCE_IN = 22      # V
COL_COMMERCE_OUT = 23     # W
COL_LAST_MOD = 40         # AN


def main() -> None:
    if sys.platform.startswith("win"):
        sys.stdout.reconfigure(encoding="utf-8")
    dry_run = "--dry-run" in sys.argv

    print(f"Loading {RESULTS_FILE}...", flush=True)
    with open(RESULTS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Map plan_number -> commerce_requested_sqm
    t5_commerce = {}
    for r in data.get("results", []):
        if r.get("status") != "success":
            continue
        v = (r.get("totals", {}) or {}).get("commerce_requested_sqm", 0) or 0
        if v > 0:
            t5_commerce[r["plan_number"]] = float(v)
    print(f"{len(t5_commerce)} plans with Table-5 commerce > 0", flush=True)

    print("Connecting to Google Sheets...", flush=True)
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID).sheet1

    all_data = sheet.get_all_values()
    print(f"Sheet has {len(all_data) - 1} data rows", flush=True)

    batch = []
    changes = []

    for row_idx, row in enumerate(all_data[1:], start=2):
        if len(row) < COL_PLAN_NAME:
            continue
        plan_name = row[COL_PLAN_NAME - 1].strip()
        if plan_name not in t5_commerce:
            continue

        t5_val = t5_commerce[plan_name]
        # Current commerce_in cell
        if COL_COMMERCE_IN - 1 >= len(row):
            continue
        ci_raw = row[COL_COMMERCE_IN - 1].strip()
        if not ci_raw:
            continue
        try:
            ci_val = float(ci_raw.replace(",", ""))
        except ValueError:
            continue

        # Match: legacy buggy write would have placed exactly t5_val here.
        # Allow tiny drift (float rounding / rounding at write).
        if abs(ci_val - t5_val) < 1.0:
            changes.append({
                "row": row_idx,
                "plan": plan_name,
                "old_ci": ci_raw,
                "t5": t5_val,
            })
            batch.append({
                "range": gspread.utils.rowcol_to_a1(row_idx, COL_COMMERCE_IN),
                "values": [[""]],
            })

    print(f"\n{len(changes)} plans will have commerce_in CLEARED (old val matches Table-5 requested):")
    for c in changes[:40]:
        print(f"  row {c['row']:4d} | {c['plan']:20} | old_ci={c['old_ci']:>10} | t5={c['t5']:>10.1f}")
    if len(changes) > 40:
        print(f"  ... and {len(changes) - 40} more")

    if not changes:
        print("Nothing to clear.")
        return

    if dry_run:
        print("\n[DRY RUN] No writes performed.")
        return

    # Touch last_modified for each row cleared
    now_iso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for c in changes:
        batch.append({
            "range": gspread.utils.rowcol_to_a1(c["row"], COL_LAST_MOD),
            "values": [[now_iso]],
        })

    print(f"\nWriting {len(batch)} cells...", flush=True)
    sheet.spreadsheet.values_batch_update(
        {"valueInputOption": "RAW", "data": batch}
    )
    print(f"Done at {now_iso}.")


if __name__ == "__main__":
    main()
