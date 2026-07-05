"""
fetch_objection_dates.py
Fills objection_end_date ("תום תקופת הפקדה") for all plans in הפקדה status
that are missing it, via the YK process JSON API (no browser / WAF issues).

Rewritten 2026-07-05 (old Playwright version: _archive/fetch_objection_dates_playwright_20260705.py):
  - Uses update_mavat_ui._yk_compute_objection_end with status_date, which
    ignores publication dates from PREVIOUS deposit cycles (the old version
    computed 28/07/2024 for plan 101-1252998 deposited 02/07/2026).
  - GeoJSON update goes through update_geojson_objections (str(taba) keying —
    the old inline update compared int taba to str keys and always wrote 0
    features — and git_sync's concurrency-safe push).

Usage:
    python fetch_objection_dates.py            # write GS + geojson
    python fetch_objection_dates.py --dry-run  # report only
"""
import sys

sys.path.insert(0, r"C:\ORANIM")
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

import gspread
from google.oauth2.service_account import Credentials

from update_mavat_ui import (_yk_compute_objection_end, _parse_dmy,
                             update_geojson_objections)

CREDS_FILE = r"C:\ORANIM\oranim-490018-ceaf784afe61.json"
SHEET_ID = "1_AcuuA1CNPh6jXc_lZKNghfpEF1aDPV8Zci8QPz2WVE"

DRY_RUN = '--dry-run' in sys.argv


def main():
    creds = Credentials.from_service_account_file(CREDS_FILE,
        scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
    sheet = gspread.authorize(creds).open_by_key(SHEET_ID).sheet1
    all_data = sheet.get_all_values()
    headers = all_data[0]
    h = {hdr.strip().lower(): i for i, hdr in enumerate(headers)}

    plans = []
    seen = set()
    for row_num, row in enumerate(all_data[1:], start=2):
        status = row[h['status_mavat']].strip()
        taba = row[h['taba']].strip()
        obj_date = row[h['objection_end_date']].strip() if len(row) > h['objection_end_date'] else ''
        if 'הפקדה להתנגדויות' in status and taba and not obj_date and taba not in seen:
            seen.add(taba)
            plans.append({
                'row': row_num, 'taba': taba,
                'status_date': _parse_dmy(row[h['mavat_date']].strip() if len(row) > h['mavat_date'] else ''),
            })

    print(f"Plans needing objection end date: {len(plans)}")
    if not plans:
        return

    results = {}
    for i, plan in enumerate(plans):
        taba = plan['taba']
        print(f"[{i+1}/{len(plans)}] TABA {taba}...", end=" ", flush=True)
        end_date, source = _yk_compute_objection_end(taba, status_date=plan['status_date'])
        if end_date:
            val = end_date if source == 'explicit' else f"{end_date} (מחושב)"
            results[taba] = {'row': plan['row'], 'objection_end': val}
            print(f"end={val} [{source}]")
        else:
            print(f"no date [{source}]")

    found = sum(1 for r in results.values() if r.get('objection_end'))
    print(f"\nFound dates: {found}/{len(plans)}")
    if not results:
        return
    if DRY_RUN:
        print("--- DRY-RUN: nothing written. ---")
        return

    # Update Sheets — the plan's own row + any duplicate rows with the same taba
    print("\nUpdating Google Sheets...")
    batch = []
    seen_ranges = set()
    for row_num, row in enumerate(all_data[1:], start=2):
        taba = row[h['taba']].strip()
        if taba in results:
            obj_existing = row[h['objection_end_date']].strip() if len(row) > h['objection_end_date'] else ''
            if not obj_existing:
                a1 = gspread.utils.rowcol_to_a1(row_num, h['objection_end_date'] + 1)
                if a1 not in seen_ranges:
                    seen_ranges.add(a1)
                    batch.append({'range': a1, 'values': [[results[taba]['objection_end']]]})
    if batch:
        sheet.spreadsheet.values_batch_update({'valueInputOption': 'RAW', 'data': batch})
        print(f"  Updated {len(batch)} cells")

    # Update plans.geojson (concurrency-safe commit+push via git_sync)
    print("Updating GeoJSON...")
    update_geojson_objections(results)


if __name__ == "__main__":
    main()
