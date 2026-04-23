"""
Update Google Sheets with Table 5 data from all_table5_v2_results.json.

Column mapping:
    W  (23)  commerce_out     ← totals.commerce_requested_sqm (main + service)
    AA (27)  employment       ← totals.employment_requested_sqm
    R  (18)  shavatz_out_sqm  ← totals.public_requested_sqm_standalone (ציבור ללא מגורים)
    S  (19)  shavatz_out_prog ← public uses description (standalone)
    AQ (43)  hafrash_sqm      ← totals.public_requested_sqm_hafrash (ציבור עם מגורים)
    AR (44)  hafrash_prg      ← public uses description (hafrash)
    AT (46)  floors_max       ← totals.max_floors                    [NEW]
    AU (47)  height_max       ← totals.max_height_m                  [NEW]
    AN (40)  last_modified    ← ISO timestamp

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

# 1-indexed column numbers
COL_PLAN_NAME = 6         # F
COL_SHAVATZ_OUT = 18      # R  - public_requested_sqm_standalone
COL_SHAVATZ_PROG = 19     # S  - public uses detail (standalone)
COL_COMMERCE_OUT = 23     # W  - commerce_requested_sqm (proposed total goes to "יוצא")
COL_COMMERCE_IN = 22      # V  - kept for cleanup of legacy wrong writes
COL_EMPLOYMENT = 27       # AA - employment_requested_sqm
COL_LAST_MOD = 40         # AN
COL_HAFRASH = 43          # AQ - public_requested_sqm_hafrash
COL_HAFRASH_PRG = 44      # AR - public uses detail (hafrash)
COL_FLOORS_MAX = 46       # AT - max_floors                 [NEW]
COL_HEIGHT_MAX = 47       # AU - max_height_m               [NEW]


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

    now_iso = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    batch: list = []
    changes: list = []

    for row_idx, row in enumerate(all_data[1:], start=2):
        if len(row) < COL_PLAN_NAME:
            continue
        plan_name = row[COL_PLAN_NAME - 1].strip()
        if plan_name not in plan_updates:
            continue
        pu = plan_updates[plan_name]

        cell_changes: dict = {}

        def maybe_set(col: int, new_val: str) -> None:
            """Stage a cell update only if the value is non-empty."""
            if new_val == "":
                return
            existing = row[col - 1].strip() if col - 1 < len(row) else ""
            if existing == new_val:
                return
            cell_changes[col] = new_val

        maybe_set(COL_COMMERCE_OUT, fmt_num(pu["commerce"]))
        maybe_set(COL_EMPLOYMENT, fmt_num(pu["employment"]))
        maybe_set(COL_SHAVATZ_OUT, fmt_num(pu["shavatz_out"]))
        maybe_set(COL_SHAVATZ_PROG, pu["shavatz_prog"])
        maybe_set(COL_HAFRASH, fmt_num(pu["hafrash"]))
        maybe_set(COL_HAFRASH_PRG, pu["hafrash_prg"])
        maybe_set(COL_FLOORS_MAX, fmt_num(pu["floors_max"]))
        maybe_set(COL_HEIGHT_MAX, fmt_num(pu["height_max"]))

        if not cell_changes:
            continue

        for col, val in cell_changes.items():
            batch.append({
                "range": gspread.utils.rowcol_to_a1(row_idx, col),
                "values": [[val]],
            })
        # Touch last_modified whenever anything else changes
        batch.append({
            "range": gspread.utils.rowcol_to_a1(row_idx, COL_LAST_MOD),
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

    print(f"{'Plan':20} | {'Cout':>6} | {'E':>6} | {'R':>6} | {'AQ':>6} | {'AT':>6} | {'AU':>6}")
    print("-" * 78)
    for c in changes[:30]:
        def g(col: int) -> str:
            return c["fields"].get(col, "")
        print(
            f"{c['plan']:20} | "
            f"{g(COL_COMMERCE_OUT):>6} | {g(COL_EMPLOYMENT):>6} | "
            f"{g(COL_SHAVATZ_OUT):>6} | {g(COL_HAFRASH):>6} | "
            f"{g(COL_FLOORS_MAX):>6} | {g(COL_HEIGHT_MAX):>6}"
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
