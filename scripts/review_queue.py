# -*- coding: utf-8 -*-
"""
review_queue.py — resolve the Stage-3 verification gate.

The GS->geojson mirror (update_plans.py) HOLDS any new built public allocation
(hafrash_prg / hafrash_sqm) instead of publishing it, and records it in
data/pending_review.json. This tool lets a human clear that queue after checking
the plan against Table 5 / הוראות (see feedback_hafrash_public_needs_table5_or_horaot).

Usage:
    python review_queue.py                     # list pending items
    python review_queue.py approve <taba> [field]   # publish held value(s) -> live geojson
    python review_queue.py reject  <taba> [field]   # clear value(s) in GS master (won't reappear)

approve/reject default to BOTH gated fields of the taba when <field> is omitted.
Dry-run safety: reject clears GS, so it prints what it will do and needs --apply.
"""
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8")

from git_sync import update_json_and_push  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
PENDING_LOCAL = os.path.join(REPO, "data", "pending_review.json")
PENDING_REL = "data/pending_review.json"
GEOJSON_REL = "data/plans.geojson"

# GS master (same coordinates as _clear_hafrash_1354356.py)
CREDS_FILE = r"C:\ORANIM\oranim-490018-ceaf784afe61.json"
SHEET_ID = "1_AcuuA1CNPh6jXc_lZKNghfpEF1aDPV8Zci8QPz2WVE"
COL_PLAN_NAME = 6
GS_COL = {"hafrash_sqm": 43, "hafrash_prg": 44}  # AQ / AR


def load_pending():
    if not os.path.exists(PENDING_LOCAL):
        return {}
    with open(PENDING_LOCAL, encoding="utf-8") as f:
        return json.load(f)


def cmd_list():
    pending = load_pending()
    if not pending:
        print("review queue is empty ✓")
        return
    print(f"{len(pending)} item(s) awaiting review:\n")
    for key, it in pending.items():
        print(f"  [{it.get('taba')}] {it.get('plan_name','')}  {it['field']}")
        print(f"      {it.get('old')!r}  ->  {it.get('held_value')!r}")
        print(f"      status={it.get('status','')}  first_seen={it.get('first_seen','')}\n")
    print("approve <taba> [field]  |  reject <taba> [field]")


def _matching_keys(pending, taba, field):
    taba = str(taba)
    return [k for k, it in pending.items()
            if str(it.get("taba")) == taba and (not field or it["field"] == field)]


def cmd_approve(taba, field):
    pending = load_pending()
    keys = _matching_keys(pending, taba, field)
    if not keys:
        print(f"no pending items for taba {taba}" + (f" field {field}" if field else ""))
        return
    vals = {pending[k]["field"]: pending[k]["held_value"] for k in keys}
    print(f"approve: publish {vals} to live geojson for taba {taba}")

    def _apply(data):
        changed = False
        for feat in data["features"]:
            p = feat.get("properties", {})
            if str(p.get("taba")) == str(taba):
                for fld, val in vals.items():
                    if p.get(fld) != val:
                        p[fld] = val
                        changed = True
        return changed

    ok = update_json_and_push(
        GEOJSON_REL, _apply,
        f"review: approve built allocation for {taba} ({', '.join(vals)})")
    if not ok:
        print("✗ geojson publish failed — queue left unchanged")
        return
    for k in keys:
        pending.pop(k, None)
    _save_pending_push(pending, f"review: dequeue {taba} (approved)")
    print("✓ approved & published")


def cmd_reject(taba, field, apply):
    pending = load_pending()
    keys = _matching_keys(pending, taba, field)
    if not keys:
        print(f"no pending items for taba {taba}" + (f" field {field}" if field else ""))
        return
    plan_name = pending[keys[0]].get("plan_name", "")
    fields = [pending[k]["field"] for k in keys]
    print(f"reject: clear {fields} in GS master for {plan_name} (taba {taba})")
    if not apply:
        print("\n(dry-run — re-run with --apply to clear GS)")
        return

    import gspread
    from google.oauth2.service_account import Credentials
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    sheet = gspread.authorize(creds).open_by_key(SHEET_ID).sheet1
    col_f = sheet.col_values(COL_PLAN_NAME)
    row = next((i for i, v in enumerate(col_f, 1) if (v or "").strip() == plan_name), None)
    if not row:
        print(f"✗ plan {plan_name} not found in GS col F — aborting")
        return
    for fld in fields:
        sheet.update_cell(row, GS_COL[fld], "")
        print(f"  cleared GS {fld} (row {row})")
    for k in keys:
        pending.pop(k, None)
    _save_pending_push(pending, f"review: dequeue {taba} (rejected, GS cleared)")
    print("✓ rejected & cleared in GS")


def _save_pending_push(pending, msg):
    def _apply(data):
        data.clear()
        data.update(pending)
        return True
    update_json_and_push(PENDING_REL, _apply, msg)


def main():
    a = sys.argv[1:]
    if not a or a[0] == "list":
        cmd_list()
    elif a[0] == "approve" and len(a) >= 2:
        cmd_approve(a[1], a[2] if len(a) > 2 and not a[2].startswith("--") else None)
    elif a[0] == "reject" and len(a) >= 2:
        field = a[2] if len(a) > 2 and not a[2].startswith("--") else None
        cmd_reject(a[1], field, "--apply" in a)
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
