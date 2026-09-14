"""
fix_level_num_column.py
-----------------------
Clean up the `level_num` column (GS col 48 / geojson `level_num`) in
Oranim_Taba where it holds an *authority* string ("ועדה מחוזית" / "ועדה מקומית")
instead of a floor count.

This is the mirror image of fix_authority_column.py: that one cleaned floors out
of `authority` (root cause: update_table5_gs.py's COL_FLOORS_MAX = 46, which is
the authority column). The same off-by-column write also left authority text in
`level_num` on 42 rows, and it survived because update_table5_xlsx_gs.py only
salvages such a cell when a fresh Table 5 value arrives for that plan — these 42
never got one.

For each contaminated row:
  1. If `authority` is EMPTY, salvage the value into it — XPLAN's
     `pl_by_auth_of` wins when it has the plan (validated 571/571 against the
     curated column, see backfill_authority.py --compare-xplan), otherwise the
     residue text itself, normalized ("וועדה" / doubled words).
  2. Clear `level_num`. The true floor count is NOT recoverable here — these
     rows have no Table 5 parse — so the column is left blank rather than
     guessed. A Table 5 pass can fill it later.
Rows whose `authority` is already set keep it: that column has been curated
(backfill_authority.py, 2026-08-30) while `level_num` is only write residue.

Usage:
    python fix_level_num_column.py            # dry run
    python fix_level_num_column.py --apply    # write GS + geojson + push
"""
import json
import re
import sys

import gspread
from google.oauth2.service_account import Credentials

sys.path.insert(0, r"C:\ORANIM")
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from git_sync import commit_and_push_after_write
import detect_new_plans as d

CREDS_FILE = r"C:\ORANIM\oranim-490018-ceaf784afe61.json"
SHEET_ID = "1_AcuuA1CNPh6jXc_lZKNghfpEF1aDPV8Zci8QPz2WVE"
GEOJSON = r"C:\ORANIM\oranim-app\data\plans.geojson"
XPLAN_URL = ("https://ags.iplan.gov.il/arcgisiplan/rest/services/"
             "PlanningPublic/Xplan/MapServer/1/query")
# XPLAN pl_by_auth_of codes (validated in backfill_authority.py)
AUTH_OF = {1: 'ארצית', 2: 'ועדה מחוזית', 3: 'ועדה מקומית'}

APPLY = "--apply" in sys.argv


def is_numeric(v):
    try:
        float(str(v).strip())
        return True
    except (TypeError, ValueError):
        return False


def normalize_residue(txt):
    """'וועדה מחוזית' -> 'ועדה מחוזית'; 'ועדה מקומית מקומית' -> 'ועדה מקומית'."""
    t = re.sub(r"\s+", " ", str(txt or "").strip())
    t = t.replace("וועדה", "ועדה")
    words, seen = [], None
    for w in t.split(" "):
        if w != seen:
            words.append(w)
        seen = w
    return d.normalize_authority(" ".join(words))


def xplan_authority(plan_names):
    """pl_by_auth_of for as many of the plans as XPLAN still carries."""
    out = {}
    for i in range(0, len(plan_names), 40):
        chunk = plan_names[i:i + 40]
        where = " OR ".join("pl_number='%s'" % p for p in chunk)
        try:
            r = d._SESSION.get(XPLAN_URL, params={
                'where': where, 'outFields': 'pl_number,pl_by_auth_of',
                'returnGeometry': 'false', 'f': 'json'}, timeout=90, verify=False)
            for f in r.json().get('features', []):
                a = f['attributes']
                lbl = AUTH_OF.get(a.get('pl_by_auth_of'))
                if lbl:
                    out[a['pl_number']] = lbl
        except Exception as e:
            print(f"  XPLAN lookup failed for chunk {i // 40}: {str(e)[:80]}")
    return out


def main():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=[
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"])
    ws = gspread.authorize(creds).open_by_key(SHEET_ID).sheet1
    data = ws.get_all_values()
    hdr = [c.strip() for c in data[0]]
    if hdr[0] != 'agam_id':
        raise RuntimeError(f"row 1 is not the header (got {hdr[:4]})")
    i_plan = hdr.index('plan_name')
    i_lvl = hdr.index('level_num')
    i_auth = hdr.index('authority')
    i_mod = hdr.index('last_modified')

    contaminated = []
    for ri, row in enumerate(data[1:], start=2):
        lvl = row[i_lvl].strip() if len(row) > i_lvl else ''
        if not lvl or is_numeric(lvl):
            continue
        contaminated.append((ri, row[i_plan].strip(), lvl,
                             row[i_auth].strip() if len(row) > i_auth else ''))

    print(f"Rows with a non-numeric level_num: {len(contaminated)}")
    if not contaminated:
        return

    xa = xplan_authority([c[1] for c in contaminated])
    print(f"XPLAN resolved pl_by_auth_of for {len(xa)} of them\n")

    now = d.get_israel_time().strftime("%Y-%m-%d %H:%M:%S")
    batch, gj_changes, conflicts = [], {}, []
    for ri, plan, lvl, auth in contaminated:
        salvaged = ''
        if not auth:
            salvaged = xa.get(plan) or normalize_residue(lvl)
            batch.append({'range': gspread.utils.rowcol_to_a1(ri, i_auth + 1),
                          'values': [[salvaged]]})
        elif normalize_residue(lvl) != auth:
            conflicts.append((plan, auth, lvl))
        batch.append({'range': gspread.utils.rowcol_to_a1(ri, i_lvl + 1),
                      'values': [['']]})
        batch.append({'range': gspread.utils.rowcol_to_a1(ri, i_mod + 1),
                      'values': [[now]]})
        gj_changes[plan] = salvaged
        print(f"  row {ri:4} {plan:14} level_num={lvl!r} -> ''"
              + (f" | authority '' -> {salvaged}" +
                 (" (XPLAN)" if xa.get(plan) else " (residue)") if salvaged else ""))

    if conflicts:
        print(f"\n  {len(conflicts)} row(s) where the residue disagrees with the "
              f"curated authority — authority kept, residue discarded:")
        for plan, auth, lvl in conflicts:
            print(f"    {plan:14} authority={auth} | residue={lvl}")

    with open(GEOJSON, encoding='utf-8') as f:
        gj = json.load(f)
    gops = 0
    for feat in gj['features']:
        p = feat['properties']
        pn = p.get('plan_name')
        if pn not in gj_changes:
            continue
        if p.get('level_num') not in (None, ''):
            p['level_num'] = None
            gops += 1
        if gj_changes[pn] and not p.get('authority'):
            p['authority'] = gj_changes[pn]
            gops += 1
        p['last_modified'] = now

    print(f"\nGS cells: {len(batch)} | geojson fields: {gops}")
    if not APPLY:
        print("--- DRY RUN --- re-run with --apply")
        return

    bak = (r"C:\ORANIM\_gs_backup_level_num_"
           + d.get_israel_time().strftime('%Y%m%d_%H%M%S') + ".json")
    with open(bak, 'w', encoding='utf-8') as f:
        json.dump([{'row': ri, 'plan_name': plan, 'full_row': data[ri - 1]}
                   for ri, plan, _lvl, _auth in contaminated], f,
                  ensure_ascii=False, indent=1)
    print(f"Backup of the {len(contaminated)} rows: {bak}")
    ws.spreadsheet.values_batch_update(
        {'valueInputOption': 'USER_ENTERED', 'data': batch})
    print(f"Wrote {len(batch)} GS cells.")
    with open(GEOJSON, 'w', encoding='utf-8') as f:
        json.dump(gj, f, ensure_ascii=False)
    ok = commit_and_push_after_write(
        "data/plans.geojson",
        f"fix: clear authority text from level_num on {len(contaminated)} plans")
    print("pushed" if ok else "PUSH FAILED — change is in the working tree")


if __name__ == '__main__':
    main()
