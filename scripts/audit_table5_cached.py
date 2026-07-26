"""Browser-free audit over cached Table 5 files (temp_xlsx/*.xlsx).

Re-parses every cached Table 5 with the current parser and:
  1. Reports plans whose declared "סך הכל" != summed detail rows (a component —
     rental or conditional — is listed as ADDITIVE).
  2. Fills rental_duration where GS is blank and the note gives one.
  3. Fixes units_total conditional double-counts (current == new_base + conditional).

Fill-only + confirmed-double-count-only, so curated values are never clobbered.
Writes GS + plans.geojson.
"""
import glob
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gspread
from google.oauth2.service_account import Credentials

import detect_new_plans as d
from parse_table5_xlsx import parse_table5_xlsx, result_to_dict

TEMP = r"C:\ORANIM\temp_xlsx"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _num(s):
    try:
        return float(str(s).replace(',', '').strip())
    except (ValueError, AttributeError):
        return None


def main():
    gc = gspread.authorize(Credentials.from_service_account_file(d.CREDS_FILE, scopes=SCOPES))
    ws = gc.open_by_key(d.SHEET_ID).sheet1
    data = ws.get_all_values()
    hdr = data[0]
    ci = {n: hdr.index(n) for n in ('plan_name', 'units_total', 'units_in', 'units_add',
                                    'conditional_housing', 'rental_duration')}
    row_of, val = {}, {}
    for i, r in enumerate(data[1:], start=2):
        p = (r[ci['plan_name']] if ci['plan_name'] < len(r) else '').strip()
        if not p:
            continue
        row_of[p] = i
        val[p] = {k: (r[ci[k]] if ci[k] < len(r) else '').strip() for k in ci}

    with open(d.PLANS_GEOJSON, encoding='utf-8') as f:
        gj = json.load(f)
    feat_of = {ft['properties'].get('plan_name'): ft for ft in gj['features']}

    files = sorted(glob.glob(os.path.join(TEMP, '*.xlsx')))
    print(f"[audit] {len(files)} cached Table 5 files")

    gs_updates = []
    inconsistent = []
    rd_filled = []
    ut_fixed = []
    parse_fail = 0

    for fp in files:
        plan = os.path.basename(fp)[:-5]
        if plan not in row_of:
            continue
        parsed = parse_table5_xlsx(fp)
        if not parsed or parsed.error:
            parse_fail += 1
            continue
        t5 = result_to_dict(parsed)
        row = row_of[plan]
        cur_ut = _num(val[plan]['units_total'])
        cur_rd = val[plan]['rental_duration'].strip()
        cur_cond = _num(val[plan]['conditional_housing']) or 0
        cur_uin = _num(val[plan]['units_in'])
        new_base = t5['total_residential_units']  # excl conditional

        # 1) consistency report
        if not t5['units_totals_consistent']:
            inconsistent.append({
                'plan': plan, 'detail_sum': t5['total_units'],
                'declared': t5['stated_total_units'], 'additive': t5['additive_units'],
                'rental': t5['rental_units'], 'conditional': t5['conditional_units'],
            })

        upd = {}
        # 2) rental_duration fill-only
        if t5.get('rental_duration_years') and not cur_rd:
            upd['rental_duration'] = t5['rental_duration_years']
            rd_filled.append((plan, t5['rental_duration_years']))
        # 3) units_total conditional double-count fix (confirmed only)
        if cur_ut is not None and new_base and cur_cond > 0 and int(cur_ut) == new_base + int(cur_cond):
            upd['units_total'] = new_base
            if cur_uin is not None:
                upd['units_add'] = new_base - int(cur_uin)
            ut_fixed.append((plan, int(cur_ut), new_base))

        if upd:
            for k, v in upd.items():
                gs_updates.append({'range': gspread.utils.rowcol_to_a1(row, ci[k] + 1),
                                   'values': [[v]]})
            ft = feat_of.get(plan)
            if ft:
                ft['properties'].update(upd)

    if gs_updates:
        ws.batch_update(gs_updates, value_input_option='RAW')
        with open(d.PLANS_GEOJSON, 'w', encoding='utf-8') as f:
            json.dump(gj, f, ensure_ascii=False)

    print(f"\n[audit] parse_fail={parse_fail} | rental_duration filled={len(rd_filled)} | "
          f"units_total fixed={len(ut_fixed)} | inconsistent={len(inconsistent)}")
    if ut_fixed:
        print("\nunits_total conditional double-counts fixed:")
        for p, old, new in ut_fixed:
            print(f"  {p}: {old} -> {new}")
    if inconsistent:
        print("\n⚠️  INCONSISTENT (declared total != detail sum — additive component):")
        for x in inconsistent:
            print(f"  {x['plan']}: detail_sum={x['detail_sum']} declared={x['declared']} "
                  f"additive={x['additive']} (rental={x['rental']} cond={x['conditional']})")


if __name__ == '__main__':
    main()
