"""Reconcile units_total for plans whose conditional units were double-counted.

Convention (user-confirmed): דיור מותנה is ALWAYS tracked separately in the
conditional_housing column and is NOT part of units_total. An earlier version of
parse_table5_xlsx wrongly added conditional מגורים into total_residential_units,
so plans auto-enriched via that path have an inflated units_total.

For every plan with conditional_housing > 0, re-download + re-parse Table 5 with
the corrected parser and compare:
  - current == new_total + conditional  → confirmed double-count → fix to new_total
                                           (and units_add = new_total - units_in)
  - current == new_total                → already correct → skip
  - otherwise                           → FLAG for manual review (don't touch)

Only confirmed double-counts are written, so a manually-curated units_total is
never clobbered. Writes GS + plans.geojson.
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gspread
from google.oauth2.service_account import Credentials
from playwright.async_api import async_playwright

import detect_new_plans as d
from scrape_table5_xlsx import download_xlsx
from parse_table5_xlsx import parse_table5_xlsx, result_to_dict

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _num(s):
    try:
        return float(str(s).replace(',', '').strip())
    except (ValueError, AttributeError):
        return None


async def run():
    gc = gspread.authorize(Credentials.from_service_account_file(d.CREDS_FILE, scopes=SCOPES))
    ws = gc.open_by_key(d.SHEET_ID).sheet1
    data = ws.get_all_values()
    hdr = data[0]
    ci = {n: hdr.index(n) for n in ('plan_name', 'taba', 'agam_id', 'units_total',
                                    'units_add', 'units_in', 'conditional_housing')}

    def cell(row, name):
        c = ci[name]
        return (row[c] if 0 <= c < len(row) else '').strip()

    targets = []
    for i, row in enumerate(data[1:], start=2):
        plan = cell(row, 'plan_name')
        cond = _num(cell(row, 'conditional_housing'))
        agam = cell(row, 'agam_id')
        if plan and cond and cond > 0 and agam:
            targets.append({'row': i, 'plan': plan, 'taba': cell(row, 'taba') or plan,
                            'agam': agam, 'cond': cond,
                            'ut': _num(cell(row, 'units_total')),
                            'uin': _num(cell(row, 'units_in'))})
    print(f"[reconcile] {len(targets)} plans with conditional_housing>0")

    with open(d.PLANS_GEOJSON, encoding='utf-8') as f:
        gj = json.load(f)
    feat_by_plan = {ft['properties'].get('plan_name'): ft for ft in gj['features']}

    fixed, ok, flagged, errors = [], [], [], []
    gs_updates = []
    geojson_dirty = False

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page(accept_downloads=True)
        for t in targets:
            plan = t['plan']
            try:
                path = await download_xlsx(page, {'agam_id': t['agam'], 'taba': t['taba'],
                                                  'plan_number': plan})
                if not path:
                    errors.append((plan, 'no_xlsx'))
                    continue
                parsed = parse_table5_xlsx(path)
                if not parsed or parsed.error:
                    errors.append((plan, 'parse_fail'))
                    continue
                new_total = result_to_dict(parsed)['total_residential_units']
            except Exception as e:
                errors.append((plan, str(e)[:40]))
                continue

            cur = t['ut']
            cond = int(t['cond'])
            if cur is None:
                flagged.append((plan, 'no current units_total', new_total))
                continue
            if int(cur) == new_total:
                ok.append(plan)
                continue
            if int(cur) == new_total + cond:
                # confirmed double-count → fix
                ua = gspread.utils.rowcol_to_a1(t['row'], ci['units_total'] + 1)
                gs_updates.append({'range': ua, 'values': [[new_total]]})
                if t['uin'] is not None:
                    aa = gspread.utils.rowcol_to_a1(t['row'], ci['units_add'] + 1)
                    gs_updates.append({'range': aa, 'values': [[new_total - int(t['uin'])]]})
                ft = feat_by_plan.get(plan)
                if ft:
                    ft['properties']['units_total'] = new_total
                    if t['uin'] is not None:
                        ft['properties']['units_add'] = new_total - int(t['uin'])
                    geojson_dirty = True
                fixed.append((plan, int(cur), new_total, cond))
                print(f"  FIX {plan}: {int(cur)} -> {new_total} (was +{cond} conditional)")
            else:
                flagged.append((plan, f'current={int(cur)} new={new_total} cond={cond}', None))
                print(f"  FLAG {plan}: current={int(cur)} new_total={new_total} cond={cond} (unexpected)")
        await browser.close()

    if gs_updates:
        ws.batch_update(gs_updates, value_input_option='RAW')
    if geojson_dirty:
        with open(d.PLANS_GEOJSON, 'w', encoding='utf-8') as f:
            json.dump(gj, f, ensure_ascii=False)

    print(f"\n[reconcile] fixed={len(fixed)} already_ok={len(ok)} flagged={len(flagged)} errors={len(errors)}")
    if flagged:
        print("FLAGGED (manual review):")
        for p, why, _ in flagged:
            print(f"  {p}: {why}")
    if errors:
        print("ERRORS:", errors)


if __name__ == '__main__':
    asyncio.run(run())
