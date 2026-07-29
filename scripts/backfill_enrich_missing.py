"""Backfill enrichment for plans missing units_in (and land-use שב"צ/שצ"פ).

For every plan whose GS row lacks units_in (and has an agam_id so Mavat can be
opened), pull:
  - units_in (יח"ד נכנס) from the Mavat "נתונים כמותיים" accordion — scrape_plan
  - Table 5 OUT fields (fill-only, only where the GS cell is currently blank)
  - שב"צ יוצא / שצ"פ יוצא from XPLAN land-use — fetch_landuse_shavaz_shatzap
    (authoritative: a positive value overrides; 0 only fills a blank)

Writes to GS + plans.geojson in flushes of FLUSH_EVERY plans so progress
survives interruption, and is RESUMABLE — a re-run only processes plans that
still lack units_in. The land-use POLYGONS themselves (for the ייעודי קרקע
layer) are synced separately by running update_mavat_ui.check_xplan_updates over
the same plan list; see backfill_landuse_layer() at the bottom.

Usage:
  python backfill_enrich_missing.py --limit 3      # test on first 3
  python backfill_enrich_missing.py                # full run
  python backfill_enrich_missing.py --landuse-layer  # phase 2: sync polygons
"""
import argparse
import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import gspread
from google.oauth2.service_account import Credentials
from playwright.async_api import async_playwright

import detect_new_plans as d
from table5_status_check import scrape_plan

FLUSH_EVERY = 8
THROTTLE_S = 1.5
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _get_sheet():
    gc = gspread.authorize(Credentials.from_service_account_file(d.CREDS_FILE, scopes=SCOPES))
    return gc.open_by_key(d.SHEET_ID).sheet1


def _col(hdr, name):
    return hdr.index(name) if name in hdr else -1


async def run(limit=None):
    ws = _get_sheet()
    data = ws.get_all_values()
    hdr = data[0]
    ci = {n: _col(hdr, n) for n in
          ('plan_name', 'taba', 'agam_id', 'units_in', 'units_add', 'units_total',
           'commerce_out', 'employment', 'shavatz_out_sqm', 'hafrash_sqm',
           'shatzap_out', 'High', 'level_num', 'rental', 'conditional_housing')}

    def cell(row, name):
        c = ci[name]
        return (row[c] if 0 <= c < len(row) else '').strip()

    # Build target list: rows lacking units_in that have an agam_id.
    targets = []
    for i, row in enumerate(data[1:], start=2):  # 1-indexed sheet rows
        plan = cell(row, 'plan_name')
        agam = cell(row, 'agam_id')
        if not plan or not agam:
            continue
        if cell(row, 'units_in'):
            continue
        targets.append({'sheet_row': i, 'plan': plan,
                        'taba': cell(row, 'taba') or plan, 'agam': agam})
    if limit:
        targets = targets[:limit]
    print(f"[backfill] {len(targets)} plans to enrich (missing units_in, have agam_id)")

    # Load geojson once.
    with open(d.PLANS_GEOJSON, encoding='utf-8') as f:
        gj = json.load(f)
    feat_by_plan = {ft['properties'].get('plan_name'): ft for ft in gj['features']}

    # t5 result key -> GS column (fill-only OUT fields).
    T5_FILL = list(d._T5_OUT_MAP)

    pending_gs = []   # {'range':A1,'values':[[v]]}
    geojson_dirty = False
    done = 0
    enriched = 0

    def flush():
        nonlocal pending_gs, geojson_dirty
        if pending_gs:
            ws.batch_update(pending_gs, value_input_option='RAW')
            pending_gs = []
        if geojson_dirty:
            with open(d.PLANS_GEOJSON, 'w', encoding='utf-8') as f:
                json.dump(gj, f, ensure_ascii=False)
            geojson_dirty = False

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page(accept_downloads=True)
        # re-read the row live per plan so resumed runs see prior flushes
        for t in targets:
            done += 1
            plan = t['plan']
            row = ws.row_values(t['sheet_row'])
            def cur(name):
                c = ci[name]
                return (row[c] if 0 <= c < len(row) else '').strip()
            try:
                res = await scrape_plan(page, {'agam_id': t['agam'], 'taba': t['taba'],
                                               'plan_number': plan})
            except Exception as e:
                print(f"  [{done}/{len(targets)}] {plan}: scrape error {str(e)[:60]}")
                await asyncio.sleep(THROTTLE_S)
                continue
            t5 = res.get('t5') or {}
            bal = res.get('bal') or {}
            lu_shz, lu_shp = d.fetch_landuse_shavaz_shatzap(plan)

            updates = {}  # gscol -> value
            # Table 5 OUT — fill only blanks.
            for k, gscol in T5_FILL:
                v = t5.get(k)
                if v and not cur(gscol):
                    updates[gscol] = v
            # units_in (accordion) + derived tosefet.
            uin = bal.get('units_in')
            if uin is not None:
                updates['units_in'] = uin
                out = t5.get('total_residential_units')
                if not out:
                    try: out = int(float(cur('units_total')))
                    except: out = None
                if out:
                    updates['units_add'] = int(out) - int(uin)
            # Land-use שב"צ/שצ"פ — positive value overrides; 0 only fills a blank.
            if lu_shz is not None:
                if lu_shz > 0:
                    updates['shavatz_out_sqm'] = lu_shz
                elif not cur('shavatz_out_sqm'):
                    updates['shavatz_out_sqm'] = 0
            if lu_shp is not None:
                if lu_shp > 0:
                    updates['shatzap_out'] = lu_shp
                elif not cur('shatzap_out'):
                    updates['shatzap_out'] = 0

            if not updates:
                print(f"  [{done}/{len(targets)}] {plan}: nothing to write (uin={uin}, lu={lu_shz})")
                await asyncio.sleep(THROTTLE_S)
                continue

            # Queue GS cell writes.
            for gscol, v in updates.items():
                a1 = gspread.utils.rowcol_to_a1(t['sheet_row'], ci[gscol] + 1)
                pending_gs.append({'range': a1, 'values': [[v]]})
            # Mirror to geojson.
            ft = feat_by_plan.get(plan)
            if ft:
                for gscol, v in updates.items():
                    ft['properties'][gscol] = v
                geojson_dirty = True

            enriched += 1
            print(f"  [{done}/{len(targets)}] {plan}: uin={updates.get('units_in','-')} "
                  f"uadd={updates.get('units_add','-')} shz={updates.get('shavatz_out_sqm','-')} "
                  f"shp={updates.get('shatzap_out','-')} (+{len(updates)} fields)")

            if enriched % FLUSH_EVERY == 0:
                flush()
                print(f"  ...flushed at {enriched} enriched")
            await asyncio.sleep(THROTTLE_S)

        await browser.close()
    flush()
    print(f"[backfill] done: {enriched}/{len(targets)} enriched")


def backfill_landuse_layer(limit=None):
    """Phase 2 — sync land-use POLYGONS (+ shavaz/easements/trees) into the map
    layers, via update_mavat_ui.check_xplan_updates (one call, writes + pushes the
    geojson layers). Scoped to plans MISSING from landuse_xplan.geojson so we don't
    needlessly re-query the ~785 plans already present."""
    import update_mavat_ui as u
    with open(u.LANDUSE_GEOJSON, encoding='utf-8') as f:
        lu = json.load(f)
    have = {str(ft['properties'].get('pl_number') or '').strip() for ft in lu['features']}
    ws = _get_sheet()
    data = ws.get_all_values()
    hdr = data[0]
    cp = _col(hdr, 'plan_name')
    cs = _col(hdr, 'status_mavat')
    plans = []
    for row in data[1:]:
        p = (row[cp] if cp < len(row) else '').strip()
        st = (row[cs] if 0 <= cs < len(row) else '').strip()
        if p and p.startswith('101-') and p not in have:
            plans.append({'plan_name': p, 'new_status': st})
    if limit:
        plans = plans[:limit]
    print(f"[landuse-layer] {len(plans)} plans missing from the landuse layer → check_xplan_updates")
    rep = u.check_xplan_updates(plans)
    for line in rep:
        print(line)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=None)
    ap.add_argument('--landuse-layer', action='store_true',
                    help='Phase 2: sync land-use polygons to the map layers')
    args = ap.parse_args()
    if args.landuse_layer:
        backfill_landuse_layer(limit=args.limit)
    else:
        asyncio.run(run(limit=args.limit))
