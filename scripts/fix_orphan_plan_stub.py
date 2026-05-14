"""
One-off fix for an orphan stub in plans.geojson: a feature created by
detect_new_plans.py that has only mavat_url+last_modified, no plan_name
or agam_id. The mavat_url's plan-id (1005512104) resolves to 101-1549260
in XPLAN, so we copy plan_name/agam_id/taba/plan_name_he/status_mavat
in from XPLAN. repair_missing_plan_geometry.py can then fill the geom.
"""
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import urllib3
urllib3.disable_warnings()
from detect_new_plans import _SESSION, XPLAN_URL  # noqa: E402

PLANS_PATH = Path(__file__).resolve().parent.parent / 'data' / 'plans.geojson'


def main():
    with PLANS_PATH.open('r', encoding='utf-8') as f:
        plans = json.load(f)

    patched = 0
    for feat in plans['features']:
        props = feat.get('properties') or {}
        if props.get('plan_name') or props.get('agam_id'):
            continue
        mavat_url = props.get('mavat_url') or ''
        m = re.search(r'/SV4/1/(\d+)/', mavat_url)
        if not m:
            continue
        mp_id = int(m.group(1))
        r = _SESSION.get(XPLAN_URL, params={
            'where': f'mp_id={mp_id}',
            'outFields': 'pl_number,mp_id,pl_name,station_desc',
            'returnGeometry': 'false',
            'f': 'geojson',
        }, timeout=60, verify=False)
        feats = r.json().get('features', [])
        if not feats:
            print(f'  skip mp_id={mp_id}: no XPLAN match')
            continue
        info = feats[0].get('properties') or {}
        pl_number = info.get('pl_number') or ''
        taba = pl_number.split('-', 1)[1] if '-' in pl_number else pl_number
        props['plan_name'] = pl_number
        props['agam_id'] = float(mp_id)
        props['taba'] = taba
        props['plan_name_he'] = info.get('pl_name') or ''
        props['status_mavat'] = info.get('station_desc') or ''
        patched += 1
        print(f'  + patched stub -> plan_name={pl_number}, agam_id={mp_id}')

    if patched:
        with PLANS_PATH.open('w', encoding='utf-8') as f:
            json.dump(plans, f, ensure_ascii=False)
        print(f'wrote {PLANS_PATH} (patched {patched} orphan stubs)')
    else:
        print('no orphan stubs to patch')


if __name__ == '__main__':
    main()
