"""
build_shatzap_quality.py
------------------------
How much of the planned open space sits on top of a private basement.

Renewal plans increasingly register the underground below a שצ"פ, path or square
as a separate three-dimensional lot (תמ"ר) belonging to the residential building —
its parking and service basement. On the map it is open space; in practice it is
the roof of a garage: shallow soil, no large trees, maintenance tied to a private
building. The plan instructions say so outright ("בתת הקרקע ובתחום החלקות התלת
ממדיות תותר הקמת מרתפים לחניה").

Scope: open-space parcels whose interior point falls inside the rova boundary
(data/district_oranim.geojson, רובע 4). XPLAN is queried over the Jerusalem
envelope and clipped here.

Sources (iplan XPLAN, 101- plans):
    MapServer/4  open-space parcels — 670 שצ"פ, 650 שטחים פתוחים, 780 ככר עירונית,
                 1670 שטחים פתוחים ומבנים ומוסדות ציבור
    MapServer/3  3D lots that put a private/other use underneath —
                 50076 מגורים, 50075 תעסוקה, 50073 תחבורה, 50071 מבני ציבור (תלת מימד)
                 + 20318 קו בנין תחתי (an underground building envelope drawn into the
                 open space without a separate 3D lot — reported apart, weaker signal)

Both are matched within the same plan only: the 3D lot and the parcel it sits under
are drawn by one plan.

Output: data/shatzap_quality.json
Aborts without writing when XPLAN returns no open-space parcels.

Usage: py -X utf8 scripts/build_shatzap_quality.py
"""
import json
import ssl
import sys
import warnings
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from pyproj import Transformer
from shapely.geometry import shape
from shapely.ops import transform as shp_transform, unary_union

warnings.filterwarnings('ignore')

BASE = 'https://ags.iplan.gov.il/arcgisiplan/rest/services/PlanningPublic/Xplan/MapServer'
DATA = Path(__file__).resolve().parent.parent / 'data'
OUTPUT = DATA / 'shatzap_quality.json'
JLM_ENVELOPE_ITM = '208000,620000,228000,644000'
PAGE = 1000

OPEN_SPACE = {670: 'שצ"פ', 650: 'שטחים פתוחים', 780: 'ככר עירונית', 1670: 'שטחים פתוחים ומבני ציבור'}
BASEMENT_3D = {50076: 'מגורים', 50075: 'תעסוקה', 50073: 'תחבורה', 50071: 'מבני ציבור'}
UNDERGROUND_LINE = {20318: 'קו בנין תחתי'}
MIN_SQM = 5           # sliver overlaps from digitising are not a basement
REJECTED = {'נגנזה', 'נדחתה', 'נגנזה/נדחתה'}


class _LegacySSLAdapter(HTTPAdapter):
    """XPLAN needs a legacy handshake; a plain request dies with SSLV3_ALERT."""
    def init_poolmanager(self, *a, **k):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.set_ciphers('DEFAULT:@SECLEVEL=0')
        k['ssl_context'] = ctx
        return super().init_poolmanager(*a, **k)


S = requests.Session()
S.mount('https://ags.iplan.gov.il', _LegacySSLAdapter())
TO_ITM = Transformer.from_crs(4326, 2039, always_xy=True).transform


def query(layer, codes, fields):
    feats, offset = [], 0
    while True:
        params = {
            'where': f"pl_number LIKE '101-%' AND mavat_code IN ({','.join(map(str, codes))})",
            'geometry': JLM_ENVELOPE_ITM, 'geometryType': 'esriGeometryEnvelope',
            'inSR': 2039, 'spatialRel': 'esriSpatialRelIntersects',
            'outFields': fields, 'returnGeometry': 'true', 'outSR': 4326, 'f': 'geojson',
            'orderByFields': 'objectid', 'resultOffset': offset, 'resultRecordCount': PAGE,
        }
        r = S.get(f'{BASE}/{layer}/query', params=params, timeout=180, verify=False)
        r.raise_for_status()
        page = r.json().get('features', [])
        feats.extend(page)
        if len(page) < PAGE:
            print(f'  layer {layer}: {len(feats)} features')
            return feats
        offset += PAGE


def itm(f):
    g = shape(f['geometry'])
    g = shp_transform(TO_ITM, g)
    return g if g.is_valid else g.buffer(0)


def main():
    os_raw = query(4, OPEN_SPACE, 'objectid,mavat_code,mavat_name,num,legal_area,pl_number,pl_name,station_desc')
    if not os_raw:
        print('XPLAN returned 0 open-space parcels — keeping the existing file.')
        sys.exit(1)
    td_raw = query(3, list(BASEMENT_3D) + list(UNDERGROUND_LINE), 'objectid,mavat_code,pl_number')

    rova = unary_union([shp_transform(TO_ITM, shape(f['geometry'])) for f in
                        json.loads((DATA / 'district_oranim.geojson').read_text(encoding='utf-8'))['features']])

    plans = {}
    for f in json.loads((DATA / 'plans.geojson').read_text(encoding='utf-8'))['features']:
        p = f.get('properties') or {}
        if p.get('plan_name') and p['plan_name'] not in plans:
            plans[p['plan_name']] = p

    under = defaultdict(list)     # pl_number -> [(code, geom)]
    for f in td_raw:
        if f.get('geometry'):
            under[f['properties']['pl_number']].append((f['properties']['mavat_code'], itm(f)))

    by_plan = defaultdict(lambda: {'parcels': [], 'os_geoms': [], 'b_geoms': [], 'u_geoms': []})
    for f in os_raw:
        if not f.get('geometry'):
            continue
        p = f['properties']
        pl = p['pl_number']
        g = itm(f)
        area = g.area
        if area < 1 or not rova.contains(g.representative_point()):
            continue
        basement = [gg.intersection(g) for c, gg in under.get(pl, []) if c in BASEMENT_3D]
        kinds = sorted({BASEMENT_3D[c] for c, gg in under.get(pl, [])
                        if c in BASEMENT_3D and gg.intersection(g).area >= MIN_SQM})
        b_area = unary_union(basement).area if basement else 0.0
        line = [gg.intersection(g) for c, gg in under.get(pl, []) if c in UNDERGROUND_LINE]
        l_area = unary_union(line).area if line else 0.0
        rec = by_plan[pl]
        rec['os_geoms'].append(g)
        if b_area >= MIN_SQM:
            rec['b_geoms'].extend(basement)
        if l_area >= MIN_SQM:
            rec['u_geoms'].extend(line)
        rec.setdefault('xplan_name', p.get('pl_name') or '')
        rec.setdefault('xplan_status', p.get('station_desc') or '')
        rec['parcels'].append({
            'num': str(p.get('num') or ''),
            'use': OPEN_SPACE.get(p['mavat_code'], p.get('mavat_name') or ''),
            'area': round(area),
            'basement_sqm': round(b_area) if b_area >= MIN_SQM else 0,
            'basement_kinds': kinds,
            'ug_line_sqm': round(l_area) if l_area >= MIN_SQM else 0,
        })

    out_plans, geoms = [], {}
    for pl, rec in by_plan.items():
        pp = plans.get(pl) or {}
        status = (pp.get('status_mavat') or rec['xplan_status'] or '').strip()
        if status in REJECTED:
            continue
        ps = rec['parcels']
        os_area = sum(x['area'] for x in ps)
        b = sum(x['basement_sqm'] for x in ps)
        u = sum(x['ug_line_sqm'] for x in ps)
        geoms[pl] = (unary_union(rec['os_geoms']),
                     unary_union(rec['b_geoms']) if rec['b_geoms'] else None,
                     unary_union(rec['u_geoms']) if rec['u_geoms'] else None)
        out_plans.append({
            'plan': pl,
            'taba': pl.split('-', 1)[1].lstrip('0'),
            'name': pp.get('plan_name_he') or rec['xplan_name'],
            'status': status,
            'in_oranim': bool(pp),
            'minahak': pp.get('minahak') or '',
            'os_sqm': os_area,
            'os_parcels': len(ps),
            'basement_sqm': b,
            'ug_line_sqm': u,
            'basement_kinds': sorted({k for x in ps for k in x['basement_kinds']}),
            # parcels are kept only for affected plans — the file stays small
            'parcels': [x for x in ps if x['basement_sqm'] or x['ug_line_sqm']] if (b or u) else [],
        })
    out_plans.sort(key=lambda r: -r['basement_sqm'])

    # An amendment plan redraws its parent's parcels and 3D lots, so the same basement
    # appears under two plan numbers. Name the overlap per plan, and total by geometric
    # union rather than by sum.
    hit = [r for r in out_plans if geoms[r['plan']][1] is not None]
    for r in hit:
        b = geoms[r['plan']][1]
        r['overlaps'] = [o['plan'] for o in hit if o is not r
                         and b.intersection(geoms[o['plan']][1]).area > 0.5 * min(b.area, geoms[o['plan']][1].area)]

    def union_area(rows, i):
        gs = [geoms[r['plan']][i] for r in rows if geoms[r['plan']][i] is not None]
        return round(unary_union(gs).area) if gs else 0

    def totals(rows):
        return {
            'plans': len(rows),
            'os_sqm': union_area(rows, 0),
            'basement_sqm': union_area(rows, 1),
            'ug_line_sqm': union_area(rows, 2),
            'plans_with_basement': sum(1 for r in rows if r['basement_sqm']),
            'plans_with_ug_line': sum(1 for r in rows if r['ug_line_sqm'] and not r['basement_sqm']),
        }

    result = {
        'generated': datetime.now().strftime('%Y-%m-%d'),
        'source': 'XPLAN MapServer/4 (open space) × MapServer/3 (3D lots, underground building line)',
        'open_space_codes': OPEN_SPACE,
        'basement_codes': BASEMENT_3D,
        'scope': 'רובע 4 — אורנים (district_oranim.geojson)',
        'totals': {'rova': totals(out_plans)},
        'plans': out_plans,
    }
    OUTPUT.write_text(json.dumps(result, ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
    for k, t in result['totals'].items():
        share = t['basement_sqm'] / t['os_sqm'] * 100 if t['os_sqm'] else 0
        print(f"{k}: {t['plans']} plans, open space {t['os_sqm'] / 1000:,.1f} dunam, over basement "
              f"{t['basement_sqm'] / 1000:,.1f} dunam ({share:.1f}%) in {t['plans_with_basement']} plans; "
              f"underground line only: {t['ug_line_sqm'] / 1000:,.1f} dunam in {t['plans_with_ug_line']} plans")
    print(f'-> {OUTPUT}')


if __name__ == '__main__':
    main()
