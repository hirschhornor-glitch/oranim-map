"""
fetch_commercial_frontage.py
----------------------------
Commercial frontage lines (חזית מסחרית) for every Jerusalem plan, from the iplan
XPLAN REST API (PlanningPublic/Xplan/MapServer/2 — linear entities).

Three mavat codes are kept:
    21100  חזית מסחרית
    21110  חזית עם הוראות מיוחדות
    56054  חזית פעילה  (the newer mavat symbol; ~25 lines in Jerusalem)

Scope: the Jerusalem ITM envelope used by the citywide commerce mapping, plans
prefixed 101- (the local committee) only.

Each line is enriched with:
    length_m      geodesic length
    street        nearest street from roads.geojson (Oranim coverage only)
    mp_match      the master plan whose boundary holds the line's midpoint

The collection also carries mp_axes: the commercial axes each master plan names
in its text (main_commercial / commercial_axes on its boundary feature).
    status        plans.geojson status_mavat when we track the plan, else XPLAN's

Output: data/commercial_frontage.geojson

Aborts without writing when XPLAN returns nothing — the layer is static, and an
empty service must never clobber it.

Usage: py -X utf8 scripts/fetch_commercial_frontage.py
"""
import json
import ssl
import sys
import warnings
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from pyproj import Geod, Transformer
from shapely.geometry import shape, LineString, MultiLineString
from shapely.ops import transform as shp_transform
from shapely.strtree import STRtree

warnings.filterwarnings('ignore')

XPLAN_URL = ('https://ags.iplan.gov.il/arcgisiplan/rest/services/'
             'PlanningPublic/Xplan/MapServer/2/query')
DATA = Path(__file__).resolve().parent.parent / 'data'
OUTPUT = DATA / 'commercial_frontage.geojson'

FRONTAGE_CODES = {21100: 'חזית מסחרית', 21110: 'חזית עם הוראות מיוחדות', 56054: 'חזית פעילה'}
JLM_ENVELOPE_ITM = '208000,620000,228000,644000'
PAGE = 1000          # XPLAN caps resultRecordCount at 1000
STREET_MAX_M = 35    # beyond this the nearest road is not "the" street

MASTER_PLANS = {
    'master_plan_moshavot': 'מושבות', 'master_plan_rasko': 'רסקו',
    'master_plan_baka': 'בקעה', 'master_plan_arnona': 'ארנונה',
    'master_plan_gonenim': 'גוננים', 'master_plan_talpiot': 'תלפיות',
}


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
GEOD = Geod(ellps='WGS84')
TO_ITM = Transformer.from_crs(4326, 2039, always_xy=True).transform


_OUT_FIELDS = 'objectid,mavat_code,mavat_name,mp_id,pl_number,pl_name,station_desc,last_update_date'


def _query(where, envelope=True):
    codes = ','.join(str(c) for c in FRONTAGE_CODES)
    feats, offset = [], 0
    while True:
        params = {
            'where': f"mavat_code IN ({codes}) AND {where}",
            'outFields': _OUT_FIELDS,
            'returnGeometry': 'true', 'outSR': 4326, 'f': 'geojson',
            'orderByFields': 'objectid', 'resultOffset': offset, 'resultRecordCount': PAGE,
        }
        if envelope:
            params.update({'geometry': JLM_ENVELOPE_ITM, 'geometryType': 'esriGeometryEnvelope',
                           'inSR': 2039, 'spatialRel': 'esriSpatialRelIntersects'})
        r = S.get(XPLAN_URL, params=params, timeout=120, verify=False)
        r.raise_for_status()
        page = r.json().get('features', [])
        feats.extend(page)
        if envelope:
            print(f'  offset={offset}: {len(page)} (total {len(feats)})')
        if len(page) < PAGE:
            return feats
        offset += PAGE


def fetch_all():
    return _query("pl_number LIKE '101-%'")


def query_plan(pl_number):
    """Raw XPLAN frontage lines of one plan. Raises on a transport error, so a
    caller can tell an outage from a plan that really has no frontage."""
    return _query(f"pl_number='{pl_number}'", envelope=False)


def geodesic_length(geom):
    lines = [geom] if isinstance(geom, LineString) else list(geom.geoms)
    return sum(GEOD.line_length(*zip(*l.coords)) for l in lines)


def load_roads():
    p = DATA / 'roads.geojson'
    if not p.exists():
        return None, []
    gs, names = [], []
    for f in json.loads(p.read_text(encoding='utf-8'))['features']:
        st = ((f.get('properties') or {}).get('street') or '').strip()
        if st and f.get('geometry'):
            gs.append(shp_transform(TO_ITM, shape(f['geometry'])))
            names.append(st)
    return (STRtree(gs) if gs else None), list(zip(gs, names))


def load_master_plans():
    """Boundary polygon per master plan.

    בקעה also carries two "חזית מסחרית" texture polygons, but they are 130-160 m²
    legend swatches, not drawn frontage — the master plans state their commercial
    axes as street names (main_commercial / commercial_axes), which the app matches.
    """
    bounds, axes = [], {}
    for key, name in MASTER_PLANS.items():
        p = DATA / f'{key}.geojson'
        if not p.exists():
            continue
        for f in json.loads(p.read_text(encoding='utf-8'))['features']:
            pr = f.get('properties') or {}
            if not f.get('geometry'):
                continue
            if pr.get('feature_type') == 'boundary':
                bounds.append((name, shp_transform(TO_ITM, shape(f['geometry']))))
                txt = ', '.join(x for x in (pr.get('main_commercial'), pr.get('commercial_axes')) if x)
                if txt:
                    axes[name] = txt
    return bounds, axes


def load_plans():
    idx = {}
    for f in json.loads((DATA / 'plans.geojson').read_text(encoding='utf-8'))['features']:
        p = f.get('properties') or {}
        pn = p.get('plan_name')
        if pn and pn not in idx:
            idx[pn] = p
    return idx


class FrontageContext:
    """Everything a raw XPLAN line needs to become an output feature. Shared with
    update_mavat_ui.py, which refreshes single plans when their status changes —
    so a per-plan refresh and a full rebuild produce identical features."""

    def __init__(self, plans=None):
        self.tree, self.roads = load_roads()
        self.bounds, self.mp_axes = load_master_plans()
        self.plans = load_plans() if plans is None else plans

    def build(self, f, status=None):
        g = f.get('geometry')
        if not g:
            return None
        p = f.get('properties') or {}
        geom = shape(g)
        if not isinstance(geom, (LineString, MultiLineString)) or geom.is_empty:
            return None
        itm = shp_transform(TO_ITM, geom)
        mid = itm.interpolate(0.5, normalized=True)

        street = ''
        if self.tree is not None:
            rg, nm = self.roads[self.tree.nearest(mid)]
            if rg.distance(mid) <= STREET_MAX_M:
                street = nm

        mp = next((n for n, b in self.bounds if b.contains(mid)), '')
        pl = self.plans.get(p.get('pl_number') or '') or {}
        code = p.get('mavat_code')
        return {
            'type': 'Feature',
            'geometry': g,
            'properties': {
                'code': code,
                'kind': FRONTAGE_CODES.get(code, p.get('mavat_name') or ''),
                'plan_name': p.get('pl_number') or '',
                'plan_name_he': pl.get('plan_name_he') or p.get('pl_name') or '',
                'agam_id': int(p['mp_id']) if p.get('mp_id') else None,
                'objectid': p.get('objectid'),
                'status': (status or pl.get('status_mavat') or p.get('station_desc') or '').strip(),
                'in_plans': bool(pl),
                'length_m': round(geodesic_length(geom), 1),
                'street': street,
                'mp_match': mp,
            },
        }


def main():
    raw = fetch_all()
    if not raw:
        print('XPLAN returned 0 frontage lines — keeping the existing file.')
        sys.exit(1)

    ctx = FrontageContext()
    mp_axes = ctx.mp_axes
    out = [x for x in (ctx.build(f) for f in raw) if x]

    # mp_axes travels with the lines: the master-plan files are on-demand in the
    # app, so the report cannot count on their boundary feature being loaded.
    OUTPUT.write_text(json.dumps({'type': 'FeatureCollection', 'mp_axes': mp_axes, 'features': out},
                                 ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
    tot = sum(x['properties']['length_m'] for x in out)
    by_mp = {}
    for x in out:
        k = x['properties']['mp_match'] or '(מחוץ לתכניות אב)'
        by_mp[k] = by_mp.get(k, 0) + x['properties']['length_m']
    print(f'wrote {len(out)} lines, {len({x["properties"]["plan_name"] for x in out})} plans, '
          f'{tot / 1000:.1f} km -> {OUTPUT}')
    for k, v in sorted(by_mp.items(), key=lambda kv: -kv[1]):
        print(f'  {k}: {v:,.0f} m')


if __name__ == '__main__':
    main()
