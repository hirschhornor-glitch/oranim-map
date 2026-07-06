# -*- coding: utf-8 -*-
"""Refine education_shanaton marker positions to sit ON a building, preferring
buildings inside a public-building lot (shavaz_kayam).

Per feature (skipping manual/override placements):
  1. anchor = the MOCH (משב"ש) point for one of the feature's semel_chinuch ids
     when it is nearby (<150m), else the current marker.
  2. lot = shavaz_kayam polygon containing the current marker (or, failing
     that, the anchor). Markers on a lot edge count (15m tolerance).
  3. building = OSM building polygon: prefer one containing the anchor; else
     the nearest building intersecting the lot; else (no lot) the nearest
     building within SNAP_RADIUS of the anchor.
  4. new position = representative point of the chosen building.

OSM buildings are fetched from Overpass (bbox of district_oranim) and cached
in data/osm_buildings_cache.json (derivable — not committed).

Run:  py scripts/snap_to_osm_buildings.py [--dry-run]
"""
import json, math, sys, io, os, urllib.request, urllib.parse
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
try:
    from shapely.geometry import shape, Point, Polygon
    from shapely.strtree import STRtree
except ImportError:
    sys.exit('shapely required')
import pandas as pd

DRY = '--dry-run' in sys.argv
ROOT = Path(__file__).parent.parent
DATA = ROOT / 'data'
MOCH_XLSX = Path(r'C:\ORANIM\mosadot_moch\mosadot.xlsx')
OSM_CACHE = DATA / 'osm_buildings_cache.json'
BBOX = (31.7205, 35.1363, 31.7798, 35.2330)   # district_oranim bounds
SNAP_RADIUS = 60      # m — snap-to-building radius when no lot is involved
LOT_EDGE_TOL = 15     # m — marker this close to a lot counts as belonging to it
MOCH_ANCHOR_MAX = 150 # m — ignore MOCH points farther than this from the marker
MAX_MOVE = 150        # m — safety cap; larger computed moves are only reported

KX = 111320 * math.cos(math.radians(31.75))
KY = 111320
def m(lon, lat):  # to local metric space
    return (lon * KX, lat * KY)
def um(x, y):     # back to lon/lat
    return (x / KX, y / KY)

# ── load OSM buildings ────────────────────────────────────────────────────────
def load_osm():
    if OSM_CACHE.exists():
        return json.load(open(OSM_CACHE, encoding='utf-8'))
    q = '[out:json][timeout:180];way["building"](%s,%s,%s,%s);out geom;' % BBOX
    for host in ('https://overpass-api.de/api/interpreter',
                 'https://overpass.kumi.systems/api/interpreter'):
        try:
            req = urllib.request.Request(host, data=('data=' + urllib.parse.quote(q)).encode(),
                                         headers={'User-Agent': 'oranim-gis/1.0'})
            with urllib.request.urlopen(req, timeout=240) as r:
                data = json.load(r)
            json.dump(data, open(OSM_CACHE, 'w', encoding='utf-8'))
            return data
        except Exception as e:
            print(f'  overpass {host} failed: {e!r}', file=sys.stderr)
    sys.exit('could not fetch OSM buildings')

osm = load_osm()
bldgs = []
for el in osm.get('elements', []):
    g = el.get('geometry') or []
    if len(g) < 4: continue
    ring = [m(p['lon'], p['lat']) for p in g]
    if ring[0] != ring[-1]: ring.append(ring[0])
    try:
        poly = Polygon(ring)
        if poly.is_valid and poly.area > 15:   # ignore sheds < 15 m²
            bldgs.append(poly)
    except Exception:
        pass
btree = STRtree(bldgs)
print(f'OSM buildings: {len(bldgs)}')

# ── load shavaz lots ──────────────────────────────────────────────────────────
sv = json.load(open(DATA / 'shavaz_kayam.geojson', encoding='utf-8'))
lots, lot_props = [], []
for f in sv['features']:
    try:
        geom = shape(f['geometry'])
    except Exception:
        continue
    pts = []
    def reproj(gm):
        from shapely.ops import transform
        return transform(lambda x, y, z=None: m(x, y), gm)
    geom = reproj(geom)
    if not geom.is_valid: geom = geom.buffer(0)
    lots.append(geom); lot_props.append(f['properties'])
ltree = STRtree(lots)
print(f'shavaz lots: {len(lots)}')

# ── MOCH anchors: semel -> metric point ──────────────────────────────────────
moch = {}
if MOCH_XLSX.exists():
    df = pd.read_excel(MOCH_XLSX, sheet_name='res_1', dtype=str)
    df = df[df['city'].astype(str).str.contains('ירושלים', na=False)]
    for _, r in df.iterrows():
        eid = str(r.get('education_id') or '').strip()
        if eid.endswith('.0'): eid = eid[:-2]
        try:
            lon, lat = float(r['lon']), float(r['lat'])
        except (TypeError, ValueError):
            continue
        if eid and eid not in moch:
            moch[eid] = m(lon, lat)
print(f'MOCH anchors: {len(moch)}')

# ── refine ────────────────────────────────────────────────────────────────────
gj = json.load(open(DATA / 'education_shanaton.geojson', encoding='utf-8'))
stats = {'skipped_manual': 0, 'kept': 0, 'moved': 0, 'flagged': 0}
outcomes = {}
moves, flags = [], []

def find_lot(pt):
    for i in ltree.query(pt.buffer(LOT_EDGE_TOL)):
        if lots[i].distance(pt) <= LOT_EDGE_TOL:
            return i
    return None

def bldg_containing(pt):
    for i in btree.query(pt):
        if bldgs[i].contains(pt):
            return i
    return None

for f in gj['features']:
    p = f['properties']
    if p.get('_manual_position') or p.get('_position_source') == 'override':
        stats['skipped_manual'] += 1
        continue
    lon, lat = f['geometry']['coordinates']
    cur = Point(*m(lon, lat))

    # 1. anchor from MOCH when nearby
    anchor = cur; anchor_src = 'marker'
    for inst in p['institutions']:
        s = str(inst.get('semel_chinuch') or '')
        if s in moch:
            mp = Point(*moch[s])
            if cur.distance(mp) <= MOCH_ANCHOR_MAX:
                anchor = mp; anchor_src = 'moch'
                break

    # 2. lot
    li = find_lot(cur)
    if li is None and anchor_src == 'moch':
        li = find_lot(anchor)

    # 3. building
    bi = None; how = None
    ci = bldg_containing(anchor)
    if li is not None:
        lot = lots[li]
        if ci is not None and bldgs[ci].intersects(lot):
            bi, how = ci, 'anchor_in_bldg_in_lot'
        else:
            cands = [i for i in btree.query(lot) if bldgs[i].intersects(lot)
                     and bldgs[i].intersection(lot).area > 0.3 * bldgs[i].area]
            if cands:
                bi = min(cands, key=lambda i: bldgs[i].distance(anchor))
                how = 'nearest_bldg_in_lot'
    if bi is None:
        if ci is not None:
            bi, how = ci, 'anchor_in_bldg'
        else:
            cands = [i for i in btree.query(anchor.buffer(SNAP_RADIUS))
                     if bldgs[i].distance(anchor) <= SNAP_RADIUS]
            if cands:
                bi = min(cands, key=lambda i: bldgs[i].distance(anchor))
                how = 'nearest_bldg'
        # a lot exists but has no OSM building: don't jump far outside the lot
        if bi is not None and li is not None and cur.distance(bldgs[bi].representative_point()) > SNAP_RADIUS:
            flags.append((p.get('address'), f'lot has no OSM building; nearest building {cur.distance(bldgs[bi].representative_point()):.0f}m away — left in place'))
            stats['flagged'] += 1
            outcomes['lot_no_bldg'] = outcomes.get('lot_no_bldg', 0) + 1
            continue

    if bi is None:
        # no building found at all
        if li is not None and lots[li].distance(cur) > LOT_EDGE_TOL:
            flags.append((p.get('address'), 'lot found but no OSM building; marker outside lot'))
            stats['flagged'] += 1
        else:
            stats['kept'] += 1
        outcomes['no_building'] = outcomes.get('no_building', 0) + 1
        continue

    tgt = bldgs[bi].representative_point()
    dist = cur.distance(tgt)
    outcomes[how] = outcomes.get(how, 0) + 1
    if dist <= 5:
        stats['kept'] += 1
        continue
    if dist > MAX_MOVE:
        flags.append((p.get('address'), f'{how}: computed move {dist:.0f}m > cap'))
        stats['flagged'] += 1
        continue

    nlon, nlat = um(tgt.x, tgt.y)
    moves.append((p.get('address'), round(dist, 1), how, li is not None, anchor_src))
    if not DRY:
        p['_osm_snapped_from'] = [lon, lat]
        p['_prev_position_source'] = p.get('_position_source')
        p['_position_source'] = ('osm_bldg_in_shavaz' if li is not None else 'osm_bldg_snap')
        p['_osm_anchor'] = anchor_src
        f['geometry']['coordinates'] = [round(nlon, 6), round(nlat, 6)]
    stats['moved'] += 1

# ── after-stats: how many markers now on building / in shavaz ────────────────
on_b = in_s = 0
for f in gj['features']:
    pt = Point(*m(*f['geometry']['coordinates']))
    if bldg_containing(pt) is not None: on_b += 1
    if find_lot(pt) is not None: in_s += 1

print(f"\nstats: {stats}")
print(f"outcomes: {outcomes}")
n = len(gj['features'])
print(f"after: on OSM building {on_b}/{n}, on/near shavaz lot {in_s}/{n}")
print(f"\nmoves ({len(moves)}), largest first:")
for a, d, how, lot, anc in sorted(moves, key=lambda x: -x[1])[:40]:
    print(f"  {d:>6}m  lot={'Y' if lot else 'n'} anchor={anc:<6} {how:<22} {a!r}")
print(f"\nflags ({len(flags)}):")
for a, why in flags:
    print(f"  {a!r}: {why}")

if not DRY:
    bak = DATA / 'education_shanaton.backup_pre_osm_snap.geojson'
    if not bak.exists():
        bak.write_text((DATA / 'education_shanaton.geojson').read_text(encoding='utf-8'), encoding='utf-8')
    json.dump(gj, open(DATA / 'education_shanaton.geojson', 'w', encoding='utf-8'),
              ensure_ascii=False, separators=(',', ':'))
    print('\nwrote education_shanaton.geojson')
else:
    print('\n[DRY RUN]')
