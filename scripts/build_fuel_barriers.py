# -*- coding: utf-8 -*-
"""
build_fuel_barriers.py — חסמי תחנות דלק למבני ציבור (תמ"א 18).

תמ"א 18/4 סעיף 15: מרחק מינימלי מאי המשאבות — 40 מ' לבנייני מגורים,
80 מ' לבתי חולים / מעונות קשישים / מוסדות חינוך ונוער.

מקורות תחנות:
  1. OSM Overpass amenity=fuel בתחום אורנים + שוליים (תחנות קיימות בפועל).
  2. data/govmap_fuel_stations.json — POI תחנות תדלוק מ-govmap (snapshot; govmap
     נגיש רק מדפדפן המשתמש — לרענון ראו ההערה בקובץ). תופס עצמאיות שאין ב-OSM.
  3. yiud_karka_kayam — ייעוד עירוני 7410 "שטח לתחנת דלק" / Descr עם "דלק".
  4. landuse_xplan — קוד מבא"ת 910 "תחנת תדלוק" (תחנות בתכניות עתידיות).

יעדים (השטחים החומים):
  - future_shavaz.geojson — מפתח TABA|MIGRASH.
  - landuse_xplan עם קודי הפרשה מבונה — מפתח taba|num (אותו keyspace כמו הפופאפ).

לכל מגרש בטווח: מרחק, band (critical<=80 / watch<=150), התחנה הקרובה,
והתב"עות העתידיות שבתחומן התחנה יושבת (פוטנציאל פינוי → עיכוב מסירה).

פלט: data/fuel_barriers.json. המדידה מגבול מגרש/נקודת OSM ולא מאי המשאבות —
סינון שמרני; band=watch עשוי להיות בעייתי אם המשאבות בקצה המגרש.
"""
import json
import sys
import urllib.request
import urllib.parse
from datetime import date
from pathlib import Path

from shapely.geometry import shape, Point
from shapely.ops import transform
import pyproj

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / 'data'
OUT = DATA / 'fuel_barriers.json'

CRITICAL_M = 80   # תמ"א 18: מוסדות חינוך/בריאות/קשישים
WATCH_M = 150     # טווח בדיקה — אי-ודאות מיקום אי המשאבות בתוך מגרש התחנה

HAFRASHAH_CODES = {1250, 1300, 1410, 1480, 1492, 1550, 1576, 1578, 1604}
# xplan lots rendered as the שב"צ עתידי polygons in-app — lot numbering differs from
# future_shavaz.geojson MIGRASH, so both keyspaces must be present in by_lot
SHAVAZ_XPLAN_CODES = {400, 410, 450, 460, 1670}
# mirror of the app's hafrashah_future selection (src/app.jsx):
# a plan with hafrash_sqm but no per-lot data and no HAFRASHAH-coded lot gets its
# marker on the largest residential/mixed lot — that lot must be screened too
RESIDENTIAL_MIXED_CODES = {10, 20, 60, 100, 140, 145, 290, 1000, 1050, 1311, 1321, 1420, 1470}
import re
HAFRASH_LOT_PREFIX_RE = re.compile(r'^מגרש\s+([\dא-תA-Za-z/+]+)\s*[-–]\s*')

to_itm = pyproj.Transformer.from_crs(4326, 2039, always_xy=True).transform


def load(fn):
    with open(DATA / fn, encoding='utf-8') as f:
        return json.load(f)


def district_bbox(gd, margin_deg=0.003):
    xs, ys = [], []
    for f in gd['features']:
        g = f['geometry']
        polys = g['coordinates'] if g['type'] == 'MultiPolygon' else [g['coordinates']]
        for poly in polys:
            for ring in poly:
                for x, y in ring:
                    xs.append(x)
                    ys.append(y)
    return (min(ys) - margin_deg, min(xs) - margin_deg, max(ys) + margin_deg, max(xs) + margin_deg)


def fetch_osm_stations(bbox):
    s, w, n, e = bbox
    q = (f'[out:json][timeout:60];(node["amenity"="fuel"]({s},{w},{n},{e});'
         f'way["amenity"="fuel"]({s},{w},{n},{e}););out center tags;')
    d = None
    last_err = None
    for host in ('https://overpass-api.de/api/interpreter',
                 'https://overpass.kumi.systems/api/interpreter',
                 'https://overpass.private.coffee/api/interpreter'):
        try:
            req = urllib.request.Request(
                host,
                data=('data=' + urllib.parse.quote(q)).encode(),
                headers={'User-Agent': 'oranim-gis/1.0 (orenpj@gmail.com)',
                         'Accept': 'application/json',
                         'Content-Type': 'application/x-www-form-urlencoded'})
            d = json.load(urllib.request.urlopen(req, timeout=90))
            break
        except Exception as e:
            last_err = e
    if d is None:
        raise last_err
    out = []
    for el in d['elements']:
        lat = el.get('lat') or el['center']['lat']
        lon = el.get('lon') or el['center']['lon']
        t = el.get('tags', {})
        label = t.get('name') or t.get('brand') or 'תחנת דלק'
        out.append({'id': f"osm_{el['id']}", 'label': label, 'kind': 'existing',
                    'lat': lat, 'lon': lon})
    return out


def taba_from_pl_number(pl_number):
    s = str(pl_number or '')
    if '-' in s:
        try:
            return str(int(s.split('-')[1]))
        except ValueError:
            return s
    return s


def main():
    district = load('district_oranim.geojson')
    bbox = district_bbox(district)

    # --- 1. stations ---
    stations = []  # {'id','label','kind','geom' (ITM), 'lat','lon'}
    try:
        osm = fetch_osm_stations(bbox)
        print(f'OSM stations: {len(osm)}')
    except Exception as e:
        print(f'OSM fetch failed ({e}); reusing stations from previous output', file=sys.stderr)
        prev = json.load(open(OUT, encoding='utf-8')) if OUT.exists() else {'stations': []}
        osm = [s for s in prev['stations'] if s['kind'] == 'existing']
        if not osm:
            sys.exit('no OSM stations available — aborting')
    for s in osm:
        stations.append({**s, 'geom': transform(to_itm, Point(s['lon'], s['lat']))})

    # govmap POI snapshot — dedup against OSM: within 40m = the same station twice
    try:
        gm = load('govmap_fuel_stations.json')['stations']
    except FileNotFoundError:
        gm = []
    gm_added = 0
    for s in gm:
        g = transform(to_itm, Point(s['lon'], s['lat']))
        if any(g.distance(t['geom']) < 40 for t in stations):
            continue
        stations.append({'id': 'govmap_' + s['id'].split('|')[-1], 'label': s['text'],
                         'kind': 'existing', 'lat': s['lat'], 'lon': s['lon'], 'geom': g})
        gm_added += 1
    print(f'govmap stations added (not in OSM): {gm_added}/{len(gm)}')

    kayam = load('yiud_karka_kayam.geojson')
    for f in kayam['features']:
        p = f['properties']
        descr = str(p.get('Descr') or '')
        if 'דלק' not in descr and 'תדלוק' not in descr:
            continue
        g = transform(to_itm, shape(f['geometry']))
        rep = f['geometry']
        stations.append({'id': f"kayam_{p.get('fid')}",
                         'label': f"מגרש בייעוד תחנת דלק — תב\"ע {p.get('TABA')} מגרש {p.get('MIGRASH')}",
                         'kind': 'zoned_lot', 'geom': g,
                         'lon': shape(rep).representative_point().x,
                         'lat': shape(rep).representative_point().y})

    xplan = load('landuse_xplan.geojson')
    for f in xplan['features']:
        p = f['properties']
        if p.get('mavat_code') != 910:
            continue
        g = transform(to_itm, shape(f['geometry']))
        stations.append({'id': f"xplan_{p.get('objectid')}",
                         'label': f"ייעוד תחנת תדלוק בתכנית {p.get('pl_number')}",
                         'kind': 'planned', 'geom': g,
                         'lon': shape(f['geometry']).representative_point().x,
                         'lat': shape(f['geometry']).representative_point().y})
    print(f'stations total: {len(stations)}')

    # --- 2. station → containing future plans (evacuation potential) ---
    plans = load('plans.geojson')
    plan_geoms = []
    MAX_PLAN_SQM = 500_000  # תכניות-על עירוניות (חיזוק מבנים, תקן חניה, 130 אלף דונם) — לא "תכנית שמפנה תחנה"
    DEAD_STATUS = ('נגנזה', 'נדחתה', 'בוטלה', 'ארכיון')
    for f in plans['features']:
        if not f.get('geometry'):
            continue
        status = f['properties'].get('status_mavat') or ''
        if any(x in status for x in DEAD_STATUS):
            continue
        try:
            g = transform(to_itm, shape(f['geometry']))
        except Exception:
            continue
        if g.area > MAX_PLAN_SQM:
            continue
        plan_geoms.append((f['properties'], g))
    for s in stations:
        here = []
        for p, g in plan_geoms:
            try:
                # buffer tolerance: an OSM point can sit anywhere on the station lot, and muni
                # lots vs Mavat blue lines are different sources — allow the station to fall a
                # bit outside the plan polygon and still count as "inside"
                if g.distance(s['geom']) <= 30:
                    here.append({'taba': str(p.get('taba') or ''),
                                 'name': (p.get('plan_name_he') or p.get('plan_summary') or '')[:80],
                                 'status': p.get('status_mavat') or '',
                                 '_area': g.area})
            except Exception:
                continue
        here.sort(key=lambda x: x['_area'])  # הספציפית ביותר קודם
        s['plans_here'] = [{k: v for k, v in h.items() if k != '_area'} for h in here[:4]]

    # --- 3. targets: future_shavaz lots + hafrashah lots from landuse_xplan ---
    # skip lots of dead plans (the app drops them from the layer too)
    status_by_taba = {str(f['properties'].get('taba') or ''): f['properties'].get('status_mavat') or ''
                      for f in plans['features']}

    def plan_dead(taba):
        return any(x in status_by_taba.get(str(taba), '') for x in DEAD_STATUS)

    targets = []  # {'key','taba','lot','geom'}
    fs = load('future_shavaz.geojson')
    for f in fs['features']:
        p = f['properties']
        taba = str(p.get('TABA') or '').strip()
        lot = str(p.get('MIGRASH') or '').strip()
        if not taba or not f.get('geometry') or plan_dead(taba):
            continue
        targets.append({'key': f'{taba}|{lot}', 'taba': taba, 'lot': lot,
                        'name': p.get('plan_name_he') or p.get('NAME') or '',
                        'uses': p.get('uses') or '',
                        'geom': transform(to_itm, shape(f['geometry']))})
    # per-lot hafrasha breakdown from hafrash_prg ("מגרש 12 - ..."), like the app's
    # __hafrashByTabaLot; plans without lot prefixes fall back to the largest
    # residential/mixed lot (the app puts the hafrashah marker there)
    hafrash_lots = {}   # taba -> set of lot nums named in hafrash_prg
    hafrash_sqm = {}    # taba -> hafrash_sqm > 0
    for f in plans['features']:
        p = f['properties']
        taba = str(p.get('taba') or '')
        try:
            if float(p.get('hafrash_sqm') or 0) > 0:
                hafrash_sqm[taba] = True
        except ValueError:
            pass
        for part in str(p.get('hafrash_prg') or '').split(';'):
            m = HAFRASH_LOT_PREFIX_RE.match(part.strip())
            if m:
                hafrash_lots.setdefault(taba, set()).add(m.group(1))

    covered = set()
    fallback_candidates = {}  # taba -> (shape_area, target dict)
    for f in xplan['features']:
        p = f['properties']
        code = p.get('mavat_code')
        if not f.get('geometry'):
            continue
        taba = taba_from_pl_number(p.get('pl_number'))
        if plan_dead(taba):
            continue
        lot = str(p.get('num') or '').strip()
        tgt = {'key': f'{taba}|{lot}', 'taba': taba, 'lot': lot,
               'name': p.get('pl_name') or '',
               'uses': p.get('mavat_name') or '',
               'geom': transform(to_itm, shape(f['geometry']))}
        if code in SHAVAZ_XPLAN_CODES:
            targets.append(tgt)
            covered.add(taba)
        elif taba in hafrash_lots:
            if lot in hafrash_lots[taba]:
                targets.append(tgt)
                covered.add(taba)
        elif code in HAFRASHAH_CODES:
            targets.append(tgt)
            covered.add(taba)
        elif code in RESIDENTIAL_MIXED_CODES and hafrash_sqm.get(taba):
            area = p.get('shape_area') or 0
            if taba not in fallback_candidates or fallback_candidates[taba][0] < area:
                fallback_candidates[taba] = (area, tgt)
    for taba, (_, tgt) in fallback_candidates.items():
        if taba not in covered:
            targets.append(tgt)
    print(f'targets: {len(targets)} (incl. {sum(1 for t, c in fallback_candidates.items() if t not in covered)} hafrash fallback lots)')

    # --- 4. proximity ---
    by_lot = {}
    for t in targets:
        best = None
        for s in stations:
            d = t['geom'].distance(s['geom'])
            if best is None or d < best[0]:
                best = (d, s)
        if best is None or best[0] > WATCH_M:
            continue
        d, s = best
        rec = {'dist_m': round(d, 1),
               'band': 'critical' if d <= CRITICAL_M else 'watch',
               'station': s['label'], 'station_kind': s['kind'],
               'station_id': s['id'],
               'station_plans': s['plans_here'],
               'name': t['name'], 'uses': t['uses']}
        # several xplan lots can share a key with a future_shavaz lot — keep the nearest
        if t['key'] not in by_lot or by_lot[t['key']]['dist_m'] > rec['dist_m']:
            by_lot[t['key']] = rec

    by_taba = {}
    for key, rec in by_lot.items():
        taba = key.split('|')[0]
        e = by_taba.setdefault(taba, {'min_dist': rec['dist_m'], 'lots': []})
        e['lots'].append(key.split('|')[1])
        e['min_dist'] = min(e['min_dist'], rec['dist_m'])

    out = {
        'generated': date.today().isoformat(),
        'params': {'critical_m': CRITICAL_M, 'watch_m': WATCH_M,
                   'rule': 'תמ"א 18/4 ס\'15: 40 מ\' ממגורים, 80 מ\' ממוסדות חינוך/בריאות/קשישים — נמדד מאי המשאבות'},
        'stations': [{k: v for k, v in s.items() if k != 'geom'} for s in stations],
        'by_lot': by_lot,
        'by_taba': by_taba,
    }
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    crit = sum(1 for r in by_lot.values() if r['band'] == 'critical')
    print(f'wrote {OUT.name}: {len(by_lot)} flagged lots ({crit} critical, {len(by_lot) - crit} watch), {len(by_taba)} plans')


if __name__ == '__main__':
    main()
