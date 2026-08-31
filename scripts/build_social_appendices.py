# -*- coding: utf-8 -*-
"""
build_social_appendices.py
--------------------------
Turns the hand-extracted C:\\ORANIM\\social_appendices.json (keyed by taba) into the
app's data/social_appendices.json (keyed by plan_name, like every other plan-attribute
file), and bakes in the CBS-2022 statistical-area comparison for each complex.

The appendix numbers describe the INCOMING state (the complex as it stands before
demolition) — never the approved plan. Derived percentages therefore divide by
units_existing, never by units_add/units_total.

Signature fields are carried through verbatim but are NOT derived into any metric:
approval needs 60% and demolition needs 100%, so a survey-date snapshot predicts
nothing (see _meta.signatures_note in the source file).

Run:  py oranim-app/scripts/build_social_appendices.py
"""
import json
import os
import sys

if sys.platform.startswith('win'):
    sys.stdout.reconfigure(encoding='utf-8')

ROOT = r"C:\ORANIM"
SRC = os.path.join(ROOT, "social_appendices.json")
DATA = os.path.join(ROOT, "oranim-app", "data")
PLANS = os.path.join(DATA, "plans.geojson")
STAT_AREAS = os.path.join(DATA, "stat_areas.geojson")
OUT = os.path.join(DATA, "social_appendices.json")


def centroid(geom):
    pts = []

    def walk(c, depth):
        if depth == 0:
            pts.append(c)
        else:
            for x in c:
                walk(x, depth - 1)

    depths = {'Point': 0, 'LineString': 1, 'MultiLineString': 2, 'Polygon': 2, 'MultiPolygon': 3}
    walk(geom['coordinates'], depths[geom['type']])
    if not pts:
        return None
    return (sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts))


def point_in_geom(pt, geom):
    def ring_test(x, y, ring):
        inside = False
        n = len(ring)
        for i in range(n):
            x1, y1 = ring[i][:2]
            x2, y2 = ring[(i + 1) % n][:2]
            if (y1 > y) != (y2 > y):
                if x < (x2 - x1) * (y - y1) / ((y2 - y1) or 1e-12) + x1:
                    inside = not inside
        return inside

    x, y = pt
    polys = [geom['coordinates']] if geom['type'] == 'Polygon' else geom['coordinates']
    for poly in polys:
        if ring_test(x, y, poly[0]) and not any(ring_test(x, y, h) for h in poly[1:]):
            return True
    return False


def pct(part, whole):
    """Percent, rounded to one decimal. None whenever the base is missing/zero."""
    try:
        part = float(part)
        whole = float(whole)
    except (TypeError, ValueError):
        return None
    if whole <= 0:
        return None
    return round(100.0 * part / whole, 1)


def derive_rented_pct(rec):
    """Rental share of the incoming complex, from whichever fields the appendix gave."""
    if rec.get('rented_pct') is not None:
        return float(rec['rented_pct'])
    p = pct(rec.get('rented'), rec.get('units_existing'))
    if p is not None:
        return p
    # some appendices only report the tenure split without a stated total
    rented = rec.get('rented')
    owner = rec.get('owner_occupied')
    if rented is not None and owner is not None:
        return pct(rented, float(rented) + float(owner) + float(rec.get('public_housing') or 0))
    if rec.get('absentee_owners_pct') is not None:
        return float(rec['absentee_owners_pct'])
    return None


def derive_elderly_pct(rec):
    """65+ share. age_75plus_* is NOT folded in — a different cohort, not comparable."""
    for key in ('age_65plus_pct', 'owners_65plus_pct'):
        if rec.get(key) is not None:
            return float(rec[key])
    dist = rec.get('age_dist_pct') or {}
    if '65+' in dist:
        return float(dist['65+'])
    if rec.get('elderly_total') is not None and rec.get('units_existing'):
        return None  # a head-count of people against a count of units is not a share
    return None


def main():
    src = json.load(open(SRC, encoding='utf-8'))
    plans = json.load(open(PLANS, encoding='utf-8'))
    stat = json.load(open(STAT_AREAS, encoding='utf-8'))

    geom_by_taba, props_by_taba = {}, {}
    for f in plans['features']:
        p = f['properties']
        t = p.get('taba')
        if not t:
            continue
        try:
            key = str(int(float(t)))
        except (TypeError, ValueError):
            continue
        if f.get('geometry'):
            geom_by_taba[key] = f['geometry']
        props_by_taba[key] = p

    out = {}
    no_geom, no_area = [], []
    for taba, rec in src['plans'].items():
        rec = dict(rec)
        props = props_by_taba.get(taba, {})
        plan_name = props.get('plan_name') or '101-%07d' % int(taba)

        rec['taba'] = taba
        rec['rented_pct_calc'] = derive_rented_pct(rec)
        rec['public_housing_pct'] = pct(rec.get('public_housing'), rec.get('units_existing'))
        rec['elderly_pct_calc'] = derive_elderly_pct(rec)

        geom = geom_by_taba.get(taba)
        area = None
        if geom:
            c = centroid(geom)
            for f in stat['features']:
                if f.get('geometry') and point_in_geom(c, f['geometry']):
                    area = f['properties']
                    break
            if area is None:
                no_area.append(taba)
        else:
            no_geom.append(taba)

        if area:
            cbs = {
                'stat_area_id': area.get('stat_area_id'),
                'rent_pcnt': area.get('rent_pcnt'),
                'age65_pcnt': area.get('age65_pcnt'),
                'household_size': area.get('householdSize'),
            }
            # Deltas: complex minus its statistical area. Positive = the complex has
            # MORE of it than the surrounding area. These are what the map colours by.
            if rec['rented_pct_calc'] is not None and cbs['rent_pcnt'] is not None:
                cbs['delta_rent'] = round(rec['rented_pct_calc'] - float(cbs['rent_pcnt']), 1)
            if rec['elderly_pct_calc'] is not None and cbs['age65_pcnt'] is not None:
                cbs['delta_elderly'] = round(rec['elderly_pct_calc'] - float(cbs['age65_pcnt']), 1)
            if rec.get('avg_household_size') is not None and cbs['household_size'] is not None:
                cbs['delta_household'] = round(float(rec['avg_household_size']) - float(cbs['household_size']), 2)
            rec['cbs'] = cbs

        out[plan_name] = rec

    payload = {'_meta': dict(src['_meta'], built_from='social_appendices.json (taba-keyed)',
                             keyed_by='plan_name'),
               'plans': out}
    json.dump(payload, open(OUT, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)

    with_cbs = sum(1 for v in out.values() if v.get('cbs'))
    with_delta = sum(1 for v in out.values() if (v.get('cbs') or {}).get('delta_rent') is not None)
    print(f"wrote {OUT}")
    print(f"  plans: {len(out)} | with CBS area: {with_cbs} | with rent delta: {with_delta}")
    if no_geom:
        print(f"  ⚠ no geometry: {no_geom}")
    if no_area:
        print(f"  ⚠ centroid outside every stat area: {no_area}")


if __name__ == '__main__':
    main()
