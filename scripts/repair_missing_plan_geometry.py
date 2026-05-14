"""
Fill in missing plan geometry from landuse_xplan.geojson.

Plans whose Mavat polygon never landed in plans.geojson (geometry: null) can
often be reconstructed by unioning the XPLAN land-use polygons that share the
same Mavat plan id (agam_id <-> mp_id).

The union must go through shapely.unary_union so the resulting MultiPolygon
renders correctly under Leaflet's evenodd fill rule (see feedback_multipolygon_evenodd).

Precision is preserved (no simplification) per feedback_geojson_precision.
"""
import json
import sys
from collections import defaultdict
from pathlib import Path

from shapely.geometry import shape, mapping, MultiPolygon, Polygon, GeometryCollection
from shapely.ops import unary_union
from shapely.validation import make_valid


def to_polygonal(geom):
    """Coerce any geometry into a Polygon / MultiPolygon (drop stray lines/points)."""
    if isinstance(geom, (Polygon, MultiPolygon)):
        return geom
    if isinstance(geom, GeometryCollection):
        polys = [g for g in geom.geoms if isinstance(g, (Polygon, MultiPolygon))]
        if not polys:
            return None
        merged = unary_union(polys)
        return merged if isinstance(merged, (Polygon, MultiPolygon)) else None
    return None

DATA_DIR = Path(__file__).resolve().parent.parent / 'data'
PLANS_PATH = DATA_DIR / 'plans.geojson'
XPLAN_PATH = DATA_DIR / 'landuse_xplan.geojson'


def main():
    with PLANS_PATH.open('r', encoding='utf-8') as f:
        plans = json.load(f)
    with XPLAN_PATH.open('r', encoding='utf-8') as f:
        xplan = json.load(f)

    polys_by_mp = defaultdict(list)
    for feat in xplan['features']:
        geom = feat.get('geometry')
        if not geom:
            continue
        mp = feat.get('properties', {}).get('mp_id')
        if mp is None:
            continue
        try:
            polys_by_mp[int(mp)].append(shape(geom))
        except Exception as e:
            print(f'  warn: skipping bad XPLAN geom for mp_id={mp}: {e}', file=sys.stderr)

    repaired = []
    unresolved = []
    for feat in plans['features']:
        existing = feat.get('geometry')
        # Re-process if missing OR if it's a non-polygonal type (e.g. GeometryCollection
        # left over from a previous run before this fix was added).
        if existing and existing.get('type') in ('Polygon', 'MultiPolygon'):
            continue
        props = feat.get('properties') or {}
        plan_name = props.get('plan_name') or '(no name)'
        agam = props.get('agam_id')
        if agam is None:
            unresolved.append((plan_name, 'no agam_id'))
            continue
        polys = polys_by_mp.get(int(float(agam)))
        if not polys:
            unresolved.append((plan_name, f'no XPLAN match for mp_id={int(float(agam))}'))
            continue
        fixed = [p if p.is_valid else make_valid(p) for p in polys]
        merged = unary_union(fixed)
        polygonal = to_polygonal(merged)
        if polygonal is None or polygonal.is_empty:
            unresolved.append((plan_name, f'union produced non-polygonal geometry ({merged.geom_type})'))
            continue
        feat['geometry'] = mapping(polygonal)
        repaired.append((plan_name, polygonal.geom_type, len(polys)))

    print(f'repaired: {len(repaired)}')
    for name, gtype, n in repaired:
        print(f'  + {name}: {gtype} from {n} XPLAN poly(s)')
    print(f'unresolved: {len(unresolved)}')
    for name, reason in unresolved:
        print(f'  - {name}: {reason}')

    if repaired:
        with PLANS_PATH.open('w', encoding='utf-8') as f:
            json.dump(plans, f, ensure_ascii=False)
        print(f'wrote {PLANS_PATH}')
    else:
        print('no changes to write')


if __name__ == '__main__':
    main()
