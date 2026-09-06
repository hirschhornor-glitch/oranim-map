"""
refresh_landuse_xplan.py
Refreshes landuse_xplan.geojson from XPLAN API using district_oranim boundary.
Output: WGS84 GeoJSON ready for the web app.
"""
import json, ssl, sys, warnings
warnings.filterwarnings('ignore')
import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

BOUNDARY_FILE = r"C:\ORANIM\oranim-app\data\district_oranim.geojson"
OUTPUT_FILE = r"C:\ORANIM\oranim-app\data\landuse_xplan.geojson"
XPLAN_URL = "https://ags.iplan.gov.il/arcgisiplan/rest/services/PlanningPublic/Xplan/MapServer/4/query"
MAX_PER_REQUEST = 1000

# תכניות שנוספו ידנית למאגר אף שהן מחוץ לגבול רובע 4 (ראה add_plan_manual.py).
# מסנן-הגבול למטה היה מוחק את מגרשי ייעוד-הקרקע שלהן בכל ריענון, ולכן הן נשמרות
# במפורש. להוסיף כאן כל תכנית חוץ-גבולית שמוכנסת ידנית.
INCLUDE_PLAN_NUMBERS = {
    "101-1214279",   # מתחם הגימנסיה העברית, רחביה — צפונית לגבול הרובע
}


class _SSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *a, **kw):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.set_ciphers('DEFAULT:@SECLEVEL=0')
        kw['ssl_context'] = ctx
        return super().init_poolmanager(*a, **kw)

_SESSION = requests.Session()
_SESSION.mount('https://', _SSLAdapter())


def get_bbox_itm():
    """Get ITM bounding box from district_oranim boundary."""
    with open(BOUNDARY_FILE, encoding='utf-8') as f:
        data = json.load(f)

    coords = []
    for feat in data['features']:
        geom = feat['geometry']
        if geom['type'] == 'Polygon':
            for ring in geom['coordinates']:
                coords.extend(ring)
        elif geom['type'] == 'MultiPolygon':
            for poly in geom['coordinates']:
                for ring in poly:
                    coords.extend(ring)

    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]

    # Convert WGS84 to ITM
    try:
        from pyproj import Transformer
        t = Transformer.from_crs("EPSG:4326", "EPSG:2039", always_xy=True)
        x_min, y_min = t.transform(min(lons), min(lats))
        x_max, y_max = t.transform(max(lons), max(lats))
        # Add 500m buffer
        return (int(x_min) - 500, int(y_min) - 500, int(x_max) + 500, int(y_max) + 500)
    except ImportError:
        # Fallback with buffer
        return (215000, 627000, 224000, 636000)


def point_in_polygon(x, y, polygon_rings):
    ring = polygon_rings[0]
    n = len(ring)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = ring[i]
        xj, yj = ring[j]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def point_in_ring(x, y, ring):
    """Ray-casting point-in-polygon test for a single ring."""
    n = len(ring)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = ring[i]
        xj, yj = ring[j]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def feature_intersects_boundary(feat, boundary_rings):
    """Check if feature geometry intersects the boundary polygon."""
    geom = feat.get('geometry')
    if not geom:
        return False

    feat_outer_rings = []
    if geom.get('type') == 'Polygon':
        feat_outer_rings = [geom['coordinates'][0]]
    elif geom.get('type') == 'MultiPolygon':
        for poly in geom['coordinates']:
            feat_outer_rings.append(poly[0])

    if not feat_outer_rings:
        return False

    boundary_outer = boundary_rings[0]

    for ring in feat_outer_rings:
        for c in ring:
            if point_in_polygon(c[0], c[1], boundary_rings):
                return True

    for bx, by in boundary_outer:
        for ring in feat_outer_rings:
            if point_in_ring(bx, by, ring):
                return True

    return False


def load_boundary_rings():
    with open(BOUNDARY_FILE, encoding='utf-8') as f:
        data = json.load(f)
    for feat in data['features']:
        geom = feat['geometry']
        if geom['type'] == 'Polygon':
            return geom['coordinates']
        elif geom['type'] == 'MultiPolygon':
            return geom['coordinates'][0]
    return None


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    print("Refreshing landuse_xplan.geojson from XPLAN API...")

    bbox = get_bbox_itm()
    print(f"ITM bbox: {bbox}")

    # Fetch all features
    all_features = []
    offset = 0
    while True:
        params = {
            'geometry': f'{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}',
            'geometryType': 'esriGeometryEnvelope',
            'inSR': '2039',
            'spatialRel': 'esriSpatialRelIntersects',
            'outFields': '*',
            'returnGeometry': 'true',
            'f': 'geojson',
            'outSR': '4326',
            'resultOffset': offset,
            'resultRecordCount': MAX_PER_REQUEST,
        }
        print(f"  Fetching offset={offset}...", end=" ", flush=True)
        r = _SESSION.get(XPLAN_URL, params=params, timeout=60, verify=False)
        r.raise_for_status()
        data = r.json()
        feats = data.get('features', [])
        all_features.extend(feats)
        print(f"{len(feats)} features (total: {len(all_features)})")
        if len(feats) < MAX_PER_REQUEST:
            break
        offset += MAX_PER_REQUEST

    print(f"\nTotal fetched: {len(all_features)}")

    # Filter to features intersecting boundary
    boundary = load_boundary_rings()
    if boundary:
        clipped = []
        kept_manual = 0
        for feat in all_features:
            if feature_intersects_boundary(feat, boundary):
                clipped.append(feat)
            elif (feat.get('properties') or {}).get('pl_number') in INCLUDE_PLAN_NUMBERS:
                clipped.append(feat)
                kept_manual += 1
        print(f"After filtering to boundary: {len(clipped)} features (from {len(all_features)})"
              f"{f'; kept {kept_manual} out-of-boundary parcels from INCLUDE_PLAN_NUMBERS' if kept_manual else ''}")
        all_features = clipped

    # Guard: never overwrite the master file with an empty result. iplan emptied
    # land-use layer 4 (~2026-06 migration); 0 features means "source is down",
    # not "no land use". Abort so the committed landuse_xplan.geojson survives.
    if not all_features:
        print(f"\nABORT: 0 land-use features from XPLAN (layer 4 is empty — likely "
              f"iplan migration). Refusing to overwrite {OUTPUT_FILE}.")
        sys.exit(1)

    # Save
    geojson = {
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
        "features": all_features
    }
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False)

    print(f"\nSaved: {OUTPUT_FILE} ({len(all_features)} features)")


if __name__ == "__main__":
    main()
