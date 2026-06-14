"""
scope_filter.py — single source of truth for "what plans are out of project scope".

Used by the regular "בדיקת סטטוס מבאת" pipeline (run_mavat_sync.bat):
  - detect_new_plans.py   — skip out-of-scope plans when ADDING new plans
  - update_mavat_ui.py    — skip out-of-scope plans when REFRESHING statuses

Two mechanisms:
  1. EXCLUDE_PLAN_NUMBERS — specific plan numbers (normalized, leading zeros and
     the 101-/151- prefix stripped) to always ignore. For large/linear plans that
     clip the Rova-4 boundary edge but are out of scope.
  2. Exclusion geometry (EXCLUDE_GEOJSONS) — drop any plan whose unioned geometry
     is majority-inside an out-of-scope area (e.g. שכונת גילה, which sits inside the
     Rova-4 district boundary but is not part of the project).

Keep this list in sync — it is the only place to add new exclusions.
"""

import os
import json

# Out-of-scope areas (WGS84 GeoJSON). A plan whose unioned geometry is
# majority-inside any of these is dropped.
# Resolve against the repo's data/ dir (relative to this file) so the exclusion
# works in CI too — a bare C:\ORANIM path is invisible on the Ubuntu runner,
# which silently disabled the Gilo filter and let ~65 out-of-scope plans through.
_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

def _exclude_path(name):
    win = os.path.join(r"C:\ORANIM\oranim-app\data", name)
    return win if os.path.exists(win) else os.path.join(_DATA_DIR, name)

EXCLUDE_GEOJSONS = [_exclude_path("exclude_gila.geojson")]

# Specific plans to permanently ignore (normalized numbers — see
# normalize_plan_number; prefix and leading zeros stripped).
EXCLUDE_PLAN_NUMBERS = {
    '184523',   # תכנית מתאר לשכונות דרום נחלאות (מרכז העיר, מחוץ לתחום)
    '659540',   # רשת חלוקה לגז טבעי, ככר עין כרם → רכס לבן (תשתית קווית)
    '387068',   # מבנים חקלאיים ותיירות כפרית, מטה יהודה (מחוץ לתחום)
    '292870',   # תכנית מתאר טבע עירוני ירושלים (עיר-כללית)
    '347427',   # רמת רחל (מחוץ לתחום)
    '53751',    # מורדות ארנונה (מחוץ לתחום)
    '979336',   # גבעת המטוס - מק/14295 (מחוץ לתחום)
    '1322924',  # בריכות שחיה/ג'קוזי, מטה יהודה (מחוץ לתחום)
    '1153048',  # גבעת המטוס - מבני ציבור (מחוץ לתחום)
    '1326313',  # גבעת המטוס - מגרשים 139,141 (מחוץ לתחום)
    '253286',   # תוספת שטחי בניה עיקריים עבור מרפסות מקורות (עיר-כללית)
    '92098',    # הוספת שימושים לאכסון תיירותי בירושלים (עיר-כללית)
    '517383',   # שני מגדלים למגורים, גילה (border plan that slipped the geo filter)
}


def normalize_plan_number(pn):
    """'101-0216515' -> '216515'; '151-0387068' -> '387068'. Keeps תתל plans as-is."""
    if not pn:
        return ''
    pn = str(pn).strip()
    if pn.startswith('תתל'):
        return ' '.join(pn.split())
    if '-' in pn:
        _, num = pn.split('-', 1)
        try:
            return str(int(num))
        except ValueError:
            return num
    try:
        return str(int(pn))
    except ValueError:
        return pn


def is_blocklisted(pl_number):
    """True if this plan number is on the permanent out-of-scope blocklist."""
    return normalize_plan_number(pl_number) in EXCLUDE_PLAN_NUMBERS


def load_exclusion_geometry():
    """Union of EXCLUDE_GEOJSONS as a single shapely geometry (WGS84), or None
    if shapely is unavailable or no exclusion files exist."""
    try:
        from shapely.geometry import shape
        from shapely.ops import unary_union
        from shapely.validation import make_valid
    except ImportError:
        return None
    geoms = []
    for path in EXCLUDE_GEOJSONS:
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding='utf-8') as f:
                data = json.load(f)
            feats = data.get('features', []) if data.get('type') == 'FeatureCollection' else [data]
            for feat in feats:
                g = make_valid(shape(feat['geometry']))
                if not g.is_empty:
                    geoms.append(g)
        except Exception:
            continue
    if not geoms:
        return None
    return unary_union(geoms)


def plan_majority_in(features, exclusion_geom):
    """True if >=50% of the plan's unioned area lies inside exclusion_geom (WGS84).
    `features` is a list of GeoJSON features (each with a 'geometry')."""
    if exclusion_geom is None:
        return False
    try:
        from shapely.geometry import shape
        from shapely.ops import unary_union
        from shapely.validation import make_valid
    except ImportError:
        return False
    polys = []
    for feat in features:
        geom = feat.get('geometry')
        if not geom:
            continue
        try:
            g = make_valid(shape(geom))
            if not g.is_empty:
                polys.append(g)
        except Exception:
            continue
    if not polys:
        return False
    try:
        u = unary_union(polys)
        if u.area <= 0:
            return False
        return u.intersection(exclusion_geom).area / u.area >= 0.5
    except Exception:
        return False


def geometry_majority_in(geometry, exclusion_geom):
    """True if >=50% of a single GeoJSON geometry's area lies inside exclusion_geom."""
    if exclusion_geom is None or not geometry:
        return False
    return plan_majority_in([{'geometry': geometry}], exclusion_geom)
