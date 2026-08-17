# -*- coding: utf-8 -*-
r"""
minahak_pip.py — derive minahak / sub_neighborhood for a plan from its geometry.

Why this exists: detect_new_plans stamped every newly caught plan with
minahak='' (it's a hand-curated field). But app.jsx's isPlanVisible() hides any
plan with an empty minahak — and the ייעודי-קרקע layer filters its polygons
through that same predicate. So a brand-new plan showed on the map (via the
"תכניות חדשות" topic layer, which bypasses isPlanVisible) while its land-use
parcels stayed invisible until someone filled minahak by hand.

Assignment is by largest overlap area against the municipal boundary layers in
oranim-app/data, not a centroid point-in-polygon: plans that straddle a minahak
edge (road widenings, corner lots) then land in the minahak that actually holds
most of the plan.

Callers must treat the result as FILL-ONLY — never overwrite a curated value
coming from the Google Sheet.
"""
import os
import json

# Same local-first / repo-relative resolution as detect_new_plans.py: the
# Windows paths exist on the machine that runs the scheduled tasks, CI (Ubuntu)
# falls back to this checkout.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)


def _data_file(name):
    win = os.path.join(r"C:\ORANIM\oranim-app\data", name)
    return win if os.path.exists(win) else os.path.join(_REPO_ROOT, "data", name)


# Layer file → canonical minahak name. The canonical spelling is the one used
# in plans.geojson and in app.jsx's MINAHAK_SUBS — it differs from the layer's
# own shem_1 for two of them (noted inline), so don't read the name off the
# polygon.
MINAHAK_LAYERS = [
    ("minahak_ganot.geojson", "גינות העיר"),
    ("minahak_beit_tzfafa.geojson", "בית צפאפא"),
    ("minahak_gonen.geojson", "גוננים"),
    ("minahak_baka.geojson", "בקעה רבתי"),          # layer shem_1 = 'בקעה'
    ("minahak_malha.geojson", "מינהל מוסדי מלחה"),
    ("minahak_talpiot.geojson", "א.ת. תלפיות"),       # layer shem_1 = 'אזור תעשייה תלפיות'
]

SUB_LAYER = "sub_neighborhoods.geojson"
SUB_NAME_FIELD = "schn_nama"

# Mirrors MINAHAK_SUBS in app.jsx — used only as a fallback when the sub layer
# matched but no minahak polygon did (the two layers don't share edges exactly).
SUB_TO_MINAHAK = {
    "א.ת. תלפיות": "א.ת. תלפיות",
    "איתרי": "א.ת. תלפיות",
    "תלפיות - תעשייה ומסחר": "א.ת. תלפיות",
    "בית צפאפא": "בית צפאפא",
    "בית צפאפא,שרפת": "בית צפאפא",
    "טנטור": "בית צפאפא",
    "תלפיות ארנונה": "בקעה רבתי",
    "תלפיות": "בקעה רבתי",
    "ארנונה": "בקעה רבתי",
    "בקעה": "בקעה רבתי",
    "גבעת חנניה - אבו תור": "בקעה רבתי",
    "מקור חיים": "בקעה רבתי",
    "מתחם הרכבת": "בקעה רבתי",
    "צפון תלפיות": "בקעה רבתי",
    "שיכוני תלפיות": "בקעה רבתי",
    "גבעת המטוס": "גבעת המטוס",
    "גוננים": "גוננים",
    "גוננים א-ו": "גוננים",
    "פת": "גוננים",
    "קטמונים": "גוננים",
    "קטמונים ח-ט": "גוננים",
    "רסקו": "גוננים",
    "רסקו - גבעת הורדים": "גוננים",
    "המושבה הגרמנית": "גינות העיר",
    "עמק רפאים - המושבה הגרמנית": "גינות העיר",
    "המושבה היוונית": "גינות העיר",
    "ניות": "גינות העיר",
    "טלביה": "גינות העיר",
    "קוממיות - טלביה": "גינות העיר",
    "קטמון הישנה": "גינות העיר",
    "קרית שמואל": "גינות העיר",
    "קריית שמואל": "גינות העיר",
    "רחביה": "גינות העיר",
    "מרכז ספורט מנחת - מלחה": "מינהל מוסדי מלחה",
    "גבעת השקד": "מינהל מוסדי מלחה",
}

_CACHE = {}


def _load_polys():
    """[(canonical_name, shapely_geom)] for minahak, and the same for subs."""
    if _CACHE:
        return _CACHE["minahak"], _CACHE["subs"]
    from shapely.geometry import shape
    from shapely.validation import make_valid

    def _prep(geom):
        g = shape(geom)
        if not g.is_valid:
            g = make_valid(g)
        return g

    minahaks = []
    for fname, canonical in MINAHAK_LAYERS:
        path = _data_file(fname)
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8") as f:
            for feat in json.load(f).get("features", []):
                if feat.get("geometry"):
                    minahaks.append((canonical, _prep(feat["geometry"])))

    subs = []
    sub_path = _data_file(SUB_LAYER)
    if os.path.exists(sub_path):
        with open(sub_path, encoding="utf-8") as f:
            for feat in json.load(f).get("features", []):
                name = (feat.get("properties", {}).get(SUB_NAME_FIELD) or "").strip()
                if name and feat.get("geometry"):
                    subs.append((name, _prep(feat["geometry"])))

    _CACHE["minahak"], _CACHE["subs"] = minahaks, subs
    return minahaks, subs


def _best_overlap(plan_geom, candidates):
    """Name of the candidate polygon holding the largest share of plan_geom."""
    best_name, best_area = "", 0.0
    for name, poly in candidates:
        try:
            if not plan_geom.intersects(poly):
                continue
            area = plan_geom.intersection(poly).area
        except Exception:
            continue
        if area > best_area:
            best_name, best_area = name, area
    if best_name:
        return best_name
    # Zero-area plan (degenerate ring) — fall back to a point test.
    try:
        pt = plan_geom.representative_point()
        for name, poly in candidates:
            if poly.contains(pt):
                return name
    except Exception:
        pass
    return ""


def derive(geometry):
    """(minahak, sub_neighborhood) for a WGS84 GeoJSON geometry dict.

    Returns ('', '') rather than raising when the geometry is unusable, the
    boundary layers are missing, or the plan falls outside every minahak
    (plans on the district edge — e.g. גבעת המטוס, which has no polygon layer).
    """
    if not geometry:
        return "", ""
    try:
        from shapely.geometry import shape
        from shapely.validation import make_valid

        g = shape(geometry)
        if not g.is_valid:
            g = make_valid(g)
        minahaks, subs = _load_polys()
    except Exception:
        return "", ""

    sub = _best_overlap(g, subs)
    # Sub-neighborhood first: app.jsx already treats SUB_TO_MINAHAK as canonical
    # and reattributes minahak from the sub at runtime, so deriving the same way
    # keeps stored and displayed values in agreement. Measured against the 1,023
    # curated plans in plans.geojson: sub-then-polygon 95.2%, polygon alone 92.8%.
    minahak = SUB_TO_MINAHAK.get(sub, "") if sub else ""
    if not minahak:
        minahak = _best_overlap(g, minahaks)
    return minahak, sub


def derive_from_features(features):
    """Same as derive(), for a list of GeoJSON features (their union)."""
    if not features:
        return "", ""
    try:
        from shapely.geometry import shape
        from shapely.ops import unary_union

        geoms = [shape(f["geometry"]) for f in features if f.get("geometry")]
        if not geoms:
            return "", ""
        u = unary_union([g if g.is_valid else g.buffer(0) for g in geoms])
        from shapely.geometry import mapping
        return derive(mapping(u))
    except Exception:
        return "", ""
