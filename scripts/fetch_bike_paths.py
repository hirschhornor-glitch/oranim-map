# -*- coding: utf-8 -*-
"""
fetch_bike_paths.py — שבילי אופניים בירושלים (עיריית ירושלים / צת"ל)

מקור: שכבת הסטטוס העדכנית של רשת שבילי האופניים, ArcGIS Online (services3),
מתוך היישום "סטטוס שבילי אופניים בירושלים"
(webappviewer id fca8964e1f9c4070bcdcdb862ad368c9 → webmap eeaecbe0e74f41d88c8ad5bcc39ad879).

מושך את כל קטעי הפוליליין, מסנן לאלה שחותכים את גבול הרובע (district_oranim)
עם חיץ קטן, ומצמצם לשדות הרלוונטיים. כותב data/bike_paths.geojson דחוס (שורה אחת).

מיועד לריצה רבעונית (ראה .github/workflows/update_bike_paths.yml). ה-host הוא
services3.arcgis.com (ענן Esri) — נגיש גם מ-GitHub Actions, בניגוד ל-gisviewer
העירוני החסום ל-IP של דאטה-סנטרים ([[project_muni_gis_local_fetch]]).
"""
import json, os, sys, io, time, urllib.request, urllib.parse

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

SERVICE = ("https://services3.arcgis.com/jeqc1A7OfE9m4EPO/arcgis/rest/"
           "services/bicyle_ZATAL_28042026/FeatureServer/0")
# חיץ סביב גבול הרובע, במעלות (~0.0016° ≈ ~150 מ') — כדי לתפוס קטעים גובלים/נכנסים.
BUFFER_DEG = 0.0016


def _find_data_dir():
    # פותר את data/ גם לוקלית (scripts/) וגם ב-CI (סקריפט בשורש). לא לקודד C:\ קשיח.
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(here, "..", "data"),
        os.path.join(here, "data"),
        os.path.join(here, "oranim-app", "data"),
        r"C:\ORANIM\oranim-app\data",
    ]
    for d in candidates:
        if os.path.isfile(os.path.join(d, "plans.geojson")):
            return os.path.abspath(d)
    raise SystemExit("could not locate data/ dir (plans.geojson) near " + here)


DATA_DIR = _find_data_dir()
DISTRICT = os.path.join(DATA_DIR, "district_oranim.geojson")
OUT = os.path.join(DATA_DIR, "bike_paths.geojson")

# שדות לשמירה (מהמקור: name, type, length, status). ממפים לשמות ברורים.
FIELD_MAP = {"name": "name", "type": "type", "status": "status", "length": "length"}


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "oranim-bike/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_all():
    """כל הפיצ'רים כ-GeoJSON ב-WGS84, בדפים של 1000."""
    feats, offset, page = [], 0, 1000
    while True:
        q = urllib.parse.urlencode({
            "where": "1=1", "outFields": ",".join(FIELD_MAP.keys()),
            "returnGeometry": "true", "outSR": "4326", "f": "geojson",
            "resultOffset": offset, "resultRecordCount": page,
        })
        d = _get(SERVICE + "/query?" + q)
        batch = d.get("features", [])
        feats.extend(batch)
        print(f"  fetched {len(batch)} (total {len(feats)})")
        if len(batch) < page:
            break
        offset += page
        time.sleep(0.3)
    return feats


# ---- clipping: shapely if available, else pure-python bbox+segment fallback ----
def load_district_shape():
    d = json.load(open(DISTRICT, encoding="utf-8"))
    geoms = [f["geometry"] for f in d["features"]]
    return geoms


def bbox_of(geom):
    xs, ys = [], []

    def walk(c):
        if isinstance(c[0], (int, float)):
            xs.append(c[0]); ys.append(c[1])
        else:
            for x in c:
                walk(x)
    walk(geom["coordinates"])
    return min(xs), min(ys), max(xs), max(ys)


def main():
    print("Fetching bike paths from ArcGIS…")
    feats = fetch_all()
    if not feats:
        raise SystemExit("no features fetched — aborting (keeping existing file)")

    district_geoms = load_district_shape()
    try:
        from shapely.geometry import shape
        from shapely.ops import unary_union
        district = unary_union([shape(g) for g in district_geoms]).buffer(BUFFER_DEG)

        def keep(f):
            try:
                return shape(f["geometry"]).intersects(district)
            except Exception:
                return True
        print("Clipping with shapely (buffered district)…")
    except ImportError:
        # fallback: bbox של הרובע + חיץ; שומר קטע אם קדקוד כלשהו בתוך התיבה.
        minx, miny, maxx, maxy = 1e9, 1e9, -1e9, -1e9
        for g in district_geoms:
            a, b, c, d = bbox_of(g)
            minx, miny, maxx, maxy = min(minx, a), min(miny, b), max(maxx, c), max(maxy, d)
        minx -= BUFFER_DEG; miny -= BUFFER_DEG; maxx += BUFFER_DEG; maxy += BUFFER_DEG

        def _pts(coords):
            if isinstance(coords[0], (int, float)):
                yield coords
            else:
                for c in coords:
                    yield from _pts(c)

        def keep(f):
            for x, y in _pts(f["geometry"]["coordinates"]):
                if minx <= x <= maxx and miny <= y <= maxy:
                    return True
            return False
        print("Clipping with bbox fallback (shapely not installed)…")

    out_feats = []
    for f in feats:
        if not f.get("geometry") or not keep(f):
            continue
        src = f.get("properties", {})
        props = {}
        for k_src, k_dst in FIELD_MAP.items():
            v = src.get(k_src)
            if k_dst == "length" and isinstance(v, (int, float)):
                v = round(v, 1)
            props[k_dst] = v
        out_feats.append({"type": "Feature", "properties": props, "geometry": f["geometry"]})

    fc = {"type": "FeatureCollection", "features": out_feats}
    # דחוס (שורה אחת) — כמו plans.geojson; לא indent=2 ([[reference_ci_workflow_conventions]]).
    with open(OUT, "w", encoding="utf-8") as fh:
        json.dump(fc, fh, ensure_ascii=False, separators=(",", ":"))
    print(f"Wrote {len(out_feats)} / {len(feats)} features → {OUT}")


if __name__ == "__main__":
    main()
