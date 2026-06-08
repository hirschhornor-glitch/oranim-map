# -*- coding: utf-8 -*-
"""
build_parcel_centroids.py — one-time / occasional index builder.

Fetches all cadastral parcels (חלקות) in the Oranim district bbox from the
Jerusalem ArcGIS service and writes a COMPACT lookup of centroids:

  data/parcel_centroids.json = {
    "meta":  { "bbox": [...], "parcels": N, "gushim": M },
    "gush_helka": { "30006/187": [lng, lat], ... },   # per-parcel centroid
    "gush":       { "30006": [lng, lat], ... }         # gush-level fallback
  }

Used by fetch_tree_permits.py to give a precise point to tree-cutting permits
whose Meirim geometry is just a degenerate/default point. ArcGIS is queried
only here (bbox queries, no quoted WHERE → WAF-safe); the daily scraper reads
the committed JSON, so it never hits ArcGIS itself.

Re-run only when the project area changes or the cadastre is updated.
"""
import json, os, sys, io, time, urllib.parse, urllib.request

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ARCGIS_URL = "https://gisviewer.jerusalem.muni.il/arcgis/rest/services/BaseLayers/MapServer/46/query"
HERE = os.path.dirname(os.path.abspath(__file__))


def _find_data_dir():
    for d in [os.path.join(HERE, "oranim-app", "data"), os.path.join(HERE, "data"), os.path.join(HERE, "..", "data")]:
        if os.path.isfile(os.path.join(d, "district_oranim.geojson")):
            return os.path.abspath(d)
    raise SystemExit("could not locate data/ dir near " + HERE)


DATA_DIR = _find_data_dir()
DISTRICT = os.path.join(DATA_DIR, "district_oranim.geojson")
OUT = os.path.join(DATA_DIR, "parcel_centroids.json")

from shapely.geometry import shape, Polygon, MultiPolygon
from shapely.ops import unary_union


def district_bbox(margin=0.002):
    gj = json.load(open(DISTRICT, encoding="utf-8"))
    feats = gj.get("features", [gj])
    geom = unary_union([shape(f["geometry"]) for f in feats if f.get("geometry")])
    x0, y0, x1, y1 = geom.bounds
    return x0 - margin, y0 - margin, x1 + margin, y1 + margin


def fetch_page(bbox, offset, batch=1000):
    xmin, ymin, xmax, ymax = bbox
    geom = json.dumps({"xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax,
                       "spatialReference": {"wkid": 4326}})
    params = {
        "where": "1=1", "outFields": "GUSH_NO,HELKA_SHOW", "geometry": geom,
        "geometryType": "esriGeometryEnvelope", "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects", "returnGeometry": "true",
        "outSR": "4326", "f": "json",
        "resultOffset": str(offset), "resultRecordCount": str(batch),
    }
    req = urllib.request.Request(ARCGIS_URL, data=urllib.parse.urlencode(params).encode(),
                                 headers={"User-Agent": "oranim-projector/1.0"})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=90) as r:
                return json.loads(r.read())
        except Exception as e:
            print(f"  offset {offset} retry {attempt+1}: {e}")
            time.sleep(5 * (attempt + 1))
    raise SystemExit(f"failed at offset {offset}")


def esri_polygon(geom):
    rings = (geom or {}).get("rings") or []
    if not rings:
        return None
    polys = [Polygon(r) for r in rings if len(r) >= 4]
    polys = [p for p in polys if p.is_valid or p.buffer(0).is_valid]
    if not polys:
        return None
    return polys[0] if len(polys) == 1 else MultiPolygon([p if isinstance(p, Polygon) else p for p in polys]).buffer(0)


def main():
    bbox = district_bbox()
    print(f"bbox {tuple(round(b,5) for b in bbox)}")
    gush_helka = {}
    gush_polys = {}
    offset = 0
    total = 0
    while True:
        page = fetch_page(bbox, offset)
        feats = page.get("features", [])
        if not feats:
            break
        for f in feats:
            a = f.get("attributes", {})
            g = str(a.get("GUSH_NO") or "").strip()
            h = str(a.get("HELKA_SHOW") or "").strip()
            poly = esri_polygon(f.get("geometry"))
            if not g or poly is None or poly.is_empty:
                continue
            c = poly.centroid
            if h and h != "0":
                gush_helka.setdefault(f"{g}/{h}", [round(c.x, 7), round(c.y, 7)])
            gush_polys.setdefault(g, []).append(poly)
            total += 1
        print(f"  offset={offset}: +{len(feats)} (total {total})")
        offset += len(feats)
        if offset > 80000:
            print("  safety break")
            break
        time.sleep(0.3)

    gush = {}
    for g, polys in gush_polys.items():
        try:
            c = unary_union(polys).centroid
            gush[g] = [round(c.x, 7), round(c.y, 7)]
        except Exception:
            c = polys[0].centroid
            gush[g] = [round(c.x, 7), round(c.y, 7)]

    out = {"meta": {"bbox": [round(b, 6) for b in bbox], "parcels": total, "gushim": len(gush)},
           "gush_helka": gush_helka, "gush": gush}
    with open(OUT, "w", encoding="utf-8") as fp:
        json.dump(out, fp, ensure_ascii=False, separators=(",", ":"))
    print(f"parcels={total} gushim={len(gush)} gush_helka_keys={len(gush_helka)}")
    print(f"wrote {OUT} ({os.path.getsize(OUT):,} bytes)")


if __name__ == "__main__":
    main()
