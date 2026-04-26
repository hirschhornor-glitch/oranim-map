"""
fetch_buildings.py — Download Jerusalem municipality buildings layer (NUM_APTS_C populated)
into oranim-app/data/buildings.geojson as a FeatureCollection of Point centroids.

Source: jergisng BaseLayers/MapServer/370 ("מבנים 2022")
Filtered to district_oranim bbox + buffer to keep file size reasonable.
"""
import json
import urllib.parse
import urllib.request
import sys

ARCGIS_BASE = "https://gisviewer.jerusalem.muni.il/arcgis/rest/services/BaseLayers/MapServer/370/query"
DISTRICT_GEOJSON = r"C:\ORANIM\oranim-app\data\district_oranim.geojson"
OUTPUT_FILE = r"C:\ORANIM\oranim-app\data\buildings.geojson"
BUFFER_DEG = 0.002  # ~200m

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def district_bbox():
    d = json.load(open(DISTRICT_GEOJSON, encoding="utf-8"))
    xs, ys = [], []
    def walk(c):
        if isinstance(c[0], (int, float)):
            xs.append(c[0]); ys.append(c[1])
        else:
            for sub in c:
                walk(sub)
    for f in d["features"]:
        g = f.get("geometry") or {}
        if g.get("coordinates"):
            walk(g["coordinates"])
    return min(xs)-BUFFER_DEG, min(ys)-BUFFER_DEG, max(xs)+BUFFER_DEG, max(ys)+BUFFER_DEG


def fetch_page(offset, xmin, ymin, xmax, ymax):
    geom = {"xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax,
            "spatialReference": {"wkid": 4326}}
    params = {
        "where": "NUM_APTS_C>0",
        "geometry": json.dumps(geom),
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "BLDG_NUM_1,BldNum_1,StreetName,StreetNa_1,NUM_FLOORS,NUM_APTS_C,BLDG_TYPE_,semel_bait,NUM_ENTR_1",
        "outSR": "4326",
        "returnGeometry": "true",
        "f": "json",
        "resultRecordCount": "1000",
        "resultOffset": str(offset),
    }
    url = ARCGIS_BASE + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


def ring_centroid(rings):
    """Average of vertices across all rings (no hole subtraction). Good enough for small building polygons."""
    xs = ys = 0.0
    n = 0
    for ring in rings:
        for (x, y) in ring[:-1] if len(ring) > 1 and ring[0] == ring[-1] else ring:
            xs += x; ys += y; n += 1
    if n == 0:
        return None
    return [xs/n, ys/n]


def main():
    xmin, ymin, xmax, ymax = district_bbox()
    print(f"district bbox: lon {xmin:.4f}..{xmax:.4f}, lat {ymin:.4f}..{ymax:.4f}")

    features = []
    offset = 0
    while True:
        data = fetch_page(offset, xmin, ymin, xmax, ymax)
        feats = data.get("features", [])
        if not feats:
            break
        for f in feats:
            attrs = f.get("attributes") or {}
            geom = f.get("geometry") or {}
            rings = geom.get("rings") or []
            c = ring_centroid(rings)
            if not c:
                continue
            street = attrs.get("StreetName") or ""
            house = attrs.get("BldNum_1") or attrs.get("BLDG_NUM_1") or ""
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [round(c[0], 6), round(c[1], 6)]},
                "properties": {
                    "units": int(attrs.get("NUM_APTS_C") or 0),
                    "floors": int(attrs.get("NUM_FLOORS") or 0) if attrs.get("NUM_FLOORS") is not None else None,
                    "entrances": int(attrs.get("NUM_ENTR_1") or 0) if attrs.get("NUM_ENTR_1") is not None else None,
                    "street": (street or "").strip(),
                    "house_num": str(house).strip() if house != "" else "",
                    "use_type": (attrs.get("BLDG_TYPE_") or "").strip(),
                    "semel_bait": (attrs.get("semel_bait") or "").strip(),
                },
            })
        print(f"  page offset={offset}: +{len(feats)} (total {len(features)})")
        offset += len(feats)
        if len(feats) == 0:
            break

    fc = {"type": "FeatureCollection", "features": features}
    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        json.dump(fc, fh, ensure_ascii=False, separators=(",", ":"))
    print(f"\nwrote {len(features)} buildings to {OUTPUT_FILE}")
    if features:
        total_units = sum(f["properties"]["units"] for f in features)
        print(f"sum of units: {total_units}")
        print("sample:")
        for f in features[:5]:
            p = f["properties"]
            print(f"  {p['street']} {p['house_num']} | units={p['units']} floors={p['floors']}")


if __name__ == "__main__":
    main()
