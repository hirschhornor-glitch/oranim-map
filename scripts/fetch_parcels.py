"""Fetch parcels (חלקות) layer from Jerusalem ArcGIS within the minahak bbox.

Outputs `data/parcels_gonenim.geojson` with each parcel having:
  - GUSH_NO (gush number)
  - HELKA_SHOW (helka number)
  - LEGAL_AREA (legal area)
  - geometry (Polygon)

Used by build_projector_gonenim.py to position projector recommendations whose
`מזהה מיקום` field contains a "helka(s)/gush" reference.
"""
from __future__ import annotations
import json
import urllib.parse
import urllib.request
import sys
from pathlib import Path

DATA_DIR = Path(r"C:\ORANIM\oranim-app\data")
MIRROR_DIR = Path(r"C:\ORANIM\oranim-map\data")
OUT_NAME = "parcels_gonenim.geojson"

ARCGIS_URL = "https://gisviewer.jerusalem.muni.il/arcgis/rest/services/BaseLayers/MapServer/46/query"

# Bbox covering the minahak (loose bounds with margin)
BBOX = "35.18,31.74,35.22,31.78"  # lon_min, lat_min, lon_max, lat_max


def fetch_page(offset: int, batch_size: int = 1000) -> dict:
    """ArcGIS query — POST with f=json (esri native format)."""
    geom = json.dumps({
        "xmin": 35.18, "ymin": 31.74,
        "xmax": 35.22, "ymax": 31.78,
        "spatialReference": {"wkid": 4326},
    })
    params = {
        "where": "1=1",
        "outFields": "GUSH_NO,HELKA_SHOW,LEGAL_AREA",
        "geometry": geom,
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "returnGeometry": "true",
        "outSR": "4326",
        "f": "json",
        "resultOffset": str(offset),
        "resultRecordCount": str(batch_size),
    }
    req = urllib.request.Request(
        ARCGIS_URL,
        data=urllib.parse.urlencode(params).encode(),
        headers={"User-Agent": "oranim-projector/1.0"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def esri_to_geojson_feature(esri_feat: dict) -> dict:
    """Convert an Esri feature (with rings) to a GeoJSON Feature (Polygon)."""
    attrs = esri_feat.get("attributes", {})
    geom = esri_feat.get("geometry") or {}
    rings = geom.get("rings", [])
    if not rings:
        return None
    # First ring is outer; others are holes (Esri convention is opposite of GeoJSON
    # winding rules; we ignore that for our display purposes).
    if len(rings) == 1:
        geojson_geom = {"type": "Polygon", "coordinates": rings}
    else:
        # Treat each ring as separate Polygon (MultiPolygon) for simplicity.
        geojson_geom = {"type": "MultiPolygon", "coordinates": [[r] for r in rings]}
    return {"type": "Feature", "properties": attrs, "geometry": geojson_geom}


def main():
    print(f"Fetching parcels in bbox {BBOX} …")
    all_features = []
    offset = 0
    while True:
        page = fetch_page(offset)
        esri_feats = page.get("features", [])
        if not esri_feats:
            break
        # Convert Esri → GeoJSON Feature
        for ef in esri_feats:
            gf = esri_to_geojson_feature(ef)
            if gf:
                all_features.append(gf)
        # The jergisng server caps at ~999/page but does NOT reliably set exceededTransferLimit.
        # Keep paging until we get a TRULY empty page.
        got = len(esri_feats)
        print(f"  offset={offset}: +{got}  (total: {len(all_features)})")
        if got == 0:
            break
        offset += got
        if offset > 50000:
            print("  Safety break")
            break

    print(f"\nTotal parcels: {len(all_features)}")
    gushim = {}
    for f in all_features:
        g = f["properties"].get("GUSH_NO")
        gushim[g] = gushim.get(g, 0) + 1
    print(f"Distinct gushim: {len(gushim)}")
    if gushim:
        top = sorted(gushim.items(), key=lambda x: -x[1])[:5]
        print(f"Top 5: {top}")

    output = {"type": "FeatureCollection", "features": all_features}
    for d in (DATA_DIR, MIRROR_DIR):
        out_path = d / OUT_NAME
        out_path.write_text(json.dumps(output, ensure_ascii=False), encoding="utf-8")
        print(f"Wrote {out_path} ({out_path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
