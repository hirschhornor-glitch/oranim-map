"""
build_unassigned_from_govmap.py
-------------------------------
Rebuild oranim-app/data/unassigned_permits.geojson from AUTHORITATIVE govmap
coordinates (govmap_coords.json — geocoded via govmap's address search, validated
by street-token match + Jerusalem bbox; see session notes). Replaces the earlier
Nominatim geocoding, which missed streets OSM lacks (e.g. דונש בן לברט).

Filters: exclude permits whose YK שכונה is גילה (out of footprint); keep points
inside the 22 sub-neighborhood polygons (+250m buffer). Enriches with field-obs.
"""
import json
from shapely.geometry import shape, Point

API = r"C:\ORANIM\missing_permits_api.json"
COORDS = r"C:\ORANIM\govmap_coords.json"
SUBN = r"C:\ORANIM\oranim-app\data\sub_neighborhoods.geojson"
FOBS = r"C:\ORANIM\oranim-app\data\field_observations.json"
OUT = r"C:\ORANIM\oranim-app\data\unassigned_permits.geojson"
OUT_OF_SCOPE_YK = ("גילה",)  # קריית שמואל IS in scope
BUF = 0.0025  # ~250m to absorb address-point vs polygon-boundary error


def main():
    rows = {r["file_number"]: r for r in json.load(open(API, encoding="utf-8"))}
    coords = json.load(open(COORDS, encoding="utf-8"))
    fobs = json.load(open(FOBS, encoding="utf-8")).get("by_file", {})
    subn = json.load(open(SUBN, encoding="utf-8"))
    polys = [(f["properties"].get("schn_nama", ""), shape(f["geometry"]).buffer(0).buffer(BUF))
             for f in subn["features"] if f.get("geometry")]

    feats = []
    excl_gilo = outside = 0
    for fn, (lng, lat) in coords.items():
        r = rows.get(fn, {})
        hood = r.get("neighborhood_yk", "")
        if any(o in hood for o in OUT_OF_SCOPE_YK):
            excl_gilo += 1
            print(f"  ⊘ {fn} excluded (YK={hood})"); continue
        pt = Point(lng, lat)
        sub = next((nm for nm, poly in polys if poly.contains(pt)), None)
        if not sub:
            outside += 1
            print(f"  ○ {fn} outside footprint ({lng:.4f},{lat:.4f})"); continue
        fo = fobs.get(fn, {})
        feats.append({"type": "Feature",
                      "geometry": {"type": "Point", "coordinates": [lng, lat]},
                      "properties": {
                          "file_number": fn, "status": r.get("status", ""), "status_date": r.get("status_date", ""),
                          "request_type": r.get("request_type", ""), "request_description": r.get("request_description", ""),
                          "address": r.get("address", ""), "sub_neighborhood": sub, "neighborhood_yk": hood,
                          "source": "unassigned (old plan, govmap-geocoded)",
                          "field_status": fo.get("field_status", ""), "visit_date": fo.get("visit_date", ""),
                          "eta_quarter": fo.get("eta_quarter", ""), "developer": fo.get("developer", ""),
                      }})
        print(f"  ✓ {fn} {r.get('address','')[:18]:18} -> {sub}")

    fc = {"type": "FeatureCollection",
          "source": "היתרים בשכונות אורנים שאינם משויכים לתב\"ע במאגר (תב\"עות ישנות, מיקום geocode מ-govmap)",
          "source_date": "2026-06-21", "features": feats}
    json.dump(fc, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"\n{len(feats)} written, {excl_gilo} excluded (גילה), {outside} outside footprint -> {OUT}")


if __name__ == "__main__":
    main()
