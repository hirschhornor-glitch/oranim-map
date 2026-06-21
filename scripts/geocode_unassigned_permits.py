"""
geocode_unassigned_permits.py
-----------------------------
The permits with no plan match (scrape_missing_permits_api.py → matched_tabas==[])
are on old pre-Mavat plans with no polygon in our data. Instead of forcing them
onto a wrong plan, place each as a standalone POINT (geocoded from its address),
keeping only those that actually fall inside our footprint (the 22 sub-neighborhood
polygons — this drops גילה / קריית שמואל that the xlsx mis-grouped).

Enriches each point with its field-observation data (by file_number).
Output: oranim-app/data/unassigned_permits.geojson  (Point features)
"""
import json, sys, time
import requests
from shapely.geometry import shape, Point

API_SRC = r"C:\ORANIM\missing_permits_api.json"
SUBN = r"C:\ORANIM\oranim-app\data\sub_neighborhoods.geojson"
FOBS = r"C:\ORANIM\oranim-app\data\field_observations.json"
OUT = r"C:\ORANIM\oranim-app\data\unassigned_permits.geojson"
NOM = "https://nominatim.openstreetmap.org/search"
UA = "oranim-gis/1.0 (unassigned-permit mapping)"


# light normalization of street names that trip Nominatim
def variants(addr, hood):
    a = addr.replace('"', "").replace("'", "")
    a2 = a.replace("רמבן", 'רמב"ן').replace("רמב״ן", 'רמב"ן')
    qs = []
    if hood:
        qs += [f"{a}, {hood}, ירושלים", f"{a2}, {hood}, ירושלים"]
    qs += [f"{a}, ירושלים", f"{a2}, ירושלים"]
    if a.split():
        qs.append(f"{a.split()[0]}, {hood}, ירושלים" if hood else f"{a.split()[0]}, ירושלים")
    seen, out = set(), []
    for q in qs:
        if q not in seen:
            seen.add(q); out.append(q)
    return out


def geocode(addr, hood=""):
    for q in variants(addr, hood):
        try:
            r = requests.get(NOM, params={"format": "json", "q": q, "limit": 1,
                             "countrycodes": "il", "accept-language": "he"},
                             headers={"user-agent": UA}, timeout=20)
            j = r.json()
            if j:
                return float(j[0]["lat"]), float(j[0]["lon"]), j[0].get("type", "")
        except Exception:
            pass
        time.sleep(1.1)
    return None


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    rows = json.load(open(API_SRC, encoding="utf-8"))
    unm = [r for r in rows if r.get("status") and not r.get("matched_tabas")]
    fobs = json.load(open(FOBS, encoding="utf-8")).get("by_file", {})
    subn = json.load(open(SUBN, encoding="utf-8"))
    # ~250m buffer (deg) to absorb geocode error at boundaries (אגרון/אלפסי edge of רחביה)
    BUF = 0.0025
    polys = [(f["properties"].get("schn_nama", ""), shape(f["geometry"]).buffer(0).buffer(BUF))
             for f in subn["features"] if f.get("geometry")]

    feats = []
    inside = outside = nogeo = 0
    for r in unm:
        fn, addr = r["file_number"], r.get("address", "").strip()
        if not addr:
            nogeo += 1
            print(f"  ✗ {fn:15} (no address)"); continue
        g = geocode(addr, r.get("neighborhood_yk", ""))
        if not g:
            nogeo += 1
            print(f"  ✗ {fn:15} '{addr[:22]}' no geocode"); continue
        lat, lon, gtype = g
        pt = Point(lon, lat)
        hit = next((nm for nm, poly in polys if poly.contains(pt)), None)
        if not hit:
            outside += 1
            print(f"  ○ {fn:15} '{addr[:22]:22}' -> outside footprint ({r.get('neighborhood_yk','')})"); continue
        inside += 1
        fo = fobs.get(fn, {})
        props = {
            "file_number": fn, "status": r.get("status", ""), "status_date": r.get("status_date", ""),
            "request_type": r.get("request_type", ""), "request_description": r.get("request_description", ""),
            "address": addr, "sub_neighborhood": hit, "neighborhood_yk": r.get("neighborhood_yk", ""),
            "geocode_type": gtype, "source": "unassigned (old plan, geocoded)",
            "field_status": fo.get("field_status", ""), "visit_date": fo.get("visit_date", ""),
            "eta_quarter": fo.get("eta_quarter", ""), "developer": fo.get("developer", ""),
        }
        feats.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [lon, lat]}, "properties": props})
        print(f"  ✓ {fn:15} '{addr[:22]:22}' -> {hit} ({lat:.5f},{lon:.5f})")

    fc = {"type": "FeatureCollection", "source": "היתרים בשכונות אורנים שאינם משויכים לתב\"ע במאגר (תב\"עות ישנות)",
          "source_date": "2026-06-21", "features": feats}
    json.dump(fc, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"\n{inside} inside footprint (written), {outside} outside, {nogeo} ungeocodable. -> {OUT}")


if __name__ == "__main__":
    main()
