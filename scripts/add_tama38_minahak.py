"""
add_tama38_minahak.py — post-process tama38_developers.json: for every enriched tik,
add `minahak` (point-in-polygon of the tama38 marker against the 6 minahak_*.geojson
layers) and `units_tose` (from the geojson feature) so the developers report can group
TAMA 38 rows by minahak + units without any in-app geometry work. Offline, instant.
"""
import json, sys
from pathlib import Path

DATA = Path(r"C:\ORANIM\oranim-app\data")
DEVS = DATA / "tama38_developers.json"
GEO = DATA / "tama38.geojson"
MINAHAK_FILES = {
    "minahak_baka.geojson": "בקעה רבתי",
    "minahak_beit_tzfafa.geojson": "בית צפאפא",
    "minahak_ganot.geojson": "גינות העיר",
    "minahak_gonen.geojson": "גוננים",
    "minahak_malha.geojson": "מינהל מוסדי מלחה",
    "minahak_talpiot.geojson": "א.ת. תלפיות",
}


def rings_of(geom):
    if not geom:
        return []
    if geom["type"] == "Polygon":
        return [geom["coordinates"]]
    if geom["type"] == "MultiPolygon":
        return geom["coordinates"]
    return []


def in_poly(lon, lat, polys):
    for poly in polys:
        inside = False
        for ring in poly:
            n = len(ring); j = n - 1
            for i in range(n):
                xi, yi = ring[i][0], ring[i][1]
                xj, yj = ring[j][0], ring[j][1]
                if ((yi > lat) != (yj > lat)) and (
                    lon < (xj - xi) * (lat - yi) / ((yj - yi) or 1e-12) + xi):
                    inside = not inside
                j = i
        if inside:
            return True
    return False


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    minahaks = []
    for fn, name in MINAHAK_FILES.items():
        gj = json.load(open(DATA / fn, encoding="utf-8"))
        polys = [r for f in gj["features"] for r in rings_of(f.get("geometry"))]
        minahaks.append((name, polys))

    geo = json.load(open(GEO, encoding="utf-8"))
    coord_by_tik, tose_by_tik = {}, {}
    for f in geo["features"]:
        tik = f["properties"].get("tik")
        g = f.get("geometry")
        if tik and g and g.get("coordinates"):
            c = g["coordinates"][0] if g["type"] == "MultiPoint" else g["coordinates"]
            coord_by_tik[tik] = c
            tose_by_tik[tik] = f["properties"].get("units_tose")

    doc = json.load(open(DEVS, encoding="utf-8"))
    by = doc["by_tik"]
    hit = 0
    for tik, rec in by.items():
        c = coord_by_tik.get(tik)
        rec["minahak"] = None
        if c:
            for name, polys in minahaks:
                if in_poly(c[0], c[1], polys):
                    rec["minahak"] = name; hit += 1; break
        # units: prefer parsed permit units, else feature units_tose
        if not rec.get("units"):
            t = tose_by_tik.get(tik)
            rec["units"] = int(t) if t not in (None, "", 0, 0.0) else None
    json.dump(doc, open(DEVS, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    from collections import Counter
    print("minahak resolved:", hit, "/", len(by))
    print(Counter(r.get("minahak") for r in by.values()))


if __name__ == "__main__":
    main()
