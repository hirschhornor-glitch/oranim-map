# -*- coding: utf-8 -*-
"""
fetch_permit_gis_points.py — נקודות גיאוקוד רשמיות לתיקי רישוי.

Source: Jerusalem muni ArcGIS, BaseLayers/MapServer/2110 ("רישוי ופיקוח
בניה") — a point per licensing file (tik_num, same '2004/7799.00' format as
our permit file_numbers). This is the city's own geocoding of permit files —
exact, unlike our Nominatim/govmap address fallbacks.

What it does:
  1. Pages all points inside the Oranim bbox (~21k).
  2. Keeps only tik_nums that appear in our permit datasets (all_permits /
     tama38 / extra / objections / unassigned) → slim lookup
     data/permit_gis_points.json  {tik: [lng, lat]}.
  3. Upgrades data/unassigned_permits.geojson in place: a feature whose
     file_number has an official point gets its coordinates replaced and
     position_source='muni_gis' (address-geocoded features keep 'geocode').

Re-run occasionally / after new permits are scraped. Idempotent.
"""
import json, sys, io, os, datetime, urllib.parse, urllib.request, collections

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

HERE = os.path.dirname(os.path.abspath(__file__))
LAYER = ("https://gisviewer.jerusalem.muni.il/arcgis/rest/services/"
         "BaseLayers/MapServer/2110/query")
BBOX = "35.134465,31.718685,35.234807,31.781668"
PAGE = 1000


def _data_dir():
    for d in [os.path.join(HERE, "data"), os.path.join(HERE, "..", "data")]:
        if os.path.isdir(d):
            return d
    raise SystemExit("data dir not found")


def _load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _get(params):
    url = LAYER + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_points():
    # NB: this 10.3 server honors resultOffset but returns odd-sized pages
    # (<resultRecordCount) WITHOUT exceededTransferLimit — the only reliable
    # stop condition is an empty page.
    pts, offset = collections.defaultdict(list), 0
    while True:
        d = _get({"f": "json", "where": "1=1", "geometry": BBOX,
                  "geometryType": "esriGeometryEnvelope", "inSR": "4326",
                  "spatialRel": "esriSpatialRelIntersects",
                  "outFields": "tik_num", "outSR": "4326",
                  "resultOffset": offset, "resultRecordCount": PAGE})
        page = d.get("features", [])
        if not page:
            break
        for f in page:
            tik = (f.get("attributes") or {}).get("tik_num")
            g = f.get("geometry") or {}
            if tik and "x" in g:
                pts[str(tik).strip()].append((g["x"], g["y"]))
        offset += len(page)
        print(f"  …{offset} points")
    return pts


def our_file_numbers(data_dir):
    tiks = set()
    ap = _load(os.path.join(data_dir, "all_permits.json"))
    for rec in ap.values():
        for p in rec.get("permits", []):
            fn = p.get("file_number")
            if fn:
                tiks.add(str(fn).strip())
    for name, extract in [
            ("tama38_permits.json", lambda rec: [rec] if isinstance(rec, dict) else rec),
            ("extra_permits.json", None),
            ("objections_permits.json", None)]:
        path = os.path.join(data_dir, name)
        if not os.path.exists(path):
            continue
        d = _load(path)

        def walk(o):
            if isinstance(o, dict):
                fn = o.get("file_number") or o.get("tik") or o.get("permit_number")
                if fn and isinstance(fn, str):
                    tiks.add(fn.strip())
                for v in o.values():
                    walk(v)
            elif isinstance(o, list):
                for v in o:
                    walk(v)
        walk(d)
    ug = os.path.join(data_dir, "unassigned_permits.geojson")
    if os.path.exists(ug):
        for f in _load(ug)["features"]:
            fn = f["properties"].get("file_number")
            if fn:
                tiks.add(str(fn).strip())
    return tiks


def main():
    data_dir = _data_dir()
    tiks = our_file_numbers(data_dir)
    print(f"our permit file numbers: {len(tiks)}")
    pts = fetch_points()
    print(f"GIS points fetched: {len(pts)} distinct tik_nums")
    # Sanity floor: ~21k points in the Oranim bbox on 2026-07-03.
    if len(pts) < 5000:
        raise SystemExit("suspiciously few GIS points — aborting")
    lookup = {}
    for tik, coords in pts.items():
        if tik not in tiks:
            continue
        lng = sum(c[0] for c in coords) / len(coords)
        lat = sum(c[1] for c in coords) / len(coords)
        lookup[tik] = [round(lng, 6), round(lat, 6)]
    result = {"meta": {"fetched_at": datetime.date.today().isoformat(),
                       "matched": len(lookup),
                       "source": "gisviewer BaseLayers/2110"},
              "points": lookup}
    dest = os.path.join(data_dir, "permit_gis_points.json")
    # Same content → skip the write (avoid meta-date-only cron commits).
    skip = False
    if os.path.exists(dest):
        try:
            skip = _load(dest).get("points") == lookup
        except Exception:
            pass
    if skip:
        print("no content change — keeping existing points file")
    else:
        with open(dest, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, separators=(",", ":"))
        print(f"wrote {len(lookup)} matched permit points -> {dest}")

    # Upgrade unassigned permits to the official coordinates
    ug = os.path.join(data_dir, "unassigned_permits.geojson")
    if os.path.exists(ug):
        d = _load(ug)
        n = 0
        for f in d["features"]:
            fn = str(f["properties"].get("file_number") or "").strip()
            if fn in lookup:
                f["geometry"] = {"type": "Point", "coordinates": lookup[fn]}
                f["properties"]["position_source"] = "muni_gis"
                n += 1
        if n:
            with open(ug, "w", encoding="utf-8") as fh:
                json.dump(d, fh, ensure_ascii=False, separators=(",", ":"))
        print(f"upgraded {n}/{len(d['features'])} unassigned permits to muni GIS points")


if __name__ == "__main__":
    main()
