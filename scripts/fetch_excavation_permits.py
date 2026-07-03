# -*- coding: utf-8 -*-
"""
fetch_excavation_permits.py — היתרי חפירה בתוקף (שכבת "עבודות בשטח").

Source: Jerusalem muni ArcGIS, BaseLayers/MapServer/232 ("היתרי חפירה בתוקף")
— live polygons of active excavation / road-use permits, incl. יזם, מהות
ומטרת העבודה, קבלן, טווח תוקף ושעות עבודה. WAF-safe access pattern: bbox
envelope query with where=1=1 (no quoted WHERE — see build_parcel_centroids).

What it does:
  1. Pages the layer inside the Oranim bbox (WGS84 out).
  2. Keeps polygons intersecting data/district_oranim.geojson.
  3. Writes data/excavation_permits.json — {meta, permits:[...]}, geometry as
     GeoJSON Polygon rings (6dp — display layer, not precision-critical).

Re-run periodically (permits expire in weeks-months). Idempotent.
"""
import json, sys, io, os, re, datetime, urllib.parse, urllib.request

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

HERE = os.path.dirname(os.path.abspath(__file__))
LAYER = ("https://gisviewer.jerusalem.muni.il/arcgis/rest/services/"
         "BaseLayers/MapServer/232/query")
BBOX = "35.134465,31.718685,35.234807,31.781668"   # Oranim district bbox
PAGE = 500


def _data_dir():
    for d in [os.path.join(HERE, "data"), os.path.join(HERE, "..", "data")]:
        if os.path.isdir(d):
            return d
    raise SystemExit("data dir not found")


def _get(params):
    url = LAYER + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_features():
    feats, offset = [], 0
    while True:
        d = _get({"f": "json", "where": "1=1", "geometry": BBOX,
                  "geometryType": "esriGeometryEnvelope", "inSR": "4326",
                  "spatialRel": "esriSpatialRelIntersects", "outFields": "*",
                  "outSR": "4326", "resultOffset": offset,
                  "resultRecordCount": PAGE})
        page = d.get("features", [])
        feats.extend(page)
        if len(page) < PAGE and not d.get("exceededTransferLimit"):
            break
        offset += len(page)
        if not page:
            break
    return feats


def parse_tokef(s):
    """'10/03/2026-10/07/2026' → (iso_from, iso_to)."""
    m = re.match(r"\s*(\d{2})/(\d{2})/(\d{4})\s*-\s*(\d{2})/(\d{2})/(\d{4})",
                 str(s or ""))
    if not m:
        return None, None
    d1, m1, y1, d2, m2, y2 = m.groups()
    return f"{y1}-{m1}-{d1}", f"{y2}-{m2}-{d2}"


def main():
    from shapely.geometry import shape, Polygon
    from shapely.ops import unary_union

    data_dir = _data_dir()
    district = unary_union([shape(f["geometry"]) for f in _load_json(
        os.path.join(data_dir, "district_oranim.geojson"))["features"]])

    feats = fetch_features()
    print(f"fetched {len(feats)} features in bbox")
    out = []
    for f in feats:
        a = f.get("attributes", {})
        rings = (f.get("geometry") or {}).get("rings") or []
        if not rings:
            continue
        rings = [[[round(x, 6), round(y, 6)] for x, y in ring]
                 for ring in rings]
        try:
            geom = unary_union([Polygon(r) for r in rings if len(r) >= 4])
            if not geom.is_valid:
                geom = geom.buffer(0)
            if not geom.intersects(district):
                continue
        except Exception:
            continue
        t_from, t_to = parse_tokef(a.get("taarihTokef"))
        out.append({
            "tik": a.get("TikID"),
            "yazam": a.get("shemYazam"),
            "mahut": a.get("mahutAvoda"),
            "matara": a.get("matratAvoda"),
            "kablan": a.get("kablanName"),
            "status": a.get("statusTeur"),
            "tokef_from": t_from, "tokef_to": t_to,
            "hours_day": a.get("shaaYom"), "hours_night": a.get("shaaLaila"),
            "geometry": {"type": "Polygon", "coordinates": rings},
        })
    # Sanity floor: 479 in bbox on 2026-07-03 — near-empty means the layer or
    # the query broke; abort rather than blank the map layer.
    if len(out) < 30:
        raise SystemExit(f"only {len(out)} permits after clip — aborting")
    result = {"meta": {"fetched_at": datetime.date.today().isoformat(),
                       "count": len(out),
                       "source": "gisviewer BaseLayers/232"},
              "permits": out}
    dest = os.path.join(data_dir, "excavation_permits.json")
    # Same content → skip the write so the weekly cron doesn't commit a
    # meta-date-only diff.
    if os.path.exists(dest):
        try:
            if _load_json(dest).get("permits") == out:
                print("no content change — keeping existing file")
                return
        except Exception:
            pass
    with open(dest, "w", encoding="utf-8") as fh:
        json.dump(result, fh, ensure_ascii=False, separators=(",", ":"))
    print(f"wrote {len(out)} excavation permits -> {dest}")


def _load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


if __name__ == "__main__":
    main()
