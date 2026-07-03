# -*- coding: utf-8 -*-
"""
build_asset_allocations.py — "מה נבחר בסוף": ההקצאות בפועל על מבני ציבור.

When a plan permits a broad public envelope (חברה/קהילה/חינוך…), the actual
choice is only visible in ספר ההקצאות — which עמותה got the asset and for
what SPECIFIC use (the book's vocabulary is fine-grained: תיאטרון, תזמורת,
אולם ספורט vs מגרש ספורט, מרפאת שיניים…). This joins the muni books onto our
existing public-buildings layer:

  allocation rows → their asset's גוש/חלקה centroid (parcel_centroids) →
  point-in-polygon vs shavaz_kayam.geojson → keyed by feature fid.

Output: data/asset_allocations.json
  { meta, by_fid: { "<fid>": [ {org, use, active, approved, asset_name,
                                asset_use, asset_id}, … ] } }

Run after fetch_muni_books.py (weekly cron). Idempotent (skip-write).
"""
import json, sys, io, os, datetime, collections

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

HERE = os.path.dirname(os.path.abspath(__file__))


def _find(relparts):
    for base in [HERE, os.path.join(HERE, "..")]:
        p = os.path.join(base, *relparts)
        if os.path.exists(p):
            return p
    raise SystemExit("not found: " + "/".join(relparts))


def _load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _date(ms):
    if not ms:
        return None
    try:
        return datetime.datetime.fromtimestamp(
            ms / 1000, datetime.timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return None


def main():
    from shapely.geometry import shape, Point
    from shapely.strtree import STRtree

    prop = _load(_find(["sources", "muni_property_book.json"]))["rows"]
    alloc = _load(_find(["sources", "muni_allocation_book.json"]))["rows"]
    cent = _load(_find(["..", "data", "parcel_centroids.json"]))["gush_helka"]
    kayam = _load(_find(["..", "data", "shavaz_kayam.geojson"]))["features"]

    # asset id → centroid point (first parcel that resolves)
    asset_pt = {}
    asset_meta = {}
    for r in prop:
        aid = r["מספר נכס"]
        if aid not in asset_meta:
            asset_meta[aid] = {"name": r.get("שם נכס"),
                               "use": r.get("שימוש עיקרי")}
        if aid in asset_pt:
            continue
        g, h = r.get("גוש"), r.get("חלקה")
        if not g or not h:
            continue
        p = cent.get(f"{g}/{h}".replace(" ", ""))
        if p:
            asset_pt[aid] = Point(p)

    polys, fids = [], []
    for f in kayam:
        if not f.get("geometry"):
            continue
        try:
            geom = shape(f["geometry"])
            if not geom.is_valid:
                geom = geom.buffer(0)
        except Exception:
            continue
        polys.append(geom)
        fids.append(str(f["properties"].get("fid")))
    tree = STRtree(polys)

    by_fid = collections.defaultdict(list)
    n_alloc_placed = 0
    for a in alloc:
        aid = a["מספר נכס"]
        pt = asset_pt.get(aid)
        if pt is None:
            continue
        hits = tree.query(pt, predicate="intersects")
        if len(hits) == 0:
            continue
        n_alloc_placed += 1
        meta = asset_meta.get(aid, {})
        rec = {
            "asset_id": aid,
            "asset_name": meta.get("name"),
            "asset_use": meta.get("use"),
            "org": a.get("שם"),
            "use": a.get("שימוש"),
            "active": a.get("פעיל"),
            "approved": _date(a.get("אישור מועצה אחרון")),
        }
        for idx in hits:
            by_fid[fids[idx]].append(rec)

    # Sanity floor: 380 allocation-bearing assets sit in the Oranim bbox
    # (2026-07-03); a near-empty join means an input broke.
    if len(by_fid) < 20:
        raise SystemExit(f"suspiciously small join ({len(by_fid)} buildings)"
                         " — aborting without writing")
    result = {"meta": {"built_at": datetime.date.today().isoformat(),
                       "buildings": len(by_fid),
                       "allocations": n_alloc_placed},
              "by_fid": dict(sorted(by_fid.items()))}
    dest = os.path.join(os.path.dirname(
        _find(["..", "data", "shavaz_kayam.geojson"])),
        "asset_allocations.json")
    if os.path.exists(dest):
        try:
            if _load(dest).get("by_fid") == result["by_fid"]:
                print("no content change — keeping existing file")
                return
        except Exception:
            pass
    with open(dest, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, separators=(",", ":"))
    print(f"wrote {len(by_fid)} buildings / {n_alloc_placed} allocations"
          f" -> {dest}")


if __name__ == "__main__":
    main()
