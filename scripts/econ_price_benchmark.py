"""Benchmark an economic report's assumed sale price against actual new-build deals nearby.

For each plan:
  1. Plan polygon (plans.geojson) -> ITM -> bbox buffered by RADIUS_M.
  2. GovMap open WFS (Parcels_ITM) -> the gush/helka pairs whose parcel lies within
     RADIUS_M of the plan polygon (distance, not just bbox).
  3. over.org.il /api/deals/search per gush (paged) -> keep deals that are:
       in one of those parcels, an apartment, a whole unit (portion 1.0),
       new-build (year_built >= NEW_FROM), price_per_sqm_normalized in a sane range,
       and dated within WINDOW_M months before the report date.
  4. Median / IQR of price_per_sqm_normalized -> compare with the report's price.

Caveats printed with the result: tax-authority area is the registered area (usually
close to net / "פלדלת" but not identical), prices include VAT like the reports, and
parcels re-plotted after demolition may be missing from the current cadastre.

Usage: python econ_price_benchmark.py 101-1394444 101-1241785 ...   (writes JSON next to it)
"""
import glob, json, statistics, sys, time, urllib.error, urllib.parse, urllib.request
from datetime import datetime, timedelta
from pyproj import Transformer
from shapely.geometry import shape
from shapely.ops import transform

sys.stdout.reconfigure(encoding="utf-8")
PLANS = r"C:\ORANIM\oranim-app\data\plans.geojson"
SUBS = r"C:\ORANIM\oranim-app\data\sub_neighborhoods.geojson"
MIN_DEALS = 10   # below this in the sub-neighbourhood, widen (minahak, then radius)
LOW_N = 20       # fewer deals than this -> confidence 'low'
ECON = r"C:\ORANIM\oranim-app\data\economic_reports.json"
OUT = r"C:\ORANIM\plan_documents\econ_price_benchmark.json"
WFS = "https://open.govmap.gov.il/geoserver/opendata/wfs"
OVER = "https://www.over.org.il/api/deals/search"
RADIUS_M = 500
WINDOW_M = 24
NEW_FROM = 2018
PPM_RANGE = (15000, 120000)
UA = {"User-Agent": "oranim-econ-benchmark/1.0"}
to_itm = Transformer.from_crs(4326, 2039, always_xy=True).transform


def get_json(url, tries=6):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers=UA)
            return json.loads(urllib.request.urlopen(req, timeout=90).read().decode("utf-8-sig"))
        except urllib.error.HTTPError as e:
            if i == tries - 1:
                raise
            time.sleep(30 * (i + 1) if e.code == 429 else 3 * (i + 1))
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(3 * (i + 1))


_SUBS = None
def sub_area(geom_itm):
    """The sub-neighbourhood polygon (ITM) holding most of the plan. A radius alone
    crosses market boundaries: הרצוג 33-37 pulled in Talbiye/Rehavia luxury deals
    at 60-67K/m² against a Katamon plan (pilot 2026-10-13)."""
    global _SUBS
    if _SUBS is None:
        _SUBS = [(f["properties"].get("schn_nama"), transform(to_itm, shape(f["geometry"])).buffer(0))
                 for f in json.load(open(SUBS, encoding="utf-8"))["features"] if f.get("geometry")]
    best = max(_SUBS, key=lambda s: s[1].intersection(geom_itm).area, default=(None, None))
    return best if best[1] is not None and best[1].intersection(geom_itm).area > 0 else (None, None)


_MH = None
def minahak_area(geom_itm):
    """The minahak polygon (ITM) holding most of the plan — the fallback when the
    sub-neighbourhood has too few new-build deals (רסקו for הרצוג 33-37)."""
    global _MH
    if _MH is None:
        _MH = []
        for path in glob.glob(r"C:\ORANIM\oranim-app\data\minahak_*.geojson"):
            if "union" in path:
                continue
            for f in json.load(open(path, encoding="utf-8"))["features"]:
                if f.get("geometry"):
                    _MH.append((path.rsplit("minahak_", 1)[1][:-8], transform(to_itm, shape(f["geometry"])).buffer(0)))
    best = max(_MH, key=lambda m: m[1].intersection(geom_itm).area, default=(None, None))
    return best if best[1] is not None and best[1].intersection(geom_itm).area > 0 else (None, None)


def parcels_near(geom_itm):
    minx, miny, maxx, maxy = geom_itm.buffer(RADIUS_M).bounds
    out, start = {}, 0
    while True:
        q = dict(service="WFS", version="2.0.0", request="GetFeature", typeNames="opendata:Parcels_ITM",
                 outputFormat="application/json", srsName="EPSG:2039", count=1000, startIndex=start,
                 CQL_FILTER=f"BBOX(the_geom,{minx},{miny},{maxx},{maxy},'EPSG:2039')")
        d = get_json(WFS + "?" + urllib.parse.urlencode(q))
        feats = d.get("features") or []
        for f in feats:
            p = f["properties"]
            try:
                g = shape(f["geometry"])
                if g.distance(geom_itm) <= RADIUS_M:
                    out[(int(p["GUSH_NUM"]), int(p["PARCEL"]))] = g.representative_point()
            except Exception:
                pass
        start += len(feats)
        if len(feats) < 1000:
            break
    return out


def deals_for_gush(gush, since=None):
    rows, off = [], 0
    while True:
        q = dict(gush=gush, settlement="ירושלים", limit=200, offset=off, sort="date_desc")
        d = get_json(OVER + "?" + urllib.parse.urlencode(q))
        batch = d.get("deals") or d.get("results") or d.get("data") or []
        rows += batch
        off += len(batch)
        if len(batch) < 200 or off >= 6000:
            break
        # sorted newest first: once a page ends before the window, the rest is older
        if since and batch and (batch[-1].get("date") or "") < since.strftime("%Y-%m-%d"):
            break
        time.sleep(1.0)
    time.sleep(1.0)
    return rows


def parse_date(s):
    for fmt in ("%Y-%m-%d", "%Y-%m"):
        try:
            return datetime.strptime(s, fmt)
        except (TypeError, ValueError):
            pass
    return None


def benchmark(pl, feat, econ):
    geom = transform(to_itm, shape(feat["geometry"]))
    rep_date = parse_date(econ.get("date")) or datetime.now()
    since = rep_date - timedelta(days=30.4 * WINDOW_M)
    parcels = parcels_near(geom)
    sub_name, sub_geom = sub_area(geom)
    in_sub = {k for k, pt in parcels.items() if sub_geom is not None and sub_geom.contains(pt)}
    mh_name, mh_geom = minahak_area(geom)
    in_mh = {k for k, pt in parcels.items() if mh_geom is not None and mh_geom.contains(pt)}
    gushes = sorted({g for g, _ in parcels})
    kept, seen = [], set()
    for g in gushes:
        for r in deals_for_gush(g, since):
            try:
                key = (int(r.get("gush")), int(r.get("helka")))
            except (TypeError, ValueError):
                continue
            if key not in parcels:
                continue
            d = parse_date((r.get("date") or "")[:10])
            if not d or not (since <= d <= rep_date + timedelta(days=31)):
                continue
            if "דירה" not in (r.get("nature") or ""):
                continue
            try:
                if float(r.get("portion_fraction") or 0) < 0.999:
                    continue
                yb = int(r.get("year_built") or 0)
                ppm = float(r.get("price_per_sqm_normalized") or 0)
            except (TypeError, ValueError):
                continue
            if yb < NEW_FROM or not (PPM_RANGE[0] <= ppm <= PPM_RANGE[1]):
                continue
            uid = (key, r.get("sub_parcel"), r.get("date"), r.get("amount"))
            if uid in seen:
                continue
            seen.add(uid)
            kept.append({"date": r.get("date"), "ppm": round(ppm), "year_built": yb, "gush": key[0],
                         "helka": key[1], "area": r.get("area_sqm"), "rooms": r.get("rooms"),
                         "in_sub": key in in_sub, "in_mh": key in in_mh})
    rep_price = (econ.get("sale_price_sqm") or {}).get("value")
    res = {"plan": pl, "report_date": econ.get("date"), "report_price": rep_price,
           "report_price_unit": (econ.get("sale_price_sqm") or {}).get("unit"),
           "radius_m": RADIUS_M, "window_months": WINDOW_M, "parcels": len(parcels), "gushes": gushes,
           "n_deals": len(kept), "n_parcels_with_deals": len({(k['gush'], k['helka']) for k in kept})}
    sub_kept = [k for k in kept if k["in_sub"]]
    mh_kept = [k for k in kept if k["in_mh"]]
    res["sub_neighborhood"], res["minahak_layer"] = sub_name, mh_name
    res["n_deals_radius"] = len(kept)
    res["scope"] = "sub" if len(sub_kept) >= MIN_DEALS else "minahak" if len(mh_kept) >= MIN_DEALS else "radius"
    if res["scope"] != "radius":
        kept = sub_kept if res["scope"] == "sub" else mh_kept
        res["n_deals"] = len(kept)
        res["n_parcels_with_deals"] = len({(k['gush'], k['helka']) for k in kept})
    res["confidence"] = "low" if len(kept) < LOW_N or res["scope"] == "radius" else "ok"
    if kept:
        v = sorted(k["ppm"] for k in kept)
        q = statistics.quantiles(v, n=4) if len(v) >= 4 else [v[0], statistics.median(v), v[-1]]
        res.update(market_median=round(statistics.median(v)), market_q1=round(q[0]), market_q3=round(q[-1]))
        if rep_price:
            res["report_vs_market_pct"] = round((rep_price / res["market_median"] - 1) * 100, 1)
    res["deals"] = sorted(kept, key=lambda k: k["date"], reverse=True)[:60]
    return res


def main(argv):
    feats = {}
    for f in json.load(open(PLANS, encoding="utf-8"))["features"]:
        pn = f["properties"].get("plan_name")
        if pn and f.get("geometry") and pn not in feats:
            feats[pn] = f
    econ = json.load(open(ECON, encoding="utf-8"))["plans"]
    try:
        out = json.load(open(OUT, encoding="utf-8"))
    except Exception:
        out = {}
    for pl in argv:
        if pl not in feats or pl not in econ:
            print(f"{pl}: missing geometry or economic data"); continue
        r = benchmark(pl, feats[pl], econ[pl])
        out[pl] = r
        print(f"{pl} [{r['sub_neighborhood']} / {r['scope']}]: report {r['report_price']} | market median {r.get('market_median')} "
              f"(IQR {r.get('market_q1')}–{r.get('market_q3')}, n={r['n_deals']} of {r['n_deals_radius']} in {r['n_parcels_with_deals']} parcels; "
              f"{r['parcels']} parcels / {len(r['gushes'])} gushes in {RADIUS_M} m) -> {r.get('report_vs_market_pct')}%")
        with open(OUT, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=1)


if __name__ == "__main__":
    main(sys.argv[1:])
