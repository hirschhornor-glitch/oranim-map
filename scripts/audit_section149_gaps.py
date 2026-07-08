"""
audit_section149_gaps.py
------------------------
Cross-checks the city's full §149 publications archive (every permit that ever
published הקלות citywide) against everything WE track, to surface permits that
"fell between the chairs" — especially TAMA 38 that cites only old local plan
numbers (no Mavat code) and isn't in the city's renewal GIS layer.

Root case that motivated this: 2023/0605.00 (תמ"א 38 פינוי-בינוי, טשרניחובסקי 48)
— present in the §149 archive the whole time, never cross-checked.

Pipeline:
  1. Load the §149 archive (fresh fetch of betokef=false, fallback to pub_false.json).
  2. Build the set of tiks we already track (all_permits + tama38 + objections + extra).
  3. Filter archive to the Oranim district (by YK neighborhood 'schn'), dedupe by tik.
  4. Subtract tracked -> missing candidates; keep file_year >= MIN_FILE_YEAR.
  5. For each candidate: YK proc 242700447 -> request_type/status/address.
     Classify; keep the תמ"א 38 ones.
  6. For each TAMA 38 hit: proc 242700456 -> gush/helka; parcels layer 46 -> centroid
     (ITM+WGS84) so the record is ready to append to tama38.geojson.

Output:
  section149_audit.json          — full结果 (candidates, classification, tama38 hits)
  permits_scan_reports/section149_audit_<date>.md — human report

CLI:
  py -X utf8 audit_section149_gaps.py [--min-file-year 2015] [--limit N] [--no-fetch]
"""
import argparse
import json
import re
import sys
import time
from datetime import date
from pathlib import Path

import requests

ROOT = Path(r"C:\ORANIM")
DATA = ROOT / "oranim-app" / "data"
ARCHIVE_CACHE = ROOT / "pub_false.json"
REPORT_DIR = ROOT / "permits_scan_reports"

YK_API = "https://jerbasicserviceapi.jerusalem.muni.il/api/Db/ExecuteGetJSON"
YK_SYS = "26400046"
S149_API = ("https://jergisrishuimessages.jerusalem.muni.il/Rishui149/api/publish/"
            "?fromDate=&toDate=&tik_num=&streetcode=&schncode=&betokef=false")
S149_REFERER = "https://jergisrishuimessages.jerusalem.muni.il/Rishui149/Pages/msg149.html"
PARCELS_URL = ("https://gisviewer.jerusalem.muni.il/arcgis/rest/services/"
               "BaseLayers/MapServer/46/query")
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

# District membership is decided GEOGRAPHICALLY (geocode the address -> point-in-
# district_oranim). The schn set below is only a FALLBACK for rows whose address
# can't be geocoded from buildings.geojson. It was validated per-neighborhood by
# in-district fraction (2026-07-08): נחלאות/מנחת/רמת דניה geocode 0% in-district
# (they're OUT — center/southwest, not rova-4) and were removed; פת/ניות/צפון
# תלפיות/ימין משה/טלביה/גונן ט geocode 100% in-district and were added. Trust the
# geometry over schn — e.g. tik 2017/0857.00 is tagged schn='רמת אשכול' but its
# real address is עמק רפאים 6 (in-district); PIP keeps it, schn alone would drop it.
ORANIM_SCHN = {
    'גוננים', 'קטמון הישנה', 'גאולים - בקעה', 'רחביה', 'תלפיות', 'בית צפפה',
    'גבעת הורדים', 'מושבה גרמנית', 'ארנונה', 'המושבה היוונית', 'מקור חיים',
    'קריית שמואל', 'טלביה/המוגרבים', 'טלביה', 'תלפיות תעשיה', 'צפון תלפיות',
    'פת', 'נוה שאנן/ניות', 'ימין משה', 'גונן ט',
}

SESSION = requests.Session()


def norm_tik(t):
    t = str(t or "").replace(" ", "")
    m = re.match(r'(\d{4})/(\d+)\.(\d+)', t)
    if m:
        return f"{m.group(1)}/{int(m.group(2))}.{int(m.group(3))}"
    m2 = re.match(r'(\d{4})/(\d+)', t)
    return f"{m2.group(1)}/{int(m2.group(2))}" if m2 else t


def file_year(t):
    m = re.match(r'(\d{4})/', t)
    return int(m.group(1)) if m else 0


def yk_call(proc, params, retries=5):
    """POST a stored-proc; empty list = throttle, retry with backoff."""
    headers = {"content-type": "application/json",
               "referer": "https://ykpubdata.jerusalem.muni.il/",
               "origin": "https://ykpubdata.jerusalem.muni.il",
               "user-agent": UA}
    last = []
    for attempt in range(retries):
        try:
            r = SESSION.post(YK_API, json={"ProcName": proc, "Cnn": "cnnGisYk",
                             "Parameters": params}, headers=headers, timeout=30)
            r.raise_for_status()
            data = r.json()
            if data:
                return data
            last = data
        except Exception:
            pass
        time.sleep(2 * (attempt + 1))
    return last


def load_archive(do_fetch):
    if do_fetch:
        try:
            print("Fetching fresh §149 archive (betokef=false)...", flush=True)
            r = SESSION.get(S149_API, headers={"User-Agent": UA, "Referer": S149_REFERER},
                            timeout=90)
            r.raise_for_status()
            rows = r.json()
            if isinstance(rows, list) and rows:
                ARCHIVE_CACHE.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
                print(f"  fetched {len(rows)} rows -> {ARCHIVE_CACHE.name}", flush=True)
                return rows
            print("  fetch returned empty; falling back to cache", flush=True)
        except Exception as e:
            print(f"  fetch failed ({e}); falling back to cache", flush=True)
    rows = json.load(open(ARCHIVE_CACHE, encoding="utf-8"))
    print(f"Loaded {len(rows)} archive rows from cache {ARCHIVE_CACHE.name}", flush=True)
    return rows


def build_tracked():
    tracked = set()
    for f in ["all_permits.json", "tama38_permits.json"]:
        p = DATA / f
        if not p.exists():
            p = ROOT / f
        j = json.load(open(p, encoding="utf-8"))
        for rec in j.values():
            for pm in rec.get("permits", []):
                tracked.add(norm_tik(pm.get("file_number")))
    for f in ["objections_permits.json", "extra_permits.json"]:
        p = DATA / f
        if not p.exists():
            continue
        s = p.read_text(encoding="utf-8")
        for m in re.findall(r'\b(20\d\d/\d{3,4}\.\d{1,2})\b', s):
            tracked.add(norm_tik(m))
    return tracked


def build_district_filter():
    """Return in_district(row) using geocode(street+house)->PIP(district_oranim),
    falling back to the ORANIM_SCHN allowlist when the address can't be geocoded."""
    dist = json.load(open(DATA / "district_oranim.geojson", encoding="utf-8"))
    polys = []
    for f in dist["features"]:
        g = f["geometry"]
        if g["type"] == "MultiPolygon":
            polys.extend(g["coordinates"])
        elif g["type"] == "Polygon":
            polys.append(g["coordinates"])

    def in_poly(lon, lat):
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

    def nkey(s):
        s = re.sub(r'^[א-ת]\.\s*', '', str(s or ''))
        s = s.replace('"', '').replace("'", '').replace('`', '')
        return re.sub(r'\s+', '', s)

    idx = {}
    for f in json.load(open(DATA / "buildings.geojson", encoding="utf-8"))["features"]:
        p = f["properties"]
        st = nkey(p.get("street")); hn = str(p.get("house_num") or "").strip()
        if st and hn:
            idx.setdefault((st, hn), f["geometry"]["coordinates"])

    def in_district(row):
        hn = str(row.get("misp_bait") or "").strip()
        st = nkey(row.get("rehov"))
        for cand in (st, st.lstrip('ה'), 'ה' + st):
            c = idx.get((cand, hn))
            if c:
                return in_poly(c[0], c[1])
        return row.get("schn") in ORANIM_SCHN     # ungeocodable -> schn fallback

    return in_district


ITM2WGS = None
def parcel_centroid(gush, helka):
    """Centroid (ITM + WGS84) of a parcel from BaseLayers/46. Returns None on miss."""
    global ITM2WGS
    if ITM2WGS is None:
        from pyproj import Transformer
        ITM2WGS = Transformer.from_crs("EPSG:2039", "EPSG:4326", always_xy=True)
    params = {"where": f"GUSH_NO='{gush}' AND HELKA_SHOW='{helka}'",
              "outFields": "GUSH_NO,HELKA_SHOW", "returnGeometry": "true",
              "outSR": 2039, "f": "pjson"}
    try:
        r = SESSION.get(PARCELS_URL, params=params,
                        headers={"User-Agent": UA, "Referer": "https://gisviewer.jerusalem.muni.il/"},
                        timeout=30)
        d = r.json()
        feats = d.get("features", [])
        if not feats:
            return None
        pts = feats[0]["geometry"]["rings"][0]
        cx = sum(p[0] for p in pts) / len(pts)
        cy = sum(p[1] for p in pts) / len(pts)
        lon, lat = ITM2WGS.transform(cx, cy)
        return {"itm": [round(cx, 2), round(cy, 2)], "wgs": [round(lon, 7), round(lat, 7)]}
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-file-year", type=int, default=2015)
    ap.add_argument("--limit", type=int, default=0, help="cap candidates classified (debug)")
    ap.add_argument("--no-fetch", action="store_true", help="use cached pub_false.json")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")

    archive = load_archive(not args.no_fetch)
    tracked = build_tracked()
    print(f"Tracked tiks: {len(tracked)}", flush=True)

    # Oranim rows (geographic filter), dedupe by tik keeping latest PD900 row
    in_district = build_district_filter()
    by_tik = {}
    for r in archive:
        if not in_district(r):
            continue
        by_tik.setdefault(norm_tik(r.get("tik_num")), []).append(r)

    def latest(rows):
        return max(rows, key=lambda r: str(r.get("PD900") or ""))

    missing = [t for t in by_tik if t not in tracked]
    candidates = sorted([t for t in missing if file_year(t) >= args.min_file_year])
    print(f"District §149 tiks: {len(by_tik)} | missing: {len(missing)} | "
          f"missing & file_year>={args.min_file_year}: {len(candidates)}", flush=True)
    if args.limit:
        candidates = candidates[:args.limit]

    tama38_hits, other_hits, dead = [], [], []
    for i, t in enumerate(candidates, 1):
        row = latest(by_tik[t])
        raw_tik = row.get("tik_num")
        d = yk_call(242700447, {"tikNum": raw_tik, "systemCode": int(YK_SYS)})
        rtype = status = addr = schuna = ""
        if d:
            x = d[0]
            rtype = (x.get("teurSugbakashaCodeMulti") or "").strip()
            status = (x.get("teurStatus") or "").strip()
            addr = " ".join(str(v) for v in [x.get("shemRehov"), x.get("misparBait")] if v).strip()
            schuna = (x.get("shemSchuna") or "").strip()
        rec = {"tik": raw_tik, "request_type": rtype, "status": status,
               "address": addr or f"{row.get('rehov')} {row.get('misp_bait')}".strip(),
               "schuna": schuna or row.get("schn"),
               "pd900": str(row.get("PD900") or "")[:10]}
        is_t38 = "תמא 38" in rtype.replace('"', "").replace("'", "") or "38" in rtype and "תמ" in rtype
        if not d:
            dead.append(rec)
        elif is_t38:
            tama38_hits.append(rec)
        else:
            other_hits.append(rec)
        tag = "T38" if is_t38 else ("···" if d else "DEAD")
        print(f"[{i}/{len(candidates)}] {tag} {raw_tik}  {rtype[:26]:26}  {rec['address'][:30]}", flush=True)
        time.sleep(1.3)

    # geometry for tama38 hits
    print(f"\nResolving geometry for {len(tama38_hits)} TAMA 38 hits...", flush=True)
    for rec in tama38_hits:
        gh = yk_call(242700456, {"SystemId": int(YK_SYS), "TikNum": rec["tik"]})
        parcels = sorted({f"{p.get('gush')}/{p.get('miHelka')}" for p in gh if p.get("gush")})
        rec["parcels"] = parcels
        geom = None
        for pc in parcels:
            g, h = pc.split("/")
            geom = parcel_centroid(g, h)
            if geom:
                break
        rec["geom"] = geom
        print(f"  {rec['tik']}  parcels={parcels}  geom={'ok' if geom else 'MISS'}", flush=True)
        time.sleep(1.3)

    out = {"generated": date.today().isoformat(),
           "min_file_year": args.min_file_year,
           "counts": {"district_tiks": len(by_tik), "missing": len(missing),
                      "classified": len(candidates), "tama38": len(tama38_hits),
                      "other": len(other_hits), "dead": len(dead)},
           "tama38": tama38_hits, "other": other_hits, "dead": dead}
    (ROOT / "section149_audit.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")

    # markdown report
    REPORT_DIR.mkdir(exist_ok=True)
    md = [f"# ביקורת §149 מול המאגר — {out['generated']}", "",
          f"- תיקי §149 באזור אורנים: **{len(by_tik)}**",
          f"- חסרים מהמאגר: **{len(missing)}**",
          f"- סווגו (שנת-תיק ≥ {args.min_file_year}): **{len(candidates)}**",
          f"- **תמ\"א 38 חסרות: {len(tama38_hits)}**",
          f"- היתרים אחרים חסרים: {len(other_hits)}",
          f"- ללא מענה מ-YK: {len(dead)}", "",
          "## תמ\"א 38 חסרות", "",
          "| תיק | סוג | סטטוס | כתובת | שכונה | פרסום | גוש/חלקה | גיאומטריה |",
          "|---|---|---|---|---|---|---|---|"]
    for r in tama38_hits:
        md.append(f"| {r['tik']} | {r['request_type']} | {r['status']} | {r['address']} | "
                  f"{r['schuna']} | {r['pd900']} | {','.join(r.get('parcels',[]))} | "
                  f"{'✓' if r.get('geom') else '—'} |")
    (REPORT_DIR / f"section149_audit_{out['generated']}.md").write_text("\n".join(md), encoding="utf-8")

    print(f"\n=== DONE ===")
    print(f"TAMA 38 missing: {len(tama38_hits)} | other missing: {len(other_hits)} | dead: {len(dead)}")
    print(f"Report: permits_scan_reports/section149_audit_{out['generated']}.md")
    print(f"JSON:   section149_audit.json")


if __name__ == "__main__":
    main()
