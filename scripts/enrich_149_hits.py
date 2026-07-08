"""
enrich_149_hits.py — resumable enrichment of the missing TAMA 38 permits found by
the §149 audit. Per tik: proc 447 (status/type/address/essence) from YK's JSON API,
then place the marker at the BUILDING FOOTPRINT by geocoding the address offline
against buildings.geojson (no gisviewer/ArcGIS -> no Akamai throttle). Writes
incrementally to missing_tama38_enriched.json (keyed by tik); just re-run to resume.

Run:  py -X utf8 enrich_149_hits.py
"""
import json, re, sys, time
import requests

ROOT = r"C:\ORANIM"
DATA = ROOT + r"\oranim-app\data"
IN = ROOT + r"\missing_tama38_149.json"
OUT = ROOT + r"\missing_tama38_enriched.json"
YK = "https://jerbasicserviceapi.jerusalem.muni.il/api/Db/ExecuteGetJSON"
YKH = {"content-type": "application/json", "referer": "https://ykpubdata.jerusalem.muni.il/",
       "origin": "https://ykpubdata.jerusalem.muni.il",
       "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
S = requests.Session()


def yk(proc, params, tries=6):
    for a in range(tries):
        try:
            r = S.post(YK, json={"ProcName": proc, "Cnn": "cnnGisYk", "Parameters": params},
                       headers=YKH, timeout=30)
            d = r.json()
            if d:
                return d
        except Exception:
            pass
        time.sleep(4 * (a + 1))
    return []


def nkey(s):
    s = re.sub(r'^[א-ת]\.\s*', '', str(s or ''))
    s = s.replace('"', '').replace("'", '').replace('`', '')
    return re.sub(r'\s+', '', s)


def build_geocoder():
    idx = {}
    for f in json.load(open(DATA + r"\buildings.geojson", encoding="utf-8"))["features"]:
        p = f["properties"]
        st = nkey(p.get("street")); hn = str(p.get("house_num") or "").strip()
        if st and hn:
            idx.setdefault((st, hn), f["geometry"]["coordinates"])

    def geocode(street, house):
        hn = str(house or "").strip()
        st = nkey(street)
        for cand in (st, st.lstrip('ה'), 'ה' + st):
            c = idx.get((cand, hn))
            if c:
                return [round(c[0], 7), round(c[1], 7)]
        return None
    return geocode


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    todo = json.load(open(IN, encoding="utf-8"))
    geocode = build_geocoder()
    try:
        done = json.load(open(OUT, encoding="utf-8"))
    except Exception:
        done = {}
    n = len(todo)
    for i, row in enumerate(todo, 1):
        tik = row["tik"]
        if done.get(tik, {}).get("status"):
            continue                       # already enriched
        d = yk(242700447, {"tikNum": tik, "systemCode": 26400046})
        rec = {"tik": tik, "schn": row.get("schn"), "pd900": row.get("PD900")}
        street = row.get("rehov"); house = row.get("bait")
        if d:
            x = d[0]
            rec["request_type"] = (x.get("teurSugbakashaCodeMulti") or "").strip()
            rec["status"] = (x.get("teurStatus") or "").strip()
            rec["status_date"] = (x.get("taarihStatus") or "").strip()
            rec["description"] = (x.get("mahutBakasha") or "").strip()
            street = x.get("shemRehov") or street
            house = x.get("misparBait") or house
            rec["address"] = " ".join(str(v) for v in [x.get("shemRehov"), x.get("misparBait")] if v).strip() \
                or f"{row.get('rehov')} {row.get('bait')}".strip()
            rec["schuna_yk"] = (x.get("shemSchuna") or "").strip() or row.get("schn")
        else:
            rec["status"] = ""
            rec["address"] = f"{row.get('rehov')} {row.get('bait')}".strip()
            rec["_447_throttled"] = True
        rec["lnglat"] = geocode(street, house)      # building footprint, offline
        done[tik] = rec
        json.dump(done, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        print(f"[{i}/{n}] {tik}  {rec.get('request_type','')[:22]:22} "
              f"geo={'ok' if rec['lnglat'] else 'MISS'}  {rec['address'][:26]}", flush=True)
        time.sleep(2.0)

    full = [r for r in done.values() if r.get("status")]
    geo = [r for r in done.values() if r.get("lnglat")]
    thr = [r for r in done.values() if not r.get("status")]
    print(f"\nDONE-149-ENRICH: {len(full)}/{n} have status; {len(geo)} geocoded; "
          f"{len(thr)} throttled (re-run to fill).")


if __name__ == "__main__":
    main()
