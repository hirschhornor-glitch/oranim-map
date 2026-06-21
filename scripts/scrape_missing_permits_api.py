"""
scrape_missing_permits_api.py
-----------------------------
Fetch the 36 in-scope permits missing from our dataset, via the YK public JSON
API (jerbasicserviceapi /api/Db/ExecuteGetJSON) — headless HTTP, no browser, no
WAF. Resolves each tik to a taba that exists in our plans.geojson so it can be
merged into all_permits.json and displayed on the map.

Procs used (discovered via probe_tik_api.py):
  242700447  {tikNum, systemCode}      -> status/date/type/description/address
  242700457  {systemID, tikNum}        -> list of תכניות (tochnit) applying to the place
  242700456  {SystemId, TikNum}        -> gush/helka parcels (kept for reference)

Usage:
  py -X utf8 scrape_missing_permits_api.py                # all from missing_permits_list.txt
  py -X utf8 scrape_missing_permits_api.py 2024/0390.00   # specific tik(s)
"""
import json, sys, time
import requests

LIST_FILE = r"C:\ORANIM\missing_permits_list.txt"
PLANS = r"C:\ORANIM\oranim-app\data\plans.geojson"
OUT = r"C:\ORANIM\missing_permits_api.json"
API = "https://jerbasicserviceapi.jerusalem.muni.il/api/Db/ExecuteGetJSON"
SYS = "26400046"
H = {"content-type": "application/json",
     "referer": "https://ykpubdata.jerusalem.muni.il/",
     "origin": "https://ykpubdata.jerusalem.muni.il",
     "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


SESSION = requests.Session()
SESSION.headers.update(H)


def call(proc, params, retries=4):
    """POST a stored-proc. Retries empty results with backoff — the API throttles
    bursts by returning [] (not an error), so an empty list is treated as a soft
    failure and retried before being accepted as genuinely empty."""
    last = []
    for attempt in range(retries):
        try:
            r = SESSION.post(API, json={"ProcName": proc, "Cnn": "cnnGisYk", "Parameters": params}, timeout=30)
            r.raise_for_status()
            data = r.json()
            if data:
                return data
            last = data
        except Exception:
            pass
        time.sleep(2 * (attempt + 1))  # 2s, 4s, 6s, 8s
    return last


def our_tabas():
    g = json.load(open(PLANS, encoding="utf-8"))
    return {str(f["properties"].get("taba")).strip() for f in g["features"]
            if f["properties"].get("taba")}


def fetch_one(tik):
    rec = {"file_number": tik}
    # 1) detail
    try:
        d = call(242700447, {"tikNum": tik, "systemCode": SYS})
        if d:
            row = d[0]
            rec["status"] = row.get("teurStatus") or ""
            rec["status_date"] = row.get("taarihStatus") or ""
            rec["request_type"] = row.get("teurSugbakashaCodeMulti") or ""
            rec["request_description"] = row.get("mahutBakasha") or ""
            rec["address"] = " ".join(str(x) for x in [row.get("shemRehov"), row.get("misparBait")] if x).strip()
            rec["neighborhood_yk"] = row.get("shemSchuna") or ""
            rec["maslul"] = row.get("maslulTik") or ""
        else:
            rec["error"] = "no_detail"
    except Exception as e:
        rec["error"] = f"detail:{e}"
    # 2) plans applying to the place
    try:
        plans = call(242700457, {"systemID": SYS, "tikNum": tik})
        rec["tochnit_all"] = sorted({str(p.get("tohnit")).strip() for p in plans if p.get("tohnit")})
    except Exception as e:
        rec["tochnit_all"] = []
        rec.setdefault("error", f"plans:{e}")
    # 3) gush/helka (reference)
    try:
        gh = call(242700456, {"SystemId": SYS, "TikNum": tik})
        rec["parcels"] = sorted({f"{p.get('gush')}/{p.get('miHelka')}" for p in gh if p.get("gush")})
    except Exception:
        rec["parcels"] = []
    return rec


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    tiks = sys.argv[1:] or [l.strip() for l in open(LIST_FILE, encoding="utf-8") if l.strip()]
    ours = our_tabas()
    results = []
    for i, tik in enumerate(tiks, 1):
        rec = fetch_one(tik)
        matches = [t for t in rec.get("tochnit_all", []) if t in ours]
        rec["matched_tabas"] = matches
        results.append(rec)
        flag = "✓" if matches else ("·" if rec.get("status") else "✗")
        print(f"[{i}/{len(tiks)}] {flag} {tik}  status='{rec.get('status','')[:24]}'  "
              f"plans={rec.get('tochnit_all',[])[:4]}  our-taba={matches}", flush=True)
        time.sleep(1.5)

    json.dump(results, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    got = sum(1 for r in results if r.get("status"))
    matched = sum(1 for r in results if r["matched_tabas"])
    print(f"\n{got}/{len(results)} returned a status; {matched} resolved to a taba in our plans.")
    print(f"Output: {OUT}")
    print("\n--- permits that did NOT match any of our plans (belong to untracked plans) ---")
    for r in results:
        if r.get("status") and not r["matched_tabas"]:
            print(f"  {r['file_number']}  {r.get('neighborhood_yk','')}  plans={r.get('tochnit_all',[])}")


if __name__ == "__main__":
    main()
