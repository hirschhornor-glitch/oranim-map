"""
enrich_tama38_developers.py — pull the developer (יזם) + architect for every TAMA 38
tik from YK's baaleiInyanList (proc 447), into a side-file keyed by tik so re-scrapes
never wipe it (same pattern as field_observations.json / extra_permits.json).

Developer = the 'מבקש/מגיש' interested party (for TAMA 38 this is the developer's SPV,
often address-named e.g. "טשרניחובסקי 48 בע"מ"). Architect = 'עורך בקשה'. Also parses
a unit count from the request description when present. Resumable + throttle-safe.

Output: oranim-app/data/tama38_developers.json
  {"built_at": ..., "count": N, "by_tik": {tik: {developer, developer_parts, architect,
   units, developer_role, is_residents}}}

Run:  py -X utf8 enrich_tama38_developers.py   (re-run to resume after a throttle stop)
"""
import json, re, sys, time
import requests

ROOT = r"C:\ORANIM"; DATA = ROOT + r"\oranim-app\data"
GEO = DATA + r"\tama38.geojson"
OUT = DATA + r"\tama38_developers.json"
YK = "https://jerbasicserviceapi.jerusalem.muni.il/api/Db/ExecuteGetJSON"
H = {"content-type": "application/json", "referer": "https://ykpubdata.jerusalem.muni.il/",
     "origin": "https://ykpubdata.jerusalem.muni.il", "user-agent": "Mozilla/5.0"}
S = requests.Session()

COMPANY_MARK = ("בע\"מ", "בעמ", "בע'מ", "חברה", "שותפות", 'יזום', 'נדל"ן', 'נדלן',
                'התחדשות', 'השקעות', 'יזמות', 'בניה', 'בנייה', 'קבוצת', 'ש.ע.ט', 'ע.ט')
RESIDENTS = ("הדיירים", "דיירים", "נציגות", "ועד הבית", "בעלי הדירות")


def yk_tik(tik):
    """YK's proc 242700447 only matches the canonical YYYY/NNNN.SS file number.
    tama38.geojson holds most tiks as 2015/318, 2014/358.01 or 2009/808.3, and YK
    answers those with an empty list - which this script used to read as a
    throttle, so the daily backfill "throttled" on every tik from July to October
    2026 without ever enriching one."""
    m = re.match(r"^(\d{4})/(\d+)(?:\.(\d+))?$", str(tik).strip())
    return f"{m.group(1)}/{int(m.group(2)):04d}.{int(m.group(3) or 0):02d}" if m else tik


def yk(proc, params, tries=5):
    """Rows for the query; [] when YK answered but found nothing; None when YK
    could not be reached (errors / non-JSON - the real throttle)."""
    for a in range(tries):
        try:
            r = S.post(YK, json={"ProcName": proc, "Cnn": "cnnGisYk", "Parameters": params},
                       headers=H, timeout=30)
            r.raise_for_status()
            d = r.json()
            if isinstance(d, list):
                return d
        except Exception:
            pass
        time.sleep(3 * (a + 1))       # 3,6,9,12,15s — grind through soft throttle
    return None


def _clean_name(s):
    s = str(s or "")
    s = re.sub(r'ת\.?\s*ז\.?\s*\d+', '', s)      # strip ID: ת.ז 058457656
    s = re.sub(r'ח\.?\s*פ\.?\s*\d+', '', s)      # strip company reg no
    s = re.sub(r'\b\d{6,9}\b', '', s)             # bare ID-like digit runs
    return re.sub(r'\s+', ' ', s).strip(" .,-")


def name_of(b):
    prati = _clean_name(b.get("prati"))
    mish = _clean_name(b.get("mishpaha"))
    if prati and mish:
        # company: prati holds the corp name, mish a group/descriptor -> keep both distinct
        return prati, mish
    return (prati or mish), ""


def extract(bili):
    """Return (developer_str, developer_parts, architect, is_residents)."""
    if not isinstance(bili, list):
        return "", [], "", False
    reqs = [b for b in bili if (b.get("teurSugBaalInyan") or "") == "מבקש/מגיש"]
    arch = ""
    for b in bili:
        if (b.get("teurSugBaalInyan") or "") == "עורך בקשה":
            p, m = name_of(b); arch = (p + " " + m).strip() if m and len(m) < 20 else p
            break
    parts, residents = [], False
    for b in reqs:
        p, m = name_of(b)
        full = (p + (" " + m if m else "")).strip()
        if any(w in full for w in RESIDENTS):
            residents = True
            continue
        # prefer the token that looks like a company/group
        pick = p
        if not any(w in p for w in COMPANY_MARK) and any(w in m for w in COMPANY_MARK):
            pick = m
        parts.append({"applicant": p, "group": m, "name": pick or full})
    dev = " / ".join(dict.fromkeys(x["name"] for x in parts if x["name"]))
    return dev, parts, arch, residents


def parse_units(desc):
    for m in re.finditer(r'(\d+)\s*יח', desc or ""):
        return int(m.group(1))
    return None


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    geo = json.load(open(GEO, encoding="utf-8"))
    perms = json.load(open(DATA + r"\tama38_permits.json", encoding="utf-8"))
    tiks = [f["properties"]["tik"] for f in geo["features"]
            if re.match(r'\d{4}/\d+', str(f["properties"].get("tik") or ""))]
    # description lookup for unit parsing
    desc_by_tik = {}
    for e in perms.values():
        for pm in e.get("permits", []):
            desc_by_tik.setdefault(pm.get("file_number"),
                                   (pm.get("request_description") or "") + " " + (pm.get("request_type") or ""))
    try:
        doc = json.load(open(OUT, encoding="utf-8")); by = doc.get("by_tik", {})
    except Exception:
        by = {}
    n = len(tiks)
    limit = int(next((a.split("=")[1] for a in sys.argv if a.startswith("--limit=")), "0"))
    got, consec_thr = 0, 0
    for i, tik in enumerate(tiks, 1):
        if tik in by:
            continue
        if limit and got >= limit:
            print(f"batch limit {limit} reached; pausing for cooldown", flush=True)
            break
        d = yk(242700447, {"tikNum": yk_tik(tik), "systemCode": 26400046})
        if d == []:
            # YK answered: no such file. Record it so the daily backfill does not
            # re-ask forever; the empty developer keeps it out of the report.
            consec_thr = 0
            by[tik] = {"developer": "", "developer_parts": [], "architect": "",
                       "units": None, "is_residents": False, "yk_not_found": yk_tik(tik)}
            json.dump({"count": len(by), "by_tik": by}, open(OUT, "w", encoding="utf-8"),
                      ensure_ascii=False, indent=1)
            print(f"[{i}/{n}] N/F {tik} (YK has no {yk_tik(tik)})", flush=True)
            time.sleep(2.2)
            continue
        if not d:
            consec_thr += 1
            print(f"[{i}/{n}] THR {tik} (throttled, will resume)", flush=True)
            if consec_thr >= 40:
                print("40 consecutive throttles — YK hard-blocked, aborting", flush=True)
                break
            continue
        consec_thr = 0
        got += 1
        dev, parts, arch, residents = extract(d[0].get("baaleiInyanList"))
        units = parse_units(desc_by_tik.get(tik, "")) or parse_units(d[0].get("mahutBakasha"))
        by[tik] = {"developer": dev, "developer_parts": parts, "architect": arch,
                   "units": units, "is_residents": residents}
        json.dump({"count": len(by), "by_tik": by}, open(OUT, "w", encoding="utf-8"),
                  ensure_ascii=False, indent=1)
        flag = "DEV" if dev else ("RES" if residents else "—")
        print(f"[{i}/{n}] {flag} {tik}  {dev[:34]}", flush=True)
        time.sleep(2.2)

    withdev = sum(1 for v in by.values() if v.get("developer"))
    res = sum(1 for v in by.values() if v.get("is_residents") and not v.get("developer"))
    print(f"\nDONE-DEVELOPERS: {len(by)}/{n} enriched; {withdev} with developer; "
          f"{res} residents-led; {n - len(by)} still throttled.")


if __name__ == "__main__":
    main()
