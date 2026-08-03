"""
weekly_tama38_149_scan.py
-------------------------
Recurring safety-net that catches TAMA 38 permits which "fell between the chairs" —
issued/published via §149 (הקלות) but citing only old local plan numbers (no Mavat
code) and absent from the city's renewal GIS. Runs weekly; INCREMENTAL — a persisted
`section149_seen.json` means each run only classifies §149 publications it hasn't seen,
so the weekly delta is tiny and never trips YK's throttle.

Pipeline (all self-contained here):
  1. Fetch the full §149 archive (betokef=false; fallback pub_false.json).
  2. Geo-filter to the Oranim district (geocode address -> PIP district_oranim; schn fallback).
  3. Candidates = in-district, NOT already tracked, NOT already seen, file_year >= MIN.
  4. Classify the delta via YK proc 447; keep request_type ⊇ "תמ"א 38".
  5. For each new TAMA 38: status/type/desc, developer (baaleiInyanList 'מבקש/מגיש'),
     architect, units, building-footprint geocode, minahak (PIP).
  6. Append to tama38.geojson + tama38_permits.json (data + root) + tama38_developers.json.
  7. Mark ALL classified tiks in section149_seen.json.
  8. If anything new: bump APP_VERSION + app.js?v=, rebuild app.js, git commit + pull --rebase + push.
  9. Email a summary (only when new permits were added).

CLI:
  py -X utf8 weekly_tama38_149_scan.py [--email] [--min-file-year 2005] [--dry-run] [--no-push]

Windows task: Oranim_Tama38_149_Weekly (Sunday 05:00). Related: [[project_permits_coverage_pipeline]].
"""
import argparse, json, os, re, smtplib, subprocess, sys, time
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import requests

ROOT = Path(r"C:\ORANIM")
DATA = ROOT / "oranim-app" / "data"
APPJSX = ROOT / "oranim-app" / "src" / "app.jsx"
INDEXHTML = ROOT / "oranim-app" / "index.html"
GEO = DATA / "tama38.geojson"
PERM_DATA = DATA / "tama38_permits.json"
PERM_ROOT = ROOT / "tama38_permits.json"
DEVS = DATA / "tama38_developers.json"
SEEN = ROOT / "section149_seen.json"
ARCHIVE_CACHE = ROOT / "pub_false.json"
REPORT_DIR = ROOT / "permits_scan_reports"

YK = "https://jerbasicserviceapi.jerusalem.muni.il/api/Db/ExecuteGetJSON"
YKH = {"content-type": "application/json", "referer": "https://ykpubdata.jerusalem.muni.il/",
       "origin": "https://ykpubdata.jerusalem.muni.il", "user-agent": "Mozilla/5.0"}
S149_API = ("https://jergisrishuimessages.jerusalem.muni.il/Rishui149/api/publish/"
            "?fromDate=&toDate=&tik_num=&streetcode=&schncode=&betokef=false")
S149_REFERER = "https://jergisrishuimessages.jerusalem.muni.il/Rishui149/Pages/msg149.html"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# schn fallback (only for un-geocodable rows); membership is decided geographically.
CORR_SCHN = {
    'גוננים', 'קטמון הישנה', 'גאולים - בקעה', 'רחביה', 'תלפיות', 'בית צפפה', 'גבעת הורדים',
    'מושבה גרמנית', 'ארנונה', 'המושבה היוונית', 'מקור חיים', 'קריית שמואל', 'טלביה/המוגרבים',
    'טלביה', 'תלפיות תעשיה', 'צפון תלפיות', 'פת', 'נוה שאנן/ניות', 'ימין משה', 'גונן ט',
}
MINAHAK_FILES = {
    "minahak_baka.geojson": "בקעה רבתי", "minahak_beit_tzfafa.geojson": "בית צפאפא",
    "minahak_ganot.geojson": "גינות העיר", "minahak_gonen.geojson": "גוננים",
    "minahak_malha.geojson": "מינהל מוסדי מלחה", "minahak_talpiot.geojson": "א.ת. תלפיות",
}
COMPANY_MARK = ("בע\"מ", "בעמ", "חברה", "שותפות", 'יזום', 'נדל"ן', 'נדלן', 'התחדשות',
                'השקעות', 'יזמות', 'בניה', 'בנייה', 'קבוצת')
RESIDENTS = ("הדיירים", "דיירים", "נציגות", "ועד הבית", "בעלי הדירות")
DEAD = ("נגנז", "בוטל", "נדחת", "סגירת", "ביטול", "נסגר")

S = requests.Session()


def log(m): print(m, flush=True)
def norm_tik(t):
    t = str(t or "").replace(" ", "")
    m = re.match(r'(\d{4})/(\d+)\.(\d+)', t)
    if m: return f"{m.group(1)}/{int(m.group(2))}.{int(m.group(3))}"
    m2 = re.match(r'(\d{4})/(\d+)', t)
    return f"{m2.group(1)}/{int(m2.group(2))}" if m2 else t
def file_year(t):
    m = re.match(r'(\d{4})/', t); return int(m.group(1)) if m else 0


def yk(proc, params, tries=4):
    for a in range(tries):
        try:
            d = S.post(YK, json={"ProcName": proc, "Cnn": "cnnGisYk", "Parameters": params},
                       headers=YKH, timeout=30).json()
            if d: return d
        except Exception:
            pass
        time.sleep(4 * (a + 1))
    return []


def load_archive(do_fetch):
    if do_fetch:
        try:
            r = S.get(S149_API, headers={"User-Agent": UA, "Referer": S149_REFERER}, timeout=90)
            r.raise_for_status()
            rows = r.json()
            if isinstance(rows, list) and rows:
                ARCHIVE_CACHE.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
                log(f"fetched §149 archive: {len(rows)} rows")
                return rows
        except Exception as e:
            log(f"§149 fetch failed ({e}); using cache")
    return json.load(open(ARCHIVE_CACHE, encoding="utf-8"))


def build_tracked():
    tracked = set()
    for f in [DATA / "all_permits.json", PERM_DATA]:
        j = json.load(open(f, encoding="utf-8"))
        for rec in j.values():
            for pm in rec.get("permits", []):
                tracked.add(norm_tik(pm.get("file_number")))
    for f in [DATA / "objections_permits.json", DATA / "extra_permits.json"]:
        if f.exists():
            for m in re.findall(r'\b(20\d\d/\d{3,4}\.\d{1,2})\b', f.read_text(encoding="utf-8")):
                tracked.add(norm_tik(m))
    return tracked


def nkey(s):
    s = re.sub(r'^(כיכר|ככר|שדרות|שד|רחוב|רח|דרך)\s+', '', str(s or ''))
    s = re.sub(r'^[א-ת]\.\s*', '', s)
    s = re.sub(r'([א-ת])\.([א-ת])\.', r'\1\2', s)
    return re.sub(r'\s+', '', s.replace('"', '').replace("'", '').replace('.', ''))


def build_geo():
    from collections import defaultdict
    idx, bystreet = {}, defaultdict(list)
    for f in json.load(open(DATA / "buildings.geojson", encoding="utf-8"))["features"]:
        p = f["properties"]; st = nkey(p.get("street")); hn = str(p.get("house_num") or "").strip()
        if st:
            bystreet[st].append(f["geometry"]["coordinates"])
            if hn: idx.setdefault((st, hn), f["geometry"]["coordinates"])
    dist = json.load(open(DATA / "district_oranim.geojson", encoding="utf-8"))
    polys = []
    for f in dist["features"]:
        g = f["geometry"]
        polys.extend(g["coordinates"] if g["type"] == "MultiPolygon" else [g["coordinates"]])

    def in_poly(lon, lat, ps):
        for poly in ps:
            inside = False
            for ring in poly:
                n = len(ring); j = n - 1
                for i in range(n):
                    xi, yi = ring[i][0], ring[i][1]; xj, yj = ring[j][0], ring[j][1]
                    if ((yi > lat) != (yj > lat)) and (lon < (xj - xi) * (lat - yi) / ((yj - yi) or 1e-12) + xi):
                        inside = not inside
                    j = i
            if inside: return True
        return False

    def geocode(street, house):
        hn = re.sub(r'\D', '', str(house or '')); st = nkey(street)
        for c in (st, st.lstrip('ה'), 'ה' + st):
            if (c, hn) in idx:
                p = idx[(c, hn)]; return [round(p[0], 7), round(p[1], 7)], 'building'
        cs = bystreet.get(st) or [x for k in bystreet if st and (st in k or k in st) for x in bystreet[k]]
        if cs: return [round(sum(x[0] for x in cs) / len(cs), 7), round(sum(x[1] for x in cs) / len(cs), 7)], 'street_approx'
        return None, None

    def in_district(row):
        # Fast filter: exact building lookup only (NO per-row street scan), else schn fallback.
        hn = re.sub(r'\D', '', str(row.get("misp_bait") or '')); st = nkey(row.get("rehov"))
        for c in (st, st.lstrip('ה'), 'ה' + st):
            if (st and (c, hn) in idx):
                p = idx[(c, hn)]; return in_poly(p[0], p[1], polys)
        return row.get("schn") in CORR_SCHN

    return geocode, in_district


def load_minahaks():
    def rings(g):
        if not g: return []
        return g["coordinates"] if g["type"] == "MultiPolygon" else [g["coordinates"]] if g["type"] == "Polygon" else []
    out = []
    for fn, name in MINAHAK_FILES.items():
        gj = json.load(open(DATA / fn, encoding="utf-8"))
        out.append((name, [r for f in gj["features"] for r in rings(f.get("geometry"))]))
    return out


def minahak_of(lon, lat, minahaks):
    for name, polys in minahaks:
        for poly in polys:
            inside = False
            for ring in poly:
                n = len(ring); j = n - 1
                for i in range(n):
                    xi, yi = ring[i][0], ring[i][1]; xj, yj = ring[j][0], ring[j][1]
                    if ((yi > lat) != (yj > lat)) and (lon < (xj - xi) * (lat - yi) / ((yj - yi) or 1e-12) + xi):
                        inside = not inside
                    j = i
            if inside: return name
    return None


def _clean(s):
    s = str(s or "")
    s = re.sub(r'ת\.?\s*ז\.?\s*\d+', '', s); s = re.sub(r'ח\.?\s*פ\.?\s*\d+', '', s)
    s = re.sub(r'\b\d{6,9}\b', '', s)
    return re.sub(r'\s+', ' ', s).strip(" .,-")


def extract_dev(bili):
    if not isinstance(bili, list): return "", "", False
    arch = ""
    for b in bili:
        if (b.get("teurSugBaalInyan") or "") == "עורך בקשה":
            arch = _clean(b.get("prati")) or _clean(b.get("mishpaha")); break
    parts, residents = [], False
    for b in bili:
        if (b.get("teurSugBaalInyan") or "") != "מבקש/מגיש": continue
        p, m = _clean(b.get("prati")), _clean(b.get("mishpaha"))
        full = (p + (" " + m if m else "")).strip()
        if any(w in full for w in RESIDENTS): residents = True; continue
        pick = p
        if not any(w in p for w in COMPANY_MARK) and any(w in m for w in COMPANY_MARK): pick = m
        if pick: parts.append(pick)
    return " / ".join(dict.fromkeys(parts)), arch, residents


def parse_units(*texts):
    for t in texts:
        m = re.search(r'(\d+)\s*יח', t or "")
        if m: return int(m.group(1))
    return None


def map_status(s):
    if "הופק" in (s or "") or "הוצא היתר" in (s or ""): return "הופק הוצא היתר"
    if "ועדת רישוי" in (s or "") or "אושר" in (s or ""): return "היתר אושר בועדת רישוי"
    return "נפתח תיק היתר"


def git(*args):
    return subprocess.run(["git", "-C", str(ROOT / "oranim-app"), *args],
                          capture_output=True, text=True, encoding="utf-8")


def send_email(added):
    to = os.environ.get("PERMITS_EMAIL_TO", "Or_hi@jerusalem.muni.il")
    pw = os.environ.get("GMAIL_APP_PASSWORD")
    if not pw:
        log("no GMAIL_APP_PASSWORD — skipping email"); return
    rows = "".join(
        f"<tr><td style='padding:4px 8px;border:1px solid #ccc'>{a['tik']}</td>"
        f"<td style='padding:4px 8px;border:1px solid #ccc'>{a['address']}</td>"
        f"<td style='padding:4px 8px;border:1px solid #ccc'>{a['minahak'] or ''}</td>"
        f"<td style='padding:4px 8px;border:1px solid #ccc'>{a['status']}</td>"
        f"<td style='padding:4px 8px;border:1px solid #ccc'>{a['developer'] or ''}</td></tr>"
        for a in added)
    html = (f"<div dir='rtl'><h3>נתפסו {len(added)} תמ\"א 38 חדשות (ביקורת §149 שבועית)</h3>"
            f"<table style='border-collapse:collapse'><tr>"
            f"<th style='padding:4px 8px;border:1px solid #ccc'>תיק</th>"
            f"<th style='padding:4px 8px;border:1px solid #ccc'>כתובת</th>"
            f"<th style='padding:4px 8px;border:1px solid #ccc'>מינה\"ק</th>"
            f"<th style='padding:4px 8px;border:1px solid #ccc'>סטטוס</th>"
            f"<th style='padding:4px 8px;border:1px solid #ccc'>יזם</th></tr>{rows}</table>"
            f"<p style='color:#888;font-size:12px'>נוצר ע\"י weekly_tama38_149_scan.py</p></div>")
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"ביקורת §149: {len(added)} תמ\"א 38 חדשות — {date.today().isoformat()}"
    msg["From"] = "hirschhorn.or@gmail.com"; msg["To"] = to
    msg.attach(MIMEText(html, "html", "utf-8"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
        s.login("hirschhorn.or@gmail.com", pw); s.send_message(msg)
    log(f"email sent to {to}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-file-year", type=int, default=2005)
    ap.add_argument("--email", action="store_true")
    ap.add_argument("--no-push", action="store_true")
    ap.add_argument("--no-fetch", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")

    archive = load_archive(not args.no_fetch)
    tracked = build_tracked()
    seen = set(json.load(open(SEEN, encoding="utf-8")).get("tiks", [])) if SEEN.exists() else set()
    geocode, in_district = build_geo()
    minahaks = load_minahaks()

    # dedupe archive by tik (latest PD900), district-filter
    by_tik = {}
    for r in archive:
        if in_district(r):
            by_tik.setdefault(norm_tik(r.get("tik_num")), []).append(r)
    latest = lambda rows: max(rows, key=lambda r: str(r.get("PD900") or ""))
    candidates = [t for t in by_tik
                  if t not in tracked and t not in seen and file_year(t) >= args.min_file_year]
    log(f"district §149 tiks: {len(by_tik)} | new candidates to classify: {len(candidates)}")

    geo = json.load(open(GEO, encoding="utf-8"))
    pdata = json.load(open(PERM_DATA, encoding="utf-8"))
    proot = json.load(open(PERM_ROOT, encoding="utf-8")) if PERM_ROOT.exists() else dict(pdata)
    devs = json.load(open(DEVS, encoding="utf-8")) if DEVS.exists() else {"by_tik": {}}
    existing_tiks = {norm_tik(f["properties"].get("tik")) for f in geo["features"] if f["properties"].get("tik")}
    max_fid = max(int(f["properties"].get("fid") or 0) for f in geo["features"])

    added, newly_seen = [], []
    for t in candidates:
        row = latest(by_tik[t]); raw = row.get("tik_num")
        d = yk(242700447, {"tikNum": raw, "systemCode": 26400046})
        if not d:
            continue                        # throttled — retry next week (not marked seen)
        newly_seen.append(t)
        x = d[0]
        rtype = (x.get("teurSugbakashaCodeMulti") or "").strip()
        if not (("תמא 38" in rtype.replace('"', "").replace("'", "")) or ("38" in rtype and "תמ" in rtype)):
            continue                        # classified, not TAMA 38
        status = (x.get("teurStatus") or "").strip()
        if any(w in status for w in DEAD):
            continue
        if norm_tik(raw) in existing_tiks:
            continue
        street = x.get("shemRehov") or row.get("rehov")
        house = x.get("misparBait") or row.get("misp_bait")
        addr = " ".join(str(v) for v in [x.get("shemRehov"), x.get("misparBait")] if v).strip() \
            or f"{row.get('rehov')} {row.get('misp_bait')}".strip()
        ll, src = geocode(street, house)
        if not ll:
            continue                        # can't place — skip (rare)
        dev, arch, residents = extract_dev(x.get("baaleiInyanList"))
        units = parse_units(x.get("mahutBakasha"), rtype)
        mnk = minahak_of(ll[0], ll[1], minahaks)
        max_fid += 1; fid = max_fid
        geo["features"].append({
            "type": "Feature", "geometry": {"type": "MultiPoint", "coordinates": [ll]},
            "properties": {"fid": fid, "qc_id": fid, "tik": raw, "address": addr,
                           "neighborho": (x.get("shemSchuna") or row.get("schn") or ""),
                           "rova": 0.0, "units_exis": 0.0, "units_tose": float(units or 0),
                           "open_": "", "status": map_status(status), "date": None, "status_1": None,
                           "comment": f"נוסף אוטומטית {date.today().isoformat()} מביקורת §149 שבועית.",
                           "harisaa": "כן" if "הריסה" in rtype else "לא", "y": None, "x": None,
                           "created_us": "weekly149", "created_da": date.today().isoformat(),
                           "last_edite": "weekly149", "last_edi_1": date.today().isoformat(), "__rec_stat": None}})
        entry = {"tik": raw, "address": addr,
                 "permits": [{"file_number": raw, "status": status,
                              "status_date": (x.get("taarihStatus") or "").strip(), "request_type": rtype,
                              "request_description": (x.get("mahutBakasha") or "").strip()}],
                 "scraped_at": None, "permit_count": 1, "error": ""}
        pdata[str(fid)] = entry; proot[str(fid)] = dict(entry)
        devs["by_tik"][raw] = {"developer": dev, "architect": arch, "units": units,
                               "minahak": mnk, "is_residents": residents}
        existing_tiks.add(norm_tik(raw))
        added.append({"tik": raw, "address": addr, "status": status, "minahak": mnk, "developer": dev})
        log(f"+ {raw}  {addr}  {rtype[:24]}  dev={dev[:24]}")
        time.sleep(1.5)

    seen |= set(newly_seen)
    log(f"\nclassified {len(newly_seen)} new; added {len(added)} TAMA 38.")

    if args.dry_run:
        log("dry-run — no writes."); return

    # persist seen even when nothing added (so we don't reclassify next week)
    SEEN.write_text(json.dumps({"tiks": sorted(seen)}, ensure_ascii=False, indent=0), encoding="utf-8")
    if not added:
        log("nothing new — done."); return

    devs["count"] = len(devs["by_tik"])
    json.dump(geo, open(GEO, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    json.dump(pdata, open(PERM_DATA, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    json.dump(proot, open(PERM_ROOT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    json.dump(devs, open(DEVS, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    # cache-bust: bump APP_VERSION + app.js?v= (CI rebuilds app.js on push touching src/app.jsx)
    stamp = date.today().isoformat() + "-149w"
    jsx = APPJSX.read_text(encoding="utf-8")
    jsx = re.sub(r"const APP_VERSION = '[^']*';", f"const APP_VERSION = '{stamp}';", jsx, count=1)
    APPJSX.write_text(jsx, encoding="utf-8")
    html = INDEXHTML.read_text(encoding="utf-8")
    html = re.sub(r'app\.js\?v=[^"]*', f'app.js?v={stamp}', html, count=1)
    INDEXHTML.write_text(html, encoding="utf-8")

    if args.no_push:
        log("--no-push — files written, not pushed."); return
    # Regenerate the derived permit files from the freshly-updated tama38 store so the
    # canonical master reflects these §149 additions immediately (no wait for biweekly).
    # Regenerate derived files + fill units/floors for the newly-added תמ"א 38 buildings
    # (enrich resumes → only the new ones), then re-check them against the master plans.
    for builder in ("build_permits_master.py", "build_permits_health.py",
                    "enrich_tama38_units.py", "tama38_master_plan_check.py"):
        r = subprocess.run([sys.executable, str(ROOT / builder)], cwd=str(ROOT),
                           capture_output=True, text=True, encoding="utf-8", errors="replace")
        log(f"ran {builder}" + (f" (exit {r.returncode})" if r.returncode else ""))
    git("add", "data/tama38.geojson", "data/tama38_permits.json", "data/tama38_developers.json",
        "data/tama38_master_plan_check.json",
        "data/permits_master.json", "data/permits_health.json", "src/app.jsx", "index.html")
    msg = f"תמ\"א 38: +{len(added)} מביקורת §149 שבועית ({date.today().isoformat()})\n\n" + \
          "\n".join(f"{a['tik']} {a['address']}" for a in added) + \
          "\n\nCo-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
    git("commit", "-m", msg)
    git("fetch", "origin"); git("rebase", "origin/master")
    push = git("push", "origin", "master")
    log("push: " + (push.stderr or push.stdout).strip()[-200:])

    if args.email:
        try: send_email(added)
        except Exception as e: log(f"email failed: {e}")
    log("done.")


if __name__ == "__main__":
    main()
