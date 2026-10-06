"""
fetch_objections_official.py
----------------------------
Build objections_permits.json from the CITY'S OWN active-§149-publications API —
the authoritative source behind jerusalem.muni.il .../activepublications/.

    GET https://jergisrishuimessages.jerusalem.muni.il/Rishui149/api/publish/
        ?fromDate=&toDate=&tik_num=&streetcode=&schncode=&betokef=true

Each record: tik_num, schn, rehov, misp_bait, teur_makom, ED850 (publication date),
PD900 (= 'תום מועד להגשת התנגדויות' — the REAL objection deadline), DocId (נוסח פרסום).

This replaces the old YK street-enumeration + תהליך-tab scraping, which (a) only
discovered permits street-by-street through the WAF, and (b) read 'השלמת פרסום'
which runs days LATER than the true 'תום מועד' deadline. Pre-publication permits
(not yet published) are intentionally absent here → omitted, as intended.

Run: python fetch_objections_official.py            # fetch + write + push
     python fetch_objections_official.py --email    # + send the summary email
     python fetch_objections_official.py --no-push   # local only
Fast (one HTTP call) → safe to run weekly/daily.
"""
import argparse
import json
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from datetime import date, datetime
from pathlib import Path

import requests

sys.path.insert(0, r"C:\ORANIM")
from scrape_objections_permits import (
    geocode, in_district, attribute_point, OUT_PATH, APP_COPY, APP_REPO,
)
from git_sync import commit_and_push_after_write

# Source = the FULL §149 archive (betokef=false), NOT the city's betokef=true "in-effect"
# list. That flag under-reports: it returned ~6 rows citywide while real in-district
# windows were open (e.g. 2026-07-19 it missed 1978/0433.03 אלעזר המודעי 5, deadline that
# very day). So we take the whole archive and decide "open for objections" OURSELVES, by
# PD900 (the real objection deadline) >= today — see build(). One HTTP call, ~14k rows.
API = ("https://jergisrishuimessages.jerusalem.muni.il/Rishui149/api/publish/"
       "?fromDate=&toDate=&tik_num=&streetcode=&schncode=&betokef=false")
REFERER = "https://jergisrishuimessages.jerusalem.muni.il/Rishui149/Pages/msg149.html"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
DOC_URL = "http://jerarchivews.jerusalem.muni.il/ArchiveNGService.svc/rest/StreamDocById?DocId="
STATUS = "פרסום פעיל לפי סעיף 149"  # kept only while PD900 window is open; app also excludes past-deadline
# District neighbourhood names — used only to LOUDLY flag a no-geocode row that is
# nonetheless in one of our neighbourhoods (a second, geocode-based, leak path).
CORR_SCHN = {
    'גוננים', 'קטמון הישנה', 'גאולים - בקעה', 'רחביה', 'תלפיות', 'בית צפפה', 'גבעת הורדים',
    'מושבה גרמנית', 'ארנונה', 'המושבה היוונית', 'מקור חיים', 'קריית שמואל', 'טלביה/המוגרבים',
    'טלביה', 'תלפיות תעשיה', 'צפון תלפיות', 'פת', 'נוה שאנן/ניות', 'ימין משה', 'גונן ט',
}


def _ddmmyyyy(iso):
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", str(iso or ""))
    return f"{m.group(3)}/{m.group(2)}/{m.group(1)}" if m else None


# ── YK detail enrichment: the official §149 API has no מהות/סוג-בקשה, so pull them
#    from YK's plain JSON API (proc 242700447). Headless, no WAF; throttle = [] → backoff.
YK_API = "https://jerbasicserviceapi.jerusalem.muni.il/api/Db/ExecuteGetJSON"
YK_SYS = "26400046"
_YK = requests.Session()
_YK.headers.update({"content-type": "application/json",
                    "referer": "https://ykpubdata.jerusalem.muni.il/",
                    "origin": "https://ykpubdata.jerusalem.muni.il",
                    "user-agent": UA})


def yk_mahut(tik):
    """Return (request_description=מהות, request_type=סוג בקשה) for a tik, or ('','')."""
    for attempt in range(4):
        try:
            r = _YK.post(YK_API, json={"ProcName": 242700447, "Cnn": "cnnGisYk",
                                       "Parameters": {"tikNum": tik, "systemCode": YK_SYS}}, timeout=30)
            r.raise_for_status()
            d = r.json()
            if d:
                row = d[0]
                return (row.get("mahutBakasha") or "", row.get("teurSugbakashaCodeMulti") or "")
        except Exception:
            pass
        time.sleep(2 * (attempt + 1))  # throttle backoff
    return ("", "")


# Shared on-disk copy of the §149 archive, also written/read by
# weekly_tama38_149_scan.py. The municipal endpoint returns HTTP 500 for days at a
# time (it has been 500 continuously since at least 2026-08-16). This script used to
# let that exception escape: the scheduled task exited 0x1, objections_permits.json
# froze, and nobody was told — while its sibling scanner hit the same endpoint and
# degraded to this cache. Same source, same failure, one resilient caller; now both.
ARCHIVE_CACHE = Path(r"C:\ORANIM") / "pub_false.json"
FETCH_RETRIES = 3


def fetch():
    """Return (rows, from_cache). Retries, then falls back to the shared cache."""
    last = None
    for attempt in range(FETCH_RETRIES):
        try:
            req = urllib.request.Request(API, headers={"User-Agent": UA, "Referer": REFERER,
                                                       "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=90) as r:
                rows = json.loads(r.read().decode("utf-8"))
            if isinstance(rows, list) and rows:
                try:
                    ARCHIVE_CACHE.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
                except Exception as e:      # cache write is best-effort
                    print(f"  (could not refresh {ARCHIVE_CACHE.name}: {e})", flush=True)
                return rows, False
            last = "empty response"
        except Exception as e:
            last = e
        if attempt < FETCH_RETRIES - 1:
            time.sleep(10 * (attempt + 1))
    if ARCHIVE_CACHE.exists():
        age_d = (datetime.now() - datetime.fromtimestamp(ARCHIVE_CACHE.stat().st_mtime)).days
        print(f"§149 fetch failed ({last}); using cache {ARCHIVE_CACHE.name} "
              f"({age_d} days old) — deadlines may be stale, no NEW publications will appear.",
              flush=True)
        return json.load(open(ARCHIVE_CACHE, encoding="utf-8")), True
    raise RuntimeError(f"§149 fetch failed ({last}) and no cache at {ARCHIVE_CACHE}")


def _parse(s):
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", str(s or ""))
    return date(int(m.group(3)), int(m.group(2)), int(m.group(1))) if m else None


def _pd(iso):
    """Parse an ISO date (YYYY-MM-DD...) to a real date for correct ordering/compare."""
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", str(iso or ""))
    return date(int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else None


def build():
    raw, from_cache = fetch()
    today = date.today()
    print(f"§149 archive: {len(raw)} rows (betokef=false).", flush=True)
    # Dedup by tik, keeping the newest publication cycle = max PD900 as a REAL date
    # (a dd/mm/yyyy string compare mis-orders across months, e.g. 30/06 > 07/07).
    by_tik = {}
    for r in raw:
        tik = str(r.get("tik_num") or "").strip()
        if not tik:
            continue
        prev = by_tik.get(tik)
        if prev is None or ((_pd(r.get("PD900")) or date.min) > (_pd(prev.get("PD900")) or date.min)):
            by_tik[tik] = r
    # "Open for objections" = the objection deadline (PD900) has NOT passed. THIS is the
    # window test that replaces trusting the city's under-reporting betokef=true flag.
    open_tik = {t: r for t, r in by_tik.items() if (_pd(r.get("PD900")) or date.min) >= today}
    print(f"  with OPEN objection window (PD900 >= {today.isoformat()}): {len(open_tik)}", flush=True)

    out, dropped_out, dropped_nogeo = {}, [], []
    for tik, r in open_tik.items():
        street = (r.get("rehov") or "").strip()
        house = (r.get("misp_bait") or "").strip()
        lnglat = geocode(street, house) if street else None
        # A record we cannot geocode is usually outside our data coverage (= outside the
        # district). Require a real point INSIDE the district polygon. If it FAILS to
        # geocode yet its schn is one of OUR neighbourhoods, mark it ⚠️ — that is an
        # in-district open window we would otherwise silently lose (needs manual check).
        if not lnglat:
            flag = "⚠️IN-DISTRICT-SCHN " if r.get("schn") in CORR_SCHN else ""
            dropped_nogeo.append(f"{flag}{tik}({r.get('schn','')}/{street} {house})")
            continue
        if not in_district(lnglat):
            dropped_out.append(f"{tik}({r.get('schn','')})")
            continue
        deadline = _ddmmyyyy(r.get("PD900"))
        attrib = (attribute_point(lnglat) if lnglat else None) or {"subs": [], "tabas": []}
        mahut, sug = yk_mahut(tik)                            # מהות + סוג בקשה from YK
        time.sleep(1.2)                                      # be gentle on the YK API
        out[tik] = {
            "tik": tik,
            "status": STATUS,
            "status_date": _ddmmyyyy(r.get("ED850")),       # publication date
            "request_type": sug or (r.get("description") or ""),
            "request_description": mahut or (r.get("description") or ""),
            "address": (r.get("teur_makom") or f"{street} {house}").strip(),
            "street": street,
            "house": house,
            "schn": r.get("schn") or "",
            "deadline_publish": deadline,                    # PD900 = real objection deadline
            "deadline_verified": str(date.today()),          # straight from the official source
            "deadline_source": "muni Rishui149 api/publish PD900 (תום מועד להגשת התנגדויות)",
            "lnglat": lnglat,
            "subs": attrib.get("subs", []),
            "tabas": attrib.get("tabas", []),
            "doc_url": (DOC_URL + r.get("DocId")) if r.get("DocId") else None,
            "yk_url": f"https://ykpubdata.jerusalem.muni.il/#/Details?TikNum={tik}&SystemCode=26400046&Page=BakashalInfo",
            "source": "official_activepublications",
        }
    print(f"In district: {len(out)}  | dropped outside-polygon: {len(dropped_out)} {dropped_out}", flush=True)
    print(f"  dropped (no geocode — verify none are in-district): {dropped_nogeo}", flush=True)
    # sanity: warn on past deadlines (API is 'in-effect', so this should be ~0)
    past = [t for t, v in out.items() if (_parse(v["deadline_publish"]) or date.max) < date.today()]
    if past:
        print(f"  note: {len(past)} have a PD900 already in the past: {past}", flush=True)
    return out, from_cache


def push():
    try:
        # Via git_sync, never raw git: an unchecked `pull --rebase` on a single-line
        # JSON file wedged this repo mid-rebase for three days (2026-08-23).
        # commit_and_push_after_write aborts the rebase and reports False instead,
        # and handles the "nothing to commit" case itself.
        commit_and_push_after_write(
            "data/objections_permits.json",
            "data: objections from official §149 api (weekly)",
            APP_REPO)
        print("push: data pushed.", flush=True)
    except Exception as e:
        print(f"push skipped: {str(e)[:80]}", flush=True)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", action="store_true")
    ap.add_argument("--no-push", action="store_true")
    args = ap.parse_args()

    data, from_cache = build()
    if from_cache:
        # A stale archive must never DELETE a still-open window. The cache predates the
        # outage, so entries published after it are simply absent from `data` — replacing
        # the file would drop 2025/0513.00 (deadline 24/08/2026, still open) and the
        # report would go blank. Merge instead: keep every previously-known entry whose
        # deadline has not passed, and let cache-derived rows fill in around them.
        try:
            prev = json.load(open(OUT_PATH, encoding="utf-8"))
        except Exception:
            prev = {}
        kept = 0
        for tik, rec in prev.items():
            if tik in data:
                continue
            if (_parse(rec.get("deadline_publish")) or date.min) >= date.today():
                data[tik] = rec
                kept += 1
        if kept:
            print(f"  kept {kept} still-open entr{'y' if kept == 1 else 'ies'} from the previous file",
                  flush=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    shutil.copy(OUT_PATH, APP_COPY)
    print(f"Wrote {len(data)} district objection permits → {OUT_PATH}", flush=True)
    if from_cache:
        # Loud, and non-zero exit, so a run built from stale cache is visibly degraded
        # rather than looking like a clean weekly refresh.
        print("WARNING: built from the cached §149 archive - the municipal API was "
              "unreachable. New publications since the cache date are MISSING.", flush=True)
    for t, v in sorted(data.items(), key=lambda kv: kv[1]["deadline_publish"] or "9"):
        print(f"  {t}  {v['schn']:14} {v['address'][:34]:34}  ->  {v['deadline_publish']}", flush=True)

    if not args.no_push:
        push()
    if args.email:
        import biweekly_objections_scan as b
        b.build_and_send_email()

    if from_cache:
        # The comment above the WARNING always promised a non-zero exit, but none
        # was ever made: from 2026-08-02 the weekly task showed success and the email
        # said "0 open objections" off a frozen archive. Alert + exit 2.
        from ops_alert import send_alert
        age_d = (datetime.now() - datetime.fromtimestamp(ARCHIVE_CACHE.stat().st_mtime)).days
        send_alert(f"התנגדויות §149: נבנה ממטמון בן {age_d} ימים — {date.today().isoformat()}",
                   f"ה-API של §149 לא זמין, ו-objections_permits.json נבנה מ-{ARCHIVE_CACHE.name} "
                   f"({age_d} ימים).\n"
                   f"פרסומים חדשים מאז תאריך המטמון חסרים — גם במייל \"התנגדויות פתוחות\" "
                   f"שנשלח עכשיו.\n"
                   f"הגיבוי הזמני: weekly_yk_149_scan (יום א' 07:00) סורק את YK ישירות.")
        sys.exit(2)


if __name__ == "__main__":
    main()
