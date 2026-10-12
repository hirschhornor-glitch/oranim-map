"""
fetch_permit_quantities.py
--------------------------
The permit's OWN numbers — יח"ד, קומות, שטחים — from YK's quantities table
(proc 242700454), instead of a regex over the free-text מהות הבקשה.

WHY: until now a permit's unit count came from `enrich_permits.py` reading
sentences like "סה\"כ 70 יח\"ד" out of the description. That works only when the
clerk wrote the number, it cannot tell existing units from added ones, and it
gives no floor count at all. Proc 242700454 returns the table the authority
itself keeps for the permit:

    {"descrShetah": "מספר כולל של יחידות דיור", "kayam": 0, "tosefet": 70, "sumSqr": 70}
    {"descrShetah": "מספר קומות ללא מרתף",       "kayam": 0, "tosefet": 9,  "sumSqr": 9}
    {"descrShetah": "שטח עיקרי. ובשימוש מעורב - מגורים", ...}

Plain HTTP JSON, no Playwright and no WAF — it supersedes the browser-driven
`scrape_permit_areas.py` (Layer 3), which fetched the same "שטחים" tab by driving
the SPA and only ever filled permits that had no units yet.

WRITES:
  * oranim-app/data/permit_quantities.json  — the full table per permit (cache +
    the areas rows, for whatever needs them next)
  * all_permits.json / tama38_permits.json  — `units` (authoritative), plus
    `units_existing`, `units_added`, `floors`, `units_source='yk_quantities'`.
    The old regex value is preserved as `units_text` whenever the two disagree,
    so a wrong overwrite stays visible instead of silently replacing the number.

Incremental by default: a permit already in the cache is not queried again.

USAGE:
  py -X utf8 fetch_permit_quantities.py                 # live root copies, new permits only
  py -X utf8 fetch_permit_quantities.py --data          # the published oranim-app/data copies
  py -X utf8 fetch_permit_quantities.py --refresh       # re-query everything
  py -X utf8 fetch_permit_quantities.py --tik 2023/0107.02
"""
import argparse
import io
import json
import os
import sys
import time
from datetime import datetime

import requests

ROOT = r"C:\ORANIM"
DATA = os.path.join(ROOT, "oranim-app", "data")
CACHE = os.path.join(DATA, "permit_quantities.json")
SYSTEM_CODE = 26400046
URL = "https://jerbasicserviceapi.jerusalem.muni.il/api/Db/ExecuteGetJSON"
HEADERS = {
    "content-type": "application/json",
    "referer": "https://ykpubdata.jerusalem.muni.il/",
    "origin": "https://ykpubdata.jerusalem.muni.il",
    "user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"),
}
# An empty answer is ambiguous here: it means "throttled" OR "this permit simply has no
# quantities table" — plenty of 1970s permits have none. Two short retries, not four long
# ones: at 20s per empty the run took 50 minutes, almost all of it waiting on permits that
# were never going to answer. A real throttle shows up as a RUN of empties, so we slow down
# only after several in a row (see EMPTY_STREAK_PAUSE).
BACKOFF = (2, 4)
EMPTY_STREAK_PAUSE = 20      # seconds to cool off after this many consecutive empties
EMPTY_STREAK_TRIGGER = 5
PAUSE = 1.2
CHECKPOINT_EVERY = 25

# The row labels we lift into named fields. Everything else is kept verbatim in `rows`.
UNITS_LABEL = "מספר כולל של יחידות דיור"
FLOORS_LABELS = ("מספר קומות ללא מרתף", "מספר קומות")

SESSION = requests.Session()


def call(proc, params):
    for wait in (0,) + BACKOFF:
        if wait:
            time.sleep(wait)
        r = SESSION.post(URL, headers=HEADERS,
                         json={"ProcName": proc, "Cnn": "cnnGisYk", "Parameters": params},
                         timeout=60)
        if r.status_code != 200:
            print("    HTTP %s — %s" % (r.status_code, r.text[:80]))
            return None
        try:
            data = r.json()
        except ValueError:
            data = []
        if data:
            return data
    return []            # genuinely empty (many old permits have no quantities table)


def num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    # YK writes -1 where the field does not apply ("units=-1" on a permit for a
    # תחנת מיישרים, a road, a tennis court). It is "no number", not minus one unit.
    return None if f == -1 else f


def quantities(num_str):
    rows = call(242700454, {"systemId": SYSTEM_CODE, "tikNum": num_str})
    if rows is None:
        return None                      # transport failure — do NOT cache
    rec = {"rows": [], "units": None, "units_existing": None, "units_added": None,
           "floors": None, "fetched_at": datetime.now().isoformat(timespec="seconds")}
    for r in rows:
        label = (r.get("descrShetah") or "").strip()
        kayam, tosefet, total = num(r.get("kayam")), num(r.get("tosefet")), num(r.get("sumSqr"))
        rec["rows"].append({"label": label, "kayam": kayam, "tosefet": tosefet, "total": total})
        if label == UNITS_LABEL:
            rec["units"] = total
            rec["units_existing"] = kayam
            rec["units_added"] = tosefet
        elif label in FLOORS_LABELS and rec["floors"] is None:
            rec["floors"] = total
    return rec


def visible(p):
    s = (p.get("status") or "").strip()
    if p.get("filtered"):
        return False
    return not ("נסגרה" in s or "בוטל" in s or s == "הגשה מקוונת הושלמה")


# A request that ADDS to a standing building, as opposed to one that builds (or
# rebuilds) the whole thing. It matters because YK fills the quantities table two
# different ways: a new building gets קיים=0 / תוספת=all, while an addition often
# gets קיים=0 / תוספת=<just this request> and leaves the building total out — the
# number the description does carry ("תוספת שתי יח\"ד ... סה\"כ 16 יח\"ד").
# Only phrases that really mean "a building rises here" — a bare "בניית"/"הקמת" stem
# also matches "בניית מרפסת" and "הקמת מחסן", which had 2017/0972.01 ("תוספת שתי קומות
# ... סה"כ 18 יח"ד", type "בניית מרפסת, ... תוספת יח\"ד") read as a new building and
# counted at 6 instead of 18.
_NEW_BUILD = ("בניה חדשה", "בנייה חדשה", "בניין חדש", "בנין חדש", "הריסה ובני",
              "פינוי בינוי", "פינוי ובינוי", "בניין חדש במקום", "הקמת בניין", "הקמת מבנה",
              "בניית בניין", "בניית מבנה")
# Kept loose on purpose, since _NEW_BUILD above already carries the exclusion: the
# earlier contiguous forms ("תוספת קומ") missed "תוספת שלוש קומות ... לכדי סה"כ 190
# יח"ד" and "בקשה לשינוי להיתר ... עבור בנין מגורים 105 יח"ד", and both plans lost
# their real count to a 2-unit and a 1-unit revision row.
_ADDITION = ("תוספת", "הרחבה", "שינוי", "חידוש היתר", "הכשרת")


def is_addition(p):
    t = (p.get("request_type") or "") + " " + (p.get("request_description") or "")
    return any(w in t for w in _ADDITION) and not any(w in t for w in _NEW_BUILD)


def as_int(v):
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return int(round(f))


def apply_to_store(path, cache, label):
    """Write the authoritative numbers into a permits store. Returns a change report."""
    try:
        data = json.load(io.open(path, encoding="utf-8"))
    except Exception as e:
        print("  skip %s (%s)" % (path, e))
        return {}
    changed, disagree, floors_added = 0, [], 0
    for key, entry in data.items():
        for p in entry.get("permits", []):
            rec = cache.get(p.get("file_number"))
            if not rec:
                continue
            kayam = as_int(rec.get("units_existing")) or 0
            tosefet = as_int(rec.get("units_added")) or 0
            units = as_int(rec.get("units"))
            if kayam > 0:
                units = kayam + tosefet          # the building as this permit leaves it
            text_units = as_int(p.get("units_text"))
            if text_units is None and p.get("units_source") != "yk_quantities":
                text_units = as_int(p.get("units"))
            if (units is not None and kayam == 0 and is_addition(p)
                    and text_units and text_units > units):
                # the table holds only this request's delta while the description states
                # the building's total — and the plan counts buildings, not deltas.
                # (2014/0821.01: table 2, text "תוספת שתי יח\"ד ... סה\"כ 16 יח\"ד")
                units = text_units
            if units is not None and units > 0:
                old = as_int(p.get("units"))
                if old is not None and old != units and p.get("units_source") != "yk_quantities":
                    # keep what the text said — a silent overwrite would hide a bad read
                    # on either side, and the two disagreeing is itself worth seeing
                    p["units_text"] = old
                    disagree.append((key, p.get("file_number"), old, units))
                if old != units:
                    changed += 1
                p["units"] = units
                p["units_source"] = ("description+yk" if text_units and units == text_units
                                     and text_units != as_int(rec.get("units")) else "yk_quantities")
                if rec.get("units_existing") is not None:
                    p["units_existing"] = as_int(rec["units_existing"])
                if rec.get("units_added") is not None:
                    p["units_added"] = as_int(rec["units_added"])
            floors = as_int(rec.get("floors"))
            if floors is not None and floors > 0 and p.get("floors") != floors:
                p["floors"] = floors
                p["floors_source"] = "yk_quantities"
                floors_added += 1
    with io.open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("  %s: %d units set, %d floors set, %d disagreed with the text"
          % (label, changed, floors_added, len(disagree)))
    return {"changed": changed, "floors": floors_added, "disagree": disagree}


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", action="store_true",
                    help="work on the published oranim-app/data copies instead of the live root")
    ap.add_argument("--refresh", action="store_true", help="re-query permits already cached")
    ap.add_argument("--tik", nargs="*", help="only these permit numbers")
    ap.add_argument("--tama38", action="store_true", help="include the תמ\"א 38 store as well")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--apply-only", action="store_true",
                    help="skip the API entirely; just re-apply the cached numbers to the stores")
    args = ap.parse_args()

    if args.data:
        all_path = os.path.join(DATA, "all_permits.json")
        tama_path = os.path.join(DATA, "tama38_permits.json")
    else:
        all_path = os.path.join(ROOT, "all_permits.json")
        tama_path = os.path.join(ROOT, "tama38_permits.json")

    cache_doc = {"generated_at": "", "source": "YK proc 242700454 (טבלת שטחים/כמויות של ההיתר)",
                 "by_permit": {}}
    if os.path.exists(CACHE):
        try:
            cache_doc = json.load(io.open(CACHE, encoding="utf-8"))
            cache_doc.setdefault("by_permit", {})
        except Exception:
            pass
    cache = cache_doc["by_permit"]
    # normalise any -1 sentinels cached before that rule existed
    for _rec in cache.values():
        for _f in ("units", "units_existing", "units_added", "floors"):
            if _rec.get(_f) == -1:
                _rec[_f] = None


    stores = [(all_path, "all_permits")]
    if args.tama38:
        stores.append((tama_path, "tama38_permits"))

    targets = []
    for path, label in stores:
        try:
            data = json.load(io.open(path, encoding="utf-8"))
        except Exception as e:
            print("skip %s (%s)" % (path, e))
            continue
        for entry in data.values():
            for p in entry.get("permits", []):
                fn = p.get("file_number")
                if not fn or not visible(p):
                    continue
                if args.tik and fn not in args.tik:
                    continue
                if not args.refresh and not args.tik and fn in cache:
                    continue
                targets.append(fn)
    targets = list(dict.fromkeys(targets))
    if args.limit:
        targets = targets[:args.limit]

    def save_cache():
        cache_doc["generated_at"] = datetime.now().isoformat(timespec="seconds")
        cache_doc["count"] = len(cache)
        with io.open(CACHE, "w", encoding="utf-8") as f:
            json.dump(cache_doc, f, ensure_ascii=False, indent=1)

    if args.apply_only:
        targets = []
        print("--apply-only: re-applying %d cached permits" % len(cache))
    print("querying %d permits (cached: %d)" % (len(targets), len(cache)))
    got = empty = 0
    streak = 0
    for i, fn in enumerate(targets, 1):
        try:
            rec = quantities(fn)
        except Exception as e:
            print("  [%d/%d] %s — ERROR %s" % (i, len(targets), fn, e))
            time.sleep(PAUSE)
            continue
        if rec is None:
            print("  [%d/%d] %s — transport failure, not cached" % (i, len(targets), fn))
        else:
            cache[fn] = rec
            if rec["units"] or rec["floors"]:
                got += 1
                print("  [%d/%d] %s units=%s floors=%s"
                      % (i, len(targets), fn, rec["units"], rec["floors"]))
            else:
                empty += 1
        streak = streak + 1 if (rec is not None and not rec["units"] and not rec["floors"]) else 0
        if streak and streak % EMPTY_STREAK_TRIGGER == 0:
            # a run of empties looks like throttling rather than a run of old permits
            print("    (%d empties in a row — cooling off %ds)" % (streak, EMPTY_STREAK_PAUSE))
            time.sleep(EMPTY_STREAK_PAUSE)
        time.sleep(PAUSE)
        if i % CHECKPOINT_EVERY == 0:
            save_cache()
    save_cache()
    print("cache: %d permits (%d new with numbers, %d with an empty table)"
          % (len(cache), got, empty))

    for path, label in stores:
        apply_to_store(path, cache, label)


if __name__ == "__main__":
    main()
