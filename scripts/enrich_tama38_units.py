"""
enrich_tama38_units.py
----------------------
Collect the יח"ד + floors breakdown for every building we track as תמ"א 38, from YK:

  PRIMARY  — the STRUCTURED "שטחים" table (proc 242700454): rows with
             descrShetah / kayam(קיים) / tosefet(תוספת) / sumSqr(סה"כ).
             "מספר כולל של יחידות דיור" row  → נכנס=kayam, תוספת=tosefet, יוצא=sumSqr
             "מספר קומות ..." row            → קומות=sumSqr
  FALLBACK — existing (נכנס) is often blank in the table; recover it from the rich
             free-text `teur` (proc 242700447): "...קיימות בו 26 יח"ד", "בעל 9 יח"ד".

Fill-only into tama38.geojson (never overwrites a value the city already has).
Resume-able: re-running skips buildings already collected in the audit. Saves the
audit incrementally so a throttle/kill loses nothing.

Usage:
  python enrich_tama38_units.py --sample 15   # validate (no write to geojson)
  python enrich_tama38_units.py               # full run, fill geojson at the end
"""
import argparse, io, json, os, re, sys, time
import requests

ROOT = r"C:\ORANIM"
DATA = ROOT + r"\oranim-app\data"
GEO = DATA + r"\tama38.geojson"
TP = DATA + r"\tama38_permits.json"
OUT = ROOT + r"\tama38_units_enrich.json"

API = "https://jerbasicserviceapi.jerusalem.muni.il/api/Db/ExecuteGetJSON"
SYS = "26400046"

# Manually verified (2026-07-29) — final authority, applied after the auto-computation so a
# forced full re-run never reverts them. These are 38/2 demolitions where the 454 table has
# k=0 and the teur's "ובו N" describes the NEW building (existing genuinely unknown → נכנס=None),
# plus אלבק 15 where the 454 table misses a teur "+2 קומה חדשה". Keyed by tik.
# Values are (נכנס, יוצא) or (נכנס, יוצא, קומות) — the 3rd element also overrides the
# unreliable "מספר קומות" row (applicants fill it as total OR as addition, inconsistently).
MANUAL_UNITS = {
    "2023/0369.00": (None, 34),   # ש. טשרניחובסקי 32 (38/2): ?→34
    "2018/0684.00": (None, 8),    # יאיר 7 (38/2): ?→8
    "2017/0592.00": (None, 4),    # שדרות בן מימון (38/2): ?→4
    "2012/0205.02": (10, 12),     # אלבק 15 (38/1): teur קומה חדשה +2
    "2023/0008.00": (None, 17),   # רמב"ן 29 (38/2): ?→17 (teur "ובו 17" = בניין חדש)
    # הרב הרצוג 6 (38/1) — read off the official הרמוניקה (2026/0190.00, doc "הרמוניקה
    # -הגשה מקוונת"): "טבלת הוספת שטחים במסגרת תמ"א 38" lists דירות 1-8 on קומות 0-3 as
    # EXISTING (each getting ~24 מ"ר), and דירות 9-12 as new (קומה 4 ×2, קומה 5, מרתף).
    # חישוב שטחים lists מפלסים -2.66/0/+3/+6/+8.98/+12/+15.20/+18.40 → 4 existing floors
    # (0-3) + קומות 4,5 + קומת גג = 7 above ground. YK's 454 table reported 0/4/4 — the
    # תוספת only. Verified 2026-08-20.
    "2026/0190.00": (8, 12, 7),
}
H = {"content-type": "application/json", "referer": "https://ykpubdata.jerusalem.muni.il/",
     "origin": "https://ykpubdata.jerusalem.muni.il",
     "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
S = requests.Session(); S.headers.update(H)


def call(proc, params, retries=6):
    """Throttle-aware: empty list = soft-fail, backoff and retry (3/6/9/12/15s)."""
    for a in range(retries):
        try:
            r = S.post(API, json={"ProcName": proc, "Cnn": "cnnGisYk", "Parameters": params}, timeout=30)
            r.raise_for_status()
            d = r.json()
            if d:
                return d
        except Exception:
            pass
        time.sleep(3 * (a + 1))
    return None


def as_num(v):
    if v in (None, "", "-"):
        return None
    try:
        f = float(str(v).replace(",", ""))
        if f < 0:                       # YK's שטחים table uses -1 as a null sentinel
            return None
        return int(f) if f == int(f) else f
    except Exception:
        return None


def is_add_only(existing, total, added, is_harisa, tot_from_teur):
    """True when the 454 total is really just the ADDITION, so it must NOT be published
    as יוצא. The שטחים table always satisfies sumSqr == kayam + tosefet (verified on all
    76 files where kayam is populated), so a 38/1 row with a blank kayam yields
    sumSqr == tosefet — the existing units are simply missing from the table, and the
    "total" understates the building by exactly the (unknown) existing count.
    Ground truth: הרב הרצוג 6 — table 0/4/4, actual 8→12 (see MANUAL_UNITS)."""
    return (not is_harisa and not tot_from_teur and existing is None
            and total is not None and added is not None and total == added)


def resolve_out(existing, total, added, is_harisa, tot_from_teur=False):
    """יח"ד יוצא. An EXPLICIT total that is ≥ existing (e.g. the teur's "סה"כ 40 יח"ד")
    is the most trustworthy — the 454 שטחים table is sometimes wrong/blank (records the
    delta as the total, or 0). Otherwise:
      • 38/2 (הריסה ובניה): the שטחים total is the new-building count.
      • 38/1 (תוספות):       out = existing + added (the 454 table records existing as 0,
                              so its total under-counts → out<existing, the bug).
      • 38/1 with existing UNKNOWN and total == added: the total is add-only and there is
        no honest יוצא — return None and let the caller publish the תוספת instead.
    Guarantees out ≥ existing whenever the data allows (תמ"א always adds)."""
    if is_add_only(existing, total, added, is_harisa, tot_from_teur):
        return None
    if total is not None and (existing is None or total >= existing):
        return total
    if is_harisa:
        return total
    if existing is not None and added is not None:
        return existing + added
    return total


def from_shtachim(rows):
    """Return (existing, total, added, floors, add_explicit) from the structured שטחים table.
    The table's קיים (existing) column is usually 0/blank even when the building has
    existing units → treat 0 as None so we fall back to the prose for נכנס.
    MULTIPLE יח"ד rows are common — a full one (k=19 t=6 s=25) plus a zero-summary
    duplicate (k=19 t=0 s=19). Taking the LAST row undercounts (19→19 instead of 19→25),
    so pick the row with the LARGEST sumSqr (the fullest picture).
    add_explicit = the chosen row carried a real (non-null) תוספת column — so תוספת==0
    is an AUTHORITATIVE 'no units added' (חיזוק-only), not a missing value."""
    floors = None
    best = None
    for r in rows or []:
        d = str(r.get("descrShetah") or "")
        if "יחידות דיור" in d:
            s = as_num(r.get("sumSqr")) or 0
            if best is None or s > (as_num(best.get("sumSqr")) or 0):
                best = r
        elif "קומות" in d and floors is None:
            floors = as_num(r.get("sumSqr")) or as_num(r.get("tosefet"))
    if best is None:
        return None, None, None, floors, False
    existing = as_num(best.get("kayam")) or None            # 0 → None (blank)
    added = as_num(best.get("tosefet"))
    total = as_num(best.get("sumSqr"))
    add_explicit = str(best.get("tosefet")).strip() not in ("", "None")
    return existing, total, added, floors, add_explicit


def clean_teur(h):
    if not h:
        return ""
    h = re.sub(r"(?is)<(style|head|script).*?</\1>", " ", h)
    t = re.sub(r"<[^>]+>", " ", h)
    t = t.replace("&nbsp;", " ").replace("&quot;", '"').replace("&amp;", "&").replace("&#39;", "'")
    t = re.sub(r"&#x[0-9a-fA-F]+;", "", t)
    return re.sub(r"\s+", " ", t).strip()


_YHD = re.compile(r'(\d{1,4})\s*יח(?:["\u05f4\u2033\'\u05f3]?\s*ד|ידות\s*דיור|ידות)')
_EXIST = re.compile(r'קיימ|בעל|בו\s*$|הקיים')
_NEW = re.compile(r'מוצע|מבוקש|חדש|התווספ|תוספת|סה')


_TOTAL = re.compile(r'סה"?כ|בבניין החדש|במבנה החדש')
# The new-building count is very often phrased WITHOUT "סה"כ": "מוצעות 43 יח"ד בבניין",
# "הכולל 34 יח"ד", "יוקמו N יח"ד", "26 יח"ד חדשות". _PROPOSED matches such context BEFORE
# the number; _ISNEW matches it AFTER (חדש/חדשות/בבניין/במבנה). For 38/2 this count is the
# new-building TOTAL (= יוצא); for 38/1 it's the ADDED units (יוצא = קיים + תוספת).
_PROPOSED = re.compile(r'מוצע|יוקמו|יבנו|יוצע|הכולל|כולל|יוקם')
_ISNEW = re.compile(r'^\s*(חדש|חדשות|בבני|במבנה|בבנין)')
_KOMOT = re.compile(r'(\d{1,2}(?:\.\d)?)\s*קומ(?:ות|ה)')


def units_from_teur(t):
    """Best-effort (existing, total, new_units, floors) + source sentence from the prose —
    used when the structured table is missing (older permits) or blank.
      • total     = an EXPLICIT "סה"כ N" figure (highest trust).
      • new_units = the new-building count from מוצעות/הכולל/N-חדשות phrasing (the largest
                    such figure). Caller maps it to יוצא (38/2) or תוספת (38/1) by tama type.
      • existing  = a "קיים … ובו N" figure NOT immediately followed by חדש/בבניין."""
    if not t:
        return None, None, None, None, ""
    existing = total = new_units = None
    src = ""
    for m in _YHD.finditer(t):
        n = int(m.group(1))
        pre = t[max(0, m.start() - 30):m.start()]
        post = t[m.end():m.end() + 14]
        # "חדש/בבניין" right AFTER the number is decisive; a proposed-verb BEFORE only counts
        # when the same window isn't describing the EXISTING building ("קיים … הכולל N").
        is_new = bool(_ISNEW.search(post) or (_PROPOSED.search(pre) and not _EXIST.search(pre)))
        if _TOTAL.search(pre) and total is None:
            total = n
        elif is_new:
            if new_units is None or n > new_units:   # new building = the largest proposed figure
                new_units = n
        elif _EXIST.search(pre) and not _NEW.search(pre) and existing is None:
            existing = n
            src = t[max(0, m.start() - 30):m.end() + 10].strip()
    floors = None
    fl = [float(m.group(1)) for m in _KOMOT.finditer(t)]
    if fl:
        floors = int(max(fl)) if max(fl) == int(max(fl)) else max(fl)
    return existing, total, new_units, floors, src


def existing_from_teur(t):
    """Best-effort existing-units from prose; returns (n, source_sentence) or (None,'')."""
    for m in _YHD.finditer(t):
        pre = t[max(0, m.start() - 30):m.start()]
        if _EXIST.search(pre) and not _NEW.search(pre):
            return int(m.group(1)), (t[max(0, m.start() - 30):m.end() + 10]).strip()
    return None, ""


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=0)
    args = ap.parse_args()

    geo = json.load(io.open(GEO, encoding="utf-8"))
    tp = json.load(io.open(TP, encoding="utf-8"))
    audit = json.load(io.open(OUT, encoding="utf-8")) if os.path.exists(OUT) and not args.sample else {}

    targets = []
    for f in geo["features"]:
        p = f["properties"]
        fid = str(p.get("fid"))
        perms = (tp.get(fid) or {}).get("permits") or []
        if not perms:
            continue
        perms_sorted = sorted(perms, key=lambda x: ("הופק" in (x.get("status") or ""), x.get("file_number") or ""), reverse=True)
        targets.append((f, fid, perms_sorted[0].get("file_number")))

    # Recent permits first — that's where the structured שטחים table (454) actually
    # exists; old permits predate it and yield little. Also validates 454 early.
    targets = sorted(targets, key=lambda t: t[2] or "", reverse=True)
    if args.sample:
        targets = targets[:args.sample]
    else:
        targets = [t for t in targets if str(t[1]) not in audit]  # resume
    print(f"targets: {len(targets)} (audit already has {len(audit)})")

    fails = 0
    for i, (f, fid, tik) in enumerate(targets):
        # STRUCTURED שטחים (proc 454 needs {SystemId, TikNum}) → יוצא(total) + קומות
        rows = call(242700454, {"SystemId": SYS, "TikNum": tik})
        time.sleep(2.5)
        got454 = bool(rows)
        ex, tot, add, fl, add_explicit = from_shtachim(rows)
        src = ""
        det = None
        t_new = None
        tot_from_teur = False
        # Always read the prose too: 454's קיים is unreliable (נכנס comes from teur), and
        # its total is sometimes wrong/blank — the teur's explicit "סה"כ N יח"ד" wins.
        if ex is None or tot is None or fl is None or not got454:
            det = call(242700447, {"tikNum": tik, "systemCode": SYS})
            time.sleep(2.5)
            if det:
                teur = clean_teur(det[0].get("teur"))
                t_ex, t_tot, t_new, t_fl, src = units_from_teur(teur)
                if ex is None:
                    ex = t_ex
                if t_tot is not None and (tot is None or t_tot >= (tot or 0)):
                    tot = t_tot; tot_from_teur = True   # explicit teur total is authoritative
                    # (>= not >: a teur "סה"כ N" that merely CONFIRMS the 454 total still
                    # vouches for it — e.g. אלפסי 18, a real 5→2 luxury rebuild — so it counts
                    # as a legit no-add, not a suspicious out≤in parse gap.)
                if fl is None:
                    fl = t_fl
        # detail_ok = we actually reached YK for this tik (structured rows OR a detail row).
        # If BOTH came back empty it's throttle/failure → DON'T record, so resume retries it.
        detail_ok = got454 or bool(det)
        if not detail_ok:
            fails += 1
            print(f"  {i+1:3}/{len(targets)} {tik:14} THROTTLED/empty — skip (will retry). consec={fails}")
            if fails % 3 == 0:
                cd = min(300, 90 * (fails // 3))   # escalating cooldown to let YK recover
                print(f"  ...cooldown {cd}s (YK throttle)"); time.sleep(cd)
            continue
        fails = 0
        # יוצא by tama-type semantics (see resolve_out). is_harisa = 38/2 (demolition).
        req = " ".join((pp.get("request_type") or "") + " " + (pp.get("request_description") or "") for pp in perms)
        is_harisa = (f["properties"].get("harisaa") == "כן") or ("הריסה ובני" in req) or ("הריסת" in req)
        # The teur's new-building count (מוצעות/הכולל/N-חדשות): for 38/2 it's the new TOTAL
        # (יוצא — the old building is demolished); for 38/1 it's the ADDED units. Apply it only
        # where the structured table left a gap, so 454 stays authoritative when present.
        if t_new is not None:
            if is_harisa:
                if tot is None or t_new > tot:
                    tot = t_new; tot_from_teur = True
            elif add is None:
                add = t_new
        add_only = is_add_only(ex, tot, add, is_harisa, tot_from_teur)
        out = resolve_out(ex, tot, add, is_harisa, tot_from_teur)
        manual = tik in MANUAL_UNITS
        if manual:
            mu = MANUAL_UNITS[tik]
            ex, out = mu[0], mu[1]
            if len(mu) > 2:
                fl = mu[2]
            add_only = False
        # out ≤ in: an ERROR (needs review) UNLESS an explicit teur total confirms it — then
        # it's a legit no-added-units permit (חיזוק / conservation rebuild), not a bug.
        under = (ex is not None and out is not None and out <= ex)
        # out ≤ in is CONFIRMED-legit (no_add, not a suspicious gap) when either an explicit
        # teur "סה"כ N" vouches for it, OR the 454 table explicitly recorded תוספת==0 (a
        # חיזוק-only permit). Otherwise it's a real ⚠️ parse/data gap needing review.
        confirmed = tot_from_teur or (add_explicit and add == 0) or manual
        review = under and not confirmed
        no_add = under and confirmed
        rec = {"tik": tik, "address": f["properties"].get("address"),
               "in_units": ex, "out_units": out, "added": add, "sum_sqr": tot, "floors": fl,
               "add_only": add_only,
               "tama_type": "38/2" if is_harisa else "38/1", "review": review, "no_add": no_add,
               "existing_src": src, "table_rows": len(rows or []), "detail_ok": True}
        audit[fid] = rec
        print(f"  {i+1:3}/{len(targets)} {tik:14} {(f['properties'].get('address') or '')[:22]:22} נכנס={ex} יוצא={out} קומות={fl}" + (" ⚠לבדיקה" if review else "") + (" (teur)" if src else ""))
        if args.sample:
            for r in (rows or []):
                dd = str(r.get('descrShetah') or '')
                if 'יחיד' in dd or 'קומות' in dd:
                    print(f"        · {dd[:30]} | קיים={r.get('kayam')} תוספת={r.get('tosefet')} סהכ={r.get('sumSqr')}")
        if not args.sample and (i + 1) % 10 == 0:      # incremental save
            json.dump(audit, io.open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    json.dump(audit, io.open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"audit -> {OUT} ({len(audit)} buildings)")

    if not args.sample:
        # Write to DEDICATED fields (not the city's unreliable units_exis/units_tose):
        # units_in = נכנס, units_out = יוצא, floors_tama = קומות. The app popup reads these.
        fe = ft = ffl = 0
        for f in geo["features"]:
            p = f["properties"]; rec = audit.get(str(p.get("fid")))
            if not rec:
                continue
            # Our own fields — overwrite from the audit (source of truth), not fill-only,
            # so re-processed corrections propagate.
            # units_add is published ONLY for add-only files (no honest יוצא) — elsewhere
            # the addition is just out-in and would duplicate the pair.
            rec = dict(rec, add_units=(rec.get("added") if rec.get("add_only") else None))
            for prop, key in (("units_in", "in_units"), ("units_out", "out_units"),
                              ("units_add", "add_units"), ("floors_tama", "floors")):
                if rec.get(key) is not None:
                    p[prop] = rec[key]
                    if prop == "units_in": fe += 1
                    elif prop == "units_out": ft += 1
                    elif prop == "floors_tama": ffl += 1
                else:
                    p.pop(prop, None)
            for prop, key in (("units_review", "review"), ("units_no_add", "no_add")):
                if rec.get(key):
                    p[prop] = True
                else:
                    p.pop(prop, None)
        json.dump(geo, io.open(GEO, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
        print(f"filled tama38.geojson (dedicated): units_in={fe} units_out={ft} floors_tama={ffl}")


if __name__ == "__main__":
    main()
