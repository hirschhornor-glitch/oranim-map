"""
fetch_permit_hakalot.py
-----------------------
הקלות שניתנו בהיתר (permit-stage variances) for building permits, straight from
YK's licensing file — the thing that explains why a permit may lawfully exceed
the plan's טבלה 5 unit count.

WHY: an approved plan's טבלה 5 fixes the יח"ד per תא שטח, but at permit stage the
applicant may publish a הקלה (§149) for more units — same floor area, smaller
apartments. When the committee approves it, the permit legitimately carries more
units than the plan. Without this the app can only show the excess as a red flag
("350 מול 296"), with no way to tell an approved הקלה from a data error.

SOURCE: proc 242700451 (see the YK JSON API notes) returns every step of the
licensing file, and the committee/appeal decisions sit in hahlataText / hearot /
hamlatzaTochen as HTML. Those texts name the published הקלות and the approved unit
count ("פורסמה הקלה להגדלת כמות יח\"ד מכח שב\"ס ... סה\"כ 72 יח\"ד בשני הבנינים").

OUTPUT: oranim-app/data/permit_hakalot.json, keyed by FULL permit number
(2023/0107.02 — decisions are per בקשה, not per תיק).

USAGE:
  py -X utf8 fetch_permit_hakalot.py              # plans whose permits exceed the plan (default)
  py -X utf8 fetch_permit_hakalot.py --all        # every permit we hold (slow: ~2s each)
  py -X utf8 fetch_permit_hakalot.py --taba 252379
  py -X utf8 fetch_permit_hakalot.py --tik 2023/0107.02
"""
import argparse
import html
import io
import json
import os
import re
import sys
import time
from datetime import datetime

import requests

ROOT = r"C:\ORANIM"
DATA = os.path.join(ROOT, "oranim-app", "data")
OUT = os.path.join(DATA, "permit_hakalot.json")
MANUAL = os.path.join(DATA, "permit_hakalot_manual.json")
SYSTEM_CODE = 26400046
URL = "https://jerbasicserviceapi.jerusalem.muni.il/api/Db/ExecuteGetJSON"
HEADERS = {
    "content-type": "application/json",
    "referer": "https://ykpubdata.jerusalem.muni.il/",
    "origin": "https://ykpubdata.jerusalem.muni.il",
    "user-agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"),
}
# Throttling answers with an empty list rather than an error — back off, don't fail.
BACKOFF = (2, 4, 6, 8)
PAUSE = 1.5
# A plan is a candidate when its permits claim more units than it plans, with a
# little slack so borderline cases still get checked.
GAP_RATIO = 1.05

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
            return []
        try:
            data = r.json()
        except ValueError:
            data = []
        if data:
            return data
    return []


def strip_html(t):
    t = re.sub(r"<[^>]+>", " ", t or "")
    t = html.unescape(t)
    return re.sub(r"\s+", " ", t).strip()


# Numbers come ONLY from the sentences that describe the הקלה, never from the
# decision's legal narrative — that one argues about interest rates and tender
# dates, and happily yields a "54" that means nothing. What the authority states:
#   "מותר על פי טבלה 5 ... לתא שטח 1 : 232 יח'ד"        → table5_units
#   "מוצע 72 יח'ד" / "שיתקבלו 70 יח'ד" / "סה'כ 280"    → allowed_units (the result)
#   "להוספת 20%" / "בשיעור של 10%"                     → pct
_TABLE5_RE = re.compile('טבלה\\s*5.{0,60}?(\\d{1,4})\\s*יח["\u05f4\']?ד')
_RESULT_RE = re.compile('(?:מוצע|מוצעים|שיתקבלו|יתקבלו|סה["\u05f4\']?כ)\\s*(\\d{1,4})\\s*יח["\u05f4\']?ד')
_PCT_RE = re.compile('(?:להוספת|תוספת של|בשיעור של)\\s*(\\d{1,3})\\s*%')
# A הקלה sentence that is ABOUT units — a parking-floor or setback הקלה does not
# change the unit count and must not be read as one.
_UNIT_WORDS = ('יח"ד', "יח\u05f4ד", "יחידות דיור", "כמות יח", "מספר יח")


# Sentences that say the opposite — "לא פורסמה הקלה", "הוועדה דוחה את הבקשה להקלה".
# 1974/0734.04 was recorded as a הקלה permit on the strength of "לא פורסמה הקלה"
# alone, with a stray 5 picked up as its unit count.
_NEGATION = ("לא פורסמה הקל", "לא פורסמו הקל", "לא הוגשה הקל", "ללא הקל", "אין הקל",
             "דוחה את הבקשה להקל", "נדחתה ההקל", "נדחו ההקל", "לא ניתן לאשר את ההקל")

# ...and what a granted/published one actually looks like.
_AFFIRMATIVE = ("פורסמה הקל", "פורסמו הקל", "פרסום הקל", "הקלות שפרסמו", "מבוקשת הקל",
                "הקלה לתוספת", "הקלה להגדלת", "הקלה בשיעור", "הקלה לשימוש")


def hakala_sentences(text):
    out = []
    for part in re.split(r"(?<=[.!?])\s+|\n", text):
        if "הקל" not in part or not any(w in part for w in _UNIT_WORDS):
            continue
        # Strip the negations, then require an affirmative marker in what is left:
        # length cutoffs do not work here — YK writes whole paragraphs as one "sentence"
        # ("...לא פורסמה הקלה... נדרש הקלה (סה"כ 5 יח"ד, מותר 6)"), and that one was
        # recorded as a granted הקלה with 5 units.
        rest = part
        for n in _NEGATION:
            rest = rest.replace(n, " ")
        if not any(a in rest for a in _AFFIRMATIVE):
            continue
        out.append(part.strip())
    return out


def read_numbers(sentences):
    """{table5_units, allowed_units, pct} exactly as the הקלה sentences state them.

    allowed_units is the RESULT the authority names ("מוצע 72", "סה'כ 280"), not
    table5 x pct: for תא שטח 1 the committee wrote 280 where 232 x 1.20 = 278.4, and
    the permit was issued on the authority's figure, not on my arithmetic.
    """
    table5, allowed, pcts = None, None, []
    for s in sentences:
        m = _TABLE5_RE.search(s)
        if m and table5 is None:
            table5 = int(m.group(1))
        for m in _RESULT_RE.finditer(s):
            v = int(m.group(1))
            if allowed is None or v > allowed:
                allowed = v
        pcts += [int(x) for x in _PCT_RE.findall(s)]
    return {"table5_units": table5, "allowed_units": allowed,
            "pct": max(pcts) if pcts else None}


def _dmy(s):
    m = re.match(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})", str(s or ""))
    return (int(m.group(3)), int(m.group(2)), int(m.group(1))) if m else (0, 0, 0)


def collect(num):
    """One permit number → its unit-הקלה record, or None when it has none.

    Two passes on purpose. The SENTENCES that describe the הקלה name the units
    ("פורסמה הקלה לשימוש בתקנת שב\"ס להוספת 20%"), but the DECISION that grants it
    usually doesn't — the 26/02/2025 approval of 2023/0107 argues about "ההקלות
    המבוקשות" with no number in sight. Requiring both in one sentence read that
    permit as "טרם אושר" although it had been approved months earlier.
    """
    steps = call(242700451, {"systemId": SYSTEM_CODE, "tikNum": num})
    published, decisions, appeal = [], [], None
    # The publication rows settle, on their own, whether a הקלה existed at all — the
    # decision text often argues about "ההקלות המבוקשות" without a number, and maslulTik
    # can lag. §149 EXECUTED = a הקלה was published; תקנה 36 alone = the request conforms
    # to the plan and the neighbours were merely notified (2022/0129.00, ברל לוקר: the
    # §149 rows stayed "מתוכן" while תקנה 36 ran, so its 143 vs 136 is not a הקלה).
    pub149, pub36 = [], []
    for s in steps:
        step_txt = " ".join(str(s.get(k) or "") for k in ("processText", "stepCodeText"))
        done = "בוצע" in str(s.get("stepStatusText") or "")
        when = s.get("execDateStr") or ""
        if done and "149" in step_txt:
            pub149.append({"step": (s.get("stepCodeText") or "").strip(), "date": when})
        elif done and "תקנה 36" in step_txt:
            pub36.append({"step": (s.get("stepCodeText") or "").strip(), "date": when})
        blob = strip_html(" ".join(str(s.get(k) or "")
                                   for k in ("hahlataText", "hearot", "hamlatzaTochen")))
        if not blob:
            continue
        verdict = strip_html(str(s.get("hahlataText") or "").split("<")[0])[:120]
        where = " ".join(str(s.get(k) or "") for k in ("vaada", "vaadaText", "processText", "stepCodeText"))
        if "ערר" in where and verdict:
            appeal = {"verdict": verdict, "date": s.get("execDateStr") or s.get("planDateStr") or ""}
        if "הקל" not in blob:
            continue
        published.extend(hakala_sentences(blob))
        decisions.append({
            "date": s.get("execDateStr") or s.get("planDateStr") or "",
            "step": s.get("stepCodeText") or s.get("processText") or "",
            "vaada": s.get("vaadaText") or "",
            "verdict": verdict,
            "text": blob,
        })
    if not published and not pub149:
        return None            # no unit-הקלה text AND no §149 publication — nothing here
    seen, uniq = set(), []
    for p in published:
        k = re.sub(r"\s+", "", p)
        if k not in seen:
            seen.add(k)
            uniq.append(p)
    decisions.sort(key=lambda d: _dmy(d["date"]))
    approving = [d for d in decisions if "לאשר" in (d["verdict"] or "")]
    # a permit can carry a §149 publication with no decision row of its own
    last = (approving[-1] if approving else (decisions[-1] if decisions else
            {"verdict": "", "date": (pub149[-1]["date"] if pub149 else ""), "vaada": "", "text": ""}))
    nums = read_numbers(uniq)
    if nums["allowed_units"] is None and nums["pct"] is None and not pub149:
        # argued about, never quantified, and never published (2024/0179.00:
        # "ההתנגדויות ... לא להקלות המבוקשות") — nothing to say.
        return None
    rec = {
        "units": nums["allowed_units"],
        "table5_units": nums["table5_units"],
        "pct": nums["pct"],
        "approved": bool(approving),
        "decision": last["verdict"],
        "decision_date": last["date"],
        "vaada": last["vaada"],
        "published": uniq[:6],
        "quote": last["text"][:600],
        # "a הקלה was published" is a weaker but far more available fact than "N units
        # were granted": it turns an unexplained gap into a documented one.
        "published_149": bool(pub149),
        "publication_149": pub149[:6],
        "publication_36": pub36[:4],
    }
    if appeal:
        rec["appeal"] = appeal
    return rec


def apply_manual(by_permit):
    """Fold in the hand-checked overlay (permit_hakalot_manual.json).

    The scrape can only see THAT a §149 was published, never on what. Reading the
    notices showed most of them are about נסיגות / קומת חניה / כניסות / שלביות and
    carry no unit count at all, while a few state one outright ("תוספת של 18 יח\"ד,
    סה\"כ מבוקש 122"). Those readings live in the overlay and win over the regex,
    so a rescan never loses them.
    """
    try:
        man = json.load(io.open(MANUAL, encoding="utf-8")).get("by_permit", {})
    except Exception as e:
        print("  (no manual overlay: %s)" % e)
        return 0
    for num, fields in man.items():
        rec = by_permit.setdefault(num, {"units": None, "table5_units": None, "pct": None,
                                         "approved": False, "decision": "", "decision_date": "",
                                         "vaada": "", "published": [], "quote": ""})
        rec.update(fields)
        rec["manual"] = True
    return len(man)


def visible(p):
    s = (p.get("status") or "").strip()
    if p.get("filtered"):
        return False
    return not ("נסגרה" in s or "בוטל" in s or s == "הגשה מקוונת הושלמה")


def gap_candidates(allp, plans):
    """(taba, [permit numbers]) for plans whose permits claim more units than planned.

    Per-tik MAX units (not sum) — deliberately crude: this only decides WHICH files to
    ask YK about; the app still applies its own inclusion rules to what comes back.
    """
    out = []
    for taba, entry in allp.items():
        props = plans.get(str(taba).strip())
        if not props or (props.get("plan_type") or "") in ("תשתיות", "מוסתר"):
            continue
        base = float(props.get("units_total") or 0) or float(props.get("units_add") or 0)
        if base <= 0:
            continue
        per_tik, nums = {}, []
        for p in entry.get("permits", []):
            if not visible(p):
                continue
            fn = p.get("file_number") or ""
            tik = fn.rsplit(".", 1)[0]
            u = int(p.get("units") or 0)
            if u:
                per_tik.setdefault(tik, set()).add(u)
            nums.append(fn)
        # DISTINCT counts per תיק, summed: revisions restating the same number count
        # once, while a תיק holding two scopes (2023/0107 = 280 in the towers + 70 in
        # the low buildings) counts both — the very case this dataset exists to explain.
        claimed = sum(sum(v) for v in per_tik.values())
        if claimed > base * GAP_RATIO:
            out.append((str(taba), nums))
    return out


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true", help="every permit in all_permits.json")
    ap.add_argument("--taba", nargs="*", help="only these plans")
    ap.add_argument("--tik", nargs="*", help="only these permit numbers")
    ap.add_argument("--fresh", action="store_true", help="ignore the existing output file")
    args = ap.parse_args()

    allp = json.load(io.open(os.path.join(DATA, "all_permits.json"), encoding="utf-8"))
    plans = {}
    for f in json.load(io.open(os.path.join(DATA, "plans.geojson"), encoding="utf-8"))["features"]:
        pr = f.get("properties") or {}
        t = str(pr.get("taba") or "").strip()
        if t and t not in plans:
            plans[t] = pr

    targets = []   # (taba, permit_number)
    if args.tik:
        # a permit can be filed under several plans; prefer a non-infrastructure host, or
        # --tik lands the record on the rail plan that merely shares the parcel
        by_num = {}
        for t, e in allp.items():
            infra = (plans.get(str(t).strip(), {}).get("plan_type") or "") in ("תשתיות", "מוסתר")
            for pp in e.get("permits", []):
                fn = pp.get("file_number")
                if not fn:
                    continue
                prev_infra = (plans.get(str(by_num.get(fn, "")).strip(), {}).get("plan_type") or "") in ("תשתיות", "מוסתר")
                if fn not in by_num or (not infra and prev_infra):
                    by_num[fn] = t
        targets = [(by_num.get(n, ""), n) for n in args.tik]
    elif args.taba:
        for t in args.taba:
            for p in (allp.get(t) or {}).get("permits", []):
                if visible(p):
                    targets.append((t, p.get("file_number")))
    elif args.all:
        for t, e in allp.items():
            for p in e.get("permits", []):
                if visible(p):
                    targets.append((t, p.get("file_number")))
    else:
        for taba, nums in gap_candidates(allp, plans):
            targets += [(taba, n) for n in nums]

    seen = set()
    targets = [(t, n) for t, n in targets if n and not (n in seen or seen.add(n))]

    out = {"generated_at": "", "source": "YK proc 242700451 (החלטות ועדה בתיק הרישוי)",
           "by_permit": {}}
    if not args.fresh and os.path.exists(OUT):
        try:
            out = json.load(io.open(OUT, encoding="utf-8"))
            out.setdefault("by_permit", {})
        except Exception:
            pass

    def save():
        out["generated_at"] = datetime.now().isoformat(timespec="seconds")
        out["count"] = len(out["by_permit"])
        with io.open(OUT, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=1)

    print("querying %d permits" % len(targets))
    found = 0
    for i, (taba, num) in enumerate(targets, 1):
        try:
            rec = collect(num)
        except Exception as e:
            print("  [%d/%d] %s — ERROR %s" % (i, len(targets), num, e))
            time.sleep(PAUSE)
            continue
        if rec:
            rec["taba"] = taba
            out["by_permit"][num] = rec
            found += 1
            print("  [%d/%d] OK %s taba=%s units=%s %s %s"
                  % (i, len(targets), num, taba, rec["units"],
                     "אושר" if rec["approved"] else "טרם", rec["decision_date"]))
            for s in rec["published"][:2]:
                print("        . " + s[:110])
        else:
            out["by_permit"].pop(num, None)
            print("  [%d/%d] -  %s" % (i, len(targets), num))
        time.sleep(PAUSE)
        if i % 25 == 0:      # checkpoint — a long --all run must not lose its work
            save()

    n_manual = apply_manual(out["by_permit"])
    out["generated_at"] = datetime.now().isoformat(timespec="seconds")
    out["count"] = len(out["by_permit"])
    out["manual_count"] = n_manual
    with io.open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    print("\nWrote %s — %d permits with a unit-הקלה this run, %d in file"
          % (OUT, found, out["count"]))


if __name__ == "__main__":
    main()
