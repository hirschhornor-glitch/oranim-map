"""
build_plan_raisers.py
---------------------
תכניות מאוחרות שמגדילות תכנית קודמת — precomputed, so the app does not have to
wait for its overlap worker.

WHY: above 101-0250563 (משתלת חוות הנוער, 316 יח"ד) sits 101-1021344 "הגדלת זכויות -
המשתלה", which takes 316 as its incoming and raises the ceiling to 361 — and the permits
are indeed for 362. Without knowing that, every הגדלת זכויות reads as the plan beneath it
exceeding itself. The app can derive this from its overlap map, but that map is built in a
worker and takes 20-45 seconds; until then the column is empty. This writes the relation
once, so the report has it the moment it opens (and so Python-side reports can use it too).

THE TEST (same one the app applies, kept in sync):
  * spatial overlap >= 50% of the smaller plan
  * the raiser is approved no earlier than the plan it raises
  * its units_total is larger
  * AND either its units_in equals the earlier plan's units_total (the giveaway — the
    later plan literally declares the earlier one's total as its own incoming), or its
    type/name says הגדלת זכויות / עיבוי.

Output: oranim-app/data/plan_raisers.json
  { "by_taba": { "250563": {"taba": "1021344", "plan_name": "101-1021344",
                            "units_total": 361, "raise": 45, "overlap": 1.0, ...} } }
Only the strongest raiser per plan is kept (highest units_total).

Run: py -X utf8 build_plan_raisers.py
"""
import io
import json
import os
import re
import sys
from datetime import datetime

from shapely.geometry import shape

ROOT = r"C:\ORANIM"
DATA = os.path.join(ROOT, "oranim-app", "data")
PLANS = os.path.join(DATA, "plans.geojson")
OUT = os.path.join(DATA, "plan_raisers.json")

MIN_OVERLAP = 0.50
INFRA_TYPES = {"תשתיות", "מוסתר"}
RAISER_KINDS = ("זכויות", "עיבוי")
# מראה של קבוצת rejected ב-STATUS_GROUP_DEFS שב-src/app.jsx (מקור יחיד לקיבוץ סטטוסים)
REJECTED = ("נגנזה", "נדחתה")


def num(v):
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def year(props):
    m = re.search(r"(\d{4})", str(props.get("mavat_date") or ""))
    return int(m.group(1)) if m else 0


def is_rejected(pr):
    """תכנית שנגנזה או נדחתה אינה צד תקף ליחס "מגדילה".

    בשני הכיוונים, ומשתי סיבות שונות: בסיס שנגנז מעולם לא אושר ולכן אין לו תקרת
    יח"ד או פרוגרמה ציבורית שמישהו "מגדיל", ומגדילה שנדחתה לא העניקה את היח"ד
    שהיא מיוחסת כמי שהוסיפה. עד התיקון הזה הקובץ זיהה חפיפה, תאריך וסך יח"ד ולא
    בדק סטטוס כלל: 9 זוגות זוכו ב-144 יח"ד ממגדילות שנגנזו או נדחו, ו-20 מתוך 47
    המגדילות ישבו רק על בסיסים שנגנזו.
    """
    s = str((pr or {}).get("status_mavat") or "")
    return any(r in s for r in REJECTED)


def load_plans():
    out = {}
    gj = json.load(io.open(PLANS, encoding="utf-8"))
    for f in gj["features"]:
        pr = f.get("properties") or {}
        taba = str(pr.get("taba") or "").strip()
        if not taba or taba in out:
            continue
        if (pr.get("plan_type") or "").strip() in INFRA_TYPES:
            continue
        if is_rejected(pr):
            continue
        try:
            geom = shape(f["geometry"])
            if geom.is_empty:
                geom = None
        except Exception:
            geom = None
        out[taba] = (pr, geom)
    return out


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    plans = load_plans()
    items = [(t, pr, g) for t, (pr, g) in plans.items() if g is not None]
    print("plans with geometry: %d" % len(items))

    by_taba = {}
    for taba, pr, geom in items:
        base = num(pr.get("units_total"))
        if base <= 0:
            continue
        best = None
        for other, pr2, geom2 in items:
            if other == taba:
                continue
            total2 = num(pr2.get("units_total"))
            if total2 <= base:
                continue
            y1, y2 = year(pr), year(pr2)
            if y1 and y2 and y2 < y1:
                continue
            try:
                inter = geom.intersection(geom2).area
            except Exception:
                continue
            small = min(geom.area, geom2.area)
            if not small or inter / small < MIN_OVERLAP:
                continue
            in2 = num(pr2.get("units_in"))
            kind = (pr2.get("plan_type") or "") + " " + (pr2.get("plan_name_he") or "")
            # the later plan declares this plan's total as its own incoming, or it says
            # outright that it is a rights increase
            if not (abs(in2 - base) <= max(2, base * 0.02) or any(k in kind for k in RAISER_KINDS)):
                continue
            if best is None or total2 > best["units_total"]:
                best = {
                    "taba": other,
                    "plan_name": pr2.get("plan_name") or ("101-" + other.zfill(7)),
                    "title": pr2.get("plan_summary") or pr2.get("plan_name_he") or "",
                    "units_total": int(total2),
                    "units_in": int(in2),
                    "raise": int(total2 - base),
                    "plan_type": (pr2.get("plan_type") or "").strip(),
                    "approved": pr2.get("mavat_date") or "",
                    "status": pr2.get("status_mavat") or "",
                    "overlap": round(inter / small, 3),
                }
        if best:
            best["base_units"] = int(base)
            by_taba[taba] = best

    doc = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "min_overlap": MIN_OVERLAP,
        "count": len(by_taba),
        "raise_units_total": sum(v["raise"] for v in by_taba.values()),
        "by_taba": by_taba,
    }
    with io.open(OUT, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=1)
    print("Wrote %s — %d plans raised by a later plan, %d יח\"ד in total"
          % (OUT, doc["count"], doc["raise_units_total"]))
    for t, v in sorted(by_taba.items(), key=lambda kv: -kv[1]["raise"])[:12]:
        pr = plans[t][0]
        print("   %-8s %-26s %5d → %-10s %5d  (+%d)"
              % (t, (pr.get("plan_summary") or pr.get("plan_name_he") or "")[:26],
                 v["base_units"], v["plan_name"], v["units_total"], v["raise"]))


if __name__ == "__main__":
    main()
