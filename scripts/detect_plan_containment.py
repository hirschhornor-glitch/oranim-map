# -*- coding: utf-8 -*-
"""
detect_plan_containment.py — find approved plans that sit (almost) entirely inside
another approved plan, so the same obligation is not counted twice.

Why this exists: 101-0571190 ("בריכת ירושלים", 2018) lies 100% inside 101-0095612
("עמק רפאים 43", 2016). Both record מגרש 201, הפרשה 1,631 מ"ר, שב"צ 1,604 מ"ר and
60 יח"ד, and both point at the same licensing file 2017/0384 — one obligation, two
rows. Any aggregate over the quarter doubles it.

This does NOT touch `overlapping_plans`: that is a GS column (תבעותעלאותושטח) used
for label placement, and GS is the master for it. The computed view lives in its own
file so the two never fight.

A pair is classified by what the two plans say about the same ground:
  duplicate_obligation  identical הפרשה figure  → almost certainly one obligation
  divergent_figure      both carry a figure, but different ones → needs a human
  one_sided             only one of the two carries a figure
  no_figure             neither does (recorded for completeness, low interest)

The OPERATIVE plan of a pair is the later one by mavat_date; a building permit breaks
a tie. The other is marked superseded_by — callers should count the operative one.

  py detect_plan_containment.py [--min-overlap 0.95] [--all] [--quiet]

Writes <repo>/data/plan_containment.json.
"""
import io
import json
import os
import re
import sys
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")

from shapely.geometry import shape

DATA = r"C:\dev\oranim-app\data"
OUT = os.path.join(DATA, "plan_containment.json")
PLANS = os.path.join(DATA, "plans.geojson")

APPROVED = {"אישור", "מאושרת", "תבע מאושרת", "תחילת תוקף", "הכרעה בהתנגדויות / אישור"}


def num(v):
    s = re.sub(r"[^0-9.]", "", str(v or ""))
    try:
        return float(s or 0)
    except ValueError:
        return 0.0


def parse_date(s):
    """dd/mm/yyyy or yyyy-mm-dd → date, else None."""
    s = str(s or "").strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    return None


def bbox(geom):
    xs, ys = [], []

    def walk(c):
        if isinstance(c[0], (int, float)):
            xs.append(c[0]); ys.append(c[1])
        else:
            for x in c:
                walk(x)
    walk(geom["coordinates"])
    return min(xs), min(ys), max(xs), max(ys)


def bbox_hit(a, b):
    return not (a[2] < b[0] or b[2] < a[0] or a[3] < b[1] or b[3] < a[1])


def operative_of(pa, pb):
    """Which of the two is the plan in force? Later approval wins; a permit breaks a tie."""
    da, db = parse_date(pa.get("mavat_date")), parse_date(pb.get("mavat_date"))
    if da and db and da != db:
        return (pa, pb) if da > db else (pb, pa)
    ha = bool(str(pa.get("building_permit") or "").strip())
    hb = bool(str(pb.get("building_permit") or "").strip())
    if ha != hb:
        return (pa, pb) if ha else (pb, pa)
    return (pa, pb)


def main():
    min_ov = 0.95
    if "--min-overlap" in sys.argv:
        min_ov = float(sys.argv[sys.argv.index("--min-overlap") + 1])
    approved_only = "--all" not in sys.argv
    quiet = "--quiet" in sys.argv

    feats = json.load(io.open(PLANS, encoding="utf-8"))["features"]
    items = []
    for f in feats:
        p = f["properties"]
        if approved_only and (p.get("status_mavat") or "").strip() not in APPROVED:
            continue
        g = f.get("geometry")
        if not g or not g.get("coordinates"):
            continue
        try:
            sh = shape(g)
            if not sh.is_valid:
                sh = sh.buffer(0)
            if sh.is_empty or sh.area <= 0:
                continue
        except Exception:
            continue
        items.append({"p": p, "g": sh, "bb": bbox(g), "haf": num(p.get("hafrash_sqm")),
                      "units": num(p.get("units_total"))})

    pairs = []
    for i in range(len(items)):
        a = items[i]
        for j in range(i + 1, len(items)):
            b = items[j]
            if not bbox_hit(a["bb"], b["bb"]):
                continue
            try:
                inter = a["g"].intersection(b["g"]).area
            except Exception:
                continue
            if inter <= 0:
                continue
            fa, fb = inter / a["g"].area, inter / b["g"].area
            cont = max(fa, fb)
            if cont < min_ov:
                continue
            inner, outer = (a, b) if fa >= fb else (b, a)
            op, sup = operative_of(inner["p"], outer["p"])
            hi, ho = inner["haf"], outer["haf"]
            if hi > 0 and ho > 0:
                # Tolerance, not equality: 101-0533711 records 202 מ"ר against
                # 101-1249358's 204 on the same ground — a 1% rounding difference, not
                # two obligations. Anything past a few percent is a real divergence.
                tol = max(5.0, 0.02 * min(hi, ho))
                kind = "duplicate_obligation" if abs(hi - ho) <= tol else "divergent_figure"
            elif hi > 0 or ho > 0:
                kind = "one_sided"
            else:
                kind = "no_figure"
            pairs.append({
                "kind": kind,
                "containment": round(cont, 4),
                "inner": inner["p"].get("plan_name"),
                "outer": outer["p"].get("plan_name"),
                "operative": op.get("plan_name"),
                "superseded": sup.get("plan_name"),
                "hafrash": {inner["p"].get("plan_name"): hi, outer["p"].get("plan_name"): ho},
                "units": {inner["p"].get("plan_name"): inner["units"],
                          outer["p"].get("plan_name"): outer["units"]},
                "dates": {inner["p"].get("plan_name"): inner["p"].get("mavat_date"),
                          outer["p"].get("plan_name"): outer["p"].get("mavat_date")},
                "summaries": {inner["p"].get("plan_name"): inner["p"].get("plan_summary")
                              or inner["p"].get("plan_name_he"),
                              outer["p"].get("plan_name"): outer["p"].get("plan_summary")
                              or outer["p"].get("plan_name_he")},
            })

    # Only duplicate_obligation is safe to net out automatically; the rest are flags.
    superseded = {}
    for pr in pairs:
        if pr["kind"] != "duplicate_obligation":
            continue
        key = re.sub(r"^101-?0*", "", str(pr["superseded"] or "")).lstrip("0")
        superseded[key] = {"superseded_by": pr["operative"],
                           "hafrash_sqm": pr["hafrash"].get(pr["superseded"]),
                           "containment": pr["containment"]}

    dup_sqm = sum(v["hafrash_sqm"] or 0 for v in superseded.values())
    out = {
        "built_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "min_overlap": min_ov,
        "scope": "approved plans" if approved_only else "all plans",
        "plans_compared": len(items),
        "counts": {k: sum(1 for p in pairs if p["kind"] == k)
                   for k in ("duplicate_obligation", "divergent_figure", "one_sided", "no_figure")},
        "double_counted_hafrash_sqm": round(dup_sqm),
        "superseded": superseded,
        # Only the pairs that carry a hafrasha figure on BOTH sides are actionable.
        # Nesting is normal — 859 of the pairs are two plans neither of which has an
        # allocation, and persisting those made the file 739 KB of noise.
        "pairs": sorted([p for p in pairs if p["kind"] in ("duplicate_obligation", "divergent_figure")],
                        key=lambda p: (p["kind"], -max(p["hafrash"].values() or [0]))),
    }
    with io.open(OUT, "w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, indent=1)

    if not quiet:
        print("תכניות שהושוו: %d | זוגות בהכלה >=%.0f%%: %d" % (len(items), min_ov * 100, len(pairs)))
        for k, label in [("duplicate_obligation", "חובה כפולה (אותו מספר)"),
                         ("divergent_figure", "מספרים שונים — לבדיקה"),
                         ("one_sided", "רק לאחת יש הפרשה"),
                         ("no_figure", "לאף אחת אין הפרשה")]:
            print("  %-26s %d" % (label, out["counts"][k]))
        print("\nמ\"ר הפרשה שנספרים פעמיים: %s" % format(int(dup_sqm), ","))
        for pr in pairs:
            if pr["kind"] != "duplicate_obligation":
                continue
            print("  %-15s ⊂ %-15s  %s מ\"ר  · בתוקף: %s" % (
                pr["inner"], pr["outer"], int(pr["hafrash"][pr["inner"]]), pr["operative"]))
        print("\n-> %s" % OUT)


if __name__ == "__main__":
    main()
