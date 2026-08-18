# -*- coding: utf-8 -*-
"""
build_hafrash_read_queue.py — who still has an unknown הפרשה מבונה, and where the
answer could come from.

An approved plan can impose a *built* public allocation (הפרשה מבונה) on a private
lot and describe it only as "מבנים ומוסדות ציבור" or "*בתיאום עם מחלקת מבני ציבור*".
That tells us an allocation exists but not WHAT it is — and for plans already at
permit stage the space may already be built.

Every plan carrying a hafrasha lands in exactly one bucket:

  known_text       hafrash_prg already names a use (hafrash_classify.domains()).
  known_delivery   the muni property book (hafrasha_delivery.json) already opened a
                   concrete asset there — the app already prefers this over the
                   statutory text (hafrashahFeatureDomains), so it IS the answer.
  plot_not_hafrasha  not a built allocation at all: the plan has a standalone public
                   LOT of that exact area, so the row is a misclassified שב"צ plot.
  superseded       the same obligation is recorded on a later plan covering the same
                   ground (detect_plan_containment.py) — counting both doubles it.
  no_permit        generic, no asset, and no licensing file exists → nothing to read.
  queued           generic, no asset, but a permit file exists → the גרמושקה in that
                   file can answer it. This is the work.

Outputs
  C:\\ORANIM\\hafrash_read_queue.json          local operational state (gitignored)
  <repo>/data/hafrash_unknown_audit.json      published; the single answer to
                                              "how many allocations are unknown"

  py build_hafrash_read_queue.py [--approved-only] [--quiet]
"""
import io
import json
import os
import re
import sys
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrich_queue as eq
from hafrash_classify import domains, has_known_use

DATA = r"C:\dev\oranim-app\data"
AUDIT_OUT = os.path.join(DATA, "hafrash_unknown_audit.json")

# status_mavat values that mean the plan is in force (or one step away).
APPROVED = {"אישור", "מאושרת", "תבע מאושרת", "תחילת תוקף", "הכרעה בהתנגדויות / אישור"}

# ── permit-revision scoring ────────────────────────────────────────────────────
# Rank REVISIONS, not files. permits_master collapses revisions and reports the
# LAST one's description, so tik 2016/886 reads "חפירה ומילוי" there while its
# category is bniya_chadasha — use all_permits' per-revision descriptions instead.
RX_PUBLIC = re.compile(r'(שטח ציבורי|שטחים ציבורי|צור?כי ציבור|מבנה ציבור|מבני ציבור|'
                       r'מוסדות ציבור|מועדון|גן ילדים|גנ"י|בית כנסת|מעון|מקווה|מרפאה)')
RX_NEW = re.compile(r'(בני[יה]ה חדשה|בני[יי]ן .{0,12}חדש|מבנה חדש|הקמת|בנין מגורים חדש)')
# The excavation trap: a file whose SUBJECT is earthworks/demolition carries no use
# information at all (verified on 2016/0886.01 "חפירה ומילוי" and 2024/0446.01
# "הגשת חפירה ודיפון לצורך הקמת 2 מגדלים" — the latter names the towers only as the
# purpose, so an anchored ^חפירה test misses it while "הקמת" wrongly scores it up).
RX_PREP = re.compile(r'^\s*(הגשת\s+|בקשה\s+ל)?'
                     r'(חפירה|דיפון|הריסה|הריסת\s+(חפירה|דיפון)|עבודות\s+(מקדימות|עפר))')
# Municipal infrastructure files (light rail, utilities) sit inside the plan footprint
# but are never the plot's building permit — 101-1048347's only file is a light-rail
# rectifier room, 101-0800771's is Purple-Line preparatory works.
RX_INFRA = re.compile(r'(הרכבת הקלה|רכבת קלה|חדר מיישרים|הקו הסגול|הקו האדום|'
                      r'תחנת שאיבה|מתקן הנדסי)')
RX_MINOR = re.compile(r'(שימוש חורג|תכנית שינויים|תוספת מתקן|תחזוקה|שלט)')
BAD_CATEGORIES = {"chafira_milui", "harisa", "tachzukah", "drachim"}


def _load(path, default=None):
    try:
        return json.load(io.open(path, encoding="utf-8"))
    except Exception:
        return default if default is not None else {}


def num(v):
    s = re.sub(r"[^0-9.]", "", str(v or ""))
    try:
        return float(s or 0)
    except ValueError:
        return 0.0


def year_of(tik):
    m = re.match(r"(\d{4})/", str(tik or ""))
    return int(m.group(1)) if m else None


def plan_year(props):
    """Approval year from mavat_date (dd/mm/yyyy or yyyy-mm-dd)."""
    s = str(props.get("mavat_date") or "")
    m = re.search(r"(\d{4})", s)
    return int(m.group(1)) if m else None


def score_revision(descr, tik, py, category):
    """Higher = more likely to be the permit that actually built the allocation."""
    d = str(descr or "")
    s = 0
    if RX_PUBLIC.search(d):
        s += 40
    if RX_NEW.search(d):
        s += 15
    ty = year_of(tik)
    if py and ty:
        # A file opened around/after approval is probably THIS plan's building permit;
        # a much older file usually belongs to the pre-existing building. Soft only —
        # 101-0342170's 1982 file has a 2026 revision that does describe the public area.
        if ty >= py - 1:
            s += 10
        elif ty < py - 3:
            s -= 5
    if RX_PREP.search(d):
        s -= 60
    if RX_INFRA.search(d):
        s -= 70
    if RX_MINOR.search(d):
        s -= 40
    if category in BAD_CATEGORIES:
        s -= 30
    return s


# ── delivery (property book) ──────────────────────────────────────────────────
# A delivery row only ANSWERS the question when it names a concrete asset, or is
# generically named but categorised AND actually built. A row like
# {use: "הפרשה מבונה - חברה/קהילה/רווחה", built_sqm: 0} is a placeholder, not an answer.
PLACEHOLDER = re.compile(r"^(הפרשה מבונה|שטח מבונה בבניין|שטח ציבורי)")
NON_PUBLIC_CATS = {"מסחר/תעסוקה"}


def delivery_answer(assets):
    out = []
    for a in assets or []:
        name = str(a.get("use") or a.get("descr") or "").strip()
        cats = [c for c in (a.get("cats") or []) if c not in NON_PUBLIC_CATS]
        sqm = num(a.get("built_sqm"))
        if name and not PLACEHOLDER.match(name):
            out.append({"asset": name, "cats": cats, "built_sqm": sqm})
        elif cats and sqm > 0:
            out.append({"asset": name, "cats": cats, "built_sqm": sqm})
    return out


# A הפרשה מבונה is public floor area INSIDE a private building. When the plan's own
# landuse has a standalone public-designated LOT of the same area, the hafrasha row is
# a misclassified שב"צ plot: no גרמושקה will ever show it, because there is nothing to
# show — the public use is the whole plot. Verified on 101-0613570 (264 מ"ר ≡ מגרש 3,
# code 400), where a full vision read came back empty for exactly this reason.
PUBLIC_LOT_CODES = {400, 410, 450, 460, 1670}


def public_lot_matching(taba, sqm, landuse_by_plan, tol=0.05):
    if not sqm:
        return None
    for lot_num, area, name in landuse_by_plan.get(taba, []):
        if area and abs(area - sqm) / sqm <= tol:
            return {"lot": lot_num, "area": area, "designation": name}
    return None


def index_public_lots(path):
    out = {}
    for f in _load(path, {"features": []})["features"]:
        p = f.get("properties", {})
        t = re.sub(r"^101-?0*", "", str(p.get("pl_number") or "")).lstrip("0")
        try:
            code = int(p.get("mavat_code") or 0)
        except (TypeError, ValueError):
            continue
        if t and code in PUBLIC_LOT_CODES:
            out.setdefault(t, []).append((p.get("num"), round(p.get("shape_area") or 0),
                                          p.get("mavat_name")))
    return out


def main():
    # The question is about plans already in force; --all widens it for auditing.
    approved_only = "--all" not in sys.argv
    quiet = "--quiet" in sys.argv

    plans = _load(os.path.join(DATA, "plans.geojson"), {"features": []})["features"]
    delivery = _load(os.path.join(DATA, "hafrasha_delivery.json"), {}).get("plans", {})
    all_permits = _load(os.path.join(DATA, "all_permits.json"), {})

    public_lots = index_public_lots(os.path.join(DATA, "landuse_xplan.geojson"))
    # Obligations recorded twice on nested plans. 101-0095612 and 101-0571190 both
    # carry מגרש 201 / 1,631 מ"ר / 60 יח"ד and the same licensing file, because the
    # 2018 plan sits wholly inside the 2016 one — the audit must count it once.
    containment = _load(os.path.join(DATA, "plan_containment.json"), {})
    superseded_map = containment.get("superseded") or {}
    buckets = {"known_text": [], "known_delivery": [], "plot_not_hafrasha": [],
               "superseded": [], "no_permit": [], "queued": []}
    queue_items = []

    for feat in plans:
        p = feat["properties"]
        prg = str(p.get("hafrash_prg") or "").strip()
        sqm = num(p.get("hafrash_sqm"))
        if not (sqm > 0 or (prg and prg != "0")):
            continue
        status = str(p.get("status_mavat") or "").strip()
        if approved_only and status not in APPROVED:
            continue

        taba = eq.norm_taba(p.get("taba") or p.get("plan_name"))
        row = {"taba": taba, "plan_name": p.get("plan_name"),
               "sub_neighborhood": p.get("sub_neighborhood") or p.get("minahak") or "",
               "status": status, "hafrash_sqm": sqm, "hafrash_prg": prg}

        # 0. the same obligation is already counted on the plan in force here
        sup = superseded_map.get(taba)
        if sup:
            row["superseded_by"] = sup.get("superseded_by")
            row["containment"] = sup.get("containment")
            buckets["superseded"].append(row)
            continue

        # 1. the statutory text already names a use
        uses = [u.strip() for u in re.split(r";", prg) if u.strip()]
        if any(has_known_use(u) for u in uses):
            row["domains"] = sorted({d for u in uses for d in domains(u)})
            buckets["known_text"].append(row)
            continue

        # 2. the property book already opened a concrete asset there
        ans = delivery_answer(delivery.get(taba))
        if ans:
            row["delivery"] = ans
            buckets["known_delivery"].append(row)
            continue

        # 3. not actually a built allocation — the plan has a public LOT of that size
        plot = public_lot_matching(taba, sqm, public_lots)
        if plot:
            row["public_lot"] = plot
            buckets["plot_not_hafrasha"].append(row)
            continue

        # 4./5. generic — is there a permit file to read a גרמושקה from?
        py = plan_year(p)
        cands = []
        seen = set()
        gs_permit = str(p.get("building_permit") or "").strip()
        for q in (all_permits.get(taba) or {}).get("permits", []):
            fn = q.get("file_number")
            if not fn or fn in seen:
                continue
            seen.add(fn)
            cands.append({"tik": fn, "descr": q.get("request_description") or "",
                          "permit_status": q.get("status") or "",
                          "status_date": q.get("status_date") or "",
                          "score": score_revision(q.get("request_description"), fn, py, None)})
        if gs_permit and gs_permit not in seen:
            # The GS column names the permit the analyst recorded — trust it as a
            # candidate even when the scraper's list missed it.
            cands.append({"tik": gs_permit, "descr": "", "permit_status": "GS",
                          "status_date": "", "score": score_revision("", gs_permit, py, None) + 20})
        if gs_permit:
            for c in cands:
                if c["tik"] == gs_permit:
                    c["score"] += 20      # the analyst-recorded file wins ties
        cands.sort(key=lambda c: -c["score"])

        if not cands:
            buckets["no_permit"].append(row)
            continue
        row["candidates"] = cands
        row["low_prospect"] = max(c["score"] for c in cands) <= 5
        buckets["queued"].append(row)
        queue_items.append({"taba": taba, "plan_name": row["plan_name"],
                            "hafrash_sqm": sqm, "hafrash_prg": prg,
                            "candidates": cands, "low_prospect": row["low_prospect"],
                            "queued_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})

    eq.enqueue_hafrash(queue_items)

    gross = sum(r["hafrash_sqm"] for v in buckets.values() for r in v)
    dropped = sum(r["hafrash_sqm"] for r in buckets["superseded"])
    audit = {"built_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
             "scope": "approved plans carrying a הפרשה מבונה",
             "counts": {k: len(v) for k, v in buckets.items()},
             "hafrash_sqm": {"gross": round(gross), "double_counted": round(dropped),
                             "net": round(gross - dropped)},
             "buckets": buckets}
    with io.open(AUDIT_OUT, "w", encoding="utf-8") as f:
        json.dump(audit, f, ensure_ascii=False, indent=1)

    if not quiet:
        print("הפרשה מבונה בתכניות מאושרות: %d" % sum(len(v) for v in buckets.values()))
        for k, label in [("known_text", "סוג ידוע מהוראות התכנית"),
                         ("known_delivery", "סוג ידוע מספר הנכסים"),
                         ("plot_not_hafrasha", "מגרש ציבורי, לא הפרשה מבונה"),
                         ("superseded", "נספר כבר בתכנית מאוחרת יותר"),
                         ("queued", "לקריאה מגרמושקת ההיתר"),
                         ("no_permit", "לא ידוע, ואין היתר לקרוא ממנו")]:
            print("  %-28s %3d" % (label, len(buckets[k])))
        print('  %-28s %s מ"ר ברוטו · %s נספרים פעמיים · %s נטו'
              % ("סה\"כ הפרשה:", format(int(gross), ","), format(int(dropped), ","),
                 format(int(gross - dropped), ",")))
        print()
        for r in sorted(buckets["queued"], key=lambda r: -r["hafrash_sqm"]):
            top = r["candidates"][0]
            print("  %-15s %8s מ\"ר  %-22s  %-14s (%+d) %s%s" % (
                r["plan_name"], format(int(r["hafrash_sqm"]), ","),
                r["sub_neighborhood"][:20], top["tik"], top["score"],
                (top["descr"] or "")[:40], "  ⚠low" if r["low_prospect"] else ""))
        print("\nqueue -> %s\naudit -> %s" % (eq.HAFRASH_QUEUE_PATH, AUDIT_OUT))


if __name__ == "__main__":
    main()
