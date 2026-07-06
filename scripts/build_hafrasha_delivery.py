# -*- coding: utf-8 -*-
"""
build_hafrasha_delivery.py — "מסירה בפועל" של הפרשות מבונות מספר הנכסים העירוני.

Joins the raw muni books (scripts/sources/muni_*.json, produced by
fetch_muni_books.py) against our hafrasha/shavatz plans and emits the slim
data/hafrasha_delivery.json the app loads:

  { "meta": {...},
    "plans": { "<short taba>": [ {asset...}, ... ] } }

Matching: each property-book row is placed via its גוש/חלקה through
data/parcel_centroids.json (Oranim-bbox parcels only) and point-in-polygon
tested against every plan in plans.geojson that has hafrash_*/shavatz_out_*
data. Only delivery-evidence rows are kept — an asset whose שימוש עיקרי is
an explicit "הפרשה מבונה" type or "שטח מבונה בבניין", or whose סטטוס is a
hafrasha process status ("פתיחת תהליך הפרשה לצורכי ציבור"). Pre-existing
public buildings inside a plan's boundary (schools, gans that predate the
plan) are deliberately NOT delivery evidence and are excluded.

delivery_state per asset: "נמסר" once ownership is registered (סטטוס contains
"נרשם"), else "בתהליך". Allocation-book rows (which עמותה operates the asset)
are embedded per asset by מספר נכס.

Run after fetch_muni_books.py. Idempotent.
"""
import json, sys, io, os, re, datetime, collections

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

HERE = os.path.dirname(os.path.abspath(__file__))

# Use-category taxonomy shared by the popup refinement and the use-gaps
# report. Classified here (single source of truth) so the app ships no regex
# copy. Labels are what the UI shows. "generic" is the wildcard "מבנים
# ומוסדות ציבור" designation — it covers every PUBLIC category below but NOT
# commerce/housing (those are not public uses).
USE_CATS = [
    ("גן ילדים",           r'גן ילדים|גני ילדים|כיתות גן|כיתת גן|גנ"?י'),
    ("מעון",               r"מעון|מעונות"),
    ("בי\"ס/חינוך",        r'בי"?ס|בית ספר|חינוך'),
    ("בית כנסת",           r'בית ה?כנסת|ביכ"?נ|בתי כנסת|\bדת\b'),
    ("מקווה",              r"מקווה|מקוה"),
    ("קהילה/רווחה/תרבות",  r'קהיל|רווחה|מתנ"?ס|תרבות|מועדון|פנאי|העצמה|שיקום|מינהל קהילתי|קשיש|גיל שלישי|גיל רך|נוער|חברה'),
    ("ספורט",              r"ספורט|טניס"),
    ("בריאות",             r"טיפת חלב|מרפאה|בריאות"),
    # מוג\w?לויות tolerates the muni book's typo "מוגלויות" (seen on 660738)
    ("דיור",               r"דיר(ה|ות)|דיור מוגן|מוג\w?לויות"),
    ("מסחר/תעסוקה",        r"מסחר|תעסוקה|משרדים"),
]
NON_PUBLIC = {"מסחר/תעסוקה", "דיור"}
GENERIC_RE = re.compile(
    r"מבני ציבור|מבנים ומוסדות ציבור|ומוסדות מבנים ציבור|שימוש ציבורי|לציבור|מוסדות ציבור")


def classify_uses(text):
    t = str(text or "")
    return [label for label, rx in USE_CATS if re.search(rx, t)]

try:
    from shapely.geometry import shape, Point
    from shapely.strtree import STRtree
except ImportError:
    raise SystemExit("shapely is required (pip install shapely)")


def _find(relparts):
    for base in [HERE, os.path.join(HERE, ".."),
                 os.path.join(HERE, "oranim-app")]:
        p = os.path.join(base, *relparts)
        if os.path.exists(p):
            return p
    raise SystemExit("not found: " + "/".join(relparts))


def _load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _date(ms):
    if not ms:
        return None
    try:
        return datetime.datetime.fromtimestamp(
            ms / 1000, datetime.timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return None


def is_delivery_evidence(row):
    use = (row.get("שימוש עיקרי") or "").strip()
    status = (row.get("סטטוס") or "").strip()
    return (use.startswith("הפרשה מבונה")
            or use == "שטח מבונה בבניין"
            or "הפרשה" in status)


def delivery_state(status):
    return "נמסר" if "נרשם" in (status or "") else "בתהליך"


def main():
    prop = _load(_find(["sources", "muni_property_book.json"]))
    alloc = _load(_find(["sources", "muni_allocation_book.json"]))
    cent = _load(_find(["..", "data", "parcel_centroids.json"]))["gush_helka"]
    plans_geo = _load(_find(["..", "data", "plans.geojson"]))["features"]

    alloc_by_asset = collections.defaultdict(list)
    for a in alloc["rows"]:
        alloc_by_asset[a["מספר נכס"]].append({
            "org": a.get("שם"),
            "use": a.get("שימוש"),
            "cats": classify_uses(
                str(a.get("שימוש") or "") + " " + str(a.get("שם") or "")),
            "active": a.get("פעיל"),
            "approved": _date(a.get("אישור מועצה אחרון")),
            "semel": a.get('סמל מוסד מנח"י') or None,
        })

    # plans with hafrasha/shavatz data → polygons
    polys, props = [], []
    for f in plans_geo:
        p = f["properties"]
        if not any(p.get(k) for k in ("hafrash_sqm", "hafrash_prg",
                                      "shavatz_out_sqm", "shavatz_out_prog")):
            continue
        if not f.get("geometry"):
            continue
        try:
            g = shape(f["geometry"])
            if not g.is_valid:
                g = g.buffer(0)
        except Exception:
            continue
        polys.append(g)
        props.append(p)
    tree = STRtree(polys)
    print(f"plans with hafrasha/shavatz data + geometry: {len(polys)}")

    out = collections.defaultdict(dict)   # taba -> asset_id -> asset
    n_rows = 0
    for r in prop["rows"]:
        if not is_delivery_evidence(r):
            continue
        g, h = r.get("גוש"), r.get("חלקה")
        if not g or not h:
            continue
        pt = cent.get(f"{g}/{h}".replace(" ", ""))
        if not pt:
            continue
        n_rows += 1
        point = Point(pt)
        for idx in tree.query(point, predicate="intersects"):
            taba = str(props[idx].get("taba") or "").strip()
            if not taba:
                continue
            aid = r["מספר נכס"]
            asset = out[taba].get(aid)
            if asset is None:
                # Use categories: the descriptive asset NAME is the reliable
                # signal; the שימוש עיקרי field is usually the catch-all
                # "הפרשה מבונה - חברה/קהילה/רווחה" bucket, so it is only a
                # fallback when the name says nothing specific.
                cats = classify_uses(r.get("שם נכס"))
                if not cats:
                    cats = classify_uses(r.get("שימוש עיקרי"))
                asset = out[taba][aid] = {
                    "asset_id": aid,
                    "name": r.get("שם נכס"),
                    "use": r.get("שימוש עיקרי"),
                    "cats": cats,
                    "status": r.get("סטטוס"),
                    "state": delivery_state(r.get("סטטוס")),
                    "opened": _date(r.get("תאריך פתיחת נכס")),
                    "built_sqm": r.get('שטח בנוי במ"ר') or 0,
                    "parcels": [],
                    "allocations": alloc_by_asset.get(aid, []),
                }
            gh = f"{g}/{h}"
            if gh not in asset["parcels"]:
                asset["parcels"].append(gh)

    plans_out = {t: sorted(assets.values(), key=lambda a: a["asset_id"])
                 for t, assets in sorted(out.items())}
    n_assets = sum(len(v) for v in plans_out.values())
    # Per-plan declared-use categories from the statutory program text, for
    # the use-gaps report (asset cats vs plan cats, overlap-aware in-app).
    plan_cats = {}
    for t in plans_out:
        p = next((pp for pp in props if str(pp.get("taba") or "") == t), {})
        prg = " ; ".join(str(p.get(k) or "")
                         for k in ("hafrash_prg", "shavatz_out_prog"))
        plan_cats[t] = {"cats": classify_uses(prg),
                        "generic": bool(GENERIC_RE.search(prg)),
                        "name": p.get("plan_name_he") or ""}
    # Sanity floor for unattended runs: 85 plans / 134 assets on 2026-07-02.
    # A near-empty result means an input broke — abort, keep the committed file.
    if len(plans_out) < 20 or n_assets < 30:
        raise SystemExit(
            f'suspiciously small join ({len(plans_out)} plans / {n_assets} '
            f'assets) — aborting without writing')
    result = {
        "meta": {
            "built_at": datetime.date.today().isoformat(),
            "source_refresh": prop.get("meta", {}).get("fetched_at"),
            "plans": len(plans_out),
            "assets": n_assets,
        },
        "plans": plans_out,
        "plan_cats": plan_cats,
        "non_public_cats": sorted(NON_PUBLIC),
    }
    dest = os.path.join(os.path.dirname(
        _find(["..", "data", "plans.geojson"])), "hafrasha_delivery.json")
    # Same join content as the committed file → skip the write, so the weekly
    # cron doesn't produce a commit whose only diff is the meta dates.
    if os.path.exists(dest):
        try:
            old = _load(dest)
            if (old.get("plans") == plans_out
                    and old.get("plan_cats") == plan_cats):
                print("no content change — keeping existing file")
                return
        except Exception:
            pass
    with open(dest, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, separators=(",", ":"))
    print(f"delivery-evidence rows placed: {n_rows}")
    print(f"wrote {len(plans_out)} plans / {n_assets} assets -> {dest}")


if __name__ == "__main__":
    main()
