# -*- coding: utf-8 -*-
"""
build_rental_public_assets.py — "נכסי ציבור בשכירות" מספר הנכסים העירוני.

Unlike build_hafrasha_delivery.py (which joins delivery-evidence assets to the
statutory plan that produced them), rentals are NOT tied to a plan — they are
public-service assets the municipality holds via a rental/lease contract on
land or a building it does NOT own. The property book records this only in the
סטטוס field (e.g. "נחתם חוזה לשכירת נכס", "נרשמה חכירה ע\"ש עיריה").

This script:
  1. Scans scripts/sources/muni_property_book.json for rows whose סטטוס is a
     rental (שכירות — municipality as tenant) or lease (חכירה — long lease)
     status, dedupes to one record per מספר נכס, aggregates its parcels.
  2. Places each asset via גוש/חלקה → data/parcel_centroids.json and keeps only
     those inside district_oranim.geojson (point-in-polygon).
  3. Classifies each by use-domain (education / welfare / community / religion /
     sport / health / admin / emergency / open-space / housing / commerce).
     Pure land-tenure infrastructure (roads, paths, utility poles/antennas) is
     tagged domain "infra" and EXCLUDED from the served layer (a road leased
     from RMI is land tenure, not a public building) — its count is kept in meta.
  4. Joins the allocation book by מספר נכס (which עמותה, if any, operates it).
  5. Emits data/rental_public_assets.geojson — a Point FeatureCollection the app
     loads as a map layer and a report.

Run after fetch_muni_books.py. Idempotent (skips the write when content is
unchanged, so the weekly cron produces no date-only commit).
"""
import json, sys, io, os, re, datetime, collections

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
HERE = os.path.dirname(os.path.abspath(__file__))

# --- tenure: which סטטוס values mean the municipality holds via rent/lease ---
RENT_STATUS = {
    "נחתם חוזה לשכירת נכס", "תחילת תהליך שכירת נכס", "נחתם חוזה שכירה/חכירה",
    "בתהליך שכירה/חכירה", "תחילת הארכת חוזה שכירה/חכירה",
}
LEASE_STATUS = {
    'נרשמה חכירה ע"ש עיריה', "נחתם חוזה חכירה", "החל תהליך חכירת נכס",
    "קבלת חוזה חכירה מרמי", "נחתם חוזה רכישה+חכירה",
}
# "בתהליך" once the contract is signed / registered, else still in negotiation.
SIGNED_RE = re.compile(r"נחתם|נרשמה|קבלת חוזה")


def tenure_of(status):
    s = (status or "").strip()
    if s in RENT_STATUS:
        return "שכירות"
    if s in LEASE_STATUS:
        return "חכירה"
    return None


def tenure_state(status):
    return "פעיל" if SIGNED_RE.search(status or "") else "בתהליך"


# The municipal source defines a "held" asset by מקור זכות (רשות שימוש / שכירות
# חופשית / שכירות מוגנת) — i.e. the city has a USE right, NOT ownership. That
# field is absent from the public Power BI model, so we approximate it with the
# owner: an asset the city OWNS (בעלים = עירית ירושלים) is not held-via-rental
# and is excluded — matching the source. RMI / רשות הפיתוח / מדינה / private
# owners stay (the city rents/holds from them). See PROMPT_municipal_rentals_logic.
def is_city_owned(owner):
    o = owner or ""
    return "עירי" in o and "ירושלים" in o


# --- use-domain taxonomy (order matters: first match wins) ---------------
# label shown in the UI, plus the machine key the app styles/filters by.
DOMAINS = [
    ("infra",      "תשתית / קרקע",   r"כביש|שביל|מדרכה|חניון|חנייה|תחנת טרנספורמצי|טרפו|מתקן שידור|סלולר|אנטנה|תשתית"),
    ("education",  "חינוך",          r'בית ספר|בתי ספר|בי"?ס|גן ילדים|גני ילדים|חינוך|כית(ה|ות)|דרמה|מוזיקה|אומנויות|תלמוד תורה|ישיבה|מתמטי'),
    ("welfare",    "רווחה",          r"רווחה|קשיש|גיל שלישי|מועדון קשישים|תחנה לטיפול"),
    ("health",     "בריאות",         r"טיפת חלב|מרפאה|בריאות|בריכת"),
    ("community",  "קהילה ותרבות",   r'מנהל קהילתי|מינהל קהילתי|מתנ"?ס|מועדון נוער|תנועת נוער|נוער|קהיל|תרבות|ספרי(ה|יה)|אולם|מרכז'),
    ("religion",   "דת",             r"בית כנסת|ביכנ|מקווה|מקוה|כנסי|מנזר|מסגד|דת"),
    ("sport",      "ספורט",          r"ספורט|מגרש ספורט|כושר|טניס|מתקני כושר"),
    ("emergency",  "חירום",          r"מקלט|מכבי אש|פיקוד העורף|הג\"א|חירום|משטרה"),
    ("open_space", "שטח פתוח",       r"שטח פתוח|גן ציבורי|גינה|פארק|שצ\"פ"),
    ("housing",    "מגורים",         r"מגורים|דיר(ה|ות)|דיור"),
    ("commerce",   "מסחר",           r"חנות|קיוסק|מסחר|מזנון|בית קפה|מסעדה"),
    ("admin",      "מנהל ומשרדים",   r"משרד|אגף|מנהל|לשכה|עיריי|מוסך|מחסן"),
]
DOMAIN_RES = [(k, lbl, re.compile(rx)) for k, lbl, rx in DOMAINS]
DOMAIN_LABEL = {k: lbl for k, lbl, _ in DOMAINS}

# "שימוש חורג" = an INSTITUTIONAL public activity operating on land whose
# statutory zoning is residential. Shelters (מקלט — normal inside residential
# blocks), religion, commerce, actual housing and infra are NOT flagged (per
# user decision 2026-08-06). Residential zoning is read from our
# yiud_karka_kayam layer (Descr), not the property book (its יעוד is blank).
NONCONFORMING_DOMAINS = {"education", "welfare", "community", "admin", "health"}
RESIDENTIAL_RE = re.compile(r"מגורים|מגורי|דיור")


def classify_domain(use, name):
    text = f"{use or ''} {name or ''}"
    for k, _lbl, rx in DOMAIN_RES:
        if rx.search(text):
            return k
    return "other"


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


def _write_rental_flags(public):
    """Join rental assets → shavaz_kayam feature fid (point-in-polygon) and
    write data/rental_flags.json { meta, by_fid: {fid: [rec, ...]} }.
    Loaded app-side as window.__rentalFlags so the shavaz popup can show a
    'מוחזק בשכירות' block. Idempotent (skip-write on no change)."""
    from shapely.geometry import shape, Point
    from shapely.strtree import STRtree

    kayam = _load(_find(["..", "data", "shavaz_kayam.geojson"]))["features"]
    polys, fids = [], []
    for f in kayam:
        if not f.get("geometry"):
            continue
        try:
            g = shape(f["geometry"])
            if not g.is_valid:
                g = g.buffer(0)
        except Exception:
            continue
        polys.append(g)
        fids.append(str(f["properties"].get("fid")))
    tree = STRtree(polys)

    by_fid = collections.defaultdict(list)
    for a in public:
        pt = Point(a["pt"])
        for idx in tree.query(pt, predicate="intersects"):
            by_fid[fids[idx]].append({
                "asset_id": a["asset_id"],
                "name": a["name"],
                "use": a["use"],
                "domain": a["domain"],
                "tenure": a["tenure"],
                "state": a["state"],
                "owner": a["owner"],
                # a rented דירה is delivered as הפרשה מבונה in our model
                "is_hafrasha_dira": a["domain"] == "housing",
            })

    result = {"meta": {"built_at": datetime.date.today().isoformat(),
                       "buildings": len(by_fid),
                       "assets": sum(len(v) for v in by_fid.values())},
              "by_fid": dict(sorted(by_fid.items()))}
    dest = os.path.join(os.path.dirname(
        _find(["..", "data", "shavaz_kayam.geojson"])), "rental_flags.json")
    if os.path.exists(dest):
        try:
            if _load(dest).get("by_fid") == result["by_fid"]:
                print("rental_flags: no change — keeping existing file")
                return
        except Exception:
            pass
    with open(dest, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, separators=(",", ":"))
    print(f"rental_flags: {len(by_fid)} shavaz buildings flagged -> {dest}")


def main():
    prop = _load(_find(["sources", "muni_property_book.json"]))
    alloc = _load(_find(["sources", "muni_allocation_book.json"]))
    cent = _load(_find(["..", "data", "parcel_centroids.json"]))["gush_helka"]
    dist = _load(_find(["..", "data", "district_oranim.geojson"]))

    # district polygon (union of all features) for point-in-polygon
    dgeoms = []
    for f in dist["features"]:
        if not f.get("geometry"):
            continue
        try:
            g = shape(f["geometry"])
            if not g.is_valid:
                g = g.buffer(0)
            dgeoms.append(g)
        except Exception:
            pass
    dtree = STRtree(dgeoms)

    def in_area(pt):
        p = Point(pt)
        for idx in dtree.query(p, predicate="intersects"):
            return True
        return False

    alloc_by_asset = collections.defaultdict(list)
    for a in alloc["rows"]:
        alloc_by_asset[a.get("מספר נכס")].append({
            "org": a.get("שם"),
            "use": a.get("שימוש"),
            "active": a.get("פעיל"),
            "semel": a.get('סמל מוסד מנח"י') or None,
        })

    assets = {}   # asset_id -> record
    for r in prop["rows"]:
        ten = tenure_of(r.get("סטטוס"))
        if not ten:
            continue
        g, h = r.get("גוש"), r.get("חלקה")
        if not g or not h:
            continue
        pt = cent.get(f"{g}/{h}".replace(" ", ""))
        if not pt or not in_area(pt):
            continue
        aid = r.get("מספר נכס")
        rec = assets.get(aid)
        if rec is None:
            use = r.get("שימוש עיקרי")
            name = r.get("שם נכס")
            rec = assets[aid] = {
                "asset_id": aid,
                "name": name,
                "use": use,
                "domain": classify_domain(use, name),
                "tenure": ten,
                "state": tenure_state(r.get("סטטוס")),
                "owner": r.get("בעלים"),
                # אחראי תפעול נכס — the municipal dept operating the asset
                # (אגף חינוך/חברה/חירום…). Asset-level, from the OPEN property
                # book — the authoritative "operated-by-the-city" signal.
                "operator_dept": (r.get("אחראי תפעול נכס") or "").strip() or None,
                "status": r.get("סטטוס"),
                "status_date": _date(r.get("תאריך סטטוס")),
                "opened": _date(r.get("תאריך פתיחת נכס")),
                "built_sqm": r.get('שטח בנוי במ"ר') or 0,
                "street": r.get("שם רחוב"),
                "house": r.get("מספר בית"),
                "neighborhood": r.get("שם שכונה"),
                "parcels": [],
                "pt": pt,
                "allocations": alloc_by_asset.get(aid, []),
            }
        gh = f"{g}/{h}"
        if gh not in rec["parcels"]:
            rec["parcels"].append(gh)

    # Split infrastructure land-tenure (roads/paths/utility) from public assets,
    # and drop city-OWNED assets (not held-via-rental) to match the source's
    # מקור-זכות filter (see is_city_owned).
    infra = [a for a in assets.values() if a["domain"] == "infra"]
    non_infra = [a for a in assets.values() if a["domain"] != "infra"]
    city_owned = [a for a in non_infra if is_city_owned(a["owner"])]
    public = [a for a in non_infra if not is_city_owned(a["owner"])]

    # ── zoning enrichment: place each asset on our land-use layer to read its
    # statutory designation, and flag שימוש חורג (institutional use on מגורים).
    yk = _load(_find(["..", "data", "yiud_karka_kayam.geojson"]))["features"]
    zpolys, zdescr = [], []
    for f in yk:
        if not f.get("geometry"):
            continue
        try:
            g = shape(f["geometry"])
            if not g.is_valid:
                g = g.buffer(0)
        except Exception:
            continue
        zpolys.append(g)
        zdescr.append((f["properties"].get("Descr") or "").strip())
    ztree = STRtree(zpolys)
    for a in public:
        p = Point(a["pt"])
        z = None
        for idx in ztree.query(p, predicate="intersects"):
            z = zdescr[idx]
            if z:
                break
        a["zoning"] = z or None
        a["residential"] = bool(z and RESIDENTIAL_RE.search(z))
        a["nonconforming"] = bool(a["residential"]
                                  and a["domain"] in NONCONFORMING_DOMAINS)
        a["pikuach"] = None

    # ── operator hint: for educational assets, read פיקוח (ממלכתי/מוכר…) from
    # the nearest education_shanaton institution (≤60m). Lets the user judge
    # whether the activity is municipally run — combined app-side with the
    # allocation-book org (which עמותה operates it). No auto-classification.
    edu = _load(_find(["..", "data", "education_shanaton.geojson"]))["features"]
    edu_pts = []
    for f in edu:
        gg = f.get("geometry")
        if not gg or gg.get("type") != "Point":
            continue
        piks = [i.get("pikuach") for i in (f["properties"].get("institutions")
                or []) if i.get("pikuach")]
        if piks:
            edu_pts.append((gg["coordinates"], piks))

    def _pikuach_near(pt):
        best, bestd = None, 60.0
        for c, piks in edu_pts:
            dx = (pt[0] - c[0]) * 93000.0
            dy = (pt[1] - c[1]) * 111000.0
            d = (dx * dx + dy * dy) ** 0.5
            if d < bestd:
                bestd, best = d, piks
        return best

    for a in public:
        if a["domain"] == "education":
            piks = _pikuach_near(a["pt"])
            if piks:
                # dedupe, keep order
                seen_p = []
                for x in piks:
                    if x not in seen_p:
                        seen_p.append(x)
                a["pikuach"] = seen_p

    tenure_counts = collections.Counter(a["tenure"] for a in public)
    domain_counts = collections.Counter(a["domain"] for a in public)
    n_nonconf = sum(1 for a in public if a["nonconforming"])

    features = []
    for a in sorted(public, key=lambda x: (x["domain"], str(x["asset_id"]))):
        addr = " ".join(str(x) for x in (a["street"], a["house"]) if x)
        props = {k: a[k] for k in (
            "asset_id", "name", "use", "domain", "tenure", "state", "owner",
            "status", "status_date", "opened", "built_sqm", "neighborhood",
            "parcels", "allocations", "zoning", "residential",
            "nonconforming", "pikuach", "operator_dept")}
        props["domain_label"] = DOMAIN_LABEL.get(a["domain"], "אחר")
        props["address"] = addr or None
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": a["pt"]},
            "properties": props,
        })

    # Sanity floor: ~100 public assets in Oranim on 2026-08. A near-empty
    # result means an input broke — abort, keep the committed file.
    if len(features) < 40:
        raise SystemExit(
            f"suspiciously few public rental assets ({len(features)}) "
            f"— aborting without writing")

    result = {
        "type": "FeatureCollection",
        "meta": {
            "built_at": datetime.date.today().isoformat(),
            "source_refresh": prop.get("meta", {}).get("fetched_at"),
            "public_assets": len(features),
            "infra_excluded": len(infra),
            "city_owned_excluded": len(city_owned),
            "nonconforming": n_nonconf,
            "tenure": dict(tenure_counts),
            "by_domain": {DOMAIN_LABEL.get(k, k): v
                          for k, v in domain_counts.most_common()},
        },
        "features": features,
    }

    # ── rental_flags.json — "מוחזק בשכירות" flag for existing-building popups ──
    # Same join pattern as build_asset_allocations.py: place each rental asset
    # via its centroid inside a shavaz_kayam polygon (by feature fid) so the
    # shavaz-popup can show the tenure fact WITHOUT loading the rental layer.
    # A housing (דירה) rental is flagged too — in our model it is an הפרשה
    # מבונה, so the popup labels it as such.
    _write_rental_flags(public)

    dest = os.path.join(os.path.dirname(
        _find(["..", "data", "district_oranim.geojson"])),
        "rental_public_assets.geojson")
    if os.path.exists(dest):
        try:
            old = _load(dest)
            if old.get("features") == features:
                print("no content change — keeping existing file")
                return
        except Exception:
            pass
    with open(dest, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, separators=(",", ":"))
    print(f"public rental/lease assets: {len(features)} "
          f"(שכירות {tenure_counts.get('שכירות', 0)} / "
          f"חכירה {tenure_counts.get('חכירה', 0)}); "
          f"infra excluded: {len(infra)}")
    print("by domain: " + ", ".join(
        f"{DOMAIN_LABEL.get(k, k)}={v}" for k, v in domain_counts.most_common()))
    print(f"wrote -> {dest}")


if __name__ == "__main__":
    main()
