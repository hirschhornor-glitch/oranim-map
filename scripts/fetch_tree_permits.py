# -*- coding: utf-8 -*-
"""
fetch_tree_permits.py — Tree-cutting permits (אישורי כריתה) open for objection,
for the Oranim project area.

Source: Meirim open API (https://api.meirim.org/api), which aggregates the
official פקיד היערות / gov.il regional XLS feeds + the Jerusalem municipality
trees-conservation page and geocodes each permit to a WGS84 polygon.
See memory: trees come from gov.il (felling before/after XLS, regional office
"ירושלים") + jerusalem.muni.il. Meirim is the convenient pre-geocoded layer;
the gov.il XLS is the documented fallback.

What it does:
  1. Pages through  GET /api/tree/?PLACE=ירושלים  (≈2000 permits, 20/page).
  2. Keeps permits whose last_date_to_objection is still open (>= today − GRACE).
  3. Clips to the project area (centroid inside data/district_oranim.geojson).
  4. Writes data/tree_permits.json — keyed dict, same shape conventions as
     objections_permits.json, dates as dd/mm/yyyy so the app's existing
     objectionsDaysLeft() parser works unchanged.

Re-run periodically (the objection window is ~14 days; new permits appear
between runs only by re-scraping). Idempotent — overwrites the json.
"""
import json, sys, io, time, datetime, urllib.parse, urllib.request, os, re

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

API = "https://api.meirim.org/api/tree/"
PLACE = "ירושלים"
GRACE_DAYS = 0           # only permits whose objection window is still open (deadline >= today)
HERE = os.path.dirname(os.path.abspath(__file__))


def _find_data_dir():
    # Works both locally (script in C:\ORANIM, data under oranim-app/data) and
    # in CI (repo root IS the app, script copied to scripts/, data at ../data).
    candidates = [
        os.path.join(HERE, "oranim-app", "data"),  # local: root ORANIM dir
        os.path.join(HERE, "data"),                # script sitting at repo root
        os.path.join(HERE, "..", "data"),          # CI: script under scripts/
    ]
    for d in candidates:
        if os.path.isfile(os.path.join(d, "district_oranim.geojson")):
            return os.path.abspath(d)
    raise SystemExit("could not locate data/ dir (district_oranim.geojson) near " + HERE)


DATA_DIR = _find_data_dir()
DISTRICT = os.path.join(DATA_DIR, "district_oranim.geojson")
OUT = os.path.join(DATA_DIR, "tree_permits.json")

UA = {"User-Agent": "Mozilla/5.0 (oranim-tree-permits-fetch)"}


def fetch_page(page, attempts=3):
    # Retry every page (incl. page 1) — a transient Meirim hiccup on the first
    # request or the final json.load must not crash the whole CI job.
    qs = urllib.parse.urlencode({"PLACE": PLACE, "page": page})
    last = None
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(API + "?" + qs, headers=UA)
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r)
        except Exception as e:
            last = e
            print(f"  page {page} retry {attempt+1}/{attempts}: {e}")
            time.sleep(2 * (attempt + 1))
    raise last


def fetch_all():
    first = fetch_page(1)
    pag = first.get("pagination", {})
    pages = pag.get("pageCount", 1)
    rows = list(first["data"])
    print(f"rowCount={pag.get('rowCount')} pages={pages}")
    for p in range(2, pages + 1):
        try:
            rows.extend(fetch_page(p)["data"])
        except Exception as e:
            print(f"  page {p} failed after retries, skipping: {e}")
        if p % 20 == 0:
            print(f"  ...fetched page {p}/{pages}")
        time.sleep(0.15)
    print(f"total rows fetched: {len(rows)}")
    return rows


# ---- geometry helpers (no shapely dependency for PIP; shapely for centroid) ----
from shapely.geometry import shape, Point


def load_district_polygon():
    gj = json.load(open(DISTRICT, encoding="utf-8"))
    feats = gj.get("features", [gj])
    geoms = [shape(f["geometry"]) for f in feats if f.get("geometry")]
    poly = geoms[0]
    for g in geoms[1:]:
        poly = poly.union(g)
    return poly


def iso_to_ddmmyyyy(s):
    if not s:
        return ""
    d = str(s)[:10]
    try:
        y, m, dd = d.split("-")
        return f"{dd}/{m}/{y}"
    except Exception:
        return ""


def iso_date(s):
    return str(s)[:10] if s else ""


def fix_reversed_digits(s):
    # Meirim's free-text fields (e.g. person_request_name) are extracted from a
    # visually-ordered RTL source, so embedded house numbers come out reversed
    # ("עין צורים 18" → "עין צורים 81"). The parsed `street` field is fine, but
    # the requester string isn't. Reverse standalone runs of 2–3 digits (house
    # numbers); leave 1-digit and 4+-digit runs (years/large ids) untouched.
    if not s:
        return s
    return re.sub(r"(?<!\d)(\d{2,3})(?!\d)", lambda m: m.group(1)[::-1], str(s))


PARCELS = os.path.join(DATA_DIR, "parcel_centroids.json")
BUILDINGS = os.path.join(DATA_DIR, "buildings.geojson")


def load_buildings_index():
    # street + house number → building point. Lets us place a permit by address
    # ("אמציה 3א" → building "אמציה 3") when the cadastral helka lookup misses.
    if not os.path.isfile(BUILDINGS):
        return None
    gj = json.load(open(BUILDINGS, encoding="utf-8"))
    idx = {}
    for f in gj.get("features", []):
        p = f.get("properties", {})
        st = (p.get("street") or "").strip()
        hn = re.sub(r"\D", "", str(p.get("house_num") or ""))
        g = f.get("geometry") or {}
        if not st or not hn or g.get("type") != "Point":
            continue
        idx.setdefault(st + "|" + hn, g["coordinates"])
    print(f"buildings index: {len(idx)} street/number addresses")
    return idx


def parse_street_house(r):
    # Return (street_name, house_digits) from Meirim's street/street_number.
    # Meirim often jams the number into `street` with a Hebrew sub-lot letter
    # ("אמציה 3א"); strip the letter and take the digits ("אמציה", "3").
    street = (r.get("street") or "").strip()
    num = r.get("street_number")
    if num:
        h = re.sub(r"\D", "", str(num))
        return (street, h) if h and h != "0" else (None, None)
    m = re.search(r"^(.*?)\s+(\d+)\s*[א-ת]?\.?\s*$", street)
    if not m:
        return (None, None)
    name, h = m.group(1).strip(), m.group(2)
    return (name, h) if h and h != "0" else (None, None)


def load_parcel_index():
    # Built once by build_parcel_centroids.py. Optional — if absent, we just
    # fall back to whatever location Meirim provides.
    if not os.path.isfile(PARCELS):
        print("note: parcel_centroids.json not found — skipping parcel-based geocoding")
        return None
    idx = json.load(open(PARCELS, encoding="utf-8"))
    print(f"parcel index: {len(idx.get('gush_helka', {}))} parcels, {len(idx.get('gush', {}))} gushim")
    return idx


def parse_gush_helka(r):
    # gush/helka fields from Meirim are messy: helka may sit in the gush field
    # ("30285, 223" = gush 30285 / helka 223), be "0"/empty, or a comma list.
    gtoks = re.findall(r"\d+", str(r.get("gush") or ""))
    htoks = [t for t in re.findall(r"\d+", str(r.get("helka") or "")) if t != "0"]
    gush = gtoks[0] if gtoks else None
    helkot = htoks if htoks else (gtoks[1:] if len(gtoks) >= 2 else [])
    return gush, helkot


def resolve_location(r, idx, buildings, meirim_centroid, has_footprint):
    # Returns (lnglat[lng,lat], loc_source) — best available position.
    #   meirim_polygon   : Meirim gave a real footprint (most precise)
    #   parcel_helka     : matched the exact חלקה centroid from the cadastre
    #   address_building : matched street + house number to a building point
    #   parcel_gush      : matched the גוש centroid (block-level, ~approximate)
    #   meirim_point     : Meirim's bare point (may be a shared default → flagged later)
    if has_footprint and meirim_centroid is not None:
        return [round(meirim_centroid.x, 7), round(meirim_centroid.y, 7)], "meirim_polygon"
    gush, helkot = parse_gush_helka(r)
    if idx and gush:
        for h in helkot:
            hit = idx["gush_helka"].get(f"{gush}/{h}")
            if hit:
                return hit, "parcel_helka"
    if buildings:
        name, house = parse_street_house(r)
        if name and house:
            bhit = buildings.get(name + "|" + house)
            if bhit:
                return [round(bhit[0], 7), round(bhit[1], 7)], "address_building"
    if idx and gush:
        ghit = idx["gush"].get(gush)
        if ghit:
            return ghit, "parcel_gush"
    if meirim_centroid is not None:
        return [round(meirim_centroid.x, 7), round(meirim_centroid.y, 7)], "meirim_point"
    return None, None


def main():
    today = datetime.date.today()
    cutoff = (today - datetime.timedelta(days=GRACE_DAYS)).isoformat()
    rows = fetch_all()

    district = load_district_polygon()
    idx = load_parcel_index()
    buildings = load_buildings_index()
    out = {}
    kept_total = skipped_deadline = skipped_outside = skipped_noloc = 0
    src_counts = {}

    for r in rows:
        deadline_iso = iso_date(r.get("last_date_to_objection"))
        if not deadline_iso or deadline_iso < cutoff:
            skipped_deadline += 1
            continue

        # Best available Meirim geometry → centroid + footprint flag.
        geom = r.get("geom")
        meirim_centroid = None
        has_footprint = False
        if geom:
            try:
                g = shape(geom)
                if not g.centroid.is_empty:
                    meirim_centroid = g.centroid
                    has_footprint = g.area > 0.0
            except Exception:
                pass

        lnglat, loc_source = resolve_location(r, idx, buildings, meirim_centroid, has_footprint)
        if lnglat is None:
            skipped_noloc += 1
            continue
        if not district.contains(Point(lnglat[0], lnglat[1])):
            skipped_outside += 1
            continue

        street = (r.get("street") or "").strip()
        num = r.get("street_number")
        address = (street + (" " + str(num) if num else "")).strip() or (r.get("place") or "")
        key = str(r.get("id"))
        out[key] = {
            "id": r.get("id"),
            "permit_number": r.get("permit_number") or "",
            "address": address,
            "street": street,
            "place": r.get("place") or "",
            "reason": r.get("reason_short") or "",
            "reason_detailed": r.get("reason_detailed") or "",
            "requester": fix_reversed_digits(r.get("person_request_name") or ""),
            "approver": r.get("approver_name") or "",
            "approver_title": r.get("approver_title") or "",
            "regional_office": r.get("regional_office") or "",
            "action": r.get("action") or "",
            "total_trees": r.get("total_trees") or 0,
            "trees": r.get("trees_per_permit") or {},
            "gush": r.get("gush") or "",
            "helka": r.get("helka") or "",
            "issue_date": iso_to_ddmmyyyy(r.get("permit_issue_date")),
            "start_date": iso_to_ddmmyyyy(r.get("start_date")),
            "deadline": iso_to_ddmmyyyy(r.get("last_date_to_objection")),
            "lnglat": lnglat,
            "loc_source": loc_source,
            "geo_approx": False,  # refined below
            # Keep the real footprint only when Meirim supplied one.
            "geom": geom if (loc_source == "meirim_polygon") else None,
            "url": "https://meirim.org/tree/{}".format(r.get("id")),
        }
        kept_total += 1
        src_counts[loc_source] = src_counts.get(loc_source, 0) + 1

    # A Meirim bare point shared by 2+ permits is the geocoder's default fallback
    # (real parcels never collide to 7 decimals) → flag those as approximate.
    # Parcel/footprint-resolved points are precise and never flagged.
    from collections import Counter
    coord_counts = Counter(tuple(v["lnglat"]) for v in out.values())
    approx = 0
    for v in out.values():
        if v["loc_source"] == "meirim_point" and coord_counts[tuple(v["lnglat"])] > 1:
            v["geo_approx"] = True
            approx += 1

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))

    print("--- summary ---")
    print(f"today={today}  grace={GRACE_DAYS}d  cutoff>={cutoff}")
    print(f"kept (open for objection, in area): {kept_total}")
    print(f"location sources: {src_counts}")
    print(f"skipped: deadline={skipped_deadline}  outside-area={skipped_outside}  no-location={skipped_noloc}")
    print(f"geo_approx (unresolved shared default): {approx}")
    print(f"wrote {OUT}  ({os.path.getsize(OUT)} bytes)")


if __name__ == "__main__":
    main()
