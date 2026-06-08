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
import json, sys, io, time, datetime, urllib.parse, urllib.request, os

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

API = "https://api.meirim.org/api/tree/"
PLACE = "ירושלים"
GRACE_DAYS = 14          # keep permits whose deadline passed up to this many days ago
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


def fetch_page(page):
    qs = urllib.parse.urlencode({"PLACE": PLACE, "page": page})
    req = urllib.request.Request(API + "?" + qs, headers=UA)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def fetch_all():
    first = fetch_page(1)
    pag = first.get("pagination", {})
    pages = pag.get("pageCount", 1)
    rows = list(first["data"])
    print(f"rowCount={pag.get('rowCount')} pages={pages}")
    for p in range(2, pages + 1):
        for attempt in range(3):
            try:
                rows.extend(fetch_page(p)["data"])
                break
            except Exception as e:
                print(f"  page {p} retry {attempt+1}: {e}")
                time.sleep(2)
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


def main():
    today = datetime.date.today()
    cutoff = (today - datetime.timedelta(days=GRACE_DAYS)).isoformat()
    rows = fetch_all()

    district = load_district_polygon()
    out = {}
    kept_open = kept_total = skipped_deadline = skipped_outside = skipped_nogeom = 0

    for r in rows:
        deadline_iso = iso_date(r.get("last_date_to_objection"))
        if not deadline_iso or deadline_iso < cutoff:
            skipped_deadline += 1
            continue
        geom = r.get("geom")
        if not geom:
            skipped_nogeom += 1
            continue
        try:
            g = shape(geom)
            c = g.centroid
            if c.is_empty:
                raise ValueError("empty centroid")
        except Exception:
            skipped_nogeom += 1
            continue
        if not district.contains(Point(c.x, c.y)):
            skipped_outside += 1
            continue
        has_footprint = g.area > 0.0  # real parcel polygon vs a bare point

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
            "requester": r.get("person_request_name") or "",
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
            "lnglat": [round(c.x, 7), round(c.y, 7)],
            "geo_approx": False,  # set below: true when several permits share one coord
            "geom": geom if has_footprint else None,
            "url": "https://meirim.org/tree/{}".format(r.get("id")),
        }
        kept_total += 1
        if deadline_iso >= today.isoformat():
            kept_open += 1

    # Mark records whose coordinate is shared by another permit — that signals
    # Meirim's geocoder fell back to a default point (real parcels never collide
    # to 7 decimals). The map shows these as approximate, not precise.
    from collections import Counter
    coord_counts = Counter(tuple(v["lnglat"]) for v in out.values())
    approx = 0
    for v in out.values():
        if coord_counts[tuple(v["lnglat"])] > 1:
            v["geo_approx"] = True
            v["geom"] = None
            approx += 1

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))

    print("--- summary ---")
    print(f"today={today}  grace={GRACE_DAYS}d  cutoff>={cutoff}")
    print(f"kept (in area, deadline open/recent): {kept_total}   of which still OPEN today: {kept_open}")
    print(f"skipped: deadline={skipped_deadline}  outside-area={skipped_outside}  no-geom={skipped_nogeom}")
    print(f"geo_approx (shared default coord): {approx}")
    print(f"wrote {OUT}  ({os.path.getsize(OUT)} bytes)")


if __name__ == "__main__":
    main()
