# -*- coding: utf-8 -*-
"""
fetch_tree_permits.py — Tree-cutting permits (אישורי כריתה) open for objection,
for the Oranim project area.

Source (2026-07-08 onward): the official פקיד היערות / משרד החקלאות **יעל"ה**
public portal — https://yeela-trees.moag.gov.il/FoPublic/FoLicence — via its
anonymous grid API:

    POST /api/Fo/FOServiceRequest/getFOGridPublicityLicenses

This REPLACES the previous Meirim source, whose Jerusalem feed silently froze on
2026-05-31 (all its data moved to יעל"ה). The יעל"ה endpoint is public — no login,
no cookies, no reCAPTCHA on this endpoint (the site's reCAPTCHA is only on the
user-exists check) — and returns fresh data with a real objection deadline
(appealLastDate) per license. See memory project_tree_cutting_permits_layer.

What it does:
  1. Pages the grid filtered to licenseStatusId=3 ("מושהה ופתוח להגשת השגה" =
     suspended & open for objection) — the only status where an objection can be
     filed. ~hundreds nationwide, grouped by requestId into distinct licenses.
  2. Keeps licenses whose appealLastDate is still open (>= today − GRACE).
  3. Geocodes each by גוש/חלקה → parcel_centroids.json, else street+house →
     buildings.geojson, else גוש centroid. The parcel/building indexes only cover
     the Oranim bbox, so a license outside the area simply fails to geocode and is
     dropped — the geocode doubles as the spatial filter. A final centroid-in-
     district_oranim.geojson check clips precisely.
  4. Emails newly-appeared licenses, each row linked to the planning at its
     address: מינה"ק (from the boundary layers), the תב"ע whose polygon contains
     it, and any תמ"א 38 file / building permit at the same street+number (or
     nearby, with the distance shown). See attach_planning_context. When no
     surveyed plan covers the address, probe_site_trees goes a round deeper —
     XPLAN tree points on the license's own גוש/חלקה, then live XPLAN by envelope
     and by plan number — instead of reporting "no survey".
  5. Writes data/tree_permits.json — same schema the app already consumes
     (keyed dict, dates dd/mm/yyyy, nested trees {action:{species:n}}), so app.jsx
     needs no data-shape change.

Each grid row is ONE tree species of a license; rows sharing a requestId are one
license (aggregated here). Dependency: shapely (urllib is stdlib).
Re-run periodically (objection window ~14–21 days). Idempotent — overwrites json.
"""
import json, sys, io, time, datetime, urllib.parse, urllib.request, os, re

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE = "https://yeela-trees.moag.gov.il"
GRID = BASE + "/api/Fo/FOServiceRequest/getFOGridPublicityLicenses"
OPEN_STATUS_ID = 3        # "מושהה ופתוח להגשת השגה" — the only objection-open status
JERUSALEM_CITY_ID = 3000  # the Oranim project area is within Jerusalem municipality. Scope the query
                          # to Jerusalem so a same-named street in another city (יד מרדכי, שדה יצחק,
                          # אברבנאל all exist elsewhere) can't be geocoded into our building index.
GRACE_DAYS = 0            # keep only licenses whose objection window is still open (deadline >= today)
STALE_DAYS = 30           # source-health guard: open permits are inherently fresh (short objection
                          # window), so if יעל"ה freezes the open set drains within ~a month. Fail loud
                          # if zero open permits OR the newest approved one is older than this.
PAGE_SIZE = 100           # server caps pageSize at 100

HERE = os.path.dirname(os.path.abspath(__file__))

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": BASE,
    "Referer": BASE + "/FoPublic/FoLicence",
}

# Email notification when a NEW open-for-objection permit appears in the area.
# Same Gmail-App-Password SMTP convention as the project's other scans
# (monthly_tree_scan / detect_new_plans). Runs in GitHub Actions: the workflow
# passes GMAIL_APP_PASSWORD + EMAIL_RECIPIENT from repo secrets. If the password
# isn't set (e.g. local run), the notification is silently skipped.
EMAIL_SENDER    = "hirschhorn.or@gmail.com"
EMAIL_PASSWORD  = os.environ.get("GMAIL_APP_PASSWORD", "")
# `or` (not a get-default): the workflow passes EMAIL_RECIPIENT from a repo secret
# that may not exist, which yields an EMPTY string (not an absent key), so a plain
# get-default would keep "" and the notifier would skip for "no recipient".
EMAIL_RECIPIENT = os.environ.get("EMAIL_RECIPIENT") or "Or_hi@jerusalem.muni.il"


def _find_data_dir():
    # Works both locally (script in C:\ORANIM, data under oranim-app/data) and in
    # CI (repo root IS the app, script copied to scripts/, data at ../data).
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
PARCELS = os.path.join(DATA_DIR, "parcel_centroids.json")
BUILDINGS = os.path.join(DATA_DIR, "buildings.geojson")
SURVEYS = os.path.join(DATA_DIR, "tree_surveys.json")   # taba -> {total,krita,shimur,haataka,...}
PLANS = os.path.join(DATA_DIR, "plans.geojson")         # plan polygons (taba, plan_type, name)
OUT = os.path.join(DATA_DIR, "tree_permits.json")

# Plan types the map popup ignores when picking the surveyed plan for a permit.
SKIP_PLAN_TYPES = {"תשתיות", "מוסתר"}

# --- planning context attached to each notified license (see attach_planning_context) ---
TAMA38 = os.path.join(DATA_DIR, "tama38.geojson")            # תמ"א 38 file registry (points)
PERMITS_MASTER = os.path.join(DATA_DIR, "permits_master.json")  # building permits by tik (points)

DEG2_TO_DUNAM = 1.049e7   # deg² → dunam at lat ~31.77 (110.54 km/° lat × 94.9 km/° lng)
# Five city-wide policy plans (תקן חניה, קווי בניין למרפסות, הוראות בינוי…) cover
# all ~130,000 dunam of Jerusalem, so they contain every license point and say
# nothing about the address. The next-largest real plan is 1,178 dunam, so any
# 5,000+ dunam polygon is one of those blankets — drop it.
BLANKET_PLAN_DUNAM = 5000
MAX_LINKED_PLANS = 2      # smallest containing plans to list per license
TABA_NEAR_M = 40          # no plan contains the point → the closest ones within this,
                          # shown with their distance (a license on the parcel next door
                          # to a plan is worth knowing about, but it isn't the same thing)
# Dead statuses: a shelved/rejected plan next door explains nothing about a felling,
# so it never earns a spot in the proximity fallback (a plan that CONTAINS the point
# is still worth showing whatever its status).
DEAD_PLAN_STATUS = {"נגנזה", "נדחתה", "ביטול פרסום"}
LINK_RADIUS_M = 50        # proximity fallback when no exact street+number match
MAX_LINKED_PERMITS = 2
MIN_LINK_YEAR = datetime.date.today().year - 10   # proximity matches only, see _match_points


# ---------------------------------------------------------------- API paging ----
def fetch_grid_page(page, attempts=4):
    # Retry every page — transient truncation/timeout shouldn't crash the run.
    # Returns the parsed json, or None if all attempts fail.
    body = {
        "orderDetails": {"orderFieldName": "requestId", "orderType": 1},
        "pageDetails": {"pageNumber": page, "pageSize": PAGE_SIZE},
        # licenseStatusId narrows to objection-open licenses; cityId to Jerusalem.
        # Both filters are applied server-side. The gush/helka+address geocode then
        # clips within Jerusalem to the Oranim district.
        "parameters": {"zoneId": None, "cityId": JERUSALEM_CITY_ID, "appealLastDate": None,
                       "licenseId": None, "licenseStatusId": OPEN_STATUS_ID},
    }
    last = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(GRID, data=json.dumps(body).encode("utf-8"),
                                         headers=UA, method="POST")
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r)
        except Exception as e:
            last = e
            print(f"  grid page {page} retry {i + 1}/{attempts}: {e}")
            time.sleep(2 * (i + 1))
    print(f"  ⚠️ grid page {page} FAILED after {attempts} attempts: {last}")
    return None


def fetch_all_open():
    # Jerusalem open-for-objection licenses (the actual data we render).
    first = fetch_grid_page(1)
    if first is None:
        raise SystemExit("could not fetch יעל\"ה grid page 1 — API unavailable")
    pag = first.get("pagination", {})
    pages = pag.get("totalPages", 1)
    total = pag.get("totalCount", 0)
    rows = list(first.get("result", []))
    print(f"open-for-objection licenses in Jerusalem: totalCount={total} pages={pages}")
    for p in range(2, pages + 1):
        page = fetch_grid_page(p)
        if page is None:
            continue
        rows.extend(page.get("result", []))
        time.sleep(0.15)
    print(f"total species-rows fetched: {len(rows)}")
    return rows, total


def probe_source_health():
    # Nationwide freshness probe (independent of Jerusalem, which may legitimately
    # have 0 open some weeks). Returns (nationwide_open_total, newest_approved_iso).
    # Used only to detect a frozen/broken source — NOT to gate the Jerusalem layer.
    body = {
        "orderDetails": {"orderFieldName": "requestId", "orderType": 1},
        "pageDetails": {"pageNumber": 1, "pageSize": PAGE_SIZE},
        "parameters": {"zoneId": None, "cityId": None, "appealLastDate": None,
                       "licenseId": None, "licenseStatusId": OPEN_STATUS_ID},
    }
    try:
        req = urllib.request.Request(GRID, data=json.dumps(body).encode("utf-8"), headers=UA, method="POST")
        with urllib.request.urlopen(req, timeout=60) as r:
            j = json.load(r)
    except Exception as e:
        print(f"  ⚠️ source-health probe failed: {e}")
        return None, ""
    total = j.get("pagination", {}).get("totalCount", 0)
    newest = ""
    for r in j.get("result", []):
        ad = iso_date(r.get("approvedDate"))
        if ad and ad > newest:
            newest = ad
    return total, newest


# ------------------------------------------------------------- geometry / geo ---
from shapely.geometry import shape, Point


def load_district_polygon():
    gj = json.load(open(DISTRICT, encoding="utf-8"))
    feats = gj.get("features", [gj])
    geoms = [shape(f["geometry"]) for f in feats if f.get("geometry")]
    poly = geoms[0]
    for g in geoms[1:]:
        poly = poly.union(g)
    return poly


_PARCEL_IDX = "unset"   # memoized: the tree probe needs the same index main() loads


def load_parcel_index():
    global _PARCEL_IDX
    if _PARCEL_IDX != "unset":
        return _PARCEL_IDX
    if not os.path.isfile(PARCELS):
        print("note: parcel_centroids.json not found — skipping parcel-based geocoding")
        _PARCEL_IDX = None
        return None
    idx = json.load(open(PARCELS, encoding="utf-8"))
    print(f"parcel index: {len(idx.get('gush_helka', {}))} parcels, {len(idx.get('gush', {}))} gushim")
    _PARCEL_IDX = idx
    return idx


def load_buildings_index():
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


def parse_street_house(street):
    # "גולדה מאיר  5" → ("גולדה מאיר", "5"); strip a trailing Hebrew sub-lot letter.
    m = re.search(r"^(.*?)\s+(\d+)\s*[א-ת]?\.?\s*$", (street or "").strip())
    if not m:
        return None, None
    name, h = m.group(1).strip(), m.group(2)
    return (name, h) if h and h != "0" else (None, None)


def resolve_location(gush, helkot, street, idx, buildings):
    # Returns (lnglat[lng,lat], loc_source) — best available position, or (None, None).
    #   parcel_helka     : exact חלקה centroid from the cadastre (most precise)
    #   address_building : street + house number → building point
    #   parcel_gush      : גוש centroid (block-level, ~approximate → "מרכז גוש" badge)
    if idx and gush:
        for h in helkot:
            hit = idx["gush_helka"].get(f"{gush}/{h}")
            if hit:
                return hit, "parcel_helka"
    if buildings:
        name, house = parse_street_house(street)
        if name and house:
            bhit = buildings.get(name + "|" + house)
            if bhit:
                return [round(bhit[0], 7), round(bhit[1], 7)], "address_building"
    if idx and gush:
        ghit = idx["gush"].get(gush)
        if ghit:
            return ghit, "parcel_gush"
    return None, None


# ---------------------------------------------------------------- formatting ----
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


# יעל"ה removal-type counts → the action buckets the app's popup understands.
ACTION_FOR = [("unproot", "כריתה"), ("copying", "העתקה"), ("conservation", "שימור")]


def build_license(rows):
    # rows = all species-rows sharing one requestId. Aggregate into one license.
    r0 = rows[0]
    ex = (r0.get("expandRows") or [{}])[0]

    # nested trees {action:{species:count}} — sum counts per species across rows.
    trees = {}
    total = 0
    for r in rows:
        sp = (r.get("treeName") or "").strip() or "עץ"
        for fld, action in ACTION_FOR:
            n = int(r.get(fld) or 0)
            if n <= 0:
                continue
            trees.setdefault(action, {})
            trees[action][sp] = trees[action].get(sp, 0) + n
            total += n

    # short action label for the popup's "פעולה" row.
    present = [a for _, a in ACTION_FOR if a in trees]
    removal = [a for a in present if a in ("כריתה", "העתקה")]
    action = " ו".join(removal) if removal else (present[0] if present else "")

    # collect all גוש/חלקה across the license's rows for a robust geocode.
    gushes, helkot = [], []
    for r in rows:
        for e in (r.get("expandRows") or []):
            gushes += re.findall(r"\d+", str(e.get("block") or ""))
            helkot += [t for t in re.findall(r"\d+", str(e.get("parcel") or "")) if t != "0"]
    gush = gushes[0] if gushes else None

    street = (ex.get("street") or "").strip()
    lic = r0.get("licenseId")
    return {
        "id": r0.get("requestId"),
        "permit_number": str(lic) if lic else "",
        "address": street or (r0.get("cityName") or ""),
        "street": street,
        "place": r0.get("cityName") or "",
        "reason": ex.get("requestReason") or ex.get("shortHebDesc") or "",
        "reason_detailed": ex.get("shortHebDesc") or "",
        "requester": (ex.get("customerName") or "").strip(),
        "approver": (ex.get("approvedBy") or "").strip(),
        "approver_title": "",
        "regional_office": r0.get("zoneName") or "",
        "action": action,
        "total_trees": total,
        "trees": trees,
        "gush": ex.get("block") or "",
        "helka": ex.get("parcel") or "",
        "issue_date": iso_to_ddmmyyyy(r0.get("approvedDate") or ex.get("dateOfIssue") or ex.get("dateOLicensePublic")),
        "start_date": "",
        "deadline": iso_to_ddmmyyyy(r0.get("appealLastDate")),
        # geometry filled below
        "lnglat": None, "loc_source": None, "geo_approx": False, "geom": None,
        "url": BASE + "/FoPublic/FoLicence",
        # keep raw parts for geocoding, popped before write
        "_gush": gush, "_helkot": helkot,
    }


def _cut_count(rec):
    # Trees the permit asks to CUT (כריתה bucket) — mirrors the app's treePermitCutCount.
    return sum(int(v) for v in ((rec.get("trees") or {}).get("כריתה") or {}).values())


_PLAN_POLYS = None


def load_plan_polys():
    """[(shapely geom, properties, dunam)] for every site-specific plan polygon.

    Shared by the survey check and the planning-context linkage, so plans.geojson
    is parsed once. Infrastructure/hidden plans and the city-wide blanket plans
    are dropped here — neither tells you anything about a specific address.
    """
    global _PLAN_POLYS
    if _PLAN_POLYS is not None:
        return _PLAN_POLYS
    _PLAN_POLYS = []
    try:
        feats = json.load(open(PLANS, encoding="utf-8")).get("features", [])
    except Exception as e:
        print(f"  [link] could not load plans.geojson ({e}) — no plan linkage.")
        return _PLAN_POLYS
    for f in feats:
        p = f.get("properties") or {}
        if (p.get("plan_type") or "") in SKIP_PLAN_TYPES:
            continue
        g = f.get("geometry")
        if not g:
            continue
        try:
            geom = shape(g)
        except Exception:
            continue
        dunam = geom.area * DEG2_TO_DUNAM
        if dunam > BLANKET_PLAN_DUNAM:
            continue
        _PLAN_POLYS.append((geom, p, dunam))
    return _PLAN_POLYS


def plans_at(ll, near_ok=True):
    """[(props, dist_m)] for the plans at the point — containing ones (dist 0) first.

    A license geocoded to a parcel centroid can sit tens of metres from the plan
    that actually governs the trees (the plan covers the parcel next door, or its
    blue line stops at the building). So when nothing contains the point we fall
    back to the nearest plans within TABA_NEAR_M, tagged with their distance —
    unless near_ok is off, i.e. the address already answered for itself (a תמ"א 38
    file on this very address), in which case the neighbours are just noise.
    """
    pt = Point(ll[0], ll[1])
    inside, near = [], []
    for geom, p, dunam in load_plan_polys():
        try:
            if geom.contains(pt):
                inside.append((dunam, p, 0.0))
                continue
            if near_ok and not inside and (p.get("status_mavat") or "") not in DEAD_PLAN_STATUS:
                d = geom.distance(pt) * 111000     # degrees → metres, close enough
                if d <= TABA_NEAR_M:
                    near.append((d, p, d))
        except Exception:
            continue
    hits = sorted(inside, key=lambda t: t[0]) or sorted(near, key=lambda t: t[0])
    return [(p, d) for _, p, d in hits]


def attach_survey_warnings(recs):
    # For each permit, find the SMALLEST surveyed plan whose polygon contains the
    # permit point (skipping תשתיות/מוסתר plans, like the map popup) and flag when
    # the requested כריתה exceeds the survey's recommended כריתה — the same
    # "מבוקשים לכריתה N מעל M שהומלצו בסקר" gap the popup shows. Best-effort: if the
    # survey/plans data is missing or unreadable, permits simply carry no warning.
    if not os.path.isfile(SURVEYS) or not os.path.isfile(PLANS):
        return
    try:
        surveys = {k: v for k, v in json.load(open(SURVEYS, encoding="utf-8")).items()
                   if not k.startswith("_") and (v or {}).get("total", 0) > 0}
    except Exception as e:
        print(f"  [survey] could not load surveys ({e}) — skipping gap check.")
        return
    cand = []  # (shapely geom, taba, plan name) for surveyed, site-specific plans
    for geom, p, _dunam in load_plan_polys():
        taba = str(p.get("taba") or "").strip()
        if taba in surveys:
            cand.append((geom, taba, p.get("plan_name_he") or p.get("plan_summary") or taba))
    for rec in recs:
        ll = rec.get("lnglat")
        if not ll:
            continue
        pt = Point(ll[0], ll[1])
        best = None
        best_area = None
        for geom, taba, name in cand:
            try:
                if geom.contains(pt) and (best_area is None or geom.area < best_area):
                    best_area, best = geom.area, (taba, name)
            except Exception:
                continue
        if not best:
            # No surveyed plan holds this point — dig one level down instead of
            # reporting "no survey": see probe_site_trees. Skipped when a תמ"א 38
            # file sits on the address itself; the felling belongs to that file,
            # and the neighbours' surveys would only muddy it.
            if not (rec.get("_ctx") or {}).get("tama38_exact"):
                probe_site_trees(rec, surveys)
            continue
        taba, name = best
        s = surveys[taba]
        cut, krita = _cut_count(rec), int(s.get("krita") or 0)
        if cut > krita:
            rec["_survey_warn"] = {"plan": name, "cut": cut, "krita": krita,
                                   "total": s.get("total", 0), "shimur": s.get("shimur", 0),
                                   "haataka": s.get("haataka", 0)}


# ------------------------------------------- second round: survey at the site ---
# When no surveyed plan contains the license point we don't stop at "אין סקר".
# The plan that surveyed these trees is often RIGHT THERE and just misses the
# point: it covers the parcel next door, its blue line stops at the building, or
# it's an infrastructure plan the containment step skips (101-0666289, the light
# rail, surveyed 229 trees 34 m from a license on עמק רפאים). So we go back to
# the raw XPLAN tree points — one point per surveyed tree, each carrying its plan
# — and count the ones standing on the license's own גוש/חלקה:
#   1. every חלקה the license lists → its cadastral centroid (parcel_centroids)
#   2. tree points from tree_points_xplan.json within SITE_PROBE_RADIUS_M of any
#      of them, grouped by plan and by כריתה/העתקה/שימור
#   3. nothing locally → ask XPLAN live (the local file is a periodic snapshot,
#      so a survey filed since the last refresh only exists upstream). Strictly
#      best-effort: iplan is often slow or down, and this must never fail the run.
SITE_PROBE_RADIUS_M = 45
# How the probe found what it found — spelled out in the email so a reader knows
# whether they're looking at our snapshot, a live lookup, or a whole-plan survey.
XPLAN_SOURCE_NOTE = {
    "xplan_points": "",
    "xplan_live": ", שאילתה חיה ל-XPLAN",
    "xplan_live_plan": ", שאילתה חיה ל-XPLAN לפי מספר תכנית — הסקר כולו, לא רק סביב החלקה",
}
TREE_POINTS = os.path.join(DATA_DIR, "tree_points_xplan.json")
XPLAN_TREES_URL = ("https://ags.iplan.gov.il/arcgisiplan/rest/services/"
                   "PlanningPublic/Xplan/MapServer/0/query")
XPLAN_TREE_STATUS = {22150: "krita", 22160: "haataka", 22170: "shimur"}
XPLAN_TIMEOUT = 25
XPLAN_ATTEMPTS = 2

_TREE_PTS = None


def load_tree_points():
    """[(lng, lat, status, taba, pl_number)] — one entry per surveyed tree."""
    global _TREE_PTS
    if _TREE_PTS is None:
        _TREE_PTS = []
        try:
            plans = json.load(open(TREE_POINTS, encoding="utf-8")).get("plans", {})
        except Exception as e:
            print(f"  [probe] could not load tree_points_xplan.json ({e}) — local probe off.")
            return _TREE_PTS
        for taba, rec in plans.items():
            pn = rec.get("pl_number") or taba
            for pt in rec.get("points", []):
                _TREE_PTS.append((pt["x"], pt["y"], pt.get("status") or "", taba, pn))
    return _TREE_PTS


def site_centroids(rec):
    """The license's own footprint: its point plus every gush/helka centroid it lists."""
    pts = [rec["lnglat"]]
    idx = load_parcel_index()
    if not idx:
        return pts
    gushim = re.findall(r"\d+", str(rec.get("gush") or ""))
    helkot = [h for h in re.findall(r"\d+", str(rec.get("helka") or "")) if h != "0"]
    for g in gushim:
        for h in helkot:
            c = idx["gush_helka"].get(f"{g}/{h}")
            if c and c not in pts:
                pts.append(c)
    return pts


def _xplan_trees(extra_params, label):
    """Live XPLAN tree-entity query → [(status, pl_number)]. [] on any failure.

    iplan is regularly slow, down, or fronted by an error page, and this runs
    inside the daily data job — so every failure mode returns [] with a note.
    """
    import ssl
    params = {
        "where": "mavat_code IN (%s)" % ",".join(str(c) for c in XPLAN_TREE_STATUS),
        "outFields": "pl_number,mavat_code", "returnGeometry": "false", "f": "json",
    }
    params.update(extra_params)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:                       # iplan still negotiates legacy ciphers
        ctx.set_ciphers("DEFAULT:@SECLEVEL=0")
    except Exception:
        pass
    url = XPLAN_TREES_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": UA["User-Agent"],
                                               "Accept": "application/json"})
    j = None
    for attempt in range(XPLAN_ATTEMPTS):     # iplan drops connections at random
        try:
            with urllib.request.urlopen(req, timeout=XPLAN_TIMEOUT, context=ctx) as r:
                if "json" not in (r.headers.get("Content-Type") or ""):
                    print(f"  [probe] XPLAN ({label}) answered with a non-JSON page (down or blocked).")
                    return []
                j = json.load(r)
            break
        except Exception as e:
            if attempt + 1 == XPLAN_ATTEMPTS:
                print(f"  [probe] XPLAN ({label}) query failed ({type(e).__name__}) — local data only.")
                return []
            time.sleep(2)
    out = []
    for f in j.get("features", []):
        a = f.get("attributes") or {}
        st = XPLAN_TREE_STATUS.get(a.get("mavat_code"))
        if st:
            out.append((st, str(a.get("pl_number") or "").strip()))
    if out:
        print(f"  [probe] XPLAN ({label}) returned {len(out)} surveyed trees.")
    return out


def fetch_xplan_trees_near(ll, radius_m=SITE_PROBE_RADIUS_M):
    """Surveyed trees standing within radius_m of the license point."""
    d = radius_m / 111000.0
    return _xplan_trees({
        "geometry": f"{ll[0] - d},{ll[1] - d},{ll[0] + d},{ll[1] + d}",
        "geometryType": "esriGeometryEnvelope", "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
    }, "envelope")


def fetch_xplan_trees_by_plan(pl_number):
    """The whole survey of one plan, by its number — the route that works when the
    trees sit outside our radius but the plan at the address is known."""
    safe = str(pl_number).replace("'", "")
    return _xplan_trees({"where": "mavat_code IN (%s) AND pl_number = '%s'"
                                  % (",".join(str(c) for c in XPLAN_TREE_STATUS), safe)},
                        f"plan {safe}")


def probe_site_trees(rec, surveys):
    """Stamp rec['_site_survey'] with the surveyed trees standing on this license's parcels."""
    centers = site_centroids(rec)
    found = []          # (status, pl_number)
    for x, y, st, taba, pn in load_tree_points():
        if any(_dist_m(c, (x, y)) <= SITE_PROBE_RADIUS_M for c in centers):
            found.append((st, pn))
    source = "xplan_points"
    if not found:
        found = fetch_xplan_trees_near(rec["lnglat"])
        source = "xplan_live"
    if not found:
        # Last round: ask by PLAN NUMBER. The plans linked to the address (from
        # attach_planning_context, which runs first) may hold a survey whose trees
        # fall outside our radius, and a plan we track but never surveyed is
        # exactly the case worth chasing upstream.
        for p, _d in ((rec.get("_ctx") or {}).get("plans") or []):
            pn = str(p.get("plan_name") or "").strip()
            if not pn or str(p.get("taba") or "").strip() in surveys:
                continue
            found = fetch_xplan_trees_by_plan(pn)
            if found:
                source = "xplan_live_plan"
                break
    if not found:
        return

    counts, by_plan = {}, {}
    for st, pn in found:
        counts[st] = counts.get(st, 0) + 1
        by_plan[pn] = by_plan.get(pn, 0) + 1
    plans = []
    for pn in sorted(by_plan, key=lambda k: -by_plan[k])[:2]:
        taba = re.sub(r"^\d+-0*", "", pn) or pn      # '101-1095124' → '1095124'
        plans.append({"pl_number": pn, "near": by_plan[pn], "survey": surveys.get(taba)})
    cut = _cut_count(rec)
    rec["_site_survey"] = {
        "radius": SITE_PROBE_RADIUS_M, "source": source, "spots": len(centers),
        "krita": counts.get("krita", 0), "shimur": counts.get("shimur", 0),
        "haataka": counts.get("haataka", 0), "total": sum(counts.values()),
        "cut": cut, "over": cut > counts.get("krita", 0), "plans": plans,
    }


# ---------------------------------------------- planning context (linkage) ----
# A felling license is an address; the planning question is what sits ON that
# address. Four answers, all matched to the license point:
#   מינה"ק   — minahak_pip.derive, the same boundary layers the app filters by
#   תב"ע     — point-in-polygon over plans.geojson (exact; smallest plan first)
#   תמ"א 38  — tama38.geojson file registry, plus permits flagged is_tama38
#   היתר     — permits_master.json, the non-תמ"א permits
# The two permit datasets are POINT layers: an identical street+number is a real
# match; anything else is proximity, and those cells carry their distance (~35 מ׳)
# so the reader can judge. Best-effort throughout — an unreadable dataset just
# leaves its column empty rather than failing the run.


def _addr_key(street):
    """'עמק רפאים 51א' → 'עמק רפאים|51'; '' when there is no usable house number."""
    name, house = parse_street_house(street)
    if not name or not house:
        return ""
    name = re.sub(r"[\"'`״׳]", "", name)
    return re.sub(r"\s+", " ", name).strip() + "|" + house


def _dist_m(a, b):
    # equirectangular approximation — exact enough at these distances/latitude
    import math
    mlat = math.radians((a[1] + b[1]) / 2)
    return math.hypot((b[0] - a[0]) * 111320 * math.cos(mlat), (b[1] - a[1]) * 110540)


def _point_of(geometry):
    """[lng, lat] for any geometry (registry rows are MultiPoint, one is a Polygon)."""
    if not geometry:
        return None
    if geometry.get("type") == "Point":
        c = geometry.get("coordinates")
        return list(c[:2]) if c else None
    try:
        c = shape(geometry).centroid
        return [c.x, c.y]
    except Exception:
        return None


_TAMA_PTS = None
_PERMIT_PTS = None


def load_tama38_points():
    """[(lnglat, addr_key, props)] — the תמ"א 38 file registry."""
    global _TAMA_PTS
    if _TAMA_PTS is None:
        _TAMA_PTS = []
        try:
            for f in json.load(open(TAMA38, encoding="utf-8")).get("features", []):
                pr = f.get("properties") or {}
                ll = _point_of(f.get("geometry"))
                if ll:
                    _TAMA_PTS.append((ll, _addr_key(pr.get("address")), pr))
        except Exception as e:
            print(f"  [link] could not load tama38.geojson ({e}) — no תמ\"א column.")
    return _TAMA_PTS


def load_permit_points():
    """[(lnglat, addr_key, permit)] — permits_master entries that carry coordinates."""
    global _PERMIT_PTS
    if _PERMIT_PTS is None:
        _PERMIT_PTS = []
        try:
            for pm in json.load(open(PERMITS_MASTER, encoding="utf-8")).get("permits", {}).values():
                if pm.get("lat") and pm.get("lng"):
                    _PERMIT_PTS.append(([pm["lng"], pm["lat"]], _addr_key(pm.get("address")), pm))
        except Exception as e:
            print(f"  [link] could not load permits_master.json ({e}) — no היתר column.")
    return _PERMIT_PTS


def _tik_year(obj):
    """Opening year from a permit/file tik ('2024/484' → 2024); None if unparseable."""
    m = re.match(r"\s*((?:19|20)\d{2})\b", str((obj or {}).get("tik") or ""))
    return int(m.group(1)) if m else None


def _match_points(ll, addr_key, items, radius=None, limit=None):
    """[(dist_m, exact_address, obj)] near the license — exact matches first.

    An exact street+number match wins outright (the permit's own geocode may sit
    tens of metres off the parcel centroid we resolved the license to); a merely
    nearby point is kept only inside `radius` and stays flagged as approximate.
    """
    radius = LINK_RADIUS_M if radius is None else radius
    hits = []
    for pt, ak, obj in items:
        d = _dist_m(ll, pt)
        exact = bool(addr_key) and ak == addr_key
        if exact and d <= 300:          # sanity cap: same name, wrong end of town
            hits.append((d, True, obj))
        elif d <= radius:
            hits.append((d, False, obj))
    if any(h[1] for h in hits):
        # An exact street+number match settles it — listing the neighbours' files
        # alongside it only dilutes the answer.
        hits = [h for h in hits if h[1]]
    else:
        # Proximity matches are a guess, so only recent files qualify: a 1950s
        # permit 20 m away tells you nothing about today's felling.
        hits = [h for h in hits if _tik_year(h[2]) is None or _tik_year(h[2]) >= MIN_LINK_YEAR]
    hits.sort(key=lambda h: (not h[1], h[0]))
    return hits if limit is None else hits[:limit]


def attach_planning_context(recs):
    """Stamp rec['_ctx'] = {minahak, sub, plans, tama38, permits} for the email."""
    try:
        import minahak_pip
    except Exception as e:
        minahak_pip = None
        print(f"  [link] minahak_pip unavailable ({e}) — no מינה\"ק column.")
    tama_pts, permit_pts = load_tama38_points(), load_permit_points()

    for rec in recs:
        ll = rec.get("lnglat")
        if not ll:
            continue
        ctx = {"minahak": "", "sub": "", "plans": [], "tama38": [], "permits": []}
        if minahak_pip:
            try:
                ctx["minahak"], ctx["sub"] = minahak_pip.derive({"type": "Point", "coordinates": ll})
            except Exception as e:
                print(f"  [link] minahak derive failed for {rec.get('permit_number')}: {e}")

        ak = _addr_key(rec.get("street") or rec.get("address"))
        t_hits = _match_points(ll, ak, tama_pts, limit=MAX_LINKED_PERMITS)
        # A תמ"א 38 file registered on this exact address IS the answer to "what is
        # going on here" — so we stop looking around: no nearby-plan fallback, no
        # proximity permits, and no radius tree probe (see attach_survey_warnings).
        ctx["tama38_exact"] = any(ex for _d, ex, _o in t_hits)
        ctx["plans"] = plans_at(ll, near_ok=not ctx["tama38_exact"])[:MAX_LINKED_PLANS]

        p_hits = _match_points(ll, ak, permit_pts)
        if ctx["tama38_exact"]:
            # Keep only permits that belong to THIS address: an exact street+number
            # match, or the licensing file of one of the matched תמ"א 38 files
            # (permits_master.taba carries the registry tik).
            own = {str(pr.get("tik") or "").strip() for _d, ex, pr in t_hits if ex}
            p_hits = [h for h in p_hits if h[1] or str(h[2].get("taba") or "").strip() in own]
        # A permit whose taba is a registry tik is that same תמ"א 38 file seen from
        # the licensing side — fold it into the תמ"א entry instead of listing it
        # twice, and keep the היתר column for everything that isn't תמ"א 38.
        by_taba = {}
        for d, ex, pm in p_hits:
            t = str(pm.get("taba") or "").strip()
            if t and t not in by_taba:
                by_taba[t] = pm
        for d, ex, pr in t_hits:
            tik = str(pr.get("tik") or "").strip()
            pm = by_taba.get(tik) or {}
            ctx["tama38"].append({
                "tik": tik, "status": (pr.get("status") or "").strip(),
                "address": (pr.get("address") or "").strip(),
                "dist": d, "exact": ex,
                "permit_tik": pm.get("tik") or "", "permit_status": pm.get("status") or "",
            })
        linked = {e["permit_tik"] for e in ctx["tama38"] if e["permit_tik"]}
        tama_exact = any(e["exact"] for e in ctx["tama38"])
        for d, ex, pm in p_hits:
            if pm.get("is_tama38") and (pm.get("tik") in linked):
                continue            # already shown in the תמ"א 38 column
            if pm.get("is_tama38") and tama_exact and not ex:
                continue            # the registry already matched this address exactly
            if pm.get("is_tama38"):
                ctx["tama38"].append({
                    "tik": "", "status": "", "address": pm.get("address") or "",
                    "dist": d, "exact": ex,
                    "permit_tik": pm.get("tik") or "", "permit_status": pm.get("status") or "",
                })
                continue
            if len(ctx["permits"]) >= MAX_LINKED_PERMITS:
                continue
            ctx["permits"].append({
                "tik": pm.get("tik") or "", "status": (pm.get("status") or "").strip(),
                "category": (pm.get("category_label") or "").strip(),
                "address": (pm.get("address") or "").strip(),
                "committee": bool(pm.get("on_committee_agenda")),
                "objections": bool(pm.get("open_for_objections")),
                "dist": d, "exact": ex,
            })
        ctx["tama38"] = ctx["tama38"][:MAX_LINKED_PERMITS]
        rec["_ctx"] = ctx
        probe_permit_file(rec)


# ------------------------------------ going INTO the file at the address --------
# When a license lands on an address that has a תמ"א 38 / building-permit file, the
# survey the felling is based on is usually IN THAT FILE — an אגף שפ"ע / agronomist
# attachment, or the גרמושקה itself. So we open the file rather than guess from the
# neighbourhood:
#   1. what we already extracted — tama38_tree_surveys.json (counts per file, from
#      the agronomist PDFs) and tama38_tree_overlap.json (survey of the plan the
#      building sits in). Keyed by both the registry tik (15710) and the licensing
#      file number (2026/0180.00).
#   2. otherwise the optical archive, live: proc 452 lists every document in the
#      file with a direct URL — we surface the tree-related ones (שפ"ע / בקרה
#      מרחבית / אגרונום / סקר עצים) and the גרמושקה/הרמוניקה, as links to click.
#   3. plus proc 450, the permit's conditions: a "שימור והעתקת העצים" / "תוכנית
#      נטיעות" condition means a survey provably exists even when no document name
#      says so (the heuristic behind fetch_tnaim_tree_flag.py).
# Live calls are best-effort — the muni gateway may be slow or block the runner.
FILE_SURVEYS = os.path.join(DATA_DIR, "tama38_tree_surveys.json")   # tik -> counts
FILE_OVERLAP = os.path.join(DATA_DIR, "tama38_tree_overlap.json")   # tik -> plan's survey
YK_API = "https://jerbasicserviceapi.jerusalem.muni.il/api/Db/ExecuteGetJSON"
YK_SYSID = "26400046"            # רישוי בניה (as opposed to 26400001 = תב"ע)
YK_DOCS_PROC = 242700452         # all documents of a file, each with a direct urlDoc
YK_TNAIM_PROC = 242700450        # the file's conditions (teurTnai)
YK_HEADERS = {
    "content-type": "application/json", "accept": "application/json, text/plain, */*",
    "referer": "https://ykpubdata.jerusalem.muni.il/",
    "origin": "https://ykpubdata.jerusalem.muni.il",
    "user-agent": UA["User-Agent"],   # the gateway answers 403 without one
}
TREE_DOC_KEYWORDS = ['סקר עצים', 'אגרונום', 'שפ"ע', 'שפע', 'בקרה מרחבית', 'גינון', 'עצים']
GRAM_DOC_KEYWORDS = ['הרמוניקה', 'גרמושקה']
MAX_FILE_DOCS = 3                # links per category — this is an email, not a listing


def _file_keys(tik):
    """Every spelling of a file number: '2024/484' → 2024/0484.00, 2024/0484, 2024/484."""
    tik = str(tik or "").strip()
    if not tik:
        return []
    keys = [tik]
    m = re.match(r"^(\d{4})/(\d+)(\.\d+)?$", tik)
    if m:
        padded = f"{m.group(1)}/{int(m.group(2)):04d}"
        keys += [padded + ".00", padded]
    return list(dict.fromkeys(keys))


def _archive_tik(tik):
    """The archive's own spelling (YYYY/NNNN.00), or '' for a non-licensing tik."""
    m = re.match(r"^(\d{4})/(\d+)(\.\d+)?$", str(tik or "").strip())
    return f"{m.group(1)}/{int(m.group(2)):04d}.00" if m else ""


def yk_api(proc, params, label, attempts=3):
    """POST one stored-proc on the ykpubdata gateway. None on failure (never raises)."""
    body = json.dumps({"ProcName": proc, "Cnn": "cnnGisYk", "Parameters": params}).encode("utf-8")
    for i in range(attempts):
        try:
            req = urllib.request.Request(YK_API, data=body, headers=YK_HEADERS, method="POST")
            with urllib.request.urlopen(req, timeout=25) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            if i + 1 == attempts:
                print(f"  [file] {label} failed ({type(e).__name__}) — skipping the file dive.")
                return None
            time.sleep(2 * (i + 1))
    return None


def lookup_file_survey(keys):
    """A survey we already extracted for one of these file keys, or None."""
    for path, source in ((FILE_SURVEYS, "file"), (FILE_OVERLAP, "overlap")):
        try:
            data = json.load(open(path, encoding="utf-8"))
        except Exception:
            continue
        for k in keys:
            hit = data.get(k)
            if hit:
                out = dict(hit)
                out["_source"] = source
                out["_key"] = k
                return out
    return None


def probe_permit_file(rec):
    """Stamp rec['_file'] with what the file at this address holds about trees."""
    ctx = rec.get("_ctx") or {}
    # Only files matched to the address itself — a neighbour's file explains nothing.
    tiks = []
    for e in ctx.get("tama38", []):
        if e.get("exact"):
            tiks += [e.get("tik"), e.get("permit_tik")]
    for e in ctx.get("permits", []):
        if e.get("exact"):
            tiks.append(e.get("tik"))
    tiks = [t for t in dict.fromkeys(tiks) if t]
    if not tiks:
        return

    keys = [k for t in tiks for k in _file_keys(t)]
    known = lookup_file_survey(keys)
    info = {"tiks": tiks, "survey": known, "docs": 0, "tree_docs": [], "gram_docs": [],
            "conditions": []}

    if not known:            # nothing extracted before — open the file in the archive
        for t in tiks:
            at = _archive_tik(t)
            if not at:
                continue     # a תמ"א registry id (15520) isn't an archive file number
                             # — only the licensing tik (2024/0484.00) is
            docs = yk_api(YK_DOCS_PROC, {"sysId": YK_SYSID, "tikNum": at}, f"docs {at}")
            if not docs:
                continue
            info["tik_used"] = at
            info["docs"] = len(docs)
            for d in docs:
                nm = (d.get("documentDescr") or "").strip()
                row = {"name": nm, "date": d.get("documentDate") or d.get("dateIn") or "",
                       "url": d.get("urlDoc") or "", "ext": d.get("docExtension") or ""}
                if any(k in nm for k in TREE_DOC_KEYWORDS):
                    info["tree_docs"].append(row)
                elif any(k in nm for k in GRAM_DOC_KEYWORDS):
                    info["gram_docs"].append(row)
            tn = yk_api(YK_TNAIM_PROC, {"TikNum": at}, f"conditions {at}") or []
            for c in tn:
                txt = (c.get("teurTnai") or "").strip()
                if txt and is_tree_condition(txt) and txt not in info["conditions"]:
                    info["conditions"].append(txt)
            break            # one file per license is enough

    info["tree_docs"] = info["tree_docs"][:MAX_FILE_DOCS]
    info["gram_docs"] = info["gram_docs"][:MAX_FILE_DOCS]
    if known or info["docs"]:
        rec["_file"] = info


def is_tree_condition(t):
    """Tree-SPECIFIC condition text (mirrors fetch_tnaim_tree_flag) — 'מחלקת השימור'
    is building conservation, not trees, so a bare שימור doesn't qualify."""
    t = t or ""
    if "עצים" in t and any(k in t for k in ("שימור", "העתק", "כרית", "נטיע", "פיצוי", "סקר")):
        return True
    return any(k in t for k in ("תוכנית נטיעות", "תכנית נטיעות", "פיצוי נופי",
                                "סקר עצים", "העתקת העצים", "כריתת עצים", "שתילה חלופ"))


def _esc(s):
    return str(s == None and "" or s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


DASH = '<span style="color:#bbb">—</span>'


def _sub(txt):
    return f'<div style="font-size:11px;color:#777">{txt}</div>' if txt else ''


def _approx(hit):
    # Distance badge — shown only for proximity matches, so an exact street+number
    # link is visibly stronger than "there is a permit 40 m away".
    return '' if hit.get("exact") else f'<span style="color:#999;font-size:11px"> ~{round(hit["dist"])} מ׳</span>'


def _file_over(fi, rec):
    """True when the license asks to cut more than the file's own survey marked."""
    s = fi.get("survey")
    return bool(s) and _cut_count(rec) > int(s.get("krita") or 0)


def _file_line(fi, rec):
    """One sentence on what the address's permit file holds about these trees."""
    tik = _esc(fi.get("tik_used") or fi["tiks"][0])
    s = fi.get("survey")
    if s:
        where = ("סקר של התכנית שהמבנה בתוכה" if s.get("_source") == "overlap"
                 else f'סקר בתיק (מקור: {_esc(s.get("source") or "")}'
                      + (f', {_esc(s.get("doc_date"))}' if s.get("doc_date") else '') + ')')
        cut = _cut_count(rec)
        verdict = (f'⚠️ מבוקשים לכריתה {cut} — מעל {s.get("krita", 0)} שסומנו בסקר'
                   if _file_over(fi, rec) else
                   f'מבוקשים לכריתה {cut} · בסקר סומנו {s.get("krita", 0)} לכריתה')
        return (f'🗂️ <b>תיק {_esc(s.get("_key") or tik)}</b> — {where}: '
                f'{s.get("total", 0)} עצים — שימור {s.get("shimur", 0)}, כריתה {s.get("krita", 0)}, '
                f'העתקה {s.get("haataka", 0)}. <b>{verdict}</b>')

    def links(rows_):
        return " · ".join(
            (f'<a href="{_esc(r["url"])}" style="color:#1a5c8a">{_esc(r["name"])}</a>'
             if r["url"] else _esc(r["name"]))
            + (f' ({_esc(r["date"])})' if r["date"] else '')
            for r in rows_)

    bits = [f'🗂️ <b>תיק ההיתר {tik}</b> — {fi["docs"]} מסמכים בארכיב']
    bits.append('🌳 מסמכי עצים/שפ"ע: ' + links(fi["tree_docs"]) if fi["tree_docs"]
                else 'אין מסמך עצים/שפ"ע בשמות המסמכים')
    if fi["conditions"]:
        bits.append('⚖️ תנאי עצים בהיתר: ' + _esc(" · ".join(fi["conditions"][:2]))
                    + ' <b>(⇒ קיים סקר)</b>')
    if fi["gram_docs"]:
        bits.append('📐 גרמושקה: ' + links(fi["gram_docs"]))
    return " | ".join(bits)


def _ctx_cells(rec):
    """(minahak, taba, tama38, permit) HTML cells for one license row."""
    ctx = rec.get("_ctx") or {}
    minahak = _esc(ctx.get("minahak")) + _sub(_esc(ctx.get("sub"))) if ctx.get("minahak") else DASH

    taba = ""
    for p, dist in ctx.get("plans", []):
        name = (p.get("plan_name_he") or p.get("plan_summary") or "").strip()
        if len(name) > 40:
            name = name[:40] + "…"
        near = '' if not dist else f'<span style="color:#999;font-size:11px"> ~{round(dist)} מ׳</span>'
        taba += ('<div style="margin-bottom:3px">'
                 f'<a href="{_esc(p.get("mavat_url") or "")}" style="color:#1a5c8a;text-decoration:none">{_esc(p.get("plan_name"))}</a>{near}'
                 f'{_sub(_esc(name) + (" · " + _esc(p.get("status_mavat")) if p.get("status_mavat") else ""))}</div>')

    tama = ""
    for h in ctx.get("tama38", []):
        head = ("תיק תמ\"א " + _esc(h["tik"])) if h["tik"] else ("היתר " + _esc(h["permit_tik"]))
        line2 = _esc(h["status"])
        if h["permit_tik"] and h["tik"]:
            line2 += (" · " if line2 else "") + "היתר " + _esc(h["permit_tik"])
        if h["permit_status"]:
            line2 += (" · " if line2 else "") + _esc(h["permit_status"])
        tama += f'<div style="margin-bottom:3px">{head}{_approx(h)}{_sub(line2)}</div>'

    permit = ""
    for h in ctx.get("permits", []):
        flags = ("  🏛️ בוועדה" if h["committee"] else "") + ("  ⚖️ פתוח להתנגדויות" if h["objections"] else "")
        permit += ('<div style="margin-bottom:3px">'
                   f'{_esc(h["tik"])}{_approx(h)}'
                   f'{_sub(" · ".join(x for x in (_esc(h["status"]), _esc(h["category"])) if x) + _esc(flags))}</div>')

    return minahak, (taba or DASH), (tama or DASH), (permit or DASH)


def notify_new_permits(new_recs):
    # Email the newly-appeared open-for-objection permits. Never raises — a mail
    # failure must not fail the data run. Skips quietly if creds/recipient absent.
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    if not new_recs:
        return
    if not EMAIL_PASSWORD:
        print(f"  [email] {len(new_recs)} new permit(s) but GMAIL_APP_PASSWORD is empty — skipping. "
              f"(set the repo secret so CI can send)")
        return
    print(f"  [email] notifying {EMAIL_RECIPIENT} about {len(new_recs)} permit(s) "
          f"(app-password length={len(EMAIL_PASSWORD)})")

    date_str = datetime.date.today().strftime("%d/%m/%Y")
    n = len(new_recs)
    pl = (lambda s: s if n > 1 else "")   # plural suffix, so subject and heading agree
    headline = (f'{n} אישור{pl("י")} כריתת עצים חדש{pl("ים")} פתוח{pl("ים")} לערר')
    subject = f'אורנים: {headline} ({date_str})'

    def cut_summary(rec):
        cut = (rec.get("trees") or {}).get("כריתה") or {}
        move = (rec.get("trees") or {}).get("העתקה") or {}
        bits = []
        if cut:
            bits.append("כריתה " + str(sum(int(v) for v in cut.values())))
        if move:
            bits.append("העתקה " + str(sum(int(v) for v in move.values())))
        return " · ".join(bits) or (rec.get("action") or "")

    rows = ""
    td = 'padding:6px;border:1px solid #ddd;vertical-align:top'
    for rec in new_recs:
        minahak, taba, tama, permit = _ctx_cells(rec)
        rows += (
            '<tr>'
            f'<td style="{td}">{_esc(rec.get("address"))}</td>'
            f'<td style="{td}">{minahak}</td>'
            f'<td style="{td}">{_esc(rec.get("gush"))}/{_esc(rec.get("helka"))}</td>'
            f'<td style="{td}">{_esc(cut_summary(rec))}</td>'
            f'<td style="{td}">{taba}</td>'
            f'<td style="{td}">{tama}</td>'
            f'<td style="{td}">{permit}</td>'
            f'<td style="{td}">{_esc(rec.get("requester"))}</td>'
            f'<td style="{td};font-weight:bold;color:#c0392b">{_esc(rec.get("deadline"))}</td>'
            f'<td style="{td}">{_esc(rec.get("permit_number"))}</td>'
            '</tr>'
        )
        # Gap warning row (spans the table) when the permit over-cuts vs the survey.
        w = rec.get("_survey_warn")
        if w:
            rows += (
                '<tr><td colspan="10" style="padding:6px 8px;border:1px solid #ddd;'
                'background:#fdecea;color:#c0392b;font-weight:bold">'
                f'⚠️ מבוקשים לכריתה {w["cut"]} עצים — מעל {w["krita"]} שהומלצו לכריתה בסקר '
                f'תכנית {_esc(w["plan"])} (נסקרו {w["total"]}: כריתה {w["krita"]}, '
                f'שימור {w["shimur"]}, העתקה {w["haataka"]})'
                '</td></tr>'
            )
        # What the file at this address holds about trees (see probe_permit_file).
        fi = rec.get("_file")
        # An "overlap" hit is the containing plan's survey seen from the building's
        # side — the same numbers the warning row above already reported.
        if fi and w and (fi.get("survey") or {}).get("_source") == "overlap":
            fi = None
        if fi:
            rows += (
                '<tr><td colspan="10" style="padding:6px 8px;border:1px solid #ddd;'
                f'background:{"#fdecea" if _file_over(fi, rec) else "#f4f7ee"};color:#4a5a3a">'
                f'{_file_line(fi, rec)}</td></tr>'
            )
        # No surveyed plan holds the point — what the second-round probe found on
        # the license's own parcels (see probe_site_trees).
        s = rec.get("_site_survey")
        if s:
            plans_txt = " · ".join(
                _esc(p["pl_number"]) + f' ({p["near"]} עצים בסביבה'
                + (f', {p["survey"]["total"]} בסקר התכנית' if p.get("survey") else '') + ')'
                for p in s["plans"])
            bg, fg = ('#fdecea', '#c0392b') if s["over"] else ('#eef5fb', '#2c6b96')
            verdict = (f'⚠️ מבוקשים לכריתה {s["cut"]} — מעל {s["krita"]} שסומנו לכריתה בסקר הסמוך'
                       if s["over"] else
                       f'מבוקשים לכריתה {s["cut"]} · בסקר סומנו {s["krita"]} לכריתה')
            rows += (
                f'<tr><td colspan="10" style="padding:6px 8px;border:1px solid #ddd;'
                f'background:{bg};color:{fg}">'
                f'🌲 <b>סקר עצים סמוך</b> (אין תכנית סקורה שמכילה את הכתובת; חיפוש ברדיוס '
                f'{s["radius"]} מ׳ סביב {s["spots"]} נקודות גוש/חלקה'
                f'{XPLAN_SOURCE_NOTE.get(s["source"], "")}): '
                f'{plans_txt} — נמצאו {s["total"]} עצים סקורים: כריתה {s["krita"]}, '
                f'שימור {s["shimur"]}, העתקה {s["haataka"]}. <b>{verdict}</b>'
                '</td></tr>'
            )
    warn_n = sum(1 for r in new_recs if r.get("_survey_warn") or (r.get("_site_survey") or {}).get("over"))
    if warn_n:
        subject += f' — ⚠️ {warn_n} מעל המלצת הסקר'
    html = (
        '<html><body dir="rtl" style="font-family:Arial,sans-serif">'
        f'<h2 style="color:#2e7d32">🌳 {headline} באזור אורנים</h2>'
        f'<p>תאריך: {_esc(date_str)}</p>'
        '<table style="border-collapse:collapse;width:100%;font-size:13px">'
        '<tr style="background:#f5f5f5">'
        + "".join(f'<th style="padding:8px;border:1px solid #ddd;white-space:nowrap">{h}</th>' for h in
                  ("כתובת", "מינה\"ק", "גוש/חלקה", "עצים", "תב\"ע", "תמ\"א 38",
                   "היתר", "מבקש", "מועד אחרון לערר", "רישיון")) +
        '</tr>'
        f'{rows}'
        '</table>'
        '<p style="color:#888;font-size:11px;margin-top:8px">'
        'מינה"ק/תב"ע נגזרים ממיקום הרישיון (גוש/חלקה או כתובת). תמ"א 38 והיתר מותאמים '
        'לפי רחוב+מספר; כשאין התאמת כתובת מוצג המרחק (~מ׳) והקישור הוא בקירוב בלבד. '
        'כשיש תיק בכתובת — נבדק מה יש בו (סקר שחולץ, מסמכי שפ"ע/עצים, תנאי היתר, גרמושקה); '
        'אחרת מחפשים סקר עצים סביב הגוש/חלקה.'
        '</p>'
        f'<p style="margin-top:16px">לצפייה ואימות: '
        f'<a href="{BASE}/FoPublic/FoLicence">פרסום רישיונות כריתה והעתקה — יעל"ה</a></p>'
        '<p style="color:#888;font-size:12px;margin-top:20px">Generated by fetch_tree_permits.py</p>'
        '</body></html>'
    )

    msg = MIMEMultipart("alternative")
    msg["From"], msg["To"], msg["Subject"] = EMAIL_SENDER, EMAIL_RECIPIENT, subject
    msg.attach(MIMEText(html, "html", "utf-8"))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
        print(f"  [email] sent to {EMAIL_RECIPIENT}: {subject}")
    except Exception as e:
        print(f"  [email] send failed (non-fatal): {e}")


def main():
    today = datetime.date.today()
    cutoff = (today - datetime.timedelta(days=GRACE_DAYS)).isoformat()

    # Baseline = the previously-committed set, read before we overwrite it. A
    # license key present now but not here is NEW → triggers an email below.
    prev = {}
    if os.path.isfile(OUT):
        try:
            prev = json.load(open(OUT, encoding="utf-8"))
        except Exception:
            prev = {}

    rows, total_open = fetch_all_open()

    district = load_district_polygon()
    idx = load_parcel_index()
    buildings = load_buildings_index()

    # group species-rows into licenses
    by_id = {}
    for r in rows:
        by_id.setdefault(r.get("requestId"), []).append(r)

    out = {}
    kept = skipped_deadline = skipped_noloc = skipped_outside = 0
    src_counts = {}

    for rid, grp in by_id.items():
        r0 = grp[0]
        deadline_iso = iso_date(r0.get("appealLastDate"))
        if not deadline_iso or deadline_iso < cutoff:
            skipped_deadline += 1
            continue

        rec = build_license(grp)
        lnglat, loc_source = resolve_location(rec.pop("_gush"), rec.pop("_helkot"), rec["street"], idx, buildings)
        if lnglat is None:
            skipped_noloc += 1
            continue
        if not district.contains(Point(lnglat[0], lnglat[1])):
            skipped_outside += 1
            continue
        rec["lnglat"] = lnglat
        rec["loc_source"] = loc_source

        key = rec["permit_number"] or str(rid)
        out[key] = rec
        kept += 1
        src_counts[loc_source] = src_counts.get(loc_source, 0) + 1

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))

    print("--- summary ---")
    print(f"today={today}  grace={GRACE_DAYS}d  cutoff>={cutoff}")
    print(f"distinct Jerusalem open licenses: {len(by_id)}   kept (open & in Oranim area): {kept}")
    print(f"location sources: {src_counts}")
    print(f"skipped: deadline-passed={skipped_deadline}  no-location(out of area)={skipped_noloc}  outside-district={skipped_outside}")
    print(f"wrote {OUT}  ({os.path.getsize(OUT)} bytes)")

    # ---- notify on NEW open-for-objection permits --------------------------
    # TREE_PERMITS_FORCE_EMAIL (set by the workflow's manual "test_email" box)
    # emails the CURRENT permits regardless of the diff — a live CI email test.
    force = str(os.environ.get("TREE_PERMITS_FORCE_EMAIL", "")).strip().lower() in ("1", "true", "yes")
    new_keys = [k for k in out if k not in prev]
    if force and out:
        print(f"TREE_PERMITS_FORCE_EMAIL set — sending all {len(out)} current permit(s) as a CI email test.")
        notify_list = list(out.values())
    elif new_keys:
        print(f"NEW permit(s) since last run: {new_keys}")
        notify_list = [out[k] for k in new_keys]
    else:
        print("no new permits since last run.")
        notify_list = []
    if notify_list:
        # Link each license to the planning at its address (מינה"ק / תב"ע / תמ"א 38 / היתר).
        # First: the survey probe below falls back to querying those plans by number.
        attach_planning_context(notify_list)
        # Flag permits that request to cut more than the plan's tree survey recommended.
        attach_survey_warnings(notify_list)
        notify_new_permits(notify_list)

    # ---- source-health guard (nationwide, NOT Jerusalem) -------------------
    # Jerusalem may legitimately have 0 open permits some weeks — that's a valid
    # empty layer, not a failure. What we must catch is יעל"ה migrating/freezing
    # like Meirim did. Probe nationwide: if the whole country shows 0 open, or the
    # newest approval anywhere is older than STALE_DAYS, the source has frozen —
    # fail loud. Exit AFTER writing so the json is still committed.
    nat_total, nat_newest = probe_source_health()
    if nat_total == 0:
        raise SystemExit(
            "STALE/BROKEN SOURCE: יעל\"ה shows 0 open-for-objection licenses "
            "nationwide — the public grid likely changed or went down. "
            "See project_tree_cutting_permits_layer memory.")
    if nat_newest:
        age = (today - datetime.date.fromisoformat(nat_newest)).days
        print(f"freshness (nationwide): {nat_total} open, newest approved {nat_newest} ({age}d ago)")
        if age > STALE_DAYS:
            raise SystemExit(
                f"STALE SOURCE: newest open יעל\"ה license approved {age}d ago (>{STALE_DAYS}). "
                f"The feed may have frozen — check yeela-trees.moag.gov.il.")


if __name__ == "__main__":
    main()
