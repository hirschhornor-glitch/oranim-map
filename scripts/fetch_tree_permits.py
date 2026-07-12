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
  4. Writes data/tree_permits.json — same schema the app already consumes
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
OUT = os.path.join(DATA_DIR, "tree_permits.json")


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


def load_parcel_index():
    if not os.path.isfile(PARCELS):
        print("note: parcel_centroids.json not found — skipping parcel-based geocoding")
        return None
    idx = json.load(open(PARCELS, encoding="utf-8"))
    print(f"parcel index: {len(idx.get('gush_helka', {}))} parcels, {len(idx.get('gush', {}))} gushim")
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


def _esc(s):
    return str(s == None and "" or s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


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
    subject = f'אורנים: {n} אישור{"י" if n > 1 else ""} כריתת עצים חדש{"ים" if n > 1 else ""} פתוח{"ים" if n > 1 else ""} לערר ({date_str})'

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
    for rec in new_recs:
        rows += (
            '<tr>'
            f'<td style="padding:6px;border:1px solid #ddd">{_esc(rec.get("address"))}</td>'
            f'<td style="padding:6px;border:1px solid #ddd">{_esc(rec.get("gush"))}/{_esc(rec.get("helka"))}</td>'
            f'<td style="padding:6px;border:1px solid #ddd">{_esc(cut_summary(rec))}</td>'
            f'<td style="padding:6px;border:1px solid #ddd">{_esc(rec.get("requester"))}</td>'
            f'<td style="padding:6px;border:1px solid #ddd;font-weight:bold;color:#c0392b">{_esc(rec.get("deadline"))}</td>'
            f'<td style="padding:6px;border:1px solid #ddd">{_esc(rec.get("permit_number"))}</td>'
            '</tr>'
        )
    html = (
        '<html><body dir="rtl" style="font-family:Arial,sans-serif">'
        f'<h2 style="color:#2e7d32">🌳 {n} אישור כריתת עצים חדש פתוח לערר באזור אורנים</h2>'
        f'<p>תאריך: {_esc(date_str)}</p>'
        '<table style="border-collapse:collapse;width:100%">'
        '<tr style="background:#f5f5f5">'
        '<th style="padding:8px;border:1px solid #ddd">כתובת</th>'
        '<th style="padding:8px;border:1px solid #ddd">גוש/חלקה</th>'
        '<th style="padding:8px;border:1px solid #ddd">עצים</th>'
        '<th style="padding:8px;border:1px solid #ddd">מבקש</th>'
        '<th style="padding:8px;border:1px solid #ddd">מועד אחרון לערר</th>'
        '<th style="padding:8px;border:1px solid #ddd">רישיון</th>'
        '</tr>'
        f'{rows}'
        '</table>'
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
        notify_new_permits(list(out.values()))
    elif new_keys:
        print(f"NEW permit(s) since last run: {new_keys}")
        notify_new_permits([out[k] for k in new_keys])
    else:
        print("no new permits since last run.")

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
