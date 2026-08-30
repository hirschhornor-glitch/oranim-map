"""
Step 1: Read target plans from Google Sheets.
Step 2: Use Playwright to navigate to each plan's actual Mavat web page.
Step 3: Scrape the status and date directly from the website UI.
Step 4: Update Google Sheets with the new data.
"""
import os
import asyncio
import json
import re
import ssl
import sys
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass
sys.path.insert(0, r"C:\ORANIM")
from git_sync import pull_before_read, commit_and_push_after_write, update_json_and_push
# Shared out-of-scope filter — same source detect_new_plans.py uses, so the whole
# "בדיקת סטטוס מבאת" pipeline ignores the same out-of-scope plans (Gilo + blocklist).
from scope_filter import is_blocklisted
# Table 5 + quantity-balance re-check on status change (like the land-use check)
from table5_status_check import scrape_plan as scrape_t5_balance, compute_changes as t5_compute_changes
import gspread
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
from google.oauth2.service_account import Credentials
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ─── Config ────────────────────────────────────────────────────
CREDS_FILE    = r"C:\ORANIM\oranim-490018-ceaf784afe61.json"
SHEET_ID      = "1_AcuuA1CNPh6jXc_lZKNghfpEF1aDPV8Zci8QPz2WVE"
BROWSER_DATA      = r"C:\ORANIM\.browser_data"
BROWSER_DATA_JLM  = r"C:\ORANIM\.browser_data_jlm"
PROGRESS_FILE     = r"C:\ORANIM\mavat_ui_sync_progress.json"
LOG_FILE          = r"C:\ORANIM\mavat_sync.log"
PLANS_GEOJSON     = r"C:\ORANIM\oranim-app\data\plans.geojson"
LANDUSE_GEOJSON   = r"C:\ORANIM\oranim-app\data\landuse_xplan.geojson"
SHAVAZ_GEOJSON    = r"C:\ORANIM\oranim-app\data\future_shavaz.geojson"
EASEMENTS_GEOJSON = r"C:\ORANIM\oranim-app\data\easements.geojson"
TREE_SURVEYS_JSON = r"C:\ORANIM\oranim-app\data\tree_surveys.json"
TREE_POINTS_JSON  = r"C:\ORANIM\oranim-app\data\tree_points_xplan.json"
CHANGELOG_JSON    = r"C:\ORANIM\oranim-app\data\plan_changelog.jsonl"  # written by the GS->geojson cron
LAST_CHANGELOG_EMAIL = r"C:\ORANIM\.last_changelog_email.txt"          # local marker, not committed
XPLAN_URL         = "https://ags.iplan.gov.il/arcgisiplan/rest/services/PlanningPublic/Xplan/MapServer/4/query"
XPLAN_BLUE_URL    = "https://ags.iplan.gov.il/arcgisiplan/rest/services/PlanningPublic/Xplan/MapServer/1/query"
XPLAN_EASEMENT_URL = "https://ags.iplan.gov.il/arcgisiplan/rest/services/PlanningPublic/Xplan/MapServer/3/query"
XPLAN_TREE_URL    = "https://ags.iplan.gov.il/arcgisiplan/rest/services/PlanningPublic/Xplan/MapServer/0/query"
YK_URL            = "https://ykpubdata.jerusalem.muni.il/#/TabaDetails?TikNum={taba}&SystemCode=26400001&Page=ProcessInfo"

PUBLIC_CODES    = {400, 410, 460, 1250, 1410, 1576, 1650, 1670}
HAFRASHAH_CODES = {1250, 1300, 1410, 1480, 1492, 1550, 1576, 1578, 1604, 1670}

# XPLAN MapServer/3 polygon entities — easements ("זיקות הנאה"). 6 codes collapse
# to 3 user-facing types matching fetch_easements_from_xplan.py.
EASEMENT_CODES = {
    20290: 'כללי', 20295: 'כללי',
    20300: 'רגלי', 20305: 'רגלי',
    20310: 'רכב',  20315: 'רכב',
}

# XPLAN MapServer/0 point entities — trees (1 point = 1 tree).
TREE_STATUS_BY_CODE = {22150: 'krita', 22160: 'haataka', 22170: 'shimur'}

# ── Email Settings ─────────────────────────────────────────────
SENDER_EMAIL = "hirschhorn.or@gmail.com"
SENDER_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
RECEIVER_EMAIL = "Or_hi@jerusalem.muni.il"

# Column indices (1-based) — updated to match current sheet layout
COL_AGAM_ID      = 1   # A  (agam_id)
COL_STATUS_MAVAT = 4   # D  (status_mavat)
COL_PLAN_NAME    = 6   # F  (plan_name)
COL_PLAN_NAME_HE = 7   # G  (plan_name_he)
COL_MAVAT_DATE   = 11  # K  (mavat_date)
COL_MINHAK       = 29  # AC (minahak)
COL_LAST_MODIFIED= 40  # AN (last_modified)
COL_HAS_OBJ_BTN  = 49  # AW (has_objection_btn)

TARGET_STATUSES = {
    "נקלטה מקובץ מבאת",
    "נקלטה",
    "בבדיקה תכנונית",
    "בבדיקת תנאי סף",
    "תכנית עומדת בתנאי סף",
    "במילוי תנאים להפקדה",
    "הפקדה להתנגדויות/השגות",
    "תום תקופת הפקדה",
    "דיון בהתנגדויות ותיקונים",
    "הכרעה בהתנגדויות / אישור",
    "תבע - טרום אישור",
    "בהליך אישור",
    "הכנת הודעה 77/78",
    'נפתח תיק תב"ע',
    "נפתח תיק למתכנן",
}

# Statuses the weekly sweep deliberately stops polling — the plan is "done".
# But "done" is not always final: 101-1215656 sat at נדחתה (13/05/2026) and was
# already back at "הפקדה להתנגדויות/השגות" on 19/08/2026 — we missed an entire
# objection window because nothing ever looked at it again. 309 plans were in this
# blind spot on 2026-08-30 (97 נדחתה + 212 נגנזה), so they get their own slower
# monthly sweep via --include-terminal instead of bloating the weekly run.
TERMINAL_STATUSES = {
    "נדחתה",
    "נגנזה",
    "נגנזה/נדחתה",
    "ביטול פרסום",
}

def log_msg(msg):
    print(msg, flush=True)
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")
    except:
        pass

def get_sheet():
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_progress(progress):
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)

def update_geojson(updates):
    """Update plans.geojson with new statuses (concurrency-safe via git_sync).

    Uses update_json_and_push (not the legacy write-then-commit path): its
    edit_fn is re-applied on a freshly-pulled file, so a remote push landing
    mid-run can't leave plans.geojson modified-but-uncommitted — which used to
    block the next pull --ff-only in every downstream/scheduled task."""
    if not updates:
        return
    try:
        # Build lookup by agam_id (pure computation — _apply below is re-appliable).
        update_map = {}
        for u in updates:
            aid = u['agam_id']
            if aid.endswith('.0'):
                aid = aid[:-2]
            update_map[aid] = u

        # Stamp once so last_modified stays identical across push-retry re-applies.
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        def _apply(geojson):
            updated = 0
            for feat in geojson['features']:
                agam = str(feat['properties'].get('agam_id', ''))
                if agam.endswith('.0'):
                    agam = agam[:-2]
                if agam in update_map:
                    u = update_map[agam]
                    feat['properties']['status_mavat'] = u['new_status']
                    if u.get('new_date'):
                        feat['properties']['mavat_date'] = u['new_date']
                    feat['properties']['last_modified'] = now_str
                    updated += 1
            if updated:
                log_msg(f"Updated {updated} features in plans.geojson")
            return updated

        # Commits (before downstream check_xplan_updates runs pull --ff-only) and
        # pushes; on a rejected push it re-applies on the new remote tip.
        update_json_and_push(
            'data/plans.geojson', _apply,
            f'data: mavat status sync ({len(update_map)} plans) (update_mavat_ui)'
        )
    except Exception as e:
        log_msg(f"Failed to update GeoJSON: {e}")


def update_future_shavaz_status(updates):
    """Sync Mavat_Status on future_shavaz.geojson features whose plan changed status.

    A feature's Mavat_Status is stamped once at creation (from XPLAN station_desc)
    and never refreshed, so it goes stale on later status changes — and the shavaz
    popup displays it directly (e.g. plan 101-1252998 kept showing a pre-deposit
    status after the plan moved to הפקדה להתנגדויות)."""
    if not updates:
        return
    try:
        def bare_taba(val):
            """Normalize '101-1252998' / '1252998' / '0095612' → '1252998' / '95612'."""
            s = str(val).strip()
            m = re.match(r'^\d+-(\d+)$', s)
            if m:
                return str(int(m.group(1)))
            return str(int(s)) if s.isdigit() else None

        status_by_taba = {}
        for u in updates:
            bt = bare_taba(u.get('plan_name', ''))
            if bt and u.get('new_status'):
                status_by_taba[bt] = u['new_status']
        if not status_by_taba:
            return

        def _apply(shavaz_gj):
            updated = 0
            for feat in shavaz_gj['features']:
                bt = bare_taba(feat['properties'].get('TABA', ''))
                if not bt or bt not in status_by_taba:
                    continue
                new_status = status_by_taba[bt]
                if feat['properties'].get('Mavat_Status') != new_status:
                    feat['properties']['Mavat_Status'] = new_status
                    updated += 1
            if updated:
                log_msg(f"Updated Mavat_Status on {updated} future_shavaz feature(s)")
            return updated

        # Re-appliable via update_json_and_push (see update_geojson) — no more
        # modified-but-uncommitted future_shavaz.geojson blocking later pulls.
        update_json_and_push(
            'data/future_shavaz.geojson', _apply,
            f'data: shavaz status sync ({len(status_by_taba)} plans) (update_mavat_ui)'
        )
    except Exception as e:
        log_msg(f"Failed to update future_shavaz statuses: {e}")


# ─── XPLAN geometry check ─────────────────────────────────────────
class _LegacySSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.set_ciphers('DEFAULT:@SECLEVEL=0')
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)

_XPLAN_SESSION = requests.Session()
_XPLAN_SESSION.mount('https://ags.iplan.gov.il', _LegacySSLAdapter())


def query_xplan_by_plan(pl_number):
    """Fetch all XPLAN features for a specific plan number."""
    params = {
        'where': f"pl_number='{pl_number}'",
        'outFields': '*',
        'returnGeometry': 'true',
        'f': 'geojson',
        'outSR': '4326',
    }
    try:
        resp = _XPLAN_SESSION.get(XPLAN_URL, params=params, timeout=60, verify=False)
        resp.raise_for_status()
        return resp.json().get('features', [])
    except Exception as e:
        log_msg(f"  XPLAN query failed for {pl_number}: {e}")
        return []


def query_xplan_blue_line(pl_number):
    """Fetch plan boundary (blue line) from XPLAN layer 1.
    This captures the full plan extent, not just the detailed landuse parcels."""
    params = {
        'where': f"pl_number='{pl_number}'",
        'outFields': 'pl_number',
        'returnGeometry': 'true',
        'f': 'geojson',
        'outSR': '4326',
    }
    try:
        resp = _XPLAN_SESSION.get(XPLAN_BLUE_URL, params=params, timeout=60, verify=False)
        resp.raise_for_status()
        return resp.json().get('features', [])
    except Exception as e:
        log_msg(f"  XPLAN blue line query failed for {pl_number}: {e}")
        return []


def query_xplan_easements_by_plan(pl_number):
    """Fetch easement polygons for a plan from XPLAN MapServer/3."""
    code_list = ','.join(str(c) for c in EASEMENT_CODES)
    params = {
        'where': f"pl_number='{pl_number}' AND mavat_code IN ({code_list})",
        'outFields': 'mavat_code,mavat_name,mp_id,pl_number,pl_name,shape_area,objectid',
        'returnGeometry': 'true',
        'f': 'geojson',
        'outSR': '4326',
    }
    try:
        resp = _XPLAN_SESSION.get(XPLAN_EASEMENT_URL, params=params, timeout=60, verify=False)
        resp.raise_for_status()
        return resp.json().get('features', [])
    except Exception as e:
        log_msg(f"  XPLAN easement query failed for {pl_number}: {e}")
        return []


def query_xplan_trees_by_plan(pl_number):
    """Fetch tree points for a plan from XPLAN MapServer/0."""
    code_list = ','.join(str(c) for c in TREE_STATUS_BY_CODE)
    params = {
        'where': f"pl_number='{pl_number}' AND mavat_code IN ({code_list})",
        'outFields': 'pl_number,mp_id,mavat_code,objectid',
        'returnGeometry': 'true',
        'f': 'geojson',
        'outSR': '4326',
    }
    try:
        resp = _XPLAN_SESSION.get(XPLAN_TREE_URL, params=params, timeout=60, verify=False)
        resp.raise_for_status()
        return resp.json().get('features', [])
    except Exception as e:
        log_msg(f"  XPLAN tree query failed for {pl_number}: {e}")
        return []


def geometry_differs(existing_geo, new_geo):
    """Check if two GeoJSON geometries are meaningfully different."""
    if existing_geo is None and new_geo is not None:
        return True
    if existing_geo is None and new_geo is None:
        return False
    return json.dumps(existing_geo, sort_keys=True) != json.dumps(new_geo, sort_keys=True)


def merge_plan_geometry(xplan_features, blue_line_features=None):
    """Union all parcel + blue-line geometries for one plan into a single
    clean Polygon/MultiPolygon (the "blue line" outline used for plan-level
    symbology in plans.geojson).

    Uses shapely.unary_union — naïve coord concat produces nested/overlapping
    rings that render as empty fill under Leaflet's evenodd rule (see
    feedback_multipolygon_evenodd memory).
    """
    all_coords = []
    for f in (xplan_features or []):
        geo = f.get('geometry')
        if not geo:
            continue
        if geo['type'] == 'MultiPolygon':
            all_coords.extend(geo['coordinates'])
        elif geo['type'] == 'Polygon':
            all_coords.append(geo['coordinates'])
    for f in (blue_line_features or []):
        geo = f.get('geometry')
        if not geo:
            continue
        if geo['type'] == 'MultiPolygon':
            all_coords.extend(geo['coordinates'])
        elif geo['type'] == 'Polygon':
            all_coords.append(geo['coordinates'])
    if not all_coords:
        return None

    from shapely.geometry import shape, mapping, MultiPolygon
    from shapely.geometry.polygon import orient
    from shapely.ops import unary_union
    from shapely.validation import make_valid

    polys = []
    for coords in all_coords:
        try:
            g = make_valid(shape({'type': 'Polygon', 'coordinates': coords}))
            if not g.is_empty:
                polys.append(g)
        except Exception:
            continue
    if not polys:
        return None

    merged = unary_union(polys)
    if merged.is_empty:
        return None

    if merged.geom_type == 'GeometryCollection':
        polys = [g for g in merged.geoms if g.geom_type in ('Polygon', 'MultiPolygon')]
        if not polys:
            return None
        merged = unary_union(polys)

    # RFC 7946 winding: outer rings CCW, holes CW. Leaflet relies on this.
    if merged.geom_type == 'Polygon':
        merged = orient(merged, sign=1.0)
    elif merged.geom_type == 'MultiPolygon':
        merged = MultiPolygon([orient(p, sign=1.0) for p in merged.geoms])

    return mapping(merged)


def check_xplan_updates(changed_plans):
    """Check XPLAN for geometry/landuse/shavaz changes for plans that changed status.
    Returns a report list of strings."""
    report = []

    # Load existing data — pull latest from remote first so we don't clobber
    if not pull_before_read():
        log_msg("ABORTING: could not pull latest geojson from remote.")
        return report
    try:
        with open(PLANS_GEOJSON, encoding='utf-8') as f:
            plans_gj = json.load(f)
        with open(LANDUSE_GEOJSON, encoding='utf-8') as f:
            landuse_gj = json.load(f)
        with open(SHAVAZ_GEOJSON, encoding='utf-8') as f:
            shavaz_gj = json.load(f)
        with open(EASEMENTS_GEOJSON, encoding='utf-8') as f:
            easements_gj = json.load(f)
        with open(TREE_SURVEYS_JSON, encoding='utf-8') as f:
            tree_surveys = json.load(f)
        with open(TREE_POINTS_JSON, encoding='utf-8') as f:
            tree_points = json.load(f)
    except Exception as e:
        log_msg(f"Failed to load GeoJSON files: {e}")
        return report

    plans_updated = 0          # user-visible: meaningful landuse delta per plan
    plans_geometry_updated = 0 # plans whose blue-line geometry was rebuilt
    landuse_added = 0
    landuse_removed = 0
    landuse_status_updated = 0 # plans whose parcels' station_desc was refreshed
    shavaz_added = 0
    easements_changed = 0      # plans whose easements changed
    easement_delta = 0         # net +/- easement count across changed plans
    trees_changed = 0          # plans whose tree counts/points changed
    tree_delta = 0             # net +/- tree count across changed plans

    # Per-plan keys of what actually changed, so the write block below can push
    # each file via update_json_and_push with a re-appliable, delta-only edit_fn
    # (splice just these plans onto the freshly-pulled file) instead of the legacy
    # whole-file overwrite that left files modified-but-uncommitted on a push race.
    plans_geom_changed = set()      # plan_name → blue-line geometry rebuilt
    landuse_changed = set()         # pl_number → landuse features replaced/status-refreshed
    shavaz_new_feats = []           # freshly-added shavaz/hafrashah features
    easements_changed_plans = set() # pl_number → easements replaced

    # Note: dedup by objectid was unreliable — XPLAN occasionally reassigns objectids,
    # causing the local file to accumulate duplicate parcels. We now do per-plan REPLACE
    # (delete all features for the plan then append fresh ones from XPLAN), which keeps
    # landuse_xplan.geojson a faithful mirror of XPLAN. Add/remove counts use
    # (mavat_code, shape_area) keys to count *real* parcel changes, not objectid churn.
    existing_shavaz_tabas = {(f['properties'].get('TABA', ''), f['properties'].get('MIGRASH', '')) for f in shavaz_gj['features']}

    for plan in changed_plans:
        pl_number = plan.get('plan_name', '')
        if not pl_number:
            continue

        log_msg(f"  Checking XPLAN for {pl_number}...")
        new_status = (plan.get('new_status') or '').strip()
        xplan_features = query_xplan_by_plan(pl_number)
        if not xplan_features:
            report.append(f"  {pl_number}: לא נמצא ב-XPLAN")
            # Still refresh station_desc on the plan's existing parcels — the
            # shavaz/hafrashah popups display it directly, and without a fresh
            # XPLAN response the parcels would keep the pre-change status.
            if new_status:
                stale = [ff for ff in landuse_gj['features']
                         if ff['properties'].get('pl_number') == pl_number
                         and ff['properties'].get('station_desc') != new_status]
                if stale:
                    for ff in stale:
                        ff['properties']['station_desc'] = new_status
                    landuse_status_updated += 1
                    landuse_changed.add(pl_number)
            continue

        # Stamp the authoritative Mavat status on the fresh parcels: XPLAN's own
        # station_desc can lag the Mavat page status that triggered this refresh,
        # and both the hafrashah/shavaz popups (landuse station_desc) and new
        # future_shavaz features (Mavat_Status, step 3 below) read it.
        if new_status:
            for xf in xplan_features:
                xf['properties']['station_desc'] = new_status

        # Fetch blue-line plan boundary (XPLAN layer 1) — included in the union
        # so plans without detailed landuse still get a meaningful outline.
        blue_line = query_xplan_blue_line(pl_number)

        # 1. Rebuild plans.geojson "blue-line" geometry as unary_union of all
        # parcels + blue line. Only write if the new geometry differs from
        # local. (Tiny precision-only shifts are tolerated to avoid churn.)
        merged_geo = merge_plan_geometry(xplan_features, blue_line)
        if merged_geo:
            for feat in plans_gj['features']:
                if feat['properties'].get('plan_name') == pl_number:
                    if geometry_differs(feat.get('geometry'), merged_geo):
                        feat['geometry'] = merged_geo
                        plans_geometry_updated += 1
                        plans_geom_changed.add(pl_number)
                    break

        # 2. Compute per-category delta for the report
        def _by_category(features):
            seen = set()
            out = {}
            for ff in features:
                pp = ff.get('properties', {})
                try:
                    ar = float(pp.get('shape_area') or 0)
                except (TypeError, ValueError):
                    ar = 0.0
                key = (pp.get('mavat_code'), round(ar, 2))
                if key in seen:
                    continue
                seen.add(key)
                nm = (pp.get('mavat_name') or '?').strip() or '?'
                out[nm] = out.get(nm, 0.0) + ar
            return out
        pre_features = [ff for ff in landuse_gj['features']
                        if ff.get('properties', {}).get('pl_number') == pl_number]
        # Count plans whose parcels carried a stale status, so the status-stamped
        # replace below gets saved even when no parcel was added/removed.
        if new_status and any(ff['properties'].get('station_desc') != new_status
                              for ff in pre_features):
            landuse_status_updated += 1
            landuse_changed.add(pl_number)
        pre_cats = _by_category(pre_features)
        post_cats = _by_category(xplan_features)
        deltas = []
        for cat in set(pre_cats) | set(post_cats):
            d = post_cats.get(cat, 0.0) - pre_cats.get(cat, 0.0)
            if abs(d) >= 50:
                deltas.append((cat, d))
        deltas.sort(key=lambda x: -abs(x[1]))

        def _fmt_area(sqm):
            sign = '+' if sqm > 0 else '-'
            v = abs(sqm)
            if v >= 10000:
                return f'{sign}{v/1000:,.1f} דונם'
            return f'{sign}{v:,.0f} מ"ר'

        if deltas:
            plans_updated += 1
            name_he = (plan.get('plan_name_he') or '').split('זיהוי וסיווג')[0].strip().rstrip(',').strip()
            minhak = plan.get('minhak') or ''
            name_short = name_he[:60] + ('…' if len(name_he) > 60 else '')
            extras = ' — '.join(x for x in (name_short, minhak) if x)
            suffix = f' ({extras})' if extras else ''
            breakdown = ', '.join(f'{_fmt_area(d)} {cat}' for cat, d in deltas)
            report.append(f"  🗺️ {pl_number}{suffix}: {breakdown}")

        # 2. Replace this plan's landuse features with the fresh XPLAN response.
        # Compute parcel-identity keys (mavat_code, shape_area_rounded) before/after
        # to count real adds/removes (objectid alone is unreliable — XPLAN churns it).
        def _parcel_key(props):
            try:
                ar = round(float(props.get('shape_area') or 0), 2)
            except (TypeError, ValueError):
                ar = 0.0
            return (props.get('mavat_code'), ar)
        pre_keys = {_parcel_key(ff['properties']) for ff in landuse_gj['features']
                    if ff['properties'].get('pl_number') == pl_number}
        post_keys = {_parcel_key(xf['properties']) for xf in xplan_features}
        # Drop the plan's existing features, then append the fresh ones
        landuse_gj['features'] = [ff for ff in landuse_gj['features']
                                  if ff['properties'].get('pl_number') != pl_number]
        landuse_gj['features'].extend(xplan_features)
        landuse_added += len(post_keys - pre_keys)
        landuse_removed += len(pre_keys - post_keys)
        landuse_changed.add(pl_number)

        # 3. Check for new public building / hafrashah parcels
        for xf in xplan_features:
            mc = xf['properties'].get('mavat_code')
            if mc is None:
                continue
            try:
                mc = int(float(mc))
            except (ValueError, TypeError):
                continue

            taba_key = (xf['properties'].get('pl_number', ''), str(xf['properties'].get('num', '')))
            if mc in PUBLIC_CODES and taba_key not in existing_shavaz_tabas:
                # Convert to shavaz format
                shavaz_feat = {
                    'type': 'Feature',
                    'geometry': xf.get('geometry'),
                    'properties': {
                        'TABA': xf['properties'].get('pl_number', ''),
                        'MIGRASH': str(xf['properties'].get('num', '')),
                        'plan_name_he': xf['properties'].get('pl_name', ''),
                        'MAVAT_NAME': xf['properties'].get('mavat_name', ''),
                        'Mavat_Status': xf['properties'].get('station_desc', ''),
                        'POLY_AREA': xf['properties'].get('shape_area'),
                    }
                }
                shavaz_gj['features'].append(shavaz_feat)
                shavaz_new_feats.append(shavaz_feat)
                existing_shavaz_tabas.add(taba_key)
                shavaz_added += 1

        # 4. Refresh easements for this plan (XPLAN MapServer/3 polygons).
        # Replace-on-change: dedup by objectid to detect real adds/removes.
        existing_eas = [ff for ff in easements_gj['features']
                        if ff['properties'].get('pl_number') == pl_number]
        new_eas_raw = query_xplan_easements_by_plan(pl_number)
        old_eas_keys = {ff['properties'].get('objectid') for ff in existing_eas}
        new_eas_keys = {ff['properties'].get('objectid') for ff in new_eas_raw}
        # Guard: an empty result almost always means the XPLAN entity layer is down
        # (iplan ~2026-06 migration emptied layers 0/3/4), not that the plan lost all
        # its easements. Never delete-to-empty — only apply the replace when we
        # actually got features back, so existing easements survive a source outage.
        if new_eas_raw and old_eas_keys != new_eas_keys:
            # Look up enrichment fields from plans.geojson (matches schema of
            # fetch_easements_from_xplan.py).
            plan_props = next((p['properties'] for p in plans_gj['features']
                               if p['properties'].get('plan_name') == pl_number), {})
            easements_gj['features'] = [ff for ff in easements_gj['features']
                                        if ff['properties'].get('pl_number') != pl_number]
            easements_changed_plans.add(pl_number)
            for ff in new_eas_raw:
                pp = ff.get('properties', {})
                easements_gj['features'].append({
                    'type': 'Feature',
                    'geometry': ff.get('geometry'),
                    'properties': {
                        'mavat_code':    pp.get('mavat_code'),
                        'mavat_name':    pp.get('mavat_name') or '',
                        'easement_type': EASEMENT_CODES.get(pp.get('mavat_code'), ''),
                        'pl_number':     pl_number,
                        'pl_name':       pp.get('pl_name') or '',
                        'plan_name_he':  plan_props.get('plan_name_he', ''),
                        'minahak':       plan_props.get('minahak', ''),
                        'mavat_url':     plan_props.get('mavat_url', ''),
                        'shape_area':    pp.get('shape_area'),
                    },
                })
            easements_changed += 1
            # Use list lengths (= rendered feature counts) so per-plan reports
            # and the summary delta agree. If existing file had stale duplicate
            # objectids from earlier runs, the rewrite naturally de-dupes.
            easement_delta += len(new_eas_raw) - len(existing_eas)
            by_type = {}
            for ff in new_eas_raw:
                t = EASEMENT_CODES.get(ff['properties'].get('mavat_code'), '?')
                by_type[t] = by_type.get(t, 0) + 1
            type_str = ', '.join(f'{k}:{v}' for k, v in sorted(by_type.items())) if by_type else 'אין'
            report.append(
                f"  🛤️ {pl_number}: זיקות הנאה {len(existing_eas)}→{len(new_eas_raw)} ({type_str})"
            )

        # 5. Refresh trees for this plan (XPLAN MapServer/0 points).
        # Derive taba (numeric) for the tree_surveys.json key.
        taba_key_tree = (plan.get('taba') or '').strip()
        if not taba_key_tree and '-' in pl_number:
            _, num = pl_number.split('-', 1)
            try:
                taba_key_tree = str(int(num))
            except ValueError:
                taba_key_tree = num
        new_tree_raw = query_xplan_trees_by_plan(pl_number)
        new_counts = {'shimur': 0, 'krita': 0, 'haataka': 0}
        new_points = []
        new_mp_ids = set()
        for ff in new_tree_raw:
            pp = ff.get('properties', {})
            code = pp.get('mavat_code')
            status = TREE_STATUS_BY_CODE.get(code)
            if not status:
                continue
            geom = ff.get('geometry') or {}
            if geom.get('type') != 'Point':
                continue
            coords = geom.get('coordinates') or []
            if len(coords) < 2:
                continue
            new_counts[status] += 1
            mp = pp.get('mp_id')
            if mp is not None:
                new_mp_ids.add(mp)
            new_points.append({
                'status':     status,
                'x':          coords[0],
                'y':          coords[1],
                'mavat_code': code,
                'objectid':   pp.get('objectid'),
                'mp_id':      mp,
            })
        old_tree_row = tree_surveys.get(taba_key_tree, {})
        old_counts = {k: old_tree_row.get(k, 0) for k in new_counts}
        old_total = old_tree_row.get('total', 0)
        new_total = sum(new_counts.values())
        # Guard: `new_tree_raw` empty almost always means the XPLAN point layer is
        # down (iplan ~2026-06 migration emptied layer 0), not that the plan lost
        # all its trees. Require features back before applying, so a source outage
        # can't drop existing xplan-sourced tree rows (the elif below).
        if taba_key_tree and new_tree_raw and (new_counts != old_counts or new_total != old_total):
            if new_total > 0:
                tree_surveys[taba_key_tree] = {
                    'total':      new_total,
                    'shimur':     new_counts['shimur'],
                    'krita':      new_counts['krita'],
                    'haataka':    new_counts['haataka'],
                    'lo_nidresh': 0,
                    'confidence': 'xplan',
                    'source':     'xplan',
                }
                tree_points.setdefault('plans', {})[taba_key_tree] = {
                    'pl_number': pl_number,
                    'mp_ids':    sorted(new_mp_ids),
                    'counts':    new_counts,
                    'points':    new_points,
                }
            elif old_tree_row.get('source') == 'xplan':
                # Was XPLAN-sourced and now empty — drop it. (Don't touch PDF-only rows.)
                tree_surveys.pop(taba_key_tree, None)
                tree_points.get('plans', {}).pop(taba_key_tree, None)
            trees_changed += 1
            tree_delta += new_total - old_total
            report.append(
                f"  🌳 {pl_number}: עצים {old_total}→{new_total} "
                f"(שימור:{new_counts['shimur']} עקירה:{new_counts['krita']} העתקה:{new_counts['haataka']})"
            )

    # Push each changed file via update_json_and_push: its edit_fn re-applies our
    # per-plan delta onto the freshly-pulled file, so (a) a remote push landing
    # mid-run can't leave the file modified-but-uncommitted (which blocked later
    # pulls), and (b) concurrent remote edits to OTHER plans are preserved.

    # plans.geojson — replace blue-line geometry in place, keyed by plan_name.
    if plans_geom_changed:
        new_geoms = {f['properties'].get('plan_name'): f.get('geometry')
                     for f in plans_gj['features']
                     if f['properties'].get('plan_name') in plans_geom_changed}
        def _apply_plans_geom(data):
            n = 0
            for feat in data['features']:
                pn = feat['properties'].get('plan_name')
                if pn in new_geoms and geometry_differs(feat.get('geometry'), new_geoms[pn]):
                    feat['geometry'] = new_geoms[pn]
                    n += 1
            return n
        update_json_and_push(
            'data/plans.geojson', _apply_plans_geom,
            f'data: rebuild blue-line geometry for {len(plans_geom_changed)} plans (update_mavat_ui)'
        )

    # landuse_xplan.geojson — replace each changed plan's parcels (drop + re-append
    # our version), keyed by pl_number.
    if landuse_added or landuse_removed or landuse_status_updated:
        ours_lu = {p: [] for p in landuse_changed}
        for ff in landuse_gj['features']:
            p = ff['properties'].get('pl_number')
            if p in ours_lu:
                ours_lu[p].append(ff)
        def _apply_landuse(data):
            data['features'] = [ff for ff in data['features']
                                if ff['properties'].get('pl_number') not in landuse_changed]
            for p in landuse_changed:
                data['features'].extend(ours_lu[p])
            return len(landuse_changed)
        msg_parts = []
        if landuse_added: msg_parts.append(f'+{landuse_added}')
        if landuse_removed: msg_parts.append(f'-{landuse_removed}')
        if landuse_status_updated: msg_parts.append(f'status×{landuse_status_updated}')
        update_json_and_push(
            'data/landuse_xplan.geojson', _apply_landuse,
            f'data: landuse refresh ({"/".join(msg_parts)} parcels) (update_mavat_ui)'
        )

    # future_shavaz.geojson — append the freshly-detected parcels, deduped by
    # (TABA, MIGRASH) against whatever is already on the remote tip.
    if shavaz_new_feats:
        def _apply_shavaz(data):
            seen = {(f['properties'].get('TABA', ''), f['properties'].get('MIGRASH', ''))
                    for f in data['features']}
            n = 0
            for feat in shavaz_new_feats:
                k = (feat['properties'].get('TABA', ''), feat['properties'].get('MIGRASH', ''))
                if k not in seen:
                    data['features'].append(feat)
                    seen.add(k)
                    n += 1
            return n
        update_json_and_push(
            'data/future_shavaz.geojson', _apply_shavaz,
            f'data: add {shavaz_added} shavaz/hafrashah parcels (update_mavat_ui)'
        )

    # easements.geojson — replace each changed plan's easements, keyed by pl_number.
    if easements_changed_plans:
        ours_eas = {p: [] for p in easements_changed_plans}
        for ff in easements_gj['features']:
            p = ff['properties'].get('pl_number')
            if p in ours_eas:
                ours_eas[p].append(ff)
        def _apply_easements(data):
            data['features'] = [ff for ff in data['features']
                                if ff['properties'].get('pl_number') not in easements_changed_plans]
            for p in easements_changed_plans:
                data['features'].extend(ours_eas[p])
            return len(easements_changed_plans)
        update_json_and_push(
            'data/easements.geojson', _apply_easements,
            f'data: easement refresh ({easements_changed} plans, net {easement_delta:+d}) (update_mavat_ui)'
        )

    # tree_surveys.json / tree_points_xplan.json stay on the legacy path: they are
    # written indent=2 (pretty) and update_json_and_push writes compact, which would
    # reformat the whole file. They are tree COUNTS, not geometry.
    if trees_changed:
        with open(TREE_SURVEYS_JSON, 'w', encoding='utf-8') as f:
            json.dump(tree_surveys, f, ensure_ascii=False, indent=2)
        with open(TREE_POINTS_JSON, 'w', encoding='utf-8') as f:
            json.dump(tree_points, f, ensure_ascii=False, indent=2)
        commit_and_push_after_write(
            'data/tree_surveys.json',
            f'data: tree refresh ({trees_changed} plans, net {tree_delta:+d} trees) (update_mavat_ui)'
        )
        commit_and_push_after_write(
            'data/tree_points_xplan.json',
            f'data: tree-points refresh ({trees_changed} plans) (update_mavat_ui)'
        )

    if (plans_updated or plans_geometry_updated or landuse_added or landuse_removed
            or landuse_status_updated or shavaz_added or easements_changed or trees_changed):
        landuse_parts = []
        if landuse_added: landuse_parts.append(f'+{landuse_added}')
        if landuse_removed: landuse_parts.append(f'-{landuse_removed}')
        landuse_str = '/'.join(landuse_parts) if landuse_parts else '0'
        summary = (f"XPLAN: {plans_updated} landuse-delta, "
                   f"{plans_geometry_updated} blue-line rebuilt, "
                   f"{landuse_str} landuse parcels, {shavaz_added} shavaz, "
                   f"{easements_changed} easements ({easement_delta:+d}), "
                   f"{trees_changed} trees ({tree_delta:+d})")
        report.insert(0, summary)
        log_msg(summary)
    else:
        report.append("✅ XPLAN: אין שינויי geometry/landuse/shavaz/זיקות/עצים")

    return report


def update_geojson_objections(results):
    """Update plans.geojson with objection end dates.

    Uses git_sync.update_json_and_push: the edit is re-applied on the fresh
    remote tip if a concurrent push lands meanwhile, so the single-line
    geojson never needs a textual merge.
    """
    if not results:
        return

    def _apply(geojson):
        updated = 0
        for feat in geojson['features']:
            taba = str(feat['properties'].get('taba', ''))
            if taba in results and results[taba].get('objection_end'):
                if feat['properties'].get('objection_end_date') != results[taba]['objection_end']:
                    feat['properties']['objection_end_date'] = results[taba]['objection_end']
                    updated += 1
        log_msg(f"Updated {updated} features in plans.geojson with objection dates")
        return updated

    try:
        update_json_and_push(
            'data/plans.geojson', _apply,
            f'data: objection_end_date updates ({len(results)} plans) (update_mavat_ui)'
        )
    except Exception as e:
        log_msg(f"Failed to update GeoJSON objection dates: {e}")


def update_geojson_objection_btn(progress, rows_to_check):
    """Update plans.geojson with has_objection_btn from progress."""
    try:
        with open(PLANS_GEOJSON, encoding='utf-8') as f:
            geojson = json.load(f)
        btn_map = {}
        for row_info in rows_to_check:
            aid = row_info['agam_id']
            if aid in progress and not progress[aid].get('error'):
                btn_map[aid] = bool(progress[aid].get('has_objection_btn'))
        updated = 0
        for feat in geojson['features']:
            agam = str(feat['properties'].get('agam_id', ''))
            if agam.endswith('.0'):
                agam = agam[:-2]
            if agam in btn_map:
                feat['properties']['has_objection_btn'] = btn_map[agam]
                updated += 1
        with open(PLANS_GEOJSON, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, ensure_ascii=False)
        log_msg(f"Updated {updated} features in plans.geojson with has_objection_btn")
        if updated:
            commit_and_push_after_write(
                'data/plans.geojson',
                f'data: has_objection_btn updates ({updated} plans) (update_mavat_ui)'
            )
    except Exception as e:
        log_msg(f"Failed to update GeoJSON has_objection_btn: {e}")


# YK Jerusalem internal API (faster + more reliable than Playwright scraping)
YK_API_URL = 'https://jerbasicserviceapi.jerusalem.muni.il/api/Db/ExecuteGetJSON'
YK_API_PROC_PROCESS = 242700451  # ProcName for "תהליך תיק" (deposit process steps)
YK_API_HEADERS = {
    'Content-Type': 'application/json',
    'Origin': 'https://ykpubdata.jerusalem.muni.il',
    'Referer': 'https://ykpubdata.jerusalem.muni.il/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
}


def _parse_dmy(s):
    if not s: return None
    try:
        pts = str(s).split('/')
        return datetime(int(pts[2]), int(pts[1]), int(pts[0]))
    except Exception:
        return None


def _yk_compute_objection_end(taba, status_date=None):
    """Call YK API and compute the objection-end date.
    Returns (date_str dd/mm/yyyy, source_label) or (None, reason).

    When status_date is given (the Mavat date the plan entered
    'הפקדה להתנגדויות'), YK pub_dates older than ~status_date are treated as
    stale (previous deposit cycles) and ignored. Fallback: status_date + 60.
    Plan 1338425 (אנטיגונוס 10) on 2026-06-21: status=17/06 but YK only had
    pub=13/03 → naïve pub+60 gave 12/05 (BEFORE status date, impossible).
    """
    body = {"ProcName": YK_API_PROC_PROCESS, "Cnn": "cnnGisYk",
            "Parameters": {"SystemID": "26400001", "TikNum": str(taba)}}
    try:
        r = requests.post(YK_API_URL, json=body, headers=YK_API_HEADERS, timeout=20)
        if r.status_code != 200:
            return None, f'HTTP {r.status_code}'
        data = r.json()
    except Exception as e:
        return None, f'ERR {str(e)[:50]}'

    def _status_fallback(reason):
        if status_date:
            return (status_date + timedelta(days=60)).strftime('%d/%m/%Y'), f'status+60 ({reason})'
        return None, reason

    deposit_steps = [s for s in data if s.get('processText') == 'תהליך הפקדת תכנית']
    if not deposit_steps:
        return _status_fallback('no deposit process in YK')

    # Explicit "תום תקופת הפקדה" wins — but only if not BEFORE status_date.
    pub_dates = []
    for step in deposit_steps:
        step_text = step.get('stepCodeText') or ''
        exec_d = _parse_dmy(step.get('execDateStr'))
        if 'תום תקופת' in step_text:
            d = exec_d or _parse_dmy(step.get('planDateStr'))
            if not d: continue
            if status_date and d < status_date:
                # Stale explicit end-date from a previous cycle. Skip.
                continue
            return d.strftime('%d/%m/%Y'), 'explicit'
        if 'פרסום ההפקדה' in step_text:
            # The 60-day objection clock starts only once the publication is ACTUALLY
            # executed. A planned/future ילקוט date (planDateStr with no execDateStr) is a
            # placeholder and must NOT extend the deadline — plan 1185578: ילקוט planned
            # 05/10 while deposited 05/08 → planDateStr+60 wrongly gave 04/12 (~4 months).
            # With exec-only, no executed ילקוט → fall back to status_date(deposit)+60.
            if exec_d:
                pub_dates.append(exec_d)

    # Filter pub_dates to current cycle (>= status_date - 7d grace).
    if status_date:
        floor = status_date - timedelta(days=7)
        relevant = [d for d in pub_dates if d >= floor]
        if relevant:
            latest = max(relevant)
            return (latest + timedelta(days=60)).strftime('%d/%m/%Y'), f'pub({latest.strftime("%d/%m/%Y")})+60'
        return _status_fallback('YK pubs stale')

    # No status_date provided — original behavior.
    if pub_dates:
        latest = max(pub_dates)
        return (latest + timedelta(days=60)).strftime('%d/%m/%Y'), f'pub({latest.strftime("%d/%m/%Y")})+60'
    return None, 'no pub dates'


def fetch_objection_dates(all_data, sheet, progress):
    """Fetch objection_end_date for plans in הפקדה status via YK Jerusalem API.

    Uses jerbasicserviceapi (the same API the YK web UI calls). Much faster
    and more reliable than Playwright. Computes end-date either from the
    explicit "תום תקופת הפקדה" step or as (latest publication + 60 days).
    """
    headers = all_data[0]
    h = {hdr.strip().lower(): i for i, hdr in enumerate(headers)}
    obj_col_idx = h.get('objection_end_date')
    if obj_col_idx is None:
        log_msg("No objection_end_date column in Sheets — skipping.")
        return []

    # Collect candidate plans (all in הפקדה להתנגדויות status, dedupe by taba)
    plan_name_he_idx = h.get('plan_name_he')
    minhak_idx = h.get('minahak')
    mavat_date_idx = h.get('mavat_date')

    def _cell(row, idx):
        return row[idx].strip() if (idx is not None and idx < len(row)) else ''

    plans = []
    seen_taba = set()
    for row_num, row in enumerate(all_data[1:], start=2):
        status = row[h['status_mavat']].strip() if h['status_mavat'] < len(row) else ''
        if 'הפקדה להתנגדויות' not in status:
            continue
        taba = row[h['taba']].strip() if h['taba'] < len(row) else ''
        if not taba or taba in seen_taba:
            continue
        seen_taba.add(taba)
        plan_name = row[h.get('plan_name', 0)].strip() if h.get('plan_name') is not None else ''
        current = row[obj_col_idx].strip() if obj_col_idx < len(row) else ''
        plans.append({
            'row': row_num, 'taba': taba, 'plan_name': plan_name,
            'plan_name_he': _cell(row, plan_name_he_idx),
            'minhak': _cell(row, minhak_idx),
            'status_date': _parse_dmy(_cell(row, mavat_date_idx)),
            'current': current,
        })

    log_msg(f"\n=== Fetching Objection Dates (YK API) ===")
    log_msg(f"Candidate plans in הפקדה status: {len(plans)}")
    if not plans:
        return []

    results = {}
    for i, plan in enumerate(plans):
        end_date, source = _yk_compute_objection_end(plan['taba'], status_date=plan.get('status_date'))
        if end_date:
            results[plan['taba']] = {
                'row': plan['row'], 'objection_end': end_date,
                'plan_name': plan['plan_name'],
                'plan_name_he': plan['plan_name_he'],
                'minhak': plan['minhak'],
                'old_date': plan['current'],
                'source': source,
                'changed': end_date != plan['current'],
            }
            tag = '✓ NEW' if end_date != plan['current'] else 'same'
            log_msg(f"  [{i+1}/{len(plans)}] {plan['plan_name']}: {end_date} ({source}) [{tag}]")
        else:
            log_msg(f"  [{i+1}/{len(plans)}] {plan['plan_name']}: – ({source})")
        time.sleep(0.3)  # be polite

    # Only push CHANGED dates to Sheets/GeoJSON
    changed = {t: d for t, d in results.items() if d['changed']}
    if not changed:
        log_msg("No objection-date changes to apply.")
        return []

    log_msg(f"Updating {len(changed)} objection dates in Google Sheets...")
    obj_col_a1 = obj_col_idx + 1
    lm_col_a1 = h.get('last_modified', 0) + 1
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    batch = []
    for row_num, row in enumerate(all_data[1:], start=2):
        t = row[h['taba']].strip() if h['taba'] < len(row) else ''
        if t in changed:
            batch.append({'range': gspread.utils.rowcol_to_a1(row_num, obj_col_a1),
                          'values': [[changed[t]['objection_end']]]})
            if 'last_modified' in h:
                batch.append({'range': gspread.utils.rowcol_to_a1(row_num, lm_col_a1),
                              'values': [[now_str]]})
    if batch:
        sheet.spreadsheet.values_batch_update({'valueInputOption': 'RAW', 'data': batch})
        log_msg(f"Updated {len(batch)} cells in Google Sheets")

    update_geojson_objections(changed)

    return [{'taba': t, 'objection_end': d['objection_end'],
             'plan_name': d.get('plan_name', ''),
             'plan_name_he': d.get('plan_name_he', ''),
             'minhak': d.get('minhak', ''),
             'old_date': d.get('old_date', ''),
             'source': d.get('source', '')}
            for t, d in changed.items()]


_PLAN_RE = re.compile(r'(101-\d+|תתל[\s/]\S+)')


def _parse_xplan_report(xplan_report, plan_meta):
    """Convert the line-based xplan_report into structured table rows.

    Returns (summary_lines, rows). Each row is a dict with keys:
      plan_name, plan_name_he, minhak, type_emoji, type_label, details (str).

    The 📊 (Table 5) header carries no detail of its own — its subsequent
    indented lines are aggregated into one cell joined by <br>.
    """
    import re as _re
    summary, rows = [], []
    t5_acc = None

    def _flush():
        nonlocal t5_acc
        if t5_acc:
            t5_acc['details'] = '<br>'.join(t5_acc.pop('details_lines')) or '-'
            rows.append(t5_acc)
            t5_acc = None

    type_map = [
        ('🗺️', 'ייעודי קרקע'),
        ('🛤️', 'זיקות הנאה'),
        ('🌳', 'עצים'),
        ('📊', 'טבלה 5 + נכנס'),
    ]

    for line in xplan_report or []:
        stripped = line.strip()
        if not stripped:
            _flush()
            continue
        # Top-level summary or no-changes marker (no per-plan info)
        if stripped.startswith('XPLAN:') or stripped.startswith('✅'):
            _flush()
            summary.append(stripped)
            continue
        # Indented Table 5 detail line ("      units_in: 0 → 28")
        if line.startswith('      ') and ':' in stripped and t5_acc is not None:
            t5_acc['details_lines'].append(stripped)
            continue
        # Plan-level entries — find emoji, plan_name, and details
        emoji_found = None
        for emoji, label in type_map:
            if emoji in stripped:
                emoji_found = (emoji, label)
                break
        m_plan = _PLAN_RE.search(stripped)
        pn = m_plan.group(1) if m_plan else ''
        meta = plan_meta.get(pn, {})
        if emoji_found:
            emoji, label = emoji_found
            m_details = _re.search(
                rf'{_re.escape(emoji)}\s*\S+(?:\s*\([^)]*\))?:\s*(.*)',
                stripped,
            )
            details = m_details.group(1).strip() if m_details else stripped
            if emoji == '📊':
                _flush()
                t5_acc = {
                    'plan_name': pn,
                    'plan_name_he': meta.get('plan_name_he', ''),
                    'minhak': meta.get('minhak', ''),
                    'status': meta.get('status', ''),
                    'type_emoji': emoji,
                    'type_label': label,
                    'details_lines': [],
                }
            else:
                _flush()
                rows.append({
                    'plan_name': pn,
                    'plan_name_he': meta.get('plan_name_he', ''),
                    'minhak': meta.get('minhak', ''),
                    'status': meta.get('status', ''),
                    'type_emoji': emoji,
                    'type_label': label,
                    'details': details,
                })
            continue
        # "לא נמצא ב-XPLAN" or other plan-level note without an emoji
        if pn:
            _flush()
            note = stripped.split(':', 1)[1].strip() if ':' in stripped else stripped
            rows.append({
                'plan_name': pn,
                'plan_name_he': meta.get('plan_name_he', ''),
                'minhak': meta.get('minhak', ''),
                'status': meta.get('status', ''),
                'type_emoji': '❓',
                'type_label': 'הערה',
                'details': note,
            })
    _flush()
    return summary, rows


def _load_pending_new_plans():
    """Read new_plans_report.json if present. Returns (list_of_plans, path).
    detect_new_plans.py writes this at the start of run_mavat_sync.bat; we
    consume it here and rename to `.consumed` so it isn't re-included."""
    path = r"C:\ORANIM\new_plans_report.json"
    try:
        if not os.path.exists(path):
            return [], path
        with open(path, encoding='utf-8') as f:
            report = json.load(f)
        if report.get('new_plans_count', 0) == 0:
            return [], path
        plans = list((report.get('plans') or {}).values())
        return plans, path
    except Exception as e:
        log_msg(f"Failed to read new_plans_report.json: {e}")
        return [], path


# Sensitive fields worth surfacing from the changelog (status/date already have
# their own email sections, so they are excluded here to avoid duplication).
SENSITIVE_FIELDS = {
    "hafrash_prg":     'הפרשה (תיאור)',
    "hafrash_sqm":     'הפרשה (מ"ר)',
    "shavatz_out_sqm": 'שב"צ יוצא',
    "shavatz_in_sqm":  'שב"צ נכנס',
    "shatzap_out":     'שצ"פ יוצא',
    "units_total":     'יח"ד יוצא',
    "units_in":        'יח"ד נכנס',
    "commerce_out":    'מסחר יוצא',
    "employment":      'תעסוקה',
    "floors_max":      'קומות',
    "High":            'גובה',
    "resident_shared": 'חדר דיירים',
}
PUBLIC_BUILDING_TERMS = ('גן ילדים', 'כיתות גן', 'מעון', 'בי"ס', 'בית ספר',
                         'בית-ספר', 'מבנה ציבור', 'מבני ציבור', 'ציבור', 'שב"צ',
                         'קהילה', 'מתנ"ס', 'בית כנסת', 'מרפאה', 'רווחה')


def _is_new_public_allocation(field, old, new):
    """Flag a NEW built public allocation (the exact class that slipped in for
    101-1354356): a hafrash that appeared where there was none, or a hafrash_prg
    that gained a public-building term. Per the rule, these must be corroborated
    against Table 5 / הוראות before they are trusted."""
    if field not in ("hafrash_prg", "hafrash_sqm"):
        return False
    o = "" if old is None else str(old).strip()
    n = "" if new is None else str(new).strip()
    if not n:
        return False
    if field == "hafrash_sqm":
        return not o  # empty -> a value
    # hafrash_prg: brand-new text, or gained a public term it lacked before
    if not o:
        return any(t in n for t in PUBLIC_BUILDING_TERMS)
    return any(t in n and t not in o for t in PUBLIC_BUILDING_TERMS)


def build_changelog_digest():
    """Read plan_changelog.jsonl and return (html, newest_ts, n_events) for
    sensitive field changes since the last email (tracked by a local marker
    file). Empty html when nothing new / file missing. Non-fatal on any error."""
    try:
        if not os.path.exists(CHANGELOG_JSON):
            return "", None, 0
        marker = ""
        try:
            with open(LAST_CHANGELOG_EMAIL, encoding="utf-8") as f:
                marker = f.read().strip()
        except OSError:
            marker = ""
        if not marker:
            # first run: only look back 7 days so we don't dump the whole history
            marker = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")

        events, newest = [], marker
        with open(CHANGELOG_JSON, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if e.get("field") not in SENSITIVE_FIELDS:
                    continue
                ts = e.get("ts", "")
                if ts <= marker:  # "YYYY-MM-DD HH:MM:SS" sorts chronologically
                    continue
                events.append(e)
                if ts > newest:
                    newest = ts
        if not events:
            return "", newest, 0

        html = '<h2>🏛️ שינויי שדות רגישים (הפרשה / שב"צ / יח"ד / קומות)</h2>'
        html += (f'<p>מאז המייל האחרון נרשמו {len(events)} שינויי שדה ביומן '
                 f'השינויים (plan_changelog):</p>')
        html += "<table border='1' cellpadding='8' style='border-collapse: collapse;'>"
        html += ("<tr style='background-color: #fff3e0;'>"
                 "<th>תאריך</th><th>מספר תכנית</th><th>שדה</th>"
                 "<th>ישן → חדש</th><th>סטטוס</th><th>דגל</th></tr>")
        for e in events:
            flag = _is_new_public_allocation(e.get("field"), e.get("old"), e.get("new"))
            flag_cell = ('🚩 <b style="color:#c62828">הקצאת ציבור חדשה — '
                         'לאמת מול טבלה 5/הוראות</b>') if flag else ''
            row_style = " style='background-color:#ffebee;'" if flag else ""
            label = SENSITIVE_FIELDS.get(e.get("field"), e.get("field"))
            old = e.get("old")
            old_s = '∅' if old in (None, "", []) else old
            html += f"<tr{row_style}>"
            html += f"<td>{e.get('ts','')}</td>"
            html += f"<td>{e.get('plan_name','')}</td>"
            html += f"<td dir='rtl'>{label}</td>"
            html += f"<td dir='rtl'>{old_s} → <b>{e.get('new')}</b></td>"
            html += f"<td dir='rtl'>{e.get('status','')}</td>"
            html += f"<td dir='rtl'>{flag_cell}</td>"
            html += "</tr>"
        html += "</table><br>"
        return html, newest, len(events)
    except Exception as ex:
        log_msg(f"changelog digest failed (non-fatal): {ex}")
        return "", None, 0


def _load_pending_enrich_email():
    """Enrichment summary dropped by enrich_changed_plans.py so its per-plan
    staging/type/floors/developer results ride in THIS script's single email
    instead of a second one. Returns (rows, path); consumed after a good send."""
    path = r"C:\ORANIM\_pending_enrich_email.json"
    try:
        with open(path, encoding="utf-8") as f:
            rows = json.load(f)
        return (rows if isinstance(rows, list) else []), path
    except Exception:
        return [], path


def send_email_notification(updates, objection_results=None, xplan_report=None):
    if not SENDER_EMAIL or not SENDER_PASSWORD or not RECEIVER_EMAIL:
        log_msg("Email settings not configured. Skipping email notification.")
        return
    if '--no-email' in sys.argv:
        log_msg("--no-email set; skipping email notification.")
        return

    has_updates = bool(updates)
    # Filter objection_results: only future objection_end dates are worth emailing
    # (past dates are still recorded to GS/geojson, just not surfaced in the email).
    today = datetime.now().date()
    def _is_future(date_str):
        try:
            d, m, y = (date_str or '').split('/')
            return datetime(int(y), int(m), int(d)).date() >= today
        except Exception:
            return False
    future_objections = [o for o in (objection_results or []) if _is_future(o.get('objection_end', ''))]
    has_objections = bool(future_objections)
    has_xplan = bool(xplan_report)

    # Consolidated new-plans section — read detect_new_plans's report if present.
    new_plans, new_plans_report_path = _load_pending_new_plans()
    has_new_plans = bool(new_plans)

    # Sensitive field-change digest from the changelog (hafrash/shavaz/units/floors)
    digest_html, digest_newest_ts, digest_n = build_changelog_digest()
    has_digest = bool(digest_html)

    # Enrichment summary handed over by enrich_changed_plans.py (one-email policy).
    enrich_rows, enrich_payload_path = _load_pending_enrich_email()
    has_enrich = bool(enrich_rows)

    if not has_updates and not has_objections and not has_xplan and not has_new_plans \
            and not has_digest and not has_enrich:
        return

    log_msg("Sending email notification...")

    # Build subject
    parts = []
    if has_new_plans:
        parts.append(f"{len(new_plans)} תכניות חדשות")
    if has_updates:
        parts.append(f"{len(updates)} סטטוסים עודכנו")
    if has_objections:
        parts.append(f"{len(future_objections)} תאריכי התנגדויות")
    if has_digest:
        parts.append(f"{digest_n} שינויי שדה")
    if has_enrich:
        parts.append(f"{len(enrich_rows)} העשרות")
    subject = "עידכון ממבאת: " + " | ".join(parts)

    html = "<html dir='rtl'><body style='font-family: Arial, sans-serif;'>"

    # New plans section — placed first so it stands out.
    if has_new_plans:
        html += "<h2>🆕 תכניות חדשות שזוהו</h2>"
        html += f"<p>נמצאו {len(new_plans)} תכניות חדשות שנוספו ל-GS ו-plans.geojson:</p>"
        html += "<table border='1' cellpadding='8' style='border-collapse: collapse;'>"
        html += ("<tr style='background-color: #e8f5e9;'>"
                 "<th>מספר תכנית</th><th>שם התכנית</th><th>מינה\"ק</th>"
                 "<th>סטטוס</th><th>יח\"ד (נכנס→יוצא)</th><th>מסחר (מ\"ר)</th>"
                 "<th>שב\"צ יוצא</th><th>שצ\"פ יוצא</th><th>הפרשה</th>"
                 "<th>גובה/קומות</th><th>מבא\"ת</th>"
                 "</tr>")
        for p in new_plans:
            def _num(v):
                if not v and v != 0: return ''
                try:
                    f = float(v); return str(int(f)) if f.is_integer() else f"{f:.1f}"
                except: return str(v)
            status = p.get('status', '')
            is_obj = 'הפקדה להתנגדויות' in status
            status_cell = status + ('<br><b style="color:#1976d2">פתוחה להתנגדויות</b>' if is_obj else '')
            hgt = _num(p.get('High'))
            lvl = _num(p.get('level_num'))
            hgt_lvl = f"{hgt}מ׳ / {lvl}ק׳" if hgt and lvl else (hgt + 'מ׳' if hgt else (lvl + ' קומות' if lvl else ''))
            mavat = p.get('mavat_url', '')
            html += "<tr>"
            html += f"<td>{p.get('pl_number', '')}</td>"
            html += f"<td dir='rtl'>{p.get('name_he', '')}</td>"
            html += f"<td dir='rtl'>{p.get('minahak', '') or '-'}</td>"
            html += f"<td dir='rtl'>{status_cell}</td>"
            _ui = _num(p.get('units_in')); _uo = _num(p.get('units_total'))
            units_cell = (f"{_ui or '0'}→{_uo}" if _uo else (_ui or '-'))
            html += f"<td>{units_cell}</td>"
            html += f"<td>{_num(p.get('commerce_out')) or '-'}</td>"
            html += f"<td>{_num(p.get('shavatz_out_sqm')) or '-'}</td>"
            html += f"<td>{_num(p.get('shatzap_out')) or '-'}</td>"
            html += f"<td>{_num(p.get('hafrash_sqm')) or '-'}</td>"
            html += f"<td>{hgt_lvl or '-'}</td>"
            html += f"<td>{'<a href=' + repr(mavat) + '>קישור</a>' if mavat else '-'}</td>"
            html += "</tr>"
        html += "</table><br>"

    # Status updates section
    if has_updates:
        html += "<h2>עידכון סטטוסים ממבאת</h2>"
        html += f"<p>הסקריפט עדכן בהצלחה {len(updates)} סעיפים בגוגל שיטס:</p>"
        html += "<table border='1' cellpadding='8' style='border-collapse: collapse;'>"
        html += "<tr style='background-color: #f2f2f2;'><th>מספר תכנית</th><th>מינה\"ק</th><th>שם תכנית</th><th>סטטוס קודם</th><th>סטטוס חדש</th><th>תאריך אישור</th></tr>"
        for u in updates:
            name_he = (u.get('plan_name_he') or '').split('זיהוי וסיווג')[0].strip().rstrip(',').strip()
            html += f"<tr>"
            html += f"<td>{u.get('plan_name', '')}</td>"
            html += f"<td>{u.get('minhak', '')}</td>"
            html += f"<td>{name_he}</td>"
            html += f"<td>{u['old_status']}</td>"
            html += f"<td><b>{u['new_status']}</b></td>"
            html += f"<td>{u['new_date']}</td>"
            html += f"</tr>"
        html += "</table><br>"

    # Enrichment section (staging / hafrash-type / floors / developer) from the
    # enrich_changed_plans run that invoked this sync — folded in for one email.
    if has_enrich:
        html += "<h2>העשרת תכניות חדשות / משנות-סטטוס</h2>"
        html += f"<p>{len(enrich_rows)} תכניות עובדו:</p>"
        html += "<table border='1' cellpadding='8' style='border-collapse: collapse;'>"
        html += ("<tr style='background-color: #ede7f6;'><th>תכנית</th><th>סיבה</th>"
                 "<th>סטטוס</th><th>שלביות</th><th>סוג הפרשה</th><th>קומות</th>"
                 "<th>יזם</th><th>הערה</th></tr>")
        for r in enrich_rows:
            html += ("<tr>"
                     f"<td>{r.get('plan_name','')}</td><td dir='rtl'>{r.get('reason','')}</td>"
                     f"<td dir='rtl'>{r.get('status','')}</td><td dir='rtl'>{r.get('staging','')}</td>"
                     f"<td dir='rtl'>{r.get('hafrash_type','')}</td><td dir='rtl'>{r.get('floors','')}</td>"
                     f"<td dir='rtl'>{r.get('developer','')}</td><td dir='rtl'>{r.get('note','')}</td>"
                     "</tr>")
        html += "</table><br>"

    # Sensitive field-change digest (from plan_changelog.jsonl)
    if has_digest:
        html += digest_html

    # Objection dates section — only future dates (past dates are stored but not surfaced)
    if has_objections:
        html += "<h2>תאריכי התנגדויות שעודכנו</h2>"
        html += f"<p>נמצאו {len(future_objections)} תאריכי תום הפקדה חדשים:</p>"
        html += "<table border='1' cellpadding='8' style='border-collapse: collapse;'>"
        html += "<tr style='background-color: #e3f2fd;'><th>מספר תכנית</th><th>מינה\"ק</th><th>שם תכנית</th><th>תאריך קודם</th><th>תאריך חדש</th></tr>"
        for o in future_objections:
            name_he = (o.get('plan_name_he') or '').split('זיהוי וסיווג')[0].strip().rstrip(',').strip()
            html += f"<tr>"
            html += f"<td>{o.get('plan_name', '') or o['taba']}</td>"
            html += f"<td>{o.get('minhak', '')}</td>"
            html += f"<td>{name_he}</td>"
            html += f"<td>{o.get('old_date', '') or '-'}</td>"
            html += f"<td><b>{o['objection_end']}</b></td>"
            html += f"</tr>"
        html += "</table><br>"

    # XPLAN geometry check section — one row per plan, columns per change type.
    if has_xplan:
        plan_meta = {u['plan_name']: {
            'plan_name_he': u.get('plan_name_he', ''),
            'minhak': u.get('minhak', ''),
            'status': u.get('new_status', ''),
        } for u in (updates or []) if u.get('plan_name')}
        xplan_summary, xplan_rows = _parse_xplan_report(xplan_report, plan_meta)
        # Group flat rows by plan_name, with one cell per change-type.
        # emoji → column key
        col_for_emoji = {
            '🗺️': 'landuse',
            '🛤️': 'easements',
            '🌳': 'trees',
            '📊': 'table5',
        }
        grouped = {}  # plan_name → {meta + cells}
        plan_order = []  # preserve appearance order
        for r in xplan_rows:
            pn = r.get('plan_name', '')
            if not pn:
                continue
            if pn not in grouped:
                grouped[pn] = {
                    'plan_name': pn,
                    'plan_name_he': r.get('plan_name_he', ''),
                    'minhak': r.get('minhak', ''),
                    'status': r.get('status', ''),
                    'cells': {'landuse': [], 'easements': [], 'trees': [], 'table5': []},
                }
                plan_order.append(pn)
            col = col_for_emoji.get(r.get('type_emoji', ''))
            detail = r.get('details', '')
            if col:
                grouped[pn]['cells'][col].append(detail)
            else:
                # ❓ "לא נמצא ב-XPLAN" → surface in landuse cell with a clear marker
                grouped[pn]['cells']['landuse'].append(f"⚠ {detail}")

        html += "<h2>בדיקת XPLAN (geometry / ייעודים / שב\"צ)</h2>"
        for s in xplan_summary:
            html += f"<p style='margin: 4px 0;'>{s}</p>"
        if grouped:
            html += "<table border='1' cellpadding='8' style='border-collapse: collapse;'>"
            html += ("<tr style='background-color: #f2f2f2;'>"
                     "<th>מספר תכנית</th><th>מינה\"ק</th><th>שם תכנית</th>"
                     "<th>סטטוס תכנית</th>"
                     "<th>🗺️ ייעודי קרקע</th><th>🛤️ זיקות הנאה</th>"
                     "<th>🌳 עצים</th><th>📊 טבלה 5 + נכנס</th></tr>")
            for pn in plan_order:
                row = grouped[pn]
                name_he = (row['plan_name_he'] or '').split('זיהוי וסיווג')[0].strip().rstrip(',').strip()
                cells = row['cells']

                def _cell(key):
                    return '<br>'.join(cells[key]) if cells[key] else ''

                html += "<tr>"
                html += f"<td>{row['plan_name']}</td>"
                html += f"<td>{row['minhak']}</td>"
                html += f"<td dir='rtl'>{name_he}</td>"
                html += f"<td dir='rtl'>{row['status']}</td>"
                html += f"<td dir='rtl'>{_cell('landuse')}</td>"
                html += f"<td dir='rtl'>{_cell('easements')}</td>"
                html += f"<td dir='rtl'>{_cell('trees')}</td>"
                html += f"<td dir='rtl'>{_cell('table5')}</td>"
                html += "</tr>"
            html += "</table><br>"

    html += "<p>הודעה זו נשלחה אוטומטית על ידי סקריפט הסנכרון של מבאת.</p></body></html>"

    msg = MIMEMultipart("alternative")
    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECEIVER_EMAIL

    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.send_message(msg)
        server.quit()
        log_msg("Email sent successfully!")
        # Advance the changelog-digest marker so the next email doesn't repeat
        # the same field changes. Only after a successful send.
        if digest_newest_ts:
            try:
                with open(LAST_CHANGELOG_EMAIL, "w", encoding="utf-8") as f:
                    f.write(digest_newest_ts)
            except OSError as e:
                log_msg(f"Failed to write changelog-digest marker: {e}")
        # Consume the enrichment payload so it isn't re-emailed on the next run.
        if has_enrich and enrich_payload_path and os.path.exists(enrich_payload_path):
            try:
                os.remove(enrich_payload_path)
            except OSError as e:
                log_msg(f"Failed to remove enrich payload: {e}")
        # Mark the new-plans report as consumed so tomorrow's run doesn't
        # re-include the same plans. Only rename after a successful send.
        if has_new_plans and os.path.exists(new_plans_report_path):
            consumed_path = new_plans_report_path + f".consumed_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            try:
                os.rename(new_plans_report_path, consumed_path)
                log_msg(f"Marked new_plans_report as consumed: {consumed_path}")
            except Exception as e:
                log_msg(f"Failed to rename new_plans_report: {e}")
    except Exception as e:
        log_msg(f"Failed to send email: {e}")

async def main():
    log_msg("==================================================")
    log_msg("Starting automated Mavat Status Sync")
    log_msg("==================================================")

    ONLY_STATUS = None
    PLANS_FILTER = None  # optional set of plan_name values to restrict to
    INCLUDE_TERMINAL = "--include-terminal" in sys.argv  # monthly revival sweep
    for arg in sys.argv[1:]:
        if arg.startswith('--only-status='):
            ONLY_STATUS = arg.split('=', 1)[1]
        elif arg.startswith('--plans-file='):
            _pf = arg.split('=', 1)[1]
            with open(_pf, encoding='utf-8') as _f:
                PLANS_FILTER = {ln.strip() for ln in _f if ln.strip()}
            log_msg(f"PLANS FILTER: restricting to {len(PLANS_FILTER)} plan_name(s) from {_pf}")

    progress = load_progress()
    log_msg("Connecting to Google Sheets...")
    sheet = get_sheet()
    all_data = sheet.get_all_values()
    log_msg(f"Total rows in sheet: {len(all_data) - 1}")

    # Sanity check: row 1 must be the header ('agam_id' in A1). If not,
    # the sheet was corrupted (see 2026-07-12 incident where detect_new_plans
    # overwrote the header with plan data, causing update_mavat_ui to shift
    # its row-based writes onto the WRONG plan). Abort BEFORE any writes.
    if not all_data or not all_data[0] or all_data[0][0].strip() != 'agam_id':
        log_msg(f"FATAL: GS row 1 is not the header (A1={all_data[0][0] if all_data and all_data[0] else 'MISSING'!r}). "
                f"Aborting to prevent row-shifted writes. Run _restore_gs_header.py first.")
        return

    if ONLY_STATUS:
        status_filter = {ONLY_STATUS}
    elif INCLUDE_TERMINAL:
        status_filter = TARGET_STATUSES | TERMINAL_STATUSES
        log_msg(f"INCLUDE-TERMINAL: also re-checking {len(TERMINAL_STATUSES)} terminal statuses "
                f"(נדחתה/נגנזה) — plans that may have come back to life.")
    else:
        status_filter = TARGET_STATUSES

    rows_to_check = []
    for row_idx, row in enumerate(all_data[1:], start=2):
        if len(row) >= COL_MAVAT_DATE:
            status = row[COL_STATUS_MAVAT - 1].strip() if len(row) >= COL_STATUS_MAVAT else ""
            agam_id = row[COL_AGAM_ID - 1].strip() if len(row) >= COL_AGAM_ID else ""
            plan_name_val = row[COL_PLAN_NAME - 1].strip() if len(row) >= COL_PLAN_NAME else ""
            if PLANS_FILTER is not None and plan_name_val not in PLANS_FILTER:
                continue
            # Out-of-scope safety net: never refresh blocklisted plans (Gilo plans
            # are removed upstream and detect_new_plans won't re-add them).
            if is_blocklisted(plan_name_val):
                continue
            # When an explicit --plans-file names the plans to refresh, process
            # them regardless of status: a plan that just changed TO an approved
            # (terminal) status — exactly when Table 5 finalises — is not in
            # TARGET_STATUSES and would otherwise be skipped. The normal
            # scheduled run (PLANS_FILTER is None) keeps the status gate.
            if (PLANS_FILTER is not None or status in status_filter) and agam_id:
                if agam_id.endswith('.0'):
                    agam_id = agam_id[:-2]
                rows_to_check.append({
                    'row': row_idx,
                    'plan_name': row[COL_PLAN_NAME - 1].strip() if len(row) >= COL_PLAN_NAME else "",
                    'plan_name_he': row[COL_PLAN_NAME_HE - 1].strip() if len(row) >= COL_PLAN_NAME_HE else "",
                    'minhak': row[COL_MINHAK - 1].strip() if len(row) >= COL_MINHAK else "",
                    'agam_id': agam_id,
                    'current_status': status,
                    'current_date': row[COL_MAVAT_DATE - 1].strip() if len(row) >= COL_MAVAT_DATE else "",
                })
    
    if ONLY_STATUS:
        log_msg(f"FILTER OVERRIDE: only checking status='{ONLY_STATUS}'")
    log_msg(f"Found {len(rows_to_check)} plans with target statuses (total).")

    RETRY_ONLY = "--retry" in sys.argv
    
    rows_to_check_final = []
    if RETRY_ONLY:
        log_msg("Filtering for RETRY ONLY (plans that previously failed)...")
        for item in rows_to_check:
            aid = item['agam_id']
            # Retry if it's not in progress OR if it has an error
            if aid not in progress or progress[aid].get('error'):
                rows_to_check_final.append(item)
    else:
        # Normal run - wipe progress and check everything
        log_msg("Normal run: Clearing old progress...")
        progress = {}
        rows_to_check_final = rows_to_check
        
    log_msg(f"Proceeding to check {len(rows_to_check_final)} plans.")
    if not rows_to_check_final:
        log_msg("No plans to check. Exiting.")
        return
        
    remaining = rows_to_check_final
    
    if remaining:
        log_msg("Starting Playwright UI scraper (Automated)...")
        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                BROWSER_DATA,
                headless=False,
                viewport={'width': 1280, 'height': 800},
            )
            page = context.pages[0] if context.pages else await context.new_page()
            page.set_default_navigation_timeout(90000)
            
            # --- WAF Auto Solver Wait ---
            log_msg("Loading initial Mavat page to test WAF...")
            try:
                await page.goto("https://mavat.iplan.gov.il/SV4/1/1000247867/310", wait_until='domcontentloaded', timeout=60000)
            except Exception:
                pass
                
            log_msg("Waiting 45 seconds to let WAF challenge clear (or for you to solve it manually)...")
            await asyncio.sleep(45)
            log_msg("Proceeding with scraping...")
            
            for index, item in enumerate(remaining, start=1):
                aid = item['agam_id']
                log_msg(f"[{index}/{len(remaining)}] Checking AGAM {aid}...")
                
                url = f"https://mavat.iplan.gov.il/SV4/1/{aid}/310"
                result = {'agam_id': aid, 'error': None, 'new_status': None, 'new_date': None}
                
                # Per-ID retry logic (up to 2 attempts)
                for attempt in range(1, 3):
                    if attempt > 1:
                        log_msg(f"  Attempt {attempt} for AGAM {aid}...")
                    
                    try:
                        await page.goto(url, wait_until='domcontentloaded')
                        await asyncio.sleep(5) # Increased robustness sleep
                        
                        content = await page.content()
                        if "לא ניתן לצפות בפרטי הישות בשלב זה" in content:
                            result['error'] = 'RESTRICTED'
                            log_msg(f"  ERR: Restricted access (RESTRICTED)")
                            break # No point in retrying restricted
                        else:
                            # Wait for either the plan data to load or an error message
                            try:
                                # Increased timeout from 15s to 30s
                                await page.wait_for_selector('.sv4-h1-aside .h3', timeout=30000)
                                
                                # Extract status using JS evaluation robust against new DOM structure
                                status_date_data = await page.evaluate('''() => {
                                    let status = null;
                                    let dateStr = null;
                                    
                                    const statusEl = document.querySelector('.sv4-h1-aside .h3');
                                    if (statusEl) {
                                        status = statusEl.textContent.trim();
                                        const dateEl = statusEl.nextElementSibling;
                                        if (dateEl && dateEl.tagName === 'P') {
                                            dateStr = dateEl.textContent.trim();
                                            const match = dateStr.match(/\\d{2}\\/\\d{2}\\/\\d{4}/);
                                            if (match) dateStr = match[0];
                                        }
                                    }
                                    
                                    return { status: status, date: dateStr };
                                }''')

                                if status_date_data['status']:
                                    result['new_status'] = status_date_data['status']
                                    result['new_date'] = status_date_data['date']
                                    result['plan_name'] = item['plan_name'] # Save local name
                                    result['minhak'] = item['minhak']
                                    result['error'] = None # Success

                                    # Check for blue objection button on Mavat page
                                    has_obj_btn = await page.evaluate(r"""() => {
                                        const els = [...document.querySelectorAll('a, button, div, span')];
                                        return els.some(el => el.textContent.includes('הגשת התנגדות לתוכנית'));
                                    }""")
                                    result['has_objection_btn'] = has_obj_btn

                                    old_stat = item['current_status']
                                    if result['new_status'] != old_stat:
                                        log_msg(f"  CHANGED: {old_stat} -> {result['new_status']} (Date: {result['new_date']})" + (" [objection btn]" if has_obj_btn else ""))
                                    else:
                                        log_msg(f"  OK: Status {result['new_status']} (Unchanged)" + (" [objection btn]" if has_obj_btn else ""))
                                    break # Success, don't retry
                                else:
                                    page_text = await page.evaluate("document.body.innerText")
                                    if "לא נמצאה" in page_text or "שגיאה" in page_text:
                                        result['error'] = 'Not found (404-like)'
                                        log_msg(f"  ERR: Not found on Mavat")
                                        break # Don't retry 404
                                    else:
                                        result['error'] = 'DOM_NOT_FOUND'
                                        log_msg(f"  ERR: DOM elements not found")
                                        # Will retry attempt 2
                                        
                            except Exception as wait_e:
                                result['error'] = 'TIMEOUT'
                                log_msg(f"  ERR: Timeout waiting for content (TIMEOUT)")
                                # Will retry attempt 2
                                
                    except Exception as e:
                        result['error'] = 'NAV_ERROR'
                        log_msg(f"  ERR: Navigation error: {str(e)}")
                        # Will retry attempt 2
                    
                    if result['error'] in ['TIMEOUT', 'NAV_ERROR', 'DOM_NOT_FOUND'] and attempt < 2:
                        await asyncio.sleep(5) # Wait before retry
                        continue
                    else:
                        break

                # On status change, re-check Table 5 + quantity balance while the
                # browser is on the plan page — mirrors the land-use check. An
                # explicit --plans-file (the enrichment pipeline naming the plans it
                # just refreshed) also re-checks, even when the status string is
                # unchanged: a re-deposited plan can carry a new Table 5 under the
                # same status.
                if (result.get('new_status') and not result.get('error')
                        and (result['new_status'] != item['current_status']
                             or PLANS_FILTER is not None)):
                    try:
                        _pn = item.get('plan_name', '')
                        _taba = str(int(_pn.split('-')[1])) if '-' in _pn else ''
                    except Exception:
                        _taba = ''
                    try:
                        # force=True: the status moved, so the plan may carry a new
                        # Table 5. Without it download_xlsx returns the file cached on
                        # first sight and the re-check is a no-op (101-1322452, 2026-08).
                        result['t5_balance'] = await scrape_t5_balance(
                            page, {'agam_id': aid, 'plan_number': item.get('plan_name', ''), 'taba': _taba},
                            force=True)
                        log_msg("  ↳ Table 5 + נכנס re-checked")
                    except Exception as te:
                        log_msg(f"  ↳ Table 5 re-check failed: {te}")

                progress[aid] = result
                save_progress(progress)
                await asyncio.sleep(2) # be nice to the server
                
            await context.close()
            
    # ── Update Google Sheets phase ──
    log_msg("\n=== Updating Google Sheets ===")
    
    updates = []
    
    for row_info in rows_to_check:
        aid = row_info['agam_id']
        if aid in progress and not progress[aid].get('error'):
            new_status = progress[aid].get('new_status', '')
            new_date = progress[aid].get('new_date', '')
            if new_status and new_status != row_info['current_status']:
                updates.append({
                    'row': row_info['row'],
                    'plan_name': progress[aid].get('plan_name', row_info.get('plan_name', '')),
                    'plan_name_he': row_info.get('plan_name_he', ''),
                    'minhak': progress[aid].get('minhak', row_info.get('minhak', '')),
                    'agam_id': aid,
                    'old_status': row_info['current_status'],
                    'new_status': new_status,
                    'new_date': new_date or '',
                })
                
    t5_report = []
    if not updates:
        log_msg("No status changes detected! All plans have the same status as before.")
    else:
        log_msg(f"\n{len(updates)} plans have changed status:")
        for u in updates:
            log_msg(f"  Row {u['row']}: {u['old_status']} → {u['new_status']} (date: {u['new_date']})")
            
        log_msg(f"Auto-updating {len(updates)} rows in Google Sheets...")

        # Race-safe write: re-fetch the sheet and RE-VERIFY each update's row
        # still holds the expected plan_name. Between our initial read (main
        # start) and now, other processes (detect_new_plans, manual edits) may
        # have shifted rows. If plan_name at the recorded row doesn't match,
        # look up the plan_name in fresh data. If STILL not found, skip.
        # 2026-07-12 incident: without this, 5 status writes went to wrong plans.
        sheet = get_sheet()
        fresh_data = sheet.get_all_values()
        # Sanity check: row 1 must be the header before we do anything
        if not fresh_data or not fresh_data[0] or fresh_data[0][0].strip() != 'agam_id':
            log_msg(f"FATAL: GS row 1 is not the header at write time "
                    f"(A1={fresh_data[0][0] if fresh_data and fresh_data[0] else 'MISSING'!r}). "
                    f"Aborting all writes.")
            return
        fresh_row_by_plan = {}
        for _ri, _row in enumerate(fresh_data[1:], start=2):
            if len(_row) >= COL_PLAN_NAME:
                _pn = _row[COL_PLAN_NAME - 1].strip()
                if _pn:
                    fresh_row_by_plan[_pn] = _ri

        batch_updates = []
        skipped = []
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for u in updates:
            captured_row = u['row']
            expected_plan = u.get('plan_name', '')
            # Re-verify: what's the plan_name AT captured_row NOW?
            actual_at_row = ''
            if 1 <= captured_row - 1 < len(fresh_data):
                _r = fresh_data[captured_row - 1]
                if len(_r) >= COL_PLAN_NAME:
                    actual_at_row = _r[COL_PLAN_NAME - 1].strip()
            if actual_at_row == expected_plan:
                target_row = captured_row  # unchanged — safe
            else:
                # Sheet shifted. Find plan by name in fresh data.
                looked_up = fresh_row_by_plan.get(expected_plan)
                if looked_up is None:
                    log_msg(f"  SKIP write for {expected_plan}: not found in fresh sheet "
                            f"(captured row {captured_row} now has plan {actual_at_row!r})")
                    skipped.append(u)
                    continue
                log_msg(f"  Row-shift detected for {expected_plan}: captured row "
                        f"{captured_row} now has {actual_at_row!r}; writing to row {looked_up} instead")
                target_row = looked_up
                u['row'] = looked_up  # so downstream code (t5, geojson) uses the correct row

            batch_updates.append({
                'range': f'{gspread.utils.rowcol_to_a1(target_row, COL_STATUS_MAVAT)}',
                'values': [[u['new_status']]]
            })
            if u['new_date']:
                batch_updates.append({
                    'range': f'{gspread.utils.rowcol_to_a1(target_row, COL_MAVAT_DATE)}',
                    'values': [[u['new_date']]]
                })
            batch_updates.append({
                'range': f'{gspread.utils.rowcol_to_a1(target_row, COL_LAST_MODIFIED)}',
                'values': [[current_time]]
            })

        if skipped:
            log_msg(f"  {len(skipped)} update(s) skipped due to unresolvable row shift.")
        # Drop skipped from updates so downstream steps don't re-touch them
        updates = [u for u in updates if u not in skipped]

        # ── Table 5 + נכנס re-check for the changed plans (mirrors land-use check) ──
        try:
            _all = sheet.get_all_values()
            _h = {n: i for i, n in enumerate(_all[0])} if _all else {}
            for u in updates:
                tb = progress.get(u['agam_id'], {}).get('t5_balance')
                if not tb:
                    continue
                ridx = u['row']
                if ridx - 1 >= len(_all):
                    continue
                cell_ups, rep = t5_compute_changes(_h, _all[ridx - 1], tb, u['plan_name'])
                for c0, val in cell_ups.items():
                    batch_updates.append({'range': gspread.utils.rowcol_to_a1(ridx, c0 + 1),
                                          'values': [[val]]})
                t5_report.extend(rep)
            if t5_report:
                n_plans = sum(1 for l in t5_report if l.startswith('  📊'))
                log_msg(f"  📊 Table 5/נכנס updated on {n_plans} changed plan(s)")
        except Exception as e:
            log_msg(f"  Table 5 GS compare failed: {e}")

        if batch_updates:
            sheet.spreadsheet.values_batch_update({
                'valueInputOption': 'RAW',
                'data': batch_updates
            })
            log_msg(f"\n✓ Successfully updated {len(updates)} rows in Google Sheets!")

        update_geojson(updates)
        update_future_shavaz_status(updates)

    # ── Check XPLAN for geometry/landuse/shavaz changes ──
    # New plans (added by detect_new_plans this run) also need their land-use
    # polygons mirrored into landuse_xplan.geojson so the ייעודי קרקע layer shows
    # them — route them through the same XPLAN sync as status-changed plans.
    _new_for_xplan = [{'plan_name': (p.get('pl_number') or '').strip(),
                       'new_status': (p.get('status') or '').strip()}
                      for p in (_load_pending_new_plans()[0] or [])
                      if (p.get('pl_number') or '').strip()]
    xplan_targets = list(updates) + _new_for_xplan
    xplan_report = []
    if xplan_targets:
        log_msg("\n=== Checking XPLAN for geometry updates ===")
        xplan_report = check_xplan_updates(xplan_targets)
    # Surface the Table 5 / נכנס deltas in the same report/email
    if t5_report:
        xplan_report = (xplan_report or []) + [""] + t5_report

    # ── Update has_objection_btn for all checked plans ──
    btn_batch = []
    for row_info in rows_to_check:
        aid = row_info['agam_id']
        if aid in progress and not progress[aid].get('error'):
            has_btn = 'TRUE' if progress[aid].get('has_objection_btn') else 'FALSE'
            btn_batch.append({
                'range': gspread.utils.rowcol_to_a1(row_info['row'], COL_HAS_OBJ_BTN),
                'values': [[has_btn]]
            })
    if btn_batch:
        sheet = get_sheet()
        sheet.spreadsheet.values_batch_update({'valueInputOption': 'RAW', 'data': btn_batch})
        log_msg(f"Updated {len(btn_batch)} rows with has_objection_btn in Google Sheets")
    update_geojson_objection_btn(progress, rows_to_check)

    # ── Fetch objection dates via YK Jerusalem API (for all plans in הפקדה status) ──
    # Re-fetch all_data first so it includes the has_objection_btn updates we just made
    all_data = sheet.get_all_values()
    objection_results = fetch_objection_dates(all_data, sheet, progress)

    # ── Refresh the Table 5 "unit bonus" overlay (side-JSON) ──
    # The t5 recheck above downloads fresh Table 5 files into temp_xlsx for any
    # status-changed plan; rebuild data/unit_bonus.json so a plan that gains (or
    # loses) the "תותר תוספת של עד N% ממספר יח״ד" note is reflected in the popup
    # and the permits-vs-תב״ע comparison. Additive + guarded — never breaks sync.
    try:
        import build_unit_bonus
        def _apply_bonus(data):
            fresh = build_unit_bonus.build(write=False)
            if data.get('by_plan') == fresh.get('by_plan'):
                return False
            data.clear(); data.update(fresh); return True
        update_json_and_push('data/unit_bonus.json', _apply_bonus,
                             'data: refresh Table 5 unit-bonus overlay (update_mavat_ui)')
    except Exception as e:
        log_msg(f"[unit_bonus] refresh skipped: {e}")

    # ── Send email if anything changed ──
    send_email_notification(updates, objection_results, xplan_report)

    log_msg(f"\nDone! {datetime.now().strftime('%H:%M:%S')}")

if __name__ == '__main__':
    asyncio.run(main())
