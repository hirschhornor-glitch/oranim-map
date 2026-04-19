"""
detect_new_plans.py
-------------------
Detects new plans in XPLAN that don't exist in our Google Sheets or plans.geojson.
For each new plan: enriches from Mavat API, adds to Sheets, updates GeoJSON, sends email.

Usage:
    python detect_new_plans.py                  # dry-run (report only)
    python detect_new_plans.py --update         # update Sheets + GeoJSON + email
    python detect_new_plans.py --setup-gmail    # one-time Gmail OAuth setup

Requirements:
    pip install gspread google-auth google-auth-oauthlib google-api-python-client
    pip install playwright requests
"""

import asyncio
import json
import os
import ssl
import sys
import argparse
import base64
from datetime import datetime
from pathlib import Path
from collections import defaultdict

import warnings
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
import gspread
from google.oauth2.service_account import Credentials

# ─── Config ───────────────────────────────────────────────────────────────────

CREDS_FILE     = r"C:\ORANIM\oranim-490018-ceaf784afe61.json"
SHEET_ID       = "1_AcuuA1CNPh6jXc_lZKNghfpEF1aDPV8Zci8QPz2WVE"
PLANS_GEOJSON  = r"C:\ORANIM\oranim-app\data\plans.geojson"
BOUNDARY_GEOJSON = r"C:\ORANIM\oranim-app\data\district_oranim.geojson"

GITHUB_REPO    = "hirschhornor-glitch/oranim-map"
BROWSER_DATA   = r"C:\ORANIM\.browser_data"

# Email config (SMTP with Gmail App Password — same as send_meeting_notification.py)
EMAIL_SENDER    = "hirschhorn.or@gmail.com"
EMAIL_PASSWORD  = os.environ.get("GMAIL_APP_PASSWORD", "")
EMAIL_RECIPIENT = os.environ.get("EMAIL_RECIPIENT", "Or_hi@jerusalem.muni.il")

# XPLAN API
XPLAN_URL = "https://ags.iplan.gov.il/arcgisiplan/rest/services/PlanningPublic/Xplan/MapServer/4/query"
MAX_PER_REQUEST = 1000

# Output files
REPORT_FILE  = r"C:\ORANIM\new_plans_report.json"
SUMMARY_FILE = r"C:\ORANIM\last_detection_summary.txt"

# Google Sheets column indices (1-based) — matches Oranim_Taba structure
SHEET_COLUMNS = {
    'agam_id': 1,          # A
    'ver_id': 2,           # B  (also used for agam_id in update_mavat — here it's the actual col)
    'taba': 3,             # C
    'status_mavat': 4,     # D
    'mavat_url': 5,        # E
    'plan_name': 6,        # F
    'plan_name_he': 7,     # G
}


# ─── SSL workaround for iplan.gov.il ─────────────────────────────────────────

class _LegacySSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.set_ciphers('DEFAULT:@SECLEVEL=0')
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)

_SESSION = requests.Session()
_SESSION.mount('https://ags.iplan.gov.il', _LegacySSLAdapter())


# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_israel_time():
    try:
        import zoneinfo
        tz_il = zoneinfo.ZoneInfo("Asia/Jerusalem")
    except ImportError:
        import pytz
        tz_il = pytz.timezone("Asia/Jerusalem")
    return datetime.now(tz_il).replace(tzinfo=None)


def normalize_plan_number(pn):
    """Normalize plan number for comparison: '101-0216515' -> '216515'
    Also handles 'תתל/ 86' style national infrastructure plans."""
    if not pn:
        return ''
    pn = str(pn).strip()
    # National infrastructure plans (תתל) - keep as-is but normalize whitespace
    if pn.startswith('תתל'):
        return ' '.join(pn.split())
    if '-' in pn:
        _, num = pn.split('-', 1)
        try:
            return str(int(num))  # strip leading zeros
        except ValueError:
            return num
    try:
        return str(int(pn))
    except ValueError:
        return pn


def get_sheet():
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    # Support both file-based (local) and env-based (CI) credentials
    google_creds_env = os.environ.get('GOOGLE_CREDS')
    if google_creds_env:
        creds_dict = json.loads(google_creds_env)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    else:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1


def get_bbox_from_boundary():
    """Load district_oranim boundary and return ITM bbox."""
    with open(BOUNDARY_GEOJSON, encoding='utf-8') as f:
        data = json.load(f)

    # The boundary is in WGS84, we need ITM (EPSG:2039) bbox
    # Use a simple approach: hardcoded bbox for Oranim area in ITM
    # This matches the values used in export_xplan_landuse.py after loading from ROVA.gpkg
    # Oranim district approximate ITM bounds:
    coords = []
    for feat in data['features']:
        geom = feat['geometry']
        if geom['type'] == 'Polygon':
            for ring in geom['coordinates']:
                coords.extend(ring)
        elif geom['type'] == 'MultiPolygon':
            for poly in geom['coordinates']:
                for ring in poly:
                    coords.extend(ring)

    if not coords:
        raise ValueError("No coordinates found in boundary GeoJSON")

    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]

    # Convert WGS84 bounds to approximate ITM using simple formula
    # For Jerusalem area: lon ~35.1-35.3, lat ~31.7-31.85
    # ITM: x ~ 210000-230000, y ~ 625000-640000
    # Use pyproj if available, otherwise hardcode known bounds
    try:
        from pyproj import Transformer
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:2039", always_xy=True)
        x_min, y_min = transformer.transform(min(lons), min(lats))
        x_max, y_max = transformer.transform(max(lons), max(lats))
        return (int(x_min), int(y_min), int(x_max), int(y_max))
    except ImportError:
        # Fallback: hardcoded Jerusalem/Oranim area bbox in ITM
        # Generous bbox to capture all plans in the area
        return (216000, 628000, 222000, 635000)


# ─── Step 1: Fetch from XPLAN ────────────────────────────────────────────────

def fetch_xplan_plans(bbox_itm):
    """Fetch all features from XPLAN layer 4 for the given ITM bbox.
    Returns list of features with their properties and geometries."""
    all_features = []
    offset = 0
    minx, miny, maxx, maxy = bbox_itm

    print(f"Fetching from XPLAN API (bbox: {bbox_itm})...")

    while True:
        params = {
            'geometry':          f'{minx},{miny},{maxx},{maxy}',
            'geometryType':      'esriGeometryEnvelope',
            'inSR':              '2039',
            'spatialRel':        'esriSpatialRelIntersects',
            'outFields':         'pl_number,mp_id,pl_name,mavat_code,mavat_name,station,station_desc,last_update_date,shape_area,legal_area',
            'returnGeometry':    'true',
            'f':                 'geojson',
            'outSR':             '4326',   # WGS84 for direct use in plans.geojson
            'resultOffset':      offset,
            'resultRecordCount': MAX_PER_REQUEST,
        }
        try:
            resp = _SESSION.get(XPLAN_URL, params=params, timeout=60, verify=False)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  Error fetching offset={offset}: {e}")
            break

        feats = data.get('features', [])
        all_features.extend(feats)
        print(f"  fetched {len(feats)} features (total: {len(all_features)})")

        if len(feats) < MAX_PER_REQUEST:
            break
        offset += MAX_PER_REQUEST

    return all_features


def load_boundary_polygon():
    """Load district_oranim boundary as a list of (lon, lat) rings for point-in-polygon."""
    with open(BOUNDARY_GEOJSON, encoding='utf-8') as f:
        data = json.load(f)
    for feat in data['features']:
        geom = feat['geometry']
        if geom['type'] == 'Polygon':
            return geom['coordinates']
        elif geom['type'] == 'MultiPolygon':
            # Use first polygon (district is a single polygon)
            return geom['coordinates'][0]
    return None


def point_in_polygon(x, y, polygon_rings):
    """Ray-casting point-in-polygon test. polygon_rings[0] is outer ring."""
    ring = polygon_rings[0]  # outer ring
    n = len(ring)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = ring[i]
        xj, yj = ring[j]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def point_in_ring(x, y, ring):
    """Ray-casting point-in-polygon test for a single ring."""
    n = len(ring)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = ring[i]
        xj, yj = ring[j]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def feature_intersects_boundary(feat, boundary_rings):
    """Check if feature geometry intersects the boundary polygon.
    Returns True if any vertex of the feature is inside the boundary,
    or any vertex of the boundary is inside any of the feature's polygons."""
    geom = feat.get('geometry', {})
    if not geom:
        return False

    # Collect all outer rings of the feature
    feat_outer_rings = []
    if geom.get('type') == 'Polygon':
        feat_outer_rings = [geom['coordinates'][0]]
    elif geom.get('type') == 'MultiPolygon':
        for poly in geom['coordinates']:
            feat_outer_rings.append(poly[0])

    if not feat_outer_rings:
        return False

    boundary_outer = boundary_rings[0]

    # Check if any feature vertex is inside boundary
    for ring in feat_outer_rings:
        for c in ring:
            if point_in_polygon(c[0], c[1], boundary_rings):
                return True

    # Check if any boundary vertex is inside any feature polygon
    for bx, by in boundary_outer:
        for ring in feat_outer_rings:
            if point_in_ring(bx, by, ring):
                return True

    return False


def extract_unique_plans(xplan_features, boundary_rings=None):
    """Group XPLAN features by pl_number and extract unique plan info.
    If boundary_rings provided, only include features that intersect the boundary.
    Returns dict: {normalized_number: {pl_number, mp_ids, features, ...}}"""
    plans = defaultdict(lambda: {
        'pl_number': '',
        'mp_ids': set(),
        'features': [],
        'mavat_names': set(),
        'total_area': 0,
    })

    skipped_outside = 0
    for feat in xplan_features:
        p = feat.get('properties', {})
        pl_num = p.get('pl_number', '')
        if not pl_num:
            continue

        # Spatial filter: check if feature intersects boundary
        if boundary_rings:
            if not feature_intersects_boundary(feat, boundary_rings):
                skipped_outside += 1
                continue

        norm = normalize_plan_number(pl_num)
        if not norm:
            continue

        plans[norm]['pl_number'] = pl_num
        mp_id = p.get('mp_id')
        if mp_id:
            plans[norm]['mp_ids'].add(str(int(mp_id)) if mp_id == int(mp_id) else str(mp_id))
        plans[norm]['features'].append(feat)
        mname = p.get('mavat_name', '')
        if mname:
            plans[norm]['mavat_names'].add(mname)
        plans[norm]['total_area'] += p.get('shape_area', 0) or 0
        # Store XPLAN metadata (pl_name, station_desc) as fallback for when Mavat is skipped
        pl_name = p.get('pl_name', '')
        if pl_name and not plans[norm].get('xplan_name'):
            plans[norm]['xplan_name'] = pl_name
        station_desc = p.get('station_desc', '')
        if station_desc and not plans[norm].get('xplan_status'):
            plans[norm]['xplan_status'] = station_desc

    if boundary_rings:
        print(f"  Filtered: {skipped_outside} features outside district boundary")

    # Convert sets to lists for JSON serialization
    for norm in plans:
        plans[norm]['mp_ids'] = list(plans[norm]['mp_ids'])
        plans[norm]['mavat_names'] = list(plans[norm]['mavat_names'])

    return dict(plans)


# ─── Step 2: Compare with existing data ──────────────────────────────────────

def load_existing_plan_numbers():
    """Load plan numbers from both Google Sheets and plans.geojson.
    Returns set of normalized plan numbers."""
    existing = set()

    # From plans.geojson
    print("Loading plans.geojson...")
    try:
        with open(PLANS_GEOJSON, encoding='utf-8') as f:
            geojson = json.load(f)
        for feat in geojson['features']:
            pn = feat['properties'].get('plan_name', '')
            norm = normalize_plan_number(pn)
            if norm:
                existing.add(norm)
        print(f"  {len(existing)} plans from GeoJSON")
    except Exception as e:
        print(f"  Error loading GeoJSON: {e}")

    # From Google Sheets
    print("Loading Google Sheets...")
    try:
        sheet = get_sheet()
        all_data = sheet.get_all_values()
        headers = all_data[0] if all_data else []

        # Find plan_name column
        pn_idx = None
        for i, h in enumerate(headers):
            if h.strip().lower() == 'plan_name':
                pn_idx = i
                break

        if pn_idx is not None:
            sheet_count = 0
            for row in all_data[1:]:
                if len(row) > pn_idx and row[pn_idx].strip():
                    norm = normalize_plan_number(row[pn_idx].strip())
                    if norm:
                        existing.add(norm)
                        sheet_count += 1
            print(f"  {sheet_count} plans from Sheets")
        else:
            print("  Warning: 'plan_name' column not found in Sheets")
    except Exception as e:
        print(f"  Error loading Sheets: {e}")

    return existing


def find_new_plans(xplan_plans, existing_numbers):
    """Find plans in XPLAN that are not in our existing data."""
    new_plans = {}
    for norm, info in xplan_plans.items():
        if norm not in existing_numbers:
            new_plans[norm] = info
    return new_plans


# ─── Step 3: Enrich from Mavat API ───────────────────────────────────────────

async def enrich_from_mavat(new_plans):
    """For each new plan with an mp_id (AGAM_ID), fetch details from Mavat API."""
    plans_with_agam = {k: v for k, v in new_plans.items() if v['mp_ids']}
    if not plans_with_agam:
        print("No plans with AGAM_ID to enrich from Mavat.")
        return

    print(f"\nEnriching {len(plans_with_agam)} plans from Mavat API...")

    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            BROWSER_DATA,
            headless=False,
            viewport={'width': 1280, 'height': 800},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            args=['--disable-blink-features=AutomationControlled'],
        )
        page = context.pages[0] if context.pages else await context.new_page()

        # Establish Mavat session
        print("Loading Mavat page to establish session...")
        try:
            await page.goto('https://mavat.iplan.gov.il/SV4/1/1000247867/310',
                           wait_until='domcontentloaded', timeout=120000)
        except Exception:
            pass
        await asyncio.sleep(10)

        print("\n" + "=" * 60)
        print("If you see a captcha in the browser, please solve it.")
        print("Once the Mavat page is loaded, press Enter.")
        print("=" * 60)
        input(">>> Press Enter to continue... ")
        await asyncio.sleep(3)

        # Test API
        test_result = await page.evaluate("""
            async () => {
                try {
                    const resp = await fetch('/rest/api/SV4/1?mid=1000247867&guid=0');
                    const data = JSON.parse(await resp.text());
                    return { ok: !!data.planDetails };
                } catch(e) {
                    return { ok: false, error: e.message };
                }
            }
        """)
        if not test_result.get('ok'):
            print(f"Mavat API test failed: {test_result}. Skipping enrichment.")
            await context.close()
            return

        print("Mavat API OK. Fetching plan details...")

        for norm, info in plans_with_agam.items():
            agam_id = info['mp_ids'][0]  # use first AGAM_ID
            try:
                result = await page.evaluate("""
                    async (id) => {
                        try {
                            const resp = await fetch('/rest/api/SV4/1?mid=' + id + '&guid=0');
                            const text = await resp.text();
                            if (!text || text.length < 50) return { error: 'empty response' };
                            const data = JSON.parse(text);
                            const d = data.planDetails || {};
                            return {
                                name_he: d.E_NAME || '',
                                status: d.LAST_STEP_DES || '',
                                status_date: d.LAST_STEP_DATE || '',
                                entity_type: d.ENTITY_SUBTYPE || '',
                                plan_id: d.NUMB || '',
                                permissions: d.PERMISSIONS || '',
                                authority: d.AUTH || '',
                                detailed: d.DETAILED || 0,
                                three_d: d.THREE_D || 0,
                                unity: (d.UNITY || '').substring(0, 500),
                                error: null
                            };
                        } catch(e) {
                            return { error: e.message };
                        }
                    }
                """, agam_id)

                if result.get('error'):
                    print(f"  {norm} (AGAM={agam_id}): ERROR - {result['error']}")
                else:
                    info['mavat_details'] = result
                    print(f"  {norm}: {result.get('name_he', '?')[:40]} | {result.get('status', '?')}")

                await asyncio.sleep(0.5)

            except Exception as e:
                print(f"  {norm} (AGAM={agam_id}): Exception - {e}")

        await context.close()


# ─── Step 4: Update Google Sheets ─────────────────────────────────────────────

def update_sheets(new_plans):
    """Add new plans as rows in Google Sheets."""
    plans_to_add = {k: v for k, v in new_plans.items() if v.get('mavat_details') or v.get('mp_ids')}
    if not plans_to_add:
        print("No enriched plans to add to Sheets.")
        return 0

    print(f"\nAdding {len(plans_to_add)} new plans to Google Sheets...")
    sheet = get_sheet()
    all_data = sheet.get_all_values()
    headers = all_data[0] if all_data else []

    # Build header index
    h_idx = {}
    for i, h in enumerate(headers):
        h_idx[h.strip().lower()] = i

    rows_to_append = []
    now_str = get_israel_time().strftime("%Y-%m-%d %H:%M:%S")

    for norm, info in plans_to_add.items():
        md = info.get('mavat_details', {})
        agam_id = info['mp_ids'][0] if info['mp_ids'] else ''
        pl_number = info['pl_number']

        # Use Mavat details if available, otherwise fall back to XPLAN metadata
        name_he = md.get('name_he', '') or info.get('xplan_name', '')
        status = md.get('status', '') or info.get('xplan_status', '')

        # Format date
        status_date = md.get('status_date', '')
        if status_date and 'T' in str(status_date):
            try:
                dt = datetime.fromisoformat(status_date.replace('Z', '+00:00'))
                status_date = dt.strftime('%d/%m/%Y')
            except Exception:
                pass

        # Build row matching sheet headers
        row = [''] * len(headers)

        def set_col(name, value):
            idx = h_idx.get(name.lower())
            if idx is not None and idx < len(row):
                row[idx] = str(value) if value else ''

        set_col('agam_id', agam_id)
        set_col('taba', norm)
        set_col('plan_name', pl_number)
        set_col('plan_name_he', name_he)
        set_col('status_mavat', status)
        set_col('mavat_date', status_date)
        set_col('mavat_url', f'https://mavat.iplan.gov.il/SV4/1/{agam_id}/310' if agam_id else '')
        set_col('plan_summary', md.get('permissions', '')[:200])
        set_col('last_modified', now_str)
        set_col('plan_type', '')
        set_col('minahak', '')
        set_col('sub_neighborhood', '')

        rows_to_append.append(row)

    if rows_to_append:
        sheet.append_rows(rows_to_append, value_input_option='RAW', table_range='A1')
        print(f"  Added {len(rows_to_append)} rows to Sheets.")

    return len(rows_to_append)


# ─── Step 5: Update plans.geojson ────────────────────────────────────────────

def create_plan_geometry(features):
    """Union all feature geometries for a plan into a single MultiPolygon.
    Features should already be in WGS84."""
    if not features:
        return None

    if len(features) == 1:
        return features[0].get('geometry')

    # Combine all geometries into a MultiPolygon
    all_coords = []
    for feat in features:
        geom = feat.get('geometry', {})
        if geom.get('type') == 'Polygon':
            all_coords.append(geom['coordinates'])
        elif geom.get('type') == 'MultiPolygon':
            all_coords.extend(geom['coordinates'])

    if not all_coords:
        return None

    return {
        'type': 'MultiPolygon',
        'coordinates': all_coords
    }


def update_geojson(new_plans, push_to_github=False):
    """Add new plan features to plans.geojson."""
    plans_to_add = {k: v for k, v in new_plans.items() if (v.get('mavat_details') or v.get('mp_ids')) and v['features']}
    if not plans_to_add:
        print("No plans with geometry to add to GeoJSON.")
        return 0

    print(f"\nAdding {len(plans_to_add)} new plans to plans.geojson...")

    with open(PLANS_GEOJSON, encoding='utf-8') as f:
        geojson = json.load(f)

    # Determine next fid
    max_fid = max((f['properties'].get('fid', 0) or 0 for f in geojson['features']), default=0)

    now_str = get_israel_time().strftime("%Y-%m-%d %H:%M:%S")
    added = 0

    for norm, info in plans_to_add.items():
        md = info.get('mavat_details', {})
        agam_id = info['mp_ids'][0] if info['mp_ids'] else ''
        geometry = create_plan_geometry(info['features'])
        if not geometry:
            continue

        # Format date
        status_date = md.get('status_date', '')
        if status_date and 'T' in str(status_date):
            try:
                dt = datetime.fromisoformat(status_date.replace('Z', '+00:00'))
                status_date = dt.strftime('%d/%m/%Y')
            except Exception:
                pass

        # Use Mavat details if available, otherwise fall back to XPLAN metadata
        name_he = md.get('name_he', '') or info.get('xplan_name', '')
        status = md.get('status', '') or info.get('xplan_status', '')

        max_fid += 1
        feature = {
            'type': 'Feature',
            'geometry': geometry,
            'properties': {
                'fid': max_fid,
                'plan_name': info['pl_number'],
                'agam_id': float(agam_id) if agam_id else None,
                'ver_id': None,
                'taba': norm,
                'mavat_url': f'https://mavat.iplan.gov.il/SV4/1/{agam_id}/310' if agam_id else '',
                'plan_name_he': name_he,
                'plan_summary': md.get('permissions', '')[:200],
                'architect': '',
                'developer': '',
                'units_total': '',
                'units_in': '',
                'units_add': None,
                'multiplier': '',
                'shavatz_in_sqm': '',
                'shavatz_in_prog': '',
                'shavatz_out_sqm': '',
                'shavatz_in_prog2': '',
                'shatzap_in': '',
                'shatzap_out': '',
                'commerce_in': '',
                'commerce_out': '',
                'hotels': '',
                'rental': '',
                'conditional_housing': '',
                'employment': '',
                'plan_type': '',
                'minahak': '',
                'sub_neighborhood': '',
                'overlapping_plans': 0.0,
                'status_mavat': status,
                'mavat_date': status_date,
                'building_permit': '',
                'permit_date': '',
                'permit_status': '',
                'stage': '',
                'request_type': '',
                'permit_date_data': '',
                'permit_addition': '',
                'permit_gap': '',
                'last_modified': now_str,
                'protected_housing': None,
                'permit_growth_reason': None,
            }
        }
        geojson['features'].append(feature)
        added += 1

    # Save locally
    with open(PLANS_GEOJSON, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False)
    print(f"  Added {added} features to plans.geojson (local)")

    # Push to GitHub if requested
    if push_to_github and added > 0:
        push_geojson_to_github(geojson)

    return added


def push_geojson_to_github(geojson_data):
    """Push updated plans.geojson to GitHub."""
    github_token = os.environ.get('GITHUB_TOKEN')
    if not github_token:
        print("  GITHUB_TOKEN not set, skipping GitHub push.")
        return False

    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }

    # Get current file SHA
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/data/plans.geojson"
    r = requests.get(url, headers=headers)
    sha = r.json().get('sha') if r.status_code == 200 else None

    # Upload
    content = json.dumps(geojson_data, ensure_ascii=False)
    payload = {
        "message": f"detect_new_plans: add new plans {get_israel_time().strftime('%Y-%m-%d %H:%M')}",
        "content": base64.b64encode(content.encode('utf-8')).decode('utf-8'),
    }
    if sha:
        payload["sha"] = sha

    r = requests.put(url, headers=headers, json=payload)
    if r.status_code in (200, 201):
        print("  Pushed to GitHub successfully.")
        return True
    else:
        print(f"  GitHub push failed: {r.status_code} {r.text[:200]}")
        return False


# ─── Step 6: Gmail notification ──────────────────────────────────────────────

def send_email(new_plans):
    """Send HTML email with new plans summary via SMTP (Gmail App Password)."""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    sender = EMAIL_SENDER
    password = EMAIL_PASSWORD
    recipient = EMAIL_RECIPIENT

    if not all([sender, password, recipient]):
        missing = []
        if not sender: missing.append("EMAIL_SENDER")
        if not password: missing.append("EMAIL_PASSWORD")
        if not recipient: missing.append("EMAIL_RECIPIENT")
        print(f"  Email skipped (missing: {', '.join(missing)})")
        return False

    # Build email content
    plans_list = {k: v for k, v in new_plans.items()
                  if v.get('mavat_details') or v.get('mp_ids')}
    count = len(plans_list)
    date_str = get_israel_time().strftime('%d/%m/%Y')

    subject = f"Oranim: {count} תכניות חדשות ({date_str})"

    rows_html = ""
    for norm, info in plans_list.items():
        md = info.get('mavat_details', {})
        agam_id = info['mp_ids'][0] if info['mp_ids'] else ''
        name = md.get('name_he', '') or info.get('xplan_name', '')
        status = md.get('status', '') or info.get('xplan_status', '')
        mavat_link = f'https://mavat.iplan.gov.il/SV4/1/{agam_id}/310' if agam_id else ''
        # Get minahak/sub_neighborhood from plan features in geojson
        minahak = ''
        sub_neigh = ''
        try:
            with open(PLANS_GEOJSON, encoding='utf-8') as gf:
                gj = json.load(gf)
            for feat in gj['features']:
                if feat['properties'].get('plan_name') == info['pl_number']:
                    minahak = feat['properties'].get('minahak', '')
                    sub_neigh = feat['properties'].get('sub_neighborhood', '')
                    break
        except:
            pass

        is_objection = 'הפקדה להתנגדויות' in status
        row_style = 'background:#e3f2fd;' if is_objection else ''
        rows_html += f"""<tr style="{row_style}">
            <td style="padding:6px;border:1px solid #ddd">{info['pl_number']}</td>
            <td style="padding:6px;border:1px solid #ddd" dir="rtl">{name}</td>
            <td style="padding:6px;border:1px solid #ddd" dir="rtl">{status}{'<br><b style=&quot;color:#1976d2&quot;>פתוחה להתנגדויות</b>' if is_objection else ''}</td>
            <td style="padding:6px;border:1px solid #ddd" dir="rtl">{minahak}</td>
            <td style="padding:6px;border:1px solid #ddd" dir="rtl">{sub_neigh}</td>
            <td style="padding:6px;border:1px solid #ddd">
                {'<a href="' + mavat_link + '">מבא&quot;ת</a>' if mavat_link else ''}
            </td>
        </tr>"""

    html = f"""<html><body dir="rtl" style="font-family:Arial,sans-serif">
    <h2>זוהו {count} תכניות חדשות באזור אורנים</h2>
    <p>תאריך: {date_str}</p>
    <table style="border-collapse:collapse;width:100%">
        <tr style="background:#f5f5f5">
            <th style="padding:8px;border:1px solid #ddd">מספר תכנית</th>
            <th style="padding:8px;border:1px solid #ddd">שם</th>
            <th style="padding:8px;border:1px solid #ddd">סטטוס</th>
            <th style="padding:8px;border:1px solid #ddd">מינה"ק</th>
            <th style="padding:8px;border:1px solid #ddd">תת-שכונה</th>
            <th style="padding:8px;border:1px solid #ddd">קישור</th>
        </tr>
        {rows_html}
    </table>
    <br>
    <p style="color:#888;font-size:12px">Generated by detect_new_plans.py</p>
    </body></html>"""

    msg = MIMEMultipart('alternative')
    msg['From'] = sender
    msg['To'] = recipient
    msg['Subject'] = subject
    msg.attach(MIMEText(html, 'html', 'utf-8'))

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender, password)
            server.send_message(msg)
        print(f"  Email sent to {recipient}")
        return True
    except Exception as e:
        print(f"  Email error: {e}")
        return False


# ─── Step 7: Report ──────────────────────────────────────────────────────────

def write_report(new_plans, sheets_added, geojson_added, email_sent):
    """Write JSON report and text summary."""
    now_str = get_israel_time().strftime("%Y-%m-%d %H:%M:%S")

    # JSON report
    report = {
        'timestamp': now_str,
        'new_plans_count': len(new_plans),
        'sheets_added': sheets_added,
        'geojson_added': geojson_added,
        'email_sent': email_sent,
        'plans': {}
    }
    for norm, info in new_plans.items():
        md = info.get('mavat_details', {})
        report['plans'][norm] = {
            'pl_number': info['pl_number'],
            'agam_ids': info['mp_ids'],
            'name_he': md.get('name_he', ''),
            'status': md.get('status', ''),
            'parcel_count': len(info['features']),
            'land_uses': info['mavat_names'][:10],
        }

    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\nReport saved: {REPORT_FILE}")

    # Text summary
    lines = [
        f"detect_new_plans.py | {now_str}",
        f"{'=' * 50}",
        f"New plans found: {len(new_plans)}",
        f"Added to Sheets: {sheets_added}",
        f"Added to GeoJSON: {geojson_added}",
        f"Email sent: {'yes' if email_sent else 'no'}",
        "",
    ]
    for norm, info in new_plans.items():
        md = info.get('mavat_details', {})
        lines.append(f"  {info['pl_number']}: {md.get('name_he', '?')[:50]} | {md.get('status', '?')}")

    summary = '\n'.join(lines)
    with open(SUMMARY_FILE, 'w', encoding='utf-8') as f:
        f.write(summary)

    print(summary)


# ─── Main ────────────────────────────────────────────────────────────────────

async def run(do_update=False, skip_mavat=False):
    sys.stdout.reconfigure(encoding='utf-8')
    print("=" * 60)
    print("  detect_new_plans.py")
    print(f"  Mode: {'UPDATE' if do_update else 'DRY-RUN'}")
    print(f"  Time: {get_israel_time().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Step 1: Fetch from XPLAN
    bbox = get_bbox_from_boundary()
    xplan_features = fetch_xplan_plans(bbox)
    if not xplan_features:
        print("No features returned from XPLAN. Exiting.")
        return

    # Load boundary for spatial filtering
    boundary_rings = load_boundary_polygon()
    if boundary_rings:
        print(f"Loaded district boundary ({len(boundary_rings[0])} vertices)")
    else:
        print("Warning: could not load district boundary, using all XPLAN features")

    xplan_plans = extract_unique_plans(xplan_features, boundary_rings)
    print(f"\nUnique plans in XPLAN (inside boundary): {len(xplan_plans)}")

    # Step 2: Compare
    existing = load_existing_plan_numbers()
    print(f"Total existing plans: {len(existing)}")

    new_plans = find_new_plans(xplan_plans, existing)
    print(f"\n{'*' * 40}")
    print(f"  NEW PLANS FOUND: {len(new_plans)}")
    print(f"{'*' * 40}")

    if not new_plans:
        print("\nNo new plans detected. Everything is up to date!")
        write_report({}, 0, 0, False)
        return

    # Display new plans
    for norm, info in new_plans.items():
        agam = info['mp_ids'][0] if info['mp_ids'] else 'N/A'
        uses = ', '.join(info['mavat_names'][:3])
        print(f"  {info['pl_number']} (AGAM={agam}) | {len(info['features'])} parcels | {uses}")

    if not do_update:
        print(f"\n--- DRY-RUN: no changes made. Use --update to apply. ---")
        write_report(new_plans, 0, 0, False)
        return

    # Step 3: Enrich from Mavat (skip in CI/headless mode)
    if not skip_mavat:
        await enrich_from_mavat(new_plans)
    else:
        print("\n--- Skipping Mavat enrichment (--no-mavat) ---")

    # Step 4: Update Sheets
    sheets_added = update_sheets(new_plans)

    # Step 5: Update GeoJSON
    push = bool(os.environ.get('GITHUB_TOKEN'))
    geojson_added = update_geojson(new_plans, push_to_github=push)

    # Step 6: Send email
    email_sent = send_email(new_plans)

    # Step 7: Report
    write_report(new_plans, sheets_added, geojson_added, email_sent)

    print(f"\nDone! {get_israel_time().strftime('%H:%M:%S')}")


def main():
    parser = argparse.ArgumentParser(description='Detect new plans in XPLAN')
    parser.add_argument('--update', action='store_true', help='Apply changes (Sheets + GeoJSON + email)')
    parser.add_argument('--no-mavat', action='store_true', help='Skip Mavat enrichment (for CI/headless)')
    args = parser.parse_args()

    asyncio.run(run(do_update=args.update, skip_mavat=args.no_mavat))


if __name__ == '__main__':
    main()
