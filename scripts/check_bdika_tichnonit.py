"""
check_bdika_tichnonit.py
Automated pipeline for plans that moved to "בדיקה תכנונית":
1. Identify plans in בדיקה תכנונית without שבצ data
2. Download Table 5 PDF from Mavat → extract שבצ (commerce/employment/public)
3. Fetch parcels + land use from XPLAN API
4. Update Google Sheets + GeoJSON

Usage:
    python check_bdika_tichnonit.py           # full run
    python check_bdika_tichnonit.py --dry      # identify only, no scraping
"""
import asyncio
import json
import os
import re
import ssl
import sys
import warnings
from datetime import datetime

import gspread
import pdfplumber
import requests
from google.oauth2.service_account import Credentials
from playwright.async_api import async_playwright
from requests.adapters import HTTPAdapter

warnings.filterwarnings("ignore")

# ── Config ──
CREDS_FILE    = r"C:\ORANIM\oranim-490018-ceaf784afe61.json"
SHEET_ID      = "1_AcuuA1CNPh6jXc_lZKNghfpEF1aDPV8Zci8QPz2WVE"
PLANS_GEOJSON = r"C:\ORANIM\oranim-app\data\plans.geojson"
SHAVAZ_GEOJSON = r"C:\ORANIM\oranim-app\data\future_shavaz.geojson"
TEMP_DIR      = r"C:\ORANIM\temp_pdfs"
MAVAT_BASE    = "https://mavat.iplan.gov.il"
XPLAN_URL     = "https://ags.iplan.gov.il/arcgisiplan/rest/services/PlanningPublic/Xplan/MapServer/4/query"
JLM_BBOX_ITM  = (210000, 622000, 238000, 642000)
PUBLIC_CODES  = {400, 410, 460, 1250, 1410, 1576, 1650, 1670}

# GS column numbers (1-indexed)
COL_PLAN_NAME     = 6   # F
COL_COMMERCE_OUT  = 23  # W
COL_EMPLOYMENT    = 27  # AA
COL_PUBLIC        = 18  # R: shavatz_out_sqm
COL_PUBLIC_DETAIL = 19  # S: shavatz_out_prog
COL_HAFRASH       = 43  # AQ
COL_HAFRASH_DETAIL= 44  # AR
COL_LAST_MOD      = 40  # AN

# ── Reuse: Hebrew fix from scrape_all_table5.py ──
REVERSED_MARKERS = ['םירוגמ', 'רחסמ', 'הקוסעת', 'םידרשמ', 'רוביצ', 'תודסומ',
                    'דועי', 'תויונח', 'ןוינח', 'ירוגמ', 'הינב']

def fix_hebrew(text):
    if not text: return ""
    text = str(text).strip()
    if any(m in text for m in REVERSED_MARKERS):
        words = text.split()
        fixed_words = []
        for w in words:
            rev = w[::-1]
            for old, new in [('ם', 'מ'), ('ן', 'נ'), ('ף', 'פ'), ('ך', 'כ'), ('ץ', 'צ')]:
                if rev.startswith(old): rev = new + rev[1:]
            for old, new in [('מ', 'ם'), ('נ', 'ן'), ('פ', 'ף'), ('כ', 'ך'), ('צ', 'ץ')]:
                if rev.endswith(old): rev = rev[:-1] + new
            fixed_words.append(rev)
        text = ' '.join(reversed(fixed_words))
    return text


def parse_number(val):
    if not val: return 0
    val = str(val).strip()
    val = re.sub(r'\(\d{1,2}\)\s*', '', val)
    val = re.sub(r'\s*\(\d{1,2}\)', '', val)
    val = val.replace(",", "").replace("'", "").replace("\u05f3", "")
    val = re.sub(r'[^\d.]', '', val)
    try: return float(val) if val else 0
    except: return 0


# ── Reuse: Table 5 extraction from scrape_all_table5.py ──
COMMERCE_KW    = ["מסחר", "רחסמ", "חנויות", "תויונח"]
EMPLOYMENT_KW  = ["תעסוקה", "הקוסעת", "משרדים", "םידרשמ"]
PUBLIC_KW      = ["מבנים ומוסדות ציבור", "רוביצ תודסומו םינבמ", "רוביצ", "ציבור",
                  "מוסדות", "תודסומ", "מבני ציבור", "מבנה ציבור"]
RESIDENTIAL_KW = ["מגורים", "םירוגמ", "ירוגמ"]


def extract_table5_from_pdf(pdf_path):
    """Extract commerce/employment/public sqm from Table 5 in a PDF."""
    results = {}
    try:
        with pdfplumber.open(pdf_path) as pdf:
            all_tables = []
            for page_num, page in enumerate(pdf.pages):
                tables = page.extract_tables({"vertical_strategy": "lines", "horizontal_strategy": "lines"})
                if not tables:
                    tables = page.extract_tables({"vertical_strategy": "text", "horizontal_strategy": "text"})
                for table in tables:
                    if table and len(table) > 2:
                        max_cols = max(len(r) for r in table if r)
                        if max_cols >= 8:
                            all_tables.append((page_num, table))

            if not all_tables:
                return results

            # Find columns by headers
            use_col = desig_col = area_col = parcel_col = -1
            use_kw = ["שומיש", "שימוש"]
            desig_kw = ["ייעוד", "דועי"]
            area_kw = ["כ\"הס", 'כ"הס', "סה\"כ", "שטחי בניי", "הינב יחטש"]
            parcel_kw = ["חטש יאת", "תאי שטח", "יאת", "מגרש"]

            for _, table in all_tables:
                for row in table[:3]:
                    if not row: continue
                    for ci, cell in enumerate(row):
                        if not cell: continue
                        ct = str(cell).strip()
                        if use_col < 0 and any(k in ct for k in use_kw): use_col = ci
                        if desig_col < 0 and any(k in ct for k in desig_kw): desig_col = ci
                        if area_col < 0 and any(k in ct for k in area_kw): area_col = ci
                        if parcel_col < 0 and any(k in ct for k in parcel_kw): parcel_col = ci

            if area_col < 0: return results

            # Extract data rows
            for _, table in all_tables:
                for row in table[1:]:
                    if not row or len(row) <= max(area_col, use_col if use_col >= 0 else 0):
                        continue
                    area_val = parse_number(row[area_col] if area_col < len(row) else "")
                    if area_val <= 0:
                        # Try adjacent columns
                        for offset in [1, -1, 2, -2]:
                            alt = area_col + offset
                            if 0 <= alt < len(row):
                                area_val = parse_number(row[alt])
                                if area_val > 0: break
                    if area_val <= 0: continue

                    use_text = fix_hebrew(str(row[use_col]).strip()) if use_col >= 0 and use_col < len(row) else ""
                    desig_text = fix_hebrew(str(row[desig_col]).strip()) if desig_col >= 0 and desig_col < len(row) else ""
                    parcel_text = str(row[parcel_col]).strip() if parcel_col >= 0 and parcel_col < len(row) else "0"

                    check_text = use_text if use_col >= 0 else desig_text
                    has_commerce = any(kw in check_text for kw in COMMERCE_KW)
                    has_employment = any(kw in check_text for kw in EMPLOYMENT_KW)
                    has_public = any(kw in check_text for kw in PUBLIC_KW)
                    if not has_public and desig_text:
                        has_public = any(kw in desig_text for kw in PUBLIC_KW)
                    has_residential = any(kw in desig_text for kw in RESIDENTIAL_KW)

                    if has_commerce and has_employment:
                        has_employment = False

                    if not (has_commerce or has_employment or has_public):
                        continue

                    parcel_key = re.sub(r'[^\d]', '', parcel_text) or "0"
                    if parcel_key not in results:
                        results[parcel_key] = {
                            "commerce": 0, "employment": 0, "public": 0,
                            "public_standalone": 0, "public_hafrash": 0,
                            "uses": [], "building_names": []
                        }

                    use_desc = use_text or desig_text
                    if has_commerce:
                        results[parcel_key]["commerce"] += area_val
                        results[parcel_key]["uses"].append(f"{use_desc} ({area_val:.0f})")
                    if has_employment:
                        results[parcel_key]["employment"] += area_val
                        results[parcel_key]["uses"].append(f"{use_desc} ({area_val:.0f})")
                    if has_public:
                        results[parcel_key]["public"] += area_val
                        if has_residential:
                            results[parcel_key]["public_hafrash"] += area_val
                        else:
                            results[parcel_key]["public_standalone"] += area_val
                        results[parcel_key]["uses"].append(f"{use_desc} ({area_val:.0f})")

    except Exception as e:
        print(f"    PDF error: {e}")
    return results


# ── Reuse: XPLAN API from fetch_xplan_public_parcels.py ──
class _LegacySSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *a, **kw):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.set_ciphers('DEFAULT:@SECLEVEL=0')
        kw['ssl_context'] = ctx
        return super().init_poolmanager(*a, **kw)

_SESSION = requests.Session()
_SESSION.mount('https://ags.iplan.gov.il', _LegacySSLAdapter())


def fetch_xplan_parcels(pl_number):
    """Fetch all parcels for a specific plan from XPLAN API."""
    all_feats = []
    offset = 0
    where = f"pl_number='{pl_number}'"
    while True:
        minx, miny, maxx, maxy = JLM_BBOX_ITM
        params = {
            'geometry': f'{minx},{miny},{maxx},{maxy}',
            'geometryType': 'esriGeometryEnvelope',
            'inSR': '2039', 'outSR': '4326',
            'spatialRel': 'esriSpatialRelIntersects',
            'outFields': 'mavat_code,mavat_name,mp_id,pl_number,num,legal_area,objectid,group_id,layer_id',
            'where': where,
            'returnGeometry': 'true',
            'f': 'geojson',
            'resultOffset': offset,
            'resultRecordCount': 1000,
        }
        try:
            resp = _SESSION.get(XPLAN_URL, params=params, timeout=60, verify=False)
            resp.raise_for_status()
            data = resp.json()
            feats = data.get('features', [])
            all_feats.extend(feats)
            if len(feats) < 1000: break
            offset += 1000
        except Exception as e:
            print(f"    XPLAN error: {e}")
            break
    return all_feats


def xplan_feat_to_shavaz(xfeat, taba, plan_info):
    """Convert an XPLAN feature to future_shavaz format."""
    xp = xfeat['properties']
    return {
        'type': 'Feature',
        'geometry': xfeat['geometry'],
        'properties': {
            'fid': None, 'LAYER_ID': xp.get('layer_id'), 'GROUP_ID': xp.get('group_id'),
            'SCENARIO': None, 'DEFQ': 0, 'PLAN_NAME': None, 'DATA_DATE': None,
            'MAVAT_CODE': xp.get('mavat_code'), 'MAVAT_NAME': xp.get('mavat_name', ''),
            'NAME': None, 'LABEL': None,
            'MIGRASH': str(xp.get('num', '')),
            'LEGAL_AREA': xp.get('legal_area'),
            'TYPE_CODE': 0, 'TYPE_NAME': None, 'SOURCE_COD': 0,
            'HASAVA': 0, 'STATUS': None, 'PL_CHANGE': None,
            'ADDRESS': None, 'PLACE_NO': 0, 'CALC_AREA': 0.0, 'OLD_MIG': None,
            'AGAM_ID': xp.get('mp_id'), 'TABA': taba,
            'C_TIME': None, 'CR_USER': 'bdika_auto', 'FromFile': 'XPLAN_API',
            '__REC_STAT': None, 'Mavat_Status': None,
            'BUILD_AREA': None, 'uses': None, 'Actual uses': None,
            'plan_name_he': plan_info.get('plan_name_he', ''),
            'POLY_AREA': round(xp.get('legal_area') or 0) or None,
        }
    }


# ── Step 1: Identify plans ──
def identify_plans():
    """Find plans in בדיקה תכנונית without שבצ data."""
    creds_file = os.environ.get('GOOGLE_CREDS_FILE', CREDS_FILE)
    if os.environ.get('GOOGLE_CREDS') and not os.path.exists(creds_file):
        creds_file = '/tmp/gcloud_creds.json'
    creds = Credentials.from_service_account_file(creds_file,
        scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
    sheet = gspread.authorize(creds).open_by_key(SHEET_ID).sheet1
    all_data = sheet.get_all_values()
    headers = all_data[0]
    h = {hdr.strip().lower(): i for i, hdr in enumerate(headers)}

    plans = []
    seen_tabas = set()
    for row_num, row in enumerate(all_data[1:], start=2):
        status = row[h['status_mavat']].strip()
        if 'בדיקה תכנונית' not in status and 'בבדיקה תכנונית' not in status:
            continue
        shavaz = row[h['shavatz_out_sqm']].strip() if 'shavatz_out_sqm' in h else ''
        commerce = row[h['commerce_out']].strip() if 'commerce_out' in h else ''
        # Skip if already has שבצ OR commerce data
        if shavaz and commerce:
            continue
        plan_name = row[h['plan_name']].strip()
        taba = row[h['taba']].strip()
        if taba in seen_tabas:
            continue
        seen_tabas.add(taba)
        agam_id = row[h['agam_id']].strip() if 'agam_id' in h else ''
        plans.append({
            'row': row_num, 'plan_name': plan_name, 'taba': taba,
            'agam_id': agam_id, 'status': status,
        })

    return plans, sheet, all_data, h, creds


# ── Step 2: Download PDF + extract ──
async def download_and_extract(plans):
    """Download Table 5 PDFs from Mavat and extract data."""
    os.makedirs(TEMP_DIR, exist_ok=True)
    results = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        ctx = await browser.new_context()
        page = await ctx.new_page()

        # Establish session
        print("Establishing Mavat session (15s)...")
        try:
            await page.goto(f"{MAVAT_BASE}/SV4/1/1000247867/310", wait_until="domcontentloaded", timeout=30000)
        except: pass
        await page.wait_for_timeout(15000)
        print("Session ready\n")

        for i, plan in enumerate(plans):
            pn = plan['plan_name']
            agam = plan['agam_id']
            taba = plan['taba']
            print(f"[{i+1}/{len(plans)}] {pn} (taba={taba})...", end=" ", flush=True)

            pdf_path = os.path.join(TEMP_DIR, f"{taba}.pdf")
            downloaded = os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 1000

            if not downloaded and agam:
                try:
                    url = f"{MAVAT_BASE}/SV4/1/{agam}/310"
                    try: await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                    except: pass

                    try:
                        await page.wait_for_function(
                            "() => document.body.innerText.includes('תוכנית') && document.body.innerText.length > 500",
                            timeout=15000)
                    except:
                        await page.wait_for_timeout(8000)

                    # Expand document sections
                    try: await page.get_by_text("מסמכי התכנית", exact=False).first.click(timeout=5000)
                    except: pass
                    await page.wait_for_timeout(1500)

                    eye_visible = False
                    for section in ["מסמכים מאושרים", "מסמכים מופקדים", "מסמכים בתהליך"]:
                        try: await page.get_by_text(section).first.click(timeout=3000)
                        except: continue
                        await page.wait_for_timeout(1000)
                        try: await page.locator("span").filter(has_text="הוראות").first.click(timeout=3000)
                        except: pass
                        await page.wait_for_timeout(1000)
                        eye_visible = await page.evaluate(
                            '() => Array.from(document.querySelectorAll("img[src*=\\"pdf-view\\"]")).some(i => i.offsetParent !== null)')
                        if eye_visible: break

                    if eye_visible:
                        try:
                            async with page.expect_popup(timeout=15000) as popup_info:
                                await page.locator('img[src*="pdf-view"]').first.click(timeout=5000)
                            popup = await popup_info.value
                            await popup.wait_for_load_state()
                            if popup.url.startswith('blob:'):
                                pdf_bytes = await popup.evaluate('''async (url) => {
                                    const res = await fetch(url);
                                    const buf = await res.arrayBuffer();
                                    return Array.from(new Uint8Array(buf));
                                }''', popup.url)
                                with open(pdf_path, 'wb') as f:
                                    f.write(bytes(pdf_bytes))
                                downloaded = True
                            await popup.close()
                        except Exception as e:
                            print(f"download err: {e}", end=" ")

                except Exception as e:
                    print(f"ERROR: {str(e)[:50]}", end=" ")

                await page.wait_for_timeout(3000)

            # Extract from PDF
            if downloaded:
                data = extract_table5_from_pdf(pdf_path)
                tc = sum(d["commerce"] for d in data.values())
                te = sum(d["employment"] for d in data.values())
                tp = sum(d["public"] for d in data.values())
                results[pn] = {
                    "status": "success" if data else "no_table5",
                    "total_commerce": tc, "total_employment": te, "total_public": tp,
                    "parcels_detail": data,
                }
                parts = []
                if tc: parts.append(f"commerce={tc:.0f}")
                if te: parts.append(f"emp={te:.0f}")
                if tp: parts.append(f"public={tp:.0f}")
                print(" | ".join(parts) if parts else "no data in table5")
            else:
                results[pn] = {"status": "no_pdf", "total_commerce": 0, "total_employment": 0, "total_public": 0, "parcels_detail": {}}
                print("no PDF found")

        await browser.close()
    return results


# ── Step 3: XPLAN parcels ──
def fetch_and_merge_parcels(plans):
    """Fetch XPLAN parcels for each plan and merge into future_shavaz.geojson."""
    with open(PLANS_GEOJSON, encoding='utf-8') as f:
        plans_gj = json.load(f)
    plans_index = {}
    for feat in plans_gj['features']:
        pn = feat['properties'].get('plan_name', '')
        plans_index[pn] = feat['properties']

    # Load existing shavaz
    with open(SHAVAZ_GEOJSON, encoding='utf-8') as f:
        shavaz = json.load(f)
    existing_keys = set()
    for feat in shavaz['features']:
        p = feat['properties']
        key = f"{p.get('TABA')}_{p.get('MIGRASH')}"
        existing_keys.add(key)

    new_feats = []
    total_parcels = 0

    for plan in plans:
        pn = plan['plan_name']
        taba = plan['taba']
        print(f"  XPLAN {pn}...", end=" ", flush=True)

        feats = fetch_xplan_parcels(pn)
        plan_info = plans_index.get(pn, {})
        added = 0
        public_added = 0

        for feat in feats:
            xp = feat['properties']
            migrash = str(xp.get('num', ''))
            key = f"{taba}_{migrash}"
            mavat_code = xp.get('mavat_code', 0)

            # Only add public parcels that don't already exist
            if mavat_code in PUBLIC_CODES and key not in existing_keys:
                shavaz_feat = xplan_feat_to_shavaz(feat, taba, plan_info)
                new_feats.append(shavaz_feat)
                existing_keys.add(key)
                public_added += 1
            added += 1

        total_parcels += added
        print(f"{added} parcels ({public_added} new public)")

    # Merge new public parcels
    if new_feats:
        shavaz['features'].extend(new_feats)
        with open(SHAVAZ_GEOJSON, 'w', encoding='utf-8') as f:
            json.dump(shavaz, f, ensure_ascii=False, indent=2)
        print(f"\n  Merged {len(new_feats)} new public parcels into future_shavaz.geojson")

    return total_parcels, len(new_feats)


# ── Step 4: Update Google Sheets ──
def update_sheets(plans, table5_results, creds):
    """Update GS with Table 5 data."""
    EXCLUDE_KW = ['מגורים', 'ןוינח', 'חניון', 'מסחר', 'רחסמ', 'תעסוקה', 'הקוסעת', 'משרד', 'דרשמ']

    sheet = gspread.authorize(creds).open_by_key(SHEET_ID).sheet1
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    batch = []

    def fmt(val):
        if val <= 0: return ""
        return str(int(val)) if val == int(val) else f"{val:.1f}"

    for plan in plans:
        pn = plan['plan_name']
        row = plan['row']
        r = table5_results.get(pn)
        if not r or r['status'] != 'success': continue

        # Aggregate public data
        pub_standalone = pub_hafrash = 0
        standalone_uses = []
        hafrash_uses = []
        for pk, pd in r.get('parcels_detail', {}).items():
            if not isinstance(pd, dict) or pk.startswith('_'): continue
            pub_standalone += pd.get('public_standalone', 0)
            pub_hafrash += pd.get('public_hafrash', 0)
            for use_str in pd.get('uses', []):
                name = use_str.rsplit('(', 1)[0].strip() if '(' in use_str else use_str
                if any(kw in name for kw in EXCLUDE_KW): continue
                if not name or len(name) <= 2: continue
                if pd.get('public_standalone', 0) > 0:
                    standalone_uses.append(use_str)
                elif pd.get('public_hafrash', 0) > 0:
                    hafrash_uses.append(use_str)

        changed = False
        def add(col, val):
            nonlocal changed
            if val:
                batch.append({'range': gspread.utils.rowcol_to_a1(row, col), 'values': [[str(val)]]})
                changed = True

        add(COL_COMMERCE_OUT, fmt(r['total_commerce']))
        add(COL_EMPLOYMENT, fmt(r['total_employment']))
        add(COL_PUBLIC, fmt(pub_standalone))
        add(COL_PUBLIC_DETAIL, '; '.join(standalone_uses) if standalone_uses else '')
        add(COL_HAFRASH, fmt(pub_hafrash))
        add(COL_HAFRASH_DETAIL, '; '.join(hafrash_uses) if hafrash_uses else '')
        if changed:
            add(COL_LAST_MOD, now)

    if batch:
        for i in range(0, len(batch), 50):
            sheet.spreadsheet.values_batch_update({'valueInputOption': 'RAW', 'data': batch[i:i+50]})
    return len([p for p in plans if table5_results.get(p['plan_name'], {}).get('status') == 'success'])


# ── Main ──
async def main():
    sys.stdout.reconfigure(encoding='utf-8')
    dry_run = '--dry' in sys.argv
    no_pdf = '--no-pdf' in sys.argv
    limit = None
    for arg in sys.argv[1:]:
        if arg.isdigit():
            limit = int(arg)

    # Step 1: Identify
    print("=" * 60)
    print("Step 1: Identifying plans in בדיקה תכנונית without שבצ...")
    print("=" * 60)
    plans, sheet, all_data, h, creds = identify_plans()
    print(f"Found {len(plans)} plans needing processing:")
    for p in plans:
        print(f"  {p['plan_name']} (taba={p['taba']}) [{p['status']}]")

    if limit:
        plans = plans[:limit]
        print(f"\n(limited to {limit} plans)")

    if not plans:
        print("\nNo plans to process!")
        return

    if dry_run:
        print("\n(dry run — stopping here)")
        return

    # Step 2: Table 5
    table5_results = {}
    if no_pdf:
        print(f"\n{'=' * 60}")
        print("Step 2: Skipping PDF download (--no-pdf)")
        print("=" * 60)
        # Try to extract from existing PDFs in temp_pdfs/
        for plan in plans:
            pdf_path = os.path.join(TEMP_DIR, f"{plan['taba']}.pdf")
            if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 1000:
                data = extract_table5_from_pdf(pdf_path)
                tc = sum(d["commerce"] for d in data.values())
                te = sum(d["employment"] for d in data.values())
                tp = sum(d["public"] for d in data.values())
                table5_results[plan['plan_name']] = {
                    "status": "success" if data else "no_table5",
                    "total_commerce": tc, "total_employment": te, "total_public": tp,
                    "parcels_detail": data,
                }
                if data: print(f"  {plan['plan_name']}: existing PDF extracted")
        print(f"  Extracted from {sum(1 for r in table5_results.values() if r['status']=='success')} existing PDFs")
    else:
        print(f"\n{'=' * 60}")
        print("Step 2: Downloading PDFs + extracting Table 5...")
        print("=" * 60)
        table5_results = await download_and_extract(plans)

    # Step 3: XPLAN parcels
    print(f"\n{'=' * 60}")
    print("Step 3: Fetching XPLAN parcels...")
    print("=" * 60)
    total_parcels, new_public = fetch_and_merge_parcels(plans)

    # Step 4: Update Sheets
    print(f"\n{'=' * 60}")
    print("Step 4: Updating Google Sheets...")
    print("=" * 60)
    updated = update_sheets(plans, table5_results, creds)
    print(f"  Updated {updated} rows")

    # Summary
    success = sum(1 for r in table5_results.values() if r['status'] == 'success')
    print(f"\n{'=' * 60}")
    print(f"Done! Plans: {len(plans)} | Table5 success: {success} | XPLAN parcels: {total_parcels} | New public: {new_public}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
