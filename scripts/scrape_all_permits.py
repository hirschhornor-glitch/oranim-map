"""
scrape_all_permits.py
---------------------
Scrapes ALL building permits for each approved plan from the Jerusalem
municipality permit system (ykpubdata.jerusalem.muni.il).

Unlike check_permits_new_plans.py which only extracts one permit per plan,
this script collects every permit row from the results table.

Output: all_permits.json  (taba -> array of permits)
"""
import asyncio
import json
import sys
import os
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials
from playwright.async_api import async_playwright

CREDS_FILE   = r"C:\ORANIM\oranim-490018-ceaf784afe61.json"
SHEET_ID     = "1_AcuuA1CNPh6jXc_lZKNghfpEF1aDPV8Zci8QPz2WVE"
JSON_PATH    = r"C:\ORANIM\all_permits.json"
PLANS_GEOJSON = r"C:\ORANIM\oranim-app\data\plans.geojson"
BROWSER_DATA = r"C:\ORANIM\.browser_data_jlm"

SITE_URL = "https://ykpubdata.jerusalem.muni.il/#/?SystemCode=26400046"
APPROVED = {'אישור', 'מאושרת', 'תבע מאושרת', 'תחילת תוקף', 'בהליך אישור'}
DELAY = 3

# Sanity-check thresholds: flag any plan that exceeds UNITS_MIN units_total
# but whose permits scrape returned fewer than PERMITS_MIN rows.
# Skip plans approved less than MIN_AGE_YEARS ago — permits typically take
# a few years to appear after approval, so recent approvals aren't evidence
# of a scraping bug. Plans missing mavat_date are still flagged.
SANITY_UNITS_MIN = 100
SANITY_PERMITS_MIN = 5
SANITY_MIN_AGE_YEARS = 3

# JS that extracts ALL rows from the permits table.
# Handles Kendo UI card-style layout where each <td> contains label+value.
EXTRACT_ALL_JS = r"""() => {
    const permits = [];
    const LABELS = ['מספר תיק', 'תאריך סטטוס', 'סטטוס', 'סוג בקשה', 'מהות הבקשה'];
    const FIELD_MAP = {
        'מספר תיק': 'file_number',
        'סטטוס': 'status',
        'תאריך סטטוס': 'status_date',
        'סוג בקשה': 'request_type',
        'מהות הבקשה': 'request_description'
    };

    function stripLabel(text) {
        // Sort labels longest-first so "תאריך סטטוס" matches before "סטטוס"
        const sorted = LABELS.slice().sort((a,b) => b.length - a.length);
        for (const lbl of sorted) {
            if (text.startsWith(lbl)) return text.substring(lbl.length).trim();
        }
        return text.trim();
    }

    function extractFromCell(cell) {
        // Try to get value from a link first (file_number is often a link)
        const link = cell.querySelector('a');
        if (link) {
            const linkText = link.textContent.trim();
            if (/\d{4}\/\d{3,5}/.test(linkText)) return linkText;
        }
        return stripLabel(cell.textContent.trim());
    }

    try {
        const tables = document.querySelectorAll('table');
        for (const table of tables) {
            const rows = table.querySelectorAll('tr');
            if (rows.length < 1) continue;

            // Detect if this is a permits table by checking first data row
            const firstRowText = rows[0].textContent || '';
            if (!firstRowText.includes('מספר תיק') && !firstRowText.includes('סטטוס')) continue;

            // Determine column mapping from first row's cells
            const sampleCells = Array.from(rows[0].querySelectorAll('td, th'));
            if (sampleCells.length < 3) continue;

            const colMap = {};
            const sorted = LABELS.slice().sort((a,b) => b.length - a.length);
            sampleCells.forEach((cell, i) => {
                const text = cell.textContent.trim();
                for (const lbl of sorted) {
                    if (text.includes(lbl) && !colMap[FIELD_MAP[lbl]]) {
                        colMap[FIELD_MAP[lbl]] = i;
                        break;
                    }
                }
            });

            if (colMap.file_number === undefined) continue;

            // Extract all rows (including row 0 since there's no separate header)
            for (let r = 0; r < rows.length; r++) {
                const cells = Array.from(rows[r].querySelectorAll('td'));
                if (cells.length < 3) continue;

                const permit = {
                    file_number: '',
                    status: '',
                    status_date: '',
                    request_type: '',
                    request_description: ''
                };

                for (const [field, idx] of Object.entries(colMap)) {
                    if (idx < cells.length) {
                        permit[field] = extractFromCell(cells[idx]);
                    }
                }

                // Validate: must have a file_number pattern
                if (/\d{4}\/\d{3,5}/.test(permit.file_number)) {
                    permits.push(permit);
                }
            }

            if (permits.length > 0) break;
        }

        // Fallback: detail page (single permit, no table)
        if (permits.length === 0) {
            let fileNumber = '', statusDate = '', statusDesc = '', reqType = '', reqDesc = '';
            const allText = document.body.innerText || '';
            const tikMatch = allText.match(/(\d{4}\/\d{3,5}\.\d{2})/);
            if (tikMatch) fileNumber = tikMatch[1];

            const els = Array.from(document.querySelectorAll('div, span, td'));
            for (let j = 0; j < els.length; j++) {
                const text = els[j].textContent.trim();
                const sib = els[j].nextElementSibling;
                if (!sib) continue;
                const sibText = sib.textContent.trim();
                if ((text === 'מספר תיק' || text.includes('מספר התיק')) && sibText)
                    fileNumber = sibText;
                if ((text === 'תאריך סטטוס' || text.includes('תאריך סטטוס')) && sibText) {
                    const dm = sibText.match(/\d{2}\/\d{2}\/\d{4}/);
                    if (dm) statusDate = dm[0];
                }
                if ((text === 'סטטוס' || text === 'תיאור סטטוס') && sibText.length > 1 && sibText.length < 100)
                    statusDesc = sibText;
                if (text === 'סוג בקשה' && sibText.length > 1 && sibText.length < 100)
                    reqType = sibText;
                if ((text === 'מהות הבקשה' || text.includes('מהות')) && sibText.length > 1)
                    reqDesc = sibText;
            }
            if (!fileNumber) {
                const links = Array.from(document.querySelectorAll('a'));
                for (const link of links) {
                    const m = (link.href||'').match(/TikNum=([^&]+)/);
                    if (m) { fileNumber = decodeURIComponent(m[1]); break; }
                }
            }
            if (fileNumber && /\d{4}\/\d{3,5}/.test(fileNumber)) {
                permits.push({
                    file_number: fileNumber,
                    status: statusDesc,
                    status_date: statusDate,
                    request_type: reqType,
                    request_description: reqDesc
                });
            }
        }
    } catch(e) {}
    return permits;
}"""


def get_approved_tabas():
    """Read approved taba list from Google Sheets (column C=taba, D=status_mavat)."""
    creds = Credentials.from_service_account_file(CREDS_FILE,
        scopes=["https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"])
    sheet = gspread.authorize(creds).open_by_key(SHEET_ID).sheet1
    all_data = sheet.get_all_values()
    headers = all_data[0]
    h = {hdr.strip().lower(): i for i, hdr in enumerate(headers)}

    tabas = []
    seen = set()
    for row in all_data[1:]:
        taba = row[h['taba']].strip()
        status = row[h['status_mavat']].strip()
        if not taba or taba in seen:
            continue
        if status not in APPROVED:
            continue
        seen.add(taba)
        tabas.append(taba)
    return tabas


def load_progress():
    if os.path.exists(JSON_PATH):
        with open(JSON_PATH, encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_progress(data):
    with open(JSON_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_plan_meta(geojson_path: str = PLANS_GEOJSON) -> dict:
    """Return {taba_str: {'units': int, 'mavat_date': 'dd/mm/yyyy'|None}} from plans.geojson."""
    if not os.path.exists(geojson_path):
        return {}
    with open(geojson_path, encoding='utf-8') as f:
        gj = json.load(f)
    meta = {}
    for feat in gj.get('features', []):
        props = feat.get('properties') or {}
        taba = props.get('taba')
        if taba is None:
            continue
        ut = props.get('units_total')
        try:
            units = int(float(ut)) if ut not in (None, '') else None
        except (ValueError, TypeError):
            units = None
        meta[str(taba).strip()] = {
            'units': units,
            'mavat_date': props.get('mavat_date'),
        }
    return meta


def _parse_date(s):
    if not s:
        return None
    for fmt in ('%d/%m/%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def run_sanity_check(permits_data: dict | None = None,
                     plan_meta: dict | None = None,
                     log_path: str | None = None) -> list:
    """
    Flag large plans (units_total > SANITY_UNITS_MIN) whose scrape returned
    fewer than SANITY_PERMITS_MIN permits. Skip plans approved less than
    SANITY_MIN_AGE_YEARS ago; plans missing mavat_date are still flagged.

    Returns list of dicts sorted by units_total desc.
    """
    if permits_data is None:
        permits_data = load_progress()
    if plan_meta is None:
        plan_meta = load_plan_meta()

    today = datetime.now().date()
    min_age_days = SANITY_MIN_AGE_YEARS * 365.25

    suspicious = []
    for taba, entry in permits_data.items():
        meta = plan_meta.get(str(taba), {})
        units = meta.get('units')
        if units is None or units <= SANITY_UNITS_MIN:
            continue
        count = entry.get('permit_count', 0) or 0
        if count >= SANITY_PERMITS_MIN:
            continue

        md = _parse_date(meta.get('mavat_date'))
        age_years = None
        if md is not None:
            age_days = (today - md).days
            if age_days < min_age_days:
                continue
            age_years = round(age_days / 365.25, 1)

        suspicious.append({
            'taba': str(taba),
            'units_total': units,
            'permit_count': count,
            'age_years': age_years,
            'mavat_date': meta.get('mavat_date') or '',
            'error': entry.get('error', '') or '',
        })
    suspicious.sort(key=lambda x: -x['units_total'])

    lines = []
    lines.append(
        f"\n=== SANITY CHECK "
        f"(units_total > {SANITY_UNITS_MIN}, permit_count < {SANITY_PERMITS_MIN}, "
        f"mavat age >= {SANITY_MIN_AGE_YEARS}y or missing) ==="
    )
    if not suspicious:
        lines.append("OK - no suspicious plans.")
    else:
        lines.append(f"Flagged {len(suspicious)} plans:")
        lines.append(f"  {'TABA':<10} {'UNITS':>6}  {'PERMITS':>7}  {'AGE':>5}  {'MAVAT':<11}  ERROR")
        for row in suspicious:
            age = f"{row['age_years']}y" if row['age_years'] is not None else "?"
            lines.append(
                f"  {row['taba']:<10} {row['units_total']:>6}  "
                f"{row['permit_count']:>7}  {age:>5}  {row['mavat_date']:<11}  {row['error']}"
            )
    out = "\n".join(lines)
    print(out)

    if log_path:
        try:
            with open(log_path, 'a', encoding='utf-8') as f:
                f.write(f"\n[{datetime.now().isoformat()}]" + out + "\n")
        except OSError as e:
            print(f"(warning: could not write sanity log to {log_path}: {e})")

    return suspicious


async def search_taba(page, ctx, taba: str) -> tuple:
    """
    Navigate to search page, enter taba, click suggestion, click search.
    Returns (status_str, active_page).
    status_str: 'ok', 'no_input', 'no_suggestion', 'no_button'
    """
    try:
        await page.goto(SITE_URL, wait_until="networkidle", timeout=20000)
    except:
        pass
    await asyncio.sleep(3)

    # Find תב"ע input (last combobox)
    inputs = await page.query_selector_all("input.k-input-inner")
    if not inputs:
        return ("no_input", page)

    taba_input = inputs[-1]
    await taba_input.click()
    await taba_input.fill("")
    await asyncio.sleep(0.5)
    await taba_input.type(str(taba), delay=100)
    await asyncio.sleep(2)

    # Select suggestion from dropdown
    try:
        suggestion = await page.wait_for_selector("li.k-list-item", timeout=5000)
        if suggestion:
            await suggestion.click()
            await asyncio.sleep(1)
    except:
        return ("no_suggestion", page)

    # Click search button
    search_btn = await page.query_selector('button.search-btn') or \
                 await page.query_selector('button:has-text("אתר תיק רישוי")')
    if not search_btn:
        return ("no_button", page)

    await search_btn.click()
    await asyncio.sleep(3)

    # Handle potential new tab
    active_page = ctx.pages[-1] if len(ctx.pages) > 1 else page
    await asyncio.sleep(10)  # Wait for Angular SPA to render table

    return ("ok", active_page)


async def main():
    sys.stdout.reconfigure(encoding='utf-8')

    # Step 1: Get approved tabas from Google Sheets
    print("Reading approved plans from Google Sheets...")
    tabas = get_approved_tabas()
    print(f"Total approved plans: {len(tabas)}")

    # Step 2: Load progress
    results = load_progress()
    remaining = [t for t in tabas if t not in results or not results[t].get('scraped_at')]
    print(f"Already scraped: {len(tabas) - len(remaining)}, Remaining: {len(remaining)}")

    if not remaining:
        print("All done!")
        return

    # Step 3: Launch browser and scrape
    async with async_playwright() as p:
        from yk_profile_lock import hold_yk_profile
        hold_yk_profile("scrape_all_permits.py")  # one YK-profile user at a time
        ctx = await p.chromium.launch_persistent_context(
            BROWSER_DATA, headless=False,
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        print("Loading Jerusalem permit site...")
        try:
            await page.goto(SITE_URL, wait_until="networkidle", timeout=30000)
        except:
            pass
        await asyncio.sleep(5)

        stats = {'success': 0, 'empty': 0, 'error': 0}

        for i, taba in enumerate(remaining):
            print(f"[{i+1}/{len(remaining)}] TABA {taba}...", end=" ", flush=True)

            try:
                status, active_page = await search_taba(page, ctx, taba)

                if status != "ok":
                    results[taba] = {
                        "permits": [],
                        "scraped_at": datetime.now().isoformat(),
                        "permit_count": 0,
                        "error": status
                    }
                    print(status)
                    if status == "no_suggestion":
                        stats['empty'] += 1
                    else:
                        stats['error'] += 1
                else:
                    permits = await active_page.evaluate(EXTRACT_ALL_JS)

                    results[taba] = {
                        "permits": permits,
                        "scraped_at": datetime.now().isoformat(),
                        "permit_count": len(permits),
                        "error": ""
                    }

                    if permits:
                        print(f"{len(permits)} permits found")
                        stats['success'] += 1
                    else:
                        print("no permits in table")
                        stats['empty'] += 1

            except Exception as e:
                results[taba] = {
                    "permits": [],
                    "scraped_at": None,  # mark for retry
                    "permit_count": 0,
                    "error": str(e)[:100]
                }
                print(f"ERROR: {str(e)[:50]}")
                stats['error'] += 1

            # Close extra tabs to avoid accumulation
            while len(ctx.pages) > 1:
                await ctx.pages[-1].close()
            page = ctx.pages[0]

            # Save every 5 iterations
            if (i + 1) % 5 == 0:
                save_progress(results)

            await asyncio.sleep(DELAY)

        save_progress(results)
        await ctx.close()

    print(f"\nDone! With permits: {stats['success']}, Empty: {stats['empty']}, Errors: {stats['error']}")

    # Sanity check: flag large plans with suspiciously few permits
    run_sanity_check(
        permits_data=results,
        log_path=os.path.join(os.path.dirname(JSON_PATH), 'all_permits_sanity.log'),
    )


if __name__ == "__main__":
    if '--sanity-check' in sys.argv:
        sys.stdout.reconfigure(encoding='utf-8')
        run_sanity_check()
    else:
        asyncio.run(main())
