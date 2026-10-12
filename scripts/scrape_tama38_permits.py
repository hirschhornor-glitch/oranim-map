"""
scrape_tama38_permits.py
------------------------
Scrapes building permits for TAMA 38 projects by searching the Jerusalem
municipality permit system by address (רחוב + בית).

Reads addresses from tama38.geojson, searches by street + house number,
and collects all permit rows.

Output: tama38_permits.json  (fid -> array of permits)
"""
import asyncio
import json
import re
import sys
import os
from datetime import datetime

from playwright.async_api import async_playwright

TAMA38_GEOJSON = r"C:\ORANIM\oranim-app\data\tama38.geojson"
JSON_PATH      = r"C:\ORANIM\tama38_permits.json"
BROWSER_DATA   = r"C:\ORANIM\.browser_data_jlm"

SITE_URL = "https://ykpubdata.jerusalem.muni.il/#/?SystemCode=26400046"
DELAY = 3

# Same JS extraction as scrape_all_permits.py
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
        const sorted = LABELS.slice().sort((a,b) => b.length - a.length);
        for (const lbl of sorted) {
            if (text.startsWith(lbl)) return text.substring(lbl.length).trim();
        }
        return text.trim();
    }

    function extractFromCell(cell) {
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

            const firstRowText = rows[0].textContent || '';
            if (!firstRowText.includes('מספר תיק') && !firstRowText.includes('סטטוס')) continue;

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

                if (/\d{4}\/\d{3,5}/.test(permit.file_number)) {
                    permits.push(permit);
                }
            }

            if (permits.length > 0) break;
        }

        // Fallback: detail page (single permit)
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


def parse_address(address: str) -> tuple:
    """Parse 'שמשון 14' into ('שמשון', '14'). Returns (street, house_num).

    Handles variants:
      - 'דוד שמעוני29' (no space before number)
      - '43 ניקנור' (number before street)
    """
    if not address:
        return ('', '')
    address = address.strip()
    # Remove control characters (e.g. \x08)
    address = re.sub(r'[\x00-\x1f]', '', address)
    if not address:
        return ('', '')

    # Try: street at start, optional whitespace, number at end
    m = re.match(r'^(.+?)\s*(\d+[א-ת]?)\s*$', address)
    if m and re.search(r'[\u0590-\u05FF]', m.group(1)):
        street, house = m.group(1).strip(), m.group(2).strip()
    else:
        # Try: number at start, then street
        m2 = re.match(r'^(\d+[א-ת]?)\s+(.+)$', address)
        if m2:
            street, house = m2.group(2).strip(), m2.group(1).strip()
        else:
            street, house = address, ''

    return (street, house)


def street_variants(street: str) -> list:
    """Return candidate street-name variants to try in autocomplete."""
    variants = []
    seen = set()
    def add(v):
        v = (v or '').strip()
        if v and v not in seen:
            seen.add(v); variants.append(v)

    # 1. Original
    add(street)
    # 2. With גרשיים stripped
    s2 = street.replace('"', '').replace("'", '').replace('\u05f4', '').replace('\u05f3', '')
    add(s2)
    # 3. Reverse word order (handles "שמעוני דוד" ↔ "דוד שמעוני")
    words = street.split()
    if len(words) == 2:
        add(' '.join(reversed(words)))
    # 4. Strip common prefixes ("הרב", "ככר", "כיכר")
    for prefix in ('הרב', 'ככר', 'כיכר'):
        if street.startswith(prefix + ' '):
            add(street[len(prefix) + 1:])
    # 5. Try just the last word (last-name only, e.g. "בנימין זאב בכר" → "בכר")
    if len(words) >= 3:
        add(words[-1])
    return variants


def get_tama38_addresses():
    """Read tama38.geojson and return list of dicts with fid, tik, address, street, house."""
    with open(TAMA38_GEOJSON, encoding='utf-8') as f:
        data = json.load(f)

    items = []
    for feat in data['features']:
        p = feat['properties']
        fid = str(p.get('fid', ''))
        tik = p.get('tik') or ''
        address = p.get('address') or ''
        street, house = parse_address(address)
        if street:
            items.append({
                'fid': fid,
                'tik': tik,
                'address': address,
                'street': street,
                'house': house,
            })
    return items


def load_progress():
    if os.path.exists(JSON_PATH):
        with open(JSON_PATH, encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_progress(data):
    with open(JSON_PATH, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


async def search_by_address(page, ctx, street: str, house: str) -> tuple:
    """
    Search the permit site by street + house number. Tries multiple street-name
    variants when the first one has no autocomplete suggestion.
    Returns (status_str, active_page).

    Form inputs (all visible):
      0: מספר תיק (k-input, text, placeholder xxxx.xx)
      1: שנה      (k-input, text, placeholder xxxx)
      2: גוש      (k-input-inner, combobox)
      3: חלקה     (k-input-inner, combobox)
      4: רחוב     (k-input-inner, combobox)
      5: בית      (k-input, type=number)  <-- NOT a combobox!
      6: תב"ע     (k-input-inner, combobox)
      7: תא שטח   (k-input, type=number)
    """
    try:
        await page.goto(SITE_URL, wait_until="networkidle", timeout=20000)
    except:
        pass
    await asyncio.sleep(3)

    # Get all visible inputs (both combobox and regular)
    all_inputs = await page.query_selector_all("input:visible")
    if len(all_inputs) < 6:
        return ("no_input", page)

    # רחוב input (index 4 among all inputs)
    street_input = all_inputs[4]

    # Try each street variant until one gets an autocomplete suggestion
    variants = street_variants(street)
    suggestion = None
    for variant in variants:
        await street_input.click()
        await street_input.fill("")
        await asyncio.sleep(0.3)
        await street_input.type(variant, delay=80)
        await asyncio.sleep(1.5)
        try:
            suggestion = await page.wait_for_selector("li.k-list-item", timeout=3000)
            if suggestion:
                await suggestion.click()
                await asyncio.sleep(1)
                break
        except:
            suggestion = None
            continue

    if not suggestion:
        return ("no_street_suggestion", page)

    # בית input (index 5 among all inputs, type=number, NOT a combobox)
    if house:
        # Extract only digits from house number (field is type=number)
        house_digits = re.sub(r'[^\d]', '', house)
        if house_digits:
            house_input = all_inputs[5]
            await house_input.click()
            await house_input.fill(house_digits)
            await asyncio.sleep(0.5)

    # Click search button
    search_btn = await page.query_selector('button.search-btn') or \
                 await page.query_selector('button:has-text("אתר תיק רישוי")')
    if not search_btn:
        return ("no_button", page)

    await search_btn.click()
    await asyncio.sleep(3)

    # Handle potential new tab
    active_page = ctx.pages[-1] if len(ctx.pages) > 1 else page
    await asyncio.sleep(10)

    return ("ok", active_page)


async def search_by_tik(page, ctx, tik: str) -> tuple:
    """
    Fallback: search by מספר תיק + שנה (inputs 0 and 1).
    tik format: '2013/81' or '2014/0609' or '1971/1425.03'
    """
    if not tik or '/' not in tik:
        return ("no_tik", page)

    try:
        await page.goto(SITE_URL, wait_until="networkidle", timeout=20000)
    except:
        pass
    await asyncio.sleep(3)

    all_inputs = await page.query_selector_all("input:visible")
    if len(all_inputs) < 2:
        return ("no_input", page)

    # Parse tik: "2013/81" -> year=2013, num=81
    # "1971/1425.03" -> year=1971, num=1425.03
    parts = tik.split('/')
    year = parts[0].strip()
    num = parts[1].strip()

    # Input 0: מספר תיק (format xxxx.xx)
    tik_input = all_inputs[0]
    await tik_input.click()
    await tik_input.fill(num)
    await asyncio.sleep(0.5)

    # Input 1: שנה (format xxxx)
    year_input = all_inputs[1]
    await year_input.click()
    await year_input.fill(year)
    await asyncio.sleep(0.5)

    # Click search button
    search_btn = await page.query_selector('button.search-btn') or \
                 await page.query_selector('button:has-text("אתר תיק רישוי")')
    if not search_btn:
        return ("no_button", page)

    await search_btn.click()
    await asyncio.sleep(3)

    active_page = ctx.pages[-1] if len(ctx.pages) > 1 else page
    await asyncio.sleep(10)

    return ("ok", active_page)


async def main():
    sys.stdout.reconfigure(encoding='utf-8')

    # Step 1: Get tama38 addresses
    print("Reading tama38.geojson...")
    items = get_tama38_addresses()
    print(f"Total tama38 features with address: {len(items)}")

    # Step 2: Load progress
    results = load_progress()
    remaining = [it for it in items if it['fid'] not in results or not results[it['fid']].get('scraped_at')]
    print(f"Already scraped: {len(items) - len(remaining)}, Remaining: {len(remaining)}")

    if not remaining:
        print("All done!")
        return

    # Step 3: Launch browser and scrape
    async with async_playwright() as p:
        from yk_profile_lock import hold_yk_profile
        hold_yk_profile("scrape_tama38_permits.py")  # one YK-profile user at a time
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

        for i, item in enumerate(remaining):
            fid = item['fid']
            print(f"[{i+1}/{len(remaining)}] fid={fid} {item['address']}...", end=" ", flush=True)

            try:
                # Prefer tik-based search when tik is available (unique, reliable).
                # Address search has ambiguity (short street names match multiple streets).
                if item['tik'] and '/' in item['tik']:
                    status, active_page = await search_by_tik(page, ctx, item['tik'])
                    # Fallback: if tik search fails, try address
                    if status != "ok":
                        print(f"tik failed ({status}), trying address...", end=" ", flush=True)
                        while len(ctx.pages) > 1:
                            await ctx.pages[-1].close()
                        page = ctx.pages[0]
                        status, active_page = await search_by_address(page, ctx, item['street'], item['house'])
                else:
                    # No tik: address-based only
                    status, active_page = await search_by_address(page, ctx, item['street'], item['house'])

                if status != "ok":
                    results[fid] = {
                        "tik": item['tik'],
                        "address": item['address'],
                        "permits": [],
                        "scraped_at": datetime.now().isoformat(),
                        "permit_count": 0,
                        "error": status
                    }
                    print(status)
                    if status in ("no_street_suggestion", "no_tik"):
                        stats['empty'] += 1
                    else:
                        stats['error'] += 1
                else:
                    permits = await active_page.evaluate(EXTRACT_ALL_JS)

                    results[fid] = {
                        "tik": item['tik'],
                        "address": item['address'],
                        "permits": permits,
                        "scraped_at": datetime.now().isoformat(),
                        "permit_count": len(permits),
                        "error": ""
                    }

                    if permits:
                        print(f"{len(permits)} permits found")
                        stats['success'] += 1
                    else:
                        print("no permits")
                        stats['empty'] += 1

            except Exception as e:
                results[fid] = {
                    "tik": item['tik'],
                    "address": item['address'],
                    "permits": [],
                    "scraped_at": None,
                    "permit_count": 0,
                    "error": str(e)[:100]
                }
                print(f"ERROR: {str(e)[:50]}")
                stats['error'] += 1

            # Close extra tabs
            while len(ctx.pages) > 1:
                await ctx.pages[-1].close()
            page = ctx.pages[0]

            if (i + 1) % 5 == 0:
                save_progress(results)

            await asyncio.sleep(DELAY)

        save_progress(results)
        await ctx.close()

    print(f"\nDone! With permits: {stats['success']}, Empty: {stats['empty']}, Errors: {stats['error']}")


if __name__ == "__main__":
    asyncio.run(main())
