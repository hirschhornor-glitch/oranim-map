"""
enrich_permits.py
-----------------
Step 2: Enrich permit data with:
  - units (יח"ד) — extracted from description (Layer 1) or detail page (Layer 2)
  - tama_type — "38/1" or "38/2" classification (tama38 only)
  - filtered — True if permit is just balcony/closure (not interesting).
                Only with --with-filter: `filtered` HIDES the permit from every
                view in the app, so re-stamping it is an explicit decision, not a
                side effect of the (safe, additive) units enrichment.

Layer 1: regex on existing request_description (no scraping needed)
Layer 2: Playwright scrape of detail pages for permits still missing units

Usage:
  python enrich_permits.py           # Layer 1 only (fast, no browser)
  python enrich_permits.py --layer2  # Layer 1 + Layer 2 (slow, browser)
"""
import asyncio
import json
import re
import sys
import os
from datetime import datetime

ALL_PERMITS_PATH   = r"C:\ORANIM\all_permits.json"
TAMA38_PERMITS_PATH = r"C:\ORANIM\tama38_permits.json"
# --data → enrich the PUBLISHED copies (oranim-app/data) instead of the live root
# ones. The bi-weekly pipeline enriches the root copy between the merge and the
# publish step; --data is for back-filling a copy that was published un-enriched.
if "--data" in sys.argv:
    ALL_PERMITS_PATH    = r"C:\ORANIM\oranim-app\data\all_permits.json"
    TAMA38_PERMITS_PATH = r"C:\ORANIM\oranim-app\data\tama38_permits.json"
BROWSER_DATA       = r"C:\ORANIM\.browser_data_jlm"
SITE_URL           = "https://ykpubdata.jerusalem.muni.il/#/?SystemCode=26400046"
DELAY = 3
APPLY_FILTER = "--with-filter" in sys.argv
# --with-tama-type re-stamps `tama_type` (38/1 or 38/2) on every permit in the תמ"א 38
# store. Off by default on purpose: the store holds EVERY permit of a registered
# building, and classify_tama38 keys off wide stems ("תוספת בני"), so stamping it
# marks ordinary additions as תמ"א 38 — build_permits_master takes is_tama38 =
# bool(tama_type), and the bucket jumped 472 -> 621 when this ran unguarded.
APPLY_TAMA_TYPE = "--with-tama-type" in sys.argv


# ─── Layer 1: Regex-based extraction ──────────────────────────────────────────

def extract_units_from_text(text: str) -> int | None:
    """Extract total housing units (יח"ד) from text. Returns int or None."""
    if not text:
        return None

    # Patterns to find units — ordered from most specific to least
    patterns = [
        # "סה"כ 210 יח"ד"
        r'סה"?כ\s+(\d+)\s*יח[\'""]?ד',
        # "210 יח"ד" or "210 יחידות דיור"
        r'(\d+)\s*יח[\'""]?ד',
        r'(\d+)\s*יחידות\s*דיור',
        # "X דירות"
        r'(\d+)\s*דירות',
    ]

    best = None
    for pat in patterns:
        matches = re.findall(pat, text)
        if matches:
            # Take the largest number found (usually the total)
            nums = [int(m) for m in matches]
            candidate = max(nums)
            if best is None or candidate > best:
                best = candidate

    return best


def classify_tama38(permit: dict) -> str | None:
    """Classify a tama38 permit as '38/1', '38/2', or None (unknown).

    38/2: הריסה ובנייה מחדש (demolition and rebuild)
    38/1: חיזוק ותוספת (strengthening and addition)
    """
    desc = (permit.get('request_description', '') + ' ' +
            permit.get('request_type', '')).lower()

    # 38/2 keywords (demolition & rebuild)
    if any(kw in desc for kw in [
        'הריסה ובני', 'הריסה ובנ', 'הריסת מבנ', 'הריסת הבנ',
        'פינוי בינוי', 'פינוי ובינוי',
        'הקמת בניין חדש', 'הקמת מבנה חדש', 'בניין חדש במקום',
        'הריסת בנין קיים והקמ', 'הריסת בניין קיים והקמ',
        'הריסה והקמ',
    ]):
        return '38/2'

    # 38/1 keywords (strengthening & addition)
    if any(kw in desc for kw in [
        'חיזוק', 'תוספת בני', 'תוספת קומ', 'תוספות בני',
        'תוספות לחיזוק', 'תוספות לבניין',
        'מכוח תמ', 'בהתאם לתמ', 'ע"פ תמ', 'עפ"י תמ',
        'על פי תמ', 'במסגרת תמ',
    ]):
        return '38/1'

    return None


def is_balcony_only(permit: dict) -> bool:
    """Check if permit is just a balcony addition/closure (not interesting)."""
    desc = permit.get('request_description', '')
    req_type = permit.get('request_type', '')
    combined = desc + ' ' + req_type

    balcony_keywords = [
        'תוספת מרפסת', 'סגירת מרפסת', 'בניית מרפסת',
        'הוספת מרפסת', 'סגירת מרפסות',
    ]

    # Only filter if it's JUST a balcony/closure — not part of a bigger project
    is_balcony = any(kw in combined for kw in balcony_keywords)
    if not is_balcony:
        return False

    # If the request also covers something bigger, it's NOT balcony-only. The list
    # below is deliberately wide: `filtered` HIDES the permit from every view in the
    # app, so a request that merely mentions a balcony alongside real work (הרחבה
    # לבניין קיים + בניית מרפסת, תוספת מעליות ומרפסות) must stay visible.
    # 'מעלי' is a stem on purpose — it has to catch מעלית AND מעליות.
    significant_keywords = ['יח"ד', 'יחידות דיור', 'דירות', 'בניין חדש',
                            'בניה חדשה', 'הריסה', 'חיזוק', 'קומות',
                            'הרחבה', 'תוספת בניה', 'מעלי', 'ממ"ד',
                            'נגישות', 'הגבהת', 'שימוש חורג', 'מחסן']
    if any(kw in combined for kw in significant_keywords):
        return False

    return True


def enrich_layer1():
    """Layer 1: Enrich all permits using regex on existing descriptions."""
    # Process all_permits.json
    with open(ALL_PERMITS_PATH, encoding='utf-8') as f:
        all_data = json.load(f)

    all_enriched = 0
    all_filtered = 0
    for taba, entry in all_data.items():
        for permit in entry.get('permits', []):
            # Units
            if 'units' not in permit or permit['units'] is None:
                units = extract_units_from_text(permit.get('request_description', ''))
                permit['units'] = units
                permit['units_source'] = 'description' if units else None
                if units:
                    all_enriched += 1

            # Filter (opt-in — see APPLY_FILTER)
            if APPLY_FILTER:
                permit['filtered'] = is_balcony_only(permit)
                if permit['filtered']:
                    all_filtered += 1

    with open(ALL_PERMITS_PATH, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    print(f"all_permits.json: {all_enriched} permits got units from description, {all_filtered} filtered as balcony-only")

    # Process tama38_permits.json
    with open(TAMA38_PERMITS_PATH, encoding='utf-8') as f:
        tama_data = json.load(f)

    tama_enriched = 0
    tama_classified = 0
    tama_filtered = 0
    for fid, entry in tama_data.items():
        for permit in entry.get('permits', []):
            # Units
            if 'units' not in permit or permit['units'] is None:
                units = extract_units_from_text(permit.get('request_description', ''))
                permit['units'] = units
                permit['units_source'] = 'description' if units else None
                if units:
                    tama_enriched += 1

            # TAMA type (opt-in — see APPLY_TAMA_TYPE)
            if APPLY_TAMA_TYPE:
                tama_type = classify_tama38(permit)
                permit['tama_type'] = tama_type
                if tama_type:
                    tama_classified += 1

            # Filter (opt-in — see APPLY_FILTER)
            if APPLY_FILTER:
                permit['filtered'] = is_balcony_only(permit)
                if permit['filtered']:
                    tama_filtered += 1

    with open(TAMA38_PERMITS_PATH, 'w', encoding='utf-8') as f:
        json.dump(tama_data, f, ensure_ascii=False, indent=2)

    print(f"tama38_permits.json: {tama_enriched} got units, {tama_classified} classified (38/1 or 38/2), {tama_filtered} filtered")


# ─── Layer 2: Detail page scraping ───────────────────────────────────────────

EXTRACT_UNITS_JS = r"""() => {
    const text = document.body.innerText || '';
    const results = {};

    // Extract units from page text
    const patterns = [
        /סה"?כ\s+(\d+)\s*יח['""]?ד/g,
        /(\d+)\s*יח['""]?ד/g,
        /(\d+)\s*יחידות\s*דיור/g,
    ];

    let allNums = [];
    for (const pat of patterns) {
        let m;
        while ((m = pat.exec(text)) !== null) {
            allNums.push(parseInt(m[1]));
        }
    }

    results.units = allNums.length > 0 ? Math.max(...allNums) : null;
    return results;
}"""

EXTRACT_SHTACHIM_JS = r"""() => {
    // Parse the שטחים table: rows with תיאור | קיים | תוספת | סה"כ
    const rows = document.querySelectorAll('table tr, .k-grid tr, .k-master-row, [role=row]');
    const result = {units: null, sqm_main: null, sqm_service: null, sqm_mirpesot: null, floors: null};

    for (const row of rows) {
        const text = row.innerText || '';
        const cells = text.split('\t').map(c => c.trim());
        if (cells.length < 4) continue;

        const desc = cells[0];
        // Take סה"כ (last column), fallback to תוספת; skip '-' values
        const rawTotal = cells[3] !== '-' ? cells[3] : cells[2];
        const total = parseFloat(rawTotal);
        if (!total || isNaN(total)) continue;

        if (/יחידות דיור/.test(desc) && total > 0) {
            result.units = Math.round(total);
        } else if (/שטח עיקרי/.test(desc) && !/מעל|מתחת/.test(desc) && total > 0) {
            result.sqm_main = total;
        } else if (/שטחי שירות/.test(desc)) {
            result.sqm_service = total;
        } else if (/מרפסות/.test(desc)) {
            result.sqm_mirpesot = total;
        } else if (/קומות/.test(desc) && !/מרתף/.test(desc)) {
            result.floors = Math.round(total);
        }
    }
    return result;
}"""


def _is_tama38_permit(permit: dict) -> bool:
    """Check if a permit is actually related to tama 38."""
    desc = (permit.get('request_description', '') + ' ' +
            permit.get('request_type', '')).lower()
    return any(kw in desc for kw in [
        'תמ"א', 'תמא', '38', 'חיזוק', 'הריסה ובני', 'פינוי בינוי',
    ])


async def enrich_layer2(json_path: str, label: str, tama38_only: bool = False):
    """Layer 2: Scrape detail pages for permits missing units."""
    from playwright.async_api import async_playwright

    with open(json_path, encoding='utf-8') as f:
        data = json.load(f)

    # Collect permits needing units
    to_scrape = []
    skipped_non_tama = 0
    for key, entry in data.items():
        for i, permit in enumerate(entry.get('permits', [])):
            if permit.get('filtered'):
                continue
            if permit.get('units') is not None:
                continue
            fn = permit.get('file_number', '')
            if not fn or '/' not in fn:
                continue
            if tama38_only and not _is_tama38_permit(permit):
                skipped_non_tama += 1
                continue
            to_scrape.append((key, i, fn))

    if skipped_non_tama:
        print(f"  Skipped {skipped_non_tama} non-tama38 permits")

    print(f"\n{label}: {len(to_scrape)} permits need detail page scraping")
    if not to_scrape:
        return

    async with async_playwright() as p:
        from yk_profile_lock import hold_yk_profile
        hold_yk_profile("enrich_permits.py")  # one YK-profile user at a time
        ctx = await p.chromium.launch_persistent_context(
            BROWSER_DATA, headless=False,
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        try:
            await page.goto(SITE_URL, wait_until="networkidle", timeout=30000)
        except:
            pass
        await asyncio.sleep(5)

        found = 0
        for idx, (key, permit_idx, file_number) in enumerate(to_scrape):
            print(f"[{idx+1}/{len(to_scrape)}] {file_number}...", end=" ", flush=True)

            try:
                # Parse file_number: "2022/0246.01" -> year=2022, num=0246.01
                parts = file_number.split('/')
                year = parts[0].strip()
                num = parts[1].strip()

                try:
                    await page.goto(SITE_URL, wait_until="networkidle", timeout=20000)
                except:
                    pass
                await asyncio.sleep(3)

                all_inputs = await page.query_selector_all("input:visible")
                if len(all_inputs) < 2:
                    print("no_input")
                    continue

                await all_inputs[0].click()
                await all_inputs[0].fill(num)
                await asyncio.sleep(0.5)
                await all_inputs[1].click()
                await all_inputs[1].fill(year)
                await asyncio.sleep(0.5)

                search_btn = await page.query_selector('button.search-btn') or \
                             await page.query_selector('button:has-text("אתר תיק רישוי")')
                if not search_btn:
                    print("no_button")
                    continue

                await search_btn.click()
                await asyncio.sleep(3)
                active_page = ctx.pages[-1] if len(ctx.pages) > 1 else page
                await asyncio.sleep(10)

                # Step 1: Try extracting units from תיאור הבקשה tab
                result = await active_page.evaluate(EXTRACT_UNITS_JS)
                units = result.get('units')

                if units:
                    data[key]['permits'][permit_idx]['units'] = units
                    data[key]['permits'][permit_idx]['units_source'] = 'detail_page'
                    print(f"{units} units (description)")
                    found += 1
                else:
                    # Step 2: Click שטחים tab and extract from table
                    shtachim_tab = await active_page.query_selector(
                        'a.navbar-link:has-text("שטחים")'
                    )
                    if shtachim_tab:
                        await shtachim_tab.click()
                        await asyncio.sleep(3)

                        shtachim = await active_page.evaluate(EXTRACT_SHTACHIM_JS)
                        permit = data[key]['permits'][permit_idx]
                        parts = []

                        if shtachim.get('units') and shtachim['units'] > 0:
                            permit['units'] = shtachim['units']
                            permit['units_source'] = 'shtachim_tab'
                            found += 1
                            parts.append(f"{shtachim['units']} units")
                        else:
                            parts.append("no units")

                        # Always save area data if available
                        if shtachim.get('sqm_main') and shtachim['sqm_main'] > 0:
                            permit['sqm_main'] = shtachim['sqm_main']
                            parts.append(f"sqm={shtachim['sqm_main']}")
                        if shtachim.get('sqm_service') and shtachim['sqm_service'] > 0:
                            permit['sqm_service'] = shtachim['sqm_service']
                        if shtachim.get('sqm_mirpesot') and shtachim['sqm_mirpesot'] > 0:
                            permit['sqm_mirpesot'] = shtachim['sqm_mirpesot']
                        if shtachim.get('floors') and shtachim['floors'] > 0:
                            permit['floors'] = shtachim['floors']
                            parts.append(f"floors={shtachim['floors']}")

                        print(" | ".join(parts))
                    else:
                        print("no שטחים tab")

            except Exception as e:
                print(f"ERROR: {str(e)[:50]}")

            # Close extra tabs
            while len(ctx.pages) > 1:
                await ctx.pages[-1].close()
            page = ctx.pages[0]

            # Save every 10 iterations
            if (idx + 1) % 10 == 0:
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

            await asyncio.sleep(DELAY)

        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        await ctx.close()

    print(f"{label}: Found units for {found} permits from detail pages")


# ─── Main ────────────────────────────────────────────────────────────────────

def print_summary():
    """Print summary of enrichment results."""
    for path, label in [(ALL_PERMITS_PATH, "all_permits"), (TAMA38_PERMITS_PATH, "tama38_permits")]:
        with open(path, encoding='utf-8') as f:
            data = json.load(f)
        total = sum(len(e.get('permits', [])) for e in data.values())
        with_units = sum(1 for e in data.values() for p in e.get('permits', []) if p.get('units'))
        filtered = sum(1 for e in data.values() for p in e.get('permits', []) if p.get('filtered'))
        classified = sum(1 for e in data.values() for p in e.get('permits', []) if p.get('tama_type'))
        print(f"\n{label}: {total} permits total")
        print(f"  With units: {with_units}")
        print(f"  Filtered (balcony-only): {filtered}")
        if label == "tama38_permits":
            t1 = sum(1 for e in data.values() for p in e.get('permits', []) if p.get('tama_type') == '38/1')
            t2 = sum(1 for e in data.values() for p in e.get('permits', []) if p.get('tama_type') == '38/2')
            print(f"  Classified: {classified} (38/1: {t1}, 38/2: {t2})")


async def main():
    sys.stdout.reconfigure(encoding='utf-8')

    # Layer 1: Always run
    print("=== Layer 1: Regex enrichment ===")
    enrich_layer1()

    # Layer 2: Only with --layer2 flag
    if '--layer2' in sys.argv:
        print("\n=== Layer 2: Detail page scraping ===")
        await enrich_layer2(ALL_PERMITS_PATH, "all_permits")
        await enrich_layer2(TAMA38_PERMITS_PATH, "tama38_permits", tama38_only=True)

    print("\n=== Summary ===")
    print_summary()


if __name__ == "__main__":
    asyncio.run(main())
