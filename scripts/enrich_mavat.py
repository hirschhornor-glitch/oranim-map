"""
enrich_mavat.py — Same approach as update_mavat_status.py:
persistent browser context + input() for captcha + API + DOM scraping.
"""
import asyncio, json, sys
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from playwright.async_api import async_playwright

CREDS_FILE    = r"C:\ORANIM\oranim-490018-ceaf784afe61.json"
SHEET_ID      = "1_AcuuA1CNPh6jXc_lZKNghfpEF1aDPV8Zci8QPz2WVE"
PLANS_GEOJSON = r"C:\ORANIM\oranim-app\data\plans.geojson"
BROWSER_DATA  = r"C:\ORANIM\.browser_data_mavat_enrich"
OUTPUT_FILE   = r"C:\ORANIM\enrichment_results.json"

SKIP = {
    '101-0133942','101-0209593','101-0178129','101-0666289','101-0800771','101-1024272',
    'תתל/ 86','תתל/ 108','תתל/ 108/ 77-78','תתל/ 108/ א/ 77',
    'תתל/ 108/ סעיף-78-77','תתל/ 130','תתל/ 131',
    '101-0644476','101-0906826','101-1095892',
    '101-0210054','101-0635938','101-1185099',
}

EXTRACT_QUANT_JS = r"""
() => {
    const r = {};
    // Process each accordion item: title + content together
    const items = document.querySelectorAll('.ng-star-inserted.uk-accordion li');
    for (const li of items) {
        const titleEl = li.querySelector('.uk-accordion-title');
        const contentEl = li.querySelector('.uk-accordion-content');
        if (!titleEl) continue;
        const text = titleEl.textContent.replace(/\s+/g, ' ').trim();
        const ctext = contentEl ? contentEl.textContent.replace(/\s+/g, ' ').trim() : '';
        let m;

        // Extract totals from title
        m = text.match(/מגורים\s*\(יח.?ד\)\s*([\d,]+)/); if (m) r.units_total = m[1].replace(/,/g, '');
        m = text.match(/מגורים\s*\(מ.?ר\)\s*([\d,]+)/); if (m) r.housing_sqm = m[1].replace(/,/g, '');
        m = text.match(/מסחר\s*\(מ.?ר\)\s*([\d,]+)/); if (m) r.commerce_sqm = m[1].replace(/,/g, '');
        m = text.match(/(תעסוקה|משרדים)\s*\(מ.?ר\)\s*([\d,]+)/); if (m) r.employment_sqm = m[2].replace(/,/g, '');
        m = text.match(/חדרי מלון[^(]*\(חדר[^)]*\)\s*([\d,]+)/); if (m) r.hotel_rooms = m[1].replace(/,/g, '');
        m = text.match(/מלונ[^\s]*\s*\(חדר[^)]*\)\s*([\d,]+)/); if (m && !r.hotel_rooms) r.hotel_rooms = m[1].replace(/,/g, '');
        m = text.match(/מלונ[^\s]*\s*\(יח.?ד\)\s*([\d,]+)/); if (m && !r.hotel_rooms) r.hotel_rooms = m[1].replace(/,/g, '');
        m = text.match(/דיור מיוחד\s*\(יח.?ד\)\s*([\d,]+)/); if (m) r.special_housing = m[1].replace(/,/g, '');
        m = text.match(/דירות להשכרה\s*\(יח.?ד\)\s*([\d,]+)/); if (m) r.rental_units = m[1].replace(/,/g, '');
        m = text.match(/דיור מותנה\s*\(יח.?ד\)\s*([\d,]+)/); if (m) r.conditional_housing = m[1].replace(/,/g, '');
        m = text.match(/דיור מוגן\s*\(יח.?ד\)\s*([\d,]+)/); if (m) r.sheltered_housing = m[1].replace(/,/g, '');
        m = text.match(/מבני ציבור\s*\(מ.?ר\)\s*([\d,]+)/); if (m) r.public_buildings_sqm = m[1].replace(/,/g, '');

        // Extract change + approved values from content
        if (ctext) {
            const changeMatch = ctext.match(/שינוי.*?([+-]?\s*[\d,.]+)/);
            const approvedMatch = ctext.match(/מצב מאושר[^\d]*([+-]?\s*[\d,.]+)/);
            if (changeMatch) {
                const changeVal = changeMatch[1].replace(/[\s,]/g, '');
                if (text.includes('מגורים') && text.includes('יח')) {
                    if (!r.units_add) r.units_add = changeVal;
                }
            }
            if (approvedMatch) {
                const approvedVal = approvedMatch[1].replace(/[\s,]/g, '');
                if (text.includes('מסחר')) {
                    r.commerce_in = approvedVal;
                }
                if (text.match(/(תעסוקה|משרדים)/)) {
                    r.employment_in = approvedVal;
                }
            } else if (ctext.includes('מצב מאושר')) {
                // "מצב מאושר" label exists but no number parsed → existing approved = 0
                if (text.includes('מסחר')) {
                    r.commerce_in = '0';
                }
                if (text.match(/(תעסוקה|משרדים)/)) {
                    r.employment_in = '0';
                }
            }
        }
    }
    // Fallback: also scan accordion titles directly (for pages without li structure)
    const titles = document.querySelectorAll('.ng-star-inserted.uk-accordion .uk-accordion-title');
    for (const t of titles) {
        const text = t.textContent.replace(/\s+/g, ' ').trim();
        let m;
        if (!r.units_total) { m = text.match(/מגורים\s*\(יח.?ד\)\s*([\d,]+)/); if (m) r.units_total = m[1].replace(/,/g, ''); }
        if (!r.commerce_sqm) { m = text.match(/מסחר\s*\(מ.?ר\)\s*([\d,]+)/); if (m) r.commerce_sqm = m[1].replace(/,/g, ''); }
        if (!r.employment_sqm) { m = text.match(/(תעסוקה|משרדים)\s*\(מ.?ר\)\s*([\d,]+)/); if (m) r.employment_sqm = m[2].replace(/,/g, ''); }
        if (!r.hotel_rooms) { m = text.match(/(חדרי מלון[^(]*|מלונ[^\s]*)\s*\((חדר|יח.?ד)[^)]*\)\s*([\d,]+)/); if (m) r.hotel_rooms = m[3].replace(/,/g, ''); }
        if (!r.rental_units) { m = text.match(/דירות להשכרה\s*\(יח.?ד\)\s*([\d,]+)/); if (m) r.rental_units = m[1].replace(/,/g, ''); }
        if (!r.conditional_housing) { m = text.match(/דיור מותנה\s*\(יח.?ד\)\s*([\d,]+)/); if (m) r.conditional_housing = m[1].replace(/,/g, ''); }
        if (!r.sheltered_housing) { m = text.match(/דיור מוגן\s*\(יח.?ד\)\s*([\d,]+)/); if (m) r.sheltered_housing = m[1].replace(/,/g, ''); }
        if (!r.public_buildings_sqm) { m = text.match(/מבני ציבור\s*\(מ.?ר\)\s*([\d,]+)/); if (m) r.public_buildings_sqm = m[1].replace(/,/g, ''); }
    }
    // Total area
    const aside = (document.querySelector('.sv4-cols-aside') || document.body).innerText;
    const am = aside.match(/סה"כ שטח בדונם\s*([\d,.]+)/);
    if (am) r.total_area = am[1];
    return r;
}
"""


async def main():
    sys.stdout.reconfigure(encoding='utf-8')

    run_all = '--all' in sys.argv

    if run_all:
        # Read all plans from Google Sheets
        print("Reading all plans from Google Sheets...")
        creds = Credentials.from_service_account_file(CREDS_FILE,
            scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
        sheet_src = gspread.authorize(creds).open_by_key(SHEET_ID).sheet1
        all_rows = sheet_src.get_all_values()
        hdrs = {hdr.strip().lower(): i for i, hdr in enumerate(all_rows[0])}
        plans = []
        seen = set()
        for row in all_rows[1:]:
            pn = row[hdrs['plan_name']].strip()
            agam = row[hdrs['agam_id']].strip()
            if agam.endswith('.0'): agam = agam[:-2]
            if pn and agam and pn not in SKIP and pn not in seen:
                seen.add(pn)
                plans.append((pn, agam))
    else:
        with open('new_plans_xplan_status.json', encoding='utf-8') as f:
            xplan = json.load(f)
        plans = [(pn, str(int(info['mp_id']))) for pn, info in xplan.items() if pn not in SKIP]

    print(f"Plans to enrich: {len(plans)}\n")

    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(
            BROWSER_DATA, headless=False,
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        # Navigate to establish session (same as update_mavat_status.py)
        print("Loading Mavat...")
        try:
            await page.goto("https://mavat.iplan.gov.il/SV4/1/1000247867/310",
                           wait_until="domcontentloaded", timeout=120000)
        except: pass
        await asyncio.sleep(10)

        print("\nIf you see a captcha, solve it now. Waiting 45 seconds...")
        await asyncio.sleep(45)

        # Test API
        api_works = False
        for attempt in range(3):
            test = await page.evaluate("""async()=>{
                try{const r=await fetch('/rest/api/SV4/1?mid=1000247867&guid=0');
                const d=JSON.parse(await r.text());
                return{ok:!!d.planDetails,s:(d.planDetails||{}).LAST_STEP_DES};}
                catch(e){return{ok:false,e:e.message};}
            }""")
            print(f"API test {attempt+1}: {test}")
            if test.get("ok"):
                api_works = True
                break
            await asyncio.sleep(5)

        results = {}

        # Phase 1: API batch (status + date + name) — if API works
        if api_works:
            print(f"\n=== Phase 1: API batch fetch ===")
            for i in range(0, len(plans), 10):
                batch = plans[i:i+10]
                ids = [a for _, a in batch]
                batch_res = await page.evaluate("""async(ids)=>{
                    const R=[];
                    for(const id of ids){
                        try{
                            const r=await fetch('/rest/api/SV4/1?mid='+id+'&guid=0');
                            const t=await r.text();
                            if(t.length<50){R.push({id,error:'short'});continue;}
                            const d=JSON.parse(t).planDetails||{};
                            R.push({id, name:d.E_NAME||'', status:d.LAST_STEP_DES||'',
                                date:d.LAST_STEP_DATE||'', permissions:(d.PERMISSIONS||'').substring(0,500)});
                        }catch(e){R.push({id,error:e.message});}
                        await new Promise(r=>setTimeout(r,300));
                    }
                    return R;
                }""", ids)
                for j, (pn, agam) in enumerate(batch):
                    if j < len(batch_res):
                        r = batch_res[j]
                        results[pn] = r
                        if not r.get("error"):
                            print(f"  {pn}: {r.get('status','?')} | {r.get('name','')[:40]}")
                        else:
                            print(f"  {pn}: ERR {r['error']}")
                await asyncio.sleep(1)
        else:
            print("API not available, will get status from DOM only.")

        # Phase 2: DOM scraping (quantitative data — navigate to each page)
        print(f"\n=== Phase 2: DOM scraping (quantitative data) ===")
        for i, (pn, agam) in enumerate(plans):
            url = f"https://mavat.iplan.gov.il/SV4/1/{agam}/310"
            print(f"[{i+1}/{len(plans)}] {pn}...", end=" ", flush=True)
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(8)

                # Get status from DOM if API didn't work
                if pn not in results or results[pn].get("error"):
                    dom_status = await page.evaluate("""()=>{
                        const lines = document.body.innerText.split('\\n').slice(0,30).map(l=>l.trim());
                        const r = {};
                        for(const l of lines){
                            if(!r.status && /^(אישור|בבדיקה|הפקדה|דיון|במילוי|נקלטה|נפתח|תום|הכנת|בהליך|תכנית עומדת|מאושרת|תחילת)/.test(l)) r.status=l;
                            const dm=l.match(/^(\\d{2}\\/\\d{2}\\/\\d{4})$/);
                            if(dm&&!r.date) r.date=dm[1];
                        }
                        // Description
                        const h1=document.querySelector('.sv4-h1-content');
                        if(h1){const ls=h1.innerText.split('\\n').map(l=>l.trim()).filter(l=>l);
                            if(ls.length>1) r.name=ls.slice(1).join(' ').substring(0,200);}
                        return r;
                    }""")
                    results[pn] = {**(results.get(pn) or {}), **dom_status}

                # Expand quantitative sections
                try:
                    btn = page.locator("text=נתונים כמותיים עיקריים")
                    if await btn.count() > 0:
                        await btn.first.click()
                        await asyncio.sleep(4)
                except: pass
                try:
                    btn2 = page.locator("text=נתונים נוספים")
                    if await btn2.count() > 0:
                        await btn2.first.click()
                        await asyncio.sleep(4)
                        await page.evaluate("window.scrollBy(0, 600)")
                        await asyncio.sleep(2)
                except: pass
                # Expand all quantitative accordion items to get change values
                try:
                    await page.evaluate(r"""() => {
                        document.querySelectorAll('.uk-accordion-title').forEach(t => {
                            const txt = t.textContent;
                            if (txt.match(/(מגורים|מסחר|תעסוקה|משרדים|מלונ|דיור|השכרה|ציבור)/)) {
                                t.click();
                            }
                        });
                    }""")
                    await asyncio.sleep(3)
                except: pass

                # Extract quantitative data
                qdata = await page.evaluate(EXTRACT_QUANT_JS)
                if any(qdata.values()):
                    results.setdefault(pn, {}).update(qdata)

                r = results.get(pn, {})
                parts = [r.get("status", "?")]
                if r.get("date"): parts.append(r["date"] if "/" in str(r["date"]) else "")
                if r.get("units_total"): parts.append(f"units={r['units_total']}")
                if r.get("commerce_sqm"): parts.append(f"commerce={r['commerce_sqm']}" + (f"(+{r['commerce_in']})" if r.get('commerce_in') else ""))
                if r.get("employment_sqm"): parts.append(f"emp={r['employment_sqm']}" + (f"(+{r['employment_in']})" if r.get('employment_in') else ""))
                if r.get("hotel_rooms"): parts.append(f"hotels={r['hotel_rooms']}")
                if r.get("rental_units"): parts.append(f"rental={r['rental_units']}")
                if r.get("conditional_housing"): parts.append(f"cond={r['conditional_housing']}")
                if r.get("sheltered_housing"): parts.append(f"shelter={r['sheltered_housing']}")
                if r.get("public_buildings_sqm"): parts.append(f"pub={r['public_buildings_sqm']}")
                if r.get("special_housing"): parts.append(f"special={r['special_housing']}")
                print(" | ".join(p for p in parts if p))

            except Exception as e:
                print(f"ERROR: {e}")
                results.setdefault(pn, {})["page_error"] = str(e)

            await asyncio.sleep(1)

        await ctx.close()

    # Save results
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # === Update Sheets + GeoJSON ===
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("\nUpdating Google Sheets...")
    creds = Credentials.from_service_account_file(CREDS_FILE,
        scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
    sheet = gspread.authorize(creds).open_by_key(SHEET_ID).sheet1
    all_data = sheet.get_all_values()
    headers = all_data[0]
    h = {hdr.strip().lower(): i for i, hdr in enumerate(headers)}

    batch = []
    for row_num, row in enumerate(all_data[1:], start=2):
        pn = row[h["plan_name"]].strip()
        if pn not in results: continue
        r = results[pn]
        if r.get("error") and not any(r.get(k) for k in ("status","units_total","commerce_sqm")): continue

        def add(col, val):
            if val and col in h:
                batch.append({"range": gspread.utils.rowcol_to_a1(row_num, h[col]+1), "values": [[str(val)]]})

        dt = r.get("date", "")
        if dt and "T" in str(dt):
            try: dt = datetime.fromisoformat(dt.replace("Z", "+00:00")).strftime("%d/%m/%Y")
            except: pass

        add("plan_name_he", r.get("name"))
        add("status_mavat", r.get("status"))
        add("mavat_date", dt)
        add("plan_summary", (r.get("permissions") or "")[:200])
        add("units_total", r.get("units_total"))
        add("units_add", r.get("units_add"))
        add("commerce_out", r.get("commerce_sqm"))
        add("commerce_in", r.get("commerce_in"))
        add("employment", r.get("employment_sqm"))
        add("employment_in", r.get("employment_in"))
        add("hotel_rooms", r.get("hotel_rooms"))
        add("rental_units", r.get("rental_units"))
        add("conditional_housing", r.get("conditional_housing"))
        add("sheltered_housing", r.get("sheltered_housing"))
        add("public_buildings_sqm", r.get("public_buildings_sqm"))
        add("last_modified", now_str)

    if batch:
        import time
        for i in range(0, len(batch), 50):
            sheet.spreadsheet.values_batch_update({"valueInputOption": "RAW", "data": batch[i:i+50]})
            if i + 50 < len(batch):
                time.sleep(5)  # avoid quota limit
    print(f"  Sheets: {len(batch)} cells updated")

    print("Updating plans.geojson...")
    with open(PLANS_GEOJSON, encoding="utf-8") as f:
        geojson = json.load(f)

    for feat in geojson["features"]:
        pn = feat["properties"].get("plan_name", "")
        if pn not in results: continue
        r = results[pn]
        props = feat["properties"]
        dt = r.get("date", "")
        if dt and "T" in str(dt):
            try: dt = datetime.fromisoformat(dt.replace("Z", "+00:00")).strftime("%d/%m/%Y")
            except: pass
        if r.get("name"): props["plan_name_he"] = r["name"]
        if r.get("status"): props["status_mavat"] = r["status"]
        if dt: props["mavat_date"] = dt
        if r.get("permissions"): props["plan_summary"] = r["permissions"][:200]
        if r.get("units_total"): props["units_total"] = r["units_total"]
        if r.get("units_add"):
            try: props["units_add"] = float(r["units_add"].replace("+",""))
            except: pass
        if r.get("commerce_sqm"): props["commerce_out"] = r["commerce_sqm"]
        if r.get("commerce_in"): props["commerce_in"] = r["commerce_in"]
        if r.get("employment_sqm"): props["employment"] = r["employment_sqm"]
        if r.get("employment_in"): props["employment_in"] = r["employment_in"]
        if r.get("hotel_rooms"): props["hotel_rooms"] = r["hotel_rooms"]
        if r.get("rental_units"): props["rental_units"] = r["rental_units"]
        if r.get("conditional_housing"): props["conditional_housing"] = r["conditional_housing"]
        if r.get("sheltered_housing"): props["sheltered_housing"] = r["sheltered_housing"]
        if r.get("public_buildings_sqm"): props["public_buildings_sqm"] = r["public_buildings_sqm"]
        props["last_modified"] = now_str

    with open(PLANS_GEOJSON, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False)

    ok = sum(1 for r in results.values() if r.get("status"))
    wu = sum(1 for r in results.values() if r.get("units_total"))
    wc = sum(1 for r in results.values() if r.get("commerce_sqm"))
    wr = sum(1 for r in results.values() if r.get("rental_units"))
    wh = sum(1 for r in results.values() if r.get("hotel_rooms"))
    print(f"\nDone! with_status={ok}, with_units={wu}, with_commerce={wc}, with_rental={wr}, with_hotels={wh}")


if __name__ == "__main__":
    asyncio.run(main())
