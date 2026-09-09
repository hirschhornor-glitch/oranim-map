"""Quick API-based enrichment for all new plans."""
import asyncio, json, sys
from datetime import datetime
from plan_name_util import clean_plan_name
from playwright.async_api import async_playwright
from xplan_units import units_for_plan

SKIP = {
    '101-0133942','101-0209593','101-0178129','101-0666289','101-0800771','101-1024272',
    'תתל/ 86','תתל/ 108','תתל/ 108/ 77-78','תתל/ 108/ א/ 77',
    'תתל/ 108/ סעיף-78-77','תתל/ 130','תתל/ 131',
    '101-0644476','101-0906826','101-1095892',
    '101-0210054','101-0635938','101-1185099',
}

async def main():
    sys.stdout.reconfigure(encoding='utf-8')

    with open('new_plans_xplan_status.json', encoding='utf-8') as f:
        xplan = json.load(f)

    plans = [(pn, str(int(info['mp_id']))) for pn, info in xplan.items() if pn not in SKIP]
    print(f"Plans: {len(plans)}")

    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(
            r"C:\ORANIM\.browser_data_enrich",
            headless=False, viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        print("Loading Mavat...")
        try:
            await page.goto("https://mavat.iplan.gov.il/SV4/1/1000247867/310",
                           wait_until="domcontentloaded", timeout=60000)
        except: pass
        await asyncio.sleep(8)

        # Test API
        test = await page.evaluate("""async()=>{
            try{const r=await fetch('/rest/api/SV4/1?mid=1000247867&guid=0');
            const d=JSON.parse(await r.text());
            return{ok:!!d.planDetails,s:(d.planDetails||{}).LAST_STEP_DES};}
            catch(e){return{ok:false,e:e.message};}
        }""")
        print(f"API test: {test}")

        if not test.get("ok"):
            print("API blocked. Exiting.")
            await ctx.close()
            return

        # Batch fetch API data
        print(f"\nFetching {len(plans)} plans via API...\n")
        results = {}

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
                        R.push({id,
                            name:d.E_NAME||'',
                            status:d.LAST_STEP_DES||'',
                            date:d.LAST_STEP_DATE||'',
                            type:d.ENTITY_SUBTYPE||'',
                            permissions:(d.PERMISSIONS||'').substring(0,500),
                            authority:d.AUTH||''
                        });
                    }catch(e){R.push({id,error:e.message});}
                    await new Promise(r=>setTimeout(r,300));
                }
                return R;
            }""", ids)

            for j, (pn, agam) in enumerate(batch):
                if j < len(batch_res):
                    r = batch_res[j]
                    results[pn] = r
                    if r.get("error"):
                        print(f"  {pn}: ERR {r['error']}")
                    else:
                        print(f"  {pn}: {r.get('status','?')} | {r.get('name','?')[:50]}")

            await asyncio.sleep(1)

        # Navigate to each plan page for quantitative data
        print(f"\nScraping quantitative data from plan pages...")
        ok_plans = [(pn, agam) for pn, agam in plans if pn in results and not results[pn].get("error")]

        for i, (pn, agam) in enumerate(ok_plans):
            url = f"https://mavat.iplan.gov.il/SV4/1/{agam}/310"
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                await asyncio.sleep(6)

                # Expand quantitative section
                try:
                    btn = page.locator("text=נתונים כמותיים עיקריים")
                    if await btn.count() > 0:
                        await btn.first.click()
                        await asyncio.sleep(3)
                except:
                    pass

                # Extract numbers
                qdata = await page.evaluate(r"""()=>{
                    const r={};
                    const text=document.body.innerText||'';
                    const lines=text.split('\n').map(l=>l.trim()).filter(l=>l);
                    let inQ=false;
                    for(let i=0;i<lines.length;i++){
                        if(lines[i].includes('נתונים כמותיים')) inQ=true;
                        if(!inQ) continue;
                        const l=lines[i];
                        if(l.includes('מגורים') && l.includes('יח')){
                            for(let j=i-2;j<=i+2;j++){
                                if(j>=0&&j<lines.length){
                                    const m=lines[j].match(/^([\d,]+)$/);
                                    if(m&&parseInt(m[1].replace(/,/g,''))>0){r.units=m[1].replace(/,/g,'');break;}
                                }
                            }
                        }
                        if(l.includes('מסחר') && l.includes('מ')){
                            for(let j=i-2;j<=i+2;j++){
                                if(j>=0&&j<lines.length){
                                    const m=lines[j].match(/^([\d,]+)$/);
                                    if(m&&parseInt(m[1].replace(/,/g,''))>100){r.commerce=m[1].replace(/,/g,'');break;}
                                }
                            }
                        }
                        if((l.includes('תעסוקה')||l.includes('משרדים')) && l.includes('מ')){
                            for(let j=i-2;j<=i+2;j++){
                                if(j>=0&&j<lines.length){
                                    const m=lines[j].match(/^([\d,]+)$/);
                                    if(m&&parseInt(m[1].replace(/,/g,''))>100){r.employment=m[1].replace(/,/g,'');break;}
                                }
                            }
                        }
                        if(l.includes('מלונ') && l.includes('חדר')){
                            for(let j=i-2;j<=i+2;j++){
                                if(j>=0&&j<lines.length){
                                    const m=lines[j].match(/^([\d,]+)$/);
                                    if(m){r.hotels=m[1].replace(/,/g,'');break;}
                                }
                            }
                        }
                        if(l.includes('יחס לתכניות')||l.includes('החלטות מוסדות')) break;
                    }
                    return r;
                }""")

                if any(qdata.values()):
                    results[pn].update(qdata)
                    print(f"  [{i+1}/{len(ok_plans)}] {pn}: units={qdata.get('units','-')} commerce={qdata.get('commerce','-')} emp={qdata.get('employment','-')} hotels={qdata.get('hotels','-')}")

            except Exception as e:
                pass

            await asyncio.sleep(0.5)

        await ctx.close()

    # Save
    with open("enrichment_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Update Sheets + GeoJSON
    import gspread
    from google.oauth2.service_account import Credentials

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Sheets
    creds = Credentials.from_service_account_file(
        r"C:\ORANIM\oranim-490018-ceaf784afe61.json",
        scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    )
    sheet = gspread.authorize(creds).open_by_key("1_AcuuA1CNPh6jXc_lZKNghfpEF1aDPV8Zci8QPz2WVE").sheet1
    all_data = sheet.get_all_values()
    headers = all_data[0]
    h = {hdr.strip().lower(): i for i, hdr in enumerate(headers)}

    batch = []
    updated = 0
    for row_num, row in enumerate(all_data[1:], start=2):
        pn = row[h["plan_name"]].strip()
        if pn not in results or results[pn].get("error"):
            continue
        r = results[pn]

        def add(col, val):
            if val and col in h:
                batch.append({"range": gspread.utils.rowcol_to_a1(row_num, h[col]+1), "values": [[str(val)]]})

        # Format date
        dt = r.get("date", "")
        if dt and "T" in str(dt):
            try:
                dt = datetime.fromisoformat(dt.replace("Z", "+00:00")).strftime("%d/%m/%Y")
            except:
                pass

        add("plan_name_he", r.get("name"))
        add("status_mavat", r.get("status"))
        add("mavat_date", dt)
        add("plan_summary", (r.get("permissions") or "")[:200])
        add("units_total", r.get("units"))
        # נכנס/תוספת from XPLAN (gated on reconciling with the total). Until
        # 2026-09 this script wrote units_total here and units_add=units_total to
        # plans.geojson only — the two stores disagreed from birth and units_in
        # was never written at all. See xplan_units.py.
        xu, _why = units_for_plan(pn, r.get("units"))
        r["_xplan_units"] = xu          # reused for the geojson pass below
        if xu:
            # fill-only, and 0 is a real value here (new build on an empty lot),
            # so this can't go through add() — it drops falsy values.
            for col, val in (("units_in", xu["units_in"]), ("units_add", xu["units_add"])):
                ci = h.get(col)
                if ci is None or (ci < len(row) and row[ci].strip()):
                    continue
                batch.append({"range": gspread.utils.rowcol_to_a1(row_num, ci + 1),
                              "values": [[str(int(val))]]})
        add("commerce_out", r.get("commerce"))
        add("employment", r.get("employment"))
        add("hotels", r.get("hotels"))
        add("last_modified", now_str)
        updated += 1

    if batch:
        for i in range(0, len(batch), 50):
            sheet.spreadsheet.values_batch_update({"valueInputOption": "RAW", "data": batch[i:i+50]})
    print(f"\nSheets: updated {updated} rows ({len(batch)} cells)")

    # GeoJSON
    with open(r"C:\ORANIM\oranim-app\data\plans.geojson", encoding="utf-8") as f:
        geojson = json.load(f)

    geo_updated = 0
    for feat in geojson["features"]:
        pn = feat["properties"].get("plan_name", "")
        if pn not in results or results[pn].get("error"):
            continue
        r = results[pn]
        props = feat["properties"]

        dt = r.get("date", "")
        if dt and "T" in str(dt):
            try: dt = datetime.fromisoformat(dt.replace("Z", "+00:00")).strftime("%d/%m/%Y")
            except: pass

        if r.get("name"): props["plan_name_he"] = clean_plan_name(r["name"])
        if r.get("status"): props["status_mavat"] = r["status"]
        if dt: props["mavat_date"] = dt
        if r.get("permissions"): props["plan_summary"] = r["permissions"][:200]
        if r.get("units"): props["units_total"] = r["units"]
        # Was: units_add = units_total — a guess that overstated every plan built
        # over existing units, and that GS never mirrored. Now both stores get the
        # same XPLAN-verified pair, or neither gets one.
        xu = r.get("_xplan_units")
        if xu:
            props["units_in"] = xu["units_in"]
            props["units_add"] = xu["units_add"]
        if r.get("commerce"): props["commerce_out"] = r["commerce"]
        if r.get("employment"): props["employment"] = r["employment"]
        if r.get("hotels"): props["hotels"] = r["hotels"]
        props["last_modified"] = now_str
        geo_updated += 1

    with open(r"C:\ORANIM\oranim-app\data\plans.geojson", "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False)
    print(f"GeoJSON: updated {geo_updated} features")

    ok = sum(1 for r in results.values() if not r.get("error"))
    wu = sum(1 for r in results.values() if r.get("units"))
    wc = sum(1 for r in results.values() if r.get("commerce"))
    print(f"\nDone! OK={ok}/{len(results)}, with_units={wu}, with_commerce={wc}")


if __name__ == "__main__":
    asyncio.run(main())
