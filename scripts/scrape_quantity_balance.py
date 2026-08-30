"""Scrape the Mavat 'נתונים כמותיים עיקריים בתכנית' section per plan to get the
existing-state (נכנס) figures, then compute תוספת (units_add) and מכפיל (multiplier).

Data is in textContent (incl. collapsed cards), so we only need to expand the outer
accordions and click "נתונים נוספים" — no per-card chevron clicking. See
parse_quantity_balance.py for the parse format.

Usage:
    python scrape_quantity_balance.py            # full run, resume from checkpoint
    python scrape_quantity_balance.py 20         # limit to 20 plans
    python scrape_quantity_balance.py stats       # print stats only
"""
import asyncio
import json
import sys
import time
from pathlib import Path

from playwright.async_api import async_playwright
from parse_quantity_balance import parse_quantity_balance

if sys.platform.startswith("win"):
    sys.stdout.reconfigure(encoding="utf-8")

PLANS_GEOJSON = Path(r"C:\ORANIM\oranim-app\data\plans.geojson")
OUTPUT_FILE = Path(r"C:\ORANIM\quantity_balance_results.json")
BROWSER_DATA = r"C:\ORANIM\.browser_data_table5_xlsx"
MAVAT_BASE = "https://mavat.iplan.gov.il"

RELEVANT_STATUSES = {
    "אישור", "הפקדה", "הפקדה להתנגדויות/השגות",
    "דיון בהתנגדויות ותיקונים", "בבדיקה תכנונית",
    "תום תקופת הפקדה", "במילוי תנאים להפקדה",
    "הכרעה בהתנגדויות / אישור", "בהליך אישור",
    "תבע - טרום אישור",
}

EXPAND_ALL_JS = r"""
() => { let c=0; document.querySelectorAll('.uk-accordion-title').forEach(t=>{
  const p=t.parentElement; if(p&&!p.classList.contains('uk-open')){try{t.click();c++;}catch(e){}}}); return c; }
"""
CLICK_MORE_JS = r"""
() => { let c=0; document.querySelectorAll('*').forEach(el=>{
  if(el.children.length) return; const t=(el.textContent||'').trim();
  if(t.includes('נתונים נוספים')){try{el.click();c++;}catch(e){}} }); return c; }
"""
READ_SECTION_JS = r"""
() => {
  const titles=[...document.querySelectorAll('.uk-accordion-title')];
  const t=titles.find(x=>(x.textContent||'').includes('נתונים כמותיים'));
  if(!t) return '';
  let panel=t.parentElement?t.parentElement.querySelector('.uk-accordion-content'):null;
  if(!panel) panel=t.nextElementSibling||t.parentElement;
  return panel ? (panel.textContent||'') : '';
}
"""


def load_plans():
    gj = json.loads(PLANS_GEOJSON.read_text(encoding="utf-8"))
    plans, seen = [], set()
    for feat in gj["features"]:
        p = feat["properties"]
        status = (p.get("status_mavat") or "").strip()
        agam_id = p.get("agam_id", "")
        plan_name = p.get("plan_name", "")
        if not (status in RELEVANT_STATUSES and agam_id and plan_name):
            continue
        if plan_name in seen:
            continue
        seen.add(plan_name)
        try:
            agam_str = str(int(float(agam_id)))
        except (ValueError, TypeError):
            continue
        plans.append({"plan_number": plan_name, "agam_id": agam_str,
                      "taba": p.get("taba", ""), "status": status,
                      "units_total_gs": p.get("units_total", "")})
    return plans


def load_results():
    if OUTPUT_FILE.exists():
        return json.loads(OUTPUT_FILE.read_text(encoding="utf-8")).get("results", [])
    return []


def save_results(results, scope):
    OUTPUT_FILE.write_text(json.dumps(
        {"version": 1, "scope": scope, "processed": len(results), "results": results},
        ensure_ascii=False, indent=1), encoding="utf-8")


async def scrape_plan(page, plan):
    url = f"{MAVAT_BASE}/SV4/1/{plan['agam_id']}/310"
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except Exception:
        pass
    await asyncio.sleep(5)
    for _ in range(3):
        try:
            await page.evaluate(EXPAND_ALL_JS)
        except Exception:
            break
        await asyncio.sleep(1.0)
    try:
        await page.evaluate(CLICK_MORE_JS)
    except Exception:
        pass
    await asyncio.sleep(1.0)
    try:
        text = await page.evaluate(READ_SECTION_JS)
    except Exception:
        text = ""
    if not text or "נתונים כמותיים" not in text and "(יח" not in text and "(מ" not in text:
        return {"plan_number": plan["plan_number"], "taba": plan.get("taba", ""),
                "status": "no_data", "text_len": len(text or "")}
    parsed = parse_quantity_balance(text)
    rollup = {k: v for k, v in parsed.items() if k != "cards"}
    return {"plan_number": plan["plan_number"], "taba": plan.get("taba", ""),
            "agam_id": plan["agam_id"], "status": "success",
            "cards": parsed.get("cards", []), **rollup}


def print_stats(results):
    succ = [r for r in results if r.get("status") == "success"]
    print(f"Processed: {len(results)} | success: {len(succ)} | "
          f"no_data: {sum(1 for r in results if r.get('status')=='no_data')}")
    print(f"  with units_in>0:  {sum(1 for r in succ if (r.get('units_in') or 0) > 0)}")
    print(f"  with units_add>0: {sum(1 for r in succ if r.get('units_add',0)>0)}")
    print(f"  with multiplier:  {sum(1 for r in succ if r.get('multiplier'))}")
    print(f"  with commerce_in>0: {sum(1 for r in succ if r.get('commerce_in',0)>0)}")


async def main():
    args = sys.argv[1:]
    plans = load_plans()
    def _taba_key(p):
        try:
            return int(float(p.get("taba") or 0))
        except (ValueError, TypeError):
            return 0
    plans.sort(key=_taba_key, reverse=True)
    existing = load_results()
    done = {r["plan_number"] for r in existing}

    if args and args[0] == "stats":
        print_stats(existing)
        return

    to_do = [p for p in plans if p["plan_number"] not in done]
    if args and args[0].isdigit():
        to_do = to_do[: int(args[0])]
    print(f"Scope: {len(plans)} | already done: {len(existing)} | to do: {len(to_do)}")
    if not to_do:
        print_stats(existing)
        return

    results = list(existing)
    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            BROWSER_DATA, headless=False, viewport={"width": 1400, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36")
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        try:
            await page.goto(f"{MAVAT_BASE}/SV4/1/{to_do[0]['agam_id']}/310",
                            wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(10)
        except Exception:
            pass

        start = time.time()
        for i, plan in enumerate(to_do):
            try:
                r = await scrape_plan(page, plan)
            except Exception as e:
                r = {"plan_number": plan["plan_number"], "status": "error", "error": str(e)[:200]}
            results.append(r)
            el = time.time() - start
            print(f"  [{i+1}/{len(to_do)}] {plan['plan_number']:16s} -> {r.get('status'):8s} "
                  f"units_in={r.get('units_in','-')} add={r.get('units_add','-')} "
                  f"mult={r.get('multiplier','-')} commerce_in={r.get('commerce_in','-')} ({el:.0f}s)",
                  flush=True)
            if (i + 1) % 10 == 0:
                save_results(results, len(plans))
            await asyncio.sleep(1.0)
        save_results(results, len(plans))
        print("Done.")
        print_stats(results)


if __name__ == "__main__":
    asyncio.run(main())
