"""Pass A: scrape Table 5 XLS files from Mavat for all relevant plans.

Mavat serves 'זכויות והוראות בניה XLS' files under the קבצים דיגיטלים accordion.
The file is actually HTML, parseable via pandas.read_html.

Usage:
    python scrape_table5_xlsx.py                  # full run, resume from checkpoint
    python scrape_table5_xlsx.py 20               # limit to 20 plans
    python scrape_table5_xlsx.py stats            # print stats only
    python scrape_table5_xlsx.py recheck          # reparse already-downloaded XLSs

Output:
    C:\\ORANIM\\temp_xlsx\\<taba>.xlsx              raw HTML/XLS per plan
    C:\\ORANIM\\all_table5_xlsx_results.json        results
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

from playwright.async_api import async_playwright

from parse_table5_xlsx import parse_table5_xlsx, result_to_dict


PLANS_GEOJSON = Path(r"C:\ORANIM\oranim-app\data\plans.geojson")
OUTPUT_FILE = Path(r"C:\ORANIM\all_table5_xlsx_results.json")
TEMP_DIR = Path(r"C:\ORANIM\temp_xlsx")
BROWSER_DATA = r"C:\ORANIM\.browser_data_table5_xlsx"
MAVAT_BASE = "https://mavat.iplan.gov.il"

# Same scope as the existing PDF-based scrape_all_table5_v2.py
RELEVANT_STATUSES = {
    "אישור", "הפקדה", "הפקדה להתנגדויות/השגות",
    "דיון בהתנגדויות ותיקונים", "בבדיקה תכנונית",
    "תכנית עומדת בתנאי סף", "במילוי תנאים להפקדה",
    "תום תקופת הפקדה", "פרסום להפקדה/ביטול/החלטות",
    "נקלטה מקובץ מבאת", "אישור על תנאי",
    "תבע מאושרת", "תבע - טרום אישור", "מאושרת",
    "תחילת תוקף", "בהליך אישור",
}

TEMP_DIR.mkdir(exist_ok=True)
if sys.platform.startswith("win"):
    sys.stdout.reconfigure(encoding="utf-8")


EXPAND_ALL_JS = r"""
() => {
    let clicked = 0;
    document.querySelectorAll('.uk-accordion-title').forEach(t => {
        const parent = t.parentElement;
        if (parent && !parent.classList.contains('uk-open')) {
            try { t.click(); clicked++; } catch(e) {}
        }
    });
    return clicked;
}
"""

XLS_PRESENT_JS = r"""
() => {
    const leads = document.querySelectorAll('.uk-text-lead');
    for (const lead of leads) {
        if ((lead.textContent || '').trim() === 'זכויות והוראות בניה XLS') return true;
    }
    return false;
}
"""


def load_plans() -> List[Dict]:
    with open(PLANS_GEOJSON, "r", encoding="utf-8") as f:
        gj = json.load(f)
    plans: List[Dict] = []
    seen = set()
    for feat in gj["features"]:
        p = feat["properties"]
        status = (p.get("status_mavat") or "").strip()
        agam_id = p.get("agam_id", "")
        plan_name = p.get("plan_name", "")
        taba = p.get("taba", "")
        if not (status in RELEVANT_STATUSES and agam_id and plan_name):
            continue
        if plan_name in seen:
            continue
        seen.add(plan_name)
        try:
            agam_str = str(int(float(agam_id)))
            taba_str = str(int(float(taba))) if taba else ""
        except (ValueError, TypeError):
            continue
        plans.append({
            "plan_number": plan_name,
            "agam_id": agam_str,
            "taba": taba_str,
            "status": status,
        })
    return plans


def load_results() -> List[Dict]:
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("results", [])
    return []


def save_results(results: List[Dict], total_scope: int) -> None:
    def _count(s: str) -> int:
        return sum(1 for r in results if r["status"] == s)
    output = {
        "version": 1,
        "plans_scope": total_scope,
        "total_processed": len(results),
        "success": _count("success"),
        "no_xlsx": _count("no_xlsx"),
        "download_fail": _count("download_fail"),
        "parse_fail": _count("parse_fail"),
        "results": results,
    }
    OUTPUT_FILE.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")


def find_xlsx_path(plan: Dict) -> Optional[Path]:
    """Return existing local XLS path for this plan, or None."""
    candidates = []
    if plan.get("taba"):
        candidates.append(TEMP_DIR / f"{plan['taba']}.xlsx")
    pn = plan.get("plan_number", "").replace("-", "_")
    candidates.append(TEMP_DIR / f"{pn}.xlsx")
    for path in candidates:
        if path.exists() and path.stat().st_size > 500:
            return path
    return None


def build_result(plan: Dict, status: str, xlsx_path: Optional[Path], parse_data: Optional[Dict]) -> Dict:
    out = {
        "plan_number": plan["plan_number"],
        "agam_id": plan["agam_id"],
        "taba": plan.get("taba", ""),
        "status": status,
        "xlsx_path": str(xlsx_path) if xlsx_path else None,
    }
    if parse_data:
        out.update(parse_data)
    return out


async def _wait_render(page, timeout_s: float) -> bool:
    """Wait until the plan SPA renders its accordion. True if rendered in time."""
    try:
        await page.wait_for_selector(".uk-accordion-title", timeout=int(timeout_s * 1000))
        return True
    except Exception:
        return False


async def _warm_homepage(page) -> None:
    """Prime Mavat's SV4 SPA. A direct plan-page load can return only the
    ~215-char nav shell — the Angular app bundle bootstraps only after a homepage
    visit (diagnosed 2026-07). Visiting '/' first makes subsequent plan pages
    render."""
    try:
        await page.goto(f"{MAVAT_BASE}/", wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(6)
    except Exception:
        pass


async def download_xlsx(page, plan: Dict) -> Optional[Path]:
    """Navigate to Mavat plan, expand accordions, download XLS. Returns path or None."""
    agam = plan["agam_id"]
    taba = plan.get("taba") or plan["plan_number"]
    target = TEMP_DIR / f"{taba}.xlsx"
    if target.exists() and target.stat().st_size > 500:
        return target

    url = f"{MAVAT_BASE}/SV4/1/{agam}/310"
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except Exception:
        pass
    # Wait for the SPA to render (it can take ~16s). If it stays on the cold
    # ~215-char shell, warm the homepage and retry once with a longer budget.
    # None on failure keeps the existing contract (caller treats it as no_xlsx),
    # but the warm-up retry avoids the false no_xlsx we used to get on cold pages.
    if not await _wait_render(page, 14):
        await _warm_homepage(page)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        except Exception:
            pass
        if not await _wait_render(page, 28):
            return None

    # Expand accordions (nested) and poll for the XLS link (loads lazily).
    has_xls = False
    for _ in range(6):
        try:
            await page.evaluate(EXPAND_ALL_JS)
        except Exception:
            pass
        try:
            has_xls = await page.evaluate(XLS_PRESENT_JS)
        except Exception:
            has_xls = False
        if has_xls:
            break
        await asyncio.sleep(2)
    if not has_xls:
        return None

    # Locate the download icon and click it
    img_loc = page.locator(
        "xpath=//span[@class='uk-text-lead doc-info' and contains(text(),'זכויות והוראות בניה XLS')]"
        "/ancestor::*[@role='row'][1]//img[contains(@title,'אקסל')]"
    ).first
    try:
        await img_loc.scroll_into_view_if_needed(timeout=10000)
        await asyncio.sleep(0.5)
        async with page.expect_download(timeout=30000) as dl_info:
            await img_loc.click(timeout=8000, force=True)
        dl = await dl_info.value
        await dl.save_as(str(target))
        if target.exists() and target.stat().st_size > 500:
            return target
    except Exception as e:
        return None
    return None


async def process_plan(page, plan: Dict) -> Dict:
    """Download (or reuse cached) XLS, parse, build result."""
    existing = find_xlsx_path(plan)
    if existing:
        parsed = parse_table5_xlsx(existing)
        if parsed is None:
            return build_result(plan, "parse_fail", existing, None)
        return build_result(plan, "success", existing, result_to_dict(parsed))

    path = await download_xlsx(page, plan)
    if path is None:
        return build_result(plan, "no_xlsx", None, None)

    parsed = parse_table5_xlsx(path)
    if parsed is None:
        return build_result(plan, "parse_fail", path, None)
    return build_result(plan, "success", path, result_to_dict(parsed))


def print_stats(results: List[Dict]) -> None:
    by_status: Dict[str, int] = {}
    for r in results:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    print(f"Total processed: {len(results)}")
    for k, v in sorted(by_status.items(), key=lambda kv: -kv[1]):
        print(f"  {k}: {v}")
    succ = [r for r in results if r["status"] == "success"]
    with_rental = [r for r in succ if r.get("rental_units", 0) > 0]
    with_cond = [r for r in succ if r.get("conditional_units", 0) > 0]
    with_height = [r for r in succ if r.get("max_height_m", 0) > 0]
    with_floors = [r for r in succ if r.get("max_floors", 0) > 0]
    print(f"\nWith rental_units>0: {len(with_rental)}")
    print(f"With conditional_units>0: {len(with_cond)}")
    print(f"With max_height>0: {len(with_height)}")
    print(f"With max_floors>0: {len(with_floors)}")


async def main() -> None:
    args = sys.argv[1:]

    plans = load_plans()
    # Process newest plans first (high taba) — they're more likely to have XLS exports
    def _taba_key(p):
        try:
            return int(p.get("taba") or 0)
        except (ValueError, TypeError):
            return 0
    plans.sort(key=_taba_key, reverse=True)
    print(f"Plans in scope: {len(plans)}")

    if args and args[0] == "stats":
        results = load_results()
        print_stats(results)
        return

    if args and args[0] == "recheck":
        # Reparse cached XLSs only — does NOT mark non-cached plans as processed.
        print("Re-parsing already-downloaded XLSs (no browser)...")
        results: List[Dict] = []
        for plan in plans:
            existing = find_xlsx_path(plan)
            if not existing:
                continue
            parsed = parse_table5_xlsx(existing)
            if parsed is None:
                results.append(build_result(plan, "parse_fail", existing, None))
                continue
            results.append(build_result(plan, "success", existing, result_to_dict(parsed)))
        save_results(results, len(plans))
        print_stats(results)
        return

    limit = None
    if args:
        try:
            limit = int(args[0])
        except ValueError:
            pass

    existing_results = load_results()
    processed = {r["plan_number"] for r in existing_results}
    to_do = [p for p in plans if p["plan_number"] not in processed]
    if limit is not None:
        to_do = to_do[:limit]
    if not to_do:
        print("Nothing to do (all plans already processed).")
        print_stats(existing_results)
        return

    print(f"Plans to process this run: {len(to_do)}")
    print(f"Already in results: {len(existing_results)}")

    results = list(existing_results)
    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            BROWSER_DATA, headless=False,
            viewport={"width": 1400, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            accept_downloads=True,
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        # Warm session
        try:
            await page.goto(f"{MAVAT_BASE}/SV4/1/{to_do[0]['agam_id']}/310",
                            wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(10)
        except Exception:
            pass

        consecutive_errors = 0
        start_time = time.time()
        for i, plan in enumerate(to_do):
            try:
                r = await process_plan(page, plan)
            except Exception as e:
                r = build_result(plan, "download_fail", None, None)
                r["error"] = str(e)[:200]

            results.append(r)
            elapsed = time.time() - start_time
            print(f"  [{i+1}/{len(to_do)}] {plan['plan_number']:20s} taba={plan.get('taba',''):>8} "
                  f"-> {r['status']:14s} rental={r.get('rental_units','-')} cond={r.get('conditional_units','-')} "
                  f"H={r.get('max_height_m','-')} F={r.get('max_floors','-')} "
                  f"({elapsed:.0f}s)",
                  flush=True)

            if r["status"] in ("download_fail",):
                consecutive_errors += 1
            else:
                consecutive_errors = 0

            if consecutive_errors >= 5:
                print("  5 consecutive errors — pausing 30s...")
                await asyncio.sleep(30)
                consecutive_errors = 0

            if (i + 1) % 10 == 0:
                save_results(results, len(plans))

            await asyncio.sleep(1.5)

        save_results(results, len(plans))
        await ctx.close()

    print_stats(results)


if __name__ == "__main__":
    asyncio.run(main())
