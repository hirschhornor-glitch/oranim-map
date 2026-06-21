"""Probe: capture the JSON API calls the YK TikDetails page makes, so we can
fetch permit details (status/taba/description) over HTTP instead of DOM scraping."""
import asyncio, json, sys
from playwright.async_api import async_playwright

BROWSER_DATA = r"C:\ORANIM\.browser_data_jlm"
TIK = sys.argv[1] if len(sys.argv) > 1 else "2024/0390.00"
URL = f"https://ykpubdata.jerusalem.muni.il/#/TikDetails?TikNum={TIK}&SystemCode=26400046"


async def main():
    sys.stdout.reconfigure(encoding="utf-8")
    calls = []
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(
            BROWSER_DATA, headless=False, viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        async def on_response(resp):
            u = resp.url
            req = resp.request
            is_data = (req.method == "POST") or ("jerusalem.muni.il" in u and not any(
                u.lower().endswith(e) for e in (".js", ".css", ".html", ".png", ".jpg", ".svg", ".woff", ".woff2", ".ico", ".map")))
            if is_data and "go-mpulse" not in u and "akstat" not in u:
                post = None
                try:
                    post = req.post_data
                except Exception:
                    pass
                body = None
                try:
                    body = await resp.text()
                except Exception:
                    pass
                calls.append({"url": u, "method": req.method, "status": resp.status,
                              "post": post, "body": (body or "")[:1500]})

        ctx.on("response", lambda r: asyncio.create_task(on_response(r)))
        # use the SEARCH flow (direct hash-URL doesn't trigger the data load)
        year, num = TIK.split("/")[0].strip(), TIK.split("/")[1].strip()
        await page.goto("https://ykpubdata.jerusalem.muni.il/#/?SystemCode=26400046",
                        wait_until="networkidle", timeout=30000)
        await asyncio.sleep(3)
        inputs = await page.query_selector_all("input:visible")
        await inputs[0].click(); await inputs[0].fill(num); await asyncio.sleep(0.4)
        await inputs[1].click(); await inputs[1].fill(year); await asyncio.sleep(0.4)
        btn = await page.query_selector('button.search-btn') or \
              await page.query_selector('button:has-text("אתר תיק רישוי")')
        await btn.click()
        await asyncio.sleep(6)
        active = ctx.pages[-1] if len(ctx.pages) > 1 else page
        for label in ["נתוני מקום", "תהליך", "תיאור הבקשה"]:
            try:
                el = await active.query_selector(f'text="{label}"')
                if el:
                    await el.click(); await asyncio.sleep(3)
            except Exception:
                pass
        await asyncio.sleep(3)
        await ctx.close()

    print(f"captured {len(calls)} API calls for tik {TIK}\n")
    for c in calls:
        print("="*70)
        print(c["method"], c["status"], c["url"])
        if c["post"]:
            print("POST:", c["post"][:400])
        print("BODY:", c["body"][:800])
    json.dump(calls, open(r"C:\ORANIM\probe_tik_api_out.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)


if __name__ == "__main__":
    asyncio.run(main())
