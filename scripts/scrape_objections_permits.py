"""
scrape_objections_permits.py
----------------------------
Event-driven discovery of building permits that are OPEN FOR OBJECTIONS
(הקלות / שימוש חורג, published under §149). The regular pipelines are blind to
these because they pull from a known list (taba / tama38 address); a permit like
2025/0569.00 (עמק רפאים 50, status 'תחילת פרסום הבקשה להקלה') sits on a plot/plan
we don't track, so only an enumeration keyed on the publication event finds it.

Strategy (mode: "recent tiks + streets that have active plans"):
  1. Build the street list from active plans (plan polygons ∩ roads.geojson),
     reusing scrape_address_fallback's spatial + street-matching helpers.
  2. For each street: street-only YK search → read the full results table.
  3. Keep rows whose status is a publication/objection status AND whose
     status_date is within --since-days.
  4. Per kept tik: open detail (address, gush/helka) and the 'תהליך' tab
     (expand 'פרסום/ משלוח הודעות' accordion) → objection deadline =
     planned date of 'השלמת פרסום לפי סעיף 149 לחוק'.
  5. Best-effort geocode (house number interpolated along the road segment) and
     attribute to the sub-neighborhood/taba of the street's source plan(s).
  6. Write objections_permits.json (+ copy into oranim-app/data).

Read-only against YK. Akamai WAF throttles bursts → runs slowly, saves as it goes.

Usage:
    python scrape_objections_permits.py --dry-run          # street list only, no YK
    python scrape_objections_permits.py --street "עמק רפאים"  # one street (validation)
    python scrape_objections_permits.py --limit 20         # first N streets
    python scrape_objections_permits.py --since-days 120   # recency window (default 120)
    python scrape_objections_permits.py                    # full fast run
"""
import argparse
import asyncio
import json
import re
import shutil
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from playwright.async_api import async_playwright
from shapely.geometry import shape
from pyproj import Transformer

_ITM = Transformer.from_crs("EPSG:2039", "EPSG:4326", always_xy=True)  # ITM → WGS84

sys.path.insert(0, r"C:\ORANIM")
from scrape_all_permits import SITE_URL, BROWSER_DATA, EXTRACT_ALL_JS
from scrape_address_fallback import (
    _load_roads, _intersect_streets, _street_variants, _best_suggestion_match,
)
from git_sync import commit_and_push_after_write

PLANS_GEOJSON = r"C:\ORANIM\oranim-app\data\plans.geojson"
DISTRICT_GEOJSON = r"C:\ORANIM\oranim-app\data\district_oranim.geojson"
OUT_PATH      = r"C:\ORANIM\objections_permits.json"
APP_COPY      = r"C:\ORANIM\oranim-app\data\objections_permits.json"
SCANNED_PATH  = r"C:\ORANIM\objections_scanned_streets.json"  # resume sidecar

# Statuses where the objection window is (or is about to be) OPEN. NOTE:
# 'תום תקופת פרסום' (publication period ended) is intentionally EXCLUDED — once
# it's reached, objections can no longer be submitted, so we don't collect them.
PUBLICATION_STATUSES = {
    "תחילת פרסום הבקשה להקלה",
    "ממתין לאישור נוסח פרסום",
}
# Looser keyword fallback in case YK varies the wording (still excludes 'תום').
PUBLICATION_KEYWORDS = ("פרסום הבקשה להקלה", "פרסום הבקשה לשימוש חורג")
CLOSED_STATUS = "תום תקופת פרסום"

REJECTED_STATUSES = {"נגנזה", "נדחתה", "נגנזה/נדחתה"}
EXCLUDED_PLAN_TYPES = {"תשתיות", "מוסתר"}

DELAY = 4  # base seconds between YK actions (be gentle with the WAF)
SOFT_BLOCK_LIMIT = 8  # consecutive form-load failures (no_input) → assume throttled, stop


# ───────────────────────── detail / process extraction JS ──────────────────────

# Pull address + gush/helka label pairs from the open permit detail page.
DETAIL_JS = r"""() => {
    const out = {};
    const WANT = {'כתובת':'address','גוש':'gush','חלקה':'helka','תא שטח':'cell',
                  'שכונה':'neighborhood','סוג בקשה':'request_type','מהות בקשה':'request_description',
                  'מהות הבקשה':'request_description','סטטוס':'status'};
    const els = Array.from(document.querySelectorAll('div, span, td, label, p'));
    for (const el of els) {
        const t = (el.textContent || '').trim();
        if (t.length > 30) continue;
        const sib = el.nextElementSibling;
        const sibText = sib ? (sib.textContent || '').trim() : '';
        if (WANT[t] && sibText && sibText.length < 120 && !out[WANT[t]]) out[WANT[t]] = sibText;
    }
    return out;
}"""

# 'נתוני מקום' tab: ITM center coords (most accurate position) + address + gush/helka.
PLACE_JS = r"""() => {
    const b = document.body.innerText || '';
    const out = {};
    let m = b.match(/קואורדינטות מרכז \(אורך\)\s*([0-9]{5,7})/);  if (m) out.itm_x = parseInt(m[1]);
    m = b.match(/קואורדינטות מרכז \(רוחב\)\s*([0-9]{5,7})/);      if (m) out.itm_y = parseInt(m[1]);
    m = b.match(/\nרחוב\s*\n([^\n]+)/);                            if (m) out.place_address = m[1].trim();
    for (const t of Array.from(document.querySelectorAll('table'))) {
        const rows = Array.from(t.querySelectorAll('tr'));
        if (rows.length < 2) continue;
        const hdr = Array.from(rows[0].querySelectorAll('th,td')).map(c=>c.textContent.trim());
        if (hdr[0] && hdr[0].indexOf('גוש') === 0 && hdr[1] && hdr[1].indexOf('חלקה') === 0) {
            const cells = Array.from(rows[1].querySelectorAll('td')).map(c=>c.textContent.trim());
            const g = (cells[0]||'').replace(/^גוש/,'').trim(), h = (cells[1]||'').replace(/^חלקה/,'').trim();
            if (/^\d+$/.test(g)) out.gush = g;
            if (/^\d+$/.test(h)) out.helka = h;
            break;
        }
    }
    return out;
}"""

# After expanding the publication accordion, read its stage rows.
PROCESS_JS = r"""() => {
    const dateRe = /\d{2}\/\d{2}\/\d{4}/;
    const rows = [];
    for (const tr of Array.from(document.querySelectorAll('tr'))) {
        const cells = Array.from(tr.querySelectorAll('td,th')).map(c => c.textContent.replace(/\s+/g,' ').trim());
        if (!cells.length) continue;
        const joined = cells.join(' | ');
        if (dateRe.test(joined) || /פרסום|סעיף 149|השלמת פרסום|הפקדה/.test(joined)) rows.push(cells);
    }
    return rows;
}"""


def _clean(text, label):
    """Kendo cells arrive as 'labelvalue' (e.g. 'סטטוסהופק...'); strip the label."""
    t = (text or "").strip()
    return t[len(label):].strip() if t.startswith(label) else t


def _parse_date(s):
    if not s:
        return None
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", s)
    if not m:
        return None
    try:
        return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1))).date()
    except ValueError:
        return None


def is_publication_status(status: str) -> bool:
    s = (status or "").strip()
    if s == CLOSED_STATUS:
        return False  # objection window already closed — never collect
    if s in PUBLICATION_STATUSES:
        return True
    return any(k in s for k in PUBLICATION_KEYWORDS)


# ───────────────────────── street list from active plans ──────────────────────

def build_street_index():
    """Return {street_name: {'subs': set, 'tabas': set}} for streets that touch
    an active plan (excludes תשתיות/מוסתר/rejected)."""
    with open(PLANS_GEOJSON, encoding="utf-8") as f:
        plans = json.load(f)
    idx = defaultdict(lambda: {"subs": set(), "tabas": set()})
    seen_taba = set()
    for feat in plans.get("features", []):
        p = feat.get("properties") or {}
        ptype = (p.get("plan_type") or "").strip()
        if ptype in EXCLUDED_PLAN_TYPES:
            continue
        status = (p.get("status_mavat") or "").strip()
        if status in REJECTED_STATUSES:
            continue
        taba = str(p.get("taba") or "").strip()
        geom = feat.get("geometry")
        if not geom:
            continue
        try:
            g = shape(geom)
        except Exception:
            continue
        subs = {s.strip() for s in str(p.get("sub_neighborhood") or "").split(",") if s.strip()}
        if taba:
            _PLAN_POLYS.append({"taba": taba, "subs": subs, "geom": g})
        for street in _intersect_streets(g).keys():
            idx[street]["subs"].update(subs)
            if taba:
                idx[street]["tabas"].add(taba)
        seen_taba.add(taba)
    return dict(idx)


# ───────────────────────── geocoding (best effort) ────────────────────────────

BUILDINGS_GEOJSON = r"C:\ORANIM\oranim-app\data\buildings.geojson"
_buildings_index = None

def _load_buildings_index():
    """{(street, house_digits): [[lng,lat], ...]} from buildings.geojson (point
    footprints with street + house_num) — lets us place a marker on the actual
    BUILDING at the address instead of on the road centerline."""
    global _buildings_index
    if _buildings_index is None:
        _buildings_index = {}
        try:
            gj = json.load(open(BUILDINGS_GEOJSON, encoding="utf-8"))
            for f in gj.get("features", []):
                p = f.get("properties") or {}
                st = (p.get("street") or "").strip()
                hn = re.sub(r"[^\d]", "", str(p.get("house_num") or ""))
                if not st or not hn:
                    continue
                c = (f.get("geometry") or {}).get("coordinates")
                if c and len(c) >= 2:
                    _buildings_index.setdefault((st, hn), []).append([c[0], c[1]])
        except Exception:
            pass
    return _buildings_index


def geocode(street: str, house) -> list | None:
    """Return [lng,lat] for street+house. Prefers the BUILDING point (exact
    address match in buildings.geojson, trying street variants), so the marker
    sits on the building — not 'on the road'. Falls back to interpolating along
    the road segment only when no building matches."""
    house_digits = re.sub(r"[^\d]", "", str(house)) if house else ""
    # 1) Building match by address (street + house), with street-name variants.
    if house_digits:
        bidx = _load_buildings_index()
        for variant in _street_variants(street):
            hits = bidx.get((variant, house_digits))
            if hits:
                return hits[0]
    # 2) Fallback: interpolate along the road segment.
    try:
        house = int(house_digits) if house_digits else None
    except (ValueError, TypeError):
        house = None
    best = None
    for rf in _load_roads()["features"]:
        if (rf["properties"].get("street") or "") != street:
            continue
        geom = rf["geometry"]
        if geom["type"] == "LineString":
            lines = [geom["coordinates"]]
        elif geom["type"] == "MultiLineString":
            lines = geom["coordinates"]
        else:
            continue
        p = rf["properties"]
        ranges = [(p.get("fromleft"), p.get("toleft")), (p.get("fromright"), p.get("toright"))]
        for coords in lines:
            if len(coords) < 2:
                continue
            for fr, to in ranges:
                if house and fr and to:
                    lo, hi = min(fr, to), max(fr, to)
                    if lo <= house <= hi and hi > lo:
                        frac = (house - lo) / (hi - lo)
                        i = min(int(frac * (len(coords) - 1)), len(coords) - 2)
                        a, b = coords[i], coords[i + 1]
                        return [a[0] + (b[0] - a[0]) * 0.5, a[1] + (b[1] - a[1]) * 0.5]
            if best is None:
                mid = coords[len(coords) // 2]
                best = [mid[0], mid[1]]
    return best


# District boundary — drop permits geocoded outside our area (streets can run out of scope).
_district_geom = None

def in_district(lnglat) -> bool:
    """True if the point is inside the Oranim district. Keeps records with no
    coordinates (can't verify) — they're hidden in the app anyway."""
    global _district_geom
    if not lnglat:
        return True
    if _district_geom is None:
        try:
            gj = json.load(open(DISTRICT_GEOJSON, encoding="utf-8"))
            _district_geom = shape(gj["features"][0]["geometry"])
        except Exception:
            _district_geom = False
    if _district_geom is False:
        return True
    from shapely.geometry import Point
    return _district_geom.contains(Point(lnglat[0], lnglat[1]))


# Plan polygons for precise point-in-polygon attribution (built by build_street_index).
_PLAN_POLYS = []  # list of {taba, subs:set, geom}

def attribute_point(lnglat):
    """Return {'subs':[...], 'tabas':[...]} of active plans whose polygon contains
    the point. Precise alternative to the street-level union (which over-attributes)."""
    if not lnglat or not _PLAN_POLYS:
        return None
    from shapely.geometry import Point
    pt = Point(lnglat[0], lnglat[1])
    subs, tabas = set(), set()
    for rec in _PLAN_POLYS:
        try:
            if rec["geom"].contains(pt):
                tabas.add(rec["taba"])
                subs.update(rec["subs"])
        except Exception:
            continue
    if not tabas:
        return None
    return {"subs": sorted(subs), "tabas": sorted(tabas)}


# ───────────────────────── YK interactions ────────────────────────────────────

async def _goto(page):
    try:
        await page.goto(SITE_URL, wait_until="networkidle", timeout=30000)
    except Exception:
        pass
    await asyncio.sleep(DELAY + 2)


async def ensure_search_form(page, min_inputs):
    """Return the search form's visible inputs, REUSING the already-loaded SPA
    when possible. YK's Akamai WAF throttles by full page load (~38), so we only
    page.goto() when the form isn't already present — this keeps the whole run at
    ~1 page load instead of one per search. The results open in a new tab, so the
    original search tab (pages[0]) normally still holds a live form to refill.
    Returns (inputs_list, 'reused'|'loaded'|'blocked')."""
    inputs = await page.query_selector_all("input:visible")
    if len(inputs) >= min_inputs:
        return inputs, "reused"
    await _goto(page)
    if "Access Denied" in (await page.content()):
        return [], "blocked"
    return await page.query_selector_all("input:visible"), "loaded"


async def search_street(page, ctx, street: str):
    """Street-only search → returns (status, active_page, rows)."""
    inputs, how = await ensure_search_form(page, 5)
    if how == "blocked":
        return ("waf_blocked", page, [])
    if len(inputs) < 5:
        return ("no_input", page, [])
    street_input = inputs[4]
    chosen = None
    for variant in _street_variants(street):
        await street_input.click()
        await street_input.fill("")
        await asyncio.sleep(0.4)
        await street_input.type(variant, delay=90)
        await asyncio.sleep(2)
        items = await page.query_selector_all("li.k-list-item")
        if not items:
            continue
        texts = [(await it.inner_text()).strip() for it in items]
        bi = _best_suggestion_match(variant, texts)
        if bi is None:
            bi = 0
        await items[bi].click()
        await asyncio.sleep(1)
        chosen = texts[bi]
        break
    if chosen is None:
        return ("no_suggestion", page, [])
    btn = await page.query_selector('button.search-btn') or \
          await page.query_selector('button:has-text("אתר תיק רישוי")')
    if not btn:
        return ("no_button", page, [])
    await btn.click()
    await asyncio.sleep(DELAY + 8)
    active = ctx.pages[-1] if len(ctx.pages) > 1 else page
    rows = await active.evaluate(EXTRACT_ALL_JS)
    # EXTRACT_ALL_JS returns dicts with raw 'labelvalue' fields → clean them
    cleaned = []
    for r in rows:
        cleaned.append({
            "file_number": _clean(r.get("file_number"), "מספר תיק"),
            "status": _clean(r.get("status"), "סטטוס"),
            "status_date": _clean(r.get("status_date"), "תאריך סטטוס"),
            "request_type": _clean(r.get("request_type"), "סוג בקשה"),
            "request_description": _clean(r.get("request_description"), "מהות הבקשה"),
        })
    return ("ok", active, cleaned)


async def _click_text(page, text, contains=False, selector='a, span, li, div, button, h3, h4'):
    try:
        handles = await page.query_selector_all(selector)
        for h in handles:
            try:
                t = (await h.inner_text()).strip()
            except Exception:
                continue
            if (text in t) if contains else (t == text):
                await h.click()
                return True
    except Exception:
        pass
    return False


def _extract_deadline(rows):
    """From תהליך rows, derive (deadline, check_date).
    The objection deadline is, in order of authority:
      1. 'השלמת פרסום לפי סעיף 149 לחוק' — the original/common label (keep PRIMARY so the
         many already-verified permits are unaffected), latest cycle.
      2. 'תום מועד להגשת התנגדויות' — the literal end-of-objection-period row, used by permits
         that have NO 'השלמת פרסום' row (e.g. 2019/0608.00, 2023/0294.00). Latest cycle.
    'latest cycle' = max parsed date, so a republished permit's current window wins over an
    old one. Each row's date = its first dd/mm/yyyy (= מועד מתוכנן, the planned date)."""
    def row_date(cells):
        for c in cells:
            m = re.search(r"\d{2}/\d{2}/\d{4}", c)
            if m:
                return m.group(0)
        return None

    def latest(ds):
        if not ds:
            return None
        try:
            return max(ds, key=lambda x: datetime.strptime(x, "%d/%m/%Y"))
        except ValueError:
            return ds[-1]

    pub_done, obj_end, check_date = [], [], None
    for cells in rows:
        joined = " ".join(cells)
        d = row_date(cells)
        if "השלמת פרסום" in joined and d:
            pub_done.append(d)
        elif "תום מועד להגשת התנגדויות" in joined and d:
            obj_end.append(d)
        elif "הליך הפרסום" in joined and d:
            check_date = d
    return (latest(pub_done) or latest(obj_end)), check_date


async def fetch_detail_and_deadline(page, ctx, tik: str) -> dict:
    """tik search → detail (address/gush) + תהליך tab (deadline)."""
    out = {}
    inputs, how = await ensure_search_form(page, 2)
    if len(inputs) >= 2:
        num = tik.split("/")[1].strip()
        year = tik.split("/")[0].strip()
        await inputs[0].click(); await inputs[0].fill(num); await asyncio.sleep(0.4)
        await inputs[1].click(); await inputs[1].fill(year); await asyncio.sleep(0.4)
        btn = await page.query_selector('button.search-btn') or \
              await page.query_selector('button:has-text("אתר תיק רישוי")')
        if btn:
            await btn.click(); await asyncio.sleep(DELAY + 7)
    active = ctx.pages[-1] if len(ctx.pages) > 1 else page
    # Deterministic deep-link to the permit page (active.url is unreliable under reuse-SPA).
    out["yk_url"] = f"https://ykpubdata.jerusalem.muni.il/#/Details?TikNum={tik}&SystemCode=26400046&Page=BakashalInfo"
    try:
        out.update(await active.evaluate(DETAIL_JS))
    except Exception:
        pass
    # נתוני מקום tab — ITM center coords (precise position) + full address + gush/helka.
    try:
        await _click_text(active, "נתוני מקום")
        await asyncio.sleep(DELAY)
        out.update(await active.evaluate(PLACE_JS))
    except Exception:
        pass
    # Navigate to תהליך tab, then expand ONLY the publication panel.
    # The תהליך tab is a Kendo PanelBar (single-expand): clicking one header
    # collapses the others, and panel content loads lazily — so target the
    # 'פרסום/ משלוח הודעות' k-link specifically and wait for its rows to render.
    await _click_text(active, "תהליך")
    await asyncio.sleep(DELAY + 1)
    if not await _click_text(active, "פרסום/ משלוח", contains=True, selector="span.k-link"):
        await _click_text(active, "פרסום/ משלוח", contains=True)
    await asyncio.sleep(DELAY + 3)  # lazy content load
    try:
        rows = await active.evaluate(PROCESS_JS)
    except Exception:
        rows = []
    deadline, check_date = _extract_deadline(rows)
    # Some permits keep the deadline row in a DIFFERENT PanelBar section (not
    # 'פרסום/ משלוח') — its rows aren't in the DOM until that section is expanded.
    # If the fast path found nothing, sweep every section, accumulating rows, until
    # a deadline appears. (Rare slow path — only the exceptions pay for it.)
    if not deadline:
        try:
            headers = await active.query_selector_all("span.k-link")
            allrows = list(rows)
            for idx in range(len(headers)):
                hs = await active.query_selector_all("span.k-link")
                if idx >= len(hs):
                    break
                try:
                    await hs[idx].click()
                except Exception:
                    continue
                await asyncio.sleep(DELAY + 1)
                try:
                    allrows += await active.evaluate(PROCESS_JS)
                except Exception:
                    pass
                deadline, c2 = _extract_deadline(allrows)
                if deadline:
                    check_date = check_date or c2
                    break
        except Exception:
            pass
    out["deadline_publish"] = deadline
    out["deadline_check"] = check_date
    return out


# ───────────────────────── main ───────────────────────────────────────────────

def load_out():
    if Path(OUT_PATH).exists():
        with open(OUT_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_out(data):
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    try:
        shutil.copy(OUT_PATH, APP_COPY)
    except Exception:
        pass


def load_scanned():
    if Path(SCANNED_PATH).exists():
        try:
            with open(SCANNED_PATH, encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()
    return set()


def save_scanned(s):
    with open(SCANNED_PATH, "w", encoding="utf-8") as f:
        json.dump(sorted(s), f, ensure_ascii=False)


APP_REPO = r"C:\ORANIM\oranim-app"

def git_push_progress(label):
    """Commit + push ONLY data/objections_permits.json (no app.js → no CI conflict),
    so the live app reflects scan progress. Called every N streets. Best-effort —
    network/rebase hiccups are logged, never fatal to the scrape."""
    try:
        # Via git_sync, never raw git: an unchecked `pull --rebase` on a single-line
        # JSON file wedged this repo mid-rebase for three days (2026-08-23).
        # commit_and_push_after_write aborts the rebase and reports False instead,
        # and handles the "nothing to commit" case itself.
        commit_and_push_after_write(
            "data/objections_permits.json",
            f"data: objections progress ({label})",
            APP_REPO)
        print(f"     ⇪ pushed data ({label})", flush=True)
    except Exception as e:
        print(f"     git push skipped: {str(e)[:70]}", flush=True)


async def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--street")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--since-days", type=int, default=120)
    ap.add_argument("--fresh", action="store_true", help="ignore the resume sidecar and rescan all")
    args = ap.parse_args()

    print("Building street index from active plans...")
    idx = build_street_index()
    streets = sorted(idx.keys())
    if args.street:
        streets = [args.street]
    elif args.limit:
        streets = streets[:args.limit]
    # Resume: skip streets already scanned in a prior run (full runs only).
    scanned = set() if (args.fresh or args.street) else load_scanned()
    if args.fresh and Path(SCANNED_PATH).exists():
        Path(SCANNED_PATH).unlink()
    if scanned:
        before = len(streets)
        streets = [s for s in streets if s not in scanned]
        print(f"Resume: skipping {before - len(streets)} already-scanned streets.")
    print(f"Streets to scan: {len(streets)} (of {len(idx)} total streets touching active plans)")

    if args.dry_run:
        for s in streets[:60]:
            print(f"  {s}  subs={sorted(idx.get(s,{}).get('subs',[]))}  tabas={len(idx.get(s,{}).get('tabas',[]))}")
        if len(streets) > 60:
            print(f"  ... +{len(streets)-60} more")
        return

    cutoff = (datetime.now().date() - timedelta(days=args.since_days))
    results = load_out()
    # Drop any record whose status is no longer open for objections (e.g. it moved
    # to 'תום תקופת פרסום' since it was scraped) — self-cleans the file each run.
    _before = len(results)
    results = {k: v for k, v in results.items() if is_publication_status(v.get("status", ""))}
    if len(results) < _before:
        print(f"Pruned {_before - len(results)} records whose objection window has closed.")
    found = 0

    async with async_playwright() as p:
        from yk_profile_lock import hold_yk_profile
        hold_yk_profile("scrape_objections_permits.py")  # one YK-profile user at a time
        ctx = await p.chromium.launch_persistent_context(
            BROWSER_DATA, headless=False, viewport={"width": 1366, "height": 950},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        consec_no_input = 0

        for i, street in enumerate(streets):
            print(f"[{i+1}/{len(streets)}] {street}...", end=" ", flush=True)
            try:
                status, active, rows = await search_street(page, ctx, street)
            except Exception as e:
                print(f"ERROR {str(e)[:60]}")
                rows, status = [], "error"
            if status == "waf_blocked":
                print("!! WAF Access Denied — stopping. Re-run to resume (scanned streets are skipped).")
                break
            # Soft-block detection: YK throttles by IP after ~30-40 page loads and
            # then serves the search page WITHOUT the form (status 'no_input').
            # A run of these means we're throttled — stop so a later run (after a
            # cooldown) can retry them. Crucially, do NOT mark no_input/no_button/
            # error streets as scanned, or resume would skip un-searched streets.
            if status in ("no_input", "no_button"):
                consec_no_input += 1
                print(status + (f" (soft-block streak {consec_no_input})" if consec_no_input >= 3 else ""))
                if consec_no_input >= SOFT_BLOCK_LIMIT:
                    print(f"!! {consec_no_input} consecutive form-load failures — likely throttled. "
                          "Stopping; re-run after a cooldown to resume.")
                    break
                while len(ctx.pages) > 1:
                    await ctx.pages[-1].close()
                page = ctx.pages[0]
                await asyncio.sleep(DELAY)
                continue
            consec_no_input = 0
            # Mark scanned only when the form actually loaded (real result).
            if not args.street and status in ("ok", "no_suggestion"):
                scanned.add(street)
                save_scanned(scanned)
            if status != "ok":
                print(status)
            else:
                # filter: publication status + recent
                hits = []
                for r in rows:
                    if not is_publication_status(r["status"]):
                        continue
                    d = _parse_date(r["status_date"])
                    if d and d < cutoff:
                        continue
                    hits.append(r)
                print(f"{len(rows)} rows, {len(hits)} open-for-objections")
                for r in hits:
                    tik = r["file_number"]
                    if not tik:
                        continue
                    # close extra tabs before per-tik navigation
                    while len(ctx.pages) > 1:
                        await ctx.pages[-1].close()
                    page = ctx.pages[0]
                    detail = await fetch_detail_and_deadline(page, ctx, tik)
                    addr = (detail.get("address") or detail.get("place_address") or "").strip()
                    hm = re.search(r"(\d+)", addr)
                    house = hm.group(1) if hm else None
                    # ITM center coords from נתוני מקום = most accurate; else address geocode.
                    if detail.get("itm_x") and detail.get("itm_y"):
                        _lng, _lat = _ITM.transform(detail["itm_x"], detail["itm_y"])
                        lnglat = [round(_lng, 6), round(_lat, 6)]
                    else:
                        lnglat = geocode(street, house)
                    if lnglat and not in_district(lnglat):
                        print(f"     ↷ skip (out of district): {tik}  {addr}")
                        continue
                    # Precise attribution by point-in-polygon; fall back to the
                    # street-level union only when the point isn't inside any plan.
                    attr = attribute_point(lnglat) or {
                        "subs": sorted(idx.get(street, {}).get("subs", [])),
                        "tabas": sorted(idx.get(street, {}).get("tabas", [])),
                    }
                    desc = (r["request_description"] or detail.get("request_description") or "")
                    desc = re.sub(r"'{2,}", '"', desc)  # YK quote artifact: ממ''''ד → ממ"ד
                    rec = {
                        "tik": tik,
                        "status": r["status"],
                        "status_date": r["status_date"],
                        "request_type": r["request_type"] or detail.get("request_type"),
                        "request_description": desc,
                        "address": addr,
                        "street": street,
                        "house": house,
                        "gush": detail.get("gush"),
                        "helka": detail.get("helka"),
                        "deadline_publish": detail.get("deadline_publish"),
                        "deadline_check": detail.get("deadline_check"),
                        "yk_url": detail.get("yk_url"),
                        "itm": ([detail["itm_x"], detail["itm_y"]] if detail.get("itm_x") and detail.get("itm_y") else None),
                        "subs": attr["subs"],
                        "tabas": attr["tabas"],
                        "lnglat": lnglat,
                        "scraped_at": datetime.now().isoformat(),
                    }
                    results[tik] = rec
                    found += 1
                    print(f"     ✓ {tik}  {addr}  deadline={rec['deadline_publish']}")
                    save_out(results)
            while len(ctx.pages) > 1:
                await ctx.pages[-1].close()
            page = ctx.pages[0]
            # Push progress to the live app every 50 streets.
            if not args.street and (i + 1) % 50 == 0:
                git_push_progress(f"{len(scanned)} scanned, {found} found")
            await asyncio.sleep(DELAY)

        save_out(results)
        if not args.street:
            save_scanned(scanned)
            git_push_progress(f"{len(scanned)} scanned, {found} found (batch end)")
        await ctx.close()

    print(f"\nDone. Open-for-objections permits found: {found}. Output: {OUT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
