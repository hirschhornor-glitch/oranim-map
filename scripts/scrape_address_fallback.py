"""
scrape_address_fallback.py
--------------------------
Address-based fallback for building permits — catches permits that the
regular taba search misses (when YK's permit records don't reference the
plan's taba number). Uses spatial intersection between plan polygons and
roads.geojson to derive (street, house_number) pairs, then queries YK
with both fields so YK filters server-side.

Results are MERGED into all_permits.json:
  - permits already present (by file_number) are left unchanged
  - new permits are appended with "source": "address_fallback"
  - existing entries with "units_source": "manual" are preserved
  - permit_count is updated

Default target list: the sanity-check flagged plans (large plans with
few permits, approved ≥ SANITY_MIN_AGE_YEARS ago).

Usage:
    python scrape_address_fallback.py                 # run all flagged plans
    python scrape_address_fallback.py --taba 511923   # single plan
    python scrape_address_fallback.py --dry-run       # show plan of work, no YK
    python scrape_address_fallback.py --limit 3       # first N flagged plans
"""
import argparse
import asyncio
import json
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright
from shapely.geometry import shape

sys.path.insert(0, r"C:\ORANIM")
from scrape_all_permits import (
    SITE_URL, BROWSER_DATA, JSON_PATH, PLANS_GEOJSON,
    EXTRACT_ALL_JS, load_progress, save_progress,
    load_plan_meta, run_sanity_check,
)

ROADS_PATH = r"C:\ORANIM\oranim-app\data\roads.geojson"
FALLBACK_LOG = r"C:\ORANIM\all_permits_fallback.log"


# --- Spatial helpers ---------------------------------------------------------

_roads_cache = None

def _load_roads():
    global _roads_cache
    if _roads_cache is None:
        with open(ROADS_PATH, encoding="utf-8") as f:
            _roads_cache = json.load(f)
    return _roads_cache


STREET_PREFIXES = ("דרך ", "שדרות ", "שד' ", "שד׳ ", "רחוב ", "רח' ", "רח׳ ",
                   "סמטה ", "סמטת ", "ככר ", "כיכר ", "הרב ", "הגאון ")

def _levenshtein(a: str, b: str) -> int:
    if len(a) < len(b):
        a, b = b, a
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr = [i + 1]
        for j, cb in enumerate(b):
            curr.append(min(prev[j+1] + 1, curr[j] + 1, prev[j] + (ca != cb)))
        prev = curr
    return prev[-1]


def _plan_text_fuzzy_variants(street: str, plan_text: str) -> list[str]:
    """Find Hebrew words in plan_text that are close variants of `street`
    (Levenshtein distance ≤ 2, similar length). Handles spelling differences
    like roads 'אנטיגונוס' vs plan 'אנטיגנוס'."""
    if not plan_text or not street:
        return []
    out: list[str] = []
    for word in re.findall(r"[\u0590-\u05FF]{3,}", plan_text):
        if word == street or word in out:
            continue
        if abs(len(word) - len(street)) > 2:
            continue
        if _levenshtein(word, street) <= 2:
            out.append(word)
    return out


# Geresh/gershayim/apostrophe characters that differ between our names and YK's
# (e.g. "בית''ר", "ביל''ו", "איתמר בן אב''י", "אל ט'הארה", "אנצ'ו").
_QUOTE_CHARS = "\"'`׳״‘’“”"
_QUOTE_STRIP_RE = re.compile("[" + re.escape(_QUOTE_CHARS) + "]+")

def _strip_quotes(s: str) -> str:
    """Drop all geresh/gershayim/apostrophes: 'בית''ר' → 'ביתר'."""
    return _QUOTE_STRIP_RE.sub("", s).strip()

def _norm_gershayim(s: str) -> str:
    """Collapse any run of quote chars to a single gershayim: 'בית''ר' → 'בית\"ר'."""
    return _QUOTE_STRIP_RE.sub('"', s).strip()

def _expand_base(body: str) -> list[str]:
    """Prefix-strip, suffix-only, and 2-word reversal for a single base name."""
    out = []
    b = body
    for pref in STREET_PREFIXES:
        if b.startswith(pref):
            b = b[len(pref):].strip()
            break
    if b and b != body:
        out.append(b)
    words = b.split()
    for i in range(1, len(words)):
        suffix = " ".join(words[i:])
        if len(suffix) >= 3:
            out.append(suffix)
    # reversed order — YK often lists Hebrew names as "family given" vs GIS "given family"
    if len(words) == 2:
        out.append(" ".join(reversed(words)))
    return out

def _street_variants(name: str, plan_text: str = "") -> list[str]:
    """Return the street name plus variants to try for matching. Handles
    differences between roads.geojson names and YK's street list:
      - quote/gershayim removal ('בית''ר' → 'ביתר') and normalization ('בית\"ר')
      - prefix-stripped ('דרך הרכבת' → 'הרכבת')
      - suffix-only for 2+ word names ('אברהם שלום יהודה' → 'שלום יהודה', 'יהודה')
      - reversed order for 2-word names ('בית חוגלה' → 'חוגלה בית')
      - plan-text fuzzy variants ('אנטיגונוס' + plan 'אנטיגנוס' → also try 'אנטיגנוס')
    Each transform is also applied to the quote-stripped/normalized bases.
    """
    bases = [name]
    for b in (_strip_quotes(name), _norm_gershayim(name)):
        if b and b not in bases:
            bases.append(b)
    out = []
    for base in bases:
        if base not in out:
            out.append(base)
        for v in _expand_base(base):
            if v not in out:
                out.append(v)
    for v in _plan_text_fuzzy_variants(name, plan_text):
        if v not in out:
            out.append(v)
    return out


_PREFIX_WORDS = set()
for _pref in STREET_PREFIXES:
    for _w in _pref.split():
        _w = _w.strip()
        if _w:
            _PREFIX_WORDS.add(_w)


def _required_words(original: str) -> set[str]:
    """Return the 'meaningful' words of a street name — original words minus
    common prefixes like 'דרך'/'שדרות'/'הרב'. These are the words a legitimate
    YK match must share. Guarantees at least one word even if everything is
    a prefix (falls back to original)."""
    words = [w for w in original.split() if w]
    core = [w for w in words if w not in _PREFIX_WORDS]
    return set(core) if core else set(words)


def _fuzzy_word_overlap(required: set[str], suggestion_words: set[str]) -> int:
    """Count required words that exist in suggestion_words, either exactly or
    via Levenshtein ≤ 2 for similar-length words (handles 'אנטיגונוס' ↔ 'אנטיגנוס')."""
    count = 0
    for rw in required:
        if rw in suggestion_words:
            count += 1
            continue
        for sw in suggestion_words:
            if abs(len(sw) - len(rw)) <= 2 and _levenshtein(sw, rw) <= 2:
                count += 1
                break
    return count


def _best_suggestion_match(original: str, suggestions: list[str]) -> int | None:
    """Pick the suggestion that shares the most meaningful words with original.
    Accept only if the suggestion contains ALL required words of the original
    (prefixes like 'דרך' excluded, fuzzy word-level match allowed) — rejects
    false matches like 'לויד ג'ורג'' → 'המלך ג'ורג''.
    """
    if not suggestions:
        return None
    required = _required_words(original)
    need = len(required)
    scored = []
    for i, s in enumerate(suggestions):
        s_words = set(s.split())
        overlap = _fuzzy_word_overlap(required, s_words)
        scored.append((overlap, i, s))
    scored.sort(key=lambda x: (-x[0], x[1]))
    best_overlap, best_idx, _ = scored[0]
    if best_overlap < need:
        return None
    return best_idx


# Buffer (in degrees) applied to plan polygons before intersecting with roads.
# Building polygons typically sit 5-15m back from a road's centerline, so
# zero-buffer intersection misses them. 0.0001° ≈ 11m at Jerusalem latitude.
POLYGON_BUFFER_DEG = 0.0001


def _intersect_streets(plan_geom) -> dict[str, dict]:
    """Return {street_name: {'geo_nums': set[int], 'has_unbounded': bool}}
    for roads intersecting a slightly buffered plan polygon."""
    buffered = plan_geom.buffer(POLYGON_BUFFER_DEG)
    minx, miny, maxx, maxy = buffered.bounds
    margin = 0.0003
    bbox = (minx - margin, miny - margin, maxx + margin, maxy + margin)

    out: dict[str, dict] = defaultdict(lambda: {"geo_nums": set(), "has_unbounded": False})
    for rf in _load_roads()["features"]:
        name = rf["properties"].get("street")
        if not name:
            continue
        rg = rf["geometry"]
        if rg["type"] not in ("LineString", "MultiLineString"):
            continue
        try:
            road = shape(rg)
        except Exception:
            continue
        rb = road.bounds
        if rb[2] < bbox[0] or rb[0] > bbox[2] or rb[3] < bbox[1] or rb[1] > bbox[3]:
            continue
        if not road.intersects(buffered):
            continue

        p = rf["properties"]
        fl, tl = (p.get("fromleft") or 0), (p.get("toleft") or 0)
        fr, tr = (p.get("fromright") or 0), (p.get("toright") or 0)
        got = False
        if fl and tl:
            lo, hi = min(fl, tl), max(fl, tl)
            for n in range(lo, hi + 1, 2):
                out[name]["geo_nums"].add(n)
            got = True
        if fr and tr:
            lo, hi = min(fr, tr), max(fr, tr)
            for n in range(lo, hi + 1, 2):
                out[name]["geo_nums"].add(n)
            got = True
        if not got:
            out[name]["has_unbounded"] = True
    return dict(out)


_NUM_EXPR = r"\d+(?:\s*[,\-–]\s*\d+)*"

def _parse_number_expression(expr: str) -> set[int]:
    """Parse '5,7,11-15,20' into {5,7,11,12,13,14,15,20}. Ignores ranges > 50."""
    results: set[int] = set()
    for part in re.split(r"\s*,\s*", expr or ""):
        part = part.strip()
        m = re.match(r"^(\d+)\s*[-–]\s*(\d+)$", part)
        if m:
            a, b = int(m.group(1)), int(m.group(2))
            lo, hi = min(a, b), max(a, b)
            if hi - lo <= 50:
                results.update(range(lo, hi + 1))
            continue
        m = re.match(r"^(\d+)$", part)
        if m:
            results.add(int(m.group(1)))
    return results


def extract_numbers_for_street(text: str, street: str) -> set[int]:
    """Find occurrences of 'street' (including prefix-stripped and plan-text
    fuzzy variants) in text; collect numbers immediately after. Returns the
    union over variants."""
    if not text or not street:
        return set()
    results: set[int] = set()
    for variant in _street_variants(street, text):
        street_pattern = r"\s*".join(re.escape(w) for w in variant.split())
        pattern = rf"{street_pattern}\s+({_NUM_EXPR})"
        for m in re.finditer(pattern, text):
            results.update(_parse_number_expression(m.group(1)))
    return results


def extract_plan_addresses(plan_feat) -> list[tuple[str, int | None]]:
    """Return sorted [(street, house_number), ...].
    Algorithm:
      1. Intersect plan polygon with roads to get a street whitelist.
      2. For each street, look for explicit numbers in plan_name_he/plan_summary.
      3. If ANY street has explicit numbers, restrict to those streets only
         (plan text is authoritative — other streets are usually adjacent
         access roads, not part of the plan's buildings).
      4. Otherwise, fall back: geographic ranges, or street-only.
    """
    plan_geom = shape(plan_feat["geometry"])
    props = plan_feat.get("properties") or {}
    streets_geo = _intersect_streets(plan_geom)

    text_blobs = [props.get("plan_name_he") or "", props.get("plan_summary") or ""]

    # Pass 1 — find streets with text-numbers
    text_numbers: dict[str, set[int]] = {}
    for name in streets_geo:
        nums: set[int] = set()
        for blob in text_blobs:
            nums |= extract_numbers_for_street(blob, name)
        if nums:
            text_numbers[name] = nums

    pairs: list[tuple[str, int | None]] = []
    if text_numbers:
        # Narrow mode: only the streets with explicit plan-text numbers
        for name in sorted(text_numbers):
            for n in sorted(text_numbers[name]):
                pairs.append((name, n))
    else:
        # Broad mode: geographic fallback. SKIP street-only searches (house=None)
        # — they return every permit on a major arterial, producing mostly
        # false positives we cannot attribute to this plan.
        for name in sorted(streets_geo):
            info = streets_geo[name]
            if info["geo_nums"]:
                for n in sorted(info["geo_nums"]):
                    pairs.append((name, n))
            # else: skip — no usable house numbers anywhere
    return pairs


# --- YK search helpers -------------------------------------------------------

BLOCK_BACKOFF_SEC = (90, 300)   # cool-downs before retrying a no_input query
MAX_CONSEC_BLOCKED = 5          # queries still blocked after backoff → abort the run
_blocked = {"consec": 0}


class SiteBlocked(RuntimeError):
    """YK keeps refusing to render the search form — abort rather than log 150 misses."""


async def _commit_street(page, street_input, street: str,
                         plan_text: str = "") -> tuple[str, str | None]:
    """Try variants of `street` until YK's autocomplete offers a matching
    suggestion. Returns ('ok', matched_variant) | ('no_suggestion', None).
    """
    variants = _street_variants(street, plan_text)
    for v in variants:
        await street_input.click()
        await street_input.fill("")
        await asyncio.sleep(0.25)
        await street_input.type(v, delay=70)
        await asyncio.sleep(1.5)

        suggestions: list[str] = []
        try:
            await page.wait_for_selector("li.k-list-item", timeout=1200)
            items = await page.query_selector_all("li.k-list-item")
            for it in items:
                t = (await it.text_content() or "").strip()
                if t:
                    suggestions.append(t)
        except Exception:
            items = []

        if items and suggestions:
            idx = _best_suggestion_match(street, suggestions)
            if idx is not None:
                await items[idx].click()
                await asyncio.sleep(0.4)
                return ("ok", suggestions[idx])

        # Not found with this variant — close dropdown before next try
        try:
            await street_input.press("Escape")
        except Exception:
            pass
        await asyncio.sleep(0.2)

    return ("no_suggestion", None)


async def search_address(page, ctx, street: str, house: int | None,
                         plan_text: str = "") -> tuple:
    """Returns (status, active_page). status in {ok, no_input, no_suggestion,
    no_button}."""
    try:
        await page.goto(SITE_URL, wait_until="networkidle", timeout=20000)
    except Exception:
        pass
    await asyncio.sleep(3)

    inputs = await page.query_selector_all("input.k-input-inner")
    if len(inputs) < 4:
        return ("no_input", page)

    street_input = inputs[2]  # 3rd combobox = רחוב
    commit_status, matched = await _commit_street(page, street_input, street, plan_text)
    if commit_status != "ok":
        return ("no_suggestion", page)
    if matched and matched != street:
        _log(f"     street variant matched: {street!r} → {matched!r}")

    if house is not None:
        nums = await page.query_selector_all('input[type="number"]')
        if nums:
            house_input = nums[0]  # בית (first number input in DOM)
            await house_input.click()
            await house_input.fill("")
            await house_input.type(str(house), delay=40)
            await asyncio.sleep(0.6)

    btn = (await page.query_selector('button.search-btn')
           or await page.query_selector('button:has-text("אתר תיק רישוי")'))
    if not btn:
        return ("no_button", page)

    await btn.click()
    await asyncio.sleep(3)
    active_page = ctx.pages[-1] if len(ctx.pages) > 1 else page
    await asyncio.sleep(8)
    return ("ok", active_page)


# --- Merge logic -------------------------------------------------------------

def _file_year(fn: str) -> int | None:
    m = re.match(r"(\d{4})/", fn or "")
    return int(m.group(1)) if m else None


def _parse_dt(s):
    if not s: return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(s, fmt).date()
        except ValueError: continue
    return None


def merge_fallback_permits(existing_entry: dict, new_permits: list[dict],
                           mavat_year: int) -> tuple[dict, list[dict]]:
    """Merge new_permits into existing_entry['permits'].
    Returns (updated_entry, list_of_newly_added_permits).
    - Dedup by file_number (existing wins)
    - Only accept new permits where file_year >= mavat_year
    - New permits get source='address_fallback'
    - manual entries (units_source='manual') stay untouched
    """
    existing_permits = list(existing_entry.get("permits") or [])
    existing_file_nums = {p.get("file_number") for p in existing_permits if p.get("file_number")}

    added = []
    for prm in new_permits:
        fn = prm.get("file_number")
        if not fn or fn in existing_file_nums:
            continue
        fy = _file_year(fn)
        if fy is None or fy < mavat_year:
            continue
        new_row = dict(prm)
        new_row["source"] = "address_fallback"
        existing_permits.append(new_row)
        existing_file_nums.add(fn)
        added.append(new_row)

    updated = dict(existing_entry)
    updated["permits"] = existing_permits
    updated["permit_count"] = len(existing_permits)
    updated["fallback_scraped_at"] = datetime.now().isoformat()
    return updated, added


# --- CLI / Orchestrator ------------------------------------------------------

def _log(line: str):
    print(line)
    try:
        with open(FALLBACK_LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


async def process_taba(page, ctx, taba: str, plan_feat: dict,
                       plan_meta: dict, results: dict, dry_run: bool) -> dict:
    """Return per-taba stats dict."""
    meta = plan_meta.get(str(taba), {})
    mavat_str = meta.get("mavat_date")
    md = _parse_dt(mavat_str)
    mavat_year = md.year if md else 0
    if not md:
        _log(f"[{taba}] WARNING: no mavat_date — skipping year filter (keeping all)")

    pairs = extract_plan_addresses(plan_feat)
    streets = sorted(set(s for s, _ in pairs))
    props = plan_feat.get("properties") or {}
    plan_text = " ".join([props.get("plan_name_he") or "", props.get("plan_summary") or ""])

    _log(f"\n[{taba}] units={meta.get('units')}  mavat={mavat_str or '?'}  "
         f"streets={len(streets)}  queries_planned={len(pairs)}")
    for s in streets:
        nums = sorted(n for st, n in pairs if st == s and n is not None)
        has_none = any(n is None for st, n in pairs if st == s)
        label = f"{nums[0]}..{nums[-1]} ({len(nums)} nums)" if nums else "(street only)" if has_none else ""
        _log(f"   - {s}: {label}")

    if dry_run:
        return {"taba": taba, "queries": len(pairs), "added": 0, "skipped": "dry_run"}

    found_permits_by_fn: dict[str, dict] = {}
    query_stats = {"ok": 0, "no_suggestion": 0, "no_input": 0, "error": 0}

    for street, house in pairs:
        label = f"{street} {house}" if house else f"{street} (street only)"
        try:
            status, active_page = await search_address(page, ctx, street, house, plan_text)
            # no_input = YK's form never rendered: Akamai/reCAPTCHA throttling, typically
            # right after the multi-hour taba scrape. Without backoff every remaining
            # query failed the same way (since 07/2026 each run: ok=2, no_input≈160).
            for pause in BLOCK_BACKOFF_SEC:
                if status != "no_input":
                    break
                _log(f"   [{label}] no_input — cooldown {pause}s then retry")
                await asyncio.sleep(pause)
                status, active_page = await search_address(page, ctx, street, house, plan_text)
            if status == "no_input":
                _blocked["consec"] += 1
                if _blocked["consec"] >= MAX_CONSEC_BLOCKED:
                    raise SiteBlocked(f"{_blocked['consec']} consecutive no_input after backoff")
            else:
                _blocked["consec"] = 0
            if status != "ok":
                query_stats[status] = query_stats.get(status, 0) + 1
                _log(f"   [{label}] status={status}")
            else:
                permits = await active_page.evaluate(EXTRACT_ALL_JS)
                query_stats["ok"] += 1
                for prm in permits:
                    fn = prm.get("file_number")
                    if fn and fn not in found_permits_by_fn:
                        prm_with_addr = dict(prm)
                        prm_with_addr["_fallback_street"] = street
                        prm_with_addr["_fallback_house"] = house
                        found_permits_by_fn[fn] = prm_with_addr
                _log(f"   [{label}] ok: +{len(permits)} permits (total unique so far: {len(found_permits_by_fn)})")
        except SiteBlocked:
            raise
        except Exception as e:
            query_stats["error"] += 1
            _log(f"   [{label}] ERROR: {str(e)[:80]}")
        # Housekeeping
        while len(ctx.pages) > 1:
            try: await ctx.pages[-1].close()
            except Exception: pass
        # page reference may have changed
        if ctx.pages:
            page = ctx.pages[0]
        await asyncio.sleep(1)

    # Merge into results
    existing = results.get(str(taba), {"permits": [], "permit_count": 0})
    updated, added = merge_fallback_permits(existing, list(found_permits_by_fn.values()), mavat_year)
    results[str(taba)] = updated

    _log(f"[{taba}] queries={query_stats}  new_permits_added={len(added)}  "
         f"total_in_json={updated['permit_count']}")
    for prm in added:
        _log(f"   + {prm.get('file_number')}  {prm.get('status_date'):>11}  "
             f"st={prm.get('_fallback_street')}  house={prm.get('_fallback_house')}  "
             f"status={(prm.get('status') or '')[:40]!r}")

    return {"taba": taba, "queries": len(pairs), "added": len(added), "stats": query_stats}


async def main_async(args):
    sys.stdout.reconfigure(encoding="utf-8")

    # Load plans + permits + meta
    with open(PLANS_GEOJSON, encoding="utf-8") as f:
        plans_gj = json.load(f)
    plan_by_taba = {str(f["properties"].get("taba")): f for f in plans_gj["features"]
                    if f["properties"].get("taba") is not None}

    plan_meta = load_plan_meta()
    results = load_progress()

    # Decide targets
    if args.taba:
        targets = [args.taba]
    elif args.tabas:
        targets = [t.strip() for t in args.tabas.split(",") if t.strip()]
    else:
        flagged = run_sanity_check(permits_data=results, plan_meta=plan_meta, log_path=None)
        targets = [row["taba"] for row in flagged]
        if args.limit:
            targets = targets[:args.limit]

    _log(f"\n{'='*60}")
    _log(f"address fallback run  at {datetime.now().isoformat()}")
    _log(f"targets: {len(targets)} plans  (dry_run={args.dry_run})")
    _log(f"{'='*60}")

    missing = [t for t in targets if t not in plan_by_taba]
    if missing:
        _log(f"WARN: tabas not in plans.geojson (skipping): {missing}")
        targets = [t for t in targets if t in plan_by_taba]

    if args.dry_run:
        for t in targets:
            await process_taba(None, None, t, plan_by_taba[t], plan_meta, results, dry_run=True)
        return

    async with async_playwright() as p:
        from yk_profile_lock import hold_yk_profile
        hold_yk_profile("scrape_address_fallback.py")  # one YK-profile user at a time
        ctx = await p.chromium.launch_persistent_context(
            BROWSER_DATA, headless=False,
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        try:
            await page.goto(SITE_URL, wait_until="networkidle", timeout=30000)
        except Exception:
            pass
        await asyncio.sleep(5)

        total_added = 0
        per_plan_stats: list[dict] = []
        for i, taba in enumerate(targets):
            try:
                stats = await process_taba(page, ctx, taba, plan_by_taba[taba],
                                           plan_meta, results, dry_run=False)
            except SiteBlocked as e:
                save_progress(results)
                _log(f"!! ABORT at plan {i+1}/{len(targets)} ({taba}): YK blocked — {e}. "
                     f"{total_added} permits added before the block.")
                await ctx.close()
                sys.exit(2)
            total_added += stats.get("added", 0)
            per_plan_stats.append(stats)
            # Save every plan — fallback queries are expensive; protect progress
            save_progress(results)
            _log(f"[progress] {i+1}/{len(targets)} plans done, {total_added} permits added overall")

        await ctx.close()

    _log(f"\nDONE — {total_added} permits added across {len(targets)} plans")
    _log(f"Saved to {JSON_PATH}")

    # --- Post-run sanity: flag suspicious jumps and no_suggestion failures ---
    BIG_JUMP_THRESHOLD = 20
    FAIL_RATE_THRESHOLD = 0.5
    suspicious_jumps = []
    heavy_failures = []
    for s in per_plan_stats:
        added = s.get("added", 0) or 0
        qstats = s.get("stats") or {}
        q_ok = qstats.get("ok", 0)
        q_total = sum(v for k, v in qstats.items() if k != "dry_run")
        q_fails = q_total - q_ok
        if added >= BIG_JUMP_THRESHOLD:
            suspicious_jumps.append((s["taba"], added, q_ok, q_total))
        if q_total > 0 and q_fails / q_total >= FAIL_RATE_THRESHOLD:
            heavy_failures.append((s["taba"], q_fails, q_total))

    if suspicious_jumps:
        _log(f"\n=== SUSPICIOUS JUMPS (+{BIG_JUMP_THRESHOLD} or more new permits) ===")
        _log("  Review manually — may include unrelated buildings on same street:")
        for taba, added, q_ok, q_total in suspicious_jumps:
            _log(f"  {taba:<10} +{added} permits via {q_ok}/{q_total} queries")

    if heavy_failures:
        _log(f"\n=== HEAVY FAILURE RATE (no_suggestion ≥ {int(FAIL_RATE_THRESHOLD*100)}%) ===")
        _log("  These plans may need manual investigation or retry:")
        for taba, fails, total in heavy_failures:
            _log(f"  {taba:<10} {fails}/{total} queries failed")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--taba", help="Run on a single taba (overrides sanity-check list)")
    ap.add_argument("--tabas", help="Comma-separated list of tabas (overrides sanity-check list)")
    ap.add_argument("--limit", type=int, help="Process only first N flagged plans")
    ap.add_argument("--dry-run", action="store_true",
                    help="Show planned queries without calling YK")
    args = ap.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
