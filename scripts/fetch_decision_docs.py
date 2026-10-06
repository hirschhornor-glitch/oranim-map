"""Fetch the district-committee DECISION document (מסמך החלטות) from Mavat for
plans in status "במילוי תנאים להפקדה".

About a week after a plan is heard by the ועדה מחוזית, a decision document is
published on the plan's Mavat page under the "החלטות מוסדות תכנון" accordion. This
script downloads that "מסמך החלטות" PDF (NOT סדר יום / תמליל הישיבה) so it can be
manually summarized into decision_summaries.json (the app reads that file).

Patterned on:
  - update_mavat_status.py  → persistent-context + manual-captcha WAF flow
  - download_tree_surveys.py → accordion expand + PDF-via-popup download

Usage:
  py fetch_decision_docs.py            # process all target-status plans (resumable)
  py fetch_decision_docs.py --limit 2  # only first N (e.g. test on 1316918)
  py fetch_decision_docs.py --only 1316918,833038
  py fetch_decision_docs.py --debug    # dump the decisions-section HTML per plan
"""
import asyncio, json, os, sys, argparse, re
sys.stdout.reconfigure(encoding='utf-8')

MAVAT_BASE   = "https://mavat.iplan.gov.il"
BROWSER_DATA = r"C:\ORANIM\.browser_data"
PLANS_GEOJSON = r"C:\ORANIM\oranim-app\data\plans.geojson"
OUTPUT_DIR   = r"C:\ORANIM\decisions"
INDEX_FILE   = r"C:\ORANIM\decision_docs_index.json"
DEBUG_DIR    = os.path.join(OUTPUT_DIR, "_debug")

TARGET_STATUS = "במילוי תנאים להפקדה"
# We only want the district committee's decision document.
COMMITTEE_KW  = "מחוזית"
# The attached file we want (and the ones to ignore).
DECISION_DOC_KW = ["מסמך החלטות", "מסמך החלטה"]
IGNORE_DOC_KW   = ["סדר יום", "תמליל"]

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ─── Work list ─────────────────────────────────────────────────

def normalize_agam(agam):
    try:
        return str(int(float(agam)))
    except Exception:
        return ""

def load_index():
    if os.path.exists(INDEX_FILE):
        with open(INDEX_FILE, encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_index(idx):
    with open(INDEX_FILE, 'w', encoding='utf-8') as f:
        json.dump(idx, f, ensure_ascii=False, indent=2)

def build_work_list(only=None):
    with open(PLANS_GEOJSON, encoding='utf-8') as f:
        geo = json.load(f)
    seen, work = set(), []
    for feat in geo['features']:
        p = feat['properties']
        status = (p.get('status_mavat', '') or '').strip()
        if TARGET_STATUS not in status:
            continue
        taba = str(p.get('taba', '')).strip()
        agam = normalize_agam(p.get('agam_id', ''))
        if not taba or not agam or taba in seen:
            continue
        if only and taba not in only:
            continue
        seen.add(taba)
        work.append({
            'taba': taba,
            'agam_id': agam,
            'plan_name': p.get('plan_name', '') or f'101-{int(taba):07d}',
            # status date = the date the plan entered "במילוי תנאים להפקדה" = the
            # DEPOSIT decision date. Used to pick the right meeting (not the latest).
            'mavat_date': (p.get('mavat_date', '') or '').strip(),
            'mavat_url': p.get('mavat_url', '') or f'{MAVAT_BASE}/SV4/1/{agam}/310',
        })
    return work


# ─── DOM scan for the decision document ────────────────────────
# The "החלטות מוסדות תכנון" accordion holds one expandable row per committee
# meeting (columns: מספר ישיבה | מוסד תכנון | מתאריך ישיבה). Expanding a row
# reveals "קבצים מצורפים": סדר יום / מסמך החלטות / תמליל הישיבה, each with PDF
# icons. We want the district-committee ("מחוזית") meeting's "מסמך החלטות".

# Step A: open the top-level "החלטות מוסדות תכנון" accordion + expand each
# meeting row so its attached files render. Returns meeting metadata.
# Open the top "החלטות מוסדות תכנון" section (its own UIkit accordion item). The
# per-meeting list (li.sv4-decision-meeting) only renders once this is open.
OPEN_SECTION_JS = r"""
() => {
    for (const t of document.querySelectorAll('div.uk-accordion-title, span.uk-text-lead, a, h3, h4')) {
        if ((t.innerText || '').includes('החלטות מוסדות תכנון')) {
            const li = t.closest('li');
            const opened = !!(li && (li.className || '').includes('uk-open'));
            if (!opened) t.click();
            return { found: true, was_open: opened };
        }
    }
    return { found: false };
}
"""

# Each meeting is <li class="sv4-decision-meeting"> with a toggle .uk-accordion-title
# and files in <div class="uk-accordion-content" hidden>. The file URLs lazy-load only
# once expanded. Click the title of every meeting whose content is still hidden, then
# report how many "מסמך החלטות" icons are now visible. (Proven via probe_decision_dom.py.)
OPEN_MEETINGS_JS = r"""
() => {
    let clicked = 0;
    for (const li of document.querySelectorAll('li.sv4-decision-meeting')) {
        const c = li.querySelector('.uk-accordion-content');
        const t = li.querySelector('.uk-accordion-title');
        if (t && c && c.hasAttribute('hidden')) { try { t.click(); clicked++; } catch(e) {} }
    }
    let visible = 0;
    for (const item of document.querySelectorAll('div.meetingItem')) {
        const label = (((item.querySelector('.uk-text-lead')||{}).innerText) || '').trim();
        if (!label.includes('מסמך החלטות')) continue;
        const icon = item.querySelector('div.clickable img') || item.querySelector('img.sv4-icon-file');
        if (icon && icon.offsetParent) visible++;
    }
    return { meetings: document.querySelectorAll('li.sv4-decision-meeting').length, clicked, visible };
}
"""

# Step B: after expansion, find every "מסמך החלטות" file and tag ITS OWN pdf icon.
# Mavat DOM (verified): each document is a `div.meetingItem` holding the doc label in
# `.uk-text-lead` + an `img.sv4-icon-file` (the clickable view icon — note src is
# *pdf-download*.svg, not pdf-view). Each MEETING is an accordion `<li>` whose header
# carries the institution name ("ועדה מחוזית…") + date. So: per meetingItem labelled
# "מסמך החלטות", grab its own icon and read the committee/date from its enclosing <li>.
# (The earlier span-walk grabbed the first pdf icon in a broad card = the agenda.)
SCAN_DECISION_DOC_JS = r"""
() => {
    const DEC = %s;
    const IGN = %s;
    const matches = [];
    const items = document.querySelectorAll('div.meetingItem');
    for (const item of items) {
        const lead = item.querySelector('.uk-text-lead');
        const label = ((lead && lead.innerText) || item.innerText || '').trim();
        if (!DEC.some(k => label.includes(k))) continue;   // must be "מסמך החלטות"
        if (IGN.some(k => label.includes(k))) continue;    // skip סדר יום / תמליל

        // this document's OWN icon (scoped to the meetingItem, never a sibling doc)
        const icon = item.querySelector('div.clickable img')
            || item.querySelector('img.sv4-icon-file')
            || item.querySelector('img[src*="pdf"]');
        if (!icon) continue;

        // meeting context = the accordion <li> (institution name + date live there)
        const li = item.closest('li') || item.closest('.uk-accordion') || item;
        const ctxText = (li.innerText || '').replace(/\s+/g, ' ').slice(0, 600);
        const committee = ctxText.includes('מחוזית') ? 'מחוזית'
                        : (ctxText.includes('מקומית') ? 'מקומית' : '');
        const dm = ctxText.match(/(\d{2}\/\d{2}\/\d{4})/);
        const mn = ctxText.match(/(\d{6,})/);

        icon.setAttribute('data-decision-match', matches.length.toString());
        matches.push({
            index: matches.length,
            text: label.slice(0, 120),
            committee: committee,
            date: dm ? dm[1] : '',
            meeting_number: mn ? mn[1] : '',
            visible: !!icon.offsetParent,
            ctxText: ctxText,
            hasPdf: true,
        });
    }
    return matches;
}
""" % (json.dumps(DECISION_DOC_KW, ensure_ascii=False),
       json.dumps(IGNORE_DOC_KW, ensure_ascii=False))


async def fetch_pdf_from_icon(page, match):
    """Click the tagged PDF icon and return (bytes, suggested_filename).

    Verified: the "מסמך החלטות" view icon triggers a DOWNLOAD (filename like
    101-<taba>_<committee>_מסמך החלטות.pdf), and it needs a TRUSTED click
    (Playwright locator.click), not a programmatic el.click(). Some docs open a
    blob POPUP instead, so we fall back to that.
    """
    sel = f'[data-decision-match="{match["index"]}"]'
    el = page.locator(sel).first
    try:
        await el.scroll_into_view_if_needed(timeout=5000)
    except Exception:
        pass
    # Primary path: download.
    try:
        async with page.expect_download(timeout=20000) as dl_info:
            await el.click(timeout=8000)
        dl = await dl_info.value
        path = await dl.path()          # waits for the download to finish
        with open(path, 'rb') as f:
            return f.read(), (dl.suggested_filename or '')
    except Exception:
        pass
    # Fallback: blob/pdf popup.
    try:
        async with page.expect_popup(timeout=8000) as popup_info:
            await el.click(timeout=8000)
        popup = await popup_info.value
        await popup.wait_for_load_state()
        data = None
        if popup.url.startswith('blob:') or 'pdf' in popup.url.lower():
            arr = await popup.evaluate('''async (url) => {
                const res = await fetch(url);
                const buf = await res.arrayBuffer();
                return Array.from(new Uint8Array(buf));
            }''', popup.url)
            data = bytes(arr)
        await popup.close()
        return data, ''
    except Exception:
        return None, ''


async def run(limit=None, only=None, debug=False):
    from playwright.async_api import async_playwright

    work = build_work_list(only=set(only) if only else None)
    if limit:
        work = work[:limit]
    index = load_index()
    # Resume: skip only plans whose decision doc is already DOWNLOADED. Plans
    # marked 'no_decision_doc' are re-checked every run — the מסמך החלטות is
    # published ~a week after the hearing, so a plan that had none last week may
    # have one now (this is what makes the weekly scan find "new" docs).
    todo = [w for w in work
            if index.get(w['taba'], {}).get('status') != 'downloaded']
    print(f"Target-status plans: {len(work)} | to process now: {len(todo)}")
    if not todo:
        print("Nothing to do — all resolved. (delete entries from the index to retry)")
        return
    if debug:
        os.makedirs(DEBUG_DIR, exist_ok=True)

    stats = {'downloaded': 0, 'no_doc': 0, 'errors': 0}

    from yk_profile_lock import hold_mavat_profile
    hold_mavat_profile("fetch_decision_docs.py")  # one Mavat-profile user at a time
    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            BROWSER_DATA, headless=False,
            viewport={'width': 1280, 'height': 900},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            args=['--disable-blink-features=AutomationControlled'],
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        # Establish session (Mavat has no captcha; just warm up the persistent
        # context so the SPA + REST endpoints are reachable, then proceed).
        try:
            await page.goto(f"{MAVAT_BASE}/SV4/1/{todo[0]['agam_id']}/310",
                            wait_until='domcontentloaded', timeout=120000)
        except Exception as e:
            # Was `pass` — a dead Mavat looked like a quiet week (2026-10-06 audit).
            print(f"  ⚠ warm-up navigation failed: {str(e)[:120]}", flush=True)
        try:
            await page.wait_for_function(
                "() => document.body.innerText.length > 500", timeout=20000)
        except Exception:
            await asyncio.sleep(8)
        print("Session warmed up — starting.", flush=True)

        for i, plan in enumerate(todo):
            taba, agam, pn = plan['taba'], plan['agam_id'], plan['plan_name']
            print(f"\n[{i+1}/{len(todo)}] {pn} (taba={taba}, agam={agam})...")
            entry = dict(plan); entry['status'] = 'pending'; entry['doc_names'] = []
            try:
                # Navigation / render failures used to be swallowed and the plan then
                # recorded as 'no_decision_doc' — indistinguishable from "not published
                # yet" (1583848 sat at no_decision_doc for four weeks, 2026-10-06 audit).
                # Now a page that never renders, or has no "החלטות מוסדות תכנון" section,
                # is an ERROR (status error_render — retried next run, fails the exit code).
                render_problem = ''
                try:
                    await page.goto(f"{MAVAT_BASE}/SV4/1/{agam}/310",
                                    wait_until='domcontentloaded', timeout=20000)
                except Exception as e:
                    render_problem = f"goto failed: {str(e)[:100]}"
                rendered = True
                try:
                    await page.wait_for_function(
                        "() => document.body.innerText.length > 500", timeout=12000)
                except Exception:
                    await page.wait_for_timeout(5000)
                    rendered = await page.evaluate("() => document.body.innerText.length > 500")

                # Expand: open the section first (meeting list renders after), WAIT, then
                # open each meeting (files lazy-load on expand). Retry until icons visible.
                sec = await page.evaluate(OPEN_SECTION_JS)
                await page.wait_for_timeout(2500)
                exp = {}
                for _pass in range(4):
                    exp = await page.evaluate(OPEN_MEETINGS_JS)
                    await page.wait_for_timeout(2200)
                    if exp.get('visible', 0) > 0:
                        break
                # Printed every run (was --debug only) so a render problem is visible in the log.
                print(f"    section: {sec}  expand: {exp}")
                if not rendered:
                    render_problem = (render_problem + '; ' if render_problem else '') + 'page never rendered'
                elif not sec.get('found'):
                    render_problem = (render_problem + '; ' if render_problem else '') + \
                        "section 'החלטות מוסדות תכנון' not found"
                elif not exp.get('meetings'):
                    # A plan in "במילוי תנאים להפקדה" was heard by a committee, so an
                    # empty meeting list means the list failed to render, not "no doc yet".
                    render_problem = (render_problem + '; ' if render_problem else '') + \
                        'section found but no meetings rendered'
                if render_problem and not (sec.get('found') and exp.get('meetings')):
                    print(f"  → ERROR (render): {render_problem}")
                    entry['status'] = 'error_render'
                    entry['error_reason'] = render_problem
                    stats['errors'] += 1
                    index[taba] = entry
                    save_index(index)
                    await page.wait_for_timeout(2000)
                    continue

                if debug:
                    html = await page.content()
                    with open(os.path.join(DEBUG_DIR, f"{taba}.html"), 'w', encoding='utf-8') as f:
                        f.write(html)

                matches = await page.evaluate(SCAN_DECISION_DOC_JS)
                entry['doc_names'] = [m['text'] for m in matches]
                if debug:
                    for m in matches:
                        print(f"    match: committee={m.get('committee')!r} date={m.get('date')!r} "
                              f"meeting={m.get('meeting_number')!r} visible={m.get('visible')} "
                              f"label={m.get('text')!r}")

                # A plan may have several committee meetings, each with its own
                # "מסמך החלטות". The one we want is the DEPOSIT decision — the meeting
                # whose date == the plan's status date (mavat_date). A later meeting is
                # usually a procedural follow-up (e.g. time extension), NOT the deposit.
                # Fallbacks: nearest district meeting on-or-before the status date, else
                # latest district, else latest.
                def _dkey(m):
                    p = (m.get('date') or '').split('/')
                    return (p[2], p[1], p[0]) if len(p) == 3 else ('0', '0', '0')
                pdf_matches = [m for m in matches if m.get('hasPdf')]
                district = [m for m in pdf_matches if m.get('committee') == 'מחוזית']
                pool = district or pdf_matches
                status_date = plan.get('mavat_date', '')
                chosen = None
                if status_date:
                    exact = [m for m in pool if m.get('date') == status_date]
                    if exact:
                        chosen = exact[0]
                    else:
                        sd = status_date.split('/')
                        sd_key = (sd[2], sd[1], sd[0]) if len(sd) == 3 else None
                        on_before = [m for m in pool if sd_key and _dkey(m) <= sd_key]
                        if on_before:
                            chosen = sorted(on_before, key=_dkey, reverse=True)[0]
                if not chosen and pool:
                    chosen = sorted(pool, key=_dkey, reverse=True)[0]
                if chosen and status_date and chosen.get('date') != status_date:
                    print(f"  ⚠ no meeting on status date {status_date}; using {chosen.get('date')}")

                if not chosen:
                    print(f"  → no 'מסמך החלטות' PDF found among {exp.get('meetings')} meeting(s) "
                          f"(maybe not published yet)")
                    entry['status'] = 'no_decision_doc'
                    stats['no_doc'] += 1
                else:
                    if chosen.get('committee') != 'מחוזית':
                        print(f"  ⚠ using non-district match: {chosen['text'][:50]}")
                    pdf_path = os.path.join(OUTPUT_DIR, f"101-{int(taba):07d}.pdf")
                    data, fname = await fetch_pdf_from_icon(page, chosen)
                    # sanity: the download filename should be the decision doc, not agenda
                    if fname and ('סדר יום' in fname or 'תמליל' in fname):
                        print(f"  ⚠ wrong doc downloaded (filename: {fname[:60]}) — skipping")
                        data = None
                    if data and len(data) > 1000 and data[:4] == b'%PDF':
                        with open(pdf_path, 'wb') as f:
                            f.write(data)
                        print(f"  ✓ downloaded {len(data)//1024}KB → {os.path.basename(pdf_path)}"
                              f"  [{fname or chosen['text'][:40]}]")
                        entry['status'] = 'downloaded'
                        entry['pdf_file'] = os.path.basename(pdf_path)
                        entry['source_filename'] = fname
                        # meeting metadata comes straight from the scan (read off the <li>)
                        entry['decision_date'] = chosen.get('date', '')
                        entry['meeting_number'] = chosen.get('meeting_number', '')
                        entry['committee'] = ('ועדה מחוזית לתכנון ולבניה מחוז ירושלים'
                                              if chosen.get('committee') == 'מחוזית'
                                              else chosen.get('committee', ''))
                        entry['summarized'] = False
                        stats['downloaded'] += 1
                    else:
                        print("  → PDF icon found but download failed")
                        entry['status'] = 'download_failed'
                        stats['errors'] += 1

            except Exception as e:
                print(f"  → error: {str(e)[:120]}")
                entry['status'] = f'error: {str(e)[:100]}'
                stats['errors'] += 1

            index[taba] = entry
            save_index(index)
            await page.wait_for_timeout(2000)

        await ctx.close()

    print(f"\n{'='*60}")
    print(f"downloaded={stats['downloaded']}  no_doc={stats['no_doc']}  errors={stats['errors']}")
    print(f"PDFs → {OUTPUT_DIR}\\101-<taba>.pdf   index → {INDEX_FILE}")
    print("Next: read each new PDF and add a structured entry to decision_summaries.json")
    print(f"{'='*60}")
    return stats


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description="Download district-committee decision PDFs from Mavat")
    ap.add_argument('--limit', type=int, default=None)
    ap.add_argument('--only', type=str, default=None, help='comma-separated taba numbers')
    ap.add_argument('--debug', action='store_true', help='dump page HTML to decisions/_debug/')
    args = ap.parse_args()
    only = [t.strip() for t in args.only.split(',')] if args.only else None
    stats = asyncio.run(run(limit=args.limit, only=only, debug=args.debug))
    # Non-zero on any error so weekly_decision_docs_scan.py alerts instead of
    # reporting "nothing to send" over a broken scrape (2026-10-06 audit).
    if stats and stats.get('errors'):
        print(f"EXIT 1: {stats['errors']} plan(s) failed (render/download/error)")
        sys.exit(1)
