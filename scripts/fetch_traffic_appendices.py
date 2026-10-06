# -*- coding: utf-8 -*-
"""
fetch_traffic_appendices.py
---------------------------
Download each plan's traffic/parking appendix (נספח תנועה / חניה) from Mavat.

Reverse-engineered download path (probed 2026-08-31): clicking a document row's
`sv4-icon-pdf-download.svg` icon triggers a real browser download — the click
itself issues the reCAPTCHA v3 token Mavat requires. This is far cheaper than
the per-section ZIP (one 5-10MB appendix instead of a 150MB+ section dump).

Per plan we record EVERY document row (name + date), so "has no traffic
appendix" is a fact we can prove rather than a silent miss.

Usage:
    py fetch_traffic_appendices.py --only 836809 --dry-run
    py fetch_traffic_appendices.py --worklist worklist.json --resume
"""
import argparse, asyncio, json, os, re, sys, time
sys.stdout.reconfigure(encoding='utf-8')
from playwright.async_api import async_playwright

DATA = r"C:\ORANIM\oranim-app\data"
BROWSER_DATA = r"C:\ORANIM\.browser_data_traffic"
OUTDIR = r"C:\ORANIM\traffic_appendices"
PDFDIR = os.path.join(OUTDIR, 'pdf')
INDEX = os.path.join(OUTDIR, 'doc_index.json')
os.makedirs(PDFDIR, exist_ok=True)

# What counts as the traffic/parking appendix.
# 'חניות' (plural) and a standalone 'דרכים' sheet were missed by the first pass —
# a 'דרכים וחניות' appendix is the parking balance under another name.
WANT = re.compile(r'תנוע|תחבור|חני|חניון|דרכים')
# Rows that mention traffic but are not the appendix sheet.
REJECT = re.compile(r'שומ|שמאי|היטל|תסקיר|נוסח|פרוטוקול|תכתוב|התנגד|בקשה לאישור|אישור רשות')

EXPAND_JS = r"""() => {
    if (!window.UIkit || !UIkit.accordion) return;
    document.querySelectorAll('.uk-accordion').forEach(el => {
        try { const a = UIkit.accordion(el);
          const items = el.querySelectorAll(':scope > li');
          for (let i=0;i<items.length;i++) if(!items[i].classList.contains('uk-open')) a.toggle(i,false);
        } catch(e){} });
}"""

# Every download icon on the page, with the text of the row it belongs to.
ROWS_JS = r"""() => Array.from(document.querySelectorAll('img[src*="pdf-download"]')).map((img, i) => {
    let n = img, txt = '';
    for (let k = 0; k < 8 && n; k++) {
        n = n.parentElement;
        if (!n) break;
        const t = (n.innerText || '').replace(/\s+/g, ' ').trim();
        if (t.length > 3) { txt = t; break; }
    }
    return { i: i, txt: txt.slice(0, 200) };
})"""

WAIT_ICONS = "() => document.querySelectorAll('img[src*=\"pdf-download\"]').length>0"


def norm_taba(t):
    return re.sub(r'^101-0*', '', str(t or '')).lstrip('0')


def agam_index():
    plans = json.load(open(os.path.join(DATA, 'plans.geojson'), encoding='utf-8'))
    idx = {}
    for f in plans['features']:
        p = f['properties']
        taba = norm_taba(p.get('taba'))
        if taba and p.get('agam_id'):
            idx[taba] = int(float(p['agam_id']))
    return idx


STAGE = re.compile(r'\s*-?\s*חתום ל?(אישור|הפקדה).*$|\d{2}/\d{2}/\d{4}|רקע|מנחה')


def pick(rows):
    """Rank rows: the signed/approved traffic sheet first, then keep one row per
    distinct document (a plan usually carries the same appendix twice — the
    להפקדה and the לאישור signing)."""
    out = []
    for r in rows:
        t = r['txt']
        if not WANT.search(t):
            continue
        if REJECT.search(t):
            continue
        s = 0
        if 'תנוע' in t:
            s += 5
        if 'חני' in t:
            s += 3
        if 'נספח' in t:
            s += 2
        if 'חתום לאישור' in t or 'מאושר' in t:
            s += 4        # the approved signing supersedes the deposit one
        elif 'חתום' in t:
            s += 2
        if 'תחבור' in t:
            s += 1
        out.append((s, r))
    out.sort(key=lambda x: -x[0])
    if not out:
        return []
    top = out[0][0]
    seen, uniq = set(), []
    for s, r in out:
        if s < top - 2:
            break            # secondary material (תח"צ, בה"ת) once the sheet is found
        key = STAGE.sub('', r['txt']).strip()
        if key in seen:
            continue         # same appendix, deposit vs approval signing
        seen.add(key)
        uniq.append(r)
    return uniq


async def do_plan(page, ctx, taba, agam, dry=False, max_docs=4):
    rec = {'agam': agam, 'ts': int(time.time())}
    try:
        await page.goto('https://mavat.iplan.gov.il/SV4/1/%d/310' % agam,
                        wait_until='domcontentloaded', timeout=60000)
    except Exception:
        pass
    try:
        await page.wait_for_function(WAIT_ICONS, timeout=45000)
    except Exception:
        rec['status'] = 'no_doclist'
        return rec
    await asyncio.sleep(2.5)
    for _ in range(6):
        await page.evaluate(EXPAND_JS)
        await asyncio.sleep(0.6)

    rows = await page.evaluate(ROWS_JS)
    rec['rows'] = [r['txt'] for r in rows]
    cands = pick(rows)
    rec['candidates'] = [c['txt'] for c in cands]
    if not cands:
        rec['status'] = 'no_traffic_doc'
        return rec
    if dry:
        rec['status'] = 'dry'
        return rec

    pdir = os.path.join(PDFDIR, taba)
    os.makedirs(pdir, exist_ok=True)
    saved = []
    icons = await page.query_selector_all('img[src*="pdf-download"]')
    for c in cands[:max_docs]:
        dest = os.path.join(pdir, 'doc%d.pdf' % c['i'])
        if os.path.exists(dest) and os.path.getsize(dest) > 20000:
            saved.append({'file': dest, 'txt': c['txt'], 'cached': True})
            continue
        err = None
        for attempt in (1, 2):
            try:
                ic = icons[c['i']]
                await ic.scroll_into_view_if_needed(timeout=4000)
                async with page.expect_download(timeout=90000) as di:
                    await ic.click(force=True, timeout=6000)
                dl = await di.value
                await dl.save_as(dest)
                saved.append({'file': dest, 'txt': c['txt'], 'bytes': os.path.getsize(dest)})
                err = None
                break
            except Exception as e:
                err = str(e)[:120]
                await asyncio.sleep(3)
        if err:
            saved.append({'txt': c['txt'], 'error': err})
    # a stray pdf-view click can leave a blob tab open; keep the context tidy
    for p in list(ctx.pages)[1:]:
        try:
            await p.close()
        except Exception:
            pass
    rec['saved'] = saved
    ok = sum(1 for s in saved if s.get('file'))
    rec['status'] = ('ok' if ok == len(saved) else 'partial') if ok else 'download_failed'
    return rec


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--only')
    ap.add_argument('--worklist')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--resume', action='store_true')
    ap.add_argument('--limit', type=int, default=0)
    ap.add_argument('--shard', type=int, default=0)
    ap.add_argument('--shards', type=int, default=1)
    args = ap.parse_args()

    # Sharded runs get their own browser profile and their own index file —
    # a shared index would be clobbered by concurrent writers.
    global BROWSER_DATA, INDEX
    if args.shards > 1:
        BROWSER_DATA = BROWSER_DATA + '_s%d' % args.shard
        INDEX = os.path.join(OUTDIR, 'doc_index_s%d.json' % args.shard)

    idx = agam_index()
    if args.only:
        tabas = [norm_taba(t) for t in args.only.split(',')]
    elif args.worklist:
        tabas = [norm_taba(t) for t in json.load(open(args.worklist, encoding='utf-8'))]
    else:
        sys.exit('need --only or --worklist')

    index = json.load(open(INDEX, encoding='utf-8')) if os.path.exists(INDEX) else {}
    if args.resume:
        # honour work already recorded by ANY shard, so restarts never redo it
        seen = {}
        for f in sorted(os.listdir(OUTDIR)):
            if f.startswith('doc_index') and f.endswith('.json'):
                try:
                    seen.update(json.load(open(os.path.join(OUTDIR, f), encoding='utf-8')))
                except Exception:
                    pass
        tabas = [t for t in tabas
                 if seen.get(t, {}).get('status') not in ('ok', 'no_traffic_doc')]
    if args.shards > 1:
        tabas = [t for i, t in enumerate(tabas) if i % args.shards == args.shard]
    if args.limit:
        tabas = tabas[:args.limit]
    print('%d plans to process' % len(tabas), flush=True)

    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            BROWSER_DATA, headless=False, accept_downloads=True,
            args=['--disable-blink-features=AutomationControlled'])
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        for n, taba in enumerate(tabas, 1):
            agam = idx.get(taba)
            if not agam:
                index[taba] = {'status': 'no_agam'}
                print('[%d/%d] %s: no agam_id' % (n, len(tabas), taba), flush=True)
                continue
            try:
                rec = await do_plan(page, ctx, taba, agam, dry=args.dry_run)
            except Exception as e:
                rec = {'status': 'error', 'error': str(e)[:200], 'agam': agam}
            index[taba] = rec
            got = [s for s in rec.get('saved', []) if s.get('file')]
            print('[%d/%d] %s: %s (%d docs, %d cand, %d saved)'
                  % (n, len(tabas), taba, rec['status'], len(rec.get('rows', [])),
                     len(rec.get('candidates', [])), len(got)), flush=True)
            for c in rec.get('candidates', [])[:3]:
                print('      cand: %s' % c[:110], flush=True)
            json.dump(index, open(INDEX, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
            await asyncio.sleep(1.5)
        await ctx.close()

asyncio.run(main())
