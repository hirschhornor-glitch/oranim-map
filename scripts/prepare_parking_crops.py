# -*- coding: utf-8 -*-
"""
prepare_parking_crops.py
------------------------
Turn each downloaded traffic appendix into something readable.

Two routes, cheapest first:
  TEXT  — some appendices (and every בה"ת traffic study) are ordinary text PDFs.
          Their parking passages are pulled straight out and saved as .txt.
  CROP  — a CAD sheet has no text layer at all. find_parking_table locates the
          parking-balance block and we render just that block at high DPI.

Output per plan lands in traffic_appendices/crops/<taba>/ and every plan gets a
row in queue.json describing what is waiting to be read.
"""
import argparse, json, os, re, sys, traceback
sys.stdout.reconfigure(encoding='utf-8')
import fitz
from find_parking_table import candidates, render_crop

OUT = r"C:\ORANIM\traffic_appendices"
PDFDIR = os.path.join(OUT, 'pdf')
CROPS = os.path.join(OUT, 'crops')
QUEUE = os.path.join(OUT, 'queue.json')
os.makedirs(CROPS, exist_ok=True)

PARK_TXT = re.compile(r'מאזן\s*חני|דריש[הת]\s*חני|תקן\s*חני|מקומות\s*חני|חישוב\s*חני|'
                      r'סה"כ\s*חני|היצע\s*חני|מקומות\s*חנייה')
MIN_TEXT = 400          # chars on a page before we trust it as a text PDF


def page_text(pg):
    try:
        return pg.get_text()
    except Exception:
        return ''


def do_pdf(taba, path, max_pages=60, min_score=8):
    """Return a queue entry describing how this PDF can be read."""
    res = {'file': path, 'items': []}
    try:
        d = fitz.open(path)
    except Exception as e:
        res['error'] = 'open: %s' % str(e)[:120]
        return res
    res['pages'] = d.page_count

    # ---- text route
    text_hits = []
    for pno in range(min(d.page_count, max_pages)):
        t = page_text(d[pno])
        if len(t) < MIN_TEXT:
            continue
        if PARK_TXT.search(t):
            text_hits.append((pno, t))
    if text_hits:
        tdir = os.path.join(CROPS, taba)
        os.makedirs(tdir, exist_ok=True)
        base = os.path.splitext(os.path.basename(path))[0]
        dest = os.path.join(tdir, '%s_text.txt' % base)
        with open(dest, 'w', encoding='utf-8') as fh:
            for pno, t in text_hits:
                fh.write('\n\n========== page %d ==========\n' % pno)
                fh.write(t)
        res['items'].append({'kind': 'text', 'path': dest,
                             'pages': [p for p, _ in text_hits]})
        return res

    # ---- crop route (CAD sheet)
    tdir = os.path.join(CROPS, taba)
    os.makedirs(tdir, exist_ok=True)
    base = os.path.splitext(os.path.basename(path))[0]
    for pno in range(min(d.page_count, 12)):
        pg = d[pno]
        try:
            cs = candidates(pg)
        except Exception as e:
            res.setdefault('warn', []).append('p%d: %s' % (pno, str(e)[:90]))
            continue
        # keep several: a sheet with plots 01/02/03 carries one parking
        # table per plot and only summing them all reconciles with the units
        top = cs[0]['score'] if cs else 0
        keep = [c for c in cs if c['score'] >= max(min_score, 0.35 * top)][:4]
        for k, c in enumerate(keep):
            dest = os.path.join(tdir, '%s_p%d_k%d.png' % (base, pno, k))
            try:
                _, w, h = render_crop(path, pno, c['rect'], dest)
            except Exception as e:
                res.setdefault('warn', []).append('render p%d: %s' % (pno, str(e)[:90]))
                continue
            res['items'].append({'kind': 'crop', 'path': dest, 'page': pno,
                                 'rect': [round(v) for v in c['rect']],
                                 'score': c['score'], 'strong': c['strong'],
                                 'px': [w, h], 'sample': c['sample'][:120]})
    if not res['items']:
        res['status'] = 'no_table_found'
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--only')
    ap.add_argument('--force', action='store_true')
    ap.add_argument('--limit', type=int, default=0)
    ap.add_argument('--shard', type=int, default=0)
    ap.add_argument('--shards', type=int, default=1)
    args = ap.parse_args()

    # OCR is CPU-bound, so this runs sharded across processes; each shard owns
    # its own queue file and they are merged by merge_queue().
    global QUEUE
    if args.shards > 1:
        QUEUE = os.path.join(OUT, 'queue_s%d.json' % args.shard)

    queue = json.load(open(QUEUE, encoding='utf-8')) if os.path.exists(QUEUE) else {}
    done_elsewhere = set()
    if args.shards > 1:
        for f in os.listdir(OUT):
            if f.startswith('queue') and f.endswith('.json'):
                try:
                    done_elsewhere |= set(json.load(open(os.path.join(OUT, f), encoding='utf-8')))
                except Exception:
                    pass
    tabas = ([t.strip() for t in args.only.split(',')] if args.only
             else sorted(os.listdir(PDFDIR)))
    if args.shards > 1:
        tabas = [t for t in tabas if t not in done_elsewhere or args.force]
        tabas = [t for i, t in enumerate(tabas) if i % args.shards == args.shard]
    done = 0
    for taba in tabas:
        pdir = os.path.join(PDFDIR, taba)
        if not os.path.isdir(pdir):
            continue
        pdfs = [os.path.join(pdir, f) for f in sorted(os.listdir(pdir))
                if f.lower().endswith('.pdf')]
        if not pdfs:
            continue
        if taba in queue and not args.force and \
                len(queue[taba].get('docs', [])) == len(pdfs):
            continue
        docs = []
        for p in pdfs:
            try:
                docs.append(do_pdf(taba, p))
            except Exception:
                docs.append({'file': p, 'error': traceback.format_exc()[-200:]})
        n_txt = sum(1 for d in docs for i in d.get('items', []) if i['kind'] == 'text')
        n_crop = sum(1 for d in docs for i in d.get('items', []) if i['kind'] == 'crop')
        queue[taba] = {'docs': docs, 'n_text': n_txt, 'n_crop': n_crop,
                       'status': 'ready' if (n_txt or n_crop) else 'nothing_found'}
        json.dump(queue, open(QUEUE, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
        print('%s: %d pdf, %d text, %d crops -> %s'
              % (taba, len(pdfs), n_txt, n_crop, queue[taba]['status']), flush=True)
        done += 1
        if args.limit and done >= args.limit:
            break


if __name__ == '__main__':
    main()
