# -*- coding: utf-8 -*-
"""
rescue_nothing_found.py
-----------------------
Second pass for the plans where find_parking_table came up empty.

The structural detector keys on rasterised table blocks and on ruling. It misses
a sheet whose parking table is drawn with hairlines, sits inside the drawing
frame, or states the counts as annotations rather than a ruled table. So here we
OCR the whole sheet in tiles (expensive, hence second) and crop around whatever
tile actually says "מאזן חניה" / "דרישת חניה".

Verified on 1063957, 969162, 1261510 and 1350255 — all four were rescued this
way after the structural pass found nothing usable.
"""
import argparse, json, os, re, sys
sys.stdout.reconfigure(encoding='utf-8')
import fitz
from find_parking_table import render_crop
from ocr_grep_sheet import run as ocr_run

OUT = r"C:\ORANIM\traffic_appendices"
PDFDIR = os.path.join(OUT, 'pdf')
CROPS = os.path.join(OUT, 'crops')
RESCUE = os.path.join(OUT, 'rescue.json')

# Phrases that only appear on an actual parking table/annotation.
STRONG = re.compile(r'מאזן\s*חני|דריש[הת]\s*חני|תקן\s*חני|טבלת\s*חני|מקומות\s*חני|'
                    r'היצע\s*חני|חניות\s*מגורים|חישוב\s*חני')
TILE = 1250.0          # ocr_grep_sheet reports tile origins on this pitch (pdf units)


def rescue_pdf(taba, path, max_pages=3):
    out = []
    try:
        d = fitz.open(path)
    except Exception as e:
        return [{'error': str(e)[:100]}]
    base = os.path.splitext(os.path.basename(path))[0]
    for pno in range(min(d.page_count, max_pages)):
        try:
            lines = ocr_run(path, pno)
        except Exception as e:
            out.append({'page': pno, 'error': str(e)[:100]})
            continue
        tiles = {}
        for x, y, line in lines:
            if STRONG.search(line):
                tiles.setdefault((x, y), []).append(line)
        if not tiles:
            continue
        # richest tile first; a sheet rarely carries more than two such blocks
        for (x, y), hits in sorted(tiles.items(), key=lambda kv: -len(kv[1]))[:2]:
            pg = d[pno]
            rect = [max(0, x - 60), max(0, y - 60),
                    min(pg.rect.width, x + TILE + 60),
                    min(pg.rect.height, y + TILE + 60)]
            dest = os.path.join(CROPS, taba, '%s_p%d_rescue%d_%d.png'
                                % (base, pno, int(x), int(y)))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            try:
                _, w, h = render_crop(path, pno, rect, dest, max_px=2800)
            except Exception as e:
                out.append({'page': pno, 'error': 'render %s' % str(e)[:80]})
                continue
            out.append({'kind': 'crop', 'path': dest, 'page': pno,
                        'rect': [round(v) for v in rect], 'px': [w, h],
                        'hits': hits[:6], 'via': 'ocr_rescue'})
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--only')
    ap.add_argument('--shard', type=int, default=0)
    ap.add_argument('--shards', type=int, default=1)
    args = ap.parse_args()

    dest_file = RESCUE if args.shards == 1 else \
        os.path.join(OUT, 'rescue_s%d.json' % args.shard)
    done = {}
    for f in os.listdir(OUT):
        if f.startswith('rescue') and f.endswith('.json'):
            try:
                done.update(json.load(open(os.path.join(OUT, f), encoding='utf-8')))
            except Exception:
                pass
    mine = json.load(open(dest_file, encoding='utf-8')) if os.path.exists(dest_file) else {}

    if args.only:
        tabas = [t.strip() for t in args.only.split(',')]
    else:
        q = {}
        for f in os.listdir(OUT):
            if f.startswith('queue') and f.endswith('.json'):
                q.update(json.load(open(os.path.join(OUT, f), encoding='utf-8')))
        todo_file = os.path.join(OUT, 'rescue_todo.json')
        if os.path.exists(todo_file):
            # explicit backlog: every unread plan, not just the ones the
            # structural detector gave up on — its false positives (a floor
            # plan scored as a table) cost a wasted read otherwise
            tabas = [t for t in json.load(open(todo_file, encoding='utf-8'))
                     if t not in done]
        else:
            tabas = sorted(t for t, v in q.items()
                           if v.get('status') == 'nothing_found' and t not in done)
        tabas = [t for i, t in enumerate(tabas) if i % args.shards == args.shard]
    print('%d plans to rescue' % len(tabas), flush=True)

    for n, taba in enumerate(tabas, 1):
        pdir = os.path.join(PDFDIR, taba)
        if not os.path.isdir(pdir):
            continue
        items = []
        for f in sorted(os.listdir(pdir)):
            if f.lower().endswith('.pdf'):
                items += rescue_pdf(taba, os.path.join(pdir, f))
        crops = [i for i in items if i.get('kind') == 'crop']
        mine[taba] = {'items': items, 'status': 'ready' if crops else 'still_nothing'}
        json.dump(mine, open(dest_file, 'w', encoding='utf-8'),
                  ensure_ascii=False, indent=1)
        print('[%d/%d] %s: %d crop(s) %s'
              % (n, len(tabas), taba, len(crops),
                 (crops[0]['hits'][0][:60] if crops and crops[0].get('hits') else '')),
              flush=True)


if __name__ == '__main__':
    main()
