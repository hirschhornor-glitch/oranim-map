# -*- coding: utf-8 -*-
"""
find_parking_table.py
---------------------
Locate the parking-balance table (טבלת מאזן חניה) on a traffic-appendix sheet
and render a high-resolution crop of it.

A traffic appendix is a CAD export with no usable text layer, and the sheet can
be 7,000pt wide — rendering the whole thing legibly is hopeless. Two structural
signals narrow it down before any OCR runs:

  1. Pasted tables are usually a BLOCK OF TILED RASTER IMAGES (the table was
     pasted from Excel and rasterised into 100+ tiles). Clustering the image
     placement rects finds that block instantly.
  2. Otherwise the table is drawn as ruling — a stack of horizontal rules
     sharing an x-extent, crossed by verticals.

Each candidate block is then OCR'd (small crop, cheap) and scored on how many
parking words it contains, so we pick the parking table and not the legend,
the area schedule or the drawing frame.
"""
import json, os, re, sys
sys.stdout.reconfigure(encoding='utf-8')
import fitz
import pytesseract
from PIL import Image

pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

PARK = re.compile(r'חני|חנ״|מאזן|נדרש|מוצע|תקן|רכב|אופנ')
STRONG = re.compile(r'חני|חנ״')


# ---------------------------------------------------------------- candidates
def _to_page_space(page, r):
    """A rotated page reports image/drawing rects in un-rotated media space;
    map them into the space page.rect (and get_pixmap clip) uses."""
    r = fitz.Rect(r)
    if page.rotation and not fitz.Rect(page.rect).contains(r):
        r = r * page.rotation_matrix
        r.normalize()
    return r


def image_blocks(page, gap=40, min_tiles=6):
    rects = []
    for im in page.get_images(full=True):
        try:
            for r in page.get_image_rects(im[0]):
                r = _to_page_space(page, r)
                if r.width > 2 and r.height > 2:
                    rects.append(r)
        except Exception:
            pass
    blocks = []
    for r in rects:
        hit = None
        for b in blocks:
            if (r.x0 < b['r'].x1 + gap and r.x1 > b['r'].x0 - gap and
                    r.y0 < b['r'].y1 + gap and r.y1 > b['r'].y0 - gap):
                hit = b
                break
        if hit:
            hit['r'] |= r
            hit['n'] += 1
        else:
            blocks.append({'r': fitz.Rect(r), 'n': 1})
    merged = True
    while merged:
        merged = False
        for i in range(len(blocks)):
            for j in range(i + 1, len(blocks)):
                a, b = blocks[i], blocks[j]
                if (a['r'].x0 < b['r'].x1 + gap and a['r'].x1 > b['r'].x0 - gap and
                        a['r'].y0 < b['r'].y1 + gap and a['r'].y1 > b['r'].y0 - gap):
                    a['r'] |= b['r']
                    a['n'] += b['n']
                    blocks.pop(j)
                    merged = True
                    break
            if merged:
                break
    return [b for b in blocks if b['n'] >= min_tiles]


def _ov(a0, a1, b0, b1):
    lo, hi = max(a0, b0), min(a1, b1)
    return 0.0 if hi <= lo else (hi - lo) / max(1e-6, min(a1 - a0, b1 - b0))


def ruling_blocks(page, min_rows=6, y_gap=220, x_ov=0.55, min_verticals=3):
    hor, ver = [], []
    for it in page.get_cdrawings():
        r = it.get('rect')
        if not r:
            continue
        x0, y0, x1, y1 = _to_page_space(page, r)
        w, h = x1 - x0, y1 - y0
        if h <= 3 and w >= 40:
            hor.append((x0, x1, (y0 + y1) / 2))
        elif w <= 3 and h >= 40:
            ver.append((y0, y1, (x0 + x1) / 2))
    hor.sort(key=lambda l: l[2])
    cl = []
    for x0, x1, y in hor:
        hit = None
        for c in cl:
            if y - c['y1'] <= y_gap and _ov(x0, x1, c['x0'], c['x1']) >= x_ov:
                hit = c
                break
        if hit:
            hit['x0'] = min(hit['x0'], x0); hit['x1'] = max(hit['x1'], x1)
            hit['y1'] = max(hit['y1'], y); hit['n'] += 1
        else:
            cl.append({'x0': x0, 'x1': x1, 'y0': y, 'y1': y, 'n': 1})
    out = []
    for c in cl:
        if c['n'] < min_rows or (c['y1'] - c['y0']) < 40:
            continue
        vs = sum(1 for y0, y1, x in ver
                 if c['x0'] - 5 <= x <= c['x1'] + 5 and _ov(y0, y1, c['y0'], c['y1']) > 0.25)
        if vs >= min_verticals:
            out.append({'r': fitz.Rect(c['x0'], c['y0'], c['x1'], c['y1']), 'n': c['n']})
    return out


# --------------------------------------------------------------------- score
def ocr_text(page, rect, target_px=2000, max_scale=4.0):
    scale = min(max_scale, max(1.0, target_px / max(1.0, rect.width)))
    pm = page.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=rect)
    if pm.width < 30 or pm.height < 30 or pm.width * pm.height > 60_000_000:
        return ''
    img = Image.frombytes('RGB', (pm.width, pm.height), pm.samples)
    return pytesseract.image_to_string(img, lang='heb+eng')


def score_block(page, rect):
    t = ocr_text(page, rect)
    strong = len(STRONG.findall(t))
    words = len(PARK.findall(t))
    return {'strong': strong, 'words': words, 'chars': len(t),
            'score': strong * 3 + words, 'sample': re.sub(r'\s+', ' ', t)[:160]}


def grow(rect, others, y_ov=0.6, gap=150, max_widen=2.5):
    """A parking table often arrives as several blocks side by side (the sheet
    pastes it as separate images, or the ruling breaks at a heading), and a crop
    of one block clips half the columns off. Absorb only close neighbours that
    genuinely share the band, and never let the rect run away across the sheet."""
    r = fitz.Rect(rect)
    w0, h0 = max(1.0, r.width), max(1.0, r.height)
    for _ in range(2):
        for o in others:
            o = fitz.Rect(o)
            if r.contains(o) or o.is_empty:
                continue
            side_by_side = (_ov(r.y0, r.y1, o.y0, o.y1) >= y_ov and
                            min(abs(o.x0 - r.x1), abs(r.x0 - o.x1)) <= gap)
            stacked = (_ov(r.x0, r.x1, o.x0, o.x1) >= y_ov and
                       min(abs(o.y0 - r.y1), abs(r.y0 - o.y1)) <= gap)
            if not (side_by_side or stacked):
                continue
            cand = fitz.Rect(r) | o
            if cand.width <= w0 * max_widen and cand.height <= h0 * max_widen:
                r = cand
    return r


def widen_rtl(page, rect, right=2.0, left=0.3, tall=0.15):
    """These tables are Hebrew: the use/label column sits on the RIGHT and the
    numeric columns run off to the left. Detection usually latches onto the
    ruled numeric half, so widen mostly rightwards to recover the labels."""
    r = fitz.Rect(rect)
    w, h = r.width, r.height
    return fitz.Rect(max(0, r.x0 - left * w), max(0, r.y0 - tall * h),
                     min(page.rect.width, r.x1 + right * w),
                     min(page.rect.height, r.y1 + tall * h))


def candidates(page, max_eval=8):
    cands = []
    for b in image_blocks(page):
        cands.append({'kind': 'image', 'r': b['r'], 'tiles': b['n']})
    for b in ruling_blocks(page):
        r = b['r']
        if any(c['r'].intersects(r) and (c['r'] & r).get_area() > 0.6 * r.get_area()
               for c in cands):
            continue
        cands.append({'kind': 'ruling', 'r': r, 'tiles': b['n']})
    allr = [c['r'] for c in cands]
    # biggest first — the parking balance is never a small block
    cands.sort(key=lambda c: -c['r'].get_area())
    out = []
    for c in cands[:max_eval]:
        s = score_block(page, c['r'])
        c.update(s)
        out.append(c)
    out.sort(key=lambda c: -c['score'])
    # only the winner is grown: a block that scored zero is not worth widening,
    # and growing everything just chains the whole sheet into one rect
    if out and out[0]['score'] > 0:
        best = out[0]
        for label, cand in (('grown', grow(best['r'], allr)),
                            ('widened', widen_rtl(page, best['r']))):
            if cand == best['r']:
                continue
            s = score_block(page, cand)
            # only take the wider view when it genuinely exposes more of the table
            if s['strong'] > best['strong'] * 1.25 or s['score'] > best['score'] * 1.25:
                best['r'] = cand
                best.update(s)
                best[label] = True
    for c in out:
        c['rect'] = [c['r'].x0, c['r'].y0, c['r'].x1, c['r'].y1]
        del c['r']
    return out


def detect_rotation(pg, rect, scale=1.2):
    """CAD sheets are often drawn sideways or upside-down inside an upright
    page, which makes the crop unreadable. Ask tesseract's orientation
    detector on a cheap preview and return the rotation to apply."""
    try:
        pm = pg.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=fitz.Rect(rect))
        if pm.width < 60 or pm.height < 60:
            return 0
        img = Image.frombytes('RGB', (pm.width, pm.height), pm.samples)
        osd = pytesseract.image_to_osd(img, output_type=pytesseract.Output.DICT)
        rot = int(osd.get('rotate', 0)) % 360
        return rot if rot in (90, 180, 270) else 0
    except Exception:
        return 0


def render_crop(pdf, page_no, rect, dest, target_px=2600, max_px=2600, rotate=None):
    d = fitz.open(pdf)
    pg = d[page_no]
    r = fitz.Rect(rect)
    scale = min(max_px / max(1.0, r.width), max_px / max(1.0, r.height))
    scale = max(0.5, min(scale, 6.0))
    if rotate is None:
        rotate = detect_rotation(pg, r)
    m = fitz.Matrix(scale, scale)
    if rotate:
        m = m.prerotate(rotate)
    pm = pg.get_pixmap(matrix=m, clip=r)
    pm.save(dest)
    return dest, pm.width, pm.height


if __name__ == '__main__':
    path = sys.argv[1]
    d = fitz.open(path)
    for pno in range(min(d.page_count, 3)):
        cs = candidates(d[pno])
        print('== page', pno, d[pno].rect)
        for c in cs[:5]:
            print(' %-6s tiles=%3d score=%3d strong=%2d  rect=%s\n        %s'
                  % (c['kind'], c['tiles'], c['score'], c['strong'],
                     [round(v) for v in c['rect']], c['sample'][:110]))
