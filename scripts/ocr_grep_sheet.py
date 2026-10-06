# -*- coding: utf-8 -*-
"""OCR a whole CAD sheet in tiles and print every line mentioning parking.

Used when find_parking_table finds no balance table: some sheets state the
counts as annotations on the parking-floor plans ("100 חניות מגורים") instead
of in a table. Cheaper than eyeballing the sheet.
"""
import re, sys
sys.stdout.reconfigure(encoding='utf-8')
import fitz
import pytesseract
from PIL import Image

pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
KEY = re.compile(r'חני|מאזן|תקן|מקומות')


def run(path, page_no=0, scale=1.8, tile=2400, overlap=150):
    d = fitz.open(path)
    pg = d[page_no]
    W, H = pg.rect.width * scale, pg.rect.height * scale
    out = []
    for oy in range(0, int(H), tile - overlap):
        for ox in range(0, int(W), tile - overlap):
            clip = fitz.Rect(ox / scale, oy / scale,
                             min(W, ox + tile) / scale, min(H, oy + tile) / scale)
            if clip.width < 5 or clip.height < 5:
                continue
            pm = pg.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=clip)
            img = Image.frombytes('RGB', (pm.width, pm.height), pm.samples)
            for line in pytesseract.image_to_string(img, lang='heb+eng').splitlines():
                line = line.strip()
                if line and KEY.search(line):
                    out.append((round(clip.x0), round(clip.y0), line))
    return out


if __name__ == '__main__':
    p = sys.argv[1]
    pno = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    seen = set()
    for x, y, line in run(p, pno):
        if line in seen:
            continue
        seen.add(line)
        print('(%5d,%5d) %s' % (x, y, line))
