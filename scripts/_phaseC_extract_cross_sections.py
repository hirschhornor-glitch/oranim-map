"""Extract cross-section pages to JPG thumbnails.

Reads `_phaseC_cross_sections.json`, renders each listed page as a JPG to
`data/cross_sections/<basename>_p<page>.jpg`, and updates the manifest with
the output URL (relative path).

Output JPG specs:
  - Width ~1200 px (matrix scale)
  - JPEG quality 80
  - Typical size: 50-150 KB per page (37 pages → ~3 MB total)
"""
from __future__ import annotations
import json, sys, io, re
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

try:
    import fitz  # PyMuPDF
except ImportError:
    print("PyMuPDF not installed", file=sys.stderr); sys.exit(1)

TIKIYA = Path(r"C:\Users\orlev\OneDrive - Municipality of Jerusalem\פרוייקטור שכונתי תוצרים")
MANIFEST = Path(r"C:\ORANIM\oranim-app\scripts\_phaseC_cross_sections.json")
OUT_DIR = Path(r"C:\ORANIM\oranim-app\data\cross_sections")
OUT_DIR.mkdir(parents=True, exist_ok=True)

manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
print(f"Manifest: {len(manifest)} pages to extract")

# Index PDF paths by basename
pdf_paths = {p.name: p for p in TIKIYA.rglob("*.pdf")}

# Open each PDF once
docs = {}
def get_doc(name):
    if name not in docs:
        p = pdf_paths.get(name)
        if not p:
            return None
        try:
            docs[name] = fitz.open(str(p))
        except Exception as e:
            print(f"  ERR opening {name}: {e}")
            docs[name] = None
    return docs[name]

def safe_basename(s):
    return re.sub(r"[^a-zA-Z0-9_-]+", "_", s)[:40].strip("_")

total_bytes = 0
extracted = []
for entry in manifest:
    pdf_name = entry["pdf"]
    page_num = entry["page"]  # 1-indexed
    doc = get_doc(pdf_name)
    if not doc:
        entry["error"] = "pdf not found"
        continue
    if page_num - 1 >= len(doc):
        entry["error"] = f"page {page_num} out of range ({len(doc)} pages)"
        continue
    page = doc[page_num - 1]
    # Render at ~150 DPI (matrix 2x)
    mat = fitz.Matrix(2, 2)
    pix = page.get_pixmap(matrix=mat)
    base = safe_basename(pdf_name.replace(".pdf", ""))
    out_name = f"{base}_p{page_num:03d}.jpg"
    out_path = OUT_DIR / out_name
    try:
        pix.pil_save(str(out_path), format="JPEG", quality=80, optimize=True)
        size = out_path.stat().st_size
        total_bytes += size
        entry["url"] = f"data/cross_sections/{out_name}"
        entry["bytes"] = size
        extracted.append(entry)
        print(f"  ✓ {out_name} ({size//1024} KB)")
    except Exception as e:
        # Fallback: try save as PNG then re-encode
        entry["error"] = str(e)
        print(f"  ✗ {out_name}: {e}")

# Write updated manifest
MANIFEST.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"\nTotal extracted: {len(extracted)} pages, {total_bytes/1024/1024:.2f} MB")
print(f"Output dir: {OUT_DIR}")
