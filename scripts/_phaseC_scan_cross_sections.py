"""Scan transport-related PDFs for pages containing street cross-sections (חתכים).

Heuristic: a page is a "cross-section" page if its text contains "חתך" AND
a street/road-related keyword (רחוב, ציר, מסעה, נתיב, מדרכה).

Outputs:
  scripts/_phaseC_cross_sections.json — manifest of {pdf, page, text_snippet, street_hints}
"""
from __future__ import annotations
import json, re, sys, io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

try:
    import fitz  # PyMuPDF
except ImportError:
    print("PyMuPDF (fitz) not installed", file=sys.stderr); sys.exit(1)

# Find all PDFs in projector tikiya
TIKIYA = Path(r"C:\Users\orlev\OneDrive - Municipality of Jerusalem\פרוייקטור שכונתי תוצרים")
if not TIKIYA.exists():
    print(f"NOT FOUND: {TIKIYA}", file=sys.stderr); sys.exit(2)

pdfs = list(TIKIYA.rglob("*.pdf"))
print(f"Total PDFs in tikiya: {len(pdfs)}")

# Focus on transport-related PDFs
TRANSPORT_KW = ("כבישים", "תנועה", "תח\"צ", "תחבורה", "דוח מסכם", "תיק שכונה")
transport_pdfs = [p for p in pdfs if any(kw in p.name for kw in TRANSPORT_KW)]
print(f"\nTransport-related PDFs ({len(transport_pdfs)}):")
for p in transport_pdfs:
    print(f"  {p.name}")

# Scan each for cross-section pages
print(f"\n=== Scanning for cross-sections ===")
CROSS_SECTION_KW = re.compile(r"חתך|מסעה|רוחב\s*נתיב|רוחב\s*מדרכה|פרופיל\s*רחוב")
STREET_KW = re.compile(r"רחוב|ציר|מדרכה|נתיב|מסעה|אופניים|חניה")

manifest = []
for pdf_path in transport_pdfs:
    try:
        doc = fitz.open(str(pdf_path))
    except Exception as e:
        print(f"  ERR opening {pdf_path.name}: {e}")
        continue
    found_in = 0
    for i, page in enumerate(doc):
        try:
            text = page.get_text()
        except Exception:
            continue
        if not text:
            continue
        cs_hits = CROSS_SECTION_KW.findall(text)
        st_hits = STREET_KW.findall(text)
        if len(cs_hits) >= 1 and len(st_hits) >= 1:
            # Extract a snippet (first 200 chars)
            snippet = re.sub(r"\s+", " ", text)[:300]
            # Find street names — heuristic: words after רח'/רחוב/ציר
            street_hints = []
            for m in re.finditer(r"(?:רח[\'’]?|רחוב|ציר)\s+([^\d\.,\n]{2,30})", text):
                name = m.group(1).strip()
                if 2 <= len(name) <= 30:
                    street_hints.append(name)
            street_hints = list(dict.fromkeys(street_hints))[:5]  # dedup, top 5
            manifest.append({
                "pdf": pdf_path.name,
                "page": i + 1,
                "cross_section_keyword_hits": len(cs_hits),
                "street_hints": street_hints,
                "snippet": snippet,
            })
            found_in += 1
    print(f"  {pdf_path.name}: {found_in} cross-section pages")

# Write manifest
out = Path(r"C:\ORANIM\oranim-app\scripts\_phaseC_cross_sections.json")
out.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"\n=== Wrote manifest: {out} ({len(manifest)} pages) ===")
