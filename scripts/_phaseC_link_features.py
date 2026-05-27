"""Build a feature → cross-sections mapping.

Reads `_phaseC_cross_sections.json` (manifest) and `projector_gonenim.geojson`.
For each feature, finds cross-sections whose extracted text mentions the
feature's project_name street(s).

Output: writes a SUPPLEMENTAL file `data/cross_sections_index.json`
  { "<project_id>": [{ "pdf": ..., "page": ..., "url": ..., "snippet": ..., "match": "..." }] }
The frontend will fetch this on map load and surface a button per feature.
"""
from __future__ import annotations
import json, sys, io, re
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

MANIFEST = Path(r"C:\ORANIM\oranim-app\scripts\_phaseC_cross_sections.json")
FEATURES = Path(r"C:\ORANIM\oranim-app\data\projector_gonenim.geojson")
OUT = Path(r"C:\ORANIM\oranim-app\data\cross_sections_index.json")

manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
gj = json.loads(FEATURES.read_text(encoding="utf-8"))

# Common street names to look for in features. We extract them from PDF
# snippets and also from feature project_names themselves.
STREET_RE = re.compile(r"(?:רח[\'’]?|רחוב|ציר)\s+([א-ת][א-ת\s\"\'״׳]{1,25})")

# Index manifest entries by streets they mention (from snippets)
def normalize_street(s):
    return re.sub(r"\s+", " ", s.strip().replace('"', '"').replace("'", "'"))

cs_by_street = {}  # street_name → list of manifest entries
for entry in manifest:
    if entry.get("error"):
        continue
    text = (entry.get("snippet") or "")
    # Common street keywords directly in text — match on substrings.
    for m in STREET_RE.finditer(text):
        st = normalize_street(m.group(1))
        if 2 < len(st) < 25:
            cs_by_street.setdefault(st, []).append(entry)
    # Also use street_hints
    for st in entry.get("street_hints", []):
        st = normalize_street(st)
        if 2 < len(st) < 25:
            cs_by_street.setdefault(st, []).append(entry)

print(f"Streets indexed from cross-sections: {len(cs_by_street)}")

# Manual high-value mappings — explicit feature-name keyword → list of cross-section pages.
# This handles cases where automatic matching is unreliable.
KEYWORD_TO_CS = {
    "הרצוג": ["28", "29", "30", "31", "32", "33"],  # רסקו תיק שכונה
    "סן מרטין": ["34"],  # גוננים ח-ט ופת דוח מסכם
    "רבי צדוק": ["32"],  # גוננים ח-ט ופת דוח מסכם
    "השומר": ["33"],  # גוננים ח-ט ופת דוח מסכם
    "נוטרים": ["33"],
    "מרגולין": ["33"],
    "חוות הנוער": ["14"],  # גוננים תיק שכונה
    # User-flagged 2026-05-28: בית שוויץ "שדרוג רחובות" — the road work
    # spans רחוב השומר which is documented on page 33.
    "בית שוויץ": ["33"],
}

# For each feature, look for street matches in project_name
out_index = {}  # project_id → [cross_section entries]
matched_features = 0
total_links = 0

# Build a quick lookup: PDF+page → manifest entry
m_by_pp = {(e["pdf"], e["page"]): e for e in manifest if not e.get("error")}

# Map keyword → PDFs likely to contain it
KEYWORD_TO_PDFS = {
    "הרצוג":      ["25-09-30 רסקו - תיק שכונה.pptx.pdf"],
    "סן מרטין":   ["גוננים ח-ט ופת- דוח מסכם.pdf"],
    "רבי צדוק":   ["גוננים ח-ט ופת- דוח מסכם.pdf"],
    "השומר":      ["גוננים ח-ט ופת- דוח מסכם.pdf"],
    "נוטרים":     ["גוננים ח-ט ופת- דוח מסכם.pdf"],
    "מרגולין":    ["גוננים ח-ט ופת- דוח מסכם.pdf"],
    "חוות הנוער": ["25-12-29 גוננים א-ו - תיק שכונה.pdf"],
    "בית שוויץ":  ["גוננים ח-ט ופת- דוח מסכם.pdf"],
}

for f in gj["features"]:
    p = f["properties"]
    name = p.get("project_name") or ""
    pid = p.get("project_id")
    if not pid:
        continue
    cs_for_feat = []
    seen_keys = set()
    # When a project name matches a keyword, the button label uses that keyword.
    # But sometimes the project name and the cross-section street differ
    # (e.g. "בית שוויץ" project shows a cross-section of רחוב השומר).
    # MATCH_LABEL_OVERRIDE remaps the button label per keyword.
    MATCH_LABEL_OVERRIDE = {
        "בית שוויץ": "השומר",
    }
    for kw, pages in KEYWORD_TO_CS.items():
        if kw in name:
            label = MATCH_LABEL_OVERRIDE.get(kw, kw)
            for pdf in KEYWORD_TO_PDFS.get(kw, []):
                for pg in pages:
                    pg_i = int(pg)
                    e = m_by_pp.get((pdf, pg_i))
                    if e and (pdf, pg_i) not in seen_keys:
                        seen_keys.add((pdf, pg_i))
                        cs_for_feat.append({
                            "pdf": pdf,
                            "page": pg_i,
                            "url": e.get("url"),
                            "match": label,
                        })
    if cs_for_feat:
        out_index[pid] = cs_for_feat
        matched_features += 1
        total_links += len(cs_for_feat)

OUT.write_text(json.dumps(out_index, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"\nFeatures with cross-sections linked: {matched_features}")
print(f"Total links: {total_links}")
print(f"Wrote: {OUT}")
