"""Parser for the Mavat 'זכויות והוראות בניה XLS' file (which is actually HTML).

Public API:
    parse_table5_xlsx(path) -> ParseResult | None
"""
from __future__ import annotations

import html
import io
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd


CONDITIONAL_USE_KEYS = ["מותנה", "מותנות", "מותנ"]
RENTAL_USE_KEYS = ["השכרה", "מיוחד", "מוגן", "קשיש", "סיעוד"]
TOTAL_ROW_MARKERS = ["<סך הכל>", "סך הכל", "סכום", "סה\"כ"]

# Use-category classification (matched on the שימוש column, fallback יעוד).
RESID_KEYS      = ["מגורים", "דיור"]
COMMERCE_KEYS   = ["מסחר"]
EMPLOYMENT_KEYS = ["תעסוקה", "משרד", "תעשיה", "תעשייה"]
PUBLIC_KEYS     = ["מבנים ומוסדות ציבור", "מוסדות ציבור", "מוסד ציבור",
                   "מבני ציבור", "מבנה ציבור"]
SHATZAP_KEYS    = ["שטח ציבורי פתוח", "שטח פרטי פתוח", 'שצ"פ', "שצפ", "שצ”פ"]

# ── Shared-tenant ("חדר דיירים") spaces ──────────────────────────────────────
# Private common areas that serve the building's OWN residents — shared leisure
# floors, shared balconies, a tenants' club room. These are NOT public program
# (שב"צ / שצ"פ) and must never be merged with it — see the memory
# reference_program_vs_shared_tenant_spaces. They live only in the Table 5
# free-text notes, e.g. plan 101-1563642 (אריה בעהם):
#   "150 מ\"ר לטובת שטחי פנאי משותפים בכל מבנה מגורים"
#   "500 מ\"ר לטובת מרפסות משותפות לטובת הדיירים"
# Quote glyphs vary across files: straight ' / ", Hebrew gershayim (U+05F4),
# and the curly/prime family — so מ"ר is matched permissively.
_QUOTE = r"[\"'״’″“”‘]"
_SQM_RE = re.compile(r"(\d[\d,]*)\s*מ" + _QUOTE + r"{0,2}ר")
# (label, pattern) — first match in a window wins, so order by specificity.
_SHARED_TENANT_CATS = [
    ("מרפסות משותפות",   re.compile(r"מרפסות\s+משותפ")),
    ("פנאי משותף",        re.compile(r"פנאי\s+משותפ")),
    ("חדר דיירים",        re.compile(r"חדר(?:י)?\s+דייר")),
    ("שטח משותף לדיירים", re.compile(r"משותפ\w*\s+לדייר|משותפ\w*\s+לטובת\s+הדייר")),
]


def _shared_tenant_spaces(note_txt: str) -> Tuple[float, List[str]]:
    """Extract shared-tenant ("חדר דיירים") areas from Table 5 note text.

    Anchors on each "<num> מ\"ר" figure and keeps it only when a shared-tenant
    keyword sits in the surrounding window, so ordinary floor-area numbers (e.g.
    a יח"ד size mix "40 מ\"ר") are not swept in.

    Returns (total_sqm, notes) where notes is a list of "<label>: <n> מ\"ר"
    strings, one per matched mention (per-building footnotes stay separate).
    """
    total = 0.0
    notes: List[str] = []
    seen = set()
    for m in _SQM_RE.finditer(note_txt):
        win = note_txt[max(0, m.start() - 30): m.end() + 45]
        label = next((lab for lab, pat in _SHARED_TENANT_CATS if pat.search(win)), None)
        if not label:
            continue
        val = _to_float(m.group(1))
        if val <= 0:
            continue
        key = (label, val, m.start())
        if key in seen:
            continue
        seen.add(key)
        total += val
        notes.append(f'{label}: {int(val) if float(val).is_integer() else val} מ"ר')
    return total, notes


def _categories(text: str):
    """Set of use-categories present in a use/yiyud string."""
    cats = set()
    if not text:
        return cats
    if any(k in text for k in RESID_KEYS):      cats.add("resid")
    if any(k in text for k in COMMERCE_KEYS):   cats.add("commerce")
    if any(k in text for k in EMPLOYMENT_KEYS): cats.add("employment")
    if any(k in text for k in PUBLIC_KEYS):     cats.add("public")
    if any(k in text for k in SHATZAP_KEYS):    cats.add("shatzap")
    return cats


@dataclass
class XlsxRow:
    yiyud: str = ""
    use: str = ""
    parcel: str = ""
    location: str = ""
    plot_size_sqm: float = 0.0
    primary_above_sqm: float = 0.0
    service_above_sqm: float = 0.0
    primary_below_sqm: float = 0.0
    service_below_sqm: float = 0.0
    total_building_sqm: float = 0.0
    total_above_sqm: float = 0.0
    total_below_sqm: float = 0.0
    units: float = 0.0
    height_m: float = 0.0
    floors_above: float = 0.0
    is_conditional: bool = False
    is_rental_candidate: bool = False
    is_total: bool = False


@dataclass
class ParseResult:
    rental_units: int = 0
    rental_duration_years: int = 0   # from the Table 5 note ("להשכרה לתקופה של N שנה")
    conditional_units: int = 0
    max_height_m: float = 0.0
    max_floors: int = 0
    total_residential_units: int = 0
    # Expanded aggregation (planned/approved state — maps to GS "out" columns)
    total_units: int = 0              # sum of יח"ד across all rows
    commerce_sqm: float = 0.0         # built area of pure-מסחר rows
    employment_sqm: float = 0.0       # built area of pure-תעסוקה/משרדים rows
    public_building_sqm: float = 0.0  # built area of pure-מבנים ומוסדות ציבור (שב"צ) — יעוד must be public
    hafrash_built_sqm: float = 0.0    # built area of institutional space WITHIN a residential lot (הפרשה מבונה)
    shatzap_sqm: float = 0.0          # land area of שטח ציבורי פתוח (שצ"פ) rows
    resident_shared_sqm: float = 0.0  # shared-tenant ("חדר דיירים") area from notes — PRIVATE, not program
    shared_tenant_notes: str = ""     # per-mention breakdown, e.g. 'פנאי משותף: 150 מ"ר; מרפסות משותפות: 500 מ"ר'
    mixed_use_sqm: float = 0.0        # built area of multi-use rows (can't be split)
    total_building_sqm_all: float = 0.0
    rows: List[dict] = field(default_factory=list)
    rental_candidate_uses: List[str] = field(default_factory=list)
    columns_found: List[Tuple] = field(default_factory=list)
    column_mapping: dict = field(default_factory=dict)
    raw_html_tables: int = 0
    error: str = ""


def _clean(val) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    if s.lower() == "nan" or s == "&nbsp;":
        return ""
    return s


def _to_float(val) -> float:
    s = _clean(val)
    if not s:
        return 0.0
    s = re.sub(r"\(\d+\)", "", s).strip()
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _is_total_row(row: XlsxRow) -> bool:
    if any(m in row.use for m in TOTAL_ROW_MARKERS):
        return True
    if any(m in row.yiyud for m in TOTAL_ROW_MARKERS):
        return True
    if any(m in row.parcel for m in TOTAL_ROW_MARKERS):
        return True
    # Grand-total row at the end: yiyud + use both empty but has numeric content
    # (parcel and units may contain aggregate sums)
    if not row.yiyud and not row.use and (
        row.units > 0 or row.height_m > 0 or row.floors_above > 0 or row.primary_above_sqm > 0
    ):
        return True
    return False


def _matches_any(text: str, keys: List[str]) -> bool:
    if not text:
        return False
    return any(k in text for k in keys)


def _bldg_area(row: "XlsxRow") -> float:
    """Total built area for a row, robust to the different Table 5 layouts."""
    if row.total_building_sqm:
        return row.total_building_sqm
    detailed = (row.primary_above_sqm + row.service_above_sqm
                + row.primary_below_sqm + row.service_below_sqm)
    if detailed:
        return detailed
    return row.total_above_sqm + row.total_below_sqm


def _norm_level(s) -> str:
    s = str(s)
    return "" if s.startswith("Unnamed:") else s


def _resolve_columns(columns) -> dict:
    """Use full multi-index path to disambiguate same-named level-3 columns.

    Returns dict mapping semantic name → column index.
    """
    # Build list of (idx, levels) where levels is a tuple of normalized levels
    cols = []
    for idx, c in enumerate(columns):
        if isinstance(c, tuple):
            levels = tuple(_norm_level(x) for x in c)
        else:
            levels = (_norm_level(c),)
        cols.append((idx, levels))

    def find_one(predicate, exclude_indices=()):
        for idx, levels in cols:
            if idx in exclude_indices:
                continue
            if predicate(levels):
                return idx
        return None

    mapping: dict = {}

    # 1. יעוד ראשי
    mapping["yiyud"] = find_one(lambda L: "יעוד ראשי" in L)
    # 2. שימוש
    mapping["use"] = find_one(lambda L: "שימוש" in L)
    # 3. תאי שטח
    mapping["parcel"] = find_one(lambda L: "תאי שטח" in L)
    # 4. בניין / מקום
    mapping["location"] = find_one(lambda L: "בניין / מקום" in L or "בניין/מקום" in L)
    # 5. גודל מגרש מוחלט
    mapping["plot_size"] = find_one(lambda L: "גודל מגרש מוחלט" in L)
    # 6. שטחי בניה / מעל / עיקרי
    mapping["primary_above"] = find_one(
        lambda L: ("שטחי בניה" in L and "מעל הכניסה הקובעת" in L and "עיקרי" in L)
                  or ("עיקרי" in L and any("שטחי" in lev for lev in L))
    )
    # service above
    mapping["service_above"] = find_one(
        lambda L: ("שטחי בניה" in L and "מעל הכניסה הקובעת" in L and "שרות" in L)
                  or ("שרות" in L and "מעל הכניסה הקובעת" in L)
    )
    # total building area — summary layout uses a single "סה"כ שטחי בניה" column
    # instead of the detailed עיקרי/שרות × מעל/מתחת breakdown
    mapping["total_building"] = find_one(
        lambda L: 'סה"כ שטחי בניה' in L or 'סהכ שטחי בניה' in L
                  or ("שטחי בניה" in L and any('סה"כ' in lev or 'סהכ' in lev for lev in L))
    )
    # combined layout — "שטחי בניה כוללים" (total areas, no עיקרי/שרות split) × מעל/מתחת
    mapping["total_above"] = find_one(
        lambda L: "שטחי בניה כוללים" in L and "מעל הכניסה הקובעת" in L and "מספר קומות" not in L
    )
    mapping["total_below"] = find_one(
        lambda L: "שטחי בניה כוללים" in L and "מתחת לכניסה הקובעת" in L and "מספר קומות" not in L
    )
    # primary below — שטחי בניה / מתחת לכניסה הקובעת / עיקרי (exclude floors columns)
    mapping["primary_below"] = find_one(
        lambda L: "מתחת לכניסה הקובעת" in L and "עיקרי" in L and "מספר קומות" not in L
    )
    # service below
    mapping["service_below"] = find_one(
        lambda L: "מתחת לכניסה הקובעת" in L and "שרות" in L and "מספר קומות" not in L
    )
    # units
    mapping["units"] = find_one(
        lambda L: any('יח"ד' in lev or 'יחד' in lev for lev in L)
                  and not any("קומות" in lev for lev in L)
    )
    # height — header "גובה מבנה- מעל הכניסה הקובעת" or unit=מטר with "גובה"
    mapping["height"] = find_one(
        lambda L: any("גובה" in lev for lev in L)
    )
    # floors above — level1="מספר קומות" and level3="מעל הכניסה הקובעת"
    mapping["floors_above"] = find_one(
        lambda L: "מספר קומות" in L and "מעל הכניסה הקובעת" in L
    )
    # floors below
    mapping["floors_below"] = find_one(
        lambda L: "מספר קומות" in L and "מתחת לכניסה הקובעת" in L
    )

    return {k: v for k, v in mapping.items() if v is not None}


def parse_table5_xlsx(path: Path) -> Optional[ParseResult]:
    path = Path(path)
    if not path.exists() or path.stat().st_size < 200:
        return None

    try:
        content = path.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        r = ParseResult()
        r.error = f"read_error: {e}"
        return r

    cleaned = re.sub(r"<sup[^>]*>\([0-9]+\)</sup>\s*", "", content)

    # Rental period from the free-text note ("יח\"ד להשכרה לתקופה של 15 שנה").
    # Parsed from the note text, not the table — kept here so it rides along with
    # rental_units. Strip tags first so the phrase isn't split across elements.
    _note_txt = html.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", content)))
    _rent_years = 0
    _m = re.search(r"להשכרה[^.]{0,40}?לתקופה של\s*(\d+)\s*שנ", _note_txt) \
        or re.search(r"השכרה[^.]{0,30}?(\d+)\s*שנ", _note_txt)
    if _m:
        try: _rent_years = int(_m.group(1))
        except ValueError: _rent_years = 0

    try:
        tables = pd.read_html(io.StringIO(cleaned), encoding="utf-8")
    except Exception as e:
        r = ParseResult()
        r.error = f"read_html_error: {e}"
        return r

    if not tables:
        r = ParseResult()
        r.error = "no_tables"
        return r

    result = ParseResult(raw_html_tables=len(tables))
    result.rental_duration_years = _rent_years

    # Shared-tenant ("חדר דיירים") areas live only in the free-text notes.
    _shared_total, _shared_notes = _shared_tenant_spaces(_note_txt)
    result.resident_shared_sqm = _shared_total
    result.shared_tenant_notes = "; ".join(_shared_notes)

    t = tables[0]
    # Store columns for debugging
    result.columns_found = [tuple(c) if isinstance(c, tuple) else (c,) for c in t.columns]
    mapping = _resolve_columns(t.columns)
    result.column_mapping = mapping

    def g(row, key):
        idx = mapping.get(key)
        if idx is None:
            return None
        return row.iloc[idx] if idx < len(row) else None

    rows_data: List[XlsxRow] = []
    for _, raw in t.iterrows():
        row = XlsxRow(
            yiyud   = _clean(g(raw, "yiyud")),
            use     = _clean(g(raw, "use")),
            parcel  = _clean(g(raw, "parcel")),
            location= _clean(g(raw, "location")),
            plot_size_sqm     = _to_float(g(raw, "plot_size")),
            primary_above_sqm = _to_float(g(raw, "primary_above")),
            service_above_sqm = _to_float(g(raw, "service_above")),
            primary_below_sqm = _to_float(g(raw, "primary_below")),
            service_below_sqm = _to_float(g(raw, "service_below")),
            total_building_sqm = _to_float(g(raw, "total_building")),
            total_above_sqm   = _to_float(g(raw, "total_above")),
            total_below_sqm   = _to_float(g(raw, "total_below")),
            units             = _to_float(g(raw, "units")),
            height_m          = _to_float(g(raw, "height")),
            floors_above      = _to_float(g(raw, "floors_above")),
        )
        row.is_total = _is_total_row(row)
        if _matches_any(row.use, CONDITIONAL_USE_KEYS) or _matches_any(row.location, CONDITIONAL_USE_KEYS):
            row.is_conditional = True
        if _matches_any(row.use, RENTAL_USE_KEYS) or _matches_any(row.location, RENTAL_USE_KEYS):
            row.is_rental_candidate = True
        rows_data.append(row)

    # Aggregate (excluding total rows)
    for r in rows_data:
        if r.is_total:
            continue
        if r.units > 0:
            result.total_units += int(r.units)
            if r.is_conditional:
                result.conditional_units += int(r.units)
            if r.is_rental_candidate:
                result.rental_units += int(r.units)
                if r.use and r.use not in result.rental_candidate_uses:
                    result.rental_candidate_uses.append(r.use)
            # יח"ד "מותנות" are conditional rights (their realization is tied to a
            # קרן תחזוקה / long-term maintenance fund — see Table 5 note ד). By
            # convention they are ADDITIVE extras, not part of the proposed unit
            # count, so exclude them from total_residential_units (they remain
            # tracked in conditional_units). Rental units ARE proposed — keep them.
            if "מגורים" in r.use and not r.is_conditional:
                result.total_residential_units += int(r.units)
        if r.height_m > result.max_height_m:
            result.max_height_m = r.height_m
        if r.floors_above > result.max_floors:
            result.max_floors = int(r.floors_above)

        # Area aggregation by use-category. Area columns are total-per-parcel and
        # NOT split by use, so attribute area only when a row is single-purpose
        # (one non-residential category and not residential); multi-use rows go to
        # mixed_use_sqm rather than being double-counted.
        area = _bldg_area(r)
        result.total_building_sqm_all += area
        # יעוד (lot zoning) and שימוש (specific use) carry different signals.
        # שב"צ vs הפרשה מבונה distinction (2026-06-07, plan 1300003):
        #   - True שב"צ: יעוד=מבני ציבור AND שימוש=מבני ציבור → public_building_sqm
        #   - הפרשה מבונה: יעוד=מגורים AND שימוש=מוסדות ציבור → hafrash_built_sqm
        # Without checking יעוד, the "מבנים ומוסדות ציבור" row of a residential
        # plot was wrongly counted as שב"צ.
        yiyud_cats = _categories(r.yiyud)
        use_cats = _categories(r.use)
        cats = use_cats or yiyud_cats  # generic combined view (back-compat)
        # שצ"פ is open land — use plot area, not built area, and count even if mixed.
        if "shatzap" in cats:
            result.shatzap_sqm += r.plot_size_sqm
        # Hafrash override: institutional space inside a residential lot.
        if "public" in use_cats and "resid" in yiyud_cats and "public" not in yiyud_cats:
            result.hafrash_built_sqm += area
        else:
            non_resid = cats - {"resid", "shatzap"}
            if len(non_resid) == 1 and "resid" not in cats:
                c = next(iter(non_resid))
                if c == "commerce":
                    result.commerce_sqm += area
                elif c == "employment":
                    result.employment_sqm += area
                elif c == "public":
                    result.public_building_sqm += area
            elif non_resid:
                # 2+ non-residential categories, or non-resid mixed with residential
                result.mixed_use_sqm += area

    result.rows = [asdict(r) for r in rows_data]
    return result


def result_to_dict(r: ParseResult, include_rows: bool = False) -> dict:
    out = {
        "rental_units": r.rental_units,
        "conditional_units": r.conditional_units,
        "rental_duration_years": r.rental_duration_years,
        "max_height_m": r.max_height_m,
        "max_floors": r.max_floors,
        "total_residential_units": r.total_residential_units,
        "total_units": r.total_units,
        "commerce_sqm": round(r.commerce_sqm, 1),
        "employment_sqm": round(r.employment_sqm, 1),
        "public_building_sqm": round(r.public_building_sqm, 1),
        "hafrash_built_sqm": round(r.hafrash_built_sqm, 1),
        "shatzap_sqm": round(r.shatzap_sqm, 1),
        "resident_shared_sqm": round(r.resident_shared_sqm, 1),
        "shared_tenant_notes": r.shared_tenant_notes,
        "mixed_use_sqm": round(r.mixed_use_sqm, 1),
        "total_building_sqm_all": round(r.total_building_sqm_all, 1),
        "rental_candidate_uses": r.rental_candidate_uses,
        "column_mapping": r.column_mapping,
        "raw_html_tables": r.raw_html_tables,
        "error": r.error,
    }
    if include_rows:
        out["rows"] = r.rows
    return out


if __name__ == "__main__":
    import sys
    if sys.platform.startswith("win"):
        sys.stdout.reconfigure(encoding="utf-8")
    for p in sorted(Path(r"C:\ORANIM\temp_xlsx").glob("*.xlsx")):
        r = parse_table5_xlsx(p)
        if r is None:
            print(f"{p.name}: <none>")
            continue
        print(f"{p.name}: rental={r.rental_units} cond={r.conditional_units} "
              f"H={r.max_height_m:.1f} F={r.max_floors} resid={r.total_residential_units} "
              f"err={r.error!r}")
        if r.rental_candidate_uses:
            print(f"    rental uses: {r.rental_candidate_uses}")
        if r.column_mapping:
            missing = {k for k in ("yiyud","use","parcel","units","height","floors_above")
                       if k not in r.column_mapping}
            if missing:
                print(f"    missing cols: {missing}")
