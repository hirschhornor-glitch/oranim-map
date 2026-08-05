"""
extract_maintenance_fund.py  —  קרן תחזוקה (long-term maintenance fund) mapping.

For every plan in plans.geojson this locates the plan's regulations document
(horaot\\<plan_name>.pdf) and detects whether the plan grants CONDITIONAL
building rights / dwelling units tied to establishing a long-term maintenance
fund ("זכויות / יח\"ד מותנות בהקמת קרן תחזוקה").

Per qualifying plan it extracts:
  has_fund              True when the plan conditions rights/units on the fund
  conditional_units     number of יח"ד מותנות (Table-5 result preferred, horaot fallback)
  conditional_units_src "table5" | "horaot"
  fund_section          governing section reference (e.g. "6.10", "1.6")
  fund_amount_ils       explicit ₪ sum, when the horaot state one (usually null —
                        the sum is set externally via the trust agreement /
                        הרשות להתחדשות עירונית cost-assessment doc)
  mechanism_text        the condition sentence, in logical (readable) order
  total_units           plan total units (for context / share)
  source                "horaot"

The horaot PDF text layer comes out char- AND word-reversed (visual RTL); we
restore logical order (mirrors extract_execution_staging.py) before matching.

Output: oranim-app\\data\\maintenance_fund.json  keyed by plan_name.

Run:  py scripts/extract_maintenance_fund.py                 # all plans (resumable)
      py scripts/extract_maintenance_fund.py 101-1024645 ... # only given plans (debug)
"""
from __future__ import annotations

import io
import json
import os
import re
import sys

import fitz  # PyMuPDF — fast full-text extraction (pdfplumber stalls on תשריט pages)

HORAOT_DIR = r"C:\ORANIM\horaot"
PLANS_GEOJSON = os.path.join(os.path.dirname(__file__), "..", "data", "plans.geojson")
TABLE5_RESULTS = r"C:\ORANIM\all_table5_xlsx_results.json"
OUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "maintenance_fund.json")

# Manual corrections (user-verified, 2026-08-05):
#   plans outside the Oranim quarter — drop from the fund mapping entirely
EXCLUDE_PLANS = {
    "101-0581850",   # פינוי בינוי פנמה 2 ו-6 — מחוץ לרובע
    "101-1252980",   # מתחמים לאורך ציר הרק"ל — מתחם מודעי — מחוץ לרובע
}
#   sub-neighborhood overrides where the plan's stored value is blank/wrong
SUB_OVERRIDE = {
    "101-1563642": "פת",   # התחדשות עירונית אריה בעהם
}
#   canonical sub-neighborhood names (same neighbourhood stored under aliases —
#   canonical form taken from the authoritative sub_neighborhoods layer)
SUB_CANON = {
    "רסקו": "רסקו - גבעת הורדים",
    "א.ת. תלפיות": "תלפיות - תעשייה ומסחר",
}
MIN_FLOORS = 13   # scope: urban-renewal / tower plans of MORE THAN 13 floors


# ── RTL normalization (mirrors extract_execution_staging.py) ──────────────
_NUM_TOKEN = re.compile(r"^[0-9][0-9().,/_%\"'-]*$")


def _rev_word(w: str) -> str:
    r = w[::-1]
    return re.sub(r"\)(\d+)\(", lambda m: "(" + m.group(1)[::-1] + ")", r)


def _norm_line(line: str) -> str:
    words = []
    for w in line.split():
        words.append(w if _NUM_TOKEN.match(w) else _rev_word(w))
    return " ".join(reversed(words))


def read_horaot_text(pdf_path: str) -> str:
    """Full-text via PyMuPDF — returns Hebrew in LOGICAL order (no reversal
    needed, unlike pdfplumber) and is fast even on תשריט-heavy PDFs."""
    doc = fitz.open(pdf_path)
    try:
        parts = [pg.get_text("text") for pg in doc]
    finally:
        doc.close()
    return "\n".join(parts)


def collapse(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


# ── Vocabulary ────────────────────────────────────────────────────────────
FUND_TERM = re.compile(r"קרן\s+תחזוק")            # "קרן תחזוקה"
COND_TERM = re.compile(r'מותנ(?:ה|ות|ית)')        # מותנה / מותנות / מותנית

# The fund CONDITION: rights/units conditioned on establishing the fund.
FUND_CONDITION = [
    re.compile(r"מותנ(?:ית|ה)\s+בהקמת\s+קרן\s+תחזוק"),
    re.compile(r"מותנות\s+למגורים\s+עבור\s+קרן\s+תחזוק"),
    re.compile(r'זכויות\s+"?מותנות"?[^.]{0,120}?קרן\s+תחזוק'),
    re.compile(r"קרן\s+תחזוק[^.]{0,120}?זכויות\s+מותנות"),
]

# governing section, e.g. "כמפורט בסעיף 1.6" / "המפורטות בסעיף6.10" (fitz glues
# the number to the preceding word, so \s* not \s+)
SECTION_RE = re.compile(r"בסעיף\s*(\d{1,2}(?:\.\d{1,2})*)")

# explicit conditional-unit count in the horaot text
COND_UNITS_RE = [
    re.compile(r'(\d{1,3})\s*יח"?ד[^.]{0,25}?(?:ל?טובת|עבור)\s+קרן\s+תחזוק'),
    re.compile(r'כולל\s+(\d{1,3})\s*יח"?ד[^.]{0,25}?קרן\s+תחזוק'),
    re.compile(r'קרן\s+תחזוק[^.]{0,40}?(\d{1,3})\s*יח"?ד'),
]

# The deposit-amount clause: "(סכום/שיעור) הכספים שיופקד בקרן התחזוקה יהיה ..."
# or "קרן תחזוקה בשווי של לפחות ...". The amount lives in the detailed fund
# section (well past the objectives list), so we need the full document text.
AMOUNT_ANCHORS = [
    re.compile(r'(?:סכום|שיעור|שווי)\s+ה?כספים?\s+שיופקד\w*\b[^.]{0,60}?תחזוק\w*\s+יהיה'),
    re.compile(r'קרנ?ות?\s+ה?תחזוקה\s+יהיה\s+לפחות'),
    re.compile(r'קרן\s+תחזוקה\s+בשווי\s+של\s+לפחות'),
]
# a money figure: 6+ digit grouped number, or a small number + million/thousand
# unit. fitz separates the unit from the number with newlines/dots, so allow a
# little junk (up to 5 chars of whitespace/dots) before the unit.
MONEY_RE = re.compile(
    r'(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)[\s.\'’"]{0,6}(מלש"ח|מיליון\s*ש"ח|מיליון|ש"ח|אלף)?')
# "עד N קומות" — a floor count stated in the horaot
FLOORS_RE = re.compile(r'עד\s+(\d{1,3})\s*קומות')


def _money_to_ils(numstr, unit):
    n = float(numstr.replace(",", ""))
    has_comma = "," in numstr
    if has_comma or n >= 100_000:      # already full shekels (the "מיליון" that
        return n                       # sometimes follows a grouped number is a typo
    if unit and ("מלש" in unit or "מיליון" in unit):
        return n * 1_000_000
    if unit and "אלף" in unit:
        return n * 1_000
    return n


# a sentence-ending period: one that is NOT sitting between two digits
# (so "1.6" / "6.10" section numbers don't split a sentence mid-token)
_SENT_END = re.compile(r"(?<!\d)\.(?!\d)")


def _sentence_end(text: str, frm: int) -> int:
    m = _SENT_END.search(text, frm)
    return m.start() if m else frm + 220


def find_condition_sentence(text: str):
    """Return the first sentence that ties conditional rights/units to the fund."""
    for pat in FUND_CONDITION:
        m = pat.search(text)
        if m:
            # sentence start = previous real sentence break
            prev = list(_SENT_END.finditer(text, max(0, m.start() - 300), m.start()))
            start = (prev[-1].end() if prev else max(0, m.start() - 300))
            # prefer to open the clause at the rights/units anchor when it sits
            # between the sentence break and the fund mention
            anchor = re.search(r'זכויות\s+"?מותנות"?|מספר\s+יחידות\s+הדיור',
                               text[start:m.start()])
            if anchor:
                start += anchor.start()
            end = _sentence_end(text, m.end())
            return collapse(text[start:end])[:400]
    return ""


def extract_section(sentence: str, text: str):
    m = SECTION_RE.search(sentence)
    if m:
        return m.group(1)
    # else scan a window right after any fund mention in the full text
    for fm in FUND_TERM.finditer(text):
        window = text[fm.start():fm.start() + 260]
        m = SECTION_RE.search(window)
        if m:
            return m.group(1)
    return ""


def extract_cond_units(text: str):
    for pat in COND_UNITS_RE:
        m = pat.search(text)
        if m:
            try:
                v = int(m.group(1))
                if 0 < v < 400:
                    return v
            except ValueError:
                pass
    return None


def extract_amount(text: str):
    """Return (total_ils, verbatim_clause). Reads the actual deposit clause in
    the detailed fund section and parses the total ₪ to be deposited — handling
    a stated total, per-tower/מתחם breakdowns, and the stray 'מיליון' typo."""
    anchor = None
    for pat in AMOUNT_ANCHORS:
        m = pat.search(text)
        if m:
            anchor = m
            break
    if not anchor:
        return None, ""
    seg = text[anchor.start():anchor.start() + 300]
    # cut the clause at the next instruction / page header-footer (which leak
    # plan numbers + dates that MONEY_RE would otherwise mistake for amounts)
    for stop in ("סטייה מהוראות", "תנאי לתעודת גמר", "תנאי בהליך הרישוי",
                 "שם התכנית", "תכנית מס", "מונה תדפיס", "מונה הדפסה",
                 "מועד הפקה", "8. ", "9. "):
        i = seg.find(stop, 30)
        if i != -1:
            seg = seg[:i]
    clause = collapse(seg)[:400]
    # collect money figures ≥ ₪500k (drops floor counts, section numbers, dates)
    vals = []
    for numstr, unit in MONEY_RE.findall(seg):
        v = _money_to_ils(numstr, unit)
        if v >= 500_000:
            vals.append(v)
    if not vals:
        return None, clause
    # "סה\"כ"/"הכולל" means a total is stated explicitly → take it (don't add the
    # per-tower breakdown on top). Otherwise the figures are per-tower → sum.
    if 'סה"כ' in seg or "הכולל" in seg or "סה”כ" in seg:
        total = max(vals)
    else:
        total = sum(vals)
    return int(round(total)), clause


def extract_floors(text: str, mechanism: str):
    """Max floor count stated in the horaot ('עד N קומות')."""
    nums = [int(m) for m in FLOORS_RE.findall(text) if int(m) < 200]
    return max(nums) if nums else None


def analyze(text: str) -> dict:
    """Return fund findings for one plan's horaot text."""
    has_term = bool(FUND_TERM.search(text))
    if not has_term:
        return {"has_fund": False}
    sentence = find_condition_sentence(text)
    # scope = only plans that CONDITION rights/units on the fund
    conditioned = bool(sentence) or (
        bool(FUND_TERM.search(text)) and bool(re.search(
            r'זכויות\s+"?מותנות"?|יח"?ד\s+מותנות|מותנ(?:ית|ה)\s+בהקמת', text)))
    if not conditioned:
        return {"has_fund": False, "mentions_fund": True}
    amount_ils, amount_text = extract_amount(text)
    return {
        "has_fund": True,
        "mechanism_text": sentence,
        "fund_section": extract_section(sentence, text),
        "conditional_units_horaot": extract_cond_units(text),
        "fund_amount_ils": amount_ils,
        "fund_amount_text": amount_text,
        "floors_horaot": extract_floors(text, sentence),
    }


# ── Sources ───────────────────────────────────────────────────────────────
def load_plans():
    with io.open(PLANS_GEOJSON, encoding="utf-8") as f:
        feats = json.load(f)["features"]
    out = {}
    for ft in feats:
        p = ft["properties"]
        pn = p.get("plan_name")
        if not pn or pn in out:
            continue
        def _int(v):
            try:
                return int(float(str(v)))
            except (TypeError, ValueError):
                return None
        out[pn] = {
            "plan_name": pn,
            "plan_name_he": p.get("plan_name_he") or "",
            "sub_neighborhood": (lambda s: SUB_CANON.get(s, s))(SUB_OVERRIDE.get(pn) or p.get("sub_neighborhood") or p.get("SUB_N") or ""),
            "minahak": p.get("minahak") or "",
            "status_mavat": p.get("status_mavat") or "",
            "mavat_url": p.get("mavat_url") or "",
            "units_total": p.get("units_total") or "",
            "plan_type": p.get("plan_type") or "",
            "level_num": _int(p.get("level_num")),
            "high_m": (lambda v: float(v) if v not in (None, "") and str(v).replace(".", "", 1).isdigit() else None)(p.get("High")),
            "taba": str(p.get("taba") or ""),
        }
    return out


def load_table5_conditional():
    """taba(str) -> conditional_units from the Table-5 extraction results."""
    if not os.path.exists(TABLE5_RESULTS):
        return {}
    data = json.load(io.open(TABLE5_RESULTS, encoding="utf-8"))
    out = {}
    for r in data.get("results", []):
        if not isinstance(r, dict):
            continue
        cu = r.get("conditional_units")
        taba = str(r.get("taba") or "")
        if taba:
            out[taba] = {
                "conditional_units": cu,
                "total_units": r.get("total_units") or r.get("total_residential_units"),
                "max_floors": r.get("max_floors"),
            }
    return out


def scan_pdf(pdf_path):
    """Worker (runs in a subprocess so a malformed PDF can be timed out)."""
    try:
        return analyze(read_horaot_text(pdf_path))
    except Exception as e:
        return {"has_fund": False, "error": f"{type(e).__name__}: {str(e)[:80]}"}


def merge(rec, finding, t5rec):
    cu_t5 = t5rec.get("conditional_units")
    cu_h = finding.get("conditional_units_horaot")
    if cu_t5:
        rec["conditional_units"] = cu_t5
        rec["conditional_units_src"] = "table5"
    elif cu_h:
        rec["conditional_units"] = cu_h
        rec["conditional_units_src"] = "horaot"
    else:
        rec["conditional_units"] = None
        rec["conditional_units_src"] = ""
    # scope = rights/units CONDITIONED on the fund. Confirmed by the horaot
    # condition sentence, OR by a Table-5 "מותנה" row (in these Jerusalem plans a
    # conditional Table-5 row IS the maintenance-fund mechanism — so it counts
    # even when the horaot PDF is missing, e.g. 101-1404177).
    conditioned = bool(finding.get("has_fund")) or bool(cu_t5)

    # floor count = the MAX across all signals (a single wrong low value — e.g.
    # level_num=2 on a 43-floor tower — must not win over height/horaot).
    floor_signals = [finding.get("floors_horaot"), t5rec.get("max_floors"),
                     rec.get("level_num"),
                     round(rec["high_m"] / 3.1) if rec.get("high_m") else None]
    floors = max([f for f in floor_signals if f], default=None)
    rec["max_floors"] = floors

    # SCOPE (user rule): only urban-renewal / tower plans of MORE THAN 13 floors,
    # and never plans outside the quarter.
    tall_enough = (floors is None) or (floors > MIN_FLOORS)   # keep if floors unknown
    rec["has_fund"] = conditioned and tall_enough and rec["plan_name"] not in EXCLUDE_PLANS

    rec["mechanism_text"] = finding.get("mechanism_text", "")
    rec["fund_section"] = finding.get("fund_section", "")
    rec["fund_amount_ils"] = finding.get("fund_amount_ils")
    rec["fund_amount_text"] = finding.get("fund_amount_text", "")
    if t5rec.get("total_units"):
        rec["units_total"] = t5rec["total_units"]
    if finding.get("error"):
        rec["error"] = finding["error"]
    return rec


def push_updates(plan_names):
    """Recompute the given plans and push data/maintenance_fund.json via
    git_sync (concurrency-safe delta write). Called by the enrichment pipeline
    when a plan is new or changes status, so the fund data is re-checked
    alongside the Table-5 / יח"ד-מותנות re-check (horaot is freshly downloaded
    by enrich before this runs)."""
    for d in (os.path.dirname(__file__), r"C:\ORANIM"):
        if d not in sys.path:
            sys.path.insert(0, d)
    import git_sync

    plans = load_plans()
    t5 = load_table5_conditional()

    def t5_for(meta):
        taba7 = meta["taba"].zfill(7) if meta["taba"] else ""
        return t5.get(meta["taba"]) or t5.get(taba7) or {}

    deltas = {}   # plan_name -> record | None (remove)
    for pn in plan_names:
        meta = plans.get(pn)
        if not meta:
            continue
        pdf = os.path.join(HORAOT_DIR, pn + ".pdf")
        rec = dict(meta); rec["has_horaot"] = os.path.exists(pdf)
        finding = {"has_fund": False}
        if rec["has_horaot"]:
            try:
                finding = analyze(read_horaot_text(pdf))
            except Exception as e:
                finding = {"has_fund": False, "error": f"{type(e).__name__}: {str(e)[:80]}"}
        rec = merge(rec, finding, t5_for(meta))
        deltas[pn] = rec if rec.get("has_fund") else None

    if not deltas:
        print("push_updates: no matching plans", flush=True)
        return

    def edit_fn(data):
        n = 0
        for pn, rec in deltas.items():
            if rec:
                if data.get(pn) != rec:
                    data[pn] = rec
                    n += 1
            elif pn in data:                      # lost fund status → remove
                del data[pn]
                n += 1
        return n

    changed = [pn for pn, r in deltas.items() if r]
    git_sync.update_json_and_push(
        "data/maintenance_fund.json", edit_fn,
        f"data: maintenance-fund recheck ({len(deltas)} plans, {len(changed)} funds)")
    print(f"push_updates: {len(deltas)} rechecked, {len(changed)} funds "
          f"({', '.join(changed) if changed else 'none'})", flush=True)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    if "--push" in sys.argv:
        push_updates([a for a in sys.argv[1:] if a.startswith("101-")])
        return
    only = [a for a in sys.argv[1:] if a.startswith("101-")]

    plans = load_plans()
    t5 = load_table5_conditional()

    def t5_for(meta):
        taba7 = meta["taba"].zfill(7) if meta["taba"] else ""
        return t5.get(meta["taba"]) or t5.get(taba7) or {}

    # A full scan is a fresh build (fitz is fast — no need to resume). `only`
    # mode rebuilds just the given plans on top of the existing file.
    out = {}
    if only and os.path.exists(OUT_PATH):
        out = json.load(io.open(OUT_PATH, encoding="utf-8"))
    targets = only or sorted(plans)

    processed, funds = 0, 0
    print(f"{len(targets)} plans to scan", flush=True)
    for pn in targets:
        if pn not in plans:
            continue
        pdf_path = os.path.join(HORAOT_DIR, pn + ".pdf")
        rec = dict(plans[pn]); rec["has_horaot"] = os.path.exists(pdf_path)
        finding = {"has_fund": False}
        if rec["has_horaot"]:
            try:
                finding = analyze(read_horaot_text(pdf_path))
            except Exception as e:
                finding = {"has_fund": False, "error": f"{type(e).__name__}: {str(e)[:80]}"}
        out[pn] = merge(rec, finding, t5_for(plans[pn]))
        processed += 1
        if out[pn]["has_fund"]:
            funds += 1
            print(f"[fund] {pn}  floors={out[pn]['max_floors']} units={out[pn]['conditional_units']} "
                  f"sec={out[pn]['fund_section']} amt={out[pn]['fund_amount_ils']}", flush=True)
        if processed % 200 == 0:
            print(f"  ...{processed}/{len(targets)} scanned, {funds} funds", flush=True)

    # only fund plans are consumed by the app (layer filter + report); drop the
    # rest so the file stays small (the report's denominator comes from plans.geojson).
    out = {k: v for k, v in out.items() if v.get("has_fund")}
    _write(out)
    allfunds = [r for r in out.values() if r.get("has_fund")]
    print(f"\nDone. {len(out)} plans scanned, {len(allfunds)} with קרן תחזוקה.", flush=True)
    print(f"  with conditional_units: {len([r for r in allfunds if r.get('conditional_units')])}", flush=True)
    print(f"  with ₪ amount stated:   {len([r for r in allfunds if r.get('fund_amount_ils')])}", flush=True)


def _write(out):
    tmp = OUT_PATH + ".tmp"
    io.open(tmp, "w", encoding="utf-8").write(json.dumps(out, ensure_ascii=False, indent=1))
    os.replace(tmp, OUT_PATH)


if __name__ == "__main__":
    main()
