# -*- coding: utf-8 -*-
"""Build data/unit_bonus.json — the Table 5 "unit bonus" overlay.

Scans every downloaded Table 5 file in temp_xlsx/, detects the bottom-of-table
note that permits INCREASING the number of housing units at building-permit
stage (see detect_unit_bonus_note.py), and writes a side-JSON keyed by
plan_name that the app loads into window.__unitBonus.

TWO SOURCES. temp_xlsx is the primary one, but a plan whose Table 5 never came
down as xlsx was simply invisible: 101-0666032 (פת/בר יוחאי) carries
"תותר תוספת של עד 10% מהיקף יחידות הדיור לעת מתן היתר בניה" as a table-level note
in its הוראות, 356 × 1.10 = 392, and its permit is for exactly 392 — which the
extra-units report was reading as 36 units out of nowhere. So the הוראות corpus
in horaot/ is scanned as a fallback for plans the xlsx pass did not cover.

Kept as a side-JSON (not GS/geojson columns) so it survives the GS->geojson
rebuild, matching the app's other curated overlays (occupancy_status.json,
binui_plans.json, field_observations.json...).

Usage:
    python build_unit_bonus.py           # rebuild from all temp_xlsx files
    python build_unit_bonus.py --stats   # print without writing
"""
from __future__ import annotations

import glob
import json
import os
import re
import sys
from pathlib import Path

from detect_unit_bonus_note import detect_unit_bonus
from parse_table5_xlsx import parse_table5_xlsx

ROOT = Path(r"C:\ORANIM")
TEMP_DIR = ROOT / "temp_xlsx"
HORAOT_DIR = ROOT / "horaot"
PLANS_GEOJSON = ROOT / "oranim-app" / "data" / "plans.geojson"
OUT_FILE = ROOT / "oranim-app" / "data" / "unit_bonus.json"
MANUAL_FILE = ROOT / "oranim-app" / "data" / "unit_bonus_manual.json"

# Only these note kinds count as a unit bonus (per user decision 2026-07-30):
# תוספת / הגדלה = a straight increase; גמישות = bidirectional split/merge (can
# increase). דיוריות (granny suites) are deliberately excluded.
ACCEPT_KINDS = {"תוספת", "הגדלה", "גמישות"}


def _taba_of(fn: str) -> str:
    b = os.path.basename(fn)[:-5]           # strip .xlsx
    return b[4:] if b.startswith("101-") else b


def _taba_int(v) -> str:
    try:
        return str(int(float(v)))
    except (ValueError, TypeError):
        return ""


def _normalize_pdf_text(text: str) -> str:
    """The bonus patterns were calibrated on Table-5 xlsx text. fitz glues a digit
    onto the preceding Hebrew word ("עד10%") and drifts the sentence stop between a
    number and its unit, so the same patterns fail silently on הוראות PDFs — the
    normalisation in reference_fitz_hebrew_regex_mines took this scan 4 hits → 11."""
    t = re.sub(r"([א-ת])(\d)", r"\1 \2", text)
    t = re.sub(r"(\d)([א-ת])", r"\1 \2", t)
    t = re.sub(r"\s+", " ", t)
    return re.sub(r"(\d)\s*[.,;:]\s*(?=[א-ת])", r"\1 ", t)


_BONUS_OPEN = re.compile(r"(?:תותר|תותרנה|מותרת|תתאפשר|יתאפשר|ניתן|יותר)\s+"
                         r"(?:ו?תותר\s+)?(?:תוספת|הגדל|להגדיל)")


def _tidy_note(sentence: str) -> str:
    """The clause boundaries the detector relies on (a letter then a period) barely
    exist in a הוראות PDF, so the matched "sentence" arrives with the tail of the
    previous instruction glued to its front. Cut it back to the bonus verb."""
    s = re.sub(r"\s+", " ", sentence or "").strip()
    m = _BONUS_OPEN.search(s)
    if m:
        s = s[m.start():]
    return s[:240].strip()


def _horaot_text(pdf: str) -> str:
    """Text layer of a הוראות PDF, falling back to its OCR sidecar."""
    try:
        import fitz
        d = fitz.open(pdf)
        out = []
        for i in range(d.page_count):
            try:
                out.append(d[i].get_text())
            except Exception:
                break
        txt = "\n".join(out)
    except Exception:
        txt = ""
    if len(txt) >= 500:
        return txt
    ocr = pdf[:-4] + ".ocr.txt"
    if os.path.exists(ocr):
        return Path(ocr).read_text(encoding="utf-8", errors="ignore")
    return txt


def build(write: bool = True) -> dict:
    gj = json.loads(PLANS_GEOJSON.read_text(encoding="utf-8"))
    # taba(int-string) -> {plan_name, status}
    taba_to_plan = {}
    for f in gj["features"]:
        p = f["properties"]
        t = _taba_int(p.get("taba"))
        if t and t not in taba_to_plan:
            taba_to_plan[t] = {
                "plan_name": p.get("plan_name") or "",
                "status": (p.get("status_mavat") or "").strip(),
            }

    by_plan: dict = {}
    skipped_kind = 0
    unmapped = 0
    for fn in sorted(glob.glob(str(TEMP_DIR / "*.xlsx"))):
        t = _taba_int(_taba_of(fn))
        if not t:
            continue
        info = taba_to_plan.get(t)
        if not info or not info["plan_name"]:
            unmapped += 1
            continue
        res = parse_table5_xlsx(Path(fn))
        if not res or not res.unit_bonus_pct:
            continue
        if res.unit_bonus_kind not in ACCEPT_KINDS:
            skipped_kind += 1
            continue
        pn = info["plan_name"]
        # If two files map to the same plan, keep the higher percentage.
        prev = by_plan.get(pn)
        if prev and prev["pct"] >= res.unit_bonus_pct:
            continue
        by_plan[pn] = {
            "pct": res.unit_bonus_pct,
            "kind": res.unit_bonus_kind,
            "note": res.unit_bonus_note,
        }

    from_xlsx = len(by_plan)

    # Second pass — the הוראות corpus, for plans the xlsx pass never saw. A plan
    # already carrying an xlsx-derived bonus keeps it: the xlsx is the table itself,
    # the PDF is the table printed into a document.
    from_horaot = 0
    for pdf in sorted(glob.glob(str(HORAOT_DIR / "101-*.pdf"))):
        t = _taba_int(os.path.basename(pdf)[:-4].split("-")[-1])
        info = taba_to_plan.get(t)
        if not info or not info["plan_name"] or info["plan_name"] in by_plan:
            continue
        txt = _horaot_text(pdf)
        if len(txt) < 500:
            continue
        res = detect_unit_bonus(_normalize_pdf_text(txt))
        if not res or not res.get("pct") or res.get("kind") not in ACCEPT_KINDS:
            continue
        by_plan[info["plan_name"]] = {
            "pct": res["pct"],
            "kind": res["kind"],
            "note": _tidy_note(res.get("sentence")),
            "source": "horaot",
        }
        from_horaot += 1

    # Third source — hand-read clauses that are not percentages at all, and so cannot
    # be detected: an absolute ceiling (ברל לוקר 13, "עד לכמות מירבית של 143 יחידות
    # דיור") or a count stated as a floor (דרך חברון 101, "מספר יח״ד בתוכנית הינו 46
    # לכל הפחות"). Both were the last rows left unexplained in the extra-units report.
    from_manual = 0
    try:
        man = json.loads(MANUAL_FILE.read_text(encoding="utf-8")).get("by_plan", {})
    except Exception as e:
        man = {}
        print("  (no manual overlay: %s)" % e)
    for plan, fields in man.items():
        rec = by_plan.setdefault(plan, {})
        rec.update(fields)
        rec.setdefault("source", "manual")
        from_manual += 1

    out = {
        "generated_from": "temp_xlsx + horaot via detect_unit_bonus_note, + unit_bonus_manual",
        "count": len(by_plan),
        "by_plan": by_plan,
    }
    print(f"plans with a unit-bonus note: {len(by_plan)}"
          f"  (xlsx: {from_xlsx}, horaot: {from_horaot}, manual: {from_manual},"
          f" skipped kinds: {skipped_kind}, unmapped files: {unmapped})")
    from collections import Counter
    print("by kind:", dict(Counter(v.get("kind") for v in by_plan.values())))
    print("by pct :", dict(Counter(v.get("pct") or v.get("max_units") or "מינימום" for v in by_plan.values())))
    if write:
        # Compact single-line JSON — matches the repo convention and the format
        # git_sync.update_json_and_push rewrites it to (avoids diff churn).
        OUT_FILE.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
        print(f"wrote {OUT_FILE}")
    return out


if __name__ == "__main__":
    build(write="--stats" not in sys.argv)
