# -*- coding: utf-8 -*-
"""Build data/diyuriot.json — the plan-level דיוריות allowance.

A דיורית is a room or set of rooms inside an existing יח"ד that can serve as a separate
household (own entrance), whose area is counted INSIDE the host unit and which "אינה
ניתנת לרישום ברשם המקרקעין בנפרד מיחידת הדיור". So it is NOT another יח"ד — but it IS
another household, which is what matters for population, schools and parking. That is
why it gets its own channel rather than being folded into the טבלה 5 unit bonus
([[project_extra_units_report]]), and why detect_unit_bonus_note deliberately excluded
kind='דיוריות' back in 07/2026.

TWO CLAUSES, AND ONLY ONE OF THEM GRANTS ANYTHING:
  * the definition ("דיורית — חדר או מערכת חדרים שהיא חלק מיחידת דיור קיימת...") is
    boilerplate in nearly every recent plan and grants no quota;
  * the grant is a separate clause with a share — "תותר הכללת דיורית כהגדרתה בתכנית זו,
    בעד 20% ממספר יחידות הדיור בתכנית".
The user's rule of thumb, which held on every case checked: **if the definition is in
the plan, the quota appears further down**. So a plan with the definition and no quota
is a parser miss, not a plan that grants nothing — those are reported as `defined_only`
rather than silently dropped.

Run: py -X utf8 build_diyuriot.py  [--stats]
"""
from __future__ import annotations

import glob
import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(r"C:\ORANIM")
HORAOT = ROOT / "horaot"
PLANS = ROOT / "oranim-app" / "data" / "plans.geojson"
OUT_FILE = ROOT / "oranim-app" / "data" / "diyuriot.json"

DIYUR = r"\u05d3\u05d9\u05d5\u05e8\u05d9(?:\u05ea|\u05d5\u05ea)"          # דיורית / דיוריות
# "of the housing units", in every spelling the corpus uses: יחידות הדיור / יח"ד /
# יחה"ד / הדירות / כלל הדירות (ברל לוקר 25-26 counts דירות).
OF_UNITS = (r"\u05de(?:\u05de\u05e1\u05e4\u05e8|\u05e1\u05da|\u05db\u05dc\u05dc|\u05e1\u05d4[\"\u05f4']?\u05db)?\s*"
            r"(?:\u05d9\u05d7\u05d9\u05d3\u05d5\u05ea\s+\u05d4\u05d3\u05d9\u05d5\u05e8"
            r"|\u05d9\u05d7[\u05d4]?[\"\u05f4']?\u05d3"
            r"|(?:\u05db\u05dc\u05dc\s+)?\u05d4?\u05d3\u05d9\u05e8\u05d5\u05ea"
            r"|\u05d9\u05d7\u05d9\u05d3\u05d5\u05ea\s+\u05d0\u05dc\u05d5)")   # \u2026\u05d5\u05d9\u05d7\u05d9\u05d3\u05d5\u05ea \u05d0\u05dc\u05d5 (\u05d6\u05d0\u05e4\u05d4)
# ⚠️ Punctuation drifts between the % and the noun — "בעד 20% . ממספר יחידות הדיור" and
# "בעד 20% - ממספר יחידות הדיור". norm() only strips punctuation that follows a DIGIT,
# and here it follows the '%'. Nineteen plans that DO grant a quota read as granting
# nothing until this was allowed for. See [[reference_fitz_hebrew_regex_mines]].
PCT_OF_UNITS = re.compile(r"(\d{1,2})\s*%\s*[.,;:\-\u2013\u05be]{0,2}\s*" + OF_UNITS)
VERB = re.compile(r"\u05ea\u05d5\u05ea\u05e8|\u05ea\u05ea\u05d0\u05e4\u05e9\u05e8|\u05d9\u05d5\u05ea\u05e8"
                  r"|\u05e0\u05d9\u05ea\u05df \u05dc\u05d4\u05ea\u05d9\u05e8|\u05d4\u05db\u05dc\u05dc\u05ea"
                  r"|\u05ea\u05d5\u05e1\u05e4\u05ea|\u05d4\u05e7\u05de\u05ea|\u05d4\u05e6\u05de\u05d3\u05ea"
                  # "\u05de\u05d5\u05ea\u05e8\u05ea \u05d1\u05e0\u05d9\u05d9\u05ea \u05d3\u05d9\u05d5\u05e8\u05d9\u05d5\u05ea ... \u05d1\u05d4\u05d9\u05e7\u05e3 \u05e9\u05dc \u05e2\u05d3 20%" (\u05d9\u05d4\u05d5\u05d3\u05d4 \u05d4\u05e0\u05e9\u05d9\u05d0 24), and a cap
                  # phrased as a limit rather than a permission (\u05d1\u05d5\u05e1\u05ea\u05e0\u05d0\u05d9 32-34)
                  r"|\u05de\u05d5\u05ea\u05e8|\u05d9\u05ea\u05d0\u05e4\u05e9\u05e8"
                  r"|\u05dc\u05d0 \u05d9\u05e2\u05dc\u05d4 \u05e2\u05dc")
# "לפחות 20% מיחידות הדיור יהיו/תהיינה דירות קטנות" — an apartment-size mix that happens
# to sit next to the דיורית clause, not a דיוריות quota.
SIZE_MIX = re.compile(r"\u05d9\u05d4\u05d9\u05d5|\u05ea\u05d4\u05d9\u05d9\u05e0\u05d4|\u05e7\u05d8\u05e0")
DEFN = re.compile(r"\u05d4\u05d2\u05d3\u05e8\u05ea \u05de\u05d5\u05e0\u05d7"
                  r"|\u05e9\u05d8\u05d7 \u05d4\u05d3\u05d9\u05d5\u05e8\u05d9\u05ea"
                  r"|\u05d0\u05d9\u05e0\u05d4 \u05e0\u05d9\u05ea\u05e0\u05ea \u05dc\u05e8\u05d9\u05e9\u05d5\u05dd")
# A quota can also come with no noun after it at all — "סך יח״ד שבהם יתאפשר תוספת
# דיורית לא יעלה על 30%." (גבעת השקד דרום), "מספר הדיורית לא יעלה על 20%" (בוסתנאי).
CAP_NO_NOUN = re.compile(r"לא יעלה על\s*(\d{1,2})\s*%")
# RTL reordering sometimes throws the '%' in front of its own number — מתחם מודעי reads
# "(כהגדרתן בסעיף 1.9 % -( לכל היותר ב 25 מיח״ד". Accept a bare number against the units
# noun when a '%' sits just before it, so the sign's position does not decide the reading.
PCT_DISPLACED = re.compile(r"%[^\dא-ת]{0,12}(?:[א-ת\s\(\)\-]{0,22})(\d{1,2})\s*" + OF_UNITS)
FORWARD = 190          # chars after the דיורית word to look for its share

# Checked against the CURRENT הוראות on Mavat (download_doc_by_category --category הוראות,
# 2026-10-12) — every local copy predated its plan's status date, so a quota could have been
# added since. None had been: all ten carry the same template of conditions on the דיורית
# itself (25-50 מ"ר, based on the host unit's infrastructure, not separately registrable)
# with NO share of the units. 101-1115450 is the clearest — "לכל יחידת דיור תתאפשר דיורית
# אחת", host over 130 מ"ר (110 when deposited); 101-1523851 is a 4-unit plan whose stated
# purpose is "תוספת שטחים ודיורית", a single one. Recorded so the negative read is not
# repeated — see [[project_hafrash_permit_negative_reads]].
VERIFIED_NO_QUOTA = {
    "101-1115450", "101-1133180", "101-1394444", "101-1417039", "101-1431808",
    "101-1449511", "101-1492735", "101-1523851", "101-1551530", "101-1563642",
}
VERIFIED_ON = "2026-10-12"


def _norm(text: str) -> str:
    """fitz glues digits to words and drifts punctuation; see reference_fitz_hebrew_regex_mines."""
    t = re.sub(r"([\u05d0-\u05ea])(\d)", r"\1 \2", text)
    t = re.sub(r"(\d)([\u05d0-\u05ea])", r"\1 \2", t)
    t = re.sub(r"\s+", " ", t)
    return re.sub(r"(\d)\s*[.,;:]\s*(?=[\u05d0-\u05ea])", r"\1 ", t)


def _text_of(pdf: str) -> str:
    try:
        import fitz
        d = fitz.open(pdf)
        out = []
        for i in range(d.page_count):
            try:
                out.append(d[i].get_text())
            except Exception:
                break
        t = "\n".join(out)
    except Exception:
        t = ""
    if len(t) >= 500:
        return t
    ocr = pdf[:-4] + ".ocr.txt"
    return Path(ocr).read_text(encoding="utf-8", errors="ignore") if os.path.exists(ocr) else t


def detect_diyuriot(txt: str) -> dict | None:
    """{pct, clause} for the largest quota the text grants, or None.

    Searched FORWARD from each דיורית mention: the apartment-size mix usually sits just
    BEFORE the clause ("20% מיחידות הדיור יהיו יחידות דיור קטנות ... ניתן להתיר דיוריות
    בהיקף של עד 30%"), and a window centred on the word picks the wrong number.
    """
    best = None
    for m in re.finditer(DIYUR, txt):
        fwd = txt[m.end(): m.end() + FORWARD]
        q = PCT_OF_UNITS.search(fwd) or CAP_NO_NOUN.search(fwd) or PCT_DISPLACED.search(fwd)
        if not q:
            continue
        before = txt[max(0, m.start() - 90): m.start()]
        if not (VERB.search(before) or VERB.search(fwd[:q.start()])):
            continue
        if SIZE_MIX.search(fwd[q.end(): q.end() + 40]):
            continue
        pct = int(q.group(1))
        # 80% is a real quota — בית הערבה 22-24: "ב-80% מסה"כ יחידות הדיור תתאפשר
        # תוספת דיורית". The old 1..50 window was a guess; the size-mix guard is what
        # actually keeps noise out.
        if not (1 <= pct <= 100):
            continue
        if best is None or pct > best["pct"]:
            best = {"pct": pct,
                    "clause": re.sub(r"\s+", " ", txt[max(0, m.start() - 90): m.end() + q.end() + 60]).strip()}
    return best


def _taba_int(v) -> str:
    try:
        return str(int(float(v)))
    except (ValueError, TypeError):
        return ""


def build(write: bool = True) -> dict:
    gj = json.loads(PLANS.read_text(encoding="utf-8"))
    by_taba = {}
    for f in gj["features"]:
        p = f["properties"]
        t = _taba_int(p.get("taba"))
        if t and t not in by_taba:
            by_taba[t] = p

    by_plan, defined_only = {}, []
    for pdf in sorted(glob.glob(str(HORAOT / "101-*.pdf"))):
        t = _taba_int(os.path.basename(pdf)[4:-4])
        props = by_taba.get(t)
        if not props or not props.get("plan_name"):
            continue
        txt = _norm(_text_of(pdf))
        if len(txt) < 400 or not re.search(DIYUR, txt):
            continue
        hit = detect_diyuriot(txt)
        if hit:
            by_plan[props["plan_name"]] = {
                "pct": hit["pct"],
                "note": hit["clause"][:260],
                "source": "\u05d4\u05d5\u05e8\u05d0\u05d5\u05ea " + props["plan_name"],
            }
        elif DEFN.search(txt):
            # the definition is there, so a quota almost certainly is too — flag it
            defined_only.append(props["plan_name"])

    out = {
        "generated_from": "horaot via build_diyuriot.detect_diyuriot",
        "count": len(by_plan),
        "defined_without_quota": sorted(defined_only),
        "verified_no_quota": sorted(VERIFIED_NO_QUOTA & set(defined_only)),
        "verified_on": VERIFIED_ON,
        "by_plan": by_plan,
    }
    print("plans granting a דיוריות quota: %d   (definition but no quota found: %d)"
          % (len(by_plan), len(defined_only)))
    from collections import Counter
    print("by pct:", dict(sorted(Counter(v["pct"] for v in by_plan.values()).items())))
    if write:
        OUT_FILE.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
        print("wrote %s" % OUT_FILE)
    return out


if __name__ == "__main__":
    build(write="--stats" not in sys.argv)
