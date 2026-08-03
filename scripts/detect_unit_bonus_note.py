# -*- coding: utf-8 -*-
"""Detect the Table 5 "unit bonus" note — a note at the bottom of טבלה 5 that
permits INCREASING the number of housing units (מספר יח"ד) at building-permit
stage, without an additional planning process.

Example (plan 101-1033869):
  "תותר תוספת של עד 10% ממספר יחידות הדיור לעת מתן היתר הבניה, ללא שינוי
   בשטחי הבניה העיקריים, בהתאם להוראות הבינוי וללא צורך בהליך נוסף."

Public API:
    detect_unit_bonus(note_txt) -> dict | None
        {pct, kind, sentence}  or None when no bonus note.

`note_txt` should be the tag-stripped, whitespace-collapsed text of the whole
Table 5 file (same as parse_table5_xlsx._note_txt).
"""
from __future__ import annotations
import re
from typing import Optional

_Q = r'["\'״′″‘’“”]'
UNITS = r'(?:יחידות\s+הדיור|יח' + _Q + r'?ד|הדירות|הדיור)'

# The bonus verb: "תותר תוספת", "תותר הגדלת", "מותרת תוספת", "ניתן/יתאפשר להגדיל",
# "תתאפשר תוספת", "תותר ... תוספת". The '%' and a units-count object must sit
# close together, and the clause is about the COUNT of units (מספר/כמות/היקף/סך
# יח"ד) — not floor area.
_INC_VERB = r'(?:תותר|מותרת|תתאפשר|יתאפשר|ניתן|יותר|תתאפש|תותרנה)'
# count-noun that turns "% of units" into "% MORE units"
_COUNT = r'(?:מספר|מס' + _Q + r'?|כמות|היקף|סך|סה' + _Q + r'?כ|מ)'

# Strong bonus signatures (ordered). Each captures the percentage.
_PATTERNS = [
    # תותר תוספת של [עד] N% [מ]מספר/היקף/כמות (ה)יח"ד / יחידות הדיור
    (re.compile(_INC_VERB + r'\s+תוספת\s+(?:של\s+)?(?:עד\s+)?(\d{1,2})\s*%\s*(?:ל|מ)?\s*' + _COUNT + r'?\s*' + UNITS), 'תוספת'),
    # תותר הגדלת מספר יח"ד [עד] N%   /  הגדלת מס' יח"ד ... בשיעור ... N%
    (re.compile(_INC_VERB + r'\s+הגדלת\s+' + _COUNT + r'\s*' + UNITS + r'[^.]{0,25}?(\d{1,2})\s*%'), 'הגדלה'),
    # ניתן/יתאפשר להגדיל את כמות/מספר יחידות הדיור ... N%
    (re.compile(r'(?:ניתן|יתאפשר|תתאפשר)\s+להגדיל\s+את\s+' + _COUNT + r'\s*' + UNITS + r'[^.]{0,25}?(\d{1,2})\s*%'), 'הגדלה'),
    # תותר תוספת של [עד] N% בכמות/במספר (ה)יח"ד
    (re.compile(_INC_VERB + r'\s+תוספת\s+(?:של\s+)?(?:עד\s+)?(\d{1,2})\s*%\s*ב' + _COUNT + r'\s*' + UNITS), 'תוספת'),
    # מותרת/תותר תוספת של עד N% לסה"כ יח"ד
    (re.compile(_INC_VERB + r'\s+תוספת\s+(?:של\s+)?(?:עד\s+)?(\d{1,2})\s*%\s*(?:ל)?סה' + _Q + r'?כ\s*' + UNITS), 'תוספת'),
    # תוספת ו/או הפחתה של N% ממספר יח"ד (bidirectional flexibility)
    (re.compile(r'תוספת\s+ו/?או\s+הפחתה\s+של\s+(?:עד\s+)?(\d{1,2})\s*%\s*(?:מ)?' + _COUNT + r'?\s*' + UNITS), 'גמישות'),
]

# דיוריות = secondary "granny" units inside a main unit — a different animal;
# reported with kind='דיוריות' so the caller can decide.
# Hard negatives — if the matched clause is really one of these, reject.
_NIYUD = re.compile(r'ניוד')                                  # transfer between lots

# Verbs that open a bonus clause — used to tighten the extracted sentence so we
# don't drag in a whole run of unrelated bulleted instructions (some Table 5
# files list clauses with no letter-period boundary between them).
_CLAUSE_OPEN = re.compile(r'(?:תותר|מותרת|תתאפשר|יתאפשר|ניתן|יותר)\b')


def _sentence_around(txt: str, pos: int) -> str:
    """Return a tight clause around pos.

    Prefer the span from the nearest preceding clause-opening verb to the next
    sentence terminator; fall back to a bounded window so a boundary-less run of
    clauses (e.g. plan 101-1218999) can't balloon the sentence.
    """
    # Nominal sentence bounds (Hebrew letter + period, or ")" separators, or numbered "N ").
    start = 0
    for m in re.finditer(r'[א-ת]\.\s|\)\s|\.\s', txt[:pos]):
        start = m.end()
    end = len(txt)
    m = re.search(r'[א-ת]\.\s|\.\s*\(\d|\.\s', txt[pos:])
    if m:
        end = pos + m.start() + 1
    # Tighten the start: if the nominal sentence is long, begin at the last
    # clause-opening verb at or before the match.
    if pos - start > 130:
        opens = list(_CLAUSE_OPEN.finditer(txt, start, pos + 1))
        if opens:
            start = opens[-1].start()
    # Hard cap so nothing runs away.
    if end - start > 240:
        end = min(end, pos + 160)
        start = max(start, pos - 80)
    return txt[start:end].strip()


def detect_unit_bonus(note_txt: str) -> Optional[dict]:
    if not note_txt:
        return None
    best = None
    for rx, kind in _PATTERNS:
        for m in rx.finditer(note_txt):
            pct = int(m.group(1))
            if not (1 <= pct <= 50):
                continue
            sent = _sentence_around(note_txt, m.start())
            # reject transfer-between-lots and pure size-mix false hits
            clause = note_txt[max(0, m.start()-15): m.end()+15]
            if _NIYUD.search(clause):
                continue
            # size-mix guard: "N% מיח"ד יהיו קטנות/בשטח עד" — the % qualifies a
            # size requirement, not a bonus. Detect when the token right after the
            # units-word is a size cue and there is no bonus verb before the %.
            after = note_txt[m.end(): m.end()+40]
            if re.match(r'\s*(?:יהיו|תהיינה|יכלול)?\s*(?:קטנ|דיר|בשטח)', after) and 'תוספת' not in clause and 'הגדל' not in clause:
                continue
            cand = {'pct': pct, 'kind': kind, 'sentence': sent}
            # prefer the match whose sentence carries the "no extra process" hallmark
            hallmark = bool(re.search(r'ללא\s+(?:שינוי|תוספת|צורך)|בלא\s+|לעת\s+מתן\s+היתר', sent))
            cand['hallmark'] = hallmark
            if best is None or (hallmark and not best['hallmark']) or (pct > best['pct'] and hallmark == best['hallmark']):
                best = cand
    return best
