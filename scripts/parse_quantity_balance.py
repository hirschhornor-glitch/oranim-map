"""Parser for the Mavat 'נתונים כמותיים עיקריים בתכנית' section textContent.

Each category card reads (in textContent, incl. collapsed/hidden):
    <label> (<unit>) <proposed> <unit> [מצב מאושר* <existing> <unit>]
        [שינוי (+/-) למצב המאושר* <change> <unit>] [הערות: ...]

Distinction: existing token = "מצב מאושר*" (מאושר, no ה);
             change   token = "למצב המאושר*" (המאושר, with ה).
proposed = existing + change  (existing defaults to 0 when the token is absent).

Public API: parse_quantity_balance(text) -> dict
"""
import re

UNIT_RE = r'(?:יח"ד|מ"ר|מ”ר|יח”ד)'
# a number like 1,500 or 31.436 or 311 (keep . for dunam but cards use , for thousands)
NUM = r'([+\-]?[\d][\d,]*(?:\.\d+)?)'

# Split points: a category label immediately followed by (unit)
CARD_RE = re.compile(r'([^\d()*]{2,40}?)\s*\((יח"ד|מ"ר|מ”ר|יח”ד)\)\s*' + NUM)
EXISTING_RE = re.compile(r'(?<![ל])מצב מאושר\*\s*' + NUM)   # מצב מאושר* (not למצב)
CHANGE_RE   = re.compile(r'למצב המאושר\*\s*' + NUM)


def _num(s):
    if s is None:
        return 0.0
    s = s.replace(',', '').replace('+', '').strip()
    try:
        return float(s)
    except ValueError:
        return 0.0


def parse_quantity_balance(text: str) -> dict:
    """Return per-category planned/existing/change + residential/commerce/etc rollups."""
    if not text:
        return {}
    cards = []
    matches = list(CARD_RE.finditer(text))
    for i, m in enumerate(matches):
        label = m.group(1).strip(' \t\n*-:')
        unit = m.group(2).replace('”', '"')
        proposed = _num(m.group(3))
        seg = text[m.end(): matches[i + 1].start() if i + 1 < len(matches) else len(text)]
        ex_m = EXISTING_RE.search(seg)
        ch_m = CHANGE_RE.search(seg)
        existing = _num(ex_m.group(1)) if ex_m else None
        change = _num(ch_m.group(1)) if ch_m else None
        # derive: proposed = existing + change
        if existing is None and change is not None:
            existing = proposed - change
        if existing is None:
            existing = 0.0
        if change is None:
            change = proposed - existing
        cards.append({'label': label, 'unit': unit, 'proposed': proposed,
                      'existing': existing, 'change': change})

    def _is_resid(lbl):
        return ('מגורים' in lbl or 'דיור' in lbl or 'דירות' in lbl) and 'מסחר' not in lbl
    out = {'cards': cards}
    # residential units (יח"ד)
    #
    # No residential card at all ⇒ the accordion says NOTHING about housing, which
    # is not the same as "0 existing units". Plan 101-1003177 (תוספות בניה לשם
    # הרחבות יח"ד, בית"ר 34) renders a נתונים-כמותיים panel holding only
    # "סה"כ שטח בדונם 0.593"; summing an empty list produced units_in=0, and the
    # caller's `units_add = table5_total − units_in` then reported 10 brand-new
    # יח"ד for a plan that only ENLARGES 10 existing ones (in 10 → out 10, add 0).
    # Return None so callers — all of which guard with `is not None` — leave the
    # field blank instead of writing a fabricated zero.
    ru = [c for c in cards if c['unit'] == 'יח"ד' and _is_resid(c['label'])]
    if ru:
        out['units_total']  = int(round(sum(c['proposed'] for c in ru)))
        out['units_in']     = int(round(sum(c['existing'] for c in ru)))
        out['units_add']    = int(round(sum(c['change'] for c in ru)))
    else:
        out['units_total'] = out['units_in'] = out['units_add'] = None
    # commerce / employment / public (מ"ר) existing (נכנס) + proposed
    def _area(pred, key):
        sel = [c for c in cards if c['unit'] == 'מ"ר' and pred(c['label'])]
        out[key + '_in']  = round(sum(c['existing'] for c in sel), 1)
        out[key + '_out'] = round(sum(c['proposed'] for c in sel), 1)
    _area(lambda l: 'מסחר' in l, 'commerce')
    _area(lambda l: 'תעסוקה' in l or 'משרד' in l, 'employment')
    _area(lambda l: 'מבני ציבור' in l or 'מבנים ומוסדות' in l or 'מוסדות ציבור' in l, 'public')
    # multiplier
    out['multiplier'] = (round(out['units_total'] / out['units_in'], 2)
                         if out['units_in'] else None)
    return out


if __name__ == '__main__':
    import sys
    if sys.platform.startswith('win'):
        sys.stdout.reconfigure(encoding='utf-8')
    SAMPLE = ('טבלה זו הינה לנתונים סטטיסטים בלבד ... סה”כ שטח בדונם 31.436 '
              'דירות להשכרה (יח"ד) 350 יח"ד שינוי (+/-) למצב המאושר* +350 יח"ד '
              'מבני ציבור (מ"ר) 31,833 מ"ר שינוי (+/-) למצב המאושר* +31,833 מ"ר '
              'מגורים (יח"ד) 1,500 יח"ד מצב מאושר* +311 יח"ד שינוי (+/-) למצב המאושר* +1,189 יח"ד '
              'הערות: מספר יח"ד המוצעות כולל ... מק/14295. '
              'מגורים (מ"ר) 126,293 מ"ר שינוי (+/-) למצב המאושר* +126,293 מ"ר סגור')
    r = parse_quantity_balance(SAMPLE)
    import json
    for c in r['cards']:
        print(f"  {c['label']:20s} ({c['unit']})  proposed={c['proposed']:>10.0f}  existing={c['existing']:>8.0f}  change={c['change']:>10.0f}")
    print(json.dumps({k: v for k, v in r.items() if k != 'cards'}, ensure_ascii=False, indent=1))
