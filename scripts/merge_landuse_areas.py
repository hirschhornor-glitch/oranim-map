# -*- coding: utf-8 -*-
"""
ממזג לתוך data/plan_public_areas.json את שטחי הקרקע הציבורית מתוך
data/landuse_xplan.geojson — ייעודי הקרקע של התכנית עצמה, עם גיאומטריה.

למה זה המקור העדיף על טבלה 5 ועל הגיליון:
  · כיסוי — 167 תכניות עם שצ"פ מול 139, ו-89 עם שבילים מול 18.
  · דיוק — shape_area נגזר מהגיאומטריה. באימות מול shavatz_out_plot שבגיליון
    72 מתוך 73 תואמים ±15%; בשצ"פ 93 מתוך 120, וכל הפערים הם ערכי-דמה
    בגיליון (101-0242560 רשום "1" מול 1,192 מ"ר בפועל).
  · legal_area דווקא אינו אמין — ריק ב-1,713 מתוך 3,562 פוליגונים ו-630
    חורגים פי 3+ מ-shape_area, ולכן נעשה שימוש ב-shape_area בלבד.

מכאן שאין צורך למשוך את "דוח תאי שטח" מ-Mavat: הגיאומטריה כבר נותנת את
אותו מספר, בלי reCAPTCHA ובלי חילוץ מ-PDF.

הרצה: py scripts/merge_landuse_areas.py   (אחרי build_plan_public_areas.py)
"""
import json, sys, collections
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8')

LANDUSE = Path(r'C:\ORANIM\oranim-app\data\landuse_xplan.geojson')
DST = Path(r'C:\ORANIM\oranim-app\data\plan_public_areas.json')

OPEN_W   = ['שטח ציבורי פתוח']
PATH_W   = ['שביל']
SQUARE_W = ['ככר עירונית', 'כיכר עירונית']
BROWN_W  = ['מבנים ומוסדות ציבור', 'מוסדות ציבור', 'מבני ציבור']
# ייעוד מעורב עם מגורים/מסחר אינו קרקע ציבורית: רוב המגרש פרטי וההקצאה
# יושבת בתוכו כהפרשה מבונה.
BROWN_DQ = ['מגורים', 'מסחר', 'תעסוקה', 'תיירות', 'מלונ', 'דיור מיוחד', 'תחבורה', 'דרך']
# "שטח פרטי פתוח" אינו ציבורי. מערכות תנועה אינן נספרות (מדריך 2018).


def bucket(name):
    if any(w in name for w in OPEN_W):   return 'lu_open'
    if any(w in name for w in PATH_W):   return 'lu_path'
    if any(w in name for w in SQUARE_W): return 'lu_square'
    if any(w in name for w in BROWN_W) and not any(d in name for d in BROWN_DQ):
        return 'lu_brown'
    return None


agg = collections.defaultdict(lambda: collections.defaultdict(float))
feats = json.load(open(LANDUSE, encoding='utf-8'))['features']
for f in feats:
    p = f['properties']
    pn = str(p.get('pl_number') or '').strip()
    area = p.get('shape_area') or 0
    if not pn or area <= 0:
        continue
    b = bucket(str(p.get('mavat_name') or '').strip())
    if b:
        agg[pn][b] += area


# מפתח הקובץ הוא taba (ללא הקידומת 101-); landuse ממופתח ב-pl_number המלא
def taba_of(pl_number):
    return pl_number.split('-')[-1].lstrip('0') or pl_number


out = json.load(open(DST, encoding='utf-8')) if DST.exists() else {}
touched = 0
for pn, vals in agg.items():
    t = taba_of(pn)
    # התאמה לצורת המפתח הקיימת בקובץ (לרוב ללא אפסים מובילים)
    key = t if t in out else (pn.split('-')[-1] if pn.split('-')[-1] in out else t)
    rec = out.setdefault(key, {})
    for k, v in vals.items():
        rec[k] = round(v, 1)
    touched += 1

json.dump(out, open(DST, 'w', encoding='utf-8'), ensure_ascii=False, separators=(',', ':'))
cnt = lambda k: sum(1 for v in out.values() if v.get(k))
tot = lambda k: sum(v.get(k, 0) for v in out.values())
print('פוליגונים שנסרקו: %d → %d תב"עות עודכנו' % (len(feats), touched))
for k, lbl in (('lu_open', 'שצ"פ'), ('lu_brown', 'קרקע חומה'),
               ('lu_path', 'שבילים'), ('lu_square', 'כיכרות')):
    print('  %-12s %3d תב"עות · %s מ"ר' % (lbl, cnt(k), format(int(tot(k)), ',')))
print('סה"כ רשומות בקובץ: %d' % len(out))
print('נכתב → %s' % DST)
