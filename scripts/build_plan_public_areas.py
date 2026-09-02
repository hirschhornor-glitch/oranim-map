# -*- coding: utf-8 -*-
"""
בונה data/plan_public_areas.json — נתונים פר-תב"ע שאינם קיימים בגיליון ולכן
אינם ב-plans.geojson, ודרושים לדוח "מ"ר הפרשה ציבורית ליח"ד":

  open_path    שבילים (שטח קרקע)          — מטבלה 5
  open_square  כיכרות עירוניות (קרקע)      — מטבלה 5
  open_t5      שצ"פ לפי טבלה 5             — גיבוי כש-shatzap_out בגיליון ריק
  brown_land   קרקע חומה (שטח מגרש)        — גיבוי כש-shavatz_out_plot בגיליון ריק
  resid_sqm    מ"ר מגורים בנוי             — מטבלה 5, מכנה ליחס ציבור:מגורים

שצ"פ עצמו נלקח בדוח מ-shatzap_out שבגיליון (המאסטר); open_t5 משמש רק כגיבוי.
שבילים וכיכרות נשמרים בנפרד ולא מקופלים לתוך השצ"פ, כדי שלא ייספרו פעמיים
במקרים שבהם הגיליון כבר כלל אותם (למשל 101-0836809: 7,143 בגיליון מול
6,201 שצ"פ + 608 שביל בטבלה 5).

הרצה: py scripts/build_plan_public_areas.py
ואחריו: py scripts/merge_landuse_areas.py  (מוסיף את שכבת ייעודי הקרקע, שהיא
המקור המדויק יותר לשטחים המרחביים).

מקור: קבצי טבלה 5 השמורים ב-temp_xlsx.
"""
import sys, json
from pathlib import Path
sys.path.insert(0, r'C:\ORANIM')
sys.stdout.reconfigure(encoding='utf-8')
from parse_table5_xlsx import parse_table5_xlsx, _categories

SRC = Path(r'C:\ORANIM\temp_xlsx')
DST = Path(r'C:\ORANIM\oranim-app\data\plan_public_areas.json')

OPEN_CATS = {
    'open_t5':     ['שטח ציבורי פתוח', 'שצ"פ'],
    'open_path':   ['שביל'],
    'open_square': ['ככר עירונית', 'כיכר עירונית'],
}
# דרך משולבת / דרך ו/או טיפול נופי אינן נספרות: מדריך מינהל התכנון 2018 מוציא
# מערכות תנועה ואיי תנועה מספירת המרחב הציבורי הפתוח.

# קרקע חומה = מגרש שייעודו חום-דומיננטי בלבד. ייעוד מעורב ("מגורים מסחר ומבנים
# ומוסדות ציבור") נפסל: שם רוב שטח המגרש אינו ציבורי וההקצאה יושבת בתוכו כהפרשה
# מבונה, כך שספירת המגרש כולו כקרקע ציבורית מנפחת פי כמה. הכלל המחמיר אומת מול
# shavatz_out_plot שבגיליון: 42 מתוך 48 החופפים תואמים ±10%.
BROWN_WORDS = ['מבנים ומוסדות ציבור', 'מוסדות ציבור', 'מבני ציבור']
BROWN_DISQUALIFY = ['מגורים', 'מסחר', 'תעסוקה', 'תיירות', 'מלונ', 'דיור מיוחד',
                    'תחבורה', 'דרך']


def is_brown_plot(yiyud):
    if not any(w in yiyud for w in BROWN_WORDS):
        return False
    return not any(d in yiyud for d in BROWN_DISQUALIFY)


out, n_open, n_resid, n_brown = {}, 0, 0, 0
files = sorted(SRC.glob('*.xlsx'))
for i, f in enumerate(files):
    try:
        r = parse_table5_xlsx(f)
    except Exception:
        continue
    if r is None or r.error:
        continue
    rec = {k: 0.0 for k in OPEN_CATS}
    rec['brown_land'] = 0.0
    resid = 0.0
    seen = set()          # מגרש חוזר על פני כמה שורות שימוש — קרקע נספרת פעם אחת
    for row in r.rows:
        if row.get('is_total') or row.get('is_grand_total'):
            continue
        y = (row.get('yiyud') or '').strip()
        matched = False
        for key, words in OPEN_CATS.items():
            if any(w in y for w in words):
                sig = (key, str(row.get('parcel')), row.get('plot_size_sqm'))
                if sig not in seen:
                    seen.add(sig)
                    rec[key] += (row.get('plot_size_sqm') or 0)
                matched = True
                break
        if matched:
            continue
        if is_brown_plot(y):
            sig = ('brown', str(row.get('parcel')), row.get('plot_size_sqm'))
            if sig not in seen:
                seen.add(sig)
                rec['brown_land'] += (row.get('plot_size_sqm') or 0)
            continue
        # שורות מגורים טהורות בלבד — שורה מעורבת אי אפשר לפצל
        cats = _categories(y + ' ' + (row.get('use') or ''))
        if cats == {'resid'}:
            a = row.get('total_building_sqm') or 0.0
            if not a:
                a = (row.get('total_above_sqm') or 0.0) + (row.get('total_below_sqm') or 0.0)
            resid += a
    rec = {k: round(v, 1) for k, v in rec.items() if v}
    if resid:
        rec['resid_sqm'] = round(resid, 1)
        n_resid += 1
    if any(k.startswith('open_') for k in rec):
        n_open += 1
    if rec.get('brown_land'):
        n_brown += 1
    if rec:
        out[f.stem] = rec
    if i % 100 == 0:
        print('  %d/%d' % (i, len(files)), file=sys.stderr)

DST.parent.mkdir(parents=True, exist_ok=True)
json.dump(out, open(DST, 'w', encoding='utf-8'), ensure_ascii=False, separators=(',', ':'))
tot = lambda k: sum(v.get(k, 0) for v in out.values())
print('נסרקו %d קבצים → %d תב"עות' % (len(files), len(out)))
print('  עם שטח ציבורי פתוח: %d  (שצ"פ %s · שבילים %s · כיכרות %s מ"ר)'
      % (n_open, format(int(tot('open_t5')), ','), format(int(tot('open_path')), ','),
         format(int(tot('open_square')), ',')))
print('  עם קרקע חומה: %d  (%s מ"ר)' % (n_brown, format(int(tot('brown_land')), ',')))
print('  עם מ"ר מגורים: %d  (%s מ"ר)' % (n_resid, format(int(tot('resid_sqm')), ',')))
print('נכתב → %s' % DST)
