# -*- coding: utf-8 -*-
"""
ממלא hafrash_sqm בגיליון עבור תכניות שבהן השדה ריק/אפס בעוד hafrash_prg
מפרט מ"ר מפורשים. מילוי בלבד — לעולם לא דורס ערך קיים שאינו 0.

הרקע: הסריקה מצאה 32 מועמדים ב-plans.geojson, אך התברר שהגיליון כבר מעודכן
ב-30 מהם (ה-geojson הוא שהיה מיושן). נותרו שני פערים אמיתיים בגיליון,
שהוחלו ב-2026-08-27: 101-0813329 (0 → 250, לפי ההוראות) ו-101-0275636
(ריק → 1200). ריצה חוזרת אמורה להחזיר "מועמדים למילוי: 0".

הרצה יבשה: py scripts/fix_hafrash_gaps_gs.py
כתיבה בפועל: py scripts/fix_hafrash_gaps_gs.py --apply
"""
import json, re, sys, datetime
sys.stdout.reconfigure(encoding='utf-8')
import gspread
from google.oauth2.service_account import Credentials

CREDS = r"C:\ORANIM\oranim-490018-ceaf784afe61.json"   # מחוץ לריפו בכוונה
SHEET_ID = "1_AcuuA1CNPh6jXc_lZKNghfpEF1aDPV8Zci8QPz2WVE"
COL_PLAN, COL_HAF, COL_HAF_PRG, COL_MOD, COL_OUT = 6, 43, 44, 40, 18
# הפורמט בגיליון אינו אחיד: לעתים '(250 מ"ר)' ולעתים '(250)' בלבד — שתי הצורות
# נתפסות. ערך קטן מ-MIN_SQM נפסל, כי שם הסוגריים כמעט תמיד מונים מספר כיתות
# או מגרש ולא שטח.
MR = re.compile(r'\((\d[\d,]*(?:\.\d+)?)\s*(?:מ["\'׳״]{0,2}\s*ר)?\)')
MIN_SQM = 40
APPLY = '--apply' in sys.argv

gc = gspread.authorize(Credentials.from_service_account_file(
    CREDS, scopes=['https://www.googleapis.com/auth/spreadsheets'])) if APPLY else None
ws = gc.open_by_key(SHEET_ID).sheet1 if APPLY else None
# הריצה היבשה נשענת על _gs_snapshot.json כדי לא לבזבז קריאות API
vals = ws.get_all_values() if APPLY else json.load(open(r'C:\ORANIM\_gs_snapshot.json', encoding='utf-8'))


def cell(row, c):
    return row[c-1].strip() if len(row) >= c else ''


todo = []
for i, row in enumerate(vals[1:], start=2):
    pn = cell(row, COL_PLAN)
    if not pn:
        continue
    cur, prg = cell(row, COL_HAF), cell(row, COL_HAF_PRG)
    if cur not in ('', '0'):      # מילוי בלבד
        continue
    # מגן מפני כפל ספירה: כשיש שב"צ יוצא, אותה הקצאה בדרך כלל כבר נספרה שם.
    # אומת פרטנית ב-101-0857086 (מגרש 17 בשני השדות), 101-0657593 (השב"צ מסומן
    # במפורש "הפרשה מבונה + עצמאי"), 101-1372465 ו-101-0350173.
    if cell(row, COL_OUT) not in ('', '0'):
        continue
    ns = [v for v in (float(n.replace(',', '')) for n in MR.findall(prg)) if v >= MIN_SQM]
    if not ns:
        continue
    todo.append((i, pn, cur, sum(ns), prg))

print('מועמדים למילוי: %d' % len(todo))
for i, pn, cur, new, prg in todo:
    print('  שורה %-5d %-13s  %-4s → %-8.0f  %s' % (i, pn, repr(cur), new, prg[:70]))

if not APPLY:
    print('\n(ריצה יבשה — הוסף --apply כדי לכתוב)')
    sys.exit(0)

if not todo:
    print('אין מה לכתוב.')
    sys.exit(0)

bak = r'C:\ORANIM\_bak_hafrash_gs_%s.json' % datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
json.dump([{'row': i, 'plan': pn, 'old_hafrash_sqm': cur} for i, pn, cur, _, _ in todo],
          open(bak, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
print('\nגיבוי → %s' % bak)

stamp = datetime.datetime.now().strftime('%Y-%m-%d')
updates = []
for i, pn, cur, new, prg in todo:
    updates.append({'range': gspread.utils.rowcol_to_a1(i, COL_HAF), 'values': [[str(int(new))]]})
    updates.append({'range': gspread.utils.rowcol_to_a1(i, COL_MOD), 'values': [[stamp]]})
ws.batch_update(updates)
print('עודכנו %d תאים ב-%d שורות.' % (len(updates), len(todo)))
