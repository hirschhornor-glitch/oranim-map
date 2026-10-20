# -*- coding: utf-8 -*-
"""Build data/hotels.json — planned hotel supply in the Oranim plans.

Per plan, three numbers and where each came from:
  rooms     hotel rooms (Table 5 units of a pure hotel row; else a Table 5 PDF
            read / footnote). None = unknown ('?'), never 0.
  sqm       hotel main area (עיקרי, above+below) of pure hotel rows in Table 5.
            Table 5 is binding. When the hotel sits in a MIXED row that Table 5
            does not split, the Mavat quantitative panel's hotel figure is used and
            flagged — the panel is statistical, not binding.
  sqm_in    approved-state hotel area from the Mavat panel (statistical) →
            tells a new hotel from an extension of an existing one.

Plans where hotel is only one PERMITTED use among several (e.g. 'משרדים/מלונאות')
are listed under kind='optional' with the quoted clause — they are not supply.

Inputs: _hotels_audit.json (hotels_audit.py, Table 5 xlsx), PDF_T5 below (Table 5
pages read visually from the horaot PDFs), quantity_balance_results.json (panel),
plans.geojson, landuse_xplan.geojson, sub_neighborhoods.geojson.

  py scripts/build_hotels.py            # write data/hotels.json
  py scripts/build_hotels.py --push     # …and push via git_sync
"""
import io, json, os, re, sys
from datetime import date

sys.stdout.reconfigure(encoding='utf-8')
HERE = os.path.dirname(os.path.abspath(__file__))
APP = os.path.dirname(HERE)
ROOT = r'C:\ORANIM'
sys.path.insert(0, HERE)
from extract_maintenance_fund import load_sub_polys, geo_sub  # noqa: E402

OUT_REL = 'data/hotels.json'
REJECTED = ('נדחתה', 'נגנזה', 'בוטלה')

# Table 5 read visually from the horaot PDF (plans with no Table 5 xlsx), 2026-10-20.
# Page images, not text/OCR (RTL digits). sqm = main area above+below of the hotel row.
# Cross-check vs the Mavat panel (independent): 0415992 2848=2848, 0283978 4221=4221,
# 0193193 18500=18500, 0236612 3900=3900, 0548891 1233.7≈1234, 0210054 10022+300 balc.
# =10322, 0049056 21770+2000 balc.=23770 — 7/7 agree.
# rooms None = Table 5 silent on rooms → '?'.
PDF_T5 = {
    '101-0548891': {'rooms': None, 'sqm': 1233.69, 'page': 12},
    '101-0415992': {'rooms': 74, 'sqm': 2848, 'page': 20, 'note': 'הערה ח: "כמות חדרי האירוח בתכנית - 74"'},
    '101-0790360': {'rooms': 17, 'sqm': 795, 'page': 18, 'note': 'הערה (2): 17 = יח\' אכסון מלונאי'},
    '101-0888032': {'rooms': 24, 'sqm': None, 'page': 14, 'mixed': True,
                    'note': 'הערה (4): יחידות מלונאיות; לשימוש מגורים עד 8 יח"ד'},
    '101-0387449': {'rooms': 300, 'sqm': 16200, 'page': 45, 'note': 'הערה (6): "עבור כ-300 חדרי מלון"'},
    '101-0818575': {'rooms': None, 'sqm': None, 'page': 12, 'mixed': True},
    '101-0283978': {'rooms': None, 'sqm': 4221, 'page': 22},
    '101-0792077': {'rooms': None, 'sqm': 6208, 'page': 14},
    '101-0182667': {'rooms': 44, 'sqm': 1304, 'page': 15,
                    'note': '44 בעמודת יח"ד של שורת המלונאות; אין הערה המפרשת'},
    '101-0193193': {'rooms': 330, 'sqm': 18500, 'page': 13, 'note': 'הערה (2): "מינימום 330 חדרי מלון"'},
    '101-0236612': {'rooms': 70, 'sqm': 3900, 'page': 16, 'note': 'הערה (3): יחידות איכסון מלונאי; סעיף 2.3: 70 חדרים'},
    '101-0049056': {'rooms': 200, 'sqm': 21770, 'page': 17, 'note': 'סעיף 2.3: לפחות 200 חדרי מלון'},
    '101-0657601': {'rooms': None, 'sqm': None, 'page': 16, 'mixed': True},
    '101-0210054': {'rooms': 239, 'sqm': 10022, 'page': 15,
                    'note': 'הערה 4: 239 חדרים — 129 מאושרים בהיתר קיים + 110 מוצעים'},
    '101-0547992': {'rooms': None, 'sqm': None, 'page': 15, 'mixed': True},
}

# Hotel is a permitted (not dedicated) use — not counted as supply.
# plan -> clause (paraphrased) with its place in the horaot.
OPTIONAL = {
    '101-0132944': 'טבלה 5 הערה (6): יותרו כ-30 יחידות מלונאיות כחלופה; 50% מגורים / 50% תעסוקה-מסחר-מלונאות',
    '101-1380765': 'אותו בניין (דוד המלך 6) — טבלה 5 ללא שורת מלונאות; פאנל מבא"ת 605 מ"ר',
    '101-0606129': 'טבלה 5 הערה (1): מלונאות, הסעדה ומסחר יחד עד 1,440 מ"ר בייעוד ספורט ונופש',
    '101-0150805': 'טבלה 5 הערה (6): שטח התעסוקה בתא 2 (19,162 מ"ר) ישמש גם למלונאות',
    '101-0653840': 'טבלה 5: שטחי התעסוקה כוללים שטחי תיירות במידת הצורך',
    '101-1422401': 'סעיף 4.1.2: בניין C ישמש לתעסוקה שיתופית, משרדים ומלונאות + מגורים להשכרה',
    '101-0955450': 'סעיף 4.1.2: קומות המסד לשימושי תעסוקה/מלונאות',
    '101-0921213': 'סעיף 2.1: מגדלים למגורים, מלונאות, תעסוקה ומסחר — ללא שורת מלונאות נפרדת',
    '101-1225218': 'סעיף 2.1: מסד למסחר ו/או תעסוקה, מלונאות ו/או מגורי סטודנטים',
    '101-0901140': 'סעיף 4.1: מגורים מסחר מלונאות תעסוקה; תותר המרת מלונאות לתעסוקה',
    '101-0867317': 'סעיף 4: "דיור מיוחד ותיירות" — מעונות סטודנטים / מלונאות / דיור מוגן',
    '101-1250026': 'סעיף 4: מלונאות ברשימת השימושים המותרים',
    '101-1587294': 'סעיף 4: מלונאות, ספא וחדרי טיפולים ברשימת השימושים המותרים',
    '101-1095330': 'סעיף 4 (תעסוקה): מלונאות ברשימת השימושים המותרים',
    '101-1236736': 'סעיף 4.3: תתאפשר המרת שטחי תעסוקה ומסחר למלונאות',
}

# Same site counted by two plans → count the operative one only.
# plan -> (counted plan, reason)
SUPERSEDED = {
    '101-0779546': ('101-1443126', 'כתר דוד — plan_containment: 1443126 אופרטיבית'),
    '101-0548891': ('101-1052901', 'אחד העם 5 — 1052901 מוסיפה קומות על אותו מלון (פאנל: מאושר 1,234)'),
    '101-0547992': ('101-1422401', 'שער דרום — plan_containment: 1422401 אופרטיבית'),
    '101-1160019': ('101-0182667', 'דוד המלך 8 — אותו בניין; 1160019 מוסיפה קומת מגורים'),
}

# GS `hotels` values with no basis in Table 5 / panel / horaot (for the Sheet fix).
GS_NO_BASIS = {
    '101-1026574': 'טבלה 5: 29,400 מ"ר בלי מספר חדרים; ה-400 ללא מקור',
    '101-1052901': 'טבלה 5: 2,225 מ"ר בלי מספר חדרים; ה-54 ללא מקור',
    '101-0548891': 'טבלה 5: אין מספר חדרים; ה-47 ללא מקור',
    '101-1231349': 'טבלה 5: מגורים בלבד; ה-1 ללא מקור',
    '101-1380765': 'טבלה 5: אין שורת מלונאות; ה-30 הוא מספר החלופה של 101-0132944',
    '101-1160019': 'טבלה 5: שורה מעורבת (9 יח"ד); ה-44 שייך ל-101-0182667',
    '101-1488261': 'טבלה 5 "לא רלוונטי" (הארכת תוקף ל-101-0193193); ה-330 כפול',
    '101-0606129': 'טבלה 5: ספורט ונופש; ה-20 ללא מקור',
}


def _f(v):
    try:
        return float(str(v).replace(',', '').replace('~', '').strip())
    except (TypeError, ValueError):
        return None


def load_panel():
    d = json.load(io.open(os.path.join(ROOT, 'quantity_balance_results.json'), encoding='utf-8'))
    out = {}
    for r in d.get('results', d) if isinstance(d, dict) else d:
        for c in r.get('cards') or []:
            if 'מלון' in c.get('label', '') and c.get('unit') == 'מ"ר':
                out[r['plan_number']] = {'out': c.get('proposed'), 'in': c.get('existing'),
                                         'change': c.get('change')}
    return out


def build():
    audit = json.load(io.open(os.path.join(ROOT, '_hotels_audit.json'), encoding='utf-8'))
    panel = load_panel()
    plans = json.load(io.open(os.path.join(APP, 'data', 'plans.geojson'), encoding='utf-8'))['features']
    subs = load_sub_polys()
    geo = {}
    for f in plans:
        pn = f['properties'].get('plan_name')
        if pn and pn not in geo:
            geo[pn] = f
    out = {}
    cands = set(PDF_T5) | set(OPTIONAL) | set(panel) & set(audit)
    for pn, a in audit.items():
        if (a.get('rooms') or a.get('sqm_main') or a.get('mixed_rows')
                or a.get('landuse_hotel_plots') or _f(a.get('gs_hotels'))):
            cands.add(pn)
    for pn in sorted(cands):
        if pn not in geo:
            continue
        p = geo[pn]['properties']; a = audit.get(pn, {}); pan = panel.get(pn)
        rooms = sqm = None; rooms_src = sqm_src = ''
        mixed = bool(a.get('mixed_rows')) and not a.get('sqm_main') and not a.get('rooms')
        if a.get('t5') == 'ok':
            if a.get('rooms'):
                rooms, rooms_src = a['rooms'], 'טבלה 5'
            if a.get('sqm_main'):
                sqm, sqm_src = a['sqm_main'], 'טבלה 5'
        note = ''
        if pn in PDF_T5:
            r = PDF_T5[pn]; note = r.get('note', '')
            if rooms is None and r.get('rooms'):
                rooms, rooms_src = r['rooms'], 'טבלה 5 (PDF עמ\' %s)' % r.get('page', '?')
            if sqm is None and r.get('sqm'):
                sqm, sqm_src = r['sqm'], 'טבלה 5 (PDF עמ\' %s)' % r.get('page', '?')
            mixed = mixed or bool(r.get('mixed'))
        if sqm is None and pan and pan.get('out'):
            sqm = pan['out']
            sqm_src = 'פאנל מבא"ת' + (' — שורה מעורבת בטבלה 5' if mixed else '')
        kind = 'optional' if pn in OPTIONAL else ('mixed' if mixed else 'dedicated')
        if kind == 'optional':          # a permitted use is not supply — keep the panel figure apart
            sqm = sqm_src = None
        if kind != 'optional' and rooms is None and sqm is None:
            continue
        sqm_in = pan.get('in') if pan else None
        stage = None
        if pan:
            if (pan.get('in') or 0) == 0:
                stage = 'new'
            elif (pan.get('change') or 0) > 0:
                stage = 'extension'
            elif (pan.get('change') or 0) == 0:
                stage = 'unchanged'
        st = (p.get('status_mavat') or '').strip()
        out[pn] = {
            'plan_name': pn,
            'title': p.get('plan_name_he') or pn,
            'status': st,
            'rejected': any(x in st for x in REJECTED),
            'minahak': p.get('minahak') or '',
            'sub_neighborhood': geo_sub(geo[pn].get('geometry'), subs) or p.get('sub_neighborhood') or '',
            'mavat_url': p.get('mavat_url') or '',
            'kind': kind,
            'rooms': rooms, 'rooms_src': rooms_src,
            'sqm': sqm, 'sqm_src': sqm_src,
            'sqm_in': sqm_in, 'sqm_change': pan.get('change') if pan else None,
            'stage': stage,
            'mixed_rows': [{'yiyud': m['yiyud'], 'use': m['use'], 'sqm': m['main_above'] + m['main_below'],
                            'units': m['units']} for m in a.get('mixed_rows', [])],
            'optional_clause': OPTIONAL.get(pn),
            'panel_sqm': pan.get('out') if pan else None,
            'note': note,
            'superseded_by': SUPERSEDED.get(pn, (None, ''))[0],
            'superseded_note': SUPERSEDED.get(pn, (None, ''))[1],
            'plots': [x['num'] for x in a.get('landuse_hotel_plots', [])],
            'gs_note': GS_NO_BASIS.get(pn, ''),
        }
    return out


def main():
    data = build()
    payload = {'generated': date.today().isoformat(), 'plans': data}
    path = os.path.join(APP, OUT_REL)
    with io.open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, separators=(',', ':'))
    act = [r for r in data.values() if not r['rejected'] and r['kind'] != 'optional' and not r['superseded_by']]
    print(f"{len(data)} plans → {OUT_REL}: {len(act)} active supply, "
          f"{sum(r['rooms'] or 0 for r in act):,.0f} rooms, {sum(r['sqm'] or 0 for r in act):,.0f} sqm")
    if '--push' in sys.argv:
        sys.path.insert(0, ROOT)
        import git_sync
        def edit(d):
            if d == payload:
                return False
            d.clear(); d.update(payload); return True
        import subprocess
        tracked = subprocess.run(['git', 'ls-files', '--error-unmatch', OUT_REL], cwd=APP,
                                 capture_output=True).returncode == 0
        # update_json_and_push stashes untracked files first — a new file vanishes from
        # under it. First run: commit the written file as is.
        ok = (git_sync.update_json_and_push(OUT_REL, edit, 'data: hotels.json — planned hotel supply')
              if tracked else
              git_sync._commit_and_push_paths([OUT_REL], 'data: hotels.json — planned hotel supply', repo_dir=APP))
        if not ok:
            sys.exit('push failed')


if __name__ == '__main__':
    main()
