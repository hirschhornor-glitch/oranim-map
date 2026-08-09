"""
extract_developers_from_horaot.py
Extracts developer (יזם / מגיש התכנית) and architect (עורך ראשי) from the
section-1.8 stakeholder tables in plan regulation PDFs (C:\\ORANIM\\horaot).

Sections used (in priority order for developer):
  1.8.2 יזם           -> developer
  1.8.1 מגיש התכנית   -> developer fallback
  1.8.4 עורך התכנית   -> architect (row where role = עורך ראשי)
Per entity row: prefer שם תאגיד (corporate name), fallback שם (person name).

Output: _developer_extract_results.json (review before writing to GS/geojson).

Usage:
    py extract_developers_from_horaot.py            # all targets
    py extract_developers_from_horaot.py 10         # limit
    py extract_developers_from_horaot.py --plan 101-1062645   # single plan test
"""
import json
import os
import re
import sys
from concurrent.futures import ProcessPoolExecutor, TimeoutError as FutTimeout

HORAOT_DIR = r"C:\ORANIM\horaot"
PLANS_GEOJSON = r"C:\ORANIM\oranim-app\data\plans.geojson"
OUT_FILE = r"C:\ORANIM\_developer_extract_results.json"

GARBAGE_DEVS = {'כתובת', 'זיהוי התכנית', 'סמכות:', 'מס\' יח"ד',
                'ניסיון', 'אדריכלים ומתכנני ערים', 'אדריכלים', 'פרטי'}
# עירייה / ועדה מקומית / רשות מקומית — גורם ציבורי-מוניציפלי בבעלי העניין
MUNI_RE = re.compile(r'עיריית|עירית|ועדה מקומית|הועדה המקומית|'
                     r'הוועדה המקומית|רשות מקומית')
# ערך שנראה כמו כתובת ("בלפור 16 , טלביה") — הסקרייפר הישן כתב כתובות בשדות
ADDR_RE = re.compile(r'\d+\s*,\s*\S|^(רחוב|שד|דרך|ככר)\b')


def value_is_garbage(v):
    v = (v or '').strip() if isinstance(v, str) else str(v or '')
    if not v:
        return False
    if v in GARBAGE_DEVS or v.startswith('.4') or v.isdigit():
        return True
    if ADDR_RE.search(v):
        return True
    # length is checked per JV partner — a 3-way partnership is legit at 70+ chars
    return any(len(part.strip()) >= 60 for part in v.split(' / '))

HEB = re.compile(r'[֐-׿]')


def fix_rtl(s):
    """Reverse visually-stored Hebrew text back to logical order.
    Reverses the whole string, then un-reverses ASCII/digit runs and
    swaps mirrored brackets."""
    if not s or not HEB.search(s):
        return (s or '').strip()
    out_lines = []
    for line in s.split('\n'):
        line = line.strip()
        if not line:
            continue
        if not HEB.search(line):
            out_lines.append(line)
            continue
        rev = line[::-1]
        # un-reverse runs of ASCII letters/digits (kept LTR inside RTL text)
        rev = re.sub(r'[0-9A-Za-z@.\-/]{2,}', lambda m: m.group(0)[::-1], rev)
        # swap mirrored brackets
        rev = rev.translate(str.maketrans('()[]{}<>', ')(][}{><'))
        out_lines.append(rev)
    return ' '.join(out_lines).strip()


def cell_has(cell, needle):
    return needle in (cell or '')


FOOTNOTE_RE = re.compile(r'\s*\(\s*\d+\s*\)\s*$')
# values that are not actual names: city names leaking from the ישוב column,
# bare legal-form words appearing in שם תאגיד, and entity-type values from
# the סוג column (פרטי/בעלים/...)
CITY_VALUES = {'ירושלים', 'תל אביב', 'תל אביב-יפו', 'בית שמש', 'מבשרת ציון',
               'שותפות מוגבלת', 'שותפות רשומה', 'חברה בע"מ',
               'פרטי', 'בעלים', 'מדינה', 'חברה', 'רשות מקומית', 'אחר',
               'עורך ראשי', 'אדריכל', 'מודד', 'ל"ר', 'לא רלוונטי',
               # generic firm descriptions that appear alone in שם תאגיד —
               # the real name is in the שם column (e.g. אדר' יגאל לוי)
               'אדריכלים ומתכנני ערים', 'אדריכלים', 'מהנדסים', 'ניסיון'}


def clean_name(val, reversed_text):
    fixed = fix_rtl(val) if reversed_text else re.sub(r'\s+', ' ', val).strip()
    fixed = FOOTNOTE_RE.sub('', fixed).strip()
    # collapse exact doubling from wrapped cells ("X בע"מ X בע"מ" -> "X בע"מ")
    words = fixed.split()
    if len(words) >= 4 and len(words) % 2 == 0 and words[:len(words)//2] == words[len(words)//2:]:
        fixed = ' '.join(words[:len(words)//2])
    # a real name must contain at least one letter (rejects "0", phone nums…)
    if not re.search(r'[֐-׿A-Za-z]', fixed):
        return ''
    return fixed


def parse_entity_table(table, reversed_text):
    """Given a data table (header row + entity rows), return list of names.
    Prefers corporate name column, falls back to person name column."""
    if not table:
        return []
    header = table[0]
    corp_marker = 'דיגאת םש' if reversed_text else 'שם תאגיד'
    name_marker = 'םש' if reversed_text else 'שם'
    corp_idx = name_idx = None
    for i, c in enumerate(header):
        c = (c or '').replace('\n', ' ').strip()
        if corp_marker in c:
            corp_idx = i
        elif c == name_marker:
            name_idx = i
    if corp_idx is None and name_idx is None:
        return []
    names = []
    for row in table[1:]:
        val = ''
        if corp_idx is not None and corp_idx < len(row) and (row[corp_idx] or '').strip():
            cand = clean_name(row[corp_idx], reversed_text)
            # corp column sometimes holds a stray city value — fall through
            if cand and cand not in CITY_VALUES:
                val = cand
        if not val and name_idx is not None and name_idx < len(row) and (row[name_idx] or '').strip():
            cand = clean_name(row[name_idx], reversed_text)
            if cand and cand not in CITY_VALUES:
                val = cand
        if val and val not in names:
            names.append(val)
    return names


def parse_architect_table(table, reversed_text):
    """1.8.4 table: return corporate/person name of the עורך ראשי row."""
    if not table:
        return ''
    header = table[0]
    corp_marker = 'דיגאת םש' if reversed_text else 'שם תאגיד'
    name_marker = 'םש' if reversed_text else 'שם'
    role_marker = 'ישאר ךרוע' if reversed_text else 'עורך ראשי'
    corp_idx = name_idx = None
    for i, c in enumerate(header):
        c = (c or '').replace('\n', ' ').strip()
        if corp_marker in c:
            corp_idx = i
        elif c == name_marker:
            name_idx = i
    for row in table[1:]:
        joined = ' '.join((c or '').replace('\n', ' ') for c in row)
        if role_marker not in joined:
            continue
        for idx in (corp_idx, name_idx):
            if idx is not None and idx < len(row) and (row[idx] or '').strip():
                cand = clean_name(row[idx], reversed_text)
                if cand and cand not in CITY_VALUES:
                    return cand
    return ''


def extract_one(pdf_path):
    """Worker: extract developer/architect from one PDF. Runs in subprocess."""
    import pdfplumber
    result = {'developer': '', 'architect': '', 'source': '', 'error': ''}
    try:
        with pdfplumber.open(pdf_path) as pdf:
            target_page = None
            reversed_text = True
            for page in pdf.pages[:20]:
                t = page.extract_text() or ''
                # require the section number too — the phrase מגיש התכנית also
                # appears in free-text explanations on earlier pages
                if '1.8.1' not in t and '1.8.2' not in t:
                    continue
                if 'תינכתה שיגמ' in t or 'םזי' in t:
                    target_page, reversed_text = page, True
                    break
                if 'מגיש התכנית' in t or 'יזם' in t:
                    target_page, reversed_text = page, False
                    break
            if target_page is None:
                # scanned PDF or unusual layout
                any_text = any((p.extract_text() or '').strip() for p in pdf.pages[:3])
                result['error'] = 'no_section' if any_text else 'no_text'
                return result

            yazam_marker = 'םזי' if reversed_text else 'יזם'
            magish_marker = 'תינכתה שיגמ' if reversed_text else 'מגיש התכנית'

            section = None
            yazam_names, magish_names, architect = [], [], ''
            # section 1.8.4 may spill to the next page
            pages = [target_page]
            idx = pdf.pages.index(target_page)
            if idx + 1 < len(pdf.pages):
                pages.append(pdf.pages[idx + 1])
            for pg in pages:
                for tbl in pg.extract_tables():
                    flat = [(c or '').replace('\n', ' ').strip() for row in tbl for c in row]
                    joined = ' | '.join(flat)
                    # title tables: ['םזי', '1.8.2'] etc.
                    if len(tbl) == 1 and len(flat) <= 3:
                        if '1.8.2' in joined or joined.strip(' |') == yazam_marker:
                            section = 'yazam'
                            continue
                        if '1.8.1' in joined or magish_marker in joined:
                            section = 'magish'
                            continue
                        if '1.8.4' in joined:
                            section = 'architect'
                            continue
                        if '1.8.3' in joined or '1.8.5' in joined or re.search(r'1\.9|2\.\d', joined):
                            section = None
                            continue
                    # data tables
                    if section == 'yazam' and not yazam_names:
                        yazam_names = parse_entity_table(tbl, reversed_text)
                        section = None
                    elif section == 'magish' and not magish_names:
                        magish_names = parse_entity_table(tbl, reversed_text)
                        section = None
                    elif section == 'architect' and not architect:
                        architect = parse_architect_table(tbl, reversed_text)
                        section = None
                if yazam_names and architect:
                    break

            # מגיש משותף של רשות מקומית + יזם פרטי: היזם הפרטי הוא היזם בפועל
            def prefer_private(names):
                priv = [n for n in names if not MUNI_RE.search(n)]
                return priv if priv and len(priv) < len(names) else names

            if yazam_names:
                result['developer'] = ' / '.join(prefer_private(yazam_names)[:3])
                result['source'] = 'horaot_1.8.2_yazam'
            elif magish_names:
                result['developer'] = ' / '.join(prefer_private(magish_names)[:3])
                result['source'] = 'horaot_1.8.1_magish'
            result['architect'] = architect

            # "העירייה כמגישה שותפה": מאגר בעלי-העניין (1.8.2 יזם + 1.8.1 מגיש)
            # כולל גם עירייה/ועדה מקומית וגם גורם פרטי. שדה developer מנקה את
            # העירייה (prefer_private), לכן תופסים כאן ומזינים muni_cosubmitter.json.
            pool = []
            for n in (yazam_names + magish_names):
                if n not in pool:
                    pool.append(n)
            muni = [n for n in pool if MUNI_RE.search(n)]
            priv = [n for n in pool if not MUNI_RE.search(n)]
            if muni and priv:
                dev_priv = ([n for n in yazam_names if not MUNI_RE.search(n)]
                            or [n for n in magish_names if not MUNI_RE.search(n)])
                result['muni_co'] = {'muni': muni, 'dev': dev_priv}
            if not result['developer'] and not architect:
                result['error'] = 'section_found_no_names'
    except Exception as e:
        result['error'] = f'{type(e).__name__}: {str(e)[:120]}'
    return result


def load_targets(all_units=False):
    """Default: unit-bearing plans with missing/garbage developer.
    all_units=True: every unit-bearing plan (for cross-checking existing
    values against the statutory horaot document)."""
    with open(PLANS_GEOJSON, encoding='utf-8') as f:
        feats = json.load(f)['features']
    targets = {}
    for ft in feats:
        p = ft['properties']

        def num(v):
            try:
                return float(v or 0)
            except (TypeError, ValueError):
                return 0
        # units_add is sometimes only filled by the GS override layer at app
        # load time — fall back to units_total so those plans aren't skipped
        u = num(p.get('units_add')) or num(p.get('units_total'))
        d = p.get('developer')
        d = d.strip() if isinstance(d, str) else (str(d) if d else '')
        pn = str(p.get('plan_name') or '')
        if not pn:
            continue
        bad_dev = (not d) or value_is_garbage(d)
        bad_arch = value_is_garbage(p.get('architect'))
        if u <= 0:
            # zero-unit plans (public buildings etc.) still show dev/arch in
            # popups — include them when a field holds scrape garbage
            if not (value_is_garbage(d) or bad_arch):
                continue
        elif not (bad_dev or bad_arch or all_units):
            continue
        targets[pn] = {'taba': str(p.get('taba')), 'units': u,
                       'old_developer': d}
    return targets


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    limit = None
    single = None
    all_units = '--all' in sys.argv
    for i, arg in enumerate(sys.argv[1:]):
        if arg == '--plan':
            single = sys.argv[i + 2]
        elif arg.isdigit():
            limit = int(arg)

    if single:
        path = os.path.join(HORAOT_DIR, single + '.pdf')
        result = extract_one(path)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        # --save: merge into the results store (used by the enrichment
        # pipeline; apply_developer_extract.py picks it up from there)
        if '--save' in sys.argv and (result.get('developer') or result.get('architect')):
            store = {}
            if os.path.exists(OUT_FILE):
                with open(OUT_FILE, encoding='utf-8') as f:
                    store = json.load(f)
            result['taba'] = single.split('-')[-1].lstrip('0')
            store[single] = result
            with open(OUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(store, f, ensure_ascii=False, indent=1)
            print(f'saved -> {OUT_FILE}')
        return

    targets = load_targets(all_units=all_units)
    work = [(pn, os.path.join(HORAOT_DIR, pn + '.pdf'))
            for pn in sorted(targets)
            if os.path.exists(os.path.join(HORAOT_DIR, pn + '.pdf'))]
    if limit:
        work = work[:limit]
    print(f'{len(targets)} targets, {len(work)} with local PDF')

    results = {}
    # keep previous partial results (resumable)
    if os.path.exists(OUT_FILE):
        with open(OUT_FILE, encoding='utf-8') as f:
            results = json.load(f)
        RETRY_ERRS = ('timeout', 'section_found_no_names', 'no_section')
        work = [(pn, p) for pn, p in work if pn not in results
                or results[pn].get('error') in RETRY_ERRS
                or results[pn].get('developer') in CITY_VALUES]
        print(f'resuming: {len(work)} remaining')

    ok = 0
    with ProcessPoolExecutor(max_workers=4) as ex:
        futures = {pn: ex.submit(extract_one, path) for pn, path in work}
        for j, (pn, fut) in enumerate(futures.items()):
            try:
                r = fut.result(timeout=120)
            except FutTimeout:
                r = {'developer': '', 'architect': '', 'source': '',
                     'error': 'timeout'}
            except Exception as e:
                r = {'developer': '', 'architect': '', 'source': '',
                     'error': f'pool: {str(e)[:100]}'}
            r.update(targets[pn])
            results[pn] = r
            if r['developer']:
                ok += 1
            status = r['developer'] or r['error']
            print(f"[{j+1}/{len(work)}] {pn}: {status[:70]}")
            if (j + 1) % 10 == 0:
                with open(OUT_FILE, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=1)

    with open(OUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=1)
    got_dev = sum(1 for r in results.values() if r.get('developer'))
    print(f'\nDone. {got_dev}/{len(results)} with developer -> {OUT_FILE}')


if __name__ == '__main__':
    main()
