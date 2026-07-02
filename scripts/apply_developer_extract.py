"""
apply_developer_extract.py
Writes developer/architect extracted from horaot PDFs
(_developer_extract_results.json) into Google Sheets + plans.geojson.

Policy for the developer field:
  - fill when current value is empty
  - overwrite when current value is a known scraper artifact
    (כתובת / זיהוי התכנית / סמכות: / מס' יח"ד / ".4..." / numeric / >=60 chars)
  - overwrite a legit-looking current value ONLY when the extraction came from
    the statutory יזם section (1.8.2) and the two names genuinely differ —
    the old YK scrape often stored a contact person instead of the company.
  - 1.8.1 (מגיש) mismatches are NOT auto-applied (the submitter is often the
    municipality while GS holds the actual developer) — they are written to
    _developer_mismatches_review.json for manual review.
Architect: fill/garbage-overwrite only.

Usage:
    py apply_developer_extract.py --dry     # show what would change
    py apply_developer_extract.py           # apply to GS + geojson, commit+push
"""
import json
import re
import sys
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

from git_sync import pull_before_read, commit_and_push_after_write

CREDS_FILE    = r"C:\ORANIM\oranim-490018-ceaf784afe61.json"
SHEET_ID      = "1_AcuuA1CNPh6jXc_lZKNghfpEF1aDPV8Zci8QPz2WVE"
PLANS_GEOJSON = r"C:\ORANIM\oranim-app\data\plans.geojson"
RESULTS_FILE  = r"C:\ORANIM\_developer_extract_results.json"

GARBAGE = {'כתובת', 'זיהוי התכנית', 'סמכות:', 'מס\' יח"ד',
           # generic/junk architect values from the old scrape
           'ניסיון', 'אדריכלים ומתכנני ערים', 'אדריכלים', 'פרטי'}


def is_garbage(v):
    v = (v or '').strip()
    if not v:
        return False
    return (v in GARBAGE or v.startswith('.4') or len(v) >= 60
            or v.isdigit())


def should_write(current):
    cur = (current or '').strip() if isinstance(current, str) else str(current or '')
    return (not cur) or is_garbage(cur)


def _load_alias_rev():
    try:
        with open(r'C:\ORANIM\oranim-app\data\developer_aliases.json', encoding='utf-8') as f:
            aliases = json.load(f).get('aliases', {})
    except OSError:
        return {}
    rev = {}
    for canon, lst in aliases.items():
        rev[_norm(canon)] = canon
        for a in (lst or []):
            rev[_norm(a)] = canon
    return rev


def _norm(s):
    s = re.sub(r'["\'״׳.,()\-–]', ' ', s or '')
    s = re.sub(r'\bבע\s*מ\b', '', s)
    s = s.replace('יי', 'י')  # collapse full-spelling double yod (בניין=בנין)
    s = re.sub(r'\b\d{4}\b', '', s)  # ignore year tokens like (1965)
    return re.sub(r'\s+', ' ', s).strip()


_ALIAS_REV = None


def names_match(old, new):
    """True when old and new plausibly refer to the same entity:
    alias-canonical equality, containment, or >=2 shared words
    against any partner in `new`."""
    global _ALIAS_REV
    if _ALIAS_REV is None:
        _ALIAS_REV = _load_alias_rev()
    no = _norm(old)
    if not no:
        return False
    co = _ALIAS_REV.get(no, no)
    for part in (new or '').split(' / '):
        p = _norm(part)
        if not p:
            continue
        if _ALIAS_REV.get(p, p) == co:
            return True
        if no in p or p in no:
            return True
        if len(set(no.split()) & set(p.split())) >= min(2, len(no.split())):
            return True
    return False


def dev_action(current, r):
    """Returns 'write' | 'review' | None for the developer field."""
    cur = (current or '').strip() if isinstance(current, str) else str(current or '')
    new = r.get('developer') or ''
    if not new:
        return None
    if (not cur) or is_garbage(cur):
        return 'write'
    if names_match(cur, new):
        return None  # same entity, keep existing form (aliases handle display)
    if r.get('source') == 'horaot_1.8.2_yazam':
        return 'write'  # statutory יזם section beats old scrape
    return 'review'


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    dry = '--dry' in sys.argv

    with open(RESULTS_FILE, encoding='utf-8') as f:
        results = json.load(f)
    # keep only successful extractions
    good = {pn: r for pn, r in results.items() if r.get('developer') or r.get('architect')}
    print(f"extraction results: {len(results)} total, {len(good)} with data")

    # --- Google Sheets ---
    creds = Credentials.from_service_account_file(CREDS_FILE,
        scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
    sheet = gspread.authorize(creds).open_by_key(SHEET_ID).sheet1
    all_data = sheet.get_all_values()
    headers = all_data[0]
    h = {hdr.strip().lower(): i for i, hdr in enumerate(headers)}
    for col in ('plan_name', 'developer', 'architect', 'last_modified'):
        if col not in h:
            print(f"FATAL: column '{col}' not found in GS headers")
            return

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    batch = []
    gs_updated = 0
    review = {}
    for row_num, row in enumerate(all_data[1:], start=2):
        pn = row[h['plan_name']].strip() if h['plan_name'] < len(row) else ''
        if pn not in good:
            continue
        r = good[pn]
        cur_dev = row[h['developer']] if h['developer'] < len(row) else ''
        cur_arch = row[h['architect']] if h['architect'] < len(row) else ''
        changed = False
        act = dev_action(cur_dev, r)
        if act == 'write':
            batch.append({'range': gspread.utils.rowcol_to_a1(row_num, h['developer'] + 1),
                          'values': [[r['developer']]]})
            changed = True
        elif act == 'review':
            review[pn] = {'taba': r.get('taba'), 'units': r.get('units'),
                          'current': (cur_dev or '').strip(),
                          'horaot': r['developer'], 'source': r.get('source')}
        if r.get('architect') and should_write(cur_arch):
            batch.append({'range': gspread.utils.rowcol_to_a1(row_num, h['architect'] + 1),
                          'values': [[r['architect']]]})
            changed = True
        if changed:
            batch.append({'range': gspread.utils.rowcol_to_a1(row_num, h['last_modified'] + 1),
                          'values': [[now_str]]})
            gs_updated += 1
            if dry:
                print(f"  GS row {row_num} {pn}: dev '{(cur_dev or '')[:25]}' -> '{r.get('developer','')[:40]}'"
                      f" | arch '{(cur_arch or '')[:20]}' -> '{r.get('architect','')[:30]}'")

    print(f"GS rows to update: {gs_updated} ({len(batch)} cells) | review-only mismatches: {len(review)}")
    with open(r'C:\ORANIM\_developer_mismatches_review.json', 'w', encoding='utf-8') as f:
        json.dump(review, f, ensure_ascii=False, indent=1)
    if not dry and batch:
        for i in range(0, len(batch), 50):
            sheet.spreadsheet.values_batch_update({'valueInputOption': 'RAW', 'data': batch[i:i + 50]})
        print("GS updated.")

    # --- plans.geojson ---
    if not dry:
        if not pull_before_read():
            print("ABORTING geojson write: could not pull latest plans.geojson.")
            return
    with open(PLANS_GEOJSON, encoding='utf-8') as f:
        geojson = json.load(f)

    geo_updated = 0
    for feat in geojson['features']:
        props = feat['properties']
        pn = str(props.get('plan_name') or '').strip()
        if pn not in good:
            continue
        r = good[pn]
        changed = False
        if dev_action(props.get('developer'), r) == 'write':
            props['developer'] = r['developer']
            changed = True
        if r.get('architect') and should_write(props.get('architect')):
            props['architect'] = r['architect']
            changed = True
        if changed:
            props['last_modified'] = now_str
            geo_updated += 1

    print(f"geojson features to update: {geo_updated}")
    if not dry and geo_updated:
        with open(PLANS_GEOJSON, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, ensure_ascii=False)
        commit_and_push_after_write(
            'data/plans.geojson',
            f'data: developer/architect from horaot section 1.8 for {geo_updated} plans')
        print("geojson written + pushed.")


if __name__ == '__main__':
    main()
