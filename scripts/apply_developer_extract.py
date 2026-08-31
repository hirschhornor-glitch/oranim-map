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
import os
import re
import sys
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

from git_sync import (REPO_DIR, pull_before_read, commit_and_push_after_write,
                      update_json_and_push)

MUNI_CO_JSON = "data/muni_cosubmitter.json"  # relative to the repo (oranim-app)

CREDS_FILE    = r"C:\ORANIM\oranim-490018-ceaf784afe61.json"
SHEET_ID      = "1_AcuuA1CNPh6jXc_lZKNghfpEF1aDPV8Zci8QPz2WVE"
PLANS_GEOJSON = r"C:\ORANIM\oranim-app\data\plans.geojson"
RESULTS_FILE  = r"C:\ORANIM\_developer_extract_results.json"

GARBAGE = {'כתובת', 'זיהוי התכנית', 'סמכות:', 'מס\' יח"ד',
           # generic/junk architect values from the old scrape
           'ניסיון', 'אדריכלים ומתכנני ערים', 'אדריכלים', 'פרטי'}
# ערך שנראה כמו כתובת ("בלפור 16 , טלביה") — הסקרייפר הישן כתב כתובות בשדות
ADDR_RE = re.compile(r'\d+\s*,\s*\S|^(רחוב|שד|דרך|ככר)\b')


def is_garbage(v):
    v = (v or '').strip()
    if not v:
        return False
    if v in GARBAGE or v.startswith('.4') or v.isdigit():
        return True
    if ADDR_RE.search(v):
        return True
    # length per JV partner — a 3-way partnership is legit at 70+ chars
    return any(len(part.strip()) >= 60 for part in v.split(' / '))


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
        # same entity — but if the statutory doc lists MORE partners than the
        # current single value (e.g. ש.ב.ח -> ש.ב.ח / רמי לוי), upgrade to the
        # fuller JV list so co-developers aren't lost.
        if (r.get('source') == 'horaot_1.8.2_yazam'
                and len(new.split(' / ')) > len(cur.split(' / '))):
            return 'write'
        return None  # keep existing form (aliases handle display)
    if r.get('source') == 'horaot_1.8.2_yazam':
        return 'write'  # statutory יזם section beats old scrape
    return 'review'


def _dedup(seq):
    out = []
    for x in seq:
        x = (x or '').strip()
        if x and x not in out:
            out.append(x)
    return out


REVIEW_FILE = r'C:\ORANIM\_developer_mismatches_review.json'


def _merge_review(new_items):
    """Keep already-dispositioned rows; only surface genuinely new mismatches.

    A row that carries a 'verdict' has been decided by a human pass. It must
    survive, and the same plan must not be re-raised — otherwise every run
    undoes the review.
    """
    try:
        with open(REVIEW_FILE, encoding='utf-8') as f:
            old = json.load(f)
    except (OSError, ValueError):
        old = {}
    merged = dict(old)
    resurfaced, added = 0, 0
    for pn, row in new_items.items():
        prev = old.get(pn)
        if prev and prev.get('verdict'):
            # Already decided. Re-raise ONLY if the הוראות now says something
            # different from what was reviewed — that is new information.
            if str(prev.get('horaot') or '') == str(row.get('horaot') or ''):
                continue
            row = dict(row)
            row['superseded_verdict'] = prev.get('verdict')
            resurfaced += 1
        elif pn not in old:
            added += 1
        merged[pn] = row
    kept = sum(1 for r in merged.values() if r.get('verdict'))
    print(f"review file: {added} new, {resurfaced} re-raised (הוראות changed), "
          f"{kept} previously decided kept")
    return merged


def backfill_developer_from_muni_cosubmitter(sheet, h, geojson, dry):
    """Fill an EMPTY developer from muni_cosubmitter.json's private `dev`.

    muni_cosubmitter.json records {muni, dev} where `dev` IS the private
    developer, but nothing ever fed it back into the developer column — rows
    added by the 1.8.1 scan and the OCR backfill (src='scan'/'ocr') never came
    through this script's extraction at all. 101-0512301 sat with its answer
    in the side file for a month because of it.

    Fill-only: an existing developer value is never touched.
    """
    path = os.path.join(REPO_DIR, MUNI_CO_JSON.replace('/', os.sep))
    try:
        with open(path, encoding='utf-8') as f:
            rows = json.load(f)
    except (OSError, ValueError):
        print("muni-cosubmitter backfill: side file unreadable — skipped")
        return

    want = {}
    for pn, row in rows.items():
        devs = [d for d in (row.get('dev') or []) if str(d).strip()]
        if devs:
            want[pn] = ' / '.join(devs)
    if not want:
        return

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    batch, filled = [], []
    for row_num, row in enumerate(sheet.get_all_values()[1:], start=2):
        pn = row[h['plan_name']].strip() if h['plan_name'] < len(row) else ''
        if pn not in want:
            continue
        cur = row[h['developer']] if h['developer'] < len(row) else ''
        if str(cur or '').strip() and not is_garbage(cur):
            continue
        batch.append({'range': gspread.utils.rowcol_to_a1(row_num, h['developer'] + 1),
                      'values': [[want[pn]]]})
        batch.append({'range': gspread.utils.rowcol_to_a1(row_num, h['last_modified'] + 1),
                      'values': [[now_str]]})
        filled.append(pn)

    print(f"muni-cosubmitter backfill: {len(filled)} empty developer(s) to fill")
    for pn in filled:
        print(f"  {pn} -> {want[pn][:70]}")
    if dry or not filled:
        return

    for i in range(0, len(batch), 50):
        sheet.spreadsheet.values_batch_update(
            {'valueInputOption': 'RAW', 'data': batch[i:i + 50]})
    for feat in geojson.get('features', []):
        props = feat.get('properties') or {}
        pn = str(props.get('plan_name') or '').strip()
        if pn in filled and not str(props.get('developer') or '').strip():
            props['developer'] = want[pn]
            props['last_modified'] = now_str
    with open(PLANS_GEOJSON, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False)
    commit_and_push_after_write(
        'data/plans.geojson',
        f'data: developer backfilled from muni_cosubmitter for {len(filled)} plans')
    print("muni-cosubmitter backfill written + pushed.")


def apply_muni_cosubmitter(results, dry):
    """Fold "העירייה כמגישה שותפה" (result['muni_co'] from section 1.8) into
    data/muni_cosubmitter.json. Upsert-only, keyed by plan_name; never deletes
    existing rows (the 1.8.1-table + OCR backfill stays intact). src='horaot'
    marks pipeline-derived rows. Concurrency-safe via update_json_and_push."""
    upserts = {}
    for pn, r in results.items():
        mc = r.get('muni_co')
        if not mc:
            continue
        muni = _dedup(mc.get('muni') or [])
        dev = _dedup(mc.get('dev') or [])
        if not muni or not dev:
            continue
        row = {'muni': muni, 'dev': dev, 'src': 'horaot'}
        u = r.get('units')
        if u:
            try:
                row['units'] = int(float(u))
            except (TypeError, ValueError):
                pass
        upserts[pn] = row
    print(f"muni-cosubmitter rows from horaot 1.8: {len(upserts)}")
    if not upserts:
        return
    if dry:
        for pn, row in list(upserts.items())[:20]:
            print(f"  muni_co {pn}: {row['muni']} + {row['dev']}")
        return

    def edit(data):
        changed = False
        for pn, row in upserts.items():
            if data.get(pn) != row:
                data[pn] = row
                changed = True
        return changed

    ok = update_json_and_push(
        MUNI_CO_JSON, edit,
        f'data: muni-cosubmitter from horaot 1.8 for {len(upserts)} plans')
    print(f"muni_cosubmitter.json: {'pushed' if ok else 'FAILED'}")


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
    # MERGE, don't overwrite. Items already dispositioned carry a 'verdict'
    # (gs_correct / applied_company / applied_horaot / fixed) written during a
    # manual review pass; blowing the file away would resurrect every one of
    # them as "to review" on the next pipeline run and lose the reasoning.
    review = _merge_review(review)
    with open(REVIEW_FILE, 'w', encoding='utf-8') as f:
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

    # "העירייה כמגישה שותפה" — full 1.8 info (יזם + אדריכל כבר נכתבו למעלה; כאן
    # המגיש-העירוני) → data/muni_cosubmitter.json. רץ תמיד (גם כשאין שינוי GS).
    apply_muni_cosubmitter(results, dry)

    # …and feed the side file BACK: rows whose private `dev` is known while the
    # developer column is still empty. Runs after the upsert so rows added in
    # this very run are included, and covers src='scan'/'ocr' rows that never
    # pass through this script's extraction at all.
    backfill_developer_from_muni_cosubmitter(sheet, h, geojson, dry)


if __name__ == '__main__':
    main()
