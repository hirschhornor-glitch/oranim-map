# -*- coding: utf-8 -*-
"""Consolidate the per-use parking readings into one file the app can consume.

The closure rule: every plan's per-use figures must sum to
`req_private - req_residential`, the non-residential remainder already implied
by the two totals the appendix itself prints. Where they do not, the difference
is NOT silently absorbed into a use — it is carried as `unsplit` (or `over`)
so the category totals and the known grand total can never drift apart.

Why a reading legitimately fails to close, from the re-read:
  * the sheet adds accessible (נכים) spaces ON TOP of its own sub-totals, so
    part of the remainder is not a land use at all;
  * the sheet's own rows do not sum to the total it prints (arithmetic slips
    were found in several);
  * the sheet applies a reduced standard inconsistently between rows and totals.
Each of those is a property of the source, not of the reading, so the honest
resolution is to show the residual rather than to force a split.
"""
import glob, json, os, sys

sys.stdout.reconfigure(encoding='utf-8')
OUT = r'C:\ORANIM\traffic_appendices'
PARK = r'C:\ORANIM\oranim-app\data\parking.json'

BUCKETS = ['מסחר', 'תעסוקה', 'ציבור', 'חינוך', 'מלונאות', 'אחר']
ALIAS = {'משרדים': 'תעסוקה', 'תעסוקה/משרדים': 'תעסוקה', 'מבני ציבור': 'ציבור',
         'מלון': 'מלונאות', 'מסחרי': 'מסחר'}


def load_sources():
    recs = {}
    files = sorted(glob.glob(os.path.join(OUT, 'byuse_out_*.json')))
    files += [os.path.join(OUT, 'byuse_manual_out.json')]
    for f in files:
        if not os.path.exists(f):
            continue
        for t, v in json.load(open(f, encoding='utf-8')).items():
            recs[str(t)] = v           # manual file is last → it wins on overlap
    return recs, files


# NOTE: run merge → build → merge → build when byuse_base_fixes.json changes a
# req_private/req_residential. The closure target is read from parking.json, so the
# first merge still sees the pre-fix totals and the residual comes out wrong by the
# size of the correction. Two passes settle it.
def main():
    park = json.load(open(PARK, encoding='utf-8'))
    plans = park['plans']
    recs, files = load_sources()

    out, stats = {}, {'closed': 0, 'unsplit': 0, 'over': 0, 'unreadable': 0}
    for t, v in recs.items():
        p = plans.get(t)
        if not p:
            continue
        if v.get('readable') is False:
            stats['unreadable'] += 1
            out[t] = {'readable': False, 'note': v.get('note', '')}
            continue
        raw = v.get('by_use') or {}
        bu = {}
        for k, n in raw.items():
            k = ALIAS.get(k.strip(), k.strip())
            if k not in BUCKETS:
                k = 'אחר'
            bu[k] = bu.get(k, 0) + int(n or 0)
        s = sum(bu.values())
        req, res = p.get('req_private'), p.get('req_residential')
        target = (req - res) if (req is not None and res is not None) else None
        rec = {'by_use': bu, 'sum': s}
        if target is not None:
            rec['target'] = target
            if s == target:
                stats['closed'] += 1
            elif s < target:
                rec['unsplit'] = target - s
                stats['unsplit'] += 1
            else:
                rec['over'] = s - target
                stats['over'] += 1
        if v.get('note'):
            rec['note'] = v['note']
        if v.get('confidence'):
            rec['confidence'] = v['confidence']
        if v.get('corrects_base'):
            rec['corrects_base'] = v['corrects_base']
        out[t] = rec

    dest = os.path.join(OUT, 'byuse.json')
    json.dump(out, open(dest, 'w', encoding='utf-8', newline='\n'),
              ensure_ascii=False, indent=1)

    tot = {b: 0 for b in BUCKETS}
    unsplit = 0
    for r in out.values():
        for k, n in (r.get('by_use') or {}).items():
            tot[k] += n
        unsplit += r.get('unsplit', 0)
    print('sources:', len(files), ' plans with a per-use reading:', len(out))
    print('closed exactly: {closed}  under (residual kept): {unsplit}  over: {over}  unreadable: {unreadable}'.format(**stats))
    print('--- spaces by use ---')
    for b in BUCKETS:
        if tot[b]:
            print(f'  {b:<10} {tot[b]:>7,}')
    print(f'  {"לא מפולח":<10} {unsplit:>7,}')
    print('  total attributed:', f'{sum(tot.values()):,}')


if __name__ == '__main__':
    main()
