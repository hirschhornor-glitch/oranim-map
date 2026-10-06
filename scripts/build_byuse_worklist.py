# -*- coding: utf-8 -*-
"""Build the worklist for the per-use parking split.

Only plans whose appendix records BOTH a private-car grand total and a
residential sub-total, and where the two differ, need a per-use reading: when
they are equal the plan has no non-residential parking and its split is already
complete. `target_nonres` is the closure figure every reading must reconcile to.
"""
import json, os, sys

sys.stdout.reconfigure(encoding='utf-8')
# ── Paths ─────────────────────────────────────────────────────
# Two roots on purpose:
#   EVIDENCE — small, irreplaceable provenance kept IN the repo: the hand-read
#              balance tables, the per-use pass, the base corrections and the
#              per-plan document lists. Re-deriving these means re-reading
#              hundreds of sheets by eye, so they are versioned.
#   WORK     — the bulky downloads (pdf/, crops/, zips/ — ~2.5 GB). Regenerable
#              from Mavat with fetch_traffic_appendices.py, so they stay local.
#              Override with the TRAFFIC_WORK_DIR environment variable.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.dirname(_HERE)
EVIDENCE = os.path.join(_REPO, 'data', 'parking_evidence')
DATA = os.path.join(_REPO, 'data')
WORK = os.environ.get('TRAFFIC_WORK_DIR', r'C:\ORANIM\traffic_appendices')
OUT = WORK          # worklist/slices are scratch
P = json.load(open(os.path.join(DATA, 'parking.json'), encoding='utf-8'))['plans']

work, missing = [], []
for t, v in P.items():
    req = v.get('req_private')
    res = v.get('req_residential')
    if not req:
        continue
    nr = (req - res) if res is not None else None
    if nr is not None and nr <= 0:
        continue
    src = (v.get('source') or {}).get('crop') or ''
    p = os.path.join(OUT, src.replace('/', os.sep)) if src else ''
    if not p or not os.path.exists(p):
        missing.append((t, src))
        p = ''
    work.append({'taba': t, 'name': v.get('plan_name', ''), 'crop': p,
                 'req_private': req, 'req_residential': res,
                 'target_nonres': nr, 'doc': (v.get('source') or {}).get('doc', '')})

work.sort(key=lambda w: -(w['target_nonres'] or 0))
json.dump(work, open(os.path.join(OUT, 'byuse_worklist.json'), 'w', encoding='utf-8'),
          ensure_ascii=False, indent=1)
print('worklist:', len(work), ' crops missing on disk:', len(missing))
for t, s in missing[:12]:
    print('   missing', t, s)
