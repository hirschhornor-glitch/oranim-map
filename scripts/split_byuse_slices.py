# -*- coding: utf-8 -*-
"""Split the per-use worklist into slices, one per reader.

Plans whose `crop` is empty are composite readings (several crops stitched by
hand) — they are held back for direct reading rather than handed out, because a
reader given no image would have to guess.
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
work = json.load(open(os.path.join(OUT, 'byuse_worklist.json'), encoding='utf-8'))

auto = [w for w in work if w['crop']]
manual = [w for w in work if not w['crop']]
N = 6
slices = [auto[i::N] for i in range(N)]          # round-robin keeps big plans spread
for i, s in enumerate(slices):
    json.dump(s, open(os.path.join(OUT, f'byuse_slice_{i}.json'), 'w', encoding='utf-8'),
              ensure_ascii=False, indent=1)
    print(f'slice {i}: {len(s)} plans, {sum(w["target_nonres"] or 0 for w in s)} spaces')
json.dump(manual, open(os.path.join(OUT, 'byuse_manual.json'), 'w', encoding='utf-8'),
          ensure_ascii=False, indent=1)
print('held back for direct reading:', len(manual), [w['taba'] for w in manual])
