# -*- coding: utf-8 -*-
"""Split the per-use worklist into slices, one per reader.

Plans whose `crop` is empty are composite readings (several crops stitched by
hand) — they are held back for direct reading rather than handed out, because a
reader given no image would have to guess.
"""
import json, os, sys

sys.stdout.reconfigure(encoding='utf-8')
OUT = r'C:\ORANIM\traffic_appendices'
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
