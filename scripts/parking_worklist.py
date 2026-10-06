# -*- coding: utf-8 -*-
"""List what is still waiting to be read, richest plans first."""
import json, os, re, sys
sys.stdout.reconfigure(encoding='utf-8')

OUT = r"C:\ORANIM\traffic_appendices"
DATA = r"C:\ORANIM\oranim-app\data"


def readings():
    got = {}
    rdir = os.path.join(OUT, 'readings')
    if os.path.isdir(rdir):
        for f in sorted(os.listdir(rdir)):
            if f.endswith('.json'):
                got.update(json.load(open(os.path.join(rdir, f), encoding='utf-8')))
    return got


def plan_index():
    pj = json.load(open(os.path.join(DATA, 'plans.geojson'), encoding='utf-8'))
    idx = {}
    for f in pj['features']:
        p = f['properties']
        t = re.sub(r'^101-0*', '', str(p.get('taba') or '')).lstrip('0')
        if t:
            idx[t] = p
    return idx


def num(x):
    try:
        return float(str(x).replace(',', ''))
    except Exception:
        return 0


if __name__ == '__main__':
    q = {}
    for f in sorted(os.listdir(OUT)):
        if f.startswith('queue') and f.endswith('.json'):
            q.update(json.load(open(os.path.join(OUT, f), encoding='utf-8')))
    # crops recovered by the OCR rescue pass count as ready work too
    for f in sorted(os.listdir(OUT)):
        if f.startswith('rescue') and f.endswith('.json') and f != 'rescue_todo.json':
            data = json.load(open(os.path.join(OUT, f), encoding='utf-8'))
            if not isinstance(data, dict):
                continue          # rescue_todo.json is a plain backlog list
            for taba, v in data.items():
                if v.get('status') == 'ready':
                    q[taba] = {'status': 'ready',
                               'docs': [{'items': [i for i in v['items']
                                                   if i.get('kind') == 'crop']}]}
    got, idx = readings(), plan_index()
    rows = []
    for taba, v in q.items():
        if taba in got or v['status'] != 'ready':
            continue
        p = idx.get(taba, {})
        items = [i for d in v['docs'] for i in d.get('items', [])]
        rows.append((num(p.get('units_total')) or num(p.get('units_add')), taba, p, items))
    rows.sort(key=lambda r: -r[0])
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    print('pending: %d plans (read: %d)' % (len(rows), len(got)))
    for units, taba, p, items in rows[:limit]:
        print('%-9s %5d units  %-14s %s' % (taba, units, (p.get('minahak') or '')[:14],
                                            (p.get('plan_summary') or p.get('plan_name_he') or '')[:38]))
        for i in items:
            if i['kind'] == 'crop':
                print('    crop %s  %sx%s %s' % (i['path'], i['px'][0], i['px'][1],
                      ('score=%s' % i['score']) if 'score' in i else 'via=ocr_rescue'))
            else:
                print('    text %s pages=%s' % (i['path'], i.get('pages')))
