# -*- coding: utf-8 -*-
"""
Re-applies manual curation to a freshly-rebuilt education_shanaton.geojson.

Why: the rebuild groups institutions by address and geocodes via MOE/jergisng.
That loses (a) the splits we made (pulling one institution out of a shared
address into its own marker), (b) manual point placements the user supplied
from the map, (c) type re-classifications, (d) address corrections, and
(e) the building-level positioning we derived.

This module is idempotent and OFFLINE. Re-derivable positioning (snap to the
building at the address, else nearest house-number on the street, else the
jergisng institution coordinate) is RECOMPUTED here, so only genuinely-manual
decisions live in data/education_overrides.json.

Pipeline order:  splits -> type/address fixes -> reposition -> frozen overrides
(frozen manual/OSM points always win).

Usage:
    from apply_education_overrides import apply
    apply()                                  # rewrites data/education_shanaton.geojson in place
    # or
    python scripts/apply_education_overrides.py [in.geojson] [out.geojson]
"""
import json, re, math, os, sys
from collections import Counter

DATA = os.path.join(os.path.dirname(__file__), '..', 'data')
GEO  = os.path.join(DATA, 'education_shanaton.geojson')
OVR  = os.path.join(DATA, 'education_overrides.json')
BLD  = os.path.join(DATA, 'buildings.geojson')
JRG  = os.path.join(DATA, 'jergisng_mosadot_chinuch.json')

def _norm(s):
    if not s: return ''
    s = str(s)
    s = re.sub(r'\([^)]*\)', ' ', s)
    s = re.sub(r'["\'\(\)\[\]׳״.]', '', s)
    s = re.sub(r'^(רחוב|רח\'?|שדרות|שד\'?|דרך|סמטת|סמ\'?|מבוא|כיכר|ככר|שכ\'?|פרופ\'?|השופט|פרופסור)\s+', '', s)
    s = s.replace('־', ' ').replace('-', ' ').replace("'", '')
    return re.sub(r'\s+', ' ', s).strip()

def _split_addr(a):
    a = (a or '').strip()
    m = re.search(r'(\d+)\s*$', a)
    return (_norm(a[:m.start()]) if m else _norm(a)), (m.group(1) if m else None)

def _dm(a, b):
    return math.hypot((a[0]-b[0])*math.cos(math.radians(31.75))*111320, (a[1]-b[1])*111320)

def _load_buildings():
    bd = json.load(open(BLD, encoding='utf-8'))
    by_sn, by_street = {}, {}
    for f in bd['features']:
        p = f['properties']; g = f['geometry']['coordinates']
        st = _norm(p.get('street')); raw = p.get('house_num'); hn = ni = None
        if raw not in (None, ''):
            mm = re.match(r'(\d+)', str(raw))
            if mm: hn = mm.group(1); ni = int(hn)
        by_sn.setdefault((st, hn), []).append(g)
        if ni is not None:
            by_street.setdefault(st, []).append((ni, g))
    return by_sn, by_street

def _load_jergisng():
    jg = json.load(open(JRG, encoding='utf-8'))
    out = {}
    for r in jg:
        a = r['attributes']; g = r.get('geometry') or {}
        s = a.get('semel_misrad_hinuch')
        if s and g.get('x'): out[int(s)] = (g['x'], g['y'])
    return out

GUARD = 40  # only nearest-house-number snap a point currently farther than this

def _reposition(feat, by_sn, by_street, jerg):
    """Deterministic, offline building-level positioning. Returns True if moved."""
    p = feat['properties']; ep = feat['geometry']['coordinates']
    st, num = _split_addr(p.get('address'))
    # 1) exact street + house number
    pts = by_sn.get((st, num)) if num is not None else None
    if pts:
        t = min(pts, key=lambda b: _dm(ep, b))
        feat['geometry']['coordinates'] = [round(t[0], 6), round(t[1], 6)]
        p['_position_source'] = 'building_addr_exact'; return True
    # 2) nearest house number on the same street (only if currently off)
    if st in by_street and num and num.isdigit():
        tn = int(num)
        c = min(by_street[st], key=lambda x: (abs(x[0]-tn), _dm(ep, x[1])))
        if _dm(ep, c[1]) > GUARD:
            feat['geometry']['coordinates'] = [round(c[1][0], 6), round(c[1][1], 6)]
            p['_position_source'] = 'building_street_nearestnum'; return True
    # 3) jergisng institution coordinate (by MOE semel)
    for inst in p['institutions']:
        s = inst.get('semel_chinuch')
        if s and int(s) in jerg:
            t = jerg[int(s)]
            feat['geometry']['coordinates'] = [round(t[0], 6), round(t[1], 6)]
            p['_position_source'] = 'jergisng_semel'; return True
    return False

def apply(in_path=GEO, out_path=GEO):
    gj  = json.load(open(in_path, encoding='utf-8'))
    ovr = json.load(open(OVR, encoding='utf-8'))
    by_sn, by_street = _load_buildings()
    jerg = _load_jergisng()

    split_set   = set(ovr.get('splits_semel', []))
    pos_semel   = {int(k): v for k, v in ovr.get('position_by_semel', {}).items()}
    pos_name    = ovr.get('position_by_name', {})
    pos_addr    = ovr.get('position_by_address', {})
    type_semel  = {int(k): v for k, v in ovr.get('type_by_semel', {}).items()}
    type_name   = ovr.get('type_by_name', {})
    addr_semel  = {int(k): v for k, v in ovr.get('address_by_semel', {}).items()}
    stud_semel  = {int(k): v for k, v in ovr.get('students_by_semel', {}).items()}
    stud_name   = ovr.get('students_by_name', {})
    additions   = ovr.get('add_institutions', [])

    feats = gj['features']

    # --- (1) carve split institutions into their own features ---
    new_feats = []
    for f in feats:
        p = f['properties']; insts = p['institutions']
        keep, carved = [], []
        for inst in insts:
            s = inst.get('semel_chinuch')
            (carved if (s and int(s) in split_set) else keep).append(inst)
        if carved and keep:
            # shrink the group
            p['institutions'] = keep
            p['institutions_count'] = len(keep)
            p['total_students'] = sum((i.get('students') or 0) for i in keep)
            p['primary_type'] = Counter(i['type'] for i in keep).most_common(1)[0][0]
            for inst in carved:
                new_feats.append({'type': 'Feature',
                    'geometry': {'type': 'Point', 'coordinates': list(f['geometry']['coordinates'])},
                    'properties': {'address': p['address'], 'institutions_count': 1,
                        'total_students': inst.get('students') or 0,
                        'primary_type': inst['type'], 'institutions': [inst],
                        '_split_from': p['address']}})
        # if carved and not keep -> the group WAS only this institution; leave as-is
    feats.extend(new_feats)

    # --- (1b) address corrections by semel. AFTER splits, so a carved
    #     institution's new address never renames the group it came from.
    #     Rename only when every institution in the feature agrees on the
    #     same override address (single-inst features trivially qualify). ---
    for f in feats:
        p = f['properties']
        targets = set()
        for inst in p['institutions']:
            s = inst.get('semel_chinuch')
            if not (s and int(s) in addr_semel):
                targets = set(); break
            targets.add(addr_semel[int(s)])
        if len(targets) == 1 and p.get('address') != next(iter(targets)):
            p['_addr_renamed_from'] = p.get('address')
            p['address'] = next(iter(targets))

    # --- (1c) institutions absent from the shanaton sources (e.g. Mitzpen
    #     2026-07). Merge into the feature at the same normalized address,
    #     else create a new feature at the entry's coords (jergisng point);
    #     step (3) then snaps new features to the building when possible.
    #     Idempotent: skipped when the same semel+name is already present. ---
    by_addr = {}
    for f in feats:
        by_addr.setdefault(_norm(f['properties'].get('address', '')), f)
    added = 0
    for add in additions:
        inst = dict(add['institution'])
        f = by_addr.get(_norm(add['address']))
        if f:
            p = f['properties']
            if any(i.get('semel_chinuch') == inst.get('semel_chinuch')
                   and i.get('name') == inst.get('name') for i in p['institutions']):
                continue
            p['institutions'].append(inst)
            p['institutions_count'] = len(p['institutions'])
            p['primary_type'] = Counter(i['type'] for i in p['institutions']).most_common(1)[0][0]
        else:
            nf = {'type': 'Feature',
                  'geometry': {'type': 'Point', 'coordinates': list(add['coords'])},
                  'properties': {'address': add['address'], 'institutions_count': 1,
                                 'total_students': inst.get('students') or 0,
                                 'primary_type': inst['type'], 'institutions': [inst],
                                 '_added_from_override': True}}
            feats.append(nf)
            by_addr[_norm(add['address'])] = nf
        added += 1

    # --- (2) type + student-count overrides by semel/name ---
    # (name-keyed overrides win over semel-keyed — duplicate-semel cases)
    for f in feats:
        for inst in f['properties']['institutions']:
            s = inst.get('semel_chinuch')
            nm = inst.get('name')
            if nm in type_name:
                inst['type'] = type_name[nm]
            elif s and int(s) in type_semel:
                inst['type'] = type_semel[int(s)]
            if nm in stud_name:
                inst['students'] = stud_name[nm]
            elif s and int(s) in stud_semel:
                inst['students'] = stud_semel[int(s)]
        if f['properties']['institutions_count'] == 1:
            f['properties']['primary_type'] = f['properties']['institutions'][0]['type']
        f['properties']['total_students'] = sum((i.get('students') or 0) for i in f['properties']['institutions'])

    # --- (3) deterministic building-level repositioning ---
    for f in feats:
        _reposition(f, by_sn, by_street, jerg)

    # --- (4) frozen manual / OSM points win over everything ---
    frozen = 0
    for f in feats:
        p = f['properties']; insts = p['institutions']
        done = False
        if p['institutions_count'] == 1:
            nm = insts[0].get('name', ''); s = insts[0].get('semel_chinuch')
            if s and int(s) in pos_semel:
                f['geometry']['coordinates'] = list(pos_semel[int(s)]); done = True
            elif nm in pos_name:
                f['geometry']['coordinates'] = list(pos_name[nm]); done = True
        if not done and p.get('address') in pos_addr:
            f['geometry']['coordinates'] = list(pos_addr[p['address']]); done = True
        if done:
            p['_manual_position'] = True; p['_position_source'] = 'override'; frozen += 1

    json.dump(gj, open(out_path, 'w', encoding='utf-8'), ensure_ascii=False)
    print('apply_education_overrides: features=%d splits_carved=%d added=%d frozen=%d'
          % (len(feats), len(new_feats), added, frozen))
    return gj

if __name__ == '__main__':
    a = sys.argv[1] if len(sys.argv) > 1 else GEO
    b = sys.argv[2] if len(sys.argv) > 2 else a
    apply(a, b)
