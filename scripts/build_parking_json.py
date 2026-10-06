# -*- coding: utf-8 -*-
"""
build_parking_json.py
---------------------
Fold the parking readings taken off the traffic appendices into app data.

Writes oranim-app/data/parking.json — one record per plan, with the raw counts
as read plus the derived ratios — and prints the area-level roll-up.

A note on the denominator: the traffic appendix always sizes parking against
the TOTAL proposed dwellings, not the net addition, so units_total is the right
denominator. Where the appendix states its own unit count we keep both and flag
any gap, because a stale appendix (drawn for an earlier version of the plan) is
the main way this data goes wrong.
"""
import glob, json, os, re, sys
sys.stdout.reconfigure(encoding='utf-8')

OUT = r"C:\ORANIM\traffic_appendices"
DATA = r"C:\ORANIM\oranim-app\data"
DEST = os.path.join(DATA, 'parking.json')


def norm(t):
    return re.sub(r'^101-0*', '', str(t or '')).lstrip('0')


def num(x):
    try:
        return float(str(x).replace(',', ''))
    except Exception:
        return 0.0


def load_readings():
    r = {}
    for f in sorted(glob.glob(os.path.join(OUT, 'readings', '*.json'))):
        r.update(json.load(open(f, encoding='utf-8')))
    return r


def load_plans():
    pj = json.load(open(os.path.join(DATA, 'plans.geojson'), encoding='utf-8'))
    return {norm(f['properties'].get('taba')): f['properties'] for f in pj['features']}


def build():
    readings, plans = load_readings(), load_plans()
    idx_status = {}
    for f in glob.glob(os.path.join(OUT, 'doc_index*.json')):
        idx_status.update(json.load(open(f, encoding='utf-8')))

    # Per-use split of the non-residential remainder, produced by a second pass
    # over the balance tables (merge_byuse.py). Absent for plans whose remainder
    # is zero — those are already fully split by definition.
    byuse_path = os.path.join(OUT, 'byuse.json')
    byuse = json.load(open(byuse_path, encoding='utf-8')) if os.path.exists(byuse_path) else {}

    # Corrections to the base figures that the per-use re-read turned up. Kept in
    # their own file rather than edited into the batch readings, so the original
    # reading and the reason it was overridden both stay on the record.
    fixes_path = os.path.join(OUT, 'byuse_base_fixes.json')
    fixes = json.load(open(fixes_path, encoding='utf-8')) if os.path.exists(fixes_path) else {}
    base_fix, base_review = fixes.get('apply', {}), fixes.get('review', {})

    out = {}
    # A plan whose appendix we fetched and read, but which carries no parking
    # balance table at all. Without this bucket such a plan would vanish: it is
    # no longer 'no_traffic_doc' (the document exists) and it has no numbers to
    # put in `plans` — so it would silently disappear from every count.
    no_balance = {}
    for taba, v in readings.items():
        if v.get('coverage') == 'no_balance':
            no_balance[taba] = {k: v.get(k) for k in ('plan_name', 'notes', 'source')
                                if v.get(k) is not None}
            continue
        fx = base_fix.get(taba)
        if fx:
            v = dict(v)
            for k in ('req_private', 'prov_private', 'req_residential',
                      'prov_residential', 'units_in_table'):
                if k in fx:
                    v[k] = fx[k]
        p = plans.get(taba, {})
        u_gs = num(p.get('units_total')) or num(p.get('units_add'))
        u_tab = v.get('units_in_table')
        u_tab = None if u_tab in (None, '') else num(u_tab)
        # Use the appendix's own unit count whenever it states one: the parking
        # figures were sized against THOSE units, so the ratio stays internally
        # consistent. A wide gap from GS is flagged, not silently reconciled —
        # it usually means the appendix predates the plan's current version.
        if u_tab:
            units, basis = u_tab, 'appendix'
            gap = round(u_tab - u_gs) if u_gs else None
        else:
            units, basis, gap = u_gs, 'gs', None

        rec = {k: v.get(k) for k in
               ('plan_name', 'coverage', 'req_private', 'req_private_full_standard', 'prov_private',
                'req_residential', 'prov_residential', 'guests', 'bikes_req', 'bikes_prov',
                'moto_req', 'moto_prov', 'operational', 'accessible', 'standard',
                'confidence', 'notes', 'source') if v.get(k) is not None}
        rec['units'] = units
        rec['units_basis'] = basis
        if gap:
            rec['units_gap_vs_gs'] = gap
        # The appendix's residential figure should never exceed its own grand total.
        # Where it does, the two numbers were read off different columns (one the
        # requirement, the other what is provided) — flag it instead of letting the
        # derived "non-residential" quietly go negative in every aggregate.
        if v.get('req_private') and v.get('req_residential') and                 num(v['req_residential']) > num(v['req_private']):
            rec['resid_exceeds_total'] = True
        if units:
            for key, src in (('ratio_req', 'req_private'), ('ratio_prov', 'prov_private'),
                             ('ratio_res_req', 'req_residential'),
                             ('ratio_res_prov', 'prov_residential')):
                if v.get(src):
                    rec[key] = round(num(v[src]) / units, 3)
        bu = byuse.get(taba)
        if bu and bu.get('by_use'):
            rec['by_use'] = bu['by_use']
            # the part of the non-residential remainder the sheet does not break
            # down by use — kept visible so category totals never silently drift
            if bu.get('unsplit'):
                rec['by_use_unsplit'] = bu['unsplit']
            if bu.get('over'):
                rec['by_use_over'] = bu['over']
        if fx:
            rec['base_corrected'] = fx.get('why', '')
        if taba in base_review:
            rec['base_uncertain'] = base_review[taba].get('why', '')
        rec['minahak'] = p.get('minahak') or ''
        rec['sub_neighborhood'] = p.get('sub_neighborhood') or ''
        rec['status'] = p.get('status_mavat') or ''
        out[taba] = rec

    # plans we checked and proved have no traffic appendix at all
    no_doc = sorted(t for t, v in idx_status.items()
                    if v.get('status') == 'no_traffic_doc'
                    and t not in out and t not in no_balance)
    # plans where the appendix and GS disagree by more than 15% on unit count —
    # worth a human look before the ratio is quoted
    mismatch = sorted((t for t, r in out.items()
                       if r.get('units_gap_vs_gs') and r.get('units')
                       and abs(r['units_gap_vs_gs']) / max(r['units'], 1) > 0.15),
                      key=lambda t: -abs(out[t]['units_gap_vs_gs']))
    payload = {'plans': out, 'no_traffic_appendix': no_doc,
               'appendix_no_balance': no_balance,
               'unit_count_mismatch': mismatch,
               'counts': {'read': len(out), 'no_appendix': len(no_doc),
                          'no_balance': len(no_balance),
                          'unit_mismatch': len(mismatch)}}
    # newline='' keeps LF on Windows — the repo stores LF and a CRLF rewrite
    # would show the whole file as changed in every diff
    with open(DEST, 'w', encoding='utf-8', newline=chr(10)) as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=1)
    return payload, plans


# Mirrors SUB_NORMALIZE in app.jsx — the app's single source for merging duplicate
# sub-neighborhood spellings. Kept in sync by hand; grouping on the raw value split
# א.ת. תלפיות into two rows that are one area on the ground.
SUB_NORMALIZE = {
    'תלפיות - תעשייה ומסחר': 'א.ת. תלפיות',
    'תלפיות תעשייה ומסחר': 'א.ת. תלפיות',
    'תלפיות תעשיה ומסחר': 'א.ת. תלפיות',
    'קטמונים ח-ט': 'קטמונים',
    'רסקו - גבעת הורדים': 'רסקו',
    'גוננים א-ו': 'גוננים',
    'ארנונה': 'תלפיות ארנונה',
    'תלפיות': 'תלפיות ארנונה',
    'עמק רפאים - המושבה הגרמנית': 'המושבה הגרמנית',
    'קוממיות - טלביה': 'טלביה',
    'קריית שמואל': 'קרית שמואל',
}


def rollup(payload, plans, key='minahak'):
    """Area roll-up. Ratios are unit-weighted (total spaces / total units), not a
    mean of per-plan ratios — a 20-unit plan must not swing the area average."""
    agg = {}
    for taba, r in payload['plans'].items():
        # a partial reading (only some of the plan's plots were on the sheet)
        # would drag the area ratio down; keep the record, drop it from the mean
        if not r.get('units') or r.get('coverage') == 'partial':
            continue
        k = r.get(key) or '(ללא)'
        if key == 'sub_neighborhood':
            k = SUB_NORMALIZE.get(k, k)
        a = agg.setdefault(k, {'plans': 0, 'units': 0, 'req': 0, 'prov': 0,
                               'res_req': 0, 'units_req': 0, 'units_prov': 0,
                               'units_res': 0})
        a['plans'] += 1
        a['units'] += r['units']
        if r.get('req_private'):
            a['req'] += r['req_private']
            a['units_req'] += r['units']
        if r.get('prov_private'):
            a['prov'] += r['prov_private']
            a['units_prov'] += r['units']
        if r.get('req_residential'):
            a['res_req'] += r['req_residential']
            a['units_res'] += r['units']
    return agg


if __name__ == '__main__':
    payload, plans = build()
    print('parking.json: %d plans read, %d proven without an appendix'
          % (payload['counts']['read'], payload['counts']['no_appendix']))
    for key in ('minahak', 'sub_neighborhood'):
        print('\n=== %s' % key)
        print('%-22s %6s %7s %9s %9s %9s' %
              ('', 'תכניות', 'יח"ד', 'נדרש/יח"ד', 'מוצע/יח"ד', 'מגורים/יח"ד'))
        agg = rollup(payload, plans, key)
        for k, a in sorted(agg.items(), key=lambda kv: -kv[1]['units']):
            f = lambda n, d: ('%.2f' % (n / d)) if d else '-'
            print('%-22s %6d %7d %9s %9s %9s'
                  % (k[:22], a['plans'], a['units'], f(a['req'], a['units_req']),
                     f(a['prov'], a['units_prov']), f(a['res_req'], a['units_res'])))
