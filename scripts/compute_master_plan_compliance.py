"""
compute_master_plan_compliance.py
Geographic compliance check: for each plan in plans.geojson, determine which
master plan covers it, what sub-zone, and what conservation conflicts exist.

Output: data/master_plan_compliance.json — keyed by plan_name.
Re-run after master plan layers OR plans.geojson change.
"""
import json, os, sys, io, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from shapely.geometry import shape
from shapely.errors import GEOSException
from shapely.validation import make_valid

DATA = r'C:\ORANIM\oranim-app\data'
PLANS = os.path.join(DATA, 'plans.geojson')
OUT = os.path.join(DATA, 'master_plan_compliance.json')

MASTER_PLANS = {
    'מושבות': 'master_plan_moshavot.geojson',
    'רסקו': 'master_plan_rasko.geojson',
    'בקעה': 'master_plan_baka.geojson',
    'ארנונה': 'master_plan_arnona.geojson',
    'גוננים': 'master_plan_gonenim.geojson',
    'תלפיות': 'master_plan_talpiot.geojson',
}

# Heuristics for "no densification"
NO_DENSIF_KEYWORDS = ['לא לציפוף', 'אזור לא לציפוף']

# Land-use categories for compliance checks (jergisng layer 241 land_use values).
# Used to flag statutory plans that add housing/commerce on incompatible land use.
LANDUSE_NO_HOUSING = [        # housing on these = violation
    'שטח ציבורי פתוח', 'בית עלמין', 'תחנת דלק', 'מתקנים הנדסיים',
    'מסילה', 'שטח נוף פתוח', 'פארק רובעי', 'כיכר עירונית',
]
LANDUSE_ROAD = ['דרך מאושרת', 'דרך מוצעת', 'דרך משולבת', 'שביל', 'שטח לדרך', 'דרך לביטול']
LANDUSE_PUBLIC_RESERVED = ['מבנים ומוסדות ציבור']  # housing here = warning (re-allocation)
LANDUSE_INDUSTRIAL = ['תעסוקה ותעשייה', 'תעסוקה ומסחר', 'תעסוקה']  # housing here = warning (rezone)


def allows_housing(text):
    """Does the master plan let housing stand on this land use?

    categorize_landuse() answers a different question (is this a road, a park,
    a public plot), and most master-plan fabrics are named after the treatment
    rather than the use — "עיבוי מרקם קיים", "אזורי ליבה", "אזור עירוני מעורב ב׳"
    are all residential fabric. Used to tell a plan that puts units where they
    are forbidden from one whose polygon merely reaches into the street.
    """
    t = (text or '').strip()
    if not t:
        return False
    if categorize_landuse(t) in ('residential', 'commercial'):
        return True
    return any(k in t for k in (
        'מגורים', 'מעורב', 'מרקם', 'מרקמי', 'ליבה', 'התחדשות', 'פינוי', 'שכונ', 'שיכונ'))


def categorize_landuse(text):
    """Coarse category for a jergisng land_use value."""
    if not text:
        return None
    t = text.strip()
    if any(k in t for k in LANDUSE_ROAD):
        return 'road'
    if any(k == t or (k in t and 'מגורים' not in t) for k in LANDUSE_NO_HOUSING):
        return 'no_housing'
    if 'מבנים ומוסדות ציבור' in t and 'מגורים' not in t and 'מסחר' not in t:
        return 'public_reserved'
    if any(k in t for k in LANDUSE_INDUSTRIAL) and 'מגורים' not in t:
        return 'industrial'
    if 'מגורים' in t:
        return 'residential'
    if 'מסחר' in t:
        return 'commercial'
    if 'שימור' in t:
        return 'preservation'
    return 'other'


def safe_shape(geom):
    if geom is None:
        return None
    try:
        s = shape(geom)
        if not s.is_valid:
            s = make_valid(s)
        return s
    except (GEOSException, ValueError, TypeError, AttributeError):
        return None


def load_master_plan(path):
    with open(path, encoding='utf-8') as f:
        d = json.load(f)
    boundary = None
    subzones = []
    textures = []
    bldg_heights = []
    cons_points = []
    cons_polys = []
    special_focus = []
    landuses = []
    for feat in d['features']:
        ft = feat['properties'].get('feature_type')
        g = safe_shape(feat['geometry'])
        if g is None:
            continue
        rec = {'g': g, 'p': feat['properties']}
        if ft == 'boundary': boundary = rec
        elif ft == 'subzone': subzones.append(rec)
        elif ft == 'texture': textures.append(rec)
        elif ft == 'building_height': bldg_heights.append(rec)
        elif ft == 'conservation_point': cons_points.append(rec)
        elif ft == 'conservation': cons_polys.append(rec)
        elif ft == 'special_focus': special_focus.append(rec)
        elif ft == 'landuse': landuses.append(rec)
    return {
        'boundary': boundary,
        'subzones': subzones,
        'textures': textures,
        'bldg_heights': bldg_heights,
        'cons_points': cons_points,
        'cons_polys': cons_polys,
        'special_focus': special_focus,
        'landuses': landuses,
    }


def best_overlap(plan_geom, candidates, prop_extractor):
    """Return (best_record, fraction_of_plan_covered) for the candidate with max overlap."""
    if not candidates:
        return None, 0.0
    plan_area = plan_geom.area or 1e-12
    best = None
    best_frac = 0.0
    for rec in candidates:
        try:
            inter = plan_geom.intersection(rec['g'])
        except GEOSException:
            continue
        if inter.is_empty:
            continue
        frac = inter.area / plan_area
        if frac > best_frac:
            best_frac = frac
            best = rec
    return best, best_frac


def grade_for_point(p):
    g = (p.get('conservation_grade') or p.get('דרגת1') or p.get('דרגת_') or '').strip()
    # Normalize: 'מבנה לשימור א' -> 'א'
    if 'א' in g and 'ב' not in g and 'ג' not in g:
        return 'א'
    if 'שימור מלא' in g: return 'א'
    if 'מחמיר' in g: return 'א'
    if 'ב' in g and 'ג' not in g: return 'ב'
    if 'שימור עם תוספות' in g: return 'ב'
    if 'מבנן לשימור' in g: return 'ב'
    if 'ג' in g: return 'ג'
    return ''


def is_tama38(plan_props):
    s = (plan_props.get('plan_type') or '') + ' ' + (plan_props.get('plan_summary') or '')
    return 'תמ"א' in s or 'תמא 38' in s or 'tama' in s.lower()


def main():
    print('Loading master plans...')
    mps = {}
    for name, fn in MASTER_PLANS.items():
        path = os.path.join(DATA, fn)
        if not os.path.exists(path):
            print(f'  ⚠ skip {name}: file missing')
            continue
        mps[name] = load_master_plan(path)
        b = mps[name]['boundary']
        n_sub = len(mps[name]['subzones'])
        n_tex = len(mps[name]['textures'])
        n_h = len(mps[name]['bldg_heights'])
        n_pts = len(mps[name]['cons_points'])
        n_lu = len(mps[name]['landuses'])
        print(f'  {name}: boundary={"✓" if b else "✗"} subzones={n_sub} textures={n_tex} heights={n_h} cons_points={n_pts} landuses={n_lu}')

    print('\nLoading plans.geojson...')
    with open(PLANS, encoding='utf-8') as f:
        plans = json.load(f)
    print(f'  {len(plans["features"])} plans')

    # For each master plan, build a list of master plan polygons for "subzone characterization"
    # Priority order: subzones > textures > bldg_heights
    def height_cap(mp_data, plan_geom):
        """Highest permitted height among the building-height polygons the plan
        covers (Baka's גיליון 7). The max, not the dominant one: a plan usually
        spans several footprints with different caps, and the appendix marks the
        taller category exactly where the taller building is allowed."""
        best, label = None, None
        for rec in mp_data['bldg_heights']:
            n = rec['p'].get('floors_max_int')
            if not n:
                continue
            try:
                if not plan_geom.intersects(rec['g']):
                    continue
            except GEOSException:
                continue
            if best is None or n > best:
                best, label = n, rec['p'].get('floors_per_master_plan')
        return best, label

    def characterize(mp_data, plan_geom):
        """Return dict with subzone/character/max_floors/no_densif from best-overlapping zone."""
        # subzone_source records HOW the master-plan limit was matched, so the
        # floor check can tier severity: a precise polygon (subzone/bldg_height)
        # is a hard cap; the coarse area-wide boundary number is only indicative.
        out = {'subzone_label': None, 'subzone_max_floors': None, 'subzone_far_pct': None,
               'subzone_character': None, 'is_no_densification': False, 'subzone_source': None}
        # Try subzones first (typically rich attributes)
        rec, frac = best_overlap(plan_geom, mp_data['subzones'], None)
        if rec and frac > 0.05:
            p = rec['p']
            out['subzone_label'] = p.get('zone_name') or p.get('zone_code') or p.get('character') or p.get('אזור') or p.get('sog') or ''
            out['subzone_max_floors'] = p.get('max_floors') or p.get('floors_max')
            out['subzone_far_pct'] = p.get('far_pct') or p.get('far_pct_max')
            out['subzone_character'] = p.get('character') or ''
            out['subzone_source'] = 'subzone'
            label_text = ' '.join(str(v) for v in p.values() if isinstance(v, str))
            if any(k in label_text for k in NO_DENSIF_KEYWORDS):
                out['is_no_densification'] = True
            return out
        # Fallback to textures (Baka has these)
        rec, frac = best_overlap(plan_geom, mp_data['textures'], None)
        if rec and frac > 0.05:
            p = rec['p']
            merkam = (p.get('merkam') or p.get('אזור') or '').strip()
            out['subzone_label'] = merkam
            out['subzone_character'] = merkam
            out['subzone_source'] = 'texture'
            if any(k in merkam for k in NO_DENSIF_KEYWORDS):
                out['is_no_densification'] = True
            # Textures (Baka) name the fabric but carry no height — take the cap
            # from the building-height appendix, which does.
            cap, cap_label = height_cap(mp_data, plan_geom)
            if cap:
                out['subzone_max_floors'] = cap
                out['subzone_source'] = 'texture+bldg_height'
                if cap_label:
                    out['subzone_character'] = f'{merkam} · {cap_label}'.strip(' ·')
            return out
        # Fallback to building heights
        cap, cap_label = height_cap(mp_data, plan_geom)
        if cap:
            out['subzone_label'] = cap_label or ''
            out['subzone_max_floors'] = cap
            out['subzone_source'] = 'bldg_height'
            return out
        # Per-zone rights carried on the land-use polygons themselves (Talpiot:
        # the master plan has no subzone polygons, but every land-use polygon
        # carries its תא (א׳-ד׳) rights straight from Table 5 — far more precise
        # than the plan-wide boundary number).
        # Only where the plan has no subzone polygons of its own: there the
        # land-use zone fields come straight from the plan's own table, while a
        # plan WITH subzones already had its say above (its landuse zone fields
        # are only inherited from those same polygons by centroid).
        rec, frac = (best_overlap(plan_geom, mp_data['landuses'], None)
                     if not mp_data['subzones'] else (None, 0))
        if rec and frac > 0.05 and rec['p'].get('zone_max_floors'):
            p = rec['p']
            out['subzone_label'] = p.get('zone_subzone_code') or p.get('land_use') or ''
            out['subzone_max_floors'] = p.get('zone_max_floors')
            out['subzone_far_pct'] = p.get('zone_far_pct') or p.get('zone_far_pct_max')
            out['subzone_character'] = p.get('zone_character') or ''
            # A plan straddling two zones may legitimately place its towers in
            # the taller one, so only a dominant match is a hard cap.
            out['subzone_source'] = 'landuse_zone' if frac > 0.6 else 'landuse_zone_partial'
            return out

        # Last resort: boundary-level rules (Gonenim — no subzones)
        if mp_data['boundary']:
            bp = mp_data['boundary']['p']
            mf = bp.get('max_floors') or bp.get('max_floors_base')
            fp = bp.get('far_pct') or bp.get('far_pct_max')
            if mf or fp:
                out['subzone_label'] = 'כללי תכנית האב'
                out['subzone_max_floors'] = mf
                out['subzone_far_pct'] = fp
                out['subzone_character'] = bp.get('plan_type') or ''
                out['subzone_source'] = 'boundary'
        return out

    def count_conservation(mp_data, plan_geom):
        """Count conservation buildings inside plan geometry, by grade."""
        c = {'א': 0, 'ב': 0, 'ג': 0}
        for rec in mp_data['cons_points']:
            try:
                if plan_geom.contains(rec['g']) or plan_geom.intersects(rec['g']):
                    g = grade_for_point(rec['p'])
                    if g in c:
                        c[g] += 1
            except GEOSException:
                continue
        # Also count from cons_polys (Rasko)
        for rec in mp_data['cons_polys']:
            try:
                if plan_geom.intersects(rec['g']):
                    p = rec['p']
                    if p.get('is_strict'):
                        c['א'] += 1
                    else:
                        c['ב'] += 1
            except GEOSException:
                continue
        return c

    out = {}
    n_match = 0
    n_violations = 0
    n_warnings = 0

    for feat in plans['features']:
        plan_name = feat['properties'].get('plan_name', '')
        plan_geom = safe_shape(feat['geometry'])
        if plan_geom is None or plan_geom.is_empty:
            continue
        try:
            units_add = float(feat['properties'].get('units_add') or 0)
        except (ValueError, TypeError):
            units_add = 0
        try:
            plan_floors_max = float(feat['properties'].get('floors_max') or 0)
        except (ValueError, TypeError):
            plan_floors_max = 0
        try:
            plan_height_max = float(feat['properties'].get('height_max') or 0)
        except (ValueError, TypeError):
            plan_height_max = 0
        is_tama = is_tama38(feat['properties'])

        # Find best matching master plan (by max overlap with boundary)
        best_mp_name = None
        best_overlap_pct = 0
        for mp_name, mp_data in mps.items():
            if not mp_data['boundary']:
                continue
            try:
                inter = plan_geom.intersection(mp_data['boundary']['g'])
                if inter.is_empty:
                    continue
                pct = inter.area / (plan_geom.area or 1e-12)
                if pct > best_overlap_pct:
                    best_overlap_pct = pct
                    best_mp_name = mp_name
            except GEOSException:
                continue

        if best_mp_name is None or best_overlap_pct < 0.05:
            # Plan not significantly inside any master plan
            continue

        n_match += 1
        mp_data = mps[best_mp_name]
        char = characterize(mp_data, plan_geom)
        cons = count_conservation(mp_data, plan_geom)

        # Land-use overlap (jergisng layer 241 polygons within the master plan)
        lu_rec, lu_frac = best_overlap(plan_geom, mp_data['landuses'], None)
        mp_landuse = ''
        mp_landuse_cat = None
        if lu_rec and lu_frac > 0.30:  # require 30% overlap to attribute a single land-use
            mp_landuse = (lu_rec['p'].get('land_use') or '').strip()
            mp_landuse_cat = categorize_landuse(mp_landuse)

        # How much of the plan the master plan designates for housing. A plan
        # usually reaches the middle of the street, so the dominant polygon can
        # be a road strip while the units sit on the residential remainder —
        # that is not a deviation, and only a plan with no housing land at all
        # is really putting units where the master plan forbids them.
        resid_frac = 0.0
        for rec in mp_data['landuses']:
            lu_txt = (rec['p'].get('land_use') or '').strip()
            if not allows_housing(lu_txt):
                continue
            try:
                resid_frac += plan_geom.intersection(rec['g']).area / (plan_geom.area or 1e-12)
            except GEOSException:
                continue

        # Compute status + issues
        issues = []
        status = 'ok'

        if char['is_no_densification'] and units_add > 0:
            issues.append(f'הפרה: התכנית מוסיפה {int(units_add)} יח״ד באזור "לא לציפוף"')
            status = 'violation'

        # Land-use compliance — based on dominant jergisng land_use polygon.
        # Talpiot 2021 explicitly allows industrial→mixed-use conversion (the whole
        # purpose of the master plan), so skip the 'industrial' warning there.
        mp_allows_industrial_to_mixed = (
            best_mp_name == 'תלפיות'
            or (mp_data.get('boundary') and 'תעשייה' in (mp_data['boundary']['p'].get('plan_type') or ''))
        )
        if units_add > 0 and mp_landuse_cat in ('no_housing', 'road'):
            kind = 'דרך/שביל' if mp_landuse_cat == 'road' else 'שטח שאינו מיועד למגורים'
            governs = best_overlap_pct >= 0.5
            if governs and resid_frac < 0.05:
                issues.append(
                    f'הפרה: {int(units_add)} יח״ד על "{mp_landuse}" ({kind} לפי תכנית האב)'
                )
                status = 'violation'
            else:
                why = ('רוב שטח התכנית מחוץ לתכנית האב' if not governs
                       else f'{resid_frac * 100:.0f}% משטח התכנית מיועד למגורים בתכנית האב')
                issues.append(
                    f'אזהרה: הייעוד הדומיננטי בתכנית האב הוא "{mp_landuse}" ({kind}) '
                    f'מול {int(units_add)} יח״ד — {why}'
                )
                if status == 'ok':
                    status = 'warning'
        elif units_add > 0 and mp_landuse_cat == 'public_reserved':
            issues.append(
                f'אזהרה: {int(units_add)} יח״ד על "{mp_landuse}" — מצריך הקצאה מחדש של שטח ציבור'
            )
            if status == 'ok':
                status = 'warning'
        elif units_add > 0 and mp_landuse_cat == 'industrial' and not mp_allows_industrial_to_mixed:
            issues.append(
                f'אזהרה: {int(units_add)} יח״ד על "{mp_landuse}" — שינוי מתעשייה למגורים'
            )
            if status == 'ok':
                status = 'warning'

        # Floors check — plan's Table 5 floors (synced from GS level_num into
        # floors_max) vs the master-plan cap. Severity is tiered by how the cap
        # was matched: a precise sub-zone / building-height polygon is a hard
        # cap (הפרה); the coarse area-wide boundary number ("כללי תכנית האב",
        # e.g. Gonenim/Talpiot, which the master plans note varies by sub-zone)
        # is only indicative (אזהרה).
        try:
            mp_max_floors = float(char.get('subzone_max_floors') or 0)
        except (ValueError, TypeError):
            mp_max_floors = 0
        if plan_floors_max > 0 and mp_max_floors > 0 and plan_floors_max > mp_max_floors:
            precise = char.get('subzone_source') in (
                'subzone', 'bldg_height', 'texture+bldg_height', 'landuse_zone')
            if precise:
                issues.append(
                    f'הפרה: התכנית מציעה {int(plan_floors_max)} קומות — '
                    f'תכנית האב מגבילה ל-{int(mp_max_floors)} קומות'
                )
                status = 'violation'
            else:
                issues.append(
                    f'אזהרה: התכנית מציעה {int(plan_floors_max)} קומות מול '
                    f'~{int(mp_max_floors)} קומות בתכנית האב '
                    + ('(התכנית משתרעת על כמה תאי שטח — ייתכן שתא אחר מתיר יותר)'
                       if char.get('subzone_source') == 'landuse_zone_partial'
                       else '(ערך כללי לכל השטח, ייתכן שמתחם ספציפי מתיר יותר)')
                )
                if status == 'ok':
                    status = 'warning'

        if cons['א'] > 0:
            # Containing a grade-א׳ building is not itself a breach — the plan may
            # well preserve it. What the master plans forbid is demolishing it, so
            # only a plan that is demolition-based by definition is a violation.
            ptxt = ' '.join(str(feat['properties'].get(k) or '') for k in
                            ('plan_type', 'plan_name_he', 'plan_summary'))
            demolition_plan = any(k in ptxt for k in ('התחדשות', 'פינוי', 'הריסה'))
            if demolition_plan:
                issues.append(f'הפרה: התכנית כוללת {cons["א"]} מבני שימור דרגה א\' '
                              f'והיא מבוססת הריסה — הריסתם אסורה')
                status = 'violation'
            else:
                issues.append(f'אזהרה: בגבול התכנית {cons["א"]} מבני שימור דרגה א\' — '
                              f'הריסה אסורה, יש לוודא שהתכנית משמרת אותם')
                if status == 'ok':
                    status = 'warning'

        if cons['ב'] > 0:
            msg = f'אזהרה: {cons["ב"]} מבני שימור דרגה ב\' בתוך התכנית'
            issues.append(msg)
            if status == 'ok':
                status = 'warning'

        if cons['ג'] > 0:
            issues.append(f'מידע: {cons["ג"]} מבנים בדרגה ג\' (רשימה טנטטיבית)')
            if status == 'ok':
                status = 'warning'

        if is_tama and (cons['א'] > 0 or cons['ב'] > 0):
            issues.append('הפרה: תמ"א 38 על מבנה שימור — מותר רק באגף אחורי')
            status = 'violation'

        # A plan the district committee rejected or shelved is not a live
        # deviation — keep the finding, but do not present it as one.
        plan_status_txt = (feat['properties'].get('status_mavat') or '').strip()
        if status == 'violation' and any(k in plan_status_txt for k in ('נדחתה', 'נגנזה', 'בוטלה')):
            status = 'warning'
            issues.insert(0, f'מידע: סטטוס התכנית "{plan_status_txt}" — הסתירה אינה אקטואלית')

        n_violations += 1 if status == 'violation' else 0
        n_warnings += 1 if status == 'warning' else 0

        out[plan_name] = {
            'mp_match': best_mp_name,
            'mp_overlap_pct': round(best_overlap_pct * 100, 1),
            'mp_subzone': char['subzone_label'] or '',
            'mp_subzone_max_floors': char['subzone_max_floors'],
            'mp_subzone_far_pct': char['subzone_far_pct'],
            'mp_subzone_source': char['subzone_source'] or '',
            'mp_subzone_character': char['subzone_character'] or '',
            'mp_is_no_densification': char['is_no_densification'],
            'mp_cons_a': cons['א'],
            'mp_cons_b': cons['ב'],
            'mp_cons_g': cons['ג'],
            'mp_is_tama38': is_tama,
            'mp_landuse': mp_landuse,
            'mp_landuse_overlap_pct': round(lu_frac * 100, 1) if mp_landuse else 0,
            'mp_landuse_category': mp_landuse_cat or '',
            'mp_plan_status': plan_status_txt,
            'mp_status': status,
            'mp_issues': issues,
        }

    print(f'\nResults:')
    print(f'  plans inside a master plan: {n_match} / {len(plans["features"])}')
    print(f'  status=violation: {n_violations}')
    print(f'  status=warning:   {n_warnings}')
    print(f'  status=ok:        {n_match - n_violations - n_warnings}')

    # Save
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, separators=(',', ':'))
    print(f'\nSaved: {OUT} ({os.path.getsize(OUT)/1024:.0f} KB)')

    # Per-master-plan breakdown
    from collections import Counter
    by_mp = Counter(v['mp_match'] for v in out.values())
    print(f'\nBy master plan:')
    for k, v in by_mp.most_common():
        print(f'  {k}: {v} plans')


if __name__ == '__main__':
    main()
