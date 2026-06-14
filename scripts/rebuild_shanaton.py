import pandas as pd
import json, re, sys
sys.stdout.reconfigure(encoding='utf-8')

df = pd.read_html(r'C:\Users\orlev\Downloads\Data.xls', encoding='utf-8')[0]
dfB = pd.read_html(r'C:\Users\orlev\Downloads\גני ילדים 1.xls', encoding='utf-8')[0]
df['source'] = 'general'; dfB['source'] = 'kindergartens'
df = pd.concat([df, dfB], ignore_index=True).drop(columns=['Unnamed: 0'])
df['סמל חינוך'] = df['סמל חינוך'].astype('Int64')

with open(r'C:\ORANIM\oranim-app\data\moe_coordinates_cache.json', encoding='utf-8') as f:
    moe = json.load(f)
moe_by_semel = {}
for r in moe:
    s = r.get('SEMEL_MOSAD')
    if s:
        try: moe_by_semel[int(s)] = r
        except: pass

with open(r'C:\ORANIM\oranim-app\data\jergisng_mosadot_chinuch.json', encoding='utf-8') as f:
    jerg = json.load(f)
jerg_by_semel = {}
for f in jerg:
    a = f['attributes']; g = f.get('geometry', {})
    if not g.get('x'): continue
    s = a.get('semel_misrad_hinuch')
    if s: jerg_by_semel[int(s)] = (a, g)

def normalize_address(addr):
    if not isinstance(addr, str): return None
    a = re.sub(r'^ר"?ח\s+', '', addr.strip())
    a = re.sub(r'\s*מיקוד\s+\d+\s*$', '', a)
    a = re.sub(r'\s+', ' ', a).strip()
    a = a.replace('"', '').replace("'", '').replace('.', '').strip()
    return a

def institution_type(name, students):
    n = name if isinstance(name, str) else ''
    s = float(students) if pd.notna(students) else None
    # Special-ed name keywords (highest priority)
    if 'תקשורת' in n or 'ליקויי' in n or 'לקויי' in n or 'אוטיז' in n: return 'גן חינוך מיוחד'
    # USER RULE: name has 'גן'/'גני' as a standalone word → גן ילדים (overrides student count >35)
    # But still גן חינוך מיוחד if very small (≤10 students)
    if re.search(r'(?:^|[\s,\-.\(\)"\'])(גן|גני)(?:$|[\s,\-.\(\)"\'])', n):
        if s is not None and s <= 10: return 'גן חינוך מיוחד'
        return 'גן'
    if 'מעון' in n: return 'מעון'
    if 'אולפנ' in n: return 'אולפנה'
    if 'מדרשי' in n: return 'מדרשייה'
    if 'תיכון' in n or 'מקיף' in n or 'אורט' in n: return 'תיכון'
    if 'חטיב' in n: return 'חטיבה'
    if 'ישיב' in n or 'אהל שמעון' in n: return 'ישיבה'
    if 'ת"ת' in n or 'תלמוד תורה' in n: return 'תלמוד תורה'
    if 'יסוד' in n or n.startswith('מ"מ ') or 'ממ"ד' in n or 'בית חינוך' in n: return 'יסודי'
    if s is not None:
        if s <= 10: return 'גן חינוך מיוחד'
        if s <= 35: return 'גן'
        if s >= 200: return 'יסודי/תיכון'
        return 'יסודי'
    if 'חובה' in n: return 'גן'
    return 'אחר'

records = []
for _, r in df.iterrows():
    semel = int(r['סמל חינוך']) if pd.notna(r['סמל חינוך']) else None
    rec = {
        'name': r['שם מוסד'],
        'address_full': r['כתובת מוסד'],
        'semel_chinuch': semel,
        'students': int(r["סה''כ תלמידים"]) if pd.notna(r["סה''כ תלמידים"]) else None,
        'source': r['source'],
        'lat': None, 'lng': None,
        'precision_source': None,
    }
    if semel and semel in moe_by_semel:
        m = moe_by_semel[semel]
        try:
            rec['lng'] = float(m['UTM_X']); rec['lat'] = float(m['UTM_Y'])
            rec['precision_source'] = f'moe-{m.get("RAMAT_DIYUK_MIKUM", "?")}'
        except: pass
    if rec['lat'] is None and semel and semel in jerg_by_semel:
        a, g = jerg_by_semel[semel]
        rec['lat'] = g['y']; rec['lng'] = g['x']
        rec['precision_source'] = 'jergisng'
    if semel and semel in jerg_by_semel:
        a, _ = jerg_by_semel[semel]
        rec['pikuach'] = a.get('migzar')
        rec['shuna'] = a.get('shuna')
        rec['maamad'] = a.get('maamad')
        rec['herkev_minim'] = a.get('herkevMinim')
    rec['type'] = institution_type(rec['name'], rec['students'])
    rec['addr_norm'] = normalize_address(rec['address_full'])
    records.append(rec)

dfr = pd.DataFrame(records)
print(f'With coords: {dfr["lat"].notna().sum()}/{len(dfr)}')
print('Source:')
print(dfr['precision_source'].value_counts(dropna=False))

for addr in dfr[dfr['lat'].isna()]['addr_norm'].dropna().unique():
    m = re.match(r'^(.+?)\s+\d+\s*$', addr)
    if not m: continue
    street = m.group(1).strip()
    siblings = dfr[(dfr['addr_norm'].str.startswith(street + ' ', na=False)) & dfr['lat'].notna()]
    if len(siblings):
        for i in dfr[dfr['addr_norm'] == addr].index:
            dfr.at[i, 'lat'] = siblings['lat'].mean()
            dfr.at[i, 'lng'] = siblings['lng'].mean()
            dfr.at[i, 'precision_source'] = 'sibling-street'

print(f'After sibling: {dfr["lat"].notna().sum()}/{len(dfr)}')
dfr.to_csv(r'C:\Users\orlev\Downloads\education_shanaton_v3.csv', index=False, encoding='utf-8-sig')

minahaks =['minahak_baka', 'minahak_beit_tzfafa', 'minahak_ganot', 'minahak_gonen', 'minahak_malha', 'minahak_talpiot']
all_geoms = []
for mh in minahaks:
    with open(rf'C:\ORANIM\oranim-app\data\{mh}.geojson', encoding='utf-8') as f:
        d = json.load(f)
    for ft in d.get('features', []):
        if ft.get('geometry'): all_geoms.append(ft['geometry'])

def point_in_ring(lng, lat, ring):
    inside = False; j = len(ring) - 1
    for i in range(len(ring)):
        xi, yi = ring[i][0], ring[i][1]; xj, yj = ring[j][0], ring[j][1]
        if ((yi > lat) != (yj > lat)) and (lng < (xj - xi) * (lat - yi) / (yj - yi + 1e-15) + xi):
            inside = not inside
        j = i
    return inside

def point_in_geom(lng, lat, g):
    if g['type'] == 'MultiPolygon':
        for poly in g['coordinates']:
            if point_in_ring(lng, lat, poly[0]):
                if not any(point_in_ring(lng, lat, h) for h in poly[1:]): return True
    elif g['type'] == 'Polygon':
        if point_in_ring(lng, lat, g['coordinates'][0]):
            if not any(point_in_ring(lng, lat, h) for h in g['coordinates'][1:]): return True
    return False

with open(r'C:\ORANIM\oranim-app\data\mosadot_shchuna.geojson', encoding='utf-8') as f:
    msh = json.load(f)
mosadot_by_semel = {}
for ft in msh['features']:
    s = ft['properties'].get('סמל חינוך')
    if s:
        try: mosadot_by_semel[int(float(s))] = ft['properties']
        except: pass

dfr = dfr[dfr['lat'].notna()].copy()
dfr['in_rova'] = dfr.apply(lambda r: any(point_in_geom(r['lng'], r['lat'], g) for g in all_geoms), axis=1)
dfr = dfr[dfr['in_rova']].copy()

features = []
for addr, group in dfr.groupby('addr_norm'):
    lat = group['lat'].mean(); lng = group['lng'].mean()
    institutions = []
    for _, r in group.iterrows():
        semel = int(r['semel_chinuch']) if pd.notna(r['semel_chinuch']) else None
        inst = {
            'name': r['name'],
            'semel_chinuch': semel,
            'students': int(r['students']) if pd.notna(r['students']) else None,
            'type': r['type'],
        }
        if pd.notna(r.get('pikuach')): inst['pikuach'] = r['pikuach']
        if pd.notna(r.get('shuna')): inst['shuna'] = str(r['shuna']).replace(')', '').replace('(', '')
        if pd.notna(r.get('maamad')): inst['maamad'] = r['maamad']
        if pd.notna(r.get('herkev_minim')): inst['herkev_minim'] = r['herkev_minim']
        if semel and semel in mosadot_by_semel:
            mp = mosadot_by_semel[semel]
            for src, dest in [('כיתות רגילות','kitot_regilot'),('כיתות מקדמות','kitot_mekadmot')]:
                v = mp.get(src)
                try:
                    if v and v != 'None': inst[dest] = int(round(float(v)))
                except: pass
        institutions.append(inst)
    type_counts = {}
    for inst in institutions:
        type_counts[inst['type']] = type_counts.get(inst['type'], 0) + 1
    primary_type = max(type_counts, key=type_counts.get)
    features.append({
        'type': 'Feature',
        'properties': {
            'address': addr,
            'institutions_count': len(institutions),
            'total_students': int(group["students"].fillna(0).sum()),
            'primary_type': primary_type,
            'institutions': institutions,
        },
        'geometry': {'type': 'Point', 'coordinates': [round(lng, 6), round(lat, 6)]},
    })

with open(r'C:\ORANIM\oranim-app\data\education_shanaton.geojson', 'w', encoding='utf-8') as f:
    json.dump({'type': 'FeatureCollection', 'features': features}, f, ensure_ascii=False)

print(f'Features: {len(features)}')
print(f'Institutions: {sum(f["properties"]["institutions_count"] for f in features)}')
print(f'Students: {sum(f["properties"]["total_students"] for f in features)}')

# --- Re-apply manual curation so a full rebuild never loses hand-placed institutions ---
# Splits, manual map points, type/address fixes, and building-level positioning are
# reconstructed from data/education_overrides.json. See scripts/apply_education_overrides.py.
import importlib.util as _ilu, os as _os
_spec = _ilu.spec_from_file_location(
    'apply_education_overrides',
    _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), 'apply_education_overrides.py'))
_mod = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
_mod.apply()
print('Manual overrides re-applied (see data/education_overrides.json).')
