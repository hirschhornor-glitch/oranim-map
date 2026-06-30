"""
Snap mosadot_moch + education_shanaton positions to nearest jergisng building centroid.

Strategy (per feature):
  1. Skip if _manual_position=True (manually corrected)
  2. Skip if position_source already 'jergisng_building_snap' or 'building_addr_exact' or 'jergisng_semel'
  3. Try address match: parse street+num from feature address → find building with same street+num
  4. Fallback: nearest building within MAX_RADIUS metres
  5. If best candidate is farther than MAX_RADIUS → keep current

Run:  py -3 scripts/snap_moch_to_buildings.py [--dry-run]
"""

import json, re, sys, math
from pathlib import Path

DRY_RUN   = '--dry-run' in sys.argv
MAX_RADIUS = 80   # metres — snap only if confident

ROOT = Path(__file__).parent.parent

# ── helpers ─────────────────────────────────────────────────────────────────

def haversine(lon1, lat1, lon2, lat2):
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

def parse_address(addr):
    """Return (street, house_num_str) or (addr, None)."""
    if not addr:
        return None, None
    m = re.match(r'^(.+?)\s+(\d+\w*)$', addr.strip())
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return addr.strip(), None

# ── load buildings index ─────────────────────────────────────────────────────

with open(ROOT / 'data' / 'buildings.geojson', encoding='utf-8') as f:
    buildings_gj = json.load(f)

buildings = []
addr_index = {}   # (street, house_num) → list of building centroids

for feat in buildings_gj['features']:
    p = feat['properties']
    coords = feat['geometry']['coordinates']   # already a centroid Point
    lon, lat = coords[0], coords[1]
    entry = {'lon': lon, 'lat': lat, 'street': p.get('street',''), 'num': str(p.get('house_num',''))}
    buildings.append(entry)
    key = (entry['street'], entry['num'])
    addr_index.setdefault(key, []).append(entry)

print(f"Loaded {len(buildings):,} buildings")

# ── snap function ─────────────────────────────────────────────────────────────

SKIP_SOURCES = {'jergisng_building_snap', 'building_addr_exact', 'jergisng_semel'}

def snap_features(features, label):
    snapped = 0; addr_hit = 0; radius_hit = 0; skipped = 0; no_match = 0

    for feat in features:
        p = feat['properties']
        coords = feat['geometry']['coordinates']
        lon, lat = coords[0], coords[1]

        # skip already precise
        if p.get('_manual_position'):
            skipped += 1; continue
        src = p.get('position_source') or p.get('_position_source') or ''
        if src in SKIP_SOURCES:
            skipped += 1; continue

        # try address match
        raw_addr = p.get('address') or ''
        street, num = parse_address(raw_addr)
        best = None

        if street and num:
            candidates = addr_index.get((street, num), [])
            if candidates:
                # pick closest among addr matches
                best = min(candidates, key=lambda b: haversine(lon, lat, b['lon'], b['lat']))
                dist = haversine(lon, lat, best['lon'], best['lat'])
                if dist > MAX_RADIUS * 2:
                    best = None   # address matched but very far → probably wrong street name variant
                else:
                    addr_hit += 1

        # fallback: nearest within MAX_RADIUS
        if best is None:
            nearest = min(buildings, key=lambda b: haversine(lon, lat, b['lon'], b['lat']))
            dist = haversine(lon, lat, nearest['lon'], nearest['lat'])
            if dist <= MAX_RADIUS:
                best = nearest
                radius_hit += 1
            else:
                no_match += 1

        if best is None:
            continue

        old_coords = [lon, lat]
        new_lon, new_lat = best['lon'], best['lat']
        move_m = haversine(lon, lat, new_lon, new_lat)

        if not DRY_RUN:
            feat['geometry']['coordinates'] = [new_lon, new_lat]
            # store old coords for reference
            if 'position_source' in p:
                p['_prev_position_source'] = p['position_source']
                p['position_source'] = 'jergisng_building_snap'
            elif '_position_source' in p:
                p['_prev_position_source'] = p['_position_source']
                p['_position_source'] = 'jergisng_building_snap'
            else:
                p['position_source'] = 'jergisng_building_snap'
            p['_snapped_from'] = old_coords
            p['_snap_dist_m'] = round(move_m, 1)

        snapped += 1

    print(f"\n{label}:")
    print(f"  snapped={snapped}  addr_match={addr_hit}  radius_match={radius_hit}  no_match={no_match}  skipped={skipped}")
    return snapped

# ── process both files ────────────────────────────────────────────────────────

moch_path    = ROOT / 'data' / 'mosadot_moch.geojson'
shanaton_path = ROOT / 'data' / 'education_shanaton.geojson'

with open(moch_path, encoding='utf-8') as f:
    moch_gj = json.load(f)

with open(shanaton_path, encoding='utf-8') as f:
    shanaton_gj = json.load(f)

snap_features(moch_gj['features'],     'mosadot_moch')
snap_features(shanaton_gj['features'], 'education_shanaton')

if not DRY_RUN:
    with open(moch_path, 'w', encoding='utf-8') as f:
        json.dump(moch_gj, f, ensure_ascii=False, separators=(',', ':'))
    print(f"\nWrote {moch_path.name}")

    with open(shanaton_path, 'w', encoding='utf-8') as f:
        json.dump(shanaton_gj, f, ensure_ascii=False, separators=(',', ':'))
    print(f"Wrote {shanaton_path.name}")
else:
    print("\n[DRY RUN — no files written]")
