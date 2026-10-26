"""
geocode_committee_permits.py
----------------------------
Permits surfaced in the Jerusalem licensing sub-committee (ועדת המשנה לרישוי) are
identified by a permit number (e.g. "1981/0501.05"), not a תב"ע — so they have no
polygon in plans.geojson. In the "תכניות בדיון בוועדות הקרובות" report + layer this
means they are neither clickable nor placed on the map, and the "מינה"ק" column
shows the licensing-committee sub-neighborhood (טלביה, רחביה…) instead of the real
community-administration (מינה"ק).

This script geocodes each committee permit's address (reusing the Nominatim helper
from geocode_unassigned_permits.py), then resolves its real מינה"ק by point-in-polygon
against the minahak_*.geojson boundaries. Results are written to a cache keyed by the
raw permit_no so update_meetings_layer.py can attach lat/lng + minahak to meetings.json.

Output: C:\\ORANIM\\committee_geocode_cache.json
  { "<permit_no_raw>": {lat, lng, minahak, sub_neighborhood, address, geocode_type} }

Reruns are cheap: a permit whose address is unchanged in the cache is not re-geocoded.
"""
import io
import json
import os
import re
import sys
import time

import requests
from shapely.geometry import shape, Point

# Reuse the Nominatim endpoint/UA from the sibling geocoder — importing is safe:
# geocode_unassigned_permits.py guards its work behind if __name__ == "__main__".
from geocode_unassigned_permits import NOM, UA

ROOT = r"C:\ORANIM"
SRC = os.path.join(ROOT, "jerusalem_committee_matches.json")
DATA = os.path.join(ROOT, "oranim-app", "data")
SUBN = os.path.join(DATA, "sub_neighborhoods.geojson")
CACHE = os.path.join(ROOT, "committee_geocode_cache.json")

# Authoritative geocoding: the city's own point-per-permit-file layer, keyed by
# tik_num in the exact same format as our permit_no (e.g. "1981/0501.05"). This
# is exact — unlike the Nominatim address fallback, which mis-hits or fails
# entirely on the "dirty" committee address strings. fetch_permit_gis_points.py
# pre-materialises the in-area subset to permit_gis_points.json {tik:[lng,lat]};
# for permits missing from that file we query the live layer directly (this
# script runs locally, where the gisviewer WAF allows the request).
POINTS = os.path.join(DATA, "permit_gis_points.json")
LAYER_2110 = ("https://gisviewer.jerusalem.muni.il/arcgis/rest/services/"
              "BaseLayers/MapServer/2110/query")

# מינה"ק boundary files -> the label the app expects (matches LAYER_CONFIG minahak
# labels in app.jsx, verified against each file's shem_1 property). These official
# boundaries do NOT overlap, so matching is unbuffered + smallest-area-wins; a small
# buffer is only a fallback for points that geocode just outside every boundary.
# (minahak_gonenim_union.geojson is a rough union — omitted so it can't swallow area.)
MINHAK_FILES = {
    "minahak_ganot.geojson":       "גינות העיר",
    "minahak_gonen.geojson":       "גוננים",
    "minahak_baka.geojson":        "בקעה רבתי",
    "minahak_malha.geojson":       "מינהל מוסדי מלחה",
    "minahak_talpiot.geojson":     "א.ת. תלפיות",
    "minahak_beit_tzfafa.geojson": "בית צפאפא",
}
BUF = 0.001  # ~100m fallback buffer for points just outside every boundary
# A muni tik point that is outside every boundary but this close to one is NOT
# admitted (the Gila case was 74m out) — it is only tagged edge_minahak so the
# email lists it for a manual look. Boundaries often run along a street, and
# 2025/0182.00 (אל חארה אל שרקייה 17) sat 11m outside בקעה רבתי.
EDGE_M = 30
# Oranim area bbox (SW Jerusalem) — reject geocode hits that land outside it.
BBOX = (31.72, 31.82, 35.17, 35.25)  # (lat_min, lat_max, lon_min, lon_max)


def load_json(path):
    with io.open(path, encoding="utf-8") as f:
        return json.load(f)


def load_minhak_polys():
    """[(minahak_label, shapely_polygon, area), ...] sorted so smaller-area wins ties."""
    out = []
    for fname, label in MINHAK_FILES.items():
        path = os.path.join(DATA, fname)
        if not os.path.exists(path):
            print(f"  WARN: missing {fname}")
            continue
        gj = load_json(path)
        for feat in gj.get("features", []):
            if not feat.get("geometry"):
                continue
            g = shape(feat["geometry"]).buffer(0)
            out.append((label, g, g.area))
    return out


def pip_minhak(lat, lng, minhak_polys, allow_buffer=True):
    """Real מינה"ק for a point: unbuffered smallest-area containment first, then a small
    buffer for points that land just outside every boundary. '' if still none.

    allow_buffer=False for authoritative municipal points. The ~100m fallback buffer
    exists to absorb Nominatim's fuzziness — an exact tik point that sits outside every
    boundary is genuinely outside. With the buffer on, permit 2026/0099.00 (שבתאי הנגבי
    39, גילה) landed 74m south of בית צפאפא and was reported as ours. גילה is not ours.
    """
    pt = Point(lng, lat)
    bufs = (0, BUF) if allow_buffer else (0,)
    for buf in bufs:
        best, best_area = "", float("inf")
        for label, g, area in minhak_polys:
            gg = g if buf == 0 else g.buffer(buf)
            if area < best_area and gg.contains(pt):
                best, best_area = label, area
        if best:
            return best
    return ""


def load_subn_polys():
    """[(schn_nama, polygon)] — the sub-neighborhood a point actually sits in."""
    if not os.path.exists(SUBN):
        return []
    out = []
    for feat in load_json(SUBN).get("features", []):
        name = (feat.get("properties", {}).get("schn_nama") or "").strip()
        if name and feat.get("geometry"):
            out.append((name, shape(feat["geometry"]).buffer(0)))
    return out


def pip_subn(lat, lng, subn_polys):
    pt = Point(lng, lat)
    return next((n for n, g in subn_polys if g.contains(pt)), "")


def nearest_minhak(lat, lng, minhak_polys):
    """(label, metres) of the closest boundary — for edge flagging only."""
    pt = Point(lng, lat)
    best = min(((lab, g.distance(pt)) for lab, g, _ in minhak_polys),
               key=lambda x: x[1], default=("", float("inf")))
    return best[0], best[1] * 100000


def build_subn_fallback(minhak_polys):
    """
    Sub-neighborhood -> מינה"ק map, derived from the same geometry (not guessed): each
    sub_neighborhoods.geojson polygon's representative point is resolved via pip_minhak.
    Keyed by schn_nama. Used when a permit fails to geocode.
    """
    fallback = {}
    if not os.path.exists(SUBN):
        return fallback
    for feat in load_json(SUBN).get("features", []):
        name = (feat.get("properties", {}).get("schn_nama") or "").strip()
        geom = feat.get("geometry")
        if not name or not geom:
            continue
        pt = shape(geom).representative_point()
        label = pip_minhak(pt.y, pt.x, minhak_polys)
        if label:
            fallback[name] = label
    return fallback


def _norm_name(s):
    """Normalize a neighborhood name for exact comparison."""
    s = re.sub(r'["\'״’]', '', str(s or ''))
    s = re.sub(r'\s+', ' ', s).strip(' ,.-/')
    return " ".join(w[1:] if w.startswith("ה") and len(w) > 2 else w for w in s.split())


def _name_parts(s):
    """Split a compound neighborhood label into its individual names."""
    return [p for p in (_norm_name(x) for x in re.split(r'[,/–—-]', s or '')) if p]


def resolve_minhak_by_name(neighborhood, subn_fallback):
    """Map a permit's neighborhood string to a מינה"ק via schn_nama.

    EXACT match on whole names only — never substring. Jerusalem neighbourhood
    names nest, and the old two-way `n in schn or schn in n` produced live
    false positives that put other people's permits in our layer:
      'תלפיות מזרח' (ארמון הנציב) -> 'בקעה רבתי'   via the key 'תלפיות'
      'צפת'                        -> 'גוננים'      via the 2-letter key 'פת'
      'רפת'                        -> 'בית צפאפא'   via 'בית צפאפא,שרפת'
    Compound keys ('בית צפאפא,שרפת', 'רסקו - גבעת הורדים') are split, so each
    real name in them still resolves on its own.
    """
    n = (neighborhood or "").strip()
    if not n:
        return ""
    if n in subn_fallback:
        return subn_fallback[n]
    index = {}
    for schn, label in subn_fallback.items():
        index.setdefault(_norm_name(schn), label)
        for part in _name_parts(schn):
            index.setdefault(part, label)
    for part in _name_parts(n) or [_norm_name(n)]:
        if part in index:
            return index[part]
    return ""


def address_candidates(address, hood):
    """
    Committee addresses look like "<applicant name...> <street> <number> , <neighborhood>",
    with the house number usually jammed onto the last street word (e.g. "יהודה הנשיא11").
    Build ordered Nominatim queries: strip the neighborhood tail, peel the trailing number,
    and try the last 1/2/3 words as the street (closest-to-number first, walking back past
    the applicant name). House number 0 = unknown → street-only.
    """
    a = address.split(" , ")[0].split(",")[0]
    a = a.replace('"', " ").replace("'", " ")
    a = re.sub(r"\s+", " ", a).strip()
    toks = a.split()
    num, idx = "", None
    for i in range(len(toks) - 1, -1, -1):
        m = re.search(r"(\d+)$", toks[i])
        if m:
            num = m.group(1)
            toks[i] = toks[i][:m.start()]
            idx = i
            break
    if idx is None:
        return []
    toks = [t for t in toks if t]
    if not toks:
        return []
    housenum = "" if num in ("0", "") else num
    qs = []
    for k in (1, 2, 3):
        street = " ".join(toks[-k:])
        if housenum:
            qs.append(f"{street} {housenum}, {hood}, ירושלים")
            qs.append(f"{street} {housenum}, ירושלים")
        else:
            qs.append(f"{street}, {hood}, ירושלים")
    seen, out = set(), []
    for q in qs:
        if q not in seen:
            seen.add(q)
            out.append(q)
    return out


def committee_geocode(address, hood):
    """Return (lat, lon, gtype) for the first candidate that resolves inside the Oranim
    bbox, else None. Rate-limited to respect Nominatim's usage policy."""
    for q in address_candidates(address, hood):
        try:
            r = requests.get(NOM, params={"format": "json", "q": q, "limit": 1,
                             "countrycodes": "il", "accept-language": "he"},
                             headers={"user-agent": UA}, timeout=20)
            j = r.json()
            if j:
                lat, lon = float(j[0]["lat"]), float(j[0]["lon"])
                if BBOX[0] <= lat <= BBOX[1] and BBOX[2] <= lon <= BBOX[3]:
                    return lat, lon, j[0].get("type", "")
        except Exception:
            pass
        time.sleep(1.1)
    return None


# --- Authoritative muni GIS points (layer 2110), keyed by canonical tik --------
_TIK_RE = re.compile(r"(\d{4})\s*/\s*(\d+)(?:\s*\.\s*(\d+))?")


def canon_tik(raw):
    """Normalise any permit_no / tik_num spelling to the layer-2110 form
    'YYYY/NNNN.VV' (4-digit file, 2-digit version). Handles both our raw
    '2015/0858.04' and the normalised '2015/858.3'. None if unparseable."""
    if not raw:
        return None
    m = _TIK_RE.search(str(raw))
    if not m:
        return None
    y, num, ver = m.group(1), m.group(2), (m.group(3) or "0")
    return f"{y}/{int(num):04d}.{int(ver):02d}"


def load_muni_points():
    """(idx, base) from permit_gis_points.json. idx: canon_tik -> (lng, lat).
    base: 'YYYY/NNNN' -> {ver: (lng, lat)} for same-parcel version fallback."""
    idx, base = {}, {}
    if not os.path.exists(POINTS):
        return idx, base
    for k, v in (load_json(POINTS).get("points") or {}).items():
        c = canon_tik(k)
        if not c or not isinstance(v, (list, tuple)) or len(v) < 2:
            continue
        idx[c] = (v[0], v[1])
        b, ver = c.split(".")
        base.setdefault(b, {})[ver] = (v[0], v[1])
    return idx, base


def point_from_index(pno, idx, base, exact_only=False):
    """Authoritative (lng, lat) for a permit_no from the pre-fetched index:
    exact version first, else (unless exact_only) another version of the same file.

    The other-version fallback is NOT the same parcel: 2024/0390.00/.01 sit in Baka
    and 2024/0390.02 in Romema (5.3 km apart, layer 2110, 2026-10-12). It is used only
    after the live exact lookup failed, and tagged muni_gis_sibling so it is retried."""
    c = canon_tik(pno)
    if not c:
        return None
    if c in idx:
        return idx[c]
    if exact_only:
        return None
    b, ver = c.split(".")
    variants = base.get(b)
    if variants:
        return variants[max(variants)]
    return None


def live_query_points(canons):
    """Query layer 2110 live for canonical tiks missing from the fetched file.
    Returns ({canon_tik: (lng, lat)} exact, {canon_tik: (lng, lat)} other-version).
    Robust: a WAF 403 / network error just yields no hit (-> Nominatim)."""
    out, sib = {}, {}
    bases = sorted({c.split(".")[0] for c in canons})
    CHUNK = 40  # keep the OR-of-LIKE where-clause (and URL) bounded
    for i in range(0, len(bases), CHUNK):
        grp = bases[i:i + CHUNK]
        where = " OR ".join(f"tik_num LIKE '{b}.%'" for b in grp)
        try:
            r = requests.get(LAYER_2110, params={
                "f": "json", "where": where, "outFields": "tik_num",
                "returnGeometry": "true", "outSR": "4326"},
                headers={"user-agent": UA}, timeout=40)
            feats = r.json().get("features", [])
        except Exception as e:
            print(f"  live 2110 query failed ({e}) — falling back to Nominatim")
            continue
        by_base = {}
        for f in feats:
            tik = canon_tik((f.get("attributes") or {}).get("tik_num"))
            g = f.get("geometry") or {}
            if tik and "x" in g:
                out[tik] = (g["x"], g["y"])
                b, ver = tik.split(".")
                by_base.setdefault(b, {})[ver] = (g["x"], g["y"])
        for c in canons:               # same-file fallback when exact ver absent
            if c in out:
                continue
            b, ver = c.split(".")
            if b in by_base:
                sib[c] = by_base[b][max(by_base[b])]
    return out, sib


def resolve_muni_points(pnos, idx, base):
    """permit_no -> (lng, lat) using the fetched file first, then one batched
    live layer-2110 query for whatever is missing."""
    muni, sibling, miss = {}, set(), {}
    for p in pnos:
        pt = point_from_index(p, idx, base, exact_only=True)
        if pt:
            muni[p] = pt
        else:
            c = canon_tik(p)
            if c:
                miss.setdefault(c, []).append(p)
    file_hits = len(muni)
    if miss:
        live, live_sib = live_query_points(sorted(miss))
        for c, ps in miss.items():
            for p in ps:
                pt = live.get(c)
                if pt is None:                      # no exact anywhere → other version
                    pt = point_from_index(p, idx, base) or live_sib.get(c)
                    if pt is not None:
                        sibling.add(p)
                if pt is not None:
                    muni[p] = pt
    print(f"  authoritative muni points: {file_hits} from file + "
          f"{len(muni) - file_hits - len(sibling)} live + {len(sibling)} other-version "
          f"= {len(muni)}/{len(pnos)}")
    return muni, sibling


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    print("=== geocode_committee_permits.py ===")

    src = load_json(SRC)
    matches = src.get("matches", []) if isinstance(src, dict) else (src or [])
    print(f"  {len(matches)} committee permits in source")

    minhak_polys = load_minhak_polys()
    subn_fallback = build_subn_fallback(minhak_polys)
    subn_polys = load_subn_polys()
    print(f"  loaded {len(minhak_polys)} minhak polygons, {len(subn_fallback)} sub-neighborhood fallbacks")

    idx, base = load_muni_points()
    print(f"  loaded {len(idx)} muni GIS points from {os.path.basename(POINTS)}")
    pnos = [p for p in ((m.get("permit_no") or "").strip() for m in matches) if p]
    muni, sibling = resolve_muni_points(pnos, idx, base)

    cache = load_json(CACHE) if os.path.exists(CACHE) else {}

    geocoded = reused = failed = muni_n = 0
    for m in matches:
        pno = (m.get("permit_no") or "").strip()
        if not pno:
            continue
        addr = (m.get("address") or "").strip()
        hood = (m.get("neighborhood") or "").strip()

        # 1) Authoritative muni GIS point (exact, keyed by tik) — always preferred,
        #    and it upgrades any stale Nominatim/text-fallback hit in the cache.
        if pno in muni:
            lng, lat = muni[pno]
            # Geometry is final for an authoritative tik point: no buffer, and no
            # name-based rescue. A מינה"ק guessed from the PDF's neighborhood string
            # would re-admit exactly what the unbuffered PiP just ruled out.
            minhak = pip_minhak(lat, lng, minhak_polys, allow_buffer=False)
            # The PDF's neighborhood string is per-street and wrong on streets that
            # cross a boundary (ניקנור 216 "קטמון הישנה" is in גוננים) — show the
            # sub-neighborhood the point is actually in.
            cache[pno] = {
                "lat": lat, "lng": lng, "minahak": minhak or hood,
                "sub_neighborhood": pip_subn(lat, lng, subn_polys) or hood,
                "address": addr, "pdf_neighborhood": hood,
                "geocode_type": "muni_gis_sibling" if pno in sibling else "muni_gis",
            }
            if not minhak:
                lab, dist = nearest_minhak(lat, lng, minhak_polys)
                if dist <= EDGE_M:
                    cache[pno]["edge_minahak"] = lab
                    cache[pno]["edge_dist_m"] = round(dist)
            muni_n += 1
            print(f"  ⌖ {pno:14} muni GIS -> {minhak or '(out-of-area)'} ({lat:.5f},{lng:.5f})")
            continue

        prev = cache.get(pno)
        # 2) Keep a prior good hit: an earlier muni_gis point (even if this run's
        #    live query was unavailable), or a Nominatim hit whose address is unchanged.
        if prev and prev.get("lat") and prev.get("lng") and (
                prev.get("geocode_type") == "muni_gis" or prev.get("address") == addr):
            reused += 1
            continue

        # 3) Fuzzy address geocode (Nominatim) — last resort.
        g = committee_geocode(addr, hood) if addr else None
        if g:
            lat, lng, gtype = g
            minhak = pip_minhak(lat, lng, minhak_polys)
            if not minhak:
                minhak = resolve_minhak_by_name(hood, subn_fallback)
            cache[pno] = {
                "lat": lat, "lng": lng, "minahak": minhak or hood,
                "sub_neighborhood": pip_subn(lat, lng, subn_polys) or hood,
                "address": addr, "pdf_neighborhood": hood, "geocode_type": gtype,
            }
            geocoded += 1
            print(f"  ✓ {pno:14} '{addr[:26]:26}' -> {minhak or '(?)'} ({lat:.5f},{lng:.5f})")
        else:
            # No coords, but still record a name-based מינה"ק so the report column is correct.
            minhak = resolve_minhak_by_name(hood, subn_fallback)
            cache[pno] = {
                "lat": None, "lng": None, "minahak": minhak or hood,
                "sub_neighborhood": hood, "address": addr, "geocode_type": "",
            }
            failed += 1
            print(f"  ✗ {pno:14} '{addr[:26]:26}' no geocode (minhak={minhak or hood})")

    with io.open(CACHE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    print(f"\n  {muni_n} muni-GIS (authoritative), {geocoded} Nominatim, "
          f"{reused} reused from cache, {failed} ungeocodable.")
    print(f"  Wrote {CACHE} ({len(cache)} entries)")


if __name__ == "__main__":
    main()
