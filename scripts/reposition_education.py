"""Reposition education layers using MOCH-derived gush/chelka lot centroids.

For education_shanaton features: take the first institution's semel_chinuch, look up in MOCH,
  use the lot centroid (yiud_karka_kayam taba match).
For mosadot_shchuna features: same via סמל חינוך.

Saves a snapshot of the original (lon, lat) under `_original_coords` and updates `coordinates`.
"""
import pandas as pd
import json, sys, io, re
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

XLSX_IN = Path("C:/ORANIM/mosadot_moch/mosadot.xlsx")  # source xlsx stays in its data dir
DATA = Path("C:/ORANIM/oranim-app/data")

# ----- Geometry (copied from build_geojson.py) -----
def point_in_ring(x, y, ring):
    inside = False; n = len(ring); j = n - 1
    for i in range(n):
        xi,yi = ring[i][0],ring[i][1]; xj,yj = ring[j][0],ring[j][1]
        if ((yi>y)!=(yj>y)) and (x < (xj-xi)*(y-yi)/((yj-yi) or 1e-12) + xi):
            inside = not inside
        j = i
    return inside

def point_in_polygon(x,y,polygon):
    if not polygon: return False
    if not point_in_ring(x,y,polygon[0]): return False
    return not any(point_in_ring(x,y,h) for h in polygon[1:])

def point_in_feature(x,y,feat):
    g = feat["geometry"]; t = g["type"]; c = g["coordinates"]
    if t=="Polygon":   return point_in_polygon(x,y,c)
    if t=="MultiPolygon": return any(point_in_polygon(x,y,p) for p in c)
    return False

def feature_bbox(feat):
    g = feat.get("geometry") or {}
    if not g: return None
    t = g["type"]; c = g["coordinates"]
    pts = [pt for ring in c for pt in ring] if t=="Polygon" else [pt for p in c for ring in p for pt in ring]
    xs = [p[0] for p in pts]; ys = [p[1] for p in pts]
    return min(xs),min(ys),max(xs),max(ys)

def ring_area(ring):
    a = 0.0; n = len(ring)
    for i in range(n):
        x1,y1 = ring[i][0], ring[i][1]
        x2,y2 = ring[(i+1)%n][0], ring[(i+1)%n][1]
        a += (x1*y2 - x2*y1)
    return a/2.0

def ring_centroid(ring):
    a = ring_area(ring)
    if abs(a) < 1e-12:
        xs = [p[0] for p in ring]; ys = [p[1] for p in ring]
        return sum(xs)/len(xs), sum(ys)/len(ys)
    cx = cy = 0.0
    for i in range(len(ring)):
        x1,y1 = ring[i][0], ring[i][1]
        x2,y2 = ring[(i+1)%len(ring)][0], ring[(i+1)%len(ring)][1]
        f = (x1*y2 - x2*y1)
        cx += (x1+x2)*f; cy += (y1+y2)*f
    return cx/(6*a), cy/(6*a)

def feature_centroid(feat):
    g = feat.get("geometry") or {}
    t = g.get("type"); c = g.get("coordinates")
    if t == "Polygon": return ring_centroid(c[0])
    if t == "MultiPolygon":
        best = None; best_area = 0
        for poly in c:
            a = abs(ring_area(poly[0]))
            if a > best_area: best_area = a; best = poly[0]
        if best: return ring_centroid(best)
    return None

def norm_taba(v):
    if v is None or (isinstance(v,float) and pd.isna(v)) or v == "": return ""
    s = str(v).strip().replace("101-","").replace("מק/","").replace("תמ\"א/","")
    m = re.match(r"^0*(\d+)(.*)$", s)
    return m.group(1) + m.group(2) if m else s

def norm_semel(v):
    """Normalize semel: strip .0, leading zeros, whitespace."""
    if v is None or (isinstance(v,float) and pd.isna(v)) or v == "": return ""
    s = str(v).strip()
    if s.endswith(".0"): s = s[:-2]
    s = s.lstrip("0") or s
    return s

# ----- Load polygons -----
print("Loading yiud_karka_kayam + landuse_xplan...")
ykk_gj = json.loads((DATA/"yiud_karka_kayam.geojson").read_text(encoding="utf-8"))
ykk_with_centroid = []
for f in ykk_gj["features"]:
    f["_bbox"] = feature_bbox(f)
    if f.get("_bbox"):
        c = feature_centroid(f)
        if c: f["_centroid"] = c; ykk_with_centroid.append(f)

xpl_gj = json.loads((DATA/"landuse_xplan.geojson").read_text(encoding="utf-8"))
xpl_with_centroid = []
for f in xpl_gj["features"]:
    f["_bbox"] = feature_bbox(f)
    if f.get("_bbox"):
        c = feature_centroid(f)
        if c: f["_centroid"] = c; xpl_with_centroid.append(f)

def lookup_centroid(orig_lon, orig_lat, taba_id):
    t = norm_taba(taba_id)
    if t:
        for f in ykk_with_centroid:
            x0,y0,x1,y1 = f["_bbox"]
            if not (x0<=orig_lon<=x1 and y0<=orig_lat<=y1): continue
            if norm_taba(f["properties"].get("TABA")) != t: continue
            if point_in_feature(orig_lon, orig_lat, f):
                return f["_centroid"][0], f["_centroid"][1], "yiud_karka_taba"
    for f in ykk_with_centroid:
        x0,y0,x1,y1 = f["_bbox"]
        if not (x0<=orig_lon<=x1 and y0<=orig_lat<=y1): continue
        if point_in_feature(orig_lon, orig_lat, f):
            return f["_centroid"][0], f["_centroid"][1], "yiud_karka_any"
    if t:
        for f in xpl_with_centroid:
            x0,y0,x1,y1 = f["_bbox"]
            if not (x0<=orig_lon<=x1 and y0<=orig_lat<=y1): continue
            if norm_taba(f["properties"].get("pl_number")) != t: continue
            if point_in_feature(orig_lon, orig_lat, f):
                return f["_centroid"][0], f["_centroid"][1], "landuse_xplan_taba"
    best = None; best_area = float('inf')
    for f in xpl_with_centroid:
        x0,y0,x1,y1 = f["_bbox"]
        if not (x0<=orig_lon<=x1 and y0<=orig_lat<=y1): continue
        if point_in_feature(orig_lon, orig_lat, f):
            try: area = float(f["properties"].get("shape_area") or (x1-x0)*(y1-y0))
            except: area = (x1-x0)*(y1-y0)
            if area < best_area: best_area = area; best = f
    if best:
        return best["_centroid"][0], best["_centroid"][1], "landuse_xplan_any"
    return None, None, None

# ----- Build MOCH education_id → centroid mapping -----
print("Loading mosadot.xlsx...")
df = pd.read_excel(XLSX_IN, sheet_name="res_1", dtype=str)
jlm = df[df["city"].astype(str).str.contains("ירושלים", na=False)].copy()
jlm["lon_f"] = pd.to_numeric(jlm["lon"], errors="coerce")
jlm["lat_f"] = pd.to_numeric(jlm["lat"], errors="coerce")
jlm = jlm.dropna(subset=["lon_f","lat_f"])

edu_records = jlm[jlm["education_id"].notna()]
print(f"MOCH records with education_id (Jerusalem): {len(edu_records)}")

semel_to_centroid = {}  # norm_semel → (lon, lat, source, taba_id, original_address)
for _, r in edu_records.iterrows():
    sem = norm_semel(r["education_id"])
    if not sem: continue
    lon, lat, src = lookup_centroid(float(r["lon_f"]), float(r["lat_f"]), r["taba_id"])
    if lon is None: continue
    if sem not in semel_to_centroid:
        addr = ((str(r["address_street_name"]) if pd.notna(r["address_street_name"]) else "") + " " +
                (str(r["address_house_num"]) if pd.notna(r["address_house_num"]) else "")).strip()
        semel_to_centroid[sem] = (lon, lat, src, r.get("taba_id"), addr)
print(f"Unique semel_chinuch with mapped centroid: {len(semel_to_centroid)}")

# ----- Reposition education_shanaton -----
print("\nRepositioning education_shanaton...")
es_gj = json.loads((DATA/"education_shanaton.geojson").read_text(encoding="utf-8"))
moved = 0; unchanged = 0
_CURATED_SRC = {"override", "building_addr_exact", "building_street_nearestnum",
                "jergisng_semel", "osm_nearest_building", "addr_alias_jergisng"}
for f in es_gj["features"]:
    # Respect curated placements — lot centroids are coarser than building-level,
    # so never clobber a manual / building / jergisng / OSM position.
    # (Curation is anchored in oranim-app/data/education_overrides.json.)
    if f["properties"].get("_manual_position") or f["properties"].get("_position_source") in _CURATED_SRC:
        unchanged += 1
        continue
    inst = f["properties"].get("institutions") or []
    centroid = None; src = None; taba = None
    for ins in inst:
        sem = norm_semel(ins.get("semel_chinuch"))
        if sem and sem in semel_to_centroid:
            lon, lat, s, tb, _ = semel_to_centroid[sem]
            centroid = (lon, lat); src = s; taba = tb
            break
    if centroid:
        orig = f["geometry"]["coordinates"]
        # only move if more than ~5m apart (avoid tiny noise updates)
        if abs(orig[0] - centroid[0]) > 0.00005 or abs(orig[1] - centroid[1]) > 0.00005:
            f["properties"]["_orig_lon"] = orig[0]
            f["properties"]["_orig_lat"] = orig[1]
            f["properties"]["_position_source"] = "moch_lot_" + src
            f["properties"]["_lot_taba"] = str(taba) if taba is not None else ""
            f["geometry"]["coordinates"] = [centroid[0], centroid[1]]
            moved += 1
        else:
            unchanged += 1
print(f"  Moved: {moved}/{len(es_gj['features'])}, unchanged: {unchanged}")

bak = DATA/"education_shanaton.backup_pre_moch.geojson"
if not bak.exists():
    bak.write_text((DATA/"education_shanaton.geojson").read_text(encoding="utf-8"), encoding="utf-8")
    print(f"  Backup → {bak.name}")
(DATA/"education_shanaton.geojson").write_text(json.dumps(es_gj, ensure_ascii=False), encoding="utf-8")
print(f"  Updated education_shanaton.geojson")

# ----- Reposition mosadot_shchuna -----
print("\nRepositioning mosadot_shchuna...")
ms_gj = json.loads((DATA/"mosadot_shchuna.geojson").read_text(encoding="utf-8"))
moved = 0; unchanged = 0
for f in ms_gj["features"]:
    sem = norm_semel(f["properties"].get("סמל חינוך"))
    if sem and sem in semel_to_centroid:
        lon, lat, src, taba, _ = semel_to_centroid[sem]
        orig = f["geometry"]["coordinates"]
        if abs(orig[0] - lon) > 0.00005 or abs(orig[1] - lat) > 0.00005:
            f["properties"]["_orig_lon"] = orig[0]
            f["properties"]["_orig_lat"] = orig[1]
            f["properties"]["_position_source"] = "moch_lot_" + src
            f["properties"]["_lot_taba"] = str(taba) if taba is not None else ""
            f["geometry"]["coordinates"] = [lon, lat]
            moved += 1
        else:
            unchanged += 1
print(f"  Moved: {moved}/{len(ms_gj['features'])}, unchanged: {unchanged}")

bak = DATA/"mosadot_shchuna.backup_pre_moch.geojson"
if not bak.exists():
    bak.write_text((DATA/"mosadot_shchuna.geojson").read_text(encoding="utf-8"), encoding="utf-8")
    print(f"  Backup → {bak.name}")
(DATA/"mosadot_shchuna.geojson").write_text(json.dumps(ms_gj, ensure_ascii=False), encoding="utf-8")
print(f"  Updated mosadot_shchuna.geojson")
