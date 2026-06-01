"""
build_stat_areas.py — Download CBS census-2022 statistical-area polygons for the
Oranim district into oranim-app/data/stat_areas.geojson, enriched with the
demographic fields the program-balance ("מאזן פרוגרמתי") uses for per-area
household-size resolution.

Source: Systematics "אטלס מפקד 2022" FeatureServer, which republishes the CBS
census-2022 statistical-areas layer (census_2022_statistical_areas_2022) WITH the
full demographic attribute table (the official ISRAEL_CBS_GIS layer carries
boundaries only). Keyed by SEMEL_YISHUV (3000 = ירושלים) + STAT_2022.

Output properties per feature (consumed by getStatAreaAssumptions in app.jsx):
    stat_area_id        - STAT_2022 (stat-area number within Jerusalem)
    name                - human label, e.g. 'אז"ס 211 · חילוני'
    householdSize       - size_avg (ממוצע נפשות במשק הבית)
    ageYearPctGeneral   - derived: age0_19_pcnt / 20  (single-year cohort %)
    ageYearPctHaredi    - same per-area value (the census age structure already
                          reflects the area's haredi share)
    haredi              - fraction 0..1, proxied from hh_MidatDatiyut_Name
    religious           - fraction 0..1, proxied from hh_MidatDatiyut_Name
Plus reference-only: pop_approx, datiyut, main_function.

A property is OMITTED when its source value is null/blank, so the runtime overlay
(getStatAreaAssumptions) only overrides the minahak baseline with real data.

NOTE: haredi/religious written here are a DOMINANT-CATEGORY PROXY. After running this
script, run enrich_stat_areas_datiyut.py to overwrite them with REAL per-sector
lifestyle fractions from the CBS census explorer. Re-running this script reverts to the
proxy, so always follow it with the enrichment step.
"""
import json
import urllib.parse
import urllib.request

ARCGIS_BASE = ("https://services.arcgis.com/b6zSqPB0TQc3mBgU/arcgis/rest/services/"
               "%D7%9E%D7%A4%D7%A7%D7%93_2022/FeatureServer/0/query")
DISTRICT_GEOJSON = r"C:\ORANIM\oranim-app\data\district_oranim.geojson"
OUTPUT_FILE = r"C:\ORANIM\oranim-app\data\stat_areas.geojson"
SEMEL_JERUSALEM = 3000
BUFFER_DEG = 0.003  # ~300m — match buildings.geojson edge coverage

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

OUT_FIELDS = ("STAT_2022,YISHUV_STAT_2022,size_avg,age0_19_pcnt,age20_64_pcnt,age65_pcnt,"
              "hh_MidatDatiyut_Name,Main_Function_Txt,pop_approx,hh_total_approx,"
              "age_median,ChldBorn_avg,pop_density,"
              "own_pcnt,rent_pcnt,Vehicle0_pcnt,Vehicle2up_pcnt,Parking_pcnt")

# Dominant-lifestyle (hh_MidatDatiyut_Name) -> (haredi, religious) fractions.
# Proxy: CBS publishes only the *dominant* household lifestyle per stat area, not
# a full breakdown. These representative fractions feed computePublicNeeds (school
# stream split) and computeNeighborhoodProgramServices (synagogue/mikve basis).
# Tune here if better local estimates become available.
DATIYUT_MAP = {
    "חרדי":            {"haredi": 0.85, "religious": 0.10},
    "דתי/ דתי מאוד":   {"haredi": 0.10, "religious": 0.70},
    "מסורתי":          {"haredi": 0.0,  "religious": 0.35},
    "חילוני":          {"haredi": 0.0,  "religious": 0.05},
}


def district_bbox():
    d = json.load(open(DISTRICT_GEOJSON, encoding="utf-8"))
    xs, ys = [], []

    def walk(c):
        if isinstance(c[0], (int, float)):
            xs.append(c[0]); ys.append(c[1])
        else:
            for sub in c:
                walk(sub)
    for f in d["features"]:
        g = f.get("geometry") or {}
        if g.get("coordinates"):
            walk(g["coordinates"])
    return (min(xs) - BUFFER_DEG, min(ys) - BUFFER_DEG,
            max(xs) + BUFFER_DEG, max(ys) + BUFFER_DEG)


def fetch_page(offset, xmin, ymin, xmax, ymax):
    geom = {"xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax,
            "spatialReference": {"wkid": 4326}}
    params = {
        "where": f"SEMEL_YISHUV={SEMEL_JERUSALEM}",
        "geometry": json.dumps(geom),
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": OUT_FIELDS,
        "outSR": "4326",
        "returnGeometry": "true",
        "f": "geojson",
        "resultRecordCount": "2000",
        "resultOffset": str(offset),
    }
    url = ARCGIS_BASE + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=90) as r:
        return json.loads(r.read().decode("utf-8"))


def round_coords(c, nd=6):
    if isinstance(c[0], (int, float)):
        return [round(c[0], nd), round(c[1], nd)]
    return [round_coords(s, nd) for s in c]


def num(v):
    """Return float if v is a usable number, else None."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f


def derive_props(attrs):
    stat = attrs.get("STAT_2022")
    size_avg = num(attrs.get("size_avg"))
    age0_19 = num(attrs.get("age0_19_pcnt"))
    datiyut = (attrs.get("hh_MidatDatiyut_Name") or "").strip()
    func = (attrs.get("Main_Function_Txt") or "").strip()
    pop = num(attrs.get("pop_approx"))

    props = {"stat_area_id": stat}
    label = f'אז"ס {stat}'
    if datiyut:
        label += f" · {datiyut}"
    props["name"] = label

    if size_avg and size_avg > 0:
        props["householdSize"] = round(size_avg, 2)
    if age0_19 and age0_19 > 0:
        per_year = round(age0_19 / 20.0, 2)  # 0-19 spans 20 single-year cohorts
        props["ageYearPctGeneral"] = per_year
        props["ageYearPctHaredi"] = per_year
    if datiyut in DATIYUT_MAP:
        props["haredi"] = DATIYUT_MAP[datiyut]["haredi"]
        props["religious"] = DATIYUT_MAP[datiyut]["religious"]

    # reference-only (not consumed by the assumptions overlay)
    if pop is not None:
        props["pop_approx"] = int(pop)
    if datiyut:
        props["datiyut"] = datiyut
    if func:
        props["main_function"] = func

    # Population-dashboard fields (computePopulationByGeography aggregates these).
    # raw census percentages / counts, omitted when null.
    hh_total = num(attrs.get("hh_total_approx"))
    if hh_total is not None:
        props["hh_total"] = int(hh_total)
    for src, dst in (("age_median", "age_median"), ("age20_64_pcnt", "age20_64_pcnt"),
                     ("age65_pcnt", "age65_pcnt"), ("ChldBorn_avg", "children_per_woman"),
                     ("pop_density", "pop_density"), ("own_pcnt", "own_pcnt"),
                     ("rent_pcnt", "rent_pcnt"), ("Vehicle0_pcnt", "vehicle0_pcnt"),
                     ("Vehicle2up_pcnt", "vehicle2up_pcnt"), ("Parking_pcnt", "parking_pcnt")):
        v = num(attrs.get(src))
        if v is not None:
            props[dst] = round(v, 2)
    # age0_19_pcnt as a display field (separate from the per-year ageYearPct above)
    if age0_19 is not None:
        props["age0_19_pcnt"] = round(age0_19, 2)
    return props


def main():
    xmin, ymin, xmax, ymax = district_bbox()
    print(f"district bbox: lon {xmin:.4f}..{xmax:.4f}, lat {ymin:.4f}..{ymax:.4f}")

    features = []
    offset = 0
    while True:
        data = fetch_page(offset, xmin, ymin, xmax, ymax)
        feats = data.get("features", [])
        if not feats:
            break
        for f in feats:
            geom = f.get("geometry")
            if not geom or not geom.get("coordinates"):
                continue
            attrs = f.get("properties") or {}
            geom["coordinates"] = round_coords(geom["coordinates"])
            features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": derive_props(attrs),
            })
        print(f"  page offset={offset}: +{len(feats)} (total {len(features)})")
        offset += len(feats)
        if len(feats) < 2000:
            break

    fc = {"type": "FeatureCollection", "features": features}
    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        json.dump(fc, fh, ensure_ascii=False, separators=(",", ":"))
    print(f"\nwrote {len(features)} statistical areas to {OUTPUT_FILE}")

    with_hh = [f for f in features if "householdSize" in f["properties"]]
    with_dat = [f for f in features if "haredi" in f["properties"]]
    print(f"  with householdSize: {len(with_hh)} | with datiyut: {len(with_dat)}")
    if with_hh:
        sizes = [f["properties"]["householdSize"] for f in with_hh]
        print(f"  householdSize range: {min(sizes)}..{max(sizes)}")
    print("  sample:")
    for f in features[:6]:
        p = f["properties"]
        print(f"    {p.get('name')}: hh={p.get('householdSize')} "
              f"ageGen={p.get('ageYearPctGeneral')} haredi={p.get('haredi')} "
              f"religious={p.get('religious')}")


if __name__ == "__main__":
    main()
