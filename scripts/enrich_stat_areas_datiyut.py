"""
enrich_stat_areas_datiyut.py — Replace the dominant-category haredi/religious PROXY
in data/stat_areas.geojson with REAL per-sector lifestyle fractions from the CBS
census-2022 interactive explorer (census.cbs.gov.il).

The geographic FeatureServer (build_stat_areas.py) only carries the *dominant*
household lifestyle (hh_MidatDatiyut_Name). The interactive census tool additionally
exposes the full distribution "אוכלוסייה, לפי אורח החיים העיקרי של משק הבית"
(אחר / דתי / חילוני / חרדי / מסורתי / מעורב) per statistical area, via its CSV export.

Pipeline (htmx server-rendered site):
  1. Resolve STAT_2022 -> opaque area id through the search partial
     GET /he/partials/search/area?search=ירושלים אזור סטטיסטי <STAT>
     (each result is <button data-id=".." data-search="ירושלים אזור סטטיסטי <STAT>">);
     results are pooled so a handful of queries resolve all areas.
  2. GET /he/api/get-csv?dashboardId=<דת ודתיות>&ID=<id> -> a ZIP of CSVs;
     parse the lifestyle-distribution CSV.
  3. haredi  = חרדי%
     religious = דתי% + MASORTI_RELIGIOUS_WEIGHT * מסורתי%
     (masorti partially observant; counts toward synagogue/mikve basis at half weight).

Writes the fractions back into each feature's properties (overwriting the proxy) and
stores the raw breakdown in `datiyut_breakdown` for transparency. Areas with no
lifestyle data (non-Jewish / suppressed) are left untouched (keep proxy/fallback).
"""
import csv
import io
import json
import re
import time
import zipfile
import requests

GEOJSON = r"C:\ORANIM\oranim-app\data\stat_areas.geojson"
BASE = "https://census.cbs.gov.il"
SEARCH_URL = BASE + "/he/partials/search/area"
CSV_URL = BASE + "/he/api/get-csv"
DASHBOARD_DAT = "e0dbubs8My5n3QcIjFcOXb"   # "דת ודתיות" dashboard
LIFESTYLE_FILE_HINT = "אורח החיים העיקרי"   # substring of the distribution CSV name
MASORTI_RELIGIOUS_WEIGHT = 0.5
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
SLEEP = 0.35

session = requests.Session()
session.headers.update({"User-Agent": UA})


def resolve_search_ids(queries):
    """search-key (area-number string OR unified 'a+b') -> census area id, pooled across
    queries. The site lists unified areas as data-search='ירושלים אזור סטטיסטי 1818+1817',
    so the key is the raw string after the prefix (not necessarily pure digits)."""
    id_by_key = {}
    pat = re.compile(r'data-id="([a-z0-9]+)"\s+data-search="ירושלים אזור סטטיסטי ([^"]+?)"')
    for q in queries:
        q = str(q)
        if q in id_by_key:
            continue
        try:
            r = session.get(SEARCH_URL, params={"search": q},
                            headers={"HX-Request": "true"}, timeout=40)
            r.encoding = "utf-8"
            for m in pat.finditer(r.text):
                id_by_key.setdefault(m.group(2).strip(), m.group(1))
        except Exception as e:
            print(f"  search {q} failed: {e}")
        time.sleep(SLEEP)
    return id_by_key


def fetch_lifestyle(area_id):
    """Return {category: pct_float} or None."""
    try:
        r = session.get(CSV_URL, params={"dashboardId": DASHBOARD_DAT, "ID": area_id}, timeout=40)
        if r.status_code != 200 or not r.content[:2] == b"PK":
            return None
        z = zipfile.ZipFile(io.BytesIO(r.content))
        name = next((n for n in z.namelist() if LIFESTYLE_FILE_HINT in n), None)
        if not name:
            return None
        text = z.read(name).decode("utf-8-sig")
        rows = list(csv.reader(io.StringIO(text)))
        if len(rows) < 3:
            return None
        headers = [h.strip() for h in rows[0]]
        values = rows[2]  # row 1 is the "ערך" label row
        out = {}
        for h, v in zip(headers, values):
            v = (v or "").strip().rstrip("%")
            try:
                out[h] = float(v)
            except ValueError:
                pass
        return out or None
    except Exception as e:
        print(f"  csv {area_id} failed: {e}")
        return None


def derive(breakdown):
    haredi = breakdown.get("חרדי", 0.0)
    dati = breakdown.get("דתי/דתי מאוד", 0.0)
    masorti = breakdown.get("מסורתי", 0.0)
    haredi_frac = round(haredi / 100.0, 4)
    religious_frac = round((dati + MASORTI_RELIGIOUS_WEIGHT * masorti) / 100.0, 4)
    return haredi_frac, religious_frac


def main():
    gj = json.load(open(GEOJSON, encoding="utf-8"))
    feats = gj["features"]
    stats = [f["properties"]["stat_area_id"] for f in feats
             if isinstance(f["properties"].get("stat_area_id"), int)]
    unites = sorted({f["properties"].get("unite_id") for f in feats if f["properties"].get("unite_id")})
    print(f"{len(feats)} areas, resolving census ids for {len(stats)} stats + {len(unites)} unified...")
    id_by_key = resolve_search_ids([str(s) for s in stats] + list(unites))
    print(f"resolved {len(id_by_key)} ids")

    enriched = 0
    unified_used = 0
    for f in feats:
        p = f["properties"]
        stat = p.get("stat_area_id")
        aid = id_by_key.get(str(stat))
        bd = fetch_lifestyle(aid) if aid else None
        time.sleep(SLEEP)
        src_id, unified = aid, None
        # Fallback: unified-publication area (e.g. 1818+1817) when the area has no own breakdown
        if not bd and p.get("unite_id"):
            uid = id_by_key.get(p["unite_id"])
            if uid:
                bd = fetch_lifestyle(uid)
                time.sleep(SLEEP)
                if bd:
                    src_id, unified = uid, p["unite_id"]
        if not bd:
            continue
        haredi_frac, religious_frac = derive(bd)
        p["haredi"] = haredi_frac
        p["religious"] = religious_frac
        p["datiyut_breakdown"] = {k: bd[k] for k in bd}
        p["census_id"] = src_id
        if unified:
            p["datiyut_unified"] = unified
            unified_used += 1
        enriched += 1
        tag = f" [מאוחד {unified}]" if unified else ""
        print(f"  STAT {stat}{tag}: haredi={haredi_frac} religious={religious_frac} "
              f"(חרדי={bd.get('חרדי')}% דתי={bd.get('דתי/דתי מאוד')}% מסורתי={bd.get('מסורתי')}%)")

    with open(GEOJSON, "w", encoding="utf-8") as fh:
        json.dump(gj, fh, ensure_ascii=False, separators=(",", ":"))
    print(f"\nenriched {enriched}/{len(feats)} areas ({unified_used} via unified-area fallback)")


if __name__ == "__main__":
    main()
