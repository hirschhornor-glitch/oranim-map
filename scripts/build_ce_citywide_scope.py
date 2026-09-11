# -*- coding: utf-8 -*-
"""
build_ce_citywide_scope.py
Citywide (Jerusalem) inventory of plans that grant commerce / employment / offices.

Pulls XPLAN land-use polygons (MapServer/4) over a Jerusalem-wide ITM envelope and
keeps every parcel whose DESIGNATION NAME mentions commerce, offices, employment,
industry/crafts or "עירוני מעורב".

Filtering by NAME, not by a hand-written code list: a hand-listed set of mavat_code
values missed 8 real codes present in the city (220 משרדים, 1200 מגורים ומשרדים,
1001 דיור מיוחד ומסחר, 1560, 1574, 1600, 1411, 1630).

Scope is restricted to pl_number prefix "101-" = the Jerusalem local committee.
Checked: the envelope also catches 151-/152- (מטה יהודה, מבשרת, אבו גוש) and 3
תמל plans, all of them outside Jerusalem — so no municipal boundary polygon needed.

Outputs (both under oranim-app/data/):
  ce_citywide.geojson     — one feature per CE parcel, WGS84, with geometry
  ce_citywide_scope.json  — {pl_number: {pl_name, mp_id, station_desc, parcels[]}}

`mp_id` IS the Mavat agam_id (verified 730/730 against plans.geojson) — it is what
scrape_table5_xlsx.download_xlsx() needs, so downstream stages need no lookup.
"""
import json, os, re, ssl, sys, warnings
warnings.filterwarnings('ignore')
import requests
from requests.adapters import HTTPAdapter

if sys.platform.startswith("win"):
    sys.stdout.reconfigure(encoding="utf-8")

DATA = r"C:\ORANIM\oranim-app\data"
OUT_GEOJSON = os.path.join(DATA, "ce_citywide.geojson")
OUT_SCOPE = os.path.join(DATA, "ce_citywide_scope.json")
EXPAND_RESULTS = r"C:\ORANIM\ce_expand_results.json"
XPLAN_URL = "https://ags.iplan.gov.il/arcgisiplan/rest/services/PlanningPublic/Xplan/MapServer/4/query"
MAX_PER_REQUEST = 1000

# Jerusalem-wide ITM (EPSG:2039) envelope, generous on purpose; the 101- prefix
# filter below is what actually defines the municipal scope.
BBOX = (208000, 620000, 228000, 644000)

# Designation-name filter. "עירוני מעורב" carries no commerce word but always has
# ground-floor commerce and often employment; included per the scoping decision.
CE_RE = re.compile("מסחר|משרד|תעסוק|תעשי|מלאכה|עירוני מעורב")

COMMITTEE_PREFIX = "101-"


class _SSLAdapter(HTTPAdapter):
    """iplan still negotiates legacy TLS; requests refuses it without this."""
    def init_poolmanager(self, *a, **kw):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.set_ciphers('DEFAULT:@SECLEVEL=0')
        kw['ssl_context'] = ctx
        return super().init_poolmanager(*a, **kw)


_SESSION = requests.Session()
_SESSION.mount('https://', _SSLAdapter())


def fetch_all():
    feats, offset = [], 0
    while True:
        params = {
            'geometry': "%d,%d,%d,%d" % BBOX,
            'geometryType': 'esriGeometryEnvelope',
            'inSR': '2039',
            'spatialRel': 'esriSpatialRelIntersects',
            'outFields': '*',
            'returnGeometry': 'true',
            'f': 'geojson',
            'outSR': '4326',
            'resultOffset': offset,
            'resultRecordCount': MAX_PER_REQUEST,
        }
        print(f"  fetching offset={offset}...", end=" ", flush=True)
        r = _SESSION.get(XPLAN_URL, params=params, timeout=180, verify=False)
        r.raise_for_status()
        got = r.json().get('features', [])
        feats.extend(got)
        print(f"{len(got)} (total {len(feats)})")
        if len(got) < MAX_PER_REQUEST:
            break
        offset += len(got)
    return feats


def main():
    print("Building citywide commerce/employment scope from XPLAN...")
    all_feats = fetch_all()
    print(f"\nTotal land-use parcels in envelope: {len(all_feats)}")

    # Guard: iplan emptied land-use layer 4 during the ~2026-06 migration. Zero
    # features means "the source is down", not "the city has no land use" —
    # refuse to overwrite good committed data with an empty file.
    if not all_feats:
        print("ABORT: 0 features from XPLAN (layer 4 empty — source likely down). "
              "Refusing to write.")
        sys.exit(1)

    # Plans whose Table 5 grants commerce/employment even though NO parcel of theirs
    # carries a CE designation. The designation filter is a proxy and misses them
    # entirely -- 101-1261510 books "מסחר ותעסוקה" on parcels designated as roads.
    # expand_ce_scope.py finds them by reading Table 5 for every other 101- plan;
    # its qualifying list is folded in here so they get geometry like any other plan.
    include = set()
    try:
        exp = json.load(open(EXPAND_RESULTS, encoding='utf-8'))
        include = {pn for pn, v in exp.get('by_plan', {}).items() if v.get('qualifies')}
        print(f"including {len(include)} plans found by Table-5 scan (non-CE designation)")
    except FileNotFoundError:
        pass

    kept = []
    for f in all_feats:
        p = f.get('properties') or {}
        pn = str(p.get('pl_number') or '')
        if not pn.startswith(COMMITTEE_PREFIX):
            continue
        # For an included plan keep EVERY parcel: its commerce sits on parcels the
        # designation filter would drop, so filtering them out would leave the plan
        # on the map with no geometry at all.
        if not (CE_RE.search(p.get('mavat_name') or '') or pn in include):
            continue
        kept.append(f)

    scope = {}
    for f in kept:
        p = f['properties']
        pn = p['pl_number']
        rec = scope.setdefault(pn, {
            'pl_name': p.get('pl_name') or '',
            'mp_id': int(p['mp_id']) if p.get('mp_id') else None,   # == Mavat agam_id
            'station': p.get('station'),
            'station_desc': p.get('station_desc') or '',
            'parcels': [],
        })
        rec['parcels'].append({
            'num': str(p.get('num') or '').strip(),
            'mavat_code': p.get('mavat_code'),
            'mavat_name': p.get('mavat_name') or '',
            'legal_area': p.get('legal_area'),
        })

    approved = sum(1 for v in scope.values() if v['station_desc'] == 'אישור')
    no_mp = [k for k, v in scope.items() if not v['mp_id']]
    print(f"CE parcels: {len(kept)} | CE plans: {len(scope)} | approved: {approved}")
    if no_mp:
        print(f"WARNING: {len(no_mp)} plans have no mp_id (cannot fetch Table 5): {no_mp[:10]}")

    with open(OUT_GEOJSON, 'w', encoding='utf-8') as fh:
        json.dump({"type": "FeatureCollection",
                   "crs": {"type": "name",
                           "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
                   "features": kept}, fh, ensure_ascii=False, separators=(",", ":"))
    with open(OUT_SCOPE, 'w', encoding='utf-8') as fh:
        json.dump(scope, fh, ensure_ascii=False, indent=1)

    print(f"\n-> {OUT_GEOJSON} ({len(kept)} features)")
    print(f"-> {OUT_SCOPE} ({len(scope)} plans)")


if __name__ == "__main__":
    main()
