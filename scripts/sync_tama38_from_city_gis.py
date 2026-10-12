"""
sync_tama38_from_city_gis.py
----------------------------
Pulls the live "התחדשות עירונית תמ"א 38" layer from gisviewer.jerusalem.muni.il
(BaseLayers/MapServer/63) and adds any new records to our tama38.geojson.

Why this exists:
  Our tama38.geojson originated as a one-time SDE export (2023-05-04). New TAMA 38
  projects opened since then never made it into our data, so the biweekly permit
  scraper — which iterates from tama38.geojson — never queried YK about them.
  Example: משואות יצחק 4 (tik 2025/0308.00, תיק תמ"א 38 15831) was missing entirely.

What it does:
  1. Fetches all city-GIS TAMA 38 points intersecting the Oranim district bbox
  2. Refines with a point-in-polygon test against district_oranim.geojson
  3. Matches against current tama38.geojson by tik (and by address as a fallback)
  4. Appends any city record that isn't ours, with scraped_at=null on the
     companion tama38_permits.json entry so the next scrape picks it up

CLI:
  python sync_tama38_from_city_gis.py            # do it
  python sync_tama38_from_city_gis.py --dry-run  # just report what would change
"""
import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

import requests
from pyproj import Transformer

ROOT = Path(r"C:\ORANIM")
TAMA38_GEOJSON = ROOT / "oranim-app" / "data" / "tama38.geojson"
TAMA38_PERMITS_PUBLISHED = ROOT / "oranim-app" / "data" / "tama38_permits.json"
TAMA38_PERMITS_LIVE = ROOT / "tama38_permits.json"
DISTRICT_GEOJSON = ROOT / "oranim-app" / "data" / "district_oranim.geojson"
REPORT_DIR = ROOT / "permits_scan_reports"

CITY_LAYER_URL = (
    "https://gisviewer.jerusalem.muni.il/arcgis/rest/services/"
    "BaseLayers/MapServer/63/query"
)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# City field → our geojson property
FIELD_MAP = {
    "tik": "tik",
    "address": "address",
    "neighborhood": "neighborho",
    "rova": "rova",
    "units_exists": "units_exis",
    "units_tosefet": "units_tose",
    "open_": "open_",
    "status": "status",
    "date": "date",
    "status_1": "status_1",
    "comment": "comment",
    "harisaa": "harisaa",
}

ITM_TO_WGS = Transformer.from_crs("EPSG:2039", "EPSG:4326", always_xy=True)


def load_district_polygons():
    with open(DISTRICT_GEOJSON, encoding="utf-8") as f:
        gj = json.load(f)
    polys = []
    for feat in gj["features"]:
        g = feat.get("geometry", {})
        if g.get("type") == "Polygon":
            polys.append(g["coordinates"])
        elif g.get("type") == "MultiPolygon":
            polys.extend(g["coordinates"])
    return polys


def point_in_polygons(lon, lat, polys):
    """Even-odd point-in-polygon test honoring holes.

    Each `poly` is [outer_ring, hole1, hole2, ...]. Crossings are counted across
    ALL rings of a polygon together — so a point sitting inside a hole gets two
    crossings (outer + hole) → False. Previous version reset `inside` per ring
    and treated holes as separate enclosing polygons, which let 18 Gila records
    leak in on 2026-05-28.
    """
    for poly in polys:
        inside = False
        for ring in poly:
            n = len(ring)
            j = n - 1
            for i in range(n):
                xi, yi = ring[i][0], ring[i][1]
                xj, yj = ring[j][0], ring[j][1]
                if ((yi > lat) != (yj > lat)) and (
                    lon < (xj - xi) * (lat - yi) / ((yj - yi) or 1e-12) + xi
                ):
                    inside = not inside
                j = i
        if inside:
            return True
    return False


def district_bbox_itm():
    """Compute the bbox of the Oranim district in EPSG:2039."""
    wgs_to_itm = Transformer.from_crs("EPSG:4326", "EPSG:2039", always_xy=True)
    with open(DISTRICT_GEOJSON, encoding="utf-8") as f:
        gj = json.load(f)
    xs, ys = [], []

    def walk(o):
        if isinstance(o[0], (int, float)) and len(o) >= 2:
            x, y = wgs_to_itm.transform(o[0], o[1])
            xs.append(x)
            ys.append(y)
            return
        for c in o:
            walk(c)

    for feat in gj["features"]:
        walk(feat["geometry"]["coordinates"])
    return min(xs), min(ys), max(xs), max(ys)


def fetch_city_features():
    xmin, ymin, xmax, ymax = district_bbox_itm()
    geom_param = json.dumps({
        "xmin": xmin, "ymin": ymin, "xmax": xmax, "ymax": ymax,
        "spatialReference": {"wkid": 2039},
    })
    params = {
        "geometry": geom_param,
        "geometryType": "esriGeometryEnvelope",
        "inSR": 2039,
        "outSR": 2039,
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "*",
        "returnGeometry": "true",
        "f": "pjson",
    }
    r = requests.get(CITY_LAYER_URL, params=params, headers={"User-Agent": USER_AGENT}, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("features", [])


def _clean(s):
    """Strip control chars the city layer sometimes ships (e.g. a lone \\x08
    address, 2026-07-05) — they fail validate_data.py and str.strip() keeps them."""
    if not isinstance(s, str):
        return s
    return re.sub(r"[\x00-\x1f\x7f]", "", s)


def _norm(s):
    return _clean(s or "").replace('"', "").replace("'", "").strip().lower()


def build_existing_keys(geojson):
    """Return three lookup sets we use to detect duplicates."""
    tiks = set()
    addresses = set()
    for feat in geojson["features"]:
        p = feat.get("properties", {})
        tik = _norm(p.get("tik"))
        if tik:
            tiks.add(tik)
        addr = _norm(p.get("address"))
        if addr:
            addresses.add(addr)
    return tiks, addresses


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="report only, no writes")
    args = parser.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")

    print(f"Fetching city-GIS TAMA 38 layer (gisviewer/BaseLayers/63)...")
    city_features = fetch_city_features()
    print(f"  city features in Oranim bbox: {len(city_features)}")

    polys = load_district_polygons()
    print(f"  district polygon rings: {sum(len(p) for p in polys)}")

    with open(TAMA38_GEOJSON, encoding="utf-8") as f:
        our_geojson = json.load(f)
    our_count_before = len(our_geojson["features"])
    print(f"  our tama38.geojson features: {our_count_before}")

    existing_tiks, existing_addrs = build_existing_keys(our_geojson)
    max_fid = max((feat["properties"].get("fid") or 0) for feat in our_geojson["features"])
    max_qc = max((feat["properties"].get("qc_id") or 0) for feat in our_geojson["features"])
    today = date.today().isoformat()

    to_add = []
    skipped_outside = 0
    skipped_dup_tik = 0
    skipped_dup_addr = 0
    for feat in city_features:
        attrs = feat.get("attributes") or {}
        geom = feat.get("geometry") or {}
        x_itm = geom.get("x")
        y_itm = geom.get("y")
        if x_itm is None or y_itm is None:
            continue
        lon, lat = ITM_TO_WGS.transform(x_itm, y_itm)
        if not point_in_polygons(lon, lat, polys):
            skipped_outside += 1
            continue
        tik = _norm(attrs.get("tik"))
        addr = _norm(attrs.get("address"))
        if not tik and not addr:
            # No identifier at all — useless for matching or scraping
            continue
        if tik and tik in existing_tiks:
            skipped_dup_tik += 1
            continue
        if addr and addr in existing_addrs:
            skipped_dup_addr += 1
            continue
        to_add.append((attrs, x_itm, y_itm, lon, lat))

    print()
    print(f"Sync summary (vs current tama38.geojson):")
    print(f"  inside district + new      : {len(to_add)}")
    print(f"  outside district (bbox FP) : {skipped_outside}")
    print(f"  duplicate by tik           : {skipped_dup_tik}")
    print(f"  duplicate by address       : {skipped_dup_addr}")
    print()

    if to_add:
        print(f"New records:")
        for attrs, *_ in to_add[:30]:
            tik = attrs.get("tik") or ""
            addr = attrs.get("address") or ""
            status = attrs.get("status") or ""
            print(f"  tik={tik:<14s} addr={addr:<28s} status={status}")
        if len(to_add) > 30:
            print(f"  ... and {len(to_add) - 30} more")

    if args.dry_run:
        print("\n[dry-run] no files written")
        return

    if not to_add:
        print("Nothing to add.")
        return

    # Build new geojson features
    new_features = []
    new_fids = []
    next_fid = max_fid + 1
    next_qc = max_qc + 1
    for attrs, x_itm, y_itm, lon, lat in to_add:
        props = {
            "fid": next_fid,
            "qc_id": next_qc,
            "tik": attrs.get("tik"),
            "address": attrs.get("address"),
            "neighborho": attrs.get("neighborhood"),
            "rova": attrs.get("rova"),
            "units_exis": attrs.get("units_exists"),
            "units_tose": attrs.get("units_tosefet"),
            "open_": attrs.get("open_"),
            "status": attrs.get("status"),
            "date": attrs.get("date"),
            "status_1": attrs.get("status_1"),
            "comment": attrs.get("comment"),
            "harisaa": attrs.get("harisaa"),
            "y": y_itm,
            "x": x_itm,
            "created_us": "city_gis_sync",
            "created_da": today,
            "last_edite": "city_gis_sync",
            "last_edi_1": today,
            "__rec_stat": None,
        }
        props = {k: _clean(v) for k, v in props.items()}
        feature = {
            "type": "Feature",
            "properties": props,
            "geometry": {"type": "MultiPoint", "coordinates": [[lon, lat]]},
        }
        new_features.append(feature)
        new_fids.append(next_fid)
        next_fid += 1
        next_qc += 1

    our_geojson["features"].extend(new_features)
    with open(TAMA38_GEOJSON, "w", encoding="utf-8") as f:
        json.dump(our_geojson, f, ensure_ascii=False, indent=1)
    print(f"\nWrote {TAMA38_GEOJSON} (features: {our_count_before} → {len(our_geojson['features'])})")

    # NOTE: do NOT touch tama38_permits.json here. New fids won't have entries
    # by definition (since their fid didn't exist before), so the scraper will
    # naturally pick them up. Earlier versions of this script aggressively
    # deleted "stubs" for the new fids — that wiped real scraped data on re-runs.

    # Auto-commit immediately so a concurrent git_sync pull/rebase doesn't stash
    # our uncommitted changes and drop them. This bit us once on 2026-05-28 when
    # a background update_plans.py invocation reset the file mid-flight.
    if new_fids:
        repo_dir = TAMA38_GEOJSON.parent.parent  # oranim-app/
        relpath = TAMA38_GEOJSON.relative_to(repo_dir).as_posix()
        msg = f"data(tama38): sync {len(new_fids)} new records from city GIS layer 63 ({today})"
        try:
            import subprocess
            subprocess.run(["git", "add", relpath], cwd=str(repo_dir), check=True)
            res = subprocess.run(
                ["git", "commit", "-m", msg],
                cwd=str(repo_dir), capture_output=True, text=True,
            )
            if res.returncode == 0:
                print(f"  auto-committed: {msg}")
            elif "nothing to commit" in (res.stdout + res.stderr).lower():
                pass
            else:
                print(f"  WARN: auto-commit failed: {res.stderr.strip()[:200]}")
        except Exception as e:
            print(f"  WARN: auto-commit raised: {e}")

    # Brief markdown report
    REPORT_DIR.mkdir(exist_ok=True)
    report_path = REPORT_DIR / f"tama38_sync_{today}.md"
    lines = [
        f"# סנכרון תמ\"א 38 משכבת העירייה — {today}",
        "",
        f"מקור: gisviewer.jerusalem.muni.il / BaseLayers / MapServer / 63",
        f"",
        f"- features ב-bbox אורנים: {len(city_features)}",
        f"- חדשים שנוספו: {len(to_add)} (fids {min(new_fids)}–{max(new_fids)})",
        f"- מחוץ לפוליגון: {skipped_outside}",
        f"- כפילות לפי tik: {skipped_dup_tik}",
        f"- כפילות לפי כתובת: {skipped_dup_addr}",
        "",
        "## רשומות חדשות",
        "| fid | tik | כתובת | שכונה | סטטוס |",
        "|---|---|---|---|---|",
    ]
    for (attrs, *_), fid in zip(to_add, new_fids):
        lines.append(
            f"| {fid} | {attrs.get('tik') or ''} | {attrs.get('address') or ''} | "
            f"{attrs.get('neighborhood') or ''} | {attrs.get('status') or ''} |"
        )
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nReport: {report_path}")


if __name__ == "__main__":
    main()
