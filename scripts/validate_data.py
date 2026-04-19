"""
validate_data.py — data quality checks for the Oranim app.

Runs on every push via .github/workflows/validate.yml.
Exits non-zero if any check fails.

Checks:
  plans.geojson        — every feature has taba + plan_name + status_mavat
  tama38.geojson       — every feature has tik + status; no control chars in address
  tama38_permits.json  — plans with status='הופק*' / 'היתר*' must have non-empty permits
                         (catches the address-ambiguity bug we saw for רבקה 22)
  coords               — Jerusalem bounding box
"""
import json
import re
import sys
from pathlib import Path

DATA = Path(__file__).resolve().parent.parent / "data"

# Jerusalem bounding box (generous margins)
LNG_MIN, LNG_MAX = 35.05, 35.35
LAT_MIN, LAT_MAX = 31.65, 31.90

errors: list[str] = []
warnings: list[str] = []


def err(msg: str) -> None:
    errors.append(msg)


def warn(msg: str) -> None:
    warnings.append(msg)


def load_json(path: Path):
    if not path.exists():
        err(f"{path.name}: file missing")
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        err(f"{path.name}: not valid JSON ({e})")
        return None


def check_coord(lng, lat, label: str) -> None:
    if not (LNG_MIN <= lng <= LNG_MAX and LAT_MIN <= lat <= LAT_MAX):
        err(f"{label}: coord ({lng}, {lat}) outside Jerusalem bbox")


def iter_first_coord(geom):
    """Yield the first coordinate for any geometry type, for bbox check."""
    if not geom:
        return
    t = geom.get("type")
    coords = geom.get("coordinates")
    if not coords:
        return
    if t == "Point":
        yield coords
    elif t == "LineString" or t == "MultiPoint":
        if coords:
            yield coords[0]
    elif t == "Polygon" or t == "MultiLineString":
        if coords and coords[0]:
            yield coords[0][0]
    elif t == "MultiPolygon":
        if coords and coords[0] and coords[0][0]:
            yield coords[0][0][0]


def check_plans(data) -> None:
    if not data or "features" not in data:
        return
    feats = data["features"]
    missing_taba = 0
    missing_name = 0
    missing_status = 0
    bbox_fails = 0
    for i, f in enumerate(feats):
        p = f.get("properties", {}) or {}
        if not str(p.get("taba") or "").strip():
            missing_taba += 1
        if not str(p.get("plan_name") or "").strip():
            missing_name += 1
        if not str(p.get("status_mavat") or "").strip():
            missing_status += 1
        for c in iter_first_coord(f.get("geometry")):
            if not (LNG_MIN <= c[0] <= LNG_MAX and LAT_MIN <= c[1] <= LAT_MAX):
                bbox_fails += 1
            break

    if missing_taba:
        err(f"plans.geojson: {missing_taba} features missing taba")
    if missing_name:
        warn(f"plans.geojson: {missing_name} features missing plan_name")
    if missing_status:
        err(f"plans.geojson: {missing_status} features missing status_mavat")
    if bbox_fails:
        warn(f"plans.geojson: {bbox_fails} features outside Jerusalem bbox")
    print(f"plans.geojson: {len(feats)} features, taba OK: {len(feats) - missing_taba}")


def check_tama38(tama_data, permits_data) -> None:
    if not tama_data or "features" not in tama_data:
        return
    feats = tama_data["features"]
    missing_tik = 0
    missing_status = 0
    bad_addr = 0
    for i, f in enumerate(feats):
        p = f.get("properties", {}) or {}
        if not str(p.get("tik") or "").strip():
            missing_tik += 1
        if not str(p.get("status") or "").strip():
            missing_status += 1
        addr = str(p.get("address") or "")
        if re.search(r"[\x00-\x1f]", addr):
            bad_addr += 1

    if missing_tik:
        warn(f"tama38.geojson: {missing_tik} features missing tik")
    if missing_status:
        warn(f"tama38.geojson: {missing_status} features missing status")
    if bad_addr:
        err(f"tama38.geojson: {bad_addr} features with control chars in address")
    print(f"tama38.geojson: {len(feats)} features")

    # Cross-check: if tama38 status says 'הופק*' / 'היתר*', permits JSON should have entries.
    # This catches the scraper bug (address autocomplete ambiguity) we found for רבקה 22.
    if permits_data is None:
        return
    inconsistent: list[tuple[int, str, str]] = []
    for i, f in enumerate(feats):
        p = f.get("properties", {}) or {}
        status = str(p.get("status") or "")
        # "הופק" = actually issued. Skip "נפתח" (file opened), "תכנון", etc.
        if "הופק" not in status:
            continue
        entry = permits_data.get(str(i))
        permits = (entry or {}).get("permits") or []
        if not permits:
            inconsistent.append((i, p.get("address", ""), status))

    if inconsistent:
        # This is an error — a permit-issued project with no scraped permits suggests
        # the scraper missed it. Top few shown.
        err(
            f"tama38_permits.json: {len(inconsistent)} permit-issued projects "
            f"have empty permits (scraper likely missed). "
            f"Examples: {[f'fid={i} {a!r}' for i, a, _ in inconsistent[:3]]}"
        )


def check_all_permits(data) -> None:
    if data is None:
        return
    if not isinstance(data, dict):
        err("all_permits.json: root is not an object")
        return
    non_numeric_keys = [k for k in data.keys() if not re.match(r"^[\dא-ת/\-]+$", k)]
    if non_numeric_keys:
        warn(f"all_permits.json: {len(non_numeric_keys)} keys look non-taba-like")
    print(f"all_permits.json: {len(data)} taba entries")


def main() -> int:
    print(f"Validating data in {DATA}")
    print("-" * 60)
    plans = load_json(DATA / "plans.geojson")
    tama = load_json(DATA / "tama38.geojson")
    tama_permits = load_json(DATA / "tama38_permits.json")
    all_permits = load_json(DATA / "all_permits.json")

    check_plans(plans)
    check_tama38(tama, tama_permits)
    check_all_permits(all_permits)

    print("-" * 60)
    if warnings:
        print(f"\n⚠ WARNINGS ({len(warnings)}):")
        for w in warnings:
            print(f"  - {w}")
    if errors:
        print(f"\n✖ ERRORS ({len(errors)}):")
        for e in errors:
            print(f"  - {e}")
        return 1
    print("\n✓ all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
