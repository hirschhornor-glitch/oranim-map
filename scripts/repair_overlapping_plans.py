"""
repair_overlapping_plans.py
---------------------------
One-off repair for plans whose MultiPolygon geometry has overlapping/nested
outer rings. Such geometries render as visually-empty under Leaflet's default
fillRule:'evenodd'. Cause: detect_new_plans.create_plan_geometry used to
concatenate xplan lot polygons + the blue-line plan boundary as separate
top-level entries in a MultiPolygon, with no hole-nesting.

This script applies shapely.unary_union to the existing geometry of the
specified plans, collapsing overlapping rings into a clean Polygon (with
holes) or non-overlapping MultiPolygon. No network calls.

Usage:
    python repair_overlapping_plans.py
"""

import json
from datetime import datetime
from pathlib import Path

from shapely.geometry import shape, mapping
from shapely.ops import unary_union
from shapely.validation import make_valid

PLANS_GEOJSON = Path(__file__).resolve().parent.parent / "data" / "plans.geojson"

PLANS_TO_REPAIR = [
    "101-1144179",  # קצנלסון 6
    "101-1255397",
    "101-1425875",
    "101-1296284",
    "101-1494814",
]


def clean_geometry(geom):
    """Run unary_union on the outer rings of a (Multi)Polygon to remove
    nested/overlapping rings. Returns a GeoJSON-shaped dict or None."""
    if not geom:
        return None

    if geom["type"] == "Polygon":
        polys = [make_valid(shape({"type": "Polygon", "coordinates": geom["coordinates"]}))]
    elif geom["type"] == "MultiPolygon":
        polys = []
        for coords in geom["coordinates"]:
            try:
                g = make_valid(shape({"type": "Polygon", "coordinates": coords}))
                if not g.is_empty:
                    polys.append(g)
            except Exception:
                continue
    else:
        return geom

    if not polys:
        return None

    merged = unary_union(polys)
    if merged.is_empty:
        return None

    if merged.geom_type == "GeometryCollection":
        polys = [g for g in merged.geoms if g.geom_type in ("Polygon", "MultiPolygon")]
        if not polys:
            return None
        merged = unary_union(polys)

    return mapping(merged)


def main():
    with open(PLANS_GEOJSON, encoding="utf-8") as f:
        data = json.load(f)

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    repaired = 0
    targets = set(PLANS_TO_REPAIR)

    for feat in data["features"]:
        name = feat.get("properties", {}).get("plan_name")
        if name not in targets:
            continue

        before = feat["geometry"]
        after = clean_geometry(before)
        if after is None:
            print(f"[SKIP] {name}: clean_geometry returned None")
            continue

        before_subs = (
            len(before["coordinates"]) if before["type"] == "MultiPolygon" else 1
        )
        after_subs = (
            len(after["coordinates"]) if after["type"] == "MultiPolygon" else 1
        )
        feat["geometry"] = after
        feat["properties"]["last_modified"] = now_str
        repaired += 1
        print(
            f"[OK]   {name}: {before['type']}({before_subs}) -> "
            f"{after['type']}({after_subs})"
        )

    print(f"\nRepaired {repaired}/{len(targets)} plans.")

    with open(PLANS_GEOJSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"Wrote {PLANS_GEOJSON}")


if __name__ == "__main__":
    main()
