"""
build_junction_graph.py — Build a directed, turn-aware street graph for the
Oranim junction area from OpenStreetMap (Overpass), for the standalone
"How do I get from A to B?" mini-app (junction.html).

Output: oranim-app/data/junction_graph.json  (edge-oriented directed graph)
Raw Overpass response is cached to data/_junction_overpass_cache.json so reruns
are offline (matching the repo's *_cache.json habit).

The hand-authored change layer (data/junction_changes.json) is NOT produced here;
this script only VALIDATES that every edge/node id it references still exists.

Run manually:  py scripts/build_junction_graph.py            (uses cache if present)
               py scripts/build_junction_graph.py --refresh  (force re-fetch)
               py scripts/build_junction_graph.py --validate-changes
"""
import json
import math
import os
import sys
import urllib.parse
import urllib.request

# --- Junction bbox (S, W, N, E), covers the junction core + all reroute streets
# (Pierre Koenig, Ben Zakai, Yehuda, Rivka, Mekor Chaim, HaRakevet, Emek Refaim,
#  Tzrat, Naftali, Asher, Maagalei Yavne, Ben Gamla, Derech Hevron, Park HaMesila).
BBOX = (31.7460, 35.2060, 31.7625, 35.2235)

DATA_DIR = r"C:\ORANIM\oranim-app\data"
OUTPUT_FILE = os.path.join(DATA_DIR, "junction_graph.json")
CACHE_FILE = os.path.join(DATA_DIR, "_junction_overpass_cache.json")
ROADS_FILE = os.path.join(DATA_DIR, "roads.geojson")
CHANGES_FILE = os.path.join(DATA_DIR, "junction_changes.json")

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
UA = "oranim-junction-tool/1.0 (urban planning; contact via github oranim-map)"

# Highway classes drivable by private car
CAR_HW = {
    "motorway", "trunk", "primary", "secondary", "tertiary", "unclassified",
    "residential", "living_street", "service", "road",
    "motorway_link", "trunk_link", "primary_link", "secondary_link",
    "tertiary_link", "residential_link",
}
# Highway classes usable on foot (in addition to most car roads)
WALK_HW = CAR_HW | {"footway", "path", "pedestrian", "steps", "track", "cycleway"}
# Car-only classes that pedestrians must NOT use
NO_WALK_HW = {"motorway", "trunk", "motorway_link", "trunk_link"}


def overpass_query():
    s, w, n, e = BBOX
    return (
        "[out:json][timeout:180];\n"
        "(\n"
        f'  way["highway"]({s},{w},{n},{e});\n'
        f'  relation["type"="restriction"]({s},{w},{n},{e});\n'
        ");\n"
        "out body;\n"
        ">;\n"
        "out skel qt;\n"
    )


def fetch_overpass():
    body = urllib.parse.urlencode({"data": overpass_query()}).encode()
    req = urllib.request.Request(OVERPASS_URL, data=body, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=200) as r:
        return json.loads(r.read().decode("utf-8"))


def load_overpass(refresh=False):
    if not refresh and os.path.exists(CACHE_FILE):
        print(f"Using cached Overpass response: {CACHE_FILE}")
        return json.load(open(CACHE_FILE, encoding="utf-8"))
    print("Fetching from Overpass ...")
    j = fetch_overpass()
    json.dump(j, open(CACHE_FILE, "w", encoding="utf-8"), ensure_ascii=False)
    print(f"Cached -> {CACHE_FILE}")
    return j


def haversine(a, b):
    """a,b = [lng,lat]; returns metres."""
    R = 6371000.0
    lon1, lat1, lon2, lat2 = map(math.radians, [a[0], a[1], b[0], b[1]])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


def polyline_length(coords):
    return sum(haversine(coords[i], coords[i + 1]) for i in range(len(coords) - 1))


def parse_oneway(tags):
    """Return 'ft' (forward only), 'tf' (reverse only), or None (both)."""
    ow = (tags.get("oneway") or "").strip().lower()
    if tags.get("junction") == "roundabout" and ow not in ("no", "-1"):
        return "ft"
    if ow in ("yes", "true", "1"):
        return "ft"
    if ow in ("-1", "reverse"):
        return "tf"
    return None


def build_graph(overpass):
    nodes_raw = {}   # osm node id -> [lng, lat]
    ways = []        # {id, tags, nodes:[osm ids]}
    relations = []   # restriction relations
    for el in overpass["elements"]:
        t = el["type"]
        if t == "node":
            nodes_raw[el["id"]] = [el["lon"], el["lat"]]
        elif t == "way" and "highway" in el.get("tags", {}):
            ways.append({"id": el["id"], "tags": el["tags"], "nodes": el["nodes"]})
        elif t == "relation" and el.get("tags", {}).get("type") == "restriction":
            relations.append(el)

    # Count node usage across highway ways -> split points are shared/endpoint nodes
    usage = {}
    for wy in ways:
        for nid in wy["nodes"]:
            usage[nid] = usage.get(nid, 0) + 1

    def is_split(nid, wy, idx):
        return idx == 0 or idx == len(wy["nodes"]) - 1 or usage.get(nid, 0) >= 2

    nodes = {}       # graph node id -> {id,lng,lat,osm}
    edges = []       # directed edges
    used_nodes = set()
    # index: (way_id, via_osm_node) -> {"in": edgeId entering via, "out": edgeId leaving via}
    way_node_edges = {}

    def ensure_node(osm_id):
        gid = f"n{osm_id}"
        if gid not in nodes:
            lng, lat = nodes_raw[osm_id]
            nodes[gid] = {"id": gid, "lng": lng, "lat": lat, "osm": osm_id}
        used_nodes.add(gid)
        return gid

    eidx = 0
    for wy in ways:
        tags = wy["tags"]
        hw = tags["highway"]
        name = tags.get("name") or tags.get("ref") or ""
        car = hw in CAR_HW and tags.get("access") not in ("no", "private")
        walk = (hw in WALK_HW) and (hw not in NO_WALK_HW)
        ow = parse_oneway(tags)
        seq = wy["nodes"]
        # break into segments at split points
        seg_start = 0
        for i in range(1, len(seq)):
            if not is_split(seq[i], wy, i):
                continue
            seg_nodes = seq[seg_start:i + 1]
            if len(seg_nodes) < 2:
                seg_start = i
                continue
            coords = [nodes_raw[nid] for nid in seg_nodes if nid in nodes_raw]
            if len(coords) < 2:
                seg_start = i
                continue
            a_osm, b_osm = seg_nodes[0], seg_nodes[-1]
            if a_osm not in nodes_raw or b_osm not in nodes_raw:
                seg_start = i
                continue
            a = ensure_node(a_osm)
            b = ensure_node(b_osm)
            length = round(polyline_length(coords), 2)

            def add_edge(frm, to, geom, frm_osm, to_osm):
                nonlocal eidx
                eid = f"e{eidx}"
                eidx += 1
                edges.append({
                    "id": eid, "from": frm, "to": to, "way": wy["id"],
                    "name": name, "length": length,
                    "modes": {"car": car, "walk": walk},
                    "oneway": ow is not None,
                    "pairId": None,
                    "geometry": [[round(c[0], 6), round(c[1], 6)] for c in geom],
                })
                way_node_edges.setdefault((wy["id"], to_osm), {})["in"] = eid
                way_node_edges.setdefault((wy["id"], frm_osm), {})["out"] = eid
                return eid

            fwd = rev = None
            if ow != "tf":
                fwd = add_edge(a, b, coords, a_osm, b_osm)
            if ow != "ft":
                rev = add_edge(b, a, list(reversed(coords)), b_osm, a_osm)
            if fwd and rev:
                ef = next(e for e in edges if e["id"] == fwd)
                er = next(e for e in edges if e["id"] == rev)
                ef["pairId"] = rev
                er["pairId"] = fwd
            seg_start = i

    # keep only nodes that are actually referenced by an edge
    nodes = {k: v for k, v in nodes.items() if k in used_nodes}

    # --- turn restrictions from OSM relations ---
    turns = []
    tr_idx = 0
    skipped = 0
    for rel in relations:
        rtype = rel["tags"].get("restriction", "")
        from_way = via_node = to_way = None
        via_is_way = False
        for m in rel.get("members", []):
            if m["role"] == "from" and m["type"] == "way":
                from_way = m["ref"]
            elif m["role"] == "to" and m["type"] == "way":
                to_way = m["ref"]
            elif m["role"] == "via":
                if m["type"] == "node":
                    via_node = m["ref"]
                else:
                    via_is_way = True
        if via_is_way or not (from_way and to_way and via_node):
            skipped += 1
            continue
        fe = way_node_edges.get((from_way, via_node), {}).get("in")
        te = way_node_edges.get((to_way, via_node), {}).get("out")
        if not fe or not te:
            skipped += 1
            continue
        turns.append({
            "id": f"tr_osm_{tr_idx}", "fromEdge": fe, "viaNode": f"n{via_node}",
            "toEdge": te, "type": rtype, "source": "osm",
        })
        tr_idx += 1

    graph = {
        "meta": {
            "bbox": [BBOX[1], BBOX[0], BBOX[3], BBOX[2]],  # [W,S,E,N]
            "source": "overpass",
            "crs": "EPSG:4326",
            "counts": {"nodes": len(nodes), "edges": len(edges),
                       "turnRestrictions": len(turns), "skippedRestrictions": skipped},
        },
        "nodes": nodes,
        "edges": edges,
        "turnRestrictions": turns,
    }
    return graph


def cross_validate_oneway(graph):
    """Best-effort: compare OSM oneway flags against the muni roads.geojson
    (oneway codes ft/tf/2/N). Only compares the *is-oneway* boolean via a coarse
    ~40m midpoint grid. Direction sense is not compared. Logs a summary only."""
    if not os.path.exists(ROADS_FILE):
        print("roads.geojson not found — skipping oneway cross-validation.")
        return
    try:
        roads = json.load(open(ROADS_FILE, encoding="utf-8"))
    except Exception as ex:  # noqa
        print(f"Could not read roads.geojson ({ex}) — skipping cross-validation.")
        return

    def cell(lng, lat):
        return (round(lng, 3), round(lat, 3))  # ~110m lon / ~110m lat cells

    grid = {}
    for f in roads.get("features", []):
        p = f.get("properties", {}) or {}
        ow = str(p.get("oneway", "")).strip().lower()
        is_ow = ow in ("ft", "tf")
        g = f.get("geometry") or {}
        gt = g.get("type")
        coords = g.get("coordinates") or []
        lines = []
        if gt == "LineString":
            lines = [coords]
        elif gt == "MultiLineString":
            lines = coords
        for ln in lines:
            if len(ln) < 2:
                continue
            mid = ln[len(ln) // 2]
            grid.setdefault(cell(mid[0], mid[1]), []).append(is_ow)

    agree = mism = nomatch = 0
    seen_pairs = set()
    for e in graph["edges"]:
        if not e["modes"]["car"]:
            continue
        key = tuple(sorted([e["from"], e["to"]]))  # count each segment once
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        g = e["geometry"]
        mid = g[len(g) // 2]
        c = cell(mid[0], mid[1])
        cand = grid.get(c)
        if not cand:
            nomatch += 1
            continue
        if e["oneway"] == any(cand):
            agree += 1
        else:
            mism += 1
    print(f"oneway cross-check vs roads.geojson: agree={agree} mismatch={mism} "
          f"no-nearby-road={nomatch} (coarse, boolean-only)")


def validate_changes(graph):
    if not os.path.exists(CHANGES_FILE):
        print(f"No change layer at {CHANGES_FILE} yet — nothing to validate.")
        return True
    changes = json.load(open(CHANGES_FILE, encoding="utf-8"))
    edge_ids = {e["id"] for e in graph["edges"]}
    node_ids = set(graph["nodes"].keys())
    added_edges, added_nodes = set(), set()
    ok = True
    # pass 1: collect all added/synthetic ids (order-independent)
    for cs in changes.get("changesets", []):
        for m in cs.get("mutations", []):
            if m.get("op") == "addNode":
                added_nodes.add(m["id"])
            elif m.get("op") == "addEdge":
                added_edges.add(m["id"])
                if m.get("bidirectional"):
                    added_edges.add(m["id"] + "_r")
            elif m.get("op") == "setOneway" and m.get("oneway") is False:
                added_edges.add(m.get("edgeId") + "_2w")
    # pass 2: check references
    for cs in changes.get("changesets", []):
        for m in cs.get("mutations", []):
            op = m.get("op")
            if op == "addNode":
                pass
            elif op == "addEdge":
                for k in ("from", "to"):
                    nid = m.get(k)
                    if nid and nid not in node_ids and nid not in added_nodes:
                        print(f"  [{cs['id']}] addEdge {m['id']} references missing node {nid}")
                        ok = False
            elif op in ("removeEdge", "modifyEdge", "setOneway"):
                eid = m.get("edgeId")
                if op == "setOneway" and m.get("oneway") is False:
                    added_edges.add(eid + "_2w")  # synthetic two-way reverse
                if eid not in edge_ids and eid not in added_edges:
                    print(f"  [{cs['id']}] {op} references missing edge {eid}")
                    ok = False
            elif op in ("addTurnRestriction", "removeTurnRestriction"):
                for k in ("fromEdge", "toEdge"):
                    eid = m.get(k)
                    if eid and eid not in edge_ids and eid not in added_edges:
                        print(f"  [{cs['id']}] {op} references missing edge {eid}")
                        ok = False
                vn = m.get("viaNode")
                if vn and vn not in node_ids and vn not in added_nodes:
                    print(f"  [{cs['id']}] {op} references missing node {vn}")
                    ok = False
    print("Change-layer validation:", "OK" if ok else "FAILED")
    return ok


def main():
    refresh = "--refresh" in sys.argv
    overpass = load_overpass(refresh=refresh)
    graph = build_graph(overpass)
    c = graph["meta"]["counts"]
    print(f"Graph: {c['nodes']} nodes, {c['edges']} edges, "
          f"{c['turnRestrictions']} turn restrictions "
          f"({c['skippedRestrictions']} restrictions skipped).")
    if c["nodes"] == 0 or c["edges"] == 0:
        print("ERROR: empty graph — aborting.")
        sys.exit(1)
    cross_validate_oneway(graph)
    json.dump(graph, open(OUTPUT_FILE, "w", encoding="utf-8"),
              ensure_ascii=False, separators=(",", ":"))
    size = os.path.getsize(OUTPUT_FILE)
    print(f"Wrote {OUTPUT_FILE} ({size/1024:.0f} KB)")
    if "--validate-changes" in sys.argv:
        validate_changes(graph)


if __name__ == "__main__":
    main()
