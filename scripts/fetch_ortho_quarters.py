# -*- coding: utf-8 -*-
"""
fetch_ortho_quarters.py — גילוי שירותי האורתופוטו הרבעוניים של העירייה.

The muni publishes a cached tile service per quarter (Ortho032025,
Ortho062025, …) on gisviewer, EPSG:2039, 12 LODs down to ~3.3cm/px.
This probes candidate service names (quarters 03/06/09/12 for every year
from 2025 through next year) and records the live ones + the shared tile
scheme, so the in-app viewer picks up NEW quarters automatically — nothing
is stored locally, the tiles stay on the muni server.

Output: data/ortho_quarters.json
  { meta, tile: {origin, resolutions, extent}, quarters: [{id, label}] }
Newest quarter first. Re-run via the weekly cron. Idempotent (skip-write).
"""
import json, sys, io, os, datetime, urllib.request, urllib.error

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = "https://gisviewer.jerusalem.muni.il/arcgis/rest/services/{}/MapServer"


def _data_dir():
    for d in [os.path.join(HERE, "data"), os.path.join(HERE, "..", "data")]:
        if os.path.isdir(d):
            return d
    raise SystemExit("data dir not found")


# The bare `except: return None` made a WAF 403 / timeout look exactly like "this
# quarter does not exist", so a blocked run silently dropped quarters (2026-10-06
# audit). Now only a genuine not-found (HTTP 404, or ArcGIS's "service not found"
# error payload) means "absent"; anything else aborts with exit 2 and keeps the file.
STALE_DAYS = 270   # ~9 months: newest quarter older than this -> WARNING line


class ProbeError(Exception):
    pass


def _is_not_found_payload(err):
    msg = str(err.get("message") or "").lower()
    return err.get("code") == 404 or "not found" in msg


def _get_json(url):
    """Returns the service JSON, or None if the service genuinely doesn't exist.
    Raises ProbeError on network/HTTP/WAF errors."""
    req = urllib.request.Request(url + "?f=json",
                                 headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            d = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise ProbeError(f"HTTP {e.code}")
    except Exception as e:
        raise ProbeError(f"{type(e).__name__}: {str(e)[:100]}")
    if isinstance(d, dict) and d.get("error"):
        if _is_not_found_payload(d["error"]):
            return None
        raise ProbeError(f"ArcGIS error {d['error']}")
    return d


def main():
    year_now = datetime.date.today().year
    quarters, tile, probe_errors = [], None, []
    for year in range(2025, year_now + 2):
        for mm in ("03", "06", "09", "12"):
            name = f"Ortho{mm}{year}"
            try:
                d = _get_json(BASE.format(name))
            except ProbeError as e:
                print(f"probe FAILED {name}: {e}")
                probe_errors.append(name)
                continue
            if not d or not d.get("tileInfo"):
                continue
            quarters.append({"id": name, "label": f"{mm}/{year}"})
            ti = d["tileInfo"]
            fe = d.get("fullExtent") or {}
            tile = {  # newest probe wins — the scheme is shared
                "origin": [ti["origin"]["x"], ti["origin"]["y"]],
                "resolutions": [l["resolution"] for l in ti["lods"]],
                "extent": [fe.get("xmin"), fe.get("ymin"),
                           fe.get("xmax"), fe.get("ymax")],
            }
            print("found", name)
    if probe_errors:
        print(f"ERROR: {len(probe_errors)} probe(s) failed (not a 'not found'): "
              f"{probe_errors} — aborting, keeping existing file")
        sys.exit(2)
    if not quarters or tile is None:
        raise SystemExit("no ortho quarters found — aborting (server change?)")
    quarters.sort(key=lambda q: (q["label"].split("/")[1],
                                 q["label"].split("/")[0]), reverse=True)
    # Staleness: the muni stopped publishing for months (newest Ortho122025 still in
    # Oct 2026) and nothing said so. Warn (exit stays 0); weekly_muni_gis echoes it.
    mm, yyyy = quarters[0]["label"].split("/")
    age = (datetime.date.today() - datetime.date(int(yyyy), int(mm), 1)).days
    if age > STALE_DAYS:
        print(f"WARNING: newest ortho quarter {quarters[0]['id']} is ~{age // 30} months old "
              f"(> {STALE_DAYS // 30}) — muni may have stopped publishing new quarters")
    result = {"meta": {"fetched_at": datetime.date.today().isoformat(),
                       "source": "gisviewer Ortho* MapServer catalog"},
              "tile": tile, "quarters": quarters}
    dest = os.path.join(_data_dir(), "ortho_quarters.json")
    if os.path.exists(dest):
        try:
            with open(dest, encoding="utf-8") as f:
                old = json.load(f)
            if old.get("quarters") == quarters and old.get("tile") == tile:
                print("no content change — keeping existing file")
                return
        except Exception:
            pass
    with open(dest, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, separators=(",", ":"))
    print(f"wrote {len(quarters)} quarters -> {dest}")


if __name__ == "__main__":
    main()
