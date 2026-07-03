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
import json, sys, io, os, datetime, urllib.request

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = "https://gisviewer.jerusalem.muni.il/arcgis/rest/services/{}/MapServer"


def _data_dir():
    for d in [os.path.join(HERE, "data"), os.path.join(HERE, "..", "data")]:
        if os.path.isdir(d):
            return d
    raise SystemExit("data dir not found")


def _get_json(url):
    req = urllib.request.Request(url + "?f=json",
                                 headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception:
        return None


def main():
    year_now = datetime.date.today().year
    quarters, tile = [], None
    for year in range(2025, year_now + 2):
        for mm in ("03", "06", "09", "12"):
            name = f"Ortho{mm}{year}"
            d = _get_json(BASE.format(name))
            if not d or d.get("error") or not d.get("tileInfo"):
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
    if not quarters or tile is None:
        raise SystemExit("no ortho quarters found — aborting (server change?)")
    quarters.sort(key=lambda q: (q["label"].split("/")[1],
                                 q["label"].split("/")[0]), reverse=True)
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
