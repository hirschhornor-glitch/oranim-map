# -*- coding: utf-8 -*-
"""
fetch_transit_bus_jlm.py — תחנות אוטובוס בירושלים (Open Bus / GTFS)

מקור: Open Bus "Stride" API של הסדנא לידע ציבורי — mirror של ה-GTFS של משרד
התחבורה. https://open-bus-stride-api.hasadna.org.il (API ציבורי, נגיש גם
מ-GitHub Actions — בניגוד ל-gisviewer העירוני החסום, [[project_muni_gis_local_fetch]]).

מושך את כל תחנות האוטובוס בירושלים (route_type=3 בלבד — רק"ל/רכבת לא נכללים)
דרך /gtfs_ride_stops, שמחזיר גם את התחנה וגם את הקו המשרת אותה. חובה חלון
arrival_time (סינון city לבד → 400), לכן דוגמים כמה חלונות לאורך יום שירות
ומאחדים ל-(תחנה → קווים). כותב data/transit_bus_stops_jlm.geojson דחוס.

מיועד לריצה רבעונית (ראה .github/workflows/update_transit_bus.yml).
"""
import datetime as _dt
import io
import json
import os
import sys
import time
import urllib.parse
import urllib.request

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

BASE = "https://open-bus-stride-api.hasadna.org.il"
CITY = "ירושלים"
PAGE = 1000
TIMEOUT = 60
RETRIES = 4

# חלונות UTC (ישראל = UTC+3): בוקר מוקדם, שיא בוקר, אמצע-בוקר, צהריים, אחה"צ,
# שיא אחה"צ, ערב. האיחוד מכסה כמעט את כל רשת הקווים ביום.
WINDOWS_UTC = [(3, 4), (5, 6), (8, 9), (11, 12), (14, 15), (17, 18), (20, 21)]


def _find_data_dir():
    here = os.path.dirname(os.path.abspath(__file__))
    for d in (os.path.join(here, "..", "data"), os.path.join(here, "data"),
              os.path.join(here, "oranim-app", "data"),
              r"C:\ORANIM\oranim-app\data"):
        if os.path.isfile(os.path.join(d, "plans.geojson")):
            return os.path.abspath(d)
    raise SystemExit("could not locate data/ dir (plans.geojson) near " + here)


OUT = os.path.join(_find_data_dir(), "transit_bus_stops_jlm.geojson")


def pick_service_date():
    """יום שירות מלא אחרון (א'-ה' בישראל), לפחות יומיים אחורה — כדי שה-mirror
    כבר יכיל אותו ולא ליפול על שישי/שבת (שירות חלקי)."""
    d = _dt.date.today() - _dt.timedelta(days=2)
    while d.weekday() in (4, 5):        # 4=Fri, 5=Sat
        d -= _dt.timedelta(days=1)
    return d.isoformat()


def _get(path, **params):
    url = BASE + path + "?" + urllib.parse.urlencode(params)
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "oranim-transit/1.0"})
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                return json.load(r)
        except Exception as e:          # noqa: BLE001
            last = e
            time.sleep(2 * attempt)
    raise RuntimeError(f"request failed after {RETRIES}: {last}\n{url}")


def _natural_line_key(s):
    s = str(s)
    return (0, int(s)) if s.isdigit() else (1, s)


def fetch_bus_stops(date):
    stops = {}
    for h0, h1 in WINDOWS_UTC:
        win = {"arrival_time_from": f"{date}T{h0:02d}:00:00Z",
               "arrival_time_to":   f"{date}T{h1:02d}:00:00Z"}
        offset = 0
        while True:
            rows = _get("/gtfs_ride_stops/list", limit=PAGE, offset=offset,
                        order_by="id asc",
                        **{"gtfs_stop__city": CITY, "gtfs_route__route_type": "3", **win})
            if not rows:
                break
            for r in rows:
                code = r.get("gtfs_stop__code")
                if code is None:
                    continue
                st = stops.get(code)
                if st is None:
                    st = stops[code] = {"name": r.get("gtfs_stop__name"),
                                        "lat": r.get("gtfs_stop__lat"),
                                        "lon": r.get("gtfs_stop__lon"),
                                        "lines": set()}
                line = r.get("gtfs_route__route_short_name")
                if line:
                    st["lines"].add(str(line).strip())
            offset += len(rows)
            if len(rows) < PAGE:
                break
        print(f"  window {h0:02d}-{h1:02d}Z -> {len(stops)} stops so far")
    return stops


def to_geojson(stops):
    feats = []
    for code, st in stops.items():
        if st["lat"] is None or st["lon"] is None:
            continue
        lines = sorted(st["lines"], key=_natural_line_key)
        feats.append({"type": "Feature",
                      "geometry": {"type": "Point", "coordinates": [st["lon"], st["lat"]]},
                      "properties": {"code": code, "name": st["name"],
                                     "lines": ", ".join(lines), "n_lines": len(lines)}})
    feats.sort(key=lambda f: -f["properties"]["n_lines"])
    return {"type": "FeatureCollection", "features": feats}


def main():
    date = pick_service_date()
    print(f"Fetching Jerusalem bus stops from Stride (service date {date})…")
    stops = fetch_bus_stops(date)
    if not stops:
        raise SystemExit("no stops fetched — aborting (keeping existing file)")
    fc = to_geojson(stops)
    # דחוס (שורה אחת) — כמו plans.geojson ([[reference_ci_workflow_conventions]]).
    with open(OUT, "w", encoding="utf-8") as fh:
        json.dump(fc, fh, ensure_ascii=False, separators=(",", ":"))
    n = len(fc["features"])
    lines = {l for st in stops.values() for l in st["lines"]}
    print(f"Wrote {n} bus stops, {len(lines)} distinct lines → {OUT}")


if __name__ == "__main__":
    main()
