# -*- coding: utf-8 -*-
"""שנת קליטה מ-XPLAN לכל תכנית ב-plans.geojson → data/xplan_plan_dates.json

Feeds the "מכפילי התחדשות עירונית" dashboard (src/app.jsx, openRenewalMultiplierDashboard).

XPLAN MapServer/1 `receiving_date` ("תאריך קבלת תכנית") = the year the plan
file was opened. That date is the ONLY thing taken from XPLAN here: unit counts
always come from Table 5 (the sheet), never from XPLAN's pq_120/delta_120.

Fetches ALL plans (not only renewal) so a plan_type reclassification in the
sheet never leaves a plan without a date. Plans XPLAN doesn't know (rejected /
archived / received but not yet published) simply have no entry.

Empty-clobber guard: a run that gets 0 features back exits 1 without writing.

Usage:  python scripts/fetch_xplan_plan_dates.py
"""
from __future__ import annotations

import json
import os
import sys
import time
import warnings
from datetime import datetime, timezone

warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from xplan_units import _SESSION, XPLAN_PLAN_URL  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLANS = os.path.join(ROOT, "data", "plans.geojson")
OUT = os.path.join(ROOT, "data", "xplan_plan_dates.json")
CHUNK = 80
FIELDS = "pl_number,receiving_date"


def _iso(ms):
    if not ms:
        return None
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d")


def fetch(names):
    out = {}
    for i in range(0, len(names), CHUNK):
        chunk = names[i:i + CHUNK]
        where = "pl_number IN (%s)" % ",".join("'%s'" % n.replace("'", "''") for n in chunk)
        for attempt in range(3):
            try:
                r = _SESSION.post(XPLAN_PLAN_URL, data={
                    "where": where, "outFields": FIELDS,
                    "returnGeometry": "false", "f": "json"}, timeout=90, verify=False)
                r.raise_for_status()
                js = r.json()
                if "error" in js:
                    raise RuntimeError(js["error"])
                break
            except Exception as e:  # noqa: BLE001
                if attempt == 2:
                    raise
                print(f"  retry chunk {i}: {e}")
                time.sleep(5)
        for ft in js.get("features") or []:
            a = ft["attributes"]
            out[a["pl_number"]] = {"recv": _iso(a.get("receiving_date"))}
    return out


def main():
    with open(PLANS, encoding="utf-8") as f:
        g = json.load(f)
    names = sorted({(ft["properties"].get("plan_name") or "").strip()
                    for ft in g["features"]} - {""})
    print(f"{len(names)} plans in plans.geojson")
    data = fetch(names)
    if not data:
        print("XPLAN returned 0 plans — refusing to overwrite", OUT)
        sys.exit(1)
    dated = sum(1 for v in data.values() if v["recv"])
    print(f"XPLAN matched {len(data)}; {dated} with receiving_date")
    payload = {
        "source": "ags.iplan.gov.il PlanningPublic/Xplan/MapServer/1",
        "fetched": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "plans": dict(sorted(data.items())),
    }
    # Skip the write when only the fetch date changed, so the weekly job doesn't commit noise.
    if os.path.exists(OUT):
        with open(OUT, encoding="utf-8") as f:
            old = json.load(f)
        if old.get("plans") == payload["plans"]:
            print("no change")
            return
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    print("wrote", OUT)


if __name__ == "__main__":
    main()
