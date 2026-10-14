"""Build oranim-app/data/soil_barriers.json from the soil-survey extractions.

Source: plan_documents/soil_extractions/*.json — historical soil surveys, soil-
quality surveys and the soil chapter of environmental opinions, read from Mavat
(plans picked via the planning administration's documents layer). Each plan:
status confirmed | suspected | not_found | not_assessed, affected areas, required
actions, whether it's a permit condition, the MoEP position, and any mention of
the plan's public plots.

Keyed by taba (number without 101- and leading zeros), the key the public-lot
popup and the barriers report use, like fuel_barriers.json.
"""
import glob, json, os, sys
from datetime import date

sys.stdout.reconfigure(encoding="utf-8")
ROOT = r"C:\ORANIM"
SRC = os.path.join(ROOT, "plan_documents", "soil_extractions")
OUT = os.path.join(ROOT, "oranim-app", "data", "soil_barriers.json")
KEEP = ("doc_type", "date", "author", "past_uses", "sampling", "status", "areas", "required_actions",
        "permit_condition", "moep", "public_plots_mentioned", "summary")
RANK = {"confirmed": 0, "suspected": 1, "not_found": 2, "not_assessed": 3}


def main():
    ext = {}
    for p in sorted(glob.glob(os.path.join(SRC, "run_*.json"))):
        ext.update(json.load(open(p, encoding="utf-8")))
    queue = json.load(open(os.path.join(ROOT, "plan_documents_queue.json"), encoding="utf-8")).get("soil", {})
    names = {}
    for f in json.load(open(os.path.join(ROOT, "oranim-app", "data", "plans.geojson"), encoding="utf-8"))["features"]:
        p = f["properties"]
        names.setdefault(p.get("plan_name"), p.get("plan_name_he") or "")
    by_taba = {}
    for pl, e in ext.items():
        if not isinstance(e, dict) or "status" not in e:
            continue
        taba = str(int(pl.split("-")[1])) if pl.startswith("101-") else pl
        rec = {k: e[k] for k in KEEP if k in e and e[k] not in (None, "", [])}
        rec["plan"] = pl
        rec["name"] = names.get(pl, "")
        rec["url"] = (queue.get(pl) or {}).get("url", "")
        by_taba[taba] = rec
    by_taba = dict(sorted(by_taba.items(), key=lambda kv: (RANK.get(kv[1]["status"], 9), kv[0])))
    payload = {"generated": date.today().isoformat(),
               "description": "ממצאי זיהום קרקע מסקרי קרקע היסטוריים, סקרי איכות קרקע ופרק הקרקע בחוות דעת סביבתיות של התכניות (מבא\"ת). confirmed = זיהום נמדד; suspected = חשד שטרם נדגם; not_found = נבדק ולא נמצא.",
               "by_taba": by_taba}
    with open(OUT + ".tmp", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
    os.replace(OUT + ".tmp", OUT)
    from collections import Counter
    print(f"wrote {OUT}: {len(by_taba)} plans", dict(Counter(r["status"] for r in by_taba.values())))


if __name__ == "__main__":
    main()
