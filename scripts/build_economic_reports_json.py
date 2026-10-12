"""Build oranim-app/data/economic_reports.json from the economic-report extractions.

Inputs:
  plan_documents/economic_extractions/*.json   per plan: {"versions": [...], ...}
      (written by the extraction agents; figures verified against page images)
  plan_documents/<plan>/economic/api.json      Mavat SV4/1 response: rsQuantities
      gives the APPROVED residential units (existing + added = total)
  oranim-app/data/plans.geojson                fallback units_in / units_total

Only plans that are in plans.geojson (the quarter) are written; the citywide
pilot plans outside it stay in the extraction files only.

Per plan the app gets the latest version's headline figures plus a compact
version history, and the approved multiplier so the report can show the gap
between what the economic report says is required and what was approved.
"""
import glob, json, os, re, sys
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")
ROOT = r"C:\ORANIM"
DOCS = os.path.join(ROOT, "plan_documents")
REPO = os.path.join(ROOT, "oranim-app")
OUT_REL = "data/economic_reports.json"

KEEP = ("label", "date", "appraiser", "unit_ratio", "area_ratio", "existing_units", "new_units",
        "profit_pct_cost", "profit_pct_sales", "profit_threshold", "sale_price_sqm",
        "construction_cost_sqm", "tenant_added_sqm", "tenant_package", "rent_support",
        "betterment_levy", "existing_avg_sqm", "commerce_sqm", "notes")


def load(p, d=None):
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return d


def approved_units(pl, props):
    """(existing, total, source). Mavat quantities first, our data as fallback."""
    api = load(os.path.join(DOCS, pl, "economic", "api.json"), {}) or {}
    for q in api.get("rsQuantities") or []:
        if q.get("QUANTITY_DESC") == 'מגורים (יח"ד)':
            try:
                ex = int(str(q.get("AUTHORISED_QUANTITY") or "").strip("+") or 0)
                tot = int(float(q.get("IMPLEMENTATION") or 0))
                if ex and tot:
                    return ex, tot, "mavat"
            except ValueError:
                pass
    try:
        ex, tot = float(props.get("units_in") or 0), float(props.get("units_total") or 0)
        if ex and tot:
            return int(ex), int(tot), "gs"
    except (TypeError, ValueError):
        pass
    return None, None, None


def is_main(v):
    # A maintenance-fund supplement or a no-harm opinion carries no unit ratio;
    # the headline must come from the Standard-21 report itself.
    return v.get("unit_ratio") is not None or v.get("profit_pct_cost") is not None


def main():
    plans = {}
    for f in load(os.path.join(REPO, "data", "plans.geojson"), {"features": []})["features"]:
        p = f["properties"]
        plans.setdefault(p.get("plan_name"), p)
    ext = {}
    for path in sorted(glob.glob(os.path.join(DOCS, "economic_extractions", "*.json"))):
        ext.update(load(path, {}) or {})
    queue = load(os.path.join(ROOT, "plan_documents_queue.json"), {}).get("economic", {})

    out = {}
    for pl, rec in sorted(ext.items()):
        if pl not in plans:
            continue
        vers = [v for v in rec.get("versions") or [] if isinstance(v, dict)]
        main_vers = [v for v in vers if is_main(v)]
        if not main_vers:
            continue
        head = main_vers[-1]
        ex, tot, src = approved_units(pl, plans[pl])
        e = {k: head[k] for k in KEEP if k in head and head[k] not in (None, "", [])}
        e["approved_existing"], e["approved_total"], e["approved_source"] = ex, tot, src
        if ex and tot:
            e["approved_ratio"] = round(tot / ex, 2)
        if e.get("approved_ratio") and head.get("unit_ratio"):
            e["gap_pct"] = round((e["approved_ratio"] / head["unit_ratio"] - 1) * 100, 1)
        e["versions"] = [{k: v[k] for k in ("label", "date", "unit_ratio", "profit_pct_cost", "sale_price_sqm",
                                            "tenant_added_sqm") if k in v} for v in main_vers]
        if rec.get("comment"):
            e["comment"] = rec["comment"]
        q = queue.get(pl) or {}
        e["url"] = q.get("url") or plans[pl].get("mavat_url") or ""
        out[pl] = {k: v for k, v in e.items() if v is not None}

    payload = {"_meta": {"generated_at": datetime.now().isoformat(timespec="seconds"),
                         "description": "חוות דעת כלכליות (תקן 21) לתכניות ברובע: המכפיל הנדרש לפי הדוח מול המכפיל המאושר, רווח יזמי, הנחות מחיר ותמורה לדיירים.",
                         "plans": len(out),
                         "gap_pct": "מכפיל מאושר ÷ מכפיל נדרש − 1. שלילי = אושר פחות ממה שהדוח קבע כנדרש."},
               "plans": out}
    dst = os.path.join(REPO, "data", "economic_reports.json")
    with open(dst + ".tmp", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
    os.replace(dst + ".tmp", dst)
    print(f"wrote {dst}: {len(out)} plans")
    for pl, e in out.items():
        print(f"  {pl}: required {e.get('unit_ratio')} approved {e.get('approved_ratio')} ({e.get('approved_source')}) "
              f"gap {e.get('gap_pct')}% profit {e.get('profit_pct_cost')}")


if __name__ == "__main__":
    main()
