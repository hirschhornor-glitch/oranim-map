# -*- coding: utf-8 -*-
"""
detect_occupancy_candidates.py — surface plans that may have reached טופס 4 /
גמר בנייה + אכלוס, for human review. Produces a candidate list; it NEVER writes
occupancy_status.json directly (confirmation is manual, then edit that file or
feed a פיקוח export to build_occupancy_status.py).

Signals (see plan indexed-beaming-cake.md — no single authoritative feed exists):
  1. plans.geojson permit_status containing "גמר בנייה" — strongest existing signal.
  2. all_permits.json / tama38_permits.json request_description mentioning
     "טופס 4 / תעודת גמר / אכלוס" (weak — often prep text like "כהכנה לטופס 4",
     flagged as prep_only).
  3. field_observations.json entries with field_status == "גמר עבודה" (address-level,
     not always mappable to a tracked plan — reported separately).

Output: oranim-app/_occupancy_candidates.json
"""
import json, os, re

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.normpath(os.path.join(HERE, "..", "data"))
OUT = os.path.normpath(os.path.join(HERE, "..", "_occupancy_candidates.json"))

FORM4_RE = re.compile(r"טופס\s*4|תעודת\s*גמר|תעודת\s*אכלוס|אכלוס|איכלוס")
PREP_RE = re.compile(r"כהכנה|הכנה\s+ל|לצורך\s+קבלת|לקראת")
DONE_STATUSES = {"גמר עבודה"}


def load(name):
    p = os.path.join(DATA, name)
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def taba_key(taba):
    try:
        return str(int(re.sub(r"\D", "", str(taba))))
    except (ValueError, TypeError):
        return ""


def scan_permits(bucket, form4_hits):
    """bucket: {taba: {permits:[...]}} -> {taba_key: [ (file_number, snippet, prep_only) ]}"""
    if not bucket:
        return
    for taba, v in bucket.items():
        tk = taba_key(taba)
        if not tk:
            continue
        for p in (v.get("permits") or []):
            desc = str(p.get("request_description") or "")
            if FORM4_RE.search(desc):
                form4_hits.setdefault(tk, []).append({
                    "file_number": p.get("file_number", ""),
                    "snippet": desc[:120],
                    "prep_only": bool(PREP_RE.search(desc)),
                    "status": p.get("status", ""),
                })


def main():
    plans = load("plans.geojson")
    all_p = load("all_permits.json")
    tama_p = load("tama38_permits.json")
    field = load("field_observations.json")
    occ = load("occupancy_status.json")
    already = set((occ or {}).get("by_plan", {}).keys())

    form4_hits = {}
    scan_permits(all_p, form4_hits)
    scan_permits(tama_p, form4_hits)

    plan_candidates = []
    for f in (plans or {}).get("features", []):
        pp = f.get("properties", {})
        plan_name = str(pp.get("plan_name") or pp.get("PLAN_NAME") or "").strip()
        if not plan_name or plan_name in already:
            continue
        tk = taba_key(pp.get("taba") or pp.get("TABA"))
        signals = []
        pstat = str(pp.get("permit_status") or "").strip()
        if "גמר" in pstat:
            signals.append("permit_status: " + pstat)
        hits = form4_hits.get(tk, [])
        issued = [h for h in hits if not h["prep_only"]]
        prep = [h for h in hits if h["prep_only"]]
        if issued:
            signals.append("permit desc (טופס 4 issued-context): " + issued[0]["snippet"])
        elif prep:
            signals.append("permit desc (prep only): " + prep[0]["snippet"])
        if not signals:
            continue
        plan_candidates.append({
            "plan_name": plan_name,
            "plan_name_he": pp.get("plan_name_he", ""),
            "plan_summary": pp.get("plan_summary", ""),
            "status_mavat": pp.get("status_mavat", ""),
            "permit_status": pstat,
            "stage": pp.get("stage", ""),
            "signals": signals,
            "strength": "high" if ("גמר" in pstat) else ("medium" if issued else "low"),
        })

    # field-observation completions (address-level; not auto-mapped to a plan)
    field_completions = []
    for fn, rec in ((field or {}).get("by_file", {})).items():
        if str(rec.get("field_status") or "").strip() in DONE_STATUSES:
            field_completions.append({
                "file_number": fn,
                "neighborhood": rec.get("neighborhood", ""),
                "street": rec.get("street", ""),
                "house": rec.get("house", ""),
                "field_status": rec.get("field_status", ""),
            })

    strength_rank = {"high": 0, "medium": 1, "low": 2}
    plan_candidates.sort(key=lambda c: strength_rank.get(c["strength"], 3))

    out = {
        "generated_from": "plans.geojson + all_permits.json + tama38_permits.json + field_observations.json",
        "note": "Review list only — confirm manually, then edit occupancy_status.json.",
        "already_marked": sorted(already),
        "plan_candidates": plan_candidates,
        "field_completions": field_completions,
    }
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)

    print(f"wrote {OUT}")
    print(f"  plan candidates: {len(plan_candidates)} "
          f"(high:{sum(1 for c in plan_candidates if c['strength']=='high')}, "
          f"medium:{sum(1 for c in plan_candidates if c['strength']=='medium')}, "
          f"low:{sum(1 for c in plan_candidates if c['strength']=='low')})")
    print(f"  field-observation completions (גמר עבודה): {len(field_completions)}")
    for c in plan_candidates[:8]:
        print(f"    [{c['strength']}] {c['plan_name']} {c['plan_summary']} — {c['signals'][0][:70]}")


if __name__ == "__main__":
    main()
