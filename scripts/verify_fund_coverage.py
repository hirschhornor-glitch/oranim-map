"""
verify_fund_coverage.py — QA pass over the tower plans that were NOT flagged as
having a קרן תחזוקה, to catch false negatives.

For every non-rejected TOWER plan (>13 floors) that is NOT in maintenance_fund.json,
it reads the horaot text and checks whether the fund term appears at all. Output
buckets:
  MISS?      — horaot mentions "קרן תחזוק"/"תחזוקה ארוכת" but the plan was not
               flagged → REVIEW (possible missed fund).
  NO_HORAOT  — no local horaot PDF → cannot verify automatically (manual check).
  CLEAN      — no fund term anywhere in the horaot → confirmed no fund.

Run:  py scripts/verify_fund_coverage.py
"""
import io, json, os, re, sys
sys.path.insert(0, os.path.dirname(__file__))
from extract_maintenance_fund import read_horaot_text, HORAOT_DIR, PLANS_GEOJSON, load_table5_conditional

FUND_TERM = re.compile(r"קרן\s+ה?תחזוק|תחזוקה\s+ארוכת")
COND_TERM = re.compile(r'זכויות\s+"?מותנות"?|יח"?ד\s+מותנות|מותנ(?:ית|ה)\s+בהקמת')
REJECT = re.compile(r"נגנז|נדחת")


def floors(p, t5):
    ln = 0
    try:
        v = float(p.get("level_num"))
        if 0 < v < 200:
            ln = v
    except (TypeError, ValueError):
        pass
    hf = 0
    try:
        h = float(p.get("High"))
        if h > 0:
            hf = round(h / 3.1)
    except (TypeError, ValueError):
        pass
    t5f = t5.get(str(p.get("taba") or ""), {}).get("max_floors") or 0
    return max(ln, hf, t5f or 0)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    feats = json.load(io.open(PLANS_GEOJSON, encoding="utf-8"))["features"]
    funds = json.load(io.open(os.path.join(os.path.dirname(__file__), "..", "data",
                       "maintenance_fund.json"), encoding="utf-8"))
    t5 = load_table5_conditional()

    seen = set()
    towers_non_fund = []
    for f in feats:
        p = f["properties"]; pn = p.get("plan_name")
        if not pn or pn in seen:
            continue
        seen.add(pn)
        if REJECT.search((p.get("status_mavat") or "")):
            continue
        if floors(p, t5) <= 13:
            continue
        if pn in funds:                       # already a confirmed fund
            continue
        towers_non_fund.append((pn, p))

    review, no_horaot, clean = [], [], []
    for pn, p in towers_non_fund:
        path = os.path.join(HORAOT_DIR, pn + ".pdf")
        if not os.path.exists(path):
            no_horaot.append(pn); continue
        try:
            txt = read_horaot_text(path)
        except Exception as e:
            review.append((pn, f"ERROR {e}")); continue
        if FUND_TERM.search(txt):
            m = FUND_TERM.search(txt)
            ctx = re.sub(r"\s+", " ", txt[max(0, m.start()-60):m.start()+120])
            has_cond = bool(COND_TERM.search(txt))
            review.append((pn, f"cond={has_cond} :: {ctx}"))
        else:
            clean.append(pn)

    out = io.open(os.path.join(os.path.dirname(__file__), "..",
                  "_fund_coverage_review.txt"), "w", encoding="utf-8")
    out.write(f"Tower plans (>13 floors, non-rejected) NOT flagged as fund: {len(towers_non_fund)}\n")
    out.write(f"  CLEAN (no fund term in horaot): {len(clean)}\n")
    out.write(f"  NO_HORAOT (cannot auto-verify): {len(no_horaot)}\n")
    out.write(f"  REVIEW (horaot mentions fund term!): {len(review)}\n\n")
    out.write("=== REVIEW — possible missed funds ===\n")
    for pn, ctx in review:
        he = next((f['properties'].get('plan_name_he','') for f in feats
                   if f['properties'].get('plan_name') == pn), '')
        out.write(f"{pn} | {he[:45]}\n    {ctx[:200]}\n")
    out.write("\n=== NO_HORAOT — manual check needed ===\n")
    for pn in no_horaot:
        he = next((f['properties'].get('plan_name_he','') for f in feats
                   if f['properties'].get('plan_name') == pn), '')
        out.write(f"{pn} | {he[:50]}\n")
    out.write("\n=== CLEAN ===\n" + ", ".join(clean) + "\n")
    out.close()
    print(f"tower non-fund: {len(towers_non_fund)} | CLEAN {len(clean)} | "
          f"NO_HORAOT {len(no_horaot)} | REVIEW {len(review)}")
    print("-> _fund_coverage_review.txt")


if __name__ == "__main__":
    main()
