"""
build_extra_permits.py
----------------------
From missing_permits_api.json (output of scrape_missing_permits_api.py), build
oranim-app/data/extra_permits.json — permits that exist on plans we track but were
missed by the taba-driven scraper. Merged into __allPermits at app load (side-file,
survives re-scrapes), keyed by taba in the same shape as all_permits.json.

Best-taba resolution: when a permit matches several of our plans, pick the largest
numeric Mavat code — the specific recent plan that authorized it — which drops the
broad old corridor plan 178129 (מתחם הרכבת) whenever a specific match co-occurs.
"""
import json

SRC = r"C:\ORANIM\missing_permits_api.json"
OUT = r"C:\ORANIM\oranim-app\data\extra_permits.json"
SRC_DATE = "2026-06-21"


def best_taba(matches):
    # largest numeric code = most specific/newest plan
    return max(matches, key=lambda t: int(t)) if matches else None


def main():
    rows = json.load(open(SRC, encoding="utf-8"))
    by_taba = {}
    assigned = skipped = 0
    for r in rows:
        matches = r.get("matched_tabas") or []
        if not matches or not r.get("status"):
            skipped += 1
            continue
        taba = best_taba(matches)
        permit = {
            "file_number": r["file_number"],
            "status": r.get("status", ""),
            "status_date": r.get("status_date", ""),
            "request_type": r.get("request_type", ""),
            "request_description": r.get("request_description", ""),
            "address": r.get("address", ""),
            "source": "missing_permits_api",
            "source_date": SRC_DATE,
            "all_matched_tabas": matches,
        }
        by_taba.setdefault(taba, []).append(permit)
        assigned += 1

    out = {"source": "YK API — היתרים שהסקרייפר פספס (טבלת טרם החלו בנייה)",
           "source_date": SRC_DATE, "count": assigned, "by_taba": by_taba}
    json.dump(out, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"wrote {OUT}")
    print(f"  {assigned} permits across {len(by_taba)} plans; {skipped} skipped (no taba match / no status)")
    for taba, ps in sorted(by_taba.items()):
        print(f"  taba {taba}: {', '.join(p['file_number'] for p in ps)}")


if __name__ == "__main__":
    main()
