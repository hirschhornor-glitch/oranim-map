"""
refresh_extra_permits_status.py
-------------------------------
Re-check the status of every non-closed permit in oranim-app/data/extra_permits.json
via the YK JSON API (proc 242700447, same call as scrape_missing_permits_api.py).

extra_permits.json is filled once per permit by several one-off finders
(missing_permits_api / plan_sweep_yk / committee_yk / spatial_2110) and nothing ever
refreshed it: 2026/0245.00 (יהודה הנשיא 1) still carried its 14/05/2026 status in
10/2026. The biweekly scrapers don't see these permits (that's why they are "extra"),
so this pass is the only thing that keeps them current. build_permits_master.py picks
the freshest status across stores by status_date, so updating here is enough.

Writes per entry: status / status_date (when YK has a newer one) + status_checked
(YYYY-MM-DD of the last successful check, read by build_permits_health.py).

Exit codes (silent-success rule): 0 = ok; 1 = the API answered for fewer than half of
the permits checked (down / throttled) — the file is still saved with whatever did
come back, and an ops alert is sent.

Usage:
  py -X utf8 refresh_extra_permits_status.py            # refresh + save
  py -X utf8 refresh_extra_permits_status.py --dry-run  # print changes only
"""
import argparse
import json
import re
import sys
import time
from datetime import date

from scrape_missing_permits_api import call, SYS

EXTRA = r"C:\ORANIM\oranim-app\data\extra_permits.json"
CLOSED_RE = re.compile(r"נסגרה|בוטל")
MIN_OK_SHARE = 0.5


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    data = json.load(open(EXTRA, encoding="utf-8"))
    today = date.today().isoformat()
    entries = [p for plist in (data.get("by_taba") or {}).values() for p in (plist or [])]
    todo = [p for p in entries if not CLOSED_RE.search(p.get("status") or "")]
    print(f"extra_permits: {len(entries)} entries, {len(todo)} not closed → checking YK")

    ok, changed, cache = 0, [], {}
    for i, p in enumerate(todo, 1):
        fn = (p.get("file_number") or "").strip()
        if fn not in cache:
            try:
                d = call(242700447, {"tikNum": fn, "systemCode": SYS})
            except Exception as e:
                print(f"  [{i}/{len(todo)}] {fn}: error {e}")
                d = []
            cache[fn] = d[0] if d else None
            time.sleep(1.0)
        row = cache[fn]
        if not row:
            print(f"  [{i}/{len(todo)}] {fn}: no answer")
            continue
        ok += 1
        st = (row.get("teurStatus") or "").strip()
        sd = (row.get("taarihStatus") or "").strip()
        if st and (st != (p.get("status") or "").strip() or sd != (p.get("status_date") or "").strip()):
            changed.append((fn, p.get("status"), p.get("status_date"), st, sd))
            print(f"  [{i}/{len(todo)}] {fn}: {p.get('status')} {p.get('status_date')} → {st} {sd}")
            p["status"], p["status_date"] = st, sd
        p["status_checked"] = today

    share = ok / len(todo) if todo else 1.0
    print(f"\nanswered {ok}/{len(todo)} ({share:.0%}); {len(changed)} status changes")
    if not args.dry_run:
        with open(EXTRA, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
        print(f"saved {EXTRA}")

    if share < MIN_OK_SHARE:
        msg = (f"refresh_extra_permits_status: YK ענה רק ל-{ok}/{len(todo)} היתרים משלימים "
               f"— ה-API למטה או חוסם. הסטטוסים לא רועננו.")
        print("!! " + msg)
        try:
            from ops_alert import send_alert
            send_alert("ריענון סטטוס היתרים משלימים נכשל", msg + f"\n\n{EXTRA}")
        except Exception as e:
            print(f"  alert failed: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
