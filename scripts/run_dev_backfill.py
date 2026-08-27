"""
run_dev_backfill.py — gentle DAILY backfill of TAMA 38 developer names for the
~229 existing permits, one small batch at a time so YK's per-IP throttle never
trips (the mistake that burned us: many concurrent callers). Single process, no
loop, ~20 tiks/run. Data-only push (no APP_VERSION bump → no CI rebuild); the
weekly §149 task busts the cache. Self-terminating: once the backlog is empty it
just reports "complete".

Scheduled: Oranim_Tama38_Dev_Backfill (daily 04:30). Related: [[project_developers_report]].
"""
import json, subprocess, sys
from pathlib import Path

sys.path.insert(0, r"C:\ORANIM")
from git_sync import commit_and_push_after_write

ROOT = Path(r"C:\ORANIM")
DATA = ROOT / "oranim-app" / "data"
DEVS = DATA / "tama38_developers.json"
PYEXE = sys.executable
BATCH = 20


def sh(*args, cwd=None):
    return subprocess.run(args, capture_output=True, text=True, encoding="utf-8", cwd=cwd)


def remaining():
    import re
    geo = json.load(open(DATA / "tama38.geojson", encoding="utf-8"))
    tiks = [f["properties"]["tik"] for f in geo["features"]
            if re.match(r'\d{4}/\d+', str(f["properties"].get("tik") or ""))]
    by = json.load(open(DEVS, encoding="utf-8")).get("by_tik", {}) if DEVS.exists() else {}
    return sum(1 for t in tiks if t not in by), len(tiks)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    before, total = remaining()
    print(f"backfill: {total - before}/{total} enriched, {before} remaining")
    if before == 0:
        print("backfill complete — noop."); return

    # one gentle batch
    r = sh(PYEXE, "-X", "utf8", str(ROOT / "enrich_tama38_developers.py"), f"--limit={BATCH}")
    print(r.stdout[-600:] if r.stdout else "", (r.stderr[-300:] if r.stderr else ""))

    after, _ = remaining()
    got = before - after
    print(f"this run enriched {got}; {after} remaining")
    if got == 0:
        print("YK throttled this run — will retry tomorrow."); return

    # minahak + units on the file
    sh(PYEXE, "-X", "utf8", str(ROOT / "add_tama38_minahak.py"))

    # data-only push (no version bump -> no CI rebuild; weekly §149 task busts cache).
    # Goes through git_sync, never raw git: a rebase conflicting on a single-line
    # JSON file left this repo mid-rebase for three days (2026-08-23) because the
    # bare subprocess git calls here never checked their exit code. git_sync aborts
    # the rebase, keeps the data in the working tree, and reports False instead.
    msg = f"תמ\"א 38: backfill יזמים +{got} ({total - after}/{total})\n\nCo-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
    if commit_and_push_after_write("data/tama38_developers.json", msg):
        print("push: ok")
    else:
        print("push: FAILED — data kept in working tree, retries next run")
    if after == 0:
        print("★ backfill DONE — all 229 developers enriched.")


if __name__ == "__main__":
    main()
