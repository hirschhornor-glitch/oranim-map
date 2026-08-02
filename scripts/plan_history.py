# -*- coding: utf-8 -*-
"""
plan_history.py — review what changed on a plan, and when.

Primary source: data/plan_changelog.jsonl (one JSON object per field change,
written by update_plans.py on every GS->geojson mirror). This is the durable,
field-level audit trail.

Fallback: for history predating the changelog, reconstruct field values from
git history of the single-line data/plans.geojson.

Usage:
    python plan_history.py <taba>       # full change history of one plan
    python plan_history.py --recent 40  # last N field changes across all plans
    python plan_history.py <taba> --git # force git-history reconstruction
"""
import os
import sys
import json
import subprocess

sys.stdout.reconfigure(encoding="utf-8")

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)                      # oranim-app/
CHANGELOG = os.path.join(REPO, "data", "plan_changelog.jsonl")
GEOJSON_REL = "data/plans.geojson"

# fields worth reconstructing from git when the changelog doesn't reach back
GIT_FIELDS = ["status_mavat", "mavat_date", "units_total", "units_in",
              "units_add", "commerce_out", "hafrash_sqm", "hafrash_prg",
              "shavatz_out_sqm", "resident_shared", "floors_max", "High"]


def load_events():
    if not os.path.exists(CHANGELOG):
        return []
    out = []
    with open(CHANGELOG, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return out


def show_plan(taba):
    evs = [e for e in load_events() if str(e.get("taba")) == str(taba)]
    if not evs:
        print(f"(no changelog events for {taba} — trying git reconstruction)\n")
        show_git(taba)
        return
    print(f"### change history — taba {taba}  ({len(evs)} field changes)\n")
    last_ts = None
    for e in evs:
        if e.get("ts") != last_ts:
            print(f"\n{e.get('ts')}   [{e.get('status','')}]")
            last_ts = e.get("ts")
        print(f"   {e['field']}: {e.get('old')!r}  ->  {e.get('new')!r}")


def show_recent(n):
    evs = load_events()[-n:]
    if not evs:
        print("(changelog empty)")
        return
    print(f"### last {len(evs)} field changes\n")
    for e in evs:
        print(f"{e.get('ts')}  {e.get('plan_name','')}  {e['field']}: "
              f"{e.get('old')!r} -> {e.get('new')!r}")


def _git(args):
    return subprocess.run(["git"] + args, cwd=REPO, capture_output=True,
                          text=True, encoding="utf-8").stdout


def show_git(taba, n=40):
    log = _git(["log", "-n", str(n), "--format=%H|%ad|%s", "--date=short",
                "--", GEOJSON_REL]).strip().splitlines()
    prev = None
    for line in log[::-1]:
        h, date, subj = line.split("|", 2)
        blob = _git(["show", f"{h}:{GEOJSON_REL}"])
        if not blob:
            continue
        try:
            gj = json.loads(blob)
        except json.JSONDecodeError:
            continue
        rec = None
        for feat in gj["features"]:
            if str(feat["properties"].get("taba")) == str(taba):
                rec = {k: feat["properties"].get(k) for k in GIT_FIELDS}
                break
        if rec is None or (prev is not None and rec == prev):
            continue
        changed = [k for k in GIT_FIELDS if prev is None or rec.get(k) != prev.get(k)]
        print(f"\n{date}  {h[:8]}  {subj[:60]}")
        for k in changed:
            old = "" if prev is None else prev.get(k)
            print(f"   {k}: {old!r}  ->  {rec.get(k)!r}")
        prev = rec


def main():
    args = sys.argv[1:]
    if not args or args[0] == "--recent":
        n = int(args[1]) if len(args) > 1 else 30
        show_recent(n)
    elif "--git" in args:
        show_git(args[0])
    else:
        show_plan(args[0])


if __name__ == "__main__":
    main()
