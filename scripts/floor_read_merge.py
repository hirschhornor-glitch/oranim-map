"""
floor_read_merge.py — finalize a Claude-vision floor read.

After the vision agents have written their per-plan readings to
_floor_agent_out_*.json files (each a {taba: {source_doc, source_type,
confidence, allocations:[...]}} dict), this:
  1. merges them into _floor_alloc_staging.json,
  2. runs add_floor_alloc.py to fold them into floor_allocations.json (+ app copy),
  3. removes the handled tabas from floor_read_queue.json.

Usage:
  py floor_read_merge.py _floor_agent_out_*.json
  py floor_read_merge.py            # globs _floor_agent_out_*.json in cwd
"""
import glob
import io
import json
import subprocess
import sys
from pathlib import Path

import enrich_queue as eq

ROOT = Path(r"C:\ORANIM")
STAGING = ROOT / "_floor_alloc_staging.json"


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    files = sys.argv[1:] or glob.glob(str(ROOT / "_floor_agent_out_*.json"))
    if not files:
        print("no _floor_agent_out_*.json files found")
        return
    merged = {}
    for fp in files:
        try:
            d = json.load(io.open(fp, encoding="utf-8"))
        except Exception as e:
            print(f"  skip {fp}: {e}")
            continue
        for taba, entry in d.items():
            merged[eq.norm_taba(taba)] = entry
    if not merged:
        print("nothing to merge")
        return
    json.dump(merged, io.open(STAGING, "w", encoding="utf-8"),
              ensure_ascii=False, indent=2)
    print(f"wrote {len(merged)} entries -> {STAGING.name}")

    r = subprocess.run([sys.executable, str(ROOT / "add_floor_alloc.py"), str(STAGING)],
                       cwd=str(ROOT), text=True, encoding="utf-8", errors="replace")
    if r.returncode != 0:
        print("add_floor_alloc.py failed — leaving queue intact")
        return

    remaining = eq.dequeue_floor(list(merged.keys()))
    print(f"floor_read_queue.json: {remaining} plan(s) remain")


if __name__ == "__main__":
    main()
