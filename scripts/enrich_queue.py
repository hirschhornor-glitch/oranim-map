"""
enrich_queue.py — shared work-queue for the deep-enrichment pipeline.

Two triggers feed the queue:
  - detect_new_plans.py     (a plan appeared in XPLAN that we didn't have)
  - update_mavat_status.py  (an existing plan changed Mavat status)
Both call enqueue(); enrich_changed_plans.py later drains it and runs the
staging / hafrash-type / floor-allocation enrichment per plan.

Files (local root, gitignored — transient operational state, like the
extract_execution_staging .done/.cur checkpoints):
  pending_enrichment.json   {taba: {taba, agam_id, plan_name, reason, queued_at}}
  floor_read_queue.json     {taba: {taba, plan_name, png_paths, source_hint, queued_at}}
  hafrash_read_queue.json   {taba: {taba, plan_name, hafrash_sqm, candidates, queued_at}}

Keyed by *bare* taba so re-queuing the same plan de-dups naturally. Writes are
atomic (.tmp -> os.replace) so a killed trigger can never corrupt the queue.
"""
import io
import json
import os
import re

ROOT = r"C:\ORANIM"
QUEUE_PATH = os.path.join(ROOT, "pending_enrichment.json")
FLOOR_QUEUE_PATH = os.path.join(ROOT, "floor_read_queue.json")
HAFRASH_QUEUE_PATH = os.path.join(ROOT, "hafrash_read_queue.json")


def norm_taba(t):
    """'101-0511923' / '0511923' / 511923 -> '511923'."""
    return re.sub(r"^101-?0*", "", str(t or "")).lstrip("0")


def _load(path):
    if os.path.exists(path):
        try:
            return json.load(io.open(path, encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _atomic_write(path, data):
    tmp = path + ".tmp"
    with io.open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def load_queue(path=QUEUE_PATH):
    return _load(path)


def enqueue(items, path=QUEUE_PATH):
    """items: iterable of dicts with at least 'taba'. Merges by taba; non-empty
    new values overwrite, blanks never clobber an existing value. Returns the
    number of items merged in."""
    q = _load(path)
    n = 0
    for it in items or []:
        tb = norm_taba(it.get("taba"))
        if not tb:
            continue
        merged = dict(q.get(tb, {}))
        merged.update({k: v for k, v in it.items() if v not in (None, "")})
        merged["taba"] = tb
        q[tb] = merged
        n += 1
    _atomic_write(path, q)
    return n


def dequeue(tabas, path=QUEUE_PATH):
    """Remove the given tabas from the queue. Returns remaining count."""
    q = _load(path)
    for tb in tabas:
        q.pop(norm_taba(tb), None)
    _atomic_write(path, q)
    return len(q)


# Convenience wrappers for the floor-read queue (same shape, different file).
def load_floor_queue():
    return _load(FLOOR_QUEUE_PATH)


def enqueue_floor(items):
    return enqueue(items, path=FLOOR_QUEUE_PATH)


def dequeue_floor(tabas):
    return dequeue(tabas, path=FLOOR_QUEUE_PATH)


# Same for the hafrasha permit-use read queue (build_hafrash_read_queue.py fills
# it, process_hafrash_queue.md drains it).
def load_hafrash_queue():
    return _load(HAFRASH_QUEUE_PATH)


def enqueue_hafrash(items):
    return enqueue(items, path=HAFRASH_QUEUE_PATH)


def dequeue_hafrash(tabas):
    return dequeue(tabas, path=HAFRASH_QUEUE_PATH)
