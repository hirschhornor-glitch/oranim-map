# -*- coding: utf-8 -*-
"""Version trigger for the Table 5 re-check — used by update_mavat_ui.py.

Why this exists (2026-11-15): the weekly sync re-read Table 5 only when a plan's
status or date changed. A plan whose הוראות were reissued under the same status
kept the old numbers for months:
  - 101-1151901 יצחק שדה 5-7: "במילוי תנאים" since 06/01/2026, checked 35 times,
    always "Unchanged", while its הוראות went through versions 33 → 39 → 43.
  - 101-1442656 בן יאיר 18: entered the stage in 03/2026, reissued 16 → 28 later;
    High stayed 34.9 (current 28.35).
  - 101-1215656 בני בתירא 7-9: status fixed BY HAND in the sheet (נדחתה → הפקדה),
    so the sync never saw a change; units stayed 123 while the plan is 94.

XPLAN's blue-line layer carries `pl_order_print_version` — the print counter of
the current הוראות — for every plan. A higher number than the one we last read
means the plan has new documents, whatever its status says.

State: xplan_versions_seen.json = {plan_name: {"version": int, "read_at": iso}}.
A version is recorded ONLY after its Table 5 was actually read (non-empty XLSX)
and the write phase ran — a failed read stays unrecorded and triggers again.

Failure policy: a chunk that fails is counted and reported, never read as
"no change". The caller surfaces failures in the weekly email.

    python xplan_versions.py --selftest     # known plans against known counters
"""
from __future__ import annotations

import json
import os
import ssl
import sys
import time
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter

XPLAN_URL = ('https://ags.iplan.gov.il/arcgisiplan/rest/services/'
             'PlanningPublic/Xplan/MapServer/1/query')
SEEN_PATH = r'C:\ORANIM\xplan_versions_seen.json'
CHUNK = 50

# Version-triggered re-checks per run for plans with NO recorded version yet
# (the initial catch-up). Plans whose version demonstrably moved are never capped.
# 40 x ~10 s ≈ 7 min — keeps the Sunday chain well inside its 07:30 deadline.
MAX_BASELINE_RECHECKS = 40

# Catch-up order for plans with no recorded version: public-facing stages first.
STATUS_PRIORITY = [
    "הפקדה להתנגדויות/השגות",
    "תום תקופת הפקדה",
    "דיון בהתנגדויות ותיקונים",
    "הכרעה בהתנגדויות / אישור",
    "במילוי תנאים להפקדה",
    "בהליך אישור",
]


class _LegacySSLAdapter(HTTPAdapter):
    # Same pattern as fetch_easements_from_xplan.py — iplan needs legacy TLS.
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.set_ciphers('DEFAULT:@SECLEVEL=0')
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)


def _session():
    s = requests.Session()
    s.mount('https://ags.iplan.gov.il', _LegacySSLAdapter())
    return s


def fetch_versions(plan_numbers):
    """{plan_number: int version} for the given '101-xxxxxxx' numbers.

    Returns (versions, failures). `failures` lists failed chunks — those plans
    are simply absent from `versions`, and the caller must not treat absence
    as "unchanged". A plan XPLAN does not know is also absent (counted apart).
    """
    import urllib3
    urllib3.disable_warnings()
    nums = sorted({str(p).strip() for p in plan_numbers if str(p).strip()})
    versions, failures = {}, []
    s = _session()
    for i in range(0, len(nums), CHUNK):
        chunk = nums[i:i + CHUNK]
        where = "pl_number IN (%s)" % ",".join("'%s'" % n.replace("'", "") for n in chunk)
        last_err = None
        # iplan is intermittently down ("Could not access any server machines",
        # read timeouts) — seen several times within minutes on 2026-11-15, with
        # good answers in between. Retry before declaring the chunk failed.
        for attempt, wait in enumerate((0, 10, 30, 60)):
            if wait:
                time.sleep(wait)
            try:
                # GET, not POST: on 2026-11-15 the same query timed out as a POST
                # and returned in seconds as a GET.
                r = s.get(XPLAN_URL, params={'where': where,
                                             'outFields': 'pl_number,pl_order_print_version',
                                             'returnGeometry': 'false', 'f': 'json'}, timeout=60)
                r.raise_for_status()
                j = r.json()
                if 'error' in j or 'features' not in j:
                    raise RuntimeError(str(j.get('error', j))[:200])
                for f in j['features']:
                    a = f.get('attributes') or {}
                    v = a.get('pl_order_print_version')
                    if a.get('pl_number') and v is not None:
                        # A plan with several blue-line rows: the highest counter wins.
                        versions[a['pl_number']] = max(int(v), versions.get(a['pl_number'], 0))
                last_err = None
                break
            except Exception as e:
                last_err = e
        if last_err is not None:
            failures.append(f"chunk {i // CHUNK + 1} ({len(chunk)} plans), 4 attempts: {last_err}")
    return versions, failures


def load_seen(path=SEEN_PATH):
    if not os.path.exists(path):
        return {}
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def save_seen(seen, path=SEEN_PATH):
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(seen, f, ensure_ascii=False, indent=1, sort_keys=True)
    os.replace(tmp, path)


def plan_triggers(rows, versions, seen, cap=MAX_BASELINE_RECHECKS):
    """Decide which rows get a version-triggered Table 5 re-check.

    rows: dicts with 'plan_name' and 'current_status'.
    Returns ({plan_name: reason}, summary dict).
      reason 'moved'    — XPLAN version > recorded version (never capped)
      reason 'baseline' — no recorded version yet (capped, priority by status)
    """
    moved, unknown = {}, []
    for r in rows:
        pn = r.get('plan_name', '')
        v = versions.get(pn)
        if v is None:
            continue
        rec = (seen.get(pn) or {}).get('version')
        if rec is None:
            unknown.append(r)
        elif v > rec:
            moved[pn] = f'moved {rec}→{v}'

    def prio(r):
        st = r.get('current_status', '')
        return STATUS_PRIORITY.index(st) if st in STATUS_PRIORITY else len(STATUS_PRIORITY)

    unknown.sort(key=prio)
    chosen = {r['plan_name']: f"baseline v{versions[r['plan_name']]}" for r in unknown[:cap]}
    triggers = {**chosen, **moved}
    return triggers, {'moved': len(moved), 'baseline_chosen': len(chosen),
                      'baseline_deferred': max(0, len(unknown) - cap)}


def record(seen, plan_name, version):
    seen[plan_name] = {'version': int(version),
                       'read_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}


def _selftest():
    # Independent anchors: counters read by eye off the הוראות footers on
    # 2026-11-15, and two XPLAN totals that match the current Table 5.
    expect_at_least = {'101-1215656': 27, '101-1442656': 28, '101-1500909': 11,
                       '101-1422401': 25, '101-1151901': 43, '101-1278654': 19}
    v, fails = fetch_versions(list(expect_at_least) + ['101-9999999'])
    ok = True
    for pn, mn in expect_at_least.items():
        got = v.get(pn)
        good = got is not None and got >= mn
        ok &= good
        print(f"  {'OK ' if good else 'BAD'} {pn}: {got} (expected ≥ {mn})")
    print(f"  {'OK ' if '101-9999999' not in v else 'BAD'} unknown plan absent")
    print(f"  failures: {fails or 'none'}")
    # Trigger logic on synthetic state.
    rows = [{'plan_name': 'A', 'current_status': 'בבדיקה תכנונית'},
            {'plan_name': 'B', 'current_status': 'הפקדה להתנגדויות/השגות'},
            {'plan_name': 'C', 'current_status': 'במילוי תנאים להפקדה'},
            {'plan_name': 'D', 'current_status': 'במילוי תנאים להפקדה'}]
    trig, summ = plan_triggers(rows, {'A': 5, 'B': 9, 'C': 7, 'D': 3},
                               {'C': {'version': 6}, 'D': {'version': 3}}, cap=1)
    exp = {'B': 'baseline v9', 'C': 'moved 6→7'}
    good = trig == exp and summ == {'moved': 1, 'baseline_chosen': 1, 'baseline_deferred': 1}
    ok &= good
    print(f"  {'OK ' if good else 'BAD'} trigger logic: {trig} {summ}")
    print('SELFTEST', 'PASSED' if ok and not fails else 'FAILED')
    return ok and not fails


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    if '--selftest' in sys.argv:
        sys.exit(0 if _selftest() else 1)
    print(__doc__)
