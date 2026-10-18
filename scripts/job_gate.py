"""
job_gate.py
-----------
Machine-wide "one Oranim scheduled job at a time" gate.

Why: every Oranim task has StartWhenAvailable=True (see the battery memo), so
when the laptop wakes after missed slots Task Scheduler fires all of them at
once. On 2026-10-18 08:50 ~15 jobs started in the same second and collided on
the one oranim-app checkout: one job wrote data/tama38_developers.json while
another was mid `pull --rebase` ("cannot rebase: You have unstaged changes"),
failing MuniGIS + ScriptMirror. git_sync.repo_lock only covers the git calls,
not the scripts' own unlocked writes into data/, so the jobs themselves must
not overlap.

How: holds an OS byte-range lock (msvcrt.locking) on LOCK_FILE for the whole
run of the wrapped command. The OS drops it when this process exits or dies,
so a crashed holder can never wedge the gate (no stale-lock heuristics).
Children get ORANIM_JOB_GATE set, so nested launchers/scripts don't re-gate
(no self-deadlock).

The gate process puts itself in a Windows Job Object with KILL_ON_JOB_CLOSE
before starting the job, so the whole child tree inherits it: if the gate is
killed (taskkill, Task Scheduler "End"), the OS kills the job's processes too,
instead of freeing the lock while the job keeps running beside the next one.
Side effect: anything a job leaves running after it returns is killed when
the gate exits (no gated launcher detaches processes on purpose).

Ordered catch-up: waiters register a ticket in QUEUE_DIR and only try the lock
while theirs is the minimal live ticket. A ticket's key is the most recent
scheduled occurrence (SCHEDULE time-of-day, at or before registration) of its
job, so jobs run in the order they were due — e.g. mavat_sync (06:00) before
enrich_changed_plans (08:00), weekly_objections (06:00) before yk_149 (07:00).
Unscheduled commands key on their arrival time. Tickets of dead processes
(pid + creation time) are dropped by whoever sees them. A new waiter first
sleeps SETTLE_SEC so jobs fired in the same second all register before anyone
takes the lock.

Usage:
  - .bat launchers, right after `@echo off`:
        if defined ORANIM_JOB_GATE goto :oranim_job_gated
        "C:\\Users\\orlev\\AppData\\Local\\Python\\bin\\python.exe" "C:\\ORANIM\\job_gate.py" "%~f0" %*
        exit /b %ERRORLEVEL%
        :oranim_job_gated
  - non-bat tasks: the task action runs  python job_gate.py <command...>
    (a leading *.py is run with this interpreter).
  - a new scheduled job: add its basename + time-of-day to SCHEDULE.

The wrapped command's exit code is returned unchanged. If the gate can't be
had within MAX_WAIT_SEC the job runs anyway (pre-gate behaviour) — a late,
possibly-colliding run beats a silently skipped one; git_sync now fails loud
on any collision. Activity -> permits_scan_reports/job_gate.log.
"""
import ctypes
import json
import msvcrt
import os
import subprocess
import sys
import time
from ctypes import wintypes
from datetime import datetime, timedelta

LOCK_FILE = r"C:\ORANIM\.oranim_job_gate.lock"
HOLDER_FILE = r"C:\ORANIM\.oranim_job_gate.holder"
QUEUE_DIR = r"C:\ORANIM\.oranim_job_queue"
LOG_FILE = r"C:\ORANIM\permits_scan_reports\job_gate.log"
ENV_FLAG = "ORANIM_JOB_GATE"
MAX_WAIT_SEC = int(os.environ.get("ORANIM_JOB_GATE_MAX_WAIT", 8 * 3600))
POLL_SEC = 3
SETTLE_SEC = 8

# Scheduled time-of-day per job (bat / script basename, lowercase), as set in
# Task Scheduler. Only the order matters; keep in sync when a schedule moves.
SCHEDULE = {
    "run_biweekly_permits_scan.bat": "01:00",
    "run_tama38_street_harvest_driver.bat": "02:00",
    "run_monthly_pikuah_scan.bat": "03:00",
    "run_monthly_tree_scan.bat": "03:00",
    "run_mavat_revival_sweep.bat": "04:00",
    "backup_repo_to_drive.ps1": "04:00",          # Oranim_RepoBackup_Weekly
    "run_dev_backfill.bat": "04:30",
    "run_weekly_muni_gis.bat": "05:00",
    "run_weekly_tama38_149.bat": "05:00",
    "run_all_committees.bat": "05:00",
    "run_mavat_sync.bat": "06:00",
    "run_weekly_objections.bat": "06:00",
    "run_weekly_allocations_alert.bat": "06:30",
    "run_weekly_yk_149_scan.bat": "07:00",
    "run_enrich_changed_plans.bat": "08:00",
    "run_process_objections_scan.bat": "08:00",
    "run_yearly_yearbook_reminder.bat": "09:00",
    "run_weekly_decision_docs_scan.bat": "20:00",
    "run_biweekly_objections_scan.bat": "22:00",
    "git_sync.py": "22:30",                       # Oranim_ScriptMirror_Daily
}

_k32 = ctypes.WinDLL("kernel32", use_last_error=True)
_k32.OpenProcess.restype = wintypes.HANDLE
_k32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
_k32.GetCurrentProcess.restype = wintypes.HANDLE
_k32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
_k32.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
_k32.CloseHandle.argtypes = [wintypes.HANDLE]
_k32.CreateJobObjectW.restype = wintypes.HANDLE
_k32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
_k32.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
_k32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]

PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
STILL_ACTIVE = 259
ERROR_ACCESS_DENIED = 5
JobObjectExtendedLimitInformation = 9
JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000


class _BasicLimit(ctypes.Structure):
    _fields_ = [("PerProcessUserTimeLimit", ctypes.c_int64),
                ("PerJobUserTimeLimit", ctypes.c_int64),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD)]


class _ExtendedLimit(ctypes.Structure):
    _fields_ = [("BasicLimitInformation", _BasicLimit),
                ("IoInfo", ctypes.c_uint64 * 6),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t)]


def _log(msg: str):
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] pid={os.getpid()} {msg}"
    print(f"[job_gate] {msg}", flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


def _holder() -> str:
    try:
        with open(HOLDER_FILE, encoding="utf-8") as f:
            return f.read().strip() or "?"
    except OSError:
        return "?"


def _kill_tree_with_me() -> bool:
    """Put this process (and so every child it starts) in a kill-on-close job.
    The handle is deliberately never closed: it closes when this process ends."""
    job = _k32.CreateJobObjectW(None, None)
    if not job:
        return False
    info = _ExtendedLimit()
    info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    if (_k32.SetInformationJobObject(job, JobObjectExtendedLimitInformation,
                                     ctypes.byref(info), ctypes.sizeof(info))
            and _k32.AssignProcessToJobObject(job, _k32.GetCurrentProcess())):
        return True
    _k32.CloseHandle(job)
    return False


def _creation_time(handle) -> int:
    t = [wintypes.FILETIME() for _ in range(4)]
    if not _k32.GetProcessTimes(handle, *[ctypes.byref(x) for x in t]):
        return 0
    return (t[0].dwHighDateTime << 32) | t[0].dwLowDateTime


def _alive(pid: int, ctime: int) -> bool:
    h = _k32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not h:
        return ctypes.get_last_error() == ERROR_ACCESS_DENIED
    try:
        code = wintypes.DWORD()
        if not _k32.GetExitCodeProcess(h, ctypes.byref(code)) or code.value != STILL_ACTIVE:
            return False
        return not ctime or _creation_time(h) == ctime   # pid reuse check
    finally:
        _k32.CloseHandle(h)


def _sched_key(argv: list, now: datetime) -> float:
    """Epoch of the job's latest scheduled occurrence at/before now (now if unscheduled)."""
    for a in argv:
        hhmm = SCHEDULE.get(os.path.basename(a).lower())
        if hhmm:
            h, m = map(int, hhmm.split(":"))
            due = now.replace(hour=h, minute=m, second=0, microsecond=0)
            if due > now:
                due -= timedelta(days=1)
            return due.timestamp()
    return now.timestamp()


def _ticket_path(pid: int) -> str:
    return os.path.join(QUEUE_DIR, f"{pid}.tkt")


def _register(label: str, argv: list) -> dict:
    now = datetime.now()
    t = {"key": _sched_key(argv, now), "reg": now.timestamp(), "pid": os.getpid(),
         "ctime": _creation_time(_k32.GetCurrentProcess()), "label": label}
    os.makedirs(QUEUE_DIR, exist_ok=True)
    tmp = _ticket_path(t["pid"]) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(t, f)
    os.replace(tmp, _ticket_path(t["pid"]))
    return t


def _queue() -> list:
    """Live tickets sorted by turn; dead ones are removed on the way."""
    live = []
    try:
        names = os.listdir(QUEUE_DIR)
    except OSError:
        return live
    for name in names:
        if not name.endswith(".tkt"):
            continue
        path = os.path.join(QUEUE_DIR, name)
        try:
            with open(path, encoding="utf-8") as f:
                t = json.load(f)
        except (OSError, ValueError):
            continue   # being written, or just removed
        if _alive(int(t["pid"]), int(t.get("ctime") or 0)):
            live.append(t)
        else:
            try:
                os.remove(path)
            except OSError:
                pass
    return sorted(live, key=lambda t: (t["key"], t["reg"], t["pid"]))


def _acquire(label: str, argv: list):
    """Return the open lock fd once held, or None if MAX_WAIT_SEC expired."""
    fd = os.open(LOCK_FILE, os.O_RDWR | os.O_CREAT)
    ticket = _register(label, argv)
    start = time.time()
    next_note = start + SETTLE_SEC + POLL_SEC
    try:
        time.sleep(SETTLE_SEC)
        while True:
            queue = _queue()
            ahead = next((i for i, t in enumerate(queue) if t["pid"] == ticket["pid"]), None)
            if ahead is None:   # ticket vanished (deleted by hand?) — re-register
                ticket = _register(label, argv)
                continue
            if ahead == 0:
                os.lseek(fd, 0, os.SEEK_SET)
                try:
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                    waited = time.time() - start
                    _log(f"ACQUIRE {label}" + (f" (waited {waited:.0f}s)" if waited >= SETTLE_SEC + 1 else ""))
                    try:
                        with open(HOLDER_FILE, "w", encoding="utf-8") as f:
                            f.write(f"{label} pid={os.getpid()} since {time.strftime('%Y-%m-%d %H:%M:%S')}")
                    except OSError:
                        pass
                    return fd
                except OSError:
                    pass
            now = time.time()
            if now - start > MAX_WAIT_SEC:
                _log(f"TIMEOUT {label}: gate still held by [{_holder()}] after "
                     f"{MAX_WAIT_SEC}s — running UNGATED")
                os.close(fd)
                return None
            if now >= next_note:
                behind = ", ".join(t["label"] for t in queue[:ahead])
                _log(f"WAIT {label}: gate held by [{_holder()}]"
                     + (f"; queued after {behind}" if behind else ""))
                next_note = now + 600
            time.sleep(POLL_SEC)
    finally:
        try:
            os.remove(_ticket_path(ticket["pid"]))
        except OSError:
            pass


def _command(argv: list) -> list:
    first = argv[0].lower()
    if first.endswith((".bat", ".cmd")):
        return ["cmd.exe", "/c", argv[0]] + argv[1:]
    if first.endswith(".py"):
        return [sys.executable] + argv
    return argv


def main(argv: list) -> int:
    if not argv:
        print("usage: job_gate.py <command|script.bat|script.py> [args...]", file=sys.stderr)
        return 2
    label = os.path.basename(argv[0])
    if os.environ.get(ENV_FLAG):  # already inside a gated job
        return subprocess.call(_command(argv))
    if not _kill_tree_with_me():
        _log(f"NOJOBOBJ {label}: job object failed (winerr {ctypes.get_last_error()}) "
             "— a killed gate would leave the job running")
    fd = _acquire(label, argv)
    env = dict(os.environ, **{ENV_FLAG: label})
    t0 = time.time()
    try:
        rc = subprocess.call(_command(argv), env=env)
    finally:
        if fd is not None:
            try:
                with open(HOLDER_FILE, "w", encoding="utf-8"):
                    pass
            except OSError:
                pass
            os.lseek(fd, 0, os.SEEK_SET)
            try:
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
            os.close(fd)
    _log(f"RELEASE {label} rc={rc} after {time.time() - t0:.0f}s")
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
