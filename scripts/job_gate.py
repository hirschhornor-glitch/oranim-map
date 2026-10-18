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

Usage:
  - .bat launchers, right after `@echo off`:
        if defined ORANIM_JOB_GATE goto :oranim_job_gated
        "C:\\Users\\orlev\\AppData\\Local\\Python\\bin\\python.exe" "C:\\ORANIM\\job_gate.py" "%~f0" %*
        exit /b %ERRORLEVEL%
        :oranim_job_gated
  - non-bat tasks: the task action runs  python job_gate.py <command...>
    (a leading *.py is run with this interpreter).

The wrapped command's exit code is returned unchanged. If the gate can't be
had within MAX_WAIT_SEC the job runs anyway (pre-gate behaviour) — a late,
possibly-colliding run beats a silently skipped one; git_sync now fails loud
on any collision. Activity -> permits_scan_reports/job_gate.log.
"""
import msvcrt
import os
import subprocess
import sys
import time

LOCK_FILE = r"C:\ORANIM\.oranim_job_gate.lock"
HOLDER_FILE = r"C:\ORANIM\.oranim_job_gate.holder"
LOG_FILE = r"C:\ORANIM\permits_scan_reports\job_gate.log"
ENV_FLAG = "ORANIM_JOB_GATE"
MAX_WAIT_SEC = int(os.environ.get("ORANIM_JOB_GATE_MAX_WAIT", 8 * 3600))
POLL_SEC = 5


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


def _acquire(label: str):
    """Return the open lock fd once held, or None if MAX_WAIT_SEC expired."""
    fd = os.open(LOCK_FILE, os.O_RDWR | os.O_CREAT)
    start = time.time()
    next_note = start
    while True:
        os.lseek(fd, 0, os.SEEK_SET)
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            waited = time.time() - start
            _log(f"ACQUIRE {label}" + (f" (waited {waited:.0f}s)" if waited >= 1 else ""))
            try:
                with open(HOLDER_FILE, "w", encoding="utf-8") as f:
                    f.write(f"{label} pid={os.getpid()} since {time.strftime('%Y-%m-%d %H:%M:%S')}")
            except OSError:
                pass
            return fd
        except OSError:
            now = time.time()
            if now - start > MAX_WAIT_SEC:
                _log(f"TIMEOUT {label}: gate still held by [{_holder()}] after "
                     f"{MAX_WAIT_SEC}s — running UNGATED")
                os.close(fd)
                return None
            if now >= next_note:
                _log(f"WAIT {label}: gate held by [{_holder()}]")
                next_note = now + 600
            time.sleep(POLL_SEC)


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
    fd = _acquire(label)
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
