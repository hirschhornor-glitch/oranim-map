"""Exclusive locks on the shared Chromium profiles.

  YK    -> C:\\ORANIM\\.browser_data_jlm   hold_yk_profile()
  Mavat -> C:\\ORANIM\\.browser_data       hold_mavat_profile()

Chromium refuses a second launch on a profile that is already open, and the
failure (TargetClosedError at launch_persistent_context) kills the whole run.
Scheduled jobs collide when the laptop wakes and Task Scheduler fires every
missed task at once: on 2026-10-03 23:39 the biweekly permits scan and the
monthly street harvest both started together and both died at launch (YK
profile). The same night the monthly Mavat revival sweep died at launch while
fetch_decision_docs.py had the Mavat profile open, and on 2026-10-04 09:14
enrich's table-5 pass (update_mavat_ui.py) crashed against the weekly sync.

Every script that launches a shared profile calls the matching hold_*() right
before launch_persistent_context. The first caller gets the lock and keeps it
until its process exits; everyone else waits. The lock is an OS byte-range lock
(msvcrt.locking) on a file handle, so Windows releases it when the holder
exits or crashes - no stale lock files to clean up.

A child process launched by a holder (env YK_PROFILE_LOCK_PID /
MAVAT_PROFILE_LOCK_PID) inherits the lock instead of waiting on its own parent
forever.
"""
from __future__ import annotations

import msvcrt
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

DEFAULT_WAIT_MIN = 240
POLL_SEC = 60
_LOCK_OFFSET = 1 << 20  # lock a byte past the owner text so others can still read it

# name -> (profile dir, lock file, inheritance env var)
_PROFILES = {
    "YK": (Path(r"C:\ORANIM\.browser_data_jlm"), Path(r"C:\ORANIM\.browser_data_jlm.lock"),
           "YK_PROFILE_LOCK_PID"),
    "Mavat": (Path(r"C:\ORANIM\.browser_data"), Path(r"C:\ORANIM\.browser_data.lock"),
              "MAVAT_PROFILE_LOCK_PID"),
}
# Back-compat names (YK profile).
PROFILE_DIR, LOCK_PATH, _ENV = _PROFILES["YK"]

_handles: dict = {}  # name -> open file handle, kept for the life of the process


class ProfileBusy(RuntimeError):
    pass


YKProfileBusy = ProfileBusy  # back-compat


def _log(msg: str) -> None:
    print(f"[profile-lock {datetime.now():%H:%M:%S}] {msg}", flush=True)


def _owner(lock_path: Path) -> str:
    try:
        return lock_path.read_text(encoding="utf-8", errors="replace").strip() or "?"
    except OSError:
        return "?"


def _foreign_chrome_on_profile(profile_dir: Path) -> bool:
    """A chrome on the profile that did not take the lock (ad-hoc/debug script)."""
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-CimInstance Win32_Process -Filter \"Name='chrome.exe'\" | "
             "Select-Object -ExpandProperty CommandLine"],
            capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=60)
        # trailing space: ".browser_data " must not match ".browser_data_jlm"
        return profile_dir.name.lower() + " " in ((r.stdout or "").lower().replace('"', " ") + " ")
    except Exception:
        return False


def _hold(name: str, owner: str | None, wait_min: float) -> None:
    if name in _handles:
        return
    profile_dir, lock_path, env = _PROFILES[name]
    if os.environ.get(env) and os.environ[env] != str(os.getpid()):
        # Parent holds it and handed the profile down to us.
        return

    owner = owner or Path(sys.argv[0]).name
    deadline = time.time() + wait_min * 60
    fh = open(lock_path, "a+", encoding="utf-8")
    announced = False
    while True:
        try:
            fh.seek(_LOCK_OFFSET)
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            break
        except OSError:
            if time.time() > deadline:
                fh.close()
                raise ProfileBusy(f"{name} profile still locked by [{_owner(lock_path)}] "
                                  f"after {wait_min:.0f} min")
            if not announced:
                _log(f"{owner}: {name} profile held by [{_owner(lock_path)}] - waiting "
                     f"(up to {wait_min:.0f} min)")
                announced = True
            time.sleep(POLL_SEC)

    # Lock is ours; still wait out any chrome that opened the profile without the lock.
    while _foreign_chrome_on_profile(profile_dir):
        if time.time() > deadline:
            fh.seek(_LOCK_OFFSET)
            msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            fh.close()
            raise ProfileBusy(f"a chrome outside the lock keeps the {name} profile open")
        if not announced:
            _log(f"{owner}: a chrome is already open on the {name} profile - waiting")
            announced = True
        time.sleep(POLL_SEC)

    # Record the owner (the locked byte is far past this text).
    fh.seek(0)
    fh.truncate()
    fh.write(f"{owner} pid={os.getpid()} since={datetime.now():%Y-%m-%d %H:%M}")
    fh.flush()
    os.environ[env] = str(os.getpid())
    _handles[name] = fh
    if announced:
        _log(f"{owner}: got the {name} profile")


def hold_yk_profile(owner: str | None = None, wait_min: float = DEFAULT_WAIT_MIN) -> None:
    """Block until this process exclusively holds the YK profile. Idempotent.
    Raises ProfileBusy after `wait_min` minutes."""
    _hold("YK", owner, wait_min)


def hold_mavat_profile(owner: str | None = None, wait_min: float = DEFAULT_WAIT_MIN) -> None:
    """Block until this process exclusively holds the Mavat profile (.browser_data).
    Idempotent. Raises ProfileBusy after `wait_min` minutes."""
    _hold("Mavat", owner, wait_min)
