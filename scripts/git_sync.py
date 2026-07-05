"""
git_sync.py
-----------
Shared helpers for scripts that read/write files inside the oranim-app
repo (plans.geojson, etc). Prevents clobbering remote updates (e.g.
the scheduled update_plans.py workflow) and collisions between
concurrent local writers (parallel Claude sessions, scheduled tasks) by:

  - a cross-process lock file serializing all local git operations
  - `git pull` before reading, so the local file matches remote
  - `update_json_and_push()` — the preferred write path: applies a
    *semantic* edit function to the freshly-pulled file, commits, pushes,
    and on push rejection re-applies the edit on the new remote tip.
    plans.geojson is a single line, so a textual rebase/merge of two
    concurrent edits ALWAYS conflicts; re-applying the edit never does.

Usage (preferred, for JSON data files):
    from git_sync import update_json_and_push

    def _apply(data):
        ...mutate parsed JSON in place...
        return True   # False/None -> nothing changed, no commit
    update_json_and_push('data/plans.geojson', _apply, 'data: update X')

Usage (legacy, for files already written to disk):
    from git_sync import pull_before_read, commit_and_push_after_write

    pull_before_read()
    # ... read plans.geojson, modify it, write it back ...
    commit_and_push_after_write('data/plans.geojson', 'fix: update X')
"""
import json
import os
import subprocess
import sys
import time
from contextlib import contextmanager

REPO_DIR = r"C:\ORANIM\oranim-app"

LOCK_FILE = r"C:\ORANIM\.git_sync.lock"
LOCK_STALE_SEC = 300   # steal a lock older than this (holder crashed)
LOCK_WAIT_SEC = 120    # max time to wait for a busy lock


@contextmanager
def repo_lock(label: str = 'git_sync'):
    """Cross-process mutex for git operations on the repo.

    Serializes concurrent local writers (parallel sessions, scheduled
    scripts). Does NOT protect against pushes arriving on GitHub from CI —
    update_json_and_push handles those with its re-apply loop.
    Not reentrant: don't nest repo_lock sections.
    """
    deadline = time.time() + LOCK_WAIT_SEC
    while True:
        try:
            fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, f"pid={os.getpid()} {label} {time.strftime('%H:%M:%S')}".encode())
            os.close(fd)
            break
        except FileExistsError:
            try:
                if time.time() - os.path.getmtime(LOCK_FILE) > LOCK_STALE_SEC:
                    print(f"[git_sync] stealing stale lock {LOCK_FILE}")
                    os.remove(LOCK_FILE)
                    continue
            except OSError:
                continue  # lock vanished between the check and the stat
            if time.time() > deadline:
                raise TimeoutError(
                    f"[git_sync] could not acquire {LOCK_FILE} within {LOCK_WAIT_SEC}s "
                    "— another writer is stuck? Inspect/delete the lock file.")
            time.sleep(2)
    try:
        yield
    finally:
        try:
            os.remove(LOCK_FILE)
        except OSError:
            pass


def _run(cmd: list, cwd: str = REPO_DIR, check: bool = True) -> subprocess.CompletedProcess:
    """Run a git command, stream output."""
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, encoding='utf-8')
    if result.stdout:
        print("    " + result.stdout.strip().replace('\n', '\n    '))
    if result.returncode != 0:
        print(f"    [stderr] {result.stderr.strip()}", file=sys.stderr)
        if check:
            raise RuntimeError(f"git command failed: {' '.join(cmd)}")
    return result


def _stash_wip(repo_dir: str, label: str) -> bool:
    """Stash all uncommitted work (incl. untracked). Returns True if stashed."""
    wt_check = _run(['git', 'diff', '--quiet'], cwd=repo_dir, check=False)
    staged_check = _run(['git', 'diff', '--cached', '--quiet'], cwd=repo_dir, check=False)
    untracked = _run(['git', 'ls-files', '--others', '--exclude-standard'],
                     cwd=repo_dir, check=False)
    if wt_check.returncode == 0 and staged_check.returncode == 0 and not untracked.stdout.strip():
        return False
    stash_res = _run(['git', 'stash', 'push', '-u', '-m', label], cwd=repo_dir, check=False)
    stashed = 'no local changes' not in (stash_res.stdout + stash_res.stderr).lower()
    if stashed:
        print("[git_sync] stashed other working-tree changes temporarily")
    return stashed


def _pop_wip(repo_dir: str):
    pop_res = _run(['git', 'stash', 'pop'], cwd=repo_dir, check=False)
    if pop_res.returncode != 0:
        print("[git_sync] stash pop reported issues — review with 'git stash list'")
    else:
        print("[git_sync] restored other working-tree changes")


def pull_before_read(repo_dir: str = REPO_DIR) -> bool:
    """
    Pull latest from origin before reading local files.
    Returns True if successful, False otherwise.

    Auto-stashes unstaged changes around the pull so unrelated work-in-progress
    (e.g. data/projector_*.geojson, index.html) doesn't block the fast-forward.
    """
    print(f"[git_sync] Pulling latest from origin in {repo_dir}...")
    with repo_lock('pull_before_read'):
        stashed = False
        try:
            _run(['git', 'fetch', 'origin'], cwd=repo_dir)
            stashed = _stash_wip(repo_dir, 'git_sync auto-pre-pull')
            result = _run(['git', 'pull', '--ff-only', 'origin', 'master'], cwd=repo_dir, check=False)
            if result.returncode != 0:
                print("[git_sync] WARNING: Could not fast-forward. Local may be diverged.")
                return False
            return True
        except Exception as e:
            print(f"[git_sync] ERROR: {e}", file=sys.stderr)
            return False
        finally:
            if stashed:
                _pop_wip(repo_dir)


def update_json_and_push(relative_path: str, edit_fn, message: str,
                         repo_dir: str = REPO_DIR, max_attempts: int = 4) -> bool:
    """
    Concurrency-safe update of a JSON data file in the repo. PREFERRED over
    write-then-commit_and_push_after_write for plans.geojson and friends.

    edit_fn(data) receives the parsed JSON of the freshly-pulled file,
    mutates it in place, and returns truthy if it changed anything
    (falsy -> nothing to do, no commit). It MUST be re-appliable: if the
    push is rejected because remote moved, we drop our commit, fast-forward
    to the new tip, and call edit_fn again on the fresh file. This never
    needs a textual merge — which for single-line JSON always conflicts.

    NOTE: any uncommitted changes to relative_path are stashed and restored
    like other WIP; the file content pushed is pull + edit_fn only.
    """
    abs_path = os.path.join(repo_dir, relative_path)
    print(f"[git_sync] update_json_and_push: {relative_path}...")
    with repo_lock(f'update {relative_path}'):
        stashed = False
        try:
            stashed = _stash_wip(repo_dir, f'git_sync auto-update: {relative_path}')
            for attempt in range(1, max_attempts + 1):
                _run(['git', 'fetch', 'origin'], cwd=repo_dir)
                ff = _run(['git', 'pull', '--ff-only', 'origin', 'master'], cwd=repo_dir, check=False)
                if ff.returncode != 0:
                    print("[git_sync] ERROR: local branch diverged from origin/master — "
                          "resolve manually (unpushed commits?).", file=sys.stderr)
                    return False
                with open(abs_path, encoding='utf-8') as f:
                    data = json.load(f)
                if not edit_fn(data):
                    print("[git_sync] edit_fn made no changes — nothing to push.")
                    return True
                with open(abs_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False)
                status = _run(['git', 'status', '--porcelain', relative_path], cwd=repo_dir, check=False)
                if not status.stdout.strip():
                    print("[git_sync] file content unchanged — nothing to push.")
                    return True
                _run(['git', 'add', relative_path], cwd=repo_dir)
                _run(['git', 'commit', '-m', message], cwd=repo_dir)
                push = _run(['git', 'push', 'origin', 'master'], cwd=repo_dir, check=False)
                if push.returncode == 0:
                    return True
                # Remote moved under us: drop our commit, re-apply on the new tip.
                # (Everything else is stashed, so --hard only discards our own edit.)
                print(f"[git_sync] push rejected (attempt {attempt}/{max_attempts}) — "
                      "re-applying edit on the new remote tip")
                _run(['git', 'reset', '--hard', 'HEAD~1'], cwd=repo_dir)
            print(f"[git_sync] ERROR: gave up after {max_attempts} rejected pushes.", file=sys.stderr)
            return False
        except Exception as e:
            print(f"[git_sync] ERROR: {e}", file=sys.stderr)
            return False
        finally:
            if stashed:
                _pop_wip(repo_dir)


def commit_and_push_after_write(
    relative_path: str, message: str, repo_dir: str = REPO_DIR
) -> bool:
    """
    Stage, commit, and push a specific file after writing (legacy path —
    prefer update_json_and_push for JSON data files).

    If the pull --rebase hits a conflict (concurrent edit of the same
    file), the rebase is aborted and the commit undone: the repo is left
    clean and non-diverged, the data stays in the working tree, and False
    is returned. It is NEVER left mid-rebase.
    """
    print(f"[git_sync] Committing + pushing {relative_path}...")
    with repo_lock(f'push {relative_path}'):
        stashed = False
        try:
            status = _run(['git', 'status', '--porcelain', relative_path], cwd=repo_dir, check=False)
            if not status.stdout.strip():
                print("[git_sync] No changes to commit — skipping push.")
                return True
            _run(['git', 'add', relative_path], cwd=repo_dir)
            _run(['git', 'commit', '-m', message], cwd=repo_dir)

            # Stash other unstaged work (incl. untracked) so pull --rebase succeeds
            stashed = _stash_wip(repo_dir, f'git_sync auto-pre-push: {relative_path}')

            # Pull any new remote commits first (in case something landed meanwhile)
            rebase = _run(['git', 'pull', '--rebase', 'origin', 'master'], cwd=repo_dir, check=False)
            if rebase.returncode != 0:
                # Conflict (single-line JSON files always conflict). Clean up:
                # abort the rebase, undo the commit but keep the change in the
                # working tree, and report failure — never leave a mid-rebase repo.
                print("[git_sync] pull --rebase failed (concurrent edit?) — aborting rebase",
                      file=sys.stderr)
                _run(['git', 'rebase', '--abort'], cwd=repo_dir, check=False)
                _run(['git', 'reset', '--mixed', 'HEAD~1'], cwd=repo_dir, check=False)
                print("[git_sync] ERROR: push skipped; change left uncommitted in working tree. "
                      "Use update_json_and_push (re-appliable edit) for this file.", file=sys.stderr)
                return False
            _run(['git', 'push', 'origin', 'master'], cwd=repo_dir)
            return True
        except Exception as e:
            print(f"[git_sync] ERROR: {e}", file=sys.stderr)
            return False
        finally:
            if stashed:
                _pop_wip(repo_dir)
