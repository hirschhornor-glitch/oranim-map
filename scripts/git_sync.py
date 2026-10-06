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
import glob
import json
import os
import shutil
import subprocess
import sys
import time
from contextlib import contextmanager

REPO_DIR = r"C:\ORANIM\oranim-app"

# Script mirroring: the working copies at SCRIPTS_ROOT_DIR are the declared
# source of truth (that is where scripts actually run and get edited); the
# repo's scripts/ folder is an archive that is kept in sync FROM root.
SCRIPTS_ROOT_DIR = r"C:\ORANIM"
SCRIPTS_REPO_SUBDIR = "scripts"

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


def _stash_wip(repo_dir: str, label: str):
    """Stash all uncommitted work (incl. untracked) so a pull/rebase can run on a
    clean tree. Returns the created stash's commit SHA (str) if anything was
    stashed, else None.

    The SHA is returned — not a bool — so _pop_wip can restore *this exact* stash
    even if the stash stack changes underneath us, and so that on a conflicting pop
    we can recover the stashed (freshly-written) file content authoritatively
    instead of silently leaving stale HEAD content in the working tree
    (the data-loss bug of 2026-08-05: a failed `stash pop` orphaned the stash and
    reverted a freshly-scraped data file to the old committed version).
    """
    wt_check = _run(['git', 'diff', '--quiet'], cwd=repo_dir, check=False)
    staged_check = _run(['git', 'diff', '--cached', '--quiet'], cwd=repo_dir, check=False)
    untracked = _run(['git', 'ls-files', '--others', '--exclude-standard'],
                     cwd=repo_dir, check=False)
    if wt_check.returncode == 0 and staged_check.returncode == 0 and not untracked.stdout.strip():
        return None
    stash_res = _run(['git', 'stash', 'push', '-u', '-m', label], cwd=repo_dir, check=False)
    if 'no local changes' in (stash_res.stdout + stash_res.stderr).lower():
        return None
    sha = _run(['git', 'rev-parse', 'stash@{0}'], cwd=repo_dir, check=False).stdout.strip()
    print("[git_sync] stashed other working-tree changes temporarily")
    return sha or 'stash@{0}'


def _pop_wip(repo_dir: str, stash_ref) -> bool:
    """Restore WIP saved by _stash_wip. Returns True on a clean restore.

    NEVER leaves freshly-written working-tree content reverted to old HEAD content,
    and NEVER orphans the stash. `git stash pop` keeps the stash on the stack when
    it conflicts; the old code only logged that and returned, so a fresh data file
    could be left reverted while its new content sat trapped in an orphan stash
    (the 2026-08-05 pikuah_status.json incident). Here, on any pop trouble we:
      1. take the STASHED (fresh) version of every file the stash carried —
         authoritative, because these are files a writer had just produced,
      2. drop the reconciled stash so nothing orphans,
      3. return False and shout on stderr so the caller aborts loudly.
    """
    if not stash_ref:
        return True
    pop_res = _run(['git', 'stash', 'pop'], cwd=repo_dir, check=False)
    out = (pop_res.stdout + pop_res.stderr)
    if pop_res.returncode == 0 and 'conflict' not in out.lower():
        print("[git_sync] restored other working-tree changes")
        return True

    # Conflict / failure: reconcile in favour of the stashed (fresh) content.
    print("[git_sync] WARNING: stash pop conflicted — restoring stashed (fresh) "
          "content authoritatively so nothing is reverted to stale HEAD",
          file=sys.stderr)
    files = _run(['git', 'stash', 'show', '--include-untracked', '--name-only', stash_ref],
                 cwd=repo_dir, check=False).stdout.split()
    _run(['git', 'reset', '-q'], cwd=repo_dir, check=False)  # clear half-applied conflict index
    for f in files:
        r = _run(['git', 'checkout', stash_ref, '--', f], cwd=repo_dir, check=False)
        if r.returncode != 0:
            # untracked-in-stash file lives on the stash's 3rd parent
            _run(['git', 'checkout', f'{stash_ref}^3', '--', f], cwd=repo_dir, check=False)
    # The conflicting pop kept the stash on the stack; drop it now that we've
    # reconciled. `git stash drop` needs a stash@{N} ref, not a raw SHA — resolve
    # our SHA to its current stack position (defensive against concurrent entries).
    listing = _run(['git', 'stash', 'list', '--format=%gd %H'], cwd=repo_dir, check=False).stdout
    for line in listing.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1] == stash_ref:
            _run(['git', 'stash', 'drop', parts[0]], cwd=repo_dir, check=False)
            break
    print("[git_sync] ERROR: WIP restored from stash but the pop conflicted — "
          "review the working tree. No stale data was pushed; no stash was orphaned.",
          file=sys.stderr)
    return False


def _assert_committed_matches_disk(repo_dir: str, relative_path: str):
    """Guard against pushing stale content: verify the just-committed content for
    relative_path matches what is on disk. Uses `git diff --quiet HEAD` so git's own
    line-ending / .gitattributes normalisation applies (a raw byte compare gives
    false mismatches on Windows autocrlf). Raises on divergence so a
    reverted/clobbered file is caught BEFORE it is pushed, never silently.
    """
    abs_path = os.path.join(repo_dir, relative_path)
    if not os.path.exists(abs_path):
        return
    diff = subprocess.run(['git', 'diff', '--quiet', 'HEAD', '--', relative_path],
                          cwd=repo_dir, capture_output=True)
    if diff.returncode != 0:
        raise RuntimeError(
            f"[git_sync] ABORT: working-tree {relative_path} does NOT match the commit "
            "just made — refusing to push possibly-stale content. (Was the file "
            "reverted by a stash pop between write and commit?)")


def pull_before_read(repo_dir: str = REPO_DIR) -> bool:
    """
    Pull latest from origin before reading local files.
    Returns True if successful, False otherwise.

    Auto-stashes unstaged changes around the pull so unrelated work-in-progress
    (e.g. data/projector_*.geojson, index.html) doesn't block the fast-forward.
    """
    print(f"[git_sync] Pulling latest from origin in {repo_dir}...")
    with repo_lock('pull_before_read'):
        wip = None
        try:
            _run(['git', 'fetch', 'origin'], cwd=repo_dir)
            wip = _stash_wip(repo_dir, 'git_sync auto-pre-pull')
            result = _run(['git', 'pull', '--ff-only', 'origin', 'master'], cwd=repo_dir, check=False)
            if result.returncode != 0:
                print("[git_sync] WARNING: Could not fast-forward. Local may be diverged.")
                return False
            return True
        except Exception as e:
            print(f"[git_sync] ERROR: {e}", file=sys.stderr)
            return False
        finally:
            if wip:
                _pop_wip(repo_dir, wip)


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
        wip = None
        try:
            wip = _stash_wip(repo_dir, f'git_sync auto-update: {relative_path}')
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
                _assert_committed_matches_disk(repo_dir, relative_path)
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
            if wip:
                _pop_wip(repo_dir, wip)


def _norm_bytes(path: str):
    """File bytes with line endings normalised to LF, or None if absent.

    Used so a pure CRLF/LF difference never counts as a real change — the repo's
    .gitattributes already checks scripts out as LF, but the working root may hold
    either, and we must not mirror (or commit) a line-ending-only difference.
    """
    try:
        with open(path, 'rb') as f:
            return f.read().replace(b'\r\n', b'\n').replace(b'\r', b'\n')
    except OSError:
        return None


def _commit_and_push_paths(relative_paths: list, message: str,
                           repo_dir: str = REPO_DIR) -> bool:
    """Stage, commit and push a set of already-written files, rebase-safe.

    Multi-file sibling of commit_and_push_after_write. Commits ONLY the given
    pathspec (unrelated staged work is left alone). On a rebase conflict the
    rebase is aborted and the commit undone (changes kept in the working tree);
    the repo is never left mid-rebase.
    """
    with repo_lock('mirror scripts'):
        wip = None
        try:
            _run(['git', 'add', '--'] + relative_paths, cwd=repo_dir)
            staged = _run(['git', 'diff', '--cached', '--quiet', '--'] + relative_paths,
                          cwd=repo_dir, check=False)
            if staged.returncode == 0:
                print("[git_sync] nothing staged after add — skipping push.")
                return True
            _run(['git', 'commit', '-m', message, '--'] + relative_paths, cwd=repo_dir)
            for p in relative_paths:
                _assert_committed_matches_disk(repo_dir, p)
            # Stash unrelated WIP so the pre-push rebase runs on a clean tree.
            wip = _stash_wip(repo_dir, 'git_sync auto-pre-mirror-push')
            rebase = _run(['git', 'pull', '--rebase', 'origin', 'master'], cwd=repo_dir, check=False)
            if rebase.returncode != 0:
                print("[git_sync] pull --rebase failed during mirror — aborting rebase",
                      file=sys.stderr)
                _run(['git', 'rebase', '--abort'], cwd=repo_dir, check=False)
                _run(['git', 'reset', '--mixed', 'HEAD~1'], cwd=repo_dir, check=False)
                print("[git_sync] ERROR: mirror push skipped; changes left in working tree.",
                      file=sys.stderr)
                return False
            _run(['git', 'push', 'origin', 'master'], cwd=repo_dir)
            return True
        except Exception as e:
            print(f"[git_sync] ERROR: {e}", file=sys.stderr)
            return False
        finally:
            if wip:
                _pop_wip(repo_dir, wip)


def _pending_mirrored_scripts(root_dir: str, repo_dir: str) -> list:
    """Repo-relative scripts/*.py that are modified-but-uncommitted AND already equal
    to their root working copy — i.e. a previous mirror copied them but its commit
    or push failed (_commit_and_push_paths undoes the commit with reset --mixed,
    leaving the copies in the tree). The working-tree compare then saw root == repo
    and printed "already in sync" forever, never retrying (audit 2026-10-06).
    Untracked/deleted entries and repo edits that differ from root are left alone."""
    st = _run(['git', 'status', '--porcelain', '--', SCRIPTS_REPO_SUBDIR + '/'],
              cwd=repo_dir, check=False)
    pending = []
    for line in st.stdout.splitlines():
        code, path = line[:2], line[3:].strip().strip('"')
        if 'M' not in code or not path.endswith('.py'):
            continue
        name = os.path.basename(path)
        root_bytes = _norm_bytes(os.path.join(root_dir, name))
        if root_bytes is not None and root_bytes == _norm_bytes(os.path.join(repo_dir, path)):
            pending.append(f"{SCRIPTS_REPO_SUBDIR}/{name}")
    return pending


def _push_unpushed_scripts_commits(repo_dir: str) -> bool:
    """A mirror commit whose `git push` itself failed stays committed locally (no
    reset), so the next run also saw "in sync" and never pushed it (audit
    2026-10-06). If HEAD has unpushed commits touching scripts/, push now."""
    ahead = _run(['git', 'rev-list', '--count', 'origin/master..HEAD', '--',
                  SCRIPTS_REPO_SUBDIR + '/'], cwd=repo_dir, check=False)
    n = ahead.stdout.strip()
    if ahead.returncode != 0 or not n.isdigit():
        print("[git_sync] ERROR: could not count unpushed scripts/ commits.", file=sys.stderr)
        return False
    if int(n) == 0:
        return True
    print(f"[git_sync] {n} unpushed commit(s) touching scripts/ — pushing.")
    with repo_lock('mirror push unpushed'):
        push = _run(['git', 'push', 'origin', 'master'], cwd=repo_dir, check=False)
    if push.returncode != 0:
        print("[git_sync] ERROR: push of unpushed scripts/ commits failed.", file=sys.stderr)
        return False
    return True


def mirror_scripts_to_repo(root_dir: str = SCRIPTS_ROOT_DIR,
                           repo_dir: str = REPO_DIR,
                           push: bool = True):
    """Mirror the working-copy scripts at `root_dir` INTO the repo's scripts/ folder.

    Direction is FIXED: root is the declared source of truth. Only files that
    ALREADY exist in the repo's scripts/ folder are mirrored — a new, un-archived
    root script is never auto-added, so this can never flood the repo with the
    hundreds of loose scripts at root. To start tracking a new script, copy it into
    scripts/ once (and commit); it stays mirrored thereafter.

    Content is compared line-ending-insensitively, so a pure CRLF/LF difference is
    never mirrored. Returns (changed, ok): the repo-relative paths updated (empty if
    in sync) and False if the pull, commit or push failed. It used to return only
    the list and swallow every failure, so the CLI always said "mirror done" and
    exited 0 (audit 2026-10-06). When push=False the files are copied but not
    committed (dry inspection).
    """
    # Refresh repo copies first so the comparison and the push start from the tip.
    # A failed pull used to be ignored; a diverged repo can't be pushed anyway.
    if not pull_before_read(repo_dir):
        print("[git_sync] ERROR: pull failed — mirror skipped.", file=sys.stderr)
        return [], False
    # Copies left uncommitted by an earlier failed mirror are pending work, too.
    pending = _pending_mirrored_scripts(root_dir, repo_dir) if push else []
    repo_scripts = os.path.join(repo_dir, SCRIPTS_REPO_SUBDIR)
    changed = []
    for repo_path in sorted(glob.glob(os.path.join(repo_scripts, '*.py'))):
        name = os.path.basename(repo_path)
        root_path = os.path.join(root_dir, name)
        root_bytes = _norm_bytes(root_path)
        if root_bytes is None:
            continue  # no working copy for this archived script — leave it be
        if root_bytes == _norm_bytes(repo_path):
            continue  # identical content (ignoring line endings)
        shutil.copyfile(root_path, repo_path)
        changed.append(f"{SCRIPTS_REPO_SUBDIR}/{name}")
    if pending:
        print(f"[git_sync] {len(pending)} script(s) copied by an earlier run but never "
              "committed/pushed: " + ", ".join(os.path.basename(p) for p in pending))
    to_commit = sorted(set(changed) | set(pending))
    if not to_commit:
        print("[git_sync] scripts already in sync — nothing to mirror.")
        return [], (_push_unpushed_scripts_commits(repo_dir) if push else True)
    if changed:
        print(f"[git_sync] mirrored {len(changed)} script(s) root -> repo: "
              + ", ".join(os.path.basename(c) for c in changed))
    if push:
        ok = _commit_and_push_paths(
            to_commit, f"chore: mirror {len(to_commit)} script(s) from working root", repo_dir)
        if not ok:
            print("[git_sync] ERROR: files copied but commit/push failed — see errors above.",
                  file=sys.stderr)
            return to_commit, False
        # "nothing staged" (line-ending-only pending) returns True without pushing:
        # make sure nothing committed is left behind origin.
        return to_commit, _push_unpushed_scripts_commits(repo_dir)
    return to_commit, True


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
        wip = None
        try:
            status = _run(['git', 'status', '--porcelain', relative_path], cwd=repo_dir, check=False)
            if not status.stdout.strip():
                print("[git_sync] No changes to commit — skipping push.")
                return True
            _run(['git', 'add', relative_path], cwd=repo_dir)
            _run(['git', 'commit', '-m', message], cwd=repo_dir)
            # Verify the commit captured the on-disk content BEFORE we touch the
            # working tree (stash) — catches a file reverted between write & commit.
            _assert_committed_matches_disk(repo_dir, relative_path)

            # Stash other unstaged work (incl. untracked) so pull --rebase succeeds
            wip = _stash_wip(repo_dir, f'git_sync auto-pre-push: {relative_path}')

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
            if wip:
                _pop_wip(repo_dir, wip)


MIRROR_LOG = r"C:\ORANIM\permits_scan_reports\script_mirror_last_run.log"


class _Tee:
    def __init__(self, *streams):
        self.streams = [s for s in streams if s is not None]

    def write(self, s):
        for st in self.streams:
            try:
                st.write(s)
            except Exception:
                pass
        return len(s)

    def flush(self):
        for st in self.streams:
            try:
                st.flush()
            except Exception:
                pass


def _mirror_cli(push: bool) -> int:
    """`git_sync.py mirror` as run nightly by Oranim_ScriptMirror_Daily. It had no
    log, ignored pull/push failures and always exited 0 with "mirror done", so a
    stuck mirror went unnoticed (audit 2026-10-06). Now: rotated log + stdout,
    exit 1 and an alert email on any failure."""
    sys.path.insert(0, SCRIPTS_ROOT_DIR)
    try:
        from rotate_log import rotate
        rotate(MIRROR_LOG)
    except Exception as e:  # rotation must never block the job itself
        print(f"[rotate_log] {e}")
    try:
        log = open(MIRROR_LOG, 'w', encoding='utf-8', buffering=1)
        sys.stdout = _Tee(sys.stdout, log)
        sys.stderr = _Tee(sys.stderr, log)
    except OSError as e:
        print(f"[git_sync] cannot open log {MIRROR_LOG}: {e}")
    print(f"[git_sync] mirror run {time.strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        changed, ok = mirror_scripts_to_repo(push=push)
        err = ''
    except Exception as e:
        import traceback
        traceback.print_exc()
        changed, ok, err = [], False, repr(e)
    if ok:
        print(f"[git_sync] mirror done: {len(changed)} script(s) updated.")
        return 0
    print(f"[git_sync] mirror FAILED ({len(changed)} script(s) pending) — exit 1", file=sys.stderr)
    sys.stdout.flush()
    try:
        from ops_alert import send_alert, log_tail
        from pathlib import Path
        send_alert("סנכרון סקריפטים לרפו נכשל",
                   "ה-mirror הלילי של הסקריפטים ל-oranim-app/scripts נכשל (pull/commit/push).\n"
                   "הקבצים יישארו ממתינים וינוסו שוב בריצה הבאה.\n"
                   + (f"{err}\n" if err else "")
                   + f"pending: {', '.join(changed) or '-'}\n\nlog: {MIRROR_LOG}\n\n"
                   + log_tail(Path(MIRROR_LOG), 40))
    except Exception as e:
        print(f"[git_sync] alert failed: {e}", file=sys.stderr)
    return 1


if __name__ == '__main__':
    import argparse

    ap = argparse.ArgumentParser(description="git_sync utilities")
    ap.add_argument('command', nargs='?', default='mirror', choices=['mirror'],
                    help="mirror: sync working-root scripts into repo scripts/ and push")
    ap.add_argument('--no-push', action='store_true',
                    help="copy changed scripts into the repo but do not commit/push")
    _args = ap.parse_args()
    if _args.command == 'mirror':
        sys.exit(_mirror_cli(push=not _args.no_push))
