"""
push_to_oranim_map.py

Helper שדוחף קבצי דאטה ספציפיים לריפו oranim-app (אשר ה-remote שלו הוא github.com/.../oranim-map).

תומך ב-stash אוטומטי של עבודה לא-קומיטת אחרת — כך ש-pull --rebase לא יקרוס.

שימוש:
    python push_to_oranim_map.py "סיבת ה-commit" data/file1.json data/file2.geojson

דוגמה:
    python push_to_oranim_map.py "auto-update meetings.json" data/meetings.json
"""
import sys
import subprocess

# Reuse the hardened, lock-protected git helpers so this script cannot (a) run its
# stash/pop concurrently with git_sync.py and pop the WRONG stash@{0}, or (b) leave
# a freshly-written data file reverted to old HEAD content on a conflicting pop.
# (Root cause of the 2026-08-05 pikuah_status.json silent-revert incident.)
from git_sync import (
    REPO_DIR as REPO,
    repo_lock,
    _stash_wip,
    _pop_wip,
    _assert_committed_matches_disk,
    record_failure,
)

BRANCH = "master"


def run_git(args, **kw):
    """Helper that runs git with the right cwd + utf-8 encoding."""
    return subprocess.run(
        ["git"] + args,
        cwd=REPO,
        capture_output=True, text=True,
        encoding="utf-8", errors="replace",
        **kw,
    )


def _push():
    commit_msg = sys.argv[1]
    files = sys.argv[2:]

    # 1. Stage the requested files
    for f in files:
        r = run_git(["add", f])
        if r.returncode != 0:
            print(f"[git add {f}] error: {r.stderr.strip()}")

    # 2. Check if anything is actually staged → commit if yes
    diff_check = run_git(["diff", "--cached", "--quiet"])
    if diff_check.returncode != 0:
        commit_res = run_git(["commit", "-m", commit_msg])
        if commit_res.returncode != 0:
            print(f"[git commit] error: {commit_res.stderr.strip()}")
            return 1
        print(f"[git] committed: {commit_msg}")
        # 2b. Verify each target committed byte-for-byte matches disk BEFORE we
        #     stash/rebase — catches a file reverted between write and commit.
        for f in files:
            _assert_committed_matches_disk(REPO, f)
    else:
        print(f"[git] no new staged changes for: {files}")

    # 3. Check if there are local commits ahead of remote (from this run OR earlier failed pushes)
    ahead_check = run_git(["log", f"origin/{BRANCH}..HEAD", "--oneline"])
    if not ahead_check.stdout.strip():
        print(f"[git] local is in sync with origin/{BRANCH} — nothing to push")
        return 0
    ahead_lines = ahead_check.stdout.strip().splitlines()
    print(f"[git] {len(ahead_lines)} local commit(s) ahead of origin:")
    for ln in ahead_lines:
        print(f"      {ln}")

    # 4. Stash other unstaged work so pull --rebase can proceed (guarded: the pop
    #    restores stashed content authoritatively and never orphans the stash).
    wip = _stash_wip(REPO, "auto-pre-push-meetings")

    exit_code = 0
    try:
        # 5. Pull --rebase to get latest remote
        pull_res = run_git(["pull", "--rebase", "origin", BRANCH])
        if pull_res.returncode != 0:
            # A conflicting rebase (one-line JSON vs a remote edit) used to fall through
            # to "try push anyway": the repo was left mid-rebase with conflict markers in
            # plans.geojson (2026-10-18). Abort, keep our commits local, fail loudly.
            print(f"[git pull --rebase] error: {pull_res.stderr.strip()[:300]}")
            run_git(["rebase", "--abort"])
            print("[git] rebase aborted — local commits kept, NOT pushed")
            record_failure("pull --rebase", (pull_res.stderr.strip() or pull_res.stdout.strip())[:200])
            exit_code = 1
        else:
            # 6. Push
            push_res = run_git(["push", "origin", BRANCH])
            if push_res.returncode == 0:
                print("[git] pushed successfully")
            else:
                print(f"[git push] failed: {push_res.stderr.strip()[:400]}")
                record_failure("push", push_res.stderr.strip()[:200])
                exit_code = 1
    finally:
        # 7. Restore stashed WIP (loud + lossless on conflict)
        if wip and not _pop_wip(REPO, wip):
            exit_code = 1
    return exit_code


def main():
    if len(sys.argv) < 3:
        print("Usage: python push_to_oranim_map.py <commit-message> <file1> [<file2> ...]")
        sys.exit(2)
    # Serialize against git_sync.py writers on the same repo.
    with repo_lock('push_to_oranim_map'):
        sys.exit(_push())


if __name__ == "__main__":
    main()
