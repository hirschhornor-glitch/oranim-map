# -*- coding: utf-8 -*-
r"""
check_script_mirrors.py — weekly guard against script-mirror drift.

Operational scripts live in C:\ORANIM (where the scheduled tasks run them)
with a versioned mirror in oranim-app\scripts. Nothing enforces that the two
copies stay identical, and they drift (plan_lookup / detect_new_plans /
enrich_tama38_developers all diverged silently before 2026-07-20).

This script compares every same-named pair (CRLF-insensitive), prints a
report, and exits 1 so the calling batch can flag it. Run weekly from
run_all_committees.bat.

Email is OPT-IN (--email). The natural workflow is edit-in-root -> run this
check -> mirror -> commit, so an ad-hoc run (a human's, or an agent's own
closing checklist) lands mid-edit: on 2026-08-31 it mailed a "problem" that
the same session fixed 44 seconds later. Only the scheduled batch passes
--email. That batch races the same way, so a second guard covers it: a
divergence whose newer copy was touched within --grace-minutes (default 60)
is printed as in-flight but withheld from the email.

It does NOT auto-sync: direction matters (sometimes the repo copy is the
fixed one, e.g. scope_filter's CI path fallback), so a human/agent decides.
"""
import os
import sys
import ast
import time
import argparse
import datetime
import smtplib
from email.mime.text import MIMEText

ROOT = r"C:\ORANIM"
REPO_SCRIPTS = r"C:\ORANIM\oranim-app\scripts"

EMAIL_SENDER = "hirschhorn.or@gmail.com"
EMAIL_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "")
EMAIL_RECIPIENT = os.environ.get("EMAIL_RECIPIENT", "Or_hi@jerusalem.muni.il")


def _norm(path):
    with open(path, 'rb') as f:
        return f.read().replace(b'\r\n', b'\n')


def _local_imports(path):
    """Absolute module names imported UNCONDITIONALLY at module scope by `path`.

    Only direct children of the module body count — these are the imports that
    raise ImportError the moment the file is imported/run. Imports nested inside
    a function, `try/except`, or `if` are deliberately lazy/optional (e.g.
    update_mavat_ui's guarded `import build_unit_bonus`) and must NOT be flagged.
    Relative imports (from . / from ..) are skipped — they can't name a sibling
    script by bare name anyway."""
    try:
        tree = ast.parse(_norm(path), filename=path)
    except SyntaxError:
        return set()
    mods = set()
    for node in tree.body:                            # module scope only
        if isinstance(node, ast.Import):
            for a in node.names:
                mods.add(a.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:       # absolute import only
                mods.add(node.module.split('.')[0])
    return mods


def _check_missing_deps(grace_seconds=0):
    """Second guard: a versioned script that imports a LOCAL sibling module
    (a bare `import foo` where foo.py lives next to the operational scripts in
    C:\\ORANIM) is broken in the repo if foo.py was never mirrored into
    oranim-app\\scripts — `python scripts\\that_script.py` / CI would ImportError.
    The pair-diff above never catches this: it only compares same-named pairs
    and silently skips modules present on one side only.
    (This is exactly how detect_unit_bonus_note slipped through on 2026-08-03.)

    Returns (missing, fresh): `fresh` is the subset whose local module was
    written within grace_seconds — a module created minutes ago is mid-workflow,
    not a lasting gap, so it is reported but not emailed."""
    missing = []
    fresh = []
    now = time.time()
    for name in sorted(os.listdir(REPO_SCRIPTS)):
        if not name.endswith('.py'):
            continue
        repo_p = os.path.join(REPO_SCRIPTS, name)
        for mod in sorted(_local_imports(repo_p)):
            # "local" == a sibling script exists in ROOT; skip stdlib/3rd-party.
            if not os.path.isfile(os.path.join(ROOT, mod + '.py')):
                continue
            if not os.path.isfile(os.path.join(REPO_SCRIPTS, mod + '.py')):
                line = f"{name} imports {mod} -> {mod}.py missing from repo"
                missing.append(line)
                if now - os.path.getmtime(os.path.join(ROOT, mod + '.py')) < grace_seconds:
                    fresh.append(line)
    return missing, fresh


def _email_alert(diverged, missing):
    if not EMAIL_PASSWORD:
        print("[mirror-check] GMAIL_APP_PASSWORD not set — skipping email alert")
        return
    body = ""
    if diverged:
        body += ("הסקריפטים הבאים שונים בין C:\\ORANIM לבין oranim-app\\scripts.\n"
                 "צריך להחליט כיוון סנכרון (לא תמיד העותק החדש הוא הנכון!) ולדחוף.\n\n"
                 + "\n".join(diverged) + "\n\n")
    if missing:
        body += ("סקריפטים מגובים שמייבאים מודול לוקאלי שלא הועתק לרפו "
                 "(העותק ברפו ישבור ב-ImportError):\n\n"
                 + "\n".join(missing) + "\n\n")
    body += "בדיקה: python check_script_mirrors.py"
    msg = MIMEText(body, 'plain', 'utf-8')
    n = len(diverged) + len(missing)
    msg['Subject'] = f"[Oranim] {n} בעיות סנכרון סקריפטים בין לוקאל לרפו"
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_RECIPIENT
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, timeout=30) as s:
            s.login(EMAIL_SENDER, EMAIL_PASSWORD)
            s.send_message(msg)
        print(f"[mirror-check] alert emailed to {EMAIL_RECIPIENT}")
    except Exception as e:
        print(f"[mirror-check] email failed: {e}")


def main(email=False, grace_minutes=60):
    grace_seconds = max(0, grace_minutes) * 60
    now = time.time()
    diverged = []
    fresh = set()          # in flight: printed, never emailed
    pairs = 0
    for name in sorted(os.listdir(REPO_SCRIPTS)):
        if not name.endswith('.py'):
            continue
        root_p = os.path.join(ROOT, name)
        repo_p = os.path.join(REPO_SCRIPTS, name)
        if not os.path.isfile(root_p):
            continue
        pairs += 1
        if _norm(root_p) != _norm(repo_p):
            mt_root = os.path.getmtime(root_p)
            mt_repo = os.path.getmtime(repo_p)
            rt = datetime.date.fromtimestamp(mt_root)
            pt = datetime.date.fromtimestamp(mt_repo)
            newer = 'root' if mt_root > mt_repo else 'repo'
            line = f"{name}: root {rt} vs repo {pt} (newer: {newer})"
            diverged.append(line)
            if now - max(mt_root, mt_repo) < grace_seconds:
                fresh.add(line)

    missing, fresh_missing = _check_missing_deps(grace_seconds)
    fresh.update(fresh_missing)

    print(f"[mirror-check] {pairs} mirrored scripts checked")
    if not diverged and not missing:
        print("[mirror-check] all in sync")
        return 0
    def _show(line):
        return "  " + line + ("   [in flight — not emailed]" if line in fresh else "")

    if diverged:
        print(f"[mirror-check] {len(diverged)} DIVERGED:")
        for d in diverged:
            print(_show(d))
    if missing:
        print(f"[mirror-check] {len(missing)} MISSING DEPENDENCIES in repo:")
        for m in missing:
            print(_show(m))

    if not email:
        print("[mirror-check] --email not given — report only, no alert sent")
        return 1
    mail_div = [d for d in diverged if d not in fresh]
    mail_missing = [m for m in missing if m not in fresh]
    if not mail_div and not mail_missing:
        print(f"[mirror-check] all findings edited within {grace_minutes}min "
              "(still in flight) — no alert sent")
        return 1
    _email_alert(mail_div, mail_missing)
    return 1


if __name__ == '__main__':
    ap = argparse.ArgumentParser(
        description=r"Compare C:\ORANIM scripts against their oranim-app\scripts mirrors.")
    ap.add_argument('--email', action='store_true',
                    help="send the alert email (scheduled batch only; ad-hoc runs "
                         "report to stdout so a mid-edit check does not mail the user)")
    ap.add_argument('--grace-minutes', type=int, default=60, metavar='N',
                    help="do not email a finding whose newer copy was touched in the "
                         "last N minutes — it is mid-workflow (default: 60)")
    _a = ap.parse_args()
    sys.exit(main(email=_a.email, grace_minutes=_a.grace_minutes))
