# -*- coding: utf-8 -*-
r"""
check_script_mirrors.py — weekly guard against script-mirror drift.

Operational scripts live in C:\ORANIM (where the scheduled tasks run them)
with a versioned mirror in oranim-app\scripts. Nothing enforces that the two
copies stay identical, and they drift (plan_lookup / detect_new_plans /
enrich_tama38_developers all diverged silently before 2026-07-20).

This script compares every same-named pair (CRLF-insensitive), prints a
report, emails an alert when something diverged, and exits 1 so the calling
batch can flag it. Run weekly from run_all_committees.bat.

It does NOT auto-sync: direction matters (sometimes the repo copy is the
fixed one, e.g. scope_filter's CI path fallback), so a human/agent decides.
"""
import os
import sys
import ast
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


def _check_missing_deps():
    """Second guard: a versioned script that imports a LOCAL sibling module
    (a bare `import foo` where foo.py lives next to the operational scripts in
    C:\\ORANIM) is broken in the repo if foo.py was never mirrored into
    oranim-app\\scripts — `python scripts\\that_script.py` / CI would ImportError.
    The pair-diff above never catches this: it only compares same-named pairs
    and silently skips modules present on one side only.
    (This is exactly how detect_unit_bonus_note slipped through on 2026-08-03.)"""
    missing = []
    for name in sorted(os.listdir(REPO_SCRIPTS)):
        if not name.endswith('.py'):
            continue
        repo_p = os.path.join(REPO_SCRIPTS, name)
        for mod in sorted(_local_imports(repo_p)):
            # "local" == a sibling script exists in ROOT; skip stdlib/3rd-party.
            if not os.path.isfile(os.path.join(ROOT, mod + '.py')):
                continue
            if not os.path.isfile(os.path.join(REPO_SCRIPTS, mod + '.py')):
                missing.append(f"{name} imports {mod} -> {mod}.py missing from repo")
    return missing


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


def main():
    diverged = []
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
            rt = datetime.date.fromtimestamp(os.path.getmtime(root_p))
            pt = datetime.date.fromtimestamp(os.path.getmtime(repo_p))
            newer = 'root' if os.path.getmtime(root_p) > os.path.getmtime(repo_p) else 'repo'
            diverged.append(f"{name}: root {rt} vs repo {pt} (newer: {newer})")

    missing = _check_missing_deps()

    print(f"[mirror-check] {pairs} mirrored scripts checked")
    if not diverged and not missing:
        print("[mirror-check] all in sync")
        return 0
    if diverged:
        print(f"[mirror-check] {len(diverged)} DIVERGED:")
        for d in diverged:
            print("  " + d)
    if missing:
        print(f"[mirror-check] {len(missing)} MISSING DEPENDENCIES in repo:")
        for m in missing:
            print("  " + m)
    _email_alert(diverged, missing)
    return 1


if __name__ == '__main__':
    sys.exit(main())
