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


def _email_alert(diverged):
    if not EMAIL_PASSWORD:
        print("[mirror-check] GMAIL_APP_PASSWORD not set — skipping email alert")
        return
    body = ("הסקריפטים הבאים שונים בין C:\\ORANIM לבין oranim-app\\scripts.\n"
            "צריך להחליט כיוון סנכרון (לא תמיד העותק החדש הוא הנכון!) ולדחוף.\n\n"
            + "\n".join(diverged)
            + "\n\nבדיקה: python check_script_mirrors.py")
    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = f"[Oranim] {len(diverged)} סקריפטים מפוצלים בין לוקאל לרפו"
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

    print(f"[mirror-check] {pairs} mirrored scripts checked")
    if not diverged:
        print("[mirror-check] all in sync")
        return 0
    print(f"[mirror-check] {len(diverged)} DIVERGED:")
    for d in diverged:
        print("  " + d)
    _email_alert(diverged)
    return 1


if __name__ == '__main__':
    sys.exit(main())
