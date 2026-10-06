"""Failure alerts for the scheduled scans — so a broken run is never mistaken
for a quiet one ("0 new"). Same Gmail sender/recipient as the permits email."""
from __future__ import annotations

import html
import os
import re
import smtplib
from email.mime.text import MIMEText
from pathlib import Path

EMAIL_SENDER = "hirschhorn.or@gmail.com"
EMAIL_PASSWORD_ENV = "GMAIL_APP_PASSWORD"
EMAIL_RECIPIENT_ENV = "PERMITS_EMAIL_TO"
EMAIL_RECIPIENT_DEFAULT = "Or_hi@jerusalem.muni.il"


def log_tail(path: Path, lines: int = 40) -> str:
    try:
        return "\n".join(path.read_text(encoding="utf-8", errors="replace").splitlines()[-lines:])
    except OSError:
        return ""


_HEBREW = re.compile(r"[֐-׿]")


def alert_html(body: str) -> str:
    """Plain-text alert body -> HTML. Lines with Hebrew are RTL prose; every other
    run of lines (tracebacks, log tails, commands) goes into a <pre dir="ltr">.
    Sent as plain text, the mail client laid the whole body out RTL (the subject
    is Hebrew), which scrambled tracebacks: lines broke apart and the ^^^ markers
    landed under the wrong code (alert of 2026-10-06)."""
    blocks = []  # [is_hebrew, [lines]]
    for line in body.splitlines():
        heb = bool(_HEBREW.search(line))
        if not line.strip() and blocks:
            heb = blocks[-1][0]          # blank lines stay with the block they're in
        if blocks and blocks[-1][0] == heb:
            blocks[-1][1].append(line)
        else:
            blocks.append([heb, [line]])
    parts = []
    for heb, lines in blocks:
        while lines and not lines[-1].strip():
            lines.pop()
        if not lines:
            continue
        text = html.escape("\n".join(lines))
        if heb:
            parts.append(f'<p dir="rtl" style="margin:0 0 10px;white-space:pre-wrap">{text}</p>')
        else:
            parts.append('<pre dir="ltr" style="text-align:left;direction:ltr;background:#f5f5f5;'
                         'border:1px solid #ddd;padding:8px;font-size:12px;white-space:pre-wrap;'
                         f'margin:0 0 10px">{text}</pre>')
    return ('<div dir="rtl" style="font-family:Arial,sans-serif;font-size:14px">'
            + "".join(parts) + "</div>")


def send_alert(subject: str, body: str) -> bool:
    return send_email(f"⚠️ {subject}", alert_html(body), "html")


def send_email(subject: str, body: str, subtype: str = "html") -> bool:
    password = os.environ.get(EMAIL_PASSWORD_ENV, "")
    recipient = os.environ.get(EMAIL_RECIPIENT_ENV, EMAIL_RECIPIENT_DEFAULT)
    if not password:
        print(f"[alert] {EMAIL_PASSWORD_ENV} not set - cannot send: {subject}")
        return False
    msg = MIMEText(body, subtype, "utf-8")
    msg["From"] = EMAIL_SENDER
    msg["To"] = recipient
    msg["Subject"] = subject
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_SENDER, password)
            server.send_message(msg)
        print(f"[alert] sent to {recipient}: {subject}")
        return True
    except Exception as e:
        print(f"[alert] send failed: {e}")
        return False
