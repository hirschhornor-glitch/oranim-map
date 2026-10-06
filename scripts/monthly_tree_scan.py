"""
monthly_tree_scan.py — scheduled monthly tree-survey refresh.
--------------------------------------------------------------------
Two complementary stages, because tree data lives in two sources:

  STAGE A — COUNTS (from XPLAN, quantitative):
    fetch_tree_points_from_xplan.py re-scans EVERY plan's tree points.
    The weekly status-check (update_mavat_ui) only refreshes counts for
    plans whose Mavat status changed; this full scan also catches plans
    whose tree points changed WITHOUT a status change (its blind spot).

  STAGE B — VALENCY (from PDF surveys, qualitative):
    XPLAN has NO valency (ערכיות) — that lives only in the agronomist's
    PDF survey. This stage downloads new survey PDFs from Mavat, extracts
    the valency × recommendation cross-tab, and rebuilds tree_valencies.json.
      download_tree_surveys.py  -> tree_surveys/*.pdf   (Playwright, resumable;
        never-checked plans first, then a rolling re-check window — one run
        covers ~300 of 620 in its 3h slot, so a full pass takes ~2 runs)
      run_tree_valencies_all.py -> tree_surveys/valency_pivots.json  (PyMuPDF)
      build_tree_valencies_app_json.py -> data/tree_valencies.json

Flow: git pull -> Stage A -> Stage B -> commit changed files -> unified email.

Scheduled monthly. Needs GMAIL_APP_PASSWORD env var (User scope), same as the
permits/objections scans. Recipient override: PERMITS_EMAIL_TO.

Usage:
  python monthly_tree_scan.py                 # both stages + commit + email
  python monthly_tree_scan.py --no-valency    # counts only (skip PDF stage)
  python monthly_tree_scan.py --valency-only   # valency only (skip XPLAN counts)
  python monthly_tree_scan.py --no-email       # run stages, skip email
  python monthly_tree_scan.py --dry-run        # counts via --dry-run; skips valency (it writes files)
"""

import argparse
import json
import os
import smtplib
import subprocess
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from git_sync import pull_before_read, commit_and_push_after_write

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# ─── Config ──────────────────────────────────────────────────────────────────
ROOT            = r"C:\ORANIM"
REPO_DIR        = r"C:\ORANIM\oranim-app"
PLANS_GEOJSON   = r"C:\ORANIM\oranim-app\data\plans.geojson"
LOG_PATH        = r"C:\ORANIM\tree_scan_log.txt"

# Stage A (counts / XPLAN)
TREE_SURVEYS    = r"C:\ORANIM\oranim-app\data\tree_surveys.json"
FETCH_SCRIPT    = r"C:\ORANIM\fetch_tree_points_from_xplan.py"
COUNT_PUSH      = ["data/tree_surveys.json", "data/tree_points_xplan.json"]

# Stage B (valency / PDF)
TREE_VALENCIES  = r"C:\ORANIM\oranim-app\data\tree_valencies.json"
DOWNLOAD_SCRIPT = r"C:\ORANIM\download_tree_surveys.py"
EXTRACT_SCRIPT  = r"C:\ORANIM\run_tree_valencies_all.py"
BUILD_VAL_SCRIPT= r"C:\ORANIM\build_tree_valencies_app_json.py"
FILTER_SCRIPT   = r"C:\ORANIM\filter_valency_by_boundary.py"   # drop out-of-scope (straddler) plans
VALENCY_PUSH    = "data/tree_valencies.json"

# Stage B2 (TAMA 38 — full refresh: download new surveys → counts + valency).
# Valency popup wiring is a follow-up; counts already feed the existing popup.
TAMA38_DL_SCRIPT   = r"C:\ORANIM\download_tama38_tree_surveys.py"    # ykpubdata, Playwright
HARMONICA_SCRIPT   = r"C:\ORANIM\harmonica_tree_surveys.py"          # content-scan generic attachments
TAMA38_PARSE       = r"C:\ORANIM\parse_tama38_tree_surveys.py"       # -> parsed_results.json
TAMA38_COUNT_BUILD = r"C:\ORANIM\build_tama38_tree_surveys_json.py"  # -> tama38_tree_surveys.json
TAMA38_VAL_SCRIPT  = r"C:\ORANIM\build_tama38_valencies.py"          # -> tama38_tree_valencies.json
TAMA38_SURVEYS     = r"C:\ORANIM\oranim-app\data\tama38_tree_surveys.json"
TAMA38_VALENCIES   = r"C:\ORANIM\oranim-app\data\tama38_tree_valencies.json"
TAMA38_SURVEYS_PUSH = "data/tama38_tree_surveys.json"
TAMA38_VAL_PUSH     = "data/tama38_tree_valencies.json"

# Email — mirrors biweekly_permits_scan.py conventions.
EMAIL_SENDER      = "hirschhorn.or@gmail.com"
EMAIL_PASSWORD    = os.environ.get("GMAIL_APP_PASSWORD", "")
EMAIL_RECIPIENT   = os.environ.get("PERMITS_EMAIL_TO", "Or_hi@jerusalem.muni.il")


# ─── Shared helpers ──────────────────────────────────────────────────────────

# Problems seen this run. A stage that failed used to return None exactly like a
# stage that was skipped on purpose, so tree_scan_log.txt said "valency: skipped",
# no email warned, and the task showed success — valency_pivots.json went unwritten
# from 2026-08-04 to 2026-10-06. FAILED: stage aborted. PROBLEMS: a "non-fatal"
# step that still broke (e.g. the downloader died at plan 4 on 2026-10-06).
FAILED = {}     # stage -> reason
PROBLEMS = []   # human-readable lines


def _problem(msg):
    print(f"[problem] {msg}")
    PROBLEMS.append(msg)


def _rc_text(rc):
    return "TIMEOUT" if rc == 124 else f"exit {rc}"


def _run(cmd, timeout=None):
    """Run a subprocess in ROOT; return exit code (or 124 on timeout)."""
    print(f"[run] {' '.join(cmd)}")
    try:
        return subprocess.run(cmd, cwd=ROOT, timeout=timeout).returncode
    except subprocess.TimeoutExpired:
        print(f"[run] TIMEOUT after {timeout}s: {' '.join(cmd)}")
        return 124


def load_name_map():
    """taba -> display name, from plans.geojson."""
    try:
        with open(PLANS_GEOJSON, encoding="utf-8") as f:
            g = json.load(f)
    except FileNotFoundError:
        return {}
    m = {}
    for feat in g.get("features", []):
        p = feat.get("properties") or {}
        t = str(p.get("taba") or "").strip()
        if t:
            m[t] = p.get("plan_name_he") or p.get("plan_summary") or p.get("plan_name") or ""
    return m


def esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ─── Stage A: counts (XPLAN) ─────────────────────────────────────────────────

def load_totals(path):
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    return {t: (row.get("total", 0) or 0) for t, row in data.items() if not t.startswith("_")}


def diff_totals(old, new):
    """Returns (added, removed, changed) lists of (taba, old_total, new_total)."""
    added, removed, changed = [], [], []
    for taba in sorted(set(old) | set(new)):
        o, n = old.get(taba, 0), new.get(taba, 0)
        if o == n:
            continue
        if taba not in old or o == 0:
            added.append((taba, o, n))
        elif taba not in new or n == 0:
            removed.append((taba, o, n))
        else:
            changed.append((taba, o, n))
    return added, removed, changed


def run_count_stage(dry_run):
    """Full XPLAN count rescan. Returns (added, removed, changed) or None."""
    old = load_totals(TREE_SURVEYS)
    print(f"[counts] baseline: {len(old)} plans with tree counts")
    cmd = [sys.executable, FETCH_SCRIPT] + (["--dry-run"] if dry_run else [])
    rc = _run(cmd, timeout=1800)
    if rc != 0:
        print(f"[counts] fetch exited {rc} — skipping count stage.")
        FAILED["counts"] = f"fetch {_rc_text(rc)}"
        return None
    if dry_run:
        print("[counts] --dry-run: nothing written.")
        return None
    new = load_totals(TREE_SURVEYS)
    added, removed, changed = diff_totals(old, new)
    print(f"[counts] +{len(added)} new, ~{len(changed)} changed, -{len(removed)} dropped")
    if added or removed or changed:
        net = sum(n - o for _, o, n in added + removed + changed)
        msg = (f"data: monthly tree rescan (+{len(added)} new, ~{len(changed)} changed, "
               f"-{len(removed)} dropped, net {net:+d} trees) (monthly_tree_scan)")
        for rel in COUNT_PUSH:
            commit_and_push_after_write(rel, msg, REPO_DIR)
    return added, removed, changed


# ─── Stage B: valency (PDF) ──────────────────────────────────────────────────

def load_valency(path):
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    return {k: v for k, v in data.items() if not k.startswith("_")}


def diff_valency(old, new):
    """Returns (added, removed, changed) lists of keys. Compares full row content
    (order-independent), so it works for BOTH the valency files (by_bucket…) and
    the tama38 count file ({total,shimur,…}) — and JSON key-order churn, where the
    row content is unchanged, correctly registers as no change."""
    added, removed, changed = [], [], []
    for t in sorted(set(old) | set(new)):
        if t not in old:
            added.append(t)
        elif t not in new:
            removed.append(t)
        elif old[t] != new[t]:
            changed.append(t)
    return added, removed, changed


def run_valency_stage():
    """Download new PDFs -> extract valency -> rebuild app JSON. Returns
    (added, removed, changed, new_data) or None on hard failure."""
    old = load_valency(TREE_VALENCIES)
    print(f"[valency] baseline: {len(old)} plans with valency data")

    # 1. Download survey PDFs. --recheck-empty re-checks ALL plans previously
    #    marked 'no_tree_docs' (a survey may have been uploaded since), not just
    #    newly-added plans — this is the whole point of the monthly full pass.
    #    Headed browser. Non-fatal if it fails/times out — extract still runs on
    #    whatever PDFs already exist.
    #
    #    TIMING (measured 2026-09-01, NOT the ~2-2.5h this comment used to claim):
    #    ~36s/plan × 620 plans = ~6.2h for a full pass, so the 3h cap below is
    #    hit around plan 300 every single month. Do NOT "fix" that by raising the
    #    timeout: the Task Scheduler entry caps the WHOLE run at PT12H (03:00 →
    #    killed 15:00), and the tama38 stage below still needs its hours after
    #    this one — a 7h download would starve it instead.
    #
    #    Coverage is handled by ordering, not by wall time: download_tree_surveys
    #    puts never-checked plans first and walks the re-checks as a rolling
    #    window (tree_surveys/scan_cursor.json), resuming where the last run was
    #    cut off. ~2 runs cover all 620. Before that, the list restarted from the
    #    top every month and the last ~319 plans were never reached at all.
    rc = _run([sys.executable, DOWNLOAD_SCRIPT, "--recheck-empty"], timeout=10800)
    if rc != 0:
        print(f"[valency] download exited {rc} (non-fatal) — extracting existing PDFs.")
        if rc != 124:   # the 3h cap is hit by design (rolling window, see above)
            _problem(f"valency download {_rc_text(rc)} — extracted only the PDFs already on disk")

    # 2. Extract valency from all PDFs (deterministic, no network).
    #    --delete-after: surveys aren't kept after analysis (2026-10-06); each PDF
    #    goes to the Recycle Bin once its result is cached, and stays in the output.
    rc = _run([sys.executable, EXTRACT_SCRIPT, "--delete-after"], timeout=1800)
    if rc != 0:
        print(f"[valency] extract exited {rc} — aborting valency stage.")
        FAILED["valency"] = f"extract {_rc_text(rc)}"
        return None

    # 3. Rebuild the app JSON from the pivots.
    rc = _run([sys.executable, BUILD_VAL_SCRIPT], timeout=300)
    if rc != 0:
        print(f"[valency] build exited {rc} — aborting valency stage.")
        FAILED["valency"] = f"build {_rc_text(rc)}"
        return None

    # 4. Drop valency for plans that mostly fall OUTSIDE the district (the PDF is
    #    not geo-clipped, so straddlers over-count). Decided via XPLAN in/full.
    rc = _run([sys.executable, FILTER_SCRIPT], timeout=600)
    if rc != 0:
        print(f"[valency] boundary filter exited {rc} (non-fatal).")
        _problem(f"valency boundary filter {_rc_text(rc)} — out-of-district plans not dropped")

    new = load_valency(TREE_VALENCIES)
    added, removed, changed = diff_valency(old, new)
    print(f"[valency] +{len(added)} new, ~{len(changed)} changed, -{len(removed)} dropped")

    if added or removed or changed:
        msg = (f"data: monthly valency refresh (+{len(added)} new, ~{len(changed)} changed, "
               f"-{len(removed)} dropped) (monthly_tree_scan)")
        commit_and_push_after_write(VALENCY_PUSH, msg, REPO_DIR)
    else:
        # No real change — discard any order-only churn so the tree stays clean.
        _run(["git", "-C", REPO_DIR, "checkout", "--", VALENCY_PUSH])

    return added, removed, changed, new


def _commit_or_discard(push_path, old_data, new_data, kind):
    """Commit push_path if the diff is real; else discard order-churn. Returns
    (added, changed, removed) counts."""
    a, r, c = diff_valency(old_data, new_data)   # works for counts & valency (both dict-of-dict)
    if a or r or c:
        msg = (f"data: monthly tama38 {kind} (+{len(a)} new, ~{len(c)} changed, "
               f"-{len(r)} dropped) (monthly_tree_scan)")
        commit_and_push_after_write(push_path, msg, REPO_DIR)
    elif old_data:  # only discard churn if already tracked
        _run(["git", "-C", REPO_DIR, "checkout", "--", push_path])
    return len(a), len(c), len(r)


def run_tama38_stage(recheck_empty=True):
    """Full TAMA 38 refresh: search permit files for surveys, re-parse counts,
    rebuild count + valency files, commit both. Returns a summary dict, or None
    if the parse/build chain fails.
      recheck_empty=True  (monthly): also re-check permits marked 'no_tree_docs'.
      recheck_empty=False (catch-up): only permits never processed (the backlog)."""
    old_counts  = load_valency(TAMA38_SURVEYS)     # keyed by tik: {total,...}
    old_valency = load_valency(TAMA38_VALENCIES)

    # 1. Search permit files (ykpubdata optical archive, HEADED browser). Non-fatal.
    dl_cmd = [sys.executable, TAMA38_DL_SCRIPT] + (["--recheck-empty"] if recheck_empty else [])
    rc = _run(dl_cmd, timeout=21600)   # up to 6h: the historical backlog is ~948 permits
    if rc != 0:
        print(f"[tama38] download exited {rc} (non-fatal) — parsing existing PDFs.")
        _problem(f"tama38 download {_rc_text(rc)} — parsed only the PDFs already on disk")

    # 1b. Content-scan the generic "צרופה" attachments of permits still marked
    # 'no_tree_docs' — the name-based download above only matches שפ"ע/גינון
    # rows, but recent (online-submission) permits bury the survey inside a
    # generically-named attachment. Only accepts a doc when extraction yields
    # data (skips committee-decision mentions / architectural sheets). Its
    # metadata is folded into the count + valency builds below. Non-fatal.
    rc = _run([sys.executable, HARMONICA_SCRIPT, "--recent", "--no-ocr"], timeout=21600)
    if rc != 0:
        print(f"[tama38] harmonica content-scan exited {rc} (non-fatal).")
        _problem(f"tama38 harmonica content-scan {_rc_text(rc)}")

    # 2. Parse counts from the PDFs.
    rc = _run([sys.executable, TAMA38_PARSE], timeout=1800)
    if rc != 0:
        print(f"[tama38] parse exited {rc} — aborting tama38 stage.")
        FAILED["tama38"] = f"parse {_rc_text(rc)}"
        return None

    # 3. Rebuild the count file (keyed by tik) and 4. the valency file.
    rc1 = _run([sys.executable, TAMA38_COUNT_BUILD], timeout=300)
    rc2 = _run([sys.executable, TAMA38_VAL_SCRIPT, "--delete-after"], timeout=1800)
    if rc1 != 0 or rc2 != 0:
        print(f"[tama38] build exited counts={rc1} valency={rc2} — aborting tama38 stage.")
        FAILED["tama38"] = f"build counts={_rc_text(rc1)} valency={_rc_text(rc2)}"
        return None

    new_counts  = load_valency(TAMA38_SURVEYS)
    new_valency = load_valency(TAMA38_VALENCIES)
    ca, cc, cr = _commit_or_discard(TAMA38_SURVEYS_PUSH, old_counts, new_counts, "counts")
    va, vc, vr = _commit_or_discard(TAMA38_VAL_PUSH, old_valency, new_valency, "valency")
    print(f"[tama38] counts: +{ca}/~{cc}/-{cr} ({len(new_counts)} tiks) | "
          f"valency: +{va}/~{vc}/-{vr} ({len(new_valency)} tiks)")
    return {"counts_total": len(new_counts), "counts_added": ca,
            "valency_total": len(new_valency), "valency_added": va,
            "added": va, "changed": vc, "removed": vr, "total": len(new_valency)}


# ─── Email + log ─────────────────────────────────────────────────────────────

def send_email(counts, valency, name_map, tama38=None):
    """counts = (added, removed, changed) of (taba,o,n) or None.
    valency = (added, removed, changed, new_data) or None.
    tama38  = summary dict from run_tama38_stage() or None."""
    if not EMAIL_PASSWORD:
        print("[email] GMAIL_APP_PASSWORD not set — skipping email.")
        return False

    date_str = datetime.now().strftime("%Y-%m-%d")
    name = lambda t: esc(name_map.get(t, ""))

    subj_bits, sections = [], []

    # Counts section
    if counts:
        c_add, c_rem, c_chg = counts
        net = sum(n - o for _, o, n in c_add + c_rem + c_chg)
        subj_bits.append(f"כמות: +{len(c_add)}/~{len(c_chg)}/-{len(c_rem)} ({net:+d})")

        def ctable(title, rows):
            if not rows:
                return ""
            body = "".join(
                f"<tr><td>{esc(t)}</td><td>{name(t)}</td>"
                f"<td align='center'>{o}</td><td align='center'>{n}</td>"
                f"<td align='center'>{n - o:+d}</td></tr>" for t, o, n in rows)
            return (f"<h3>{title} ({len(rows)})</h3>"
                    "<table border='1' cellpadding='4' cellspacing='0' style='border-collapse:collapse'>"
                    "<tr><th>תב\"ע</th><th>שם תכנית</th><th>קודם</th><th>עכשיו</th><th>שינוי</th></tr>"
                    f"{body}</table>")

        sections.append(
            '<h2>שלב א׳ — כמות עצים (XPLAN)</h2>'
            f'<p style="color:#555">סה"כ נטו: <b>{net:+d}</b> עצים.</p>'
            + ctable('תכניות עם ספירת עצים חדשה', c_add)
            + ctable('שינוי בספירה', c_chg)
            + ctable('ספירה שירדה/אופסה', c_rem))

    # Valency section
    if valency:
        v_add, v_rem, v_chg, v_data = valency
        subj_bits.append(f"ערכיות: +{len(v_add)}/~{len(v_chg)}/-{len(v_rem)}")

        def vtable(title, keys):
            if not keys:
                return ""
            body = "".join(
                f"<tr><td>{esc(t)}</td><td>{name(t)}</td>"
                f"<td align='center'>{v_data.get(t, {}).get('total_trees', '—')}</td>"
                f"<td align='center'>{esc(v_data.get(t, {}).get('status', ''))}</td></tr>"
                for t in keys)
            return (f"<h3>{title} ({len(keys)})</h3>"
                    "<table border='1' cellpadding='4' cellspacing='0' style='border-collapse:collapse'>"
                    "<tr><th>תב\"ע</th><th>שם תכנית</th><th>עצים בסקר</th><th>סטטוס חילוץ</th></tr>"
                    f"{body}</table>")

        sections.append(
            '<h2>שלב ב׳ — ערכיות עצים (מסקר PDF)</h2>'
            '<p style="color:#555">פירוק ערכיות × המלצה מתוך סקר האגרונום (לא קיים ב-XPLAN).</p>'
            + vtable('תכניות עם ערכיות חדשה', v_add)
            + vtable('ערכיות שהשתנתה', v_chg)
            + vtable('ערכיות שירדה', v_rem))

    # TAMA 38 section (summary only — valency popup wiring is a follow-up)
    if tama38:
        subj_bits.append(f'תמ"א38: כמות +{tama38["counts_added"]}/ערכיות +{tama38["valency_added"]}')
        sections.append(
            '<h2>תמ"א 38 — סקרי עצים מהארכיב האופטי</h2>'
            f'<p style="color:#555">כמות: {tama38["counts_total"]} היתרים '
            f'(+{tama38["counts_added"]} חדשים) | ערכיות: {tama38["valency_total"]} היתרים '
            f'(+{tama38["valency_added"]} חדשים, ~{tama38["changed"]} שונו, -{tama38["removed"]}).</p>')

    if not sections:
        print("[email] nothing changed — no email.")
        return False

    subject = "סקר עצים חודשי אורנים — " + " | ".join(subj_bits) + f" ({date_str})"
    html = (f'<html><body dir="rtl" style="font-family:Arial,sans-serif">'
            f'<h1>סקר עצים חודשי — {esc(date_str)}</h1>' + "".join(sections)
            + '<p style="color:#888;font-size:12px;margin-top:24px">Generated by monthly_tree_scan.py</p>'
            '</body></html>')

    msg = MIMEMultipart("alternative")
    msg["From"], msg["To"], msg["Subject"] = EMAIL_SENDER, EMAIL_RECIPIENT, subject
    msg.attach(MIMEText(html, "html", "utf-8"))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
        print(f"[email] Sent to {EMAIL_RECIPIENT}: {subject}")
        return True
    except Exception as e:
        print(f"[email] Send failed: {e}")
        return False


def append_log(counts, valency, tama38=None):
    parts = [datetime.now().isoformat(timespec="seconds")]
    if counts:
        a, r, c = counts
        net = sum(n - o for _, o, n in a + r + c)
        parts.append(f"counts: added={len(a)} changed={len(c)} removed={len(r)} net={net:+d}")
    else:
        parts.append(f"counts: FAILED ({FAILED['counts']})" if "counts" in FAILED else "counts: skipped")
    if valency:
        a, r, c, _ = valency
        parts.append(f"valency: added={len(a)} changed={len(c)} removed={len(r)}")
    else:
        parts.append(f"valency: FAILED ({FAILED['valency']})" if "valency" in FAILED else "valency: skipped")
    if tama38:
        parts.append(f"tama38: added={tama38['added']} changed={tama38['changed']} "
                     f"removed={tama38['removed']} total={tama38['total']}")
    elif "tama38" in FAILED:
        parts.append(f"tama38: FAILED ({FAILED['tama38']})")
    if PROBLEMS:
        parts.append(f"PROBLEMS: {len(PROBLEMS)}")
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(" | ".join(parts) + "\n")


def report_problems():
    """Alert on failed stages / broken non-fatal steps; return the exit code.
    The change email above is sent only when data changed, so it cannot carry
    this — a failed run changes nothing."""
    if not FAILED and not PROBLEMS:
        return 0
    lines = [f"שלב {k} נכשל: {v}" for k, v in FAILED.items()] + PROBLEMS
    print("[problems]\n  " + "\n  ".join(lines))
    try:
        from ops_alert import send_alert
        send_alert(f"סקר עצים חודשי: {len(FAILED)} שלבים נכשלו, {len(PROBLEMS)} תקלות — "
                   f"{datetime.now():%Y-%m-%d}",
                   "\n".join(lines) + "\n\nהלוג המלא: permits_scan_reports\\tree_scan_last_run.log")
    except Exception as e:
        print(f"[problems] alert failed: {e}")
    return 1


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Monthly tree-survey refresh (counts + valency)")
    ap.add_argument("--no-email", action="store_true", help="Skip email even if changes")
    ap.add_argument("--no-valency", action="store_true", help="Skip the PDF valency stage")
    ap.add_argument("--valency-only", action="store_true", help="Skip the XPLAN count stage")
    ap.add_argument("--tama38-only", action="store_true",
                    help="Run ONLY the TAMA 38 stage (skip counts + תב\"ע valency)")
    ap.add_argument("--tama38-recheck", action="store_true",
                    help="With --tama38-only: also re-check no_tree_docs permits "
                         "(default catch-up = only the never-processed backlog)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Counts via --dry-run; skips valency (it writes files)")
    args = ap.parse_args()

    print(f"=== Monthly tree scan {datetime.now().isoformat(timespec='seconds')} ===")

    # Pull so we diff/commit against latest origin.
    if not pull_before_read(REPO_DIR):
        _problem("git pull failed — diffed/committed against local state")

    name_map = load_name_map()

    # TAMA 38 catch-up mode — run only that stage (dedicated backlog run).
    if args.tama38_only:
        tama38 = run_tama38_stage(recheck_empty=args.tama38_recheck)
        if not args.no_email:
            send_email(None, None, name_map, tama38)
        append_log(None, None, tama38)
        print("=== Done (tama38-only) ===")
        return report_problems()

    # Stage A — counts (XPLAN).
    counts = None
    if not args.valency_only:
        counts = run_count_stage(args.dry_run)

    # Stage B — valency (PDF). Skipped on dry-run (it downloads + writes files).
    valency = None
    tama38 = None
    if not args.no_valency and not args.dry_run:
        valency = run_valency_stage()
        # Stage B2 — TAMA 38: re-check permit files for new surveys, rebuild
        # counts + valency. HEADED browser (ykpubdata), like the תב"ע stage.
        tama38 = run_tama38_stage()
    elif args.dry_run:
        print("[valency] --dry-run: skipping valency stage.")

    # Email + log.
    if not args.no_email and not args.dry_run:
        send_email(counts, valency, name_map, tama38)
    append_log(counts, valency, tama38)

    print("=== Done ===")
    return report_problems()


if __name__ == "__main__":
    sys.exit(main())
