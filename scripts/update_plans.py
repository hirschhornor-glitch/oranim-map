import os
import json
import time
import base64
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

_HEADER_START = ["agam_id", "ver_id", "taba", "status_mavat", "mavat_url", "plan_name"]

GITHUB_TOKEN   = os.environ["GITHUB_TOKEN"]
GOOGLE_CREDS   = os.environ["GOOGLE_CREDS"]
GITHUB_REPO    = "hirschhornor-glitch/oranim-map"
SHEET_NAME     = "Oranim_Taba"
KEY_FIELD      = "plan_name"
TS_FIELD       = "last_modified"
TIMESTAMP_FILE = "data/last_update.txt"
SUMMARY_FILE   = "data/last_run_summary.txt"
CHANGELOG_FILE = "data/plan_changelog.jsonl"
# plan changes are infrequent (few plans/run, most runs zero), so growth is
# slow; cap defensively so the file can't grow unbounded over years.
MAX_CHANGELOG_LINES = 50000

# Stage-3 verification gate: a NEW built public allocation (hafrash) must not go
# live until a human corroborates it against Table 5 / הוראות (see the
# 101-1354356 incident, where a binui-misread kindergarten slipped through).
# When detected, the field is HELD (not written to the live geojson) and queued
# in pending_review.json; scripts/review_queue.py approves (publish) or rejects.
PENDING_REVIEW_FILE = "data/pending_review.json"
GATED_FIELDS = ("hafrash_prg", "hafrash_sqm")
PUBLIC_BUILDING_TERMS = ('גן ילדים', 'כיתות גן', 'מעון', 'בי"ס', 'בית ספר',
                         'בית-ספר', 'מבנה ציבור', 'מבני ציבור', 'ציבור', 'שב"צ',
                         'קהילה', 'מתנ"ס', 'בית כנסת', 'מרפאה', 'רווחה')


def _is_new_public_allocation(field, old, new):
    """True for a hafrash that appeared where there was none, or a hafrash_prg
    that gained a public-building term it lacked before. Mirrors the same
    predicate in update_mavat_ui.py (Stage 2 email flag) so what gets flagged is
    exactly what gets gated."""
    if field not in GATED_FIELDS:
        return False
    o = "" if old is None else str(old).strip()
    n = "" if new is None else str(new).strip()
    if not n:
        return False
    if field == "hafrash_sqm":
        return not o
    if not o:
        return any(t in n for t in PUBLIC_BUILDING_TERMS)
    return any(t in n and t not in o for t in PUBLIC_BUILDING_TERMS)


HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

def get_sheet():
    creds_dict = json.loads(GOOGLE_CREDS)
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME).sheet1

def get_github_file(path):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    r = requests.get(url, headers=HEADERS)
    if r.status_code == 200:
        data = r.json()
        if "content" in data and data["content"]:
            content = base64.b64decode(data["content"].replace("\n", "")).decode("utf-8")
        elif "download_url" in data:
            r2 = requests.get(data["download_url"])
            content = r2.text
        else:
            return None, None
        return data["sha"], content
    return None, None

def upload_github_file(path, content, sha, message):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    payload = {
        "message": message,
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
    }
    if sha:
        payload["sha"] = sha
    r = requests.put(url, headers=HEADERS, json=payload)
    return r.status_code in (200, 201)

def get_israel_time():
    try:
        import zoneinfo
        tz_il = zoneinfo.ZoneInfo("Asia/Jerusalem")
    except ImportError:
        import pytz
        tz_il = pytz.timezone("Asia/Jerusalem")
    return datetime.now(tz_il).replace(tzinfo=None)

def load_last_update():
    _, content = get_github_file(TIMESTAMP_FILE)
    if content:
        try:
            return datetime.strptime(content.strip(), "%Y-%m-%d %H:%M:%S")
        except:
            pass
    return datetime.min

def save_last_update():
    sha, _ = get_github_file(TIMESTAMP_FILE)
    now_str = get_israel_time().strftime("%Y-%m-%d %H:%M:%S")
    upload_github_file(TIMESTAMP_FILE, now_str, sha,
                       f"update timestamp {now_str}")

def write_summary(updated, changed_rows, last_update):
    now_str = get_israel_time().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        f"הרצה: {now_str}",
        f"עדכון קודם: {last_update.strftime('%Y-%m-%d %H:%M:%S')}",
    ]

    if updated > 0:
        lines.append(f"✅ עודכנו {updated} תכניות:")
        for plan_name, row in changed_rows.items():
            plan_name_he = row.get("plan_name_he", "")
            lines.append(f"  - {plan_name} {plan_name_he}")
    else:
        lines.append("ℹ️ אין שינויים מאז העדכון האחרון")

    summary = "\n".join(lines)
    print(summary)

    # כתוב לקובץ ב-repo כדי שניתן יהיה לשלוף דרך API
    sha, _ = get_github_file(SUMMARY_FILE)
    upload_github_file(SUMMARY_FILE, summary, sha, f"summary {now_str}")

    # כתוב גם ל-GitHub Step Summary אם קיים
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "w", encoding="utf-8") as f:
            f.write("## " + "\n\n".join(lines))

def read_sheet_rows(attempts=4):
    """Read all Sheet records, retrying transient Google Sheets API errors
    (429 quota / 5xx / network blips). This cron runs every 10 min, so a brief
    quota spike — e.g. another script reading the same sheet — would otherwise
    fail the job and email a false alarm. The 'header missing' check is a REAL
    condition (not transient), so it aborts immediately without retrying."""
    last_err = None
    for i in range(attempts):
        try:
            sheet = get_sheet()
            # The Oranim_Taba sheet1 header row periodically vanishes (a sort over
            # an unfrozen header — now frozen, see reference_gs_oranim_taba_columns).
            # Without it get_all_records() dies with a cryptic "duplicate header ''".
            row1 = sheet.row_values(1)
            if row1[:6] != _HEADER_START:
                raise SystemExit(
                    "ERROR: Oranim_Taba sheet1 is missing its header row (row 1 is "
                    f"data, not headers — got {row1[:6]}). Restore the 53-column "
                    "header (it should be frozen). Aborting to avoid corrupting "
                    "plans.geojson."
                )
            return sheet.get_all_records()
        except SystemExit:
            raise  # header genuinely missing — do not retry
        except Exception as e:
            last_err = e
            if i < attempts - 1:
                wait = 5 * (2 ** i)  # 5, 10, 20s
                print(f"  sheet read failed ({type(e).__name__}: {str(e)[:80]}); "
                      f"retry {i + 1}/{attempts} in {wait}s")
                time.sleep(wait)
    raise SystemExit(f"ERROR: sheet read failed after {attempts} attempts: {last_err}")


def _norm(v):
    """Canonical comparison form so type/formatting noise (130 vs '130',
    '' vs None, 34.0 vs 34) is not logged as a field change."""
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v).strip()


def append_changelog(events):
    """Append per-field change events to data/plan_changelog.jsonl (one JSON
    object per line). Non-fatal: a changelog failure must never break the
    geojson mirror, so all errors are swallowed with a warning."""
    if not events:
        return
    try:
        sha, content = get_github_file(CHANGELOG_FILE)
        lines = content.splitlines() if content else []
        for ev in events:
            lines.append(json.dumps(ev, ensure_ascii=False))
        if len(lines) > MAX_CHANGELOG_LINES:
            lines = lines[-MAX_CHANGELOG_LINES:]
        new_content = "\n".join(lines) + "\n"
        ok = upload_github_file(
            CHANGELOG_FILE, new_content, sha,
            f"changelog +{len(events)} field changes "
            f"{get_israel_time().strftime('%Y-%m-%d %H:%M')}")
        print(f"{'✓' if ok else '✗'} changelog: +{len(events)} field changes")
    except Exception as e:
        print(f"  ⚠ changelog write failed (non-fatal): {e}")


def save_pending_review(pending):
    """Persist the verification-gate queue. Non-fatal on error."""
    try:
        sha, _ = get_github_file(PENDING_REVIEW_FILE)
        content = json.dumps(pending, ensure_ascii=False, indent=1)
        ok = upload_github_file(
            PENDING_REVIEW_FILE, content, sha,
            f"review queue: {len(pending)} pending public-allocation item(s) "
            f"{get_israel_time().strftime('%Y-%m-%d %H:%M')}")
        print(f"{'✓' if ok else '✗'} review queue: {len(pending)} pending item(s)")
    except Exception as e:
        print(f"  ⚠ review queue save failed (non-fatal): {e}")


def update_plans():
    last_update = load_last_update()
    print(f"עדכון אחרון: {last_update}")

    all_rows = read_sheet_rows()

    changed_rows = {}
    for row in all_rows:
        ts_str = row.get(TS_FIELD, "")
        if not ts_str:
            continue
        try:
            ts = datetime.strptime(str(ts_str), "%Y-%m-%d %H:%M:%S")
        except:
            continue
        if ts > last_update:
            changed_rows[str(row[KEY_FIELD])] = row

    if not changed_rows:
        print("אין שינויים מאז העדכון האחרון")
        write_summary(0, {}, last_update)
        return

    print(f"נמצאו {len(changed_rows)} שורות שהשתנו")

    sha, existing_geojson = get_github_file("data/plans.geojson")
    if not existing_geojson:
        print("לא נמצא plans.geojson ב-GitHub")
        return

    geojson_data = json.loads(existing_geojson)

    # Verification gate — load the review queue. Fail-open: if it can't be read,
    # set to None so the gate is disabled this run (never withhold legit data or
    # break the mirror because the queue was momentarily unavailable).
    try:
        _, pr_content = get_github_file(PENDING_REVIEW_FILE)
        pending_review = json.loads(pr_content) if pr_content else {}
    except Exception as e:
        print(f"  ⚠ review queue unavailable — gate disabled this run: {e}")
        pending_review = None
    pending_changed = False

    now_str = get_israel_time().strftime("%Y-%m-%d %H:%M:%S")
    change_events = []
    updated = 0
    for feature in geojson_data["features"]:
        props = feature["properties"]
        plan_name = str(props.get(KEY_FIELD, ""))
        if plan_name in changed_rows:
            row = changed_rows[plan_name]
            new_status = row.get("status_mavat", props.get("status_mavat", ""))
            taba = row.get("taba", props.get("taba", ""))
            for k, v in row.items():
                # ── verification gate for built public allocations ──
                if pending_review is not None and k in GATED_FIELDS:
                    gkey = f"{taba}|{k}"
                    if gkey in pending_review:
                        continue  # already held & queued — keep old value, don't republish
                    if _is_new_public_allocation(k, props.get(k), v):
                        pending_review[gkey] = {
                            "taba": taba,
                            "plan_name": plan_name,
                            "field": k,
                            "old": props.get(k),
                            "held_value": v,
                            "status": new_status,
                            "first_seen": now_str,
                        }
                        pending_changed = True
                        change_events.append({
                            "ts": now_str, "taba": taba, "plan_name": plan_name,
                            "status": new_status, "field": k,
                            "old": props.get(k), "new": v, "held": True,
                        })
                        continue  # HOLD: do not write to the live geojson
                # capture old->new BEFORE overwriting; skip the trigger field
                if k != TS_FIELD and _norm(props.get(k)) != _norm(v):
                    change_events.append({
                        "ts": now_str,
                        "taba": taba,
                        "plan_name": plan_name,
                        "status": new_status,
                        "field": k,
                        "old": props.get(k),
                        "new": v,
                    })
                props[k] = v
            updated += 1

    print(f"מעדכן {updated} פיצ'רים... ({len(change_events)} שינויי שדה)")
    geojson_str = json.dumps(geojson_data, ensure_ascii=False)
    success = upload_github_file(
        "data/plans.geojson", geojson_str, sha,
        f"update plans {get_israel_time().strftime('%Y-%m-%d %H:%M')}"
    )

    if success:
        print("✓ plans.geojson עודכן")
        append_changelog(change_events)
        if pending_review is not None and pending_changed:
            n_held = sum(1 for e in change_events if e.get("held"))
            print(f"🚩 {n_held} built-allocation change(s) HELD for review")
            save_pending_review(pending_review)
        save_last_update()
        write_summary(updated, changed_rows, last_update)
    else:
        print("✗ שגיאה בעדכון")
        write_summary(0, {}, last_update)

update_plans()
