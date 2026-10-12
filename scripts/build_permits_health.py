"""
build_permits_health.py
-----------------------
One verdict for "is the whole permits topic up-to-date and correct?".

The permits topic is refreshed by 5+ independent mechanisms (biweekly scan,
§149 weekly, dev-backfill daily, committees weekly, tree-permits CI daily) whose
logs are scattered in permits_scan_reports\\. This script aggregates a single
freshness + correctness signal per source into data/permits_health.json.

NOTE (audit 2026-10-06): nothing renders permits_health.json yet — no app
report and no email reads it (an earlier version of this docstring claimed the
app showed it as a traffic light). Its only active surface is the ops alert
sent below when an item newly turns red. It stays advisory: always exit 0.

Freshness = last time the file reached the LIVE app (git last-commit date of the
data file), cross-checked against the scan log so a "scanned but not published"
gap is visible. It says nothing about whether the scraper behind it succeeded
(see run_status in the meetings JSONs for that). Caveat: this runs BEFORE its
pipeline's push, so a file being published in this very run still shows the
previous commit date — pass `--publishing <file> ...` (data file names) for the
files this run pushes; a listed file that differs from HEAD counts as published
today. Correctness = the committee-vs-permits audit (real permits on the
licensing agenda that aren't in our data), which also goes yellow/red by age.

Run standalone, or at the end of a permits pipeline (before its push) so the
health snapshot reflects the just-published data.
"""
import io
import json
import os
import re
import subprocess
from datetime import date, datetime

ROOT = r"C:\ORANIM"
ORANIM_APP = os.path.join(ROOT, "oranim-app")
DATA_DIR = os.path.join(ORANIM_APP, "data")
REPORTS = os.path.join(ROOT, "permits_scan_reports")
OUT = os.path.join(DATA_DIR, "permits_health.json")

# Each source: data file + human label + refresh cadence + expected max age (days)
# before it's considered stale (None = on-demand, no staleness alarm).
SOURCES = [
    {"file": "all_permits.json",         "label": "היתרים לפי תב\"ע",            "refresh": "דו-שבועי (סריקה)",        "max_days": 18},
    {"file": "tama38_permits.json",      "label": "היתרי תמ\"א 38",             "refresh": "דו-שבועי + §149 שבועי",  "max_days": 18},
    {"file": "extra_permits.json",       "label": "היתרים משלימים (YK API)",    "refresh": "לפי צורך",               "max_days": None},
    {"file": "meetings.json",            "label": "ישיבות ועדה קרובות",         "refresh": "שבועי (ועדות)",          "max_days": 9},
    {"file": "objections_permits.json",  "label": "היתרים פתוחים להתנגדות §149", "refresh": "שבועי",                  "max_days": 9},
    {"file": "unassigned_permits.geojson","label": "היתרים לא-משויכים (מגוקודד)", "refresh": "לפי צורך",              "max_days": None},
    {"file": "tree_permits.json",        "label": "אישורי כריתת עצים",          "refresh": "יומי (CI)",              "max_days": 3},
    {"file": "field_observations.json",  "label": "תצפיות שטח",                 "refresh": "לפי צורך",               "max_days": None},
    {"file": "occupancy_status.json",    "label": "מאוכלס (טופס 4)",            "refresh": "לפי צורך",               "max_days": None},
]


# Committee audit runs weekly (run_all_committees.bat) and now writes a report
# even when there is nothing to check: >14d = yellow, >28d = red.
AUDIT_MAX_DAYS = 14


def load_json(path):
    try:
        with io.open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def git_last_commit_date(rel_path):
    """YYYY-MM-DD of the last commit that touched a file in oranim-app, or None."""
    try:
        r = subprocess.run(["git", "log", "-1", "--format=%cs", "--", rel_path],
                           cwd=ORANIM_APP, capture_output=True, text=True,
                           encoding="utf-8", errors="replace")
        s = (r.stdout or "").strip()
        return s or None
    except Exception:
        return None


def pending_publish(rel_path):
    """True if the working-tree file differs from HEAD (or is untracked) —
    i.e. the push that follows this script will publish it today."""
    try:
        tracked = subprocess.run(["git", "ls-files", "--error-unmatch", "--", rel_path],
                                 cwd=ORANIM_APP, capture_output=True).returncode == 0
        if not tracked:
            return os.path.exists(os.path.join(ORANIM_APP, rel_path))
        return subprocess.run(["git", "diff", "--quiet", "HEAD", "--", rel_path],
                              cwd=ORANIM_APP, capture_output=True).returncode == 1
    except Exception:
        return False


def count_records(fname, data):
    """Best-effort record count across the several permit file shapes."""
    if data is None:
        return None
    if fname.endswith(".geojson"):
        return len(data.get("features", [])) if isinstance(data, dict) else None
    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict):
        vals = list(data.values())
        # dict keyed by taba/fid with {permits:[...]}
        if vals and all(isinstance(v, dict) and "permits" in v for v in vals[:8]):
            return sum(len(v.get("permits") or []) for v in data.values())
        for wrap in ("by_taba", "by_file", "by_plan"):
            if isinstance(data.get(wrap), dict):
                return sum((len(v) if isinstance(v, list) else 1) for v in data[wrap].values())
        return len(data)
    return None


def days_since(dstr):
    try:
        return (date.today() - datetime.strptime(dstr, "%Y-%m-%d").date()).days
    except Exception:
        return None


def status_for(days, max_days):
    if max_days is None:
        return "info"          # on-demand source — no staleness alarm
    if days is None:
        return "unknown"
    if days > 2 * max_days:
        return "red"
    if days > max_days:
        return "yellow"
    return "green"


def latest_scan():
    """Parse the newest line of scan_log.txt → date + all/tama38 deltas."""
    path = os.path.join(REPORTS, "scan_log.txt")
    try:
        lines = [l for l in io.open(path, encoding="utf-8").read().splitlines() if l.strip()]
    except Exception:
        return None
    if not lines:
        return None
    last = lines[-1]
    m_date = re.search(r"date=(\d{4}-\d{2}-\d{2})", last)
    out = {"date": m_date.group(1) if m_date else None, "removed_flag": False, "raw": last}
    for grp in ("all", "tama38"):
        mm = re.search(grp + r"=new:(\d+),status:(\d+),removed:(\d+)", last)
        if mm:
            out[grp] = {"new": int(mm.group(1)), "status": int(mm.group(2)), "removed": int(mm.group(3))}
            if int(mm.group(3)) > 0:
                out["removed_flag"] = True
    if out["date"]:
        out["age_days"] = days_since(out["date"])
    return out


def latest_committee_audit():
    """Newest committee_audit_*.json → matched/missing counts + the missing list."""
    try:
        files = sorted(f for f in os.listdir(REPORTS)
                       if f.startswith("committee_audit_") and f.endswith(".json"))
    except Exception:
        files = []
    if not files:
        return None
    d = load_json(os.path.join(REPORTS, files[-1]))
    if not isinstance(d, dict):
        return None
    missing = d.get("missing") or []
    a_date = files[-1].replace("committee_audit_", "").replace(".json", "")
    # Age check (audit 2026-10-06): the newest audit was 2026-09-14 for three
    # weeks and the snapshot still reported it as the current verdict.
    age = days_since(a_date)
    return {
        "date": a_date,
        "age_days": age,
        "status": status_for(age, AUDIT_MAX_DAYS),
        "checked": d.get("checked"),
        "matched": len(d.get("matched") or []),
        "missing": len(missing),
        "missing_list": [
            {"permit_no": m.get("permit_no_raw") or m.get("permit_no"),
             "neighborhood": m.get("neighborhood"),
             "address": m.get("address"),
             "essence": (m.get("essence") or "")[:80]}
            for m in missing
        ],
    }


FAILURE_LOG_MAX_DAYS = 8
_FAILURE_RE = re.compile(r"throttl|hard-?block|access denied|aborting|נחסם|WAF", re.IGNORECASE)
# Not failures: "finished without throttle" / "no throttle" (objections scan),
# and Chrome command-line flags like --disable-background-timer-throttling that
# Playwright echoes on launch errors (audit 2026-10-06 false positives).
_FAILURE_NEGATED = re.compile(r"\b(without|no|not)\s+throttl\w*|--[\w-]+", re.IGNORECASE)


def is_failure_line(line):
    return bool(_FAILURE_RE.search(_FAILURE_NEGATED.sub(" ", line)))


def known_failures(max_days=FAILURE_LOG_MAX_DAYS):
    """Scan recent *.log for throttle/WAF/abort signals → short notes. Only logs
    modified in the last max_days count — every *.log used to be read regardless
    of age, so months-old one-off runs kept reporting "current" failures."""
    notes = []
    cutoff = datetime.now().timestamp() - max_days * 86400
    try:
        logs = [f for f in os.listdir(REPORTS) if f.endswith(".log")
                and os.path.getmtime(os.path.join(REPORTS, f)) >= cutoff]
    except Exception:
        logs = []
    for lg in logs:
        try:
            txt = io.open(os.path.join(REPORTS, lg), encoding="utf-8", errors="replace").read()
        except Exception:
            continue
        hits = [ln.strip() for ln in txt.splitlines() if is_failure_line(ln)]
        if hits:
            notes.append({"log": lg, "sample": hits[-1][:160], "count": len(hits)})
    return notes


def red_keys(snapshot):
    """Keys of the red items in a health snapshot (sources by file + audit)."""
    if not isinstance(snapshot, dict):
        return set()
    keys = {s.get("file") for s in snapshot.get("sources") or [] if s.get("status") == "red"}
    ca = snapshot.get("committee_audit")
    if isinstance(ca, dict) and ca.get("status") == "red":
        keys.add("committee_audit")
    return keys


def alert_new_reds(prev, out):
    """Ops alert only for items that turned red since the previous snapshot —
    the snapshot was surfaced nowhere, so reds went unseen (audit 2026-10-06);
    alerting on every red would repeat weekly, so only transitions are sent."""
    new = sorted(red_keys(out) - red_keys(prev))
    if not new:
        return []
    labels = {s["file"]: s for s in out["sources"]}
    lines = []
    for k in new:
        if k == "committee_audit":
            ca = out["committee_audit"]
            lines.append(f"בדיקת ועדת רישוי מול המאגר: הדוח האחרון מ-{ca.get('date')} "
                         f"({ca.get('age_days')} ימים)")
        else:
            s = labels[k]
            extra = ("; " + "; ".join(s["notes"])) if s["notes"] else ""
            lines.append(f"{s['label']} ({k}): פורסם לאחרונה {s.get('last_publish')} "
                         f"({s.get('age_days')} ימים){extra}")
    try:
        from ops_alert import send_alert
        send_alert("בריאות היתרים: פריטים חדשים באדום",
                   "פריטים שעברו לאדום מאז הריצה הקודמת:\n" + "\n".join(lines)
                   + f"\n\n{OUT}")
    except Exception as e:
        print(f"  alert failed: {e}")
    return new


def main():
    import argparse
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--publishing", nargs="*", default=[],
                    help="data file names this run is about to push (e.g. meetings.json)")
    args = ap.parse_args()
    publishing = {os.path.basename(f) for f in args.publishing}

    prev = load_json(OUT)  # read before overwriting — for the new-red alert
    today = date.today().isoformat()
    scan = latest_scan()
    sources = []
    for s in SOURCES:
        path = os.path.join(DATA_DIR, s["file"])
        data = load_json(path)
        published = git_last_commit_date("data/" + s["file"])
        pending = s["file"] in publishing and pending_publish("data/" + s["file"])
        if pending:
            published = today  # pushed right after this script, in this run
        age = days_since(published) if published else None
        row = {
            "file": s["file"], "label": s["label"], "refresh": s["refresh"],
            "records": count_records(s["file"], data),
            "last_publish": published, "age_days": age,
            "status": status_for(age, s["max_days"]),
            "notes": ["ממתין לפרסום בריצה זו"] if pending else [],
        }
        # scan↔publish gap: biweekly sources scanned more recently than last publish
        if s["file"] in ("all_permits.json", "tama38_permits.json") and scan and scan.get("date") and published:
            if scan["date"] > published:
                row["status"] = "red"
                row["notes"].append(f"נסרק {scan['date']} אך פורסם לאחרונה {published} — פער סריקה↔פרסום")
        sources.append(row)

    out = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "sources": sources,
        "scan": scan,
        "committee_audit": latest_committee_audit(),
        "known_failures": known_failures(),
    }
    with io.open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    reds = [s["label"] for s in sources if s["status"] == "red"]
    print(f"Wrote {OUT}")
    print(f"  sources: {len(sources)} | red: {len(reds)} {reds}")
    if out["committee_audit"]:
        ca = out["committee_audit"]
        print(f"  committee audit {ca['date']} ({ca['age_days']}d, {ca['status']}): "
              f"{ca['matched']} matched, {ca['missing']} MISSING")
    print(f"  known failures: {len(out['known_failures'])}")
    new_reds = alert_new_reds(prev, out)
    if new_reds:
        print(f"  newly red since last snapshot: {new_reds} (alert sent)")


if __name__ == "__main__":
    main()
