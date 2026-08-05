"""
enrich_changed_plans.py
-----------------------
Deep-enrichment orchestrator for new / status-changed plans.

Drains pending_enrichment.json (fed by detect_new_plans.py and
update_mavat_status.py) and, per plan, runs best-effort:

  1. ensure horaot/101-<taba7>.pdf      (download via mavat_download_tashrits if missing)
  2. extract_execution_staging.py <taba>  -> oranim-app/data/execution_staging.json
  3. classify hafrash_prg -> hafrash_type -> oranim-app/data/hafrash_types.json
  4. (built allocation, unless --no-floors) download נספח בינוי + render PNGs,
     then enqueue floor_read_queue.json for the Claude-vision floor read.

A failed step records a skip reason and never aborts the run. Plans that fully
succeed (or have no docs at all) are removed from the queue; plans that failed a
*download* stay queued so the next run retries (Mavat WAF/captcha is transient).

The floor read itself is the one step that needs Claude vision — a Claude session
drains floor_read_queue.json (see process_floor_queue.md) and merges results via
add_floor_alloc.py. Everything here is plain Python.

NOTE: horaot/nispach downloads use Playwright + a logged-in Mavat session
(.browser_data*). This must run on the local machine, not the GitHub cloud cron.

Usage:
  py enrich_changed_plans.py                  # drain the queue
  py enrich_changed_plans.py --single 1242700
  py enrich_changed_plans.py --from-report    # seed queue from new_plans_report.json first
  py enrich_changed_plans.py --no-floors      # staging + type only
  py enrich_changed_plans.py --no-download    # use cached horaot/nispach only (no browser)
  py enrich_changed_plans.py --no-email --limit 5
"""
import argparse
import io
import json
import os
import smtplib
import subprocess
import sys
import tempfile
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import enrich_queue as eq
import hafrash_classify as hc

ROOT = Path(r"C:\ORANIM")
DATA = ROOT / "oranim-app" / "data"
PLANS_GEOJSON = DATA / "plans.geojson"
HORAOT_DIR = ROOT / "horaot"
PLAN_DOCS = ROOT / "plan_documents"
BINUI_PNG = ROOT / "_binui_png"
STAGING_JSON = DATA / "execution_staging.json"
HAFRASH_TYPES_JSON = DATA / "hafrash_types.json"
NEW_PLANS_REPORT = ROOT / "new_plans_report.json"
SUMMARY_TXT = ROOT / "last_enrichment_summary.txt"
ENRICH_EMAIL_PAYLOAD = ROOT / "_pending_enrich_email.json"  # handed to update_mavat_ui for one-email policy
SEEN_PATH = ROOT / ".enriched_seen"   # tabas we've already seen (for --derive-new)

EMAIL_FROM = "hirschhorn.or@gmail.com"
EMAIL_TO = os.environ.get("ENRICH_EMAIL_TO", "Or_hi@jerusalem.muni.il")


def taba7(taba):
    return str(taba).zfill(7)


def pl_number(taba):
    return "101-" + taba7(taba)


def run_py(args, timeout, label):
    """Run `py <args>` in C:\\ORANIM, stream-capture, return (ok, tail)."""
    try:
        r = subprocess.run(
            [sys.executable, *map(str, args)],
            cwd=str(ROOT), timeout=timeout,
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        tail = (r.stdout or "")[-400:] + (("\nSTDERR:" + r.stderr[-300:]) if r.stderr else "")
        if r.returncode != 0:
            print(f"  [{label}] exit {r.returncode}: {tail.strip()[:300]}")
            return False, tail
        return True, tail
    except subprocess.TimeoutExpired:
        print(f"  [{label}] TIMEOUT after {timeout}s")
        return False, "timeout"
    except Exception as e:
        print(f"  [{label}] error: {e}")
        return False, str(e)


# ── plan metadata from plans.geojson ─────────────────────────────────────
def load_plan_props():
    """plans.geojson has one feature per parcel, and fields like hafrash_prg are
    only filled on some of a plan's features — so merge across all features of a
    taba, keeping the first non-empty value for each field."""
    pj = json.load(io.open(PLANS_GEOJSON, encoding="utf-8"))
    out = {}
    for feat in pj["features"]:
        p = feat.get("properties", {})
        tb = eq.norm_taba(p.get("taba"))
        if not tb:
            continue
        rec = out.setdefault(tb, {"taba": tb, "agam_id": None,
                                  "plan_name": "", "hafrash_prg": "",
                                  "status_mavat": "", "developer": ""})
        if rec["agam_id"] is None:
            ag = p.get("agam_id")
            try:
                rec["agam_id"] = int(float(ag)) if ag not in (None, "") else None
            except (TypeError, ValueError):
                pass
        for k in ("plan_name", "hafrash_prg", "status_mavat", "developer"):
            v = p.get(k)
            if v not in (None, "") and not rec.get(k):
                rec[k] = v
    for tb, rec in out.items():
        if not rec["plan_name"]:
            rec["plan_name"] = pl_number(tb)
    return out


# ── per-plan enrichment steps ────────────────────────────────────────────
def horaot_path(taba):
    return HORAOT_DIR / f"{pl_number(taba)}.pdf"


def has_horaot(taba):
    p = horaot_path(taba)
    if p.exists() and p.stat().st_size > 1024:
        return True
    ocr = HORAOT_DIR / f"{pl_number(taba)}.ocr.txt"
    return ocr.exists() and ocr.stat().st_size > 64


def ensure_horaot(taba, agam_id, allow_download):
    if has_horaot(taba):
        return "cached"
    if not allow_download:
        return "missing"
    if not agam_id:
        return "no_agam"
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as tf:
        json.dump([{"pl_number": pl_number(taba), "agam_id": agam_id}], tf,
                  ensure_ascii=False)
        tmp = tf.name
    try:
        run_py(["mavat_download_tashrits.py", "--plans-json", tmp], 360, "horaot")
    finally:
        os.unlink(tmp)
    return "downloaded" if has_horaot(taba) else "no_doc"


def dev_needs_fill(current):
    """True when the plan's developer field is empty or a known scrape artifact
    (mirrors apply_developer_extract.GARBAGE)."""
    v = (current or "").strip() if isinstance(current, str) else str(current or "")
    if not v:
        return True
    return (v in {"כתובת", "זיהוי התכנית", "סמכות:", "מס' יח\"ד",
                  "ניסיון", "אדריכלים ומתכנני ערים", "אדריכלים", "פרטי"}
            or v.startswith(".4") or len(v) >= 60 or v.isdigit())


def run_developer(taba):
    """Extract developer/architect from horaot section 1.8 into the shared
    results store. Returns 'ok'/'no data'/'fail'."""
    ok, tail = run_py(["extract_developers_from_horaot.py",
                       "--plan", pl_number(taba), "--save"], 180, "developer")
    if not ok:
        return "fail"
    return "ok" if '"developer": "' in tail and '"developer": ""' not in tail else "no data"


def run_staging(taba):
    """Run the extractor for this single taba; report whether it now has data."""
    ok, _ = run_py(["extract_execution_staging.py", str(taba)], 600, "staging")
    try:
        data = json.load(io.open(STAGING_JSON, encoding="utf-8"))
        return str(taba) in data
    except Exception:
        return False


def run_fund(plan_names):
    """Re-check קרן תחזוקה (maintenance fund) for the enriched plans and push
    data/maintenance_fund.json via git_sync. Runs on the freshly-ensured horaot,
    so a plan that just changed status has its fund amount / conditional-units /
    section re-extracted alongside the Table-5 check. Reuses the repo extractor's
    --push (concurrency-safe delta write). best-effort."""
    if not plan_names:
        return False
    fund_script = ROOT / "oranim-app" / "scripts" / "extract_maintenance_fund.py"
    ok, tail = run_py([str(fund_script), "--push", *plan_names], 600, "fund")
    if not ok:
        print(f"  [fund] {tail.strip()[-200:]}")
    return ok


def run_table5(plan_names, allow_download, no_email=False):
    """Refresh Table 5 -> GS for the plans enriched this run by invoking the
    guarded Mavat sync (update_mavat_ui.py) scoped to just these plans via
    --plans-file. Reuses that script's validated compute_changes + header/
    row-shift guard + backup + its own status email, rather than duplicating the
    historically bug-prone GS-write here. Needs a browser, so it is skipped under
    --no-download. Returns (ran, n_plans)."""
    names = sorted({n for n in plan_names if n})
    if not names or not allow_download:
        return False, 0
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False,
                                     encoding="utf-8") as tf:
        tf.write("\n".join(names))
        tmp = tf.name
    try:
        cmd = ["update_mavat_ui.py", f"--plans-file={tmp}"]
        if no_email:
            cmd.append("--no-email")
        ok, tail = run_py(cmd, 3600, "table5")
        print(f"[table5] refresh of {len(names)} plan(s): {'ok' if ok else 'FAILED'}")
        if not ok:
            print(tail[-500:])
        return ok, len(names)
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass


def _write_enrich_email_payload(rows):
    """Drop the per-plan enrichment summary for update_mavat_ui to fold into its
    single consolidated email (one-email policy)."""
    try:
        with io.open(ENRICH_EMAIL_PAYLOAD, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False)
    except OSError as e:
        print(f"[table5] could not write enrich email payload: {e}")


def _clear_enrich_email_payload():
    try:
        os.remove(ENRICH_EMAIL_PAYLOAD)
    except OSError:
        pass


def classify_type(taba, plan_name, hafrash_prg):
    types = hc.classify(hafrash_prg)
    if not types:
        return None  # no hafrash described -> nothing to record
    try:
        cur = json.load(io.open(HAFRASH_TYPES_JSON, encoding="utf-8"))
    except Exception:
        cur = {}
    cur[str(taba)] = {
        "plan_name": plan_name,
        "types": types,
        "primary": types[0],
        "source_text": hafrash_prg,
        "classified_at": datetime.now().strftime("%Y-%m-%d"),
    }
    eq._atomic_write(str(HAFRASH_TYPES_JSON), cur)
    return types


def binui_pngs(taba):
    d = BINUI_PNG / str(taba)
    return sorted(str(p) for p in d.glob("*.png")) if d.exists() else []


def prep_floors(taba, agam_id, allow_download):
    """Download + render the building appendix; enqueue PNGs for the vision read.
    Returns one of: queued / cached / no_nispach / no_agam / downloaded_no_png."""
    pngs = binui_pngs(taba)
    if pngs:
        eq.enqueue_floor([{"taba": taba, "plan_name": pl_number(taba),
                           "png_paths": pngs, "queued_at": _now()}])
        return "cached"
    if not allow_download:
        return "no_nispach"
    if not agam_id:
        return "no_agam"
    # download the נספח בינוי
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as tf:
        json.dump([{"taba": str(taba), "agam_id": agam_id}], tf, ensure_ascii=False)
        tmp = tf.name
    try:
        run_py(["download_nispach_binui.py", "--plans-json", tmp], 420, "nispach")
    finally:
        os.unlink(tmp)
    # render candidate drawing sheets to PNG (prep_binui_thumbs matches the
    # download's _*.pdf / appendix_*.pdf naming and uses fast pypdfium2).
    nispach_dir = PLAN_DOCS / f"101-{taba}"
    has_pdf = nispach_dir.exists() and (
        list(nispach_dir.glob("_*.pdf")) or list(nispach_dir.glob("appendix_*.pdf")))
    if has_pdf:
        run_py(["prep_binui_thumbs.py", str(taba)], 420, "render")
    pngs = binui_pngs(taba)
    if pngs:
        eq.enqueue_floor([{"taba": taba, "plan_name": pl_number(taba),
                           "png_paths": pngs, "queued_at": _now()}])
        return "queued"
    return "no_nispach"


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def derive_new(props):
    """Find plans present in plans.geojson but not seen before, independent of
    the queue/report — catches plans detected by the *cloud* detect_new_plans run
    (whose local queue we never received). First call seeds the marker silently
    (returns []) so we don't flood on a pre-existing dataset."""
    current = set(props.keys())
    if not SEEN_PATH.exists():
        io.open(SEEN_PATH, "w", encoding="utf-8").write("\n".join(sorted(current)))
        print(f"[derive-new] seeded marker with {len(current)} plans (no work first run)")
        return []
    # split on newlines only — some "tabas" are infra plan names with spaces
    # (e.g. 'תת"ל/ 108'), which .split() would tear apart.
    seen = set(l for l in io.open(SEEN_PATH, encoding="utf-8").read().splitlines() if l)
    new = sorted(current - seen)
    io.open(SEEN_PATH, "w", encoding="utf-8").write("\n".join(sorted(current)))
    print(f"[derive-new] {len(new)} new plan(s) since last run")
    return new


# ── email / summary ──────────────────────────────────────────────────────
def send_email(rows, dry):
    pw = os.environ.get("GMAIL_APP_PASSWORD")
    if dry or not pw or not rows:
        return False
    head = ("<tr style='background:#37474f;color:#fff'>"
            "<th>תכנית</th><th>סיבה</th><th>סטטוס</th><th>שלביות</th>"
            "<th>סוג הפרשה</th><th>קומות</th><th>יזם</th><th>הערה</th></tr>")
    trs = []
    for r in rows:
        trs.append(
            "<tr>"
            f"<td>{r['plan_name']}</td><td>{r['reason']}</td><td>{r['status']}</td>"
            f"<td>{r['staging']}</td><td>{r['hafrash_type']}</td>"
            f"<td>{r['floors']}</td><td>{r.get('developer', '—')}</td><td>{r['note']}</td></tr>")
    html = (
        "<div dir='rtl' style='font-family:Arial,sans-serif'>"
        f"<h3>העשרת תכניות חדשות / משנות-סטטוס — {datetime.now():%d/%m/%Y}</h3>"
        f"<p>{len(rows)} תכניות עובדו.</p>"
        "<table border='1' cellpadding='6' cellspacing='0' "
        "style='border-collapse:collapse;font-size:13px'>"
        f"{head}{''.join(trs)}</table>"
        "<p style='color:#666;font-size:12px'>תכניות עם נספח בינוי שנדרשת בהן "
        "קריאת קומות נכנסו ל-floor_read_queue.json (קריאת Claude-vision).</p></div>")
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Oranim: העשרת {len(rows)} תכניות ({datetime.now():%Y-%m-%d})"
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg.attach(MIMEText(html, "html", "utf-8"))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(EMAIL_FROM, pw)
            s.sendmail(EMAIL_FROM, [EMAIL_TO], msg.as_string())
        print(f"[email] sent to {EMAIL_TO}")
        return True
    except Exception as e:
        print(f"[email] failed: {e}")
        return False


def write_summary(rows):
    lines = [f"enrich_changed_plans  {_now()}", f"{len(rows)} plans processed", ""]
    for r in rows:
        lines.append(f"{r['plan_name']}  reason={r['reason']}  staging={r['staging']}  "
                     f"type={r['hafrash_type']}  floors={r['floors']}  "
                     f"developer={r.get('developer', '—')}  {r['note']}")
    io.open(SUMMARY_TXT, "w", encoding="utf-8").write("\n".join(lines))


# ── main ─────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--single", help="enrich one taba (bare or 101-form)")
    ap.add_argument("--from-report", action="store_true",
                    help="seed the queue from new_plans_report.json first")
    ap.add_argument("--derive-new", action="store_true",
                    help="seed the queue with plans newly present in plans.geojson "
                         "(catches cloud-detected plans); first run just seeds the marker")
    ap.add_argument("--no-floors", action="store_true", help="skip floor prep")
    ap.add_argument("--no-download", action="store_true",
                    help="use cached horaot/nispach only, no browser")
    ap.add_argument("--no-email", action="store_true")
    ap.add_argument("--no-table5", action="store_true",
                    help="skip the Table 5 -> GS refresh (update_mavat_ui --plans-file)")
    ap.add_argument("--no-fund", action="store_true",
                    help="skip the קרן תחזוקה recheck (maintenance_fund.json)")
    ap.add_argument("--limit", type=int, help="process at most N plans")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    allow_download = not args.no_download

    props = load_plan_props()

    # Optionally seed from the new-plans report (useful after a cloud detection
    # run whose plans.geojson we just pulled but whose queue we never received).
    if args.from_report and NEW_PLANS_REPORT.exists():
        try:
            rep = json.load(io.open(NEW_PLANS_REPORT, encoding="utf-8"))
            seeds = []
            for item in (rep.get("new_plans") or rep.get("plans") or []):
                tb = eq.norm_taba(item.get("plan_number") or item.get("taba")
                                  or item.get("pl_number"))
                if tb:
                    seeds.append({"taba": tb, "reason": "new", "queued_at": _now()})
            n = eq.enqueue(seeds)
            print(f"[from-report] seeded {n} plans from new_plans_report.json")
        except Exception as e:
            print(f"[from-report] could not read report: {e}")

    if args.derive_new and not args.single:
        eq.enqueue([{"taba": tb, "reason": "new(derived)", "queued_at": _now()}
                    for tb in derive_new(props)])

    # Build worklist
    if args.single:
        tb = eq.norm_taba(args.single)
        queue = {tb: {"taba": tb, "reason": "manual"}}
    else:
        queue = eq.load_queue()

    if not queue:
        print("Queue empty — nothing to enrich.")
        return

    tabas = list(queue.keys())
    if args.limit:
        tabas = tabas[: args.limit]
    print(f"Enriching {len(tabas)} plan(s) "
          f"(download={'on' if allow_download else 'off'}, "
          f"floors={'off' if args.no_floors else 'on'})\n")

    rows = []
    done_tabas = []        # safe to drop from the queue
    any_developer_extracted = [False]   # set inside the loop; triggers apply at end
    for i, tb in enumerate(tabas, 1):
        meta = props.get(tb, {})
        q = queue.get(tb, {})
        plan_name = meta.get("plan_name") or q.get("plan_name") or pl_number(tb)
        agam_id = meta.get("agam_id") or q.get("agam_id")
        hafrash_prg = meta.get("hafrash_prg", "")
        status = meta.get("status_mavat", "")
        reason = q.get("reason", "")
        print(f"[{i}/{len(tabas)}] {plan_name}  ({reason})")

        notes = []
        download_failed = False

        # 1+2. staging
        h = ensure_horaot(tb, agam_id, allow_download)
        if h in ("missing", "no_doc", "no_agam"):
            staging = "—"
            notes.append(f"horaot:{h}")
            if h == "no_doc" and allow_download:
                download_failed = True  # may appear later; keep queued
        else:
            staging = "✓" if run_staging(tb) else "—"

        # 2b. developer/architect from horaot section 1.8 (only when the
        # current value is missing or a known scrape artifact)
        if h in ("missing", "no_doc", "no_agam"):
            developer = "—"
        elif not dev_needs_fill(meta.get("developer")):
            developer = "קיים"
        else:
            d = run_developer(tb)
            developer = {"ok": "✓", "no data": "—(אין בסעיף 1.8)", "fail": "שגיאה"}[d]
            if d == "ok":
                any_developer_extracted[0] = True

        # 3. type
        types = classify_type(tb, plan_name, hafrash_prg)
        hafrash_type = ", ".join(types) if types else "—"

        # 4. floors
        if args.no_floors:
            floors = "—(skip)"
        elif not hafrash_prg:
            floors = "—(no hafrash)"
        elif not hc.has_built_allocation(hafrash_prg):
            floors = "—(not built)"
        else:
            f = prep_floors(tb, agam_id, allow_download)
            floors = {"queued": "ממתין לקריאה", "cached": "ממתין לקריאה",
                      "no_nispach": "אין נספח", "no_agam": "—(no agam)"}.get(f, f)
            if f == "no_nispach" and allow_download:
                download_failed = True

        rows.append({
            "plan_name": plan_name, "reason": reason, "status": status,
            "staging": staging, "hafrash_type": hafrash_type, "floors": floors,
            "developer": developer,
            "note": "; ".join(notes),
        })
        if not download_failed:
            done_tabas.append(tb)

    # Drop fully-processed plans from the queue; keep download failures for retry.
    if not args.single:
        remaining = eq.dequeue(done_tabas)
        print(f"\nQueue: removed {len(done_tabas)}, {remaining} remain (retry next run)")

    # Write freshly-extracted developers/architects to GS + plans.geojson
    # (single batched run; fill-only + statutory-yazam overwrite policy).
    if any_developer_extracted[0]:
        ok, tail = run_py(["apply_developer_extract.py"], 900, "apply-dev")
        print(f"apply_developer_extract: {'ok' if ok else 'FAILED'}")
        if not ok:
            print(tail[-400:])

    write_summary(rows)

    # קרן תחזוקה recheck — re-extract the maintenance fund for the enriched plans
    # from their (freshly-ensured) horaot and push data/maintenance_fund.json.
    if not args.no_fund and rows:
        run_fund([r["plan_name"] for r in rows])

    # One-email policy: hand our per-plan enrichment summary to update_mavat_ui,
    # which folds it into its single consolidated "עידכון ממבאת" email while it
    # refreshes Table 5 -> GS for these plans (auto-write). We send our own email
    # only as a fallback when that pass is skipped (--no-table5 / --no-download)
    # or fails. --no-email suppresses email on both sides.
    ran_table5 = False
    if not args.no_table5 and allow_download and rows:
        if not args.no_email:
            _write_enrich_email_payload(rows)
        ok, _ = run_table5([r["plan_name"] for r in rows], allow_download, no_email=args.no_email)
        ran_table5 = ok
        if not ok and not args.no_email:
            _clear_enrich_email_payload()   # update_mavat_ui didn't run — no stale payload
    if not (ran_table5 and not args.no_email):
        send_email(rows, dry=args.no_email)

    print(f"\nDone. {len(rows)} plans. Summary -> {SUMMARY_TXT.name}")
    fq = eq.load_floor_queue()
    if fq:
        print(f"floor_read_queue.json: {len(fq)} plan(s) await a Claude-vision floor read.")


if __name__ == "__main__":
    main()
