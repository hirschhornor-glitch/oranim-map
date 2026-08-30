# -*- coding: utf-8 -*-
r"""
backfill_authority.py — fill the GS/geojson `authority` column from Mavat.

`planDetails.AUTH` ("מקומית" / "מחוזית") is returned by the Mavat SV4/1 API for
every plan, but until 2026-08-30 detect_new_plans only echoed it into its
detection report and never persisted it. 458 of 1,087 rows were left blank, and
a blank authority breaks the objection window: both the plan popup's objection
banner and the "מופקדות להתנגדויות" report special-case ועדה מחוזית for the
mavat_date+60 fallback, so a deposited plan with no authority silently drops out.

FILL-ONLY. A row that already reads "ועדה מקומית"/"ועדה מחוזית" (or the
"וועדה …" spelling variant) is never touched — GS is the master and those values
are hand-curated. Bare "מקומית"/"מחוזית" are expanded locally, no API call.
Mavat values that CONTRADICT a curated row are reported, never written.

Usage:
    python backfill_authority.py                 # dry-run: report only
    python backfill_authority.py --update        # fetch + write GS + geojson
    python backfill_authority.py --update --limit 20

Resumable: every fetched AUTH is checkpointed to C:\ORANIM\_authority_backfill.json,
so a captcha timeout or a crash costs nothing. Re-run to continue.
"""
import argparse
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from detect_new_plans import get_sheet, normalize_authority, BROWSER_DATA
from mavat_auth_js import MAVAT_AUTH_JS

CHECKPOINT = r"C:\ORANIM\_authority_backfill.json"
XPLAN_CACHE = r"C:\ORANIM\_xplan_auth_codes.json"
PLANS_GEOJSON = os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "data", "plans.geojson")

# Rows already carrying a curated value in either accepted spelling.
CURATED_PREFIXES = ("ועדה", "וועדה")


def clean_mid(agam):
    """GS stores agam_id on the older rows as a FLOAT string ('1000311789.0').

    Mavat's /rest/api/SV4/1?mid=... answers 404 (the Angular index.html, ~1.3KB)
    for anything that is not a bare integer, so the entire pre-2015 block of the
    sheet failed until this was stripped. Verified 2026-08-30: mid=1000374779.0
    -> 404, mid=1000374779 -> {"AUTH": "מחוזית"}.
    """
    a = str(agam or '').strip()
    if a.endswith('.0'):
        a = a[:-2]
    return a if a.isdigit() else ''


def col_letter(i):
    """0-based column index -> A1 letter."""
    s = ""
    i += 1
    while i:
        i, r = divmod(i - 1, 26)
        s = chr(65 + r) + s
    return s


def load_checkpoint():
    if os.path.exists(CHECKPOINT):
        try:
            with open(CHECKPOINT, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_checkpoint(d):
    with open(CHECKPOINT, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=1)


def survey(rows, idx):
    """Split the sheet into (curated, expand_locally, needs_fetch, no_agam)."""
    ip, ia, ig = idx["plan_name"], idx["authority"], idx["agam_id"]
    curated, expand, fetch, no_agam = [], [], [], []
    for r_i, row in enumerate(rows[1:], start=1):   # r_i is 0-based into rows
        pn = row[ip].strip() if len(row) > ip else ""
        if not pn:
            continue
        auth = row[ia].strip() if len(row) > ia else ""
        agam = clean_mid(row[ig] if len(row) > ig else "")
        rec = {"row": r_i, "plan_name": pn, "agam_id": agam, "current": auth}
        if auth.startswith(CURATED_PREFIXES):
            curated.append(rec)
        elif auth:
            # bare "מקומית" / "מחוזית" — expand without spending an API call
            expand.append(rec)
        elif agam:
            fetch.append(rec)
        else:
            no_agam.append(rec)
    return curated, expand, fetch, no_agam


async def fetch_authorities(targets, ckpt, limit=None):
    """Fetch planDetails.AUTH for each target, checkpointing as we go."""
    # An entry that only recorded an error is retried on the next run; a
    # resolved authority is never re-fetched.
    todo = [t for t in targets
            if not (ckpt.get(t["plan_name"]) or {}).get("authority")]
    if limit:
        todo = todo[:limit]
    if not todo:
        print("Nothing left to fetch — checkpoint already covers every target.")
        return

    from playwright.async_api import async_playwright

    print(f"\nFetching AUTH for {len(todo)} plans from Mavat...")
    async with async_playwright() as p:
        context = await p.chromium.launch_persistent_context(
            BROWSER_DATA,
            headless=False,
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            args=["--disable-blink-features=AutomationControlled"],
        )
        page = context.pages[0] if context.pages else await context.new_page()

        print("Loading Mavat to establish a session...")
        try:
            await page.goto("https://mavat.iplan.gov.il/SV4/1/1000247867/310",
                            wait_until="domcontentloaded", timeout=120000)
        except Exception:
            pass
        await asyncio.sleep(10)

        async def probe():
            return await page.evaluate("async () => {" + MAVAT_AUTH_JS + """
                try {
                    const r = await fetch('/rest/api/SV4/1?mid=1000247867&guid=0',
                                          { headers: await mvHeaders() });
                    const t = await r.text();
                    return { ok: t.length > 50 && t.trim()[0] === '{', len: t.length };
                } catch (e) { return { ok: false, error: e.message }; }
            }""")

        print("=" * 60)
        print("If a captcha shows in the browser window, solve it — the script")
        print("polls for up to 5 minutes and starts the moment the API answers.")
        print("=" * 60)
        for attempt in range(30):          # up to ~300s at 10s/attempt
            res = await probe()
            if res.get("ok"):
                break
            print(f"  Mavat API not ready ({res}); retrying in 10s...")
            await asyncio.sleep(10)
        else:
            print("Mavat API never became available. Nothing fetched.")
            await context.close()
            return

        print("Mavat API OK.")

        async def fetch_one(mid):
            try:
                return await page.evaluate("async (id) => {" + MAVAT_AUTH_JS + """
                    try {
                        const r = await fetch('/rest/api/SV4/1?mid=' + id + '&guid=0',
                                              { headers: await mvHeaders() });
                        const txt = await r.text();
                        if (!txt || txt.length < 50 || txt.trim()[0] !== '{')
                            return { error: 'http ' + r.status + ' / ' + txt.length + 'b' };
                        const d = JSON.parse(txt).planDetails || {};
                        return { auth: d.AUTH || '', subtype: d.ENTITY_SUBTYPE || '',
                                 numb: d.NUMB || '' };
                    } catch (e) { return { error: e.message }; }
                }""", mid)
            except Exception as e:
                return {"error": str(e)}

        # A 404 here is per-PLAN, not per-session: Mavat answers the next mid
        # happily after refusing one (verified 2026-08-30 — mid 1000374779
        # succeeded between two 404s). So a run of failures must NOT abort the
        # batch, the way the first version did. Only a failure of the known-good
        # probe mid means the session itself died.
        ok = miss = consecutive = 0
        for n, t in enumerate(todo, 1):
            res = await fetch_one(t["agam_id"])
            if res.get("error"):
                await asyncio.sleep(2)
                res = await fetch_one(t["agam_id"])          # one retry

            if res.get("error"):
                miss += 1
                consecutive += 1
                ckpt[t["plan_name"]] = {"auth_raw": "", "authority": "",
                                        "error": res["error"]}
                print(f"  [{n}/{len(todo)}] {t['plan_name']}: unresolved ({res['error']})")
                if consecutive >= 15:
                    if not (await probe()).get("ok"):
                        print("  Session died (probe failed) — stopping; "
                              "re-run to resume from the checkpoint.")
                        break
                    print("  (15 in a row, but the session is alive — continuing.)")
                    consecutive = 0
            else:
                ok += 1
                consecutive = 0
                ckpt[t["plan_name"]] = {"auth_raw": res.get("auth", ""),
                                        "authority": normalize_authority(res.get("auth", "")),
                                        "subtype": res.get("subtype", "")}
                print(f"  [{n}/{len(todo)}] {t['plan_name']}: "
                      f"{ckpt[t['plan_name']]['authority'] or '(ריק)'}")
            if n % 25 == 0:
                save_checkpoint(ckpt)
                print(f"    -- checkpoint: {ok} resolved / {miss} unresolved --")
            await asyncio.sleep(0.4)

        print(f"Mavat: {ok} resolved, {miss} unresolved.")

        save_checkpoint(ckpt)
        await context.close()


def write_sheet(rows, idx, expand, fetch, ckpt):
    """Fill-only write of `authority`. Returns the {plan_name: value} applied."""
    ia = idx["authority"]
    batch, applied, conflicts = [], {}, []

    for rec in expand:
        val = normalize_authority(rec["current"])
        if val and val != rec["current"]:
            batch.append({"range": f"{col_letter(ia)}{rec['row'] + 1}",
                          "values": [[val]]})
            applied[rec["plan_name"]] = val

    for rec in fetch:
        got = ckpt.get(rec["plan_name"])
        if not got or not got.get("authority"):
            continue                     # never fetched, or Mavat 404'd it
        val = got["authority"]
        if rec["current"]:                       # never overwrite a curated cell
            if rec["current"] != val:
                conflicts.append((rec["plan_name"], rec["current"], val))
            continue
        batch.append({"range": f"{col_letter(ia)}{rec['row'] + 1}",
                      "values": [[val]]})
        applied[rec["plan_name"]] = val

    if batch:
        get_sheet().batch_update(batch, value_input_option="RAW")
    print(f"\nGoogle Sheets: {len(batch)} cells written.")
    if conflicts:
        print(f"⚠️  {len(conflicts)} rows where Mavat disagrees with the curated "
              f"value (NOT overwritten):")
        for pn, cur, new in conflicts:
            print(f"    {pn}: GS={cur!r} vs Mavat={new!r}")
    return applied


def xplan_fallback(rows, idx, ckpt):
    """Secondary source for rows Mavat will not serve.

    XPLAN MapServer/1 carries `pl_by_auth_of`, a numeric authority code, with no
    captcha and no per-plan lookup. Validated 2026-08-30 against the 571 rows that
    already had a hand-maintained authority:

        code 2 -> ועדה מחוזית   448 agree, 3 disagree
        code 3 -> ועדה מקומית   118 agree, 2 disagree
        code 1 -> תת"ל          8 rows, all תכנית לתשתית לאומית — the authority is
                                the ועדה לתשתיות לאומיות, which is NEITHER of the
                                two values this column holds, so code 1 is skipped
                                rather than guessed.

    99.1% on the validation set — good, not authoritative, so this only ever fills
    a row Mavat could not answer, and the 5 disagreements are listed by
    `--compare-xplan` for a human to arbitrate.
    """
    codes = {2.0: "ועדה מחוזית", 3.0: "ועדה מקומית"}
    try:
        with open(XPLAN_CACHE, encoding="utf-8") as f:
            xp = json.load(f)
    except Exception as e:
        print(f"XPLAN cache unreadable ({e}) — run with --refresh-xplan first.")
        return {}

    ip, ia = idx["plan_name"], idx["authority"]
    batch, applied, skipped_ttl = [], {}, 0
    for r_i, row in enumerate(rows[1:], start=1):
        pn = row[ip].strip() if len(row) > ip else ""
        auth = row[ia].strip() if len(row) > ia else ""
        if not pn or auth:
            continue
        if (ckpt.get(pn) or {}).get("authority"):
            continue                       # Mavat already answered — it wins
        rec = xp.get(pn)
        if not rec:
            continue
        if rec.get("code") == 1.0:
            skipped_ttl += 1
            continue
        val = codes.get(rec.get("code"))
        if not val:
            continue
        batch.append({"range": f"{col_letter(ia)}{r_i + 1}", "values": [[val]]})
        applied[pn] = val

    if batch:
        get_sheet().batch_update(batch, value_input_option="RAW")
    print(f"XPLAN fallback: {len(batch)} cells written"
          + (f" ({skipped_ttl} תת\"ל rows skipped — ות\"ל, not this column's values)"
             if skipped_ttl else ""))
    return applied


def compare_xplan(rows, idx):
    """List rows where the curated authority contradicts XPLAN's code. Never writes."""
    try:
        with open(XPLAN_CACHE, encoding="utf-8") as f:
            xp = json.load(f)
    except Exception:
        print("XPLAN cache unreadable.")
        return
    codes = {2.0: "מחוזית", 3.0: "מקומית"}
    ip, ia = idx["plan_name"], idx["authority"]
    hits = []
    for row in rows[1:]:
        pn = row[ip].strip() if len(row) > ip else ""
        auth = row[ia].strip() if len(row) > ia else ""
        rec = xp.get(pn)
        if not (pn and auth and rec):
            continue
        want = codes.get(rec.get("code"))
        if want and want not in auth:
            hits.append((pn, auth, want, rec.get("subtype", "")))
    print(f"\n{len(hits)} rows where the curated authority contradicts XPLAN:")
    for pn, auth, want, sub in hits:
        print(f"    {pn}: GS={auth!r} vs XPLAN={want!r}  ({sub})")


def write_geojson(applied, source="מבא\"ת"):
    """Mirror the same values into plans.geojson (fill-only) and push."""
    from git_sync import update_json_and_push

    def edit(data):
        changed = 0
        for f in data["features"]:
            p = f["properties"]
            val = applied.get(str(p.get("plan_name", "")).strip())
            if not val:
                continue
            cur = str(p.get("authority") or "").strip()
            if cur.startswith(CURATED_PREFIXES):
                continue
            if p.get("authority") != val:
                p["authority"] = val
                changed += 1
        return changed

    return update_json_and_push(
        "data/plans.geojson", edit,
        f"data(plans): backfill סמכות ל-{len(applied)} תכניות מ{source}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--update", action="store_true",
                    help="fetch from Mavat and write GS + geojson")
    ap.add_argument("--limit", type=int, default=None,
                    help="fetch at most N plans this run (checkpoint resumes)")
    ap.add_argument("--no-fetch", action="store_true",
                    help="write from the existing checkpoint only")
    ap.add_argument("--xplan-fallback", action="store_true",
                    help="after Mavat, fill the rows it could not serve from "
                         "XPLAN's pl_by_auth_of (99.1%% on the validation set)")
    ap.add_argument("--compare-xplan", action="store_true",
                    help="report curated rows that contradict XPLAN; writes nothing")
    args = ap.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")

    sheet = get_sheet()
    rows = sheet.get_all_values()
    idx = {h.strip(): i for i, h in enumerate(rows[0])}
    for need in ("plan_name", "authority", "agam_id"):
        if need not in idx:
            raise RuntimeError(f"GS header '{need}' not found — refusing to write "
                               f"by position. Restore the header row first.")

    curated, expand, fetch, no_agam = survey(rows, idx)
    print(f"Sheet survey ({len(curated) + len(expand) + len(fetch) + len(no_agam)} plans):")
    print(f"  already curated : {len(curated)}")
    print(f"  bare → expand   : {len(expand)}")
    print(f"  blank → fetch   : {len(fetch)}")
    print(f"  blank, no agam_id: {len(no_agam)}"
          + (f"  {[r['plan_name'] for r in no_agam]}" if no_agam else ""))

    ckpt = load_checkpoint()
    print(f"  checkpoint holds: {len(ckpt)}")

    if args.compare_xplan:
        compare_xplan(rows, idx)
        return

    if not args.update:
        print("\nDry run — pass --update to fetch and write.")
        return

    if not args.no_fetch:
        asyncio.run(fetch_authorities(fetch, ckpt, args.limit))
        ckpt = load_checkpoint()

    applied = write_sheet(rows, idx, expand, fetch, ckpt)
    if applied:
        print(f"geojson: mirroring {len(applied)} values...")
        write_geojson(applied)
    else:
        print("Nothing from Mavat to write.")

    if args.xplan_fallback:
        # Re-read: write_sheet just changed the cells survey() was built from.
        rows = get_sheet().get_all_values()
        fb = xplan_fallback(rows, idx, ckpt)
        if fb:
            print(f"geojson: mirroring {len(fb)} XPLAN-derived values...")
            write_geojson(fb, source="XPLAN (pl_by_auth_of)")

    still = [r["plan_name"] for r in fetch
             if not (ckpt.get(r["plan_name"]) or {}).get("authority")]
    if still:
        print(f"\n{len(still)} plans still without an authority (unfetched or Mavat 404) — re-run to resume.")


if __name__ == "__main__":
    main()
