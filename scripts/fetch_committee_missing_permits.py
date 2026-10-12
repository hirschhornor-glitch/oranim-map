"""
fetch_committee_missing_permits.py
----------------------------------
סוגר את הלולאה של audit_committee_vs_permits.py: כל היתר שעלה בוועדת הרישוי
(במינה"קים שלנו) ואינו במאגר — נמשך מ-YK לפי מספר תיק, מסווג, ונכנס למאגר.

Before this (2026-10-12) the audit only *reported* missing permits ("לבדוק ב-YK
ולהוסיף ידנית") and nobody did — 42 of 44 flagged permits were still missing,
among them 2026/0088.00 (28 יח"ד, קורא הדורות 13) and 2022/0299.01 (תמ"א 38, בכר 10).

Flow (run by run_all_committees.bat right after the audit, before build_permits_master):
  1. candidates = every `missing` in every committee_audit_*.json + the current
     jerusalem_committee_matches.json, restricted to IN_AREA_MINHAK via the geocode cache
  2. skip what is already in a permit store or already fetched
  3. YK JSON API by tik: detail (447), applicable plans (457), quantities table (454)
  4. write oranim-app/data/committee_permits.json (every fetched permit, also the ones
     with no plan of ours) and add the ones that resolve to one of our plans to
     extra_permits.json (by_taba — the app merges it into the plan popups)
  5. email the significant ones (תמ"א 38 / בנייה חדשה / תוספת יח"ד / התחדשות) once

CLI:
  py -X utf8 fetch_committee_missing_permits.py              # normal (incremental)
  py -X utf8 fetch_committee_missing_permits.py --no-email
  py -X utf8 fetch_committee_missing_permits.py --refresh    # re-fetch everything
  py -X utf8 fetch_committee_missing_permits.py 2026/0088.00 # specific tik(s), no area filter
"""
import argparse
import glob
import io
import json
import os
import re
import sys
import time
from datetime import datetime

ROOT = r"C:\ORANIM"
sys.path.insert(0, ROOT)

from build_permits_master import (classify, bucket_of, units_added, is_urban_renewal,   # noqa: E402
                                  norm_permit, BUCKET_LABEL)
from geocode_committee_permits import canon_tik                                          # noqa: E402
from scrape_missing_permits_api import fetch_one                                         # noqa: E402
from fetch_permit_quantities import quantities                                           # noqa: E402
from update_meetings_layer import IN_AREA_MINHAK                                         # noqa: E402

DATA = os.path.join(ROOT, "oranim-app", "data")
OUT = os.path.join(DATA, "committee_permits.json")
EXTRA = os.path.join(DATA, "extra_permits.json")
PLANS = os.path.join(DATA, "plans.geojson")
GEO_CACHE = os.path.join(ROOT, "committee_geocode_cache.json")
MATCHES = os.path.join(ROOT, "jerusalem_committee_matches.json")
REPORT_GLOB = os.path.join(ROOT, "permits_scan_reports", "committee_audit_*.json")
STREET_HARVEST = os.path.join(ROOT, "tama38_street_harvest.json")
SOURCE_TAG = "committee_yk"
PAUSE = 1.5           # the API throttles bursts by returning [] — see scrape_missing_permits_api
SIGNIFICANT = {"tama38", "tava_gadol", "ibuy"}


def jload(path, default=None):
    try:
        with io.open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def jsave(path, data):
    tmp = path + ".tmp"
    with io.open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    os.replace(tmp, path)


def known_permits():
    """norm_permit keys of every permit already in one of our stores."""
    known = set()
    for name in ("all_permits.json", "tama38_permits.json"):
        for entry in (jload(os.path.join(DATA, name), {}) or {}).values():
            for p in entry.get("permits") or []:
                k = norm_permit(p.get("file_number"))
                if k:
                    known.add(k)
    for plist in ((jload(EXTRA, {}) or {}).get("by_taba") or {}).values():
        for p in plist or []:
            k = norm_permit(p.get("file_number"))
            if k:
                known.add(k)
    for fn in (jload(os.path.join(DATA, "objections_permits.json"), {}) or {}):
        k = norm_permit(fn)
        if k:
            known.add(k)
    return known


def geo_by_norm():
    """norm_permit → geocode-cache entry (minahak/lat/lng). The cache is keyed by the
    raw agenda spelling and keeps every permit ever geocoded, so it covers old audits."""
    out = {}
    for raw, g in (jload(GEO_CACHE, {}) or {}).items():
        k = norm_permit(raw)
        if k and g:
            out[k] = g
    return out


def collect_candidates():
    """{norm: {raw, meeting_date, essence, address}} from all audits + current agenda."""
    cands = {}

    def add(raw, meeting_date, essence, address):
        k = norm_permit(raw)
        if not k:
            return
        c = cands.setdefault(k, {"raw": raw, "meeting_dates": [], "essence": "", "address": ""})
        if meeting_date and meeting_date not in c["meeting_dates"]:
            c["meeting_dates"].append(meeting_date)
        c["essence"] = c["essence"] or (essence or "")
        c["address"] = c["address"] or (address or "")

    for path in sorted(glob.glob(REPORT_GLOB)):
        for m in (jload(path, {}) or {}).get("missing") or []:
            add(m.get("permit_no_raw") or m.get("permit_no"), m.get("meeting_date"),
                m.get("essence"), m.get("address"))
    cur = jload(MATCHES, {}) or {}
    for m in cur.get("matches") or []:
        add(m.get("permit_no"), cur.get("meeting_date"), m.get("essence"), m.get("address"))
    return cands


def street_harvest_candidates():
    """Significant non-TAMA rows the monthly street harvest saw and we don't hold
    (harvest_tama38_by_street.py → significant_new)."""
    cands = {}
    for r in (jload(STREET_HARVEST, {}) or {}).get("significant_new") or []:
        k = norm_permit(r.get("file_number"))
        if k:
            cands[k] = {"raw": r["file_number"], "meeting_dates": [], "origin": "street_harvest",
                        "essence": r.get("request_description") or r.get("request_type") or "",
                        "address": r.get("street_search", "")}
    return cands


def locate(cands):
    """{norm: {lat, lng, minahak}} from the municipal tik point (layer 2110, exact
    revision only) + unbuffered PiP — the same rule the committee geocoder uses."""
    import geocode_committee_permits as G
    polys = G.load_minhak_polys()
    idx, base = G.load_muni_points()
    raws = {c["raw"]: k for k, c in cands.items()}
    muni, sibling = G.resolve_muni_points(list(raws), idx, base)
    out = {}
    for raw, (lng, lat) in muni.items():
        if raw in sibling:            # another revision's point — not trustworthy
            continue
        out[raws[raw]] = {"lat": lat, "lng": lng,
                          "minahak": G.pip_minhak(lat, lng, polys, allow_buffer=False)}
    return out, G, polys


def our_tabas():
    g = jload(PLANS, {}) or {}
    return {str(f["properties"].get("taba")).strip() for f in g.get("features", [])
            if f.get("properties", {}).get("taba")}


def best_taba(matches):
    # same rule as build_extra_permits.py: largest numeric code = the specific recent plan
    def key(t):
        digits = "".join(ch for ch in t if ch.isdigit())
        return int(digits) if digits else -1
    return max(matches, key=key) if matches else None


def fetch(tik_canon, tabas):
    rec = fetch_one(tik_canon)
    if not rec.get("status"):
        return rec                                   # transient / unknown — retried next run
    q = quantities(tik_canon)
    if q:
        if q.get("units") is not None and q["units"] > 0:
            rec["units"] = int(q["units"])
            rec["units_source"] = "yk_quantities"
        for f in ("units_existing", "units_added", "floors"):
            if q.get(f) is not None:
                rec[f] = int(q[f])
    rec["matched_tabas"] = [t for t in rec.get("tochnit_all", []) if t in tabas]
    rec["taba"] = best_taba(rec["matched_tabas"])
    if 'תמ"א 38' in (rec.get("request_type") or "") or "תמא 38" in (rec.get("request_type") or ""):
        rec["tama_type"] = rec["request_type"]
    return rec


def assess(rec, essence):
    """bucket + significance. The agenda's own מהות is folded in — YK's latest revision
    text can be a small change while the agenda line says what the file really is."""
    view = {"request_type": rec.get("request_type", ""),
            "request_description": (rec.get("request_description") or "") + " " + (essence or ""),
            "units": rec.get("units"), "tama_type": rec.get("tama_type", ""),
            "is_tama38": bool(rec.get("tama_type"))}
    cat = classify(view)
    bucket = bucket_of(cat, view)
    reasons = []
    if bucket == "tama38":
        reasons.append('תמ"א 38')
    if cat in ("bniya_chadasha", "harisa"):
        reasons.append("בנייה חדשה / הריסה")
    u = rec.get("units_added") if rec.get("units_source") == "yk_quantities" else units_added(view)
    if u:
        reasons.append(f'+{u} יח"ד')
    if is_urban_renewal(view):
        reasons.append("התחדשות עירונית")
    return {"category": cat, "bucket": bucket, "bucket_label": BUCKET_LABEL.get(bucket, ""),
            "significant": bucket in SIGNIFICANT, "reasons": reasons}


_DEAD = ("נסגרה", "נדחתה", "נדחה", "בוטל", "בוטלה", "גנוז")


def email_worthy(r):
    if r.get("origin", "committee") == "committee":
        return True                      # on the agenda = live by definition
    st = r.get("status") or ""
    if any(w in st for w in _DEAD):
        return False
    m = re.search(r"(\d{4})\s*$", r.get("status_date") or "")
    return bool(m) and int(m.group(1)) >= datetime.now().year - 1


def plan_names():
    """taba -> (name, year). The name of a plot-level plan IS its address
    ("מקור חיים 56"); the year tells a current plan from the one it replaced."""
    g = jload(PLANS, {}) or {}
    out = {}
    for f in g.get("features", []):
        pr = f.get("properties") or {}
        t = str(pr.get("taba") or "").strip()
        if t and t not in out:
            m = re.search(r"(\d{4})", str(pr.get("mavat_date") or ""))
            out[t] = (pr.get("plan_summary") or pr.get("plan_name_he") or "",
                      int(m.group(1)) if m else 0)
    return out


def _addr_key(text):
    """street + house number, normalised, for comparing a permit address to a plan name."""
    t = re.sub(r"[^א-ת0-9 ]", " ", str(text or ""))
    t = re.sub(r"\s+", " ", t).strip()
    m = re.search(r"(.+?)\s+(\d+)$", t)
    return (m.group(1).strip(), m.group(2)) if m else (t, "")


def report_suspect_filings(by_taba, plan_names):
    """FLAG rows whose address names a different matched plan — never move them.

    A row sits under one of its all_matched_tabas, and when the match is wrong the plan
    is credited with someone else's building: 2026/0230.00 (מקור חיים 56, 68 יח"ד) sat
    under 101-0430249 (מקור חיים 54), which then read 140 יח"ד against a planned 72 and
    produced a 54-unit "gap" that never existed.

    ⚠️ Three rules were tried and all three mis-filed. "Largest numeric code" moved two
    rail permits ("עבודות מקדימות עבור הרכבת הקלה - הקו הכחול") onto a residential plan and
    2024/0081.02 from 101-0653840 (19,152 מ"ר) onto 101-0682849 (30,140 מ"ר) — the LARGER
    plan, because plan codes carry no order of specificity. Address-match, even requiring a
    unique one, moved permits off 101-1256684 onto 101-0104273, the superseded plan it
    replaced: an address match proves a plan COVERS the parcel, not that it is the one being
    built. So this only reports; the call is made per case in permit_plan_overrides.json,
    where the evidence is written down.
    """
    # A tik that already has a decided override is settled — do not keep crying wolf.
    try:
        decided = set(json.load(io.open(os.path.join(DATA, "permit_plan_overrides.json"),
                                        encoding="utf-8")).get("by_tik", {}))
    except Exception:
        decided = set()
    moved = []
    for taba in list(by_taba):
        keep = []
        for p in by_taba[taba] or []:
            if re.sub(r"\.\d+$", "", str(p.get("file_number") or "")) in decided:
                keep.append(p)
                continue
            matches = [t for t in (p.get("all_matched_tabas") or []) if t != taba]
            street, num = _addr_key(p.get("address"))
            # Only an UNAMBIGUOUS address match moves a row. Three plans are all named
            # "קורא הדורות 13"; taking the first match there would have moved permits from
            # the current plan onto the superseded one it replaced.
            # …and only when that plan is NEWER than the one the row sits under. On a plot
            # that has been replanned, the OLD plan is the one named after the address
            # ("קורא הדורות 13", "דוד המלך 8") while its successor carries a project name —
            # so a bare address match points backwards, at the plan already superseded.
            # Equal-area pairs from proc 457 are exactly these successions.
            here_year = (plan_names.get(taba) or ("", 0))[1]
            cands = []
            if street and num:
                for t in matches:
                    s2, n2 = _addr_key((plan_names.get(t) or ("", 0))[0])
                    if n2 != num or not s2 or not (s2 in street or street in s2):
                        continue
                    if (plan_names.get(t) or ("", 0))[1] <= here_year:
                        continue
                    cands.append(t)
            want = cands[0] if len(cands) == 1 else None
            if want:
                moved.append((p.get("file_number"), taba, want, p.get("address")))
            keep.append(p)
        by_taba[taba] = keep
    return moved


def merge_into_extra(records):
    """Add the taba-resolved permits to extra_permits.json (additive; never touches
    entries from other sources). Returns how many were added/updated."""
    extra = jload(EXTRA, {}) or {}
    by_taba = extra.setdefault("by_taba", {})
    for fn, was, now, addr in report_suspect_filings(by_taba, plan_names()):
        print("  ⚠ %s (%s) sits under %s but its address names %s — check and, if it is"
              " really that plan's, add it to permit_plan_overrides.json" % (fn, addr, was, now))
    # drop our own previous rows first so a permit whose taba changed doesn't linger
    for taba in list(by_taba):
        by_taba[taba] = [p for p in by_taba[taba] if p.get("source") != SOURCE_TAG]
        if not by_taba[taba]:
            del by_taba[taba]
    present = {norm_permit(p.get("file_number")) for pl in by_taba.values() for p in pl}
    n = 0
    for r in records.values():
        if not r.get("taba") or not r.get("status") or norm_permit(r["file_number"]) in present:
            continue
        by_taba.setdefault(r["taba"], []).append({
            k: r[k] for k in ("file_number", "status", "status_date", "request_type",
                              "request_description", "address", "units", "units_source",
                              "units_existing", "units_added", "floors", "tama_type")
            if r.get(k) not in (None, "")
        } | {"source": SOURCE_TAG, "source_date": r.get("fetched_at", "")[:10],
             "all_matched_tabas": r.get("matched_tabas", [])})
        n += 1
    extra["count"] = sum(len(v) for v in by_taba.values())
    jsave(EXTRA, extra)
    return n


def email_new_significant(new_sig, dry):
    if not new_sig:
        return False
    rows = "".join(
        "<tr>" + "".join(f'<td style="border:1px solid #ccc;padding:4px 8px">{c}</td>' for c in (
            r["file_number"], r.get("address", ""), r.get("neighborhood_yk", ""),
            "، ".join(r.get("reasons", [])).replace("،", ","),
            (r.get("request_description") or r.get("committee_essence") or "")[:120],
            r.get("status", ""), r.get("taba") or "— (תכנית לא אצלנו)",
            ", ".join(r.get("meeting_dates", [])) or
            ("סריקת רחובות" if r.get("origin") == "street_harvest" else ""))) + "</tr>"
        for r in new_sig)
    head = "".join(f'<th style="border:1px solid #ccc;padding:4px 8px;background:#f2f2f2">{h}</th>'
                   for h in ("תיק", "כתובת", "שכונה", "למה משמעותי", "מהות", "סטטוס YK",
                             "תב\"ע אצלנו", "ישיבה / מקור"))
    n_st = sum(1 for r in new_sig if r.get("origin") == "street_harvest")
    src_txt = ", ".join(t for t in (f"ועדת רישוי: {len(new_sig) - n_st}" if len(new_sig) - n_st else "",
                                     f"סריקת רחובות: {n_st}" if n_st else "") if t)
    reg = set()
    for entry in (jload(os.path.join(DATA, "tama38_permits.json"), {}) or {}).values():
        for p in entry.get("permits") or []:
            k = norm_permit(p.get("file_number"))
            if k:
                reg.add(k.rsplit(".", 1)[0])
    tama = [r["file_number"] for r in new_sig if r.get("bucket") == "tama38"
            and (r.get("norm") or "").rsplit(".", 1)[0] not in reg]
    note = (f'<p>תיקי תמ"א 38 ({", ".join(tama)}) נכנסו למאגר ההיתרים אבל <b>לא</b> '
            f'לרישום הבניינים tama38.geojson — אם רוצים אותם בשכבת תמ"א 38, להוסיף ידנית.</p>'
            if tama else "")
    body = (f'<div dir="rtl" style="font-family:Arial,sans-serif;font-size:14px">'
            f'<p>{len(new_sig)} היתרים משמעותיים לא היו במאגר ונמשכו עכשיו מ-YK '
            f'({src_txt}):</p><table style="border-collapse:collapse">'
            f'<tr>{head}</tr>{rows}</table>{note}</div>')
    subject = f"היתרים משמעותיים חדשים במאגר: {len(new_sig)} ({src_txt})"
    if dry:
        print(f"[email skipped --no-email] {subject}")
        return False
    from ops_alert import send_email
    return send_email(subject, body, "html")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("tiks", nargs="*")
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--no-email", action="store_true")
    ap.add_argument("--from-street-harvest", action="store_true",
                    help="candidates = significant_new of tama38_street_harvest.json")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")

    store = jload(OUT, {}) or {}
    records = store.get("permits") or {}
    known = known_permits()
    geo = geo_by_norm()
    tabas = our_tabas()

    skipped = store.get("out_of_area") or {}
    by_name = None                   # (G, subn_fallback) for street-harvest name fallback
    if args.from_street_harvest:
        cands = {k: c for k, c in street_harvest_candidates().items()
                 if k not in known and k not in skipped
                 and (args.refresh or not (records.get(k) or {}).get("status"))}
        loc, G, polys = locate(cands)
        geo = {**geo, **loc}
        out_area = {k: (loc[k]["minahak"] or "?") for k in cands
                    if k in loc and loc[k]["minahak"] not in IN_AREA_MINHAK}
        skipped.update(out_area)
        todo = [k for k in sorted(cands) if k not in out_area]
        by_name = (G, G.build_subn_fallback(polys))
        print(f"street harvest: {len(cands)} new significant, {len(out_area)} outside our "
              f"מינה\"קים by point, {len(todo)} to fetch ({len(todo) - len([k for k in todo if k in loc])} "
              f"without a point — decided by YK neighborhood)")
    elif args.tiks:
        cands = {norm_permit(t): {"raw": t, "meeting_dates": [], "essence": "", "address": ""}
                 for t in args.tiks if norm_permit(t)}
        todo = list(cands)
    else:
        cands = collect_candidates()
        in_area = {k for k in cands if (geo.get(k) or {}).get("minahak") in IN_AREA_MINHAK}
        print(f"candidates: {len(cands)} flagged by audits/agenda, {len(in_area)} in our מינה\"קים")
        todo = [k for k in sorted(in_area)
                if args.refresh or (k not in known and not (records.get(k) or {}).get("status"))]
        for k in in_area:                       # keep agenda metadata fresh on known records
            if k in records:
                records[k]["meeting_dates"] = sorted(set(records[k].get("meeting_dates", []))
                                                     | set(cands[k]["meeting_dates"]))
    print(f"to fetch from YK: {len(todo)}")

    fetched = failed = 0
    for i, k in enumerate(todo, 1):
        c = cands[k]
        tik = canon_tik(c["raw"]) or c["raw"]
        rec = fetch(tik, tabas)
        g = geo.get(k) or {}
        if by_name and not g.get("minahak"):
            G, subn = by_name
            mh = G.resolve_minhak_by_name(rec.get("neighborhood_yk", ""), subn) if rec.get("status") else ""
            if rec.get("status") and mh not in IN_AREA_MINHAK:
                skipped[k] = mh or rec.get("neighborhood_yk") or "?"
                print(f"[{i}/{len(todo)}] – {tik}  outside our area ({skipped[k]})")
                time.sleep(PAUSE)
                continue
            g = {"minahak": mh}
        rec.update({"norm": k, "meeting_dates": c["meeting_dates"],
                    "origin": c.get("origin", "committee"),
                    "committee_essence": c["essence"], "committee_address": c["address"],
                    "lat": g.get("lat"), "lng": g.get("lng"), "minahak": g.get("minahak"),
                    "fetched_at": datetime.now().isoformat(timespec="seconds")})
        if not rec.get("status"):
            failed += 1
            print(f"[{i}/{len(todo)}] ✗ {tik}  {rec.get('error', 'no detail')}")
            prev = records.get(k) or {}
            rec["attempts"] = prev.get("attempts", 0) + 1
            records[k] = prev | {"attempts": rec["attempts"], "last_error": rec.get("error", "no detail")} \
                if prev.get("status") else rec
            time.sleep(PAUSE)
            continue
        prev = records.get(k) or {}
        rec.update(assess(rec, c["essence"]))
        rec["alerted_at"] = prev.get("alerted_at")
        records[k] = rec
        fetched += 1
        print(f"[{i}/{len(todo)}] ✓ {tik}  {rec['bucket_label']:24} {'★' if rec['significant'] else ' '} "
              f"taba={rec.get('taba') or '-'}  {', '.join(rec['reasons'])}  | {rec['status'][:28]}")
        time.sleep(PAUSE)

    with_taba = merge_into_extra(records)
    new_sig = [r for r in records.values()
               if r.get("significant") and r.get("status") and not r.get("alerted_at")]
    # The street harvest surfaces history too (closed files, permits issued in 2023).
    # Email only what is alive: not closed/rejected and touched since last year. The
    # rest is in the store and on the map, just not worth a mail — mark it so it never is.
    quiet = [r for r in new_sig if not email_worthy(r)]
    for r in quiet:
        r["alerted_at"] = "suppressed:inactive"
    new_sig = [r for r in new_sig if email_worthy(r)]
    if email_new_significant(sorted(new_sig, key=lambda r: r["file_number"]), args.no_email):
        now = datetime.now().isoformat(timespec="seconds")
        for r in new_sig:
            r["alerted_at"] = now

    ok = [r for r in records.values() if r.get("status")]
    jsave(OUT, {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "source": "YK API — היתרים שלא היו במאגר: מסדר היום של ועדת הרישוי + משמעותיים מסריקת הרחובות",
        "count": len(ok),
        "significant": sum(1 for r in ok if r.get("significant")),
        "permits": records,
        "out_of_area": skipped,
    })
    print(f"\nfetched {fetched}, failed {failed}; store: {len(ok)} permits "
          f"({sum(1 for r in ok if r.get('significant'))} significant, {with_taba} on a plan of ours)")
    print(f"Wrote {OUT}")
    sys.exit(2 if failed and not fetched else 0)


if __name__ == "__main__":
    main()
