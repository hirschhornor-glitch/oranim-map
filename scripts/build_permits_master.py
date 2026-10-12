"""
build_permits_master.py
------------------------
THE single canonical permit store. Every permit surfaces across many sources with
differently-padded numbers and different keys; this merges them all into ONE record
per permit application (keyed by tik = year/number, collapsing .00/.01/.02 revisions)
that carries all the info + provenance + facet flags + a classification, so the app
derives each view by filtering the master instead of reading separate datasets.

Two correctness rules that keep the buckets honest:
  • ONE record per tik — revisions (.NN) of the same application are aggregated, not
    counted as separate permits.
  • תמ"א 38 is decided by the permit's OWN text (or tama_type), NOT by mere membership
    in the tama38 store — the tama38 scraper pulls every permit of a registered
    building, most of which are ordinary (ממ"ד, מרפסת, שימוש חורג). Store membership is
    kept as `in_tama38_registry` metadata instead.

Classification mirrors classifyPermitCategory in app.jsx (the 11-category YK taxonomy),
then groups into super-buckets (unit-count aware):
  tama38          → תמ"א 38
  zchuyot         → הגדלת זכויות (ללא יח"ד)   (ממ"ד/מחסן/מרפסת/מעלית, שינויים — 0 יח"ד)
  ibuy            → עיבוי (תוספת יח"ד)          (מוסיף 1..10 יח"ד, לא תמ"א/לא התחדשות)
  shimush_chorag  → שימוש חורג                  (בית כנסת/גן ילדים/מסחר וכו')
  tava_gadol      → תב"ע גדול / התחדשות        (>10 יח"ד, פינוי-בינוי, בניין חדש)
  acher           → אחר                         (תשתית/דרכים/לא-מסווג)

Output: oranim-app/data/permits_master.json
"""
import io
import json
import os
import re
from datetime import datetime

ROOT = r"C:\ORANIM"
DATA = os.path.join(ROOT, "oranim-app", "data")
OUT = os.path.join(DATA, "permits_master.json")
IBUY_MAX_UNITS = 10  # תוספת > זה, או פינוי-בינוי/התחדשות → תב"ע גדול (לא "עיבוי")
# ועדת הרישוי כלל-עירונית; אנחנו רוצים רק תיקים שנפתר להם מינה"ק שבתחום אורנים.
# מסננים לפי המינה"ק שה-geocoder חישב ב-PiP (לא לפי שם-שכונה גולמי — זה מה שהשמיט
# בעבר תיקים תקינים כמו גבעת הורדים). מחוץ לתחום / ללא מינה"ק → לא נקלט כלל.
IN_AREA_MINHAK = {"גינות העיר", "גוננים", "בקעה רבתי",
                  "מינהל מוסדי מלחה", "א.ת. תלפיות", "בית צפאפא"}


# ── עבודות תשתית כלל-עירוניות שהתגלגלו לתב"ע שלנו (רק"ל) ──────────────────────
# YK files a permit under EVERY plan whose area its works touch, so the light rail's
# works surface under ordinary plans the line merely crosses — "עבודות הכנה לרכבת
# הקלה - הקו הירוק - קטע פת דרום" sits under 101-0252379 (מתחם אגד). Attributing it
# to that plan inflates its permit count and, via the פיקוח file that scrape_pikuah_status
# keys off `taba`, hands it a bogus "גמר בנייה". Those works belong to the rail plan
# itself (101-0209593 / 101-0178129 …, plan_type=תשתיות), so we refuse the taba on any
# other plan — the record itself is kept, with the refused plans listed for audit.
# Mirrored by isRailInfraPermit() in oranim-app/src/app.jsx — keep the two in sync.
# NOTE: an explicit rail token is required. "הקו הכחול" alone is NOT enough — permits
# also use it for a plan's own blue line (הריסה בתחום הקו הכחול לפי תב"ע …).
RAIL_INFRA_RE = re.compile(r"""רכבת\s*ה?קלה|רק["״']ל|חדר\s*מיישרים|תחנת\s*מיישרים|חניון\s*חנה\s*וסע""")
INFRA_PLAN_TYPES = {"תשתיות", "מוסתר"}


def is_rail_infra(p):
    txt = (p.get("request_type") or "") + " " + (p.get("request_description") or "")
    return bool(RAIL_INFRA_RE.search(txt))


def plan_types_by_taba():
    """{taba: plan_type} from plans.geojson — decides which plan a rail permit may stay on."""
    gj = load("plans.geojson")
    out = {}
    for f in ((gj or {}).get("features") or []):
        pr = f.get("properties") or {}
        t = str(pr.get("taba") or "").strip()
        if t and t not in out:
            out[t] = (pr.get("plan_type") or "").strip()
    return out


def load(fname, root=DATA):
    try:
        with io.open(os.path.join(root, fname), encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# ── canonical key + tik (mirror normalize_permit_no / normPermitNo) ────────────
_PERMIT_RE = re.compile(r"^(\d{4})/0*(\d+)\.0*(\d+)$")


def norm_permit(raw):
    s = str(raw or "").strip()
    if not s:
        return None
    if "/" in s and "." not in s:
        s += ".0"
    m = _PERMIT_RE.match(s)
    if not m:
        return None
    return f"{m.group(1)}/{int(m.group(2))}.{int(m.group(3))}"


def tik_key(raw):
    """year/number without the revision — the identity of the application."""
    k = norm_permit(raw)
    return k.rsplit(".", 1)[0] if k else None


def parse_dmy(s):
    """DD/MM/YYYY → sortable (Y, M, D) tuple, or None."""
    m = re.match(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})", str(s or ""))
    if not m:
        return None
    return (int(m.group(3)), int(m.group(2)), int(m.group(1)))


def revision_of(raw):
    k = norm_permit(raw)
    try:
        return int(k.rsplit(".", 1)[1]) if k else -1
    except Exception:
        return -1


# ── classification (faithful port of classifyPermitCategory, app.jsx:19095) ────
CAT_LABEL = {
    "bniya_chadasha": "בניה חדשה", "tosefet": "תוספת בניה", "harisa": "הריסה",
    "chafira_milui": "חפירה ומילוי", "tama38": "תמ\"א 38", "shinuyim": "תוכנית שינויים",
    "shimush_chorech": "שימוש חורג", "drachim": "עבודות בדרכים", "mivne_nilve": "מבנה נלווה",
    "tachzukah": "תחזוקה/בטיחות", "acher": "אחר",
}
BUCKET_LABEL = {
    "tama38": "תמ\"א 38",
    "zchuyot": "הגדלת זכויות (ללא יח\"ד)",
    "ibuy": "עיבוי (תוספת יח\"ד)",
    "shimush_chorag": "שימוש חורג",
    "tava_gadol": "תב\"ע גדול / התחדשות",
    "acher": "אחר",
}
BUCKET_ORDER = ["tama38", "zchuyot", "ibuy", "shimush_chorag", "tava_gadol", "acher"]
_UNIT_RE = re.compile(r"(\d+)\s*(?:יח[\"״'׳ד]|דירות|יחיד)")


def classify(p):
    if not p:
        return "acher"
    if p.get("tama_type") or p.get("is_tama38"):
        return "tama38"
    text = (p.get("request_type") or "") + " " + (p.get("request_description") or "")
    if not text.strip():
        return "acher"

    def has(*subs):
        return any(s in text for s in subs)

    if has('תמ"א 38', "תמא 38"):
        return "tama38"
    if has("בניה חדשה", "בנייה חדשה", "בניין חדש"):
        return "bniya_chadasha"
    if has("תוספת בניה", "תוספת בנייה", "הרחבה לבניין", "תוספת יח"):
        return "tosefet"
    if has("שימוש חורג"):
        return "shimush_chorech"
    if has("תוכנית שינויים", "תכנית שינויים", "שינויים ללא תוספת", "שינוי בפתחי"):
        return "shinuyim"
    if has("עבודות בדרכים", "הארכת רחוב", "פתיחת רחוב"):
        return "drachim"
    if has("חפירה ומילוי", "חפירה ודיפון"):
        return "chafira_milui"
    if has("עבודות הריסה"):
        return "harisa"
    if has("חידוש היתר", "הסרת סכנה", "מבנה ארעי", 'מנ"ד'):
        return "tachzukah"
    if has('ממ"ד', "מחסן", "מרפסת", "גדר", "מעלית"):
        return "mivne_nilve"
    if (float(p.get("units") or 0) > 0):
        if has("תוספ", "הרחב", "סגירת מרפסת"):
            return "tosefet"
        return "bniya_chadasha"
    if has("חפירה", "דיפון", "ביסוס"):
        return "chafira_milui"
    if has("הריס"):
        return "harisa"
    if has("הרחב", "תוספ"):
        return "tosefet"
    if has("בקשת שינויים"):
        return "shinuyim"
    if has("כביש", "רכבת", "דרך"):
        return "drachim"
    if has("הקמת", "בניית", "היתר בניה"):
        return "bniya_chadasha"
    if "חדש" in text and has("בניין", "בנין", "מבנה", "בית "):
        return "bniya_chadasha"
    return "acher"


def units_added(p):
    u = p.get("units")
    try:
        if u not in (None, "", 0, "0"):
            n = float(u)
            if n > 0:
                return int(n)
    except Exception:
        pass
    text = (p.get("request_description") or "") + " " + (p.get("request_type") or "")
    best = 0
    for m in _UNIT_RE.finditer(text):
        try:
            best = max(best, int(m.group(1)))
        except Exception:
            pass
    return best


def is_urban_renewal(p):
    text = (p.get("request_description") or "") + " " + (p.get("request_type") or "")
    return any(k in text for k in ("פינוי בינוי", "פינוי-בינוי", "פינוי ובינוי", "התחדשות עירונית"))


def bucket_of(cat, p):
    if cat == "tama38":
        return "tama38"
    if cat == "shimush_chorech":
        return "shimush_chorag"
    u = units_added(p)
    if is_urban_renewal(p) or u > IBUY_MAX_UNITS:
        return "tava_gadol"
    if cat in ("bniya_chadasha", "harisa", "chafira_milui"):
        return "tava_gadol"
    if u >= 1:
        return "ibuy"
    if cat in ("mivne_nilve", "tosefet", "shinuyim", "tachzukah"):
        return "zchuyot"
    return "acher"


# ── merge (keyed by tik; revisions aggregated) ─────────────────────────────────
def upsert(master, permit, *, source, taba=None, in_registry=False):
    fn = permit.get("file_number")
    tik = tik_key(fn)
    if not tik:
        return
    rec = master.get(tik)
    if rec is None:
        rec = master[tik] = {
            "tik": tik, "file_number": fn, "revisions": [], "status": "", "status_date": "",
            "request_type": "", "request_description": "", "units": None, "tama_type": "",
            "address": "", "taba": None, "provenance": [], "in_tama38_registry": False,
            "floors": None, "units_source": "",
            "lat": None, "lng": None, "on_committee_agenda": False, "open_for_objections": False,
            "has_field_obs": False, "unassigned": False,
            "_text": "", "_maxrev": -1, "_status_dt": None,
        }
    rev = revision_of(fn)
    nk = norm_permit(fn)
    if nk and nk not in rec["revisions"]:
        rec["revisions"].append(nk)
    if source not in rec["provenance"]:
        rec["provenance"].append(source)
    if in_registry:
        rec["in_tama38_registry"] = True
    if taba and not rec["taba"]:
        rec["taba"] = str(taba)
    rec["_text"] += " " + (permit.get("request_type") or "") + " " + (permit.get("request_description") or "")
    # STATUS: freshest wins across revisions AND stores (by status_date). This is what
    # reconciles all_permits vs tama38 disagreeing on the same permit.
    if permit.get("status"):
        pd = parse_dmy(permit.get("status_date"))
        take = (rec["_status_dt"] is None and not rec["status"])   # first status seen
        if pd is not None and (rec["_status_dt"] is None or pd > rec["_status_dt"]):
            take = True
        if take:
            rec["status"] = permit["status"]
            rec["status_date"] = permit.get("status_date") or rec["status_date"]
            rec["_status_dt"] = pd if pd is not None else rec["_status_dt"]
    # other display fields follow the latest revision (highest .NN)
    if rev >= rec["_maxrev"]:
        rec["_maxrev"] = rev
        rec["file_number"] = fn
        for f in ("request_type", "request_description"):
            if permit.get(f):
                rec[f] = permit[f]
    for f in ("address", "tama_type"):   # first non-empty
        if not rec[f] and permit.get(f):
            rec[f] = permit[f]
    # units: the authority's own quantities table (fetch_permit_quantities, proc
    # 242700454) beats anything read out of a description, so it overrides even a value
    # already taken from a sibling revision; otherwise first non-empty still wins.
    if permit.get("units_source") == "yk_quantities" and permit.get("units") not in (None, ""):
        rec["units"] = permit["units"]
        rec["units_source"] = "yk_quantities"
    elif rec["units"] in (None, "", 0) and permit.get("units") not in (None, ""):
        rec["units"] = permit["units"]
        if not rec["units_source"]:
            rec["units_source"] = permit.get("units_source") or "description"
    if not rec.get("floors") and permit.get("floors"):
        rec["floors"] = permit["floors"]


def main():
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    master = {}

    ptypes = plan_types_by_taba()
    allp = load("all_permits.json") or {}
    # hand-made attributions (data/permit_plan_overrides.json) — the one plan a permit
    # belongs to when YK filed it under several neighbouring plans. Keyed by tik.
    # keys are normalised through tik_key: the file is written the human way ("2024/0568"),
    # the stores speak "2024/568"
    overrides = {(tik_key(k) or k): v for k, v in
                 (((load("permit_plan_overrides.json") or {}).get("by_tik") or {}).items())}
    rail_detached = 0
    for taba, entry in allp.items():
        infra_plan = ptypes.get(str(taba).strip(), "") in INFRA_PLAN_TYPES
        for p in (entry.get("permits") or []):
            ov = overrides.get(tik_key(p.get("file_number")) or "")
            if ov and str(ov.get("taba")) != str(taba).strip():
                upsert(master, p, source="all_permits")   # kept, but not on this plan
                rec = master.get(tik_key(p.get("file_number")))
                if rec is not None:
                    rec["cross_attributed"] = True
                    refused = rec.setdefault("cross_attributed_from", [])
                    if str(taba) not in refused:
                        refused.append(str(taba))
                continue
            if not infra_plan and is_rail_infra(p):
                # citywide rail works — keep the permit, refuse the plan attribution
                upsert(master, p, source="all_permits")
                rec = master.get(tik_key(p.get("file_number")))
                if rec is not None:
                    rec["cross_attributed"] = True
                    refused = rec.setdefault("cross_attributed_from", [])
                    if str(taba) not in refused:
                        refused.append(str(taba))
                rail_detached += 1
                continue
            upsert(master, p, source="all_permits", taba=taba)

    tama = load("tama38_permits.json") or {}
    for fid, entry in tama.items():
        for p in (entry.get("permits") or []):
            upsert(master, p, source="tama38", taba=entry.get("tik"), in_registry=True)

    extra = load("extra_permits.json") or {}
    for taba, plist in (extra.get("by_taba") or {}).items():
        for p in (plist or []):
            upsert(master, p, source="extra", taba=taba)

    # permits from the licensing-committee agenda that no scraper had — fetched from YK
    # by tik (fetch_committee_missing_permits.py). The taba-resolved ones are also in
    # extra_permits.json; this store also carries the ones on plans we don't track.
    cperm = load("committee_permits.json") or {}
    for p in (cperm.get("permits") or {}).values():
        if p.get("status"):
            upsert(master, p, source="committee_yk", taba=p.get("taba"))
            t = tik_key(p.get("file_number"))
            if t in master and master[t]["lat"] is None and p.get("lat") is not None:
                master[t]["lat"], master[t]["lng"] = p["lat"], p["lng"]

    objp = load("objections_permits.json") or {}
    for fn, p in objp.items():
        pp = dict(p); pp.setdefault("file_number", fn)
        upsert(master, pp, source="objections")
        t = tik_key(fn)
        if t in master:
            master[t]["open_for_objections"] = True
    proc = load("objections_process.json") or {}
    for tik in (proc.get("confirmed_open") or []):
        t = tik_key(tik if isinstance(tik, str) else (tik.get("tik") if isinstance(tik, dict) else ""))
        if t in master:
            master[t]["open_for_objections"] = True

    # committee agenda + approx coords
    cache = load("committee_geocode_cache.json", root=ROOT) or {}
    for pno, g in cache.items():
        t = tik_key(pno)
        if not t:
            continue
        # אזור-בלבד: רק תיקים שנפתר להם מינה"ק בתחום אורנים. מחוץ-לתחום או ללא
        # מינה"ק (כתובת שלא גוקודדה) → מתעלמים לגמרי לצורכי ועדה.
        if g.get("minahak") not in IN_AREA_MINHAK:
            continue
        rec = master.get(t)
        if rec is None:
            upsert(master, {"file_number": pno, "request_description": g.get("essence", ""),
                            "address": g.get("address", "")}, source="committee")
            rec = master.get(t)
        if rec is None:
            continue
        rec["on_committee_agenda"] = True
        if "committee" not in rec["provenance"]:
            rec["provenance"].append("committee")
        if rec["lat"] is None and g.get("lat") is not None:
            rec["lat"], rec["lng"] = g.get("lat"), g.get("lng")

    un = load("unassigned_permits.geojson") or {}
    for ft in (un.get("features") or []):
        pr = ft.get("properties") or {}
        t = tik_key(pr.get("file_number"))
        if not t or t not in master:
            continue
        master[t]["unassigned"] = True
        c = (ft.get("geometry") or {}).get("coordinates")
        if c and master[t]["lat"] is None:
            master[t]["lng"], master[t]["lat"] = c[0], c[1]

    gis = load("permit_gis_points.json") or {}
    pts = gis.get("points")
    if isinstance(pts, dict):
        for fn, v in pts.items():
            t = tik_key(fn)
            if not (t in master and master[t]["lat"] is None):
                continue
            if isinstance(v, dict):
                master[t]["lat"], master[t]["lng"] = v.get("lat"), v.get("lng") or v.get("lon")
            elif isinstance(v, (list, tuple)) and len(v) >= 2:
                # gisviewer cache stores [lon, lat] per tik — the common shape.
                master[t]["lng"], master[t]["lat"] = v[0], v[1]
    elif isinstance(pts, list):
        for v in pts:
            t = tik_key((v or {}).get("file_number"))
            if t in master and master[t]["lat"] is None:
                master[t]["lat"] = (v or {}).get("lat"); master[t]["lng"] = (v or {}).get("lng") or (v or {}).get("lon")

    fobs = load("field_observations.json") or {}
    for fn in (fobs.get("by_file") or {}):
        t = tik_key(fn)
        if t in master:
            master[t]["has_field_obs"] = True

    # ── classify (from aggregated revision text; תמ"א 38 by text/tama_type only) ─
    bucket_counts, cat_counts = {}, {}
    for rec in master.values():
        agg = rec.pop("_text", "")
        rec.pop("_maxrev", None)
        rec.pop("_status_dt", None)
        rec["revisions"] = sorted(rec["revisions"], key=revision_of)
        rec["is_tama38"] = bool(rec.get("tama_type")) or ('תמ"א 38' in agg) or ("תמא 38" in agg)
        # classification view uses the aggregated text of all revisions
        view = {"request_type": "", "request_description": agg,
                "units": rec.get("units"), "tama_type": rec.get("tama_type"),
                "is_tama38": rec["is_tama38"]}
        cat = classify(view)
        rec["units_added"] = units_added(view)
        rec["urban_renewal"] = is_urban_renewal(view)
        rec["category"] = cat
        rec["category_label"] = CAT_LABEL.get(cat, "אחר")
        rec["bucket"] = bucket_of(cat, view)
        rec["bucket_label"] = BUCKET_LABEL[rec["bucket"]]
        bucket_counts[rec["bucket"]] = bucket_counts.get(rec["bucket"], 0) + 1
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

    out = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "count": len(master),
        "bucket_counts": bucket_counts,
        "category_counts": cat_counts,
        "permits": master,
    }
    with io.open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)

    print(f"Wrote {OUT}")
    print(f"  unique permit applications (by tik): {len(master)}")
    print("  by bucket:")
    for b in BUCKET_ORDER:
        print(f"    {BUCKET_LABEL[b]:26} {bucket_counts.get(b, 0)}")
    reg = sum(1 for r in master.values() if r["in_tama38_registry"])
    t38 = sum(1 for r in master.values() if r["is_tama38"])
    print(f"  in tama38 registry: {reg} | classified תמ\"א 38 (by text): {t38}")
    xattr = sum(1 for r in master.values() if r.get("cross_attributed"))
    print(f"  rail-infra attributions refused: {rail_detached} (on {xattr} permit files)")


if __name__ == "__main__":
    main()
