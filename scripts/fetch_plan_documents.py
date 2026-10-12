"""Weekly pull of the planning administration's plan-documents layer (MipuiMN).

Source: https://services2.arcgis.com/9xNzs4HrnCQY9yx4/arcgis/rest/services/MipuiMN/FeatureServer/10
(the "תכניות עם מסמכים ונספחים" Experience Builder app). One row per appendix of a
plan: type, name, document group, status. No file links, no content. It says WHICH
plans have a tree survey / social appendix / documentation file / economic
opinion etc., so the heavy browser scrapers only visit plans that need it.

Coverage measured 2026-10-12 against the 240 tree surveys we actually downloaded:
the layer flags 175 (73%). It is a TRIGGER, not a replacement — the quarterly
full re-check in monthly_tree_scan stays as the safety net.

Runs in run_mavat_sync.bat (Sunday) before update_mavat_ui, which carries the
summary in its single email (one weekly check — no email of its own).

Outputs:
  oranim-app/data/plan_documents.json      per plan: categories + document list
  C:/ORANIM/plan_documents_queue.json      follow-up work per category (pending)
  C:/ORANIM/_pending_plan_docs_email.json  email section, consumed by update_mavat_ui

Exit: 0 ok, 1 failure (API error / suspicious shrink) + ops alert.
"""
import json, os, re, subprocess, sys, urllib.parse, urllib.request
from datetime import datetime

sys.path.insert(0, r"C:\ORANIM")
from git_sync import update_json_and_push, pull_before_read, commit_and_push_after_write

REPO = r"C:\ORANIM\oranim-app"
PLANS = os.path.join(REPO, "data", "plans.geojson")
OUT_REL = "data/plan_documents.json"
OUT = os.path.join(REPO, "data", "plan_documents.json")
QUEUE = r"C:\ORANIM\plan_documents_queue.json"
EMAIL = r"C:\ORANIM\_pending_plan_docs_email.json"
TREE_META = r"C:\ORANIM\tree_surveys\metadata.json"
SOCIAL = os.path.join(REPO, "data", "social_appendices.json")
URL = ("https://services2.arcgis.com/9xNzs4HrnCQY9yx4/arcgis/rest/services/"
       "MipuiMN/FeatureServer/10/query")
FIELDS = "MP_ID,PL_NUMBER,DOC_GROUP_DESC,DOC_NAME,ED_DOC_INFO,MN_STT,PL_URL"
DRY = "--dry-run" in sys.argv

# MN_STT = the app's display class. 9 = infrastructure (building + water), 10 = general.
STT_CAT = {1: "binui", 2: "environment", 3: "economic", 4: "shading", 5: "water",
           6: "social", 7: "trees", 8: "conservation", 9: "infrastructure", 10: "general"}
# The document name is more specific than MN_STT (a public-space programme is filed
# under building, a shading study sometimes under trees). Checked in order.
NAME_CAT = [
    ("פרוגרמה לשטחי ציבור", "program"),
    ("הצללה", "shading"),
    ("עצ", "trees"),
    ("נספח חברתי", "social"),
    ("כלכלית", "economic"),
    ("תיעוד", "conservation"),
    ("שימור", "conservation"),
]
CAT_HE = {"trees": "עצים", "social": "נספח חברתי", "program": "פרוגרמה לשטחי ציבור",
          "conservation": "תיעוד ושימור", "economic": "חוות דעת כלכלית",
          "shading": "הצללה", "environment": "סביבה/תסקיר", "water": "מים/ביוב/ניקוז",
          "infrastructure": "תשתיות", "binui": "בינוי ופיתוח", "general": "כללי/אחר"}
# Categories that feed a follow-up pipeline (download/extract). The rest are
# displayed only.
QUEUED = ("trees", "social", "program", "conservation", "economic")


def log(m):
    print(f"[{datetime.now():%H:%M:%S}] {m}", flush=True)


def doc_cat(name, stt):
    for kw, cat in NAME_CAT:
        if kw in (name or ""):
            return cat
    return STT_CAT.get(stt, "general")


def load(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def our_plans():
    """mp_id -> {taba, pl_number, name}. mp_id is the Mavat SV4/1 id (= agam_id)."""
    g = load(PLANS, {"features": []})
    out = {}
    for f in g["features"]:
        p = f["properties"]
        mp = None
        m = re.search(r"SV4/1/(\d+)", p.get("mavat_url") or "")
        if m:
            mp = m.group(1)
        elif p.get("agam_id"):
            try:
                mp = str(int(float(p["agam_id"])))
            except (TypeError, ValueError):
                pass
        if mp and mp not in out:
            out[mp] = {"taba": str(p.get("taba") or ""),
                       "pl_number": p.get("plan_name") or "",
                       "name": p.get("plan_name_he") or p.get("plan_name") or ""}
    return out


def fetch(ids):
    rows = []
    for k in range(0, len(ids), 150):
        q = dict(where=f"MP_ID IN ({','.join(ids[k:k + 150])})", outFields=FIELDS,
                 returnGeometry="false", resultRecordCount=4000, f="json")
        req = urllib.request.Request(URL, data=urllib.parse.urlencode(q).encode(),
                                     headers={"User-Agent": "oranim-plan-docs/1.0"})
        d = json.loads(urllib.request.urlopen(req, timeout=120).read())
        if "error" in d:
            raise RuntimeError(f"ArcGIS error: {d['error']}")
        if d.get("exceededTransferLimit"):
            raise RuntimeError("exceededTransferLimit on a 150-id batch — lower the batch size")
        rows += [x["attributes"] for x in d["features"]]
    return rows


def build(rows, plans):
    by = {}
    for r in rows:
        mp = str(int(r["MP_ID"]))
        pl = plans.get(mp)
        if not pl:
            continue
        key = r.get("PL_NUMBER") or pl["pl_number"]
        e = by.setdefault(key, {"mp_id": mp, "taba": pl["taba"], "name": pl["name"],
                                "url": r.get("PL_URL") or f"https://mavat.iplan.gov.il/SV4/1/{mp}/310",
                                "cats": [], "docs": {}})
        name = (r.get("DOC_NAME") or "").strip()
        cat = doc_cat(name, r.get("MN_STT"))
        d = e["docs"].setdefault(name, {"n": name, "c": cat, "g": []})
        grp = (r.get("DOC_GROUP_DESC") or "").strip()
        if grp and grp not in d["g"]:
            d["g"].append(grp)
    for e in by.values():
        e["docs"] = sorted(e["docs"].values(), key=lambda d: d["n"])
        for d in e["docs"]:
            d["g"].sort()
        e["cats"] = sorted({d["c"] for d in e["docs"]})
    return dict(sorted(by.items()))


def have_already():
    """Plans whose follow-up is already done, per queued category."""
    tree_ok = {str(e.get("taba")) for e in load(TREE_META, []) if e.get("status") == "success"}
    soc = load(SOCIAL, {}).get("plans", {})
    return {"trees": ("taba", tree_ok), "social": ("pl", set(soc))}


def main():
    if not DRY and not pull_before_read(REPO):
        log("WARN: git pull failed — working on local state")
    plans = our_plans()
    log(f"{len(plans)} plans with a Mavat id")
    rows = fetch(sorted(plans))
    data = build(rows, plans)
    log(f"{len(rows)} layer rows -> {len(data)} of our plans have documents")

    old = load(OUT, {}).get("plans", {})
    if old and len(data) < 0.5 * len(old):
        raise RuntimeError(f"shrink {len(old)} -> {len(data)} plans; refusing to write")
    baseline = not old

    # What appeared since last week: a plan gaining a category it didn't have.
    new_items = []
    for pl, e in data.items():
        prev = set(old.get(pl, {}).get("cats", []))
        added = [c for c in e["cats"] if c not in prev]
        if added and not baseline:
            new_items.append({"pl": pl, "name": e["name"], "url": e["url"], "cats": added,
                              "docs": [d["n"] for d in e["docs"] if d["c"] in added]})

    # Queue: on the baseline run, every plan in a queued category we don't cover
    # yet (the gap). Afterwards, only what's new this week.
    queue = load(QUEUE, {})
    done = have_already()
    today = datetime.now().strftime("%Y-%m-%d")
    queued_now = {c: 0 for c in QUEUED}
    candidates = data.items() if baseline else [(i["pl"], data[i["pl"]]) for i in new_items]
    for pl, e in candidates:
        for cat in e["cats"]:
            if cat not in QUEUED:
                continue
            if cat in done:
                kind, ok = done[cat]
                if (e["taba"] if kind == "taba" else pl) in ok and baseline:
                    continue
            q = queue.setdefault(cat, {})
            if pl in q and q[pl].get("status") == "pending":
                continue
            q[pl] = {"status": "pending", "first_seen": today, "taba": e["taba"],
                     "mp_id": e["mp_id"], "name": e["name"], "url": e["url"],
                     "docs": [d["n"] for d in e["docs"] if d["c"] == cat]}
            queued_now[cat] += 1
    log("queued: " + ", ".join(f"{c}={n}" for c, n in queued_now.items()))

    counts = {}
    for e in data.values():
        for c in e["cats"]:
            counts[c] = counts.get(c, 0) + 1
    payload = {"_meta": {"generated_at": datetime.now().isoformat(timespec="seconds"),
                         "source": URL.rsplit("/query", 1)[0],
                         "note": "רשימת הנספחים לכל תכנית לפי שכבת מינהל התכנון. "
                                 "הקטגוריה נגזרת משם המסמך ומסיווג התצוגה (MN_STT).",
                         "plans": len(data), "by_category": dict(sorted(counts.items()))},
               "categories": CAT_HE, "plans": data}

    if DRY:
        log(f"dry run: {len(new_items)} new items, baseline={baseline}")
        return 0

    def _apply(cur):
        if cur.get("plans") == payload["plans"]:
            return False
        cur.clear(); cur.update(payload); return True
    msg = (f"data: plan documents layer ({len(data)} plans, +{len(new_items)} new) "
           f"(fetch_plan_documents)")
    tracked = subprocess.run(["git", "-C", REPO, "ls-files", "--error-unmatch", OUT_REL],
                             capture_output=True).returncode == 0
    if tracked:
        ok = update_json_and_push(OUT_REL, _apply, msg)
    else:
        # update_json_and_push stashes untracked files, so it can't create one.
        with open(OUT, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
        ok = commit_and_push_after_write(OUT_REL, msg, REPO)
    if ok is False:
        raise RuntimeError("push of plan_documents.json failed")

    with open(QUEUE + ".tmp", "w", encoding="utf-8") as f:
        json.dump(queue, f, ensure_ascii=False, indent=1)
    os.replace(QUEUE + ".tmp", QUEUE)

    # Email section. Merged with an unconsumed payload so a skipped email (no
    # other changes that week) doesn't lose the items.
    em = load(EMAIL, {})
    seen = {(i["pl"], tuple(i["cats"])) for i in em.get("new", [])}
    em.setdefault("new", []).extend(i for i in new_items if (i["pl"], tuple(i["cats"])) not in seen)
    em["baseline"] = em.get("baseline") or baseline
    em["queued"] = {c: em.get("queued", {}).get(c, 0) + n for c, n in queued_now.items()}
    em["pending"] = {c: sum(1 for v in queue.get(c, {}).values() if v.get("status") == "pending")
                     for c in QUEUED}
    em["generated_at"] = payload["_meta"]["generated_at"]
    if em["new"] or any(em["queued"].values()):
        with open(EMAIL, "w", encoding="utf-8") as f:
            json.dump(em, f, ensure_ascii=False, indent=1)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        import traceback
        tb = traceback.format_exc()
        log(f"FATAL:\n{tb}")
        try:
            from ops_alert import send_alert
            send_alert(f"שכבת מסמכי תכניות נכשלה — {datetime.now():%Y-%m-%d}", tb[-3000:])
        except Exception as e:
            log(f"(alert failed: {e})")
        sys.exit(1)
