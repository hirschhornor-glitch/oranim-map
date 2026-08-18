# -*- coding: utf-8 -*-
"""
hafrash_read_merge.py — fold the vision agents' gramoshka reads into the published
`data/hafrash_permit_use.json`, with the evidence crop that backs each one.

Reads every `C:\\ORANIM\\_hafrash_agent_out_<taba>.json` written per
`process_hafrash_queue.md`, validates it, publishes what qualifies, and dequeues
the plan from `hafrash_read_queue.json`.

What this file is NOT: a statement about what the plan allocated. `hafrash_prg`
(the statutory text) stays the source of truth for that. This records only what one
permit's approved drawing actually shows, which is a different fact and may disagree
— hence the separate file and the "לפי גרמושקת ההיתר" wording in the popup.

Publishing rules (deliberately strict — a wrong-but-confident label is the failure
mode that matters here):
  * publish only outcome == "found" and confidence != "low"
  * confidence "high" is REJECTED when every cited evidence PNG is a whole-sheet
    render (`*_pNN.png`). A label "read" at that scale is ~2 px tall, i.e. invented.
  * "allocation_dropped" needs a manual `verified_by` before it is ever published —
    it is the one outcome with policy consequences.
  * sqm_match == false still publishes, but carries a ⚠ into the popup. A 118 vs
    1,631 gap is a finding, not noise to smooth over.

  py hafrash_read_merge.py [--dry-run] [--push] [--calibrate-report]
"""
import glob
import io
import json
import os
import re
import shutil
import sys
from datetime import date

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import enrich_queue as eq

ROOT = r"C:\ORANIM"
GRAM = os.path.join(ROOT, "_hafrash_gram")
REPO = r"C:\dev\oranim-app"
DATA = os.path.join(REPO, "data")
OUT_REL = "data/hafrash_permit_use.json"
OUT = os.path.join(REPO, OUT_REL.replace("/", os.sep))
EVID_DIR = os.path.join(DATA, "hafrash_evidence")
EVID_REL = "data/hafrash_evidence"
AGENT_GLOB = os.path.join(ROOT, "_hafrash_agent_out_*.json")

OUTCOMES = {"found", "not_found", "allocation_dropped", "wrong_permit", "wrong_plan", "illegible"}
# Does the drawing's label say anything the plan's own text did not? A pattern match
# on the whole string is too brittle — real labels read "בניני ציבור מוצע",
# "מבנה ציבור 300 מ\"ר" or "מבנה ציבור / בניני ציבור — ללא שימוש ספציפי". So test
# SUBTRACTIVELY: strip every generic public-area token, the boilerplate qualifiers and
# the numbers, and see whether a real facility name survives. "מועדון לקשיש" and
# "מרכז מידע / גלריה" survive; "בניני ציבור מוצע" does not.
_GENERIC_TOKENS = re.compile(
    r"(מבנה|מבני|מבנים|בניני|בנייני)\s*(ו?מוסדות)?\s*ציבור(יים)?|"
    r"שטח[יי]?\s*ציבור(י|יים)?|שטח\s*לצו?רכי\s*ציבור|צו?רכי\s*ציבור|"
    r"שימוש\s*ציבורי|ציבורי|ציבור")
_BOILERPLATE = re.compile(
    r"מוצע|קיים|מאושר|סה\"כ|ללא\s*שימוש(\s*ספציפי)?|ההיתר\s*משאיר\s*את\s*"
    r"השימוש\s*פתוח|לא\s*נקבע(\s*שימוש)?|טרם\s*נקבע|מ\"ר|מ\"\"ר")
_NOISE = re.compile(r"[\d,.\s\*\-–—/;:()\[\]\"']+")


def is_generic_label(label):
    """True when the label is no more specific than the plan's own generic text."""
    s = str(label or "").strip()
    if not s:
        return True
    s = _GENERIC_TOKENS.sub(" ", s)
    s = _BOILERPLATE.sub(" ", s)
    return not _NOISE.sub("", s).strip()


CONFIDENCES = {"high", "medium", "low"}
SCHEMA_NOTE = ("שימוש ההפרשה המבונה כפי שהוא מופיע בגרמושקת ההיתר, בקריאה ויזואלית. "
               "אינו מחליף את הטקסט הסטטוטורי (hafrash_prg) — זו עובדה נפרדת, ולעתים סותרת. "
               "doc_kind='מאושר' = הרמוניקה חתומה; 'הגשה' = הגשה מקוונת, לא היתר מאושר.")


def load(path, default=None):
    try:
        return json.load(io.open(path, encoding="utf-8"))
    except Exception:
        return default if default is not None else {}


def validate(taba, r, index_rec):
    """Return (publishable, reason). Never raises — a bad agent file must not
    take the whole merge down."""
    out = r.get("outcome")
    if out not in OUTCOMES:
        return False, "bad outcome %r" % out
    conf = r.get("confidence")
    if conf not in CONFIDENCES:
        return False, "bad confidence %r" % conf
    if out == "allocation_dropped":
        if not r.get("verified_by"):
            return False, "allocation_dropped needs manual verified_by"
        return True, ""
    if out != "found":
        return False, out
    if conf == "low":
        return False, "low confidence — flagged for manual review only"
    label = str(r.get("label_he") or "").strip()
    if not label:
        return False, "found without label_he"
    # A generic label is NOT a failed read — it is a different answer. 101-0696104's
    # permit builds the full 4,000 מ"ר as an open shell with no partitions and books it
    # as "מבנה ציבור"; the use was deliberately left open. That is worth publishing
    # ("built, use still undecided"), but it must NOT render as a use row, because it
    # would just repeat hafrash_prg. build_record marks it use_specified: false.
    ev = r.get("evidence") or []
    if not ev:
        return False, "found without evidence"
    # A label "read" off a whole-sheet render is ~2 px tall and therefore invented.
    # Match on the whole-sheet NAME SHAPE (…_pNN.png) rather than on the crop naming,
    # since agents legitimately rename their crops to something descriptive.
    if conf == "high" and not any(not re.search(r"_p\d+\.png$", str(e.get("png") or ""))
                                  for e in ev):
        return False, "high confidence backed only by whole-sheet renders"
    for e in ev:
        base = os.path.basename(str(e.get("png") or "").replace("\\", "/"))
        if not base or not os.path.exists(os.path.join(GRAM, str(taba), base)):
            return False, "evidence png missing: %s" % e.get("png")
    return True, ""


def save_evidence(taba, ev):
    """Commit one cropped JPEG per published read so every claim is checkable from
    the app, following the data/cross_sections/*.jpg precedent."""
    try:
        from PIL import Image
        Image.MAX_IMAGE_PIXELS = None
    except Exception:
        return None
    # Agents cite whatever name they rendered under, sometimes with a folder prefix
    # or an absolute path — reduce to a bare, filesystem-safe basename.
    base = os.path.basename(str(ev["png"]).replace("\\", "/"))
    src = os.path.join(GRAM, str(taba), base)
    os.makedirs(EVID_DIR, exist_ok=True)
    stem = re.sub(r"[^\w.-]", "_", os.path.splitext(base)[0])[-24:].strip("_")
    name = "%s_%s.jpg" % (taba, stem)
    dst = os.path.join(EVID_DIR, name)
    try:
        im = Image.open(src).convert("RGB")
        im.thumbnail((1600, 1600))
        im.save(dst, "JPEG", quality=80, optimize=True)
    except Exception:
        shutil.copyfile(src, dst)
    return "%s/%s" % (EVID_REL, name)


def build_record(taba, r, index_rec, write_evidence=True):
    ch = (index_rec or {}).get("chosen") or {}
    ev = (r.get("evidence") or [{}])[0]
    return {
        "plan_name": (index_rec or {}).get("plan_name") or r.get("plan_name"),
        "outcome": r["outcome"],
        "confidence": r["confidence"],
        "label_he": r.get("label_he"),
        "uses": r.get("uses") or [],
        # False = the drawing locates and quantifies the allocation but never names a
        # facility. The popup then shows a note, not a use row.
        "use_specified": not is_generic_label(r.get("label_he")),
        "sqm_read": r.get("sqm_read"),
        "sqm_expected": r.get("sqm_expected") or (index_rec or {}).get("hafrash_sqm"),
        "sqm_match": r.get("sqm_match"),
        "lot": r.get("lot"),
        "taba_on_sheet": r.get("taba_on_sheet"),
        "taba_match": r.get("taba_match"),
        "permit": {"tik": ch.get("tik"), "doc_kind": ch.get("doc_kind"),
                   "doc_descr": ch.get("doc_descr"), "doc_date": ch.get("doc_date"),
                   "permit_subject": r.get("permit_subject") or ch.get("permit_descr"),
                   "sheet": ev.get("sheet"), "panel": ev.get("panel")},
        "quote_he": ev.get("quote_he"),
        # bbox_frac is the AUTHORITATIVE locator, not the filename: zoom_hafrash's
        # ad-hoc --bbox renders used to reuse one name, so a crop can be stale. Any
        # evidence can be regenerated exactly with
        #   py zoom_hafrash.py <taba> --sheet <sheet> --bbox <x0 y0 x1 y1> --scale 3
        "evidence_bbox": ev.get("bbox_frac"),
        "evidence_jpg": (save_evidence(taba, ev) if (write_evidence and ev.get("png")) else None),
        "read_at": date.today().strftime("%Y-%m-%d"),
        "reader": "claude-vision",
        "verified_by": r.get("verified_by"),
    }


def calibrate_report():
    """Blind check against the property book, for the 6 plans it already answers."""
    from hafrash_classify import domains
    from build_hafrash_read_queue import delivery_answer
    dlv = load(os.path.join(DATA, "hafrasha_delivery.json"), {}).get("plans", {})
    CAT2DOM = {"גן ילדים": "education", "מעון": "education", 'בי"ס/חינוך': "education",
               "בית כנסת": "religion", "מקווה": "religion", "ספורט": "sport",
               "בריאות": "health", "דיור": "welfare", "קהילה/רווחה/תרבות": "welfare"}
    # Two different things are measured, because conflating them misleads:
    #   accuracy — of the reads that named a use, how many hit the right domain.
    #              This is what says whether the reader can be trusted.
    #   coverage — how often the drawing named a use at all.
    agree = named = total = 0
    unnamed = []
    for path in sorted(glob.glob(AGENT_GLOB)):
        taba = re.search(r"_out_(\d+)\.json", path).group(1)
        r = list(load(path).values())[0]
        # Only plans the property book ACTUALLY answers are calibration cases. A
        # placeholder row ("הפרשה מבונה - חברה/קהילה/רווחה", built_sqm 0) is the
        # generic default, not a known answer, and scoring against it would measure
        # agreement with a guess.
        book = delivery_answer(dlv.get(taba))
        if not book:
            continue
        book_doms = set()
        for a in book:
            for c in (a.get("cats") or []):
                if c in CAT2DOM:
                    book_doms.add(CAT2DOM[c])
            book_doms |= set(domains(a.get("asset") or ""))
        read_doms = set(domains(r.get("label_he") or ""))
        total += 1
        # A read that names no use is a gap in the SOURCE, not a wrong answer:
        # 101-0224477 pins the allocation to 103.53 מ"ר (0.5% off) but its document
        # set has no floor plans at all, so nothing there names the facility. Scoring
        # that as a miss would punish the reader for being honest and would make the
        # gate measure document quality instead of reading accuracy.
        if not read_doms:
            unnamed.append((taba, r.get("label_he") or r.get("outcome")))
            print("  %-9s %-6s read=%-28s → %-22s | book=%-22s %s" % (
                taba, r.get("outcome"), (r.get("label_he") or "")[:26], "ללא שימוש",
                ",".join(sorted(book_doms)) or "-", "○"))
            continue
        named += 1
        ok = bool(book_doms & read_doms)
        agree += ok
        print("  %-9s %-6s read=%-28s → %-22s | book=%-22s %s" % (
            taba, r.get("outcome"), (r.get("label_he") or "")[:26],
            ",".join(sorted(read_doms)) or "-", ",".join(sorted(book_doms)) or "-",
            "✓" if ok else "✗"))
    # The gate is proportional and measured on NAMED reads only — a fixed 5-of-6 was
    # never reachable anyway (101-1249358 has no licensing file at all).
    need = max(3, int(named * 0.8 + 0.999)) if named else 0
    ok = bool(named) and agree >= need
    print("\nדיוק: %d/%d מהקריאות שנקבו בשימוש  (שער: >=%d)  %s"
          % (agree, named, need, "עבר" if ok else "לא עבר — לא לפרסם"))
    print("כיסוי: %d/%d מהמסמכים בכלל נקבו בשימוש" % (named, total))
    for t, lbl in unnamed:
        print("   – %-9s ללא שימוש מפורש: %s" % (t, str(lbl)[:40]))
    return agree, named


def main():
    dry = "--dry-run" in sys.argv
    if "--calibrate-report" in sys.argv:
        calibrate_report()
        return

    index = load(os.path.join(GRAM, "_index.json"))
    cur = load(OUT, {"_schema": {"description": SCHEMA_NOTE}, "by_plan": {}})
    cur.setdefault("_schema", {"description": SCHEMA_NOTE})["description"] = SCHEMA_NOTE
    by_plan = cur.setdefault("by_plan", {})

    published, skipped, done = [], [], []
    for path in sorted(glob.glob(AGENT_GLOB)):
        blob = load(path)
        for taba, r in blob.items():
            taba = eq.norm_taba(taba)
            ok, why = validate(taba, r, index.get(taba))
            done.append(taba)
            if not ok:
                skipped.append((taba, why))
                continue
            by_plan[taba] = build_record(taba, r, index.get(taba), write_evidence=not dry)
            published.append(taba)

    named = sum(1 for t in published if by_plan[t].get("use_specified"))
    print("agent files: %d | published: %d (%d with a named use, %d built-but-unspecified) "
          "| not published: %d" % (len(done), len(published), named,
                                   len(published) - named, len(skipped)))
    for t, why in skipped:
        print("   – %-9s %s" % (t, why))
    for t in published:
        rec = by_plan[t]
        print("   %s %-9s %-30s %s%s" % (
            "✓" if rec.get("use_specified") else "◐", t, (rec["label_he"] or "")[:28],
            rec["confidence"], " ⚠sqm" if rec.get("sqm_match") is False else ""))

    if dry:
        print("\n(dry run — nothing written)")
        return

    cur["count"] = len(by_plan)
    cur["built_at"] = date.today().strftime("%Y-%m-%d")
    with io.open(OUT, "w", encoding="utf-8") as f:
        json.dump(cur, f, ensure_ascii=False, indent=1)
    print("\n-> %s (%d plans)" % (OUT, len(by_plan)))

    if done:
        left = eq.dequeue_hafrash(done)
        print("queue: %d left" % left)

    if "--push" in sys.argv:
        import git_sync
        payload = json.loads(json.dumps(cur, ensure_ascii=False))
        git_sync.update_json_and_push(
            OUT_REL, lambda d: (d.clear(), d.update(payload), True)[-1],
            "data: hafrasha allocation use read from permit gramoshkas (%d plans)" % len(by_plan))
        evid = sorted(glob.glob(os.path.join(EVID_DIR, "*.jpg")))
        if evid:
            git_sync._commit_and_push_paths(
                [EVID_REL + "/" + os.path.basename(p) for p in evid],
                "data: gramoshka evidence crops for the hafrasha use reads")


if __name__ == "__main__":
    main()
