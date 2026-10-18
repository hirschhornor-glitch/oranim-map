# -*- coding: utf-8 -*-
"""Download one document section of a Mavat plan as ZIP, by keyword.

Generalises plan_documents/_dl_zip.py (economic opinion only). The individual-doc
URL needs a reCAPTCHA token, so we let the page click its own "הורדת קבצים ב ZIP"
link in the section whose text contains the keyword (see memory
reference_mavat_document_download). The page's SV4/1 doc-list response is
captured too, so the ZIP's cp862-mangled filenames can be matched to real names.

Usage:
  python download_plan_section_zip.py --kind conservation --plans 101-0218677,101-0055293
  python download_plan_section_zip.py --kind economic --queue      (pending items in plan_documents_queue.json)

Output: plan_documents/<pl>/<kind>/*.pdf + doclist.json
"""
import argparse, io, json, os, sys, zipfile
from playwright.sync_api import sync_playwright

sys.stdout.reconfigure(encoding="utf-8")
OUT = r"C:\ORANIM\plan_documents"
QUEUE = r"C:\ORANIM\plan_documents_queue.json"
KEYWORDS = {
    "conservation": ["תיעוד", "שימור"],
    "economic": ["כלכלית", "שמאי"],
    "social": ["נספח חברתי", "חברתי"],
    "program": ["פרוגרמה"],
    "balance": ["טבלאות איזון", "טבלת הקצאה", "איזון והקצאה"],
    "horaot": ["הוראות התכנית"],
    "engineer": ["מהנדס העיר", "96א"],
    "soil": ["קרקע היסטורי", "סקר קרקע", "איכות קרקע", "זיהום", "דו\"ח סביבה", "סביבה ונוף", "חוות דעת סביבתית"],
}
EXPAND = """()=>{try{document.querySelectorAll('.uk-accordion').forEach(el=>{const a=UIkit.accordion(el);
 el.querySelectorAll(':scope>li').forEach((li,i)=>{if(!li.classList.contains('uk-open'))a.toggle(i,false);});});}catch(e){}
 document.querySelectorAll('.uk-accordion-content').forEach(c=>{c.hidden=false;c.style.display='block';c.style.maxHeight='none';});}"""
# Mark every ZIP link whose nearest enclosing section mentions a keyword. Walk up
# only until the first ancestor that contains another ZIP link — beyond that we'd
# be reading a neighbouring section's text.
MARK = """(kws)=>{const links=[...document.querySelectorAll('*')].filter(e=>e.childElementCount===0&&e.innerText&&e.innerText.includes('הורדת קבצים ב ZIP'));
 const out=[];links.forEach((lk,idx)=>{let n=lk,sec=null;for(let i=0;i<16&&n.parentElement;i++){const p=n.parentElement;
   const nz=[...p.querySelectorAll('*')].filter(e=>e.childElementCount===0&&e.innerText&&e.innerText.includes('הורדת קבצים ב ZIP')).length;
   if(nz>1)break;n=p;sec=p;}
   const t=sec?sec.innerText:'';if(kws.some(k=>t.includes(k))){lk.setAttribute('data-zipmark',String(idx));out.push({idx,head:t.slice(0,80)});}});
 return out;}"""


def doc_rows(api):
    rows = []
    for key in ("rsPlanDocs", "rsPlanDocsAdd"):
        for d in api.get(key) or []:
            rows.append({"id": d.get("ID"), "name": d.get("DOC_NAME"), "info": d.get("ED_DOC_INFO"),
                         "section": d.get("RUB_DESC"),
                         "fname": (d.get("FILE_DATA") or {}).get("fname")})
    return rows


def process(pg, pl, mp, kind):
    dest = os.path.join(OUT, pl, kind)
    os.makedirs(dest, exist_ok=True)
    api = {}

    def on_resp(r):
        if "rest/api/SV4/1?mid=" in r.url and r.status == 200:
            try:
                api.update(r.json())
            except Exception:
                pass
    pg.on("response", on_resp)
    pg.goto(f"https://mavat.iplan.gov.il/SV4/1/{mp}/310", wait_until="domcontentloaded", timeout=90000)
    pg.wait_for_timeout(6000)
    pg.remove_listener("response", on_resp)
    docs = doc_rows(api)
    kws = KEYWORDS[kind]
    wanted = [d for d in docs if any(k in f"{d['name']} {d['info']} {d['section']}" for k in kws)]
    with open(os.path.join(dest, "doclist.json"), "w", encoding="utf-8") as f:
        json.dump({"mp_id": mp, "matched": wanted, "all": docs}, f, ensure_ascii=False, indent=1)
    # The full plan JSON also carries quantities/status, useful to check a
    # report's numbers against the plan (e.g. approved units vs the report's).
    with open(os.path.join(dest, "api.json"), "w", encoding="utf-8") as f:
        json.dump(api, f, ensure_ascii=False)
    for _ in range(5):
        pg.evaluate(EXPAND); pg.wait_for_timeout(800)
    marks = pg.evaluate(MARK, kws)
    print(f"  {pl}: {len(docs)} docs, {len(wanted)} match '{kind}', {len(marks)} ZIP section(s)")
    saved = []
    for m in marks:
        try:
            with pg.expect_download(timeout=300000) as dl:
                pg.click(f"[data-zipmark='{m['idx']}']")
            raw = open(dl.value.path(), "rb").read()
            z = zipfile.ZipFile(io.BytesIO(raw))
            # Iterate entries, not names: Mavat ZIP names are cp862 mojibake and
            # often collide, and z.read(name) then returns the LAST entry with that
            # name for every one of them — 32 documents in 21 plans were lost that
            # way (2026-10-14) before this read each entry by its ZipInfo.
            for i, info in enumerate(z.infolist()):
                nm = info.filename
                if nm.lower().endswith((".pdf", ".dwfx")):
                    out = os.path.join(dest, f"s{m['idx']}_{i}{os.path.splitext(nm)[1].lower()}")
                    with open(out, "wb") as f:
                        f.write(z.read(info))
                    saved.append(out)
        except Exception as e:
            print(f"    ZIP {m['idx']} failed: {str(e)[:80]}")
    print(f"    saved {len(saved)} file(s) -> {dest}")
    return {"docs_listed": len(wanted), "files": [os.path.basename(s) for s in saved]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kind", required=True, choices=sorted(KEYWORDS))
    ap.add_argument("--plans", help="comma-separated plan numbers (must be in the queue or plans.geojson)")
    ap.add_argument("--queue", action="store_true", help="all pending queue items of this kind")
    ap.add_argument("--mp", help="plan:mp_id pairs, comma-separated (plans outside the queue)")
    ap.add_argument("--headed", action="store_true")
    a = ap.parse_args()
    q = json.load(open(QUEUE, encoding="utf-8")).get(a.kind, {})
    if a.mp:
        targets = dict(x.split(":", 1) for x in a.mp.split(","))
    elif a.plans:
        targets = {p: q[p]["mp_id"] for p in a.plans.split(",") if p in q}
    else:
        targets = {p: v["mp_id"] for p, v in q.items() if v.get("status") == "pending"}
    results = {}
    with sync_playwright() as pw:
        b = pw.chromium.launch(headless=not a.headed)
        for pl, mp in targets.items():
            ctx = b.new_context(accept_downloads=True)   # SV4/1 token is single-use per load
            try:
                results[pl] = process(ctx.new_page(), pl, mp, a.kind)
            except Exception as e:
                print(f"  {pl}: FAILED {str(e)[:100]}")
                results[pl] = {"error": str(e)[:200]}
            finally:
                ctx.close()
        b.close()
    print(json.dumps(results, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
