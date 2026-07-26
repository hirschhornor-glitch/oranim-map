"""Table 5 re-check for plans that change status — used by update_mavat_ui.py.

When a plan's status changes, while the browser is on its Mavat page we
download the "זכויות והוראות בניה XLS" (Table 5) file from the
"מסמכי מידע מנהלי > קבצים דיגיטלים" section, parse it, compare to the
Google Sheet, write the authoritative values, and return a delta report
for the email.

Source split (2026-06-07):
  - OUT fields (units_total, commerce_out, hafrash_sqm, shavatz_out_sqm, etc.):
    ONLY from the XLSX. The accordion's "מבני ציבור" bucket lumps הפרשה מבונה
    together with שב"צ — unreliable for OUT.
  - IN fields (units_in, commerce_in): from the accordion ("נתונים כמותיים
    עיקריים בתכנית"). The XLSX has no "existing" column.
  - Derived units_add: t5["total_units"] − bal["units_in"]. No accordion
    fallback for total_units.
"""
import asyncio

from parse_table5_xlsx import parse_table5_xlsx, result_to_dict
from parse_quantity_balance import parse_quantity_balance
from scrape_table5_xlsx import download_xlsx, MAVAT_BASE

CLICK_MORE_JS = r"""
() => { let c=0; document.querySelectorAll('*').forEach(el=>{
  if(el.children.length) return; const t=(el.textContent||'').trim();
  if(t.includes('נתונים נוספים')){try{el.click();c++;}catch(e){}} }); return c; }
"""
READ_SECTION_JS = r"""
() => {
  const titles=[...document.querySelectorAll('.uk-accordion-title')];
  const t=titles.find(x=>(x.textContent||'').includes('נתונים כמותיים'));
  if(!t) return '';
  const li=t.closest('li')||t.parentElement;
  let panel=li?li.querySelector('.uk-accordion-content'):null;
  if(!panel) panel=t.nextElementSibling||t.parentElement;
  return panel ? (panel.textContent||'') : '';
}
"""


async def _read_quantity_balance(page, plan=None):
    """Read the 'נתונים כמותיים' accordion so parse_quantity_balance can see the
    per-category מצב מאושר/שינוי rows (that's where units_in lives).

    The section is a NESTED UIkit accordion that (a) only shows the first couple
    of categories until a 'נתונים נוספים' load-more control is clicked, and (b)
    renders each category's מצב מאושר/שינוי detail lazily, only once that category
    is expanded. UIkit ignores JS .click(), so we drive it with real Playwright
    locator clicks. Returns the panel textContent (or '')."""
    # Ensure we're on the plan page. download_xlsx short-circuits (no navigation)
    # when the XLSX is already cached, which would leave us on a blank page.
    agam = str((plan or {}).get("agam_id") or "")
    if agam and agam not in (page.url or ""):
        try:
            await page.goto(f"{MAVAT_BASE}/SV4/1/{agam}/310",
                            wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(6)
        except Exception:
            pass
    try:
        main = page.locator(".uk-accordion-title", has_text="נתונים כמותיים").first
        await main.scroll_into_view_if_needed(timeout=8000)
        # Only open if closed — an earlier download_xlsx pass may have already
        # expanded it, and a blind click would TOGGLE it shut.
        if (await main.get_attribute("aria-expanded")) != "true":
            await main.click(timeout=6000)
    except Exception:
        return ""
    await asyncio.sleep(1.2)
    # Reveal the remaining categories via the "load more" control (bounded loop).
    for _ in range(8):
        more = page.locator("text=נתונים נוספים")
        try:
            cnt = await more.count()
        except Exception:
            cnt = 0
        if not cnt:
            break
        clicked = False
        for i in range(cnt):
            try:
                el = more.nth(i)
                await el.scroll_into_view_if_needed(timeout=3000)
                await el.click(timeout=3000)
                clicked = True
            except Exception:
                pass
        await asyncio.sleep(0.9)
        if not clicked:
            break
    # Expand each category so its מצב מאושר/שינוי rows render.
    try:
        cats = page.locator("ul[sv4-quantity] .uk-accordion-title")
        n = await cats.count()
        for i in range(n):
            try:
                bt = cats.nth(i)
                # Expand only closed categories — clicking an open one collapses it.
                if (await bt.get_attribute("aria-expanded")) != "true":
                    await bt.click(timeout=3000)
            except Exception:
                pass
    except Exception:
        pass
    await asyncio.sleep(1.2)
    try:
        return await page.evaluate(READ_SECTION_JS)
    except Exception:
        return ""

# Table-5 xlsx result key -> GS header (the authoritative "out"/planned state)
# units_total = residential units only ("מגורים" in שימוש). Hotel rooms /
# dorms / other non-residential unit-counted uses are excluded — see plan
# 1360650 (דרך בית לחם 150): the XLSX has 80 מגורים + 200 מלונאות, plan
# proposes 80 yu"d, not 280.
OUT_MAP = [
    ("total_residential_units", "units_total"),
    ("commerce_sqm", "commerce_out"),
    ("employment_sqm", "employment"),
    ("public_building_sqm", "shavatz_out_sqm"),
    ("hafrash_built_sqm", "hafrash_sqm"),
    ("shatzap_sqm", "shatzap_out"),
    ("rental_units", "rental"),
    ("rental_duration_years", "rental_duration"),
    ("conditional_units", "conditional_housing"),
    ("max_height_m", "High"),
    ("max_floors", "level_num"),
]


async def scrape_plan(page, plan):
    """Download + parse both the Table 5 XLSX (OUT) and the accordion (IN).
    Returns {'t5': <dict>, 'bal': <dict>}."""
    t5 = {}
    try:
        path = await download_xlsx(page, plan)
        if path:
            parsed = parse_table5_xlsx(path)
            if parsed and not parsed.error:
                t5 = result_to_dict(parsed)
    except Exception:
        pass
    bal = {}
    try:
        text = await _read_quantity_balance(page, plan)
        if text:
            bal = {k: v for k, v in parse_quantity_balance(text).items() if k != "cards"}
    except Exception:
        pass
    return {"t5": t5, "bal": bal}


def _num(s):
    s = str(s).replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def _fmt(field, v):
    if field in ("units_total", "units_in", "units_add", "rental",
                 "conditional_housing", "level_num"):
        return str(int(round(float(v))))
    return str(int(round(float(v)))) if float(v) == int(float(v)) else str(round(float(v), 1))


def compute_changes(h, row, scraped, plan_label=""):
    """Return (updates {col0idx: strval}, report_lines).

    Per-source split (user rule, 2026-06-07):
      - OUT (XLSX only): every field in OUT_MAP comes from `t5`. The accordion
        is NOT consulted for OUT — its "מבני ציבור" bucket conflates שב"צ with
        הפרשה מבונה (see plan 1300003).
      - IN (accordion only): units_in, commerce_in come from `bal`. The XLSX
        has no "existing/approved" column.
      - units_add = t5["total_units"] − bal["units_in"]. No accordion fallback
        for total_units.

    Writes are applied to GS and reported in the email simultaneously.
    """
    t5 = scraped.get("t5", {})
    bal = scraped.get("bal", {})
    updates, report = {}, []

    def cur(field):
        ci = h.get(field)
        return row[ci].strip() if ci is not None and ci < len(row) else ""

    def set_field(field, new_val, label):
        ci = h.get(field)
        if ci is None or new_val is None:
            return
        old = cur(field)
        new_s = _fmt(field, new_val)
        if _num(old) is not None and _num(new_s) is not None and abs(_num(old) - _num(new_s)) < 0.5:
            return
        if old == new_s:
            return
        updates[ci] = new_s
        report.append(f"      {label}: {old or '∅'} → {new_s}")

    # OUT fields — Table 5 XLSX is authoritative.
    if t5:
        for key, field in OUT_MAP:
            v = t5.get(key, 0)
            if v:  # don't overwrite with zeros from a sparse table
                set_field(field, v, field)

    # IN fields — accordion only.
    ui = bal.get("units_in")
    if ui is not None and t5.get("total_residential_units"):
        set_field("units_in", ui, "units_in")
    if bal.get("commerce_in"):
        set_field("commerce_in", bal["commerce_in"], "commerce_in")

    # Derived מכפיל/תוספת — residential OUT (t5) minus residential IN (bal).
    # Uses total_residential_units to match the units_total mapping above.
    out_total = t5.get("total_residential_units")
    if out_total is not None and ui is not None and ui >= 0 and out_total >= ui:
        set_field("units_add", out_total - ui, "units_add")
        if ui:
            set_field("Machpil", round(out_total / ui, 2), "Machpil")

    if report:
        report.insert(0, f"  📊 {plan_label}: עדכון מטבלה 5 (יוצא) + אקורדיון (נכנס)")
    return updates, report
