"""Table 5 re-check for plans that change status — used by update_mavat_ui.py,
mirroring the land-use (XPLAN) check. When a plan's status changes, while the
browser is on its Mavat page we (re)download Table 5 and read the quantity-balance
section, compare to the Google Sheet, write the authoritative values, and return a
delta report for the email.

Precedence ("טבלה 5 מנצחת את האקורדיון בחוץ"): the *out* fields come from Table 5;
units_in/commerce_in come from the accordion (נכנס); units_add/Machpil derive from
Table-5 units_total minus accordion units_in (skipped if out < in).
"""
import asyncio

from parse_table5_xlsx import parse_table5_xlsx, result_to_dict
from parse_quantity_balance import parse_quantity_balance
# reuse the tested navigation+download (goto + expand accordions + download xls)
from scrape_table5_xlsx import download_xlsx

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
  let panel=t.parentElement?t.parentElement.querySelector('.uk-accordion-content'):null;
  if(!panel) panel=t.nextElementSibling||t.parentElement;
  return panel ? (panel.textContent||'') : '';
}
"""

# Table-5 xlsx result key -> GS header (the authoritative "out"/planned state)
OUT_MAP = [
    ("total_units", "units_total"),
    ("commerce_sqm", "commerce_out"),
    ("employment_sqm", "employment"),
    ("public_building_sqm", "shavatz_out_sqm"),
    ("shatzap_sqm", "shatzap_out"),
    ("rental_units", "rental"),
    ("conditional_units", "conditional_housing"),
    ("max_height_m", "High"),
    ("max_floors", "level_num"),
]


async def scrape_plan(page, plan):
    """page will be navigated to the plan by download_xlsx. Returns {'t5':..,'bal':..}."""
    t5 = {}
    try:
        path = await download_xlsx(page, plan)  # goto + expand accordions + download
        if path:
            parsed = parse_table5_xlsx(path)
            if parsed and not parsed.error:
                t5 = result_to_dict(parsed)
    except Exception:
        pass
    bal = {}
    try:
        await page.evaluate(CLICK_MORE_JS)
        await asyncio.sleep(1.0)
        text = await page.evaluate(READ_SECTION_JS)
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
    """Return (updates {col0idx: strval}, report_lines). Authoritative overwrite for
    status-changed plans, but only when the scraped value is present/meaningful."""
    t5, bal = scraped.get("t5", {}), scraped.get("bal", {})
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
            return  # unchanged
        if old == new_s:
            return
        updates[ci] = new_s
        report.append(f"      {label}: {old or '∅'} → {new_s}")

    # OUT fields from Table 5 (only if Table 5 was found)
    if t5:
        for key, field in OUT_MAP:
            v = t5.get(key, 0)
            if v:  # don't overwrite with zeros from a sparse table
                set_field(field, v, field)
    # IN fields from the accordion
    ui = bal.get("units_in")
    if ui is not None and (bal.get("units_total") or t5.get("total_units")):
        set_field("units_in", ui, "units_in")
    if bal.get("commerce_in"):
        set_field("commerce_in", bal["commerce_in"], "commerce_in")
    # derived תוספת/מכפיל — Table 5 out wins; skip if out < in
    out_total = t5.get("total_units") if t5.get("total_units") else bal.get("units_total")
    if out_total is not None and ui is not None and ui >= 0 and out_total >= ui:
        set_field("units_add", out_total - ui, "units_add")
        if ui:
            set_field("Machpil", round(out_total / ui, 2), "Machpil")

    if report:
        report.insert(0, f"  📊 {plan_label}: עדכון טבלה 5/נכנס")
    return updates, report
