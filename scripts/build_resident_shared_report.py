"""Build the shared-tenant ("חדר דיירים") report.

Reads data/plans.geojson and renders a standalone HTML report of every plan
that has a resident_shared (shared-tenant) area, with its unit count (יח"ד) and
the ratio מ"ר-משותף-לדירה — so shared leisure/common space can be seen against
the residential program of each plan and of the projects as a whole.

Shared-tenant space is PRIVATE to a building's own residents and is tracked
apart from public program (שב"צ/שצ"פ) — see the memory
reference_program_vs_shared_tenant_spaces. This report is deliberately SEPARATE
from the plan popup (the popup shows program; this shows resident-shared).

Usage:
    python build_resident_shared_report.py            # write reports/resident_shared_report.html
    python build_resident_shared_report.py --open      # + open in the browser
"""
from __future__ import annotations

import argparse
import html
import json
import sys
import webbrowser
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
GEOJSON = REPO_ROOT / "data" / "plans.geojson"
OUT = REPO_ROOT / "reports" / "resident_shared_report.html"


def _num(v):
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def collect(geo: dict) -> list:
    rows = []
    for f in geo["features"]:
        p = f["properties"]
        sqm = _num(p.get("resident_shared"))
        if not sqm or sqm <= 0:
            continue
        units = _num(p.get("units_total"))
        rows.append({
            "plan_name": str(p.get("plan_name", "")),
            "name_he": (p.get("plan_summary") or p.get("plan_name_he") or "").strip(),
            "neighborhood": (p.get("SUB_N") or p.get("minahak") or "").strip(),
            "units": int(units) if units else None,
            "sqm": sqm,
            "ratio": (sqm / units) if units else None,
            "notes": (p.get("resident_shared_prg") or "").strip(),
        })
    rows.sort(key=lambda r: r["sqm"], reverse=True)
    return rows


def render(rows: list) -> str:
    n = len(rows)
    total_sqm = sum(r["sqm"] for r in rows)
    total_units = sum(r["units"] for r in rows if r["units"])
    avg_ratio = (total_sqm / total_units) if total_units else 0
    e = html.escape

    def fmt(x, dp=0):
        if x is None:
            return "—"
        return f"{x:,.{dp}f}"

    cards = [
        ("תכניות עם שטחי דיירים משותפים", fmt(n), ""),
        ('סה"כ שטח דיירים משותף', fmt(total_sqm), 'מ"ר'),
        ('יח"ד בתכניות אלה', fmt(total_units), ""),
        ('ממוצע משוקלל', fmt(avg_ratio, 2), 'מ"ר לדירה'),
    ]
    card_html = "".join(
        f'<div class="card"><div class="card-val">{v}<span class="card-unit">{u}</span></div>'
        f'<div class="card-lbl">{lbl}</div></div>'
        for lbl, v, u in cards
    )

    trs = []
    for r in rows:
        ratio_cls = "hi" if (r["ratio"] and r["ratio"] >= 1.5) else ""
        trs.append(
            f'<tr><td class="mono">{e(r["plan_name"])}</td>'
            f'<td>{e(r["name_he"]) or "—"}</td>'
            f'<td class="num">{fmt(r["units"])}</td>'
            f'<td class="num">{fmt(r["sqm"])}</td>'
            f'<td class="num {ratio_cls}">{fmt(r["ratio"], 2)}</td>'
            f'<td class="notes">{e(r["notes"])}</td></tr>'
        )
    rows_html = "\n".join(trs) or '<tr><td colspan="6" class="empty">אין נתונים עדיין</td></tr>'

    return f"""<!doctype html>
<html lang="he" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>שטחי דיירים משותפים · דו"ח</title>
<style>
  :root {{ color-scheme: dark; }}
  * {{ box-sizing: border-box; }}
  body {{ margin:0; background:#0f1420; color:#e6ecf5;
         font-family:"Assistant","Segoe UI",Arial,sans-serif; line-height:1.5; }}
  .wrap {{ max-width:1080px; margin:0 auto; padding:28px 20px 60px; }}
  h1 {{ font-size:26px; margin:0 0 4px; }}
  .sub {{ color:#8ea0bd; font-size:14px; margin-bottom:24px; }}
  .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr));
            gap:14px; margin-bottom:28px; }}
  .card {{ background:linear-gradient(135deg,#16213e,#0f3460); border:1px solid #24365c;
           border-radius:12px; padding:16px 18px; }}
  .card-val {{ font-size:30px; font-weight:700; }}
  .card-unit {{ font-size:13px; color:#9db4d8; margin-right:6px; font-weight:500; }}
  .card-lbl {{ font-size:13px; color:#9db4d8; margin-top:4px; }}
  table {{ width:100%; border-collapse:collapse; font-size:14px;
           background:#131a2a; border-radius:12px; overflow:hidden; }}
  th,td {{ padding:10px 12px; text-align:right; border-bottom:1px solid #223052; }}
  th {{ background:#1b2743; color:#c6d4ee; font-weight:600; position:sticky; top:0; }}
  tr:last-child td {{ border-bottom:none; }}
  tr:hover td {{ background:#182238; }}
  .num {{ text-align:left; font-variant-numeric:tabular-nums; direction:ltr; }}
  .mono {{ font-family:ui-monospace,Consolas,monospace; color:#9db4d8; direction:ltr; text-align:left; }}
  .notes {{ color:#a9bad8; font-size:13px; }}
  .hi {{ color:#ffd479; font-weight:700; }}
  .empty {{ text-align:center; color:#7f8db0; padding:24px; }}
  .foot {{ margin-top:18px; color:#6f7fa3; font-size:12px; }}
  .legend {{ margin-top:10px; color:#8ea0bd; font-size:12.5px; }}
</style>
</head>
<body>
<div class="wrap">
  <h1>שטחי דיירים משותפים ("חדר דיירים")</h1>
  <div class="sub">שטחי פנאי משותפים · מרפסות משותפות · חדר דיירים — פרטיים לדיירי הבניין, לא פרוגרמה ציבורית (שב"צ/שצ"פ)</div>
  <div class="cards">{card_html}</div>
  <table>
    <thead><tr>
      <th>תב"ע</th><th>תכנית</th><th class="num">יח"ד</th>
      <th class="num">שטח משותף (מ"ר)</th><th class="num">מ"ר לדירה</th><th>פירוט</th>
    </tr></thead>
    <tbody>
{rows_html}
    </tbody>
  </table>
  <div class="legend">מודגש בצהוב = ‎1.5 מ"ר לדירה ומעלה. "מ"ר לדירה" = שטח משותף חלקי מספר יח"ד בתכנית.</div>
  <div class="foot">המקור: הערות טבלה 5 (זכויות והוראות בניה) שחולצו ב-parse_table5_xlsx. מסמך פנימי — לא לפרסום.</div>
</div>
</body>
</html>
"""


def main() -> None:
    if sys.platform.startswith("win"):
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--open", action="store_true", help="open the report in the browser")
    args = ap.parse_args()

    geo = json.loads(GEOJSON.read_text(encoding="utf-8"))
    rows = collect(geo)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(render(rows), encoding="utf-8")
    total = sum(r["sqm"] for r in rows)
    print(f"wrote {OUT.relative_to(REPO_ROOT)} — {len(rows)} plans, {total:,.0f} מ\"ר total")
    if args.open:
        webbrowser.open(OUT.as_uri())


if __name__ == "__main__":
    main()
