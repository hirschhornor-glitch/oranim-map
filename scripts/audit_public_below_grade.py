# -*- coding: utf-8 -*-
"""Audit every plan whose Table 5 puts PUBLIC program below the determining entrance.

Since 2026-08-09 שב"צ / הפרשה count only מעל הכניסה הקובעת (parse_table5_xlsx._above_grade_area),
on the premise that below-grade = parking/service. That premise holds when the plan folds
parking into the building's own Table-5 row — 101-1322452 (מתחם תדהר) is the reference case:
no חניון row, and the נספח בינוי shows two levels of חניון under the school, so dropping its
1,400 מ"ר is right.

It does NOT hold when the plan books חניון separately and still places program below the
entrance. Confirmed from the הוראות:
    101-1420926  "התכנית מציעה קומת מסד חלקית לשימושים ציבוריים, מתחת למפלס הכניסה הקובעת"
    101-0650747  "מקלט ציבורי המשמש כחלל דו שימושי"  (plan is exempt from parking entirely)
    101-1370881  "השימושים שמוגדרים מתחת לכניסה הקובעת: מגורים, מבנים ומוסדות ציבור ומסחר
                  יתוכננו בקומת המרתף העליונה (1-)"
Applying the rule to those would delete a real allocation, so this is a REPORT, not a fixer.
`has_parking_row` is a hint, not a verdict — 101-0650747 has no חניון row and is still program.

Verdicts vs the GS value:
    GS=above        the sheet is on the current (above-grade) basis
    GS=with-below   the sheet still carries the pre-2026-08-09 figure
    GS=neither      the sheet disagrees with both — a real error, look at it

Usage:  py audit_public_below_grade.py [--live] [--csv out.csv]
        --live reads the sheet directly; without it the local _gs_snapshot.json is used
        and can be days out of date.
"""
import csv, glob, io, json, os, sys

sys.path.insert(0, r"C:\ORANIM")
sys.stdout.reconfigure(encoding="utf-8")
import parse_table5_xlsx as P

GS_SNAPSHOT = r"C:\ORANIM\_gs_snapshot.json"
XLSX_DIR = r"C:\ORANIM\temp_xlsx"


def _num(v):
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def load_gs(live=False):
    """Rows keyed by taba. --live reads the sheet itself; otherwise the local
    _gs_snapshot.json, which can be days old and show values already corrected."""
    if live:
        import gspread
        from google.oauth2.service_account import Credentials
        gc = gspread.authorize(Credentials.from_service_account_file(
            r"C:\ORANIM\oranim-490018-ceaf784afe61.json",
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]))
        gs = gc.open_by_key("1_AcuuA1CNPh6jXc_lZKNghfpEF1aDPV8Zci8QPz2WVE").sheet1.get_all_values()
    else:
        gs = json.load(io.open(GS_SNAPSHOT, encoding="utf-8"))
        if isinstance(gs, dict):
            gs = gs.get("rows") or list(gs.values())[0]
    hdr = gs[0]
    idx = {h: i for i, h in enumerate(hdr)}
    out = {}
    need = ["taba", "plan_name", "shavatz_out_sqm", "hafrash_sqm"]
    for r in gs[1:]:
        if not isinstance(r, list) or len(r) <= max(idx[n] for n in need):
            continue
        out[str(r[idx["taba"]]).strip()] = {n: r[idx[n]] for n in need}
    return out


def verdict(gs_val, above, below):
    gv = _num(gs_val)
    if gv is None:
        return "GS=empty"
    if abs(gv - above) < 1:
        return "GS=above"
    if abs(gv - (above + below)) < 1:
        return "GS=with-below"
    return "GS=neither"


def main():
    live = "--live" in sys.argv
    gs = load_gs(live)
    src = "live sheet" if live else GS_SNAPSHOT + "  (may be stale — use --live)"
    print(f"GS source: {src}\n")
    rows = []
    for fp in sorted(glob.glob(os.path.join(XLSX_DIR, "*.xlsx"))):
        taba = os.path.splitext(os.path.basename(fp))[0]
        try:
            d = P.result_to_dict(P.parse_table5_xlsx(fp))
        except Exception:
            continue
        g = gs.get(taba)
        if not g:
            continue
        for kind, above_k, below_k, gs_k in (
                ('שב"צ', "public_building_sqm", "public_below_sqm", "shavatz_out_sqm"),
                ("הפרשה", "hafrash_built_sqm", "hafrash_below_sqm", "gs_hafrash")):
            below = d.get(below_k) or 0
            if below <= 0:
                continue
            above = d.get(above_k) or 0
            gs_val = g["shavatz_out_sqm"] if gs_k == "shavatz_out_sqm" else g["hafrash_sqm"]
            rows.append({
                "plan_name": g["plan_name"] or taba, "taba": taba, "field": kind,
                "gs": gs_val, "above": above, "below": below,
                "with_below": round(above + below, 1),
                "has_parking_row": d.get("has_parking_row"),
                "verdict": verdict(gs_val, above, below),
            })

    rows.sort(key=lambda r: -r["below"])
    print(f"{len(rows)} Table-5 lines put public program below the determining entrance "
          f"(of {len(glob.glob(os.path.join(XLSX_DIR, '*.xlsx')))} local Table 5 files)\n")
    print(f"{'plan':<15} {'field':<7} {'GS':>10} {'above':>10} {'below':>9} {'with_below':>11}  "
          f"{'חניון row':<10} verdict")
    for r in rows:
        print(f"{r['plan_name']:<15} {r['field']:<7} {str(r['gs']):>10} {r['above']:>10} "
              f"{r['below']:>9} {r['with_below']:>11}  {str(r['has_parking_row']):<10} {r['verdict']}")

    if "--csv" in sys.argv:
        path = sys.argv[sys.argv.index("--csv") + 1]
        with io.open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
            w.writeheader()
            w.writerows(rows)
        print(f"\nCSV: {path}")


if __name__ == "__main__":
    main()
