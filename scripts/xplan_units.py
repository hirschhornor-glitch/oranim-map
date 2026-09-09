# -*- coding: utf-8 -*-
"""יח"ד מאושרות/תוספת מ-XPLAN — fallback ל"נתונים כמותיים" של מבא"ת.

XPLAN MapServer/1 carries two per-plan dwelling-unit columns:
    pq_authorised_quantity_120  "כמות מאושרת יח\"ד"  → units_in
    quantity_delta_120          "תוספת מס' יח' דיור" → units_add

Verified 2026-09-09 to be exactly this dataset's convention
(units_total = units_in + units_add): 101-1430339 → pq 14 / delta 25 = GS
14/25/39, 101-1417328 → pq 0 / delta 96 = GS 0/+96/96, and across the whole
sheet units_in equals pq120 on 623 of 668 plans.

⚠️ XPLAN also serves garbage in these columns for a minority of plans — m²
values landing in a units field (92,196 on 101-1131192; 4,261.6 on
101-1329770), plain zeros where it simply has no data, and negative deltas.
So a read is trusted ONLY when it reconciles with the Table 5 total:
    |pq + delta − total| <= max(1, 5% of total)
On the current data that gate accepts 457 plans and rejects all the garbage.

Used by table5_status_check.py (accordion fallback) and enrich_api.py.
"""
from __future__ import annotations

import ssl

import requests
from requests.adapters import HTTPAdapter

XPLAN_PLAN_URL = ("https://ags.iplan.gov.il/arcgisiplan/rest/services/"
                  "PlanningPublic/Xplan/MapServer/1/query")
MAX_UNITS = 20000          # anything above this is an area, not a unit count


class _LegacySSLAdapter(HTTPAdapter):
    """ags.iplan.gov.il needs the relaxed cipher list (same as update_mavat_ui)."""

    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.set_ciphers("DEFAULT:@SECLEVEL=0")
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)


_SESSION = requests.Session()
_SESSION.mount("https://ags.iplan.gov.il", _LegacySSLAdapter())


def _clean(val):
    """A dwelling-unit count, or None if the value can't be one."""
    if val is None:
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    if f < 0 or f > MAX_UNITS:          # negative delta / m² in a units column
        return None
    if abs(f - round(f)) > 0.01:        # 97.26 units doesn't exist
        return None
    return float(round(f))


def fetch_raw(pl_number, timeout=45):
    """Return (units_in, units_add) as reported by XPLAN, or (None, None)."""
    if not pl_number:
        return None, None
    params = {
        "where": f"pl_number='{pl_number}'",
        "outFields": "pq_authorised_quantity_120,quantity_delta_120",
        "returnGeometry": "false",
        "f": "json",
    }
    try:
        r = _SESSION.get(XPLAN_PLAN_URL, params=params, timeout=timeout, verify=False)
        r.raise_for_status()
        feats = r.json().get("features") or []
    except Exception:
        return None, None
    if not feats:
        return None, None
    a = feats[0].get("attributes") or {}
    return _clean(a.get("pq_authorised_quantity_120")), _clean(a.get("quantity_delta_120"))


def units_for_plan(pl_number, total_units):
    """XPLAN's נכנס/תוספת for a plan, gated on reconciling with Table 5.

    Returns (values, reason) where values is {'units_in', 'units_add'} or None
    and reason is a short human-readable string for the log/email either way.
    """
    if not total_units:
        return None, "אין סה\"כ יח\"ד מטבלה 5 — אין מול מה להצליב"
    try:
        total = float(total_units)
    except (TypeError, ValueError):
        return None, "סה\"כ יח\"ד לא מספרי"

    pq, delta = fetch_raw(pl_number)
    if pq is None or delta is None:
        return None, f"XPLAN בלי נתוני יח\"ד תקינים ל-{pl_number}"

    tol = max(1.0, 0.05 * total)
    if abs((pq + delta) - total) > tol:
        return None, (f"XPLAN לא מסתדר עם טבלה 5 ({pq:g}+{delta:g}≠{total:g}) — לא נכתב")
    return {"units_in": pq, "units_add": delta}, f"XPLAN pq={pq:g} delta={delta:g}"


if __name__ == "__main__":                       # py xplan_units.py 101-1430339 39
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    pn = sys.argv[1] if len(sys.argv) > 1 else "101-1430339"
    tot = float(sys.argv[2]) if len(sys.argv) > 2 else None
    print("raw   :", fetch_raw(pn))
    print("gated :", units_for_plan(pn, tot))
