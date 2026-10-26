"""
update_meetings_layer.py

ממזג את 4 קבצי הישיבות לפורמט אחיד שהאפליקציה צורכת
ומעדכן את meetings.json ב-oranim-app/data (הריפו האמיתי — push דרכו לגיטהאב).

מקורות:
  - meeting_matches.json (מחוזית, פורמט שטוח של array)
  - jerusalem_taba_matches.json (תב"ע מקומית, אובייקט עם matches[])
  - jerusalem_committee_matches.json (היתרים מקומית)
  - jerusalem_shimur_matches.json (שימור מקומית)

פלט: מערך יחיד בפורמט של "מחוזית" עבור כל הישיבות.
"""
import json
import os
import re
from datetime import datetime


def normalize_plan_id(pid):
    """ממיר plan_id מקומי (כמו "1261510") לפורמט המלא של geojson ("101-1261510").
    משאיר ערכים שכבר במצב המלא או בפורמט מיוחד (כמו "813245.001") ללא שינוי."""
    if not pid:
        return pid
    pid = str(pid).strip()
    if not pid or pid.startswith("101-"):
        return pid
    if "." in pid or "/" in pid:
        return pid  # פורמט שונה (תת"ל וכו'), לא לגעת
    digits = re.sub(r'\D', '', pid)
    if not digits:
        return pid
    if len(digits) <= 7:
        return f"101-{digits.zfill(7)}"
    return pid

ROOT = r"C:\ORANIM"
# ועדת הרישוי כלל-עירונית — משאירים רק תיקים שה-geocoder פתר להם מינה"ק בתחום אורנים.
IN_AREA_MINHAK = {"גינות העיר", "גוננים", "בקעה רבתי",
                  "מינהל מוסדי מלחה", "א.ת. תלפיות", "בית צפאפא"}
SOURCES = {
    "mavat":     os.path.join(ROOT, "meeting_matches.json"),
    "taba":      os.path.join(ROOT, "jerusalem_taba_matches.json"),
    "committee": os.path.join(ROOT, "jerusalem_committee_matches.json"),
    "shimur":    os.path.join(ROOT, "jerusalem_shimur_matches.json"),
}
TARGETS = [
    os.path.join(ROOT, "oranim-app", "data", "meetings.json"),
]


def load_json(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"  WARN: failed to load {path}: {e}")
        return None


def flatten_local_committee(data, committee_label, geo_cache=None):
    """ממיר {meeting_date, matches: [...]} לרשימה שטוחה בפורמט מחוזית.

    עבור ועדת ההיתרים (geo_cache מסופק): כל היתר מועשר ב-lat/lng (אם עלה בגיאוקוד),
    ובמינה"ק האמיתי (point-in-polygon) במקום תת-השכונה של ועדת הרישוי. תת-השכונה
    המקורית נשמרת ב-sub_neighborhood.
    """
    out = []
    if not data:
        return out
    meeting_date = data.get("meeting_date", "")
    parent_time = data.get("meeting_time", "")
    for m in data.get("matches", []):
        # אזור-בלבד לוועדת הרישוי (geo_cache מסופק): מדלגים על תיקים שלא נפתר להם
        # מינה"ק בתחום אורנים (מחוץ-לתחום או כתובת שלא גוקודדה).
        if geo_cache is not None:
            geo_m = geo_cache.get(m.get("permit_no", "")) or {}
            if geo_m.get("minahak") not in IN_AREA_MINHAK:
                continue
        raw_id = m.get("plan_id") or m.get("permit_no", "")
        plan_id = normalize_plan_id(raw_id)
        plan_name = m.get("essence") or m.get("plan_name", "")
        purpose = m.get("discussion_type", "")
        if not purpose:
            purpose = "דיון" if committee_label != "מקומית - שימור" else ""
        neighborhood = m.get("neighborhood", "")
        row = {
            "meeting_date": meeting_date,
            "meeting_time": m.get("time") or parent_time,
            "plan_id":      str(plan_id),
            "plan_name":    plan_name,
            "purpose":      purpose,
            "minahak":      neighborhood,
            "committee":    committee_label,
        }
        geo = (geo_cache or {}).get(m.get("permit_no", ""))
        if geo:
            row["minahak"] = geo.get("minahak") or neighborhood
            row["sub_neighborhood"] = geo.get("sub_neighborhood") or neighborhood
            if geo.get("lat") is not None and geo.get("lng") is not None:
                row["lat"] = geo["lat"]
                row["lng"] = geo["lng"]
        out.append(row)
    return out


def main():
    print(f"=== update_meetings_layer.py @ {datetime.now().isoformat()} ===")
    merged = []

    # 1. מחוזית — כבר בפורמט הנכון
    mavat = load_json(SOURCES["mavat"])
    if isinstance(mavat, list):
        merged.extend(mavat)
        print(f"  מחוזית: {len(mavat)} entries")
    else:
        print("  מחוזית: missing or invalid")

    # cache of geocoded committee permits (lat/lng + real מינה"ק). See
    # geocode_committee_permits.py — run it before this script to refresh.
    geo_cache = load_json(os.path.join(ROOT, "committee_geocode_cache.json")) or {}
    if geo_cache:
        print(f"  geocode cache: {len(geo_cache)} committee permits")

    # 2-4. ועדות מקומיות
    label_map = {
        "taba":      "מקומית - תב\"ע",
        "committee": "מקומית - היתרים",
        "shimur":    "מקומית - שימור",
    }
    for key, label in label_map.items():
        data = load_json(SOURCES[key])
        flat = flatten_local_committee(data, label, geo_cache if key == "committee" else None)
        merged.extend(flat)
        print(f"  {label}: {len(flat)} entries")

    # מיון לפי תאריך (DD/MM/YYYY → datetime)
    def parse_date(d):
        try:
            return datetime.strptime(d, "%d/%m/%Y")
        except Exception:
            return datetime.max

    # סינון defensive: זרוק ישיבות עם תאריך שעבר
    today = datetime.now().date()
    before = len(merged)
    merged = [m for m in merged if parse_date(m.get("meeting_date", "")).date() >= today]
    dropped = before - len(merged)
    if dropped:
        print(f"  Filtered out {dropped} past-date meetings")

    merged.sort(key=lambda x: (parse_date(x.get("meeting_date", "")), x.get("meeting_time", "")))

    # כתיבה לכל היעדים
    for target in TARGETS:
        try:
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with open(target, "w", encoding="utf-8") as f:
                json.dump(merged, f, ensure_ascii=False, indent=2)
            print(f"  Wrote {target}: {len(merged)} entries")
        except Exception as e:
            print(f"  ERROR writing {target}: {e}")

    print("=== Done ===")


if __name__ == "__main__":
    main()
