"""
add_149_to_tama38.py — append the enriched missing TAMA 38 permits (from the §149
audit) to tama38.geojson + tama38_permits.json (data copy AND root copy), the same
way 2023/0605.00 (Tchernichovsky 48) was added. Idempotent: skips any tik already
present. Marker point = building footprint (rec['lnglat']); rows without a geocode
are reported and skipped (need manual placement).

Run:  py -X utf8 add_149_to_tama38.py [--dry-run]
"""
import argparse, json, re, sys

ROOT = r"C:\ORANIM"
DATA = ROOT + r"\oranim-app\data"
ENRICHED = ROOT + r"\missing_tama38_enriched.json"
GEO = DATA + r"\tama38.geojson"
PERM_DATA = DATA + r"\tama38_permits.json"
PERM_ROOT = ROOT + r"\tama38_permits.json"
TODAY = "2026-07-08"


def norm(t):
    m = re.match(r'(\d{4})/(\d+)\.(\d+)', str(t).replace(' ', ''))
    return f"{m.group(1)}/{int(m.group(2))}.{int(m.group(3))}" if m else str(t)


def map_status(yk_status):
    s = yk_status or ""
    if "הופק" in s or "הוצא היתר" in s:
        return "הופק הוצא היתר"
    if "ועדת רישוי" in s or "אושר" in s:
        return "היתר אושר בועדת רישוי"
    return "נפתח תיק היתר"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")

    enriched = json.load(open(ENRICHED, encoding="utf-8"))
    geo = json.load(open(GEO, encoding="utf-8"))
    pdata = json.load(open(PERM_DATA, encoding="utf-8"))
    proot = json.load(open(PERM_ROOT, encoding="utf-8"))

    existing = {norm(f["properties"].get("tik")) for f in geo["features"] if f["properties"].get("tik")}
    max_fid = max(int(f["properties"].get("fid") or 0) for f in geo["features"])

    DEAD = ("נגנז", "בוטל", "נדחת", "סגירת", "ביטול")
    added, skipped_dup, skipped_nogeo, skipped_dead = 0, 0, 0, 0
    for tik, rec in enriched.items():
        if not rec.get("status"):
            continue                       # not enriched (throttled) — skip this run
        if any(w in rec.get("status", "") for w in DEAD):
            skipped_dead += 1
            print(f"  DEAD (skip): {tik}  {rec.get('status')}  {rec.get('address')}")
            continue
        if norm(tik) in existing:
            skipped_dup += 1
            continue
        if not rec.get("lnglat"):
            skipped_nogeo += 1
            print(f"  NO-GEO (skip): {tik}  {rec.get('address')}")
            continue
        max_fid += 1
        fid = max_fid
        rtype = rec.get("request_type", "")
        harisaa = "כן" if "הריסה" in rtype else "לא"
        lon, lat = rec["lnglat"]
        geo["features"].append({
            "type": "Feature",
            "geometry": {"type": "MultiPoint", "coordinates": [[lon, lat]]},
            "properties": {
                "fid": fid, "qc_id": fid, "tik": tik,
                "address": rec.get("address", ""),
                "neighborho": rec.get("schuna_yk") or rec.get("schn") or "",
                "rova": 0.0, "units_exis": 0.0, "units_tose": 0.0,
                "open_": "", "status": map_status(rec.get("status")),
                "date": None, "status_1": None,
                "comment": f"נוסף ידנית {TODAY} מביקורת §149 (audit_section149_gaps) — "
                           f"חסר במאגר/GIS העירייה; מיקום=מבנה לפי כתובת.",
                "harisaa": harisaa, "y": None, "x": None,
                "created_us": "audit149", "created_da": TODAY,
                "last_edite": "audit149", "last_edi_1": TODAY, "__rec_stat": None,
            }})
        entry = {
            "tik": tik, "address": rec.get("address", ""),
            "permits": [{
                "file_number": tik,
                "status": rec.get("status", ""),
                "status_date": rec.get("status_date", ""),
                "request_type": rtype,
                "request_description": rec.get("description", ""),
            }],
            "scraped_at": None, "permit_count": 1, "error": "",
        }
        pdata[str(fid)] = entry
        proot[str(fid)] = dict(entry)
        existing.add(norm(tik))
        added += 1

    print(f"\nadd: {added} | skip dup: {skipped_dup} | skip no-geo: {skipped_nogeo} | skip dead: {skipped_dead}")
    if args.dry_run:
        print("(dry-run — nothing written)")
        return
    json.dump(geo, open(GEO, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    json.dump(pdata, open(PERM_DATA, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    json.dump(proot, open(PERM_ROOT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"written. tama38 features now: {len(geo['features'])}")


if __name__ == "__main__":
    main()
