"""
add_plan_manual.py — הוספה ידנית של תכנית בודדת (בדרך כלל כזו שנמצאת מחוץ לגבול
רובע 4 ולכן detect_new_plans.py לעולם לא יאתר אותה) למאגר: שורה ב-Google Sheets,
פיצ'ר ב-plans.geojson, מגרשי ייעוד-קרקע ב-landuse_xplan.geojson, ותור העשרה.

שימוש:
    py add_plan_manual.py 101-1214279 --minahak "גינות העיר" --sub "רחביה"
    py add_plan_manual.py 101-1214279 --dry-run
    py add_plan_manual.py 101-1214279 --no-mavat      (בלי דפדפן — רק XPLAN)

כל הכתיבות עוברות דרך הפונקציות של detect_new_plans.py כדי שהשורה/הפיצ'ר ייראו
בדיוק כמו של תכנית שזוהתה אוטומטית (כולל שומרי-הכותרת של הגיליון).
"""
import argparse, asyncio, json, os, sys

sys.path.insert(0, r"C:\ORANIM")
import detect_new_plans as d

LANDUSE_APP = r"C:\ORANIM\oranim-app\data\landuse_xplan.geojson"
XPLAN_LANDUSE_URL = ("https://ags.iplan.gov.il/arcgisiplan/rest/services/"
                     "PlanningPublic/Xplan/MapServer/4/query")


def fetch_plan_features(pl_number):
    """הקו הכחול של התכנית (MapServer/1) כ-GeoJSON WGS84 — אותו פורמט
    ש-fetch_xplan_plans מחזיר, כדי ש-extract/create_plan_geometry יעבדו."""
    params = {
        'where': f"pl_number='{pl_number}'",
        'outFields': 'pl_number,mp_id,pl_name,station_desc,last_update_date,shape_area',
        'returnGeometry': 'true', 'f': 'geojson', 'outSR': '4326',
    }
    r = d._SESSION.get(d.XPLAN_URL, params=params, timeout=60, verify=False)
    r.raise_for_status()
    return r.json().get('features', [])


def build_info(pl_number):
    feats = fetch_plan_features(pl_number)
    if not feats:
        raise SystemExit(f"XPLAN לא מכיר את {pl_number}")
    info = {'pl_number': pl_number, 'mp_ids': [], 'features': feats,
            'mavat_names': [], 'total_area': 0}
    for f in feats:
        p = f.get('properties', {})
        mp = p.get('mp_id')
        if mp:
            s = str(int(mp)) if float(mp) == int(mp) else str(mp)
            if s not in info['mp_ids']:
                info['mp_ids'].append(s)
        info['total_area'] += p.get('shape_area') or 0
        if p.get('pl_name') and not info.get('xplan_name'):
            info['xplan_name'] = p['pl_name']
        if p.get('station_desc') and not info.get('xplan_status'):
            info['xplan_status'] = p['station_desc']
    names, shavaz, shatzap = d.fetch_landuse(pl_number)
    if names:
        info['mavat_names'] = names
    if shavaz is not None:
        info['landuse_shavatz_out'] = shavaz
    if shatzap is not None:
        info['landuse_shatzap_out'] = shatzap
    return info


def add_landuse_parcels(pl_number):
    """מוסיף את מגרשי ייעוד-הקרקע של התכנית ל-landuse_xplan.geojson.
    (refresh_landuse_xplan.py בונה את הקובץ מחדש עם מסנן-גבול, ולכן תכנית
    מחוץ לגבול חייבת גם להיכנס ל-INCLUDE_PLAN_NUMBERS שם כדי לשרוד ריענון.)"""
    params = {'where': f"pl_number='{pl_number}'", 'outFields': '*',
              'returnGeometry': 'true', 'f': 'geojson', 'outSR': '4326',
              'resultRecordCount': 1000}
    r = d._SESSION.get(XPLAN_LANDUSE_URL, params=params, timeout=60, verify=False)
    r.raise_for_status()
    feats = r.json().get('features', [])
    if not feats:
        print("  [landuse] אין מגרשי ייעוד ב-MapServer/4 לתכנית זו")
        return 0
    with open(LANDUSE_APP, encoding='utf-8') as f:
        gj = json.load(f)
    have = {ft['properties'].get('objectid') for ft in gj['features']
            if ft['properties'].get('pl_number') == pl_number}
    added = 0
    for ft in feats:
        if ft.get('properties', {}).get('objectid') in have:
            continue
        gj['features'].append(ft)
        added += 1
    if added:
        with open(LANDUSE_APP, 'w', encoding='utf-8') as f:
            json.dump(gj, f, ensure_ascii=False)
    print(f"  [landuse] נוספו {added} מגרשים (מתוך {len(feats)})")
    return added


async def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser()
    ap.add_argument('pl_number')
    ap.add_argument('--minahak', default='')
    ap.add_argument('--sub', default='')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--no-mavat', action='store_true')
    ap.add_argument('--no-landuse', action='store_true')
    a = ap.parse_args()

    pl = a.pl_number.strip()
    norm = d.normalize_plan_number(pl)
    print(f"=== הוספה ידנית: {pl} (taba={norm}) ===")

    existing = d.load_existing_plan_numbers()
    if norm in existing:
        print(f"התכנית {norm} כבר קיימת (גיליון או geojson) — יוצא.")
        return

    info = build_info(pl)
    print(f"  שם XPLAN: {info.get('xplan_name')}")
    print(f"  סטטוס XPLAN: {info.get('xplan_status')}")
    print(f"  AGAM: {info['mp_ids']}")
    print(f"  ייעודים: {', '.join(info['mavat_names'])}")
    print(f"  שב\"צ יוצא (ייעודי קרקע): {info.get('landuse_shavatz_out')}")

    auto = d.admin_for(info)
    minahak = a.minahak or auto[0]
    sub = a.sub or auto[1]
    info['_admin'] = (minahak, sub)
    print(f"  מינה\"ק/תת-שכונה: {minahak} / {sub}  (אוטומטי: {auto})")

    if a.dry_run:
        print("\n--- DRY-RUN: לא בוצע שינוי ---")
        return

    if not a.no_mavat:
        await d.enrich_from_mavat({norm: info})
        info['_admin'] = (minahak, sub)   # enrich לא נוגע, אבל ליתר ביטחון

    new_plans = {norm: info}
    sheets_added = d.update_sheets(new_plans)
    push = bool(os.environ.get('GITHUB_TOKEN'))
    geojson_added = d.update_geojson(new_plans, push_to_github=push)
    print(f"  גיליון: +{sheets_added} | plans.geojson: +{geojson_added}")

    if not a.no_landuse:
        add_landuse_parcels(pl)

    try:
        import enrich_queue as _eq
        _eq.enqueue([{ "taba": _eq.norm_taba(pl),
                       "agam_id": info['mp_ids'][0] if info['mp_ids'] else None,
                       "plan_name": pl, "reason": "manual-add",
                       "queued_at": d.get_israel_time().strftime("%Y-%m-%d %H:%M:%S")}])
        print("  [enrich-queue] נכנסה לתור ההעשרה העמוקה")
    except Exception as e:
        print(f"  [enrich-queue] לא נכנס לתור: {e}")


if __name__ == '__main__':
    asyncio.run(main())
