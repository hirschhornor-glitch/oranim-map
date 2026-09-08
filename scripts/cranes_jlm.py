# -*- coding: utf-8 -*-
"""
cranes_jlm.py - אתרי בנייה פעילים (מנהל הבטיחות, gov.il) -> רובע אורנים.

1. מושך את קולקטור "אתרי בנייה פעילים" (gov.il) עבור ירושלים.
2. ממקם כל אתר לפי שם הרחוב + מספר בית מול roads.geojson של הרובע.
3. מצליב מול permits_master.json / pikuah_status.json ומדווח פערים.

פלטים:
  active_building_sites_jlm_raw.json   - כל האתרים הפעילים בירושלים (גולמי)
  active_sites_oranim.geojson          - האתרים שמוקמו בתוך הרובע (שכבה מוצעת)
  active_sites_oranim_report.json      - הצלבה מול היתרים/פיקוח + פערים
"""
import json, os, re, time, http.cookiejar, urllib.request, collections, math
from shapely.geometry import shape
from shapely.ops import unary_union

DATA = r"C:/dev/oranim-app/data/"
RAW = r"C:/ORANIM/active_building_sites_jlm_raw.json"
GEO = r"C:/ORANIM/active_sites_oranim.geojson"
REP = r"C:/ORANIM/active_sites_oranim_report.json"
APP = r"C:/dev/oranim-app/data/active_sites.json"

URL = "https://www.gov.il/he/api/DataGovProxy/GetDGResults"
TPL = "dd270778-2117-4b1c-9385-e06146bfc08c"
XCLIENT = "149a5bad-edde-49a6-9fb9-188bd17d4788"
PAGE = "https://www.gov.il/he/departments/dynamiccollectors/active-building-sites"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/128.0 Safari/537.36")


# ---------------------------------------------------------------- fetch
def fetch(city="ירושלים", out=RAW):
    cj = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    op.open(urllib.request.Request(PAGE, headers={"User-Agent": UA}), timeout=60).read()
    hdrs = {"Content-Type": "application/json", "Accept": "application/json, text/plain, */*",
            "x-client-id": XCLIENT, "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://www.gov.il", "Referer": PAGE, "User-Agent": UA}

    def page(skip):
        qf = {"skip": {"Query": skip}}
        if city:
            qf["city_name"] = {"Query": city}
        body = json.dumps({"DynamicTemplateID": TPL, "QueryFilters": qf, "From": skip}).encode()
        for a in range(8):
            try:
                raw = op.open(urllib.request.Request(URL, data=body, headers=hdrs),
                              timeout=90).read().decode("utf-8")
                if raw.lstrip().startswith("<"):
                    raise RuntimeError("WAF html")
                d = json.loads(raw)
                if not (d.get("Results") or []):
                    raise RuntimeError("empty page (soft block)")
                return d
            except Exception as e:
                print("  retry %d @%d (%s)" % (a + 1, skip, e), flush=True)
                time.sleep(min(60, 5 * (a + 1)))
        raise SystemExit("failed @%d" % skip)

    rows = json.load(open(out, encoding="utf-8")) if os.path.exists(out) else []
    seen = {r["work_id"] for r in rows}
    skip = len(rows)
    while True:
        d = page(skip)
        tot = d.get("TotalResults") or 0
        for x in d["Results"]:
            if x["Data"]["work_id"] not in seen:
                seen.add(x["Data"]["work_id"])
                rows.append(x["Data"])
        skip += len(d["Results"])
        print("%d/%d" % (skip, tot), flush=True)
        if skip >= tot:
            break
        time.sleep(1.5)
    json.dump(rows, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    return rows


# ---------------------------------------------------------------- geocode
def norm(s):
    s = (s or "").replace('"', " ").replace("'", " ").replace("\u05f4", " ").replace("\u05f3", " ")
    s = re.sub(r"[\u05be\u2013\u2014-]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


OUT_NEIGH = ["גבעת מרדכי", "נחלאות", "פסגת זאב", "רמות", "גילה", "הר חומה", "בית חנינא",
             "שועפט", "רמת שלמה", "סנהדריה", "רמת אשכול", "קרית יובל", "עיר גנים",
             "גבעת המטוס", "הר הצופים", "עין כרם", "גבעת משואה", "רוממה", "מרכז העיר",
             "גאולה", "הר חוצבים", "עטרות", "נווה יעקב", "קרית מנחם", "בית הכרם",
             "גבעת שאול", "תלפיות מזרח", "ארמון הנציב", "צור באהר", "אום טובא",
             "סילוואן", "עיסאוויה", "מוסררה", "בית וגן", "עיר העתיקה", "מאה שערים"]
IN_NEIGH = ["ארנונה", "בקעה", "תלפיות", "קטמון", "קטמונים", "גונן", "מקור חיים", "רסקו",
            "רחביה", "עמק רפאים", "המושבה הגרמנית", "המושבה היוונית", "טלביה",
            "קרית שמואל", "מנחת", "בית צפפא", "גבעת אורנים", "מלחה", "שרפאת",
            "עמק הצבאים", "אבו תור", "גבעת חנניה", "ניות", "איתרי", "בקעה"]

# alias בשם אתר -> שם תת-שכונה ב-sub_neighborhoods.geojson (fallback כשאין רחוב)
NEIGH_ALIAS = {
    "מורדות ארנונה": "ארנונה", "ארנונה": "ארנונה", "בקעה": "בקעה",
    "קטמון הישנה": "קטמון הישנה", "קטמונים": "קטמונים", "גוננים": "גוננים",
    "גונן": "גוננים", "קטמון": "קטמון הישנה", "רחביה": "רחביה", "טלביה": "קוממיות - טלביה",
    "מקור חיים": "מקור חיים", "רסקו": "רסקו - גבעת הורדים", "מלחה": "מרכז ספורט מנחת - מלחה",
    "מנחת": "מרכז ספורט מנחת - מלחה", "עמק הצבאים": "פת", "פת": "פת",
    "המושבה הגרמנית": "עמק רפאים - המושבה הגרמנית", "עמק רפאים": "עמק רפאים - המושבה הגרמנית",
    "המושבה היוונית": "המושבה היוונית", "תלפיות": "תלפיות", "צפון תלפיות": "צפון תלפיות",
    "בית צפפא": "בית צפאפא,שרפת", "שרפאת": "בית צפאפא,שרפת", "אבו תור": "גבעת חנניה - אבו תור",
    "גבעת חנניה": "גבעת חנניה - אבו תור", "ניות": "ניות", "איתרי": "איתרי",
}


def build_gazetteer():
    roads = json.load(open(DATA + "roads.geojson", encoding="utf-8"))["features"]
    segs = collections.defaultdict(list)
    for f in roads:
        st = norm(f["properties"].get("street"))
        if not st or len(st) < 3:
            continue
        p, g = f["properties"], shape(f["geometry"])
        rng = []
        for a, b in (("fromleft", "toleft"), ("fromright", "toright")):
            lo, hi = p.get(a) or 0, p.get(b) or 0
            if lo or hi:
                rng.append((min(lo, hi), max(lo, hi)))
        segs[st].append((g, rng))
    return segs


def match_street(txt, segs):
    best = None
    for st in segs:
        if re.search(r"(?<![\u0590-\u05FF])%s(?![\u0590-\u05FF])" % re.escape(st), txt):
            if best is None or len(st) > len(best):
                best = st
    return best


def locate(st, num, segs):
    cands = segs[st]
    if num:
        for g, rng in cands:
            for lo, hi in rng:
                if lo <= num <= hi:
                    return g.interpolate(0.5, normalized=True), "house-number"
    return unary_union([g for g, _ in cands]).centroid, "street-centroid"


# ---------------------------------------------------------------- main
def main():
    sites = json.load(open(RAW, encoding="utf-8")) if os.path.exists(RAW) else fetch()
    district = shape(json.load(open(DATA + "district_oranim.geojson",
                                   encoding="utf-8"))["features"][0]["geometry"])
    segs = build_gazetteer()
    subn = {f["properties"]["schn_nama"]: shape(f["geometry"])
            for f in json.load(open(DATA + "sub_neighborhoods.geojson",
                                    encoding="utf-8"))["features"]}

    permits = json.load(open(DATA + "permits_master.json", encoding="utf-8"))["permits"]
    pikuah = json.load(open(DATA + "pikuah_status.json", encoding="utf-8"))["by_permit"]
    pts = [(p, p["lat"], p["lng"]) for p in permits.values() if p.get("lat") and p.get("lng")]

    feats, unlocated, rejected = [], [], []
    for s in sites:
        txt = norm(s["site_name"])
        st = match_street(txt, segs)
        out_hit = [n for n in OUT_NEIGH if n in txt]
        in_hit = [n for n in IN_NEIGH if n in txt]
        if not st:
            # fallback: \u05d0\u05d9\u05df \u05e8\u05d7\u05d5\u05d1 \u05de\u05d6\u05d5\u05d4\u05d4, \u05d0\u05d1\u05dc \u05e9\u05dd \u05d4\u05d0\u05ea\u05e8 \u05de\u05d6\u05db\u05d9\u05e8 \u05ea\u05ea-\u05e9\u05db\u05d5\u05e0\u05d4 \u05e9\u05dc \u05d4\u05e8\u05d5\u05d1\u05e2
            alias = None
            for a in sorted(NEIGH_ALIAS, key=len, reverse=True):
                if a in txt:
                    alias = a
                    break
            if alias and not out_hit and NEIGH_ALIAS[alias] in subn:
                pt, how, num = subn[NEIGH_ALIAS[alias]].centroid, "neighborhood-centroid", None
                st = NEIGH_ALIAS[alias]
            else:
                unlocated.append(s)
                continue
        else:
            m = re.search(re.escape(st) + r"\s*(?:[\u05d0-\u05ea]\s+)?(\d{1,3})(?![\d])", txt)
            num = int(m.group(1)) if m else None
            pt, how = locate(st, num, segs)
            if not (district.contains(pt) or district.buffer(0.0012).contains(pt)):
                continue
            if out_hit and not in_hit:
                rejected.append({"work_id": s["work_id"], "site_name": s["site_name"],
                                 "street": st, "neigh": out_hit})
                continue

        near = sorted(((math.dist((pt.y, pt.x), (la, lo)) * 111000, p) for p, la, lo in pts),
                      key=lambda t: t[0])[:1]
        dist, perm = (near[0][0], near[0][1]) if near else (None, None)
        pk = pikuah.get(perm["tik"]) if perm else None
        feats.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [round(pt.x, 6), round(pt.y, 6)]},
            "properties": {
                "work_id": s["work_id"], "site_name": s["site_name"],
                "executor_name": s["executor_name"], "executor_id": s["executor_id"],
                "foreman_name": s["foreman_name"],
                "has_cranes": int(s["has_cranes"]),
                "safety_warrants": int(s["safety_warrents"]),
                "sanctions": int(s["sanctions"]),
                "street": st, "house_number": num, "geocode": how,
                "neigh_hint": in_hit or out_hit,
                "nearest_permit": (perm or {}).get("tik"),
                "nearest_permit_m": round(dist) if dist is not None else None,
                "nearest_permit_status": (perm or {}).get("status"),
                "nearest_permit_desc": ((perm or {}).get("request_description") or "")[:120],
                "nearest_permit_units": (perm or {}).get("units_added"),
                "pikuah_class": (pk or {}).get("classification"),
                "pikuah_status": (pk or {}).get("pikuah_status"),
                "sub": next((n for n, g in subn.items() if g.contains(pt)), ""),
                "confidence": ("high" if how == "house-number" and (dist or 9999) <= 75
                               else "medium" if how != "neighborhood-centroid" and (dist or 9999) <= 150
                               else "low"),
            }})

    json.dump({"type": "FeatureCollection",
               "meta": {"source": "gov.il active-building-sites",
                        "fetched": time.strftime("%Y-%m-%d"), "city_total": len(sites)},
               "features": feats},
              open(GEO, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    # --- שכבת האפליקציה (data/active_sites.json) ---
    app_sites = []
    for f in feats:
        q = f["properties"]
        app_sites.append({
            "id": q["work_id"], "name": q["site_name"], "executor": q["executor_name"],
            "executor_id": q["executor_id"], "foreman": q["foreman_name"],
            "cranes": q["has_cranes"], "warrants": q["safety_warrants"],
            "sanctions": q["sanctions"], "street": q["street"], "num": q["house_number"],
            "sub": q["sub"], "conf": q["confidence"], "geocode": q["geocode"],
            "lnglat": f["geometry"]["coordinates"],
            "permit": q["nearest_permit"], "permit_m": q["nearest_permit_m"],
            "permit_status": q["nearest_permit_status"], "permit_desc": q["nearest_permit_desc"],
            "pikuah": q["pikuah_class"], "pikuah_status": q["pikuah_status"],
        })
    app_sites.sort(key=lambda r: (-r["cranes"], -(r["warrants"] + r["sanctions"]), r["name"]))
    json.dump({"meta": {"source": "מנהל הבטיחות בעבודה — אתרי בנייה פעילים (gov.il)",
                        "url": PAGE, "fetched": time.strftime("%Y-%m-%d"),
                        "city_total": len(sites), "in_district": len(app_sites)},
               "sites": app_sites},
              open(APP, "w", encoding="utf-8"), ensure_ascii=False)

    P = [f["properties"] for f in feats]
    close = [p for p in P if (p["nearest_permit_m"] or 9999) <= 150]
    rep = {
        "jerusalem_total": len(sites),
        "jerusalem_cranes": sum(int(s["has_cranes"]) for s in sites),
        "jerusalem_warrants": sum(int(s["safety_warrents"]) for s in sites),
        "jerusalem_sanctions": sum(int(s["sanctions"]) for s in sites),
        "no_street_match": len(unlocated),
        "rejected_by_neighborhood": len(rejected),
        "in_district": len(P),
        "in_district_cranes": sum(p["has_cranes"] for p in P),
        "in_district_warrants": sum(p["safety_warrants"] for p in P),
        "in_district_sanctions": sum(p["sanctions"] for p in P),
        "matched_permit_150m": len(close),
        "no_permit_150m": len(P) - len(close),
        "geocode_precision": dict(collections.Counter(p["geocode"] for p in P)),
        "confidence_mix": dict(collections.Counter(p["confidence"] for p in P)),
        "pikuah_mix": dict(collections.Counter(p["pikuah_class"] for p in close)),
        "sites_with_enforcement": [p for p in P if p["safety_warrants"] or p["sanctions"]],
        "active_but_pikuah_done": [p for p in close
                                   if p["pikuah_class"] in ("closed", "gmar", "built")],
        "sites_no_permit": [p for p in P if (p["nearest_permit_m"] or 9999) > 150],
        "unlocated_examples": [s["site_name"] for s in unlocated[:40]],
    }
    json.dump(rep, open(REP, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    for k, v in rep.items():
        print(k, len(v) if isinstance(v, list) else v)


if __name__ == "__main__":
    main()
