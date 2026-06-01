"""Validate the stat-area -> sub-neighborhood -> minhak assignment that
computePopulationByGeography does at runtime, against the real data, using shapely.
Reports coverage (how many areas/population map to a sub and minhak) and a per-minhak
population summary so we can sanity-check the dashboard before it ships."""
import json
from shapely.geometry import shape, Point

DATA = r"C:\ORANIM\oranim-app\data"
stat = json.load(open(DATA + r"\stat_areas.geojson", encoding="utf-8"))
subs = json.load(open(DATA + r"\sub_neighborhoods.geojson", encoding="utf-8"))

SUB_NORMALIZE = {
    'קטמונים ח-ט': 'קטמונים', 'רסקו - גבעת הורדים': 'רסקו', 'גבעת הורדים': 'רסקו',
    'גבעת חורדים - רסקו': 'רסקו', 'גבעת הורדים - רסקו': 'רסקו', 'גבעת הורדיפ רסקו': 'רסקו',
    'גוננים א-ו': 'גוננים', 'ארנונה': 'תלפיות ארנונה', 'תלפיות': 'תלפיות ארנונה',
    'תלפיות - תעשייה ומסחר': 'א.ת. תלפיות', 'תלפיות תעשייה ומסחר': 'א.ת. תלפיות',
    'תלפיות תעשיה ומסחר': 'א.ת. תלפיות', 'עמק רפאים': 'המושבה הגרמנית',
    'עמק רפאים - המושבה הגרמנית': 'המושבה הגרמנית', 'עמק רפאים- המושבה הגרמנית': 'המושבה הגרמנית',
    'עמק רפאים -המושבה הגרמנית': 'המושבה הגרמנית', 'עמק רפאים-המושבה הגרמנית': 'המושבה הגרמנית',
    'קוממיות - טלביה': 'טלביה', 'קריית שמואל': 'קרית שמואל', 'בית צפאפא,שרפת': 'בית צפאפא',
    'בית צפאפא, שרפת': 'בית צפאפא', 'שרפת': 'בית צפאפא',
}
MINAHAK_SUBS = {
    'א.ת. תלפיות': ['א.ת. תלפיות', 'איתרי', 'תלפיות - תעשייה ומסחר'],
    'בית צפאפא': ['בית צפאפא', 'בית צפאפא,שרפת', 'טנטור', 'גבעת המטוס - בעלות ערבית'],
    'בקעה רבתי': ['תלפיות ארנונה', 'בקעה', 'גבעת חנניה - אבו תור', 'מקור חיים', 'מתחם הרכבת', 'צפון תלפיות', 'שיכוני תלפיות'],
    'גבעת המטוס': ['גבעת המטוס'],
    'גוננים': ['גוננים א-ו', 'פת', 'קטמונים', 'רסקו'],
    'גינות העיר': ['המושבה הגרמנית', 'המושבה היוונית', 'ניות', 'טלביה', 'קטמון הישנה', 'קרית שמואל', 'רחביה'],
    'מינהל מוסדי מלחה': ['מרכז ספורט מנחת - מלחה', 'גבעת השקד'],
}
SUB_TO_MINAHAK = {}
for mk, subnames in MINAHAK_SUBS.items():
    for s in subnames:
        SUB_TO_MINAHAK[SUB_NORMALIZE.get(s, s)] = mk

sub_polys = [(SUB_NORMALIZE.get(f["properties"].get("schn_nama"), f["properties"].get("schn_nama")),
             shape(f["geometry"])) for f in subs["features"]]

# findMinhak fallback: centroid inside a minahak_*.geojson polygon
MINAHAK_HEB_TO_LAYER = {
    'בית צפאפא': 'minahak_beit_tzfafa', 'גוננים': 'minahak_gonen', 'בקעה רבתי': 'minahak_baka',
    'גינות העיר': 'minahak_ganot', 'מינהל מוסדי מלחה': 'minahak_malha', 'א.ת. תלפיות': 'minahak_talpiot',
}
minhak_polys = []
for heb, key in MINAHAK_HEB_TO_LAYER.items():
    try:
        mj = json.load(open(DATA + "\\" + key + ".geojson", encoding="utf-8"))
        for f in mj["features"]:
            if f.get("geometry"):
                minhak_polys.append((heb, shape(f["geometry"])))
    except FileNotFoundError:
        print("missing", key)


def find_minhak(pt):
    for heb, poly in minhak_polys:
        if poly.contains(pt):
            return heb
    return None

from collections import defaultdict
by_minhak = defaultdict(lambda: defaultdict(lambda: {"pop": 0, "n": 0}))
unassigned = 0; unassigned_pop = 0; total_pop = 0; no_sub = 0

for f in stat["features"]:
    p = f["properties"]
    pop = p.get("pop_approx")
    if not isinstance(pop, (int, float)) or pop <= 0:
        continue
    total_pop += pop
    c = shape(f["geometry"]).centroid  # mirror geomCentroid in app.jsx
    sub = None
    for name, poly in sub_polys:
        if poly.contains(c):
            sub = name; break
    if sub is None:
        no_sub += 1
    minhak = (SUB_TO_MINAHAK.get(sub) if sub else None) or find_minhak(c)
    if not minhak:
        unassigned += 1; unassigned_pop += pop; continue
    d = by_minhak[minhak][sub or "לא ידוע"]
    d["pop"] += pop; d["n"] += 1

print(f"total residential pop: {total_pop:,.0f}")
print(f"areas with no sub match: {no_sub}")
print(f"unassigned (no minhak): {unassigned} areas, {unassigned_pop:,.0f} pop "
      f"({unassigned_pop/total_pop*100:.1f}%)")
print("\nper-minhak:")
for mk in sorted(by_minhak, key=lambda m: -sum(s['pop'] for s in by_minhak[m].values())):
    subs_d = by_minhak[mk]
    mpop = sum(s["pop"] for s in subs_d.values())
    print(f"  {mk}: {mpop:,.0f} pop, {len(subs_d)} subs")
    for sn in sorted(subs_d, key=lambda s: -subs_d[s]['pop']):
        print(f"      {sn}: {subs_d[sn]['pop']:,.0f} ({subs_d[sn]['n']} areas)")
