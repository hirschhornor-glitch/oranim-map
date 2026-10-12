"""
tama38_master_plan_check.py
---------------------------
Cross-reference each תמ"א 38 building against the master plans (תכניות אב):
  • FLOORS: the building's collected floors (floors_tama) vs the master-plan cap at its
    location (subzone / texture / building-height polygon, else the coarse boundary
    number). Over the cap on a PRECISE polygon = הפרה; over a coarse boundary = אזהרה.
  • שימור: is the building on/adjacent to a conservation-designated building, or inside a
    conservation texture — a preservation sensitivity for a demolition-and-rebuild תמ"א.

Reuses MASTER_PLANS / load_master_plan / grade_for_point from compute_master_plan_compliance.
Master-plan matching for a POINT uses contains() (not area overlap).

Output: oranim-app/data/tama38_master_plan_check.json  (keyed by fid) + printed summary.
"""
import io, json, os, re, sys
from shapely.geometry import shape, Point

import compute_master_plan_compliance as C

DATA = C.DATA
GEO = os.path.join(DATA, "tama38.geojson")
MIVNEI = os.path.join(DATA, "arcgis", "mivnei_lashimur.geojson")
OUT = os.path.join(DATA, "tama38_master_plan_check.json")


def num(v):
    try:
        return float(v)
    except Exception:
        return None


def _norm(s):
    s = str(s or "").replace('"', "").replace("'", "").replace("״", "").replace("׳", "")
    return re.sub(r"\s+", " ", s).strip()


def _parse_addr(a):
    """'הל"ה 34' → ('הלה','34'). Returns (street, house|None)."""
    a = _norm(a)
    m = re.match(r"^(.+?)\s+(\d+)[א-ת]?$", a)
    return (_norm(m.group(1)), m.group(2)) if m else (a, None)


def load_conservation_index():
    """{(street, house): grade} from the consolidated conservation buildings layer —
    a BUILDING-LEVEL match (the building itself is on the approved conservation list),
    far more precise than proximity in dense historic areas."""
    idx = {}
    try:
        ml = json.load(io.open(MIVNEI, encoding="utf-8"))
    except Exception:
        return idx
    for f in ml.get("features", []):
        p = f.get("properties", {})
        st, hn = _norm(p.get("mp_street")), _norm(p.get("mp_house_num"))
        if st and hn:
            grade = C.grade_for_point(p) or (p.get("grade") or p.get("status") or "שימור")
            idx.setdefault((st, hn), grade)
    return idx


def zone_cap(mp, pt):
    """Master-plan floor cap for a POINT: first containing subzone→texture→bldg_height,
    else the boundary number. Returns (cap, source, label)."""
    for key, src in (("subzones", "subzone"), ("textures", "texture"), ("bldg_heights", "bldg_height")):
        for rec in mp.get(key, []):
            try:
                if not rec["g"].contains(pt):
                    continue
            except Exception:
                continue
            p = rec["p"]
            cap = num(p.get("max_floors") or p.get("floors_max") or p.get("floors_max_int"))
            label = (p.get("zone_name") or p.get("character") or p.get("אזור")
                     or p.get("merkam") or p.get("floors_per_master_plan") or "")
            if cap is not None or key != "bldg_heights":
                return cap, src, str(label)[:40]
    bp = mp.get("boundary")
    if bp:
        cap = num(bp["p"].get("max_floors") or bp["p"].get("max_floors_base"))
        if cap is not None:
            return cap, "boundary", "כללי תכנית האב"
    return None, None, ""


def conservation_hit(mp, pt, address, cons_idx):
    """BUILDING-LEVEL conservation: the building's own street+house is on the approved
    conservation list → ('מבנה לשימור', grade). Else, if it sits inside a conservation
    texture zone → ('באזור שימור', label) as softer context. Else (None, None).
    Proximity buffers were dropped — conservation is dense in historic areas and any
    buffer catches neighbours, inflating the count (178 → 33 with address matching)."""
    st, hn = _parse_addr(address)
    if hn and (st, hn) in cons_idx:
        return (cons_idx[(st, hn)], "מבנה לשימור")
    for rec in mp.get("textures", []):
        try:
            if rec["g"].contains(pt):
                merkam = str(rec["p"].get("merkam") or rec["p"].get("אזור") or "")
                if any(k in merkam for k in ("שימור", "היסטורי", "מרקם")):
                    return (merkam[:24], "באזור שימור")
        except Exception:
            continue
    return None, None


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    print("Loading master plans...")
    mps = {}
    for name, fn in C.MASTER_PLANS.items():
        path = os.path.join(DATA, fn)
        if os.path.exists(path):
            mps[name] = C.load_master_plan(path)
            m = mps[name]
            print(f"  {name}: subzones={len(m['subzones'])} textures={len(m['textures'])} "
                  f"heights={len(m['bldg_heights'])} cons_points={len(m['cons_points'])}")

    cons_idx = load_conservation_index()
    print(f"  conservation buildings indexed by address: {len(cons_idx)}")

    geo = json.load(io.open(GEO, encoding="utf-8"))
    out = {}
    n_in_mp = n_floor_check = viol = warn = ok = cons_bldg = cons_area = unverified = 0
    viol_ex, cons_ex = [], []

    for f in geo["features"]:
        p = f["properties"]
        g = f.get("geometry")
        if not g:
            continue
        try:
            geom = shape(g)
            pt = geom if geom.geom_type == "Point" else geom.representative_point()
        except Exception:
            continue
        # which master plan contains this building?
        mp_name = None
        for name, mp in mps.items():
            b = mp.get("boundary")
            try:
                if b and b["g"].contains(pt):
                    mp_name = name
                    break
            except Exception:
                continue
        if not mp_name:
            continue
        n_in_mp += 1
        mp = mps[mp_name]
        cap, src, label = zone_cap(mp, pt)
        cons_grade, cons_kind = conservation_hit(mp, pt, p.get("address"), cons_idx)
        fl = num(p.get("floors_tama"))
        # floors_unverified (from refetch_tama38_floors.py): YK's "מספר קומות" row left the
        # קיים column blank on a building that already has built area, so the number is a
        # LOWER BOUND on the after-works height, not the total. It can still PROVE a
        # violation (total ≥ reported > cap) but can never prove compliance.
        fl_unv = bool(p.get("floors_unverified"))
        fl_src = p.get("floors_src") or ""
        rec = {"address": p.get("address"), "tik": p.get("tik"), "master_plan": mp_name,
               "floors_tama": fl, "floors_unverified": fl_unv, "floors_src": fl_src,
               "cap": cap, "cap_source": src,
               "zone": label, "conservation": cons_grade, "conservation_kind": cons_kind,
               "flag": None}
        if cons_kind == "מבנה לשימור":
            cons_bldg += 1
            if len(cons_ex) < 8:
                cons_ex.append((p.get("address", "")[:20], cons_grade))
        elif cons_kind == "באזור שימור":
            cons_area += 1
        if fl is not None and cap is not None:
            n_floor_check += 1
            if fl > cap:
                # A floors count parsed out of free text can be wrong in EITHER direction
                # (it often reports the EXISTING floors, or the addition) — never call that
                # a violation. A table lower bound over the cap still is one.
                soft = fl_src in ("text", "unread")
                rec["flag"] = ("אזהרה" if soft else
                               ("הפרה" if src in ("subzone", "texture", "bldg_height") else "אזהרה"))
                if rec["flag"] == "הפרה":
                    viol += 1
                    if len(viol_ex) < 8:
                        viol_ex.append((p.get("address", "")[:20], f"{fl}>{cap}", src, mp_name))
                else:
                    warn += 1
            elif fl_unv:
                unverified += 1          # ≤cap on an unverified number proves nothing
            else:
                ok += 1
        out[str(p.get("fid"))] = rec

    json.dump(out, io.open(OUT, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"\n=== תמ\"א 38 מול תכניות אב ===")
    print(f"  buildings inside a master plan: {n_in_mp}")
    print(f"  floor check (have both floors+cap): {n_floor_check}  →  הפרה={viol}  אזהרה={warn}  תקין={ok}  לא-ניתן-לאמת={unverified}")
    print(f"  מבנה לשימור (התאמת-כתובת): {cons_bldg}   |   באזור שימור (מרקם): {cons_area}")
    print(f"\n  דוגמאות חריגת-קומות (הפרה):")
    for e in viol_ex:
        print(f"    {e[0]:20} {e[1]:8} [{e[2]}] {e[3]}")
    print(f"\n  דוגמאות מבנה-לשימור (התאמת-כתובת):")
    for e in cons_ex:
        print(f"    {e[0]:20} דרגה/סטטוס: {e[1]}")
    print(f"\n  -> {OUT}")


if __name__ == "__main__":
    main()
