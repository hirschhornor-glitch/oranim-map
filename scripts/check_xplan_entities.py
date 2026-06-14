"""
check_xplan_entities.py — watchdog for the iplan XPLAN entity-layer migration.

Background (~2026-06): iplan emptied the XPLAN entity layers — land-use
(MapServer/4), easement polygons (MapServer/3) and tree points (MapServer/0)
all return 0 features, and the new `entities` service is still empty too. Only
the blue-line plan boundaries (MapServer/1) survived. Our easement/tree/land-use
scrapers were therefore guarded to abort on empty rather than overwrite the
committed static data (see fetch_easements_from_xplan.py, fetch_tree_points_from_xplan.py,
refresh_landuse_xplan.py, update_mavat_ui.py).

This script polls the candidate sources. While they are all still empty it exits
0 quietly. The moment ANY of them reports features again, it exits 1 with a loud
message — the scheduled workflow then surfaces that as a failure email, the
signal to revive (and possibly re-point) those scrapers.

Run locally:  python scripts/check_xplan_entities.py
"""
import ssl
import sys

import requests
from requests.adapters import HTTPAdapter

requests.packages.urllib3.disable_warnings()  # noqa


class _LegacySSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.set_ciphers('DEFAULT:@SECLEVEL=0')
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)


_SESSION = requests.Session()
_SESSION.mount('https://ags.iplan.gov.il', _LegacySSLAdapter())

BASE = "https://ags.iplan.gov.il/arcgisiplan/rest/services/PlanningPublic"

# (service, layer, human label) — the layers that hold the entity data we lost.
# All of these are currently 0; any non-zero count means the data is coming back.
WATCH = [
    ("Xplan",    0, "tree points (XPLAN/0)"),
    ("Xplan",    3, "easement polygons (XPLAN/3)"),
    ("Xplan",    4, "land-use polygons (XPLAN/4)"),
    ("entities", 1, "entities service / point layer"),
    ("entities", 2, "entities service / line layer"),
    ("entities", 3, "entities service / polygon layer"),
]


def layer_count(service, layer):
    """National feature count for a layer, or None on request error."""
    url = f"{BASE}/{service}/MapServer/{layer}/query"
    try:
        r = _SESSION.get(url, params={"where": "1=1", "returnCountOnly": "true", "f": "json"},
                         timeout=60, verify=False)
        r.raise_for_status()
        return r.json().get("count")
    except Exception as e:
        print(f"  ! {service}/{layer}: request error: {e}")
        return None


def main():
    print("Checking iplan XPLAN entity layers (expected: all 0 during migration)...")
    back = []
    for service, layer, label in WATCH:
        c = layer_count(service, layer)
        flag = "  <-- DATA IS BACK" if (c or 0) > 0 else ""
        print(f"  {label:42} count={c}{flag}")
        if (c or 0) > 0:
            back.append((label, c))

    if back:
        print("\n" + "=" * 60)
        print("ENTITY DATA HAS RETURNED on iplan:")
        for label, c in back:
            print(f"  - {label}: {c} features")
        print("Revive the guarded scrapers (easements / trees / land-use) and, if "
              "the data is on the new `entities` service, re-point their URLs. "
              "See scripts/scope_filter.py siblings + the iplan memory note.")
        print("=" * 60)
        return 1

    print("\nStill empty — migration not finished. Scrapers correctly stay guarded.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
