"""
check_xplan_entities.py — watchdog for the iplan XPLAN entity-layer migration.

Background (~2026-06): iplan emptied the XPLAN entity layers — land-use
(MapServer/4), easement polygons (MapServer/3) and tree points (MapServer/0)
all return 0 features, and the new `entities` service is still empty too. Only
the blue-line plan boundaries (MapServer/1) survived. Our easement/tree/land-use
scrapers were guarded to abort on empty rather than overwrite the committed
static data.

This watchdog polls the candidate sources and exits:
  0 (green)  — all still empty, OR iplan was unreachable/slow (inconclusive).
  1 (red)    — a layer CONFIRMED (queried twice) reports features again. That's
               the rare, intended alert: revive (and maybe re-point) the scrapers.

Design notes (it must never cry wolf):
  - short timeouts + no retries, so a slow/unreachable iplan can't hang the job
    or burn CI minutes (this replaced a 60s-timeout version that ran ~5.5 min).
  - request errors are treated as "unknown", NOT as "data is back".
  - a positive count is re-queried; we only alert if BOTH calls agree it's >0.
  - if EVERY request failed (iplan unreachable from the runner), we exit 0 —
    inconclusive is not an alert.

Run locally:  python scripts/check_xplan_entities.py
"""
import ssl
import sys

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    import urllib3
    urllib3.disable_warnings()
except Exception as e:  # pragma: no cover - environment problem, not "data back"
    print(f"setup error ({e}); treating as inconclusive.")
    sys.exit(0)


class _LegacySSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.set_ciphers('DEFAULT:@SECLEVEL=0')
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)


_SESSION = requests.Session()
_SESSION.mount('https://ags.iplan.gov.il', _LegacySSLAdapter(max_retries=Retry(total=0)))

BASE = "https://ags.iplan.gov.il/arcgisiplan/rest/services/PlanningPublic"
CONNECT_TIMEOUT, READ_TIMEOUT = 8, 12   # bounded: ~20s worst case per request

# (service, layer, human label) — where our lost entity data lived / may return.
WATCH = [
    ("Xplan",    0, "tree points (XPLAN/0)"),
    ("Xplan",    3, "easement polygons (XPLAN/3)"),
    ("Xplan",    4, "land-use polygons (XPLAN/4)"),
    ("entities", 1, "entities service / point layer"),
    ("entities", 2, "entities service / line layer"),
    ("entities", 3, "entities service / polygon layer"),
]


def layer_count(service, layer):
    """Return (count:int) on a clean response, or None on any error/non-count."""
    url = f"{BASE}/{service}/MapServer/{layer}/query"
    try:
        r = _SESSION.get(url, params={"where": "1=1", "returnCountOnly": "true", "f": "json"},
                         timeout=(CONNECT_TIMEOUT, READ_TIMEOUT), verify=False)
        r.raise_for_status()
        c = r.json().get("count")
        return c if isinstance(c, int) else None
    except Exception as e:
        print(f"  ! {service}/{layer}: {type(e).__name__}: {str(e)[:80]}")
        return None


def main():
    print("Polling iplan XPLAN entity layers (expected: all 0 during migration)...")
    reached = 0          # how many layers we got a clean numeric answer from
    back = []
    for service, layer, label in WATCH:
        c = layer_count(service, layer)
        if c is None:
            print(f"  {label:42} count=?(unreachable)")
            continue
        reached += 1
        if c > 0:
            # Confirm before crying wolf — re-query; only alert if it agrees.
            c2 = layer_count(service, layer)
            confirmed = isinstance(c2, int) and c2 > 0
            print(f"  {label:42} count={c} (recheck={c2}) {'<-- DATA IS BACK' if confirmed else '(unconfirmed, ignoring)'}")
            if confirmed:
                back.append((label, c))
        else:
            print(f"  {label:42} count=0")

    if back:
        print("\n" + "=" * 60)
        print("ENTITY DATA HAS RETURNED on iplan:")
        for label, c in back:
            print(f"  - {label}: {c} features")
        print("Revive the guarded scrapers (easements / trees / land-use); if the "
              "data is on the new `entities` service, re-point their URLs first. "
              "See the iplan memory note.")
        print("=" * 60)
        return 1

    if reached == 0:
        print("\nCould not reach any iplan layer (slow/blocked from this runner) — "
              "inconclusive, not alerting. Will retry next run.")
        return 0

    print(f"\nStill empty ({reached}/{len(WATCH)} layers answered, all 0). "
          "Migration not finished; scrapers correctly stay guarded.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
