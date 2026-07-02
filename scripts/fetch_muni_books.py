# -*- coding: utf-8 -*-
"""
fetch_muni_books.py — ספר הנכסים + ספר ההקצאות העירוניים.

Source: the two public Power BI reports embedded in jerusalem.muni.il
(נכסי העירייה). The muni pages themselves are WAF-blocked, but they only
wrap app.powerbi.com iframes whose "publish to web" API is freely
accessible — no browser, no auth beyond the public resource key.
See memory: reference_muni_property_allocation_books.

What it does:
  1. POSTs a SemanticQueryDataShapeCommand to the public querydata endpoint
     for each book, selecting the useful columns (the underlying model is
     much richer than the visible report table — incl. גוש/חלקה, שטח בנוי,
     תיק הפקעה, סמל מוסד מנח"י).
  2. Decodes the DSR wire format (value dictionaries + R/Ø repeat/null
     bitmaps) into plain row dicts.
  3. Writes scripts/sources/muni_property_book.json (~30k rows = asset×parcel,
     city-wide, ~21MB — raw source, NOT committed / NOT served) and
     scripts/sources/muni_allocation_book.json (~1.8k rows).

The app never reads these directly — build_hafrasha_delivery.py joins them
against the hafrasha plans and emits the slim data/hafrasha_delivery.json.
Re-run occasionally (the reports refresh ~daily; the data drifts slowly),
then re-run the build. Idempotent — overwrites both jsons.
"""
import json, sys, io, os, time, datetime, urllib.request

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

HOST = "https://wabi-west-europe-api.analysis.windows.net"
HERE = os.path.dirname(os.path.abspath(__file__))

BOOKS = [
    {
        "out": "muni_property_book.json",
        "resource_key": "2cb9698a-6207-49fb-b9c6-0f27af708589",
        "model_id": 7914730,
        "table": "נכסים משני",
        # Sanity floor for unattended runs: the book had ~30k rows on 2026-07-02.
        # Far fewer means the API/report broke — abort instead of writing junk.
        "min_rows": 5000,
        "columns": [
            "מספר מזהה", "מספר נכס", "שם נכס", "שימוש עיקרי", "יעוד",
            "תאור יעוד", "סטטוס", "סטטוס על", "תאריך סטטוס", "בעלים",
            "שטח בנוי במ\"ר", "גוש", "חלקה", "שם רחוב", "מספר בית",
            "שם שכונה", "מספר תיק הפקעה", "תאריך פתיחת נכס",
            "נסח-זיקת הנאה", "אחראי תפעול נכס",
        ],
    },
    {
        "out": "muni_allocation_book.json",
        "resource_key": "dbe94b29-2463-4cf3-b9cb-d06dad3162ee",
        "model_id": 7721227,
        "table": "ספר הקצאות",
        "min_rows": 500,
        "columns": [
            "מספר נכס-משתמש", "מספר נכס", "שם נכס", "שם", "שימוש",
            "שימוש עיקרי", "סוג משתמש", "מספר זיהוי/תאגיד", "פעיל",
            "סטטוס על", "שם רחוב", "מספר בית", "שם שכונה",
            "אישור מועצה אחרון", "תקופת הקצאה", "סמל מוסד מנח\"י ",
            "אחראי תפעול נכס",
        ],
    },
]


def _find_out_dir():
    # Raw books live under scripts/sources (not the served data/ dir — the
    # property book is ~21MB and the app only consumes the slim derivative).
    for d in [os.path.join(HERE, "sources"),
              os.path.join(HERE, "scripts", "sources"),
              os.path.join(HERE, "oranim-app", "scripts", "sources")]:
        if os.path.isdir(d):
            return d
    raise SystemExit("scripts/sources dir not found")


def _query(book, restart_tokens=None):
    src = {"Entity": book["table"], "Name": "t", "Type": 0}
    select = [{"Column": {"Expression": {"SourceRef": {"Source": "t"}},
                          "Property": c}, "Name": c}
              for c in book["columns"]]
    window = {"Count": 30000}
    if restart_tokens:
        window["RestartTokens"] = restart_tokens
    q = {
        "version": "1.0.0",
        "queries": [{
            "Query": {"Commands": [{"SemanticQueryDataShapeCommand": {
                "Query": {"Version": 2, "From": [src], "Select": select},
                "Binding": {
                    "Primary": {"Groupings": [
                        {"Projections": list(range(len(select)))}]},
                    "DataReduction": {"DataVolume": 4,
                                      "Primary": {"Window": window}},
                    "Version": 1,
                },
            }}]},
            "CacheKey": "", "QueryId": "",
            "ApplicationContext": {"DatasetId": "x", "Sources": []},
        }],
        "cancelQueries": [], "modelId": book["model_id"],
    }
    req = urllib.request.Request(
        HOST + "/public/reports/querydata?synchronous=true",
        data=json.dumps(q).encode("utf-8"),
        headers={"X-PowerBI-ResourceKey": book["resource_key"],
                 "Content-Type": "application/json"})
    # Transient connect timeouts happen even seconds after a successful call —
    # retry with backoff so an unattended (cron) run survives them.
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            if attempt == 2:
                raise
            wait = 15 * (attempt + 1)
            print(f'  query failed ({e}); retrying in {wait}s…')
            time.sleep(wait)


def _decode_dsr(resp, n_cols):
    """DSR wire format → list of row value-lists.

    Each row carries C (new values), R (bitmask: repeat column from previous
    row) and Ø (bitmask: null). String columns hold indexes into ValueDicts.
    """
    ds = resp["results"][0]["result"]["data"]["dsr"]["DS"][0]
    dicts = ds.get("ValueDicts", {})
    rows_out, schema, prev = [], None, None
    for ph in ds.get("PH", []):
        for row in ph.get("DM0", []):
            if "S" in row:
                schema = row["S"]
            if schema is None or len(schema) != n_cols:
                continue
            c, ci = row.get("C", []), 0
            rbits, nbits = row.get("R", 0), row.get("Ø", 0)
            vals = []
            for i, col in enumerate(schema):
                if nbits & (1 << i):
                    vals.append(None)
                elif rbits & (1 << i):
                    vals.append(prev[i])
                else:
                    v = c[ci] if ci < len(c) else None
                    ci += 1
                    dn = col.get("DN")
                    if dn is not None and isinstance(v, int):
                        v = dicts[dn][v]
                    if isinstance(v, str) and v.endswith("L") \
                            and v[:-1].lstrip("-").isdigit():
                        v = int(v[:-1])
                    vals.append(v)
            rows_out.append(vals)
            prev = vals
    return rows_out, ds.get("RT")


def fetch_book(book):
    all_rows, tokens, page = [], None, 0
    while True:
        resp = _query(book, tokens)
        rows, tokens = _decode_dsr(resp, len(book["columns"]))
        all_rows.extend(rows)
        page += 1
        print(f'  page {page}: +{len(rows)} rows'
              + (' (more…)' if tokens else ''))
        if not tokens or not rows:
            break
    cols = [c.strip() for c in book["columns"]]
    return [dict(zip(cols, r)) for r in all_rows]


def main():
    data_dir = _find_out_dir()
    for book in BOOKS:
        print(f'fetching {book["out"]} ({book["table"]})…')
        rows = fetch_book(book)
        if len(rows) < book["min_rows"]:
            raise SystemExit(
                f'{book["out"]}: only {len(rows)} rows (< {book["min_rows"]}) '
                f'— API/report likely broke, aborting without writing')
        out = {"meta": {"fetched_at": datetime.date.today().isoformat(),
                        "source": "app.powerbi.com publish-to-web "
                                  + book["resource_key"],
                        "rows": len(rows)},
               "rows": rows}
        path = os.path.join(data_dir, book["out"])
        with open(path, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
        print(f'  wrote {len(rows)} rows -> {path}')


if __name__ == "__main__":
    main()
