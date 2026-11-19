"""Retry transient Google Sheets / Drive API errors — for every gspread client.

`import gs_retry` once (right after `import gspread`) and every gspread request
in the process goes through the retry below. It patches
gspread.http_client.HTTPClient.request at class level, so it covers clients
made by authorize() / service_account() / oauth() alike.

Why: on 2026-11-15 a single Drive 500 killed the nightly update_github_data run.

What is retried (and what is deliberately NOT):
  * 429 (rate limit) — any method. Google rejected the request, nothing was
    applied, so resending a write is safe.
  * 5xx / 408 / connection errors — GET only. A POST/PUT that got a 500 may
    still have been applied (append_rows, insert_rows, batch_update with
    insertDimension...). Resending it could write a row twice — a silent,
    plausible-looking wrong result, which is worse than a loud failure. So
    writes keep failing loudly as before.
Every retry is printed to stderr so it shows up in the task logs; after the
last attempt the original exception is raised unchanged.
"""
import sys
import time

import requests
from gspread import http_client as _hc
from gspread.exceptions import APIError

DELAYS = (10, 30, 60, 90)          # seconds; ~3 min total before giving up
_GET_RETRY_CODES = {408, 500, 502, 503, 504}

if not getattr(_hc.HTTPClient.request, "_gs_retry", False):
    _orig_request = _hc.HTTPClient.request

    def _request(self, method, endpoint, *args, **kwargs):
        is_get = str(method).upper() == "GET"
        for attempt in range(len(DELAYS) + 1):
            try:
                return _orig_request(self, method, endpoint, *args, **kwargs)
            except APIError as e:
                code = e.code
                retry = code == 429 or (is_get and code in _GET_RETRY_CODES)
                why = f"HTTP {code}"
                err = e
            except (requests.ConnectionError, requests.Timeout) as e:
                retry = is_get
                why = type(e).__name__
                err = e
            if not retry or attempt == len(DELAYS):
                raise err
            wait = DELAYS[attempt]
            print(f"[gs_retry] {method} {str(endpoint)[:90]} -> {why}; "
                  f"retry {attempt + 1}/{len(DELAYS)} in {wait}s",
                  file=sys.stderr, flush=True)
            time.sleep(wait)

    _request._gs_retry = True
    _hc.HTTPClient.request = _request
