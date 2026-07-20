# -*- coding: utf-8 -*-
"""
Shared JS snippet for calling the Mavat SV4 REST API from a browser page.

Since ~06/2026 Mavat validates a reCAPTCHA v3 token on every /rest/api request:
the site's Angular interceptor calls grecaptcha.execute(sitekey, {action:
'importantAction'}) and sends the token in the Authorization header. A bare
fetch() without it gets the SPA's index.html back with HTTP 404.

Usage inside a page.evaluate() JS string:

    result = await page.evaluate(
        "async () => {" + MAVAT_AUTH_JS + \"\"\"
            const resp = await fetch('/rest/api/SV4/1?mid=...&guid=0',
                                     { headers: await mvHeaders() });
            ...
        }\"\"\")

Tokens are short-lived and single-use — call mvHeaders() per request,
not once per batch.
"""

MAVAT_AUTH_JS = """
    const mvHeaders = async () => {
        const h = { 'Accept': 'application/json, text/plain, */*' };
        try {
            let key = null;
            for (const s of document.querySelectorAll('script[src*="recaptcha"]')) {
                const m = s.src.match(/render=([0-9A-Za-z_-]+)/);
                if (m) key = m[1];
            }
            if (key && window.grecaptcha)
                h['Authorization'] = await grecaptcha.execute(key, { action: 'importantAction' });
        } catch (e) {}
        return h;
    };
"""
