"""
hafrash_classify.py — map a hafrash_prg description to a coarse allocation type.

A built public allocation (הפרשה מבונה) is described free-text in hafrash_prg
(GS col AR, also mirrored in plans.geojson). This classifies that text into one
or more canonical use types so the type can be learned automatically when a plan
is new / changes status. Keyword set mirrors add_tracking_to_mivnei.py:119,
extended with the common synonyms seen across the dataset.

A single plot routinely holds several uses (e.g. "בי\"ס, גנים ובית כנסת"), so
classify() returns a *list*; primary() returns the first / most-specific.

Two classifiers live here on purpose:
  * HAFRASH_TYPE_KEYWORDS / classify() — canonical *facility* types, the labels
    the enrichment email and hafrash_types.json speak in.
  * HAFRASH_DOMAIN_RX / domains()      — coarse *domain* buckets, a verbatim port
    of the app's own regexes (src/app.jsx HAFRASH_DOMAIN_RX). This is what decides
    whether a hafrasha's use is KNOWN AT ALL, and it must stay in step with the JS
    so the map symbology and the "unknown allocation" audit never disagree.
The keyword list alone used to miss "מועדון גיל שלישי", "2 דירות קלט",
"דירה לבעלי מוגבלויות" and "חינוך" — classify() now falls back to the domain
regexes so those stop reading as unclassified.
"""
import re

# Ordered domain regexes — VERBATIM port of HAFRASH_DOMAIN_RX in src/app.jsx.
# Order matters: the more specific / higher-priority domains are tested first.
# Keep this list byte-for-byte in step with the JS; a use string may match more
# than one domain (e.g. "טיפת חלב ומעון יום" = health + education).
HAFRASH_DOMAIN_RX = [
    ("education", r'(תיכון|חטיב|אולפנ|מדרשי|ישיב|על[\- ]?יסודי|בתי ספר|בית ספר|בי"?ס|בי״ס|ביה"?ס|ביה״ס|בית-ספר|יסודי|מעון|פעוטון|גן ילדים|גני ילדים|גנון|כיתת? גן|כיתות גן|חינוך)'),
    ("religion",  r'(בית[- ]?כנסת|בתי כנסת|ביכ"?נ|ביכ״נ|מקווה|מקוואות|כנסיי|מנזר|מסגד|בית מדרש|כולל|דת)'),
    ("sport",     r'(ספורט|בריכ|התעמלות|איצטדיון|מגרש משחק|מגרש כדור|אולם התעמלות)'),
    ("health",    r'(מרפאה|קופת חולים|טיפת חלב|תחנת בריאות|בריאות|רפוא)'),
    ("emergency", r'(חירום|מקלט|מקלוט|מיגון|תפעול|פיקוד העורף|כיבוי אש)'),
    ("welfare",   r'(רווחה|שירותים חברתיים|חברתי|שימושי חברה|שירותי חברה|חברה וקהיל|מועדון נוער|מועדונית|נוער|קשיש|גיל שלישי|אזרחים ותיקים|תשוש|מרכז יום|נכים|מוגבלויות|שיקום|דיר(?:ת|ות) קלט|דיור ציבורי|דיור מוגן)'),
    ("culture",   r'(מתנ"?ס|מתנ״ס|מרכז קהילתי|מועדון קהילתי|שלוחת מתנ|קהיל|ספריי|ספריה|תרבות|אמנות|אומנות|אולם מופעים|פנאי|מוזיאון|שימושי ציבור|שימ.*קהיל)'),
]
_DOMAIN_RX = [(d, re.compile(p)) for d, p in HAFRASH_DOMAIN_RX]

# One representative canonical type per domain, for the classify() fallback.
DOMAIN_TO_TYPE = {
    "education": "מבנה חינוך", "religion": "מבנה דת", "sport": "ספורט",
    "health": "בריאות", "emergency": "חירום", "welfare": "רווחה", "culture": "תרבות",
}


def domains(text):
    """ALL domain buckets present in a use string (mirrors hafrashUseDomainsAll)."""
    s = str(text or "")
    if not s.strip():
        return []
    return [d for d, rx in _DOMAIN_RX if rx.search(s)]


def has_known_use(text):
    """True when the free text names a public use we can actually identify.

    False for the generic envelopes ("מבנים ומוסדות ציבור", "*בתיאום עם מחלקת
    מבני ציבור*", blank) — i.e. exactly the plans whose allocation type is unknown
    and that need the permit גרמושקה read.
    """
    return bool(domains(text))


# (keyword, canonical type). Specific uses first; the generic "מבנה ציבור"
# bucket is only kept when nothing more specific matched.
HAFRASH_TYPE_KEYWORDS = [
    ("בית ספר", "בית ספר"), ('בי"ס', "בית ספר"), ('ביה"ס', "בית ספר"),
    ("גני ילדים", "גן ילדים"), ("גן ילדים", "גן ילדים"), ("גנים", "גן ילדים"),
    ("כיתות גן", "גן ילדים"), ("כיתת גן", "גן ילדים"), ("גן", "גן ילדים"),
    ("מעון", "מעון יום"),
    ("בית כנסת", "בית כנסת"), ('ביכנ"ס', "בית כנסת"), ('ביהכנ"ס', "בית כנסת"),
    ("מקווה", "מקווה"), ("מקוה", "מקווה"),
    ('מתנ"ס', "קהילה"), ("קהילה", "קהילה"), ("מרכז קהילתי", "קהילה"),
    ("רווחה", "רווחה"),
    ("תרבות", "תרבות"),
    ("מועדון", "מועדון"),
    ("מרפאה", "בריאות"), ("טיפת חלב", "בריאות"), ("בריאות", "בריאות"),
    ("ספורט", "ספורט"), ("אולם", "ספורט"),
    ("מבנים ומוסדות ציבור", "מבנה ציבור"), ("מבנה ציבור", "מבנה ציבור"),
    ("מוסדות ציבור", "מבנה ציבור"), ("מבני ציבור", "מבנה ציבור"),
    ("שטח ציבורי בנוי", "מבנה ציבור"), ("ציבורי", "מבנה ציבור"),
]


def classify(text):
    """Return a de-duped list of canonical use types found in the text."""
    s = str(text or "")
    found = []
    for kw, typ in HAFRASH_TYPE_KEYWORDS:
        if kw in s and typ not in found:
            found.append(typ)
    specific = [t for t in found if t != "מבנה ציבור"]
    if specific:
        return specific
    # Nothing specific from the keyword list — fall back to the app's domain
    # regexes, which know the phrasings the keyword list never learned
    # ("מועדון גיל שלישי", "דירת קלט", "דירה לבעלי מוגבלויות", "חינוך").
    fallback = [DOMAIN_TO_TYPE[d] for d in domains(s) if d in DOMAIN_TO_TYPE]
    return fallback or found


def primary(text):
    """The single most-specific type, or '' when nothing matched."""
    f = classify(text)
    return f[0] if f else ""


# Uses that imply a *built* allocation worth pulling a building appendix for
# (i.e. floors are meaningful). Open space / road allocations are excluded.
BUILT_USE_TYPES = {
    "בית ספר", "גן ילדים", "מעון יום", "בית כנסת", "מקווה",
    "קהילה", "רווחה", "תרבות", "מועדון", "בריאות", "ספורט", "מבנה ציבור",
    # domain-fallback labels (see DOMAIN_TO_TYPE)
    "מבנה חינוך", "מבנה דת", "חירום",
}


def has_built_allocation(text):
    """True when hafrash_prg describes a built public allocation."""
    return bool(set(classify(text)) & BUILT_USE_TYPES)


if __name__ == "__main__":
    import sys
    for arg in sys.argv[1:]:
        print(f"{arg!r} -> types={classify(arg)} primary={primary(arg)!r} "
              f"domains={domains(arg)} known={has_known_use(arg)} "
              f"built={has_built_allocation(arg)}")
