"""
hafrash_classify.py — map a hafrash_prg description to a coarse allocation type.

A built public allocation (הפרשה מבונה) is described free-text in hafrash_prg
(GS col AR, also mirrored in plans.geojson). This classifies that text into one
or more canonical use types so the type can be learned automatically when a plan
is new / changes status. Keyword set mirrors add_tracking_to_mivnei.py:119,
extended with the common synonyms seen across the dataset.

A single plot routinely holds several uses (e.g. "בי\"ס, גנים ובית כנסת"), so
classify() returns a *list*; primary() returns the first / most-specific.
"""

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
    return specific if specific else found


def primary(text):
    """The single most-specific type, or '' when nothing matched."""
    f = classify(text)
    return f[0] if f else ""


# Uses that imply a *built* allocation worth pulling a building appendix for
# (i.e. floors are meaningful). Open space / road allocations are excluded.
BUILT_USE_TYPES = {
    "בית ספר", "גן ילדים", "מעון יום", "בית כנסת", "מקווה",
    "קהילה", "רווחה", "תרבות", "מועדון", "בריאות", "ספורט", "מבנה ציבור",
}


def has_built_allocation(text):
    """True when hafrash_prg describes a built public allocation."""
    return bool(set(classify(text)) & BUILT_USE_TYPES)


if __name__ == "__main__":
    import sys
    for arg in sys.argv[1:]:
        print(f"{arg!r} -> types={classify(arg)} primary={primary(arg)!r} "
              f"built={has_built_allocation(arg)}")
