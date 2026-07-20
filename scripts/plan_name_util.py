"""
Shared sanitizer for plan_name_he (Hebrew plan name).

ROOT FIX for the recurring "garbage tail" problem: several Mavat scrapers
(enrich_new_plans_dom.py DOM h1 capture, enrich_mavat / enrich_api / enrich_new_plans,
detect_new_plans) used to grab the whole Mavat "זיהוי וסיווג" header panel into
plan_name_he, producing names like:
    "מוקד אזרחי - פייר קניג זיהוי וסיווג ... עדכון אחרון ... סה\"כ שטח בדונם ..."

Apply clean_plan_name() at EVERY point that writes plan_name_he so the panel text
can never leak in again, regardless of which scraper ran.
"""

# Panel section labels that mark the start of scraped boilerplate. Anything from
# the first marker onward is NOT part of the plan name.
PANEL_MARKERS = (
    'זיהוי וסיווג',
    'עדכון אחרון',
    'סה"כ שטח בדונם',
    'מטרות התכנית',
    'מטרת התכנית',
    'יחס בין תכניות',
    'לתכנית זו יש עותק',   # "* לתכנית זו יש עותק חדש יותר, שטרם נקלט במערכת"
)

MAX_LEN = 80  # a real plan name is short; longer => something leaked


def clean_plan_name(name):
    """Trim a scraped plan_name_he to just the name.

    - Cuts at the first panel marker (keeping only the text before it).
    - Collapses whitespace/newlines.
    - Strips trailing punctuation.
    - Hard-caps length as a backstop.
    Returns '' for falsy input.
    """
    if not name:
        return ''
    s = ' '.join(str(name).split())  # collapse \r\n and runs of spaces
    cut = len(s)
    for m in PANEL_MARKERS:
        i = s.find(m)
        if 0 <= i < cut:
            cut = i
    s = s[:cut].strip().rstrip(',*').strip().rstrip(',*').strip()
    if len(s) > MAX_LEN:
        s = s[:MAX_LEN].rstrip() + '…'
    return s


if __name__ == '__main__':
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    tests = [
        'מוקד אזרחי - פייר קניג זיהוי וסיווג ירושלים עדכון אחרון 12/2025 סה"כ שטח בדונם 4.2',
        'מתחם לשימושים מעורבים / דינמומטר-אפריקה ישראל, תלפיות ירושלים',
        '  שם\r\nעם\r\nשורות  ',
        '',
        None,
    ]
    for t in tests:
        print(repr(t), '->', repr(clean_plan_name(t)))
