# פרוייקטור גוננים — המלצות בלי מיקום מדויק

נכון ל-347 פיצ׳רים סך הכל ב-`projector_gonenim.geojson`.

**10 שורות מ-xlsx ממוקמות כיום על מרכז המינה"ק / מרכז תת-שכונה (לא נקודה אמיתית).**

הסיבה: לא נמצא מקור מיקום מתאים — אין `מזהה מיקום` (גוש/חלקה), לא תאם shapefile רשמי, אין כתובת מזוהה בתיאור, אין chip בתשריט.

## הרשימה

| # | אזור | מס פרויקט | שם | שירות | תחום | סיבה | תקנה אפשרית |
|---|---|---|---|---|---|---|---|
| 1 | רסקו | 2 | מתחם לוריא | בי"ס יסודי | programa | match_quality=None + geometry_source=sub_neighborhood_centroid | הוסיפי `מזהה מיקום` (גוש/חלקה) ב-xlsx — מתחם לוריא בכניסה לרסקו |
| 2 | רסקו | 2 | מתחם לוריא | גני ילדים | programa | match_quality=None + geometry_source=sub_neighborhood_centroid | הוסיפי `מזהה מיקום` (גוש/חלקה) ב-xlsx — מתחם לוריא בכניסה לרסקו |
| 3 | רסקו | 2 | מתחם לוריא | מעון יום | programa | match_quality=None + geometry_source=sub_neighborhood_centroid | הוסיפי `מזהה מיקום` (גוש/חלקה) ב-xlsx — מתחם לוריא בכניסה לרסקו |
| 4 | רסקו | 2 | מתחם לוריא | בית כנסת | programa | match_quality=None + geometry_source=sub_neighborhood_centroid | הוסיפי `מזהה מיקום` (גוש/חלקה) ב-xlsx — מתחם לוריא בכניסה לרסקו |
| 5 | רסקו | 10 | ציר הרצוג | שדרוג רחובות | public_space | geometry_source=sub_neighborhood_centroid | שדרוג רחוב הרצוג — בקוד צריך לקחת את הקו של הרחוב מ-roads.geojson כשמופיע "ציר" + שם רחוב |
| 6 | גוננים א-ו | 2 | 3 גני ילדים ברחוב גוננים (משרה מלכה, מנח"י) | גן ילדים | programa | geometry_source=sub_neighborhood_centroid | הוסיפי `מזהה מיקום` ב-xlsx — בדרכ גוננים יש 3 גנים פנימיים |
| 7 | גוננים א-ו | None | תב"ע לכביש 34 עתידי | רחוב חדש | transport | match_quality=None + geometry_source=sub_neighborhood_centroid | דורש חיבור לשכבת תב"עות לפי מספר 34 — או מזהה מיקום |
| 8 | קטמונים ח-ט | 4 | גשר בין דב הוז לגולומב | גשר | public_space | geometry_source=sub_neighborhood_centroid | גשר מתוכנן — צריך הוספת גיאומטריה ידנית (קצוות גשר) או shapefile ייעודי |
| 9 | קטמונים ח-ט | None | דוד איילון | שיפור רמת שירות | transport | geometry_source=sub_neighborhood_centroid | שיפור רחוב — להפעיל לוגיקה דומה ל"ציר הרצוג" (קו של הרחוב) |
| 10 | פת | None | רח' אברהם ארנסט | שיפור רמת שירות | transport | match_quality=None + geometry_source=sub_neighborhood_centroid | שיפור רחוב — אותה לוגיקה כמו "דוד איילון" |

## תקנות אפשריות (מוצעות)

1. **הוספת `מזהה מיקום` ב-xlsx** — עבור 4 שורות "מתחם לוריא" (כולם רסקו #2) + "גני ילדים ברחוב גוננים" + "תב"ע לכביש 34".
2. **לוגיקת "ציר/רחוב"** — להוסיף לקוד: אם service מכיל "שדרוג רחוב" או "שיפור רמת שירות", לחפש את שם הרחוב בתיאור/שם ולקחת את ה-LineString מ-roads.geojson. (פותר 3 שורות)
3. **גיאומטריה ידנית לגשר** — שורה אחת ("גשר בין דב הוז לגולומב") דורשת קוארדינטות של 2 קצוות הגשר — להוסיף ל-`MANUAL_LOCATION_OVERRIDES`.

אחרי תיקון כל אלה, אמורות להישאר 0 שורות בלי מיקום מדויק.