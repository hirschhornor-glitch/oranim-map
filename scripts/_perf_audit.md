# דו"ח Perf — אפליקציית אורנים

תאריך: 2026-05-27 · מצב APP_VERSION: `2026-05-27-summary-report`

## תקציר מנהלים

האתר איטי מ-3 סיבות עיקריות:
1. **כמות עצומה של GeoJSON** (~70MB נכסים סטטיים, ~50MB ב-deferred load)
2. **רנדורי Leaflet כבדים** — אלפי polygon/path elements ב-DOM
3. **חוסר אגרסיביות ב-caching** — כל ניווט/רענון טוען הכל מחדש

**3 שיפורים מומלצים בעדיפות גבוהה** (ROI טוב, מאמץ נמוך):
1. **קאשinng עם Service Worker** — שיפור x10 בטעינה חוזרת
2. **debounce של `setDeferredTick`** — עוצר re-render גלי בטעינת deferred
3. **הוספת `plans` ו-`projector_gonenim` ל-DEFERRED_FILES** — קיצור first paint

---

## ניתוח מפורט

### 1. גדלי קבצים

| קובץ | גודל | סטטוס | המלצה |
|---|---|---|---|
| `yiud_karka_kayam.geojson` | 11 MB | deferred | OK — נטען רק אם השכבה דלוקה |
| `parcels_gonenim.geojson` | 11 MB | **לא ב-GEOJSON_FILES?** | בדיקה — אולי נטען רק ב-build |
| `roads.geojson` | 9 MB | deferred | OK |
| `osm_buildings_cache.json` | 8.4 MB | cache | לא נטען ב-app — OK |
| `landuse_xplan.geojson` | 7.6 MB | deferred | OK |
| `moe_coordinates_cache.json` | 5.9 MB | cache | לא נטען — OK |
| `buildings.geojson` | 3.9 MB | deferred | OK |
| `plans.geojson` | 3.5 MB | **טעינה ראשונית** | ⚠ העבר ל-DEFERRED אם אפשר |
| `master_plan_moshavot.geojson` | 2.8 MB | deferred | OK |
| `projector_gonenim.geojson` | 1.9 MB | **טעינה ראשונית** | ⚠ העבר ל-DEFERRED (נצרך רק אם השכבה דלוקה) |
| `index.html` | 1.7 MB | אינליין | קשה לפצל |

**סיכום:** טעינה ראשונית כ-7-10 MB · deferred כ-50 MB.

### 2. ארכיטקטורת טעינה

המבנה הנוכחי (`index.html` שורות 5162-5605):
- `DEFERRED_FILES`: 10 קבצים גדולים נטענים אחרי first paint
- `entries` (mandatory): כל השאר (כ-30+ קבצים קטנים)
- כל קובץ deferred → `setDeferredTick(t => t+1)` → re-render של כל React tree

**בעיה #1:** `setDeferredTick` נקרא 10 פעמים — 10 רנדורים מלאים ברצף.
**בעיה #2:** `applyKayamFilter()` נקרא בכל deferred load — חישוב יקר.
**בעיה #3:** אין IndexedDB cache — כל רענון = טעינה מהשרת.

### 3. רנדור Leaflet

מהבדיקה ב-DOM:
- `leaflet-roads-pane`: 8,146 path elements
- `leaflet-map-pane`: 8,471 path elements
- `leaflet-plans-pane`: 296 path elements
- `leaflet-projectorPane`: ~270 elements
- `leaflet-subNeighborhood-pane`: 22 elements

**8,000+ SVG paths** = canvas browser overhead משמעותי בכל pan/zoom.

### 4. רנדור React (פילטרים החדשים)

הפילטרים שהוספתי עושים `removeLayer + buildProjectorLayer + addToMap` בכל toggle.
זה ~260 features × יצירת div/svg = ~0.5-1 שניה ב-CPU רגיל.

---

## המלצות לפי עדיפות

### עדיפות גבוהה (ROI מצוין, מאמץ נמוך)

#### A. Service Worker לקאשинг
טעינה ראשונה: ללא שינוי. טעינות חוזרות: **x10 מהירות** (מ-30s → 3s).

```javascript
// Add to index.html top:
if ('serviceWorker' in navigator) {
  navigator.serviceWorker.register('/sw.js');
}
// Create sw.js with cache-first strategy for /data/*.geojson
```

**מאמץ:** 1-2 שעות · **רווח:** 90% מהמשתמשים יראו שיפור דרמטי

#### B. debounce של setDeferredTick
במקום 10 רנדורים, אחד אחרי שכל הקבצים נטענו:

```javascript
let deferredTickTimer;
.then(data => {
  geoDataRef.current[key] = data;
  clearTimeout(deferredTickTimer);
  deferredTickTimer = setTimeout(() => setDeferredTick(t => t+1), 200);
});
```

**מאמץ:** 5 דקות · **רווח:** UI חלק יותר אחרי טעינה ראשונית

#### C. דחיית `projector_gonenim` + `plans`
ב-`DEFERRED_FILES` להוסיף:
```javascript
const DEFERRED_FILES = [
    'yiud_karka_kayam', 'roads', 'buildings',
    'landuse_xplan', 'plans', 'projector_gonenim', 'projector_gonenim_tzatal',
    'master_plan_*',
];
```

**הסתייגות:** plans נדרש למסכי "אחוזי מימוש". יש לוודא שהקוד מטפל ב-undefined.
**מאמץ:** 15-30 דקות (כולל בדיקות) · **רווח:** קיצור first paint ב-5MB

### עדיפות בינונית

#### D. שיטוח React state — להוציא state גדולים מהקומפוננטה הראשית
הקומפוננטה הראשית כנראה מחזיקה ~20+ state hooks. כל setState = re-render של הכל. שימוש ב-Zustand/Jotai יכול לעזור, אבל זה refactor גדול.

#### E. Canvas renderer במקום SVG
ב-buildProjectorLayer + plans:
```javascript
L.geoJSON(data, { renderer: L.canvas({ pane: 'plansPane' }) })
```
מהיר יותר ל-2000+ features, אבל פחות אינטראקטיבי (קליק על polygon — אבל יש כבר עבודה ב-popup).

**מאמץ:** 30 דקות · **רווח:** zoom/pan חלק יותר באזורי תב"עות צפופים

#### F. TopoJSON עבור גדולים
`landuse_xplan` (7.6MB) → TopoJSON כ-1-2MB.
דורש libtopo בצד client. **מאמץ:** 2-3 שעות.

### עדיפות נמוכה

#### G. שינוי APP_VERSION לסקירת cache-bust
`fetch(url + '?v=' + APP_VERSION)` נכון, אבל בכל deployment הכל נטען מחדש. אפשר לחתום קבצים בנפרד (ETag-based) כדי שרק קבצים ששונו ייטענו.

#### H. Code-splitting של index.html
1.7MB HTML עם React inline. אפשר לפצל ל-modules → טעינה דינמית.
**מאמץ:** רב · **רווח:** משמעותי אבל דורש refactor גדול.

---

## תוכנית פעולה מומלצת (אם רוצים להריץ)

**שלב 1 (1-2 שעות, x10 שיפור לטעינות חוזרות):**
- Service Worker עם cache-first

**שלב 2 (חצי שעה, UI חלק יותר):**
- debounce setDeferredTick

**שלב 3 (חצי שעה, first paint מהיר יותר):**
- העברת plans + projector ל-DEFERRED_FILES + טיפול ב-null

**שלב 4 (אופציונלי, אם עדיין איטי):**
- Canvas renderer ל-plans layer

---

## מדדים שאפשר לאסוף

לפני/אחרי כל שיפור, למדוד:
- **First Contentful Paint** (DevTools → Lighthouse)
- **Time to Interactive**
- **Total bundle size** (DevTools → Network)
- **Frame rate בזמן pan** (DevTools → Performance)

ערכים יעד:
- FCP: < 2s
- TTI: < 5s
- Pan rate: > 30fps

---

## מה לא קריטי כעת

- **גודל index.html** (1.7MB) — לא בעיה אם cached
- **cross_sections JPGs** (9.89MB) — נטענים lazy בקליק
- **PDFs בתיקיית הפרוייקטור** — לא קשורים לאתר
