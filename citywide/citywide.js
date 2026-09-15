/* citywide.js — the three citywide reports in one page, list beside map.
 *
 *   מסחר ותעסוקה · קרן תחזוקה ודיור מותנה · דיור להשכרה
 *
 * Deliberately does NOT touch commerce.html / commerce.js, which another session
 * owns and is still working on. This page only READS their two data files
 * (ce_citywide_plans.json, ce_citywide_parcels.geojson); their page keeps working
 * unchanged, and a structural change on their side is a change here, not a conflict.
 *
 * Geometry comes from three layers. The two borrowed ones each cover only their own
 * scope — ce_citywide_parcels the commerce plans, plans.geojson Oranim's neighbourhoods
 * — which left 40 housing plans drawable by neither and labelled "ללא מיקום". Those are
 * now fetched from XPLAN's blue line into housing_plan_boundaries.geojson (40/40 found).
 * The counter and the label stay in the code: if a future plan is missing again, the
 * map must say so rather than quietly drawing a subset the eye reads as "all of them".
 *
 * Plain DOM rather than React: commerce.js is a React app, but Leaflet is
 * framework-agnostic and the rest is three tables. Importing a framework to share
 * a page would buy nothing here.
 */
(function () {
  "use strict";

  var HOUSING = "../data/housing_terms.json";
  var CE = "../data/ce_citywide_plans.json";
  var PARCELS = "../data/ce_citywide_parcels.geojson";
  // Blue-line boundaries for the housing plans that neither existing layer covers —
  // they are in neither the commerce scope nor Oranim's, so they showed as
  // "ללא מיקום" despite XPLAN publishing a boundary for every one of them.
  var BOUNDS = "../data/housing_plan_boundaries.geojson";
  // Per-plan realization, built for the commerce report but plan-level and so
  // useful to all three: it already covers 56 of the 104 maintenance-fund plans
  // and 73 of the 152 rental ones. "Is the fund money actually coming?" and "are
  // the rental units being built?" are the same question as "did it get a permit".
  var PERMITS = "../data/ce_permit_status_citywide.json";
  var CENTER = [31.7683, 35.2137], ZOOM = 12;

  var state = { tab: "ce", q: "", status: "", sub: "", src: "", sort: "", desc: true,
                sel: null };
  var D = { ce: null, housing: null, parcels: null, bounds: null, permits: null };
  var map = null, layer = null, geoByPlan = {}, mapNote = "";

  var esc = function (s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  };
  var n0 = function (v) { return v == null ? "" : Number(v).toLocaleString("he-IL"); };
  var ils = function (v) { return v == null ? "" : "₪" + Number(v).toLocaleString("he-IL"); };
  var short = function (v) {
    if (!v) return "0";
    if (v >= 1e9) return (v / 1e9).toFixed(2) + "B";
    if (v >= 1e6) return (v / 1e6).toFixed(1) + "M";
    if (v >= 1e3) return Math.round(v / 1e3) + "K";
    return String(Math.round(v));
  };
  // Realization, shared by all three reports. Keyed by the same padded plan number.
  var realOf = function (plan) { return (D.permits || {})[plan] || null; };
  var yn = function (v) {
    return v ? '<span class="hi">✓</span>' : '<span class="empty">—</span>';
  };
  // Stage columns, appended to every report's table. A plan that is not in the
  // realization file is left blank rather than shown as "no" -- absence there
  // means "not checked", and the two must not read the same.
  var REAL_COLS = [
    { k: "rfile", t: "תיק רישוי", get: function (r) { var v = realOf(r.plan); return v ? (v.has_licensing_file ? 1 : 0) : null; },
      fmt: function (v) { return v == null ? '<span class="empty">?</span>' : yn(v); } },
    { k: "rissued", t: "היתר", get: function (r) { var v = realOf(r.plan); return v ? (v.any_issued ? 1 : 0) : null; },
      fmt: function (v) { return v == null ? '<span class="empty">?</span>' : yn(v); } },
    { k: "rbuild", t: "בביצוע", get: function (r) { var v = realOf(r.plan); return v ? (v.in_construction ? 1 : 0) : null; },
      fmt: function (v) { return v == null ? '<span class="empty">?</span>' : yn(v); } },
    { k: "rdone", t: "גמר", get: function (r) { var v = realOf(r.plan); return v ? (v.completed ? 1 : 0) : null; },
      fmt: function (v) { return v == null ? '<span class="empty">?</span>' : yn(v); } }
  ];
  var realDetail = function (plan) {
    var v = realOf(plan);
    if (!v) return '<div class="empty">מצב המימוש לא נבדק לתכנית זו.</div>';
    var bits = [];
    if (v.permit_count) bits.push("תיקי רישוי: " + v.permit_count);
    if (v.stage) bits.push("שלב: " + esc(v.stage));
    if (v.form4_date) bits.push("טופס 4: " + esc(v.form4_date));
    if (v.occupied) bits.push("מאוכלס");
    return bits.length ? "<div><b>מימוש:</b> " + bits.join(" · ") + "</div>" : "";
  };

  // Table-5 unit counts have three outcomes and they must stay distinguishable:
  // a number, "the export has no such row" (—), and "Mavat never exported a table"
  // (?). Collapsing the last two into 0 would state a fact the data does not carry.
  // Three states, never collapsed: a count, a table with no such row, and a plan
  // Mavat refuses to export. A value read off the PDF by eye carries a dagger, so a
  // hand read is never mistaken for the export's own sum across the whole table.
  // Where a count came from, marked in the cell itself. Three sources of decreasing
  // authority: Mavat's own xlsx export of Table 5 (unmarked), a hand read of the table
  // page for the plans it will not export (†), and an explicit sentence in the הוראות
  // for plans whose table has no rental row at all (‡). A reader must be able to see
  // which figures rest on a machine-readable table and which on a reading.
  var SRC_MARK = {
    visual: ["†", "נקרא ידנית מעמוד טבלה 5 ב-PDF — מבא\"ת לא מייצא טבלה לתכנית זו"],
    prose:  ["‡", "נאמר במפורש בהוראות התכנית — בטבלה 5 אין שורת מגורים להשכרה"],
    pct:    ["*", "מחושב: אחוז ההשכרה שבהוראות כפול סך יח\"ד המגורים בטבלה 5. מספר גזור, לא מספר שהתכנית נוקבת בו"]
  };
  var unitCell = function (v, state, read, note, dispute) {
    if (v) {
      var mk = SRC_MARK[read];
      return '<span class="hi">' + n0(v) + "</span>" +
        (mk ? '<sup class="vis" title="' + mk[1] + '">' + mk[0] + "</sup>" : "") +
        (note ? '<sup class="vis dup" title="' + note + '">≡</sup>' : "");
    }
    if (dispute)
      return '<span class="empty dup" title="' + dispute + '">—</span>';
    if (state === "no_export")
      return '<span class="empty" title="מבא\"ת לא מייצא טבלה 5 לתכנית זו">?</span>';
    var t = read === "visual" ? "נבדק ידנית בטבלה 5 — אין שורה כזו" : "אין שורה כזו בטבלה 5";
    return '<span class="empty" title="' + t + '">—</span>';
  };

  // Status colours MIRROR the single source in src/app.jsx (STATUS_GROUP_DEFS).
  // Kept as a copy only because this is a separate site that does not load the app
  // bundle — the keys, labels and colours must not drift from it, and a report must
  // never invent its own status map. If the app's groups change, change them here too.
  var STATUS_GROUPS = [
    { key: "approved",    label: "אישור / מאושרת",      color: "#50d25a",
      match: ["אישור", "מאושרת", "תבע מאושרת", "תחילת תוקף"] },
    { key: "in_approval", label: "בהליך אישור",         color: "#50d25a",
      match: ["בהליך אישור", "תבע - טרום אישור"] },
    { key: "objections",  label: "התנגדויות",           color: "#fafa3c",
      match: ["דיון בהתנגדויות ותיקונים", "הכרעה בהתנגדויות / אישור"] },
    { key: "deposit",     label: "הפקדה",               color: "#fafa3c",
      match: ["הפקדה להתנגדויות/השגות"] },
    { key: "conditions",  label: "במילוי תנאים להפקדה", color: "#f56e05",
      match: ["במילוי תנאים להפקדה"] },
    { key: "open",        label: "פתיחת תיק / בבדיקה",  color: "#eb0000",
      match: ["נפתח תיק למתכנן", "נפתח תיק תב\"ע", "נפתח תיק תבע", "בבדיקה תכנונית",
              "נקלטה מקובץ מבאת", "נקלטה", "תכנית עומדת בתנאי סף", "בבדיקת תנאי סף",
              "הכנת הודעה 77/78"] },
    { key: "rejected",    label: "נגנזה / נדחתה",       color: "#888888",
      match: ["נגנזה", "נדחתה", "נגנזה/נדחתה"] },
    { key: "other",       label: "אחר",                 color: "#b0b0b0", match: [] }
  ];
  var _STATUS_IDX = (function () {
    var m = {};
    STATUS_GROUPS.forEach(function (g) {
      g.match.forEach(function (x) { m[x] = g; m[x.replace(/\s+/g, "")] = g; });
    });
    return m;
  })();
  var statusGroup = function (st) {
    if (!st) return STATUS_GROUPS[STATUS_GROUPS.length - 1];
    return _STATUS_IDX[st] || _STATUS_IDX[String(st).replace(/\s+/g, "")] ||
           STATUS_GROUPS[STATUS_GROUPS.length - 1];
  };

  var pad7 = function (t) {
    return "101-" + String(t).split("-").pop().replace(/\D/g, "").padStart(7, "0");
  };

  /* ---------------- the three reports ---------------- */
  var REPORTS = {
    ce: {
      title: "מסחר, תעסוקה ומשרדים", tag: "ce", color: "var(--ce)", file: "מסחר_ותעסוקה",
      note: "<b>איך לקרוא:</b> השטחים מגיעים מטבלה 5. תכנית שלא נמצאה לה טבלה 5 קריאה אינה מופיעה כאן — זה <b>לא נמצא</b> ולא <b>אין מסחר</b>. עמודת הלא-מפוצל היא שטח שטבלה 5 מאחדת ולא ניתן לייחס למסחר או לתעסוקה בנפרד.",
      rows: function () {
        return Object.keys(D.ce.by_plan).map(function (k) {
          var v = D.ce.by_plan[k];
          var tot = (v.commerce_sqm || 0) + (v.employment_sqm || 0) +
                    (v.mixed_other_sqm || 0) + (v.ce_unsplit_sqm || 0);
          return { plan: pad7(k), name: v.pl_name, status: v.station_desc,
                   sub_neighborhood: null, source: "text", ce: v, total: tot };
        }).filter(function (r) { return r.total > 0; });
      },
      cols: [
        { k: "plan", t: "תכנית", get: function (r) { return r.plan; } },
        { k: "name", t: "שם התכנית", get: function (r) { return r.name || ""; } },
        { k: "status", t: "סטטוס", get: function (r) { return r.status || ""; } },
        { k: "com", t: "מסחר (מ\"ר)", n: true, get: function (r) { return r.ce.commerce_sqm || 0; },
          fmt: function (v) { return v ? '<span class="hi">' + n0(v) + "</span>" : '<span class="empty">—</span>'; } },
        { k: "emp", t: "תעסוקה (מ\"ר)", n: true, get: function (r) { return r.ce.employment_sqm || 0; },
          fmt: function (v) { return v ? '<span class="hi">' + n0(v) + "</span>" : '<span class="empty">—</span>'; } },
        { k: "uns", t: "לא מפוצל", n: true, get: function (r) { return r.ce.ce_unsplit_sqm || 0; },
          fmt: function (v) { return v ? n0(v) : '<span class="empty">—</span>'; } },
        // Government offices. Table 5 designates these "מבנים ומוסדות ציבור
        // למינהל ציבורי" -- a PUBLIC designation -- so they are their own column
        // and are never added into תעסוקה.
        { k: "adm", t: "מינהל ציבורי", n: true, get: function (r) { return r.ce.public_admin_sqm || 0; },
          fmt: function (v) { return v ? n0(v) : '<span class="empty">—</span>'; } },
        // CE combined with a use that is neither (residential/public/tourism):
        // reported so the gap is visible, never attributed to either side.
        { k: "mix", t: "מעורב", n: true, get: function (r) { return r.ce.mixed_other_sqm || 0; },
          fmt: function (v) { return v ? n0(v) : '<span class="empty">—</span>'; } }
      ].concat(REAL_COLS),
      detail: function (r) {
        var h = "";
        if (r.ce.split_source) h += "<div><b>מקור הפיצול:</b> " + esc(r.ce.split_source) + "</div>";
        if (r.ce.split_note) h += '<div class="quote">' + esc(r.ce.split_note) + "</div>";
        if (r.ce.t5_status && r.ce.t5_status !== "success")
          h += '<div class="empty">טבלה 5: ' + esc(r.ce.t5_status) + "</div>";
        h += realDetail(r.plan);
        h += realDetail(r.plan);
        return h || '<div class="empty">אין פירוט נוסף.</div>';
      },
      kpis: function (rows) {
        var com = rows.reduce(function (a, r) { return a + (r.ce.commerce_sqm || 0); }, 0);
        var emp = rows.reduce(function (a, r) { return a + (r.ce.employment_sqm || 0); }, 0);
        var uns = rows.reduce(function (a, r) { return a + (r.ce.ce_unsplit_sqm || 0); }, 0);
        var mix = rows.reduce(function (a, r) { return a + (r.ce.mixed_other_sqm || 0); }, 0);
        var adm = rows.reduce(function (a, r) { return a + (r.ce.public_admin_sqm || 0); }, 0);
        var appr = rows.filter(function (r) { return r.status === "אישור"; });
        var cnt = function (f) {
          return appr.filter(function (r) { var v = realOf(r.plan); return v && v[f]; }).length;
        };
        var k = [
          { n: n0(rows.length), l: "תכניות עם שטחי מסחר/תעסוקה" },
          { n: short(com) + ' מ"ר', l: "סך מסחר" },
          { n: short(emp) + ' מ"ר', l: "סך תעסוקה" }
        ];
        if (adm) k.push({ n: short(adm) + ' מ"ר', l: "מינהל ציבורי (ייעוד ציבורי)" });
        if (uns) k.push({ n: short(uns) + ' מ"ר', l: "מסחר+תעסוקה יחד, לא מפוצל" });
        if (mix) k.push({ n: short(mix) + ' מ"ר', l: "מעורב עם שימוש שאינו CE" });
        k.push({ n: n0(appr.length), l: "מאושרות" });
        k.push({ n: n0(cnt("has_licensing_file")), l: "בעלות תיק רישוי" });
        k.push({ n: n0(cnt("in_construction")), l: "בביצוע" });
        k.push({ n: n0(cnt("completed")), l: "גמר בנייה" });
        return k;
      }
    },
    fund: {
      title: "קרן תחזוקה ודיור מותנה", tag: "fund", color: "var(--fund)", file: "קרן_תחזוקה",
      note: "<b>סך הקרנות הוא תקרה, לא סכום מחויב.</b> ההוראות קובעות במפורש ש\"מימוש זכויות הבנייה ה'מותנות' נתון לבחירת מגיש הבקשה להיתר ומימושן אינו מחייב\" — כלומר הסכום מתממש רק ככל שהיזם בוחר לממש את הזכויות המותנות. " +
            "היעדר תכנית פירושו <b>לא נמצא</b> ולא <b>אין</b>, וסכומי הקרן כולם מגיעים משכבת טקסט ולא מ-OCR. מספרי יח\"ד מגיעים מייצוא האקסל של טבלה 5: <b>—</b> = אין שורה כזו בטבלה (החובה מנוסחת באחוזים בהוראות), <b>?</b> = מבא\"ת לא מייצא טבלה 5 לתכנית זו.",
      rows: function () {
        return D.housing.rows.filter(function (r) { return r.fund; });
      },
      cols: [
        { k: "plan", t: "תכנית", get: function (r) { return r.plan; } },
        { k: "name", t: "שם התכנית", get: function (r) { return r.name || ""; } },
        { k: "status", t: "סטטוס", get: function (r) { return r.status || ""; } },
        { k: "sub", t: "שכונה", get: function (r) { return r.sub_neighborhood || ""; } },
        { k: "floors", t: "קומות", n: true, get: function (r) { return r.fund.floors; } },
        { k: "cond", t: 'יח"ד מותנות', n: true,
          get: function (r) { return r.fund.conditional_units_t5; },
          fmt: function (v, r) { return unitCell(v, r.fund.conditional_units_state, r.fund.conditional_units_read); } },
        { k: "amount", t: "גובה הקרן", n: true, get: function (r) { return r.fund.amount_ils; },
          fmt: function (v) { return v ? '<span class="hi">' + ils(v) + "</span>" : '<span class="empty">לא צוין</span>'; } },
        { k: "section", t: "סעיף", get: function (r) { return r.fund.section || ""; } }
      ].concat(REAL_COLS),
      detail: function (r) {
        var h = "";
        if (r.fund.mechanism) h += '<div class="quote"><b>המנגנון:</b> ' + esc(r.fund.mechanism) + "</div>";
        if (r.fund.amount_text) h += '<div class="quote"><b>ציטוט הסכום:</b> ' + esc(r.fund.amount_text) + "</div>";
        h += realDetail(r.plan);
        return h || '<div class="empty">אין ציטוט שנשמר.</div>';
      },
      kpis: function (rows) {
        var w = rows.filter(function (r) { return r.fund.amount_ils; });
        var tot = w.reduce(function (a, r) { return a + r.fund.amount_ils; }, 0);
        var s = w.map(function (r) { return r.fund.amount_ils; }).sort(function (a, b) { return a - b; });
        return [
          { n: n0(rows.length), l: "תכניות עם קרן תחזוקה" },
          { n: "₪" + short(tot), l: "סך הקרנות (" + w.length + " עם סכום)" },
          { n: s.length ? "₪" + short(s[Math.floor(s.length / 2)]) : "—", l: "חציון גובה הקרן" },
          { n: n0(rows.reduce(function (a, r) { return a + (r.fund.conditional_units_t5 || 0); }, 0)),
            l: 'יח"ד מותנות (' + rows.filter(function (r) { return r.fund.conditional_units_t5; }).length + " תכניות)" }
        ];
      }
    },
    rental: {
      title: "דיור להשכרה", tag: "rent", color: "var(--rent)", file: "דיור_להשכרה",
      note: "<b>איך לקרוא:</b> היעדר תכנית פירושו <b>לא נמצא</b> ולא <b>אין</b>. הרשימה כוללת רק תכניות המחייבות השכרה — 15 תכניות שבהן ההשכרה היא שימוש מותר בלבד הוצאו בסקירה ידנית. כל משך שמקורו ב-OCR אומת בקריאה ויזואלית של המסמך. מספרי יח\"ד מגיעים מייצוא האקסל של טבלה 5: <b>—</b> = אין שורה כזו בטבלה (החובה מנוסחת באחוזים בהוראות), <b>?</b> = מבא\"ת לא מייצא טבלה 5 לתכנית זו.",
      rows: function () {
        return D.housing.rows.filter(function (r) { return r.rental; });
      },
      cols: [
        { k: "plan", t: "תכנית", get: function (r) { return r.plan; } },
        { k: "name", t: "שם התכנית", get: function (r) { return r.name || ""; } },
        { k: "status", t: "סטטוס", get: function (r) { return r.status || ""; } },
        { k: "runits", t: 'יח"ד להשכרה', n: true,
          get: function (r) { return r.rental.units_t5; },
          fmt: function (v, r) { return unitCell(v, r.rental.units_state, r.rental.units_read,
              r.rental.restates_plan && ("אותן יח\"ד נקבעו כבר בתכנית " +
                r.rental.restates_plan + " — לא נספרות פעמיים במניין העירוני"),
              r.rental.units_disputed); } },
        // Special housing gets its own column rather than being folded into the
        // rental count. The two are different commitments that share a zone in
        // Jerusalem, and folding them together is exactly what put 929 units in the
        // rental figure on no basis.
        // Disjoint from the rental column by construction: a row whose use is rental
        // is counted as rental, and a zone the הוראות define as rental moves there
        // whole. The two columns can therefore be added without counting a flat twice.
        { k: "spec", t: 'יח"ד דיור מיוחד', n: true,
          get: function (r) { return r.rental.special_units; },
          fmt: function (v, r) {
            if (!v)
              return '<span class="empty" title="' +
                (r.rental.special_moved_to_rental
                   ? 'יח\"ד הדיור המיוחד בתכנית זו הן יחידות ההשכרה עצמן, ולכן נספרות בעמודת ההשכרה בלבד'
                   : "אין שורת דיור מיוחד בטבלה 5") + '">—</span>';
            return '<span class="hi">' + n0(v) + "</span>";
          } },
        { k: "dur", t: "משך ההשכרה", get: function (r) { return r.rental.duration; },
          fmt: function (v) {
            return v ? '<span class="hi">' + (isNaN(+v) ? esc(v) : v + " שנים") + "</span>"
                     : '<span class="empty">לא צוין</span>';
          } },
        { k: "sub", t: "שכונה", get: function (r) { return r.sub_neighborhood || ""; } },
        { k: "dsrc", t: "מקור המשך", get: function (r) { return r.rental.duration_source || ""; } },
        { k: "flag", t: "לבדיקה", get: function (r) { return r.rental.review ? 1 : 0; },
          fmt: function (v) { return v ? '<span class="pill ocr">שימוש מותר?</span>' : ""; } }
      ].concat(REAL_COLS),
      detail: function (r) {
        var h = "";
        if (r.rental.units_quote)
          h += '<div class="quote"><b>' +
               (r.rental.units_read === "pct" ? "הציטוט שממנו חושב מספר היח\"ד"
                                             : "הציטוט שממנו נקבע מספר היח\"ד") +
               ':</b> ' + esc(r.rental.units_quote) +
               (r.rental.units_read === "pct"
                 ? ' <span class="empty">(' + r.rental.units_pct + "% × " +
                   n0(r.rental.units_base) + ' יח"ד מגורים בטבלה 5 = ' +
                   n0(r.rental.units_t5) + ")</span>"
                 : "") + "</div>";
        if (r.rental.units_disputed)
          h += '<div class="quote"><b>למה אין כאן מספר:</b> ' +
               esc(r.rental.units_disputed) + "</div>";
        if (r.rental.special_moved_to_rental)
          h += '<div class="quote"><b>דיור מיוחד = דיור להשכרה בתכנית זו:</b> ' +
               esc(r.rental.special_moved_to_rental) +
               ' — לכן היח"ד נספרות בעמודת ההשכרה ולא בעמודת הדיור המיוחד.</div>';
        if ((r.rental.special_rows || []).length)
          h += '<div class="quote"><b>שורות דיור מיוחד בטבלה 5:</b> ' +
               esc(r.rental.special_rows.join("  |  ")) + "</div>";
        if (r.rental.restates_plan)
          h += '<div class="quote"><b>שימו לב:</b> התכנית מחלקת מחדש את יחידות ההשכרה ' +
               'שנקבעו בתכנית ' + esc(r.rental.restates_plan) +
               ' — אותן יחידות, ולכן הן נספרות פעם אחת בלבד במניין העירוני.</div>';
        (r.rental.evidence || []).forEach(function (e) {
          var m = /^([RPD])(\[[^\]]*\])?:/.exec(e), lab = "";
          if (m) {
            lab = m[1] === "D" ? "הציטוט שממנו נקבע המשך"
                : m[1] === "R" ? "אזכור ההשכרה" : "אזכור דיור מוגן";
            e = e.slice(m[0].length);
          }
          h += '<div class="quote">' + (lab ? "<b>" + lab + ":</b> " : "") + esc(e) + "</div>";
        });
        h += realDetail(r.plan);
        return h || '<div class="empty">אין ציטוט שנשמר.</div>';
      },
      kpis: function (rows) {
        var expl = rows.filter(function (r) { return r.rental.duration_source === "הוראות התכנית"; });
        var stat = rows.filter(function (r) { return r.rental.duration_source === "תוספת שישית"; });
        return [
          { n: n0(rows.length), l: "תכניות עם חובת השכרה" },
          // Units of a plan that only re-divides another plan's rental quota are left
          // out of the sum: 101-1122241 splits the same 120 units 101-0969162 already
          // fixed, and adding both would invent 120 rental units in the city.
          { n: n0(rows.reduce(function (a, r) {
              return a + (r.rental.restates_plan ? 0 : (r.rental.units_t5 || 0)); }, 0)),
            l: 'יח"ד להשכרה (' + rows.filter(function (r) { return r.rental.units_t5; }).length + " תכניות)" },
          // Derived figures shown apart as well as inside the sum: a third of the
          // citywide total is arithmetic on a percentage, and a reader deciding what to
          // rely on needs to see how much of the number that is.
          { n: n0(rows.reduce(function (a, r) {
              return a + (r.rental.units_read === "pct" ? (r.rental.units_t5 || 0) : 0); }, 0)),
            l: "מתוכם מחושבים מאחוז (" +
               rows.filter(function (r) { return r.rental.units_read === "pct"; }).length +
               " תכניות)" },
          // Rental and special housing never share a unit, so this addition is safe —
          // and it is stated as its own KPI precisely so nobody adds the two columns by
          // hand and wonders whether they overlap.
          { n: n0(rows.reduce(function (a, r) {
              return a + (r.rental.restates_plan ? 0 : (r.rental.units_t5 || 0))
                       + (r.rental.special_units || 0); }, 0)),
            l: 'סה"כ יח"ד להשכרה + דיור מיוחד (ללא כפילות)' },
          { n: n0(expl.length), l: "משך מפורש בהוראות" },
          { n: n0(stat.length), l: "משך מהתוספת השישית" }
        ];
      }
    }
  };

  /* ---------------- filtering ---------------- */
  function visible() {
    var R = REPORTS[state.tab], rows = R.rows();
    var q = state.q.trim();
    if (q) rows = rows.filter(function (r) {
      return ((r.plan || "") + " " + (r.name || "") + " " + (r.sub_neighborhood || "")).indexOf(q) !== -1;
    });
    if (state.status) rows = rows.filter(function (r) { return r.status === state.status; });
    if (state.src) rows = rows.filter(function (r) { return r.source === state.src; });
    if (state.sort) {
      var col = R.cols.filter(function (c) { return c.k === state.sort; })[0];
      if (col) rows = rows.slice().sort(function (a, b) {
        var x = col.get(a), y = col.get(b);
        if (col.n) { x = x == null ? -Infinity : +x; y = y == null ? -Infinity : +y; return state.desc ? y - x : x - y; }
        x = String(x || ""); y = String(y || "");
        return state.desc ? y.localeCompare(x, "he") : x.localeCompare(y, "he");
      });
    }
    return rows;
  }

  /* ---------------- map ---------------- */
  function initMap() {
    if (map || !window.L) return;
    map = L.map("cmap", { center: CENTER, zoom: ZOOM, zoomControl: false,
                          maxZoom: 19, minZoom: 10 });
    L.control.zoom({ position: "topleft" }).addTo(map);
    // Basemap lifted from commerce.js: CARTO watermarks keyless tiles, so this uses
    // OpenFreeMap via MapLibre, and the RTL plugin is what stops Hebrew labels
    // rendering reversed.
    if (L.maplibreGL) {
      try {
        if (window.maplibregl && maplibregl.getRTLTextPluginStatus &&
            maplibregl.getRTLTextPluginStatus() === "unavailable") {
          maplibregl.setRTLTextPlugin(
            "https://unpkg.com/@mapbox/mapbox-gl-rtl-text@0.2.3/mapbox-gl-rtl-text.js", null, true);
        }
      } catch (e) { /* labels stay latin; not fatal */ }
      var gl = L.maplibreGL({
        style: "https://tiles.openfreemap.org/styles/bright",
        attribution: "© OpenFreeMap © OpenMapTiles © OpenStreetMap contributors",
        interactive: false
      }).addTo(map);
      var gm = gl.getMaplibreMap && gl.getMaplibreMap();
      if (gm) {
        var heb = function () {
          try {
            (gm.getStyle().layers || []).forEach(function (l) {
              if (l.type !== "symbol") return;
              var tf = gm.getLayoutProperty(l.id, "text-field");
              if (tf === undefined || JSON.stringify(tf).indexOf("name") === -1) return;
              gm.setLayoutProperty(l.id, "text-field",
                ["coalesce", ["get", "name:he"], ["get", "name:nonlatin"], ["get", "name"]]);
            });
          } catch (e) { /* style not ready */ }
        };
        gm.on("style.load", heb);
      }
    } else {
      L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
                  { maxZoom: 19, attribution: "© OpenStreetMap contributors" }).addTo(map);
    }
  }

  // Two indexes, not one merged bag. The blue line IS the plan; a parcel is a piece
  // inside it, and the parcels layer only holds the pieces the commerce pipeline
  // extracted. Merging them let 101-1284223 — a 195-dunam plan — draw as three small
  // parcels covering a third of its width, which reads as a different plan entirely.
  // So: blue line wherever we have one, parcels only as a fallback.
  var boundsByPlan = {}, parcelsByPlan = {};
  function indexGeometry() {
    boundsByPlan = {}; parcelsByPlan = {}; geoByPlan = {};
    function add(bag, src) {
      if (!src || !src.features) return;
      src.features.forEach(function (f) {
        var key = f.properties && (f.properties.plan_name || f.properties.taba);
        if (!key) return;
        (bag[pad7(key)] = bag[pad7(key)] || []).push(f);
      });
    }
    add(boundsByPlan, D.bounds);
    add(parcelsByPlan, D.parcels);
    Object.keys(parcelsByPlan).forEach(function (k) { geoByPlan[k] = parcelsByPlan[k]; });
    Object.keys(boundsByPlan).forEach(function (k) { geoByPlan[k] = boundsByPlan[k]; });
  }

  function drawMap(rows) {
    if (!map) return;
    if (layer) { map.removeLayer(layer); layer = null; }
    var color = getComputedStyle(document.documentElement)
                  .getPropertyValue("--accent").trim() || "#c084fc";
    var feats = [];
    rows.forEach(function (r) {
      (geoByPlan[r.plan] || []).forEach(function (f) {
        feats.push({ type: "Feature", geometry: f.geometry,
                     properties: { plan: r.plan, name: r.name || "", st: r.status || "",
                                   stColor: statusGroup(r.status).color,
                                   stLabel: statusGroup(r.status).label } });
      });
    });
    if (!feats.length) return;
    drawLegend(rows);
    layer = L.geoJSON({ type: "FeatureCollection", features: feats }, {
      style: function (f) {
        // Outline carries the plan's STATUS, fill keeps the tab's own colour. Two
        // independent facts, two independent channels: which report you are reading,
        // and how far along the plan is.
        var on = state.sel === f.properties.plan;
        return { color: f.properties.stColor, weight: on ? 4 : 2, opacity: on ? 1 : .9,
                 fillColor: color, fillOpacity: on ? .45 : .16 };
      },
      onEachFeature: function (f, lyr) {
        lyr.bindPopup('<div style="font-weight:700">' + esc(f.properties.name || f.properties.plan) +
                      "</div><div>" + esc(f.properties.plan) + "</div>" +
                      (f.properties.st ? '<div style="margin-top:3px"><span style="display:inline-block;' +
                        'width:9px;height:9px;border-radius:2px;background:' + f.properties.stColor +
                        ';margin-left:5px"></span>' + esc(f.properties.st) + "</div>" : ""));
        lyr.on("click", function () { select(f.properties.plan, false); });
      }
    }).addTo(map);
  }


  var legendCtl = null;
  function drawLegend(rows) {
    if (!map) return;
    if (legendCtl) { map.removeControl(legendCtl); legendCtl = null; }
    var present = [], seen = {};
    rows.forEach(function (r) {
      if (!geoByPlan[r.plan]) return;           // only what is actually drawn
      var g = statusGroup(r.status);
      if (!seen[g.key]) { seen[g.key] = 1; present.push(g); }
    });
    if (!present.length) return;
    var order = {};
    STATUS_GROUPS.forEach(function (g, i) { order[g.key] = i; });
    present.sort(function (a, b) { return order[a.key] - order[b.key]; });
    legendCtl = L.control({ position: "bottomright" });
    legendCtl.onAdd = function () {
      var d = L.DomUtil.create("div", "maplegend");
      d.innerHTML = '<div style="font-weight:700;margin-bottom:4px">קו המתאר — סטטוס</div>' +
        present.map(function (g) {
          return '<div><span style="display:inline-block;width:14px;height:0;border-top:3px solid ' +
                 g.color + ';vertical-align:middle;margin-left:6px"></span>' + esc(g.label) + "</div>";
        }).join("");
      return d;
    };
    legendCtl.addTo(map);
  }

  function select(plan, zoom) {
    state.sel = state.sel === plan ? null : plan;
    render();
    if (state.sel && zoom !== false && map && geoByPlan[state.sel]) {
      var b = L.geoJSON({ type: "FeatureCollection", features: geoByPlan[state.sel] }).getBounds();
      if (b.isValid()) map.fitBounds(b, { padding: [40, 40], maxZoom: 17 });
    }
    var row = document.querySelector('tr.row[data-plan="' + state.sel + '"]');
    if (row && row.scrollIntoView) row.scrollIntoView({ block: "nearest" });
  }

  /* ---------------- export / deep link ---------------- */
  function csv() {
    var R = REPORTS[state.tab], rows = visible();
    var lines = [R.cols.map(function (c) { return c.t; }).concat(["מקור קריאה"]).join(",")];
    rows.forEach(function (r) {
      var cells = R.cols.map(function (c) {
        return '"' + String(c.get(r) == null ? "" : c.get(r)).replace(/"/g, '""') + '"';
      });
      cells.push('"' + (r.source === "ocr" ? "OCR" : "טקסט") + '"');
      lines.push(cells.join(","));
    });
    var blob = new Blob(["﻿" + lines.join("\r\n")], { type: "text/csv;charset=utf-8;" });
    var a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = R.file + "_" + new Date().toISOString().slice(0, 10) + ".csv";
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 1000);
  }

  function writeHash() {
    var p = [state.tab];
    if (state.q) p.push("q=" + encodeURIComponent(state.q));
    if (state.status) p.push("st=" + encodeURIComponent(state.status));
    if (state.src) p.push("src=" + state.src);
    if (state.sort) p.push("sort=" + state.sort + (state.desc ? ":d" : ":a"));
    if (state.sel) p.push("sel=" + state.sel);
    history.replaceState(null, "", "#" + p.join("&"));
  }
  function readHash() {
    var h = (location.hash || "").replace(/^#/, "");
    if (!h) return;
    var parts = h.split("&");
    if (REPORTS[parts[0]]) state.tab = parts[0];
    parts.slice(1).forEach(function (kv) {
      var i = kv.indexOf("="); if (i < 0) return;
      var k = kv.slice(0, i), v = decodeURIComponent(kv.slice(i + 1));
      if (k === "q") state.q = v; else if (k === "st") state.status = v;
      else if (k === "src") state.src = v; else if (k === "sel") state.sel = v;
      else if (k === "sort") { var s = v.split(":"); state.sort = s[0]; state.desc = s[1] !== "a"; }
    });
  }

  /* ---------------- render ---------------- */
  function render() {
    var R = REPORTS[state.tab], rows = visible();
    document.documentElement.style.setProperty("--accent", R.color);
    var nogeo = rows.filter(function (r) { return !geoByPlan[r.plan]; }).length;

    var statuses = {};
    R.rows().forEach(function (r) { if (r.status) statuses[r.status] = 1; });

    var kpis = R.kpis(rows).map(function (k) {
      return '<div class="kpi"><div class="n">' + esc(k.n) + '</div><div class="l">' +
             esc(k.l) + "</div></div>";
    }).join("");

    var head = R.cols.map(function (c) {
      return '<th data-sort="' + c.k + '">' + esc(c.t) +
             (state.sort === c.k ? (state.desc ? " ↓" : " ↑") : "") + "</th>";
    }).join("") + "<th>מקור</th>";

    var body = rows.map(function (r) {
      var tds = R.cols.map(function (c) {
        var v = c.get(r);
        // Pass the row too: a cell may need sibling fields to render honestly (the
        // unit counts distinguish "no such row" from "no export", which the value
        // alone cannot express).
        return '<td class="' + (c.n ? "num" : "") + '">' + (c.fmt ? c.fmt(v, r) : esc(v)) + "</td>";
      }).join("");
      tds += "<td>" + (r.source === "ocr" ? '<span class="pill ocr">OCR</span>'
                                          : '<span class="pill stat">טקסט</span>') +
             (geoByPlan[r.plan] ? "" : ' <span class="pill nogeo">ללא מיקום</span>') + "</td>";
      var out = '<tr class="row' + (state.sel === r.plan ? " sel" : "") +
                '" data-plan="' + esc(r.plan) + '">' + tds + "</tr>";
      if (state.sel === r.plan) {
        out += '<tr class="detail"><td colspan="' + (R.cols.length + 1) + '">' + R.detail(r) +
               '<div style="margin-top:7px" class="noprint">' +
               (r.mavat_url ? '<a class="btn" target="_blank" rel="noopener" href="' +
                 esc(r.mavat_url) + '">פתיחה במבא"ת ↗</a>' : "") + "</div></td></tr>";
      }
      return out;
    }).join("");

    // The map container must survive re-renders. Rebuilding #root wholesale removed
    // the div Leaflet had attached to, while initMap() saw `map` already set and
    // returned early — so the map drew once and vanished on the next filter, search
    // or tab switch. The skeleton is therefore built once and only its parts refresh.
    if (!document.getElementById("cmap")) {
      document.getElementById("root").innerHTML =
        '<header id="hdr"></header><div class="kpis" id="kpis"></div>' +
        '<div class="bar noprint" id="bar"></div><div id="notes"></div>' +
        '<div class="split"><div class="list" id="list"></div><div id="cmap"></div></div>';
    }

    document.getElementById("hdr").innerHTML =
      "<h1>" + esc(R.title) + "</h1>" +
      '<div class="sub">כלל עירוני — ירושלים · ' +
        (D.housing ? "ארכיון הוראות: " + n0(D.housing.totals.read_text + D.housing.totals.read_ocr) +
          " מסמכים נקראו" : "") + "</div>" +
      '<div class="tabs noprint">' +
        '<button class="tab ce' + (state.tab === "ce" ? " on" : "") + '" data-tab="ce">מסחר ותעסוקה</button>' +
        '<button class="tab fund' + (state.tab === "fund" ? " on" : "") + '" data-tab="fund">קרן תחזוקה</button>' +
        '<button class="tab rent' + (state.tab === "rental" ? " on" : "") + '" data-tab="rental">דיור להשכרה</button>' +
      "</div>";

    document.getElementById("kpis").innerHTML = kpis;

    document.getElementById("bar").innerHTML =
        '<input id="q" placeholder="חיפוש: מספר תכנית, שם…" value="' + esc(state.q) + '">' +
        '<select id="fst"><option value="">כל הסטטוסים</option>' +
          Object.keys(statuses).sort(function (a, b) { return a.localeCompare(b, "he"); })
            .map(function (v) { return '<option' + (v === state.status ? " selected" : "") +
              ">" + esc(v) + "</option>"; }).join("") + "</select>" +
        (state.tab === "ce" ? "" :
          '<select id="fsrc"><option value="">טקסט + OCR</option>' +
          '<option value="text"' + (state.src === "text" ? " selected" : "") + ">טקסט בלבד</option>" +
          '<option value="ocr"' + (state.src === "ocr" ? " selected" : "") + ">OCR בלבד</option></select>") +
        '<span class="spacer"></span>' +
        (nogeo ? '<span class="warnpill">' + nogeo + " ללא מיקום — ברשימה בלבד</span>" : "") +
        '<span style="color:var(--dim);font-size:12.5px">' + n0(rows.length) + " תכניות</span>" +
        '<button class="btn" id="csv">ייצוא לאקסל</button>' +
        '<button class="btn" id="prn">הדפסה</button>' +
        '<button class="btn" id="lnk">🔗 קישור</button>';

    document.getElementById("notes").innerHTML =
      (R.note ? '<div class="note">' + R.note + "</div>" : "") +
      (mapNote ? '<div class="note">' + mapNote + "</div>" : "");

    document.getElementById("list").innerHTML =
      "<table><thead><tr>" + head + "</tr></thead><tbody>" +
      (rows.length ? body :
        '<tr><td colspan="' + (R.cols.length + 1) +
        '" style="padding:24px;text-align:center;color:var(--faint)">אין תכניות התואמות את הסינון</td></tr>') +
      "</tbody></table>";

    wire();
    initMap();
    drawMap(rows);
    if (map) setTimeout(function () { map.invalidateSize(); }, 0);
    writeHash();
  }

  function wire() {
    var on = function (id, ev, fn) { var el = document.getElementById(id); if (el) el.addEventListener(ev, fn); };
    [].forEach.call(document.querySelectorAll(".tab"), function (b) {
      b.addEventListener("click", function () {
        state.tab = b.getAttribute("data-tab");
        state.sort = ""; state.sel = null; state.status = ""; state.src = "";
        render();
      });
    });
    var q = document.getElementById("q");
    if (q) q.addEventListener("input", function () {
      state.q = q.value; var p = q.selectionStart; render();
      var nq = document.getElementById("q"); nq.focus(); nq.setSelectionRange(p, p);
    });
    on("fst", "change", function (e) { state.status = e.target.value; render(); });
    on("fsrc", "change", function (e) { state.src = e.target.value; render(); });
    on("csv", "click", csv);
    on("prn", "click", function () { window.print(); });
    on("lnk", "click", function () {
      var el = document.getElementById("lnk");
      navigator.clipboard.writeText(location.href).then(function () {
        el.textContent = "✓ הועתק";
        setTimeout(function () { el.textContent = "🔗 קישור"; }, 1500);
      });
    });
    [].forEach.call(document.querySelectorAll("th[data-sort]"), function (th) {
      th.addEventListener("click", function () {
        var k = th.getAttribute("data-sort");
        if (state.sort === k) state.desc = !state.desc; else { state.sort = k; state.desc = true; }
        render();
      });
    });
    [].forEach.call(document.querySelectorAll("tr.row"), function (tr) {
      tr.addEventListener("click", function () { select(tr.getAttribute("data-plan")); });
    });
  }

  /* ---------------- load ---------------- */
  function getJSON(url, optional) {
    return fetch(url + (location.protocol === "file:" ? "" : "?t=" + Date.now()),
                 { cache: "no-store" })
      .then(function (r) { if (!r.ok) throw new Error("HTTP " + r.status); return r.json(); })
      .catch(function (e) {
        if (url === HOUSING) {
          // Same file:// fallback as housing.html: fetch is blocked there, a script
          // tag is not. Only the housing payload has a JS twin.
          return new Promise(function (res, rej) {
            var s = document.createElement("script");
            s.src = url.replace(/\.json$/, ".js");
            s.onload = function () {
              window.__HOUSING_TERMS__ ? res(window.__HOUSING_TERMS__) : rej(e);
            };
            s.onerror = function () { rej(e); };
            document.head.appendChild(s);
          });
        }
        if (optional) return null;
        throw e;
      });
  }

  Promise.all([getJSON(HOUSING), getJSON(CE), getJSON(PARCELS, true), getJSON(BOUNDS, true),
               getJSON(PERMITS, true)])
    .then(function (res) {
      D.housing = res[0]; D.ce = res[1]; D.parcels = res[2]; D.bounds = res[3];
      D.permits = (res[4] && res[4].by_plan) || {};
      if (!D.parcels) {
        // Losing the map is survivable; losing it silently is not.
        mapNote = "<b>המפה אינה זמינה:</b> שכבת המגרשים לא נטענה — " +
                  (location.protocol === "file:"
                    ? "הדף נפתח ישירות מהדיסק, ודפדפנים חוסמים טעינת קבצים כאלה ב-file://. פתחו דרך שרת כדי לראות את המפה."
                    : "בדקו את " + esc(PARCELS) + ".") +
                  " הרשימות, הסינון והייצוא פועלים כרגיל.";
      }
      indexGeometry();
      readHash();
      render();
      window.addEventListener("hashchange", function () { readHash(); render(); });
    })
    .catch(function (e) {
      document.getElementById("root").innerHTML =
        '<div style="padding:40px;text-align:center;color:#f0883e;line-height:1.9">' +
        "לא ניתן לטעון את נתוני הדוחות" +
        '<div style="color:#a6a2b5;font-size:13px;margin-top:8px">' + esc(e.message) + "</div></div>";
    });
})();
