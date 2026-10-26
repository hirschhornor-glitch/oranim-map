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
  // Fields the spec asks for that no single source holds: approval date (to filter
  // "from 2022"), neighbourhood (asked for in all three tabs, absent from the commerce
  // file), developer, the unit count the commerce tab needs for its 100+ filter, and
  // the old תב"ע number. Assembled by horaot_job/build_citywide_meta.py so none of the
  // three upstream pipelines has to change.
  var META = "../data/citywide_plan_meta.json";
  var CENTER = [31.7683, 35.2137], ZOOM = 12;

  // scope=true applies the filter the spec defines for each tab. It is the DEFAULT
  // because the headline figures are meant to answer the spec's question, not to
  // describe everything in the file — but it is switchable, because a number nobody
  // can widen is a number nobody can check.
  var state = { tab: "ce", q: "", status: "", sub: "", src: "", sort: "", desc: true,
                sel: null, scope: true, hood: "", year: "", kind: "" };
  // The initial tab is "ce", whose scope is off by default; the hash parser may
  // still turn it on. Set after SCOPES exists -- see applyScopeDefault() below.
  var D = { ce: null, housing: null, parcels: null, bounds: null, permits: null, meta: null };
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
  var metaOf = function (plan) { return ((D.meta || {}).by_plan || {})[plan] || {}; };
  var _ceKeys = null;
  var inCommerce = function (plan) {
    if (!_ceKeys) {
      _ceKeys = {};
      Object.keys((D.ce || {}).by_plan || {}).forEach(function (k) { _ceKeys[pad7(k)] = 1; });
    }
    return !!_ceKeys[plan];
  };
  var yn = function (v) {
    return v ? '<span class="hi">✓</span>' : '<span class="empty">—</span>';
  };
  // Stage columns, appended to every report's table. A plan that is not in the
  // realization file is left blank rather than shown as "no" -- absence there
  // means "not checked", and the two must not read the same.
  // ONE current-status value, as the spec asks, instead of four tick columns. The
  // ladder is read off the realization file in order, strongest signal first:
  //   אכלוס  occupied, or a טופס 4 date
  //   שלד    construction complete but not yet occupied
  //   ביצוע  under construction
  //   תכנון  checked, and no licensing file or permit has been reached
  // A plan the realization pass never examined is "לא נבדק" and NOT "תכנון": absence
  // of a check and a checked absence are different facts, and 226 plans are in the
  // first group.
  var CURRENT = function (plan) {
    var v = realOf(plan);
    if (!v) return null;
    if (v.occupied || v.form4_date) return "אכלוס";
    if (v.completed) return "שלד";
    if (v.in_construction) return "ביצוע";
    return "תכנון";
  };
  var CUR_COL = {
    k: "cur", t: "סטטוס נוכחי", get: function (r) { return CURRENT(r.plan); },
    fmt: function (v, r) {
      if (!v) return '<span class="empty" title="מצב המימוש לא נבדק לתכנית זו">לא נבדק</span>';
      var st = realOf(r.plan) || {};
      var tip = [];
      if (st.stage) tip.push(st.stage);
      if (st.permit_count) tip.push("תיקי רישוי: " + st.permit_count);
      if (st.form4_date) tip.push("טופס 4: " + st.form4_date);
      return '<span class="hi" title="' + esc(tip.join(" · ")) + '">' + v + "</span>";
    }
  };
  // Asked for in all three tabs. Both come from the side-car, so a tab that never had
  // them gets them without its own pipeline changing.
  var HOOD_COL = { k: "hood", t: "שכונה",
    get: function (r) { return r.sub_neighborhood || metaOf(r.plan).neighborhood || ""; },
    fmt: function (v) { return v ? esc(v) : '<span class="empty">—</span>'; } };
  // Taken from the plan's NAME, which is where Mavat puts the street. Reading one out
  // of the document's prose was tried and dropped — it produced sentence fragments that
  // pass every shape test, and a field that is quietly wrong is worse than an empty one.
  var ADDR_COL = { k: "addr", t: "כתובת",
    get: function (r) { return metaOf(r.plan).address || ""; },
    fmt: function (v) {
      return v ? '<span title="מתוך שם התכנית">' + esc(v) + "</span>"
               : '<span class="empty">—</span>';
    } };
  var DEV_COL = { k: "dev", t: "יזם",
    get: function (r) { return metaOf(r.plan).developer || ""; },
    fmt: function (v) { return v ? esc(v) : '<span class="empty">—</span>'; } };
  var REAL_COLS = [CUR_COL];

  /* ---------------- realization model ----------------
   * Parameters taken verbatim from the municipal presentation
   * "התחדשות עירונית: הפרויקטור — אחוזי מימוש ושלביות ביצוע", ניסן תשפ"ו / אפריל 2026.
   *
   * Stage durations and realization rates are slides 3 and 5; the delay ("זנב") rules
   * are slide 4. Everything the model does is a restatement of that document, so if
   * the document is revised this block is the only thing that changes.
   *
   * Two things in the source are ambiguous and are resolved here in the open:
   *   · slide 3's table gives 86% for מתן תוקף → פתיחת תיק רישוי, while slide 5's
   *     adopted recommendation rounds it to 90%. The recommendation is what the
   *     presentation asks to adopt, so 90% is used; the difference moves only the
   *     plans sitting between approval and a licensing file.
   *   · slide 4's fourth rule says "בתהליכי רישוי" but then names the PLANNING stage.
   *     Read as the escalation of the third rule (licensing beyond 3.5 years), which
   *     is the only reading under which the four rules form a ladder.
   */
  var MODEL = {
    source: 'הפרויקטור — אחוזי מימוש ושלביות ביצוע, אפריל 2026',
    stages: [
      { k: "toApproval",  m: 42, rate: 0.80, label: "פתיחת תיק עד מתן תוקף" },
      { k: "toLicensing", m: 6,  rate: 0.90, label: "מתן תוקף עד פתיחת תיק רישוי" },
      { k: "toDigPermit", m: 7,  rate: 1.00, label: "תיק רישוי עד היתר הריסה/חפירה" },
      { k: "organise",    m: 6,  rate: 1.00, label: "התארגנות לקראת ביצוע" },
      { k: "toPermit",    m: 15, rate: 1.00, label: "היתר בנייה" },
      { k: "build",       m: 54, rate: 1.00, label: "ביצוע עד אכלוס" }
    ],
    sizeBump: [{ units: 500, m: 12 }, { units: 300, m: 6 }],
    tail: [
      { phase: "plan", years: 5,   rate: 0.30, add: 24 },
      { phase: "plan", years: 4,   rate: 0.50, add: 12 },
      { phase: "lic",  years: 3.5, rate: 0.30, add: 24 },
      { phase: "lic",  years: 2.5, rate: 0.70, add: 12 }
    ]
  };
  var MONTHS_FROM = {};          // remaining months, by the stage a plan is at
  (function () {
    var st = MODEL.stages, acc = 0;
    for (var i = st.length - 1; i >= 0; i--) { acc += st[i].m; MONTHS_FROM[st[i].k] = acc; }
  })();
  var RATE_FROM = {};
  (function () {
    var st = MODEL.stages, acc = 1;
    for (var i = st.length - 1; i >= 0; i--) { acc *= st[i].rate; RATE_FROM[st[i].k] = acc; }
  })();

  var NOW = new Date();
  var monthsSince = function (iso) {
    if (!iso) return null;
    var d = new Date(iso);
    if (isNaN(d)) return null;
    return (NOW.getFullYear() - d.getFullYear()) * 12 + (NOW.getMonth() - d.getMonth());
  };
  // Earliest permit record = when the licensing file opened; the issued permit's date
  // is when building could start. Both come from the realization file's own rows.
  var licDates = function (plan) {
    var v = realOf(plan), out = { open: null, issued: null };
    if (!v || !v.permits) return out;
    v.permits.forEach(function (p) {
      if (!p.status_date) return;
      if (!out.open || p.status_date < out.open) out.open = p.status_date;
      if (/היתר/.test(p.status || "") && (!out.issued || p.status_date > out.issued))
        out.issued = p.status_date;
    });
    return out;
  };

  /* Where a plan stands, how likely it is to reach occupancy, and when.
   * Returns null when the realization pass never examined the plan AND the plan is not
   * approved — there is then nothing to place it on the ladder with. */
  function forecast(r) {
    var v = realOf(r.plan) || {}, meta = metaOf(r.plan);
    var units = meta.units || (r.rental && r.rental.units_t5) || 0;
    var ld = licDates(r.plan);
    var stage, months, rate, basis, elapsed = null;

    if (v.occupied || v.form4_date) {
      return { done: true, rate: 1, months: 0, year: (v.form4_date || "").slice(0, 4) || null,
               stage: "אכלוס", basis: "מאוכלס" };
    }
    if (v.completed) { stage = "build"; months = 0; rate = 1; basis = "גמר בנייה"; }
    else if (v.in_construction) {
      elapsed = monthsSince(ld.issued);
      months = Math.max(6, MODEL.stages[5].m - (elapsed || 0));
      rate = 1; stage = "build"; basis = "בביצוע";
    } else if (v.any_issued) { stage = "build"; months = MONTHS_FROM.build; rate = 1; basis = "היתר בנייה"; }
    else if (v.has_licensing_file) {
      stage = "toDigPermit"; months = MONTHS_FROM.toDigPermit; rate = RATE_FROM.toDigPermit;
      basis = "תיק רישוי פתוח";
      var le = monthsSince(ld.open);
      if (le != null) MODEL.tail.forEach(function (t) {
        if (t.phase === "lic" && le / 12 > t.years && rate > t.rate) { rate = t.rate; months += t.add; }
      });
    } else if (isApproved(r.status)) {
      stage = "toLicensing"; months = MONTHS_FROM.toLicensing; rate = RATE_FROM.toLicensing;
      basis = "מאושרת, טרם נפתח תיק רישוי";
    } else {
      var re = monthsSince(meta.received);
      stage = "toApproval"; rate = RATE_FROM.toApproval;
      months = Math.max(12, MONTHS_FROM.toApproval - (re || 0));
      basis = "בתכנון";
      if (re != null) MODEL.tail.forEach(function (t) {
        if (t.phase === "plan" && re / 12 > t.years && rate > t.rate) { rate = t.rate; months += t.add; }
      });
    }
    MODEL.sizeBump.some(function (b) {
      if (units >= b.units) { months += b.m; return true; }
      return false;
    });
    var d = new Date(NOW.getTime());
    d.setMonth(d.getMonth() + months);
    return { done: false, rate: rate, months: months, year: String(d.getFullYear()),
             stage: stage, basis: basis, units: units };
  }
  // 110 plans are in both the commerce and the rental tab, 16 in all three. Nothing
  // is duplicated WITHIN a tab, so the risk is a reader adding two tabs together. The
  // row says which other tabs hold the same plan.
  var tabsOf = function (plan) {
    var out = [];
    if (inCommerce(plan)) out.push("מסחר");
    (D.housing ? D.housing.rows : []).forEach(function (r) {
      if (r.plan !== plan) return;
      if (r.fund) out.push("קרן");
      if (r.rental) out.push("השכרה");
    });
    return out;
  };
  var TAB_LABEL = { ce: "מסחר", fund: "קרן", rental: "השכרה" };
  var alsoIn = function (plan) {
    return tabsOf(plan).filter(function (t) { return t !== TAB_LABEL[state.tab]; });
  };

  var FORECAST_COLS = [
    { k: "fyear", t: "צפי אכלוס", n: true,
      get: function (r) { var f = forecast(r); return f.year ? +f.year : null; },
      fmt: function (v, r) {
        var f = forecast(r);
        if (f.done) return '<span class="hi" title="' + esc(f.basis) + '">מאוכלס</span>';
        return '<span class="hi" title="' + esc(f.basis + " · " + f.months + " חודשים לפי המתווה") +
               '">' + (v || "") + "</span>";
      } },
    { k: "frate", t: "% מימוש", n: true,
      get: function (r) { return Math.round(forecast(r).rate * 100); },
      fmt: function (v, r) {
        var f = forecast(r);
        var cls = v >= 100 ? "hi" : (v <= 50 ? "empty dup" : "");
        return '<span class="' + cls + '" title="' + esc(f.basis) + '">' + v + "%</span>";
      } }
  ];


  // The scope each tab is SUPPOSED to answer for, per the spec. Applied by default:
  // the headline figures are meant to answer that question, and computing them over
  // everything in the file is what made them wrong.
  // statusGroup is defined further down; this only runs at render time, by which
  // point it exists. "approved" is the group that means the plan is in force —
  // "בהליך אישור" is deliberately NOT in it.
  var isApproved = function (st) { return statusGroup(st).key === "approved"; };
  // What a neighbourhood row should total in each tab — the same measure its headline
  // uses, so the summary and the KPI cannot tell different stories.
  var HOOD_METRIC = {
    ce:     { label: 'מסחר + תעסוקה (מ"ר)',
              of: function (r) { return (r.ce.commerce_sqm || 0) + (r.ce.employment_sqm || 0); },
              fmt: function (v) { return short(v); } },
    fund:   { label: "סך הקרנות",
              of: function (r) { return r.fund.amount_ils || 0; },
              fmt: function (v) { return "₪" + short(v); } },
    rental: { label: 'יח"ד להשכרה',
              of: function (r) { return r.rental.restates_plan ? 0 : (r.rental.units_t5 || 0); },
              fmt: function (v) { return n0(v); } }
  };
  function hoodSummary(rows) {
    var m = HOOD_METRIC[state.tab];
    if (!m) return "";
    var by = {};
    rows.forEach(function (r) {
      var h = r.sub_neighborhood || metaOf(r.plan).neighborhood || "ללא שכונה";
      by[h] = by[h] || { n: 0, v: 0 };
      by[h].n += 1; by[h].v += m.of(r) || 0;
    });
    var keys = Object.keys(by).sort(function (a, b) { return by[b].v - by[a].v || by[b].n - by[a].n; });
    if (!keys.length) return "";
    var rowsHtml = keys.slice(0, 12).map(function (h) {
      return "<tr" + (h === state.hood ? ' class="sel"' : "") + ' data-hood="' + esc(h) + '">' +
             "<td>" + esc(h) + "</td><td class=num>" + n0(by[h].n) + "</td>" +
             "<td class=num>" + m.fmt(by[h].v) + "</td></tr>";
    }).join("");
    var rest = keys.length > 12
      ? '<div class="empty" style="padding:4px 6px">ועוד ' + (keys.length - 12) + " שכונות — ראו הסינון</div>"
      : "";
    // Closed by default. Open, this block is 336px tall -- on a 860px window it
    // left the map and table 197px between them, which is what "why is the map so
    // small?" was. The open state lives in `state` because the whole block is
    // re-rendered on every filter change, so an `open` attribute alone would snap
    // back shut (or back open) on the next keystroke.
    return '<details class="hoodsum"' + (state.hoodOpen ? " open" : "") +
           '><summary>פילוח לפי שכונה · ' + keys.length +
           " שכונות</summary><table class=\"mini\"><thead><tr><th>שכונה</th><th>תכניות</th><th>" +
           esc(m.label) + "</th></tr></thead><tbody>" + rowsHtml + "</tbody></table>" + rest +
           "</details>";
  }

  var SCOPES = {
    // OFF by default on this tab. A unit-count filter is a housing criterion, and
    // on a commerce report it excludes, by construction, every dedicated
    // employment or commerce scheme -- those have no יח"ד at all. With it on, all
    // 79 pure schemes vanish (גן חוצבים, פארק גב ים, מגדלי טופז among them) and
    // only the 114 housing plans that also grant commerce remain. The checkbox
    // stays, so the spec's view is one click away.
    ce: { label: 'מאושרות · 100+ יח"ד', off: true,
          why: 'האפיון: תכניות מאושרות בלבד, מעל 100 יח"ד. כבוי כברירת מחדל — הוא מסנן החוצה פרויקטי תעסוקה/מסחר טהורים, שאין להם יח"ד.',
          test: function (r) { return isApproved(r.status) && (metaOf(r.plan).units || 0) >= 100; },
          drop: function (r) {
            if (!isApproved(r.status)) return "אינן מאושרות";
            if (!metaOf(r.plan).units) return 'ללא ספירת יח"ד — לא ידוע אם מעל 100';
            return 'מתחת ל-100 יח"ד';
          } },
    rental: { label: "מאושרות בלבד",
              why: "האפיון: מספר יח\"ד להשכרה בתכניות מאושרות.",
              test: function (r) { return isApproved(r.status); },
              drop: function () { return "אינן מאושרות"; } },
    fund: { label: "מאושרות · מ-2022",
            why: "האפיון: תכניות מ-2022 ואילך. התאריך הוא מועד מתן התוקף מ-XPLAN.",
            test: function (r) {
              return isApproved(r.status) && (metaOf(r.plan).effective || "") >= "2022";
            },
            drop: function (r) {
              if (!isApproved(r.status)) return "אינן מאושרות";
              if (!metaOf(r.plan).effective) return "מאושרות אך ללא תאריך מתן תוקף ב-XPLAN";
              return "אושרו לפני 2022";
            } }
  };
  // What the scope left out, counted by reason. A plan excluded for want of a unit
  // count is not the same as one excluded for having fewer than 100, and the second
  // group is a finding while the first is a gap in our data.
  function scopeDropped() {
    var sc = SCOPES[state.tab];
    if (!sc) return null;
    var by = {}, n = 0;
    REPORTS[state.tab].rows().forEach(function (r) {
      if (sc.test(r)) return;
      var why = sc.drop(r); by[why] = (by[why] || 0) + 1; n += 1;
    });
    return { n: n, by: by };
  }
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
    pct:    ["*", "מחושב: אחוז ההשכרה שבהוראות כפול סך יח\"ד המגורים בטבלה 5. מספר גזור, לא מספר שהתכנית נוקבת בו"],
    curated:["‡", "נקרא ואומת ידנית בחלק הסטטוטורי של ההוראות — הציטוט בפאנל"]
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
          var pk = pad7(k);
          return { plan: pk, name: v.pl_name, status: v.station_desc,
                   sub_neighborhood: metaOf(pk).neighborhood || null,
                   source: "text", ce: v, total: tot };
        }).filter(function (r) { return r.total > 0; });
      },
      cols: [
        { k: "plan", t: "תכנית", get: function (r) { return r.plan; } },
        { k: "name", t: "שם התכנית", get: function (r) { return r.name || ""; } },
        { k: "status", t: "סטטוס", get: function (r) { return r.status || ""; } },
        // The old plan number. It is in the commerce file already and was never shown.
        { k: "taba", t: 'תב"ע', get: function (r) { return metaOf(r.plan).taba || ""; },
          fmt: function (v) { return v ? esc(v) : '<span class="empty">—</span>'; } },
        ADDR_COL, HOOD_COL, DEV_COL,
        { k: "units", t: 'יח"ד', n: true, get: function (r) { return metaOf(r.plan).units || null; },
          fmt: function (v, r) {
            if (!v) return '<span class="empty" title="אין ספירת יח\"ד לתכנית זו — לא בטבלה 5 ולא בגיליון">—</span>';
            return '<span class="hi">' + n0(v) + "</span>" +
                   (metaOf(r.plan).units_src === "gs"
                     ? '<sup class="vis" title="מהגיליון, לא מטבלה 5">¹</sup>' : "");
          } },
        { k: "renew", t: "התחדשות", get: function (r) { return metaOf(r.plan).renewal || ""; },
          fmt: function (v) {
            return v ? '<span class="pill stat">' + esc(v) + "</span>"
                     : '<span class="empty">—</span>';
          } },
        { k: "ofc", t: "המרה למשרדים", get: function (r) { return metaOf(r.plan).office_conv ? 1 : 0; },
          fmt: function (v, r) {
            var q = metaOf(r.plan).office_conv;
            return q ? '<span class="hi" title="' + esc(q) + '">✓</span>'
                     : '<span class="empty" title="לא נמצאה בהוראות הוראה המתירה משרדים בשטחי המסחר">—</span>';
          } },
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
          fmt: function (v) { return v ? n0(v) : '<span class="empty">—</span>'; } },
        // Dedicated employment/commerce scheme, or commerce riding inside a
        // housing plan? 79 plans are pure (1.81M מ"ר) against 578 mixed (2.86M),
        // so most of the city's commerce and employment sits inside residential
        // plans. Blank where no Table 5 could be read -- not "no housing".
        { k: "kind", t: "סוג הפרויקט", get: function (r) { return r.ce.project_kind || ""; },
          fmt: function (v) {
            return v === "pure_ce" ? '<span class="hi">תעסוקה/מסחר</span>'
                 : v === "mixed_resid" ? 'מעורב עם מגורים'
                 : '<span class="empty">?</span>'; } }
      ].concat(REAL_COLS).concat(FORECAST_COLS),
      detail: function (r) {
        var h = "";
        if (r.ce.split_source) h += "<div><b>מקור הפיצול:</b> " + esc(r.ce.split_source) + "</div>";
        if (r.ce.split_note) h += '<div class="quote">' + esc(r.ce.split_note) + "</div>";
        var rem = metaOf(r.plan).remarks;
        if (rem) h += '<div class="quote"><b>הערות לטבלה 5:</b> ' + esc(rem.slice(0, 500)) + "</div>";
        var oc = metaOf(r.plan).office_conv;
        if (oc) h += '<div class="quote"><b>המרה למשרדים:</b> ' + esc(oc) + "</div>";
        if (r.ce.t5_status && r.ce.t5_status !== "success")
          h += '<div class="empty">טבלה 5: ' + esc(r.ce.t5_status) + "</div>";
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
        ADDR_COL, HOOD_COL, DEV_COL,
        { k: "floors", t: "קומות", n: true, get: function (r) { return r.fund.floors; } },
        { k: "cond", t: 'יח"ד מותנות', n: true,
          get: function (r) { return r.fund.conditional_units_t5; },
          fmt: function (v, r) { return unitCell(v, r.fund.conditional_units_state, r.fund.conditional_units_read); } },
        { k: "amount", t: "גובה הקרן", n: true, get: function (r) { return r.fund.amount_ils; },
          fmt: function (v) { return v ? '<span class="hi">' + ils(v) + "</span>" : '<span class="empty">לא צוין</span>'; } },
        { k: "section", t: "סעיף", get: function (r) { return r.fund.section || ""; } }
      ].concat(REAL_COLS).concat(FORECAST_COLS),
      detail: function (r) {
        var h = "";
        if (r.fund.conditional_units_quote)
          h += '<div class="quote"><b>הציטוט שממנו נקבע מספר היח"ד המותנות:</b> ' +
               esc(r.fund.conditional_units_quote) + "</div>";
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
        // "מס' יחידות בפיקוח" in the spec. Price-controlled rental in Jerusalem is
        // דיור בהישג יד under the Sixth Addendum. A count only where the document
        // states one — the usual formula is a percentage of the affordable SUBSET,
        // and converting that would be inventing a number.
        { k: "sup", t: 'יח"ד בפיקוח', n: true,
          get: function (r) { return metaOf(r.plan).affordable_units || null; },
          fmt: function (v, r) {
            var m = metaOf(r.plan);
            if (v) return '<span class="hi" title="' + esc(m.affordable_quote || "") + '">' + n0(v) + "</span>";
            if (m.affordable) return '<span class="pill stat" title="התכנית כוללת דיור בהישג יד לפי התוספת השישית, בלי מספר נקוב">בהישג יד</span>';
            return '<span class="empty">—</span>';
          } },
        // The spec asks for a countdown from the moment the plan reaches completion.
        // It can only run once occupancy has actually started, so a plan that is not
        // yet occupied says so instead of showing a number that has not begun.
        { k: "left", t: "נותרו", n: true,
          get: function (r) {
            var v = realOf(r.plan) || {}, d = +r.rental.duration;
            if (!d || !v.form4_date) return null;
            var start = new Date(v.form4_date);
            if (isNaN(start)) return null;
            var years = (NOW - start) / (365.25 * 24 * 3600 * 1000);
            return Math.max(0, Math.round((d - years) * 10) / 10);
          },
          fmt: function (v, r) {
            var vv = realOf(r.plan) || {};
            if (v == null) {
              return '<span class="empty" title="' +
                (r.rental.duration ? "הספירה מתחילה בטופס 4; התכנית טרם אוכלסה"
                                   : "אין משך השכרה ידוע") + '">—</span>';
            }
            if (v <= 0) return '<span class="empty dup" title="תקופת ההשכרה הסתיימה">הסתיימה</span>';
            return '<span class="hi" title="מטופס 4 ב-' + esc(vv.form4_date) + '">' +
                   v + " שנים</span>";
          } },
        { k: "dur", t: "משך ההשכרה", get: function (r) { return r.rental.duration; },
          fmt: function (v) {
            return v ? '<span class="hi">' + (isNaN(+v) ? esc(v) : v + " שנים") + "</span>"
                     : '<span class="empty">לא צוין</span>';
          } },
        ADDR_COL, HOOD_COL, DEV_COL,
        { k: "dsrc", t: "מקור המשך", get: function (r) { return r.rental.duration_source || ""; } },
        { k: "flag", t: "לבדיקה", get: function (r) { return r.rental.review ? 1 : 0; },
          fmt: function (v) { return v ? '<span class="pill ocr">שימוש מותר?</span>' : ""; } }
      ].concat(REAL_COLS).concat(FORECAST_COLS),
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
    var sc = SCOPES[state.tab];
    if (state.scope && sc) rows = rows.filter(sc.test);
    // "by year Y": everything the model expects to be occupied by the end of Y,
    // including what already is.
    if (state.year) rows = rows.filter(function (r) {
      var f = forecast(r);
      return f.done || (f.year && +f.year <= +state.year);
    });
    if (state.hood) rows = rows.filter(function (r) {
      return (r.sub_neighborhood || metaOf(r.plan).neighborhood || "") === state.hood;
    });
    var q = state.q.trim();
    if (q) rows = rows.filter(function (r) {
      return ((r.plan || "") + " " + (r.name || "") + " " + (r.sub_neighborhood || "")).indexOf(q) !== -1;
    });
    if (state.status) rows = rows.filter(function (r) { return r.status === state.status; });
    // CE tab only: dedicated employment/commerce schemes vs commerce inside housing.
    if (state.kind && state.tab === "ce")
      rows = rows.filter(function (r) { return (r.ce || {}).project_kind === state.kind; });
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
    if (state.hood) p.push("hood=" + encodeURIComponent(state.hood));
    if (state.year) p.push("year=" + state.year);
    // Record the scope only when it differs from this tab's default, in either
    // direction -- otherwise a CE link (default off) would always carry all=1,
    // and turning the filter ON would not survive being shared.
    var defOn = !(SCOPES[state.tab] && SCOPES[state.tab].off);
    if (!state.scope && defOn) p.push("all=1");
    if (state.scope && !defOn) p.push("scope=1");
    if (state.kind) p.push("kind=" + state.kind);
    if (state.src) p.push("src=" + state.src);
    if (state.sort) p.push("sort=" + state.sort + (state.desc ? ":d" : ":a"));
    if (state.sel) p.push("sel=" + state.sel);
    history.replaceState(null, "", "#" + p.join("&"));
  }
  function readHash() {
    var h = (location.hash || "").replace(/^#/, "");
    var parts = h ? h.split("&") : [];
    if (parts.length && REPORTS[parts[0]]) state.tab = parts[0];
    // Default for whichever tab we landed on, applied BEFORE the hash is read so
    // an explicit all=1 / scope=1 in the link still wins.
    state.scope = !(SCOPES[state.tab] && SCOPES[state.tab].off);
    if (!h) return;
    parts.slice(1).forEach(function (kv) {
      var i = kv.indexOf("="); if (i < 0) return;
      var k = kv.slice(0, i), v = decodeURIComponent(kv.slice(i + 1));
      if (k === "q") state.q = v; else if (k === "st") state.status = v;
      else if (k === "hood") state.hood = v;
      else if (k === "year") state.year = v;
      else if (k === "all") state.scope = false;
      else if (k === "scope") state.scope = true;
      else if (k === "kind") state.kind = v;
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
    // Built from the tab's full set, not from `rows`: a menu that narrows itself to
    // the selection it just made cannot be undone from the menu.
    var hoodSet = {};
    R.rows().forEach(function (r) {
      var h = r.sub_neighborhood || metaOf(r.plan).neighborhood;
      if (h) hoodSet[h] = 1;
    });
    var hoodNames = Object.keys(hoodSet).sort(function (a, b) { return a.localeCompare(b, "he"); });
    var thisYear = NOW.getFullYear(), yearOpts = [];
    for (var yy = thisYear; yy <= thisYear + 15; yy++) yearOpts.push(yy);

    // The quantity the model EXPECTS, not the quantity on paper: each plan's measure
    // weighted by its realization rate. Shown whenever a target year is chosen, because
    // that is the question a year cut asks — "how much is actually likely by then".
    var extraKpis = [];
    if (state.year && HOOD_METRIC[state.tab]) {
      var m = HOOD_METRIC[state.tab];
      var paper = rows.reduce(function (a, r) { return a + (m.of(r) || 0); }, 0);
      var exp = rows.reduce(function (a, r) { return a + (m.of(r) || 0) * forecast(r).rate; }, 0);
      // The KPI renderer escapes the label itself; escaping here too printed the
      // entity (יח&quot;ד) on screen.
      extraKpis.push({ n: m.fmt(Math.round(exp)),
                       l: m.label + " — צפי מימוש עד " + state.year });
      if (paper) extraKpis.push({ n: Math.round(100 * exp / paper) + "%",
                                  l: "מהמתוכנן בשורות המוצגות" });
    }
    var kpis = extraKpis.concat(R.kpis(rows)).map(function (k) {
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
      var also = alsoIn(r.plan);
      var out = '<tr class="row' + (state.sel === r.plan ? " sel" : "") +
                (also.length ? " multi" : "") + '" data-plan="' + esc(r.plan) + '"' +
                (also.length ? ' title="התכנית מופיעה גם בלשונית: ' + esc(also.join(", ")) +
                               ' — אין לחבר בין הלשוניות"' : "") + ">" + tds + "</tr>";
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
        (state.tab === "ce" ?
          // Pure employment/commerce scheme vs commerce inside a housing plan.
          '<select id="fkind"><option value="">כל סוגי הפרויקטים</option>' +
          '<option value="pure_ce"' + (state.kind === "pure_ce" ? " selected" : "") +
            ">תעסוקה/מסחר בלבד</option>" +
          '<option value="mixed_resid"' + (state.kind === "mixed_resid" ? " selected" : "") +
            ">מעורב עם מגורים</option></select>" : "") +
        (state.tab === "ce" ? "" :
          '<select id="fsrc"><option value="">טקסט + OCR</option>' +
          '<option value="text"' + (state.src === "text" ? " selected" : "") + ">טקסט בלבד</option>" +
          '<option value="ocr"' + (state.src === "ocr" ? " selected" : "") + ">OCR בלבד</option></select>") +
        (SCOPES[state.tab]
          ? '<label class="scopebox" title="' + esc(SCOPES[state.tab].why) + '">' +
            '<input type="checkbox" id="fscope"' + (state.scope ? " checked" : "") + "> " +
            esc(SCOPES[state.tab].label) + "</label>"
          : "") +
        '<select id="fyear" title="חיתוך לפי המתווה: כל מה שצפוי להיות מאוכלס עד סוף השנה שנבחרה">' +
          '<option value="">ללא חיתוך לשנה</option>' +
          yearOpts.map(function (y) {
            return '<option value="' + y + '"' + (String(y) === String(state.year) ? " selected" : "") +
                   ">עד " + y + "</option>";
          }).join("") + "</select>" +
        '<select id="fhood"><option value="">כל השכונות</option>' +
          hoodNames.map(function (v) {
            return '<option' + (v === state.hood ? " selected" : "") + ">" + esc(v) + "</option>";
          }).join("") + "</select>" +
        '<span class="spacer"></span>' +
        (nogeo ? '<span class="warnpill">' + nogeo + " ללא מיקום — ברשימה בלבד</span>" : "") +
        '<span style="color:var(--dim);font-size:12.5px">' + n0(rows.length) + " תכניות</span>" +
        '<button class="btn" id="csv">ייצוא לאקסל</button>' +
        '<button class="btn" id="prn">הדפסה</button>' +
        '<button class="btn" id="lnk">🔗 קישור</button>';

    document.getElementById("notes").innerHTML =
      (function () {
        if (!state.scope || !SCOPES[state.tab]) return "";
        var d = scopeDropped();
        var parts = Object.keys(d.by).sort(function (a, b) { return d.by[b] - d.by[a]; })
          .map(function (k) { return n0(d.by[k]) + " " + k; });
        return '<div class="note scope">מוצגות <b>' + esc(SCOPES[state.tab].label) +
          "</b>, והמספרים למעלה מחושבים עליהן. " + esc(SCOPES[state.tab].why) +
          (d.n ? " <b>מחוץ לסינון: " + n0(d.n) + "</b> — " + esc(parts.join(" · ")) + "." : "") +
          " להצגת כולן — הסירו את הסימון בשורת הסינון.</div>";
      })() +
      (state.year
        ? '<div class="note scope">חיתוך לשנה מחושב לפי <b>' + esc(MODEL.source) +
          '</b>: משך כל שלב ואחוז המימוש שלו, כולל הפחתת אחוז המימוש לתכניות ' +
          'שמתעכבות ותוספת זמן לתכניות גדולות. "צפי אכלוס" הוא תוצאת המתווה ולא ' +
          'התחייבות של התכנית.</div>'
        : "") +
      hoodSummary(rows) +
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
        state.hood = ""; state.year = ""; state.kind = "";
        state.scope = !(SCOPES[state.tab] && SCOPES[state.tab].off);
        render();
      });
    });
    var q = document.getElementById("q");
    if (q) q.addEventListener("input", function () {
      state.q = q.value; var p = q.selectionStart; render();
      var nq = document.getElementById("q"); nq.focus(); nq.setSelectionRange(p, p);
    });
    on("fst", "change", function (e) { state.status = e.target.value; render(); });
    on("fscope", "change", function (e) { state.scope = e.target.checked; state.sel = null; render(); });
    on("fhood", "change", function (e) { state.hood = e.target.value; state.sel = null; render(); });
    on("fyear", "change", function (e) { state.year = e.target.value; state.sel = null; render(); });
    var sum = document.querySelector(".hoodsum");
    if (sum) sum.addEventListener("toggle", function () { state.hoodOpen = sum.open; });
    if (sum) sum.addEventListener("click", function (e) {
      var tr = e.target.closest("tr[data-hood]");
      if (!tr) return;
      var h = tr.getAttribute("data-hood");
      state.hood = (state.hood === h ? "" : h); state.sel = null; render();
    });
    on("fsrc", "change", function (e) { state.src = e.target.value; render(); });
    on("fkind", "change", function (e) { state.kind = e.target.value; state.sel = null; render(); });
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
        if (url === HOUSING || url === META) {
          // Same file:// fallback as housing.html: fetch is blocked there, a script
          // tag is not. Only the housing payload has a JS twin.
          return new Promise(function (res, rej) {
            var s = document.createElement("script");
            s.src = url.replace(/\.json$/, ".js");
            s.onload = function () {
              var g = url === META ? window.__CITYWIDE_META__ : window.__HOUSING_TERMS__;
              g ? res(g) : rej(e);
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
               getJSON(PERMITS, true), getJSON(META, true)])
    .then(function (res) {
      D.housing = res[0]; D.ce = res[1]; D.parcels = res[2]; D.bounds = res[3];
      D.permits = (res[4] && res[4].by_plan) || {};
      D.meta = res[5] || { by_plan: {} };
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
