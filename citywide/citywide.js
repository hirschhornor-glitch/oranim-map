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
  var CENTER = [31.7683, 35.2137], ZOOM = 12;

  var state = { tab: "ce", q: "", status: "", sub: "", src: "", sort: "", desc: true,
                sel: null };
  var D = { ce: null, housing: null, parcels: null, bounds: null };
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
          fmt: function (v) { return v ? n0(v) : '<span class="empty">—</span>'; } }
      ],
      detail: function (r) {
        var h = "";
        if (r.ce.split_source) h += "<div><b>מקור הפיצול:</b> " + esc(r.ce.split_source) + "</div>";
        if (r.ce.split_note) h += '<div class="quote">' + esc(r.ce.split_note) + "</div>";
        if (r.ce.stage) h += "<div><b>שלב ביצוע:</b> " + esc(r.ce.stage) + "</div>";
        return h || '<div class="empty">אין פירוט נוסף.</div>';
      },
      kpis: function (rows) {
        var com = rows.reduce(function (a, r) { return a + (r.ce.commerce_sqm || 0); }, 0);
        var emp = rows.reduce(function (a, r) { return a + (r.ce.employment_sqm || 0); }, 0);
        return [
          { n: n0(rows.length), l: "תכניות עם שטחי מסחר/תעסוקה" },
          { n: short(com) + ' מ"ר', l: "סך מסחר" },
          { n: short(emp) + ' מ"ר', l: "סך תעסוקה" }
        ];
      }
    },
    fund: {
      title: "קרן תחזוקה ודיור מותנה", tag: "fund", color: "var(--fund)", file: "קרן_תחזוקה",
      note: "<b>סך הקרנות הוא תקרה, לא סכום מחויב.</b> ההוראות קובעות במפורש ש\"מימוש זכויות הבנייה ה'מותנות' נתון לבחירת מגיש הבקשה להיתר ומימושן אינו מחייב\" — כלומר הסכום מתממש רק ככל שהיזם בוחר לממש את הזכויות המותנות. " +
            "היעדר תכנית פירושו <b>לא נמצא</b> ולא <b>אין</b>, וסכומי הקרן כולם מגיעים משכבת טקסט ולא מ-OCR.",
      rows: function () {
        return D.housing.rows.filter(function (r) { return r.fund; });
      },
      cols: [
        { k: "plan", t: "תכנית", get: function (r) { return r.plan; } },
        { k: "name", t: "שם התכנית", get: function (r) { return r.name || ""; } },
        { k: "status", t: "סטטוס", get: function (r) { return r.status || ""; } },
        { k: "sub", t: "שכונה", get: function (r) { return r.sub_neighborhood || ""; } },
        { k: "floors", t: "קומות", n: true, get: function (r) { return r.fund.floors; } },
        { k: "cond", t: 'יח"ד מותנות', n: true, get: function (r) { return r.fund.conditional_units; } },
        { k: "amount", t: "גובה הקרן", n: true, get: function (r) { return r.fund.amount_ils; },
          fmt: function (v) { return v ? '<span class="hi">' + ils(v) + "</span>" : '<span class="empty">לא צוין</span>'; } },
        { k: "section", t: "סעיף", get: function (r) { return r.fund.section || ""; } }
      ],
      detail: function (r) {
        var h = "";
        if (r.fund.mechanism) h += '<div class="quote"><b>המנגנון:</b> ' + esc(r.fund.mechanism) + "</div>";
        if (r.fund.amount_text) h += '<div class="quote"><b>ציטוט הסכום:</b> ' + esc(r.fund.amount_text) + "</div>";
        return h || '<div class="empty">אין ציטוט שנשמר.</div>';
      },
      kpis: function (rows) {
        var w = rows.filter(function (r) { return r.fund.amount_ils; });
        var tot = w.reduce(function (a, r) { return a + r.fund.amount_ils; }, 0);
        var s = w.map(function (r) { return r.fund.amount_ils; }).sort(function (a, b) { return a - b; });
        return [
          { n: n0(rows.length), l: "תכניות עם קרן תחזוקה" },
          { n: "₪" + short(tot), l: "סך הקרנות (" + w.length + " עם סכום)" },
          { n: s.length ? "₪" + short(s[Math.floor(s.length / 2)]) : "—", l: "חציון גובה הקרן" }
        ];
      }
    },
    rental: {
      title: "דיור להשכרה", tag: "rent", color: "var(--rent)", file: "דיור_להשכרה",
      note: "<b>איך לקרוא:</b> היעדר תכנית פירושו <b>לא נמצא</b> ולא <b>אין</b>. הרשימה כוללת רק תכניות המחייבות השכרה — 15 תכניות שבהן ההשכרה היא שימוש מותר בלבד הוצאו בסקירה ידנית. כל משך שמקורו ב-OCR אומת בקריאה ויזואלית של המסמך.",
      rows: function () {
        return D.housing.rows.filter(function (r) { return r.rental; });
      },
      cols: [
        { k: "plan", t: "תכנית", get: function (r) { return r.plan; } },
        { k: "name", t: "שם התכנית", get: function (r) { return r.name || ""; } },
        { k: "status", t: "סטטוס", get: function (r) { return r.status || ""; } },
        { k: "dur", t: "משך ההשכרה", get: function (r) { return r.rental.duration; },
          fmt: function (v) {
            return v ? '<span class="hi">' + (isNaN(+v) ? esc(v) : v + " שנים") + "</span>"
                     : '<span class="empty">לא צוין</span>';
          } },
        { k: "sub", t: "שכונה", get: function (r) { return r.sub_neighborhood || ""; } },
        { k: "dsrc", t: "מקור המשך", get: function (r) { return r.rental.duration_source || ""; } },
        { k: "flag", t: "לבדיקה", get: function (r) { return r.rental.review ? 1 : 0; },
          fmt: function (v) { return v ? '<span class="pill ocr">שימוש מותר?</span>' : ""; } }
      ],
      detail: function (r) {
        var h = "";
        (r.rental.evidence || []).forEach(function (e) {
          var m = /^([RPD])(\[[^\]]*\])?:/.exec(e), lab = "";
          if (m) {
            lab = m[1] === "D" ? "הציטוט שממנו נקבע המשך"
                : m[1] === "R" ? "אזכור ההשכרה" : "אזכור דיור מוגן";
            e = e.slice(m[0].length);
          }
          h += '<div class="quote">' + (lab ? "<b>" + lab + ":</b> " : "") + esc(e) + "</div>";
        });
        return h || '<div class="empty">אין ציטוט שנשמר.</div>';
      },
      kpis: function (rows) {
        var expl = rows.filter(function (r) { return r.rental.duration_source === "הוראות התכנית"; });
        var stat = rows.filter(function (r) { return r.rental.duration_source === "תוספת שישית"; });
        return [
          { n: n0(rows.length), l: "תכניות עם חובת השכרה" },
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

  function indexGeometry() {
    geoByPlan = {};
    [D.parcels, D.bounds].forEach(function (src) {
      if (!src || !src.features) return;
      src.features.forEach(function (f) {
        var key = f.properties && (f.properties.plan_name || f.properties.taba);
        if (!key) return;
        var p = pad7(key);
        (geoByPlan[p] = geoByPlan[p] || []).push(f);
      });
    });
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
                     properties: { plan: r.plan, name: r.name || "", st: r.status || "" } });
      });
    });
    if (!feats.length) return;
    layer = L.geoJSON({ type: "FeatureCollection", features: feats }, {
      style: function (f) {
        var on = state.sel === f.properties.plan;
        return { color: color, weight: on ? 3 : 1, opacity: on ? 1 : .75,
                 fillColor: color, fillOpacity: on ? .45 : .18 };
      },
      onEachFeature: function (f, lyr) {
        lyr.bindPopup('<div style="font-weight:700">' + esc(f.properties.name || f.properties.plan) +
                      "</div><div>" + esc(f.properties.plan) +
                      (f.properties.st ? " · " + esc(f.properties.st) : "") + "</div>");
        lyr.on("click", function () { select(f.properties.plan, false); });
      }
    }).addTo(map);
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
        return '<td class="' + (c.n ? "num" : "") + '">' + (c.fmt ? c.fmt(v) : esc(v)) + "</td>";
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

  Promise.all([getJSON(HOUSING), getJSON(CE), getJSON(PARCELS, true), getJSON(BOUNDS, true)])
    .then(function (res) {
      D.housing = res[0]; D.ce = res[1]; D.parcels = res[2]; D.bounds = res[3];
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
