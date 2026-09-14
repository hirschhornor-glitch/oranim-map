/* housing.js — the two citywide housing-terms reports.
 *
 *   1. דיור מותנה וקרן תחזוקה   2. דיור להשכרה
 *
 * Both read one file, data/housing_terms.json, produced by a single pass over the
 * הוראות archive (4,570 plans): 3,739 from a real text layer and 831 via OCR.
 *
 * Two things this deliberately refuses to flatten:
 *
 *  - Provenance. OCR rows are marked, because exact figures there (₪ amounts, unit
 *    counts) ride on digits and quotation marks, which is what Hebrew OCR gets wrong.
 *    The detection words (קרן תחזוקה, דירות להשכרה) are punctuation-free and survive.
 *  - "Not found" is not "none". A plan absent from a report may simply have an
 *    unreadable document, so the coverage note stays on screen rather than letting a
 *    clean-looking table imply completeness.
 *
 * Plain DOM, no framework: these are two tables with filters, and a build step for
 * that would cost more than it returns.
 */
(function () {
  "use strict";

  var DATA = "data/housing_terms.json";
  var state = { tab: "fund", q: "", status: "", sub: "", src: "", sort: "", desc: true, open: {} };
  var D = null;

  var esc = function (s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  };
  var ils = function (n) {
    return n == null ? "" : "₪" + Number(n).toLocaleString("he-IL");
  };
  var short = function (n) {
    if (!n) return "₪0";
    if (n >= 1e9) return "₪" + (n / 1e9).toFixed(2) + " מיליארד";
    if (n >= 1e6) return "₪" + (n / 1e6).toFixed(1) + "M";
    return ils(n);
  };

  /* ---- the two report definitions, so the shell stays generic ---- */
  var REPORTS = {
    fund: {
      title: "דיור מותנה וקרן תחזוקה",
      tag: "fund",
      has: function (r) { return !!r.fund; },
      file: "קרן_תחזוקה",
      cols: [
        { k: "plan", t: "תכנית", get: function (r) { return r.plan; } },
        { k: "name", t: "שם התכנית", get: function (r) { return r.name || ""; }, wide: true },
        { k: "status", t: "סטטוס", get: function (r) { return r.status || ""; } },
        { k: "sub", t: "שכונה", get: function (r) { return r.sub_neighborhood || ""; } },
        { k: "floors", t: "קומות", n: true, get: function (r) { return r.fund.floors; } },
        { k: "cond", t: 'יח"ד מותנות', n: true, get: function (r) { return r.fund.conditional_units; } },
        { k: "amount", t: "גובה הקרן", n: true, get: function (r) { return r.fund.amount_ils; },
          fmt: function (v) { return v ? '<span class="money">' + ils(v) + "</span>" : '<span class="empty">לא צוין</span>'; } },
        { k: "section", t: "סעיף", get: function (r) { return r.fund.section || ""; } }
      ],
      detail: function (r) {
        var h = "";
        if (r.fund.mechanism) h += '<div class="quote"><b>המנגנון:</b> ' + esc(r.fund.mechanism) + "</div>";
        if (r.fund.amount_text) h += '<div class="quote"><b>ציטוט הסכום:</b> ' + esc(r.fund.amount_text) + "</div>";
        if (!h) h = '<div class="empty">אין ציטוט שנשמר עבור תכנית זו.</div>';
        return h;
      },
      kpis: function (rows) {
        var withAmt = rows.filter(function (r) { return r.fund.amount_ils; });
        var total = withAmt.reduce(function (a, r) { return a + r.fund.amount_ils; }, 0);
        var sorted = withAmt.map(function (r) { return r.fund.amount_ils; })
                            .sort(function (a, b) { return a - b; });
        var med = sorted.length ? sorted[Math.floor(sorted.length / 2)] : 0;
        // NOT a sum of conditional units: the הוראות state that number only rarely
        // (in the Oranim version it came from Table 5, which does not exist citywide),
        // so a total would read as "5 conditional units in Jerusalem" — worse than
        // saying nothing. Report how many plans state it at all.
        var withUnits = rows.filter(function (r) { return r.fund.conditional_units; });
        return [
          { n: rows.length, l: "תכניות עם קרן תחזוקה", c: "gold" },
          { n: short(total), l: "סך הקרנות (" + withAmt.length + " עם סכום)", c: "gold" },
          { n: med ? short(med) : "—", l: "חציון גובה הקרן" },
          { n: withUnits.length, l: 'תכניות שבהן ההוראות מציינות יח"ד מותנות' }
        ];
      }
    },
    rental: {
      title: "דיור להשכרה",
      tag: "rent",
      has: function (r) { return !!r.rental; },
      file: "דיור_להשכרה",
      cols: [
        { k: "plan", t: "תכנית", get: function (r) { return r.plan; } },
        { k: "name", t: "שם התכנית", get: function (r) { return r.name || ""; }, wide: true },
        { k: "status", t: "סטטוס", get: function (r) { return r.status || ""; } },
        { k: "sub", t: "שכונה", get: function (r) { return r.sub_neighborhood || ""; } },
        { k: "dur", t: "משך ההשכרה", get: function (r) { return r.rental.duration; },
          fmt: function (v) {
            return v ? '<span class="dur">' + (isNaN(+v) ? esc(v) : v + " שנים") + "</span>"
                     : '<span class="empty">לא צוין</span>';
          } },
        { k: "dsrc", t: "מקור המשך", get: function (r) { return r.rental.duration_source || ""; } },
        { k: "flag", t: "לבדיקה", get: function (r) { return r.rental.review ? 1 : 0; },
          fmt: function (v) { return v ? '<span class="pill rev">שימוש מותר?</span>' : ""; } }
      ],
      detail: function (r) {
        var h = "";
        (r.rental.evidence || []).forEach(function (e) {
          // The classifier tags each snippet with its own marker (R: rental wording,
          // D[n]: the matched term, P: protected housing). Useful provenance, but raw
          // debug codes in a planner's report just look like corruption — label them.
          var m = /^([RPD])(\[[^\]]*\])?:/.exec(e), lab = "";
          if (m) {
            lab = m[1] === "D" ? "הציטוט שממנו נקבע המשך"
                : m[1] === "R" ? "אזכור ההשכרה" : "אזכור דיור מוגן";
            e = e.slice(m[0].length);
          }
          h += '<div class="quote r">' + (lab ? "<b>" + lab + ":</b> " : "") + esc(e) + "</div>";
        });
        if (r.rental.review) {
          h += "<div><b>מדוע מסומן לבדיקה:</b> אזכור השכרה בודד במסמך — לרוב זהו " +
               "<b>שימוש מותר</b> ברשימת שימושים ולא חובה להקים דירות להשכרה. " +
               "דורש הכרעה תכנונית.</div>";
        }
        if (!h) h = '<div class="empty">אין ציטוט שנשמר עבור תכנית זו.</div>';
        return h;
      },
      kpis: function (rows) {
        var expl = rows.filter(function (r) { return r.rental.duration_source === "הוראות התכנית"; });
        var stat = rows.filter(function (r) { return r.rental.duration_source === "תוספת שישית"; });
        var rev = rows.filter(function (r) { return r.rental.review; });
        var none = rows.filter(function (r) { return !r.rental.duration; });
        // The review queue is empty after the hand pass, so the fourth tile shows the
        // gap that is still open — obligated rental with no term stated anywhere —
        // and falls back to the queue if new plans ever arrive flagged.
        return [
          { n: rows.length, l: "תכניות עם דיור להשכרה", c: "teal" },
          { n: expl.length, l: "משך מפורש בהוראות", c: "teal" },
          { n: stat.length, l: "משך מהתוספת השישית (20 שנה)" },
          rev.length ? { n: rev.length, l: "לבדיקה — ייתכן שימוש מותר בלבד" }
                     : { n: none.length, l: "ללא משך מצוין בהוראות" }
        ];
      }
    }
  };

  /* ---- filtering ---- */
  function visible() {
    var R = REPORTS[state.tab];
    var rows = D.rows.filter(R.has);
    var q = state.q.trim();
    if (q) {
      rows = rows.filter(function (r) {
        return (r.plan + " " + (r.name || "") + " " + (r.sub_neighborhood || "") +
                " " + (r.minahak || "")).indexOf(q) !== -1;
      });
    }
    if (state.status) rows = rows.filter(function (r) { return r.status === state.status; });
    if (state.sub) rows = rows.filter(function (r) { return r.sub_neighborhood === state.sub; });
    if (state.src) rows = rows.filter(function (r) { return r.source === state.src; });
    if (state.sort) {
      var col = R.cols.filter(function (c) { return c.k === state.sort; })[0];
      if (col) {
        rows = rows.slice().sort(function (a, b) {
          var x = col.get(a), y = col.get(b);
          if (col.n) { x = x == null ? -Infinity : +x; y = y == null ? -Infinity : +y; return state.desc ? y - x : x - y; }
          x = String(x || ""); y = String(y || "");
          return state.desc ? y.localeCompare(x, "he") : x.localeCompare(y, "he");
        });
      }
    }
    return rows;
  }

  function uniq(key) {
    var R = REPORTS[state.tab], s = {};
    D.rows.filter(R.has).forEach(function (r) { if (r[key]) s[r[key]] = 1; });
    return Object.keys(s).sort(function (a, b) { return a.localeCompare(b, "he"); });
  }

  /* ---- export ---- */
  function csv() {
    var R = REPORTS[state.tab], rows = visible();
    var head = R.cols.map(function (c) { return c.t; }).concat(["מקור קריאה", "קישור מבא\"ת"]);
    var lines = [head.join(",")];
    rows.forEach(function (r) {
      var cells = R.cols.map(function (c) {
        var v = c.get(r);
        return '"' + String(v == null ? "" : v).replace(/"/g, '""') + '"';
      });
      cells.push('"' + (r.source === "ocr" ? "OCR" : "טקסט") + '"');
      cells.push('"' + (r.mavat_url || "") + '"');
      lines.push(cells.join(","));
    });
    // BOM so Excel opens Hebrew correctly instead of mojibake.
    var blob = new Blob(["﻿" + lines.join("\r\n")], { type: "text/csv;charset=utf-8;" });
    var a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = R.file + "_" + new Date().toISOString().slice(0, 10) + ".csv";
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 1000);
  }

  /* ---- deep link, so a filtered view can be pasted to someone ---- */
  function writeHash() {
    var p = [state.tab];
    if (state.q) p.push("q=" + encodeURIComponent(state.q));
    if (state.status) p.push("st=" + encodeURIComponent(state.status));
    if (state.sub) p.push("sub=" + encodeURIComponent(state.sub));
    if (state.src) p.push("src=" + state.src);
    if (state.sort) p.push("sort=" + state.sort + (state.desc ? ":d" : ":a"));
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
      if (k === "q") state.q = v;
      else if (k === "st") state.status = v;
      else if (k === "sub") state.sub = v;
      else if (k === "src") state.src = v;
      else if (k === "sort") { var s = v.split(":"); state.sort = s[0]; state.desc = s[1] !== "a"; }
    });
  }

  /* ---- render ---- */
  function render() {
    var R = REPORTS[state.tab], rows = visible(), T = D.totals;
    var root = document.getElementById("root");

    var kpis = R.kpis(rows).map(function (k) {
      return '<div class="kpi ' + (k.c || "") + '"><div class="n">' + esc(k.n) +
             '</div><div class="l">' + esc(k.l) + "</div></div>";
    }).join("");

    var opts = function (arr, sel) {
      return arr.map(function (v) {
        return '<option value="' + esc(v) + '"' + (v === sel ? " selected" : "") + ">" + esc(v) + "</option>";
      }).join("");
    };

    var head = R.cols.map(function (c) {
      var ar = state.sort === c.k ? (state.desc ? " ↓" : " ↑") : "";
      return '<th data-sort="' + c.k + '">' + esc(c.t) + ar + "</th>";
    }).join("") + "<th>מקור</th>";

    var body = rows.map(function (r, i) {
      var id = r.plan;
      var tds = R.cols.map(function (c) {
        var v = c.get(r);
        var html = c.fmt ? c.fmt(v) : esc(v);
        return '<td class="' + (c.n ? "num " : "") + (c.wide ? "wide" : "") + '">' + html + "</td>";
      }).join("");
      tds += '<td>' + (r.source === "ocr" ? '<span class="pill ocr">OCR</span>'
                                          : '<span class="pill stat">טקסט</span>') + "</td>";
      var out = '<tr class="row' + (state.open[id] ? " open" : "") + '" data-plan="' + esc(id) + '">' + tds + "</tr>";
      if (state.open[id]) {
        out += '<tr class="detail"><td colspan="' + (R.cols.length + 1) + '">' + R.detail(r) +
               (r.mavat_url ? '<div style="margin-top:8px"><a class="btn noprint" target="_blank" rel="noopener" href="' +
                 esc(r.mavat_url) + '">פתיחה במבא"ת ↗</a></div>' : "") + "</td></tr>";
      }
      return out;
    }).join("");

    root.innerHTML =
      '<header>' +
        "<h1>" + esc(R.title) + "</h1>" +
        '<div class="sub">' + esc(D.scope) + " · נקראו " + (T.read_text + T.read_ocr).toLocaleString("he-IL") +
        " מסמכי הוראות (" + T.read_text.toLocaleString("he-IL") + " טקסט, " + T.read_ocr.toLocaleString("he-IL") +
        " OCR) · עודכן " + esc((D.generated_at || "").slice(0, 10)) + "</div>" +
        '<div class="tabs noprint">' +
          '<button class="tab fund' + (state.tab === "fund" ? " on" : "") + '" data-tab="fund">קרן תחזוקה ודיור מותנה</button>' +
          '<button class="tab rent' + (state.tab === "rental" ? " on" : "") + '" data-tab="rental">דיור להשכרה</button>' +
        "</div>" +
      "</header><main>" +
        '<div class="kpis">' + kpis + "</div>" +
        '<div class="note"><b>איך לקרוא את הדוח:</b> ' +
          "היעדר תכנית מהרשימה פירושו <b>לא נמצא</b> ולא <b>אין</b> — " + T.unreadable +
          " מסמכים לא ניתנים לקריאה כלל, ו-" + T.read_ocr.toLocaleString("he-IL") +
          " נקראו ב-OCR. שורות המסומנות <span class=\"pill ocr\">OCR</span> אמינות לזיהוי " +
          "(מילות המפתח חפות מפיסוק) אך <b>פחות אמינות למספרים מדויקים</b> — סכומי ₪ ומספרי יח\"ד " +
          "נשענים על ספרות וגרשיים, שהם בדיוק מה ש-OCR עברי שוגה בו. " +
          "אפס ממצאי קרן בסרוקות עקבי עם היות הקרן מכשיר של מגדלים מודרניים, " +
          "אך הכיול נעשה על מסמכים דיגיטליים שרונדרו ולא על צילומים." +
          "<br><b>סקירה ידנית:</b> 23 תכניות שבהן ההשכרה הופיעה פעם אחת נקראו אחת-אחת. " +
          "<b>14 הוצאו</b> כשימוש מותר בלבד (פריט ברשימת שימושים, בלי אחוז/מספר יח\"ד/תקופה), " +
          "<b>אחת הוצאה</b> משום שהיא משנה שימוש מהשכרה למגורים רגיל, ו-<b>8 אומתו כמחייבות</b>. " +
          "אגב כך תוקנו 4 תקופות שגויות (אחת מהן דווחה 20 שנה במקום 10)." +
        "</div>" +
        '<div class="bar noprint">' +
          '<input id="q" placeholder="חיפוש: מספר תכנית, שם, שכונה…" value="' + esc(state.q) + '">' +
          '<select id="fst"><option value="">כל הסטטוסים</option>' + opts(uniq("status"), state.status) + "</select>" +
          '<select id="fsub"><option value="">כל השכונות</option>' + opts(uniq("sub_neighborhood"), state.sub) + "</select>" +
          '<select id="fsrc"><option value="">טקסט + OCR</option>' +
            '<option value="text"' + (state.src === "text" ? " selected" : "") + ">טקסט בלבד</option>" +
            '<option value="ocr"' + (state.src === "ocr" ? " selected" : "") + ">OCR בלבד</option></select>" +
          '<span class="spacer"></span>' +
          '<span style="color:var(--dim);font-size:13px">' + rows.length + " תכניות</span>" +
          '<button class="btn" id="csv">ייצוא לאקסל</button>' +
          '<button class="btn" id="prn">הדפסה</button>' +
          '<button class="btn" id="lnk">🔗 העתקת קישור</button>' +
        "</div>" +
        '<div class="tblwrap"><table><thead><tr>' + head + "</tr></thead><tbody>" +
          (rows.length ? body : '<tr><td colspan="' + (R.cols.length + 1) +
            '" style="padding:26px;text-align:center;color:var(--faint)">אין תכניות התואמות את הסינון</td></tr>') +
        "</tbody></table></div>" +
      "</main>";

    wire();
    writeHash();
  }

  function wire() {
    var on = function (sel, ev, fn) {
      var el = document.getElementById(sel);
      if (el) el.addEventListener(ev, fn);
    };
    Array.prototype.forEach.call(document.querySelectorAll(".tab"), function (b) {
      b.addEventListener("click", function () {
        state.tab = b.getAttribute("data-tab");
        state.sort = ""; state.open = {};
        render();
      });
    });
    var q = document.getElementById("q");
    if (q) {
      q.addEventListener("input", function () {
        state.q = q.value;
        var pos = q.selectionStart;
        render();
        var nq = document.getElementById("q");
        nq.focus(); nq.setSelectionRange(pos, pos);
      });
    }
    on("fst", "change", function (e) { state.status = e.target.value; render(); });
    on("fsub", "change", function (e) { state.sub = e.target.value; render(); });
    on("fsrc", "change", function (e) { state.src = e.target.value; render(); });
    on("csv", "click", csv);
    on("prn", "click", function () { window.print(); });
    on("lnk", "click", function () {
      var el = document.getElementById("lnk");
      navigator.clipboard.writeText(location.href).then(function () {
        el.textContent = "✓ הועתק";
        setTimeout(function () { el.textContent = "🔗 העתקת קישור"; }, 1600);
      });
    });
    Array.prototype.forEach.call(document.querySelectorAll("th[data-sort]"), function (th) {
      th.addEventListener("click", function () {
        var k = th.getAttribute("data-sort");
        if (state.sort === k) state.desc = !state.desc;
        else { state.sort = k; state.desc = true; }
        render();
      });
    });
    Array.prototype.forEach.call(document.querySelectorAll("tr.row"), function (tr) {
      tr.addEventListener("click", function () {
        var p = tr.getAttribute("data-plan");
        state.open[p] = !state.open[p];
        render();
      });
    });
  }

  // The app's service worker serves /data/*.json stale-while-revalidate, which means
  // a rebuilt report shows up one load late — and `cache: "no-store"` does not help,
  // because the SW answers before the HTTP cache is consulted. sw.js strips only the
  // `v` parameter when building its cache key, so a unique `t` misses the cache every
  // time and always reaches the network. At ~230 KB that is the same trade the SW's
  // own FRESH_DATA_FILES list makes. The tidier fix — adding this file to that list
  // and bumping CACHE_VERSION — is a change to the shared app worker.
  // Loading the data by <script> tag instead of fetch. Needed on file:// URLs, where
  // fetch is blocked outright and the page would otherwise show only "Failed to fetch".
  function loadViaScript() {
    return new Promise(function (resolve, reject) {
      var s = document.createElement("script");
      // No cache-buster on file://: there is no HTTP cache to bust, and a query
      // string on a file: URL can be treated as part of the filename, which would
      // turn the fallback itself into a 404.
      s.src = DATA.replace(/\.json$/, ".js") +
              (location.protocol === "file:" ? "" : "?t=" + Date.now());
      s.onload = function () {
        window.__HOUSING_TERMS__ ? resolve(window.__HOUSING_TERMS__)
                                 : reject(new Error("no data in fallback"));
      };
      s.onerror = function () { reject(new Error("fallback script failed")); };
      document.head.appendChild(s);
    });
  }

  fetch(DATA + "?t=" + Date.now(), { cache: "no-store" })
    .then(function (r) {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    })
    .catch(loadViaScript)
    .then(function (d) {
      D = d;
      readHash();
      render();
      // A pasted deep link that only changes the hash is a same-document
      // navigation: without this the page keeps showing whatever was already on
      // screen, so the link silently does nothing for anyone already on the page.
      window.addEventListener("hashchange", function () {
        state.open = {};
        readHash();
        render();
      });
    })
    .catch(function (e) {
      // If even the script fallback failed and we are on file://, the data file is
      // simply missing next to the page — say that, rather than repeating a browser
      // error message that explains nothing to the reader.
      var local = location.protocol === "file:";
      document.getElementById("root").innerHTML =
        '<div style="padding:40px;text-align:center;color:#f0883e;line-height:1.9">' +
        "לא ניתן לטעון את " + DATA +
        '<div style="color:#a6a2b5;font-size:13px;margin-top:10px">' +
        (local
          ? "הדף נפתח ישירות מהדיסק, וקובץ הנתונים " +
            esc(DATA.replace(/\.json$/, ".js")) + " לא נמצא לצידו.<br>" +
            "הריצו את build_housing_reports_data.py, או פתחו את הדף דרך שרת."
          : esc(e.message)) +
        "</div></div>";
    });
})();
