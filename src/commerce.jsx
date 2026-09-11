/* commerce.jsx — "מסחר, תעסוקה ומשרדים: מיפוי עירוני"
 * Standalone page (like junction.jsx). Compiled to commerce.js by Babel
 * (preset-react). No bundler; React/Leaflet/MapLibre from CDN.
 *
 * Deliberately independent of app.jsx: this covers all 722 Jerusalem plans that
 * grant commerce/employment/offices, while the main app is scoped to the Oranim
 * district. Nothing here is imported by, or imports from, the main app.
 *
 * Data:
 *   data/ce_citywide_parcels.geojson  parcel polygons + commerce/employment + split_source
 *   data/ce_citywide_plans.json       per-plan rollup + licensing / construction / form-4
 */
const { useState, useEffect, useRef, useMemo, useCallback } = React;

const PARCELS_URL = 'data/ce_citywide_parcels.geojson?v=2026-09-11e';
const PLANS_URL = 'data/ce_citywide_plans.json?v=2026-09-11e';
const CENTER = [31.7767, 35.2245];
const ZOOM = 12;

/* Palette: purple = commerce (the project's thematic colour for מסחר),
   amber = employment, so a mixed parcel reads as a blend rather than a new hue. */
const COL = {
  commerce: '#a855f7',
  employment: '#f0a13a',
  unresolved: '#e0b0ff',   // commerce+employment granted together, mix unresolved
  none: '#5a4a63',
  sel: '#22d3ee',
};

const STATUS_COL = {
  'אישור': '#4ade80',
  'בהליך אישור': '#a3e635',
  'הכרעה בהתנגדויות / אישור': '#facc15',
  'הפקדה להתנגדויות/השגות': '#fb923c',
  'במילוי תנאים להפקדה': '#f472b6',
  'בבדיקה תכנונית': '#94a3b8',
};

const fmt = n => (n == null ? '—' : Math.round(n).toLocaleString('en-US'));
const esc = s => String(s == null ? '' : s).replace(/[&<>"]/g, c =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

/* Realization state of an approved plan, from the licensing + supervision data. */
function realization(p) {
  if (p.station_desc !== 'אישור') return { key: 'notapproved', label: 'טרם אושרה', col: '#94a3b8' };
  if (p.completed) return { key: 'done', label: 'גמר בנייה / טופס 4', col: '#0d9488' };
  if (p.in_construction) return { key: 'building', label: 'בביצוע', col: '#f0824b' };
  if (p.any_issued) return { key: 'permit', label: 'הופק היתר', col: '#4ba1f0' };
  if (p.has_licensing_file) return { key: 'licensing', label: 'ברישוי', col: '#7fb3e8' };
  if (p.has_licensing_file === false) return { key: 'none', label: 'אין תיק רישוי', col: '#64748b' };
  return { key: 'unknown', label: 'טרם נבדק', col: '#475569' };
}

/* ---------------------------------------------------------------- hash state */
function readHash() {
  const h = new URLSearchParams((location.hash || '').replace(/^#/, ''));
  return {
    q: h.get('q') || '',
    status: h.get('status') || 'all',
    src: h.get('src') || 'all',
    real: h.get('real') || 'all',
    sort: h.get('sort') || 'commerce',
    plan: h.get('plan') || '',
  };
}
function writeHash(s) {
  const h = new URLSearchParams();
  if (s.q) h.set('q', s.q);
  if (s.status !== 'all') h.set('status', s.status);
  if (s.src !== 'all') h.set('src', s.src);
  if (s.real !== 'all') h.set('real', s.real);
  if (s.sort !== 'commerce') h.set('sort', s.sort);
  if (s.plan) h.set('plan', s.plan);
  const str = h.toString();
  const next = str ? '#' + str : location.pathname;
  if (location.hash.replace(/^#/, '') !== str) history.replaceState(null, '', next);
}

function CommerceApp() {
  const [status, setStatus] = useState('loading');
  const [err, setErr] = useState('');
  const [parcels, setParcels] = useState(null);
  const [plans, setPlans] = useState({});
  const init = readHash();
  const [q, setQ] = useState(init.q);
  const [fStatus, setFStatus] = useState(init.status);
  const [fSrc, setFSrc] = useState(init.src);
  const [fReal, setFReal] = useState(init.real);
  const [sort, setSort] = useState(init.sort);
  const [sel, setSel] = useState(init.plan);
  const mapRef = useRef(null);
  const layerRef = useRef(null);
  const selRef = useRef(sel);
  selRef.current = sel;

  useEffect(() => { writeHash({ q, status: fStatus, src: fSrc, real: fReal, sort, plan: sel }); },
    [q, fStatus, fSrc, fReal, sort, sel]);

  /* ---------------------------------------------------------------- load */
  useEffect(() => {
    Promise.all([
      fetch(PARCELS_URL).then(r => { if (!r.ok) throw new Error('parcels ' + r.status); return r.json(); }),
      fetch(PLANS_URL).then(r => { if (!r.ok) throw new Error('plans ' + r.status); return r.json(); }),
    ]).then(([gj, pl]) => {
      setParcels(gj);
      setPlans(pl.by_plan || {});
      setStatus('ready');
    }).catch(e => { setErr(String(e)); setStatus('error'); });
  }, []);

  /* -------------------------------------------------------------- derived */
  const rows = useMemo(() => {
    const out = [];
    for (const pn in plans) {
      const p = plans[pn];
      const r = realization(p);
      out.push({
        plan: pn, name: p.pl_name || '', taba: p.taba,
        st: p.station_desc || '', commerce: p.commerce_sqm || 0,
        employment: p.employment_sqm || 0, mixed: p.mixed_other_sqm || 0,
        // Commerce+employment the plan grants as one figure without resolving the
        // mix. Kept out of `commerce` on purpose — folding it in would overstate
        // commerce by the whole combined amount.
        unsplit: p.ce_unsplit_sqm || 0, note: p.split_note || '',
        src: p.split_source || '', t5: p.t5_status,
        parcels: p.parcel_count || 0,
        hasFile: p.has_licensing_file, permits: p.permit_count,
        issued: p.any_issued, building: p.in_construction,
        done: p.completed, form4: p.form4_date, real: r,
        total: (p.commerce_sqm || 0) + (p.employment_sqm || 0) + (p.ce_unsplit_sqm || 0),
      });
    }
    return out;
  }, [plans]);

  const filtered = useMemo(() => {
    const needle = q.trim();
    let out = rows.filter(r => {
      if (fStatus !== 'all' && r.st !== fStatus) return false;
      if (fSrc !== 'all' && r.src !== fSrc) return false;
      if (fReal !== 'all' && r.real.key !== fReal) return false;
      if (needle && !(r.plan.includes(needle) || r.name.includes(needle) || r.taba.includes(needle)))
        return false;
      return true;
    });
    const cmp = {
      commerce: (a, b) => b.commerce - a.commerce,
      employment: (a, b) => b.employment - a.employment,
      total: (a, b) => b.total - a.total,
      unsplit: (a, b) => b.unsplit - a.unsplit,
      plan: (a, b) => a.plan.localeCompare(b.plan),
      name: (a, b) => a.name.localeCompare(b.name, 'he'),
    }[sort] || ((a, b) => b.commerce - a.commerce);
    return out.sort(cmp);
  }, [rows, q, fStatus, fSrc, fReal, sort]);

  const totals = useMemo(() => {
    const t = { c: 0, e: 0, m: 0, u: 0, plans: filtered.length, withData: 0,
                file: 0, issued: 0, building: 0, done: 0, approved: 0 };
    for (const r of filtered) {
      t.c += r.commerce; t.e += r.employment; t.m += r.mixed; t.u += r.unsplit;
      if (r.commerce || r.employment || r.unsplit) t.withData++;
      if (r.st === 'אישור') {
        t.approved++;
        if (r.hasFile) t.file++;
        if (r.issued) t.issued++;
        if (r.building) t.building++;
        if (r.done) t.done++;
      }
    }
    return t;
  }, [filtered]);

  const statusOpts = useMemo(() =>
    Array.from(new Set(rows.map(r => r.st).filter(Boolean))).sort(), [rows]);
  const srcOpts = useMemo(() =>
    Array.from(new Set(rows.map(r => r.src).filter(Boolean))).sort(), [rows]);

  /* ------------------------------------------------------------------ map */
  useEffect(() => {
    if (status !== 'ready' || mapRef.current) return;
    const map = L.map('cmap', { center: CENTER, zoom: ZOOM, zoomControl: false, maxZoom: 19, minZoom: 10 });
    L.control.zoom({ position: 'topleft' }).addTo(map);
    // CARTO watermarks anonymous tiles ("API KEY REQUIRED"), so the base is
    // OpenFreeMap's Bright style via MapLibre (free, keyless). The RTL plugin is
    // what keeps Hebrew labels from rendering reversed.
    if (window.L && L.maplibreGL) {
      try {
        if (window.maplibregl && maplibregl.getRTLTextPluginStatus &&
            maplibregl.getRTLTextPluginStatus() === 'unavailable') {
          maplibregl.setRTLTextPlugin('https://unpkg.com/@mapbox/mapbox-gl-rtl-text@0.2.3/mapbox-gl-rtl-text.js', null, true);
        }
      } catch (e) { console.warn('[basemap] RTL plugin:', e); }
      const glLayer = L.maplibreGL({
        style: 'https://tiles.openfreemap.org/styles/bright',
        attribution: '© OpenFreeMap © OpenMapTiles © OpenStreetMap contributors',
        interactive: false, preserveDrawingBuffer: true,
        canvasContextAttributes: { preserveDrawingBuffer: true },
      }).addTo(map);
      const glMap = glLayer.getMaplibreMap && glLayer.getMaplibreMap();
      if (glMap) {
        const hebrewLabels = () => {
          try {
            (glMap.getStyle().layers || []).forEach(l => {
              if (l.type !== 'symbol') return;
              const tf = glMap.getLayoutProperty(l.id, 'text-field');
              if (tf === undefined || !JSON.stringify(tf).includes('name')) return;
              glMap.setLayoutProperty(l.id, 'text-field',
                ['coalesce', ['get', 'name:he'], ['get', 'name:nonlatin'], ['get', 'name'], ['get', 'name:latin']]);
            });
            glMap.triggerRepaint();
          } catch (e) { console.warn('[basemap] label rewrite:', e); }
        };
        glMap.on('style.load', hebrewLabels);
        if (glMap.isStyleLoaded()) hebrewLabels();
      }
    } else {
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
        { maxZoom: 19, attribution: '© OpenStreetMap contributors' }).addTo(map);
    }
    mapRef.current = map;
  }, [status]);

  const visiblePlans = useMemo(() => new Set(filtered.map(r => r.plan)), [filtered]);

  // Draw / redraw parcels
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !parcels) return;
    if (layerRef.current) { map.removeLayer(layerRef.current); layerRef.current = null; }
    const feats = parcels.features.filter(f => visiblePlans.has(f.properties.plan_name));
    const layer = L.geoJSON({ type: 'FeatureCollection', features: feats }, {
      style: f => {
        const p = f.properties;
        const unres = p.ce_unsplit_sqm || 0;
        // A parcel granting 30,000 מ"ר of commerce+employment must not render as
        // empty just because the plan does not say how the mix divides.
        const has = (p.commerce_sqm || 0) + (p.employment_sqm || 0) + unres > 0;
        const isSel = selRef.current && p.plan_name === selRef.current;
        const col = !has ? COL.none
          : unres > 0 ? COL.unresolved
          : (p.dominant === 'employment' ? COL.employment : COL.commerce);
        return {
          color: isSel ? COL.sel : col,
          weight: isSel ? 3 : 1,
          // A parcel that genuinely mixes commerce AND employment is drawn with a
          // dashed edge rather than a fake precise fill — the same honesty rule the
          // district layer uses for `combined`.
          dashArray: (p.combined || unres > 0) ? '5,3' : null,
          fillColor: col,
          fillOpacity: has ? (isSel ? 0.65 : 0.42) : 0.12,
        };
      },
      onEachFeature: (f, lyr) => {
        const p = f.properties;
        lyr.on('click', () => setSel(p.plan_name));
        lyr.bindPopup(
          '<div class="ce-pop" style="min-width:230px">' +
          '<div style="font-weight:800;font-size:14px;margin-bottom:2px">' + esc(p.pl_name || p.plan_name) + '</div>' +
          '<div style="color:#c9a7d8;font-size:11px;margin-bottom:6px">' + esc(p.plan_name) + ' · מגרש ' + esc(p.parcel) + '</div>' +
          '<div style="font-size:12px;line-height:1.7">' +
          '<div>ייעוד: <b>' + esc(p.mavat_name) + '</b></div>' +
          '<div>מסחר: <b style="color:' + COL.commerce + '">' + fmt(p.commerce_sqm) + '</b> מ"ר</div>' +
          '<div>תעסוקה: <b style="color:' + COL.employment + '">' + fmt(p.employment_sqm) + '</b> מ"ר</div>' +
          (p.ce_unsplit_sqm ? '<div style="color:' + COL.unresolved + '">מסחר+תעסוקה יחד (לא מפוצל): <b>' + fmt(p.ce_unsplit_sqm) + '</b> מ"ר</div>' : '') +
          (p.mixed_other_sqm ? '<div style="color:#c9a7d8">מעורב עם שימוש אחר: ' + fmt(p.mixed_other_sqm) + ' מ"ר</div>' : '') +
          '<div style="margin-top:5px;color:#9f86ab;font-size:11px">מקור: ' + esc(p.split_source || '—') + '</div>' +
          '</div></div>');
      },
    }).addTo(map);
    layerRef.current = layer;
  }, [parcels, visiblePlans]);

  // Restyle + zoom on selection
  useEffect(() => {
    const lyr = layerRef.current;
    if (!lyr) return;
    lyr.setStyle && lyr.resetStyle && lyr.eachLayer(l => lyr.resetStyle(l));
    if (!sel) return;
    const map = mapRef.current;
    let bounds = null;
    lyr.eachLayer(l => {
      if (l.feature && l.feature.properties.plan_name === sel) {
        l.setStyle({ color: COL.sel, weight: 3 });
        l.bringToFront();
        bounds = bounds ? bounds.extend(l.getBounds()) : l.getBounds();
      }
    });
    if (bounds && bounds.isValid() && map) map.fitBounds(bounds, { padding: [60, 60], maxZoom: 17 });
  }, [sel, parcels, visiblePlans]);

  /* --------------------------------------------------------------- export */
  const exportCsv = useCallback(() => {
    const head = ['מספר תכנית', 'שם', 'תב"ע', 'סטטוס', 'מסחר מ"ר', 'תעסוקה מ"ר',
      'מסחר+תעסוקה יחד מ"ר', 'מעורב מ"ר', 'מקור הפיצול', 'מגרשים', 'טבלה 5', 'תיק רישוי', 'מס\' תיקים',
      'הופק היתר', 'בביצוע', 'גמר/טופס 4', 'תאריך גמר', 'מצב מימוש'];
    const yn = v => v === true ? 'כן' : v === false ? 'לא' : '';
    const lines = [head.join(',')];
    for (const r of filtered) {
      lines.push([r.plan, r.name, r.taba, r.st, r.commerce, r.employment, r.unsplit, r.mixed,
        r.src, r.parcels, r.t5, yn(r.hasFile), r.permits == null ? '' : r.permits,
        yn(r.issued), yn(r.building), yn(r.done), r.form4 || '', r.real.label]
        .map(v => {
          const s = String(v == null ? '' : v);
          return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
        }).join(','));
    }
    // BOM so Excel opens the Hebrew as UTF-8 rather than as mojibake.
    const uri = 'data:text/csv;charset=utf-8,' + encodeURIComponent('﻿' + lines.join('\r\n'));
    const a = document.createElement('a');
    a.href = uri; a.download = 'מסחר_ותעסוקה_עירוני.csv';
    document.body.appendChild(a); a.click(); a.remove();
  }, [filtered]);

  /* ------------------------------------------------------------------- ui */
  if (status === 'loading') return null;
  if (status === 'error') return (
    <div style={{ height: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', flexDirection: 'column', gap: 10 }}>
      <div style={{ fontSize: 34 }}>⚠️</div>
      <div style={{ fontWeight: 700 }}>שגיאה בטעינת הנתונים</div>
      <div style={{ color: '#c9a7d8', fontSize: 12 }}>{err}</div>
    </div>
  );

  const S = {
    sel: { background: '#1d1023', color: '#f0e9f3', border: '1px solid #4a2c58', borderRadius: 7, padding: '5px 7px', fontSize: 12, maxWidth: 190 },
    btn: { background: '#3b1d47', color: '#f0e9f3', border: '1px solid #6b3f7d', borderRadius: 7, padding: '6px 11px', fontSize: 12, cursor: 'pointer', fontWeight: 600 },
    th: { textAlign: 'right', padding: '7px 8px', position: 'sticky', top: 0, background: '#231029', borderBottom: '1px solid #4a2c58', fontSize: 11, color: '#c9a7d8', whiteSpace: 'nowrap', cursor: 'pointer' },
    td: { padding: '6px 8px', borderBottom: '1px solid #2a1533', fontSize: 12, whiteSpace: 'nowrap' },
  };
  const kpi = (label, val, col) => (
    <div style={{ padding: '4px 10px', background: '#231029', border: '1px solid #3b1d47', borderRadius: 8, minWidth: 74 }}>
      <div style={{ fontSize: 10, color: '#9f86ab' }}>{label}</div>
      <div style={{ fontSize: 15, fontWeight: 800, color: col || '#f0e9f3' }}>{val}</div>
    </div>
  );

  return (
    <div style={{ height: '100vh', display: 'flex', flexDirection: 'column' }}>
      <header className="noprint" style={{ padding: '8px 14px', background: '#1a0c20', borderBottom: '1px solid #3b1d47', display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
        <div style={{ fontWeight: 800, fontSize: 16, marginLeft: 6 }}>🏪 מסחר, תעסוקה ומשרדים — מיפוי עירוני</div>
        {kpi('תכניות', fmt(totals.plans))}
        {kpi('מסחר מ"ר', fmt(totals.c), COL.commerce)}
        {kpi('תעסוקה מ"ר', fmt(totals.e), COL.employment)}
        {totals.u > 0 && kpi('מסחר+תעסוקה יחד', fmt(totals.u), '#e0b0ff')}
        {totals.m > 0 && kpi('מעורב מ"ר', fmt(totals.m), '#c9a7d8')}
        {kpi('מאושרות', fmt(totals.approved), '#4ade80')}
        {kpi('ברישוי', fmt(totals.file), '#7fb3e8')}
        {kpi('בביצוע', fmt(totals.building), '#f0824b')}
        {kpi('גמר בנייה', fmt(totals.done), '#0d9488')}
        <div style={{ flex: 1 }} />
        <button style={S.btn} onClick={exportCsv}>⬇ ייצוא לאקסל</button>
        <button style={S.btn} onClick={() => window.print()}>🖨 הדפסה</button>
      </header>

      <div className="noprint" style={{ padding: '7px 14px', background: '#1d1023', borderBottom: '1px solid #3b1d47', display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
        <input style={{ ...S.sel, minWidth: 190 }} placeholder="חיפוש: מספר תכנית / שם / תב״ע"
          value={q} onChange={e => setQ(e.target.value)} />
        <select style={S.sel} value={fStatus} onChange={e => setFStatus(e.target.value)}>
          <option value="all">כל הסטטוסים</option>
          {statusOpts.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
        <select style={S.sel} value={fReal} onChange={e => setFReal(e.target.value)}>
          <option value="all">כל מצבי המימוש</option>
          <option value="done">גמר בנייה / טופס 4</option>
          <option value="building">בביצוע</option>
          <option value="permit">הופק היתר</option>
          <option value="licensing">ברישוי</option>
          <option value="none">אין תיק רישוי</option>
          <option value="unknown">טרם נבדק</option>
          <option value="notapproved">טרם אושרה</option>
        </select>
        <select style={S.sel} value={fSrc} onChange={e => setFSrc(e.target.value)}>
          <option value="all">כל מקורות הפיצול</option>
          {srcOpts.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
        <select style={S.sel} value={sort} onChange={e => setSort(e.target.value)}>
          <option value="commerce">מיון: מסחר</option>
          <option value="employment">מיון: תעסוקה</option>
          <option value="total">מיון: סה״כ</option>
          <option value="unsplit">מיון: מסחר+תעסוקה יחד</option>
          <option value="plan">מיון: מספר תכנית</option>
          <option value="name">מיון: שם</option>
        </select>
        {sel && <button style={S.btn} onClick={() => setSel('')}>✕ נקה בחירה</button>}
        <div style={{ fontSize: 11, color: '#9f86ab' }}>
          <span style={{ color: COL.commerce }}>■</span> מסחר &nbsp;
          <span style={{ color: COL.employment }}>■</span> תעסוקה &nbsp;
          <span style={{ color: COL.unresolved }}>■</span> יחד (לא מפוצל) &nbsp;
          <span style={{ borderBottom: '2px dashed #c9a7d8' }}>מקווקו</span> = מעורב/לא מפוצל
        </div>
      </div>

      <div className="ce-split" style={{ flex: 1, display: 'flex', minHeight: 0 }}>
        <div className="ce-list" style={{ width: '46%', minWidth: 380, overflow: 'auto', borderLeft: '1px solid #3b1d47' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <thead>
              <tr>
                <th style={S.th} onClick={() => setSort('plan')}>תכנית</th>
                <th style={S.th} onClick={() => setSort('name')}>שם</th>
                <th style={S.th}>סטטוס</th>
                <th style={S.th} onClick={() => setSort('commerce')}>מסחר</th>
                <th style={S.th} onClick={() => setSort('employment')}>תעסוקה</th>
                <th style={S.th} onClick={() => setSort('unsplit')} title="שטח שהתכנית נותנת למסחר ותעסוקה יחד בלי לפצל">יחד</th>
                <th style={S.th}>מקור</th>
                <th style={S.th}>מימוש</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map(r => (
                <tr key={r.plan} onClick={() => setSel(r.plan)}
                    style={{ cursor: 'pointer', background: sel === r.plan ? '#3b1d47' : 'transparent' }}>
                  <td style={{ ...S.td, fontFamily: 'monospace', fontSize: 11 }}>{r.plan}</td>
                  <td style={{ ...S.td, maxWidth: 190, overflow: 'hidden', textOverflow: 'ellipsis' }} title={r.name}>{r.name}</td>
                  <td style={{ ...S.td, color: STATUS_COL[r.st] || '#94a3b8', fontSize: 11 }}>{r.st}</td>
                  <td style={{ ...S.td, color: COL.commerce, fontWeight: 700 }}>{r.commerce ? fmt(r.commerce) : '—'}</td>
                  <td style={{ ...S.td, color: COL.employment, fontWeight: 700 }}>{r.employment ? fmt(r.employment) : '—'}</td>
                  <td style={{ ...S.td, color: '#e0b0ff', fontWeight: 700 }} title={r.note}>{r.unsplit ? fmt(r.unsplit) : '—'}</td>
                  <td style={{ ...S.td, fontSize: 10, color: r.src.includes('טרם פוצל') ? '#fb923c' : '#9f86ab', maxWidth: 130, overflow: 'hidden', textOverflow: 'ellipsis' }} title={r.src}>
                    {r.t5 === 'no_xlsx' ? 'אין טבלה 5' : (r.src || '—')}
                  </td>
                  <td style={{ ...S.td, color: r.real.col, fontSize: 11 }}>
                    {r.real.label}{r.permits ? ' (' + r.permits + ')' : ''}
                  </td>
                </tr>
              ))}
              {!filtered.length && (
                <tr><td colSpan={8} style={{ ...S.td, textAlign: 'center', color: '#9f86ab', padding: 24 }}>אין תוצאות</td></tr>
              )}
            </tbody>
          </table>
        </div>
        <div id="cmap" style={{ flex: 1, minWidth: 0 }} />
      </div>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById('root')).render(<CommerceApp />);
