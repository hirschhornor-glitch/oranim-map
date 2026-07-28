/* junction.jsx — "צומת אורנים: איך מגיעים מ-A ל-B?"
 * Standalone mini-app. Turn-aware routing over an OSM-derived directed graph,
 * with a declarative, toggleable change layer (Today vs. Final plan + experiments).
 * Compiled to junction.js by Babel (preset-react). No bundler; React/Leaflet from CDN.
 */
const { useState, useEffect, useRef, useMemo, useCallback } = React;

const GRAPH_URL = 'data/junction_graph.json?v=2026-07-27b';
const CHANGES_URL = 'data/junction_changes.json?v=2026-07-27f';
const LRT_URL = 'data/junction_lrt.json?v=2026-07-26';
const LRT_COL = { '3': '#22c3a6', '8': '#b06cff' };
const CENTER = [31.7573, 35.2150];
const ZOOM = 16;

const COL = {
  today: '#f4a13a',   // amber
  final: '#22c3a6',   // teal
  a: '#2ecc71', b: '#e5484d',
  banned: '#e5484d',
};

const LANDMARKS = [
  { name: 'פייר קניג דרום (ליד הרחוב החדש)', lat: 31.756462, lng: 35.214686 },
  { name: 'צומת רבקה (פייר קניג × רבקה)', lat: 31.755145, lng: 35.214455 },
  { name: 'צומת מקור חיים (פייר קניג × יהודה)', lat: 31.757987, lng: 35.215016 },
  { name: 'צומת בן זכאי / עמק רפאים (צפון)', lat: 31.759527, lng: 35.215198 },
  { name: 'תחנת רק"ל אורנים (משוער)', lat: 31.75700, lng: 35.21510 },
  { name: 'בי"ס מקור חיים (משוער)', lat: 31.756749, lng: 35.213441 },
  { name: 'יהודה × נפתלי', lat: 31.757821, lng: 35.216860 },
];

/* ------------------------------------------------------------------ geo */
function haversine(a, b) { // [lng,lat]
  const R = 6371000, tr = Math.PI / 180;
  const dLat = (b[1] - a[1]) * tr, dLon = (b[0] - a[0]) * tr;
  const la1 = a[1] * tr, la2 = b[1] * tr;
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(la1) * Math.cos(la2) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
}
function polyLen(coords) { let s = 0; for (let i = 0; i < coords.length - 1; i++) s += haversine(coords[i], coords[i + 1]); return s; }
function bearing(a, b) { // [lng,lat] -> deg 0..360
  const tr = Math.PI / 180;
  const y = Math.sin((b[0] - a[0]) * tr) * Math.cos(b[1] * tr);
  const x = Math.cos(a[1] * tr) * Math.sin(b[1] * tr) - Math.sin(a[1] * tr) * Math.cos(b[1] * tr) * Math.cos((b[0] - a[0]) * tr);
  return (Math.atan2(y, x) / tr + 360) % 360;
}
function sideOf(via, nb) { // dominant compass of nb relative to via, both {lng,lat}
  const dx = (nb.lng - via.lng) * Math.cos(via.lat * Math.PI / 180), dy = nb.lat - via.lat;
  if (Math.abs(dy) >= Math.abs(dx)) return dy >= 0 ? 'N' : 'S';
  return dx >= 0 ? 'E' : 'W';
}

/* ------------------------------------------------- graph working-form + mutations */
function toWorking(base) {
  const nodes = {};
  for (const k in base.nodes) nodes[k] = Object.assign({}, base.nodes[k]);
  const edges = {};
  for (const e of base.edges) edges[e.id] = Object.assign({}, e, { modes: Object.assign({}, e.modes) });
  const turns = (base.turnRestrictions || []).map(t => Object.assign({}, t));
  return { nodes, edges, turns };
}
function cloneWG(wg) {
  const nodes = {}; for (const k in wg.nodes) nodes[k] = Object.assign({}, wg.nodes[k]);
  const edges = {}; for (const k in wg.edges) { const e = wg.edges[k]; edges[k] = Object.assign({}, e, { modes: Object.assign({}, e.modes), geometry: e.geometry }); }
  const turns = wg.turns.map(t => Object.assign({}, t));
  return { nodes, edges, turns };
}
function resolveTurn(wg, spec) {
  const via = wg.nodes[spec.viaNode]; if (!via) return null;
  let bestIn = null, bestOut = null, si = -2, so = -2;
  const want = (side, node) => (sideOf(via, node) === side ? 1 : 0);
  for (const id in wg.edges) {
    const e = wg.edges[id];
    if (e.to === spec.viaNode && spec.fromName && (e.name || '').indexOf(spec.fromName) >= 0) {
      const sc = want(spec.fromSide, wg.nodes[e.from]); if (sc > si) { si = sc; bestIn = e; }
    }
    if (e.from === spec.viaNode && spec.toName && (e.name || '').indexOf(spec.toName) >= 0) {
      const sc = want(spec.toSide, wg.nodes[e.to]); if (sc > so) { so = sc; bestOut = e; }
    }
  }
  if (!bestIn || !bestOut) return null;
  return { fromEdge: bestIn.id, toEdge: bestOut.id, viaNode: spec.viaNode };
}
function applyMutation(wg, m, warn) {
  switch (m.op) {
    case 'addNode':
      wg.nodes[m.id] = { id: m.id, lng: m.lng, lat: m.lat, landmark: m.landmark, synthetic: true }; break;
    case 'addEdge': {
      const fn = wg.nodes[m.from], tn = wg.nodes[m.to];
      if (!fn || !tn) { warn('addEdge missing node ' + m.from + '/' + m.to); break; }
      const geom = m.geometry || [[fn.lng, fn.lat], [tn.lng, tn.lat]];
      const len = m.length != null ? m.length : polyLen(geom);
      wg.edges[m.id] = { id: m.id, from: m.from, to: m.to, name: m.name || '', length: Math.round(len * 100) / 100, modes: Object.assign({ car: true, walk: true }, m.modes), oneway: !m.bidirectional, pairId: null, geometry: geom, synthetic: true, note: m.note };
      if (m.bidirectional) {
        const rid = m.id + '_r';
        wg.edges[rid] = { id: rid, from: m.to, to: m.from, name: m.name || '', length: wg.edges[m.id].length, modes: Object.assign({}, wg.edges[m.id].modes), oneway: false, pairId: m.id, geometry: geom.slice().reverse(), synthetic: true, note: m.note };
        wg.edges[m.id].pairId = rid; wg.edges[m.id].oneway = false;
      }
      break;
    }
    case 'removeEdge': {
      const ed = wg.edges[m.edgeId]; if (!ed) { warn('removeEdge missing ' + m.edgeId); break; }
      if (m.pair !== false && ed.pairId && wg.edges[ed.pairId]) delete wg.edges[ed.pairId];
      delete wg.edges[m.edgeId];
      wg.turns = wg.turns.filter(t => t.fromEdge !== m.edgeId && t.toEdge !== m.edgeId);
      break;
    }
    case 'modifyEdge': { const ed = wg.edges[m.edgeId]; if (ed) Object.assign(ed, m.set || {}); break; }
    case 'setOneway': {
      const ed = wg.edges[m.edgeId]; if (!ed) { warn('setOneway missing ' + m.edgeId); break; }
      if (m.oneway === false) {
        if (ed.pairId && wg.edges[ed.pairId]) { ed.oneway = false; break; }
        const rid = m.edgeId + '_2w';
        wg.edges[rid] = { id: rid, from: ed.to, to: ed.from, name: ed.name, length: ed.length, modes: Object.assign({}, ed.modes), oneway: false, pairId: ed.id, geometry: ed.geometry.slice().reverse(), synthetic: true, note: ed.note };
        ed.pairId = rid; ed.oneway = false;
      } else {
        if (ed.pairId && wg.edges[ed.pairId]) delete wg.edges[ed.pairId];
        ed.pairId = null; ed.oneway = true;
      }
      break;
    }
    case 'addTurnRestriction': {
      let fromEdge = m.fromEdge, toEdge = m.toEdge, viaNode = m.viaNode;
      if (!fromEdge || !toEdge) {
        const r = resolveTurn(wg, m);
        if (!r) { warn('turn "' + (m.id || '') + '" unresolved'); break; }
        fromEdge = r.fromEdge; toEdge = r.toEdge; viaNode = r.viaNode;
      }
      wg.turns.push({ id: m.id, fromEdge, viaNode, toEdge, type: m.type || 'no_turn', note: m.note, added: true });
      break;
    }
    case 'removeTurnRestriction':
      wg.turns = wg.turns.filter(t => t.id !== m.id); break;
    default: warn('unknown op ' + m.op);
  }
}
function buildEffective(baseWG, changes, activeIds, includeFinal) {
  const wg = cloneWG(baseWG);
  const notes = [], features = [];
  const warn = (msg) => console.warn('[change]', msg);
  for (const cs of (changes.changesets || [])) {
    const on = cs.isFinalPlan ? includeFinal : activeIds.has(cs.id);
    if (!on) continue;
    for (const m of cs.mutations) {
      if (m.op === 'markRoad') { // visual-only highlight (existing edges or explicit geometry)
        let segs;
        if (m.geometry) segs = Array.isArray(m.geometry[0][0]) ? m.geometry : [m.geometry];
        else segs = (m.edgeIds || []).map(id => wg.edges[id]).filter(Boolean).map(e => e.geometry);
        if (segs.length) features.push({ kind: 'marked', style: m.style || 'new_road', label: m.label, segs });
        if (m.note) notes.push({ cs: cs.id, note: m.note });
        continue;
      }
      let preGeom = null;
      if (m.op === 'removeEdge' && wg.edges[m.edgeId]) preGeom = wg.edges[m.edgeId].geometry;
      applyMutation(wg, m, warn);
      if (m.note) notes.push({ cs: cs.id, note: m.note });
      if (m.op === 'addEdge' && wg.edges[m.id]) { if (m.noMark) { /* routing-only segment */ } else if (m.style) features.push({ kind: 'marked', style: m.style, label: m.name || m.note, segs: [wg.edges[m.id].geometry] }); else features.push({ kind: 'new_road', coords: wg.edges[m.id].geometry, label: m.name || m.note }); }
      else if (m.op === 'addTurnRestriction') { const t = wg.turns[wg.turns.length - 1]; const nd = t && wg.nodes[t.viaNode]; if (nd) { const mm = (m.note || '').match(/#(\d+)/); features.push({ kind: 'ban', lng: nd.lng, lat: nd.lat, label: m.note, num: mm ? mm[1] : '' }); } }
      else if (m.op === 'setOneway' && m.oneway === false && wg.edges[m.edgeId]) features.push({ kind: 'twoway', coords: wg.edges[m.edgeId].geometry, label: m.note });
      else if (m.op === 'removeEdge' && preGeom) features.push({ kind: 'closed', coords: preGeom, label: m.note });
    }
  }
  return { wg, notes, features };
}

/* ---------------------------------------------------------------- routing */
function MinHeap() { this.a = []; }
MinHeap.prototype.push = function (item) { const a = this.a; a.push(item); let i = a.length - 1; while (i > 0) { const p = (i - 1) >> 1; if (a[p].c <= a[i].c) break; [a[p], a[i]] = [a[i], a[p]]; i = p; } };
MinHeap.prototype.pop = function () { const a = this.a; const top = a[0], last = a.pop(); if (a.length) { a[0] = last; let i = 0; for (;;) { let l = 2 * i + 1, r = l + 1, s = i; if (l < a.length && a[l].c < a[s].c) s = l; if (r < a.length && a[r].c < a[s].c) s = r; if (s === i) break; [a[s], a[i]] = [a[i], a[s]]; i = s; } } return top; };
MinHeap.prototype.size = function () { return this.a.length; };

function carIndex(wg) {
  const out = {}; // nodeId -> [edge]
  for (const id in wg.edges) { const e = wg.edges[id]; if (!e.modes.car) continue; (out[e.from] || (out[e.from] = [])).push(e); }
  const banned = new Set(), only = {};
  for (const t of wg.turns) {
    if (/^only_/.test(t.type)) { const k = t.fromEdge + '|' + t.viaNode; (only[k] || (only[k] = new Set())).add(t.toEdge); }
    else banned.add(t.fromEdge + '|' + t.viaNode + '|' + t.toEdge);
  }
  return { out, banned, only };
}
function routeCar(wg, aNode, bNode) {
  const { out, banned, only } = carIndex(wg);
  const dist = {}, prev = {};
  const heap = new MinHeap();
  for (const e of (out[aNode] || [])) { if (dist[e.id] == null || e.length < dist[e.id]) { dist[e.id] = e.length; prev[e.id] = null; heap.push({ id: e.id, c: e.length }); } }
  let goalEdge = null;
  while (heap.size()) {
    const { id, c } = heap.pop();
    if (c > dist[id]) continue;
    const e = wg.edges[id];
    if (e.to === bNode) { goalEdge = id; break; }
    const via = e.to;
    for (const e2 of (out[via] || [])) {
      if (e2.id === e.pairId) continue; // no U-turn
      if (banned.has(e.id + '|' + via + '|' + e2.id)) continue;
      const ok = only[e.id + '|' + via]; if (ok && !ok.has(e2.id)) continue;
      const nc = c + e2.length;
      if (dist[e2.id] == null || nc < dist[e2.id]) { dist[e2.id] = nc; prev[e2.id] = e.id; heap.push({ id: e2.id, c: nc }); }
    }
  }
  if (goalEdge == null) return null;
  const seq = []; let cur = goalEdge; while (cur != null) { seq.unshift(wg.edges[cur]); cur = prev[cur]; }
  return orientedPath(seq.map(e => ({ e, rev: false })), dist[goalEdge]);
}
function walkIndex(wg) {
  const adj = {}; // nodeId -> [{to, len, edge, rev}]
  for (const id in wg.edges) {
    const e = wg.edges[id]; if (!e.modes.walk) continue;
    (adj[e.from] || (adj[e.from] = [])).push({ to: e.to, len: e.length, edge: e, rev: false });
    (adj[e.to] || (adj[e.to] = [])).push({ to: e.from, len: e.length, edge: e, rev: true });
  }
  return adj;
}
function routeWalk(wg, aNode, bNode) {
  const adj = walkIndex(wg);
  const dist = {}, prev = {};
  const heap = new MinHeap(); dist[aNode] = 0; heap.push({ id: aNode, c: 0 });
  while (heap.size()) {
    const { id, c } = heap.pop(); if (c > dist[id]) continue; if (id === bNode) break;
    for (const nb of (adj[id] || [])) { const nc = c + nb.len; if (dist[nb.to] == null || nc < dist[nb.to]) { dist[nb.to] = nc; prev[nb.to] = { from: id, edge: nb.edge, rev: nb.rev }; heap.push({ id: nb.to, c: nc }); } }
  }
  if (dist[bNode] == null) return null;
  const steps = []; let cur = bNode; while (prev[cur]) { steps.unshift({ e: prev[cur].edge, rev: prev[cur].rev }); cur = prev[cur].from; }
  return orientedPath(steps, dist[bNode]);
}
function orientedPath(steps, length) {
  const path = steps.map(s => ({ id: s.e.id, name: s.e.name, length: s.e.length, note: s.e.note, coords: s.rev ? s.e.geometry.slice().reverse() : s.e.geometry }));
  const coords = [];
  for (const p of path) for (const c of p.coords) { if (!coords.length || coords[coords.length - 1][0] !== c[0] || coords[coords.length - 1][1] !== c[1]) coords.push(c); }
  return { path, coords, length: Math.round(length) };
}
function nearestNode(wg, latlng, mode) {
  // routable nodes = those touched by a usable edge
  let best = null, bd = Infinity;
  const touched = {};
  for (const id in wg.edges) { const e = wg.edges[id]; if (mode === 'car' ? e.modes.car : e.modes.walk) { touched[e.from] = 1; touched[e.to] = 1; } }
  for (const id in touched) { const n = wg.nodes[id]; if (!n) continue; const d = haversine([latlng.lng, latlng.lat], [n.lng, n.lat]); if (d < bd) { bd = d; best = id; } }
  return best;
}
function route(wg, mode, A, B) {
  if (!A || !B) return null;
  const an = nearestNode(wg, A, mode), bn = nearestNode(wg, B, mode);
  if (!an || !bn || an === bn) return null;
  return mode === 'car' ? routeCar(wg, an, bn) : routeWalk(wg, an, bn);
}

/* --- transit: walk to nearest LRT boarding point, ride the network, walk to B --- */
function buildLrtGraph(lrtSegs) {
  const key = (p) => p[0].toFixed(6) + ',' + p[1].toFixed(6);
  const adj = {}, coord = {}, lineAt = {};
  for (const s of lrtSegs) {
    for (let i = 0; i < s.coords.length; i++) {
      const k = key(s.coords[i]); coord[k] = s.coords[i]; lineAt[k] = s.line;
      if (i > 0) {
        const k0 = key(s.coords[i - 1]); const d = haversine(s.coords[i - 1], s.coords[i]);
        (adj[k0] || (adj[k0] = [])).push({ to: k, d }); (adj[k] || (adj[k] = [])).push({ to: k0, d });
      }
    }
  }
  return { adj, coord, lineAt, keys: Object.keys(coord) };
}
function nearestLrt(lrt, ll) {
  let best = null, bd = Infinity;
  for (const k of lrt.keys) { const d = haversine([ll.lng, ll.lat], lrt.coord[k]); if (d < bd) { bd = d; best = k; } }
  return { k: best, d: bd };
}
function routeTransit(lrt, A, B) {
  if (!lrt || !lrt.keys.length) return null;
  const board = nearestLrt(lrt, A), alight = nearestLrt(lrt, B);
  if (!board.k || !alight.k) return null;
  // dijkstra on LRT graph
  const dist = {}, prev = {}; const heap = new MinHeap(); dist[board.k] = 0; heap.push({ id: board.k, c: 0 });
  while (heap.size()) { const { id, c } = heap.pop(); if (c > dist[id]) continue; if (id === alight.k) break; for (const nb of (lrt.adj[id] || [])) { const nc = c + nb.d; if (dist[nb.to] == null || nc < dist[nb.to]) { dist[nb.to] = nc; prev[nb.to] = id; heap.push({ id: nb.to, c: nc }); } } }
  if (dist[alight.k] == null) return null;
  const rideKeys = []; let cur = alight.k; while (cur != null) { rideKeys.unshift(cur); cur = prev[cur]; }
  const ride = rideKeys.map(k => lrt.coord[k]);
  const walkA = [[A.lng, A.lat], lrt.coord[board.k]];
  const walkB = [lrt.coord[alight.k], [B.lng, B.lat]];
  const lines = Array.from(new Set(rideKeys.map(k => lrt.lineAt[k])));
  const rideLen = haversine ? ride.reduce((s, p, i) => i ? s + haversine(ride[i - 1], p) : 0, 0) : 0;
  return { transit: true, walkA, ride, walkB, lines, coords: walkA.concat(ride, walkB.slice(1)),
    length: Math.round(board.d + rideLen + alight.d), walkDist: Math.round(board.d + alight.d), rideDist: Math.round(rideLen) };
}
function turnSteps(path) {
  if (!path || !path.path || !path.path.length) return [];
  // merge consecutive same-name edges into legs
  const legs = [];
  for (const p of path.path) {
    const last = legs[legs.length - 1];
    if (last && last.name === p.name) { last.length += p.length; last.coords = last.coords.concat(p.coords.slice(1)); }
    else legs.push({ name: p.name, length: p.length, coords: p.coords.slice() });
  }
  const steps = [];
  for (let i = 0; i < legs.length; i++) {
    let action;
    if (i === 0) action = 'start';
    else {
      const prev = legs[i - 1].coords, cur = legs[i].coords;
      const inB = bearing(prev[Math.max(0, prev.length - 2)], prev[prev.length - 1]);
      const outB = bearing(cur[0], cur[1] || cur[0]);
      let d = ((outB - inB + 540) % 360) - 180;
      if (Math.abs(d) < 25) action = 'straight'; else if (Math.abs(d) > 150) action = 'uturn'; else action = d > 0 ? 'right' : 'left';
    }
    steps.push({ name: legs[i].name || 'דרך ללא שם', length: Math.round(legs[i].length), action });
  }
  return steps;
}
const ACTION_HE = { start: 'צא אל', straight: 'המשך ישר אל', left: 'פנה שמאלה אל', right: 'פנה ימינה אל', uturn: 'פניית פרסה אל' };
const ACTION_ICON = { start: '●', straight: '↑', left: '↰', right: '↱', uturn: '↺' };

/* free-text scenario parser: "חסום <רחוב|edgeId>" / "חד-סטרי X" / "דו-סטרי X" */
function parseFreeText(str, baseWG) {
  const cmds = (str || '').split(/[\n;]+/).map(s => s.trim()).filter(Boolean);
  const muts = [], msgs = [];
  for (const cmd of cmds) {
    const m = cmd.match(/^(חסום|סגור|block|חד[- ]?סטרי|oneway|דו[- ]?סטרי|twoway|פתח|open)\s+(.+)$/i);
    if (!m) { msgs.push('✗ לא הובן: "' + cmd + '"'); continue; }
    const verb = m[1], target = m[2].trim();
    let ids = [];
    if (/^e\d+/.test(target) || /^x_/.test(target)) { if (baseWG.edges[target]) ids = [target]; }
    else { for (const id in baseWG.edges) { const nm = baseWG.edges[id].name || ''; if (nm && nm.indexOf(target) >= 0) ids.push(id); } }
    if (!ids.length) { msgs.push('✗ לא נמצא: "' + target + '"'); continue; }
    for (const id of ids) {
      if (/חסום|סגור|block/i.test(verb)) muts.push({ op: 'removeEdge', edgeId: id, pair: true });
      else if (/חד|oneway/i.test(verb)) muts.push({ op: 'setOneway', edgeId: id, oneway: true });
      else if (/פתח|open/i.test(verb)) muts.push({ op: 'modifyEdge', edgeId: id, set: { modes: { car: true, walk: true } } });
      else muts.push({ op: 'setOneway', edgeId: id, oneway: false });
    }
    msgs.push('✓ ' + cmd + ' (' + ids.length + ' מקטעים)');
  }
  return { muts, msg: msgs.join(' · ') };
}

/* ---------------------------------------------------------------- UI */
function Chip({ active, onClick, children, disabled, title }) {
  return <button title={title} disabled={disabled} onClick={onClick} style={{
    padding: '7px 12px', borderRadius: 9, border: '1px solid ' + (active ? '#39d1a8' : '#2c4a56'),
    background: active ? 'rgba(57,209,168,.16)' : 'rgba(255,255,255,.03)', color: disabled ? '#5c7482' : (active ? '#8ff0d6' : '#cfe0e6'),
    fontWeight: 700, fontSize: 13, cursor: disabled ? 'not-allowed' : 'pointer', opacity: disabled ? .6 : 1,
  }}>{children}</button>;
}

function JunctionApp() {
  const [status, setStatus] = useState('loading');
  const [errMsg, setErrMsg] = useState('');
  const baseRef = useRef(null); const changesRef = useRef(null); const lrtRef = useRef(null); const lrtDataRef = useRef(null);
  const [mode, setMode] = useState('car');
  const [A, setA] = useState(null); // no default route — only when the user picks A/B
  const [B, setB] = useState(null);
  const [pick, setPick] = useState('A');
  const [activeExp, setActiveExp] = useState(() => new Set());
  const [showToday, setShowToday] = useState(true);
  const [showFinal, setShowFinal] = useState(true);
  const [showChanges, setShowChanges] = useState(false);
  const [showBans, setShowBans] = useState(false);
  const [showCad, setShowCad] = useState(true); const cadRef = useRef(null);
  const [debug, setDebug] = useState(false);
  const [detailState, setDetailState] = useState('final'); // which turn list to show
  const [draw, setDraw] = useState(false); const [drawPts, setDrawPts] = useState([]);
  const drawRef = useRef(false); useEffect(() => { drawRef.current = draw; }, [draw]);
  const [freeText, setFreeText] = useState(''); const [freeMuts, setFreeMuts] = useState([]); const [freeMsg, setFreeMsg] = useState('');

  const mapRef = useRef(null); const layersRef = useRef({});
  const graphCache = useRef({});

  // street-name index (distinct names within ~1km of the junction) for free-text A/B
  const streets = useMemo(() => {
    if (status !== 'ready') return [];
    const base = baseRef.current, byName = {};
    for (const id in base.edges) {
      const e = base.edges[id]; const nm = (e.name || '').trim(); if (!nm) continue;
      for (const nid of [e.from, e.to]) {
        const n = base.nodes[nid]; if (!n) continue;
        const d = haversine([CENTER[1], CENTER[0]], [n.lng, n.lat]);
        if (!byName[nm] || d < byName[nm].d) byName[nm] = { d, lat: n.lat, lng: n.lng };
      }
    }
    const arr = Object.keys(byName).filter(nm => byName[nm].d <= 1000)
      .map(nm => ({ name: nm, lat: byName[nm].lat, lng: byName[nm].lng, d: byName[nm].d }))
      .sort((a, b) => a.d - b.d);
    const land = LANDMARKS.map(l => ({ name: l.name, lat: l.lat, lng: l.lng, d: 0, landmark: true }));
    return land.concat(arr);
  }, [status]);

  // load data
  useEffect(() => {
    Promise.all([fetch(GRAPH_URL).then(r => r.json()), fetch(CHANGES_URL).then(r => r.json()), fetch(LRT_URL).then(r => r.json()).catch(() => null)])
      .then(([g, c, lrt]) => { baseRef.current = toWorking(g); changesRef.current = c; if (lrt) { lrtDataRef.current = lrt; lrtRef.current = buildLrtGraph(lrt.segments || []); } setStatus('ready'); })
      .catch(err => { setErrMsg(String(err)); setStatus('error'); });
  }, []);

  const expIds = useMemo(() => Array.from(activeExp).sort().join(','), [activeExp]);
  function effective(includeFinal) {
    if (status !== 'ready') return null;
    const key = (includeFinal ? 'F' : 'T') + '|' + expIds;
    if (!graphCache.current[key]) graphCache.current[key] = buildEffective(baseRef.current, changesRef.current, activeExp, includeFinal);
    return graphCache.current[key];
  }
  // invalidate cache when experiments change
  useEffect(() => { graphCache.current = {}; }, [expIds]);

  const todayEff = effective(false), finalEff = effective(true);
  // transit: "today" has no LRT (walk), "final" rides the two new lines
  const applyFreeWG = (eff) => { if (!eff) return null; if (!freeMuts.length) return eff.wg; const wg = cloneWG(eff.wg); for (const m of freeMuts) applyMutation(wg, m, () => {}); return wg; };
  const todayWG = useMemo(() => applyFreeWG(todayEff), [todayEff, freeMuts]);
  const finalWG = useMemo(() => applyFreeWG(finalEff), [finalEff, freeMuts]);
  const routeToday = useMemo(() => (!todayWG || !A || !B) ? null : (mode === 'transit' ? routeWalk(todayWG, nearestNode(todayWG, A, 'walk'), nearestNode(todayWG, B, 'walk')) : route(todayWG, mode, A, B)), [todayWG, mode, A, B, status]);
  const routeFinal = useMemo(() => (!finalWG || !A || !B) ? null : (mode === 'transit' ? routeTransit(lrtRef.current, A, B) : route(finalWG, mode, A, B)), [finalWG, mode, A, B, status]);

  // init map
  useEffect(() => {
    if (status !== 'ready' || mapRef.current) return;
    const map = L.map('jmap', { center: CENTER, zoom: ZOOM, zoomControl: false, maxZoom: 20, minZoom: 14 });
    L.control.zoom({ position: 'topleft' }).addTo(map);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', { maxZoom: 20, attribution: '© OpenStreetMap, © CARTO', subdomains: 'abcd' }).addTo(map);
    map.on('click', (ev) => {
      const ll = { lat: ev.latlng.lat, lng: ev.latlng.lng };
      if (drawRef.current) { setDrawPts(pts => [...pts, [+ll.lng.toFixed(6), +ll.lat.toFixed(6)]]); return; }
      setPick(p => { if (p === 'A') { setA(ll); return 'B'; } setB(ll); return 'A'; });
    });
    mapRef.current = map;
    layersRef.current = { cad: L.layerGroup().addTo(map), routes: L.layerGroup().addTo(map), changes: L.layerGroup().addTo(map), bans: L.layerGroup().addTo(map), draw: L.layerGroup().addTo(map), markers: L.layerGroup().addTo(map), debug: L.layerGroup().addTo(map) };
  }, [status]);

  // draw planned-infrastructure overlay (new road, banned turns, two-way, closures)
  useEffect(() => {
    const map = mapRef.current; if (!map) return;
    const layer = layersRef.current.changes; layer.clearLayers();
    if (!showChanges || !finalEff) return;
    const toLL = (c) => c.map(p => [p[1], p[0]]);
    // LRT lines (from the rakal layer): teal = line 3, purple = line 8
    if (lrtDataRef.current) {
      for (const s of (lrtDataRef.current.segments || [])) {
        L.polyline(toLL(s.coords), { color: LRT_COL[s.line] || '#39d1a8', weight: 4, opacity: .8, dashArray: '1 7', lineCap: 'round' }).addTo(layer)
          .bindTooltip((((lrtDataRef.current.meta || {}).lines || {})[s.line] || {}).name || ('רק"ל קו ' + s.line), { sticky: true });
      }
    }
    const chip = (text, bg) => L.divIcon({ className: '', html: '<span style="background:' + bg + ';color:#12303a;font-weight:800;font-size:10px;padding:1px 6px;border-radius:8px;white-space:nowrap;box-shadow:0 1px 3px #000">' + text + '</span>', iconSize: [0, 0] });
    // two-way highlight on existing roads (subtle underlay)
    for (const f of finalEff.features) {
      if (f.kind === 'twoway') L.polyline(toLL(f.coords), { color: '#ffd166', weight: 12, opacity: .22, lineCap: 'round' }).addTo(layer).bindTooltip('דו-סטרי', { sticky: true });
    }
    // ALL new roads drawn uniformly: road-like fill + colored outline (contour). Outline color marks the type; the fill looks like an existing road.
    const OUTLINE = { ring: '#c026d3', road34: '#c026d3', new_road: '#c026d3', lrt_only: '#0d9488' };
    const drawNewRoad = (segs, style, label) => {
      const outline = OUTLINE[style] || '#c026d3';
      let pts = [];
      for (const seg of segs) {
        const ll = toLL(seg);
        L.polyline(ll, { color: outline, weight: 9, opacity: .95, lineCap: 'round', lineJoin: 'round' }).addTo(layer);          // קו מתאר
        L.polyline(ll, { color: '#fbf6ea', weight: 4.5, opacity: 1, lineCap: 'round', lineJoin: 'round', dashArray: style === 'lrt_only' ? '3 5' : null }).addTo(layer).bindTooltip(label, { sticky: true }); // מיסעה כמו רחוב קיים
        pts = pts.concat(seg);
      }
    };
    for (const f of finalEff.features) {
      if (f.kind === 'marked') drawNewRoad(f.segs, f.style, f.label);
      else if (f.kind === 'new_road') drawNewRoad([f.coords], 'new_road', f.label || 'רחוב חדש');
      else if (f.kind === 'closed') L.polyline(toLL(f.coords), { color: '#e5484d', weight: 6, opacity: .85, dashArray: '4 8' }).addTo(layer).bindTooltip(f.label || 'מקטע סגור', { sticky: true });
    }
  }, [showChanges, finalEff, status]);

  // turn-cancellations layer (numbered markers, like the presentation p19) — toggled independently
  useEffect(() => {
    const map = mapRef.current; if (!map) return;
    const layer = layersRef.current.bans; layer.clearLayers();
    if (!showBans || !finalEff) return;
    for (const f of finalEff.features) {
      if (f.kind !== 'ban') continue;
      const html = '<div style="width:22px;height:22px;border-radius:50%;background:#e5484d;border:2px solid #fff;box-shadow:0 1px 3px #000;color:#fff;font-weight:800;font-size:12px;display:flex;align-items:center;justify-content:center">' + (f.num || '⛔') + '</div>';
      L.marker([f.lat, f.lng], { icon: L.divIcon({ className: '', html, iconSize: [22, 22], iconAnchor: [11, 11] }) }).addTo(layer).bindTooltip(f.label || 'תנועה מבוטלת', { direction: 'top' });
    }
  }, [showBans, finalEff, status]);

  // draw routes + markers
  useEffect(() => {
    const map = mapRef.current; if (!map) return;
    const { routes, markers } = layersRef.current; routes.clearLayers(); markers.clearLayers();
    const toLL = (c) => c.map(p => [p[1], p[0]]);
    const drawRoute = (r, isFinal) => {
      if (!r) return;
      if (r.transit) {
        L.polyline(toLL(r.walkA), { color: '#9fb7c0', weight: 4, opacity: .9, dashArray: '2 8', lineCap: 'round' }).addTo(routes);
        L.polyline(toLL(r.walkB), { color: '#9fb7c0', weight: 4, opacity: .9, dashArray: '2 8', lineCap: 'round' }).addTo(routes);
        L.polyline(toLL(r.ride), { color: COL.final, weight: 7, opacity: .95, lineCap: 'round' }).addTo(routes);
      } else {
        L.polyline(toLL(r.coords), isFinal ? { color: COL.final, weight: 6, opacity: .95, lineCap: 'round' } : { color: COL.today, weight: 9, opacity: .55, dashArray: '2 10', lineCap: 'round' }).addTo(routes);
      }
    };
    if (showToday) drawRoute(routeToday, false);
    if (showFinal) drawRoute(routeFinal, true);
    const pin = (p, label, color) => L.marker([p.lat, p.lng], { icon: L.divIcon({ className: '', html: '<div class="ab-pin" style="color:' + color + '">' + label + '</div>', iconSize: [26, 26], iconAnchor: [13, 24] }) }).addTo(markers);
    if (A) pin(A, '📍', COL.a); if (B) pin(B, '🏁', COL.b);
  }, [routeToday, routeFinal, showToday, showFinal, A, B, status]);

  // precise CAD plan overlay (georeferenced curbs from the DWG) — lazy-loaded, toggleable
  useEffect(() => {
    const map = mapRef.current; if (!map) return;
    const layer = layersRef.current.cad; if (!layer) return;
    layer.clearLayers();
    if (!showCad) return;
    // new roads render like ordinary OSM/basemap streets (white casing) — future state
    const STYLE = {
      curb: { color: '#9aa7b0', weight: .5, opacity: .18 },
      island: { color: '#7fb98a', weight: 1, opacity: .5 },
      bus: { color: '#5b9bd5', weight: 1.6, opacity: .6 },
      bike: { color: '#e0a94a', weight: 1.3, opacity: .55 },
      furniture: { color: '#b6c2cb', weight: .6, opacity: .35 },
      tree: { color: '#5aa06f', weight: 1, opacity: .45 },
    };
    const draw = (fc) => {
      for (const f of fc.features) {
        const kind = (f.properties || {}).kind;
        if (kind === 'centerline') continue; // existing roads already drawn by the basemap
        const g = f.geometry;
        const lines = g.type === 'MultiLineString' ? g.coordinates : [g.coordinates];
        for (const ln of lines) {
          const ll = ln.map(p => [p[1], p[0]]);
          if (kind === 'new_axis') { // draw as a normal white street (casing + fill); dual carriageways read as separate
            L.polyline(ll, { color: '#c8bda3', weight: 7.5, opacity: .95, lineCap: 'round', lineJoin: 'round' }).addTo(layer);
            L.polyline(ll, { color: '#ffffff', weight: 5, opacity: 1, lineCap: 'round', lineJoin: 'round' }).addTo(layer);
          } else L.polyline(ll, STYLE[kind] || { color: '#8794a0', weight: 1, opacity: .5 }).addTo(layer);
        }
      }
    };
    if (cadRef.current) draw(cadRef.current);
    else fetch('data/junction_cad.geojson?v=2026-07-27d').then(r => r.json()).then(fc => { cadRef.current = fc; if (showCad) draw(fc); }).catch(() => { });
  }, [showCad, status]);

  // digitize overlay: the polyline the user is drawing
  useEffect(() => {
    const map = mapRef.current; if (!map) return;
    const dl = layersRef.current.draw; dl.clearLayers();
    if (!drawPts.length) return;
    if (drawPts.length > 1) L.polyline(drawPts.map(p => [p[1], p[0]]), { color: '#ffe14d', weight: 5, opacity: .95, lineCap: 'round' }).addTo(dl);
    drawPts.forEach((p, i) => L.circleMarker([p[1], p[0]], { radius: 4, color: '#ffe14d', fillColor: '#12303a', fillOpacity: 1, weight: 2 }).addTo(dl).bindTooltip(String(i + 1)));
  }, [drawPts, status]);

  // debug overlay: edge/node ids of current-final graph within view
  useEffect(() => {
    const map = mapRef.current; if (!map) return;
    const dbg = layersRef.current.debug; dbg.clearLayers();
    if (!debug || !finalEff) return;
    const draw = () => {
      dbg.clearLayers(); const b = map.getBounds();
      const wg = finalEff.wg; let n = 0;
      for (const id in wg.edges) {
        const e = wg.edges[id]; const mid = e.geometry[Math.floor(e.geometry.length / 2)];
        if (!b.contains([mid[1], mid[0]])) continue; if (n++ > 400) break;
        L.polyline(e.geometry.map(p => [p[1], p[0]]), { color: e.modes.car ? '#4ad' : '#a6a', weight: 1.5, opacity: .5 }).addTo(dbg);
        L.marker([mid[1], mid[0]], { icon: L.divIcon({ className: '', html: '<span class="lbl-badge">' + id + '</span>', iconSize: [0, 0] }) }).addTo(dbg);
      }
      for (const id in wg.nodes) { const nd = wg.nodes[id]; if (!b.contains([nd.lat, nd.lng])) continue; if (Math.random() < .55) continue; L.marker([nd.lat, nd.lng], { icon: L.divIcon({ className: '', html: '<span class="lbl-node">' + id + '</span>', iconSize: [0, 0] }) }).addTo(dbg); }
    };
    draw(); map.on('moveend', draw);
    return () => map.off('moveend', draw);
  }, [debug, finalEff, status]);

  if (status === 'loading') return null;
  if (status === 'error') return <div style={{ padding: 30, color: '#f88' }}>שגיאה בטעינת הנתונים: {errMsg}</div>;

  const changes = changesRef.current;
  const experiments = (changes.changesets || []).filter(c => !c.isFinalPlan);
  const detailRoute = detailState === 'today' ? routeToday : routeFinal;
  const steps = turnSteps(detailRoute);
  const fmt = (m) => m == null ? '—' : (m >= 1000 ? (m / 1000).toFixed(2) + ' ק"מ' : Math.round(m) + ' מ׳');
  const delta = (routeToday && routeFinal) ? routeFinal.length - routeToday.length : null;

  return (
    <div style={{ display: 'flex', height: '100vh', width: '100vw' }}>
      {/* sidebar */}
      <div style={{ width: 360, minWidth: 360, height: '100%', overflowY: 'auto', background: 'linear-gradient(180deg,#12303a,#0d1b22)', borderInlineStart: '1px solid #1d3a45', padding: '16px 16px 40px', display: 'flex', flexDirection: 'column', gap: 14 }}>
        <div>
          <div style={{ fontSize: 20, fontWeight: 800, color: '#8ff0d6' }}>צומת אורנים · מסלול A→B</div>
          <div style={{ fontSize: 12, color: '#7fa3b0', marginTop: 2 }}>השוואת מצב היום מול התכנית הסופית (דו-סטרי + רק"ל)</div>
        </div>

        {/* mode */}
        <div>
          <div style={sectLbl}>אופן תנועה</div>
          <div style={{ display: 'flex', gap: 8 }}>
            <Chip active={mode === 'car'} onClick={() => setMode('car')}>🚗 רכב</Chip>
            <Chip active={mode === 'walk'} onClick={() => setMode('walk')}>🚶 הליכה</Chip>
            <Chip active={mode === 'transit'} onClick={() => setMode('transit')} title={'רק"ל: הקו התכלת + הסגול (סכמטי)'}>🚈 רק"ל</Chip>
          </div>
        </div>

        {/* A/B */}
        <div>
          <div style={sectLbl}>נקודות מוצא ויעד</div>
          <StreetPicker label="מוצא (A)" icon="📍" streets={streets} onPick={p => setA({ lat: p.lat, lng: p.lng })} />
          <div style={{ display: 'flex', justifyContent: 'center', margin: '-2px 0' }}>
            <Chip active={false} onClick={() => { setA(B); setB(A); }} title="הפוך מוצא ויעד">⇅ הפוך A↔B</Chip>
          </div>
          <StreetPicker label="יעד (B)" icon="🏁" streets={streets} onPick={p => setB({ lat: p.lat, lng: p.lng })} />
          <div style={{ display: 'flex', gap: 8, marginTop: 2 }}>
            <Chip active={pick === 'A'} onClick={() => setPick('A')}>📍 סימון A במפה</Chip>
            <Chip active={pick === 'B'} onClick={() => setPick('B')}>🏁 סימון B במפה</Chip>
          </div>
        </div>

        {/* comparison summary */}
        <div style={{ background: 'rgba(255,255,255,.03)', border: '1px solid #1d3a45', borderRadius: 12, padding: 12 }}>
          <div style={sectLbl}>השוואת מסלול ({mode === 'car' ? 'רכב' : mode === 'walk' ? 'הליכה' : 'רק"ל'})</div>
          <Row swatch={COL.today} dash label={mode === 'transit' ? 'היום (הליכה)' : 'היום'} val={fmt(routeToday && routeToday.length)} on={showToday} toggle={() => setShowToday(s => !s)} />
          <Row swatch={COL.final} label="סופי" val={fmt(routeFinal && routeFinal.length)} on={showFinal} toggle={() => setShowFinal(s => !s)} />
          {delta != null && <div style={{ marginTop: 6, fontSize: 12, color: delta > 5 ? '#f4a13a' : (delta < -5 ? '#39d1a8' : '#9bb') }}>
            {delta > 5 ? '▲ המסלול הסופי ארוך ב-' + fmt(delta) : delta < -5 ? '▼ המסלול הסופי קצר ב-' + fmt(-delta) : '≈ אורך דומה'}
          </div>}
          {(!routeToday || !routeFinal) && <div style={{ marginTop: 6, fontSize: 12, color: '#f88' }}>{!routeToday ? 'אין מסלול במצב היום. ' : ''}{!routeFinal ? 'אין מסלול במצב הסופי.' : ''}</div>}
        </div>

        {/* experiments */}
        <div>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div style={sectLbl}>ביטולי פניות (כמו במצגת)</div>
            <label style={{ display: 'flex', gap: 5, alignItems: 'center', fontSize: 11, color: '#9bb', cursor: 'pointer' }}>
              <input type="checkbox" checked={showBans} onChange={() => setShowBans(s => !s)} /> הצג
            </label>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 5, marginBottom: 12 }}>
            {(finalEff ? finalEff.features.filter(f => f.kind === 'ban') : []).map((b, i) => (
              <button key={i} onClick={() => mapRef.current && mapRef.current.setView([b.lat, b.lng], 18, { animate: true })}
                style={{ display: 'flex', gap: 8, alignItems: 'center', textAlign: 'start', padding: '6px 8px', borderRadius: 8, border: '1px solid #4a2530', background: 'rgba(229,72,77,.08)', color: '#f3d6da', fontSize: 11.5, cursor: 'pointer' }}>
                <span style={{ flex: '0 0 20px', width: 20, height: 20, borderRadius: '50%', background: '#e5484d', color: '#fff', fontWeight: 800, fontSize: 11, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>{b.num || '⛔'}</span>
                <span style={{ flex: 1 }}>{(b.label || 'תנועה מבוטלת').replace(/^ביטול #\d+:\s*/, '')}</span>
              </button>
            ))}
          </div>

          <div style={sectLbl}>תרחישים ניסיוניים (מוגדרים בקוד)</div>
          {experiments.map(cs => (
            <label key={cs.id} style={{ display: 'flex', gap: 8, alignItems: 'flex-start', padding: '6px 0', cursor: 'pointer', fontSize: 13, color: '#d6e6ec' }}>
              <input type="checkbox" checked={activeExp.has(cs.id)} onChange={() => setActiveExp(s => { const n = new Set(s); n.has(cs.id) ? n.delete(cs.id) : n.add(cs.id); return n; })} style={{ marginTop: 3 }} />
              <span>{cs.label}</span>
            </label>
          ))}
          <div style={{ marginTop: 10, padding: 8, background: 'rgba(255,255,255,.03)', border: '1px solid #2c4a56', borderRadius: 10 }}>
            <div style={{ fontSize: 11.5, fontWeight: 700, color: '#8ff0d6', marginBottom: 4 }}>🧪 בדיקת שינוי חופשי</div>
            <input dir="rtl" value={freeText} placeholder='למשל: חסום מקור חיים · חד-סטרי e3094' onChange={e => setFreeText(e.target.value)}
              onKeyDown={e => { if (e.key === 'Enter') { const r = parseFreeText(freeText, baseRef.current); setFreeMuts(r.muts); setFreeMsg(r.msg); } }}
              style={{ width: '100%', padding: '6px 8px', borderRadius: 8, background: '#0f2530', color: '#d6e6ec', border: '1px solid #2c4a56', fontSize: 12, fontFamily: 'inherit' }} />
            <div style={{ display: 'flex', gap: 6, marginTop: 5 }}>
              <Chip active={false} onClick={() => { const r = parseFreeText(freeText, baseRef.current); setFreeMuts(r.muts); setFreeMsg(r.msg); }}>החל</Chip>
              <Chip active={false} onClick={() => { setFreeText(''); setFreeMuts([]); setFreeMsg(''); }}>נקה</Chip>
              {freeMuts.length > 0 && <span style={{ alignSelf: 'center', fontSize: 11, color: '#39d1a8' }}>{freeMuts.length} שינויים פעילים</span>}
            </div>
            {freeMsg && <div style={{ fontSize: 10.5, color: '#9bb', marginTop: 4 }}>{freeMsg}</div>}
            <div style={{ fontSize: 10, color: '#5c7482', marginTop: 4 }}>פעלים: חסום · חד-סטרי · דו-סטרי · פתח. יעד: שם רחוב או מזהה קשת (e123 / x_ring). לאימות מזהים — הפעל DebugOverlay.</div>
          </div>
          <label style={{ display: 'flex', gap: 8, alignItems: 'center', marginTop: 8, fontSize: 12.5, color: '#8ff0d6', cursor: 'pointer' }}>
            <input type="checkbox" checked={showCad} onChange={() => setShowCad(s => !s)} /> הצג את הכבישים החדשים (מצב עתידי, מהתשריט)
          </label>
          <label style={{ display: 'flex', gap: 8, alignItems: 'center', marginTop: 8, fontSize: 12.5, color: '#e3c6ee', cursor: 'pointer' }}>
            <input type="checkbox" checked={showChanges} onChange={() => setShowChanges(s => !s)} /> הצג שינויי תשתית (רחוב חדש · ביטולי פניות · דו-סטרי)
          </label>
          <label style={{ display: 'flex', gap: 8, alignItems: 'center', marginTop: 6, fontSize: 12, color: '#9bb', cursor: 'pointer' }}>
            <input type="checkbox" checked={debug} onChange={() => setDebug(d => !d)} /> הצג מזהי קשתות/צמתים (DebugOverlay)
          </label>
        </div>

        {/* digitize tool */}
        <div>
          <div style={sectLbl}>✏️ שרטוט תוואי (טבעת / כביש חדש)</div>
          <div style={{ display: 'flex', gap: 8, marginBottom: 6, flexWrap: 'wrap' }}>
            <Chip active={draw} onClick={() => setDraw(d => !d)}>{draw ? '● משרטט…' : 'שרטט תוואי'}</Chip>
            <Chip active={false} onClick={() => setDrawPts(p => p.slice(0, -1))}>↶ בטל נקודה</Chip>
            <Chip active={false} onClick={() => setDrawPts([])}>נקה</Chip>
          </div>
          <div style={{ fontSize: 11, color: '#7fa3b0', marginBottom: 6 }}>הדליקו "שרטט", לחצו על המפה לאורך התוואי המדויק של הטבעת (יהודה→בן זכאי), והעתיקו את הקואורדינטות אליי או ל-junction_changes.json.</div>
          {drawPts.length > 0 && <textarea readOnly value={'"geometry": ' + JSON.stringify(drawPts)} onFocus={e => e.target.select()}
            style={{ width: '100%', height: 68, background: '#0f2530', color: '#ffe14d', border: '1px solid #2c4a56', borderRadius: 8, fontSize: 10.5, fontFamily: 'monospace', padding: 6, direction: 'ltr' }} />}
        </div>
        <div style={{ display: 'none' }}>
        </div>

        {/* turn list */}
        <div>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div style={sectLbl}>הוראות מסלול</div>
            <div style={{ display: 'flex', gap: 6 }}>
              <Chip active={detailState === 'today'} onClick={() => setDetailState('today')}>היום</Chip>
              <Chip active={detailState === 'final'} onClick={() => setDetailState('final')}>סופי</Chip>
            </div>
          </div>
          {!detailRoute && <div style={{ fontSize: 12, color: '#f88', marginTop: 6 }}>אין מסלול.</div>}
          {detailRoute && detailRoute.transit && <div style={{ display: 'flex', flexDirection: 'column', gap: 4, marginTop: 4 }}>
            <div style={legRow}><span>🚶</span><span style={{ flex: 1 }}>הליכה לתחנת הרק"ל וממנה</span><span style={legDist}>{fmt(detailRoute.walkDist)}</span></div>
            <div style={legRow}><span>🚈</span><span style={{ flex: 1 }}>נסיעה ברק"ל ({detailRoute.lines.map(l => (LRT_NAMES[l] || 'קו ' + l)).join(' + ')})</span><span style={legDist}>{fmt(detailRoute.rideDist)}</span></div>
          </div>}
          {steps.map((s, i) => (
            <div key={i} style={{ display: 'flex', gap: 8, alignItems: 'baseline', padding: '4px 0', borderBottom: '1px solid #17323c' }}>
              <span style={{ color: COL.final, fontSize: 15, width: 16 }}>{ACTION_ICON[s.action]}</span>
              <span style={{ fontSize: 13, color: '#e3eef2', flex: 1 }}>{ACTION_HE[s.action]} <b>{s.name}</b></span>
              <span style={{ fontSize: 11, color: '#7fa3b0' }}>{fmt(s.length)}</span>
            </div>
          ))}
        </div>
        <div style={{ marginTop: 'auto', fontSize: 10.5, color: '#5c7482' }}>מקור גיאומטריה: OpenStreetMap · שכבת שינויים מהמצגת 02.07.2026. תח"צ ומסלולי הליכה מפורטים — סכמטיים.</div>
      </div>
      {/* map */}
      <div style={{ position: 'relative', flex: 1 }}>
        <div id="jmap" style={{ position: 'absolute', inset: 0 }}></div>
        <div style={{ position: 'absolute', top: 10, insetInlineEnd: 10, zIndex: 500, background: 'rgba(13,27,34,.85)', border: '1px solid #1d3a45', borderRadius: 10, padding: '8px 10px', fontSize: 12, color: '#cfe0e6' }}>
          <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}><span style={{ width: 22, height: 0, borderTop: '4px dotted ' + COL.today }}></span> מסלול היום</div>
          <div style={{ display: 'flex', gap: 6, alignItems: 'center', marginTop: 3 }}><span style={{ width: 22, height: 0, borderTop: '5px solid ' + COL.final }}></span> מסלול סופי</div>
          <div style={{ borderTop: '1px solid #1d3a45', margin: '6px 0' }}></div>
          <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}><span style={{ width: 22, height: 0, borderTop: '4px dashed #ff4dd2' }}></span> רחוב חדש (מחבר)</div>
          <div style={{ display: 'flex', gap: 6, alignItems: 'center', marginTop: 3 }}><span style={{ width: 22, height: 8, background: '#ff4dd2', opacity: .35, borderRadius: 2 }}></span> כבישי הצומת החדשים</div>
          <div style={{ display: 'flex', gap: 6, alignItems: 'center', marginTop: 3 }}><span style={{ width: 22, height: 0, borderTop: '5px solid #e8912e' }}></span> כביש 34 / רחוב המסילה</div>
          <div style={{ display: 'flex', gap: 6, alignItems: 'center', marginTop: 3 }}><span style={{ width: 22, height: 6, background: '#2dd4bf', opacity: .5, borderRadius: 2 }}></span> ציר רק"ל בלבד (ללא רכב)</div>
          <div style={{ display: 'flex', gap: 6, alignItems: 'center', marginTop: 3 }}><span>⛔</span> ביטול פנייה/גישה</div>
          <div style={{ display: 'flex', gap: 6, alignItems: 'center', marginTop: 3 }}><span style={{ width: 22, height: 6, background: '#ffd166', opacity: .5, borderRadius: 2 }}></span> מקטע דו-סטרי</div>
        </div>
      </div>
    </div>
  );
}
const sectLbl = { fontSize: 11, fontWeight: 700, color: '#6f97a4', textTransform: 'uppercase', letterSpacing: .3, marginBottom: 6 };
const legRow = { display: 'flex', gap: 8, alignItems: 'center', fontSize: 13, color: '#e3eef2', background: 'rgba(34,195,166,.06)', border: '1px solid #1d4a44', borderRadius: 8, padding: '6px 8px' };
const legDist = { fontSize: 11, color: '#7fa3b0' };
const LRT_NAMES = { '3': 'התכלת', '8': 'הסגול' };
const selStyle = { width: '100%', marginBottom: 6, padding: '7px 8px', borderRadius: 8, background: '#0f2530', color: '#d6e6ec', border: '1px solid #2c4a56', fontSize: 12, fontFamily: 'inherit' };
function StreetPicker({ label, icon, streets, onPick }) {
  const [q, setQ] = useState(''); const [open, setOpen] = useState(false); const [geo, setGeo] = useState([]); const [busy, setBusy] = useState(false); const boxRef = useRef(null);
  const matches = useMemo(() => { const s = q.trim(); const base = !s ? streets : streets.filter(x => x.name.indexOf(s) >= 0); return base.slice(0, 8); }, [q, streets]);
  // address geocoding (street + house number) via Nominatim, biased to the Jerusalem area
  useEffect(() => {
    const s = q.trim(); if (s.length < 3) { setGeo([]); return; }
    setBusy(true);
    const ctrl = new AbortController();
    const t = setTimeout(async () => {
      const to = setTimeout(() => ctrl.abort(), 5000);
      try {
        const url = 'https://nominatim.openstreetmap.org/search?format=jsonv2&limit=5&countrycodes=il&accept-language=he&viewbox=35.185,31.805,35.255,31.735&bounded=1&q=' + encodeURIComponent(s + ', ירושלים');
        const r = await fetch(url, { signal: ctrl.signal }); const j = await r.json();
        setGeo((j || []).map(x => ({ name: x.display_name.split(',').slice(0, 3).join(',').trim(), lat: +x.lat, lng: +x.lon, addr: true })));
      } catch (e) { setGeo([]); }
      clearTimeout(to); setBusy(false);
    }, 550);
    return () => { clearTimeout(t); ctrl.abort(); setBusy(false); };
  }, [q]);
  useEffect(() => { const h = (ev) => { if (boxRef.current && !boxRef.current.contains(ev.target)) setOpen(false); }; document.addEventListener('mousedown', h); return () => document.removeEventListener('mousedown', h); }, []);
  const list = matches.concat(geo);
  return <div ref={boxRef} style={{ position: 'relative', marginBottom: 6 }}>
    <input dir="rtl" value={q} placeholder={icon + ' ' + label + ' — רחוב או כתובת (רחוב + מס׳)…'} onFocus={() => setOpen(true)} onChange={e => { setQ(e.target.value); setOpen(true); }} style={selStyle} />
    {open && (list.length > 0 || busy) && <div style={{ position: 'absolute', zIndex: 1000, top: '100%', insetInlineStart: 0, insetInlineEnd: 0, background: '#0f2530', border: '1px solid #2c4a56', borderRadius: 8, maxHeight: 260, overflowY: 'auto', boxShadow: '0 8px 24px rgba(0,0,0,.5)' }}>
      {list.map((m, i) => <div key={i} onMouseDown={() => { onPick(m); setQ(m.addr ? m.name.split(',')[0] : m.name); setOpen(false); }} style={{ padding: '7px 9px', fontSize: 12.5, color: '#d6e6ec', cursor: 'pointer', borderBottom: '1px solid #17323c', display: 'flex', justifyContent: 'space-between', gap: 8 }}>
        <span>{m.addr ? '🏠 ' : m.landmark ? '📌 ' : ''}{m.name}</span>{m.d ? <span style={{ color: '#6f97a4', fontSize: 10 }}>{Math.round(m.d)} מ׳</span> : null}</div>)}
      {busy && geo.length === 0 && <div style={{ padding: '7px 9px', fontSize: 11, color: '#6f97a4' }}>מחפש כתובת…</div>}
    </div>}
  </div>;
}
function Row({ swatch, dash, label, val, on, toggle }) {
  return <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '3px 0' }}>
    <input type="checkbox" checked={on} onChange={toggle} />
    <span style={{ width: 22, height: 0, borderTop: (dash ? '4px dotted ' : '5px solid ') + swatch }}></span>
    <span style={{ fontSize: 13, color: '#d6e6ec', flex: 1 }}>{label}</span>
    <span style={{ fontSize: 13, fontWeight: 700, color: '#8ff0d6' }}>{val}</span>
  </div>;
}

// lightweight test hook (used for verification; harmless in production)
window.__J = { route, buildEffective, toWorking, turnSteps, nearestNode };

ReactDOM.createRoot(document.getElementById('root')).render(<JunctionApp />);
