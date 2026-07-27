/* junction.jsx — "צומת אורנים: איך מגיעים מ-A ל-B?"
 * Standalone mini-app. Turn-aware routing over an OSM-derived directed graph,
 * with a declarative, toggleable change layer (Today vs. Final plan + experiments).
 * Compiled to junction.js by Babel (preset-react). No bundler; React/Leaflet from CDN.
 */
'use strict';

var _slicedToArray = (function () { function sliceIterator(arr, i) { var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i['return']) _i['return'](); } finally { if (_d) throw _e; } } return _arr; } return function (arr, i) { if (Array.isArray(arr)) { return arr; } else if (Symbol.iterator in Object(arr)) { return sliceIterator(arr, i); } else { throw new TypeError('Invalid attempt to destructure non-iterable instance'); } }; })();

function _toConsumableArray(arr) { if (Array.isArray(arr)) { for (var i = 0, arr2 = Array(arr.length); i < arr.length; i++) arr2[i] = arr[i]; return arr2; } else { return Array.from(arr); } }

var _React = React;
var useState = _React.useState;
var useEffect = _React.useEffect;
var useRef = _React.useRef;
var useMemo = _React.useMemo;
var useCallback = _React.useCallback;

var GRAPH_URL = 'data/junction_graph.json?v=2026-07-27b';
var CHANGES_URL = 'data/junction_changes.json?v=2026-07-27c';
var LRT_URL = 'data/junction_lrt.json?v=2026-07-26';
var LRT_COL = { '3': '#22c3a6', '8': '#b06cff' };
var CENTER = [31.7573, 35.2150];
var ZOOM = 16;

var COL = {
  today: '#f4a13a', // amber
  final: '#22c3a6', // teal
  a: '#2ecc71', b: '#e5484d',
  banned: '#e5484d'
};

var LANDMARKS = [{ name: 'פייר קניג דרום (ליד הרחוב החדש)', lat: 31.756462, lng: 35.214686 }, { name: 'צומת רבקה (פייר קניג × רבקה)', lat: 31.755145, lng: 35.214455 }, { name: 'צומת מקור חיים (פייר קניג × יהודה)', lat: 31.757987, lng: 35.215016 }, { name: 'צומת בן זכאי / עמק רפאים (צפון)', lat: 31.759527, lng: 35.215198 }, { name: 'תחנת רק"ל אורנים (משוער)', lat: 31.75700, lng: 35.21510 }, { name: 'בי"ס מקור חיים (משוער)', lat: 31.756749, lng: 35.213441 }, { name: 'יהודה × נפתלי', lat: 31.757821, lng: 35.216860 }];

/* ------------------------------------------------------------------ geo */
function haversine(a, b) {
  // [lng,lat]
  var R = 6371000,
      tr = Math.PI / 180;
  var dLat = (b[1] - a[1]) * tr,
      dLon = (b[0] - a[0]) * tr;
  var la1 = a[1] * tr,
      la2 = b[1] * tr;
  var h = Math.pow(Math.sin(dLat / 2), 2) + Math.cos(la1) * Math.cos(la2) * Math.pow(Math.sin(dLon / 2), 2);
  return 2 * R * Math.asin(Math.sqrt(h));
}
function polyLen(coords) {
  var s = 0;for (var i = 0; i < coords.length - 1; i++) {
    s += haversine(coords[i], coords[i + 1]);
  }return s;
}
function bearing(a, b) {
  // [lng,lat] -> deg 0..360
  var tr = Math.PI / 180;
  var y = Math.sin((b[0] - a[0]) * tr) * Math.cos(b[1] * tr);
  var x = Math.cos(a[1] * tr) * Math.sin(b[1] * tr) - Math.sin(a[1] * tr) * Math.cos(b[1] * tr) * Math.cos((b[0] - a[0]) * tr);
  return (Math.atan2(y, x) / tr + 360) % 360;
}
function sideOf(via, nb) {
  // dominant compass of nb relative to via, both {lng,lat}
  var dx = (nb.lng - via.lng) * Math.cos(via.lat * Math.PI / 180),
      dy = nb.lat - via.lat;
  if (Math.abs(dy) >= Math.abs(dx)) return dy >= 0 ? 'N' : 'S';
  return dx >= 0 ? 'E' : 'W';
}

/* ------------------------------------------------- graph working-form + mutations */
function toWorking(base) {
  var nodes = {};
  for (var k in base.nodes) {
    nodes[k] = Object.assign({}, base.nodes[k]);
  }var edges = {};
  var _iteratorNormalCompletion = true;
  var _didIteratorError = false;
  var _iteratorError = undefined;

  try {
    for (var _iterator = base.edges[Symbol.iterator](), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
      var e = _step.value;
      edges[e.id] = Object.assign({}, e, { modes: Object.assign({}, e.modes) });
    }
  } catch (err) {
    _didIteratorError = true;
    _iteratorError = err;
  } finally {
    try {
      if (!_iteratorNormalCompletion && _iterator['return']) {
        _iterator['return']();
      }
    } finally {
      if (_didIteratorError) {
        throw _iteratorError;
      }
    }
  }

  var turns = (base.turnRestrictions || []).map(function (t) {
    return Object.assign({}, t);
  });
  return { nodes: nodes, edges: edges, turns: turns };
}
function cloneWG(wg) {
  var nodes = {};for (var k in wg.nodes) {
    nodes[k] = Object.assign({}, wg.nodes[k]);
  }var edges = {};for (var k in wg.edges) {
    var e = wg.edges[k];edges[k] = Object.assign({}, e, { modes: Object.assign({}, e.modes), geometry: e.geometry });
  }
  var turns = wg.turns.map(function (t) {
    return Object.assign({}, t);
  });
  return { nodes: nodes, edges: edges, turns: turns };
}
function resolveTurn(wg, spec) {
  var via = wg.nodes[spec.viaNode];if (!via) return null;
  var bestIn = null,
      bestOut = null,
      si = -2,
      so = -2;
  var want = function want(side, node) {
    return sideOf(via, node) === side ? 1 : 0;
  };
  for (var id in wg.edges) {
    var e = wg.edges[id];
    if (e.to === spec.viaNode && spec.fromName && (e.name || '').indexOf(spec.fromName) >= 0) {
      var sc = want(spec.fromSide, wg.nodes[e.from]);if (sc > si) {
        si = sc;bestIn = e;
      }
    }
    if (e.from === spec.viaNode && spec.toName && (e.name || '').indexOf(spec.toName) >= 0) {
      var sc = want(spec.toSide, wg.nodes[e.to]);if (sc > so) {
        so = sc;bestOut = e;
      }
    }
  }
  if (!bestIn || !bestOut) return null;
  return { fromEdge: bestIn.id, toEdge: bestOut.id, viaNode: spec.viaNode };
}
function applyMutation(wg, m, warn) {
  switch (m.op) {
    case 'addNode':
      wg.nodes[m.id] = { id: m.id, lng: m.lng, lat: m.lat, landmark: m.landmark, synthetic: true };break;
    case 'addEdge':
      {
        var fn = wg.nodes[m.from],
            tn = wg.nodes[m.to];
        if (!fn || !tn) {
          warn('addEdge missing node ' + m.from + '/' + m.to);break;
        }
        var geom = m.geometry || [[fn.lng, fn.lat], [tn.lng, tn.lat]];
        var len = m.length != null ? m.length : polyLen(geom);
        wg.edges[m.id] = { id: m.id, from: m.from, to: m.to, name: m.name || '', length: Math.round(len * 100) / 100, modes: Object.assign({ car: true, walk: true }, m.modes), oneway: !m.bidirectional, pairId: null, geometry: geom, synthetic: true, note: m.note };
        if (m.bidirectional) {
          var rid = m.id + '_r';
          wg.edges[rid] = { id: rid, from: m.to, to: m.from, name: m.name || '', length: wg.edges[m.id].length, modes: Object.assign({}, wg.edges[m.id].modes), oneway: false, pairId: m.id, geometry: geom.slice().reverse(), synthetic: true, note: m.note };
          wg.edges[m.id].pairId = rid;wg.edges[m.id].oneway = false;
        }
        break;
      }
    case 'removeEdge':
      {
        var ed = wg.edges[m.edgeId];if (!ed) {
          warn('removeEdge missing ' + m.edgeId);break;
        }
        if (m.pair !== false && ed.pairId && wg.edges[ed.pairId]) delete wg.edges[ed.pairId];
        delete wg.edges[m.edgeId];
        wg.turns = wg.turns.filter(function (t) {
          return t.fromEdge !== m.edgeId && t.toEdge !== m.edgeId;
        });
        break;
      }
    case 'modifyEdge':
      {
        var ed = wg.edges[m.edgeId];if (ed) Object.assign(ed, m.set || {});break;
      }
    case 'setOneway':
      {
        var ed = wg.edges[m.edgeId];if (!ed) {
          warn('setOneway missing ' + m.edgeId);break;
        }
        if (m.oneway === false) {
          if (ed.pairId && wg.edges[ed.pairId]) {
            ed.oneway = false;break;
          }
          var rid = m.edgeId + '_2w';
          wg.edges[rid] = { id: rid, from: ed.to, to: ed.from, name: ed.name, length: ed.length, modes: Object.assign({}, ed.modes), oneway: false, pairId: ed.id, geometry: ed.geometry.slice().reverse(), synthetic: true, note: ed.note };
          ed.pairId = rid;ed.oneway = false;
        } else {
          if (ed.pairId && wg.edges[ed.pairId]) delete wg.edges[ed.pairId];
          ed.pairId = null;ed.oneway = true;
        }
        break;
      }
    case 'addTurnRestriction':
      {
        var fromEdge = m.fromEdge,
            toEdge = m.toEdge,
            viaNode = m.viaNode;
        if (!fromEdge || !toEdge) {
          var r = resolveTurn(wg, m);
          if (!r) {
            warn('turn "' + (m.id || '') + '" unresolved');break;
          }
          fromEdge = r.fromEdge;toEdge = r.toEdge;viaNode = r.viaNode;
        }
        wg.turns.push({ id: m.id, fromEdge: fromEdge, viaNode: viaNode, toEdge: toEdge, type: m.type || 'no_turn', note: m.note, added: true });
        break;
      }
    case 'removeTurnRestriction':
      wg.turns = wg.turns.filter(function (t) {
        return t.id !== m.id;
      });break;
    default:
      warn('unknown op ' + m.op);
  }
}
function buildEffective(baseWG, changes, activeIds, includeFinal) {
  var wg = cloneWG(baseWG);
  var notes = [],
      features = [];
  var warn = function warn(msg) {
    return console.warn('[change]', msg);
  };
  var _iteratorNormalCompletion2 = true;
  var _didIteratorError2 = false;
  var _iteratorError2 = undefined;

  try {
    for (var _iterator2 = (changes.changesets || [])[Symbol.iterator](), _step2; !(_iteratorNormalCompletion2 = (_step2 = _iterator2.next()).done); _iteratorNormalCompletion2 = true) {
      var cs = _step2.value;

      var on = cs.isFinalPlan ? includeFinal : activeIds.has(cs.id);
      if (!on) continue;
      var _iteratorNormalCompletion3 = true;
      var _didIteratorError3 = false;
      var _iteratorError3 = undefined;

      try {
        for (var _iterator3 = cs.mutations[Symbol.iterator](), _step3; !(_iteratorNormalCompletion3 = (_step3 = _iterator3.next()).done); _iteratorNormalCompletion3 = true) {
          var m = _step3.value;

          if (m.op === 'markRoad') {
            // visual-only highlight (existing edges or explicit geometry)
            var segs = undefined;
            if (m.geometry) segs = Array.isArray(m.geometry[0][0]) ? m.geometry : [m.geometry];else segs = (m.edgeIds || []).map(function (id) {
              return wg.edges[id];
            }).filter(Boolean).map(function (e) {
              return e.geometry;
            });
            if (segs.length) features.push({ kind: 'marked', style: m.style || 'new_road', label: m.label, segs: segs });
            if (m.note) notes.push({ cs: cs.id, note: m.note });
            continue;
          }
          var preGeom = null;
          if (m.op === 'removeEdge' && wg.edges[m.edgeId]) preGeom = wg.edges[m.edgeId].geometry;
          applyMutation(wg, m, warn);
          if (m.note) notes.push({ cs: cs.id, note: m.note });
          if (m.op === 'addEdge' && wg.edges[m.id]) {
            if (m.noMark) {/* routing-only segment */} else if (m.style) features.push({ kind: 'marked', style: m.style, label: m.name || m.note, segs: [wg.edges[m.id].geometry] });else features.push({ kind: 'new_road', coords: wg.edges[m.id].geometry, label: m.name || m.note });
          } else if (m.op === 'addTurnRestriction') {
            var t = wg.turns[wg.turns.length - 1];var nd = t && wg.nodes[t.viaNode];if (nd) {
              var mm = (m.note || '').match(/#(\d+)/);features.push({ kind: 'ban', lng: nd.lng, lat: nd.lat, label: m.note, num: mm ? mm[1] : '' });
            }
          } else if (m.op === 'setOneway' && m.oneway === false && wg.edges[m.edgeId]) features.push({ kind: 'twoway', coords: wg.edges[m.edgeId].geometry, label: m.note });else if (m.op === 'removeEdge' && preGeom) features.push({ kind: 'closed', coords: preGeom, label: m.note });
        }
      } catch (err) {
        _didIteratorError3 = true;
        _iteratorError3 = err;
      } finally {
        try {
          if (!_iteratorNormalCompletion3 && _iterator3['return']) {
            _iterator3['return']();
          }
        } finally {
          if (_didIteratorError3) {
            throw _iteratorError3;
          }
        }
      }
    }
  } catch (err) {
    _didIteratorError2 = true;
    _iteratorError2 = err;
  } finally {
    try {
      if (!_iteratorNormalCompletion2 && _iterator2['return']) {
        _iterator2['return']();
      }
    } finally {
      if (_didIteratorError2) {
        throw _iteratorError2;
      }
    }
  }

  return { wg: wg, notes: notes, features: features };
}

/* ---------------------------------------------------------------- routing */
function MinHeap() {
  this.a = [];
}
MinHeap.prototype.push = function (item) {
  var a = this.a;a.push(item);var i = a.length - 1;while (i > 0) {
    var p = i - 1 >> 1;if (a[p].c <= a[i].c) break;var _ref = [a[i], a[p]];
    a[p] = _ref[0];
    a[i] = _ref[1];
    i = p;
  }
};
MinHeap.prototype.pop = function () {
  var a = this.a;var top = a[0],
      last = a.pop();if (a.length) {
    a[0] = last;var i = 0;for (;;) {
      var l = 2 * i + 1,
          r = l + 1,
          s = i;if (l < a.length && a[l].c < a[s].c) s = l;if (r < a.length && a[r].c < a[s].c) s = r;if (s === i) break;var _ref2 = [a[i], a[s]];
      a[s] = _ref2[0];
      a[i] = _ref2[1];
      i = s;
    }
  }return top;
};
MinHeap.prototype.size = function () {
  return this.a.length;
};

function carIndex(wg) {
  var out = {}; // nodeId -> [edge]
  for (var id in wg.edges) {
    var e = wg.edges[id];if (!e.modes.car) continue;(out[e.from] || (out[e.from] = [])).push(e);
  }
  var banned = new Set(),
      only = {};
  var _iteratorNormalCompletion4 = true;
  var _didIteratorError4 = false;
  var _iteratorError4 = undefined;

  try {
    for (var _iterator4 = wg.turns[Symbol.iterator](), _step4; !(_iteratorNormalCompletion4 = (_step4 = _iterator4.next()).done); _iteratorNormalCompletion4 = true) {
      var t = _step4.value;

      if (/^only_/.test(t.type)) {
        var k = t.fromEdge + '|' + t.viaNode;(only[k] || (only[k] = new Set())).add(t.toEdge);
      } else banned.add(t.fromEdge + '|' + t.viaNode + '|' + t.toEdge);
    }
  } catch (err) {
    _didIteratorError4 = true;
    _iteratorError4 = err;
  } finally {
    try {
      if (!_iteratorNormalCompletion4 && _iterator4['return']) {
        _iterator4['return']();
      }
    } finally {
      if (_didIteratorError4) {
        throw _iteratorError4;
      }
    }
  }

  return { out: out, banned: banned, only: only };
}
function routeCar(wg, aNode, bNode) {
  var _carIndex = carIndex(wg);

  var out = _carIndex.out;
  var banned = _carIndex.banned;
  var only = _carIndex.only;

  var dist = {},
      prev = {};
  var heap = new MinHeap();
  var _iteratorNormalCompletion5 = true;
  var _didIteratorError5 = false;
  var _iteratorError5 = undefined;

  try {
    for (var _iterator5 = (out[aNode] || [])[Symbol.iterator](), _step5; !(_iteratorNormalCompletion5 = (_step5 = _iterator5.next()).done); _iteratorNormalCompletion5 = true) {
      var e = _step5.value;
      if (dist[e.id] == null || e.length < dist[e.id]) {
        dist[e.id] = e.length;prev[e.id] = null;heap.push({ id: e.id, c: e.length });
      }
    }
  } catch (err) {
    _didIteratorError5 = true;
    _iteratorError5 = err;
  } finally {
    try {
      if (!_iteratorNormalCompletion5 && _iterator5['return']) {
        _iterator5['return']();
      }
    } finally {
      if (_didIteratorError5) {
        throw _iteratorError5;
      }
    }
  }

  var goalEdge = null;
  while (heap.size()) {
    var _heap$pop = heap.pop();

    var id = _heap$pop.id;
    var c = _heap$pop.c;

    if (c > dist[id]) continue;
    var e = wg.edges[id];
    if (e.to === bNode) {
      goalEdge = id;break;
    }
    var via = e.to;
    var _iteratorNormalCompletion6 = true;
    var _didIteratorError6 = false;
    var _iteratorError6 = undefined;

    try {
      for (var _iterator6 = (out[via] || [])[Symbol.iterator](), _step6; !(_iteratorNormalCompletion6 = (_step6 = _iterator6.next()).done); _iteratorNormalCompletion6 = true) {
        var e2 = _step6.value;

        if (e2.id === e.pairId) continue; // no U-turn
        if (banned.has(e.id + '|' + via + '|' + e2.id)) continue;
        var ok = only[e.id + '|' + via];if (ok && !ok.has(e2.id)) continue;
        var nc = c + e2.length;
        if (dist[e2.id] == null || nc < dist[e2.id]) {
          dist[e2.id] = nc;prev[e2.id] = e.id;heap.push({ id: e2.id, c: nc });
        }
      }
    } catch (err) {
      _didIteratorError6 = true;
      _iteratorError6 = err;
    } finally {
      try {
        if (!_iteratorNormalCompletion6 && _iterator6['return']) {
          _iterator6['return']();
        }
      } finally {
        if (_didIteratorError6) {
          throw _iteratorError6;
        }
      }
    }
  }
  if (goalEdge == null) return null;
  var seq = [];var cur = goalEdge;while (cur != null) {
    seq.unshift(wg.edges[cur]);cur = prev[cur];
  }
  return orientedPath(seq.map(function (e) {
    return { e: e, rev: false };
  }), dist[goalEdge]);
}
function walkIndex(wg) {
  var adj = {}; // nodeId -> [{to, len, edge, rev}]
  for (var id in wg.edges) {
    var e = wg.edges[id];if (!e.modes.walk) continue;
    (adj[e.from] || (adj[e.from] = [])).push({ to: e.to, len: e.length, edge: e, rev: false });
    (adj[e.to] || (adj[e.to] = [])).push({ to: e.from, len: e.length, edge: e, rev: true });
  }
  return adj;
}
function routeWalk(wg, aNode, bNode) {
  var adj = walkIndex(wg);
  var dist = {},
      prev = {};
  var heap = new MinHeap();dist[aNode] = 0;heap.push({ id: aNode, c: 0 });
  while (heap.size()) {
    var _heap$pop2 = heap.pop();

    var id = _heap$pop2.id;
    var c = _heap$pop2.c;
    if (c > dist[id]) continue;if (id === bNode) break;
    var _iteratorNormalCompletion7 = true;
    var _didIteratorError7 = false;
    var _iteratorError7 = undefined;

    try {
      for (var _iterator7 = (adj[id] || [])[Symbol.iterator](), _step7; !(_iteratorNormalCompletion7 = (_step7 = _iterator7.next()).done); _iteratorNormalCompletion7 = true) {
        var nb = _step7.value;
        var nc = c + nb.len;if (dist[nb.to] == null || nc < dist[nb.to]) {
          dist[nb.to] = nc;prev[nb.to] = { from: id, edge: nb.edge, rev: nb.rev };heap.push({ id: nb.to, c: nc });
        }
      }
    } catch (err) {
      _didIteratorError7 = true;
      _iteratorError7 = err;
    } finally {
      try {
        if (!_iteratorNormalCompletion7 && _iterator7['return']) {
          _iterator7['return']();
        }
      } finally {
        if (_didIteratorError7) {
          throw _iteratorError7;
        }
      }
    }
  }
  if (dist[bNode] == null) return null;
  var steps = [];var cur = bNode;while (prev[cur]) {
    steps.unshift({ e: prev[cur].edge, rev: prev[cur].rev });cur = prev[cur].from;
  }
  return orientedPath(steps, dist[bNode]);
}
function orientedPath(steps, length) {
  var path = steps.map(function (s) {
    return { id: s.e.id, name: s.e.name, length: s.e.length, note: s.e.note, coords: s.rev ? s.e.geometry.slice().reverse() : s.e.geometry };
  });
  var coords = [];
  var _iteratorNormalCompletion8 = true;
  var _didIteratorError8 = false;
  var _iteratorError8 = undefined;

  try {
    for (var _iterator8 = path[Symbol.iterator](), _step8; !(_iteratorNormalCompletion8 = (_step8 = _iterator8.next()).done); _iteratorNormalCompletion8 = true) {
      var p = _step8.value;
      var _iteratorNormalCompletion9 = true;
      var _didIteratorError9 = false;
      var _iteratorError9 = undefined;

      try {
        for (var _iterator9 = p.coords[Symbol.iterator](), _step9; !(_iteratorNormalCompletion9 = (_step9 = _iterator9.next()).done); _iteratorNormalCompletion9 = true) {
          var c = _step9.value;
          if (!coords.length || coords[coords.length - 1][0] !== c[0] || coords[coords.length - 1][1] !== c[1]) coords.push(c);
        }
      } catch (err) {
        _didIteratorError9 = true;
        _iteratorError9 = err;
      } finally {
        try {
          if (!_iteratorNormalCompletion9 && _iterator9['return']) {
            _iterator9['return']();
          }
        } finally {
          if (_didIteratorError9) {
            throw _iteratorError9;
          }
        }
      }
    }
  } catch (err) {
    _didIteratorError8 = true;
    _iteratorError8 = err;
  } finally {
    try {
      if (!_iteratorNormalCompletion8 && _iterator8['return']) {
        _iterator8['return']();
      }
    } finally {
      if (_didIteratorError8) {
        throw _iteratorError8;
      }
    }
  }

  return { path: path, coords: coords, length: Math.round(length) };
}
function nearestNode(wg, latlng, mode) {
  // routable nodes = those touched by a usable edge
  var best = null,
      bd = Infinity;
  var touched = {};
  for (var id in wg.edges) {
    var e = wg.edges[id];if (mode === 'car' ? e.modes.car : e.modes.walk) {
      touched[e.from] = 1;touched[e.to] = 1;
    }
  }
  for (var id in touched) {
    var n = wg.nodes[id];if (!n) continue;var d = haversine([latlng.lng, latlng.lat], [n.lng, n.lat]);if (d < bd) {
      bd = d;best = id;
    }
  }
  return best;
}
function route(wg, mode, A, B) {
  if (!A || !B) return null;
  var an = nearestNode(wg, A, mode),
      bn = nearestNode(wg, B, mode);
  if (!an || !bn || an === bn) return null;
  return mode === 'car' ? routeCar(wg, an, bn) : routeWalk(wg, an, bn);
}

/* --- transit: walk to nearest LRT boarding point, ride the network, walk to B --- */
function buildLrtGraph(lrtSegs) {
  var key = function key(p) {
    return p[0].toFixed(6) + ',' + p[1].toFixed(6);
  };
  var adj = {},
      coord = {},
      lineAt = {};
  var _iteratorNormalCompletion10 = true;
  var _didIteratorError10 = false;
  var _iteratorError10 = undefined;

  try {
    for (var _iterator10 = lrtSegs[Symbol.iterator](), _step10; !(_iteratorNormalCompletion10 = (_step10 = _iterator10.next()).done); _iteratorNormalCompletion10 = true) {
      var s = _step10.value;

      for (var i = 0; i < s.coords.length; i++) {
        var k = key(s.coords[i]);coord[k] = s.coords[i];lineAt[k] = s.line;
        if (i > 0) {
          var k0 = key(s.coords[i - 1]);var d = haversine(s.coords[i - 1], s.coords[i]);
          (adj[k0] || (adj[k0] = [])).push({ to: k, d: d });(adj[k] || (adj[k] = [])).push({ to: k0, d: d });
        }
      }
    }
  } catch (err) {
    _didIteratorError10 = true;
    _iteratorError10 = err;
  } finally {
    try {
      if (!_iteratorNormalCompletion10 && _iterator10['return']) {
        _iterator10['return']();
      }
    } finally {
      if (_didIteratorError10) {
        throw _iteratorError10;
      }
    }
  }

  return { adj: adj, coord: coord, lineAt: lineAt, keys: Object.keys(coord) };
}
function nearestLrt(lrt, ll) {
  var best = null,
      bd = Infinity;
  var _iteratorNormalCompletion11 = true;
  var _didIteratorError11 = false;
  var _iteratorError11 = undefined;

  try {
    for (var _iterator11 = lrt.keys[Symbol.iterator](), _step11; !(_iteratorNormalCompletion11 = (_step11 = _iterator11.next()).done); _iteratorNormalCompletion11 = true) {
      var k = _step11.value;
      var d = haversine([ll.lng, ll.lat], lrt.coord[k]);if (d < bd) {
        bd = d;best = k;
      }
    }
  } catch (err) {
    _didIteratorError11 = true;
    _iteratorError11 = err;
  } finally {
    try {
      if (!_iteratorNormalCompletion11 && _iterator11['return']) {
        _iterator11['return']();
      }
    } finally {
      if (_didIteratorError11) {
        throw _iteratorError11;
      }
    }
  }

  return { k: best, d: bd };
}
function routeTransit(lrt, A, B) {
  if (!lrt || !lrt.keys.length) return null;
  var board = nearestLrt(lrt, A),
      alight = nearestLrt(lrt, B);
  if (!board.k || !alight.k) return null;
  // dijkstra on LRT graph
  var dist = {},
      prev = {};var heap = new MinHeap();dist[board.k] = 0;heap.push({ id: board.k, c: 0 });
  while (heap.size()) {
    var _heap$pop3 = heap.pop();

    var id = _heap$pop3.id;
    var c = _heap$pop3.c;
    if (c > dist[id]) continue;if (id === alight.k) break;var _iteratorNormalCompletion12 = true;
    var _didIteratorError12 = false;
    var _iteratorError12 = undefined;

    try {
      for (var _iterator12 = (lrt.adj[id] || [])[Symbol.iterator](), _step12; !(_iteratorNormalCompletion12 = (_step12 = _iterator12.next()).done); _iteratorNormalCompletion12 = true) {
        var nb = _step12.value;
        var nc = c + nb.d;if (dist[nb.to] == null || nc < dist[nb.to]) {
          dist[nb.to] = nc;prev[nb.to] = id;heap.push({ id: nb.to, c: nc });
        }
      }
    } catch (err) {
      _didIteratorError12 = true;
      _iteratorError12 = err;
    } finally {
      try {
        if (!_iteratorNormalCompletion12 && _iterator12['return']) {
          _iterator12['return']();
        }
      } finally {
        if (_didIteratorError12) {
          throw _iteratorError12;
        }
      }
    }
  }
  if (dist[alight.k] == null) return null;
  var rideKeys = [];var cur = alight.k;while (cur != null) {
    rideKeys.unshift(cur);cur = prev[cur];
  }
  var ride = rideKeys.map(function (k) {
    return lrt.coord[k];
  });
  var walkA = [[A.lng, A.lat], lrt.coord[board.k]];
  var walkB = [lrt.coord[alight.k], [B.lng, B.lat]];
  var lines = Array.from(new Set(rideKeys.map(function (k) {
    return lrt.lineAt[k];
  })));
  var rideLen = haversine ? ride.reduce(function (s, p, i) {
    return i ? s + haversine(ride[i - 1], p) : 0;
  }, 0) : 0;
  return { transit: true, walkA: walkA, ride: ride, walkB: walkB, lines: lines, coords: walkA.concat(ride, walkB.slice(1)),
    length: Math.round(board.d + rideLen + alight.d), walkDist: Math.round(board.d + alight.d), rideDist: Math.round(rideLen) };
}
function turnSteps(path) {
  if (!path || !path.path || !path.path.length) return [];
  // merge consecutive same-name edges into legs
  var legs = [];
  var _iteratorNormalCompletion13 = true;
  var _didIteratorError13 = false;
  var _iteratorError13 = undefined;

  try {
    for (var _iterator13 = path.path[Symbol.iterator](), _step13; !(_iteratorNormalCompletion13 = (_step13 = _iterator13.next()).done); _iteratorNormalCompletion13 = true) {
      var p = _step13.value;

      var last = legs[legs.length - 1];
      if (last && last.name === p.name) {
        last.length += p.length;last.coords = last.coords.concat(p.coords.slice(1));
      } else legs.push({ name: p.name, length: p.length, coords: p.coords.slice() });
    }
  } catch (err) {
    _didIteratorError13 = true;
    _iteratorError13 = err;
  } finally {
    try {
      if (!_iteratorNormalCompletion13 && _iterator13['return']) {
        _iterator13['return']();
      }
    } finally {
      if (_didIteratorError13) {
        throw _iteratorError13;
      }
    }
  }

  var steps = [];
  for (var i = 0; i < legs.length; i++) {
    var action = undefined;
    if (i === 0) action = 'start';else {
      var prev = legs[i - 1].coords,
          cur = legs[i].coords;
      var inB = bearing(prev[Math.max(0, prev.length - 2)], prev[prev.length - 1]);
      var outB = bearing(cur[0], cur[1] || cur[0]);
      var d = (outB - inB + 540) % 360 - 180;
      if (Math.abs(d) < 25) action = 'straight';else if (Math.abs(d) > 150) action = 'uturn';else action = d > 0 ? 'right' : 'left';
    }
    steps.push({ name: legs[i].name || 'דרך ללא שם', length: Math.round(legs[i].length), action: action });
  }
  return steps;
}
var ACTION_HE = { start: 'צא אל', straight: 'המשך ישר אל', left: 'פנה שמאלה אל', right: 'פנה ימינה אל', uturn: 'פניית פרסה אל' };
var ACTION_ICON = { start: '●', straight: '↑', left: '↰', right: '↱', uturn: '↺' };

/* free-text scenario parser: "חסום <רחוב|edgeId>" / "חד-סטרי X" / "דו-סטרי X" */
function parseFreeText(str, baseWG) {
  var cmds = (str || '').split(/[\n;]+/).map(function (s) {
    return s.trim();
  }).filter(Boolean);
  var muts = [],
      msgs = [];
  var _iteratorNormalCompletion14 = true;
  var _didIteratorError14 = false;
  var _iteratorError14 = undefined;

  try {
    for (var _iterator14 = cmds[Symbol.iterator](), _step14; !(_iteratorNormalCompletion14 = (_step14 = _iterator14.next()).done); _iteratorNormalCompletion14 = true) {
      var cmd = _step14.value;

      var m = cmd.match(/^(חסום|סגור|block|חד[- ]?סטרי|oneway|דו[- ]?סטרי|twoway|פתח|open)\s+(.+)$/i);
      if (!m) {
        msgs.push('✗ לא הובן: "' + cmd + '"');continue;
      }
      var verb = m[1],
          target = m[2].trim();
      var ids = [];
      if (/^e\d+/.test(target) || /^x_/.test(target)) {
        if (baseWG.edges[target]) ids = [target];
      } else {
        for (var id in baseWG.edges) {
          var nm = baseWG.edges[id].name || '';if (nm && nm.indexOf(target) >= 0) ids.push(id);
        }
      }
      if (!ids.length) {
        msgs.push('✗ לא נמצא: "' + target + '"');continue;
      }
      var _iteratorNormalCompletion15 = true;
      var _didIteratorError15 = false;
      var _iteratorError15 = undefined;

      try {
        for (var _iterator15 = ids[Symbol.iterator](), _step15; !(_iteratorNormalCompletion15 = (_step15 = _iterator15.next()).done); _iteratorNormalCompletion15 = true) {
          var id = _step15.value;

          if (/חסום|סגור|block/i.test(verb)) muts.push({ op: 'removeEdge', edgeId: id, pair: true });else if (/חד|oneway/i.test(verb)) muts.push({ op: 'setOneway', edgeId: id, oneway: true });else if (/פתח|open/i.test(verb)) muts.push({ op: 'modifyEdge', edgeId: id, set: { modes: { car: true, walk: true } } });else muts.push({ op: 'setOneway', edgeId: id, oneway: false });
        }
      } catch (err) {
        _didIteratorError15 = true;
        _iteratorError15 = err;
      } finally {
        try {
          if (!_iteratorNormalCompletion15 && _iterator15['return']) {
            _iterator15['return']();
          }
        } finally {
          if (_didIteratorError15) {
            throw _iteratorError15;
          }
        }
      }

      msgs.push('✓ ' + cmd + ' (' + ids.length + ' מקטעים)');
    }
  } catch (err) {
    _didIteratorError14 = true;
    _iteratorError14 = err;
  } finally {
    try {
      if (!_iteratorNormalCompletion14 && _iterator14['return']) {
        _iterator14['return']();
      }
    } finally {
      if (_didIteratorError14) {
        throw _iteratorError14;
      }
    }
  }

  return { muts: muts, msg: msgs.join(' · ') };
}

/* ---------------------------------------------------------------- UI */
function Chip(_ref3) {
  var active = _ref3.active;
  var onClick = _ref3.onClick;
  var children = _ref3.children;
  var disabled = _ref3.disabled;
  var title = _ref3.title;

  return React.createElement(
    'button',
    { title: title, disabled: disabled, onClick: onClick, style: {
        padding: '7px 12px', borderRadius: 9, border: '1px solid ' + (active ? '#39d1a8' : '#2c4a56'),
        background: active ? 'rgba(57,209,168,.16)' : 'rgba(255,255,255,.03)', color: disabled ? '#5c7482' : active ? '#8ff0d6' : '#cfe0e6',
        fontWeight: 700, fontSize: 13, cursor: disabled ? 'not-allowed' : 'pointer', opacity: disabled ? .6 : 1
      } },
    children
  );
}

function JunctionApp() {
  var _useState = useState('loading');

  var _useState2 = _slicedToArray(_useState, 2);

  var status = _useState2[0];
  var setStatus = _useState2[1];

  var _useState3 = useState('');

  var _useState32 = _slicedToArray(_useState3, 2);

  var errMsg = _useState32[0];
  var setErrMsg = _useState32[1];

  var baseRef = useRef(null);var changesRef = useRef(null);var lrtRef = useRef(null);var lrtDataRef = useRef(null);

  var _useState4 = useState('car');

  var _useState42 = _slicedToArray(_useState4, 2);

  var mode = _useState42[0];
  var setMode = _useState42[1];

  var _useState5 = useState(null);

  var _useState52 = _slicedToArray(_useState5, 2);

  var A = _useState52[0];
  var setA = _useState52[1];
  // no default route — only when the user picks A/B

  var _useState6 = useState(null);

  var _useState62 = _slicedToArray(_useState6, 2);

  var B = _useState62[0];
  var setB = _useState62[1];

  var _useState7 = useState('A');

  var _useState72 = _slicedToArray(_useState7, 2);

  var pick = _useState72[0];
  var setPick = _useState72[1];

  var _useState8 = useState(function () {
    return new Set();
  });

  var _useState82 = _slicedToArray(_useState8, 2);

  var activeExp = _useState82[0];
  var setActiveExp = _useState82[1];

  var _useState9 = useState(true);

  var _useState92 = _slicedToArray(_useState9, 2);

  var showToday = _useState92[0];
  var setShowToday = _useState92[1];

  var _useState10 = useState(true);

  var _useState102 = _slicedToArray(_useState10, 2);

  var showFinal = _useState102[0];
  var setShowFinal = _useState102[1];

  var _useState11 = useState(false);

  var _useState112 = _slicedToArray(_useState11, 2);

  var showChanges = _useState112[0];
  var setShowChanges = _useState112[1];

  var _useState12 = useState(false);

  var _useState122 = _slicedToArray(_useState12, 2);

  var showBans = _useState122[0];
  var setShowBans = _useState122[1];

  var _useState13 = useState(true);

  var _useState132 = _slicedToArray(_useState13, 2);

  var showCad = _useState132[0];
  var setShowCad = _useState132[1];
  var cadRef = useRef(null);

  var _useState14 = useState(false);

  var _useState142 = _slicedToArray(_useState14, 2);

  var debug = _useState142[0];
  var setDebug = _useState142[1];

  var _useState15 = useState('final');

  var _useState152 = _slicedToArray(_useState15, 2);

  var detailState = _useState152[0];
  var setDetailState = _useState152[1];
  // which turn list to show

  var _useState16 = useState(false);

  var _useState162 = _slicedToArray(_useState16, 2);

  var draw = _useState162[0];
  var setDraw = _useState162[1];

  var _useState17 = useState([]);

  var _useState172 = _slicedToArray(_useState17, 2);

  var drawPts = _useState172[0];
  var setDrawPts = _useState172[1];

  var drawRef = useRef(false);useEffect(function () {
    drawRef.current = draw;
  }, [draw]);

  var _useState18 = useState('');

  var _useState182 = _slicedToArray(_useState18, 2);

  var freeText = _useState182[0];
  var setFreeText = _useState182[1];

  var _useState19 = useState([]);

  var _useState192 = _slicedToArray(_useState19, 2);

  var freeMuts = _useState192[0];
  var setFreeMuts = _useState192[1];

  var _useState20 = useState('');

  var _useState202 = _slicedToArray(_useState20, 2);

  var freeMsg = _useState202[0];
  var setFreeMsg = _useState202[1];

  var mapRef = useRef(null);var layersRef = useRef({});
  var graphCache = useRef({});

  // street-name index (distinct names within ~1km of the junction) for free-text A/B
  var streets = useMemo(function () {
    if (status !== 'ready') return [];
    var base = baseRef.current,
        byName = {};
    for (var id in base.edges) {
      var e = base.edges[id];var nm = (e.name || '').trim();if (!nm) continue;
      var _arr = [e.from, e.to];
      for (var _i = 0; _i < _arr.length; _i++) {
        var nid = _arr[_i];
        var n = base.nodes[nid];if (!n) continue;
        var d = haversine([CENTER[1], CENTER[0]], [n.lng, n.lat]);
        if (!byName[nm] || d < byName[nm].d) byName[nm] = { d: d, lat: n.lat, lng: n.lng };
      }
    }
    var arr = Object.keys(byName).filter(function (nm) {
      return byName[nm].d <= 1000;
    }).map(function (nm) {
      return { name: nm, lat: byName[nm].lat, lng: byName[nm].lng, d: byName[nm].d };
    }).sort(function (a, b) {
      return a.d - b.d;
    });
    var land = LANDMARKS.map(function (l) {
      return { name: l.name, lat: l.lat, lng: l.lng, d: 0, landmark: true };
    });
    return land.concat(arr);
  }, [status]);

  // load data
  useEffect(function () {
    Promise.all([fetch(GRAPH_URL).then(function (r) {
      return r.json();
    }), fetch(CHANGES_URL).then(function (r) {
      return r.json();
    }), fetch(LRT_URL).then(function (r) {
      return r.json();
    })['catch'](function () {
      return null;
    })]).then(function (_ref4) {
      var _ref42 = _slicedToArray(_ref4, 3);

      var g = _ref42[0];
      var c = _ref42[1];
      var lrt = _ref42[2];
      baseRef.current = toWorking(g);changesRef.current = c;if (lrt) {
        lrtDataRef.current = lrt;lrtRef.current = buildLrtGraph(lrt.segments || []);
      }setStatus('ready');
    })['catch'](function (err) {
      setErrMsg(String(err));setStatus('error');
    });
  }, []);

  var expIds = useMemo(function () {
    return Array.from(activeExp).sort().join(',');
  }, [activeExp]);
  function effective(includeFinal) {
    if (status !== 'ready') return null;
    var key = (includeFinal ? 'F' : 'T') + '|' + expIds;
    if (!graphCache.current[key]) graphCache.current[key] = buildEffective(baseRef.current, changesRef.current, activeExp, includeFinal);
    return graphCache.current[key];
  }
  // invalidate cache when experiments change
  useEffect(function () {
    graphCache.current = {};
  }, [expIds]);

  var todayEff = effective(false),
      finalEff = effective(true);
  // transit: "today" has no LRT (walk), "final" rides the two new lines
  var applyFreeWG = function applyFreeWG(eff) {
    if (!eff) return null;if (!freeMuts.length) return eff.wg;var wg = cloneWG(eff.wg);var _iteratorNormalCompletion16 = true;
    var _didIteratorError16 = false;
    var _iteratorError16 = undefined;

    try {
      for (var _iterator16 = freeMuts[Symbol.iterator](), _step16; !(_iteratorNormalCompletion16 = (_step16 = _iterator16.next()).done); _iteratorNormalCompletion16 = true) {
        var m = _step16.value;
        applyMutation(wg, m, function () {});
      }
    } catch (err) {
      _didIteratorError16 = true;
      _iteratorError16 = err;
    } finally {
      try {
        if (!_iteratorNormalCompletion16 && _iterator16['return']) {
          _iterator16['return']();
        }
      } finally {
        if (_didIteratorError16) {
          throw _iteratorError16;
        }
      }
    }

    return wg;
  };
  var todayWG = useMemo(function () {
    return applyFreeWG(todayEff);
  }, [todayEff, freeMuts]);
  var finalWG = useMemo(function () {
    return applyFreeWG(finalEff);
  }, [finalEff, freeMuts]);
  var routeToday = useMemo(function () {
    return !todayWG || !A || !B ? null : mode === 'transit' ? routeWalk(todayWG, nearestNode(todayWG, A, 'walk'), nearestNode(todayWG, B, 'walk')) : route(todayWG, mode, A, B);
  }, [todayWG, mode, A, B, status]);
  var routeFinal = useMemo(function () {
    return !finalWG || !A || !B ? null : mode === 'transit' ? routeTransit(lrtRef.current, A, B) : route(finalWG, mode, A, B);
  }, [finalWG, mode, A, B, status]);

  // init map
  useEffect(function () {
    if (status !== 'ready' || mapRef.current) return;
    var map = L.map('jmap', { center: CENTER, zoom: ZOOM, zoomControl: false, maxZoom: 20, minZoom: 14 });
    L.control.zoom({ position: 'topleft' }).addTo(map);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', { maxZoom: 20, attribution: '© OpenStreetMap, © CARTO', subdomains: 'abcd' }).addTo(map);
    map.on('click', function (ev) {
      var ll = { lat: ev.latlng.lat, lng: ev.latlng.lng };
      if (drawRef.current) {
        setDrawPts(function (pts) {
          return [].concat(_toConsumableArray(pts), [[+ll.lng.toFixed(6), +ll.lat.toFixed(6)]]);
        });return;
      }
      setPick(function (p) {
        if (p === 'A') {
          setA(ll);return 'B';
        }setB(ll);return 'A';
      });
    });
    mapRef.current = map;
    layersRef.current = { cad: L.layerGroup().addTo(map), routes: L.layerGroup().addTo(map), changes: L.layerGroup().addTo(map), bans: L.layerGroup().addTo(map), draw: L.layerGroup().addTo(map), markers: L.layerGroup().addTo(map), debug: L.layerGroup().addTo(map) };
  }, [status]);

  // draw planned-infrastructure overlay (new road, banned turns, two-way, closures)
  useEffect(function () {
    var map = mapRef.current;if (!map) return;
    var layer = layersRef.current.changes;layer.clearLayers();
    if (!showChanges || !finalEff) return;
    var toLL = function toLL(c) {
      return c.map(function (p) {
        return [p[1], p[0]];
      });
    };
    // LRT lines (from the rakal layer): teal = line 3, purple = line 8
    if (lrtDataRef.current) {
      var _iteratorNormalCompletion17 = true;
      var _didIteratorError17 = false;
      var _iteratorError17 = undefined;

      try {
        for (var _iterator17 = (lrtDataRef.current.segments || [])[Symbol.iterator](), _step17; !(_iteratorNormalCompletion17 = (_step17 = _iterator17.next()).done); _iteratorNormalCompletion17 = true) {
          var s = _step17.value;

          L.polyline(toLL(s.coords), { color: LRT_COL[s.line] || '#39d1a8', weight: 4, opacity: .8, dashArray: '1 7', lineCap: 'round' }).addTo(layer).bindTooltip((((lrtDataRef.current.meta || {}).lines || {})[s.line] || {}).name || 'רק"ל קו ' + s.line, { sticky: true });
        }
      } catch (err) {
        _didIteratorError17 = true;
        _iteratorError17 = err;
      } finally {
        try {
          if (!_iteratorNormalCompletion17 && _iterator17['return']) {
            _iterator17['return']();
          }
        } finally {
          if (_didIteratorError17) {
            throw _iteratorError17;
          }
        }
      }
    }
    var chip = function chip(text, bg) {
      return L.divIcon({ className: '', html: '<span style="background:' + bg + ';color:#12303a;font-weight:800;font-size:10px;padding:1px 6px;border-radius:8px;white-space:nowrap;box-shadow:0 1px 3px #000">' + text + '</span>', iconSize: [0, 0] });
    };
    // two-way highlight on existing roads (subtle underlay)
    var _iteratorNormalCompletion18 = true;
    var _didIteratorError18 = false;
    var _iteratorError18 = undefined;

    try {
      for (var _iterator18 = finalEff.features[Symbol.iterator](), _step18; !(_iteratorNormalCompletion18 = (_step18 = _iterator18.next()).done); _iteratorNormalCompletion18 = true) {
        var f = _step18.value;

        if (f.kind === 'twoway') L.polyline(toLL(f.coords), { color: '#ffd166', weight: 12, opacity: .22, lineCap: 'round' }).addTo(layer).bindTooltip('דו-סטרי', { sticky: true });
      }
      // ALL new roads drawn uniformly: road-like fill + colored outline (contour). Outline color marks the type; the fill looks like an existing road.
    } catch (err) {
      _didIteratorError18 = true;
      _iteratorError18 = err;
    } finally {
      try {
        if (!_iteratorNormalCompletion18 && _iterator18['return']) {
          _iterator18['return']();
        }
      } finally {
        if (_didIteratorError18) {
          throw _iteratorError18;
        }
      }
    }

    var OUTLINE = { ring: '#c026d3', road34: '#c026d3', new_road: '#c026d3', lrt_only: '#0d9488' };
    var drawNewRoad = function drawNewRoad(segs, style, label) {
      var outline = OUTLINE[style] || '#c026d3';
      var pts = [];
      var _iteratorNormalCompletion19 = true;
      var _didIteratorError19 = false;
      var _iteratorError19 = undefined;

      try {
        for (var _iterator19 = segs[Symbol.iterator](), _step19; !(_iteratorNormalCompletion19 = (_step19 = _iterator19.next()).done); _iteratorNormalCompletion19 = true) {
          var seg = _step19.value;

          var ll = toLL(seg);
          L.polyline(ll, { color: outline, weight: 9, opacity: .95, lineCap: 'round', lineJoin: 'round' }).addTo(layer); // קו מתאר
          L.polyline(ll, { color: '#fbf6ea', weight: 4.5, opacity: 1, lineCap: 'round', lineJoin: 'round', dashArray: style === 'lrt_only' ? '3 5' : null }).addTo(layer).bindTooltip(label, { sticky: true }); // מיסעה כמו רחוב קיים
          pts = pts.concat(seg);
        }
      } catch (err) {
        _didIteratorError19 = true;
        _iteratorError19 = err;
      } finally {
        try {
          if (!_iteratorNormalCompletion19 && _iterator19['return']) {
            _iterator19['return']();
          }
        } finally {
          if (_didIteratorError19) {
            throw _iteratorError19;
          }
        }
      }
    };
    var _iteratorNormalCompletion20 = true;
    var _didIteratorError20 = false;
    var _iteratorError20 = undefined;

    try {
      for (var _iterator20 = finalEff.features[Symbol.iterator](), _step20; !(_iteratorNormalCompletion20 = (_step20 = _iterator20.next()).done); _iteratorNormalCompletion20 = true) {
        var f = _step20.value;

        if (f.kind === 'marked') drawNewRoad(f.segs, f.style, f.label);else if (f.kind === 'new_road') drawNewRoad([f.coords], 'new_road', f.label || 'רחוב חדש');else if (f.kind === 'closed') L.polyline(toLL(f.coords), { color: '#e5484d', weight: 6, opacity: .85, dashArray: '4 8' }).addTo(layer).bindTooltip(f.label || 'מקטע סגור', { sticky: true });
      }
    } catch (err) {
      _didIteratorError20 = true;
      _iteratorError20 = err;
    } finally {
      try {
        if (!_iteratorNormalCompletion20 && _iterator20['return']) {
          _iterator20['return']();
        }
      } finally {
        if (_didIteratorError20) {
          throw _iteratorError20;
        }
      }
    }
  }, [showChanges, finalEff, status]);

  // turn-cancellations layer (numbered markers, like the presentation p19) — toggled independently
  useEffect(function () {
    var map = mapRef.current;if (!map) return;
    var layer = layersRef.current.bans;layer.clearLayers();
    if (!showBans || !finalEff) return;
    var _iteratorNormalCompletion21 = true;
    var _didIteratorError21 = false;
    var _iteratorError21 = undefined;

    try {
      for (var _iterator21 = finalEff.features[Symbol.iterator](), _step21; !(_iteratorNormalCompletion21 = (_step21 = _iterator21.next()).done); _iteratorNormalCompletion21 = true) {
        var f = _step21.value;

        if (f.kind !== 'ban') continue;
        var html = '<div style="width:22px;height:22px;border-radius:50%;background:#e5484d;border:2px solid #fff;box-shadow:0 1px 3px #000;color:#fff;font-weight:800;font-size:12px;display:flex;align-items:center;justify-content:center">' + (f.num || '⛔') + '</div>';
        L.marker([f.lat, f.lng], { icon: L.divIcon({ className: '', html: html, iconSize: [22, 22], iconAnchor: [11, 11] }) }).addTo(layer).bindTooltip(f.label || 'תנועה מבוטלת', { direction: 'top' });
      }
    } catch (err) {
      _didIteratorError21 = true;
      _iteratorError21 = err;
    } finally {
      try {
        if (!_iteratorNormalCompletion21 && _iterator21['return']) {
          _iterator21['return']();
        }
      } finally {
        if (_didIteratorError21) {
          throw _iteratorError21;
        }
      }
    }
  }, [showBans, finalEff, status]);

  // draw routes + markers
  useEffect(function () {
    var map = mapRef.current;if (!map) return;
    var _layersRef$current = layersRef.current;
    var routes = _layersRef$current.routes;
    var markers = _layersRef$current.markers;
    routes.clearLayers();markers.clearLayers();
    var toLL = function toLL(c) {
      return c.map(function (p) {
        return [p[1], p[0]];
      });
    };
    var drawRoute = function drawRoute(r, isFinal) {
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
    var pin = function pin(p, label, color) {
      return L.marker([p.lat, p.lng], { icon: L.divIcon({ className: '', html: '<div class="ab-pin" style="color:' + color + '">' + label + '</div>', iconSize: [26, 26], iconAnchor: [13, 24] }) }).addTo(markers);
    };
    if (A) pin(A, '📍', COL.a);if (B) pin(B, '🏁', COL.b);
  }, [routeToday, routeFinal, showToday, showFinal, A, B, status]);

  // precise CAD plan overlay (georeferenced curbs from the DWG) — lazy-loaded, toggleable
  useEffect(function () {
    var map = mapRef.current;if (!map) return;
    var layer = layersRef.current.cad;if (!layer) return;
    layer.clearLayers();
    if (!showCad) return;
    // new roads render like ordinary OSM/basemap streets (white casing) — future state
    var STYLE = {
      curb: { color: '#9aa7b0', weight: .5, opacity: .18 },
      island: { color: '#7fb98a', weight: 1, opacity: .5 },
      bus: { color: '#5b9bd5', weight: 1.6, opacity: .6 },
      bike: { color: '#e0a94a', weight: 1.3, opacity: .55 },
      furniture: { color: '#b6c2cb', weight: .6, opacity: .35 },
      tree: { color: '#5aa06f', weight: 1, opacity: .45 }
    };
    var draw = function draw(fc) {
      var _iteratorNormalCompletion22 = true;
      var _didIteratorError22 = false;
      var _iteratorError22 = undefined;

      try {
        for (var _iterator22 = fc.features[Symbol.iterator](), _step22; !(_iteratorNormalCompletion22 = (_step22 = _iterator22.next()).done); _iteratorNormalCompletion22 = true) {
          var f = _step22.value;

          var kind = (f.properties || {}).kind;
          if (kind === 'centerline') continue; // existing roads already drawn by the basemap
          var g = f.geometry;
          var lines = g.type === 'MultiLineString' ? g.coordinates : [g.coordinates];
          var _iteratorNormalCompletion23 = true;
          var _didIteratorError23 = false;
          var _iteratorError23 = undefined;

          try {
            for (var _iterator23 = lines[Symbol.iterator](), _step23; !(_iteratorNormalCompletion23 = (_step23 = _iterator23.next()).done); _iteratorNormalCompletion23 = true) {
              var ln = _step23.value;

              var ll = ln.map(function (p) {
                return [p[1], p[0]];
              });
              if (kind === 'new_axis') {
                // draw as a normal white street (casing + fill)
                L.polyline(ll, { color: '#c8bda3', weight: 6.5, opacity: .95, lineCap: 'round', lineJoin: 'round' }).addTo(layer);
                L.polyline(ll, { color: '#ffffff', weight: 4, opacity: 1, lineCap: 'round', lineJoin: 'round' }).addTo(layer);
              } else L.polyline(ll, STYLE[kind] || { color: '#8794a0', weight: 1, opacity: .5 }).addTo(layer);
            }
          } catch (err) {
            _didIteratorError23 = true;
            _iteratorError23 = err;
          } finally {
            try {
              if (!_iteratorNormalCompletion23 && _iterator23['return']) {
                _iterator23['return']();
              }
            } finally {
              if (_didIteratorError23) {
                throw _iteratorError23;
              }
            }
          }
        }
      } catch (err) {
        _didIteratorError22 = true;
        _iteratorError22 = err;
      } finally {
        try {
          if (!_iteratorNormalCompletion22 && _iterator22['return']) {
            _iterator22['return']();
          }
        } finally {
          if (_didIteratorError22) {
            throw _iteratorError22;
          }
        }
      }
    };
    if (cadRef.current) draw(cadRef.current);else fetch('data/junction_cad.geojson?v=2026-07-27c').then(function (r) {
      return r.json();
    }).then(function (fc) {
      cadRef.current = fc;if (showCad) draw(fc);
    })['catch'](function () {});
  }, [showCad, status]);

  // digitize overlay: the polyline the user is drawing
  useEffect(function () {
    var map = mapRef.current;if (!map) return;
    var dl = layersRef.current.draw;dl.clearLayers();
    if (!drawPts.length) return;
    if (drawPts.length > 1) L.polyline(drawPts.map(function (p) {
      return [p[1], p[0]];
    }), { color: '#ffe14d', weight: 5, opacity: .95, lineCap: 'round' }).addTo(dl);
    drawPts.forEach(function (p, i) {
      return L.circleMarker([p[1], p[0]], { radius: 4, color: '#ffe14d', fillColor: '#12303a', fillOpacity: 1, weight: 2 }).addTo(dl).bindTooltip(String(i + 1));
    });
  }, [drawPts, status]);

  // debug overlay: edge/node ids of current-final graph within view
  useEffect(function () {
    var map = mapRef.current;if (!map) return;
    var dbg = layersRef.current.debug;dbg.clearLayers();
    if (!debug || !finalEff) return;
    var draw = function draw() {
      dbg.clearLayers();var b = map.getBounds();
      var wg = finalEff.wg;var n = 0;
      for (var id in wg.edges) {
        var e = wg.edges[id];var mid = e.geometry[Math.floor(e.geometry.length / 2)];
        if (!b.contains([mid[1], mid[0]])) continue;if (n++ > 400) break;
        L.polyline(e.geometry.map(function (p) {
          return [p[1], p[0]];
        }), { color: e.modes.car ? '#4ad' : '#a6a', weight: 1.5, opacity: .5 }).addTo(dbg);
        L.marker([mid[1], mid[0]], { icon: L.divIcon({ className: '', html: '<span class="lbl-badge">' + id + '</span>', iconSize: [0, 0] }) }).addTo(dbg);
      }
      for (var id in wg.nodes) {
        var nd = wg.nodes[id];if (!b.contains([nd.lat, nd.lng])) continue;if (Math.random() < .55) continue;L.marker([nd.lat, nd.lng], { icon: L.divIcon({ className: '', html: '<span class="lbl-node">' + id + '</span>', iconSize: [0, 0] }) }).addTo(dbg);
      }
    };
    draw();map.on('moveend', draw);
    return function () {
      return map.off('moveend', draw);
    };
  }, [debug, finalEff, status]);

  if (status === 'loading') return null;
  if (status === 'error') return React.createElement(
    'div',
    { style: { padding: 30, color: '#f88' } },
    'שגיאה בטעינת הנתונים: ',
    errMsg
  );

  var changes = changesRef.current;
  var experiments = (changes.changesets || []).filter(function (c) {
    return !c.isFinalPlan;
  });
  var detailRoute = detailState === 'today' ? routeToday : routeFinal;
  var steps = turnSteps(detailRoute);
  var fmt = function fmt(m) {
    return m == null ? '—' : m >= 1000 ? (m / 1000).toFixed(2) + ' ק"מ' : Math.round(m) + ' מ׳';
  };
  var delta = routeToday && routeFinal ? routeFinal.length - routeToday.length : null;

  return React.createElement(
    'div',
    { style: { display: 'flex', height: '100vh', width: '100vw' } },
    React.createElement(
      'div',
      { style: { width: 360, minWidth: 360, height: '100%', overflowY: 'auto', background: 'linear-gradient(180deg,#12303a,#0d1b22)', borderInlineStart: '1px solid #1d3a45', padding: '16px 16px 40px', display: 'flex', flexDirection: 'column', gap: 14 } },
      React.createElement(
        'div',
        null,
        React.createElement(
          'div',
          { style: { fontSize: 20, fontWeight: 800, color: '#8ff0d6' } },
          'צומת אורנים · מסלול A→B'
        ),
        React.createElement(
          'div',
          { style: { fontSize: 12, color: '#7fa3b0', marginTop: 2 } },
          'השוואת מצב היום מול התכנית הסופית (דו-סטרי + רק"ל)'
        )
      ),
      React.createElement(
        'div',
        null,
        React.createElement(
          'div',
          { style: sectLbl },
          'אופן תנועה'
        ),
        React.createElement(
          'div',
          { style: { display: 'flex', gap: 8 } },
          React.createElement(
            Chip,
            { active: mode === 'car', onClick: function () {
                return setMode('car');
              } },
            '🚗 רכב'
          ),
          React.createElement(
            Chip,
            { active: mode === 'walk', onClick: function () {
                return setMode('walk');
              } },
            '🚶 הליכה'
          ),
          React.createElement(
            Chip,
            { active: mode === 'transit', onClick: function () {
                return setMode('transit');
              }, title: 'רק"ל: הקו התכלת + הסגול (סכמטי)' },
            '🚈 רק"ל'
          )
        )
      ),
      React.createElement(
        'div',
        null,
        React.createElement(
          'div',
          { style: sectLbl },
          'נקודות מוצא ויעד'
        ),
        React.createElement(StreetPicker, { label: 'מוצא (A)', icon: '📍', streets: streets, onPick: function (p) {
            return setA({ lat: p.lat, lng: p.lng });
          } }),
        React.createElement(
          'div',
          { style: { display: 'flex', justifyContent: 'center', margin: '-2px 0' } },
          React.createElement(
            Chip,
            { active: false, onClick: function () {
                setA(B);setB(A);
              }, title: 'הפוך מוצא ויעד' },
            '⇅ הפוך A↔B'
          )
        ),
        React.createElement(StreetPicker, { label: 'יעד (B)', icon: '🏁', streets: streets, onPick: function (p) {
            return setB({ lat: p.lat, lng: p.lng });
          } }),
        React.createElement(
          'div',
          { style: { display: 'flex', gap: 8, marginTop: 2 } },
          React.createElement(
            Chip,
            { active: pick === 'A', onClick: function () {
                return setPick('A');
              } },
            '📍 סימון A במפה'
          ),
          React.createElement(
            Chip,
            { active: pick === 'B', onClick: function () {
                return setPick('B');
              } },
            '🏁 סימון B במפה'
          )
        )
      ),
      React.createElement(
        'div',
        { style: { background: 'rgba(255,255,255,.03)', border: '1px solid #1d3a45', borderRadius: 12, padding: 12 } },
        React.createElement(
          'div',
          { style: sectLbl },
          'השוואת מסלול (',
          mode === 'car' ? 'רכב' : mode === 'walk' ? 'הליכה' : 'רק"ל',
          ')'
        ),
        React.createElement(Row, { swatch: COL.today, dash: true, label: mode === 'transit' ? 'היום (הליכה)' : 'היום', val: fmt(routeToday && routeToday.length), on: showToday, toggle: function () {
            return setShowToday(function (s) {
              return !s;
            });
          } }),
        React.createElement(Row, { swatch: COL.final, label: 'סופי', val: fmt(routeFinal && routeFinal.length), on: showFinal, toggle: function () {
            return setShowFinal(function (s) {
              return !s;
            });
          } }),
        delta != null && React.createElement(
          'div',
          { style: { marginTop: 6, fontSize: 12, color: delta > 5 ? '#f4a13a' : delta < -5 ? '#39d1a8' : '#9bb' } },
          delta > 5 ? '▲ המסלול הסופי ארוך ב-' + fmt(delta) : delta < -5 ? '▼ המסלול הסופי קצר ב-' + fmt(-delta) : '≈ אורך דומה'
        ),
        (!routeToday || !routeFinal) && React.createElement(
          'div',
          { style: { marginTop: 6, fontSize: 12, color: '#f88' } },
          !routeToday ? 'אין מסלול במצב היום. ' : '',
          !routeFinal ? 'אין מסלול במצב הסופי.' : ''
        )
      ),
      React.createElement(
        'div',
        null,
        React.createElement(
          'div',
          { style: { display: 'flex', justifyContent: 'space-between', alignItems: 'center' } },
          React.createElement(
            'div',
            { style: sectLbl },
            'ביטולי פניות (כמו במצגת)'
          ),
          React.createElement(
            'label',
            { style: { display: 'flex', gap: 5, alignItems: 'center', fontSize: 11, color: '#9bb', cursor: 'pointer' } },
            React.createElement('input', { type: 'checkbox', checked: showBans, onChange: function () {
                return setShowBans(function (s) {
                  return !s;
                });
              } }),
            ' הצג'
          )
        ),
        React.createElement(
          'div',
          { style: { display: 'flex', flexDirection: 'column', gap: 5, marginBottom: 12 } },
          (finalEff ? finalEff.features.filter(function (f) {
            return f.kind === 'ban';
          }) : []).map(function (b, i) {
            return React.createElement(
              'button',
              { key: i, onClick: function () {
                  return mapRef.current && mapRef.current.setView([b.lat, b.lng], 18, { animate: true });
                },
                style: { display: 'flex', gap: 8, alignItems: 'center', textAlign: 'start', padding: '6px 8px', borderRadius: 8, border: '1px solid #4a2530', background: 'rgba(229,72,77,.08)', color: '#f3d6da', fontSize: 11.5, cursor: 'pointer' } },
              React.createElement(
                'span',
                { style: { flex: '0 0 20px', width: 20, height: 20, borderRadius: '50%', background: '#e5484d', color: '#fff', fontWeight: 800, fontSize: 11, display: 'flex', alignItems: 'center', justifyContent: 'center' } },
                b.num || '⛔'
              ),
              React.createElement(
                'span',
                { style: { flex: 1 } },
                (b.label || 'תנועה מבוטלת').replace(/^ביטול #\d+:\s*/, '')
              )
            );
          })
        ),
        React.createElement(
          'div',
          { style: sectLbl },
          'תרחישים ניסיוניים (מוגדרים בקוד)'
        ),
        experiments.map(function (cs) {
          return React.createElement(
            'label',
            { key: cs.id, style: { display: 'flex', gap: 8, alignItems: 'flex-start', padding: '6px 0', cursor: 'pointer', fontSize: 13, color: '#d6e6ec' } },
            React.createElement('input', { type: 'checkbox', checked: activeExp.has(cs.id), onChange: function () {
                return setActiveExp(function (s) {
                  var n = new Set(s);n.has(cs.id) ? n['delete'](cs.id) : n.add(cs.id);return n;
                });
              }, style: { marginTop: 3 } }),
            React.createElement(
              'span',
              null,
              cs.label
            )
          );
        }),
        React.createElement(
          'div',
          { style: { marginTop: 10, padding: 8, background: 'rgba(255,255,255,.03)', border: '1px solid #2c4a56', borderRadius: 10 } },
          React.createElement(
            'div',
            { style: { fontSize: 11.5, fontWeight: 700, color: '#8ff0d6', marginBottom: 4 } },
            '🧪 בדיקת שינוי חופשי'
          ),
          React.createElement('input', { dir: 'rtl', value: freeText, placeholder: 'למשל: חסום מקור חיים · חד-סטרי e3094', onChange: function (e) {
              return setFreeText(e.target.value);
            },
            onKeyDown: function (e) {
              if (e.key === 'Enter') {
                var r = parseFreeText(freeText, baseRef.current);setFreeMuts(r.muts);setFreeMsg(r.msg);
              }
            },
            style: { width: '100%', padding: '6px 8px', borderRadius: 8, background: '#0f2530', color: '#d6e6ec', border: '1px solid #2c4a56', fontSize: 12, fontFamily: 'inherit' } }),
          React.createElement(
            'div',
            { style: { display: 'flex', gap: 6, marginTop: 5 } },
            React.createElement(
              Chip,
              { active: false, onClick: function () {
                  var r = parseFreeText(freeText, baseRef.current);setFreeMuts(r.muts);setFreeMsg(r.msg);
                } },
              'החל'
            ),
            React.createElement(
              Chip,
              { active: false, onClick: function () {
                  setFreeText('');setFreeMuts([]);setFreeMsg('');
                } },
              'נקה'
            ),
            freeMuts.length > 0 && React.createElement(
              'span',
              { style: { alignSelf: 'center', fontSize: 11, color: '#39d1a8' } },
              freeMuts.length,
              ' שינויים פעילים'
            )
          ),
          freeMsg && React.createElement(
            'div',
            { style: { fontSize: 10.5, color: '#9bb', marginTop: 4 } },
            freeMsg
          ),
          React.createElement(
            'div',
            { style: { fontSize: 10, color: '#5c7482', marginTop: 4 } },
            'פעלים: חסום · חד-סטרי · דו-סטרי · פתח. יעד: שם רחוב או מזהה קשת (e123 / x_ring). לאימות מזהים — הפעל DebugOverlay.'
          )
        ),
        React.createElement(
          'label',
          { style: { display: 'flex', gap: 8, alignItems: 'center', marginTop: 8, fontSize: 12.5, color: '#8ff0d6', cursor: 'pointer' } },
          React.createElement('input', { type: 'checkbox', checked: showCad, onChange: function () {
              return setShowCad(function (s) {
                return !s;
              });
            } }),
          ' הצג את הכבישים החדשים (מצב עתידי, מהתשריט)'
        ),
        React.createElement(
          'label',
          { style: { display: 'flex', gap: 8, alignItems: 'center', marginTop: 8, fontSize: 12.5, color: '#e3c6ee', cursor: 'pointer' } },
          React.createElement('input', { type: 'checkbox', checked: showChanges, onChange: function () {
              return setShowChanges(function (s) {
                return !s;
              });
            } }),
          ' הצג שינויי תשתית (רחוב חדש · ביטולי פניות · דו-סטרי)'
        ),
        React.createElement(
          'label',
          { style: { display: 'flex', gap: 8, alignItems: 'center', marginTop: 6, fontSize: 12, color: '#9bb', cursor: 'pointer' } },
          React.createElement('input', { type: 'checkbox', checked: debug, onChange: function () {
              return setDebug(function (d) {
                return !d;
              });
            } }),
          ' הצג מזהי קשתות/צמתים (DebugOverlay)'
        )
      ),
      React.createElement(
        'div',
        null,
        React.createElement(
          'div',
          { style: sectLbl },
          '✏️ שרטוט תוואי (טבעת / כביש חדש)'
        ),
        React.createElement(
          'div',
          { style: { display: 'flex', gap: 8, marginBottom: 6, flexWrap: 'wrap' } },
          React.createElement(
            Chip,
            { active: draw, onClick: function () {
                return setDraw(function (d) {
                  return !d;
                });
              } },
            draw ? '● משרטט…' : 'שרטט תוואי'
          ),
          React.createElement(
            Chip,
            { active: false, onClick: function () {
                return setDrawPts(function (p) {
                  return p.slice(0, -1);
                });
              } },
            '↶ בטל נקודה'
          ),
          React.createElement(
            Chip,
            { active: false, onClick: function () {
                return setDrawPts([]);
              } },
            'נקה'
          )
        ),
        React.createElement(
          'div',
          { style: { fontSize: 11, color: '#7fa3b0', marginBottom: 6 } },
          'הדליקו "שרטט", לחצו על המפה לאורך התוואי המדויק של הטבעת (יהודה→בן זכאי), והעתיקו את הקואורדינטות אליי או ל-junction_changes.json.'
        ),
        drawPts.length > 0 && React.createElement('textarea', { readOnly: true, value: '"geometry": ' + JSON.stringify(drawPts), onFocus: function (e) {
            return e.target.select();
          },
          style: { width: '100%', height: 68, background: '#0f2530', color: '#ffe14d', border: '1px solid #2c4a56', borderRadius: 8, fontSize: 10.5, fontFamily: 'monospace', padding: 6, direction: 'ltr' } })
      ),
      React.createElement('div', { style: { display: 'none' } }),
      React.createElement(
        'div',
        null,
        React.createElement(
          'div',
          { style: { display: 'flex', justifyContent: 'space-between', alignItems: 'center' } },
          React.createElement(
            'div',
            { style: sectLbl },
            'הוראות מסלול'
          ),
          React.createElement(
            'div',
            { style: { display: 'flex', gap: 6 } },
            React.createElement(
              Chip,
              { active: detailState === 'today', onClick: function () {
                  return setDetailState('today');
                } },
              'היום'
            ),
            React.createElement(
              Chip,
              { active: detailState === 'final', onClick: function () {
                  return setDetailState('final');
                } },
              'סופי'
            )
          )
        ),
        !detailRoute && React.createElement(
          'div',
          { style: { fontSize: 12, color: '#f88', marginTop: 6 } },
          'אין מסלול.'
        ),
        detailRoute && detailRoute.transit && React.createElement(
          'div',
          { style: { display: 'flex', flexDirection: 'column', gap: 4, marginTop: 4 } },
          React.createElement(
            'div',
            { style: legRow },
            React.createElement(
              'span',
              null,
              '🚶'
            ),
            React.createElement(
              'span',
              { style: { flex: 1 } },
              'הליכה לתחנת הרק"ל וממנה'
            ),
            React.createElement(
              'span',
              { style: legDist },
              fmt(detailRoute.walkDist)
            )
          ),
          React.createElement(
            'div',
            { style: legRow },
            React.createElement(
              'span',
              null,
              '🚈'
            ),
            React.createElement(
              'span',
              { style: { flex: 1 } },
              'נסיעה ברק"ל (',
              detailRoute.lines.map(function (l) {
                return LRT_NAMES[l] || 'קו ' + l;
              }).join(' + '),
              ')'
            ),
            React.createElement(
              'span',
              { style: legDist },
              fmt(detailRoute.rideDist)
            )
          )
        ),
        steps.map(function (s, i) {
          return React.createElement(
            'div',
            { key: i, style: { display: 'flex', gap: 8, alignItems: 'baseline', padding: '4px 0', borderBottom: '1px solid #17323c' } },
            React.createElement(
              'span',
              { style: { color: COL.final, fontSize: 15, width: 16 } },
              ACTION_ICON[s.action]
            ),
            React.createElement(
              'span',
              { style: { fontSize: 13, color: '#e3eef2', flex: 1 } },
              ACTION_HE[s.action],
              ' ',
              React.createElement(
                'b',
                null,
                s.name
              )
            ),
            React.createElement(
              'span',
              { style: { fontSize: 11, color: '#7fa3b0' } },
              fmt(s.length)
            )
          );
        })
      ),
      React.createElement(
        'div',
        { style: { marginTop: 'auto', fontSize: 10.5, color: '#5c7482' } },
        'מקור גיאומטריה: OpenStreetMap · שכבת שינויים מהמצגת 02.07.2026. תח"צ ומסלולי הליכה מפורטים — סכמטיים.'
      )
    ),
    React.createElement(
      'div',
      { style: { position: 'relative', flex: 1 } },
      React.createElement('div', { id: 'jmap', style: { position: 'absolute', inset: 0 } }),
      React.createElement(
        'div',
        { style: { position: 'absolute', top: 10, insetInlineEnd: 10, zIndex: 500, background: 'rgba(13,27,34,.85)', border: '1px solid #1d3a45', borderRadius: 10, padding: '8px 10px', fontSize: 12, color: '#cfe0e6' } },
        React.createElement(
          'div',
          { style: { display: 'flex', gap: 6, alignItems: 'center' } },
          React.createElement('span', { style: { width: 22, height: 0, borderTop: '4px dotted ' + COL.today } }),
          ' מסלול היום'
        ),
        React.createElement(
          'div',
          { style: { display: 'flex', gap: 6, alignItems: 'center', marginTop: 3 } },
          React.createElement('span', { style: { width: 22, height: 0, borderTop: '5px solid ' + COL.final } }),
          ' מסלול סופי'
        ),
        React.createElement('div', { style: { borderTop: '1px solid #1d3a45', margin: '6px 0' } }),
        React.createElement(
          'div',
          { style: { display: 'flex', gap: 6, alignItems: 'center' } },
          React.createElement('span', { style: { width: 22, height: 0, borderTop: '4px dashed #ff4dd2' } }),
          ' רחוב חדש (מחבר)'
        ),
        React.createElement(
          'div',
          { style: { display: 'flex', gap: 6, alignItems: 'center', marginTop: 3 } },
          React.createElement('span', { style: { width: 22, height: 8, background: '#ff4dd2', opacity: .35, borderRadius: 2 } }),
          ' כבישי הצומת החדשים'
        ),
        React.createElement(
          'div',
          { style: { display: 'flex', gap: 6, alignItems: 'center', marginTop: 3 } },
          React.createElement('span', { style: { width: 22, height: 0, borderTop: '5px solid #e8912e' } }),
          ' כביש 34 / רחוב המסילה'
        ),
        React.createElement(
          'div',
          { style: { display: 'flex', gap: 6, alignItems: 'center', marginTop: 3 } },
          React.createElement('span', { style: { width: 22, height: 6, background: '#2dd4bf', opacity: .5, borderRadius: 2 } }),
          ' ציר רק"ל בלבד (ללא רכב)'
        ),
        React.createElement(
          'div',
          { style: { display: 'flex', gap: 6, alignItems: 'center', marginTop: 3 } },
          React.createElement(
            'span',
            null,
            '⛔'
          ),
          ' ביטול פנייה/גישה'
        ),
        React.createElement(
          'div',
          { style: { display: 'flex', gap: 6, alignItems: 'center', marginTop: 3 } },
          React.createElement('span', { style: { width: 22, height: 6, background: '#ffd166', opacity: .5, borderRadius: 2 } }),
          ' מקטע דו-סטרי'
        )
      )
    )
  );
}
var sectLbl = { fontSize: 11, fontWeight: 700, color: '#6f97a4', textTransform: 'uppercase', letterSpacing: .3, marginBottom: 6 };
var legRow = { display: 'flex', gap: 8, alignItems: 'center', fontSize: 13, color: '#e3eef2', background: 'rgba(34,195,166,.06)', border: '1px solid #1d4a44', borderRadius: 8, padding: '6px 8px' };
var legDist = { fontSize: 11, color: '#7fa3b0' };
var LRT_NAMES = { '3': 'התכלת', '8': 'הסגול' };
var selStyle = { width: '100%', marginBottom: 6, padding: '7px 8px', borderRadius: 8, background: '#0f2530', color: '#d6e6ec', border: '1px solid #2c4a56', fontSize: 12, fontFamily: 'inherit' };
function StreetPicker(_ref5) {
  var _this = this;

  var label = _ref5.label;
  var icon = _ref5.icon;
  var streets = _ref5.streets;
  var onPick = _ref5.onPick;

  var _useState21 = useState('');

  var _useState212 = _slicedToArray(_useState21, 2);

  var q = _useState212[0];
  var setQ = _useState212[1];

  var _useState22 = useState(false);

  var _useState222 = _slicedToArray(_useState22, 2);

  var open = _useState222[0];
  var setOpen = _useState222[1];

  var _useState23 = useState([]);

  var _useState232 = _slicedToArray(_useState23, 2);

  var geo = _useState232[0];
  var setGeo = _useState232[1];

  var _useState24 = useState(false);

  var _useState242 = _slicedToArray(_useState24, 2);

  var busy = _useState242[0];
  var setBusy = _useState242[1];
  var boxRef = useRef(null);
  var matches = useMemo(function () {
    var s = q.trim();var base = !s ? streets : streets.filter(function (x) {
      return x.name.indexOf(s) >= 0;
    });return base.slice(0, 8);
  }, [q, streets]);
  // address geocoding (street + house number) via Nominatim, biased to the Jerusalem area
  useEffect(function () {
    var s = q.trim();if (s.length < 3) {
      setGeo([]);return;
    }
    setBusy(true);
    var ctrl = new AbortController();
    var t = setTimeout(function callee$2$0() {
      var to, url, r, j;
      return regeneratorRuntime.async(function callee$2$0$(context$3$0) {
        while (1) switch (context$3$0.prev = context$3$0.next) {
          case 0:
            to = setTimeout(function () {
              return ctrl.abort();
            }, 5000);
            context$3$0.prev = 1;
            url = 'https://nominatim.openstreetmap.org/search?format=jsonv2&limit=5&countrycodes=il&accept-language=he&viewbox=35.185,31.805,35.255,31.735&bounded=1&q=' + encodeURIComponent(s + ', ירושלים');
            context$3$0.next = 5;
            return regeneratorRuntime.awrap(fetch(url, { signal: ctrl.signal }));

          case 5:
            r = context$3$0.sent;
            context$3$0.next = 8;
            return regeneratorRuntime.awrap(r.json());

          case 8:
            j = context$3$0.sent;

            setGeo((j || []).map(function (x) {
              return { name: x.display_name.split(',').slice(0, 3).join(',').trim(), lat: +x.lat, lng: +x.lon, addr: true };
            }));
            context$3$0.next = 15;
            break;

          case 12:
            context$3$0.prev = 12;
            context$3$0.t0 = context$3$0['catch'](1);
            setGeo([]);

          case 15:
            clearTimeout(to);setBusy(false);

          case 17:
          case 'end':
            return context$3$0.stop();
        }
      }, null, _this, [[1, 12]]);
    }, 550);
    return function () {
      clearTimeout(t);ctrl.abort();setBusy(false);
    };
  }, [q]);
  useEffect(function () {
    var h = function h(ev) {
      if (boxRef.current && !boxRef.current.contains(ev.target)) setOpen(false);
    };document.addEventListener('mousedown', h);return function () {
      return document.removeEventListener('mousedown', h);
    };
  }, []);
  var list = matches.concat(geo);
  return React.createElement(
    'div',
    { ref: boxRef, style: { position: 'relative', marginBottom: 6 } },
    React.createElement('input', { dir: 'rtl', value: q, placeholder: icon + ' ' + label + ' — רחוב או כתובת (רחוב + מס׳)…', onFocus: function () {
        return setOpen(true);
      }, onChange: function (e) {
        setQ(e.target.value);setOpen(true);
      }, style: selStyle }),
    open && (list.length > 0 || busy) && React.createElement(
      'div',
      { style: { position: 'absolute', zIndex: 1000, top: '100%', insetInlineStart: 0, insetInlineEnd: 0, background: '#0f2530', border: '1px solid #2c4a56', borderRadius: 8, maxHeight: 260, overflowY: 'auto', boxShadow: '0 8px 24px rgba(0,0,0,.5)' } },
      list.map(function (m, i) {
        return React.createElement(
          'div',
          { key: i, onMouseDown: function () {
              onPick(m);setQ(m.addr ? m.name.split(',')[0] : m.name);setOpen(false);
            }, style: { padding: '7px 9px', fontSize: 12.5, color: '#d6e6ec', cursor: 'pointer', borderBottom: '1px solid #17323c', display: 'flex', justifyContent: 'space-between', gap: 8 } },
          React.createElement(
            'span',
            null,
            m.addr ? '🏠 ' : m.landmark ? '📌 ' : '',
            m.name
          ),
          m.d ? React.createElement(
            'span',
            { style: { color: '#6f97a4', fontSize: 10 } },
            Math.round(m.d),
            ' מ׳'
          ) : null
        );
      }),
      busy && geo.length === 0 && React.createElement(
        'div',
        { style: { padding: '7px 9px', fontSize: 11, color: '#6f97a4' } },
        'מחפש כתובת…'
      )
    )
  );
}
function Row(_ref6) {
  var swatch = _ref6.swatch;
  var dash = _ref6.dash;
  var label = _ref6.label;
  var val = _ref6.val;
  var on = _ref6.on;
  var toggle = _ref6.toggle;

  return React.createElement(
    'div',
    { style: { display: 'flex', alignItems: 'center', gap: 8, padding: '3px 0' } },
    React.createElement('input', { type: 'checkbox', checked: on, onChange: toggle }),
    React.createElement('span', { style: { width: 22, height: 0, borderTop: (dash ? '4px dotted ' : '5px solid ') + swatch } }),
    React.createElement(
      'span',
      { style: { fontSize: 13, color: '#d6e6ec', flex: 1 } },
      label
    ),
    React.createElement(
      'span',
      { style: { fontSize: 13, fontWeight: 700, color: '#8ff0d6' } },
      val
    )
  );
}

// lightweight test hook (used for verification; harmless in production)
window.__J = { route: route, buildEffective: buildEffective, toWorking: toWorking, turnSteps: turnSteps, nearestNode: nearestNode };

ReactDOM.createRoot(document.getElementById('root')).render(React.createElement(JunctionApp, null));
/* sidebar */ /* mode */ /* A/B */ /* comparison summary */ /* experiments */ /* digitize tool */ /* turn list */ /* map */
