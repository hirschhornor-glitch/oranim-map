// Verify aggregatePopulationAreas (population dashboard) — replicates the function
// from src/app.jsx and checks additivity, weighting, and lifestyle normalization.
function aggregatePopulationAreas(areas) {
  const sum = (k) => areas.reduce((s, a) => s + (typeof a[k] === 'number' ? a[k] : 0), 0);
  const wavg = (vk, wk) => {
    let sv = 0, sw = 0;
    for (const a of areas) { const v = a[vk], w = a[wk]; if (typeof v === 'number' && typeof w === 'number' && w > 0) { sv += v * w; sw += w; } }
    return sw > 0 ? sv / sw : null;
  };
  const LIFE = ['חרדי', 'דתי/דתי מאוד', 'מסורתי', 'חילוני', 'מעורב', 'אחר'];
  const lifeRaw = {}; let lifeSum = 0;
  LIFE.forEach(c => {
    let sv = 0, sw = 0;
    for (const a of areas) { const bd = a.datiyut_breakdown, w = a.pop_approx; if (bd && typeof bd[c] === 'number' && typeof w === 'number' && w > 0) { sv += bd[c] * w; sw += w; } }
    lifeRaw[c] = sw > 0 ? sv / sw : 0; lifeSum += lifeRaw[c];
  });
  const lifestyle = {}; LIFE.forEach(c => lifestyle[c] = lifeSum > 0 ? lifeRaw[c] / lifeSum * 100 : 0);
  return {
    pop: sum('pop_approx'), hh: sum('hh_total'),
    householdSize: wavg('householdSize', 'pop_approx'), own: wavg('own_pcnt', 'hh_total'),
    lifestyle, hasLifestyle: lifeSum > 0, areaCount: areas.length,
  };
}

let pass = 0, fail = 0;
const ok = (c, m) => { if (c) pass++; else { fail++; console.log('FAIL', m); } };
const near = (a, b, m, t = 1e-9) => ok(Math.abs(a - b) <= t, m + ` (got ${a}, exp ${b})`);

const A = [
  { pop_approx: 1000, hh_total: 400, householdSize: 2.5, own_pcnt: 40, datiyut_breakdown: { 'חרדי': 5, 'דתי/דתי מאוד': 20, 'מסורתי': 30, 'חילוני': 40, 'מעורב': 3, 'אחר': 2 } },
  { pop_approx: 3000, hh_total: 600, householdSize: 5.0, own_pcnt: 70, datiyut_breakdown: { 'חרדי': 80, 'דתי/דתי מאוד': 15, 'מסורתי': 3, 'חילוני': 1, 'מעורב': 1, 'אחר': 0 } },
];
const B = [
  { pop_approx: 2000, hh_total: 500, householdSize: 3.0, own_pcnt: 55 },  // no lifestyle
];

const aggA = aggregatePopulationAreas(A);
const aggB = aggregatePopulationAreas(B);
const aggAB = aggregatePopulationAreas(A.concat(B));

// 1. additivity of sums (minhak total == Σ sub totals)
near(aggAB.pop, aggA.pop + aggB.pop, 'pop additivity', 0);
near(aggAB.hh, aggA.hh + aggB.hh, 'hh additivity', 0);

// 2. population-weighted household size for A: (1000*2.5 + 3000*5.0)/4000 = 4.375
near(aggA.householdSize, (1000 * 2.5 + 3000 * 5.0) / 4000, 'pop-weighted householdSize');

// 3. household-weighted ownership for A: (400*40 + 600*70)/1000 = 58
near(aggA.own, (400 * 40 + 600 * 70) / 1000, 'hh-weighted own%');

// 4. lifestyle normalized to 100 when data present
const lifeTotal = Object.values(aggA.lifestyle).reduce((s, v) => s + v, 0);
near(lifeTotal, 100, 'lifestyle sums to 100', 1e-6);
ok(aggA.hasLifestyle === true, 'A has lifestyle');

// 5. combined household-weighted own includes B's own% (1500 households total)
near(aggAB.own, (400 * 40 + 600 * 70 + 500 * 55) / 1500, 'AB hh-weighted own%');

// 6. lifestyle still normalizes over only areas that have it (A only) in AB
near(Object.values(aggAB.lifestyle).reduce((s, v) => s + v, 0), 100, 'AB lifestyle sums to 100', 1e-6);

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
