// Standalone verification of the per-stat-area bucketing math (no build toolchain
// available locally). Replicates computePublicNeeds + sumPublicNeeds verbatim from
// src/app.jsx and asserts the invariants the refactor relies on.
const PUBLIC_NEEDS_SERVICES = [
  { key:'maon', ageFrom:0, ageTo:3, participation:0.5, dunamPerClass:1.0/3,
    streams:{ mamlakhti:{classSize:20}, haredi_b:{classSize:20}, haredi_g:{classSize:20} } },
  { key:'gan', ageFrom:3, ageTo:6, participation:1.0, dunamPerClass:0.5,
    streams:{ mamlakhti:{classSize:30}, haredi_b:{classSize:20}, haredi_g:{classSize:30} } },
  { key:'yesodi', ageFrom:6, ageTo:14, participation:1.0,
    streams:{ mamlakhti:{classSize:27,dunamPerClass:0.3}, haredi_b:{classSize:20,dunamPerClass:0.2}, haredi_g:{classSize:27,dunamPerClass:0.3} } },
  { key:'al_yesodi', ageFrom:14, ageTo:18, participation:1.0,
    streams:{ mamlakhti:{classSize:27,dunamPerClass:0.4}, haredi_b:{classSize:20,dunamPerClass:0.3}, haredi_g:{classSize:27,dunamPerClass:0.4} } },
];

function computePublicNeeds(totalUnits, assumptions) {
  const { householdSize, haredi, ageYearPctGeneral, ageYearPctHaredi } = assumptions;
  const population = Math.max(0, totalUnits) * householdSize;
  const haredi_frac = Math.max(0, Math.min(1, haredi));
  const mam_frac = 1 - haredi_frac;
  const byService = {}; let totalDunam = 0, totalClasses = 0;
  PUBLIC_NEEDS_SERVICES.forEach(svc => {
    const ageSpan = svc.ageTo - svc.ageFrom;
    const children_mam = population * mam_frac * (ageYearPctGeneral/100) * ageSpan * svc.participation;
    const children_hrd_total = population * haredi_frac * (ageYearPctHaredi/100) * ageSpan * svc.participation;
    const calcStream = (k, children) => {
      const s = svc.streams[k]; const classSize = s.classSize;
      const dpc = s.dunamPerClass !== undefined ? s.dunamPerClass : svc.dunamPerClass;
      const classes = children > 0 ? Math.ceil(children/classSize) : 0;
      const dunam = classes * dpc;
      return { children: Math.round(children*10)/10, classes, dunam: Math.round(dunam*100)/100, classSize, dunamPerClass: dpc };
    };
    const mam = calcStream('mamlakhti', children_mam);
    const hb = calcStream('haredi_b', children_hrd_total*0.5);
    const hg = calcStream('haredi_g', children_hrd_total*0.5);
    const tc = mam.classes+hb.classes+hg.classes, td = mam.dunam+hb.dunam+hg.dunam;
    byService[svc.key] = { mam, haredi_b:hb, haredi_g:hg, totalClasses:tc, totalDunam: Math.round(td*100)/100 };
    totalClasses += tc; totalDunam += td;
  });
  return { population: Math.round(population), byService, totalClasses, totalDunam: Math.round(totalDunam*100)/100 };
}

function sumPublicNeeds(buckets, unitsFn) {
  let population=0, totalClasses=0, totalDunam=0; const byService={};
  for (const b of buckets.values()) {
    const n = computePublicNeeds(unitsFn(b), b.assumptions);
    population += n.population; totalClasses += n.totalClasses; totalDunam += n.totalDunam;
    for (const key of Object.keys(n.byService)) {
      const s = n.byService[key];
      if (!byService[key]) byService[key] = {
        mam:{children:0,classes:0,dunam:0,classSize:s.mam.classSize,dunamPerClass:s.mam.dunamPerClass},
        haredi_b:{children:0,classes:0,dunam:0,classSize:s.haredi_b.classSize,dunamPerClass:s.haredi_b.dunamPerClass},
        haredi_g:{children:0,classes:0,dunam:0,classSize:s.haredi_g.classSize,dunamPerClass:s.haredi_g.dunamPerClass},
        totalClasses:0,totalDunam:0 };
      const agg = byService[key];
      ['mam','haredi_b','haredi_g'].forEach(st => { agg[st].children+=s[st].children; agg[st].classes+=s[st].classes; agg[st].dunam+=s[st].dunam; });
      agg.totalClasses += s.totalClasses; agg.totalDunam += s.totalDunam;
    }
  }
  for (const key of Object.keys(byService)) { const agg=byService[key];
    ['mam','haredi_b','haredi_g'].forEach(st => { agg[st].children=Math.round(agg[st].children*10)/10; agg[st].dunam=Math.round(agg[st].dunam*100)/100; });
    agg.totalDunam = Math.round(agg.totalDunam*100)/100;
  }
  return { population: Math.round(population), byService, totalClasses, totalDunam: Math.round(totalDunam*100)/100 };
}

const A = { householdSize:3.5, haredi:0.15, religious:0.30, ageYearPctGeneral:2.2, ageYearPctHaredi:3.0 };
const B = { householdSize:2.4, haredi:0.0,  religious:0.05, ageYearPctGeneral:1.31, ageYearPctHaredi:1.31 };
let pass=0, fail=0;
const eq=(a,b,m)=>{ if(JSON.stringify(a)===JSON.stringify(b)){pass++;} else {fail++; console.log('FAIL',m,'\n  got',JSON.stringify(a),'\n  exp',JSON.stringify(b)); } };
const approx=(a,b,m,tol=0.5)=>{ if(Math.abs(a-b)<=tol){pass++;} else {fail++; console.log('FAIL',m,'got',a,'exp',b);} };

// (1) single bucket == direct call (deep equal)
const single = new Map([['x',{assumptions:A,existing:120,planned:80}]]);
eq(sumPublicNeeds(single, b=>b.existing+b.planned), computePublicNeeds(200, A), 'single-bucket parity');

// (2) population additivity across buckets with different profiles
const multi = new Map([['a',{assumptions:A,existing:300,planned:100}],['b',{assumptions:B,existing:150,planned:50}]]);
const sm = sumPublicNeeds(multi, b=>b.existing+b.planned);
approx(sm.population, computePublicNeeds(400,A).population + computePublicNeeds(200,B).population, 'population additivity', 0);

// (3) ceil-per-bucket >= single combined call (same assumptions split in two)
const split = new Map([['a',{assumptions:A,existing:130,planned:0}],['b',{assumptions:A,existing:70,planned:0}]]);
const combined = computePublicNeeds(200, A);
const splitSum = sumPublicNeeds(split, b=>b.existing+b.planned);
const ge = splitSum.totalClasses >= combined.totalClasses;
if (ge) pass++; else { fail++; console.log('FAIL ceil-per-bucket>=combined', splitSum.totalClasses, combined.totalClasses); }
console.log('split totalClasses', splitSum.totalClasses, 'vs combined', combined.totalClasses, '(ceil overhead expected >=0)');

// (4) zero units -> zero needs
const zero = new Map([['__base__',{assumptions:A,existing:0,planned:0}]]);
eq(sumPublicNeeds(zero, b=>b.existing+b.planned).totalClasses, 0, 'zero units -> zero classes');

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
