// Pure, deterministic models behind the plan page's demos. No DOM here.

// mulberry32: a small seeded PRNG, so every visit draws the same figures.
export function prng(seed) {
  let a = seed >>> 0;
  return function () {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const HOURS = 168;

// ---------------------------------------------------------------------------
// Demo 2: one member's week. Hourly coding demand against a weekly plan limit,
// and the member's own GPU, hour by hour.
// Each day: [first active hour, last active hour (exclusive), intensity].
const WEEKS = {
  deadline: {
    seed: 2026,
    days: [[9, 18, 0.55], [9, 19, 0.85], [9, 20, 1.35], [9, 20, 1.4], [9, 20, 1.3], [13, 16, 0.6], [14, 17, 0.5]],
    gpuLit: 15,
  },
  quiet: {
    seed: 811,
    days: [[10, 16, 0.32], [10, 15, 0.28], [10, 16, 0.3], [11, 15, 0.25], [10, 14, 0.22], [0, 0, 0], [15, 17, 0.2]],
    gpuLit: 11,
  },
};
// The weekly limit is set so the deadline week reaches it on Wednesday afternoon.
const LIMIT_AT = 2 * 24 + 15; // Wed 15:00

function weekDemand(kind) {
  const w = WEEKS[kind];
  const r = prng(w.seed);
  const d = new Array(HOURS).fill(0);
  w.days.forEach(([s, e, k], day) => {
    for (let h = s; h < e; h++) {
      if (h === 12 && e - s > 6) continue; // lunch
      d[day * 24 + h] = k * (0.45 + r() * 1.1);
    }
  });
  return d;
}

let weekLimit = null;
function limit() {
  if (weekLimit === null) {
    const d = weekDemand('deadline');
    let c = 0;
    for (let h = 0; h < LIMIT_AT; h++) c += d[h];
    weekLimit = c;
  }
  return weekLimit;
}

export function week(kind) {
  const d = weekDemand(kind);
  const L = limit();
  const used = []; // cumulative share of the limit, up to the hour the limit is hit
  let c = 0, hitAt = -1, blocked = 0;
  for (let h = 0; h < HOURS; h++) {
    if (hitAt >= 0) { if (d[h] > 0) blocked++; continue; }
    const next = c + d[h];
    if (next >= L - 1e-9) {
      used.push({ h: h + (L - c) / d[h], v: 1 });
      hitAt = h + (L - c) / d[h];
      continue;
    }
    c = next;
    used.push({ h: h + 1, v: c / L });
  }
  // The GPU: the owner's own use, mostly evenings.
  const r = prng(WEEKS[kind].seed * 31 + 7);
  const lit = new Set();
  while (lit.size < WEEKS[kind].gpuLit) {
    const day = Math.floor(r() * 7);
    const hour = r() < 0.7 ? 18 + Math.floor(r() * 6) : Math.floor(r() * 24);
    lit.add(day * 24 + hour);
  }
  const gpu = Array.from({ length: HOURS }, (_, i) => lit.has(i));
  return { demand: d, used, hitAt, blocked, gpu, gpuBusy: lit.size / HOURS };
}

// ---------------------------------------------------------------------------
// Demo 4: why pooling works. Each member's hourly demand is a daily rhythm,
// shifted by their time zone, times bursts that mix a shared component with
// their own. Headroom = 95th percentile over mean.
export const OVERLAPS = ['Low', 'Medium', 'High'];
const SHARED = [0.03, 0.3, 0.7];
const TZ_SPREAD = [24, 8, 0];

function bursts(r) {
  const b = new Float64Array(HOURS);
  for (let h = 0; h < HOURS; h++) b[h] = 0.12 * r();
  const n = 25 + Math.floor(r() * 6);
  for (let i = 0; i < n; i++) {
    const start = Math.floor(r() * HOURS);
    const len = 1 + Math.floor(r() * 5);
    const amp = 0.6 + Math.pow(r(), 2) * 2.4;
    for (let k = 0; k < len; k++) {
      const h = (start + k) % HOURS;
      b[h] += amp * (0.7 + 0.3 * Math.sin((k + 0.5) / len * Math.PI));
    }
  }
  return b;
}

function rhythm(localHour) {
  const x = (localHour - 14) / 4.2;
  return 0.28 + Math.exp(-x * x);
}

const sharedBursts = bursts(prng(4242));
const memberCache = new Map();
function member(i, overlap) {
  const key = i * 3 + overlap;
  let m = memberCache.get(key);
  if (m) return m;
  const r = prng(9001 + i * 7919);
  const tzU = r();
  const own = bursts(r);
  const w = SHARED[overlap];
  const tz = Math.round((tzU - 0.5) * TZ_SPREAD[overlap]);
  m = new Float64Array(HOURS);
  for (let h = 0; h < HOURS; h++) {
    const local = (((h + tz) % 24) + 24) % 24;
    m[h] = rhythm(local) * ((1 - w) * own[h] + w * sharedBursts[h]);
  }
  memberCache.set(key, m);
  return m;
}

function mean(a) { let s = 0; for (const v of a) s += v; return s / a.length; }
function p95(a) {
  const s = Array.from(a).sort((x, y) => x - y);
  return s[Math.min(s.length - 1, Math.ceil(0.95 * s.length) - 1)];
}

export function pool(members, overlap) {
  const one = member(0, overlap);
  const sum = new Float64Array(HOURS);
  for (let i = 0; i < members; i++) {
    const m = member(i, overlap);
    for (let h = 0; h < HOURS; h++) sum[h] += m[h] / members;
  }
  const mo = mean(one), mp = mean(sum);
  return {
    one: Array.from(one, (v) => v / mo),
    pooled: Array.from(sum, (v) => v / mp),
    alone: p95(one) / mo,
    pooledHeadroom: p95(sum) / mp,
  };
}

// ---------------------------------------------------------------------------
// Demo 5: your month in the pool.
export const RATE_PER_GPU_HOUR = 4;
export const COST_PER_CRUNCH_DAY = 120;
export function month(hoursPerDay, crunchDays) {
  const earned = RATE_PER_GPU_HOUR * hoursPerDay * 30;
  const spent = COST_PER_CRUNCH_DAY * crunchDays;
  return { earned, spent, balance: earned - spent };
}
