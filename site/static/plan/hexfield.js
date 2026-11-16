// The live hexagon field: dim comb cells, a few honey clusters (members busy),
// and thin light trails carrying capacity from idle cells to whoever needs it.
// Every 6–10 s one cluster fades and another lights, so the flows re-route.
import { prng } from './models.js';
import { Loop } from './loop.js';

const SQ3 = Math.sqrt(3);
const PRESETS = {
  hero: { r: 17, clusters: 2, trails: 18, hub: [0.7, 0.47], minX: 0.56, region: [0.4, 0.98, 0.08, 0.92], big: true, bend: 1.4, line: 0.36, lw: 1.3 },
  hub: { r: 13, clusters: 2, trails: 20, hub: [0.74, 0.34], minX: 0.46, region: [0.5, 0.95, 0.08, 0.6] },
  cells: { r: 15, clusters: 0, trails: 0, sparks: 13, region: [0.25, 0.98, 0.15, 0.85] },
};

function glowSprite(rgb) {
  const c = document.createElement('canvas');
  c.width = c.height = 64;
  const g = c.getContext('2d');
  const grd = g.createRadialGradient(32, 32, 0, 32, 32, 32);
  grd.addColorStop(0, `rgba(${rgb},1)`);
  grd.addColorStop(0.25, `rgba(${rgb},.45)`);
  grd.addColorStop(1, `rgba(${rgb},0)`);
  g.fillStyle = grd;
  g.fillRect(0, 0, 64, 64);
  return c;
}

function hexPath(ctx, x, y, r) {
  ctx.moveTo(x, y - r);
  ctx.lineTo(x + (SQ3 / 2) * r, y - r / 2);
  ctx.lineTo(x + (SQ3 / 2) * r, y + r / 2);
  ctx.lineTo(x, y + r);
  ctx.lineTo(x - (SQ3 / 2) * r, y + r / 2);
  ctx.lineTo(x - (SQ3 / 2) * r, y - r / 2);
  ctx.closePath();
}

export function hexField(canvas) {
  const kind = canvas.dataset.field || 'hub';
  const P = { ...PRESETS[kind] };
  if (canvas.dataset.clusters) P.clusters = +canvas.dataset.clusters;
  const seed = +(canvas.dataset.seed || 1);
  const ctx = canvas.getContext('2d');
  const glow = glowSprite('255,198,92');
  let W = 0, H = 0, dpr = 1, base = null, cells = [], cols = 0, rows = 0;
  let state = null;

  function cellAt(c, r) { return cells[r * cols + c]; }
  function neighbours(c, r) {
    const odd = r & 1;
    const d = odd ? [[1, 0], [-1, 0], [0, -1], [1, -1], [0, 1], [1, 1]] : [[1, 0], [-1, 0], [-1, -1], [0, -1], [-1, 1], [0, 1]];
    return d.map(([dc, dr]) => [c + dc, r + dr]).filter(([a, b]) => a >= 0 && b >= 0 && a < cols && b < rows);
  }

  function build() {
    const rect = canvas.getBoundingClientRect();
    W = Math.max(1, Math.round(rect.width));
    H = Math.max(1, Math.round(rect.height));
    dpr = Math.min(2, window.devicePixelRatio || 1);
    canvas.width = Math.round(W * dpr);
    canvas.height = Math.round(H * dpr);
    // a finer comb on narrow screens
    P.r = W < 700 ? Math.max(9, Math.min(PRESETS[kind].r, W / 32)) : PRESETS[kind].r;
    const r = P.r;
    const w = SQ3 * r;
    cols = Math.ceil(W / w) + 2;
    rows = Math.ceil(H / (1.5 * r)) + 2;
    cells = [];
    const rr = prng(seed * 97 + 3);
    for (let j = 0; j < rows; j++) {
      for (let i = 0; i < cols; i++) {
        cells.push({ c: i, r: j, x: (i - 0.5) * w + (j & 1 ? w / 2 : 0), y: (j - 0.5) * 1.5 * r, n: rr() });
      }
    }
    // The comb itself, drawn once.
    base = document.createElement('canvas');
    base.width = canvas.width;
    base.height = canvas.height;
    const b = base.getContext('2d');
    b.scale(dpr, dpr);
    for (const cell of cells) {
      b.beginPath();
      hexPath(b, cell.x, cell.y, r - 1.6);
      b.fillStyle = `rgba(243,232,207,${0.022 + cell.n * 0.03})`;
      b.fill();
      b.strokeStyle = `rgba(237,167,44,${0.07 + cell.n * 0.05})`;
      b.lineWidth = 1;
      b.stroke();
    }
    reset();
  }

  function inRegion(rr) {
    const [x0, x1, y0, y1] = P.region;
    const c = Math.floor((x0 + rr() * (x1 - x0)) * (cols - 2)) + 1;
    const r = Math.floor((y0 + rr() * (y1 - y0)) * (rows - 2)) + 1;
    return [c, r];
  }
  function nearestCell(x, y) {
    const r = Math.max(1, Math.min(rows - 2, Math.round(y / (1.5 * P.r) + 0.5)));
    const w = SQ3 * P.r;
    const c = Math.max(1, Math.min(cols - 2, Math.round((x - (r & 1 ? w / 2 : 0)) / w + 0.5)));
    return [c, r];
  }
  // The hub: where capacity is gathered before it is handed to a busy cluster.
  function hubPoint() {
    const [fx, fy] = P.hub;
    const [c, r] = nearestCell(fx * W, fy * H);
    const h = cellAt(c, r);
    return { x: h.x, y: h.y };
  }
  const reach = () => Math.min(W * 0.55, H * 0.62);

  function makeCluster(rr, taken, hub) {
    let c, r, tries = 0;
    do {
      const a = rr() * Math.PI * 2;
      const d = reach() * (0.32 + rr() * 0.42);
      [c, r] = nearestCell(hub.x + Math.cos(a) * d, hub.y + Math.sin(a) * d * 0.9);
      tries++;
    } while (tries < 60 && (cellAt(c, r).x < (P.minX || 0) * W || taken.some((t) => Math.hypot(t.c - c, (t.r - r) * 0.87) < (P.big ? 7.5 : 5.5))));
    const members = [cellAt(c, r)];
    const ring = neighbours(c, r);
    for (const [a, b] of ring) if (P.big || rr() < 0.8) members.push(cellAt(a, b));
    if (P.big) {
      // a second, partial ring, so a busy member reads as a big warm patch
      const seen = new Set([`${c},${r}`, ...ring.map(([a, b]) => `${a},${b}`)]);
      for (const [a0, b0] of ring) for (const [a, b] of neighbours(a0, b0)) {
        const key = `${a},${b}`;
        if (seen.has(key)) continue;
        seen.add(key);
        if (rr() < 0.3) members.push(cellAt(a, b));
      }
    }
    const centre = cellAt(c, r);
    return { c, r, x: centre.x, y: centre.y, cells: members.filter(Boolean), i: 0, target: 1 };
  }

  // Trails run from an idle cell, bend into the hub, and on to the busy cluster.
  function makeTrails(rr, cl, n, hub) {
    const out = [];
    for (let k = 0; k < n; k++) {
      const a = rr() * Math.PI * 2;
      const d = reach() * (0.35 + rr() * 0.75);
      const [c, r] = nearestCell(hub.x + Math.cos(a) * d, hub.y + Math.sin(a) * d * 0.85);
      const src = cellAt(c, r);
      const bend = (rr() - 0.5) * (P.bend || 0.9);
      const mx = src.x + (hub.x - src.x) * 0.5, my = src.y + (hub.y - src.y) * 0.5;
      const v = { x: mx - (hub.y - src.y) * bend, y: my + (hub.x - src.x) * bend };
      out.push({ s: { x: src.x, y: src.y }, v, h: hub, cl, p: [rr(), rr()], sp: 0.06 + rr() * 0.08, w: 0.5 + rr() * 0.7 });
    }
    return out;
  }

  function reset() {
    const rr = prng(seed);
    const hub = P.hub ? hubPoint() : null;
    const clusters = [];
    for (let k = 0; k < P.clusters; k++) {
      const cl = makeCluster(rr, clusters, hub);
      cl.i = 1;
      clusters.push(cl);
    }
    if (hub && !clusters.length) Object.assign(hub, { solo: true });
    const trails = [];
    if (P.trails && hub) {
      if (clusters.length) clusters.forEach((cl) => trails.push(...makeTrails(rr, cl, Math.round(P.trails / clusters.length), hub)));
      else trails.push(...makeTrails(rr, hub, P.trails, hub));
    }
    const sparks = [];
    for (let k = 0; k < (P.sparks || 0); k++) {
      const [c, r] = inRegion(rr);
      const big = rr() < 0.3;
      const near = big ? neighbours(c, r).filter(() => rr() < 0.5).map(([a, b]) => cellAt(a, b)).filter(Boolean) : [];
      sparks.push({ cell: cellAt(c, r), near, ph: rr() * Math.PI * 2, sp: 0.25 + rr() * 0.35, big });
    }
    state = { rr: prng(seed * 13 + 5), clusters, trails, hub, sparks, next: 6 + rr() * 4, t: 0 };
  }

  function reroute() {
    const s = state;
    const live = s.clusters.filter((c) => c.target === 1);
    if (!live.length) return;
    live[0].target = 0;
    const cl = makeCluster(s.rr, s.clusters.filter((c) => c.target === 1), s.hub);
    s.clusters.push(cl);
    s.trails.push(...makeTrails(s.rr, cl, Math.round(P.trails / P.clusters), s.hub));
  }

  // Position along source → hub (first 72%) → cluster (the rest).
  const SPLIT = 0.72;
  function along(tr, t) {
    if (t < SPLIT || tr.cl === tr.h) {
      const u = tr.cl === tr.h ? t : t / SPLIT, m = 1 - u;
      return { x: m * m * tr.s.x + 2 * m * u * tr.v.x + u * u * tr.h.x, y: m * m * tr.s.y + 2 * m * u * tr.v.y + u * u * tr.h.y };
    }
    const u = (t - SPLIT) / (1 - SPLIT);
    const cx = (tr.h.x + tr.cl.x) / 2 + (tr.cl.y - tr.h.y) * 0.18, cy = (tr.h.y + tr.cl.y) / 2 - (tr.cl.x - tr.h.x) * 0.18;
    const m = 1 - u;
    return { x: m * m * tr.h.x + 2 * m * u * cx + u * u * tr.cl.x, y: m * m * tr.h.y + 2 * m * u * cy + u * u * tr.cl.y };
  }
  function strokeTrail(tr) {
    ctx.moveTo(tr.s.x, tr.s.y);
    ctx.quadraticCurveTo(tr.v.x, tr.v.y, tr.h.x, tr.h.y);
    if (tr.cl !== tr.h) {
      const cx = (tr.h.x + tr.cl.x) / 2 + (tr.cl.y - tr.h.y) * 0.18, cy = (tr.h.y + tr.cl.y) / 2 - (tr.cl.x - tr.h.x) * 0.18;
      ctx.quadraticCurveTo(cx, cy, tr.cl.x, tr.cl.y);
    }
  }

  function draw(dt) {
    const s = state;
    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.drawImage(base, 0, 0);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.globalCompositeOperation = 'lighter';

    // Clusters fade toward their target.
    for (const cl of s.clusters) {
      cl.i += Math.sign(cl.target - cl.i) * Math.min(Math.abs(cl.target - cl.i), dt / 2.6);
    }
    s.clusters = s.clusters.filter((c) => c.target === 1 || c.i > 0.001);
    s.trails = s.trails.filter((t) => t.cl === s.hub || t.cl.target === 1 || t.cl.i > 0.001);

    // Trails.
    ctx.lineCap = 'round';
    for (const tr of s.trails) {
      const a = tr.cl.i ?? 1;
      if (a <= 0) continue;
      ctx.beginPath();
      strokeTrail(tr);
      ctx.strokeStyle = `rgba(237,167,44,${(P.line || 0.2) * a * tr.w})`;
      ctx.lineWidth = P.lw || 1;
      ctx.stroke();
      // the idle source cell
      const sz = 7;
      ctx.globalAlpha = 0.7 * a;
      ctx.drawImage(glow, tr.s.x - sz, tr.s.y - sz, sz * 2, sz * 2);
      // capacity in flight
      for (let k = 0; k < tr.p.length; k++) {
        tr.p[k] = (tr.p[k] + dt * tr.sp) % 1;
        const t = tr.p[k];
        const fade = Math.min(1, t * 6, (1 - t) * 6);
        for (let q = 0; q < 4; q++) {
          const pt = along(tr, Math.max(0, t - q * 0.012));
          const z = (q === 0 ? 6 : 4.5 - q) * (0.8 + tr.w * 0.4);
          ctx.globalAlpha = a * fade * (q === 0 ? 0.95 : 0.35 / q);
          ctx.drawImage(glow, pt.x - z, pt.y - z, z * 2, z * 2);
        }
      }
      ctx.globalAlpha = 1;
    }

    // Busy clusters.
    for (const cl of s.clusters) {
      if (cl.i <= 0) continue;
      const e = cl.i * cl.i * (3 - 2 * cl.i);
      ctx.globalCompositeOperation = 'lighter';
      ctx.globalAlpha = 0.45 * e;
      const g = P.r * 5.5;
      ctx.drawImage(glow, cl.x - g, cl.y - g, g * 2, g * 2);
      ctx.globalAlpha = 1;
      ctx.globalCompositeOperation = 'source-over';
      cl.cells.forEach((cell, k) => {
        ctx.beginPath();
        hexPath(ctx, cell.x, cell.y, P.r - 2.2);
        const hot = k === 0 ? 0.95 : 0.62 + (cell.n * 0.3);
        const grd = ctx.createRadialGradient(cell.x, cell.y - P.r * 0.3, 0, cell.x, cell.y, P.r);
        grd.addColorStop(0, `rgba(255,198,92,${hot * e})`);
        grd.addColorStop(1, `rgba(237,167,44,${hot * e * 0.9})`);
        ctx.fillStyle = grd;
        ctx.fill();
        ctx.strokeStyle = `rgba(255,198,92,${0.7 * e})`;
        ctx.lineWidth = 1;
        ctx.stroke();
      });
    }

    ctx.globalCompositeOperation = 'lighter';
    // A lone hub (fields with no clusters).
    if (s.hub && s.hub.solo) {
      const g = P.r * 2.6;
      ctx.drawImage(glow, s.hub.x - g, s.hub.y - g, g * 2, g * 2);
      ctx.beginPath();
      hexPath(ctx, s.hub.x, s.hub.y, P.r * 0.55);
      ctx.fillStyle = 'rgba(255,198,92,.95)';
      ctx.fill();
    } else if (s.hub && s.trails.length) {
      const g = P.r * 1.6;
      ctx.globalAlpha = 0.9;
      ctx.drawImage(glow, s.hub.x - g, s.hub.y - g, g * 2, g * 2);
      ctx.globalAlpha = 1;
    }

    // Twinkling cells (the ask band).
    for (const sp of s.sparks) {
      const v = 0.5 + 0.5 * Math.sin(s.t * sp.sp + sp.ph);
      const e = Math.pow(v, 1.6);
      const cell = sp.cell;
      if (!cell) continue;
      ctx.globalAlpha = 0.6 * e;
      const g = P.r * (sp.big ? 3.4 : 2.4);
      ctx.drawImage(glow, cell.x - g, cell.y - g, g * 2, g * 2);
      ctx.globalAlpha = 1;
      ctx.beginPath();
      hexPath(ctx, cell.x, cell.y, P.r - 2.2);
      ctx.fillStyle = `rgba(237,167,44,${0.15 + 0.8 * e})`;
      ctx.fill();
      for (const n of sp.near) {
        ctx.beginPath();
        hexPath(ctx, n.x, n.y, P.r - 2.2);
        ctx.fillStyle = `rgba(237,167,44,${0.08 + 0.5 * e})`;
        ctx.fill();
      }
    }
    ctx.globalCompositeOperation = 'source-over';
  }

  function frame(dt) {
    const s = state;
    s.t += dt;
    if (P.clusters && s.t >= s.next) {
      reroute();
      s.next = s.t + 6 + s.rr() * 4;
    }
    draw(dt);
  }

  function staticFrame() {
    reset();
    draw(0);
  }

  build();
  const loop = new Loop(canvas, frame, staticFrame);
  let lastW = W, lastH = H;
  new ResizeObserver(() => {
    const r = canvas.getBoundingClientRect();
    if (Math.round(r.width) === lastW && Math.round(r.height) === lastH) return;
    build();
    lastW = W; lastH = H;
    loop.redraw();
  }).observe(canvas);
  return loop;
}
