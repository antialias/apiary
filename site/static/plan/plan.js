// apiary.wtf/plan — rail, controls and demos. No libraries, no network.
import { week, pool, month, OVERLAPS, prng } from './models.js';
import { Loop, reducedMotion } from './loop.js';
import { hexField } from './hexfield.js';
import { market } from './market.js';

const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => [...r.querySelectorAll(s)];
const NS = 'http://www.w3.org/2000/svg';
function svgEl(tag, attrs = {}, parent) {
  const n = document.createElementNS(NS, tag);
  for (const k in attrs) n.setAttribute(k, attrs[k]);
  if (parent) parent.appendChild(n);
  return n;
}
function onWidth(el, fn) {
  let w = -1;
  new ResizeObserver(() => {
    const now = Math.round(el.getBoundingClientRect().width);
    if (now !== w) { w = now; fn(); }
  }).observe(el);
}

// ---------------------------------------------------------------------------
// Rail scroll-spy and the phone bar.
function rail() {
  const links = $$('.rail a[data-spy]');
  const secs = links.map((a) => document.getElementById(a.dataset.spy));
  const name = $('#phonebar-name');
  const bar = $('#phonebar-progress');
  let current = -1, queued = false;
  function update() {
    queued = false;
    const line = window.innerHeight * 0.35;
    let idx = 0;
    secs.forEach((s, i) => { if (s.getBoundingClientRect().top <= line) idx = i; });
    if (idx !== current) {
      current = idx;
      links.forEach((a, i) => {
        a.classList.toggle('is-current', i === idx);
        if (i === idx) a.setAttribute('aria-current', 'location'); else a.removeAttribute('aria-current');
      });
      name.textContent = links[idx].textContent;
    }
    const max = document.documentElement.scrollHeight - window.innerHeight;
    bar.style.setProperty('--progress', max > 0 ? Math.min(1, Math.max(0, window.scrollY / max)).toFixed(4) : 0);
  }
  const queue = () => { if (!queued) { queued = true; requestAnimationFrame(update); } };
  addEventListener('scroll', queue, { passive: true });
  addEventListener('resize', queue);
  update();
}

// ---------------------------------------------------------------------------
// Segmented controls (radiogroups with roving focus).
function segmented(group, onChange) {
  const btns = $$('[role="radio"]', group);
  const select = (b, focus) => {
    btns.forEach((x) => {
      const on = x === b;
      x.setAttribute('aria-checked', on);
      x.tabIndex = on ? 0 : -1;
    });
    if (focus) b.focus();
    onChange(b.dataset.value);
  };
  btns.forEach((b) => {
    b.tabIndex = b.getAttribute('aria-checked') === 'true' ? 0 : -1;
    b.addEventListener('click', () => select(b, false));
    b.addEventListener('keydown', (e) => {
      const i = btns.indexOf(b);
      let j = null;
      if (e.key === 'ArrowRight' || e.key === 'ArrowDown') j = (i + 1) % btns.length;
      if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') j = (i - 1 + btns.length) % btns.length;
      if (e.key === 'Home') j = 0;
      if (e.key === 'End') j = btns.length - 1;
      if (j !== null) { e.preventDefault(); select(btns[j], true); }
    });
  });
}

// ---------------------------------------------------------------------------
// Sliders: role="slider", pointer and keyboard. toFrac/fromFrac map value <-> position.
function slider(el, { min, max, value, toFrac, fromFrac, text, onChange }) {
  toFrac = toFrac || ((v) => (v - min) / (max - min));
  fromFrac = fromFrac || ((f) => Math.round(min + f * (max - min)));
  let v = value;
  const set = (nv, fire = true) => {
    nv = Math.max(min, Math.min(max, nv));
    const changed = nv !== v;
    v = nv;
    el.style.setProperty('--p', (toFrac(v) * 100).toFixed(3) + '%');
    el.setAttribute('aria-valuemin', min);
    el.setAttribute('aria-valuemax', max);
    el.setAttribute('aria-valuenow', v);
    el.setAttribute('aria-valuetext', text(v));
    if (fire && changed) onChange(v);
  };
  const fromEvent = (e) => {
    const r = el.querySelector('.track').getBoundingClientRect();
    return fromFrac(Math.max(0, Math.min(1, (e.clientX - r.left) / r.width)));
  };
  el.addEventListener('pointerdown', (e) => {
    el.setPointerCapture(e.pointerId);
    el.focus({ preventScroll: true });
    set(fromEvent(e));
    const move = (ev) => set(fromEvent(ev));
    const up = () => { el.removeEventListener('pointermove', move); el.removeEventListener('pointerup', up); el.removeEventListener('pointercancel', up); };
    el.addEventListener('pointermove', move);
    el.addEventListener('pointerup', up);
    el.addEventListener('pointercancel', up);
  });
  el.addEventListener('keydown', (e) => {
    const big = Math.max(1, Math.round((max - min) / 10));
    const k = { ArrowRight: 1, ArrowUp: 1, ArrowLeft: -1, ArrowDown: -1, PageUp: big, PageDown: -big }[e.key];
    if (k) { e.preventDefault(); set(v + k); }
    else if (e.key === 'Home') { e.preventDefault(); set(min); }
    else if (e.key === 'End') { e.preventDefault(); set(max); }
  });
  set(v, false);
  return { get value() { return v; }, set };
}

// ---------------------------------------------------------------------------
// 01 · One member's week.
const DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

function dayAxis(svg, W, y, h) {
  const dw = W / 7;
  DAYS.forEach((d, i) => {
    const t = svgEl('text', { x: dw * i + dw / 2, y: y + 20, 'text-anchor': 'middle', class: 'ax' }, svg);
    t.textContent = d;
    if (i) svgEl('line', { x1: dw * i, y1: y + 4, x2: dw * i, y2: y + 4 + h, stroke: 'rgba(185,172,141,.38)' }, svg);
  });
}

function weekDemo() {
  const plan = $('#week-plan');
  const gpu = $('#week-gpu');
  let kind = 'deadline';
  let animate = false;

  function drawPlan(data) {
    plan.innerHTML = '';
    const W = plan.clientWidth, H = plan.clientHeight;
    plan.setAttribute('viewBox', `0 0 ${W} ${H}`);
    const small = W < 480;
    const top = small ? 32 : 26, base = H - 34;
    const X = (h) => (h / 168) * W, Y = (v) => base - v * (base - top);
    // weekly limit: inline at the left on wide charts, above the line on narrow ones
    const lt = svgEl('text', { x: 0, y: small ? top - 10 : top + 5, class: 'ax ax-cream' }, plan);
    lt.textContent = 'Weekly limit';
    const ltw = small ? 0 : 124;
    svgEl('line', { x1: ltw, y1: top, x2: W, y2: top, stroke: 'rgba(243,232,207,.7)', 'stroke-dasharray': '6 5' }, plan);
    svgEl('line', { x1: 0, y1: base, x2: W, y2: base, stroke: 'rgba(185,172,141,.45)' }, plan);
    if (data.hitAt >= 0) {
      const x0 = X(data.hitAt);
      const defs = svgEl('defs', {}, plan);
      const lg = svgEl('linearGradient', { id: 'blk', x1: 0, y1: 0, x2: 0, y2: 1 }, defs);
      svgEl('stop', { offset: 0, 'stop-color': '#e0603a', 'stop-opacity': 0.3 }, lg);
      svgEl('stop', { offset: 1, 'stop-color': '#e0603a', 'stop-opacity': 0.16 }, lg);
      svgEl('rect', { x: x0, y: top, width: W - x0, height: base - top, fill: 'url(#blk)', class: 'blocked' }, plan);
      svgEl('line', { x1: x0, y1: top, x2: x0, y2: base, stroke: 'rgba(224,96,58,.6)' }, plan);
      const t = svgEl('text', { x: (x0 + W) / 2, y: (top + base) / 2 + 5, 'text-anchor': 'middle', class: 'ax', style: 'fill:#e0603a' }, plan);
      t.textContent = 'Out of capacity';
      const bb = t.getBBox();
      const plate = svgEl('rect', { x: bb.x - 10, y: bb.y - 5, width: bb.width + 20, height: bb.height + 10, rx: 4, fill: '#16130d', 'fill-opacity': 0.82 });
      plan.insertBefore(plate, t);
    }
    const pts = [[0, Y(0)], ...data.used.map((u) => [X(u.h), Y(u.v)])];
    const line = svgEl('polyline', { points: pts.map((p) => p.map((n) => n.toFixed(1)).join(',')).join(' '), fill: 'none', stroke: '#eda72c', 'stroke-width': 2.4, 'stroke-linejoin': 'round', 'stroke-linecap': 'round' }, plan);
    if (data.hitAt >= 0) svgEl('circle', { cx: X(data.hitAt), cy: top, r: 5.5, fill: '#eda72c' }, plan);
    dayAxis(plan, W, base, 22);
    if (animate && !reducedMotion.matches) {
      const len = line.getTotalLength();
      line.style.strokeDasharray = len;
      line.style.strokeDashoffset = len;
      line.getBoundingClientRect();
      line.style.transition = 'stroke-dashoffset 1.1s cubic-bezier(.3,.6,.2,1)';
      line.style.strokeDashoffset = 0;
      const blk = plan.querySelectorAll('.blocked, circle, text[style], rect[rx]');
      blk.forEach((n) => { n.style.opacity = 0; n.style.transition = 'opacity .45s .95s'; });
      requestAnimationFrame(() => blk.forEach((n) => { n.style.opacity = 1; }));
    }
  }

  function drawGpu(data) {
    gpu.innerHTML = '';
    const W = gpu.clientWidth;
    const gap = W < 480 ? 4 : 8;
    const bw = W / 7 - gap;
    const r = bw / (4.5 * Math.sqrt(3));
    const hw = Math.sqrt(3) * r;
    const bh = 9.5 * r;
    const H = Math.ceil(bh + 32);
    gpu.style.height = H + 'px';
    gpu.setAttribute('viewBox', `0 0 ${W} ${H}`);
    const defs = svgEl('defs', {}, gpu);
    const f = svgEl('filter', { id: 'gpu-glow', x: '-1', y: '-1', width: '3', height: '3' }, defs);
    svgEl('feGaussianBlur', { stdDeviation: r * 0.45, result: 'b' }, f);
    const m = svgEl('feMerge', {}, f);
    svgEl('feMergeNode', { in: 'b' }, m);
    svgEl('feMergeNode', { in: 'SourceGraphic' }, m);
    const hex = (cx, cy, rr) => {
      const p = [];
      for (let k = 0; k < 6; k++) {
        const a = Math.PI / 6 + (k * Math.PI) / 3;
        p.push((cx + rr * Math.cos(a)).toFixed(2) + ',' + (cy + rr * Math.sin(a)).toFixed(2));
      }
      return p.join(' ');
    };
    const lit = [];
    for (let d = 0; d < 7; d++) {
      const x0 = d * (bw + gap) + gap / 2;
      for (let h = 0; h < 24; h++) {
        const row = Math.floor(h / 4), col = h % 4;
        const cx = x0 + hw / 2 + col * hw + (row & 1 ? hw / 2 : 0);
        const cy = r + row * 1.5 * r;
        const on = data.gpu[d * 24 + h];
        const poly = svgEl('polygon', { points: hex(cx, cy, r - 1), fill: on ? '#eda72c' : 'rgba(243,232,207,.05)', stroke: on ? '#ffc65c' : 'rgba(237,167,44,.16)', 'stroke-width': 1 }, gpu);
        if (on) { poly.setAttribute('filter', 'url(#gpu-glow)'); lit.push(poly); }
      }
    }
    svgEl('line', { x1: 0, y1: bh + 4, x2: W, y2: bh + 4, stroke: 'rgba(185,172,141,.45)' }, gpu);
    dayAxis(gpu, W, bh + 4, 22);
    lit.forEach((p) => gpu.appendChild(p)); // glows on top
  }

  function render() {
    const data = week(kind);
    drawPlan(data);
    drawGpu(data);
    $('#fig-blocked').textContent = String(data.blocked);
    $('#fig-gpu').textContent = Math.round(data.gpuBusy * 100) + '%';
  }
  segmented($('[data-seg="week"]'), (v) => { kind = v; animate = true; render(); });
  onWidth(plan, () => { animate = false; render(); });
  // Draw the line in when the panel first comes into view.
  if (!reducedMotion.matches) {
    const io = new IntersectionObserver((es) => {
      if (es.some((e) => e.isIntersecting)) { animate = true; render(); io.disconnect(); }
    }, { threshold: 0.4 });
    io.observe($('#week'));
  }
}

// ---------------------------------------------------------------------------
// 02 · Routing: tools → proxy → brain → a ladder of destinations, tried in order.
const SCENARIOS = {
  normal: { live: 0, dest: 'Your own accounts', why: 'Why: your accounts have room, so they answer first.',
    rungs: [[42, 'serving'], [30, 'standby'], [10, 'standby']] },
  cap: { live: 1, dest: 'Pool GPUs · open models', why: "Why: your accounts are full, so the pool's GPUs answer instead.",
    rungs: [[97, 'full'], [38, 'serving'], [10, 'standby']] },
  offline: { live: 2, dest: 'Your own GPU · local model', why: 'Why: your accounts are full and the pool is offline, so your own GPU answers.',
    rungs: [[97, 'full'], [null, 'offline'], [24, 'serving']] },
};

function routeDemo() {
  const stage = $('#route-stage');
  const svg = $('#route-svg');
  const left = $('.route-left', stage);
  const rungs = $$('#rungs .rung');
  const zoneLabel = $('.ladder-zone span');
  let scenario = 'normal';
  let live = [];
  const dots = [];
  let t = 0;
  const shown = rungs.map(() => 0); // displayed percentages, for easing the numbers

  function setRungs(animate) {
    const sc = SCENARIOS[scenario];
    rungs.forEach((li, i) => {
      const [v, state] = sc.rungs[i];
      li.dataset.state = state;
      $$('.cs', li).forEach((c) => c.setAttribute('aria-hidden', String(!c.classList.contains('cs-' + state))));
      const fill = $('.rung-fill', li);
      const pct = $('.rung-pct', li);
      if (v === null) { pct.textContent = ''; return; }
      fill.style.setProperty('--v', v + '%');
      const from = shown[i];
      shown[i] = v;
      if (!animate || reducedMotion.matches || from === v) { pct.textContent = v + '%'; return; }
      const t0 = performance.now();
      const step = (now) => {
        if (shown[i] !== v) return;
        const k = Math.min(1, (now - t0) / 600);
        const e = 1 - Math.pow(1 - k, 3);
        pct.textContent = Math.round(from + (v - from) * e) + '%';
        if (k < 1) requestAnimationFrame(step);
      };
      requestAnimationFrame(step);
    });
    $('#route-dest').textContent = sc.dest;
    $('#route-why').textContent = sc.why;
  }

  function node(g, b, lines) {
    const n = svgEl('g', { class: 'node' }, g);
    svgEl('rect', { x: b.x, y: b.y, width: b.w, height: b.h, rx: 7 }, n);
    const tx = svgEl('text', { x: b.x + b.w / 2, y: b.y + b.h / 2 + 5, 'text-anchor': 'middle' }, n);
    const [main, small] = lines;
    const a = svgEl('tspan', {}, tx); a.textContent = main;
    if (small) { const s2 = svgEl('tspan', { class: 'sm' }, tx); s2.textContent = small; }
  }

  function build() {
    const W = stage.clientWidth;
    if (!W) return; // not laid out yet; onWidth builds once the stage has a width
    svg.innerHTML = '';
    // size the chain's boxes to their words
    const probe = svgEl('g', { class: 'node' }, svg);
    const sizes = [['Your tools'], ['Proxy', ' · your machine'], ['Brain · scheduler']].map((l) => {
      const tx = svgEl('text', {}, probe);
      const a = svgEl('tspan', {}, tx); a.textContent = l[0];
      if (l[1]) { const b = svgEl('tspan', { class: 'sm' }, tx); b.textContent = l[1]; }
      return Math.ceil(tx.getComputedTextLength()) + 26;
    });
    probe.remove();
    // side by side only when the chain fits beside the ladder with room to breathe
    const rem = parseFloat(getComputedStyle(document.documentElement).fontSize);
    const vertical = W < sizes[0] + sizes[1] + sizes[2] + 76 + 48 + 21.5 * rem;
    stage.classList.toggle('vertical', vertical);
    const nh = 50, gapV = 40;
    if (vertical) stage.style.setProperty('--left-h', (3 * nh + 2 * gapV + 26) + 'px');
    svg.innerHTML = '';
    const sr = stage.getBoundingClientRect();
    const H = sr.height;
    svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
    const sc = SCENARIOS[scenario];
    // rung anchor points (left edge middles), relative to the stage
    const R = rungs.map((li) => { const r = li.getBoundingClientRect(); return { x: r.left - sr.left, y: r.top - sr.top + r.height / 2 }; });
    const z = $('.rung-zone', rungs[0]).getBoundingClientRect();
    zoneLabel.style.setProperty('--zone', z.width + 'px');
    const defs = svgEl('defs', {}, svg);
    for (const [id, col] of [['ah-on', '#eda72c'], ['ah-off', 'rgba(185,172,141,.6)']]) {
      const m = svgEl('marker', { id, viewBox: '0 0 10 10', refX: 9, refY: 5, markerWidth: 6, markerHeight: 6, orient: 'auto-start-reverse' }, defs);
      svgEl('path', { d: 'M0 0 10 5 0 10z', fill: col }, m);
    }
    const wires = svgEl('g', {}, svg);
    const nodes = svgEl('g', {}, svg);
    const P = {};
    const segs = [];
    let branch;
    if (!vertical) {
      const lw = left.getBoundingClientRect().width;
      const [tw, pw, bw] = sizes;
      const g = Math.max(14, (lw - (tw + pw + bw) - 76) / 2);
      const cy = R[1].y;
      P.tools = { x: 1, y: cy - nh / 2, w: tw, h: nh };
      P.proxy = { x: 1 + tw + g, y: cy - nh / 2, w: pw, h: nh };
      P.brain = { x: P.proxy.x + pw + g, y: cy - nh / 2, w: bw, h: nh };
      segs.push(`M${P.tools.x + tw} ${cy} H${P.proxy.x - 2}`);
      segs.push(`M${P.proxy.x + pw} ${cy} H${P.brain.x - 2}`);
      branch = R.map((d) => {
        const sx = P.brain.x + bw, sy = cy, ex = d.x - 2, ey = d.y;
        const k = (ex - sx) * 0.5;
        return `M${sx} ${sy} C${sx + k} ${sy} ${ex - k} ${ey} ${ex} ${ey}`;
      });
    } else {
      const w = Math.min(W - 2, 236), cx = W / 2;
      P.tools = { x: cx - w / 2, y: 1, w, h: nh };
      P.proxy = { x: cx - w / 2, y: 1 + nh + gapV, w, h: nh };
      P.brain = { x: cx - w / 2, y: 1 + 2 * (nh + gapV), w, h: nh };
      segs.push(`M${cx} ${P.tools.y + nh} V${P.proxy.y - 2}`);
      segs.push(`M${cx} ${P.proxy.y + nh} V${P.brain.y - 2}`);
      const tx = 14;
      branch = R.map((d) => {
        const sx = cx, sy = P.brain.y + nh;
        return `M${sx} ${sy} V${sy + 10} Q${sx} ${sy + 18} ${sx - 8} ${sy + 18} H${tx + 10} Q${tx} ${sy + 18} ${tx} ${sy + 28} V${d.y - 10} Q${tx} ${d.y} ${tx + 10} ${d.y} H${d.x - 2}`;
      });
    }
    const offline = scenario === 'offline';
    const order = [0, 1, 2].filter((i) => i !== sc.live).concat(sc.live);
    const paths = {};
    order.forEach((i) => {
      const on = i === sc.live;
      const dead = offline && i === 1;
      paths[i] = svgEl('path', { d: branch[i], class: 'wire' + (on ? ' on' : '') + (dead ? ' dead' : ''), 'marker-end': `url(#${on ? 'ah-on' : 'ah-off'})` }, wires);
    });
    const s0 = svgEl('path', { d: segs[0], class: 'wire on', 'marker-end': 'url(#ah-on)' }, wires);
    const s1 = svgEl('path', { d: segs[1], class: 'wire on', 'marker-end': 'url(#ah-on)' }, wires);
    if (!vertical) {
      [0, 1, 2].filter((i) => i !== sc.live && !(offline && i === 1)).forEach((i) => {
        const p = paths[i], L = p.getTotalLength();
        [0.5, 0.56].forEach((f) => { const q = p.getPointAtLength(L * f); svgEl('circle', { cx: q.x, cy: q.y, r: 2.8, class: 'stub' }, wires); });
      });
    }
    live = [s0, s1, paths[sc.live]];
    node(nodes, P.tools, ['Your tools']);
    node(nodes, P.proxy, ['Proxy', ' · your machine']);
    node(nodes, P.brain, ['Brain · scheduler']);
    dots.length = 0;
    const dg = svgEl('g', {}, svg);
    live.forEach((p, si) => {
      const L = p.getTotalLength();
      const n = si === 2 ? 4 : 2;
      for (let k = 0; k < n; k++) dots.push({ p, L, off: k / n, c: svgEl('circle', { r: 3.4, class: 'dot' }, dg) });
    });
    place();
  }

  function place() {
    for (const d of dots) {
      const f = (d.off + t * (64 / d.L)) % 1;
      const q = d.p.getPointAtLength(f * d.L);
      d.c.setAttribute('cx', q.x.toFixed(1));
      d.c.setAttribute('cy', q.y.toFixed(1));
    }
  }

  const loop = new Loop(stage, (dt) => { t += dt; place(); }, () => { t = 0.4; place(); });
  segmented($('[data-seg="route"]'), (v) => {
    scenario = v;
    setRungs(true);
    build();
    loop.redraw();
  });
  setRungs(false);
  onWidth(stage, () => { build(); loop.redraw(); });
  // the rungs can change height without the stage changing width (fonts arriving, wrapping)
  let lh = -1;
  new ResizeObserver(() => {
    const h = Math.round($('.ladder', stage).getBoundingClientRect().height);
    if (h !== lh) { lh = h; build(); loop.redraw(); }
  }).observe($('.ladder', stage));
  if (document.fonts) document.fonts.ready.then(() => { build(); loop.redraw(); });
}

// ---------------------------------------------------------------------------
// 03 · Pooling.
const STOPS = [10, 25, 50, 100];
function membersToFrac(v) {
  for (let i = 0; i < 3; i++) if (v <= STOPS[i + 1]) return (i + (v - STOPS[i]) / (STOPS[i + 1] - STOPS[i])) / 3;
  return 1;
}
function fracToMembers(f) {
  const s = Math.min(2.9999, f * 3), i = Math.floor(s);
  return Math.round(STOPS[i] + (s - i) * (STOPS[i + 1] - STOPS[i]));
}

function poolDemo() {
  const svg = $('#pool-svg');
  let members = 40, overlap = 0;

  function draw() {
    const d = pool(members, overlap);
    $('#fig-alone').textContent = d.alone.toFixed(1) + '×';
    $('#fig-pooled').textContent = d.pooledHeadroom.toFixed(1) + '×';
    svg.innerHTML = '';
    const W = svg.clientWidth, H = svg.clientHeight;
    svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
    const small = W < 480;
    const L = small ? 58 : 74, R = 4, T = 10, B = 34;
    const pw = W - L - R, ph = H - T - B;
    // The axis tops out a little above 4×. Higher peaks run off the plot (clipped), not flattened.
    const X = (h) => L + (h / 167) * pw, Y = (v) => T + ph - (v / 4.3) * ph;
    const defs = svgEl('defs', {}, svg);
    const lg = svgEl('linearGradient', { id: 'pool-fill', x1: 0, y1: 0, x2: 0, y2: 1 }, defs);
    svgEl('stop', { offset: 0, 'stop-color': '#eda72c', 'stop-opacity': 0.42 }, lg);
    svgEl('stop', { offset: 1, 'stop-color': '#eda72c', 'stop-opacity': 0.02 }, lg);
    const clip = svgEl('clipPath', { id: 'pool-clip' }, defs);
    svgEl('rect', { x: L, y: T, width: pw, height: ph + 2 }, clip);
    for (let v = 0; v <= 4; v++) {
      if (v) svgEl('line', { x1: L, y1: Y(v), x2: L + pw, y2: Y(v), stroke: 'rgba(185,172,141,.3)', 'stroke-dasharray': '3 5' }, svg);
      const t = svgEl('text', { x: L - 12, y: Y(v) + 5, 'text-anchor': 'end', class: 'ax' }, svg);
      t.textContent = v + '×';
    }
    svgEl('line', { x1: L, y1: T, x2: L, y2: T + ph, stroke: 'rgba(185,172,141,.45)' }, svg);
    const yt = svgEl('text', { x: 0, y: 0, 'text-anchor': 'middle', class: 'ax', transform: `translate(${small ? 10 : 14} ${T + ph / 2}) rotate(-90)` }, svg);
    yt.textContent = 'Relative demand';
    DAYS.forEach((dn, i) => {
      const t = svgEl('text', { x: L + (i + 0.5) * (pw / 7), y: H - 8, 'text-anchor': 'middle', class: 'ax' }, svg);
      t.textContent = dn;
    });
    const line = (arr) => arr.map((v, h) => (h ? 'L' : 'M') + X(h).toFixed(1) + ' ' + Y(v).toFixed(1)).join('');
    const plot = svgEl('g', { 'clip-path': 'url(#pool-clip)' }, svg);
    svgEl('path', { d: line(d.pooled) + `L${X(167)} ${Y(0)}L${X(0)} ${Y(0)}Z`, fill: 'url(#pool-fill)' }, plot);
    svgEl('path', { d: line(d.one), fill: 'none', stroke: 'rgba(243,232,207,.82)', 'stroke-width': 1.3, 'stroke-linejoin': 'round' }, plot);
    svgEl('path', { d: line(d.pooled), fill: 'none', stroke: '#eda72c', 'stroke-width': 2.6, 'stroke-linejoin': 'round' }, plot);
  }

  slider($('#members'), {
    min: 10, max: 100, value: members, toFrac: membersToFrac, fromFrac: fracToMembers,
    text: (v) => String(v),
    onChange: (v) => { members = v; $('#members-out').textContent = v; draw(); },
  });
  // Overlap: a slider on wide screens, a segmented control on phones; both drive the same state.
  const seg = $('[data-seg="overlap"]');
  const segBtns = $$('[role="radio"]', seg);
  const showSeg = (v) => segBtns.forEach((b) => {
    const on = +b.dataset.value === v;
    b.setAttribute('aria-checked', on);
    b.tabIndex = on ? 0 : -1;
  });
  const ov = slider($('#overlap'), {
    min: 0, max: 2, value: overlap,
    text: (v) => OVERLAPS[v],
    onChange: (v) => { overlap = v; $('#overlap-out').textContent = OVERLAPS[v]; showSeg(v); draw(); },
  });
  segmented(seg, (v) => { if (+v !== overlap) ov.set(+v); });
  onWidth(svg, draw);
}

// ---------------------------------------------------------------------------
// 05 · Your month in the pool.
function monthDemo() {
  let hours = 4, days = 3;
  const render = () => {
    const m = month(hours, days);
    $('#fig-earned').textContent = m.earned;
    $('#fig-spent').textContent = m.spent;
    $('#fig-balance').textContent = m.balance > 0 ? '+' + m.balance : m.balance < 0 ? '−' + Math.abs(m.balance) : '0';
  };
  slider($('#hours'), { min: 0, max: 12, value: hours, text: (v) => `${v} h a day`, onChange: (v) => { hours = v; $('#hours-out').textContent = `${v} h a day`; render(); } });
  slider($('#days'), { min: 0, max: 10, value: days, text: (v) => String(v), onChange: (v) => { days = v; $('#days-out').textContent = v; render(); } });
  render();
}

// ---------------------------------------------------------------------------
// 04 · The engine is real: lines typed in one after another, on a loop.
const TERM = [
  [['k', 'class=frontier'], ['', ' → your account · '], ['ok', '200']],
  [['k', 'class=fast'], ['', ' → your account · '], ['ok', '200']],
  [['k', 'plan at limit'], ['', ' → pool GPU · qwen3-coder-30b · '], ['ok', '200']],
  [['k', 'pool GPU busy'], ['', " → another member's GPU · "], ['ok', '200']],
  [['k', 'class=local'], ['', ' → your GPU · 127.0.0.1 · '], ['ok', '200']],
];
function termDemo() {
  const host = $('#term-lines');
  const rows = TERM.map(() => { const p = document.createElement('p'); p.className = 'term-line'; host.appendChild(p); return p; });
  const lens = TERM.map((l) => l.reduce((s, [, t]) => s + t.length, 0));
  const CPS = 34, GAP = 0.55, HOLD = 3.6;
  const total = lens.reduce((s, n) => s + n / CPS + GAP, 0) + HOLD;
  let t = 0, lastKey = '';
  function show(at) {
    let rem = at % total;
    const counts = lens.map((n) => {
      const need = n / CPS;
      if (rem >= need + GAP) { rem -= need + GAP; return n; }
      const c = Math.max(0, Math.min(n, Math.floor(rem * CPS)));
      rem = -1e9;
      return c;
    });
    const typing = counts.findIndex((c, i) => c < lens[i]);
    const key = counts.join(',') + '|' + typing;
    if (key === lastKey) return;
    lastKey = key;
    rows.forEach((row, i) => {
      row.textContent = '';
      let left = counts[i];
      for (const [cls, txt] of TERM[i]) {
        if (left <= 0) break;
        const s = document.createElement('span');
        if (cls) s.className = cls;
        s.textContent = txt.slice(0, left);
        left -= txt.length;
        row.appendChild(s);
      }
      if (i === typing || (typing === -1 && i === rows.length - 1)) {
        const c = document.createElement('span');
        c.className = 'cur';
        row.appendChild(c);
      }
    });
  }
  function full() {
    lastKey = '';
    rows.forEach((row, i) => {
      row.textContent = '';
      for (const [cls, txt] of TERM[i]) { const s = document.createElement('span'); if (cls) s.className = cls; s.textContent = txt; row.appendChild(s); }
    });
  }
  new Loop(host, (dt) => { t += dt; show(t); }, full);
}

// ---------------------------------------------------------------------------
// 07 · Roadmap: tabs with arrow keys; the detail shows beneath.
function stepArt(svg, i) {
  svg.innerHTML = '';
  const r = prng(301 + i * 17);
  const big = '120,6 232,68 232,192 120,254 8,192 8,68';
  const defs = svgEl('defs', {}, svg);
  const cp = svgEl('clipPath', { id: 'art-clip' }, defs);
  svgEl('polygon', { points: big }, cp);
  const rg = svgEl('radialGradient', { id: 'art-glow' }, defs);
  svgEl('stop', { offset: 0, 'stop-color': '#ffc65c', 'stop-opacity': 0.9 }, rg);
  svgEl('stop', { offset: 0.35, 'stop-color': '#eda72c', 'stop-opacity': 0.35 }, rg);
  svgEl('stop', { offset: 1, 'stop-color': '#eda72c', 'stop-opacity': 0 }, rg);
  svgEl('polygon', { points: big, fill: 'rgba(30,25,16,.9)' }, svg);
  const g = svgEl('g', { 'clip-path': 'url(#art-clip)' }, svg);
  const rr = 11, w = Math.sqrt(3) * rr;
  const cells = [];
  for (let row = 0; row < 18; row++) for (let col = 0; col < 12; col++) {
    const cx = col * w + (row & 1 ? w / 2 : 0) - 4, cy = row * 1.5 * rr;
    cells.push([cx, cy]);
    const p = [];
    for (let k = 0; k < 6; k++) { const a = Math.PI / 6 + (k * Math.PI) / 3; p.push((cx + (rr - 1.2) * Math.cos(a)).toFixed(1) + ',' + (cy + (rr - 1.2) * Math.sin(a)).toFixed(1)); }
    svgEl('polygon', { points: p.join(' '), fill: 'rgba(243,232,207,.035)', stroke: 'rgba(237,167,44,.16)', 'stroke-width': 0.8 }, g);
  }
  const inside = cells.filter(([x, y]) => Math.hypot(x - 120, y - 130) < 84);
  const pick = () => inside[Math.floor(r() * inside.length)];
  const hub = [120 + (r() - 0.5) * 30, 130 + (r() - 0.5) * 30];
  const lit = [hub, pick(), pick(), pick()];
  for (let k = 0; k < 16; k++) {
    const a = pick(), b = r() < 0.5 ? hub : lit[1 + Math.floor(r() * 3)];
    svgEl('path', { d: `M${a[0]} ${a[1]} Q${(a[0] + b[0]) / 2 + (r() - 0.5) * 30} ${(a[1] + b[1]) / 2 + (r() - 0.5) * 30} ${b[0]} ${b[1]}`, fill: 'none', stroke: 'rgba(237,167,44,.35)', 'stroke-width': 0.9 }, g);
  }
  lit.forEach(([x, y], k) => {
    svgEl('circle', { cx: x, cy: y, r: k ? 20 : 30, fill: 'url(#art-glow)' }, g);
    svgEl('circle', { cx: x, cy: y, r: k ? 3.2 : 4.5, fill: '#ffc65c' }, g);
  });
  svgEl('polygon', { points: big, fill: 'none', stroke: 'rgba(237,167,44,.6)', 'stroke-width': 1.5 }, svg);
}

function roadmap() {
  const tabs = $$('#steps [role="tab"]');
  const panel = $('#step-panel');
  const title = $('#step-title');
  const detail = $('#step-detail');
  const copy = $('.step-copy');
  const details = $$('p', $('#step-details').content);
  const art = $('#step-art');
  const steps = $('#steps');
  let cur = 0;
  function select(i, focus) {
    if (i === cur && !focus) return;
    cur = i;
    tabs.forEach((t, k) => {
      t.setAttribute('aria-selected', k === i);
      t.tabIndex = k === i ? 0 : -1;
    });
    panel.setAttribute('aria-labelledby', tabs[i].id);
    const apply = () => {
      title.textContent = tabs[i].querySelector('.step-label').textContent;
      detail.textContent = details[i].textContent;
      stepArt(art, i);
      copy.classList.remove('swap');
    };
    if (reducedMotion.matches) apply();
    else { copy.classList.add('swap'); setTimeout(apply, 160); }
    if (focus) tabs[i].focus({ preventScroll: true });
    // keep the chosen step in view when the row scrolls sideways (phones)
    if (steps.scrollWidth > steps.clientWidth) {
      const t = tabs[i];
      const left = t.offsetLeft - (steps.clientWidth - t.offsetWidth) / 2;
      steps.scrollTo({ left, behavior: reducedMotion.matches ? 'auto' : 'smooth' });
    }
  }
  tabs.forEach((t, i) => {
    t.addEventListener('click', () => select(i, false));
    t.addEventListener('keydown', (e) => {
      let j = null;
      if (e.key === 'ArrowRight' || e.key === 'ArrowDown') j = (i + 1) % tabs.length;
      if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') j = (i - 1 + tabs.length) % tabs.length;
      if (e.key === 'Home') j = 0;
      if (e.key === 'End') j = tabs.length - 1;
      if (j !== null) { e.preventDefault(); select(j, true); }
    });
  });
  stepArt(art, 0);
}

// ---------------------------------------------------------------------------
// 08 · Risks: rows expand and collapse; the first is open.
function risks() {
  $$('.risk').forEach((row) => {
    const btn = $('button[aria-expanded]', row);
    const why = document.getElementById(btn.getAttribute('aria-controls'));
    const set = (open) => {
      btn.setAttribute('aria-expanded', open);
      why.hidden = !open;
      row.classList.toggle('open', open);
    };
    set(btn.getAttribute('aria-expanded') === 'true');
    btn.addEventListener('click', (e) => { e.stopPropagation(); set(btn.getAttribute('aria-expanded') !== 'true'); });
    row.addEventListener('click', (e) => {
      if (e.target.closest('a, .why')) return;
      if (window.getSelection && String(window.getSelection()).length) return;
      set(btn.getAttribute('aria-expanded') !== 'true');
    });
  });
}

// ---------------------------------------------------------------------------
rail();
$$('canvas[data-field]').forEach((c) => hexField(c));
weekDemo();
routeDemo();
poolDemo();
termDemo();
monthDemo();
market();
roadmap();
risks();
