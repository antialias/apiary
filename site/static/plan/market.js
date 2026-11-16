// 06 · Market: the open-weight share line and the log-scale price bars, with
// hover or focus details that name each figure's source.
import { reducedMotion } from './loop.js';

const NS = 'http://www.w3.org/2000/svg';
const el = (tag, attrs = {}, parent) => {
  const n = document.createElementNS(NS, tag);
  for (const k in attrs) n.setAttribute(k, attrs[k]);
  if (parent) parent.appendChild(n);
  return n;
};

function sourceTitle(n) {
  const a = document.querySelector(`#src-${n} a`);
  return a ? a.textContent : '';
}

// A detail box inside a chart panel, shown on hover or focus.
function tipFor(panel) {
  const tip = document.createElement('div');
  tip.className = 'tip';
  tip.setAttribute('role', 'tooltip');
  tip.id = panel.id + '-tip';
  panel.appendChild(tip);
  let pinned = null;
  return {
    id: tip.id,
    show(target, label, srcs, anchor) {
      tip.innerHTML = '';
      const b = document.createElement('b');
      b.textContent = label;
      tip.appendChild(b);
      srcs.forEach((n) => {
        const line = document.createElement('span');
        line.style.display = 'block';
        const sup = document.createElement('sup');
        sup.textContent = n;
        line.append(sup, ' ' + sourceTitle(n));
        tip.appendChild(line);
      });
      const pr = panel.getBoundingClientRect();
      const ar = anchor.getBoundingClientRect();
      tip.style.left = '0px';
      tip.style.top = '0px';
      tip.classList.add('show');
      const tw = tip.offsetWidth, th = tip.offsetHeight;
      let x = ar.left - pr.left + ar.width / 2 - tw / 2;
      x = Math.max(8, Math.min(pr.width - tw - 8, x));
      let y = ar.top - pr.top - th - 10;
      if (y < 8) y = ar.bottom - pr.top + 10;
      tip.style.left = x + 'px';
      tip.style.top = y + 'px';
      target.setAttribute('aria-describedby', tip.id);
    },
    hide(force) {
      if (pinned && !force) return;
      tip.classList.remove('show');
    },
    pin(t) { pinned = t; },
    pinned() { return pinned; },
  };
}

function wire(panel, tip, target, label, srcs, anchor) {
  const show = () => tip.show(target, label, srcs, anchor || target);
  target.addEventListener('mouseenter', show);
  target.addEventListener('focus', show);
  target.addEventListener('mouseleave', () => tip.hide());
  target.addEventListener('blur', () => { if (tip.pinned() === target) tip.pin(null); tip.hide(true); });
  target.addEventListener('click', () => {
    if (tip.pinned() === target) { tip.pin(null); tip.hide(true); return; }
    tip.pin(target);
    show();
  });
  target.addEventListener('keydown', (e) => { if (e.key === 'Escape') { tip.pin(null); tip.hide(true); } });
}

// ---------------------------------------------------------------------------
const SHARE = [
  { when: 'Late 2025 ·', what: 'about a third', v: 33, x: 0.06, src: [7], place: 'below-right' },
  { when: 'Feb 2026 ·', what: '50%', v: 50, x: 0.35, src: [3], place: 'below' },
  { when: 'Jul 2026 ·', what: '79%', v: 79, x: 0.67, src: [3], place: 'above' },
  { when: 'Sep 2026 ·', what: '72%', v: 72, x: 0.93, src: [3], place: 'below' },
];

function drawShare(host, tip) {
  host.innerHTML = '';
  const W = host.clientWidth, H = host.clientHeight;
  const small = W < 420;
  const L = small ? 40 : 52, R = small ? 14 : 30, T = 12, B = 14;
  const pw = W - L - R, ph = H - T - B;
  const X = (f) => L + f * pw, Y = (v) => T + (1 - v / 100) * ph;
  const svg = el('svg', { width: W, height: H, viewBox: `0 0 ${W} ${H}` }, host);
  const axis = 'rgba(185,172,141,.5)';
  el('line', { x1: L, y1: T, x2: L, y2: T + ph, stroke: axis }, svg);
  el('line', { x1: L, y1: T + ph, x2: L + pw, y2: T + ph, stroke: axis }, svg);
  el('line', { x1: L, y1: Y(100), x2: L + pw, y2: Y(100), stroke: 'rgba(185,172,141,.32)', 'stroke-dasharray': '4 5' }, svg);
  el('line', { x1: L, y1: Y(50), x2: L + pw, y2: Y(50), stroke: 'rgba(185,172,141,.6)', 'stroke-dasharray': '6 5' }, svg);
  for (const v of [100, 50, 0]) {
    const t = el('text', { x: L - 10, y: Y(v) + 5, 'text-anchor': 'end', class: 'ax' }, svg);
    t.textContent = v + '%';
  }
  const pts = SHARE.map((d) => [X(d.x), Y(d.v)]);
  el('polyline', { points: pts.map((p) => p.join(',')).join(' '), fill: 'none', stroke: '#eda72c', 'stroke-width': 2.5, 'stroke-linejoin': 'round' }, svg);
  SHARE.forEach((d, i) => {
    const [x, y] = pts[i];
    const g = el('g', { class: 'pt', tabindex: 0, role: 'button', 'aria-label': `${d.when} ${d.what}` }, svg);
    el('circle', { cx: x, cy: y, r: 13, class: 'halo' }, g);
    el('circle', { cx: x, cy: y, r: 6.5, class: 'core' }, g);
    let tx = x, ty, anchor = 'middle';
    if (d.place === 'above') ty = y - 44;
    else ty = y + 30;
    if (d.place === 'below-right') { tx = x - 16; anchor = 'start'; }
    if (small && i === SHARE.length - 1) { anchor = 'end'; tx = x + 8; }
    const t = el('text', { x: tx, y: ty, 'text-anchor': anchor, class: 'ax ax-cream' }, svg);
    const a = el('tspan', { x: tx }, t); a.textContent = d.when;
    const b = el('tspan', { x: tx, dy: '1.45em' }, t); b.textContent = d.what;
    if (small) t.setAttribute('style', 'font-size:12.5px');
    wire(host.parentElement, tip, g, `${d.when} ${d.what}`, d.src, g.querySelector('.core'));
  });
}

// ---------------------------------------------------------------------------
const PRICES = [
  { name: 'gpt-6-astra', p: 50, val: '$50', src: [9] },
  { name: 'Claude Opus 5.5', p: 20, val: '$20', src: [8] },
  { name: 'Claude Sonnet 5.5', p: 10, val: '$10', src: [8] },
  { name: 'Claude Haiku 5.5', p: 0.5, val: '$0.50', src: [8] },
  { name: 'gpt-6-luna', p: 0.5, val: '$0.50', src: [9] },
  { name: '30B open model on a rented RTX 4090', p: 0.05, val: '≈ $0.05', src: [10], open: true },
];
// A true log scale from $0.01 to $100.
export const logPos = (p) => (Math.log10(p) + 2) / 4;

function buildBars(host, tip) {
  PRICES.forEach((d) => {
    const row = document.createElement('div');
    row.className = 'bar-row';
    const label = document.createElement('span');
    label.className = 'bar-label';
    label.textContent = d.name;
    const track = document.createElement('button');
    track.type = 'button';
    track.className = 'bar-track';
    track.setAttribute('aria-label', `${d.name} ${d.val}`);
    track.style.setProperty('--w', (logPos(d.p) * 100).toFixed(2) + '%');
    const bar = document.createElement('span');
    bar.className = 'bar' + (d.open ? ' open-model' : '');
    const val = document.createElement('span');
    val.className = 'bar-val';
    val.textContent = d.val;
    if (d.open) val.style.color = 'var(--honey)';
    track.append(bar, val);
    row.append(label, track);
    host.appendChild(row);
    label.addEventListener('click', () => track.click());
    label.addEventListener('mouseenter', () => track.dispatchEvent(new Event('mouseenter')));
    label.addEventListener('mouseleave', () => track.dispatchEvent(new Event('mouseleave')));
    wire(host.parentElement, tip, track, `${d.name} ${d.val}`, d.src, bar);
  });
  const axis = document.createElement('div');
  axis.className = 'bar-axis';
  axis.setAttribute('aria-hidden', 'true');
  ['$0.01', '$0.1', '$1', '$10', '$100'].forEach((t, i) => {
    const s = document.createElement('span');
    s.style.left = i * 25 + '%';
    s.textContent = t;
    axis.appendChild(s);
  });
  host.appendChild(axis);
  const unit = document.createElement('div');
  unit.className = 'bar-unit';
  unit.setAttribute('aria-hidden', 'true');
  unit.textContent = 'USD (log scale)';
  host.appendChild(unit);
  const grow = () => host.classList.add('grown');
  if (reducedMotion.matches) grow();
  else {
    const io = new IntersectionObserver((es) => {
      if (es.some((e) => e.isIntersecting)) { grow(); io.disconnect(); }
    }, { threshold: 0.35 });
    io.observe(host);
  }
}

export function market() {
  const sharePanel = document.getElementById('share');
  const pricePanel = document.getElementById('price');
  const plot = document.getElementById('share-plot');
  const shareTip = tipFor(sharePanel);
  const priceTip = tipFor(pricePanel);
  let w = 0;
  new ResizeObserver(() => {
    if (plot.clientWidth === w) return;
    w = plot.clientWidth;
    shareTip.hide(true);
    drawShare(plot, shareTip);
  }).observe(plot);
  buildBars(document.getElementById('bars'), priceTip);
}
