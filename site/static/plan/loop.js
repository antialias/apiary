// Animation loops that run only when they can be seen: on screen, tab visible,
// and motion not reduced. With reduced motion each loop draws one static frame.
export const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)');
const loops = new Set();

const io = new IntersectionObserver((entries) => {
  for (const e of entries) {
    const loop = e.target.__loop;
    if (loop) { loop.visible = e.isIntersecting; loop.update(); }
  }
}, { rootMargin: '80px 0px' });

document.addEventListener('visibilitychange', () => loops.forEach((l) => l.update()));
reducedMotion.addEventListener('change', () => loops.forEach((l) => { l.update(); if (reducedMotion.matches) l.staticFrame(); }));

export class Loop {
  // frame(dt) advances and draws; staticFrame() draws the reduced-motion frame.
  constructor(el, frame, staticFrame) {
    this.el = el;
    this.frame = frame;
    this.staticFrame = staticFrame || (() => frame(0));
    this.visible = false;
    this.raf = 0;
    this.last = 0;
    el.__loop = this;
    loops.add(this);
    io.observe(el);
    if (reducedMotion.matches) this.staticFrame();
    else this.frame(0);
  }
  get running() { return this.raf !== 0; }
  update() {
    const go = this.visible && !document.hidden && !reducedMotion.matches;
    if (go && !this.raf) {
      this.last = performance.now();
      const tick = (now) => {
        const dt = Math.min(0.1, (now - this.last) / 1000);
        this.last = now;
        this.frame(dt);
        this.raf = requestAnimationFrame(tick);
      };
      this.raf = requestAnimationFrame(tick);
    } else if (!go && this.raf) {
      cancelAnimationFrame(this.raf);
      this.raf = 0;
    }
  }
  // Redraw after a state change, whether or not the loop is running.
  redraw() {
    if (reducedMotion.matches) this.staticFrame();
    else if (!this.raf) this.frame(0);
  }
}
