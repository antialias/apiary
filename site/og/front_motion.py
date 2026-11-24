"""The front door's clip, og/motion/front.mp4, drawn from og/art/front.jpg itself rather than by a
video model (which kept moving the honey and the plates the words sit on).

It tells what Apiary does, in a five-second loop: Claude Code's jar drains, its honey running as
light down its pipe into the hive, while Kimi's jar resets and fills; before Claude Code runs dry
the hive switches to Kimi and draws on it, and Claude Code resets. It ends where it began.

A jar's level moves by moving its own honey surface and filling in around it: a draining jar gets
the glass of the near-empty local jar, a filling jar the honey of the full Claude Code jar. The
jars are the same shape, so with the offsets below the walls and the label plates line up to a
pixel. The plates' dark middles never change, so the card's labels stay readable on them.

Run: uv run --with numpy --with pillow site/og/front_motion.py   (needs ffmpeg)
Then: site/og/animate.sh front
"""
import os
import subprocess
import sys

import numpy as np
from PIL import Image, ImageFilter

here = os.path.dirname(os.path.abspath(__file__))
art = np.asarray(Image.open(os.path.join(here, "art", "front.jpg")).convert("RGB")).astype(np.float32)
H, W, _ = art.shape
FPS, T = 30, 5.0

# Measured on art/front.jpg (1920x1088). Jars: walls x0..x1, honey surface y, plate box.
J1 = dict(x0=1124, x1=1276, s0=202, plate=(1134, 1265, 267, 310))   # Claude Code, nearly full
J3 = dict(x0=1493, x1=1643, s0=336, plate=(1506, 1631, 274, 317))   # Kimi, low
J1_LOW, J3_HIGH = 336, 209
OUTLET = {"J1": (1198, 422), "J3": (1568, 422)}                     # where each jar's pipe starts
HUB = (1471, 546)                                                   # the pipe meets the hexagon
PIPES = (1140, 1800, 414, 950)                                      # x0, x1, y0, y1


def shifted(dx, dy):
    """The art moved so that pixel (x, y) shows what was at (x + dx, y + dy)."""
    return np.roll(art, (-dy, -dx), axis=(0, 1))


def ramp(a, b, y):
    """0 at a, 1 at b (either way round), linear between."""
    return np.clip((y - a) / (b - a), 0, 1)


ys = np.arange(H, dtype=np.float32)[:, None]
xs = np.arange(W, dtype=np.float32)[None, :]


# Jar 1 drains: its glass below the original surface comes from jar 4 (local), feathered in.
j1_glass = art + (shifted(553, 10) - art) * ramp(J1["s0"] - 10, J1["s0"] - 3, ys)[..., None]
j1_honey = art
# Jar 3 fills: its honey above the original surface comes from jar 1, feathered out below it.
j3_glass = art
j3_honey = shifted(-368, -7) + (art - shifted(-368, -7)) * ramp(J3["s0"] + 12, J3["s0"] + 26, ys)[..., None]


def jar_at(frame, j, glass, honey, s):
    """Draw jar j into frame with its honey surface at row s."""
    x0, x1 = j["x0"] - 6, j["x1"] + 7
    y0, y1 = min(s, j["s0"]) - 14, max(s, j["s0"]) + 18
    sl = (slice(y0, y1), slice(x0, x1))
    y = ys[y0:y1]
    band = np.roll(art, int(s - j["s0"]), axis=0)[sl]                 # the jar's own surface, moved
    w_top = ramp(s - 11, s - 8, y)[..., None]                         # glass -> surface
    w_bot = ramp(s + 9, s + 12, y)[..., None]                         # surface -> honey
    out = glass[sl] * (1 - w_top) + band * w_top
    out = out * (1 - w_bot) + honey[sl] * w_bot
    px0, px1, py0, py1 = j["plate"]                                   # the plate: no surface across it
    inplate = ((xs[:, x0:x1] >= px0 - 2) & (xs[:, x0:x1] <= px1 + 2) & (y >= py0 - 2) & (y <= py1 + 2))
    plate = np.where((y < s)[..., None], glass[sl], honey[sl])
    out = np.where(inplate[..., None], plate, out)
    inner = ((xs[:, x0:x1] >= px0 + 3) & (xs[:, x0:x1] <= px1 - 3) & (y >= py0 + 3) & (y <= py1 - 3))
    out = np.where(inner[..., None], art[sl], out)                    # its dark middle never changes
    edge = np.minimum(ramp(x0, x0 + 6, xs[:, x0:x1]), ramp(x1, x1 - 6, xs[:, x0:x1]))[..., None]
    frame[sl] = art[sl] + (out - art[sl]) * edge


# Pipes: the glowing pixels below the jars, and how far along each jar's route to the hive they are.
px0, px1, py0, py1 = PIPES
lum = art[py0:py1, px0:px1].mean(axis=2)
pipe = lum > 60


def distance_from(seed_xy):
    """Steps along the pipes from a point (8-connected flood)."""
    sx, sy = seed_xy
    d = np.full(pipe.shape, np.inf, np.float32)
    front = np.zeros_like(pipe)
    front[sy - py0 - 4:sy - py0 + 5, sx - px0 - 12:sx - px0 + 13] = True
    front &= pipe
    seen = front.copy()
    step = 0
    while front.any():
        d[front] = step
        grow = front.copy()
        grow[1:] |= front[:-1]; grow[:-1] |= front[1:]
        grow[:, 1:] |= grow[:, :-1].copy(); grow[:, :-1] |= grow[:, 1:].copy()
        front = grow & pipe & ~seen
        seen |= front
        step += 1
    return d


d_hub = distance_from(HUB)
routes = {}
for name, xy in OUTLET.items():
    d = distance_from(xy)
    to_hub = d[HUB[1] - py0, HUB[0] - px0 - 8:HUB[0] - px0 + 9].min()
    rows = np.arange(py0, py1)[:, None]
    on = np.isfinite(d) & ((d + d_hub <= to_hub + 30) | (rows >= HUB[1] + 2))
    routes[name] = (np.where(on, d, np.nan), to_hub)
    if not np.isfinite(to_hub):
        sys.exit(f"front_motion.py: no pipe from {name}'s outlet to the hive; check OUTLET and HUB")

SPEED, SIGMA, EVERY = 430.0, 11.0, 0.2
FLOWS = [("J1", 0.0, 2.5), ("J3", 2.5, 4.4)]                         # who the hive draws on, when
pulses = [(name, t) for name, a, b in FLOWS for t in np.arange(a, b - 1e-6, EVERY)]


def light(t):
    """The light running down the pipes at time t (looping: last cycle's pulses still travel)."""
    out = np.zeros(pipe.shape, np.float32)
    for name, t0 in pulses:
        f, to_hub = routes[name]
        for start in (t0, t0 - T):
            pos = SPEED * (t - start)
            if pos < 0 or pos > to_hub + 900:
                continue
            fade = np.where(f > to_hub, np.exp(-(f - to_hub) / 380), 1.0) * np.clip(f / 25, 0, 1)
            out += np.nan_to_num(np.exp(-((f - pos) ** 2) / (2 * SIGMA ** 2)) * fade)
    return np.clip(out, 0, 1.4)


def smooth(u):
    u = np.clip(u, 0, 1)
    return u * u * (3 - 2 * u)


def out_cubic(u):
    return 1 - (1 - np.clip(u, 0, 1)) ** 3


def between(t, a, b):
    return (t - a) / (b - a)


def levels(t):
    if t < 2.5:   s1 = J1["s0"] + (J1_LOW - J1["s0"]) * between(t, 0, 2.5)
    elif t < 3.1: s1 = J1_LOW
    else:         s1 = J1_LOW - (J1_LOW - J1["s0"]) * out_cubic(between(t, 3.1, 3.7))
    if t < 0.6:   s3 = J3["s0"]
    elif t < 2.5: s3 = J3["s0"] - (J3["s0"] - J3_HIGH) * out_cubic(between(t, 0.6, 1.2))
    else:         s3 = J3_HIGH + (J3["s0"] - J3_HIGH) * min(between(t, 2.5, 4.4), 1)
    return int(round(s1)), int(round(s3))


def flash(t, a):
    """A reset's warm glow: up as the honey rises, then fading."""
    u = between(t, a, a + 1.1)
    return float(np.sin(np.pi * smooth(u)) ** 2) if 0 <= u <= 1 else 0.0


def glow_mask(j):
    m = Image.new("L", (W, H), 0)
    m.paste(255, (j["x0"], 150, j["x1"], 420))
    return np.asarray(m.filter(ImageFilter.GaussianBlur(28))).astype(np.float32)[..., None] / 255


GLOW = {"J1": glow_mask(J1), "J3": glow_mask(J3)}
GOLD = np.array([255, 214, 140], np.float32)

out = os.path.join(here, "motion", "front.mp4")
enc = subprocess.Popen(["ffmpeg", "-v", "error", "-y", "-f", "rawvideo", "-pix_fmt", "rgb24",
                        "-s", f"{W}x{H}", "-r", str(FPS), "-i", "-", "-c:v", "libx264", "-crf", "16",
                        "-pix_fmt", "yuv420p", "-movflags", "+faststart", out], stdin=subprocess.PIPE)
for i in range(int(FPS * T)):
    t = i / FPS
    frame = art.copy()
    s1, s3 = levels(t)
    jar_at(frame, J1, j1_glass, j1_honey, s1)
    jar_at(frame, J3, j3_glass, j3_honey, s3)
    for name, a in (("J3", 0.6), ("J1", 3.1)):
        g = flash(t, a)
        if g:
            frame = frame * (1 + 0.35 * g * GLOW[name]) + GOLD * 0.10 * g * GLOW[name]
    lit = light(t)
    core = lit * pipe
    bloom = np.asarray(Image.fromarray(np.uint8(np.clip(core, 0, 1) * 255)).filter(ImageFilter.GaussianBlur(7)),
                       np.float32) / 255
    region = frame[py0:py1, px0:px1]
    region += GOLD * (0.85 * core + 0.9 * bloom)[..., None]
    enc.stdin.write(np.uint8(np.clip(frame, 0, 255)).tobytes())
enc.stdin.close()
sys.exit(enc.wait())
