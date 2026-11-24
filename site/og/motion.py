#!/usr/bin/env python3
"""Makes a page's motion clip, og/motion/<page>.mp4, for its animated card (animate.sh).

Five seconds of image-to-video through Runway's API (Kling 3.0 Pro, 1920:1080), with og/art/<page>.jpg
as both the first and the last frame, so the clip stays anchored to the art and ends where it began.
The art carries no words (card.html lays them over it), and each prompt keeps the camera still and
the motion small, so the words stay readable over it. Asked for drops running down the pipes, the
model spilled honey out of them; keep the motion to light, bubbles and glints.

Needs RUNWAY_API_KEY; RUNWAY_BASE overrides the API's address. An existing clip is kept: delete it
to make a new one. The clips for home, plan, signin and invite came before this script.
Run: python3 -I site/og/motion.py <page>
"""
import base64, json, os, sys, time, urllib.error, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.environ.get("RUNWAY_BASE", "https://api.dev.runwayml.com/v1").rstrip("/")
STILL = ("Static locked-off camera; the framing never moves. Everything stays exactly as in the image, "
         "in the same place, size and brightness; only small details move. ")
PROMPTS = {
    "front": STILL + "The four glass jars, the dark blank label plate on each jar, the honey level in each "
    "jar, the pipes and the hexagon all stay exactly as they are, and the centre of the hexagon stays dark "
    "and empty. Tiny bubbles drift slowly up through the honey, a faint glint slides across the glass, and "
    "a soft warm glow pulses gently along the pipes and the hexagon's rim. The dark left side stays dark. "
    "Calm, subtle, quiet motion.",
}


def call(path, body=None):
    req = urllib.request.Request(BASE + path, method="POST" if body else "GET",
                                 data=json.dumps(body).encode() if body else None,
                                 headers={"Authorization": "Bearer " + os.environ["RUNWAY_API_KEY"],
                                          "Content-Type": "application/json",
                                          "X-Runway-Version": "2024-11-06"})
    with urllib.request.urlopen(req, timeout=180) as r:
        return json.loads(r.read() or b"{}")


def main(page):
    out = os.path.join(HERE, "motion", page + ".mp4")
    if os.path.exists(out):
        print(f"motion.py: {out} exists; delete it to make a new one")
        return
    if page not in PROMPTS:
        sys.exit(f"motion.py: no prompt for {page}; add one to PROMPTS")
    art = os.path.join(HERE, "art", page + ".jpg")
    img = "data:image/jpeg;base64," + base64.b64encode(open(art, "rb").read()).decode()
    try:
        task = call("/image_to_video", {"model": "kling3.0_pro", "ratio": "1920:1080", "duration": 5,
                                        "promptImage": [{"uri": img, "position": "first"},
                                                        {"uri": img, "position": "last"}],
                                        "promptText": PROMPTS[page]})
    except urllib.error.HTTPError as e:
        sys.exit(f"motion.py: HTTP {e.code}: {e.read()[:400].decode(errors='replace')}")
    deadline = time.time() + 900
    while time.time() < deadline:
        time.sleep(8)
        t = call(f"/tasks/{task['id']}")
        if t.get("status") == "SUCCEEDED":
            os.makedirs(os.path.dirname(out), exist_ok=True)
            urllib.request.urlretrieve(t["output"][0], out)
            print(out)
            return
        if t.get("status") in ("FAILED", "CANCELLED"):
            sys.exit(f"motion.py: {t.get('status')}: {t.get('failure')} {t.get('failureCode')}")
    sys.exit("motion.py: no clip after 15 minutes")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: motion.py <page>")
    main(sys.argv[1])
