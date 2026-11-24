# Apiary landing site (stub)

Two static pages (the landing page and the business plan) + an interest sign-up
form, served by one Go binary. Its job is to gauge interest and collect rough
profiles to ground user stories (see [`../docs/DESIGN.md`](../docs/DESIGN.md)
§6) — not to onboard anyone.

- `GET /` — the page (embedded at build time)
- `GET /plan/` — the business plan, embedded the same way (its spec is
  [`../docs/design/plan/`](../docs/design/plan/))
- `POST /signup` — appends one JSON line to `SIGNUPS_FILE`
  (default `data/signups.jsonl`); honeypot field + 10s per-IP throttle,
  16 KiB body cap, everything clipped server-side
- `GET /healthz` — liveness

No accounts, no cookies, no analytics. The stored record is exactly the form
fields + UTC timestamp + a 12-hex-char IP hash (dedup signal, not identity).

## Run locally

```bash
go run .                      # http://localhost:8080, signups → ./data/
```

## Docker

```bash
docker build -t apiary-site .
docker run -d --name apiary-site -p 8080:8080 -v apiary_signups:/data apiary-site
```

Behind a reverse proxy, keep `X-Forwarded-For` flowing (the throttle and IP
hash use it).

## Link-preview cards

Each page's preview card is `static/og/<page>.jpg` (1200x630), named by the page's `og:image`.
`og/card.html` lays out a card: the page's art from `og/art/<page>.jpg`, its kicker and its headline,
in the site's own fonts. `og/render.sh [page ...]` screenshots it with Google Chrome into
`static/og/`. A new page adds a row to `PAGES` in `card.html`, its art, and its `og:*` tags.

Two cards are for the members app that shares apiary.wtf: `signin` for its `/sign-in` page and
`invite` for every `/join/...` link. They live here so `/og/` serves every card; the app's pages
name them in their own `og:*` tags. An invite's card is the same for every invite.

Apps that play an animated preview (Slack and Discord, told apart by User-Agent in `og.go`) get
`static/og/<page>.gif` instead, when the page has one; everyone else gets the JPEG. The GIF is made
by `og/animate.sh <page>` from `og/motion/<page>.mp4`, a five-second image-to-video clip of the
page's art, so its first frame is the still card; it fades back into that frame to loop.

## The keepers

The landing page's "the keepers" section, "Meet your fellow keepers", introduces four beekeepers, each
a kind of person with more than one account, as if they were members. They are types, not customers:
pun names, server racks for hives and a honey-cooling joke keep them plainly parody. Nobody real is
quoted or pictured, and no keeper may be rewritten to read as a real customer's review.

The four appear on a hive-cam sculpture: `static/og/keepers/skep.webp`, a skep of old televisions with
four screens cut out (transparent). Each keeper's photo sits under a hole, and a code-native caption
(REC, a name strip, a subtitle) sits over it, all placed by percentages of the art, so the sculpture
scales as one. The subtitles show only when the sculpture is wide enough to read them; the list beside
it carries every keeper's full line. The accepted design is `docs/design/keepers/`.

The screens are live: each shows a short loop, `static/og/keepers/<name>.mp4`, over its photo. A loop
starts as a five-second clip made with Runway (Kling 3.0) from the keeper's photo as both its first
and its last frame, so it ends where it began; the clips are kept in `og/motion/keepers/`.
`og/keepers-loop.sh <name>` makes the seamless 640 square loop: it evens out the small difference in
light the model leaves between the last frame and the first, drops the last, skips the few frames
where the model eases into and out of the photo, and steadies the grain with the loop wrapped round,
so the restart shows neither a jump nor a pause. The page plays a screen only while it is in view,
and never for a reader who asked for reduced motion; they, and any browser without the script, see
the photo.

The photos are generated, and every face is behind a veil. They are `static/og/keepers/<name>.webp`
(960x960), under `/og/` because that is a path apiary.wtf's shared router already sends to this site;
a new top-level path would need the production router changed first.

## Deploy (prod: https://apiary.wtf)

The `site` workflow (`.github/workflows/site.yml`) vets and builds on every PR
and push. It does not deploy. A deploy is run by hand on the prod host, which
resets its checkout to `origin/main`, rebuilds the image and recreates the
container. Deploys and infra changes go through Thomas.

Notes:
- Only `origin/main` ever deploys.
- `site/docker-compose.yml` here is a local-dev example. The production
  compose (Traefik labels, signup-data volume) is pinned host-side on purpose.
  Its router rule and priority must match this one's: apiary.wtf is shared
  with the members app, and the comment on the labels gives the order.
- Signup data lives outside the checkout and survives deploys.

## Reading the signups

```bash
jq -s 'group_by(.contribute) | map({contribute: .[0].contribute, n: length})' data/signups.jsonl
jq -r '.pain | select(. != "")' data/signups.jsonl     # the user-story goldmine
```
