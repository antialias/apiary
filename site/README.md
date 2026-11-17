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

Apps that play an animated preview (Slack and Discord, told apart by User-Agent in `og.go`) get
`static/og/<page>.gif` instead, when the page has one; everyone else gets the JPEG. The GIF is made
by `og/animate.sh <page>` from `og/motion/<page>.mp4`, a five-second image-to-video clip of the
page's art, so its first frame is the still card; it fades back into that frame to loop.

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
