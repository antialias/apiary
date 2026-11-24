# Apiary landing site (stub)

Three static pages (the front door, the long page and the business plan) + an
interest sign-up form, served by one Go binary. Its job is to gauge interest and collect rough
profiles to ground user stories (see [`../docs/DESIGN.md`](../docs/DESIGN.md)
§6) — not to onboard anyone.

- `GET /` — the front door: one screen, never scrolls, for a signed-out visitor
  (a signed-in one gets the members app's console there instead). Embedded at
  build time; its spec is [`../docs/design/front-door/`](../docs/design/front-door/)
- `GET /about/` — the long page, with the waggle-list form at `#join`
- `GET /plan/` — the business plan, embedded the same way (its spec is
  [`../docs/design/plan/`](../docs/design/plan/))
- `POST /signup` — appends one JSON line to `SIGNUPS_FILE`
  (default `data/signups.jsonl`); honeypot field + 10s per-IP throttle,
  16 KiB body cap, everything clipped server-side
- `GET /healthz` — liveness
- `www.apiary.wtf` redirects to `apiary.wtf`, path and query kept

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
in the site's own fonts. `og/render.sh [page ...]` screenshots it with Playwright's own browser into
`static/og/`. A new page adds a row to `PAGES` in `card.html`, its art, and its `og:*` tags.

**A change to a public page updates its card in the same commit.** A new or changed headline, pitch
or look means new art or a new `PAGES` row, a fresh `render.sh`, and `og:*` tags that say the same
thing as the page (title, description, alt text). `og_pages_test.go` holds the floor: every page has
a full set of tags, its own card at 1200x630, and alt text that opens with the page's headline in
quotes, so a headline changed without its card fails the build.

Two cards are for the members app that shares apiary.wtf: `signin` for its `/sign-in` page and
`invite` for every `/join/...` link. They live here so `/og/` serves every card; the app's pages
name them in their own `og:*` tags. An invite's card is the same for every invite.

Apps that play an animated preview (Slack and Discord, told apart by User-Agent in `og.go`) get
`static/og/<page>.gif` instead, when the page has one; everyone else gets the JPEG. The GIF is made
by `og/animate.sh <page>` from `og/motion/<page>.mp4`, a five-second image-to-video clip of the
page's art, so its first frame is the still card; it fades back into that frame to loop.

## The keepers

The long page's (`/about/`) "the keepers" section, "Meet your fellow keepers", introduces four beekeepers, each
a kind of person with more than one account, as if they were members. They are types, not customers:
pun names, server racks for hives and a honey-cooling joke keep them plainly parody. Nobody real is
quoted or pictured, and no keeper may be rewritten to read as a real customer's review.

The four appear on a hive-cam sculpture: `static/og/keepers/skep.webp`, a skep of old televisions with
four screens cut out (transparent). Each keeper's photo sits under a hole, and a code-native caption
(REC, a name strip, a subtitle) sits over it, all placed by percentages of the art, so the sculpture
scales as one. The subtitles show only when the sculpture is wide enough to read them; the list beside
it carries every keeper's full line. The accepted design is `docs/design/keepers/`.

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
