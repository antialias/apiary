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

## Deploy (prod: https://apiary.wtf)

The `site` workflow (`.github/workflows/site.yml`) vets and builds on every PR
and push. It does not deploy. A deploy is run by hand on the prod host, which
resets its checkout to `origin/main`, rebuilds the image and recreates the
container. Deploys and infra changes go through Thomas.

Notes:
- Only `origin/main` ever deploys.
- `site/docker-compose.yml` here is a local-dev example. The production
  compose (Traefik labels, signup-data volume) is pinned host-side on purpose.
- Signup data lives outside the checkout and survives deploys.

## Reading the signups

```bash
jq -s 'group_by(.contribute) | map({contribute: .[0].contribute, n: length})' data/signups.jsonl
jq -r '.pain | select(. != "")' data/signups.jsonl     # the user-story goldmine
```
