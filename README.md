# Apiary

Apiary routes Claude Code, and anything else that speaks the Anthropic Messages API, across a
pool of model accounts. It is two services:

- **`proxy/`, the switch proxy** (Go, standard library only). Clients point
  `ANTHROPIC_BASE_URL` at it. It maps each request's model class (`claude-fast`,
  `claude-capable`, `claude-frontier`) to an account and a model, signs the request for that
  account and forwards it. When an account runs out or fails, it moves to the class's next
  member, and it reports what it served to the brain.
- **`brain/`, the brain** (Python, standard library only). It keeps the accounts (Anthropic API
  keys, Claude-plan sign-ins, Codex, Kimi, a local model) and polls their usage windows. It
  publishes `/preferred`, which says which account each class should use now and what each
  account may spend.

## Quickstart

You need Docker with Compose, and an Anthropic API key. From a checkout of this repository:

    docker compose run --rm init
    docker compose up -d
    docker compose exec brain accounts add anthropic-api main

1. `init` writes two secrets, the brain's sealing key, the tenant registry and the class
   document onto the `data` volume, then prints both secrets and the tenant id. This is the
   only time it prints the secrets. It refuses to run a second time.
   - The **client secret** is what Claude Code sends the proxy.
   - The **switch key** guards the brain's routes. The `accounts` command reads it from the
     volume itself.
   - The **tenant** is this machine's one (`ten_` and 26 letters, not a secret): every brain
     route that reads or writes its data is under `/t/<tenant>/`.
2. `up -d` starts the brain on `127.0.0.1:9115` and the proxy on `127.0.0.1:8787`. Both
   listen on loopback only.
3. `accounts add anthropic-api main` asks for your API key and adds it as the account `main`.
   The key is read from the terminal without being shown, and never printed. The brain checks
   the key with a model list before it saves anything; the command then waits up to 90 seconds
   for the account's first check and says whether it serves. Adding the same name again
   replaces that account's key.

The images come from `ghcr.io/antialias/apiary-proxy` and `apiary-brain`. To build them from
this checkout instead, run `docker compose build` before `up -d`.

To read the secrets again:

    docker compose exec brain cat /creds/client_secret
    docker compose exec brain cat /creds/switch_key

## Accounts

The brain serves no page. Accounts are managed with the `accounts` command in the brain's
container. It drives the brain's account routes (`/t/<tenant>/auth/…` and `/t/<tenant>/preferred`)
with the switch key and the tenant it reads from the volume:

    docker compose exec brain accounts list
    docker compose exec brain accounts add <provider> <name>
    docker compose exec brain accounts sign-in claude|codex [name]
    docker compose exec brain accounts remove <provider> <name>

- `list` shows, for each provider, how it picks among its accounts, then the accounts in the
  order that rule takes them, each with its status and its key's last four characters. API
  keys come after the Claude subscriptions, by name; every other provider keeps the account
  in use until it runs low or can't be used. There is no order to set.
- `add` is for API keys (`anthropic-api`, `kimi`). Add `-T` to `exec` when you pipe the key in.
- `sign-in` signs in a Claude or ChatGPT login (below). With a name, it signs that account in
  again.
- `add` and `sign-in` wait up to 90 s for the account's first check after the save and say
  whether it serves.

## Claude Code

    export ANTHROPIC_BASE_URL=http://127.0.0.1:8787
    export ANTHROPIC_AUTH_TOKEN=<the client secret>
    export CLAUDE_CODE_ENABLE_GATEWAY_MODEL_DISCOVERY=1
    claude --model claude-capable

`ANTHROPIC_AUTH_TOKEN` is the only credential Claude Code sends. The proxy checks it, strips it
and signs each request with the chosen account's own key, so your own Claude login is never
sent. Every request except `GET /_switch/healthz` needs it.

With gateway model discovery on, Claude Code asks the proxy's `GET /v1/models` at startup. The
proxy answers itself, and the class ids appear in the `/model` picker as "From gateway" rows.
Claude Code has to be version 2.1.129 or later for this.

## Classes

A class id is a promise about the work, not a model. Each class is an ordered list of members.
Each member is an Anthropic lane (`opus`, `sonnet`, `haiku`, `fable`) served by your Anthropic
accounts, or a model from another provider. For each session, the brain picks the first member
that has headroom. The proxy swaps the request's model for that member's model and signs the
request for its account. If an account answers 429, the proxy tries the lane's other accounts,
then moves one hop to the next member.

| Id | For | Members in the shipped document |
|---|---|---|
| `claude-fast` | search, extraction, mechanical edits | Sonnet, Haiku, then Kimi |
| `claude-capable` | carrying out a clear brief | Opus, then Kimi |
| `claude-frontier` | hard reasoning and open design | Fable, then Kimi |

The Kimi members do nothing until you set a Kimi key (below). Until then each class runs on
its Anthropic lanes alone. Named Anthropic ids such as `claude-opus-5-5` also work. They route
by lane on the same accounts.

The document is `classes.json` on the volume. `init` seeds it from
`brain/classes.example.json`. To edit it:

    docker compose cp brain:/creds/classes.json .
    # edit classes.json
    docker compose cp classes.json brain:/creds/classes.json
    curl -X POST -H "X-Switch-Key: $(docker compose exec -T brain cat /creds/switch_key)" \
      http://127.0.0.1:9115/classes/reload

The reload refuses a document that does not parse, and the brain keeps the one it has.
`POST /classes/validate` with `{"doc": ...}` reports what is wrong with a draft first. Each
member declares its `context_window`. A member from another provider also declares its
`anthropic_token_ratio`: that provider's token count for a body divided by Anthropic's count
for the same body.

## Where state lives

Everything is on the Compose volume `apiary_data`, mounted at `/creds` in both services:

- `switch_key` and `client_secret`, from `init`
- `key`, the brain's sealing key, and `tenants.json`, the tenant registry, both from `init`
- `classes.json`, the one operator document
- under `<tenant>/`, the tenant's files, each sealed with `key` (AES-256-GCM; the brain
  refuses a plain one, and without the key refuses a sealed one): the accounts,
  `anthropic-api.json` (API keys), `accounts.json` (Claude-plan sign-ins), `openai-codex.json`
  (Codex), `kimi.json` (a Kimi key, from `accounts add kimi`); the brain's documents, `consumers.json`
  and `money.json`; its routing state, `switch_state.json`; and the ledger, `ledger.jsonl`, plain
- the proxy's class state, under `proxy/`

The sealing key beside the data defends a backup or a copied volume, not the machine itself:
whoever can read `key` can open everything. Back the volume up whole.

`docker compose down` keeps the volume. `docker compose down -v` deletes it, keys and all.

## More accounts and providers

**Claude-plan and Codex sign-ins.** `accounts sign-in` signs these in.
- **Claude**: `docker compose exec brain accounts sign-in claude`. Open the link it prints,
  approve, and paste back the code the callback page shows. The account is named by its
  verified email. The brain keeps the refresh token, and an independent grant never disturbs a
  laptop's login. A plan account serves the same lanes as an API key.
- **Codex**: `docker compose exec brain accounts sign-in codex <name>`. Open the link it prints.
  The browser ends on a `localhost:1455` page that fails to load. That is expected. Paste the
  whole address-bar URL back. Don't run `codex login` with the same grant anywhere else: the
  brain is that chain's only refresher, and a second refresher kills it.
- A Codex account serves a class only through a member that names it. Add one to a class's
  `members` with `"kind": "provider"`, `"provider": "gpt"`, its `model`, `context_window` and
  `anthropic_token_ratio`.

> **Terms.** Using a Claude-plan login (Free, Pro, Max or Ultra) through third-party software
> such as Apiary is against Anthropic's terms for those plans, and a ChatGPT-plan login may be
> against OpenAI's. Read the terms that apply to you. Apiary supports both, but if you sign
> one in, you do so at your own risk. API keys are the route those terms allow.

**Kimi.** `docker compose exec brain accounts add kimi <name>`, and paste the key. The brain
keeps it sealed in the tenant's `kimi.json`, polls Kimi's quota with it, and dispenses it to
the proxy, which signs that tenant's Kimi requests with it; the proxy holds no key of its own.
Once it is enrolled, the Kimi members of the shipped classes come into use.

**A local model.** Serve one with an Anthropic-compatible API and an OpenAI-style
`GET /models` (llama-server does both). Point both services at it in `.env`, then run
`docker compose up -d`:

    LOCAL_UPSTREAM=http://host.docker.internal:8080

Add a class for it to `classes.json`. Each member needs `"kind": "provider"`,
`"provider": "local"`, the server's `model` id, its `context_window` and
`"anthropic_token_ratio": 1.0`. Claude Code assumes a 200k window for any `claude-` id it
doesn't know, so keep a local class's prompts within the window you declared.

## Where this repository comes from

`proxy/`, `brain/`, this README, `LICENSE`, `docker-compose.yml`, `docker-compose.smoke.yml`,
`init.sh`, `smoke/`, `.github/workflows/ci.yml` and `.github/workflows/images.yml` are published
from a private trunk on every green push there. Each publish replaces them whole, so the next
publish overwrites any edit made to them here. `site/`, `docs/`, `.github/workflows/site.yml`
and `.gitignore` belong to this repository, and a publish never touches them.

## Running the tests

    cd proxy && go vet ./... && go test ./...
    cd brain && python3 test_exporter.py
    bash smoke/run.sh

The proxy needs the Go version named in `proxy/go.mod`, and the brain needs Python 3.12.
Neither has dependencies beyond its standard library, and neither suite reaches the network.
`smoke/run.sh` needs Docker. It builds both images from the checkout and runs the stack with
`docker-compose.smoke.yml`, against a stub Anthropic API inside the Compose network, and
removes everything it made when it ends.

## More

[docs/VISION.md](docs/VISION.md) sets out what the project is for. Each service lists its
environment variables at the top of its main file (`proxy/main.go`,
`brain/claude_usage_exporter.py`).

## License

Apache License 2.0. See [LICENSE](LICENSE).
