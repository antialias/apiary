# Apiary 🐝

**Pooled LLM capacity, owned by the people who contribute it.**

Apiary is a DAO-governed network that aggregates members' agentic-coding
capacity and community-run inference into one shared pool. Contributors bring
capacity to the hive; in exchange they receive tokens representing their stake
in the pool and their right to draw from it. One member's idle hours become
another member's burst capacity.

> **Status: early.** What exists today is the routing/metering engine the pool
> will run on, extracted from a working single-user deployment (it has been
> load-balancing one person's coding agents across capacity sources, per model
> lane, in daily use). The DAO layer — token, governance, contribution
> accounting — is design-stage. This README describes both what runs now and
> where it's going.

## The idea

Agentic coding subscriptions and GPU boxes have the same problem from opposite
directions: **capacity is bursty**. A person with a coding-agent plan uses a
fraction of it most weeks, then slams the ceiling during a crunch. A person
with a 4090 in the closet has inference to spare all day. Neither can trade
their surplus for the other's.

Apiary makes the surplus liquid:

- **Contribute** capacity to the pool — metered agentic-coding throughput or
  community inference (self-hosted open models).
- **Earn** tokens proportional to what your contribution actually delivered
  (measured by the same metering brain that routes traffic — contribution
  accounting and routing are one system, so there's nothing to game that the
  router doesn't already see).
- **Spend** tokens to draw from the pool: burst past your own ceiling during a
  crunch, or run inference without owning hardware.
- **Govern** the pool as a DAO: admission of capacity sources, lane policy,
  emission schedule, and treasury are token-holder decisions.

## Architecture

Two components, split so the data plane stays local and the control plane stays
shared:

```
┌─────────────┐     what lane? which source?     ┌─────────────┐
│   proxy/    │ ───────────────────────────────▶ │   brain/    │
│  (per-user  │ ◀─────────────────────────────── │  (shared    │
│ data plane) │     route + budget decision      │control plane)│
└─────┬───────┘                                  └─────┬───────┘
      │                                                │
      ▼                                                ▼
 LLM traffic flows directly                 usage polling, lane state,
 user ↔ capacity source                     quota math, alerting,
 (never through the brain)                  contribution metering
```

### `proxy/` — the data plane (Go, runs on each member's machine)

A local HTTP proxy that agentic tooling points at (one env var). It routes
**per model lane** — each model family is an independent lane that can ride a
different capacity source at the same time — so a heavy lane can burst onto
pooled capacity while a light lane stays on the member's home source.

- Lane-aware routing with independent per-lane failover
- Rate-limit responses (429s) trigger source-wide cooldown and automatic
  switchover; tiered alerts stay quiet until the last fallback is burned
- Local-model lanes: self-hosted models (Ollama today) appear in the same
  model picker as hosted ones, with the context-window pitfalls handled
- Pin overrides (`/pin?lane=`) when a human wants to overrule the brain

See [`proxy/`](../proxy/).

### `brain/` — the control plane (Python, runs wherever the pool lives)

The yardmaster. Polls capacity sources for real usage, computes each lane's
effective headroom, and answers one question for every proxy:
*"which source should this lane ride right now?"*

- `/preferred` — per-lane routing decisions (home-base-until-forced policy:
  members ride their own capacity until it's actually constrained)
- Usage polling with backoff, last-good caching, and per-source metrics
  (Prometheus) — this same metering is the basis for contribution accounting
- Guarded credential handoff (`/token`) so the proxy never stores pool
  credentials
- Alert fan-out for switchover and exhaustion events

See [`brain/`](../brain/).

### What the DAO layer adds (not yet built)

- **Contribution ledger**: brain metering → attested contribution records →
  token emission
- **Draw accounting**: pool usage debits, priced per lane/model class
- **Governance**: capacity-source admission, lane policy, emission curve
- **Sybil/abuse resistance**: the brain already observes real delivered
  throughput, which is the hard part; the ledger makes it binding

## Running the current engine

```bash
# data plane (per user)
cd proxy && go build -o claude-switch .
# see proxy/README.md for env vars and the install script

# control plane (shared host)
cd brain && ./deploy.sh   # see brain/README.md for credential setup
```

The proxy activates via `ANTHROPIC_BASE_URL` (or your tool's equivalent
base-URL override) and is a pure passthrough when the brain is unreachable —
the pool can never take a member's tooling down.

## Roadmap

1. **Generalize the engine** — strip single-deployment assumptions (hardcoded
   hosts, personal alert plumbing) into config; multi-tenant brain
2. **Contribution metering → ledger** — turn the brain's per-source usage
   metrics into signed contribution records
3. **Token + governance design** — economics, emission, draw pricing.
   *We will get real legal counsel on the token design before anything is
   issued. Tokenized claims on a shared revenue-producing pool sit in
   regulated territory and we intend to treat that seriously, not vibe it.*
4. **Community inference mesh** — grow the local-lane support (Ollama today)
   into pooled self-hosted inference with health/capability discovery
5. **Membership UX** — join flow, wallet linkage, dashboards

## Repo layout

| Path       | What                                                          |
|------------|---------------------------------------------------------------|
| `proxy/`   | Go lane-routing proxy (data plane, per member)                 |
| `brain/`   | Python usage/quota brain (control plane, shared)               |

## Contributing

Private repo, small crew, everything is up for discussion — open an issue or
just start a thread. The engine has tests (`cd proxy && go test ./...`); keep
them green.
