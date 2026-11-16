# Apiary — design notes & open questions

*Working document. Nothing here is decided unless it's marked decided. Edit
freely; argue in PRs or issues.*

## 1. What we're building (one paragraph)

A DAO-governed pool of LLM capacity. Members contribute metered agentic-coding
throughput and/or community-run inference (self-hosted open models). The pool's
routing engine (proxy + brain, already working for a single operator) meters
what each source actually delivers; contribution earns tokens; tokens are spent
to draw from the pool. Governance — which sources are admitted, how lanes are
priced, how tokens are emitted — belongs to token holders.

## 2. Why it can work (the asymmetry we're arbitraging)

Individual LLM capacity is **bursty on both sides**:

- A coding-agent subscription sits mostly idle, then hits its ceiling exactly
  when the owner needs it most (a crunch).
- Self-hosted GPU capacity runs at a tiny duty cycle but its owner has no way to
  trade surplus inference for anything they want.

Peak demand across many members is far smoother than any one member's peak. The
pool converts individual burstiness into collective smoothness, and tokens are
the accounting mechanism for who smoothed whom.

## 3. Terminology

| Term | Meaning |
|---|---|
| **Member** | a person/entity in the pool, identified by a wallet + profile |
| **Capacity source** | anything that can serve requests: a metered coding-agent account, a self-hosted inference node |
| **Lane** | a model class (routing granularity — already implemented) |
| **Contribution** | delivered capacity, as metered by the brain (not promised capacity) |
| **Draw** | consumption from the pool, debited per lane/model class |
| **Cell** | (proposed) a brain instance + the sources it manages; the pool may federate multiple cells |

## 4. Architecture direction

### 4.1 Today (single-cell, single-operator)

```
member's tools ──▶ proxy (laptop) ──▶ capacity sources
                      │  ▲
                      ▼  │ decisions + tokens
                    brain (one host, one operator's sources)
```

### 4.2 Target (pooled)

```
member A tools ─▶ proxy A ─┐                 ┌─ A's sources
member B tools ─▶ proxy B ─┼──▶ cell brain ◀─┼─ B's sources
member C tools ─▶ proxy C ─┘        │        └─ community inference nodes
                                    ▼
                          contribution ledger ──▶ token emission
                          draw accounting     ──▶ token burn/transfer
                                    ▼
                               DAO governance
```

Key properties to preserve from the current engine:

- **Data plane stays local.** Traffic goes member ⟷ source; the brain sees
  metadata, not conversations. This is both a privacy stance and a scalability
  one.
- **Degrades to a no-op.** A member's tooling must keep working if the pool
  disappears. Home base until forced.
- **The meter is the router.** Contribution accounting reads the same
  observations that routing does — there is no separate self-reported number to
  game.

### 4.3 Trust boundaries (the hard part)

Three distinct trust problems, in increasing difficulty:

1. **Member ⟷ brain**: the brain holds source credentials and hands out
   bearers. Today: one shared secret. Pooled: per-member authn, per-source
   authz, audit log. Conventional engineering.
2. **Pool ⟷ contribution claims**: how do we know a source delivered what the
   ledger says? For *hosted-subscription* sources, the provider's own usage API
   is the meter (hard to fake, third-party). For *community inference*, the
   requester and the server can collude or lie — we need request receipts
   signed by both sides, spot-check verification, or reputation-weighted
   trust. **Open question Q5.**
3. **Draw fairness**: a member could route garbage through expensive lanes.
   Draw pricing per lane/model class plus per-member budgets bounds the damage.

## 5. Token design (nothing decided)

Deliberately sketched, not specified — this section is mostly questions
(§7 Q1–Q4) plus constraints we already believe:

- **Earn on delivery, not on pledge.** Emission follows metered contribution,
  full stop. No pre-mine for capacity that hasn't served requests. (Founders'
  allocation, if any, is a separate explicit decision — not smuggled in.)
- **Draw pricing must reflect real cost heterogeneity.** A frontier-model
  agentic hour and a 30B-local-model completion are not the same good. Lanes
  already give us the pricing granularity.
- **The token is a claim on capacity, not on revenue** (as currently
  envisioned) — closer to a utility credit than a dividend. Whether that
  framing survives legal review is exactly what legal review is for; **no
  token is issued before counsel signs off.** *(Decided.)*
- **Governance weight vs. spend balance may need to be separate dimensions** —
  otherwise heavy spenders lose their vote by using the pool for exactly what
  it's for. Consider earn-history or staked balance for governance weight.

## 6. Membership funnel (what the stub site is for)

We know almost nothing about who actually wants this. The landing site
(`site/`) collects rough interest profiles to ground user stories:

- what they'd **contribute** (coding-agent plan tier / GPU hardware / nothing,
  just want to draw)
- what they'd **draw** (burst coding capacity, cheap bulk inference, model
  variety)
- current monthly LLM spend (bucketed)
- crypto familiarity (bucketed — tells us how much wallet UX we must hide)
- biggest pain with current setup (free text — the user-story goldmine)

Profiles feed persona synthesis → user stories → epic prioritization. The form
is deliberately low-friction (no wallet, no verification — this is a signal
sweep, not onboarding).

## 7. Open questions

| # | Question | Current lean | Owner |
|---|---|---|---|
| Q1 | Chain / L2 choice — or start with a plain database ledger and tokenize later? | **DB-first.** Prove metering + accounting off-chain; tokenizing a working ledger is easier than debugging a broken one on-chain | — |
| Q2 | Fixed emission curve vs. dynamic (peg to delivered capacity)? | dynamic feels right (capacity-backed), needs modeling | — |
| Q3 | Founders'/early-contributor allocation? | decide explicitly, publish rationale | Thomas + Jared |
| Q4 | Legal wrapper (Wyoming DAO LLC? Foundation? Unincorporated association?) | needs counsel; blocks any issuance | Thomas + Jared |
| Q5 | Verifiable metering for community inference (collusion resistance) | dual-signed request receipts + random spot-checks to start; research zkML/TEE attestation as it matures | — |
| Q6 | Sybil resistance for signup rewards (if any) | no signup rewards → no sybil problem; earn-on-delivery only | — |
| Q7 | Privacy: what does the brain log about a request? | metadata only (lane, tokens, timing, source); never content. Write this down as policy before first external member | — |
| Q8 | Provider-ToS posture for hosted-subscription sources | members warrant their sources' terms permit their use; pool policy per provider; needs a real written policy | Thomas + Jared |
| Q9 | What's the minimum viable pool? (N members, which lane types) | ~5 friendly members, coding-burst + local-inference lanes, DB ledger, no token yet | — |
| Q10 | Does the brain federate (cells) or centralize? | single cell until it hurts | — |

## 8. Sequencing (matches the GitHub epics)

1. **Generalize the engine** — config over hardcoded hosts, pluggable alert
   sinks, member authn on the brain. *(Everything else builds on this.)*
2. **Source-driver abstraction** — hosted-subscription driver (exists) +
   community-inference driver (grow from the Ollama lane).
3. **Contribution metering → ledger** — DB-first (Q1), schema from the brain's
   existing metrics.
4. **Membership funnel** — stub site, profiles, personas, user stories.
5. **Token + governance design** — economics modeling, counsel (Q3, Q4), only
   then issuance.
6. **Minimum viable pool** — Q9's friendly-members pilot, no token, real
   routing.

The pilot (6) deliberately precedes the token (5) going fully live: we want
delivery data and member behavior before economics are frozen.
