# apiary.wtf/plan — copy lock and demo spec

The accepted design is "Live hive" (`concept-*.jpg` beside this file). This file locks every visible
string on the page. Where a concept image shows different words, this file wins (the concepts were
drawn before the copy was settled). Nothing that isn't here ships.

Concepts: 1 hero, short version and four ways in · 2 how it works and why pooling works ·
3 the problem · 4 what exists and economics · 5 market · 6 roadmap and risks · 7 team, the ask,
sources and footer · 8 phone layout. Known places where the concepts' words or data are wrong and
this file wins:

- 1: the rail order (use the order below); short-version item 03; the Contribute line.
- 2: the routing scenarios' labels; the bottom destination ("Donated credits" is out).
- 3: the second fact's wording; the GPU strip's day labels are uneven (use seven equal days).
- 4: the ledger and fee lines lack their AI Horde and OpenRouter notes.
- 5: the model names ("GPT-6 Astra" and "GPT-6 Luna" are out); the bars aren't to scale (draw a
  true log scale).
- 6: the rail's misspelled "Market".
- 7: its Sources list is invented; use the Sources below.

Third-party figures are cited with superscript note numbers that link to the Sources list. Every
figure was checked against its source on 2026-11-15. Anything simulated or illustrative is labelled
as such on the page.

## Page

- `<title>`: Apiary — business plan
- meta description: A member-owned pool of LLM capacity: lend idle GPUs running open models, burst past your own limits, earn credit for what you deliver.
- URL: `/plan/` (file `site/static/plan/index.html`).

## Header (scrolls away)

- Wordmark: honey hexagon + "apiary" (links to `/`)
- Nav: "The idea" → `#short` · "How it works" → `#how` · "Economics" → `#economics` · "Roadmap" → `#roadmap`
- Button (filled honey): "Get involved" → `#ask`

## Section rail (desktop: sticky, far left; the current section carries a honey tick)

Short version · Four ways in · The problem · How it works · Why pooling works · What exists ·
Economics · Market · Roadmap · Risks · Team · The ask

Mobile: the rail becomes a slim sticky bar under the header showing the current section's name with
a thin honey progress line beneath it.

## Hero

- Kicker: BUSINESS PLAN · NOVEMBER 2026
- H1: "Share the idle." / "Survive the *crunch*." ("crunch" honey italic)
- Paragraph: Apiary is a member-owned pool of LLM capacity. Members contribute what sits idle, earn credit for what they actually deliver, and draw on the pool when their own work spikes. The routing engine already runs every day for its first operator.
- Buttons: "Get involved" (filled) → `#ask` · "See it work" (outlined) → `#how`
- Background: the live hexagon field (see Demos, 1). No text in it.

## The short version (`#short`)

H2: The short version

| Numeral | Label | Line |
|---|---|---|
| 01 | The problem | Plans hit their caps mid-crunch. GPUs sit idle most of the day. |
| 02 | The fix | Pool it. Members' peaks rarely line up, so a shared pool needs far less headroom. |
| 03 | What exists | A working proxy and scheduler that route, meter and fail over. A self-host edition is built and tested. |
| 04 | The ask | Pilot members, engineers, GPU donors and early backers. |

## Four ways in (`#ways`)

H2: Four ways in

| Icon (hexagon outline) | Label | Line |
|---|---|---|
| up arrow | Use it | Burst past your own limits when work spikes. |
| GPU card | Contribute | Lend idle capacity: a GPU running open models, or a plan or key its provider lets you pool. |
| `</>` | Build it | Help build the engine in Go and Python. |
| heart | Back it | Fund the pilot and the legal groundwork. |

## 01 · The problem (`#problem`)

- Tag: 01 · THE PROBLEM
- H2: Capacity comes in bursts, *on both sides.*
- Lede: A coding agent works in sprints. A quiet week barely touches a plan's allowance; a deadline week can use it up by Wednesday. Meanwhile the GPUs that could fill the gap sit idle whenever their owners aren't using them.
- Panel title: One member's week · segmented control: "Quiet week" / "Deadline week" (default)
- Lanes: "Plan used this week" (line vs dashed "Weekly limit"; blocked span shaded ember, labelled "Out of capacity") and "Their GPU" (7 × 24 hexagon strip)
- Figures: "Hours blocked" and "GPU busy" (computed)
- Caption: Simulated: one member's coding demand and home GPU, hour by hour.
- Facts:
  - Plan limits “assume ordinary, individual usage.”¹
  - Microsoft says heavy Copilot use weighed on margins until GitHub moved to usage-based pricing in June 2026.²

## 02 · How it works (`#how`)

- Tag: 02 · HOW IT WORKS
- H2: Your tools stay yours. The pool is a *fallback, not a dependency.*
- Lede: A small proxy runs beside your tools. The brain decides where each request goes, from the same measurements it pays out on.
- Scenario control: "Normal day" (default) / "Your plan hits its cap" / "The pool goes offline"
- Nodes: "Your tools" → "Proxy · your machine" → "Brain · scheduler" → "Your own accounts" (top), "Pool GPUs · open models" (middle), "Your own GPU · local model" (bottom)
- Ladder: the three destinations are a ladder, after Scry's route view. Each destination node is a
  rung with its order numeral (1, 2, 3, honey italic), a usage bar with a shaded "near its limit"
  zone over the bar's last fifth, its percentage, and one chip: "Serving" (green), "Standby" (muted
  cream), "Full" (ember) or "Offline" (ember; the node dims with a dashed border, no fill and no
  percentage).
  - Rule line above the rungs (small mono, muted): "Tried in order. The first with room answers."
  - Zone label, once, over the first rung's zone: "near its limit"

  | Scenario | Your own accounts | Pool GPUs · open models | Your own GPU · local model |
  |---|---|---|---|
  | Normal day | 42% · Serving | 30% · Standby | 10% · Standby |
  | Your plan hits its cap | 97% · Full | 38% · Serving | 10% · Standby |
  | The pool goes offline | 97% · Full | Offline | 24% · Serving |

- Status line: "Routed to: {destination}" + green dot + "live", and under it the reason:
  - Normal day: "Why: your accounts have room, so they answer first."
  - Your plan hits its cap: "Why: your accounts are full, so the pool's GPUs answer instead."
  - The pool goes offline: "Why: your accounts are full and the pool is offline, so your own GPU answers."
- Note: The brain sees model class, token counts, timing and source. Never your prompts or your code.

## 03 · Why pooling works (`#pooling`)

- Tag: 03 · WHY POOLING WORKS
- H2: Everyone's peak is someone else's quiet hour.
- Sliders: "Members" (10–100, ticks 10 · 25 · 50 · 100, default 40) · "Overlap" (Low · Medium · High, default Low)
- Chart: y "Relative demand" 0×–4×, x Mon–Sun; legend "One member's demand" (cream) and "Pool demand, per member" (honey)
- Figures: "Headroom alone" and "Headroom pooled" (computed, one decimal, "×")
- Caption: Simulated: bursty weekly demand, 95th-percentile peak over average.

## 04 · What exists (`#exists`)

- Tag: 04 · WHAT EXISTS
- H2: The engine already runs.
- Lede: Built for one operator's own accounts and GPUs, used every day, and split so anyone can run it.
- Rows:
  - Routing proxy — Go. Sits beside Claude Code, Codex and other tools, and routes each request by model class.
  - Failover — On a limit or an error it retries elsewhere before your tool notices.
  - The brain — Python. Reads each source's usage and limits and picks who serves what.
  - Local models — A model on your own GPU is one more source in the same picker.
  - Self-host edition — Two containers and one setup command, tested end to end on every change.
- Terminal panel: green dot + "The engine is real"; lines, typed in one after another on a loop:
  - class=frontier → your account · 200
  - class=fast → your account · 200
  - plan at limit → pool GPU · qwen3-coder-30b · 200
  - pool GPU busy → another member's GPU · 200
  - class=local → your GPU · 127.0.0.1 · 200
- Caption: Illustrative.

## 05 · Economics (`#economics`)

- Tag: 05 · ECONOMICS
- H2: Earn on delivery. *Pay by model class.*
- Columns:
  - Credit for delivery only — The pool mints credit only for capacity it actually used, as the brain measured it. Nothing for pledges, no pre-mine.
  - Priced by model class — A frontier-class hour and a small local model's answer are different goods, so members price each class.
  - Credit now, a token later — Pilot credit is a ledger entry that can't be bought, sold or transferred, the way AI Horde's kudos have worked since 2022.¹¹ The token comes after economic modeling and counsel; none exists yet.
- Panel title: Your month in the pool · sliders "Your GPU serves" (0–12, "{n} h a day", default 4) and "Crunch days you draw" (0–10, default 3)
- Figures: "Credit earned" · "Credit spent" · "Balance" (signed, honey)
- Caption: Illustrative rates. Members will set real prices per class.
- Sub-heading: How the company earns
  - Equity and tokens, not fees — Apiary is an exchange of equity and tokens among the people who build it: idea people, engineers and leaders. It takes no cut of the capacity it routes.
  - Running it yourself stays free — The engine runs on your machines at no charge.

## 06 · Market (`#market`)

- Tag: 06 · MARKET
- H2: Open models now carry *most routed traffic.*
- Key figures:
  - 128.4T — tokens a week through OpenRouter in September 2026, up from 0.5T in January 2025.³
  - $2.5B — run-rate for Claude Code in February 2026.⁴
  - 4.7M — paid GitHub Copilot subscribers, up 75% in a year.⁵
  - $37B — enterprise generative-AI spend in 2025; coding was $4.0B of it.⁶
- Chart A, "Open-weight share of OpenRouter tokens": points "Late 2025 · about a third" (33), "Feb 2026 · 50%" (50), "Jul 2026 · 79%" (79), "Sep 2026 · 72%" (72); dashed line at 50%. Caption: Approximate, from OpenRouter's reports.³ ⁷
- Chart B, "Price per million output tokens", log scale: "gpt-6-astra $50" · "Claude Opus 5.5 $20" · "Claude Sonnet 5.5 $10" · "Claude Haiku 5.5 $0.50" · "gpt-6-luna $0.50" · honey "30B open model on a rented RTX 4090 ≈ $0.05"; axis ticks $0.01 · $0.1 · $1 · $10 · $100, titled "USD (log scale)". Caption: List prices; the open-model bar is batched serving cost, not a list price.⁸ ⁹ ¹⁰
- Table headed "Who" · "What it is" · "How contributors are rewarded":
  - AI Horde · Volunteer GPU pool for images and text, since 2022 · Kudos buy priority and can never be bought or sold¹¹
  - OpenRouter · One API across many model providers · 5.5% fee on credit purchases¹²
  - LiteLLM, Portkey · Gateways for your own keys · Open source plus paid plans
  - Chutes · Token-rewarded inference network · Lost 45% of its tokens when it ended subsidized use¹³
  - Apiary (honey tint) · Your accounts, your GPU and a member pool in one router · Credit for delivered capacity; equity and tokens for the people who build it

## 07 · Roadmap (`#roadmap`)

- Tag: 07 · ROADMAP
- H2: Pilot *before token.*
- Steps (number, label, chip, detail shown when selected; step 1 selected by default):
  1. Generalize the engine — Underway — Configuration instead of hard-coded hosts, pluggable alert channels, and member sign-in on the brain. The self-host edition is the first piece.
  2. Source drivers — Partly built — Your own accounts are built; community inference grows out of the local-model support.
  3. Metering into a ledger — Not started — A plain database first, built from the brain's existing measurements.
  4. Membership funnel — Live — The waggle list at apiary.wtf, then personas and user stories.
  5. Token and governance design — Later — Economic modeling and counsel; issuance only after both.
  6. Minimum viable pool — Not started — About five friendly members, coding-burst and local-inference classes, a database ledger, no token.
- Note: The pilot (6) runs before any token (5): real delivery data first, economics second.

## 08 · Risks (`#risks`)

- Tag: 08 · RISKS
- H2: What could go wrong, and what we do about it.
- Columns: "Risk" · "What we do". Each row expands to show "Why" (first row open by default).

| Risk | What we do | Why (expanded) |
|---|---|---|
| Provider terms | Provider by provider, the pool takes everything that provider's terms allow: members' GPUs running open-weight models, and plans, keys and credits wherever their provider permits pooling. Anything else serves only its owner's own work. | Each provider sets its own terms. Some subscription plans can't be made available to anyone else,¹⁴ some API keys can't be transferred to a third party,¹⁵ and some startup credits can't be passed on to end customers.¹⁶ Anthropic also bars routing plan credentials on behalf of other users.¹ |
| Proving delivery | Receipts signed by both sides, random spot checks and reputation; hardware attestation as it matures. | A GPU owner and a requester could collude to mint credit for work that never happened. |
| Securities law | No token without counsel. Pilot credit can't be bought, sold or transferred. | Credit that can be traded for money may count as a security. |
| Privacy | A pooled request runs on another member's GPU. You choose which work may burst; the brain logs metadata only. | Whoever serves a request sees its content, as any inference provider does. The logging policy is written down before the first outside member joins. |
| The subsidy trap | Credit only for delivery, and no signup rewards. | Free capacity attracts load that never pays back: Chutes processed 45% fewer tokens in six weeks once it cut subsidized use.¹³ |
| Everyone crunches at once | Members across time zones, GPU owners with different peaks, per-member budgets. | Correlated demand erodes the pooling benefit. Try "Overlap: High" in the demo above. |
| Open models aren't frontier models | Routing per model class: you decide what may burst, and to what. | A 30B open model is a step down for some work. |

## 09 · Team (`#team`)

- Tag: 09 · TEAM
- H2: Who's building it.
- Thomas — Built the routing engine and runs it every day.
- Jared — Co-founder.
- Paragraph: Members will govern which sources join, how classes are priced and how credit is issued. Until counsel helps choose a legal wrapper, the founders steward the pool, and they publish their reasons for any founders' allocation and for each provider policy.

## 10 · The ask (`#ask`)

- Tag: 10 · THE ASK
- H2: Get *involved.*
- Columns (label, text, button → link):
  - Pilot members — We're choosing about five members for the first pool: people who hit their limits, and people with GPUs to lend. — "Join the waggle list" → `/#join` (no choice ticked: pilot members both draw and lend)
  - Engineers — Metering, verification and scheduling, with real traffic behind them. — "Join the waggle list" → `/?want=dev-time#join`
  - Capacity donors — Lend a GPU running open-weight models, or a plan or key its provider lets you pool. Or fund rented GPUs. — "Join the waggle list" → `/?want=gpu#join`
  - Backers — Fund the pilot: engineering time, counsel for the legal wrapper, and seed GPU capacity. — "Talk to us" (filled) → `/?want=funding#join`
- Small print: This page describes plans. It is not an offer of tokens, securities or any investment. No token exists, and none will be issued before counsel signs off. Anything marked simulated or illustrative is a model, not data.

## Sources (`#sources`)

1. Anthropic, Claude Code legal and compliance — https://code.claude.com/docs/en/legal-and-compliance
2. Microsoft, FY2026 Q4 earnings call, July 2026 — https://www.microsoft.com/en-us/investor/events/fy-2026/earnings-fy-2026-q4
3. OpenRouter, The token state of play, September 2026 — https://openrouter.ai/data/newsletter/the-token-state-of-play
4. Anthropic, Series G announcement, February 2026 — https://www.anthropic.com/news/anthropic-raises-30-billion-series-g-funding-380-billion-post-money-valuation
5. Microsoft, FY2026 Q2 earnings call, January 2026 — https://www.microsoft.com/en-us/investor/events/fy-2026/earnings-fy-2026-q2
6. Menlo Ventures, 2025: The State of Generative AI in the Enterprise, December 2025 — https://menlovc.com/perspective/2025-the-state-of-generative-AI-in-the-enterprise/
7. OpenRouter and a16z, State of AI, January 2026 — https://arxiv.org/html/2601.10088v1
8. Anthropic, Pricing — https://platform.claude.com/docs/en/about-claude/pricing
9. OpenAI, API pricing — https://developers.openai.com/api/docs/pricing
10. CloudRift, Benchmarking RTX GPUs for LLM inference, October 2025 (Qwen3-Coder-30B, 4-bit, high concurrency) — https://www.cloudrift.ai/blog/benchmarking-rtx-gpus-for-llm-inference
11. AI Horde, Mission — https://aihorde.net/mission
12. OpenRouter, FAQ — https://openrouter.ai/docs/faq
13. Chutes, From volume to value, March 2026 — https://chutes.ai/news/from-volume-to-value-building-a-sustainable-ai-inference-platform-2
14. Anthropic, Consumer terms — https://www.anthropic.com/legal/consumer-terms
15. OpenAI, Services Agreement — https://cdn.openai.com/osa/openai-services-agreement.pdf
16. AWS, Activate terms — https://aws.amazon.com/activate/terms

## Footer

Honey hexagon + "apiary" (→ `/`) · right: "apiary.wtf" · "The code" (→ `https://github.com/antialias/apiary`) · "Back to the waggle list" (→ `/#join`)

## Home page (`site/static/index.html`) changes

- Add a fifth "What would you contribute?" choice: value `funding`, label "funding or sponsorship".
- On load, if the URL has `?want=<value>`, tick the matching contribute choice.

## Demos

All deterministic (seeded), no network, no libraries. Respect `prefers-reduced-motion`: draw a static
frame and skip animation. Pause animation off-screen and when the tab is hidden.

1. **Hero hexagon field** (canvas, behind the right two-thirds of the hero, with a soft dark fade behind
   the text). Small dim hexagon cells; a few clusters glow honey (members busy); thin light trails flow
   from idle cells toward busy clusters (capacity moving to whoever needs it). Every 6–10 s one cluster
   fades and another lights, so the flows re-route. Calm, slow.
2. **One member's week**: 168 hourly values. Deadline week: heavy weekday bursts that reach the weekly
   limit on Wednesday afternoon, after which demand is blocked ("Out of capacity", ember). Quiet week:
   stays well under the limit, nothing blocked. "Hours blocked" = hours with demand after the limit;
   "GPU busy" = share of the 168 GPU cells that are lit (the owner's own use).
3. **Routing**: SVG nodes and paths, honey dots travelling along the live path.
   - Normal day → Your own accounts.
   - Your plan hits its cap → Pool GPUs · open models.
   - The pool goes offline → Your own GPU · local model; the pool node dims with a dashed border.
   - On a scenario change the bars ease to their new values (about 600 ms), chips cross-fade, the
     serving rung glows softly and the dots re-route to it. Reduced motion: values jump, no glow.
   - The rungs are a list; the status and reason lines are a polite live region.
4. **Pooling**: each member's hourly demand = a daily rhythm shifted by the member's time zone × bursts.
   Bursts mix a shared component and the member's own, with the shared weight set by Overlap (Low,
   Medium, High); Low also spreads time zones widely, High puts everyone in one. Headroom = 95th
   percentile over mean, for one member (alone) and for the pool's per-member demand (pooled). Plot both
   normalised to a mean of 1, capped at 4× for display.
5. **Your month in the pool**: earned = 4 credits per GPU-hour × hours a day × 30; spent = 120 credits per
   crunch day; balance = earned − spent, signed.
6. **Market charts**: points and bars with hover or focus details naming the source; bars grow in when
   scrolled into view.
7. **Roadmap**: click or arrow keys select a step; its detail shows beneath.
8. **Risks**: rows expand and collapse; the first is open.
