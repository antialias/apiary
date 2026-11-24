# The front door

`apiary.wtf/` for a signed-out visitor: one screen that says what Apiary is, offers
sign-in, and links to the long page (`/about/`). A signed-in visitor gets the console at
`/` instead; that routing is the proxy's, not this site's.

The accepted concept is the spec: `front-door-desktop.jpg` (drawn at 1536x1024) and
`front-door-phone.jpg` (390 wide). The jars, pipes and hexagon are artwork with no text
(`site/static/og/front/`); every word on them is HTML laid over it.

## Copy

- Header: apiary · Sign in
- Headline: All your coding accounts. *One hive.*
- Line (wide screens): Apiary is the gateway your coding agents point at. Claude Code,
  Codex, Kimi and your local models act as one, and work moves to your next account before
  this one runs dry.
- Line (phones): The gateway your coding agents point at. Your accounts act as one, and
  work moves to the next before one runs dry.
- Jars: Claude Code · Codex · Kimi · local. Meter: 38% left, across 4 accounts.
- Buttons: Sign in · See how it works → (to `/about/`)
- Invite-only for now · Join the waggle list (to `/about/#join`)
- Proof points:
  - Switches before it runs dry: No half-finished runs, no switching by hand.
  - Ask for a class, not a model: Fast, capable, frontier, with your own fallbacks.
  - Knows when you'll run out: Forecasts the next run-out and spends the reset worth most.
  - Quiet until it matters: The status line says “14% left” only when you're low.
- Footer: Business plan · Privacy · Code on GitHub

## Never scrolls

Every size is the concept's pixels times one scale, the smaller of width and height
against the drawing, with floors for text. Where the floors would push the page past the
fold, it drops detail in this order:

- Wide laptop screens 820px tall or less: proof points keep their titles only.
- Phones in a browser (760px tall or less): the same.
- Phones 600px tall or less: the two buttons share a row.
- Phones held sideways: no proof points, the short line, no jar labels.

Checked with no scroll at 21 sizes, from 320x568 to 2560x1440.

## Departures from the concept

- The phone concept drew other icons and "Local models"; both layouts use the desktop's
  icons and "local".
