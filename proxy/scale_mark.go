package main

// ---- usage-scaling provenance (#76): the scaleMark ----
//
// #44 made the transport rewrite pass-through/translated usage into Anthropic
// units, and it fails OPEN by design: on any surprise the original bytes pass
// verbatim, which is the safe wire behavior — and an invisible one. A log line
// aside, "scaled" and "passed verbatim" are indistinguishable downstream, so
// scry cannot render an honest badge and a systematic fail-open (a provider
// forcing gzip back on, say) would be discovered by calibration drift instead
// of observation. The scaleMark closes that: one mutex-guarded record per
// request, created request-side at actuation, written by whichever layer
// actually scales (the kimi rewriters, the codex synthesis sites, or the
// RoundTrip exit for hops where scaling doesn't apply), and read by the
// response tap at terminal publish — the servedHop pattern (class.go:107),
// which rebuildForMember's anthropic-arm comment anticipated.
//
// Ordering: tap-aggregated usage arrives through tapRecorder.Write AFTER the
// scale site rewrote those bytes (the SSE rewriter sits upstream of the client
// copy, JSON bodies are rewritten before ReverseProxy writes at all, and codex
// events are authored already-scaled), so by the time the tap publishes its
// terminal event a scaled turn's mark is resolved. The mutex makes the
// residual race benign: a reader that loses it sees "pending" and omits the
// block — never a torn record, never a false claim.
//
// States: "" (pending) resolves exactly once to one of
//   - ok        — input-side usage was rewritten into Anthropic units
//   - fail_open — scaling should have applied and the original bytes passed
//     verbatim instead (reason says why)
//   - none      — no scaling applies to the hop that served (an Anthropic hop,
//     or an undeclared/1.0 ratio)
//
// A mark whose ratio is not a scaling ratio (<=0 or 1) resolves "none" no
// matter which resolver fires: claiming "applied" — or "failed" — for a hop
// that was never going to be scaled would both be lies.

import "sync"

type scaleMark struct {
	mu    sync.Mutex
	key   string  // memberScaleKey of the hop this mark is aimed at ("provider/model"; "" = anthropic)
	ratio float64 // declared r_p at actuation; retargeted by mid-request crosses

	state  string // "" pending | "ok" | "fail_open" | "none"
	mode   string // ok only: sse | json | count_tokens | translated | translated_agg
	reason string // fail_open only: content_encoding | oversize | unparseable | no_usage | stream_truncated

	// Pre-scale input-side counts, overlay semantics: a field is captured only
	// when the wire actually carried it — tapUsage's unknown-vs-zero doctrine.
	// A codex response carrying no input_tokens_details therefore captures rawIn
	// alone: the cache zeros the CLIENT sees on that turn are synthesized to keep
	// the turn countable, and recording them here would launder a synthesized
	// zero into a reported one.
	rawIn, rawCacheRead, rawCacheCreate *int
}

// scaleMarkView is the read side: only a RESOLVED ok/fail_open mark renders.
type scaleMarkView struct {
	applied                             bool // state == ok
	mode                                string
	ratio                               float64
	reason                              string
	rawIn, rawCacheRead, rawCacheCreate *int
}

func newScaleMark(key string, ratio float64) *scaleMark {
	return &scaleMark{key: key, ratio: ratio}
}

// retarget re-aims the mark at the member a mid-request cross actually rebuilt
// for — the resolution that eventually fires must be attributed (and gated) by
// the hop that served, not the one merely bound.
func (m *scaleMark) retarget(key string, ratio float64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.key, m.ratio = key, ratio
	m.mu.Unlock()
}

// noteRaw captures the PRE-scale input-side counts off one usage map. Call it
// BEFORE scaleUsageMap rewrites the map in place; fields absent from the map
// stay unknown (nil). Latest wire value wins, mirroring tapUsage.overlay.
func (m *scaleMark) noteRaw(u map[string]any) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if f, ok := u["input_tokens"].(float64); ok {
		n := int(f)
		m.rawIn = &n
	}
	if f, ok := u["cache_read_input_tokens"].(float64); ok {
		n := int(f)
		m.rawCacheRead = &n
	}
	if f, ok := u["cache_creation_input_tokens"].(float64); ok {
		n := int(f)
		m.rawCacheCreate = &n
	}
}

// noteRawIn is the codex synthesis sites' capture of the uncached remainder —
// the input count as it will be emitted, before scaling. Cache fields the
// upstream never carried stay unknown here even though the wire the CLIENT sees
// asserts zeros (anthUsage synthesizes those).
func (m *scaleMark) noteRawIn(n int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.rawIn = &n
	m.mu.Unlock()
}

// noteRawCache is the rest of that capture, called ONLY when the codex response
// actually carried an input_tokens_details block. Split from noteRawIn rather
// than folded into it precisely so the call site has to decide: a turn with no
// details block must leave these unknown, or scry renders "upstream reported no
// cache" for a turn where the upstream reported nothing at all.
func (m *scaleMark) noteRawCache(read, create int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.rawCacheRead, m.rawCacheCreate = &read, &create
	m.mu.Unlock()
}

// resolveOK latches the mark scaled. First resolution wins — a multi-frame SSE
// stream calls this on every input-carrying frame and counts once.
func (m *scaleMark) resolveOK(mode string) { m.resolve("ok", mode, "") }

// failOpen latches the mark failed-open: scaling should have applied and the
// original bytes passed verbatim instead.
func (m *scaleMark) failOpen(reason string) { m.resolve("fail_open", "", reason) }

// resolveNone latches the mark as "no scaling applies to this hop".
func (m *scaleMark) resolveNone() { m.resolve("none", "", "") }

func (m *scaleMark) resolve(state, mode, reason string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != "" {
		return // first resolution wins; the latch is also the counters' dedup
	}
	if m.ratio <= 0 || m.ratio == 1 {
		m.state = "none" // never claim applied/failed for a hop that scaling was never aimed at
		return
	}
	m.state, m.mode, m.reason = state, mode, reason
	if state == "ok" || state == "fail_open" {
		countScaleResolution(m.key, state, reason)
	}
}

// view renders the mark for publication: ok only when RESOLVED ok/fail_open.
// Pending and none both answer !ok — the tap then omits the scaling block, so
// a consumer can never read "verbatim" out of "unknown" (the badge-honesty
// contract scry builds on).
func (m *scaleMark) view() (scaleMarkView, bool) {
	if m == nil {
		return scaleMarkView{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != "ok" && m.state != "fail_open" {
		return scaleMarkView{}, false
	}
	return scaleMarkView{
		applied: m.state == "ok",
		mode:    m.mode,
		ratio:   m.ratio,
		reason:  m.reason,
		rawIn:   m.rawIn, rawCacheRead: m.rawCacheRead, rawCacheCreate: m.rawCacheCreate,
	}, true
}

// ---- fleet counters (#77): usage_scaling on /_switch/status ----
//
// Display only. No alerts, no notification hooks — the alerting rebuild is
// parked (platform#13) and this hard rule holds here too. Scry's status band
// consumes this for its only-when-nonzero system line; the absence of anomaly
// renders literally nothing.

var (
	scaleStatsMu   sync.Mutex
	scaleStatsOK   = map[string]int64{}            // memberScaleKey -> scaled turns
	scaleStatsFail = map[string]map[string]int64{} // memberScaleKey -> fail-open reason -> count
)

func countScaleResolution(key, state, reason string) {
	if key == "" {
		return // scaling never fires on an anthropic hop; belt and suspenders
	}
	scaleStatsMu.Lock()
	defer scaleStatsMu.Unlock()
	switch state {
	case "ok":
		scaleStatsOK[key]++
	case "fail_open":
		if scaleStatsFail[key] == nil {
			scaleStatsFail[key] = map[string]int64{}
		}
		scaleStatsFail[key][reason]++
	}
}

// usageScalingStatus is the /_switch/status "usage_scaling" block. Takes only
// scaleStatsMu — a leaf, like eventsStatus: it must never reach for st.mu,
// which is held across the whole status literal.
func usageScalingStatus() map[string]any {
	scaleStatsMu.Lock()
	defer scaleStatsMu.Unlock()
	out := map[string]any{}
	for key, n := range scaleStatsOK {
		out[key] = map[string]any{"ok": n, "fail_open": map[string]int64{}}
	}
	for key, reasons := range scaleStatsFail {
		e, ok := out[key].(map[string]any)
		if !ok {
			e = map[string]any{"ok": int64(0)}
			out[key] = e
		}
		fo := map[string]int64{}
		for r, n := range reasons {
			fo[r] = n
		}
		e["fail_open"] = fo
	}
	return out
}
