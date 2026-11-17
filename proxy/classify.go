package main

// classify.go — §3 of docs/degraded-failover/60-spec-v2.md: the signal
// taxonomy. Rule 1: one classification site, one outcome type. The head site
// (classRoundTrip / walkRungs) classifies via classify; the shape sites — the
// codex exit-site classifier (classifyCodexExit), the healthTap
// (health_tap.go) and the codex non-stream site (main.go, beside the
// translate call) — construct hopOutcome directly. healthRecord (health.go)
// is the one consumer. §3's table IS this file's specification: the rows are
// the enum, columns (a)/(b) are the ladder/evidence fields.
//
// Classification can mis-score a hop; it can never mis-route one. The only
// field routing reads is ladder, and ladderWalkQuota is assigned exactly
// where the provider's adapter classes the refusal as one that walks
// (refusal.go, quota-model §2.6), so the walk and the rows never disagree.

import (
	"context"
	"errors"
	"net/http"
)

// Signal classes — §3's class column, the closed set.
const (
	sigHealthy   = "healthy"
	sigDegraded  = "degraded"
	sigQuota     = "quota"
	sigClient    = "client_error"
	sigAuth      = "auth"
	sigCancelled = "cancelled"
	sigUnknown   = "unknown"
)

// Column (a) — what the router MAY do with the hop. Two failure words and no
// third (§3): detour is only ever a failure at the response head; return is
// everything else. walk_quota is the ladder the refusal taxonomy walks (§2.6).
const (
	ladderStop      = "stop"
	ladderReturn    = "return"
	ladderDetour    = "detour" // recorded from #122; consumed by #123
	ladderWalkQuota = "walk_quota"
)

// Column (b) — whether the hop writes an evidence sample.
const (
	evDeg  = "deg"
	evOK   = "ok"
	evNone = "none"
)

// ctxKeyProxyDeadline tags a context whose deadline the PROXY set (§3 Rule 0).
// The error is the discriminator — main.go builds the server with
// ReadHeaderTimeout alone, so a DeadlineExceeded on a request context can only
// be a proxy-owned WithTimeout — and the tag is confirmation of whose deadline
// a tagged context is, never of what ended it.
type proxyDeadlineKey struct{}

var ctxKeyProxyDeadline proxyDeadlineKey

// hopOutcome is the ONE authoritative result of classifying an upstream hop
// (§3 Rule 1, r10): every producer constructs it, healthRecord consumes it —
// no producer hands a (sig, cause) tuple or loose booleans around routing.
type hopOutcome struct {
	row      string      // §3 row id ("1".."20", "4a", "4b") — the table is the enum
	sig      string      // class column: healthy|degraded|quota|client_error|auth|cancelled|unknown
	cause    string      // §6.2(1)'s cause token; "" when none
	phase    string      // "head" | "post_head"
	ladder   string      // column (a); nothing downstream re-reads resp
	evidence string      // column (b): deg|ok|none
	final    bool        // terminal for the hop (Rule 3); record runs exactly once, on a final outcome
	key      string      // endpoint_key the outcome scores (the member's ID)
	lease    *probeLease // non-nil iff this hop holds the probe token (§4.1)
	// refusal is the adapter's class for a non-2xx head (refusal.go), its
	// buffered body included. Not a §3 column: it rides here so the walk
	// records and logs the refusal without re-reading resp.Body.
	refusal refusal
}

// settled is true when a shape site has scored the hop's terminal row: the
// reply was read to its end (or to its failure) before anything later — a
// client hang-up included — could reach the decision record.
func (o hopOutcome) settled() bool { return o.final && o.phase == "post_head" }

// memberWireName discriminates the §3 row families by the wire that serves a
// member: "anthropic" (lane), else the wire's own name (wire.name: "codex"
// translated, "bearer" kimi, "runway" its asynchronous task, "local"), and
// "other" for a provider not configured here.
func memberWireName(m classMemberPref) string {
	if m.Kind == "anthropic" {
		return "anthropic"
	}
	if p := providerByName(m.Provider); p != nil && p.wire != nil {
		return p.wire.name()
	}
	return "other"
}

// classify is the HEAD site's classifier (§5.3): every hop the ladder issues
// runs through it, before the err/nil early return, so transport errors are
// visible (signals-4). It accepts a nil response. ctx is the hop request's
// context — Rule 0's cancel test reads the ERROR first and the context as
// corroboration. A 200 head is deliberately NOT final: the shape sites (§3's
// stream/non-stream producers) own its terminal row.
func classify(m classMemberPref, ctx context.Context, resp *http.Response, err error) hopOutcome {
	tn := tenantFrom(ctx)
	out := hopOutcome{phase: "head", key: m.ID}
	if err != nil {
		out.final = true
		switch {
		// Row 22 — our own first-event budget abandoned the hop (first_event.go).
		// Before Rule 0: the budget cancels the hop's context too, but a client
		// hang-up cancels the parent first and leaves Cause context.Canceled.
		case ctx != nil && context.Cause(ctx) == errFirstEventBudget:
			out.row, out.sig, out.cause, out.ladder, out.evidence = "22", sigDegraded, "first_event_timeout", ladderReturn, evDeg
		// Rule 0 — cancellation first, and the test is on the error: Canceled
		// can only have come from outside our own deadlines, i.e. the client.
		case errors.Is(err, context.Canceled) ||
			(ctx != nil && errors.Is(ctx.Err(), context.Canceled)):
			out.row, out.sig, out.ladder, out.evidence = "1", sigCancelled, ladderStop, evNone
		// Row 18 — a proxy-owned deadline (the server itself sets none, so
		// DeadlineExceeded here is ours by construction; ctxKeyProxyDeadline is
		// confirmation where the tagged context is in hand). The guard is the
		// hop count alone — the ladder is sequential, so exactly one hop is in
		// flight — and zero bytes forwarded is true here by construction.
		case errors.Is(err, context.DeadlineExceeded):
			out.row, out.sig, out.cause, out.ladder, out.evidence = "18", sigDegraded, "deadline", ladderReturn, evDeg
		// Row 17 — dial/TLS/reset/EOF with no response.
		default:
			out.row, out.sig, out.cause, out.ladder, out.evidence = "17", sigDegraded, "transport", ladderDetour, evDeg
		}
		return out
	}
	if resp == nil { // defensive: transports return err on every nil-resp path
		out.row, out.sig, out.cause, out.ladder, out.evidence = "17", sigDegraded, "transport", ladderDetour, evDeg
		out.final = true
		return out
	}
	if resp.StatusCode == 200 {
		// Row 5's head-side half: not final — the shape site scores the reply.
		out.row, out.sig = "5", sigHealthy
		return out
	}
	out.final = true // headFinal: non-200 heads terminate here (§5.3)
	ref := tn.rungRefusal(m, resp)
	out.refusal = ref
	w := memberWireName(m)
	if ref.walks() {
		// The quota-walk rows. ladderWalkQuota ⟺ the adapter's class walks
		// (§2.6, C7): exactly the refusals the taxonomy walks, no more, no
		// fewer. degraded and invalid keep their wire's rows below.
		out.sig, out.ladder, out.evidence = sigQuota, ladderWalkQuota, evNone
		switch {
		case ref.class == refWrongMember:
			out.row, out.sig = "14", sigClient // #90: the client's shape, not capacity — still walks
		case resp.StatusCode == 403:
			out.row = "10" // a provider's quota 403 (#89): Kimi's wordings
		case w == "codex":
			out.row = "6"
		case m.Kind == "anthropic":
			out.row = "13"
		default:
			out.row = "12" // a provider refusal off the codex wire (kimi, runway)
		}
		return out
	}
	sc := resp.StatusCode
	switch w {
	case "codex":
		switch {
		case sc == 401 || sc == 403:
			out.row, out.sig, out.ladder, out.evidence = "9", sigAuth, ladderStop, evNone
		case sc >= 500:
			out.row, out.sig, out.cause, out.ladder, out.evidence = "7", sigDegraded, "http_5xx", ladderDetour, evDeg
		case ref.class == refDegraded:
			// The outage 404 (codexRefusal): the backend failing, row 7's
			// ladder under its own cause.
			out.row, out.sig, out.cause, out.ladder, out.evidence = "7", sigDegraded, "outage_404", ladderDetour, evDeg
		case sc >= 400:
			out.row, out.sig, out.ladder, out.evidence = "8", sigClient, ladderStop, evNone
		}
	case "bearer", "runway":
		if sc >= 500 {
			out.row, out.sig, out.cause, out.ladder, out.evidence = "11", sigDegraded, "http_5xx", ladderDetour, evDeg
		}
	case "anthropic":
		switch {
		case sc == 529:
			out.row, out.sig, out.cause, out.ladder, out.evidence = "15", sigDegraded, "upstream_529", ladderDetour, evDeg
		case sc >= 500:
			out.row, out.sig, out.cause, out.ladder, out.evidence = "16", sigDegraded, "http_5xx", ladderDetour, evDeg
		}
	}
	if out.row == "" {
		// Row 20, the head site's residual — load-bearing: health acts only on
		// causes it can name (signals-11). No sample; Rule 4 only.
		out.row, out.sig, out.ladder, out.evidence = "20", sigUnknown, ladderReturn, evNone
	}
	return out
}

// classifyCodexExit is the codex stream producer: ONE classification at
// translateCodexSSE's exit (r9 decision 1) reading the translator's state
// once, in Rule 0 order. A sink write error while the stream never ended means
// ReverseProxy closed the pipe's read half — the client went away. Rule 0's
// exception: once a terminal event arrived (a.ended — completed, incomplete
// or failed, each parsed from the upstream's own event), the hop finished and
// a cancel after it is not row 1.
func classifyCodexExit(a *anthStream, err error, ctx context.Context) hopOutcome {
	out := hopOutcome{phase: "post_head", final: true}
	canceled := errors.Is(err, context.Canceled) ||
		(ctx != nil && errors.Is(ctx.Err(), context.Canceled))
	switch {
	case !a.ended && (canceled || a.writeErr != nil):
		out.row, out.sig, out.ladder, out.evidence = "1", sigCancelled, ladderStop, evNone
	case a.failed:
		out.row, out.sig, out.cause, out.ladder, out.evidence = "2", sigDegraded, "upstream_failed", ladderReturn, evDeg
	case a.refused:
		// Row 21 (#132): the backend answered, and the answer was no for this
		// request. Nothing about the member's health; the cause names it.
		out.row, out.sig, out.cause, out.ladder, out.evidence = "21", sigClient, "upstream_incomplete", ladderReturn, evNone
	case a.ended:
		out.row, out.sig, out.evidence = "5", sigHealthy, evOK
	case a.blocksOpened > 0:
		// Row 4b: truncation after content — the client's own network is a
		// candidate cause (R-32), so no evidence; the cause names it anyway.
		out.row, out.sig, out.cause, out.ladder, out.evidence = "4b", sigUnknown, "stream_truncated_late", ladderReturn, evNone
	default:
		out.row, out.sig, out.cause, out.ladder, out.evidence = "4a", sigDegraded, "stream_truncated", ladderReturn, evDeg
	}
	return out
}
