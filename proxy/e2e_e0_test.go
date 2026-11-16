package main

// E0 — Stage 0 characterization goldens (docs/degraded-failover/60-spec-v2.md §9,
// #120). These tests pin TODAY's ladder behaviour at the byte level —
// upstream-received bodies, auth sequence, client-visible status/body/headers —
// so the walkRungs/rankCandidates/classify extraction can be proven a pure
// refactor: this file is written against the pre-refactor code and must pass
// unchanged after it. SWITCH_HEALTH is unset throughout (default off).

import (
	"io"
	"sort"
	"strings"
	"testing"
	"time"
)

// clientHeaderNames returns the sorted set of response header names the client
// saw. Values are deliberately not pinned here (Date and Content-Length vary);
// the NAME SET is what a refactor could silently grow or shrink.
func clientHeaderNames(h map[string][]string) string {
	names := make([]string, 0, len(h))
	for k := range h {
		names = append(names, k)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// (E0-1) Healthy non-stream hop: one upstream attempt, exact spliced body out,
// exact upstream bytes back to the client.
func TestE0HealthyClassHopByteIdentical(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-e0-healthy")
	defer resp.Body.Close()

	hit := h.hit(t)
	wantOut := `{"model":"claude-fable-5","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`
	if string(hit.body) != wantOut {
		t.Fatalf("upstream body drifted:\n got %q\nwant %q", hit.body, wantOut)
	}
	if hit.auth != "Bearer "+e2eToken(other) {
		t.Fatalf("attempt 1 auth = %q, want the fable incumbent's bearer", hit.auth)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	wantIn := `{"type":"message","content":[{"type":"text","text":"ok"}]}`
	if string(b) != wantIn {
		t.Fatalf("client bytes drifted:\n got %q\nwant %q", b, wantIn)
	}
	if got := clientHeaderNames(resp.Header); got != "Content-Length,Content-Type,Date" {
		t.Fatalf("client header name set drifted: %s", got)
	}
	select {
	case extra := <-h.seen:
		t.Fatalf("healthy hop must be exactly one upstream attempt, saw another: %+v", extra)
	default:
	}
}

// (E0-2) Healthy STREAM hop: the SSE script must reach the client byte-identical,
// frame boundaries included.
func TestE0HealthyClassStreamByteIdentical(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()

	script := "event: message_start\ndata: {\"type\":\"message_start\"}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"hi\"}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	h.anthSSE.Store(script)
	h.anthGap.Store(5 * time.Millisecond)
	defer h.anthSSE.Store("")

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-e0-stream")
	defer resp.Body.Close()
	h.hit(t)

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}
	b, _ := io.ReadAll(resp.Body)
	if string(b) != script {
		t.Fatalf("stream bytes drifted:\n got %q\nwant %q", b, script)
	}
}

// (E0-3) The full quota walk: account A 429 → same-member account swap →
// account B 429 → cross to kimi under /coding → 200. Pins the auth sequence,
// that the replayed body is byte-identical between the two anthropic attempts,
// the kimi splice + base-path re-join, and the client's final bytes.
func TestE0QuotaWalkByteIdentical(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	h.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"You have exceeded your weekly usage limit."}}`)
	h.deny429.Store("*")

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-e0-walk")
	defer resp.Body.Close()

	first := h.hit(t)
	second := h.hit(t)
	if first.auth != "Bearer "+e2eToken(other) || second.auth != "Bearer "+e2eToken(native) {
		t.Fatalf("auth sequence drifted: %q then %q", first.auth, second.auth)
	}
	if string(first.body) != string(second.body) {
		t.Fatalf("account-swap replay must reuse the identical body:\n a1 %q\n a2 %q", first.body, second.body)
	}
	wantAnth := `{"model":"claude-fable-5","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`
	if string(first.body) != wantAnth {
		t.Fatalf("anthropic body drifted:\n got %q\nwant %q", first.body, wantAnth)
	}

	third := h.hostedHit(t)
	wantKimi := `{"model":"kimi-k3","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`
	if string(third.body) != wantKimi {
		t.Fatalf("kimi body drifted:\n got %q\nwant %q", third.body, wantKimi)
	}
	if third.auth != "Bearer kimi-secret" || third.path != "/coding/v1/messages" {
		t.Fatalf("kimi hop = auth %q path %q", third.auth, third.path)
	}

	if resp.StatusCode != 200 {
		t.Fatalf("client status = %d, want kimi's 200", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	wantIn := `{"type":"message","content":[{"type":"text","text":"hosted ok"}]}`
	if string(b) != wantIn {
		t.Fatalf("client bytes drifted:\n got %q\nwant %q", b, wantIn)
	}

	// two moves, two alerts (drained so the coalescer never outlives restore)
	classAlertsN(t, h.e2eEnv, 2)
}

// (E0-4) Every rung refuses: the client receives the LAST refusal intact —
// kimi's own 429 status and exact body bytes, not a synthesized error.
func TestE0AllRungsRefusedSurfacesLastRefusalIntact(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	h.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"You have exceeded your weekly usage limit."}}`)
	h.deny429.Store("*")
	h.deny.Store(true) // kimi refuses too; gpt is usable=false and never tried

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-e0-exhausted")
	defer resp.Body.Close()

	h.hit(t)       // attempt 1: fable incumbent
	h.hit(t)       // attempt 2: swapped account
	h.hostedHit(t) // attempt 3: kimi, also refuses

	if resp.StatusCode != 429 {
		t.Fatalf("client status = %d, want the last rung's 429", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	want := `{"type":"error","error":{"type":"rate_limit_error","message":"provider busy"}}`
	if string(b) != want {
		t.Fatalf("client must see kimi's refusal verbatim:\n got %q\nwant %q", b, want)
	}
	h.noCodexLeak(t)
	classAlertsN(t, h.e2eEnv, 2)
}
