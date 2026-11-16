package main

// scaleMark (#76) + usage_scaling counters (#77): the latch semantics that make
// the tap's scaling block honest, and the status shape scry consumes.

import (
	"strings"
	"testing"
)

// withCleanScaleStats snapshots and restores the package counters so these
// tests neither see nor leave residue from the e2e suites.
func withCleanScaleStats(t *testing.T) {
	t.Helper()
	scaleStatsMu.Lock()
	oldOK, oldFail := scaleStatsOK, scaleStatsFail
	scaleStatsOK, scaleStatsFail = map[string]int64{}, map[string]map[string]int64{}
	scaleStatsMu.Unlock()
	t.Cleanup(func() {
		scaleStatsMu.Lock()
		scaleStatsOK, scaleStatsFail = oldOK, oldFail
		scaleStatsMu.Unlock()
	})
}

func TestScaleMarkFirstResolutionWins(t *testing.T) {
	withCleanScaleStats(t)
	m := newScaleMark("kimi/kimi-k3", 0.5)
	if _, ok := m.view(); ok {
		t.Fatal("pending mark rendered")
	}
	m.resolveOK("sse")
	m.failOpen("unparseable") // late per-line surprise must not repaint an ok mark
	m.resolveOK("json")       // nor double-count
	v, ok := m.view()
	if !ok || !v.applied || v.mode != "sse" {
		t.Fatalf("mark = %+v ok=%v, want the FIRST resolution (applied sse)", v, ok)
	}
	scaleStatsMu.Lock()
	n, nf := scaleStatsOK["kimi/kimi-k3"], len(scaleStatsFail)
	scaleStatsMu.Unlock()
	if n != 1 || nf != 0 {
		t.Fatalf("counters ok=%d failKeys=%d, want exactly one ok and no fail-opens", n, nf)
	}
}

func TestScaleMarkNonScalingRatioResolvesNone(t *testing.T) {
	withCleanScaleStats(t)
	// ratio 1 (same tokenizer) and 0 (undeclared): any resolver lands "none" —
	// claiming applied OR failed for a hop scaling was never aimed at is a lie.
	for _, r := range []float64{0, 1} {
		m := newScaleMark("gpt/gpt-5.6", r)
		m.resolveOK("translated")
		if _, ok := m.view(); ok {
			t.Fatalf("ratio %v rendered a scaling block", r)
		}
		m = newScaleMark("gpt/gpt-5.6", r)
		m.failOpen("stream_truncated")
		if _, ok := m.view(); ok {
			t.Fatalf("ratio %v rendered a fail-open block", r)
		}
	}
	scaleStatsMu.Lock()
	total := len(scaleStatsOK) + len(scaleStatsFail)
	scaleStatsMu.Unlock()
	if total != 0 {
		t.Fatal("none-resolutions leaked into the counters")
	}
}

func TestScaleMarkNilSafe(t *testing.T) {
	var m *scaleMark
	m.retarget("x", 0.5)
	m.noteRaw(map[string]any{"input_tokens": float64(1)})
	m.noteRawIn(1)
	m.resolveOK("sse")
	m.failOpen("oversize")
	m.resolveNone()
	if _, ok := m.view(); ok {
		t.Fatal("nil mark rendered")
	}
}

func TestScaleMarkRetargetMovesAttribution(t *testing.T) {
	withCleanScaleStats(t)
	// A class bound to kimi crosses to gpt mid-request: the resolution must be
	// attributed to (and gated by) the member that actually served.
	m := newScaleMark("kimi/kimi-k3", 0.61)
	m.retarget("gpt/gpt-5.6-sol", 0.6)
	m.noteRawIn(100)
	m.resolveOK("translated")
	v, ok := m.view()
	if !ok || !v.applied || v.ratio != 0.6 {
		t.Fatalf("mark = %+v ok=%v, want applied at the retargeted 0.6", v, ok)
	}
	scaleStatsMu.Lock()
	kimi, gpt := scaleStatsOK["kimi/kimi-k3"], scaleStatsOK["gpt/gpt-5.6-sol"]
	scaleStatsMu.Unlock()
	if kimi != 0 || gpt != 1 {
		t.Fatalf("counters kimi=%d gpt=%d, want the served member credited", kimi, gpt)
	}

	// And a cross ONTO anthropic (ratio zeroed) turns any late resolution into
	// "none" — the RoundTrip exit resolves the shared mark on that path.
	m = newScaleMark("kimi/kimi-k3", 0.61)
	m.retarget("", 0)
	m.resolveNone()
	if _, ok := m.view(); ok {
		t.Fatal("anthropic-crossed mark rendered a scaling block")
	}
}

func TestUsageScalingStatusShape(t *testing.T) {
	withCleanScaleStats(t)
	newScaleMarkResolved := func(key, state, reason string) {
		m := newScaleMark(key, 0.5)
		if state == "ok" {
			m.resolveOK("sse")
		} else {
			m.failOpen(reason)
		}
	}
	newScaleMarkResolved("kimi/kimi-k3", "ok", "")
	newScaleMarkResolved("kimi/kimi-k3", "ok", "")
	newScaleMarkResolved("kimi/kimi-k3", "fail_open", "content_encoding")
	newScaleMarkResolved("gpt/gpt-5.6-sol", "fail_open", "stream_truncated")

	s := usageScalingStatus()
	kimi, _ := s["kimi/kimi-k3"].(map[string]any)
	if kimi == nil || kimi["ok"] != int64(2) {
		t.Fatalf("kimi block = %v, want ok 2", s)
	}
	if fo, _ := kimi["fail_open"].(map[string]int64); fo["content_encoding"] != 1 {
		t.Fatalf("kimi fail_open = %v", kimi["fail_open"])
	}
	gpt, _ := s["gpt/gpt-5.6-sol"].(map[string]any)
	if gpt == nil || gpt["ok"] != int64(0) {
		t.Fatalf("gpt block = %v, want ok 0 alongside its fail-open", s)
	}
	if fo, _ := gpt["fail_open"].(map[string]int64); fo["stream_truncated"] != 1 {
		t.Fatalf("gpt fail_open = %v", gpt["fail_open"])
	}
}

// The codex truncated close is the one site that publishes usage the client
// can misread as real (input_tokens: 0, cache fields omitted) — the mark must
// say fail_open{stream_truncated}, which is what lets scry render "unknown"
// instead of "scaled" there.
func TestCodexTruncatedStreamFailsOpen(t *testing.T) {
	withCleanScaleStats(t)
	truncated := "event: response.created\n" +
		`data: {"type":"response.created","response":{"id":"resp_1","model":"gpt-5.6"}}` + "\n\n" +
		"event: response.output_text.delta\n" +
		`data: {"type":"response.output_text.delta","delta":"half an ans"}` + "\n\n"
	mark := newScaleMark("gpt/gpt-5.6-sol", 0.5)
	var out strings.Builder
	if err := translateCodexSSE(strings.NewReader(truncated), &out, 0.5, mark, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	v, ok := mark.view()
	if !ok || v.applied || v.reason != "stream_truncated" {
		t.Fatalf("mark = %+v ok=%v, want fail_open stream_truncated", v, ok)
	}
	// The pre-start death (no events at all) publishes only an error — no usage
	// reaches the client, so the mark stays PENDING and the tap omits the block.
	mark = newScaleMark("gpt/gpt-5.6-sol", 0.5)
	out.Reset()
	if err := translateCodexSSE(strings.NewReader(""), &out, 0.5, mark, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := mark.view(); ok {
		t.Fatal("pre-start death resolved the mark; no usage was published, it must stay pending")
	}
}
