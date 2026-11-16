package main

// Unit tests for the response tap (#61). The e2e side (byte transparency, the three
// lanes, latency) lives in e2e_resp_test.go; this file drives respTap directly and
// proves the review-mandated safety properties: S1/S10 (the backstop never closes the
// feed channel), S3 (any drop poisons the aggregate; drain on every exit), S8 (usage
// presence is preserved, never invented), and the hot-path shape itself (a go/parser
// guard, because the mechanism that protects live inference must not erode silently).

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"
)

// respTapSettle waits out any aggregator goroutine still running. Call it BEFORE a
// cleanup restores tunables the aggregator reads (evMaxEventBytes, tapReserve, ...):
// the gauge is incremented synchronously with spawn and decremented as the goroutine's
// final act, so zero here is authoritative. Any fixture whose test can drive the full
// handler (which spawns aggregators per response) needs this, not just respTest —
// e2e tests reach respTaps through plain tapTest/evTest too.
func respTapSettle() {
	deadline := time.Now().Add(5 * time.Second)
	for respTapActive.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
}

// respTest resets the hub, the tap tunables (shared budgets live in tap.go), and this
// file's own knobs and counters.
func respTest(t *testing.T) {
	t.Helper()
	tapTest(t)
	chanSize, idle, maxJSON, errMax := respTapChanSize, respTapIdleMax, respTapMaxJSON, respTapErrMax
	enabled := respTapEnabled
	t.Cleanup(func() {
		// A Fatalf can fire between spawn and finish; join before restoring.
		respTapSettle()
		respTapChanSize, respTapIdleMax, respTapMaxJSON, respTapErrMax = chanSize, idle, maxJSON, errMax
		respTapEnabled = enabled
		respTapHook = nil
	})
	respTapStarts.Store(0)
	respTapMessages.Store(0)
	respTapEnds.Store(0)
	respTapDropChunks.Store(0)
	respTapLossy.Store(0)
	respTapPanics.Store(0)
	respTapBackstops.Store(0)
}

func respIdentFixture() tapRespIdent {
	return tapRespIdent{
		ok:         true,
		convKey:    "abcd1234/ef567890",
		session:    "sess0000",
		sessionKey: strings.Repeat("a", 64),
		contentFP:  "ef567890",
		epoch:      1,
		turn:       3,
		index:      7,
		lane:       "fable",
		modelReq:   "claude-fable-5",
		modelUp:    "claude-fable-5",
		route:      "ROUTED->cafe [acct] lane=fable",
		account:    "acct@example.com",
		stream:     true,
	}
}

// respSSEFixture is a complete Anthropic SSE stream: text block, tool_use block with
// streamed input, partial usage on message_start, full usage on message_delta.
func respSSEFixture() string {
	return "event: message_start\n" +
		`data: {"type":"message_start","message":{"id":"msg_01","type":"message","role":"assistant","model":"claude-fable-5-upstream","content":[],"usage":{"input_tokens":25,"cache_read_input_tokens":100}}}` + "\n\n" +
		"event: ping\ndata: {\"type\":\"ping\"}\n\n" +
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}` + "\n\n" +
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello "}}` + "\n\n" +
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"world"}}` + "\n\n" +
		`data: {"type":"content_block_stop","index":0}` + "\n\n" +
		`data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_01","name":"Bash","input":{}}}` + "\n\n" +
		`data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"command\":"}}` + "\n\n" +
		`data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"\"ls\"}"}}` + "\n\n" +
		`data: {"type":"content_block_stop","index":1}` + "\n\n" +
		`data: {"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"input_tokens":25,"output_tokens":50,"cache_read_input_tokens":100,"cache_creation_input_tokens":7}}` + "\n\n" +
		`data: {"type":"message_stop"}` + "\n\n"
}

const respJSONFixture = `{"id":"msg_01","type":"message","role":"assistant","model":"claude-fable-5-upstream","content":[{"type":"text","text":"Hello world"},{"type":"tool_use","id":"toolu_01","name":"Bash","input":{"command":"ls"}}],"stop_reason":"tool_use","stop_sequence":null,"usage":{"input_tokens":25,"output_tokens":50,"cache_read_input_tokens":100,"cache_creation_input_tokens":7}}`

func respHeader(ct string) http.Header {
	h := http.Header{}
	h.Set("Content-Type", ct)
	return h
}

// respDrive runs one full tap cycle: feed the body in deliberately misaligned chunks
// (frame reassembly is part of what's under test), close, wait for the aggregator.
func respDrive(t *testing.T, id tapRespIdent, status int, ct, body string, chunkSize int) []tapFrame {
	t.Helper()
	drain := tapListen(t)
	rt := newRespTap(id)
	hdr := respHeader(ct)
	for len(body) > 0 {
		n := chunkSize
		if n > len(body) {
			n = len(body)
		}
		rt.feed([]byte(body[:n]), status, hdr)
		body = body[n:]
	}
	rt.finish()
	respWaitIdle(t)
	return drain()
}

func respWaitIdle(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if respTapActive.Load() == 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("aggregator did not exit — the drain-until-close contract is broken")
}

func respKinds(frames []tapFrame) []string {
	out := []string{}
	for _, f := range frames {
		out = append(out, f.kind)
	}
	return out
}

func respOne(t *testing.T, frames []tapFrame, kind string) json.RawMessage {
	t.Helper()
	var got []json.RawMessage
	for _, f := range frames {
		if f.kind == kind {
			got = append(got, f.raw)
		}
	}
	if len(got) != 1 {
		t.Fatalf("%d %q events, want exactly 1 (saw %v)", len(got), kind, respKinds(frames))
	}
	return got[0]
}

func respNone(t *testing.T, frames []tapFrame, kind string) {
	t.Helper()
	for _, f := range frames {
		if f.kind == kind {
			t.Fatalf("unexpected %q event (saw %v)", kind, respKinds(frames))
		}
	}
}

func respDecodeMsg(t *testing.T, raw json.RawMessage) respMessageEvent {
	t.Helper()
	var ev respMessageEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		t.Fatalf("assistant_message does not decode: %v", err)
	}
	return ev
}

func respDecodeEnd(t *testing.T, raw json.RawMessage) respEndedEvent {
	t.Helper()
	var ev respEndedEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		t.Fatalf("turn_ended does not decode: %v", err)
	}
	return ev
}

// ---- 1: the SSE happy path ----

func TestRespIdentRenderUsesFinalServedHop(t *testing.T) {
	respTest(t)
	id := respIdentFixture()
	id.modelReq = "claude-frontier"
	id.modelUp = "claude-fable-5"
	id.route = "CLASS[frontier→lane:fable]"
	id.served = &servedHop{class: "frontier", member: "lane:fable", account: "acct-a",
		model: "claude-fable-5", lane: "fable", attempts: 1}

	id.served.selectHop(classMemberPref{ID: "gpt/gpt-5.6-sol", Model: "gpt-5.6-sol"},
		"gpt-pro", "class:frontier", true, 0.6)
	got := id.render()
	if got.ModelRequested != "claude-frontier" || got.ModelUpstream != "gpt-5.6-sol" {
		t.Fatalf("models = requested:%q upstream:%q", got.ModelRequested, got.ModelUpstream)
	}
	if got.Class != "frontier" || got.Member != "gpt/gpt-5.6-sol" || got.Account != "gpt-pro" {
		t.Fatalf("served identity = class:%q member:%q account:%q", got.Class, got.Member, got.Account)
	}
	if got.Lane != "class:frontier" || !got.Translated || got.Attempts != 2 {
		t.Fatalf("served route = lane:%q translated:%v attempts:%d", got.Lane, got.Translated, got.Attempts)
	}
	if got.Route != "CLASS[frontier→lane:fable]" {
		t.Fatalf("desired route changed to %q; it must remain distinct from the served hop", got.Route)
	}
}

// The identity names the request's harness, so every response-side event carries it.
func TestRespIdentCarriesHarness(t *testing.T) {
	respTest(t)
	id := respIdentFixture()
	id.harness = harnessKimiCode
	frames := respDrive(t, id, 200, "text/event-stream", respSSEFixture(), 37)
	var st respStartedEvent
	if err := json.Unmarshal(respOne(t, frames, "turn_started"), &st); err != nil {
		t.Fatalf("turn_started does not decode: %v", err)
	}
	if st.Harness != harnessKimiCode {
		t.Fatalf("turn_started harness = %q, want %q", st.Harness, harnessKimiCode)
	}
	if ev := respDecodeMsg(t, respOne(t, frames, "assistant_message")); ev.Harness != harnessKimiCode {
		t.Fatalf("assistant_message harness = %q, want %q", ev.Harness, harnessKimiCode)
	}
}

// A request that re-sends an identical array takes tapTurn's "nothing new" exit and
// publishes no turn event; its identity still carries the harness, so the attempt's
// turn_started names it rather than leaving the consumer to wait for route_decision.
func TestTapResendIdentityCarriesHarness(t *testing.T) {
	respTest(t)
	drain := tapListen(t)
	ctx := withHarness(tapLaneCtx("default"), harnessClaudeCode)
	post := func() tapRespIdent {
		r := httptest.NewRequest("POST", "/v1/messages", nil)
		r.Header.Set("x-claude-code-session-id", "sess-H")
		return tapTurn(ctx, r, []byte(tapConvo("hello", 3)), "claude-fable-1", " ROUTED [acct] lane=default", "acct", "deadbeef")
	}
	if first := post(); first.harness != harnessClaudeCode {
		t.Fatalf("first identity harness = %q, want %q", first.harness, harnessClaudeCode)
	}
	if ev := tapOnly(t, drain()); ev.Harness != harnessClaudeCode {
		t.Fatalf("turn harness = %q, want %q", ev.Harness, harnessClaudeCode)
	}
	again := post()
	if got := drain(); len(got) != 0 {
		t.Fatalf("an identical resend emitted %d events, want 0", len(got))
	}
	if !again.ok || again.harness != harnessClaudeCode {
		t.Fatalf("resend identity ok=%v harness=%q, want ok and %q", again.ok, again.harness, harnessClaudeCode)
	}
	frames := respDrive(t, again, 200, "text/event-stream", respSSEFixture(), 37)
	var st respStartedEvent
	if err := json.Unmarshal(respOne(t, frames, "turn_started"), &st); err != nil {
		t.Fatalf("turn_started does not decode: %v", err)
	}
	if st.Harness != harnessClaudeCode {
		t.Fatalf("resend's turn_started harness = %q, want %q", st.Harness, harnessClaudeCode)
	}
}

func TestRespAggAnthropicSSE(t *testing.T) {
	respTest(t)
	id := respIdentFixture()
	frames := respDrive(t, id, 200, "text/event-stream", respSSEFixture(), 37)

	respNone(t, frames, "turn_ended")
	start := respOne(t, frames, "turn_started")
	var st respStartedEvent
	if err := json.Unmarshal(start, &st); err != nil {
		t.Fatalf("turn_started does not decode: %v", err)
	}
	if !st.SSE || st.Status != 200 || st.ConversationKey != id.convKey || st.Index != 7 {
		t.Fatalf("turn_started = sse:%v status:%d key:%q index:%d", st.SSE, st.Status, st.ConversationKey, st.Index)
	}

	ev := respDecodeMsg(t, respOne(t, frames, "assistant_message"))
	if ev.ConversationKey != id.convKey || ev.SessionKey != id.sessionKey || ev.Epoch != 1 || ev.Turn != 3 {
		t.Fatalf("identity did not carry: %+v", ev.respIdentJSON)
	}
	if ev.Message.Index != 7 || ev.Message.Role != "assistant" {
		t.Fatalf("message index/role = %d/%q, want 7/assistant", ev.Message.Index, ev.Message.Role)
	}
	if ev.ModelStream != "claude-fable-5-upstream" || ev.StopReason != "tool_use" {
		t.Fatalf("model_stream=%q stop_reason=%q", ev.ModelStream, ev.StopReason)
	}
	if ev.StreamTruncated || ev.Lossy || ev.ChunksDropped != 0 {
		t.Fatalf("clean stream flagged: truncated=%v lossy=%v dropped=%d", ev.StreamTruncated, ev.Lossy, ev.ChunksDropped)
	}
	if len(ev.Message.Blocks) != 2 {
		t.Fatalf("blocks = %d, want 2: %+v", len(ev.Message.Blocks), ev.Message.Blocks)
	}
	if b := ev.Message.Blocks[0]; b.Type != "text" || b.Text != "Hello world" {
		t.Fatalf("text block = %+v", b)
	}
	if b := ev.Message.Blocks[1]; b.Type != "tool_use" || b.Name != "Bash" || b.Text != `{"command":"ls"}` {
		t.Fatalf("tool block = %+v", b)
	}
	if ev.Usage == nil || ev.Usage.InputTokens == nil || *ev.Usage.InputTokens != 25 ||
		ev.Usage.OutputTokens == nil || *ev.Usage.OutputTokens != 50 ||
		ev.Usage.CacheReadInput == nil || *ev.Usage.CacheReadInput != 100 ||
		ev.Usage.CacheCreationInput == nil || *ev.Usage.CacheCreationInput != 7 {
		t.Fatalf("usage = %+v", ev.Usage)
	}
	if ev.Bytes != int64(len(respSSEFixture())) {
		t.Fatalf("bytes = %d, want %d", ev.Bytes, len(respSSEFixture()))
	}
}

// ---- 2: multi-block, budget clip, block cap ----

func TestRespAggMultiBlockAndBudget(t *testing.T) {
	respTest(t)
	tapMaxText = 8
	tapMaxBlocks = 2
	body := `data: {"type":"message_start","message":{"id":"m","model":"x","usage":{}}}` + "\n\n" +
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}` + "\n\n" +
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"pondering deeply"}}` + "\n\n" +
		`data: {"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}` + "\n\n" +
		`data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"a long answer that will clip"}}` + "\n\n" +
		`data: {"type":"content_block_start","index":2,"content_block":{"type":"text","text":""}}` + "\n\n" +
		`data: {"type":"content_block_delta","index":2,"delta":{"type":"text_delta","text":"third"}}` + "\n\n" +
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}` + "\n\n" +
		`data: {"type":"message_stop"}` + "\n\n"
	frames := respDrive(t, respIdentFixture(), 200, "text/event-stream", body, 23)

	ev := respDecodeMsg(t, respOne(t, frames, "assistant_message"))
	if len(ev.Message.Blocks) != 2 || ev.BlocksElided != 1 {
		t.Fatalf("blocks=%d elided=%d, want 2/1", len(ev.Message.Blocks), ev.BlocksElided)
	}
	// Thinking renders TEXT under budget — the request side does (tap.go's
	// thinking case), and the two sides' block shape must not drift.
	think := ev.Message.Blocks[0]
	if think.Type != "thinking" || think.Text != "ponderin" /* tapMaxText=8 */ || !think.Clipped || think.Bytes != len("pondering deeply") {
		t.Fatalf("thinking block = %+v", think)
	}
	text := ev.Message.Blocks[1]
	if text.Type != "text" || !text.Clipped || len(text.Text) > tapMaxText {
		t.Fatalf("text block = %+v", text)
	}
	if !ev.Truncated {
		t.Fatal("clipped+elided aggregate not marked truncated")
	}
}

// ---- 3: non-streaming JSON produces the same event shape ----

func TestRespAggNonStreamingJSONShapeIdentity(t *testing.T) {
	respTest(t)
	sseFrames := respDrive(t, respIdentFixture(), 200, "text/event-stream", respSSEFixture(), 37)
	sseRaw := respOne(t, sseFrames, "assistant_message")

	jsonFrames := respDrive(t, respIdentFixture(), 200, "application/json", respJSONFixture, 41)
	jsonRaw := respOne(t, jsonFrames, "assistant_message")

	keys := func(raw json.RawMessage) []string {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	if got, want := keys(jsonRaw), keys(sseRaw); !equalStrings(got, want) {
		t.Fatalf("JSON-derived key set %v != SSE-derived %v — the acceptance criterion is one shape", got, want)
	}

	ev := respDecodeMsg(t, jsonRaw)
	if ev.SSE {
		t.Fatal("non-streaming response marked sse")
	}
	if len(ev.Message.Blocks) != 2 || ev.Message.Blocks[0].Text != "Hello world" ||
		ev.Message.Blocks[1].Text != `{"command":"ls"}` {
		t.Fatalf("JSON blocks = %+v", ev.Message.Blocks)
	}
	if ev.ModelStream != "claude-fable-5-upstream" || ev.StopReason != "tool_use" {
		t.Fatalf("model_stream=%q stop_reason=%q", ev.ModelStream, ev.StopReason)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---- 4: error envelopes ----

func TestRespAggErrorEnvelope(t *testing.T) {
	respTest(t)
	body := `{"type":"error","error":{"type":"proxy_error","message":"apiary-proxy: upstream failure"}}`
	frames := respDrive(t, respIdentFixture(), 502, "application/json", body, 16)

	respNone(t, frames, "assistant_message")
	ev := respDecodeEnd(t, respOne(t, frames, "turn_ended"))
	if ev.Reason != "error" || ev.Status != 502 || ev.ErrorType != "proxy_error" {
		t.Fatalf("turn_ended = %+v", ev)
	}
	if !strings.Contains(ev.ErrorMessage, "upstream failure") {
		t.Fatalf("error_message = %q", ev.ErrorMessage)
	}

	// The SSE error event terminal, same contract.
	sse := `data: {"type":"message_start","message":{"id":"m","model":"x"}}` + "\n\n" +
		`data: {"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}` + "\n\n"
	frames = respDrive(t, respIdentFixture(), 200, "text/event-stream", sse, 19)
	respNone(t, frames, "assistant_message")
	ev = respDecodeEnd(t, respOne(t, frames, "turn_ended"))
	if ev.Reason != "error" || ev.ErrorType != "overloaded_error" {
		t.Fatalf("SSE error terminal = %+v", ev)
	}
}

// ---- 5: usage presence is preserved, never invented (S8) ----

func TestRespAggUsageUnknownPreserved(t *testing.T) {
	respTest(t)
	// A truncated-close-shaped stream: in/out present (as zeros), cache fields
	// ABSENT — absent means UNKNOWN downstream (codex.go's contract) and the tap
	// must republish exactly that, not invent zeros.
	body := `data: {"type":"message_start","message":{"id":"m","model":"gpt-5.2"}}` + "\n\n" +
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}` + "\n\n" +
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial"}}` + "\n\n" +
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":0,"output_tokens":0}}` + "\n\n" +
		`data: {"type":"message_stop"}` + "\n\n"
	frames := respDrive(t, respIdentFixture(), 200, "text/event-stream", body, 29)
	raw := respOne(t, frames, "assistant_message")

	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	var usage map[string]json.RawMessage
	if err := json.Unmarshal(m["usage"], &usage); err != nil {
		t.Fatalf("usage does not decode: %v (raw %s)", err, m["usage"])
	}
	for _, want := range []string{"input_tokens", "output_tokens"} {
		if _, has := usage[want]; !has {
			t.Fatalf("usage lost the wire's explicit %s: %s", want, m["usage"])
		}
	}
	// raw_output_tokens too: no codex hop reported an output count for it.
	for _, absent := range []string{"cache_read_input_tokens", "cache_creation_input_tokens", "raw_output_tokens"} {
		if _, has := usage[absent]; has {
			t.Fatalf("usage INVENTED %s — absent means unknown, an explicit zero is a lie: %s", absent, m["usage"])
		}
	}

	// And a stream with no usage anywhere omits the whole field.
	body = `data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}` + "\n\n" +
		`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}` + "\n\n" +
		`data: {"type":"message_stop"}` + "\n\n"
	frames = respDrive(t, respIdentFixture(), 200, "text/event-stream", body, 29)
	raw = respOne(t, frames, "assistant_message")
	m = nil // Unmarshal into a non-nil map MERGES — a fresh map or the stale usage key survives
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if _, has := m["usage"]; has {
		t.Fatalf("usage key present on a stream that never carried usage: %s", m["usage"])
	}
}

// ---- 6: any drop poisons the aggregate (S3) ----

func TestRespAggLossyDropPublishesTurnEnded(t *testing.T) {
	respTest(t)
	id := respIdentFixture()
	drain := tapListen(t)

	// Build the tap WITHOUT its aggregator so the drop is deterministic: two chunks
	// that form a COMPLETE, valid stream fit the channel; the third is dropped.
	// If dropped>0 did not poison the aggregate, this would publish a clean,
	// plausible assistant_message — the exact corruption S3 forbids.
	rt := &respTap{ch: make(chan []byte, 2), done: make(chan struct{}), id: id}
	hdr := respHeader("text/event-stream")
	whole := respSSEFixture()
	half := len(whole) / 2
	rt.feed([]byte(whole[:half]), 200, hdr)
	rt.feed([]byte(whole[half:]), 200, hdr)
	rt.feed([]byte("data: {\"type\":\"ping\"}\n\n"), 200, hdr) // channel full -> dropped
	if rt.dropped.Load() != 1 {
		t.Fatalf("dropped = %d, want 1 (the test harness itself is wrong)", rt.dropped.Load())
	}

	respTapActive.Add(1) // hand-spawned aggregator honors newRespTap's gauge contract
	go rt.aggregate()
	rt.finish()
	respWaitIdle(t)

	frames := drain()
	respNone(t, frames, "assistant_message")
	ev := respDecodeEnd(t, respOne(t, frames, "turn_ended"))
	if ev.Reason != "tap_lossy" || ev.ChunksDropped != 1 {
		t.Fatalf("turn_ended = %+v, want reason tap_lossy dropped 1", ev)
	}
	if respTapLossy.Load() != 1 {
		t.Fatalf("lossy counter = %d", respTapLossy.Load())
	}
}

// ---- 7: drain applies after a clean terminal too (S3's second half) ----

func TestRespAggDrainsAfterMessageStop(t *testing.T) {
	respTest(t)
	drain := tapListen(t)
	rt := newRespTap(respIdentFixture())
	hdr := respHeader("text/event-stream")
	rt.feed([]byte(respSSEFixture()), 200, hdr)

	// Wait for the terminal so the feeds below land on an aggregator that has
	// already exited its parse loop and is (or will be) draining.
	deadline := time.Now().Add(5 * time.Second)
	for respTapMessages.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}

	start := time.Now()
	for i := 0; i < 500; i++ {
		rt.feed([]byte("data: {\"type\":\"ping\"}\n\n"), 200, hdr)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("500 post-terminal feeds took %v — feed blocked behind a dead consumer", elapsed)
	}
	rt.finish()
	respWaitIdle(t)

	frames := drain()
	respOne(t, frames, "assistant_message") // exactly one, despite the flood
	respNone(t, frames, "turn_ended")
}

// ---- 8: the backstop never closes the feed channel (S1/S10) ----

func TestRespAggBackstopNeverClosesFeedChannel(t *testing.T) {
	respTest(t)
	respTapIdleMax = 40 * time.Millisecond
	drain := tapListen(t)
	rt := newRespTap(respIdentFixture())
	hdr := respHeader("text/event-stream")

	// A partial stream, then silence past the idle window.
	rt.feed([]byte(`data: {"type":"message_start","message":{"id":"m","model":"x"}}`+"\n\n"), 200, hdr)
	deadline := time.Now().Add(5 * time.Second)
	for respTapBackstops.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if respTapBackstops.Load() == 0 {
		t.Fatal("backstop never fired")
	}

	// The channel MUST still be open: post-backstop feeds are no-ops, never panics.
	// (A timer-goroutine close would make this send panic the request goroutine —
	// the failure mode S1 exists to forbid.)
	for i := 0; i < 50; i++ {
		rt.feed([]byte("data: {\"type\":\"ping\"}\n\n"), 200, hdr)
	}
	if respTapPanics.Load() != 0 {
		t.Fatalf("panics = %d — a post-backstop feed hit a closed channel", respTapPanics.Load())
	}
	rt.finish()
	respWaitIdle(t)

	frames := drain()
	ev := respDecodeEnd(t, respOne(t, frames, "turn_ended"))
	if ev.Reason != "backstop" {
		t.Fatalf("reason = %q, want backstop", ev.Reason)
	}
}

// The backstop measures from LAST ACTIVITY: a stream trickling inside the window is
// never killed, no matter its total duration.
func TestRespAggBackstopMeasuresLastActivity(t *testing.T) {
	respTest(t)
	respTapIdleMax = 250 * time.Millisecond
	drain := tapListen(t)
	rt := newRespTap(respIdentFixture())
	hdr := respHeader("text/event-stream")

	rt.feed([]byte(`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`+"\n\n"), 200, hdr)
	for i := 0; i < 10; i++ { // 10 x 50ms = 500ms of trickle, double the idle window
		time.Sleep(50 * time.Millisecond)
		rt.feed([]byte(`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"x"}}`+"\n\n"), 200, hdr)
	}
	rt.feed([]byte(`data: {"type":"message_stop"}`+"\n\n"), 200, hdr)
	rt.finish()
	respWaitIdle(t)

	if respTapBackstops.Load() != 0 {
		t.Fatal("backstop fired on a stream that never went idle — it is measuring from creation, not last activity")
	}
	frames := drain()
	respOne(t, frames, "assistant_message")
	respNone(t, frames, "turn_ended")
}

// ---- 9: panic containment ----

func TestRespAggPanicContained(t *testing.T) {
	respTest(t)
	drain := tapListen(t)
	respTapHook = func() { panic("synthetic aggregator failure") }
	rt := newRespTap(respIdentFixture())
	hdr := respHeader("text/event-stream")
	rt.feed([]byte(respSSEFixture()), 200, hdr) // feeds must survive a dead aggregator
	rt.finish()
	respWaitIdle(t) // the drain still ran — a panicked aggregator does not leak

	if respTapPanics.Load() == 0 {
		t.Fatal("panic not counted")
	}
	frames := drain()
	respNone(t, frames, "assistant_message")
	ev := respDecodeEnd(t, respOne(t, frames, "turn_ended"))
	if ev.Reason != "tap_panic" {
		t.Fatalf("reason = %q, want tap_panic", ev.Reason)
	}
}

// respTapRecover is feed's guard; prove the mechanism itself (feed has no organic
// panic path to trigger — that absence is the design, this is the belt).
func TestRespFeedRecoverLatchesDead(t *testing.T) {
	respTest(t)
	rt := &respTap{ch: make(chan []byte, 1), done: make(chan struct{})}
	func() {
		defer respTapRecover(rt)
		panic("synthetic feed failure")
	}()
	if !rt.dead.Load() {
		t.Fatal("recover did not latch dead")
	}
	if respTapPanics.Load() != 1 {
		t.Fatalf("panics = %d", respTapPanics.Load())
	}
	rt.feed([]byte("x"), 200, respHeader("text/event-stream"))
	if len(rt.ch) != 0 {
		t.Fatal("a dead tap still enqueued")
	}
}

// ---- 10: feed never blocks, and costs one allocation ----

func TestRespFeedNeverBlocksAndAllocs(t *testing.T) {
	respTest(t)
	full := &respTap{ch: make(chan []byte, 1), done: make(chan struct{})}
	hdr := respHeader("text/event-stream")
	chunk := []byte("data: {\"type\":\"ping\"}\n\n")
	full.feed(chunk, 200, hdr) // fills the channel

	start := time.Now()
	for i := 0; i < 10000; i++ {
		full.feed(chunk, 200, hdr)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("10k feeds against a full channel took %v — feed blocked", elapsed)
	}
	if full.dropped.Load() != 10000 {
		t.Fatalf("dropped = %d, want 10000", full.dropped.Load())
	}

	// Steady state (post-latch, channel with room): exactly the mandatory copy.
	roomy := &respTap{ch: make(chan []byte, 1<<16), done: make(chan struct{})}
	roomy.feed(chunk, 200, hdr) // latch outside the measured loop
	n := testing.AllocsPerRun(1000, func() {
		roomy.feed(chunk, 200, hdr)
	})
	if n > 1 {
		t.Fatalf("feed allocates %.1f times per chunk on the live inference path, budget is 1 (the copy)", n)
	}
}

// ---- 11: the cold path still costs nothing ----

func TestRespTapColdPathNoAlloc(t *testing.T) {
	respTest(t)
	evDisarm()
	body := []byte(tapConvo("hello", 6))
	ctx := tapLaneCtx("default")
	r := httptest.NewRequest("POST", "/v1/messages", nil)
	r.Header.Set("x-claude-code-session-id", "sess-cold-resp")

	var id tapRespIdent
	n := testing.AllocsPerRun(1000, func() {
		id = tapTurn(ctx, r, body, "claude-fable-1", " ROUTED [acct] lane=default", "acct", "deadbeef")
	})
	if n != 0 {
		t.Fatalf("cold tapTurn allocated %.1f times — returning the identity must not cost the idle proxy anything", n)
	}
	if id.ok {
		t.Fatal("unarmed hub produced a tappable identity — every request would spawn an aggregator")
	}
	if respTapActive.Load() != 0 || respTapStarts.Load() != 0 {
		t.Fatal("cold path touched tap state")
	}
}

// ---- 12: the hot-path guard (S5, extended) ----

// The mechanism that protects live inference is a SHAPE: recorder stays two methods,
// the tee is one call in one wrapper method, feed is a guarded copy plus a
// select-with-default. This test pins that shape with go/parser so it cannot erode
// in a refactor without a deliberate edit here.
func TestRecorderHotPathGuard(t *testing.T) {
	fset := token.NewFileSet()
	methods := map[string]map[string]*ast.FuncDecl{}
	for _, fname := range []string{"main.go", "resptap.go"} {
		f, err := parser.ParseFile(fset, fname, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", fname, err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Recv == nil || len(fd.Recv.List) == 0 {
				continue
			}
			recv := ""
			switch rt := fd.Recv.List[0].Type.(type) {
			case *ast.StarExpr:
				if id, ok := rt.X.(*ast.Ident); ok {
					recv = id.Name
				}
			case *ast.Ident:
				recv = rt.Name
			}
			if recv == "" {
				continue
			}
			if methods[recv] == nil {
				methods[recv] = map[string]*ast.FuncDecl{}
			}
			methods[recv][fd.Name.Name] = fd
		}
	}

	names := func(recv string) []string {
		out := []string{}
		for n := range methods[recv] {
			out = append(out, n)
		}
		sort.Strings(out)
		return out
	}
	if got := names("recorder"); !equalStrings(got, []string{"Flush", "WriteHeader"}) {
		t.Fatalf("recorder methods = %v — recorder itself must NEVER grow Write; the tee lives on tapRecorder so SWITCH_TAP=off removes it entirely", got)
	}
	if got := names("tapRecorder"); !equalStrings(got, []string{"Write"}) {
		t.Fatalf("tapRecorder methods = %v, want exactly [Write]", got)
	}

	// Flush and WriteHeader run under maxLatencyWriter's mutex — Flush on a TIMER
	// goroutine — and must never touch tap state.
	for _, m := range []string{"Flush", "WriteHeader"} {
		ast.Inspect(methods["recorder"][m], func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok {
				low := strings.ToLower(id.Name)
				if strings.Contains(low, "tap") || strings.Contains(low, "feed") {
					t.Fatalf("recorder.%s references %q — Flush runs on a timer goroutine; tap state there is a race", m, id.Name)
				}
			}
			return true
		})
	}

	// tapRecorder.Write: small, exactly one feed call, no locks, no goroutines.
	wr := methods["tapRecorder"]["Write"]
	if got := len(wr.Body.List); got > 10 {
		t.Fatalf("tapRecorder.Write is %d statements, budget 10", got)
	}
	feeds := 0
	ast.Inspect(wr, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.CallExpr:
			if sel, ok := v.Fun.(*ast.SelectorExpr); ok {
				switch sel.Sel.Name {
				case "feed":
					feeds++
				case "Lock", "RLock", "Wait":
					t.Fatalf("tapRecorder.Write takes a lock (%s) on the inference path", sel.Sel.Name)
				}
			}
		case *ast.GoStmt, *ast.SelectStmt:
			t.Fatalf("tapRecorder.Write contains %T — the tee is one plain call", v)
		}
		return true
	})
	if feeds != 1 {
		t.Fatalf("tapRecorder.Write calls feed %d times, want exactly 1", feeds)
	}

	// respTap.feed: first statement defers the recover guard; exactly one send,
	// inside exactly one select that has a default clause (never a bare send —
	// a bare send on this path is a block or a panic waiting to happen).
	feed := methods["respTap"]["feed"]
	first, ok := feed.Body.List[0].(*ast.DeferStmt)
	if !ok {
		t.Fatalf("feed's first statement is %T, want the deferred recover guard", feed.Body.List[0])
	}
	if id, ok := first.Call.Fun.(*ast.Ident); !ok || id.Name != "respTapRecover" {
		t.Fatalf("feed's deferred guard is %v, want respTapRecover", first.Call.Fun)
	}
	sends, selects, defaults := 0, 0, 0
	ast.Inspect(feed, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.SendStmt:
			sends++
		case *ast.SelectStmt:
			selects++
			for _, c := range v.Body.List {
				if cc, ok := c.(*ast.CommClause); ok && cc.Comm == nil {
					defaults++
				}
			}
		case *ast.CallExpr:
			if sel, ok := v.Fun.(*ast.SelectorExpr); ok {
				switch sel.Sel.Name {
				case "Lock", "RLock", "Wait":
					t.Fatalf("feed takes a lock (%s)", sel.Sel.Name)
				}
			}
		}
		return true
	})
	if sends != 1 || selects != 1 || defaults != 1 {
		t.Fatalf("feed has %d sends / %d selects / %d defaults, want 1/1/1 (one non-blocking send, nothing else)", sends, selects, defaults)
	}
}

// ---- content-encoding: the wire carries what the CLIENT negotiated ----
//
// Live lesson: every real CLI offers compression and Anthropic's edge
// gzips SSE when offered, so the tee hands the aggregator gzip bytes — the plain
// fixtures above never see production's actual wire. These prove the aggregator
// decodes for itself and stays honest about encodings it cannot decode.

func respGzip(t *testing.T, s string) []byte {
	t.Helper()
	var b bytes.Buffer
	zw := gzip.NewWriter(&b)
	if _, err := zw.Write([]byte(s)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

// respDriveEnc is respDrive for a body that is not the wire's plaintext: raw is fed
// exactly as given and hdr carries the Content-Encoding the aggregator must undo.
func respDriveEnc(t *testing.T, id tapRespIdent, status int, ct, enc string, raw []byte, chunkSize int) []tapFrame {
	t.Helper()
	drain := tapListen(t)
	rt := newRespTap(id)
	hdr := respHeader(ct)
	if enc != "" {
		hdr.Set("Content-Encoding", enc)
	}
	for len(raw) > 0 {
		n := chunkSize
		if n > len(raw) {
			n = len(raw)
		}
		rt.feed(raw[:n], status, hdr)
		raw = raw[n:]
	}
	rt.finish()
	respWaitIdle(t)
	return drain()
}

func TestRespTapGzipSSE(t *testing.T) {
	respTest(t)
	id := respIdentFixture()
	frames := respDriveEnc(t, id, 200, "text/event-stream", "gzip", respGzip(t, respSSEFixture()), 37)

	respNone(t, frames, "turn_ended")
	ev := respDecodeMsg(t, respOne(t, frames, "assistant_message"))
	if ev.StreamTruncated {
		t.Fatal("complete gzip stream reported stream_truncated")
	}
	if len(ev.Message.Blocks) != 2 || ev.Message.Blocks[0].Text != "Hello world" {
		t.Fatalf("gzip aggregate differs from the plain fixture's: %+v", ev.Message.Blocks)
	}
	if ev.StopReason != "tool_use" {
		t.Fatalf("stop_reason %q, want tool_use", ev.StopReason)
	}
}

func TestRespTapDeflateSSE(t *testing.T) {
	respTest(t)
	var b bytes.Buffer
	zw := zlib.NewWriter(&b)
	if _, err := zw.Write([]byte(respSSEFixture())); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	frames := respDriveEnc(t, respIdentFixture(), 200, "text/event-stream", "deflate", b.Bytes(), 41)

	respNone(t, frames, "turn_ended")
	ev := respDecodeMsg(t, respOne(t, frames, "assistant_message"))
	if len(ev.Message.Blocks) != 2 || ev.Message.Blocks[0].Text != "Hello world" {
		t.Fatalf("deflate aggregate differs from the plain fixture's: %+v", ev.Message.Blocks)
	}
}

func TestRespTapGzipJSON(t *testing.T) {
	respTest(t)
	frames := respDriveEnc(t, respIdentFixture(), 200, "application/json", "gzip", respGzip(t, respJSONFixture), 29)

	respNone(t, frames, "turn_ended")
	ev := respDecodeMsg(t, respOne(t, frames, "assistant_message"))
	if ev.SSE {
		t.Fatal("JSON body reported sse:true")
	}
	if len(ev.Message.Blocks) != 2 || ev.Message.Blocks[0].Text != "Hello world" {
		t.Fatalf("gzip JSON aggregate: %+v", ev.Message.Blocks)
	}
}

func TestRespTapUnsupportedEncoding(t *testing.T) {
	respTest(t)
	frames := respDriveEnc(t, respIdentFixture(), 200, "text/event-stream", "br", []byte("(brotli bytes)"), 8)

	respNone(t, frames, "assistant_message")
	ev := respDecodeEnd(t, respOne(t, frames, "turn_ended"))
	if ev.Reason != "content_encoding" {
		t.Fatalf("reason %q, want content_encoding — compressed bytes must never be called unparseable", ev.Reason)
	}
	if !strings.Contains(ev.ErrorMessage, "br") {
		t.Fatalf("error_message %q does not name the encoding", ev.ErrorMessage)
	}
	if respTapMessages.Load() != 0 {
		t.Fatal("an undecodable stream published a message")
	}
}

func TestRespTapGzipCorruptHeader(t *testing.T) {
	respTest(t)
	// Claims gzip, carries plaintext: NewReader fails on the magic bytes.
	frames := respDriveEnc(t, respIdentFixture(), 200, "text/event-stream", "gzip", []byte(respSSEFixture()), 37)

	respNone(t, frames, "assistant_message")
	ev := respDecodeEnd(t, respOne(t, frames, "turn_ended"))
	if ev.Reason != "unparseable" || ev.ErrorMessage == "" {
		t.Fatalf("reason %q / error %q, want unparseable with the decoder's error", ev.Reason, ev.ErrorMessage)
	}
}

func TestRespTapGzipTruncatedStream(t *testing.T) {
	respTest(t)
	full := respSSEFixture()
	cutStr := strings.Index(full, `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"world"}}`)
	if cutStr < 0 {
		t.Fatal("fixture drifted: second text delta not found")
	}
	// Compress the prefix, Flush so its deflate block is complete on the wire, then
	// cut there: exactly what a client abort mid-stream leaves behind — decodable
	// prefix, no gzip trailer.
	var b bytes.Buffer
	zw := gzip.NewWriter(&b)
	if _, err := zw.Write([]byte(full[:cutStr])); err != nil {
		t.Fatal(err)
	}
	if err := zw.Flush(); err != nil {
		t.Fatal(err)
	}
	frames := respDriveEnc(t, respIdentFixture(), 200, "text/event-stream", "gzip", b.Bytes(), 23)

	respNone(t, frames, "turn_ended")
	ev := respDecodeMsg(t, respOne(t, frames, "assistant_message"))
	if !ev.StreamTruncated {
		t.Fatal("truncated gzip stream not marked stream_truncated")
	}
	if len(ev.Message.Blocks) == 0 || ev.Message.Blocks[0].Text != "Hello " {
		t.Fatalf("partial aggregate %+v, want the pre-cut text", ev.Message.Blocks)
	}
}

// The assistant's tool_use carries its id on both response paths (SSE and
// whole JSON), and a server tool's result carries the tool_use_id it answers,
// so a reader can pair a later tool_result with its tool's name.
func TestRespAggToolBlocksCarryTheirIDs(t *testing.T) {
	respTest(t)
	sse := respDecodeMsg(t, respOne(t, respDrive(t, respIdentFixture(), 200, "text/event-stream", respSSEFixture(), 37), "assistant_message"))
	if b := sse.Message.Blocks[1]; b.Type != "tool_use" || b.ID != "toolu_01" || b.Name != "Bash" {
		t.Fatalf("SSE tool block = %+v, want id=toolu_01", b)
	}
	if b := sse.Message.Blocks[0]; b.ID != "" || b.ToolUseID != "" {
		t.Fatalf("SSE text block carries ids: %+v", b)
	}
	js := respDecodeMsg(t, respOne(t, respDrive(t, respIdentFixture(), 200, "application/json", respJSONFixture, 41), "assistant_message"))
	if b := js.Message.Blocks[1]; b.Type != "tool_use" || b.ID != "toolu_01" {
		t.Fatalf("JSON tool block = %+v, want id=toolu_01", b)
	}

	body := `data: {"type":"message_start","message":{"id":"m","model":"x","usage":{}}}` + "\n\n" +
		`data: {"type":"content_block_start","index":0,"content_block":{"type":"server_tool_use","id":"srvtoolu_9","name":"web_search","input":{}}}` + "\n\n" +
		`data: {"type":"content_block_stop","index":0}` + "\n\n" +
		`data: {"type":"content_block_start","index":1,"content_block":{"type":"web_search_tool_result","tool_use_id":"srvtoolu_9","content":[]}}` + "\n\n" +
		`data: {"type":"content_block_stop","index":1}` + "\n\n" +
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}` + "\n\n" +
		`data: {"type":"message_stop"}` + "\n\n"
	ev := respDecodeMsg(t, respOne(t, respDrive(t, respIdentFixture(), 200, "text/event-stream", body, 29), "assistant_message"))
	if len(ev.Message.Blocks) != 2 {
		t.Fatalf("blocks = %+v", ev.Message.Blocks)
	}
	if b := ev.Message.Blocks[0]; b.ID != "srvtoolu_9" || b.Name != "web_search" {
		t.Fatalf("server_tool_use = %+v", b)
	}
	if b := ev.Message.Blocks[1]; b.ToolUseID != "srvtoolu_9" || b.ID != "" {
		t.Fatalf("web_search_tool_result = %+v", b)
	}
}

// respSpeedOf is a response-side frame's speed, failing when the key is absent:
// a consumer reads an absent speed as a proxy older than the field.
func respSpeedOf(t *testing.T, f tapFrame) string {
	t.Helper()
	var ev map[string]json.RawMessage
	if err := json.Unmarshal(f.raw, &ev); err != nil {
		t.Fatalf("%s does not decode: %v", f.kind, err)
	}
	raw, ok := ev["speed"]
	if !ok {
		t.Fatalf("%s has no speed key: %s", f.kind, f.raw)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("%s speed is not a string: %s", f.kind, raw)
	}
	return s
}

// armedCap is a capture its served hop armed at speed (nil: a hop that names none,
// as an image hop does), the way noteServedCap arms it.
func armedCap(speed *string) *servedCap {
	c := &servedCap{}
	c.request("claude-fable-5", reqFacts{Speed: "fast"}, true)
	c.hop("acct", 200, func(string) servedHopFacts { return servedHopFacts{kind: servedPassthrough, speed: speed} })
	return c
}

// Every response-side event (turn_started, assistant_message, turn_ended) names the
// speed the served hop ran at, read off the request's capture: the armed hop's own
// speed, and "unknown" for a hop that names none (an image), a capture no hop armed
// (an error) or no capture at all. The key is never left out.
func TestRespIdentCarriesTheServedSpeed(t *testing.T) {
	respTest(t)
	std, fast := "standard", "fast"
	unarmed := &servedCap{}
	unarmed.request("claude-fable-5", reqFacts{Speed: "fast"}, true)
	unarmed.hop("acct", 502, func(string) servedHopFacts { t.Fatal("an error armed the capture"); return servedHopFacts{} })
	errBody := `{"type":"error","error":{"type":"proxy_error","message":"apiary-proxy: upstream failure"}}`
	for _, tc := range []struct {
		label    string
		cap      *servedCap
		want     string
		status   int
		ct, body string
	}{
		{"fast hop", armedCap(&fast), "fast", 200, "text/event-stream", respSSEFixture()},
		{"standard hop asked fast", armedCap(&std), "standard", 200, "text/event-stream", respSSEFixture()},
		{"image hop", armedCap(nil), "unknown", 200, "text/event-stream", respSSEFixture()},
		{"error", unarmed, "unknown", 502, "application/json", errBody},
		{"no capture", nil, "unknown", 200, "text/event-stream", respSSEFixture()},
	} {
		id := respIdentFixture()
		id.cap = tc.cap
		frames := respDrive(t, id, tc.status, tc.ct, tc.body, 37)
		seen := 0
		for _, f := range frames {
			switch f.kind {
			case "turn_started", "assistant_message", "turn_ended":
				seen++
				if got := respSpeedOf(t, f); got != tc.want {
					t.Fatalf("%s: %s speed = %q, want %q", tc.label, f.kind, got, tc.want)
				}
			}
		}
		if seen < 2 {
			t.Fatalf("%s: %d response-side events, want at least 2 (saw %v)", tc.label, seen, respKinds(frames))
		}
	}
}

// The request-side turn event names no speed (the hop is not known yet), and a
// resend's identity carries the request's capture, so its turn_started names the
// served hop's speed though the resend published no turn event.
func TestTapTurnLeavesSpeedToTheResponse(t *testing.T) {
	respTest(t)
	drain := tapListen(t)
	fast := "fast"
	c := armedCap(&fast)
	ctx := context.WithValue(withHarness(tapLaneCtx("default"), harnessClaudeCode), servedCapKey{}, c)
	post := func() tapRespIdent {
		r := httptest.NewRequest("POST", "/v1/messages", nil)
		r.Header.Set("x-claude-code-session-id", "sess-S")
		return tapTurn(ctx, r, []byte(tapConvo("hello", 3)), "claude-fable-1", " ROUTED [acct] lane=default", "acct", "deadbeef")
	}
	if first := post(); first.cap != c {
		t.Fatal("the identity does not carry the request's capture")
	}
	frames := drain()
	if len(frames) != 1 || frames[0].kind != "turn" {
		t.Fatalf("frames %v, want one turn", respKinds(frames))
	}
	if strings.Contains(string(frames[0].raw), `"speed"`) {
		t.Fatalf("the turn event names a speed: %s", frames[0].raw)
	}
	again := post()
	if got := drain(); len(got) != 0 {
		t.Fatalf("an identical resend emitted %d events, want 0", len(got))
	}
	resp := respDrive(t, again, 200, "text/event-stream", respSSEFixture(), 37)
	if got := respSpeedOf(t, tapFrame{kind: "turn_started", raw: respOne(t, resp, "turn_started")}); got != "fast" {
		t.Fatalf("resend's turn_started speed = %q, want fast", got)
	}
}
