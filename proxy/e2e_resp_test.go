package main

// End-to-end tests for the response-side transcript tap (#61) through the real
// handler stack: buildHandler -> ReverseProxy -> tapRecorder tee -> aggregator ->
// hub. The properties only provable above the socket: client bytes are untouched
// by the tee (transparency), incremental SSE delivery survives it (latency), all
// three lanes publish one assistant_message joined to the request-side turn event,
// the kill switch removes the machinery entirely, and a client abort still ends in
// exactly one terminal event with the aggregator gone.
//
// TEARDOWN HAZARD (same as e2e_events_test.go): SSE subscriber streams never end on
// their own and httptest.Server.Close() blocks on them — every stream teardown is
// registered AFTER `defer restore()` so LIFO runs it first.
//
// respTapEnabled is a boot-time switch (SWITCH_TAP is resolved once in main), so
// tests flip it only between proxy generations: Close() joins every handler
// goroutine before the write, the new server starts after it — no unsynchronized
// access, exactly like production where the value never changes mid-serve.

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// respSetTap flips the tap between proxy generations (see the header comment).
func respSetTap(env *e2eEnv, on bool) {
	env.proxy.Close()
	respTapEnabled = on
	env.proxy = httptest.NewServer(buildHandler())
}

func respEvData(t *testing.T, p evParsed) map[string]any {
	t.Helper()
	d, ok := p.env.Data.(map[string]any)
	if !ok {
		t.Fatalf("event %q data is %T, want an object: %s", p.event, p.env.Data, p.raw)
	}
	return d
}

// respFirstBlockText digs message.blocks[0].text out of an assistant_message payload.
func respFirstBlockText(t *testing.T, d map[string]any) string {
	t.Helper()
	msg, _ := d["message"].(map[string]any)
	blocks, _ := msg["blocks"].([]any)
	if len(blocks) == 0 {
		t.Fatalf("assistant_message has no blocks: %v", d)
	}
	first, _ := blocks[0].(map[string]any)
	text, _ := first["text"].(string)
	return text
}

// respPost is postStreamSess presenting the test gate secret (these tests arm the
// client gate for their hub subscriber, so "Bearer CLIENT" would bounce with 401).
func respPost(t *testing.T, env *e2eEnv, model string, stream bool, session string) *http.Response {
	t.Helper()
	body := fmt.Sprintf(`{"model":%q,"max_tokens":16,"stream":%v,"messages":[{"role":"user","content":"hi"}]}`, model, stream)
	req, _ := http.NewRequest("POST", env.proxy.URL+"/v1/messages", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer sekrit")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("anthropic-beta", "oauth-2025-04-20")
	if session != "" {
		req.Header.Set("x-claude-code-session-id", session)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request failed: %v", err)
	}
	return resp
}

func respReadAll(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading client body: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	return b
}

// ---- 13: byte transparency — the tee never changes what the client receives ----

// The provider lanes are class members served at r_p 1.0: the usage scaler is
// another test's subject, and byte transparency is about the tee alone.
func TestE2ERespTapByteTransparency(t *testing.T) {
	h, restore := respClassHarness(t)
	defer restore()
	withClientSecret(t, h.e2eEnv, "sekrit")
	h.anthSSE.Store(respSSEFixture())

	post := func(model string, stream bool, sess string) []byte {
		return respReadAll(t, respPost(t, h.e2eEnv, model, stream, sess))
	}

	// Leg A: tap on AND armed — the aggregator actually runs on every response.
	msgs0 := respTapMessages.Load()
	_, frames, closeStream := evStream(t, h.e2eEnv, "sekrit", "")
	evHello(t, frames)
	aSSE := post("claude-fable-5", true, "s13-sse")
	h.bindTo.Store("kimi/kimi-k3")
	aJSON := post("claude-frontier", false, "s13-json")
	h.bindTo.Store("gpt/gpt-5.6")
	aCodex := post("claude-frontier", true, "s13-codex")
	evWaitFor(t, "three aggregated messages", 3*time.Second, func() bool {
		return respTapMessages.Load() >= msgs0+3
	})
	closeStream()

	// Leg B: booted with the tap off — hub unarmed, Write override absent.
	respSetTap(h.e2eEnv, false)
	defer respSetTap(h.e2eEnv, true)
	starts0 := respTapStarts.Load()
	bSSE := post("claude-fable-5", true, "s13-sse")
	bJSON := post("claude-frontier", false, "s13-json")  // still bound to kimi
	bCodex := post("claude-frontier", true, "s13-codex") // still bound to gpt
	if respTapStarts.Load() != starts0 {
		t.Fatal("tap ran with the switch off")
	}

	if !bytes.Equal(aSSE, []byte(respSSEFixture())) {
		t.Fatalf("tapped SSE body != upstream script:\n got %q", aSSE)
	}
	if !bytes.Equal(aJSON, []byte(`{"type":"message","content":[{"type":"text","text":"hosted ok"}]}`)) {
		t.Fatalf("tapped JSON body changed: %q", aJSON)
	}
	for _, c := range []struct {
		name string
		a, b []byte
	}{{"sse", aSSE, bSSE}, {"json", aJSON, bJSON}, {"codex", aCodex, bCodex}} {
		if !bytes.Equal(c.a, c.b) {
			t.Errorf("%s: tap-on bytes differ from tap-off bytes:\n on  %q\n off %q", c.name, c.a, c.b)
		}
	}
}

// ---- 14/15: all three lanes publish one assistant_message joined to its turn ----

// respClassHarness serves kimi and gpt as usable members of the fixture class
// at r_p 1.0; a test picks which one a NEW session binds with bindTo.
func respClassHarness(t *testing.T) (*e2eHosted, func()) {
	t.Helper()
	h, restore := newE2EClass(t)
	h.serveMember(t, "gpt/gpt-5.6", 1.0)
	h.serveMember(t, "kimi/kimi-k3", 1.0)
	return h, restore
}

// respDriveLane posts one request and awaits its turn / turn_started /
// assistant_message trio, asserting the join keys and the aggregated content.
func respDriveLane(t *testing.T, env *e2eEnv, frames <-chan string, model string, stream bool, sess, wantText string, wantTranslated bool) {
	t.Helper()
	respReadAll(t, respPost(t, env, model, stream, sess))

	turn := respEvData(t, evAwait(t, frames, "turn", 3*time.Second))
	started := respEvData(t, evAwait(t, frames, "turn_started", 3*time.Second))
	msg := respEvData(t, evAwait(t, frames, "assistant_message", 3*time.Second))

	ck, _ := turn["conversation_key"].(string)
	if ck == "" || started["conversation_key"] != ck || msg["conversation_key"] != ck {
		t.Fatalf("%s: conversation_key does not join: turn=%v started=%v msg=%v",
			model, turn["conversation_key"], started["conversation_key"], msg["conversation_key"])
	}
	// The reply's index IS the request's message count — Scry's byIndex upsert
	// composes with the next request re-sending the reply at exactly this index.
	if msg["index"] != turn["total"] {
		t.Fatalf("%s: assistant index %v != turn total %v", model, msg["index"], turn["total"])
	}
	if started["status"] != float64(200) || started["sse"] != stream || msg["sse"] != stream {
		t.Fatalf("%s: started status=%v sse=%v msg sse=%v want status 200 sse=%v",
			model, started["status"], started["sse"], msg["sse"], stream)
	}
	if turn["translated"] != wantTranslated || msg["translated"] != wantTranslated {
		t.Fatalf("%s: translated turn=%v msg=%v, want %v", model, turn["translated"], msg["translated"], wantTranslated)
	}
	if got := respFirstBlockText(t, msg); got != wantText {
		t.Fatalf("%s: first block text %q, want %q", model, got, wantText)
	}
	if tr, _ := msg["stream_truncated"].(bool); tr {
		t.Fatalf("%s: clean stream marked stream_truncated", model)
	}
}

func TestE2ERespTapThreeLanesStreamed(t *testing.T) {
	h, restore := respClassHarness(t)
	defer restore()
	withClientSecret(t, h.e2eEnv, "sekrit")
	h.anthSSE.Store(respSSEFixture())
	h.kimiSSE.Store(respSSEFixture())

	_, frames, closeStream := evStream(t, h.e2eEnv, "sekrit", "")
	defer closeStream()
	evHello(t, frames)

	respDriveLane(t, h.e2eEnv, frames, "claude-fable-5", true, "s14-a", "Hello world", false)
	h.bindTo.Store("kimi/kimi-k3")
	respDriveLane(t, h.e2eEnv, frames, "claude-frontier", true, "s14-k", "Hello world", false)
	// The gpt lane serves Anthropic SSE the proxy itself authored from the codex
	// wire — the ONE lane whose response never existed at the upstream, hence
	// translated:true (Scry stops inferring this from model names).
	h.bindTo.Store("gpt/gpt-5.6")
	respDriveLane(t, h.e2eEnv, frames, "claude-frontier", true, "s14-g", "codex ok", true)
}

func TestE2ERespTapThreeLanesNonStreaming(t *testing.T) {
	h, restore := respClassHarness(t)
	defer restore()
	withClientSecret(t, h.e2eEnv, "sekrit")
	// anthSSE / kimiSSE unset: both fakes answer plain JSON. The codex upstream
	// still streams, but aggregateCodexSSE hands the client ONE JSON message —
	// the tap must see all three as its JSON mode.
	_, frames, closeStream := evStream(t, h.e2eEnv, "sekrit", "")
	defer closeStream()
	evHello(t, frames)

	respDriveLane(t, h.e2eEnv, frames, "claude-fable-5", false, "s15-a", "ok", false)
	h.bindTo.Store("kimi/kimi-k3")
	respDriveLane(t, h.e2eEnv, frames, "claude-frontier", false, "s15-k", "hosted ok", false)
	h.bindTo.Store("gpt/gpt-5.6")
	respDriveLane(t, h.e2eEnv, frames, "claude-frontier", false, "s15-g", "codex ok", true)
}

// ---- 16: the tee does not buffer — incremental delivery, measured ----

func TestE2ERespTapPreservesIncrementalDelivery(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	withClientSecret(t, env, "sekrit")
	script := respSSEFixture()
	gap := 25 * time.Millisecond
	env.anthSSE.Store(script)
	env.anthGap.Store(gap)

	_, frames, closeStream := evStream(t, env, "sekrit", "")
	defer closeStream()
	evHello(t, frames)

	resp := respPost(t, env, "claude-fable-5", true, "s16")
	defer resp.Body.Close()
	// Read frames on THIS goroutine, stamping each as its final newline arrives:
	// arrival timing is the measurement, so no pump goroutine in between.
	br := bufio.NewReader(resp.Body)
	var times []time.Time
	pending := false
	for {
		line, err := br.ReadString('\n')
		switch {
		case line == "\n" && pending:
			times = append(times, time.Now())
			pending = false
		case line != "" && line != "\n":
			pending = true
		}
		if err != nil {
			break
		}
	}

	want := strings.Count(script, "\n\n")
	if len(times) != want {
		t.Fatalf("client saw %d frames, want %d", len(times), want)
	}
	// If the tee (or anything it dragged in) buffered the stream, every frame
	// would land in one burst at the end. The upstream spaces them gap apart, so
	// genuine incremental delivery spreads first-to-last over at least half the
	// upstream's own span — an honest bound that tolerates CI scheduling noise.
	spread := times[len(times)-1].Sub(times[0])
	if minSpread := time.Duration(want-1) * gap / 2; spread < minSpread {
		t.Fatalf("frames arrived in %v, want >= %v — streaming was buffered", spread, minSpread)
	}
	for i := 1; i < len(times); i++ {
		if d := times[i].Sub(times[i-1]); d > time.Second {
			t.Fatalf("frame %d stalled %v behind its predecessor", i, d)
		}
	}
	// And the measured stream still aggregated: one assistant_message, full text.
	msg := respEvData(t, evAwait(t, frames, "assistant_message", 3*time.Second))
	if got := respFirstBlockText(t, msg); got != "Hello world" {
		t.Fatalf("aggregated text %q", got)
	}
}

// ---- 17: SWITCH_TAP off — turn events flow, response machinery is absent ----

func TestE2ERespTapKillSwitch(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	withClientSecret(t, env, "sekrit")
	respSetTap(env, false)
	defer respSetTap(env, true)
	env.anthSSE.Store(respSSEFixture())

	_, frames, closeStream := evStream(t, env, "sekrit", "")
	defer closeStream()
	evHello(t, frames)

	starts0 := respTapStarts.Load()
	respReadAll(t, respPost(t, env, "claude-fable-5", true, "s17"))

	// The request-side tap is a separate circuit and stays live.
	turn := respEvData(t, evAwait(t, frames, "turn", 3*time.Second))
	if turn["conversation_key"] == "" {
		t.Fatal("turn event lost its identity")
	}
	// No response event may follow — bound the wait by publishing a sentinel we
	// KNOW arrives after anything the (nonexistent) aggregator could have sent.
	publish("resp_test_sentinel", map[string]any{})
	for {
		p := evParseFrame(t, []byte(evNext(t, frames, 3*time.Second)))
		if p.event == "resp_test_sentinel" {
			break
		}
		switch p.event {
		case "turn_started", "assistant_message", "turn_ended":
			t.Fatalf("response event %q published with the tap off", p.event)
		}
	}
	if respTapStarts.Load() != starts0 {
		t.Fatal("aggregator ran with the tap off")
	}
}

// ---- 18: client abort mid-stream still ends in one terminal, aggregator exits ----

func TestE2ERespTapClientAbortMidStream(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	withClientSecret(t, env, "sekrit")
	env.anthSSE.Store(respSSEFixture())
	env.anthGap.Store(150 * time.Millisecond) // wide gaps: the abort lands mid-stream for sure

	_, frames, closeStream := evStream(t, env, "sekrit", "")
	defer closeStream()
	evHello(t, frames)

	active0 := respTapActive.Load()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := `{"model":"claude-fable-5","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}`
	req, _ := http.NewRequestWithContext(ctx, "POST", env.proxy.URL+"/v1/messages", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer sekrit")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("anthropic-beta", "oauth-2025-04-20")
	req.Header.Set("x-claude-code-session-id", "s18")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request failed: %v", err)
	}
	// Take the first frame (message_start reached us), then walk away.
	one := make([]byte, 1)
	if _, err := resp.Body.Read(one); err != nil {
		t.Fatalf("first byte never arrived: %v", err)
	}
	cancel()
	resp.Body.Close()

	// The handler's deferred finish() closes the feed on the abort path too, so the
	// aggregator publishes what the client actually got — a truncated message (the
	// first frame carried content) — and exits. Nothing outlives the request.
	msg := respEvData(t, evAwait(t, frames, "assistant_message", 3*time.Second))
	if tr, _ := msg["stream_truncated"].(bool); !tr {
		t.Fatalf("aborted stream not marked stream_truncated: %v", msg)
	}
	evWaitFor(t, "aggregator exit after abort", 3*time.Second, func() bool {
		return respTapActive.Load() <= active0
	})
}

// ---- 19: encoded upstream — the transport owns the encoding ----
//
// Live lesson: every real CLI offers compression and the Anthropic
// edge encodes when offered. The brotli lesson is why the client's offer no
// longer reaches upstream at all: Claude Code offers br, Anthropic brotli'd its
// 429 bodies, and every body-inspecting path (classify429 above all) went blind
// on bytes the proxy cannot decode. So the transport now owns negotiation — the
// upstream leg stays gzip (Go negotiates and transparently decodes), and the
// CLIENT leg is identity. The invariants this test carries forward: the client's
// bytes are the upstream script exactly, and the aggregator reads the same wire.
func TestE2ERespTapGzipEncodedUpstream(t *testing.T) {
	h, restore := newE2EHosted(t, "/anthropic")
	defer restore()
	withClientSecret(t, h.e2eEnv, "sekrit")
	h.anthSSE.Store(respSSEFixture())
	h.anthGzip.Store(true)
	defer h.anthGzip.Store(false)

	_, frames, closeStream := evStream(t, h.e2eEnv, "sekrit", "")
	defer closeStream()
	evHello(t, frames)

	// The client offers ONLY an encoding the proxy cannot decode — the strongest
	// form of the brotli case. The transport must negotiate its own gzip
	// upstream and hand the client identity bytes.
	body := `{"model":"claude-fable-5","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}`
	req, _ := http.NewRequest("POST", h.proxy.URL+"/v1/messages", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer sekrit")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("anthropic-beta", "oauth-2025-04-20")
	req.Header.Set("Accept-Encoding", "br")
	req.Header.Set("x-claude-code-session-id", "s19-gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request failed: %v", err)
	}
	if hit := h.hit(t); strings.Contains(hit.ae, "br") || !strings.Contains(hit.ae, "gzip") {
		t.Fatalf("the transport must own upstream negotiation (its own gzip, never the client's offer), upstream saw AE=%q", hit.ae)
	}
	raw := respReadAll(t, resp)

	if ce := resp.Header.Get("Content-Encoding"); ce != "" {
		t.Fatalf("the client leg must be identity since the transport owns upstream encoding, got CE=%q", ce)
	}
	if string(raw) != respSSEFixture() {
		t.Fatalf("client bytes must be the upstream script exactly, got %q", raw[:min(64, len(raw))])
	}

	started := respEvData(t, evAwait(t, frames, "turn_started", 3*time.Second))
	msg := respEvData(t, evAwait(t, frames, "assistant_message", 3*time.Second))
	if started["conversation_key"] != msg["conversation_key"] {
		t.Fatalf("started/message do not join: %v vs %v", started["conversation_key"], msg["conversation_key"])
	}
	if got := respFirstBlockText(t, msg); got != "Hello world" {
		t.Fatalf("aggregated text %q, want %q — the aggregator did not decode the wire", got, "Hello world")
	}
	if tr, _ := msg["stream_truncated"].(bool); tr {
		t.Fatal("clean gzip stream marked stream_truncated")
	}
	if sr, _ := msg["stop_reason"].(string); sr != "tool_use" {
		t.Fatalf("stop_reason %q, want tool_use", sr)
	}
}
