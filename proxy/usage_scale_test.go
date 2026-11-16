package main

// anthropic_token_ratio (#44 Phase 4): unit tests for the scaling helpers and
// the kimi pass-through rewriters, plus e2e proof that a scaled provider's
// usage reaches the client in Anthropic units on all three response shapes
// (SSE, single JSON message, count_tokens) while everything else — text
// deltas, output_tokens, framing — passes byte-verbatim.

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestScaleTokensByRatio(t *testing.T) {
	cases := []struct {
		n    int
		r    float64
		want int
	}{
		{1000, 0.5, 2000},
		{143924, 0.57, 252498}, // the live incident's direction: gpt under-report inflates back
		{1000, 0, 1000},        // 0 = undeclared → identity
		{1000, 1, 1000},        // 1 = same tokenizer → identity
		{0, 0.5, 0},
		{-5, 0.5, -5}, // nonsense stays nonsense rather than flipping sign
		{999, 0.61, 1638},
	}
	for _, c := range cases {
		if got := scaleTokensByRatio(c.n, c.r); got != c.want {
			t.Errorf("scaleTokensByRatio(%d, %v) = %d, want %d", c.n, c.r, got, c.want)
		}
	}
}

// sseFixtureScaled is an Anthropic-shaped kimi stream: usage on message_start
// (input side, upstream units) and message_delta (output only), with a text
// delta between them that must never be touched.
const sseFixtureScaled = "event: message_start\n" +
	`data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"kimi-k3","usage":{"input_tokens":1000,"cache_creation_input_tokens":200,"cache_read_input_tokens":400,"output_tokens":1}}}` + "\n" +
	"\n" +
	"event: content_block_delta\n" +
	`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"usage says {\"input_tokens\": 7}"}}` + "\n" +
	"\n" +
	"event: message_delta\n" +
	`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":50}}` + "\n" +
	"\n" +
	"event: message_stop\n" +
	`data: {"type":"message_stop"}` + "\n" +
	"\n"

func sseEventPayload(t *testing.T, stream, typ string) map[string]any {
	t.Helper()
	for _, line := range strings.Split(stream, "\n") {
		payload, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(payload), &m); err != nil {
			t.Fatalf("unparseable data line %q: %v", line, err)
		}
		if m["type"] == typ {
			return m
		}
	}
	t.Fatalf("no %s event in stream:\n%s", typ, stream)
	return nil
}

func TestSSEUsageScaler(t *testing.T) {
	mark := newScaleMark("kimi/kimi-k3", 0.5)
	sc := newSSEUsageScaler(io.NopCloser(strings.NewReader(sseFixtureScaled)), 0.5, mark)
	out, err := io.ReadAll(sc)
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)

	// #76: the mark resolved ok with the pre-scale raw triple captured.
	v, ok := mark.view()
	if !ok || !v.applied || v.mode != "sse" || v.ratio != 0.5 {
		t.Fatalf("mark = %+v ok=%v, want applied sse @0.5", v, ok)
	}
	if v.rawIn == nil || *v.rawIn != 1000 || v.rawCacheRead == nil || *v.rawCacheRead != 400 ||
		v.rawCacheCreate == nil || *v.rawCacheCreate != 200 {
		t.Fatalf("raw triple not captured: %+v", v)
	}

	start := sseEventPayload(t, got, "message_start")
	u := start["message"].(map[string]any)["usage"].(map[string]any)
	if u["input_tokens"] != float64(2000) || u["cache_creation_input_tokens"] != float64(400) ||
		u["cache_read_input_tokens"] != float64(800) {
		t.Fatalf("message_start usage not scaled: %v", u)
	}
	if u["output_tokens"] != float64(1) {
		t.Fatalf("output_tokens must stay upstream units, got %v", u["output_tokens"])
	}

	delta := sseEventPayload(t, got, "message_delta")
	du := delta["usage"].(map[string]any)
	if du["output_tokens"] != float64(50) {
		t.Fatalf("message_delta output_tokens changed: %v", du)
	}

	// The text delta — which itself CONTAINS a usage-looking string — and all
	// event:/blank lines pass verbatim.
	if !strings.Contains(got, `"text":"usage says {\"input_tokens\": 7}"`) {
		t.Fatalf("text delta was rewritten:\n%s", got)
	}
	if strings.Count(got, "event: ") != strings.Count(sseFixtureScaled, "event: ") {
		t.Fatalf("event framing changed:\n%s", got)
	}
}

func TestSSEUsageScalerVerbatimCases(t *testing.T) {
	huge := `data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"` +
		strings.Repeat("x", 2<<20) + `"}}` + "\n"
	in := "event: ping\r\n" + // \r\n framing survives
		"data: {\"type\":\"ping\"}\r\n" +
		"\r\n" +
		huge + // >1MiB single line — must not break framing (PLAN §212)
		"\n" +
		"data: {not json, mentions \"usage\" though}\n" + // malformed → verbatim
		"\n" +
		"data: {\"type\":\"message_delta\",\"usage\":{\"input_tokens\":100}}" // EOF without trailing newline

	sc := newSSEUsageScaler(io.NopCloser(strings.NewReader(in)), 0.5, newScaleMark("kimi/kimi-k3", 0.5))
	out, err := io.ReadAll(sc)
	if err != nil {
		t.Fatal(err)
	}
	got := string(out)
	if !strings.Contains(got, huge) {
		t.Fatal("huge line did not pass byte-verbatim")
	}
	if !strings.Contains(got, `data: {not json, mentions "usage" though}`) {
		t.Fatal("malformed line did not pass verbatim")
	}
	if !strings.Contains(got, "event: ping\r\n") {
		t.Fatal("\\r\\n framing was not preserved")
	}
	if !strings.Contains(got, `"input_tokens":200`) {
		t.Fatalf("final unterminated usage line was not scaled: %s", got)
	}
}

func scaledResp(path, contentType, body string) *http.Response {
	req, _ := http.NewRequest("POST", "http://kimi.test"+path, nil)
	h := http.Header{}
	h.Set("Content-Type", contentType)
	return &http.Response{
		StatusCode:    200,
		Header:        h,
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}

func TestScaleProviderUsageCountTokens(t *testing.T) {
	mark := newScaleMark("kimi/kimi-k3", 0.5)
	resp := scaledResp("/coding/v1/messages/count_tokens", "application/json", `{"input_tokens":1000}`)
	scaleProviderUsage(resp, 0.5, mark)
	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatal(err)
	}
	if m["input_tokens"] != float64(2000) {
		t.Fatalf("count_tokens answer = %v, want 2000 (Anthropic units)", m)
	}
	v, ok := mark.view()
	if !ok || !v.applied || v.mode != "count_tokens" || v.rawIn == nil || *v.rawIn != 1000 {
		t.Fatalf("mark = %+v ok=%v, want applied count_tokens raw 1000", v, ok)
	}
}

func TestScaleProviderUsageJSONMessage(t *testing.T) {
	mark := newScaleMark("kimi/kimi-k3", 0.5)
	body := `{"type":"message","content":[{"type":"text","text":"ok"}],"usage":{"input_tokens":300,"output_tokens":7}}`
	resp := scaledResp("/coding/v1/messages", "application/json", body)
	scaleProviderUsage(resp, 0.5, mark)
	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatal(err)
	}
	u := m["usage"].(map[string]any)
	if u["input_tokens"] != float64(600) || u["output_tokens"] != float64(7) {
		t.Fatalf("usage = %v, want input 600 / output 7", u)
	}
	if resp.ContentLength != int64(63+len(`{"input_tokens":600,"output_tokens":7}`)-len(`x`)) && resp.ContentLength <= 0 {
		t.Fatalf("ContentLength not maintained: %d", resp.ContentLength)
	}
	v, ok := mark.view()
	if !ok || !v.applied || v.mode != "json" || v.rawIn == nil || *v.rawIn != 300 {
		t.Fatalf("mark = %+v ok=%v, want applied json raw 300", v, ok)
	}
	// The cache fields were absent from the wire — they must stay unknown.
	if v.rawCacheRead != nil || v.rawCacheCreate != nil {
		t.Fatalf("absent cache fields captured as known: %+v", v)
	}
}

func TestScaleProviderUsageFailOpen(t *testing.T) {
	// Content-Encoding present → untouched (upstream forced compression),
	// and the mark records the fail-open instead of leaving it machine-silent.
	mark := newScaleMark("kimi/kimi-k3", 0.5)
	resp := scaledResp("/coding/v1/messages", "application/json", "GZIPBYTES")
	resp.Header.Set("Content-Encoding", "gzip")
	scaleProviderUsage(resp, 0.5, mark)
	b, _ := io.ReadAll(resp.Body)
	if string(b) != "GZIPBYTES" {
		t.Fatalf("encoded body was touched: %q", b)
	}
	if v, ok := mark.view(); !ok || v.applied || v.reason != "content_encoding" {
		t.Fatalf("mark = %+v ok=%v, want fail_open content_encoding", v, ok)
	}

	// Non-200 → untouched, mark PENDING (an error envelope carries no usage —
	// there is no scaling claim to make, so the tap must omit the block).
	mark = newScaleMark("kimi/kimi-k3", 0.5)
	resp = scaledResp("/coding/v1/messages", "application/json", `{"error":"x","usage":{"input_tokens":9}}`)
	resp.StatusCode = 429
	scaleProviderUsage(resp, 0.5, mark)
	var m map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&m)
	if m["usage"].(map[string]any)["input_tokens"] != float64(9) {
		t.Fatal("non-200 body was rewritten")
	}
	if _, ok := mark.view(); ok {
		t.Fatal("non-200 resolved the mark; it must stay pending")
	}

	// No usage anywhere → byte-identical restore, fail_open{no_usage}.
	mark = newScaleMark("kimi/kimi-k3", 0.5)
	orig := `{"type":"message","content":[]}`
	resp = scaledResp("/coding/v1/messages", "application/json", orig)
	scaleProviderUsage(resp, 0.5, mark)
	b, _ = io.ReadAll(resp.Body)
	if string(b) != orig {
		t.Fatalf("usage-free body changed: %q", b)
	}
	if v, ok := mark.view(); !ok || v.applied || v.reason != "no_usage" {
		t.Fatalf("mark = %+v ok=%v, want fail_open no_usage", v, ok)
	}

	// Unparseable body → restore, fail_open{unparseable}.
	mark = newScaleMark("kimi/kimi-k3", 0.5)
	resp = scaledResp("/coding/v1/messages", "application/json", "not json at all")
	scaleProviderUsage(resp, 0.5, mark)
	b, _ = io.ReadAll(resp.Body)
	if string(b) != "not json at all" {
		t.Fatalf("unparseable body changed: %q", b)
	}
	if v, ok := mark.view(); !ok || v.applied || v.reason != "unparseable" {
		t.Fatalf("mark = %+v ok=%v, want fail_open unparseable", v, ok)
	}

	// Oversize body → restore, fail_open{oversize}. Drive scaleJSONBody
	// directly with a tiny limit so the test doesn't allocate 32MiB.
	mark = newScaleMark("kimi/kimi-k3", 0.5)
	big := `{"usage":{"input_tokens":1},"pad":"` + strings.Repeat("x", 64) + `"}`
	resp = scaledResp("/coding/v1/messages", "application/json", big)
	scaleJSONBody(resp, mark, "json", 16, func(m map[string]any) bool { return true })
	b, _ = io.ReadAll(resp.Body)
	if string(b) != big {
		t.Fatalf("oversize body changed: %q", b)
	}
	if v, ok := mark.view(); !ok || v.applied || v.reason != "oversize" {
		t.Fatalf("mark = %+v ok=%v, want fail_open oversize", v, ok)
	}
}

// TestSSEUsageScalerNoUsageStream: a stream that ends cleanly without ever
// carrying an input-side usage frame resolves fail_open{no_usage} — the mark
// never stays pending on a completed response.
func TestSSEUsageScalerNoUsageStream(t *testing.T) {
	in := "event: ping\ndata: {\"type\":\"ping\"}\n\n"
	mark := newScaleMark("kimi/kimi-k3", 0.5)
	sc := newSSEUsageScaler(io.NopCloser(strings.NewReader(in)), 0.5, mark)
	if _, err := io.ReadAll(sc); err != nil {
		t.Fatal(err)
	}
	if v, ok := mark.view(); !ok || v.applied || v.reason != "no_usage" {
		t.Fatalf("mark = %+v ok=%v, want fail_open no_usage", v, ok)
	}
	// Close after EOF must not repaint an already-resolved mark.
	_ = sc.Close()
	if v, _ := mark.view(); v.reason != "no_usage" {
		t.Fatalf("Close repainted the mark: %+v", v)
	}
}

// TestSSEUsageScalerAbandonedStream: the client walks away before any usage
// frame — Close resolves fail_open{stream_truncated}.
func TestSSEUsageScalerAbandonedStream(t *testing.T) {
	mark := newScaleMark("kimi/kimi-k3", 0.5)
	sc := newSSEUsageScaler(io.NopCloser(strings.NewReader("event: ping\n")), 0.5, mark)
	_ = sc.Close()
	if v, ok := mark.view(); !ok || v.applied || v.reason != "stream_truncated" {
		t.Fatalf("mark = %+v ok=%v, want fail_open stream_truncated", v, ok)
	}
}

// ---- e2e: the scaled provider through the real handler stack ----

// The class's kimi member is served at ratio 0.5 by the brain, so scaling is
// driven exactly the way production drives it: by the bound member's r_p.
func TestE2EKimiUsageScaledSSE(t *testing.T) {
	h, restore := newE2EClassOn(t, "kimi/kimi-k3", 0.5)
	defer restore()
	h.kimiSSE.Store(sseFixtureScaled)

	body := `{"model":"claude-frontier","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}`
	req, _ := http.NewRequest("POST", h.proxy.URL+"/v1/messages", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer client-bearer")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "gzip") // the real CLI negotiates gzip (#61) — scaling must survive it
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d: %s", resp.StatusCode, raw)
	}
	start := sseEventPayload(t, string(raw), "message_start")
	u := start["message"].(map[string]any)["usage"].(map[string]any)
	if u["input_tokens"] != float64(2000) || u["cache_read_input_tokens"] != float64(800) {
		t.Fatalf("client-visible usage not in Anthropic units: %v", u)
	}
	if !strings.Contains(string(raw), `"text":"usage says {\"input_tokens\": 7}"`) {
		t.Fatalf("content was rewritten:\n%s", raw)
	}
}

func TestE2EKimiUsageScaledJSONAndCount(t *testing.T) {
	h, restore := newE2EClassOn(t, "kimi/kimi-k3", 0.5)
	defer restore()

	// Non-streaming: the fake answers a plain JSON message without usage — it
	// must arrive untouched (fail-open restore), proving no over-reach.
	resp := h.post(t, "claude-frontier", "client-bearer")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(b), "hosted ok") {
		t.Fatalf("plain message mangled: %d %s", resp.StatusCode, b)
	}

	// count_tokens: the provider counts in its own units (canned 1000) and the
	// client must read Anthropic units (2000 at ratio 0.5).
	cb := `{"model":"claude-frontier","messages":[{"role":"user","content":"hi"}]}`
	req, _ := http.NewRequest("POST", h.proxy.URL+"/v1/messages/count_tokens", strings.NewReader(cb))
	req.Header.Set("Authorization", "Bearer client-bearer")
	req.Header.Set("Content-Type", "application/json")
	cresp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer cresp.Body.Close()
	var m map[string]any
	if err := json.NewDecoder(cresp.Body).Decode(&m); err != nil {
		t.Fatal(err)
	}
	if cresp.StatusCode != 200 || m["input_tokens"] != float64(2000) {
		t.Fatalf("count_tokens = %d %v, want 200 with input_tokens 2000", cresp.StatusCode, m)
	}
	hit := h.hostedHit(t) // the message POST above
	_ = hit
}

// ---- codex sites: the translated wire divides at synthesis time ----

func TestCodexUsageScaled(t *testing.T) {
	script := strings.ReplaceAll(defaultCodexSSE, `"usage":{"input_tokens":11,"output_tokens":5}`,
		`"usage":{"input_tokens":1000,"output_tokens":105,"output_tokens_details":{"reasoning_tokens":100}}`)

	// Aggregate (non-streaming client): input ÷ ratio, output − reasoning, never ÷.
	aggMark := newScaleMark("gpt/gpt-5.6-sol", 0.5)
	status, body := aggregateCodexSSE(strings.NewReader(script), 0.5, aggMark, nil, nil, nil)
	if status != 200 {
		t.Fatalf("status %d: %s", status, body)
	}
	if v, ok := aggMark.view(); !ok || !v.applied || v.mode != "translated_agg" ||
		v.rawIn == nil || *v.rawIn != 1000 || v.rawCacheRead != nil {
		t.Fatalf("aggregate mark = %+v ok=%v, want applied translated_agg raw 1000, cache raws unknown", v, ok)
	}
	var m struct {
		Usage map[string]int `json:"usage"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatal(err)
	}
	if m.Usage["input_tokens"] != 2000 || m.Usage["output_tokens"] != 5 {
		t.Fatalf("aggregate usage = %v, want input 2000 / output 5", m.Usage)
	}
	if m.Usage["cache_read_input_tokens"] != 0 || m.Usage["cache_creation_input_tokens"] != 0 {
		t.Fatalf("explicit zero cache fields lost: %v", m.Usage)
	}

	// Streaming client: the same division lands in message_delta.
	sseMark := newScaleMark("gpt/gpt-5.6-sol", 0.5)
	var out strings.Builder
	if err := translateCodexSSE(strings.NewReader(script), &out, 0.5, sseMark, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if v, ok := sseMark.view(); !ok || !v.applied || v.mode != "translated" || v.rawIn == nil || *v.rawIn != 1000 {
		t.Fatalf("stream mark = %+v ok=%v, want applied translated raw 1000", v, ok)
	}
	delta := sseEventPayload(t, out.String(), "message_delta")
	du := delta["usage"].(map[string]any)
	if du["input_tokens"] != float64(2000) || du["output_tokens"] != float64(5) {
		t.Fatalf("stream usage = %v, want input 2000 / output 5", du)
	}
	// message_start still reports 0 — usage is only known at response.completed.
	start := sseEventPayload(t, out.String(), "message_start")
	su := start["message"].(map[string]any)["usage"].(map[string]any)
	if su["input_tokens"] != float64(0) {
		t.Fatalf("message_start input_tokens = %v, want 0 (unknown until completed)", su)
	}
}

// ---- e2e: scaling provenance on the tap events + status counters (#76/#77) ----

// TestE2EScalingProvenance drives all three lanes through the real stack and
// reads the provenance where scry will: the assistant_message events and the
// /_switch/status usage_scaling block. The brain serves the class's kimi and gpt
// members at ratio 0.5; the anthropic lane must carry NO ratio and NO scaling
// key at all.
func TestE2EScalingProvenance(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	h.serveMember(t, "gpt/gpt-5.6", 0.5)
	h.serveMember(t, "kimi/kimi-k3", 0.5) // new bindings land on kimi first
	withClientSecret(t, h.e2eEnv, "sekrit")
	h.anthSSE.Store(respSSEFixture())
	h.kimiSSE.Store(sseFixtureScaled)

	_, frames, closeStream := evStream(t, h.e2eEnv, "sekrit", "")
	defer closeStream()
	evHello(t, frames)

	awaitMsg := func(model string, sess string) map[string]any {
		t.Helper()
		respReadAll(t, respPost(t, h.e2eEnv, model, true, sess))
		return respEvData(t, evAwait(t, frames, "assistant_message", 3*time.Second))
	}

	// kimi pass-through: scaled SSE, raw triple from the wire.
	msg := awaitMsg("claude-frontier", "sp-kimi")
	sc, _ := msg["scaling"].(map[string]any)
	if msg["ratio"] != 0.5 || sc == nil || sc["applied"] != true || sc["mode"] != "sse" ||
		sc["raw_input_tokens"] != float64(1000) || sc["raw_cache_read"] != float64(400) {
		t.Fatalf("kimi provenance = ratio:%v scaling:%v", msg["ratio"], sc)
	}
	if u, _ := msg["usage"].(map[string]any); u["input_tokens"] != float64(2000) {
		t.Fatalf("kimi usage not scaled on the event: %v", msg["usage"])
	}

	// codex translated: division at synthesis, raw input only (cache raws are
	// synthesized zeros on the wire, never REPORTED — they must stay absent).
	h.bindTo.Store("gpt/gpt-5.6") // a fresh session binds the gpt member
	msg = awaitMsg("claude-frontier", "sp-gpt")
	sc, _ = msg["scaling"].(map[string]any)
	if msg["ratio"] != 0.5 || sc == nil || sc["applied"] != true || sc["mode"] != "translated" ||
		sc["raw_input_tokens"] != float64(11) {
		t.Fatalf("gpt provenance = ratio:%v scaling:%v", msg["ratio"], sc)
	}
	if _, has := sc["raw_cache_read"]; has {
		t.Fatalf("codex cache raws must stay unknown: %v", sc)
	}
	if u, _ := msg["usage"].(map[string]any); u["input_tokens"] != float64(22) {
		t.Fatalf("gpt usage not scaled on the event: %v", msg["usage"])
	}

	// anthropic lane: native units — no ratio, no scaling block (badge honesty:
	// absence of the block is the ONLY unscaled signal, never applied:false).
	msg = awaitMsg("claude-fable-5", "sp-anth")
	if _, has := msg["ratio"]; has {
		t.Fatalf("anthropic event carries a ratio: %v", msg["ratio"])
	}
	if _, has := msg["scaling"]; has {
		t.Fatalf("anthropic event carries a scaling block: %v", msg["scaling"])
	}

	// #77: the status counters saw both scaled turns, keyed by member.
	sreq, _ := http.NewRequest("GET", h.proxy.URL+"/_switch/status", nil)
	sreq.Header.Set("Authorization", "Bearer sekrit") // the client gate is armed above
	sresp, err := http.DefaultClient.Do(sreq)
	if err != nil {
		t.Fatal(err)
	}
	defer sresp.Body.Close()
	if sresp.StatusCode != 200 {
		t.Fatalf("status endpoint answered %d", sresp.StatusCode)
	}
	var status struct {
		UsageScaling map[string]struct {
			OK       int64            `json:"ok"`
			FailOpen map[string]int64 `json:"fail_open"`
		} `json:"usage_scaling"`
	}
	if err := json.NewDecoder(sresp.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if status.UsageScaling["kimi/kimi-k3"].OK < 1 || status.UsageScaling["gpt/gpt-5.6"].OK < 1 {
		t.Fatalf("usage_scaling counters = %+v, want ok >= 1 on both scaled members", status.UsageScaling)
	}
}

// The ratio a class provider hop is scaled by is the bound member's
// anthropic_token_ratio as the brain serves it — the only source since
// quota-model C2 deleted the models.json named rows that used to carry it.
// The same upstream count (the fake kimi's canned 1000) reads 2000 client
// units at 0.5 and 4000 once the brain serves 0.25, and /_switch/status shows
// the ratio each binding is scaled by.
func TestE2EClassHopScaledByBrainRatio(t *testing.T) {
	h, restore := newE2EClassOn(t, "kimi/kimi-k3", 0.5)
	defer restore()

	count := func(sess string) float64 {
		t.Helper()
		req, _ := http.NewRequest("POST", h.proxy.URL+"/v1/messages/count_tokens",
			strings.NewReader(`{"model":"claude-frontier","messages":[{"role":"user","content":"hi"}]}`))
		req.Header.Set("Authorization", "Bearer client-bearer")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-claude-code-session-id", sess)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&m); err != nil || resp.StatusCode != 200 {
			t.Fatalf("%s count_tokens = %d %v (%v)", sess, resp.StatusCode, m, err)
		}
		if hit := h.hostedHit(t); hit.model != "kimi-k3" {
			t.Fatalf("%s reached kimi as %q, want the member's model", sess, hit.model)
		}
		n, _ := m["input_tokens"].(float64)
		return n
	}
	boundRatio := func(sess string) any {
		t.Helper()
		resp, err := http.Get(h.proxy.URL + "/_switch/status")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var s struct {
			ClassBindings map[string]map[string]map[string]any `json:"class_bindings"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
			t.Fatal(err)
		}
		b := s.ClassBindings["frontier"][fpSess(sess)]
		if b == nil || b["member"] != "kimi/kimi-k3" {
			t.Fatalf("class_bindings[frontier][%s] = %v, want the kimi member", fpSess(sess), b)
		}
		return b["anthropic_token_ratio"]
	}

	if got := count("ratio-half"); got != 2000 {
		t.Fatalf("at the brain's 0.5, count_tokens = %v, want 2000 (1000 / 0.5)", got)
	}
	if r := boundRatio("ratio-half"); r != 0.5 {
		t.Fatalf("status anthropic_token_ratio = %v, want the brain's 0.5", r)
	}

	h.brainMu.Lock()
	h.classMembers[2]["anthropic_token_ratio"] = 0.25
	h.brainMu.Unlock()
	if got := count("ratio-quarter"); got != 4000 {
		t.Fatalf("after the brain moved to 0.25, a fresh binding's count_tokens = %v, want 4000", got)
	}
	if r := boundRatio("ratio-quarter"); r != 0.25 {
		t.Fatalf("status anthropic_token_ratio = %v, want the brain's 0.25", r)
	}
	h.noAnthropicLeak(t)
}
