package main

// End-to-end contract for the OpenAI inbound wire (#96), covering plan v2
// C1–C7 and C9. The wire is default-deny (workload classes only), never
// touches an anthropic member (D2), and leaves inject/tap unreachable (D9).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// resetWireBuckets keeps the process-global rate limiter from bleeding
// tokens across tests (buckets are keyed by class name, shared per-process).
func resetWireBuckets() {
	lan.wireMu.Lock()
	lan.wireBuckets = map[string]*wireBucket{}
	lan.wireMu.Unlock()
}

func wirePost(t *testing.T, env *e2eEnv, body, session string) *http.Response {
	t.Helper()
	req, err := http.NewRequest("POST", env.proxy.URL+"/v1/chat/completions", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer CLIENT")
	req.Header.Set("Content-Type", "application/json")
	if session != "" {
		req.Header.Set("x-claude-code-session-id", session)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("wire request failed: %v", err)
	}
	return resp
}

func wireBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return b
}

// noHostedHit asserts nothing reached the hosted (provider) upstream.
func (h *e2eHosted) noHostedHit(t *testing.T) {
	t.Helper()
	select {
	case hit := <-h.hosted:
		t.Fatalf("request unexpectedly reached the hosted upstream: %+v", hit)
	case <-time.After(150 * time.Millisecond):
	}
}

func drainHostedHits(h *e2eHosted) {
	for {
		select {
		case <-h.hosted:
		default:
			return
		}
	}
}

const wireChatReq = `{
	"model": "%s",
	"messages": [
		{"role":"system","content":"You write quizzes."},
		{"role":"user","content":"make a quiz"}
	],
	"max_tokens": 64,
	"response_format": {"type":"json_schema","json_schema":{
		"name":"quiz",
		"schema":{"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"]}}}
}`

// C1: happy path — full OpenAI request through the class chain to the
// provider member, response re-rendered as a chat completion with the
// class transport's Anthropic-unit usage scaling (kimi ratio 0.61) applied.
func TestE2EOpenAIWireHappyPath(t *testing.T) {
	h, done := newE2EClass(t)
	defer done()
	resetWireBuckets()
	h.bindTo.Store("kimi/kimi-k3")
	h.kimiJSON.Store(`{"type":"message","id":"msg_abc","stop_reason":"tool_use",
		"content":[{"type":"tool_use","name":"quiz","input":{"answer":42}}],
		"usage":{"input_tokens":100,"output_tokens":20,"cache_read_input_tokens":40}}`)

	resp := wirePost(t, h.e2eEnv, fmt.Sprintf(wireChatReq, "claude-frontier"), "wire-c1")
	body := wireBody(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, body: %s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("X-Switch-Wire"); got != "openai" {
		t.Errorf("X-Switch-Wire = %q, want openai", got)
	}
	if got := resp.Header.Get("X-Switch-Class"); got != "claude-frontier" {
		t.Errorf("X-Switch-Class = %q, want claude-frontier", got)
	}

	var m struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Model   string `json:"model"`
		Choices []struct {
			Message      struct{ Role, Content string } `json:"message"`
			FinishReason string                         `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("response not JSON: %v\n%s", err, body)
	}
	if m.ID != "chatcmpl-msg_abc" || m.Object != "chat.completion" || m.Model != "claude-frontier" {
		t.Errorf("envelope = id %q object %q model %q", m.ID, m.Object, m.Model)
	}
	if len(m.Choices) != 1 {
		t.Fatalf("choices = %d, want 1", len(m.Choices))
	}
	var payload struct{ Answer int }
	if err := json.Unmarshal([]byte(m.Choices[0].Message.Content), &payload); err != nil || payload.Answer != 42 {
		t.Errorf("content = %q, want the synthetic tool's input JSON", m.Choices[0].Message.Content)
	}
	if m.Choices[0].FinishReason != "stop" {
		t.Errorf("finish_reason = %q, want stop", m.Choices[0].FinishReason)
	}
	// The class transport rescales input-side usage to Anthropic units
	// (divide by the member ratio) before the wire renderer sums it.
	wantPrompt := scaleTokensByRatio(100, 0.61) + scaleTokensByRatio(40, 0.61)
	if m.Usage.PromptTokens != wantPrompt || m.Usage.CompletionTokens != 20 || m.Usage.TotalTokens != wantPrompt+20 {
		t.Errorf("usage = %+v, want prompt %d / completion 20 / total %d (D10 scaling)",
			m.Usage, wantPrompt, wantPrompt+20)
	}

	hit := h.hostedHit(t)
	if hit.auth != "Bearer kimi-secret" {
		t.Errorf("upstream auth = %q, want the provider key", hit.auth)
	}
	if hit.model != "kimi-k3" {
		t.Errorf("upstream model = %q, want kimi-k3", hit.model)
	}
	if !strings.Contains(hit.path, "/coding/v1/messages") {
		t.Errorf("upstream path = %q, want the hosted /coding/v1/messages", hit.path)
	}
	ob := string(hit.body)
	for _, want := range []string{`"quiz"`, `"tool_choice"`, `"thinking"`, `"disabled"`, "make a quiz"} {
		if !strings.Contains(ob, want) {
			t.Errorf("outbound body missing %s:\n%s", want, ob)
		}
	}
	h.noAnthropicLeak(t)
	h.noCodexLeak(t)
}

// #460: an OpenAI image_url part reaches the provider as an Anthropic image
// block. kimi is a body pass-through wire, so what the upstream sees here is
// exactly what the normalizer wrote.
func TestE2EOpenAIWireImagePassthroughToKimi(t *testing.T) {
	h, done := newE2EClass(t)
	defer done()
	resetWireBuckets()
	h.bindTo.Store("kimi/kimi-k3")
	h.kimiJSON.Store(`{"type":"message","id":"msg_img","stop_reason":"end_turn",
		"content":[{"type":"text","text":"the bed is clear"}],
		"usage":{"input_tokens":900,"output_tokens":8}}`)

	resp := wirePost(t, h.e2eEnv, `{"model":"frontier","max_tokens":64,"messages":[
		{"role":"system","content":"You inspect printer beds."},
		{"role":"user","content":[
			{"type":"text","text":"is the bed clear?"},
			{"type":"image_url","image_url":{"url":"data:image/jpeg;base64,QUJD","detail":"high"}}
		]}
	]}`, "wire-460")
	body := wireBody(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, body: %s", resp.StatusCode, body)
	}

	ob := string(h.hostedHit(t).body)
	for _, want := range []string{`"type":"image"`, `"type":"base64"`, `"media_type":"image/jpeg"`, `"data":"QUJD"`, "is the bed clear?"} {
		if !strings.Contains(ob, want) {
			t.Errorf("outbound body missing %s:\n%s", want, ob)
		}
	}
	for _, never := range []string{"image_url", "detail"} {
		if strings.Contains(ob, never) {
			t.Errorf("outbound body still carries the OpenAI dialect (%s):\n%s", never, ob)
		}
	}
	h.noAnthropicLeak(t)
	h.noCodexLeak(t)
}

// C2: the bare class alias resolves and echoes back verbatim.
func TestE2EOpenAIWireBareAlias(t *testing.T) {
	h, done := newE2EClass(t)
	defer done()
	resetWireBuckets()
	h.bindTo.Store("kimi/kimi-k3")
	h.kimiJSON.Store(`{"type":"message","id":"msg_alias","stop_reason":"end_turn",
		"content":[{"type":"text","text":"plain answer"}],
		"usage":{"input_tokens":10,"output_tokens":5}}`)

	resp := wirePost(t, h.e2eEnv,
		`{"model":"frontier","messages":[{"role":"user","content":"hi"}],"max_tokens":32}`, "wire-c2")
	body := wireBody(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, body: %s", resp.StatusCode, body)
	}
	var m struct {
		Model   string `json:"model"`
		Choices []struct {
			Message struct{ Content string } `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, body)
	}
	if m.Model != "frontier" {
		t.Errorf("model = %q, want the bare alias echoed back", m.Model)
	}
	if m.Choices[0].Message.Content != "plain answer" {
		t.Errorf("content = %q, want the text blocks", m.Choices[0].Message.Content)
	}
	h.hostedHit(t)
	h.noAnthropicLeak(t)
}

// C3: default-deny — anything that isn't a workload class 404s at the door
// and never touches any upstream (D8).
func TestE2EOpenAIWireDefaultDeny(t *testing.T) {
	h, done := newE2EClass(t)
	defer done()
	resetWireBuckets()
	for _, model := range []string{"claude-kimi-k3", "claude-fable-5", "gpt-4o", "kimi-k3"} {
		resp := wirePost(t, h.e2eEnv,
			`{"model":"`+model+`","messages":[{"role":"user","content":"hi"}]}`, "")
		body := wireBody(t, resp)
		if resp.StatusCode != 404 {
			t.Errorf("%s: status = %d, want 404; body: %s", model, resp.StatusCode, body)
		}
		if !bytes.Contains(body, []byte("model_not_found")) {
			t.Errorf("%s: body missing model_not_found: %s", model, body)
		}
	}
	h.noHostedHit(t)
	h.e2eEnv.noUpstream(t)
	h.noCodexLeak(t)
}

// Lane-only environment (no class rows at all): even real lane model IDs 404.
func TestE2EOpenAIWireNoClassesConfigured(t *testing.T) {
	env, done := newE2E(t)
	defer done()
	resetWireBuckets()
	resp := wirePost(t, env,
		`{"model":"claude-fable-5","messages":[{"role":"user","content":"hi"}]}`, "")
	body := wireBody(t, resp)
	if resp.StatusCode != 404 || !bytes.Contains(body, []byte("model_not_found")) {
		t.Errorf("status = %d, body: %s (want 404 model_not_found)", resp.StatusCode, body)
	}
	env.noUpstream(t)
}

// The wire sits behind the client gate like every other endpoint.
func TestE2EOpenAIWireClientGate(t *testing.T) {
	h, done := newE2EClass(t)
	defer done()
	resetWireBuckets()
	withClientSecret(t, h.e2eEnv, "sekrit")
	req, _ := http.NewRequest("POST", h.proxy.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"frontier","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	wireBody(t, resp)
	if resp.StatusCode != 401 {
		t.Errorf("unauthenticated wire request: status = %d, want 401", resp.StatusCode)
	}
	h.noHostedHit(t)
}

// C4: anthropic members are invisible to the wire — a sticky bind to the
// anthropic lane is skipped (D2 try-filter), and an all-anthropic class
// refuses rather than routes.
func TestE2EOpenAIWireNeverAnthropic(t *testing.T) {
	h, done := newE2EClass(t)
	defer done()
	resetWireBuckets()
	h.bindTo.Store("lane:fable")
	h.kimiJSON.Store(`{"type":"message","id":"msg_c4","stop_reason":"end_turn",
		"content":[{"type":"text","text":"served by kimi"}],
		"usage":{"input_tokens":1,"output_tokens":1}}`)

	resp := wirePost(t, h.e2eEnv,
		`{"model":"frontier","messages":[{"role":"user","content":"hi"}],"max_tokens":16}`, "wire-c4")
	body := wireBody(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, body: %s", resp.StatusCode, body)
	}
	if !bytes.Contains(body, []byte("served by kimi")) {
		t.Errorf("response should come from the provider member: %s", body)
	}
	hit := h.hostedHit(t)
	if hit.model != "kimi-k3" {
		t.Errorf("upstream model = %q, want kimi-k3 (anthropic bind must be skipped)", hit.model)
	}
	h.noAnthropicLeak(t)

	// Now strip the class down to only the anthropic member: the wire must
	// refuse (OpenAI-shaped 503), not fall through to the subscription lane.
	h.brainMu.Lock()
	h.classMembers = []map[string]any{{
		"id": "lane:fable", "kind": "anthropic", "model": "claude-fable-5",
		"priority": 1, "usable": true,
	}}
	h.brainMu.Unlock()
	fetchPreferredSync()

	resp = wirePost(t, h.e2eEnv,
		`{"model":"frontier","messages":[{"role":"user","content":"hi"}],"max_tokens":16}`, "wire-c4b")
	body = wireBody(t, resp)
	if resp.StatusCode < 500 {
		t.Fatalf("all-anthropic class: status = %d, want a 5xx refusal; body: %s", resp.StatusCode, body)
	}
	var e struct {
		Error struct{ Message, Type string } `json:"error"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("refusal not OpenAI-shaped JSON: %v\n%s", err, body)
	}
	if !strings.Contains(e.Error.Message, "no provider members usable") {
		t.Errorf("refusal message = %q", e.Error.Message)
	}
	h.noHostedHit(t)
	h.noAnthropicLeak(t)
}

// C5: the inject queue is unreachable from the wire (D9) — a queued
// injection survives a wire request untouched and still fires on the next
// native Anthropic-wire request for the same session.
func TestE2EOpenAIWireInjectUnreachable(t *testing.T) {
	h, done := newE2EClass(t)
	defer done()
	resetWireBuckets()
	h.bindTo.Store("kimi/kimi-k3")

	lan.injectObserve("sess-c5", time.Now())
	key := injectSessionKey("sess-c5")
	resp := enqueueE2E(t, h.e2eEnv, "", key, "SECRET-INSTR")
	if resp.StatusCode != 202 {
		t.Fatalf("enqueue status = %d, want 202", resp.StatusCode)
	}
	resp.Body.Close()

	wresp := wirePost(t, h.e2eEnv,
		`{"model":"frontier","messages":[{"role":"user","content":"wire turn"}],"max_tokens":16}`, "sess-c5")
	wireBody(t, wresp)
	if wresp.StatusCode != 200 {
		t.Fatalf("wire status = %d", wresp.StatusCode)
	}
	hit := h.hostedHit(t)
	if strings.Contains(string(hit.body), "SECRET-INSTR") {
		t.Fatalf("injection leaked into a wire request:\n%s", hit.body)
	}

	// The same queue entry must still be live for the native wire.
	nresp := h.e2eEnv.postSess(t, "claude-frontier", "CLIENT", "sess-c5")
	nresp.Body.Close()
	if nresp.StatusCode != 200 {
		t.Fatalf("native status = %d", nresp.StatusCode)
	}
	hit = h.hostedHit(t)
	if !strings.Contains(string(hit.body), "SECRET-INSTR") {
		t.Errorf("injection should fire on the native wire:\n%s", hit.body)
	}
}

// C6: streaming is refused at the door; nothing reaches any upstream.
func TestE2EOpenAIWireStreamRefused(t *testing.T) {
	h, done := newE2EClass(t)
	defer done()
	resetWireBuckets()
	resp := wirePost(t, h.e2eEnv,
		`{"model":"frontier","stream":true,"messages":[{"role":"user","content":"hi"}]}`, "")
	body := wireBody(t, resp)
	if resp.StatusCode != 400 || !bytes.Contains(body, []byte("unsupported_value")) {
		t.Errorf("status = %d, body: %s (want 400 unsupported_value)", resp.StatusCode, body)
	}
	h.noHostedHit(t)
	h.e2eEnv.noUpstream(t)
}

// C7: the per-class token bucket refuses over-limit requests with an
// OpenAI-shaped 429 + Retry-After before any routing happens.
func TestE2EOpenAIWireRateLimit(t *testing.T) {
	h, done := newE2EClass(t)
	defer done()
	h.bindTo.Store("kimi/kimi-k3")

	oldRPM, oldBurst := wireRPM, wireBurst
	wireRPM, wireBurst = 1, 2
	resetWireBuckets()
	defer func() {
		wireRPM, wireBurst = oldRPM, oldBurst
		resetWireBuckets()
	}()

	body := `{"model":"frontier","messages":[{"role":"user","content":"hi"}],"max_tokens":16}`
	for i := 0; i < 2; i++ {
		resp := wirePost(t, h.e2eEnv, body, "")
		wireBody(t, resp)
		if resp.StatusCode != 200 {
			t.Fatalf("request %d: status = %d, want 200 (inside burst)", i+1, resp.StatusCode)
		}
		h.hostedHit(t)
	}
	resp := wirePost(t, h.e2eEnv, body, "")
	rb := wireBody(t, resp)
	if resp.StatusCode != 429 {
		t.Fatalf("over-burst status = %d, body: %s", resp.StatusCode, rb)
	}
	if got := resp.Header.Get("Retry-After"); got != "30" {
		t.Errorf("Retry-After = %q, want 30", got)
	}
	if !bytes.Contains(rb, []byte("rate_limit_exceeded")) {
		t.Errorf("body missing rate_limit_exceeded: %s", rb)
	}
	h.noHostedHit(t)
}

// C9: when the provider member fails mid-request, the recovery walk must
// not cross onto the anthropic member — the ladder exhausts and the
// provider's error surfaces OpenAI-shaped.
func TestE2EOpenAIWireWalkNeverCrossesToAnthropic(t *testing.T) {
	h, done := newE2EClass(t)
	defer done()
	resetWireBuckets()
	h.bindTo.Store("kimi/kimi-k3")
	h.deny.Store(true)
	defer h.deny.Store(false)

	resp := wirePost(t, h.e2eEnv,
		`{"model":"frontier","messages":[{"role":"user","content":"hi"}],"max_tokens":16}`, "wire-c9")
	body := wireBody(t, resp)
	if resp.StatusCode != 429 {
		t.Fatalf("status = %d, want the provider 429 surfaced; body: %s", resp.StatusCode, body)
	}
	var e struct {
		Error struct{ Message, Type string } `json:"error"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("not OpenAI-shaped: %v\n%s", err, body)
	}
	if e.Error.Type != "rate_limit_error" {
		t.Errorf("error.type = %q, want the provider's rate_limit_error mapped through", e.Error.Type)
	}
	drainHostedHits(h)
	h.noAnthropicLeak(t)
	h.noCodexLeak(t)
}
