package main

// e2e tests for the codex-backed gpt lane (#33) through the REAL handler stack:
// requestHandler -> Rewrite -> failoverTransport -> translateCodexResponse,
// with the fake brain dispensing bearers and a fake codex upstream speaking
// SSE. Since quota-model C2 a class member is the only way a request reaches
// gpt: member routing, the 401 bearer-retry, the mid-request class cross in
// BOTH directions, and the wire's path refusal.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

// postStream is postSess with an explicit top-level "stream" flag.
func (env *e2eEnv) postStream(t *testing.T, model string, stream bool) *http.Response {
	t.Helper()
	return env.postStreamSess(t, model, stream, "")
}

// postStreamSess is postStream with a session id — class routing binds per
// session, so the streaming CLASS paths need both.
func (env *e2eEnv) postStreamSess(t *testing.T, model string, stream bool, session string) *http.Response {
	t.Helper()
	body := fmt.Sprintf(`{"model":%q,"max_tokens":16,"stream":%v,"messages":[{"role":"user","content":"hi"}]}`, model, stream)
	req, _ := http.NewRequest("POST", env.proxy.URL+"/v1/messages", bytes.NewReader([]byte(body)))
	req.Header.Set("Authorization", "Bearer CLIENT")
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

// ---- class routing on the codex member ----

// classWithUsableGpt rebinds the class fixture so the gpt member is usable and
// new sessions bind to it.
func classWithUsableGpt(t *testing.T) (*e2eHosted, func()) {
	t.Helper()
	h, restore := newE2EClass(t)
	h.brainMu.Lock()
	h.classMembers[1]["usable"] = true
	h.classMembers[1]["status"] = "ok"
	h.classMembers[1]["effective"] = 20.0
	h.brainMu.Unlock()
	h.bindTo.Store("gpt/gpt-5.6")
	fetchPreferredSync()
	return h, restore
}

// The headline contract: a class request bound to the gpt member is
// TRANSLATED (codex body, fixed backend path, dispensed bearer +
// account-id/originator/UA, no Anthropic client headers), and the streamed
// upstream is aggregated into ONE Anthropic message for a non-streaming client.
// The member is served at r_p 1.0 so the usage reads in native units here; the
// scaling of a translated hop is pinned in usage_scale_test.go.
func TestE2ECodexMemberTranslatedNonStream(t *testing.T) {
	h, restore := newE2EClassOn(t, "gpt/gpt-5.6", 1.0)
	defer restore()

	resp := h.postSess(t, "claude-frontier", "client-oauth-bearer", "sess-codex-headline")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json (aggregated)", ct)
	}
	var msg map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&msg); err != nil {
		t.Fatal(err)
	}
	content := msg["content"].([]any)[0].(map[string]any)
	if msg["type"] != "message" || content["text"] != "codex ok" || msg["stop_reason"] != "end_turn" {
		t.Fatalf("aggregated message = %v", msg)
	}
	usage := msg["usage"].(map[string]any)
	if usage["input_tokens"] != float64(11) || usage["output_tokens"] != float64(5) {
		t.Fatalf("usage = %v", usage)
	}

	hit := h.codexHitOrFail(t)
	if hit.path != codexResponsesPath {
		t.Fatalf("path = %q, want %s", hit.path, codexResponsesPath)
	}
	// MUTATION GUARD: the brain-dispensed bearer + account id — never the
	// client's Anthropic bearer, never a static key.
	if hit.auth != "Bearer codex-tok-1" || hit.acct != "acct-e2e" {
		t.Fatalf("auth/acct = %q/%q, want the dispensed bearer pair", hit.auth, hit.acct)
	}
	if hit.originator != "codex-cli" || hit.ua != codexUA {
		t.Fatalf("originator/ua = %q/%q", hit.originator, hit.ua)
	}
	if hit.beta != "" {
		t.Fatalf("anthropic-beta leaked to the codex upstream: %q", hit.beta)
	}
	var cb map[string]any
	if err := json.Unmarshal(hit.body, &cb); err != nil {
		t.Fatal(err)
	}
	if cb["model"] != "gpt-5.6" || cb["stream"] != true || cb["store"] != false {
		t.Fatalf("codex body envelope = %v", cb)
	}
	if _, has := cb["max_tokens"]; has {
		t.Fatal("max_tokens leaked into the codex body")
	}
	h.noAnthropicLeak(t)
}

// A codex 401 (chain rotated/revoked) on a class hop triggers exactly one
// retry on a freshly dispensed bearer — not a member cross.
func TestE2ECodexMember401RetriesOnFreshBearer(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()
	h.codex401.Store(1)

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-codex-401")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want the retried 200", resp.StatusCode)
	}
	first := h.codexHitOrFail(t)
	second := h.codexHitOrFail(t)
	if first.auth != "Bearer codex-tok-1" || second.auth != "Bearer codex-tok-2" {
		t.Fatalf("retry bearers = %q -> %q, want tok-1 -> tok-2", first.auth, second.auth)
	}
	if !bytes.Equal(first.body, second.body) {
		t.Fatal("the retry must replay the identical translated body")
	}
	h.noAnthropicLeak(t)
}

// Non-/v1/messages calls (count_tokens) have no codex equivalent: the codex
// wire refuses the path, so a session bound to the gpt member never sends one
// to codex — the class's anthropic member answers it.
func TestE2ECodexMemberCountTokensNeverReachesCodex(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()

	body := `{"model":"claude-frontier","messages":[{"role":"user","content":"hi"}]}`
	req, _ := http.NewRequest("POST", h.proxy.URL+"/v1/messages/count_tokens", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer CLIENT")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-claude-code-session-id", "sess-codex-count")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d: %s, want another member to answer", resp.StatusCode, b)
	}
	if hit := h.hit(t); hit.model != "claude-fable-5" {
		t.Fatalf("count_tokens served as %q, want the anthropic member", hit.model)
	}
	h.noCodexLeak(t)
	h.noHostedHit(t)
}

// A class session bound to the gpt member actuates codex mechanics end to end.
func TestE2EClassCodexMemberRouting(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-codex-1")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	var msg map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&msg); err != nil {
		t.Fatal(err)
	}
	if msg["content"].([]any)[0].(map[string]any)["text"] != "codex ok" {
		t.Fatalf("class codex response = %v", msg)
	}
	hit := h.codexHitOrFail(t)
	if hit.auth != "Bearer codex-tok-1" || hit.path != codexResponsesPath {
		t.Fatalf("class codex hit = %+v", hit)
	}
	var cb map[string]any
	_ = json.Unmarshal(hit.body, &cb)
	if cb["model"] != "gpt-5.6" {
		t.Fatalf("class picker id must be rewritten to the member model, got %v", cb["model"])
	}
	h.noAnthropicLeak(t)
}

// A codex-member 429 crosses mid-request to the anthropic member — and the
// crossed hop must carry CLEAN Anthropic headers (the original client set plus
// the oauth beta flag), not the codex-built ones.
func TestE2EClassCodex429CrossesToAnthropicCleanHeaders(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()
	h.codexDeny.Store(true)

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-codex-cross")
	defer resp.Body.Close()

	first := h.codexHitOrFail(t) // attempt 1: the bound gpt member, 429'd
	if first.auth != "Bearer codex-tok-1" {
		t.Fatalf("attempt 1 = %+v, want the codex member", first)
	}
	second := h.hit(t) // the cross: Anthropic upstream, fable lane's account
	if second.auth != "Bearer "+e2eToken(other) || second.model != "claude-fable-5" {
		t.Fatalf("cross = %+v, want the anthropic member", second)
	}
	// Header hygiene on the cross: oauth beta present, codex identifiers gone.
	if !strings.Contains(second.beta, oauthBetaFlag) {
		t.Fatalf("crossed hop lacks the oauth beta flag: %q", second.beta)
	}
	if second.acct != "" || second.ua == codexUA {
		t.Fatalf("codex headers leaked onto the anthropic cross: acct=%q ua=%q", second.acct, second.ua)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("client must see the crossed 200, got %d", resp.StatusCode)
	}
	// The crossed response is a plain Anthropic response — it must NOT have
	// been run through the codex translator.
	var m map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&m)
	if m["content"].([]any)[0].(map[string]any)["text"] != "ok" {
		t.Fatalf("crossed body = %v, want the anthropic upstream's message", m)
	}
	if !throttledOn("gpt") {
		t.Fatal("the 429'd codex member's account must be throttled (it said rate limited)")
	}
}

// The reverse cross: the anthropic member 429s on every account and the ladder
// lands ON the codex member — translated request out, translated response back.
func TestE2EClassAnthropic429CrossesIntoCodex(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	// gpt usable and FIRST candidate after the bound anthropic member: bump it
	// above kimi (priority order in the fixture is fable=1, gpt=2, kimi=3).
	h.brainMu.Lock()
	h.classMembers[1]["usable"] = true
	h.classMembers[1]["status"] = "ok"
	h.classMembers[1]["effective"] = 20.0
	h.brainMu.Unlock()
	fetchPreferredSync()
	h.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"You have exceeded your weekly usage limit."}}`)
	h.deny429.Store("*") // every Anthropic account 429s; codex stays healthy

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-into-codex")
	defer resp.Body.Close()

	h.hit(t) // attempt 1: fable's preferred account, 429
	h.hit(t) // attempt 2: same member, swapped account, 429
	third := h.codexHitOrFail(t)
	if third.auth != "Bearer codex-tok-1" || third.path != codexResponsesPath {
		t.Fatalf("cross into codex = %+v", third)
	}
	var cb map[string]any
	_ = json.Unmarshal(third.body, &cb)
	if cb["model"] != "gpt-5.6" || cb["stream"] != true {
		t.Fatalf("crossed codex body = %v", cb)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("client must see codex's 200, got %d", resp.StatusCode)
	}
	var msg map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&msg)
	if msg["content"].([]any)[0].(map[string]any)["text"] != "codex ok" {
		t.Fatalf("crossed response must be translated, got %v", msg)
	}
}

// After a mid-request cross, an error from the crossed-INTO member must name
// THAT member. The rebuilt request is cloned from attempt 1 and so inherits its
// context: unless the ladder re-stamps the hop's own reqInfo, the translated
// envelope is labelled with the ANTHROPIC member that was bound and never
// served — pointing every reader at the wrong backend. This is the only test
// that can catch it; the direct path carries the right info by construction.
func TestE2EClassCrossIntoCodexErrorNamesCodexMember(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	h.brainMu.Lock()
	h.classMembers[1]["usable"] = true
	h.classMembers[1]["status"] = "ok"
	h.classMembers[1]["effective"] = 20.0
	// kimi out of the ladder: with it usable the walk would carry on past gpt's
	// 429 and serve kimi's 200 — the label under test is the LAST rung's error.
	h.classMembers[2]["usable"] = false
	h.classMembers[2]["status"] = "no_target"
	h.brainMu.Unlock()
	fetchPreferredSync()
	h.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"weekly limit"}}`)
	h.deny429.Store("*")    // every Anthropic account 429s -> cross to gpt
	h.codexDeny.Store(true) // ...which 429s too, so the CLIENT sees codex's error

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-cross-err")
	defer resp.Body.Close()
	h.hit(t)            // attempt 1: bound anthropic member
	h.hit(t)            // attempt 2: same member, swapped account
	h.codexHitOrFail(t) // the cross
	if resp.StatusCode != 429 {
		t.Fatalf("codex 429 must surface intact, got %d", resp.StatusCode)
	}
	var m map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&m)
	msg, _ := m["error"].(map[string]any)["message"].(string)
	if !strings.HasPrefix(msg, "[gpt/gpt-5.6] codex upstream HTTP 429: ") {
		t.Fatalf("error must name the member that SERVED (gpt/gpt-5.6), got %q", msg)
	}
	// LEAK GUARD. eff means the ACCOUNT on an Anthropic hop, and accounts are
	// email addresses. Mislabelling this envelope with attempt 1's info does not
	// merely point at the wrong backend — it publishes an account to the client.
	if strings.Contains(msg, "@") {
		t.Fatalf("account identity leaked into a client-visible error: %q", msg)
	}
}

// The interlock itself: an Anthropic-shaped reqInfo reaching the codex
// translator must yield NO tag rather than its eff, whatever went wrong
// upstream of here. Guards the leak directly, independent of the ladder.
func TestCodexTranslateNeverLabelsWithAnAnthropicAccount(t *testing.T) {
	resp := &http.Response{StatusCode: 429, Header: http.Header{},
		Body: httpBody(`{"detail":"rate limited"}`)}
	// wire == nil is what makes this an Anthropic hop: lane infos never set one.
	out := codexWire{}.translate(resp, reqInfo{eff: "someone@example.com"})
	var m map[string]any
	_ = json.NewDecoder(out.Body).Decode(&m)
	msg := m["error"].(map[string]any)["message"].(string)
	if strings.Contains(msg, "example.com") || strings.Contains(msg, "[") {
		t.Fatalf("anthropic eff leaked through the translator: %q", msg)
	}
}

// /_switch/status labels the gpt provider's auth mode.
func TestE2ECodexStatusAndDiscovery(t *testing.T) {
	h, restore := newE2EHosted(t, "")
	defer restore()

	resp, err := http.Get(h.proxy.URL + "/_switch/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var s struct {
		DirectProviders []map[string]any `json:"direct_providers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Fatal(err)
	}
	var gpt map[string]any
	for _, p := range s.DirectProviders {
		if p["name"] == "gpt" {
			gpt = p
		}
	}
	if gpt == nil || gpt["auth"] != "codex-bearer" {
		t.Fatalf("status direct_providers gpt = %v, want auth codex-bearer", gpt)
	}
}

// A class session bound to the gpt member with stream:true must get SSE back,
// not an aggregated JSON message. The class path resolves the client's stream
// wish from the ORIGINAL Anthropic body — a mid-request cross can land on codex
// when no wish was ever recorded for that hop — and until #41 this resolution
// was pinned only in the FALSE direction (every other class→codex test is
// non-streaming). This is the test that guards the true direction.
func TestE2EClassCodexMemberStreamSSE(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()

	resp := h.postStreamSess(t, "claude-frontier", true, "sess-codex-stream-1")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}
	b, _ := io.ReadAll(resp.Body)
	frames := parseAnthFrames(t, string(b))
	want := []string{"message_start", "content_block_start", "content_block_delta",
		"content_block_delta", "content_block_stop", "message_delta", "message_stop"}
	if got := frameEvents(frames); !strings.HasPrefix(strings.Join(got, ","), strings.Join(want, ",")) {
		t.Fatalf("frame events = %v, want %v", got, want)
	}
	hit := h.codexHitOrFail(t)
	if hit.path != codexResponsesPath {
		t.Fatalf("class codex hit path = %q, want the codex backend", hit.path)
	}
	h.noAnthropicLeak(t)
}
