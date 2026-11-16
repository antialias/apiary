package main

// e2e tests for the `local` class (#135) through the REAL handler stack: the
// a LAN PC's llama-server (router mode) is the static provider "local", reached
// only as a member of the class claude-local, which the fake brain binds per
// session like any other class. They prove the contract: the session's member
// model is rewritten into the body and the client's credentials are stripped;
// an unreachable PC is a 502 naming the member, with no walk, no record and no
// alert; llama-server's over-window refusal reaches the client as Anthropic's
// "prompt is too long" 400, and any other 400 as it was sent, neither walked,
// retried, recorded nor scored; count_tokens reaches the member under its own
// model name; a stale claude-local-<tag> picker id is refused typed and never
// forwarded; and /v1/models states the class's real window.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// localBadRequest is a 400 from the local runtime that is not the over-window
// refusal: llama-server b12110's router envelope (server-common.cpp
// format_error_response, wrapped as {"error": …} by res_err in
// server-models.cpp) for a model it does not know.
const localBadRequest = `{"error":{"code":400,"message":"model 'local-nope' not found","type":"invalid_request_error"}}`

// llamaOverWindowBody is llama-server b12110's over-window refusal as the child
// sends it and the router passes it through: format_error_response's envelope
// plus the two counts server_task_result_error::to_json adds for this type,
// under server_res_generator::error's {"error": …}. msg is one of
// server-context.cpp's two wordings.
func llamaOverWindowBody(msg string, n, m int) string {
	return fmt.Sprintf(`{"error":{"code":400,"message":%q,"type":"exceed_context_size_error","n_prompt_tokens":%d,"n_ctx":%d}}`,
		msg, n, m)
}

type e2eLocalClass struct {
	*e2eHosted
	llama     *httptest.Server
	localSeen chan e2eProvHit
	refuse    atomic.Bool  // when set, /v1/messages answers localBadRequest with a 400
	overBody  atomic.Value // string; when non-empty, /v1/messages answers it with a 400
}

// localClassMembers is the class document's three members exactly as the
// brain's /preferred serves them after a fresh ok poll_local (dumped from
// _preferred_payload, test_exporter TestLocalRoutes): provider "local", the
// preset windows, r_p 1.0, no quota number (targets {"local": null}) and,
// because the reading is unmetered, status ok / usable.
func localClassMembers() []map[string]any {
	included := func() map[string]any {
		return map[string]any{"check": "included", "pot": nil, "reason": nil, "until": nil, "basis": "fresh"}
	}
	m := func(prio int, model string, window int) map[string]any {
		return map[string]any{"id": "local/" + model, "route": "local/local/" + model, "scope": model,
			"priority": prio, "kind": "provider", "provider": "local", "model": model,
			"context_window": window, "effective_window": window, "anthropic_token_ratio": 1.0,
			"calibration_ref": "num_ctx, model's own tokenizer", "reserve_ceiling": nil,
			"pay": []any{}, "stop_at": nil, "targets": map[string]any{"local": nil},
			"checks": map[string]any{"local": included()}, "check": included(),
			"incumbent": "local", "effective": nil, "reserved": false, "usable": true, "status": "ok"}
	}
	return []map[string]any{
		m(1, "local-gemma4-12b", 65536),
		m(2, "local-gpt-oss-20b", 65536),
		m(3, "local-qwen3-coder-30b", 131072),
	}
}

// localStanding is the brain's /preferred accounts["local"] after a fresh ok
// poll_local, field for field: an open, unmetered allowance with no window,
// and a credits pot that is closed.
func localStanding() map[string]any {
	now := float64(time.Now().Add(-time.Minute).UnixNano()) / 1e9
	derived := func(state string) map[string]any { return map[string]any{"state": state, "basis": "derived"} }
	return map[string]any{"provider": "local", "kind": "subscription", "as_of": now, "next_poll_at": now + 300,
		"recheck_epoch": nil, "source": "GET /models",
		"plan": map[string]any{"label": "LAN PC", "basis": "derived"},
		"allowance": map[string]any{"type": "allowance", "entitlement": derived("active"), "open": true,
			"open_basis": "derived", "unmetered": true, "limits": map[string]any{}, "windows": map[string]any{}},
		"money": map[string]any{"credits": map[string]any{"type": "money", "unit": nil, "unit_usd": nil,
			"entitlement": derived("none"), "open": false, "open_basis": "derived",
			"limits": map[string]any{}, "balance": nil}},
		"beside": []any{}, "models": map[string]any{}}
}

// newE2ELocalClass is the hosted harness (kimi and gpt configured, so a walk
// would have somewhere to go) plus an httptest llama-server router as the local
// provider, and a fake brain serving the one class `local`. Like the router, it
// routes every POST by the body's "model" and refuses one it does not hold.
func newE2ELocalClass(t *testing.T) (*e2eLocalClass, func()) {
	t.Helper()
	h, restoreHosted := newE2EHosted(t, "/coding")
	lc := &e2eLocalClass{e2eHosted: h, localSeen: make(chan e2eProvHit, 32)}
	lc.overBody.Store("")
	held := map[string]bool{"local-gemma4-12b": true, "local-gpt-oss-20b": true, "local-qwen3-coder-30b": true}
	lc.llama = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		model := extractModel(body)
		lc.localSeen <- e2eProvHit{auth: r.Header.Get("Authorization"), model: model,
			path: r.URL.Path, header: r.Header.Clone(), body: body}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if !held[model] {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, `{"error":{"code":400,"message":"model '%s' not found","type":"invalid_request_error"}}`, model)
			return
		}
		if r.URL.Path == "/v1/messages/count_tokens" {
			_, _ = w.Write([]byte(`{"input_tokens":42}`))
			return
		}
		if lc.refuse.Load() {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(localBadRequest))
			return
		}
		if ob := lc.overBody.Load().(string); ob != "" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(ob))
			return
		}
		_, _ = w.Write([]byte(`{"type":"message","role":"assistant","content":[{"type":"text","text":"local ok"}],` +
			`"usage":{"input_tokens":10,"output_tokens":2}}`))
	}))
	oldLocal := localUpstream
	localUpstream = lc.llama.URL

	h.brainMu.Lock()
	h.className, h.classDisplay, h.classWindow = "local", "Local (LAN PC)", 65536
	h.classMembers = localClassMembers()
	if h.standings == nil {
		h.standings = map[string]any{}
	}
	h.standings["local"] = localStanding()
	h.brainMu.Unlock()
	withModelsFile(t, `[{"name":"fable-5","display":"Claude Fable 5","type":"anthropic","context_window":200000}]`)
	fetchPreferredSync()
	rebuildProxy(h.e2eEnv)

	return lc, func() {
		lc.llama.Close()
		localUpstream = oldLocal
		restoreHosted()
	}
}

func (lc *e2eLocalClass) localHit(t *testing.T) e2eProvHit {
	t.Helper()
	select {
	case hit := <-lc.localSeen:
		return hit
	case <-time.After(3 * time.Second):
		t.Fatal("the local runtime never saw the request")
		return e2eProvHit{}
	}
}

func (lc *e2eLocalClass) noLocalHit(t *testing.T) {
	t.Helper()
	select {
	case hit := <-lc.localSeen:
		t.Fatalf("request reached the local runtime: %+v", hit)
	default:
	}
}

// noOtherUpstream asserts nothing reached Anthropic, kimi or codex: local never
// falls back to another provider.
func (lc *e2eLocalClass) noOtherUpstream(t *testing.T) {
	t.Helper()
	lc.noAnthropicLeak(t)
	lc.noCodexLeak(t)
	select {
	case hit := <-lc.hosted:
		t.Fatalf("request walked to kimi: %+v", hit)
	default:
	}
}

func (lc *e2eLocalClass) noAlert(t *testing.T) {
	t.Helper()
	select {
	case msg := <-lc.bridge:
		t.Fatalf("a local request fired an alert: %s", msg)
	case <-time.After(150 * time.Millisecond):
	}
}

// The bound member's own model is rewritten into the body, the client's
// credentials never reach the PC, and nothing else is dialled.
func TestE2ELocalClassRoutesToLlamaServerAuthStripped(t *testing.T) {
	lc, restore := newE2ELocalClass(t)
	defer restore()

	req, _ := http.NewRequest("POST", lc.proxy.URL+"/v1/messages",
		strings.NewReader(`{"model":"claude-local","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer client-oauth-bearer")
	req.Header.Set("X-Api-Key", "client-api-key")
	req.Header.Set("X-Claude-Code-Session-Id", "sess-local-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !strings.Contains(string(b), "local ok") {
		t.Fatalf("status %d body %s, want the local runtime's 200", resp.StatusCode, b)
	}
	hit := lc.localHit(t)
	if hit.model != "local-gemma4-12b" {
		t.Fatalf("the PC saw model %q, want the P1 member's local-gemma4-12b", hit.model)
	}
	if hit.path != "/v1/messages" {
		t.Fatalf("path = %q, want /v1/messages", hit.path)
	}
	if hit.auth != "" || hit.header.Get("X-Api-Key") != "" {
		t.Fatalf("a client credential reached the PC: auth=%q x-api-key=%q", hit.auth, hit.header.Get("X-Api-Key"))
	}
	lc.brainMu.Lock()
	bound := lc.binds["sess-local-1"]
	lc.brainMu.Unlock()
	if bound != "local/local-gemma4-12b" {
		t.Fatalf("brain bound %q, want local/local-gemma4-12b", bound)
	}
	lc.noOtherUpstream(t)
}

// A session bound to another member (a class-pin) is served by that member's
// own model: one model is never presented as another.
func TestE2ELocalClassBoundMemberServesItsOwnModel(t *testing.T) {
	lc, restore := newE2ELocalClass(t)
	defer restore()
	lc.bindTo.Store("local/local-qwen3-coder-30b")

	resp := lc.postSess(t, "claude-local", "CLIENT", "sess-local-qwen")
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if hit := lc.localHit(t); hit.model != "local-qwen3-coder-30b" {
		t.Fatalf("the PC saw model %q, want local-qwen3-coder-30b", hit.model)
	}
	lc.noOtherUpstream(t)
}

// When llama-server is stopped the port refuses connections.
// The client gets the proxy's 502 upstream_error naming the member; the class
// does not walk (local is not under SWITCH_HEALTH enforce), nothing else is
// dialled, no record is written and no alert fires.
func TestE2ELocalClassPCOffIs502NoWalkNoAlert(t *testing.T) {
	lc, restore := newE2ELocalClass(t)
	defer restore()
	lc.llama.Close() // the listener is gone: connection refused

	resp := lc.postSess(t, "claude-local", "CLIENT", "sess-local-off")
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d body %s, want 502", resp.StatusCode, b)
	}
	var env struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatalf("502 body is not an Anthropic error envelope: %v: %s", err, b)
	}
	if env.Type != "error" || env.Error.Type != "upstream_error" ||
		!strings.Contains(env.Error.Message, "local/local-gemma4-12b") {
		t.Fatalf("error = %+v, want upstream_error naming local/local-gemma4-12b", env)
	}
	if n := lc.bindCalls.Load(); n != 1 {
		t.Fatalf("bind calls = %d, want 1: an unreachable PC is not walked", n)
	}
	lc.brainMu.Lock()
	bound := lc.binds["sess-local-off"]
	lc.brainMu.Unlock()
	if bound != "local/local-gemma4-12b" {
		t.Fatalf("session moved to %q, want it still on local/local-gemma4-12b", bound)
	}
	lc.noOtherUpstream(t)
	if n := liveRecords(); n != 0 {
		t.Fatalf("an unreachable PC wrote %d record(s)", n)
	}
	lc.noAlert(t)
}

// A 400 from the local runtime that is not the over-window refusal reaches the
// client unchanged, and the class neither walks nor records it.
func TestE2ELocalClassBadRequestPassesThrough(t *testing.T) {
	lc, restore := newE2ELocalClass(t)
	defer restore()
	lc.refuse.Store(true)

	resp := lc.postSess(t, "claude-local", "CLIENT", "sess-local-400")
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d body %s, want the runtime's 400", resp.StatusCode, b)
	}
	if string(b) != localBadRequest {
		t.Fatalf("body %s, want the runtime's body unchanged", b)
	}
	if hit := lc.localHit(t); hit.model != "local-gemma4-12b" {
		t.Fatalf("the PC saw model %q", hit.model)
	}
	lc.noLocalHit(t) // exactly one dial: no retry
	if n := lc.bindCalls.Load(); n != 1 {
		t.Fatalf("bind calls = %d, want 1: a refusal by meaning is not walked", n)
	}
	lc.noOtherUpstream(t)
	if n := liveRecords(); n != 0 {
		t.Fatalf("a 400 wrote %d record(s)", n)
	}
	lc.noAlert(t)
}

// A picker cache still holding a deleted claude-local-<tag> row: refused typed,
// never forwarded to the PC or anywhere else.
func TestE2ELocalStaleTagIsRefusedNeverForwarded(t *testing.T) {
	lc, restore := newE2ELocalClass(t)
	defer restore()

	for _, model := range []string{"claude-local-qwen3-coder:30b", "claude-local-gemma4:12b", "claude-local-mistral"} {
		resp := lc.post(t, model, "CLIENT")
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("%s: status %d body %s, want the typed 503", model, resp.StatusCode, b)
		}
		// A tag with a digit is the deleted namespace's own refusal; a
		// digit-free one is class-shaped and refused as an unknown class.
		if strings.ContainsAny(model, "0123456789") && !strings.Contains(string(b), "direct local model ids were removed") {
			t.Fatalf("%s: body %s, want the deleted-id reason", model, b)
		}
	}
	lc.noLocalHit(t)
	lc.noOtherUpstream(t)
	if n := lc.bindCalls.Load(); n != 0 {
		t.Fatalf("bind calls = %d, want 0: a stale tag is no class", n)
	}
}

// /v1/models lists claude-local as a class with its honest 65536 window, and no
// claude-local-<tag> row: the provider adds none of its own.
func TestE2ELocalClassModelsHonestWindow(t *testing.T) {
	lc, restore := newE2ELocalClass(t)
	defer restore()

	resp, err := http.Get(lc.proxy.URL + "/v1/models?limit=1000")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Data []struct {
			ID            string `json:"id"`
			DisplayName   string `json:"display_name"`
			Kind          string `json:"kind"`
			ContextWindow int    `json:"context_window"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, d := range out.Data {
		if strings.HasPrefix(d.ID, "claude-local-") {
			t.Fatalf("/v1/models lists %q: the local provider advertises no row of its own", d.ID)
		}
		if d.ID == "claude-local" {
			found = true
			if d.Kind != kindClass || d.ContextWindow != 65536 || d.DisplayName != "Local (LAN PC)" {
				t.Fatalf("claude-local row = %+v, want a class at 65536 named Local (LAN PC)", d)
			}
		}
	}
	if !found {
		t.Fatalf("claude-local missing from /v1/models: %+v", out.Data)
	}
	lc.noLocalHit(t) // discovery never asks the PC
}

// /_switch/status lists the local provider like any other: its upstream, the
// strip wire, and no model ids of its own.
func TestE2ELocalStatusListsTheProvider(t *testing.T) {
	lc, restore := newE2ELocalClass(t)
	defer restore()

	resp, err := http.Get(lc.proxy.URL + "/_switch/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var status struct {
		Providers []struct {
			Name     string   `json:"name"`
			Upstream string   `json:"upstream"`
			Auth     string   `json:"auth"`
			Models   []string `json:"models"`
		} `json:"direct_providers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	for _, p := range status.Providers {
		if p.Name == "local" {
			if p.Upstream != lc.llama.URL || p.Auth != "strip" || len(p.Models) != 0 {
				t.Fatalf("local provider = %+v", p)
			}
			return
		}
	}
	t.Fatalf("no local provider in status: %+v", status.Providers)
}

// postLocal sends one claude-local request with a body of the caller's own.
func (lc *e2eLocalClass) postLocal(t *testing.T, path, body, session string) (*http.Response, []byte) {
	t.Helper()
	req, _ := http.NewRequest("POST", lc.proxy.URL+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer CLIENT")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-claude-code-session-id", session)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

// llama-server's over-window refusal, in both of its wordings and whether or
// not the client asked to stream (llama-server answers a stream's first-result
// error unstreamed), reaches the client as Anthropic's own "prompt is too long"
// 400 with the counts from llama-server's message: the one shape Claude Code
// compacts and retries on. It is dialled once, not walked, and writes no record.
func TestE2ELocalClassOverWindowIsPromptTooLong(t *testing.T) {
	for _, tc := range []struct {
		name, msg string
		n, m      int
	}{
		{"request", "request (70123 tokens) exceeds the available context size (65536 tokens), try increasing it", 70123, 65536},
		{"input", "input (140000 tokens) is larger than the max context size (131072 tokens). skipping", 140000, 131072},
	} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%v", tc.name, stream), func(t *testing.T) {
				lc, restore := newE2ELocalClass(t)
				defer restore()
				lc.overBody.Store(llamaOverWindowBody(tc.msg, tc.n, tc.m))

				body := fmt.Sprintf(`{"model":"claude-local","max_tokens":16,"stream":%v,"messages":[{"role":"user","content":"hi"}]}`, stream)
				resp, b := lc.postLocal(t, "/v1/messages", body, "sess-local-over")
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("status %d body %s, want 400", resp.StatusCode, b)
				}
				want := fmt.Sprintf(`{"error":{"message":"prompt is too long: %d tokens \u003e %d maximum","type":"invalid_request_error"},"type":"error"}`, tc.n, tc.m)
				if string(b) != want {
					t.Fatalf("body\n got %s\nwant %s", b, want)
				}
				var env struct {
					Type  string                         `json:"type"`
					Error struct{ Type, Message string } `json:"error"`
				}
				if err := json.Unmarshal(b, &env); err != nil || env.Type != "error" ||
					env.Error.Message != fmt.Sprintf("prompt is too long: %d tokens > %d maximum", tc.n, tc.m) {
					t.Fatalf("envelope = %+v (%v)", env, err)
				}
				if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
					t.Fatalf("Content-Type = %q", ct)
				}
				lc.localHit(t)
				lc.noLocalHit(t) // exactly one dial: no retry
				if n := lc.bindCalls.Load(); n != 1 {
					t.Fatalf("bind calls = %d, want 1: an over-window prompt is not walked", n)
				}
				lc.noOtherUpstream(t)
				if n := liveRecords(); n != 0 {
					t.Fatalf("an over-window 400 wrote %d record(s)", n)
				}
			})
		}
	}
}

// A 400 from a class member, over-window or not, is the client's to handle:
// under SWITCH_HEALTH observe and enforce alike it writes no health sample,
// the member stays where it was, and nothing walks.
func TestE2ELocalClass400NoHealthSampleNoWalk(t *testing.T) {
	for _, mode := range []healthMode{healthObserve, healthEnforce} {
		for _, over := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/over=%v", mode, over), func(t *testing.T) {
				lc, restore := newE2ELocalClass(t)
				defer restore()
				hs := newHealthStore(defaultHealthCfg())
				armHealth(t, hs)
				if mode == healthEnforce {
					armEnforce(t)
				} else {
					armObserve(t)
				}
				if over {
					lc.overBody.Store(llamaOverWindowBody(
						"request (70123 tokens) exceeds the available context size (65536 tokens), try increasing it", 70123, 65536))
				} else {
					lc.refuse.Store(true)
				}
				resp := lc.postSess(t, "claude-local", "CLIENT", "sess-local-400-health")
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("status %d body %s, want 400", resp.StatusCode, b)
				}
				if !over && string(b) != localBadRequest {
					t.Fatalf("body %s, want the runtime's own", b)
				}
				lc.localHit(t)
				lc.noLocalHit(t)
				if n := lc.bindCalls.Load(); n != 1 {
					t.Fatalf("bind calls = %d, want 1", n)
				}
				lc.noOtherUpstream(t)
				time.Sleep(50 * time.Millisecond) // a sample would be written before the client's EOF
				hs.mu.Lock()
				defer hs.mu.Unlock()
				for key, st := range hs.keys {
					if len(st.ring) != 0 {
						t.Fatalf("health key %s has %d sample(s): a 400 counted against the member", key, len(st.ring))
					}
				}
			})
		}
	}
}

// count_tokens for claude-local reaches the PC on its own path, carrying the
// bound member's model name in the body (the router routes by it and refuses a
// request without one), and its answer comes back as sent.
func TestE2ELocalClassCountTokensReachesMember(t *testing.T) {
	lc, restore := newE2ELocalClass(t)
	defer restore()

	resp, b := lc.postLocal(t, "/v1/messages/count_tokens",
		`{"model":"claude-local","messages":[{"role":"user","content":"hi"}]}`, "sess-local-count")
	if resp.StatusCode != 200 || string(b) != `{"input_tokens":42}` {
		t.Fatalf("status %d body %s, want the PC's count", resp.StatusCode, b)
	}
	hit := lc.localHit(t)
	if hit.path != "/v1/messages/count_tokens" || hit.model != "local-gemma4-12b" {
		t.Fatalf("the PC saw path %q model %q, want /v1/messages/count_tokens for local-gemma4-12b", hit.path, hit.model)
	}
	if hit.auth != "" {
		t.Fatalf("a client credential reached the PC on count_tokens")
	}
	lc.noOtherUpstream(t)
}
