package main

// Tests for the #35 server deployment surface: the client gate (SWITCH_CLIENT_SECRET
// default-deny + strip-after-validate) and the oauth beta-flag re-add on every
// Anthropic-bound path. A client in ANTHROPIC_AUTH_TOKEN mode drops the
// oauth-2025-04-20 flag from anthropic-beta (it believes it's on API-key auth),
// and Anthropic 401s an injected OAuth bearer without it — so the proxy must
// re-add it wherever it injects one: the Rewrite (direct lane), the lane 429
// retry, the class same-lane account swap, and the class provider→anthropic
// cross (which rebuilds a request that never saw the account Rewrite).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

// postRaw is postSess with FULL header control: no implicit Authorization, no
// implicit anthropic-beta — the AUTH_TOKEN-mode client shapes we're testing.
func postRaw(t *testing.T, env *e2eEnv, model, bearer, beta, session string) *http.Response {
	t.Helper()
	body := fmt.Sprintf(`{"model":%q,"max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`, model)
	req, _ := http.NewRequest("POST", env.proxy.URL+"/v1/messages", bytes.NewReader([]byte(body)))
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if beta != "" {
		req.Header.Set("anthropic-beta", beta)
	}
	if session != "" {
		req.Header.Set("x-claude-code-session-id", session)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request failed: %v", err)
	}
	return resp
}

// withClientSecret arms the gate for one test and rebuilds the proxy under it.
func withClientSecret(t *testing.T, env *e2eEnv, secret string) {
	t.Helper()
	old := clientSecret
	clientSecret = secret
	rebuildProxy(env)
	t.Cleanup(func() { clientSecret = old })
}

func TestEnsureOAuthBeta(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", oauthBetaFlag},
		{oauthBetaFlag, oauthBetaFlag},
		{"claude-code-20250219", "claude-code-20250219," + oauthBetaFlag},
		{"claude-code-20250219," + oauthBetaFlag, "claude-code-20250219," + oauthBetaFlag},
		{"claude-code-20250219, " + oauthBetaFlag + " ", "claude-code-20250219, " + oauthBetaFlag + " "},
	}
	for _, c := range cases {
		h := http.Header{}
		if c.in != "" {
			h.Set("anthropic-beta", c.in)
		}
		ensureOAuthBeta(h)
		if got := h.Get("anthropic-beta"); got != c.want {
			t.Errorf("ensureOAuthBeta(%q) = %q, want %q", c.in, got, c.want)
		}
		// idempotent: a second application never doubles the flag
		ensureOAuthBeta(h)
		if got := h.Get("anthropic-beta"); got != c.want {
			t.Errorf("ensureOAuthBeta twice on %q = %q, want %q", c.in, got, c.want)
		}
	}
}

// hasOAuthBeta reports whether the captured anthropic-beta header carries the flag.
func hasOAuthBeta(beta string) bool {
	for _, f := range strings.Split(beta, ",") {
		if strings.TrimSpace(f) == oauthBetaFlag {
			return true
		}
	}
	return false
}

// The gate: default-deny with only /_switch/healthz open, Anthropic-shaped 401,
// and — the part that matters most — the validated secret is STRIPPED so neither
// it nor any ambient client credential can ride an upstream request.
func TestE2EClientGate(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	const secret = "lan-secret-for-tests"
	withClientSecret(t, env, secret)

	// healthz is the ONLY open path
	resp, err := http.Get(env.proxy.URL + "/_switch/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("healthz must stay open, got %d", resp.StatusCode)
	}

	// no secret / wrong secret -> 401 with an authentication_error envelope
	for _, bearer := range []string{"", "wrong-secret"} {
		r := postRaw(t, env, "claude-haiku-4-5", bearer, "", "")
		b, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if r.StatusCode != http.StatusUnauthorized {
			t.Fatalf("bearer=%q: got %d, want 401", bearer, r.StatusCode)
		}
		var e struct {
			Type  string `json:"type"`
			Error struct {
				Type string `json:"type"`
			} `json:"error"`
		}
		if json.Unmarshal(b, &e) != nil || e.Type != "error" || e.Error.Type != "authentication_error" {
			t.Fatalf("401 body must be an Anthropic-shaped authentication_error, got %s", b)
		}
		select { // and nothing may have reached upstream
		case hit := <-env.seen:
			t.Fatalf("gated request leaked upstream: %+v", hit)
		default:
		}
	}

	// status and /v1/models are gated too (status leaks account emails)
	for _, path := range []string{"/_switch/status", "/v1/models"} {
		r, err := http.Get(env.proxy.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != http.StatusUnauthorized {
			t.Fatalf("GET %s without the secret: got %d, want 401", path, r.StatusCode)
		}
		req, _ := http.NewRequest("GET", env.proxy.URL+path, nil)
		req.Header.Set("Authorization", "Bearer "+secret)
		r, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != 200 {
			t.Fatalf("GET %s with the secret: got %d, want 200", path, r.StatusCode)
		}
	}

	// the right secret routes normally — and upstream sees the INJECTED account
	// bearer, never the client secret
	r := postRaw(t, env, "claude-haiku-4-5", secret, "", "")
	r.Body.Close()
	if r.StatusCode != 200 {
		t.Fatalf("authed inference got %d, want 200", r.StatusCode)
	}
	hit := env.hit(t)
	if hit.auth != "Bearer "+e2eToken(native) {
		t.Fatalf("upstream auth = %q, want the injected account bearer", hit.auth)
	}
	if strings.Contains(hit.auth, secret) {
		t.Fatal("the client secret leaked upstream")
	}

	// an unmatched path falls through to the reverse proxy: with the inbound
	// Authorization stripped and nothing injected, upstream must see NO auth at
	// all (before the gate existed this forwarded the client's header verbatim)
	req, _ := http.NewRequest("POST", env.proxy.URL+"/foo/bar", bytes.NewReader([]byte("x")))
	req.Header.Set("Authorization", "Bearer "+secret)
	r, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if hit := env.hit(t); hit.auth != "" {
		t.Fatalf("fall-through path forwarded auth upstream: %q — the secret (or any client credential) must die at the gate", hit.auth)
	}
}

// Anthropic-bound path 1+2: the direct lane Rewrite and the lane 429 retry both
// carry the oauth flag even when the AUTH_TOKEN-mode client sent none — and the
// client's own flags survive alongside it.
func TestE2EBetaFlagOnLanePaths(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	// no anthropic-beta at all from the client
	postRaw(t, env, "claude-haiku-4-5", "CLIENT", "", "").Body.Close()
	if hit := env.hit(t); !hasOAuthBeta(hit.beta) {
		t.Fatalf("direct lane: beta = %q, must include %s", hit.beta, oauthBetaFlag)
	}

	// client flags preserved, oauth appended
	postRaw(t, env, "claude-haiku-4-5", "CLIENT", "claude-code-20250219", "").Body.Close()
	hit := env.hit(t)
	if !strings.Contains(hit.beta, "claude-code-20250219") || !hasOAuthBeta(hit.beta) {
		t.Fatalf("direct lane: beta = %q, want the client's flag AND %s", hit.beta, oauthBetaFlag)
	}

	// lane 429 retry: the failover clone must keep the flag
	env.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"You have exceeded your weekly usage limit."}}`)
	env.deny429.Store(e2eToken(other))
	resp := postRaw(t, env, "claude-fable-5", "CLIENT", "", "")
	defer resp.Body.Close()
	first := env.hit(t)
	second := env.hit(t)
	if !hasOAuthBeta(first.beta) || !hasOAuthBeta(second.beta) {
		t.Fatalf("429 retry: beta first=%q second=%q, both must include %s", first.beta, second.beta, oauthBetaFlag)
	}
}

// Anthropic-bound paths 3+4: the class same-lane account swap and — the sharp
// edge — the provider→anthropic cross, whose rebuilt request never went through
// the account Rewrite and would otherwise ship flag-less.
func TestE2EBetaFlagOnClassPaths(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()

	// (a) provider→anthropic cross: bind to kimi, 429 the hosted upstream
	h.bindTo.Store("kimi/kimi-k3")
	h.deny.Store(true)
	resp := postRaw(t, h.e2eEnv, "claude-frontier", "CLIENT", "", "sess-beta-cross")
	resp.Body.Close()
	h.hostedHit(t) // attempt 1: kimi, 429'd
	if cross := h.hit(t); !hasOAuthBeta(cross.beta) {
		t.Fatalf("provider→anthropic cross: beta = %q, must include %s (rebuildForMember misses the re-add)",
			cross.beta, oauthBetaFlag)
	}
	h.deny.Store(false)
	h.bindTo.Store("") // (b)'s fresh session must bind to the P1 anthropic member

	// (b) same-lane account swap on an anthropic member: both attempts flagged
	lan.recMu.Lock()
	lan.records = map[recKey]refusalRecord{}
	lan.recMu.Unlock()
	h.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"You have exceeded your weekly usage limit."}}`)
	h.deny429.Store(e2eToken(other)) // only the first account 429s; the swap lands
	resp = postRaw(t, h.e2eEnv, "claude-frontier", "CLIENT", "", "sess-beta-swap")
	defer resp.Body.Close()
	first := h.hit(t)
	second := h.hit(t)
	if !hasOAuthBeta(first.beta) || !hasOAuthBeta(second.beta) {
		t.Fatalf("class same-lane swap: beta first=%q second=%q, both must include %s",
			first.beta, second.beta, oauthBetaFlag)
	}
}

// With the local provider not configured, a stale claude-local-* picker id
// (the rows #135 deleted) must die as a typed 503 — never fall through to lane
// routing and reach Anthropic under an injected account bearer. The provider
// configured is e2e_local_test.go's TestE2ELocalStaleTagIsRefusedNeverForwarded.
func TestE2ELocalStaleTagWhenDisabled(t *testing.T) {
	h, restore := newE2EHosted(t, "/anthropic") // localUpstream = "" in this harness
	defer restore()

	resp := h.post(t, "claude-local-qwen3.6:35b-a3b", "CLIENT")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want a typed 503", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "direct local model ids were removed") {
		t.Fatalf("503 body lacks the deleted-id reason: %s", b)
	}
	h.noAnthropicLeak(t)
	select {
	case hit := <-h.hosted:
		t.Fatalf("local id leaked to a hosted provider: %+v", hit)
	default:
	}
}
