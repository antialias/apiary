package main

// Phase 0 of the OpenAI-wire plan (spikes/openai-inbound-wire/PLAN.md):
// /v1/* is default-DENY. Before this, any unknown /v1/* POST fell through the
// matcher chain to laneForModel + decide() and was forwarded to
// api.anthropic.com under a minted subscription bearer. These tests pin the
// allowlist (exactly /v1/messages and /v1/messages/count_tokens), prove a
// denied request produces ZERO upstream traffic, prove allowed paths still
// forward the client's body byte-identical, and pin the body cap (413, zero
// upstream).

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

// noUpstream asserts the fake upstream saw nothing — the whole point of a deny.
func (env *e2eEnv) noUpstream(t *testing.T) {
	t.Helper()
	select {
	case h := <-env.seen:
		t.Fatalf("denied request reached upstream: model=%q auth=%q", h.model, h.auth)
	case <-time.After(150 * time.Millisecond):
	}
}

func (env *e2eEnv) postPath(t *testing.T, path, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest("POST", env.proxy.URL+path, bytes.NewReader([]byte(body)))
	req.Header.Set("Authorization", "Bearer CLIENT")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request failed: %v", err)
	}
	return resp
}

func TestUnknownV1PathDenied(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	// Paths that must never enter the routing chain, including the OpenAI
	// shapes a misconfigured client would send, a would-be batch endpoint,
	// and a traversal-flavored variant. (POST /v1/chat/completions is no
	// longer here: #96 made it the OpenAI inbound wire — its own default-deny
	// contract lives in e2e_openai_test.go.)
	for _, p := range []string{
		"/v1/complete",
		"/v1/responses",
		"/v1/messages/batches",
		"/v1/messages/",
		"/v1/anything/at/all",
	} {
		body := `{"model":"claude-fable-5","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`
		resp := env.postPath(t, p, body)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s: want 404, got %d (%s)", p, resp.StatusCode, raw)
		}
		var envl struct {
			Type  string `json:"type"`
			Error struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(raw, &envl); err != nil || envl.Type != "error" || envl.Error.Type != "not_found_error" {
			t.Errorf("%s: want Anthropic-shaped not_found_error envelope, got %s", p, raw)
		}
		env.noUpstream(t)
	}
}

// The two allowlisted paths still forward — and the body arrives upstream
// byte-identical to what the client sent (no injection, no rewrite for a
// plain request on the default lane).
func TestAllowedV1PathsForwardByteIdentical(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	for _, p := range []string{"/v1/messages", "/v1/messages/count_tokens"} {
		body := fmt.Sprintf(`{"model":"claude-haiku-4-5","max_tokens":16,"messages":[{"role":"user","content":"path %s"}]}`, p)
		resp := env.postPath(t, p, body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("%s: want 200 from fake upstream, got %d", p, resp.StatusCode)
		}
		h := env.hit(t)
		if string(h.body) != body {
			t.Errorf("%s: upstream body not byte-identical:\n sent %s\n got  %s", p, body, h.body)
		}
	}
}

func TestOversizedBody413(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	oldMax := maxBodyBytes
	maxBodyBytes = 4096
	defer func() { maxBodyBytes = oldMax }()

	big := `{"model":"claude-haiku-4-5","pad":"` + strings.Repeat("x", 8192) + `"}`
	resp := env.postPath(t, "/v1/messages", big)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413, got %d (%s)", resp.StatusCode, raw)
	}
	if !bytes.Contains(raw, []byte("request_too_large")) {
		t.Errorf("want request_too_large error type, got %s", raw)
	}
	env.noUpstream(t)

	// At exactly the cap the request still forwards (boundary pin).
	exact := `{"model":"claude-haiku-4-5","pad":"` +
		strings.Repeat("x", 4096-len(`{"model":"claude-haiku-4-5","pad":""}`)) + `"}`
	if int64(len(exact)) != maxBodyBytes {
		t.Fatalf("test bug: boundary body is %d bytes, want %d", len(exact), maxBodyBytes)
	}
	resp = env.postPath(t, "/v1/messages", exact)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("body at exactly the cap: want 200, got %d", resp.StatusCode)
	}
	env.hit(t)
}
