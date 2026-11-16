package main

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"
)

// postWithCacheControl sends a /v1/messages request carrying a BARE 5-minute ephemeral
// breakpoint — the exact shape Claude Code emits by default — through the e2e proxy.
func postWithCacheControl(t *testing.T, env *e2eEnv, model string) {
	t.Helper()
	body := `{"model":"` + model + `","max_tokens":16,` +
		`"system":[{"type":"text","text":"sys","cache_control":{"type":"ephemeral"}}],` +
		`"messages":[{"role":"user","content":"hi"}]}`
	req, _ := http.NewRequest("POST", env.proxy.URL+"/v1/messages", bytes.NewReader([]byte(body)))
	req.Header.Set("Authorization", "Bearer CLIENT")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("anthropic-beta", "oauth-2025-04-20") // the client's own flag — must survive
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request failed: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

// End-to-end proof of the forced-1h on-path: with the flag on, an Anthropic-lane
// request reaches the upstream with the extended-cache beta AND a ttl:"1h" body, and
// the client's own beta flag is preserved; with it off, the request passes through
// untouched. (kimi/codex exclusion is structural — provider lanes take the bypass
// branch and never reach this Rewrite else-branch, and their actuation never calls
// force1hCacheControl — so it is guaranteed by the split, not re-litigated here.)
func TestE2EForce1hCacheOnAnthropicLane(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	saved := forceCache1h
	defer func() { forceCache1h = saved }()

	// ON: upstream sees the extended-cache beta (alongside the client's oauth flag) and
	// the body upgraded to 1h, with no bare 5m breakpoint left behind.
	forceCache1h = true
	postWithCacheControl(t, env, "claude-fable-5")
	h := env.hit(t)
	if !strings.Contains(h.beta, extendedCacheBetaFlag) {
		t.Errorf("on: upstream anthropic-beta %q missing %q", h.beta, extendedCacheBetaFlag)
	}
	if !strings.Contains(h.beta, "oauth-2025-04-20") {
		t.Errorf("on: the client's existing beta flag was dropped: %q", h.beta)
	}
	if !bytes.Contains(h.body, []byte(`"cache_control":{"type":"ephemeral","ttl":"1h"}`)) {
		t.Errorf("on: body not upgraded to 1h: %s", h.body)
	}
	if bytes.Contains(h.body, []byte(`"cache_control":{"type":"ephemeral"}`)) {
		t.Errorf("on: a bare 5m breakpoint survived the rewrite: %s", h.body)
	}

	// OFF: neither the beta nor the ttl is added — the request is forwarded verbatim.
	forceCache1h = false
	postWithCacheControl(t, env, "claude-fable-5")
	h = env.hit(t)
	if strings.Contains(h.beta, extendedCacheBetaFlag) {
		t.Errorf("off: extended-cache beta leaked with the flag off: %q", h.beta)
	}
	if !bytes.Contains(h.body, []byte(`"cache_control":{"type":"ephemeral"}`)) {
		t.Errorf("off: bare breakpoint should be untouched: %s", h.body)
	}
	if bytes.Contains(h.body, []byte(`"ttl":"1h"`)) {
		t.Errorf("off: ttl was added despite the flag being off: %s", h.body)
	}
}
