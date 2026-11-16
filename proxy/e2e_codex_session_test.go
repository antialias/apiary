package main

// The codex hop's session_id (the backend's prompt-cache affinity, measured in
// codexHeaders' comment) through the REAL handler stack: the client's
// x-claude-code-session-id rides through on a class hop bound to the gpt
// member and on a class cross INTO codex, and without one the value is derived from the system
// prompt, so it is stable for one prompt and differs between two.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// sessionFallbackFor is the fallback spelled out independently of codexSession,
// so a change to the derivation fails here instead of agreeing with itself.
func sessionFallbackFor(instructions string) string {
	sum := sha256.Sum256([]byte(instructions))
	return hex.EncodeToString(sum[:])[:32]
}

// postSystem sends a class request with a system prompt and, when session is
// non-empty, a Claude Code session id; classWithUsableGpt binds it to gpt.
func (h *e2eHosted) postSystem(t *testing.T, system, session string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"model": "claude-frontier", "max_tokens": 16, "system": system,
		"messages": []any{map[string]any{"role": "user", "content": "hi"}},
	})
	req, _ := http.NewRequest("POST", h.proxy.URL+"/v1/messages", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer CLIENT")
	req.Header.Set("Content-Type", "application/json")
	if session != "" {
		req.Header.Set("x-claude-code-session-id", session)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
}

// MUTATION GUARD for the header line in codexHeaders: without it the codex
// backend caches nothing, and nothing else would notice.
func TestE2ECodexSessionIsTheClientSession(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()

	h.postSystem(t, "You are terse.", "agent-gw")
	if hit := h.codexHitOrFail(t); hit.session != "agent-gw" {
		t.Fatalf("session_id = %q, want the inbound x-claude-code-session-id", hit.session)
	}
}

func TestE2ECodexSessionFallbackFollowsTheSystemPrompt(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()

	h.postSystem(t, "prompt A", "")
	a1 := h.codexHitOrFail(t).session
	h.postSystem(t, "prompt A", "")
	a2 := h.codexHitOrFail(t).session
	h.postSystem(t, "prompt B", "")
	b := h.codexHitOrFail(t).session

	if a1 != sessionFallbackFor("prompt A") {
		t.Fatalf("fallback = %q, want hex(sha256(instructions))[:32] = %q", a1, sessionFallbackFor("prompt A"))
	}
	if a2 != a1 {
		t.Fatalf("fallback not stable across one system prompt: %q then %q", a1, a2)
	}
	if b == a1 {
		t.Fatalf("two system prompts share fallback %q", b)
	}
}

// The class ladder's rebuild is the other caller of the wire's apply: a cross
// INTO codex starts from the original inbound headers, so the session survives.
func TestE2EClassCrossIntoCodexCarriesTheSession(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	h.brainMu.Lock()
	h.classMembers[1]["usable"] = true
	h.classMembers[1]["status"] = "ok"
	h.classMembers[1]["effective"] = 20.0
	h.brainMu.Unlock()
	fetchPreferredSync()
	h.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"You have exceeded your weekly usage limit."}}`)
	h.deny429.Store("*")

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-cross-cache")
	defer resp.Body.Close()
	h.hit(t)
	h.hit(t)
	if hop := h.codexHitOrFail(t); hop.session != "sess-cross-cache" {
		t.Fatalf("crossed codex hop session_id = %q, want the client's session", hop.session)
	}
}

// An inbound value the proxy can't vouch for goes out as its hash: still one
// stable value per client session, never the raw bytes.
func TestCodexSessionHashesUnreadableInbound(t *testing.T) {
	long := strings.Repeat("s", 129)
	for _, in := range []string{long, "sess-é", "sess\x01"} {
		got := codexSession(in, nil)
		if got == in || got != sessionFallbackFor(in) {
			t.Fatalf("codexSession(%q) = %q, want hex(sha256(inbound))[:32]", in, got)
		}
	}
	if got := codexSession(strings.Repeat("s", 128), nil); got != strings.Repeat("s", 128) {
		t.Fatalf("a 128-byte printable session was rewritten to %q", got)
	}
}
