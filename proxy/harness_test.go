package main

import (
	"context"
	"net/http"
	"testing"
)

const ccTestSession = "3f2a9c1e-7b4d-4e8a-9c2f-1a2b3c4d5e6f"

func harnessOf(ua, session, body string) string {
	h := http.Header{}
	if ua != "" {
		h.Set("User-Agent", ua)
	}
	if session != "" {
		h.Set("x-claude-code-session-id", session)
	}
	return recognizeHarness(h, parseReqFacts(h, []byte(body)).FirstSystem)
}

func TestRecognizeHarness(t *testing.T) {
	plain := `{"model":"m","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`
	baseURLSystem := `{"model":"m","system":[{"type":"text","text":"You are Claude Code, Anthropic's official CLI for Claude."}],"messages":[{"role":"user","content":"hi"}]}`
	for _, tc := range []struct {
		name, ua, session, body, want string
	}{
		// The billing header alone is enough, on a create and on a continue.
		{"claude code create", "", "", tapThreadBody(`{"type":"create"}`, tapUserMsg("hi")), harnessClaudeCode},
		{"claude code continue", "", "", tapThreadBody(tapContinue("msg_1"), tapUserMsg("next")), harnessClaudeCode},
		{"claude code create, other user agent", "something/1.0", "x", tapThreadBody(`{"type":"create"}`, tapUserMsg("hi")), harnessClaudeCode},
		// Without it: a claude-cli user agent and a UUID session header.
		{"ua + uuid fallback", "claude-cli/2.1.287 (external, cli)", ccTestSession, plain, harnessClaudeCode},
		{"ua without uuid", "claude-cli/2.1.287 (external, cli)", "sess-1", plain, harnessUnrecognised},
		{"uuid without ua", "", ccTestSession, plain, harnessUnrecognised},
		// Qwen Code through a base URL calls itself claude-cli but sends
		// neither the billing header nor a UUID session header.
		{"qwen via base url", "claude-cli/1.0.0 (external, cli)", "", baseURLSystem, harnessUnrecognised},
		// The agent gateway's constant session id (e2e_codex_live_test.go's request).
		{"agent-gw", "", "agent-gw", `{"model":"claude-frontier","max_tokens":64,"system":"You are a helpful assistant.","messages":[{"role":"user","content":"Turn 1"}]}`, harnessUnrecognised},
		{"agent gateway with a claude-cli ua", "claude-cli/2.1.287", "agent-gw", plain, harnessUnrecognised},
		{"qwen code", "QwenCode/0.4.1 (darwin; arm64)", "", plain, harnessQwenCode},
		{"deepseek harness", "deepseek-harness/0.3.0", "", plain, harnessDeepSeek},
		{"kimi code", "kimi-code-cli/1.2.0", "", plain, harnessKimiCode},
		{"zcode", "ZCode/2.0.1", "", plain, harnessZCode},
		{"curl", "curl/8.7.1", "", plain, harnessUnrecognised},
		{"no body", "", "", "", harnessUnrecognised},
		// A billing-header-looking prefix with no cc_version is not Claude Code's.
		{"bare billing prefix", "", "", `{"model":"m","system":[{"type":"text","text":"x-anthropic-billing-header: nothing"}],"messages":[]}`, harnessUnrecognised},
	} {
		if got := harnessOf(tc.ua, tc.session, tc.body); got != tc.want {
			t.Errorf("%s: harness = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// Every id recognizeHarness can return is in the registry Scry mirrors, and the
// registry's ids are exactly the contract's, in order.
func TestHarnessRegistryIDs(t *testing.T) {
	want := []string{"claude-code", "codex", "antigravity", "cursor", "qwen-code", "deepseek-harness", "kimi-code", "zcode", "unrecognised"}
	if len(harnessRegistry) != len(want) {
		t.Fatalf("registry has %d entries, want %d", len(harnessRegistry), len(want))
	}
	for i, e := range harnessRegistry {
		if e.ID != want[i] || e.Name == "" {
			t.Fatalf("registry[%d] = %+v, want id %q with a name", i, e, want[i])
		}
	}
	in := map[string]bool{}
	for _, e := range harnessRegistry {
		in[e.ID] = true
	}
	for _, p := range harnessUAPrefixes {
		if !in[p.id] {
			t.Fatalf("user-agent prefix %q maps to %q, not in the registry", p.prefix, p.id)
		}
	}
}

func TestHarnessFromContext(t *testing.T) {
	if got := harnessFrom(nil); got != harnessUnrecognised { //nolint:staticcheck // nil is the contract under test
		t.Fatalf("nil ctx = %q", got)
	}
	if got := harnessFrom(context.Background()); got != harnessUnrecognised {
		t.Fatalf("unstamped ctx = %q", got)
	}
	if got := harnessFrom(withHarness(context.Background(), harnessKimiCode)); got != harnessKimiCode {
		t.Fatalf("stamped ctx = %q", got)
	}
}
