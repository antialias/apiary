package main

import (
	"context"
	"net/http"
	"strings"
)

// The harness registry: which coding harness sent a request. The ids are a
// contract shared with Scry (lib/harness.ts), which turns any other value into
// null, so an id here is never renamed and a new one is added in both places.
// The spec is routing-observability/docs/design/console-sessions/README.md,
// "Harness registry".
const (
	harnessClaudeCode   = "claude-code"
	harnessCodex        = "codex"
	harnessAntigravity  = "antigravity"
	harnessCursor       = "cursor"
	harnessQwenCode     = "qwen-code"
	harnessDeepSeek     = "deepseek-harness"
	harnessKimiCode     = "kimi-code"
	harnessZCode        = "zcode"
	harnessUnrecognised = "unrecognised"
)

// harnessEntry is one row of the registry: its id and the name it is shown as.
type harnessEntry struct {
	ID   string
	Name string
}

// harnessRegistry lists every id recognizeHarness can return, in the order the
// console shows them. codex, antigravity and cursor are recognised by nothing
// yet: none of them reaches the proxy in a form it accepts.
var harnessRegistry = []harnessEntry{
	{harnessClaudeCode, "Claude Code"},
	{harnessCodex, "Codex"},
	{harnessAntigravity, "Antigravity"},
	{harnessCursor, "Cursor"},
	{harnessQwenCode, "Qwen Code"},
	{harnessDeepSeek, "DeepSeek Harness"},
	{harnessKimiCode, "Kimi Code"},
	{harnessZCode, "ZCode (GLM)"},
	{harnessUnrecognised, "Unrecognised"},
}

// harnessUAPrefixes are the harnesses known by their own user agent.
var harnessUAPrefixes = []struct{ prefix, id string }{
	{"QwenCode/", harnessQwenCode},
	{"deepseek-harness/", harnessDeepSeek},
	{"kimi-code-cli/", harnessKimiCode},
	{"ZCode/", harnessZCode},
}

// recognizeHarness names the harness that sent a request, from its headers and
// the text of the first `system` block (reqFacts.FirstSystem). Claude Code is
// known by the billing-header block it puts first in `system`; without it, by a
// claude-cli user agent together with a UUID session header. A harness that
// borrows Claude Code's user agent through a base URL (Qwen Code) sends neither
// the billing block nor a UUID session, and the agent gateway's session id is a constant,
// so both read as unrecognised rather than as Claude Code.
func recognizeHarness(h http.Header, firstSystemText string) string {
	if strings.HasPrefix(firstSystemText, tapBillingHeader) && strings.Contains(firstSystemText, "cc_version=") {
		return harnessClaudeCode
	}
	ua := h.Get("User-Agent")
	for _, p := range harnessUAPrefixes {
		if strings.HasPrefix(ua, p.prefix) {
			return p.id
		}
	}
	if strings.HasPrefix(ua, "claude-cli/") && isUUID(h.Get("x-claude-code-session-id")) {
		return harnessClaudeCode
	}
	return harnessUnrecognised
}

// isUUID says s is the 8-4-4-4-12 hex form Claude Code's session ids take.
func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
				return false
			}
		}
	}
	return true
}

// The harness is worked out once per request, on the request path, and carried
// on its context, so the turn event and the route_decision always agree.
type harnessKeyT struct{}

func withHarness(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, harnessKeyT{}, id)
}

// harnessFrom is the request's harness, or unrecognised when none was stamped
// (a request that never reached the /v1 body read).
func harnessFrom(ctx context.Context) string {
	if ctx != nil {
		if id, ok := ctx.Value(harnessKeyT{}).(string); ok && id != "" {
			return id
		}
	}
	return harnessUnrecognised
}
