package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestForce1hCacheControl_UpgradesBareEphemeral(t *testing.T) {
	in := []byte(`{"model":"m","system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral"}}]}`)
	out := string(force1hCacheControl(in))
	if !strings.Contains(out, `"cache_control":{"type":"ephemeral","ttl":"1h"}`) {
		t.Fatalf("expected ttl upgrade, got %s", out)
	}
	if strings.Contains(out, `{"type":"ephemeral"}`) {
		t.Fatalf("a bare ephemeral block survived: %s", out)
	}
}

// The whole point of the substring approach: cache_control lives at arbitrary depth
// (system[], messages[].content[], tools[]) and every occurrence must be upgraded.
func TestForce1hCacheControl_DepthAgnostic(t *testing.T) {
	in := []byte(`{"system":[{"cache_control":{"type":"ephemeral"}}],` +
		`"messages":[{"role":"user","content":[{"type":"text","text":"x","cache_control":{"type":"ephemeral"}}]}],` +
		`"tools":[{"name":"t","cache_control":{"type":"ephemeral"}}]}`)
	out := string(force1hCacheControl(in))
	if n := strings.Count(out, `"ttl":"1h"`); n != 3 {
		t.Fatalf("expected 3 upgrades, got %d: %s", n, out)
	}
	if strings.Contains(out, `{"type":"ephemeral"}`) {
		t.Fatalf("a bare block survived: %s", out)
	}
}

// Idempotent: an already-1h body is left byte-for-byte identical (the A+B compose case,
// where the client set ENABLE_PROMPT_CACHING_1H itself).
func TestForce1hCacheControl_NoDoubleAdd(t *testing.T) {
	in := []byte(`{"system":[{"cache_control":{"type":"ephemeral","ttl":"1h"}}]}`)
	out := force1hCacheControl(in)
	if string(out) != string(in) {
		t.Fatalf("already-1h body was modified: %s", out)
	}
	if strings.Count(string(out), `"ttl":"1h"`) != 1 {
		t.Fatalf("ttl duplicated: %s", out)
	}
}

// No cache_control at all: unchanged AND the same backing slice (no allocation) — the
// zero-cost path that keeps the on-flag case cheap for uncacheable requests.
func TestForce1hCacheControl_NoOpNoAlloc(t *testing.T) {
	in := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	out := force1hCacheControl(in)
	if string(out) != string(in) {
		t.Fatalf("body without cache_control was modified: %s", out)
	}
	if &out[0] != &in[0] {
		t.Fatalf("expected the same backing slice (no allocation) on no-op")
	}
}

// An explicit 5m ttl is a deliberate client choice — the bare pattern doesn't match it,
// so it is respected, not clobbered.
func TestForce1hCacheControl_RespectsExplicit5m(t *testing.T) {
	in := []byte(`{"system":[{"cache_control":{"type":"ephemeral","ttl":"5m"}}]}`)
	out := force1hCacheControl(in)
	if string(out) != string(in) {
		t.Fatalf("explicit 5m ttl was rewritten: %s", out)
	}
}

// Mixed: one block already 1h, one bare — only the bare one is upgraded, and nothing
// ends up double-tagged.
func TestForce1hCacheControl_MixedBlocks(t *testing.T) {
	in := []byte(`{"a":{"cache_control":{"type":"ephemeral","ttl":"1h"}},` +
		`"b":{"cache_control":{"type":"ephemeral"}}}`)
	out := string(force1hCacheControl(in))
	if n := strings.Count(out, `"ttl":"1h"`); n != 2 {
		t.Fatalf("expected both blocks at 1h, got %d: %s", n, out)
	}
	if strings.Contains(out, `{"type":"ephemeral"}`) {
		t.Fatalf("bare block survived: %s", out)
	}
}

// breakpointTTLs lists the ttl of every ephemeral breakpoint in the order Anthropic
// checks them (tools, then system, then messages, a tool result's own content
// included), with "" for one that names none, which Anthropic reads as 5m.
func breakpointTTLs(body []byte) ([]string, error) {
	var req map[string]any
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	var ttls []string
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case []any:
			for _, e := range x {
				walk(e)
			}
		case map[string]any:
			if cc, ok := x["cache_control"].(map[string]any); ok && cc["type"] == "ephemeral" {
				ttl, _ := cc["ttl"].(string)
				ttls = append(ttls, ttl)
			}
			walk(x["content"])
		}
	}
	walk(req["tools"])
	walk(req["system"])
	walk(req["messages"])
	return ttls, nil
}

// ttlsAcceptable is Anthropic's rule: no 1h breakpoint after a 5m one.
func ttlsAcceptable(ttls []string) bool {
	fell := false
	for _, ttl := range ttls {
		if ttl != "1h" {
			fell = true
		} else if fell {
			return false
		}
	}
	return true
}

func requireAcceptableTTLs(t *testing.T, body []byte) {
	t.Helper()
	ttls, err := breakpointTTLs(body)
	if err != nil {
		t.Fatalf("the rewrite left invalid JSON (%v): %s", err, body)
	}
	if !ttlsAcceptable(ttls) {
		t.Fatalf("a 1h breakpoint follows a 5m one (%q), which Anthropic answers 400: %s", ttls, body)
	}
}

// The request that stopped a session: Claude Code 2.1.289 compacting, which it
// leaves at 5m, so a global-scoped static system block and then bare breakpoints.
// Replacing only the bare literal made that 5m then 1h, and Anthropic refused every
// compaction with "system.3.cache_control.ttl".
func TestForce1hCacheControl_ClaudeCodeCompactionRequest(t *testing.T) {
	in := []byte(`{"model":"claude-opus-5-5","tools":[{"name":"Bash","input_schema":{"type":"object"}}],` +
		`"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.289"},` +
		`{"type":"text","text":"You are Claude Code"},` +
		`{"type":"text","text":"static","cache_control":{"type":"ephemeral","scope":"global"}},` +
		`{"type":"text","text":"dynamic","cache_control":{"type":"ephemeral"}}],` +
		`"messages":[{"role":"user","content":[{"type":"text","text":"build the PDF"}]},` +
		`{"role":"assistant","content":[{"type":"text","text":"ok","cache_control":{"type":"ephemeral"}}]},` +
		`{"role":"user","content":[{"type":"text","text":"Your task is to create a detailed summary of the conversation so far","cache_control":{"type":"ephemeral"}}]}]}`)
	out := force1hCacheControl(in)
	requireAcceptableTTLs(t, out)
	if ttls, _ := breakpointTTLs(out); strings.Join(ttls, ",") != "1h,1h,1h,1h" {
		t.Fatalf("want every breakpoint at 1h, got %q: %s", ttls, out)
	}
	// The scoped block reads exactly as Claude Code writes it when it chooses 1h itself.
	if !bytes.Contains(out, []byte(`"cache_control":{"type":"ephemeral","ttl":"1h","scope":"global"}`)) {
		t.Fatalf("the scoped block is not in the client's own shape: %s", out)
	}
	if !bytes.Equal(bytes.ReplaceAll(out, ttl1hMember, nil), in) {
		t.Fatalf("bytes outside the four ttl members moved:\n in: %s\nout: %s", in, out)
	}
}

// The invariant the rewrite lives under: whatever mix of breakpoints a client sends,
// if Anthropic accepts the client's body it accepts the rewritten one. Every shape
// Claude Code writes, at every position the order rule spans.
func TestForce1hCacheControl_NeverMakesAnAcceptableBodyUnacceptable(t *testing.T) {
	shapes := []string{
		``,
		`,"cache_control":{"type":"ephemeral"}`,
		`,"cache_control":{"type":"ephemeral","scope":"global"}`,
		`,"cache_control":{"type":"ephemeral","ttl":"1h"}`,
		`,"cache_control":{"type":"ephemeral","ttl":"1h","scope":"global"}`,
		`,"cache_control":{"type":"ephemeral","ttl":"5m"}`,
	}
	tried := 0
	for _, tool := range shapes {
		for _, static := range shapes {
			for _, dynamic := range shapes {
				for _, last := range shapes {
					in := []byte(`{"model":"m","tools":[{"name":"t","input_schema":{"type":"object"}` + tool + `}],` +
						`"system":[{"type":"text","text":"static"` + static + `},{"type":"text","text":"dynamic"` + dynamic + `}],` +
						`"messages":[{"role":"user","content":[{"type":"text","text":"hi"` + last + `}]}]}`)
					ttls, err := breakpointTTLs(in)
					if err != nil {
						t.Fatalf("bad fixture %s: %v", in, err)
					}
					if !ttlsAcceptable(ttls) {
						continue // a body the client could not send either
					}
					tried++
					requireAcceptableTTLs(t, force1hCacheControl(in))
				}
			}
		}
	}
	if tried < 100 {
		t.Fatalf("only %d acceptable bodies generated", tried)
	}
}

// A ttl the client chose itself means the body goes upstream as the client wrote it:
// upgrading the bare breakpoints around a 5m one is the other road to the 400.
func TestForce1hCacheControl_ClientChosenTTLLeavesBodyAlone(t *testing.T) {
	for _, in := range []string{
		`{"tools":[{"name":"t","cache_control":{"type":"ephemeral","ttl":"5m"}}],"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral"}}]}`,
		`{"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":[{"type":"text","text":"x","cache_control":{"type":"ephemeral","ttl":"5m"}}]}]}`,
		`{"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral","ttl":"1h"}},{"type":"text","text":"d","cache_control":{"type":"ephemeral","ttl":"5m","scope":"global"}}],"messages":[{"role":"user","content":[{"type":"text","text":"x","cache_control":{"type":"ephemeral"}}]}]}`,
		// A ttl that is there but not a string is not one the rewrite understands.
		`{"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral","ttl":null}},{"type":"text","text":"d","cache_control":{"type":"ephemeral"}}]}`,
	} {
		if out := force1hCacheControl([]byte(in)); string(out) != in {
			t.Errorf("a client-chosen ttl was overridden:\n in: %s\nout: %s", in, out)
		}
	}
}

// Whatever else a breakpoint carries, and however its client spaces it, it gets the ttl.
func TestForce1hCacheControl_EveryBreakpointShape(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		// The top-level automatic breakpoint Claude Code sends with prompt-caching-evict.
		{`{"cache_control":{"type":"ephemeral","evict_on_complete":true},"messages":[]}`,
			`{"cache_control":{"type":"ephemeral","ttl":"1h","evict_on_complete":true},"messages":[]}`},
		// Another key order: the ttl goes before the closing brace.
		{`{"system":[{"type":"text","text":"s","cache_control":{"scope":"global","type":"ephemeral"}}]}`,
			`{"system":[{"type":"text","text":"s","cache_control":{"scope":"global","type":"ephemeral","ttl":"1h"}}]}`},
		// A client that spaces its JSON, up to the closing brace.
		{`{"system": [{"type": "text", "text": "s", "cache_control": {"type": "ephemeral"}}]}`,
			`{"system": [{"type": "text", "text": "s", "cache_control": {"type": "ephemeral","ttl":"1h"}}]}`},
		{`{"system": [{"type": "text", "text": "s", "cache_control": { "type": "ephemeral" }}]}`,
			`{"system": [{"type": "text", "text": "s", "cache_control": { "type": "ephemeral" ,"ttl":"1h"}}]}`},
		// A tool result's own content.
		{`{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"t","content":[{"type":"text","text":"r","cache_control":{"type":"ephemeral"}}]}]}]}`,
			`{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"t","content":[{"type":"text","text":"r","cache_control":{"type":"ephemeral","ttl":"1h"}}]}]}]}`},
	} {
		out := force1hCacheControl([]byte(c.in))
		if string(out) != c.want {
			t.Errorf("\n  in: %s\n out: %s\nwant: %s", c.in, out, c.want)
		}
		requireAcceptableTTLs(t, out)
	}
}

// Only a cache_control KEY whose value is an ephemeral object is a breakpoint: not the
// words inside a string (a transcript quoting this file), not a key that ends in them,
// not a string value, not a null, and not a tool schema's property of that name.
func TestForce1hCacheControl_OnlyRealBreakpoints(t *testing.T) {
	notBreakpoints := []string{
		`{"messages":[{"role":"user","content":[{"type":"text","text":"it matched \"cache_control\":{\"type\":\"ephemeral\"}"}]}]}`,
		`{"system":[{"type":"text","text":"s","he wrote \"cache_control":{"type":"ephemeral"}}]}`,
		`{"note":"cache_control","n":1}`,
		`{"system":[{"type":"text","text":"s","cache_control":null}]}`,
		`{"tools":[{"name":"t","input_schema":{"type":"object","properties":{"cache_control":{"type":"object"}}}}]}`,
	}
	for _, in := range notBreakpoints {
		if out := force1hCacheControl([]byte(in)); string(out) != in {
			t.Errorf("rewrote something that is not a breakpoint:\n in: %s\nout: %s", in, out)
		}
	}
	breakpoint := `{"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral"}}]}`
	all := "[" + strings.Join(append(notBreakpoints, breakpoint), ",") + "]"
	out := force1hCacheControl([]byte(all))
	if n := bytes.Count(out, ttl1hMember); n != 1 {
		t.Fatalf("want only the one real breakpoint upgraded, got %d: %s", n, out)
	}
	if bytes.Count(out, []byte(`"cache_control":{"type":"ephemeral","ttl":"1h"}`)) != 1 {
		t.Fatalf("the real breakpoint was not the one upgraded: %s", out)
	}
}

// A breakpoint the scan cannot read leaves the whole body alone, upgradable
// breakpoints ahead of it included: a partial rewrite is the failure being fixed.
func TestForce1hCacheControl_UnreadableBreakpointLeavesBodyAlone(t *testing.T) {
	in := `{"system":[{"type":"text","text":"s","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":[{"type":"text","text":"x","cache_control":{"type":"ephemeral"`
	if out := force1hCacheControl([]byte(in)); string(out) != in {
		t.Fatalf("a truncated body was rewritten:\n in: %s\nout: %s", in, out)
	}
}

func TestEnsureExtendedCacheBeta(t *testing.T) {
	// empty header -> sets the flag
	h := http.Header{}
	ensureExtendedCacheBeta(h)
	if got := h.Get("anthropic-beta"); got != extendedCacheBetaFlag {
		t.Fatalf("empty header: got %q", got)
	}
	// idempotent on repeat
	ensureExtendedCacheBeta(h)
	if got := h.Get("anthropic-beta"); got != extendedCacheBetaFlag {
		t.Fatalf("idempotency: got %q", got)
	}
	// preserves an existing flag (the oauth beta) and appends
	h2 := http.Header{}
	h2.Set("anthropic-beta", oauthBetaFlag)
	ensureExtendedCacheBeta(h2)
	got := h2.Get("anthropic-beta")
	if !strings.Contains(got, oauthBetaFlag) || !strings.Contains(got, extendedCacheBetaFlag) {
		t.Fatalf("expected both flags, got %q", got)
	}
	// already present among others -> no duplicate
	ensureExtendedCacheBeta(h2)
	if n := strings.Count(h2.Get("anthropic-beta"), extendedCacheBetaFlag); n != 1 {
		t.Fatalf("flag duplicated: %q", h2.Get("anthropic-beta"))
	}
}
