package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
)

// forceCache1h (SWITCH_FORCE_1H_CACHE) makes the proxy upgrade every Anthropic-served
// request to the 1-hour prompt-cache TTL, replicating what the client env var
// ENABLE_PROMPT_CACHING_1H does on the wire — but at the proxy, so EVERY client
// through it (Claude Code, the agent gateway) gets it without any local config.
//
// Why it's needed: a client in ANTHROPIC_AUTH_TOKEN (Bearer) mode believes it is on
// API-key auth and silently drops to the 5-minute TTL. A think-gap over 5 min then
// expires the whole cached prefix and the next turn re-prefills 150-200k tokens at
// the 1.25x cache-CREATION rate. Forcing 1h writes once at 2x and then reads at 0.1x
// for up to an hour — a large net win for interactive, gap-heavy sessions.
//
// Two wire changes, mirroring the client env var exactly:
//   - add the extended-cache-ttl beta capability to anthropic-beta, and
//   - give every ephemeral cache_control breakpoint that names no ttl ttl:"1h" — all of
//     them or none of them (force1hCacheControl says why).
//
// Anthropic-served lanes ONLY. kimi/codex/local ride their own upstreams and either
// ignore the field or would be corrupted by it, so the rewrite is invoked from inside
// the two Anthropic branches (the account-lane fallthrough in requestHandler and the
// anthropic class-member case), never the bypass/provider path; the beta append sits
// in the Rewrite else-branch, which only runs for Anthropic hops. Verify it took
// effect by watching the response tap's ephemeral_1h_input_tokens go non-zero.
//
// The binary defaults this ON (resolved in main via SWITCH_FORCE_1H_CACHE); the
// package zero-value is off so the test suite stays hermetic and opts in explicitly.
// Kill switch: SWITCH_FORCE_1H_CACHE=off.
var forceCache1h bool

// extendedCacheBetaFlag is the anthropic-beta capability that unlocks the 1-hour
// prompt-cache TTL. It is what the client sends when ENABLE_PROMPT_CACHING_1H is set.
const extendedCacheBetaFlag = "extended-cache-ttl-2025-04-11"

// ensureExtendedCacheBeta makes h's anthropic-beta header include the extended-cache
// flag, preserving the client's other flags (including the oauth flag). Idempotent.
// Mirrors ensureOAuthBeta.
func ensureExtendedCacheBeta(h http.Header) {
	cur := h.Get("anthropic-beta")
	if cur == "" {
		h.Set("anthropic-beta", extendedCacheBetaFlag)
		return
	}
	for _, f := range strings.Split(cur, ",") {
		if strings.TrimSpace(f) == extendedCacheBetaFlag {
			return
		}
	}
	h.Set("anthropic-beta", cur+","+extendedCacheBetaFlag)
}

// A raw "cache_control", quotes unescaped on both sides, is an object key unless an odd
// run of backslashes precedes it, because a quote inside a JSON string is always
// escaped. That is what lets bytes.Index find every breakpoint at any depth (tools[],
// system[], messages[].content[], a tool result's content, the top-level automatic one)
// without a token walk of a multi-MB body.
var (
	cacheControlKey = []byte(`"cache_control"`)
	// Claude Code writes its own ttl straight after the type ({type, ttl, scope}), and
	// the rewrite puts it in the same place, so an upgraded block reads byte for byte
	// like the one the client sends when it chooses 1h itself.
	ephemeralHead = []byte(`{"type":"ephemeral"`)
	ttl1hMember   = []byte(`,"ttl":"1h"`)
)

// force1hCacheControl gives every ephemeral cache_control breakpoint in an Anthropic
// request body that names no ttl the 1-hour TTL: every one of them, or none.
//
// All or none, because Anthropic refuses a body whose TTLs rise after they fall — "a
// ttl='1h' cache_control block must not come after a ttl='5m' cache_control block",
// counting tools, then system, then messages — and a breakpoint that names no ttl is a
// 5-minute one. Upgrading only some is how this broke. The rewrite used to replace one
// literal, {"type":"ephemeral"}, and Claude Code 2.1.289 also writes
// {"type":"ephemeral","scope":"global"} on the static system block. On a request the
// client leaves at 5m, compaction among them (its own 1h list is the main thread, sdk,
// auto_mode and memdir_relevance), the scoped block stayed 5m ahead of upgraded 1h ones
// and Anthropic refused the request. A client running without
// ENABLE_PROMPT_CACHING_1H could not compact: a session grew from
// the 267k auto-compact threshold to the 1M ceiling and stopped.
//
// The body comes back unchanged, as the same slice, when no breakpoint needs a ttl,
// when one already names a ttl other than 1h (the client chose it, and its own body is
// the valid one), or when a breakpoint cannot be read. Otherwise ttl:"1h" goes into
// every breakpoint without one, whatever else it carries, and no other byte moves:
// re-marshalling would reorder keys, the tool schemas' among them, and lose the cache.
func force1hCacheControl(body []byte) []byte {
	var at []int // where ttl1hMember goes, ascending
	for off := 0; ; {
		k := bytes.Index(body[off:], cacheControlKey)
		if k < 0 {
			break
		}
		key := off + k
		off = key + len(cacheControlKey)
		if escapedQuote(body, key) {
			continue // the words, inside a string
		}
		v := skipPastColon(body, int64(off))
		if v < 0 || body[v] != '{' {
			continue // a string value that says cache_control, or a null
		}
		dec := json.NewDecoder(bytes.NewReader(body[v:]))
		var bp map[string]json.RawMessage
		if dec.Decode(&bp) != nil {
			return body
		}
		start, end := int(v), int(v)+int(dec.InputOffset())
		off = end
		var typ string
		if json.Unmarshal(bp["type"], &typ) != nil || typ != "ephemeral" {
			continue // not a breakpoint: a tool's schema may have a property of this name
		}
		if raw, ok := bp["ttl"]; ok {
			var ttl string
			if json.Unmarshal(raw, &ttl) != nil || ttl != "1h" {
				return body
			}
			continue
		}
		if bytes.HasPrefix(body[start:end], ephemeralHead) {
			at = append(at, start+len(ephemeralHead))
		} else {
			at = append(at, end-1) // before the closing brace
		}
	}
	if len(at) == 0 {
		return body
	}
	out := make([]byte, 0, len(body)+len(at)*len(ttl1hMember))
	prev := 0
	for _, p := range at {
		out = append(out, body[prev:p]...)
		out = append(out, ttl1hMember...)
		prev = p
	}
	return append(out, body[prev:]...)
}

// escapedQuote reports whether the quote at body[i] is escaped, preceded by an odd run
// of backslashes, and so sits inside a string.
func escapedQuote(body []byte, i int) bool {
	n := 0
	for j := i - 1; j >= 0 && body[j] == '\\'; j-- {
		n++
	}
	return n%2 == 1
}
