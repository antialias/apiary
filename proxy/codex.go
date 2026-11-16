package main

// ---- codex provider (#33) ----
//
// The gpt lane talks to the ChatGPT-subscription codex backend DIRECTLY: the
// proxy translates Anthropic /v1/messages <-> codex Responses
// (POST /backend-api/codex/responses, stream-only upstream), replacing the
// LiteLLM container + dev API key. Auth is a rotating ~10-day bearer dispensed
// by the brain (GET /openai-token, X-Switch-Key guarded); the proxy NEVER
// refreshes (STRICT rotation — the brain owns the chain) and caches the bearer
// with a STALE fallback. Caching is safe: rotation does not invalidate
// outstanding access tokens (verified live — an old AT survived chain death).
//
// The request body sent upstream carries ONLY the probe-validated fields
// (Stage A probe: model, instructions, input, tools, tool_choice,
// stream, store) — max_tokens/temperature/top_p/stop_sequences are dropped
// rather than risking an unvalidated-field 400 against an undocumented
// backend. The subscription serves the suffixed ids only (gpt-6.1-sol,
// gpt-6-astra, gpt-6-luna); a bare family id 400s, so a class member's model
// must name the suffixed id.
//
// Wire shape (translated request):
//
//	system            -> instructions (text concatenated)
//	messages[].text   -> input[] message items (input_text / output_text)
//	tool_use          -> {"type":"function_call", name, arguments:<json str>, call_id}
//	tool_result       -> {"type":"function_call_output", call_id, output:<text>}
//	image (base64)    -> {"type":"input_image","image_url":"data:<media>;base64,…"}
//	tools[]           -> {"type":"function", name, description, parameters} (custom tools only)
//	tool_choice       -> auto | none | required | {"type":"function","name":…}
//
// Response: codex SSE events are translated back to the Anthropic SSE contract
// (message_start / content_block_* / message_delta / message_stop) when the
// client asked to stream, or aggregated into one Anthropic message JSON when it
// didn't (upstream is ALWAYS streamed — codex 400s otherwise).

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	codexResponsesPath = "/backend-api/codex/responses"
	codexUA            = "codex-cli/0.137.0"
)

// codexUpstream is the codex backend base URL, resolved in main() ONLY
// (default https://chatgpt.com; SWITCH_CODEX_UPSTREAM overrides; "off"
// disables the gpt provider). The package default "" keeps tests — which never
// run main — from ever building a provider aimed at the real backend.
var codexUpstream = ""

// ---- dispensed-bearer cache ----
//
// Keyed by the POOL ACCOUNT NAME requested from the brain (#71) — "" is the
// incumbent-follow slot (no ?account= param; the brain resolves its pool
// incumbent). Each key caches independently so a class detour onto a pool
// sibling never evicts the incumbent's bearer.
type codexTokEntry struct {
	val     string // the bearer
	acct    string // ChatGPT-Account-Id (workspace UUID — NOT the pool name)
	name    string // pool account name the brain says served this ("" = old brain)
	exp     time.Time
	fetched time.Time
}

var (
	codexTokMu sync.Mutex
	codexToks  = map[string]codexTokEntry{}
)

// codexTokenFresh is how long a dispensed bearer is used without re-confirming
// with the brain. The bearer's own TTL is ~10 days; re-fetching every few
// minutes is a localhost hop that adopts a brain-side rotation promptly.
var codexTokenFresh = 5 * time.Minute

// getCodexToken returns (bearer, account_id, pool name, ok) from the brain's
// /openai-token dispenser for one pool account (""=incumbent), mirroring
// getToken's contract: a fetch failure rides the cached bearer while it is
// still unexpired (STALE), and ok=false only when there is nothing usable at
// all. The returned pool name is the brain's own (response `name` key); an old
// brain sends none and the requested name is echoed back so labels stay honest.
func getCodexToken(account string) (string, string, string, bool) {
	codexTokMu.Lock()
	e, has := codexToks[account]
	codexTokMu.Unlock()
	now := time.Now()
	if has && e.val != "" && now.Before(e.exp) && now.Sub(e.fetched) < codexTokenFresh {
		return e.val, e.acct, e.name, true
	}
	stale := func() (string, string, string, bool) {
		if has && e.val != "" && now.Before(e.exp) {
			return e.val, e.acct, e.name, true // STALE — dispenser down, bearer still valid
		}
		return "", "", "", false
	}
	if brainURL == "" {
		return stale()
	}
	u := brainURL + "/openai-token"
	if account != "" {
		u += "?account=" + url.QueryEscape(account)
	}
	req, _ := http.NewRequest("GET", u, nil)
	req.Header.Set("X-Switch-Key", switchKey)
	resp, err := brainClient.Do(req)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == 200 {
			var d struct {
				AccountID   string  `json:"account_id"`
				AccessToken string  `json:"access_token"`
				ExpiresAt   float64 `json:"expires_at"`
				Name        string  `json:"name"`
			}
			if json.NewDecoder(resp.Body).Decode(&d) == nil && d.AccessToken != "" {
				name := d.Name
				if name == "" {
					name = account
				}
				codexTokMu.Lock()
				codexToks[account] = codexTokEntry{val: d.AccessToken, acct: d.AccountID,
					name: name, exp: time.Unix(int64(d.ExpiresAt), 0), fetched: now}
				codexTokMu.Unlock()
				return d.AccessToken, d.AccountID, name, true
			}
		}
		err = fmt.Errorf("brain /openai-token %d", resp.StatusCode)
	}
	if tok, acct, name, ok := stale(); ok {
		return tok, acct, name, ok
	}
	log.Printf("codex bearer fetch failed (account=%q): %v (no cached bearer — gpt lane will refuse)", account, err)
	return "", "", "", false
}

// dropCodexToken forgets one pool account's cached bearer (called on an
// upstream 401 — the chain may have been revoked; the next request re-fetches
// from the brain).
func dropCodexToken(account string) {
	codexTokMu.Lock()
	delete(codexToks, account)
	codexTokMu.Unlock()
}

// ---- Anthropic -> codex request translation ----

type anthContentBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`          // tool_use
	Name      string          `json:"name,omitempty"`        // tool_use
	Input     json.RawMessage `json:"input,omitempty"`       // tool_use
	ToolUseID string          `json:"tool_use_id,omitempty"` // tool_result
	Content   json.RawMessage `json:"content,omitempty"`     // tool_result: string | []block
	IsError   bool            `json:"is_error,omitempty"`    // tool_result
	Source    *anthImage      `json:"source,omitempty"`      // image
}

type anthImage struct {
	Type      string `json:"type"` // "base64" | "url"
	MediaType string `json:"media_type"`
	Data      string `json:"data"`
	URL       string `json:"url"`
}

type anthMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"` // string | []anthContentBlock
}

type anthTool struct {
	Type        string          `json:"type,omitempty"` // "" | "custom" = a schema tool; anything else is an Anthropic builtin
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
}

type anthRequest struct {
	Model      string          `json:"model"`
	System     json.RawMessage `json:"system"` // string | []block
	Messages   []anthMessage   `json:"messages"`
	Tools      []anthTool      `json:"tools"`
	ToolChoice json.RawMessage `json:"tool_choice"`
	Stream     bool            `json:"stream"`
	// Effort is read from the same two places parseReqFacts reads it, with the
	// same precedence: output_config.effort first, then top-level effort.
	Effort       json.RawMessage `json:"effort"`
	OutputConfig struct {
		Effort json.RawMessage `json:"effort"`
	} `json:"output_config"`
}

// codexEfforts is the set of Claude Code effort levels codex accepts verbatim
// as reasoning.effort (verified against the subscription endpoint
// for gpt-6.1-sol, gpt-6-astra and gpt-6-luna). The map is identity; anything
// else ("minimal" 400s upstream) is never passed through.
var codexEfforts = map[string]bool{"low": true, "medium": true, "high": true, "xhigh": true, "max": true}

// codexReasoning maps the client's effort onto the codex reasoning field. No
// effort means no field (the upstream default applies); an effort codex does
// not accept is logged by value and dropped rather than sent upstream to 400.
func codexReasoning(ar *anthRequest) map[string]any {
	effort := rawScalar(ar.OutputConfig.Effort)
	if effort == "" {
		effort = rawScalar(ar.Effort)
	}
	if effort == "" {
		return nil
	}
	if !codexEfforts[effort] {
		codexWarnOnce(fmt.Sprintf("codex: effort %q has no codex reasoning.effort equivalent; sent without reasoning (upstream default)", effort))
		return nil
	}
	return map[string]any{"effort": effort}
}

// anthBlocks decodes a content value that may be a bare string or a block list.
func anthBlocks(raw json.RawMessage) ([]anthContentBlock, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return []anthContentBlock{{Type: "text", Text: s}}, nil
	}
	var blocks []anthContentBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil, err
	}
	return blocks, nil
}

// flattenText concatenates the text blocks of a string-or-blocks value (system
// prompts, tool_result content).
func flattenText(raw json.RawMessage) string {
	blocks, err := anthBlocks(raw)
	if err != nil {
		return ""
	}
	parts := []string{}
	for _, b := range blocks {
		if b.Type == "text" && b.Text != "" {
			parts = append(parts, b.Text)
		}
	}
	return strings.Join(parts, "\n")
}

// codexToolOutput preserves multimodal tool results. Claude Code's Read puts
// pixels INSIDE tool_result.content; flattening that content silently blinded GPT.
// Keep text-only outputs byte-compatible and retain the call_id on image outputs.
func codexToolOutput(b anthContentBlock) (any, error) {
	blocks, err := anthBlocks(b.Content)
	if err != nil {
		return nil, fmt.Errorf("bad tool result %s: %w", b.ToolUseID, err)
	}
	hasImage := false
	for _, part := range blocks {
		hasImage = hasImage || part.Type == "image"
	}
	if !hasImage {
		out := flattenText(b.Content)
		if b.IsError && !strings.HasPrefix(out, "Error") {
			out = "Error: " + out
		}
		return out, nil
	}
	parts := []map[string]any{}
	if b.IsError {
		parts = append(parts, map[string]any{"type": "input_text", "text": "Error: tool reported failure"})
	}
	for _, part := range blocks {
		switch part.Type {
		case "text":
			parts = append(parts, map[string]any{"type": "input_text", "text": part.Text})
		case "image":
			if part.Source == nil {
				return nil, fmt.Errorf("tool result %s: image has no source", b.ToolUseID)
			}
			src := part.Source
			url := src.URL
			if src.Type == "base64" && src.Data != "" && src.MediaType != "" {
				url = "data:" + src.MediaType + ";base64," + src.Data
			}
			if url == "" {
				return nil, fmt.Errorf("tool result %s: image source is empty", b.ToolUseID)
			}
			parts = append(parts, map[string]any{"type": "input_image", "image_url": url, "detail": "auto"})
		}
	}
	return parts, nil
}

// translateAnthropicToCodex turns an Anthropic Messages body into the codex
// Responses body. Returns (encoded body, clientWantsStream, model).
func translateAnthropicToCodex(body []byte) ([]byte, bool, string, error) {
	var ar anthRequest
	if err := json.Unmarshal(body, &ar); err != nil {
		return nil, false, "", fmt.Errorf("bad Anthropic body: %w", err)
	}

	items := []map[string]any{}
	// pending user/assistant content parts, flushed as one message item whenever
	// a non-message item (function_call / function_call_output) interleaves.
	var parts []map[string]any
	var partsRole string
	flush := func() {
		if len(parts) > 0 {
			items = append(items, map[string]any{"role": partsRole, "content": parts})
			parts = nil
		}
	}
	addPart := func(role string, p map[string]any) {
		if partsRole != role {
			flush()
			partsRole = role
		}
		parts = append(parts, p)
	}

	for _, m := range ar.Messages {
		blocks, err := anthBlocks(m.Content)
		if err != nil {
			return nil, false, "", fmt.Errorf("bad message content: %w", err)
		}
		for _, b := range blocks {
			switch b.Type {
			case "text":
				kind := "input_text"
				if m.Role == "assistant" {
					kind = "output_text"
				}
				addPart(m.Role, map[string]any{"type": kind, "text": b.Text})
			case "image":
				if m.Role == "user" && b.Source != nil {
					urlStr := b.Source.URL
					if b.Source.Type == "base64" {
						urlStr = "data:" + b.Source.MediaType + ";base64," + b.Source.Data
					}
					if urlStr != "" {
						addPart(m.Role, map[string]any{"type": "input_image", "image_url": urlStr})
					}
				}
			case "tool_use":
				flush()
				args := "{}"
				if len(b.Input) > 0 {
					args = string(b.Input)
				}
				items = append(items, map[string]any{
					"type": "function_call", "name": b.Name,
					"arguments": args, "call_id": b.ID,
				})
			case "tool_result":
				flush()
				out, err := codexToolOutput(b)
				if err != nil {
					return nil, false, "", err
				}
				items = append(items, map[string]any{
					"type": "function_call_output", "call_id": b.ToolUseID, "output": out,
				})
				// thinking / redacted_thinking / server-side blocks: nothing codex
				// can consume — dropped.
			}
		}
		flush()
	}
	flush()

	out := map[string]any{
		"model":  ar.Model,
		"input":  items,
		"stream": true, // codex 400s non-stream; the client's own stream flag only shapes OUR response
		"store":  false,
	}
	if sys := flattenText(ar.System); sys != "" {
		out["instructions"] = sys
	}
	if r := codexReasoning(&ar); r != nil {
		out["reasoning"] = r
	}
	ftools := []map[string]any{}
	skipped := 0
	for _, tl := range ar.Tools {
		if (tl.Type == "" || tl.Type == "custom") && len(tl.InputSchema) > 0 && tl.Name != "" {
			ftools = append(ftools, map[string]any{
				"type": "function", "name": tl.Name,
				"description": tl.Description, "parameters": json.RawMessage(tl.InputSchema),
				// Anthropic schemas allow omitted optional arguments. Responses may
				// normalize an unspecified strict setting, forcing invalid empty
				// Read.pages / WebSearch domain filters into otherwise valid calls.
				"strict": false,
			})
		} else {
			skipped++
		}
	}
	if skipped > 0 {
		codexWarnOnce(fmt.Sprintf("codex: skipped %d non-custom tool(s) — Anthropic builtins have no codex equivalent", skipped))
	}
	if len(ftools) > 0 {
		out["tools"] = ftools
		if tc := translateToolChoice(ar.ToolChoice); tc != nil {
			out["tool_choice"] = tc
		}
	}
	enc, err := json.Marshal(out)
	if err != nil {
		return nil, false, "", err
	}
	return enc, ar.Stream, ar.Model, nil
}

func translateToolChoice(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	var tc struct {
		Type string `json:"type"`
		Name string `json:"name"`
	}
	if json.Unmarshal(raw, &tc) != nil {
		return nil
	}
	switch tc.Type {
	case "auto":
		return "auto"
	case "none":
		return "none"
	case "any":
		return "required"
	case "tool":
		return map[string]any{"type": "function", "name": tc.Name}
	}
	return nil
}

// codexWarnOnce logs each distinct message once (translation warnings repeat on
// every request of a session otherwise).
var (
	codexWarnMu   sync.Mutex
	codexWarnSeen = map[string]bool{}
)

func codexWarnOnce(msg string) {
	codexWarnMu.Lock()
	seen := codexWarnSeen[msg]
	codexWarnSeen[msg] = true
	codexWarnMu.Unlock()
	if !seen {
		log.Print(msg)
	}
}

// ---- outbound request plumbing ----

// codexHeaders is the COMPLETE outbound header set for a codex hop — built
// fresh, never inherited from the inbound request: the client's Anthropic
// headers (anthropic-version/-beta, x-app, x-stainless-*) have no business at
// chatgpt.com, and the account-id/originator/UA trio is mandatory there.
//
// session_id is what makes the codex backend's prompt cache work, and nothing
// else here reads it, so it looks removable. It is not. Measured
// straight at the backend, a 6-turn append-only conversation behind a ~28k-token
// prefix cached on turns 2–6: no extra field 0%, prompt_cache_key in the body
// 0%, a conversation_id header 0%, a session_id header 99.4%. It is a routing-
// affinity hint, not a conversation identity: two conversations interleaved
// under one shared value both cached 99%+, so a coarse value (the agent gateway's
// constant "agent-gw") is fine. The real Codex CLI sends it too. The key is
// stored lowercase, as the CLI sends it, rather than canonicalized.
func codexHeaders(tok, acct, session string) http.Header {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+tok)
	h.Set("ChatGPT-Account-Id", acct)
	h.Set("Content-Type", "application/json")
	h.Set("Accept", "text/event-stream")
	h.Set("originator", "codex-cli")
	h.Set("User-Agent", codexUA)
	h["session_id"] = []string{session}
	return h
}

// codexSession picks the session_id for a codex hop: the client's own
// x-claude-code-session-id when it sent one (Claude Code: a per-session UUID;
// The agent gateway: the constant "agent-gw"). Without one, the value is derived from
// the translated body's instructions, so requests that share a system prompt,
// and therefore a cacheable prefix, still share affinity. A body with no
// instructions hashes the empty string: one shared value, which is still better
// than none. An inbound value that is long or not printable ASCII goes out as
// its hash instead: the proxy only vouches for header values it can read, and
// the hash keeps the affinity.
func codexSession(inbound string, codexBody []byte) string {
	if s := strings.TrimSpace(inbound); s != "" {
		if len(s) <= 128 && printableASCII(s) {
			return s
		}
		return hashSession(s)
	}
	var b struct {
		Instructions string `json:"instructions"`
	}
	_ = json.Unmarshal(codexBody, &b)
	return hashSession(b.Instructions)
}

func hashSession(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:32]
}

func printableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// wantsStream reads the top-level "stream" flag of an ANTHROPIC request body —
// the client's wish, which shapes OUR response even though the codex upstream
// is always streamed.
func wantsStream(body []byte) bool {
	var s struct {
		Stream bool `json:"stream"`
	}
	_ = json.Unmarshal(body, &s)
	return s.Stream
}

// ---- codex SSE -> Anthropic response translation ----

// codexSSEEvent is one decoded codex/Responses SSE data payload. Delta stays
// raw: it is a JSON string for text/argument deltas but other event families
// shape it differently.
type codexSSEEvent struct {
	Type     string          `json:"type"`
	Delta    json.RawMessage `json:"delta"`
	Item     *codexItem      `json:"item"`
	Response *codexRespObj   `json:"response"`
	// Codex backends use both top-level and nested error envelopes.
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Error   *codexAPIError `json:"error"`
}

type codexItem struct {
	Type      string `json:"type"` // "message" | "function_call" | "reasoning" | …
	Name      string `json:"name"`
	CallID    string `json:"call_id"`
	Arguments string `json:"arguments"`
	Content   []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
}

type codexRespObj struct {
	ID                string         `json:"id"`
	Model             string         `json:"model"`
	Status            string         `json:"status"` // "completed" | "incomplete" | "failed"
	Usage             *codexUsageObj `json:"usage"`
	Error             *codexAPIError `json:"error"`
	IncompleteDetails *struct {
		Reason string `json:"reason"` // "max_output_tokens" | "content_filter"
	} `json:"incomplete_details"`
}

// incompleteRefusal says why an incomplete codex response cannot reach the
// client as a reply, or "" when it can (as stop_reason max_tokens). produced
// is whether any content block came out of it.
//
// #132: the backend answers some requests with response.incomplete, reason
// max_output_tokens, before generating anything: zero usage, no output, and
// the request set no limit. As max_tokens that is an empty reply the client
// cannot tell from a real one, so a response that generated nothing is an
// error. Reasoning-only output under a tiny max_tokens is not this: the model
// worked and hit the limit, so it stays max_tokens. A reason other than
// max_output_tokens has no stop_reason to map to and is an error whatever
// came before it.
func incompleteRefusal(r *codexRespObj, produced bool) string {
	reason := ""
	if r != nil && r.IncompleteDetails != nil {
		reason = r.IncompleteDetails.Reason
	}
	if reason != "" && reason != "max_output_tokens" {
		return "response incomplete (" + reason + ")"
	}
	if produced || (r != nil && r.Usage != nil && r.Usage.OutputTokens > 0) {
		return ""
	}
	if reason == "" {
		reason = "no reason given"
	}
	return "response incomplete before any output (" + reason + ")"
}

type codexAPIError struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Keep actionable upstream refusals instead of turning every SSE error into a
// retriable 502. Unknown codes remain server errors; never classify by prose.
func codexFailure(ev codexSSEEvent) (int, string) {
	detail := codexFailureDetail(ev)
	status := http.StatusBadGateway
	for _, code := range []string{detail.Code, detail.Type} {
		switch code {
		case "invalid_request_error", "invalid_request", "invalid_value", "invalid_image", "invalid_image_format", "invalid_base64_image", "image_too_large", "context_length_exceeded", "unsupported_parameter", "unknown_parameter", "missing_required_parameter":
			status = http.StatusBadRequest
		case "rate_limit_exceeded", "rate_limit_error", "usage_limit_reached", "insufficient_quota":
			status = http.StatusTooManyRequests
		case "authentication_error", "invalid_api_key":
			status = http.StatusUnauthorized
		}
	}
	msg := detail.Message
	if msg == "" {
		msg = "codex stream error"
	}
	if detail.Code != "" {
		msg = detail.Code + ": " + msg
	}
	return status, msg
}

// codexFailureDetail is a failed response's error, wherever the event put it.
func codexFailureDetail(ev codexSSEEvent) codexAPIError {
	detail := codexAPIError{Code: ev.Code, Message: ev.Message}
	if ev.Error != nil {
		detail = *ev.Error
	}
	if ev.Response != nil && ev.Response.Error != nil {
		detail = *ev.Response.Error
	}
	return detail
}

// codexFailureKind names a failed response's error for the served event: its
// code, else its type.
func codexFailureKind(ev codexSSEEvent) string {
	d := codexFailureDetail(ev)
	if d.Code != "" {
		return d.Code
	}
	return d.Type
}

// codexUsageObj is the Responses API usage block. Named rather than inline
// because both details sub-blocks are optional, and a caller constructing one
// should not have to restate the whole anonymous shape to set a single field.
type codexUsageObj struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	// Cache accounting. Both fields are SUBSETS of InputTokens — see codexUsage,
	// where that distinction is the entire substance of the conversion.
	// cache_write_tokens is gpt-5.6-and-later only; earlier models bill no cache
	// write and report cached_tokens alone.
	//
	// CacheWriteTokens is nil when the response does not report it, which the
	// served event tells apart from a reported 0. Its lifetimes, when given,
	// are cache_write_tokens_details: 30-minute and 12-hour writes, billed
	// apart, summing to cache_write_tokens.
	InputTokensDetails *codexInputDetails `json:"input_tokens_details"`
	// Reasoning tokens are billed under output_tokens but are NOT part of
	// the transcript — codex discards them and they are never re-sent.
	OutputTokensDetails *struct {
		ReasoningTokens int `json:"reasoning_tokens"`
	} `json:"output_tokens_details"`
}

// codexInputDetails is the usage's input_tokens_details.
type codexInputDetails struct {
	CachedTokens            int                   `json:"cached_tokens"`
	CacheWriteTokens        *int                  `json:"cache_write_tokens"`
	CacheWriteTokensDetails *codexCacheWriteSplit `json:"cache_write_tokens_details"`
}

// codexCacheWriteSplit is a codex cache write by lifetime, each nil when the
// response does not give it.
type codexCacheWriteSplit struct {
	TTL30m *int `json:"ttl_30m_tokens"`
	TTL12h *int `json:"ttl_12h_tokens"`
}

// codexTokens is one codex response's usage, already converted into Anthropic
// accounting: the three input-side counts are DISJOINT and sum to the prompt.
type codexTokens struct {
	in         int // uncached remainder of the prompt
	cacheRead  int // prompt tokens served from the cache
	cacheWrite int // prompt tokens newly written to the cache
	out        int // transcript-visible output (reasoning already subtracted)

	// cacheReported separates a reported zero from a synthesized one. The CLIENT
	// wire asserts zeros either way (see anthUsage) because to the agent gateway an absent
	// cache field makes the whole turn uncountable — this flag exists so the
	// scaleMark's provenance record does not claim the upstream said something it
	// never said.
	cacheReported bool
}

// codexUsage reads the TRANSCRIPT-VISIBLE token counts off a codex response and
// converts OpenAI's cache accounting into Anthropic's.
//
// The two schemas partition the prompt differently, and reading one as the other
// double-counts every cached token:
//
//	OpenAI     input_tokens is the TOTAL prompt. input_tokens_details.cached_tokens
//	           and .cache_write_tokens are SUBSETS of it — read, written, and
//	           neither, summing back to input_tokens.
//	Anthropic  input_tokens is the UNCACHED REMAINDER. cache_read_input_tokens and
//	           cache_creation_input_tokens are DISJOINT from it, which is precisely
//	           why a harness is allowed to add all three.
//
// So the remainder is input_tokens - cached - written. Passing the upstream
// input_tokens through ALONGSIDE the cache fields would count the cached prefix
// twice, and on this lane the prefix measured ~93% of the prompt: the numerator
// would run near double and compaction would fire that much early.
//
// Formerly no details block was decoded at all and the wire asserted a
// hard zero for both cache fields. That zero was assumed, never measured, and it
// made the gpt lane's cache behaviour unobservable in the one place the numbers
// were already sitting.
//
// Reasoning tokens are subtracted from output_tokens. Both harnesses compute
// their compaction numerator by summing the usage block — Claude Code's is
// literally `input + (cache_creation??0) + (cache_read??0) + output` — and
// reasoning tokens never enter the transcript that the next request replays.
// Counting them would inflate the fill estimate on every gpt-served turn, which
// is the same class of error as the wrong denominator: it compacts too early.
// The vocabulary contract states that output_tokens on this proxy means
// transcript-visible output.
//
// Two clamps, both of which preserve a sum:
//   - reasoning is floored at zero. A details block larger than its total is
//     nonsense, and a negative count would corrupt a harness numerator far worse
//     than an unsubtracted one.
//   - each cache subset is capped at what remains of the total, so the three
//     input counts ALWAYS sum to upstream input_tokens. A nonsense details block
//     can misattribute the prompt between buckets; it cannot change how big the
//     prompt is, and the size is what compaction actually turns on.
func codexUsage(r *codexRespObj) codexTokens {
	if r == nil || r.Usage == nil {
		return codexTokens{}
	}
	t := codexTokens{in: r.Usage.InputTokens, out: r.Usage.OutputTokens}
	if t.in < 0 {
		t.in = 0
	}
	if d := r.Usage.OutputTokensDetails; d != nil && d.ReasoningTokens > 0 {
		if t.out -= d.ReasoningTokens; t.out < 0 {
			t.out = 0
		}
	}
	if d := r.Usage.InputTokensDetails; d != nil {
		t.cacheReported = true
		t.cacheRead = clampToBudget(d.CachedTokens, t.in)
		t.in -= t.cacheRead
		if d.CacheWriteTokens != nil {
			t.cacheWrite = clampToBudget(*d.CacheWriteTokens, t.in)
		}
		t.in -= t.cacheWrite
	}
	return t
}

// clampToBudget bounds n to [0, budget] — the subset can be neither negative nor
// larger than the total it is claimed to be part of.
func clampToBudget(n, budget int) int {
	if n > budget {
		n = budget
	}
	if n < 0 {
		n = 0
	}
	return n
}

// scaledInput divides the three input-side counts into Anthropic units (PLAN §1,
// #44). output_tokens stays in upstream units — it is transcript-visible output,
// and the transcript re-enters the next turn's input where the divided counts
// cover it.
//
// Each component rounds independently, exactly as the kimi rewriter scales each
// of usageInputKeys, so the scaled triple can differ from round(total/r) by a
// token or two. That is noise against a 200k window, and the alternative — scale
// the total, then re-split it — would invent precision the ratio does not have.
func (t codexTokens) scaledInput(r float64) codexTokens {
	t.in = scaleTokensByRatio(t.in, r)
	t.cacheRead = scaleTokensByRatio(t.cacheRead, r)
	t.cacheWrite = scaleTokensByRatio(t.cacheWrite, r)
	return t
}

// anthUsage renders codex counts as an Anthropic usage block.
//
// The cache fields are always emitted EXPLICITLY, zero included, and that is
// half the point of this function. To the agent gateway an ABSENT cache field means
// "unknown", not "none": its message_delta guard is
//
//	inputTokens !== undefined && cacheReadTokens !== undefined && cacheWriteTokens !== undefined
//
// so omitting them yields contextUsage.state === 'unavailable' and the
// compaction gate is fed "I don't know" on every gpt-served turn. A response
// carrying no details block has nothing cached to declare, and zero is then both
// true and countable.
//
// The other half is codexUsage's subset→disjoint conversion. A harness SUMS
// these three fields, so whatever they hold must not overlap.
//
// Use this ONLY where the numbers are real. Where they are not, omit the cache
// fields deliberately — see translateCodexSSE's truncated-stream close.
func anthUsage(t codexTokens) map[string]int {
	return map[string]int{
		"input_tokens":                t.in,
		"output_tokens":               t.out,
		"cache_read_input_tokens":     t.cacheRead,
		"cache_creation_input_tokens": t.cacheWrite,
	}
}

func (e *codexSSEEvent) deltaString() string {
	var s string
	_ = json.Unmarshal(e.Delta, &s)
	return s
}

// parseSSE walks an SSE byte stream and calls fn once per event with the
// assembled data payload (multiple data: lines join with \n per the SSE spec).
// fn returning false stops the walk (client went away).
func parseSSE(r io.Reader, fn func(data []byte) bool) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	var data []byte
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			if len(data) > 0 && string(data) != "[DONE]" {
				if !fn(data) {
					return nil
				}
			}
			data = nil
			continue
		}
		if s, ok := strings.CutPrefix(line, "data:"); ok {
			if len(data) > 0 {
				data = append(data, '\n')
			}
			data = append(data, strings.TrimPrefix(s, " ")...)
		}
		// "event:" lines are redundant — the data payload carries "type".
	}
	return sc.Err()
}

// anthEmitter renders the Anthropic Messages SSE contract from codex events:
// message_start / content_block_start / content_block_delta /
// content_block_stop / message_delta / message_stop, blocks indexed in arrival
// order. Text deltas map 1:1; a function_call becomes a tool_use block whose
// arguments stream as input_json_delta. Reasoning items are dropped (nothing
// Anthropic-shaped to put them in). writeErr latches the first sink failure.
type anthStream struct {
	w        io.Writer
	started  bool
	idx      int    // index the NEXT block will open at
	open     string // "" | "text" | "tool" | "toolPending"
	pending  codexItem
	pendArgs strings.Builder // arguments buffered for a name-less function_call
	toolCall string          // call_id of the open "tool" block
	toolArgs int             // argument bytes streamed into the open "tool" block
	sawTool  bool
	ended    bool
	failed   bool // response.failed was handled — §3 row 2's discriminator (#122)
	refused  bool // an incomplete response ended as an error (incompleteRefusal) — §3 row 21 (#132)
	writeErr error
	// blocksOpened counts content_block_start emits — §3's 4a/4b discriminator
	// (#122). Incremented at the three emit sites, never in ensureStart (C1).
	blocksOpened int
	textBytes    int           // model text only; the note never contributes evidence
	note         *responseNote // #124: immutable route snapshot, nil on healthy bound hops
	ratio        float64       // anthropic_token_ratio (#44): input_tokens ÷= ratio at response.completed
	mark         *scaleMark    // #76: provenance — resolved where the division happens (or doesn't)
	led          *ledgerHop    // C6: a paid dial's ledger entry, settled at the response's end; nil = none
	srv          *servedCap    // 65 §7: the served event's capture, given the upstream's own usage; nil = none
}

func (a *anthStream) emit(event string, payload any) {
	if a.writeErr != nil {
		return
	}
	b, err := json.Marshal(payload)
	if err != nil {
		a.writeErr = err
		return
	}
	_, a.writeErr = io.WriteString(a.w, "event: "+event+"\ndata: "+string(b)+"\n\n")
}

// emitError ends the Anthropic stream with an error frame carrying the
// upstream's own words.
func (a *anthStream) emitError(msg string) {
	a.emitTypedError(http.StatusBadGateway, msg)
}

func (a *anthStream) emitTypedError(status int, msg string) {
	a.emit("error", map[string]any{
		"type":  "error",
		"error": map[string]any{"type": codexErrType(status), "message": "codex upstream: " + msg},
	})
}

func (a *anthStream) ensureStart(model, msgID string) {
	if a.started {
		return
	}
	a.started = true
	if model == "" {
		model = "gpt"
	}
	if msgID == "" {
		msgID = "msg_codex"
	}
	// input_tokens STAYS 0 here, and no cache fields: codex reports usage only at
	// response.completed, and message_start is emitted at first byte — we do not
	// have the counts yet and must not invent them.
	//
	// This is safe because the agent gateway's readAnthropicPromptUsageSnapshot only seeds
	// contextUsage when messageStartPromptTokens is NON-ZERO. A zero leaves the
	// snapshot undefined and the message_delta guard carries the turn instead,
	// which is exactly what the explicit cache zeros there make possible. So the
	// whole TTFT-deadline problem is sidestepped: we never have to produce a
	// prompt-size estimate before dispatch.
	a.emit("message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id": msgID, "type": "message", "role": "assistant", "model": model,
			"content": []any{}, "stop_reason": nil, "stop_sequence": nil,
			"usage": map[string]int{"input_tokens": 0, "output_tokens": 0},
		},
	})
}

func (a *anthStream) closeBlock() {
	switch a.open {
	case "toolPending":
		// The added event had no name — emit the whole tool block from what the
		// done/flush point knows.
		a.blocksOpened++
		a.emit("content_block_start", map[string]any{
			"type": "content_block_start", "index": a.idx,
			"content_block": map[string]any{"type": "tool_use", "id": a.pending.CallID,
				"name": a.pending.Name, "input": map[string]any{}},
		})
		if s := a.pendArgs.String(); s != "" {
			a.emit("content_block_delta", map[string]any{
				"type": "content_block_delta", "index": a.idx,
				"delta": map[string]any{"type": "input_json_delta", "partial_json": s},
			})
		}
		fallthrough
	case "text", "tool":
		a.emit("content_block_stop", map[string]any{"type": "content_block_stop", "index": a.idx})
		a.idx++
	}
	a.open = ""
	a.pendArgs.Reset()
	a.toolCall, a.toolArgs = "", 0
}

func (a *anthStream) handle(ev codexSSEEvent) {
	switch ev.Type {
	case "response.created":
		var model, id string
		if ev.Response != nil {
			model, id = ev.Response.Model, ev.Response.ID
		}
		a.ensureStart(model, id)
	case "response.output_item.added":
		if ev.Item == nil || ev.Item.Type != "function_call" {
			return
		}
		a.ensureStart("", "")
		a.closeBlock()
		a.sawTool = true
		if ev.Item.Name == "" {
			a.open, a.pending = "toolPending", *ev.Item
			return
		}
		a.blocksOpened++
		a.emit("content_block_start", map[string]any{
			"type": "content_block_start", "index": a.idx,
			"content_block": map[string]any{"type": "tool_use", "id": ev.Item.CallID,
				"name": ev.Item.Name, "input": map[string]any{}},
		})
		a.open, a.toolCall = "tool", ev.Item.CallID
	case "response.output_text.delta":
		a.ensureStart("", "")
		if a.open != "text" {
			a.closeBlock()
			a.blocksOpened++
			a.emit("content_block_start", map[string]any{
				"type": "content_block_start", "index": a.idx,
				"content_block": map[string]any{"type": "text", "text": ""},
			})
			a.open = "text"
		}
		delta := ev.deltaString()
		if a.note != nil {
			a.textBytes += len(delta)
		}
		a.emit("content_block_delta", map[string]any{
			"type": "content_block_delta", "index": a.idx,
			"delta": map[string]any{"type": "text_delta", "text": delta},
		})
	case "response.function_call_arguments.delta":
		switch a.open {
		case "tool":
			delta := ev.deltaString()
			a.toolArgs += len(delta)
			a.emit("content_block_delta", map[string]any{
				"type": "content_block_delta", "index": a.idx,
				"delta": map[string]any{"type": "input_json_delta", "partial_json": delta},
			})
		case "toolPending":
			a.pendArgs.WriteString(ev.deltaString())
		}
	case "response.output_item.done":
		if ev.Item == nil {
			return
		}
		if ev.Item.Type == "function_call" && a.open == "toolPending" {
			// Backfill what the added event lacked before the flush.
			a.pending.Name, a.pending.CallID = ev.Item.Name, ev.Item.CallID
			if a.pendArgs.Len() == 0 && ev.Item.Arguments != "" {
				a.pendArgs.WriteString(ev.Item.Arguments)
			}
		}
		// A named call whose arguments never streamed carries them whole on the done
		// item. gpt-5.6-sol does this for parallel calls; dropping
		// them handed the client every call in the batch as `input: {}`.
		if ev.Item.Type == "function_call" && a.open == "tool" && a.toolArgs == 0 &&
			ev.Item.Arguments != "" && ev.Item.CallID == a.toolCall {
			a.emit("content_block_delta", map[string]any{
				"type": "content_block_delta", "index": a.idx,
				"delta": map[string]any{"type": "input_json_delta", "partial_json": ev.Item.Arguments},
			})
		}
		a.closeBlock()
	case "response.completed", "response.incomplete":
		// The response ended upstream: what it used is billed whether or not
		// the client can be given it.
		a.led.settle(ev.Response)
		a.srv.noteCodex(ev.Response)
		incomplete := ev.Type == "response.incomplete" || (ev.Response != nil && ev.Response.Status == "incomplete")
		if incomplete {
			if msg := incompleteRefusal(ev.Response, a.blocksOpened > 0 || a.open != ""); msg != "" {
				a.srv.inBand(http.StatusInternalServerError, msg, a.blocksOpened > 0 || a.open != "")
				a.emitError(msg)
				a.ended, a.refused = true, true
				return
			}
		}
		a.ensureStart("", "")
		a.closeBlock()
		stop := "end_turn"
		if incomplete {
			stop = "max_tokens"
		}
		// PLAN §1 (#44): only the input side converts to Anthropic units.
		// output_tokens stays the upstream count minus reasoning — it is
		// transcript-visible output, and the transcript re-enters the next
		// turn's input where the divided input_tokens covers it.
		u := codexUsage(ev.Response)
		a.mark.noteRawIn(u.in)
		if u.cacheReported {
			a.mark.noteRawCache(u.cacheRead, u.cacheWrite)
		}
		u = u.scaledInput(a.ratio)
		a.mark.resolveOK("translated")
		if stop == "end_turn" && a.sawTool {
			stop = "tool_use"
		}
		if a.note != nil && a.textBytes > 0 && stop != "tool_use" && a.writeErr == nil && !a.note.noted.Load() {
			emitNoteFrames(a.emit, a.idx, a.note.text)
			a.idx++
			if a.writeErr == nil {
				a.note.emitted()
			}
		}
		a.emit("message_delta", map[string]any{
			"type":  "message_delta",
			"delta": map[string]any{"stop_reason": stop, "stop_sequence": nil},
			"usage": anthUsage(u),
		})
		a.emit("message_stop", map[string]any{"type": "message_stop"})
		a.ended = true
	case "response.failed", "error":
		status, msg := codexFailure(ev)
		a.led.settleFailed(ev.Response, msg)
		a.srv.noteCodex(ev.Response) // the upstream billed what a failed response reports
		a.srv.inBand(status, codexFailureKind(ev), a.blocksOpened > 0 || a.open != "")
		a.emitTypedError(status, msg)
		a.ended, a.failed = true, true
	}
	// response.in_progress / content_part.* / *_text.done / reasoning*: no
	// Anthropic-side representation — ignored.
}

// translateCodexSSE streams a codex SSE body into w as Anthropic SSE. It always
// terminates the Anthropic stream (best-effort closes when the upstream stream
// dies mid-response).
// hs is §3's codex stream producer: ONE classification at this function's
// exit (r9 decision 1) — the five terminal exits all pass through the defer —
// reading (ended, failed, started, blocksOpened, writeErr, err, ctx.Err())
// once. nil = scoring off (mode off, a non-class hop, unit tests).
// up records how the upstream's events arrived, for the log line; nil = off.
// led is the dial's ledger entry, nil when it drew no counted pot: settled
// at the response's end, else entered uncounted when the stream ends first.
func translateCodexSSE(r io.Reader, w io.Writer, ratio float64, mark *scaleMark, hs *hopScore, up *upstreamTrace, led *ledgerHop, srv *servedCap) (err error) {
	a := &anthStream{w: w, ratio: ratio, mark: mark, led: led, srv: srv}
	defer func() {
		if a.writeErr != nil {
			led.abandon("the client left before the response ended")
			srv.incomplete(servedWhyLeft)
		} else {
			led.abandon("the stream ended before response.completed")
			srv.incomplete(servedWhyCut)
		}
	}()
	if hs != nil {
		if hs.cc != nil {
			a.note = hs.cc.note
		}
		defer func() { hs.settle(classifyCodexExit(a, err, hs.hopCtx())) }()
	}
	err = parseSSE(r, func(data []byte) bool {
		var ev codexSSEEvent
		if json.Unmarshal(data, &ev) != nil {
			up.event("", time.Now())
			return true // unparseable frame — skip, keep the stream alive
		}
		up.event(ev.Type, time.Now())
		a.handle(ev)
		return a.writeErr == nil && !a.ended
	})
	up.ended(a.ended, a.writeErr, err, time.Now())
	if a.writeErr != nil {
		return a.writeErr
	}
	if !a.ended {
		// Upstream cut off without response.completed: close the Anthropic
		// stream in-contract so the client doesn't hang on a half-open message.
		if !a.started {
			a.emitError("stream ended before any response event")
			return firstErr(a.writeErr, err)
		}
		if a.blocksOpened == 0 && a.open == "" {
			// Nothing came out (#132). An end_turn here is an empty reply the
			// client takes for a real one, so it gets the error the non-stream
			// path gives. No usage is published, so the mark stays pending, as
			// for the pre-start death above.
			a.emitError("stream ended before response.completed")
			return firstErr(a.writeErr, err)
		}
		a.closeBlock()
		// #76: the numbers below are placeholders, not conversions — the stream
		// died before the real counts arrived, so whatever the client reads was
		// never scaled. The mark says so instead of leaving "unknown" ambiguous.
		a.mark.failOpen("stream_truncated")
		// The cache fields are OMITTED here, deliberately, and this is the one
		// site where that is correct: the stream died before response.completed,
		// so we never learned the real counts. Absent means UNKNOWN to the agent gateway,
		// which then falls back to its persisted projection.
		//
		// Emitting explicit zeros here — the fix applied at every other site —
		// would assert "the prompt was empty", and a truncated turn would silently
		// reset the harness's idea of transcript size to nothing. input_tokens: 0
		// alone already does exactly that today (it lands in promptTokens as an
		// empty transcript after any truncated turn); leaving the cache fields off
		// is what makes the whole block read as unknown rather than as zero.
		a.emit("message_delta", map[string]any{
			"type":  "message_delta",
			"delta": map[string]any{"stop_reason": "end_turn", "stop_sequence": nil},
			"usage": map[string]int{"input_tokens": 0, "output_tokens": 0},
		})
		a.emit("message_stop", map[string]any{"type": "message_stop"})
	}
	return firstErr(a.writeErr, err)
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

// aggregateCodexSSE consumes the whole codex SSE stream and builds ONE
// Anthropic message JSON for a client that did not ask to stream. It reads
// only the item-done and terminal events — no delta bookkeeping to get wrong.
// Returns (status, body): 200 with the message, or an error envelope (which
// carries no usage, so the mark stays pending — no scaling claim either way).
func aggregateCodexSSE(r io.Reader, ratio float64, mark *scaleMark, up *upstreamTrace, led *ledgerHop, srv *servedCap) (int, []byte) {
	defer led.abandon("the stream ended before response.completed") // a no-op once settled
	defer srv.incomplete(servedWhyCut)                              // a no-op once its usage came
	blocks := []map[string]any{}
	var final *codexRespObj
	failed, incomplete := "", false
	failureStatus := http.StatusBadGateway
	err := parseSSE(r, func(data []byte) bool {
		var ev codexSSEEvent
		if json.Unmarshal(data, &ev) != nil {
			up.event("", time.Now())
			return true
		}
		up.event(ev.Type, time.Now())
		switch ev.Type {
		case "response.output_item.done":
			if ev.Item == nil {
				return true
			}
			switch ev.Item.Type {
			case "message":
				parts := []string{}
				for _, c := range ev.Item.Content {
					if c.Text != "" {
						parts = append(parts, c.Text)
					}
				}
				blocks = append(blocks, map[string]any{"type": "text", "text": strings.Join(parts, "")})
			case "function_call":
				var input any = map[string]any{}
				if ev.Item.Arguments != "" {
					var v any
					if json.Unmarshal([]byte(ev.Item.Arguments), &v) == nil {
						input = v
					}
				}
				blocks = append(blocks, map[string]any{"type": "tool_use",
					"id": ev.Item.CallID, "name": ev.Item.Name, "input": input})
			}
		case "response.completed", "response.incomplete":
			final = ev.Response
			led.settle(final)
			srv.noteCodex(final)
			incomplete = ev.Type == "response.incomplete" || (final != nil && final.Status == "incomplete")
			if final == nil && incomplete {
				final = &codexRespObj{Status: "incomplete"}
			}
			return false
		case "response.failed", "error":
			failureStatus, failed = codexFailure(ev)
			led.settleFailed(ev.Response, failed)
			srv.noteCodex(ev.Response)
			srv.inBand(failureStatus, codexFailureKind(ev), false) // a non-streamed client was sent nothing yet
			return false
		}
		return true
	})
	up.ended(failed != "" || final != nil, nil, err, time.Now())
	if failed != "" || final == nil {
		if failed == "" {
			failed = "stream ended before response.completed"
		}
		b, _ := json.Marshal(map[string]any{
			"type":  "error",
			"error": map[string]any{"type": codexErrType(failureStatus), "message": "codex upstream: " + failed},
		})
		return failureStatus, b
	}
	if incomplete {
		// The streaming path's error frame, as a status. 500 rather than 502
		// keeps it apart at the non-stream site: §3 row 21, not row 3.
		if msg := incompleteRefusal(final, len(blocks) > 0); msg != "" {
			srv.inBand(http.StatusInternalServerError, msg, false)
			b, _ := json.Marshal(map[string]any{
				"type":  "error",
				"error": map[string]any{"type": "api_error", "message": "codex upstream: " + msg},
			})
			return http.StatusInternalServerError, b
		}
	}
	stop := "end_turn"
	if incomplete {
		stop = "max_tokens"
	} else {
		for _, blk := range blocks {
			if blk["type"] == "tool_use" {
				stop = "tool_use"
			}
		}
	}
	u := codexUsage(final)
	mark.noteRawIn(u.in)
	if u.cacheReported {
		mark.noteRawCache(u.cacheRead, u.cacheWrite)
	}
	u = u.scaledInput(ratio) // input side only — see translateCodexSSE's completed case
	mark.resolveOK("translated_agg")
	model, id := final.Model, final.ID
	if model == "" {
		model = "gpt"
	}
	if id == "" {
		id = "msg_codex"
	}
	b, _ := json.Marshal(map[string]any{
		"id": id, "type": "message", "role": "assistant", "model": model,
		"content": blocks, "stop_reason": stop, "stop_sequence": nil,
		"usage": anthUsage(u),
	})
	return http.StatusOK, b
}

// codexErrType maps a codex HTTP status to the Anthropic error type vocabulary.
func codexErrType(status int) string {
	switch {
	case status == 429:
		return "rate_limit_error"
	case status == 401 || status == 403:
		return "authentication_error"
	case status >= 400 && status < 500:
		return "invalid_request_error"
	}
	return "api_error"
}

// translateCodexResponse rewraps a codex upstream response as the Anthropic
// response the client expects. Headers are REPLACED, not filtered — chatgpt.com
// sets cookies and fingerprinting headers that must not reach the client
// (Retry-After survives on errors; it is actionable). Non-200s become Anthropic
// error envelopes with the status preserved; a 200 is translated per the
// client's own stream flag (SSE re-framing, or one aggregated message).
//
// member labels WHICH backend produced an error, and is the only part of the
// envelope that identifies it. A class request can cross members mid-flight, so
// "codex upstream HTTP 429" alone leaves the reader unable to tell the bound
// member from the one that actually served — and the client cannot recover the
// difference from anything else in the response. Empty is tolerated (unit tests,
// and any future caller with nothing truthful to say) and simply omits the tag:
// a WRONG member id would be worse than none.
func translateCodexResponse(resp *http.Response, clientStream bool, member string, ratio float64, mark *scaleMark, hs *hopScore, up *upstreamTrace, led *ledgerHop, srv *servedCap) *http.Response {
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		snippet := strings.TrimSpace(string(b))
		if len(snippet) > 400 {
			snippet = snippet[:400] + "…"
		}
		tag := ""
		if member != "" {
			tag = "[" + member + "] "
		}
		env, _ := json.Marshal(map[string]any{
			"type": "error",
			"error": map[string]any{"type": codexErrType(resp.StatusCode),
				"message": fmt.Sprintf("%scodex upstream HTTP %d: %s", tag, resp.StatusCode, snippet)},
		})
		h := http.Header{}
		h.Set("Content-Type", "application/json")
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			h.Set("Retry-After", ra)
		}
		resp.Header = h
		resp.Body = io.NopCloser(bytes.NewReader(env))
		resp.ContentLength = int64(len(env))
		resp.TransferEncoding = nil
		return resp
	}
	if clientStream {
		pr, pw := io.Pipe()
		orig := resp.Body
		go func() {
			err := translateCodexSSE(orig, pw, ratio, mark, hs, up, led, srv)
			orig.Close()
			pw.CloseWithError(err)
		}()
		h := http.Header{}
		h.Set("Content-Type", "text/event-stream; charset=utf-8")
		h.Set("Cache-Control", "no-cache")
		resp.Header = h
		resp.Body = pr
		resp.ContentLength = -1
		resp.TransferEncoding = nil
		return resp
	}
	status, body := aggregateCodexSSE(resp.Body, ratio, mark, up, led, srv)
	resp.Body.Close()
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	resp.Header = h
	resp.StatusCode = status
	resp.Status = fmt.Sprintf("%d %s", status, http.StatusText(status))
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	resp.TransferEncoding = nil
	return resp
}

// Request actuation, the client's stream wish, and the 401 re-auth all live on
// codexWire (wire_codex.go) — this file is the translator plus the bearer cache.
