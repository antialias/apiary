package main

// The OpenAI-compatible inbound wire (#96): POST /v1/chat/completions accepts
// the OpenAI Chat Completions dialect, normalizes it AT THE DOOR into one
// Anthropic-shaped /v1/messages request (D1), and re-dispatches it through the
// SAME requestHandler chain under a context marker — never a parallel routing
// path. The marker flips the chain into default-deny (D8): workload classes
// only, providers-only members (D2), no inject (D9), no transcript tap.
//
// Structured output rides a forced single tool_use carrier (D3) with thinking
// disabled (D4) — the kimi-validated shape from the strict-output probe.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// wireKey marks a request context as having entered through a non-Anthropic
// inbound wire. Value "openai" is the only wire today; the empty string means
// the native Anthropic wire (every existing caller).
const wireKey ctxKey = 1

func wireFromContext(ctx context.Context) string {
	s, _ := ctx.Value(wireKey).(string)
	return s
}

// wireDeadline bounds a wire request end to end: OpenAI SDK clients default to
// short timeouts and retry aggressively; a proxy-side cap keeps an upstream
// stall from pinning goroutines behind a client that already gave up.
var wireDeadline = 100 * time.Second

// D6: per-class token bucket. The numbers are blast-radius insurance for the
// shared subscription upstreams, not capacity planning.
var (
	wireRPM   = 10.0 // sustained requests/minute per class; <=0 disables the limiter
	wireBurst = 20.0 // bucket depth

	wireNow = time.Now // test hook; the buckets are per tenant (tenant.wireMu, tenant.wireBuckets)
)

type wireBucket struct {
	tokens float64
	last   time.Time
}

func wireInitFromEnv() {
	if v := os.Getenv("SWITCH_WIRE_RPM"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			wireRPM = f
		}
	}
	if v := os.Getenv("SWITCH_WIRE_BURST"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			wireBurst = f
		}
	}
}

func (tn *tenant) wireTake(class string) bool {
	if wireRPM <= 0 {
		return true
	}
	tn.wireMu.Lock()
	defer tn.wireMu.Unlock()
	now := wireNow()
	b := tn.wireBuckets[class]
	if b == nil {
		b = &wireBucket{tokens: wireBurst, last: now}
		tn.wireBuckets[class] = b
	}
	if el := now.Sub(b.last).Minutes(); el > 0 {
		b.tokens = min(wireBurst, b.tokens+el*wireRPM)
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// wireError is a typed refusal in the OpenAI error dialect.
type wireError struct {
	status  int
	errType string
	code    string
	msg     string
}

func writeOpenAIError(w http.ResponseWriter, status int, errType, code, msg string) {
	writeOpenAIErrorBody(w, status, errType, code, "", msg, nil)
}

// writeOpenAIErrorBody is writeOpenAIError with OpenAI's `param` (the request
// field at fault; omitted when "") and extra top-level fields beside the
// envelope (the image door's 400 carries the §2.8 body as "unavailable").
func writeOpenAIErrorBody(w http.ResponseWriter, status int, errType, code, param, msg string, extra map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Switch-Wire", "openai")
	w.WriteHeader(status)
	e := map[string]any{"message": msg, "type": errType, "code": code}
	if param != "" {
		e["param"] = param
	}
	env := map[string]any{"error": e}
	for k, v := range extra {
		env[k] = v
	}
	_ = json.NewEncoder(w).Encode(env)
}

func wireDeny(w http.ResponseWriter, r *http.Request, we *wireError, retryAfter string) {
	if retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	writeOpenAIError(w, we.status, we.errType, we.code, we.msg)
	st.record(r.Method, r.URL.Path, we.status, fp(r.Header.Get("Authorization")), "")
	log.Printf("%-4s %-28s -> %d  WIRE[openai] DENIED (%s)", r.Method, r.URL.Path, we.status, we.code)
}

// wireCapture buffers the core chain's answer so the renderer can translate it
// back into the OpenAI dialect. Stateless Flush: the response tap is never
// installed for wire traffic (tapTurn is skipped), so nothing streams here.
type wireCapture struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func newWireCapture() *wireCapture {
	return &wireCapture{header: make(http.Header), status: http.StatusOK}
}

func (c *wireCapture) Header() http.Header         { return c.header }
func (c *wireCapture) WriteHeader(s int)           { c.status = s }
func (c *wireCapture) Write(b []byte) (int, error) { return c.body.Write(b) }
func (c *wireCapture) Flush()                      {}

// wireNorm is a normalized wire request: the Anthropic-shaped body the core
// chain will route, plus what the renderer needs to translate the answer back.
type wireNorm struct {
	anthropicBody  []byte
	requestedModel string // echoed back verbatim in the response
	class          *registryEntry
	syntheticTool  string // non-empty: structured output rides this forced tool_use
}

var wireToolNameRe = regexp.MustCompile(`[^a-zA-Z0-9_-]`)

func sanitizeToolName(name string) string {
	s := wireToolNameRe.ReplaceAllString(name, "_")
	if len(s) > 64 {
		s = s[:64]
	}
	if s == "" {
		s = "structured_output"
	}
	return s
}

// oaContentText flattens an OpenAI message content field to plain text:
// a JSON string, null, or an array of {type:"text",text} parts. It is the
// only shape a system/developer/assistant turn may take — Anthropic's system
// is a string and codex's instructions is a string, and neither wire carries
// a non-user image. User turns go through oaContentBlocks instead, which also
// maps image_url parts. Any other shape (audio, file, tool parts) is a
// refusal, never a silent drop.
func oaContentText(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", true
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s, true
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &parts) != nil {
		return "", false
	}
	var b strings.Builder
	for _, p := range parts {
		if p.Type != "text" {
			return "", false
		}
		b.WriteString(p.Text)
	}
	return b.String(), true
}

// oaContentBlocks maps a USER message's content field to Anthropic content
// blocks: a JSON string or {type:"text"} parts become text blocks, and
// {type:"image_url"} parts become {type:"image",source:…} — base64 for data:
// URLs, url for http(s). Audio, file, tool and unknown parts, a non-image
// media type, and any other URL scheme are refusals, never a silent drop.
func oaContentBlocks(raw json.RawMessage) ([]map[string]any, bool) {
	blocks, _, ok := oaParseContent(raw)
	return blocks, ok
}

// oaParseContent is oaContentBlocks plus the reason: badImage separates "that
// image_url is not usable" from "that part type is not supported at all", so
// the refusal can name the real problem.
func oaParseContent(raw json.RawMessage) (blocks []map[string]any, badImage bool, ok bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, false, true
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return []map[string]any{{"type": "text", "text": s}}, false, true
	}
	var parts []struct {
		Type     string          `json:"type"`
		Text     string          `json:"text"`
		ImageURL json.RawMessage `json:"image_url"`
	}
	if json.Unmarshal(raw, &parts) != nil {
		return nil, false, false
	}
	blocks = make([]map[string]any, 0, len(parts))
	for _, p := range parts {
		switch p.Type {
		case "text":
			blocks = append(blocks, map[string]any{"type": "text", "text": p.Text})
		case "image_url":
			url, ok := oaImageURL(p.ImageURL)
			if !ok {
				return nil, true, false
			}
			src, ok := anthImageSource(url)
			if !ok {
				return nil, true, false
			}
			blocks = append(blocks, map[string]any{"type": "image", "source": src})
		default:
			return nil, false, false
		}
	}
	return blocks, false, true
}

// oaImageURL reads the url out of an image_url part, which OpenAI clients
// write either as {url, detail} or as a bare string. detail is advisory —
// Anthropic has no equivalent — so it is dropped.
func oaImageURL(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", false
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s, s != ""
	}
	var obj struct {
		URL string `json:"url"`
	}
	if json.Unmarshal(raw, &obj) != nil || obj.URL == "" {
		return "", false
	}
	return obj.URL, true
}

// hasSchemeFold is a case-insensitive prefix test that does not lower-case the
// whole string — a data URL here is megabytes of base64.
func hasSchemeFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

// anthImageSource turns an OpenAI image url into an Anthropic image source:
// "data:<media_type>[;param…];base64,<data>" -> a base64 source, http(s) -> a
// url source. The scheme and the base64 token are matched case-insensitively
// (RFC 2397 is case-insensitive there) and extra ;parameters are dropped, but
// base64 must be the LAST parameter — anything else is a differently encoded
// payload we would be mislabelling. A non-image/* media type and every other
// scheme (file:, blob:, a bare path) are refused rather than handed to an
// upstream that would fetch or reinterpret them on our behalf.
func anthImageSource(rawURL string) (map[string]any, bool) {
	url := strings.TrimSpace(rawURL)
	if hasSchemeFold(url, "data:") {
		meta, data, ok := strings.Cut(url[len("data:"):], ",")
		if !ok {
			return nil, false
		}
		if data = strings.TrimSpace(data); data == "" {
			return nil, false
		}
		params := strings.Split(meta, ";")
		if len(params) < 2 || !strings.EqualFold(strings.TrimSpace(params[len(params)-1]), "base64") {
			return nil, false
		}
		mediaType := strings.ToLower(strings.TrimSpace(params[0]))
		if !strings.HasPrefix(mediaType, "image/") || mediaType == "image/" {
			return nil, false
		}
		return map[string]any{"type": "base64", "media_type": mediaType, "data": data}, true
	}
	if hasSchemeFold(url, "http://") || hasSchemeFold(url, "https://") {
		return map[string]any{"type": "url", "url": url}, true
	}
	return nil, false
}

// isTextBlock reports whether a block is a text block carrying actual text.
// An empty (or whitespace-only) text block is not something a turn should
// ever carry: both Anthropic and Moonshot 400 on one.
func isTextBlock(blk map[string]any) bool { return blk["type"] == "text" }

func blockText(blk map[string]any) string {
	s, _ := blk["text"].(string)
	return s
}

// collapseTextBlocks keeps the flattened string form for text-only turns: the
// outbound body then stays byte-identical to the pre-image wire, and only a
// turn that actually carries an image pays the array shape. In array form the
// empty text blocks are dropped instead of being sent.
func collapseTextBlocks(blocks []map[string]any) any {
	var b strings.Builder
	for _, blk := range blocks {
		if !isTextBlock(blk) {
			return dropEmptyText(blocks)
		}
		b.WriteString(blockText(blk))
	}
	return b.String()
}

// dropEmptyText removes text blocks with nothing in them (a null turn, an
// empty "text" part, a padding newline next to an image).
func dropEmptyText(blocks []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(blocks))
	for _, blk := range blocks {
		if isTextBlock(blk) && strings.TrimSpace(blockText(blk)) == "" {
			continue
		}
		out = append(out, blk)
	}
	return out
}

// asBlocks promotes a flattened string turn back to block form so a text-only
// turn can be merged with a following same-role turn that carries images. An
// empty string promotes to nothing.
func asBlocks(c any) []map[string]any {
	switch v := c.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return nil
		}
		return []map[string]any{{"type": "text", "text": v}}
	case []map[string]any:
		return v
	}
	return nil
}

// mergeContent joins two consecutive same-role turns. String+string keeps the
// historical "\n\n" join; once either side carries blocks the string side is
// promoted, and a leading text block still joins into a trailing one with the
// same "\n\n" rather than arriving as a second adjacent text block.
func mergeContent(prev, next any) any {
	if ps, ok := prev.(string); ok {
		if ns, ok := next.(string); ok {
			return ps + "\n\n" + ns
		}
	}
	pb, nb := asBlocks(prev), asBlocks(next)
	merged := make([]map[string]any, 0, len(pb)+len(nb))
	merged = append(merged, pb...)
	for i, blk := range nb {
		if i == 0 && len(merged) > 0 {
			last := merged[len(merged)-1]
			if isTextBlock(last) && isTextBlock(blk) {
				merged[len(merged)-1] = map[string]any{
					"type": "text", "text": blockText(last) + "\n\n" + blockText(blk)}
				continue
			}
		}
		merged = append(merged, blk)
	}
	return merged
}

// oaContentRefusal names why a turn's content was rejected: an image in a turn
// that may not carry one, an image_url we cannot turn into a source, or a part
// type this wire does not speak at all.
func oaContentRefusal(i int, raw json.RawMessage) string {
	_, badImage, ok := oaParseContent(raw)
	switch {
	case ok:
		return fmt.Sprintf("messages[%d]: images are only supported in user messages", i)
	case badImage:
		return fmt.Sprintf("messages[%d]: image_url must be an http(s) URL or a base64 data URL with an image/* media type", i)
	default:
		return fmt.Sprintf("messages[%d]: only text content is supported on this endpoint", i)
	}
}

// wireKnownKeys is the OpenAI Chat Completions surface we either map or
// deliberately ignore. Anything else is logged BY NAME ONLY (bodies are
// user content) so a client quietly relying on an unmapped knob is visible.
var wireKnownKeys = map[string]bool{
	"model": true, "messages": true, "max_tokens": true, "max_completion_tokens": true,
	"temperature": true, "top_p": true, "stop": true, "n": true, "stream": true,
	"stream_options": true, "response_format": true, "user": true, "metadata": true,
	"frequency_penalty": true, "presence_penalty": true, "logit_bias": true,
	"logprobs": true, "top_logprobs": true, "seed": true, "store": true,
	"service_tier": true, "parallel_tool_calls": true, "tools": true,
	"tool_choice": true, "reasoning_effort": true,
}

func present(top map[string]json.RawMessage, key string) (json.RawMessage, bool) {
	raw, has := top[key]
	if !has || string(raw) == "null" {
		return nil, false
	}
	return raw, true
}

func normalizeChatRequest(tn *tenant, body []byte) (*wireNorm, *wireError) {
	bad := func(code, msg string) (*wireNorm, *wireError) {
		return nil, &wireError{http.StatusBadRequest, "invalid_request_error", code, msg}
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return bad("invalid_json", "request body is not a JSON object")
	}
	for k := range top {
		if !wireKnownKeys[k] {
			log.Printf("WIRE[openai] ignoring unknown request key %q", k)
		}
	}

	var model string
	if raw, has := present(top, "model"); !has || json.Unmarshal(raw, &model) != nil || model == "" {
		return bad("invalid_model", "model is required")
	}
	// D7/D8: workload classes only, with the bare alias ("tutor" for
	// "claude-tutor") accepted on THIS wire alone. Named providers, lane ids,
	// and unknown ids all die here — never matchProvider, never laneForModel.
	ce, ok := tn.matchRegistryClass(model)
	if !ok {
		ce, ok = tn.matchRegistryClass("claude-" + model)
	}
	if !ok {
		return nil, &wireError{http.StatusNotFound, "invalid_request_error", "model_not_found",
			"model " + model + " does not exist or is not a workload class (this endpoint routes workload classes only)"}
	}

	if raw, has := present(top, "stream"); has {
		var b bool
		if json.Unmarshal(raw, &b) == nil && b {
			return bad("unsupported_value", "stream is not supported on this endpoint")
		}
	}
	if raw, has := present(top, "n"); has {
		var n float64
		if json.Unmarshal(raw, &n) == nil && n > 1 {
			return bad("unsupported_value", "n must be 1")
		}
	}
	if _, has := present(top, "tools"); has {
		return bad("unsupported_value", "client tools are not supported; use response_format json_schema for structured output")
	}
	if _, has := present(top, "tool_choice"); has {
		return bad("unsupported_value", "tool_choice is not supported; use response_format json_schema for structured output")
	}

	rawMsgs, has := present(top, "messages")
	if !has {
		return bad("invalid_messages", "messages is required")
	}
	var msgs []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(rawMsgs, &msgs); err != nil {
		return bad("invalid_messages", "messages must be an array of {role, content}")
	}
	// Content is `any`: a plain string for text-only turns (the historical
	// shape, kept byte-for-byte) or a block array once a user turn carries an
	// image.
	type anthMsg struct {
		Role    string `json:"role"`
		Content any    `json:"content"`
	}
	var systemParts []string
	var out []anthMsg
	for i, m := range msgs {
		switch m.Role {
		case "system", "developer":
			text, ok := oaContentText(m.Content)
			if !ok {
				return bad("unsupported_content", oaContentRefusal(i, m.Content))
			}
			systemParts = append(systemParts, text)
		case "user", "assistant":
			var content any
			if m.Role == "user" {
				blocks, ok := oaContentBlocks(m.Content)
				if !ok {
					return bad("unsupported_content", oaContentRefusal(i, m.Content))
				}
				content = collapseTextBlocks(blocks)
			} else {
				// An assistant turn is model output; images there are a client
				// mistake, and dropping them silently would rewrite history.
				text, ok := oaContentText(m.Content)
				if !ok {
					return bad("unsupported_content", oaContentRefusal(i, m.Content))
				}
				content = text
			}
			// Merge consecutive same-role turns: Anthropic requires strict
			// alternation and some upstreams (kimi) 400 without it.
			if len(out) > 0 && out[len(out)-1].Role == m.Role {
				out[len(out)-1].Content = mergeContent(out[len(out)-1].Content, content)
			} else {
				out = append(out, anthMsg{Role: m.Role, Content: content})
			}
		case "tool":
			return bad("unsupported_value", fmt.Sprintf("messages[%d]: tool messages are not supported", i))
		default:
			return bad("unsupported_value", fmt.Sprintf("messages[%d]: unsupported role %q", i, m.Role))
		}
	}
	if len(out) == 0 {
		return bad("invalid_messages", "messages must contain at least one user or assistant message")
	}

	maxTokens := 4096
	for _, k := range []string{"max_completion_tokens", "max_tokens"} {
		if raw, has := present(top, k); has {
			var f float64
			if json.Unmarshal(raw, &f) != nil || f < 1 {
				return bad("unsupported_value", k+" must be a positive integer")
			}
			maxTokens = int(f)
			break
		}
	}

	ab := map[string]any{
		"model":      ce.renderedID(), // the id the core chain's class match speaks
		"max_tokens": maxTokens,
		"messages":   out,
	}
	if len(systemParts) > 0 {
		ab["system"] = strings.Join(systemParts, "\n\n")
	}
	if raw, has := present(top, "temperature"); has {
		var f float64
		if json.Unmarshal(raw, &f) != nil {
			return bad("unsupported_value", "temperature must be a number")
		}
		if f > 1 {
			log.Printf("WIRE[openai] clamping temperature %.2f -> 1.0 (anthropic range)", f)
			f = 1.0
		}
		ab["temperature"] = f
	}
	if raw, has := present(top, "top_p"); has {
		var f float64
		if json.Unmarshal(raw, &f) != nil {
			return bad("unsupported_value", "top_p must be a number")
		}
		ab["top_p"] = f
	}
	if raw, has := present(top, "stop"); has {
		var one string
		var many []string
		if json.Unmarshal(raw, &one) == nil {
			ab["stop_sequences"] = []string{one}
		} else if json.Unmarshal(raw, &many) == nil {
			if len(many) > 0 {
				ab["stop_sequences"] = many
			}
		} else {
			return bad("unsupported_value", "stop must be a string or an array of strings")
		}
	}

	syntheticTool := ""
	if raw, has := present(top, "response_format"); has {
		var rf struct {
			Type       string `json:"type"`
			JSONSchema struct {
				Name        string          `json:"name"`
				Description string          `json:"description"`
				Schema      json.RawMessage `json:"schema"`
			} `json:"json_schema"`
		}
		if err := json.Unmarshal(raw, &rf); err != nil {
			return bad("unsupported_value", "response_format could not be parsed")
		}
		switch rf.Type {
		case "", "text":
		case "json_schema":
			if len(rf.JSONSchema.Schema) == 0 {
				return bad("unsupported_value", "response_format.json_schema.schema is required")
			}
			// D3+D4: one forced tool_use carries the schema; thinking off —
			// the probe-validated shape every target provider honors.
			syntheticTool = sanitizeToolName(rf.JSONSchema.Name)
			desc := rf.JSONSchema.Description
			if desc == "" {
				desc = "Emit the structured output."
			}
			ab["tools"] = []map[string]any{{
				"name":         syntheticTool,
				"description":  desc,
				"input_schema": rf.JSONSchema.Schema,
			}}
			ab["tool_choice"] = map[string]any{"type": "tool", "name": syntheticTool}
			ab["thinking"] = map[string]any{"type": "disabled"}
		case "json_object":
			return bad("unsupported_value", "response_format json_object is not supported; use json_schema")
		default:
			return bad("unsupported_value", "unsupported response_format type "+rf.Type)
		}
	}

	nb, err := json.Marshal(ab)
	if err != nil {
		return bad("invalid_json", "request could not be normalized")
	}
	return &wireNorm{anthropicBody: nb, requestedModel: model, class: ce, syntheticTool: syntheticTool}, nil
}

// renderChatResponse translates the core chain's Anthropic-shaped answer back
// into the OpenAI Chat Completions dialect. Usage numbers are already in
// Anthropic units — the class transport scaled provider-native input by r_p —
// so prompt_tokens is simply the (scaled) input + cache reads/creates (D10).
func renderChatResponse(w http.ResponseWriter, cw *wireCapture, norm *wireNorm) {
	if cw.header.Get("X-Switch-Quota") == "exhausted" {
		// The class is unavailable (§2.8): the class transport already rendered
		// it in this dialect (rate_limit_exceeded with Retry-After when a time
		// exists, insufficient_quota when only money would help), so it goes
		// out as it is, headers and all.
		for k, vs := range cw.header {
			if k != "Content-Length" {
				w.Header()[k] = vs
			}
		}
		w.Header().Set("X-Switch-Wire", "openai")
		w.WriteHeader(cw.status)
		_, _ = w.Write(cw.body.Bytes())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Switch-Wire", "openai")
	w.Header().Set("X-Switch-Class", norm.class.renderedID())
	if ra := cw.header.Get("Retry-After"); ra != "" {
		w.Header().Set("Retry-After", ra)
	}
	for _, k := range []string{"X-Switch-Decision", "X-Switch-Health"} {
		if v := cw.header.Get(k); v != "" {
			w.Header().Set(k, v)
		}
	}
	if cw.status < 200 || cw.status > 299 {
		var env struct {
			Error struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			} `json:"error"`
		}
		msg, typ := "upstream error", "api_error"
		if json.Unmarshal(cw.body.Bytes(), &env) == nil && env.Error.Message != "" {
			msg = env.Error.Message
			if env.Error.Type != "" {
				typ = env.Error.Type
			}
		}
		w.WriteHeader(cw.status)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"message": msg, "type": typ, "code": typ},
		})
		return
	}
	var msg struct {
		ID         string `json:"id"`
		StopReason string `json:"stop_reason"`
		Content    []struct {
			Type  string          `json:"type"`
			Text  string          `json:"text"`
			Name  string          `json:"name"`
			Input json.RawMessage `json:"input"`
		} `json:"content"`
		Usage struct {
			InputTokens              int `json:"input_tokens"`
			OutputTokens             int `json:"output_tokens"`
			CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
			CacheReadInputTokens     int `json:"cache_read_input_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(cw.body.Bytes(), &msg); err != nil {
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"message": "upstream returned an unparseable response", "type": "api_error", "code": "bad_upstream_response"},
		})
		return
	}
	content := ""
	if norm.syntheticTool != "" {
		for _, b := range msg.Content {
			if b.Type == "tool_use" && b.Name == norm.syntheticTool {
				content = string(b.Input)
				break
			}
		}
	}
	if content == "" {
		var sb strings.Builder
		for _, b := range msg.Content {
			if b.Type == "text" {
				sb.WriteString(b.Text)
			}
		}
		content = sb.String()
	}
	finish := "stop"
	if msg.StopReason == "max_tokens" {
		finish = "length"
	}
	prompt := msg.Usage.InputTokens + msg.Usage.CacheCreationInputTokens + msg.Usage.CacheReadInputTokens
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":      "chatcmpl-" + msg.ID,
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   norm.requestedModel,
		"choices": []map[string]any{{
			"index":         0,
			"message":       map[string]any{"role": "assistant", "content": content},
			"finish_reason": finish,
		}},
		"usage": map[string]any{
			"prompt_tokens":     prompt,
			"completion_tokens": msg.Usage.OutputTokens,
			"total_tokens":      prompt + msg.Usage.OutputTokens,
		},
	})
}

// openaiWireHandler is the door: normalize, rate-limit, mark the context, and
// re-dispatch through the same core chain the Anthropic wire uses.
func openaiWireHandler(core http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, tooLarge := bufferBody(r)
		if tooLarge {
			wireDeny(w, r, &wireError{http.StatusRequestEntityTooLarge, "invalid_request_error", "request_too_large",
				fmt.Sprintf("request body exceeds %d bytes (SWITCH_MAX_BODY_BYTES)", maxBodyBytes)}, "")
			return
		}
		// The OpenAI wire is the base-URL entry's, so its tenant is the lan
		// tenant (tenantFrom); the classes and the buckets are that tenant's.
		// No tenant: the typed 503 before anything reads a tenant's classes,
		// never a model_not_found that blames the caller's model.
		tn := tenantFrom(r.Context())
		if tn == nil {
			writeNoTenant(w, "")
			return
		}
		norm, werr := normalizeChatRequest(tn, body)
		if werr != nil {
			wireDeny(w, r, werr, "")
			return
		}
		if !tn.wireTake(norm.class.Class) {
			wireDeny(w, r, &wireError{http.StatusTooManyRequests, "rate_limit_exceeded", "rate_limit_exceeded",
				"class " + norm.class.Class + " is over its wire rate limit"}, "30")
			return
		}
		// ctxKeyProxyDeadline tags this WithTimeout as PROXY-owned so §3 Rule 0
		// can confirm a DeadlineExceeded is ours, not the client's (#122).
		ctx, cancel := context.WithTimeout(
			context.WithValue(r.Context(), ctxKeyProxyDeadline, true), wireDeadline)
		defer cancel()
		ctx = context.WithValue(ctx, wireKey, "openai")
		r2 := r.Clone(ctx)
		r2.URL.Path = "/v1/messages"
		r2.URL.RawPath = ""
		r2.Body = io.NopCloser(bytes.NewReader(norm.anthropicBody))
		r2.ContentLength = int64(len(norm.anthropicBody))
		r2.Header.Set("Content-Type", "application/json")
		// The capture must hold plain JSON: without this a gzip-negotiating
		// client would make the upstream compress and the renderer read garbage.
		// (Go's transport re-adds its own gzip and decodes it transparently.)
		r2.Header.Del("Accept-Encoding")
		// x-claude-code-session-id passes through untouched: class stickiness
		// wants it, and D9 safety comes from the context marker, not the header.
		cw := newWireCapture()
		core.ServeHTTP(cw, r2)
		renderChatResponse(w, cw, norm)
	})
}
