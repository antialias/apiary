package main

// Pure-function tests for the lane plumbing: model extraction, rule matching,
// and body buffering. 429 classification is scopes_test.go's.

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

var testRules = []laneRule{
	{Lane: "fable", Match: []string{"fable"}},
	{Lane: "opus", Match: []string{"opus"}},
	{Lane: "haiku", Match: []string{"haiku"}},
	{Lane: "default", Match: nil},
}

func TestLaneForModel(t *testing.T) {
	cases := []struct {
		model, want string
	}{
		{"claude-fable-5", "fable"},
		{"claude-opus-4-8", "opus"},
		{"claude-haiku-4-5-20251001", "haiku"},
		{"CLAUDE-FABLE-5", "fable"},    // case-insensitive
		{"claude-sonnet-5", "default"}, // no sonnet rule in this set -> catch-all
		{"", "default"},                // no model
		{"gpt-4o", "default"},          // unknown family
	}
	for _, c := range cases {
		if got := laneForModel(c.model, testRules); got != c.want {
			t.Errorf("laneForModel(%q) = %q, want %q", c.model, got, c.want)
		}
	}
	// no rules at all (old brain) => default, i.e. the legacy single-lane path
	if got := laneForModel("claude-fable-5", nil); got != "default" {
		t.Errorf("nil rules must mean single-lane fallback, got %q", got)
	}
}

func TestLaneRuleOrderFirstMatchWins(t *testing.T) {
	rules := []laneRule{
		{Lane: "special", Match: []string{"fable-5"}},
		{Lane: "fable", Match: []string{"fable"}},
	}
	if got := laneForModel("claude-fable-5", rules); got != "special" {
		t.Errorf("first matching rule must win, got %q", got)
	}
}

func TestExtractModelTopLevelOnly(t *testing.T) {
	cases := []struct {
		name, body, want string
	}{
		{"simple", `{"model":"claude-fable-5","max_tokens":1}`, "claude-fable-5"},
		{"model not first key", `{"max_tokens":1,"stream":true,"model":"claude-opus-4-8"}`, "claude-opus-4-8"},
		{"nested model must NOT match", `{"messages":[{"role":"user","content":"set model to x"},{"model":"claude-haiku-4-5"}],"max_tokens":1}`, ""},
		{"nested then top-level", `{"metadata":{"model":"claude-haiku-4-5"},"model":"claude-fable-5"}`, "claude-fable-5"},
		{"model in string value", `{"system":"the model field looks like \"model\":\"x\"","model":"claude-opus-4-8"}`, "claude-opus-4-8"},
		{"non-string model", `{"model":42}`, ""},
		{"empty body", ``, ""},
		{"not json", `GIF89a....binary`, ""},
		{"top-level array", `[{"model":"claude-fable-5"}]`, ""},
		{"truncated json", `{"stream":true,"mode`, ""},
	}
	for _, c := range cases {
		if got := extractModel([]byte(c.body)); got != c.want {
			t.Errorf("%s: extractModel = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestBufferBodyRestores(t *testing.T) {
	payload := `{"model":"claude-fable-5","big":"` + strings.Repeat("x", 4096) + `"}`
	r := httptest.NewRequest("POST", "/v1/messages", bytes.NewReader([]byte(payload)))
	r.ContentLength = -1 // simulate chunked: length unknown

	b, tooLarge := bufferBody(r)
	if tooLarge {
		t.Fatal("bufferBody flagged a small body as too large")
	}
	if string(b) != payload {
		t.Fatal("bufferBody returned wrong bytes")
	}
	if r.ContentLength != int64(len(payload)) {
		t.Errorf("ContentLength must be pinned to the buffered size, got %d", r.ContentLength)
	}
	// the body must still be fully readable downstream (ReverseProxy will send it)
	again, _ := io.ReadAll(r.Body)
	if string(again) != payload {
		t.Error("request body was not restored after buffering")
	}
}

// The 429 log line's ratelimit[...] capture is where an unseen scoped token is
// first seen (the body message is generic), so its format is
// load-bearing: every Anthropic-Ratelimit-* header, shortened and sorted,
// nothing else leaking in.
func TestRatelimitDetail(t *testing.T) {
	h := http.Header{}
	h.Set("Anthropic-Ratelimit-Unified-Status", "rejected")
	h.Set("Anthropic-Ratelimit-Unified-Reset", "1756150000")
	h.Set("Content-Type", "application/json")
	h.Set("Request-Id", "req_x")
	if got, want := ratelimitDetail(h), "unified-reset=1756150000 unified-status=rejected"; got != want {
		t.Errorf("ratelimitDetail = %q, want %q", got, want)
	}
	if got := ratelimitDetail(http.Header{}); got != "" {
		t.Errorf("no ratelimit headers must render empty, got %q", got)
	}
}
