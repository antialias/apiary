package main

import (
	"net/http"
	"testing"
)

func TestParseReqFacts(t *testing.T) {
	hdr := func(beta string) http.Header {
		h := http.Header{}
		if beta != "" {
			h.Set("anthropic-beta", beta)
		}
		return h
	}
	cases := []struct {
		name string
		h    http.Header
		body string
		want reqFacts
		tag  string
	}{
		{
			name: "claude code 1m child: beta + output_config effort + adaptive thinking",
			h:    hdr("oauth-2025-04-20,context-1m-2025-08-07,interleaved-thinking-2025-05-14"),
			body: `{"model":"claude-capable","output_config":{"effort":"medium"},"thinking":{"type":"adaptive"},"messages":[]}`,
			want: reqFacts{OneMega: true, Effort: "medium", Think: "adaptive"},
			tag:  " 1m=y effort=medium think=adaptive",
		},
		{
			name: "200k child: no context-1m flag, xhigh",
			h:    hdr("oauth-2025-04-20"),
			body: `{"model":"claude-capable","output_config":{"effort":"xhigh"},"messages":[]}`,
			want: reqFacts{Effort: "xhigh"},
			tag:  " 1m=n effort=xhigh think=-",
		},
		{
			name: "legacy top-level effort and a thinking budget",
			h:    hdr(""),
			body: `{"model":"m","effort":"low","thinking":{"type":"enabled","budget_tokens":4096}}`,
			want: reqFacts{Effort: "low", Think: "enabled:4096"},
			tag:  " 1m=n effort=low think=enabled:4096",
		},
		{
			name: "integer effort renders as digits",
			h:    hdr(""),
			body: `{"model":"m","output_config":{"effort":42}}`,
			want: reqFacts{Effort: "42"},
			tag:  " 1m=n effort=42 think=-",
		},
		{
			name: "output_config wins over top-level when both present",
			h:    hdr(""),
			body: `{"effort":"high","output_config":{"effort":"low"}}`,
			want: reqFacts{Effort: "low"},
			tag:  " 1m=n effort=low think=-",
		},
		{
			name: "non-JSON body keeps the header fact only",
			h:    hdr("context-1m-2025-08-07"),
			body: `not json`,
			want: reqFacts{OneMega: true},
			tag:  " 1m=y effort=- think=-",
		},
		{
			name: "empty body, no header",
			h:    hdr(""),
			body: ``,
			want: reqFacts{},
			tag:  " 1m=n effort=- think=-",
		},
		{
			name: "null and object efforts are absent, not garbage",
			h:    hdr(""),
			body: `{"effort":null,"output_config":{"effort":{"x":1}}}`,
			want: reqFacts{},
			tag:  " 1m=n effort=- think=-",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseReqFacts(c.h, []byte(c.body))
			if got != c.want {
				t.Fatalf("facts: got %+v, want %+v", got, c.want)
			}
			if tag := got.logTag(); tag != c.tag {
				t.Fatalf("tag: got %q, want %q", tag, c.tag)
			}
		})
	}
}

// The bound's facts (quota-model 66 §1): max_tokens when it is an integer
// >= 0, and whether the request names a server-side tool: a tools entry
// typed other than "custom", or mcp_servers. Neither is logged.
func TestParseReqFactsBound(t *testing.T) {
	none := -1
	cases := []struct {
		name   string
		body   string
		max    int // none: nil
		server bool
	}{
		{"max_tokens, no tools", `{"model":"m","max_tokens":32000,"messages":[]}`, 32000, false},
		{"zero", `{"max_tokens":0}`, 0, false},
		{"absent", `{"model":"m"}`, none, false},
		{"null", `{"max_tokens":null}`, none, false},
		{"a fraction", `{"max_tokens":16.5}`, none, false},
		{"negative", `{"max_tokens":-1}`, none, false},
		{"a string", `{"max_tokens":"4096"}`, none, false},
		{"client tools, untyped or custom",
			`{"max_tokens":8,"tools":[{"name":"Read","input_schema":{}},{"type":"custom","name":"x"}]}`, 8, false},
		{"a typed null is untyped", `{"max_tokens":8,"tools":[{"type":null,"name":"x"}]}`, 8, false},
		{"web search", `{"max_tokens":8,"tools":[{"name":"Read"},{"type":"web_search_20250305","name":"web_search"}]}`,
			8, true},
		{"an Anthropic-defined client tool errs toward no bound",
			`{"max_tokens":8,"tools":[{"type":"bash_20250124","name":"bash"}]}`, 8, true},
		{"a type that is not a string", `{"max_tokens":8,"tools":[{"type":7}]}`, 8, true},
		{"mcp_servers", `{"max_tokens":8,"mcp_servers":[{"type":"url","url":"https://x"}]}`, 8, true},
		{"mcp_servers null", `{"max_tokens":8,"mcp_servers":null}`, 8, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseReqFacts(http.Header{}, []byte(c.body))
			switch {
			case c.max == none && got.MaxTokens != nil:
				t.Fatalf("MaxTokens = %d, want nil", *got.MaxTokens)
			case c.max != none && (got.MaxTokens == nil || *got.MaxTokens != c.max):
				t.Fatalf("MaxTokens = %v, want %d", got.MaxTokens, c.max)
			}
			if got.ServerTools != c.server {
				t.Fatalf("ServerTools = %v, want %v", got.ServerTools, c.server)
			}
			if tag := got.logTag(); tag != " 1m=n effort=- think=-" {
				t.Fatalf("tag %q: the bound's facts are not logged", tag)
			}
		})
	}
}

// A tools field that is not a list of objects costs no other fact, and errs
// toward no bound.
func TestParseReqFactsMalformedTools(t *testing.T) {
	for _, tools := range []string{`{}`, `["x"]`, `"web_search"`, `7`} {
		t.Run(tools, func(t *testing.T) {
			got := parseReqFacts(http.Header{}, []byte(`{"max_tokens":8,"speed":"fast","output_config":{"effort":"high"},`+
				`"thinking":{"type":"adaptive"},"tools":`+tools+`}`))
			if got.MaxTokens == nil || *got.MaxTokens != 8 || !got.ServerTools || got.Effort != "high" ||
				got.Think != "adaptive" || got.Speed != "fast" {
				t.Fatalf("facts %+v", got)
			}
		})
	}
}
