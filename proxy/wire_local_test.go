package main

import (
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"
)

func localResp(status int, body string) *http.Response {
	h := http.Header{}
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body))}
}

// localWire rewrites exactly llama-server's over-window refusal, taking N and M
// from its message, and hands every other response back byte for byte.
func TestLocalWireTranslate(t *testing.T) {
	const tooLong = `{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: %s tokens > %s maximum"}}`
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   string // "" = untouched
	}{
		{"request wording", 400,
			`{"error":{"code":400,"message":"request (70123 tokens) exceeds the available context size (65536 tokens), try increasing it","type":"exceed_context_size_error","n_prompt_tokens":70123,"n_ctx":65536}}`,
			"70123|65536"},
		{"input wording", 400,
			`{"error":{"code":400,"message":"input (140000 tokens) is larger than the max context size (131072 tokens). skipping","type":"exceed_context_size_error","n_prompt_tokens":140000,"n_ctx":131072}}`,
			"140000|131072"},
		{"message from the message, not the fields", 400,
			`{"error":{"code":400,"message":"request (9 tokens) exceeds the available context size (8 tokens), try increasing it","type":"exceed_context_size_error","n_prompt_tokens":1,"n_ctx":2}}`,
			"9|8"},
		{"unknown wording passes through", 400,
			`{"error":{"code":400,"message":"something new","type":"exceed_context_size_error","n_prompt_tokens":70000,"n_ctx":65536}}`,
			""},
		{"other 400", 400, `{"error":{"code":400,"message":"model 'x' not found","type":"invalid_request_error"}}`, ""},
		{"anthropic-shaped 400", 400, `{"type":"error","error":{"type":"invalid_request_error","message":"request (5 tokens) exceeds the available context size (4 tokens)"}}`, ""},
		{"not json", 400, `bad request`, ""},
		{"same type, not a 400", 500, `{"error":{"code":500,"message":"request (5 tokens) exceeds the available context size (4 tokens), try increasing it","type":"exceed_context_size_error"}}`, ""},
		{"no counts anywhere", 400, `{"error":{"code":400,"message":"?","type":"exceed_context_size_error"}}`, ""},
		{"200", 200, `{"type":"message"}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := localWire{}.translate(localResp(tc.status, tc.body), reqInfo{})
			b, _ := io.ReadAll(out.Body)
			if out.StatusCode != tc.status {
				t.Fatalf("status %d, want %d", out.StatusCode, tc.status)
			}
			if tc.want == "" {
				if string(b) != tc.body || out.Header.Get("Content-Type") != "application/json; charset=utf-8" {
					t.Fatalf("touched: %s %v", b, out.Header)
				}
				return
			}
			nm := strings.Split(tc.want, "|")
			want := strings.Replace(strings.Replace(tooLong, "%s", nm[0], 1), "%s", nm[1], 1)
			var gotJSON, wantJSON any
			if err := json.Unmarshal(b, &gotJSON); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(want), &wantJSON); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotJSON, wantJSON) {
				t.Fatalf("body\n got %s\nwant %s", b, want)
			}
			if out.Header.Get("Content-Length") != strconv.Itoa(len(b)) || out.ContentLength != int64(len(b)) {
				t.Fatalf("length header %q / %d for %d bytes", out.Header.Get("Content-Length"), out.ContentLength, len(b))
			}
			if out.Header.Get("Content-Type") != "application/json" {
				t.Fatalf("Content-Type %q", out.Header.Get("Content-Type"))
			}
		})
	}
}

// A valid refusal in a partial body must not turn a truncated response into a
// successful translation or hide a later read error from the downstream client.
func TestLocalWireIncompleteRefusal(t *testing.T) {
	raw := llamaOverWindowBody("request (9 tokens) exceeds the available context size (8 tokens), try increasing it", 9, 8)
	for _, tc := range []struct {
		name        string
		body        string
		interrupted bool
	}{
		{"oversize", raw + strings.Repeat(" ", (64<<10)-len(raw)) + "trailing bytes", false},
		{"read error", raw, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := localResp(400, tc.body)
			if tc.interrupted {
				resp.Body = io.NopCloser(iotest.TimeoutReader(strings.NewReader(tc.body)))
			}
			out := localWire{}.translate(resp, reqInfo{})
			b, err := io.ReadAll(out.Body)
			if string(b) != tc.body || tc.interrupted != (err == iotest.ErrTimeout) {
				t.Fatalf("returned %d of %d bytes with err %v", len(b), len(tc.body), err)
			}
			out.Body.Close()
		})
	}
}
