package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCodexErrorEnvelopesPreserveCauseAndRetryClass(t *testing.T) {
	cases := []struct {
		name, event, kind, message string
		status                     int
	}{
		{"nested invalid", `{"type":"error","error":{"type":"invalid_request_error","code":"invalid_value","message":"input image cannot be decoded"}}`, "invalid_request_error", "invalid_value: input image cannot be decoded", 400},
		{"nested quota", `{"type":"error","error":{"code":"usage_limit_reached","message":"credits exhausted"}}`, "rate_limit_error", "usage_limit_reached: credits exhausted", 429},
		{"top level", `{"type":"error","code":"server_error","message":"backend unavailable"}`, "api_error", "server_error: backend unavailable", 502},
		{"response failed", `{"type":"response.failed","response":{"error":{"code":"context_length_exceeded","message":"context too large"}}}`, "invalid_request_error", "context_length_exceeded: context too large", 400},
		{"unknown", `{"type":"error"}`, "api_error", "codex stream error", 502},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			script := "data: " + tc.event + "\n\n"
			status, body := aggregateCodexSSE(strings.NewReader(script), 0, nil, nil, nil, nil)
			var result map[string]any
			if err := json.Unmarshal(body, &result); err != nil {
				t.Fatal(err)
			}
			e := result["error"].(map[string]any)
			if status != tc.status || e["type"] != tc.kind || e["message"] != "codex upstream: "+tc.message {
				t.Fatalf("aggregate = %d %s", status, body)
			}
			frames := streamFrames(t, script)
			last := frames[len(frames)-1]
			e = last.data["error"].(map[string]any)
			if last.event != "error" || e["type"] != tc.kind || e["message"] != "codex upstream: "+tc.message {
				t.Fatalf("stream = %#v", last)
			}
		})
	}
}
