package main

// #132. testdata/codex_incomplete_132.sse is the codex backend's real reply to
// a 126-tool request. It is an HTTP 200 stream:
// response.created, response.in_progress, response.metadata, then
// response.incomplete with reason max_output_tokens, zero usage and no output.
// The backend turned the request away before generating anything.
//
// The translator used to have no case for response.incomplete, so the stream
// ended unterminated and was closed as an ordinary, empty end_turn: the client
// took the refusal for a real reply. Non-streaming clients got a 502 for the
// same bytes. Both now get an api_error naming the reason (§3 row 21), and
// health takes no evidence from it: the answer was about this request.

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
)

// incomplete132 returns the fixture after checking it still parses to the four
// events received. A lost closing blank line once hid the last one: SSE drops
// an unterminated event at EOF.
func incomplete132(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("testdata/codex_incomplete_132.sse")
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	_ = parseSSE(strings.NewReader(string(b)), func(data []byte) bool {
		var ev codexSSEEvent
		_ = json.Unmarshal(data, &ev)
		types = append(types, ev.Type)
		return true
	})
	if got := strings.Join(types, ","); got != "response.created,response.in_progress,response.metadata,response.incomplete" {
		t.Fatalf("fixture events = %s", got)
	}
	return string(b)
}

const refusal132 = "codex upstream: response incomplete before any output (max_output_tokens)"

// checkErrorEnd asserts the stream ends in one api_error frame whose message
// contains want, after events (the frames before it).
func checkErrorEnd(t *testing.T, frames []anthFrame, events, want string) {
	t.Helper()
	if got := strings.Join(frameEvents(frames), ","); got != events+",error" {
		t.Fatalf("frame events = %s, want %s,error", got, events)
	}
	e, _ := frames[len(frames)-1].data["error"].(map[string]any)
	if e["type"] != "api_error" || !strings.Contains(e["message"].(string), want) {
		t.Fatalf("error frame = %v, want api_error containing %q", e, want)
	}
}

// translate runs a codex script through the translator as translateCodexSSE
// does and returns the stream state its exit classifier reads.
func translate(t *testing.T, script string) (*anthStream, string) {
	t.Helper()
	var out strings.Builder
	a := &anthStream{w: &out}
	_ = parseSSE(strings.NewReader(script), func(data []byte) bool {
		var ev codexSSEEvent
		if json.Unmarshal(data, &ev) != nil {
			return true
		}
		a.handle(ev)
		return !a.ended
	})
	return a, out.String()
}

func TestRepro132TranslatorEndsIncompleteAsError(t *testing.T) {
	checkErrorEnd(t, streamFrames(t, incomplete132(t)), "message_start", refusal132)
}

// The refusal is the backend's answer about this request: row 21, no
// evidence against the member.
func TestRepro132ClassifiesAsRow21(t *testing.T) {
	a, _ := translate(t, incomplete132(t))
	out := classifyCodexExit(a, nil, nil)
	if out.row != "21" || out.sig != sigClient || out.evidence != evNone || out.cause != "upstream_incomplete" {
		t.Fatalf("outcome = %+v, want row 21 client_error, no evidence", out)
	}
}

func TestRepro132StreamingClientGetsError(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()
	h.codexSSE.Store(incomplete132(t))

	resp := h.postStreamSess(t, "claude-frontier", true, "sess-132-stream")
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	checkErrorEnd(t, parseAnthFrames(t, string(b)), "message_start", refusal132)
	h.codexHitOrFail(t)
}

// The class route is the one helpdesk requests take to gpt-6.1-sol.
// With health armed, the refusal writes no sample against the member.
func TestRepro132ClassRouteGetsErrorAndNoEvidence(t *testing.T) {
	for _, stream := range []bool{true, false} {
		t.Run(map[bool]string{true: "stream", false: "non-stream"}[stream], func(t *testing.T) {
			h, restore := classWithUsableGpt(t)
			defer restore()
			hs := newHealthStore(defaultHealthCfg())
			armHealth(t, hs)
			h.codexSSE.Store(incomplete132(t))

			resp := h.postStreamSess(t, "claude-frontier", stream, "sess-repro-132")
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			if stream {
				if resp.StatusCode != 200 {
					t.Fatalf("status %d: %s", resp.StatusCode, b)
				}
				checkErrorEnd(t, parseAnthFrames(t, string(b)), "message_start", refusal132)
			} else if resp.StatusCode != http.StatusInternalServerError || !strings.Contains(string(b), refusal132) {
				t.Fatalf("status %d: %s; want 500 naming the refusal", resp.StatusCode, b)
			}
			h.codexHitOrFail(t)
			if m := hs.renderMetrics(); strings.Contains(m, `outcome="deg"`) {
				t.Fatalf("the refusal scored against the member:\n%s", m)
			}

			// Control: the same harness does score a stream that dies.
			h.codexSSE.Store(created132)
			resp2 := h.postStreamSess(t, "claude-frontier", stream, "sess-repro-132")
			_, _ = io.ReadAll(resp2.Body)
			resp2.Body.Close()
			h.codexHitOrFail(t)
			if m := hs.renderMetrics(); !strings.Contains(m, `outcome="deg"`) {
				t.Fatalf("control: a truncated stream wrote no deg sample, so the check above proves nothing:\n%s", m)
			}
		})
	}
}

// Same bytes, same answer: the non-streaming client gets the error the
// streaming one does, as a status.
func TestRepro132NonStreamingClientGetsSameError(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()
	h.codexSSE.Store(incomplete132(t))

	resp := h.postStreamSess(t, "claude-frontier", false, "sess-132-json")
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var env struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(b, &env)
	if resp.StatusCode != http.StatusInternalServerError || env.Type != "error" ||
		env.Error.Type != "api_error" || env.Error.Message != refusal132 {
		t.Fatalf("status %d: %s; want 500 api_error %q", resp.StatusCode, b, refusal132)
	}
	h.codexHitOrFail(t)
}

// ---- the other incomplete and error endings ----

const created132 = `data: {"type":"response.created","response":{"id":"r","model":"gpt-6.1-sol"}}

`

// An incomplete response that generated text is a real limit: max_tokens, as
// before, and healthy.
func TestIncompleteWithOutputIsMaxTokens(t *testing.T) {
	script := created132 + `data: {"type":"response.output_text.delta","delta":"half"}

data: {"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"usage":{"input_tokens":10,"output_tokens":4}}}

`
	frames := streamFrames(t, script)
	want := "message_start,content_block_start,content_block_delta,content_block_stop,message_delta,message_stop"
	if got := strings.Join(frameEvents(frames), ","); got != want {
		t.Fatalf("frame events = %s, want %s", got, want)
	}
	if d, _ := frames[4].data["delta"].(map[string]any); d["stop_reason"] != "max_tokens" {
		t.Fatalf("stop_reason = %v, want max_tokens", d["stop_reason"])
	}
	a, _ := translate(t, script)
	if out := classifyCodexExit(a, nil, nil); out.row != "5" {
		t.Fatalf("row = %s, want 5", out.row)
	}
	agg := `data: {"type":"response.output_item.done","item":{"type":"message","content":[{"type":"output_text","text":"half"}]}}

data: {"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"usage":{"input_tokens":10,"output_tokens":4}}}

`
	status, body := aggregateCodexSSE(strings.NewReader(agg), 1, nil, nil, nil, nil)
	if status != 200 || !strings.Contains(string(body), `"stop_reason":"max_tokens"`) {
		t.Fatalf("aggregate: %d %s", status, body)
	}
}

// Reasoning spent under a tiny max_tokens is the model working, not a
// refusal: max_tokens with no content, as before.
func TestIncompleteReasoningOnlyIsMaxTokens(t *testing.T) {
	script := created132 + `data: {"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"usage":{"input_tokens":10,"output_tokens":1,"output_tokens_details":{"reasoning_tokens":1}}}}

`
	frames := streamFrames(t, script)
	if got := strings.Join(frameEvents(frames), ","); got != "message_start,message_delta,message_stop" {
		t.Fatalf("frame events = %s", got)
	}
	if d, _ := frames[1].data["delta"].(map[string]any); d["stop_reason"] != "max_tokens" {
		t.Fatalf("stop_reason = %v, want max_tokens", d["stop_reason"])
	}
}

// response.completed with status incomplete and nothing generated is the same
// refusal as the response.incomplete event.
func TestCompletedIncompleteWithNothingIsError(t *testing.T) {
	script := created132 + `data: {"type":"response.completed","response":{"status":"incomplete","usage":{"input_tokens":0,"output_tokens":0}}}

`
	checkErrorEnd(t, streamFrames(t, script), "message_start", "response incomplete before any output (no reason given)")
	if status, _ := aggregateCodexSSE(strings.NewReader(script), 1, nil, nil, nil, nil); status != http.StatusInternalServerError {
		t.Fatalf("aggregate status %d, want 500", status)
	}
}

// content_filter has no stop_reason to map to: an error even after output.
func TestIncompleteContentFilterIsError(t *testing.T) {
	script := created132 + `data: {"type":"response.output_text.delta","delta":"half"}

data: {"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"content_filter"},"usage":{"input_tokens":10,"output_tokens":4}}}

`
	checkErrorEnd(t, streamFrames(t, script), "message_start,content_block_start,content_block_delta", "response incomplete (content_filter)")
	a, _ := translate(t, script)
	if out := classifyCodexExit(a, nil, nil); out.row != "21" {
		t.Fatalf("row = %s, want 21", out.row)
	}
}

// The stream's own error event is the same failure as response.failed.
func TestCodexErrorEventIsUpstreamFailure(t *testing.T) {
	script := created132 + `data: {"type":"error","code":"server_error","message":"kaput","sequence_number":3}

`
	checkErrorEnd(t, streamFrames(t, script), "message_start", "codex upstream: server_error: kaput")
	a, _ := translate(t, script)
	if out := classifyCodexExit(a, nil, nil); out.row != "2" || out.evidence != evDeg {
		t.Fatalf("outcome = %+v, want row 2 deg", out)
	}
	status, body := aggregateCodexSSE(strings.NewReader(script), 1, nil, nil, nil, nil)
	if status != http.StatusBadGateway || !strings.Contains(string(body), "server_error: kaput") {
		t.Fatalf("aggregate: %d %s", status, body)
	}
}

// A stream that started and died before any content used to close as an
// empty end_turn. It is an error, the one the non-stream path gives, and it
// stays row 4a. No usage is published, so the mark stays pending.
func TestTruncatedBeforeContentIsError(t *testing.T) {
	mark := newScaleMark("gpt/gpt-6.1-sol", 0.5)
	var out strings.Builder
	if err := translateCodexSSE(strings.NewReader(created132), &out, 0.5, mark, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	checkErrorEnd(t, parseAnthFrames(t, out.String()), "message_start", "codex upstream: stream ended before response.completed")
	if _, ok := mark.view(); ok {
		t.Fatal("the mark resolved, but no usage was published")
	}
	a, _ := translate(t, created132)
	if row := classifyCodexExit(a, io.ErrUnexpectedEOF, nil).row; row != "4a" {
		t.Fatalf("row = %s, want 4a", row)
	}
}
