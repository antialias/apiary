package main

// Unit tests for the codex wire translation (#33), both directions: Anthropic
// Messages -> codex Responses request bodies, and codex SSE -> Anthropic SSE /
// aggregated-message responses. The canned scripts mirror what the
// Stage A probe observed from the real backend.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// ---- request direction ----

func codexBodyMap(t *testing.T, anth string) (map[string]any, bool) {
	t.Helper()
	enc, stream, _, err := translateAnthropicToCodex([]byte(anth))
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(enc, &m); err != nil {
		t.Fatalf("bad codex body: %v", err)
	}
	return m, stream
}

func TestTranslateRequestFullShape(t *testing.T) {
	anth := `{
	  "model":"gpt-5.6","max_tokens":1000,"temperature":0.7,"top_p":0.9,
	  "stop_sequences":["END"],"system":"Be brief.","stream":true,
	  "messages":[
	    {"role":"user","content":"hello"},
	    {"role":"assistant","content":[
	      {"type":"thinking","thinking":"hmm"},
	      {"type":"text","text":"checking"},
	      {"type":"tool_use","id":"toolu_1","name":"get_weather","input":{"city":"Oslo"}}
	    ]},
	    {"role":"user","content":[
	      {"type":"tool_result","tool_use_id":"toolu_1","content":"sunny"},
	      {"type":"text","text":"and an image"},
	      {"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}
	    ]}
	  ],
	  "tools":[
	    {"name":"get_weather","description":"w","input_schema":{"type":"object"}},
	    {"type":"web_search_20250305","name":"web_search"}
	  ],
	  "tool_choice":{"type":"auto"}
	}`
	m, stream := codexBodyMap(t, anth)
	if !stream {
		t.Fatal("client stream flag lost")
	}
	// Only the probe-validated fields — never the unvalidated Anthropic knobs.
	for _, k := range []string{"max_tokens", "temperature", "top_p", "stop_sequences"} {
		if _, has := m[k]; has {
			t.Fatalf("unvalidated field %q leaked into the codex body", k)
		}
	}
	if m["model"] != "gpt-5.6" || m["stream"] != true || m["store"] != false {
		t.Fatalf("envelope wrong: model=%v stream=%v store=%v", m["model"], m["stream"], m["store"])
	}
	if m["instructions"] != "Be brief." {
		t.Fatalf("instructions = %v", m["instructions"])
	}
	wantInput := []any{
		map[string]any{"role": "user", "content": []any{
			map[string]any{"type": "input_text", "text": "hello"}}},
		map[string]any{"role": "assistant", "content": []any{
			map[string]any{"type": "output_text", "text": "checking"}}},
		map[string]any{"type": "function_call", "name": "get_weather",
			"arguments": `{"city":"Oslo"}`, "call_id": "toolu_1"},
		map[string]any{"type": "function_call_output", "call_id": "toolu_1", "output": "sunny"},
		map[string]any{"role": "user", "content": []any{
			map[string]any{"type": "input_text", "text": "and an image"},
			map[string]any{"type": "input_image", "image_url": "data:image/png;base64,AAAA"}}},
	}
	if !reflect.DeepEqual(m["input"], wantInput) {
		got, _ := json.MarshalIndent(m["input"], "", " ")
		want, _ := json.MarshalIndent(wantInput, "", " ")
		t.Fatalf("input mismatch:\ngot  %s\nwant %s", got, want)
	}
	wantTools := []any{map[string]any{"type": "function", "name": "get_weather",
		"description": "w", "parameters": map[string]any{"type": "object"}, "strict": false}}
	if !reflect.DeepEqual(m["tools"], wantTools) {
		t.Fatalf("tools = %v (builtin must be skipped)", m["tools"])
	}
	if m["tool_choice"] != "auto" {
		t.Fatalf("tool_choice = %v", m["tool_choice"])
	}
}

func TestTranslateRequestSystemBlocksAndErrorResult(t *testing.T) {
	anth := `{
	  "model":"gpt-5.6",
	  "system":[{"type":"text","text":"a"},{"type":"text","text":"b"}],
	  "messages":[{"role":"user","content":[
	    {"type":"tool_result","tool_use_id":"t1","is_error":true,
	     "content":[{"type":"text","text":"boom"}]}
	  ]}]
	}`
	m, stream := codexBodyMap(t, anth)
	if stream {
		t.Fatal("stream must default false")
	}
	if m["instructions"] != "a\nb" {
		t.Fatalf("blocks system = %v", m["instructions"])
	}
	items := m["input"].([]any)
	fco := items[0].(map[string]any)
	if fco["type"] != "function_call_output" || fco["output"] != "Error: boom" {
		t.Fatalf("error tool_result = %v", fco)
	}
	if _, has := m["tools"]; has {
		t.Fatal("no tools in, tools key out")
	}
}

func TestTranslateToolChoiceVariants(t *testing.T) {
	cases := []struct {
		in   string
		want any
	}{
		{`{"type":"auto"}`, "auto"},
		{`{"type":"none"}`, "none"},
		{`{"type":"any"}`, "required"},
		{`{"type":"tool","name":"f"}`, map[string]any{"type": "function", "name": "f"}},
		{`{"type":"weird"}`, nil},
		{``, nil},
	}
	for _, c := range cases {
		got := translateToolChoice(json.RawMessage(c.in))
		if !reflect.DeepEqual(got, c.want) {
			t.Fatalf("tool_choice %q -> %v, want %v", c.in, got, c.want)
		}
	}
}

func TestTranslateRequestBadJSON(t *testing.T) {
	if _, _, _, err := translateAnthropicToCodex([]byte(`{nope`)); err == nil {
		t.Fatal("want an error on a bad body")
	}
}

// The client's effort rides to codex as reasoning.effort (identity map); no
// effort or an effort codex does not accept means no reasoning key at all, so
// the upstream default applies instead of a 400.
func TestTranslateRequestEffortBecomesReasoning(t *testing.T) {
	msgs := `"messages":[{"role":"user","content":"hi"}]`
	cases := []struct {
		name string
		body string
		want any // nil = no reasoning key
	}{
		{"output_config", `{"model":"gpt-6.1-sol","output_config":{"effort":"xhigh"},` + msgs + `}`,
			map[string]any{"effort": "xhigh"}},
		{"top-level", `{"model":"gpt-6.1-sol","effort":"low",` + msgs + `}`,
			map[string]any{"effort": "low"}},
		{"output_config wins", `{"model":"gpt-6.1-sol","effort":"low","output_config":{"effort":"max"},` + msgs + `}`,
			map[string]any{"effort": "max"}},
		{"absent", `{"model":"gpt-6.1-sol",` + msgs + `}`, nil},
		{"unknown", `{"model":"gpt-6.1-sol","output_config":{"effort":"minimal"},` + msgs + `}`, nil},
		{"integer", `{"model":"gpt-6.1-sol","effort":3,` + msgs + `}`, nil},
	}
	for _, c := range cases {
		m, _ := codexBodyMap(t, c.body)
		for _, k := range []string{"effort", "output_config"} {
			if _, leaked := m[k]; leaked {
				t.Fatalf("%s: client field %q leaked into the codex body", c.name, k)
			}
		}
		got, has := m["reasoning"]
		if c.want == nil {
			if has {
				t.Fatalf("%s: reasoning = %v, want no reasoning key", c.name, got)
			}
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Fatalf("%s: reasoning = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestWantsStream(t *testing.T) {
	for body, want := range map[string]bool{
		`{"stream":true}`: true, `{"stream":false}`: false, `{}`: false, `nope`: false,
	} {
		if got := wantsStream([]byte(body)); got != want {
			t.Fatalf("wantsStream(%s) = %v", body, got)
		}
	}
}

// ---- SSE mechanics ----

func TestParseSSEJoinsDataLinesSkipsDone(t *testing.T) {
	in := "event: x\ndata: {\"a\":\ndata: 1}\n\ndata: [DONE]\n\nevent: y\ndata: {\"b\":2}\n\n"
	var got []string
	if err := parseSSE(strings.NewReader(in), func(d []byte) bool {
		got = append(got, string(d))
		return true
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{"{\"a\":\n1}", `{"b":2}`}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// ---- response direction: streaming ----

type anthFrame struct {
	event string
	data  map[string]any
}

func parseAnthFrames(t *testing.T, s string) []anthFrame {
	t.Helper()
	frames := []anthFrame{}
	for _, chunk := range strings.Split(s, "\n\n") {
		if strings.TrimSpace(chunk) == "" {
			continue
		}
		var f anthFrame
		for _, line := range strings.Split(chunk, "\n") {
			if v, ok := strings.CutPrefix(line, "event: "); ok {
				f.event = v
			}
			if v, ok := strings.CutPrefix(line, "data: "); ok {
				if err := json.Unmarshal([]byte(v), &f.data); err != nil {
					t.Fatalf("frame %q: %v", chunk, err)
				}
			}
		}
		frames = append(frames, f)
	}
	return frames
}

func streamFrames(t *testing.T, script string) []anthFrame {
	t.Helper()
	var out strings.Builder
	if err := translateCodexSSE(strings.NewReader(script), &out, 0, nil, nil, nil, nil, nil); err != nil {
		t.Fatalf("translate stream: %v", err)
	}
	return parseAnthFrames(t, out.String())
}

func frameEvents(frames []anthFrame) []string {
	ev := make([]string, len(frames))
	for i, f := range frames {
		ev[i] = f.event
	}
	return ev
}

func TestStreamGoldenText(t *testing.T) {
	frames := streamFrames(t, defaultCodexSSE)
	want := []string{"message_start", "content_block_start", "content_block_delta",
		"content_block_delta", "content_block_stop", "message_delta", "message_stop"}
	if !reflect.DeepEqual(frameEvents(frames), want) {
		t.Fatalf("events = %v, want %v", frameEvents(frames), want)
	}
	msg := frames[0].data["message"].(map[string]any)
	if msg["model"] != "gpt-5.6" || msg["id"] != "resp_1" || msg["role"] != "assistant" {
		t.Fatalf("message_start = %v", msg)
	}
	cb := frames[1].data["content_block"].(map[string]any)
	if cb["type"] != "text" || frames[1].data["index"] != float64(0) {
		t.Fatalf("content_block_start = %v", frames[1].data)
	}
	d1 := frames[2].data["delta"].(map[string]any)
	d2 := frames[3].data["delta"].(map[string]any)
	if d1["type"] != "text_delta" || d1["text"] != "codex " || d2["text"] != "ok" {
		t.Fatalf("text deltas = %v / %v", d1, d2)
	}
	md := frames[5].data
	if md["delta"].(map[string]any)["stop_reason"] != "end_turn" {
		t.Fatalf("stop_reason = %v", md["delta"])
	}
	usage := md["usage"].(map[string]any)
	if usage["input_tokens"] != float64(11) || usage["output_tokens"] != float64(5) {
		t.Fatalf("usage = %v", usage)
	}
}

const codexToolSSE = `event: response.created
data: {"type":"response.created","response":{"id":"resp_2","model":"gpt-5.6"}}

event: response.output_item.added
data: {"type":"response.output_item.added","item":{"type":"function_call","name":"get_weather","call_id":"call_1"}}

event: response.function_call_arguments.delta
data: {"type":"response.function_call_arguments.delta","delta":"{\"city\":"}

event: response.function_call_arguments.delta
data: {"type":"response.function_call_arguments.delta","delta":"\"Oslo\"}"}

event: response.output_item.done
data: {"type":"response.output_item.done","item":{"type":"function_call","name":"get_weather","call_id":"call_1","arguments":"{\"city\":\"Oslo\"}"}}

event: response.completed
data: {"type":"response.completed","response":{"id":"resp_2","model":"gpt-5.6","status":"completed","usage":{"input_tokens":3,"output_tokens":7}}}

`

func TestStreamGoldenToolCall(t *testing.T) {
	frames := streamFrames(t, codexToolSSE)
	want := []string{"message_start", "content_block_start", "content_block_delta",
		"content_block_delta", "content_block_stop", "message_delta", "message_stop"}
	if !reflect.DeepEqual(frameEvents(frames), want) {
		t.Fatalf("events = %v, want %v", frameEvents(frames), want)
	}
	cb := frames[1].data["content_block"].(map[string]any)
	if cb["type"] != "tool_use" || cb["id"] != "call_1" || cb["name"] != "get_weather" {
		t.Fatalf("tool block = %v", cb)
	}
	j1 := frames[2].data["delta"].(map[string]any)
	j2 := frames[3].data["delta"].(map[string]any)
	if j1["type"] != "input_json_delta" || j1["partial_json"].(string)+j2["partial_json"].(string) != `{"city":"Oslo"}` {
		t.Fatalf("json deltas = %v / %v", j1, j2)
	}
	if frames[5].data["delta"].(map[string]any)["stop_reason"] != "tool_use" {
		t.Fatalf("stop_reason = %v", frames[5].data["delta"])
	}
}

// A function_call whose added event lacks the name: the block must still come
// out coherent (start with name/id from the done event, one json delta, stop).
func TestStreamToolCallNamePendingUntilDone(t *testing.T) {
	script := `data: {"type":"response.created","response":{"id":"r","model":"gpt-5.6"}}

data: {"type":"response.output_item.added","item":{"type":"function_call"}}

data: {"type":"response.function_call_arguments.delta","delta":"{\"x\":1}"}

data: {"type":"response.output_item.done","item":{"type":"function_call","name":"late","call_id":"call_9","arguments":"{\"x\":1}"}}

data: {"type":"response.completed","response":{"status":"completed"}}

`
	frames := streamFrames(t, script)
	want := []string{"message_start", "content_block_start", "content_block_delta",
		"content_block_stop", "message_delta", "message_stop"}
	if !reflect.DeepEqual(frameEvents(frames), want) {
		t.Fatalf("events = %v, want %v", frameEvents(frames), want)
	}
	cb := frames[1].data["content_block"].(map[string]any)
	if cb["name"] != "late" || cb["id"] != "call_9" {
		t.Fatalf("late-named tool block = %v", cb)
	}
	if frames[2].data["delta"].(map[string]any)["partial_json"] != `{"x":1}` {
		t.Fatalf("buffered args = %v", frames[2].data["delta"])
	}
}

// Parallel calls from gpt-5.6-sol: each named call opens and closes with no
// argument deltas, the arguments only on the done item. Every block must still
// carry its own arguments (else they all reach the client as `{}`).
func TestStreamParallelToolCallsArgumentsOnDone(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("data: {\"type\":\"response.created\",\"response\":{\"id\":\"r\",\"model\":\"gpt-5.6-sol\"}}\n\n")
	for _, c := range []string{"a", "b", "c"} {
		item := `"type":"function_call","name":"exec","call_id":"call_` + c + `"`
		sb.WriteString(`data: {"type":"response.output_item.added","item":{` + item + "}}\n\n")
		sb.WriteString(`data: {"type":"response.output_item.done","item":{` + item +
			`,"arguments":"{\"command\":\"echo ` + c + `\"}"}}` + "\n\n")
	}
	sb.WriteString("data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n")
	frames := streamFrames(t, sb.String())
	want := []string{"message_start"}
	for range 3 {
		want = append(want, "content_block_start", "content_block_delta", "content_block_stop")
	}
	want = append(want, "message_delta", "message_stop")
	if !reflect.DeepEqual(frameEvents(frames), want) {
		t.Fatalf("events = %v, want %v", frameEvents(frames), want)
	}
	for i, c := range []string{"a", "b", "c"} {
		start, delta := frames[1+3*i].data, frames[2+3*i].data
		if start["index"] != float64(i) || delta["index"] != float64(i) ||
			start["content_block"].(map[string]any)["id"] != "call_"+c {
			t.Fatalf("block %d = %v / %v", i, start, delta)
		}
		if got := delta["delta"].(map[string]any)["partial_json"]; got != `{"command":"echo `+c+`"}` {
			t.Fatalf("block %d args = %v", i, got)
		}
	}
}

// Upstream dies mid-message: the Anthropic stream still terminates in-contract.
func TestStreamUpstreamCutClosesInContract(t *testing.T) {
	script := `data: {"type":"response.created","response":{"id":"r","model":"gpt-5.6"}}

data: {"type":"response.output_text.delta","delta":"half"}

`
	frames := streamFrames(t, script)
	want := []string{"message_start", "content_block_start", "content_block_delta",
		"content_block_stop", "message_delta", "message_stop"}
	if !reflect.DeepEqual(frameEvents(frames), want) {
		t.Fatalf("events = %v, want %v", frameEvents(frames), want)
	}
}

func TestStreamFailedEmitsError(t *testing.T) {
	script := `data: {"type":"response.created","response":{"id":"r","model":"gpt-5.6"}}

data: {"type":"response.failed","response":{"error":{"code":"x","message":"kaput"}}}

`
	frames := streamFrames(t, script)
	last := frames[len(frames)-1]
	if last.event != "error" {
		t.Fatalf("events = %v, want a trailing error frame", frameEvents(frames))
	}
	msg := last.data["error"].(map[string]any)["message"].(string)
	if !strings.Contains(msg, "kaput") {
		t.Fatalf("error message = %q", msg)
	}
}

// ---- response direction: aggregation ----

func TestAggregateGoldenTextAndTool(t *testing.T) {
	script := defaultCodexSSE
	// splice a tool item before completed by concatenating scripts minus the
	// second completed — simpler: build a combined script explicitly.
	script = `data: {"type":"response.created","response":{"id":"resp_3","model":"gpt-5.6"}}

data: {"type":"response.output_text.delta","delta":"ignored by the aggregator"}

data: {"type":"response.output_item.done","item":{"type":"message","content":[{"type":"output_text","text":"codex ok"}]}}

data: {"type":"response.output_item.done","item":{"type":"function_call","name":"get_weather","call_id":"call_1","arguments":"{\"city\":\"Oslo\"}"}}

data: {"type":"response.output_item.done","item":{"type":"reasoning"}}

data: {"type":"response.completed","response":{"id":"resp_3","model":"gpt-5.6","status":"completed","usage":{"input_tokens":11,"output_tokens":5}}}

`
	status, body := aggregateCodexSSE(strings.NewReader(script), 0, nil, nil, nil, nil)
	if status != 200 {
		t.Fatalf("status %d: %s", status, body)
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatal(err)
	}
	if m["id"] != "resp_3" || m["model"] != "gpt-5.6" || m["role"] != "assistant" ||
		m["type"] != "message" || m["stop_reason"] != "tool_use" {
		t.Fatalf("envelope = %v", m)
	}
	wantContent := []any{
		map[string]any{"type": "text", "text": "codex ok"},
		map[string]any{"type": "tool_use", "id": "call_1", "name": "get_weather",
			"input": map[string]any{"city": "Oslo"}},
	}
	if !reflect.DeepEqual(m["content"], wantContent) {
		t.Fatalf("content = %v", m["content"])
	}
	usage := m["usage"].(map[string]any)
	if usage["input_tokens"] != float64(11) || usage["output_tokens"] != float64(5) {
		t.Fatalf("usage = %v", usage)
	}
}

func TestAggregateIncompleteIsMaxTokens(t *testing.T) {
	script := `data: {"type":"response.output_item.done","item":{"type":"message","content":[{"type":"output_text","text":"cut"}]}}

data: {"type":"response.completed","response":{"status":"incomplete"}}

`
	status, body := aggregateCodexSSE(strings.NewReader(script), 0, nil, nil, nil, nil)
	var m map[string]any
	_ = json.Unmarshal(body, &m)
	if status != 200 || m["stop_reason"] != "max_tokens" {
		t.Fatalf("status=%d stop_reason=%v", status, m["stop_reason"])
	}
}

func TestAggregateFailedAndTruncatedAre502(t *testing.T) {
	for name, script := range map[string]string{
		"failed":    "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"kaput\"}}}\n\n",
		"truncated": "data: {\"type\":\"response.created\",\"response\":{}}\n\n",
	} {
		status, body := aggregateCodexSSE(strings.NewReader(script), 0, nil, nil, nil, nil)
		if status != http.StatusBadGateway {
			t.Fatalf("%s: status %d", name, status)
		}
		var m map[string]any
		_ = json.Unmarshal(body, &m)
		if m["type"] != "error" {
			t.Fatalf("%s: body = %s", name, body)
		}
	}
}

// ---- response wrapper + error mapping ----

func TestCodexErrType(t *testing.T) {
	for status, want := range map[int]string{
		429: "rate_limit_error", 401: "authentication_error", 403: "authentication_error",
		400: "invalid_request_error", 404: "invalid_request_error",
		500: "api_error", 502: "api_error",
	} {
		if got := codexErrType(status); got != want {
			t.Fatalf("codexErrType(%d) = %q, want %q", status, got, want)
		}
	}
}

func TestTranslateCodexResponseNon200(t *testing.T) {
	hdr := http.Header{}
	hdr.Set("Retry-After", "17")
	hdr.Set("Set-Cookie", "cf=secret")
	resp := &http.Response{StatusCode: 429, Header: hdr,
		Body: httpBody(`{"detail":"rate limited"}`)}
	out := translateCodexResponse(resp, false, "gpt", 0, nil, nil, nil, nil, nil)
	if out.StatusCode != 429 || out.Header.Get("Retry-After") != "17" {
		t.Fatalf("status/retry-after = %d/%q", out.StatusCode, out.Header.Get("Retry-After"))
	}
	if out.Header.Get("Set-Cookie") != "" {
		t.Fatal("upstream cookie leaked to the client")
	}
	var m map[string]any
	_ = json.NewDecoder(out.Body).Decode(&m)
	e := m["error"].(map[string]any)
	if e["type"] != "rate_limit_error" || !strings.Contains(e["message"].(string), "rate limited") {
		t.Fatalf("envelope = %v", m)
	}
}

// The error envelope is the ONLY place the client learns which backend failed.
// A class request can cross members mid-flight, so an untagged message leaves
// "which member 429'd?" unanswerable from the response alone.
func TestCodexErrorEnvelopeNamesTheServedMember(t *testing.T) {
	errMsg := func(member string) string {
		resp := &http.Response{StatusCode: 429, Header: http.Header{},
			Body: httpBody(`{"detail":"rate limited"}`)}
		var m map[string]any
		_ = json.NewDecoder(translateCodexResponse(resp, false, member, 0, nil, nil, nil, nil, nil).Body).Decode(&m)
		return m["error"].(map[string]any)["message"].(string)
	}
	if got := errMsg("gpt/gpt-5.6"); !strings.HasPrefix(got, "[gpt/gpt-5.6] codex upstream HTTP 429: ") {
		t.Fatalf("member not named: %q", got)
	}
	// Empty member omits the tag entirely rather than emitting "[] " — a caller
	// with nothing truthful to say must not look like it named a backend.
	got := errMsg("")
	if !strings.HasPrefix(got, "codex upstream HTTP 429: ") || strings.Contains(got, "[") {
		t.Fatalf("empty member must omit the tag, got %q", got)
	}
}

func httpBody(s string) *nopBody { return &nopBody{Reader: strings.NewReader(s)} }

type nopBody struct{ *strings.Reader }

func (n *nopBody) Close() error { return nil }

// ---- dispensed-bearer cache ----

func TestGetCodexTokenCacheStaleAndFail(t *testing.T) {
	var dispenses atomic.Int64
	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != tpath("/openai-token") || r.Header.Get("X-Switch-Key") != "unit-key" {
			w.WriteHeader(401)
			return
		}
		n := dispenses.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"account_id":   "acct-unit",
			"access_token": "tok-" + string(rune('0'+n)),
			"expires_at":   float64(time.Now().Add(time.Hour).Unix()),
		})
	}))

	oldBrain, oldKey := brainBase, switchKey
	lan.codexTokMu.Lock()
	oldToks := lan.codexToks
	lan.codexToks = map[string]codexTokEntry{}
	lan.codexTokMu.Unlock()
	brainBase, switchKey = brain.URL, "unit-key"
	defer func() {
		brain.Close()
		brainBase, switchKey = oldBrain, oldKey
		lan.codexTokMu.Lock()
		lan.codexToks = oldToks
		lan.codexTokMu.Unlock()
	}()

	tok, acct, _, ok := lan.getCodexToken("")
	if !ok || tok != "tok-1" || acct != "acct-unit" {
		t.Fatalf("first fetch = %q/%q/%v", tok, acct, ok)
	}
	if tok2, _, _, _ := lan.getCodexToken(""); tok2 != "tok-1" || dispenses.Load() != 1 {
		t.Fatalf("second call must ride the fresh cache (tok=%q dispenses=%d)", tok2, dispenses.Load())
	}

	// Brain dies + cache aged past codexTokenFresh but bearer unexpired: STALE ride.
	brain.Close()
	lan.codexTokMu.Lock()
	e := lan.codexToks[""]
	e.fetched = time.Now().Add(-2 * codexTokenFresh)
	lan.codexToks[""] = e
	lan.codexTokMu.Unlock()
	if tok3, _, _, ok3 := lan.getCodexToken(""); !ok3 || tok3 != "tok-1" {
		t.Fatalf("stale ride = %q/%v, want the cached bearer", tok3, ok3)
	}

	// Nothing cached + brain dead: refuse.
	lan.dropCodexToken("")
	if _, _, _, ok4 := lan.getCodexToken(""); ok4 {
		t.Fatal("no cache + dead brain must refuse")
	}
}

// #71: each pool account caches its bearer independently, the ?account= param
// reaches the brain, the brain's `name` labels the entry, and dropping one
// account's bearer leaves its siblings untouched.
func TestGetCodexTokenPerAccountPool(t *testing.T) {
	var dispenses atomic.Int64
	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != tpath("/openai-token") || r.Header.Get("X-Switch-Key") != "unit-key" {
			w.WriteHeader(401)
			return
		}
		dispenses.Add(1)
		name := r.URL.Query().Get("account")
		if name == "" {
			name = "gpt-a" // the brain resolves its pool incumbent
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"account_id":   "uuid-" + name,
			"access_token": "tok-" + name,
			"expires_at":   float64(time.Now().Add(time.Hour).Unix()),
			"name":         name,
		})
	}))
	defer brain.Close()

	oldBrain, oldKey := brainBase, switchKey
	lan.codexTokMu.Lock()
	oldToks := lan.codexToks
	lan.codexToks = map[string]codexTokEntry{}
	lan.codexTokMu.Unlock()
	brainBase, switchKey = brain.URL, "unit-key"
	defer func() {
		brainBase, switchKey = oldBrain, oldKey
		lan.codexTokMu.Lock()
		lan.codexToks = oldToks
		lan.codexTokMu.Unlock()
	}()

	tok, acct, name, ok := lan.getCodexToken("gpt-b")
	if !ok || tok != "tok-gpt-b" || acct != "uuid-gpt-b" || name != "gpt-b" {
		t.Fatalf("named fetch = %q/%q/%q/%v", tok, acct, name, ok)
	}
	tok, _, name, ok = lan.getCodexToken("")
	if !ok || tok != "tok-gpt-a" || name != "gpt-a" {
		t.Fatalf("incumbent fetch = %q name=%q ok=%v — the brain resolves the pool pick", tok, name, ok)
	}
	if dispenses.Load() != 2 {
		t.Fatalf("dispenses = %d, want 2 (separate cache slots)", dispenses.Load())
	}
	if tok2, _, _, _ := lan.getCodexToken("gpt-b"); tok2 != "tok-gpt-b" || dispenses.Load() != 2 {
		t.Fatalf("named re-fetch must ride its own cache slot (tok=%q dispenses=%d)", tok2, dispenses.Load())
	}
	// Dropping gpt-b must not evict the incumbent slot.
	lan.dropCodexToken("gpt-b")
	if tok3, _, _, _ := lan.getCodexToken(""); tok3 != "tok-gpt-a" || dispenses.Load() != 2 {
		t.Fatalf("sibling drop evicted the incumbent slot (tok=%q dispenses=%d)", tok3, dispenses.Load())
	}
	if tok4, _, _, ok4 := lan.getCodexToken("gpt-b"); !ok4 || tok4 != "tok-gpt-b" || dispenses.Load() != 3 {
		t.Fatalf("dropped slot must re-fetch (tok=%q dispenses=%d ok=%v)", tok4, dispenses.Load(), ok4)
	}
}

// ---- the compaction numerator (#44 Phase 1) ----
//
// Everything below pins ONE contract: what a harness can compute from the usage
// block we synthesize. "No cache" and "I don't know" are different claims, and
// an earlier version made the second one by accident on every gpt-served turn.
//
// The fix for that asserted a hard zero, on the belief that codex had no prompt
// cache to report. It does — see the subset/disjoint tests further down, added
// when the zeros turned out to be an assumption rather than a
// measurement. Both claims still have to be explicit on the wire; what changed
// is that they now have to be true as well.

// gatewayContextUsageAvailable replicates the agent gateway's message_delta guard
// verbatim. Its contextUsage is 'available' only when all THREE fields are
// present; absent means unknown, not zero. Modelled here rather than asserted
// field-by-field so the test states the rule it is actually defending, and so
// the reason survives someone "tidying" the assertions.
func gatewayContextUsageAvailable(usage map[string]any) bool {
	for _, k := range []string{"input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"} {
		if _, ok := usage[k]; !ok {
			return false
		}
	}
	return true
}

// gatewayPromptTokens is how the agent gateway sums the numerator once it is available.
func gatewayPromptTokens(usage map[string]any) float64 {
	var n float64
	for _, k := range []string{"input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"} {
		v, _ := usage[k].(float64)
		n += v
	}
	return n
}

func TestStreamUsageIsCountableByAgentGateway(t *testing.T) {
	// THE PHASE 1 GATE, in unit form. Before the fix this stream produced
	// {"input_tokens":11,"output_tokens":5} with no cache fields, the agent gateway read
	// contextUsage.state === 'unavailable', and the compaction gate ran on a
	// persisted projection instead of the turn it had just been handed.
	frames := streamFrames(t, defaultCodexSSE)
	usage := frames[5].data["usage"].(map[string]any)
	if !gatewayContextUsageAvailable(usage) {
		t.Fatalf("contextUsage would be 'unavailable' — usage = %v", usage)
	}
	if got := gatewayPromptTokens(usage); got != 11 {
		t.Fatalf("promptTokens = %v, want 11 (the cache zeros must not add)", got)
	}
	if usage["cache_read_input_tokens"] != float64(0) || usage["cache_creation_input_tokens"] != float64(0) {
		t.Fatalf("cache fields must be explicit ZERO, not merely present: %v", usage)
	}
}

func TestStreamMessageStartStaysUnseeded(t *testing.T) {
	// message_start is emitted at first byte, BEFORE codex has reported usage.
	// It must not claim a prompt size. the agent gateway seeds contextUsage from
	// message_start only when input_tokens is non-zero, so a bare 0 correctly
	// leaves the snapshot undefined and lets message_delta carry the turn —
	// which is the whole reason we never need a pre-dispatch estimate.
	frames := streamFrames(t, defaultCodexSSE)
	msg := frames[0].data["message"].(map[string]any)
	usage := msg["usage"].(map[string]any)
	if usage["input_tokens"] != float64(0) {
		t.Fatalf("message_start must not claim a prompt size: %v", usage)
	}
	if gatewayContextUsageAvailable(usage) {
		t.Fatalf("message_start must NOT look seedable — it has no real counts yet: %v", usage)
	}
}

func TestStreamTruncatedOmitsCacheFieldsToSignalUnknown(t *testing.T) {
	// The deliberate inverse of the fix. The stream died before
	// response.completed, so we never learned the counts. Explicit zeros here
	// would assert "the prompt was empty" and reset the harness's idea of
	// transcript size to nothing after any truncated turn; omitting the cache
	// fields makes the block read as UNKNOWN, so the agent gateway keeps its projection.
	script := `data: {"type":"response.created","response":{"id":"r","model":"gpt-5.6"}}

data: {"type":"response.output_text.delta","delta":"half"}

`
	frames := streamFrames(t, script)
	usage := frames[4].data["usage"].(map[string]any)
	if gatewayContextUsageAvailable(usage) {
		t.Fatalf("a truncated turn must read as UNKNOWN, not as an empty transcript: %v", usage)
	}
}

func TestReasoningTokensAreNotTranscriptOutput(t *testing.T) {
	// Reasoning tokens are billed under output_tokens but are discarded by codex
	// and never re-sent, so they are not in the transcript the next request
	// replays. Claude Code's numerator is input + cache_creation + cache_read +
	// output, so counting them inflates fill on every gpt-served frontier turn.
	script := `data: {"type":"response.created","response":{"id":"r","model":"gpt-5.6"}}

data: {"type":"response.output_text.delta","delta":"hi"}

data: {"type":"response.completed","response":{"id":"r","model":"gpt-5.6","status":"completed","usage":{"input_tokens":100,"output_tokens":80,"output_tokens_details":{"reasoning_tokens":60}}}}

`
	frames := streamFrames(t, script)
	usage := frames[len(frames)-2].data["usage"].(map[string]any)
	if usage["output_tokens"] != float64(20) {
		t.Fatalf("output_tokens = %v, want 20 (80 billed - 60 reasoning)", usage["output_tokens"])
	}
	if usage["input_tokens"] != float64(100) {
		t.Fatalf("input_tokens must be untouched by the reasoning subtraction: %v", usage)
	}
}

func TestReasoningSubtractionClampsAtZero(t *testing.T) {
	// A details block larger than the total is nonsense, but a NEGATIVE token
	// count would corrupt a harness numerator far worse than an unsubtracted one.
	u := codexUsage(&codexRespObj{Usage: &codexUsageObj{
		InputTokens: 7, OutputTokens: 5,
		OutputTokensDetails: &struct {
			ReasoningTokens int `json:"reasoning_tokens"`
		}{ReasoningTokens: 999},
	}})
	if u.in != 7 || u.out != 0 {
		t.Fatalf("codexUsage = (in %d, out %d), want (7, 0)", u.in, u.out)
	}
}

// ---- OpenAI subsets vs Anthropic disjoint sets (the cache fix) ----
//
// OpenAI's input_tokens is the TOTAL prompt and input_tokens_details.cached_tokens
// / .cache_write_tokens are SUBSETS of it. Anthropic's input_tokens is the
// UNCACHED REMAINDER, disjoint from its two cache fields — which is the only
// reason a harness may sum all three. Read one schema as the other and every
// cached token is counted twice.

// codexDetails builds an input_tokens_details block without restating its shape.
func codexDetails(cached, written int) *codexInputDetails {
	return &codexInputDetails{CachedTokens: cached, CacheWriteTokens: &written}
}

func TestCacheSubsetsBecomeDisjointCounts(t *testing.T) {
	// The worked example straight out of OpenAI's caching guide: of 2,600 input
	// tokens, 2,000 were read from cache and 400 newly written, leaving 200 that
	// were neither. Anthropic-side that is input=200, read=2000, creation=400 —
	// NOT input=2600, which is what passing the upstream total through would say.
	u := codexUsage(&codexRespObj{Usage: &codexUsageObj{
		InputTokens: 2600, OutputTokens: 9,
		InputTokensDetails: codexDetails(2000, 400),
	}})
	if u.in != 200 || u.cacheRead != 2000 || u.cacheWrite != 400 {
		t.Fatalf("got in=%d read=%d write=%d, want 200/2000/400", u.in, u.cacheRead, u.cacheWrite)
	}
	if !u.cacheReported {
		t.Fatal("a response carrying input_tokens_details reported its cache")
	}
	// The harness numerator must still describe a 2,600-token prompt.
	if got := gatewayPromptTokens(anthUsageAsAny(u)); got != 2600 {
		t.Fatalf("promptTokens = %v, want 2600 — the cached prefix was counted twice", got)
	}
}

func TestCacheCountsAlwaysSumToTheUpstreamTotal(t *testing.T) {
	// The invariant that makes a malformed details block survivable: it may
	// misattribute the prompt between buckets, but it must never change how big
	// the prompt is, because the size is what compaction turns on.
	for _, tc := range []struct{ total, cached, written int }{
		{2600, 2000, 400}, // well-formed
		{100, 0, 0},       // block present, nothing cached
		{100, 999, 0},     // cached alone exceeds the total
		{100, 80, 999},    // write overruns what cached left behind
		{100, -5, -5},     // negative counts
		{0, 50, 50},       // an empty prompt that claims a cache
	} {
		u := codexUsage(&codexRespObj{Usage: &codexUsageObj{
			InputTokens: tc.total, InputTokensDetails: codexDetails(tc.cached, tc.written),
		}})
		if sum := u.in + u.cacheRead + u.cacheWrite; sum != tc.total {
			t.Fatalf("total=%d cached=%d written=%d: parts sum to %d (in=%d read=%d write=%d)",
				tc.total, tc.cached, tc.written, sum, u.in, u.cacheRead, u.cacheWrite)
		}
		if u.in < 0 || u.cacheRead < 0 || u.cacheWrite < 0 {
			t.Fatalf("negative count from total=%d cached=%d written=%d: %+v", tc.total, tc.cached, tc.written, u)
		}
	}
}

func TestAbsentDetailsBlockStaysUnreported(t *testing.T) {
	// No details block means the upstream said nothing about caching. The CLIENT
	// still gets explicit zeros — an absent field makes the turn uncountable —
	// but cacheReported stays false so the scaleMark does not record a zero the
	// upstream never sent.
	u := codexUsage(&codexRespObj{Usage: &codexUsageObj{InputTokens: 11, OutputTokens: 5}})
	if u.in != 11 || u.cacheRead != 0 || u.cacheWrite != 0 {
		t.Fatalf("got %+v, want the whole prompt uncached", u)
	}
	if u.cacheReported {
		t.Fatal("cacheReported must be false when no details block arrived")
	}
	if !gatewayContextUsageAvailable(anthUsageAsAny(u)) {
		t.Fatal("the wire must still assert zeros, or the turn reads 'unavailable'")
	}
}

func TestRatioScalesEveryInputSideCount(t *testing.T) {
	// #44 divides the INPUT side by the member's declared ratio. Scaling only
	// input_tokens would leave ~93% of a codex prompt — the cached part — in
	// upstream units, which is the same undercount that made compaction fire late
	// on frontier, just hidden in a different field.
	u := codexUsage(&codexRespObj{Usage: &codexUsageObj{
		InputTokens: 2600, OutputTokens: 100,
		InputTokensDetails: codexDetails(2000, 400),
	}}).scaledInput(0.5)
	if u.in != 400 || u.cacheRead != 4000 || u.cacheWrite != 800 {
		t.Fatalf("got in=%d read=%d write=%d, want 400/4000/800", u.in, u.cacheRead, u.cacheWrite)
	}
	if u.out != 100 {
		t.Fatalf("output_tokens = %d, want 100 — output stays in upstream units", u.out)
	}
}

func TestStreamCarriesReportedCacheCounts(t *testing.T) {
	// End to end through the SSE translator: a codex turn that reports a cache
	// must reach the client as a cache, not as a hard zero.
	script := `data: {"type":"response.created","response":{"id":"r","model":"gpt-5.6"}}

data: {"type":"response.output_text.delta","delta":"hi"}

data: {"type":"response.completed","response":{"id":"r","model":"gpt-5.6","status":"completed","usage":{"input_tokens":2600,"output_tokens":9,"input_tokens_details":{"cached_tokens":2000,"cache_write_tokens":400}}}}

`
	frames := streamFrames(t, script)
	usage := frames[len(frames)-2].data["usage"].(map[string]any)
	if usage["cache_read_input_tokens"] != float64(2000) || usage["cache_creation_input_tokens"] != float64(400) {
		t.Fatalf("cache counts did not survive translation: %v", usage)
	}
	if usage["input_tokens"] != float64(200) {
		t.Fatalf("input_tokens = %v, want 200 (the uncached remainder)", usage["input_tokens"])
	}
	if got := gatewayPromptTokens(usage); got != 2600 {
		t.Fatalf("promptTokens = %v, want 2600", got)
	}
}

// anthUsageAsAny renders a usage block in the map[string]any shape the agent gateway
// helpers above read, matching what JSON round-tripping produces on the wire.
func anthUsageAsAny(t codexTokens) map[string]any {
	m := map[string]any{}
	for k, v := range anthUsage(t) {
		m[k] = float64(v)
	}
	return m
}

func TestAggregateUsageIsCountableByAgentGateway(t *testing.T) {
	// Same contract on the non-streaming path — a client that did not ask to
	// stream still gets a usage block a harness can add up.
	status, body := aggregateCodexSSE(strings.NewReader(defaultCodexSSE), 0, nil, nil, nil, nil)
	if status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	usage := m["usage"].(map[string]any)
	if !gatewayContextUsageAvailable(usage) {
		t.Fatalf("aggregated usage would read 'unavailable': %v", usage)
	}
	if got := gatewayPromptTokens(usage); got != 11 {
		t.Fatalf("promptTokens = %v, want 11", got)
	}
}
