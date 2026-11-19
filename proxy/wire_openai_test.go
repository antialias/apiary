package main

// Unit tests for the OpenAI inbound wire (#96): the normalizer's dialect
// mapping and refusals, the tool-name sanitizer, the per-class token bucket,
// and the renderer's error mapping. The full door-to-upstream contract lives
// in e2e_openai_test.go.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSanitizeToolName(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"quiz", "quiz"},
		{"My Answer!", "My_Answer_"},
		{"a.b/c", "a_b_c"},
		{"", "structured_output"},
		{"日本語", "___"},
		{strings.Repeat("x", 80), strings.Repeat("x", 64)},
	} {
		if got := sanitizeToolName(tc.in); got != tc.want {
			t.Errorf("sanitizeToolName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestOAContentText(t *testing.T) {
	for _, tc := range []struct {
		name, in string
		want     string
		ok       bool
	}{
		{"string", `"hello"`, "hello", true},
		{"null", `null`, "", true},
		{"empty raw", ``, "", true},
		{"parts", `[{"type":"text","text":"a"},{"type":"text","text":"b"}]`, "ab", true},
		{"image part", `[{"type":"image_url","image_url":{"url":"x"}}]`, "", false},
		{"number", `42`, "", false},
	} {
		got, ok := oaContentText(json.RawMessage(tc.in))
		if got != tc.want || ok != tc.ok {
			t.Errorf("%s: oaContentText(%s) = (%q, %v), want (%q, %v)", tc.name, tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

func TestOAContentBlocks(t *testing.T) {
	text := func(s string) map[string]any { return map[string]any{"type": "text", "text": s} }
	b64 := func(mediaType, data string) map[string]any {
		return map[string]any{"type": "image", "source": map[string]any{
			"type": "base64", "media_type": mediaType, "data": data}}
	}
	url := func(u string) map[string]any {
		return map[string]any{"type": "image", "source": map[string]any{"type": "url", "url": u}}
	}
	for _, tc := range []struct {
		name, in string
		want     []map[string]any
		ok       bool
	}{
		{"string", `"hello"`, []map[string]any{text("hello")}, true},
		{"null", `null`, nil, true},
		{"empty raw", ``, nil, true},
		{"text parts", `[{"type":"text","text":"a"},{"type":"text","text":"b"}]`,
			[]map[string]any{text("a"), text("b")}, true},
		{"jpeg data url", `[{"type":"image_url","image_url":{"url":"data:image/jpeg;base64,QUJD"}}]`,
			[]map[string]any{b64("image/jpeg", "QUJD")}, true},
		{"png data url", `[{"type":"image_url","image_url":{"url":"data:image/png;base64,iVBORw0KGgo="}}]`,
			[]map[string]any{b64("image/png", "iVBORw0KGgo=")}, true},
		{"detail ignored", `[{"type":"image_url","image_url":{"url":"data:image/png;base64,AA","detail":"high"}}]`,
			[]map[string]any{b64("image/png", "AA")}, true},
		{"http url", `[{"type":"image_url","image_url":{"url":"http://ref.test/a.png"}}]`,
			[]map[string]any{url("http://ref.test/a.png")}, true},
		{"https url", `[{"type":"image_url","image_url":{"url":"https://ref.test/a.png"}}]`,
			[]map[string]any{url("https://ref.test/a.png")}, true},
		{"bare string image_url", `[{"type":"image_url","image_url":"data:image/webp;base64,AA"}]`,
			[]map[string]any{b64("image/webp", "AA")}, true},
		{"interleaved", `[{"type":"text","text":"look"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AA"}},{"type":"text","text":"?"}]`,
			[]map[string]any{text("look"), b64("image/png", "AA"), text("?")}, true},
		{"data url with extra params", `[{"type":"image_url","image_url":{"url":"data:image/jpeg;charset=utf-8;base64,QUJD"}}]`,
			[]map[string]any{b64("image/jpeg", "QUJD")}, true},
		{"uppercase data url and token", `[{"type":"image_url","image_url":{"url":"DATA:IMAGE/PNG;BASE64,AA"}}]`,
			[]map[string]any{b64("image/png", "AA")}, true},
		{"surrounding whitespace", `[{"type":"image_url","image_url":{"url":"  data:image/png;base64,AA  "}}]`,
			[]map[string]any{b64("image/png", "AA")}, true},
		{"non-image media type", `[{"type":"image_url","image_url":{"url":"data:text/plain;base64,QUJD"}}]`, nil, false},
		{"bare image/ media type", `[{"type":"image_url","image_url":{"url":"data:image/;base64,AA"}}]`, nil, false},
		{"base64 not the last param", `[{"type":"image_url","image_url":{"url":"data:image/png;base64;charset=utf-8,AA"}}]`, nil, false},
		{"data url without base64", `[{"type":"image_url","image_url":{"url":"data:image/png,AAAA"}}]`, nil, false},
		{"data url with empty payload", `[{"type":"image_url","image_url":{"url":"data:image/png;base64,"}}]`, nil, false},
		{"data url without media type", `[{"type":"image_url","image_url":{"url":"data:;base64,AA"}}]`, nil, false},
		{"other scheme", `[{"type":"image_url","image_url":{"url":"file:///etc/passwd"}}]`, nil, false},
		{"missing url", `[{"type":"image_url"}]`, nil, false},
		{"empty url", `[{"type":"image_url","image_url":{"url":""}}]`, nil, false},
		{"input_audio", `[{"type":"input_audio","input_audio":{"data":"AA","format":"wav"}}]`, nil, false},
		{"file part", `[{"type":"file","file":{"file_id":"f1"}}]`, nil, false},
		{"unknown type", `[{"type":"video_url","video_url":{"url":"https://ref.test/a.mp4"}}]`, nil, false},
		{"number", `42`, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := oaContentBlocks(json.RawMessage(tc.in))
			if ok != tc.ok {
				t.Fatalf("oaContentBlocks(%s) ok = %v, want %v", tc.in, ok, tc.ok)
			}
			if !ok {
				return
			}
			if len(got) != len(tc.want) || !reflect.DeepEqual(got, tc.want) {
				t.Errorf("oaContentBlocks(%s) = %+v, want %+v", tc.in, got, tc.want)
			}
		})
	}
}

func TestWireTake(t *testing.T) {
	oldRPM, oldBurst, oldNow := wireRPM, wireBurst, wireNow
	reset := func() {
		lan.wireMu.Lock()
		lan.wireBuckets = map[string]*wireBucket{}
		lan.wireMu.Unlock()
	}
	defer func() {
		wireRPM, wireBurst, wireNow = oldRPM, oldBurst, oldNow
		reset()
	}()
	reset()
	now := time.Unix(1_000_000, 0)
	wireNow = func() time.Time { return now }
	wireRPM, wireBurst = 60, 2 // 1 token/sec refill, depth 2

	if !lan.wireTake("tutor") || !lan.wireTake("tutor") {
		t.Fatal("burst of 2 should allow two takes")
	}
	if lan.wireTake("tutor") {
		t.Fatal("third take should be denied")
	}
	if !lan.wireTake("other") {
		t.Fatal("classes have independent buckets")
	}
	now = now.Add(1500 * time.Millisecond)
	if !lan.wireTake("tutor") {
		t.Fatal("refill should allow a take")
	}
	if lan.wireTake("tutor") {
		t.Fatal("only ~half a token remains after the refill take")
	}
	wireRPM = 0
	if !lan.wireTake("tutor") {
		t.Fatal("rpm<=0 disables the limiter")
	}
}

// withWireClassRows installs a lone class row (claude-frontier) for the
// normalizer to resolve against, restoring the prior snapshot on cleanup.
func withWireClassRows(t *testing.T) {
	t.Helper()
	oldRows, oldSrc := lan.classRowsSnapshot()
	lan.setClassRows([]registryEntry{{Name: "frontier", Display: "Frontier", Type: "class", Class: "frontier"}}, "test")
	t.Cleanup(func() { lan.setClassRows(oldRows, oldSrc) })
}

func TestNormalizeChatRequestHappyPath(t *testing.T) {
	withWireClassRows(t)
	norm, werr := normalizeChatRequest(lan, []byte(`{
		"model": "frontier",
		"messages": [
			{"role":"system","content":"sys one"},
			{"role":"user","content":"hello"},
			{"role":"user","content":[{"type":"text","text":" again"}]},
			{"role":"assistant","content":"hi"},
			{"role":"developer","content":"dev note"},
			{"role":"user","content":"question"}
		],
		"max_completion_tokens": 512,
		"max_tokens": 9,
		"temperature": 1.7,
		"stop": "END",
		"response_format": {"type":"json_schema","json_schema":{
			"name":"My Answer!","description":"d",
			"schema":{"type":"object","properties":{"x":{"type":"integer"}},"required":["x"]}}}
	}`))
	if werr != nil {
		t.Fatalf("unexpected refusal: %+v", werr)
	}
	if norm.requestedModel != "frontier" {
		t.Errorf("requestedModel = %q, want frontier (verbatim echo)", norm.requestedModel)
	}
	if norm.class.renderedID() != "claude-frontier" {
		t.Errorf("class = %q, want claude-frontier (bare alias resolution)", norm.class.renderedID())
	}
	if norm.syntheticTool != "My_Answer_" {
		t.Errorf("syntheticTool = %q, want My_Answer_", norm.syntheticTool)
	}
	var ab struct {
		Model    string `json:"model"`
		System   string `json:"system"`
		Messages []struct{ Role, Content string }
		MaxTok   int      `json:"max_tokens"`
		Temp     float64  `json:"temperature"`
		Stop     []string `json:"stop_sequences"`
		Tools    []struct {
			Name        string          `json:"name"`
			InputSchema json.RawMessage `json:"input_schema"`
		} `json:"tools"`
		ToolChoice struct{ Type, Name string } `json:"tool_choice"`
		Thinking   struct{ Type string }       `json:"thinking"`
	}
	if err := json.Unmarshal(norm.anthropicBody, &ab); err != nil {
		t.Fatalf("anthropic body not JSON: %v", err)
	}
	if ab.Model != "claude-frontier" {
		t.Errorf("outbound model = %q, want claude-frontier", ab.Model)
	}
	if ab.System != "sys one\n\ndev note" {
		t.Errorf("system = %q (system+developer roles should join)", ab.System)
	}
	wantMsgs := []struct{ Role, Content string }{
		{"user", "hello\n\n again"}, {"assistant", "hi"}, {"user", "question"},
	}
	if len(ab.Messages) != len(wantMsgs) {
		t.Fatalf("messages = %+v, want %+v", ab.Messages, wantMsgs)
	}
	for i, w := range wantMsgs {
		if ab.Messages[i] != w {
			t.Errorf("messages[%d] = %+v, want %+v", i, ab.Messages[i], w)
		}
	}
	if ab.MaxTok != 512 {
		t.Errorf("max_tokens = %d, want 512 (max_completion_tokens wins)", ab.MaxTok)
	}
	if ab.Temp != 1.0 {
		t.Errorf("temperature = %v, want clamped 1.0", ab.Temp)
	}
	if len(ab.Stop) != 1 || ab.Stop[0] != "END" {
		t.Errorf("stop_sequences = %v, want [END]", ab.Stop)
	}
	if len(ab.Tools) != 1 || ab.Tools[0].Name != "My_Answer_" || len(ab.Tools[0].InputSchema) == 0 {
		t.Errorf("tools = %+v, want one synthetic tool carrying the schema", ab.Tools)
	}
	if ab.ToolChoice.Type != "tool" || ab.ToolChoice.Name != "My_Answer_" {
		t.Errorf("tool_choice = %+v, want forced tool", ab.ToolChoice)
	}
	if ab.Thinking.Type != "disabled" {
		t.Errorf("thinking = %+v, want disabled (D4)", ab.Thinking)
	}
}

func TestNormalizeChatRequestRefusals(t *testing.T) {
	withWireClassRows(t)
	msgs := `[{"role":"user","content":"hi"}]`
	img := `[{"type":"image_url","image_url":{"url":"data:image/png;base64,AA"}}]`
	for _, tc := range []struct {
		name, body string
		status     int
		code       string
		wantMsg    string // substring, when the wording itself is the contract
	}{
		{"not json", `[]`, 400, "invalid_json", ""},
		{"no model", `{"messages":` + msgs + `}`, 400, "invalid_model", ""},
		{"former named provider id", `{"model":"claude-kimi-k3","messages":` + msgs + `}`, 404, "model_not_found", ""},
		{"anthropic model id", `{"model":"claude-fable-5","messages":` + msgs + `}`, 404, "model_not_found", ""},
		{"garbage model", `{"model":"gpt-4o","messages":` + msgs + `}`, 404, "model_not_found", ""},
		{"stream", `{"model":"frontier","stream":true,"messages":` + msgs + `}`, 400, "unsupported_value", ""},
		{"n>1", `{"model":"frontier","n":2,"messages":` + msgs + `}`, 400, "unsupported_value", ""},
		{"client tools", `{"model":"frontier","tools":[{"type":"function"}],"messages":` + msgs + `}`, 400, "unsupported_value", ""},
		{"tool_choice", `{"model":"frontier","tool_choice":"auto","messages":` + msgs + `}`, 400, "unsupported_value", ""},
		{"no messages", `{"model":"frontier"}`, 400, "invalid_messages", ""},
		{"only system", `{"model":"frontier","messages":[{"role":"system","content":"x"}]}`, 400, "invalid_messages", ""},
		{"tool role", `{"model":"frontier","messages":[{"role":"tool","content":"x"}]}`, 400, "unsupported_value", ""},
		{"unknown role", `{"model":"frontier","messages":[{"role":"wizard","content":"x"}]}`, 400, "unsupported_value", ""},
		{"image content", `{"model":"frontier","messages":[{"role":"user","content":[{"type":"image_url"}]}]}`, 400, "unsupported_content", ""},
		{"image in system", `{"model":"frontier","messages":[{"role":"system","content":` + img + `},{"role":"user","content":"hi"}]}`,
			400, "unsupported_content", "images are only supported in user messages"},
		{"image in developer", `{"model":"frontier","messages":[{"role":"developer","content":` + img + `},{"role":"user","content":"hi"}]}`,
			400, "unsupported_content", "images are only supported in user messages"},
		{"image in assistant", `{"model":"frontier","messages":[{"role":"user","content":"hi"},{"role":"assistant","content":` + img + `}]}`,
			400, "unsupported_content", "images are only supported in user messages"},
		{"audio part", `{"model":"frontier","messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"AA","format":"wav"}}]}]}`,
			400, "unsupported_content", "only text content is supported"},
		{"file part", `{"model":"frontier","messages":[{"role":"user","content":[{"type":"file","file":{"file_id":"f1"}}]}]}`,
			400, "unsupported_content", "only text content is supported"},
		{"non-http image scheme", `{"model":"frontier","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"file:///etc/passwd"}}]}]}`,
			400, "unsupported_content", "image_url must be an http(s) URL or a base64 data URL with an image/* media type"},
		{"non-image data url", `{"model":"frontier","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:text/plain;base64,QUJD"}}]}]}`,
			400, "unsupported_content", "image_url must be an http(s) URL or a base64 data URL with an image/* media type"},
		{"malformed data url", `{"model":"frontier","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,"}}]}]}`,
			400, "unsupported_content", "image_url must be an http(s) URL or a base64 data URL with an image/* media type"},
		{"json_object", `{"model":"frontier","response_format":{"type":"json_object"},"messages":` + msgs + `}`, 400, "unsupported_value", ""},
		{"schema missing", `{"model":"frontier","response_format":{"type":"json_schema","json_schema":{"name":"x"}},"messages":` + msgs + `}`, 400, "unsupported_value", ""},
		{"bad stop", `{"model":"frontier","stop":7,"messages":` + msgs + `}`, 400, "unsupported_value", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			norm, werr := normalizeChatRequest(lan, []byte(tc.body))
			if werr == nil {
				t.Fatalf("accepted; norm=%+v", norm)
			}
			if werr.status != tc.status || werr.code != tc.code {
				t.Errorf("got (%d, %q), want (%d, %q): %s", werr.status, werr.code, tc.status, tc.code, werr.msg)
			}
			if tc.wantMsg != "" && !strings.Contains(werr.msg, tc.wantMsg) {
				t.Errorf("message = %q, want it to name %q", werr.msg, tc.wantMsg)
			}
		})
	}
}

// TestNormalizeChatRequestImages pins the two shapes the normalizer may emit:
// a plain string while a turn is text-only (the historical bytes) and a block
// array as soon as it carries an image.
func TestNormalizeChatRequestImages(t *testing.T) {
	withWireClassRows(t)
	msgContent := func(t *testing.T, body string, want int) []json.RawMessage {
		t.Helper()
		norm, werr := normalizeChatRequest(lan, []byte(body))
		if werr != nil {
			t.Fatalf("unexpected refusal: %+v", werr)
		}
		var ab struct {
			Messages []struct {
				Role    string          `json:"role"`
				Content json.RawMessage `json:"content"`
			} `json:"messages"`
		}
		if err := json.Unmarshal(norm.anthropicBody, &ab); err != nil {
			t.Fatalf("anthropic body not JSON: %v", err)
		}
		if len(ab.Messages) != want {
			t.Fatalf("messages = %d, want %d: %s", len(ab.Messages), want, norm.anthropicBody)
		}
		out := make([]json.RawMessage, len(ab.Messages))
		for i, m := range ab.Messages {
			out[i] = m.Content
		}
		return out
	}
	blocksOf := func(t *testing.T, raw json.RawMessage) []struct {
		Type   string `json:"type"`
		Text   string `json:"text"`
		Source struct {
			Type      string `json:"type"`
			MediaType string `json:"media_type"`
			Data      string `json:"data"`
			URL       string `json:"url"`
		} `json:"source"`
	} {
		t.Helper()
		var blocks []struct {
			Type   string `json:"type"`
			Text   string `json:"text"`
			Source struct {
				Type      string `json:"type"`
				MediaType string `json:"media_type"`
				Data      string `json:"data"`
				URL       string `json:"url"`
			} `json:"source"`
		}
		if err := json.Unmarshal(raw, &blocks); err != nil {
			t.Fatalf("content is not a block array: %v\n%s", err, raw)
		}
		return blocks
	}

	t.Run("text and images keep their order", func(t *testing.T) {
		contents := msgContent(t, `{"model":"frontier","messages":[{"role":"user","content":[
			{"type":"text","text":"wide"},
			{"type":"image_url","image_url":{"url":"data:image/jpeg;base64,QUJD","detail":"high"}},
			{"type":"text","text":"crop"},
			{"type":"image_url","image_url":{"url":"https://ref.test/a.png"}}
		]}]}`, 1)
		blocks := blocksOf(t, contents[0])
		if len(blocks) != 4 {
			t.Fatalf("blocks = %d, want 4: %s", len(blocks), contents[0])
		}
		if blocks[0].Type != "text" || blocks[0].Text != "wide" || blocks[2].Type != "text" || blocks[2].Text != "crop" {
			t.Errorf("text blocks = %+v / %+v", blocks[0], blocks[2])
		}
		if blocks[1].Type != "image" || blocks[1].Source.Type != "base64" ||
			blocks[1].Source.MediaType != "image/jpeg" || blocks[1].Source.Data != "QUJD" {
			t.Errorf("data-url block = %+v, want a base64 image source", blocks[1])
		}
		if blocks[3].Type != "image" || blocks[3].Source.Type != "url" || blocks[3].Source.URL != "https://ref.test/a.png" {
			t.Errorf("http block = %+v, want a url image source", blocks[3])
		}
	})

	t.Run("a preceding text-only user turn merges in as a text block", func(t *testing.T) {
		contents := msgContent(t, `{"model":"frontier","messages":[
			{"role":"user","content":"look at this"},
			{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AA"}}]}
		]}`, 1)
		blocks := blocksOf(t, contents[0])
		if len(blocks) != 2 {
			t.Fatalf("blocks = %d, want 2: %s", len(blocks), contents[0])
		}
		if blocks[0].Type != "text" || blocks[0].Text != "look at this" {
			t.Errorf("blocks[0] = %+v, want the promoted text turn", blocks[0])
		}
		if blocks[1].Type != "image" || blocks[1].Source.MediaType != "image/png" {
			t.Errorf("blocks[1] = %+v, want the image", blocks[1])
		}
	})

	t.Run("following text turns append and keep the blank-line join", func(t *testing.T) {
		contents := msgContent(t, `{"model":"frontier","messages":[
			{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AA"}}]},
			{"role":"user","content":"what is it?"},
			{"role":"user","content":"and this?"}
		]}`, 1)
		blocks := blocksOf(t, contents[0])
		if len(blocks) != 2 || blocks[0].Type != "image" || blocks[1].Type != "text" {
			t.Fatalf("blocks = %+v, want [image, text]", blocks)
		}
		if blocks[1].Text != "what is it?\n\nand this?" {
			t.Errorf("merged text = %q, want the two turns joined by a blank line", blocks[1].Text)
		}
	})

	t.Run("empty text blocks never reach the wire", func(t *testing.T) {
		contents := msgContent(t, `{"model":"frontier","messages":[
			{"role":"user","content":null},
			{"role":"user","content":[
				{"type":"text","text":"   "},
				{"type":"image_url","image_url":{"url":"data:image/png;base64,AA"}},
				{"type":"text","text":""}
			]}
		]}`, 1)
		blocks := blocksOf(t, contents[0])
		if len(blocks) != 1 || blocks[0].Type != "image" {
			t.Errorf("blocks = %+v, want the image alone (empty text blocks 400 upstream)", blocks)
		}
	})

	t.Run("a text-only null turn still flattens to an empty string", func(t *testing.T) {
		contents := msgContent(t, `{"model":"frontier","messages":[{"role":"user","content":null}]}`, 1)
		var s string
		if err := json.Unmarshal(contents[0], &s); err != nil || s != "" {
			t.Errorf("content = %s, want the empty JSON string (unchanged)", contents[0])
		}
	})

	t.Run("text-only turns stay JSON strings", func(t *testing.T) {
		contents := msgContent(t, `{"model":"frontier","messages":[
			{"role":"user","content":[{"type":"text","text":"a"},{"type":"text","text":"b"}]},
			{"role":"assistant","content":"ok"},
			{"role":"user","content":"c"},
			{"role":"user","content":"d"}
		]}`, 3)
		want := []string{"ab", "ok", "c\n\nd"}
		for i, w := range want {
			var s string
			if err := json.Unmarshal(contents[i], &s); err != nil {
				t.Fatalf("messages[%d].content is not a string (regression): %s", i, contents[i])
			}
			if s != w {
				t.Errorf("messages[%d].content = %q, want %q", i, s, w)
			}
		}
	})
}

func TestRenderChatResponseMapping(t *testing.T) {
	norm := &wireNorm{
		requestedModel: "frontier",
		class:          &registryEntry{Name: "frontier", Type: "class", Class: "frontier"},
		syntheticTool:  "out",
	}

	t.Run("anthropic error maps to openai shape", func(t *testing.T) {
		cw := newWireCapture()
		cw.status = 503
		cw.header.Set("Retry-After", "17")
		cw.body.WriteString(`{"type":"error","error":{"type":"api_error","message":"no member of class frontier is usable"}}`)
		rec := httptest.NewRecorder()
		renderChatResponse(rec, cw, norm)
		if rec.Code != 503 {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
		if got := rec.Header().Get("Retry-After"); got != "17" {
			t.Errorf("Retry-After = %q, want copied 17", got)
		}
		var e struct {
			Error struct{ Message, Type, Code string } `json:"error"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
			t.Fatalf("not JSON: %v", err)
		}
		if e.Error.Type != "api_error" || !strings.Contains(e.Error.Message, "no member of class") {
			t.Errorf("error = %+v", e.Error)
		}
	})

	t.Run("unparseable 200 becomes 502", func(t *testing.T) {
		cw := newWireCapture()
		cw.body.WriteString("event: message_start\ndata: {}\n\n")
		rec := httptest.NewRecorder()
		renderChatResponse(rec, cw, norm)
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502", rec.Code)
		}
	})

	t.Run("tool_use carrier extracted with usage math", func(t *testing.T) {
		cw := newWireCapture()
		cw.body.WriteString(`{"type":"message","id":"msg_1","stop_reason":"max_tokens",
			"content":[{"type":"text","text":"preamble"},{"type":"tool_use","name":"out","input":{"x":1}}],
			"usage":{"input_tokens":100,"output_tokens":20,"cache_read_input_tokens":40}}`)
		rec := httptest.NewRecorder()
		renderChatResponse(rec, cw, norm)
		if rec.Code != 200 {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var m struct {
			ID      string `json:"id"`
			Model   string `json:"model"`
			Choices []struct {
				Message      struct{ Content string } `json:"message"`
				FinishReason string                   `json:"finish_reason"`
			} `json:"choices"`
			Usage struct {
				PromptTokens     int `json:"prompt_tokens"`
				CompletionTokens int `json:"completion_tokens"`
				TotalTokens      int `json:"total_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
			t.Fatalf("not JSON: %v", err)
		}
		if m.ID != "chatcmpl-msg_1" || m.Model != "frontier" {
			t.Errorf("id/model = %q/%q", m.ID, m.Model)
		}
		var payload struct{ X int }
		if err := json.Unmarshal([]byte(m.Choices[0].Message.Content), &payload); err != nil || payload.X != 1 {
			t.Errorf("content = %q, want the tool input JSON", m.Choices[0].Message.Content)
		}
		if m.Choices[0].FinishReason != "length" {
			t.Errorf("finish_reason = %q, want length (max_tokens)", m.Choices[0].FinishReason)
		}
		if m.Usage.PromptTokens != 140 || m.Usage.CompletionTokens != 20 || m.Usage.TotalTokens != 160 {
			t.Errorf("usage = %+v, want 140/20/160", m.Usage)
		}
	})
}
