package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func noteSSEFrame(kind, fields string) string {
	return "event: " + kind + "\ndata: {\"type\":\"" + kind + "\"" + fields + "}\n\n"
}

func noteSSEFixture(stop, text string) string {
	txt, _ := json.Marshal(text)
	return ": comment\n\n" +
		noteSSEFrame("message_start", `,"message":{"content":[],"usage":{"input_tokens":17}}`) +
		noteSSEFrame("content_block_start", `,"index":0,"content_block":{"type":"thinking","thinking":""}`) +
		noteSSEFrame("content_block_delta", `,"index":0,"delta":{"type":"thinking_delta","thinking":"hmm"}`) +
		noteSSEFrame("content_block_stop", `,"index":0`) +
		noteSSEFrame("content_block_start", `,"index":4,"content_block":{"type":"text","text":""}`) +
		noteSSEFrame("content_block_delta", `,"index":4,"delta":{"type":"text_delta","text":`+string(txt)+`}`) +
		noteSSEFrame("content_block_stop", `,"index":4`) +
		noteSSEFrame("message_delta", `,"delta":{"stop_reason":"`+stop+`"},"usage":{"output_tokens":9007199254740993}`) +
		noteSSEFrame("message_stop", "")
}

func renderedNoteFrames(index int, text string) string {
	var out bytes.Buffer
	emitNoteFrames(func(event string, payload any) {
		b, _ := json.Marshal(payload)
		out.WriteString("event: " + event + "\ndata: " + string(b) + "\n\n")
	}, index, text)
	return out.String()
}

func assertSSEFrames(t *testing.T, stream string) {
	t.Helper()
	for _, frame := range strings.Split(strings.ReplaceAll(stream, "\r\n", "\n"), "\n\n") {
		var event string
		for _, line := range strings.Split(frame, "\n") {
			if strings.HasPrefix(line, "event: ") {
				event = strings.TrimPrefix(line, "event: ")
			}
			if strings.HasPrefix(line, "data: ") {
				var v struct{ Type string }
				if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &v) != nil || event != "" && event != v.Type {
					t.Fatalf("SSE event and data were split: %q", frame)
				}
			}
		}
	}
}

func TestNoteSSEPlacementAndBytes(t *testing.T) {
	for _, variant := range []string{"normal", "CRLF", "data-only", "large", "metadata"} {
		t.Run(variant, func(t *testing.T) {
			raw := noteSSEFixture("end_turn", "answer")
			switch variant {
			case "CRLF":
				raw = strings.ReplaceAll(raw, "\n", "\r\n")
			case "data-only":
				var out strings.Builder
				for _, line := range strings.SplitAfter(raw, "\n") {
					if !strings.HasPrefix(line, "event:") {
						out.WriteString(line)
					}
				}
				raw = out.String()
			case "large":
				raw = noteSSEFixture("max_tokens", strings.Repeat("long text ", 12000))
			case "metadata":
				raw = strings.Replace(raw, "event: message_delta", "id: terminal-1\n: terminal comment\nevent: message_delta", 1)
			}
			n := testResponseNote()
			reader := io.Reader(strings.NewReader(raw))
			if variant != "large" {
				reader = iotest.OneByteReader(reader)
			}
			s := newNoteSSE(io.NopCloser(reader), n)
			got, err := io.ReadAll(s)
			s.Close()
			insertion := renderedNoteFrames(5, n.text)
			if err != nil || strings.Count(string(got), noteSentinel) != 1 || !n.noted.Load() || !strings.Contains(string(got), insertion) {
				t.Fatalf("missing or duplicate note: err=%v noted=%v", err, n.noted.Load())
			}
			if strings.Replace(string(got), insertion, "", 1) != raw {
				t.Fatal("non-note bytes changed")
			}
			assertSSEFrames(t, string(got))
			if strings.Index(string(got), insertion) > strings.Index(string(got), `"stop_reason"`) {
				t.Fatal("note followed the terminal message delta")
			}
		})
	}
}

func TestNoteSSENegativeGuards(t *testing.T) {
	raw := noteSSEFixture("end_turn", "answer")
	terminal := strings.Index(raw, "event: message_delta")
	for name, input := range map[string]string{
		"tool_use":                noteSSEFixture("tool_use", "answer"),
		"no text":                 noteSSEFixture("end_turn", ""),
		"not text":                strings.Replace(raw, `"type":"text","text":""`, `"type":"tool_use","text":""`, 1),
		"truncated":               raw[:terminal],
		"no terminal delimiter":   raw[:strings.Index(raw, "event: message_stop")-1],
		"unclosed block":          strings.Replace(raw, noteSSEFrame("content_block_stop", `,"index":4`), "", 1),
		"error":                   raw[:terminal] + noteSSEFrame("error", `,"error":{"type":"api_error"}`) + raw[terminal:],
		"malformed":               raw[:terminal] + "data: garbage\n\n" + raw[terminal:],
		"multiline terminal":      strings.Replace(raw, `,"usage":{"output_tokens":9007199254740993}`, "\ndata: ,\"usage\":{\"output_tokens\":9007199254740993}", 1),
		"duplicate terminal data": raw[:terminal] + strings.Replace(raw[terminal:], "}\n\n", "}\ndata: {}\n\n", 1),
		"late event field":        raw[:terminal] + strings.Replace(raw[terminal:], "}\n\n", "}\nevent: error\n\n", 1),
		"oversize metadata":       raw[:terminal] + "id: " + strings.Repeat("x", 70<<10) + "\n" + raw[terminal:],
	} {
		t.Run(name, func(t *testing.T) {
			n := testResponseNote()
			body := &noteTestBody{Reader: iotest.OneByteReader(strings.NewReader(input))}
			s := newNoteSSE(body, n)
			got, err := io.ReadAll(s)
			s.Close()
			if err != nil || string(got) != input || n.noted.Load() || !body.closed {
				t.Fatalf("ineligible stream changed: err=%v noted=%v\n%s", err, n.noted.Load(), got)
			}
		})
	}
}

// Both readers must emit model text before the producer releases the terminal.
// An io.Pipe handshake proves this without relying on a timing threshold.
func TestNoteSSEDoesNotWaitForTerminalToForwardText(t *testing.T) {
	raw := noteSSEFixture("end_turn", "answer")
	before := raw[:strings.Index(raw, "event: message_delta")]
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()
	go func() { _, _ = io.WriteString(w, before) }()
	n := testResponseNote()
	s := newNoteSSE(r, n)
	got := make([]byte, len(before))
	if _, err := io.ReadFull(s, got); err != nil || string(got) != before || n.noted.Load() {
		t.Fatalf("model text held for terminal: %v", err)
	}
}

func TestNoteCodexCompletedOnly(t *testing.T) {
	text := `data: {"type":"response.output_text.delta","delta":"answer"}` + "\n\n"
	complete := `data: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":17,"output_tokens":9}}}` + "\n\n"
	for name, input := range map[string]string{
		"completed": text + complete,
		"truncated": text,
		"failed":    text + `data: {"type":"response.failed","response":{"error":{"message":"upstream error"}}}` + "\n\n",
		"empty":     complete,
		"tool_use":  text + `data: {"type":"response.output_item.added","item":{"type":"function_call","name":"tool","call_id":"id"}}` + "\n\n" + complete,
	} {
		t.Run(name, func(t *testing.T) {
			var baseline, out bytes.Buffer
			_ = translateCodexSSE(strings.NewReader(input), &baseline, 1, nil, nil, nil, nil, nil)
			n := testResponseNote()
			hs := &hopScore{ctx: context.Background(), cc: &classRoute{tenant: lan, note: n}}
			_ = translateCodexSSE(strings.NewReader(input), &out, 1, nil, hs, nil, nil, nil)
			if name == "completed" {
				insertion := renderedNoteFrames(1, n.text)
				if !n.noted.Load() || strings.Count(out.String(), noteSentinel) != 1 || strings.Replace(out.String(), insertion, "", 1) != baseline.String() {
					t.Fatalf("translated note changed model content or usage:\n%s", out.String())
				}
				assertSSEFrames(t, out.String())
			} else if n.noted.Load() || out.String() != baseline.String() {
				t.Fatal("ineligible translated stream was annotated")
			}
		})
	}
}
