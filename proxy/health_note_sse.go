package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
)

func emitNoteFrames(emit func(string, any), index int, text string) {
	emit("content_block_start", map[string]any{"type": "content_block_start", "index": index,
		"content_block": map[string]string{"type": "text", "text": ""}})
	emit("content_block_delta", map[string]any{"type": "content_block_delta", "index": index,
		"delta": map[string]string{"type": "text_delta", "text": text}})
	emit("content_block_stop", map[string]any{"type": "content_block_stop", "index": index})
}

// Only installed on a note candidate. Model data passes through a line at a
// time; headers wait for that frame's data so insertion never splits an
// `event: message_delta` from its payload. The terminal frame waits for its
// blank line so malformed/multiline/incomplete SSE fails open byte-for-byte.
// This reader never waits for message_stop or peeks into the next frame.
type noteSSE struct {
	r      *bufio.Reader
	body   io.Closer
	note   *responseNote
	out    bytes.Buffer
	prefix bytes.Buffer
	err    error

	event     string
	dataLines int
	terminal  bool
	stop      string
	disabled  bool
	started   bool
	readable  bool
	nextIndex int
	open      map[int]string
	seen      map[int]bool
}

func newNoteSSE(body io.ReadCloser, n *responseNote) *noteSSE {
	return &noteSSE{r: bufio.NewReaderSize(body, 64<<10), body: body, note: n,
		open: map[int]string{}, seen: map[int]bool{}}
}

func (s *noteSSE) Close() error { return s.body.Close() }

func (s *noteSSE) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for s.out.Len() == 0 && s.err == nil {
		line, err := s.r.ReadBytes('\n')
		if err != nil {
			// Even a valid-looking terminal without its frame delimiter is
			// incomplete. Preserve the bytes and the underlying read error.
			s.flushPrefix()
			s.out.Write(line)
			s.err = err
			break
		}
		s.line(line)
	}
	if s.out.Len() > 0 {
		return s.out.Read(p)
	}
	return 0, s.err
}

func (s *noteSSE) flushPrefix() {
	s.out.Write(s.prefix.Bytes())
	s.prefix.Reset()
}

func (s *noteSSE) line(line []byte) {
	if s.disabled {
		s.out.Write(line)
		return
	}
	value := bytes.TrimSuffix(bytes.TrimSuffix(line, []byte{'\n'}), []byte{'\r'})
	if len(value) == 0 {
		if s.terminal && s.dataLines == 1 && s.started && s.readable && len(s.open) == 0 &&
			s.stop != "" && s.stop != "tool_use" && !s.note.noted.Load() {
			emitNoteFrames(func(event string, payload any) {
				b, _ := json.Marshal(payload)
				s.out.WriteString("event: " + event + "\ndata: ")
				s.out.Write(b)
				s.out.WriteString("\n\n")
			}, s.nextIndex, s.note.text)
			s.note.emitted()
		}
		s.flushPrefix()
		s.out.Write(line)
		if s.terminal {
			s.disabled = true // one message, at most one note, even on duplicate terminals
		}
		s.event, s.stop, s.dataLines, s.terminal = "", "", 0, false
		return
	}
	field, v, _ := bytes.Cut(value, []byte{':'})
	v = bytes.TrimPrefix(v, []byte{' '}) // SSE removes exactly one optional space
	if bytes.Equal(field, []byte("event")) && (s.dataLines > 0 || s.event != "") {
		// A late/repeated event field can change how the data is dispatched.
		// Preserve that unusual frame instead of assuming our parsed type wins.
		s.disabled = true
		s.flushPrefix()
		s.out.Write(line)
		return
	}
	if bytes.Equal(field, []byte("data")) {
		s.dataLines++
		if s.dataLines != 1 || !s.inspect(v) {
			s.disabled = true
			s.flushPrefix()
			s.out.Write(line)
			return
		}
		if s.terminal {
			s.prefix.Write(line)
		} else {
			s.flushPrefix()
			s.out.Write(line)
		}
	} else if s.dataLines == 0 || s.terminal {
		if bytes.Equal(field, []byte("event")) {
			s.event = string(v)
		}
		s.prefix.Write(line)
	} else {
		s.out.Write(line)
	}
	if s.prefix.Len() > 64<<10 {
		// Only frame metadata and terminal payloads are held here. Large model
		// deltas flow through unchanged and do not disable the note.
		s.disabled = true
		s.flushPrefix()
	}
}

func (s *noteSSE) inspect(data []byte) bool {
	var ev struct {
		Type         string                      `json:"type"`
		Index        *int                        `json:"index"`
		ContentBlock struct{ Type, Text string } `json:"content_block"`
		Delta        struct {
			Type, Text string
			StopReason string `json:"stop_reason"`
		} `json:"delta"`
	}
	if json.Unmarshal(data, &ev) != nil || ev.Type == "" || s.event != "" && s.event != ev.Type {
		return false
	}
	switch ev.Type {
	case "error", "message_stop":
		return false
	case "message_start":
		if s.started {
			return false
		}
		s.started = true
	case "content_block_start":
		if !s.started || ev.Index == nil || *ev.Index < 0 || *ev.Index == int(^uint(0)>>1) || s.seen[*ev.Index] || ev.ContentBlock.Type == "" {
			return false
		}
		s.open[*ev.Index], s.seen[*ev.Index] = ev.ContentBlock.Type, true
		s.nextIndex = max(s.nextIndex, *ev.Index+1)
		s.readable = s.readable || ev.ContentBlock.Type == "text" && ev.ContentBlock.Text != ""
	case "content_block_delta":
		if ev.Index == nil || s.open[*ev.Index] == "" {
			return false
		}
		if ev.Delta.Type == "text_delta" && s.open[*ev.Index] == "text" && ev.Delta.Text != "" {
			s.readable = true
		}
	case "content_block_stop":
		if ev.Index == nil || s.open[*ev.Index] == "" {
			return false
		}
		delete(s.open, *ev.Index)
	case "message_delta":
		if ev.Delta.StopReason != "" {
			s.terminal, s.stop = true, ev.Delta.StopReason
		}
	}
	return true
}
