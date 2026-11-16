package main

// healthTap is the §3 passthrough shape producer (#122): a body-wrapping
// reader on 200 replies the proxy delivers AS-IS (anthropic wire, kimi/bearer
// passthrough). It watches the bytes stream past — four sentinel scans per
// chunk, nothing buffered beyond a 32-byte tail for split markers — and
// classifies ONCE at body exit (read error, EOF, or Close), producing rows
// 1/2/3/4a/4b/5/20. Mode is the CLIENT's stream wish (wantsStream on the
// original body), never the upstream Content-Type (r3): a non-stream reply to
// a stream request lands on 4a, which is the right score for a passthrough
// that did not speak SSE when asked to.
//
// The literal-sentinel false positive (a reply whose CONTENT contains
// "event: error") is spec-accepted (§3): one mis-scored sample, never
// mis-routed.

import (
	"bytes"
	"context"
	"errors"
	"io"
)

var (
	tapEvStart = []byte("event: message_start")
	tapEvBlock = []byte("event: content_block_start") // longest marker: 26 bytes
	tapEvError = []byte("event: error")
	tapEvStop  = []byte("event: message_stop")
)

const tapTailLen = 32 // > longest marker, so no marker can span three chunks

type healthTap struct {
	inner  io.ReadCloser
	ctx    context.Context
	cc     *classRoute
	key    string
	stream bool

	sawStart, sawBlock, sawErr, sawStop bool
	tail                                []byte // last ≤32 bytes of the stream so far
	n                                   int64  // total bytes seen
	first                               byte   // first non-whitespace byte (non-stream mode)
	done                                bool
}

func newHealthTap(inner io.ReadCloser, ctx context.Context, cc *classRoute, key string, stream bool) io.ReadCloser {
	return &healthTap{inner: inner, ctx: ctx, cc: cc, key: key, stream: stream}
}

func (t *healthTap) Read(p []byte) (int, error) {
	n, err := t.inner.Read(p)
	if n > 0 {
		t.scan(p[:n])
	}
	if err != nil {
		t.finish(err)
	}
	return n, err
}

func (t *healthTap) Close() error {
	err := t.inner.Close()
	t.finish(nil)
	return err
}

func (t *healthTap) scan(chunk []byte) {
	t.n += int64(len(chunk))
	if !t.stream {
		if t.first == 0 {
			for _, b := range chunk {
				if b != ' ' && b != '\t' && b != '\n' && b != '\r' {
					t.first = b
					break
				}
			}
		}
		return
	}
	// A marker split across chunks: check tail+head of this chunk first. The
	// bound window is tail plus the first len(longest)-1 bytes of the chunk,
	// so a marker fully inside the chunk is found by the chunk scan alone.
	if len(t.tail) > 0 {
		head := len(tapEvBlock)
		if head > len(chunk) {
			head = len(chunk)
		}
		bound := make([]byte, 0, len(t.tail)+head)
		bound = append(bound, t.tail...)
		bound = append(bound, chunk[:head]...)
		t.mark(bound)
	}
	t.mark(chunk)
	// Roll the tail forward (self-append of tail+chunk suffix is forward-safe).
	t.tail = append(t.tail, chunk...)
	if len(t.tail) > tapTailLen {
		t.tail = t.tail[len(t.tail)-tapTailLen:]
	}
}

func (t *healthTap) mark(b []byte) {
	if !t.sawStart && bytes.Contains(b, tapEvStart) {
		t.sawStart = true
	}
	if !t.sawBlock && bytes.Contains(b, tapEvBlock) {
		t.sawBlock = true
	}
	if !t.sawErr && bytes.Contains(b, tapEvError) {
		t.sawErr = true
	}
	if !t.sawStop && bytes.Contains(b, tapEvStop) {
		t.sawStop = true
	}
}

// finish classifies once. readErr is the error the terminal Read returned
// (nil when the exit is Close). Rule 0 first: a canceled request context is
// the client going away — row 1, no sample, Rule 4 only (settle handles the
// lease arm; without one an evidence-none outcome writes nothing) — unless the
// reply had already finished: message_stop seen on a stream, a '{' body read
// to its EOF otherwise. Then the cancel came after the reply and the hop is
// scored by what it was. A lone "event: error" does not finish a stream here:
// it is a literal match that reply text can carry (§3), and letting it beat a
// mid-reply interrupt would turn those into degraded samples.
func (t *healthTap) finish(readErr error) {
	if t.done {
		return
	}
	t.done = true
	hs := &hopScore{ctx: t.ctx, key: t.key, cc: t.cc, lease: t.cc.leaseFor(t.key)}
	hs.settle(t.outcome(readErr))
}

// outcome maps the latched marks to a §3 row; split out so the row table is
// unit-testable without a store.
func (t *healthTap) outcome(readErr error) hopOutcome {
	out := hopOutcome{phase: "post_head", final: true}
	canceled := errors.Is(t.ctx.Err(), context.Canceled)
	if t.stream {
		switch {
		case canceled && !t.sawStop:
			out.row, out.sig, out.ladder, out.evidence = "1", sigCancelled, ladderStop, evNone
		case t.sawErr:
			out.row, out.sig, out.cause, out.ladder, out.evidence = "2", sigDegraded, "upstream_failed", ladderReturn, evDeg
		case t.sawStop:
			out.row, out.sig, out.evidence = "5", sigHealthy, evOK
		case t.sawBlock:
			out.row, out.sig, out.cause, out.ladder, out.evidence = "4b", sigUnknown, "stream_truncated_late", ladderReturn, evNone
		default:
			out.row, out.sig, out.cause, out.ladder, out.evidence = "4a", sigDegraded, "stream_truncated", ladderReturn, evDeg
		}
	} else {
		finished := t.first == '{' && errors.Is(readErr, io.EOF)
		switch {
		case canceled && !finished:
			out.row, out.sig, out.ladder, out.evidence = "1", sigCancelled, ladderStop, evNone
		case t.n == 0:
			out.row, out.sig, out.cause, out.ladder, out.evidence = "3", sigDegraded, "bad_body", ladderReturn, evDeg
		case t.first == '{' && (readErr == nil || errors.Is(readErr, io.EOF)):
			out.row, out.sig, out.evidence = "5", sigHealthy, evOK
		default:
			out.row, out.sig, out.ladder, out.evidence = "20", sigUnknown, ladderReturn, evNone
		}
	}
	return out
}
