package main

// What the served tap (served.go) adds to the copy of a reply, which runs on
// ReverseProxy's copy loop: a streamed reply's per-chunk work, and the read
// that returns EOF, where a JSON reply is read for its usage.
//
// As TestTapFiveMegabyteBodyLatency: the assertions are ratios between two
// sizes, not wall-clock ceilings, so a loaded CI box scales both sides. The
// absolute numbers are logged.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

// servedSSEBody is an Anthropic stream of about n bytes: message_start,
// 1 KiB text deltas, message_delta.
func servedSSEBody(n int) []byte {
	var b bytes.Buffer
	b.WriteString(servedAnthStart)
	text := strings.Repeat("y", 1024)
	for b.Len() < n {
		fmt.Fprintf(&b, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,"+
			"\"delta\":{\"type\":\"text_delta\",\"text\":%q}}\n\n", text)
	}
	b.WriteString(servedAnthEnd)
	return b.Bytes()
}

// servedJSONBody is an Anthropic JSON reply of about n bytes, its usage last.
func servedJSONBody(n int) []byte {
	text := strings.Repeat("z", max(0, n-400))
	return []byte(`{"id":"msg_1","type":"message","role":"assistant","model":"claude-fable-5",` +
		`"content":[{"type":"text","text":"` + text + `"}],"stop_reason":"end_turn",` +
		`"usage":{"input_tokens":10,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,` +
		`"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0},"output_tokens":20}}`)
}

// servedCopy reads body through the tap (or not) in ReverseProxy's 32 KiB
// reads; it returns the whole copy's time and the time of the read that
// returned EOF.
func servedCopy(body []byte, tapped, stream bool) (total, eof time.Duration) {
	var r io.ReadCloser = io.NopCloser(bytes.NewReader(body))
	var c *servedCap
	if tapped {
		_, c = withServedCap(context.Background())
		c.request("claude-fable-5", reqFacts{}, true)
		c.hop("a", 200, func(string) servedHopFacts { return servedHopFacts{kind: servedPassthrough} })
		r = &servedTap{inner: r, cap: c, stream: stream}
	}
	buf := make([]byte, 32<<10)
	start := time.Now()
	for {
		at := time.Now()
		_, err := r.Read(buf)
		if err != nil {
			eof = time.Since(at)
			break
		}
	}
	total = time.Since(start)
	_ = r.Close()
	if tapped {
		c.mu.Lock()
		final := c.final
		c.mu.Unlock()
		if !final {
			panic("the tap missed the final usage")
		}
	}
	return total, eof
}

// best runs f five times and keeps the fastest of each figure.
func servedBest(f func() (time.Duration, time.Duration)) (time.Duration, time.Duration) {
	bt, be := time.Hour, time.Hour
	for range 5 {
		t, e := f()
		bt, be = min(bt, t), min(be, e)
	}
	return bt, be
}

func TestServedTapLatency(t *testing.T) {
	t.Cleanup(resetQuotaState())

	// A streamed reply: the tap's per-chunk work is linear in the bytes.
	small, large := servedSSEBody(2<<20), servedSSEBody(8<<20)
	baseS, _ := servedBest(func() (time.Duration, time.Duration) { return servedCopy(small, false, true) })
	baseL, _ := servedBest(func() (time.Duration, time.Duration) { return servedCopy(large, false, true) })
	tapS, _ := servedBest(func() (time.Duration, time.Duration) { return servedCopy(small, true, true) })
	tapL, _ := servedBest(func() (time.Duration, time.Duration) { return servedCopy(large, true, true) })
	mb := float64(len(large)) / (1 << 20)
	t.Logf("stream %.1f MB: copy %v, through the tap %v (adds %v, %.1f µs/MB); %.1f MB: %v vs %v",
		mb, baseL, tapL, tapL-baseL, float64((tapL-baseL).Microseconds())/mb,
		float64(len(small))/(1<<20), baseS, tapS)
	sizeRatio := float64(len(large)) / float64(len(small))
	if r := float64(tapL) / float64(tapS); r > 3*sizeRatio {
		t.Fatalf("the streamed tap is not linear: %.1fx bytes in %.1fx time", sizeRatio, r)
	}

	// A JSON reply: the read that returns EOF is where its usage is read.
	// It must not grow with the reply.
	jSmall, jLarge := servedJSONBody(64<<10), servedJSONBody(1<<20)
	_, eofBase := servedBest(func() (time.Duration, time.Duration) { return servedCopy(jLarge, false, false) })
	totS, eofS := servedBest(func() (time.Duration, time.Duration) { return servedCopy(jSmall, true, false) })
	totL, eofL := servedBest(func() (time.Duration, time.Duration) { return servedCopy(jLarge, true, false) })
	t.Logf("JSON 1 MB: EOF read %v through the tap (untapped %v), whole copy %v; 64 KB: EOF read %v, copy %v",
		eofL, eofBase, totL, eofS, totS)
	if eofL > 4*eofS+50*time.Microsecond {
		t.Fatalf("the EOF read grows with the reply: %v at 1 MB vs %v at 64 KB", eofL, eofS)
	}
}
