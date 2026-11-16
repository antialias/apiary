package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// A scheduled job's stall shape: a slow head, three events, then
// silence until the client gave up.
func TestUpstreamTraceStallThenCancel(t *testing.T) {
	t0 := time.Date(2026, 10, 20, 12, 2, 29, 0, time.UTC)
	head := t0.Add(7040 * time.Millisecond)
	u := newUpstreamTrace(context.WithValue(context.Background(), reqStartKey, t0), head)
	u.event("response.created", head.Add(300*time.Millisecond))
	u.event("response.in_progress", head.Add(310*time.Millisecond))
	u.event("response.output_item.added", head.Add(2*time.Second))
	u.ended(false, nil, context.Canceled, head.Add(390*time.Second))

	got := u.logTag(head.Add(391 * time.Second))
	want := " up=head:7.04s,first:300ms,gap:6m28s,last:output_item.added,end:canceled" +
		" upev=created:1,in_progress:1,output_item.added:1"
	if got != want {
		t.Fatalf("logTag\n got %q\nwant %q", got, want)
	}
}

// A read still going when the line is written is measured to then.
func TestUpstreamTraceStillReading(t *testing.T) {
	t0 := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	u := newUpstreamTrace(context.Background(), t0)
	got := u.logTag(t0.Add(42 * time.Second))
	want := " up=head:none,first:none,gap:42s,last:none,end:reading upev=none"
	if got != want {
		t.Fatalf("logTag\n got %q\nwant %q", got, want)
	}
}

// The longest silence is between events when the tail is short, and types keep
// the order they first arrived in.
func TestUpstreamTraceCountsAndGap(t *testing.T) {
	t0 := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	u := newUpstreamTrace(context.Background(), t0)
	u.event("response.created", t0.Add(time.Second))
	u.event("response.output_text.delta", t0.Add(2*time.Second))
	u.event("", t0.Add(2*time.Second))
	u.event("response.output_text.delta", t0.Add(20*time.Second))
	u.event("response.completed", t0.Add(21*time.Second))
	u.ended(true, nil, nil, t0.Add(21*time.Second))

	got := u.logTag(t0.Add(time.Minute))
	want := " up=head:none,first:1s,gap:18s,last:completed,end:done" +
		" upev=created:1,output_text.delta:2,unparsed:1,completed:1"
	if got != want {
		t.Fatalf("logTag\n got %q\nwant %q", got, want)
	}
}

func TestUpstreamTraceEnd(t *testing.T) {
	for _, c := range []struct {
		terminal          bool
		writeErr, readErr error
		want              string
	}{
		{true, nil, nil, "end:done"},
		{false, nil, nil, "end:eof"},
		{false, errors.New("io: read/write on closed pipe"), nil, "end:client_gone"},
		{false, nil, fmt.Errorf("read: %w", context.Canceled), "end:canceled"},
		{false, nil, context.DeadlineExceeded, "end:canceled"},
		{false, nil, io.ErrUnexpectedEOF, "end:read_error"},
	} {
		t0 := time.Now()
		u := newUpstreamTrace(context.Background(), t0)
		u.ended(c.terminal, c.writeErr, c.readErr, t0)
		if got := u.logTag(t0); !strings.Contains(got, ","+c.want+" ") {
			t.Errorf("ended(%v, %v, %v) → %q, want %s", c.terminal, c.writeErr, c.readErr, got, c.want)
		}
	}
}

func TestUpstreamTraceNil(t *testing.T) {
	var u *upstreamTrace
	u.event("response.created", time.Now())
	u.ended(true, nil, nil, time.Now())
	if got := u.logTag(time.Now()); got != "" {
		t.Fatalf("nil trace rendered %q", got)
	}
}

// Both translator paths record what they read and how the read ended.
func TestUpstreamTraceTranslators(t *testing.T) {
	full := "upev=created:1,output_item.added:1,output_text.delta:2,output_item.done:1,completed:1"
	for _, c := range []struct {
		name, script, want string
	}{
		{"complete", defaultCodexSSE, ",last:completed,end:done " + full},
		{"truncated", created132, ",last:created,end:eof upev=created:1"},
	} {
		t.Run(c.name+"/stream", func(t *testing.T) {
			u := newUpstreamTrace(context.Background(), time.Now())
			_ = translateCodexSSE(strings.NewReader(c.script), io.Discard, 0, nil, nil, u, nil, nil)
			if got := u.logTag(time.Now()); !strings.HasSuffix(got, c.want) {
				t.Fatalf("got %q, want suffix %q", got, c.want)
			}
		})
		t.Run(c.name+"/aggregate", func(t *testing.T) {
			u := newUpstreamTrace(context.Background(), time.Now())
			_, _ = aggregateCodexSSE(strings.NewReader(c.script), 0, nil, u, nil, nil)
			if got := u.logTag(time.Now()); !strings.HasSuffix(got, c.want) {
				t.Fatalf("got %q, want suffix %q", got, c.want)
			}
		})
	}
}

// stall1020 is what the stalled stream plausibly sent before going quiet: the
// opening events and a reasoning item the translator forwards nothing for.
const stall1020 = `event: response.created
data: {"type":"response.created","response":{"id":"resp_s","model":"gpt-6.1-sol"}}

event: response.in_progress
data: {"type":"response.in_progress","response":{"id":"resp_s"}}

event: response.output_item.added
data: {"type":"response.output_item.added","item":{"type":"reasoning"}}

`

// waitLog polls the captured log for a line containing want.
func waitLog(t *testing.T, logs *syncBuf, want string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, line := range strings.Split(logs.String(), "\n") {
			if strings.Contains(line, want) {
				return line
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no log line containing %q in:\n%s", want, logs.String())
	return ""
}

// End to end on the class route the helpdesk takes: GPT goes quiet after a few
// events and the client gives up. The line that records the cancel says what
// GPT sent and how long it had been silent.
func TestE2EUpstreamTraceStall(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()
	armHealth(t, newHealthStore(defaultHealthCfg()))
	armObserve(t)
	logs := captureLogSync(t)
	h.codexSSE.Store(stall1020)
	h.codexHold.Store(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := `{"model":"claude-frontier","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}`
	req, _ := http.NewRequestWithContext(ctx, "POST", h.proxy.URL+"/v1/messages", bytes.NewReader([]byte(body)))
	req.Header.Set("Authorization", "Bearer CLIENT")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("anthropic-beta", "oauth-2025-04-20")
	req.Header.Set("x-claude-code-session-id", "sess-stall-1020")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request failed: %v", err)
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() && !strings.Contains(sc.Text(), "message_start") {
	}
	h.codexHitOrFail(t)
	time.Sleep(200 * time.Millisecond) // the silence the gap has to show
	cancel()

	line := waitLog(t, logs, "request ended")
	t.Log(line)
	for _, want := range []string{
		"out=cancelled up=head:",
		",last:output_item.added,end:canceled upev=created:1,in_progress:1,output_item.added:1",
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("request ended line lacks %q:\n%s", want, line)
		}
	}
	if strings.Contains(line, "gap:0s") || strings.Contains(line, "gap:none") {
		t.Fatalf("the silence before the cancel is missing from the gap:\n%s", line)
	}
}

// A served stream that completes logs every event and end:done, streamed or
// aggregated.
func TestE2EUpstreamTraceComplete(t *testing.T) {
	for _, stream := range []bool{true, false} {
		t.Run(map[bool]string{true: "stream", false: "non-stream"}[stream], func(t *testing.T) {
			h, restore := classWithUsableGpt(t)
			defer restore()
			armHealth(t, newHealthStore(defaultHealthCfg()))
			armObserve(t)
			logs := captureLogSync(t)

			resp := h.postStreamSess(t, "claude-frontier", stream, "sess-complete-1020")
			_, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
			h.codexHitOrFail(t)

			line := waitLog(t, logs, "out=ok up=head:")
			want := ",last:completed,end:done upev=created:1,output_item.added:1," +
				"output_text.delta:2,output_item.done:1,completed:1"
			if !strings.Contains(line, want) {
				t.Fatalf("access line lacks %q:\n%s", want, line)
			}
		})
	}
}
