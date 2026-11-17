package main

// End-to-end tests for GET /_switch/events (#59) through the real handler stack:
// buildHandler() + clientGate + net/http, not a recorder. A ResponseRecorder cannot
// test a long-lived stream, and the properties worth proving here — that the gate
// covers the endpoint, that a real dead peer cannot stall real requests, that a
// reconnect over real HTTP replays the gap — only exist above the socket.
//
// TEARDOWN HAZARD: httptest.Server.Close() blocks until every outstanding request has
// finished, and an SSE stream never finishes on its own. Every test here registers its
// stream teardown AFTER `defer restore()` so LIFO runs it first. Leak one and the suite
// stalls for 5s and then warns, rather than failing usefully.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// ---- fixtures ----

// evStream opens a subscriber against the proxy. The returned close func cancels the
// request context AND closes the body — see the teardown hazard above.
func evStream(t *testing.T, env *e2eEnv, bearer, lastID string) (*http.Response, <-chan string, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "GET", env.proxy.URL+"/_switch/events", nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("events request failed: %v", err)
	}
	return resp, evPump(resp.Body), func() { cancel(); resp.Body.Close() }
}

// evPump splits a live SSE body into frames on a channel, so tests can wait with a
// timeout instead of blocking forever on a hung server.
//
// bufio.Reader.ReadString, deliberately NOT bufio.Scanner: a Scanner stops with
// ErrTooLong on an oversized line, which is indistinguishable from a clean end of
// stream — exactly the failure mode a frame-size cap exists to prevent, so the test
// harness must not be able to hide it.
func evPump(r io.Reader) <-chan string {
	br := bufio.NewReader(r)
	out := make(chan string, 256)
	go func() {
		defer close(out)
		var b strings.Builder
		for {
			line, err := br.ReadString('\n')
			switch {
			case line == "\n" && b.Len() > 0:
				out <- b.String() + "\n"
				b.Reset()
			case line != "" && line != "\n":
				b.WriteString(line)
			}
			if err != nil {
				return
			}
		}
	}()
	return out
}

func evNext(t *testing.T, ch <-chan string, within time.Duration) string {
	t.Helper()
	select {
	case s, ok := <-ch:
		if !ok {
			t.Fatal("event stream closed while we were still reading it")
		}
		return s
	case <-time.After(within):
		t.Fatalf("no frame within %s", within)
		return ""
	}
}

// evAwait waits for a frame with the given event name, skipping keepalives and any
// other traffic.
func evAwait(t *testing.T, ch <-chan string, event string, within time.Duration) evParsed {
	t.Helper()
	deadline := time.After(within)
	for {
		select {
		case s, ok := <-ch:
			if !ok {
				t.Fatalf("stream closed before a %q frame arrived", event)
			}
			if strings.HasPrefix(s, ":") || !strings.Contains(s, "event: ") {
				continue // keepalive or the bare retry: hint
			}
			p := evParseFrame(t, []byte(s))
			if p.event == event {
				return p
			}
		case <-deadline:
			t.Fatalf("no %q frame within %s", event, within)
		}
	}
}

func evWaitFor(t *testing.T, what string, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", within, what)
}

func evSubsNow() int {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return len(hub.subs)
}

func evStatus(t *testing.T, env *e2eEnv) map[string]any {
	t.Helper()
	resp, err := http.Get(env.proxy.URL + "/_switch/status")
	if err != nil {
		t.Fatalf("status request failed: %v", err)
	}
	defer resp.Body.Close()
	var snap map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("status decode: %v", err)
	}
	ev, ok := snap["events"].(map[string]any)
	if !ok {
		t.Fatalf("status has no events object: %v", snap["events"])
	}
	return ev
}

// evHello reads the connection preamble and returns the hub_hello payload.
func evHello(t *testing.T, ch <-chan string) map[string]any {
	t.Helper()
	if first := evNext(t, ch, 3*time.Second); !strings.HasPrefix(first, "retry: ") {
		t.Fatalf("first block was %q, want the retry: hint", first)
	}
	p := evParseFrame(t, []byte(evNext(t, ch, 3*time.Second)))
	if p.event != "hub_hello" {
		t.Fatalf("second block was %q, want hub_hello", p.event)
	}
	if p.nID != 0 {
		t.Fatalf("hub_hello carries an id: line — it would clobber the client's Last-Event-ID:\n%s", p.raw)
	}
	data, _ := p.env.Data.(map[string]any)
	return data
}

// ---- tests ----

func TestE2EEventsStreamsThroughTheGate(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	withClientSecret(t, env, "sekrit")

	unauth, err := http.Get(env.proxy.URL + "/_switch/events")
	if err != nil {
		t.Fatal(err)
	}
	unauth.Body.Close()
	if unauth.StatusCode != 401 {
		t.Fatalf("unauthenticated stream got %d, want 401 — the LAN listener must not hand out the transcript", unauth.StatusCode)
	}

	resp, frames, closeStream := evStream(t, env, "sekrit", "")
	defer closeStream()
	if resp.StatusCode != 200 {
		t.Fatalf("authenticated stream got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream; charset=utf-8" {
		t.Fatalf("Content-Type = %q", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("Cache-Control = %q", cc)
	}

	hello := evHello(t, frames)
	gap, _ := hello["gap"].(map[string]any)
	if gap["reason"] != "fresh" {
		t.Fatalf("hub_hello gap.reason = %v, want \"fresh\" on a first connect", gap["reason"])
	}
	if hello["epoch"] != evEpoch {
		t.Fatalf("hub_hello epoch = %v, want %q", hello["epoch"], evEpoch)
	}
}

// The endpoint is not dead on arrival: a real lane flip on the real request path
// reaches a real subscriber.
func TestE2EEventsSwitchoverReachesSubscriber(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	// start fable on `native` so there is a previous effective account to move from
	env.brainMu.Lock()
	env.lanes["fable"] = map[string]any{"account": native, "reason": "incumbent",
		"effective": map[string]float64{native: 41, other: 30}}
	env.brainMu.Unlock()
	fetchPreferredSync()
	env.post(t, "claude-fable-5", "CLIENT").Body.Close()
	env.hit(t)

	_, frames, closeStream := evStream(t, env, "", "")
	defer closeStream()
	evHello(t, frames)
	evWaitFor(t, "the subscriber to register", 3*time.Second, func() bool { return evSubsNow() == 1 })

	env.brainMu.Lock()
	env.lanes["fable"] = map[string]any{"account": other, "reason": "fable cap hot",
		"effective": map[string]float64{native: 85, other: 70}}
	env.brainMu.Unlock()
	fetchPreferredSync()
	env.post(t, "claude-fable-5", "CLIENT").Body.Close()
	env.hit(t)

	p := evAwait(t, frames, "switchover", 5*time.Second)
	data, _ := p.env.Data.(map[string]any)
	if data["lane"] != "fable" || data["from"] != native || data["to"] != other {
		t.Fatalf("switchover payload = %v, want fable %s->%s", data, native, other)
	}
	if p.env.Type != "switchover" {
		t.Fatalf("payload type = %q, want it to mirror the event: line", p.env.Type)
	}
}

// The adversarial case the whole design exists for: a subscriber that reads nothing
// must not stall the publisher, real inference requests, or /_switch/status.
func TestE2EEventsNeverReadingSubscriberDoesNotStallRequests(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	oldChan := evChanSize
	evChanSize = 4 // channel is sized at attach, so this must precede the dial
	defer func() { evChanSize = oldChan }()

	// The previous test's stream teardown detaches asynchronously (the handler notices
	// the cancelled context on its own time), so wait for a quiet hub — otherwise the
	// evSubsNow()==1 wait below can be satisfied by the leaving subscriber instead of
	// the wedged one.
	evWaitFor(t, "prior subscribers to drain", 3*time.Second, func() bool { return evSubsNow() == 0 })

	// A raw socket, not an http.Client: a Client reads response headers eagerly, so it
	// is never truly never-reading. Only this wedges the server like a dead consumer.
	conn, err := net.Dial("tcp", env.proxy.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn, "GET /_switch/events HTTP/1.1\r\nHost: proxy\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	evWaitFor(t, "the wedged subscriber to register", 3*time.Second, func() bool { return evSubsNow() == 1 })

	// The flood must overwhelm the KERNEL, not just the 4-slot channel: the handler
	// drains the channel into the socket's send buffer, and 500 small frames fit in
	// loopback buffering entirely — whether any send ever found the channel full was a
	// scheduling race between the publisher and the drain loop (flaked under -race in
	// CI, where the instrumented publisher runs slower than the syscall drain). ~36 MB
	// of 24 KiB frames cannot be absorbed: the write must block, the channel must
	// fill, and publishes must drop.
	filler := strings.Repeat("x", 24<<10) // rendered frame stays under evMaxEventBytes
	flooded := make(chan struct{})
	go func() {
		defer close(flooded)
		for i := 0; i < 1500; i++ {
			publish("", "flood", map[string]any{"i": i, "fill": filler})
		}
	}()
	select {
	case <-flooded:
	case <-time.After(10 * time.Second):
		t.Fatal("publisher stalled behind a socket nobody is reading — this is the failure that would stall inference")
	}

	// Real requests still complete. Run them off the test goroutine with a watchdog so
	// a hang fails here rather than as a suite-wide timeout.
	type res struct {
		code int
		err  error
	}
	done := make(chan res, 3)
	for i := 0; i < 3; i++ {
		go func() {
			body := strings.NewReader(`{"model":"claude-fable-5","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`)
			req, _ := http.NewRequest("POST", env.proxy.URL+"/v1/messages", body)
			req.Header.Set("Authorization", "Bearer CLIENT")
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				done <- res{err: err}
				return
			}
			resp.Body.Close()
			done <- res{code: resp.StatusCode}
		}()
	}
	for i := 0; i < 3; i++ {
		select {
		case r := <-done:
			if r.err != nil {
				t.Fatalf("inference request failed while a subscriber was wedged: %v", r.err)
			}
			if r.code != 200 {
				t.Fatalf("inference request got %d while a subscriber was wedged", r.code)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("an inference request never completed while a subscriber was wedged")
		}
	}

	ev := evStatus(t, env) // proves st.mu is not stuck behind hub.mu
	if n, _ := ev["dropped"].(float64); n < 1 {
		t.Fatalf("dropped = %v, want >= 1 — backpressure must be counted", ev["dropped"])
	}
	if n, _ := ev["evicted"].(float64); n < 1 {
		t.Fatalf("evicted = %v, want >= 1 — the wedged subscriber must lose its slot", ev["evicted"])
	}
	if n, _ := ev["subscribers"].(float64); n != 0 {
		t.Fatalf("subscribers = %v, want 0", ev["subscribers"])
	}
}

func TestE2EEventsReconnectReplaysTheGap(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	_, frames, closeStream := evStream(t, env, "", "")
	evHello(t, frames)
	evWaitFor(t, "the subscriber to register", 3*time.Second, func() bool { return evSubsNow() == 1 })

	publish("", "marker", map[string]any{"n": 0})
	first := evAwait(t, frames, "marker", 3*time.Second)
	closeStream()
	evWaitFor(t, "the subscriber to detach", 3*time.Second, func() bool { return evSubsNow() == 0 })

	// Published into the gap — the hub is still armed by the replay grace.
	for i := 1; i <= 3; i++ {
		publish("", "marker", map[string]any{"n": i})
	}

	_, frames2, closeStream2 := evStream(t, env, "", first.id)
	defer closeStream2()
	hello := evHello(t, frames2)
	gap, _ := hello["gap"].(map[string]any)
	if gap["reason"] != "" {
		t.Fatalf("gap.reason = %v, want clean — the ring still held every missed event", gap["reason"])
	}
	for i := 1; i <= 3; i++ {
		p := evAwait(t, frames2, "marker", 3*time.Second)
		data, _ := p.env.Data.(map[string]any)
		if n, _ := data["n"].(float64); int(n) != i {
			t.Fatalf("replayed marker %v, want n=%d — the gap was not replayed in order", data, i)
		}
		if p.env.Seq != first.env.Seq+int64(i) {
			t.Fatalf("replayed seq %d, want %d", p.env.Seq, first.env.Seq+int64(i))
		}
	}
}

func TestE2EStatusExposesEvents(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	ev := evStatus(t, env)
	for _, k := range []string{"subscribers", "published", "dropped", "buffer_len"} {
		if _, ok := ev[k]; !ok {
			t.Fatalf("status events object is missing %q: %v", k, ev)
		}
	}

	_, frames, closeStream := evStream(t, env, "", "")
	defer closeStream()
	evHello(t, frames)
	evWaitFor(t, "subscribers to reach 1 in status", 3*time.Second, func() bool {
		n, _ := evStatus(t, env)["subscribers"].(float64)
		return n == 1
	})
}

func TestE2EEventsKeepalive(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	old := evKeepalive
	evKeepalive = 50 * time.Millisecond // the ticker is created at attach
	defer func() { evKeepalive = old }()

	_, frames, closeStream := evStream(t, env, "", "")
	defer closeStream()
	evHello(t, frames)

	deadline := time.After(3 * time.Second)
	for {
		select {
		case s, ok := <-frames:
			if !ok {
				t.Fatal("stream closed before a keepalive arrived")
			}
			if strings.HasPrefix(s, ":") {
				return // got one
			}
		case <-deadline:
			t.Fatal("no keepalive within 3s — it is the only thing that ever notices a wedged peer")
		}
	}
}

// The write deadline is the last line of defence for a peer that is connected, not
// reading, and whose queue is not full. Isolating it needs enough volume to overflow
// the socket buffers, so it is the slow one.
func TestE2EEventsWriteDeadlineReapsAWedgedPeer(t *testing.T) {
	if testing.Short() {
		t.Skip("needs to push megabytes into a wedged socket")
	}
	env, restore := newE2E(t)
	defer restore()
	oldChan, oldMax, oldWrite, oldKeep := evChanSize, evMaxEventBytes, evWriteTimeout, evKeepalive
	// A queue large enough that eviction-by-backpressure cannot be what reaps this.
	evChanSize, evMaxEventBytes = 4096, 64<<10
	evWriteTimeout, evKeepalive = 200*time.Millisecond, 50*time.Millisecond
	defer func() {
		evChanSize, evMaxEventBytes, evWriteTimeout, evKeepalive = oldChan, oldMax, oldWrite, oldKeep
	}()

	conn, err := net.Dial("tcp", env.proxy.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn, "GET /_switch/events HTTP/1.1\r\nHost: proxy\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	evWaitFor(t, "the wedged subscriber to register", 3*time.Second, func() bool { return evSubsNow() == 1 })

	blob := strings.Repeat("x", 60<<10)
	for i := 0; i < 200; i++ {
		publish("", "fat", map[string]any{"blob": blob})
	}

	evWaitFor(t, "the wedged peer to be reaped by the write deadline", 15*time.Second, func() bool {
		return evSubsNow() == 0
	})
	hub.mu.Lock()
	dropped := hub.dropped
	hub.mu.Unlock()
	if dropped != 0 {
		t.Fatalf("dropped = %d — the queue overflowed, so this proved backpressure eviction, not the write deadline", dropped)
	}
}
