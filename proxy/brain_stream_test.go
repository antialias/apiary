package main

// The proxy's subscription to the brain's /events (quota-model 64 §3), against
// fake brains that speak §2: `retry: 1000` on connect, then `id` / `event:
// preferred` / `data: <the document on one line>` / blank line, and a
// `: keepalive` comment when nothing else is sent. Durations are shrunk; the
// fakes pace themselves, nothing here sleeps the production 30 s.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// streamBrain runs the proxy's stream against a fake brain whose /events is
// events (n counts connections from 1); every other path is 404. The proxy's
// decision is fresh and restored after the test, as are the shrunk durations.
func streamBrain(t *testing.T, silence, backoffMin time.Duration, events func(w http.ResponseWriter, r *http.Request, n int)) *atomic.Int32 {
	t.Helper()
	return streamBrainWith(t, silence, backoffMin, nil, events)
}

// streamBrainWith is streamBrain whose other paths are other's (nil: 404).
func streamBrainWith(t *testing.T, silence, backoffMin time.Duration, other http.HandlerFunc, events func(w http.ResponseWriter, r *http.Request, n int)) *atomic.Int32 {
	t.Helper()
	var conns atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != tpath("/events") {
			if other != nil {
				other(w, r)
				return
			}
			http.NotFound(w, r)
			return
		}
		events(w, r, int(conns.Add(1)))
	}))
	t.Cleanup(resetQuotaState())
	oldBrain, oldPref := brainBase, lan.pref
	oldSilence, oldMin, oldMax := brainSilence, brainBackoffMin, brainBackoffMax
	brainBase, lan.pref = srv.URL, &decision{}
	brainSilence, brainBackoffMin, brainBackoffMax = silence, backoffMin, 4*backoffMin
	lan.classRowsMu.Lock()
	oldRows, oldSource := lan.classRows, lan.classRowsSource
	lan.classRowsMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); lan.runBrainStream(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
		srv.Close()
		prewarmWG.Wait()
		stopPrewarmTimers()
		brainBase, lan.pref = oldBrain, oldPref
		brainSilence, brainBackoffMin, brainBackoffMax = oldSilence, oldMin, oldMax
		lan.classRowsMu.Lock()
		lan.classRows, lan.classRowsSource = oldRows, oldSource
		lan.classRowsMu.Unlock()
	})
	return &conns
}

// sseOpen answers like the brain on connect.
func sseOpen(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(200)
	fmt.Fprint(w, "retry: 1000\n\n")
	w.(http.Flusher).Flush()
}

// ssePreferred sends one `preferred` event carrying doc on one line.
func ssePreferred(t *testing.T, w http.ResponseWriter, seq int, doc map[string]any) {
	b, err := json.Marshal(doc)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Fprintf(w, "id: boot1-%d\nevent: preferred\ndata: %s\n\n", seq, b)
	w.(http.Flusher).Flush()
}

func prefState() (ok bool, account string, heard time.Time) {
	lan.pref.mu.RLock()
	defer lan.pref.mu.RUnlock()
	return lan.pref.ok, lan.pref.account, lan.pref.heardAt
}

// waitUntil polls cond (a test-side check, not a production wait) for up to 5 s.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// Every `preferred` event goes through the decode, in order: the second
// document replaces the first.
func TestBrainStreamDecodesEachEvent(t *testing.T) {
	second := make(chan struct{})
	streamBrain(t, time.Minute, 10*time.Millisecond, func(w http.ResponseWriter, r *http.Request, n int) {
		sseOpen(w)
		d := brainPreferred(t)
		d["account"] = "first"
		ssePreferred(t, w, 1, d)
		select {
		case <-second:
		case <-r.Context().Done():
			return
		}
		d["account"] = "second"
		ssePreferred(t, w, 2, d)
		<-r.Context().Done()
	})
	waitUntil(t, "the first document", func() bool { ok, a, _ := prefState(); return ok && a == "first" })
	close(second)
	waitUntil(t, "the second document", func() bool { ok, a, _ := prefState(); return ok && a == "second" })
	lan.pref.mu.RLock()
	routes, accounts, ledger := len(lan.pref.routes), len(lan.pref.accounts), lan.pref.ledger
	lan.pref.mu.RUnlock()
	if routes == 0 || accounts == 0 || ledger == nil {
		t.Fatalf("the event's document was not decoded in full: routes=%d accounts=%d ledger=%v", routes, accounts, ledger)
	}
}

// Keepalives alone keep the brain reachable well past brainSilence.
func TestBrainStreamKeepaliveKeepsViewLive(t *testing.T) {
	const silence = 150 * time.Millisecond
	conns := streamBrain(t, silence, 10*time.Millisecond, func(w http.ResponseWriter, r *http.Request, n int) {
		sseOpen(w)
		ssePreferred(t, w, 1, brainPreferred(t))
		tick := time.NewTicker(silence / 3)
		defer tick.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-tick.C:
				fmt.Fprint(w, ": keepalive\n")
				w.(http.Flusher).Flush()
			}
		}
	})
	waitUntil(t, "the first document", func() bool { ok, _, _ := prefState(); return ok })
	end := time.Now().Add(5 * silence)
	for time.Now().Before(end) {
		ok, _, heard := prefState()
		if !ok {
			t.Fatal("the view went stale while keepalives were arriving")
		}
		if routerViewStale(heard, time.Now()) {
			t.Fatalf("heardAt %s ago, past brainSilence, with keepalives arriving", time.Since(heard))
		}
		time.Sleep(silence / 10)
	}
	if c := conns.Load(); c != 1 {
		t.Fatalf("%d connections, want the one stream held open", c)
	}
}

// brainSilence with nothing at all closes the stream and makes the view stale,
// with the "cannot reach the brain" problem; the proxy then reconnects.
func TestBrainStreamSilenceMakesViewStale(t *testing.T) {
	const silence = 100 * time.Millisecond
	var closedBySilence atomic.Bool
	conns := streamBrain(t, silence, 10*time.Millisecond, func(w http.ResponseWriter, r *http.Request, n int) {
		if n > 1 {
			<-r.Context().Done() // the brain stays away: the reconnect hangs until the proxy gives up
			return
		}
		sseOpen(w)
		ssePreferred(t, w, 1, brainPreferred(t))
		<-r.Context().Done() // nothing more, ever; the proxy must hang up
		closedBySilence.Store(true)
	})
	waitUntil(t, "the first document", func() bool { ok, _, _ := prefState(); return ok })
	start := time.Now()
	waitUntil(t, "a stale view", func() bool { ok, _, _ := prefState(); return !ok })
	if el := time.Since(start); el > 10*silence {
		t.Fatalf("stale after %s, want about brainSilence (%s)", el, silence)
	}
	waitUntil(t, "the proxy to close the silent stream", closedBySilence.Load)
	problems := lan.proxyProblems()
	if len(problems) != 1 || !strings.Contains(problems[0], "cannot reach the brain") {
		t.Fatalf("problems = %q, want the cannot-reach-the-brain problem", problems)
	}
	if _, known := lan.routerDry(); known {
		t.Fatal("routerDry claims to know the fleet on a stale view")
	}
	waitUntil(t, "a reconnect", func() bool { return conns.Load() >= 2 })
}

// A drop of a connection that held (a document, then open past brainSilence:
// a brain restart after a healthy stream) is followed by a reconnect at once,
// and the new stream's document makes the brain reachable again.
func TestBrainStreamReconnectsAfterDrop(t *testing.T) {
	const silence = 200 * time.Millisecond
	var at [3]atomic.Int64
	var dropped atomic.Int64
	// A long backoff, so a reconnect that waited for it would show.
	conns := streamBrain(t, silence, 5*time.Second, func(w http.ResponseWriter, r *http.Request, n int) {
		if n <= len(at) {
			at[n-1].Store(time.Now().UnixNano())
		}
		sseOpen(w)
		d := brainPreferred(t)
		d["account"] = fmt.Sprintf("conn%d", n)
		ssePreferred(t, w, n, d)
		if n == 1 {
			// Healthy for longer than the watchdog, then the brain drops it.
			for end := time.Now().Add(silence + silence/2); time.Now().Before(end); {
				time.Sleep(silence / 4)
				fmt.Fprint(w, ": keepalive\n")
				w.(http.Flusher).Flush()
			}
			dropped.Store(time.Now().UnixNano())
			return
		}
		<-r.Context().Done()
	})
	waitUntil(t, "the second connection's document", func() bool { ok, a, _ := prefState(); return ok && a == "conn2" })
	if c := conns.Load(); c != 2 {
		t.Fatalf("%d connections, want 2", c)
	}
	// "At once": no backoff after a connection that held.
	if gap := time.Duration(at[1].Load() - dropped.Load()); gap >= time.Second {
		t.Fatalf("reconnect after %s, want at once (the backoff is 5 s)", gap)
	}
}

// A brain that answers every connect with one document and a close is not
// redialled in a loop: a connection shorter than brainSilence does not reset
// the backoff, so the waits double as for a refused connect.
func TestBrainStreamOneShotBrainBacksOff(t *testing.T) {
	const backoff = 100 * time.Millisecond
	var at [3]atomic.Int64
	conns := streamBrain(t, time.Minute, backoff, func(w http.ResponseWriter, r *http.Request, n int) {
		if n <= len(at) {
			at[n-1].Store(time.Now().UnixNano())
		}
		sseOpen(w)
		ssePreferred(t, w, n, brainPreferred(t))
	})
	waitUntil(t, "a third connection", func() bool { return conns.Load() >= 3 })
	g1 := time.Duration(at[1].Load() - at[0].Load())
	g2 := time.Duration(at[2].Load() - at[1].Load())
	if g1 < backoff || g2 < 2*backoff {
		t.Fatalf("redials after %s and %s, want at least %s and %s", g1, g2, backoff, 2*backoff)
	}
}

// heldConnection needs both a document and a lifetime of brainSilence.
func TestHeldConnection(t *testing.T) {
	old := brainSilence
	brainSilence = 30 * time.Second
	t.Cleanup(func() { brainSilence = old })
	for _, c := range []struct {
		delivered bool
		lasted    time.Duration
		want      bool
	}{
		{true, 31 * time.Second, true},
		{true, 30 * time.Second, true},
		{true, 50 * time.Millisecond, false}, // one document, then a close
		{false, time.Hour, false},            // open, keepalives only
		{false, 0, false},                    // refused
	} {
		if got := heldConnection(c.delivered, c.lasted); got != c.want {
			t.Errorf("heldConnection(%v, %s) = %v, want %v", c.delivered, c.lasted, got, c.want)
		}
	}
}

// A brain that keeps failing is retried with a doubling wait that stops at
// brainBackoffMax, and a connection that held starts the count again.
func TestBrainStreamBackoffCaps(t *testing.T) {
	oldMin, oldMax := brainBackoffMin, brainBackoffMax
	brainBackoffMin, brainBackoffMax = time.Second, 30*time.Second
	t.Cleanup(func() { brainBackoffMin, brainBackoffMax = oldMin, oldMax })
	var got []time.Duration
	w := time.Duration(0)
	w = nextBackoff(w, true)
	got = append(got, w)
	for range 8 {
		w = nextBackoff(w, false)
		got = append(got, w)
	}
	w = nextBackoff(w, true)
	got = append(got, w)
	w = nextBackoff(w, false)
	got = append(got, w)
	want := []time.Duration{0, 1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second,
		30 * time.Second, 30 * time.Second, 30 * time.Second, 0, 1 * time.Second}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("backoff = %v, want %v", got, want)
	}
}

// A brain without /events (404) is retried, backing off, and never makes the
// view reachable: there is no fallback to /preferred.
func TestBrainStreamRefusedIsRetriedWithoutFallback(t *testing.T) {
	conns := streamBrain(t, time.Minute, 10*time.Millisecond, func(w http.ResponseWriter, r *http.Request, n int) {
		http.NotFound(w, r)
	})
	waitUntil(t, "several attempts", func() bool { return conns.Load() >= 4 })
	if ok, _, _ := prefState(); ok {
		t.Fatal("the view is reachable with no /events")
	}
}

// A document the proxy cannot use marks the view not-ok, as a bad pull did,
// keeps the last good state, and the next good event on the same stream
// restores it.
func TestBrainStreamBadDocumentMarksNotOK(t *testing.T) {
	step := make(chan struct{})
	conns := streamBrain(t, time.Minute, 10*time.Millisecond, func(w http.ResponseWriter, r *http.Request, n int) {
		sseOpen(w)
		good := brainPreferred(t)
		good["account"] = "good"
		ssePreferred(t, w, 1, good)
		for _, send := range []func(){
			func() {
				bad := brainPreferred(t)
				delete(bad, "routes")
				bad["account"] = "bad"
				ssePreferred(t, w, 2, bad)
			},
			func() {
				good["account"] = "good again"
				ssePreferred(t, w, 4, good)
			},
		} {
			select {
			case <-step:
			case <-r.Context().Done():
				return
			}
			send()
		}
		<-r.Context().Done()
	})
	waitUntil(t, "the good document", func() bool { ok, a, _ := prefState(); return ok && a == "good" })
	step <- struct{}{}
	waitUntil(t, "not ok on a document without routes", func() bool { ok, _, _ := prefState(); return !ok })
	if _, a, _ := prefState(); a != "good" {
		t.Fatalf("account = %q, want the last good state kept", a)
	}
	// Unparseable JSON is a decode failure too.
	lan.pref.mu.Lock()
	lan.pref.ok = true
	lan.pref.mu.Unlock()
	lan.applyPreferred(strings.NewReader("{not json"))
	if ok, a, _ := prefState(); ok || a != "good" {
		t.Fatalf("after unparseable JSON: ok=%v account=%q, want not ok on the last good state", ok, a)
	}
	step <- struct{}{}
	waitUntil(t, "ok again on the next good document", func() bool { ok, a, _ := prefState(); return ok && a == "good again" })
	if c := conns.Load(); c != 1 {
		t.Fatalf("%d connections: a bad document must not drop the stream", c)
	}
}

// The parser: multi-line data, comments, every line ending, a field without a
// value, and an event cut off by the end of the stream.
func TestReadSSE(t *testing.T) {
	in := "retry: 1000\r\n\r\n" +
		": keepalive\n" +
		"id: b-1\r\nevent: preferred\r\ndata: {\"a\":\r\ndata:1}\r\n\r\n" +
		"event: preferred\rdata: x\r\r" +
		"data\n\n" +
		"event: other\ndata: y\n\n" +
		"event: preferred\ndata: cut off"
	type ev struct{ event, data string }
	var got []ev
	// One byte at a time, so a "\r\n" split across reads is exercised.
	if err := readSSE(&oneByte{s: in}, func(e, d string) { got = append(got, ev{e, d}) }); err != nil {
		t.Fatal(err)
	}
	want := []ev{{"preferred", "{\"a\":\n1}"}, {"preferred", "x"}, {"message", ""}, {"other", "y"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("events = %q, want %q", got, want)
	}
}

type oneByte struct {
	s string
	i int
}

func (o *oneByte) Read(p []byte) (int, error) {
	if o.i >= len(o.s) {
		return 0, io.EOF
	}
	p[0] = o.s[o.i]
	o.i++
	return 1, nil
}

// keepaliveEvents is a healthy brain: one document, then a keepalive every
// period until the proxy hangs up.
func keepaliveEvents(t *testing.T, period time.Duration) func(w http.ResponseWriter, r *http.Request, n int) {
	return func(w http.ResponseWriter, r *http.Request, n int) {
		sseOpen(w)
		ssePreferred(t, w, 1, brainPreferred(t))
		tick := time.NewTicker(period)
		defer tick.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-tick.C:
				fmt.Fprint(w, ": keepalive\n")
				w.(http.Flusher).Flush()
			}
		}
	}
}

// Reports run off the stream's reader: a brain whose /reports hangs longer
// than brainSilence, while /events keeps talking, stays reachable. The
// document wakes the sender and the connection's first event queues the
// resync; neither posts on the reader.
func TestBrainStreamReportPostDoesNotBlockReader(t *testing.T) {
	const silence = 150 * time.Millisecond
	var posts atomic.Int32
	release := make(chan struct{})
	streamBrainWith(t, silence, 10*time.Millisecond, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != tpath("/reports") {
			http.NotFound(w, r)
			return
		}
		posts.Add(1)
		select { // hang far past brainSilence
		case <-release:
		case <-r.Context().Done():
		}
		http.Error(w, "gone", 503)
	}, keepaliveEvents(t, silence/3))
	t.Cleanup(func() { close(release) }) // runs before streamBrain's cleanup closes the server
	// One entry the brain has not accepted, so the sender has work.
	lan.ledgerMu.Lock()
	lan.ledgerOwn = append(lan.ledgerOwn, &ownEntry{e: ledgerEntry{ID: "proxy-hang", At: unixSeconds(time.Now())}})
	lan.ledgerMu.Unlock()
	runReporter(t) // stopped first: its hung post is cancelled
	waitUntil(t, "the document and the report post", func() bool { ok, _, _ := prefState(); return ok && posts.Load() >= 1 })
	end := time.Now().Add(4 * silence)
	for time.Now().Before(end) {
		// heardAt moves only while the reader reads: a reader stuck in the
		// post would leave it behind, and its watchdog would drop the stream
		// the moment the post gave up.
		if ok, _, heard := prefState(); !ok || routerViewStale(heard, time.Now()) {
			t.Fatalf("the view went stale (ok=%v, heard %s ago) while /reports hung and /events kept talking", ok, time.Since(heard))
		}
		time.Sleep(silence / 10)
	}
}

// tokenBrain serves /token for the prewarm tests: expires(n) is the n-th
// fetch's expiry (n from 1); a zero time makes that fetch fail (502). The
// warm set, timers and cached tokens are restored after.
func tokenBrain(t *testing.T, expires func(n int) time.Time) *atomic.Int32 {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != tpath("/token") {
			http.NotFound(w, r)
			return
		}
		n := int(hits.Add(1))
		exp := expires(n)
		if exp.IsZero() {
			http.Error(w, "refresh failed", http.StatusBadGateway)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"access_token": fmt.Sprintf("tok-%d", n), "expires_at": float64(exp.Unix())})
	}))
	oldBrain := brainBase
	brainBase = srv.URL
	lan.tokMu.Lock()
	oldTokens := lan.tokens
	lan.tokens = map[string]tokEntry{}
	lan.tokMu.Unlock()
	t.Cleanup(func() {
		stopPrewarmTimers()
		prewarmWG.Wait()
		srv.Close()
		brainBase = oldBrain
		lan.tokMu.Lock()
		lan.tokens = oldTokens
		lan.tokMu.Unlock()
	})
	return &hits
}

// A token fetched with just over the margin left is fetched again when it
// reaches the margin, with no document and no request.
func TestPrewarmRefreshesAtTheMargin(t *testing.T) {
	hits := tokenBrain(t, func(n int) time.Time {
		if n == 1 {
			return time.Now().Add(tokenRefreshMargin + 2*time.Second) // whole seconds on the wire
		}
		return time.Now().Add(time.Hour)
	})
	lan.setWarmSet(map[string]bool{"warm-acct": true})
	if tok := lan.getToken("warm-acct"); tok != "tok-1" {
		t.Fatalf("first token %q", tok)
	}
	// The fake brain counts a fetch as its request arrives, before the proxy
	// has cached the reply, so the wait is on the cache itself.
	waitUntil(t, "the timer's refresh in the cache", func() bool {
		lan.tokMu.Lock()
		defer lan.tokMu.Unlock()
		return lan.tokens["warm-acct"].token == "tok-2"
	})
	if n := hits.Load(); n != 2 {
		t.Fatalf("token fetches = %d, want 2: the first and one refresh", n)
	}
}

// A token handed out already inside the margin arms nothing.
func TestPrewarmInsideMarginArmsNothing(t *testing.T) {
	tokenBrain(t, func(int) time.Time { return time.Now().Add(tokenRefreshMargin - time.Minute) })
	lan.setWarmSet(map[string]bool{"near-acct": true})
	lan.getToken("near-acct")
	lan.warmMu.Lock()
	_, armed := lan.warmTimers["near-acct"]
	lan.warmMu.Unlock()
	if armed {
		t.Fatal("a timer was armed for a token already inside the margin")
	}
}

// An account that leaves the warm set has no live timer; one never in it is
// never armed.
func TestPrewarmTimerStopsWhenAccountLeaves(t *testing.T) {
	tokenBrain(t, func(int) time.Time { return time.Now().Add(time.Hour) })
	lan.setWarmSet(map[string]bool{"lane-a": true, "lane-b": true})
	lan.getToken("lane-a")
	lan.getToken("lane-b")
	lan.getToken("not-routed")
	lan.warmMu.Lock()
	ta, tb := lan.warmTimers["lane-a"], lan.warmTimers["lane-b"]
	_, stray := lan.warmTimers["not-routed"]
	lan.warmMu.Unlock()
	if ta == nil || tb == nil || stray {
		t.Fatalf("timers: lane-a=%v lane-b=%v not-routed=%v, want the two routed accounts only", ta != nil, tb != nil, stray)
	}
	lan.setWarmSet(map[string]bool{"lane-b": true})
	lan.warmMu.Lock()
	_, still := lan.warmTimers["lane-a"]
	lan.warmMu.Unlock()
	if still || ta.Stop() {
		t.Fatal("lane-a left the warm set but its timer is still live")
	}
	if !tb.Stop() {
		t.Fatal("lane-b's timer was stopped though it is still routed")
	}
}

// An account that leaves the warm set and rejoins it with its cached token
// still fresh gets its timer back from that token, with no fetch: getToken's
// cache hit (the per-document prewarm) arms nothing.
func TestPrewarmTimerRearmsWhenAccountRejoins(t *testing.T) {
	hits := tokenBrain(t, func(int) time.Time { return time.Now().Add(time.Hour) })
	lan.setWarmSet(map[string]bool{"lane-a": true})
	lan.getToken("lane-a")
	lan.setWarmSet(map[string]bool{})
	lan.warmMu.Lock()
	_, kept := lan.warmTimers["lane-a"]
	lan.warmMu.Unlock()
	if kept {
		t.Fatal("lane-a left the warm set but kept its timer")
	}
	lan.setWarmSet(map[string]bool{"lane-a": true})
	lan.getToken("lane-a")
	lan.warmMu.Lock()
	back := lan.warmTimers["lane-a"]
	lan.warmMu.Unlock()
	if back == nil {
		t.Fatal("lane-a rejoined with a fresh cached token but has no prewarm timer")
	}
	if n := hits.Load(); n != 1 {
		t.Fatalf("token fetches = %d, want 1: rejoining arms from the cache, it does not fetch", n)
	}
}

// shrinkPrewarmRetry sets the timed refresh's retry waits for one test.
func shrinkPrewarmRetry(t *testing.T, lo, hi time.Duration) {
	t.Helper()
	oldMin, oldMax := prewarmRetryMin, prewarmRetryMax
	prewarmRetryMin, prewarmRetryMax = lo, hi
	t.Cleanup(func() { prewarmRetryMin, prewarmRetryMax = oldMin, oldMax })
}

// The retry schedule from the margin with the production waits: doubling from
// 30 s to 2 min, never later than halfway to expiry (the last lands 37.5 s
// before it), none once under a minute is left.
func TestNextPrewarmRetry(t *testing.T) {
	shrinkPrewarmRetry(t, 30*time.Second, 2*time.Minute)
	left := tokenRefreshMargin
	var got []time.Duration
	for w := time.Duration(0); ; {
		w = nextPrewarmRetry(w, left)
		if w == 0 {
			break
		}
		got = append(got, w)
		left -= w
	}
	want := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 2 * time.Minute, 2 * time.Minute, 75 * time.Second, 37500 * time.Millisecond}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("retries = %v, want %v", got, want)
	}
	if left <= 0 {
		t.Fatalf("the last retry lands %s after expiry", -left)
	}
	if w := nextPrewarmRetry(0, 0); w != 0 {
		t.Fatalf("retry %s for an expired (or absent) token, want none", w)
	}
}

// A timed refresh that fails is tried again, with no document and no request,
// and the retry's token replaces the cached one.
func TestPrewarmRetriesAFailedRefresh(t *testing.T) {
	shrinkPrewarmRetry(t, 50*time.Millisecond, 200*time.Millisecond)
	hits := tokenBrain(t, func(n int) time.Time {
		switch n {
		case 1:
			return time.Now().Add(tokenRefreshMargin + 2*time.Second)
		case 2, 3:
			return time.Time{} // the timer's refresh and the first retry fail
		}
		return time.Now().Add(time.Hour)
	})
	lan.setWarmSet(map[string]bool{"warm-acct": true})
	if tok := lan.getToken("warm-acct"); tok != "tok-1" {
		t.Fatalf("first token %q", tok)
	}
	waitUntil(t, "the retried refresh in the cache", func() bool {
		lan.tokMu.Lock()
		defer lan.tokMu.Unlock()
		return lan.tokens["warm-acct"].token == "tok-4"
	})
	if n := hits.Load(); n != 4 {
		t.Fatalf("token fetches = %d, want 4: the first, a failed refresh, a failed retry, a good retry", n)
	}
	lan.warmMu.Lock()
	_, armed := lan.warmTimers["warm-acct"]
	lan.warmMu.Unlock()
	if !armed {
		t.Fatal("no timer armed from the retried token")
	}
}

// A failed refresh's retry is the account's one timer, so it stops when the
// account leaves the warm set.
func TestPrewarmRetryStopsWhenAccountLeaves(t *testing.T) {
	shrinkPrewarmRetry(t, time.Minute, time.Minute)
	hits := tokenBrain(t, func(n int) time.Time {
		if n == 1 {
			return time.Now().Add(tokenRefreshMargin + 2*time.Second)
		}
		return time.Time{}
	})
	lan.setWarmSet(map[string]bool{"lane-a": true})
	lan.getToken("lane-a")
	waitUntil(t, "a retry armed after the failed refresh", func() bool {
		lan.warmMu.Lock()
		defer lan.warmMu.Unlock()
		return hits.Load() == 2 && lan.warmTimers["lane-a"] != nil
	})
	lan.warmMu.Lock()
	retry := lan.warmTimers["lane-a"]
	lan.warmMu.Unlock()
	lan.setWarmSet(map[string]bool{})
	if retry.Stop() {
		t.Fatal("lane-a left the warm set but its retry timer is still live")
	}
}
