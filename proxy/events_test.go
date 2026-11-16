package main

// Unit tests for the event hub (#59), exercised against the hub API directly so the
// concurrency contract is pinned down before any HTTP is in the way. The HTTP surface
// is covered by e2e_events_test.go.
//
// The load-bearing tests here are TestEventsZeroSubscribersNoAlloc (the literal
// acceptance criterion), TestEventsAdversarialSubscriberNeverBlocksPublisher (the whole
// reason the hub is shaped this way) and TestEventsAttachIsAtomicWithReplay (a race
// that becomes invisible once a handler is in the way).

import (
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- fixtures ----

// evReset clears the hub between tests. The hub is a package-level singleton (it has to
// be — buildHandler is rebuilt per e2e test), so a prior test's subscribers, ring and
// counters would otherwise leak into this one.
//
// It deliberately does NOT reset evSeq or evEpoch, so no test can come to depend on
// absolute event ids.
func evReset() {
	hub.mu.Lock()
	for _, s := range hub.subs {
		hub.removeLocked(s)
	}
	hub.subs = map[int64]*evSub{}
	hub.nextSub = 0
	hub.ring, hub.head, hub.ringBytes = nil, 0, 0
	hub.floor = atomic.LoadInt64(&evSeq) + 1
	hub.published, hub.dropped, hub.evicted = 0, 0, 0
	hub.mu.Unlock()
	atomic.StoreInt64(&evSubCount, 0)
	atomic.StoreInt64(&evArmedUntil, 0)
}

// evTest resets the hub and restores every tunable afterwards.
func evTest(t *testing.T) {
	t.Helper()
	ring, maxBytes, maxEvent := evRingSize, evRingMaxBytes, evMaxEventBytes
	chanSize, maxSubs, retry := evChanSize, evMaxSubs, evRetryHintMS
	keep, write, grace := evKeepalive, evWriteTimeout, evReplayGrace
	evReset()
	t.Cleanup(func() {
		// Two async publishers read these tunables from their own goroutines:
		// respTap aggregators, and an armed alert coalesce flush — a test can
		// observe the flush's dispatch frames and finish while the delivery
		// publish is still ahead of that goroutine. evTest arms AFTER
		// swapAlertTestWorld, so this cleanup runs BEFORE that fixture's own
		// quiesce (LIFO); join both publishers here, before restoring what they
		// read. Instant when neither ran.
		alertQuiesce(t)
		respTapSettle()
		evRingSize, evRingMaxBytes, evMaxEventBytes = ring, maxBytes, maxEvent
		evChanSize, evMaxSubs, evRetryHintMS = chanSize, maxSubs, retry
		evKeepalive, evWriteTimeout, evReplayGrace = keep, write, grace
		evReset()
	})
}

// evArm keeps the hub armed with no subscriber attached, so ring-shape tests are not
// perturbed by a subscriber queue filling up.
func evArm() { atomic.StoreInt64(&evArmedUntil, time.Now().Add(time.Hour).UnixNano()) }

// evDisarm expires the replay grace, as if the last subscriber left long ago.
func evDisarm() { atomic.StoreInt64(&evArmedUntil, time.Now().Add(-time.Second).UnixNano()) }

// evPublishN publishes n events and returns the seq of the last one.
func evPublishN(n int) int64 {
	for i := 0; i < n; i++ {
		publish("t", map[string]any{"i": i})
	}
	return atomic.LoadInt64(&evSeq)
}

// evDrain non-blockingly takes everything currently queued for a subscriber.
func evDrain(s *evSub) []int64 {
	var out []int64
	for {
		select {
		case f := <-s.ch:
			out = append(out, f.seq)
		default:
			return out
		}
	}
}

func evSeqsOf(frames []*evFrame) []int64 {
	out := make([]int64, 0, len(frames))
	for _, f := range frames {
		out = append(out, f.seq)
	}
	return out
}

func evBufferLen() int {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return len(hub.ring) - hub.head
}

func evRingFrames() []*evFrame {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return append([]*evFrame(nil), hub.ring[hub.head:]...)
}

func evAlive(s *evSub) bool {
	select {
	case <-s.done:
		return false
	default:
		return true
	}
}

type evParsed struct {
	id, event string
	env       evEnvelope
	nID       int
	nEvent    int
	nData     int
	raw       string
}

func evParseFrame(t *testing.T, b []byte) evParsed {
	t.Helper()
	p := evParsed{raw: string(b)}
	if !strings.HasSuffix(p.raw, "\n\n") {
		t.Fatalf("frame does not end in a blank line — SSE will never dispatch it: %q", p.raw)
	}
	for _, line := range strings.Split(strings.TrimSuffix(p.raw, "\n\n"), "\n") {
		switch {
		case strings.HasPrefix(line, "id: "):
			p.id, p.nID = strings.TrimPrefix(line, "id: "), p.nID+1
		case strings.HasPrefix(line, "event: "):
			p.event, p.nEvent = strings.TrimPrefix(line, "event: "), p.nEvent+1
		case strings.HasPrefix(line, "data: "):
			p.nData++
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &p.env); err != nil {
				t.Fatalf("data line is not valid JSON (%v): %q", err, line)
			}
		}
	}
	return p
}

// ---- the acceptance criteria ----

func TestEventsZeroSubscribersNoAlloc(t *testing.T) {
	evTest(t)
	payload := map[string]any{"lane": "fable", "from": "native", "to": "other"}
	n := testing.AllocsPerRun(1000, func() {
		if eventsArmed() {
			publish("switchover", payload)
		}
	})
	if n != 0 {
		t.Fatalf("cold path allocated %.1f times per call — the inference path must pay nothing when nobody is listening", n)
	}
	hub.mu.Lock()
	pub := hub.published
	hub.mu.Unlock()
	if pub != 0 {
		t.Fatalf("published = %d, want 0 — an unarmed hub must not record anything", pub)
	}
}

func TestEventsRingWarmAfterLastUnsubscribe(t *testing.T) {
	evTest(t)
	// This is the test that proves the arming window did not quietly gut replay:
	// the ring must keep filling while the only subscriber is away, because that gap
	// is exactly what Last-Event-ID exists to cover.
	sub, _, _ := hub.attach("first", "")
	last := evPublishN(2)
	evDrain(sub)
	hub.detach(sub)

	if !eventsArmed() {
		t.Fatal("hub disarmed the instant its last subscriber left — the replay grace is not being applied")
	}
	evPublishN(3)
	if got := evBufferLen(); got != 5 {
		t.Fatalf("buffer_len = %d, want 5 — events published during the gap were dropped", got)
	}

	sub2, backlog, gap := hub.attach("second", evID(last))
	defer hub.detach(sub2)
	if gap.Reason != "" {
		t.Fatalf("gap.reason = %q, want clean — the ring still holds every missed event", gap.Reason)
	}
	if got := evSeqsOf(backlog); len(got) != 3 || got[0] != last+1 || got[2] != last+3 {
		t.Fatalf("backlog seqs = %v, want exactly %d..%d", got, last+1, last+3)
	}
}

func TestEventsStaleRingAfterDisarm(t *testing.T) {
	evTest(t)
	// Once the grace expires, published events are dropped on the floor. A resume token
	// from before the disarm must NOT be told "clean" just because its id is still in
	// the ring — that would be a silent, permanent gap in scry's transcript.
	sub, _, _ := hub.attach("first", "")
	last := evPublishN(2)
	evDrain(sub)
	hub.detach(sub)
	evDisarm()

	publish("t", map[string]any{"lost": true}) // dropped: nobody is listening or coming back

	sub2, backlog, gap := hub.attach("second", evID(last))
	defer hub.detach(sub2)
	if gap.Reason != "stale" {
		t.Fatalf("gap.reason = %q, want \"stale\" — the hub cannot claim continuity across a disarm", gap.Reason)
	}
	if len(backlog) != 0 {
		t.Fatalf("backlog = %v, want empty — a stale ring must not be replayed as if it were continuous", evSeqsOf(backlog))
	}
	if got := evBufferLen(); got != 0 {
		t.Fatalf("buffer_len = %d, want 0 — the stale ring should have been dropped", got)
	}
}

func TestEventsTwoSubscribersEachGetEverything(t *testing.T) {
	evTest(t)
	evChanSize = 128
	a, _, _ := hub.attach("a", "")
	b, _, _ := hub.attach("b", "")
	defer hub.detach(a)
	defer hub.detach(b)

	first := atomic.LoadInt64(&evSeq) + 1
	evPublishN(50)

	for name, s := range map[string]*evSub{"a": a, "b": b} {
		got := evDrain(s)
		if len(got) != 50 {
			t.Fatalf("subscriber %s received %d of 50 events", name, len(got))
		}
		for i, seq := range got {
			if want := first + int64(i); seq != want {
				t.Fatalf("subscriber %s event %d has seq %d, want %d — fan-out is lossy or reordered", name, i, seq, want)
			}
		}
	}
}

func TestEventsAdversarialSubscriberNeverBlocksPublisher(t *testing.T) {
	evTest(t)
	evChanSize = 8
	wedged, _, _ := hub.attach("wedged", "") // never drained, on purpose

	done := make(chan struct{})
	go func() {
		defer close(done)
		evPublishN(evChanSize + 500)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		// Deliberately a watchdog rather than a wall-clock threshold: the claim is
		// "cannot block", not "is fast", and a threshold would be flaky under -race.
		t.Fatal("publisher stalled behind a subscriber that never reads — a hub that can do this can stall inference")
	}

	if evAlive(wedged) {
		t.Fatal("wedged subscriber survived: it must be evicted, not allowed to hold a slot forever")
	}
	hub.mu.Lock()
	dropped, evicted, subs := hub.dropped, hub.evicted, len(hub.subs)
	hub.mu.Unlock()
	if dropped < 1 || evicted < 1 {
		t.Fatalf("dropped=%d evicted=%d, want both >= 1 — backpressure must be counted, not silent", dropped, evicted)
	}
	if subs != 0 {
		t.Fatalf("subs = %d, want 0 — the evicted subscriber is still registered", subs)
	}
}

func TestEventsLiveSubscriberSurvivesAWedgedOne(t *testing.T) {
	evTest(t)
	evChanSize = 8
	reader, _, _ := hub.attach("reader", "")
	wedged, _, _ := hub.attach("wedged", "")
	defer hub.detach(reader)

	first := atomic.LoadInt64(&evSeq) + 1
	var got []int64
	for i := 0; i < 100; i++ {
		publish("t", map[string]any{"i": i})
		got = append(got, evDrain(reader)...) // drained inline, so it can never fill
	}

	if evAlive(wedged) {
		t.Fatal("wedged subscriber survived 100 events with a queue of 8")
	}
	if len(got) != 100 {
		t.Fatalf("healthy subscriber got %d of 100 events — one bad subscriber degraded a good one", len(got))
	}
	for i, seq := range got {
		if want := first + int64(i); seq != want {
			t.Fatalf("healthy subscriber event %d has seq %d, want %d", i, seq, want)
		}
	}
}

func TestEventsAttachIsAtomicWithReplay(t *testing.T) {
	evTest(t)
	// attach() must read the ring and join the fan-out under ONE lock acquisition. If it
	// does not, an event published in between lands in neither the backlog nor the
	// channel — a one-event hole that no straightforward test would ever show.
	evChanSize = 100000
	evRingSize = 100000
	evRingMaxBytes = 1 << 30

	for iter := 0; iter < 100; iter++ {
		evReset()
		warm, _, _ := hub.attach("warm", "") // keeps the hub armed
		evPublishN(5)
		base := atomic.LoadInt64(&evSeq)

		started := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				publish("t", map[string]any{"i": i})
				if i == 0 {
					close(started)
				}
			}
		}()
		<-started // guarantee the publisher is live while we attach

		sub, backlog, _ := hub.attach("racer", evID(base))
		wg.Wait()

		got := append(evSeqsOf(backlog), evDrain(sub)...)
		for i, seq := range got {
			if want := base + 1 + int64(i); seq != want {
				t.Fatalf("iter %d: event %d has seq %d, want %d (backlog=%d) — attach lost or duplicated an event published mid-join",
					iter, i, seq, want, len(backlog))
			}
		}
		if len(got) != 200 {
			t.Fatalf("iter %d: saw %d of 200 events across backlog+stream", iter, len(got))
		}
		hub.detach(sub)
		hub.detach(warm)
	}
}

// ---- resume contract ----

func TestEventsReplayLastEventID(t *testing.T) {
	evTest(t)
	evArm()
	first := atomic.LoadInt64(&evSeq) + 1
	evPublishN(10)

	sub, backlog, gap := hub.attach("s", evID(first+3))
	defer hub.detach(sub)
	if gap.Reason != "" {
		t.Fatalf("gap.reason = %q, want clean", gap.Reason)
	}
	got := evSeqsOf(backlog)
	if len(got) != 6 || got[0] != first+4 {
		t.Fatalf("backlog = %v, want the 6 events after seq %d", got, first+3)
	}
}

func TestEventsReplayWholeRingOnZero(t *testing.T) {
	evTest(t)
	evArm()
	evPublishN(4)
	sub, backlog, gap := hub.attach("s", evEpoch+"-0")
	defer hub.detach(sub)
	if gap.Reason != "" || len(backlog) != 4 {
		t.Fatalf("gap=%q backlog=%d, want a clean whole-ring replay of 4", gap.Reason, len(backlog))
	}
}

func TestEventsFreshConnectGetsLiveOnly(t *testing.T) {
	evTest(t)
	evArm()
	evPublishN(10)
	sub, backlog, gap := hub.attach("s", "")
	defer hub.detach(sub)
	if gap.Reason != "fresh" {
		t.Fatalf("gap.reason = %q, want \"fresh\"", gap.Reason)
	}
	if len(backlog) != 0 {
		t.Fatalf("backlog = %d, want 0 — a fresh connect must not be handed the whole ring unasked", len(backlog))
	}
}

func TestEventsReplayGapEvicted(t *testing.T) {
	evTest(t)
	evArm()
	evRingSize = 3
	first := atomic.LoadInt64(&evSeq) + 1
	evPublishN(10)

	sub, backlog, gap := hub.attach("s", evID(first)) // asks for seq 2.., long since evicted
	defer hub.detach(sub)
	if gap.Reason != "evicted" {
		t.Fatalf("gap.reason = %q, want \"evicted\" — a truncated replay must be flagged, not rendered as continuous", gap.Reason)
	}
	if len(backlog) != 3 {
		t.Fatalf("backlog = %d, want the 3 frames still buffered", len(backlog))
	}
	if gap.Oldest != first+7 {
		t.Fatalf("gap.oldest = %d, want %d so the client can see how much it lost", gap.Oldest, first+7)
	}
}

func TestEventsReplayEpochMismatch(t *testing.T) {
	evTest(t)
	evArm()
	evPublishN(5)
	// A restart resets seq. Without the epoch, a stored id of 1500 would match nothing,
	// replay would return empty, and the client would believe it was caught up forever.
	sub, backlog, gap := hub.attach("s", "deadbeef-1500")
	defer hub.detach(sub)
	if gap.Reason != "epoch" {
		t.Fatalf("gap.reason = %q, want \"epoch\"", gap.Reason)
	}
	if len(backlog) != 0 {
		t.Fatalf("backlog = %d, want 0 — seq numbers from another epoch are meaningless here", len(backlog))
	}
}

func TestEventsReplayMalformedID(t *testing.T) {
	evTest(t)
	evArm()
	evPublishN(3)
	for _, bad := range []string{"nonsense", evEpoch + "-abc", evEpoch + "--1"} {
		sub, backlog, gap := hub.attach("s", bad)
		if gap.Reason != "fresh" || len(backlog) != 0 {
			t.Fatalf("Last-Event-ID %q: gap=%q backlog=%d, want a clean fall back to live", bad, gap.Reason, len(backlog))
		}
		hub.detach(sub)
		evArm()
	}
}

func TestEventsIDMonotonicAcrossArmDisarm(t *testing.T) {
	evTest(t)
	evArm()
	evPublishN(3)
	evDisarm()
	evPublishN(5) // dropped
	evArm()
	evPublishN(3)

	var last int64
	for _, f := range evRingFrames() {
		if f.seq <= last {
			t.Fatalf("seq %d followed %d — ids must never repeat or go backwards within an epoch", f.seq, last)
		}
		last = f.seq
	}
}

// ---- bounds ----

func TestEventsRingByteCeiling(t *testing.T) {
	evTest(t)
	evArm()
	evRingSize = 100000 // make bytes the binding constraint
	evRingMaxBytes = 8 << 10
	evPublishN(500)

	hub.mu.Lock()
	bytes, n := hub.ringBytes, len(hub.ring)-hub.head
	hub.mu.Unlock()
	if bytes > evRingMaxBytes {
		t.Fatalf("ringBytes = %d > cap %d — the memory ceiling is aspirational, not enforced", bytes, evRingMaxBytes)
	}
	if n >= 500 {
		t.Fatalf("buffer_len = %d — nothing was evicted, so the byte bound never engaged", n)
	}
}

func TestEventsRingBackingArrayDoesNotGrow(t *testing.T) {
	evTest(t)
	evArm()
	evRingSize = 10
	evPublishN(5000)

	hub.mu.Lock()
	capacity, n := cap(hub.ring), len(hub.ring)-hub.head
	hub.mu.Unlock()
	if n != 10 {
		t.Fatalf("buffer_len = %d, want 10", n)
	}
	if capacity > 200 {
		t.Fatalf("ring backing array grew to cap %d over 5000 events — head is advancing without compaction, which leaks slowly and silently", capacity)
	}
}

func TestEventsFrameCapTruncates(t *testing.T) {
	evTest(t)
	evArm()
	evMaxEventBytes = 512
	publish("big", map[string]any{"blob": strings.Repeat("x", 8192)})

	frames := evRingFrames()
	if len(frames) != 1 {
		t.Fatalf("ring holds %d frames, want 1", len(frames))
	}
	p := evParseFrame(t, frames[0].b)
	data, ok := p.env.Data.(map[string]any)
	if !ok {
		t.Fatalf("data is %T, want an object — an oversized frame must degrade to a valid envelope, never a sliced JSON fragment", p.env.Data)
	}
	if data["truncated"] != true {
		t.Fatalf("data = %v, want truncated:true so the consumer knows something was dropped", data)
	}
	if n, _ := data["orig_bytes"].(float64); n <= 512 {
		t.Fatalf("orig_bytes = %v, want the real pre-truncation size", data["orig_bytes"])
	}
}

func TestEventsFrameShape(t *testing.T) {
	evTest(t)
	evArm()
	publish("switchover", map[string]any{"lane": "fable"})

	frames := evRingFrames()
	p := evParseFrame(t, frames[0].b)
	if p.nID != 1 || p.nEvent != 1 || p.nData != 1 {
		t.Fatalf("frame has %d id / %d event / %d data lines, want exactly one of each:\n%s", p.nID, p.nEvent, p.nData, p.raw)
	}
	if p.event != "switchover" {
		t.Fatalf("event: line = %q", p.event)
	}
	if p.env.Type != p.event {
		t.Fatalf("payload type = %q but event: line = %q — they are duplicated on purpose so a consumer using onmessage still works", p.env.Type, p.event)
	}
	if p.id != p.env.ID || p.id != evID(p.env.Seq) {
		t.Fatalf("id line %q, payload id %q, seq %d — all three must agree", p.id, p.env.ID, p.env.Seq)
	}
	if !strings.HasPrefix(p.id, evEpoch+"-") {
		t.Fatalf("id %q is not epoch-qualified — replay would misread it across a restart", p.id)
	}
	if _, err := time.Parse(time.RFC3339Nano, p.env.TS); err != nil {
		t.Fatalf("ts %q does not parse as RFC3339Nano: %v", p.env.TS, err)
	}
}

func TestEventsKindCannotForgeFrames(t *testing.T) {
	evTest(t)
	evArm()
	publish("evil\ndata: {\"injected\":true}\n\nevent: spoof", map[string]any{"ok": true})
	p := evParseFrame(t, evRingFrames()[0].b)
	if p.nData != 1 {
		t.Fatalf("a newline in the event name produced %d data lines — one publisher could forge frames for another", p.nData)
	}
}

// ---- subscriber cap ----

func TestEventsCapEvictsOldest(t *testing.T) {
	evTest(t)
	evMaxSubs = 2
	a, _, _ := hub.attach("a", "")
	b, _, _ := hub.attach("b", "")
	c, _, _ := hub.attach("c", "")
	defer hub.detach(b)
	defer hub.detach(c)

	// Never 503 at the cap: real scry reconnecting must always get in, and a wedged
	// subscriber must be the one that loses its slot.
	if evAlive(a) {
		t.Fatal("oldest subscriber survived the cap — a full hub would lock real scry out until something else gave up")
	}
	if !evAlive(b) || !evAlive(c) {
		t.Fatal("cap eviction took a subscriber that was not the oldest")
	}
	hub.mu.Lock()
	subs, evicted := len(hub.subs), hub.evicted
	hub.mu.Unlock()
	if subs != 2 || evicted != 1 {
		t.Fatalf("subs=%d evicted=%d, want 2 and 1", subs, evicted)
	}
}

// ---- status ----

func TestEventsStatusNoStMuDeadlock(t *testing.T) {
	evTest(t)
	evArm()
	evPublishN(2)

	// switchHandler builds its snapshot literal with st.mu held, so eventsStatus must
	// never reach for st.mu. hub.mu is a leaf; this line is the invariant.
	st.mu.Lock()
	s := eventsStatus()
	st.mu.Unlock()

	for _, k := range []string{"subscribers", "published", "dropped", "buffer_len", "evicted", "buffer_bytes", "capacity", "epoch", "armed"} {
		if _, ok := s[k]; !ok {
			t.Fatalf("status is missing %q: %v", k, s)
		}
	}
	if s["published"] != int64(2) {
		t.Fatalf("published = %v, want 2", s["published"])
	}
	if s["buffer_len"] != 2 {
		t.Fatalf("buffer_len = %v, want 2", s["buffer_len"])
	}
	if s["capacity"] != evMaxSubs {
		t.Fatalf("capacity = %v, want %d", s["capacity"], evMaxSubs)
	}
}
