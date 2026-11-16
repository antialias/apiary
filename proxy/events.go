package main

// ---- event hub (#59): GET /_switch/events ----
//
// An in-process fan-out of what the proxy sees, published as Server-Sent Events so
// scry (services/routing-observability) can render it live instead of polling
// /_switch/status every 5s.
//
// The direction is deliberate: scry SUBSCRIBES to us; we never push to scry. This proxy
// sits in the inference path for every client, so it must not take a
// dependency on a consumer being reachable.
//
// SSE rather than a WebSocket because this hop is server-to-client only, the stdlib has
// no WebSocket, and this binary keeps go.mod at zero requires. Last-Event-ID resumption
// is in the protocol, which is the feature we actually want. The scry-server -> browser
// hop is a separate concern and can use whatever it likes.
//
// A browser EventSource cannot set an Authorization header, so a browser can never
// consume this endpoint directly — the Next server always mediates, and there is
// deliberately NO query-param auth bypass (it would put the client secret in URLs, logs
// and Referer headers, which is exactly what scry's server-only posture forbids).
//
// THE load-bearing property: publish() must never block. See the locking discipline
// note on (*evHub).append.

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Tunables are source vars, not env vars, following alertCoalesce (main.go) — tests
// shrink them. There is deliberately no hub-level kill switch: the hub is passive (at
// rest eventsArmed() is two atomic loads), the risk lives entirely with publishers, and
// a SWITCH_EVENTS=off would be the lever you reach for in an incident and the wrong one
// — it disables the observability you most want while the incident is happening.
//
// evKeepalive in particular has no "0"/off value ON PURPOSE. The server sets no
// IdleTimeout (main.go), and a write deadline only fires on a write ATTEMPT, so the
// keepalive tick is the only thing that ever notices a wedged peer. Disabling it would
// let dead subscribers hold slots forever.
var (
	evRingSize      = 2000             // frames retained for replay
	evRingMaxBytes  = 16 << 20         // the REAL ring ceiling — count alone guarantees nothing
	evMaxEventBytes = 32 << 10         // per rendered frame; also keeps us under any reader's line limit
	evChanSize      = 64               // per-subscriber queue depth
	evMaxSubs       = 8                // counts SERVER PROCESSES (scry is one), not browser tabs
	evKeepalive     = 15 * time.Second // also the liveness probe — see above
	evWriteTimeout  = 10 * time.Second
	evReplayGrace   = 5 * time.Minute // how long the ring stays warm after the last subscriber leaves
	evRetryHintMS   = 2000            // SSE reconnect backoff hint
)

// evFrame is one rendered SSE frame plus what replay needs. b is built once at publish
// and never mutated: the ring and every subscriber queue share the same array.
type evFrame struct {
	seq int64
	b   []byte
}

// evSub is one connected subscriber. Channel invariant, relied on everywhere:
// ch is created by attach and NEVER closed; done is closed EXACTLY once, by
// removeLocked, which is idempotent because it first tests map membership. That
// eliminates every send-on-closed and close-of-closed hazard.
type evSub struct {
	id   int64
	ch   chan *evFrame
	done chan struct{}
	at   time.Time
	addr string
}

type evHub struct {
	mu        sync.Mutex
	subs      map[int64]*evSub
	nextSub   int64
	ring      []*evFrame // live frames are ring[head:]
	head      int
	ringBytes int
	floor     int64 // smallest seq the ring can still serve; evSeq+1 when empty
	published int64
	dropped   int64 // events that failed to enqueue
	evicted   int64 // subscribers forcibly removed
}

// evGap tells a reconnecting subscriber whether its resume point was satisfiable.
// Reason: "" clean | "fresh" | "epoch" | "evicted" | "stale".
type evGap struct {
	Reason string `json:"reason"`
	After  int64  `json:"after"`
	Oldest int64  `json:"oldest"`
	Next   int64  `json:"next"`
}

// evEnvelope is the JSON body of every frame. Type is duplicated into the payload on
// purpose: codex.go already states the convention ("event: lines are redundant — the
// data payload carries type") and parseSSE dispatches on it. It also survives the
// commonest consumer bug, using onmessage, which never sees the event: line.
//
// Data is NESTED rather than spread so a publisher's arbitrary keys can never shadow
// id/seq/ts/type.
type evEnvelope struct {
	ID   string `json:"id"`
	Seq  int64  `json:"seq"`
	TS   string `json:"ts"`
	Type string `json:"type"`
	Data any    `json:"data"`
}

// The hub is a package-level singleton, sibling to `st`. Required, not stylistic:
// buildHandler() is called by main() AND by every e2e test (and again mid-test by
// rebuildProxy), so a per-handler hub would orphan live subscribers on rebuild and,
// worse, disconnect publishers — fireSwitchAlert has no handle on a hub object.
var hub = &evHub{subs: map[int64]*evSub{}, floor: 1}

// evEpoch qualifies every event id so ids cannot be misread across a restart. Without
// it: scry stores Last-Event-ID 1500, we restart and are at seq 3, nothing has seq>1500,
// replay returns nothing, and scry believes it is caught up — a silent permanent gap.
// Not a secret; it only has to differ per process.
var evEpoch = strconv.FormatInt(time.Now().UnixNano(), 36)

var (
	evSeq        int64 // monotonic within an epoch; never reset, not even by tests
	evSubCount   int64 // atomic mirror of len(hub.subs); written under hub.mu, read lock-free
	evArmedUntil int64 // unix-nanos; the ring stays warm this long after the last subscriber leaves
)

var evPing = []byte(": ping\n\n")

// eventsArmed reports whether anything could consume a published event: a live
// subscriber, or a recent one still inside the replay window.
//
// This is the gate the inference path calls BEFORE building a payload. Taps must be
// written as
//
//	if eventsArmed() { publish("turn", buildTurn(...)) }
//
// because the expensive allocation is the caller's payload construction, not ours.
//
// The window (rather than a bare subscriber count) is what reconciles "zero subscribers
// costs no allocation" with "the ring must be warm for replay": replay exists for the
// subscriber that left 40 seconds ago and is coming back, so arming and replay are the
// same window by construction. Two atomic loads and a vDSO clock read — zero heap.
func eventsArmed() bool {
	if atomic.LoadInt64(&evSubCount) > 0 {
		return true
	}
	return time.Now().UnixNano() < atomic.LoadInt64(&evArmedUntil)
}

// publish fans one event out to every current subscriber and appends it to the replay
// ring. It NEVER blocks. Safe to call from any goroutine, including the request path.
func publish(kind string, data any) {
	if !eventsArmed() {
		return
	}
	seq := atomic.AddInt64(&evSeq, 1)
	hub.append(&evFrame{seq: seq, b: renderFrame(seq, kind, data)}) // marshal is lock-free
}

func evID(seq int64) string { return evEpoch + "-" + strconv.FormatInt(seq, 10) }

// evSanitizeKind keeps the "event:" line single-line. Kinds are internal constants
// today, but a newline here would let a publisher forge frames, so it is not left to
// convention. ContainsAny is cheap and false in the normal case.
func evSanitizeKind(kind string) string {
	if kind == "" {
		return "message"
	}
	if strings.ContainsAny(kind, "\r\n") {
		return strings.NewReplacer("\r", "", "\n", "").Replace(kind)
	}
	return kind
}

// renderFrame builds the complete wire frame. json.Marshal escapes newlines inside
// strings and emits none at top level, so "data:" is always exactly one line — no
// multi-line data handling is needed anywhere.
func renderFrame(seq int64, kind string, data any) []byte {
	kind = evSanitizeKind(kind)
	ev := evEnvelope{
		ID:   evID(seq),
		Seq:  seq,
		TS:   time.Now().UTC().Format(time.RFC3339Nano),
		Type: kind,
		Data: data,
	}
	b, err := json.Marshal(ev)
	if err != nil || len(b) > evMaxEventBytes {
		// Backstop, not the primary defence — publishers pre-truncate anything they know
		// can be large. Emit an envelope-only stub: never slice valid JSON at a byte
		// boundary, which would hand the consumer a parser-killing fragment. Keeping
		// frames small is also what stops a downstream bufio.Scanner hitting its token
		// limit and mistaking truncation for a clean end of stream.
		ev.Data = map[string]any{"truncated": true, "orig_bytes": len(b), "error": evErrStr(err)}
		b, _ = json.Marshal(ev)
	}
	return []byte("id: " + ev.ID + "\nevent: " + kind + "\ndata: " + string(b) + "\n\n")
}

func evErrStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// append records a frame and fans it out.
//
// LOCKING DISCIPLINE — hub.mu is a strict LEAF lock. Under it this function does only:
// integer arithmetic, a slice append plus amortized-O(1) compaction, at most evMaxSubs
// NON-BLOCKING channel sends, map deletes and close(done). No I/O (log lines are
// deferred until after the unlock), no json.Marshal, no unbounded allocation, and no
// second mutex is ever acquired. Nothing in this package acquires another lock while
// holding hub.mu, so no lock cycle exists: the only orders that occur are
// st.mu -> hub.mu (the /_switch/status snapshot) and never pref.mu -> hub.mu (the
// switchover publisher runs before pref.mu.RLock()).
//
// "Never blocks" therefore means "never blocks on anything unbounded" — hub.mu is still
// a mutex, and bounding this critical section is what makes the claim real.
func (h *evHub) append(f *evFrame) {
	var gone []string // logged after the unlock; nil in the normal case

	h.mu.Lock()
	h.published++
	h.ring = append(h.ring, f)
	h.ringBytes += len(f.b)

	// Evict by BOTH count and bytes. Count alone guarantees no ceiling once payloads are
	// arbitrary. Never evict the frame just appended, so a pathologically small
	// evRingMaxBytes cannot spin here.
	for (len(h.ring)-h.head > evRingSize || h.ringBytes > evRingMaxBytes) && len(h.ring)-h.head > 1 {
		old := h.ring[h.head]
		h.ringBytes -= len(old.b)
		h.ring[h.head] = nil // let the frame be collected
		h.head++
	}
	h.floor = h.ring[h.head].seq

	// Compact rather than re-slicing with ring = ring[1:], which grows the backing array
	// without bound — a slow leak no test would catch.
	if h.head > 0 && h.head*2 >= len(h.ring) {
		n := copy(h.ring, h.ring[h.head:])
		for i := n; i < len(h.ring); i++ {
			h.ring[i] = nil
		}
		h.ring, h.head = h.ring[:n], 0
	}

	for _, s := range h.subs { // deleting during range is legal
		select {
		case s.ch <- f:
		default:
			// Drop the SUBSCRIBER, not just the event. A subscriber that keeps
			// receiving after a drop hands scry a silently-gappy stream with no way to
			// notice; evicting forces the honest reconnect-and-replay path that this
			// same file implements, and it keeps the memory bound deterministic.
			h.dropped++
			if h.removeLocked(s) {
				h.evicted++
				gone = append(gone, s.addr)
			}
		}
	}
	live := len(h.subs)
	h.mu.Unlock()

	for _, addr := range gone {
		log.Printf("events: dropped subscriber %s (backpressure — queue full at %d) — %d live",
			addr, evChanSize, live)
	}
}

// clearRing drops all retained frames. Caller holds h.mu.
func (h *evHub) clearRing() {
	for i := range h.ring {
		h.ring[i] = nil
	}
	h.ring, h.head, h.ringBytes = h.ring[:0], 0, 0
	h.floor = atomic.LoadInt64(&evSeq) + 1
}

// removeLocked unregisters a subscriber. Idempotent. Caller holds h.mu and is
// responsible for any log line (this must stay I/O-free — see append).
func (h *evHub) removeLocked(s *evSub) bool {
	if _, ok := h.subs[s.id]; !ok {
		return false
	}
	delete(h.subs, s.id)
	close(s.done)
	atomic.StoreInt64(&evSubCount, int64(len(h.subs)))
	if len(h.subs) == 0 {
		atomic.StoreInt64(&evArmedUntil, time.Now().Add(evReplayGrace).UnixNano())
	}
	return true
}

// removeOldestLocked evicts the longest-connected subscriber. Caller holds h.mu.
// Ties break on id so the choice is deterministic under a coarse clock.
func (h *evHub) removeOldestLocked() *evSub {
	var oldest *evSub
	for _, s := range h.subs {
		if oldest == nil || s.at.Before(oldest.at) || (s.at.Equal(oldest.at) && s.id < oldest.id) {
			oldest = s
		}
	}
	if oldest == nil {
		return nil
	}
	h.evicted++
	h.removeLocked(oldest)
	return oldest
}

// attach installs a subscriber and, atomically with the install, returns the backlog it
// should receive first.
//
// Doing both under ONE lock acquisition is what makes replay lossless: an event
// published between reading the ring and joining the fan-out would otherwise land in
// neither. This is the classic SSE hub bug and it is invisible to the straightforward
// tests.
func (h *evHub) attach(addr, lastID string) (*evSub, []*evFrame, evGap) {
	h.mu.Lock()

	// If the hub disarmed since the last subscriber left, events published in that
	// window were dropped, so the ring can no longer represent a continuous history.
	// Drop it and say so — otherwise a resume finds its id still in the ring, is told
	// "clean", and silently misses everything after the disarm.
	stale := len(h.subs) == 0 && time.Now().UnixNano() >= atomic.LoadInt64(&evArmedUntil)
	if stale {
		h.clearRing()
	}

	backlog, gap := h.resolveLocked(lastID, stale)

	// Never 503 at the cap — evict the oldest instead. Real scry reconnecting always
	// gets in; a wedged subscriber is pushed out. This is self-healing even if the
	// keepalive and the write deadline both somehow fail to reap.
	var evicted []*evSub
	for len(h.subs) >= evMaxSubs {
		old := h.removeOldestLocked()
		if old == nil {
			break
		}
		evicted = append(evicted, old)
	}

	h.nextSub++
	s := &evSub{
		id:   h.nextSub,
		ch:   make(chan *evFrame, evChanSize),
		done: make(chan struct{}),
		at:   time.Now(),
		addr: addr,
	}
	h.subs[s.id] = s
	atomic.StoreInt64(&evSubCount, int64(len(h.subs)))
	atomic.StoreInt64(&evArmedUntil, 0) // a live subscriber arms us; the grace only matters once empty
	live := len(h.subs)
	h.mu.Unlock()

	for _, old := range evicted {
		log.Printf("events: dropped subscriber %s (subscriber cap %d reached) — %d live",
			old.addr, evMaxSubs, live)
	}
	return s, backlog, gap
}

// detach unregisters a subscriber that is leaving cleanly. Idempotent.
func (h *evHub) detach(s *evSub) {
	h.mu.Lock()
	h.removeLocked(s)
	h.mu.Unlock()
}

// resolveLocked works out what a resume token is owed. Caller holds h.mu.
//
//	absent                      -> live only ("fresh"); never a whole-ring dump by default
//	<epoch>-0                   -> whole ring (explicit "everything you have")
//	<epoch>-n, n >= floor-1     -> frames with seq > n            (clean)
//	<epoch>-n, n <  floor-1     -> whole ring                     ("evicted")
//	<otherEpoch>-n              -> nothing                        ("epoch")
//	ring cleared by disarm      -> nothing                        ("stale")
//	malformed                   -> live only                      ("fresh")
//
// The returned slice is always a fresh copy: h.ring is mutated after the lock drops.
// The frames themselves are immutable and shared.
func (h *evHub) resolveLocked(lastID string, stale bool) ([]*evFrame, evGap) {
	g := evGap{Oldest: h.floor, Next: atomic.LoadInt64(&evSeq) + 1}
	live := h.ring[h.head:]
	all := func() []*evFrame { return append([]*evFrame(nil), live...) }

	if lastID == "" {
		g.Reason = "fresh"
		return nil, g
	}
	epoch, seqStr, ok := strings.Cut(lastID, "-")
	after, err := strconv.ParseInt(seqStr, 10, 64)
	if !ok || err != nil || after < 0 {
		// Only a gate-authenticated client can reach this, so it is a bug, not an attack.
		log.Printf("events: malformed Last-Event-ID %q — starting live", lastID)
		g.Reason = "fresh"
		return nil, g
	}
	g.After = after

	switch {
	case epoch != evEpoch:
		g.Reason = "epoch" // we restarted; their seq numbering means nothing here
		return nil, g
	case stale:
		g.Reason = "stale"
		return nil, g
	case after == 0:
		return all(), g // explicit "send me everything you have"
	case after < h.floor-1:
		g.Reason = "evicted"
		return all(), g
	}

	out := make([]*evFrame, 0, len(live))
	for _, f := range live {
		if f.seq > after {
			out = append(out, f)
		}
	}
	return out, g
}

// eventsStatus renders the hub summary for /_switch/status.
//
// MUST NOT take st.mu: the caller (switchHandler) holds it across the whole snapshot
// literal. hub.mu is a leaf, so taking it here can neither deadlock nor stall.
func eventsStatus() map[string]any {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return map[string]any{
		"subscribers":  len(hub.subs),
		"capacity":     evMaxSubs, // so "why was I evicted?" is answerable from status
		"published":    hub.published,
		"dropped":      hub.dropped,
		"evicted":      hub.evicted,
		"buffer_len":   len(hub.ring) - hub.head,
		"buffer_bytes": hub.ringBytes,
		"epoch":        evEpoch,
		"armed":        eventsArmed(),
	}
}

// ---- HTTP ----

// eventsHandler serves GET /_switch/events.
//
// Registered as a literal pattern so it outranks the "/_switch/" subtree (Go 1.22
// precedence: it matches a strict subset), which lets switchHandler keep its
// unconditional Content-Type: application/json. It is gated for free by clientGate —
// default-deny, and only /_switch/healthz is exempt.
func eventsHandler(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
	lastID := r.Header.Get("Last-Event-ID")

	sub, backlog, gap := hub.attach(r.RemoteAddr, lastID)
	defer hub.detach(sub)

	hd := w.Header()
	hd.Set("Content-Type", "text/event-stream; charset=utf-8")
	hd.Set("Cache-Control", "no-cache")
	hd.Set("X-Accel-Buffering", "no") // insurance if a buffering proxy ever lands in front
	w.WriteHeader(http.StatusOK)

	log.Printf("events: subscriber %s attached (resume=%q gap=%q backlog=%d)",
		r.RemoteAddr, lastID, gap.Reason, len(backlog))

	if err := evWrite(rc, w, evPreamble(gap)); err != nil {
		return
	}
	for _, f := range backlog {
		if err := evWrite(rc, w, f.b); err != nil {
			return
		}
	}

	var healthGap *string
	if gap.Reason != "" {
		healthGap = timeValue(nowFn())
	}
	publishHealthSnapshot(healthGap)

	// First time.Ticker in this package — the existing background loops are
	// `for { ...; time.Sleep(d) }` and uncancellable. This timer must die with the
	// request, and teardown keys off r.Context().Done() because the server has no
	// graceful shutdown to hook (main() ends in log.Fatal(srv.ListenAndServe())).
	tick := time.NewTicker(evKeepalive)
	defer tick.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-sub.done: // evicted by the hub (backpressure or cap)
			return
		case f := <-sub.ch:
			if err := evWrite(rc, w, f.b); err != nil {
				evLogGone(sub, err)
				return
			}
		case <-tick.C:
			if err := evWrite(rc, w, evPing); err != nil {
				evLogGone(sub, err)
				return
			}
		}
	}
}

// evPreamble is the per-connection opener: the SSE reconnect hint, then one hub_hello
// carrying the whole reconnect truth so a client can decide whether to resync from
// /_switch/status.
//
// hub_hello deliberately carries NO id: line — it is not a ring event, and an id here
// would clobber the client's stored Last-Event-ID.
func evPreamble(g evGap) []byte {
	hub.mu.Lock()
	bufLen := len(hub.ring) - hub.head
	hub.mu.Unlock()

	body, _ := json.Marshal(evEnvelope{
		TS:   time.Now().UTC().Format(time.RFC3339Nano),
		Type: "hub_hello",
		Data: map[string]any{
			"epoch":             evEpoch,
			"gap":               g,
			"buffer_len":        bufLen,
			"capacity":          evMaxSubs,
			"keepalive_seconds": int(evKeepalive / time.Second),
		},
	})
	return []byte("retry: " + strconv.Itoa(evRetryHintMS) + "\n\n" +
		"event: hub_hello\ndata: " + string(body) + "\n\n")
}

// evWrite writes one frame under a deadline.
//
// The deadline is what actually reclaims a wedged peer: without it a client with a
// closed TCP window and no RST blocks Write forever — not stalling the publisher (that
// runs in another goroutine) but pinning a subscriber slot and a connection
// permanently. Paired with the keepalive tick, which guarantees a write ATTEMPT every
// evKeepalive even on a completely idle proxy, a dead peer is reaped within
// evKeepalive+evWriteTimeout. net/http clears the deadline in finishRequest, so it
// cannot leak onto a later request on a reused keep-alive connection.
func evWrite(rc *http.ResponseController, w io.Writer, b []byte) error {
	if err := rc.SetWriteDeadline(time.Now().Add(evWriteTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	if err := rc.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

// evLogGone distinguishes a reaped wedged peer from an ordinary disconnect, so the
// difference is visible in docker logs.
func evLogGone(s *evSub, err error) {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		log.Printf("events: subscriber %s write timed out after %s — reaping", s.addr, evWriteTimeout)
		return
	}
	log.Printf("events: subscriber %s gone: %v", s.addr, err)
}
