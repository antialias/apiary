package main

// ---- response-side transcript tap (#61): the assistant's reply, live ----
//
// The request tap (#60, tap.go) publishes the conversation as the CLIENT sent it, which
// leaves scry one reply behind: an assistant message only appears when the next request
// re-sends it. This file closes the gap by tapping the RESPONSE path. By the time bytes
// reach the client they are Anthropic-shaped for every lane — codexWire.translate
// rewrites the gpt lane's stream inside the transport — so ONE aggregator understands
// all of them, the same by-construction argument tap.go leads with.
//
// The tee lives in tapRecorder.Write (main.go): copy the chunk, non-blocking send into
// a per-request channel, done. A separate goroutine (the aggregator) consumes the
// channel, reassembles the SSE stream (or buffers a non-streaming JSON body), and
// publishes at most three events: `turn_started` on the first byte, then exactly one
// terminal — `assistant_message` when the aggregate is trustworthy, `turn_ended` when
// it is not. Whole messages, never per-token deltas: the browser re-walks its event
// ring on every arrival and holds only 200 events.
//
// This is the one place a bug corrupts live inference — the agent gateway rides this exact
// path — so the properties tap.go holds are held harder here:
//
//  1. THE HOT PATH IS THREE STATEMENTS. feed() is a latch check, one bounded copy
//     (mandatory: ReverseProxy reuses its 32 KiB copy buffer, so retaining b would
//     alias bytes that are about to be overwritten), and one select-with-default.
//     The honest cost claim — and it sits BETWEEN the buffered write and the wire
//     flush inside maxLatencyWriter.Write, not after the bytes leave — is one
//     allocation plus one non-blocking send. TestE2ERespStreamingLatencyPreserved
//     measures it; nothing here argues it away.
//
//  2. IT CANNOT FAIL THE REQUEST. feed recovers and latches dead. A full channel
//     DROPS the chunk (never blocks) and any drop poisons the aggregate: a
//     boundary-aligned byte loss can reassemble into VALID, WRONG events, so a lossy
//     turn publishes turn_ended{tap_lossy}, never a plausible-looking message.
//
//  3. ONLY THE HANDLER CLOSES THE CHANNEL, exactly once, in a defer on the same
//     goroutine that runs every Write — send-on-closed is structurally impossible.
//     The idle backstop lives INSIDE chanReader.Read (select on the timer), so it can
//     unpark the aggregator without ever touching the channel; it then latches dead
//     and DRAINS until the handler's close. Drain-until-close applies to EVERY
//     aggregator exit — message_stop, error, lossy, backstop, panic — so feed can
//     never back up behind a consumer that already left.
//
// Hold seams (#51): a future hold interposes at the TRANSPORT by replacing resp.Body
// with an io.Pipe it authors (the translateCodexResponse pattern) — this tap then
// observes the held stream unchanged. chanReader.Read selects on `done` so a
// controller can abandon the aggregator externally; the only-the-handler-closes rule
// above is also the hold invariant. respAgg tracks maxIndex so a continuation that
// swallows a tool_use knows the next content_block index to author (Q2). The tap owns
// nothing that outlives the request (Q5).

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Tunables are source vars, not env vars, for tap.go's reasons — except
// respTapEnabled, which IS the ticket's operator kill switch (SWITCH_TAP=off): the
// response tap is new code teeing production inference, and "turn it off without a
// build" is an acceptance criterion. Resolved once in main(), never re-read.
var (
	respTapEnabled  = true
	respTapChanSize = 256              // per-request chunk queue depth
	respTapIdleMax  = 30 * time.Minute // backstop; measured from last activity by construction (Read resets it per receive)
	respTapMaxJSON  = 4 << 20          // non-streaming body accumulation cap
	respTapErrMax   = 512              // error_message clip, bytes
)

var (
	respTapStarts     atomic.Int64 // turn_started published
	respTapMessages   atomic.Int64 // assistant_message published
	respTapEnds       atomic.Int64 // turn_ended published
	respTapDropChunks atomic.Int64 // chunks dropped on a full channel (fleet-wide)
	respTapLossy      atomic.Int64 // turns poisoned by a drop
	respTapPanics     atomic.Int64 // recovered panics (feed or aggregator)
	respTapBackstops  atomic.Int64 // idle backstops fired
	respTapActive     atomic.Int64 // aggregators currently running (gauge)
)

// respTapStatus is the /_switch/status "response_tap" block. Atomics only — it is
// read under st.mu and must not reach for any other lock.
func respTapStatus() map[string]any {
	return map[string]any{
		"enabled":        respTapEnabled,
		"starts":         respTapStarts.Load(),
		"messages":       respTapMessages.Load(),
		"ends":           respTapEnds.Load(),
		"chunks_dropped": respTapDropChunks.Load(),
		"lossy_turns":    respTapLossy.Load(),
		"panics":         respTapPanics.Load(),
		"backstops":      respTapBackstops.Load(),
		"active":         respTapActive.Load(),
	}
}

// ---- identity: computed on the request side, carried to the response ----

// tapRespIdent is what tapTurn hands requestHandler so the response tap can label its
// events with the SAME conversation identity the paired "turn" event carried — scry
// joins the two sides on conversation_key and upserts the message by index. It is a
// plain value returned up the stack, not a ctx key: reqInfo is a value type cloned
// across retries, and the ctx is sealed before tapTurn runs.
type tapRespIdent struct {
	decisionID string
	ok         bool    // armed + parseable /v1/messages body; zero value = don't tap
	tenant     *tenant // whose turn: its anchors, its events, its tap mode
	convKey    string
	tapKey     string // the tapConvs key, never rendered: what tapRememberReply files a reply under
	sess       string // the full session id, never rendered: an anchor is only good within it
	session    string // fpSess display form
	sessionKey string // #66's opaque enqueue key
	contentFP  string
	epoch      int
	turn       int64
	index      int // the assistant reply's index = the conversation's length (tapTurn's total)
	lane       string
	modelReq   string
	modelUp    string
	route      string
	account    string
	translated bool // served by a translating wire (codex) — as routed at request time
	served     *servedHop
	stream     bool       // the CLIENT's stream wish (the response's framing can differ)
	harness    string     // the request's registry id (harness.go), as the turn event carries it
	cap        *servedCap // the request's served capture: the speed its served hop ran at (liveSpeed)

	// #76 usage-scaling provenance: ratio is the declared r_p as routed at
	// request time (0/1 = unscaled; the served hop's record wins when a
	// mid-request cross changed it) and scale is the request's shared mark,
	// read — never written — at terminal publish.
	ratio float64
	scale *scaleMark
}

// respIdentJSON is the identity block every response-side event opens with. Every
// field always present — tapTurnEvent's stable-schema doctrine.
type respIdentJSON struct {
	DecisionID      string `json:"decision_id,omitempty"`
	ConversationKey string `json:"conversation_key"`
	Session         string `json:"session"`
	SessionKey      string `json:"session_key"`
	ContentFP       string `json:"content_fp,omitempty"` // withheld under the metadata tap
	Epoch           int    `json:"epoch"`
	Turn            int64  `json:"turn"`
	Index           int    `json:"index"`
	Lane            string `json:"lane"`
	ModelRequested  string `json:"model_requested"`
	ModelUpstream   string `json:"model_upstream"`
	Translated      bool   `json:"translated"`
	Route           string `json:"route"`
	Class           string `json:"class,omitempty"`
	Member          string `json:"member,omitempty"`
	BindReason      string `json:"bind_reason,omitempty"` // #86; see tapTurnEvent for why omitempty
	Account         string `json:"account"`
	Attempts        int    `json:"attempts"`
	// Harness rides on every response-side event so a consumer knows the harness
	// from the attempt's first event, even when its request re-sent an identical
	// array and so published no turn event. Omitted only by an identity tapTurn
	// did not build.
	Harness string `json:"harness,omitempty"`
	// Speed is the speed the served hop ran at, the one its served event is
	// priced at: "standard", "fast" or "unknown" (servedCap.liveSpeed). Only
	// the response side carries it: at request time the hop is not known.
	// Never omitted.
	Speed string `json:"speed"`
}

func (id *tapRespIdent) render() respIdentJSON {
	lane, modelUp, translated, account := id.lane, id.modelUp, id.translated, id.account
	class, member, attempts := "", "", 1
	// Read off the served hop rather than the request-time route so that a
	// mid-request cross drops it: selectHop clears the reason the moment another
	// member takes over, which is precisely when it would start lying.
	bindReason := ""
	speed := id.cap.liveSpeed()
	if served := id.served.snapshot(); served.attempts > 0 {
		lane, modelUp, translated, account = served.lane, served.model, served.translated, served.account
		class, member, attempts = served.class, served.member, served.attempts
		bindReason = served.bindReason
	}
	return respIdentJSON{
		DecisionID:      id.decisionID,
		ConversationKey: id.convKey,
		Session:         id.session,
		SessionKey:      id.sessionKey,
		ContentFP:       id.contentFP,
		Epoch:           id.epoch,
		Turn:            id.turn,
		Index:           id.index,
		Lane:            lane[:tapRuneCut(lane, tapMaxIdent)],
		ModelRequested:  id.modelReq[:tapRuneCut(id.modelReq, tapMaxIdent)],
		ModelUpstream:   modelUp[:tapRuneCut(modelUp, tapMaxIdent)],
		Translated:      translated,
		Route:           id.route[:tapRuneCut(id.route, tapMaxIdent)],
		Class:           class[:tapRuneCut(class, tapMaxIdent)],
		Member:          member[:tapRuneCut(member, tapMaxIdent)],
		BindReason:      bindReason[:tapRuneCut(bindReason, tapMaxIdent)],
		Account:         account[:tapRuneCut(account, tapMaxIdent)],
		Attempts:        attempts,
		Harness:         id.harness,
		Speed:           speed,
	}
}

// ---- the events ----

type respStartedEvent struct {
	respIdentJSON
	Status int  `json:"status"`
	SSE    bool `json:"sse"` // the response's ACTUAL framing, not the request's wish
}

type respMessageEvent struct {
	respIdentJSON
	ModelStream     string      `json:"model_stream"` // what the stream itself claimed served it
	StopReason      string      `json:"stop_reason"`
	StreamTruncated bool        `json:"stream_truncated"` // stream died before message_stop; content is real but incomplete
	Usage           *respUsage  `json:"usage,omitempty"`
	Ratio           float64     `json:"ratio,omitempty"`   // #76: declared r_p of the served hop (absent = unscaled)
	Scaling         *tapScaling `json:"scaling,omitempty"` // #76: present ONLY when the mark resolved ok/fail_open
	Message         tapMessage  `json:"message"`           // same shape as the request side, so scry's reader is one function
	Content         string      `json:"content,omitempty"` // "withheld" under the metadata tap: Message then carries no blocks
	BlocksElided    int         `json:"blocks_elided"`
	Truncated       bool        `json:"truncated"`
	Lossy           bool        `json:"lossy"`
	ChunksDropped   int64       `json:"chunks_dropped"`
	Bytes           int64       `json:"bytes"`
	DurationMS      int64       `json:"duration_ms"`
	Status          int         `json:"status"`
	SSE             bool        `json:"sse"`
}

type respEndedEvent struct {
	respIdentJSON
	Reason        string      `json:"reason"` // tap_lossy | error | backstop | abandoned | oversize | unparseable | tap_panic
	Status        int         `json:"status"`
	ErrorType     string      `json:"error_type"`
	ErrorMessage  string      `json:"error_message"`
	Ratio         float64     `json:"ratio,omitempty"`
	Scaling       *tapScaling `json:"scaling,omitempty"`
	ChunksDropped int64       `json:"chunks_dropped"`
	Bytes         int64       `json:"bytes"`
	DurationMS    int64       `json:"duration_ms"`
}

// tapScaling is the usage-scaling provenance block (#76). It appears ONLY when
// the request's scaleMark RESOLVED ok or fail_open — a pending or "none" mark
// omits the block entirely, so a consumer can never read "verbatim" out of
// "unknown" (the badge-honesty contract scry builds on: a truncated codex
// stream publishes usage without `scaling` → render "unknown", never "⚠").
type tapScaling struct {
	Applied          bool    `json:"applied"`
	Mode             string  `json:"mode,omitempty"`             // applied only: sse | json | count_tokens | translated | translated_agg
	Ratio            float64 `json:"ratio,omitempty"`            // applied only: the divisor actually used
	RawInputTokens   *int    `json:"raw_input_tokens,omitempty"` // pre-scale, upstream units; absent = the wire never carried it
	RawCacheRead     *int    `json:"raw_cache_read,omitempty"`
	RawCacheCreation *int    `json:"raw_cache_creation,omitempty"`
	Reason           string  `json:"reason,omitempty"` // !applied only: content_encoding | oversize | unparseable | no_usage | stream_truncated
}

// scaleInfo resolves the served hop's declared ratio and the scaling block for
// terminal publication. The ratio prefers the served-hop record — a
// mid-request cross changes which member's r_p governed — and the block
// renders only a RESOLVED mark.
func (id *tapRespIdent) scaleInfo() (float64, *tapScaling) {
	r := id.ratio
	if sv := id.served.snapshot(); sv.attempts > 0 {
		r = sv.ratio
	}
	v, ok := id.scale.view()
	if !ok {
		return r, nil
	}
	sc := &tapScaling{Applied: v.applied}
	if v.applied {
		sc.Mode, sc.Ratio = v.mode, v.ratio
		sc.RawInputTokens, sc.RawCacheRead, sc.RawCacheCreation = v.rawIn, v.rawCacheRead, v.rawCacheCreate
	} else {
		sc.Reason = v.reason
	}
	return r, sc
}

// tapUsage preserves the wire's unknown-vs-zero distinction: a field is a pointer, and
// only a field PRESENT on the wire overwrites the aggregate. codex's truncated-close
// contract (codex.go: cache fields omitted = UNKNOWN, explicit zero = a lie) survives
// republication for free — the tap never synthesizes a usage field the stream did not
// carry.
type tapUsage struct {
	InputTokens        *int              `json:"input_tokens,omitempty"`
	OutputTokens       *int              `json:"output_tokens,omitempty"`
	CacheReadInput     *int              `json:"cache_read_input_tokens,omitempty"`
	CacheCreationInput *int              `json:"cache_creation_input_tokens,omitempty"`
	CacheCreation      *tapCacheCreation `json:"cache_creation,omitempty"`
}

// tapCacheCreation is Anthropic's per-TTL split of cache_creation_input_tokens:
// the two ephemeral counts sum to it. It is republished verbatim so scry can see
// WHICH cache TTL the client asked for — a session downgraded to the 5m TTL
// rewrites its whole context to cache-creation on every idle gap, and that is a
// fact worth recording rather than inferring from a high creation count. The
// pointers keep the unknown-vs-zero contract: a lane that reports no breakdown
// (codex has no TTL) leaves both nil, which is "unknown", never zero.
type tapCacheCreation struct {
	Ephemeral5m *int `json:"ephemeral_5m_input_tokens,omitempty"`
	Ephemeral1h *int `json:"ephemeral_1h_input_tokens,omitempty"`
}

func (u *tapUsage) overlay(raw json.RawMessage) {
	if len(raw) == 0 {
		return
	}
	var w tapUsage
	if json.Unmarshal(raw, &w) != nil {
		return
	}
	if w.InputTokens != nil {
		u.InputTokens = w.InputTokens
	}
	if w.OutputTokens != nil {
		u.OutputTokens = w.OutputTokens
	}
	if w.CacheReadInput != nil {
		u.CacheReadInput = w.CacheReadInput
	}
	if w.CacheCreationInput != nil {
		u.CacheCreationInput = w.CacheCreationInput
	}
	// The nested split rides in message_start's usage (caching is decided at
	// prompt-read time); message_delta carries output_tokens and does not repeat
	// it, so a per-field merge — not whole-object replace — is what keeps the
	// start-side breakdown from being blanked by the delta.
	if w.CacheCreation != nil {
		if u.CacheCreation == nil {
			u.CacheCreation = &tapCacheCreation{}
		}
		if w.CacheCreation.Ephemeral5m != nil {
			u.CacheCreation.Ephemeral5m = w.CacheCreation.Ephemeral5m
		}
		if w.CacheCreation.Ephemeral1h != nil {
			u.CacheCreation.Ephemeral1h = w.CacheCreation.Ephemeral1h
		}
	}
}

// respUsage is assistant_message's usage: the client-visible counts the tap
// aggregated, plus raw_output_tokens, the served hop's own output count in
// upstream units, when it differs in kind from what the client sees. That is
// a codex hop: codexUsage takes reasoning tokens off the output the client is
// given, and the served event bills the upstream's whole output
// (codexServedTokens). It is present on every codex turn whose upstream
// reported usage, equal to output_tokens when nothing was subtracted, and
// absent when the wire never carried one (an Anthropic or pass-through hop,
// whose output reaches the client unchanged, or a codex turn that reported no
// usage). Read off the request's served capture (servedCap.rawOutput), so it
// is the number the served event bills, not a second count of it.
type respUsage struct {
	tapUsage
	RawOutputTokens *int `json:"raw_output_tokens,omitempty"`
}

func (u *tapUsage) empty() bool {
	return u.InputTokens == nil && u.OutputTokens == nil &&
		u.CacheReadInput == nil && u.CacheCreationInput == nil && u.CacheCreation == nil
}

// ---- the tap ----

type respTap struct {
	ch   chan []byte   // chunk queue; closed EXACTLY ONCE, by finish(), on the request goroutine
	done chan struct{} // external abandon (tests today, a hold controller later); NEVER closes ch
	dead atomic.Bool   // feed no-op latch; set on every aggregator exit so a dead consumer costs nothing

	closeOnce sync.Once
	dropped   atomic.Int64

	// Latched by the REQUEST goroutine at first feed, before the first send; the
	// aggregator reads them only after its first receive, so the channel is the
	// happens-before edge and no lock exists on this struct.
	started bool
	status  int
	ct      string
	ce      string // Content-Encoding: the wire is what the CLIENT negotiated, so the tap decodes for itself
	t0      time.Time

	id tapRespIdent

	// Aggregator-goroutine-only.
	terminal bool
}

// newRespTap starts the aggregator. finish() — and nothing else, ever — closes ch.
// The active gauge is incremented HERE, synchronously with creation, not inside the
// goroutine: waiters polling the gauge for zero must never observe it before a
// just-spawned aggregator has been scheduled.
func newRespTap(id tapRespIdent) *respTap {
	t := &respTap{
		ch:   make(chan []byte, respTapChanSize),
		done: make(chan struct{}),
		id:   id,
	}
	respTapActive.Add(1)
	go t.aggregate()
	return t
}

func (t *respTap) finish() { t.closeOnce.Do(func() { close(t.ch) }) }

// respTapRecover is feed's deferred guard: a plain deferred call, not a closure, so
// the hot path does not allocate for it.
func respTapRecover(t *respTap) {
	if recover() != nil {
		respTapPanics.Add(1)
		t.dead.Store(true)
	}
}

// feed is the hot path: called from tapRecorder.Write on the request goroutine for
// every chunk the client-side writer accepted. It must never block, never panic
// through, and never retain b — ReverseProxy reuses its copy buffer under the bytes.
func (t *respTap) feed(b []byte, status int, hdr http.Header) {
	defer respTapRecover(t)
	if t.dead.Load() || len(b) == 0 {
		return
	}
	if !t.started {
		t.started = true
		t.status = status
		t.ct = hdr.Get("Content-Type")
		t.ce = hdr.Get("Content-Encoding")
		t.t0 = time.Now()
	}
	c := make([]byte, len(b))
	copy(c, b)
	select {
	case t.ch <- c:
	default:
		t.dropped.Add(1)
		respTapDropChunks.Add(1)
	}
}

// ---- the reader between the channel and the parser ----

var (
	errRespIdle      = errors.New("resptap: idle backstop")
	errRespAbandoned = errors.New("resptap: abandoned")
)

// chanReader adapts the chunk channel to io.Reader for parseSSE / io.Copy. The idle
// backstop lives HERE, not on an external timer: an external timer cannot unpark a
// goroutine blocked on a bare receive, and it must never close the channel (a
// timer-goroutine close racing a live feed panics the REQUEST goroutine mid-stream —
// the one failure mode worse than every tap loss). Reset-per-receive is what makes
// the backstop measure from last activity. go1.23 timer semantics: bare Reset is
// safe, no drain dance.
type chanReader struct {
	t     *respTap
	idle  *time.Timer
	buf   []byte
	total int64 // WIRE bytes handed onward (compressed when the client negotiated an encoding) — the events' `bytes` field
	cause error // latched sentinel (errRespIdle / errRespAbandoned): a decompressor between here and the parser may wrap or swallow the returned error, so terminal dispatch reads THIS, not the parser's error
}

func (r *chanReader) Read(p []byte) (int, error) {
	if len(r.buf) == 0 {
		r.idle.Reset(respTapIdleMax)
		select {
		case b, ok := <-r.t.ch:
			if !ok {
				return 0, io.EOF
			}
			r.buf = b
			r.total += int64(len(b))
		case <-r.t.done:
			r.cause = errRespAbandoned
			return 0, errRespAbandoned
		case <-r.idle.C:
			r.cause = errRespIdle
			return 0, errRespIdle
		}
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

// ---- the aggregator ----

// respTapHook is a test seam: when non-nil it runs at the top of the aggregator,
// inside its recover scope, so the panic-containment test can prove the machinery
// without a synthetic panic path in real code. Nil in production — one nil check per
// request, off the hot path.
var respTapHook func()

func (t *respTap) aggregate() {
	defer func() {
		if r := recover(); r != nil {
			// An unrecovered panic on a plain goroutine kills the PROCESS. This
			// recover is a correctness requirement, not hygiene. publishPanic, not
			// publishEnded: a panic can land BEFORE the first channel receive, and
			// the feed-latched fields (status, t0) are only readable after it.
			respTapPanics.Add(1)
			t.publishPanic()
		}
		t.dead.Store(true)
		for range t.ch { // drain until the handler's close — every exit path, no exceptions
		}
		respTapActive.Add(-1)
	}()
	if h := respTapHook; h != nil {
		h()
	}
	t.run()
}

func (t *respTap) run() {
	cr := &chanReader{t: t, idle: time.NewTimer(respTapIdleMax)}
	defer cr.idle.Stop()

	// Wait for the first chunk before anything else: feed latches status/ct/t0
	// before its first send, so the first receive is what makes them readable here.
	// A response that produced zero bytes publishes nothing — like a body the
	// request tap cannot parse, silence beats a guess.
	select {
	case b, ok := <-t.ch:
		if !ok {
			return
		}
		cr.buf = b
		cr.total += int64(len(b))
	case <-t.done:
		return
	case <-cr.idle.C:
		respTapBackstops.Add(1)
		return
	}

	sse := strings.HasPrefix(t.ct, "text/event-stream")
	respTapStarts.Add(1)
	publish(t.id.tenant.id, "turn_started", respStartedEvent{
		respIdentJSON: t.id.render(),
		Status:        t.status,
		SSE:           sse,
	})

	// The wire carries whatever encoding the CLIENT negotiated — the tee is a
	// byte-transparent copy, so gzip on the wire is gzip in the channel (live
	// Anthropic returns gzip'd SSE whenever it is offered, and every real CLI
	// offers it). Decoding happens HERE, on the aggregator goroutine, never on
	// the hot path; the client's bytes are untouched.
	r, ok := t.decode(cr)
	if !ok {
		return
	}

	if sse {
		t.runSSE(r, cr)
	} else {
		t.runJSON(r, cr)
	}
}

// decode wraps cr per the latched Content-Encoding. On any failure it publishes the
// honest terminal itself and reports !ok. Stdlib covers gzip and deflate; an encoding
// it cannot decode is a fact about the tap, not the turn — turn_ended{content_encoding}
// says so instead of letting the parser call compressed bytes "unparseable".
func (t *respTap) decode(cr *chanReader) (io.Reader, bool) {
	enc := strings.ToLower(strings.TrimSpace(t.ce))
	var (
		r   io.Reader
		err error
	)
	switch enc {
	case "", "identity":
		return cr, true
	case "gzip", "x-gzip":
		r, err = gzip.NewReader(cr) // reads the header eagerly; blocks on cr like any read
	case "deflate":
		r, err = zlib.NewReader(cr) // RFC 9110 deflate = zlib-wrapped
	default: // includes multi-token lists ("gzip, br") — none observed, none guessed at
		t.publishEnded("content_encoding", &respAgg{errMsg: "unsupported content-encoding: " + enc}, cr)
		return nil, false
	}
	if err != nil {
		switch {
		case t.dropped.Load() > 0:
			respTapLossy.Add(1)
			t.publishEnded("tap_lossy", nil, cr)
		case errors.Is(cr.cause, errRespIdle):
			respTapBackstops.Add(1)
			t.publishEnded("backstop", nil, cr)
		case errors.Is(cr.cause, errRespAbandoned):
			t.publishEnded("abandoned", nil, cr)
		default: // corrupt or truncated-before-header stream
			t.publishEnded("unparseable", &respAgg{errMsg: err.Error()}, cr)
		}
		return nil, false
	}
	return r, true
}

func (t *respTap) runSSE(r io.Reader, cr *chanReader) {
	a := &respAgg{budget: tapTextBudget, byIndex: map[int]*respBlock{}}
	remembered := false
	parseSSE(r, func(data []byte) bool {
		a.handle(data)
		if a.started && !remembered {
			// The reply's id is known from message_start on. File it now rather than at
			// message_stop: a continue naming it may follow a stream the tap ends early
			// (a backstop, a lossy drop), and the conversation is the same either way.
			remembered = true
			t.id.tenant.tapRememberReply(a.id, &t.id, time.Now())
		}
		return !a.ended
	})

	switch {
	case t.dropped.Load() > 0:
		// S3: a drop loses an arbitrary byte range, and a boundary-aligned one
		// reassembles into valid, WRONG events. Never publish a plausible corrupt
		// message — say the turn happened and that the tap lost it.
		respTapLossy.Add(1)
		t.publishEnded("tap_lossy", a, cr)
	case a.errType != "":
		t.publishEnded("error", a, cr)
	case errors.Is(cr.cause, errRespIdle):
		respTapBackstops.Add(1)
		t.publishEnded("backstop", a, cr)
	case errors.Is(cr.cause, errRespAbandoned):
		t.publishEnded("abandoned", a, cr)
	case a.ended:
		t.publishMessage(a, cr, false)
	case a.hasContent():
		// The channel closed (or the scanner gave up) before message_stop. What
		// aggregated IS what the client received — publish it, marked incomplete.
		t.publishMessage(a, cr, true)
	default:
		t.publishEnded("unparseable", a, cr)
	}
}

func (t *respTap) runJSON(r io.Reader, cr *chanReader) {
	var buf bytes.Buffer
	// The cap measures DECODED bytes — with a decompressor in front, that is the
	// bound that matters (the wire size of a compressed body says nothing about
	// what it inflates to).
	io.Copy(&buf, io.LimitReader(r, int64(respTapMaxJSON)+1))

	switch {
	case t.dropped.Load() > 0:
		respTapLossy.Add(1)
		t.publishEnded("tap_lossy", nil, cr)
		return
	case errors.Is(cr.cause, errRespIdle):
		respTapBackstops.Add(1)
		t.publishEnded("backstop", nil, cr)
		return
	case errors.Is(cr.cause, errRespAbandoned):
		t.publishEnded("abandoned", nil, cr)
		return
	case buf.Len() > respTapMaxJSON:
		t.publishEnded("oversize", nil, cr)
		return
	}

	body := buf.Bytes()
	if t.status < 200 || t.status >= 300 {
		// The Anthropic error envelope — including our own ErrorHandler's 502 and
		// the failover's buffered non-2xx replays. Tolerant parse; a shape we don't
		// recognise still reports the status.
		et, em := respErrEnvelope(body)
		a := &respAgg{errType: et, errMsg: em}
		t.publishEnded("error", a, cr)
		return
	}

	var f map[string]json.RawMessage
	if json.Unmarshal(body, &f) != nil {
		t.publishEnded("unparseable", nil, cr)
		return
	}
	a := &respAgg{budget: tapTextBudget, byIndex: map[int]*respBlock{}}
	a.id = tapStr(f["id"])
	a.model = tapStr(f["model"])
	a.stopReason = tapStr(f["stop_reason"])
	a.usage.overlay(f["usage"])
	t.id.tenant.tapRememberReply(a.id, &t.id, time.Now()) // see runSSE
	// The response body IS a role+content object — the request side's renderer
	// applies verbatim, which is exactly what keeps the two sides' block shape from
	// ever drifting.
	msg := tapRenderMessage(body, t.id.index, &a.budget)
	t.publishBuilt(a, cr, msg, msg.Elided, false, false)
}

// ---- SSE aggregation state ----

type respBlock struct {
	typ     string
	id      string // tool_use family: the id a later tool_result names
	name    string // tool_use family
	useID   string // tool_result family: the tool_use it answers
	text    strings.Builder
	textCap bool // per-block tapMaxText reached
	bytes   int  // SOURCE bytes seen for this block, so a clip is measurable
	clipped bool
}

type respAgg struct {
	id         string
	model      string
	stopReason string
	usage      tapUsage
	errType    string
	errMsg     string

	byIndex  map[int]*respBlock
	order    []int        // arrival order of block indexes
	maxIndex int          // Q2 hold seam: the next authored block must open above this
	elidedAt map[int]bool // indexes beyond tapMaxBlocks (a block, not each of its deltas)
	elided   int          // len of the above, kept as a counter for render
	budget   int          // shared event-wide text budget (tapTextBudget)

	started bool // message_start seen
	ended   bool // message_stop or error seen
}

// respSSEEvent is the slice of an Anthropic SSE data payload the aggregator reads.
// Everything is RawMessage + tolerant helpers: a frame this doesn't understand
// degrades to counted bytes, never an error — tap.go's property 2.
type respSSEEvent struct {
	Type         string          `json:"type"`
	Index        *int            `json:"index"`
	Message      json.RawMessage `json:"message"`
	ContentBlock json.RawMessage `json:"content_block"`
	Delta        json.RawMessage `json:"delta"`
	Usage        json.RawMessage `json:"usage"`
	Error        json.RawMessage `json:"error"`
}

func (a *respAgg) handle(data []byte) {
	var ev respSSEEvent
	if json.Unmarshal(data, &ev) != nil {
		return // unparseable frame — skip, keep aggregating (translateCodexSSE's rule)
	}
	switch ev.Type {
	case "message_start":
		var m map[string]json.RawMessage
		if json.Unmarshal(ev.Message, &m) == nil {
			a.id = tapStr(m["id"])
			a.model = tapStr(m["model"])
			a.usage.overlay(m["usage"])
		}
		a.started = true
	case "content_block_start":
		if ev.Index == nil {
			return
		}
		b := a.open(*ev.Index)
		if b == nil {
			return
		}
		var cb map[string]json.RawMessage
		if json.Unmarshal(ev.ContentBlock, &cb) != nil {
			return
		}
		if typ := tapShort(tapStr(cb["type"]), 40); typ != "" {
			b.typ = typ
		}
		switch b.typ {
		case "tool_use", "server_tool_use", "mcp_tool_use":
			b.id = tapShort(tapStr(cb["id"]), 64)
			b.name = tapShort(tapStr(cb["name"]), 96)
			// A non-streamed input can arrive whole on the start event.
			if in := cb["input"]; len(in) > 0 && !bytes.Equal(in, []byte("{}")) {
				b.addRaw(in, &a.budget)
			}
		case "text", "thinking":
			if s := tapStr(cb["text"]) + tapStr(cb["thinking"]); s != "" {
				b.add(s, &a.budget)
			}
		case "tool_result", "mcp_tool_result", "web_search_tool_result":
			b.useID = tapShort(tapStr(cb["tool_use_id"]), 64)
		case "redacted_thinking":
			b.bytes += len(cb["data"]) // size only — same as the request side's image rule
		}
	case "content_block_delta":
		if ev.Index == nil {
			return
		}
		b := a.open(*ev.Index)
		if b == nil {
			return
		}
		var d map[string]json.RawMessage
		if json.Unmarshal(ev.Delta, &d) != nil {
			return
		}
		switch tapStr(d["type"]) {
		case "text_delta":
			b.add(tapStr(d["text"]), &a.budget)
		case "thinking_delta":
			b.add(tapStr(d["thinking"]), &a.budget)
		case "input_json_delta":
			b.add(tapStr(d["partial_json"]), &a.budget)
		case "signature_delta":
			// verification material, not transcript
		default:
			for _, v := range d {
				b.bytes += len(v)
			}
		}
	case "message_delta":
		var d map[string]json.RawMessage
		if json.Unmarshal(ev.Delta, &d) == nil {
			if sr := tapStr(d["stop_reason"]); sr != "" {
				a.stopReason = sr
			}
		}
		a.usage.overlay(ev.Usage)
	case "message_stop":
		a.ended = true
	case "error":
		a.errType, a.errMsg = respErrFields(ev.Error)
		a.ended = true
	}
	// content_block_stop / ping / unknown: nothing to record — bytes are already
	// counted at the reader.
}

// open returns the block for index, creating it if the stream never announced it (a
// delta on an unopened index is tolerated, typed "?"). Returns nil past the block
// cap, counting the elision instead.
func (a *respAgg) open(idx int) *respBlock {
	if idx > a.maxIndex {
		a.maxIndex = idx
	}
	if b := a.byIndex[idx]; b != nil {
		return b
	}
	if len(a.byIndex) >= tapMaxBlocks {
		if !a.elidedAt[idx] {
			if a.elidedAt == nil {
				a.elidedAt = map[int]bool{}
			}
			a.elidedAt[idx] = true
			a.elided++
		}
		return nil
	}
	b := &respBlock{typ: "?"}
	a.byIndex[idx] = b
	a.order = append(a.order, idx)
	return b
}

func (a *respAgg) hasContent() bool {
	return a.started || len(a.order) > 0
}

// add appends delta text under both the per-block cap and the shared budget, always
// counting source bytes so a clip stays measurable — tapFit's contract, incremental.
func (b *respBlock) add(s string, budget *int) {
	b.bytes += len(s)
	if s == "" || b.textCap {
		if s != "" {
			b.clipped = true
		}
		return
	}
	room := tapMaxText - b.text.Len()
	if *budget < room {
		room = *budget
	}
	if room <= 0 {
		b.textCap = true
		b.clipped = true
		return
	}
	if len(s) > room {
		n := tapRuneCut(s, room)
		b.text.WriteString(s[:n])
		*budget -= n
		b.textCap = true
		b.clipped = true
		return
	}
	b.text.WriteString(s)
	*budget -= len(s)
}

func (b *respBlock) addRaw(raw json.RawMessage, budget *int) {
	b.bytes += len(raw)
	room := tapMaxText - b.text.Len()
	if *budget < room {
		room = *budget
	}
	if room <= 0 {
		b.clipped = true
		return
	}
	if len(raw) > room {
		n := room
		for n > 0 && raw[n]&0xC0 == 0x80 { // back off a UTF-8 continuation byte
			n--
		}
		b.text.Write(raw[:n])
		*budget -= n
		b.clipped = true
		return
	}
	b.text.Write(raw)
	*budget -= len(raw)
}

// render seals the aggregate into the request side's message shape.
func (a *respAgg) render(index int) (tapMessage, int) {
	m := tapMessage{Index: index, Role: "assistant", Blocks: []tapBlock{}}
	for _, idx := range a.order {
		b := a.byIndex[idx]
		m.Blocks = append(m.Blocks, tapBlock{
			Type:      b.typ,
			Text:      b.text.String(),
			ID:        b.id,
			Name:      b.name,
			ToolUseID: b.useID,
			Bytes:     b.bytes,
			Clipped:   b.clipped,
		})
	}
	m.Elided = a.elided
	return m, a.elided
}

// ---- publication ----

func (t *respTap) publishMessage(a *respAgg, cr *chanReader, streamTruncated bool) {
	msg, elided := a.render(t.id.index)
	t.publishBuilt(a, cr, msg, elided, streamTruncated, true)
}

func (t *respTap) publishBuilt(a *respAgg, cr *chanReader, msg tapMessage, elided int, streamTruncated, sse bool) {
	if t.terminal {
		return
	}
	t.terminal = true

	clipped := false
	for _, b := range msg.Blocks {
		clipped = clipped || b.Clipped
	}
	ev := respMessageEvent{
		respIdentJSON:   t.id.render(),
		ModelStream:     tapShort(a.model, tapMaxIdent),
		StopReason:      tapShort(a.stopReason, 64),
		StreamTruncated: streamTruncated,
		Message:         msg,
		BlocksElided:    elided,
		Truncated:       clipped || elided > 0,
		ChunksDropped:   t.dropped.Load(),
		Bytes:           cr.total,
		DurationMS:      time.Since(t.t0).Milliseconds(),
		Status:          t.status,
		SSE:             sse,
	}
	raw := t.id.cap.rawOutput()
	if !a.usage.empty() || raw != nil {
		ev.Usage = &respUsage{tapUsage: a.usage, RawOutputTokens: raw}
	}
	ev.Ratio, ev.Scaling = t.id.scaleInfo()
	// ONE marshal-to-measure over the whole event, tapTurn's fallback verbatim: an
	// oversize payload keeps its envelope — losing the envelope would lose the fact
	// that the assistant answered at all.
	if b, err := json.Marshal(ev); err != nil || len(b) > evMaxEventBytes-tapReserve {
		ev.BlocksElided += len(ev.Message.Blocks)
		ev.Message.Blocks = []tapBlock{}
		ev.Truncated = true
	}
	if t.id.tenant.metadataOnly() {
		// The metadata tap: the reply's shape and usage, not its words.
		ev.Message.Blocks, ev.Content = nil, tapWithheld
		ev.BlocksElided = 0
	}
	respTapMessages.Add(1)
	publish(t.id.tenant.id, "assistant_message", ev)
}

// publishPanic is the recover path's terminal. It reads ONLY fields that are
// immutable since before the goroutine started (id) or atomic (dropped): the panic
// may have landed before the first channel receive, and the feed-latched fields
// (status, ct, t0) have no happens-before edge until that receive.
func (t *respTap) publishPanic() {
	if t.terminal {
		return
	}
	t.terminal = true
	respTapEnds.Add(1)
	publish(t.id.tenant.id, "turn_ended", respEndedEvent{
		respIdentJSON: t.id.render(),
		Reason:        "tap_panic",
		ChunksDropped: t.dropped.Load(),
	})
}

func (t *respTap) publishEnded(reason string, a *respAgg, cr *chanReader) {
	if t.terminal {
		return
	}
	t.terminal = true

	ev := respEndedEvent{
		respIdentJSON: t.id.render(),
		Reason:        reason,
		Status:        t.status,
		ChunksDropped: t.dropped.Load(),
	}
	ev.Ratio, ev.Scaling = t.id.scaleInfo()
	if a != nil {
		ev.ErrorType = tapShort(a.errType, 64)
		ev.ErrorMessage = tapShort(a.errMsg, respTapErrMax)
	}
	if cr != nil {
		ev.Bytes = cr.total
	}
	if !t.t0.IsZero() {
		ev.DurationMS = time.Since(t.t0).Milliseconds()
	}
	respTapEnds.Add(1)
	publish(t.id.tenant.id, "turn_ended", ev)
}

// respErrEnvelope reads {"type":"error","error":{"type":...,"message":...}} without
// trusting any of it.
func respErrEnvelope(body []byte) (string, string) {
	var f map[string]json.RawMessage
	if json.Unmarshal(body, &f) != nil {
		return "", ""
	}
	return respErrFields(f["error"])
}

func respErrFields(raw json.RawMessage) (string, string) {
	var e map[string]json.RawMessage
	if json.Unmarshal(raw, &e) != nil {
		return "", ""
	}
	return tapStr(e["type"]), tapStr(e["message"])
}
