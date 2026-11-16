package main

// ---- served events (quota-model 65 §7) ----
//
// Every request an account answers (an upstream 2xx on inference) goes to the
// brain as one `served` event once its response ends: the account, who asked
// for what, the member and model that answered, and the tokens as the
// provider counted them, before anything here rewrites them for the client
// (#44 scales the input side, codex's output drops its reasoning).
//
// The capture point is the served hop, noteServedHop in the failover
// transport. Every lane passes through it, whichever door the request came
// in by (the Anthropic wire, the OpenAI wire, the image door), and it already
// names the account the reports use. A request's servedCap rides its context:
// the door makes it (requestHandler, hpImagesDial), the hop
// arms it, the lane's own source fills its tokens, and the door finishes it
// once the response has ended, which queues the event (reports.go) and gives
// the log line its srv=<seq>.
//
// Where the tokens come from, by the hop that served:
//
//   - An Anthropic account, or a pass-through provider (Kimi): servedTap, a
//     body reader installed on the upstream's own bytes, inside the health tap
//     and the #44 scaler, so it reads them before anything rewrites them. It
//     reads message_start's and message_delta's usage and any error event
//     off a stream, or the usage of a JSON reply.
//   - A codex hop: the translator, which already parses every upstream event,
//     hands over response.completed's (or response.incomplete's) usage as the
//     upstream sent it.
//   - The image door: no tokens; sent incomplete.
//
// From quota-model 66 §1 the event also says when the served hop's response
// head arrived (head_at) and the window readings its headers carried
// (readings). noteServedHop parses the headers once and gives the one parse
// to the overlay, and so to the reading reports, and to the capture: the
// event's readings are the parsed windows that carry a use, named as the
// reading reports name them, with the same use and reset. read_at is head_at
// plus the time since the head on the monotonic clock, so a wall-clock step
// can't put the head after the end.
//
// An error inside the 2xx (inBand) decides the rest: before any output, a 4xx
// sends no event (a refusal or a request error); otherwise the event goes
// incomplete, naming the error.
//
// The hot path holds resptap.go's rules: nothing blocks (one short lock), a
// recovered panic latches the capture lossy and never reaches the request, and
// nothing is sent from here, only queued.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The kind of hop that served, which says where its tokens come from.
const (
	servedPassthrough = "passthrough" // an Anthropic account or a pass-through provider: servedTap
	servedCodex       = "codex"       // the codex translator
	servedImage       = "image"       // the image door: no tokens reported
	servedOtherWire   = "wire"        // a translating wire that reports no usage here
)

// Why an event is incomplete, in the brain's log.
const (
	servedWhyCut      = "stream ended before the final usage"
	servedWhyLeft     = "the client left before the response ended"
	servedWhyNoUsage  = "the response carried no usage"
	servedWhyImage    = "image tokens not reported"
	servedWhyWire     = "this lane reports no usage"
	servedWhyLossy    = "lossy capture"
	servedWhyEncoded  = "the response body was encoded"
	servedWhyTooLarge = "a usage line too long to read"
)

// servedTokens are a response's token counts as the provider counted them,
// each nil when it did not report that kind. input is neither cached nor
// written to the cache. Cache writes go by lifetime: Anthropic's 5m and 1h,
// OpenAI's 30m and 12h; cacheWrite is a total whose lifetimes are unknown,
// nil where they are known.
type servedTokens struct {
	input, cacheWrite5m, cacheWrite1h, cacheWrite30m, cacheWrite12h *int
	cacheWrite, cacheRead, output                                   *int
}

// servedReading is one window's reading from the served response's headers:
// the two fields a `reading` report carries, each nil when not carried.
type servedReading struct {
	usedPct, resetsAt *float64
}

// servedReadings is the event's readings from a parse of the response's
// headers (parseHeaderReading): the parsed windows that carry a use, by the
// brain's ids as the reading reports name them, each its use and reset only.
// A window without a use (a reset alone, a status alone) is left out: sizing
// needs a use for a head. The credits pot is not a window.
func servedReadings(hr headerReading) map[string]servedReading {
	out := make(map[string]servedReading, len(hr.windows))
	for name, w := range hr.windows {
		if w.UsedPct == nil {
			continue
		}
		u := *w.UsedPct
		r := servedReading{usedPct: &u}
		if w.ResetsAt != nil {
			v := *w.ResetsAt
			r.resetsAt = &v
		}
		out[name] = r
	}
	return out
}

// servedEvent is one `served` event as POST /reports takes it (wire).
type servedEvent struct {
	id, account string
	readAt      float64
	boot        string
	seq         int64
	consumer    *string
	selection   *string
	member      *string
	model       *string
	speed       *string
	paid        bool
	tokens      servedTokens
	complete    bool
	why         *string

	// headAt is when the upstream's response head reached the proxy, the
	// moment readings describe (quota-model 66 §1).
	headAt   float64
	readings map[string]servedReading

	// outputMax bounds the output of a request the client hung up on: its
	// own max_tokens, the most it could have drawn (quota-model 66 §1, §4);
	// nil on every other event (eventLocked).
	outputMax *int
}

// wire is the event's body: every key of testdata/served_golden.json's
// events, null where unknown. The brain refuses a key it does not know.
func (e servedEvent) wire() map[string]any {
	rd := make(map[string]any, len(e.readings))
	for name, r := range e.readings {
		rd[name] = map[string]any{"used_pct": r.usedPct, "resets_at": r.resetsAt}
	}
	return map[string]any{
		"id": e.id, "kind": reportServed, "account": e.account, "read_at": e.readAt,
		"head_at": e.headAt, "readings": rd,
		"boot": e.boot, "seq": e.seq,
		"consumer": e.consumer, "selection": e.selection, "member": e.member,
		"model": e.model, "speed": e.speed, "paid": e.paid,
		"tokens": map[string]any{
			"input": e.tokens.input, "cache_write_5m": e.tokens.cacheWrite5m,
			"cache_write_1h": e.tokens.cacheWrite1h, "cache_write_30m": e.tokens.cacheWrite30m,
			"cache_write_12h": e.tokens.cacheWrite12h, "cache_write": e.tokens.cacheWrite,
			"cache_read": e.tokens.cacheRead, "output": e.tokens.output,
		},
		"complete": e.complete, "why": e.why, "output_max": e.outputMax,
	}
}

// servedCap is one request's capture. The door sets what was asked (request),
// the served hop arms it (hop), the lane's source fills the tokens, and the
// door's finish queues the event. Once finished it is sealed: a late write (a
// codex translator still running after the client left) changes nothing.
type servedCap struct {
	mu sync.Mutex

	// what was asked, from the door
	inference   bool
	selection   string
	speedAsked  string // the request's own speed field, "" when it has none
	outputMax   *int   // the request's own max_tokens, nil when it has none
	serverTools bool   // the request names a server-side tool (reqFacts)

	// the hop that answered: account for any status (the log line's acct=),
	// the rest only once armed (a 2xx on inference with an account)
	account  string
	armed    bool
	kind     string
	consumer string
	member   string
	sent     string // the model sent upstream
	speed    *string
	paid     bool
	headAt   time.Time                // when the head arrived
	readings map[string]servedReading // what its headers read

	anthropic bool // the hop dialled Anthropic's own API (servedHopFacts)

	// what the response said
	usage tapUsage // the Anthropic shape's usage, field by field
	codex *servedTokens
	model string // the response's own model field
	final bool   // the final usage arrived
	why   string // why it did not, when known

	// an error the upstream sent inside its 2xx (inBand): errWhy names it
	// for the event, refused drops the event (a refusal, not a serve)
	errWhy  string
	refused bool

	finished bool
	seq      int64
}

type servedCapKey struct{}

// withServedCap puts a new capture on ctx.
func withServedCap(ctx context.Context) (context.Context, *servedCap) {
	c := &servedCap{}
	return context.WithValue(ctx, servedCapKey{}, c), c
}

func servedCapOf(ctx context.Context) *servedCap {
	if ctx == nil {
		return nil
	}
	c, _ := ctx.Value(servedCapKey{}).(*servedCap)
	return c
}

// request records what the client asked: selection is the model it named
// ("[1m]", the picker's window mark this proxy renders, taken off); from f,
// read off the client's own body, its speed field, its max_tokens and whether
// it names a server-side tool; inference, whether the request is one the
// account answers with tokens (not count_tokens, not a listing).
func (c *servedCap) request(selection string, f reqFacts, inference bool) {
	if c == nil {
		return
	}
	selection = strings.TrimSuffix(selection, "[1m]")
	c.mu.Lock()
	c.selection, c.speedAsked, c.inference = selection, f.Speed, inference
	c.outputMax, c.serverTools = f.MaxTokens, f.ServerTools
	c.mu.Unlock()
}

// servedHopFacts is what the served hop knows of itself: head is when its
// response head arrived, readings what that head's headers read.
type servedHopFacts struct {
	kind, consumer, member, sent string
	speed                        *string
	paid                         bool
	anthropic                    bool // the hop dialled Anthropic's own API
	head                         time.Time
	readings                     map[string]servedReading
}

// hop records the account the final hop dialled, and arms the capture when
// that hop answered an inference request with a 2xx. facts, called only then
// and outside the lock, is given the speed the request asked for.
func (c *servedCap) hop(account string, status int, facts func(speedAsked string) servedHopFacts) {
	if c == nil || account == "" {
		return
	}
	c.mu.Lock()
	if c.finished {
		c.mu.Unlock()
		return
	}
	c.account = account
	arm := c.inference && status >= 200 && status < 300
	asked := c.speedAsked
	if !arm {
		c.armed = false
	}
	c.mu.Unlock()
	if !arm {
		return
	}
	f := facts(asked)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return
	}
	c.armed, c.kind, c.consumer, c.member, c.sent, c.speed, c.paid = true, f.kind, f.consumer, f.member, f.sent, f.speed, f.paid
	c.anthropic = f.anthropic
	c.headAt, c.readings = f.head, f.readings
	c.usage, c.codex, c.model, c.final, c.why = tapUsage{}, nil, "", false, ""
	c.errWhy, c.refused = "", false
}

// inBand records an error the upstream sent inside its 2xx: status is the
// status the proxy gives it (the client's, or what its error type maps to),
// what its type or code, streamed whether output reached the client before
// it. Before any output, a 4xx (429 included) is a refusal or a request
// error, not a serve: the request sends no served event (a quota refusal
// still goes as a refusal report, from where it always has). Otherwise the
// event goes incomplete, its why naming the error, with whatever usage the
// upstream reported. The first error wins.
func (c *servedCap) inBand(status int, what string, streamed bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished || c.refused || c.errWhy != "" {
		return
	}
	if status >= 400 && status < 500 && !streamed {
		c.refused = true
		return
	}
	if what == "" {
		what = "unknown error"
	}
	c.errWhy = "error in-band: " + what
}

// armedAs reports whether the capture is armed for a hop of kind.
func (c *servedCap) armedAs(kind string) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.armed && !c.finished && c.kind == kind
}

// noteAnth takes one Anthropic-shaped usage block (message_start's, a
// message_delta's, or a JSON reply's), field by field; final says it is the
// response's final usage.
func (c *servedCap) noteAnth(model string, usage json.RawMessage, final bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return
	}
	if model != "" {
		c.model = model
	}
	c.usage.overlay(usage)
	if final {
		c.final = true
	}
}

// noteCodex takes a codex response's usage as the upstream reported it
// (codexServedTokens), output_tokens whole, reasoning included. A response
// with no usage block notes nothing.
func (c *servedCap) noteCodex(r *codexRespObj) {
	if c == nil || r == nil || r.Usage == nil {
		return
	}
	t := codexServedTokens(r.Usage)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finished {
		return
	}
	c.codex, c.final = &t, true
	if r.Model != "" {
		c.model = r.Model
	}
}

// codexServedTokens splits a codex usage block into the served event's
// disjoint kinds. Field names per OpenAI's prompt-caching guide
// (developers.openai.com/api/docs/guides/prompt-caching): input_tokens is the
// whole prompt; input_tokens_details.cached_tokens and .cache_write_tokens
// are parts of it, and .cache_write_tokens_details gives the write by
// lifetime, ttl_30m_tokens and ttl_12h_tokens, billed apart.
//
// Each part is clamped to what the prompt has left, in that order (read,
// 30m, 12h), so input + cache_read + the writes never exceed input_tokens,
// as codexUsage clamps its subsets. The write by lifetime:
//   - with the details, each as given (one it leaves out is null);
//   - with the total alone, all of it at the lifetime the request asked for.
//     The proxy builds every codex body itself (translateAnthropicToCodex) and
//     never sets prompt_cache_options.ttl, so that is the default, 30 minutes,
//     and 12 hours is 0;
//   - with no cache_write_tokens, neither is known: null.
//
// cache_write (a total of unknown lifetimes) stays null on this lane.
func codexServedTokens(u *codexUsageObj) servedTokens {
	out := u.OutputTokens
	rest := max(u.InputTokens, 0)
	t := servedTokens{output: &out}
	if d := u.InputTokensDetails; d != nil {
		read := clampToBudget(d.CachedTokens, rest)
		rest -= read
		t.cacheRead = &read
		take := func(n *int) *int {
			if n == nil {
				return nil
			}
			v := clampToBudget(*n, rest)
			rest -= v
			return &v
		}
		switch s := d.CacheWriteTokensDetails; {
		case s != nil && (s.TTL30m != nil || s.TTL12h != nil):
			t.cacheWrite30m = take(s.TTL30m)
			t.cacheWrite12h = take(s.TTL12h)
		case d.CacheWriteTokens != nil:
			zero := 0
			t.cacheWrite30m, t.cacheWrite12h = take(d.CacheWriteTokens), &zero
		}
	}
	t.input = &rest
	return t
}

// incomplete names why the final usage never came, unless it came or a
// reason is already named.
func (c *servedCap) incomplete(why string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.finished && !c.final && c.why == "" {
		c.why = why
	}
}

// finish seals the capture and, when a hop armed it, queues its event; seq is
// the event's (0: none). first is false when it was already finished. left
// says the client had gone by then (its request's context was cancelled):
// whatever ended the response early was its leaving, so that is the why,
// over a cut the hang-up caused or a source (the codex translator, on its
// own goroutine) that has not said so yet.
func (c *servedCap) finish(left bool) (seq int64, first bool) {
	if c == nil {
		return 0, false
	}
	c.mu.Lock()
	if c.finished {
		seq = c.seq
		c.mu.Unlock()
		return seq, false
	}
	c.finished = true
	if left && !c.final && (c.why == "" || c.why == servedWhyCut) {
		c.why = servedWhyLeft
	}
	if !c.armed || c.refused {
		c.mu.Unlock()
		return 0, true
	}
	ev := c.eventLocked()
	c.mu.Unlock()
	seq = reportServedEvent(ev)
	c.mu.Lock()
	c.seq = seq
	c.mu.Unlock()
	return seq, true
}

// servedReadAt is the event's read_at for a response that ended at end: its
// head's wall time plus the time since the head, which Go measures on the
// monotonic clock when both readings carry it (time.Now's do), so a wall
// clock stepped back mid-response can't make read_at precede head_at. A time
// without a monotonic reading (a test's fixed nowFn) falls back to the wall
// difference. No head (zero) is end's own wall time.
func servedReadAt(head, end time.Time) float64 {
	if head.IsZero() {
		return unixSeconds(end)
	}
	return unixSeconds(head) + end.Sub(head).Seconds()
}

// eventLocked builds the event from what was captured. Caller holds mu.
func (c *servedCap) eventLocked() servedEvent {
	ev := servedEvent{account: c.account, readAt: servedReadAt(c.headAt, nowFn()), paid: c.paid,
		headAt: unixSeconds(c.headAt), readings: c.readings,
		consumer: strOrNil(c.consumer), selection: strOrNil(c.selection), member: strOrNil(c.member),
		speed: c.speed}
	model := c.model
	if model == "" {
		model = c.sent
	}
	ev.model = strOrNil(model)
	switch c.kind {
	case servedPassthrough:
		u := c.usage
		ev.tokens.input, ev.tokens.cacheRead = u.InputTokens, u.CacheReadInput
		if cc := u.CacheCreation; cc != nil && (cc.Ephemeral5m != nil || cc.Ephemeral1h != nil) {
			ev.tokens.cacheWrite5m, ev.tokens.cacheWrite1h = cc.Ephemeral5m, cc.Ephemeral1h
		} else {
			ev.tokens.cacheWrite = u.CacheCreationInput
		}
		if c.final {
			ev.tokens.output = u.OutputTokens
		}
	case servedCodex:
		if c.codex != nil {
			ev.tokens = *c.codex
		}
	}
	ev.complete = c.final && c.errWhy == ""
	if c.errWhy != "" {
		c.why = c.errWhy
	}
	switch c.kind {
	case servedImage:
		ev.complete, c.why = false, servedWhyImage
	case servedOtherWire:
		ev.complete, c.why = false, servedWhyWire
	}
	if !ev.complete {
		why := c.why
		if why == "" {
			why = servedWhyCut
		}
		ev.why = &why
		// A request hung up on after its answer's start stated the input,
		// served by Anthropic itself, naming no server-side tool: what it
		// drew is that input and at most its max_tokens of output (66 §1,
		// §4). Another provider on the same wire (Kimi) is left out: what
		// its start states is not known.
		hungUp := why == servedWhyLeft || why == servedWhyCut
		if hungUp && c.kind == servedPassthrough && c.anthropic && c.outputMax != nil && !c.serverTools &&
			ev.tokens.input != nil && ev.tokens.output == nil {
			m := *c.outputMax
			ev.outputMax = &m
		}
	}
	return ev
}

// logTag is " srv=<seq> acct=<account>" for the request's log line: srv when
// an event was queued, acct when an account answered (any status).
func (c *servedCap) logTag() string {
	if c == nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	tag := ""
	if c.seq > 0 {
		tag += " srv=" + strconv.FormatInt(c.seq, 10)
	}
	if c.account != "" {
		tag += " acct=" + logToken(c.account)
	}
	return tag
}

// logToken keeps a value to one log field: anything but letters, digits and
// ._:@+- becomes "_", so a value can neither split the line's fields nor the
// tag's own separators.
func logToken(v string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case strings.ContainsRune("._:@+-", r):
			return r
		}
		return '_'
	}, v)
}

func strOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// servedSpeed is what an Anthropic request asked for: fast when its body's
// speed is "fast", standard when it names none or "standard", nil for a value
// this proxy cannot tell.
func servedSpeed(asked string) *string {
	switch asked {
	case "", "standard":
		s := "standard"
		return &s
	case "fast":
		s := "fast"
		return &s
	}
	return nil
}

// liveSpeed is the speed the served hop ran at, as the response-side live
// events name it (respIdentJSON): the armed hop's speed, the one its served
// event is priced at ("standard" or "fast"), and "unknown" when that hop names
// none (an image, a speed field servedSpeed cannot tell) or no hop armed the
// capture (a refusal, an error, a request with no capture). Takes only c.mu,
// a leaf lock: nothing is called while it is held.
func (c *servedCap) liveSpeed() string {
	if c == nil {
		return speedUnknown
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.armed || c.speed == nil {
		return speedUnknown
	}
	return *c.speed
}

// rawOutput is the output count the served event bills for a codex hop, the
// upstream's own (codexServedTokens), for assistant_message's
// raw_output_tokens: nil when no codex response reported usage on this
// request's serving hop. Takes only c.mu, a leaf lock.
func (c *servedCap) rawOutput() *int {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.codex == nil || c.codex.output == nil {
		return nil
	}
	n := *c.codex.output
	return &n
}

// speedUnknown is the speed a live event names when no served hop says one.
const speedUnknown = "unknown"

// noteServedCap arms the request's capture from the hop that served (called
// by noteServedHop, which resolved account and the model sent, and parsed the
// response's headers into hr when its head arrived, at head).
func noteServedCap(hop *http.Request, info reqInfo, resp *http.Response, anthropicHost, account, sent string,
	head time.Time, hr headerReading) {
	c := servedCapOf(hop.Context())
	if c == nil {
		return
	}
	c.hop(account, resp.StatusCode, func(asked string) servedHopFacts {
		f := servedHopFacts{kind: servedPassthrough, sent: sent, paid: servedCheck(hop, info).paid(),
			head: head, readings: servedReadings(hr)}
		cc := info.classCtx
		switch {
		case cc != nil && cc.image != nil:
			f.kind = servedImage
		case wireFor(hop) != nil:
			f.kind = servedOtherWire
			if _, ok := wireFor(hop).(codexWire); ok {
				f.kind = servedCodex
			}
		}
		switch {
		case f.kind == servedImage:
			// an image request names no speed
		case f.kind == servedCodex:
			// As the ledger names it (armLedger): this proxy asks codex for
			// no speed mode, so every request is the standard speed.
			s := "standard"
			f.speed = &s
		case hop.URL.Host == anthropicHost:
			f.speed = servedSpeed(asked)
			f.anthropic = true
		default:
			s := "standard" // a pass-through provider has one speed
			f.speed = &s
		}
		if cc != nil {
			f.member = cc.served.snapshot().member
			pref.mu.RLock()
			f.consumer = pref.classes[cc.class].Consumer
			pref.mu.RUnlock()
		} else if lane := servedLane(hop, info); lane != "" {
			f.member = laneRouteID(lane)
		}
		return f
	})
}

// tapServed puts servedTap on a pass-through hop's body when the capture is
// armed for one. Called before any other reader wraps the body, so it reads
// the upstream's own bytes.
func tapServed(hop *http.Request, resp *http.Response) {
	c := servedCapOf(hop.Context())
	if !c.armedAs(servedPassthrough) || resp.Body == nil {
		return
	}
	if ce := resp.Header.Get("Content-Encoding"); ce != "" && !strings.EqualFold(ce, "identity") {
		c.incomplete(servedWhyEncoded + " (" + ce + ")")
		return
	}
	stream := strings.Contains(resp.Header.Get("Content-Type"), "event-stream")
	resp.Body = &servedTap{inner: resp.Body, cap: c, stream: stream}
}

// ---- the pass-through tap ----

// servedLineMax bounds a line servedTap reads: message_start, message_delta
// and error lines are a few hundred bytes. A longer one is skipped, counted
// when it was one of those.
// servedJSONWhole bounds a JSON reply kept and parsed whole; past it only its
// head and a rolling tail are kept, where its model and its usage are, so
// the read that returns EOF parses a few KiB whatever the reply's size.
var (
	servedLineMax   = 64 << 10
	servedJSONWhole = 32 << 10
)

const (
	servedJSONHead = 4 << 10
	servedJSONTail = 16 << 10
	// servedSniff is how much of a line the tap needs to know what it is.
	servedSniff = 48
)

// servedTap reads the upstream's bytes as they pass. On a stream it walks
// each chunk's lines once (one IndexByte pass), names each line by its start
// (an `event:` line, or the data's leading "type"), and parses only
// message_start, message_delta and error lines; the unfinished last line is
// kept only while it may be one of those. On a JSON reply it keeps a bounded
// head and tail and parses them once, at the end. It settles once, at the
// body's end (EOF, a read error, or Close).
type servedTap struct {
	inner  io.ReadCloser
	cap    *servedCap
	stream bool

	cur      []byte // the line begun in an earlier chunk
	long     bool   // that line is being skipped
	skips    int    // wanted lines too long to read
	event    string // the current event's `event:` name
	streamed bool   // a content block reached the client

	buf   []byte // JSON: the body, or its head once it passed servedJSONWhole
	tail  []byte // JSON past servedJSONWhole: its last servedJSONTail bytes
	large bool

	dead, done bool
}

func (t *servedTap) Read(p []byte) (int, error) {
	n, err := t.inner.Read(p)
	if n > 0 && !t.dead {
		t.feed(p[:n])
	}
	if err != nil {
		why := servedWhyCut
		if err == io.EOF {
			why = ""
		}
		t.settle(why)
	}
	return n, err
}

func (t *servedTap) Close() error {
	err := t.inner.Close()
	t.settle(servedWhyLeft)
	return err
}

// feed is the per-chunk work. It never retains p.
func (t *servedTap) feed(p []byte) {
	defer func() {
		if recover() != nil {
			t.dead = true
			t.cap.incomplete(servedWhyLossy)
		}
	}()
	if !t.stream {
		t.keepJSON(p)
		return
	}
	for len(p) > 0 {
		j := bytes.IndexByte(p, '\n')
		if j < 0 {
			t.hold(p)
			return
		}
		l := p[:j]
		p = p[j+1:]
		if len(t.cur) == 0 && !t.long {
			t.line(l)
			continue
		}
		if !t.long {
			if len(t.cur)+len(l) <= servedLineMax {
				t.cur = append(t.cur, l...)
				t.line(t.cur)
			} else if t.wanted(t.cur) {
				t.skips++
			}
		}
		t.cur, t.long = t.cur[:0], false
	}
}

// hold keeps the unfinished last line while it may be wanted, up to
// servedLineMax.
func (t *servedTap) hold(p []byte) {
	if t.long {
		return
	}
	t.cur = append(t.cur, p...)
	switch {
	case len(t.cur) > servedLineMax:
		if t.wanted(t.cur) {
			t.skips++
		}
		t.cur, t.long = t.cur[:0], true
	case len(t.cur) >= servedSniff && !t.wanted(t.cur):
		t.noteKind(t.cur) // a content block's start is seen here
		t.cur, t.long = t.cur[:0], true
	}
}

// lineKind names an SSE line by its start: "event:<name>", the data's
// leading "type", or "" (a blank line, a comment, data of no leading type).
func (t *servedTap) lineKind(l []byte) (event bool, kind string) {
	if name, ok := bytes.CutPrefix(l, []byte("event:")); ok {
		return true, string(bytes.TrimSpace(name))
	}
	data, ok := bytes.CutPrefix(l, []byte("data:"))
	if !ok {
		return false, ""
	}
	if t.event != "" {
		return false, t.event
	}
	data = bytes.TrimLeft(data, " ")
	rest, ok := bytes.CutPrefix(data, []byte(`{"type":"`))
	if !ok {
		return false, ""
	}
	if end := bytes.IndexByte(rest, '"'); end >= 0 {
		return false, string(rest[:end])
	}
	return false, string(rest) // cut short: a prefix of the type
}

// wanted says whether a line (or a line's start) may be one the tap parses.
func (t *servedTap) wanted(l []byte) bool {
	event, kind := t.lineKind(l)
	if event {
		return true
	}
	for _, w := range []string{"message_start", "message_delta", "error"} {
		if strings.HasPrefix(w, kind) && kind != "" || kind == w {
			return true
		}
	}
	return false
}

// noteKind marks output reaching the client.
func (t *servedTap) noteKind(l []byte) {
	if _, kind := t.lineKind(l); strings.HasPrefix(kind, "content_block") {
		t.streamed = true
	}
}

// line reads one SSE line.
func (t *servedTap) line(l []byte) {
	l = bytes.TrimSuffix(l, []byte("\r"))
	if len(l) == 0 {
		t.event = "" // the event is dispatched
		return
	}
	event, kind := t.lineKind(l)
	if event {
		t.event = kind
		return
	}
	switch {
	case kind == "message_start", kind == "message_delta", kind == "error":
	case strings.HasPrefix(kind, "content_block"):
		t.streamed = true
		return
	default:
		return
	}
	data, _ := bytes.CutPrefix(l, []byte("data:"))
	var ev struct {
		Type    string `json:"type"`
		Message *struct {
			Model string          `json:"model"`
			Usage json.RawMessage `json:"usage"`
		} `json:"message"`
		Usage json.RawMessage `json:"usage"`
		Error *struct {
			Type string `json:"type"`
		} `json:"error"`
	}
	if json.Unmarshal(bytes.TrimSpace(data), &ev) != nil {
		return
	}
	switch ev.Type {
	case "message_start":
		if ev.Message != nil {
			t.cap.noteAnth(ev.Message.Model, ev.Message.Usage, false)
		}
	case "message_delta":
		if len(ev.Usage) > 0 {
			t.cap.noteAnth("", ev.Usage, true)
		}
	case "error":
		what := ""
		if ev.Error != nil {
			what = ev.Error.Type
		}
		t.cap.inBand(anthErrorStatus(what), what, t.streamed)
	}
}

// anthErrorStatus is the HTTP status of an Anthropic error type, as its API
// answers it outside a stream; an unknown type is a server error.
func anthErrorStatus(typ string) int {
	switch typ {
	case "invalid_request_error":
		return http.StatusBadRequest
	case "authentication_error":
		return http.StatusUnauthorized
	case "billing_error":
		return http.StatusPaymentRequired
	case "permission_error":
		return http.StatusForbidden
	case "not_found_error":
		return http.StatusNotFound
	case "request_too_large":
		return http.StatusRequestEntityTooLarge
	case "rate_limit_error":
		return http.StatusTooManyRequests
	case "overloaded_error":
		return 529
	}
	return http.StatusInternalServerError
}

// keepJSON keeps a JSON reply: whole up to servedJSONWhole, then its head and
// a rolling tail.
func (t *servedTap) keepJSON(p []byte) {
	if !t.large {
		if len(t.buf)+len(p) <= servedJSONWhole {
			t.buf = append(t.buf, p...)
			return
		}
		t.large = true
		t.tail = append(t.tail, t.buf[max(0, len(t.buf)-servedJSONTail):]...)
		t.buf = t.buf[:min(len(t.buf), servedJSONHead):min(len(t.buf), servedJSONHead)]
		if len(t.buf) < servedJSONHead {
			t.buf = append(t.buf, p[:min(len(p), servedJSONHead-len(t.buf))]...)
		}
	}
	if len(p) >= servedJSONTail {
		t.tail = append(t.tail[:0], p[len(p)-servedJSONTail:]...)
		return
	}
	if over := len(t.tail) + len(p) - servedJSONTail; over > 0 {
		t.tail = append(t.tail[:0], t.tail[over:]...)
	}
	t.tail = append(t.tail, p...)
}

// settle reads what the body said, once: a JSON reply's usage, or, on a
// stream, why the final usage never came (why "" at a clean end).
func (t *servedTap) settle(why string) {
	if t.done {
		return
	}
	t.done = true
	defer func() {
		if recover() != nil {
			t.cap.incomplete(servedWhyLossy)
		}
	}()
	if t.stream && len(t.cur) > 0 && !t.long && why == "" {
		t.line(t.cur) // a last line with no newline after it
	}
	if !t.stream && !t.dead && why == "" {
		t.readJSON()
	}
	if why == "" && t.skips > 0 {
		why = servedWhyTooLarge
	}
	if why == "" {
		why = servedWhyCut
		if !t.stream {
			why = servedWhyNoUsage
		}
	}
	t.cap.incomplete(why) // a no-op once the final usage came
	t.buf, t.tail, t.cur = nil, nil, nil
}

// readJSON reads a JSON reply's model and usage: whole when it was kept
// whole, else its first "model" in the head and its last "usage" in the tail
// (an Anthropic reply names its model before its content and ends with its
// usage; inside the content a key is either escaped in a string or earlier
// than the usage).
func (t *servedTap) readJSON() {
	var r struct {
		Model string          `json:"model"`
		Usage json.RawMessage `json:"usage"`
	}
	if !t.large {
		if json.Unmarshal(t.buf, &r) != nil {
			return
		}
	} else {
		_ = jsonValueAfter(t.buf, `"model"`, false, &r.Model)
		_ = jsonValueAfter(t.tail, `"usage"`, true, &r.Usage)
	}
	if len(r.Usage) == 0 || bytes.Equal(r.Usage, []byte("null")) {
		return
	}
	var u tapUsage
	if json.Unmarshal(r.Usage, &u) != nil {
		return
	}
	t.cap.noteAnth(r.Model, r.Usage, u.OutputTokens != nil)
}

// jsonValueAfter decodes into v the value after key's first (or last) unescaped
// occurrence in b followed by a colon.
func jsonValueAfter(b []byte, key string, last bool, v any) error {
	k := []byte(key)
	for len(b) > 0 {
		i := bytes.Index(b, k)
		if last {
			i = bytes.LastIndex(b, k)
		}
		if i < 0 {
			return io.EOF
		}
		rest := bytes.TrimLeft(b[i+len(k):], " \t\r\n")
		if (i == 0 || b[i-1] != '\\') && len(rest) > 0 && rest[0] == ':' {
			return json.NewDecoder(bytes.NewReader(rest[1:])).Decode(v)
		}
		if last {
			b = b[:i]
		} else {
			b = b[i+len(k):]
		}
	}
	return io.EOF
}
