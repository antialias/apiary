package main

// ---- transcript tap (#60): "turn" events off /v1/messages ----
//
// Every /v1/messages body carries the COMPLETE conversation — every prior tool_use and
// tool_result. The proxy already buffers it (bufferBody), so a live transcript needs no
// client cooperation, no file tailing and no polling: the request IS the event.
//
// It works for every provider because the tap reads the body BEFORE rewriteModel and
// before any wire translation — codexWire.prepare replaces info.body with the
// Responses-shaped payload only afterwards. Anthropic, kimi, gpt, local and class
// routes therefore all present the same Anthropic `messages` array here, so the event
// shape is identical BY CONSTRUCTION rather than by three code paths agreeing.
//
// Three properties this file exists to hold:
//
//  1. THE COLD PATH COSTS NOTHING. tapTurn's first statement is #59's eventsArmed()
//     gate (two atomic loads). Everything below it — the body walk, the fingerprint,
//     the render — happens only while something is actually listening. This is why the
//     tap is one call and not `if eventsArmed() { publish("turn", buildTurn(...)) }` at
//     the call site: the gate has to be inside, or the payload gets built regardless.
//
//  2. IT CANNOT FAIL THE REQUEST. This is the live inference path for every client.
//     Nothing here panics on a
//     malformed body (encoding/json only — no hand-rolled scanning, no unchecked type
//     assertion, no unchecked index), nothing here is unbounded (fixed-size retention
//     ring, clipped strings, a swept conversation map), and nothing here blocks.
//     A body the tap cannot make sense of produces NO event, never an error.
//
//  3. hub.mu STAYS A LEAF. tapMu is released before publish() is reached — see
//     tapAdvance. events.go's (*evHub).append documents why that invariant is the whole
//     basis of "publish never blocks".

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Tunables are source vars, not env vars — same reasoning as events.go's block: tests
// shrink them, and an operator knob here would be a lever to reach for in an incident
// and the wrong one.
//
// The budgets are sized against evMaxEventBytes (32 KiB). tapTextBudget is deliberately
// far below it: JSON escaping expands text, and the marshal-to-measure check in
// tapTurn is the hard guarantee, not these.
var (
	tapMaxMessages = 12      // messages carried per event; older tail entries are elided
	tapMaxBlocks   = 16      // content blocks rendered per message
	tapMaxText     = 1 << 10 // per-block text cap, bytes
	tapTextBudget  = 8 << 10 // event-wide text cap, bytes
	tapKeyText     = 4 << 10 // how much of system + the first user message feeds the conversation fp
	tapMaxDepth    = 4       // content-flattening recursion cap (json's own cap is 10000)
	tapMaxConvs    = 512     // live conversations tracked
	tapConvTTL     = 6 * time.Hour
	tapMaxAnchors  = 4096    // reply ids remembered for thread continues (tapAnchors); shares tapConvTTL
	tapMaxAnchorID = 128     // reply ids longer than this are not filed; no real message id comes near it
	tapMaxIdent    = 256     // per-field cap on model/route/account strings, bytes
	tapReserve     = 4 << 10 // frame bytes held back for the SSE frame + evEnvelope wrapper
)

var tapTrue = []byte("true")

// ---- per-conversation cursor ----

// tapConv is what the tap remembers about one conversation: how far into the messages
// array it has already emitted, and which epoch that count belongs to.
type tapConv struct {
	epoch  int
	cursor int // messages already emitted in this epoch
	turns  int64
	seen   time.Time
}

var (
	tapMu    sync.Mutex
	tapConvs = map[string]*tapConv{}
)

// tapAdvance moves a conversation's cursor and reports what this request is owed: the
// index of the first message to emit, the epoch it belongs to, and whether this event
// opens a new one. It is the only thing that touches tapConvs.
//
// LOCK ORDER: tapMu is a leaf, like hub.mu, and the two are never held together —
// tapTurn calls this, unlocks, and only then reaches publish(). Nesting hub.mu inside
// anything is the one thing events.go's append() forbids.
func tapAdvance(key string, count int, now time.Time) (from, epoch int, compacted bool, turn int64) {
	tapMu.Lock()
	defer tapMu.Unlock()
	return tapAdvanceLocked(key, count, now)
}

// tapAdvanceLocked is tapAdvance's body, shared with tapAdvanceThread so a continue's
// anchor lookup and its cursor move happen under one hold of tapMu. Caller holds tapMu.
func tapAdvanceLocked(key string, count int, now time.Time) (from, epoch int, compacted bool, turn int64) {
	c := tapConvs[key]
	if c == nil {
		tapEvictLocked(now)
		c = &tapConv{epoch: 1}
		tapConvs[key] = c
	}
	if count < c.cursor {
		// The array got SHORTER. That is /compact (or Claude Code's mid-session
		// microcompaction, which drops old tool_result pairs while keeping the head)
		// replacing the array — the designed shape of the feature, not corruption.
		// Open a new epoch and re-emit from zero; reporting a truncated transcript
		// instead would be the actual data loss.
		c.epoch++
		c.cursor = 0
		compacted = true
	}
	from, epoch = c.cursor, c.epoch
	c.cursor = count
	c.seen = now
	if from < count {
		c.turns++
	}
	return from, epoch, compacted, c.turns
}

// tapEvictLocked bounds tapConvs. Caller holds tapMu.
//
// Conversations are unbounded in the wild — every session, every sub-agent, and every
// one-shot haiku title call mints one — so this map is a slow leak unless something
// prunes it. Sweep expired entries first (the normal case, and it usually clears the
// pressure on its own), then evict least-recently-seen until back under the cap.
func tapEvictLocked(now time.Time) {
	if len(tapConvs) < tapMaxConvs {
		return
	}
	for k, c := range tapConvs {
		if now.Sub(c.seen) > tapConvTTL {
			delete(tapConvs, k)
		}
	}
	for len(tapConvs) >= tapMaxConvs {
		oldest, at := "", time.Time{}
		for k, c := range tapConvs {
			if at.IsZero() || c.seen.Before(at) {
				oldest, at = k, c.seen
			}
		}
		if oldest == "" {
			return
		}
		delete(tapConvs, oldest)
	}
}

// ---- reply anchors: Claude Code's thread protocol ----
//
// After a conversation's first request (thread:{type:"create"}, the full array), Claude
// Code sends thread:{type:"continue", previous_message_id:"msg_…"}: `messages` is sliced
// to what follows that assistant reply, `system` is cut to its uncached blocks and
// `tools` is left out. The key's text — system plus the first user message — is then
// the newest delta, so keying a continue on content minted a new key on nearly every
// main-thread request (132 turns under 127 keys in one live session, against 69 turns
// under 4 keys for the same session's thread-less classifier calls).
//
// What a continue does name is the reply it follows, and that reply went through the
// response tap. So the response tap remembers each reply's id against the conversation
// it answered, and a continue that names it takes that conversation's key and numbering.
// Ids, keys and ints only: nothing here holds text.

// tapAnchor is one remembered reply: whose it was and where it sits.
type tapAnchor struct {
	key       string // the tapConvs key (full session id + "\x00" + content fp)
	sess      string // the full session id, so a continue never crosses sessions
	convKey   string // the emitted conversation_key
	contentFP string
	epoch     int
	index     int // the reply's index in its conversation = len(messages) of the request it answered
	seen      time.Time
}

// tapAnchors is guarded by tapMu, like tapConvs: the lookup and the cursor move it feeds
// must be one step, or two continues racing on one anchor could each read a stale cursor.
var tapAnchors = map[string]tapAnchor{}

// tapRememberReply records the reply id the response tap read off message_start. It is
// only ever reached from a response tap, which only exists while the hub is armed, so
// the cold path never gets here.
func tapRememberReply(msgID string, id *tapRespIdent, now time.Time) {
	if msgID == "" || len(msgID) > tapMaxAnchorID || id == nil || !id.ok || id.tapKey == "" {
		return
	}
	tapMu.Lock()
	defer tapMu.Unlock()
	if _, ok := tapAnchors[msgID]; !ok {
		tapEvictAnchorsLocked(now)
	}
	tapAnchors[msgID] = tapAnchor{
		key:       id.tapKey,
		sess:      id.sess,
		convKey:   id.convKey,
		contentFP: id.contentFP,
		epoch:     id.epoch,
		index:     id.index,
		seen:      now,
	}
}

// tapEvictAnchorsLocked bounds tapAnchors exactly as tapEvictLocked bounds tapConvs —
// every reply of every conversation lands here, so it fills far faster. Caller holds tapMu.
func tapEvictAnchorsLocked(now time.Time) {
	if len(tapAnchors) < tapMaxAnchors {
		return
	}
	for k, a := range tapAnchors {
		if now.Sub(a.seen) > tapConvTTL {
			delete(tapAnchors, k)
		}
	}
	for len(tapAnchors) >= tapMaxAnchors {
		oldest, at := "", time.Time{}
		for k, a := range tapAnchors {
			if at.IsZero() || a.seen.Before(at) {
				oldest, at = k, a.seen
			}
		}
		if oldest == "" {
			return
		}
		delete(tapAnchors, oldest)
	}
}

// tapThreadAdvance is what a continue whose anchor is known gets back from
// tapAdvanceThread.
type tapThreadAdvance struct {
	anchor    tapAnchor
	from      int
	epoch     int
	compacted bool
	turn      int64
	total     int // anchor.index + 1 + len(delta): the full conversation's length
}

// tapAdvanceThread looks up a continue's anchor and, on a hit, moves that conversation's
// cursor as if the request had carried the whole array: the earlier messages, the anchor
// reply at anchor.index, then the delta. ok=false is a miss — an unknown id (the proxy
// restarted, or another process served the create), another session's reply, or a
// conversation that has since been evicted or moved to a new epoch, where the anchor's
// index no longer means anything. A miss is never resolved by guessing.
func tapAdvanceThread(prevID, sess string, delta int, now time.Time) (tapThreadAdvance, bool) {
	tapMu.Lock()
	defer tapMu.Unlock()
	a, ok := tapAnchors[prevID]
	if !ok || a.sess != sess {
		return tapThreadAdvance{}, false
	}
	if c := tapConvs[a.key]; c == nil || c.epoch != a.epoch {
		return tapThreadAdvance{}, false
	}
	a.seen = now
	tapAnchors[prevID] = a
	r := tapThreadAdvance{anchor: a, total: a.index + 1 + delta}
	r.from, r.epoch, r.compacted, r.turn = tapAdvanceLocked(a.key, r.total, now)
	return r, true
}

// ---- the event ----

// tapBlock is one rendered content block. omitempty here (unlike the turn envelope) is
// deliberate: blocks are the bulk of the payload and their fields genuinely differ by
// type, so a fixed shape would spend the frame budget on nulls.
type tapBlock struct {
	Type      string `json:"type"`
	Text      string `json:"text,omitempty"`
	ID        string `json:"id,omitempty"`          // tool_use: what a later tool_result's tool_use_id names
	Name      string `json:"name,omitempty"`        // tool_use
	ToolUseID string `json:"tool_use_id,omitempty"` // tool_result
	Error     bool   `json:"is_error,omitempty"`    // tool_result
	Bytes     int    `json:"bytes,omitempty"`       // size of the SOURCE JSON value, so a clip is measurable
	Clipped   bool   `json:"clipped,omitempty"`     // the tap dropped part of this block
	Malformed bool   `json:"malformed,omitempty"`   // the block did not parse; nothing was inferred
}

type tapMessage struct {
	Index     int        `json:"index"` // position in THIS epoch's conversation (a thread continue's delta is offset past its anchor)
	Role      string     `json:"role"`
	Blocks    []tapBlock `json:"blocks"`
	Elided    int        `json:"elided_blocks,omitempty"`
	Malformed bool       `json:"malformed,omitempty"`
}

// tapTurnEvent is the "turn" payload. Every field is always present (no omitempty
// except class/member/bind_reason, which are genuinely absent off a class route)
// because the acceptance criterion is that a kimi-routed, gpt-routed and
// Anthropic-routed request produce an IDENTICAL shape — a stable schema is worth a
// few bytes of nulls.
type tapTurnEvent struct {
	DecisionID string `json:"decision_id,omitempty"`
	// ConversationKey is the ticket's escape hatch: if the keying below turns out to
	// be wrong (sub-agents are the part most likely to be), scry can regroup on this
	// composite, or on Session / ContentFP separately, with no proxy redeploy.
	ConversationKey string `json:"conversation_key"`
	Session         string `json:"session"`     // fpSess of x-claude-code-session-id
	SessionKey      string `json:"session_key"` // opaque full hash accepted by #66's enqueue endpoint
	ContentFP       string `json:"content_fp"`  // fingerprint(system + first user message)

	Epoch     int   `json:"epoch"`
	Compacted bool  `json:"compacted"` // this event opens a new epoch
	Turn      int64 `json:"turn"`      // nth emitted turn for this conversation

	From       int `json:"from"`  // index of the first message carried
	To         int `json:"to"`    // index of the last carried (inclusive)
	Total      int `json:"total"` // the conversation's length: len(messages), plus what a thread continue left out
	New        int `json:"new"`   // messages this event carries
	ElidedHead int `json:"elided_head"`
	// ThreadOrphan: a thread continue whose previous_message_id the tap has no record of
	// (a proxy restart, or a create another process served). It is keyed and numbered on
	// its own delta, as every request was before continues were followed — a new
	// conversation whose head is missing, not a guess at which one it belongs to.
	// omitempty: absent is the normal case.
	ThreadOrphan bool `json:"thread_orphan,omitempty"`

	ModelRequested string `json:"model_requested"`
	ModelUpstream  string `json:"model_upstream"`
	Translated     bool   `json:"translated"` // served by a translating wire (codex) — a wire fact, not a model-name guess
	Lane           string `json:"lane"`
	Class          string `json:"class,omitempty"`
	Member         string `json:"member,omitempty"`
	// BindReason is the brain's stated reason for the class binding this turn was
	// routed on (#86). omitempty like class/member, and for a stronger reason: a
	// routine re-bind carries none, so its ABSENCE is the common case and an empty
	// string on every turn would read as "no reason given" rather than "not asked".
	BindReason string `json:"bind_reason,omitempty"`
	Account    string `json:"account"`
	State      string `json:"state"`
	Route      string `json:"route"` // the tag requestHandler already computed
	ClientFP   string `json:"client_fp"`
	// Entry / Client: which front door served this turn (#126) and, for the
	// HTTPS-proxy entry, which machine's proxy login. omitempty on BOTH, and
	// that is load-bearing, not a style choice: base-URL never sets an entry, so
	// its event bytes are identical to before these fields existed. Scry's
	// routeDecisionSchema is a non-strict z.object, so an older consumer drops
	// them; they are the hook T7 joins Remote Control sessions on.
	Entry  string `json:"entry,omitempty"`
	Client string `json:"client,omitempty"`
	// Device / User: the #128 Scry device id and the approving person's opaque
	// Scry user id, present only on a device-token login. omitempty for the
	// same reason as Entry/Client. Never an email, and never the token.
	Device string `json:"device,omitempty"`
	User   string `json:"user,omitempty"`
	// Beta1M / Effort: the client's own window claim (anthropic-beta context-1m)
	// and reasoning effort, as sent. Neither is rewritten by the proxy; both are
	// invisible in the transcript and decide compaction ceiling and thinking cost.
	Beta1M bool   `json:"beta_1m"`
	Effort string `json:"effort,omitempty"`
	// Harness is the registry id (harness.go) of the harness that sent this
	// turn, the same value the route_decision and every response-side event
	// (respIdentJSON) carry, so a resend that publishes no turn event still
	// names its harness from turn_started on. Folder / Platform are
	// what the session's create said it works in, and Name the name its last
	// /rename gave it (session_labels.go), looked up by the FULL session id;
	// omitempty because each is unknown until this process has seen the
	// request that says it.
	Harness  string `json:"harness"`
	Folder   string `json:"folder,omitempty"`
	Platform string `json:"platform,omitempty"`
	Name     string `json:"name,omitempty"`

	Stream         bool `json:"stream"`
	BodyBytes      int  `json:"body_bytes"`
	Truncated      bool `json:"truncated"`
	MessagesElided int  `json:"messages_elided"`

	Messages []tapMessage `json:"messages"`
}

// tapTurn emits one "turn" event for a /v1/messages request, and returns the
// conversation identity so requestHandler can hand it to the RESPONSE tap (#61) —
// the two sides join on it. The zero value (ok=false) means "don't tap the response
// either": not armed, not a turn-shaped request, or a body we couldn't parse.
//
// ctx is the ROUTE context — it carries the reqInfo requestHandler resolved — not a
// cancellation scope; the tap never blocks and never waits on anything.
func tapTurn(ctx context.Context, r *http.Request, body []byte, model, tag, eff, clientFP string) tapRespIdent {
	// The gate, first statement, before anything can allocate. count_tokens is
	// excluded by exact path match: it carries the same conversation but is a
	// pricing probe, not a turn, and it would double every event.
	if !eventsArmed() || r == nil || r.URL == nil || r.URL.Path != "/v1/messages" || len(body) == 0 {
		return tapRespIdent{}
	}

	s, ok := tapScanBody(body)
	if !ok {
		// Not a body we understand. Say nothing rather than guess — a wrong
		// transcript is worse than a missing one; the response side stays silent
		// with it (no identity to hang the reply on).
		return tapRespIdent{}
	}

	sess := r.Header.Get("x-claude-code-session-id")
	if sess == "" {
		sess = "no-session" // curl and friends; matches the class-routing fallback
	}

	// THE KEY. The session id alone is not enough: a sub-agent (Task tool) issues its
	// own /v1/messages with its own messages array under, most likely, the same
	// session id, and keying on the session alone would interleave its turns into the
	// parent transcript.
	//
	// The second component is the flattened TEXT of the system prompt plus the first
	// user message, never their raw bytes — Claude Code moves its cache_control
	// breakpoints between requests, so the raw bytes of messages[0] are NOT stable
	// across a conversation. Keying on them minted a new key mid-conversation and
	// re-sent the whole history; that is a falsified implementation, not a hypothesis.
	//
	// The text leaves out what Claude Code rewrites on every request: its billing-header
	// block (tapSystemKeyText) and any role:"system" message ahead of the first user one
	// (tapScanMessages). Keying on them gave nearly every request its own key.
	//
	// A thread continue (see tapAnchors) has neither to offer: its system and first message
	// are the newest delta. It takes the key of the conversation whose reply it names,
	// and its messages are numbered from just past that reply — so `total` and every
	// index count the full conversation, and a sliced array never reads as a shrink.
	sessionKey := injectObserve(sess, time.Now())
	var (
		key, contentFP         string
		from, epoch, total     int
		base                   int // full-conversation index of the body's messages[0]
		compacted, orphan, hit bool
		turn                   int64
		th                     tapThreadAdvance
	)
	if s.threadType == "continue" {
		th, hit = tapAdvanceThread(s.threadPrev, sess, s.count, time.Now())
		orphan = !hit
	}
	if hit {
		key, contentFP = th.anchor.key, th.anchor.contentFP
		from, epoch, compacted, turn, total = th.from, th.epoch, th.compacted, th.turn, th.total
		base = th.anchor.index + 1
	} else {
		sysText := tapSystemKeyText(tapSlice(body, s.sys))
		firstText, _ := tapText(tapSlice(body, s.first), tapKeyText, tapMaxDepth)
		contentFP = tapFP(sysText, firstText)
		key = sess + "\x00" + contentFP // the FULL session id, so two sessions never merge
		from, epoch, compacted, turn = tapAdvance(key, s.count, time.Now())
		total = s.count
	}

	// The identity handed to the response tap. Built BEFORE the nothing-new check:
	// a request that re-sends an identical array is not a new turn, but its RESPONSE
	// is still a real reply worth tapping — a blown streaming turn retries with the
	// same messages, and the retry's answer is exactly what the lost stream held.
	id := tapRespIdent{
		ok:         true,
		convKey:    tapFP(sess) + "/" + contentFP,
		tapKey:     key,
		sess:       sess,
		session:    fpSess(sess),
		sessionKey: sessionKey,
		contentFP:  contentFP,
		epoch:      epoch,
		turn:       turn,
		index:      total, // the reply lands after the conversation's messages 0..total-1
		modelReq:   model,
		modelUp:    model,
		route:      strings.TrimSpace(tag),
		account:    eff,
		stream:     s.stream,
		harness:    harnessFrom(ctx),
		cap:        servedCapOf(ctx),
	}

	// ctx is never nil from requestHandler, but this function's contract is that it
	// cannot be the thing that takes a request down, so it does not assume that.
	info, has := reqInfo{}, false
	if ctx != nil {
		info, has = ctx.Value(infoKey).(reqInfo)
	}
	if d := decisionFrom(ctx); d != nil {
		id.decisionID = d.record.DecisionID
	}
	if has {
		id.lane = info.lane
		id.ratio = info.ratio
		id.scale = info.scale
		if info.classCtx != nil {
			id.served = info.classCtx.served
		}
		// A wire FACT, not a model-name guess: codexWire is the one translating wire
		// (wire.go's translatingWires), and class routes carry the member's wire too.
		// Known limit: a mid-request class cross INTO codex (the member ladder) keeps
		// the pre-flight label; the response events' model_stream makes the
		// disagreement visible.
		_, id.translated = info.wire.(codexWire)
		switch {
		case info.classCtx != nil:
			if m := info.classCtx.member.Model; m != "" {
				id.modelUp = m
			}
		case info.wire != nil:
			// The direct route's eff is "<provider>:<upstream-model>" (the contract
			// wire_codex.go's translate() comment names). Read the upstream id off it
			// rather than re-scanning info.body: a translated wire's body is a
			// Responses payload whose marshalled key order puts "model" AFTER the
			// whole input array, so extractModel there would walk multiple MB.
			if _, up, cut := strings.Cut(info.eff, ":"); cut && up != "" {
				id.modelUp = up
			}
		}
	}

	// What this body cannot carry on a hit: everything before its messages[0]. The
	// anchor reply at base-1 went out as the response tap's assistant_message, so a
	// cursor parked on it (the normal case) owes nothing. A cursor further back means a
	// gap, and a continue that shrank the conversation (a new epoch) re-emits from zero
	// with no head to send — both are elided, never skipped quietly.
	elidedHead := 0
	if hit && from < base {
		if gap := base - 1 - from; compacted {
			elidedHead = base - from
		} else if gap > 0 {
			elidedHead = gap
		}
		from = base
	}

	if from >= total {
		// Nothing new. A request that re-sends an identical array (a client retry, a
		// one-shot probe) is not a turn; emitting it would make the transcript claim
		// a turn happened that did not. The identity still goes back so the response
		// tap can catch the reply.
		return id
	}

	// Only the last tapMaxMessages ranges were retained, so a cursor that is further
	// behind than that yields a head elision rather than a lie about continuity.
	firstKept := base + s.count - len(s.tail)
	if from < firstKept {
		elidedHead += firstKept - from
		from = firstKept
	}
	sel := s.tail[from-firstKept:]
	msgs, clipped := tapRender(body, sel, from)

	// Parsed here, on the armed path only: the cold path must stay allocation-free
	// (TestTapColdPathNoAlloc) and the facts are only rendered into this event.
	// The base facts only: the harness comes from ctx and the folder and name
	// from the label store, all worked out on the request path, so the armed
	// path does no decoding beyond what it did before they existed.
	facts := parseReqFactsOpt(r.Header, body, false)
	ent := entryFrom(ctx)
	label := sessionLabelsLookup(r.Header.Get("x-claude-code-session-id"), time.Now())
	ev := tapTurnEvent{
		DecisionID: id.decisionID,
		// tapFP over the FULL session id, not fpSess. fpSess is an 8-char PREFIX, kept
		// below as Session because that is the form the brain and our logs display and
		// joining on it is the whole point. But a prefix is the wrong thing to key on:
		// the cursor key above uses the full id precisely "so two sessions never
		// merge", and emitting a prefix would hand scry a key that merges what the
		// proxy kept apart. It also stops depending on the id's internal structure —
		// today's ids are UUIDv4 so the prefix is random, but a switch to UUIDv7 would
		// make the first 8 chars a timestamp shared by every session for ~49 days.
		ConversationKey: id.convKey,
		Session:         id.session,
		SessionKey:      sessionKey,
		ContentFP:       contentFP,
		Epoch:           epoch,
		Compacted:       compacted,
		Turn:            turn,
		From:            from,
		To:              total - 1,
		Total:           total,
		New:             len(sel),
		ElidedHead:      elidedHead,
		ThreadOrphan:    orphan,
		ModelRequested:  model,
		ModelUpstream:   id.modelUp, // already refined from reqInfo above
		Translated:      id.translated,
		Lane:            id.lane,
		Route:           id.route,
		Account:         eff,
		ClientFP:        clientFP,
		Entry:           ent.kind,
		Client:          ent.client,
		Device:          ent.device,
		User:            ent.user,
		Beta1M:          facts.OneMega,
		Effort:          facts.Effort,
		Harness:         id.harness,
		Folder:          label.Folder,
		Platform:        label.Platform,
		Name:            label.Name,
		State:           "REFUSED", // no reqInfo reached ctx => short-circuited to 503
		Stream:          s.stream,
		BodyBytes:       len(body),
		// Truncated means exactly "this event does not carry everything the request
		// held" — a head elision, a clipped block or a dropped one all count, so a
		// consumer never has to diff the counts to find out.
		Truncated: clipped || elidedHead > 0,
		Messages:  msgs,
	}
	if has {
		ev.State = info.state.String()
		if info.bypass && info.classCtx == nil {
			ev.State = "DIRECT" // direct providers sit outside the account state machine
		}
		if info.classCtx != nil {
			ev.Class = info.classCtx.class
			ev.Member = info.classCtx.member.ID
			ev.BindReason = info.classCtx.bindReason
		}
	}

	// Metadata is not free either, and two of these fields are sized by the CLIENT:
	// ModelRequested is its `model` string verbatim, and Route embeds that same string
	// in the unroutable branches. Sizing only the messages left a 40 KB model name free
	// to blow the frame past evMaxEventBytes — the exact outcome the block below exists
	// to prevent, reached around it. Clamp first, then measure the whole event.
	//
	// The clamps are the fix; measuring `ev` rather than `ev.Messages` is what makes the
	// budget exact instead of reserved (tapReserve no longer has to guess at metadata)
	// and keeps it true for whatever field someone adds next. With the clamps in place
	// the metadata cannot reach the ceiling on its own, so that half is a guard, not a
	// live check — don't remove the clamps on the strength of it.
	ev.ModelRequested = ev.ModelRequested[:tapRuneCut(ev.ModelRequested, tapMaxIdent)]
	ev.ModelUpstream = ev.ModelUpstream[:tapRuneCut(ev.ModelUpstream, tapMaxIdent)]
	ev.Route = ev.Route[:tapRuneCut(ev.Route, tapMaxIdent)]
	ev.Account = ev.Account[:tapRuneCut(ev.Account, tapMaxIdent)]
	ev.Class = ev.Class[:tapRuneCut(ev.Class, tapMaxIdent)]
	ev.Member = ev.Member[:tapRuneCut(ev.Member, tapMaxIdent)]
	// Brain-authored prose, so the only field here sized by something that is
	// neither us nor the client — clamped on exactly the same grounds.
	ev.BindReason = ev.BindReason[:tapRuneCut(ev.BindReason, tapMaxIdent)]

	// ONE marshal-to-measure, over the WHOLE event. The only thing that predicts a
	// payload's ESCAPED size is escaping it, and renderFrame's oversize backstop stubs
	// the WHOLE frame — envelope included. That is strictly worse here than dropping the
	// payload: the cursor has already advanced, so these messages are never re-sent, and
	// a stub carries no conversation key to hang the gap on. Losing the envelope loses
	// the fact that a turn happened at all.
	if b, err := json.Marshal(ev); err != nil || len(b) > evMaxEventBytes-tapReserve {
		ev.MessagesElided = len(ev.Messages)
		ev.Messages = []tapMessage{}
		ev.Truncated = true
	}

	publish("turn", ev)
	return id
}

// ---- body walk ----

// tapRange is a byte range within the buffered body. Ranges rather than copies: the
// body is already in memory and pinned by reqInfo, so retaining 12 pairs of ints costs
// nothing while retaining 12 message copies could cost megabytes.
type tapRange struct{ lo, hi int }

// tapScanned is everything one pass over the top-level object collects.
type tapScanned struct {
	sys           tapRange
	first         tapRange   // the first role:"user" message, else messages[0]
	tail          []tapRange // the last min(count, tapMaxMessages) messages, oldest-first
	count         int
	messagesClose int // byte offset of the messages array's closing ']' (for #66's splice)
	stream        bool
	threadType    string // top-level thread.type: "create", "continue", or "" when absent
	threadPrev    string // thread.previous_message_id
}

// tapThread is the slice of Claude Code's top-level `thread` object the tap reads.
type tapThread struct {
	Type string `json:"type"`
	Prev string `json:"previous_message_id"`
}

func tapSlice(body []byte, r tapRange) []byte {
	if r.hi <= r.lo || r.lo < 0 || r.hi > len(body) {
		return nil
	}
	return body[r.lo:r.hi]
}

// tapScanBody walks the request body ONCE. ok=false means "this is not a shape we can
// speak about" — a non-object, a missing or non-array `messages`, any parse error.
//
// The walk is O(body) and that is unavoidable: the cursor is an index into the messages
// array, so the count has to be known, and the count is only known at the end. What it
// avoids is O(body) ALLOCATION — the skip buffer is reused (json.RawMessage's
// UnmarshalJSON appends into existing capacity) and only ranges are retained.
func tapScanBody(body []byte) (tapScanned, bool) {
	var s tapScanned
	dec := json.NewDecoder(bytes.NewReader(body))
	t, err := dec.Token()
	if err != nil {
		return s, false
	}
	if d, ok := t.(json.Delim); !ok || d != '{' {
		return s, false
	}

	var skip json.RawMessage
	haveMsgs := false
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return s, false
		}
		key, _ := kt.(string)
		switch {
		case key == "messages" && !haveMsgs: // a duplicate key is hostile; first wins
			if !tapScanMessages(dec, body, &s) {
				return s, false
			}
			haveMsgs = true
		case key == "system":
			s.sys = tapValueRange(dec, body, &skip)
		case key == "stream":
			r := tapValueRange(dec, body, &skip)
			s.stream = bytes.Equal(tapSlice(body, r), tapTrue)
		case key == "thread":
			// Two short strings. A thread that isn't this shape reads as no thread,
			// which keys the request as before — the body is still a transcript.
			var th tapThread
			if err := dec.Decode(&skip); err != nil {
				return s, false
			}
			if json.Unmarshal(skip, &th) == nil {
				s.threadType = tapShort(th.Type, 16)
				s.threadPrev = th.Prev
			}
		default:
			if err := dec.Decode(&skip); err != nil {
				return s, false
			}
		}
	}
	if _, err := dec.Token(); err != nil { // the closing '}'
		return s, false
	}
	return s, haveMsgs
}

// tapScanMessages consumes the messages array, retaining a fixed-size ring of the last
// ranges. Returns false on anything that is not an array of values.
func tapScanMessages(dec *json.Decoder, body []byte, s *tapScanned) bool {
	t, err := dec.Token()
	if err != nil {
		return false
	}
	if d, ok := t.(json.Delim); !ok || d != '[' {
		return false // messages: null / a string / an object — not a transcript
	}
	keep := tapMaxMessages
	if keep < 1 {
		keep = 1
	}
	ring := make([]tapRange, 0, keep)
	var raw json.RawMessage
	// Claude Code sends role:"system" messages inside `messages` and re-renders them per
	// request, so the key's message is the first user one; messages[0] when none is.
	firstIsUser := false
	for dec.More() {
		r := tapValueRange(dec, body, &raw)
		if r.hi <= r.lo {
			return false
		}
		if s.count == 0 {
			s.first = r
		}
		if !firstIsUser && tapIsUser(raw) {
			s.first, firstIsUser = r, true
		}
		s.count++
		if len(ring) < keep {
			ring = append(ring, r)
		} else {
			copy(ring, ring[1:])
			ring[keep-1] = r
		}
	}
	s.messagesClose = int(dec.InputOffset())
	if _, err := dec.Token(); err != nil { // the closing ']'
		return false
	}
	s.tail = ring
	return true
}

// tapValueRange consumes the next value and returns its byte range in body. The zero
// range means "could not read it".
//
// json.Decoder.InputOffset is exact against body because the decoder reads from a
// bytes.Reader over body itself. The offset BEFORE the Decode sits after the previous
// token, so it may point at the ':' or ',' and any whitespace ahead of the value —
// tapValueStart walks those off. No JSON value can begin with one of them, so the walk
// cannot overshoot into the value.
func tapValueRange(dec *json.Decoder, body []byte, skip *json.RawMessage) tapRange {
	start := int(dec.InputOffset())
	if err := dec.Decode(skip); err != nil {
		return tapRange{}
	}
	end := int(dec.InputOffset())
	lo := tapValueStart(body, start, end)
	if lo < 0 || end > len(body) {
		return tapRange{}
	}
	return tapRange{lo, end}
}

func tapValueStart(body []byte, from, end int) int {
	if from < 0 {
		from = 0
	}
	if end > len(body) {
		end = len(body)
	}
	for i := from; i < end; i++ {
		switch body[i] {
		case ' ', '\t', '\r', '\n', ':', ',':
		default:
			return i
		}
	}
	return -1
}

// ---- rendering ----

// tapRender renders the selected messages under ONE shared text budget, and reports
// whether anything was dropped along the way.
func tapRender(body []byte, ranges []tapRange, firstIndex int) ([]tapMessage, bool) {
	budget := tapTextBudget
	clipped := false
	out := make([]tapMessage, 0, len(ranges))
	for i, r := range ranges {
		m := tapRenderMessage(tapSlice(body, r), firstIndex+i, &budget)
		clipped = clipped || m.Elided > 0
		for _, b := range m.Blocks {
			clipped = clipped || b.Clipped
		}
		out = append(out, m)
	}
	return out, clipped
}

func tapRenderMessage(raw []byte, index int, budget *int) tapMessage {
	m := tapMessage{Index: index, Role: "?", Blocks: []tapBlock{}}
	// A map, not a struct: every field is then read through a helper that tolerates
	// the wrong JSON type, so a hostile `"role": 7` degrades one field instead of
	// failing the whole message.
	var f map[string]json.RawMessage
	if json.Unmarshal(raw, &f) != nil {
		m.Malformed = true
		return m
	}
	if role := tapStr(f["role"]); role != "" {
		m.Role = tapShort(role, 32)
	}
	content := f["content"]
	if len(content) == 0 {
		return m
	}
	switch content[0] {
	case '"':
		m.Blocks = append(m.Blocks, tapRenderBlock(content, budget))
	case '[':
		dec := json.NewDecoder(bytes.NewReader(content))
		if _, err := dec.Token(); err != nil {
			m.Malformed = true
			return m
		}
		var el json.RawMessage
		for dec.More() {
			if err := dec.Decode(&el); err != nil {
				m.Malformed = true
				break
			}
			if len(m.Blocks) >= tapMaxBlocks {
				m.Elided++ // still counted: "how much did we not show you" is the point
				continue
			}
			m.Blocks = append(m.Blocks, tapRenderBlock(el, budget))
		}
	default:
		m.Malformed = true
	}
	return m
}

func tapRenderBlock(raw []byte, budget *int) tapBlock {
	if len(raw) == 0 {
		return tapBlock{Type: "?", Malformed: true}
	}
	if raw[0] == '"' { // a bare string where a block was expected: legal Anthropic content
		b := tapBlock{Type: "text", Bytes: len(raw)}
		b.Text, b.Clipped = tapFit(tapStr(raw), budget)
		return b
	}
	if raw[0] != '{' {
		return tapBlock{Type: "?", Malformed: true, Bytes: len(raw)}
	}
	var f map[string]json.RawMessage
	if json.Unmarshal(raw, &f) != nil {
		return tapBlock{Type: "?", Malformed: true, Bytes: len(raw)}
	}
	b := tapBlock{Type: tapShort(tapStr(f["type"]), 40)}
	if b.Type == "" {
		b.Type = "?"
	}
	switch b.Type {
	case "text":
		b.Bytes = len(f["text"])
		b.Text, b.Clipped = tapFit(tapStr(f["text"]), budget)
	case "thinking", "redacted_thinking":
		b.Bytes = len(f["thinking"])
		b.Text, b.Clipped = tapFit(tapStr(f["thinking"]), budget)
	case "tool_use", "server_tool_use", "mcp_tool_use":
		b.ID = tapShort(tapStr(f["id"]), 64)
		b.Name = tapShort(tapStr(f["name"]), 96)
		// The input IS the interesting half of a tool_use (the command, the path), and
		// it is already compact JSON — emit its source text clipped rather than
		// flattening it into prose that loses the argument names.
		b.Bytes = len(f["input"])
		b.Text, b.Clipped = tapFitRaw(f["input"], budget)
	case "tool_result", "mcp_tool_result", "web_search_tool_result":
		b.ToolUseID = tapShort(tapStr(f["tool_use_id"]), 64)
		b.Error = bytes.Equal(f["is_error"], tapTrue)
		b.Bytes = len(f["content"])
		text, cut := tapText(f["content"], tapLimit(budget), tapMaxDepth)
		b.Text, b.Clipped = tapFit(text, budget)
		b.Clipped = b.Clipped || cut
	case "image", "document":
		// NEVER the base64. It is the single largest thing in a real body, it is
		// worthless as transcript, and it is exactly what would blow the frame budget.
		// The size is the only interesting part.
		b.Bytes = len(f["source"])
	default:
		b.Bytes = len(raw)
	}
	return b
}

// tapText flattens an Anthropic content value — a bare string, an array of blocks, or a
// block object — into plain text bounded by limit. Anything it does not recognise
// contributes nothing rather than raising an error: this feeds a fingerprint and a
// display payload, never a routing decision.
//
// depth bounds our OWN recursion. encoding/json refuses beyond 10000 levels on its own,
// which rules out a stack overflow from the scanner, but nothing bounds a hand-rolled
// walk over what the scanner accepted.
func tapText(raw []byte, limit, depth int) (string, bool) {
	if len(raw) == 0 || limit <= 0 || depth <= 0 {
		return "", len(raw) > 0
	}
	switch raw[0] {
	case '"':
		s := tapStr(raw)
		if len(s) > limit {
			return s[:tapRuneCut(s, limit)], true
		}
		return s, false
	case '[':
		dec := json.NewDecoder(bytes.NewReader(raw))
		if _, err := dec.Token(); err != nil {
			return "", true
		}
		var b strings.Builder
		var el json.RawMessage
		cut := false
		for dec.More() {
			if b.Len() >= limit {
				// Stop WALKING, not just appending: a 500-block tool_result must not
				// cost a full parse to produce a string we already filled.
				cut = true
				break
			}
			if err := dec.Decode(&el); err != nil {
				return b.String(), true
			}
			t, c := tapText(el, limit-b.Len(), depth-1)
			cut = cut || c
			if t == "" {
				continue
			}
			if b.Len() > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(t)
		}
		return b.String(), cut
	case '{':
		var f map[string]json.RawMessage
		if json.Unmarshal(raw, &f) != nil {
			return "", true
		}
		for _, k := range [...]string{"text", "thinking", "content", "input"} {
			if v, has := f[k]; has {
				return tapText(v, limit, depth-1)
			}
		}
		return "", false
	}
	return "", false
}

// tapBillingHeader opens the attribution block Claude Code puts first in `system`. When
// the client believes it talks to Anthropic directly (no ANTHROPIC_BASE_URL, as behind
// the HTTPS-proxy entry) the block carries per-request fields: cc_prev_req, cc_turn_index,
// cc_prompt_id. The API leaves the block out of its cache prefix; the key must too.
const tapBillingHeader = "x-anthropic-billing-header:"

// tapSystemKeyText is the system prompt's text for the key: tapText, less any block that
// is the billing header. A bare-string system is used as it is.
func tapSystemKeyText(raw []byte) string {
	if len(raw) == 0 || raw[0] != '[' {
		t, _ := tapText(raw, tapKeyText, tapMaxDepth)
		return t
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if _, err := dec.Token(); err != nil {
		return ""
	}
	var b strings.Builder
	var el json.RawMessage
	for dec.More() && b.Len() < tapKeyText {
		if dec.Decode(&el) != nil {
			break
		}
		t, _ := tapText(el, tapKeyText-b.Len(), tapMaxDepth-1)
		if t == "" || strings.HasPrefix(t, tapBillingHeader) {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(t)
	}
	return b.String()
}

// tapIsUser says whether a message is the person's side of the transcript, role "user".
func tapIsUser(raw json.RawMessage) bool {
	var m struct {
		Role json.RawMessage `json:"role"`
	}
	return json.Unmarshal(raw, &m) == nil && tapStr(m.Role) == "user"
}

// ---- small helpers ----

// tapStr reads a JSON string, returning "" for anything that is not one. Every field
// read in this file goes through it or a sibling, which is what makes a hostile body a
// degraded event instead of a panic.
func tapStr(raw json.RawMessage) string {
	if len(raw) == 0 || raw[0] != '"' {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return ""
	}
	return s
}

// tapShort hard-caps an identifier (a role, a tool name, a tool_use_id). Not budgeted —
// these are bounded and are what makes the event readable at all.
func tapShort(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:tapRuneCut(s, n)]
}

// tapLimit is how much text the next block may produce, given what is left.
func tapLimit(budget *int) int {
	n := tapMaxText
	if *budget < n {
		n = *budget
	}
	if n < 0 {
		n = 0
	}
	return n
}

// tapFit clips s against both the per-block cap and the event-wide budget, and charges
// what it kept. Clipping lands on a rune boundary so we never emit half a UTF-8
// sequence (json.Marshal would substitute U+FFFD and the clip would look like content).
func tapFit(s string, budget *int) (string, bool) {
	n := tapLimit(budget)
	if len(s) <= n {
		*budget -= len(s)
		return s, false
	}
	n = tapRuneCut(s, n)
	*budget -= n
	return s[:n], true
}

// tapFitRaw is tapFit over raw JSON source text, clipped BEFORE the string conversion so
// a 200 KB tool input is never copied whole just to throw it away.
func tapFitRaw(raw []byte, budget *int) (string, bool) {
	n := tapLimit(budget)
	if len(raw) <= n {
		*budget -= len(raw)
		return string(raw), false
	}
	for n > 0 && !utf8.RuneStart(raw[n]) {
		n--
	}
	*budget -= n
	return string(raw[:n]), true
}

func tapRuneCut(s string, n int) int {
	if n >= len(s) {
		return len(s)
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return n
}

// tapFP fingerprints conversation IDENTITY. fp() stays the credential fingerprinter —
// the tap never hashes a credential (the client's bearer arrives already reduced to
// fp()'s 8 hex chars) — but the 8-hex-of-sha256 shape is deliberately the same, so an
// operator reading /_switch/status and a turn event sees one convention.
func tapFP(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		_, _ = h.Write([]byte(p))
		_, _ = h.Write([]byte{0}) // separator: ("ab","c") must not collide with ("a","bc")
	}
	return hex.EncodeToString(h.Sum(nil))[:8]
}
