package main

// #125: request identity exists before routing; the response header is frozen
// at the head and the final event adds the shape producer's terminal outcome.
// No observer participates in scoring and no response bytes are held here.
import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

type decisionContextKey struct{}

type healthEvidence struct {
	Scope   string  `json:"scope"`
	WindowS int     `json:"window_s"`
	Samples int     `json:"samples"`
	Deg     int     `json:"deg"`
	Rate    float64 `json:"rate"`
}

type routeDecision struct {
	DecisionID     string         `json:"decision_id"`
	TS             string         `json:"ts"`
	RequestedModel string         `json:"requested_model"`
	Class          *string        `json:"class"`
	Session        string         `json:"session"`
	HopKind        string         `json:"hop_kind"`
	BoundMember    *string        `json:"bound_member"`
	ServedMember   *string        `json:"served_member"`
	ServedAccount  *string        `json:"served_account"`
	Reason         string         `json:"reason"`
	HealthState    string         `json:"health_state"`
	Episode        *string        `json:"episode"`
	Evidence       healthEvidence `json:"evidence"`
	Walked         string         `json:"walked"`
	Noted          bool           `json:"noted"`
	NoteKind       *string        `json:"note_kind"`
	Attempts       int            `json:"attempts"`
	Outcome        string         `json:"outcome"`
	Cause          *string        `json:"cause"`
	EstInput       int            `json:"est_input"`
	ElapsedMS      int64          `json:"elapsed_ms"`
	Forced         bool           `json:"forced"`
	Ratio          *float64       `json:"ratio"`
	// Entry / Client: the front door (#126) and the machine's proxy login.
	// omitempty on BOTH — base-URL sets neither, so its decision event bytes are
	// unchanged. Never scored, never routed on; attribution for T7 only.
	Entry  string `json:"entry,omitempty"`
	Client string `json:"client,omitempty"`
	// Device / User: the #128 Scry device id and the approving person's opaque
	// Scry user id, set only when the login was a device token. omitempty for
	// the same reason as above: a users-file login leaves both out, so those
	// event bytes are unchanged too. Never an email.
	Device string `json:"device,omitempty"`
	User   string `json:"user,omitempty"`
	// Harness: the registry id (harness.go) of the harness that sent the
	// request, the same value its turn event carries. Always set; a request
	// whose body was never read is unrecognised.
	Harness string `json:"harness"`
	// Transport correlation only: the durable journal stores the eight decision
	// columns on the existing turn, never a copy of the full event or transcript.
	ConversationKey string `json:"conversation_key"`
	Epoch           int    `json:"epoch"`
	Turn            int64  `json:"turn"`
}

type decisionTrace struct {
	mu       sync.Mutex
	tenant   *tenant // whose request: its store notes the decision, its events carry it
	record   routeDecision
	start    time.Time
	out      hopOutcome
	header   string
	cc       *classRoute
	up       *upstreamTrace // the served codex 200's event record, for the log line
	finished bool
}

func nextDecisionID() string { return fmt.Sprintf("r-%s-%d", evEpoch, healthReqCtr.Add(1)) }

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
func decisionFrom(ctx context.Context) *decisionTrace {
	if ctx == nil {
		return nil
	}
	d, _ := ctx.Value(decisionContextKey{}).(*decisionTrace)
	return d
}
func newDecision(r *http.Request) *decisionTrace {
	if healthModeV == healthOff {
		return nil
	}
	id := nextDecisionID()
	sess := r.Header.Get("x-claude-code-session-id")
	if sess == "" {
		sess = "no-session"
	} else {
		sess = fpSess(sess)
	}
	now := time.Now()
	ent := entryFrom(r.Context())
	return &decisionTrace{tenant: tenantFrom(r.Context()), start: now, header: "none", record: routeDecision{
		DecisionID: id, TS: now.UTC().Format(time.RFC3339Nano), Session: sess,
		HopKind: "direct", Reason: "none", HealthState: "n/a", Walked: "none", Outcome: "unknown",
		Evidence: healthEvidence{Scope: "none"}, ConversationKey: "route:" + id, Epoch: 0, Turn: 1,
		Entry: ent.kind, Client: ent.client, Device: ent.device, User: ent.user,
		Harness: harnessUnrecognised,
	}}
}
func (d *decisionTrace) outcome(out hopOutcome) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.out = out
	d.mu.Unlock()
}

// upstream attaches the served hop's codex event record; finish renders it.
func (d *decisionTrace) upstream(u *upstreamTrace) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.up = u
	d.mu.Unlock()
}
func (d *decisionTrace) forced(forced bool) {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.record.Forced = forced
	d.mu.Unlock()
}
func (d *decisionTrace) correlate(id tapRespIdent) {
	if d == nil || !id.ok {
		return
	}
	d.mu.Lock()
	d.record.ConversationKey, d.record.Epoch, d.record.Turn = id.convKey, id.epoch, id.turn
	d.mu.Unlock()
}

// Route headers describe what actually routed, including quota walks in
// observe mode. The notes kill switch and final stop_reason do not affect them.
func decisionRoute(cc *classRoute, state string) (hop, reason, header string) {
	hop, reason, header = cc.hopKind, cc.routeReason, "none"
	if hop == "" || hop == "none" {
		hop = "bound"
	}
	switch reason {
	case "health:detour", "health:ramp", "health:probe", "health:ranked", "health:forced-single":
	default:
		reason = "bind:pointer"
		if cc.pinned {
			reason = "bind:pinned"
		}
	}
	if cc.healthExhaust {
		header = "exhausted"
		if cc.exhaustPinned {
			header = "pinned-exhausted"
			hop = "pinned"
		}
		return
	}
	if cc.walked != "" {
		header = "walked"
		if cc.walked == "quota" {
			hop = "quota_cross"
		}
		if cc.walked == "health" || cc.walked == "both" {
			reason = "health:detour"
		} else if cc.walked == "retry" {
			reason = "stall:first_event"
		} else {
			reason = "quota:429"
			if cc.quotaNote != nil {
				if cc.quotaNote.shapeRejected {
					reason = "shape:429"
				} else if cc.quotaNote.status == 403 {
					reason = "quota:403"
				}
			}
		}
		return
	}
	if cc.leaseFor(cc.served.snapshot().member) != nil {
		return "probe", "health:probe", "probe"
	}
	if reason == "health:ramp" {
		return hop, reason, "ramp"
	}
	switch hop {
	case "detour":
		header = "detoured"
		if reason == "bind:pointer" {
			reason = "health:detour"
		}
	case "ranked", "forced", "probe":
		header = hop
	default:
		if state == hsOpen || state == hsHalfOpen {
			header = "ride"
		}
	}
	return
}
func (d *decisionTrace) head(info reqInfo) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	cc := info.classCtx
	if cc == nil {
		d.record.Attempts = 1
		if info.eff != "" {
			d.record.ServedAccount = nullable(info.eff)
		}
		return
	}
	d.cc = cc
	sv := cc.served.snapshot()
	r := &d.record
	r.Class, r.BoundMember = nullable(cc.class), nullable(cc.boundMember)
	if r.BoundMember == nil {
		r.BoundMember = nullable(cc.member.ID)
	}
	r.ServedMember, r.ServedAccount, r.Attempts = nullable(sv.member), nullable(sv.account), sv.attempts
	if cc.healthExhaust {
		r.ServedMember, r.ServedAccount = nil, nil
	}
	r.Ratio = nil
	if sv.ratio > 0 {
		ratio := sv.ratio
		r.Ratio = &ratio
	}
	r.Walked = cc.walked
	if r.Walked == "" {
		r.Walked = "none"
	}
	r.HealthState = hsHealthy
	if health := cc.tenant.health; health != nil {
		_, pinSource := cc.tenant.pinForBinding(cc.class, cc.session, cc.bindReason)
		health.mu.Lock()
		if health.pins == nil {
			health.pins = map[string]classPinHealth{}
		}
		pin := classPinHealth{source: pinSource}
		if cc.pinned {
			pin.member = cc.boundMember
		}
		health.pins[cc.class] = pin
		if st := health.keys[sv.member]; st != nil {
			r.HealthState, r.Episode = st.state, nullable(st.ep)
			r.Evidence = evidenceLocked(health, st, "", false)
		} else {
			r.Evidence = healthEvidence{Scope: "endpoint_key", WindowS: int(health.cfg.tripWindow.Seconds())}
		}
		health.mu.Unlock()
	}
	r.HopKind, r.Reason, d.header = decisionRoute(cc, r.HealthState)
	// The final producer supplies the same forced classification as the ring.
	r.Forced = r.Forced || r.HopKind == "forced"
}
func (d *decisionTrace) headers(h http.Header) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	h.Set("X-Switch-Decision", d.record.DecisionID)
	if d.record.Class != nil {
		h.Set("X-Switch-Health", d.header)
	}
}
func (d *decisionTrace) finish(ctx context.Context, status int) string {
	if d == nil {
		return ""
	}
	d.mu.Lock()
	if d.finished {
		d.mu.Unlock()
		return ""
	}
	d.finished = true
	r, up := d.record, d.up
	r.ElapsedMS = time.Since(d.start).Milliseconds()
	if d.cc != nil && d.cc.note != nil && d.cc.note.noted.Load() {
		r.Noted = true
		r.NoteKind = nullable(d.cc.note.kind)
	}
	r.Cause = nullable(d.out.cause)
	switch {
	// Rule 0, with its one exception (§3): a cancel wins only while the reply
	// is unfinished. A shape site that already settled the hop (post_head,
	// final) read the reply before the cancel and applied Rule 0 itself, so
	// its row stands — a client that reads the whole reply and then closes
	// is not a cancel.
	case ctx.Err() == context.Canceled && !d.out.settled():
		r.Outcome = "cancelled"
	case d.cc != nil && d.cc.healthExhaust:
		r.Outcome = "exhausted"
	case d.out.final:
		r.Outcome = d.out.sig
		if r.Outcome == sigHealthy {
			r.Outcome = "ok"
		}
		if r.Outcome == sigDegraded {
			r.Outcome = "deg"
		}
	case status == 401 || status == 403:
		r.Outcome = "auth"
	case status == 429:
		r.Outcome = "quota"
	case status >= 400 && status < 500:
		r.Outcome = "client_error"
	default:
		r.Outcome = "unknown" // no final producer; never turn a 200 head into an ok
	}
	d.record = r
	d.mu.Unlock()
	if d.tenant != nil {
		if health := d.tenant.health; health != nil {
			health.noteRouteDecision(valueOrEmpty(r.Class), r.HopKind, r.Reason)
		}
		if eventsArmed() {
			publish(d.tenant.id, "route_decision", r)
		}
	}
	// out= is how the hop ended. On a stream that is known only here, after the
	// 200 went out, so this is where a refused or cut-off stream shows (#132).
	out := r.Outcome
	if c := valueOrEmpty(r.Cause); c != "" {
		out += ":" + c
	}
	return fmt.Sprintf(" d=%s hop=%s hs=%s walked=%s noted=%t ep=%s out=%s", r.DecisionID, r.HopKind, r.HealthState, r.Walked, r.Noted, valueOrEmpty(r.Episode), out) +
		up.logTag(time.Now())
}
func valueOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func noteLastServed(cc *classRoute) {
	if cc == nil || cc.tenant == nil || cc.tenant.health == nil || healthModeV == healthOff {
		return
	}
	tn := cc.tenant
	sv := cc.served.snapshot()
	tn.bindMu.Lock()
	key := cc.class + "\x00" + cc.session
	if b, ok := tn.bindCache[key]; ok {
		_, reason, _ := decisionRoute(cc, "")
		b.servedMember, b.servedReason = sv.member, reason
		tn.bindCache[key] = b
	}
	tn.bindMu.Unlock()
}

func (tn *tenant) pinForBinding(class, session, fallback string) (bool, string) {
	tn.bindMu.Lock()
	b, ok := tn.bindCache[class+"\x00"+session]
	tn.bindMu.Unlock()
	if ok && b.pinned != nil {
		return *b.pinned, "bind_payload"
	}
	return strings.HasPrefix(fallback, "bind: pinned"), "reason_prefix"
}
