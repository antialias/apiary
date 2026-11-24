package main

// ---- class routing (#27) ----
//
// A CLASS row (registry type "class", e.g. claude-frontier) is a synthetic picker
// id whose serving member is chosen by the brain per SESSION (#26): the proxy
// calls GET /class-bind?class=&session= (session = the client's
// x-claude-code-session-id header, present on every real Claude Code request)
// and actuates whatever comes back — an anthropic member (lane mechanics: decide
// the lane's account, inject its bearer, rewrite the model to the member's
// declared Anthropic id) or a provider member (direct mechanics: provider key +
// upstream + model rewrite). Bindings are sticky brain-side; the proxy caches
// them briefly (bindFresh) and rides the cached member at ANY age when the brain
// is unreachable (STALE — mirrors the lane/token contract). A brain REFUSAL
// (404 unknown class / 503 nothing usable / 401 bad key) is a decision, not an
// outage: typed 503, no cache fallback.
//
// The brain has NO 429 feedback channel, so reactive failover is entirely local
// (user decision on #27: full reactive, cross-provider included — window-safe
// because a class advertises min(member windows), so every member can serve any
// request the client believes fits). Two mechanisms:
//
//   - mid-request (classRoundTrip): a bounded ladder — bound member; for an
//     anthropic member one same-lane account swap (today's mechanics), for a
//     pooled provider member one pool-account swap (#71); then a walk across
//     the remaining candidate members in classCandidates order (headroom →
//     reserve → brain-exhausted), each re-actuated for its kind (model +
//     upstream + auth all change). Bounded by the class's member count. The
//     order matters: `usable` is auth+config health, NOT capacity — the brain
//     serves `effective` for that, and a rung at/over the hard line goes last.
//   - request-time (routeClass): a member cooling from a 429 (memExhausted) is
//     DETOURED around for the cooldown, mirroring decide()'s "avoiding
//     recently-429'd" post-filter. A detour never re-ranks a healthy binding —
//     the brain owns proactive placement (#26's drift-up rationing), and the
//     bind cache is never mutated locally.
//
// Class switch alerts are per-(class, session), fired on a bind change or a
// mid-request cross — never via noteEffective under a shared class key (two
// sessions bound to different members would alert-storm on every interleave).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// classMemberPref is one class member as served by the brain (/preferred
// classes[*].members[*] and /class-bind member — same shape, priority-sorted).
type classMemberPref struct {
	ID            string `json:"id"`
	Priority      int    `json:"priority"`
	Kind          string `json:"kind"`     // "anthropic" | "provider"
	Lane          string `json:"lane"`     // anthropic members: the lane whose account serves this
	Provider      string `json:"provider"` // provider members: static provider name
	Model         string `json:"model"`    // the concrete model id the class picker id is rewritten to
	ContextWindow int    `json:"context_window"`
	// AnthropicTokenRatio is r_p (#44): tokens this member's provider reports for
	// a body over tokens Anthropic reports for the same body. The brain serves it
	// on every member, validated in (0, 4] on providers and 1.0 by definition on
	// anthropic members; it is the one source a provider hop's usage is scaled by
	// (memberRatio).
	AnthropicTokenRatio float64 `json:"anthropic_token_ratio"`
	// Effective is the included tiers' scalar (§2.3): display and q 0/1/2 only.
	// Whether the member may be dialled at all is its route's dial check.
	Effective *float64 `json:"effective"` // nil = unknown
	// Route is the member's route id ("<class>/<member id>"): its policy (pay,
	// stop_at, scope) and backing accounts are /preferred routes[Route].
	Route string `json:"route"`
	// ReserveCeiling is the brain's per-member reserve line (nil = none): a
	// member over it is passed over while any other member can serve. Mirrored
	// by classCandidates so a mid-request cross ranks the way the pointer does.
	ReserveCeiling *float64 `json:"reserve_ceiling"`

	// #71 pool view. A member names a CAPABILITY; the accounts backing it are a
	// pool the brain ranks by utilization. Incumbent is the account the brain
	// has this member on: the provider pool's pick, or for an anthropic member
	// its lane's pick while the member's own checks agree, else the coolest
	// account they admit (a lane's pay and stop_at never steer a member). Targets
	// maps pool account name -> effective utilization (null = polled but
	// unknown); for anthropic members the lane's effective ranks instead.
	Incumbent string              `json:"incumbent"`
	Targets   map[string]*float64 `json:"targets"`

	// priorityOrder is its class's `order: "priority"` (classPref.Order),
	// stamped on every member as /preferred is read, so the ranker, which
	// sees members only, knows the class walks in priority order.
	priorityOrder bool

	// req is the request this member would be dialled for (requestKind's
	// request: a Runway image's max_cost and output count), set by routeClass
	// once the request is known; zero elsewhere, the brain's own check.
	req checkRequest
}

// withRequest is the class's members with the request each would be dialled
// for set on it (§1.6: a check is per request), a copy: the snapshot's
// members are shared.
func withRequest(members []classMemberPref, q classReq) []classMemberPref {
	out := make([]classMemberPref, len(members))
	for i, m := range members {
		m.req = q.kind().request(m, q)
		out[i] = m
	}
	return out
}

type classPref struct {
	Guidance      string            `json:"guidance"`
	Display       string            `json:"display"`
	ContextWindow int               `json:"context_window"`
	Pointer       *string           `json:"pointer"`
	Consumer      string            `json:"consumer"` // owning consumer id; "" = unowned (#95)
	Members       []classMemberPref `json:"members"`  // brain serves these in priority order
	// RequestKind is what the class serves (§2.9, C4): "chat", or "image" for
	// the image door's class. A brain before C4 serves none: every class chat.
	RequestKind string `json:"request_kind"`
	// Order is how the class's members are walked: "" (funding, the default:
	// every included member before any paid one, §2.3) or "priority" (in
	// priority order, whatever pays; priorityOrder). A brain that serves none
	// is funding order.
	Order string `json:"order"`
}

// classOrderPriority is classPref.Order's priority order.
const classOrderPriority = "priority"

// stampOrder marks each member of a class in priority order (priorityOrder).
func stampOrder(classes map[string]classPref) {
	for name, c := range classes {
		if c.Order != classOrderPriority {
			continue
		}
		ms := slices.Clone(c.Members)
		for i := range ms {
			ms[i].priorityOrder = true
		}
		c.Members = ms
		classes[name] = c
	}
}

// classRoute travels on reqInfo for class-routed requests: everything the
// failover transport needs to re-actuate the SAME request on a different member.
// Its classReq (kinds.go) is the request as it arrived, which every rebuild
// starts from. origHeader is the
// INBOUND header snapshot (post client-gate): a translated wire builds its
// outbound headers fresh, so a cross AWAY from such a member must rebase on the
// original client headers or the next hop loses anthropic-version and inherits
// the previous wire's UA.
type classRoute struct {
	classReq           // the request itself, of its kind (kinds.go, C4); every rebuild starts from it
	tenant     *tenant // whose request this is: its binds, health and accounts
	class      string
	session    string
	member     classMemberPref   // the member attempt 1 was actuated on
	members    []classMemberPref // pref.classes snapshot at request time (may be empty)
	origHeader http.Header
	served     *servedHop // mutable final-hop record; sealed before ReverseProxy writes the response
	scale      *scaleMark // #76: the request's ONE scaling record; rebuilds retarget it, never replace it
	reqID      string     // #122: ring rule 0's per-request dedup key; "" when no health mode is armed

	// #123 routing consumption (§5.2/§5.3). hopKind/routeReason are §6.1's
	// decision tokens for attempt 1; walked is which walk(s) actually crossed
	// ("" none, "quota", "health", "both", "retry" = first_event.go's resend); lease is the probe lease attempt 1
	// holds (leaseFor guards every settle site to the leased member).
	// healthExhaust marks the request as answered by §5.4's synthetic 529 —
	// RoundTrip must hand that response out untouched (no translate, no scale).
	pinned        bool
	pinFact       bool   // the brain pinned the session (68 §4.4), health armed or not; session facts only, never routing
	hopKind       string // bound | pinned | detour | ranked | forced | probe | none
	routeReason   string // "" | health:probe | health:ramp | health:detour | health:ranked | health:forced-single | health:fallback | health:exhausted
	walked        string
	trace         *decisionTrace
	boundMember   string // brain's original binding, before any health/local detour
	quotaNote     *noteWalkCause
	healthNote    *noteWalkCause
	note          *responseNote // immutable decision snapshot; built before any body reader starts
	lease         *probeLease
	healthExhaust bool
	exhaustPinned bool

	// unavail is healthExhaust's twin on the QUOTA axis (§2.8): the class is
	// unavailable — the brain's 409 at bind, the pre-flight, or a walk that
	// ran out of rungs — and this is the body its reply renders, by the
	// client's wire (unavailable.go). Set, the request dials nobody further
	// and the reply flows out of RoundTrip untouched.
	unavail *unavailable

	// bindReason is the brain's own words for WHY this session is bound where it
	// is ("forced down: quota", a pin, drift-up) — the sentence fireClassAlert
	// already logs, carried onto the transcript so a flip explains itself where
	// it is read (#86). Set ONLY when attempt 1 landed on the BOUND member: a
	// detour is explained by the detour, and attaching the binding's reason to a
	// member the brain did not choose would misattribute it. Empty is the normal
	// case (a routine re-bind carries no reason) and stays absent from the wire.
	bindReason string
}

// servedHop is shared between the transport retry ladder and the response tap.
// ReverseProxy completes RoundTrip before writing response bytes, so every response
// event sees the selected final hop. The request-side turn deliberately keeps the
// initial/desired route; this record answers the separate "what actually served?".
type servedHop struct {
	mu         sync.RWMutex
	class      string
	member     string
	account    string
	model      string
	lane       string
	translated bool
	ratio      float64 // #76: the served member's declared r_p (0 = anthropic/unscaled)
	attempts   int

	// bindReason travels with the hop rather than beside it so that a cross can
	// invalidate it in the one place the member changes (selectHop): the reason
	// describes why the BOUND member is bound, and the moment something else is
	// serving it no longer describes what this record reports.
	bindReason string
}

type servedHopView struct {
	class      string
	member     string
	account    string
	model      string
	lane       string
	translated bool
	ratio      float64
	attempts   int
	bindReason string
}

func servedAccount(m classMemberPref, eff string) string {
	if account, ok := strings.CutPrefix(eff, m.ID+"@"); ok {
		return account
	}
	if m.Kind == "provider" {
		return m.Provider
	}
	return eff
}

func newServedHop(class string, m classMemberPref, info reqInfo, eff, bindReason string) *servedHop {
	_, translated := info.wire.(codexWire)
	return &servedHop{class: class, member: m.ID, account: servedAccount(m, eff), model: m.Model,
		lane: info.lane, translated: translated, ratio: info.ratio, attempts: 1, bindReason: bindReason}
}

func (s *servedHop) selectHop(m classMemberPref, account, lane string, translated bool, ratio float64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if m.ID != s.member {
		s.bindReason = "" // serving someone else; the binding's reason is no longer this hop's
	}
	s.member, s.account, s.model, s.lane, s.translated, s.ratio = m.ID, account, m.Model, lane, translated, ratio
	s.attempts++
	s.mu.Unlock()
}

func (s *servedHop) snapshot() servedHopView {
	if s == nil {
		return servedHopView{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return servedHopView{class: s.class, member: s.member, account: s.account,
		model: s.model, lane: s.lane, translated: s.translated, ratio: s.ratio,
		attempts: s.attempts, bindReason: s.bindReason}
}

// memberScaleKey is a class member's metering key — the brain's "<provider>/<model>"
// member id. Anthropic members meter on the lane machinery, not here; their
// scaling key is empty.
func memberScaleKey(m classMemberPref) string {
	if m.Kind == "provider" {
		return m.Provider + "/" + m.Model
	}
	return ""
}

// memberRatio is the brain-served r_p for the member a hop lands on (0 for an
// anthropic member — native units by definition).
func memberRatio(m classMemberPref) float64 {
	if m.Kind == "provider" {
		return m.AnthropicTokenRatio
	}
	return 0
}

// ---- bind cache ----
type classBind struct {
	member                     classMemberPref
	reason                     string
	pinned                     *bool
	servedMember, servedReason string
	fetchedAt                  time.Time
	// memberSince is when we last OBSERVED the brain re-point this session to a
	// different member — NOT when the cache entry was refreshed (fetchedAt is
	// renewed by every routine refresh, so it cannot carry PR5's "binding
	// minted after the episode opened" test). Zero on first observation: a
	// first-ever bind cannot distinguish a fresh mint from a fetch, and the
	// conservative answer keeps the episode's detour in charge.
	memberSince time.Time
}

// Per tenant: tenant.bindMu, tenant.bindCache (class + "\x00" + session ->
// last-good bind) and tenant.sessionFundings (the same key -> how that
// session's last class request ended, §2.11's statusline; its own map, not a
// binding's field: the brain's 409 refuses a session it never bound).
var (
	// fundingIdle is the brain's SESSION_IDLE: a session idle longer has no
	// binding there, so its funding is dropped here too.
	fundingIdle = 6 * time.Hour
	bindFresh   = 15 * time.Second // positive-cache TTL; var so tests can shrink it
)

func bindKey(class, session string) string { return class + "\x00" + session }

// fpSess is the session id as logged/exposed: the first 8 chars (session ids are
// client-minted UUIDs; the brain stores them verbatim, we only display less).
func fpSess(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

// bindMemberSince is the session's last observed bind re-point (PR5's
// bindingAt); zero when never observed to move.
func (tn *tenant) bindMemberSince(class, session string) time.Time {
	tn.bindMu.Lock()
	defer tn.bindMu.Unlock()
	return tn.bindCache[bindKey(class, session)].memberSince
}

// bindClassMember resolves (class, session) to the brain's bound member.
// refusal != "" means the brain actively refused (unknown class, bad key, no
// member with an account) — the caller answers a typed 503 and must NOT fall
// back to the cache: a refusal is a decision, not an outage. unavail != nil is the brain's 409
// class_unavailable (§2.8): no route of the class is included or paid, and
// its body is the reply. A transport failure rides the cached member at any
// age (STALE); no cache + no brain = FAILED.
func (tn *tenant) bindClassMember(class, session string) (classMemberPref, string, routeState, string, *unavailable) {
	key := bindKey(class, session)
	tn.bindMu.Lock()
	cached, has := tn.bindCache[key]
	tn.bindMu.Unlock()
	if has && time.Since(cached.fetchedAt) < bindFresh {
		tn.pref.mu.RLock()
		ok := tn.pref.ok
		tn.pref.mu.RUnlock()
		state := stateRouted
		if !ok {
			state = stateStale
		}
		return cached.member, cached.reason, state, "", nil
	}
	stale := func() (classMemberPref, string, routeState, string, *unavailable) {
		if has {
			return cached.member, cached.reason, stateStale, "", nil
		}
		return classMemberPref{}, "", stateFailed, "", nil
	}
	if tn.brainURL() == "" {
		return stale()
	}
	req, _ := http.NewRequest("GET", tn.brainURL()+"/class-bind?class="+url.QueryEscape(class)+
		"&session="+url.QueryEscape(session), nil)
	req.Header.Set("X-Switch-Key", switchKey)
	resp, err := brainClient.Do(req)
	if err != nil {
		return stale()
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		switch resp.StatusCode {
		case 400, 401, 404, 503:
			// 503: the brain's configuration refusal (§2.8 item 6), no member
			// of the class has an account, so nothing can be bound or waited for.
			return classMemberPref{}, "", stateFailed, brainRefusalReason(resp.StatusCode, b), nil
		case http.StatusConflict:
			// members and retryable are required: a body without retryable
			// would read as the money reply, so it is malformed, the typed 503.
			var u unavailable
			if err := json.Unmarshal(b, &u); err != nil || u.Members == nil || u.Retryable == nil {
				return classMemberPref{}, "", stateFailed, brainRefusalReason(resp.StatusCode, b), nil
			}
			u.tenant = tn // the refusal renders with this tenant's router-dry clause
			return classMemberPref{}, "", stateRouted, "", &u
		}
		return stale() // 5xx from a proxy in between etc. — treat as outage
	}
	var d struct {
		Member  classMemberPref `json:"member"`
		Reason  string          `json:"reason"`
		Pinned  *bool           `json:"pinned"`
		Created bool            `json:"created"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil || d.Member.ID == "" {
		return stale()
	}
	tn.bindMu.Lock()
	prev, hadPrev := tn.bindCache[key]
	nb := classBind{member: d.Member, reason: d.Reason, fetchedAt: time.Now(), memberSince: prev.memberSince, pinned: d.Pinned, servedMember: prev.servedMember, servedReason: prev.servedReason}
	if hadPrev && prev.member.ID != d.Member.ID {
		nb.memberSince = time.Now() // an observed re-point — PR5's binding instant
	}
	tn.bindCache[key] = nb
	tn.bindMu.Unlock()
	if tn.health != nil && (d.Created || !hadPrev || prev.member.ID != d.Member.ID) {
		marked := strings.HasPrefix(d.Reason, "bind: pinned")
		if d.Pinned != nil {
			marked = *d.Pinned
		}
		tn.health.noteBinding(class, d.Member.ID, marked)
	}
	if hadPrev && prev.member.ID != d.Member.ID {
		// The brain re-pointed this session (forced-down / drift-up / pin) and
		// we just observed it — the one place a background move becomes visible.
		st.mu.Lock()
		st.switches++
		st.mu.Unlock()
		tn.fireClassAlert(class, session, prev.member.ID, d.Member.ID, d.Reason)
	}
	return d.Member, d.Reason, stateRouted, "", nil
}

func brainRefusalReason(code int, body []byte) string {
	var e struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(body, &e)
	switch {
	case code == 401:
		return "brain rejected the switch key (401)"
	case e.Error != "":
		return "brain refused: " + e.Error
	}
	return fmt.Sprintf("brain refused the class bind (HTTP %d)", code)
}

// ---- refusal clamps ----

// quotaCooldown is the FLOOR of a spent record (records.go), and it is a
// different physical quantity from forcedCooldown. forcedCooldown (300s) is
// calibrated for a capacity blip. A quota bucket refills on the provider's
// schedule — 1-5 hours for a rolling window, days for a weekly one — so 300s
// against a 5-hour window re-dials a limit known to be closed ~60 times, and
// against gpt's weekly bucket ~720 times.
//
// A var so tests can shrink it; quotaCooldownCap bounds how far we will trust
// an upstream's own clock (a bad resets_at must not park a limit for a week).
var (
	quotaCooldown    = 30 * time.Minute
	quotaCooldownCap = 6 * time.Hour
)

// memberIncumbent is the account the brain put a member on, chosen from the
// member's own route's checks (§2.2 row 16).
func (tn *tenant) memberIncumbent(m classMemberPref) string { return m.Incumbent }

// memberPicks judges a member's route accounts (§2.2 rows 5-9): the ones its
// check admits, in swap order, and the ones it refuses.
func (tn *tenant) memberPicks(m classMemberPref, now time.Time) (ok, refused []acctPick) {
	pct := func(a string) *float64 { return m.Targets[a] }
	if m.Kind == "anthropic" {
		pct = tn.lanePct(m.Lane)
	}
	return tn.rankRouteAccounts(m.Route, m.Model, m.req, tn.memberIncumbent(m), tn.routeAccounts(m.Route), pct, now)
}

// memberVerdict is a member's best account: its check, whether that account is
// throttled, and — for a member none of whose accounts may be dialled —
// whether an unattributed record alone holds it out (PR11's re-admission).
func (tn *tenant) memberVerdict(m classMemberPref, now time.Time) (best acctPick, ok, held bool) {
	admitted, refused := tn.memberPicks(m, now)
	if len(admitted) > 0 {
		return admitted[0], true, false
	}
	for _, p := range refused {
		if p.held {
			held = true
		}
	}
	if len(refused) > 0 {
		return refused[0], false, held
	}
	return acctPick{check: dialCheck{Check: checkNo, Reason: "unknown", Basis: "unknown"}}, false, false
}

// providerMemberDial is the account a provider member dials and its check
// (§2.2 row 9, the same rule as the lane swap): want when given and its check
// admits it, else the first account rankRouteAccounts admits — the incumbent
// while its check admits it. A pool member (one with a pool view, Targets)
// dials only accounts in that view and names the one it picked; a member
// with no pool view returns "" — the wire follows the brain's incumbent —
// with that account's check, unless its wire signs with the picked account's
// own key (bearerWire, D6), which needs the account named. readmit lets PR11 (§2.2 row 14) dial
// an account an unattributed record alone holds out. entitlement is
// count_tokens (§2.2 row 20): free, so every account of the route may answer
// it, the admitted first. ok=false: no account may be dialled.
func (tn *tenant) providerMemberDial(m classMemberPref, want string, readmit, entitlement bool, now time.Time) (string, dialCheck, bool) {
	admitted, refused := tn.memberPicks(m, now)
	if entitlement {
		admitted, refused = append(admitted, refused...), nil
	}
	pool := len(m.Targets) > 0
	inPool := func(a string) bool {
		_, has := m.Targets[a]
		return !pool || has
	}
	named := pool || wireNamesAccount(m.Provider)
	pick := func(p acctPick) (string, dialCheck, bool) {
		if named {
			return p.account, p.check, true
		}
		return "", p.check, true
	}
	if want != "" {
		for _, p := range admitted {
			if p.account == want && inPool(want) {
				return pick(p)
			}
		}
		return "", dialCheck{}, false
	}
	for _, p := range admitted {
		if inPool(p.account) {
			return pick(p)
		}
	}
	if readmit {
		for _, p := range refused {
			if p.held && inPool(p.account) {
				return pick(p)
			}
		}
	}
	return "", dialCheck{}, false
}

// anthropicMemberDial is decideRoute for a class's anthropic member, on the
// member's own route (§2.2 rows 6-8: its scope, its pay, its stop_at), plus
// PR11's re-admission: with readmit, a member every account of which is held
// out only by an unattributed record dials the first such account.
func (tn *tenant) anthropicMemberDial(m classMemberPref, countTokens, readmit bool) laneDecision {
	d := tn.decideOn(m.Route, m.Lane, m.Model, m.Incumbent, "member incumbent", countTokens)
	if !d.none || !readmit {
		return d
	}
	for _, p := range d.refused {
		if !p.held {
			continue
		}
		if tok := tn.credentialFor(p.account); tok != "" {
			return laneDecision{tok: tok, acct: p.account, reason: "PR11 re-admission", state: d.state, check: p.check}
		}
	}
	return d
}

// providerByName returns the provider with that name (nil for none).
func providerByName(name string) *provider {
	for _, p := range providers {
		if p.name == name {
			return p
		}
	}
	return nil
}

// hardPct is the brain's SWITCH_HARD_PCT as last served on /preferred, or the
// mirrored default before any fetch / on an old brain that omits thresholds.
func (tn *tenant) hardPct() float64 {
	tn.pref.mu.RLock()
	h := tn.pref.hardPct
	tn.pref.mu.RUnlock()
	if h <= 0 {
		return defaultHardPct
	}
	return h
}

// memberExhaustedByNumber: the brain's own quota reading says this member has
// nothing left (known effective at/over the hard line). `usable` does NOT say
// this — it is "auth+config healthy" (status==ok), and a member at 100% is
// perfectly usable by that definition. That gap is exactly how a
// class cross once went kimi(429, transient overload) -> gpt(100%, dead) while
// lane:opus sat at 34%: the ONE cross was spent on a rung the pointer had already
// passed over. An unknown effective is not a verdict — nil passes.
func memberExhaustedByNumber(m classMemberPref, hard float64) bool {
	return m.Effective != nil && *m.Effective >= hard
}

// memberOverCeiling mirrors the brain's _under_ceiling: nil ceiling or nil
// effective is never a violation.
func memberOverCeiling(m classMemberPref) bool {
	return m.ReserveCeiling != nil && m.Effective != nil && *m.Effective >= *m.ReserveCeiling
}

// rankCandidates returns the class's members in the order a mid-request cross
// (and the request-time detour) should try them, dropping the excluded (bound)
// member, every member whose dial check is `no` on all of its accounts (§2.2
// row 5: that replaces the brain's !usable verdict and the quota cooldown),
// and provider members with no locally-configured provider.
//
// The funding tiers (quota-model 50 §2.3), each in brain-served priority
// order:
//
//   - included: q 0 inside its reserve ceiling, q 1 over it (a reserve is
//     passed over while anything else can serve), q 2 at or over the hard line
//     (memberExhaustedByNumber; 95-99 % still holds real allowance, and a
//     reading can be a poll stale). The same two-pass rule the brain's pointer
//     applies, plus the last tier it keeps for after every rung with headroom;
//   - paid: after every included tier, in pay_order (payOrderLess).
//
// Health deranks within a funding tier, never across: an OPEN/HALF_OPEN key, or
// a throttle on the account the member would dial (a throttle is health, not
// funding), adds tierOpenBase to an included rung and one to a paid one, so an
// OPEN included member still ranks ahead of a healthy paid one — money is
// never spent to dodge a health derank.
//
// It powers the LOCAL ladder only — it never re-ranks a healthy binding; the
// brain owns proactive placement (#26). It is also the ONE ranker of the
// degraded-failover spec (§5.1, docs/degraded-failover/60-spec-v2.md).
// rankOpt extends the ranker for §5.1's health-aware call sites (#123). The
// zero value is Stage 0 exactly: no health column, no wire/window filters.
type rankOpt struct {
	est    int  // estimated input tokens; PR9's window fit (only bites over LARGE_PROMPT_TOK)
	wire   bool // request arrived on the OpenAI wire; PR7/D2 exclusion
	health bool // consult the FSM: composite tiers + PR11 re-admission
	// ctx is the request's the ranking is for (nil: none, as the prober's),
	// so a key it asks for (memberKeyMissing) ends with it. The ranker reads
	// it through the tenant it ranks for (tenant.ctxOf), so the key asked for
	// is that tenant's whatever the caller stamped.
	ctx context.Context
}

// Composite tiers (§2.3): 0-2 included and healthy; 3-5 included and
// OPEN/HALF_OPEN or throttled; 6 paid and healthy; 7 paid and OPEN/HALF_OPEN or
// throttled; 8 PR11's re-admission (§2.2 row 14): a member held out only by an
// unattributed record (§2.6) whose check would otherwise be included, appended
// only when every other candidate is OPEN. It never re-admits into money, and
// it stays after paid: ahead of it, it would re-ask that provider on every
// request — the loop the record exists to stop.
const (
	tierOpenBase = 3
	tierPaid     = 6
	tierReadmit  = 8
)

// rankCalls counts ranker invocations — E19's fast-path assertion (§5.2: a
// healthy bound member is served with ZERO ranking) reads its delta.
var rankCalls atomic.Int64

func (tn *tenant) rankCandidates(members []classMemberPref, exclude string, opt rankOpt) []ranked {
	out, _ := tn.rankCandidatesSkips(members, exclude, opt)
	return out
}

// memberSkip is a member rankCandidates left out, and why. The ranker used to
// drop these silently; the walk now logs them once, and the terminal reply
// names the same states (availability.go) so a skipped member is never just
// absent from the story.
type memberSkip struct{ id, reason string }

func renderSkips(sk []memberSkip) string {
	parts := make([]string, 0, len(sk))
	for _, s := range sk {
		parts = append(parts, s.id+" ("+s.reason+")")
	}
	return strings.Join(parts, ", ")
}

// rankCandidatesSkips is rankCandidates plus the members it skipped.
func (tn *tenant) rankCandidatesSkips(members []classMemberPref, exclude string, opt rankOpt) ([]ranked, []memberSkip) {
	rankCalls.Add(1)
	ctx := tn.ctxOf(opt.ctx)
	var skipped []memberSkip
	skip := func(m classMemberPref, why string) { skipped = append(skipped, memberSkip{m.ID, why}) }
	hard := tn.hardPct()
	now := nowFn()
	tn.pref.mu.RLock()
	payOrder := tn.pref.payOrder
	tn.pref.mu.RUnlock()
	var states map[string]string
	if opt.health && tn.health != nil {
		ids := make([]string, 0, len(members))
		for _, m := range members {
			ids = append(ids, m.ID)
		}
		states = tn.health.statesOf(ids)
	}
	var buckets [tierReadmit][]ranked
	var held []ranked
	for _, m := range members {
		if m.ID == exclude {
			continue
		}
		if m.Kind == "provider" && providerByName(m.Provider) == nil {
			skip(m, "provider "+m.Provider+" not configured here")
			continue
		}
		if why := tn.memberKeyMissing(ctx, m); why != "" {
			skip(m, why)
			continue
		}
		if opt.health {
			// PR7/D2: wire traffic never lands on an anthropic member. PR8: a
			// member with no declared model can't actuate. PR9: a large request
			// must fit the member's declared window.
			if opt.wire && m.Kind == "anthropic" {
				skip(m, "anthropic member on the openai wire")
				continue
			}
			if m.Model == "" {
				skip(m, "no declared model")
				continue
			}
			if opt.est > healthLargePromptTok && m.ContextWindow > 0 && opt.est > m.ContextWindow {
				skip(m, "request larger than its context window")
				continue
			}
		}
		best, ok, isHeld := tn.memberVerdict(m, now)
		if !ok {
			if opt.health && isHeld {
				held = append(held, ranked{m: m, tier: tierReadmit, health: states[m.ID], check: best.check})
			}
			skip(m, "check "+best.check.String())
			continue
		}
		rc := ranked{m: m, health: states[m.ID], check: best.check, throttled: best.throttled}
		deranked := best.throttled || (opt.health && (rc.health == hsOpen || rc.health == hsHalfOpen))
		switch {
		case best.check.paid():
			rc.tier = tierPaid
			if deranked {
				rc.tier++
			}
		default:
			switch {
			case memberExhaustedByNumber(m, hard):
				rc.tier = 2
			case memberOverCeiling(m):
				rc.tier = 1
			}
			if deranked {
				// PR10: an OPEN/HALF_OPEN key is deranked, never held out (§0.0).
				rc.tier += tierOpenBase
			}
		}
		buckets[rc.tier] = append(buckets[rc.tier], rc)
	}
	for _, t := range []int{tierPaid, tierPaid + 1} {
		b := buckets[t]
		sort.SliceStable(b, func(i, j int) bool {
			return payOrderLess(b[i].check.Pot, b[i].m.Priority, b[j].check.Pot, b[j].m.Priority, payOrder)
		})
	}
	var out []ranked
	for _, b := range buckets {
		out = append(out, b...)
	}
	if inPriorityOrder(members) {
		out = priorityOrder(out)
	}
	if opt.health {
		allOpen := true
		for _, rc := range out {
			if rc.health != hsOpen {
				allOpen = false
				break
			}
		}
		if allOpen && len(held) > 0 {
			// PR11: every other candidate's key is OPEN (vacuously: none
			// exist) — re-admit the held at the very back rather than refuse
			// while a member that would answer sits idle.
			out = append(out, held...)
		}
		if len(out) == 1 {
			out[0].forced = true
		}
	}
	return out, skipped
}

// inPriorityOrder reports a class whose document says `order: "priority"`:
// its members are walked in priority order whatever pays for them.
func inPriorityOrder(members []classMemberPref) bool {
	return len(members) > 0 && members[0].priorityOrder
}

// priorityOrder is a ranking (funding tiers, above) re-cut for a class in
// priority order: members are taken in priority order, and only a run of
// neighbours of one funding (included or paid) is ranked among itself by
// its tiers, so a throttled or OPEN member still yields to a healthy
// neighbour of its own funding but never to a member past a member of the
// other funding.
func priorityOrder(in []ranked) []ranked {
	out := slices.Clone(in)
	sort.SliceStable(out, func(i, j int) bool { return out[i].m.Priority < out[j].m.Priority })
	for lo := 0; lo < len(out); {
		hi := lo + 1
		for hi < len(out) && out[hi].check.paid() == out[lo].check.paid() {
			hi++
		}
		run := out[lo:hi]
		sort.SliceStable(run, func(i, j int) bool { return run[i].tier < run[j].tier })
		lo = hi
	}
	return out
}

// ranked is one candidate plus the evidence that ordered it (spec §5.1).
type ranked struct {
	m         classMemberPref
	tier      int       // §2.3's composite tier above
	health    string    // endpoint health state
	forced    bool      // the post-filter list had exactly one entry (#123 computes it)
	check     dialCheck // the member's best account's check
	throttled bool      // that account is throttled
}

// classCandidates is rankCandidates for the call sites that only want the
// members (spec §11: the wrapper form). ctx is the request's the walk is
// for (nil: a caller with none), carried so a key the ranking asks for is
// asked on the request's behalf and for its tenant.
func (tn *tenant) classCandidates(ctx context.Context, members []classMemberPref, exclude string) []classMemberPref {
	r := tn.rankCandidates(members, exclude, rankOpt{ctx: ctx})
	out := make([]classMemberPref, 0, len(r))
	for _, c := range r {
		out = append(out, c.m)
	}
	return out
}

// memberThrottled reports a dialable member whose best account is throttled.
func (tn *tenant) memberThrottled(m classMemberPref, now time.Time) bool {
	best, ok, _ := tn.memberVerdict(m, now)
	return ok && best.throttled
}

// memberDialable reports why a member can't be dialled right now ("" =
// fine): its provider isn't configured here, or no account of its route has a
// check that is not `no` (§2.2 row 6: the bound member is dialled only while
// its check admits it). Anthropic token availability is checked at actuation.
func (tn *tenant) memberDialable(m classMemberPref, now time.Time) string {
	if m.Kind == "provider" && providerByName(m.Provider) == nil {
		return "has no configured provider (" + m.Provider + ")"
	}
	if best, ok, _ := tn.memberVerdict(m, now); !ok {
		return "is " + best.check.String()
	}
	return ""
}

// fireClassAlert announces one SESSION's class-member move (bind change or
// mid-request 429 cross). Deliberately not fireSwitchAlert: its weekly/lane
// tiering is keyed by account emails and would render garbage for member ids.
// Telegram-tier for v1 — external monitoring owns the emergency page. History gating
// still applies: the dedup key is (class, from→to) — the session id is
// deliberately EXCLUDED, so two sessions flap-flopping the same member pair
// read as ONE alert stream and gate 2 suppresses the repeats.
// classAlertQuiet reports whether a class switchover to or from a member or
// account (its id, or an id with it as one "/" segment) sends no alert: one
// that is often away by design, for which a switchover is the ordinary
// route. An edition may set it; by default every switchover alerts.
var classAlertQuiet = func(id string) bool { return false }

func (tn *tenant) fireClassAlert(class, session, from, to, reason string) {
	log.Printf("🔀 CLASS SWITCHOVER[%s] sess=%s %s -> %s (%s)", class, fpSess(session), from, to, reason)
	if classAlertQuiet(from) || classAlertQuiet(to) {
		// A member whose closing is routine (classAlertQuiet) pages no one
		// on a switchover to or from it. The log line above records it.
		return
	}
	if bridgeTarget() == "" {
		log.Printf("   (no SWITCH_BRIDGE_URL set — class switch alert skipped)")
		return
	}
	entry := map[string]any{
		"status": "firing",
		"labels": map[string]string{
			"alertname": "ClaudeClassSwitchover", "alertroute": "claude-usage",
			"delivery": "telegram", "lane": "class:" + class,
			"from_account": from, "to_account": to,
		},
		"annotations": map[string]string{
			"title":   "🔀 Claude class switchover",
			"summary": fmt.Sprintf("%s session %s now using %s (was %s)", class, fpSess(session), to, from),
			"description": fmt.Sprintf("Class %s session %s moved from %s to %s. %s. "+
				"(Observed at this session's first class request since the move.)",
				class, fpSess(session), from, to, reason),
		},
	}
	// nil demote closure: class alerts are always telegram-tier, so gate 1
	// (critical cooldown) can never fire for them — only gate 2 dedup applies.
	// nil tierInputs for the same reason: there is no tiering decision to show.
	rec := &alertRecord{alertname: "ClaudeClassSwitchover", tenant: tn.id, lane: "class:" + class, from: from, to: to, tier: "telegram"}
	if out := gateAndEnqueue(rec, entry, reason, nil, nil); !out.enqueued {
		// Suppressed dispatches publish at the caller; enqueued ones publish
		// from flushSwitchAlerts (see alert_history.go's ordering contract).
		publish(tn.id, "alert_dispatch", out.dispatch)
		log.Printf("   suppressed: identical class alert (class=%s %s->%s) fired within %s", class, from, to, alertRepeatWindow)
	}
}

// memberEff labels a provider member's served hop, mirroring the anthropic
// member@account form. A pool of one whose account name IS the provider id
// (kimi, a legacy single-chain gpt) adds nothing — keep the bare member id so
// labels only change once genuinely named pool accounts exist.
func memberEff(m classMemberPref, poolAccount string) string {
	if poolAccount != "" && poolAccount != m.Provider {
		return m.ID + "@" + poolAccount
	}
	return m.ID
}

// ---- request-time routing ----

// classOutcome is routeClass's verdict: either refuse (typed 503, message in
// refuse) or an actuated reqInfo ready for the proxy.
type classOutcome struct {
	info   reqInfo
	eff    string
	tag    string
	refuse string
}

// imageNoMemberCan reports whether q is an image request every member of
// the class refuses on capability, each with a field: the client's request,
// which no quota or health would change.
func imageNoMemberCan(members []classMemberPref, q classReq) bool {
	if q.image == nil || len(members) == 0 {
		return false
	}
	for _, m := range members {
		if cs := q.kind().canServe(m, q); cs == nil || cs.Field == "" {
			return false
		}
	}
	return true
}

// routeClass resolves and actuates a class-routed request. The try-order is the
// bound member first (brain-authoritative), then — only when it can't serve —
// the local candidates in priority order; the first successful ACTUATION wins.
func (tn *tenant) routeClass(ce *registryEntry, sess string, q classReq, hdr http.Header, decisionID ...string) classOutcome {
	// The walk is tn's: every helper below that hands q's context on (a key
	// ask, a hop) reads tn from it, whatever the caller stamped or left bare.
	q.ctx = tn.ctxOf(q.ctx)
	class := ce.Class
	body, inPath, wireOpenAI := q.origBody, q.inPath, q.wireOpenAI
	laneKey := "class:" + class
	bound, bindReason, bindState, refusal, unavail := tn.bindClassMember(class, sess)
	if unavail != nil {
		// §2.8: the brain bound nothing; its body is the reply.
		tn.pref.mu.RLock()
		members := tn.pref.classes[class].Members
		tn.pref.mu.RUnlock()
		return tn.unavailableOutcome(ce, class, sess, bound, members, q, hdr, tn.healthReqIDOf(decisionID), unavail)
	}
	if refusal != "" {
		tn.noteState(laneKey, stateFailed)
		return classOutcome{eff: ce.renderedID(), refuse: refusal,
			tag: " CLASS[" + class + "] UNROUTABLE (" + refusal + ")"}
	}
	if bindState == stateFailed {
		tn.noteState(laneKey, stateFailed)
		reason := "brain unreachable and no cached class binding"
		return classOutcome{eff: ce.renderedID(), refuse: reason,
			tag: " CLASS[" + class + "] FAILED (" + reason + ")"}
	}

	tn.pref.mu.RLock()
	var members []classMemberPref
	if cp, has := tn.pref.classes[class]; has {
		members = cp.Members
	}
	tn.pref.mu.RUnlock()
	members = withRequest(members, q)
	// /class-bind names the member and carries its configuration (the newest
	// copy: a bind is fetched per session); the live view a dial reads (route,
	// incumbent, targets, effective) is only on /preferred, so it comes from
	// that snapshot's entry of the same id. A member the snapshot does not
	// have keeps none: no route, so no account may be dialled for it, and the
	// try list moves on.
	if lm, ok := memberByID(members, bound.ID); ok {
		bound.Route, bound.Incumbent, bound.Targets, bound.Effective = lm.Route, lm.Incumbent, lm.Targets, lm.Effective
	}
	bound.req = q.kind().request(bound, q)

	reqID := tn.healthReqIDOf(decisionID)
	// Read the binding once: session facts keeps the pin with health off,
	// while routing's pin remains gated on health, exactly as before.
	pinFact, _ := tn.pinForBinding(class, sess, bindReason)
	pinned := tn.health != nil && pinFact

	// Each keyed member's key is asked before any check reads its records
	// (memberKeyMissing; cached, so this asks the brain only when a key was
	// dropped or has aged, and a failed ask stands for apiKeyRetry, so the
	// walk's own asks after it wait on no hung brain): a refusal's record of a key ends once the account's
	// key is another (tenant.holds), so a key the owner replaced after the
	// refusal serves at once, the bound member's check included.
	for _, m := range members {
		tn.memberKeyMissing(q.context(), m)
	}

	// §5.2 (#123): under enforce, the health verdict picks the LEAD — the
	// member that heads the try list — and may short-circuit to a typed 503
	// (nothing admissible) or §5.4's synthetic 529 (everything OPEN). Under
	// observe (and off) the lead is the bound member, exactly today's flow.
	lead, hopKind, routeReason := bound, "bound", ""
	if pinned {
		hopKind = "pinned"
	}
	var lease *probeLease
	enforced := healthEnforcedClass(class)
	if enforced {
		hl := tn.decideHealthRoute(q.context(), class, sess, bound, members, body, wireOpenAI, reqID, pinned)
		switch {
		case hl.refuse != "":
			tn.noteState(laneKey, stateFailed)
			return classOutcome{eff: ce.renderedID(), refuse: hl.refuse,
				tag: " CLASS[" + class + "] UNROUTABLE (" + hl.refuse + ")"}
		case hl.exhaust:
			return tn.exhaustOutcome(ce, class, sess, bound, members, q, hdr, reqID)
		}
		lead, hopKind, routeReason, lease = hl.m, hl.hopKind, hl.reason, hl.lease
	}
	abandonLease := func() {
		if lease != nil {
			tn.health.settleLease(lease, outcomeProbeAbandoned, sample{class: class, reqID: reqID})
		}
	}

	// serve() (§5.2): the lead enters the existing try-list construction
	// first. A health-chosen lead goes first even when its check is held out
	// only by an unattributed record (PR11 re-admitted it deliberately, §2.2
	// row 14); a bound/pinned lead goes first only while its check admits it
	// (row 6). A refused member is dialled again only when its record ends.
	//
	// count_tokens asks entitlement only (§2.2 row 20): it is free, so no
	// check holds a member out — the lead, then every other member in order.
	countTokens := strings.HasSuffix(inPath, "/count_tokens")
	var try []classMemberPref
	switch {
	case countTokens:
		try = append(try, lead)
		for _, m := range members {
			if m.ID != lead.ID {
				try = append(try, m)
			}
		}
	case hopKind == "bound" && inPriorityOrder(members):
		// A class in priority order walks its members in that order on
		// every request: the session's binding names no lead of its own.
		try = tn.classCandidates(q.ctx, members, "")
	case hopKind == "bound" && tn.memberThrottled(lead, nowFn()):
		// A throttle deranks (§2.3): the bound member, throttled on the
		// account it would dial, ranks with the rest, so a healthy member of
		// its funding tier serves first. It is still dialled when nothing
		// healthy is left — a throttle is not a `no`. A pin is never detoured.
		try = tn.classCandidates(q.ctx, members, "")
	case tn.memberDialable(lead, nowFn()) == "" || (enforced && hopKind != "bound" && hopKind != "pinned"):
		try = append([]classMemberPref{lead}, tn.classCandidates(q.ctx, members, lead.ID)...)
	default:
		try = tn.classCandidates(q.ctx, members, lead.ID)
	}
	// Unavailability (§2.8, §2.2 row 5), judged over every member this wire
	// could possibly have used — NOT over try, which has already had the
	// refused members pulled out of it — on the same checks the dial asks.
	// Every client is refused when no member would get a real attempt; its
	// wire picks how the reply is rendered.
	//
	// Deliberately NOT gated on the health axis being enforced — quota is its own
	// axis and this must work with SWITCH_HEALTH off.
	if !countTokens {
		if u := tn.classUnavailable(members, q, nowFn()); u != nil {
			abandonLease()
			return tn.unavailableOutcome(ce, class, sess, bound, members, q, hdr, reqID, u)
		}
	}
	// Capability before money (§2.9): the request's kind filters the whole
	// try-order (bound member included) down to the members that can serve it
	// at all — D2 for chat on the OpenAI wire (a subscription bearer is never
	// put behind an OpenAI-compatible endpoint), provider members with an image
	// side for an image. A class none of whose members can is unroutable for it.
	kind := q.kind()
	filtered := try[:0:0]
	var skips []string
	for _, m := range try {
		if cs := kind.canServe(m, q); cs != nil {
			// The skip names its field (§2.9): the reply lists the member too
			// (classUnavailable's cannot_serve), and the log says why here.
			skips = append(skips, m.ID+" ("+cs.String()+")")
			continue
		}
		filtered = append(filtered, m)
	}
	if len(skips) > 0 && q.image != nil {
		log.Printf("     CLASS[%s] skips %s", class, strings.Join(skips, "; "))
	}
	if len(filtered) == 0 && len(try) > 0 {
		abandonLease()
		// An image no member of the class can draw at all is the client's
		// request, not the class failing (the image door answers it 400):
		// the class's state is left as it was.
		if !imageNoMemberCan(members, q) {
			tn.noteState(laneKey, stateFailed)
		}
		reason := "class " + class + " has no members that serve " + kind.name()
		if q.image != nil {
			reason = "class " + class + " has no member that can draw this image request: " + strings.Join(skips, "; ")
		}
		if wireOpenAI && q.image == nil {
			reason = "class " + class + " has no provider members usable on the openai wire"
		}
		return classOutcome{eff: ce.renderedID(), refuse: reason,
			tag: " CLASS[" + class + "] UNROUTABLE (" + reason + ")"}
	}
	try = filtered
	if classPreActuateHook != nil {
		classPreActuateHook() // tests: interleave a concurrent request here
	}
	unactuated := map[string]string{}
	for _, m := range try {
		readmit := enforced && m.ID == lead.ID && hopKind != "bound" && hopKind != "pinned"
		info, eff, state, ok := tn.actuateClassMember(class, m, bindState, q, readmit, unactuated)
		if !ok {
			continue
		}
		info.scale = newScaleMark(memberScaleKey(m), info.ratio)
		reason := bindReason
		if m.ID != bound.ID {
			reason = "" // a detour; the tag below is its explanation, not the binding's
		}
		hk, rr := hopKind, routeReason
		if enforced && m.ID != lead.ID {
			// Actuation fell past the health verdict's lead — a local detour
			// the FSM didn't choose (no token / no wire on the lead).
			hk, rr = "detour", "health:fallback"
		}
		ccLease := lease
		if lease != nil && lease.member != m.ID {
			// The leased member didn't actuate: the probe never flew (Rule 4).
			tn.health.settleLease(lease, outcomeProbeAbandoned, sample{class: class, reqID: reqID})
			ccLease = nil
		}
		info.classCtx = &classRoute{classReq: q, tenant: tn, class: class, session: sess, member: m,
			members: members, origHeader: hdr.Clone(),
			served: newServedHop(class, m, info, eff, reason), scale: info.scale, bindReason: reason,
			reqID: reqID, pinned: pinned, pinFact: pinFact, hopKind: hk, routeReason: rr, lease: ccLease, boundMember: bound.ID}
		tn.noteState(laneKey, state)
		tag := " CLASS[" + class + "→" + m.ID + "] sess=" + fpSess(sess) + " " + state.String() + " check=" + info.check.String()
		if m.ID != bound.ID {
			tag += " (detour from " + bound.ID + ")"
		}
		if rr != "" && rr != "health:fallback" {
			tag += " [" + rr + "]"
		}
		return classOutcome{info: info, eff: eff, tag: tag}
	}
	abandonLease()
	if !countTokens {
		// The pre-flight said a member would get an attempt, but by actuation
		// it would not — a concurrent request's refusal wrote a record in
		// between. That is unavailability, not "nothing usable".
		if u := tn.classUnavailable(members, q, nowFn()); u != nil {
			return tn.unavailableOutcome(ce, class, sess, bound, members, q, hdr, reqID, u)
		}
	}
	tn.noteState(laneKey, stateFailed)
	reason := "no member of class " + class + " is usable (bound: " + bound.ID + ")"
	if len(unactuated) > 0 {
		ids := make([]string, 0, len(unactuated))
		for id := range unactuated {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		var skipped []string
		for _, id := range ids {
			skipped = append(skipped, id+" (account kind "+unactuated[id]+")")
		}
		reason += "; skipped for want of an actuator in this build: " + strings.Join(skipped, ", ")
	}
	return classOutcome{eff: ce.renderedID(), refuse: reason,
		tag: " CLASS[" + class + "] FAILED (" + reason + ")"}
}

// classPreActuateHook runs between routeClass's pre-flight and actuation. nil
// in production; a test sets it to stand in for a concurrent request (one whose
// refusal writes a record the pre-flight did not see).
var classPreActuateHook func()

// actuateClassMember builds the reqInfo that routes body onto member m, or
// ok=false when m can't serve (missing provider, no declared model — an old
// brain without the #27 schema — no account whose dial check admits it, no
// usable token for that account, or a member whose wire refuses this request —
// e.g. a translated wire asked to serve a path its upstream has no equivalent
// for). The check is asked here, at the dial, for the account the dial would
// use (§2.2 rows 6, 7, 9); count_tokens asks only entitlement (row 20).
// readmit is PR11's re-admission (row 14): the health verdict chose a member
// an unattributed record alone holds out.
//
// unactuated collects, per member id, the account kind an anthropic member
// was skipped for having no actuator in this build.
func (tn *tenant) actuateClassMember(class string, m classMemberPref, bindState routeState, q classReq, readmit bool, unactuated map[string]string) (reqInfo, string, routeState, bool) {
	if m.Model == "" {
		return reqInfo{}, "", 0, false // brain predates the anthropic-member model field
	}
	if q.kind().canServe(m, q) != nil {
		return reqInfo{}, "", 0, false
	}
	inPath := q.inPath
	switch m.Kind {
	case "anthropic":
		nb := q.origBody
		if b := rewriteModel(q.origBody, m.Model); b != nil {
			nb = b
		}
		// The member IS lane traffic on its own route: the lane's pick while
		// the member's check admits it, else the route's next account, sharing
		// the lane's bookkeeping and alerts.
		d := tn.anthropicMemberDial(m, strings.HasSuffix(inPath, "/count_tokens"), readmit)
		tn.noteState(m.Lane, d.state)
		if d.unactuated != "" {
			// Never dialled, and said: the member's accounts are of a kind
			// this build cannot sign for (actuator.go).
			log.Printf("     CLASS[%s] skips %s: its accounts are of kind %s, which this build has no actuator for", class, m.ID, d.unactuated)
			unactuated[m.ID] = d.unactuated
		}
		if d.none || d.state == stateFailed {
			return reqInfo{}, "", 0, false
		}
		tn.noteEffective(m.Lane, d.acct, d.reason)
		state := d.state
		if bindState == stateStale {
			state = stateStale // riding a cached binding is stale even on a fresh token
		}
		// Anthropic member (wire==nil): optionally force the 1h prompt-cache TTL. The
		// caller re-sets r.Body/ContentLength from this body, and the 429 replay reads
		// reqInfo.body, so rewriting nb here covers both; the beta goes on in Rewrite.
		if forceCache1h {
			nb = force1hCacheControl(nb)
		}
		return reqInfo{tenant: tn, token: d.tok, eff: d.acct, state: state, lane: m.Lane, body: nb, check: d.check},
			m.ID + "@" + d.acct, state, true
	case "provider":
		p := providerByName(m.Provider)
		if p == nil {
			return reqInfo{}, "", 0, false
		}
		acct, chk, ok := tn.providerMemberDial(m, "", readmit, strings.HasSuffix(inPath, "/count_tokens"), nowFn())
		if !ok {
			return reqInfo{}, "", 0, false
		}
		// The member's wire (#41) validates + prepares the request, through
		// the request's kind (a chat body spliced, or one image built). A
		// refusal here just falls through to the next candidate — an anthropic
		// member can serve anything this class advertises, including the
		// /count_tokens calls a translated wire may have no equivalent for.
		info := reqInfo{tenant: tn, eff: m.ID, state: bindState, lane: "class:" + class,
			bypass: true, upstream: p.target, wire: p.wire,
			poolAccount: acct, check: chk,
			ratio: memberRatio(m)}
		if refuse := q.kind().prepare(p, m, &info, q); refuse != "" {
			return reqInfo{}, "", 0, false
		}
		eff := memberEff(m, info.poolAccount)
		info.eff = eff
		return info, eff, bindState, true
	}
	return reqInfo{}, "", 0, false
}

// ---- mid-request 429 ladder ----

// bufferResp buffers up to 64KiB of an error response body and restores it so
// the caller can still hand the response back intact.
func bufferResp(resp *http.Response) []byte {
	if resp.Body == nil {
		return nil
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(b))
	return b
}

// ---- what an upstream refusal MEANS for the ladder (#89, #90) ----
//
// The ladder used to react to exactly one thing: HTTP 429. Two refusals broke
// that, in opposite directions:
//
//   - kimi's quota cap arrives as a 403 (`permission_error`, "You've reached
//     your usage limit for this billing cycle"), so a spent kimi rung was never
//     cooled and never walked past — every request 403'd straight through (#89).
//   - Anthropic's client-shape refusal arrives as a 429 (`rate_limit_error`,
//     message literally "Error", NO Anthropic-Ratelimit-Unified-* headers). It
//     is not a quota signal at all: a subscription OAuth bearer is served only
//     to requests whose first system block is the Claude Code identity line, and
//     the same account served Claude Code in the same minute. Treating it as
//     capacity cooled the account, swapped to the sibling account, and fired a
//     switchover — a rung the client can never use was flipping the human's
//     lane (#90).

func errBodyTypeMsg(body []byte) (string, string) {
	var e struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &e) != nil {
		return "", ""
	}
	return e.Error.Type, strings.TrimSpace(e.Error.Message)
}

// logBodyMax bounds how much of a provider's words a household log line
// quotes.
const logBodyMax = 300

// logProviderText is how a log line about tn's request shows the provider's
// words (a refusal's body, an error's message): the household's quoted, as
// key=%q (bare %q when key is ""), clipped to logBodyMax bytes; a member's
// never, only the error type they carried, as error_type=%q. The log is the
// operator's, and the operator sees a member's metadata and nothing more.
func (tn *tenant) logProviderText(key, errType, text string) string {
	if tn == nil || !tn.lan {
		if errType == "" {
			errType = "unknown"
		}
		return "error_type=" + strconv.Quote(tapShort(errType, 64))
	}
	if len(text) > logBodyMax {
		text = text[:logBodyMax] + "…"
	}
	if key == "" {
		return strconv.Quote(text)
	}
	return key + "=" + strconv.Quote(text)
}

// logBody is logProviderText for a provider's response body: body=%q for
// the household, the body's error type for a member.
func (tn *tenant) logBody(body []byte) string {
	errType, _ := errBodyTypeMsg(body)
	return tn.logProviderText("body", errType, string(body))
}

// shapeRejected429 reports Anthropic's client-shape refusal (#90). Both marks
// are required — the bare message AND the absence of the unified ratelimit
// headers a capacity 429 carries — so a real quota 429 that merely lacks a
// descriptive message still cools the account as before.
func shapeRejected429(resp *http.Response, body []byte) bool {
	if resp == nil || resp.StatusCode != 429 {
		return false
	}
	if resp.Header.Get("Anthropic-Ratelimit-Unified-Status") != "" {
		return false
	}
	typ, msg := errBodyTypeMsg(body)
	return typ == "rate_limit_error" && strings.EqualFold(msg, "Error")
}

// classRoundTrip is the bounded 429 ladder for a class-routed request: bound
// member -> (anthropic / pooled provider) one same-member account swap -> a walk
// across the remaining candidate members, headroom first (see rankCandidates).
// All retries happen before ReverseProxy writes a byte to the
// client, so streaming is unaffected. The member cooldown set here is what
// steers SUBSEQUENT requests away (routeClass's detour) until it expires — the
// brain never learns about 429s (no feedback channel by design).
func (t *failoverTransport) classRoundTrip(req *http.Request, info reqInfo) (*http.Response, error) {
	resp, err := t.classRoundTripInner(req, info)
	info.classCtx.tenant.healthNoteWouldAct(info.classCtx)
	info.classCtx.trace.head(info)
	noteLastServed(info.classCtx)
	return resp, err
}

func (t *failoverTransport) classRoundTripInner(req *http.Request, info reqInfo) (*http.Response, error) {
	cc := info.classCtx
	tn := cc.tenant
	if cc.healthExhaust {
		// §5.2's allOpen verdict, delivered without a dial: routeClass built
		// this classCtx purely to carry §5.4's synthetic 529 out through
		// RoundTrip, so the response tap sees it like any upstream reply.
		return tn.degradedExhaustResponse(cc, cc.exhaustPinned), nil
	}
	if cc.unavail != nil {
		// Same shape on the quota axis: the class is unavailable, so there is
		// nothing to dial. Without this, a request rode a spent
		// kimi rung into a 120 s hang and reported itself as "timeout".
		return tn.unavailableResponse(cc), nil
	}
	resp, err := t.tripFirstEvent(req, info) // no-op wrapper for plain Anthropic attempts; one stall resend on a budget class
	if errors.Is(err, errResendRefused) {
		// §2.2 row 18: the stalled hop's account is `no` now — walk the rest,
		// on the quota axis (the check, not health, turned it away).
		before := cc.served.snapshot().attempts
		resp, err, _ = t.walkRungs(req, info, nil, refusal{}, walkOpt{markQuota: true, alert: true,
			cause: "first-event resend refused — mid-request member failover"})
		if tn.health != nil && cc.served.snapshot().attempts > before {
			cc.walked = "quota"
		}
		if err == nil && resp == nil {
			err = fmt.Errorf("class %s: %w, and no other member may be dialled", cc.class, errResendRefused)
		}
		return resp, err
	}
	// §3 Rule 1's head site, deliberately BEFORE the err/nil early return so a
	// later stage can see the transport errors the ladder is blind to today
	// (spec signals-4).
	out := classify(cc.member, req.Context(), resp, err)
	out.lease = cc.leaseFor(cc.member.ID) // a final head settles the probe here (Rule 4)
	tn.recordHopHead(cc, cc.member, out)
	if healthEnforcedClass(cc.class) && out.ladder == ladderDetour {
		// §5.3: degraded AT THE HEAD (≥500/529, or a transport error with no
		// response) — the one failure shape health may walk. Row 18 (proxy
		// deadline) is ladderReturn and rides; post-head failures never reach
		// here (the shape sites own them, and they are delivered as-is).
		return t.healthWalk(req, info, resp, err, out.cause, false)
	}
	if err != nil || resp == nil {
		return resp, err
	}
	if resp.StatusCode == http.StatusNotFound {
		// A 404 once came back with out=unknown nobody could explain
		// afterwards. Same bounded snippet the refusals get.
		log.Printf("⚠️  404 on class %s member %s (eff=%s) %s", cc.class, cc.member.ID, info.eff, tn.logBody(bufferResp(resp)))
	}
	if out.ladder != ladderWalkQuota {
		return resp, err
	}
	log.Printf("⚠️  %d %s on class %s member %s (eff=%s) ratelimit[%s] %s",
		resp.StatusCode, out.refusal.class, cc.class, cc.member.ID, info.eff, ratelimitDetail(resp.Header), tn.logBody(out.refusal.body))
	before := cc.served.snapshot().attempts
	if tn.health != nil {
		cc.quotaNote = captureNoteWalk(cc.member.ID, resp)
		cc.quotaNote.shapeRejected = out.refusal.class == refWrongMember
	}
	resp, err, exhausted := t.walkRungs(req, info, resp, out.refusal, walkOpt{markQuota: true, alert: true})
	if tn.health != nil && cc.served.snapshot().attempts > before {
		cc.walked = "quota"
	}
	if exhausted && err == nil {
		if q := classExhaustAfterWalk(cc, resp); q != nil {
			if resp != nil {
				_ = resp.Body.Close()
			}
			resp = q
		}
	}
	if healthEnforcedClass(cc.class) {
		// "both" (§5.3): the quota walk's landing member may itself have
		// head-failed — its outcome was already recorded inside the walk;
		// this classify only re-reads the ladder to decide the second walk.
		sid := cc.served.snapshot().member
		if lm, ok := memberByID(cc.members, sid); ok {
			if out2 := classify(lm, req.Context(), resp, err); out2.ladder == ladderDetour {
				return t.healthWalk(req, info, resp, err, out2.cause, true)
			}
		}
	}
	return resp, err
}

// walkOpt selects the walk's caller-visible behaviour (degraded-failover spec
// §5.3). The quota ladder — today's only caller — passes {markQuota: true,
// alert: true}; the health detour (#123) will pass its own cause and
// pre-ranked candidates with both flags false.
type walkOpt struct {
	cause      string   // crossReason seed; "" derives it from the refusal in hand (the quota rule)
	markQuota  bool     // write each refusal to the record store as the walk moves
	alert      bool     // fire the switch/class alerts for each move
	candidates []ranked // explicit rung order; nil = rank at walk time (the quota rule)
	health     bool     // §5.3 continuation: classify each rung — detour/walk_quota passes on, anything else serves; resp may be nil (transport head failure)
	skipSwaps  bool     // "both" (§5.3): the serving member is the quota walk's landing, not cc.member — the same-member swap arms would act on the wrong member
}

// walkRungs is the extracted mid-request ladder (spec §5.3): one same-member
// account swap — the anthropic lane-account replay, or the #71 provider pool
// swap — then one hop per remaining candidate. resp and ref are the refusal
// already in hand, ref its adapter's class (refusal.go). exhausted reports that every rung refused (or nothing could
// actuate): resp is then the LAST refusal, surfaced to the client intact.
//
// Every refusal is written to the one record store (records.go) under the
// account that refused, and every dial — swap or cross — asks the check again
// for the account it would use (§2.2 rows 8, 9, 11), so an account a refusal
// in this very walk made `no` is never dialled. A throttle on an included
// route stops the walk at the paid tier (§2.3): the 429 goes back with the
// shortest Retry-After rather than money dodging a short wait.
func (t *failoverTransport) walkRungs(req *http.Request, info reqInfo, resp *http.Response, ref refusal, opt walkOpt) (_ *http.Response, _ error, exhausted bool) {
	cc := info.classCtx
	tn := cc.tenant
	body := info.body
	var err error
	crossReason := opt.cause
	if crossReason == "" {
		crossReason = refusalCross(resp, ref)
	}
	// throttled is the included accounts this walk saw throttle: while one is
	// held only by that, money does not serve.
	var throttled []string
	// count_tokens is entitlement only (§2.2 row 20): it writes no record.
	countTokens := strings.HasSuffix(cc.inPath, "/count_tokens")
	note := func(m classMemberPref, account string, c dialCheck, r *http.Response, rf refusal) {
		if !opt.markQuota || countTokens {
			return
		}
		kind, what := tn.recordRefusal(account, rf, r)
		if kind == "" {
			return
		}
		log.Printf("⚠️  class %s member %s account %s refused (%s) — recorded %s %s",
			cc.class, m.ID, account, c.String(), kind, what)
		if kind == recThrottled && c.included() {
			throttled = append(throttled, account)
		}
	}

	if !opt.health && cc.member.Kind == "anthropic" && ref.class == refWrongMember {
		// #90: not capacity. The account is fine; this CLIENT's request shape is
		// what Anthropic refused. No record (§2.6: a wrong-member refusal is no
		// usage fact about anyone), no sibling swap, no lane switchover — walk
		// to a rung whose credential this client can use.
		log.Printf("⚠️  429 on class %s member %s is a client-shape rejection (message \"Error\", no ratelimit headers) — not capacity; account %s untouched, walking",
			cc.class, cc.member.ID, info.eff)
		crossReason = "client-shape rejection — mid-request member failover"
	} else if !opt.health && cc.member.Kind == "anthropic" {
		// A 429 on one account says nothing about the route's other accounts —
		// swap accounts on the SAME member first (§2.2 row 8).
		note(cc.member, info.eff, info.check, resp, ref)
		if ftok, facct, fc, ok := tn.failoverRoute(cc.member.Route, cc.member.Lane, cc.member.Model, info.eff); ok &&
			!(len(throttled) > 0 && fc.paid()) {
			log.Printf("⚠️  429 on %s — retrying class member %s on account %s check=%s", info.eff, cc.member.ID, facct, fc)
			tn.forceEffective(cc.member.Lane, facct)
			st.mu.Lock()
			st.switches++
			st.mu.Unlock()
			if opt.alert {
				tn.fireSwitchAlert(cc.member.Lane, info.eff, facct, "hit a usage limit ("+ref.class+") — auto-flipped mid-request")
			}
			resp.Body.Close()
			req2 := req.Clone(withAnthDial(context.WithValue(req.Context(), servedCheckKey{}, fc), facct, body))
			req2.Body = io.NopCloser(bytes.NewReader(body))
			req2.ContentLength = int64(len(body))
			tn.signAccount(req2.Header, facct, ftok)
			resp, err = t.tripWithReauth(req2) // a 401 on a rotated copy re-fetches and retries once
			cc.served.selectHop(cc.member, facct, cc.member.Lane, false, 0)
			if tn.health != nil { // §5.3: the head site covers the walk's re-issues too
				tn.recordHopHead(cc, cc.member, classify(cc.member, req2.Context(), resp, err))
			}
			if err != nil || resp == nil {
				return resp, err, false
			}
			// The swapped account's refusal walks on as the member's; any
			// other answer (served, invalid, degraded) is the reply.
			if ref = tn.rungRefusal(cc.member, resp); !ref.walks() {
				return resp, err, false
			}
			note(cc.member, facct, fc, resp, ref)
		}
	}

	if !opt.skipSwaps && cc.member.Kind == "provider" {
		// A refusal on one pool account says nothing about the provider's other
		// accounts — swap accounts on the SAME member first (#71), the exact
		// mirror of the anthropic same-lane swap above. One swap, keeping the
		// ladder bounded; the account's record steers every class.
		if !opt.health {
			note(cc.member, servedAccount(cc.member, memberEff(cc.member, info.poolAccount)), info.check, resp, ref)
		}
		if info.poolAccount != "" {
			if alt, _, ok := tn.providerMemberDial(cc.member, "", false, countTokens, nowFn()); ok && alt != info.poolAccount {
				if req2, eff2, ac, ok := t.rebuildForMember(req, cc, cc.member, alt, false); ok {
					log.Printf("⚠️  429 on %s — retrying class member %s on pool account %s check=%s",
						info.eff, cc.member.ID, alt, ac)
					st.mu.Lock()
					st.switches++
					st.mu.Unlock()
					if opt.alert {
						tn.fireClassAlert(cc.class, cc.session, info.eff, eff2,
							"429 — mid-request pool account failover")
					}
					if resp != nil {
						resp.Body.Close()
					}
					resp, err = t.tripWithReauth(req2)
					_, translated := providerByName(cc.member.Provider).wire.(codexWire)
					cc.served.selectHop(cc.member, servedAccount(cc.member, eff2), "class:"+cc.class, translated, memberRatio(cc.member))
					var hout hopOutcome
					if tn.health != nil {
						hout = classify(cc.member, req2.Context(), resp, err)
						tn.recordHopHead(cc, cc.member, hout)
					}
					if opt.health {
						// §5.3 continuation: a swapped-account head that fails
						// (detour) or refuses (walk_quota) passes the walk on to
						// the member cross; anything else is the served reply.
						if hout.ladder != ladderDetour && hout.ladder != ladderWalkQuota {
							return resp, err, false
						}
						ref = hout.refusal
					} else {
						if err != nil || resp == nil {
							return resp, err, false
						}
						if ref = tn.rungRefusal(cc.member, resp); !ref.walks() {
							return resp, err, false
						}
						note(cc.member, alt, ac, resp, ref)
					}
				}
			}
		}
	}

	// The member itself is out (every account, or the provider's own limit):
	// walk the remaining candidates in rankCandidates order — included before
	// paid, healthy before throttled or OPEN — re-actuating each for its kind
	// (model + upstream + auth change). Ranked after the records above, so the
	// refused accounts are already `no` here. One hop per candidate, so the
	// walk is bounded by the class's member count; a rung
	// that refuses is recorded like the bound member was, so the NEXT request
	// skips it too. Before this walk the ladder crossed exactly once, to
	// whatever came first — which once was the 100% rung, while a 34%
	// rung sat idle and the client saw the 429.
	cands := opt.candidates
	if cands == nil {
		var skipped []memberSkip
		cands, skipped = tn.rankCandidatesSkips(cc.members, cc.member.ID, rankOpt{ctx: req.Context()})
		if len(skipped) > 0 {
			log.Printf("⚠️  class %s walk from %s skips %s", cc.class, cc.member.ID, renderSkips(skipped))
		}
	}
	from := cc.member.ID
	for _, rc := range cands {
		next := rc.m
		if cs := cc.kind().canServe(next, cc.classReq); cs != nil {
			// Capability before money (§2.9); D2: the OpenAI wire never
			// crosses onto a subscription-bearer member.
			if cc.image != nil {
				log.Printf("     class %s walk skips %s (%s)", cc.class, next.ID, cs)
			}
			continue
		}
		req2, eff2, nc, ok := t.rebuildForMember(req, cc, next, "", rc.tier == tierReadmit)
		if !ok {
			continue // can't actuate this rung (its check is `no` now, no token, no wire) — try the next
		}
		if len(throttled) > 0 && nc.paid() {
			// §2.3: an included route is held only by a throttle — a short,
			// stated wait. Money does not dodge it: the throttle goes back.
			log.Printf("⚠️  class %s walk stops before paid %s (%s): included %s throttled",
				cc.class, next.ID, nc, strings.Join(throttled, ","))
			return tn.throttleReply(resp, throttled), err, true
		}
		if resp != nil {
			resp.Body.Close()
		}
		log.Printf("⚠️  class %s crossing %s -> %s (%s) check=%s mid-request", cc.class, from, next.ID, eff2, nc)
		st.mu.Lock()
		st.switches++
		st.mu.Unlock()
		if opt.alert {
			tn.fireClassAlert(cc.class, cc.session, from, next.ID, crossReason)
		}
		translated := false
		lane := next.Lane
		if next.Kind == "provider" {
			lane = "class:" + cc.class
			if p := providerByName(next.Provider); p != nil {
				_, translated = p.wire.(codexWire)
			}
		}
		resp, err = t.tripWithReauth(req2) // the cross may land on a translated wire — 401-retry applies there too
		cc.served.selectHop(next, servedAccount(next, eff2), lane, translated, memberRatio(next))
		var hout hopOutcome
		if tn.health != nil {
			hout = classify(next, req2.Context(), resp, err)
			tn.recordHopHead(cc, next, hout)
		}
		if opt.health {
			// §5.3 continuation: a rung that head-fails (detour) or refuses
			// (walk_quota) passes the walk on; every other ladder — a 200
			// head, an auth/client stop, a deadline return — is served as-is.
			if hout.ladder != ladderDetour && hout.ladder != ladderWalkQuota {
				return resp, err, false
			}
			ref = hout.refusal
			crossReason = "health: " + hout.cause + " — mid-request member failover"
			log.Printf("⚠️  class %s member %s (crossed) row %s %s — health walk continues",
				cc.class, next.ID, hout.row, hout.sig)
			from = next.ID
			continue
		}
		if err != nil || resp == nil {
			return resp, err, false
		}
		if ref = tn.rungRefusal(next, resp); !ref.walks() {
			return resp, err, false
		}
		log.Printf("⚠️  %d %s on class %s member %s (crossed) ratelimit[%s] %s",
			resp.StatusCode, ref.class, cc.class, next.ID, ratelimitDetail(resp.Header), tn.logBody(ref.body))
		crossReason = refusalCross(resp, ref)
		note(next, servedAccount(next, eff2), nc, resp, ref)
		from = next.ID
	}
	return resp, err, true // every rung refused (or nothing to cross to) — surface the last refusal intact
}

// refusalCross is the cross alert's reason for a refusal the walk moves on
// from: its status and its class (§2.6).
func refusalCross(resp *http.Response, ref refusal) string {
	if ref.class == refWrongMember {
		return "client-shape rejection — mid-request member failover"
	}
	status := 0
	if resp != nil {
		status = resp.StatusCode
	}
	return fmt.Sprintf("%d %s — mid-request member failover", status, ref.class)
}

// throttleReply is the walk's 429 when it stops before paid: the refusal in
// hand, its Retry-After the soonest end of the throttles this walk recorded.
func (tn *tenant) throttleReply(resp *http.Response, accounts []string) *http.Response {
	if resp == nil {
		return nil
	}
	now := nowFn()
	var soonest time.Time
	for _, a := range accounts {
		if t := tn.throttledUntil(a, now); !t.IsZero() && (soonest.IsZero() || t.Before(soonest)) {
			soonest = t
		}
	}
	if !soonest.IsZero() {
		secs := int(soonest.Sub(now).Round(time.Second) / time.Second)
		if secs < 1 {
			secs = 1
		}
		resp.Header.Set("Retry-After", strconv.Itoa(secs))
	}
	return resp
}

// rebuildForMember re-actuates the buffered class request for a member (a
// DIFFERENT member on the cross, or the SAME member on a #71 pool-account
// swap): the model splice starts from the ORIGINAL body (claude-<class> ->
// member model, never a double splice), the headers rebase on the ORIGINAL
// inbound snapshot (attempt 1 may have been a translated wire, whose outbound
// headers were built fresh and would corrupt the next hop), the URL is re-joined
// from the inbound path (each upstream has its own base path — kimi serves
// under /coding), and the auth is the member's own — its wire's credential, or a
// lane account bearer. poolAcct forces a provider pool account; "" lets
// providerMemberDial pick (and is ignored by the anthropic arm). Either way the
// account is dialled only while its check admits it (§2.2 rows 8, 9, 11);
// readmit is PR11's re-admission (row 14). It returns that check.
func (t *failoverTransport) rebuildForMember(req *http.Request, cc *classRoute, m classMemberPref, poolAcct string, readmit bool) (*http.Request, string, dialCheck, bool) {
	if m.Model == "" || cc.kind().canServe(m, cc.classReq) != nil {
		return nil, "", dialCheck{}, false
	}
	tn := cc.tenant
	req2 := req.Clone(req.Context())
	if cc.origHeader != nil {
		req2.Header = cc.origHeader.Clone()
	}
	req2.GetBody = nil                // only a translated wire replays via GetBody; its apply sets one
	req2.Header.Del("Content-Length") // the cross-member rewrite changes the length
	switch m.Kind {
	case "provider":
		p := providerByName(m.Provider)
		if p == nil {
			return nil, "", dialCheck{}, false
		}
		// Prepare on the member's own wire (#41), through the request's kind: a
		// translated wire re-translates the freshly spliced body (or builds the
		// image request again) and re-acquires its credential here, since
		// attempt 1 may have been a different member entirely.
		poolAcct, chk, ok := tn.providerMemberDial(m, poolAcct, readmit, strings.HasSuffix(cc.inPath, "/count_tokens"), nowFn())
		if !ok {
			return nil, "", dialCheck{}, false
		}
		// classCtx rides onto the rebuilt hop (#122): RoundTrip's evidence
		// producers read it off the hop's own context. Mode-off behaviour is
		// unchanged — the only other reader this reaches is clientStream, which
		// computes the same bool prepare already set from the spliced body.
		info := reqInfo{tenant: tn, eff: m.ID, upstream: p.target, wire: p.wire, poolAccount: poolAcct,
			ratio: memberRatio(m), classCtx: cc, check: chk}
		// Re-aim the request's ONE scaling record at this member — the scale
		// site attributes (and gates) by the hop that serves, not the one bound.
		cc.scale.retarget(memberScaleKey(m), info.ratio)
		info.scale = cc.scale
		if refuse := cc.kind().prepare(p, m, &info, cc.classReq); refuse != "" {
			return nil, "", dialCheck{}, false
		}
		info.eff = memberEff(m, info.poolAccount)
		req2.URL.Scheme, req2.URL.Host = p.target.Scheme, p.target.Host
		req2.URL.Path = joinBasePath(p.target.Path, cc.inPath)
		req2.Host = p.target.Host
		req2.Body = io.NopCloser(bytes.NewReader(info.body))
		req2.ContentLength = int64(len(info.body))
		// apply runs LAST: a translated wire overrides the path it just joined
		// and replaces the header set wholesale.
		p.wire.apply(req2, info)
		if info.ratio > 0 && info.ratio != 1 {
			// Crossing INTO a scaled pass-through hop: same Accept-Encoding rule
			// as RoundTrip's entry — the usage rewriter must see readable bytes.
			req2.Header.Del("Accept-Encoding")
		}
		// Make the hop SELF-DESCRIBING. The clone carries attempt 1's context, so
		// without this the request names this member while the reqInfo riding with
		// it still names the previous one — and RoundTrip reads that info to label
		// the translated response. reqInfo is a value type, so this cannot alias
		// attempt 1's; nothing else reads infoKey off an outbound request.
		req2 = req2.WithContext(context.WithValue(req2.Context(), infoKey, info))
		return req2, info.eff, chk, true
	case "anthropic":
		body := rewriteModel(cc.origBody, m.Model)
		if body == nil {
			return nil, "", dialCheck{}, false
		}
		d := tn.anthropicMemberDial(m, strings.HasSuffix(cc.inPath, "/count_tokens"), readmit)
		if d.none || d.state == stateFailed {
			return nil, "", dialCheck{}, false
		}
		req2.Body = io.NopCloser(bytes.NewReader(body))
		req2.ContentLength = int64(len(body))
		tok, acct := d.tok, d.acct
		req2.URL.Scheme, req2.URL.Host = t.anthropic.Scheme, t.anthropic.Host
		req2.URL.Path = joinBasePath(t.anthropic.Path, cc.inPath)
		req2.Host = t.anthropic.Host
		// Attempt 1 may have been a PROVIDER member, whose outbound request never
		// went through the account Rewrite — so the account's actuator signs this
		// clone whole here (for a subscription account, the oauth beta flag its
		// bearer requires too), or the provider→anthropic cross 401s only on this
		// path. signAccount drops the provider's key first.
		tn.signAccount(req2.Header, acct, tok)
		// Deliberately NOT re-stamping infoKey the way the provider arm does: an
		// Anthropic hop is never a translated hop, so nothing reads it — and the
		// only honest label here is "<member>@<account>", which is not what eff
		// means on this path (eff is the ACCOUNT). A wrong-shaped eff riding in the
		// context would be a worse trap than the stale one. The shared records
		// (cc.served, cc.scale) carry what the response side needs instead; the
		// retarget zeroes the ratio so RoundTrip's Host guard resolves this hop
		// "none" even though the outer info still carries attempt 1's ratio.
		cc.scale.retarget("", 0)
		// The one thing the response side does need from THIS hop: its lane. A
		// 2xx disproves the scoped windows of the lane that served
		// (noteServed), and the outer info names attempt 1's lane — an
		// opus refusal followed by a sonnet success on the same account would
		// otherwise lift the opus cap. Its own key, so infoKey stays as above.
		req2 = req2.WithContext(context.WithValue(req2.Context(), servedLaneKey{}, m.Lane))
		req2 = req2.WithContext(context.WithValue(req2.Context(), servedCheckKey{}, d.check))
		// And the account and body this hop dials, which the outer info does
		// not name either: a 401 here re-fetches THIS account's copy and
		// replays THIS body (tripWithReauth).
		req2 = req2.WithContext(withAnthDial(req2.Context(), acct, body))
		return req2, m.ID + "@" + acct, d.check, true
	}
	return nil, "", dialCheck{}, false
}

// joinBasePath joins an upstream base path with the inbound request path — the
// same rule httputil's SetURL applies at rewrite time (singleJoiningSlash).
func joinBasePath(base, in string) string {
	switch {
	case base == "" || base == "/":
		return in
	case strings.HasSuffix(base, "/") && strings.HasPrefix(in, "/"):
		return base + in[1:]
	case !strings.HasSuffix(base, "/") && !strings.HasPrefix(in, "/"):
		return base + "/" + in
	}
	return base + in
}

// ---- #122: the head site's record glue (§5.3) ----

// soleAdmissible reports that the serving member was the only admissible hop
// for this request (§4 I5): ranking the class's OTHER members and applying
// the walk's own filters — D2 (wire traffic never crosses onto an anthropic
// member), actuability (a member with no declared model can't serve), and
// R-22's refinement that a request over LARGE_PROMPT_TOK also rules out
// members whose declared window cannot hold it — leaves nothing. Forced
// samples ride the ring for the status surface but never trip E (I5).
func soleAdmissible(cc *classRoute, servingID string) bool {
	est := estInputTokens(cc.origBody)
	for _, rc := range cc.tenant.rankCandidates(cc.members, servingID, rankOpt{ctx: cc.classReq.ctx}) {
		m := rc.m
		if cc.wireOpenAI && m.Kind == "anthropic" {
			continue
		}
		if m.Model == "" {
			continue
		}
		if est > healthLargePromptTok && m.ContextWindow > 0 && est > m.ContextWindow {
			continue
		}
		return false
	}
	return true
}

// recordHopHead scores a hop's HEAD outcome (§5.3): record runs iff the head
// was final — non-200 or errored — because a 200's terminal row belongs to the
// shape sites; the two conditions partition every hop, so nothing is scored
// twice and nothing is missed. The fp/forced work is skipped when the row
// writes no sample and holds no token (Rule 4 vacuous). The walk's re-issued
// hops run through here too, each against its own member's key.
func (tn *tenant) recordHopHead(cc *classRoute, m classMemberPref, out hopOutcome) {
	if cc != nil {
		cc.trace.outcome(out)
	}
	if tn.health == nil || cc == nil || !out.final {
		return
	}
	if out.sig == sigAuth {
		tn.health.noteAuthFailure(m.ID)
	}
	if out.evidence == evNone && out.lease == nil {
		return
	}
	forced := soleAdmissible(cc, m.ID)
	cc.trace.forced(forced)
	healthRecord(tn.health, out, cc.class, cc.reqID, healthFP(cc.origBody), forced)
}

// ---- #123: routing consumption (§5.2 route-time, §5.3 mid-request, §5.4 exhaust) ----

// leaseFor hands a scoring site the request's probe lease iff the hop being
// scored IS the leased member — a walked hop scores its own key and must not
// settle a lease it does not hold (Rule 4 belongs to the leased hop alone).
func (cc *classRoute) leaseFor(memberID string) *probeLease {
	if cc == nil || cc.lease == nil || cc.lease.member != memberID {
		return nil
	}
	return cc.lease
}

// memberByID resolves an id against the request's member snapshot.
func memberByID(members []classMemberPref, id string) (classMemberPref, bool) {
	for _, m := range members {
		if m.ID == id {
			return m, true
		}
	}
	return classMemberPref{}, false
}

// allOpenCand is PR12's trigger: every ranked candidate's key is OPEN. A
// half-open key is probe-able and rides as T2, so it blocks the 529; PR11's
// re-admitted tail (tierReadmit) blocks it too — a member that would answer
// must be tried before the class refuses.
func allOpenCand(cand []ranked) bool {
	for _, rc := range cand {
		if rc.tier == tierReadmit || rc.health != hsOpen {
			return false
		}
	}
	return len(cand) > 0
}

// healthLead is decideHealthRoute's verdict (§5.2): the member that leads the
// try list, tagged with §6.1's hop_kind/reason, an optional probe lease, or
// one of the two short-circuits — a typed 503 (nothing admissible, refuse) or
// §5.4's synthetic 529 (everything admissible is OPEN, exhaust).
type healthLead struct {
	m       classMemberPref
	hopKind string
	reason  string
	lease   *probeLease
	refuse  string
	exhaust bool
}

// decideHealthRoute is §5.2's routeClass verdict, evaluated in the spec's
// precedence order (§5.1 PR0–PR13). It reads the store and may seat an
// election or claim a probe; it never dials and never mutates the bind cache.
func (tn *tenant) decideHealthRoute(ctx context.Context, class, sess string, bound classMemberPref, members []classMemberPref,
	body []byte, wireOpenAI bool, reqID string, pinned bool) healthLead {
	ids := make([]string, 0, len(members))
	for _, m := range members {
		ids = append(ids, m.ID)
	}
	bindAt := tn.bindMemberSince(class, sess)
	h := tn.health.routeHints(bound.ID, ids, class, sess, bindAt)
	if pinned {
		// PR1: a pin is never detoured and never pre-empted. If the class
		// holds a probe token and the pinned hop lands on it, the probe role
		// is adopted (§4.5) — a member whose check is `no` never probes (§0.2).
		var l *probeLease
		if h.halfOpenAny && tn.memberDialable(bound, nowFn()) == "" {
			l = tn.health.adoptProbe(bound.ID, class, bound.ID, reqID, estInputTokens(body))
		}
		hk := "pinned"
		if l != nil {
			hk = "probe"
		}
		return healthLead{m: bound, hopKind: hk, lease: l}
	}
	boundOK := h.boundState == "" || h.boundState == hsHealthy || h.boundState == hsConvalescent
	if !h.halfOpenAny && !h.rampHolds && boundOK {
		// §5.2's fast path (E19): healthy bound member, no probe token up, no
		// ramp pending — serve it with ZERO ranking and zero body walk.
		return healthLead{m: bound, hopKind: "bound"}
	}
	est := estInputTokens(body)
	cand := tn.rankCandidates(members, "", rankOpt{est: est, wire: wireOpenAI, health: true, ctx: ctx})
	// Fast work must not become recovery traffic while a healthy member has
	// capacity. The live canary exposed PR2 stealing a healthy route for a
	// reasoning-only probe; the lease TTL releases bookkeeping, not the stream.
	// Keep pins (above), healthy ramp/bind/election affinity and quota/window/wire
	// admissibility. With no healthy capacity, the ordinary last-resort/probe
	// rules below still apply. Other classes retain their recovery policy.
	if class == "fast" {
		var ready []classMemberPref
		for _, rc := range cand {
			if rc.tier < 2 { // healthy/unknown/convalescent, fresh or reserve
				ready = append(ready, rc.m)
			}
		}
		if len(ready) > 0 {
			if h.rampHolds {
				if m, ok := memberByID(ready, h.rampTo); ok {
					return healthLead{m: m, hopKind: "detour", reason: "health:ramp"}
				}
			}
			if m, ok := memberByID(ready, bound.ID); ok {
				return healthLead{m: m, hopKind: "bound"}
			}
			pick, kind := ready[0], "ranked"
			if seat, ok := tn.health.electionFor(bound.ID, class); ok {
				if m, eligible := memberByID(ready, seat); eligible {
					pick, kind = m, "detour"
				}
			} else if seat, created := tn.health.elect(bound.ID, class, pick.ID); !created {
				if m, eligible := memberByID(ready, seat); eligible {
					pick, kind = m, "detour"
				}
			}
			tn.health.noteSessionDetour(bound.ID, class, sess)
			return healthLead{m: pick, hopKind: kind, reason: "health:" + kind}
		}
	}
	// PR2 — probe pre-emption, ABOVE the election: the first request to catch
	// a HALF_OPEN member's token carries the probe.
	for _, rc := range cand {
		if rc.tier == tierReadmit || rc.health != hsHalfOpen {
			continue
		}
		if l := tn.health.claimProbe(rc.m.ID, class, rc.m.ID, reqID, est); l != nil {
			if rc.m.ID != bound.ID {
				tn.health.noteSessionDetour(bound.ID, class, sess)
			}
			return healthLead{m: rc.m, hopKind: "probe", reason: "health:probe", lease: l}
		}
	}
	if h.rampHolds {
		// §4.6's ramp hold, tested above PR3 (BL-9) — but only while the
		// detour member itself can still serve; a degraded or refused detour
		// member falls through to the ordinary precedence below.
		if dm, ok := memberByID(members, h.rampTo); ok && tn.memberDialable(dm, nowFn()) == "" &&
			!(wireOpenAI && dm.Kind == "anthropic") {
			if ss := tn.health.stateOf(dm.ID); ss != hsOpen && ss != hsHalfOpen {
				return healthLead{m: dm, hopKind: "detour", reason: "health:ramp"}
			}
		}
	}
	if boundOK || (!h.epOpenedAt.IsZero() && !bindAt.IsZero() && bindAt.After(h.epOpenedAt)) {
		// PR3/PR5: a healthy-enough binding — or one the brain re-pointed
		// AFTER the episode opened (thrash-8: the move was deliberate) — is
		// served. If the bound key itself is the half-open one, the hop
		// adopts the probe role on its way through.
		var l *probeLease
		if tn.memberDialable(bound, nowFn()) == "" {
			l = tn.health.adoptProbe(bound.ID, class, bound.ID, reqID, est)
		}
		hk, rr := "bound", ""
		if l != nil {
			hk, rr = "probe", "health:probe"
		}
		return healthLead{m: bound, hopKind: hk, reason: rr, lease: l}
	}
	// PR4 — the episode election: every hop of this class rides ONE detour
	// (I1). A seated member that has itself degraded is passed over — the
	// ranked serve below takes charge without re-seating (the election CAS
	// keeps the seat; a churning seat would thrash every session).
	if em, ok := tn.health.electionFor(bound.ID, class); ok {
		if dm, has := memberByID(members, em); has && tn.memberDialable(dm, nowFn()) == "" &&
			!(wireOpenAI && dm.Kind == "anthropic") && dm.Model != "" {
			if ss := tn.health.stateOf(dm.ID); ss != hsOpen {
				var l *probeLease
				if ss == hsHalfOpen {
					l = tn.health.adoptProbe(dm.ID, class, dm.ID, reqID, est)
				}
				tn.health.noteSessionDetour(bound.ID, class, sess)
				hk := "detour"
				if l != nil {
					hk = "probe"
				}
				return healthLead{m: dm, hopKind: hk, reason: "health:detour", lease: l}
			}
		}
	}
	if len(cand) == 0 {
		// PR8/PR9 emptied the list — today's typed 503 shape, never a 529
		// naming nobody (§5.4 is for members that exist and are open).
		return healthLead{refuse: "no member of class " + class + " is admissible (bound: " + bound.ID + ")"}
	}
	if len(cand) == 1 {
		// R-27 beats R-26: a single admissible member rides its own state.
		m := cand[0].m
		var l *probeLease
		if cand[0].tier != tierReadmit && cand[0].health == hsHalfOpen {
			l = tn.health.adoptProbe(m.ID, class, m.ID, reqID, est)
		}
		if m.ID != bound.ID {
			tn.health.noteSessionDetour(bound.ID, class, sess)
		}
		return healthLead{m: m, hopKind: "forced", reason: "health:forced-single", lease: l}
	}
	// Stage 2 still tries ranked members when synthetic all-degraded refusals
	// are disabled; a stale OPEN verdict must not become a client-only outage.
	if allOpenCand(cand) && healthAllDegradedEnabled {
		return healthLead{exhaust: true}
	}
	pick := cand[0].m
	if seated, created := tn.health.elect(bound.ID, class, pick.ID); !created {
		// A concurrent hop seated its member between our electionFor read and
		// this elect — ride the seat (I1) when it is still in the snapshot.
		if dm, has := memberByID(members, seated); has {
			pick = dm
		}
	}
	var l *probeLease
	if tn.health.stateOf(pick.ID) == hsHalfOpen {
		l = tn.health.adoptProbe(pick.ID, class, pick.ID, reqID, est)
	}
	if pick.ID != bound.ID {
		tn.health.noteSessionDetour(bound.ID, class, sess)
	}
	hk := "ranked"
	if l != nil {
		hk = "probe"
	}
	return healthLead{m: pick, hopKind: hk, reason: "health:ranked", lease: l}
}

// exhaustOutcome carries §5.2's allOpen verdict to §5.4: an actuation-free
// classOutcome whose classCtx is flagged healthExhaust — classRoundTrip
// answers the synthetic 529 before any dial, so it flows out of RoundTrip and
// through the response tap like any upstream reply (§5.4 property 1).
func (tn *tenant) exhaustOutcome(ce *registryEntry, class, sess string, bound classMemberPref,
	members []classMemberPref, q classReq, hdr http.Header, reqID string) classOutcome {
	sc := newScaleMark("", 0)
	sc.resolveNone()
	info := reqInfo{tenant: tn, eff: bound.ID, body: q.origBody}
	cc := &classRoute{classReq: q, tenant: tn, class: class, session: sess, member: bound,
		members: members, origHeader: hdr.Clone(),
		served: newServedHop(class, bound, info, bound.ID, ""), scale: sc,
		reqID: reqID, hopKind: "none", routeReason: "health:exhausted",
		healthExhaust: true}
	cc.served.attempts = 0 // no upstream rung was attempted
	info.classCtx = cc
	info.scale = sc
	return classOutcome{info: info, eff: ce.renderedID(),
		tag: " CLASS[" + class + "] HEALTH-EXHAUSTED sess=" + fpSess(sess) + " (every admissible member is open)"}
}

// unavailableOutcome is exhaustOutcome's twin on the quota axis: it builds a
// classCtx whose only job is to carry the unavailable reply (§2.8) out
// through RoundTrip, so the response tap, the transcript and the status page
// all see it exactly like an upstream reply — no dial, no actuation, no member
// touched. bound stays the nominal member (none, when the brain bound nothing)
// so the served-hop record names the binding the brain actually chose.
func (tn *tenant) unavailableOutcome(ce *registryEntry, class, sess string, bound classMemberPref,
	members []classMemberPref, q classReq, hdr http.Header, reqID string, u *unavailable) classOutcome {
	sc := newScaleMark("", 0)
	sc.resolveNone()
	info := reqInfo{tenant: tn, eff: bound.ID, body: q.origBody}
	cc := &classRoute{classReq: q, tenant: tn, class: class, session: sess, member: bound,
		members: members, origHeader: hdr.Clone(),
		served: newServedHop(class, bound, info, bound.ID, ""), scale: sc,
		reqID: reqID, hopKind: "none", routeReason: "quota:unavailable",
		unavail: u, boundMember: bound.ID}
	info.classCtx = cc
	info.scale = sc
	at := "none"
	if u.timed() {
		at = time.Unix(0, int64(*u.AvailableAt*1e9)).UTC().Format(time.RFC3339) + " " + strOrEmpty(u.AvailableBasis)
	}
	return classOutcome{info: info, eff: ce.renderedID(),
		tag: " CLASS[" + class + "] UNAVAILABLE sess=" + fpSess(sess) + " (back " + at + ")"}
}

// healthReqIDOf is the request's decision id when the handler made one, else
// a fresh one (ring rule 0's dedup key).
func (tn *tenant) healthReqIDOf(decisionID []string) string {
	if len(decisionID) > 0 {
		return decisionID[0]
	}
	return tn.healthReqID()
}

// healthWalk is §5.3's health detour: the serving hop failed AT THE HEAD
// (ladder=detour) under enforce. Admissibility ≤1 rides the failure untouched
// (E13, R-27); a pinned hop is never walked — §5.6's same-member budget is
// zero, so the pin goes straight to §5.4's pinned 529 (BL-7); otherwise the
// remaining candidates are walked with quota marks and alerts OFF, and
// exhaustion mints §5.4's 529 rather than handing back the last refusal.
func (t *failoverTransport) healthWalk(req *http.Request, info reqInfo, resp *http.Response,
	origErr error, cause string, afterQuota bool) (*http.Response, error) {
	cc := info.classCtx
	tn := cc.tenant
	est := estInputTokens(cc.origBody)
	cand := tn.rankCandidates(cc.members, "", rankOpt{est: est, wire: cc.wireOpenAI, health: true, ctx: req.Context()})
	if len(cand) <= 1 {
		return resp, origErr // sole admissible (or none): ride the failure as-is
	}
	if cc.pinned {
		if resp != nil {
			resp.Body.Close()
		}
		return tn.degradedExhaustResponse(cc, true), nil
	}
	servingID := cc.served.snapshot().member
	rungs := make([]ranked, 0, len(cand))
	for _, rc := range cand {
		if rc.m.ID != servingID {
			rungs = append(rungs, rc)
		}
	}
	if len(rungs) == 0 {
		return resp, origErr
	}
	if cause == "" {
		cause = "head failure"
	}
	walked := "health"
	if afterQuota {
		walked = "both"
	}
	before := cc.served.snapshot().attempts
	if tn.health != nil {
		cc.healthNote = captureNoteWalk(servingID, resp)
	}
	wresp, werr, exhausted := t.walkRungs(req, info, resp, refusal{}, walkOpt{
		cause:      "health: " + cause + " — mid-request member failover",
		candidates: rungs, health: true, skipSwaps: afterQuota})
	if cc.served.snapshot().attempts > before {
		cc.walked = walked
	}
	// With the rollout gate off, preserve walkRungs' final upstream failure.
	// The pinned branch above deliberately remains unconditional.
	if exhausted && healthAllDegradedEnabled {
		if wresp != nil {
			wresp.Body.Close()
		}
		return tn.degradedExhaustResponse(cc, false), nil
	}
	return wresp, werr
}

// degradedDetail names the class's degraded keys for §5.4's message —
// "gpt/gpt-5.6-sol open 4m0s, kimi/kimi-k3 open 40s".
func (tn *tenant) degradedDetail(members []classMemberPref) string {
	if tn.health == nil {
		return ""
	}
	ids := make([]string, 0, len(members))
	byID := make(map[string]classMemberPref, len(members))
	for _, m := range members {
		ids = append(ids, m.ID)
		byID[m.ID] = m
	}
	var parts []string
	for _, kd := range tn.health.describeKeys(ids) {
		if kd.state != hsOpen && kd.state != hsHalfOpen {
			continue
		}
		name := kd.id
		if m, ok := byID[kd.id]; ok && m.Model != "" {
			name += "/" + m.Model
		}
		parts = append(parts, name+" "+kd.state+" "+kd.since.Round(time.Second).String())
	}
	return strings.Join(parts, ", ")
}

// degradedExhaustResponse mints §5.4's synthetic 529: anthropic-shaped
// overloaded_error JSON, a jittered Retry-After, and the two X-Switch-*
// headers. It flags the classCtx so RoundTrip hands it out untouched —
// translate would replace the header map (destroying Retry-After) and the
// scaler would misread the body. 529 means health; 503 stays brain/decision.
func (tn *tenant) degradedExhaustResponse(cc *classRoute, pinned bool) *http.Response {
	cc.healthExhaust, cc.exhaustPinned = true, pinned
	// clamp(8, 5, 60) with ±20% jitter per response (R-48): jitterDur's FNV
	// spread over the reqID keeps concurrent retries from re-converging.
	retry := int(jitterDur(cc.reqID, 8*time.Second).Round(time.Second) / time.Second)
	if retry < 5 {
		retry = 5
	}
	if retry > 60 {
		retry = 60
	}
	var msg string
	if pinned {
		cond := "degraded"
		for _, kd := range tn.health.describeKeys([]string{cc.member.ID}) {
			if kd.state == hsOpen || kd.state == hsHalfOpen {
				cond = fmt.Sprintf("%s (%s, %d/%d failed)",
					kd.state, kd.since.Round(time.Second), kd.ringDeg, kd.ringTot)
			}
		}
		msg = fmt.Sprintf("class %s is pinned to %s, which is %s; the pin is honoured — unpin with GET :9115/class-pin?class=%s&member=auto, or retry in %ds",
			cc.class, cc.member.ID, cond, cc.class, retry)
	} else {
		msg = "every member of class " + cc.class + " is degraded"
		if detail := tn.degradedDetail(cc.members); detail != "" {
			msg += " (" + detail + ")"
		}
		msg += fmt.Sprintf("; retry in %ds", retry)
	}
	body, _ := json.Marshal(map[string]any{
		"type":  "error",
		"error": map[string]any{"type": "overloaded_error", "message": msg},
	})
	hv := "exhausted"
	if pinned {
		hv = "pinned-exhausted"
	}
	hd := http.Header{}
	hd.Set("Content-Type", "application/json")
	hd.Set("Retry-After", strconv.Itoa(retry))
	hd.Set("X-Switch-Health", hv)
	if cc.reqID != "" {
		hd.Set("X-Switch-Decision", cc.reqID)
	}
	log.Printf("🩺 class %s HEALTH-EXHAUSTED (%s) — synthetic 529, retry in %ds", cc.class, hv, retry)
	return &http.Response{
		StatusCode: 529, Status: "529 Overloaded",
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: hd, Body: io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

// healthNoteWouldAct scores §5.5's would-act clauses at request completion —
// under observe AND enforce (N24), so the counter reads the same across the
// rollout: a walk crossed, an exhaust fired, route-time acted (or would
// have), or the request was served on a key the FSM holds degraded.
func (tn *tenant) healthNoteWouldAct(cc *classRoute) {
	if tn.health == nil || cc == nil {
		return
	}
	act := cc.walked != "" || cc.healthExhaust ||
		cc.hopKind == "detour" || cc.hopKind == "ranked" || cc.hopKind == "forced" || cc.hopKind == "probe" ||
		cc.routeReason == "health:ramp"
	if !act {
		switch tn.health.stateOf(cc.served.snapshot().member) {
		case "", hsHealthy, hsConvalescent:
		default:
			act = true
		}
	}
	if act {
		tn.health.noteWouldAct(cc.class)
	}
	if cc.trace == nil {
		tn.health.noteRouteDecision(cc.class, cc.hopKind, cc.routeReason)
	}
	if cc.healthExhaust {
		tn.health.noteExhausted(cc.class, cc.exhaustPinned)
	}
}

// sessionFunding is how a session's last class request ended: served on a
// funding tier (Tier set), or refused with the class unavailable (Unavailable
// set; Until zero when no reading says when it is back).
type sessionFunding struct {
	Tier        string
	Unavailable bool
	Until       time.Time
	at          time.Time // when it was written
}

// noteSessionFunding records how a session's last class request ended, bound
// or not: every 2xx a class serves (noteServedHop) and every unavailable
// reply (unavailableResponse) writes it, and the next one replaces it.
// count_tokens writes nothing: it is free and asks entitlement only (§2.2 row
// 20), so it says nothing of what the session's requests are funded by. A
// session idle past the brain's binding lifetime is dropped here.
func (tn *tenant) noteSessionFunding(cc *classRoute, f sessionFunding) {
	if cc == nil || cc.session == "" || strings.HasSuffix(cc.inPath, "/count_tokens") {
		return
	}
	if f.Unavailable {
		tn.noteSessionUnavailable(cc.class, cc.session, f.Until)
	}
	now := nowFn()
	f.at = now
	tn.bindMu.Lock()
	tn.sessionFundings[bindKey(cc.class, cc.session)] = f
	for k, o := range tn.sessionFundings {
		if now.Sub(o.at) > fundingIdle {
			delete(tn.sessionFundings, k)
		}
	}
	tn.bindMu.Unlock()
}

// json is a session's funding as /_switch/status class_funding shows it.
func (f sessionFunding) json() map[string]any {
	switch {
	case f.Unavailable && f.Until.IsZero():
		return map[string]any{"unavailable": true, "until": nil}
	case f.Unavailable:
		return map[string]any{"unavailable": true, "until": f.Until.UTC().Format(time.RFC3339)}
	case f.Tier != "":
		return map[string]any{"tier": f.Tier}
	}
	return nil
}
