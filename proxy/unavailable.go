package main

// ---- the unavailable reply (quota-model 50 §2.8, C3) ----
//
// One reply for a class (or a bare lane) none of whose routes has a check that
// is `included` or `paid`, wherever that is found: the brain's 409
// class_unavailable at bind, this proxy's pre-flight over its own checks, or a
// walk that ran out of rungs. Its body is the same in all three:
//
//   - members[]: every member's route and each account's check, until, basis,
//     why and the window the until is the reset of;
//   - available_at / available_basis: the earliest `until` that is a time —
//     a window's reset (exact, or early_possible on codex), a refusal
//     record's end (record), or the next poll of an account only a reading
//     can tell about (next_poll);
//   - retry_after_s: the whole seconds to it;
//   - retryable: whether anything but money or the owner could make it serve;
//   - options[]: the owner actions that would serve sooner.
//
// `no (until paid)`, a `no` with no known time and one whose time has already
// passed (stated as null: the brain's next poll, when the brain has been down
// past stale_max) contribute no time. Retryable is by meaning, not by whether
// a time is known: the reply is the non-retryable money reply only when every
// account is in a known state no time ends (`until paid`). Any other `no`, a
// `no (unknown)` above all, may serve again with no one acting (the brain's
// next reading), so it stays retryable even when no time is known. A reply
// that knows no time never states one: no Retry-After, no minted unified
// reset (Retry-After: 1 would be a guess, and a past reset a lie), and
// X-Switch-Available-At: unknown. unavailableBody is golden-tested against
// the brain's unavailable_body (testdata/dialcheck_golden.json, `unavailable`).
//
// The body is rendered by wire (renderUnavailable, the §2.8 table):
//
//	                  retryable, a time      retryable, no time    non-retryable
//	Claude Code       429 + minted reset     429 rate_limit_error  402 billing_error
//	other Anthropic   529 + Retry-After      529 overloaded_error  402 billing_error
//	OpenAI wire       429 rate_limit + R-A   429 rate_limit        429 insufficient_quota
//
// With no time, each wire's client backs off by its own schedule.
//
// The typed 503 "not routable" stays for configuration errors and blindness
// (an unknown class, a member this proxy cannot route at all, no brain and no
// cache).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// unavailAccount is one account of a member's route, as the reply lists it.
type unavailAccount struct {
	Account string     `json:"account"`
	Check   string     `json:"check"` // included | paid:<pot> | no
	Why     *string    `json:"why"`   // the check's reason; null unless `no`
	Until   checkUntil `json:"until"`
	Basis   string     `json:"basis"`
	Window  *string    `json:"window"` // the binding window whose reset is the until
	// Detail is this proxy's alone: the upstream's own words from the live
	// spent record that holds the account out longest ("403 You've reached
	// your 5-hour usage limit"), for the operator reading the message.
	Detail string `json:"detail,omitempty"`
	// Plan is this proxy's alone: the account's plan label from /preferred
	// ("free"), beside a `no (blocked)`, which the plan explains.
	Plan string `json:"plan,omitempty"`
	// provider is the account's provider, for the words of a blocked check.
	provider string
}

// whyBlocked is the reason of a `no (blocked)` check (§1.6 step 5, §2.8): the
// provider refuses the model on this plan (a not_entitled record). It gives
// no time and no owner option, and no money is dialled behind it.
const whyBlocked = "blocked"

// blockedWords says why a blocked account can't serve, for the provider it
// is on, with the plan's label where one is known.
func blockedWords(provider, plan string) string {
	s := "the provider refuses this model on this account's plan"
	if provider == "gpt" || provider == "codex" {
		s = "OpenAI refuses this model on this ChatGPT plan"
	}
	if plan != "" {
		s += " (" + plan + ")"
	}
	return s
}

// unavailMember is one member's route. Unroutable and CannotServe are this
// proxy's alone, listed so the reply names every member: a member it cannot
// dial at all (its provider is not configured here, or the brain serves no
// such route), and a member that can never take this request (§2.9, a size
// off Runway's ratios, a mask), with the field and what it would draw.
// Neither has accounts, so neither gives a time or an option.
type unavailMember struct {
	Route       string           `json:"route"`
	Accounts    []unavailAccount `json:"accounts"`
	Unroutable  string           `json:"unroutable,omitempty"`
	CannotServe *cantServe       `json:"cannot_serve,omitempty"`
}

// unavailOption is one owner action that would serve sooner (§2.8 item 1).
type unavailOption struct {
	Kind    string  `json:"kind"` // permit | enable | fund | reset
	Route   string  `json:"route"`
	Account string  `json:"account"`
	Pot     *string `json:"pot"`
	Action  string  `json:"action"`
}

// unavailable is the §2.8 body: the brain's 409 and this proxy's own.
type unavailable struct {
	tenant         *tenant         // whose refusal: its router-dry clause reads its classes
	Members        []unavailMember `json:"members"`
	AvailableAt    *float64        `json:"available_at"`
	AvailableBasis *string         `json:"available_basis"`
	RetryAfterS    *int            `json:"retry_after_s"`
	Retryable      *bool           `json:"retryable"`
	Options        []unavailOption `json:"options"`
}

// retryable: anything but money or the owner could make it serve. Required
// on the brain's 409 (bindClassMember refuses a body without it), so nil is
// never read here as the money reply.
func (u *unavailable) retryable() bool { return u.Retryable != nil && *u.Retryable }

// timed: the reply knows when it is back (a time still to come).
func (u *unavailable) timed() bool { return u.AvailableAt != nil && u.RetryAfterS != nil }

// unavailRoute is one member route the body is built over, in member order.
type unavailRoute struct {
	policy      routePolicy
	accounts    []string
	unroutable  string
	cannotServe *cantServe
	// req is the request its checks are for (the member's, classMemberPref
	// req); zero for none, the brain's own check.
	req checkRequest
}

func checkLabel(c dialCheck) string {
	if c.paid() {
		return checkPaid + ":" + c.Pot
	}
	return c.Check
}

// untilWindow is the binding window whose reset is this until (the first by
// name), "" for none: the limit the reply says comes back then.
func untilWindow(st *acctStanding, scope string, u checkUntil) string {
	if st == nil || !u.known() {
		return ""
	}
	names := make([]string, 0, len(st.Allowance.Windows))
	for n := range st.Allowance.Windows {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		w := st.Allowance.Windows[n]
		binds := w.Scope == nil || (scope != "" && strings.EqualFold(*w.Scope, scope))
		if binds && w.ResetsAt != nil && *w.ResetsAt == u.At {
			return n
		}
	}
	return ""
}

// untilBasis is how firm a contributing time is: the window's reset_basis;
// exact for a budget's period end; next_poll when it is the account's next
// poll; derived for an empty balance's hourly refill under topup on (§1.9);
// record for any other time (this proxy's refusal records).
func untilBasis(st *acctStanding, window string, u checkUntil, why string, now float64) string {
	if window != "" {
		if b := st.Allowance.Windows[window].ResetBasis; b != "" {
			return b
		}
		return "exact"
	}
	if _, end := budgetPeriod(now); why == "budget" && u.At == end {
		return "exact"
	}
	if st != nil && st.NextPollAt != nil && *st.NextPollAt == u.At {
		return "next_poll"
	}
	if why == "balance" && st != nil && st.AsOf != nil && u.At == *st.AsOf+topupRefillS {
		return "derived"
	}
	return "record"
}

// potOf is the account's money pot as the dial check reads it (the first by
// name), "" for none.
func potOf(st *acctStanding, account string) (string, *moneyPot) {
	if st == nil || len(st.Money) == 0 {
		return "", nil
	}
	names := make([]string, 0, len(st.Money))
	for n := range st.Money {
		names = append(names, n)
	}
	sort.Strings(names)
	p := st.Money[names[0]]
	return account + "/" + names[0], &p
}

// replyOptions are the owner actions that would make this (route, account)
// serve sooner: its money pot, where permitting, funding or enabling it would
// serve, raising each budget the route has reached on it (§1.9), and the
// allowance's reset options while the allowance holds it. req is the request
// checked for. Mirrors the brain's _reply_options.
func replyOptions(r routePolicy, account string, st *acctStanding, c dialCheck, pots map[string]potSettings, l *checkLedger, now float64, req checkRequest) []unavailOption {
	if !c.no() {
		return nil
	}
	var out []unavailOption
	pid, pot := potOf(st, account)
	listed := pid != "" && containsStr(r.Pay, pid)
	if pot != nil && (c.Reason == "pay" || c.Reason == "unknown") && pot.Open != nil && *pot.Open && !listed {
		p := pid
		out = append(out, unavailOption{Kind: "permit", Route: r.ID, Account: account, Pot: &p,
			Action: "add " + pid + " to " + r.ID + "'s pay"})
	}
	if pot != nil && (c.Reason == "balance" ||
		((c.Reason == "spent" || c.Reason == "pay") && pot.Open != nil && !*pot.Open)) {
		// An empty pot, or a balance below this request's cost: money would
		// serve, and where the route may not draw the pot, the permit too.
		then := ""
		if !listed {
			then = ", and add " + pid + " to " + r.ID + "'s pay"
		}
		p := pid
		if st.Provider == "anthropic" {
			out = append(out, unavailOption{Kind: "enable", Route: r.ID, Account: account, Pot: &p,
				Action: "enable usage credits on " + account + then})
		} else {
			out = append(out, unavailOption{Kind: "fund", Route: r.ID, Account: account, Pot: &p,
				Action: "add money to " + pid + then})
		}
	}
	if c.Reason == "budget" {
		for _, which := range budgetsReached(r, pid, pots[pid], l, now, req) {
			p, action := pid, "raise "+pid+"'s budget"
			if which == "route" {
				action = "raise " + r.ID + "'s route budget on " + pid
			}
			out = append(out, unavailOption{Kind: "budget", Route: r.ID, Account: account, Pot: &p, Action: action})
		}
	}
	if (c.Reason == "pay" || c.Reason == "spent" || c.Reason == "budget") && st != nil {
		if ro := st.Allowance.ResetOptions; ro != nil {
			switch {
			case ro.ResetCredits != nil && *ro.ResetCredits > 0:
				out = append(out, unavailOption{Kind: "reset", Route: r.ID, Account: account,
					Action: "redeem one of " + strconv.FormatFloat(*ro.ResetCredits, 'f', -1, 64) +
						" allowance reset credits on " + account})
			case ro.PurchaseEligible != nil && *ro.PurchaseEligible:
				out = append(out, unavailOption{Kind: "reset", Route: r.ID, Account: account,
					Action: "buy an immediate allowance reset on " + account})
			}
		}
	}
	return out
}

// unavailableBody builds the §2.8 body from the checks: checks and standings
// are keyed route id, then account (a standing is per route here: a model
// record binds one route's dial); pots and l are what the checks read.
// Mirrors the brain's unavailable_body. nil
// when no route has an account: nothing has a check, so no time or owner
// action would serve, and that is configuration, the caller's typed 503
// (§2.8 item 6).
func unavailableBody(routes []unavailRoute, checks map[string]map[string]dialCheck, standings map[string]map[string]*acctStanding, now float64, pots map[string]potSettings, l *checkLedger) *unavailable {
	accounts := 0
	for _, rt := range routes {
		accounts += len(rt.accounts)
	}
	if accounts == 0 {
		return nil
	}
	u := &unavailable{Members: []unavailMember{}, Options: []unavailOption{}}
	retryable := false
	for _, rt := range routes {
		m := unavailMember{Route: rt.policy.ID, Accounts: []unavailAccount{}, Unroutable: rt.unroutable,
			CannotServe: rt.cannotServe}
		for _, a := range rt.accounts {
			c := checks[rt.policy.ID][a]
			st := standings[rt.policy.ID][a]
			ua := unavailAccount{Account: a, Check: checkLabel(c), Basis: c.Basis}
			if c.no() {
				why := c.Reason
				ua.Why, ua.Until = &why, c.Until
				if ua.Until.known() && ua.Until.At <= now {
					// A time already past (the next poll of a brain that
					// answered long ago) is no time the reply can state: unknown.
					ua.Until = checkUntil{}
				}
			}
			if w := untilWindow(st, rt.policy.Scope, ua.Until); w != "" {
				ua.Window = &w
			}
			m.Accounts = append(m.Accounts, ua)
			// Blocked waits on the owner as until_paid does: no time, and
			// not retryable (§2.8 item 2).
			retryable = retryable || (!ua.Until.Paid && c.Reason != whyBlocked)
			if ua.Until.known() && (u.AvailableAt == nil || ua.Until.At < *u.AvailableAt) {
				at, basis := ua.Until.At, untilBasis(st, strOrEmpty(ua.Window), ua.Until, c.Reason, now)
				u.AvailableAt, u.AvailableBasis = &at, &basis
			}
			u.Options = append(u.Options, replyOptions(rt.policy, a, st, c, pots, l, now, rt.req)...)
		}
		u.Members = append(u.Members, m)
	}
	if u.AvailableAt != nil {
		secs := int(math.Ceil(*u.AvailableAt - now)) // AvailableAt > now, so at least 1
		u.RetryAfterS = &secs
	}
	u.Retryable = &retryable
	return u
}

func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// ---- this proxy's own body, from its checks ----

// memberUnroutable is why this proxy cannot dial a member at all, "" when it
// can: a configuration fact, not a quota one. ctx is the asking request's.
func (tn *tenant) memberUnroutable(ctx context.Context, m classMemberPref) string {
	if m.Kind == "provider" && providerByName(m.Provider) == nil {
		return "provider " + m.Provider + " is not configured here"
	}
	if r, _, _, ok := tn.routeOf(m.Route); !ok || len(r.Accounts) == 0 {
		return "the brain serves no route " + m.Route
	}
	return tn.memberKeyMissing(ctx, m)
}

// keyedWire is a wire that sends an account's key the brain dispenses
// (apikey.go). keyMissing is why the account has none, "" when it has one.
type keyedWire interface {
	keyMissing(ctx context.Context, account string) string
}

// memberKeyMissing is why a keyed wire's member has no key on any account of
// its route ("" = one has a key, or the wire sends none of its own): it is
// then unusable, with that reason, and nothing is recorded. The wire is asked
// on tn's behalf (ctxOf): the key it looks for is tn's dispense of tn's
// account, whatever ctx carried — a walk with no request context (the
// prober's, the status page's) is never judged against another tenant.
func (tn *tenant) memberKeyMissing(ctx context.Context, m classMemberPref) string {
	ctx = tn.ctxOf(ctx)
	p := providerByName(m.Provider)
	if m.Kind != "provider" || p == nil {
		return ""
	}
	kw, ok := p.wire.(keyedWire)
	if !ok {
		return ""
	}
	r, _, _, ok := tn.routeOf(m.Route)
	if !ok {
		return ""
	}
	why := ""
	for _, a := range r.Accounts {
		if why = kw.keyMissing(ctx, a); why == "" {
			return ""
		}
	}
	return why
}

// classUnavailable is the pre-flight's and the walk's question (§2.8 item 7,
// §2.2 row 5): does no member this wire can use have an account whose check
// admits a dial? Then it is the body; nil when a member would get a real
// attempt, and nil when no member is routable here at all (an empty universe,
// or every member unroutable, so no route has an account): that is a
// configuration error, and the typed 503 is the caller's.
func (tn *tenant) classUnavailable(members []classMemberPref, q classReq, now time.Time) *unavailable {
	routes, routable := tn.classRoutes(members, q, now, true)
	if routable {
		return nil
	}
	return tn.routesUnavailable(routes, modelsOf(members), now)
}

// classRoutes is every member's route as the reply lists it, in member
// order: an unroutable one with why, the rest with their accounts. A member
// that can never take q (capability before money, §2.9) gives no verdict:
// for an image it is listed with its cannot_serve and no accounts, so the
// reply names every member and why; chat's D2 skip (an anthropic member on
// the OpenAI wire) is left out, as a member that wire never uses. With stop,
// it stops at the first member that would get a real attempt now and says
// so (routable).
func (tn *tenant) classRoutes(members []classMemberPref, q classReq, now time.Time, stop bool) (routes []unavailRoute, routable bool) {
	kind := q.kind()
	for _, m := range members {
		if cs := kind.canServe(m, q); cs != nil {
			if q.image != nil {
				routes = append(routes, unavailRoute{policy: routePolicy{ID: m.Route}, cannotServe: cs})
			}
			continue
		}
		if why := tn.memberUnroutable(q.context(), m); why != "" {
			routes = append(routes, unavailRoute{policy: routePolicy{ID: m.Route}, unroutable: why})
			continue
		}
		if stop && tn.memberRoutableNow(m, now) {
			return nil, true
		}
		r, _, _, _ := tn.routeOf(m.Route)
		routes = append(routes, unavailRoute{policy: r.routePolicy, accounts: r.Accounts, req: m.req})
	}
	return routes, false
}

// classRefusal is the §2.8 body of every member of the class for q, in
// member order, for a reply that is not the unavailable one (the image
// door's 400): built even when no route has an account (every member
// skipped for capability or unroutable), which the unavailable reply leaves
// to the typed 503.
func (tn *tenant) classRefusal(members []classMemberPref, q classReq, now time.Time) *unavailable {
	routes, _ := tn.classRoutes(members, q, now, false)
	if q.image != nil {
		q.image.refusalRoutes(members, q, routes, now)
	}
	if u := tn.routesUnavailable(routes, modelsOf(members), now); u != nil {
		return u
	}
	f := false
	u := &unavailable{tenant: tn, Members: []unavailMember{}, Options: []unavailOption{}, Retryable: &f}
	for _, rt := range routes {
		u.Members = append(u.Members, unavailMember{Route: rt.policy.ID, Accounts: []unavailAccount{},
			Unroutable: rt.unroutable, CannotServe: rt.cannotServe})
	}
	return u
}

// modelsOf maps each member route to the upstream model it dials, for the
// records that name one model.
func modelsOf(members []classMemberPref) map[string]string {
	out := make(map[string]string, len(members))
	for _, m := range members {
		out[m.Route] = m.Model
	}
	return out
}

// routesUnavailable asks every account of every route its check, on the
// standing that check read, and builds the body (nil with no account at all).
func (tn *tenant) routesUnavailable(routes []unavailRoute, models map[string]string, now time.Time) *unavailable {
	checks := map[string]map[string]dialCheck{}
	standings := map[string]map[string]*acctStanding{}
	for _, rt := range routes {
		checks[rt.policy.ID], standings[rt.policy.ID] = map[string]dialCheck{}, map[string]*acctStanding{}
		for _, a := range rt.accounts {
			c, st, _ := tn.routeCheckStanding(rt.policy.ID, a, models[rt.policy.ID], rt.req, now)
			checks[rt.policy.ID][a], standings[rt.policy.ID][a] = c, st
		}
	}
	tn.pref.mu.RLock()
	pots := tn.pref.pots
	tn.pref.mu.RUnlock()
	u := unavailableBody(routes, checks, standings, unixSeconds(now), pots, tn.ledgerView(unixSeconds(now)))
	if u == nil {
		return nil
	}
	u.tenant = tn
	for i := range u.Members {
		for j := range u.Members[i].Accounts {
			a := &u.Members[i].Accounts[j]
			if a.Why == nil {
				continue
			}
			if rec, ok := tn.spentRecordOf(a.Account, now); ok && rec.detail != "" {
				a.Detail = strconv.Itoa(rec.status) + " " + rec.detail
			}
			if st := standings[u.Members[i].Route][a.Account]; st != nil {
				a.provider = st.Provider
				if *a.Why == whyBlocked {
					a.Plan = st.planLabel()
				}
			}
		}
	}
	return u
}

// laneUnavailable is a bare lane's body: one member, the lane's route.
func (tn *tenant) laneUnavailable(lane, model string, now time.Time) *unavailable {
	id := laneRouteID(lane)
	r, _, _, _ := tn.routeOf(id)
	return tn.routesUnavailable([]unavailRoute{{policy: r.routePolicy, accounts: r.Accounts}},
		map[string]string{id: model}, now)
}

// ---- rendering by wire ----

// replyWire is which §2.8 row a client gets.
type replyWire int

const (
	replyAnthropic  replyWire = iota // an Anthropic-wire client other than Claude Code (the agent gateway)
	replyClaudeCode                  // the Claude Code CLI, which reads the unified headers
	replyOpenAI                      // the OpenAI wire (chat completions, images)
)

func (w replyWire) String() string {
	switch w {
	case replyClaudeCode:
		return "claude-code"
	case replyOpenAI:
		return "openai"
	}
	return "anthropic"
}

func replyWireFor(h http.Header, wireOpenAI bool) replyWire {
	switch {
	case wireOpenAI:
		return replyOpenAI
	case isClaudeCodeClient(h):
		return replyClaudeCode
	}
	return replyAnthropic
}

// isClaudeCodeClient: the UA every Claude Code build sends, base-URL and
// HTTPS-proxy mode alike (the entry leaves the UA untouched).
func isClaudeCodeClient(h http.Header) bool {
	return h != nil && strings.HasPrefix(h.Get("User-Agent"), "claude-cli/")
}

// fmtAvail renders an instant for a human reading a terminal: the clock alone
// when it is today (UTC), the date as well otherwise.
func fmtAvail(t, now time.Time) string {
	t, now = t.UTC(), now.UTC()
	if t.Year() == now.Year() && t.YearDay() == now.YearDay() {
		return t.Format("15:04Z")
	}
	return t.Format("Mon Jan 2 15:04Z")
}

func epochTime(s float64) time.Time { return time.Unix(0, int64(s*1e9)) }

// basisWords says how firm a time is, for the message.
func basisWords(b string) string {
	switch b {
	case "early_possible":
		return "by then; it has come early before"
	case "next_poll":
		return "the next reading, which may say more"
	case "record":
		return "when a refusal's record ends"
	}
	return b
}

func (a unavailAccount) render(now time.Time) string {
	if a.Why == nil {
		return a.Account + " " + a.Check
	}
	if *a.Why == whyBlocked {
		// No time and no option: the plan refuses the model (§2.8, D5).
		s := a.Account + " blocked: " + blockedWords(a.provider, a.Plan)
		if a.Detail != "" {
			s += " (" + a.Detail + ")"
		}
		return s
	}
	s := a.Account + " " + *a.Why
	if a.Window != nil {
		s += " (" + windowLabel(*a.Window) + ")"
	}
	switch {
	case a.Until.known():
		s += " until " + fmtAvail(a.Until.Time(), now)
	case a.Until.Paid:
		s += " until paid"
	}
	if a.Basis != "fresh" {
		s += ", " + a.Basis + " reading"
	}
	if a.Detail != "" {
		s += " (" + a.Detail + ")"
	}
	return s
}

// allBlocked reports whether the reply has accounts and every one is
// blocked: money can't serve it, only the owner (a plan that offers the
// model).
func (u *unavailable) allBlocked() bool {
	n := 0
	for _, m := range u.Members {
		for _, a := range m.Accounts {
			if a.Why == nil || *a.Why != whyBlocked {
				return false
			}
			n++
		}
	}
	return n > 0
}

// message is the reply's one sentence: who is unavailable, when it is back and
// how firm that is, every member and account, and what the owner could do.
func (u *unavailable) message(subject string, now time.Time) string {
	var b strings.Builder
	b.WriteString(subject + " is unavailable. ")
	switch {
	case u.timed():
		b.WriteString("Back at " + fmtAvail(epochTime(*u.AvailableAt), now) +
			" (" + basisWords(strOrEmpty(u.AvailableBasis)) + ").")
	case u.retryable():
		b.WriteString("No time is known: it may serve again once a reading says so; retry later.")
	case u.allBlocked():
		b.WriteString("No time is known: the provider refuses the model on this plan, so only an owner action would serve it.")
	default:
		b.WriteString("No time is known: only money or an owner action would serve it.")
	}
	for _, m := range u.Members {
		b.WriteString(" " + m.Route + ": ")
		if m.Unroutable != "" {
			b.WriteString("not routable (" + m.Unroutable + ")")
		}
		if cs := m.CannotServe; cs != nil {
			b.WriteString("cannot draw this request (" + cs.String())
			if len(cs.Nearest) > 0 {
				b.WriteString("; it draws " + nearestWords(cs.Nearest))
			}
			b.WriteString(")")
		}
		parts := make([]string, 0, len(m.Accounts))
		for _, a := range m.Accounts {
			parts = append(parts, a.render(now))
		}
		b.WriteString(strings.Join(parts, "; ") + ".")
	}
	if len(u.Options) > 0 {
		acts := make([]string, 0, len(u.Options))
		for _, o := range u.Options {
			acts = append(acts, o.Action)
		}
		b.WriteString(" Options: " + strings.Join(acts, "; ") + ".")
	}
	return b.String() + u.tenant.routerDryClause()
}

// claim is the representative-claim Claude Code reads for the minted reset:
// the window of the account whose until is the class's time.
func (u *unavailable) claim() string {
	for _, m := range u.Members {
		for _, a := range m.Accounts {
			if a.Window != nil && a.Until.known() && u.AvailableAt != nil && a.Until.At == *u.AvailableAt {
				return claimOf(*a.Window)
			}
		}
	}
	return "seven_day"
}

// claimOf maps a window to the representative-claim vocabulary the CLI reads.
// A scoped cap is a weekly window.
func claimOf(name string) string {
	if name == "five_hour" {
		return "five_hour"
	}
	return "seven_day"
}

// renderUnavailable is the reply on the wire: status, headers and body, the
// §2.8 body riding beside the wire's own error envelope as "unavailable".
//
// Claude Code's retryable reply keeps the facts of the honest reply it had
// before C3 (CLI 2.1.289-2.1.308): a 429 counts as a usage limit only with
// representative-claim; unified-reset drives "resets <time>" and auto-resume;
// status=rejected sets the limited bar; overloaded_error in a 429 re-enables
// retries; overage-* would persist into the user's config or latch a purchase
// prompt (A-D11). So: claim + reset + status, rate_limit_error, and no
// overage-*, fallback* or x-should-retry. The other Anthropic-wire clients
// keep the 529 that escapes the agent gateway's 60 s same-model retry clamp. The
// money reply is 402 billing_error: Claude Code 2.1.314 sends the request
// once, does not retry, and prints "API Error: 402 <message>" (one run
// against a stub, §2.8's open question). The
// OpenAI money reply says x-should-retry: false, which the OpenAI SDKs obey
// (the codex image bridge's precedent).
func renderUnavailable(u *unavailable, subject, class, reqID string, rw replyWire, now time.Time) (status int, hd http.Header, body []byte, msg string) {
	msg = u.message(subject, now)
	hd = http.Header{}
	hd.Set("Content-Type", "application/json")
	hd.Set("X-Switch-Quota", "exhausted")
	if class != "" {
		hd.Set("X-Switch-Class", class)
	}
	if reqID != "" {
		hd.Set("X-Switch-Decision", reqID)
	}
	hd.Set("X-Switch-Available-At", "unknown")
	retry := u.retryable()
	if u.timed() {
		at := epochTime(*u.AvailableAt)
		hd.Set("X-Switch-Available-At", at.UTC().Format(time.RFC3339))
		hd.Set("Retry-After", strconv.Itoa(*u.RetryAfterS))
		if rw == replyClaudeCode {
			hd.Set("anthropic-ratelimit-unified-status", "rejected")
			hd.Set("anthropic-ratelimit-unified-reset", strconv.FormatInt(int64(math.Ceil(*u.AvailableAt)), 10))
			hd.Set("anthropic-ratelimit-unified-representative-claim", u.claim())
		}
	}
	var env map[string]any
	switch {
	case rw == replyOpenAI && retry:
		status = http.StatusTooManyRequests
		env = map[string]any{"error": map[string]any{"message": msg, "type": "requests", "param": nil, "code": "rate_limit_exceeded"}}
	case rw == replyOpenAI:
		status = http.StatusTooManyRequests
		hd.Set("x-should-retry", "false")
		env = map[string]any{"error": map[string]any{"message": msg, "type": "insufficient_quota", "param": nil, "code": "insufficient_quota"}}
	case !retry:
		status = http.StatusPaymentRequired
		env = map[string]any{"type": "error", "error": map[string]any{"type": "billing_error", "message": msg}}
	case rw == replyClaudeCode:
		status = http.StatusTooManyRequests
		env = map[string]any{"type": "error", "error": map[string]any{"type": "rate_limit_error", "message": msg}}
	default:
		status = 529
		env = map[string]any{"type": "error", "error": map[string]any{"type": "overloaded_error", "message": msg}}
	}
	env["unavailable"] = u
	body, _ = json.Marshal(env)
	return status, hd, body, msg
}

// unavailableResponse mints the reply for a classCtx carrying a body; it flows
// out of RoundTrip untouched (cc.unavail exempts it from translate, which
// would rebuild the header map).
func (tn *tenant) unavailableResponse(cc *classRoute) *http.Response {
	now := nowFn()
	rw := replyWireFor(cc.origHeader, cc.wireOpenAI)
	status, hd, body, msg := renderUnavailable(cc.unavail, "class "+cc.class, cc.class, cc.reqID, rw, now)
	// The statusline's "unavailable until" (§2.11): the time the reply gives,
	// or none when only money or an owner action serves.
	f := sessionFunding{Unavailable: true}
	if cc.unavail.retryable() && cc.unavail.timed() {
		f.Until = epochTime(*cc.unavail.AvailableAt)
	}
	tn.noteSessionFunding(cc, f)
	logUnavailable("class "+cc.class, status, rw, hd, msg)
	return &http.Response{
		StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: hd, Body: io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

func logUnavailable(subject string, status int, rw replyWire, hd http.Header, msg string) {
	log.Printf("🪫 %s UNAVAILABLE — %d to %s, available %s, retry %q: %s",
		subject, status, rw, hd.Get("X-Switch-Available-At"), hd.Get("Retry-After"), msg)
}

// writeLaneRefused answers a bare-lane request whose route admits no account
// (§2.2 row 11) with the lane's unavailable reply; nobody is dialled. A bare
// lane is the Anthropic wire.
func (tn *tenant) writeLaneRefused(w http.ResponseWriter, d *laneDecision, hdr http.Header, now time.Time) {
	lane := strings.TrimPrefix(d.refusedRoute, "lane:")
	u := tn.laneUnavailable(lane, d.model, now)
	if u == nil {
		// No account serves the lane at all: configuration, not quota.
		writeModelUnroutable(w, tn, "lane:"+lane, "the brain serves no account on lane "+lane, 0)
		return
	}
	rw := replyWireFor(hdr, false)
	status, hd, body, msg := renderUnavailable(u, "lane "+lane, "", "", rw, now)
	logUnavailable("lane "+lane, status, rw, hd, msg)
	for k, vs := range hd {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
