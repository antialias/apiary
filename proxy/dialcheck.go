package main

// ---- the dial check (quota-model 50 §1.6, C2) ----
//
// One function answers, for one route on one account: `included` (the
// allowance serves), `paid:<pot>` (a money pot the route's `pay` lists serves),
// or `no` with a reason and an `until`. The brain evaluates it per (route,
// account) for the pointer, the bind, the pin and session moves
// (claude_usage_exporter.py dial_check). This proxy evaluates it per request,
// on the brain's facts (/preferred accounts{}) merged with its own overlay
// (records.go), under the brain's policy (routes{}, pots{}). It never ANDs its
// answer with the brain's (62 m2) and never widens policy.
//
// The two evaluators are kept in agreement by one fixture set,
// testdata/dialcheck_golden.json, which dialcheck_test.go and the brain's
// test_exporter.py both run (§5.1). Any change here changes that file and the
// brain's dial_check together.

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
	"time"
)

// checkWindow is one allowance window of an account's standing.
type checkWindow struct {
	UsedPct  *float64 `json:"used_pct"`
	ResetsAt *float64 `json:"resets_at"`
	// AsOf is when this window was read: the brain's poll, or a response's
	// headers that carried it (§1.4: staleness is per limit).
	AsOf   *float64 `json:"as_of"`
	Period string   `json:"period"` // "5h" | "7d" | ...
	Scope  *string  `json:"scope"`  // nil = binds every route on the account
	// ResetBasis is how firm ResetsAt is (§1.5): exact; early_possible (codex
	// has ended periods early); record (a refusal record's end, this proxy's).
	ResetBasis string `json:"reset_basis,omitempty"`
	// Open is the window's own gate (a codex additional limit's allowed):
	// false shuts it whatever its percent says; nil where none is read.
	Open *bool `json:"open"`
	// Aliases are the names a scoped window's scope goes by (the brain's
	// SCOPE_ALIASES): the scope map the proxy matches a model name against.
	Aliases *windowAliases `json:"aliases"`
}

// resetOptions are an allowance's own ways to reset early (§1.3): the codex
// reset credits on hand, and whether an immediate reset may be bought. nil
// fields: no reading says. Only the unavailable reply's options read them.
type resetOptions struct {
	ResetCredits     *float64 `json:"reset_credits"`
	PurchaseEligible *bool    `json:"purchase_eligible"`
}

// moneyPot is one money pot's state. Open nil = no reading says (Kimi Extra
// Usage, an Anthropic account whose poll carried no extra_usage).
type moneyPot struct {
	Open    *bool    `json:"open"`
	Balance *float64 `json:"balance"`
	Unit    *string  `json:"unit"`
	// Limits are the pot's own limits other than its balance, keyed by id: an
	// api account's rolling count (Runway's daily generations per model).
	Limits map[string]potLimit `json:"limits"`
}

// potLimit is one of a pot's limits as the check reads it. A rolling_count
// counts Counts ("output": each output of a request) over a rolling window,
// for the model its Scope names (nil: every model on the pot); Used or Limit
// nil = no reading says, and an unknown count does not block (§1.4).
type potLimit struct {
	Kind   string   `json:"kind"`
	Scope  *string  `json:"scope"`
	Used   *float64 `json:"used"`
	Limit  *float64 `json:"limit"`
	Counts string   `json:"counts"`
}

// acctStanding is one /preferred accounts{} entry: the brain's latest reading
// of every limit on the account. AsOf nil = never measured.
type acctStanding struct {
	Provider string `json:"provider"`
	// Kind is the account's kind (§1.2): "subscription", or "api" (no
	// allowance; every dial draws its money pot, Runway's balance).
	Kind       string   `json:"kind"`
	AsOf       *float64 `json:"as_of"`
	NextPollAt *float64 `json:"next_poll_at"`
	// RecheckEpoch is the owner's last re-check of the account (POST
	// /recheck): every refusal record on it older than this ends (§2.6).
	RecheckEpoch *float64 `json:"recheck_epoch"`
	Allowance    struct {
		// Open is the provider's own gate (codex rate_limit.allowed); nil
		// where the windows alone decide (Anthropic, Kimi).
		Open         *bool                  `json:"open"`
		Windows      map[string]checkWindow `json:"windows"`
		ResetOptions *resetOptions          `json:"reset_options,omitempty"`
		// Entitlement is whether the plan includes the allowance at all:
		// active, or what a reading says otherwise. A not_entitled record
		// on the plan ends at a newer reading of it active (§2.6).
		Entitlement *entitlement `json:"entitlement,omitempty"`
	} `json:"allowance"`
	Money map[string]moneyPot `json:"money"`
	// Plan is the account's plan as the brain read it ({label, basis});
	// only its label is read here, to say which plan refuses a model.
	Plan *struct {
		Label *string `json:"label"`
	} `json:"plan,omitempty"`
}

// planLabel is the account's plan label, "" when none was read.
func (st *acctStanding) planLabel() string {
	if st == nil || st.Plan == nil || st.Plan.Label == nil {
		return ""
	}
	return *st.Plan.Label
}

// entitlement is a pot's {state, basis}: state "active" when the plan
// includes it.
type entitlement struct {
	State string `json:"state"`
}

// stopAt is the owner's per-route stop on one allowance window (§2.4).
type stopAt struct {
	Window string  `json:"window"` // a period ("7d"), a scope ("Fable"), or "effective"
	Level  float64 `json:"level"`
}

// routePolicy is one route's owner settings as the brain serves them.
type routePolicy struct {
	ID     string   `json:"id"`
	Pay    []string `json:"pay"`
	StopAt *stopAt  `json:"stop_at"`
	// Scope is the lane's scoped cap ("Fable"); "" for a route no scoped
	// window binds.
	Scope string `json:"scope"`
}

// routePref is one /preferred routes{} entry: the policy plus what the route
// is. The brain's own checks ride beside it and are not read here: this proxy
// evaluates its own and never ANDs it with the brain's (§2.7, 62 m2).
type routePref struct {
	routePolicy
	Kind     string   `json:"kind"` // member | lane
	Class    string   `json:"class"`
	Member   string   `json:"member"`
	Lane     string   `json:"lane"`
	Provider string   `json:"provider"`
	Accounts []string `json:"accounts"`
	Priority *int     `json:"priority"`
}

// potSettings is one money pot's entry in the money document.
type potSettings struct {
	Topup string `json:"topup"` // "off" (default) | "on"
	// Budget is the owner's cap on our own spend from the pot this budget
	// period, in its native unit; RouteBudgets the cap per route (§1.9). nil
	// / absent = no budget. Only a pot whose charges the ledger counts takes
	// one (the brain refuses it at PUT otherwise).
	Budget       *float64           `json:"budget"`
	RouteBudgets map[string]float64 `json:"route_budgets"`
}

// checkLedger is the /preferred `ledger` block as the check reads it: the
// budget period, when the brain read its ledger, and this period's counted
// spend per pot and per route (§1.7). The proxy adds the entries it has
// posted and the brain has not yet counted (ledger.go).
type checkLedger struct {
	Period struct {
		Start float64 `json:"start"`
		End   float64 `json:"end"`
	} `json:"period"`
	AsOf *float64 `json:"as_of"`
	// Seq is the count of entries the brain's ledger holds: an entry this
	// proxy posted is in this ledger once Seq reaches the seq its post
	// returned (ledger.go).
	Seq   int64                  `json:"seq"`
	Pots  map[string]ledgerPot   `json:"pots"`
	Rates map[string]ledgerRates `json:"rates"` // per unit: what ledger.go charges by
	// RateTables is, per unit, the tables its charges need (the brain's
	// _LEDGER_RATE_TABLES): the one rule the brain's gate, this one and the
	// deploy preflight check. A unit it does not list has no rule, and no
	// budget on its pot can say it has room.
	RateTables map[string][]string `json:"rate_tables"`
	// UnreadableLines counts the brain's ledger lines it could not read: any
	// may hide spend, so while there is one no budget can say it has room.
	UnreadableLines int `json:"unreadable_lines"`
	// unposted is, per pot, the time of the oldest charged entry of ours this
	// period the brain has not accepted (a post failing, or refused). Only
	// this process holds it: ledgerView sets it, the brain has no such field.
	unposted map[string]float64
}

// ledgerPot is one countable pot this period: its native unit, our counted
// spend in total and per route, and per route the entries whose charge is a
// lower bound (part of their cost has no published price).
type ledgerPot struct {
	Unit        *string            `json:"unit"`
	PeriodSpend float64            `json:"period_spend"`
	Routes      map[string]float64 `json:"routes"`
	Unpriced    map[string]int     `json:"unpriced"`
	// Reserved and RoutesReserved are what this proxy's dials in flight hold
	// against the pot, and per route (a Runway task's max_cost, §1.7), until
	// each settles. The brain holds none: only ledgerView sets them.
	Reserved       float64            `json:"reserved"`
	RoutesReserved map[string]float64 `json:"routes_reserved"`
}

// checkRequest is the one request a check is made for (§1.6): MaxCost is the
// most it can charge in the pot's unit (nil: unknown in advance, a chat
// request's cost is learned afterwards), Outputs how many outputs a rolling
// count counts (below 1 counts as 1). The zero value is the brain's own
// check: unknown cost, one output.
type checkRequest struct {
	MaxCost *float64 `json:"max_cost"`
	Outputs int      `json:"outputs"`
}

// topupRefillS: an empty Runway balance under topup on (§1.9) is refilled by
// autobilling's hourly check (13 §5), due by the empty reading's as_of plus
// this. Mirrors the brain's TOPUP_REFILL_S.
const topupRefillS = 3600

// checkUntil is a check's `until`: an instant (epoch seconds), "until_paid",
// or unknown (JSON null).
type checkUntil struct {
	At   float64
	Paid bool
}

func untilAt(t *float64) checkUntil {
	if t == nil {
		return checkUntil{}
	}
	return checkUntil{At: *t}
}

func (u checkUntil) known() bool { return u.At != 0 }

// Time is the instant, zero when the until is not one.
func (u checkUntil) Time() time.Time {
	if !u.known() {
		return time.Time{}
	}
	return time.Unix(0, int64(u.At*1e9))
}

func (u checkUntil) String() string {
	switch {
	case u.known():
		return u.Time().UTC().Format(time.RFC3339)
	case u.Paid:
		return "until_paid"
	}
	return "unknown"
}

func (u checkUntil) MarshalJSON() ([]byte, error) {
	switch {
	case u.known():
		return json.Marshal(u.At)
	case u.Paid:
		return []byte(`"until_paid"`), nil
	}
	return []byte("null"), nil
}

func (u *checkUntil) UnmarshalJSON(b []byte) error {
	*u = checkUntil{}
	b = bytes.TrimSpace(b)
	switch {
	case bytes.Equal(b, []byte("null")):
		return nil
	case bytes.Equal(b, []byte(`"until_paid"`)):
		u.Paid = true
		return nil
	}
	return json.Unmarshal(b, &u.At)
}

// dialCheck is the answer. Pot is set on `paid`; Reason on `no` (unknown |
// stop_at | pay | spent | budget | balance).
type dialCheck struct {
	Check  string     `json:"check"`
	Pot    string     `json:"pot"`
	Reason string     `json:"reason"`
	Until  checkUntil `json:"until"`
	Basis  string     `json:"basis"` // fresh | stale | unknown

	// route is the route the check was made for (routeCheck): the ledger
	// names it on a paid dial's entry.
	route string

	// held: the check is `no (spent)` only because an unattributed record
	// names the model (routeCheck): without it the route would be included,
	// and no money can open behind it. PR11 re-admits exactly these (§2.2
	// row 14).
	held bool
}

const (
	checkIncluded = "included"
	checkPaid     = "paid"
	checkNo       = "no"
)

func (c dialCheck) no() bool       { return c.Check == checkNo }
func (c dialCheck) included() bool { return c.Check == checkIncluded }
func (c dialCheck) paid() bool     { return c.Check == checkPaid }

// onAllowance: a dial on this check draws on the included allowance, so its
// 2xx speaks for the allowance (noteServed). A paid dial's 2xx says only that
// money served.
func (c dialCheck) onAllowance() bool { return c.included() || c.held }

// String is the check as every dial logs it: "included", "paid:gpt/credits",
// "no(spent until 2026-10-30T12:00:00Z)"; a basis other than fresh is appended.
func (c dialCheck) String() string {
	s := c.Check
	switch c.Check {
	case checkPaid:
		s += ":" + c.Pot
	case checkNo:
		s += "(" + c.Reason + " until " + c.Until.String() + ")"
	case "":
		return "unchecked"
	}
	if c.Basis != "" && c.Basis != "fresh" {
		s += " basis=" + c.Basis
	}
	return s
}

// funding is the tier a served dial drew on, as the owner reads it (§2.11):
// "included", "paid:<pot>", or "paid?:<pot>" where no reading says the pot
// is open.
func (c dialCheck) funding() string {
	switch {
	case c.paid() && c.Basis == "unknown":
		return "paid?:" + c.Pot
	case c.paid():
		return "paid:" + c.Pot
	case c.onAllowance():
		return checkIncluded
	}
	return ""
}

// unixSeconds is now in the evaluator's unit.
func unixSeconds(t time.Time) float64 { return float64(t.UnixNano()) / 1e9 }

func bindingWindows(st *acctStanding, scope string) []checkWindow {
	names := make([]string, 0, len(st.Allowance.Windows))
	for n := range st.Allowance.Windows {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]checkWindow, 0, len(names))
	for _, n := range names {
		w := st.Allowance.Windows[n]
		if w.Scope == nil || (scope != "" && strings.EqualFold(*w.Scope, scope)) {
			out = append(out, w)
		}
	}
	return out
}

// windowClosed: at its limit, or shut by its own gate, and its reset (when
// known) still ahead: a reset that has passed opens the window whatever the
// last reading said (§1.4).
func windowClosed(w checkWindow, now float64) bool {
	shut := (w.Open != nil && !*w.Open) || (w.UsedPct != nil && *w.UsedPct >= 100)
	return shut && !(w.ResetsAt != nil && *w.ResetsAt <= now)
}

// budgetPeriod is the budget period holding now (§1.9): the calendar month
// in UTC, as epoch seconds. Mirrors the brain's _budget_period.
func budgetPeriod(now float64) (start, end float64) {
	d := time.Unix(int64(now), 0).UTC()
	s := time.Date(d.Year(), d.Month(), 1, 0, 0, 0, 0, time.UTC)
	return float64(s.Unix()), float64(s.AddDate(0, 1, 0).Unix())
}

// budgetGate is the owner's budget on the drawn money pot (§1.7, §1.9): nil
// when no budget binds this route, else its `no`. A pot budget and the
// route's own both apply; the first reached closes, until the allowance
// ahead reopens and no later than the period's end. A reached budget stays
// reached whatever else the ledger lacks (spend only grows), so it is
// decided first. A ledger of an earlier period has spent nothing of this
// one. Otherwise a ledger that may undercount cannot say the budget has
// room: no (unknown), its basis saying why:
//
//	unknown            the ledger is older than staleMax (pulls failing):
//	                   until the account's next poll;
//	ledger_unreadable  the brain's ledger file has a line it cannot read;
//	no_rates           the ledger lists no unit for the pot (armLedger would
//	                   enter nothing), or its rates lack a table its rule
//	                   (the ledger's rate_tables) says the unit's charges
//	                   need, or it publishes no rule for the unit
//	                   (ledgerRates.complete), so some charge would enter
//	                   uncounted;
//	unpriced           a lower_bound entry this period on the pot (a pot
//	                   budget) or on this route (its route budget): its spend
//	                   may be higher than counted;
//	ledger_unposted    (this proxy's only) a charged entry of ours on the pot
//	                   has gone unaccepted longer than staleMax: a restart
//	                   would lose it from the count.
//
// The last three state no time. req is the request checked for: its max_cost
// is reserved against the budget (budgetsReached). Mirrors the brain's
// _budget_gate.
func budgetGate(r routePolicy, potID string, set potSettings, l *checkLedger, now, staleMax float64, reopen *float64, basis string, nextPollAt *float64, req checkRequest) *dialCheck {
	if _, hasRB := set.RouteBudgets[r.ID]; set.Budget == nil && !hasRB {
		return nil
	}
	if l != nil && len(budgetsReached(r, potID, set, l, now, req)) > 0 {
		_, end := budgetPeriod(now)
		until := end
		if reopen != nil && *reopen < end {
			until = *reopen
		}
		return &dialCheck{Check: checkNo, Reason: "budget", Until: checkUntil{At: until}, Basis: basis}
	}
	if l == nil || l.AsOf == nil || now-*l.AsOf > staleMax {
		return &dialCheck{Check: checkNo, Reason: "unknown", Until: untilAt(nextPollAt), Basis: "unknown"}
	}
	if l.UnreadableLines > 0 {
		return &dialCheck{Check: checkNo, Reason: "unknown", Basis: "ledger_unreadable"}
	}
	u := l.Pots[potID].Unit
	if u == nil {
		return &dialCheck{Check: checkNo, Reason: "unknown", Basis: "no_rates"}
	}
	if need, ok := l.RateTables[*u]; !ok || !l.Rates[*u].complete(need) {
		return &dialCheck{Check: checkNo, Reason: "unknown", Basis: "no_rates"}
	}
	unpriced := 0
	if start, _ := budgetPeriod(now); l.Period.Start == start {
		up := l.Pots[potID].Unpriced
		if set.Budget != nil {
			for _, n := range up {
				unpriced += n
			}
		}
		if _, ok := set.RouteBudgets[r.ID]; ok {
			unpriced += up[r.ID]
		}
	}
	if unpriced > 0 {
		return &dialCheck{Check: checkNo, Reason: "unknown", Basis: "unpriced"}
	}
	if at, ok := l.unposted[potID]; ok && now-at > staleMax {
		return &dialCheck{Check: checkNo, Reason: "unknown", Basis: "ledger_unposted"}
	}
	return nil
}

// budgetsReached is which of the owner's budgets on the pot this route has
// reached this period: a subset of ["pot", "route"], in that order. A ledger
// of an earlier period has spent nothing of this one. A request whose most it
// can charge is known (a Runway task's max_cost, §1.4) is admitted only if
// spend + reserved + max_cost <= budget (§1.7 overrun); reserved is what this
// proxy's dials in flight hold, whatever the ledger's period. With the cost
// unknown the budget is reached at spend + reserved >= budget. Mirrors the
// brain's _budgets_reached.
func budgetsReached(r routePolicy, potID string, set potSettings, l *checkLedger, now float64, req checkRequest) []string {
	start, _ := budgetPeriod(now)
	var spend, rspend float64
	if l != nil {
		if lp, ok := l.Pots[potID]; ok {
			if l.Period.Start == start {
				spend, rspend = lp.PeriodSpend, lp.Routes[r.ID]
			}
			spend += lp.Reserved
			rspend += lp.RoutesReserved[r.ID]
		}
	}
	reached := func(limit, spent float64) bool {
		if req.MaxCost == nil {
			return spent >= limit
		}
		return spent+*req.MaxCost > limit
	}
	var out []string
	if set.Budget != nil && reached(*set.Budget, spend) {
		out = append(out, "pot")
	}
	if rb, ok := set.RouteBudgets[r.ID]; ok && reached(rb, rspend) {
		out = append(out, "route")
	}
	return out
}

// stopWindow is the window a stop_at names among the binding ones, nil when
// none of them is it (the stop then has no effect on this account, §1.9).
func stopWindow(s *stopAt, ws []checkWindow) *checkWindow {
	if s.Window == "effective" {
		var best *checkWindow
		for i := range ws {
			if ws[i].UsedPct != nil && (best == nil || *ws[i].UsedPct > *best.UsedPct) {
				best = &ws[i]
			}
		}
		return best
	}
	for i := range ws {
		if ws[i].Scope == nil && ws[i].Period == s.Window {
			return &ws[i]
		}
	}
	for i := range ws {
		if ws[i].Scope != nil && strings.EqualFold(*ws[i].Scope, s.Window) {
			return &ws[i]
		}
	}
	return nil
}

// allowanceState: open is 1 (open), 0 (closed) or -1 (unknown); reopen is when
// a closed allowance opens again, nil when no reading says.
func allowanceState(st *acctStanding, ws []checkWindow, now float64) (open int, reopen *float64) {
	latest := func(ws []checkWindow) *float64 {
		var max *float64
		for _, w := range ws {
			if w.ResetsAt == nil {
				return nil
			}
			if max == nil || *w.ResetsAt > *max {
				r := *w.ResetsAt
				max = &r
			}
		}
		return max
	}
	var closed []checkWindow
	for _, w := range ws {
		if windowClosed(w, now) {
			closed = append(closed, w)
		}
	}
	if len(closed) > 0 {
		return 0, latest(closed)
	}
	gate := st.Allowance.Open
	if gate != nil && !*gate {
		r := latest(ws)
		if r != nil && *r <= now {
			return 1, nil
		}
		return 0, r
	}
	if gate == nil {
		if len(ws) == 0 {
			return -1, nil
		}
		for _, w := range ws {
			if w.UsedPct == nil {
				return -1, nil
			}
		}
	}
	return 1, nil
}

// evalDialCheck is §1.6 steps 1-4 for one (route, account) and one request,
// at now (epoch seconds), with the budget gate at the paid step (§1.7).
// Entitlement (step 5) has no reading before C9, and every request is
// admitted by the allowance until a reading says otherwise, so neither
// appears yet. req is the request (its zero value the brain's own check:
// unknown cost, one output). An api account has its own walk (apiCheck),
// unless its reading opens an allowance (an api key with no quota to read,
// anthropic-api): then the allowance walk below is the whole answer, as for
// any open allowance. Mirrors the brain's dial_check line for line.
func evalDialCheck(r routePolicy, account string, st *acctStanding, pots map[string]potSettings, ledger *checkLedger, staleMax, now float64, req checkRequest) dialCheck {
	if st == nil || st.AsOf == nil {
		var next checkUntil
		if st != nil {
			next = untilAt(st.NextPollAt)
		}
		return dialCheck{Check: checkNo, Reason: "unknown", Until: next, Basis: "unknown"}
	}
	if st.Kind == "api" && (st.Allowance.Open == nil || !*st.Allowance.Open) {
		return apiCheck(r, account, st, pots, ledger, staleMax, now, req)
	}
	ws := bindingWindows(st, r.Scope)
	stale := routeStale(st, ws, now, staleMax)
	allow, reopen := allowanceState(st, ws, now)

	// One money pot per account (a subscription's credits, an api account's
	// balance); sorted so both evaluators take the same one regardless.
	potID, pot := potOf(st, account)
	mayPay := potID != "" && containsStr(r.Pay, potID)
	moneyMayOpen := pot != nil && (pot.Open == nil || *pot.Open)

	basis := "fresh"
	switch {
	case stale:
		basis = "stale"
	case allow < 0:
		basis = "unknown"
	}
	// Step 4: stale or unknown blocks only where it could hide money the route
	// may not pay (62 B1). Elsewhere the last value is used, and ranks after fresh.
	if (stale || allow < 0) && moneyMayOpen && !mayPay {
		return dialCheck{Check: checkNo, Reason: "unknown", Until: untilAt(st.NextPollAt), Basis: basis}
	}
	if allow != 0 {
		if s := r.StopAt; s != nil && s.Level < 100 {
			// A window whose reset has passed since the reading has reopened:
			// its last value no longer counts against the level (§1.4). Level 0
			// never uses that allowance (§1.9), so it holds across the reset,
			// until the reading after it.
			live := ws
			if s.Level > 0 {
				live = make([]checkWindow, 0, len(ws))
				for _, w := range ws {
					if !(w.ResetsAt != nil && *w.ResetsAt <= now) {
						live = append(live, w)
					}
				}
			}
			if w := stopWindow(s, live); w != nil && (s.Level <= 0 || (w.UsedPct != nil && *w.UsedPct >= s.Level)) {
				until := w.ResetsAt
				if until != nil && *until <= now {
					until = st.NextPollAt
				}
				return dialCheck{Check: checkNo, Reason: "stop_at", Until: untilAt(until), Basis: basis}
			}
		}
		return dialCheck{Check: checkIncluded, Basis: basis}
	}
	if moneyMayOpen {
		if mayPay {
			if g := budgetGate(r, potID, pots[potID], ledger, now, staleMax, reopen, basis, st.NextPollAt, req); g != nil {
				return *g
			}
			b := basis
			if pot.Open == nil {
				b = "unknown"
			}
			return dialCheck{Check: checkPaid, Pot: potID, Basis: b}
		}
		u := untilAt(reopen)
		if !u.known() {
			u.Paid = true
		}
		return dialCheck{Check: checkNo, Reason: "pay", Until: u, Basis: basis}
	}
	var refill *float64
	if pot != nil && pots[potID].Topup == "on" {
		refill = st.NextPollAt
	}
	u := untilAt(reopen)
	if refill != nil && (!u.known() || *refill < u.At) {
		u = untilAt(refill)
	}
	if !u.known() {
		// No reading says when the allowance reopens: retry at the next poll,
		// which will (no money is involved: the pot is closed or absent).
		u = untilAt(st.NextPollAt)
	}
	return dialCheck{Check: checkNo, Reason: "spent", Until: u, Basis: basis}
}

// apiCheck is §1.6 for an api account (§1.2: no allowance, a money pot drawn
// first, Runway's balance): every dial spends money. In order:
//
//	pay            the route may not draw the pot: no (pay, until_paid);
//	a closed binding window (only a refusal record makes one here, this
//	               proxy's): no (spent, until it reopens);
//	balance        below the request's max_cost, or nothing left when the
//	               cost is unknown, or the pot shut: no (balance), until a
//	               reading shows money (until_paid) with topup off, and with
//	               on until the hourly refill: the empty reading's as_of +
//	               topupRefillS, else the next poll once that has passed (§1.9);
//	rolling_count  used + what the request counts > limit, for a count that
//	               binds the route's model: no (spent) until the next poll (a
//	               rolling count names no reset);
//	budget         the budget gate, the request's max_cost reserved (§1.7);
//	paid           otherwise, basis unknown while the balance is.
//
// A pot whose limits are unknown does not block: the route may pay it, so the
// provider's refusal is the worst it costs (§1.4), and that walks. Mirrors the
// brain's _api_check.
func apiCheck(r routePolicy, account string, st *acctStanding, pots map[string]potSettings, ledger *checkLedger, staleMax, now float64, req checkRequest) dialCheck {
	potID, pot := potOf(st, account)
	ws := bindingWindows(st, r.Scope)
	basis := "fresh"
	if routeStale(st, ws, now, staleMax) {
		basis = "stale"
	}
	if potID == "" || !containsStr(r.Pay, potID) {
		return dialCheck{Check: checkNo, Reason: "pay", Until: checkUntil{Paid: true}, Basis: basis}
	}
	if len(ws) > 0 {
		if allow, reopen := allowanceState(st, ws, now); allow == 0 {
			u := untilAt(reopen)
			if !u.known() {
				u = untilAt(st.NextPollAt)
			}
			return dialCheck{Check: checkNo, Reason: "spent", Until: u, Basis: basis}
		}
	}
	bal := pot.Balance
	short := bal != nil && ((req.MaxCost != nil && *bal < *req.MaxCost) || (req.MaxCost == nil && *bal <= 0))
	if (pot.Open != nil && !*pot.Open) || short {
		u := checkUntil{Paid: true}
		if pots[potID].Topup == "on" {
			u = checkUntil{At: *st.AsOf + topupRefillS}
			if u.At <= now {
				u = untilAt(st.NextPollAt)
			}
		}
		return dialCheck{Check: checkNo, Reason: "balance", Until: u, Basis: basis}
	}
	names := make([]string, 0, len(pot.Limits))
	for n := range pot.Limits {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		lim := pot.Limits[n]
		if lim.Kind != "rolling_count" || !scopeBinds(lim.Scope, r.Scope) {
			continue
		}
		counted := 1.0
		if lim.Counts == "output" && req.Outputs > 1 {
			counted = float64(req.Outputs)
		}
		if lim.Used != nil && lim.Limit != nil && *lim.Used+counted > *lim.Limit {
			return dialCheck{Check: checkNo, Reason: "spent", Until: untilAt(st.NextPollAt), Basis: basis}
		}
	}
	if g := budgetGate(r, potID, pots[potID], ledger, now, staleMax, nil, basis, st.NextPollAt, req); g != nil {
		return *g
	}
	b := basis
	if bal == nil {
		b = "unknown"
	}
	return dialCheck{Check: checkPaid, Pot: potID, Basis: b}
}

// scopeBinds: does a limit of this scope bind a route of that scope? An
// unscoped limit binds every route; a scoped one the route of its own scope,
// compared case-blind. Mirrors the brain's _scope_binds.
func scopeBinds(limitScope *string, scope string) bool {
	return limitScope == nil || (scope != "" && strings.EqualFold(*limitScope, scope))
}

// routeStale: staleness is per limit (§1.4), stale when a limit that binds
// the route was read more than staleMax ago. A window carries its own as_of (a
// response refreshes the windows it carries); a provider's own gate (codex)
// and an account with no binding window are as old as the poll that read
// them, the standing's as_of. Mirrors the brain's _route_stale.
func routeStale(st *acctStanding, ws []checkWindow, now, staleMax float64) bool {
	times := make([]*float64, 0, len(ws)+1)
	for _, w := range ws {
		times = append(times, w.AsOf)
	}
	if len(ws) == 0 || st.Allowance.Open != nil {
		times = append(times, st.AsOf)
	}
	for _, t := range times {
		if t == nil || now-*t > staleMax {
			return true
		}
	}
	return false
}

func containsStr(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

var checkRank = map[string]int{checkIncluded: 0, checkPaid: 1, checkNo: 2}
var basisRank = map[string]int{"fresh": 0, "unknown": 1, "stale": 2}

// checkLess orders checks: included, then paid, then no; fresh before unknown
// before stale inside a tier; the earliest `until` first among `no`
// ("until_paid" after any instant, an unknown time last).
func checkLess(a, b dialCheck) bool {
	if checkRank[a.Check] != checkRank[b.Check] {
		return checkRank[a.Check] < checkRank[b.Check]
	}
	if a.Check != checkNo {
		ra, ok := basisRank[a.Basis]
		if !ok {
			ra = 2
		}
		rb, ok := basisRank[b.Basis]
		if !ok {
			rb = 2
		}
		return ra < rb
	}
	ua, ub := untilRank(a.Until), untilRank(b.Until)
	if ua != ub {
		return ua < ub
	}
	return ua == 0 && a.Until.At < b.Until.At
}

func untilRank(u checkUntil) int {
	switch {
	case u.known():
		return 0
	case u.Paid:
		return 1
	}
	return 2
}

// payOrderLess is the paid tier's order (§1.9): the drawn pot's rank in
// pay_order (every unlisted pot after every listed one), then member priority,
// then pot id.
func payOrderLess(potA string, prioA int, potB string, prioB int, payOrder []string) bool {
	rank := func(p string) int {
		for i, x := range payOrder {
			if x == p {
				return i
			}
		}
		return len(payOrder)
	}
	if ra, rb := rank(potA), rank(potB); ra != rb {
		return ra < rb
	}
	if prioA != prioB {
		return prioA < prioB
	}
	return potA < potB
}
