package main

// ---- the ledger's entries (quota-model 50 §1.7, C6) ----
//
// The brain keeps the ledger; this proxy writes it. A request a `paid` check
// sent to a pot whose charges can be counted (the brain lists those pots in
// /preferred ledger.pots) is entered once its response ends: the charge from
// its usage at the rates the brain publishes (ledger.rates), or, when no
// usage came back, `uncounted` with its reason (shown in reconciliation, never
// in a budget). Entries go to the brain as `ledger` events on its POST
// /reports (reports.go), which dedupes them by id, so a resend after a lost
// reply counts once.
//
// The budget gate (dialcheck.go budgetGate) reads the brain's ledger plus
// every entry of ours the brain has not yet counted: one not yet accepted, or
// accepted at a seq newer than the ledger this proxy last pulled. So a budget
// closes at the next check after the request that reached it, not at the next
// pull. An entry is dropped once a pulled ledger's seq covers it.
//
// Entries live in memory: an entry not yet accepted when this process stops is
// lost to the ledger, and the brain's reconciliation shows its charge as
// "spent elsewhere or uncounted". Every settled entry wakes the reporter at
// once, and what waits is posted once more at shutdown (reportsFlush), so that
// window is a brain outage, not a batch interval. A budget never rests on that window for long: once a
// charged entry on its pot has gone unaccepted longer than stale_max (a post
// failing, or refused), the budget reads no (unknown) (budgetGate), so a
// crash loses at most what went unposted for less than stale_max.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ledgerRates is one unit's rates as the brain publishes them: per_tokens
// tokens per rate, each model's input / cached / output rate, each image
// model's rates for its image and its text tokens, and the speed weights.
type ledgerRates struct {
	PerTokens    float64                   `json:"per_tokens"`
	Models       map[string]tokenRate      `json:"models"`
	Images       map[string]imageTokenRate `json:"images"`
	SpeedWeights map[string]float64        `json:"speed_weights"`
}

// complete reports whether the rates carry every table in need, the tables
// the brain publishes (ledger.rate_tables, its _LEDGER_RATE_TABLES) that the
// unit's charges are made by: codex credits' chat tokens by model and speed
// and the image tool's tokens, each per per_tokens; none for a Runway task,
// charged its own cost.credits. With any missing, some charge on the pot would
// enter uncounted. A table this proxy does not know is never published.
func (r ledgerRates) complete(need []string) bool {
	for _, t := range need {
		var ok bool
		switch t {
		case "per_tokens":
			ok = r.PerTokens > 0
		case "models":
			ok = len(r.Models) > 0
		case "speed_weights":
			ok = len(r.SpeedWeights) > 0
		case "images":
			ok = len(r.Images) > 0
		}
		if !ok {
			return false
		}
	}
	return true
}

// tokenRate is one kind of token's input / cached / output rate.
type tokenRate struct {
	Input  float64 `json:"input"`
	Cached float64 `json:"cached"`
	Output float64 `json:"output"`
}

// imageTokenRate is an image model's rates: its image tokens and its text
// tokens are priced apart (11 §3: GPT-Image-2 200/50/750 and 125/31.25/250).
type imageTokenRate struct {
	Image tokenRate `json:"image"`
	Text  tokenRate `json:"text"`
}

// ledgerEntry is one charge as the brain's ledger takes it (a `ledger`
// event's fields). Charge nil = uncounted, with its Reason.
type ledgerEntry struct {
	ID      string         `json:"id"`
	At      float64        `json:"at"`
	Account string         `json:"account"`
	Pot     string         `json:"pot"`
	Route   string         `json:"route"`
	Model   *string        `json:"model"`
	Charge  *float64       `json:"charge"`
	Unit    string         `json:"unit"`
	Basis   *string        `json:"basis"`
	Reason  *string        `json:"reason"`
	Usage   map[string]int `json:"usage"`
	Speed   *string        `json:"speed"`
}

func (e ledgerEntry) potID() string { return e.Account + "/" + e.Pot }

// ownEntry is an entry of ours and how far the brain has it: seq 0 = not yet
// accepted; refused = the brain refused it (its result `refused`, and why),
// so it is never posted again and is counted here until its period ends.
type ownEntry struct {
	e       ledgerEntry
	seq     int64
	refused string
}

// Per tenant: tenant.ledgerMu, tenant.ledgerOwn, tenant.ledgerLastErr (the
// failure of the last post that carried entries, or the brain's refusal of
// one, "" after a post it took them in: shown on /_switch/status) and
// tenant.ledgerHolds (the dials in flight that reserved their max_cost,
// ledgerReserve, until each releases its hold).

// ledgerMark is one client request's: set once any dial made for it entered a
// ledger entry (any pot, any basis, uncounted included), so its reply tells
// the client not to run it again on its own. An OpenAI or Anthropic SDK
// re-runs a 5xx, a 429 and a 409, and a re-run of a request whose dial was
// charged is charged again. The front doors put one on the request's context
// (withLedgerMark); a hop armed on a context without one marks nothing.
type ledgerMark struct{ entered atomic.Bool }

type ledgerMarkKey struct{}

func withLedgerMark(ctx context.Context) (context.Context, *ledgerMark) {
	m := &ledgerMark{}
	return context.WithValue(ctx, ledgerMarkKey{}, m), m
}

// charged: a dial of this request entered a ledger entry. nil-safe.
func (m *ledgerMark) charged() bool { return m != nil && m.entered.Load() }

// ledgerNoRetry is the header a charged request's reply carries.
func ledgerNoRetry(h http.Header) { h.Set("x-should-retry", "false") }

// ledgerHold is one dial in flight's reservation on its pot (§1.7 overrun):
// a Runway task's max_cost, held against the owner's budgets from before its
// create until its charge is entered.
type ledgerHold struct {
	pot, route string
	amount     float64
}

// ledgerHop is one dial a paid check sent to a countable pot, waiting for its
// response to end. The translator settles it once: counted from the
// response's usage, or uncounted when the response ended without one. A nil
// hop (a dial on the allowance, or a pot whose charges are not counted) does
// nothing.
type ledgerHop struct {
	once                sync.Once
	tenant              *tenant // whose ledger the entry goes to
	account, pot, route string
	model, unit, speed  string
	rates               *ledgerRates
	mark                *ledgerMark // the client request's (nil: none)
}

// armLedger is the ledger hop of a served codex dial, nil when its check did
// not draw a pot the ledger counts. model is the model the dial asked for; the
// response's own model, when it names one, is the one charged. ctx is the
// hop's: its client request's ledgerMark is set when the hop enters.
func (tn *tenant) armLedger(ctx context.Context, chk dialCheck, model string) *ledgerHop {
	if !chk.paid() || chk.route == "" {
		return nil
	}
	i := strings.LastIndex(chk.Pot, "/")
	if i <= 0 {
		return nil
	}
	tn.pref.mu.RLock()
	defer tn.pref.mu.RUnlock()
	if tn.pref.ledger == nil {
		return nil
	}
	lp, ok := tn.pref.ledger.Pots[chk.Pot]
	if !ok || lp.Unit == nil {
		return nil // not a pot whose charges the ledger counts
	}
	h := &ledgerHop{tenant: tn, account: chk.Pot[:i], pot: chk.Pot[i+1:], route: chk.route, model: model,
		unit: *lp.Unit, mark: ledgerMarkOf(ctx)}
	if containsStr(tn.pref.ledger.RateTables[h.unit], "speed_weights") {
		// A unit whose charges a speed weighs (codex credits). This proxy
		// asks codex for no speed mode (translateAnthropicToCodex sets no
		// service_tier), so every request is the standard speed. A unit with
		// no speed (a Runway task's credits) enters none.
		h.speed = "standard"
	}
	if r, ok := tn.pref.ledger.Rates[h.unit]; ok {
		h.rates = &r
	}
	return h
}

// settle enters the hop from the response that ended it. A response with no
// usage block is uncounted.
func (h *ledgerHop) settle(r *codexRespObj) {
	if h == nil {
		return
	}
	if r == nil || r.Usage == nil {
		h.abandon("the response carried no usage")
		return
	}
	h.once.Do(func() {
		model := h.model
		if r.Model != "" {
			model = r.Model
		}
		u := r.Usage
		cached := 0
		if u.InputTokensDetails != nil {
			cached = u.InputTokensDetails.CachedTokens
		}
		usage := map[string]int{"input_tokens": u.InputTokens, "cached_tokens": cached, "output_tokens": u.OutputTokens}
		charge, basis, why := codexCharge(h.rates, model, h.speed, u.InputTokens, cached, u.OutputTokens)
		e := h.entry(model)
		e.Usage = usage
		if why != "" {
			e.Reason = &why
		} else {
			e.Charge, e.Basis = &charge, &basis
		}
		h.enter(e)
	})
}

// settleFailed enters a hop whose response failed: from the usage the failed
// response reports, since the upstream billed what it reports, else
// uncounted with the failure.
func (h *ledgerHop) settleFailed(r *codexRespObj, msg string) {
	if r != nil && r.Usage != nil {
		h.settle(r)
		return
	}
	h.abandon("the response failed: " + msg)
}

// abandon enters the hop uncounted: it ended with no usage returned.
func (h *ledgerHop) abandon(reason string) {
	if h == nil {
		return
	}
	h.once.Do(func() {
		e := h.entry(h.model)
		e.Reason = &reason
		h.enter(e)
	})
}

// enter makes the hop's entry and marks its client request charged.
func (h *ledgerHop) enter(e ledgerEntry) {
	if h.mark != nil {
		h.mark.entered.Store(true)
	}
	h.tenant.ledgerEnter(e)
}

func ledgerMarkOf(ctx context.Context) *ledgerMark {
	if ctx == nil {
		return nil
	}
	m, _ := ctx.Value(ledgerMarkKey{}).(*ledgerMark)
	return m
}

func (h *ledgerHop) entry(model string) ledgerEntry {
	e := ledgerEntry{ID: newLedgerID(), At: unixSeconds(nowFn()), Account: h.account, Pot: h.pot,
		Route: h.route, Unit: h.unit}
	if model != "" {
		e.Model = &model
	}
	if h.speed != "" {
		sp := h.speed
		e.Speed = &sp
	}
	return e
}

// settleTask enters a Runway task's hop (§1.7: Runway's entries are its
// cost.credits): the task's cost as Runway reports it, basis exact (a refunded
// failure reports 0); where it reports none, the most the task may charge,
// basis upper_bound, so a budget may overcount, never undercount: the
// create's estimatedCost ("the maximum credits this task may charge"), else
// the price table's maxCost. ended names how a task that did not succeed
// ended (its shape), for the log; "" for one that did.
func (h *ledgerHop) settleTask(cost *int64, estimated float64, maxCost int64, ended string) {
	if h == nil {
		return
	}
	h.once.Do(func() {
		e := h.entry(h.model)
		charge, basis := estimated, "upper_bound"
		switch {
		case cost != nil:
			charge, basis = float64(*cost), "exact"
		case estimated <= 0:
			charge = float64(maxCost)
		}
		e.Charge, e.Basis = &charge, &basis
		e.Usage = map[string]int{"outputs": 1}
		if ended != "" {
			why := "the task ended " + ended
			e.Reason = &why
		}
		h.enter(e)
	})
}

// settleCharge enters a charge the door computed itself (a song's length at
// its published rate, elevenlabs_api.go): charge with its basis, the usage it
// was computed from, and why, when there is something to say. Once, like
// every settle.
func (h *ledgerHop) settleCharge(charge float64, basis, reason string, usage map[string]int) {
	if h == nil {
		return
	}
	h.once.Do(func() {
		e := h.entry(h.model)
		e.Charge, e.Basis = &charge, &basis
		e.Usage = usage
		if reason != "" {
			e.Reason = &reason
		}
		h.enter(e)
	})
}

// codexCharge is a chat request's charge in codex credits (§1.7, 11 §3):
// the uncached input, the cached input and the output (reasoning included,
// it is billed as output) at the model's rate per per_tokens, times the speed
// weight. basis derived: the rates are documented, not read. A model with no
// rate is charged at the highest rate of each kind, and a speed with no
// weight at the heaviest: upper_bound, so a budget may overcount, never
// undercount. why is set (and the charge unusable) when no rates were
// published for the unit.
func codexCharge(r *ledgerRates, model, speed string, input, cached, output int) (charge float64, basis, why string) {
	if r == nil || r.PerTokens <= 0 || len(r.Models) == 0 || len(r.SpeedWeights) == 0 {
		return 0, "", "the brain published no rates for the pot's unit"
	}
	basis = "derived"
	rate, ok := r.Models[model]
	if !ok {
		basis = "upper_bound"
		for _, m := range r.Models {
			rate.Input = max(rate.Input, m.Input)
			rate.Cached = max(rate.Cached, m.Cached)
			rate.Output = max(rate.Output, m.Output)
		}
	}
	w, ok := r.SpeedWeights[speed]
	if !ok {
		basis = "upper_bound"
		for _, v := range r.SpeedWeights {
			w = max(w, v)
		}
	}
	uncached := max(input-cached, 0)
	charge = (float64(uncached)*rate.Input + float64(cached)*rate.Cached + float64(output)*rate.Output) / r.PerTokens * w
	return charge, basis, ""
}

func newLedgerID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "proxy-" + hex.EncodeToString(b[:])
}

// ledgerEnter keeps the entry (it counts in the budget gate from now on) and
// wakes the reporter, which sends it to the brain (reports.go).
func (tn *tenant) ledgerEnter(e ledgerEntry) {
	tn.ledgerMu.Lock()
	tn.ledgerOwn = append(tn.ledgerOwn, &ownEntry{e: e})
	tn.ledgerMu.Unlock()
	what := "uncounted"
	if e.Charge != nil {
		what = fmt.Sprintf("%.4f %s (%s)", *e.Charge, e.Unit, *e.Basis)
	} else if e.Reason != nil {
		what += ": " + *e.Reason
	}
	log.Printf("ledger: %s on %s route %s %s", e.ID, e.potID(), e.Route, what)
	tn.reportWake()
}

// ledgerPulled drops the entries a pulled ledger covers (accepted at a seq it
// has reached) and, of the rest, the refused ones whose period has ended.
// Caller passes the new ledger.
func (tn *tenant) ledgerPulled(l *checkLedger, now float64) {
	start, _ := budgetPeriod(now)
	tn.ledgerMu.Lock()
	defer tn.ledgerMu.Unlock()
	keep := tn.ledgerOwn[:0]
	for _, o := range tn.ledgerOwn {
		switch {
		case o.seq > 0 && l != nil && l.Seq >= o.seq:
		case o.refused != "" && o.e.At < start:
		default:
			keep = append(keep, o)
		}
	}
	for i := len(keep); i < len(tn.ledgerOwn); i++ {
		tn.ledgerOwn[i] = nil
	}
	tn.ledgerOwn = keep
}

// ledgerView is the ledger the budget gate reads at now: the last pulled one
// with every entry of ours it does not cover added, this period's only, and
// per pot the oldest of those the brain has not accepted (budgetGate). nil
// when no ledger was ever pulled (a budgeted pot then reads unknown). A pulled
// ledger of an earlier period has spent nothing of this one.
//
// The ledger and our entries are read under ledgerMu together: a pull swaps
// pref.ledger and then drops what the new one covers (ledgerPulled, which
// takes ledgerMu), so read apart, an older ledger could meet the entries a
// newer one covers already dropped, and spend would vanish from one check.
// Lock order: ledgerMu, then pref.mu. Every hold in flight is added to its
// pot's reserved and its route's (ledgerReserve).
func (tn *tenant) ledgerView(now float64) *checkLedger {
	tn.ledgerMu.Lock()
	defer tn.ledgerMu.Unlock()
	return tn.ledgerViewLocked(now)
}

// ledgerReserve is the dial check of (route, account) for req, on the ledger
// view, and when it admits a paid dial the hold of req's max_cost on the
// drawn pot, both under one ledgerMu: two dials in flight cannot both take
// the room one budget has left (§1.7: spend + reserved + max_cost <=
// budget). release ends the hold, once the dial's entry is made; a no-op
// when nothing was held. Lock order: ledgerMu, then pref.mu and recMu
// (routeCheckOn), as ledgerView.
func (tn *tenant) ledgerReserve(routeID, account, model string, req checkRequest, now time.Time) (dialCheck, func()) {
	c, _, release := tn.ledgerReserveBy(routeID, req, now, func(l *checkLedger) dialCheck {
		c, _, _ := tn.routeCheckOn(routeID, account, model, req, l, now)
		return c
	})
	return c, release
}

// ledgerReserveBy is ledgerReserve with the check made by check, on the
// ledger view, under ledgerMu (lock order as ledgerReserve's). It also
// returns the hold (nil when nothing was held), which ledgerHoldTo may
// lower once the upstream names its own maximum.
func (tn *tenant) ledgerReserveBy(routeID string, req checkRequest, now time.Time, check func(*checkLedger) dialCheck) (dialCheck, *ledgerHold, func()) {
	tn.ledgerMu.Lock()
	defer tn.ledgerMu.Unlock()
	c := check(tn.ledgerViewLocked(unixSeconds(now)))
	if !c.paid() || req.MaxCost == nil {
		return c, nil, func() {}
	}
	h := &ledgerHold{pot: c.Pot, route: routeID, amount: *req.MaxCost}
	tn.ledgerHolds[h] = struct{}{}
	return c, h, func() { tn.ledgerRelease(h) }
}

// ledgerHoldOn holds amount on pot for route with no check: a task that
// already exists, restored after a restart (video_api.go), whose charge is
// still to be entered. release ends it.
func (tn *tenant) ledgerHoldOn(pot, route string, amount float64) (h *ledgerHold, release func()) {
	h = &ledgerHold{pot: pot, route: route, amount: amount}
	tn.ledgerMu.Lock()
	tn.ledgerHolds[h] = struct{}{}
	tn.ledgerMu.Unlock()
	return h, func() { tn.ledgerRelease(h) }
}

// ledgerHoldTo lowers a hold to amount (never raises it): the upstream's
// own maximum for a task, once known, replaces the price-table bound.
func (tn *tenant) ledgerHoldTo(h *ledgerHold, amount float64) {
	if h == nil {
		return
	}
	tn.ledgerMu.Lock()
	defer tn.ledgerMu.Unlock()
	if amount >= 0 && amount < h.amount {
		h.amount = amount
	}
}

func (tn *tenant) ledgerRelease(h *ledgerHold) {
	tn.ledgerMu.Lock()
	delete(tn.ledgerHolds, h)
	tn.ledgerMu.Unlock()
}

// ledgerViewLocked is ledgerView; the caller holds ledgerMu.
func (tn *tenant) ledgerViewLocked(now float64) *checkLedger {
	tn.pref.mu.RLock()
	l := tn.pref.ledger
	tn.pref.mu.RUnlock()
	if l == nil {
		return nil
	}
	start, end := budgetPeriod(now)
	v := *l
	v.Pots = make(map[string]ledgerPot, len(l.Pots))
	for id, p := range l.Pots {
		q := ledgerPot{Unit: p.Unit, Routes: make(map[string]float64, len(p.Routes)), Unpriced: map[string]int{}}
		if l.Period.Start == start { // else it has spent nothing of this period
			q.PeriodSpend = p.PeriodSpend
			for r, s := range p.Routes {
				q.Routes[r] = s
			}
			for r, n := range p.Unpriced {
				q.Unpriced[r] = n
			}
		}
		v.Pots[id] = q
	}
	v.Period.Start, v.Period.End = start, end
	v.unposted = map[string]float64{}
	for _, o := range tn.ledgerOwn {
		e := o.e
		if e.Charge == nil || e.At < start || e.At >= end || (o.seq > 0 && l.Seq >= o.seq) {
			continue
		}
		if at, ok := v.unposted[e.potID()]; o.seq == 0 && (!ok || e.At < at) {
			v.unposted[e.potID()] = e.At
		}
		p := v.Pots[e.potID()]
		if p.Routes == nil {
			p.Routes, p.Unpriced = map[string]float64{}, map[string]int{}
		}
		p.PeriodSpend += *e.Charge
		p.Routes[e.Route] += *e.Charge
		if e.Basis != nil && *e.Basis == "lower_bound" {
			p.Unpriced[e.Route]++
		}
		v.Pots[e.potID()] = p
	}
	for h := range tn.ledgerHolds {
		p := v.Pots[h.pot]
		if p.RoutesReserved == nil {
			p.RoutesReserved = map[string]float64{}
		}
		p.Reserved += h.amount
		p.RoutesReserved[h.route] += h.amount
		v.Pots[h.pot] = p
	}
	return &v
}

// ledgerStatus is /_switch/status's ledger block: what of ours the brain has
// not counted yet, and the last post's failure.
func (tn *tenant) ledgerStatus() map[string]any {
	tn.pref.mu.RLock()
	l := tn.pref.ledger
	tn.pref.mu.RUnlock()
	tn.ledgerMu.Lock()
	defer tn.ledgerMu.Unlock()
	var pending, unseen, refused int
	for _, o := range tn.ledgerOwn {
		switch {
		case o.refused != "":
			refused++
		case o.seq == 0:
			pending++
		default:
			unseen++
		}
	}
	out := map[string]any{"pending": pending, "accepted_unpulled": unseen, "refused": refused,
		"last_error": tn.ledgerLastErr, "pulled_seq": nil, "pulled_as_of": nil}
	if l != nil {
		out["pulled_seq"], out["pulled_as_of"] = l.Seq, l.AsOf
		spend := map[string]float64{}
		ids := make([]string, 0, len(l.Pots))
		for id := range l.Pots {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			spend[id] = l.Pots[id].PeriodSpend
		}
		out["period_spend"] = spend
	}
	return out
}
