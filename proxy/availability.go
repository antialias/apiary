package main

// What an Anthropic response says about its account's limits, read by meaning:
// the brain's scope vocabulary (scopeMap), the unified rate-limit headers
// (parseUnifiedRL), the brain's per-target window aliases, and the
// pre-flight's question of one member (memberRoutableNow). The reply a class
// gets when none of its members may be dialled is unavailable.go's (§2.8).
//
// The incident this file began with: a Claude Code session on
// a class got one pooled account's bare 429 while every member was
// spent, read it as "not your usage limit", and nobody learned when anything
// would come back.

import (
	"math"
	"net/http"
	"sort"
	"strings"
	"time"
)

// availabilityClamp bounds how far ahead any reset is believed. The longest real
// window is seven days; a bad value must not park an account for a month.
const availabilityClamp = 8 * 24 * time.Hour

// windowAliases: the header window token (anthropic-ratelimit-unified-<token>-*)
// and the representative-claim a scoped window arrives as on a response, as
// the brain publishes them on each scoped window of accounts{} (null when it
// knows none).
type windowAliases struct {
	Header []string `json:"header"`
	Claim  []string `json:"claim"`
}

// ---- the brain's scope vocabulary (C5) ----

// scopeMap reads Anthropic's window names by meaning. Anthropic names one limit
// three ways: the poll's limits[].scope.model.display_name (the brain's
// "scoped:Fable"), a header window token ("7d_oi") and a representative-claim
// ("seven_day_overage_included"). The brain publishes the joins — each scoped
// window's aliases on the accounts{} standings, each lane's scope — and this
// is them, indexed. Nothing here
// matches a name by substring: a token no window claims stays "unmapped:<tok>",
// binds no lane, and is shown under that name (Fable 429s were once
// dropped for every lane because "fable" is not in "7d_oi").
type scopeMap struct {
	lane   map[string]string // lane -> its scoped window ("fable" -> "scoped:Fable")
	header map[string]string // header token -> scoped window ("7d_oi" -> "scoped:Fable")
	claim  map[string]string // representative-claim -> scoped window
}

// newScopeMap indexes the brain's joins. An alias two windows both claim is
// dropped rather than guessed between. The lane join is required: the brain's
// own scoped:<Name> reading binds a lane only through lane_rules[].scope, so
// the brain that publishes it deploys before this proxy. A window with no
// aliases leaves its header token and claim unmapped.
func newScopeMap(rules []laneRule, accounts map[string]acctStanding) scopeMap {
	sm := scopeMap{lane: map[string]string{}, header: map[string]string{}, claim: map[string]string{}}
	for _, r := range rules {
		if r.Scope != "" {
			sm.lane[r.Lane] = "scoped:" + r.Scope
		}
	}
	clash := map[string]bool{}
	put := func(idx map[string]string, kind, alias, win string) {
		if alias == "" || clash[kind+alias] {
			return
		}
		if prev, had := idx[alias]; had && prev != win {
			delete(idx, alias)
			clash[kind+alias] = true
			return
		}
		idx[alias] = win
	}
	for _, st := range accounts {
		for name, w := range st.Allowance.Windows {
			if w.Aliases == nil || !strings.HasPrefix(name, "scoped:") {
				continue
			}
			for _, a := range w.Aliases.Header {
				put(sm.header, "h:", a, name)
			}
			for _, a := range w.Aliases.Claim {
				put(sm.claim, "c:", a, name)
			}
		}
	}
	return sm
}

// window names a header window token by meaning.
func (sm scopeMap) window(tok string) string {
	switch tok {
	case "5h":
		return "five_hour"
	case "7d":
		return "seven_day"
	}
	if w := sm.header[tok]; w != "" {
		return w
	}
	return "unmapped:" + tok
}

// claimWindow names the window a representative-claim points at. No claim is
// the weekly window, as before.
func (sm scopeMap) claimWindow(claim string) string {
	switch claim {
	case "", "seven_day":
		return "seven_day"
	case "five_hour":
		return "five_hour"
	}
	if w := sm.claim[claim]; w != "" {
		return w
	}
	return "unmapped:" + claim
}

// binds: the 5h and 7d windows bind every lane; a scoped window binds the lane
// whose scope the brain says it is; an unmapped window binds none.
func (sm scopeMap) binds(name, lane string) bool {
	switch name {
	case "five_hour", "seven_day":
		return true
	}
	return lane != "" && sm.lane[lane] == name
}

// currentScopes is the scope map of the brain's last /preferred.
func currentScopes() scopeMap {
	pref.mu.RLock()
	defer pref.mu.RUnlock()
	return newScopeMap(pref.laneRules, pref.accounts)
}

// ---- an Anthropic response's unified rate-limit headers ----

// rlWindow is one anthropic-ratelimit-unified-<window>-* triple from a response.
type rlWindow struct {
	token  string    // the header token: "5h", "7d", "7d_oi", ...
	name   string    // the window it is, by meaning: five_hour | seven_day | scoped:<x> | unmapped:<token>
	status string    // allowed | allowed_warning | rejected | ""
	pct    float64   // utilization as a percent; -1 when absent
	reset  time.Time // zero when absent
}

// rlPot is the usage-credits state the overage-* headers carry, every one of
// them, keyed by what follows "overage-" ("status", "disabled-reason", ...). It
// is the money pot's, not a window: "rejected" with org_level_disabled is
// credits switched off, not a limit that resets (quota-model 10 §2).
type rlPot map[string]string

// acctObs is what one Anthropic response said: the overall verdict and its
// claim, every window, and the credits pot.
type acctObs struct {
	observed  time.Time
	status    string
	reset     time.Time
	claim     string // anthropic-ratelimit-unified-representative-claim, as sent
	claimName string // the window it names, by meaning
	windows   []rlWindow
	credits   rlPot
}

// rejectedWindows: every window a refusal names. A per-window rejected status
// names its own; unified-status=rejected with unified-reset is Anthropic's
// explicit "spent until T" on the claimed window, which counts alongside them
// (a claim of seven_day with only 7d_oi marked rejected is still the 7d).
func (o acctObs) rejectedWindows() []string {
	var out []string
	add := func(name string) {
		for _, n := range out {
			if n == name {
				return
			}
		}
		out = append(out, name)
	}
	for _, w := range o.windows {
		if strings.EqualFold(w.status, "rejected") {
			add(w.name)
		}
	}
	if strings.EqualFold(o.status, "rejected") {
		add(o.claimName)
	}
	return out
}

// parseUnifiedRL reads the anthropic-ratelimit-unified-* set off a response,
// naming each window and the claim by meaning through sm. Every overage-* field
// is the credits pot's state and is kept as that; fallback-* is not a window
// (its meaning is unknown, quota-model 10 U5). ok is false when the response
// carried no rate-limit state — a throttle, a shape rejection, or a
// non-Anthropic upstream. The pot alone is not rate-limit state: an
// overage-only 429 is a throttle and leaves no spent record.
func parseUnifiedRL(h http.Header, now time.Time, sm scopeMap) (acctObs, bool) {
	o := acctObs{observed: now}
	byName := map[string]*rlWindow{}
	var order []string
	found := false
	for k, vs := range h {
		lk := strings.ToLower(k)
		if !strings.HasPrefix(lk, hpUnifiedRLPrefix) || len(vs) == 0 {
			continue
		}
		v := strings.TrimSpace(vs[0])
		rest := strings.TrimPrefix(lk, hpUnifiedRLPrefix)
		switch rest {
		case "status":
			o.status, found = v, true
			continue
		case "reset":
			o.reset = clampReset(parseResetValue(v, now), now)
			found = true
			continue
		case "representative-claim":
			o.claim, found = v, true
			continue
		}
		if f, isPot := strings.CutPrefix(rest, "overage-"); isPot {
			if o.credits == nil {
				o.credits = rlPot{}
			}
			o.credits[f] = v
			continue
		}
		i := strings.LastIndex(rest, "-")
		if i <= 0 {
			continue
		}
		win, field := rest[:i], rest[i+1:]
		if strings.HasPrefix(win, "fallback") {
			continue
		}
		if field != "status" && field != "reset" && field != "utilization" {
			continue
		}
		w := byName[win]
		if w == nil {
			w = &rlWindow{token: win, name: sm.window(win), pct: -1}
			byName[win] = w
			order = append(order, win)
		}
		switch field {
		case "status":
			w.status = v
		case "reset":
			w.reset = clampReset(parseResetValue(v, now), now)
		case "utilization":
			if f, ok := parseFiniteFloat(v); ok && f >= 0 {
				// Anthropic sends a fraction (0.82). Tolerate a percent too.
				// Rounded to 4 decimals: 0.07*100 is 7.000000000000001, which
				// the overlay, records and the news filter would all read as
				// differing from the brain's 7.0.
				if f <= 1 {
					f *= 100
				}
				w.pct = math.Round(f*1e4) / 1e4
			}
		}
		found = true
	}
	o.claimName = sm.claimWindow(o.claim)
	sort.Strings(order)
	for _, n := range order {
		o.windows = append(o.windows, *byName[n])
	}
	return o, found
}

// accountByBearer maps an injected Anthropic bearer back to its account (the
// transport only has the request in hand). "" when it is not one of ours.
func accountByBearer(auth string) string {
	tok := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	if tok == "" {
		return ""
	}
	tokMu.Lock()
	defer tokMu.Unlock()
	for a, e := range tokens {
		if e.token == tok {
			return a
		}
	}
	return ""
}

// servedLaneKey carries, on a hop the class walk rebuilt onto an Anthropic
// member, that member's lane (rebuildForMember leaves infoKey alone there).
type servedLaneKey struct{}

// servedLane is the lane of the hop that actually served: the walk's stamp when
// it rebuilt the hop, else the request's own info (the bare lane, class
// attempt 1, and same-lane account retries, which all carry the right one).
func servedLane(hop *http.Request, info reqInfo) string {
	if l, ok := hop.Context().Value(servedLaneKey{}).(string); ok && l != "" {
		return l
	}
	return info.lane
}

// laneScope is the scoped cap a lane draws ("Fable"), "" for none.
func laneScope(lane string) string {
	pref.mu.RLock()
	defer pref.mu.RUnlock()
	for _, r := range pref.laneRules {
		if r.Lane == lane {
			return r.Scope
		}
	}
	return ""
}

// ---- the pre-flight's question ----

// windowLabel names a window for a human: 5h, 7d, a scope's own name.
func windowLabel(name string) string {
	switch name {
	case "five_hour":
		return "5h"
	case "seven_day":
		return "7d"
	}
	if tok, ok := strings.CutPrefix(name, "unmapped:"); ok {
		return tok + " (unmapped)"
	}
	return strings.TrimPrefix(name, "scoped:")
}

// clampReset bounds a reset at the moment it is stored: no window is longer
// than a week, so anything past observed+8d is a bad value. Clamping against
// the observation time (not against "now" at read time) is what lets a bad
// value expire instead of parking the account forever.
func clampReset(t, observed time.Time) time.Time {
	if t.IsZero() || observed.IsZero() {
		return t
	}
	if lim := observed.Add(availabilityClamp); t.After(lim) {
		return lim
	}
	return t
}

// memberRoutableNow is the pre-flight's question for one member: would the
// walk get a real attempt out of it right now? Some account of its route has
// a check that is not `no` (§2.2 row 5).
func memberRoutableNow(m classMemberPref, now time.Time) bool {
	if m.Kind == "provider" && providerByName(m.Provider) == nil {
		return false
	}
	ok, _ := memberPicks(m, now)
	return len(ok) > 0
}

// hpUnifiedRLPrefix is the prefix of Anthropic's unified rate-limit headers.
const hpUnifiedRLPrefix = "anthropic-ratelimit-unified-"
