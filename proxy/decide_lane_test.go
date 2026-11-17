package main

// Tests for the per-lane routing decision (decideRoute, §2.2 row 18): split
// routing, the brain's pick under its check, account-wide vs scoped records,
// stop_at, pay and topup on a bare lane, the throttle derank, the manual
// inject file, count_tokens, and per-lane switch detection/dedup.

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// decide is decideRoute on the bare lane's route, in the shape these tests read.
func decide(lane string) (tok, eff, reason string, state routeState) {
	d := lan.decideRoute(laneRouteID(lane), lane, "", false)
	return d.tok, d.acct, d.reason, d.state
}

// laneWorld seeds a lane route per lane (fable scoped "Fable") over both
// accounts, each measured now with the windows given (nil: open at 40 %).
func laneWorld(t *testing.T, lanes []string, st map[string]acctStanding) {
	t.Helper()
	now := nowFn()
	open := standingAt("anthropic", now, map[string]checkWindow{
		"seven_day": week(40, now.Add(48*time.Hour)), "scoped:Fable": scoped("Fable", 40, now.Add(48*time.Hour))}, "")
	accounts := map[string]acctStanding{native: open, other: open}
	for a, s := range st {
		accounts[a] = s
	}
	routes := map[string]routePref{}
	for _, l := range lanes {
		scope := ""
		if l == "fable" {
			scope = "Fable"
		}
		routes[laneRouteID(l)] = laneRoute(l, scope, native, other)
	}
	seedQuota(t, accounts, routes, nil, nil)
}

var laneWorldLanes = []string{"fable", "opus", "haiku", "default", "anything"}

// withLaneState installs a brain decision + token cache and returns a restore func.
func withLaneState(t *testing.T, account string, lanes map[string]lanePref, rules []laneRule) func() {
	t.Helper()
	lan.pref.mu.Lock()
	oldA, oldR, oldW, oldRules, oldLanes, oldOK := lan.pref.account, lan.pref.reason, lan.pref.weekly, lan.pref.laneRules, lan.pref.lanes, lan.pref.ok
	lan.pref.account, lan.pref.reason, lan.pref.ok = account, "test", true
	lan.pref.laneRules, lan.pref.lanes = rules, lanes
	lan.pref.mu.Unlock()

	// Every account gets a cached token — the proxy always injects the chosen account's
	// bearer (no account is passthrough anymore).
	lan.tokMu.Lock()
	oldTokens := lan.tokens
	lan.tokens = map[string]tokEntry{
		native: {token: "tok-native", exp: time.Now().Add(time.Hour)},
		other:  {token: "tok-other", exp: time.Now().Add(time.Hour)},
	}
	lan.tokMu.Unlock()

	laneWorld(t, laneWorldLanes, nil)

	lan.effMu.Lock()
	oldEff, oldState := lan.lastEffective, lan.lastState
	lan.lastEffective = map[string]string{}
	lan.lastState = map[string]routeState{}
	lan.effMu.Unlock()

	oldBridge := swapBridgeURL("") // alerts log-only in these tests

	return func() {
		lan.pref.mu.Lock()
		lan.pref.account, lan.pref.reason, lan.pref.weekly, lan.pref.laneRules, lan.pref.lanes, lan.pref.ok = oldA, oldR, oldW, oldRules, oldLanes, oldOK
		lan.pref.mu.Unlock()
		lan.tokMu.Lock()
		lan.tokens = oldTokens
		lan.tokMu.Unlock()
		lan.effMu.Lock()
		lan.lastEffective, lan.lastState = oldEff, oldState
		lan.effMu.Unlock()
		swapBridgeURL(oldBridge)
	}
}

// The point of the whole build: two lanes on two accounts AT THE SAME TIME. BOTH lanes
// inject their chosen account's token — neither rides the client's ambient login.
func TestSplitRouting(t *testing.T) {
	restore := withLaneState(t, native, map[string]lanePref{
		"fable":   {Account: other, Reason: "fable cap hot"},
		"haiku":   {Account: native, Reason: "incumbent"},
		"default": {Account: native, Reason: "incumbent"},
	}, testRules)
	defer restore()

	tok, eff, _, state := decide("fable")
	if eff != other || tok != "tok-other" || state != stateRouted {
		t.Errorf("fable must ROUTE-inject on %s, got eff=%q tok=%q state=%v", other, eff, tok, state)
	}
	tok, eff, _, state = decide("haiku")
	if eff != native || tok != "tok-native" || state != stateRouted {
		t.Errorf("haiku must ROUTE-inject on %s concurrently (no passthrough), got eff=%q tok=%q state=%v", native, eff, tok, state)
	}
}

// A brain with no per-lane picks: every lane dials the brain's account,
// injecting its token, while that account's check admits it.
func TestSingleLaneFallback(t *testing.T) {
	restore := withLaneState(t, other, nil, nil)
	defer restore()

	for _, lane := range []string{"default", "fable", "anything"} {
		tok, eff, _, state := decide(lane)
		if eff != other || tok != "tok-other" || state != stateRouted {
			t.Errorf("lane %q with a lane-less brain must inject the legacy account, got eff=%q tok=%q state=%v", lane, eff, tok, state)
		}
	}
}

// A lane the rules name but the lanes map lacks dials the brain's account,
// injecting its token.
func TestUnknownLaneFallsBack(t *testing.T) {
	restore := withLaneState(t, native, map[string]lanePref{
		"fable": {Account: other},
	}, testRules)
	defer restore()

	tok, eff, _, state := decide("opus") // in rules, absent from lanes map
	if eff != native || tok != "tok-native" || state != stateRouted {
		t.Errorf("lane missing from the brain's lanes map must inject the legacy account, got eff=%q tok=%q state=%v", eff, tok, state)
	}
}

// A record on an account-wide window covers every route on the account; a
// scoped one (scoped:Fable) only the routes of that scope (§2.6).
func TestRecordScopes(t *testing.T) {
	restore := withLaneState(t, native, nil, nil)
	defer restore()
	now := nowFn()
	no := func(lane, acct string) bool {
		c, _ := lan.routeCheck(laneRouteID(lane), acct, "", now)
		return c.no()
	}

	seedSpent(other, "seven_day", 429, "weekly limit", time.Time{})
	if !no("fable", other) || !no("haiku", other) || !no("default", other) {
		t.Error("an account-wide record must close every route on the account")
	}
	if no("haiku", native) {
		t.Error("a record on one account must not touch another")
	}

	t.Cleanup(resetQuotaState())
	laneWorld(t, laneWorldLanes, nil)
	seedSpent(other, "scoped:Fable", 429, "Fable limit", time.Time{})
	if !no("fable", other) {
		t.Error("a scoped record must close its own scope's route")
	}
	if no("haiku", other) || no("opus", other) {
		t.Error("a scoped record must NOT leak to routes of no scope")
	}
}

// A lane whose pick is spent steers to the route's other account at decide()
// time (§2.2 row 9) — INJECTING that account's token, never falling back to the
// client's ambient bearer.
func TestDecideSteersAroundLaneCooldown(t *testing.T) {
	restore := withLaneState(t, native, map[string]lanePref{
		"fable": {Account: other},
	}, testRules)
	defer restore()
	lan.pref.mu.Lock()
	lan.pref.weekly = map[string]float64{native: 50, other: 60} // failoverFor consults these
	lan.pref.mu.Unlock()

	seedSpent(other, "scoped:Fable", 429, "Fable limit", time.Time{})
	tok, eff, reason, state := decide("fable")
	if eff != native || tok != "tok-native" || state != stateRouted {
		t.Errorf("fable must fail over by INJECTING %s while %s is spent, got eff=%q tok=%q state=%v (%s)",
			native, other, eff, tok, state, reason)
	}
	if !strings.Contains(reason, "avoiding "+other) || !strings.Contains(reason, "no(spent") {
		t.Errorf("reason = %q, want it to name the pick and its check", reason)
	}
}

// Both accounts spent: the lane refuses — it dials nobody (§2.2 row 18).
func TestDecideRefusesWhenEveryAccountIsNo(t *testing.T) {
	restore := withLaneState(t, native, nil, testRules)
	defer restore()
	seedSpent(native, "seven_day", 429, "weekly limit", time.Time{})
	seedSpent(other, "seven_day", 429, "weekly limit", time.Time{})
	d := lan.decideRoute(laneRouteID("opus"), "opus", "", false)
	if !d.none || d.tok != "" || len(d.refused) != 2 {
		t.Fatalf("decision = %+v, want a refusal naming both accounts", d)
	}
}

// When a refused account is dialled again (§2.6): not while its record lives,
// and again as soon as it ends — at the floor when the upstream named no
// reset, at its reset when it named one.
func TestRefusedPickRedialledWhenItsRecordEnds(t *testing.T) {
	c := testClock(t)
	restore := withLaneState(t, native, nil, testRules)
	defer restore()
	seedSpent(native, "seven_day", 429, "weekly limit", time.Time{})
	if _, eff, _, _ := decide("opus"); eff != other {
		t.Fatalf("opus dialled %q with its pick spent, want %s", eff, other)
	}
	c.advance(quotaCooldown - time.Second)
	if _, eff, _, _ := decide("opus"); eff != other {
		t.Fatalf("opus dialled %q a second before the floor", eff)
	}
	c.advance(2 * time.Second)
	if _, eff, _, _ := decide("opus"); eff != native {
		t.Fatalf("opus dialled %q after the floor, want the pick back", eff)
	}

	seedSpent(native, "seven_day", 429, "weekly limit", c.now().Add(2*time.Hour))
	c.advance(time.Hour)
	if _, eff, _, _ := decide("opus"); eff != other {
		t.Fatalf("opus dialled %q before the named reset", eff)
	}
	c.advance(time.Hour + time.Second)
	if _, eff, _, _ := decide("opus"); eff != native {
		t.Fatalf("opus dialled %q after the named reset", eff)
	}
}

// stop_at on a bare lane refuses an account at the owner's level on the
// lane's own window, and only on that lane (§1.9, §2.4).
func TestLaneStopAt(t *testing.T) {
	restore := withLaneState(t, native, nil, testRules)
	defer restore()
	now := nowFn()
	laneWorld(t, laneWorldLanes, map[string]acctStanding{
		native: standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(70, now.Add(48*time.Hour))}, ""),
	})
	lan.pref.mu.Lock()
	r := lan.pref.routes["lane:opus"]
	r.StopAt = &stopAt{Window: "7d", Level: 60}
	lan.pref.routes["lane:opus"] = r
	lan.pref.mu.Unlock()

	d := lan.decideRoute("lane:opus", "opus", "", false)
	if d.acct != other || d.check.Check != checkIncluded {
		t.Fatalf("opus = %+v, want %s: %s is over the lane's stop_at", d, other, native)
	}
	if c, _ := lan.routeCheck("lane:opus", native, "", now); c.Reason != "stop_at" {
		t.Fatalf("native on opus = %s, want no(stop_at)", c)
	}
	if _, eff, _, _ := decide("haiku"); eff != native {
		t.Fatalf("haiku dialled %q: stop_at must bind its own route only", eff)
	}
}

// pay on a bare lane: an account whose allowance is closed is dialled on its
// credits only when the lane's pay names the pot, after every included
// account; topup decides when an empty pot's `no` ends (§1.9, §2.2 row 18).
func TestLanePayAndTopup(t *testing.T) {
	restore := withLaneState(t, native, nil, testRules)
	defer restore()
	now := nowFn()
	reset := now.Add(48 * time.Hour)
	spentFunded := standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(100, reset)}, "open")
	laneWorld(t, laneWorldLanes, map[string]acctStanding{native: spentFunded, other: spentFunded})

	// pay unset ([]): both refused, until the window resets.
	d := lan.decideRoute("lane:opus", "opus", "", false)
	if !d.none || len(d.refused) != 2 || d.refused[0].check.Reason != "pay" {
		t.Fatalf("pay []: %+v, want both refused for pay", d)
	}
	if got := d.refused[0].check.Until.Time(); !got.Equal(time.Unix(0, int64(unixSeconds(reset)*1e9))) {
		t.Fatalf("pay []: until %v, want the window reset %v", got, reset)
	}

	// pay names other's pot: other is dialled, paid; native stays refused.
	lan.pref.mu.Lock()
	r := lan.pref.routes["lane:opus"]
	r.Pay = []string{other + "/credits"}
	lan.pref.routes["lane:opus"] = r
	lan.pref.mu.Unlock()
	d = lan.decideRoute("lane:opus", "opus", "", false)
	if d.acct != other || d.check.Check != checkPaid || d.check.Pot != other+"/credits" {
		t.Fatalf("pay [other]: %+v, want %s paid", d, other)
	}

	// An included account still goes first, even when it is not the pick.
	lan.pref.mu.Lock()
	lan.pref.accounts[native] = standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(50, reset)}, "open")
	lan.pref.lanes = map[string]lanePref{"opus": {Account: other}}
	lan.pref.mu.Unlock()
	if d = lan.decideRoute("lane:opus", "opus", "", false); d.acct != native || !d.check.included() {
		t.Fatalf("included before paid: %+v, want %s included", d, native)
	}

	// The pot runs dry: topup off waits for the window; topup on asks again at
	// the brain's next reading.
	empty := standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(100, reset)}, "closed")
	next := now.Add(5 * time.Minute)
	lan.pref.mu.Lock()
	lan.pref.accounts[native] = spentFunded
	lan.pref.accounts[other] = empty
	lan.pref.mu.Unlock()
	c, _ := lan.routeCheck("lane:opus", other, "", now)
	if c.Reason != "spent" || !c.Until.Time().Equal(time.Unix(0, int64(unixSeconds(reset)*1e9))) {
		t.Fatalf("topup off: %s, want no(spent) until the window reset", c)
	}
	lan.pref.mu.Lock()
	lan.pref.pots = map[string]potSettings{other + "/credits": {Topup: "on"}}
	lan.pref.mu.Unlock()
	c, _ = lan.routeCheck("lane:opus", other, "", now)
	if c.Reason != "spent" || !c.Until.Time().Equal(time.Unix(0, int64(unixSeconds(next)*1e9))) {
		t.Fatalf("topup on: %s, want no(spent) until the next reading %v", c, next)
	}
}

// A throttle deranks the pick; it never refuses it (§2.3).
func TestLaneThrottleDeranks(t *testing.T) {
	restore := withLaneState(t, native, nil, testRules)
	defer restore()
	lan.recordThrottle(native, 429, "slow down", http.Header{"Retry-After": {"30"}})
	d := lan.decideRoute("lane:opus", "opus", "", false)
	if d.acct != other || !d.check.included() {
		t.Fatalf("throttled pick: %+v, want %s", d, other)
	}
	// Throttled everywhere: still dialled — a throttle is not a `no`.
	lan.recordThrottle(other, 429, "slow down", http.Header{"Retry-After": {"30"}})
	if d = lan.decideRoute("lane:opus", "opus", "", false); d.none || d.tok == "" {
		t.Fatalf("throttled everywhere: %+v, want a dial", d)
	}
}

// injectToken points the manual inject file at tok for the test.
func injectToken(t *testing.T, tok string) {
	t.Helper()
	f := filepath.Join(t.TempDir(), "inject")
	if err := os.WriteFile(f, []byte(tok+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := injectFile
	injectFile = f
	t.Cleanup(func() { injectFile = old })
}

// The manual inject file passes the check of the account whose token it holds
// and never grants pay: it serves only while that account's check is included.
func TestManualInjectPassesTheCheck(t *testing.T) {
	restore := withLaneState(t, native, nil, testRules)
	defer restore()
	injectToken(t, "tok-native")

	if d := lan.decideRoute("lane:opus", "opus", "", false); d.tok != "tok-native" || !d.check.included() {
		t.Fatalf("included: %+v, want the manual token", d)
	}
	now := nowFn()
	lan.pref.mu.Lock()
	lan.pref.accounts[native] = standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(100, now.Add(time.Hour))}, "open")
	r := lan.pref.routes["lane:opus"]
	r.Pay = []string{native + "/credits"}
	lan.pref.routes["lane:opus"] = r
	lan.pref.mu.Unlock()
	d := lan.decideRoute("lane:opus", "opus", "", false)
	if d.tok != "" || !d.none || !strings.Contains(d.reason, "paid") {
		t.Fatalf("paid: %+v, want a refusal — manual inject never spends money", d)
	}
	// count_tokens is free: the manual token answers it whatever the check.
	if d = lan.decideRoute("lane:opus", "opus", "", true); d.tok != "tok-native" {
		t.Fatalf("count_tokens: %+v, want the manual token", d)
	}
}

// The check is the injected account's, not the brain's pick: an inject of a
// spent account is refused while the pick is included, and a token no account
// of this proxy holds has no reading at all.
func TestManualInjectChecksTheInjectedAccount(t *testing.T) {
	restore := withLaneState(t, native, nil, testRules)
	defer restore()
	now := nowFn()
	lan.pref.mu.Lock()
	lan.pref.accounts[other] = standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(100, now.Add(time.Hour))}, "closed")
	lan.pref.mu.Unlock()
	if c, _ := lan.routeCheck("lane:opus", native, "", now); !c.included() {
		t.Fatalf("fixture: the pick is %s, want included", c)
	}
	injectToken(t, "tok-other")
	if d := lan.decideRoute("lane:opus", "opus", "", false); d.tok != "" || !d.none || !strings.Contains(d.reason, other) {
		t.Fatalf("inject of a spent account: %+v, want a refusal naming %s", d, other)
	}
	injectToken(t, "tok-nobody")
	if d := lan.decideRoute("lane:opus", "opus", "", false); d.tok != "" || !d.none {
		t.Fatalf("inject of an unknown token: %+v, want a refusal", d)
	}
}

// count_tokens is an entitlement (§2.2 row 20): every account backing the
// route answers it, the pick first, whatever its check.
func TestCountTokensIgnoresTheCheck(t *testing.T) {
	restore := withLaneState(t, native, nil, testRules)
	defer restore()
	seedSpent(native, "seven_day", 429, "weekly limit", time.Time{})
	seedSpent(other, "seven_day", 429, "weekly limit", time.Time{})
	d := lan.decideRoute("lane:opus", "opus", "", true)
	if d.none || d.acct != native || d.tok != "tok-native" {
		t.Fatalf("count_tokens: %+v, want the pick, spent or not", d)
	}
}

// THE REGRESSION. The client is /login'd as `other` (its ambient bearer) but the brain
// prefers `native`. The proxy MUST inject native's token — the old code passed through
// the client's ambient login (silently riding `other`, the maxed account). This is the
// bug that made the whole switchover invisible.
func TestRoutesToBrainAccountNotAmbientLogin(t *testing.T) {
	restore := withLaneState(t, native, map[string]lanePref{
		"default": {Account: native, Reason: "incumbent"},
	}, testRules)
	defer restore()

	tok, eff, _, state := decide("default")
	if tok != "tok-native" || eff != native || state != stateRouted {
		t.Errorf("must INJECT the brain's account regardless of the client's ambient login, "+
			"got eff=%q tok=%q state=%v", eff, tok, state)
	}
}

// Brain unreachable but a still-valid cached token exists -> STALE: keep executing the
// last-good decision (inject the cached token), surfaced as stale — not passthrough.
func TestBrainDownWithCachedTokenIsStale(t *testing.T) {
	restore := withLaneState(t, native, map[string]lanePref{
		"default": {Account: native, Reason: "incumbent"},
	}, testRules)
	defer restore()
	lan.pref.mu.Lock()
	lan.pref.ok = false // brain went unreachable; last-good decision + cached token remain
	lan.pref.mu.Unlock()

	tok, eff, _, state := decide("default")
	if tok != "tok-native" || eff != native || state != stateStale {
		t.Errorf("brain-down with a cached token must be STALE inject, got eff=%q tok=%q state=%v", eff, tok, state)
	}
}

// Brain unreachable AND no cached token -> FAILED: no token, so the handler 503s rather
// than forward the client's ambient login.
func TestBrainDownColdIsFailed(t *testing.T) {
	restore := withLaneState(t, native, map[string]lanePref{
		"default": {Account: native, Reason: "incumbent"},
	}, testRules)
	defer restore()
	lan.pref.mu.Lock()
	lan.pref.ok = false
	lan.pref.mu.Unlock()
	lan.tokMu.Lock()
	lan.tokens = map[string]tokEntry{} // cold cache: nothing valid to inject
	lan.tokMu.Unlock()

	tok, _, _, state := decide("default")
	if tok != "" || state != stateFailed {
		t.Errorf("brain-down with no cached token must be FAILED (no token), got tok=%q state=%v", tok, state)
	}
}

// Per-lane switch detection: each lane's first sighting is silent; a change fires
// once for THAT lane only; forceEffective suppresses the follow-up duplicate.
func TestNoteEffectivePerLane(t *testing.T) {
	restore := withLaneState(t, native, nil, nil)
	defer restore()

	st.mu.Lock()
	before := st.switches
	st.mu.Unlock()

	lan.noteEffective("fable", native, "first sighting")    // no alert: no previous
	lan.noteEffective("haiku", native, "first sighting")    // no alert: independent lane
	lan.noteEffective("fable", other, "flip")               // ALERT: fable changed
	lan.noteEffective("haiku", native, "steady")            // no alert: unchanged
	lan.forceEffective("haiku", other)                      // 429 path already alerted
	lan.noteEffective("haiku", other, "would-be duplicate") // no alert: matches forced

	st.mu.Lock()
	delta := st.switches - before
	st.mu.Unlock()
	if delta != 1 {
		t.Errorf("exactly ONE switch must be counted (fable's flip), got %d", delta)
	}
}

// An anthropic class member dials from the account the brain put the member
// on, never from its lane's: a bare lane's stop_at and pay bind lane:<lane>
// alone (§1.9, §2.2 row 16), so the lane's pick can be empty or spent while
// the member's own route serves.
func TestAnthropicMemberDialsFromItsOwnIncumbent(t *testing.T) {
	restore := withLaneState(t, native, map[string]lanePref{"opus": {Account: ""}}, testRules)
	defer restore()
	lan.pref.mu.Lock()
	lan.pref.routes["frontier/lane:opus"] = memberRoute("frontier/lane:opus", "frontier", "lane:opus", "opus", "anthropic", native, other)
	lan.pref.mu.Unlock()
	m := classMemberPref{ID: "lane:opus", Kind: "anthropic", Lane: "opus", Route: "frontier/lane:opus", Incumbent: other}
	if got := lan.memberIncumbent(m); got != other {
		t.Fatalf("memberIncumbent = %q, want the member's own %q", got, other)
	}
	d := lan.anthropicMemberDial(m, false, false)
	if d.acct != other || d.check.Check != checkIncluded || d.state != stateRouted {
		t.Fatalf("member dial = %+v, want %s (the member's incumbent), included", d, other)
	}
}

// An empty pick on a route the brain serves is not "no brain decision": the
// brain names no incumbent only when none of the route's accounts passes, and
// the refusal then says so with each account's check. A route the brain does
// not serve has no decision at all.
func TestEmptyPickOnAKnownRouteNamesTheChecks(t *testing.T) {
	restore := withLaneState(t, native, nil, testRules)
	defer restore()
	seedSpent(native, "seven_day", 429, "weekly limit", time.Now().Add(time.Hour))
	seedSpent(other, "seven_day", 429, "weekly limit", time.Now().Add(time.Hour))

	d := lan.decideOn(laneRouteID("opus"), "opus", "claude-opus-5", "", "", false)
	if !d.none || strings.Contains(d.reason, "no brain decision") ||
		!strings.Contains(d.reason, native) || !strings.Contains(d.reason, other) {
		t.Fatalf("decision = %+v, want a refusal naming both accounts' checks", d)
	}
	if d := lan.decideOn("lane:unserved", "unserved", "m", "", "", false); d.state != stateFailed || d.reason != "no brain decision yet" {
		t.Fatalf("unserved route = %+v, want no brain decision", d)
	}
}
