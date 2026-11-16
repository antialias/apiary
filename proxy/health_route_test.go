package main

// #123 unit suite: §5.2's routing verdict (decideHealthRoute), §5.4's
// synthetic 529, the ranker's health tiers (PR9/PR10/PR11), the would-act
// clauses (§5.5 N24), and the registry roster sync (#115). Everything runs on
// the fake clock and a fresh store; quota globals are perturbed only through
// per-test unique class/member ids so nothing leaks between tests.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// uMember is a wire-free fixture member: anthropic kind dodges the provider
// registry (empty in unit tests), so admissibility is purely its route's dial
// check (uRoute: included until a record says otherwise) + health.
func uMember(id string) classMemberPref {
	return classMemberPref{ID: id, Kind: "anthropic", Lane: "l-" + id,
		Model: "m-" + id, ContextWindow: 200000, Route: uRoute(id)}
}

// tripK drives an arbitrary key OPEN (trip() is pinned to tKey).
func tripK(t *testing.T, h *healthStore, c *fakeClock, key string) {
	t.Helper()
	h.record(key, okS(uniq("ok")))
	c.advance(6 * time.Second)
	for i := 0; i < 3; i++ {
		h.record(key, degS(uniq("deg"), uint64(301+i)))
		c.advance(6 * time.Second)
	}
	mustState(t, h, key, hsOpen)
}

// halfOpenK trips key then ticks it into HALF_OPEN.
func halfOpenK(t *testing.T, h *healthStore, c *fakeClock, key string) {
	t.Helper()
	tripK(t, h, c, key)
	c.advance(61 * time.Second)
	h.tick()
	mustState(t, h, key, hsHalfOpen)
}

func wouldActOf(h *healthStore, class string) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.wouldAct[class]
}

// ---- decideHealthRoute: §5.1's precedence, one arm per test ----

// E19: healthy bound member, no probe token up, no ramp — the verdict is the
// bound member with ZERO ranker calls (the health layer adds no work).
func TestRouteFastPathHealthySkipsRanking(t *testing.T) {
	testClock(t)
	h := newHS()
	armHealth(t, h)
	a, b := uMember(uniq("fp-a")), uMember(uniq("fp-b"))
	before := rankCalls.Load()
	hl := decideHealthRoute(context.Background(), uniq("cls"), "s1", a, []classMemberPref{a, b}, []byte(`{}`), false, "r-1", false)
	if hl.m.ID != a.ID || hl.hopKind != "bound" || hl.lease != nil || hl.refuse != "" || hl.exhaust {
		t.Fatalf("fast path verdict = %+v, want bound %s", hl, a.ID)
	}
	if d := rankCalls.Load() - before; d != 0 {
		t.Fatalf("fast path ranked %d times, want 0 (E19)", d)
	}
}

// PR1: a pin is never detoured — even with the bound key OPEN and a sibling
// half-open, the verdict is the pinned member. When the PIN ITSELF lands on
// the half-open key, the hop adopts the probe role.
func TestRoutePinnedNeverPreempted(t *testing.T) {
	c := testClock(t)
	h := newHS()
	armHealth(t, h)
	class := uniq("cls")
	a, b := uMember(uniq("pin-a")), uMember(uniq("pin-b"))
	halfOpenK(t, h, c, b.ID)
	tripK(t, h, c, a.ID)

	hl := decideHealthRoute(context.Background(), class, "s1", a, []classMemberPref{a, b}, []byte(`{}`), false, "r-1", true)
	if hl.m.ID != a.ID || hl.hopKind != "pinned" || hl.lease != nil {
		t.Fatalf("pinned verdict = kind=%s m=%s lease=%v, want pinned/%s/nil", hl.hopKind, hl.m.ID, hl.lease, a.ID)
	}

	// The pin IS the half-open member: it carries the probe through.
	hl = decideHealthRoute(context.Background(), class, "s1", b, []classMemberPref{a, b}, []byte(`{}`), false, "r-2", true)
	if hl.m.ID != b.ID || hl.hopKind != "probe" || hl.lease == nil {
		t.Fatalf("pinned-on-half-open verdict = kind=%s lease=%v, want probe with a lease", hl.hopKind, hl.lease)
	}
}

// PR2: the first request to catch a HALF_OPEN member's token carries the
// probe, pre-empting the election; the next request finds the token gone and
// falls through to the ordinary precedence.
func TestRouteProbePreemptionClaimsToken(t *testing.T) {
	c := testClock(t)
	h := newHS()
	armHealth(t, h)
	class := uniq("cls")
	a, b, cm := uMember(uniq("pp-a")), uMember(uniq("pp-b")), uMember(uniq("pp-c"))
	halfOpenK(t, h, c, b.ID)
	tripK(t, h, c, a.ID)
	members := []classMemberPref{a, b, cm}

	hl := decideHealthRoute(context.Background(), class, "s1", a, members, []byte(`{}`), false, "r-1", false)
	if hl.m.ID != b.ID || hl.hopKind != "probe" || hl.reason != "health:probe" || hl.lease == nil {
		t.Fatalf("probe verdict = %+v, want probe on %s with a lease", hl, b.ID)
	}

	// Token consumed: the second request must not double-probe.
	hl2 := decideHealthRoute(context.Background(), class, "s2", a, members, []byte(`{}`), false, "r-2", false)
	if hl2.hopKind == "probe" {
		t.Fatalf("second request also probed — the token was single-flight")
	}
}

// PR3 with halfOpenAny: a healthy bound member rides even while a sibling's
// probe token is out on another request.
func TestRouteBoundRidesWhenTokenTaken(t *testing.T) {
	c := testClock(t)
	h := newHS()
	armHealth(t, h)
	class := uniq("cls")
	a, b := uMember(uniq("pr3-a")), uMember(uniq("pr3-b"))
	halfOpenK(t, h, c, b.ID)
	if l := h.claimProbe(b.ID, class, b.ID, "r-infl", 100); l == nil {
		t.Fatal("setup: token claim failed")
	}
	hl := decideHealthRoute(context.Background(), class, "s1", a, []classMemberPref{a, b}, []byte(`{}`), false, "r-1", false)
	if hl.m.ID != a.ID || hl.hopKind != "bound" || hl.lease != nil {
		t.Fatalf("verdict = kind=%s m=%s, want bound/%s", hl.hopKind, hl.m.ID, a.ID)
	}
}

// PR4/I1: the first request past the bound member's OPEN key seats ONE
// election; every later session rides the same seat as a detour.
func TestRouteElectionSeatsOnceAndReused(t *testing.T) {
	c := testClock(t)
	h := newHS()
	armHealth(t, h)
	class := uniq("cls")
	a, b, cm := uMember(uniq("el-a")), uMember(uniq("el-b")), uMember(uniq("el-c"))
	tripK(t, h, c, a.ID)
	members := []classMemberPref{a, b, cm}

	hl := decideHealthRoute(context.Background(), class, "s1", a, members, []byte(`{}`), false, "r-1", false)
	if hl.hopKind != "ranked" || hl.reason != "health:ranked" || hl.m.ID != b.ID {
		t.Fatalf("first verdict = %+v, want ranked onto %s", hl, b.ID)
	}
	hl2 := decideHealthRoute(context.Background(), class, "s2", a, members, []byte(`{}`), false, "r-2", false)
	if hl2.hopKind != "detour" || hl2.reason != "health:detour" || hl2.m.ID != b.ID {
		t.Fatalf("second verdict = %+v, want the seated detour %s (I1)", hl2, b.ID)
	}
}

// PR5/thrash-8: a binding the brain re-pointed AFTER the episode opened is
// served — it beats a seated election.
func TestRouteFreshBindingBeatsElection(t *testing.T) {
	c := testClock(t)
	h := newHS()
	armHealth(t, h)
	class := uniq("cls")
	sess := uniq("s")
	a, b := uMember(uniq("pr5-a")), uMember(uniq("pr5-b"))
	tripK(t, h, c, a.ID)
	h.elect(a.ID, class, b.ID)
	// The brain re-pointed this session at a AFTER the episode opened: the
	// fake clock's episode instant is 2023, real time.Now is far later.
	bindMu.Lock()
	bindCache[bindKey(class, sess)] = classBind{member: a, fetchedAt: time.Now(), memberSince: time.Now()}
	bindMu.Unlock()
	defer func() {
		bindMu.Lock()
		delete(bindCache, bindKey(class, sess))
		bindMu.Unlock()
	}()

	hl := decideHealthRoute(context.Background(), class, sess, a, []classMemberPref{a, b}, []byte(`{}`), false, "r-1", false)
	if hl.m.ID != a.ID || hl.hopKind != "bound" {
		t.Fatalf("verdict = %+v, want the freshly re-pointed bound %s", hl, a.ID)
	}
}

// §4.6/BL-9: a session under a ramp dwell keeps serving the detour member.
func TestRouteRampHoldServesDetourMember(t *testing.T) {
	c := testClock(t)
	h := newHS()
	armHealth(t, h)
	class := uniq("cls")
	sess := uniq("s")
	a, b := uMember(uniq("rmp-a")), uMember(uniq("rmp-b"))
	tripK(t, h, c, a.ID)
	h.elect(a.ID, class, b.ID)
	h.noteSessionDetour(a.ID, class, sess)
	c.advance(61 * time.Second)
	h.tick()
	c.advance(950 * time.Second) // past the RETURN_DWELL floor
	l := h.claimProbe(a.ID, class, a.ID, uniq("p"), 100)
	h.settleLease(l, outcomeProbeOK, sample{reqID: "p1", class: class})
	c.advance(6 * time.Second)
	l = h.claimProbe(a.ID, class, a.ID, uniq("p"), 100)
	h.settleLease(l, outcomeProbeOK, sample{reqID: "p2", class: class})
	mustState(t, h, a.ID, hsConvalescent)
	// Give the ramp a queue: another session moved later, so sess's dwell
	// (i=1 for a second session would jitter) — sess is i=0, dwell = closeAt;
	// re-check via routeHints instead of asserting the schedule.
	hl := decideHealthRoute(context.Background(), class, sess, a, []classMemberPref{a, b}, []byte(`{}`), false, "r-1", false)
	if h2, holds := h.rampHolds(a.ID, class, sess, time.Time{}); holds {
		if hl.hopKind != "detour" || hl.reason != "health:ramp" || hl.m.ID != h2 {
			t.Fatalf("verdict = %+v, want the ramp hold on %s", hl, h2)
		}
	} else {
		// i=0 drains immediately (zero jitter): the verdict must then be the
		// recovered bound member, not a stale detour.
		if hl.m.ID != a.ID {
			t.Fatalf("ramp drained but verdict = %+v, want bound %s", hl, a.ID)
		}
	}
}

// PR6/PR11: a member held out only by an unattributed record (§2.6) is
// excluded — and re-admitted at the very back when everything else is OPEN,
// blocking the 529.
func TestRouteQuotaCoolingReadmittedBlocksExhaust(t *testing.T) {
	c := testClock(t)
	h := newHS()
	armHealth(t, h)
	class := uniq("cls")
	a, b := uMember(uniq("q-a")), uMember(uniq("q-b"))
	tripK(t, h, c, a.ID)
	uModelSpent(b)

	hl := decideHealthRoute(context.Background(), class, "s1", a, []classMemberPref{a, b}, []byte(`{}`), false, "r-1", false)
	if hl.exhaust {
		t.Fatal("529 fired while a quota-cooling member could be re-admitted (PR11)")
	}
	if hl.refuse != "" {
		t.Fatalf("refused: %s", hl.refuse)
	}
}

// PR12 + R-27: two admissible all-OPEN members mint the 529; a class whose
// ONLY admissible member is open rides it instead (single-member E13).
func TestRouteAllOpenExhaustAndSingleRides(t *testing.T) {
	armAllDegraded(t, true)
	c := testClock(t)
	h := newHS()
	armHealth(t, h)
	class := uniq("cls")
	a, b := uMember(uniq("ex-a")), uMember(uniq("ex-b"))
	tripK(t, h, c, a.ID)
	tripK(t, h, c, b.ID)

	hl := decideHealthRoute(context.Background(), class, "s1", a, []classMemberPref{a, b}, []byte(`{}`), false, "r-1", false)
	if !hl.exhaust {
		t.Fatalf("verdict = %+v, want exhaust (all admissible OPEN)", hl)
	}

	hl = decideHealthRoute(context.Background(), class, "s1", a, []classMemberPref{a}, []byte(`{}`), false, "r-2", false)
	if hl.exhaust || hl.hopKind != "forced" || hl.m.ID != a.ID {
		t.Fatalf("single-member verdict = %+v, want forced ride on %s", hl, a.ID)
	}
}

// PR8-emptied: nothing admissible answers the typed 503 shape, never a 529.
func TestRouteRefuseWhenNothingAdmissible(t *testing.T) {
	c := testClock(t)
	h := newHS()
	armHealth(t, h)
	class := uniq("cls")
	a := uUnroutable(uMember(uniq("rf-a")))
	b := uUnroutable(uMember(uniq("rf-b")))
	tripK(t, h, c, a.ID) // boundOK false so the fast path can't mask the empty list

	hl := decideHealthRoute(context.Background(), class, "s1", a, []classMemberPref{a, b}, []byte(`{}`), false, "r-1", false)
	if hl.refuse == "" || !strings.Contains(hl.refuse, "no member of class") {
		t.Fatalf("verdict = %+v, want a typed refusal", hl)
	}
}

// ---- rankCandidates: the health tiers ----

// PR9: a request over LARGE_PROMPT_TOK is only offered members whose declared
// window can hold it; at or under the threshold the window is not consulted.
func TestRankWindowFitPR9(t *testing.T) {
	testClock(t)
	h := newHS()
	armHealth(t, h)
	big := uMember(uniq("w-big"))
	big.ContextWindow = 500000
	small := uMember(uniq("w-small"))
	small.ContextWindow = 100000
	members := []classMemberPref{small, big}

	got := rankCandidates(members, "", rankOpt{est: 150000, health: true})
	if len(got) != 1 || got[0].m.ID != big.ID {
		t.Fatalf("est=150k ranked %d members, want only %s", len(got), big.ID)
	}
	// Over the threshold but inside both windows: nobody is excluded.
	got = rankCandidates(members, "", rankOpt{est: 90000, health: true})
	if len(got) != 2 {
		t.Fatalf("est=90k (fits both windows) ranked %d members, want 2", len(got))
	}
	// At or under the threshold the window is never consulted.
	got = rankCandidates(members, "", rankOpt{est: 60000, health: true})
	if len(got) != 2 {
		t.Fatalf("est=60k (under the gate) ranked %d members, want 2", len(got))
	}
}

// PR10/PR11: OPEN keys are deranked to q+3, never held out; a member held out
// only by an unattributed record re-enters at tierReadmit only when every
// ranked key is OPEN — and one whose account is spent never does.
func TestRankHealthTiers(t *testing.T) {
	c := testClock(t)
	h := newHS()
	armHealth(t, h)
	a, b, cm := uMember(uniq("t-a")), uMember(uniq("t-b")), uMember(uniq("t-c"))
	tripK(t, h, c, a.ID)
	uModelSpent(cm)
	members := []classMemberPref{a, b, cm}

	// b healthy: cooling stays out, open a ranks behind b.
	got := rankCandidates(members, "", rankOpt{health: true})
	if len(got) != 2 || got[0].m.ID != b.ID || got[0].tier != 0 ||
		got[1].m.ID != a.ID || got[1].tier != tierOpenBase {
		t.Fatalf("tiers = %+v, want [%s@0 %s@%d]", got, b.ID, a.ID, tierOpenBase)
	}

	// b gone: everything ranked is OPEN — cooling re-admitted at the back.
	got = rankCandidates([]classMemberPref{a, cm}, "", rankOpt{health: true})
	if len(got) != 2 || got[0].m.ID != a.ID || got[1].m.ID != cm.ID || got[1].tier != tierReadmit {
		t.Fatalf("re-admit ranking = %+v, want [%s %s@%d]", got, a.ID, cm.ID, tierReadmit)
	}

	// A spent ACCOUNT is not re-admitted: its own words said the budget is
	// gone, and asking again would only repeat the refusal.
	sp := uMember(uniq("t-s"))
	uSpent(sp)
	got = rankCandidates([]classMemberPref{a, sp}, "", rankOpt{health: true})
	if len(got) != 1 || got[0].m.ID != a.ID {
		t.Fatalf("spent account re-admitted: %+v", got)
	}
}

// pr11World is a member whose account reads like the reviewer's probe: a
// codex-like gate open, windows at 40 % and 60 %, credits per credits, and a
// model record on the member's model. a is a tripped (OPEN) other member, so
// PR11 is the only way back to pm.
func pr11World(t *testing.T, credits string) (c *fakeClock, a, pm classMemberPref, acct string) {
	t.Helper()
	c = testClock(t)
	h := newHS()
	armHealth(t, h)
	a, pm = uMember(uniq("m-a")), uMember(uniq("m-p"))
	tripK(t, h, c, a.ID)
	acct = uAccount(pm.ID)
	st := standingAt("gpt", nowFn(), map[string]checkWindow{
		"five_hour": fiveHour(40, nowFn().Add(3*time.Hour)),
		"seven_day": week(60, nowFn().Add(72*time.Hour))}, credits)
	gate := true
	st.Allowance.Open = &gate
	pref.mu.Lock()
	pref.accounts[acct] = st
	pref.mu.Unlock()
	if chk, _ := routeCheck(pm.Route, acct, pm.Model, nowFn()); !chk.included() {
		t.Fatalf("fixture: check %s, want included before the record", chk)
	}
	uModelSpent(pm)
	return c, a, pm, acct
}

// §2.2 row 14, the reviewer's probe: the model record is the one limit no
// reading sees, and the account's credits are open. If credits serve that
// model, a dial would bill a route that pays from nothing, and its 2xx would
// erase the only record of the clamp. So the route reads `no (pay)`, is not
// held, and PR11 never dials it.
func TestReadmitNotWhereMoneyMayOpen(t *testing.T) {
	_, a, pm, acct := pr11World(t, "open")
	c, held := routeCheck(pm.Route, acct, pm.Model, nowFn())
	if !c.no() || c.Reason != "pay" || held || c.held || c.onAllowance() {
		t.Fatalf("model record, credits open, pay []: check %s held=%v, want no(pay), not held", c, held)
	}
	got := rankCandidates([]classMemberPref{a, pm}, "", rankOpt{health: true})
	if len(got) != 1 || got[0].m.ID != a.ID {
		t.Fatalf("re-admitted where money may open: %+v", got)
	}
	if _, chk, ok := providerMemberDial(pm, "", true, false, nowFn()); ok {
		t.Fatalf("readmit dial on a route money may serve: %s", chk)
	}
	if _, ok, held := memberVerdict(pm, nowFn()); ok || held {
		t.Fatalf("member verdict ok=%v held=%v, want refused and not held", ok, held)
	}
}

// With the account's credits off no money can open behind the record: the
// route is held by it alone, PR11 re-admits it, and the re-admitted dial's 2xx
// ends the record (it drew on the allowance).
func TestReadmitWhereNoMoneyCanOpenAndItsServedEndsTheRecord(t *testing.T) {
	clk, a, pm, acct := pr11World(t, "closed")
	c, held := routeCheck(pm.Route, acct, pm.Model, nowFn())
	if !c.no() || c.Reason != "spent" || !held {
		t.Fatalf("model record, credits off: check %s held=%v, want no(spent), held", c, held)
	}
	got := rankCandidates([]classMemberPref{a, pm}, "", rankOpt{health: true})
	if len(got) != 2 || got[1].m.ID != pm.ID || got[1].tier != tierReadmit {
		t.Fatalf("ranking = %+v, want %s re-admitted at %d", got, pm.ID, tierReadmit)
	}
	_, chk, ok := providerMemberDial(pm, "", true, false, nowFn())
	if !ok || !chk.held {
		t.Fatalf("readmit dial = %s ok=%v, want the held check", chk, ok)
	}

	// The re-admitted dial answers 2xx: the hop carries the readmit's check.
	tokMu.Lock()
	oldTokens := tokens
	tokens = map[string]tokEntry{acct: {token: "tok-" + acct, exp: nowFn().Add(time.Hour)}}
	tokMu.Unlock()
	t.Cleanup(func() { tokMu.Lock(); tokens = oldTokens; tokMu.Unlock() })
	hop := httptest.NewRequest(http.MethodPost, "https://anthropic.test/v1/messages", nil)
	hop.Header.Set("Authorization", "Bearer tok-"+acct)
	hop = hop.WithContext(context.WithValue(hop.Context(), servedCheckKey{}, chk))
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}}
	clk.advance(time.Second)
	noteServedHop(hop, reqInfo{body: []byte(`{"model":"` + pm.Model + `"}`)}, resp, "anthropic.test", nowFn())
	recMu.Lock()
	_, still := records[recKey{acct, modelLimit(pm.Model)}]
	recMu.Unlock()
	if still {
		t.Fatal("the re-admitted dial's 2xx left the model record standing")
	}
}

// §2.2 row 14: PR11 never re-admits into money. A model record on a route that
// may pay is a spent limit the route pays past: it reads paid, so the paid tier
// offers it and PR11 has nothing to re-admit. Its 2xx is paid and never ends the
// record that keeps routes which may not pay off the account.
func TestReadmitNeverIntoMoney(t *testing.T) {
	c := testClock(t)
	h := newHS()
	armHealth(t, h)
	a, pm := uMember(uniq("m-a")), uMember(uniq("m-p"))
	tripK(t, h, c, a.ID)
	acct := uAccount(pm.ID)
	pref.mu.Lock()
	r := pref.routes[pm.Route]
	r.Pay = []string{acct + "/credits"}
	pref.routes[pm.Route] = r
	pref.accounts[acct] = standingAt("anthropic", nowFn(), map[string]checkWindow{
		"seven_day": week(100, nowFn().Add(48*time.Hour))}, "open")
	pref.mu.Unlock()
	if c, _ := routeCheck(pm.Route, acct, "", nowFn()); !c.paid() {
		t.Fatalf("fixture: check %s, want paid", c)
	}
	uModelSpent(pm)
	c2, held := routeCheck(pm.Route, acct, pm.Model, nowFn())
	if !c2.paid() || held || c2.onAllowance() {
		t.Fatalf("model record on a paid route: check %s held=%v, want paid, not held", c2, held)
	}
	got := rankCandidates([]classMemberPref{a, pm}, "", rankOpt{health: true})
	for _, g := range got {
		if g.tier == tierReadmit {
			t.Fatalf("re-admitted into money: %+v", got)
		}
	}
	if _, chk, ok := providerMemberDial(pm, "", true, false, nowFn()); !ok || !chk.paid() {
		t.Fatalf("dial = %s ok=%v, want the paid check", chk, ok)
	}
}

// ---- §5.4: the synthetic 529 ----

func TestDegradedExhaustResponseProperties(t *testing.T) {
	c := testClock(t)
	h := newHS()
	armHealth(t, h)
	class := uniq("cls")
	a, b := uMember(uniq("d-a")), uMember(uniq("d-b"))
	tripK(t, h, c, a.ID)
	tripK(t, h, c, b.ID)
	mk := func() *classRoute {
		return &classRoute{class: class, reqID: "r-fixed", member: a,
			members: []classMemberPref{a, b},
			served:  newServedHop(class, a, reqInfo{}, a.ID, "")}
	}

	cc := mk()
	resp := degradedExhaustResponse(cc, false)
	if resp.StatusCode != 529 || resp.Status != "529 Overloaded" {
		t.Fatalf("status = %d %q", resp.StatusCode, resp.Status)
	}
	if !cc.healthExhaust || cc.exhaustPinned {
		t.Fatalf("classCtx flags = %v/%v, want exhaust unpinned", cc.healthExhaust, cc.exhaustPinned)
	}
	if got := resp.Header.Get("X-Switch-Health"); got != "exhausted" {
		t.Fatalf("X-Switch-Health = %q", got)
	}
	if got := resp.Header.Get("X-Switch-Decision"); got != "r-fixed" {
		t.Fatalf("X-Switch-Decision = %q", got)
	}
	ra, _ := time.ParseDuration(resp.Header.Get("Retry-After") + "s")
	if ra < 5*time.Second || ra > 60*time.Second {
		t.Fatalf("Retry-After = %v, want within [5s,60s]", ra)
	}
	// Deterministic per reqID: the same decision renders the same delay.
	if r2 := degradedExhaustResponse(mk(), false); r2.Header.Get("Retry-After") != resp.Header.Get("Retry-After") {
		t.Fatalf("Retry-After drifted for one reqID: %s vs %s",
			resp.Header.Get("Retry-After"), r2.Header.Get("Retry-After"))
	}
	body, _ := io.ReadAll(resp.Body)
	var e struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("body not JSON: %v (%s)", err, body)
	}
	if e.Type != "error" || e.Error.Type != "overloaded_error" ||
		!strings.Contains(e.Error.Message, "every member of class "+class+" is degraded") {
		t.Fatalf("body = %+v", e)
	}
	if !strings.Contains(e.Error.Message, hsOpen) {
		t.Fatalf("message names no degraded key detail: %s", e.Error.Message)
	}

	pc := mk()
	presp := degradedExhaustResponse(pc, true)
	if got := presp.Header.Get("X-Switch-Health"); got != "pinned-exhausted" {
		t.Fatalf("pinned X-Switch-Health = %q", got)
	}
	pbody, _ := io.ReadAll(presp.Body)
	if !strings.Contains(string(pbody), "pinned to "+a.ID) || !pc.exhaustPinned {
		t.Fatalf("pinned body = %s (flag %v)", pbody, pc.exhaustPinned)
	}
}

// The Retry-After jitter lands in [6,10]s for every reqID (0.8–1.2 × 8s,
// rounded) — the clamp at [5,60] never actually binds for the 8s base.
func TestExhaustRetryAfterJitterRange(t *testing.T) {
	for i := 0; i < 200; i++ {
		d := jitterDur(uniq("jr"), 8*time.Second).Round(time.Second)
		if s := int(d / time.Second); s < 6 || s > 10 {
			t.Fatalf("jitter rounded to %ds, outside [6,10]", s)
		}
	}
}

// ---- §5.5 N24: the would-act clauses ----

func TestWouldActClauses(t *testing.T) {
	c := testClock(t)
	h := newHS()
	armHealth(t, h)
	class := uniq("cls")
	a := uMember(uniq("wa-a"))
	open := uMember(uniq("wa-open"))
	tripK(t, h, c, open.ID)
	mk := func(m classMemberPref) *classRoute {
		return &classRoute{class: class, member: m,
			served: newServedHop(class, m, reqInfo{}, m.ID, "")}
	}

	base := mk(a) // healthy bound serve: no clause fires
	base.hopKind = "bound"
	healthNoteWouldAct(base)
	if n := wouldActOf(h, class); n != 0 {
		t.Fatalf("healthy serve bumped would_act to %d", n)
	}

	for name, cc := range map[string]*classRoute{
		"walked":       func() *classRoute { x := mk(a); x.walked = "quota"; return x }(),
		"exhaust":      func() *classRoute { x := mk(a); x.healthExhaust = true; return x }(),
		"detour":       func() *classRoute { x := mk(a); x.hopKind = "detour"; return x }(),
		"ranked":       func() *classRoute { x := mk(a); x.hopKind = "ranked"; return x }(),
		"ramp":         func() *classRoute { x := mk(a); x.hopKind = "bound"; x.routeReason = "health:ramp"; return x }(),
		"degraded-key": func() *classRoute { x := mk(open); x.hopKind = "bound"; return x }(),
	} {
		before := wouldActOf(h, class)
		healthNoteWouldAct(cc)
		if wouldActOf(h, class) != before+1 {
			t.Fatalf("clause %q did not bump would_act", name)
		}
	}
	// nil store / nil ctx: inert.
	healthNoteWouldAct(nil)
}

// ---- #115: the registry roster sync ----

func TestSyncRegistryFreezesAbsentKeys(t *testing.T) {
	c := testClock(t)
	h := newHS()
	armHealth(t, h)
	class := uniq("cls")
	a, b := uMember(uniq("sr-a")), uMember(uniq("sr-b"))
	halfOpenK(t, h, c, b.ID)
	h.record(a.ID, okS(uniq("ok")))

	hints := h.routeHints(a.ID, []string{a.ID, b.ID}, class, "s1", time.Time{})
	if !hints.halfOpenAny {
		t.Fatal("setup: no half-open visible")
	}
	// b leaves the registry: frozen — no longer probe-able, invisible to hints.
	h.syncRegistry(map[string]bool{a.ID: true})
	hints = h.routeHints(a.ID, []string{a.ID, b.ID}, class, "s1", time.Time{})
	if hints.halfOpenAny {
		t.Fatal("an absent key still reads half-open")
	}
	if l := h.claimProbe(b.ID, class, b.ID, "r-abs", 100); l != nil {
		t.Fatal("claimed a probe on an absent key")
	}
	// b returns: unfrozen in place.
	h.syncRegistry(map[string]bool{a.ID: true, b.ID: true})
	hints = h.routeHints(a.ID, []string{a.ID, b.ID}, class, "s1", time.Time{})
	if !hints.halfOpenAny {
		t.Fatal("a returning key stayed frozen")
	}
}

var _ = http.Header{} // keep the import set stable if assertions above change
