package main

import (
	"io"
	"testing"
	"time"
)

// §2.3: a throttle is not exhaustion, so money never dodges it. Each swap site
// below is offered a paid sibling after an included account throttled, and
// must not take it.

// anthHits drains the Anthropic upstream's hits so far.
func anthHits(env *e2eEnv) []e2eHit {
	var out []e2eHit
	for {
		select {
		case h := <-env.seen:
			out = append(out, h)
		case <-time.After(50 * time.Millisecond):
			return out
		}
	}
}

// throttleWorld: other is included (the fable lane's incumbent) and throttles;
// native is spent with credits on, so native is paid wherever routeID may pay.
func throttleWorld(env *e2eEnv, routeID string) {
	env.nearlySpent(other, "anthropic", 50, false)
	env.nearlySpent(native, "anthropic", 100, true)
	env.payFrom(routeID, native+"/credits")
	env.deny429.Store(getToken(other)) // no unified headers: a throttle
	env.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"busy"}}`)
}

// The bare-lane swap (dispatch): the throttle goes back to the client.
func TestE2ELaneThrottleDoesNotSwapToPaid(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	throttleWorld(env, laneRouteID("fable"))
	fetchPreferredSync()
	if c, _ := routeCheck(laneRouteID("fable"), native, "", nowFn()); !c.paid() {
		t.Fatalf("fixture: native on lane:fable = %s, want paid", c)
	}
	resp := env.post(t, "claude-fable-5", "CLIENT")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 429 {
		t.Fatalf("status %d, want the throttle surfaced", resp.StatusCode)
	}
	for _, h := range anthHits(env) {
		if h.auth == "Bearer "+getToken(native) {
			t.Fatal("the lane swapped to the paid account to dodge a throttle")
		}
	}
}

// (A-D6, C7) Anthropic's money refusals walk: the 402 billing_error and the
// 400 "credit balance is too low" are unpaid on the refusing account's
// credits pot, so the bare lane swaps to its other included account. Before
// C7 neither walked (only a 429 did). An invalid 400 does not walk.
func TestE2ELaneUnpaidSwapsAccount(t *testing.T) {
	for _, c := range []struct {
		name   string
		status int
		body   string
	}{{"402 billing", 402, anthBilling402}, {"400 credit too low", 400, anthCreditTooLow400}} {
		t.Run(c.name, func(t *testing.T) {
			env, restore := newE2E(t)
			defer restore()
			env.nearlySpent(other, "anthropic", 50, false)
			env.nearlySpent(native, "anthropic", 30, false)
			env.deny429.Store(getToken(other))
			env.denyStatus.Store(int64(c.status))
			env.body429.Store(c.body)
			fetchPreferredSync()
			resp := env.post(t, "claude-fable-5", "CLIENT")
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Fatalf("status %d, want the other account's 200", resp.StatusCode)
			}
			hits := anthHits(env)
			if len(hits) != 2 || hits[1].auth != "Bearer "+getToken(native) {
				t.Fatalf("hits = %+v, want the refusing account then %s", hits, native)
			}
			if got := recordsOn(other); len(got) != 1 || got[0] != "unpaid:credits" {
				t.Fatalf("records on %s = %v, want unpaid:credits", other, got)
			}
		})
	}
	env, restore := newE2E(t)
	defer restore()
	env.nearlySpent(other, "anthropic", 50, false)
	env.nearlySpent(native, "anthropic", 30, false)
	env.deny429.Store(getToken(other))
	env.denyStatus.Store(400)
	env.body429.Store(anthInvalid400)
	fetchPreferredSync()
	resp := env.post(t, "claude-fable-5", "CLIENT")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 400 || len(anthHits(env)) != 1 || len(recordsOn(other)) != 0 {
		t.Fatalf("an invalid 400 must go back as it came, unrecorded (status %d)", resp.StatusCode)
	}
}

// The class's same-member account swap (walkRungs, Anthropic member).
func TestE2EMemberThrottleDoesNotSwapToPaid(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	throttleWorld(h.e2eEnv, "frontier/lane:fable")
	h.bindTo.Store("lane:fable")
	fetchPreferredSync()
	if c, _ := routeCheck("frontier/lane:fable", native, "", nowFn()); !c.paid() {
		t.Fatalf("fixture: native on frontier/lane:fable = %s, want paid", c)
	}
	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-member-throttle")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	for _, hit := range anthHits(h.e2eEnv) {
		if hit.auth == "Bearer "+getToken(native) {
			t.Fatal("the member swapped to the paid account to dodge a throttle")
		}
	}
}

// The pool swap (walkRungs, provider member): gpt-b is paid, gpt-a throttles.
func TestE2EPoolThrottleDoesNotSwapToPaid(t *testing.T) {
	h, restore := newE2ECodexPool(t)
	defer restore()
	h.spentWithCredits("gpt-b", "gpt")
	h.payFrom("frontier/gpt/gpt-5.6", "gpt-b/credits")
	fetchPreferredSync()
	if c, _ := routeCheck("frontier/gpt/gpt-5.6", "gpt-b", "", nowFn()); !c.paid() {
		t.Fatalf("fixture: gpt-b = %s, want paid", c)
	}
	h.codexDenyBearer.Store("codex-tok-gpt-a")
	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-pool-throttle")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if hit := h.codexHitOrFail(t); hit.auth != "Bearer codex-tok-gpt-a" {
		t.Fatalf("first hop %q, want gpt-a", hit.auth)
	}
	select {
	case hit := <-h.codexSeen:
		t.Fatalf("a second codex hop on %q: the pool swapped to a paid account to dodge a throttle", hit.auth)
	case <-time.After(100 * time.Millisecond):
	}
	if !throttledOn("gpt-a") {
		t.Fatal("gpt-a must be throttled after its busy 429")
	}
}

// Unknown use sorts after known: an account no reading measures is never
// taken for an idle one (records.go "unknown is never 0").
func TestRankUnknownUseAfterKnown(t *testing.T) {
	now := time.Now()
	seedQuota(t, map[string]acctStanding{
		"a": standingAt("anthropic", now, nil, ""),
		"b": standingAt("anthropic", now, nil, ""),
	}, map[string]routePref{"lane:opus": laneRoute("opus", "", "a", "b")}, nil, nil)
	pct := func(a string) *float64 {
		if a == "b" {
			return ptr(90)
		}
		return nil
	}
	ok, _ := rankRouteAccounts("lane:opus", "", checkRequest{}, "", []string{"a", "b"}, pct, now)
	if len(ok) != 2 || ok[0].account != "b" {
		t.Fatalf("ranked %v, want b (90 %%, known) before a (unknown)", ok)
	}
}

// failoverRoute never hands back the account that just refused, even when it
// still ranks first.
func TestFailoverRouteSkipsTheRefusingAccount(t *testing.T) {
	_, restore := newE2E(t)
	defer restore()
	route := laneRouteID("default")
	cands, _ := rankRouteAccounts(route, "", checkRequest{}, "", routeAccounts(route), lanePct("default"), nowFn())
	if len(cands) != 2 {
		t.Fatalf("fixture: want both accounts admitted, got %v", cands)
	}
	first, second := cands[0].account, cands[1].account
	if _, acct, _, ok := failoverRoute(route, "default", "", first); !ok || acct != second {
		t.Fatalf("failover after %s refused = %q (%v), want %s", first, acct, ok, second)
	}
}
