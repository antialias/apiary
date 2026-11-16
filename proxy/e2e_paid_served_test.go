package main

import (
	"io"
	"testing"
	"time"
)

// nearlySpent serves account as measured at pct on its weekly window with
// credits open or closed.
func (env *e2eEnv) nearlySpent(account, provider string, pct float64, credits bool) {
	now := float64(time.Now().UnixNano()) / 1e9
	env.brainMu.Lock()
	if env.standings == nil {
		env.standings = map[string]any{}
	}
	env.standings[account] = map[string]any{"provider": provider, "as_of": now, "next_poll_at": now + 300,
		"allowance": map[string]any{"open": nil, "windows": map[string]any{
			"seven_day": map[string]any{"used_pct": pct, "resets_at": now + 3*86400, "period": "7d", "scope": nil, "as_of": now}}},
		"money": map[string]any{"credits": map[string]any{"open": credits, "balance": 40.0}}}
	env.brainMu.Unlock()
}

// unpaidCheck is account's check on a route that may not pay (pay []).
func unpaidCheck(account string) dialCheck {
	_, pots, _, _ := routeOf(laneRouteID("opus"))
	now := nowFn()
	return evalDialCheck(routePolicy{ID: "b", Pay: []string{}}, account, standingFor(account, now), pots, nil, staleMax(), unixSeconds(now), checkRequest{})
}

// A paid 2xx says the money served, not that the allowance has room: it must
// not end the spent record that keeps routes which may not pay off the
// account. Kimi at 97 % with credits on; after its weekly 429, route A
// (frontier's kimi member, pay [kimi/credits]) is served on money, and a route
// with pay [] still reads no(pay).
func TestE2EPaidServeKeepsTheSpentRecord(t *testing.T) {
	h, restore := newE2EClassOn(t, "kimi/kimi-k3", 1.0)
	defer restore()
	h.nearlySpent("kimi", "kimi", 97, true)
	h.payFrom("frontier/kimi/kimi-k3", "kimi/credits")
	fetchPreferredSync()
	seedSpent("kimi", "seven_day", 429, "weekly limit", time.Now().Add(3*24*time.Hour))
	if c, _ := routeCheck("frontier/kimi/kimi-k3", "kimi", "", nowFn()); !c.paid() {
		t.Fatalf("fixture: route A check %s, want paid", c)
	}

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-paid-served")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d (%s), want route A served on money", resp.StatusCode, b)
	}
	if hit := <-h.hosted; hit.model != "kimi-k3" {
		t.Fatalf("served %q, want kimi-k3", hit.model)
	}
	if _, ok := spentRecordOf("kimi", nowFn()); !ok {
		t.Fatal("the paid 200 ended kimi's seven_day record")
	}
	if c := unpaidCheck("kimi"); !c.no() || c.Reason != "pay" {
		t.Fatalf("route B (pay []) on kimi = %s, want no(pay): its weekly window is spent", c)
	}
}

// The same on Anthropic's bare-lane path: the native account at 97 % with
// extra usage on, the other spent with none. lane:fable may pay native's
// credits and is served on them; lane:default (pay []) still reads no(pay).
func TestE2EPaidLaneServeKeepsTheSpentRecord(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	env.nearlySpent(native, "anthropic", 97, true)
	env.nearlySpent(other, "anthropic", 100, false)
	env.payFrom(laneRouteID("fable"), native+"/credits")
	fetchPreferredSync()
	seedSpent(native, "seven_day", 429, "weekly limit", time.Now().Add(3*24*time.Hour))
	if c, _ := routeCheck(laneRouteID("fable"), native, "", nowFn()); !c.paid() {
		t.Fatalf("fixture: lane:fable check %s, want paid", c)
	}

	resp := env.post(t, "claude-fable-5", "CLIENT")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d (%s), want lane:fable served on money", resp.StatusCode, b)
	}
	if hit := env.hit(t); hit.auth != "Bearer "+getToken(native) {
		t.Fatalf("served on %q, want native's token", hit.auth)
	}
	if _, ok := spentRecordOf(native, nowFn()); !ok {
		t.Fatal("the paid 200 ended native's seven_day record")
	}
	if c, _ := routeCheck(laneRouteID("default"), native, "", nowFn()); !c.no() || c.Reason != "pay" {
		t.Fatalf("lane:default (pay []) on native = %s, want no(pay)", c)
	}
}

// The class walk's cross onto an Anthropic member stamps the check that
// admitted the crossed hop: a paid cross's 2xx must not speak for the
// allowance with attempt 1's included check. Bound to kimi, which is spent; the
// frontier lane:fable member crosses onto native at 97 % with credits on,
// paid, after native's weekly 429. The 200 keeps native's record.
func TestE2EPaidCrossServeKeepsTheSpentRecord(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	h.nearlySpent(native, "anthropic", 97, true)
	h.nearlySpent(other, "anthropic", 100, false)
	h.payFrom("frontier/lane:fable", native+"/credits")
	h.bindTo.Store("kimi/kimi-k3")
	fetchPreferredSync()
	seedSpent(native, "seven_day", 429, "weekly limit", time.Now().Add(3*24*time.Hour))
	if c, _ := routeCheck("frontier/lane:fable", native, "", nowFn()); !c.paid() {
		t.Fatalf("fixture: native on frontier/lane:fable = %s, want paid", c)
	}
	h.deny403.Store(true) // kimi's quota refusal: spent, so the walk may reach money

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-paid-cross")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if first := h.hostedHit(t); first.model != "kimi-k3" {
		t.Fatalf("attempt 1 = %+v, want the bound kimi member", first)
	}
	if hit := h.hit(t); hit.auth != "Bearer "+getToken(native) || hit.model != "claude-fable-5" {
		t.Fatalf("cross = %+v, want lane:fable on native's token", hit)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status %d (%s), want the crossed 200", resp.StatusCode, b)
	}
	if _, ok := spentRecordOf(native, nowFn()); !ok {
		t.Fatal("the paid cross's 200 ended native's seven_day record")
	}
}
