package main

import (
	"reflect"
	"testing"
	"time"
)

// classCandidates tiers (§2.3): included headroom -> over-reserve -> at the
// hard line -> included but throttled -> paid in pay_order; a member whose
// check is `no` on every account (unrouted, spent, may not pay) or whose
// provider is not configured is dropped, and named. A nil effective is never a
// verdict; the hard line is whatever the brain served. Anthropic-kind members
// throughout so the table needs no provider config; each has its own account,
// so a record or a reading speaks for that member alone.
func TestClassCandidatesTiers(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	freezeQuotaLedger(t, now)
	f := func(v float64) *float64 { return &v }
	open := standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(40, now.Add(48*time.Hour))}, "closed")
	dry := standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(100, now.Add(48*time.Hour))}, "open")
	accounts := map[string]acctStanding{}
	routes := map[string]routePref{}
	m := func(id string, eff, ceiling *float64, st acctStanding, pay ...string) classMemberPref {
		acct := "acct-" + id
		accounts[acct] = st
		r := memberRoute("c/"+id, "c", id, "opus", "anthropic", acct)
		for _, p := range pay {
			r.Pay = append(r.Pay, acct+"/"+p)
		}
		routes[r.ID] = r
		return classMemberPref{ID: id, Kind: "anthropic", Lane: "opus", Model: "claude-opus-5",
			Effective: eff, ReserveCeiling: ceiling, Route: r.ID}
	}
	ids := func(ms []classMemberPref) []string {
		out := []string{}
		for _, x := range ms {
			out = append(out, x.ID)
		}
		return out
	}

	members := []classMemberPref{
		m("bound", f(10), nil, open),      // excluded: it's the bound member
		m("spent-100", f(100), nil, open), // at the hard line, served first — goes after headroom
		m("dead", f(5), nil, open),        // no route: dropped
		m("reserve-70", f(70), f(60), open),
		m("paid-y", f(100), nil, dry, "credits"), // allowance closed, may pay: the paid tier
		m("fresh-34", f(34), nil, open),
		m("unknown", nil, f(60), open),           // nil effective: neither at the line nor over ceiling
		m("cooling", f(1), nil, open),            // spent by its own 429: dropped
		m("broke", f(100), nil, dry),             // allowance closed, pay []: dropped
		m("throttled-20", f(20), nil, open),      // included but throttled: after every healthy included
		m("paid-x", f(100), nil, dry, "credits"), // later priority, but first in pay_order
		m("spent-95", f(95), nil, open),
	}
	for i := range members {
		members[i].Priority = i
	}
	members[2].Route = "c/none"
	seedQuota(t, accounts, routes, nil, []string{"acct-paid-x/credits"})
	pref.mu.Lock()
	oldHard := pref.hardPct
	pref.hardPct = 0 // => defaultHardPct (95)
	pref.mu.Unlock()
	t.Cleanup(func() {
		pref.mu.Lock()
		pref.hardPct = oldHard
		pref.mu.Unlock()
	})
	seedSpent("acct-cooling", "seven_day", 429, "usage_limit_reached", time.Time{})
	recordThrottle("acct-throttled-20", 429, "slow down", nil)

	got := ids(classCandidates(members, "bound"))
	want := []string{"fresh-34", "unknown", "reserve-70", "spent-100", "spent-95", "throttled-20", "paid-x", "paid-y"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("default hard=95: got %v want %v", got, want)
	}

	// Every member the walk drops is named with its check, so a "walk skips"
	// log line says why a turn ended without trying it.
	_, skips := rankCandidatesSkips(members, "bound", rankOpt{})
	until := now.Add(quotaCooldown).UTC().Format(time.RFC3339)
	reset := now.Add(48 * time.Hour).UTC().Format(time.RFC3339)
	wantSkips := []memberSkip{
		{"dead", "check no(unknown until unknown) basis=unknown"},
		{"cooling", "check no(spent until " + until + ")"},
		{"broke", "check no(pay until " + reset + ")"},
	}
	if !reflect.DeepEqual(skips, wantSkips) {
		t.Fatalf("skips = %+v want %+v", skips, wantSkips)
	}

	// The brain's hard line moves the tier boundary: at hard=60 the 70% reserve
	// member is at the line (served order within it), and 34 stays. The paid
	// tier stays last: money is spent only after every included rung.
	pref.mu.Lock()
	pref.hardPct = 60
	pref.mu.Unlock()
	got = ids(classCandidates(members, "bound"))
	want = []string{"fresh-34", "unknown", "spent-100", "reserve-70", "spent-95", "throttled-20", "paid-x", "paid-y"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("hard=60: got %v want %v", got, want)
	}

	// With no pay_order the paid tier falls back to member priority.
	pref.mu.Lock()
	pref.payOrder = nil
	pref.mu.Unlock()
	got = ids(classCandidates(members, "bound"))
	if n := len(got); n < 2 || got[n-2] != "paid-y" || got[n-1] != "paid-x" {
		t.Fatalf("no pay_order: got %v, want paid-y (priority 4) before paid-x (10)", got)
	}
}

func TestHardPctDefaultsWhenBrainOmitsThresholds(t *testing.T) {
	pref.mu.Lock()
	old := pref.hardPct
	pref.hardPct = 0
	pref.mu.Unlock()
	defer func() {
		pref.mu.Lock()
		pref.hardPct = old
		pref.mu.Unlock()
	}()
	if got := hardPct(); got != defaultHardPct {
		t.Fatalf("hardPct() = %v with nothing fetched, want the mirrored default %v", got, defaultHardPct)
	}
}
