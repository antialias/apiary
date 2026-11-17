package main

import (
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"
)

// brainLaneRules is the lane_rules the brain publishes (its DEFAULT_LANES): each
// lane names its scope, so "scoped:Fable" binds fable and nothing else.
var brainLaneRules = []laneRule{
	{Lane: "fable", Match: []string{"fable"}, Scope: "Fable"},
	{Lane: "opus", Match: []string{"opus"}, Scope: "Opus"},
	{Lane: "sonnet", Match: []string{"sonnet"}, Scope: "Sonnet"},
	{Lane: "haiku", Match: []string{"haiku"}, Scope: "Haiku"},
	{Lane: "default", Match: nil},
}

// availWorld isolates every global the availability model reads: the lanes,
// the brain's lane rules, one bare-lane route per rule over every account
// given, those accounts' standings, and an empty record store.
func availWorld(t *testing.T, lanes map[string]lanePref, accounts map[string]acctStanding) {
	t.Helper()
	lan.pref.mu.Lock()
	oAcct, oLanes, oWeekly, oHard, oInject := lan.pref.account, lan.pref.lanes, lan.pref.weekly, lan.pref.hardPct, injectFile
	oRules := lan.pref.laneRules
	lan.pref.account, lan.pref.lanes, lan.pref.weekly, lan.pref.hardPct = "", lanes, nil, 0
	lan.pref.laneRules = brainLaneRules
	lan.pref.mu.Unlock()
	injectFile = ""
	names := make([]string, 0, len(accounts))
	for a := range accounts {
		names = append(names, a)
	}
	sort.Strings(names)
	routes := map[string]routePref{}
	for _, r := range brainLaneRules {
		routes["lane:"+r.Lane] = laneRoute(r.Lane, r.Scope, names...)
	}
	if accounts == nil {
		accounts = map[string]acctStanding{}
	}
	seedQuota(t, accounts, routes, nil, nil)
	t.Cleanup(func() {
		lan.pref.mu.Lock()
		lan.pref.account, lan.pref.lanes, lan.pref.weekly, lan.pref.hardPct = oAcct, oLanes, oWeekly, oHard
		lan.pref.laneRules = oRules
		lan.pref.mu.Unlock()
		injectFile = oInject
	})
}

func fp64(v float64) *float64 { return &v }

func rlHeader(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Set(kv[i], kv[i+1])
	}
	return h
}

func TestParseUnifiedRL(t *testing.T) {
	now := time.Unix(1792692000, 0)
	h := rlHeader(
		"anthropic-ratelimit-unified-status", "rejected",
		"anthropic-ratelimit-unified-reset", "1792704000",
		"anthropic-ratelimit-unified-representative-claim", "five_hour",
		"anthropic-ratelimit-unified-5h-status", "rejected",
		"anthropic-ratelimit-unified-5h-reset", "1792704000",
		"anthropic-ratelimit-unified-7d-utilization", "0.82",
		"anthropic-ratelimit-unified-overage-status", "rejected",
		"anthropic-ratelimit-requests-limit", "1000",
	)
	o, ok := parseUnifiedRL(h, now, scopeMap{})
	if !ok || len(o.rejectedWindows()) != 1 || o.claim != "five_hour" || o.claimName != "five_hour" || o.reset.Unix() != 1792704000 {
		t.Fatalf("parse = %+v ok=%v", o, ok)
	}
	if len(o.windows) != 2 || o.windows[0].name != "five_hour" || o.windows[1].name != "seven_day" ||
		o.windows[0].token != "5h" || o.windows[1].pct != 82 {
		t.Fatalf("windows = %+v (overage is not a window; utilization is a fraction)", o.windows)
	}
	if _, ok := parseUnifiedRL(rlHeader("Content-Type", "application/json"), now, scopeMap{}); ok {
		t.Fatal("a response without unified headers parsed as an observation")
	}
}

// An account's state is its check: a closed window refuses it until the later
// of its closed windows, 97 % admits it (the hard line orders), and a throttle
// leaves an included check included — a throttle is not `no`. The lane's
// unavailable body lists each account by that check.
func TestAccountCheckIsTheAvailability(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	r5, r7 := now.Add(4*time.Hour), now.Add(50*time.Hour)
	availWorld(t, nil, map[string]acctStanding{
		"a": standingAt("anthropic", now, map[string]checkWindow{"five_hour": fiveHour(100, r5), "seven_day": week(100, r7)}, ""),
		"b": standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(97, r7)}, ""),
		"c": standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(10, r7)}, ""),
	})
	a, _ := lan.routeCheck("lane:opus", "a", "", now)
	if !a.no() || a.Reason != "spent" || !a.Until.Time().Equal(r7) {
		t.Fatalf("a's check = %s, want no(spent) until the later of its closed windows (%v)", a, r7)
	}
	if b, _ := lan.routeCheck("lane:opus", "b", "", now); !b.included() {
		t.Fatalf("b at 97%% = %s, want included: the hard line orders, it never excludes", b)
	}
	lan.recordThrottle("c", 429, "test throttle", rlHeader("Retry-After", "120"))
	if c, _ := lan.routeCheck("lane:opus", "c", "", now); !c.included() {
		t.Fatalf("c = %s, want included: a throttle is not `no`", c)
	}
	if d := lan.throttledUntil("c", now).Sub(now); d < 110*time.Second || d > 130*time.Second {
		t.Fatalf("c's throttle = %v from now, want Retry-After's 120 s", d)
	}
	u := lan.laneUnavailable("opus", "", now)
	acc := u.Members[0].Accounts
	if acc[0].Account != "a" || strOrEmpty(acc[0].Why) != "spent" || strOrEmpty(acc[0].Window) != "seven_day" ||
		acc[1].Check != "included" || acc[1].Why != nil {
		t.Fatalf("lane body accounts = %+v", acc)
	}
	if !u.timed() || *u.AvailableAt != unixSeconds(r7) || *u.AvailableBasis != "exact" {
		t.Fatalf("lane body time = %v %v, want a's 7d reset (exact)", u.AvailableAt, u.AvailableBasis)
	}
	if m := u.message("lane opus", now); !strings.Contains(m, "a spent (7d) until "+fmtAvail(r7, now)) {
		t.Fatalf("message = %q", m)
	}
}

// A spent record ends inside [quotaCooldown, quotaCooldownCap] of the refusal,
// whatever the upstream said; the reset it names is stored clamped to 8 d.
func TestSpentRecordClamp(t *testing.T) {
	availWorld(t, nil, nil)
	now := nowFn()
	near := func(a, b time.Time) bool { d := a.Sub(b); return d > -2*time.Second && d < 2*time.Second }
	if r := seedSpent("a", "five_hour", 429, "soon", now.Add(time.Minute)); !near(r.until, now.Add(quotaCooldown)) {
		t.Fatalf("a reset 1 min out ends at %v, want the %s floor", r.until, quotaCooldown)
	}
	if r := seedSpent("a", "seven_day", 429, "later", now.Add(48*time.Hour)); !near(r.until, now.Add(quotaCooldownCap)) ||
		!near(r.resets, now.Add(48*time.Hour)) {
		t.Fatalf("a reset 2 d out = %+v, want ending at the %s cap, resets kept", r, quotaCooldownCap)
	}
	if r := seedSpent("a", "scoped:Fable", 429, "absurd", now.Add(400*time.Hour)); !near(r.resets, now.Add(availabilityClamp)) {
		t.Fatalf("an absurd reset stored as %v, want clamped to %s", r.resets, availabilityClamp)
	}
	if r := seedSpent("a", "credits", 402, "none", time.Time{}); !near(r.until, now.Add(quotaCooldown)) {
		t.Fatalf("no reset ends at %v, want the floor", r.until)
	}
}

// §2.2 rows 8 and 16: a refused account is dialled again only when its record
// ends — no probe, no ride. The record closes the window it names for every
// route that window binds; the swap takes the other account; with both
// refused the lane refuses and dials nobody.
func TestRefusedAccountRedialledOnlyWhenRecordEnds(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	r7 := now.Add(72 * time.Hour)
	open := map[string]checkWindow{"five_hour": fiveHour(10, now.Add(time.Hour)), "seven_day": week(40, r7)}
	availWorld(t, map[string]lanePref{"opus": {Account: "a"}}, map[string]acctStanding{
		"a": standingAt("anthropic", now.Add(-time.Minute), open, ""),
		"b": standingAt("anthropic", now.Add(-time.Minute), open, ""),
	})
	withTokens(t, "a", "b")
	lan.pref.mu.Lock()
	lan.pref.account, lan.pref.ok = "a", true
	lan.pref.mu.Unlock()

	rec := seedSpent("a", "five_hour", 429, "rate limit rejected (5h)", now.Add(2*time.Hour))
	for _, at := range []time.Time{now, rec.until.Add(-time.Second)} {
		if c, _ := lan.routeCheck("lane:opus", "a", "", at); !c.no() || c.Reason != "spent" {
			t.Fatalf("a at %v = %s, want no(spent) while the record lives", at, c)
		}
	}
	d := lan.decideRoute("lane:opus", "opus", "", false)
	if d.none || d.acct != "b" || !d.check.included() {
		t.Fatalf("decide = %+v, want the swap to b", d)
	}
	seedSpent("b", "seven_day", 429, "rate limit rejected (7d)", r7)
	d = lan.decideRoute("lane:opus", "opus", "", false)
	if !d.none || d.tok != "" || len(d.refused) != 2 {
		t.Fatalf("decide = %+v, want a refusal that dials nobody", d)
	}
	if c, _ := lan.routeCheck("lane:opus", "a", "", rec.until.Add(time.Second)); !c.included() {
		t.Fatalf("a after its record = %s, want included", c)
	}
	// count_tokens asks entitlement only (row 20): it answers on a refused account.
	if d := lan.decideRoute("lane:opus", "opus", "", true); d.none || d.acct != "a" {
		t.Fatalf("count_tokens = %+v, want the pick, a, whatever its check", d)
	}
}

// A 2xx ends what it disproves on its account: the throttle, the unscoped
// windows, the served route's scoped window and the served model's record.
// Another scope's record stands, and so does a record newer than the success.
func TestNoteServedEndsWhatItDisproves(t *testing.T) {
	availWorld(t, nil, nil)
	at := nowFn()
	for _, l := range []string{"five_hour", "seven_day", "scoped:Opus", "scoped:Fable", modelLimit("m1"), modelLimit("m2")} {
		seedSpent("a", l, 429, "x", at.Add(time.Hour))
	}
	lan.recordThrottle("a", 429, "x", nil)
	seedSpent("b", "seven_day", 429, "x", at.Add(time.Hour))
	lan.noteServed("a", "Opus", "m1", at.Add(time.Second))
	lan.recMu.Lock()
	var left []string
	for k := range lan.records {
		left = append(left, k.account+"/"+k.limit)
	}
	lan.recMu.Unlock()
	sort.Strings(left)
	if got := strings.Join(left, " "); got != "a/model:m2 a/scoped:Fable b/seven_day" {
		t.Fatalf("records left = %s", got)
	}
	// A success that predates the refusal ends nothing.
	seedSpent("a", "seven_day", 429, "x", at.Add(time.Hour))
	lan.noteServed("a", "", "", at.Add(-time.Minute))
	lan.recMu.Lock()
	_, has := lan.records[recKey{"a", "seven_day"}]
	lan.recMu.Unlock()
	if !has {
		t.Fatal("a success older than the refusal ended its record")
	}
}

// A class is unavailable while no member has an account its check admits:
// the time is the earliest member's, a throttle does not make it available (a
// throttled account is admitted, so the class is not unavailable at all), and
// a member that serves ends it.
func TestClassUnavailableIsEveryMembersCheck(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	r1, r2 := now.Add(2*time.Hour), now.Add(30*time.Hour)
	availWorld(t, nil, map[string]acctStanding{
		"x": standingAt("anthropic", now, map[string]checkWindow{"five_hour": fiveHour(100, r1)}, ""),
		"y": standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(100, r2)}, ""),
		"z": standingAt("kimi", now, nil, ""),
	})
	lan.pref.mu.Lock()
	lan.pref.routes["c/x"] = memberRoute("c/x", "c", "x", "opus", "anthropic", "x")
	lan.pref.routes["c/y"] = memberRoute("c/y", "c", "y", "sonnet", "anthropic", "y")
	lan.pref.routes["c/z"] = memberRoute("c/z", "c", "z", "", "kimi", "z")
	lan.pref.mu.Unlock()
	mx := classMemberPref{ID: "x", Kind: "anthropic", Lane: "opus", Model: "m", Route: "c/x"}
	my := classMemberPref{ID: "y", Kind: "anthropic", Lane: "sonnet", Model: "m", Route: "c/y"}
	u := lan.classUnavailable([]classMemberPref{mx, my}, classReq{}, now)
	if u == nil || *u.AvailableAt != unixSeconds(r1) || u.claim() != "five_hour" {
		t.Fatalf("class = %+v, want unavailable until the earliest member (%v), five_hour", u, r1)
	}
	if len(u.Members) != 2 || u.Members[0].Route != "c/x" || u.Members[1].Route != "c/y" {
		t.Fatalf("members = %+v, want both routes in member order", u.Members)
	}
	lan.recordThrottle("x", 429, "busy", nil) // x is `no` and throttled: still unavailable
	lan.recordThrottle("y", 429, "busy", nil)
	lan.pref.mu.Lock()
	lan.pref.accounts["y"] = standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(50, r2)}, "")
	lan.pref.mu.Unlock()
	if u := lan.classUnavailable([]classMemberPref{mx, my}, classReq{}, now); u != nil {
		t.Fatalf("class = %+v, want available: y is throttled, and a throttle is not `no`", u)
	}
	lan.pref.mu.Lock()
	lan.pref.accounts["y"] = standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(100, r2)}, "")
	lan.pref.mu.Unlock()
	mz := classMemberPref{ID: "z", Kind: "provider", Provider: "kimi", Model: "k", Route: "c/z"}
	if u := lan.classUnavailable([]classMemberPref{mx, my, mz}, classReq{}, now); u == nil || u.Members[2].Unroutable == "" {
		t.Fatalf("class = %+v, want unavailable with z listed unroutable (kimi is not configured)", u)
	}
	if u := lan.classUnavailable([]classMemberPref{mz}, classReq{}, now); u != nil {
		t.Fatalf("class of z alone = %+v, want nil: nothing routable is a configuration error (typed 503)", u)
	}
	addProviders(t, &provider{name: "kimi"})
	if u := lan.classUnavailable([]classMemberPref{mx, my, mz}, classReq{}, now); u != nil {
		t.Fatalf("class = %+v, want available: z serves", u)
	}
}

// addProviders adds static providers for the length of a test.
func addProviders(t *testing.T, ps ...*provider) {
	t.Helper()
	old := providers
	providers = append(append([]*provider(nil), old...), ps...)
	t.Cleanup(func() { providers = old })
}

// Review #1: the hard line orders failover, it never excludes: the closed
// account is refused, the one under the line goes first, the one at 97 %
// after it.
func TestHardLineOrdersNeverExcludes(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	availWorld(t, nil, map[string]acctStanding{
		"x": standingAt("anthropic", now, map[string]checkWindow{"five_hour": fiveHour(100, now.Add(time.Hour))}, ""),
		"y": standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(97, now.Add(48*time.Hour))}, ""),
		"z": standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(50, now.Add(48*time.Hour))}, ""),
	})
	pct := map[string]float64{"x": 100, "y": 97, "z": 50}
	ok, refused := lan.rankRouteAccounts("lane:opus", "", checkRequest{}, "x", []string{"x", "y", "z"},
		func(a string) *float64 { v := pct[a]; return &v }, now)
	if len(ok) != 2 || ok[0].account != "z" || ok[1].account != "y" {
		t.Fatalf("admitted = %s, want z then y", renderPicks(ok))
	}
	if len(refused) != 1 || refused[0].account != "x" || !refused[0].check.no() {
		t.Fatalf("refused = %s, want x", renderPicks(refused))
	}
}

// §2.3: a throttle deranks within its funding tier — a throttled included
// account goes after an unthrottled one, even the incumbent, and still ahead
// of every paid account.
func TestThrottleDeranksAccount(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	r7 := now.Add(48 * time.Hour)
	availWorld(t, nil, map[string]acctStanding{
		"inc":  standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(10, r7)}, ""),
		"hot":  standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(80, r7)}, ""),
		"paid": standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(100, r7)}, "open"),
	})
	lan.pref.mu.Lock()
	r := lan.pref.routes["lane:opus"]
	r.Pay = []string{"paid/credits"}
	lan.pref.routes["lane:opus"] = r
	lan.pref.mu.Unlock()
	accts := []string{"inc", "hot", "paid"}
	ok, _ := lan.rankRouteAccounts("lane:opus", "", checkRequest{}, "inc", accts, nil, now)
	if renderPicks(ok) != "inc:included, hot:included, paid:paid:paid/credits" {
		t.Fatalf("unthrottled = %s", renderPicks(ok))
	}
	lan.recordThrottle("inc", 429, "busy", nil)
	ok, _ = lan.rankRouteAccounts("lane:opus", "", checkRequest{}, "inc", accts, nil, now)
	if renderPicks(ok) != "hot:included, inc:included+throttled, paid:paid:paid/credits" {
		t.Fatalf("throttled = %s, want the incumbent deranked behind hot and still ahead of paid", renderPicks(ok))
	}
}

// §2.7: per account the later as_of wins — a brain that restarted and has not
// polled (as_of null) does not blank what this proxy holds; next_poll_at is
// always the brain's newest; an account the brain no longer serves is gone.
func TestMergeStandings(t *testing.T) {
	t0 := time.Now().Truncate(time.Second)
	held := standingAt("anthropic", t0, map[string]checkWindow{"seven_day": week(40, t0.Add(time.Hour))}, "")
	unpolled := acctStanding{Provider: "anthropic", NextPollAt: epoch(t0.Add(time.Minute))}
	newer := standingAt("anthropic", t0.Add(time.Minute), map[string]checkWindow{"seven_day": week(41, t0.Add(time.Hour))}, "")
	older := standingAt("anthropic", t0.Add(-time.Minute), map[string]checkWindow{"seven_day": week(10, t0.Add(time.Hour))}, "")
	got := mergeStandings(map[string]acctStanding{"a": held, "b": held, "c": held, "gone": held},
		map[string]acctStanding{"a": unpolled, "b": newer, "c": older})
	if a := got["a"]; a.AsOf == nil || *a.AsOf != *held.AsOf || *a.NextPollAt != *unpolled.NextPollAt {
		t.Fatalf("a = %+v, want the held reading with the new next_poll_at", a)
	}
	if b := got["b"]; *b.Allowance.Windows["seven_day"].UsedPct != 41 {
		t.Fatalf("b = %+v, want the newer reading", b)
	}
	if c := got["c"]; *c.Allowance.Windows["seven_day"].UsedPct != 40 {
		t.Fatalf("c = %+v, want the held, later reading", c)
	}
	if _, has := got["gone"]; has {
		t.Fatal("an account the brain dropped survived the merge")
	}
}

// §1.6's overlay: a response's own limit headers, newer than the brain's
// reading, replace the windows they carry, and each carried window is as fresh
// as the response (staleness is per limit, §1.4). A closed codex gate stands
// until the poll's windows reset. Credits can be closed by a header, never
// opened by one.
func TestHeaderOverlay(t *testing.T) {
	t0 := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	r5, r7 := t0.Add(3*time.Hour), t0.Add(96*time.Hour)
	shut := false
	gpt := standingAt("codex", t0, map[string]checkWindow{"five_hour": fiveHour(100, r5), "seven_day": week(60, r7)}, "open")
	gpt.Allowance.Open = &shut
	availWorld(t, nil, map[string]acctStanding{"gpt-a": gpt})
	lan.pref.mu.Lock()
	lan.pref.routes["c/gpt"] = memberRoute("c/gpt", "c", "gpt", "", "codex", "gpt-a")
	lan.pref.mu.Unlock()
	if c, _ := lan.routeCheck("c/gpt", "gpt-a", "", time.Now()); !c.no() {
		t.Fatalf("before = %s, want no", c)
	}
	partial := rlHeader("x-codex-primary-used-percent", "5", "x-codex-primary-window-minutes", "300",
		"x-codex-primary-reset-after-seconds", "9000")
	lan.noteHeaderReading("gpt-a", partial, t0.Add(time.Minute))
	st := lan.standingFor("gpt-a", time.Now())
	if *st.Allowance.Windows["five_hour"].UsedPct != 5 || *st.AsOf != *gpt.AsOf || st.Allowance.Open == nil ||
		*st.Allowance.Windows["seven_day"].AsOf != *gpt.AsOf {
		t.Fatalf("partial overlay = %+v, want five_hour moved; seven_day's age, as_of and the gate kept", st)
	}
	if c, _ := lan.routeCheck("c/gpt", "gpt-a", "", time.Now()); !c.no() {
		t.Fatalf("partial = %s, want no: the brain's closed gate still stands", c)
	}
	full := rlHeader("x-codex-primary-used-percent", "5", "x-codex-primary-window-minutes", "300",
		"x-codex-primary-reset-after-seconds", "9000",
		"x-codex-secondary-used-percent", "61", "x-codex-secondary-window-minutes", "10080",
		"x-codex-secondary-reset-at", "1893456000",
		"x-codex-credits-has-credits", "False", "x-codex-credits-unlimited", "False")
	at := t0.Add(2 * time.Minute)
	lan.noteHeaderReading("gpt-a", full, at)
	st = lan.standingFor("gpt-a", time.Now())
	if *st.AsOf != *gpt.AsOf || *st.Allowance.Windows["seven_day"].AsOf != unixSeconds(at) ||
		*st.Allowance.Windows["seven_day"].ResetsAt != 1893456000 {
		t.Fatalf("full overlay = %+v, want each window read at %v and the poll's as_of kept", st, at)
	}
	if p := st.Money[potCredits]; p.Open == nil || *p.Open {
		t.Fatalf("credits = %+v, want closed by has-credits False", p)
	}
	// The gate the poll read closed stands until every window it could mean
	// has reset (the brain's _allowance_state), whatever the response shows.
	if c, _ := lan.routeCheck("c/gpt", "gpt-a", "", time.Now()); !c.no() {
		t.Fatalf("after = %s, want no: the gate stands until the poll's windows reset", c)
	}
	if st := lan.standingFor("gpt-a", r7.Add(time.Minute)); st.Allowance.Open != nil {
		t.Fatalf("past every reset the gate = %v, want gone", *st.Allowance.Open)
	}
	// An older reading never replaces a newer one.
	lan.noteHeaderReading("gpt-a", partial, t0.Add(30*time.Second))
	if st := lan.standingFor("gpt-a", time.Now()); *st.Allowance.Windows["five_hour"].AsOf != unixSeconds(at) {
		t.Fatal("an older response replaced a newer reading")
	}
	// A reading older than the brain's is not applied.
	lan.recMu.Lock()
	lan.readings = map[string]headerReading{}
	lan.recMu.Unlock()
	lan.noteHeaderReading("gpt-a", full, t0.Add(-time.Minute))
	if c, _ := lan.routeCheck("c/gpt", "gpt-a", "", time.Now()); !c.no() {
		t.Fatalf("stale overlay = %s, want the brain's newer reading (no)", c)
	}
}

// Codex names a window by its length, with the brain's slack; any other
// length is its own window:<period>, as the brain's _slot_id names it.
func TestCodexWindowName(t *testing.T) {
	for secs, want := range map[float64][2]string{18000: {"five_hour", "5h"}, 18000 + 179: {"five_hour", "5h"},
		604800: {"seven_day", "7d"}, 604800 - 180: {"seven_day", "7d"}, 86400: {"window:1d", "1d"},
		10800: {"window:3h", "3h"}, 18000 + 181: {"window:18181s", "18181s"}, 0: {"", ""}} {
		if n, p := codexWindowName(secs, nil); [2]string{n, p} != want {
			t.Errorf("codexWindowName(%v) = %q %q, want %v", secs, n, p, want)
		}
	}
	// A slot already taken leaves a second window of that length its own.
	taken := map[string]checkWindow{"five_hour": {}}
	if n, _ := codexWindowName(18000, taken); n != "window:5h" {
		t.Errorf("second 5h window = %q, want window:5h", n)
	}
	taken["window:5h"] = checkWindow{}
	if n, _ := codexWindowName(18000, taken); n != "window:5h#2" {
		t.Errorf("third 5h window = %q, want window:5h#2", n)
	}
}

// A codex response carrying a window of another length reads it as that
// window, which binds every route (the brain's window:<period>), and does
// not drop it.
func TestCodexHeaderOtherLengthWindow(t *testing.T) {
	h := rlHeader("x-codex-primary-used-percent", "100", "x-codex-primary-window-minutes", "1440",
		"x-codex-primary-reset-after-seconds", "600")
	hr, ok := parseCodexHeaders(h, time.Unix(1800000000, 0))
	if !ok {
		t.Fatal("not read as a codex reading")
	}
	w, has := hr.windows["window:1d"]
	if !has || w.Period != "1d" || w.Scope != nil || *w.UsedPct != 100 || *w.ResetsAt != 1800000600 {
		t.Fatalf("windows = %+v", hr.windows)
	}
}

type headerRecorder struct {
	h    http.Header
	code int
}

func (r *headerRecorder) Header() http.Header         { return r.h }
func (r *headerRecorder) Write(b []byte) (int, error) { return len(b), nil }
func (r *headerRecorder) WriteHeader(c int)           { r.code = c }

// withTokens seeds the token cache so the swap finds a bearer without a
// brain, and points the brain at a dead port so any other fetch fails fast.
func withTokens(t *testing.T, accts ...string) {
	t.Helper()
	lan.tokMu.Lock()
	old := lan.tokens
	lan.tokens = map[string]tokEntry{}
	for _, a := range accts {
		lan.tokens[a] = tokEntry{token: "tok-" + a, exp: time.Now().Add(time.Hour)}
	}
	lan.tokMu.Unlock()
	oldBrain := brainBase
	brainBase = "http://127.0.0.1:1"
	t.Cleanup(func() {
		lan.tokMu.Lock()
		lan.tokens = old
		lan.tokMu.Unlock()
		brainBase = oldBrain
	})
}

// A closed window with no reset is retried at the next poll, which will say
// (next_poll); with no next poll known either, no time is known. The reply is
// still retryable (a reading may reopen it; no money is asked), and it states
// no time: no Retry-After and no minted reset.
func TestUnavailableUnknownTime(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	nr := checkWindow{UsedPct: ptr(100), Period: "5h"}
	st := standingAt("anthropic", now, map[string]checkWindow{"five_hour": nr}, "")
	availWorld(t, nil, map[string]acctStanding{"x": st})
	u := lan.laneUnavailable("opus", "", now)
	if !u.timed() || *u.AvailableAt != unixSeconds(now.Add(5*time.Minute)) || *u.AvailableBasis != "next_poll" {
		t.Fatalf("lane = %+v, want back at the next poll (next_poll)", u)
	}
	st.NextPollAt = nil
	availWorld(t, nil, map[string]acctStanding{"x": st})
	u = lan.laneUnavailable("opus", "", now)
	if u.timed() || u.RetryAfterS != nil || !u.retryable() {
		t.Fatalf("lane = %+v, want retryable with no time", u)
	}
	status, hd, body, _ := renderUnavailable(u, "lane opus", "", "r", replyClaudeCode, now)
	if status != http.StatusTooManyRequests || !strings.Contains(string(body), `"rate_limit_error"`) ||
		hd.Get("Retry-After") != "" || hd.Get("X-Switch-Available-At") != "unknown" {
		t.Fatalf("no-time reply = %d %v %s, want 429 rate_limit_error with no Retry-After", status, hd, body)
	}
	for k := range hd {
		if strings.HasPrefix(strings.ToLower(k), "anthropic-ratelimit-unified-") {
			t.Fatalf("a reply with no time minted %s", k)
		}
	}
}

// A lane no account serves has no check, so there is no unavailable reply to
// give: that is configuration, the typed 503 with no Retry-After (§2.8 item
// 6), never the 402 money reply with nothing to fund.
func TestLaneWithNoAccountIsTyped503(t *testing.T) {
	availWorld(t, nil, nil)
	now := nowFn()
	if u := lan.laneUnavailable("opus", "", now); u != nil {
		t.Fatalf("a lane with no account has a body: %+v", u)
	}
	rec := httptest.NewRecorder()
	lan.writeLaneRefused(rec, &laneDecision{refusedRoute: "lane:opus"}, http.Header{}, now)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "" ||
		rec.Header().Get("X-Switch-Quota") != "" {
		t.Fatalf("lane with no account = %d %v, want the typed 503", rec.Code, rec.Header())
	}
	if !strings.Contains(rec.Body.String(), "the brain serves no account on lane opus") {
		t.Fatalf("typed 503 lacks the reason: %s", rec.Body.String())
	}
}

// §5.1's brain gap: the brain has been down past stale_max, credits are open
// and the route's pay is []. The check is no (unknown) until the next poll the
// brain promised, which has passed. The reply never states that time: no
// Retry-After (a 1 s one would stand in for "unknown"), no minted unified
// reset in the past, X-Switch-Available-At unknown — on every wire. And it is
// never the money reply: a no (unknown) is retryable whatever its time, since
// the brain coming back is a reading, not an owner action.
func TestUnavailableNeverStatesAPassedTime(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	read := now.Add(-2 * time.Hour)
	st := standingAt("anthropic", read, map[string]checkWindow{"five_hour": fiveHour(40, now.Add(time.Hour))}, "open")
	availWorld(t, nil, map[string]acctStanding{"x": st})
	if c, _ := lan.routeCheck("lane:opus", "x", "", now); !c.no() || c.Reason != "unknown" || !c.Until.known() || !c.Until.Time().Before(now) {
		t.Fatalf("check = %s, want no (unknown) until a next poll already past", c)
	}
	u := lan.laneUnavailable("opus", "", now)
	if a := u.Members[0].Accounts[0]; a.Until.known() || a.Window != nil {
		t.Fatalf("account = %+v, want until null: a passed time is unknown", a)
	}
	if u.timed() || u.AvailableAt != nil || u.RetryAfterS != nil || !u.retryable() {
		t.Fatalf("body time = %v %v retryable %v, want none, retryable", u.AvailableAt, u.RetryAfterS, u.Retryable)
	}
	wants := map[replyWire]struct {
		status int
		kind   string
	}{
		replyClaudeCode: {http.StatusTooManyRequests, `"rate_limit_error"`},
		replyAnthropic:  {529, `"overloaded_error"`},
		replyOpenAI:     {http.StatusTooManyRequests, `"rate_limit_exceeded"`},
	}
	for _, rw := range []replyWire{replyClaudeCode, replyAnthropic, replyOpenAI} {
		status, hd, body, msg := renderUnavailable(u, "lane opus", "", "r", rw, now)
		if w := wants[rw]; status != w.status || !strings.Contains(string(body), w.kind) || hd.Get("x-should-retry") != "" {
			t.Errorf("%s: %d x-should-retry %q %s, want the retryable %d %s", rw, status, hd.Get("x-should-retry"), body, w.status, w.kind)
		}
		if !strings.Contains(msg, "retry later") {
			t.Errorf("%s: the message does not say to retry: %s", rw, msg)
		}
		if hd.Get("Retry-After") != "" || hd.Get("X-Switch-Available-At") != "unknown" {
			t.Errorf("%s: Retry-After %q, X-Switch-Available-At %q, want none and unknown", rw, hd.Get("Retry-After"), hd.Get("X-Switch-Available-At"))
		}
		for k := range hd {
			if strings.HasPrefix(strings.ToLower(k), "anthropic-ratelimit-unified-") {
				t.Errorf("%s: a reply with no time minted %s", rw, k)
			}
		}
		if strings.Contains(msg, fmtAvail(read.Add(5*time.Minute), now)) || !strings.Contains(msg, "x unknown") {
			t.Errorf("%s: the message states the passed time: %s", rw, msg)
		}
	}
}
