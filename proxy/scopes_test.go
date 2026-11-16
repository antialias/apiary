package main

// C5: Anthropic headers read by meaning. The fixtures are two captured Fable 429s
// (quota-model 10 §2), on lane fable. They are the report's header sets verbatim; the report keeps no
// 5h-reset value, so that header is absent from both.

import (
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

var (
	capture2110At = time.Date(2026, 10, 29, 21, 10, 44, 0, time.UTC)
	capture2239At = time.Date(2026, 10, 29, 22, 39, 31, 0, time.UTC)

	capture2110 = map[string]string{
		"anthropic-ratelimit-unified-status":                    "rejected",
		"anthropic-ratelimit-unified-reset":                     "1793412000",
		"anthropic-ratelimit-unified-representative-claim":      "seven_day_overage_included",
		"anthropic-ratelimit-unified-5h-status":                 "allowed",
		"anthropic-ratelimit-unified-5h-utilization":            "0.07",
		"anthropic-ratelimit-unified-7d-status":                 "allowed_warning",
		"anthropic-ratelimit-unified-7d-utilization":            "0.89",
		"anthropic-ratelimit-unified-7d-surpassed-threshold":    "0.75",
		"anthropic-ratelimit-unified-7d-reset":                  "1793412000",
		"anthropic-ratelimit-unified-7d_oi-status":              "rejected",
		"anthropic-ratelimit-unified-7d_oi-utilization":         "1.0",
		"anthropic-ratelimit-unified-7d_oi-surpassed-threshold": "1.0",
		"anthropic-ratelimit-unified-7d_oi-reset":               "1793412000",
		"anthropic-ratelimit-unified-fallback-percentage":       "0.5",
		"anthropic-ratelimit-unified-overage-status":            "rejected",
		"anthropic-ratelimit-unified-overage-disabled-reason":   "org_level_disabled",
		"anthropic-ratelimit-unified-upgrade-paths":             "overage",
	}
	capture2239 = withHeader(capture2110, "anthropic-ratelimit-unified-5h-utilization", "0.0")
)

// fable429Body is the body both captures carried: it names no model.
const fable429Body = `{"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later."}}`

func withHeader(src map[string]string, k, v string) map[string]string {
	out := make(map[string]string, len(src))
	for a, b := range src {
		out[a] = b
	}
	out[k] = v
	return out
}

// capturedFable429 is a capture as a header, its reset moved to reset (zero
// keeps the captured 1793412000), so a routing test can run at any date.
func capturedFable429(capture map[string]string, reset time.Time) http.Header {
	h := http.Header{}
	for k, v := range capture {
		if v == "1793412000" && !reset.IsZero() {
			v = strconv.FormatInt(reset.Unix(), 10)
		}
		h.Set(k, v)
	}
	return h
}

// fableAliases is the brain's scoped:Fable window's aliases as accounts{}
// publishes them (C5): the header token and the claim a Fable refusal arrives
// as.
func fableAliases() *windowAliases {
	return &windowAliases{Header: []string{"7d_oi"}, Claim: []string{"seven_day_overage_included"}}
}

// aliasedAccounts is one standing per account, each with the scoped windows
// given and those windows' aliases: the slice of accounts{} the scope map reads.
func aliasedAccounts(windows map[string]map[string]*windowAliases) map[string]acctStanding {
	out := map[string]acctStanding{}
	for a, ws := range windows {
		var st acctStanding
		st.Allowance.Windows = map[string]checkWindow{}
		for n, al := range ws {
			w := windowFor(n)
			w.Aliases = al
			st.Allowance.Windows[n] = w
		}
		out[a] = st
	}
	return out
}

// setFableAliases puts aliases on every account's scoped:Fable window, as the
// brain's poll of it publishes them; nil takes them off (a brain that knows
// none).
func setFableAliases(al *windowAliases) {
	pref.mu.Lock()
	defer pref.mu.Unlock()
	for _, st := range pref.accounts {
		if w, ok := st.Allowance.Windows["scoped:Fable"]; ok {
			w.Aliases = al
			st.Allowance.Windows["scoped:Fable"] = w
		}
	}
}

// fableWorld is account a on every bare lane, measured at polled: 5h at 7 %,
// 7d at 89 % and the Fable cap at pct, the brain publishing Fable's aliases.
func fableWorld(t *testing.T, pct float64, reset, polled time.Time) {
	t.Helper()
	availWorld(t, map[string]lanePref{"fable": {Account: "a"}, "opus": {Account: "a"}}, map[string]acctStanding{
		"a": standingAt("anthropic", polled, map[string]checkWindow{
			"five_hour": fiveHour(7, polled.Add(3*time.Hour)), "seven_day": week(89, reset),
			"scoped:Fable": scoped("Fable", pct, reset)}, ""),
	})
	setFableAliases(fableAliases())
}

// laneCheck is account a's check on a bare lane.
func laneCheck(lane string, at time.Time) dialCheck {
	c, _ := routeCheck("lane:"+lane, "a", "", at)
	return c
}

// Both captures name the Fable cap: 7d_oi and its claim map to the brain's
// scoped:Fable, and the overage set is the credits pot, not a window.
func TestCapturedFable429MapsToFableScope(t *testing.T) {
	sm := newScopeMap(brainLaneRules, aliasedAccounts(map[string]map[string]*windowAliases{
		"a": {"scoped:Fable": fableAliases()}}))
	for _, c := range []struct {
		name   string
		at     time.Time
		h      map[string]string
		fivePc float64
	}{
		{"21:10:44Z", capture2110At, capture2110, 7},
		{"22:39:31Z", capture2239At, capture2239, 0},
	} {
		o, ok := parseUnifiedRL(capturedFable429(c.h, time.Time{}), c.at, sm)
		if !ok {
			t.Fatalf("%s: the capture parsed as no observation", c.name)
		}
		if o.claimName != "scoped:Fable" || o.reset.Unix() != 1793412000 {
			t.Errorf("%s: claim %q -> %q reset %v, want scoped:Fable at 1793412000", c.name, o.claim, o.claimName, o.reset.Unix())
		}
		got := map[string]rlWindow{}
		for _, w := range o.windows {
			got[w.name] = w
		}
		if len(o.windows) != 3 {
			t.Errorf("%s: windows = %+v, want 5h, 7d and 7d_oi only (overage, fallback, upgrade-paths are not windows)", c.name, o.windows)
		}
		near := func(a, b float64) bool { return math.Abs(a-b) < 1e-9 }
		if w := got["scoped:Fable"]; w.token != "7d_oi" || w.status != "rejected" || !near(w.pct, 100) || w.reset.Unix() != 1793412000 {
			t.Errorf("%s: scoped:Fable = %+v", c.name, w)
		}
		if w := got["seven_day"]; w.status != "allowed_warning" || !near(w.pct, 89) {
			t.Errorf("%s: seven_day = %+v", c.name, w)
		}
		if w := got["five_hour"]; w.status != "allowed" || !near(w.pct, c.fivePc) {
			t.Errorf("%s: five_hour = %+v", c.name, w)
		}
		if len(o.credits) != 2 || o.credits["status"] != "rejected" || o.credits["disabled-reason"] != "org_level_disabled" {
			t.Errorf("%s: credits pot = %+v, want rejected / org_level_disabled", c.name, o.credits)
		}
		if rej := o.rejectedWindows(); len(rej) != 1 || rej[0] != "scoped:Fable" {
			t.Errorf("%s: rejected = %v, want [scoped:Fable]", c.name, rej)
		}
		if !sm.binds("scoped:Fable", "fable") || sm.binds("scoped:Fable", "opus") || sm.binds("scoped:Fable", "default") {
			t.Errorf("%s: scoped:Fable must bind fable and nothing else", c.name)
		}
	}
}

// recordAnthropic429 records a 429 carrying h, as the walk does: the
// anthropic adapter classifies it, and the record store keeps what it names.
func recordAnthropic429(account, model string, h http.Header) (kind, what string) {
	resp := &http.Response{StatusCode: http.StatusTooManyRequests, Header: h,
		Body: io.NopCloser(strings.NewReader(`{"type":"error","error":{"type":"rate_limit_error","message":"rate limited"}}`))}
	return recordRefusal(account, anthropicRefusal(resp, model, nowFn()), resp)
}

// A Fable 429 records on the Fable scope only (§2.6, A-D1, A-D2): the fable
// route's check is `no` until the 7d_oi reset; opus, sonnet, haiku and default
// on the same account stay included. The record stands past the old 300 s
// gate: the window is spent until its reset (capped), not for five minutes —
// the re-probe loop is gone.
func TestScopedFable429RecordsOnFableOnly(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	reset := now.Add(28 * time.Hour)
	fableWorld(t, 99, reset, now.Add(-time.Minute))
	kind, what := recordAnthropic429("a", "claude-fable-5", capturedFable429(capture2239, reset))
	if kind != recSpent || what != "scoped:Fable" {
		t.Fatalf("recorded %s %s, want spent scoped:Fable", kind, what)
	}
	for _, l := range []string{"opus", "sonnet", "haiku", "default"} {
		if c := laneCheck(l, now); !c.included() {
			t.Errorf("%s on a = %s, want included: the Fable cap is not its limit", l, c)
		}
	}
	c := laneCheck("fable", now)
	if !c.no() || c.Reason != "spent" || !c.Until.Time().Equal(reset) {
		t.Fatalf("fable on a = %s, want no(spent) until the 7d_oi reset", c)
	}
	u := laneUnavailable("fable", "", now)
	if m := u.message("lane fable", now); !strings.Contains(m, "a spent (Fable) until "+fmtAvail(reset, now)) {
		t.Errorf("message = %q", m)
	}
	if c := laneCheck("fable", now.Add(forcedCooldown+time.Second)); !c.no() {
		t.Errorf("fable past the old gate = %s, want still no", c)
	}
}

// unified-status=rejected with unified-reset is an explicit record even with
// no per-window status: the claim names the window, unified-reset says until
// when, and a member whose every account is so refused is spent — its check is
// `no` — for the class's quota verdict, quoting the refusal.
func TestRejectedResetIsExplicitQuotaRecord(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	reset := now.Add(28 * time.Hour)
	fableWorld(t, 99, reset, now.Add(-time.Minute))
	withTokens(t, "a")
	h := rlHeader(
		"anthropic-ratelimit-unified-status", "rejected",
		"anthropic-ratelimit-unified-reset", strconv.FormatInt(reset.Unix(), 10),
		"anthropic-ratelimit-unified-representative-claim", "seven_day_overage_included")
	if kind, what := recordAnthropic429("a", "", h); kind != recSpent || what != "scoped:Fable" {
		t.Fatalf("recorded %s %s, want the record on the claimed scope", kind, what)
	}
	if c := laneCheck("fable", now); !c.no() || !c.Until.Time().Equal(reset) {
		t.Fatalf("fable = %s, want no until unified-reset", c)
	}
	if c := laneCheck("opus", now); !c.included() {
		t.Fatalf("opus = %s, want included", c)
	}
	pref.mu.Lock()
	pref.routes["frontier/lane:fable"] = fableMemberRoute()
	pref.mu.Unlock()
	fable := classMemberPref{ID: "lane:fable", Kind: "anthropic", Lane: "fable", Model: "claude-fable-5", Route: "frontier/lane:fable"}
	u := classUnavailable([]classMemberPref{fable}, classReq{}, now)
	if u == nil || len(u.Members) != 1 {
		t.Fatalf("classUnavailable = %+v, want lane:fable unavailable", u)
	}
	a := u.Members[0].Accounts[0]
	if !a.Until.Time().Equal(reset) || strOrEmpty(a.Window) != "scoped:Fable" || a.Detail != "429 rate limit rejected (Fable)" {
		t.Fatalf("account = %+v, want a 429 on the Fable window, back at its reset", a)
	}
	if *u.AvailableBasis != "exact" || *u.AvailableAt != unixSeconds(reset) {
		t.Fatalf("time = %v %v, want the upstream's reset, exact (the record clamps only its own end)",
			*u.AvailableAt, *u.AvailableBasis)
	}

	// A sibling account that serves means the member is not spent.
	pref.mu.Lock()
	pref.accounts["b"] = standingAt("anthropic", now, map[string]checkWindow{"seven_day": week(10, reset)}, "")
	r := pref.routes["frontier/lane:fable"]
	r.Accounts = []string{"a", "b"}
	pref.routes["frontier/lane:fable"] = r
	pref.mu.Unlock()
	if u := classUnavailable([]classMemberPref{fable}, classReq{}, now); u != nil {
		t.Fatal("lane:fable read unavailable while account b serves it")
	}
}

// Every overage-* field is the credits pot's state, never a window — the
// <field>-shaped ones (overage-reset, overage-utilization) included.
func TestOverageIsPotState(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	o, ok := parseUnifiedRL(rlHeader(
		"anthropic-ratelimit-unified-status", "allowed",
		"anthropic-ratelimit-unified-7d-utilization", "0.5",
		"anthropic-ratelimit-unified-overage-status", "rejected",
		"anthropic-ratelimit-unified-overage-disabled-reason", "out_of_credits",
		"anthropic-ratelimit-unified-overage-reset", "1793412000",
		"anthropic-ratelimit-unified-overage-utilization", "1.0"), now, scopeMap{})
	if !ok || len(o.windows) != 1 || o.windows[0].name != "seven_day" {
		t.Fatalf("windows = %+v ok=%v, want seven_day only", o.windows, ok)
	}
	want := rlPot{"status": "rejected", "disabled-reason": "out_of_credits", "reset": "1793412000", "utilization": "1.0"}
	if len(o.credits) != len(want) {
		t.Fatalf("credits = %v, want %v", o.credits, want)
	}
	for k, v := range want {
		if o.credits[k] != v {
			t.Fatalf("credits = %v, want %v", o.credits, want)
		}
	}
}

// The pot alone is not rate-limit state: a 429 that carries only overage-* is
// a throttle. It deranks the account and closes no window.
func TestOverageOnly429IsAThrottle(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	fableWorld(t, 10, now.Add(28*time.Hour), now.Add(-time.Minute))
	h := rlHeader(
		"anthropic-ratelimit-unified-overage-status", "rejected",
		"anthropic-ratelimit-unified-overage-disabled-reason", "out_of_credits")
	if _, ok := parseUnifiedRL(h, now, scopeMap{}); ok {
		t.Fatal("an overage-only response parsed as an observation")
	}
	if kind, _ := recordAnthropic429("a", "", h); kind != recThrottled {
		t.Fatalf("recorded %s, want a throttle", kind)
	}
	if c := laneCheck("fable", now); !c.included() {
		t.Fatalf("fable = %s, want included: a throttle never makes the check no", c)
	}
	if throttledUntil("a", now).IsZero() {
		t.Fatal("fable's account a is not throttled")
	}
}

// A token no window claims (an unseen family: Opus, Sonnet and Mythos have no
// capture, quota-model 10 U4) stays unmapped — never guessed from its
// spelling. Its record is unattributed (§2.6): keyed by the model that drew
// it, so it holds that model on that account and nothing else, and PR11 may
// re-admit it. The brain's poll of that family still binds its lane.
func TestUnknownScopedTokenStaysUnmapped(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	reset := now.Add(28 * time.Hour)
	fableWorld(t, 10, reset, now.Add(-time.Minute))
	h := rlHeader(
		"anthropic-ratelimit-unified-status", "rejected",
		"anthropic-ratelimit-unified-reset", strconv.FormatInt(reset.Unix(), 10),
		"anthropic-ratelimit-unified-representative-claim", "seven_day_opus",
		"anthropic-ratelimit-unified-7d_opus-status", "rejected",
		"anthropic-ratelimit-unified-7d_opus-reset", strconv.FormatInt(reset.Unix(), 10))
	o, _ := parseUnifiedRL(h, now, currentScopes())
	if len(o.windows) != 1 || o.windows[0].name != "unmapped:7d_opus" || o.claimName != "unmapped:seven_day_opus" {
		t.Fatalf("o = %+v, want 7d_opus and seven_day_opus unmapped", o)
	}
	if l := windowLabel("unmapped:7d_opus"); l != "7d_opus (unmapped)" {
		t.Fatalf("label = %q", l)
	}
	if kind, what := recordAnthropic429("a", "claude-opus-5", h); kind != recSpent || what != "model:claude-opus-5" {
		t.Fatalf("recorded %s %s, want the model record", kind, what)
	}
	if c, held := routeCheck("lane:opus", "a", "claude-opus-5", now); !c.no() || !held {
		t.Fatalf("opus/claude-opus-5 = %s held=%v, want no, held by the unattributed record alone", c, held)
	}
	for _, l := range []string{"opus", "fable", "sonnet", "default"} {
		if c, _ := routeCheck("lane:"+l, "a", "claude-sonnet-5", now); !c.included() {
			t.Errorf("%s with another model = %s, want included", l, c)
		}
	}
	// The poll's scoped:Opus at 100 % binds opus by the lane's scope, and
	// only opus.
	pref.mu.Lock()
	st := pref.accounts["a"]
	st.Allowance.Windows["scoped:Opus"] = scoped("Opus", 100, reset)
	pref.accounts["a"] = st
	pref.mu.Unlock()
	if c := laneCheck("opus", now); !c.no() {
		t.Fatalf("opus = %s, want no on the poll's scoped:Opus", c)
	}
	if c := laneCheck("sonnet", now); !c.included() {
		t.Fatalf("sonnet = %s, want included", c)
	}
}

// A brain that publishes lane scopes but no aliases for a scoped window. The
// 429's 7d_oi and its claim stay unmapped: the record is the model's, so the
// account keeps serving every other model. The brain's own reading of the
// scope still binds its lane.
func TestProxyWithoutBrainAliases(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	reset := now.Add(28 * time.Hour)
	fableWorld(t, 100, reset, now.Add(-time.Minute))
	setFableAliases(nil)
	sm := currentScopes()
	if len(sm.header) != 0 || len(sm.claim) != 0 || sm.lane["fable"] != "scoped:Fable" {
		t.Fatalf("scopes = %+v, want the lane join and no aliases", sm)
	}
	kind, what := recordAnthropic429("a", "claude-fable-5", capturedFable429(capture2110, reset))
	if kind != recSpent || what != "model:claude-fable-5" {
		t.Fatalf("recorded %s %s, want the unmapped windows on the model", kind, what)
	}
	if c := laneCheck("opus", now); !c.included() {
		t.Errorf("opus = %s, want included", c)
	}
	if c := laneCheck("fable", now); !c.no() {
		t.Errorf("fable = %s, want no on the brain's scoped:Fable", c)
	}
}

// An alias two windows both claim is dropped, not guessed between.
func TestScopeMapDropsClashingAlias(t *testing.T) {
	both := &windowAliases{Header: []string{"7d_x"}}
	sm := newScopeMap(brainLaneRules, aliasedAccounts(map[string]map[string]*windowAliases{
		"a": {"scoped:Fable": both}, "b": {"scoped:Opus": both}, "c": {"scoped:Fable": both},
	}))
	if got := sm.window("7d_x"); got != "unmapped:7d_x" {
		t.Fatalf("window(7d_x) = %q, want unmapped", got)
	}
	if got := sm.window("5h"); got != "five_hour" {
		t.Fatalf("window(5h) = %q", got)
	}
	if got := sm.claimWindow(""); got != "seven_day" {
		t.Fatalf("claimWindow(\"\") = %q", got)
	}
}

// accountClaimBesideFable is the review's probe: the overall verdict claims the
// account's 7d while the only per-window rejection is the Fable cap.
func accountClaimBesideFable() http.Header {
	return rlHeader(
		"anthropic-ratelimit-unified-status", "rejected",
		"anthropic-ratelimit-unified-representative-claim", "seven_day",
		"anthropic-ratelimit-unified-7d-status", "allowed_warning",
		"anthropic-ratelimit-unified-7d_oi-status", "rejected")
}

// A claim naming an account-wide window counts alongside the per-window
// rejections: both are recorded, and every lane reads `no` on the claimed 7d
// until unified-reset — not opus alone with sonnet left available.
func TestAccountClaimBesideScopedRejectionIsAccountWide(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	reset := now.Add(28 * time.Hour)
	fableWorld(t, 99, reset, now.Add(-time.Minute))
	h := accountClaimBesideFable()
	h.Set("anthropic-ratelimit-unified-reset", strconv.FormatInt(reset.Unix(), 10))
	if kind, what := recordAnthropic429("a", "", h); kind != recSpent || what != "scoped:Fable,seven_day" {
		t.Fatalf("recorded %s %s, want both windows", kind, what)
	}
	for _, l := range []string{"opus", "sonnet", "haiku", "default", "fable"} {
		if c := laneCheck(l, now); !c.no() || !c.Until.Time().Equal(reset) {
			t.Errorf("%s on a = %s, want no until unified-reset", l, c)
		}
	}
}

// A scoped rejection met on a route that scope does not bind (the Fable cap
// seen on an opus request) records on the Fable cap alone (§2.6: "closes Fable
// on that account and not opus, sonnet or haiku").
func TestScopedRejectionOffItsLaneRecordsItsScope(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	reset := now.Add(28 * time.Hour)
	fableWorld(t, 99, reset, now.Add(-time.Minute))
	if kind, what := recordAnthropic429("a", "claude-opus-5", capturedFable429(capture2110, reset)); kind != recSpent || what != "scoped:Fable" {
		t.Fatalf("recorded %s %s, want scoped:Fable", kind, what)
	}
	if c := laneCheck("opus", now); !c.included() {
		t.Errorf("opus = %s, want included: the Fable cap is not its limit", c)
	}
	if c := laneCheck("fable", now); !c.no() {
		t.Errorf("fable = %s, want no on the Fable cap", c)
	}
}

// A brain poll at 100 % makes the check `no` with no refusal at all (§2.2
// row 5: the check replaces the cooldown): the member is spent for the
// class's verdict, and the message says what its check says.
func TestPollAt100IsSpent(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	reset := now.Add(28 * time.Hour)
	fableWorld(t, 100, reset, now.Add(-time.Minute))
	pref.mu.Lock()
	pref.routes["frontier/lane:fable"] = fableMemberRoute()
	pref.mu.Unlock()
	fable := classMemberPref{ID: "lane:fable", Kind: "anthropic", Lane: "fable", Model: "m", Route: "frontier/lane:fable"}
	u := classUnavailable([]classMemberPref{fable}, classReq{}, now)
	if u == nil || !u.timed() || *u.AvailableAt != unixSeconds(reset) {
		t.Fatalf("classUnavailable = %+v, want lane:fable unavailable on its check until the reset", u)
	}
	if m := u.message("class frontier", now); !strings.Contains(m, "a spent (Fable) until "+fmtAvail(reset, now)) {
		t.Fatalf("message = %q", m)
	}
}

// fableMemberRoute is class frontier's lane:fable member route over account a:
// the Fable scope, like the bare lane.
func fableMemberRoute() routePref {
	r := memberRoute("frontier/lane:fable", "frontier", "lane:fable", "fable", "anthropic", "a")
	r.Scope = "Fable"
	return r
}
