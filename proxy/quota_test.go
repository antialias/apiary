package main

// Tests for the explicit-quota axis: telling "spent until a wall-clock time"
// apart from "said 429 and might serve the next request", and refusing a class
// whose every member is in the first category.
//
// The failure in one line: both members of a class
// had refused EXPLICITLY 11 s earlier, routeClass rode the spent rung anyway as
// a last resort, and the request hung 120 s on a half-dead connection and
// reported itself as the word "timeout".

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The refusal bodies these tests are built on. The kimi shapes are verbatim
// from live traffic; the codex shape is reconstructed from a proxy log
// line (usage_limit_reached / plan_type / resets_at), so the exact nesting is NOT verified. That is the
// reason the parser is depth-agnostic and accepts epoch or RFC3339: the tests
// below pin every shape it will accept rather than one shape we guessed.
const (
	kimi403FiveHour = `{"type":"error","error":{"type":"permission_error","message":"You've reached your 5-hour usage limit. Your quota will refresh soon."}}`
	kimi403Cycle    = `{"type":"error","error":{"type":"permission_error","message":"You've reached your usage limit for this billing cycle. To continue now, purchase extra usage or upgrade your plan: https://www.kimi.com/code/#pricing"}}`
	codex429Nested  = `{"detail":{"type":"usage_limit_reached","plan_type":"pro","resets_at":1758283200}}`
	codex429Flat    = `{"type":"usage_limit_reached","plan_type":"pro","resets_at":1758283200}`
	ambiguous429    = `{"type":"error","error":{"type":"rate_limit_error","message":"provider busy, slow down"}}`
)

func TestQuotaResetAt(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	epoch := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC).Unix()

	for _, tc := range []struct {
		name string
		hdr  http.Header
		body string
		want time.Time
	}{{
		name: "Retry-After delta-seconds wins over the body",
		hdr:  http.Header{"Retry-After": []string{"3600"}},
		body: codex429Nested,
		want: now.Add(time.Hour),
	}, {
		name: "Retry-After as an HTTP-date",
		hdr:  http.Header{"Retry-After": []string{"Sat, 19 Sep 2026 12:00:00 GMT"}},
		want: time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC),
	}, {
		name: "epoch seconds nested under detail",
		body: `{"detail":{"type":"usage_limit_reached","resets_at":` + strconv.FormatInt(epoch, 10) + `}}`,
		want: time.Unix(epoch, 0),
	}, {
		name: "epoch milliseconds",
		body: `{"error":{"resets_at":` + strconv.FormatInt(epoch*1000, 10) + `}}`,
		want: time.UnixMilli(epoch * 1000),
	}, {
		name: "RFC3339 string",
		body: `{"error":{"reset_at":"2026-09-19T12:00:00Z"}}`,
		want: time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC),
	}, {
		name: "a small number is a RELATIVE wait, not a 1970 epoch",
		body: `{"error":{"resets_in_seconds":7200}}`,
		want: now.Add(2 * time.Hour),
	}, {
		name: "buried three levels deep still found",
		body: `{"a":{"b":{"c":{"retry_after_seconds":600}}}}`,
		want: now.Add(10 * time.Minute),
	}, {
		name: "nothing readable yields zero, never an error",
		body: kimi403FiveHour,
		want: time.Time{},
	}, {
		name: "unparseable body yields zero",
		body: `<html>nope</html>`,
		want: time.Time{},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			got := quotaResetAt(tc.hdr, []byte(tc.body), now)
			if !got.Equal(tc.want) {
				t.Errorf("quotaResetAt = %v, want %v", got, tc.want)
			}
		})
	}
}

// The clamp is what keeps an upstream's clock from becoming our problem.
func TestQuotaCooldownClamp(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	freezeQuotaLedger(t, now)
	for _, tc := range []struct {
		name   string
		resets time.Time
		want   time.Time
	}{
		{"no reset given falls to the floor", time.Time{}, now.Add(quotaCooldown)},
		{"a reset sooner than the floor is raised to it", now.Add(30 * time.Second), now.Add(quotaCooldown)},
		{"a reset inside the range is honoured", now.Add(3 * time.Hour), now.Add(3 * time.Hour)},
		// gpt's weekly bucket said 2.5 days. We will not strand an account that
		// long on an upstream's say-so.
		{"a reset past the cap is clamped", now.Add(60 * time.Hour), now.Add(quotaCooldownCap)},
		{"a reset in the PAST cannot shorten the floor", now.Add(-5 * time.Hour), now.Add(quotaCooldown)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seedSpent("gpt-a", "seven_day", 429, "usage_limit_reached", tc.resets)
			recMu.Lock()
			rec, ok := records[recKey{"gpt-a", "seven_day"}]
			recMu.Unlock()
			if !ok || rec.kind != recSpent {
				t.Fatal("the account is not recorded as spent")
			}
			if !rec.until.Equal(tc.want) {
				t.Errorf("until = %v, want %v", rec.until, tc.want)
			}
		})
	}
}

// A throttle arriving mid-outage must not shorten a deadline the upstream
// itself told us about: it is its own record, and it only deranks.
func TestThrottleDoesNotDowngradeASpentRecord(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	freezeQuotaLedger(t, now)
	seedSpent("gpt-a", "seven_day", 429, "usage_limit_reached", now.Add(3*time.Hour))
	recordThrottle("gpt-a", 429, "slow down", nil)
	recMu.Lock()
	rec := records[recKey{"gpt-a", "seven_day"}]
	recMu.Unlock()
	if !rec.until.Equal(now.Add(3 * time.Hour)) {
		t.Errorf("until = %v, want the quota deadline preserved", rec.until)
	}
}

// helpdeskWorld is the incident's class: gpt on account gpt-a, kimi on kimi,
// both measured fresh and open; lane:opus of class coding on account a.
func helpdeskWorld(t *testing.T, now time.Time) (gpt, kimi classMemberPref) {
	t.Helper()
	open := func(p string) acctStanding {
		return standingAt(p, now, map[string]checkWindow{"seven_day": week(57, now.Add(72*time.Hour))}, "")
	}
	seedQuota(t, map[string]acctStanding{"gpt-a": open("codex"), "kimi": open("kimi"), "a": open("anthropic")},
		map[string]routePref{
			"helpdesk/gpt/gpt-5.6-sol": memberRoute("helpdesk/gpt/gpt-5.6-sol", "helpdesk", "gpt/gpt-5.6-sol", "", "gpt", "gpt-a"),
			"helpdesk/kimi/kimi-k3":    memberRoute("helpdesk/kimi/kimi-k3", "helpdesk", "kimi/kimi-k3", "", "kimi", "kimi"),
			"coding/lane:opus":         memberRoute("coding/lane:opus", "coding", "lane:opus", "opus", "anthropic", "a"),
		}, nil, nil)
	addProviders(t, &provider{name: "gpt"}, &provider{name: "kimi"})
	gpt = classMemberPref{ID: "gpt/gpt-5.6-sol", Kind: "provider", Provider: "gpt", Model: "gpt-5.6-sol", Route: "helpdesk/gpt/gpt-5.6-sol"}
	kimi = classMemberPref{ID: "kimi/kimi-k3", Kind: "provider", Provider: "kimi", Model: "kimi-k3", Route: "helpdesk/kimi/kimi-k3"}
	return gpt, kimi
}

func TestClassUnavailableNeedsEveryMember(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	freezeQuotaLedger(t, now)
	gpt, kimi := helpdeskWorld(t, now)
	try := []classMemberPref{gpt, kimi}
	reset := func() {
		recMu.Lock()
		records = map[recKey]refusalRecord{}
		recMu.Unlock()
	}

	if u := classUnavailable(try, classReq{}, now); u != nil {
		t.Error("no records at all must not refuse")
	}

	seedSpent("gpt-a", "seven_day", 429, "usage_limit_reached", now.Add(2*time.Hour))
	if u := classUnavailable(try, classReq{}, now); u != nil {
		t.Error("ONE spent member must not refuse the class — the other can still serve")
	}

	recordThrottle("kimi", 429, "busy", nil)
	if u := classUnavailable(try, classReq{}, now); u != nil {
		t.Error("a merely throttled member still serves (its check is included) — must not refuse")
	}

	reset()
	seedSpent("gpt-a", "seven_day", 429, "usage_limit_reached", now.Add(2*time.Hour))
	seedSpent("kimi", "five_hour", 403, "You've reached your 5-hour usage limit", time.Time{})
	u := classUnavailable(try, classReq{}, now)
	if u == nil {
		t.Fatal("both members explicitly spent MUST refuse — this is the incident case")
	}
	// kimi's record names no reset: it ends at the floor, a record time, and
	// comes back first.
	if *u.AvailableAt != unixSeconds(now.Add(quotaCooldown)) || *u.AvailableBasis != "record" ||
		*u.RetryAfterS != int(quotaCooldown/time.Second) {
		t.Fatalf("time = %v %v %v, want kimi's record end (record), %d s", *u.AvailableAt, *u.AvailableBasis,
			*u.RetryAfterS, int(quotaCooldown/time.Second))
	}
	msg := u.message("class helpdesk", now)
	for _, want := range []string{
		"class helpdesk is unavailable", "helpdesk/gpt/gpt-5.6-sol: gpt-a spent", "429 usage_limit_reached",
		"helpdesk/kimi/kimi-k3: kimi spent", "403 You've reached your 5-hour usage limit",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("message missing %q:\n%s", want, msg)
		}
	}
	if strings.ContainsAny(msg, "\n\r") {
		t.Errorf("message must stay one line: %q", msg)
	}

	reset()
	if u := classUnavailable(nil, classReq{}, now); u != nil {
		t.Error("an empty try-list is UNROUTABLE, not unavailable")
	}
}

// ---- the router-wide clause -------------------------------------------------
//
// The clause answers "would my own failover help?" and nothing else. It is a
// statement about what THIS ROUTER would do with the next request, not about any
// upstream's budget — the router cannot see a wallet, and the incident proved
// the brain's number is not a substitute: kimi read 57 against a hard line of 95
// and refused anyway, because the two were measuring different windows.

// withRouterView installs a brain snapshot for one test and restores it after.
func withRouterView(t *testing.T, ok bool, fetched time.Time, classes map[string]classPref) {
	t.Helper()
	pref.mu.Lock()
	oldOK, oldFetched, oldClasses := pref.ok, pref.heardAt, pref.classes
	pref.ok, pref.heardAt, pref.classes = ok, fetched, classes
	pref.mu.Unlock()
	t.Cleanup(func() {
		pref.mu.Lock()
		pref.ok, pref.heardAt, pref.classes = oldOK, oldFetched, oldClasses
		pref.mu.Unlock()
	})
}

// liveMember is a member nothing has any reason to pass over: an anthropic
// member on route <id> over account a, which helpdeskWorld measures open.
func liveMember(id string) classMemberPref {
	return classMemberPref{ID: id, Kind: "anthropic", Lane: "default", Model: "m", Route: "coding/lane:opus"}
}

// freezeQuotaLedger pins the clock and empties the record store for one test.
func freezeQuotaLedger(t *testing.T, now time.Time) {
	t.Helper()
	old := nowFn
	nowFn = func() time.Time { return now }
	restore := resetQuotaState()
	t.Cleanup(func() { nowFn = old; restore() })
}

// Silence is the honest answer whenever the snapshot cannot be spoken for. Each
// of these has every remembered member spent — the tempting case to talk about.
func TestRouterDryClauseStaysSilentWhenWeCannotKnow(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	spent := func(t *testing.T) map[string]classPref {
		_, kimi := helpdeskWorld(t, now)
		seedSpent("kimi", "five_hour", 403, "5-hour usage limit", now.Add(2*time.Hour))
		return map[string]classPref{"helpdesk": {Members: []classMemberPref{kimi}}}
	}

	t.Run("brain unreachable", func(t *testing.T) {
		freezeQuotaLedger(t, now)
		// pref.ok false means we are serving a last-good decision on cached
		// tokens. What we remember about other classes may be arbitrarily out of
		// date, so we do not get to describe the fleet.
		withRouterView(t, false, now, spent(t))
		if got := routerDryClause(); got != "" {
			t.Errorf("clause = %q, want silence while the brain is unreachable", got)
		}
	})

	t.Run("snapshot stale", func(t *testing.T) {
		freezeQuotaLedger(t, now)
		// ok is still true, which is exactly the case ok cannot catch: a wedged
		// stream goroutine sitting on an ancient snapshot that still claims good.
		withRouterView(t, true, now.Add(-2*brainSilence), spent(t))
		if got := routerDryClause(); got != "" {
			t.Errorf("clause = %q, want silence on a stale snapshot", got)
		}
	})

	t.Run("no classes at all", func(t *testing.T) {
		freezeQuotaLedger(t, now)
		// An old brain that omits classes says NOTHING about them (#93). Reading
		// that absence as "the router is dry" would invent a fact from a gap.
		withRouterView(t, true, now, nil)
		if got := routerDryClause(); got != "" {
			t.Errorf("clause = %q, want silence when the class view is absent", got)
		}
	})
}

// One routable member anywhere on the router is enough to withhold the clause —
// the consumer's failover has somewhere to land and should go try it.
func TestRouterDryClauseSilentWhileAnythingIsRoutable(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	freezeQuotaLedger(t, now)
	gpt, kimi := helpdeskWorld(t, now)
	seedSpent("kimi", "five_hour", 403, "5-hour usage limit", now.Add(2*time.Hour))
	seedSpent("gpt-a", "seven_day", 429, "usage_limit_reached", now.Add(52*time.Hour))
	withRouterView(t, true, now, map[string]classPref{
		"helpdesk": {Members: []classMemberPref{gpt, kimi}},
		"coding":   {Members: []classMemberPref{liveMember("lane:opus")}},
	})
	if got := routerDryClause(); got != "" {
		t.Errorf("clause = %q, but lane:opus would have been routed to right now", got)
	}

	// A record is on an account, not a member id: kimi's refusal says nothing
	// about account a, which coding's lane:opus dials.
	seedSpent("kimi", "seven_day", 403, "weekly limit", now.Add(time.Hour))
	if got := routerDryClause(); got != "" {
		t.Errorf("clause = %q; a record on kimi must not silence account a", got)
	}
}

// The incident shape, plus a member the brain has never measured: its check
// is `no (unknown)` — a DETERMINATE "will not be routed to" (never route
// blind), not a guess about its budget. That is exactly why the clause talks
// about routability.
func TestRouterDryClauseFiresWhenNothingWouldBeRouted(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	freezeQuotaLedger(t, now)
	gpt, kimi := helpdeskWorld(t, now)
	seedSpent("kimi", "five_hour", 403, "5-hour usage limit", now.Add(2*time.Hour))
	seedSpent("gpt-a", "seven_day", 429, "usage_limit_reached", now.Add(52*time.Hour))
	pref.mu.Lock()
	held := pref.accounts["a"]
	pref.accounts["a"] = acctStanding{Provider: "anthropic"} // never measured
	pref.mu.Unlock()
	blind := liveMember("lane:opus")
	withRouterView(t, true, now, map[string]classPref{
		"helpdesk": {Members: []classMemberPref{gpt, kimi}},
		"coding":   {Members: []classMemberPref{blind}},
	})

	dry, known := routerDry()
	if !known {
		t.Fatal("a fresh, reachable class view is exactly when we DO get an opinion")
	}
	if !dry {
		t.Fatal("every member is either spent or unmeasured; the failover has nowhere to land")
	}
	clause := routerDryClause()
	if !strings.Contains(clause, "no other model on this router is routable") {
		t.Fatalf("clause = %q", clause)
	}
	// It rides the one channel the consumer already reads, and stays one line.
	if strings.ContainsAny(clause, "\n\r") {
		t.Errorf("clause must stay on one line: %q", clause)
	}
	// It names no class and no member: a consumer learns whether to retry
	// elsewhere, not what this router is made of.
	for _, leak := range []string{"helpdesk", "coding", "lane:opus", "kimi", "gpt"} {
		if strings.Contains(clause, leak) {
			t.Errorf("clause leaks router topology (%q): %s", leak, clause)
		}
	}

	// And the account being measured is enough to take the sentence back.
	pref.mu.Lock()
	pref.accounts["a"] = held
	pref.mu.Unlock()
	if got := routerDryClause(); got != "" {
		t.Errorf("clause = %q after the member became routable again", got)
	}
}

// The drill must render the real payload and change nothing — a diagnostic for a
// refusal path that could itself cause a refusal would be worse than no
// diagnostic at all.
func TestQuotaDrillIsReadOnlyAndRendersTheRealPayload(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	freezeQuotaLedger(t, now)
	gpt, kimi := helpdeskWorld(t, now)
	seedSpent("kimi", "five_hour", 403, "5-hour usage limit", now.Add(2*time.Hour))
	seedSpent("gpt-a", "seven_day", 429, "usage_limit_reached", now.Add(52*time.Hour))
	withRouterView(t, true, now, map[string]classPref{"helpdesk": {Members: []classMemberPref{gpt, kimi}}})

	recMu.Lock()
	ledgerBefore := len(records)
	recMu.Unlock()
	rec := httptest.NewRecorder()
	quotaDrillHandler(rec, httptest.NewRequest("GET", "/_switch/quota-drill?class=helpdesk", nil))
	if rec.Code != 200 {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var got drillReply
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("drill body is not JSON (%v): %s", err, rec.Body.String())
	}
	if !got.ClassUnavailable {
		t.Errorf("both members are genuinely spent; drill must say the class is unavailable, got %+v", got)
	}
	if !got.RouterDryKnown || !got.RouterDry || got.Clause == "" {
		t.Errorf("drill did not surface the live router verdict: %+v", got)
	}
	// The rendering is the real thing, on each wire: same status, same
	// message, same clause.
	for wire, status := range map[string]int{"claude-code": 429, "anthropic": 529, "openai": 429} {
		ws := got.WouldServe[wire]
		if ws.Status != status {
			t.Errorf("would_serve[%s].status = %d, want %d", wire, ws.Status, status)
		}
		for _, want := range []string{"helpdesk", "kimi/kimi-k3", "gpt/gpt-5.6-sol", got.Clause} {
			if !strings.Contains(ws.Message, want) {
				t.Errorf("%s message %q missing %q", wire, ws.Message, want)
			}
		}
	}
	// And nothing moved.
	recMu.Lock()
	after := len(records)
	recMu.Unlock()
	if after != ledgerBefore {
		t.Errorf("drill wrote to the record store: %d -> %d", ledgerBefore, after)
	}
}

// drillReply is the drill's JSON.
type drillReply struct {
	ClassUnavailable bool   `json:"class_unavailable"`
	RouterDry        bool   `json:"router_dry"`
	RouterDryKnown   bool   `json:"router_dry_known"`
	Clause           string `json:"clause"`
	WouldServe       map[string]struct {
		Status  int    `json:"status"`
		Message string `json:"message"`
	} `json:"would_serve"`
}

// A class that is healthy right now still renders, marked as such — otherwise
// the endpoint is only usable during the outage it exists to explain.
func TestQuotaDrillRendersAServingClass(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	freezeQuotaLedger(t, now)
	_, kimi := helpdeskWorld(t, now)
	withRouterView(t, true, now, map[string]classPref{
		"helpdesk": {Members: []classMemberPref{kimi}},
	})

	rec := httptest.NewRecorder()
	quotaDrillHandler(rec, httptest.NewRequest("GET", "/_switch/quota-drill?class=helpdesk", nil))
	var got drillReply
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	if got.ClassUnavailable {
		t.Errorf("nothing has refused; the drill must say the class serves, got %+v", got)
	}
	if m := got.WouldServe["claude-code"].Message; !strings.Contains(m, "kimi included") {
		t.Errorf("the rendering must show kimi's real check: %q", m)
	}
	// The clause is NEVER fabricated: kimi is routable, so the live verdict is
	// "not dry" and the sentence must be absent.
	if got.RouterDry || got.Clause != "" {
		t.Errorf("clause was synthesized: %+v", got)
	}
	if strings.Contains(got.WouldServe["anthropic"].Message, "no other model on this router") {
		t.Errorf("rendered message carries a clause the live router does not: %q", got.WouldServe["anthropic"].Message)
	}
}

// A class none of whose routes has an account has no unavailable reply, only
// the typed 503 (configuration, §2.8 item 6): the drill says so with a null
// would_serve rather than rendering a money reply with nothing to fund.
func TestQuotaDrillClassWithNoAccount(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	freezeQuotaLedger(t, now)
	_, kimi := helpdeskWorld(t, now)
	kimi.ID, kimi.Route = "kimi/kimi-k9", "helpdesk/kimi/kimi-k9" // a route the brain does not serve
	withRouterView(t, true, now, map[string]classPref{"helpdesk": {Members: []classMemberPref{kimi}}})

	rec := httptest.NewRecorder()
	quotaDrillHandler(rec, httptest.NewRequest("GET", "/_switch/quota-drill?class=helpdesk", nil))
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || rec.Code != 200 {
		t.Fatalf("drill = %d %s", rec.Code, rec.Body.String())
	}
	if got["class_unavailable"] != false || got["would_serve"] != nil {
		t.Fatalf("drill = %s, want class_unavailable false and would_serve null", rec.Body.String())
	}
}

// An unknown class is a 404 that says what DOES exist — the drill is a debugging
// tool and a bare 404 during an outage is a second puzzle.
func TestQuotaDrillRejectsUnknownClass(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	freezeQuotaLedger(t, now)
	_, kimi := helpdeskWorld(t, now)
	withRouterView(t, true, now, map[string]classPref{
		"helpdesk": {Members: []classMemberPref{kimi}},
	})

	rec := httptest.NewRecorder()
	quotaDrillHandler(rec, httptest.NewRequest("GET", "/_switch/quota-drill?class=nope", nil))
	if rec.Code != 404 {
		t.Errorf("status = %d, want 404", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "helpdesk") {
		t.Errorf("404 should name the classes that exist: %s", rec.Body.String())
	}

	bare := httptest.NewRecorder()
	quotaDrillHandler(bare, httptest.NewRequest("GET", "/_switch/quota-drill", nil))
	if bare.Code != 400 {
		t.Errorf("missing class = %d, want 400", bare.Code)
	}
}
