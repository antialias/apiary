package main

// e2e for the one unavailable reply (quota-model 50 §2.8, C3): a class whose
// only member's check is `no` answers from that check, rendered by the
// client's wire — Claude Code, another Anthropic-wire client, the OpenAI wire —
// retryable when a time exists and the money reply when none does; and the
// brain's own 409 body rendered the same way. Nobody is dialled.

import (
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// gptOnlyClass is class frontier with the gpt member alone, its account's
// standing as given: allowance closed (resets nil = no reading says when),
// credits open or closed.
func gptOnlyClass(t *testing.T, resets *float64, creditsOpen bool) (*e2eHosted, float64) {
	t.Helper()
	h, restore := newE2EClass(t)
	t.Cleanup(restore)
	now := float64(time.Now().Unix())
	h.brainMu.Lock()
	h.classMembers = []map[string]any{h.classMembers[1]}
	h.classMembers[0]["usable"], h.classMembers[0]["effective"] = true, 100.0
	if h.standings == nil {
		h.standings = map[string]any{}
	}
	var r any
	if resets != nil {
		r = *resets
	}
	h.standings["gpt"] = map[string]any{"provider": "gpt", "as_of": now - 60, "next_poll_at": now + 300,
		"allowance": map[string]any{"open": false, "windows": map[string]any{
			"seven_day": map[string]any{"used_pct": 100.0, "resets_at": r, "period": "7d", "scope": nil,
				"as_of": now - 60, "reset_basis": "early_possible"}}},
		"money": map[string]any{"credits": map[string]any{"open": creditsOpen, "balance": 0.0, "unit": "codex_credit"}}}
	h.brainMu.Unlock()
	h.bindTo.Store("gpt/gpt-5.6")
	return h, now
}

// wireReply is one wire's reply, read whole.
type wireReply struct {
	status int
	h      http.Header
	body   []byte
	env    struct {
		Type  string `json:"type"`
		Error struct {
			Type, Message, Code string
		} `json:"error"`
		Unavailable unavailable `json:"unavailable"`
	}
}

func readReply(t *testing.T, resp *http.Response) wireReply {
	t.Helper()
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	r := wireReply{status: resp.StatusCode, h: resp.Header, body: b}
	if err := json.Unmarshal(b, &r.env); err != nil {
		t.Fatalf("reply is not JSON: %d %s", resp.StatusCode, b)
	}
	return r
}

// threeWires sends one request on each wire and returns the replies.
func threeWires(t *testing.T, h *e2eHosted, sess string) (cc, anth, oai wireReply) {
	t.Helper()
	cc = readReply(t, postCC(t, h.e2eEnv, "claude-frontier", sess+"-cc"))
	anth = readReply(t, h.postSess(t, "claude-frontier", "CLIENT", sess+"-anth"))
	oai = readReply(t, wirePost(t, h.e2eEnv, `{"model":"frontier","messages":[{"role":"user","content":"hi"}],"max_tokens":16}`, sess+"-oai"))
	return cc, anth, oai
}

func wantRetry(t *testing.T, wire string, r wireReply, secs float64) {
	t.Helper()
	ra, err := strconv.Atoi(r.h.Get("Retry-After"))
	if err != nil || float64(ra) < secs-10 || float64(ra) > secs+10 {
		t.Errorf("%s Retry-After = %q, want ~%.0f", wire, r.h.Get("Retry-After"), secs)
	}
	if r.env.Unavailable.RetryAfterS == nil || *r.env.Unavailable.RetryAfterS != ra {
		t.Errorf("%s body retry_after_s = %v, want the header's %d", wire, r.env.Unavailable.RetryAfterS, ra)
	}
	at, err := time.Parse(time.RFC3339, r.h.Get("X-Switch-Available-At"))
	if err != nil || r.env.Unavailable.AvailableAt == nil || at.Unix() != int64(*r.env.Unavailable.AvailableAt) {
		t.Errorf("%s X-Switch-Available-At = %q, want the body's available_at %v", wire,
			r.h.Get("X-Switch-Available-At"), r.env.Unavailable.AvailableAt)
	}
}

// The allowance is closed until its reset, credits are closed, and the route
// may pay them: no(spent) until the reset. With topup off that is the time;
// every wire gets its retryable reply, and the options say to add money.
func TestE2EUnavailableRetryableByWire(t *testing.T) {
	reset := float64(time.Now().Unix()) + 3*86400
	h, _ := gptOnlyClass(t, &reset, false)
	h.payFrom(gptRoute, "gpt/credits")
	h.brainMu.Lock()
	h.pots = map[string]any{"gpt/credits": map[string]any{"topup": "off"}}
	h.brainMu.Unlock()
	fetchPreferredSync()

	cc, anth, oai := threeWires(t, h, "sess-retry")
	secs := reset - float64(time.Now().Unix())

	if cc.status != 429 || cc.env.Error.Type != "rate_limit_error" ||
		cc.h.Get("Anthropic-Ratelimit-Unified-Status") != "rejected" ||
		cc.h.Get("Anthropic-Ratelimit-Unified-Reset") != strconv.FormatInt(int64(reset), 10) ||
		cc.h.Get("Anthropic-Ratelimit-Unified-Representative-Claim") != "seven_day" {
		t.Errorf("claude code = %d %s %v", cc.status, cc.body, cc.h)
	}
	wantRetry(t, "claude code", cc, secs)
	if anth.status != 529 || anth.env.Error.Type != "overloaded_error" ||
		anth.h.Get("Anthropic-Ratelimit-Unified-Reset") != "" {
		t.Errorf("other anthropic = %d %s %v", anth.status, anth.body, anth.h)
	}
	wantRetry(t, "other anthropic", anth, secs)
	if oai.status != 429 || oai.env.Error.Code != "rate_limit_exceeded" || oai.env.Error.Type != "requests" ||
		oai.h.Get("x-should-retry") != "" || oai.h.Get("X-Switch-Wire") != "openai" {
		t.Errorf("openai = %d %s %v", oai.status, oai.body, oai.h)
	}
	wantRetry(t, "openai", oai, secs)

	u := cc.env.Unavailable
	if *u.AvailableBasis != "early_possible" || len(u.Members) != 1 || u.Members[0].Route != gptRoute ||
		strOrEmpty(u.Members[0].Accounts[0].Why) != "spent" {
		t.Errorf("body = %s", cc.body)
	}
	if len(u.Options) != 1 || u.Options[0].Kind != "fund" || u.Options[0].Action != "add money to gpt/credits" {
		t.Errorf("options = %+v, want the one fund option", u.Options)
	}
	if !strings.Contains(cc.env.Error.Message, "Options: add money to gpt/credits.") {
		t.Errorf("message = %q", cc.env.Error.Message)
	}
	h.noCodexLeak(t)
	h.noAnthropicLeak(t)
}

// A route budget closes its route on /preferred in the brain's own shape: a
// route's id is its key in routes{}, never a field, so this decodes through
// fetchPreferred rather than a routePref built by hand. 60 spent of the
// route's 50: no(budget), and Claude Code gets its 429 with nothing dialled.
func TestE2ERouteBudgetClosesTheRouteThroughFetchPreferred(t *testing.T) {
	h, _ := gptOnlyClass(t, nil, true)
	h.payFrom(gptRoute, "gpt/credits")
	h.brainMu.Lock()
	h.pots = map[string]any{"gpt/credits": map[string]any{"topup": "off", "route_budgets": map[string]any{gptRoute: 50.0}}}
	h.ledger = e2eLedger(time.Now(), 3, map[string]any{"gpt/credits": map[string]any{
		"unit": "codex_credit", "period_spend": 60.0, "routes": map[string]any{gptRoute: 60.0}}})
	h.brainMu.Unlock()
	fetchPreferredSync()
	lan.pref.mu.RLock()
	id := lan.pref.routes[gptRoute].ID
	lan.pref.mu.RUnlock()
	if id != gptRoute {
		t.Fatalf("decoded route id %q, want %q", id, gptRoute)
	}
	if c, _ := lan.routeCheck(gptRoute, "gpt", "", time.Now()); !c.no() || c.Reason != "budget" {
		t.Fatalf("route budget 50 with 60 spent: %s, want no(budget)", c)
	}
	cc, _, _ := threeWires(t, h, "sess-route-budget")
	if cc.status != 429 {
		t.Fatalf("claude code status %d, want 429: %s", cc.status, cc.body)
	}
	if u := cc.env.Unavailable; len(u.Options) != 1 || u.Options[0].Kind != "budget" ||
		!strings.Contains(u.Options[0].Action, "route budget") {
		t.Errorf("options = %+v, want the one route budget option", u.Options)
	}
	h.noCodexLeak(t)
}

// topup on: the refill shows at the next poll, which comes before the reset,
// so the reply's time is the next poll.
func TestE2EUnavailableTopupOnRetriesAtTheNextPoll(t *testing.T) {
	reset := float64(time.Now().Unix()) + 3*86400
	h, now := gptOnlyClass(t, &reset, false)
	h.payFrom(gptRoute, "gpt/credits")
	h.brainMu.Lock()
	h.pots = map[string]any{"gpt/credits": map[string]any{"topup": "on"}}
	h.brainMu.Unlock()
	fetchPreferredSync()

	cc, _, oai := threeWires(t, h, "sess-topup")
	for wire, r := range map[string]wireReply{"claude code": cc, "openai": oai} {
		u := r.env.Unavailable
		if u.AvailableAt == nil || *u.AvailableAt != now+300 || *u.AvailableBasis != "next_poll" {
			t.Errorf("%s time = %v %v, want the next poll (next_poll)", wire, u.AvailableAt, u.AvailableBasis)
		}
		wantRetry(t, wire, r, now+300-float64(time.Now().Unix()))
	}
	h.noCodexLeak(t)
}

// No reading says when the allowance reopens and the route may not pay the
// open credits: no(until paid), which gives no time. Every wire gets the money
// reply: 402 billing_error on the Anthropic wire, 429 insufficient_quota with
// x-should-retry: false on the OpenAI wire; no Retry-After anywhere.
func TestE2EUnavailableNonRetryableByWire(t *testing.T) {
	h, _ := gptOnlyClass(t, nil, true)
	h.payFrom(gptRoute)
	fetchPreferredSync()

	cc, anth, oai := threeWires(t, h, "sess-money")
	for wire, r := range map[string]wireReply{"claude code": cc, "other anthropic": anth} {
		if r.status != http.StatusPaymentRequired || r.env.Error.Type != "billing_error" {
			t.Errorf("%s = %d %s, want 402 billing_error", wire, r.status, r.body)
		}
	}
	if oai.status != 429 || oai.env.Error.Type != "insufficient_quota" || oai.env.Error.Code != "insufficient_quota" ||
		oai.h.Get("x-should-retry") != "false" {
		t.Errorf("openai = %d %s %v, want 429 insufficient_quota, x-should-retry false", oai.status, oai.body, oai.h)
	}
	for wire, r := range map[string]wireReply{"claude code": cc, "other anthropic": anth, "openai": oai} {
		if r.h.Get("Retry-After") != "" || r.h.Get("X-Switch-Available-At") != "unknown" ||
			r.h.Get("X-Switch-Quota") != "exhausted" {
			t.Errorf("%s headers = %v, want no Retry-After and an unknown time", wire, r.h)
		}
		for k := range r.h {
			if strings.HasPrefix(strings.ToLower(k), "anthropic-ratelimit-unified-") {
				t.Errorf("%s minted %s with no time to name", wire, k)
			}
		}
		u := r.env.Unavailable
		if u.AvailableAt != nil || u.RetryAfterS != nil || len(u.Members) != 1 ||
			!u.Members[0].Accounts[0].Until.Paid || strOrEmpty(u.Members[0].Accounts[0].Why) != "pay" {
			t.Errorf("%s body = %s", wire, r.body)
		}
		if len(u.Options) != 1 || u.Options[0].Kind != "permit" {
			t.Errorf("%s options = %+v, want permit gpt/credits", wire, u.Options)
		}
	}
	if !strings.Contains(cc.env.Error.Message, "No time is known") {
		t.Errorf("message = %q", cc.env.Error.Message)
	}
	h.noCodexLeak(t)
}

// The brain's 409 body is rendered by wire like the proxy's own: here the
// golden "until paid" case, which is the money reply on every wire.
func TestE2EBrain409RenderedByWire(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	d := loadGolden(t)
	var body map[string]any
	for _, c := range d.Unavailable {
		if strings.HasPrefix(c.Name, "codex closed with no reset known") {
			_ = json.Unmarshal(c.Want, &body)
		}
	}
	if body == nil {
		t.Fatal("golden case not found")
	}
	body["error"], body["class"] = "class_unavailable", "frontier"
	b, _ := json.Marshal(body)
	h.bind409.Store(string(b))

	cc, anth, oai := threeWires(t, h, "sess-409")
	if cc.status != 402 || anth.status != 402 || oai.status != 429 || oai.env.Error.Code != "insufficient_quota" {
		t.Fatalf("statuses %d %d %d (%s), want 402 402 429 insufficient_quota", cc.status, anth.status, oai.status, oai.body)
	}
	for _, r := range []wireReply{cc, anth, oai} {
		if r.env.Unavailable.Members[0].Route != "c/gpt/gpt-6.1-sol" || r.h.Get("X-Switch-Class") != "frontier" {
			t.Errorf("the brain's body does not ride the reply: %s", r.body)
		}
	}
	// The statusline (§2.11): a session the brain refused before binding it
	// reads unavailable, with no time when only money serves.
	if f := statusFunding(t, h.proxy.URL, "frontier", "sess-409-cc"); f["unavailable"] != true || f["until"] != nil {
		t.Errorf("class_funding = %v, want unavailable until null", f)
	}
	h.noAnthropicLeak(t)
	h.noCodexLeak(t)
}

// The statusline's class row (§2.11) is how the session's last request
// ended: a served request replaces an unavailable one, and count_tokens, which
// is free and asks entitlement only (row 20), leaves it as it was.
func TestE2EClassFundingIsTheLastRequest(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	oldFresh := bindFresh
	bindFresh = 0
	defer func() { bindFresh = oldFresh }()
	post := func(path string) {
		t.Helper()
		req, _ := http.NewRequest("POST", h.proxy.URL+path,
			strings.NewReader(`{"model":"claude-frontier","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`))
		req.Header.Set("Authorization", "Bearer CLIENT")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-claude-code-session-id", "sess-funding")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	post("/v1/messages")
	h.hit(t)
	served := statusFunding(t, h.proxy.URL, "frontier", "sess-funding")
	if served["tier"] == nil {
		t.Fatalf("class_funding = %v, want the tier it was served on", served)
	}

	d := loadGolden(t)
	var body map[string]any
	_ = json.Unmarshal(d.Unavailable[0].Want, &body)
	body["error"], body["class"] = "class_unavailable", "frontier"
	b, _ := json.Marshal(body)
	h.bind409.Store(string(b))
	post("/v1/messages")
	unavail := statusFunding(t, h.proxy.URL, "frontier", "sess-funding")
	if unavail["unavailable"] != true {
		t.Fatalf("class_funding = %v, want unavailable", unavail)
	}
	h.bind409.Store("")

	post("/v1/messages/count_tokens")
	h.hit(t)
	if f := statusFunding(t, h.proxy.URL, "frontier", "sess-funding"); !reflect.DeepEqual(f, unavail) {
		t.Fatalf("count_tokens wrote class_funding = %v, want it left at %v", f, unavail)
	}
	post("/v1/messages")
	h.hit(t)
	if f := statusFunding(t, h.proxy.URL, "frontier", "sess-funding"); !reflect.DeepEqual(f, served) {
		t.Fatalf("the next served request left class_funding = %v, want %v", f, served)
	}
}

// A session's funding lives as long as its binding would at the brain
// (SESSION_IDLE): /_switch/status does not grow with every session ever seen.
func TestSessionFundingDropsSessionsIdlePastTheBindingLifetime(t *testing.T) {
	oldF, oldNow := lan.sessionFundings, nowFn
	t.Cleanup(func() { lan.sessionFundings, nowFn = oldF, oldNow })
	lan.sessionFundings = map[string]sessionFunding{}
	t0 := time.Now()
	nowFn = func() time.Time { return t0 }
	lan.noteSessionFunding(&classRoute{tenant: lan, classReq: classReq{inPath: "/v1/messages"}, class: "frontier", session: "old"}, sessionFunding{Tier: "included"})
	nowFn = func() time.Time { return t0.Add(fundingIdle) }
	lan.noteSessionFunding(&classRoute{tenant: lan, classReq: classReq{inPath: "/v1/messages"}, class: "frontier", session: "mid"}, sessionFunding{Tier: "included"})
	if len(lan.sessionFundings) != 2 {
		t.Fatalf("at exactly the lifetime: %v, want both kept", lan.sessionFundings)
	}
	nowFn = func() time.Time { return t0.Add(fundingIdle + time.Second) }
	lan.noteSessionFunding(&classRoute{tenant: lan, classReq: classReq{inPath: "/v1/messages"}, class: "frontier", session: "new"}, sessionFunding{Unavailable: true})
	if _, ok := lan.sessionFundings[bindKey("frontier", "old")]; ok || len(lan.sessionFundings) != 2 {
		t.Fatalf("past the lifetime: %v, want old dropped, mid and new kept", lan.sessionFundings)
	}
}

// Configuration is not unavailability: a class none of whose members this
// proxy can route at all (here: on the OpenAI wire, every member is
// anthropic) keeps the typed 503.
func TestE2EUnroutableClassStaysTyped503(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	h.brainMu.Lock()
	h.classMembers = []map[string]any{h.classMembers[0]}
	h.brainMu.Unlock()
	fetchPreferredSync()
	resp := wirePost(t, h.e2eEnv, `{"model":"frontier","messages":[{"role":"user","content":"hi"}],"max_tokens":16}`, "sess-cfg")
	b := wireBody(t, resp)
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("X-Switch-Quota") != "" {
		t.Fatalf("status %d X-Switch-Quota %q, want the typed 503: %s", resp.StatusCode, resp.Header.Get("X-Switch-Quota"), b)
	}
}

// C6's budget `no` (§1.9) in the same reply: the allowance is closed with no
// reading of its reset, credits are open and the route may pay them, but the
// owner's budget on gpt/credits is spent this period. Retry-After is the
// budget period's end, exact; the option is raising the budget; and the
// session's statusline reads unavailable until that end.
func TestE2EUnavailableBudgetReachedRetriesAtThePeriodEnd(t *testing.T) {
	h, now := gptOnlyClass(t, nil, true)
	h.payFrom(gptRoute, "gpt/credits")
	h.brainMu.Lock()
	h.pots = map[string]any{"gpt/credits": map[string]any{"topup": "off", "budget": 100.0}}
	h.ledger = e2eLedger(time.Now(), 0, map[string]any{"gpt/credits": map[string]any{
		"unit": "codex_credit", "period_spend": 100.0, "routes": map[string]any{gptRoute: 100.0}}})
	h.brainMu.Unlock()
	fetchPreferredSync()

	cc, anth, oai := threeWires(t, h, "sess-budget")
	_, end := budgetPeriod(now)
	secs := end - float64(time.Now().Unix())
	if cc.status != 429 || anth.status != 529 || oai.status != 429 || oai.env.Error.Code != "rate_limit_exceeded" {
		t.Fatalf("statuses %d %d %d (%s), want the retryable replies", cc.status, anth.status, oai.status, oai.body)
	}
	wantRetry(t, "claude code", cc, secs)
	wantRetry(t, "other anthropic", anth, secs)
	wantRetry(t, "openai", oai, secs)

	u := cc.env.Unavailable
	if *u.AvailableAt != end || *u.AvailableBasis != "exact" || strOrEmpty(u.Members[0].Accounts[0].Why) != "budget" {
		t.Errorf("body = %s, want available_at the period end %v, exact, why budget", cc.body, end)
	}
	if len(u.Options) != 1 || u.Options[0].Kind != "budget" || u.Options[0].Action != "raise gpt/credits's budget" {
		t.Errorf("options = %+v, want the one budget option", u.Options)
	}
	if !strings.Contains(cc.env.Error.Message, "Options: raise gpt/credits's budget.") {
		t.Errorf("message = %q", cc.env.Error.Message)
	}
	want := time.Unix(int64(end), 0).UTC().Format(time.RFC3339)
	if f := statusFunding(t, h.proxy.URL, "frontier", "sess-budget-cc"); f["unavailable"] != true || f["until"] != want {
		t.Errorf("class_funding = %v, want unavailable until %s", f, want)
	}
	h.noCodexLeak(t)
	h.noAnthropicLeak(t)
}
