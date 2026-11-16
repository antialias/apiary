package main

// #71 provider account pools: a class member names a CAPABILITY
// (provider/model); the accounts behind it are a pool the brain ranks by
// utilization. These tests pin the proxy half: the pool pick (poolPick), the
// per-account bearer plumbing on class routes, the mid-request pool-account
// swap rung (bound account -> pool sibling -> only THEN cross members), and the
// request-time detour steering around a refused pool account.

import (
	"io"
	"strings"
	"testing"
	"time"
)

func f64(v float64) *float64 { return &v }

// poolPick is the pool account providerMemberDial dials for m ("" = none).
func poolPick(m classMemberPref) string {
	a, _, _ := providerMemberDial(m, "", false, false, nowFn())
	return a
}

func gptPoolMember(incumbent string, targets map[string]*float64) classMemberPref {
	return classMemberPref{ID: "gpt/gpt-5.6", Kind: "provider", Provider: "gpt",
		Model: "gpt-5.6", Incumbent: incumbent, Targets: targets, Route: "frontier/gpt/gpt-5.6"}
}

// poolWorld seeds the gpt member's route over accounts, each measured now and
// open at 40 % unless st says otherwise.
func poolWorld(t *testing.T, st map[string]acctStanding, accounts ...string) {
	t.Helper()
	now := nowFn()
	standings := map[string]acctStanding{}
	for _, a := range accounts {
		standings[a] = standingAt("codex", now, map[string]checkWindow{"seven_day": week(40, now.Add(48*time.Hour))}, "")
	}
	for a, s := range st {
		standings[a] = s
	}
	seedQuota(t, standings, map[string]routePref{
		"frontier/gpt/gpt-5.6": memberRoute("frontier/gpt/gpt-5.6", "frontier", "gpt/gpt-5.6", "", "gpt", accounts...),
	}, nil, nil)
}

// The pool pick (§2.2 row 16): the incumbent while its check admits it, else
// the route's other accounts as the swap ranks them; never an account whose
// check is `no`.
func TestPoolPick(t *testing.T) {
	t.Run("no pool view means incumbent-follow", func(t *testing.T) {
		poolWorld(t, nil, "gpt-a")
		if got := poolPick(gptPoolMember("", nil)); got != "" {
			t.Fatalf("nil targets -> %q, want \"\"", got)
		}
	})

	t.Run("incumbent wins even when warmer", func(t *testing.T) {
		poolWorld(t, nil, "gpt-a", "gpt-b")
		m := gptPoolMember("gpt-a", map[string]*float64{"gpt-a": f64(60), "gpt-b": f64(10)})
		if got := poolPick(m); got != "gpt-a" {
			t.Fatalf("got %q, want the incumbent — the proxy never re-ranks a healthy pick", got)
		}
	})

	t.Run("spent incumbent detours to the coolest sibling", func(t *testing.T) {
		poolWorld(t, nil, "gpt-a", "gpt-b", "gpt-c")
		m := gptPoolMember("gpt-a", map[string]*float64{
			"gpt-a": f64(10), "gpt-b": f64(60), "gpt-c": f64(20)})
		seedSpent("gpt-a", "seven_day", 429, "usage_limit_reached", time.Time{})
		if got := poolPick(m); got != "gpt-c" {
			t.Fatalf("got %q, want gpt-c (coolest admitted)", got)
		}
	})

	t.Run("an unmeasured account is never dialled", func(t *testing.T) {
		poolWorld(t, map[string]acctStanding{"gpt-x": {Provider: "codex"}}, "gpt-x", "gpt-y")
		m := gptPoolMember("gpt-x", map[string]*float64{"gpt-x": nil, "gpt-y": f64(90)})
		if got := poolPick(m); got != "gpt-y" {
			t.Fatalf("got %q, want gpt-y — never route to an unknown", got)
		}
	})

	t.Run("a throttled incumbent is deranked, not refused", func(t *testing.T) {
		poolWorld(t, nil, "gpt-a", "gpt-b")
		m := gptPoolMember("gpt-a", map[string]*float64{"gpt-a": f64(10), "gpt-b": f64(20)})
		recordThrottle("gpt-a", 429, "slow down", nil)
		if got := poolPick(m); got != "gpt-b" {
			t.Fatalf("got %q, want gpt-b ahead of the throttled incumbent", got)
		}
		recordThrottle("gpt-b", 429, "slow down", nil)
		if got := poolPick(m); got == "" {
			t.Fatal("every account throttled: still dialled — a throttle is not a no")
		}
	})

	t.Run("everything no dials nobody", func(t *testing.T) {
		poolWorld(t, nil, "gpt-a", "gpt-b")
		addProviders(t, &provider{name: "gpt"})
		m := gptPoolMember("gpt-a", map[string]*float64{"gpt-a": f64(10), "gpt-b": f64(20)})
		seedSpent("gpt-a", "seven_day", 429, "usage_limit_reached", time.Time{})
		seedSpent("gpt-b", "five_hour", 429, "usage_limit_reached", time.Time{})
		if got := poolPick(m); got != "" {
			t.Fatalf("got %q, want nobody: every check is no (the old cooldown rode the incumbent)", got)
		}
		if why := memberDialable(m, nowFn()); !strings.Contains(why, "no(spent") {
			t.Fatalf("memberDialable = %q, want the member refused for spent", why)
		}
	})

	t.Run("records are per account, whatever routes it backs", func(t *testing.T) {
		poolWorld(t, nil, "gpt-a", "gpt-b")
		seedSpent("gpt-a", "seven_day", 429, "usage_limit_reached", time.Time{})
		if _, ok := spentRecordOf("gpt-b", nowFn()); ok {
			t.Fatal("a record on gpt-a leaked onto gpt-b")
		}
		if _, ok := spentRecordOf("gpt-a", nowFn()); !ok {
			t.Fatal("the gpt-a record did not stick")
		}
	})
}

func TestMemberEff(t *testing.T) {
	m := gptPoolMember("gpt-a", nil)
	if got := memberEff(m, "gpt-a"); got != "gpt/gpt-5.6@gpt-a" {
		t.Fatalf("named pool account label = %q", got)
	}
	if got := memberEff(m, ""); got != "gpt/gpt-5.6" {
		t.Fatalf("no pool account label = %q", got)
	}
	kimi := classMemberPref{ID: "kimi/kimi-k3", Kind: "provider", Provider: "kimi"}
	if got := memberEff(kimi, "kimi"); got != "kimi/kimi-k3" {
		t.Fatalf("pool-of-one named after the provider must stay bare, got %q", got)
	}
}

// ---- e2e: the pool through the real handler stack ----

// poolFixtureMembers is the #71 class fixture: the gpt member carries the
// brain's pool view (incumbent + per-account targets), kimi is a pool of one
// named after its provider (the label-suppression case), mounted under /coding
// to keep the cross-member base-path re-join honest.
func poolFixtureMembers() []map[string]any {
	return []map[string]any{
		{"id": "gpt/gpt-5.6", "priority": 1, "kind": "provider", "provider": "gpt",
			"model": "gpt-5.6", "context_window": 400000,
			"effective": 10.0, "incumbent": "gpt-a",
			"targets": map[string]any{"gpt-a": 10.0, "gpt-b": 60.0}},
		{"id": "kimi/kimi-k3", "priority": 2, "kind": "provider", "provider": "kimi",
			"model": "kimi-k3", "context_window": 256000,
			"effective": 12.0, "incumbent": "kimi", "targets": map[string]any{"kimi": 12.0}},
	}
}

func newE2ECodexPool(t *testing.T) (*e2eHosted, func()) {
	t.Helper()
	h, restore := newE2EHosted(t, "/coding")
	h.codexPool.Store("gpt-a,gpt-b")
	h.brainMu.Lock()
	h.classMembers = poolFixtureMembers()
	h.brainMu.Unlock()
	withModelsFile(t, classModelsJSON)
	fetchPreferredSync()
	rebuildProxy(h.e2eEnv)
	return h, restore
}

// A class request onto a pooled provider member serves on the POOL INCUMBENT's
// own chain: the proxy asks the brain for that specific account and the hop
// carries that account's bearer + workspace id — not a shared singleton.
func TestE2EClassCodexPoolServesIncumbentAccount(t *testing.T) {
	h, restore := newE2ECodexPool(t)
	defer restore()

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-pool-1")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	hit := h.codexHitOrFail(t)
	if hit.auth != "Bearer codex-tok-gpt-a" || hit.acct != "uuid-gpt-a" {
		t.Fatalf("auth/acct = %q/%q, want the incumbent gpt-a's dispensed pair", hit.auth, hit.acct)
	}
	h.noAnthropicLeak(t)
}

// The intra-member rung: a 429 on the incumbent's chain swaps to a pool
// sibling on the SAME member mid-request — the member is NOT crossed away
// from, and only the refusing account is recorded (a throttle: it said busy). Subsequent requests steer to the
// sibling at actuation time (request-time detour) without another 429.
func TestE2EClassCodexPool429SwapsAccountMidRequest(t *testing.T) {
	h, restore := newE2ECodexPool(t)
	defer restore()
	h.codexDenyBearer.Store("codex-tok-gpt-a")

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-pool-swap")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "codex ok") {
		t.Fatalf("response did not come from the codex upstream: %s", b)
	}
	hit1 := h.codexHitOrFail(t)
	hit2 := h.codexHitOrFail(t)
	if hit1.auth != "Bearer codex-tok-gpt-a" || hit2.auth != "Bearer codex-tok-gpt-b" {
		t.Fatalf("hop bearers = %q then %q, want gpt-a then the gpt-b swap", hit1.auth, hit2.auth)
	}
	if hit2.acct != "uuid-gpt-b" {
		t.Fatalf("swap hop workspace id = %q, want gpt-b's own", hit2.acct)
	}
	if !throttledOn("gpt-a") {
		t.Fatal("gpt-a must be throttled after its busy 429")
	}
	if got := recordsOn("gpt-b"); len(got) != 0 {
		t.Fatalf("gpt-b served fine and must carry no record, got %v", got)
	}
	if why := classMemberDialable(t, "frontier", "gpt/gpt-5.6"); why != "" {
		t.Fatalf("the pool absorbed the 429 — the MEMBER must stay dialable, got %q", why)
	}
	alerts := classAlerts(t, h.e2eEnv)
	if len(alerts) != 1 {
		t.Fatalf("alerts = %d, want exactly the pool-swap alert", len(alerts))
	}
	if from, to := alerts[0].Labels["from_account"], alerts[0].Labels["to_account"]; !strings.Contains(from, "@gpt-a") || !strings.Contains(to, "@gpt-b") {
		t.Fatalf("alert %s -> %s, want the @account-labelled member ids", from, to)
	}
	noMoreAlerts(t, h.e2eEnv)

	// Request-time detour: the next request actuates straight onto gpt-b.
	resp2 := h.postSess(t, "claude-frontier", "CLIENT", "sess-pool-swap")
	defer resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("follow-up status %d", resp2.StatusCode)
	}
	hit3 := h.codexHitOrFail(t)
	if hit3.auth != "Bearer codex-tok-gpt-b" {
		t.Fatalf("follow-up bearer = %q, want gpt-b ahead of the throttled gpt-a", hit3.auth)
	}
	h.noCodexLeak(t)
	h.noAnthropicLeak(t)
}

// Only when the WHOLE pool refused does the ladder cross members: gpt-a 429,
// gpt-b 429 (both throttles), ONE cross to the included kimi (P2) — served
// under its own base path with its own key.
func TestE2EClassCodexPoolExhaustedCrossesMember(t *testing.T) {
	h, restore := newE2ECodexPool(t)
	defer restore()
	h.codexDenyBearer.Store("codex-tok-gpt-") // both pool accounts 429

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-pool-cross")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "hosted ok") {
		t.Fatalf("response did not come from kimi after the pool drained: %s", b)
	}
	hit1, hit2 := h.codexHitOrFail(t), h.codexHitOrFail(t)
	if hit1.auth != "Bearer codex-tok-gpt-a" || hit2.auth != "Bearer codex-tok-gpt-b" {
		t.Fatalf("codex hops = %q, %q — want both pool accounts tried before crossing", hit1.auth, hit2.auth)
	}
	kh := h.hostedHit(t)
	if kh.auth != "Bearer kimi-secret" || kh.path != "/coding/v1/messages" {
		t.Fatalf("kimi hop = %+v, want its own key under its base path", kh)
	}
	for _, a := range []string{"gpt-a", "gpt-b"} {
		if !throttledOn(a) {
			t.Fatalf("%s must be throttled", a)
		}
	}
	// A throttle deranks; it never closes: the member stays dialable, behind
	// the healthy kimi.
	if why := classMemberDialable(t, "frontier", "gpt/gpt-5.6"); why != "" {
		t.Fatalf("throttles must not refuse the member, got %q", why)
	}
	alerts := classAlertsN(t, h.e2eEnv, 2) // one flush or two — only delivery is the contract
	if len(alerts) != 2 {
		t.Fatalf("alerts = %d, want the pool swap + the member cross", len(alerts))
	}
	h.noAnthropicLeak(t)
}
