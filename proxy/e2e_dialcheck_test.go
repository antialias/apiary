package main

// e2e tests for the dial check at the class dial sites the rest of the suite
// does not reach on their own (quota-model 50 §2.2): the bound member's dial
// and its pay (row 6), the walk's re-check of a rung ranked before this
// request's own refusal (row 7), the throttle that stops the walk before money
// (§2.3), the first-event resend (row 18) and count_tokens (row 20). Each
// drives the REAL handler stack against the fake brain and fake upstreams.

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// gptRoute is the fixture gpt member's route id (withRoute).
const gptRoute = "frontier/gpt/gpt-5.6"

// spentWithCredits serves account as a measured standing whose allowance is
// spent (seven_day at 100) and whose credits are open: its check is paid where
// the route may pay that pot, no(pay) where it may not.
func (env *e2eEnv) spentWithCredits(account, provider string) {
	now := float64(time.Now().UnixNano()) / 1e9
	env.brainMu.Lock()
	if env.standings == nil {
		env.standings = map[string]any{}
	}
	env.standings[account] = map[string]any{"provider": provider, "as_of": now, "next_poll_at": now + 300,
		"allowance": map[string]any{"open": false, "windows": map[string]any{
			"seven_day": map[string]any{"used_pct": 100.0, "resets_at": now + 3*86400, "period": "7d", "scope": nil, "as_of": now}}},
		"money": map[string]any{"credits": map[string]any{"open": true, "balance": 40.0, "unit": "codex_credit"}}}
	env.brainMu.Unlock()
}

// payFrom lets routeID pay pots (nil: the route's pay is []).
func (env *e2eEnv) payFrom(routeID string, pots ...string) {
	env.brainMu.Lock()
	if env.routePolicy == nil {
		env.routePolicy = map[string]map[string]any{}
	}
	if pots == nil {
		pots = []string{}
	}
	env.routePolicy[routeID] = map[string]any{"pay": pots}
	env.brainMu.Unlock()
}

func gptCheck(t *testing.T) dialCheck {
	t.Helper()
	c, _ := routeCheck(gptRoute, "gpt", "", nowFn())
	return c
}

// Row 6: the bound member is dialled only while its check is not `no`. A spent
// gpt account with open credits is no(pay) while the route's pay is unset —
// the session detours to the next member and codex never sees it — and paid
// once the owner lets the route pay gpt/credits: the bound member is dialled
// again, on money, with no other change.
func TestE2EBoundMemberDialledOnlyOnItsCheck(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()
	h.spentWithCredits("gpt", "gpt")
	fetchPreferredSync()

	if c := gptCheck(t); !c.no() || c.Reason != "pay" || !c.Until.known() {
		t.Fatalf("pay unset: gpt check %s, want no(pay until the allowance reopens)", c)
	}
	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-row6-unpaid")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("pay unset: status %d (%s), want the detour's 200", resp.StatusCode, b)
	}
	if hit := h.hit(t); hit.model != "claude-fable-5" {
		t.Fatalf("pay unset: detour dialled %q, want lane:fable", hit.model)
	}
	h.noCodexLeak(t)

	h.payFrom(gptRoute, "gpt/credits")
	fetchPreferredSync()
	logs := captureLogSync(t)
	if c := gptCheck(t); !c.paid() || c.Pot != "gpt/credits" {
		t.Fatalf("pay [gpt/credits]: gpt check %s, want paid:gpt/credits", c)
	}
	resp = h.postSess(t, "claude-frontier", "CLIENT", "sess-row6-paid")
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("pay [gpt/credits]: status %d (%s)", resp.StatusCode, b)
	}
	h.codexHitOrFail(t)
	h.noAnthropicLeak(t)
	// §5.2: the dial logs its check — here, that it spent money.
	if line := waitLog(t, logs, "sess=sess-row ROUTED"); !strings.Contains(line, "CLASS[frontier→gpt/gpt-5.6]") ||
		!strings.Contains(line, "check=paid:gpt/credits") {
		t.Fatalf("served line:\n%s", line)
	}
}

// §5.2 on the bare-lane path (rows 11, 16): the dial's line carries its check.
func TestE2EBareLaneDialLogsItsCheck(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	logs := captureLogSync(t)
	resp := env.post(t, "claude-fable-5", "CLIENT")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	env.hit(t)
	if line := waitLog(t, logs, "lane=fable"); !strings.Contains(line, "check=included") {
		t.Fatalf("served line:\n%s", line)
	}
}

// §2.3: a throttle deranks, and money never dodges it. The bound kimi answers
// a 429 that names no limit (a throttle on kimi); lane:fable has no account to
// stand behind; gpt is paid. The walk stops before gpt and hands the client
// the throttle with a Retry-After that says when kimi is back.
func TestE2EThrottleStopsTheWalkBeforePaid(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()
	h.brainMu.Lock()
	h.classMembers[0]["usable"] = false // lane:fable: its route lists no account
	h.brainMu.Unlock()
	h.spentWithCredits("gpt", "gpt")
	h.payFrom(gptRoute, "gpt/credits")
	h.bindTo.Store("kimi/kimi-k3")
	fetchPreferredSync()
	if c := gptCheck(t); !c.paid() {
		t.Fatalf("fixture: gpt check %s, want paid", c)
	}
	logs := captureLogSync(t)
	h.deny.Store(true)

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-throttle-stop")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status %d (%s), want kimi's 429", resp.StatusCode, b)
	}
	h.hostedHit(t)
	h.noCodexLeak(t)
	h.noAnthropicLeak(t)
	if !throttledOn("kimi") {
		t.Fatal("kimi's 429 wrote no throttle")
	}
	ra, err := strconv.Atoi(resp.Header.Get("Retry-After"))
	if err != nil || ra < 1 || time.Duration(ra)*time.Second > forcedCooldown {
		t.Fatalf("Retry-After = %q, want kimi's throttle (≤ %s)", resp.Header.Get("Retry-After"), forcedCooldown)
	}
	line := waitLog(t, logs, "walk stops before paid")
	if !strings.Contains(line, "gpt/gpt-5.6") || !strings.Contains(line, "paid:gpt/credits") || !strings.Contains(line, "kimi") {
		t.Fatalf("stop line:\n%s", line)
	}
}

// Row 7: the walk re-checks each rung with the newest overlay, so a refusal
// this very walk recorded turns away a rung ranked before it. kimi (bound)
// throttles; the walk ranks lane:fable then lane:opus, both on native (other is
// spent); lane:fable's 429 rejects native's 5h window account-wide, and
// lane:opus — ranked while native was still open — is never dialled.
func TestE2EWalkRechecksEachRung(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	now := time.Now().Truncate(time.Second)
	reset := now.Add(3 * time.Hour)
	h.brainMu.Lock()
	fable, kimi := h.classMembers[0], h.classMembers[2]
	kimi["priority"], fable["priority"] = 1, 2
	opus := map[string]any{"id": "lane:opus", "priority": 3, "kind": "anthropic", "lane": "opus",
		"model": "claude-opus-5", "context_window": 200000, "anthropic_token_ratio": 1.0,
		"effective": 40.0, "usable": true, "status": "ok"}
	h.classMembers = []map[string]any{kimi, fable, opus}
	h.brainMu.Unlock()
	h.spentWithCredits(other, "anthropic") // pay [] everywhere: no(pay) on both lanes
	h.bindTo.Store("kimi/kimi-k3")
	fetchPreferredSync()
	for _, r := range []string{"frontier/lane:fable", "frontier/lane:opus"} {
		if c, _ := routeCheck(r, native, "", nowFn()); !c.included() {
			t.Fatalf("fixture: %s on native %s, want included", r, c)
		}
		if c, _ := routeCheck(r, other, "", nowFn()); !c.no() {
			t.Fatalf("fixture: %s on other %s, want no", r, c)
		}
	}
	h.deny.Store(true)
	h.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later."}}`)
	h.denyModel.Store(e2eDenyModel{model: "fable", headers: map[string]string{
		"anthropic-ratelimit-unified-status":               "rejected",
		"anthropic-ratelimit-unified-reset":                strconv.FormatInt(reset.Unix(), 10),
		"anthropic-ratelimit-unified-representative-claim": "five_hour",
		"anthropic-ratelimit-unified-5h-status":            "rejected",
		"anthropic-ratelimit-unified-5h-reset":             strconv.FormatInt(reset.Unix(), 10),
		"anthropic-ratelimit-unified-5h-utilization":       "1.0",
	}})
	t.Cleanup(func() { h.denyModel.Store(e2eDenyModel{}) })

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-walk-recheck")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	h.hostedHit(t)
	if hit := h.hit(t); hit.model != "claude-fable-5" || hit.auth != "Bearer "+e2eToken(native) {
		t.Fatalf("first rung = %s on %s, want claude-fable-5 on native", hit.model, hit.auth)
	}
	h.noAnthropicLeak(t) // lane:opus was re-checked and turned away
	h.noCodexLeak(t)
	if !containsStr(recordsOn(native), "spent:five_hour") {
		t.Fatalf("native records = %v, want spent:five_hour", recordsOn(native))
	}
	if !laneNo(native, "opus") {
		t.Fatal("native still dialable on lane:opus after its 5h was rejected")
	}
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status %d, want the last refusal's 429", resp.StatusCode)
	}
}

// Row 18: the first-event resend is a dial. When the stalled hop's account
// turns `no` while it hangs (here: its allowance is spent, as a poll would
// show, and the route may not pay), the resend is refused and the walk takes
// the request to the next member; codex sees the stalled request once.
func TestE2EFirstEventResendRefusedByTheCheck(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()
	armHealth(t, newHealthStore(defaultHealthCfg()))
	armObserve(t)
	armFirstEvent(t, map[string]time.Duration{"frontier": 300 * time.Millisecond})
	logs := captureLogSync(t)
	h.codexStall.Store(1)
	h.codexStallHead.Store(true)

	done := make(chan *http.Response, 1)
	go func() { done <- h.postStreamSess(t, "claude-frontier", false, "sess-resend-refused") }()
	h.codexHitOrFail(t) // the stalled hop is in flight
	seedSpent("gpt", "seven_day", 429, "usage_limit_reached", time.Now().Add(48*time.Hour))
	resp := <-done
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d (%s), want the walk's 200", resp.StatusCode, b)
	}
	h.noCodexLeak(t) // no resend
	if hit := h.hit(t); hit.model != "claude-fable-5" {
		t.Fatalf("walk dialled %q, want lane:fable", hit.model)
	}
	waitLog(t, logs, "first-event resend on class frontier member gpt/gpt-5.6 account gpt refused by its check")
	if served := waitLog(t, logs, "sess=sess-res ROUTED"); !strings.Contains(served, "walked=quota") {
		t.Fatalf("served line:\n%s", served)
	}
}

// Row 20: count_tokens asks entitlement only. Every member's check is `no`
// (each account spent, no pay) and the class still answers count_tokens from
// its first member; the answer writes no record and spends nothing.
func TestE2EClassCountTokensIgnoresTheCheck(t *testing.T) {
	h, restore := newE2EClassOn(t, "kimi/kimi-k3", 1.0)
	defer restore()
	h.brainMu.Lock()
	h.classMembers[0]["usable"] = false // lane:fable: its route lists no account
	h.classMembers[1]["usable"] = false // gpt: likewise
	h.brainMu.Unlock()
	h.spentWithCredits("kimi", "kimi")
	fetchPreferredSync()
	for _, id := range []string{"lane:fable", "gpt/gpt-5.6", "kimi/kimi-k3"} {
		if r := classMemberDialable(t, "frontier", id); r == "" {
			t.Fatalf("fixture: %s dialable, want its check no", id)
		}
	}
	before := liveRecords()
	req, _ := http.NewRequest("POST", h.proxy.URL+"/v1/messages/count_tokens",
		strings.NewReader(`{"model":"claude-frontier","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer CLIENT")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-claude-code-session-id", "sess-ct")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(b), "input_tokens") {
		t.Fatalf("count_tokens: status %d (%s)", resp.StatusCode, b)
	}
	if hit := h.hostedHit(t); !strings.HasSuffix(hit.path, "/count_tokens") {
		t.Fatalf("count_tokens reached %s", hit.path)
	}
	if n := liveRecords(); n != before {
		t.Fatalf("count_tokens wrote %d record(s)", n-before)
	}

	// A message on the same class is refused: every member is no.
	resp = h.postSess(t, "claude-frontier", "CLIENT", "sess-ct-msg")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode == 200 {
		t.Fatal("a message was served while every member's check is no")
	}
	select {
	case hit := <-h.hosted:
		t.Fatalf("the refused message reached kimi: %+v", hit)
	default:
	}
}
