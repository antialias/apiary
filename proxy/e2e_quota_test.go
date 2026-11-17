package main

// e2e for the quota-exhausted class refusal — an
// incident, replayed end to end through the real handler stack.
//
// A class had exactly two members, gpt and kimi, and both were
// out of BUDGET (not busy — spent, for hours). kimi answered 403 "5-hour
// usage limit"; the cross to gpt answered 429 usage_limit_reached with a resets_at
// three days out. 11 seconds later the retry re-bound to kimi, whose rung
// accepted the connection and then never sent headers, and 120 s after THAT the
// operator got the word "timeout".
//
// The fix asserted here: once every member of a class is cooling from an
// EXPLICIT budget refusal, the next request dials nobody. It is refused in
// milliseconds with a 529 naming each spent member and when it returns — 529
// because that is the one 5xx the agent gateway's classifier maps to "overloaded"
// unconditionally, which fails over instead of burning retries on a member whose
// quota is hours away.

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// providerOnlyShape rebuilds the fixture class as a provider-only helpdesk: gpt P1 and
// kimi P2, no Anthropic rung at all. That absence is the point — such a class has
// no lane:opus (the subscription OAuth lane 429s any non-Claude-Code
// system prompt), so when both providers are spent there is no third rung to
// hide the failure behind.
func providerOnlyShape(h *e2eHosted) {
	h.brainMu.Lock()
	gpt, kimi := h.classMembers[1], h.classMembers[2]
	gpt["usable"], gpt["effective"], gpt["priority"] = true, 20.0, 1
	kimi["priority"] = 2
	h.classMembers = []map[string]any{gpt, kimi}
	h.brainMu.Unlock()
	h.bindTo.Store("kimi/kimi-k3")
	fetchPreferredSync()
}

// quotaErrEnvelope pulls the typed error out of a refusal body.
func quotaErrEnvelope(t *testing.T, resp *http.Response) (string, string) {
	t.Helper()
	raw, _ := io.ReadAll(resp.Body)
	var env struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("body is not an error envelope (%v): %s", err, raw)
	}
	return env.Error.Type, env.Error.Message
}

// (1) The incident. Request one walks the whole class and every member refuses
// on budget; request two is refused locally, with the evidence on the wire.
func TestE2EClassQuotaExhaustedRefusesWithoutDialing(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	providerOnlyShape(h)
	h.deny403.Store(true)    // kimi: 403 "usage limit for this billing cycle"
	h.codexQuota.Store(true) // gpt: 429 usage_limit_reached, resets_at 52 h out

	// --- request 1: the walk that spends the class -------------------------
	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-quota")
	// The request that DISCOVERS the class is spent gets the same answer as the
	// next one. Surfacing gpt's raw 429 here would be true and useless: the agent gateway
	// reads 429 as "rate limited", spends three same-model retries capped at 60 s
	// each against a member whose budget returns in days, and never fails over.
	if resp.StatusCode != 529 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("walk that exhausted the class ended %d, want 529: %s", resp.StatusCode, raw)
	}
	_, walkMsg := quotaErrEnvelope(t, resp)
	for _, want := range []string{"kimi/kimi-k3", "gpt/gpt-5.6"} {
		if !strings.Contains(walkMsg, want) {
			t.Errorf("walk refusal %q missing %q", walkMsg, want)
		}
	}
	resp.Body.Close()
	if first := h.hostedHit(t); first.model != "kimi-k3" {
		t.Fatalf("attempt 1 = %+v, want the bound kimi member", first)
	}
	if second := h.codexHitOrFail(t); second.path != codexResponsesPath {
		t.Fatalf("hop 2 = %+v, want the gpt member on codex", second)
	}
	// Both refusals were explicit budget refusals, so both must be recorded on
	// the QUOTA axis — a speculative cooldown here would let the next request
	// ride the "cooling member as last resort" detour straight into the stall.
	for _, acct := range []string{"kimi", "gpt"} {
		rec, ok := lan.spentRecordOf(acct, nowFn())
		if !ok {
			t.Fatalf("%s carries no spent record after an explicit refusal: %v", acct, recordsOn(acct))
		}
		if rec.detail == "" {
			t.Errorf("%s recorded no cause; the refusal message would name nobody", acct)
		}
	}
	if rec, _ := lan.spentRecordOf("gpt", nowFn()); rec.resets.IsZero() {
		t.Error("gpt sent a resets_at; dropping it means we invent a wait we were told")
	}
	drainClassAlerts(t, h.e2eEnv)

	// --- request 2: same session, nobody left to bet on --------------------
	resp2 := h.postSess(t, "claude-frontier", "CLIENT", "sess-quota")
	defer resp2.Body.Close()

	if resp2.StatusCode != 529 {
		t.Fatalf("status = %d, want 529 (the only 5xx the agent gateway maps to overloaded unconditionally)", resp2.StatusCode)
	}
	if got := resp2.Header.Get("X-Switch-Quota"); got != "exhausted" {
		t.Errorf("X-Switch-Quota = %q, want %q", got, "exhausted")
	}
	if got := resp2.Header.Get("Retry-After"); got == "" || got == "0" {
		t.Errorf("Retry-After = %q; a refusal that cannot say when to come back is a dead end", got)
	}
	errType, msg := quotaErrEnvelope(t, resp2)
	if errType != "overloaded_error" {
		t.Errorf("error type = %q, want overloaded_error", errType)
	}
	for _, want := range []string{"kimi/kimi-k3", "gpt/gpt-5.6", "quota"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message %q missing %q", msg, want)
		}
	}
	// The upstream's own words, not ours: the operator should be able to read
	// the cause without cross-referencing a log.
	if !strings.Contains(msg, "usage_limit_reached") && !strings.Contains(strings.ToLower(msg), "usage limit") {
		t.Errorf("message %q does not carry either upstream's stated cause", msg)
	}
	// And the clause that tells the consumer whether its OWN failover is worth a
	// round-trip. This fixture serves exactly one class and both its members are
	// cooling, so nothing on this router would be routed to — which is a fact
	// about the router, checkable here, and not a claim about anyone's budget.
	if !strings.Contains(msg, "no other model on this router is routable") {
		t.Errorf("message %q never says whether failing over elsewhere could help", msg)
	}

	// The headline: nobody was dialed. This is the request that before the fix
	// rode a spent kimi rung into a 120 s hang and called itself a timeout.
	h.noCodexLeak(t)
	h.noAnthropicLeak(t)
	select {
	case hit := <-h.hosted:
		t.Fatalf("the refusal still dialed a spent member: %+v", hit)
	default:
	}
}

// (2) The refusal is narrow: it needs EVERY member to have said "spent". One
// member refused on budget and one merely speculatively cooling is not an
// exhausted class, and must still be served — because a speculative cooldown is
// a guess about a busy upstream while a budget refusal is a fact the upstream
// stated. Getting this backwards would turn a 429 the next request could have
// survived into a class-wide outage.
func TestE2EClassQuotaRefusalNeedsEveryMemberSpent(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	providerOnlyShape(h)
	h.deny403.Store(true)   // kimi: explicit budget refusal, hours out
	h.codexDeny.Store(true) // gpt: a bare "rate limited" — busy, not spent

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-narrow")
	resp.Body.Close()
	h.hostedHit(t)
	h.codexHitOrFail(t)
	if _, ok := lan.spentRecordOf("kimi", nowFn()); !ok {
		t.Fatal("kimi stated a billing-cycle limit and was not recorded spent")
	}
	if _, ok := lan.spentRecordOf("gpt", nowFn()); ok {
		t.Fatal(`an ambiguous "rate limited" was recorded as a budget refusal; that would refuse traffic a retry could have served`)
	}
	drainClassAlerts(t, h.e2eEnv)

	// gpt's throttle does not even need to lapse: it deranks, never refuses.
	// The upstream is healthy again. kimi is still spent for hours.
	h.codexDeny.Store(false)

	resp2 := h.postSess(t, "claude-frontier", "CLIENT", "sess-narrow")
	defer resp2.Body.Close()
	if resp2.StatusCode == 529 {
		_, msg := quotaErrEnvelope(t, resp2)
		t.Fatalf("refused a class that still had a member with quota: %s", msg)
	}
	if hit := h.codexHitOrFail(t); hit.path != codexResponsesPath {
		t.Fatalf("hop = %+v, want the member that never claimed to be out of budget", hit)
	}
	if resp2.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 from the member that still had quota", resp2.StatusCode)
	}
	// And the spent member was detoured, not ridden: no second 403 for nothing.
	select {
	case hit := <-h.hosted:
		t.Fatalf("dialed the member we were told is out of budget: %+v", hit)
	default:
	}
}

// drainClassAlerts takes whatever the coalescer has flushed without asserting a
// count — these tests are about the refusal, and the switchover alerts raised by
// the setup walk are someone else's assertion.
func drainClassAlerts(t *testing.T, env *e2eEnv) {
	t.Helper()
	deadline := time.After(250 * time.Millisecond)
	for {
		select {
		case <-env.bridge:
		case <-deadline:
			return
		}
	}
}
