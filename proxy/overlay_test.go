package main

import (
	"net/http"
	"strconv"
	"testing"
	"time"
)

// opusHeaders is what an opus 200 carries: the unified 5h and 7d windows and
// no Fable cap.
func opusHeaders(at time.Time) http.Header {
	return rlHeader(
		"anthropic-ratelimit-unified-5h-status", "allowed",
		"anthropic-ratelimit-unified-5h-utilization", "0.12",
		"anthropic-ratelimit-unified-5h-reset", strconv.FormatInt(at.Add(2*time.Hour).Unix(), 10),
		"anthropic-ratelimit-unified-7d-status", "allowed",
		"anthropic-ratelimit-unified-7d-utilization", "0.40",
		"anthropic-ratelimit-unified-7d-reset", strconv.FormatInt(at.Add(90*time.Hour).Unix(), 10))
}

// Staleness is per limit (§1.4), and a response refreshes the limits it
// carries. The brain's polls of account a have failed for 45 minutes while
// opus responses kept flowing; a has credits on. lane:opus is bound by 5h and
// 7d, which every response refreshes, so it stays included. lane:fable is also
// bound by the Fable cap, which no opus response carries: that reading is 45
// minutes old with money at risk, so the Fable lane is no(unknown). With
// credits off nothing is at risk, and the Fable lane uses the old value.
func TestOverlayRefreshesTheLimitsAResponseCarries(t *testing.T) {
	now := time.Now()
	polled := now.Add(-45 * time.Minute)
	for _, credits := range []string{"open", "closed"} {
		t.Run("credits "+credits, func(t *testing.T) {
			availWorld(t, map[string]lanePref{"fable": {Account: "a"}, "opus": {Account: "a"}}, map[string]acctStanding{
				"a": standingAt("anthropic", polled, map[string]checkWindow{
					"five_hour": fiveHour(10, polled.Add(3*time.Hour)), "seven_day": week(40, polled.Add(90*time.Hour)),
					"scoped:Fable": scoped("Fable", 30, polled.Add(90*time.Hour))}, credits),
			})
			setFableAliases(fableAliases())
			noteHeaderReading("a", opusHeaders(now.Add(-time.Minute)), now.Add(-time.Minute))

			if c := laneCheck("opus", now); !c.included() || c.Basis != "fresh" {
				t.Fatalf("lane:opus = %s, want included on fresh 5h and 7d readings", c)
			}
			c := laneCheck("fable", now)
			if credits == "open" && (!c.no() || c.Reason != "unknown" || c.Basis != "stale") {
				t.Fatalf("lane:fable = %s, want no(unknown): its Fable cap is 45 min old and money is at risk", c)
			}
			if credits == "closed" && (!c.included() || c.Basis != "stale") {
				t.Fatalf("lane:fable = %s, want included on the last value: nothing to pay", c)
			}
		})
	}
}

// Codex's own gate (rate_limit.allowed) can close the allowance below 100.
// The brain keeps it closed until every window it could mean has reset
// (_allowance_state), and a response showing the windows under 100 does not
// say which one the gate meant: the overlay keeps the gate closed.
func TestOverlayKeepsCodexGateUntilItsWindowsReset(t *testing.T) {
	t0 := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	r5, r7 := t0.Add(3*time.Hour), t0.Add(96*time.Hour)
	shut := false
	gpt := standingAt("codex", t0, map[string]checkWindow{"five_hour": fiveHour(80, r5), "seven_day": week(60, r7)}, "")
	gpt.Allowance.Open = &shut
	availWorld(t, nil, map[string]acctStanding{"gpt-a": gpt})
	pref.mu.Lock()
	pref.routes["c/gpt"] = memberRoute("c/gpt", "c", "gpt", "", "codex", "gpt-a")
	pref.mu.Unlock()
	full := rlHeader("x-codex-primary-used-percent", "81", "x-codex-primary-window-minutes", "300",
		"x-codex-primary-reset-at", strconv.FormatInt(r5.Unix(), 10),
		"x-codex-secondary-used-percent", "61", "x-codex-secondary-window-minutes", "10080",
		"x-codex-secondary-reset-at", strconv.FormatInt(r7.Unix(), 10))
	noteHeaderReading("gpt-a", full, t0.Add(time.Minute))
	if c, _ := routeCheck("c/gpt", "gpt-a", "", time.Now()); !c.no() || c.Until.At != unixSeconds(r7) {
		t.Fatalf("gate closed, windows under 100 = %s, want no until the weekly reset", c)
	}
	// Once both windows it could mean have reset, the gate no longer stands.
	if c, _ := routeCheck("c/gpt", "gpt-a", "", r7.Add(time.Minute)); c.no() && c.Reason == "spent" {
		t.Fatalf("after every reset = %s, want the gate gone", c)
	}
}
