package main

// #123 e2e: SWITCH_HEALTH=enforce through the real handler stack. The healthy
// path stays byte-identical to the E0 goldens (E19's zero-cost claim); a head
// 500 walks WITHOUT quota marks or alerts (§5.3); an all-OPEN class answers
// §5.4's synthetic 529 without dialing anyone; a pinned class answers the
// pinned 529 after its head fails (BL-7); and observe counts would-act (N24).

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"
)

// armEnforce swaps the routing mode to enforce (all classes) for one test.
// The store itself is armed separately (armHealth) — matching prod, where
// mode and store are both set once in main().
func armEnforce(t *testing.T) {
	t.Helper()
	oldM, oldF := healthModeV, healthEnforceFor
	healthModeV, healthEnforceFor = healthEnforce, nil
	t.Cleanup(func() { healthModeV, healthEnforceFor = oldM, oldF })
	armAllDegraded(t, true) // Existing full-enforcement cases opt into §5.4.
}

func armAllDegraded(t *testing.T, enabled bool) {
	t.Helper()
	old := healthAllDegradedEnabled
	healthAllDegradedEnabled = enabled
	t.Cleanup(func() { healthAllDegradedEnabled = old })
}

func armObserve(t *testing.T) {
	t.Helper()
	oldM := healthModeV
	healthModeV = healthObserve
	t.Cleanup(func() { healthModeV = oldM })
}

func TestE2EFastCanaryDoesNotDialRecoveringSibling(t *testing.T) {
	for _, mode := range []healthMode{healthObserve, healthEnforce} {
		t.Run(mode.String(), func(t *testing.T) {
			c := testClock(t)
			h, restore := newE2EClass(t)
			defer restore()
			h.brainMu.Lock()
			h.className = "fast"
			h.brainMu.Unlock()
			fetchPreferredSync()
			rebuildProxy(h.e2eEnv)
			hs := newHealthStore(defaultHealthCfg())
			armHealth(t, hs)
			armEnforce(t)
			healthModeV = mode
			healthEnforceFor = map[string]bool{"fast": true}
			halfOpenK(t, hs, c, "kimi/kimi-k3")
			h.kimiSSE.Store("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"still thinking\"}}\n\n")
			resp := h.postSess(t, "claude-fast", "CLIENT", "fast-recovery-regression")
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil || resp.StatusCode != 200 || !strings.Contains(string(body), `"text":"ok"`) {
				t.Fatalf("healthy answer missing: status=%d body=%s err=%v", resp.StatusCode, body, err)
			}
			h.hit(t)
			select {
			case hit := <-h.hosted:
				t.Fatalf("recovering sibling stole fast request: %+v", hit)
			default:
			}
			if !snap(t, hs, "kimi/kimi-k3").probeTok {
				t.Fatal("recovery token consumed")
			}
		})
	}
}

func TestE2EEnforceHealthyHopStaysByteIdentical(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	armEnforce(t)

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-enf-healthy")
	defer resp.Body.Close()
	h.hit(t)

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	wantIn := `{"type":"message","content":[{"type":"text","text":"ok"}]}`
	if string(b) != wantIn {
		t.Fatalf("client bytes drifted under enforce:\n got %q\nwant %q", b, wantIn)
	}
	if got := clientHeaderNames(resp.Header); got != "Content-Length,Content-Type,Date,X-Switch-Decision,X-Switch-Health" {
		t.Fatalf("client header name set drifted under enforce: %s", got)
	}
	waitRing(t, hs, "lane:fable", 1)
	if n := wouldActOf(hs, "frontier"); n != 0 {
		t.Fatalf("a healthy enforce hop counted would_act=%d, want 0", n)
	}
}

// §5.3: a 500 at the head under enforce walks to the next admissible member —
// with NO quota cooldown on the failed member and NO switchover alert (the
// health walk is quota-mark and alert dark by design).
func TestE2EEnforce500HeadWalksWithoutQuotaMarksOrAlerts(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	armEnforce(t)
	h.deny500.Store("*")

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-enf-walk")
	defer resp.Body.Close()
	h.hit(t)       // attempt 1: bound lane:fable, 500
	h.hostedHit(t) // attempt 2: crossed to kimi

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("client status = %d (%s), want kimi's 200", resp.StatusCode, b)
	}
	io.Copy(io.Discard, resp.Body)

	if n := liveRecords(); n != 0 {
		t.Fatalf("the health walk wrote %d record(s) — §5.3 forbids it", n)
	}
	waitRing(t, hs, "lane:fable", 1)
	if s := snap(t, hs, "lane:fable").ring[0]; s.ok {
		t.Fatalf("fable sample = %+v, want a degradation", s)
	}
	waitRing(t, hs, "kimi/kimi-k3", 1)
	if s := snap(t, hs, "kimi/kimi-k3").ring[0]; !s.ok {
		t.Fatalf("kimi sample = %+v, want ok", s)
	}
	if n := wouldActOf(hs, "frontier"); n != 1 {
		t.Fatalf("would_act = %d, want 1 (the walk acted)", n)
	}
	noMoreAlerts(t, h.e2eEnv)
}

// §5.4: every admissible member OPEN — the synthetic 529, minted at route
// time, dialing NOBODY, with the full header contract.
func TestE2EEnforceAllOpenAnswers529(t *testing.T) {
	c := testClock(t)
	h, restore := newE2EClass(t)
	defer restore()
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	armEnforce(t)
	tripK(t, hs, c, "lane:fable")
	tripK(t, hs, c, "kimi/kimi-k3")

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-enf-529")
	defer resp.Body.Close()

	if resp.StatusCode != 529 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d (%s), want 529", resp.StatusCode, b)
	}
	if got := resp.Header.Get("X-Switch-Health"); got != "exhausted" {
		t.Fatalf("X-Switch-Health = %q, want exhausted", got)
	}
	if resp.Header.Get("X-Switch-Decision") == "" {
		t.Fatal("no X-Switch-Decision id on the 529")
	}
	var ra int
	if _, err := fmt.Sscanf(resp.Header.Get("Retry-After"), "%d", &ra); err != nil || ra < 5 || ra > 60 {
		t.Fatalf("Retry-After = %q, want an integer in [5,60]", resp.Header.Get("Retry-After"))
	}
	var e struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	b, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(b, &e); err != nil || e.Error.Type != "overloaded_error" ||
		!strings.Contains(e.Error.Message, "every member of class frontier is degraded") {
		t.Fatalf("body = %s (err %v)", b, err)
	}
	h.noAnthropicLeak(t)
	select {
	case hit := <-h.hosted:
		t.Fatalf("529 path dialed the hosted upstream: %+v", hit)
	default:
	}
	if n := wouldActOf(hs, "frontier"); n != 1 {
		t.Fatalf("would_act = %d, want 1", n)
	}
	noMoreAlerts(t, h.e2eEnv)
}

// BL-7: a pinned class is never walked — its head failure answers the PINNED
// 529, naming the pin, after exactly one dial.
func TestE2EEnforcePinnedHeadFailureAnswersPinned529(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	armEnforce(t)
	armAllDegraded(t, false) // The rollout switch must never weaken a pin.
	h.bindReason.Store("bind: pinned by operator")
	h.deny500.Store("*")

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-enf-pin")
	defer resp.Body.Close()
	h.hit(t) // the pinned member's single dial

	if resp.StatusCode != 529 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d (%s), want the pinned 529", resp.StatusCode, b)
	}
	if got := resp.Header.Get("X-Switch-Health"); got != "pinned-exhausted" {
		t.Fatalf("X-Switch-Health = %q, want pinned-exhausted", got)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "pinned to lane:fable") {
		t.Fatalf("body does not name the pin: %s", b)
	}
	select {
	case hit := <-h.hosted:
		t.Fatalf("a pinned failure crossed to the hosted upstream: %+v", hit)
	default:
	}
	noMoreAlerts(t, h.e2eEnv)
}

func TestE2EEnforceAllOpenRolloutOffStillCallsUpstream(t *testing.T) {
	c := testClock(t)
	h, restore := newE2EClass(t)
	defer restore()
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	armEnforce(t)
	armAllDegraded(t, false)
	tripK(t, hs, c, "lane:fable")
	tripK(t, hs, c, "kimi/kimi-k3")
	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-enf-rollout-ride")
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("rollout-off status = %d, want a real upstream success", resp.StatusCode)
	}
	h.hit(t)
	if resp.Header.Get("X-Switch-Health") == "exhausted" {
		t.Fatal("rollout-off request claimed synthetic exhaustion")
	}
}

func TestE2EEnforceExhaustedWalkRespectsRolloutSwitch(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			h, restore := newE2EClass(t)
			defer restore()
			armHealth(t, newHealthStore(defaultHealthCfg()))
			armEnforce(t)
			armAllDegraded(t, enabled)
			h.deny500.Store("*")
			h.hosted500.Store(true)
			resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-enf-exhaust-walk")
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			want := 500
			if enabled {
				want = 529
			}
			if resp.StatusCode != want {
				t.Fatalf("all-degraded=%v: status %d body %s, want %d", enabled, resp.StatusCode, body, want)
			}
			h.hit(t)
			h.hostedHit(t)
			if !enabled && !strings.Contains(string(body), "acceptance upstream failure") {
				t.Fatalf("rollout-off lost the upstream refusal: %s", body)
			}
		})
	}
}

// N24 under observe: routing untouched (byte-identical), but a request served
// on a key the FSM holds OPEN counts as would-have-acted.
func TestE2EObserveCountsWouldActOnDegradedServe(t *testing.T) {
	c := testClock(t)
	h, restore := newE2EClass(t)
	defer restore()
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	armObserve(t)
	tripK(t, hs, c, "lane:fable")

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-obs-wa")
	defer resp.Body.Close()
	h.hit(t) // observe never reroutes: fable serves despite its OPEN key

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 (observe must not act)", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	wantIn := `{"type":"message","content":[{"type":"text","text":"ok"}]}`
	if string(b) != wantIn {
		t.Fatalf("client bytes drifted under observe: %q", b)
	}
	if n := wouldActOf(hs, "frontier"); n != 1 {
		t.Fatalf("would_act = %d, want 1 (served on an OPEN key)", n)
	}
}

var _ = time.Second
