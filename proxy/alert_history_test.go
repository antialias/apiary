package main

// Tests for the alert-history gates (alert_history.go) — the
// storm-damping fix. History-as-state: gate 2 (identical-alert repeat window,
// tier-aware) runs first; gate 1 (lane-scoped critical cooldown, demotes to
// telegram) runs second. Both funnel through gateAndEnqueue.

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// bridgeCapture stands in for the alert receiver for gate tests: it records
// every POSTed alert in arrival order and lets the test control the HTTP status
// it replies with. Unlike captureBridgeLane it makes no "exactly one alert"
// assumption — suppression and multi-flush sequences are the point here.
type bridgeCapture struct {
	mu     sync.Mutex
	alerts []bridgeAlert
	status int
	body   []byte // optional response body — the real bridge replies with per-channel JSON
	srv    *httptest.Server
}

func newBridgeCapture(t *testing.T) *bridgeCapture {
	t.Helper()
	bc := &bridgeCapture{status: 200}
	bc.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload struct {
			Alerts []bridgeAlert `json:"alerts"`
		}
		if err := json.Unmarshal(body, &payload); err == nil {
			bc.mu.Lock()
			bc.alerts = append(bc.alerts, payload.Alerts...)
			bc.mu.Unlock()
		}
		bc.mu.Lock()
		status, respBody := bc.status, bc.body
		bc.mu.Unlock()
		w.WriteHeader(status)
		if len(respBody) > 0 {
			w.Write(respBody)
		}
	}))
	t.Cleanup(bc.srv.Close)
	return bc
}

func (bc *bridgeCapture) setStatus(s int) { bc.mu.Lock(); bc.status = s; bc.mu.Unlock() }

// setBody makes the capture reply like the reference alert receiver, whose JSON body
// carries per-channel delivery booleans (parseBridgeChannels reads them).
func (bc *bridgeCapture) setBody(b string) { bc.mu.Lock(); bc.body = []byte(b); bc.mu.Unlock() }

func (bc *bridgeCapture) captured() []bridgeAlert {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	return append([]bridgeAlert(nil), bc.alerts...)
}

// waitForN polls until at least n alerts have been POSTed (deadline-poll, same
// pattern as classAlertsN) — no fixed sleeps on the happy path.
func (bc *bridgeCapture) waitForN(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(bc.captured()) >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("bridge saw %d alerts, want >= %d", len(bc.captured()), n)
}

func (bc *bridgeCapture) tiers() []string {
	var out []string
	for _, a := range bc.captured() {
		out = append(out, a.Labels["delivery"])
	}
	return out
}

func (bc *bridgeCapture) assertCountStable(t *testing.T, n int, settle time.Duration) {
	t.Helper()
	time.Sleep(settle)
	if got := len(bc.captured()); got != n {
		t.Fatalf("bridge saw %d alerts, want exactly %d", got, n)
	}
}

// alertSettle waits for every in-flight flushSwitchAlerts goroutine to finish —
// the seam that keeps a prior test's coalesce flush from publishing into this
// test's freshly-armed event ring (caught by CI's race scheduler; never lost
// locally).
func alertSettle(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&alertFlushInFlight) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("in-flight alert flush never settled")
}

// alertQuiesce is the complete "no alert goroutine will touch the hub" guarantee
// a fixture needs before swapping globals: pending coalesce timer stopped, queue
// dropped, then alertSettle joins any flush already running. A fired-but-not-yet-
// drained flush is harmless afterwards — it finds the queue empty and returns
// without publishing.
func alertQuiesce(t *testing.T) {
	t.Helper()
	alertMu.Lock()
	alertQueue = nil
	if alertTimer != nil {
		alertTimer.Stop()
		alertTimer = nil
	}
	alertMu.Unlock()
	alertSettle(t)
}

// swapAlertTestWorld replaces every global the alert gates read and restores it
// on cleanup. crit/repeat/coalesce are set verbatim (timing tests shrink them).
func swapAlertTestWorld(t *testing.T, bc *bridgeCapture, loud bool,
	weekly map[string]float64, lanes map[string]lanePref,
	crit, repeat, coalesce time.Duration) {
	t.Helper()
	oldLoud, oldPct, oldCoalesce := alwaysLoud, loudWeeklyPct, alertCoalesce
	oldCrit, oldRepeat := alertCritCooldown, alertRepeatWindow
	oldBridge := swapBridgeURL(bc.srv.URL)
	oldHistory := swapAlertHistory(nil)
	lan.pref.mu.Lock()
	oldWeekly, oldLanes := lan.pref.weekly, lan.pref.lanes
	lan.pref.weekly, lan.pref.lanes = weekly, lanes
	lan.pref.mu.Unlock()
	alwaysLoud, loudWeeklyPct = loud, 80.0
	alertCritCooldown, alertRepeatWindow, alertCoalesce = crit, repeat, coalesce
	// No stale queue/timer may flush a prior test's alerts into this bridge, and
	// no ALREADY-DRAINED flush goroutine may publish a prior test's events into
	// this test's ring: a fired timer can't be un-fired, and publish() checks
	// armed-ness at publish time, not enqueue time. Arm the hub (evTest/evArm)
	// AFTER this call, never before.
	alertQuiesce(t)
	t.Cleanup(func() {
		alertQuiesce(t)
		swapBridgeURL(oldBridge)
		swapAlertHistory(oldHistory)
		alwaysLoud, loudWeeklyPct, alertCoalesce = oldLoud, oldPct, oldCoalesce
		alertCritCooldown, alertRepeatWindow = oldCrit, oldRepeat
		lan.pref.mu.Lock()
		lan.pref.weekly, lan.pref.lanes = oldWeekly, oldLanes
		lan.pref.mu.Unlock()
	})
}

// Default gate windows for tests that never wait out a window — only identity
// and ordering matter, so production defaults are exact and safe.
const (
	testCritDefault   = 6 * time.Hour
	testRepeatDefault = time.Hour
	testCoalesce      = 5 * time.Millisecond
)

var (
	quietWeekly = map[string]float64{native: 96, other: 41} // to=other: 41% < 80 → telegram
	loudWeekly  = map[string]float64{native: 96, other: 84} // to=other: 84% ≥ 80 → critical
)

func TestFirstLoudFiresCritical(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, loudWeekly, nil, testCritDefault, testRepeatDefault, testCoalesce)

	lan.fireSwitchAlert("opus", native, other, "test")
	bc.waitForN(t, 1)
	if got := bc.tiers()[0]; got != "critical" {
		t.Fatalf("first loud alert tier = %q, want critical", got)
	}
	hist := historySnapshot()
	if len(hist) != 1 || hist[0].tier != "critical" {
		t.Fatalf("history = %+v, want one critical record", hist)
	}
}

func TestSecondLoudSameLaneDemotesWithinCritCooldown(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, loudWeekly, nil, testCritDefault, testRepeatDefault, testCoalesce)

	lan.fireSwitchAlert("opus", native, other, "test") // critical
	lan.fireSwitchAlert("opus", other, native, "test") // same lane, reversed pair → demoted
	bc.waitForN(t, 2)

	got := bc.captured()
	if got[0].Labels["delivery"] != "critical" {
		t.Fatalf("first alert tier = %q, want critical", got[0].Labels["delivery"])
	}
	second := got[1]
	if second.Labels["delivery"] != "telegram" {
		t.Errorf("second loud alert on same lane must demote to telegram, got %q", second.Labels["delivery"])
	}
	if second.Annotations["title"] != switchTitleRoutine {
		t.Errorf("demoted alert must carry the routine title, got %q", second.Annotations["title"])
	}
	if !strings.Contains(second.Annotations["description"], "Demoted from critical") {
		t.Errorf("demoted alert must explain itself, desc = %q", second.Annotations["description"])
	}
	// The demoted record stores its FINAL tier — it must not re-arm gate 1.
	hist := historySnapshot()
	if len(hist) != 2 || hist[1].tier != "telegram" {
		t.Fatalf("history final tier = %+v, want [critical telegram]", hist)
	}
}

// Onset regression (review finding 1): a quiet A→B flip that lands just before
// the destination crosses the last-hop threshold must NOT mute the storm-onset
// critical for the same pair — gate 2 suppresses only at-or-below-tier matches.
func TestQuietThenLoudSamePairPagesImmediately(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, quietWeekly, nil, testCritDefault, testRepeatDefault, testCoalesce)

	lan.fireSwitchAlert("opus", native, other, "test") // quiet (other=41%)
	bc.waitForN(t, 1)

	// Destination crosses the threshold between the two flips.
	lan.pref.mu.Lock()
	lan.pref.weekly = loudWeekly
	lan.pref.mu.Unlock()

	lan.fireSwitchAlert("opus", native, other, "test") // same pair, now loud
	bc.waitForN(t, 2)

	tiers := bc.tiers()
	if tiers[0] != "telegram" || tiers[1] != "critical" {
		t.Fatalf("tiers = %v, want [telegram critical] — the onset page must not wait out the repeat window", tiers)
	}
}

func TestIdenticalPairWithinRepeatWindowSuppressed_SwitchPath(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, quietWeekly, nil, testCritDefault, testRepeatDefault, testCoalesce)

	lan.fireSwitchAlert("opus", native, other, "test")
	lan.fireSwitchAlert("opus", native, other, "test") // identical, within the window
	bc.waitForN(t, 1)
	bc.assertCountStable(t, 1, 250*time.Millisecond)

	if hist := historySnapshot(); len(hist) != 1 {
		t.Fatalf("suppressed fire must NOT be recorded — history len = %d, want 1", len(hist))
	}
}

func TestIdenticalPairWithinRepeatWindowSuppressed_ClassPath(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, nil, nil, testCritDefault, testRepeatDefault, testCoalesce)

	lan.fireClassAlert("helpdesk", "sess-a", "anthropic", "kimi", "test")
	lan.fireClassAlert("helpdesk", "sess-a", "anthropic", "kimi", "test")
	bc.waitForN(t, 1)
	bc.assertCountStable(t, 1, 250*time.Millisecond)
}

func TestDifferentPairSameLaneFires(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, quietWeekly, nil, testCritDefault, testRepeatDefault, testCoalesce)

	lan.fireSwitchAlert("opus", native, other, "test")
	lan.fireSwitchAlert("opus", other, native, "test") // reversed pair = a different alert
	bc.waitForN(t, 2)
	bc.assertCountStable(t, 2, 250*time.Millisecond)
}

// A persistent episode re-pages once per crit cooldown — the reminder. Timing
// margins are ×5 the coalesce delay (review finding: race CI is slow).
func TestCritCooldownExpiryRefiresReminder(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, loudWeekly, nil,
		750*time.Millisecond, 200*time.Millisecond, 25*time.Millisecond)

	lan.fireSwitchAlert("opus", native, other, "test") // t≈0: critical
	bc.waitForN(t, 1)

	time.Sleep(500 * time.Millisecond)                 // past repeat window, within crit cooldown
	lan.fireSwitchAlert("opus", native, other, "test") // demoted telegram
	bc.waitForN(t, 2)

	time.Sleep(750 * time.Millisecond)                 // now past the crit cooldown from t≈0
	lan.fireSwitchAlert("opus", native, other, "test") // reminder critical
	bc.waitForN(t, 3)

	tiers := bc.tiers()
	want := []string{"critical", "telegram", "critical"}
	for i := range want {
		if tiers[i] != want[i] {
			t.Fatalf("tier sequence = %v, want %v", tiers, want)
		}
	}
}

// A failed flush must roll the batch's records back out of history, so the next
// (even identical) flip retries instead of being gated by an alert the operator
// never saw.
func TestFlushFailurePrunesHistory(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, loudWeekly, nil, testCritDefault, testRepeatDefault, 25*time.Millisecond)

	bc.setStatus(500)
	lan.fireSwitchAlert("opus", native, other, "test")
	bc.waitForN(t, 1) // the bridge still SAW the POST (and 500'd it)
	time.Sleep(100 * time.Millisecond)
	if hist := historySnapshot(); len(hist) != 0 {
		t.Fatalf("failed flush must prune history, got %+v", hist)
	}

	bc.setStatus(200)
	lan.fireSwitchAlert("opus", native, other, "test") // identical, within both windows — must still fire
	bc.waitForN(t, 2)
	if got := bc.tiers()[1]; got != "critical" {
		t.Fatalf("re-fire after failed flush tier = %q, want critical", got)
	}
}

func TestAlwaysLoudDampedByCritCooldown(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, true, quietWeekly, nil, testCritDefault, testRepeatDefault, testCoalesce)

	lan.fireSwitchAlert("opus", native, other, "test") // alwaysLoud → critical
	lan.fireSwitchAlert("opus", other, native, "test") // still damped by the lane's cooldown
	bc.waitForN(t, 2)

	tiers := bc.tiers()
	if tiers[0] != "critical" || tiers[1] != "telegram" {
		t.Fatalf("ALWAYS_LOUD tiers = %v, want [critical telegram] — the escape hatch is damped, not raw", tiers)
	}
}

// The storm shape, pinned end-to-end (adversarial-review finding):
// both accounts exhausted, so EVERY flip-half is a critical candidate. Gate 2
// must compare CANDIDATE tiers — a record demoted to telegram by gate 1 still
// entered critical, and must suppress its pair's critical repeats. Comparing
// final tiers instead re-sends a demoted telegram on every flip forever
// (~12/h), which is the storm surviving the damper at a quieter volume.
func TestCriticalFlapStormSuppressedAfterDemotion(t *testing.T) {
	bc := newBridgeCapture(t)
	// Repeat window short enough to expire mid-test; crit cooldown armed
	// throughout, so re-admitted fires demote rather than re-page.
	swapAlertTestWorld(t, bc, false, loudWeekly, nil,
		5*time.Second, 300*time.Millisecond, 25*time.Millisecond)

	lan.fireSwitchAlert("opus", native, other, "test") // critical: the onset page
	lan.fireSwitchAlert("opus", other, native, "test") // critical candidate → demoted telegram
	bc.waitForN(t, 2)

	lan.fireSwitchAlert("opus", native, other, "test") // repeat vs a CRITICAL record → suppressed
	lan.fireSwitchAlert("opus", other, native, "test") // repeat vs the DEMOTED record → suppressed (the fix)
	bc.assertCountStable(t, 2, 250*time.Millisecond)

	tiers := bc.tiers()
	if tiers[0] != "critical" || tiers[1] != "telegram" {
		t.Fatalf("storm onset tiers = %v, want [critical telegram]", tiers)
	}
	// The demoted record must remember its critical INTENT — that is what
	// suppressed the fourth fire — while storing telegram as what was sent.
	hist := historySnapshot()
	if len(hist) != 2 {
		t.Fatalf("history len = %d, want 2 (suppressed fires unrecorded)", len(hist))
	}
	if hist[1].tier != "telegram" || hist[1].candTier != "critical" {
		t.Fatalf("demoted record tier/candTier = %q/%q, want telegram/critical",
			hist[1].tier, hist[1].candTier)
	}

	// Past the repeat window (cooldown still armed): each pair re-admits ONCE,
	// demoted — the ≤1 message per pair per window bound, not silence forever.
	time.Sleep(400 * time.Millisecond)
	lan.fireSwitchAlert("opus", other, native, "test")
	bc.waitForN(t, 3)
	if got := bc.tiers()[2]; got != "telegram" {
		t.Fatalf("re-admitted fire tier = %q, want telegram (cooldown still armed)", got)
	}
}

func TestClassAlertDropsSessionForDedup(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, nil, nil, testCritDefault, testRepeatDefault, testCoalesce)

	lan.fireClassAlert("helpdesk", "sess-A", "anthropic", "kimi", "test")
	lan.fireClassAlert("helpdesk", "sess-B", "anthropic", "kimi", "test") // same move, other session: dup
	bc.waitForN(t, 1)
	bc.assertCountStable(t, 1, 250*time.Millisecond)

	lan.fireClassAlert("helpdesk", "sess-B", "kimi", "anthropic", "test") // reversed pair: new alert
	bc.waitForN(t, 2)
}

// Suppression silences only the bridge dispatch — the flip itself stays visible
// on the events hub (and in logs).
// alertEvents decodes every ring frame of the given kind into its data map,
// ring order preserved. Only meaningful under evTest(t) + evArm().
func alertEvents(t *testing.T, kind string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, f := range evRingFrames() {
		p := evParseFrame(t, f.b)
		if p.event != kind {
			continue
		}
		data, ok := p.env.Data.(map[string]any)
		if !ok {
			t.Fatalf("%s frame data is %T, want an object: %s", kind, p.env.Data, p.raw)
		}
		out = append(out, data)
	}
	return out
}

// awaitEvents deadline-polls the ring for at least n frames of kind — the
// flush (and its events) runs on a timer goroutine.
func awaitEvents(t *testing.T, kind string, n int) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if evs := alertEvents(t, kind); len(evs) >= n {
			return evs
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("ring has %d %s frame(s), want >= %d", len(alertEvents(t, kind)), kind, n)
	return nil
}

// ringKindOrder returns each ring frame's event kind in order.
func ringKindOrder(t *testing.T) []string {
	t.Helper()
	var kinds []string
	for _, f := range evRingFrames() {
		kinds = append(kinds, evParseFrame(t, f.b).event)
	}
	return kinds
}

func TestSuppressedFlipStillPublishes(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, quietWeekly, nil, testCritDefault, testRepeatDefault, testCoalesce)
	evTest(t)
	evArm()

	lan.fireSwitchAlert("opus", native, other, "test")
	lan.fireSwitchAlert("opus", native, other, "test") // suppressed from the bridge
	bc.waitForN(t, 1)
	bc.assertCountStable(t, 1, 250*time.Millisecond)

	// Count by KIND, not total: suppression must not hide the flip, and every
	// candidate — the suppressed one included — must yield a dispatch event.
	if n := len(alertEvents(t, "switchover")); n != 2 {
		t.Fatalf("switchover events = %d, want 2 — suppression must not hide the switchover event", n)
	}
	if n := len(awaitEvents(t, "alert_dispatch", 2)); n != 2 {
		t.Fatalf("alert_dispatch events = %d, want 2 (one per candidate)", n)
	}
	if n := len(awaitEvents(t, "alert_delivery", 1)); n != 1 {
		t.Fatalf("alert_delivery events = %d, want 1 (one per flushed batch)", n)
	}
}

// Every candidate yields an alert_dispatch; a suppressed one names its
// suppressor and carries no final tier — it delivered nothing.
func TestDispatchSuppressedLinksSuppressor(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, quietWeekly, nil, testCritDefault, testRepeatDefault, testCoalesce)
	evTest(t)
	evArm()

	lan.fireSwitchAlert("opus", native, other, "brain says so")
	bc.waitForN(t, 1) // flush done → the admitted dispatch is on the ring
	lan.fireSwitchAlert("opus", native, other, "brain says so")

	disp := awaitEvents(t, "alert_dispatch", 2)
	passed, supp := disp[0], disp[1]
	if passed["outcome"] != "passed" || passed["final_tier"] != "telegram" || passed["cand_tier"] != "telegram" {
		t.Fatalf("admitted dispatch = %v, want outcome=passed final_tier=telegram cand_tier=telegram", passed)
	}
	id, _ := passed["id"].(string)
	if !strings.HasPrefix(id, "d-") {
		t.Fatalf("dispatch id = %q, want a d- prefix (outside the SSE frame-id namespace)", id)
	}
	// Tier evidence rides the switch-path dispatch: quietWeekly puts `other` at 41%.
	if passed["to_pct"] != 41.0 || passed["known"] != true || passed["loud_pct"] != 80.0 || passed["always_loud"] != false {
		t.Fatalf("tier inputs = %v, want to_pct=41 known=true loud_pct=80 always_loud=false", passed)
	}
	if supp["outcome"] != "suppressed" || supp["gate"] != "repeat_window" {
		t.Fatalf("suppressed dispatch = %v, want outcome=suppressed gate=repeat_window", supp)
	}
	if supp["suppressor_id"] != id {
		t.Fatalf("suppressor_id = %v, want the admitted dispatch's id %q", supp["suppressor_id"], id)
	}
	if _, hasFinal := supp["final_tier"]; hasFinal {
		t.Fatalf("suppressed dispatch carries final_tier %v — nothing was dispatched", supp["final_tier"])
	}
}

// A gate-1 demotion is visible in the dispatch: critical intent, telegram
// outcome, and the cooldown named as the cause.
func TestDispatchDemotedCarriesGateAndTierInputs(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, loudWeekly, nil, testCritDefault, testRepeatDefault, testCoalesce)
	evTest(t)
	evArm()

	lan.fireSwitchAlert("opus", native, other, "test") // to=other 84% → critical
	bc.waitForN(t, 1)
	lan.fireSwitchAlert("opus", other, native, "test") // to=native 96% → critical candidate, demoted

	disp := awaitEvents(t, "alert_dispatch", 2)
	demotedEv := disp[1]
	if demotedEv["outcome"] != "demoted" || demotedEv["cand_tier"] != "critical" || demotedEv["final_tier"] != "telegram" {
		t.Fatalf("demoted dispatch = %v, want outcome=demoted cand_tier=critical final_tier=telegram", demotedEv)
	}
	if demotedEv["gate"] != "crit_cooldown" {
		t.Fatalf("gate = %v, want crit_cooldown", demotedEv["gate"])
	}
	if detail, _ := demotedEv["gate_detail"].(string); detail == "" {
		t.Fatalf("gate_detail is empty — the feed needs the why")
	}
	if demotedEv["to_pct"] != 96.0 {
		t.Fatalf("to_pct = %v, want 96 (loudWeekly[native])", demotedEv["to_pct"])
	}
}

// The happy path: one delivery event per batch, ids matching its dispatches,
// per-channel booleans parsed from the bridge body, and dispatch strictly
// before delivery on the wire.
func TestDeliveryEventSuccessParsesChannels(t *testing.T) {
	bc := newBridgeCapture(t)
	bc.setBody(`{"ok": true, "telegram": true, "ha": null, "delivered": true}`)
	swapAlertTestWorld(t, bc, false, quietWeekly, nil, testCritDefault, testRepeatDefault, testCoalesce)
	evTest(t)
	evArm()

	lan.fireSwitchAlert("opus", native, other, "test")
	del := awaitEvents(t, "alert_delivery", 1)[0]
	disp := awaitEvents(t, "alert_dispatch", 1)[0]

	if del["delivered"] != true || del["rolled_back"] != false || del["bridge_status"] != 200.0 {
		t.Fatalf("delivery = %v, want delivered=true rolled_back=false bridge_status=200", del)
	}
	ids, _ := del["ids"].([]any)
	if len(ids) != 1 || ids[0] != disp["id"] {
		t.Fatalf("delivery ids = %v, want exactly the batch's dispatch id %v", ids, disp["id"])
	}
	ch, _ := del["channels"].(map[string]any)
	if ch == nil || ch["telegram"] != true || ch["ha"] != nil {
		t.Fatalf("channels = %v, want telegram=true ha=null (no critical in batch)", del["channels"])
	}
	kinds := ringKindOrder(t)
	dispatchAt, deliveryAt := -1, -1
	for i, k := range kinds {
		if k == "alert_dispatch" && dispatchAt == -1 {
			dispatchAt = i
		}
		if k == "alert_delivery" {
			deliveryAt = i
		}
	}
	if dispatchAt == -1 || deliveryAt == -1 || dispatchAt > deliveryAt {
		t.Fatalf("ring order %v — dispatch must precede its delivery", kinds)
	}
}

// A bridge 502 (total delivery failure) rolls back AND says so on the feed,
// with the failed channels named.
func TestDeliveryEventOn502RollsBack(t *testing.T) {
	bc := newBridgeCapture(t)
	bc.setStatus(502)
	bc.setBody(`{"ok": false, "telegram": false, "ha": false, "delivered": false}`)
	swapAlertTestWorld(t, bc, false, quietWeekly, nil, testCritDefault, testRepeatDefault, testCoalesce)
	evTest(t)
	evArm()

	lan.fireSwitchAlert("opus", native, other, "test")
	del := awaitEvents(t, "alert_delivery", 1)[0]
	if del["delivered"] != false || del["rolled_back"] != true || del["bridge_status"] != 502.0 {
		t.Fatalf("delivery = %v, want delivered=false rolled_back=true bridge_status=502", del)
	}
	ch, _ := del["channels"].(map[string]any)
	if ch == nil || ch["telegram"] != false || ch["ha"] != false {
		t.Fatalf("channels = %v, want telegram=false ha=false parsed off the 502 body", del["channels"])
	}
	if len(historySnapshot()) != 0 {
		t.Fatalf("history not rolled back after 502")
	}
}

// A transport-level POST failure still accounts for the batch: delivery event
// with no HTTP status, no channels, rolled back.
func TestDeliveryEventOnPostError(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, quietWeekly, nil, testCritDefault, testRepeatDefault, testCoalesce)
	evTest(t)
	evArm()
	bc.srv.Close() // connection refused from here on

	lan.fireSwitchAlert("opus", native, other, "test")
	del := awaitEvents(t, "alert_delivery", 1)[0]
	if del["delivered"] != false || del["rolled_back"] != true || del["bridge_status"] != nil {
		t.Fatalf("delivery = %v, want delivered=false rolled_back=true bridge_status=null", del)
	}
	if del["channels"] != nil {
		t.Fatalf("channels = %v, want null — no bridge exchange happened", del["channels"])
	}
}

// The class path emits dispatches too — telegram-intent, class lane, and gate-2
// suppression linking, all through the same funnel.
func TestClassDispatchEvents(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, nil, nil, testCritDefault, testRepeatDefault, testCoalesce)
	evTest(t)
	evArm()

	lan.fireClassAlert("helpdesk", "sess-a", "anthropic", "kimi", "quota 429")
	bc.waitForN(t, 1)
	lan.fireClassAlert("helpdesk", "sess-b", "anthropic", "kimi", "quota 429") // suppressed (session-blind dedup)

	disp := awaitEvents(t, "alert_dispatch", 2)
	passed, supp := disp[0], disp[1]
	if passed["outcome"] != "passed" || passed["lane"] != "class:helpdesk" || passed["cand_tier"] != "telegram" {
		t.Fatalf("class dispatch = %v, want outcome=passed lane=class:helpdesk cand_tier=telegram", passed)
	}
	if _, hasPct := passed["to_pct"]; hasPct {
		t.Fatalf("class dispatch carries to_pct %v — the class path has no tiering decision", passed["to_pct"])
	}
	if supp["outcome"] != "suppressed" || supp["suppressor_id"] != passed["id"] {
		t.Fatalf("suppressed class dispatch = %v, want suppressor_id=%v", supp, passed["id"])
	}
}

func TestHistoryBoundedByAgePrune(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, quietWeekly, nil,
		30*time.Millisecond, 30*time.Millisecond, 25*time.Millisecond)

	lan.fireSwitchAlert("opus", native, other, "test")
	bc.waitForN(t, 1)
	time.Sleep(120 * time.Millisecond) // both windows long past — the record is dead weight

	lan.fireSwitchAlert("opus", other, native, "test") // its gateAndEnqueue run age-prunes
	bc.waitForN(t, 2)

	hist := historySnapshot()
	if len(hist) != 1 {
		t.Fatalf("history len = %d, want 1 — aged records must be pruned", len(hist))
	}
	if hist[0].from != other || hist[0].to != native {
		t.Fatalf("surviving record = %s->%s, want the newer %s->%s", hist[0].from, hist[0].to, other, native)
	}
}

// The cooldown is LANE-scoped: a critical on fable must not demote a loud opus
// flip (review finding 4).
func TestCritCooldownIsLaneScoped(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, loudWeekly, nil, testCritDefault, testRepeatDefault, testCoalesce)

	lan.fireSwitchAlert("fable", native, other, "test") // critical
	lan.fireSwitchAlert("opus", native, other, "test")  // different lane — must stay critical
	bc.waitForN(t, 2)

	tiers := bc.tiers()
	if tiers[0] != "critical" || tiers[1] != "critical" {
		t.Fatalf("cross-lane tiers = %v, want [critical critical]", tiers)
	}
}

// Suppressed fires don't extend the repeat window: it slides from the last
// ADMITTED fire, so a fast flap keeps paging ~once per window per direction
// instead of starving itself forever (review finding 6).
func TestSuppressedFireDoesNotExtendRepeatWindow(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, quietWeekly, nil,
		testCritDefault, 500*time.Millisecond, 25*time.Millisecond)

	lan.fireSwitchAlert("opus", native, other, "test") // t≈0: admitted
	bc.waitForN(t, 1)

	time.Sleep(200 * time.Millisecond)                 // within the window from t≈0
	lan.fireSwitchAlert("opus", native, other, "test") // suppressed
	time.Sleep(100 * time.Millisecond)

	time.Sleep(400 * time.Millisecond)                 // t≈700: past the window MEASURED FROM t≈0
	lan.fireSwitchAlert("opus", native, other, "test") // admitted
	bc.waitForN(t, 2)
	bc.assertCountStable(t, 2, 250*time.Millisecond)
}

// A switchover to or from a member classAlertQuiet names logs and sends
// no alert; every other one alerts, an image class's included.
func TestClassAlertQuietMember(t *testing.T) {
	bc := newBridgeCapture(t)
	swapAlertTestWorld(t, bc, false, nil, nil, testCritDefault, testRepeatDefault, testCoalesce)
	old := classAlertQuiet
	classAlertQuiet = func(id string) bool { return strings.HasPrefix(id, "away/") }
	t.Cleanup(func() { classAlertQuiet = old })
	lan.fireClassAlert("pics", "img:a", "x/one", "away/two", "test")
	lan.fireClassAlert("pics", "img:a", "away/two", "x/one", "test")
	lan.fireClassAlert("pics", "img:a", "x/one", "y/two", "test")
	bc.waitForN(t, 1)
	bc.assertCountStable(t, 1, 250*time.Millisecond)
}
