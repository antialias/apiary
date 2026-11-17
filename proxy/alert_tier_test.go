package main

// Tests for switchover alert tiering (fireSwitchAlert).
//
// These pin the anti-crying-wolf property the whole monitoring re-tier depends on:
// a routine switchover must NOT reach the DND-bypassing critical channel, because
// the proxy handling it is the system WORKING. Only the LAST HOP — switching to an
// account that is itself nearly spent — is allowed to page the phone.
//
// With lanes, "nearly spent" is judged by the LANE-EFFECTIVE utilization of the
// destination (its overall weekly OR its scoped model cap, whichever binds): a fable
// move onto an account at overall 41% but fable-cap 84% IS fable's last hop.
//
// If someone "fixes" fireSwitchAlert back to an unconditional delivery=critical,
// TestRoutineSwitchoverIsQuiet fails. That is the point.
//
//	go test ./...

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type bridgeAlert struct {
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
}

// captureBridgeLane stands in for the alert receiver: fire one switch alert on
// `lane` with the given brain state, wait for the coalesced flush, and return the
// single alert that was POSTed.
func captureBridgeLane(t *testing.T, lane string, weekly map[string]float64,
	lanes map[string]lanePref, loud bool, from, to string) bridgeAlert {
	t.Helper()

	got := make(chan []byte, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- b
		w.WriteHeader(200)
	}))
	defer srv.Close()

	// save/restore the globals fireSwitchAlert reads
	oldLoud, oldPct, oldCoalesce := alwaysLoud, loudWeeklyPct, alertCoalesce
	oldCrit, oldRepeat := alertCritCooldown, alertRepeatWindow
	oldBridge := swapBridgeURL(srv.URL)
	// Fresh alert history per capture — load-bearing: tests in this file fire the
	// same (lane, from→to) critical pair repeatedly and would gate-2-collide.
	oldHistory := swapAlertHistory(nil)
	defer func() {
		swapBridgeURL(oldBridge)
		swapAlertHistory(oldHistory)
		alwaysLoud, loudWeeklyPct, alertCoalesce = oldLoud, oldPct, oldCoalesce
		alertCritCooldown, alertRepeatWindow = oldCrit, oldRepeat
	}()
	alwaysLoud, loudWeeklyPct, alertCoalesce = loud, 80.0, 5*time.Millisecond
	// Pin the gate windows to defaults so a prior test's shrunk values can't bleed.
	alertCritCooldown, alertRepeatWindow = 6*time.Hour, time.Hour

	lan.pref.mu.Lock()
	oldWeekly, oldLanes := lan.pref.weekly, lan.pref.lanes
	lan.pref.weekly, lan.pref.lanes = weekly, lanes
	lan.pref.mu.Unlock()
	defer func() {
		lan.pref.mu.Lock()
		lan.pref.weekly, lan.pref.lanes = oldWeekly, oldLanes
		lan.pref.mu.Unlock()
	}()

	lan.fireSwitchAlert(lane, from, to, "test")

	var payload struct {
		Alerts []bridgeAlert `json:"alerts"`
	}
	select {
	case b := <-got:
		if err := json.Unmarshal(b, &payload); err != nil {
			t.Fatalf("bridge got unparseable payload: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fireSwitchAlert did not POST to the bridge (coalesced flush never fired)")
	}
	if len(payload.Alerts) != 1 {
		t.Fatalf("want exactly 1 alert, got %d", len(payload.Alerts))
	}
	return payload.Alerts[0]
}

// captureBridge is the legacy-shaped helper: default lane, no lane data — exactly
// the single-lane world the original five tests were written in.
func captureBridge(t *testing.T, weekly map[string]float64, loud bool, from, to string) (string, string) {
	t.Helper()
	a := captureBridgeLane(t, "default", weekly, nil, loud, from, to)
	return a.Labels["delivery"], a.Annotations["title"]
}

// Two account emails used as opaque labels. Neither is privileged — the
// "native/home" concept is gone; the proxy always injects the chosen account's token.
// (The names are kept only to limit churn across the test files.)
const (
	native = "user@example.com"
	other  = "user+second@example.com"
)

// The common case: we routed around a hot account onto a cool one. Nothing
// is required of the user. This must NOT bypass Do Not Disturb.
func TestRoutineSwitchoverIsQuiet(t *testing.T) {
	delivery, title := captureBridge(t,
		map[string]float64{native: 96, other: 41}, false, native, other)
	if delivery != "telegram" {
		t.Errorf("routine switchover to a COOL account (41%%) must be telegram, got %q."+
			"\nThis is the crying-wolf regression: it would page the phone ~2x/day to say"+
			"\n\"handled it, nothing to do\", training the user to ignore the real page.", delivery)
	}
	if title != "🔀 Claude account switchover" {
		t.Errorf("unexpected title %q", title)
	}
}

// The last hop: the account we just landed on is nearly spent too, so there is
// nowhere left to switch. THIS is worth waking someone up for.
func TestLastHopSwitchoverIsLoud(t *testing.T) {
	delivery, title := captureBridge(t,
		map[string]float64{native: 96, other: 84}, false, native, other)
	if delivery != "critical" {
		t.Errorf("switchover to an account already at 84%% weekly is the LAST HOP and"+
			" must be critical, got %q", delivery)
	}
	if title != "🚨 Claude switched to its LAST account" {
		t.Errorf("unexpected title %q", title)
	}
}

// >=, not >. An account exactly at the threshold is the last hop.
func TestThresholdIsInclusive(t *testing.T) {
	if delivery, _ := captureBridge(t,
		map[string]float64{native: 96, other: 80}, false, native, other); delivery != "critical" {
		t.Errorf("exactly at the threshold (80%%) must be loud, got %q", delivery)
	}
	if delivery, _ := captureBridge(t,
		map[string]float64{native: 96, other: 79}, false, native, other); delivery != "telegram" {
		t.Errorf("just below the threshold (79%%) must be quiet, got %q", delivery)
	}
}

// If the brain is unreachable we don't know how hot the destination is. Fail QUIET:
// the real emergency page is an all-accounts-exhausted monitoring alert, which
// does not depend on this process, so staying quiet here cannot lose the emergency —
// whereas guessing "loud" would page on every brain hiccup.
func TestUnknownDestinationFailsQuiet(t *testing.T) {
	if delivery, _ := captureBridge(t, nil, false, native, other); delivery != "telegram" {
		t.Errorf("unknown destination usage (brain down) must fail quiet, got %q", delivery)
	}
	// account present in the map but absent from weekly => also unknown
	if delivery, _ := captureBridge(t,
		map[string]float64{native: 96}, false, native, other); delivery != "telegram" {
		t.Errorf("destination missing from the weekly map must fail quiet, got %q", delivery)
	}
}

// The escape hatch: SWITCH_ALERT_ALWAYS_LOUD=1 restores the original
// "every switchover pages the phone" behaviour with no rebuild.
func TestAlwaysLoudEscapeHatch(t *testing.T) {
	if delivery, _ := captureBridge(t,
		map[string]float64{native: 96, other: 41}, true, native, other); delivery != "critical" {
		t.Errorf("SWITCH_ALERT_ALWAYS_LOUD must force critical even on a cool destination, got %q", delivery)
	}
}

// ---- lane-aware tiering ----

// The scenario that motivated lane tiering: overall weekly on the destination looks
// cool (41%) but its FABLE scoped cap is 84% — for fable traffic this IS the last
// hop and must page; for haiku traffic (no binding cap) the same move is routine.
func TestLaneEffectiveDrivesLoudness(t *testing.T) {
	weekly := map[string]float64{native: 96, other: 41}
	lanes := map[string]lanePref{
		"fable": {Account: other, Effective: map[string]float64{native: 97, other: 84}},
		"haiku": {Account: other, Effective: map[string]float64{native: 96, other: 41}},
	}
	a := captureBridgeLane(t, "fable", weekly, lanes, false, native, other)
	if a.Labels["delivery"] != "critical" {
		t.Errorf("fable-effective 84%% on the destination is fable's LAST HOP — want critical, got %q."+
			"\nTiering by overall weekly (41%%) here is the bug lanes exist to fix.", a.Labels["delivery"])
	}
	b := captureBridgeLane(t, "haiku", weekly, lanes, false, native, other)
	if b.Labels["delivery"] != "telegram" {
		t.Errorf("haiku-effective 41%% is a routine move — want telegram, got %q", b.Labels["delivery"])
	}
}

// Every switch alert must carry its lane so the bridge message says WHICH traffic moved.
func TestLaneLabelPresent(t *testing.T) {
	lanes := map[string]lanePref{
		"fable": {Account: other, Effective: map[string]float64{other: 10}},
	}
	a := captureBridgeLane(t, "fable", map[string]float64{native: 96, other: 41}, lanes, false, native, other)
	if a.Labels["lane"] != "fable" {
		t.Errorf("alert must carry lane=fable, got %q", a.Labels["lane"])
	}
	if a.Labels["alertname"] != "ClaudeAccountSwitchover" {
		t.Errorf("alertname is the bridge contract and must not change, got %q", a.Labels["alertname"])
	}
}

// Fallback chain: lane effective -> overall weekly -> unknown (quiet).
func TestLaneTierFallbackChain(t *testing.T) {
	// lanes present but this lane has no effective for the destination -> weekly (84 => loud)
	lanes := map[string]lanePref{"fable": {Account: other, Effective: map[string]float64{native: 97}}}
	a := captureBridgeLane(t, "fable", map[string]float64{native: 96, other: 84}, lanes, false, native, other)
	if a.Labels["delivery"] != "critical" {
		t.Errorf("no lane-effective for destination must fall back to weekly (84%% => loud), got %q",
			a.Labels["delivery"])
	}
	// neither lane-effective nor weekly -> fail quiet
	b := captureBridgeLane(t, "fable", nil, nil, false, native, other)
	if b.Labels["delivery"] != "telegram" {
		t.Errorf("nothing known about the destination must fail quiet, got %q", b.Labels["delivery"])
	}
}

// Simultaneous multi-lane flips (the shared overall-weekly component crossing the
// threshold moves every lane at once) must coalesce into ONE bridge POST, with a
// critical entry FIRST when the batch is mixed — the bridge takes the push title
// from alerts[0].
func TestMultiLaneFlipCoalescesToOnePost(t *testing.T) {
	posts := make(chan []byte, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		posts <- b
		w.WriteHeader(200)
	}))
	defer srv.Close()

	oldLoud, oldPct, oldCoalesce := alwaysLoud, loudWeeklyPct, alertCoalesce
	oldBridge := swapBridgeURL(srv.URL)
	oldHistory := swapAlertHistory(nil) // fresh gate history: 3 first-fire flips expected
	defer func() {
		swapBridgeURL(oldBridge)
		swapAlertHistory(oldHistory)
		alwaysLoud, loudWeeklyPct, alertCoalesce = oldLoud, oldPct, oldCoalesce
	}()
	alwaysLoud, loudWeeklyPct, alertCoalesce = false, 80.0, 50*time.Millisecond

	weekly := map[string]float64{native: 96, other: 41}
	lanes := map[string]lanePref{
		"fable": {Account: other, Effective: map[string]float64{other: 84}}, // loud
		"haiku": {Account: other, Effective: map[string]float64{other: 41}}, // quiet
		"opus":  {Account: other, Effective: map[string]float64{other: 41}}, // quiet
	}
	lan.pref.mu.Lock()
	oldWeekly, oldLanes := lan.pref.weekly, lan.pref.lanes
	lan.pref.weekly, lan.pref.lanes = weekly, lanes
	lan.pref.mu.Unlock()
	defer func() {
		lan.pref.mu.Lock()
		lan.pref.weekly, lan.pref.lanes = oldWeekly, oldLanes
		lan.pref.mu.Unlock()
	}()

	lan.fireSwitchAlert("haiku", native, other, "test")
	lan.fireSwitchAlert("fable", native, other, "test")
	lan.fireSwitchAlert("opus", native, other, "test")

	var payload struct {
		Alerts []bridgeAlert `json:"alerts"`
	}
	select {
	case b := <-posts:
		if err := json.Unmarshal(b, &payload); err != nil {
			t.Fatalf("unparseable payload: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no bridge POST")
	}
	if len(payload.Alerts) != 3 {
		t.Fatalf("3 lane flips must coalesce into ONE post with 3 alerts, got %d", len(payload.Alerts))
	}
	if payload.Alerts[0].Labels["delivery"] != "critical" || payload.Alerts[0].Labels["lane"] != "fable" {
		t.Errorf("critical entry must lead the batch (bridge titles from alerts[0]), got lane=%q delivery=%q",
			payload.Alerts[0].Labels["lane"], payload.Alerts[0].Labels["delivery"])
	}
	select {
	case <-posts:
		t.Fatal("second bridge POST arrived — flips did not coalesce")
	case <-time.After(200 * time.Millisecond):
	}
}
