package main

// e2e tests for class routing (#27) through the REAL handler stack: a request for
// the synthetic picker id claude-frontier is bound per session by the fake brain
// (/class-bind), actuated onto an Anthropic lane member or a direct provider
// member, and reactively failed over on 429 — same-lane account swap first, then
// ONE cross-provider member hop. The member fixture:
// anthropic P1 (the fake brain's "fable" lane stands in for opus), gpt P2
// usable=false (no poller until #31), kimi P3 usable=true. Kimi is mounted under
// /coding to exercise the cross-member base-path re-join.

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// classModelsJSON deliberately still carries a class entry: since #93 class
// rows derive from the brain's /preferred and a file entry is logged+IGNORED
// (grace for a not-yet-migrated models.json). Keeping it here proves the whole
// class suite runs on DERIVED rows with the legacy entry present but inert —
// and TestE2EClassAdvertisedWithMembers pins that it never double-advertises.
const classModelsJSON = `[
  {"name":"fable-5","display":"Claude Fable 5","type":"anthropic","context_window":200000},
  {"name":"frontier","display":"Frontier (auto)","type":"class","class":"frontier","context_window":200000}
]`

func classFixtureMembers() []map[string]any {
	return []map[string]any{
		{"id": "lane:fable", "priority": 1, "kind": "anthropic", "lane": "fable",
			"model": "claude-fable-5", "context_window": 200000, "anthropic_token_ratio": 1.0,
			"effective": 70.0, "usable": true, "status": "ok"},
		{"id": "gpt/gpt-5.6", "priority": 2, "kind": "provider", "provider": "gpt",
			"model": "gpt-5.6", "context_window": 400000, "anthropic_token_ratio": 0.6,
			"effective": nil, "usable": false, "status": "no_target"},
		{"id": "kimi/kimi-k3", "priority": 3, "kind": "provider", "provider": "kimi",
			"model": "kimi-k3", "context_window": 256000, "anthropic_token_ratio": 0.61,
			"effective": 12.0, "usable": true, "status": "ok"},
	}
}

// newE2EClass = hosted providers (kimi under /coding, gpt at root) + a
// class-serving brain + the production-shaped models file.
func newE2EClass(t *testing.T) (*e2eHosted, func()) {
	t.Helper()
	h, restore := newE2EHosted(t, "/coding")
	h.brainMu.Lock()
	h.classMembers = classFixtureMembers()
	h.brainMu.Unlock()
	withModelsFile(t, classModelsJSON)
	fetchPreferredSync()
	rebuildProxy(h.e2eEnv)
	return h, restore
}

// newE2EClassOn is newE2EClass with every new binding landing on one provider
// member (serveMember). It is the vehicle for tests whose subject is a
// provider wire (usage scaling, codex translation, taps, provider refusals)
// rather than the ladder: since quota-model C2 a class member is the only way a
// request reaches kimi or gpt.
func newE2EClassOn(t *testing.T, memberID string, ratio float64) (*e2eHosted, func()) {
	t.Helper()
	h, restore := newE2EClass(t)
	h.serveMember(t, memberID, ratio)
	return h, restore
}

// serveMember makes a fixture member usable at the given brain-served ratio
// and points every NEW binding at it; sessions already bound keep their member.
func (h *e2eHosted) serveMember(t *testing.T, memberID string, ratio float64) {
	t.Helper()
	h.brainMu.Lock()
	found := false
	for _, m := range h.classMembers {
		if m["id"] == memberID {
			m["anthropic_token_ratio"], m["usable"], m["status"], m["effective"] = ratio, true, "ok", 50.0
			found = true
		}
	}
	h.brainMu.Unlock()
	if !found {
		t.Fatalf("no fixture member %q", memberID)
	}
	h.bindTo.Store(memberID)
	fetchPreferredSync()
}

// classAlerts drains one coalesced bridge flush and returns its alerts.
func classAlerts(t *testing.T, env *e2eEnv) []bridgeAlert {
	t.Helper()
	select {
	case b := <-env.bridge:
		var p struct {
			Alerts []bridgeAlert `json:"alerts"`
		}
		if err := json.Unmarshal(b, &p); err != nil {
			t.Fatalf("bridge payload not JSON: %v", err)
		}
		return p.Alerts
	case <-time.After(3 * time.Second):
		t.Fatal("no bridge flush arrived")
		return nil
	}
}

// classAlertsN collects alerts until n of them have arrived, across however many
// bridge flushes it takes.
//
// The coalescer batches on a timer (shrunk to milliseconds in these tests), so two
// alerts raised microseconds apart USUALLY share one flush — but "usually" is a
// scheduler bet, and on a loaded CI agent the timer fires between them and splits
// the batch. What the tests actually mean to assert is that both alerts reach the
// bridge, not that a race went their way.
func classAlertsN(t *testing.T, env *e2eEnv, n int) []bridgeAlert {
	t.Helper()
	var out []bridgeAlert
	deadline := time.After(3 * time.Second)
	for len(out) < n {
		select {
		case b := <-env.bridge:
			var p struct {
				Alerts []bridgeAlert `json:"alerts"`
			}
			if err := json.Unmarshal(b, &p); err != nil {
				t.Fatalf("bridge payload not JSON: %v", err)
			}
			out = append(out, p.Alerts...)
		case <-deadline:
			t.Fatalf("only %d of %d alert(s) reached the bridge: %v", len(out), n, out)
		}
	}
	return out
}

func noMoreAlerts(t *testing.T, env *e2eEnv) {
	t.Helper()
	select {
	case b := <-env.bridge:
		t.Fatalf("unexpected extra alert flush: %s", b)
	case <-time.After(150 * time.Millisecond):
	}
}

// (1) The class row — derived from /preferred (#93) — is advertised alongside
// the anthropic rows, and exactly once: the class entry still in classModelsJSON
// is grace-ignored, never a second claude-frontier. Its provider members add no
// row of their own (quota-model C2): they are reachable only through the class.
func TestE2EClassAdvertisedWithMembers(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()

	got := map[string]int{}
	for _, id := range getModels(t, h.proxy.URL) {
		got[id]++
	}
	if len(got) != 2 || got["claude-fable-5"] != 1 {
		t.Fatalf("advertised set = %v, want exactly claude-frontier and claude-fable-5", got)
	}
	if got["claude-frontier"] != 1 {
		t.Fatalf("claude-frontier advertised %d times, want exactly 1 (file class entry must be inert)", got["claude-frontier"])
	}
}

// (2) An anthropic-member binding actuates lane mechanics: the session reaches
// the brain, the model is rewritten to the member's REAL Anthropic id, and the
// lane's account bearer is INJECTED (never the client's).
func TestE2EClassAnthropicMemberRouting(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-anthropic-1")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	hit := h.hit(t) // Anthropic upstream
	if hit.auth != "Bearer "+e2eToken(other) {
		t.Fatalf("auth = %q, want the fable lane's account bearer (tok-%s)", hit.auth, other)
	}
	if hit.model != "claude-fable-5" {
		t.Fatalf("model = %q, want claude-frontier rewritten to the member's real id", hit.model)
	}
	if n := h.bindCalls.Load(); n != 1 {
		t.Fatalf("bind calls = %d, want exactly 1", n)
	}
	h.brainMu.Lock()
	bound := h.binds["sess-anthropic-1"]
	h.brainMu.Unlock()
	if bound != "lane:fable" {
		t.Fatalf("brain bound %q, want the P1 member lane:fable", bound)
	}
}

// (3) A provider-member binding actuates direct mechanics: provider key injected,
// model rewritten, upstream base path joined (kimi serves under /coding).
func TestE2EClassProviderMemberRouting(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	h.bindTo.Store("kimi/kimi-k3")

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-provider-1")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "hosted ok") {
		t.Fatalf("response did not come from the hosted provider: %s", b)
	}
	hit := h.hostedHit(t)
	if hit.auth != "Bearer kimi-secret" {
		t.Fatalf("auth = %q, want the injected kimi key", hit.auth)
	}
	if hit.model != "kimi-k3" {
		t.Fatalf("model = %q, want kimi-k3", hit.model)
	}
	if hit.path != "/coding/v1/messages" {
		t.Fatalf("path = %q, want the provider base path joined", hit.path)
	}
	h.noAnthropicLeak(t)
}

// (4) Bindings are sticky per session and positively cached: a repeat request
// inside bindFresh never re-contacts the brain; a second session gets its own bind.
func TestE2EClassSessionStickyAndCached(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()

	h.postSess(t, "claude-frontier", "CLIENT", "sess-sticky-a").Body.Close()
	h.hit(t)
	h.postSess(t, "claude-frontier", "CLIENT", "sess-sticky-a").Body.Close()
	h.hit(t)
	if n := h.bindCalls.Load(); n != 1 {
		t.Fatalf("bind calls after 2 same-session requests = %d, want 1 (fresh cache)", n)
	}
	h.postSess(t, "claude-frontier", "CLIENT", "sess-sticky-b").Body.Close()
	h.hit(t)
	if n := h.bindCalls.Load(); n != 2 {
		t.Fatalf("bind calls after a second session = %d, want 2", n)
	}
	h.brainMu.Lock()
	a, b := h.binds["sess-sticky-a"], h.binds["sess-sticky-b"]
	h.brainMu.Unlock()
	if a != "lane:fable" || b != "lane:fable" {
		t.Fatalf("binds = a:%q b:%q, want both on the P1 member", a, b)
	}
}

// (5) A brain-side re-point becomes visible at the next refresh: the session
// actuates the NEW member and exactly one class alert fires.
func TestE2EClassRePointFiresOneAlert(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	oldFresh := bindFresh
	bindFresh = 0 // every request refreshes: the re-point is observed immediately
	defer func() { bindFresh = oldFresh }()

	h.postSess(t, "claude-frontier", "CLIENT", "sess-repoint").Body.Close()
	h.hit(t) // on lane:fable

	h.brainMu.Lock()
	h.binds["sess-repoint"] = "kimi/kimi-k3" // brain moved the session (pin / forced-down)
	h.brainMu.Unlock()

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-repoint")
	resp.Body.Close()
	hit := h.hostedHit(t)
	if hit.auth != "Bearer kimi-secret" || hit.model != "kimi-k3" {
		t.Fatalf("re-pointed request must actuate kimi, got %+v", hit)
	}

	alerts := classAlerts(t, h.e2eEnv)
	if len(alerts) != 1 {
		t.Fatalf("want exactly 1 alert, got %d", len(alerts))
	}
	a := alerts[0]
	if a.Labels["alertname"] != "ClaudeClassSwitchover" ||
		a.Labels["lane"] != "class:frontier" ||
		a.Labels["from_account"] != "lane:fable" ||
		a.Labels["to_account"] != "kimi/kimi-k3" ||
		a.Labels["delivery"] != "telegram" {
		t.Fatalf("alert labels wrong: %v", a.Labels)
	}
	noMoreAlerts(t, h.e2eEnv) // a third request would re-observe the same member: no storm
}

// (6) A provider-member 429 crosses mid-request to the next candidate (the
// anthropic member: new model, new upstream, new auth), cools the member, and
// subsequent requests detour around it WITHOUT re-contacting the brain.
func TestE2EClass429CrossesMemberMidRequest(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	h.bindTo.Store("kimi/kimi-k3")
	h.deny.Store(true) // hosted upstream 429s; Anthropic stays healthy

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-cross")
	defer resp.Body.Close()

	first := h.hostedHit(t) // attempt 1: the bound kimi member, 429'd
	if first.auth != "Bearer kimi-secret" || first.model != "kimi-k3" {
		t.Fatalf("attempt 1 = %+v, want the bound kimi member", first)
	}
	second := h.hit(t) // the cross: Anthropic upstream, fable lane's account
	if second.auth != "Bearer "+e2eToken(other) || second.model != "claude-fable-5" {
		t.Fatalf("cross = %+v, want the anthropic member's model on its lane bearer", second)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("client must see the crossed 200, got %d", resp.StatusCode)
	}
	if !throttledOn("kimi") {
		t.Fatal("the 429'd member's account must be throttled (it said busy)")
	}
	// a provider 429 must NOT touch the Anthropic accounts
	if a, b := recordsOn(native), recordsOn(other); len(a)+len(b) != 0 {
		t.Fatalf("provider-member 429 recorded on Anthropic accounts: %v %v", a, b)
	}

	alerts := classAlerts(t, h.e2eEnv)
	if len(alerts) != 1 || alerts[0].Labels["alertname"] != "ClaudeClassSwitchover" ||
		alerts[0].Labels["to_account"] != "lane:fable" {
		t.Fatalf("want one class cross alert to lane:fable, got %v", alerts)
	}

	// next request: bind cache still says kimi (fresh), but the cooldown detours
	// it straight to the anthropic member — brain-free, no kimi attempt
	calls := h.bindCalls.Load()
	resp2 := h.postSess(t, "claude-frontier", "CLIENT", "sess-cross")
	resp2.Body.Close()
	if hit := h.hit(t); hit.model != "claude-fable-5" {
		t.Fatalf("detoured request = %+v, want the anthropic member", hit)
	}
	select {
	case hit := <-h.hosted:
		t.Fatalf("detoured request still tried the cooling member: %+v", hit)
	default:
	}
	if got := h.bindCalls.Load(); got != calls {
		t.Fatalf("detour contacted the brain (%d -> %d bind calls); it must be local", calls, got)
	}
}

// (7)+(8) Both Anthropic accounts 429 → the bounded 3-attempt ladder: bound
// member on account A, same-lane swap to account B, then cross to kimi. Both
// account cooldowns AND the member cooldown are recorded; gpt (usable=false)
// never receives traffic.
func TestE2EClassBothAccounts429CrossToProvider(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	h.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"You have exceeded your weekly usage limit."}}`)
	h.deny429.Store("*") // every Anthropic account 429s; hosted stays healthy

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-ladder")
	defer resp.Body.Close()

	first := h.hit(t) // attempt 1: fable's preferred account
	if first.auth != "Bearer "+e2eToken(other) || first.model != "claude-fable-5" {
		t.Fatalf("attempt 1 = %+v", first)
	}
	second := h.hit(t) // attempt 2: same member, swapped account
	if second.auth != "Bearer "+e2eToken(native) || second.model != "claude-fable-5" {
		t.Fatalf("attempt 2 must be the same member on the OTHER account, got %+v", second)
	}
	third := h.hostedHit(t) // attempt 3: crossed to kimi
	if third.auth != "Bearer kimi-secret" || third.model != "kimi-k3" || third.path != "/coding/v1/messages" {
		t.Fatalf("attempt 3 = %+v, want kimi with its key, model, and base path", third)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("client must see kimi's 200, got %d", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "hosted ok") {
		t.Fatalf("final body did not come from kimi: %s", b)
	}

	// No unified headers: each 429 is a throttle (§2.6), so both accounts are
	// deranked and neither check closes.
	if !throttledOn(other) || !throttledOn(native) {
		t.Fatal("both accounts must be throttled after the generic 429s")
	}
	if laneNo(other, "fable") || laneNo(native, "fable") {
		t.Fatal("a throttle must not close the fable route")
	}
	// gpt (usable=false in the fixture) must never have been tried
	h.noCodexLeak(t)
	select {
	case hit := <-h.hosted:
		t.Fatalf("unexpected extra hosted hit: %+v", hit)
	default:
	}

	// two switch events: the account swap (lane alert) + the member cross (class
	// alert) — usually coalesced into one flush, but never assert the timer race
	alerts := classAlertsN(t, h.e2eEnv, 2)
	var sawLane, sawClass bool
	for _, a := range alerts {
		switch a.Labels["alertname"] {
		case "ClaudeClassSwitchover":
			sawClass = a.Labels["to_account"] == "kimi/kimi-k3"
		default:
			sawLane = sawLane || a.Labels["lane"] == "fable"
		}
	}
	if !sawLane || !sawClass {
		t.Fatalf("want a fable account-swap alert AND a class cross alert, got %v", alerts)
	}
}

// (9) Brain outage with a cached bind: the session rides the cached member at any
// age — STALE, visible on /_switch/status — and still reaches upstream.
func TestE2EClassBrainDownRidesCachedBindStale(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()

	h.postSess(t, "claude-frontier", "CLIENT", "sess-stale").Body.Close()
	h.hit(t) // bind + token now cached

	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close() // port now refuses: a transport error, not a refusal
	oldBrain := brainBase
	brainBase = deadURL
	defer func() { brainBase = oldBrain }()
	oldFresh := bindFresh
	bindFresh = 0 // force a refresh attempt on every request
	defer func() { bindFresh = oldFresh }()

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-stale")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200 riding the cached bind", resp.StatusCode)
	}
	hit := h.hit(t)
	if hit.auth != "Bearer "+e2eToken(other) || hit.model != "claude-fable-5" {
		t.Fatalf("stale ride = %+v, want the cached member on its cached token", hit)
	}

	var s struct {
		StateByLane   map[string]string         `json:"state_by_lane"`
		ClassBindings map[string]map[string]any `json:"class_bindings"`
	}
	if err := json.Unmarshal(statusMine(t, h.proxy.URL), &s); err != nil {
		t.Fatal(err)
	}
	if s.StateByLane["class:frontier"] != "STALE" {
		t.Fatalf("state_by_lane[class:frontier] = %q, want STALE", s.StateByLane["class:frontier"])
	}
	binding, _ := s.ClassBindings["frontier"]["sess-sta"].(map[string]any)
	if binding == nil || binding["member"] != "lane:fable" {
		t.Fatalf("class_bindings must expose the ridden bind, got %v", s.ClassBindings)
	}
}

// (10) Brain outage with NO cached bind: typed 503, nothing leaks upstream.
func TestE2EClassBrainDownColdTyped503(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()

	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()
	oldBrain := brainBase
	brainBase = deadURL
	defer func() { brainBase = oldBrain }()

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-cold")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "brain unreachable and no cached class binding") {
		t.Fatalf("503 body lacks the cold-cache reason: %s", b)
	}
	h.noAnthropicLeak(t)
	select {
	case hit := <-h.hosted:
		t.Fatalf("cold class request leaked to a provider: %+v", hit)
	default:
	}
}

// (11) The brain's 409 class_unavailable is a decision, not an outage: it
// never rides the cached bind. Its body is the reply, rendered by the
// client's wire (§2.8); the 404 unknown class stays the typed 503.
func TestE2EClassBrainRefusalNoCacheFallback(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()

	// warm a cache entry for this session, then make the brain refuse
	h.postSess(t, "claude-frontier", "CLIENT", "sess-refuse").Body.Close()
	h.hit(t)
	oldFresh := bindFresh
	bindFresh = 0
	defer func() { bindFresh = oldFresh }()

	// The brain's own body: the golden §2.8 example, as /class-bind sends it.
	d := loadGolden(t)
	var brain map[string]any
	_ = json.Unmarshal(d.Unavailable[0].Want, &brain)
	brain["error"], brain["class"] = "class_unavailable", "frontier"
	bb, _ := json.Marshal(brain)
	h.bind409.Store(string(bb))
	at := time.Unix(int64(d.Now)+2*86400, 0) // the Fable lane's first reset

	resp := postCC(t, h.e2eEnv, "claude-frontier", "sess-refuse")
	msg := assertHonestAt(t, resp, at, "seven_day", 2*86400)
	for _, want := range []string{"class frontier is unavailable", "c/lane:fable: maxplan spent (Fable)",
		"Options: enable usage credits on maxplan", "add gpt/credits to c/gpt/gpt-6.1-sol's pay",
		"Immediate reset (paid) on gpt, for a fee"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message missing %q: %s", want, msg)
		}
	}
	h.noAnthropicLeak(t)

	resp = h.postSess(t, "claude-frontier", "CLIENT", "sess-refuse")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 529 || resp.Header.Get("Retry-After") != "172800" {
		t.Fatalf("non-Claude-Code status %d Retry-After %q, want 529 172800: %s", resp.StatusCode, resp.Header.Get("Retry-After"), b)
	}
	var env struct {
		Unavailable unavailable `json:"unavailable"`
	}
	if err := json.Unmarshal(b, &env); err != nil || len(env.Unavailable.Members) != 2 || len(env.Unavailable.Options) != 5 {
		t.Fatalf("the brain's body does not ride the reply: %s", b)
	}
	h.noAnthropicLeak(t)

	h.bind409.Store("")
	h.bind404.Store(true)
	resp = h.postSess(t, "claude-frontier", "CLIENT", "sess-refuse-404")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("404-refusal status %d, want 503", resp.StatusCode)
	}
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), "unknown class") {
		t.Fatalf("503 body lacks the unknown-class reason: %s", b)
	}
	h.noAnthropicLeak(t)

	// The brain's 503 is configuration (§2.8 item 6): no member has an
	// account. The C2 brain's "no usable member" 503 is the same answer, so a
	// proxy that lands first reads it the same way. Either is the typed 503
	// with the brain's words, and never the cached bind: sess-refuse still
	// holds one.
	h.bind404.Store(false)
	for _, refusal := range []string{
		`{"error": "no member of class 'frontier' has an account", "class": "frontier", "routes": ["frontier/gpt/gpt-6.1-sol"]}`,
		`{"error": "no usable member in class 'frontier'", "effective": {}, "checks": {}}`,
	} {
		h.bind503.Store(refusal)
		var want map[string]any
		_ = json.Unmarshal([]byte(refusal), &want)
		resp = h.postSess(t, "claude-frontier", "CLIENT", "sess-refuse")
		b, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "" {
			t.Fatalf("brain 503 gave status %d Retry-After %q, want the typed 503 and none: %s",
				resp.StatusCode, resp.Header.Get("Retry-After"), b)
		}
		if !strings.Contains(string(b), "brain refused: "+want["error"].(string)) {
			t.Fatalf("typed 503 lacks the brain's reason %q: %s", want["error"], b)
		}
		h.noAnthropicLeak(t)
		select {
		case hit := <-h.hosted:
			t.Fatalf("a configuration refusal rode the cached bind to a provider: %+v", hit)
		default:
		}
	}
	h.bind503.Store("")

	// A 409 without retryable is malformed: read as false it would be the
	// money reply (402, or OpenAI's x-should-retry: false) for a body that
	// never said so. The typed 503, and never the cached bind.
	delete(brain, "retryable")
	bb, _ = json.Marshal(brain)
	h.bind409.Store(string(bb))
	resp = h.postSess(t, "claude-frontier", "CLIENT", "sess-refuse")
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "" ||
		!strings.Contains(string(b), "brain refused: class_unavailable") {
		t.Fatalf("a 409 without retryable gave %d Retry-After %q, want the typed 503: %s",
			resp.StatusCode, resp.Header.Get("Retry-After"), b)
	}
	h.noAnthropicLeak(t)
	select {
	case hit := <-h.hosted:
		t.Fatalf("a malformed 409 rode the cached bind to a provider: %+v", hit)
	default:
	}
	h.bind409.Store("")
}

// (12) No session header → the "no-session" fallback identity binds normally.
func TestE2EClassNoSessionFallback(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()

	resp := h.post(t, "claude-frontier", "CLIENT") // post sends no session header
	resp.Body.Close()
	if hit := h.hit(t); hit.model != "claude-fable-5" {
		t.Fatalf("no-session request must still route, got %+v", hit)
	}
	h.brainMu.Lock()
	bound := h.binds["no-session"]
	h.brainMu.Unlock()
	if bound != "lane:fable" {
		t.Fatalf("brain must see the no-session fallback id, binds=%v", h.binds)
	}
}

// (13) A class-shaped id the proxy knows NOTHING about (no derived row, no
// tombstone, no last-good state — e.g. the class was created and removed while
// proxy state was lost): typed 503, NEVER the lane path. This inverts the
// pre-#93 documented degraded behavior, where claude-frontier fell to the
// default lane unrewritten and 404'd at live Anthropic on a real request —
// exactly the leak rule 3's generalized refusal closes.
func TestE2EClassUnknownClassShapedIdRefusedNotForwarded(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	resp := env.post(t, "claude-frontier", "CLIENT")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503 — a forgotten class id must fail closed", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "not routable") {
		t.Fatalf("503 body lacks the class-shape refusal reason: %s", b)
	}
	// A refusal the caller cannot fix by waiting must not invite waiting.
	if !strings.Contains(string(b), "Retrying will not help") {
		t.Fatalf("503 body does not say retrying is futile: %s", b)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "" {
		t.Fatalf("permanent refusal carries Retry-After: %q", ra)
	}
	select {
	case leaked := <-env.seen:
		t.Fatalf("unknown class-shaped id leaked to the Anthropic upstream: %+v", leaked)
	default:
	}
}

// (13b) The full tombstone lifecycle (#93 rule 2), through the real stack:
// active (derived from /preferred) → vanished from a fresh /preferred (the key
// present but empty = the brain positively removed it) → the id is DROPPED from
// the advertised set and a stale picker cache's request hits the typed 503,
// never an upstream 404 → re-created on the next /preferred → active again.
func TestE2EClassTombstoneLifecycle(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()

	// active: routes normally
	h.postSess(t, "claude-frontier", "CLIENT", "sess-life").Body.Close()
	if hit := h.hit(t); hit.model != "claude-fable-5" {
		t.Fatalf("active class must route, got %+v", hit)
	}

	// vanished: fresh /preferred without the class → tombstone
	h.classesGone.Store(true)
	fetchPreferredSync()
	for _, id := range getModels(t, h.proxy.URL) {
		if id == "claude-frontier" {
			t.Fatal("a removed class must leave the advertised set")
		}
	}
	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-life-2")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("tombstoned class: status %d, want 503", resp.StatusCode)
	}
	if !strings.Contains(string(b), classRemovedReason) {
		t.Fatalf("tombstone 503 lacks the %q reason: %s", classRemovedReason, b)
	}
	h.noAnthropicLeak(t)
	select {
	case hit := <-h.hosted:
		t.Fatalf("tombstoned class leaked to a provider: %+v", hit)
	default:
	}

	// re-created: the tombstone dissolves and the class routes again
	h.classesGone.Store(false)
	fetchPreferredSync()
	found := false
	for _, id := range getModels(t, h.proxy.URL) {
		found = found || id == "claude-frontier"
	}
	if !found {
		t.Fatal("a re-created class must be advertised again")
	}
	h.postSess(t, "claude-frontier", "CLIENT", "sess-life-3").Body.Close()
	if hit := h.hit(t); hit.model != "claude-fable-5" {
		t.Fatalf("re-created class must route again, got %+v", hit)
	}
}

// (14) Interleaving a provider-bound class session, a lane request, and an
// anthropic-bound class session on ONE proxy instance keeps every contract:
// provider key vs lane bearer vs class actuation, no bleed.
func TestE2EClassInterleavedContractsIntact(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()

	// class session bound to the kimi member
	h.bindTo.Store("kimi/kimi-k3")
	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-mixed-kimi")
	resp.Body.Close()
	h.bindTo.Store("") // later sessions bind the first member again
	hosted := h.hostedHit(t)
	if hosted.auth != "Bearer kimi-secret" || hosted.model != "kimi-k3" || hosted.path != "/coding/v1/messages" {
		t.Fatalf("kimi member contract broke alongside lane routing: %+v", hosted)
	}

	// account-lane pick
	resp = h.post(t, "claude-fable-5", "CLIENT")
	resp.Body.Close()
	lane := h.hit(t)
	if lane.auth != "Bearer "+e2eToken(other) || lane.model != "claude-fable-5" {
		t.Fatalf("fable lane contract broke alongside class routing: %+v", lane)
	}

	// class pick
	resp = h.postSess(t, "claude-frontier", "CLIENT", "sess-mixed")
	resp.Body.Close()
	class := h.hit(t)
	if class.auth != "Bearer "+e2eToken(other) || class.model != "claude-fable-5" {
		t.Fatalf("class routing broke alongside provider/lane traffic: %+v", class)
	}

	// healthy class traffic writes no record
	if n := liveRecords(); n != 0 {
		t.Fatalf("interleaved healthy traffic left records: %d", n)
	}
}

// ---- (15)-(18) the bind reason on the transcript (#86) ----
//
// The brain already says WHY a session sits where it does — the sentence
// fireClassAlert logs into an alert nobody reads next to the transcript.
// These four pin the one property that makes it safe to render as an explanation:
// bind_reason is present exactly when it describes the member that served, and is
// absent — not stale, not empty-string — every other time.

// classBindReasonEnv arms the gate, the hub and a distinctive brain reason, and
// hands back a subscriber. The reason string is deliberately prose with a percent
// sign in it: it is brain-authored text on the wire, not an enum.
func classBindReasonEnv(t *testing.T, h *e2eHosted) (<-chan string, string) {
	t.Helper()
	const reason = "forced down: fable weekly cap at 96%"
	withClientSecret(t, h.e2eEnv, "sekrit")
	h.bindReason.Store(reason)
	_, frames, closeStream := evStream(t, h.e2eEnv, "sekrit", "")
	t.Cleanup(closeStream)
	evHello(t, frames)
	return frames, reason
}

func classBindReasonOf(t *testing.T, d map[string]any, event string) (string, bool) {
	t.Helper()
	v, present := d["bind_reason"]
	if !present {
		return "", false
	}
	s, ok := v.(string)
	if !ok {
		t.Fatalf("%s bind_reason is %T, want a string: %v", event, v, v)
	}
	if s == "" {
		t.Fatalf("%s carries bind_reason:\"\" — omitempty exists so absence is the "+
			"signal; an empty string reads as \"the brain gave no reason\"", event)
	}
	return s, true
}

// (15) The bound member serves: the reason rides BOTH sides of the transcript,
// so a reader joining on conversation_key sees it whichever event they hold.
func TestE2EClassBindReasonRidesBothSides(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	frames, reason := classBindReasonEnv(t, h)

	h.postSess(t, "claude-frontier", "sekrit", "sess-reason").Body.Close()
	h.hit(t)

	turn := respEvData(t, evAwait(t, frames, "turn", 3*time.Second))
	if turn["class"] != "frontier" || turn["member"] != "lane:fable" {
		t.Fatalf("turn routed to class=%v member=%v, want frontier/lane:fable", turn["class"], turn["member"])
	}
	if got, ok := classBindReasonOf(t, turn, "turn"); !ok || got != reason {
		t.Fatalf("turn bind_reason = %q (present=%v), want the brain's %q", got, ok, reason)
	}
	for _, ev := range []string{"turn_started", "assistant_message"} {
		d := respEvData(t, evAwait(t, frames, ev, 3*time.Second))
		if got, ok := classBindReasonOf(t, d, ev); !ok || got != reason {
			t.Fatalf("%s bind_reason = %q (present=%v), want %q", ev, got, ok, reason)
		}
	}
}

// (16) A detour carries NO reason. The brain bound kimi and said why; a local
// record means something else serves, and the binding's reason does not explain
// that choice — the route tag's "(detour from ...)" does.
func TestE2EClassBindReasonAbsentOnADetour(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	frames, _ := classBindReasonEnv(t, h)
	h.bindTo.Store("kimi/kimi-k3")
	seedSpent("kimi", "five_hour", 403, "5-hour usage limit", time.Time{})

	h.postSess(t, "claude-frontier", "sekrit", "sess-detour").Body.Close()
	h.hit(t) // the anthropic candidate, not the bound member

	turn := respEvData(t, evAwait(t, frames, "turn", 3*time.Second))
	if turn["member"] != "lane:fable" {
		t.Fatalf("detour actuated member=%v, want the anthropic candidate", turn["member"])
	}
	if got, ok := classBindReasonOf(t, turn, "turn"); ok {
		t.Fatalf("detoured turn carries bind_reason %q — it explains the BOUND member, "+
			"which is not the one that served", got)
	}
	for _, ev := range []string{"turn_started", "assistant_message"} {
		d := respEvData(t, evAwait(t, frames, ev, 3*time.Second))
		if got, ok := classBindReasonOf(t, d, ev); ok {
			t.Fatalf("detoured %s carries bind_reason %q", ev, got)
		}
	}
}

// (17) A mid-request cross invalidates it on the RESPONSE side only. The request
// event keeps it because that side records the route as ROUTED — its member field
// says kimi too, and dropping one without the other would leave a turn whose
// member and reason disagree. The response side reports what actually served, and
// there the reason would name a member that never answered.
func TestE2EClassBindReasonDroppedByAMidRequestCross(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	frames, reason := classBindReasonEnv(t, h)
	h.bindTo.Store("kimi/kimi-k3")
	h.deny.Store(true) // the bound member 429s; the anthropic candidate answers

	resp := h.postSess(t, "claude-frontier", "sekrit", "sess-cross-reason")
	defer resp.Body.Close()
	h.hostedHit(t)
	h.hit(t)
	if resp.StatusCode != 200 {
		t.Fatalf("client must see the crossed 200, got %d", resp.StatusCode)
	}

	turn := respEvData(t, evAwait(t, frames, "turn", 3*time.Second))
	if turn["member"] != "kimi/kimi-k3" {
		t.Fatalf("request turn member=%v, want the bound member it was routed onto", turn["member"])
	}
	if got, ok := classBindReasonOf(t, turn, "turn"); !ok || got != reason {
		t.Fatalf("request turn bind_reason = %q (present=%v), want %q", got, ok, reason)
	}
	for _, ev := range []string{"turn_started", "assistant_message"} {
		d := respEvData(t, evAwait(t, frames, ev, 3*time.Second))
		if d["member"] != "lane:fable" {
			t.Fatalf("%s member=%v, want the crossed-to member", ev, d["member"])
		}
		if got, ok := classBindReasonOf(t, d, ev); ok {
			t.Fatalf("%s kept bind_reason %q across a member cross", ev, got)
		}
	}
}

// (18) A same-member ACCOUNT swap is not a cross. The ladder's first rung swaps
// Anthropic accounts under the same member, and the binding — which is about the
// member — is untouched by it.
func TestE2EClassBindReasonSurvivesASameMemberAccountSwap(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	frames, reason := classBindReasonEnv(t, h)
	h.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"limit"}}`)
	h.deny429.Store(e2eToken(other)) // the fable lane's incumbent 429s; the other account serves

	resp := h.postSess(t, "claude-frontier", "sekrit", "sess-swap-reason")
	defer resp.Body.Close()
	if first := h.hit(t); first.auth != "Bearer "+e2eToken(other) {
		t.Fatalf("attempt 1 = %+v, want the lane incumbent", first)
	}
	if second := h.hit(t); second.auth != "Bearer "+e2eToken(native) {
		t.Fatalf("swap = %+v, want the other account on the SAME member", second)
	}

	turn := respEvData(t, evAwait(t, frames, "turn", 3*time.Second))
	if got, ok := classBindReasonOf(t, turn, "turn"); !ok || got != reason {
		t.Fatalf("turn bind_reason = %q (present=%v), want %q", got, ok, reason)
	}
	for _, ev := range []string{"turn_started", "assistant_message"} {
		d := respEvData(t, evAwait(t, frames, ev, 3*time.Second))
		if d["member"] != "lane:fable" {
			t.Fatalf("%s member=%v, want the unchanged member", ev, d["member"])
		}
		if got, ok := classBindReasonOf(t, d, ev); !ok || got != reason {
			t.Fatalf("%s dropped bind_reason on an account swap: %q (present=%v)", ev, got, ok)
		}
	}
}

// ---- the cross must not spend itself on a brain-exhausted rung ----

// helpdeskShape rebuilds the class fixture in the helpdesk order — gpt P1,
// kimi P2, anthropic P3 — with gpt usable (auth+config healthy) at the given
// effective, and binds new sessions to kimi. This is the incident shape: kimi
// took a transient "engine overloaded" 429, the ONE cross went to gpt because
// `usable` says nothing about capacity, gpt was at 100%, and the client saw the
// 429 while the anthropic rung sat at 34%.
func helpdeskShape(h *e2eHosted, gptEffective float64) {
	h.brainMu.Lock()
	gpt, kimi, anth := h.classMembers[1], h.classMembers[2], h.classMembers[0]
	gpt["usable"], gpt["status"], gpt["effective"], gpt["priority"] = true, "ok", gptEffective, 1
	kimi["priority"] = 2
	anth["priority"] = 3
	h.classMembers = []map[string]any{gpt, kimi, anth}
	h.brainMu.Unlock()
	h.bindTo.Store("kimi/kimi-k3")
	fetchPreferredSync()
}

// (A) The incident, replayed against the fix: gpt at 100% is skipped, the cross
// lands on the anthropic member, gpt never sees a byte and is NOT cooled (it was
// never tried — a cooldown would be a lie about what happened).
func TestE2EClass429CrossSkipsBrainExhaustedMember(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	helpdeskShape(h, 100.0)
	h.deny.Store(true) // kimi 429s; codex would serve, Anthropic serves

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-incident")
	defer resp.Body.Close()

	first := h.hostedHit(t) // attempt 1: bound kimi, 429'd
	if first.model != "kimi-k3" {
		t.Fatalf("attempt 1 = %+v, want the bound kimi member", first)
	}
	second := h.hit(t) // the cross: past gpt, onto the anthropic member
	if second.auth != "Bearer "+e2eToken(other) || second.model != "claude-fable-5" {
		t.Fatalf("cross = %+v, want the anthropic member (gpt at 100%% must be passed over)", second)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("client must see the crossed 200, got %d", resp.StatusCode)
	}
	h.noCodexLeak(t)
	if !throttledOn("kimi") {
		t.Fatal("the 429'd member's account must be throttled")
	}
	if got := recordsOn("gpt"); len(got) != 0 {
		t.Fatalf("gpt was never tried and must carry no record, got %v", got)
	}
	alerts := classAlerts(t, h.e2eEnv)
	if len(alerts) != 1 || alerts[0].Labels["alertname"] != "ClaudeClassSwitchover" ||
		alerts[0].Labels["to_account"] != "lane:fable" {
		t.Fatalf("want one class cross alert kimi -> lane:fable, got %v", alerts)
	}
}

// (A') The line is the brain's `hard`, not `high`: a rung merely HIGH (90%)
// still takes the cross under the mirrored default (95) — the ladder's question
// is "will it serve", not "should a new session settle here" — and stops taking
// it once the brain serves a hard line beneath it. Proves the threshold is
// decoded off /preferred rather than hard-coded.
func TestE2EClass429CrossHardLineComesFromBrain(t *testing.T) {
	t.Run("90% under default hard=95 → gpt takes the cross", func(t *testing.T) {
		h, restore := newE2EClass(t)
		defer restore()
		helpdeskShape(h, 90.0)
		h.deny.Store(true)

		resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-high-ok")
		defer resp.Body.Close()
		h.hostedHit(t) // kimi 429
		hit := h.codexHitOrFail(t)
		var cb map[string]any
		_ = json.Unmarshal(hit.body, &cb)
		if cb["model"] != "gpt-5.6" {
			t.Fatalf("cross must land on gpt (90%% < hard 95), got %v", cb["model"])
		}
		if resp.StatusCode != 200 {
			t.Fatalf("client must see codex's 200, got %d", resp.StatusCode)
		}
		h.noAnthropicLeak(t)
	})
	t.Run("90% over brain hard=85 → gpt passed over", func(t *testing.T) {
		h, restore := newE2EClass(t)
		defer restore()
		h.brainMu.Lock()
		h.brainHard = 85
		h.brainMu.Unlock()
		helpdeskShape(h, 90.0)
		if got := lan.hardPct(); got != 85 {
			t.Fatalf("hardPct = %v after /preferred with thresholds.hard=85", got)
		}
		h.deny.Store(true)

		resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-high-skip")
		defer resp.Body.Close()
		h.hostedHit(t) // kimi 429
		if second := h.hit(t); second.model != "claude-fable-5" {
			t.Fatalf("cross = %+v, want the anthropic member (gpt at 90%% >= hard 85)", second)
		}
		if resp.StatusCode != 200 {
			t.Fatalf("client must see the crossed 200, got %d", resp.StatusCode)
		}
		h.noCodexLeak(t)
	})
}

// (B) The cross is a WALK, not a single shot: bound kimi 429s, gpt (20%, first
// candidate) 429s too, the anthropic member serves. Both 429'd members cool;
// the client sees the 200; one class alert per hop.
func TestE2EClass429CrossWalksPastASecond429(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	helpdeskShape(h, 20.0)
	h.deny.Store(true)      // kimi 429s
	h.codexDeny.Store(true) // gpt 429s

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-walk")
	defer resp.Body.Close()

	if first := h.hostedHit(t); first.model != "kimi-k3" {
		t.Fatalf("attempt 1 = %+v, want kimi", first)
	}
	if second := h.codexHitOrFail(t); second.path != codexResponsesPath {
		t.Fatalf("hop 2 = %+v, want the gpt member on codex", second)
	}
	if third := h.hit(t); third.model != "claude-fable-5" {
		t.Fatalf("hop 3 = %+v, want the anthropic member", third)
	}
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("client must see the anthropic 200 after two 429s, got %d: %s", resp.StatusCode, b)
	}
	if !throttledOn("kimi") || !throttledOn("gpt") {
		t.Fatal("both 429'd members' accounts must be throttled")
	}
	if a, b := recordsOn(native), recordsOn(other); len(a)+len(b) != 0 {
		t.Fatalf("the member that served must carry no record: %v %v", a, b)
	}
	alerts := classAlertsN(t, h.e2eEnv, 2)
	var sawKimiToGpt, sawGptToFable bool
	for _, a := range alerts {
		if a.Labels["alertname"] != "ClaudeClassSwitchover" {
			continue
		}
		switch a.Labels["from_account"] + ">" + a.Labels["to_account"] {
		case "kimi/kimi-k3>gpt/gpt-5.6":
			sawKimiToGpt = true
		case "gpt/gpt-5.6>lane:fable":
			sawGptToFable = true
		}
	}
	if !sawKimiToGpt || !sawGptToFable {
		t.Fatalf("want one class alert per hop (kimi>gpt, gpt>fable), got %v", alerts)
	}

	// The next request detours BOTH cooling members without contacting the brain.
	calls := h.bindCalls.Load()
	resp2 := h.postSess(t, "claude-frontier", "CLIENT", "sess-walk")
	resp2.Body.Close()
	if hit := h.hit(t); hit.model != "claude-fable-5" {
		t.Fatalf("detoured request = %+v, want the anthropic member", hit)
	}
	h.noCodexLeak(t)
	select {
	case hit := <-h.hosted:
		t.Fatalf("detoured request still tried cooling kimi: %+v", hit)
	default:
	}
	if got := h.bindCalls.Load(); got != calls {
		t.Fatalf("detour contacted the brain (%d -> %d bind calls)", calls, got)
	}
}

// (C) Brain-exhausted is a last resort, not a wall: when the 100% rung is ALL
// that is left, the walk still tries it (a quota reading can be a stale poll;
// riding it beats refusing outright) — and here it serves.
func TestE2EClass429CrossRidesExhaustedRungWhenNothingElseIsLeft(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	helpdeskShape(h, 100.0)
	h.brainMu.Lock()
	h.classMembers[2]["usable"] = false // the anthropic member is out of the ladder
	h.classMembers[2]["status"] = "no_quota_data"
	h.brainMu.Unlock()
	fetchPreferredSync()
	h.deny.Store(true) // kimi 429s; codex serves

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-last-resort")
	defer resp.Body.Close()
	h.hostedHit(t) // kimi 429
	hit := h.codexHitOrFail(t)
	var cb map[string]any
	_ = json.Unmarshal(hit.body, &cb)
	if cb["model"] != "gpt-5.6" {
		t.Fatalf("last-resort cross must land on gpt, got %v", cb["model"])
	}
	if resp.StatusCode != 200 {
		t.Fatalf("client must see codex's 200, got %d", resp.StatusCode)
	}
	h.noAnthropicLeak(t)
}

// ---- refusal classification (#89, #90) ----

// shapeRejectBody is Anthropic's client-shape refusal verbatim: a
// 429 whose message is literally "Error". The fake upstream sets no
// Anthropic-Ratelimit-Unified-* headers on it, exactly like the real one.
const shapeRejectBody = `{"type":"error","error":{"type":"rate_limit_error","message":"Error"},"request_id":"req_test"}`

// (#89) kimi's quota cap is a 403 permission_error. Bound to kimi, the ladder
// must treat it as "this rung is spent": cool the member and walk to the next
// rung — instead of handing the 403 to the client and choosing kimi again.
func TestE2EClassKimiQuota403WalksLikeA429(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	helpdeskShape(h, 20.0) // bound to kimi; gpt (codex) has room
	h.deny403.Store(true)

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-kimi403")
	defer resp.Body.Close()

	if first := h.hostedHit(t); first.model != "kimi-k3" {
		t.Fatalf("attempt 1 = %+v, want kimi", first)
	}
	if second := h.codexHitOrFail(t); second.path != codexResponsesPath {
		t.Fatalf("hop 2 = %+v, want the gpt member on codex", second)
	}
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("client must see gpt's 200 after kimi's quota 403, got %d: %s", resp.StatusCode, b)
	}
	if got := recordsOn("kimi"); len(got) != 1 || got[0] != "spent:total_quota" {
		t.Fatalf("records on kimi = %v: the billing-cycle 403 is the monthly pool (K2), the account's", got)
	}
	if why := classMemberDialable(t, "frontier", "kimi/kimi-k3"); why == "" {
		t.Fatal("the 403-quota'd member's check must be no")
	}
	alerts := classAlertsN(t, h.e2eEnv, 1)
	var saw bool
	for _, a := range alerts {
		if a.Labels["alertname"] == "ClaudeClassSwitchover" && a.Labels["to_account"] == "gpt/gpt-5.6" &&
			strings.Contains(a.Annotations["summary"]+a.Annotations["description"]+a.Labels["reason"], "403") {
			saw = true
		}
	}
	if !saw {
		t.Fatalf("want a class cross alert naming the 403 quota reason, got %v", alerts)
	}

	// The next request detours kimi without touching it.
	resp2 := h.postSess(t, "claude-frontier", "CLIENT", "sess-kimi403")
	resp2.Body.Close()
	if hop := h.codexHitOrFail(t); hop.path != codexResponsesPath {
		t.Fatalf("detoured request = %+v, want gpt", hop)
	}
	select {
	case hit := <-h.hosted:
		t.Fatalf("detoured request still tried the 403-quota'd kimi: %+v", hit)
	default:
	}
}

// (K4, C7) Kimi's expired-membership 403 names no usage limit, so before C7
// it went to the client. It is not_entitled on the plan: the walk moves to
// gpt, the plan's record closes kimi for the next request, and nothing about
// the 403 needed quota words.
func TestE2EClassKimiNotEntitled403Walks(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	helpdeskShape(h, 20.0)
	h.kimi403.Store(kimiNotEntitled403)

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-kimi-k4")
	defer resp.Body.Close()
	if first := h.hostedHit(t); first.model != "kimi-k3" {
		t.Fatalf("attempt 1 = %+v, want kimi", first)
	}
	if second := h.codexHitOrFail(t); second.path != codexResponsesPath {
		t.Fatalf("hop 2 = %+v, want the gpt member on codex", second)
	}
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("client must see gpt's 200 after kimi's not-entitled 403, got %d: %s", resp.StatusCode, b)
	}
	if got := recordsOn("kimi"); len(got) != 1 || got[0] != "not_entitled:entitlement" {
		t.Fatalf("records on kimi = %v, want the plan's not_entitled record", got)
	}
	resp2 := h.postSess(t, "claude-frontier", "CLIENT", "sess-kimi-k4")
	resp2.Body.Close()
	if hop := h.codexHitOrFail(t); hop.path != codexResponsesPath {
		t.Fatalf("next request = %+v, want gpt", hop)
	}
	select {
	case hit := <-h.hosted:
		t.Fatalf("the next request still tried the not-entitled kimi: %+v", hit)
	default:
	}
}

// A bare provider 403 (auth, not quota) is NOT a ladder trigger: the client
// must see it, and nothing cools.
func TestE2EClassProviderAuth403IsNotExhaustion(t *testing.T) {
	m := classMemberPref{ID: "kimi/kimi-k3", Kind: "provider", Provider: "kimi", Model: "kimi-k3"}
	resp := &http.Response{StatusCode: 403, Header: http.Header{},
		Body: io.NopCloser(strings.NewReader(`{"type":"error","error":{"type":"permission_error","message":"invalid api key"}}`))}
	if r := lan.rungRefusal(m, resp); r.walks() {
		t.Fatalf("an auth 403 must not walk, classified %q", r.class)
	}
	anth := classMemberPref{ID: "lane:opus", Kind: "anthropic", Lane: "opus", Model: "claude-opus-5"}
	resp = &http.Response{StatusCode: 403, Header: http.Header{},
		Body: io.NopCloser(strings.NewReader(`{"type":"error","error":{"type":"permission_error","message":"You've reached your usage limit for this billing cycle."}}`))}
	if r := lan.rungRefusal(anth, resp); r.walks() {
		t.Fatalf("a 403 on an anthropic member is auth, never quota, classified %q", r.class)
	}
}

// (#90) Anthropic's client-shape refusal on the bound anthropic member: NOT
// capacity. No record, no sibling-account swap, no lane switchover alert; the
// ladder walks to a rung whose credential the client can use (§2.6
// wrong_member).
func TestE2EClassShapeRejected429DoesNotTouchTheAccount(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	h.body429.Store(shapeRejectBody)
	h.deny429.Store("*") // every Anthropic account would refuse this client's shape

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-shape")
	defer resp.Body.Close()

	first := h.hit(t) // attempt 1: fable's preferred account
	if first.auth != "Bearer "+e2eToken(other) || first.model != "claude-fable-5" {
		t.Fatalf("attempt 1 = %+v", first)
	}
	// NO attempt 2 on the sibling account: the account is not the problem.
	second := h.hostedHit(t)
	if second.model != "kimi-k3" {
		t.Fatalf("hop 2 must cross straight to kimi, got %+v", second)
	}
	select {
	case extra := <-h.seen:
		t.Fatalf("shape rejection must not swap Anthropic accounts, saw a second Anthropic hit %+v", extra)
	default:
	}
	if resp.StatusCode != 200 {
		t.Fatalf("client must see kimi's 200, got %d", resp.StatusCode)
	}
	// wrong_member (§2.6): walk, record nothing. The accounts are fine and the
	// store has no member key; the member stays dialable for clients whose
	// shape it takes.
	if n := liveRecords(); n != 0 {
		t.Fatalf("a shape rejection must write no record, got %d", n)
	}
	if why := classMemberDialable(t, "frontier", "lane:fable"); why != "" {
		t.Fatalf("lane:fable = %q, want dialable", why)
	}
	alerts := classAlertsN(t, h.e2eEnv, 1)
	for _, a := range alerts {
		if a.Labels["alertname"] != "ClaudeClassSwitchover" {
			t.Fatalf("a shape rejection must not fire a lane switchover alert, got %v", alerts)
		}
		if a.Labels["to_account"] != "kimi/kimi-k3" ||
			!strings.Contains(a.Annotations["summary"]+a.Annotations["description"]+a.Labels["reason"], "shape") {
			t.Fatalf("class cross alert must name the shape rejection, got %v", alerts)
		}
	}
	noMoreAlerts(t, h.e2eEnv)
}

// A capacity 429 that carries the unified ratelimit headers is capacity even if
// its message is terse — the shape check needs BOTH marks.
func TestShapeRejected429NeedsBothMarks(t *testing.T) {
	mk := func(msg string, unified bool) *http.Response {
		hd := http.Header{}
		if unified {
			hd.Set("Anthropic-Ratelimit-Unified-Status", "rejected")
		}
		return &http.Response{StatusCode: 429, Header: hd}
	}
	body := func(msg string) []byte {
		return []byte(`{"type":"error","error":{"type":"rate_limit_error","message":"` + msg + `"}}`)
	}
	if !shapeRejected429(mk("Error", false), body("Error")) {
		t.Fatal("bare Error + no unified headers = shape rejection")
	}
	if shapeRejected429(mk("Error", true), body("Error")) {
		t.Fatal("unified headers present = capacity, whatever the message")
	}
	if shapeRejected429(mk("x", false), body("You have exceeded your weekly usage limit.")) {
		t.Fatal("a descriptive message = capacity, headers or not")
	}
	if shapeRejected429(&http.Response{StatusCode: 403, Header: http.Header{}}, body("Error")) {
		t.Fatal("only a 429 can be a shape rejection")
	}
}
