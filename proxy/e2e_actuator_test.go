package main

// The account-kind seam (actuator.go), through the whole proxy: an api-kind
// Anthropic account is signed with its static key, and an account whose kind
// this build has no actuator for is a typed 503 naming the kind. Both run in
// every edition.

import (
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"
)

// apiStanding is a standing of kind api for an Anthropic account: no
// allowance, a funded money pot "usd" (<account>/usd on a route's pay).
func apiStanding() map[string]any {
	st := e2eStanding("anthropic", nil, float64(time.Now().Unix()))
	st["kind"] = apiKind
	st["money"] = map[string]any{"usd": map[string]any{"open": true, "balance": 25.0, "unit": "usd"}}
	return st
}

func TestE2EAPIKindAnthropicAccountSignsWithItsKey(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	resetAPIKeys(t)
	env.brainMu.Lock()
	env.standings = map[string]any{native: apiStanding(), other: apiStanding()}
	env.apiKeys = map[string]string{native: "sk-test-native", other: "sk-test-other"}
	env.brainMu.Unlock()
	env.payFrom("lane:haiku", native+"/usd", other+"/usd")
	fetchPreferredSync()

	resp := env.post(t, "claude-haiku-4-5", "CLIENT")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200: %s", resp.StatusCode, body)
	}
	h := env.hit(t)
	if h.apiKey != "sk-test-native" {
		t.Errorf("x-api-key = %q, want the lane account's key", h.apiKey)
	}
	if h.auth != "" {
		t.Errorf("Authorization = %q, want none (never the client's ambient login)", h.auth)
	}
	if strings.Contains(h.beta, "oauth-") {
		t.Errorf("anthropic-beta = %q, want no OAuth beta on a key", h.beta)
	}
	env.brainMu.Lock()
	calls := env.apiKeyCalls
	env.brainMu.Unlock()
	if calls == 0 {
		t.Error("the key never came from the brain's /api-key")
	}
}

func TestE2EAccountKindWithoutActuatorIsATyped503(t *testing.T) {
	// The default edition registers a subscription actuator; take it away
	// for this test, which is what the shared edition is built without.
	if a, ok := actuators[subscriptionKind]; ok {
		delete(actuators, subscriptionKind)
		t.Cleanup(func() { actuators[subscriptionKind] = a })
	}
	env, restore := newE2E(t)
	defer restore()

	resp := env.post(t, "claude-haiku-4-5", "CLIENT")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatalf("status %d, want 503: %s", resp.StatusCode, body)
	}
	var env503 struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &env503); err != nil {
		t.Fatalf("body is not the Anthropic error envelope: %v: %s", err, body)
	}
	if env503.Type != "error" || env503.Error.Type != "api_error" {
		t.Errorf("envelope = %+v, want type error / api_error", env503)
	}
	for _, want := range []string{"kind " + subscriptionKind, native, "no actuator"} {
		if !strings.Contains(env503.Error.Message, want) {
			t.Errorf("message %q does not name %q", env503.Error.Message, want)
		}
	}
	select {
	case h := <-env.seen:
		t.Errorf("the upstream was dialled (auth=%q); a kind with no actuator must never be", h.auth)
	default:
	}
}

// Strip, then sign: a 429 on an api account fails over to a subscription
// account on a clone of the refused hop, which must carry the subscription
// bearer alone (never the api key beside it), and the serve is the
// subscription account's.
func TestE2EAPIToSubscriptionFailoverCarriesOnlyTheBearer(t *testing.T) {
	t.Cleanup(resetQuotaState())
	env, restore := newE2E(t)
	defer restore()
	resetAPIKeys(t)
	// Both accounts pay on the haiku lane (an api account always does): the
	// api one, native, leads on its lower effective use, and the
	// subscription one, other, is spent with its credits open.
	env.spentWithCredits(other, "anthropic")
	env.brainMu.Lock()
	env.standings[native] = apiStanding()
	env.apiKeys = map[string]string{native: "sk-test-native"}
	env.lanes["haiku"] = map[string]any{"account": native, "reason": "incumbent", "effective": map[string]float64{native: 10, other: 70}}
	env.brainMu.Unlock()
	env.payFrom("lane:haiku", native+"/usd", other+"/credits")
	fetchPreferredSync()
	// The api hop's 429 rejects its weekly window: exhaustion, which moves
	// the lane to the paid sibling (a throttle would not). A hop with no
	// bearer is the api one.
	reset := strconv.FormatInt(time.Now().Add(48*time.Hour).Unix(), 10)
	env.anthHeadersFor.Store(map[string]map[string]string{"": {
		"anthropic-ratelimit-unified-status":         "rejected",
		"anthropic-ratelimit-unified-7d-status":      "rejected",
		"anthropic-ratelimit-unified-7d-utilization": "1.0",
		"anthropic-ratelimit-unified-7d-reset":       reset,
	}})
	env.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"You have exceeded your weekly usage limit."}}`)
	env.deny429.Store("sk-test-native")
	env.anthJSON.Store(`{"id":"msg_1","type":"message","role":"assistant","model":"claude-haiku-4-5",` +
		`"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":1}}`)

	resp := env.post(t, "claude-haiku-4-5", "CLIENT")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	first, second := env.hit(t), env.hit(t)
	if first.apiKey != "sk-test-native" || first.auth != "" {
		t.Fatalf("first hop key=%q auth=%q, want the api account's key alone", first.apiKey, first.auth)
	}
	if second.auth != "Bearer "+e2eToken(other) || second.apiKey != "" {
		t.Fatalf("failover hop auth=%q key=%q, want %s's bearer alone", second.auth, second.apiKey, other)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want the failover's 200", resp.StatusCode)
	}
	ev := waitServed(t, env, 1)[0]
	wireHas(t, ev, map[string]any{"account": other})
}

// A class's anthropic member whose accounts have no actuator here is skipped
// loudly: the log says which member and kind, and when the class has nothing
// else, its refusal names the kind.
func TestE2EClassMemberWithoutActuatorIsNamed(t *testing.T) {
	if a, ok := actuators[subscriptionKind]; ok {
		delete(actuators, subscriptionKind)
		t.Cleanup(func() { actuators[subscriptionKind] = a })
	}
	h, restore := newE2EClass(t)
	defer restore()
	h.brainMu.Lock()
	h.classMembers = classFixtureMembers()[:1] // lane:fable alone
	h.brainMu.Unlock()
	fetchPreferredSync()
	logs := captureLogSync(t)

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-unactuated-1")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode == 200 {
		t.Fatalf("status 200, want a refusal: %s", body)
	}
	if !strings.Contains(string(body), "lane:fable (account kind "+subscriptionKind+")") {
		t.Errorf("refusal %s does not name the member's kind", body)
	}
	waitLog(t, logs, "skips lane:fable: its accounts are of kind "+subscriptionKind)
	select {
	case hit := <-h.seen:
		t.Errorf("the upstream was dialled (auth=%q)", hit.auth)
	default:
	}
}
