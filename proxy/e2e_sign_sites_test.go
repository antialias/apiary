package main

// Strip, then sign, at the signing sites a clone of an earlier hop goes
// through: the class walk's same-member account swap, the cross from a
// provider member onto an anthropic one (rebuildForMember), and the
// subscription reauth retry. Each clone is signed whole by its account's
// actuator, never by setting one header on top of what the earlier hop
// carried.

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// haikuClass makes the class fixture's anthropic member a haiku-lane one,
// alone or ahead of the provider members, so an api-kind account can serve
// it (the fable lane's scope wants a reading an api standing has none of).
func haikuClass(h *e2eHosted, alone bool) {
	h.brainMu.Lock()
	m := h.classMembers[0]
	m["id"], m["lane"], m["model"] = "lane:haiku", "haiku", "claude-haiku-4-5"
	if alone {
		h.classMembers = []map[string]any{m}
	}
	h.brainMu.Unlock()
}

// A 429 on the class member's api account swaps to its subscription account
// on a clone of the refused hop: that clone carries the bearer alone.
func TestE2EClassAPIToSubscriptionSwapCarriesOnlyTheBearer(t *testing.T) {
	t.Cleanup(resetQuotaState())
	h, restore := newE2EClass(t)
	defer restore()
	resetAPIKeys(t)
	haikuClass(h, true)
	h.spentWithCredits(other, "anthropic")
	h.brainMu.Lock()
	h.standings[native] = apiStanding()
	h.apiKeys = map[string]string{native: "sk-test-native"}
	h.lanes["haiku"] = map[string]any{"account": native, "reason": "incumbent", "effective": map[string]float64{native: 10, other: 70}}
	h.brainMu.Unlock()
	h.payFrom("frontier/lane:haiku", native+"/usd", other+"/credits")
	fetchPreferredSync()
	reset := strconv.FormatInt(time.Now().Add(48*time.Hour).Unix(), 10)
	h.anthHeadersFor.Store(map[string]map[string]string{"": {
		"anthropic-ratelimit-unified-status":         "rejected",
		"anthropic-ratelimit-unified-7d-status":      "rejected",
		"anthropic-ratelimit-unified-7d-utilization": "1.0",
		"anthropic-ratelimit-unified-7d-reset":       reset,
	}})
	h.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"You have exceeded your weekly usage limit."}}`)
	h.deny429.Store("sk-test-native")

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-api-swap")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	first, second := h.hit(t), h.hit(t)
	if first.apiKey != "sk-test-native" || first.auth != "" {
		t.Fatalf("first hop key=%q auth=%q, want the api account's key alone", first.apiKey, first.auth)
	}
	if second.auth != "Bearer "+e2eToken(other) || second.apiKey != "" {
		t.Fatalf("swap hop auth=%q key=%q, want %s's bearer alone", second.auth, second.apiKey, other)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want the swap's 200", resp.StatusCode)
	}
}

// A provider member's quota refusal crosses onto the anthropic member, whose account
// is api kind: the cross is signed with the account's key alone, not the
// provider's bearer and not a bearer of the key.
func TestE2EClassCrossOntoAnAPIAccountCarriesOnlyItsKey(t *testing.T) {
	t.Cleanup(resetQuotaState())
	h, restore := newE2EClass(t)
	defer restore()
	resetAPIKeys(t)
	haikuClass(h, false)
	h.brainMu.Lock()
	h.standings = map[string]any{native: apiStanding(), other: apiStanding()}
	h.apiKeys = map[string]string{native: "sk-test-native", other: "sk-test-other"}
	h.lanes["haiku"] = map[string]any{"account": other, "reason": "incumbent", "effective": map[string]float64{other: 10, native: 20}}
	h.brainMu.Unlock()
	h.payFrom("frontier/lane:haiku", other+"/usd", native+"/usd")
	h.bindTo.Store("kimi/kimi-k3")
	fetchPreferredSync()
	h.deny403.Store(true) // kimi's quota 403: exhaustion, which walks on to a paid member; Anthropic serves

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-api-cross")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if first := h.hostedHit(t); first.auth != "Bearer kimi-secret" {
		t.Fatalf("attempt 1 = %+v, want the bound kimi member", first)
	}
	second := h.hit(t)
	if second.apiKey != "sk-test-other" || second.auth != "" {
		t.Fatalf("cross key=%q auth=%q, want the api account's key alone", second.apiKey, second.auth)
	}
	if strings.Contains(second.beta, "oauth-") {
		t.Errorf("cross anthropic-beta = %q, want no OAuth beta on a key", second.beta)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want the cross's 200", resp.StatusCode)
	}
}

// signRecorder answers its first hop with a 401 on a revoked copy and every
// later one with a 200, keeping each hop's headers.
type signRecorder struct {
	mu   sync.Mutex
	hops []http.Header
}

func (r *signRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	n := len(r.hops)
	r.hops = append(r.hops, req.Header.Clone())
	r.mu.Unlock()
	status, body := 200, `{"id":"msg_1","type":"message","content":[]}`
	if n == 0 {
		status, body = 401, e2eRevoked401
	}
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
}

// The reauth retry is signed whole by the subscription actuator: whatever
// else the refused hop carried (here a stray x-api-key, and no oauth beta),
// the retry carries the fresh bearer, the oauth beta, and no key.
func TestReauthRetryIsSignedWhole(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	old := e2eToken(other)
	env.rotate(other)
	fresh := env.brainCopy(other)

	anth, _ := url.Parse("https://api.anthropic.invalid")
	rec := &signRecorder{}
	tr := &failoverTransport{base: rec, anthropic: anth}
	body := []byte(`{"model":"claude-haiku-4-5","messages":[]}`)
	req, _ := http.NewRequest("POST", anth.String()+"/v1/messages", bytes.NewReader(body))
	req = req.WithContext(withAnthDial(req.Context(), other, body))
	req.Header.Set("Authorization", "Bearer "+old)
	req.Header.Set("X-Api-Key", "sk-stray")
	resp, err := tr.tripWithReauth(req)
	if err != nil {
		t.Fatal(err)
	}
	b := readAll(t, resp)
	if resp.StatusCode != 200 || len(rec.hops) != 2 {
		t.Fatalf("reply = %d %s after %d hops, want the retry's 200 after 2", resp.StatusCode, b, len(rec.hops))
	}
	retry := rec.hops[1]
	if got := retry.Get("Authorization"); got != "Bearer "+fresh {
		t.Errorf("retry Authorization = %q, want the fresh copy", got)
	}
	if got := retry.Get("X-Api-Key"); got != "" {
		t.Errorf("retry x-api-key = %q, want none beside the bearer", got)
	}
	if got := retry.Get("anthropic-beta"); !strings.Contains(got, "oauth-") {
		t.Errorf("retry anthropic-beta = %q, want the oauth beta the bearer needs", got)
	}
}
