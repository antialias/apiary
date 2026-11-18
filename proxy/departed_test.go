package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// departedDoc is a minimal brain document listing accounts.
func departedDoc(t *testing.T, accounts ...string) string {
	t.Helper()
	accts := map[string]any{}
	for _, a := range accounts {
		accts[a] = map[string]any{}
	}
	b, err := json.Marshal(map[string]any{
		"account": "", "reason": "test", "weekly": map[string]any{},
		"accounts": accts, "routes": map[string]any{},
		"settings": map[string]any{"stale_max": 1800},
		"ledger":   e2eLedger(time.Now(), 0, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// An account that leaves the brain's document (disconnected, C11) takes
// every credential this proxy cached for it: its subscription token and
// prewarm timer, its codex bearer (by pool name, and the incumbent's under
// ""), its api key. An account still listed keeps its own, and another
// tenant's caches are untouched.
func TestDocumentDropsDepartedAccountsCredentials(t *testing.T) {
	other := addTenant(tenantRow{Tenant: "ten_" + strings.Repeat("b", 26), Name: "other", Tap: tapMetadata})
	defer removeTenant(other.id)
	exp := time.Now().Add(time.Hour)
	for _, tn := range []*tenant{lan, other} {
		tn.applyPreferred(strings.NewReader(departedDoc(t, "sub-a", "sub-b", "gpt-a", "gpt-b", "api-a", "api-b")))
		tn.tokMu.Lock()
		tn.tokens["sub-a"] = tokEntry{token: "tok-a", exp: exp}
		tn.tokens["sub-b"] = tokEntry{token: "tok-b", exp: exp}
		tn.tokMu.Unlock()
		tn.warmMu.Lock()
		tn.warmTimers["sub-a"] = time.AfterFunc(time.Hour, func() {})
		tn.warmMu.Unlock()
		tn.codexTokMu.Lock()
		tn.codexToks["gpt-a"] = codexTokEntry{val: "bearer-a", name: "gpt-a", exp: exp, fetched: time.Now()}
		tn.codexToks[""] = codexTokEntry{val: "bearer-a", name: "gpt-a", exp: exp, fetched: time.Now()}
		tn.codexToks["gpt-b"] = codexTokEntry{val: "bearer-b", name: "gpt-b", exp: exp, fetched: time.Now()}
		tn.codexTokMu.Unlock()
		tn.apiKeyMu.Lock()
		tn.apiKeys["api-a"] = apiKeyEntry{val: "key-a", fetched: time.Now()}
		tn.apiKeyStates["api-a"] = ""
		tn.apiKeys["api-b"] = apiKeyEntry{val: "key-b", fetched: time.Now()}
		tn.apiKeyMu.Unlock()
	}

	lan.applyPreferred(strings.NewReader(departedDoc(t, "sub-b", "gpt-b", "api-b")))

	lan.tokMu.Lock()
	_, hasA := lan.tokens["sub-a"]
	_, hasB := lan.tokens["sub-b"]
	lan.tokMu.Unlock()
	if hasA || !hasB {
		t.Errorf("subscription tokens: sub-a kept=%v (want false), sub-b kept=%v (want true)", hasA, hasB)
	}
	lan.warmMu.Lock()
	if lan.warmTimers["sub-a"] != nil {
		t.Error("the departed account's prewarm timer is still armed")
	}
	lan.warmMu.Unlock()
	lan.codexTokMu.Lock()
	var codex []string
	for k := range lan.codexToks {
		codex = append(codex, k)
	}
	lan.codexTokMu.Unlock()
	if len(codex) != 1 || codex[0] != "gpt-b" {
		t.Errorf("codex bearers left: %v, want [gpt-b] (the incumbent's \"\" entry named gpt-a goes too)", codex)
	}
	lan.apiKeyMu.Lock()
	_, keyA := lan.apiKeys["api-a"]
	_, stateA := lan.apiKeyStates["api-a"]
	_, keyB := lan.apiKeys["api-b"]
	lan.apiKeyMu.Unlock()
	if keyA || stateA || !keyB {
		t.Errorf("api keys: api-a kept=%v state=%v (want neither), api-b kept=%v (want true)", keyA, stateA, keyB)
	}

	other.tokMu.Lock()
	n := len(other.tokens)
	other.tokMu.Unlock()
	if n != 2 {
		t.Errorf("another tenant's tokens: %d, want 2 (its document did not change)", n)
	}

	// A document the proxy cannot decode departs nothing.
	lan.applyPreferred(strings.NewReader("{not json"))
	lan.tokMu.Lock()
	_, hasB = lan.tokens["sub-b"]
	lan.tokMu.Unlock()
	if !hasB {
		t.Error("an undecodable document dropped a listed account's token")
	}
}

func TestDepartedAccounts(t *testing.T) {
	prev := map[string]acctStanding{"a": {}, "b": {}, "c": {}}
	next := map[string]acctStanding{"b": {}, "d": {}}
	if got := departedAccounts(prev, next); strings.Join(got, ",") != "a,c" {
		t.Errorf("departed = %v, want [a c]", got)
	}
	if got := departedAccounts(nil, next); len(got) != 0 {
		t.Errorf("the first document departs %v, want nothing", got)
	}
}
