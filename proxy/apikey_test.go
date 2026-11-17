package main

// The brain's static-key dispense (apikey.go, quota-model C8): the proxy holds
// no upstream key; it is dispensed an api account's by GET /api-key, caches
// it like a codex bearer, and a key it cannot have is a named state, logged
// once per change and shown on /_switch/status, never the key itself.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// resetAPIKeys empties the key cache and its states for one test.
func resetAPIKeys(t *testing.T) {
	t.Helper()
	lan.apiKeyMu.Lock()
	oldKeys, oldStates := lan.apiKeys, lan.apiKeyStates
	lan.apiKeys, lan.apiKeyStates = map[string]apiKeyEntry{}, map[string]string{}
	lan.apiKeyMu.Unlock()
	t.Cleanup(func() {
		lan.apiKeyMu.Lock()
		lan.apiKeys, lan.apiKeyStates = oldKeys, oldStates
		lan.apiKeyMu.Unlock()
	})
}

// apiKeyBrain is a brain that dispenses keys[account] (absent: its 404).
type apiKeyBrain struct {
	mu    sync.Mutex
	keys  map[string]string
	calls int
	srv   *httptest.Server
}

func newAPIKeyBrain(t *testing.T, keys map[string]string) *apiKeyBrain {
	t.Helper()
	b := &apiKeyBrain{keys: keys}
	b.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != tpath("/api-key") || r.Header.Get("X-Switch-Key") != "unit-key" {
			w.WriteHeader(401)
			return
		}
		acct := r.URL.Query().Get("account")
		b.mu.Lock()
		b.calls++
		k, ok := b.keys[acct]
		b.mu.Unlock()
		if !ok {
			w.WriteHeader(404)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": "the brain has no key for api account '" + acct + "'", "reason": "no_key"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"account": acct, "api_key": k})
	}))
	oldBrain, oldKey, oldFresh := brainBase, switchKey, apiKeyFresh
	brainBase, switchKey = b.srv.URL, "unit-key"
	t.Cleanup(func() {
		b.srv.Close()
		brainBase, switchKey, apiKeyFresh = oldBrain, oldKey, oldFresh
	})
	resetAPIKeys(t)
	return b
}

func (b *apiKeyBrain) set(acct, key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if key == "" {
		delete(b.keys, acct)
		return
	}
	b.keys[acct] = key
}

// A key is fetched once per apiKeyFresh, and a rotation is adopted after it.
func TestAPIKeyIsCachedAndARotationAdopted(t *testing.T) {
	b := newAPIKeyBrain(t, map[string]string{"runway": "key-one"})
	for i := 0; i < 3; i++ {
		if k, err := lan.getAPIKey(context.Background(), "runway"); err != nil || k != "key-one" {
			t.Fatalf("key = %q %v", k, err)
		}
	}
	if b.calls != 1 {
		t.Fatalf("%d brain calls, want 1 within apiKeyFresh", b.calls)
	}
	b.set("runway", "key-two")
	apiKeyFresh = 0
	if k, _ := lan.getAPIKey(context.Background(), "runway"); k != "key-two" {
		t.Fatalf("after the rotation key = %q", k)
	}
}

// The brain with no key is a named state: the error says so, it is logged
// once (not per call), /_switch/status shows it without any key, and a key
// placed later ends it (logged once more).
func TestAPIKeyMissingIsANamedStateLoggedOncePerChange(t *testing.T) {
	b := newAPIKeyBrain(t, map[string]string{})
	logs := captureLog(t)
	apiKeyFresh = 0
	for i := 0; i < 3; i++ {
		_, err := lan.getAPIKey(context.Background(), "runway")
		if err == nil || !strings.Contains(err.Error(), "the brain has no key for api account 'runway'") {
			t.Fatalf("err = %v", err)
		}
	}
	if n := strings.Count(logs.String(), "api key for runway: none"); n != 1 {
		t.Fatalf("logged %d times, want once:\n%s", n, logs)
	}
	st := lan.apiKeyStatus()
	if len(st) != 1 || st[0]["state"] != "missing" || !strings.Contains(st[0]["reason"].(string), "no key") {
		t.Fatalf("status = %v", st)
	}
	b.set("runway", "key-placed")
	if k, err := lan.getAPIKey(context.Background(), "runway"); err != nil || k != "key-placed" {
		t.Fatalf("after the key was placed: %q %v", k, err)
	}
	if !strings.Contains(logs.String(), "api key for runway: dispensed") {
		t.Fatalf("the change back was not logged:\n%s", logs)
	}
	st = lan.apiKeyStatus()
	if st[0]["state"] != "ok" || st[0]["reason"] != nil {
		t.Fatalf("status = %v", st)
	}
	if strings.Contains(mustJSON(t, st), "key-placed") {
		t.Fatal("the status carries the key")
	}
}

// The brain withdrawing a key withdraws the cached one: no stale key is sent
// after the brain said it has none.
func TestAPIKeyWithdrawnByTheBrainIsNotServedStale(t *testing.T) {
	b := newAPIKeyBrain(t, map[string]string{"runway": "key-one"})
	if _, err := lan.getAPIKey(context.Background(), "runway"); err != nil {
		t.Fatal(err)
	}
	b.set("runway", "")
	apiKeyFresh = 0
	if k, err := lan.getAPIKey(context.Background(), "runway"); err == nil || k != "" {
		t.Fatalf("a withdrawn key was served: %q", k)
	}
}

// The brain unreachable: a cached key rides on (STALE); with nothing cached
// the state names the unreachable dispenser.
func TestAPIKeyBrainUnreachable(t *testing.T) {
	b := newAPIKeyBrain(t, map[string]string{"runway": "key-one"})
	if _, err := lan.getAPIKey(context.Background(), "runway"); err != nil {
		t.Fatal(err)
	}
	b.srv.Close()
	apiKeyFresh = 0
	if k, err := lan.getAPIKey(context.Background(), "runway"); err != nil || k != "key-one" {
		t.Fatalf("stale: %q %v, want the cached key", k, err)
	}
	_, err := lan.getAPIKey(context.Background(), "other")
	if err == nil || !strings.Contains(err.Error(), "the brain's /api-key is unreachable and no key is cached") {
		t.Fatalf("err = %v", err)
	}
	if st := lan.apiKeyStatus(); len(st) != 2 || st[0]["account"] != "other" || st[0]["state"] != "missing" {
		t.Fatalf("status = %v", st)
	}
}

// dropAPIKey (Runway refused the key) makes the next call ask the brain.
func TestAPIKeyDroppedIsFetchedAgain(t *testing.T) {
	b := newAPIKeyBrain(t, map[string]string{"runway": "key-one"})
	_, _ = lan.getAPIKey(context.Background(), "runway")
	lan.dropAPIKey("runway")
	_, _ = lan.getAPIKey(context.Background(), "runway")
	if b.calls != 2 {
		t.Fatalf("%d brain calls, want 2", b.calls)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// The ask runs on its caller's context (R3): a hung brain holds a caller only
// as long as the caller stays, and an ask cut short changes neither the cache
// nor the state (nothing is logged, nothing shown as missing).
func TestAPIKeyAskEndsWithItsCaller(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(release); srv.Close() })
	oldBrain, oldKey := brainBase, switchKey
	brainBase, switchKey = srv.URL, "unit-key"
	t.Cleanup(func() { brainBase, switchKey = oldBrain, oldKey })
	resetAPIKeys(t)
	logs := captureLog(t)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	t0 := time.Now()
	_, err := lan.getAPIKey(ctx, "runway")
	if took := time.Since(t0); took > time.Second {
		t.Fatalf("the ask took %s after its caller ended", took)
	}
	if err == nil || !strings.Contains(err.Error(), "cut short") {
		t.Fatalf("err = %v", err)
	}
	if st := lan.apiKeyStatus(); len(st) != 0 {
		t.Fatalf("status = %v, want nothing from an ask cut short", st)
	}
	if strings.Contains(logs.String(), "api key for runway") {
		t.Fatalf("logged:\n%s", logs)
	}
}
