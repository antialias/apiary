package main

// fetchPreferred against /preferred payloads that lack what the dial check
// reads: a decode failure (brainFail), never a silent swap to "no routes".

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
)

// brainServing runs fetchPreferred against a fake brain serving whatever
// set() last gave it, on a fresh decision restored after the test.
func brainServing(t *testing.T) (set func(map[string]any)) {
	t.Helper()
	var mu sync.Mutex
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Write(body)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(resetQuotaState())
	oldBrain, oldPref := brainBase, lan.pref
	brainBase, lan.pref = srv.URL, &decision{}
	classRowsMu.Lock()
	oldRows, oldSource := classRowsSet, classRowsSource
	classRowsMu.Unlock()
	t.Cleanup(func() {
		prewarmWG.Wait()
		stopPrewarmTimers()
		brainBase, lan.pref = oldBrain, oldPref
		classRowsMu.Lock()
		classRowsSet, classRowsSource = oldRows, oldSource
		classRowsMu.Unlock()
	})
	return func(p map[string]any) {
		b, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		body = b
		mu.Unlock()
	}
}

// brainPreferred is the brain's real /preferred (testdata/preferred_c2.json).
func brainPreferred(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile("testdata/preferred_c2.json")
	if err != nil {
		t.Fatal(err)
	}
	var fx struct {
		Preferred map[string]any `json:"preferred"`
	}
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatal(err)
	}
	return fx.Preferred
}

func TestPreferredWithoutPolicyIsADecodeFailure(t *testing.T) {
	for _, drop := range []string{"routes", "accounts", "settings.stale_max", "ledger"} {
		t.Run(drop, func(t *testing.T) {
			set := brainServing(t)
			set(brainPreferred(t))
			fetchPreferred()
			lan.pref.mu.RLock()
			ok, routes, accounts, staleMax, ledger := lan.pref.ok, len(lan.pref.routes), len(lan.pref.accounts), lan.pref.staleMax, lan.pref.ledger
			lan.pref.mu.RUnlock()
			if !ok || routes == 0 || accounts == 0 || staleMax != 1800 || ledger == nil {
				t.Fatalf("the brain's own payload: ok=%v routes=%d accounts=%d stale_max=%v", ok, routes, accounts, staleMax)
			}
			p := brainPreferred(t)
			if drop == "settings.stale_max" {
				delete(p["settings"].(map[string]any), "stale_max")
			} else {
				delete(p, drop)
			}
			set(p)
			fetchPreferred()
			lan.pref.mu.RLock()
			defer lan.pref.mu.RUnlock()
			if lan.pref.ok {
				t.Fatalf("a /preferred without %s decoded as reachable", drop)
			}
			if len(lan.pref.routes) != routes || len(lan.pref.accounts) != accounts || lan.pref.staleMax != staleMax || lan.pref.ledger != ledger {
				t.Fatalf("without %s: the last good state was replaced (routes %d accounts %d stale_max %v)",
					drop, len(lan.pref.routes), len(lan.pref.accounts), lan.pref.staleMax)
			}
		})
	}
}

// A brain with no Claude-plan account (the API-key quickstart) names no
// account, top level or per lane. Its document still decodes: the routes carry
// the accounts that serve each lane, and the class rows come from it.
func TestPreferredWithNoAccountPickIsApplied(t *testing.T) {
	set := brainServing(t)
	p := brainPreferred(t)
	p["account"] = nil
	if lanes, ok := p["lanes"].(map[string]any); ok {
		for _, l := range lanes {
			l.(map[string]any)["account"] = nil
		}
	}
	set(p)
	fetchPreferred()
	lan.pref.mu.RLock()
	ok, account, routes := lan.pref.ok, lan.pref.account, len(lan.pref.routes)
	lan.pref.mu.RUnlock()
	if !ok || account != "" || routes == 0 {
		t.Fatalf("a /preferred naming no account: ok=%v account=%q routes=%d", ok, account, routes)
	}
}
