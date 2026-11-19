package main

// A credential this proxy caches is its tenant's own and its account's
// current one: a fetch that began before a drop never re-caches what the drop
// forgot, a re-added account (its recheck_epoch advanced) is a new identity,
// and each provider signs a tenant's request with that tenant's credential,
// never another's.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func ep(v float64) *float64 { return &v }

func TestDepartedAccountsCountsAReAdd(t *testing.T) {
	prev := map[string]acctStanding{"same": {RecheckEpoch: ep(10)}, "readded": {RecheckEpoch: ep(10)},
		"first-epoch": {}, "rechecked-back": {RecheckEpoch: ep(10)}}
	next := map[string]acctStanding{"same": {RecheckEpoch: ep(10)}, "readded": {RecheckEpoch: ep(20)},
		"first-epoch": {RecheckEpoch: ep(5)}, "rechecked-back": {}}
	if got := strings.Join(departedAccounts(prev, next), ","); got != "first-epoch,readded" {
		t.Errorf("departed = %s, want first-epoch,readded (an advanced epoch is a new identity; an absent one is not)", got)
	}
}

// perTenantBrain answers /t/<tenant>/{token,openai-token,api-key} with a
// credential naming the tenant and the account. Before answering, hook (when
// set) runs: a drop landing while the fetch is in flight.
type perTenantBrain struct {
	mu   sync.Mutex
	hook func()
}

func newPerTenantBrain(t *testing.T) *perTenantBrain {
	t.Helper()
	b := &perTenantBrain{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Switch-Key") != "unit-key" {
			w.WriteHeader(401)
			return
		}
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/t/"), "/", 2)
		if len(parts) != 2 {
			http.NotFound(w, r)
			return
		}
		b.mu.Lock()
		hook := b.hook
		b.mu.Unlock()
		if hook != nil {
			hook()
		}
		tenantID, acct := parts[0], r.URL.Query().Get("account")
		cred := "cred-" + tenantID + "-" + acct
		exp := float64(time.Now().Add(time.Hour).Unix())
		switch parts[1] {
		case "token":
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": cred, "expires_at": exp})
		case "openai-token":
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": cred, "expires_at": exp,
				"account_id": "id-" + tenantID, "name": acct})
		case "api-key":
			_ = json.NewEncoder(w).Encode(map[string]any{"account": acct, "api_key": cred})
		default:
			http.NotFound(w, r)
		}
	}))
	oldBrain, oldKey := brainBase, switchKey
	brainBase, switchKey = srv.URL, "unit-key"
	t.Cleanup(func() {
		srv.Close()
		brainBase, switchKey = oldBrain, oldKey
	})
	return b
}

func (b *perTenantBrain) onFetch(f func()) {
	b.mu.Lock()
	b.hook = f
	b.mu.Unlock()
}

// Each provider a member can connect signs tenant A's request with A's
// credential and B's with B's: Claude (/token), Codex (/openai-token), and
// every key-signed one through /api-key: the Anthropic API, Runway,
// ElevenLabs and TypeSafe (getAPIKey) and Kimi (bearerWire).
func TestEachProviderSignsWithTheAskingTenantsCredential(t *testing.T) {
	newPerTenantBrain(t)
	a := addTenant(tenantRow{Tenant: "ten_" + strings.Repeat("c", 26), Name: "a", Tap: tapMetadata})
	defer removeTenant(a.id)
	b := addTenant(tenantRow{Tenant: "ten_" + strings.Repeat("d", 26), Name: "b", Tap: tapMetadata})
	defer removeTenant(b.id)
	for _, tn := range []*tenant{a, b} {
		tn.pref.mu.Lock()
		tn.pref.accounts = map[string]acctStanding{"kimi": {Kind: "provider"}}
		tn.pref.mu.Unlock()
	}
	for _, tn := range []*tenant{a, b, a} { // a again: its cache, not b's fetch
		want := func(acct string) string { return "cred-" + tn.id + "-" + acct }
		if got := tn.getToken("sub"); got != want("sub") {
			t.Errorf("claude: tenant %s signed with %q", tn.name, got)
		}
		if got, _, _, _ := tn.getCodexToken("gpt"); got != want("gpt") {
			t.Errorf("codex: tenant %s signed with %q", tn.name, got)
		}
		for _, acct := range []string{"api", "runway", "elevenlabs", "typesafe"} {
			if got, err := tn.getAPIKey(nil, acct); err != nil || got != want(acct) {
				t.Errorf("%s: tenant %s signed with %q (%v)", acct, tn.name, got, err)
			}
		}
		info := reqInfo{tenant: tn, poolAccount: "kimi"}
		if refusal := (bearerWire{}).prepare(&info, nil, ""); refusal != "" {
			t.Fatalf("kimi: tenant %s refused: %s", tn.name, refusal)
		}
		out, _ := http.NewRequest("POST", "http://kimi.invalid/v1/messages", nil)
		out.Header.Set("Authorization", "Bearer client")
		bearerWire{}.apply(out, info)
		if got := out.Header.Get("Authorization"); got != "Bearer "+want("kimi") {
			t.Errorf("kimi: tenant %s signed with %q", tn.name, got)
		}
	}
	// A second Kimi account in one tenant signs with its own key, never the
	// first's: the dial's account is the one bearerWire reads (D6).
	a.pref.mu.Lock()
	a.pref.accounts = map[string]acctStanding{"kimi": {Kind: "provider"}, "kimi2": {Kind: "provider"}}
	a.pref.mu.Unlock()
	for _, acct := range []string{"kimi2", "kimi"} {
		info := reqInfo{tenant: a, poolAccount: acct}
		if refusal := (bearerWire{}).prepare(&info, nil, ""); refusal != "" {
			t.Fatalf("%s: refused: %s", acct, refusal)
		}
		out, _ := http.NewRequest("POST", "http://kimi.invalid/v1/messages", nil)
		bearerWire{}.apply(out, info)
		if got := out.Header.Get("Authorization"); got != "Bearer cred-"+a.id+"-"+acct {
			t.Errorf("%s signed with %q", acct, got)
		}
	}
	// A hop the dial named no account for is refused, not signed as one.
	if refusal := (bearerWire{}).prepare(&reqInfo{tenant: a}, nil, ""); refusal == "" {
		t.Error("a hop with no dialled account was signed")
	}
	// A tenant whose document does not list kimi is refused before any fetch.
	c := addTenant(tenantRow{Tenant: "ten_" + strings.Repeat("e", 26), Name: "c", Tap: tapMetadata})
	defer removeTenant(c.id)
	info := reqInfo{tenant: c, poolAccount: "kimi"}
	if refusal := (bearerWire{}).prepare(&info, nil, ""); refusal == "" || info.token != "" {
		t.Errorf("kimi for a tenant without the account: refusal=%q token=%q", refusal, info.token)
	}
}

// A fetch that began before a drop answers its caller once and caches
// nothing, for each cache: the subscription token, the codex bearer and the
// api key. The next ask fetches again.
func TestAFetchRacingADropCachesNothing(t *testing.T) {
	brain := newPerTenantBrain(t)
	tn := addTenant(tenantRow{Tenant: "ten_" + strings.Repeat("f", 26), Name: "race", Tap: tapMetadata})
	defer removeTenant(tn.id)
	brain.onFetch(func() { tn.dropDeparted([]string{"sub", "gpt", "api"}) })

	if got := tn.getToken("sub"); got == "" {
		t.Fatal("the racing fetch should still answer its caller")
	}
	tn.getCodexToken("gpt")
	if _, err := tn.getAPIKey(nil, "api"); err != nil {
		t.Fatal(err)
	}
	tn.tokMu.Lock()
	_, tok := tn.tokens["sub"]
	tn.tokMu.Unlock()
	tn.codexTokMu.Lock()
	_, codex := tn.codexToks["gpt"]
	tn.codexTokMu.Unlock()
	tn.apiKeyMu.Lock()
	_, key := tn.apiKeys["api"]
	tn.apiKeyMu.Unlock()
	tn.warmMu.Lock()
	warm := tn.warmTimers["sub"] != nil
	tn.warmMu.Unlock()
	if tok || codex || key || warm {
		t.Errorf("re-cached after a drop: token=%v codex=%v apikey=%v prewarm=%v", tok, codex, key, warm)
	}

	brain.onFetch(nil)
	tn.getToken("sub")
	if _, err := tn.getAPIKey(nil, "api"); err != nil {
		t.Fatal(err)
	}
	tn.tokMu.Lock()
	_, tok = tn.tokens["sub"]
	tn.tokMu.Unlock()
	tn.apiKeyMu.Lock()
	_, key = tn.apiKeys["api"]
	tn.apiKeyMu.Unlock()
	if !tok || !key {
		t.Errorf("a fetch with no drop since it began must cache: token=%v apikey=%v", tok, key)
	}
	tn.dropDeparted([]string{"sub"}) // stop the prewarm timer it armed
}

// A fetch racing a drop that gets the brain's "account gone" answer never
// falls back to the copy cached before the drop: a dropped credential is never
// served, stale or not. Each cache holds a still-valid copy that is due for a
// refetch; the brain drops the accounts and answers 404.
func TestAFetchRacingADropNeverServesTheStaleCopy(t *testing.T) {
	tn := addTenant(tenantRow{Tenant: "ten_" + strings.Repeat("g", 26), Name: "stale", Tap: tapMetadata})
	defer removeTenant(tn.id)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tn.dropDeparted([]string{"sub", "gpt", "api"})
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "no such account"})
	}))
	oldBrain, oldKey := brainBase, switchKey
	brainBase, switchKey = srv.URL, "unit-key"
	defer func() {
		srv.Close()
		brainBase, switchKey = oldBrain, oldKey
	}()
	now := time.Now()
	seed := func() {
		tn.tokMu.Lock()
		tn.tokens["sub"] = tokEntry{token: "pre-drop-sub", exp: now.Add(tokenRefreshMargin / 2), fetchedAt: now.Add(-time.Hour)}
		tn.tokMu.Unlock()
		tn.codexTokMu.Lock()
		tn.codexToks["gpt"] = codexTokEntry{val: "pre-drop-gpt", name: "gpt", exp: now.Add(time.Hour),
			fetched: now.Add(-2 * codexTokenFresh)}
		tn.codexTokMu.Unlock()
		tn.apiKeyMu.Lock()
		tn.apiKeys["api"] = apiKeyEntry{val: "pre-drop-api", fetched: now.Add(-2 * apiKeyFresh)}
		tn.apiKeyMu.Unlock()
	}

	seed()
	if got := tn.getToken("sub"); got != "" {
		t.Errorf("claude: served %q after its drop", got)
	}
	seed()
	if got, _, _, ok := tn.getCodexToken("gpt"); ok || got != "" {
		t.Errorf("codex: served %q (ok=%v) after its drop", got, ok)
	}
	seed()
	if got, err := tn.getAPIKey(nil, "api"); err == nil || got != "" {
		t.Errorf("api key: served %q after its drop", got)
	}
}

// A tenant whose document lists kimi while its brain holds no Kimi key for it
// is refused: the request is never signed, with its own key or anyone's.
func TestKimiWithoutTheTenantsKeyIsRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/api-key") {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"account": "kimi", "reason": "no_key"})
			return
		}
		http.NotFound(w, r)
	}))
	oldBrain, oldKey := brainBase, switchKey
	brainBase, switchKey = srv.URL, "unit-key"
	defer func() {
		srv.Close()
		brainBase, switchKey = oldBrain, oldKey
	}()
	// Another tenant holds a cached Kimi key: it must not be reached for.
	other := addTenant(tenantRow{Tenant: "ten_" + strings.Repeat("h", 26), Name: "other", Tap: tapMetadata})
	defer removeTenant(other.id)
	other.apiKeyMu.Lock()
	other.apiKeys["kimi"] = apiKeyEntry{val: "sk-kimi-other", fetched: time.Now()}
	other.apiKeyMu.Unlock()
	tn := addTenant(tenantRow{Tenant: "ten_" + strings.Repeat("i", 26), Name: "keyless", Tap: tapMetadata})
	defer removeTenant(tn.id)
	tn.pref.mu.Lock()
	tn.pref.accounts = map[string]acctStanding{"kimi": {Kind: "provider"}}
	tn.pref.mu.Unlock()

	info := reqInfo{tenant: tn, poolAccount: "kimi"}
	refusal := (bearerWire{}).prepare(&info, nil, "")
	if refusal == "" || info.token != "" {
		t.Fatalf("kimi without the tenant's key: refusal=%q token=%q", refusal, info.token)
	}
	out, _ := http.NewRequest("POST", "http://kimi.invalid/v1/messages", nil)
	out.Header.Set("Authorization", "Bearer client")
	out.Header.Set("X-Api-Key", "client")
	bearerWire{}.apply(out, info)
	if a, k := out.Header.Get("Authorization"), out.Header.Get("X-Api-Key"); a != "" || k != "" {
		t.Errorf("an unkeyed request still carries Authorization=%q X-Api-Key=%q", a, k)
	}
}
