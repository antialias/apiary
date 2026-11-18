package main

// ---- actuators: how an Anthropic account's credential goes on a request ----
//
// An Anthropic account is one of several kinds (§1.2, the brain's standing
// says which): an api account holds a static key, other kinds a credential
// of their own. The routing chain picks an ACCOUNT and never asks what kind
// it is; the account's actuator gets its credential, signs the outbound
// request with it, and answers the upstream's 401 on it. Core registers the
// api actuator; an edition registers any other kind from its own init(). An
// account whose kind has no actuator is never dialled: the route fails loud
// with a typed 503 naming the kind (writeNoActuator), never the client's
// ambient login.
//
// The credential boundary is per tenant (MEMBERS-PLAN 1b): actuatorFor(tenant,
// account) is the one gate every token, key and door-client path goes
// through, and it refuses an account the tenant's own document does not list
// (errNotTenantAccount, a typed 503 at the request: writeNotTenantAccount).
// Nothing below it can draw on another tenant's subscription, whatever named
// the account.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// errNotTenantAccount is actuatorFor's refusal of an account the tenant's
// document does not list.
var errNotTenantAccount = errors.New("not one of the tenant's accounts")

// The kinds an account's standing names (the brain's _account_kind).
// subscriptionKind is also what an account is before any standing says
// otherwise: the manual inject file's, and an account the brain's document
// does not list.
const (
	subscriptionKind = "subscription"
	apiKind          = "api"
)

// actuator is one kind's credential handling.
type actuator struct {
	// credential is a usable credential for the tenant's account, "" when
	// none can be had (the caller tries the route's next account, then FAILs
	// loud). Only reached through actuatorFor, so account is the tenant's.
	credential func(tn *tenant, account string) string
	// sign puts cred on an outbound Anthropic request's headers.
	sign func(h http.Header, cred string)
	// signed is the credential h carries in this kind's header, "" for none.
	signed func(h http.Header) string
	// accountOf maps a credential this actuator handed out for the tenant
	// back to its account, "" when it is not one of the tenant's.
	accountOf func(tn *tenant, cred string) string
	// refused answers the upstream's 401 on an account's credential; nil
	// returns the 401 as it came.
	refused func(t *failoverTransport, req *http.Request, resp *http.Response) (*http.Response, error)
	// warm is told the accounts of this kind the tenant's last document
	// routes to, every time a document is applied (nil: nothing to warm).
	warm func(tn *tenant, accounts map[string]bool)
}

// actuators is the actuator of each account kind: core's api, and whatever an
// edition's init() adds.
var actuators = map[string]*actuator{}

func init() { actuators[apiKind] = apiActuator }

// apiActuator signs with the account's static key, dispensed by the brain's
// GET /api-key?account=<id> (apikey.go): x-api-key, and no Authorization.
var apiActuator = &actuator{
	credential: func(tn *tenant, account string) string {
		key, err := tn.getAPIKey(nil, account)
		if err != nil {
			return ""
		}
		return key
	},
	sign: func(h http.Header, key string) {
		h.Set("X-Api-Key", key)
		dropOAuthBetas(h)
	},
	signed:    func(h http.Header) string { return h.Get("X-Api-Key") },
	accountOf: apiKeyAccount,
	refused: func(_ *failoverTransport, req *http.Request, resp *http.Response) (*http.Response, error) {
		// A key the upstream refused is forgotten, so the next dial asks the
		// brain again (a rotation is adopted at once). This hop's 401 stands.
		if d, ok := anthDialOf(req); ok {
			d.tenant.dropAPIKey(d.acct)
		}
		return resp, nil
	},
}

// dropOAuthBetas removes every OAuth beta (oauth-*) from h's anthropic-beta,
// the client's included: a key is not an OAuth login, and the upstream
// refuses a key that claims to be one.
func dropOAuthBetas(h http.Header) {
	vals := h.Values("anthropic-beta")
	if len(vals) == 0 {
		return
	}
	var keep []string
	for _, v := range vals {
		for _, f := range strings.Split(v, ",") {
			if f = strings.TrimSpace(f); f != "" && !strings.HasPrefix(f, "oauth-") {
				keep = append(keep, f)
			}
		}
	}
	h.Del("anthropic-beta")
	if len(keep) > 0 {
		h.Set("anthropic-beta", strings.Join(keep, ","))
	}
}

// accountKind is the account's kind as the tenant's standing names it, and
// whether the tenant's document lists the account at all.
func (tn *tenant) accountKind(account string) (kind string, listed bool) {
	tn.pref.mu.RLock()
	st, ok := tn.pref.accounts[account]
	tn.pref.mu.RUnlock()
	if !ok {
		return subscriptionKind, false
	}
	if st.Kind != "" {
		return st.Kind, true
	}
	return subscriptionKind, true
}

// actuatorFor is the tenant's account's actuator, nil when its kind has none
// here, and the kind. The one credential boundary: an account the tenant's
// document does not list is refused with errNotTenantAccount, and nothing is
// fetched, signed or dialled for it. A nil tenant has no accounts.
func (tn *tenant) actuatorFor(account string) (*actuator, string, error) {
	if tn == nil {
		return nil, "", fmt.Errorf("account %s for no tenant: %w", account, errNotTenantAccount)
	}
	if strings.HasPrefix(account, "manual:") {
		// The operator's inject file (SWITCH_INJECT_TOKEN_FILE): a subscription
		// bearer the operator put on this host, not a document's account. The
		// household's alone; no member's request signs with it.
		if !tn.lan {
			return nil, "", fmt.Errorf("account %s is the operator's inject file, not tenant %s's: %w", account, tn.id, errNotTenantAccount)
		}
		return actuators[subscriptionKind], subscriptionKind, nil
	}
	kind, listed := tn.accountKind(account)
	if !listed {
		return nil, "", fmt.Errorf("account %s is not tenant %s's: %w", account, tn.id, errNotTenantAccount)
	}
	return actuators[kind], kind, nil
}

// credentialFor is a usable credential for the tenant's account, "" when none
// can be had, its kind has no actuator or it is not the tenant's.
func (tn *tenant) credentialFor(account string) string {
	if a, _, err := tn.actuatorFor(account); err == nil && a != nil {
		return a.credential(tn, account)
	}
	return ""
}

// signAccount puts account's credential cred on h, by the account's kind,
// after clearing every credential h carried (strip, then sign): a hop cloned
// from another account's, or from the client's request, carries only its
// own account's. A kind with no actuator, or an account not the tenant's,
// leaves h with none.
func (tn *tenant) signAccount(h http.Header, account, cred string) {
	stripCredentials(h)
	if a, _, err := tn.actuatorFor(account); err == nil && a != nil {
		a.sign(h, cred)
	}
}

// stripCredentials removes every Anthropic credential header from h.
func stripCredentials(h http.Header) {
	h.Del("Authorization")
	h.Del("X-Api-Key")
}

// signedCredential is the credential h carries for account's kind, "" for none.
func (tn *tenant) signedCredential(h http.Header, account string) string {
	if a, _, err := tn.actuatorFor(account); err == nil && a != nil {
		return a.signed(h)
	}
	return ""
}

// accountOfCredential maps a credential back to the tenant's account it was
// handed out for, by any actuator; "" when it is not one of the tenant's.
func (tn *tenant) accountOfCredential(cred string) string {
	if cred = strings.TrimSpace(cred); cred == "" {
		return ""
	}
	for _, a := range actuators {
		if acct := a.accountOf(tn, cred); acct != "" {
			return acct
		}
	}
	return ""
}

// accountOfHop is the tenant's account whose credential an outbound Anthropic
// hop carries, "" when none of the tenant's.
func (tn *tenant) accountOfHop(h http.Header) string {
	for _, a := range actuators {
		if c := strings.TrimSpace(a.signed(h)); c != "" {
			if acct := a.accountOf(tn, c); acct != "" {
				return acct
			}
		}
	}
	return ""
}

// warmAccounts hands each actuator the tenant's routed accounts of its kind
// (an empty set included: a warm set that emptied must stop what it armed).
func (tn *tenant) warmAccounts(warm map[string]bool) {
	byKind := map[string]map[string]bool{}
	for a := range warm {
		k, _ := tn.accountKind(a)
		if byKind[k] == nil {
			byKind[k] = map[string]bool{}
		}
		byKind[k][a] = true
	}
	for kind, a := range actuators {
		if a.warm == nil {
			continue
		}
		set := byKind[kind]
		if set == nil {
			set = map[string]bool{}
		}
		a.warm(tn, set)
	}
}

// apiKeyAccount maps a dispensed key back to the tenant's account.
func apiKeyAccount(tn *tenant, key string) string {
	tn.apiKeyMu.Lock()
	defer tn.apiKeyMu.Unlock()
	for a, e := range tn.apiKeys {
		if e.val != "" && e.val == key {
			return a
		}
	}
	return ""
}

// writeNoActuator answers a lane whose account is of a kind this build has no
// actuator for: HTTP 503 in the Anthropic error envelope, naming the kind.
func writeNoActuator(w http.ResponseWriter, tn *tenant, eff, kind string) {
	msg := product + ": account " + eff + " is of kind " + kind +
		", which this build has no actuator for — refusing to forward your ambient login. " + checkHint(tn, "Check the brain")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":  "error",
		"error": map[string]any{"type": "api_error", "message": msg},
	})
}

// writeNotTenantAccount answers a request that named an account the tenant's
// document does not list: HTTP 503 in the Anthropic error envelope, naming
// the account and the tenant. Nothing was fetched for it.
func writeNotTenantAccount(w http.ResponseWriter, tn *tenant, account string) {
	id := "no tenant"
	if tn != nil {
		id = "tenant " + tn.id
	}
	msg := product + ": account " + account + " is not " + id + "'s — refusing to draw on it. " + checkHint(tn, "Check the brain")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":  "error",
		"error": map[string]any{"type": "api_error", "message": msg},
	})
}
