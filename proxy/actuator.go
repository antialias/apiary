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

import (
	"encoding/json"
	"net/http"
	"strings"
)

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
	// credential is a usable credential for account, "" when none can be had
	// (the caller tries the route's next account, then FAILs loud).
	credential func(account string) string
	// sign puts cred on an outbound Anthropic request's headers.
	sign func(h http.Header, cred string)
	// signed is the credential h carries in this kind's header, "" for none.
	signed func(h http.Header) string
	// accountOf maps a credential this actuator handed out back to its
	// account, "" when it is not one of ours.
	accountOf func(cred string) string
	// refused answers the upstream's 401 on an account's credential; nil
	// returns the 401 as it came.
	refused func(t *failoverTransport, req *http.Request, resp *http.Response) (*http.Response, error)
	// warm is told the accounts of this kind the brain's last document routes
	// to, every time a document is applied (nil: nothing to warm).
	warm func(accounts map[string]bool)
}

// actuators is the actuator of each account kind: core's api, and whatever an
// edition's init() adds.
var actuators = map[string]*actuator{}

func init() { actuators[apiKind] = apiActuator }

// apiActuator signs with the account's static key, dispensed by the brain's
// GET /api-key?account=<id> (apikey.go): x-api-key, and no Authorization.
var apiActuator = &actuator{
	credential: func(account string) string {
		key, err := getAPIKey(nil, account)
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
			dropAPIKey(d.acct)
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

// accountKind is the account's kind as the brain's standing names it.
func accountKind(account string) string {
	pref.mu.RLock()
	st, ok := pref.accounts[account]
	pref.mu.RUnlock()
	if ok && st.Kind != "" {
		return st.Kind
	}
	return subscriptionKind
}

// actuatorFor is the account's actuator, nil when its kind has none here, and
// the kind.
func actuatorFor(account string) (*actuator, string) {
	kind := accountKind(account)
	return actuators[kind], kind
}

// credentialFor is a usable credential for account, "" when none can be had
// or its kind has no actuator.
func credentialFor(account string) string {
	if a, _ := actuatorFor(account); a != nil {
		return a.credential(account)
	}
	return ""
}

// signAccount puts account's credential cred on h, by the account's kind,
// after clearing every credential h carried (strip, then sign): a hop cloned
// from another account's, or from the client's request, carries only its
// own account's. A kind with no actuator leaves h with none.
func signAccount(h http.Header, account, cred string) {
	stripCredentials(h)
	if a, _ := actuatorFor(account); a != nil {
		a.sign(h, cred)
	}
}

// stripCredentials removes every Anthropic credential header from h.
func stripCredentials(h http.Header) {
	h.Del("Authorization")
	h.Del("X-Api-Key")
}

// signedCredential is the credential h carries for account's kind, "" for none.
func signedCredential(h http.Header, account string) string {
	if a, _ := actuatorFor(account); a != nil {
		return a.signed(h)
	}
	return ""
}

// accountOfCredential maps a credential back to the account it was handed
// out for, by any actuator; "" when it is not one of ours.
func accountOfCredential(cred string) string {
	if cred = strings.TrimSpace(cred); cred == "" {
		return ""
	}
	for _, a := range actuators {
		if acct := a.accountOf(cred); acct != "" {
			return acct
		}
	}
	return ""
}

// accountOfHop is the account whose credential an outbound Anthropic hop
// carries, "" when none of ours.
func accountOfHop(h http.Header) string {
	for _, a := range actuators {
		if c := strings.TrimSpace(a.signed(h)); c != "" {
			if acct := a.accountOf(c); acct != "" {
				return acct
			}
		}
	}
	return ""
}

// warmAccounts hands each actuator the routed accounts of its kind (an empty
// set included: a warm set that emptied must stop what it armed).
func warmAccounts(warm map[string]bool) {
	byKind := map[string]map[string]bool{}
	for a := range warm {
		k := accountKind(a)
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
		a.warm(set)
	}
}

// apiKeyAccount maps a dispensed key back to its account.
func apiKeyAccount(key string) string {
	apiKeyMu.Lock()
	defer apiKeyMu.Unlock()
	for a, e := range apiKeys {
		if e.val != "" && e.val == key {
			return a
		}
	}
	return ""
}

// writeNoActuator answers a lane whose account is of a kind this build has no
// actuator for: HTTP 503 in the Anthropic error envelope, naming the kind.
func writeNoActuator(w http.ResponseWriter, eff, kind string) {
	msg := product + ": account " + eff + " is of kind " + kind +
		", which this build has no actuator for — refusing to forward your ambient login. Check the brain: curl " + statusHint()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":  "error",
		"error": map[string]any{"type": "api_error", "message": msg},
	})
}
