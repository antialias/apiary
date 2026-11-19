package main

// ---- dispensed static keys (quota-model C8) ----
//
// An api-kind account's key (Runway's is the first) lives on the brain alone:
// the brain dispenses it by GET /api-key?account=<id>, X-Switch-Key guarded,
// as it does the codex bearer (/openai-token) and Anthropic's (/token). The
// proxy holds no upstream key of its own. It caches each account's key the
// way it caches a codex bearer: used without asking again for apiKeyFresh,
// and kept (STALE) while the brain cannot be reached. A key has no expiry,
// so a cached one stays usable until the brain says it has none.
//
// A key the proxy cannot get is a named state, not a refusal: the brain has
// no key for the account, or the brain cannot be reached and nothing is
// cached. The member is then unusable, with that reason (memberUnroutable,
// the walk's skip line, /_switch/status), and the state is logged once per
// change. No record is written: no provider said anything.
//
// The fetch runs on its caller's context: a client that left, or a stop,
// never waits on the brain. An ask cut short by its caller says nothing about
// the key: it changes neither the cache nor the state.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"time"
)

// apiKeyEntry is an account's last answer from the brain: its key, or
// (val "") the brain's word that it has none.
type apiKeyEntry struct {
	val     string
	none    string
	fetched time.Time
}

// Per tenant: tenant.apiKeyMu, tenant.apiKeys (the keys in hand),
// tenant.apiKeyStates (each account's last state, "" = a key in hand, else
// why none: "missing", "fetch failed"), tenant.apiKeyDropped (the
// fingerprint of each account's key last dropped because its provider
// refused it), tenant.apiKeyFails (each account's last ask the brain did not
// answer, for apiKeyRetry) and tenant.apiKeyFlights (each account's ask in
// flight). apiKeyMu is a leaf: it may be taken under recMu, never the other
// way round.

// apiKeyFresh is how long the brain's answer is used without asking again: a
// rotation, a key placed and a key withdrawn are each adopted within it.
var apiKeyFresh = 5 * time.Minute

// apiKeyRetry is how long an ask the brain did not answer stands before the
// brain is asked again: meanwhile the cached key, stale or not, serves, and
// with none the ask's error is the answer. A brain that hangs costs each
// account one wait (brainClient's timeout) per apiKeyRetry, whatever the
// number of requests and of asks in each (a class walk asks each keyed
// member's key before its checks and again as it dials).
var apiKeyRetry = 30 * time.Second

// apiKeyFail is an ask of the brain that failed: when, and the error a caller
// with no cached key is given.
type apiKeyFail struct {
	at  time.Time
	why string
}

// apiKeyFlight is one account's ask of the brain in flight: every caller that
// comes while it runs waits for its answer rather than asking again. retry:
// its answer is not one a waiter may take (the ask was cut short by the
// caller that made it, or a drop began during it), so a waiter asks anew.
type apiKeyFlight struct {
	done  chan struct{}
	key   string
	err   error
	retry bool
}

// getAPIKey is the account's key, or an error that names why there is none.
// ctx is the caller's (nil: it has none).
func (tn *tenant) getAPIKey(ctx context.Context, account string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		// Read before the cache: a drop after it moves the count, so neither a
		// fetch's answer nor the copy read here is kept or served stale.
		gen := tn.credDrops.Load()
		tn.apiKeyMu.Lock()
		e, has := tn.apiKeys[account]
		now := time.Now()
		if has && now.Sub(e.fetched) < apiKeyFresh {
			tn.apiKeyMu.Unlock()
			if e.val == "" {
				return "", errors.New(e.none)
			}
			return e.val, nil
		}
		if f, failed := tn.apiKeyFails[account]; failed && now.Sub(f.at) < apiKeyRetry {
			tn.apiKeyMu.Unlock()
			if has && e.val != "" {
				return e.val, nil // STALE: the brain did not answer lately
			}
			return "", errors.New(f.why)
		}
		if fl := tn.apiKeyFlights[account]; fl != nil {
			tn.apiKeyMu.Unlock()
			select {
			case <-fl.done:
			case <-ctx.Done():
				return "", fmt.Errorf("the ask for the api key was cut short: %w", ctx.Err())
			}
			if fl.retry {
				continue
			}
			return fl.key, fl.err
		}
		fl := &apiKeyFlight{done: make(chan struct{})}
		if tn.apiKeyFlights == nil {
			tn.apiKeyFlights = map[string]*apiKeyFlight{}
		}
		tn.apiKeyFlights[account] = fl
		tn.apiKeyMu.Unlock()
		tn.flyAPIKey(ctx, fl, account, gen, e, has)
		return fl.key, fl.err
	}
}

// flyAPIKey is the leader's ask for its flight. The flight ends however the
// ask does: were it to panic (net/http recovers a handler), a flight left
// registered would hold every later caller for that account, and retry (true
// until the ask returns) sends its waiters to ask again.
func (tn *tenant) flyAPIKey(ctx context.Context, fl *apiKeyFlight, account string, gen uint64, e apiKeyEntry, has bool) {
	fl.retry = true
	defer func() {
		tn.apiKeyMu.Lock()
		delete(tn.apiKeyFlights, account)
		tn.apiKeyMu.Unlock()
		close(fl.done)
	}()
	fl.key, fl.retry, fl.err = tn.askAPIKey(ctx, account, gen, e, has)
}

// askAPIKey asks the brain for account's key and keeps its answer; e (has)
// is the cache's entry as getAPIKey read it, at gen. retry: the answer is
// this caller's alone (cut short, or a drop began during the ask).
func (tn *tenant) askAPIKey(ctx context.Context, account string, gen uint64, e apiKeyEntry, has bool) (string, bool, error) {
	key, missing, err := tn.fetchAPIKey(ctx, account)
	if err != nil && ctx.Err() != nil {
		return "", true, fmt.Errorf("the ask for the api key was cut short: %w", ctx.Err())
	}
	now := time.Now()
	tn.apiKeyMu.Lock()
	defer tn.apiKeyMu.Unlock()
	if tn.credDrops.Load() != gen {
		// A drop began since this fetch did: answer it once, cache nothing,
		// and never fall back to the copy read before it.
		if err != nil {
			return "", true, err
		}
		return key, true, nil
	}
	if err == nil || missing {
		delete(tn.apiKeyFails, account)
	}
	switch {
	case err == nil:
		tn.apiKeys[account] = apiKeyEntry{val: key, fetched: now}
		if tn.apiKeyDropped[account] != apiKeyFP(key) {
			delete(tn.apiKeyDropped, account) // another key: the refused one is history
		}
		tn.noteAPIKeyState(account, "")
		return key, false, nil
	case missing:
		// The brain answered that it has no key: a cached one is withdrawn.
		tn.apiKeys[account] = apiKeyEntry{none: err.Error(), fetched: now}
	default:
		err = fmt.Errorf("the brain's /api-key is unreachable and no key is cached (%v)", err)
		if tn.apiKeyFails == nil {
			tn.apiKeyFails = map[string]apiKeyFail{}
		}
		tn.apiKeyFails[account] = apiKeyFail{at: now, why: err.Error()}
		if has && e.val != "" {
			tn.noteAPIKeyState(account, "")
			return e.val, false, nil // STALE: the brain is unreachable, the key is still the key
		}
	}
	tn.noteAPIKeyState(account, err.Error())
	return "", false, err
}

// fetchAPIKey asks the brain. missing: the brain answered that it has none
// (its 404), as against an error reaching it.
func (tn *tenant) fetchAPIKey(ctx context.Context, account string) (key string, missing bool, err error) {
	if tn.brainURL() == "" {
		return "", false, errors.New("no brain configured")
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", tn.brainURL()+"/api-key?account="+url.QueryEscape(account), nil)
	req.Header.Set("X-Switch-Key", switchKey)
	resp, err := brainClient.Do(req)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()
	var d struct {
		APIKey string `json:"api_key"`
		Error  string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&d)
	switch {
	case resp.StatusCode == http.StatusOK && d.APIKey != "":
		return d.APIKey, false, nil
	case resp.StatusCode == http.StatusNotFound:
		if d.Error == "" {
			d.Error = "the brain has no key for account " + account
		}
		return "", true, errors.New(d.Error)
	}
	return "", false, fmt.Errorf("brain /api-key %d", resp.StatusCode)
}

// noteAPIKeyState logs an account's key state when it changes. Under apiKeyMu.
func (tn *tenant) noteAPIKeyState(account, state string) {
	prev, seen := tn.apiKeyStates[account]
	if seen && prev == state {
		return
	}
	tn.apiKeyStates[account] = state
	if state == "" {
		if seen {
			log.Printf("✅ api key for %s: dispensed by the brain", account)
		}
		return
	}
	log.Printf("🚨 api key for %s: none (%s); its members are unusable", account, state)
}

// dropAPIKey forgets an account's cached key (the provider refused it), so
// the next dial asks the brain again. The refused key's fingerprint is kept
// (apiKeySentFP): the record the refusal writes is that key's.
func (tn *tenant) dropAPIKey(account string) {
	tn.apiKeyMu.Lock()
	if e, ok := tn.apiKeys[account]; ok && e.val != "" {
		tn.apiKeyDropped[account] = apiKeyFP(e.val)
	}
	delete(tn.apiKeys, account)
	tn.apiKeyMu.Unlock()
}

// apiKeyFP is a key's fingerprint, to tell one key from the next: what a
// refusal record of the key keeps. Never logged or shown.
func apiKeyFP(key string) string { return fp("Bearer " + key) }

// apiKeyNowFP is the fingerprint of account's key in hand, "" when none is
// cached (never fetched, dropped, or the brain has none).
func (tn *tenant) apiKeyNowFP(account string) string {
	tn.apiKeyMu.Lock()
	defer tn.apiKeyMu.Unlock()
	if e, ok := tn.apiKeys[account]; ok && e.val != "" {
		return apiKeyFP(e.val)
	}
	return ""
}

// apiKeySentFP is the fingerprint of the key a refusal of account's key
// refused: the one dropped for it (every path that reads a refused key drops
// it before the refusal is recorded), else the one in hand ("" when neither
// is known). A refusal of the key is recorded against it.
func (tn *tenant) apiKeySentFP(account string) string {
	tn.apiKeyMu.Lock()
	defer tn.apiKeyMu.Unlock()
	if d := tn.apiKeyDropped[account]; d != "" {
		return d
	}
	if e, ok := tn.apiKeys[account]; ok && e.val != "" {
		return apiKeyFP(e.val)
	}
	return ""
}

// apiKeyStatus is /_switch/status's "api_keys": each account asked for, its
// state and, when it has no key, why. Never the key.
func (tn *tenant) apiKeyStatus() []map[string]any {
	tn.apiKeyMu.Lock()
	defer tn.apiKeyMu.Unlock()
	names := make([]string, 0, len(tn.apiKeyStates))
	for a := range tn.apiKeyStates {
		names = append(names, a)
	}
	sort.Strings(names)
	out := make([]map[string]any, 0, len(names))
	for _, a := range names {
		row := map[string]any{"account": a, "state": "ok"}
		if why := tn.apiKeyStates[a]; why != "" {
			row["state"], row["reason"] = "missing", why
		}
		if e, ok := tn.apiKeys[a]; ok && e.val != "" {
			row["fetched_at"] = e.fetched.UTC().Format(time.RFC3339)
		}
		out = append(out, row)
	}
	return out
}
