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

// Per tenant: tenant.apiKeyMu, tenant.apiKeys (the keys in hand) and
// tenant.apiKeyStates (each account's last state, "" = a key in hand, else
// why none: "missing", "fetch failed").

// apiKeyFresh is how long the brain's answer is used without asking again: a
// rotation, a key placed and a key withdrawn are each adopted within it.
var apiKeyFresh = 5 * time.Minute

// getAPIKey is the account's key, or an error that names why there is none.
// ctx is the caller's (nil: it has none).
func (tn *tenant) getAPIKey(ctx context.Context, account string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Read before the cache: a drop after it moves the count, so neither a
	// fetch's answer nor the copy read here is kept or served stale.
	gen := tn.credDrops.Load()
	tn.apiKeyMu.Lock()
	e, has := tn.apiKeys[account]
	tn.apiKeyMu.Unlock()
	now := time.Now()
	if has && now.Sub(e.fetched) < apiKeyFresh {
		if e.val == "" {
			return "", errors.New(e.none)
		}
		return e.val, nil
	}
	key, missing, err := tn.fetchAPIKey(ctx, account)
	if err != nil && ctx.Err() != nil {
		return "", fmt.Errorf("the ask for the api key was cut short: %w", ctx.Err())
	}
	tn.apiKeyMu.Lock()
	defer tn.apiKeyMu.Unlock()
	if tn.credDrops.Load() != gen {
		// A drop began since this fetch did: answer it once, cache nothing,
		// and never fall back to the copy read before it.
		if err != nil {
			return "", err
		}
		return key, nil
	}
	switch {
	case err == nil:
		tn.apiKeys[account] = apiKeyEntry{val: key, fetched: now}
		tn.noteAPIKeyState(account, "")
		return key, nil
	case missing:
		// The brain answered that it has no key: a cached one is withdrawn.
		tn.apiKeys[account] = apiKeyEntry{none: err.Error(), fetched: now}
	case has && e.val != "":
		tn.noteAPIKeyState(account, "")
		return e.val, nil // STALE: the brain is unreachable, the key is still the key
	default:
		err = fmt.Errorf("the brain's /api-key is unreachable and no key is cached (%v)", err)
	}
	tn.noteAPIKeyState(account, err.Error())
	return "", err
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
// the next dial asks the brain again.
func (tn *tenant) dropAPIKey(account string) {
	tn.apiKeyMu.Lock()
	delete(tn.apiKeys, account)
	tn.apiKeyMu.Unlock()
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
