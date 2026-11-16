package main

// Persistence for the health FSM (§7.1): the survivable subset of every
// epState — never the ring, never per-session ramp state (R-45) — written
// temp-file + os.Rename on every transition and on the 60s tick, and read
// once at boot through §4.3's restart column. Same last-good-snapshot
// discipline as classrows.go: every failure logs and degrades to an empty
// store, never a crash.

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"time"
)

type persistedClassEp struct {
	Ep           string `json:"ep"`
	Member       string `json:"member"`
	PrefillsPaid int    `json:"prefills_paid,omitempty"`
}

type persistedKey struct {
	State     string                      `json:"state"`
	K         int                         `json:"k,omitempty"`
	LastK     int                         `json:"last_k,omitempty"`
	Since     time.Time                   `json:"since"`
	Ep        string                      `json:"ep,omitempty"`
	HoldUntil time.Time                   `json:"hold_until"`
	LastClose time.Time                   `json:"last_close"`
	PerClass  map[string]persistedClassEp `json:"per_class,omitempty"`
}

type persistedHealth struct {
	V    int                     `json:"v"`
	Keys map[string]persistedKey `json:"keys"`
}

func (h *healthStore) persistIfDirtyLocked() {
	if h.dirty {
		h.persistLocked()
	}
}

func (h *healthStore) persistLocked() {
	h.dirty = false
	if h.cfg.stateFile == "" {
		return
	}
	out := persistedHealth{V: 1, Keys: make(map[string]persistedKey, len(h.keys))}
	for key, st := range h.keys {
		pk := persistedKey{
			State:     st.state,
			K:         st.k,
			LastK:     st.lastK,
			Since:     st.since,
			Ep:        st.ep,
			HoldUntil: st.holdUntil,
			LastClose: st.lastClose,
		}
		if len(st.perClass) > 0 {
			pk.PerClass = make(map[string]persistedClassEp, len(st.perClass))
			for class, ce := range st.perClass {
				pk.PerClass[class] = persistedClassEp{Ep: ce.ep, Member: ce.member, PrefillsPaid: ce.prefillsPaid}
			}
		}
		out.Keys[key] = pk
	}
	buf, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		log.Printf("health: cannot marshal state: %v", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(h.cfg.stateFile), 0o700); err != nil {
		log.Printf("health: cannot create state dir: %v", err)
		return
	}
	tmp := h.cfg.stateFile + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o600); err != nil {
		log.Printf("health: cannot write state: %v", err)
		return
	}
	if err := os.Rename(tmp, h.cfg.stateFile); err != nil {
		log.Printf("health: cannot install state: %v", err)
	}
}

// loadHealthStore builds a store from the last-good snapshot, pushing every
// key through §4.3's restart column.
func loadHealthStore(cfg healthCfg) *healthStore {
	h := newHealthStore(cfg)
	if cfg.stateFile == "" {
		return h
	}
	raw, err := os.ReadFile(cfg.stateFile)
	if err != nil {
		if !os.IsNotExist(err) { // an absent file is a normal first boot
			log.Printf("health: cannot read state %s: %v — starting empty", cfg.stateFile, err)
		}
		return h
	}
	var p persistedHealth
	if err := json.Unmarshal(raw, &p); err != nil {
		log.Printf("health: bad JSON in %s: %v — starting empty", cfg.stateFile, err)
		return h
	}
	now := nowFn()
	for key, pk := range p.Keys {
		st := restoreKey(cfg, pk, now)
		st.key = key
		h.keys[key] = st
	}
	return h
}

// restoreKey is the restart column of §4.3: healthy stays healthy (the ring
// is gone, evidence re-accumulates); convalescent younger than HOLD_MAX
// reopens with an immediate hold expiry (the next tick probes), older goes
// healthy; open keeps its hold clamped to now+hold(k); half_open falls back
// to open — the probe token never survives a restart.
func restoreKey(cfg healthCfg, pk persistedKey, now time.Time) *epState {
	k := pk.K
	if k < 1 {
		k = 1
	}
	if k > cfg.kMax {
		k = cfg.kMax
	}
	lastK := pk.LastK
	if lastK < 0 {
		lastK = 0
	}
	if lastK > cfg.kMax {
		lastK = cfg.kMax
	}
	st := &epState{
		since:     pk.Since,
		ep:        pk.Ep,
		k:         k,
		lastK:     lastK,
		lastClose: pk.LastClose,
		perClass:  map[string]*classEp{},
	}
	for class, pce := range pk.PerClass {
		st.perClass[class] = &classEp{ep: pce.Ep, member: pce.Member, prefillsPaid: pce.PrefillsPaid, sessions: map[string]*sessRamp{}}
	}
	switch pk.State {
	case hsHealthy:
		st.state = hsHealthy
	case hsConvalescent:
		if now.Sub(pk.Since) < cfg.holdMax {
			st.state = hsOpen
			st.since = now
			st.holdUntil = now
		} else {
			st.state = hsHealthy
			st.since = now
			st.lastK = 0
		}
	case hsOpen, hsHalfOpen:
		st.state = hsOpen
		hu := now.Add(cfg.hold(k))
		if !pk.HoldUntil.IsZero() && pk.HoldUntil.Before(hu) {
			hu = pk.HoldUntil
		}
		st.holdUntil = hu
		if pk.State == hsHalfOpen {
			st.since = now
		}
	default:
		st.state = hsHealthy
		st.since = now
	}
	return st
}
