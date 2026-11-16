package main

// U-suite for the #121 health FSM (spec §10). U0 is GENERATED: every cell of
// the 4-state × 9-input table is a closure in u0Cells, and the test loops the
// full product asserting no cell is missing — none are hand-enumerated into a
// list that could silently shrink. The clock is nowFn swapped for a fake
// everywhere; no test sleeps.

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const tKey = "gpt/gpt-5.6-sol"

// ---- clock and sample helpers ----

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func testClock(t *testing.T) *fakeClock {
	t.Helper()
	c := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	old := nowFn
	nowFn = c.now
	t.Cleanup(func() { nowFn = old })
	return c
}

var uniqN atomic.Int64

func uniq(p string) string { return fmt.Sprintf("%s-%d", p, uniqN.Add(1)) }

func degS(req string, fp uint64) sample {
	return sample{ok: false, cause: "upstream_5xx", class: "helpdesk", reqID: req, fp: fp}
}

func okS(req string) sample { return sample{ok: true, class: "helpdesk", reqID: req} }

func newHS() *healthStore { return newHealthStore(defaultHealthCfg()) }

func stateOf(h *healthStore, key string) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.keys[key]
	if st == nil {
		return "", false
	}
	return st.state, true
}

func mustState(t *testing.T, h *healthStore, key, want string) {
	t.Helper()
	got, ok := stateOf(h, key)
	if !ok {
		t.Fatalf("no epState for %s (want %s)", key, want)
	}
	if got != want {
		t.Fatalf("state = %s, want %s", got, want)
	}
}

func snap(t *testing.T, h *healthStore, key string) epState {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.keys[key]
	if st == nil {
		t.Fatalf("no epState for %s", key)
	}
	return *st
}

func ringLen(h *healthStore, key string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.keys[key]
	if st == nil {
		return 0
	}
	return len(st.ring)
}

// trip drives a healthy tKey to open: one ok then three degs with distinct
// shapes, each in its own admission bucket.
func trip(t *testing.T, h *healthStore, c *fakeClock) {
	t.Helper()
	h.record(tKey, okS(uniq("ok")))
	c.advance(6 * time.Second)
	for i := 0; i < 3; i++ {
		h.record(tKey, degS(uniq("deg"), uint64(101+i)))
		c.advance(6 * time.Second)
	}
	mustState(t, h, tKey, hsOpen)
}

func toHalfOpen(t *testing.T, h *healthStore, c *fakeClock) {
	t.Helper()
	trip(t, h, c)
	c.advance(61 * time.Second)
	h.tick()
	mustState(t, h, tKey, hsHalfOpen)
}

func probeOKOnce(t *testing.T, h *healthStore, c *fakeClock) {
	t.Helper()
	l := h.claimProbe(tKey, "helpdesk", "gpt-5.6-sol", uniq("probe"), 1000)
	if l == nil {
		t.Fatal("claimProbe returned nil")
	}
	h.settleLease(l, outcomeProbeOK, sample{reqID: l.id, class: "helpdesk"})
}

func toConv(t *testing.T, h *healthStore, c *fakeClock) {
	t.Helper()
	toHalfOpen(t, h, c)
	probeOKOnce(t, h, c)
	c.advance(6 * time.Second)
	probeOKOnce(t, h, c)
	mustState(t, h, tKey, hsConvalescent)
}

// staleLease fabricates a lease from a dead episode: wrong ep, unknown id.
func staleLease(key string) *probeLease {
	return &probeLease{key: key, ep: key + "@1", id: uniq("stale"), class: "helpdesk"}
}

// ---- U0: the generated 36-cell transition table ----

var u0States = []string{hsHealthy, hsConvalescent, hsOpen, hsHalfOpen}
var u0Inputs = []string{"ok", "deg", "probe_ok", "probe_deg", "probe_abandoned", "tick", "restart", "member_absent", "member_present"}

func TestU0TransitionTable(t *testing.T) {
	cells := u0Cells()
	for s, row := range cells {
		for in := range row {
			if !contains(u0States, s) || !contains(u0Inputs, in) {
				t.Fatalf("u0Cells has a cell outside the table: %s × %s", s, in)
			}
		}
	}
	for _, s := range u0States {
		for _, in := range u0Inputs {
			fn := cells[s][in]
			if fn == nil {
				t.Fatalf("U0 table incomplete: no cell for %s × %s", s, in)
			}
			t.Run(s+"/"+in, fn)
		}
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func u0Cells() map[string]map[string]func(*testing.T) {
	return map[string]map[string]func(*testing.T){
		hsHealthy: {
			"ok": func(t *testing.T) {
				testClock(t)
				h := newHS()
				h.record(tKey, okS(uniq("ok")))
				mustState(t, h, tKey, hsHealthy)
				if n := ringLen(h, tKey); n != 1 {
					t.Fatalf("ring = %d, want 1", n)
				}
			},
			"deg": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				trip(t, h, c)
				st := snap(t, h, tKey)
				if st.k != 1 {
					t.Fatalf("k = %d, want 1", st.k)
				}
				if !strings.HasPrefix(st.ep, tKey+"@") {
					t.Fatalf("ep = %q, want %s@<ts>", st.ep, tKey)
				}
				if d := st.holdUntil.Sub(st.since); d != 60*time.Second {
					t.Fatalf("hold = %v, want 60s", d)
				}
				h.mu.Lock()
				eps, tr := h.episodes[tKey], h.transitions[[3]string{tKey, hsHealthy, hsOpen}]
				h.mu.Unlock()
				if eps != 1 || tr != 1 {
					t.Fatalf("episodes=%d transitions=%d, want 1/1", eps, tr)
				}
			},
			"probe_ok": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				h.record(tKey, okS(uniq("ok")))
				c.advance(6 * time.Second) // fresh bucket, or rule 1 collapses the pair
				h.settleLease(staleLease(tKey), outcomeProbeOK, sample{reqID: uniq("sp"), class: "helpdesk"})
				mustState(t, h, tKey, hsHealthy)
				if n := ringLen(h, tKey); n != 2 {
					t.Fatalf("stale probe_ok not scored as ordinary ok: ring = %d, want 2", n)
				}
			},
			"probe_deg": func(t *testing.T) {
				// A stale probe_deg is ordinary deg evidence: it can complete
				// arm C and trip the key.
				c := testClock(t)
				h := newHS()
				h.record(tKey, okS(uniq("ok")))
				c.advance(6 * time.Second)
				h.record(tKey, degS(uniq("d"), 1))
				c.advance(6 * time.Second)
				h.record(tKey, degS(uniq("d"), 2))
				c.advance(6 * time.Second)
				mustState(t, h, tKey, hsHealthy)
				h.settleLease(staleLease(tKey), outcomeProbeDeg, sample{reqID: uniq("sp"), class: "helpdesk", fp: 3})
				mustState(t, h, tKey, hsOpen)
				if st := snap(t, h, tKey); st.k != 1 {
					t.Fatalf("k = %d, want 1", st.k)
				}
			},
			"probe_abandoned": func(t *testing.T) {
				testClock(t)
				h := newHS()
				h.record(tKey, okS(uniq("ok")))
				h.settleLease(staleLease(tKey), outcomeProbeAbandoned, sample{})
				mustState(t, h, tKey, hsHealthy)
				if n := ringLen(h, tKey); n != 1 {
					t.Fatalf("stale abandon must add no evidence: ring = %d, want 1", n)
				}
			},
			"tick": func(t *testing.T) {
				testClock(t)
				h := newHS()
				h.record(tKey, okS(uniq("ok")))
				h.tick()
				mustState(t, h, tKey, hsHealthy) // no-op while present
				h.memberAbsent(tKey)
				h.tick() // healthy+absent → GC at once
				if _, ok := stateOf(h, tKey); ok {
					t.Fatal("healthy absent key survived tick GC")
				}
			},
			"restart": func(t *testing.T) {
				c := testClock(t)
				cfg := defaultHealthCfg()
				cfg.stateFile = t.TempDir() + "/health.json"
				h := newHealthStore(cfg)
				h.record(tKey, okS(uniq("ok")))
				h.mu.Lock()
				h.persistLocked()
				h.mu.Unlock()
				c.advance(time.Hour)
				h2 := loadHealthStore(cfg)
				mustState(t, h2, tKey, hsHealthy)
			},
			"member_absent": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				h.record(tKey, okS(uniq("ok")))
				h.memberAbsent(tKey)
				st := snap(t, h, tKey)
				if !st.absent || !st.absentSince.Equal(c.now()) {
					t.Fatalf("absent=%v absentSince=%v, want true/%v", st.absent, st.absentSince, c.now())
				}
			},
			"member_present": func(t *testing.T) {
				testClock(t)
				h := newHS()
				h.record(tKey, okS(uniq("ok")))
				h.memberAbsent(tKey)
				h.memberPresent(tKey)
				st := snap(t, h, tKey)
				if st.absent {
					t.Fatal("absent still set after member_present")
				}
				h.tick()
				if _, ok := stateOf(h, tKey); !ok {
					t.Fatal("present key was GCd")
				}
			},
		},
		hsConvalescent: {
			"ok": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				toConv(t, h, c)
				h.record(tKey, okS(uniq("ok")))
				mustState(t, h, tKey, hsConvalescent) // window not elapsed
				c.advance(601 * time.Second)
				h.record(tKey, okS(uniq("ok")))
				mustState(t, h, tKey, hsHealthy)
				if st := snap(t, h, tKey); st.lastK != 0 {
					t.Fatalf("lastK = %d, want 0 after healthy", st.lastK)
				}
			},
			"deg": func(t *testing.T) {
				// I2/U7: a re-trip inside the convalescent window resumes the
				// ladder at lastK+1.
				c := testClock(t)
				h := newHS()
				toConv(t, h, c)
				oldEp := snap(t, h, tKey).ep
				for i := 0; i < 3; i++ {
					c.advance(6 * time.Second)
					h.record(tKey, degS(uniq("d"), uint64(201+i)))
				}
				mustState(t, h, tKey, hsOpen)
				st := snap(t, h, tKey)
				if st.k != 2 {
					t.Fatalf("k = %d, want lastK+1 = 2", st.k)
				}
				if st.ep == oldEp {
					t.Fatal("re-trip did not mint a new episode")
				}
				if d := st.holdUntil.Sub(st.since); d != 120*time.Second {
					t.Fatalf("hold = %v, want 120s", d)
				}
			},
			"probe_ok": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				toConv(t, h, c)
				n := ringLen(h, tKey)
				c.advance(6 * time.Second)
				h.settleLease(staleLease(tKey), outcomeProbeOK, sample{reqID: uniq("sp"), class: "helpdesk"})
				mustState(t, h, tKey, hsConvalescent)
				if ringLen(h, tKey) != n+1 {
					t.Fatal("stale probe_ok not admitted as ordinary ok")
				}
			},
			"probe_deg": func(t *testing.T) {
				// The stale settle is the third consecutive deg: arm S trips
				// at lastK+1 exactly as an ordinary deg would.
				c := testClock(t)
				h := newHS()
				toConv(t, h, c)
				c.advance(700 * time.Second) // age the old evidence out; no tick, so still convalescent
				h.record(tKey, degS(uniq("d"), 201))
				c.advance(6 * time.Second)
				h.record(tKey, degS(uniq("d"), 202))
				c.advance(6 * time.Second)
				mustState(t, h, tKey, hsConvalescent)
				h.settleLease(staleLease(tKey), outcomeProbeDeg, sample{reqID: uniq("sp"), class: "helpdesk", fp: 203})
				mustState(t, h, tKey, hsOpen)
				if st := snap(t, h, tKey); st.k != 2 {
					t.Fatalf("k = %d, want lastK+1 = 2", st.k)
				}
			},
			"probe_abandoned": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				toConv(t, h, c)
				n := ringLen(h, tKey)
				c.advance(6 * time.Second)
				h.settleLease(staleLease(tKey), outcomeProbeAbandoned, sample{})
				mustState(t, h, tKey, hsConvalescent)
				if ringLen(h, tKey) != n {
					t.Fatal("stale abandon added evidence")
				}
			},
			"tick": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				toConv(t, h, c)
				h.tick()
				mustState(t, h, tKey, hsConvalescent)
				c.advance(601 * time.Second)
				h.tick()
				mustState(t, h, tKey, hsHealthy)
				if st := snap(t, h, tKey); st.lastK != 0 {
					t.Fatalf("lastK = %d, want 0", st.lastK)
				}
			},
			"restart": func(t *testing.T) {
				// Young convalescent reopens with an immediate hold expiry;
				// old convalescent goes straight to healthy.
				c := testClock(t)
				cfg := defaultHealthCfg()
				cfg.stateFile = t.TempDir() + "/health.json"
				h := newHealthStore(cfg)
				toConv(t, h, c)
				h.mu.Lock()
				h.persistLocked()
				h.mu.Unlock()
				c.advance(100 * time.Second)
				h2 := loadHealthStore(cfg)
				mustState(t, h2, tKey, hsOpen)
				st := snap(t, h2, tKey)
				if !st.holdUntil.Equal(c.now()) {
					t.Fatalf("holdUntil = %v, want now (%v)", st.holdUntil, c.now())
				}
				c.advance(1801 * time.Second) // past HOLD_MAX since the close
				h3 := loadHealthStore(cfg)
				mustState(t, h3, tKey, hsHealthy)
				if st := snap(t, h3, tKey); st.lastK != 0 {
					t.Fatalf("lastK = %d, want 0", st.lastK)
				}
			},
			"member_absent": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				toConv(t, h, c)
				h.memberAbsent(tKey)
				c.advance(700 * time.Second)
				h.tick()
				mustState(t, h, tKey, hsConvalescent) // frozen: no heal while absent
			},
			"member_present": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				toConv(t, h, c)
				h.memberAbsent(tKey)
				c.advance(700 * time.Second)
				h.memberPresent(tKey)
				h.tick()
				mustState(t, h, tKey, hsHealthy) // expiry resumes on return
			},
		},
		hsOpen: {
			"ok": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				trip(t, h, c)
				n := ringLen(h, tKey)
				h.record(tKey, okS(uniq("ok")))
				mustState(t, h, tKey, hsOpen)
				if ringLen(h, tKey) != n+1 {
					t.Fatal("open ok not admitted")
				}
			},
			"deg": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				trip(t, h, c)
				h.record(tKey, degS(uniq("d"), 300))
				mustState(t, h, tKey, hsOpen)
				if st := snap(t, h, tKey); st.k != 1 {
					t.Fatalf("open deg advanced k to %d", st.k)
				}
			},
			"probe_ok": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				trip(t, h, c)
				n := ringLen(h, tKey)
				h.settleLease(staleLease(tKey), outcomeProbeOK, sample{reqID: uniq("sp"), class: "helpdesk"})
				mustState(t, h, tKey, hsOpen)
				if ringLen(h, tKey) != n+1 {
					t.Fatal("stale probe_ok not admitted")
				}
			},
			"probe_deg": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				trip(t, h, c)
				h.settleLease(staleLease(tKey), outcomeProbeDeg, sample{reqID: uniq("sp"), class: "helpdesk", fp: 301})
				mustState(t, h, tKey, hsOpen)
				if st := snap(t, h, tKey); st.k != 1 {
					t.Fatalf("stale probe_deg advanced k to %d", st.k)
				}
			},
			"probe_abandoned": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				trip(t, h, c)
				n := ringLen(h, tKey)
				h.settleLease(staleLease(tKey), outcomeProbeAbandoned, sample{})
				mustState(t, h, tKey, hsOpen)
				if ringLen(h, tKey) != n {
					t.Fatal("stale abandon added evidence")
				}
			},
			"tick": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				trip(t, h, c) // now = open+18s... trip advances 6s after the last deg
				h.tick()
				mustState(t, h, tKey, hsOpen) // hold not expired
				c.advance(61 * time.Second)
				h.tick()
				mustState(t, h, tKey, hsHalfOpen)
				st := snap(t, h, tKey)
				if !st.probeTok || st.probeID != "" || st.probeOK != 0 {
					t.Fatalf("token not minted cleanly: tok=%v id=%q ok=%d", st.probeTok, st.probeID, st.probeOK)
				}
				if d := st.gateUntil.Sub(c.now()); d != 60*time.Second {
					t.Fatalf("gateUntil = now+%v, want now+60s", d)
				}
			},
			"restart": func(t *testing.T) {
				c := testClock(t)
				cfg := defaultHealthCfg()
				cfg.stateFile = t.TempDir() + "/health.json"
				h := newHealthStore(cfg)
				trip(t, h, c)
				want := snap(t, h, tKey).holdUntil
				h.mu.Lock()
				h.persistLocked()
				h.mu.Unlock()
				c.advance(10 * time.Second)
				h2 := loadHealthStore(cfg)
				mustState(t, h2, tKey, hsOpen)
				st := snap(t, h2, tKey)
				if !st.holdUntil.Equal(want) {
					t.Fatalf("holdUntil = %v, want persisted %v (min rule)", st.holdUntil, want)
				}
				if st.k != 1 {
					t.Fatalf("k = %d, want 1", st.k)
				}
			},
			"member_absent": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				trip(t, h, c)
				h.memberAbsent(tKey)
				c.advance(61 * time.Second)
				h.tick()
				mustState(t, h, tKey, hsOpen) // frozen: hold does not expire
			},
			"member_present": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				trip(t, h, c)
				h.memberAbsent(tKey)
				c.advance(61 * time.Second)
				h.memberPresent(tKey)
				h.tick()
				mustState(t, h, tKey, hsHalfOpen)
			},
		},
		hsHalfOpen: {
			"ok": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				toHalfOpen(t, h, c)
				h.record(tKey, okS(uniq("ok")))
				mustState(t, h, tKey, hsHalfOpen) // non-probe ok never closes
				st := snap(t, h, tKey)
				if !st.probeTok || st.probeOK != 0 {
					t.Fatalf("non-probe ok touched the token: tok=%v probeOK=%d", st.probeTok, st.probeOK)
				}
			},
			"deg": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				toHalfOpen(t, h, c)
				oldEp := snap(t, h, tKey).ep
				h.record(tKey, degS(uniq("d"), 400))
				mustState(t, h, tKey, hsOpen)
				st := snap(t, h, tKey)
				if st.k != 2 {
					t.Fatalf("k = %d, want 2", st.k)
				}
				if st.probeTok {
					t.Fatal("token survived the reopen")
				}
				if st.ep != oldEp {
					t.Fatal("reopen minted a new episode; it must stay inside the same one")
				}
				if d := st.holdUntil.Sub(st.since); d != 120*time.Second {
					t.Fatalf("hold = %v, want 120s", d)
				}
				h.mu.Lock()
				eps := h.episodes[tKey]
				h.mu.Unlock()
				if eps != 1 {
					t.Fatalf("episodes = %d, want 1", eps)
				}
			},
			"probe_ok": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				toHalfOpen(t, h, c)
				probeOKOnce(t, h, c)
				mustState(t, h, tKey, hsHalfOpen)
				st := snap(t, h, tKey)
				if st.probeOK != 1 || !st.probeTok || st.probeID != "" {
					t.Fatalf("after first probe ok: probeOK=%d tok=%v id=%q, want 1/true/empty", st.probeOK, st.probeTok, st.probeID)
				}
				c.advance(6 * time.Second)
				probeOKOnce(t, h, c)
				mustState(t, h, tKey, hsConvalescent)
				st = snap(t, h, tKey)
				if st.lastK != 1 || !st.lastClose.Equal(c.now()) || st.probeTok {
					t.Fatalf("close state wrong: lastK=%d lastClose=%v tok=%v", st.lastK, st.lastClose, st.probeTok)
				}
			},
			"probe_deg": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				toHalfOpen(t, h, c)
				oldEp := snap(t, h, tKey).ep
				l := h.claimProbe(tKey, "helpdesk", "gpt-5.6-sol", uniq("probe"), 1000)
				if l == nil {
					t.Fatal("claim failed")
				}
				h.settleLease(l, outcomeProbeDeg, sample{reqID: l.id, class: "helpdesk", fp: 401})
				mustState(t, h, tKey, hsOpen)
				st := snap(t, h, tKey)
				if st.k != 2 || st.ep != oldEp || st.lease != nil {
					t.Fatalf("probe_deg reopen wrong: k=%d sameEp=%v lease=%v", st.k, st.ep == oldEp, st.lease)
				}
			},
			"probe_abandoned": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				toHalfOpen(t, h, c)
				l := h.claimProbe(tKey, "helpdesk", "gpt-5.6-sol", uniq("probe"), 1000)
				if l == nil {
					t.Fatal("claim failed")
				}
				h.settleLease(l, outcomeProbeAbandoned, sample{cause: "client_cancel"})
				mustState(t, h, tKey, hsHalfOpen)
				st := snap(t, h, tKey)
				if !st.probeTok || st.probeAbandons != 1 || st.k != 1 {
					t.Fatalf("abandon: tok=%v abandons=%d k=%d, want true/1/1", st.probeTok, st.probeAbandons, st.k)
				}
			},
			"tick": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				toHalfOpen(t, h, c)
				l := h.claimProbe(tKey, "helpdesk", "gpt-5.6-sol", uniq("probe"), 1000)
				if l == nil {
					t.Fatal("claim failed")
				}
				c.advance(60 * time.Second)
				h.tick() // before the TTL: lease stands
				st := snap(t, h, tKey)
				if st.probeID != l.id {
					t.Fatal("tick released the lease before its TTL")
				}
				c.advance(61 * time.Second) // 121s > PROBE_TTL
				h.tick()
				st = snap(t, h, tKey)
				if !st.probeTok || st.probeID != "" || st.probeAbandons != 1 {
					t.Fatalf("TTL release: tok=%v id=%q abandons=%d, want true/empty/1", st.probeTok, st.probeID, st.probeAbandons)
				}
				mustState(t, h, tKey, hsHalfOpen)
			},
			"restart": func(t *testing.T) {
				c := testClock(t)
				cfg := defaultHealthCfg()
				cfg.stateFile = t.TempDir() + "/health.json"
				h := newHealthStore(cfg)
				toHalfOpen(t, h, c)
				h.mu.Lock()
				h.persistLocked()
				h.mu.Unlock()
				c.advance(10 * time.Second)
				h2 := loadHealthStore(cfg)
				mustState(t, h2, tKey, hsOpen) // the token never survives a restart
				st := snap(t, h2, tKey)
				if st.probeTok || st.probeID != "" {
					t.Fatal("token state leaked through the restart")
				}
				h2.tick() // persisted holdUntil already passed → re-probe at once
				mustState(t, h2, tKey, hsHalfOpen)
			},
			"member_absent": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				toHalfOpen(t, h, c)
				h.memberAbsent(tKey)
				if l := h.claimProbe(tKey, "helpdesk", "gpt-5.6-sol", uniq("probe"), 1000); l != nil {
					t.Fatal("claim succeeded on an absent member")
				}
				c.advance(700 * time.Second)
				h.tick()
				mustState(t, h, tKey, hsHalfOpen) // frozen
			},
			"member_present": func(t *testing.T) {
				c := testClock(t)
				h := newHS()
				toHalfOpen(t, h, c)
				h.memberAbsent(tKey)
				h.memberPresent(tKey)
				if l := h.claimProbe(tKey, "helpdesk", "gpt-5.6-sol", uniq("probe"), 1000); l == nil {
					t.Fatal("claim failed after member_present")
				}
			},
		},
	}
}

// ---- U1: trip arm R at the exact threshold ----

func TestU1TripRateArm(t *testing.T) {
	c := testClock(t)
	h := newHS()
	// 2 ok then 2 deg: n=4 ≥ TRIP_MIN, rate 0.5 ≥ TRIP_RATE → trips, even
	// though the consecutive arms cannot fire (tail holds an ok... it does
	// not: tail3 = [ok,deg,deg]).
	h.record(tKey, okS(uniq("ok")))
	c.advance(6 * time.Second)
	h.record(tKey, okS(uniq("ok")))
	c.advance(6 * time.Second)
	h.record(tKey, degS(uniq("d"), 500))
	c.advance(6 * time.Second)
	mustState(t, h, tKey, hsHealthy)     // n=3 < TRIP_MIN
	h.record(tKey, degS(uniq("d"), 500)) // same shape: only arm R can fire
	mustState(t, h, tKey, hsOpen)

	// Just under the rate: 3 ok + 2 deg = 0.4.
	h2 := newHS()
	for i := 0; i < 3; i++ {
		h2.record(tKey, okS(uniq("ok")))
		c.advance(6 * time.Second)
	}
	h2.record(tKey, degS(uniq("d"), 501))
	c.advance(6 * time.Second)
	h2.record(tKey, degS(uniq("d"), 501))
	mustState(t, h2, tKey, hsHealthy)
}

// ---- U2: background noise never trips (G4) ----

func TestU2BackgroundNoise(t *testing.T) {
	c := testClock(t)
	h := newHS()
	for i := 0; i < 60; i++ {
		if i%10 == 5 {
			h.record(tKey, degS(uniq("d"), uint64(600+i)))
		} else {
			h.record(tKey, okS(uniq("ok")))
		}
		mustState(t, h, tKey, hsHealthy)
		c.advance(10 * time.Second)
	}
}

// ---- U3: the flapper walks the ladder and caps at kMax (I2) ----

func TestU3FlapperLadder(t *testing.T) {
	c := testClock(t)
	h := newHS()
	cfg := h.cfg
	if cfg.kMax != 6 {
		t.Fatalf("derived kMax = %d, want 6 at defaults", cfg.kMax)
	}
	trip(t, h, c)
	for i := 2; i <= 8; i++ {
		// Serve the hold, probe out, close, and immediately re-trip.
		st := snap(t, h, tKey)
		c.advance(st.holdUntil.Sub(c.now()) + time.Second)
		h.tick()
		mustState(t, h, tKey, hsHalfOpen)
		probeOKOnce(t, h, c)
		c.advance(6 * time.Second)
		probeOKOnce(t, h, c)
		mustState(t, h, tKey, hsConvalescent)
		for j := 0; j < 3; j++ {
			c.advance(6 * time.Second)
			h.record(tKey, degS(uniq("d"), uint64(700+j)))
		}
		mustState(t, h, tKey, hsOpen)
		want := i
		if want > 6 {
			want = 6
		}
		st = snap(t, h, tKey)
		if st.k != want {
			t.Fatalf("cycle %d: k = %d, want %d", i, st.k, want)
		}
		if want == 6 {
			if d := st.holdUntil.Sub(st.since); d != 1800*time.Second {
				t.Fatalf("cycle %d: hold = %v, want ceiling 1800s", i, d)
			}
		}
	}
	// After a full convalescent window the ladder resets: close, heal, re-trip → k=1.
	st := snap(t, h, tKey)
	c.advance(st.holdUntil.Sub(c.now()) + time.Second)
	h.tick()
	probeOKOnce(t, h, c)
	c.advance(6 * time.Second)
	probeOKOnce(t, h, c)
	mustState(t, h, tKey, hsConvalescent)
	c.advance(601 * time.Second)
	h.tick()
	mustState(t, h, tKey, hsHealthy)
	trip(t, h, c)
	if st := snap(t, h, tKey); st.k != 1 {
		t.Fatalf("post-heal re-trip k = %d, want 1", st.k)
	}
}

// ---- U4: recovery is bounded by the first hold ----

func TestU4RecoveryLatency(t *testing.T) {
	c := testClock(t)
	h := newHS()
	trip(t, h, c)
	openedAt := snap(t, h, tKey).since
	c.advance(61 * time.Second)
	h.tick()
	probeOKOnce(t, h, c)
	c.advance(6 * time.Second)
	probeOKOnce(t, h, c)
	mustState(t, h, tKey, hsConvalescent)
	if d := c.now().Sub(openedAt); d > 90*time.Second {
		t.Fatalf("recovery took %v, want ≤ holdBase + slack (90s)", d)
	}
}

// ---- U5 (FSM-local half): the record store runs on nowFn ----

func TestU5RecordsOnNowFn(t *testing.T) {
	c := testClock(t)
	t.Cleanup(resetQuotaState())
	m := uMember(uniq("u5"))
	uSpent(m)
	if c, _ := routeCheck(m.Route, uAccount(m.ID), m.Model, nowFn()); !c.no() {
		t.Fatalf("check = %s right after the record, want no", c)
	}
	c.advance(quotaCooldown + time.Second)
	if chk, _ := routeCheck(m.Route, uAccount(m.ID), m.Model, nowFn()); !chk.included() {
		t.Fatalf("check = %s after the floor on the fake clock — the store is not on nowFn", chk)
	}
}

// ---- U6: ring admission rules ----

func TestU6RingAdmission(t *testing.T) {
	t.Run("rule0_req_dedup_deg_wins", func(t *testing.T) {
		testClock(t)
		h := newHS()
		req := uniq("r0")
		h.record(tKey, okS(req))
		h.record(tKey, sample{ok: false, class: "helpdesk", reqID: req, fp: 800})
		h.record(tKey, sample{ok: false, class: "helpdesk", reqID: req, fp: 801})
		h.record(tKey, okS(req))
		h.mu.Lock()
		ring := append([]sample(nil), h.keys[tKey].ring...)
		h.mu.Unlock()
		if len(ring) != 1 {
			t.Fatalf("ring = %d samples, want 1", len(ring))
		}
		if ring[0].ok || ring[0].fp != 800 {
			t.Fatalf("survivor = ok:%v fp:%d, want the FIRST deg (fp 800)", ring[0].ok, ring[0].fp)
		}
	})
	t.Run("rule1_bucket_collapse", func(t *testing.T) {
		c := testClock(t)
		h := newHS()
		// 20 rps for 60s, one class: at most window/bucket = 12 samples.
		for i := 0; i < 1200; i++ {
			h.record(tKey, okS(uniq("r1")))
			c.advance(50 * time.Millisecond)
		}
		if n := ringLen(h, tKey); n != 12 {
			t.Fatalf("ring = %d, want 12 (60s / 5s buckets)", n)
		}
	})
	t.Run("rule1_first_deg_wins_bucket", func(t *testing.T) {
		testClock(t)
		h := newHS()
		h.record(tKey, okS(uniq("a")))
		h.record(tKey, sample{ok: false, class: "helpdesk", reqID: uniq("b"), fp: 810})
		h.record(tKey, sample{ok: false, class: "helpdesk", reqID: uniq("c"), fp: 811})
		h.mu.Lock()
		ring := append([]sample(nil), h.keys[tKey].ring...)
		h.mu.Unlock()
		if len(ring) != 1 || ring[0].ok || ring[0].fp != 810 {
			t.Fatalf("bucket survivor wrong: %+v", ring)
		}
	})
	t.Run("rule2_class_share_cap", func(t *testing.T) {
		c := testClock(t)
		h := newHS()
		cap2 := h.cfg.ringN / 2
		for i := 0; i < cap2+10; i++ {
			h.record(tKey, sample{ok: true, class: "classA", reqID: uniq("a")})
			c.advance(6 * time.Second)
		}
		h.record(tKey, sample{ok: true, class: "classB", reqID: uniq("b")})
		h.mu.Lock()
		aN, bN := 0, 0
		for _, s := range h.keys[tKey].ring {
			if s.class == "classA" {
				aN++
			} else if s.class == "classB" {
				bN++
			}
		}
		h.mu.Unlock()
		if aN != cap2 || bN != 1 {
			t.Fatalf("classA = %d (want %d), classB = %d (want 1)", aN, cap2, bN)
		}
	})
}

// ---- U8: forced samples never trip, never escalate (I5) ----

func TestU8ForcedExcluded(t *testing.T) {
	c := testClock(t)
	h := newHS()
	for i := 0; i < 10; i++ {
		s := degS(uniq("f"), uint64(900+i))
		s.forced = true
		h.record(tKey, s)
		mustState(t, h, tKey, hsHealthy)
		c.advance(6 * time.Second)
	}
	// One real deg on top: n(live)=1 < TRIP_MIN, tail < TRIP_CONSEC → healthy.
	h.record(tKey, degS(uniq("d"), 910))
	mustState(t, h, tKey, hsHealthy)

	// A forced deg in half_open must not reopen.
	h2 := newHS()
	toHalfOpen(t, h2, c)
	s := degS(uniq("f"), 911)
	s.forced = true
	h2.record(tKey, s)
	mustState(t, h2, tKey, hsHalfOpen)
	if st := snap(t, h2, tKey); st.k != 1 || !st.probeTok {
		t.Fatalf("forced deg escalated: k=%d tok=%v", st.k, st.probeTok)
	}
}

// ---- U9: arm S catches the slow burn; a single shape does not ----

func TestU9SlowBurnArmS(t *testing.T) {
	c := testClock(t)
	h := newHS()
	// Three degs 400s apart: the rate arm's window holds only two, but the
	// consecutive tail has three distinct-shape degs → arm S trips.
	for i := 0; i < 3; i++ {
		h.record(tKey, degS(uniq("d"), uint64(1000+i)))
		if i < 2 {
			mustState(t, h, tKey, hsHealthy)
			c.advance(400 * time.Second)
		}
	}
	mustState(t, h, tKey, hsOpen)

	// Same cadence, one shape: never trips (a single failure mode repeating
	// slowly is not a degradation signature).
	h2 := newHS()
	for i := 0; i < 3; i++ {
		h2.record(tKey, degS(uniq("d"), 1010))
		mustState(t, h2, tKey, hsHealthy)
		c.advance(400 * time.Second)
	}

	// An ok inside the tail resets the consecutive arms.
	h3 := newHS()
	h3.record(tKey, degS(uniq("d"), 1020))
	c.advance(400 * time.Second)
	h3.record(tKey, degS(uniq("d"), 1021))
	c.advance(400 * time.Second)
	h3.record(tKey, okS(uniq("ok")))
	c.advance(400 * time.Second)
	h3.record(tKey, degS(uniq("d"), 1022))
	mustState(t, h3, tKey, hsHealthy)
}

// ---- U10: persistence round trip ----

func TestU10Persistence(t *testing.T) {
	c := testClock(t)
	cfg := defaultHealthCfg()
	cfg.stateFile = t.TempDir() + "/health.json"
	h := newHealthStore(cfg)

	// Walk to open k=3 with an election and a detoured session.
	trip(t, h, c)
	c.advance(61 * time.Second)
	h.tick()
	l := h.claimProbe(tKey, "helpdesk", "kimi/kimi-k3", uniq("p"), 1000)
	h.settleLease(l, outcomeProbeDeg, sample{reqID: l.id, class: "helpdesk", fp: 1100})
	c.advance(121 * time.Second)
	h.tick()
	l = h.claimProbe(tKey, "helpdesk", "kimi/kimi-k3", uniq("p"), 1000)
	h.settleLease(l, outcomeProbeDeg, sample{reqID: l.id, class: "helpdesk", fp: 1101})
	mustState(t, h, tKey, hsOpen)
	if st := snap(t, h, tKey); st.k != 3 {
		t.Fatalf("setup k = %d, want 3", st.k)
	}
	if _, elected := h.elect(tKey, "helpdesk", "kimi/kimi-k3"); !elected {
		t.Fatal("elect failed")
	}
	h.noteSessionDetour(tKey, "helpdesk", "sess-1")
	h.mu.Lock()
	h.keys[tKey].perClass["helpdesk"].prefillsPaid = 2
	h.persistLocked()
	h.mu.Unlock()
	before := snap(t, h, tKey)

	c.advance(30 * time.Second)
	h2 := loadHealthStore(cfg)
	mustState(t, h2, tKey, hsOpen)
	st := snap(t, h2, tKey)
	if st.k != 3 || st.ep != before.ep {
		t.Fatalf("restored k=%d ep=%q, want 3/%q", st.k, st.ep, before.ep)
	}
	if !st.holdUntil.Equal(before.holdUntil) {
		t.Fatalf("holdUntil = %v, want persisted %v (min rule: persisted < now+hold)", st.holdUntil, before.holdUntil)
	}
	ce := st.perClass["helpdesk"]
	if ce == nil || ce.member != "kimi/kimi-k3" || ce.prefillsPaid != 2 || ce.ep != before.ep {
		t.Fatalf("election not restored: %+v", ce)
	}
	if len(ce.sessions) != 0 {
		t.Fatal("per-session ramp state survived the restart (R-45 forbids)")
	}

	t.Run("corrupt_file_starts_empty", func(t *testing.T) {
		cfg2 := defaultHealthCfg()
		cfg2.stateFile = t.TempDir() + "/health.json"
		if err := os.WriteFile(cfg2.stateFile, []byte("{not json"), 0o600); err != nil {
			t.Fatal(err)
		}
		h3 := loadHealthStore(cfg2)
		h3.mu.Lock()
		n := len(h3.keys)
		h3.mu.Unlock()
		if n != 0 {
			t.Fatalf("corrupt file produced %d keys, want 0", n)
		}
	})

	t.Run("tampered_holdUntil_clamped", func(t *testing.T) {
		cfg2 := defaultHealthCfg()
		cfg2.stateFile = t.TempDir() + "/health.json"
		p := persistedHealth{V: 1, Keys: map[string]persistedKey{
			tKey: {State: hsOpen, K: 2, Since: c.now(), Ep: tKey + "@9", HoldUntil: c.now().Add(48 * time.Hour)},
		}}
		buf, _ := json.Marshal(p)
		if err := os.WriteFile(cfg2.stateFile, buf, 0o600); err != nil {
			t.Fatal(err)
		}
		h3 := loadHealthStore(cfg2)
		st := snap(t, h3, tKey)
		if want := c.now().Add(120 * time.Second); !st.holdUntil.Equal(want) {
			t.Fatalf("holdUntil = %v, want clamped now+hold(2) = %v", st.holdUntil, want)
		}
	})
}

// The persistence file is written by the transition itself, not only by an
// explicit save (§7.1: "on every transition and on the 60s tick").
func TestPersistOnTransition(t *testing.T) {
	c := testClock(t)
	cfg := defaultHealthCfg()
	cfg.stateFile = t.TempDir() + "/health.json"
	h := newHealthStore(cfg)
	trip(t, h, c)
	raw, err := os.ReadFile(cfg.stateFile)
	if err != nil {
		t.Fatalf("no state file after a transition: %v", err)
	}
	var p persistedHealth
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	if p.Keys[tKey].State != hsOpen {
		t.Fatalf("persisted state = %q, want open", p.Keys[tKey].State)
	}
}

// ---- U12: every abandon cause re-arms without escalation ----

func TestU12ProbeAbandon(t *testing.T) {
	c := testClock(t)
	h := newHS()
	toHalfOpen(t, h, c)
	causes := []string{"client_cancel", "http_4xx", "auth", "rate_limit", "unknown"}
	for i, cause := range causes {
		l := h.claimProbe(tKey, "helpdesk", "gpt-5.6-sol", uniq("p"), 1000)
		if l == nil {
			t.Fatalf("claim %d failed", i)
		}
		h.settleLease(l, outcomeProbeAbandoned, sample{cause: cause})
		mustState(t, h, tKey, hsHalfOpen)
		st := snap(t, h, tKey)
		if st.probeAbandons != i+1 || st.k != 1 || !st.probeTok {
			t.Fatalf("cause %q: abandons=%d k=%d tok=%v", cause, st.probeAbandons, st.k, st.probeTok)
		}
		c.advance(6 * time.Second)
	}
	if st := snap(t, h, tKey); st.probeAbandons < h.cfg.abandonNotice {
		t.Fatalf("abandons = %d, expected past the notice threshold %d", st.probeAbandons, h.cfg.abandonNotice)
	}

	t.Run("zombie_released_by_tick", func(t *testing.T) {
		c := testClock(t)
		h := newHS()
		toHalfOpen(t, h, c)
		l := h.claimProbe(tKey, "helpdesk", "gpt-5.6-sol", uniq("p"), 1000)
		if l == nil {
			t.Fatal("claim failed")
		}
		c.advance(121 * time.Second)
		h.tick() // TTL release wins the CAS
		st := snap(t, h, tKey)
		if !st.probeTok || st.probeAbandons != 1 {
			t.Fatalf("TTL release: tok=%v abandons=%d", st.probeTok, st.probeAbandons)
		}
		// The zombie finally reports: its settle loses the CAS, is counted,
		// and changes nothing.
		h.settleLease(l, outcomeProbeOK, sample{reqID: l.id, class: "helpdesk"})
		mustState(t, h, tKey, hsHalfOpen)
		st = snap(t, h, tKey)
		if st.probeOK != 0 {
			t.Fatalf("zombie settle scored a probe: probeOK=%d", st.probeOK)
		}
		h.mu.Lock()
		ds := h.doubleSettles[tKey]
		h.mu.Unlock()
		if ds != 1 {
			t.Fatalf("doubleSettles = %d, want 1", ds)
		}
	})
}

// ---- probe size gate (§4.5) ----

func TestProbeSizeGate(t *testing.T) {
	c := testClock(t)
	h := newHS()
	toHalfOpen(t, h, c)
	if l := h.claimProbe(tKey, "helpdesk", "gpt-5.6-sol", uniq("p"), 33000); l != nil {
		t.Fatal("oversized probe admitted before gateUntil")
	}
	if l := h.claimProbe(tKey, "helpdesk", "gpt-5.6-sol", uniq("p"), 1000); l == nil {
		t.Fatal("small probe refused")
	}
	// Re-arm and lift the gate.
	st := snap(t, h, tKey)
	h.settleLease(st.lease, outcomeProbeAbandoned, sample{})
	c.advance(61 * time.Second) // past gateUntil = halfOpenAt + hold(1)
	if l := h.claimProbe(tKey, "helpdesk", "gpt-5.6-sol", uniq("p"), 33000); l == nil {
		t.Fatal("oversized probe still refused after gateUntil")
	}
}

// ---- U18: the lease matrix ----

func TestU18LeaseMatrix(t *testing.T) {
	t.Run("stale_ok_lands_in_new_episode_as_ordinary_ok", func(t *testing.T) {
		c := testClock(t)
		h := newHS()
		toHalfOpen(t, h, c)
		l1 := h.claimProbe(tKey, "helpdesk", "gpt-5.6-sol", uniq("p"), 1000)
		if l1 == nil {
			t.Fatal("claim failed")
		}
		// A non-probe deg reopens; l1's token is dropped but l1 never settled.
		h.record(tKey, degS(uniq("d"), 1200))
		mustState(t, h, tKey, hsOpen)
		// Serve out the episode: half_open → two probes → convalescent.
		c.advance(121 * time.Second)
		h.tick()
		probeOKOnce(t, h, c)
		c.advance(6 * time.Second)
		probeOKOnce(t, h, c)
		mustState(t, h, tKey, hsConvalescent)
		// Fresh trip mints a new episode; walk it to half_open and claim l2.
		for j := 0; j < 3; j++ {
			c.advance(6 * time.Second)
			h.record(tKey, degS(uniq("d"), uint64(1210+j)))
		}
		mustState(t, h, tKey, hsOpen)
		st := snap(t, h, tKey)
		c.advance(st.holdUntil.Sub(c.now()) + time.Second)
		h.tick()
		l2 := h.claimProbe(tKey, "helpdesk", "gpt-5.6-sol", uniq("p"), 1000)
		if l2 == nil {
			t.Fatal("claim in new episode failed")
		}
		beforeK := snap(t, h, tKey).k
		beforeRing := ringLen(h, tKey)
		// Now l1's probe_ok finally arrives: ordinary ok, nothing else.
		h.settleLease(l1, outcomeProbeOK, sample{reqID: l1.id, class: "helpdesk"})
		mustState(t, h, tKey, hsHalfOpen)
		st = snap(t, h, tKey)
		if st.k != beforeK || st.probeOK != 0 || st.probeID != l2.id {
			t.Fatalf("stale ok touched the new episode: k=%d probeOK=%d id=%q (want %q)", st.k, st.probeOK, st.probeID, l2.id)
		}
		if ringLen(h, tKey) != beforeRing+1 {
			t.Fatal("stale ok was not admitted as ordinary evidence")
		}
		h.mu.Lock()
		ds := h.doubleSettles[tKey]
		h.mu.Unlock()
		if ds != 0 {
			t.Fatalf("doubleSettles = %d, want 0 (a stale first settle is not a double)", ds)
		}
		// l2 continues unharmed.
		h.settleLease(l2, outcomeProbeOK, sample{reqID: l2.id, class: "helpdesk"})
		if st := snap(t, h, tKey); st.probeOK != 1 {
			t.Fatalf("l2 settle scored %d, want 1", st.probeOK)
		}
	})

	t.Run("stale_deg_never_advances_k_as_probe", func(t *testing.T) {
		c := testClock(t)
		h := newHS()
		toHalfOpen(t, h, c)
		l1 := h.claimProbe(tKey, "helpdesk", "gpt-5.6-sol", uniq("p"), 1000)
		if l1 == nil {
			t.Fatal("claim failed")
		}
		h.record(tKey, degS(uniq("d"), 1300)) // reopen k=2, token dropped
		mustState(t, h, tKey, hsOpen)
		h.settleLease(l1, outcomeProbeDeg, sample{reqID: l1.id, class: "helpdesk", fp: 1301})
		mustState(t, h, tKey, hsOpen)
		if st := snap(t, h, tKey); st.k != 2 {
			t.Fatalf("stale probe_deg advanced k to %d, want 2", st.k)
		}
	})

	t.Run("double_settle_counted_and_inert", func(t *testing.T) {
		c := testClock(t)
		h := newHS()
		toHalfOpen(t, h, c)
		l := h.claimProbe(tKey, "helpdesk", "gpt-5.6-sol", uniq("p"), 1000)
		if l == nil {
			t.Fatal("claim failed")
		}
		h.settleLease(l, outcomeProbeOK, sample{reqID: l.id, class: "helpdesk"})
		st1 := snap(t, h, tKey)
		h.settleLease(l, outcomeProbeDeg, sample{reqID: l.id, class: "helpdesk", fp: 1400})
		st2 := snap(t, h, tKey)
		if st2.state != st1.state || st2.k != st1.k || st2.probeOK != st1.probeOK {
			t.Fatal("double settle changed state")
		}
		h.mu.Lock()
		ds := h.doubleSettles[tKey]
		h.mu.Unlock()
		if ds != 1 {
			t.Fatalf("doubleSettles = %d, want 1", ds)
		}
	})

	t.Run("concurrent_adopt_exactly_one", func(t *testing.T) {
		c := testClock(t)
		h := newHS()
		toHalfOpen(t, h, c)
		var got atomic.Int32
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				if l := h.adoptProbe(tKey, "helpdesk", "gpt-5.6-sol", uniq("adopt"), 1000); l != nil {
					got.Add(1)
				}
			}(g)
		}
		wg.Wait()
		if got.Load() != 1 {
			t.Fatalf("%d adopts won the token, want exactly 1", got.Load())
		}
	})
}

// ---- ramp scheduling, election, dwell (U13's FSM-local half + BL-9 + I1/I3) ----

func TestRampSchedulingAndElection(t *testing.T) {
	c := testClock(t)
	h := newHS()
	trip(t, h, c)

	m, elected := h.elect(tKey, "helpdesk", "kimi/kimi-k3")
	if !elected || m != "kimi/kimi-k3" {
		t.Fatalf("first elect = %q/%v", m, elected)
	}
	m2, e2 := h.elect(tKey, "helpdesk", "gpt/other")
	if e2 || m2 != "kimi/kimi-k3" {
		t.Fatalf("I1 violated: second elect = %q/%v, want incumbent/false", m2, e2)
	}

	for _, s := range []string{"s1", "s2", "s3"} {
		h.noteSessionDetour(tKey, "helpdesk", s)
		c.advance(time.Second)
	}
	if _, holds := h.rampHolds(tKey, "helpdesk", "s1", time.Time{}); holds {
		t.Fatal("ramp held on an open source")
	}

	c.advance(61 * time.Second)
	h.tick()
	c.advance(950 * time.Second) // let the RETURN_DWELL floor pass before the close
	probeOKOnce(t, h, c)
	c.advance(6 * time.Second)
	probeOKOnce(t, h, c)
	mustState(t, h, tKey, hsConvalescent)
	closeAt := c.now()

	// Deterministic schedule: return i dwells to closeAt + jitter(id, i·60s).
	h.mu.Lock()
	ce := h.keys[tKey].perClass["helpdesk"]
	for i, id := range []string{"s1", "s2", "s3"} {
		want := closeAt.Add(jitterDur(id, time.Duration(i)*60*time.Second))
		if got := ce.sessions[id].dwellUntil; !got.Equal(want) {
			h.mu.Unlock()
			t.Fatalf("%s dwellUntil = %v, want %v", id, got, want)
		}
	}
	h.mu.Unlock()

	// s1 (i=0, zero jitter) goes home at once and its entry is deleted.
	if _, holds := h.rampHolds(tKey, "helpdesk", "s1", time.Time{}); holds {
		t.Fatal("s1 held past its dwell")
	}
	// s2 is still dwelling on the detour member.
	m, holds := h.rampHolds(tKey, "helpdesk", "s2", time.Time{})
	if !holds || m != "kimi/kimi-k3" {
		t.Fatalf("s2 hold = %q/%v, want kimi/kimi-k3/true", m, holds)
	}
	// A binding newer than the close is not held (it re-bound after recovery).
	if _, holds := h.rampHolds(tKey, "helpdesk", "s3", closeAt.Add(time.Second)); holds {
		t.Fatal("held a session whose binding is newer than the close")
	}
	// s2 returns once its dwell passes.
	c.advance(jitterDur("s2", 60*time.Second) + time.Second)
	if _, holds := h.rampHolds(tKey, "helpdesk", "s2", time.Time{}); holds {
		t.Fatal("s2 held past its dwell")
	}
}

func TestRampOutlivesConvalescent(t *testing.T) { // BL-9
	c := testClock(t)
	h := newHS()
	trip(t, h, c)
	h.elect(tKey, "helpdesk", "kimi/kimi-k3")
	h.noteSessionDetour(tKey, "helpdesk", "s9")
	movedAt := c.now()
	c.advance(61 * time.Second)
	h.tick()
	probeOKOnce(t, h, c)
	c.advance(6 * time.Second)
	probeOKOnce(t, h, c)
	mustState(t, h, tKey, hsConvalescent)
	// The dwell floor movedAt+900s lands beyond the convalescent window.
	h.mu.Lock()
	dwell := h.keys[tKey].perClass["helpdesk"].sessions["s9"].dwellUntil
	h.mu.Unlock()
	if want := movedAt.Add(900 * time.Second); !dwell.Equal(want) {
		t.Fatalf("dwell = %v, want floor %v", dwell, want)
	}
	c.advance(601 * time.Second)
	h.tick()
	mustState(t, h, tKey, hsHealthy)
	m, holds := h.rampHolds(tKey, "helpdesk", "s9", time.Time{})
	if !holds || m != "kimi/kimi-k3" {
		t.Fatalf("ramp did not outlive convalescent: %q/%v", m, holds)
	}
	c.advance(300 * time.Second) // past the floor
	if _, holds := h.rampHolds(tKey, "helpdesk", "s9", time.Time{}); holds {
		t.Fatal("s9 held past its dwell")
	}
	h.mu.Lock()
	_, still := h.keys[tKey].perClass["helpdesk"]
	h.mu.Unlock()
	if still {
		t.Fatal("drained election not dropped")
	}
}

func TestRetripCancelsPendingReturns(t *testing.T) { // I3 + supersede
	c := testClock(t)
	h := newHS()
	trip(t, h, c)
	oldEp := snap(t, h, tKey).ep
	h.elect(tKey, "helpdesk", "kimi/kimi-k3")
	h.noteSessionDetour(tKey, "helpdesk", "s9")
	c.advance(61 * time.Second)
	h.tick()
	probeOKOnce(t, h, c)
	c.advance(6 * time.Second)
	probeOKOnce(t, h, c)
	mustState(t, h, tKey, hsConvalescent)
	// Re-trip: pending returns are canceled, the roster survives, and a new
	// election supersedes the old one.
	for j := 0; j < 3; j++ {
		c.advance(6 * time.Second)
		h.record(tKey, degS(uniq("d"), uint64(1500+j)))
	}
	mustState(t, h, tKey, hsOpen)
	h.mu.Lock()
	ce := h.keys[tKey].perClass["helpdesk"]
	dwell := ce.sessions["s9"].dwellUntil
	h.mu.Unlock()
	if !dwell.IsZero() {
		t.Fatalf("pending return not canceled: dwell = %v", dwell)
	}
	if _, holds := h.rampHolds(tKey, "helpdesk", "s9", time.Time{}); holds {
		t.Fatal("ramp held on a re-tripped source")
	}
	m, elected := h.elect(tKey, "helpdesk", "codex/gpt-5.6")
	if !elected || m != "codex/gpt-5.6" {
		t.Fatalf("supersede elect = %q/%v", m, elected)
	}
	h.mu.Lock()
	ce = h.keys[tKey].perClass["helpdesk"]
	newEp, hasS9, prefills := ce.ep, ce.sessions["s9"] != nil, ce.prefillsPaid
	h.mu.Unlock()
	if newEp == oldEp || !hasS9 || prefills != 0 {
		t.Fatalf("supersede wrong: ep=%q (old %q) s9=%v prefills=%d", newEp, oldEp, hasS9, prefills)
	}
}

// ---- race hammer: concurrent record/claim/settle/tick, zero double settles ----

func TestRaceHammer(t *testing.T) {
	c := testClock(t)
	h := newHS()
	keys := []string{"gpt/a", "gpt/b", "kimi/c"}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	// Clock driver: 100 × 1s with a tick each — enough to serve holds
	// (60–120s) but never to cross PROBE_TTL between a claim and its settle,
	// so every lease is settled exactly once by its owner.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		for i := 0; i < 100; i++ {
			c.advance(time.Second)
			h.tick()
		}
	}()
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				i++
				key := keys[i%len(keys)]
				h.record(key, sample{ok: i%4 != 0, class: "helpdesk", reqID: uniq("rh"), fp: uint64(i % 5)})
				if l := h.adoptProbe(key, "helpdesk", "m", uniq("ap"), 100); l != nil {
					h.settleLease(l, outcomeProbeOK, sample{reqID: l.id, class: "helpdesk"})
				}
				h.rampHolds(key, "helpdesk", "sess", time.Time{})
			}
		}(g)
	}
	wg.Wait()
	h.mu.Lock()
	var ds int64
	for _, v := range h.doubleSettles {
		ds += v
	}
	h.mu.Unlock()
	if ds != 0 {
		t.Fatalf("doubleSettles = %d, want 0 under owner-only settles", ds)
	}
}

// ---- derived config sanity (§8) ----

func TestDerivedConfig(t *testing.T) {
	c := defaultHealthCfg()
	if c.kMax != 6 || c.ringN != 256 || c.bucketS != 5*time.Second || c.abandonNotice != 3 {
		t.Fatalf("derived = kMax:%d ringN:%d bucket:%v notice:%d", c.kMax, c.ringN, c.bucketS, c.abandonNotice)
	}
	holds := []time.Duration{60, 120, 240, 480, 960, 1800, 1800}
	for i, want := range holds {
		if got := c.hold(i + 1); got != want*time.Second {
			t.Fatalf("hold(%d) = %v, want %vs", i+1, got, want)
		}
	}
}
