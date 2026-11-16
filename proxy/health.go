package main

// health.go — degraded-provider failover (docs/degraded-failover/60-spec-v2.md).
// Stage 0 (#120) ships ONLY the SWITCH_HEALTH mode gate and the
// clock seam: parsed at boot, logged when non-off, read by nothing else. The
// four-state FSM (§4), evidence producers (§3) and routing consumption (§5)
// land with #121–#123, all behind this gate.

import (
	"context"
	"hash/fnv"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type healthMode int

const (
	healthOff healthMode = iota
	healthObserve
	healthEnforce
)

func (m healthMode) String() string {
	switch m {
	case healthObserve:
		return "observe"
	case healthEnforce:
		return "enforce"
	}
	return "off"
}

var (
	// healthModeV / healthEnforceFor are written once in main() before the
	// listener starts, then only read.
	healthModeV healthMode
	// healthEnforceFor is the enforce:<class,…> allowlist. nil under bare
	// `enforce` (= every class) and in every other mode. Unknown class names
	// are kept — they may name a class this proxy has not fetched yet; the
	// spec's "logged and ignored" happens where the allowlist is consulted
	// (§8), which no Stage-0 code does.
	healthEnforceFor map[string]bool
	// All-degraded synthetic refusals require an explicit rollout opt-in.
	// Pinned refusals remain part of the pin contract in every enforce stage.
	healthAllDegradedEnabled bool

	// nowFn is the ONE clock the health machinery may read (spec §7.7):
	// monotonic differences for holds and windows, swapped by tests, never
	// time.Now directly in health code.
	nowFn = time.Now
)

// healthEnforceClasses renders the allowlist for logs: "all" when nil.
func healthEnforceClasses() string {
	if healthEnforceFor == nil {
		return "all"
	}
	out := make([]string, 0, len(healthEnforceFor))
	for c := range healthEnforceFor {
		out = append(out, c)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// parseHealthMode maps SWITCH_HEALTH to a mode (spec §8):
//
//	off | observe | enforce | enforce:<class>[,<class>…]
//
// Anything else — a typo, or an enforce: with no classes — falls back to off
// with a log line, never fatally: default-off is the rollout's safety story.
func parseHealthMode(v string) (healthMode, map[string]bool) {
	v = strings.TrimSpace(v)
	switch {
	case v == "" || strings.EqualFold(v, "off"):
		return healthOff, nil
	case strings.EqualFold(v, "observe"):
		return healthObserve, nil
	case strings.EqualFold(v, "enforce"):
		return healthEnforce, nil
	}
	if p := "enforce:"; len(v) > len(p) && strings.EqualFold(v[:len(p)], p) {
		classes := map[string]bool{}
		for _, c := range strings.Split(v[len(p):], ",") {
			if c = strings.TrimSpace(c); c != "" {
				classes[c] = true
			}
		}
		if len(classes) > 0 {
			return healthEnforce, classes
		}
	}
	log.Printf("⚠️  SWITCH_HEALTH=%q not understood (off | observe | enforce | enforce:<class,…>) — running off", v)
	return healthOff, nil
}

// ---------------------------------------------------------------------------
// #121 (113/T2) — the four-state FSM, probe lease, and ramp bookkeeping as a
// pure library (spec §4). Nothing on the request path references a healthStore
// yet: evidence producers arrive with #122, routing consumption with #123.
// The library never reads healthModeV — mode gating belongs to its callers.

// Health states (§4.2). Strings, so persistence and the status surface (#125)
// render them without a mapping layer.
const (
	hsHealthy      = "healthy"
	hsOpen         = "open"
	hsHalfOpen     = "half_open"
	hsConvalescent = "convalescent"
)

// Probe settle outcomes (§4.2).
const (
	outcomeProbeOK        = "probe_ok"
	outcomeProbeDeg       = "probe_deg"
	outcomeProbeAbandoned = "probe_abandoned"
)

// healthCfg carries §8's knobs plus the derived constants. #121 runs on the
// defaults; the compound-knob env parsing ships with the wiring (#123/#125).
type healthCfg struct {
	tripRate     float64       // rate-arm threshold over the window
	tripMin      int           // minimum window samples before the rate arm may fire
	tripConsec   int           // consecutive-deg arm length (arms C and S)
	tripWindow   time.Duration // evidence window for the rate arm
	holdBase     time.Duration // hold(1)
	holdMax      time.Duration // hold ceiling
	convalescent time.Duration // CONVALESCENT_S
	probeOKN     int           // consecutive probe oks that close an episode
	probeMaxIn   int           // probe size gate, est input tokens
	probeTTL     time.Duration // lease TTL before tick releases the token
	returnDwell  time.Duration // floor under any scheduled ramp return
	rampPeriod   time.Duration // spacing between scheduled ramp returns
	stateFile    string        // SWITCH_HEALTH_STATE_FILE; empty = no persistence

	// Derived (§8; echoed on the boot line by #125).
	kMax          int           // smallest k whose hold reaches holdMax
	ringN         int           // evidence ring capacity
	bucketS       time.Duration // rule-1 admission bucket
	abandonNotice int           // probeAbandons threshold surfaced by #124
}

func defaultHealthCfg() healthCfg {
	c := healthCfg{
		tripRate:     0.50,
		tripMin:      4,
		tripConsec:   3,
		tripWindow:   600 * time.Second,
		holdBase:     60 * time.Second,
		holdMax:      1800 * time.Second,
		convalescent: 600 * time.Second,
		probeOKN:     2,
		probeMaxIn:   32000,
		probeTTL:     120 * time.Second,
		returnDwell:  900 * time.Second,
		rampPeriod:   60 * time.Second,
	}
	c.derive()
	return c
}

// derive fills the §8 derived constants from the primary knobs.
func (c *healthCfg) derive() {
	c.bucketS = 5 * time.Second
	c.abandonNotice = 3
	k := 1
	for c.hold(k) < c.holdMax {
		k++
	}
	c.kMax = k
	n := 2 * int(c.tripWindow/c.bucketS)
	ring := 1
	for ring < n {
		ring <<= 1
	}
	c.ringN = ring
}

// hold implements the §4.5 ladder: min(HOLD_BASE·2^(k-1), HOLD_MAX).
func (c healthCfg) hold(k int) time.Duration {
	if k < 1 {
		k = 1
	}
	h := c.holdBase
	for i := 1; i < k; i++ {
		h *= 2
		if h >= c.holdMax {
			return c.holdMax
		}
	}
	if h > c.holdMax {
		return c.holdMax
	}
	return h
}

// sample is one hop's worth of evidence (§2). Forced samples are stored for
// the status surface but excluded from E, from k, and from probe accounting
// (I5).
type sample struct {
	t      time.Time
	ok     bool
	cause  string // §3 cause vocabulary; "" for ok
	class  string
	reqID  string
	fp     uint64 // healthFP of the buffered REQUEST body (§4.4, build-5); 0 for ok
	forced bool
	probe  bool
}

// healthFP fingerprints a failure body for the distinct-shape test in trip
// arms C/S (FNV-1a: cheap, stable, deliberately non-cryptographic).
func healthFP(body []byte) uint64 {
	f := fnv.New64a()
	f.Write(body)
	return f.Sum64()
}

// probeLease is the settled-once handle for one probe hop (§4.1, r10). The
// CAS makes a double settle provably inert; the currency test in settleLocked
// downgrades a stale settle to ordinary evidence.
type probeLease struct {
	key     string
	ep      string
	id      string
	class   string
	member  string
	until   time.Time
	settled atomic.Bool
}

// sessRamp tracks one session detoured off its home member (§4.6).
type sessRamp struct {
	movedAt    time.Time
	dwellUntil time.Time // zero until the episode closes and a return is scheduled
}

// classEp is one class's election for an endpoint_key's episode (§4.1).
type classEp struct {
	ep           string
	member       string
	prefillsPaid int
	sessions     map[string]*sessRamp
}

// epState is the whole of what the proxy remembers about one endpoint_key
// (§4.1). Every field is guarded by healthStore.mu.
type epState struct {
	key   string // this state's endpoint_key, so counter sites can label themselves
	state string
	since time.Time
	ep    string // episode id <key>@<unix_open_ts>; retained after close

	k     int
	lastK int

	lastClose time.Time
	holdUntil time.Time
	gateUntil time.Time

	probeTok       bool
	probeID        string
	probeTokUntil  time.Time
	probeOK        int
	probeAbandons  int
	lease          *probeLease // outstanding lease, so tick's TTL release can settle it
	lastClaimAt    time.Time
	waitingTraffic bool

	ring     []sample
	perClass map[string]*classEp

	absent      bool
	absentSince time.Time
}

// healthStore owns every epState under one mutex (§7.5). The lock is never
// held across a dial; settleLease's CAS runs outside it.
type healthStore struct {
	mu            sync.Mutex
	authFailures  map[string]int64
	bindingHealth map[string]bindingHealth
	pins          map[string]classPinHealth
	cfg           healthCfg
	keys          map[string]*epState
	dirty         bool // a transition happened since the last persist

	episodes      map[string]int64    // trips per endpoint_key
	doubleSettles map[string]int64    // lease_double_settle_total per key
	transitions   map[[3]string]int64 // {key, from, to} counts
	wouldAct      map[string]int64    // switch_health_would_act_total per class (§5.5 N24)

	// §6.2(1) cumulative counters. The ring is a bounded window and dies with
	// the process; these are monotonic for the whole run, so Prometheus keeps
	// the history and a restart reads as an ordinary counter reset.
	lastTick time.Time           // §7.6: a stale tick under up==1 is a dead goroutine
	samples  map[sampleCtr]int64 // samples_total{key,class,outcome,cause,forced}
	routeDec map[[3]string]int64 // route_decisions_total{class,hop_kind,reason}
	exhausts map[[2]string]int64 // exhausted_total{class,pinned}
}

// sampleCtr labels one admitted-evidence counter. Bounded by construction:
// ≤6 keys × ≤6 classes × 2 outcomes × §3's 9 cause tokens.
type sampleCtr struct {
	key, class, outcome, cause string
	forced                     bool
}

func newHealthStore(cfg healthCfg) *healthStore {
	return &healthStore{
		cfg:           cfg,
		keys:          map[string]*epState{},
		episodes:      map[string]int64{},
		doubleSettles: map[string]int64{},
		transitions:   map[[3]string]int64{},
		wouldAct:      map[string]int64{},
		samples:       map[sampleCtr]int64{},
		routeDec:      map[[3]string]int64{},
		exhausts:      map[[2]string]int64{},
	}
}

func (h *healthStore) keyLocked(key string) *epState {
	st := h.keys[key]
	if st == nil {
		st = &epState{key: key, state: hsHealthy, since: nowFn(), perClass: map[string]*classEp{}}
		h.keys[key] = st
	}
	return st
}

func (h *healthStore) countLocked(st *epState, from, to string) {
	h.transitions[[3]string{st.key, from, to}]++
	h.publishTransitionLocked(st, from, to)
}

// admitLocked applies §4.1's admission rules: one sample per (req_id, key)
// with deg winning (rule 0); one per (class, 5s bucket) with the first deg
// winning (rule 1); a per-class share cap of ringN/2 evicting that class's
// oldest (rule 2); then the global cap evicting the global oldest.
func (h *healthStore) admitLocked(st *epState, s sample) {
	if s.reqID != "" {
		for i := range st.ring {
			if st.ring[i].reqID == s.reqID {
				if st.ring[i].ok && !s.ok {
					st.ring[i] = s // deg replaces the ok in place
					h.countSampleLocked(st, s)
				}
				return // existing deg wins; a second ok drops
			}
		}
	}
	b := s.t.UnixNano() / int64(h.cfg.bucketS)
	for i := range st.ring {
		r := st.ring[i]
		if r.class == s.class && r.t.UnixNano()/int64(h.cfg.bucketS) == b {
			if r.ok && !s.ok {
				st.ring[i] = s // first deg in the bucket wins over an ok
				h.countSampleLocked(st, s)
			}
			return // else the incumbent stands
		}
	}
	classN := 0
	for _, r := range st.ring {
		if r.class == s.class {
			classN++
		}
	}
	if classN >= h.cfg.ringN/2 {
		for i, r := range st.ring {
			if r.class == s.class {
				st.ring = append(st.ring[:i], st.ring[i+1:]...)
				break
			}
		}
	}
	if len(st.ring) >= h.cfg.ringN {
		st.ring = st.ring[1:]
	}
	st.ring = append(st.ring, s)
	h.countSampleLocked(st, s)
}

// countSampleLocked is the durable half of the ring: the ring answers "is this
// key tripping right now" over a 600 s window, this answers "did the classifier
// ever score anything, and why" for the whole run — the question an observe
// week actually asks, which no bounded window can carry.
func (h *healthStore) countSampleLocked(st *epState, s sample) {
	outcome, cause := "ok", s.cause
	if !s.ok {
		outcome = "deg"
	}
	if s.ok {
		cause = ""
	}
	h.samples[sampleCtr{key: st.key, class: s.class, outcome: outcome, cause: cause, forced: s.forced}]++
}

// tripLocked evaluates E (§4.4) over the non-forced samples in the ring.
func (h *healthStore) tripLocked(st *epState) bool {
	now := nowFn()
	live := make([]sample, 0, len(st.ring))
	for _, s := range st.ring {
		if !s.forced {
			live = append(live, s)
		}
	}
	// Arm R: deg rate over the window.
	okN, degN := 0, 0
	for _, s := range live {
		if now.Sub(s.t) <= h.cfg.tripWindow {
			if s.ok {
				okN++
			} else {
				degN++
			}
		}
	}
	if n := okN + degN; n >= h.cfg.tripMin && float64(degN)/float64(n) >= h.cfg.tripRate {
		return true
	}
	// Arms C and S: the last tripConsec samples all deg with ≥2 distinct
	// shapes. S is C without the time bound, so one tail test covers both —
	// the ring's own retention is the only clock, which is what saves the
	// solo night tenant (§4.4).
	if len(live) >= h.cfg.tripConsec {
		fps := map[uint64]struct{}{}
		for _, s := range live[len(live)-h.cfg.tripConsec:] {
			if s.ok {
				return false
			}
			fps[s.fp] = struct{}{}
		}
		if len(fps) >= 2 {
			return true
		}
	}
	return false
}

func (h *healthStore) dropTokenLocked(st *epState) {
	st.probeTok = false
	st.probeID = ""
	st.probeTokUntil = time.Time{}
	st.lease = nil
}

// openLocked mints a new episode (healthy/convalescent → open on E).
func (h *healthStore) openLocked(st *epState, key string, k int) {
	now := nowFn()
	from := st.state
	if k > h.cfg.kMax {
		k = h.cfg.kMax
	}
	st.state = hsOpen
	st.since = now
	st.ep = key + "@" + strconv.FormatInt(now.Unix(), 10)
	st.k = k
	st.holdUntil = now.Add(h.cfg.hold(k))
	h.dropTokenLocked(st)
	st.probeOK = 0
	st.probeAbandons = 0
	// I3: a re-trip cancels every pending return; the session roster stays so
	// a later close can schedule the returns again.
	for _, ce := range st.perClass {
		for _, sr := range ce.sessions {
			sr.dwellUntil = time.Time{}
		}
	}
	h.episodes[key]++
	h.countLocked(st, from, hsOpen)
	h.dirty = true
}

// reopenLocked pushes half_open back to open inside the same episode.
func (h *healthStore) reopenLocked(st *epState, k int) {
	now := nowFn()
	if k > h.cfg.kMax {
		k = h.cfg.kMax
	}
	st.state = hsOpen
	st.since = now
	st.k = k
	st.holdUntil = now.Add(h.cfg.hold(k))
	h.dropTokenLocked(st)
	st.probeOK = 0
	h.countLocked(st, hsHalfOpen, hsOpen)
	h.dirty = true
}

// closeLocked ends the episode (PROBE_OK_N probes came home) and schedules
// the ramp (§4.6): return i in sorted-session-id order dwells until
// max(now + jitter(i·RAMP_PERIOD), movedAt + RETURN_DWELL).
func (h *healthStore) closeLocked(st *epState) {
	now := nowFn()
	st.state = hsConvalescent
	st.since = now
	st.lastK = st.k
	st.lastClose = now
	h.dropTokenLocked(st)
	st.probeOK = 0
	for _, ce := range st.perClass {
		if ce.ep != st.ep {
			continue
		}
		ids := make([]string, 0, len(ce.sessions))
		for id := range ce.sessions {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for i, id := range ids {
			sr := ce.sessions[id]
			d := now.Add(jitterDur(id, time.Duration(i)*h.cfg.rampPeriod))
			if floor := sr.movedAt.Add(h.cfg.returnDwell); floor.After(d) {
				d = floor
			}
			sr.dwellUntil = d
		}
	}
	h.countLocked(st, hsHalfOpen, hsConvalescent)
	h.dirty = true
}

// healthyLocked retires the key to healthy and clears the re-trip ladder.
func (h *healthStore) healthyLocked(st *epState, from string) {
	st.state = hsHealthy
	st.since = nowFn()
	st.lastK = 0
	h.countLocked(st, from, hsHealthy)
	h.dirty = true
}

// jitterDur applies the ±20% ramp jitter deterministically (hash of the
// session id, not math/rand — reproducible under test, stable across ticks).
func jitterDur(sess string, base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	f := fnv.New64a()
	f.Write([]byte(sess))
	frac := 0.8 + 0.4*float64(f.Sum64()%1000)/999.0
	return time.Duration(float64(base) * frac)
}

// record admits one non-probe sample for key and applies §4.3's evidence
// cells. Probe outcomes go through settleLease instead.
func (h *healthStore) record(key string, s sample) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s.t.IsZero() {
		s.t = nowFn()
	}
	st := h.keyLocked(key)
	h.admitLocked(st, s)
	h.evidenceLocked(st, key, s.ok, s.forced)
	h.persistIfDirtyLocked()
}

// evidenceLocked is the ok/deg column of §4.3. Forced samples are already in
// the ring for the status surface but never trip E, advance k, or drop a
// token (I5).
func (h *healthStore) evidenceLocked(st *epState, key string, ok bool, forced bool) {
	now := nowFn()
	switch st.state {
	case hsHealthy:
		if !ok && !forced && h.tripLocked(st) {
			h.openLocked(st, key, 1)
		}
	case hsConvalescent:
		if ok {
			if now.Sub(st.since) >= h.cfg.convalescent {
				h.healthyLocked(st, hsConvalescent)
			}
			return
		}
		if !forced && h.tripLocked(st) {
			h.openLocked(st, key, st.lastK+1)
		}
	case hsOpen:
		// Evidence accumulates; only tick moves an open key.
	case hsHalfOpen:
		if !ok && !forced {
			h.reopenLocked(st, st.k+1)
		}
	}
}

// claimProbe hands the half-open token to a routed hop (§4.5): state must be
// half_open with the token present and the member present, and the size gate
// holds est_input under probeMaxIn until gateUntil lifts it. Returns nil when
// the hop must route normally.
func (h *healthStore) claimProbe(key, class, member, id string, estInput int) *probeLease {
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.keys[key]
	if st == nil || st.state != hsHalfOpen || !st.probeTok || st.absent {
		return nil
	}
	now := nowFn()
	if estInput > h.cfg.probeMaxIn && now.Before(st.gateUntil) {
		return nil
	}
	st.probeTok = false
	st.probeID = id
	st.probeTokUntil = now.Add(h.cfg.probeTTL)
	st.lastClaimAt = now
	l := &probeLease{key: key, ep: st.ep, id: id, class: class, member: member, until: st.probeTokUntil}
	st.lease = l
	return l
}

// adoptProbe is claimProbe for a hop that was already routed at the key for
// its own reasons and merely adopts the probe role (§4.5). Same admission —
// two names so call sites read correctly.
func (h *healthStore) adoptProbe(key, class, member, id string, estInput int) *probeLease {
	return h.claimProbe(key, class, member, id, estInput)
}

// settleLease scores one probe hop exactly once (§4.1). The CAS runs outside
// the store lock; a lost CAS is counted and otherwise inert. A settle that is
// no longer current — the token was dropped, the episode moved on — downgrades
// to ordinary evidence and touches no token state it does not own.
func (h *healthStore) settleLease(l *probeLease, outcome string, s sample) {
	if l == nil {
		return
	}
	if !l.settled.CompareAndSwap(false, true) {
		h.mu.Lock()
		h.doubleSettles[l.key]++
		h.mu.Unlock()
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.settleLocked(l, outcome, s)
	h.persistIfDirtyLocked()
}

// settleLocked applies a won settle under the store lock. tick calls it
// directly for the TTL release — it already holds mu and has won the CAS.
func (h *healthStore) settleLocked(l *probeLease, outcome string, s sample) {
	st := h.keys[l.key]
	if st == nil {
		return
	}
	if s.t.IsZero() {
		s.t = nowFn()
	}
	if s.class == "" {
		s.class = l.class
	}
	s.probe = true
	current := st.state == hsHalfOpen && st.probeID == l.id && st.ep == l.ep
	if !current {
		// Guarded cleanup: only fields this lease still owns.
		if st.probeID == l.id {
			st.probeID = ""
			st.probeTokUntil = time.Time{}
			st.lease = nil
		}
		switch outcome {
		case outcomeProbeOK:
			s.ok = true
			h.admitLocked(st, s)
			h.evidenceLocked(st, l.key, true, s.forced)
		case outcomeProbeDeg:
			s.ok = false
			h.admitLocked(st, s)
			h.evidenceLocked(st, l.key, false, s.forced)
		case outcomeProbeAbandoned:
			// No evidence either way.
		}
		return
	}
	switch outcome {
	case outcomeProbeOK:
		s.ok = true
		h.admitLocked(st, s)
		st.probeID = ""
		st.probeTokUntil = time.Time{}
		st.lease = nil
		st.probeOK++
		st.probeAbandons = 0
		if st.probeOK >= h.cfg.probeOKN {
			h.closeLocked(st)
		} else {
			st.probeTok = true // re-arm for the next probe
		}
	case outcomeProbeDeg:
		s.ok = false
		h.admitLocked(st, s)
		h.reopenLocked(st, st.k+1)
	case outcomeProbeAbandoned:
		st.probeID = ""
		st.probeTokUntil = time.Time{}
		st.lease = nil
		st.probeTok = true
		st.probeAbandons++
	}
}

// tick is the 60s maintenance pass (§4.3's tick column): GC first, then the
// absent freeze, hold expiry (open → half_open, token minted), the probe TTL
// release, and convalescent → healthy.
func (h *healthStore) tick() {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := nowFn()
	h.lastTick = now
	for key, st := range h.keys {
		if st.absent {
			// GC is tick's alone (§7.6): a healthy absent key goes at once,
			// any absent key after 24h.
			if st.state == hsHealthy || now.Sub(st.absentSince) >= 24*time.Hour {
				delete(h.keys, key)
				h.dirty = true
			}
			continue // frozen: absence pauses every expiry
		}
		switch st.state {
		case hsOpen:
			if !now.Before(st.holdUntil) {
				st.state = hsHalfOpen
				st.since = now
				st.probeTok = true
				st.probeID = ""
				st.probeTokUntil = time.Time{}
				st.lease = nil
				st.probeOK = 0
				st.probeAbandons = 0
				st.gateUntil = now.Add(h.cfg.hold(st.k))
				st.lastClaimAt = time.Time{}
				h.countLocked(st, hsOpen, hsHalfOpen)
				h.dirty = true
			}
		case hsHalfOpen:
			if st.probeID != "" && !now.Before(st.probeTokUntil) {
				// The probe never came home: release the token (BL-1).
				// Winning the CAS here makes the zombie's own eventual
				// settle the counted no-op.
				if l := st.lease; l != nil && l.settled.CompareAndSwap(false, true) {
					h.settleLocked(l, outcomeProbeAbandoned, sample{})
				} else {
					st.probeTok = true
					st.probeID = ""
					st.probeTokUntil = time.Time{}
					st.lease = nil
					st.probeAbandons++
				}
			}
			st.waitingTraffic = st.probeTok && st.lastClaimAt.IsZero() && now.Sub(st.since) >= h.cfg.hold(st.k)
		case hsConvalescent:
			if now.Sub(st.since) >= h.cfg.convalescent {
				h.healthyLocked(st, hsConvalescent)
			}
		}
	}
	h.persistLocked()
}

// memberAbsent marks a key that left the registry (§4.2 member_absent): its
// state is kept but frozen; tick alone GCs it (§7.6).
func (h *healthStore) memberAbsent(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.keys[key]
	if st == nil || st.absent {
		return
	}
	st.absent = true
	st.absentSince = nowFn()
}

// memberPresent unfreezes a key that returned to the registry.
func (h *healthStore) memberPresent(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.keys[key]
	if st == nil || !st.absent {
		return
	}
	st.absent = false
	st.absentSince = time.Time{}
}

// elect runs the per-(class, episode) detour election (§4.6, I1): the first
// hop of a class during an episode seats its member; every later hop reuses
// it. A new episode supersedes the old election but keeps the session roster.
func (h *healthStore) elect(key, class, member string) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.keys[key]
	if st == nil || st.ep == "" {
		return member, false
	}
	ce := st.perClass[class]
	if ce != nil && ce.ep == st.ep && ce.member != "" {
		return ce.member, false
	}
	if ce == nil {
		ce = &classEp{sessions: map[string]*sessRamp{}}
		if st.perClass == nil {
			st.perClass = map[string]*classEp{}
		}
		st.perClass[class] = ce
	}
	ce.ep = st.ep
	ce.member = member
	ce.prefillsPaid = 0
	h.dirty = true
	h.persistIfDirtyLocked()
	return member, true
}

// noteSessionDetour records that a session was moved off its home member
// during the episode; movedAt is written once. Anonymous traffic passes no
// session id and gets no dwell bookkeeping (R-12).
func (h *healthStore) noteSessionDetour(key, class, sess string) {
	if sess == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.keys[key]
	if st == nil {
		return
	}
	ce := st.perClass[class]
	if ce == nil || ce.ep != st.ep {
		return
	}
	if ce.sessions[sess] == nil {
		ce.sessions[sess] = &sessRamp{movedAt: nowFn()}
	}
}

// rampHolds answers "does the ramp keep this session on the detour member?"
// (§4.6). It holds only while the source key is neither open nor half_open
// (BL-9: the ramp outlives convalescent), the session's dwell is ahead, and
// the session's binding is not newer than the close. A passed dwell deletes
// the entry — the session goes home — and a drained election is dropped.
func (h *healthStore) rampHolds(key, class, sess string, bindingAt time.Time) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.rampHoldsLocked(key, class, sess, bindingAt)
}

// rampHoldsLocked is rampHolds under an already-held h.mu — routeHints asks
// it in the same breath as the rest of §5.2's pre-ranking read.
func (h *healthStore) rampHoldsLocked(key, class, sess string, bindingAt time.Time) (string, bool) {
	if sess == "" {
		return "", false
	}
	st := h.keys[key]
	if st == nil {
		return "", false
	}
	if st.state == hsOpen || st.state == hsHalfOpen {
		return "", false // degraded source: nothing to ramp back to yet (I3)
	}
	ce := st.perClass[class]
	if ce == nil {
		return "", false
	}
	sr := ce.sessions[sess]
	if sr == nil {
		return "", false
	}
	now := nowFn()
	if sr.dwellUntil.IsZero() || !sr.dwellUntil.After(now) {
		delete(ce.sessions, sess)
		if len(ce.sessions) == 0 {
			delete(st.perClass, class)
			h.dirty = true
		}
		return "", false
	}
	if !bindingAt.IsZero() && !st.lastClose.IsZero() && bindingAt.After(st.lastClose) {
		return "", false
	}
	return ce.member, true
}

// ---- #122: the store instance and hopOutcome's one consumer ----

// health is the process-wide store. nil unless main() armed observe/enforce —
// every producer nil-guards on it, so `off` runs zero health code (E0). Written
// once in main() before the listener starts, then only read.
var health *healthStore

// healthLargePromptTok is §8's SWITCH_HEALTH_LARGE_PROMPT_TOK: the ring's
// forced gate (R-22) and nothing else.
var healthLargePromptTok = 64000

// estInputTokens estimates a buffered request body's input tokens — §5.2's
// body walk, the standard ~4 bytes/token heuristic. Used by the forced gate
// (R-22); #123's probe size gate reuses it.
func estInputTokens(body []byte) int { return len(body) / 4 }

// healthReqCtr mints sample.reqID — ring rule 0's per-request dedup key.
var healthReqCtr atomic.Uint64

// healthReqID returns a fresh request id, or "" when no store is armed (the
// producers won't run, so there is nothing to correlate).
func healthReqID() string {
	if health == nil {
		return ""
	}
	return nextDecisionID()
}

// healthRecord is hopOutcome's ONE consumer (§3 Rule 1). It does three things:
// write the evidence sample, run Rule 4, settle the lease. Producers hand it
// terminal outcomes only (Rule 3), after the last byte was forwarded — it
// takes healthMu off the client's critical path (§0.8).
func healthRecord(out hopOutcome, class, reqID string, fp uint64, forced bool) {
	h := health
	if h == nil || out.key == "" {
		return
	}
	s := sample{class: class, reqID: reqID, forced: forced}
	switch out.evidence {
	case evOK:
		s.ok = true
	case evDeg:
		s.cause, s.fp = out.cause, fp
	}
	if l := out.lease; l != nil {
		// Rule 4 — a no-evidence row still ends the probe.
		oc := outcomeProbeAbandoned
		switch out.evidence {
		case evOK:
			oc = outcomeProbeOK
		case evDeg:
			oc = outcomeProbeDeg
		}
		s.probe = true
		h.settleLease(l, oc, s)
		return
	}
	if out.evidence == evNone {
		return // no sample; Rule 4 was vacuous without a token
	}
	h.record(out.key, s)
}

// hopScore carries the identity a SHAPE site needs to score its terminal row
// after the head was forwarded (§3 Rule 3): built where the hop's route info
// is still in hand, settled once at the site's exit. nil = scoring off.
type hopScore struct {
	ctx   context.Context // the hop request's context — a client hang-up cancels it
	key   string          // served member's endpoint_key
	cc    *classRoute
	lease *probeLease // non-nil iff the hop holds the probe token (#123 claims it)
}

// settle fills the identity columns and hands the outcome to healthRecord.
func (hs *hopScore) settle(out hopOutcome) {
	if hs != nil && hs.cc != nil {
		hs.cc.trace.outcome(out)
	}
	if hs == nil || health == nil || hs.cc == nil || hs.key == "" {
		return
	}
	out.key, out.lease = hs.key, hs.lease
	if out.evidence == evNone && out.lease == nil {
		return // nothing to write and no token to end — skip the fp/forced work
	}
	forced := soleAdmissible(hs.cc, hs.key)
	hs.cc.trace.forced(forced)
	healthRecord(out, hs.cc.class, hs.cc.reqID, healthFP(hs.cc.origBody), forced)
}

// hopCtx is hs.ctx, nil-safe for the translator's defer.
func (hs *hopScore) hopCtx() context.Context {
	if hs == nil {
		return nil
	}
	return hs.ctx
}

// noteRouteDecision records one class hop's route-time outcome
// (route_decisions_total{class,hop_kind,reason}, §6.2(1)). Called once per
// class request in every armed mode — under observe the hop kind is always
// what the old router chose, which is exactly the baseline enforce is
// measured against.
func (h *healthStore) noteRouteDecision(class, hopKind, reason string) {
	if hopKind == "" {
		hopKind = "none"
	}
	h.mu.Lock()
	h.routeDec[[3]string{class, hopKind, reason}]++
	h.mu.Unlock()
}

// noteExhausted counts a synthetic 529 (§5.4). Under observe this stays 0 by
// construction — nothing mints one — so a non-zero value on the observe
// dashboard means the mode flipped, not that a class died.
func (h *healthStore) noteExhausted(class string, pinned bool) {
	p := "false"
	if pinned {
		p = "true"
	}
	h.mu.Lock()
	h.exhausts[[2]string{class, p}]++
	h.mu.Unlock()
}

// ---- #123: the routing consumer's read surface (§5.2) ----

// routeHints is §5.2's pre-ranking read: ONE lock acquisition answers
// everything the fast path needs (bound key state, any half-open key in the
// class, the ramp hold) plus what the lazy path wants from the same breath
// (the episode's open instant for PR5). The spec keeps a per-class
// halfOpenCount maintained beside the store; a bounded scan of the member ids
// under the same lock is equivalent and adds no cross-plane bookkeeping.
type routeHints struct {
	boundState  string    // bound key's FSM state; "" = never seen
	epOpenedAt  time.Time // when the bound key's episode opened (zero = none)
	halfOpenAny bool      // some present member key is HALF_OPEN (probe pre-emption, PR2)
	rampTo      string    // member the ramp holds this session on (§4.6)
	rampHolds   bool
}

func (h *healthStore) routeHints(boundKey string, memberIDs []string, class, sess string, bindingAt time.Time) routeHints {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out routeHints
	if st := h.keys[boundKey]; st != nil {
		out.boundState = st.state
		out.epOpenedAt = epOpenedAtOf(st.ep)
	}
	for _, id := range memberIDs {
		if st := h.keys[id]; st != nil && !st.absent && st.state == hsHalfOpen {
			out.halfOpenAny = true
			break
		}
	}
	out.rampTo, out.rampHolds = h.rampHoldsLocked(boundKey, class, sess, bindingAt)
	return out
}

// epOpenedAtOf parses the open instant out of an episode id (<key>@<unix>).
// ep survives close by design, so callers gate on state before trusting it.
func epOpenedAtOf(ep string) time.Time {
	i := strings.LastIndexByte(ep, '@')
	if i < 0 {
		return time.Time{}
	}
	ts, err := strconv.ParseInt(ep[i+1:], 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(ts, 0)
}

// stateOf is one key's current FSM state ("" = never seen).
func (h *healthStore) stateOf(key string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if st := h.keys[key]; st != nil {
		return st.state
	}
	return ""
}

// statesOf is stateOf for the ranker: every id in one lock acquisition.
// Absent ids are simply missing from the map (zero value "").
func (h *healthStore) statesOf(ids []string) map[string]string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]string, len(ids))
	for _, id := range ids {
		if st := h.keys[id]; st != nil {
			out[id] = st.state
		}
	}
	return out
}

// electionFor reads the bound key's seated election WITHOUT seating one
// (§5.2): a live episode, still-degraded key, and a seated member — else
// ok=false and the caller runs elect() on its ranked pick. ep survives close
// by design, so the state test is the election's currency test.
func (h *healthStore) electionFor(key, class string) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	st := h.keys[key]
	if st == nil || st.ep == "" {
		return "", false
	}
	if st.state != hsOpen && st.state != hsHalfOpen {
		return "", false
	}
	ce := st.perClass[class]
	if ce == nil || ce.ep != st.ep || ce.member == "" {
		return "", false
	}
	return ce.member, true
}

// keyDesc is one endpoint key's public condition — §5.4's message material.
type keyDesc struct {
	id      string
	state   string
	since   time.Duration // time in the current state
	ringDeg int
	ringTot int
}

// describeKeys renders the named keys for the §5.4 exhaust message. Unknown
// ids are omitted.
func (h *healthStore) describeKeys(ids []string) []keyDesc {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := nowFn()
	out := make([]keyDesc, 0, len(ids))
	for _, id := range ids {
		st := h.keys[id]
		if st == nil {
			continue
		}
		deg := 0
		for _, s := range st.ring {
			if !s.ok {
				deg++
			}
		}
		out = append(out, keyDesc{id: id, state: st.state, since: now.Sub(st.since),
			ringDeg: deg, ringTot: len(st.ring)})
	}
	return out
}

// noteWouldAct bumps §5.5's would-act counter — computed under observe AND
// enforce so the two modes are comparable across the rollout (N24).
func (h *healthStore) noteWouldAct(class string) {
	h.mu.Lock()
	h.wouldAct[class]++
	h.mu.Unlock()
}

// syncRegistry freezes keys whose member left the registry and unfreezes the
// returning (#115): the roster flips exactly on the registry delta, under one
// lock, on every /preferred refresh that carried classes.
func (h *healthStore) syncRegistry(present map[string]bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := nowFn()
	for key, st := range h.keys {
		if present[key] {
			if st.absent {
				st.absent = false
				st.absentSince = time.Time{}
			}
		} else if !st.absent {
			st.absent = true
			st.absentSince = now
		}
	}
}

// healthEnforcedClass gates #123's routing consumption: enforce mode, and the
// class on the enforce allowlist (nil = every class). Observe keeps routing
// untouched — only the would-act counter runs.
func healthEnforcedClass(class string) bool {
	return health != nil && healthModeV == healthEnforce &&
		(healthEnforceFor == nil || healthEnforceFor[class])
}

// healthTickLoop drives §4.3's tick column and the §7.1 snapshot cadence.
// Started by main() only when a mode is armed.
func healthTickLoop() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for range t.C {
		health.tick()
		publishHealthSnapshot(nil)
	}
}
