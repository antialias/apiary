package main

// §6.2(1): Prometheus text exposition for the health store, written by hand
// exactly as the brain's exporter does it — the proxy's go.mod has no
// dependencies and gains none here.
//
// Why this exists at all: the ring is a 600 s / 256-sample window that dies
// with the process, so "what did the classifier do overnight" had no answer
// anywhere in the system. These counters are monotonic for the run and
// Prometheus keeps the history; a restart reads as an ordinary counter reset,
// which promql's rate()/increase() already handle.
//
// One lock hold renders everything, so every series in a scrape agrees with
// itself. Label cardinality is bounded by construction: ≤6 keys, ≤6 classes,
// 4 states, 8 hop kinds, 11 reasons, §3's 9 cause tokens.

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const metricPrefix = "switch_health_"

// metricsBuf accumulates one exposition. help() only REGISTERS a family; the
// header is written lazily, immediately before that family's first sample, so
// a family that turns out to have no series emits nothing at all. Declaring a
// family and then emitting no series is a lie about coverage — it reads on the
// dashboard as "measured, found nothing" when the truth is "never measured" —
// and making the emission structural means no call site has to remember.
type metricsBuf struct {
	b       strings.Builder
	prefix  string
	pending map[string]string
	written map[string]bool
}

func newMetricsBuf() *metricsBuf { return newMetricsBufPrefix(metricPrefix) }

// newMetricsBufPrefix is the same buffer under another family prefix: the
// request-path series (reqmetrics.go) are not health-store state, so they do
// not wear the switch_health_ prefix.
func newMetricsBufPrefix(prefix string) *metricsBuf {
	return &metricsBuf{prefix: prefix, pending: map[string]string{}, written: map[string]bool{}}
}

func (m *metricsBuf) help(name, typ, help string) {
	if m.written[name] {
		return
	}
	m.pending[name] = fmt.Sprintf("# HELP %s%s %s\n# TYPE %s%s %s\n", m.prefix, name, help, m.prefix, name, typ)
}

func (m *metricsBuf) header(name string) {
	if h, ok := m.pending[name]; ok {
		m.b.WriteString(h)
		delete(m.pending, name)
		m.written[name] = true
	}
}

// esc escapes a label value per the exposition format.
func esc(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	return strings.ReplaceAll(v, "\n", `\n`)
}

// add writes one sample. labels are k,v pairs; an empty value is still
// emitted, because a missing label and an empty one are different series.
func (m *metricsBuf) add(name string, v float64, labels ...string) {
	m.addSuffixed(name, "", v, labels...)
}

// addSuffixed writes one sample of family name under name+suffix — a
// histogram's _bucket/_sum/_count series share the family's one header.
func (m *metricsBuf) addSuffixed(name, suffix string, v float64, labels ...string) {
	m.header(name)
	m.b.WriteString(m.prefix)
	m.b.WriteString(name)
	m.b.WriteString(suffix)
	if len(labels) > 1 {
		m.b.WriteByte('{')
		for i := 0; i+1 < len(labels); i += 2 {
			if i > 0 {
				m.b.WriteByte(',')
			}
			m.b.WriteString(labels[i])
			m.b.WriteString(`="`)
			m.b.WriteString(esc(labels[i+1]))
			m.b.WriteByte('"')
		}
		m.b.WriteByte('}')
	}
	m.b.WriteByte(' ')
	m.b.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
	m.b.WriteByte('\n')
}

func (m *metricsBuf) ts(name string, t time.Time, labels ...string) {
	if t.IsZero() {
		return // an absent instant is a missing series, never a 1970 timestamp
	}
	m.add(name, float64(t.UnixNano())/1e9, labels...)
}

func boolF(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// renderMetrics is the whole of GET /_switch/metrics.
func (h *healthStore) renderMetrics() string {
	m := newMetricsBuf()
	now := nowFn()
	if r := healthRecovery; r != nil {
		r.mu.Lock()
		m.help("recovery_in_flight", "gauge", "Synthetic static-bearer recovery attempt in progress.")
		m.add("recovery_in_flight", boolF(r.busy))
		m.help("recovery_attempts_total", "counter", "Bounded background recovery attempts by result.")
		for _, result := range []string{"ok", "deadline", "degraded", "abandoned", "unavailable", "invalid_response"} {
			m.add("recovery_attempts_total", float64(r.totals[result]), "result", result)
		}
		r.mu.Unlock()
	}

	// ---- mode and config: what this process was told to do (§8) ----
	m.help("mode", "gauge", "Armed routing mode; 1 on the active mode.")
	for _, mode := range []healthMode{healthOff, healthObserve, healthEnforce} {
		m.add("mode", boolF(healthModeV == mode), "mode", mode.String())
	}
	m.help("enforce_class", "gauge", "1 for each class the §8 allowlist enforces.")
	for class := range healthEnforceFor {
		m.add("enforce_class", 1, "class", class)
	}
	m.help("config", "gauge", "Parsed §8 configuration values.")
	m.add("config", boolF(healthAllDegradedEnabled), "name", "all_degraded_529")
	m.add("config", h.cfg.tripRate, "name", "trip_rate")
	m.add("config", float64(h.cfg.tripMin), "name", "trip_min")
	m.add("config", float64(h.cfg.tripConsec), "name", "trip_consec")
	m.add("config", h.cfg.tripWindow.Seconds(), "name", "trip_window_s")
	m.add("config", h.cfg.holdBase.Seconds(), "name", "hold_base_s")
	m.add("config", h.cfg.holdMax.Seconds(), "name", "hold_max_s")
	m.add("config", float64(h.cfg.kMax), "name", "k_max")
	m.add("config", float64(h.cfg.ringN), "name", "ring_n")
	m.add("config", float64(h.cfg.probeOKN), "name", "probe_ok_n")
	m.add("config", h.cfg.returnDwell.Seconds(), "name", "return_dwell_s")
	m.add("config", h.cfg.convalescent.Seconds(), "name", "convalescent_s")
	m.add("config", float64(h.cfg.probeMaxIn), "name", "probe_max_in")
	m.add("config", h.cfg.probeTTL.Seconds(), "name", "probe_ttl_s")
	m.add("config", h.cfg.rampPeriod.Seconds(), "name", "ramp_period_s")
	m.add("config", h.cfg.bucketS.Seconds(), "name", "bucket_s")
	m.add("config", float64(h.cfg.abandonNotice), "name", "abandon_notice")

	h.mu.Lock()
	defer h.mu.Unlock()

	keys := make([]string, 0, len(h.keys))
	for k := range h.keys {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// ---- per-key FSM position ----
	if len(keys) > 0 {
		m.help("state", "gauge", "Endpoint-key FSM state; sum by (key) is always 1.")
		m.help("episode_k", "gauge", "Hold-ladder index k for the current or last episode.")
		m.help("absent", "gauge", "1 when the registry no longer lists this key.")
		m.help("since_timestamp_seconds", "gauge", "When the current state was entered.")
		m.help("evidence_samples", "gauge", "Non-forced ring samples inside the trip window.")
		m.help("evidence_rate", "gauge", "Degradation rate over the trip window; compare to trip_rate.")
		m.help("probe_abandons", "gauge", "Consecutive probe abandons on this key.")
	}
	for _, k := range keys {
		st := h.keys[k]
		for _, name := range []string{hsHealthy, hsOpen, hsHalfOpen, hsConvalescent} {
			m.add("state", boolF(st.state == name), "key", k, "state", name)
		}
		m.add("episode_k", float64(st.k), "key", k)
		m.add("absent", boolF(st.absent), "key", k)
		m.ts("since_timestamp_seconds", st.since, "key", k)
		m.ts("hold_until_timestamp_seconds", st.holdUntil, "key", k)
		m.ts("last_close_timestamp_seconds", st.lastClose, "key", k)

		// The §4.4 window, recomputed at scrape so the panel and the trip arm
		// read the same number.
		okN, degN := 0, 0
		for _, s := range st.ring {
			if s.forced || now.Sub(s.t) > h.cfg.tripWindow {
				continue
			}
			if s.ok {
				okN++
			} else {
				degN++
			}
		}
		m.add("evidence_samples", float64(okN+degN), "key", k)
		if n := okN + degN; n > 0 {
			m.add("evidence_rate", float64(degN)/float64(n), "key", k)
		} else {
			m.add("evidence_rate", 0, "key", k)
		}

		probe := "none"
		switch {
		case st.state != hsHalfOpen:
		case st.lease != nil:
			probe = "held"
		case st.probeTok && st.probeAbandons >= h.cfg.abandonNotice:
			probe = "blocked"
		case st.probeTok && st.waitingTraffic:
			probe = "waiting_for_traffic"
		case st.probeTok:
			probe = "unclaimed"
		}
		m.help("probe_state", "gauge", "Probe-token disposition on a half-open key.")
		for _, name := range []string{"none", "unclaimed", "held", "waiting_for_traffic", "blocked"} {
			m.add("probe_state", boolF(probe == name), "key", k, "state", name)
		}
		m.add("probe_abandons", float64(st.probeAbandons), "key", k)

		for class, ce := range st.perClass {
			if ce.member == "" {
				continue
			}
			m.help("election_info", "gauge", "The member each class elected for this key's episode.")
			m.add("election_info", 1, "key", k, "class", class, "member", ce.member, "episode", ce.ep)
			m.help("prefills_paid_total", "counter", "Prefills paid re-seating a class onto a detour member.")
			m.add("prefills_paid_total", float64(ce.prefillsPaid), "key", k, "class", class)
		}
	}

	m.help("auth_failures_total", "counter", "Upstream authentication failures by endpoint key.")
	for _, k := range sortedKeys(h.authFailures) {
		m.add("auth_failures_total", float64(h.authFailures[k]), "key", k)
	}
	pending := map[string]int{}
	for _, st := range h.keys {
		for class, ce := range st.perClass {
			if _, ok := pending[class]; !ok {
				pending[class] = 0
			}
			for _, sr := range ce.sessions {
				if sr.dwellUntil.After(now) {
					pending[class]++
				}
			}
		}
	}
	for class := range h.bindingHealth {
		if _, ok := pending[class]; !ok {
			pending[class] = 0
		}
	}
	for class := range h.pins {
		if _, ok := pending[class]; !ok {
			pending[class] = 0
		}
	}
	classes := make([]string, 0, len(pending))
	for c := range pending {
		classes = append(classes, c)
	}
	sort.Strings(classes)
	m.help("ramp_pending", "gauge", "Sessions still held on a detour by the recovery ramp.")
	m.help("pointer_fighting_health", "gauge", "Repeated new unpinned bindings to an open key in one episode.")
	for _, class := range classes {
		m.add("ramp_pending", float64(pending[class]), "class", class)
		m.add("pointer_fighting_health", boolF(h.fightingLocked(class)), "class", class)
	}

	// ---- cumulative counters: the durable record ----
	if len(h.episodes) > 0 {
		m.help("episodes_total", "counter", "Episodes opened (trips) per endpoint key.")
		for _, k := range sortedKeys(h.episodes) {
			m.add("episodes_total", float64(h.episodes[k]), "key", k)
		}
	}
	if len(h.transitions) > 0 {
		m.help("transitions_total", "counter", "FSM transitions by key and direction.")
		trs := make([][3]string, 0, len(h.transitions))
		for t := range h.transitions {
			trs = append(trs, t)
		}
		sort.Slice(trs, func(i, j int) bool { return lessTriple(trs[i], trs[j]) })
		for _, t := range trs {
			m.add("transitions_total", float64(h.transitions[t]), "key", t[0], "from", t[1], "to", t[2])
		}
	}
	if len(h.samples) > 0 {
		m.help("samples_total", "counter",
			"Ring-admitted evidence samples by outcome and §3 cause. The observe stage's primary series: it separates \"the classifier scored traffic and found it clean\" from \"the classifier never ran\".")
		ctrs := make([]sampleCtr, 0, len(h.samples))
		for c := range h.samples {
			ctrs = append(ctrs, c)
		}
		sort.Slice(ctrs, func(i, j int) bool {
			a, b := ctrs[i], ctrs[j]
			if a.key != b.key {
				return a.key < b.key
			}
			if a.class != b.class {
				return a.class < b.class
			}
			if a.outcome != b.outcome {
				return a.outcome < b.outcome
			}
			return a.cause < b.cause
		})
		for _, c := range ctrs {
			m.add("samples_total", float64(h.samples[c]),
				"key", c.key, "class", c.class, "outcome", c.outcome, "cause", c.cause,
				"forced", strconv.FormatBool(c.forced))
		}
	}
	if len(h.routeDec) > 0 {
		m.help("route_decisions_total", "counter", "Class hops by route-time hop kind and reason.")
		ds := make([][3]string, 0, len(h.routeDec))
		for d := range h.routeDec {
			ds = append(ds, d)
		}
		sort.Slice(ds, func(i, j int) bool { return lessTriple(ds[i], ds[j]) })
		for _, d := range ds {
			m.add("route_decisions_total", float64(h.routeDec[d]), "class", d[0], "hop_kind", d[1], "reason", d[2])
		}
	}
	if len(h.exhausts) > 0 {
		m.help("exhausted_total", "counter", "Synthetic 529s minted because every admissible member was open (§5.4).")
		for _, e := range sortedPairs(h.exhausts) {
			m.add("exhausted_total", float64(h.exhausts[e]), "class", e[0], "pinned", e[1])
		}
	}
	if len(h.wouldAct) > 0 {
		m.help("would_act_total", "counter", "Requests enforce would have routed differently (§9 Stage 1).")
		for _, c := range sortedKeys(h.wouldAct) {
			m.add("would_act_total", float64(h.wouldAct[c]), "class", c)
		}
	}
	// Always emitted, even at zero: this one is an assertion, not a
	// measurement — §3 Rule 3 says a double settle cannot happen, and a
	// missing series would be indistinguishable from a healthy one.
	m.help("lease_double_settle_total", "counter", "Probe leases settled twice; expected 0 forever (§4.1).")
	if len(h.doubleSettles) == 0 {
		m.add("lease_double_settle_total", 0, "key", "")
	}
	for _, k := range sortedKeys(h.doubleSettles) {
		m.add("lease_double_settle_total", float64(h.doubleSettles[k]), "key", k)
	}

	// §6.2(3): the inbound strip. These live outside the store because the strip
	// runs under SWITCH_HEALTH=off too, where there is no store — under off the
	// endpoint is not served either, so what these show is the enforce-mode view.
	// walks vs stripped separates "the sentinel appeared" from "a note this proxy
	// wrote was removed": a walk with no strip is a client quoting the convention
	// back, which is exactly what the grammar is there to leave alone.
	walks, stripped := noteStripTotals()
	m.help("note_strip_walks_total", "counter", "Class bodies that passed the sentinel gate and were token-walked (§5.5(5)).")
	for _, c := range sortedKeys(walks) {
		m.add("note_strip_walks_total", float64(walks[c]), "class", c)
	}
	m.help("notes_stripped_total", "counter", "Trailing [switch-proxy] notes removed from inbound history (§5.5(5)).")
	for _, c := range sortedKeys(stripped) {
		m.add("notes_stripped_total", float64(stripped[c]), "class", c)
	}
	m.help("notes_total", "counter", "Trailing response notes appended, by class and template (§5.5).")
	emitted := noteEmissionTotals()
	labels := make([][2]string, 0, len(emitted))
	for k := range emitted {
		labels = append(labels, k)
	}
	sort.Slice(labels, func(i, j int) bool {
		if labels[i][0] == labels[j][0] {
			return labels[i][1] < labels[j][1]
		}
		return labels[i][0] < labels[j][0]
	})
	for _, k := range labels {
		m.add("notes_total", float64(emitted[k]), "class", k[0], "note_kind", k[1])
	}

	// §7.6: a stale tick under up==1 is a dead goroutine, which Prometheus
	// cannot see any other way.
	m.help("last_tick_timestamp_seconds", "gauge", "When the tick loop last ran.")
	m.ts("last_tick_timestamp_seconds", h.lastTick)

	return m.b.String()
}

func sortedKeys(m map[string]int64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedPairs(m map[[2]string]int64) [][2]string {
	out := make([][2]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i][0] != out[j][0] {
			return out[i][0] < out[j][0]
		}
		return out[i][1] < out[j][1]
	})
	return out
}

func lessTriple(a, b [3]string) bool {
	if a[0] != b[0] {
		return a[0] < b[0]
	}
	if a[1] != b[1] {
		return a[1] < b[1]
	}
	return a[2] < b[2]
}
