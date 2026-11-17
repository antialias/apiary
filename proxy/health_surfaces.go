package main

import (
	"sort"
	"time"
)

type bindingHealth struct {
	key, episode string
	consecutive  int
}
type classPinHealth struct{ member, source string }

func evidenceLocked(h *healthStore, st *epState, class string, forced bool) healthEvidence {
	e := healthEvidence{Scope: "endpoint_key", WindowS: int(h.cfg.tripWindow.Seconds())}
	if class != "" {
		e.Scope = "class"
	}
	now := nowFn()
	for _, s := range st.ring {
		if s.forced != forced || now.Sub(s.t) > h.cfg.tripWindow || (class != "" && s.class != class) {
			continue
		}
		e.Samples++
		if !s.ok {
			e.Deg++
		}
	}
	if e.Samples > 0 {
		e.Rate = float64(e.Deg) / float64(e.Samples)
	}
	return e
}
func timeValue(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	s := t.UTC().Format(time.RFC3339Nano)
	return &s
}
func nextTransitionLocked(h *healthStore, st *epState) map[string]any {
	switch st.state {
	case hsOpen:
		return map[string]any{"to": hsHalfOpen, "at": timeValue(st.holdUntil), "in_s": max(0, int(st.holdUntil.Sub(nowFn()).Seconds()))}
	case hsHalfOpen:
		n := map[string]any{"to": hsConvalescent, "on": "probe", "probes_needed": max(0, h.cfg.probeOKN-st.probeOK), "next_probe_at": nil, "waiting_for_traffic": st.waitingTraffic}
		if st.probeAbandons >= h.cfg.abandonNotice {
			n["probe_blocked"] = "abandons"
		}
		return n
	case hsConvalescent:
		return map[string]any{"to": hsHealthy, "at": timeValue(st.since.Add(h.cfg.convalescent))}
	default:
		return map[string]any{}
	}
}
func keyHealthLocked(h *healthStore, st *epState) map[string]any {
	token := "none"
	if st.probeTok {
		token = "unclaimed"
	}
	if st.probeID != "" {
		token = "held"
	}
	classes := map[string]bool{}
	okN, degN := 0, 0
	lastCause := ""
	for _, s := range st.ring {
		classes[s.class] = true
		if s.ok {
			okN++
		} else {
			degN++
			lastCause = s.cause
		}
	}
	names := make([]string, 0, len(classes))
	for c := range classes {
		names = append(names, c)
	}
	sort.Strings(names)
	perClass := make([]map[string]any, 0, len(names))
	for _, c := range names {
		perClass = append(perClass, map[string]any{"class": c, "evidence": evidenceLocked(h, st, c, false)})
	}
	return map[string]any{
		"key": st.key, "state": st.state, "since": timeValue(st.since), "ep": nullable(st.ep), "episode": nullable(st.ep), "k": st.k,
		"hold_until": timeValue(st.holdUntil), "next_transition": nextTransitionLocked(h, st),
		"evidence": evidenceLocked(h, st, "", false), "per_class": perClass, "forced": evidenceLocked(h, st, "", true),
		"probe":  map[string]any{"token": token, "held_by": nullable(st.probeID), "ttl_until": timeValue(st.probeTokUntil), "abandons": st.probeAbandons},
		"absent": st.absent, "acct_skew": nil, // samples do not carry account identity; unknown, not a fabricated balance
		"ring_ok": okN, "ring_deg": degN, "last_cause": lastCause,
	}
}
func (h *healthStore) snapshotLocked(gap *string) map[string]any {
	ids := make([]string, 0, len(h.keys))
	for k := range h.keys {
		ids = append(ids, k)
	}
	sort.Strings(ids)
	keys := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, keyHealthLocked(h, h.keys[id]))
	}
	return map[string]any{"ts": nowFn().UTC().Format(time.RFC3339Nano), "gap_since": gap, "keys": keys}
}
func (tn *tenant) publishHealthSnapshot(gap *string) {
	h := tn.health
	if h == nil || healthModeV == healthOff || !eventsArmed() {
		return
	}
	h.mu.Lock()
	data := h.snapshotLocked(gap)
	h.mu.Unlock()
	publish(tn.id, "member_health_snapshot", data)
}
func (h *healthStore) publishTransitionLocked(st *epState, from, to string) {
	if !eventsArmed() || healthModeV == healthOff {
		return
	}
	cause := "state_changed"
	switch to {
	case hsOpen:
		cause = "probe_failed"
		for i := len(st.ring) - 1; i >= 0; i-- {
			if !st.ring[i].ok && st.ring[i].cause != "" {
				cause = st.ring[i].cause
				break
			}
		}
	case hsHalfOpen:
		cause = "hold_elapsed"
	case hsConvalescent:
		cause = "probe_ok"
	case hsHealthy:
		cause = "convalescent_elapsed"
	}
	publish(h.tenantID, "member_health", map[string]any{"key": st.key, "from": from, "to": to, "ep": nullable(st.ep), "k": st.k, "cause": cause,
		"evidence": evidenceLocked(h, st, "", false), "hold_until": timeValue(st.holdUntil), "next_transition": nextTransitionLocked(h, st)})
}
func (h *healthStore) noteBinding(class, member string, pinned bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.bindingHealth == nil {
		h.bindingHealth = map[string]bindingHealth{}
	}
	if st := h.keys[member]; st != nil && st.state == hsOpen && !pinned {
		b := h.bindingHealth[class]
		if b.key != member || b.episode != st.ep {
			b = bindingHealth{key: member, episode: st.ep}
		}
		b.consecutive++
		h.bindingHealth[class] = b
	} else {
		delete(h.bindingHealth, class)
	}
}
func (h *healthStore) fightingLocked(class string) bool {
	if h.pins[class].member != "" {
		return false
	}
	b := h.bindingHealth[class]
	st := h.keys[b.key]
	return b.consecutive >= 2 && st != nil && st.state == hsOpen && st.ep == b.episode
}
func (h *healthStore) noteAuthFailure(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.authFailures == nil {
		h.authFailures = map[string]int64{}
	}
	h.authFailures[key]++
}

func (tn *tenant) healthStatus() map[string]any {
	out := map[string]any{"mode": healthModeV.String()}
	if tn.healthRecovery != nil {
		out["background_recovery"] = tn.healthRecovery.snapshot()
	}
	h := tn.health
	if h == nil {
		return out
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	snapshot := h.snapshotLocked(nil)
	out["ts"], out["gap_since"] = snapshot["ts"], snapshot["gap_since"]
	keys := map[string]any{}
	for id, st := range h.keys {
		keys[id] = keyHealthLocked(h, st)
	}
	out["keys"] = keys
	classes := map[string]any{}
	ids := make([]string, 0, len(h.keys))
	for id := range h.keys {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		st := h.keys[id]
		for class, ce := range st.perClass {
			c, ok := classes[class].(map[string]any)
			if !ok {
				c = map[string]any{"prefills_paid": 0, "elections": []map[string]any{}, "ramp": map[string]any{"pending": 0, "next_return_at": (*string)(nil)}}
				classes[class] = c
			}
			entries := append(c["elections"].([]map[string]any), map[string]any{"key": id, "member": ce.member, "episode": ce.ep})
			c["elections"] = entries
			if len(entries) == 1 {
				c["election"], c["episode"] = ce.member, ce.ep
			} else {
				delete(c, "election")
				delete(c, "episode")
			}
			c["prefills_paid"] = c["prefills_paid"].(int) + ce.prefillsPaid
			ramp := c["ramp"].(map[string]any)
			for _, sr := range ce.sessions {
				if sr.dwellUntil.After(nowFn()) {
					ramp["pending"] = ramp["pending"].(int) + 1
					next := timeValue(sr.dwellUntil)
					prior := ramp["next_return_at"].(*string)
					priorTime := time.Time{}
					if prior != nil {
						priorTime, _ = time.Parse(time.RFC3339Nano, *prior)
					}
					if prior == nil || sr.dwellUntil.Before(priorTime) {
						ramp["next_return_at"] = next
					}
				}
			}
		}
	}
	for class := range h.pins {
		if classes[class] == nil {
			classes[class] = map[string]any{}
		}
	}
	for class := range h.bindingHealth {
		if classes[class] == nil {
			classes[class] = map[string]any{}
		}
	}
	for class, v := range classes {
		c := v.(map[string]any)
		p := h.pins[class]
		c["pin_source"], c["pinned_member"] = p.source, nullable(p.member)
		c["pointer_fighting_health"] = h.fightingLocked(class)
		c["health_coverage"] = map[string]string{"class": "full", "lane": "none", "bypass": "none"}
	}
	out["classes"] = classes
	wa := map[string]int64{}
	var would int64
	for c, n := range h.wouldAct {
		wa[c] = n
		would += n
	}
	out["would_act_total"] = wa
	_, stripped := noteStripTotals()
	var strippedN, emittedN int64
	for _, n := range stripped {
		strippedN += n
	}
	for _, n := range noteEmissionTotals() {
		emittedN += n
	}
	mode := "off"
	if healthNotesEnabled && healthModeV == healthEnforce {
		mode = "on"
	}
	out["notes"] = map[string]any{"mode": mode, "emitted_total": emittedN, "stripped_total": strippedN, "would_act_total": would}
	out["config"] = map[string]any{
		"trip_rate": h.cfg.tripRate, "trip_min": h.cfg.tripMin, "trip_consec": h.cfg.tripConsec, "trip_window_s": h.cfg.tripWindow.Seconds(),
		"hold_base_s": h.cfg.holdBase.Seconds(), "hold_max_s": h.cfg.holdMax.Seconds(), "convalescent_s": h.cfg.convalescent.Seconds(),
		"probe_ok_n": h.cfg.probeOKN, "probe_max_in": h.cfg.probeMaxIn, "probe_ttl_s": h.cfg.probeTTL.Seconds(),
		"return_dwell_s": h.cfg.returnDwell.Seconds(), "ramp_period_s": h.cfg.rampPeriod.Seconds(),
		"k_max": h.cfg.kMax, "ring_n": h.cfg.ringN, "bucket_s": h.cfg.bucketS.Seconds(), "abandon_notice": h.cfg.abandonNotice,
		"enforced_classes": healthEnforceClasses(),
		"all_degraded_529": boolF(healthAllDegradedEnabled), // numeric for existing Scry config readers
	}
	return out
}
