package main

// ---- alert history as state (the storm-damping fix) ----
//
// The storm: every account exhausted on the opus lane, so the proxy
// flip-flopped every ~5 min (429-cooldown expiry) and every flip-half qualified
// as "last hop" — hundreds of DND-bypassing critical pushes. Root cause:
// last-hop is an EVENT tier, but exhaustion is a STATE. Rather than an
// exhaustion state machine, we keep a bounded in-memory history of the alerts we
// actually dispatched and gate dispatch on it — the log of what we sent IS the
// state, so it can never drift from what the operator experienced.
//
// Gate 2 (repeat window, runs first): an identical (alertname, lane, from→to)
// alert within alertRepeatWindow is suppressed entirely — TIER-AWARE on the
// CANDIDATE tier, comparing intents rather than outcomes: a record suppresses
// only candidates at-or-below the tier it *entered* the funnel with. The
// distinction is load-bearing in both directions. A genuinely quiet flip
// (candTier=telegram) that landed just before the destination crossed the
// last-hop threshold can never mute the storm-onset critical — the one page the
// tier system exists to deliver. But a record that entered critical and left
// demoted (candTier=critical, tier=telegram) DOES suppress its pair's critical
// repeats: comparing final tiers here would let an all-critical-candidate storm
// re-send a demoted telegram on every flip forever (~12/h in the storm's
// shape — adversarial-review finding), because no demoted record could ever
// outrank the next critical candidate.
//
// Gate 1 (critical cooldown, runs second): a loud candidate for a lane that
// already paged critical within alertCritCooldown is DEMOTED to telegram.
// Lane-scoped on purpose: the incident flip-flop alternates pairs on ONE lane,
// so pair-scoping would page every flip. History stores the FINAL
// (post-demotion) tier, so a demoted telegram never re-arms the cooldown — a
// persistent episode re-pages once per alertCritCooldown (the reminder).
//
// Both gates live in gateAndEnqueue, the single funnel every alert path
// (fireSwitchAlert, fireClassAlert) goes through. Records are appended under
// alertMu (main.go's coalescing leaf lock — never held across I/O); on flush
// failure the batch's records are pruned so the next flip retries. publish()
// and the SWITCHOVER log lines fire ABOVE the gates (see fireSwitchAlert), so
// suppression silences only the bridge dispatch — flips stay visible in logs
// and on the events hub.
//
// OBSERVABILITY (notification-observability design, Phase 1): every candidate
// through the funnel — suppressed ones included — yields an `alert_dispatch`
// event, and every flushed batch yields one `alert_delivery` event. The funnel
// BUILDS the dispatch payload (it alone knows the gate outcome) but never
// publishes: publish() takes hub.mu and alertMu is a leaf lock. Who publishes
// is an ordering contract, not a convenience: the flush timer arms in here
// under alertMu, so a caller-side publish of an ENQUEUED dispatch could lose
// the race to its own delivery frame. Enqueued dispatches therefore ride the
// queue (queuedAlert.dispatch) and flushSwitchAlerts publishes them pre-POST —
// one goroutine, dispatch strictly before delivery on the wire. Suppressed
// candidates never reach the flush, so their caller publishes immediately
// (gateOutcome.dispatch); nothing orders against them.

import (
	"fmt"
	"slices"
	"strconv"
	"sync/atomic"
	"time"
)

// alertRecord is one alert CANDIDATE. Admitted records are appended to
// alertHistory at enqueue time (before the coalesced POST) and are immutable
// after append — prune is by pointer equality. Suppressed candidates get a
// record too (it feeds their dispatch event) but are never appended.
type alertRecord struct {
	// id is the dispatch id ("d-<evEpoch>-<n>") minted in the funnel — the
	// persistent primary key downstream (scry alertDispatches). The "d-"
	// prefix keeps it out of the hub's SSE frame-id namespace
	// (evID = "<evEpoch>-<seq>", events.go).
	id        string
	alertname string
	lane      string // switch lane ("opus", "fable", …) or "class:"+class for class alerts
	from, to  string // account emails on the switch path, member ids on the class path
	tier      string // FINAL post-demotion delivery tier ("critical"/"telegram")
	// candTier is the tier the candidate ENTERED the funnel with, before any
	// gate-1 demotion. Gate 2 compares candidate tiers (see the header): tier
	// answers "what did the operator receive", candTier answers "what was this
	// alert trying to be" — and repeat suppression is about intent.
	candTier string
	at       time.Time
}

// queuedAlert links a coalesced queue entry to its history record (so a failed
// flush can prune exactly its own batch) and carries the pre-built dispatch
// event, which flushSwitchAlerts publishes before the POST (see the header's
// ordering contract).
type queuedAlert struct {
	entry    map[string]any
	rec      *alertRecord
	dispatch map[string]any
}

// gateOutcome is the funnel's verdict on one candidate, plus the dispatch
// event it built. When enqueued, the dispatch also rides the queue and the
// FLUSH publishes it; when suppressed, the CALLER publishes gateOutcome.dispatch
// (outside alertMu — publish takes hub.mu).
type gateOutcome struct {
	enqueued bool
	demoted  bool
	// gate names which gate acted ("repeat_window" suppression,
	// "crit_cooldown" demotion; "" = passed clean). gateDetail is the
	// machine-authored human phrase for the feed.
	gate       string
	gateDetail string
	// suppressor is the history record that armed gate 2 — its id lets the
	// feed jump from a suppressed candidate to the alert that muted it.
	suppressor *alertRecord
	dispatch   map[string]any
}

// alertDispatchSeq mints dispatch ids. Its own counter, not evSeq: event seq
// numbers every hub frame, dispatch ids number only alert candidates, and the
// two must be free to move independently. Prefixing with evEpoch makes ids
// unique across restarts (same trick, same reasoning as evID).
var alertDispatchSeq int64

var (
	// Guarded by alertMu (main.go) — a leaf lock, never held across I/O.
	alertHistory []*alertRecord
	// Vars (not consts) so tests can shrink them — the same seam pattern as
	// alertCoalesce (main.go).
	alertCritCooldown = 6 * time.Hour // SWITCH_ALERT_CRIT_COOLDOWN
	alertRepeatWindow = time.Hour     // SWITCH_ALERT_REPEAT_WINDOW
)

// tierRank orders delivery tiers for gate 2's louder-than check: critical
// outranks telegram, everything else is telegram-grade.
func tierRank(tier string) int {
	if tier == "critical" {
		return 1
	}
	return 0
}

// buildDispatch renders one candidate's alert_dispatch payload. Called under
// alertMu (map building only — no I/O, no other locks). tierInputs carries the
// switch path's tiering evidence (to_pct/known/loud_pct/always_loud); nil on
// the class path, whose tier is constitutionally telegram.
func buildDispatch(rec *alertRecord, reason, outcome string, out *gateOutcome, tierInputs map[string]any) map[string]any {
	ev := map[string]any{
		"id":        rec.id,
		"at":        rec.at.UnixMilli(),
		"alertname": rec.alertname,
		"lane":      rec.lane,
		"from":      rec.from,
		"to":        rec.to,
		"reason":    reason,
		"cand_tier": rec.candTier,
		"outcome":   outcome, // "passed" | "demoted" | "suppressed"
	}
	if out.gate != "" {
		ev["gate"] = out.gate
		ev["gate_detail"] = out.gateDetail
	}
	if outcome == "suppressed" {
		// No final tier — nothing was dispatched. The suppressor id lets the
		// feed jump to the alert that muted this one.
		if out.suppressor != nil {
			ev["suppressor_id"] = out.suppressor.id
		}
	} else {
		ev["final_tier"] = rec.tier
	}
	for k, v := range tierInputs {
		ev[k] = v
	}
	return ev
}

// gateAndEnqueue evaluates both history gates and, if admitted, appends rec to
// alertHistory and {entry, rec, dispatch} to alertQueue, arming the flush
// timer. It also mints rec.id and builds the candidate's alert_dispatch event —
// see the header for who publishes it and why.
//
// demoteIfCritRecent, when non-nil and the candidate tier is critical, is called
// with how long ago the lane last paged critical if that was within
// alertCritCooldown. It runs UNDER alertMu: map writes on entry only — no I/O,
// no logging, no other locks.
func gateAndEnqueue(rec *alertRecord, entry map[string]any, reason string, tierInputs map[string]any, demoteIfCritRecent func(ago time.Duration)) gateOutcome {
	alertMu.Lock()
	defer alertMu.Unlock()
	now := time.Now()
	rec.at = now
	rec.id = "d-" + evEpoch + "-" + strconv.FormatInt(atomic.AddInt64(&alertDispatchSeq, 1), 10)
	// Age-prune: nothing older than the longest window can ever match a gate
	// again, so the history stays bounded.
	cutoff := now.Add(-max(alertCritCooldown, alertRepeatWindow))
	alertHistory = slices.DeleteFunc(alertHistory, func(h *alertRecord) bool { return h.at.Before(cutoff) })

	// Gate 2 — repeat window, compared on CANDIDATE tiers (see the header): a
	// record suppresses only candidates at-or-below the tier it entered with,
	// so a quiet flip can never mute the storm-onset critical, while a
	// demoted-but-critical-intent record still suppresses its pair's critical
	// repeats. Suppressed fires are NOT recorded — the window slides from the
	// last ADMITTED fire, so a fast flap yields ~1 alert per direction per
	// window instead of starving itself forever.
	rec.candTier = rec.tier // rec.tier at entry IS the candidate tier
	candTier := tierRank(rec.candTier)
	for _, h := range alertHistory {
		if h.alertname == rec.alertname && h.lane == rec.lane && h.from == rec.from && h.to == rec.to &&
			now.Sub(h.at) < alertRepeatWindow && tierRank(h.candTier) >= candTier {
			out := gateOutcome{
				gate: "repeat_window",
				gateDetail: fmt.Sprintf("identical %s-intent alert %s ago (window %s)",
					h.candTier, now.Sub(h.at).Round(time.Second), alertRepeatWindow),
				suppressor: h,
			}
			out.dispatch = buildDispatch(rec, reason, "suppressed", &out, tierInputs)
			return out
		}
	}

	// Gate 1 — critical cooldown, lane-scoped. Matches only tier=="critical"
	// records, so a demoted telegram never extends the cooldown: the reminder
	// critical fires one alertCritCooldown after the last REAL critical.
	out := gateOutcome{enqueued: true}
	if rec.tier == "critical" && demoteIfCritRecent != nil {
		var lastCrit *alertRecord
		for _, h := range alertHistory {
			if h.lane == rec.lane && h.tier == "critical" && now.Sub(h.at) < alertCritCooldown {
				if lastCrit == nil || h.at.After(lastCrit.at) {
					lastCrit = h
				}
			}
		}
		if lastCrit != nil {
			ago := now.Sub(lastCrit.at)
			demoteIfCritRecent(ago)
			out.demoted = true
			out.gate = "crit_cooldown"
			out.gateDetail = fmt.Sprintf("lane paged critical %s ago (cooldown %s)",
				ago.Round(time.Second), alertCritCooldown)
		}
	}

	// Record the FINAL tier (post-demotion), read back from the entry the
	// demote closure may have just mutated — history must equal what was
	// actually dispatched.
	if labels, ok := entry["labels"].(map[string]string); ok {
		rec.tier = labels["delivery"]
	}
	outcome := "passed"
	if out.demoted {
		outcome = "demoted"
	}
	out.dispatch = buildDispatch(rec, reason, outcome, &out, tierInputs)
	alertHistory = append(alertHistory, rec)
	alertQueue = append(alertQueue, queuedAlert{entry: entry, rec: rec, dispatch: out.dispatch})
	if alertTimer == nil {
		alertTimer = time.AfterFunc(alertCoalesce, flushSwitchAlerts)
	}
	return out
}

// pruneAlertRecords splices the given records out of alertHistory — used when a
// flush fails, so alerts the bridge never saw cannot gate the next flip
// ("at most one lost critical, then self-heal").
func pruneAlertRecords(recs ...*alertRecord) {
	alertMu.Lock()
	defer alertMu.Unlock()
	alertHistory = slices.DeleteFunc(alertHistory, func(h *alertRecord) bool {
		return slices.Contains(recs, h)
	})
}

// swapAlertHistory replaces alertHistory and returns the old value — the test
// world-swap, mirroring swapBridgeURL (main.go).
func swapAlertHistory(h []*alertRecord) []*alertRecord {
	alertMu.Lock()
	defer alertMu.Unlock()
	old := alertHistory
	alertHistory = h
	return old
}

// historySnapshot returns a copy of alertHistory for test assertions — reads
// must hold alertMu because flushSwitchAlerts prunes history from a timer
// goroutine on the failure path.
func historySnapshot() []*alertRecord {
	alertMu.Lock()
	defer alertMu.Unlock()
	return slices.Clone(alertHistory)
}
