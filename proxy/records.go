package main

// ---- one record store, and the overlay (quota-model 50 §2.6, §2.7, C2, C7) ----
//
// One store keys a refusal by what refused (refusal.go's taxonomy): (account,
// limit) when the refusal names a limit, (account, model) when it names none,
// so a refusal nobody can read never cools other models. A spent record closes
// its window, count or pool; an unpaid one its money pot; a not_entitled one
// the account's plan or one model. Every record feeds the dial check
// (routeCheck) through the account's standing, so every dial site sees it
// through the one check.
//
// When a record ends depends on whether a reading can see the limit that
// refused (§2.6, C7):
//
//   - attributed (a window the account's reading carries, the codex credits
//     gate, Kimi's membership): at the reset the refusal named, else at the
//     limit's reset in the first reading newer than the refusal. It also ends
//     at the first newer reading of that limit that shows it open AND
//     changed: a different reset, a lower use, a higher balance. A reading of
//     the same period at the same or higher use, or at most a point below
//     (useAgrees, windowChanged), does not end it: the
//     provider's refusal outranks our reading of that period, which is what
//     stops a re-dial every poll. No floor and no cap.
//   - unattributed (an unmapped window, a pool no reading maps, a pot with no
//     balance reading, a model): today's clamp, a named reset held to
//     [quotaCooldown, quotaCooldownCap]; with none, quotaCooldown for spent
//     and unpaid and quotaCooldownCap for not_entitled.
//
// Either ends at the owner's re-check (the account's recheck_epoch in
// /preferred), and when the account serves a request the record would bind.
// A throttle lasts its Retry-After or forcedCooldown.
//
// The overlay is the other half of §2.7: the per-response partial readings an
// upstream sends on every reply (codex x-codex-*, Anthropic unified-*). Per
// field the later as_of wins over the brain's poll, so a header can close a
// window, keep it fresh, or show a new period at once, for every route on that
// account. The brain hears of the change as a report (reports.go), and sees it
// at its next poll whatever the report's fate.

import (
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// recKey is what refused: an account and one of its limits.
type recKey struct{ account, limit string }

const (
	recSpent       = refSpent       // the limit is closed for this account: the check reads `no`
	recUnpaid      = refUnpaid      // the money pot can't pay
	recNotEntitled = refNotEntitled // the plan, or one model, is not included
	recThrottled   = refThrottled   // rate, burst or concurrency: deranks, never `no` (§2.3)

	limitThrottle = "throttle"

	// potCredits is a subscription account's one money pot, its credits
	// (pot id "<account>/credits").
	potCredits = "credits"

	// recResetSlack is how far a newer reading's reset may sit from the one
	// seen at the refusal and still be the same period: the poll and a
	// response's headers state one reset to different precision.
	recResetSlack = 10 * time.Minute
)

// modelLimit keys a refusal that names no limit (§2.6 "unattributed").
func modelLimit(model string) string { return "model:" + model }

// limitSeen is the reading of a limit when it refused, for "open and
// changed" (§2.6): a window's use and reset, a pot's gate and balance.
type limitSeen struct {
	usedPct, resetsAt *float64
	open              *bool
	balance           *float64
	used              *float64 // a rolling count's
}

// refusalRecord is one refusal and when it stops counting.
type refusalRecord struct {
	kind      string
	limKind   string // lim* (refusal.go): which reading can end it
	refusedAt time.Time
	// until is when the record ends by time. Zero on an attributed record
	// that named no reset: a reading sets it, or ends it (pruneRecords).
	until      time.Time
	resets     time.Time // the reset the refusal named, availabilityClamp-bounded; zero when none
	attributed bool      // a reading carries the limit: it ends by reading, unclamped
	seen       limitSeen
	status     int
	detail     string
}

func (r refusalRecord) live(now time.Time) bool { return r.until.IsZero() || now.Before(r.until) }

// closes: the record makes the check read `no` on what it names. A throttle
// only deranks.
func (r refusalRecord) closes() bool { return r.kind != recThrottled }

// Per tenant: tenant.recMu, tenant.records, and tenant.readings (each
// account's latest per-response partial reading).

// clampedUntil is an unattributed record's end: the upstream's own reset
// inside [now+floor, now+quotaCooldownCap], so a missing, stale or absurd
// reset can neither shorten it below the floor nor park the limit past the
// cap. floor is quotaCooldown, or quotaCooldownCap for not_entitled (§2.6).
func clampedUntil(kind string, resets, now time.Time) time.Time {
	floor := quotaCooldown
	if kind == recNotEntitled {
		floor = quotaCooldownCap
	}
	until := now.Add(floor)
	if resets.After(until) {
		until = resets
	}
	if cap := now.Add(quotaCooldownCap); until.After(cap) {
		until = cap
	}
	return until
}

// writeRecord stores one record. An attributed record ends at the reset the
// refusal named when it is still ahead, else by reading (until zero); an
// unattributed one at its clamp.
func (tn *tenant) writeRecord(account, limit string, rec refusalRecord) refusalRecord {
	rec.resets = clampReset(rec.resets, rec.refusedAt)
	switch {
	case !rec.attributed:
		rec.until = clampedUntil(rec.kind, rec.resets, rec.refusedAt)
	case rec.resets.After(rec.refusedAt):
		rec.until = rec.resets
	}
	if account == "" || limit == "" {
		return rec
	}
	tn.recMu.Lock()
	tn.records[recKey{account, limit}] = rec
	tn.recMu.Unlock()
	return rec
}

// recordThrottle writes a throttle: Retry-After when the upstream sent one,
// else forcedCooldown (SWITCH_429_COOLDOWN).
func (tn *tenant) recordThrottle(account string, status int, detail string, h http.Header) refusalRecord {
	now := nowFn()
	until := now.Add(forcedCooldown)
	if h != nil {
		if s, err := strconv.Atoi(strings.TrimSpace(h.Get("Retry-After"))); err == nil && s > 0 {
			until = now.Add(time.Duration(s) * time.Second)
		}
	}
	if cap := now.Add(quotaCooldownCap); until.After(cap) {
		until = cap
	}
	rec := refusalRecord{kind: recThrottled, refusedAt: now, until: until, status: status, detail: detail}
	if account == "" {
		return rec
	}
	tn.recMu.Lock()
	tn.records[recKey{account, limitThrottle}] = rec
	tn.recMu.Unlock()
	return rec
}

// recordRefusal writes what one classified refusal records under the account
// that refused, after keeping the reading its headers carry. A window is
// attributed when the account's reading carries it, a pot or the
// entitlement when the adapter says a reading shows it; the record keeps
// what that reading said, for "open and changed". It returns the kind
// recorded and the limits, for the caller's log line ("" when nothing is);
// /_switch/status shows how each ends.
func (tn *tenant) recordRefusal(account string, ref refusal, resp *http.Response) (kind, what string) {
	if account == "" || !ref.records() {
		return "", ""
	}
	now := nowFn()
	status := 0
	var h http.Header
	if resp != nil {
		status, h = resp.StatusCode, resp.Header
	}
	tn.noteHeaderReading(account, h, now)
	if ref.class == refThrottled {
		r := tn.recordThrottle(account, status, ref.detail, h)
		return recThrottled, "throttle until " + r.until.UTC().Format(time.RFC3339)
	}
	st := tn.standingRead(account, now)
	var parts []string
	for _, l := range ref.limits {
		rec := refusalRecord{kind: ref.class, limKind: l.kind, refusedAt: now, resets: l.resets,
			status: status, detail: ref.detail}
		if st != nil && st.AsOf != nil {
			switch l.kind {
			case limWindow:
				if w, ok := st.Allowance.Windows[l.name]; ok {
					rec.attributed = true
					rec.seen = limitSeen{usedPct: w.UsedPct, resetsAt: w.ResetsAt}
				}
			case limPot:
				if p, ok := st.Money[l.name]; ok && l.readable {
					rec.attributed = true
					rec.seen = limitSeen{open: p.Open, balance: p.Balance}
				}
			case limEntitlement:
				rec.attributed = l.readable
			case limCount:
				if lim, ok := potLimitOf(st, l.name); ok {
					rec.attributed = true
					rec.seen = limitSeen{used: lim.Used}
				}
			}
		}
		rec = tn.writeRecord(account, l.name, rec)
		tn.reportRecord(account, l.name, rec, false, time.Time{}) // a spent window's, a model's or a pool's goes to the brain (reports.go)
		if pot := tn.apiMoneyPot(account); ref.class == refUnpaid && l.kind == limPot && pot == l.name {
			tn.reportPotShut(account, pot, now) // an api account's pot, through the class walk: the prepaid notices
		}
		parts = append(parts, l.name)
	}
	return ref.class, strings.Join(parts, ",")
}

// liveRecordsLocked is every live record on account, by limit. Caller holds recMu.
func (tn *tenant) liveRecordsLocked(account string, now time.Time) map[string]refusalRecord {
	var out map[string]refusalRecord
	for k, r := range tn.records {
		if k.account != account || !r.live(now) {
			continue
		}
		if out == nil {
			out = map[string]refusalRecord{}
		}
		out[k.limit] = r
	}
	return out
}

// throttledUntil is the end of a live throttle on account (zero when none).
func (tn *tenant) throttledUntil(account string, now time.Time) time.Time {
	tn.recMu.Lock()
	defer tn.recMu.Unlock()
	if r, ok := tn.records[recKey{account, limitThrottle}]; ok && r.live(now) {
		return r.until
	}
	return time.Time{}
}

// spentRecordOf is account's live closing record that ends last (one that
// ends only by reading outlasts any time): the refusal that keeps it out
// longest, for a message that quotes the upstream.
func (tn *tenant) spentRecordOf(account string, now time.Time) (refusalRecord, bool) {
	tn.recMu.Lock()
	defer tn.recMu.Unlock()
	var best refusalRecord
	found := false
	later := func(a, b refusalRecord) bool {
		if a.until.IsZero() || b.until.IsZero() {
			return a.until.IsZero() && !b.until.IsZero()
		}
		return a.until.After(b.until)
	}
	for k, r := range tn.records {
		if k.account == account && r.closes() && r.live(now) && (!found || later(r, best)) {
			best, found = r, true
		}
	}
	return best, found
}

// noteServed ends what a 2xx on the allowance disproves: the throttle on the
// account, every unscoped window, the scoped window the served route binds,
// the plan's entitlement, and the record on the served model. at is when the
// served request was sent: a record written at or after it stands, since the
// upstream may have answered that refusal after the success. Another scope's
// record stays in force.
func (tn *tenant) noteServed(account, scope, model string, at time.Time) {
	ends := map[string]bool{limitThrottle: true, "five_hour": true, "seven_day": true, limitEntitlement: true}
	if scope != "" {
		ends["scoped:"+scope] = true
	}
	if model != "" {
		ends[modelLimit(model)] = true
	}
	tn.endRecords(account, ends, at, "served")
}

// notePaidServed ends what a 2xx a money pot paid for disproves: that pot's
// unpaid record, and the throttle.
func (tn *tenant) notePaidServed(account, pot string, at time.Time) {
	tn.endRecords(account, map[string]bool{pot: true, limitThrottle: true}, at, "served on "+pot)
}

// endRecords ends the records on account that ends names, written before at.
// The end of a record the brain takes (a spent window's, a model's or a
// pool's, reportedAs) goes to it as its refusal with ended, at at.
func (tn *tenant) endRecords(account string, ends map[string]bool, at time.Time, why string) {
	if account == "" {
		return
	}
	var ended []string
	gone := map[string]refusalRecord{}
	tn.recMu.Lock()
	for k, r := range tn.records {
		if k.account == account && ends[k.limit] && r.refusedAt.Before(at) {
			delete(tn.records, k)
			if r.closes() {
				ended = append(ended, k.limit)
			}
			gone[k.limit] = r
		}
	}
	tn.recMu.Unlock()
	for limit, r := range gone {
		tn.reportRecord(account, limit, r, true, at)
	}
	if len(ended) > 0 {
		sort.Strings(ended)
		log.Printf("🔋 %s %s — record ended on %s", account, why, strings.Join(ended, ","))
	}
}

// pruneRecords runs after every brain document (accounts is the merged
// standing), and reports nothing to the brain, which judges its own reading
// by the same rule: expired records go; every record on an account older than its
// recheck_epoch goes (the owner's re-check); and an attributed record ends,
// or learns its end, from the first reading of its limit newer than the
// refusal (recordEnds).
func (tn *tenant) pruneRecords(accounts map[string]acctStanding) {
	now := nowFn()
	var cleared []string
	tn.recMu.Lock()
	for k, r := range tn.records {
		if !r.live(now) {
			delete(tn.records, k)
			continue
		}
		st, ok := accounts[k.account]
		if !ok {
			continue
		}
		if e := st.RecheckEpoch; e != nil && unixSeconds(r.refusedAt) < *e {
			delete(tn.records, k)
			cleared = append(cleared, k.account+"/"+k.limit+" (re-check)")
			continue
		}
		if !r.attributed {
			continue
		}
		end, until := recordEnds(r, k.limit, &st, now)
		if end {
			delete(tn.records, k)
			cleared = append(cleared, k.account+"/"+k.limit)
			continue
		}
		if !until.IsZero() {
			r.until = until
		}
		r.seen = recordSeen(r, k.limit, &st)
		tn.records[k] = r
	}
	tn.recMu.Unlock()
	if len(cleared) > 0 {
		sort.Strings(cleared)
		log.Printf("🔋 a reading ends the record on %s", strings.Join(cleared, ", "))
	}
}

// recordEnds judges an attributed record against the brain's standing (§2.6):
// end when a reading of its limit newer than the refusal shows it open and
// changed; else, for a record that named no reset, until is that reading's
// reset when it is still ahead (a null or passed reset never shortens it).
func recordEnds(r refusalRecord, limit string, st *acctStanding, now time.Time) (end bool, until time.Time) {
	nowS, refused := unixSeconds(now), unixSeconds(r.refusedAt)
	newer := func(t *float64) bool { return t != nil && *t > refused }
	switch r.limKind {
	case limWindow:
		w, ok := st.Allowance.Windows[limit]
		if !ok || !newer(w.AsOf) {
			return false, time.Time{}
		}
		gateShut := st.Allowance.Open != nil && !*st.Allowance.Open
		if !windowClosed(w, nowS) && !gateShut && windowChanged(r.seen, w, nowS) {
			return true, time.Time{}
		}
		if r.until.IsZero() && w.ResetsAt != nil && *w.ResetsAt > nowS {
			return false, time.Unix(0, int64(*w.ResetsAt*1e9))
		}
	case limPot:
		p, ok := st.Money[limit]
		if !ok || !newer(st.AsOf) || p.Open == nil || !*p.Open {
			return false, time.Time{}
		}
		// Changed: the pot read shut at the refusal and open now, or a higher
		// balance than it read. A pot whose open was unknown at the refusal
		// is judged by its balance when one was read (the same balance, open,
		// is no change); with neither read, the first newer open reading is
		// the only evidence there is.
		switch {
		case r.seen.open != nil && !*r.seen.open:
			return true, time.Time{}
		case r.seen.balance != nil:
			return p.Balance != nil && *p.Balance > *r.seen.balance, time.Time{}
		}
		return r.seen.open == nil, time.Time{}
	case limEntitlement:
		e := st.Allowance.Entitlement
		return newer(st.AsOf) && (e == nil || e.State == "active"), time.Time{}
	case limCount:
		// A rolling count names no reset: it ends at a newer reading under
		// its limit and lower than the highest read since the refusal (or
		// than none read then).
		lim, ok := potLimitOf(st, limit)
		if !ok || !newer(st.AsOf) || lim.Used == nil || lim.Limit == nil || *lim.Used >= *lim.Limit {
			return false, time.Time{}
		}
		return r.seen.used == nil || *lim.Used < *r.seen.used, time.Time{}
	}
	return false, time.Time{}
}

// recordSeen is the reading an attributed pot or count record is judged
// against from now on: the one at the refusal, moved by each newer reading
// that did not end it to the shut pot, the lowest balance or the highest
// count. A reading taken before the refusal can be far from what refused (a
// balance read at 500 that our own tasks spent to 0): judged against it, a
// top-up to 100 would never be "a higher balance", nor a count rolling from
// 200 to 150 "a lower use" than the 3 read before it. Moving the reading only
// toward the refusal keeps "the same or a worse reading does not end it".
func recordSeen(r refusalRecord, limit string, st *acctStanding) limitSeen {
	seen := r.seen
	if st.AsOf == nil || *st.AsOf <= unixSeconds(r.refusedAt) {
		return seen
	}
	switch r.limKind {
	case limPot:
		p, ok := st.Money[limit]
		if !ok {
			break
		}
		if p.Open != nil && !*p.Open {
			f := false
			seen.open = &f
		}
		if p.Balance != nil && (seen.balance == nil || *p.Balance < *seen.balance) {
			b := *p.Balance
			seen.balance = &b
		}
	case limCount:
		if lim, ok := potLimitOf(st, limit); ok && lim.Used != nil && (seen.used == nil || *lim.Used > *seen.used) {
			u := *lim.Used
			seen.used = &u
		}
	}
	return seen
}

// potLimitOf is the pot limit of that id on the account (a rolling count),
// from whichever of its money pots carries it.
func potLimitOf(st *acctStanding, id string) (potLimit, bool) {
	for _, p := range st.Money {
		if l, ok := p.Limits[id]; ok {
			return l, true
		}
	}
	return potLimit{}, false
}

// windowChanged: the reading shows a different period (a reset still ahead,
// other than the one seen at the refusal past recResetSlack) or a lower use
// than the refusal's reading. The same period at the same or higher use is
// no change, and a null or passed reset is no new period (62 m6). A use at
// most a point below the one seen, in the same period (useAgrees), is the
// seen reading again and no change: the 99 a 2xx answered after a refusal at
// 100 reads (quota-model 10, "Rounding"), which a brain restarted during the
// hold publishes with no 100 held (64 §8).
func windowChanged(seen limitSeen, w checkWindow, now float64) bool {
	if w.ResetsAt != nil && *w.ResetsAt > now &&
		(seen.resetsAt == nil || math.Abs(*seen.resetsAt-*w.ResetsAt) > recResetSlack.Seconds()) {
		return true
	}
	return seen.usedPct != nil && w.UsedPct != nil && *w.UsedPct < *seen.usedPct &&
		!useAgrees(seen.usedPct, seen.resetsAt, w.UsedPct, w.ResetsAt)
}

// useAgrees: a response's reading of a window's use (used, reset) at most a
// point below the use held for it (heldUsed, heldReset), in the same period,
// is the held reading again. The usage poll and a response's headers state
// one counter rounded half a point apart (quota-model 10, "Rounding"), so for
// the upper half of every point a response reads a point below the poll taken
// before it; use does not fall within a period. The held use stands, and the
// reading says only that it still holds. A reading above it, more than a point
// below, or of another period (a reset past recResetSlack) is newer use.
func useAgrees(heldUsed, heldReset, used, reset *float64) bool {
	if heldUsed == nil || used == nil {
		return false
	}
	if heldReset != nil && reset != nil && math.Abs(*heldReset-*reset) > recResetSlack.Seconds() {
		return false
	}
	d := *heldUsed - *used
	return d > 0 && d <= 1
}

// ---- the overlay: per-response partial readings ----

// headerReading is what one response said about its account's limits.
type headerReading struct {
	asOf    time.Time
	windows map[string]checkWindow
	// creditsOff: the response says the account has no credits at all (codex
	// has-credits and unlimited both False). It can close the money pot; it
	// cannot open one (spend control and the top-up cap are not on it).
	creditsOff bool
	balance    *float64
	// credits: the response carried the credits set at all (has-credits,
	// unlimited or balance).
	credits bool
}

// parseHeaderReading is what a response's headers say of its account's
// limits, read at at: codex's x-codex-* set, else Anthropic's unified set by
// the published aliases. ok is false when they carry neither. The served hop
// parses once and hands the one result to keepHeaderReading and to the served
// event (noteServedHop): the event's readings are this parse's windows that
// carry a use (servedReadings), named as the reading reports name them, with
// the same use and reset.
func (tn *tenant) parseHeaderReading(h http.Header, at time.Time) (headerReading, bool) {
	if h == nil {
		return headerReading{}, false
	}
	hr, ok := parseCodexHeaders(h, at)
	if !ok {
		hr, ok = parseAnthropicHeaders(h, at, tn.currentScopes())
	}
	return hr, ok
}

// noteHeaderReading keeps account's reading from a response, when it carries
// one (parseHeaderReading, keepHeaderReading).
func (tn *tenant) noteHeaderReading(account string, h http.Header, at time.Time) {
	if account == "" || h == nil {
		return
	}
	if hr, ok := tn.parseHeaderReading(h, at); ok {
		tn.keepHeaderReading(account, hr)
	}
}

// keepHeaderReading keeps account's parsed reading from a response. A reading
// older than the one on file is dropped; one kept goes to the brain where it
// is news (reportHeaderReading).
func (tn *tenant) keepHeaderReading(account string, hr headerReading) {
	if account == "" {
		return
	}
	tn.recMu.Lock()
	kept := false
	if prev, had := tn.readings[account]; !had || !prev.asOf.After(hr.asOf) {
		tn.readings[account], kept = hr, true
	}
	tn.recMu.Unlock()
	if kept {
		tn.reportHeaderReading(account, hr)
	}
}

// codexWindowName labels a codex window by its length, as the brain does
// (_slot_id): Codex names a window by its minutes, not its slot. A length
// that fits a contract slot still free takes it; any other is its own
// window, window:<period> (window:<period>#n for a second of that length),
// with its period. A zero length is no window ("").
func codexWindowName(seconds float64, taken map[string]checkWindow) (name, period string) {
	if seconds <= 0 {
		return "", ""
	}
	for _, l := range []struct {
		s            float64
		name, period string
	}{{18000, "five_hour", "5h"}, {604800, "seven_day", "7d"}} {
		if _, had := taken[l.name]; !had && math.Abs(seconds-l.s) <= 180 {
			return l.name, l.period
		}
	}
	period = periodLabel(seconds)
	name = "window:" + period
	for n := 2; ; n++ {
		if _, had := taken[name]; !had {
			return name, period
		}
		name = "window:" + period + "#" + strconv.Itoa(n)
	}
}

// periodLabel names a length by its largest whole unit (86400 -> "1d", 3h ->
// "3h"), as the brain's _period_label does.
func periodLabel(seconds float64) string {
	secs := int64(seconds)
	for _, u := range []struct {
		n    int64
		unit string
	}{{86400, "d"}, {3600, "h"}, {60, "m"}} {
		if secs%u.n == 0 {
			return strconv.FormatInt(secs/u.n, 10) + u.unit
		}
	}
	return strconv.FormatInt(secs, 10) + "s"
}

// parseCodexHeaders reads x-codex-{primary,secondary}-* and the credits set
// (quota-model 11 S1, S4). A window of zero minutes is absent.
func parseCodexHeaders(h http.Header, at time.Time) (headerReading, bool) {
	hr := headerReading{asOf: at, windows: map[string]checkWindow{}}
	num := func(k string) (float64, bool) {
		return parseFiniteFloat(strings.TrimSpace(h.Get(k)))
	}
	found := false
	for _, slot := range []string{"primary", "secondary"} {
		pre := "x-codex-" + slot + "-"
		used, ok := num(pre + "used-percent")
		if !ok {
			continue
		}
		found = true
		mins, _ := num(pre + "window-minutes")
		name, period := codexWindowName(mins*60, hr.windows)
		if name == "" {
			continue
		}
		w := checkWindow{UsedPct: &used, Period: period}
		if r, ok := num(pre + "reset-at"); ok && r > 0 {
			w.ResetsAt = &r
		} else if s, ok := num(pre + "reset-after-seconds"); ok && s >= 0 {
			r := unixSeconds(at) + s
			w.ResetsAt = &r
		}
		hr.windows[name] = w
	}
	has := strings.TrimSpace(h.Get("x-codex-credits-has-credits"))
	unl := strings.TrimSpace(h.Get("x-codex-credits-unlimited"))
	if has != "" || unl != "" {
		found = true
		hr.creditsOff = strings.EqualFold(has, "false") && strings.EqualFold(unl, "false")
	}
	if b, ok := num("x-codex-credits-balance"); ok {
		hr.balance = &b
	}
	hr.credits = has != "" || unl != "" || hr.balance != nil
	return hr, found
}

// parseAnthropicHeaders reads the unified-* windows by meaning (C5's scope
// map). An unmapped window binds no route, so it is not a reading of one.
func parseAnthropicHeaders(h http.Header, at time.Time, sm scopeMap) (headerReading, bool) {
	obs, ok := parseUnifiedRL(h, at, sm)
	if !ok {
		return headerReading{}, false
	}
	hr := headerReading{asOf: at, windows: map[string]checkWindow{}}
	for _, w := range obs.windows {
		if strings.HasPrefix(w.name, "unmapped:") {
			continue
		}
		cw := windowFor(w.name)
		if w.pct >= 0 {
			p := w.pct
			cw.UsedPct = &p
		}
		if !w.reset.IsZero() {
			r := unixSeconds(w.reset)
			cw.ResetsAt = &r
		}
		hr.windows[w.name] = cw
	}
	return hr, len(hr.windows) > 0
}

// windowFor is an empty window of that name, its period and scope filled in.
// A scoped cap is a weekly window.
func windowFor(name string) checkWindow {
	switch name {
	case "five_hour":
		return checkWindow{Period: "5h"}
	case "seven_day":
		return checkWindow{Period: "7d"}
	}
	w := checkWindow{Period: "7d"}
	if s, ok := strings.CutPrefix(name, "scoped:"); ok {
		w.Scope = &s
	}
	return w
}

// ---- the merged standing and the check ----

// recordBasis is how firm a spent record's window reset is (§2.8): exact when
// the window resets at the upstream's own reset (the refusal named one at or
// after the record's end); record when only the record's clamp says.
func recordBasis(r refusalRecord) string {
	if !r.resets.IsZero() && !r.resets.Before(r.until) {
		return "exact"
	}
	return "record"
}

// standingFor is the account's standing as this proxy sees it now: the
// brain's reading with the newer per-response reading over it (standingRead),
// and the live records closing what they name. nil = an account the brain
// does not serve; one never measured is the brain's entry as it stands (as_of
// null), so its check waits for the brain's next poll, as the brain's does.
func (tn *tenant) standingFor(account string, now time.Time) *acctStanding {
	st := tn.standingRead(account, now)
	if st == nil || st.AsOf == nil {
		return st
	}
	tn.recMu.Lock()
	recs := tn.liveRecordsLocked(account, now)
	tn.recMu.Unlock()
	for limit, r := range recs {
		if !r.closes() || strings.HasPrefix(limit, "model:") {
			continue // a model's record binds one dial, not the account: routeCheck applies it
		}
		if r.kind == recUnpaid {
			p := st.Money[limit]
			f := false
			p.Open = &f
			st.Money[limit] = p
			continue
		}
		// A spent limit, or a plan not entitled, closes the allowance as a
		// window at its limit: the check then reads it as any spent limit.
		w, has := st.Allowance.Windows[limit]
		if !has {
			w = recordLimitWindow(limit)
		}
		st.Allowance.Windows[limit] = recordWindow(r, w, now)
	}
	return st
}

// recordWindow is the window a closing record makes of its limit: at 100,
// resetting when the record ends. A record that ends only by reading resets
// at the reading's own reset while that is ahead, and else at no known time:
// a passed reset never shortens it (§2.6).
func recordWindow(r refusalRecord, w checkWindow, now time.Time) checkWindow {
	full := 100.0
	w.UsedPct = &full
	refused := unixSeconds(r.refusedAt)
	if w.AsOf == nil || *w.AsOf < refused {
		w.AsOf = &refused // the refusal is a reading of the limit
	}
	if r.until.IsZero() {
		if w.ResetsAt != nil && *w.ResetsAt <= unixSeconds(now) {
			w.ResetsAt, w.ResetBasis = nil, "record"
		}
		return w
	}
	end, basis := r.until, recordBasis(r)
	if r.resets.After(end) {
		end = r.resets // the reply names the upstream's reset; the record still ends at the clamp
	}
	e := unixSeconds(end)
	if basis == "record" || w.ResetBasis == "" {
		w.ResetBasis = basis // an upstream reset keeps the window's own basis (codex: early_possible)
	}
	w.ResetsAt = &e
	return w
}

// recordLimitWindow is the window a record makes of a limit the reading does
// not carry: a named window as windowFor makes it, a pot's rolling count
// scoped to the model it counts (so it binds that model's route alone), and
// any other limit (a pool, the plan's entitlement) with no period, so no
// stop_at names it.
func recordLimitWindow(limit string) checkWindow {
	switch {
	case limit == "five_hour", limit == "seven_day", strings.HasPrefix(limit, "scoped:"):
		return windowFor(limit)
	case strings.HasPrefix(limit, "rolling_count:"):
		m := strings.TrimPrefix(limit, "rolling_count:")
		return checkWindow{Scope: &m}
	}
	return checkWindow{}
}

// standingRead is the account's standing before records: the brain's
// reading, and the newer per-response reading over it field by field. It is
// also what a refusal is recorded against (recordRefusal).
func (tn *tenant) standingRead(account string, now time.Time) *acctStanding {
	tn.pref.mu.RLock()
	b, ok := tn.pref.accounts[account]
	tn.pref.mu.RUnlock()
	if !ok {
		return nil
	}
	st := b
	st.Allowance.Windows = make(map[string]checkWindow, len(b.Allowance.Windows))
	for n, w := range b.Allowance.Windows {
		st.Allowance.Windows[n] = w
	}
	st.Money = make(map[string]moneyPot, len(b.Money))
	for n, p := range b.Money {
		st.Money[n] = p
	}
	if b.AsOf == nil {
		return &st
	}
	tn.recMu.Lock()
	hr, hasHR := tn.readings[account]
	tn.recMu.Unlock()
	if !hasHR {
		return &st
	}
	at := unixSeconds(hr.asOf)
	// Codex's own gate can close the allowance below 100 without saying
	// which window it means. As the brain does (_allowance_state), it
	// stands until every window the poll read with it has reset; a
	// response's windows under 100 do not lift it.
	if g := b.Allowance.Open; g != nil && !*g && len(b.Allowance.Windows) > 0 {
		reset := true
		for _, w := range b.Allowance.Windows {
			if w.ResetsAt == nil || *w.ResetsAt > unixSeconds(now) {
				reset = false
			}
		}
		if reset {
			st.Allowance.Open = nil
		}
	}
	for n, hw := range hr.windows {
		w, has := st.Allowance.Windows[n]
		if !has {
			if hw.UsedPct == nil {
				// A reading with no use never adds a window: one of null use
				// and no as_of reads as stale and would close every route on
				// the account until the next poll.
				continue
			}
			w = windowFor(n)
		}
		// Per limit the later reading wins (§1.4, §2.7), field by field:
		// what the response did not carry keeps the brain's value (a
		// status with no utilization is not "unknown"), and only a
		// response that carried the limit's use refreshes its age. A use
		// that agrees with the brain's (useAgrees) refreshes only its age.
		if w.AsOf != nil && *w.AsOf >= at {
			continue
		}
		if hw.UsedPct != nil {
			if !useAgrees(w.UsedPct, w.ResetsAt, hw.UsedPct, hw.ResetsAt) {
				w.UsedPct = hw.UsedPct
			}
			w.AsOf = &at
		}
		if hw.ResetsAt != nil {
			w.ResetsAt = hw.ResetsAt
		}
		st.Allowance.Windows[n] = w
	}
	if p, has := st.Money[potCredits]; has && at > *st.AsOf {
		if hr.creditsOff {
			f := false
			p.Open = &f
		}
		if hr.balance != nil {
			p.Balance = hr.balance
		}
		st.Money[potCredits] = p
	}
	return &st
}

// mergeStandings is §2.7's merge of a fresh /preferred accounts{} over the
// last one: per account the later as_of wins, so a brain that restarted and
// has not polled yet (as_of null) does not blank a standing this proxy holds.
// The brain's schedule (next_poll_at) and the owner's re-check
// (recheck_epoch) are always the new ones. An account the
// brain no longer serves is gone.
func mergeStandings(prev, next map[string]acctStanding) map[string]acctStanding {
	out := make(map[string]acctStanding, len(next))
	for a, n := range next {
		if p, had := prev[a]; had && p.AsOf != nil && (n.AsOf == nil || *n.AsOf < *p.AsOf) {
			p.NextPollAt, p.RecheckEpoch = n.NextPollAt, n.RecheckEpoch
			n = p
		}
		out[a] = n
	}
	return out
}

// staleMax is the brain's stale limit, seconds (settings.stale_max).
func (tn *tenant) staleMax() float64 {
	tn.pref.mu.RLock()
	defer tn.pref.mu.RUnlock()
	return tn.pref.staleMax
}

// routeOf is the brain's route, the pots and pay_order, snapshotted.
func (tn *tenant) routeOf(routeID string) (routePref, map[string]potSettings, []string, bool) {
	tn.pref.mu.RLock()
	defer tn.pref.mu.RUnlock()
	r, ok := tn.pref.routes[routeID]
	return r, tn.pref.pots, tn.pref.payOrder, ok
}

// routeCheck is the dial check for one (route, account) on this proxy's facts
// (§1.6, §2.7): the brain's policy, never widened, over standingFor. model is
// the upstream model the dial would send, for a record that names only it.
// Such a record is a limit no reading carries. A spent one the check reads as
// a spent window that binds this dial alone, so money decides it as it
// decides any spent limit (paid, no(pay), no(unknown), no(spent)); a model
// not entitled is no (blocked), which no money opens. held reports a
// `no (spent)` that only the record makes: without it the route would be
// included, and no money can open behind it. PR11 re-admits exactly these
// (§2.2 row 14): an included dial whose 2xx ends the record, never one the
// upstream may bill. It is the check with no request (the brain's own:
// unknown cost, one output); a dial that knows its request asks
// routeCheckStanding.
func (tn *tenant) routeCheck(routeID, account, model string, now time.Time) (c dialCheck, held bool) {
	c, _, held = tn.routeCheckStanding(routeID, account, model, checkRequest{}, now)
	return c, held
}

// routeCheckStanding is routeCheck for one request (req: what a Runway
// task's max_cost and output count are, §1.6), with the standing the check
// read, a model record's window included: the unavailable reply names the
// window and the basis of the time it reports from it.
func (tn *tenant) routeCheckStanding(routeID, account, model string, req checkRequest, now time.Time) (c dialCheck, st *acctStanding, held bool) {
	return tn.routeCheckOn(routeID, account, model, req, tn.ledgerView(unixSeconds(now)), now)
}

// routeCheckOn is routeCheckStanding on a ledger view the caller holds: the
// reservation (ledgerReserve) checks and reserves under one ledgerMu.
func (tn *tenant) routeCheckOn(routeID, account, model string, req checkRequest, ledger *checkLedger, now time.Time) (c dialCheck, st *acctStanding, held bool) {
	r, pots, _, ok := tn.routeOf(routeID)
	if !ok {
		return dialCheck{Check: checkNo, Reason: "unknown", Basis: "unknown"}, nil, false
	}
	return tn.policyCheckOn(routeID, r.routePolicy, pots, account, model, req, ledger, now)
}

// policyCheckOn is routeCheckOn for a route policy in hand: the brain's
// (routeCheckOn), or one this proxy holds itself (the video door's,
// video_api.go, whose pay is this proxy's own configuration).
func (tn *tenant) policyCheckOn(routeID string, r routePolicy, pots map[string]potSettings, account, model string, req checkRequest, ledger *checkLedger, now time.Time) (c dialCheck, st *acctStanding, held bool) {
	st = tn.standingFor(account, now)
	c = evalDialCheck(r, account, st, pots, ledger, tn.staleMax(), unixSeconds(now), req)
	c.route = routeID
	if model == "" {
		return c, st, false
	}
	limit := modelLimit(model)
	tn.recMu.Lock()
	rec, has := tn.records[recKey{account, limit}]
	tn.recMu.Unlock()
	if !has || !rec.closes() || rec.kind == recUnpaid || !rec.live(now) {
		return c, st, false
	}
	if rec.kind == recNotEntitled {
		// The plan refuses the model (§2.8, D5): blocked, not spent, and
		// whatever the account's own check says. A spent allowance would
		// reset into the same refusal and money behind it would meet it
		// too, so nothing is dialled on it, and the reply states no time
		// and no option. The record keeps its clamp (clampedUntil's
		// not_entitled floor) as the time to try again, and no more.
		return dialCheck{Check: checkNo, Reason: whyBlocked, Basis: c.Basis, route: routeID}, st, false
	}
	if c.no() {
		return c, st, false
	}
	// As for a window record (standingFor), with no period and no scope: it
	// binds this dial, and no stop_at names it.
	st.Allowance.Windows[limit] = recordWindow(rec, checkWindow{}, now)
	was := c
	c = evalDialCheck(r, account, st, pots, ledger, tn.staleMax(), unixSeconds(now), req)
	c.route = routeID
	// Only a spent record holds: a model the plan does not include would
	// refuse a re-admission the same way.
	c.held = was.included() && c.no() && c.Reason == "spent" && rec.kind == recSpent
	return c, st, c.held
}

// ---- ranking a route's accounts (§2.2 rows 8, 9, 16) ----

// acctPick is one account of a route, judged.
type acctPick struct {
	account   string
	check     dialCheck
	throttled bool
	held      bool // `no` only by an unattributed record
}

// tier: included before paid, a throttle after the unthrottled of its kind.
func (p acctPick) tier() int {
	t := 0
	if p.check.paid() {
		t = 2
	}
	if p.throttled {
		t++
	}
	return t
}

// rankRouteAccounts judges every account of a route and orders the ones whose
// check is not `no`: included before paid, a throttled account after the
// unthrottled ones of its funding (it deranks, §2.3), the incumbent first
// within its tier, then included accounts under the hard line, by least used,
// with an unknown use after every known one (it is not 0), and paid accounts
// in pay_order. The rest come back as refused, soonest back first. req is the
// request the dial is for (zero: none known, the brain's own check).
func (tn *tenant) rankRouteAccounts(routeID, model string, req checkRequest, incumbent string, accounts []string, pct func(string) *float64, now time.Time) (ok, refused []acctPick) {
	_, _, payOrder, _ := tn.routeOf(routeID)
	hard := tn.hardPct()
	for _, a := range accounts {
		if a == "" {
			continue
		}
		c, _, held := tn.routeCheckStanding(routeID, a, model, req, now)
		p := acctPick{account: a, check: c, held: held}
		if c.no() {
			refused = append(refused, p)
			continue
		}
		p.throttled = !tn.throttledUntil(a, now).IsZero()
		ok = append(ok, p)
	}
	sort.SliceStable(ok, func(i, j int) bool {
		x, y := ok[i], ok[j]
		if x.tier() != y.tier() {
			return x.tier() < y.tier()
		}
		if (x.account == incumbent) != (y.account == incumbent) {
			return x.account == incumbent
		}
		if x.check.paid() {
			if x.check.Pot != y.check.Pot {
				return payOrderLess(x.check.Pot, 0, y.check.Pot, 0, payOrder)
			}
			return x.account < y.account
		}
		var px, py *float64
		if pct != nil {
			px, py = pct(x.account), pct(y.account)
		}
		if (px == nil) != (py == nil) {
			return px != nil
		}
		if px != nil {
			if ox, oy := *px >= hard, *py >= hard; ox != oy {
				return !ox
			}
			if *px != *py {
				return *px < *py
			}
		}
		return x.account < y.account
	})
	sort.SliceStable(refused, func(i, j int) bool { return checkLess(refused[i].check, refused[j].check) })
	return ok, refused
}

// routeAccounts is the route's backing accounts as the brain registered them.
func (tn *tenant) routeAccounts(routeID string) []string {
	r, _, _, ok := tn.routeOf(routeID)
	if !ok {
		return nil
	}
	return r.Accounts
}

// ---- rendering ----

// renderPicks is the check of each account, for a dial's log line.
func renderPicks(ps []acctPick) string {
	parts := make([]string, 0, len(ps))
	for _, p := range ps {
		s := p.account + ":" + p.check.String()
		if p.throttled {
			s += "+throttled"
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, ", ")
}

// recordsStatus is /_switch/status's view of the store.
func (tn *tenant) recordsStatus(now time.Time) map[string]any {
	tn.recMu.Lock()
	defer tn.recMu.Unlock()
	out := map[string]any{}
	for k, r := range tn.records {
		if !r.live(now) {
			continue
		}
		var resets any
		if !r.resets.IsZero() {
			resets = r.resets.UTC().Format(time.RFC3339)
		}
		// An attributed record that named no reset ends by reading: no
		// until, nothing remaining to count down.
		var until, remaining any
		if !r.until.IsZero() {
			until = r.until.UTC().Format(time.RFC3339)
			remaining = r.until.Sub(now).Round(time.Second).String()
		}
		out[k.account+"/"+k.limit] = map[string]any{
			"kind": r.kind, "attributed": r.attributed, "remaining": remaining,
			"until": until, "resets": resets, "detail": r.detail,
		}
	}
	return out
}
