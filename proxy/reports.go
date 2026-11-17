package main

// ---- reports to the brain (quota-model 64 §5) ----
//
// What this proxy learns about an account between the brain's polls goes to
// the brain as events on its POST /reports, so the brain's document (and with
// it every subscriber's) moves with traffic instead of at the next poll:
//
//   - `reading`: one window of one response's usage headers, or a codex
//     response's credits (pot "credits": open false, balance), each field null
//     when the response did not carry it. Only news is sent (reportReading).
//     And an api account's money pot shut (its pot, open false, balance
//     null) when a door's provider refused for money (reportPotShut): the
//     brain's prepaid notices read the account dry by it, which for a pot
//     the brain cannot read (no balance published) is the only way it learns.
//   - `refusal`: a spent record on a window (records.go): its until, the
//     reset it named (resets_at), by_reading and seen, with `ended: true`
//     when a 2xx ends it; and a record of a limit the brain has no window
//     for (quota-model 67 §9), a model's (spent or not_entitled) or a pool's
//     (spent), as `limit` (model:<m>, pool:<name>) with window null, seen
//     null and its clamped until. Each carries its class. Throttles, money
//     pots, the entitlement, rolling counts and an api account's records
//     (Runway's, money) are not sent as refusals: an api account's money
//     refusal goes as its pot shut, a `reading` (reportPotShut).
//   - `ledger`: one ledger entry (ledger.go), its fields unchanged, its id the
//     event's.
//   - `served`: one request an account answered (served.go, quota-model 65
//     §7), with this process's boot id and its seq, counted from 1 in the
//     order requests end.
//
// One queue holds the readings and refusals. A reading or a refusal is keyed (kind, account,
// window or pot), and a newer one replaces the one still queued; ledger
// entries are never replaced: the queue's ledger half is ledgerOwn itself
// (every entry not yet accepted and not refused). One sender (runReporter),
// woken through a one-slot channel, keeps one post in flight and sends what is
// queued in batches of at most reportBatchMax, readings and refusals first,
// then ledger entries, then served events (reportBatch). Served events are
// never replaced: they wait in their own queue, oldest first, bounded by
// reportServedMax, past which the oldest is dropped and counted (the brain sees
// the seq gap). Nothing is sent from the
// request path or the stream reader: they queue and wake the sender.
//
// The brain answers each event: applied and ignored are done; refused is
// dropped (a ledger entry or a served event refused is counted here and never
// posted again); retry stays queued. A post that fails, or that leaves
// anything to retry, waits reportBackoffMin, doubling to reportBackoffMax,
// before the next. On every stream connection's first event the proxy queues
// each live record the brain takes and the last reading it reported of each window
// and pot (reportResync), so a brain that restarted or missed a post catches
// up. At
// shutdown, one post of what is queued, bounded by reportShutdownPost
// (reportsFlush).

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	reportReading = "reading"
	reportRefusal = "refusal"
	reportLedger  = "ledger"
	reportServed  = "served"

	// reportBatchMax is the most events one post carries: the brain takes 1
	// to 500.
	reportBatchMax = 500
)

// Variables, not constants, so tests can shrink them; production never sets them.
var (
	reportBackoffMin = 1 * time.Second
	reportBackoffMax = 30 * time.Second
	// reportShutdownPost bounds the one post a shutdown makes. The stop is
	// SWITCH_SHUTDOWN_GRACE (20s) + runwayCancelTimeout (10s, the paid trips'
	// DELETEs, each sent after its charge is entered) + this (5s) = 35s, the
	// container's stop_grace_period.
	reportShutdownPost = 5 * time.Second
	// reportServedMax bounds the served events waiting while the brain cannot
	// be reached: past it the oldest is dropped and counted.
	reportServedMax = 20000
)

// reportKey is what a newer event replaces: one kind on one window, one
// limit the brain has no window for, or one pot, of one account.
type reportKey struct{ kind, account, window, limit, pot string }

// reportEvent is one reading or refusal as POST /reports takes it (wire).
type reportEvent struct {
	id, kind, account string
	readAt            float64
	window, pot       string
	// a window's reading
	usedPct, resetsAt *float64
	// a pot's reading: open is only ever false (headers can say there are no
	// credits, never that a pot is open)
	open    *bool
	balance *float64
	// a refusal: limit names a limit the brain has no window for (window
	// then ""), class is the record's; until is the record's (clamped) end;
	// resetsAt above is the reset the refusal itself named, null when it
	// named none
	limit, class string
	until        *float64
	byReading    bool
	seen         limitSeen
	ended        bool
}

// wire is the event's body: every field its kind carries, null when unknown.
func (e reportEvent) wire() map[string]any {
	m := map[string]any{"id": e.id, "kind": e.kind, "account": e.account, "read_at": e.readAt}
	switch {
	case e.kind == reportRefusal:
		m["window"], m["limit"], m["class"] = nil, nil, e.class
		if e.window != "" {
			m["window"] = e.window
		}
		if e.limit != "" {
			m["limit"] = e.limit
		}
		m["until"], m["resets_at"], m["by_reading"] = e.until, e.resetsAt, e.byReading
		m["seen"] = nil
		if e.limit == "" {
			m["seen"] = map[string]any{"used_pct": e.seen.usedPct, "resets_at": e.seen.resetsAt}
		}
		if e.ended {
			m["ended"] = true
		}
	case e.pot != "":
		m["pot"], m["open"], m["balance"] = e.pot, e.open, e.balance
	default:
		m["window"], m["used_pct"], m["resets_at"] = e.window, e.usedPct, e.resetsAt
	}
	return m
}

// ledgerEvent is a ledger entry as an event: its fields unchanged, and its
// kind.
type ledgerEvent struct {
	Kind string `json:"kind"`
	ledgerEntry
}

// queuedReport is one queued reading or refusal; gen orders the queue and
// tells a sent event from the newer one that replaced it while it was in
// flight.
type queuedReport struct {
	ev  reportEvent
	gen uint64
}

// Per tenant (MEMBERS-PLAN 1b), on the tenant struct: repMu; repQueue (what
// waits to be sent, by key); repGen; repLast (the last reading of each window
// or pot this proxy queued: the news filter's own half, reportReading, and
// what a resync sends again; a reading that was not news is not put here on
// its own account, so after a refusal's own 100, a 99 a point below stays out
// until stale_max/2 re-reports it for freshness, 64 §8's known edge, narrowed,
// not fixed); repServed (the served events waiting, oldest first), repServedSeq
// (the last seq given), repServedDropped, repServedRefused and
// repServedUnencodable (the events the bound dropped, the brain refused, and
// this side could not encode, /_switch/status); repLastErr (the last post's
// failure, "" after a clean one: /_switch/status); repWake (wakes the sender;
// one slot, so a wake never blocks and many coalesce into one); repSlot (the
// one post in flight: a channel, not a mutex, so the shutdown flush gives up
// waiting at its bound); repRunner (the running sender, startReporter, nil
// when none).

// reporterRun is a running sender: stop ends it after its post in flight,
// abort cancels that post too.
type reporterRun struct {
	stop, abort context.CancelFunc
	done        chan struct{}
}

// reportHost names this proxy in the brain's log: the container's hostname,
// which the compose file sets to claude-switch-proxy.
var reportHost = sync.OnceValue(func() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "claude-switch-proxy"
	}
	return h
})

// repBoot names this process to the brain on every served event: its seq
// starts again at 1 under a new boot.
var repBoot = func() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "boot-" + hex.EncodeToString(b[:])
}()

// servedQueued is one served event waiting to be sent.
type servedQueued struct{ ev servedEvent }

// reportServedEvent queues a served event under the next seq and wakes the
// sender. Called once a request's response has ended; it never blocks beyond
// repMu.
func (tn *tenant) reportServedEvent(ev servedEvent) int64 {
	tn.repMu.Lock()
	tn.repServedSeq++
	ev.id, ev.boot, ev.seq = newReportID(), repBoot, tn.repServedSeq
	tn.repServed = append(tn.repServed, &servedQueued{ev: ev})
	for len(tn.repServed) > reportServedMax {
		tn.repServed[0] = nil
		tn.repServed = tn.repServed[1:]
		tn.repServedDropped++
	}
	seq := ev.seq
	tn.repMu.Unlock()
	tn.reportWake()
	return seq
}

func newReportID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "report-" + hex.EncodeToString(b[:])
}

// reportWake wakes the sender. It never blocks.
func (tn *tenant) reportWake() {
	select {
	case tn.repWake <- struct{}{}:
	default:
	}
}

// queueReportLocked queues ev under k unless an event of k at least as new
// is already queued. Caller holds repMu.
func (tn *tenant) queueReportLocked(k reportKey, ev reportEvent) {
	if q, had := tn.repQueue[k]; had && q.ev.readAt > ev.readAt {
		return
	}
	if ev.id == "" {
		ev.id = newReportID()
	}
	tn.repGen++
	tn.repQueue[k] = queuedReport{ev: ev, gen: tn.repGen}
}

// ---- readings: only news ----

// reportBase is what the brain is taken to know of a window or a pot: its
// published reading, or this proxy's last report of it, whichever is newer.
type reportBase struct {
	asOf              float64
	usedPct, resetsAt *float64
	open              *bool
	balance           *float64
}

// newerBase is the newer of the brain's reading (ok: published) and the last
// report (ok: made). On a tie it is the last report: the brain stamps a window
// with the read_at of the report it applied, and a hold then publishes that
// window at 100 under the same as_of, which is the brain's ruling, not a
// newer reading. Judged against it, every unchanged reading after a refusal
// would differ and be sent again for the hold's life.
func newerBase(brain reportBase, brainOK bool, last reportEvent, lastOK bool) (reportBase, bool) {
	lb := reportBase{asOf: last.readAt, usedPct: last.usedPct, resetsAt: last.resetsAt, open: last.open, balance: last.balance}
	switch {
	case brainOK && lastOK:
		if lb.asOf >= brain.asOf {
			return lb, true
		}
		return brain, true
	case brainOK:
		return brain, true
	case lastOK:
		return lb, true
	}
	return reportBase{}, false
}

// windowNews: the reading of a window is news against base (§5 "Only news"):
// nothing newer is known of it, its use differs and does not agree with
// base's (useAgrees), its reset moved by more than recResetSlack, or what is
// known is staleMax/2 old. A reading no newer than what is known is not: the
// brain would ignore it.
func windowNews(ev reportEvent, base reportBase, known bool, staleMax float64) bool {
	if !known {
		return true
	}
	if ev.readAt <= base.asOf {
		return false
	}
	if ev.usedPct != nil && (base.usedPct == nil || *ev.usedPct != *base.usedPct) &&
		!useAgrees(base.usedPct, base.resetsAt, ev.usedPct, ev.resetsAt) {
		return true
	}
	if ev.resetsAt != nil && (base.resetsAt == nil || math.Abs(*ev.resetsAt-*base.resetsAt) > recResetSlack.Seconds()) {
		return true
	}
	return staleMax > 0 && ev.readAt-base.asOf >= staleMax/2
}

// potNews is windowNews for the credits pot: it reads shut where the brain
// did not, its balance differs, or what is known is staleMax/2 old.
func potNews(ev reportEvent, base reportBase, known bool, staleMax float64) bool {
	if !known {
		return true
	}
	if ev.readAt <= base.asOf {
		return false
	}
	if ev.open != nil && !*ev.open && (base.open == nil || *base.open) {
		return true
	}
	if ev.balance != nil && (base.balance == nil || *ev.balance != *base.balance) {
		return true
	}
	return staleMax > 0 && ev.readAt-base.asOf >= staleMax/2
}

// reportHeaderReading queues what of account's reading from one response is
// news; what it queues is also what a resync sends again. noteHeaderReading
// calls it for each reading it keeps.
func (tn *tenant) reportHeaderReading(account string, hr headerReading) {
	at := unixSeconds(hr.asOf)
	type known struct {
		base reportBase
		ok   bool
	}
	brain := map[reportKey]known{}
	tn.pref.mu.RLock()
	st, measured := tn.pref.accounts[account]
	staleMax := tn.pref.staleMax
	if measured && st.AsOf != nil {
		for name := range hr.windows {
			w, ok := st.Allowance.Windows[name]
			if !ok {
				continue
			}
			asOf := st.AsOf
			if w.AsOf != nil {
				asOf = w.AsOf
			}
			brain[reportKey{kind: reportReading, account: account, window: name}] = known{
				reportBase{asOf: *asOf, usedPct: w.UsedPct, resetsAt: w.ResetsAt}, true}
		}
		if p, ok := st.Money[potCredits]; ok {
			brain[reportKey{kind: reportReading, account: account, pot: potCredits}] = known{
				reportBase{asOf: *st.AsOf, open: p.Open, balance: p.Balance}, true}
		}
	}
	tn.pref.mu.RUnlock()

	var evs []reportEvent
	names := make([]string, 0, len(hr.windows))
	for n := range hr.windows {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		w := hr.windows[n]
		if w.UsedPct == nil && w.ResetsAt == nil {
			continue // the response said nothing of it
		}
		if w.UsedPct == nil && !brain[reportKey{kind: reportReading, account: account, window: n}].ok {
			continue // a reading with no use never adds a window; the brain would ignore it
		}
		evs = append(evs, reportEvent{kind: reportReading, account: account, readAt: at, window: n,
			usedPct: w.UsedPct, resetsAt: w.ResetsAt})
	}
	if hr.credits && (hr.creditsOff || hr.balance != nil) {
		ev := reportEvent{kind: reportReading, account: account, readAt: at, pot: potCredits, balance: hr.balance}
		if hr.creditsOff {
			f := false
			ev.open = &f
		}
		evs = append(evs, ev)
	}
	if len(evs) == 0 {
		return
	}
	queued := false
	tn.repMu.Lock()
	for _, ev := range evs {
		k := reportKey{kind: reportReading, account: account, window: ev.window, pot: ev.pot}
		last, lastOK := tn.repLast[k]
		base, ok := newerBase(brain[k].base, brain[k].ok, last, lastOK)
		news := windowNews(ev, base, ok, staleMax)
		if ev.pot != "" {
			news = potNews(ev, base, ok, staleMax)
		}
		if !news {
			continue
		}
		tn.repLast[k] = ev
		tn.queueReportLocked(k, ev)
		queued = true
	}
	tn.repMu.Unlock()
	if queued {
		tn.reportWake()
	}
}

// ---- an api account's money pot shut ----

// apiMoneyPot is the name of account's money pot when the brain's document
// says it is an api account (one money pot), else "".
func (tn *tenant) apiMoneyPot(account string) string {
	tn.pref.mu.RLock()
	st, ok := tn.pref.accounts[account]
	tn.pref.mu.RUnlock()
	if !ok || st.Kind != "api" || len(st.Money) != 1 {
		return ""
	}
	for pot := range st.Money {
		return pot
	}
	return ""
}

// reportPotShut queues an api account's money pot as shut at `at`: its
// provider refused a request for money. It is kept as the pot's last reading,
// so a resync sends it again; the brain ignores it once a poll, a counted
// ledger entry on the pot or the owner's re-check is newer. The household
// doors call it from their money refusals, and recordRefusal from any unpaid
// refusal on an api account's pot.
func (tn *tenant) reportPotShut(account, pot string, at time.Time) {
	if account == "" || pot == "" {
		return
	}
	shut := false
	ev := reportEvent{kind: reportReading, account: account, readAt: unixSeconds(at), pot: pot, open: &shut}
	k := reportKey{kind: reportReading, account: account, pot: pot}
	tn.repMu.Lock()
	tn.repLast[k] = ev
	tn.queueReportLocked(k, ev)
	tn.repMu.Unlock()
	tn.reportWake()
}

// ---- refusals ----

// reportedAs is how a record under limit goes to the brain (64 §5, 67 §9):
// a spent window as its window; a model's record, spent or not_entitled, as
// the limit model:<m> (its key); a pool's, spent, as pool:<name>. ok is false
// for every other record, and for any record of an api account (Runway's
// are money, outside 65's figure): those are not sent.
func (tn *tenant) reportedAs(account, limit string, r refusalRecord) (window, lim string, ok bool) {
	switch {
	case r.kind == recSpent && r.limKind == limWindow:
		window = limit
	case r.limKind == limModel && (r.kind == recSpent || r.kind == recNotEntitled):
		lim = limit
	case r.limKind == limPool && r.kind == recSpent:
		lim = "pool:" + limit
	default:
		return "", "", false
	}
	tn.pref.mu.RLock()
	st, known := tn.pref.accounts[account]
	tn.pref.mu.RUnlock()
	if known && st.Kind == "api" {
		return "", "", false
	}
	return window, lim, true
}

// refusalKey is the queue's key of a record's refusal event.
func refusalKey(account, window, lim string) reportKey {
	return reportKey{kind: reportRefusal, account: account, window: window, limit: lim}
}

// refusalEvent is a record as an event, under the window or limit reportedAs
// names: read_at when it refused, or, ended, when the 2xx that ended it was
// sent.
func refusalEvent(account, window, lim string, r refusalRecord, ended bool, at time.Time) reportEvent {
	ev := reportEvent{kind: reportRefusal, account: account, window: window, limit: lim, class: r.kind,
		readAt: unixSeconds(r.refusedAt), byReading: r.attributed, seen: r.seen, ended: ended}
	if !r.until.IsZero() {
		u := unixSeconds(r.until)
		ev.until = &u
	}
	if !r.resets.IsZero() {
		rs := unixSeconds(r.resets) // the brain needs both to make recordWindow's window
		ev.resetsAt = &rs
	}
	if ended {
		ev.readAt = unixSeconds(at)
	}
	return ev
}

// reportRecord queues a record's refusal, or its end, when the brain takes
// it (reportedAs).
func (tn *tenant) reportRecord(account, limit string, r refusalRecord, ended bool, at time.Time) {
	if account == "" || limit == "" {
		return
	}
	window, lim, ok := tn.reportedAs(account, limit, r)
	if !ok {
		return
	}
	tn.repMu.Lock()
	tn.queueReportLocked(refusalKey(account, window, lim), refusalEvent(account, window, lim, r, ended, at))
	tn.repMu.Unlock()
	tn.reportWake()
}

// reportResync queues every live record the brain takes and the last reading reported
// of each window and pot, for a brain that may have restarted or missed a
// post. The last reported, not the latest: a reading that was not news (the
// 99 after a refusal's own 100) would end a restarted brain's hold one
// refusal early. Once stale_max/2 re-reports that 99 it is the last reported,
// and the edge remains (64 §8). It runs on each stream connection's first
// event: no network, only the queue.
func (tn *tenant) reportResync() {
	now := nowFn()
	type rec struct {
		k          recKey
		r          refusalRecord
		window, li string
	}
	var live, recs []rec
	tn.recMu.Lock()
	for k, r := range tn.records {
		if r.live(now) {
			live = append(live, rec{k: k, r: r})
		}
	}
	tn.recMu.Unlock()
	// reportedAs reads the brain's document: never under recMu.
	for _, x := range live {
		if window, lim, ok := tn.reportedAs(x.k.account, x.k.limit, x.r); ok {
			recs = append(recs, rec{x.k, x.r, window, lim})
		}
	}
	sort.Slice(recs, func(i, j int) bool {
		return recs[i].k.account+"/"+recs[i].k.limit < recs[j].k.account+"/"+recs[j].k.limit
	})
	tn.repMu.Lock()
	keys := make([]reportKey, 0, len(tn.repLast))
	for k := range tn.repLast {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		return a.account+"/"+a.window+"/"+a.pot < b.account+"/"+b.window+"/"+b.pot
	})
	for _, k := range keys {
		ev := tn.repLast[k]
		ev.id = ""
		tn.queueReportLocked(k, ev)
	}
	for _, x := range recs {
		tn.queueReportLocked(refusalKey(x.k.account, x.window, x.li),
			refusalEvent(x.k.account, x.window, x.li, x.r, false, time.Time{}))
	}
	n := len(keys) + len(recs)
	tn.repMu.Unlock()
	if n > 0 {
		tn.reportWake()
	}
}

// ---- the sender ----

// startReporter starts the tenant's one sender against its brain routes
// (tenant.brainURL + /reports) with switchKey; stopReporter ends it.
// tenant.start starts it, beside the brain stream.
func (tn *tenant) startReporter() {
	brain, key := tn.brainURL(), switchKey
	loop, stop := context.WithCancel(context.Background())
	post, abort := context.WithCancel(context.Background())
	run := &reporterRun{stop: stop, abort: abort, done: make(chan struct{})}
	tn.repMu.Lock()
	tn.repRunner = run
	tn.repMu.Unlock()
	go func() {
		defer close(run.done)
		defer abort()
		tn.runReporter(loop, post, brain, key)
	}()
	tn.reportWake() // whatever was queued before it started
}

// stopReporter ends the sender and waits for it: a post in flight may finish
// until bound ends, and is then cancelled, what it carried staying queued
// (a resend is safe: the brain dedupes ledger ids, and a reading or a refusal
// applied twice changes nothing). A no-op when none runs.
func (tn *tenant) stopReporter(bound context.Context) {
	tn.repMu.Lock()
	run := tn.repRunner
	tn.repRunner = nil
	tn.repMu.Unlock()
	if run == nil {
		return
	}
	run.stop()
	select {
	case <-run.done:
	case <-bound.Done():
		run.abort()
		<-run.done
	}
}

// runReporter sends what is queued until loop ends (post cancels a post in
// flight): on each wake, posts until nothing is left, backing off after a
// post that failed or left something to retry. A wake during the backoff
// waits for it.
func (tn *tenant) runReporter(loop, post context.Context, brain, key string) {
	var wait time.Duration
	for {
		select {
		case <-loop.Done():
			return
		case <-tn.repWake:
		}
		for loop.Err() == nil {
			ok, more := tn.reportPost(post, brain, key)
			if loop.Err() != nil || post.Err() != nil {
				return
			}
			if ok {
				wait = 0
				if !more {
					break
				}
				continue
			}
			wait = nextReportBackoff(wait)
			t := time.NewTimer(wait)
			select {
			case <-loop.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
	}
}

func nextReportBackoff(prev time.Duration) time.Duration {
	if prev <= 0 {
		return reportBackoffMin
	}
	return min(2*prev, reportBackoffMax)
}

// sentReport is one event of a post and where its result goes: a queued
// reading or refusal (key, gen), a ledger entry, or a served event.
type sentReport struct {
	key   reportKey
	gen   uint64
	entry *ownEntry
	srv   *servedQueued
	body  any
	id    string
}

// reportBatch is what one post carries: the queue in the order it was
// queued, then ledger entries, oldest first, then served events, oldest
// first, fill the rest, at most reportBatchMax. Readings and refusals go first
// because the queue is bounded by its keys, so it can never starve the
// ledger, while ledger entries the brain keeps asking for again could starve
// every reading behind them. Served events come last: they move no dial.
func (tn *tenant) reportBatch() []sentReport {
	tn.repMu.Lock()
	out := make([]sentReport, 0, len(tn.repQueue))
	for k, q := range tn.repQueue {
		out = append(out, sentReport{key: k, gen: q.gen, id: q.ev.id, body: q.ev.wire()})
	}
	tn.repMu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].gen < out[j].gen })
	if len(out) > reportBatchMax {
		out = out[:reportBatchMax]
	}
	tn.ledgerMu.Lock()
	for _, o := range tn.ledgerOwn {
		if len(out) == reportBatchMax {
			break
		}
		if o.seq == 0 && o.refused == "" {
			out = append(out, sentReport{entry: o, id: o.e.ID, body: ledgerEvent{Kind: reportLedger, ledgerEntry: o.e}})
		}
	}
	tn.ledgerMu.Unlock()
	tn.repMu.Lock()
	for _, q := range tn.repServed {
		if len(out) == reportBatchMax {
			break
		}
		out = append(out, sentReport{srv: q, id: q.ev.id, body: q.ev.wire()})
	}
	tn.repMu.Unlock()
	return out
}

// dropServedLocked takes the served events in done out of the queue. Caller
// holds repMu.
func (tn *tenant) dropServedLocked(done map[*servedQueued]bool) {
	if len(done) == 0 {
		return
	}
	kept := tn.repServed[:0]
	for _, q := range tn.repServed {
		if !done[q] {
			kept = append(kept, q)
		}
	}
	clear(tn.repServed[len(kept):])
	tn.repServed = kept
}

// reportEncode encodes each event of batch on its own. One that cannot be
// encoded (reportDrop) leaves the post, and the rest still go.
func (tn *tenant) reportEncode(batch []sentReport) ([]sentReport, []json.RawMessage) {
	kept := batch[:0:0]
	var events []json.RawMessage
	for _, s := range batch {
		b, err := json.Marshal(s.body)
		if err != nil {
			tn.reportDrop(s, err)
			continue
		}
		kept = append(kept, s)
		events = append(events, b)
	}
	return kept, events
}

// reportDrop drops an event that cannot be encoded, as a refusal is dropped:
// a queued event leaves the queue (unless replaced since), a ledger entry is
// kept and counted here and never posted.
func (tn *tenant) reportDrop(s sentReport, err error) {
	why := "cannot be encoded: " + err.Error()
	if s.entry != nil {
		tn.ledgerMu.Lock()
		s.entry.refused = why
		tn.ledgerLastErr = "dropped: " + s.id + ": " + why
		tn.ledgerMu.Unlock()
		log.Printf("ledger: %s on %s %s; counted here, never posted", s.id, s.entry.e.potID(), why)
		return
	}
	tn.repMu.Lock()
	if s.srv != nil {
		tn.dropServedLocked(map[*servedQueued]bool{s.srv: true})
		tn.repServedUnencodable++
		tn.repLastErr = "dropped: " + s.id + ": " + why
		tn.repMu.Unlock()
		log.Printf("reports: a served event of %s (seq %d) %s; dropped", s.srv.ev.account, s.srv.ev.seq, why)
		return
	}
	if q, had := tn.repQueue[s.key]; had && q.gen == s.gen {
		delete(tn.repQueue, s.key)
	}
	tn.repLastErr = "dropped: " + s.id + ": " + why
	tn.repMu.Unlock()
	log.Printf("reports: a %s of %s %s%s%s %s; dropped", s.key.kind, s.key.account, s.key.window, s.key.limit, s.key.pot, why)
}

// reportResult is one event's answer.
type reportResult struct {
	ID     string `json:"id"`
	Result string `json:"result"`
	Why    string `json:"why"`
}

// reportPost makes one post of what is queued. ok: the post was answered and
// nothing in it is left to retry (an empty queue is ok). more: something is
// still queued after it.
func (tn *tenant) reportPost(ctx context.Context, brain, key string) (ok, more bool) {
	select {
	case tn.repSlot <- struct{}{}:
	case <-ctx.Done():
		return false, tn.reportsPending() > 0 // its events stay queued, as after a failed post
	}
	defer func() { <-tn.repSlot }()
	batch, events := tn.reportEncode(tn.reportBatch())
	if len(batch) == 0 {
		return true, tn.reportsPending() > 0 // never an empty post
	}
	hasLedger := false
	for _, s := range batch {
		hasLedger = hasLedger || s.entry != nil
	}
	body, err := json.Marshal(map[string]any{"reporter": reportHost(), "events": events})
	if err != nil { // raw events already encoded: cannot happen
		tn.reportFailed(fmt.Sprintf("POST /reports: encoding the post: %v", err), len(batch), hasLedger)
		return false, true
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", brain+"/reports", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Switch-Key", key)
	resp, err := brainClient.Do(req)
	if err != nil {
		tn.reportFailed(fmt.Sprintf("POST /reports: %v", err), len(batch), hasLedger)
		return false, true
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	var out struct {
		Results []reportResult `json:"results"`
		Seq     int64          `json:"seq"`
	}
	jerr := json.Unmarshal(b, &out)
	switch {
	case resp.StatusCode != http.StatusOK:
		tn.reportFailed(fmt.Sprintf("POST /reports: HTTP %d %s", resp.StatusCode, strings.TrimSpace(string(b))), len(batch), hasLedger)
		return false, true
	case jerr != nil || len(out.Results) != len(batch):
		tn.reportFailed(fmt.Sprintf("POST /reports: %d result(s) for %d event(s)", len(out.Results), len(batch)), len(batch), hasLedger)
		return false, true
	}
	for i, r := range out.Results {
		if r.ID != batch[i].id {
			tn.reportFailed(fmt.Sprintf("POST /reports: result %d is for %q, not %q", i, r.ID, batch[i].id), len(batch), hasLedger)
			return false, true
		}
		if batch[i].entry != nil && (r.Result == "applied" || r.Result == "ignored") && out.Seq <= 0 {
			tn.reportFailed("POST /reports: a ledger entry taken with no seq", len(batch), hasLedger)
			return false, true
		}
	}
	return tn.reportApply(batch, out.Results, out.Seq)
}

// reportApply settles a post's results: applied and ignored are done (a
// ledger entry accepted at seq), refused is dropped (a ledger entry is kept
// and counted, never posted again), retry stays.
func (tn *tenant) reportApply(batch []sentReport, results []reportResult, seq int64) (ok, more bool) {
	var refused, retry []string
	ledgerAccepted := false
	tn.ledgerMu.Lock()
	for i, s := range batch {
		if s.entry == nil {
			continue
		}
		switch r := results[i]; r.Result {
		case "applied", "ignored":
			s.entry.seq = seq
			ledgerAccepted = true
		case "refused":
			why := r.Why
			if why == "" {
				why = "refused"
			}
			s.entry.refused = why
			refused = append(refused, s.id+": "+why)
			log.Printf("ledger: the brain refused %s on %s: %s; counted here, never posted again",
				s.id, s.entry.e.potID(), why)
		default:
			retry = append(retry, s.id+": "+r.Result+" "+r.Why)
		}
	}
	if ledgerAccepted {
		tn.ledgerLastErr = ""
	}
	if n := len(refused); n > 0 {
		tn.ledgerLastErr = "refused: " + strings.Join(refused, "; ")
	}
	tn.ledgerMu.Unlock()

	tn.repMu.Lock()
	done := map[*servedQueued]bool{}
	for i, s := range batch {
		if s.entry != nil {
			continue
		}
		r := results[i]
		if s.srv != nil {
			switch r.Result {
			case "applied", "ignored", "refused":
				done[s.srv] = true
				if r.Result == "refused" {
					tn.repServedRefused++
					refused = append(refused, s.id+": "+r.Why)
					log.Printf("reports: the brain refused a served event of %s (seq %d): %s; not sent again",
						s.srv.ev.account, s.srv.ev.seq, r.Why)
				}
			default:
				retry = append(retry, s.id+": "+r.Result+" "+r.Why)
			}
			continue
		}
		switch r.Result {
		case "applied", "ignored", "refused":
			if q, had := tn.repQueue[s.key]; had && q.gen == s.gen {
				delete(tn.repQueue, s.key)
			}
			if r.Result == "refused" {
				refused = append(refused, s.id+": "+r.Why)
				log.Printf("reports: the brain refused a %s of %s %s%s%s: %s; not sent again",
					s.key.kind, s.key.account, s.key.window, s.key.limit, s.key.pot, r.Why)
			}
		default:
			retry = append(retry, s.id+": "+r.Result+" "+r.Why)
		}
	}
	tn.dropServedLocked(done)
	was := tn.repLastErr
	switch {
	case len(retry) > 0:
		tn.repLastErr = "retry: " + strings.Join(retry, "; ")
	case len(refused) > 0:
		tn.repLastErr = "refused: " + strings.Join(refused, "; ")
	default:
		tn.repLastErr = ""
	}
	now := tn.repLastErr
	tn.repMu.Unlock()
	if len(retry) > 0 && now != was {
		log.Printf("reports: the brain asked for %d event(s) again; kept for the next post", len(retry))
	}
	if was != "" && !strings.HasPrefix(was, "refused") && now == "" {
		log.Printf("reports: the brain takes reports again")
	}
	return len(retry) == 0, tn.reportsPending() > 0
}

// reportFailed notes a post that failed: everything in it stays queued. It
// logs a failure that differs from the last one.
func (tn *tenant) reportFailed(why string, n int, hasLedger bool) {
	tn.repMu.Lock()
	was := tn.repLastErr
	tn.repLastErr = why
	tn.repMu.Unlock()
	if hasLedger {
		tn.ledgerMu.Lock()
		tn.ledgerLastErr = why
		tn.ledgerMu.Unlock()
	}
	if why != was {
		log.Printf("reports: %s; %d event(s) kept for the next post", why, n)
	}
}

// reportsPending is how many events wait: queued readings and refusals, and
// ledger entries the brain has not accepted (refused ones excepted).
func (tn *tenant) reportsPending() int {
	tn.ledgerMu.Lock()
	n := 0
	for _, o := range tn.ledgerOwn {
		if o.seq == 0 && o.refused == "" {
			n++
		}
	}
	tn.ledgerMu.Unlock()
	tn.repMu.Lock()
	n += len(tn.repQueue) + len(tn.repServed)
	tn.repMu.Unlock()
	return n
}

// reportsFlush is shutdown's last act, bounded as a whole by
// reportShutdownPost: the sender stops (its post in flight may finish inside
// the bound, else it is cancelled and its events stay queued), then what is
// queued goes in one post, so an entry settled during the drain, or left by a
// post that failed, is not lost to the restart when the brain can take it.
// What is still unposted after it is lost (ledger.go's header).
func (tn *tenant) reportsFlush() {
	ctx, cancel := context.WithTimeout(context.Background(), reportShutdownPost)
	defer cancel()
	tn.stopReporter(ctx)
	if tn.brainURL() == "" {
		return
	}
	for {
		ok, more := tn.reportPost(ctx, tn.brainURL(), switchKey)
		if !ok || !more || ctx.Err() != nil {
			break
		}
	}
	tn.ledgerMu.Lock()
	n := 0
	for _, o := range tn.ledgerOwn {
		if o.seq == 0 && o.refused == "" {
			n++
		}
	}
	tn.ledgerMu.Unlock()
	if n > 0 {
		log.Printf("ledger: shutdown with %d entr(ies) the brain has not accepted; lost to its ledger", n)
	}
	tn.repMu.Lock()
	ns := len(tn.repServed)
	tn.repMu.Unlock()
	if ns > 0 {
		log.Printf("reports: shutdown with %d served event(s) unsent; the brain sees the seq gap", ns)
	}
}

// reportsStatus is /_switch/status's reports block: how many events wait, the
// last post's failure, and the served events: this process's boot and last
// seq, how many wait, and how many the bound dropped and the brain refused.
func (tn *tenant) reportsStatus() map[string]any {
	n := tn.reportsPending()
	tn.repMu.Lock()
	defer tn.repMu.Unlock()
	return map[string]any{"pending": n, "last_error": tn.repLastErr,
		"served": map[string]any{"boot": repBoot, "seq": tn.repServedSeq, "pending": len(tn.repServed),
			"dropped": tn.repServedDropped, "refused": tn.repServedRefused, "unencodable": tn.repServedUnencodable}}
}

// reportsFlushAll is shutdown's last act for every tenant, each bounded by
// reportShutdownPost; the flushes run together so the bound is the whole's.
func reportsFlushAll() {
	var wg sync.WaitGroup
	for _, tn := range allTenants() {
		wg.Add(1)
		go func(tn *tenant) { defer wg.Done(); tn.reportsFlush() }(tn)
	}
	wg.Wait()
}

// stopReporterNow ends the tenant's sender, cutting a post in flight, and
// waits for it to return, bounded: for a tenant that left the index.
func (tn *tenant) stopReporterNow(bound context.Context) {
	tn.repMu.Lock()
	run := tn.repRunner
	tn.repRunner = nil
	tn.repMu.Unlock()
	if run == nil {
		return
	}
	run.abort()
	run.stop()
	select {
	case <-run.done:
	case <-bound.Done():
		log.Printf("⚠️  reports: %s: the sender did not return after its stop", tn.id)
	}
}
