package main

// Reports to the brain (quota-model 64 §5, reports.go): the queue and what
// replaces what, the news filter, the brain's per-event results, the backoff,
// the resync on each stream connection, which records are sent, and, end to
// end against a fake brain, a refusal, a reading and a ledger entry arriving
// on POST /reports.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- helpers ----

// resetReports empties the reports' queue and memory, returning the restore.
// resetQuotaState calls it.
func resetReports() func() {
	lan.repMu.Lock()
	oq, ol, oe := lan.repQueue, lan.repLast, lan.repLastErr
	lan.repQueue, lan.repLast, lan.repLastErr = map[reportKey]queuedReport{}, map[reportKey]reportEvent{}, ""
	osrv, oseq, odrop, oref, ounenc, omax := lan.repServed, lan.repServedSeq, lan.repServedDropped, lan.repServedRefused, lan.repServedUnencodable, reportServedMax
	lan.repServed, lan.repServedSeq, lan.repServedDropped, lan.repServedRefused, lan.repServedUnencodable = nil, 0, 0, 0, 0
	lan.repMu.Unlock()
	return func() {
		lan.repMu.Lock()
		lan.repQueue, lan.repLast, lan.repLastErr = oq, ol, oe
		lan.repServed, lan.repServedSeq, lan.repServedDropped, lan.repServedRefused, lan.repServedUnencodable, reportServedMax = osrv, oseq, odrop, oref, ounenc, omax
		lan.repMu.Unlock()
	}
}

// runReporterFor runs the sender against brain for the rest of the test.
func runReporterFor(t *testing.T, brain, key string) {
	t.Helper()
	oldBrain, oldKey := brainBase, switchKey
	brainBase, switchKey = brain, key
	lan.startReporter()
	t.Cleanup(func() {
		lan.stopReporterNow(context.Background())
		brainBase, switchKey = oldBrain, oldKey
	})
}

// reportsSettle waits (tests only) up to 5 s for the running sender to have
// sent everything queued.
func reportsSettle() {
	deadline := time.Now().Add(5 * time.Second)
	for lan.reportsPending() > 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
}

// fakeReportsBrain answers POST /reports (key "k") from replies in turn (the
// last one repeats) and keeps every post's events.
func fakeReportsBrain(t *testing.T, replies ...func(events []map[string]any) (int, any)) (url string, got func() [][]map[string]any) {
	t.Helper()
	var mu sync.Mutex
	var posts [][]map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != tpath("/reports") || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("X-Switch-Key") != "k" {
			w.WriteHeader(401)
			return
		}
		var body struct {
			Reporter string           `json:"reporter"`
			Events   []map[string]any `json:"events"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || body.Reporter == "" || len(body.Events) == 0 || len(body.Events) > 500 {
			w.WriteHeader(400)
			return
		}
		mu.Lock()
		posts = append(posts, body.Events)
		i := min(len(posts), len(replies)) - 1
		mu.Unlock()
		code, out := replies[i](body.Events)
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, func() [][]map[string]any {
		mu.Lock()
		defer mu.Unlock()
		return append([][]map[string]any(nil), posts...)
	}
}

// resultsAll answers every event with result, and the ledger's seq.
func resultsAll(es []map[string]any, result string, seq int64) map[string]any {
	rs := make([]map[string]any, len(es))
	for i, e := range es {
		rs[i] = map[string]any{"id": e["id"], "result": result, "why": nil}
	}
	return map[string]any{"results": rs, "seq": seq}
}

func queuedOf(k reportKey) (reportEvent, bool) {
	lan.repMu.Lock()
	defer lan.repMu.Unlock()
	q, ok := lan.repQueue[k]
	return q.ev, ok
}

func queueLen() int {
	lan.repMu.Lock()
	defer lan.repMu.Unlock()
	return len(lan.repQueue)
}

func clearQueue() {
	lan.repMu.Lock()
	lan.repQueue = map[reportKey]queuedReport{}
	lan.repMu.Unlock()
}

func readingKey(account, window string) reportKey {
	return reportKey{kind: reportReading, account: account, window: window}
}

func windowRefusalKey(account, window string) reportKey {
	return refusalKey(account, window, "")
}

func limitRefusalKey(account, lim string) reportKey {
	return refusalKey(account, "", lim)
}

// reading is a header reading at at of windows (name -> used, reset).
func reading(at time.Time, ws map[string][2]*float64) headerReading {
	hr := headerReading{asOf: at, windows: map[string]checkWindow{}}
	for n, v := range ws {
		hr.windows[n] = checkWindow{UsedPct: v[0], ResetsAt: v[1]}
	}
	return hr
}

// ---- the queue ----

// A newer reading or record of a window replaces the one still queued, an
// older one does not, and each window, pot and kind is its own key; ledger
// entries are never replaced, and a post carries at most 500 events, the
// queue first.
func TestReportsCoalescePerKey(t *testing.T) {
	t.Cleanup(resetQuotaState())
	now := time.Now()
	r := epoch(now.Add(3 * time.Hour))
	lan.reportHeaderReading("a", reading(now, map[string][2]*float64{"five_hour": {ptr(50), r}, "seven_day": {ptr(10), nil}}))
	lan.reportHeaderReading("a", reading(now.Add(time.Second), map[string][2]*float64{"five_hour": {ptr(60), r}}))
	lan.repMu.Lock()
	lan.queueReportLocked(readingKey("a", "five_hour"), reportEvent{kind: reportReading, account: "a",
		window: "five_hour", readAt: unixSeconds(now.Add(-time.Minute)), usedPct: ptr(70)})
	lan.repMu.Unlock()
	if ev, ok := queuedOf(readingKey("a", "five_hour")); !ok || *ev.usedPct != 60 {
		t.Fatalf("five_hour queued %+v, want the newer reading (60) and not the older (70)", ev)
	}
	if ev, ok := queuedOf(readingKey("a", "seven_day")); !ok || *ev.usedPct != 10 {
		t.Fatalf("seven_day queued %+v, want its own", ev)
	}
	// A refusal is its own key on the same window, and its end replaces it.
	rec := refusalRecord{kind: recSpent, limKind: limWindow, refusedAt: now, attributed: true}
	lan.reportRecord("a", "five_hour", rec, false, time.Time{})
	lan.reportRecord("a", "five_hour", rec, true, now.Add(time.Second))
	if ev, ok := queuedOf(windowRefusalKey("a", "five_hour")); !ok || !ev.ended {
		t.Fatalf("refusal queued %+v, want its end", ev)
	}
	if n := queueLen(); n != 3 {
		t.Fatalf("%d queued, want three keys", n)
	}
	// Two entries on one pot are two events.
	enter(0.1, now)
	enter(0.1, now)
	if b := lan.reportBatch(); len(b) != 5 || b[0].entry != nil || b[2].entry != nil || b[3].entry == nil || b[4].entry == nil {
		t.Fatalf("batch of %d, want the three, then both entries", len(b))
	}
	for i := range 600 {
		lan.repMu.Lock()
		lan.queueReportLocked(readingKey("b", fmt.Sprintf("window:%dm", i+1)),
			reportEvent{kind: reportReading, account: "b", window: fmt.Sprintf("window:%dm", i+1), readAt: unixSeconds(now)})
		lan.repMu.Unlock()
	}
	if b := lan.reportBatch(); len(b) != reportBatchMax || b[0].entry != nil || b[reportBatchMax-1].entry != nil {
		t.Fatalf("batch of %d, want %d of the queue", len(b), reportBatchMax)
	}
}

// Readings and refusals go first and ledger entries fill the rest: 600
// entries the brain keeps asking for again starve no reading or refusal,
// which the brain takes on the first post.
func TestReportsBatchReadingsBeforeLedger(t *testing.T) {
	t.Cleanup(resetQuotaState())
	now := time.Now()
	for range 600 {
		enter(0.1, now)
	}
	lan.reportHeaderReading("a", reading(now, map[string][2]*float64{"five_hour": {ptr(50), nil}, "seven_day": {ptr(10), nil}}))
	lan.reportRecord("a", "five_hour", refusalRecord{kind: recSpent, limKind: limWindow, refusedAt: now, attributed: true}, false, time.Time{})
	b := lan.reportBatch()
	if len(b) != reportBatchMax {
		t.Fatalf("batch of %d, want %d", len(b), reportBatchMax)
	}
	for i, s := range b {
		if (i < 3) != (s.entry == nil) {
			t.Fatalf("event %d is a ledger entry: %v, want the three readings and refusals first", i, s.entry != nil)
		}
	}
	url, posts := fakeReportsBrain(t, func(es []map[string]any) (int, any) {
		rs := make([]map[string]any, len(es))
		for i, e := range es {
			res := "applied"
			if e["kind"] == "ledger" {
				res = "retry"
			}
			rs[i] = map[string]any{"id": e["id"], "result": res, "why": nil}
		}
		return 200, map[string]any{"results": rs, "seq": 1}
	})
	if ok, more := lan.reportPost(context.Background(), tbase(url), "k"); ok || !more {
		t.Fatalf("ok %v more %v, want the entries left to retry", ok, more)
	}
	if n := queueLen(); n != 0 {
		t.Fatalf("%d readings or refusals still queued behind the ledger", n)
	}
	if p := posts(); len(p) != 1 || p[0][0]["kind"] == "ledger" {
		t.Fatalf("posts = %d, the first event %v", len(p), p[0][0]["kind"])
	}
}

// ---- only news ----

// Each case of §5 "Only news" against the brain's published window and the
// last report, whichever is newer.
func TestReportsNewsFilter(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	reset := now.Add(3 * time.Hour)
	at := func(d time.Duration) time.Time { return now.Add(d) }
	r := func(d time.Duration) *float64 { return epoch(reset.Add(d)) }
	type step struct {
		at   time.Duration
		used *float64
		rs   *float64
	}
	cases := []struct {
		name    string
		brainAt time.Duration // the brain's window as_of, from now
		steps   []step        // readings before the one judged, each kept
		judge   step
		news    bool
	}{
		{"the same as the brain", -time.Minute, nil, step{0, ptr(40), r(0)}, false},
		{"its use differs", -time.Minute, nil, step{0, ptr(41), r(0)}, true},
		// The poll and the headers round one counter half a point apart: a
		// point below what is held is the held reading again (useAgrees).
		{"a point below the brain's", -time.Minute, nil, step{0, ptr(39), r(0)}, false},
		{"half a point below, no reset carried", -time.Minute, nil, step{0, ptr(39.5), nil}, false},
		{"more than a point below the brain's", -time.Minute, nil, step{0, ptr(38.5), r(0)}, true},
		{"a point below, its reset moved past the slack", -time.Minute, nil, step{0, ptr(39), r(11 * time.Minute)}, true},
		{"a point below the brain's stale_max/2 old reading", -901 * time.Second, nil, step{0, ptr(39), r(0)}, true},
		{"its reset within the slack", -time.Minute, nil, step{0, ptr(40), r(5 * time.Minute)}, false},
		{"its reset moved past the slack", -time.Minute, nil, step{0, ptr(40), r(11 * time.Minute)}, true},
		{"the brain's reading stale_max/2 old", -901 * time.Second, nil, step{0, ptr(40), r(0)}, true},
		{"the brain's reading just under stale_max/2", -899 * time.Second, nil, step{0, ptr(40), r(0)}, false},
		{"older than the brain's", -time.Minute, nil, step{-2 * time.Minute, ptr(41), r(0)}, false},
		{"no use and no reset carried", -time.Minute, nil, step{0, nil, nil}, false},
		{"the same as the last report, newer than the brain", -time.Minute,
			[]step{{-10 * time.Second, ptr(41), r(0)}}, step{0, ptr(41), r(0)}, false},
		{"above the last report", -time.Minute,
			[]step{{-10 * time.Second, ptr(41), r(0)}}, step{0, ptr(42), r(0)}, true},
		{"a point below the last report", -time.Minute,
			[]step{{-10 * time.Second, ptr(41), r(0)}}, step{0, ptr(40), r(0)}, false},
		{"two points below the last report", -time.Minute,
			[]step{{-10 * time.Second, ptr(41), r(0)}}, step{0, ptr(39), r(0)}, true},
		{"the last report fresh where the brain's is stale", -2000 * time.Second,
			[]step{{-100 * time.Second, ptr(40), r(0)}}, step{0, ptr(40), r(0)}, false},
		{"the last report stale_max/2 old", -2000 * time.Second,
			[]step{{-1000 * time.Second, ptr(40), r(0)}}, step{0, ptr(40), r(0)}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st := standingAt("anthropic", at(c.brainAt), map[string]checkWindow{"five_hour": fiveHour(40, reset)}, "")
			seedQuota(t, map[string]acctStanding{"a": st}, nil, nil, nil)
			for _, s := range c.steps {
				lan.reportHeaderReading("a", reading(at(s.at), map[string][2]*float64{"five_hour": {s.used, s.rs}}))
			}
			clearQueue()
			lan.reportHeaderReading("a", reading(at(c.judge.at), map[string][2]*float64{"five_hour": {c.judge.used, c.judge.rs}}))
			if _, got := queuedOf(readingKey("a", "five_hour")); got != c.news {
				t.Fatalf("queued %v, want %v", got, c.news)
			}
		})
	}
	t.Run("a window the brain does not publish", func(t *testing.T) {
		seedQuota(t, map[string]acctStanding{"a": standingAt("anthropic", at(-time.Minute), map[string]checkWindow{}, "")}, nil, nil, nil)
		lan.reportHeaderReading("a", reading(now, map[string][2]*float64{"scoped:Fable": {ptr(10), nil}}))
		if _, ok := queuedOf(readingKey("a", "scoped:Fable")); !ok {
			t.Fatal("not queued")
		}
	})
	t.Run("the credits pot", func(t *testing.T) {
		seedQuota(t, map[string]acctStanding{"g": standingAt("codex", at(-time.Minute), map[string]checkWindow{}, "open")}, nil, nil, nil)
		k := reportKey{kind: reportReading, account: "g", pot: potCredits}
		judge := func(hr headerReading) bool {
			clearQueue()
			hr.credits = true
			lan.reportHeaderReading("g", hr)
			_, ok := queuedOf(k)
			return ok
		}
		if judge(headerReading{asOf: now, balance: ptr(10)}) {
			t.Fatal("the brain's own balance, open, was news")
		}
		if !judge(headerReading{asOf: now.Add(time.Second), balance: ptr(9)}) {
			t.Fatal("a different balance was not news")
		}
		if !judge(headerReading{asOf: now.Add(2 * time.Second), balance: ptr(9), creditsOff: true}) {
			t.Fatal("no credits where the brain reads the pot open was not news")
		}
		ev, _ := queuedOf(k)
		if w := ev.wire(); w["pot"] != potCredits || w["window"] != nil {
			t.Fatalf("pot reading wire = %v", w)
		}
		if judge(headerReading{asOf: now.Add(3 * time.Second)}) {
			t.Fatal("a response that said nothing of the pot was news")
		}
		clearQueue()
		lan.reportHeaderReading("g", headerReading{asOf: now.Add(4 * time.Second), credits: true, creditsOff: true, balance: ptr(0)})
		ev, _ = queuedOf(k)
		b, _ := json.Marshal(ev.wire())
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if m["kind"] != "reading" || m["open"] != false || m["balance"] != 0.0 || m["account"] != "g" || m["read_at"] == nil {
			t.Fatalf("pot reading wire = %s", b)
		}
	})
}

// A response's headers reach the queue through noteHeaderReading, codex
// windows and credits by the brain's names, with every field of a reading on
// the wire and null when not carried.
func TestReportsFromCodexHeaders(t *testing.T) {
	t.Cleanup(resetQuotaState())
	now := time.Now().Truncate(time.Second)
	lan.noteHeaderReading("gpt-a", http.Header{
		"X-Codex-Primary-Used-Percent":     {"12"},
		"X-Codex-Primary-Window-Minutes":   {"300"},
		"X-Codex-Secondary-Used-Percent":   {"101"},
		"X-Codex-Secondary-Window-Minutes": {"10080"},
		"X-Codex-Secondary-Reset-At":       {strconv.FormatInt(now.Add(48*time.Hour).Unix(), 10)},
		"X-Codex-Credits-Has-Credits":      {"True"},
		"X-Codex-Credits-Unlimited":        {"False"},
		"X-Codex-Credits-Balance":          {"7.5"},
	}, now)
	five, ok5 := queuedOf(readingKey("gpt-a", "five_hour"))
	week, ok7 := queuedOf(readingKey("gpt-a", "seven_day"))
	pot, okp := queuedOf(reportKey{kind: reportReading, account: "gpt-a", pot: potCredits})
	if !ok5 || !ok7 || !okp {
		t.Fatalf("queued five_hour %v seven_day %v credits %v, want all three", ok5, ok7, okp)
	}
	b, _ := json.Marshal(five.wire())
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if r, has := m["resets_at"]; !has || r != nil || m["used_pct"] != 12.0 || m["window"] != "five_hour" || m["kind"] != "reading" {
		t.Fatalf("five_hour wire = %s, want its use and resets_at null", b)
	}
	if *week.usedPct != 101 || *week.resetsAt != unixSeconds(now.Add(48*time.Hour)) || week.readAt != unixSeconds(now) {
		t.Fatalf("seven_day = %v (codex can pass 100)", week.wire())
	}
	if pot.open != nil || *pot.balance != 7.5 {
		t.Fatalf("credits = %v, want open null (has credits) and the balance", pot.wire())
	}
	// An older response's reading is not kept, so it is not reported.
	clearQueue()
	lan.noteHeaderReading("gpt-a", http.Header{"X-Codex-Primary-Used-Percent": {"50"}, "X-Codex-Primary-Window-Minutes": {"300"}}, now.Add(-time.Minute))
	if n := queueLen(); n != 0 {
		t.Fatalf("an older response queued %d", n)
	}
}

// ---- the brain's results ----

// applied and ignored are done, refused leaves the queue, retry stays; a
// ledger entry refused is counted here and never sent again; an event
// replaced while its post was in flight stays queued; an answer that does
// not match the post is a failure, and everything stays.
func TestReportsResults(t *testing.T) {
	t.Cleanup(resetQuotaState())
	now := time.Now()
	lan.reportHeaderReading("a", reading(now, map[string][2]*float64{
		"w1": {ptr(1), nil}, "w2": {ptr(2), nil}, "w3": {ptr(3), nil}, "w4": {ptr(4), nil}}))
	enter(0.1, now)
	enter(0.2, now)
	byWindow := map[string]string{"w1": "applied", "w2": "ignored", "w3": "refused", "w4": "retry"}
	var replaceInFlight atomic.Bool
	url, posts := fakeReportsBrain(t,
		func(es []map[string]any) (int, any) {
			rs := []map[string]any{}
			for _, e := range es {
				res := byWindow[fmt.Sprint(e["window"])]
				if e["kind"] == "ledger" {
					res = map[bool]string{true: "applied", false: "refused"}[e["charge"] == 0.1]
				}
				rs = append(rs, map[string]any{"id": e["id"], "result": res, "why": "because"})
			}
			return 200, map[string]any{"results": rs, "seq": 7}
		},
		func(es []map[string]any) (int, any) { return 200, map[string]any{"results": []any{}, "seq": 7} },
		func(es []map[string]any) (int, any) {
			if replaceInFlight.CompareAndSwap(false, true) {
				lan.reportHeaderReading("a", reading(now.Add(time.Second), map[string][2]*float64{"w4": {ptr(44), nil}}))
			}
			return 200, resultsAll(es, "applied", 7)
		})
	ok, more := lan.reportPost(context.Background(), tbase(url), "k")
	if ok || !more {
		t.Fatalf("ok %v more %v, want a retry left", ok, more)
	}
	if n := queueLen(); n != 1 {
		t.Fatalf("%d queued, want only the retry", n)
	}
	if _, ok := queuedOf(readingKey("a", "w4")); !ok {
		t.Fatal("the retry left the queue")
	}
	if st := lan.ledgerStatus(); st["pending"] != 0 || st["refused"] != 1 || st["accepted_unpulled"] != 1 {
		t.Fatalf("ledger = %+v, want one accepted, one refused", st)
	}
	if st := lan.reportsStatus(); st["pending"] != 1 || st["last_error"] == "" {
		t.Fatalf("reports = %+v, want the retry pending and said", st)
	}
	// An answer that is not one result per event: a failure, nothing moves.
	if ok, _ := lan.reportPost(context.Background(), tbase(url), "k"); ok {
		t.Fatal("a short answer read as a post the brain took")
	}
	if st := lan.reportsStatus(); st["pending"] != 1 {
		t.Fatalf("reports = %+v after a short answer", st)
	}
	// A newer reading of w4 replaces it while its post is in flight: the
	// applied result is for the old one, and the new one stays.
	if ok, more := lan.reportPost(context.Background(), tbase(url), "k"); !ok || !more {
		t.Fatalf("ok %v more %v, want the replacement left", ok, more)
	}
	if ev, ok := queuedOf(readingKey("a", "w4")); !ok || *ev.usedPct != 44 {
		t.Fatalf("w4 queued %+v, want the replacement", ev)
	}
	if ok, more := lan.reportPost(context.Background(), tbase(url), "k"); !ok || more {
		t.Fatalf("ok %v more %v, want done", ok, more)
	}
	if st := lan.reportsStatus(); st["pending"] != 0 || st["last_error"] != "" {
		t.Fatalf("reports = %+v, want nothing pending and no error", st)
	}
	p := posts()
	if len(p) != 4 || len(p[0]) != 6 || len(p[1]) != 1 || len(p[2]) != 1 || len(p[3]) != 1 {
		t.Fatalf("posts = %v", p)
	}
	for _, e := range p[0] {
		if e["kind"] == "ledger" && e["charge"] == 0.2 {
			for _, later := range p[1:] {
				for _, l := range later {
					if l["id"] == e["id"] {
						t.Fatal("a refused entry was sent again")
					}
				}
			}
		}
	}
}

// A post that fails waits reportBackoffMin, doubling to reportBackoffMax,
// whatever wakes the sender meanwhile; the first post the brain takes ends
// the backoff and empties the queue.
func TestReportsBackoff(t *testing.T) {
	t.Cleanup(resetQuotaState())
	if got := []time.Duration{nextReportBackoff(0), nextReportBackoff(time.Second), nextReportBackoff(16 * time.Second), nextReportBackoff(30 * time.Second)}; got[0] != time.Second || got[1] != 2*time.Second || got[2] != 30*time.Second || got[3] != 30*time.Second {
		t.Fatalf("backoff steps %v, want 1s, 2s, 30s (capped), 30s", got)
	}
	oldMin, oldMax := reportBackoffMin, reportBackoffMax
	reportBackoffMin, reportBackoffMax = 40*time.Millisecond, 120*time.Millisecond
	t.Cleanup(func() { reportBackoffMin, reportBackoffMax = oldMin, oldMax })
	var mu sync.Mutex
	var at []time.Time
	url, _ := fakeReportsBrain(t, func(es []map[string]any) (int, any) {
		mu.Lock()
		at = append(at, time.Now())
		n := len(at)
		mu.Unlock()
		if n <= 4 {
			return 503, map[string]any{"error": "the brain has no switch key"}
		}
		return 200, resultsAll(es, "applied", 1)
	})
	stop := make(chan struct{})
	var wakers sync.WaitGroup
	wakers.Add(1)
	go func() { // wakes during the backoff must not cut it short
		defer wakers.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
				lan.reportWake()
			}
		}
	}()
	t.Cleanup(func() { close(stop); wakers.Wait() })
	lan.reportHeaderReading("a", reading(time.Now(), map[string][2]*float64{"five_hour": {ptr(1), nil}}))
	runReporterFor(t, url, "k")
	waitUntil(t, "the queue to empty", func() bool { return lan.reportsPending() == 0 })
	mu.Lock()
	defer mu.Unlock()
	if len(at) != 5 {
		t.Fatalf("%d posts, want four failures and one taken", len(at))
	}
	for i, want := range []time.Duration{40, 80, 120, 120} {
		if gap := at[i+1].Sub(at[i]); gap < want*time.Millisecond {
			t.Fatalf("gap %d = %s, want at least %dms", i+1, gap, want)
		}
	}
	if st := lan.reportsStatus(); st["last_error"] != "" {
		t.Fatalf("reports = %+v after the brain took the post", st)
	}
}

// ---- which records ----

// A spent window record is a refusal event carrying by_reading, seen and
// its class; a model's record (spent or not_entitled) and a pool's (spent)
// go as a limit the brain has no window for (67 §9), with window and seen
// null and their clamped until; throttles, money pots, the entitlement and
// an api account's records are not sent.
func TestReportsWhichRecords(t *testing.T) {
	now, _, r7 := recWorld(t)
	wireOf := func(ev reportEvent) (map[string]any, []byte) {
		b, _ := json.Marshal(ev.wire())
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		return m, b
	}
	refuse(t, memGpt, "gpt-a", 429, nil, s9(r7))
	ev, ok := queuedOf(windowRefusalKey("gpt-a", "seven_day"))
	if !ok {
		t.Fatal("the spent window record was not queued")
	}
	m, b := wireOf(ev)
	seen, _ := m["seen"].(map[string]any)
	if m["kind"] != "refusal" || m["account"] != "gpt-a" || m["window"] != "seven_day" || m["read_at"] != unixSeconds(*now) ||
		m["until"] != unixSeconds(r7) || m["resets_at"] != unixSeconds(r7) || m["by_reading"] != true || seen["used_pct"] != 100.0 ||
		seen["resets_at"] != unixSeconds(r7) || m["ended"] != nil || m["class"] != "spent" {
		t.Fatalf("refusal wire = %s", b)
	}
	if l, has := m["limit"]; !has || l != nil {
		t.Fatalf("a window's refusal sends limit null: %s", b)
	}
	clearQueue()
	refuse(t, memGpt, "gpt-a", 429, nil, codexCreditExhausted429) // the credits pot
	refuse(t, memKimi, "kimi", 403, nil, kimiNotEntitled403)      // the entitlement
	lan.recordRefusal("kimi", refusal{class: refThrottled, detail: "slow down"}, hResp(429, nil, ""))
	lan.pref.mu.Lock()
	lan.pref.accounts["rw"] = acctStanding{Provider: "runway", Kind: "api", AsOf: epoch(*now)}
	lan.pref.mu.Unlock()
	lan.recordRefusal("rw", refusal{class: refSpent, limits: []refLimit{{name: modelLimit("gen-4"), kind: limModel}}},
		hResp(429, nil, "")) // Runway's: money
	if n := queueLen(); n != 0 {
		t.Fatalf("%d queued for a pot, the entitlement, a throttle and an api account's model, want none", n)
	}
	// One model, not_entitled: the limit model:<m>, until the 6 h clamp.
	refuse(t, memKimi, "kimi", 403, nil, kimiModel403)
	ev, ok = queuedOf(limitRefusalKey("kimi", modelLimit("kimi-k3")))
	if !ok {
		t.Fatal("the model record was not queued")
	}
	m, b = wireOf(ev)
	if m["window"] != nil || m["limit"] != "model:kimi-k3" || m["class"] != "not_entitled" || m["seen"] != nil ||
		m["by_reading"] != false || m["until"] != unixSeconds(now.Add(quotaCooldownCap)) || m["read_at"] != unixSeconds(*now) {
		t.Fatalf("model refusal wire = %s", b)
	}
	if _, has := m["window"]; !has {
		t.Fatalf("a limit's refusal sends window null: %s", b)
	}
	// The billing-cycle pool, spent: pool:total_quota, the record's key
	// prefixed.
	refuse(t, memKimi, "kimi", 403, nil, kimi403Cycle)
	ev, ok = queuedOf(limitRefusalKey("kimi", "pool:total_quota"))
	if !ok {
		t.Fatal("the pool record was not queued")
	}
	if m, b = wireOf(ev); m["limit"] != "pool:total_quota" || m["class"] != "spent" || m["until"] == nil || m["window"] != nil {
		t.Fatalf("pool refusal wire = %s", b)
	}
	if n := queueLen(); n != 2 {
		t.Fatalf("%d queued, want the model's and the pool's", n)
	}
	// A record only a reading can end carries no until.
	refuse(t, memKimi, "kimi", 403, nil, kimi403FiveHour)
	if ev, ok := queuedOf(windowRefusalKey("kimi", "five_hour")); !ok || ev.until != nil || !ev.byReading || *ev.seen.usedPct != 80 {
		t.Fatalf("kimi five_hour = %+v, want until null, by reading, seen at 80", ev)
	}
	// A 2xx on the model ends its record, and the end is reported as the
	// limit's, at the 2xx; the pool's stands.
	clearQueue()
	lan.noteServed("kimi", "", "kimi-k3", now.Add(time.Minute))
	ev, ok = queuedOf(limitRefusalKey("kimi", modelLimit("kimi-k3")))
	if m, b = wireOf(ev); !ok || m["ended"] != true || m["read_at"] != unixSeconds(now.Add(time.Minute)) ||
		m["limit"] != "model:kimi-k3" || m["class"] != "not_entitled" {
		t.Fatalf("the model's end = %s", b)
	}
	if _, ok := queuedOf(limitRefusalKey("kimi", "pool:total_quota")); ok {
		t.Fatal("a 2xx on a model ended the pool")
	}
}

// A refusal carries the reset it named (resets_at) beside the record's end
// (until): an unattributed record that named a reset 3 days out ends at the
// 6 h clamp, and the brain needs both to publish recordWindow's window, which
// resets at the named reset. One that named none sends resets_at null. An
// ended refusal carries both too.
func TestReportsRefusalCarriesTheNamedReset(t *testing.T) {
	t.Cleanup(resetQuotaState())
	now := time.Now().Truncate(time.Second)
	named := now.Add(72 * time.Hour)
	rec := lan.writeRecord("a", "five_hour", refusalRecord{kind: recSpent, limKind: limWindow, refusedAt: now, resets: named})
	if !rec.until.Equal(now.Add(quotaCooldownCap)) {
		t.Fatalf("fixture: until %v, want the 6 h clamp", rec.until)
	}
	wireOf := func(ev reportEvent) map[string]any {
		b, _ := json.Marshal(ev.wire())
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		return m
	}
	for _, ended := range []bool{false, true} {
		m := wireOf(refusalEvent("a", "five_hour", "", rec, ended, now.Add(time.Minute)))
		if m["until"] != unixSeconds(now.Add(quotaCooldownCap)) || m["resets_at"] != unixSeconds(named) || m["by_reading"] != false {
			t.Fatalf("ended %v: refusal wire = %v, want until the clamp and resets_at the named reset", ended, m)
		}
		if w := recordWindow(rec, checkWindow{}, now); *w.ResetsAt != m["resets_at"] {
			t.Fatalf("recordWindow resets at %v, the wire says %v", *w.ResetsAt, m["resets_at"])
		}
	}
	none := lan.writeRecord("a", "seven_day", refusalRecord{kind: recSpent, limKind: limWindow, refusedAt: now})
	m := wireOf(refusalEvent("a", "seven_day", "", none, false, time.Time{}))
	if r, has := m["resets_at"]; !has || r != nil || m["until"] != unixSeconds(now.Add(quotaCooldown)) {
		t.Fatalf("refusal wire = %v, want resets_at null and until the floor", m)
	}
}

// A 2xx sent after a window record ends it and sends its refusal with ended
// at the 2xx; one sent before it does not; a reading that ends a record
// (pruneRecords) and a paid 2xx on a pot send nothing.
func TestReportsEndedOnServed(t *testing.T) {
	now, _, r7 := recWorld(t)
	refuse(t, memGpt, "gpt-a", 429, nil, s9(r7))
	refuse(t, memGpt, "gpt-a", 429, nil, codexCreditExhausted429)
	clearQueue()
	lan.noteServed("gpt-a", "", "", now.Add(-time.Second))
	if n := queueLen(); n != 0 {
		t.Fatalf("a 2xx sent before the refusal queued %d", n)
	}
	lan.notePaidServed("gpt-a", potCredits, now.Add(time.Second))
	if n := queueLen(); n != 0 {
		t.Fatalf("a paid 2xx on the pot queued %d", n)
	}
	lan.noteServed("gpt-a", "", "", now.Add(2*time.Second))
	ev, ok := queuedOf(windowRefusalKey("gpt-a", "seven_day"))
	if !ok || !ev.ended || ev.readAt != unixSeconds(now.Add(2*time.Second)) {
		t.Fatalf("queued %+v, want the record's end at the 2xx", ev)
	}
	if w := ev.wire(); w["ended"] != true || w["window"] != "seven_day" {
		t.Fatalf("ended wire = %v", w)
	}
	// pruneRecords: kimi's five_hour, ended by a newer reading at lower use.
	refuse(t, memKimi, "kimi", 403, nil, kimi403FiveHour)
	clearQueue()
	reread("kimi", now.Add(time.Minute), setWindow("five_hour", 10, nil))
	if _, ok := recOf("kimi", "five_hour"); ok {
		t.Fatal("fixture: the reading did not end the record")
	}
	if n := queueLen(); n != 0 {
		t.Fatalf("a record a reading ended queued %d", n)
	}
}

// ---- resync ----

// The resync queues every live record the brain takes (a window's, a
// model's, a pool's) and the latest reading of each window and pot,
// unfiltered; nothing for other records or expired ones.
func TestReportsResync(t *testing.T) {
	now, _, r7 := recWorld(t)
	refuse(t, memGpt, "gpt-a", 429, nil, s9(r7))
	refuse(t, memGpt, "gpt-a", 429, nil, codexCreditExhausted429)
	refuse(t, memKimi, "kimi", 403, nil, kimiModel403) // a model's: a limit the brain has no window for
	lan.recordThrottle("kimi", 429, "slow", nil)
	lan.recMu.Lock()
	lan.records[recKey{"kimi", "five_hour"}] = refusalRecord{kind: recSpent, limKind: limWindow,
		refusedAt: now.Add(-time.Hour), until: now.Add(-time.Minute)} // expired
	lan.recMu.Unlock()
	// News: five_hour moved from the brain's 40, and the credits pot shut.
	lan.noteHeaderReading("gpt-a", http.Header{
		"X-Codex-Primary-Used-Percent": {"45"}, "X-Codex-Primary-Window-Minutes": {"300"},
		"X-Codex-Credits-Has-Credits": {"False"}, "X-Codex-Credits-Unlimited": {"False"},
	}, *now)
	// A later response that carries only the weekly window, at the brain's
	// own 100: not news, never sent, so the resync does not send it either;
	// five_hour's last report still goes.
	lan.noteHeaderReading("gpt-a", http.Header{"X-Codex-Secondary-Used-Percent": {"100"}, "X-Codex-Secondary-Window-Minutes": {"10080"}}, now.Add(time.Second))
	clearQueue()
	lan.reportResync()
	if ev, ok := queuedOf(readingKey("gpt-a", "five_hour")); !ok || *ev.usedPct != 45 {
		t.Fatalf("resync's five_hour %v, want the reported 45", ev.wire())
	}
	want := []reportKey{windowRefusalKey("gpt-a", "seven_day"), limitRefusalKey("kimi", modelLimit("kimi-k3")),
		readingKey("gpt-a", "five_hour"),
		{kind: reportReading, account: "gpt-a", pot: potCredits}}
	for _, k := range want {
		if _, ok := queuedOf(k); !ok {
			t.Errorf("resync did not queue %+v", k)
		}
	}
	if n := queueLen(); n != len(want) {
		t.Fatalf("resync queued %d, want %d", n, len(want))
	}
}

// The resync runs on each stream connection's first event, and only there.
func TestReportsResyncOnEachStreamConnection(t *testing.T) {
	start, second, drop := make(chan struct{}), make(chan struct{}), make(chan struct{})
	conns := streamBrain(t, time.Minute, 10*time.Millisecond, func(w http.ResponseWriter, r *http.Request, n int) {
		if n == 1 {
			select {
			case <-start:
			case <-r.Context().Done():
				return
			}
		}
		sseOpen(w)
		ssePreferred(t, w, 1, brainPreferred(t))
		if n == 1 {
			select {
			case <-second:
			case <-r.Context().Done():
				return
			}
			d := brainPreferred(t)
			d["account"] = "second"
			ssePreferred(t, w, 2, d)
			select {
			case <-drop:
			case <-r.Context().Done():
			}
			return
		}
		<-r.Context().Done()
	})
	now := time.Now()
	lan.recMu.Lock()
	lan.records[recKey{"acct-resync", "five_hour"}] = refusalRecord{kind: recSpent, limKind: limWindow, refusedAt: now, attributed: true}
	lan.recMu.Unlock()
	lan.reportHeaderReading("acct-resync", reading(now, map[string][2]*float64{"five_hour": {ptr(100), nil}}))
	clearQueue()
	close(start)
	waitUntil(t, "the first connection's resync", func() bool { return queueLen() == 2 })
	clearQueue()
	close(second)
	waitUntil(t, "the second document", func() bool { _, a, _ := prefState(); return a == "second" })
	if n := queueLen(); n != 0 {
		t.Fatalf("a connection's second event queued %d", n)
	}
	close(drop)
	waitUntil(t, "the reconnect's resync", func() bool { return conns.Load() >= 2 && queueLen() == 2 })
}

// ---- end to end ----

// Against the e2e fake brain: a 429 whose headers reject a window arrives as
// a `refusal`, a 2xx with usage headers as a `reading`, and the status block
// says nothing waits. (A paid codex serve arriving as `ledger`:
// TestE2EPaidCodexServeIsLedgered.)
func TestE2EReportsReachTheBrain(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	env.capturedFable429(t, other)
	env.deny429.Store(e2eToken(other))
	resp := env.post(t, "claude-fable-5", "CLIENT")
	resp.Body.Close()
	env.hit(t) // 429 on other
	env.hit(t) // served on native
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want the failover's 200", resp.StatusCode)
	}
	reportsSettle()
	find := func(kind, account, window string) map[string]any {
		env.brainMu.Lock()
		defer env.brainMu.Unlock()
		for _, e := range env.reports {
			if e["kind"] == kind && e["account"] == account && e["window"] == window {
				return e
			}
		}
		return nil
	}
	ref := find("refusal", other, "scoped:Fable")
	if ref == nil {
		env.brainMu.Lock()
		t.Fatalf("no refusal of %s scoped:Fable among %v", other, env.reports)
	}
	_, hasR := ref["resets_at"]
	if _, has := ref["until"]; !has || !hasR || ref["by_reading"] == nil || ref["seen"] == nil || ref["read_at"] == nil {
		t.Fatalf("refusal = %v", ref)
	}

	reset := time.Now().Add(2 * time.Hour).Truncate(time.Second)
	env.deny429.Store("")
	env.anthHeadersFor.Store(map[string]map[string]string{})
	env.anthHeaders.Store(map[string]string{
		"anthropic-ratelimit-unified-status":         "allowed",
		"anthropic-ratelimit-unified-5h-status":      "allowed",
		"anthropic-ratelimit-unified-5h-utilization": "0.33",
		"anthropic-ratelimit-unified-5h-reset":       strconv.FormatInt(reset.Unix(), 10),
	})
	resp = env.post(t, "claude-haiku-4-5", "CLIENT")
	resp.Body.Close()
	h := env.hit(t)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	account := native
	if h.auth == "Bearer "+e2eToken(other) {
		account = other
	}
	reportsSettle()
	rd := find("reading", account, "five_hour")
	if rd == nil || rd["used_pct"] != 33.0 || rd["resets_at"] != float64(reset.Unix()) {
		env.brainMu.Lock()
		t.Fatalf("reading of %s five_hour = %v among %v", account, rd, env.reports)
	}

	var s struct {
		Reports map[string]any `json:"reports"`
	}
	if err := json.Unmarshal(statusMine(t, env.proxy.URL), &s); err != nil {
		t.Fatal(err)
	}
	if s.Reports["pending"] != 0.0 || s.Reports["last_error"] != "" {
		t.Fatalf("status reports = %v", s.Reports)
	}
}

// A hold republishes a refused window at 100 under the as_of of the report
// the brain applied (its read_at). Judged against the last report on that
// tie, the same reading again and again is not news; only a later poll's
// newer as_of could make one.
func TestReportsHoldUnderTheReportsAsOfIsNotNews(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	reset := now.Add(3 * time.Hour)
	seedQuota(t, map[string]acctStanding{"a": standingAt("anthropic", now.Add(-time.Minute),
		map[string]checkWindow{"five_hour": fiveHour(40, reset)}, "")}, nil, nil, nil)
	lan.reportHeaderReading("a", reading(now, map[string][2]*float64{"five_hour": {ptr(97), epoch(reset)}}))
	if _, ok := queuedOf(readingKey("a", "five_hour")); !ok {
		t.Fatal("the first reading was not news")
	}
	clearQueue()
	// The brain applied it, then a hold: 100 under the report's read_at.
	lan.pref.mu.Lock()
	lan.pref.accounts = map[string]acctStanding{"a": standingAt("anthropic", now,
		map[string]checkWindow{"five_hour": fiveHour(100, reset)}, "")}
	lan.pref.mu.Unlock()
	for i := 1; i <= 5; i++ {
		lan.reportHeaderReading("a", reading(now.Add(time.Duration(i)*time.Second), map[string][2]*float64{"five_hour": {ptr(97), epoch(reset)}}))
		if ev, ok := queuedOf(readingKey("a", "five_hour")); ok {
			t.Fatalf("reading %d under the hold queued %v, want nothing", i, ev.wire())
		}
	}
}

// ParseFloat takes NaN and Inf; no header reading may carry one, so none
// reaches the queue, where it could not be encoded.
func TestReportsNonFiniteHeaderValuesAreRefused(t *testing.T) {
	t.Cleanup(resetQuotaState())
	now := time.Now().Truncate(time.Second)
	for _, v := range []string{"NaN", "nan", "Inf", "+Inf", "-Inf", "infinity"} {
		if f, ok := parseFiniteFloat(v); ok {
			t.Fatalf("parseFiniteFloat(%q) = %v, want refused", v, f)
		}
		if r := parseResetValue(v, now); !r.IsZero() {
			t.Fatalf("parseResetValue(%q) = %v, want none", v, r)
		}
	}
	if f, ok := parseFiniteFloat("12.5"); !ok || f != 12.5 {
		t.Fatalf("parseFiniteFloat(12.5) = %v %v", f, ok)
	}
	finite := func(p *float64) bool { return p == nil || (!math.IsNaN(*p) && !math.IsInf(*p, 0)) }

	lan.noteHeaderReading("gpt-a", http.Header{
		"X-Codex-Primary-Used-Percent":     {"NaN"},
		"X-Codex-Primary-Window-Minutes":   {"300"},
		"X-Codex-Secondary-Used-Percent":   {"20"},
		"X-Codex-Secondary-Window-Minutes": {"10080"},
		"X-Codex-Secondary-Reset-At":       {"+Inf"},
		"X-Codex-Credits-Has-Credits":      {"True"},
		"X-Codex-Credits-Balance":          {"Inf"},
	}, now)
	lan.noteHeaderReading("anth-a", rlHeader(
		"anthropic-ratelimit-unified-5h-status", "allowed",
		"anthropic-ratelimit-unified-5h-utilization", "+Inf",
		"anthropic-ratelimit-unified-5h-reset", "Inf",
		"anthropic-ratelimit-unified-7d-status", "allowed",
		"anthropic-ratelimit-unified-7d-utilization", "0.4",
		"anthropic-ratelimit-unified-7d-reset", "NaN"), now)

	lan.recMu.Lock()
	for acct, hr := range lan.readings {
		for n, w := range hr.windows {
			if !finite(w.UsedPct) || !finite(w.ResetsAt) {
				lan.recMu.Unlock()
				t.Fatalf("%s %s kept a non-finite reading: used %v reset %v", acct, n, *w.UsedPct, *w.ResetsAt)
			}
		}
		if !finite(hr.balance) {
			lan.recMu.Unlock()
			t.Fatalf("%s kept a non-finite balance", acct)
		}
	}
	lan.recMu.Unlock()
	if ev, ok := queuedOf(readingKey("gpt-a", "seven_day")); !ok || *ev.usedPct != 20 || ev.resetsAt != nil {
		t.Fatalf("codex seven_day queued %v, want its use and no reset", ev.wire())
	}
	if ev, ok := queuedOf(readingKey("anth-a", "seven_day")); !ok || *ev.usedPct != 40 || ev.resetsAt != nil {
		t.Fatalf("anthropic seven_day queued %v, want its use and no reset", ev.wire())
	}
	lan.repMu.Lock()
	for k, q := range lan.repQueue {
		if _, err := json.Marshal(q.ev.wire()); err != nil {
			lan.repMu.Unlock()
			t.Fatalf("%v queued unencodable: %v", k, err)
		}
	}
	lan.repMu.Unlock()
}

// An event that cannot be encoded leaves the post with a log line, the rest
// still go, and a post of nothing is never sent: a queued event is dropped,
// a ledger entry is counted here and never posted.
func TestReportsUnencodableEventIsDropped(t *testing.T) {
	t.Cleanup(resetQuotaState())
	now := time.Now()
	bad := math.NaN()
	lan.repMu.Lock()
	lan.queueReportLocked(readingKey("a", "five_hour"), reportEvent{kind: reportReading, account: "a",
		window: "five_hour", readAt: unixSeconds(now), usedPct: &bad})
	lan.repMu.Unlock()
	lan.reportHeaderReading("a", reading(now, map[string][2]*float64{"seven_day": {ptr(10), nil}}))
	basis := "derived"
	lan.ledgerEnter(ledgerEntry{ID: newLedgerID(), At: unixSeconds(now), Account: "gpt", Pot: potCredits,
		Route: gptRoute, Charge: &bad, Unit: "codex_credit", Basis: &basis})
	enter(0.1, now)

	url, posts := fakeReportsBrain(t, func(es []map[string]any) (int, any) { return 200, resultsAll(es, "applied", 3) })
	if ok, more := lan.reportPost(context.Background(), tbase(url), "k"); !ok || more {
		t.Fatalf("ok %v more %v, want the rest taken and nothing left", ok, more)
	}
	p := posts()
	if len(p) != 1 || len(p[0]) != 2 {
		t.Fatalf("posts = %v, want one of the two encodable events", p)
	}
	if p[0][0]["window"] != "seven_day" || p[0][1]["charge"] != 0.1 {
		t.Fatalf("post = %v", p[0])
	}
	if st := lan.ledgerStatus(); st["pending"] != 0 || st["refused"] != 1 {
		t.Fatalf("ledger = %+v, want the unencodable entry counted as refused", st)
	}
	if st := lan.reportsStatus(); st["pending"] != 0 {
		t.Fatalf("reports = %+v, want nothing pending", st)
	}
	// Only unencodable events: nothing is posted, and the drop is said.
	lan.repMu.Lock()
	lan.queueReportLocked(readingKey("a", "five_hour"), reportEvent{kind: reportReading, account: "a",
		window: "five_hour", readAt: unixSeconds(now.Add(time.Second)), usedPct: &bad})
	lan.repMu.Unlock()
	if ok, more := lan.reportPost(context.Background(), tbase(url), "k"); !ok || more {
		t.Fatalf("ok %v more %v, want nothing left", ok, more)
	}
	if n := len(posts()); n != 1 {
		t.Fatalf("%d posts, want no empty one", n)
	}
	if st := lan.reportsStatus(); st["pending"] != 0 || st["last_error"] == "" {
		t.Fatalf("reports = %+v, want nothing pending and the drop said", st)
	}
}

// A header's fraction becomes a percent without float noise: 0.07 reads as
// 7, not 7.000000000000001, so a reading equal to the brain's poll is not
// news after every poll.
func TestReportsHeaderPercentHasNoFloatNoise(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	reset := now.Add(2 * time.Hour)
	seedQuota(t, map[string]acctStanding{"a": standingAt("anthropic", now.Add(-time.Minute),
		map[string]checkWindow{"five_hour": fiveHour(7, reset)}, "")}, nil, nil, nil)
	for _, frac := range []string{"0.07", "0.29", "0.57", "0.58"} {
		obs, ok := parseUnifiedRL(rlHeader("anthropic-ratelimit-unified-5h-status", "allowed",
			"anthropic-ratelimit-unified-5h-utilization", frac), now, lan.currentScopes())
		f, _ := strconv.ParseFloat(frac, 64)
		if want := math.Round(f * 100); !ok || len(obs.windows) != 1 || obs.windows[0].pct != want {
			t.Fatalf("%s read as %v, want %v", frac, obs.windows, want)
		}
	}
	lan.noteHeaderReading("a", rlHeader(
		"anthropic-ratelimit-unified-5h-status", "allowed",
		"anthropic-ratelimit-unified-5h-utilization", "0.07",
		"anthropic-ratelimit-unified-5h-reset", strconv.FormatInt(reset.Unix(), 10)), now)
	if ev, ok := queuedOf(readingKey("a", "five_hour")); ok {
		t.Fatalf("the brain's own 7%% queued %v", ev.wire())
	}
}

// The overlay keeps the brain's use against a response a point below it, in
// the same period, and takes the response's age (useAgrees): the poll reads
// 81 while the headers still read 80 for the upper half of every point. A
// response above it, further below, or of another period is newer use.
func TestOverlayKeepsTheBrainsUseAgainstAPointBelow(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	reset := now.Add(50 * time.Hour)
	seedQuota(t, map[string]acctStanding{"a": standingAt("anthropic", now.Add(-time.Minute),
		map[string]checkWindow{"seven_day": {UsedPct: ptr(81), ResetsAt: epoch(reset), Period: "7d"}}, "")}, nil, nil, nil)
	cases := []struct {
		name  string
		util  string
		reset time.Time
		want  float64
	}{
		{"a point below", "0.8", reset, 81},
		{"the same", "0.81", reset, 81},
		{"above", "0.82", reset, 82},
		{"more than a point below", "0.79", reset, 79},
		{"a point below, of a later period", "0.8", reset.Add(7 * 24 * time.Hour), 80},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			at := now.Add(time.Duration(i+1) * time.Second)
			lan.noteHeaderReading("a", rlHeader(
				"anthropic-ratelimit-unified-7d-status", "allowed_warning",
				"anthropic-ratelimit-unified-7d-utilization", c.util,
				"anthropic-ratelimit-unified-7d-reset", strconv.FormatInt(c.reset.Unix(), 10)), at)
			w := lan.standingRead("a", at).Allowance.Windows["seven_day"]
			if *w.UsedPct != c.want || *w.AsOf != unixSeconds(at) {
				t.Fatalf("seven_day = use %v as_of %v, want %v as of the response", *w.UsedPct, *w.AsOf, c.want)
			}
		})
	}
}

// A header reading with no use (a status and a reset, no utilization) never
// adds a window the brain's standing lacks: one of null use and no as_of
// reads as stale and would close every route on the account until the next
// poll. Of a window the standing has, it moves only the reset. The reporter
// does not send it for a window the brain lacks.
func TestReadingWithNoUseNeverAddsAWindow(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	reset := now.Add(2 * time.Hour)
	seedQuota(t, map[string]acctStanding{"a": standingAt("anthropic", now.Add(-time.Minute),
		map[string]checkWindow{"five_hour": fiveHour(30, reset)}, "")}, nil, nil, nil)
	newReset := now.Add(3 * time.Hour)
	lan.noteHeaderReading("a", rlHeader(
		"anthropic-ratelimit-unified-5h-status", "allowed",
		"anthropic-ratelimit-unified-5h-reset", strconv.FormatInt(newReset.Unix(), 10),
		"anthropic-ratelimit-unified-7d-status", "allowed",
		"anthropic-ratelimit-unified-7d-reset", strconv.FormatInt(now.Add(90*time.Hour).Unix(), 10)), now)
	lan.recMu.Lock()
	_, kept7 := lan.readings["a"].windows["seven_day"]
	lan.recMu.Unlock()
	if !kept7 {
		t.Fatal("fixture: the response's seven_day was not read")
	}
	st := lan.standingRead("a", now)
	if _, has := st.Allowance.Windows["seven_day"]; has {
		t.Fatalf("a reading with no use added seven_day: %+v", st.Allowance.Windows["seven_day"])
	}
	w := st.Allowance.Windows["five_hour"]
	if *w.UsedPct != 30 || *w.AsOf != unixSeconds(now.Add(-time.Minute)) || *w.ResetsAt != unixSeconds(newReset) {
		t.Fatalf("five_hour = use %v as_of %v reset %v, want the brain's use and age, the new reset", *w.UsedPct, *w.AsOf, *w.ResetsAt)
	}
	if _, ok := queuedOf(readingKey("a", "seven_day")); ok {
		t.Fatal("a reading with no use of a window the brain lacks was queued")
	}
	if _, ok := queuedOf(readingKey("a", "five_hour")); !ok {
		t.Fatal("the five_hour reset moved past the slack was not queued")
	}
}

// The proxy's half of the brain's TestHoldAgainstAPointBelow (quota-model 64
// §5 "Only news"; 10 "Rounding": the refusal's header reads 1.00, every 2xx
// at the window's last point 0.99). A refusal at 100 on a window the brain
// read at 98, then 2xx at 0.99 answered after it (requests sent before it:
// one sent after would end the record itself, noteServed). The 0.99 agrees
// with the 100 the proxy reported, so it changes no standing and is not news;
// at stale_max/2 it goes to the brain as a fresh reading, stamped when it
// arrived; the brain's hold stands on it, and the record stands on what the
// brain then publishes. The resync re-sends that newest reading with its
// own read_at, so a brain restarted during the hold gets the refusal (seen
// 100) and the 99 with no 100 held (64 §8). A point below the 100 seen, in
// the same period, is the seen reading again (useAgrees), so that brain
// keeps its hold, and this record holds even on a published 99 by the same
// rule (records_golden.json, "a 99 read after a refusal seen at 100").
func TestHoldAgainstAPointBelow(t *testing.T) {
	now, _, r7 := recWorld(t)
	reread("a", now.Add(-time.Minute), setWindow("seven_day", 98, epoch(r7)))
	lan.pref.mu.RLock()
	staleMax := lan.pref.staleMax
	lan.pref.mu.RUnlock()
	if staleMax <= 0 {
		t.Fatalf("fixture: stale_max %v", staleMax)
	}
	refused := *now
	r7s := strconv.FormatInt(r7.Unix(), 10)
	clearQueue()
	refuse(t, memAnth, "a", 429, map[string]string{
		"anthropic-ratelimit-unified-status":               "rejected",
		"anthropic-ratelimit-unified-representative-claim": "seven_day",
		"anthropic-ratelimit-unified-reset":                r7s,
		"anthropic-ratelimit-unified-7d-status":            "rejected",
		"anthropic-ratelimit-unified-7d-reset":             r7s,
		"anthropic-ratelimit-unified-7d-utilization":       "1.0",
	}, `{"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later."}}`)
	rec, ok := recOf("a", "seven_day")
	if !ok || rec.seen.usedPct == nil || *rec.seen.usedPct != 100 {
		t.Fatalf("record %+v, want one seen at the refusal's 100", rec)
	}
	ev, ok := queuedOf(readingKey("a", "seven_day"))
	if !ok || *ev.usedPct != 100 || ev.readAt != unixSeconds(refused) {
		t.Fatalf("the refusal's 1.00 queued %v, want news at 100 read at the refusal", ev.wire())
	}
	if ev, ok := queuedOf(windowRefusalKey("a", "seven_day")); !ok || *ev.seen.usedPct != 100 {
		t.Fatalf("refusal queued %v, want seen 100", ev.wire())
	}
	// The brain applies both and publishes its hold: 100 under the refusal's as_of.
	publish := func(used float64, asOf time.Time) {
		lan.pref.mu.Lock()
		st := lan.pref.accounts["a"]
		ws := make(map[string]checkWindow, len(st.Allowance.Windows))
		for n, w := range st.Allowance.Windows {
			ws[n] = w
		}
		w := ws["seven_day"]
		w.UsedPct, w.AsOf = ptr(used), epoch(asOf)
		ws["seven_day"] = w
		st.Allowance.Windows = ws
		lan.pref.accounts["a"] = st
		accounts := lan.pref.accounts
		lan.pref.mu.Unlock()
		lan.pruneRecords(accounts)
	}
	publish(100, refused)
	clearQueue()

	ok99 := func(at time.Time) {
		t.Helper()
		*now = at
		lan.noteHeaderReading("a", rlHeader(
			"anthropic-ratelimit-unified-status", "allowed_warning",
			"anthropic-ratelimit-unified-7d-status", "allowed_warning",
			"anthropic-ratelimit-unified-7d-utilization", "0.99",
			"anthropic-ratelimit-unified-7d-reset", r7s), at)
	}
	blocked := func(at time.Time) {
		t.Helper()
		if _, ok := recOf("a", "seven_day"); !ok {
			t.Fatal("the record ended")
		}
		if w := lan.standingRead("a", at).Allowance.Windows["seven_day"]; *w.UsedPct != 100 || *w.AsOf != unixSeconds(at) {
			t.Fatalf("overlay seven_day = %v as of %v, want 100 as of the 2xx", *w.UsedPct, *w.AsOf)
		}
		if w := lan.standingFor("a", at).Allowance.Windows["seven_day"]; *w.UsedPct != 100 {
			t.Fatalf("standing seven_day = %v, want 100", *w.UsedPct)
		}
	}
	early := refused.Add(time.Second)
	ok99(early)
	if ev, ok := queuedOf(readingKey("a", "seven_day")); ok {
		t.Fatalf("a 0.99 against the reported 100 queued %v, want nothing", ev.wire())
	}
	blocked(early)

	refresh := refused.Add(time.Duration(staleMax / 2 * float64(time.Second)))
	ok99(refresh)
	ev, ok = queuedOf(readingKey("a", "seven_day"))
	if !ok || *ev.usedPct != 99 || ev.readAt != unixSeconds(refresh) {
		t.Fatalf("at stale_max/2 queued %v (ok %v), want 99 read at %v", ev.wire(), ok, unixSeconds(refresh))
	}
	// The brain keeps its 100 and moves the age (TestHoldAgainstAPointBelow).
	publish(100, refresh)
	blocked(refresh)

	*now = refresh.Add(time.Minute)
	clearQueue()
	lan.reportResync()
	ev, ok = queuedOf(readingKey("a", "seven_day"))
	if !ok || *ev.usedPct != 99 || ev.readAt != unixSeconds(refresh) {
		t.Fatalf("resync queued %v, want the newest reading, 99, at its own read_at", ev.wire())
	}
	// The restarted brain's standing read after the refusal at 99, in the
	// refused period: a point below the 100 seen is no change, so the record
	// holds and the window it makes stays at 100.
	restarted := refresh.Add(2 * time.Minute)
	*now = restarted
	publish(99, restarted)
	if _, ok := recOf("a", "seven_day"); !ok {
		t.Fatal("a 99 published after the refusal ended the record seen at 100")
	}
	if w := lan.standingFor("a", restarted).Allowance.Windows["seven_day"]; *w.UsedPct != 100 {
		t.Fatalf("standing seven_day = %v, want the record's 100", *w.UsedPct)
	}
}

// The restart edge (64 §8): a refusal's own 1.00 is reported at 100, and a 99
// read after it, a point below, is not news. A brain that restarts during the
// hold gets the 100 again on the resync, not the 99 it was never sent. Its
// hold would stand on the 99 too (windowChanged, TestHoldAgainstAPointBelow),
// but the resync re-sends what the brain was told.
func TestReportsResyncSendsTheLastReportedReading(t *testing.T) {
	now, _, r7 := recWorld(t)
	reread("a", now.Add(-time.Minute), setWindow("seven_day", 98, epoch(r7)))
	refused := *now
	r7s := strconv.FormatInt(r7.Unix(), 10)
	clearQueue()
	refuse(t, memAnth, "a", 429, map[string]string{
		"anthropic-ratelimit-unified-status":               "rejected",
		"anthropic-ratelimit-unified-representative-claim": "seven_day",
		"anthropic-ratelimit-unified-reset":                r7s,
		"anthropic-ratelimit-unified-7d-status":            "rejected",
		"anthropic-ratelimit-unified-7d-reset":             r7s,
		"anthropic-ratelimit-unified-7d-utilization":       "1.0",
	}, `{"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later."}}`)
	if ev, ok := queuedOf(readingKey("a", "seven_day")); !ok || *ev.usedPct != 100 {
		t.Fatalf("the refusal's 1.00 queued %v, want news at 100", ev.wire())
	}
	clearQueue()
	later := refused.Add(time.Second)
	*now = later
	lan.noteHeaderReading("a", rlHeader(
		"anthropic-ratelimit-unified-status", "allowed_warning",
		"anthropic-ratelimit-unified-7d-status", "allowed_warning",
		"anthropic-ratelimit-unified-7d-utilization", "0.99",
		"anthropic-ratelimit-unified-7d-reset", r7s), later)
	if ev, ok := queuedOf(readingKey("a", "seven_day")); ok {
		t.Fatalf("the 0.99 a point below queued %v, want nothing", ev.wire())
	}
	lan.reportResync()
	ev, ok := queuedOf(readingKey("a", "seven_day"))
	if !ok || *ev.usedPct != 100 || ev.readAt != unixSeconds(refused) {
		t.Fatalf("resync queued %v (ok %v), want the reported 100 read at the refusal", ev.wire(), ok)
	}
	if ev, ok := queuedOf(windowRefusalKey("a", "seven_day")); !ok || *ev.seen.usedPct != 100 {
		t.Fatalf("resync's refusal %v, want the live record", ev.wire())
	}
}

// records_golden.json's refusal events (64 §5, 67 §9): every applied case
// that names the proxy's record is what this encoder sends for it, key for
// key and value for value; the brain's TestRefusalsGolden posts the same
// events. Cases with no record are earlier proxies' shapes, which this one
// no longer sends.
func TestReportsRefusalsGolden(t *testing.T) {
	t.Cleanup(resetQuotaState())
	raw, err := os.ReadFile("testdata/records_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Refusals struct {
			Applied []struct {
				Name   string         `json:"name"`
				Event  map[string]any `json:"event"`
				Record *struct {
					Account    string   `json:"account"`
					Limit      string   `json:"limit"`
					LimKind    string   `json:"lim_kind"`
					Class      string   `json:"class"`
					RefusedAt  float64  `json:"refused_at"`
					Until      *float64 `json:"until"`
					Resets     *float64 `json:"resets"`
					Attributed bool     `json:"attributed"`
					Seen       struct {
						UsedPct  *float64 `json:"used_pct"`
						ResetsAt *float64 `json:"resets_at"`
					} `json:"seen"`
					EndedAt *float64 `json:"ended_at"`
				} `json:"record"`
			} `json:"applied"`
		} `json:"refusals"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	at := func(f *float64) time.Time {
		if f == nil {
			return time.Time{}
		}
		return time.Unix(0, int64(*f*1e9))
	}
	encoded := map[string]bool{}
	for _, c := range doc.Refusals.Applied {
		if c.Record == nil {
			continue
		}
		rc := c.Record
		rec := refusalRecord{kind: rc.Class, limKind: rc.LimKind, refusedAt: at(&rc.RefusedAt), until: at(rc.Until),
			resets: at(rc.Resets), attributed: rc.Attributed,
			seen: limitSeen{usedPct: rc.Seen.UsedPct, resetsAt: rc.Seen.ResetsAt}}
		window, lim, ok := lan.reportedAs(rc.Account, rc.Limit, rec)
		if !ok {
			t.Errorf("%s: the record is not reported", c.Name)
			continue
		}
		ev := refusalEvent(rc.Account, window, lim, rec, rc.EndedAt != nil, at(rc.EndedAt))
		ev.id = c.Event["id"].(string)
		b, _ := json.Marshal(ev.wire())
		var got map[string]any
		_ = json.Unmarshal(b, &got)
		if !reflect.DeepEqual(got, c.Event) {
			want, _ := json.Marshal(c.Event)
			t.Errorf("%s:\n got %s\nwant %s", c.Name, b, want)
		}
		encoded[c.Name] = true
	}
	for _, name := range []string{"a model limit", "a pool limit", "a not_entitled model", "a model limit ended",
		"a window refusal with class spent and limit null"} {
		if !encoded[name] {
			t.Errorf("the golden has no encoded case %q", name)
		}
	}
}

// potShutKey is an api account's money pot reading's key.
func potShutKey(account, pot string) reportKey {
	return reportKey{kind: reportReading, account: account, pot: pot}
}

// wantPotShut fails unless account's pot shut is queued, read at at.
func wantPotShut(t *testing.T, account, pot string, at time.Time) {
	t.Helper()
	ev, ok := queuedOf(potShutKey(account, pot))
	if !ok {
		t.Fatalf("%s/%s: no pot shut queued", account, pot)
	}
	b, _ := json.Marshal(ev.wire())
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if m["kind"] != "reading" || m["account"] != account || m["pot"] != pot || m["open"] != false ||
		m["balance"] != nil || m["read_at"] != unixSeconds(at) {
		t.Fatalf("pot shut wire = %s", b)
	}
}

// An unpaid refusal on an api account's money pot (Runway's images, through
// the class walk) is reported as that pot shut, a reading the brain's prepaid
// notices read dry; the resync sends it again. A subscription's credits pot,
// another pot of the api account, or a refusal that is not money, sends none.
func TestReportsPotShutOnAnApiMoneyRefusal(t *testing.T) {
	now, _, _ := recWorld(t)
	lan.pref.mu.Lock()
	lan.pref.accounts["acme"] = acctStanding{Provider: "acme", Kind: "api", AsOf: epoch(*now),
		Money: map[string]moneyPot{"balance": {}}}
	lan.pref.mu.Unlock()
	lan.recordRefusal("acme", refusal{class: refSpent, limits: []refLimit{{name: modelLimit("m"), kind: limModel}}},
		hResp(429, nil, ""))
	lan.recordRefusal("acme", refusal{class: refUnpaid, limits: []refLimit{{name: "other", kind: limPot}}},
		hResp(402, nil, ""))
	refuse(t, memGpt, "gpt-a", 429, nil, codexCreditExhausted429) // a subscription's credits: its headers report it
	if n := queueLen(); n != 0 {
		t.Fatalf("%d queued for a model, another pot and a subscription's credits, want none", n)
	}
	lan.recordRefusal("acme", refusal{class: refUnpaid, limits: []refLimit{{name: "balance", kind: limPot, readable: true}}},
		hResp(402, nil, ""))
	wantPotShut(t, "acme", "balance", *now)
	clearQueue()
	lan.reportResync()
	wantPotShut(t, "acme", "balance", *now)
	if lan.apiMoneyPot("gpt-a") != "" || lan.apiMoneyPot("nobody") != "" || lan.apiMoneyPot("acme") != "balance" {
		t.Fatal("apiMoneyPot names only an api account's one money pot")
	}
}
