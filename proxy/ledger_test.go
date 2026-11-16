package main

// The ledger's entries and the budget gate on this proxy's side (quota-model
// 50 §1.7, C6): the charge at the brain's published rates, the translator
// settling a paid dial's entry, its report to the brain, and the gate counting
// every entry of ours the brain has not.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// codexRates are the brain's published codex credit rates (ledger.rates).
func codexRates() *ledgerRates {
	var r ledgerRates
	b, _ := json.Marshal(e2eLedger(time.Now(), 0, nil)["rates"].(map[string]any)["codex_credit"])
	if err := json.Unmarshal(b, &r); err != nil {
		panic(err)
	}
	return &r
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestCodexChargeAtThePublishedRates(t *testing.T) {
	r := codexRates()
	// 1000 in, 400 of them cached, 200 out on Sol: (600×50 + 400×2.5 + 200×250) / 1M.
	if c, basis, why := codexCharge(r, "gpt-6.1-sol", "standard", 1000, 400, 200); why != "" || basis != "derived" || !near(c, 0.081) {
		t.Fatalf("sol = %v %s %q, want 0.081 derived", c, basis, why)
	}
	// A model with no rate is charged at the highest of each kind: a budget
	// may overcount, never undercount.
	if c, basis, _ := codexCharge(r, "gpt-5.6", "standard", 1000, 400, 200); basis != "upper_bound" || !near(c, 0.41) {
		t.Fatalf("unrated model = %v %s, want 0.41 upper_bound", c, basis)
	}
	// The speed weight multiplies; a speed with no weight takes the heaviest.
	if c, _, _ := codexCharge(r, "gpt-6.1-sol", "fast", 1000, 400, 200); !near(c, 0.162) {
		t.Fatalf("fast = %v, want 2×", c)
	}
	if c, basis, _ := codexCharge(r, "gpt-6.1-sol", "warp", 1000, 400, 200); basis != "upper_bound" || !near(c, 0.486) {
		t.Fatalf("unknown speed = %v %s, want 6× upper_bound", c, basis)
	}
	// A details block claiming more cached than input charges no negative input.
	if c, _, _ := codexCharge(r, "gpt-6.1-sol", "standard", 10, 50, 0); !near(c, 50*2.5/1e6) {
		t.Fatalf("over-cached = %v", c)
	}
	// No rates published: the charge cannot be made, and says why.
	if _, _, why := codexCharge(nil, "gpt-6.1-sol", "standard", 1, 0, 1); why == "" {
		t.Fatal("no rates: want a reason")
	}
}

// paidGptWorld is the gpt account spent with credits open, on a route that may
// pay them, with the money document's settings for gpt/credits and the
// brain's ledger as given.
func paidGptWorld(t *testing.T, set potSettings, l *checkLedger) {
	t.Helper()
	now := time.Now()
	f := false
	st := standingAt("gpt", now.Add(-time.Minute), map[string]checkWindow{
		"seven_day": week(100, now.Add(72*time.Hour))}, "open")
	st.Allowance.Open = &f
	r := memberRoute(gptRoute, "frontier", "gpt/gpt-5.6", "", "gpt", "gpt")
	r.Pay = []string{"gpt/credits"}
	seedQuota(t, map[string]acctStanding{"gpt": st}, map[string]routePref{gptRoute: r},
		map[string]potSettings{"gpt/credits": set}, nil)
	pref.mu.Lock()
	pref.ledger = l
	pref.mu.Unlock()
}

// pulledLedger is a ledger as the brain publishes it, read now: seq entries,
// gpt/credits at spend (route gptRoute).
func pulledLedger(t *testing.T, now time.Time, seq int64, spend float64) *checkLedger {
	t.Helper()
	pots := map[string]any{"gpt/credits": map[string]any{"unit": "codex_credit", "period_spend": spend,
		"routes": map[string]any{gptRoute: spend}}}
	b, _ := json.Marshal(e2eLedger(now, seq, pots))
	var l checkLedger
	if err := json.Unmarshal(b, &l); err != nil {
		t.Fatal(err)
	}
	return &l
}

// enter is one counted entry of ours on gpt/credits, not posted (no brain).
func enter(charge float64, at time.Time) {
	basis := "derived"
	ledgerEnter(ledgerEntry{ID: newLedgerID(), At: unixSeconds(at), Account: "gpt", Pot: potCredits,
		Route: gptRoute, Charge: &charge, Unit: "codex_credit", Basis: &basis})
}

func checkGpt() dialCheck {
	c, _ := routeCheck(gptRoute, "gpt", "", time.Now())
	return c
}

// The budget gate reads the brain's ledger plus every entry of ours it does
// not yet count: a budget closes at the next check after the request that
// reached it, not at the next pull. Once a pulled ledger's seq covers an
// entry, the brain's number is the one read.
func TestBudgetGateCountsOurEntriesTheBrainHasNot(t *testing.T) {
	now := time.Now()
	budget := 1.0
	paidGptWorld(t, potSettings{Budget: &budget}, pulledLedger(t, now, 5, 0.5))
	if c := checkGpt(); !c.paid() || c.Pot != "gpt/credits" {
		t.Fatalf("0.5 of 1.0 spent: %s, want paid", c)
	}
	enter(0.6, now)
	_, end := budgetPeriod(unixSeconds(now))
	c := checkGpt()
	if !c.no() || c.Reason != "budget" || !c.Until.known() {
		t.Fatalf("0.5 + our 0.6: %s, want no(budget)", c)
	}
	// Until the allowance reopens, no later than the period's end.
	if want := math.Min(*pref.accounts["gpt"].Allowance.Windows["seven_day"].ResetsAt, end); c.Until.At != want {
		t.Fatalf("until %v, want %v", c.Until.At, want)
	}

	// Accepted at seq 6: a ledger at seq 5 still lacks it.
	ledgerMu.Lock()
	ledgerOwn[0].seq = 6
	ledgerMu.Unlock()
	ledgerPulled(pulledLedger(t, now, 5, 0.5), unixSeconds(now))
	if c := checkGpt(); !c.no() || c.Reason != "budget" {
		t.Fatalf("accepted, not yet pulled: %s, want no(budget)", c)
	}
	// A ledger at seq 6 has it: the entry goes, and the brain's spend decides.
	l := pulledLedger(t, now, 6, 0.4) // (the brain's number, whatever it is)
	pref.mu.Lock()
	pref.ledger = l
	pref.mu.Unlock()
	ledgerPulled(l, unixSeconds(now))
	ledgerMu.Lock()
	left := len(ledgerOwn)
	ledgerMu.Unlock()
	if left != 0 {
		t.Fatalf("%d entries kept after a ledger covering them", left)
	}
	if c := checkGpt(); !c.paid() {
		t.Fatalf("pulled ledger at 0.4: %s, want paid", c)
	}
}

// A route budget binds its route alone, beside the pot's.
func TestRouteBudgetBindsItsRoute(t *testing.T) {
	now := time.Now()
	paidGptWorld(t, potSettings{RouteBudgets: map[string]float64{gptRoute: 0.3}}, pulledLedger(t, now, 1, 0.2))
	if c := checkGpt(); !c.paid() {
		t.Fatalf("0.2 of the route's 0.3: %s, want paid", c)
	}
	enter(0.1, now)
	if c := checkGpt(); !c.no() || c.Reason != "budget" {
		t.Fatalf("0.3 of 0.3: %s, want no(budget)", c)
	}
	// Budget 0 closes the route outright.
	paidGptWorld(t, potSettings{RouteBudgets: map[string]float64{gptRoute: 0}}, pulledLedger(t, now, 1, 0))
	if c := checkGpt(); !c.no() || c.Reason != "budget" {
		t.Fatalf("route budget 0: %s, want no(budget)", c)
	}
}

// After STALE_MAX without a pull the budget cannot be known: a budgeted pot
// reads no(unknown). Without a budget the pot pays on as before.
func TestBudgetGateOnAStaleLedger(t *testing.T) {
	now := time.Now()
	budget := 100.0
	paidGptWorld(t, potSettings{Budget: &budget}, pulledLedger(t, now.Add(-31*time.Minute), 1, 0))
	c := checkGpt()
	if !c.no() || c.Reason != "unknown" || c.Basis != "unknown" {
		t.Fatalf("ledger 31 min old: %s, want no(unknown)", c)
	}
	// Until the account's next poll: the brain's next answer may bring a
	// ledger with it (C3's rule: retryable, and a passed until is null).
	if np := pref.accounts["gpt"].NextPollAt; c.Until.At != *np {
		t.Fatalf("until %v, want the next poll %v", c.Until, *np)
	}
	// A reached budget stays reached however old the ledger: spend only grows.
	paidGptWorld(t, potSettings{Budget: &budget}, pulledLedger(t, now.Add(-31*time.Minute), 1, 100))
	if c := checkGpt(); !c.no() || c.Reason != "budget" {
		t.Fatalf("100 of 100, stale ledger: %s, want no(budget)", c)
	}
	paidGptWorld(t, potSettings{}, pulledLedger(t, now.Add(-31*time.Minute), 1, 0))
	if c := checkGpt(); !c.paid() {
		t.Fatalf("no budget, stale ledger: %s, want paid", c)
	}
	// No ledger ever pulled: the same.
	paidGptWorld(t, potSettings{Budget: &budget}, nil)
	if c := checkGpt(); !c.no() || c.Reason != "unknown" {
		t.Fatalf("no ledger: %s, want no(unknown)", c)
	}
}

// A pulled ledger of an earlier period has spent nothing of this one; our
// entries of an earlier period count in none.
func TestLedgerViewStartsEachPeriodAtZero(t *testing.T) {
	now := time.Now()
	start, _ := budgetPeriod(unixSeconds(now))
	last := time.Unix(int64(start)-3600, 0)
	budget := 1.0
	l := pulledLedger(t, last, 3, 5.0)
	l.AsOf = epoch(now) // read now, but the period it covers is last month's
	paidGptWorld(t, potSettings{Budget: &budget}, l)
	enter(2.0, last)
	v := ledgerView(unixSeconds(now))
	if v.Period.Start != start || v.Pots["gpt/credits"].PeriodSpend != 0 {
		t.Fatalf("view = %+v, want this period at 0", v)
	}
	if c := checkGpt(); !c.paid() {
		t.Fatalf("last month's spend: %s, want paid", c)
	}
}

// The budget period is the UTC calendar month, as the brain's _budget_period.
func TestBudgetPeriodIsTheUTCMonth(t *testing.T) {
	for _, c := range []struct{ at, start, end string }{
		{"2026-10-30T22:00:00Z", "2026-10-01T00:00:00Z", "2026-11-01T00:00:00Z"},
		{"2026-12-31T23:59:59Z", "2026-12-01T00:00:00Z", "2027-01-01T00:00:00Z"},
		{"2027-01-01T00:00:00Z", "2027-01-01T00:00:00Z", "2027-02-01T00:00:00Z"},
	} {
		at, _ := time.Parse(time.RFC3339, c.at)
		s, e := budgetPeriod(unixSeconds(at))
		ws, _ := time.Parse(time.RFC3339, c.start)
		we, _ := time.Parse(time.RFC3339, c.end)
		if s != float64(ws.Unix()) || e != float64(we.Unix()) {
			t.Errorf("%s: [%v, %v), want [%v, %v)", c.at, s, e, ws.Unix(), we.Unix())
		}
	}
}

// testHop is a ledger hop on gpt/credits at the brain's rates, posting nowhere.
func testHop(model string) *ledgerHop {
	return &ledgerHop{account: "gpt", pot: potCredits, route: gptRoute, model: model, unit: "codex_credit",
		speed: "standard", rates: codexRates()}
}

func ownEntries(t *testing.T) []ledgerEntry {
	t.Helper()
	ledgerMu.Lock()
	defer ledgerMu.Unlock()
	out := make([]ledgerEntry, 0, len(ledgerOwn))
	for _, o := range ledgerOwn {
		out = append(out, o.e)
	}
	return out
}

const ledgerSSE = `event: response.completed
data: {"type":"response.completed","response":{"id":"r","model":"gpt-6.1-sol","status":"completed","usage":{"input_tokens":1000,"input_tokens_details":{"cached_tokens":400},"output_tokens":200}}}

`

// The translator settles a paid dial's entry once, from the response that
// ended it: the response's model and usage, at the published rates, even
// when the client can no longer be given it. A stream that ends first, a
// failure, or a client that leaves before the end enters it uncounted.
func TestTranslatorSettlesTheLedgerHop(t *testing.T) {
	t.Cleanup(resetQuotaState())

	if status, _ := aggregateCodexSSE(strings.NewReader(ledgerSSE), 0, nil, nil, testHop("gpt-5.6"), nil); status != 200 {
		t.Fatalf("status %d", status)
	}
	es := ownEntries(t)
	if len(es) != 1 || es[0].Charge == nil || !near(*es[0].Charge, 0.081) || *es[0].Basis != "derived" ||
		*es[0].Model != "gpt-6.1-sol" || es[0].Usage["cached_tokens"] != 400 || *es[0].Speed != "standard" ||
		es[0].Route != gptRoute || es[0].Account != "gpt" || es[0].Pot != "credits" || es[0].Unit != "codex_credit" {
		t.Fatalf("aggregate entry = %+v", es)
	}

	var out strings.Builder
	if err := translateCodexSSE(strings.NewReader(ledgerSSE), &out, 0, nil, nil, nil, testHop("gpt-5.6"), nil); err != nil {
		t.Fatal(err)
	}
	if es := ownEntries(t); len(es) != 2 || es[1].Charge == nil || !near(*es[1].Charge, 0.081) {
		t.Fatalf("stream entry = %+v", es)
	}

	for _, c := range []struct {
		name, script, reason string
		w                    io.Writer
	}{
		{"truncated", "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r\"}}\n\n" +
			"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n",
			"the stream ended before response.completed", &strings.Builder{}},
		{"failed", "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\",\"message\":\"boom\"}}}\n\n",
			"the response failed", &strings.Builder{}},
		// The client is gone at the first write, before the response ends: the
		// stream stops there, and what the upstream bills is not learned.
		{"client left", "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n" + ledgerSSE,
			"the client left before the response ended", failingWriter{}},
		{"no usage", "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"status\":\"completed\"}}\n\n",
			"the response carried no usage", &strings.Builder{}},
	} {
		t.Run(c.name, func(t *testing.T) {
			n := len(ownEntries(t))
			_ = translateCodexSSE(strings.NewReader(c.script), c.w, 0, nil, nil, nil, testHop("gpt-5.6"), nil)
			es := ownEntries(t)
			if len(es) != n+1 {
				t.Fatalf("%d entries entered, want 1", len(es)-n)
			}
			e := es[n]
			if e.Charge != nil || e.Reason == nil || !strings.HasPrefix(*e.Reason, c.reason) {
				t.Fatalf("entry = %+v, want uncounted (%s)", e, c.reason)
			}
		})
	}
	// A failed response that reports usage was billed for it: charged, on
	// both paths, not entered uncounted.
	failedUsage := "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"id\":\"r\",\"model\":\"gpt-6.1-sol\"," +
		"\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"boom\"}," +
		"\"usage\":{\"input_tokens\":1000,\"input_tokens_details\":{\"cached_tokens\":400},\"output_tokens\":200}}}\n\n"
	for name, run := range map[string]func(){
		"stream": func() {
			_ = translateCodexSSE(strings.NewReader(failedUsage), &strings.Builder{}, 0, nil, nil, nil, testHop("gpt-5.6"), nil)
		},
		"aggregate": func() { _, _ = aggregateCodexSSE(strings.NewReader(failedUsage), 0, nil, nil, testHop("gpt-5.6"), nil) },
	} {
		n := len(ownEntries(t))
		run()
		es := ownEntries(t)
		if len(es) != n+1 || es[n].Charge == nil || !near(*es[n].Charge, 0.081) || *es[n].Basis != "derived" {
			t.Fatalf("%s: a failed response with usage entered %+v, want 0.081 derived", name, es[n:])
		}
	}
	// A nil hop (a dial on the allowance) enters nothing.
	n := len(ownEntries(t))
	_, _ = aggregateCodexSSE(strings.NewReader(ledgerSSE), 0, nil, nil, nil, nil)
	if len(ownEntries(t)) != n {
		t.Fatal("a nil hop entered an entry")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("client gone") }

// Only a paid check on a pot the brain's ledger counts arms a hop.
func TestArmLedgerOnlyOnACountedPaidPot(t *testing.T) {
	now := time.Now()
	paidGptWorld(t, potSettings{}, pulledLedger(t, now, 0, 0))
	c := checkGpt()
	if h := armLedger(context.Background(), c, "gpt-5.6"); h == nil || h.account != "gpt" || h.pot != "credits" || h.route != gptRoute ||
		h.unit != "codex_credit" || h.rates == nil {
		t.Fatalf("paid on gpt/credits: hop %+v", h)
	}
	if h := armLedger(context.Background(), dialCheck{Check: checkIncluded, route: gptRoute}, "m"); h != nil {
		t.Fatal("an included check armed a hop")
	}
	// A pot the ledger does not list (Anthropic or Kimi credits) is not counted.
	if h := armLedger(context.Background(), dialCheck{Check: checkPaid, Pot: "kimi/credits", route: "r"}, "m"); h != nil {
		t.Fatal("an uncounted pot armed a hop")
	}
}

// A post that fails keeps every entry for the next one; an entry the brain
// refuses is never posted again but still counts here; one it asks for again
// stays; the rest are accepted at the brain's seq.
func TestLedgerPostKeepsWhatTheBrainHasNotAccepted(t *testing.T) {
	t.Cleanup(resetQuotaState())
	now := time.Now()
	enter(0.1, now)
	enter(0.2, now)
	url, posts := fakeReportsBrain(t,
		func([]map[string]any) (int, any) { return 503, map[string]any{"error": "down"} },
		func(es []map[string]any) (int, any) {
			return 200, map[string]any{"seq": 8, "results": []map[string]any{
				{"id": es[0]["id"], "result": "retry", "why": "the ledger could not be written"},
				{"id": es[1]["id"], "result": "refused", "why": "unit must be 'codex_credit'"}}}
		},
		func(es []map[string]any) (int, any) { return 200, resultsAll(es, "applied", 9) })

	if ok, _ := reportPost(context.Background(), url, "k"); ok {
		t.Fatal("a 503 read as a post the brain took")
	}
	st := ledgerStatus()
	if st["pending"] != 2 || !strings.Contains(st["last_error"].(string), "HTTP 503") {
		t.Fatalf("after a 503: %+v", st)
	}
	if ok, _ := reportPost(context.Background(), url, "k"); ok {
		t.Fatal("a retry result read as done")
	}
	if ok, more := reportPost(context.Background(), url, "k"); !ok || more {
		t.Fatalf("the third post: ok %v more %v, want done", ok, more)
	}
	p := posts()
	if len(p) != 3 || len(p[1]) != 2 || len(p[2]) != 1 || p[2][0]["charge"] != 0.1 || p[2][0]["kind"] != "ledger" {
		t.Fatalf("posts = %v, want the refused entry dropped from the third", p)
	}
	st = ledgerStatus()
	if st["pending"] != 0 || st["refused"] != 1 || st["accepted_unpulled"] != 1 || st["last_error"] != "" {
		t.Fatalf("after the refusal and the retry: %+v", st)
	}
	// The refused entry still counts in the budget view; the accepted one
	// until a ledger at seq 9 is pulled.
	pref.mu.Lock()
	pref.ledger = pulledLedger(t, now, 8, 0)
	pref.mu.Unlock()
	if v := ledgerView(unixSeconds(now)); !near(v.Pots["gpt/credits"].PeriodSpend, 0.3) {
		t.Fatalf("view spend %v, want both entries' 0.3", v.Pots["gpt/credits"].PeriodSpend)
	}
	// Posting again sends nothing: nothing is pending.
	reportPost(context.Background(), url, "k")
	if len(posts()) != 3 {
		t.Fatal("a post with nothing pending reached the brain")
	}
}

// The entry the proxy reports is one the brain's ledger takes: a `ledger`
// event is the entry's own keys (the brain's _LEDGER_KEYS) and its kind, its
// id the entry's, and an uncounted entry carries its reason.
func TestLedgerEntryWireShape(t *testing.T) {
	t.Cleanup(resetQuotaState())
	testHop("gpt-5.6").abandon("abandoned by the first-event resend (no_head)")
	batch := reportBatch()
	if len(batch) != 1 || batch[0].entry == nil {
		t.Fatalf("batch = %+v, want the one entry", batch)
	}
	b, _ := json.Marshal(batch[0].body)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	known := map[string]bool{"id": true, "at": true, "account": true, "pot": true, "route": true, "model": true,
		"charge": true, "unit": true, "basis": true, "reason": true, "usage": true, "speed": true}
	for k := range known {
		if _, has := m[k]; !has {
			t.Errorf("key %q missing: every ledger key is written, absent ones null", k)
		}
	}
	for k := range m {
		if !known[k] && k != "kind" {
			t.Errorf("key %q is not one the brain's ledger takes", k)
		}
	}
	es := ownEntries(t)
	if m["kind"] != "ledger" || m["id"] != es[0].ID || m["charge"] != nil || m["reason"] == "" ||
		m["unit"] != "codex_credit" || !strings.HasPrefix(m["id"].(string), "proxy-") {
		t.Fatalf("uncounted entry = %v", m)
	}
}

// End to end: a paid codex dial on gpt/credits reaches the brain as a
// `ledger` event on /reports with its route, its usage and its charge, and the budget it reaches
// closes the route at the next check, before the brain's ledger is pulled.
func TestE2EPaidCodexServeIsLedgered(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()
	t.Cleanup(func() {
		ledgerMu.Lock()
		ledgerOwn, ledgerLastErr = nil, ""
		ledgerMu.Unlock()
	})
	h.spentWithCredits("gpt", "gpt")
	h.payFrom(gptRoute, "gpt/credits")
	h.brainMu.Lock()
	h.pots = map[string]any{"gpt/credits": map[string]any{"topup": "off", "budget": 0.005}}
	h.ledger = e2eLedger(time.Now(), 0, map[string]any{"gpt/credits": map[string]any{
		"unit": "codex_credit", "period_spend": 0.0, "routes": map[string]any{}}})
	h.brainMu.Unlock()
	fetchPreferredSync()
	if c := gptCheck(t); !c.paid() {
		t.Fatalf("fixture: gpt check %s, want paid", c)
	}

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-ledgered")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d (%s)", resp.StatusCode, b)
	}
	h.codexHitOrFail(t)
	reportsSettle()
	h.brainMu.Lock()
	posts := h.ledgerPosts
	h.brainMu.Unlock()
	if len(posts) != 1 {
		t.Fatalf("%d entries posted, want 1", len(posts))
	}
	e := posts[0]
	// defaultCodexSSE: gpt-5.6 (no published rate: the upper bound), 11 in, 5 out.
	if e["route"] != gptRoute || e["account"] != "gpt" || e["pot"] != "credits" || e["unit"] != "codex_credit" ||
		e["basis"] != "upper_bound" || e["model"] != "gpt-5.6" || !near(e["charge"].(float64), (11*250.0+5*1250.0)/1e6) {
		t.Fatalf("posted entry = %v", e)
	}
	// 0.009 of a 0.005 budget: closed now, though the brain's ledger (seq 0)
	// has not been pulled since.
	// The statusline's class row (§2.11): this session was served on money.
	if f := statusFunding(t, h.proxy.URL, "frontier", "sess-ledgered"); f["tier"] != "paid:gpt/credits" {
		t.Fatalf("class_funding = %v, want tier paid:gpt/credits", f)
	}
	if c := gptCheck(t); !c.no() || c.Reason != "budget" {
		t.Fatalf("after the charge: gpt check %s, want no(budget)", c)
	}
	// A pull whose seq covers it drops ours; the brain's spend decides from then.
	h.brainMu.Lock()
	h.ledger = e2eLedger(time.Now(), 1, map[string]any{"gpt/credits": map[string]any{
		"unit": "codex_credit", "period_spend": 0.009, "routes": map[string]any{gptRoute: 0.009}}})
	h.brainMu.Unlock()
	fetchPreferredSync()
	if st := ledgerStatus(); st["accepted_unpulled"] != 0 || st["pending"] != 0 {
		t.Fatalf("after the covering pull: %+v", st)
	}
	if c := gptCheck(t); !c.no() || c.Reason != "budget" {
		t.Fatalf("on the brain's ledger: gpt check %s, want no(budget)", c)
	}
}

// §2.2 row 18 on money: the first-event resend abandons a hop that was sent
// and may be billed, with no usage back. On a counted pot that hop is entered
// uncounted (reconciliation shows it, no budget does), and the resend that
// serves is entered at its charge.
func TestE2EFirstEventResendLedgersTheAbandonedHopUncounted(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()
	t.Cleanup(func() {
		ledgerMu.Lock()
		ledgerOwn, ledgerLastErr = nil, ""
		ledgerMu.Unlock()
	})
	armHealth(t, newHealthStore(defaultHealthCfg()))
	armObserve(t)
	armFirstEvent(t, map[string]time.Duration{"frontier": 300 * time.Millisecond})
	h.spentWithCredits("gpt", "gpt")
	h.payFrom(gptRoute, "gpt/credits")
	h.brainMu.Lock()
	h.ledger = e2eLedger(time.Now(), 0, map[string]any{"gpt/credits": map[string]any{
		"unit": "codex_credit", "period_spend": 0.0, "routes": map[string]any{}}})
	h.brainMu.Unlock()
	fetchPreferredSync()
	if c := gptCheck(t); !c.paid() {
		t.Fatalf("fixture: gpt check %s, want paid", c)
	}
	h.codexStall.Store(1)
	h.codexStallHead.Store(true)

	resp := h.postStreamSess(t, "claude-frontier", false, "sess-resend-ledger")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d (%s)", resp.StatusCode, b)
	}
	h.codexHitOrFail(t)
	h.codexHitOrFail(t)
	reportsSettle()
	h.brainMu.Lock()
	posts := h.ledgerPosts
	h.brainMu.Unlock()
	if len(posts) != 2 {
		t.Fatalf("%d entries posted, want the abandoned hop and the resend: %v", len(posts), posts)
	}
	if e := posts[0]; e["charge"] != nil || !strings.Contains(e["reason"].(string), "first-event resend (no_head)") ||
		e["route"] != gptRoute {
		t.Fatalf("abandoned hop = %v, want uncounted", e)
	}
	if e := posts[1]; e["charge"] == nil || e["route"] != gptRoute {
		t.Fatalf("resend = %v, want counted", e)
	}
}

// statusFunding is a session's class_funding on /_switch/status.
func statusFunding(t *testing.T, proxyURL, class, sess string) map[string]any {
	t.Helper()
	resp, err := http.Get(proxyURL + "/_switch/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var s struct {
		ClassFunding map[string]map[string]map[string]any `json:"class_funding"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Fatal(err)
	}
	f := s.ClassFunding[class][fpSess(sess)]
	if f == nil {
		t.Fatalf("no class_funding for %s/%s: %v", class, sess, s.ClassFunding)
	}
	return f
}

// A charged entry of ours the brain has not accepted for longer than
// STALE_MAX (posts failing while pulls succeed, or refused) lives only in
// this process: a restart would lose it from the count. Past that a budget
// on its pot reads no (unknown), stating no time, so a crash can lose at
// most what went unposted for less than STALE_MAX.
func TestBudgetGateOnAnEntryUnpostedPastStaleMax(t *testing.T) {
	now := time.Now()
	budget := 1.0
	old := now.Add(-31 * time.Minute)
	paidGptWorld(t, potSettings{Budget: &budget}, pulledLedger(t, now, 5, 0))
	enter(0.3, now)
	if c := checkGpt(); !c.paid() {
		t.Fatalf("0.3 unposted for a moment: %s, want paid", c)
	}
	enter(0.3, old)
	c := checkGpt()
	if !c.no() || c.Reason != "unknown" || c.Basis != "ledger_unposted" || c.Until.known() || c.Until.Paid {
		t.Fatalf("0.3 unposted 31 min: %s, want no(unknown, ledger_unposted) with no until", c)
	}
	// Refused entries are never posted again: the same.
	ledgerMu.Lock()
	ledgerOwn[1].refused = "entries[0]: unit must be 'codex_credit'"
	ledgerMu.Unlock()
	if c := checkGpt(); c.Basis != "ledger_unposted" {
		t.Fatalf("refused 31 min ago: %s, want no(unknown, ledger_unposted)", c)
	}
	// Accepted (the brain has it on disk) is not unposted, though no pull
	// covers it yet; nor is an uncounted entry, which no budget counts.
	ledgerMu.Lock()
	ledgerOwn[1].refused, ledgerOwn[1].seq = "", 6
	ledgerOwn = append(ledgerOwn, &ownEntry{e: ledgerEntry{ID: newLedgerID(), At: unixSeconds(old),
		Account: "gpt", Pot: potCredits, Route: gptRoute, Unit: "codex_credit"}})
	ledgerMu.Unlock()
	if c := checkGpt(); !c.paid() {
		t.Fatalf("accepted, and an uncounted one unposted: %s, want paid", c)
	}
	// Without a budget nothing is at stake.
	enter(0.1, old)
	paidGptWorld(t, potSettings{}, pulledLedger(t, now, 5, 0))
	if c := checkGpt(); !c.paid() {
		t.Fatalf("no budget: %s, want paid", c)
	}
}

// The brain's ledger file holds a line it cannot read, or a rate table the
// pot's charges need is not published: a budget cannot say it has room, and
// the check says which.
func TestBudgetGateOnALedgerThatMayUndercount(t *testing.T) {
	now := time.Now()
	budget := 1.0
	l := pulledLedger(t, now, 5, 0)
	l.UnreadableLines = 1
	paidGptWorld(t, potSettings{Budget: &budget}, l)
	if c := checkGpt(); !c.no() || c.Reason != "unknown" || c.Basis != "ledger_unreadable" || c.Until.known() {
		t.Fatalf("an unreadable line: %s, want no(unknown, ledger_unreadable)", c)
	}
	for name, mut := range map[string]func(*checkLedger){
		"no rates":         func(l *checkLedger) { l.Rates = nil },
		"no speed weights": func(l *checkLedger) { r := l.Rates["codex_credit"]; r.SpeedWeights = nil; l.Rates["codex_credit"] = r },
		"no models":        func(l *checkLedger) { r := l.Rates["codex_credit"]; r.Models = nil; l.Rates["codex_credit"] = r },
		"no image rates":   func(l *checkLedger) { r := l.Rates["codex_credit"]; r.Images = nil; l.Rates["codex_credit"] = r },
		"no per_tokens":    func(l *checkLedger) { r := l.Rates["codex_credit"]; r.PerTokens = 0; l.Rates["codex_credit"] = r },
	} {
		l := pulledLedger(t, now, 5, 0)
		mut(l)
		paidGptWorld(t, potSettings{Budget: &budget}, l)
		if c := checkGpt(); !c.no() || c.Reason != "unknown" || c.Basis != "no_rates" || c.Until.known() {
			t.Fatalf("%s: %s, want no(unknown, no_rates)", name, c)
		}
	}
}

// A pull drops what its seq covers and nothing else: an entry not yet
// accepted (seq 0) survives any pull, however far its seq has run.
func TestLedgerPulledKeepsAnEntryNotYetAccepted(t *testing.T) {
	now := time.Now()
	budget := 1.0
	paidGptWorld(t, potSettings{Budget: &budget}, pulledLedger(t, now, 5, 0.5))
	enter(0.6, now)
	l := pulledLedger(t, now, 1000, 0.5)
	pref.mu.Lock()
	pref.ledger = l
	pref.mu.Unlock()
	ledgerPulled(l, unixSeconds(now))
	ledgerMu.Lock()
	left := len(ledgerOwn)
	ledgerMu.Unlock()
	if left != 1 {
		t.Fatalf("%d entries after a pull, want the unaccepted one kept", left)
	}
	if c := checkGpt(); !c.no() || c.Reason != "budget" {
		t.Fatalf("0.5 + our unaccepted 0.6: %s, want no(budget)", c)
	}
}

// A check reads the pulled ledger and our entries as one: a pull landing
// between the two reads would pair the older ledger with the entries the
// newer one covers already dropped, and the spend would vanish from that
// check. Run with -race; each round races one pull against one view.
func TestLedgerViewReadsTheLedgerAndOurEntriesTogether(t *testing.T) {
	t.Cleanup(resetQuotaState())
	now := time.Now()
	at := unixSeconds(now)
	older, newer := pulledLedger(t, now, 5, 0), pulledLedger(t, now, 6, 0.6)
	charge, basis := 0.6, "derived"
	for i := 0; i < 2000; i++ {
		pref.mu.Lock()
		pref.ledger = older
		pref.mu.Unlock()
		ledgerMu.Lock()
		ledgerOwn = []*ownEntry{{seq: 6, e: ledgerEntry{ID: "e", At: at, Account: "gpt", Pot: potCredits,
			Route: gptRoute, Charge: &charge, Unit: "codex_credit", Basis: &basis}}}
		ledgerMu.Unlock()
		done := make(chan struct{})
		go func() {
			defer close(done)
			pref.mu.Lock()
			pref.ledger = newer
			pref.mu.Unlock()
			ledgerPulled(newer, at)
		}()
		v := ledgerView(at)
		<-done
		if got := v.Pots["gpt/credits"].PeriodSpend; !near(got, 0.6) {
			t.Fatalf("round %d: the view read %v, want 0.6 (seq %d)", i, got, v.Seq)
		}
	}
}

// The flush's bound holds behind the sender's post already in flight, which
// would otherwise hold the one post for brainClient's whole timeout: the flush
// stops the sender, cancelling its post (its events stay queued), and makes
// its own.
func TestShutdownFlushDoesNotWaitOnAPostInFlight(t *testing.T) {
	t.Cleanup(resetQuotaState())
	now := time.Now()
	hang, arrived := make(chan struct{}), make(chan struct{}, 4)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		select {
		case <-hang:
		case <-r.Context().Done():
		}
	}))
	release := sync.OnceFunc(func() { close(hang) })
	t.Cleanup(func() { release(); slow.Close() })
	oldBrain, oldKey := brainURL, switchKey
	brainURL, switchKey = slow.URL, "k"
	t.Cleanup(func() { brainURL, switchKey = oldBrain, oldKey })
	oldBound := reportShutdownPost
	reportShutdownPost = 200 * time.Millisecond
	t.Cleanup(func() { reportShutdownPost = oldBound })
	runReporterFor(t, slow.URL, "k")
	basis, c := "derived", 0.5
	ledgerEnter(ledgerEntry{ID: newLedgerID(), At: unixSeconds(now), Account: "gpt", Pot: potCredits,
		Route: gptRoute, Charge: &c, Unit: "codex_credit", Basis: &basis}) // the sender's post hangs
	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("the entry's own post never reached the brain")
	}
	t0 := time.Now()
	reportsFlush()
	if d := time.Since(t0); d > time.Second {
		t.Fatalf("flush took %s with a 200ms bound behind a post in flight (brainClient timeout %s)",
			d.Round(time.Millisecond), brainClient.Timeout)
	}
	if s := ledgerStatus(); s["pending"] != 1 {
		t.Fatalf("status = %v, want the entry still pending (lost to the restart)", s)
	}
}

// An entry of ours whose charge is a lower bound (an image with no image
// usage) counts at once, and a budget it reaches reads unknown, before any
// pull carries it.
func TestBudgetGateOnOurLowerBoundEntry(t *testing.T) {
	now := time.Now()
	budget := 10.0
	paidGptWorld(t, potSettings{Budget: &budget}, pulledLedger(t, now, 5, 0))
	charge, basis := 0.5, "lower_bound"
	ledgerEnter(ledgerEntry{ID: newLedgerID(), At: unixSeconds(now), Account: "gpt", Pot: potCredits,
		Route: gptRoute, Charge: &charge, Unit: "codex_credit", Basis: &basis})
	if c := checkGpt(); !c.no() || c.Reason != "unknown" || c.Basis != "unpriced" || c.Until.known() {
		t.Fatalf("our lower bound: %s, want no(unknown, unpriced)", c)
	}
	if v := ledgerView(unixSeconds(now)); !near(v.Pots["gpt/credits"].PeriodSpend, 0.5) {
		t.Fatalf("view spend %v, want the lower bound counted", v.Pots["gpt/credits"].PeriodSpend)
	}
}

// A hop marks its client request when it enters (any basis, uncounted
// included), and a chat reply written after that says x-should-retry: false,
// whatever its status; a request none of whose dials entered says nothing.
func TestAChargedRequestsReplyIsNotRetried(t *testing.T) {
	t.Cleanup(func() {
		ledgerMu.Lock()
		ledgerOwn = nil
		ledgerMu.Unlock()
	})
	ctx, mark := withLedgerMark(context.Background())
	clean := &recorder{ResponseWriter: httptest.NewRecorder(), mark: mark}
	clean.WriteHeader(http.StatusBadGateway)
	if v := clean.Header().Get("x-should-retry"); v != "" || mark.charged() {
		t.Fatalf("x-should-retry %q before any entry", v)
	}
	h := &ledgerHop{account: "gpt", pot: "credits", route: "r", unit: "codex_credit", mark: ledgerMarkOf(ctx)}
	h.abandon("the stream ended before response.completed")
	if !mark.charged() {
		t.Fatal("an uncounted entry did not mark its request")
	}
	charged := &recorder{ResponseWriter: httptest.NewRecorder(), mark: mark}
	charged.WriteHeader(http.StatusBadGateway)
	if v := charged.Header().Get("x-should-retry"); v != "false" {
		t.Fatalf("x-should-retry %q on a charged request's reply, want false", v)
	}
	var none *ledgerMark
	if none.charged() || ledgerMarkOf(context.Background()) != nil {
		t.Fatal("a request with no mark reads charged")
	}
}
