package main

// When a refusal record ends (quota-model 50 §2.6, §5.1 "Record ending", C7).
// An attributed record (a limit some reading carries) ends at the reset the
// refusal named, else by the first newer reading of the limit that shows it
// open and changed, with no clamp. An unattributed one keeps the clamp. The
// owner's re-check ends both. Each case runs the real path: the adapter
// classifies a recorded refusal, recordRefusal writes it against the standing,
// and pruneRecords judges each brain reading after it.

import (
	"strconv"
	"testing"
	"time"
)

// recWorld seeds three accounts read a minute before now: gpt-a (codex, its
// weekly window at 100 until r7, credits open with 10), kimi (5h at 80 until
// r5, entitled) and a (Anthropic, no credits reading). Each has one route with
// pay []. The clock is the returned pointer.
func recWorld(t *testing.T) (now *time.Time, r5, r7 time.Time) {
	t.Helper()
	withTestProviders(t)
	start := time.Date(2026, 10, 29, 21, 0, 0, 0, time.UTC)
	clock := start
	old := nowFn
	nowFn = func() time.Time { return clock }
	t.Cleanup(func() { nowFn = old })
	r5, r7 = start.Add(3*time.Hour), start.Add(72*time.Hour)
	read := start.Add(-time.Minute)
	gpt := standingAt("codex", read, map[string]checkWindow{"seven_day": week(100, r7), "five_hour": fiveHour(40, start.Add(4*time.Hour))}, "open")
	kimi := standingAt("kimi", read, map[string]checkWindow{"five_hour": fiveHour(80, r5), "seven_day": week(30, r7)}, "")
	kimi.Allowance.Entitlement = &entitlement{State: "active"}
	a := standingAt("anthropic", read, map[string]checkWindow{"five_hour": fiveHour(20, r5), "seven_day": week(40, r7)}, "")
	seedQuota(t, map[string]acctStanding{"gpt-a": gpt, "kimi": kimi, "a": a}, map[string]routePref{
		"r-gpt":  memberRoute("r-gpt", "c", "gpt/gpt-5.6", "", "gpt", "gpt-a"),
		"r-kimi": memberRoute("r-kimi", "c", "kimi/kimi-k3", "", "kimi", "kimi"),
		"r-a":    memberRoute("r-a", "c", "lane:opus", "opus", "anthropic", "a"),
	}, nil, nil)
	return &clock, r5, r7
}

func refuse(t *testing.T, m classMemberPref, account string, status int, hdr map[string]string, body string) {
	t.Helper()
	resp := hResp(status, hdr, body)
	if kind, _ := lan.recordRefusal(account, lan.rungRefusal(m, resp), resp); kind == "" {
		t.Fatalf("%d %s recorded nothing", status, body)
	}
}

func recOf(account, limit string) (refusalRecord, bool) {
	lan.recMu.Lock()
	defer lan.recMu.Unlock()
	r, ok := lan.records[recKey{account, limit}]
	return r, ok && r.live(nowFn())
}

// reread replaces account's standing with a reading at asOf and runs the
// prune the pull runs after it.
func reread(account string, asOf time.Time, edit func(*acctStanding)) {
	lan.pref.mu.Lock()
	st := lan.pref.accounts[account]
	st.AsOf = epoch(asOf)
	ws := make(map[string]checkWindow, len(st.Allowance.Windows))
	for n, w := range st.Allowance.Windows {
		w.AsOf = st.AsOf
		ws[n] = w
	}
	st.Allowance.Windows = ws
	money := make(map[string]moneyPot, len(st.Money))
	for n, p := range st.Money {
		money[n] = p
	}
	st.Money = money
	if edit != nil {
		edit(&st)
	}
	lan.pref.accounts[account] = st
	accounts := lan.pref.accounts
	lan.pref.mu.Unlock()
	lan.pruneRecords(accounts)
}

func setWindow(name string, pct float64, reset *float64) func(*acctStanding) {
	return func(st *acctStanding) {
		w := st.Allowance.Windows[name]
		w.UsedPct, w.ResetsAt = ptr(pct), reset
		st.Allowance.Windows[name] = w
	}
}

func s9(resetsAt time.Time) string {
	return `{"error":{"type":"usage_limit_reached","message":"The usage limit has been reached","plan_type":"pro","resets_at":` +
		strconv.FormatInt(resetsAt.Unix(), 10) + `,"limit_window_minutes":10080}}`
}

// A codex usage_limit_reached on a window the reading carries is attributed:
// it ends at the reset it names, three days out, with no 6 h cap. The codex
// early reset (a new resets_at, use back to 0) ends it at the next reading.
func TestRecordEndCodexEarlyReset(t *testing.T) {
	now, _, r7 := recWorld(t)
	refuse(t, memGpt, "gpt-a", 429, nil, s9(r7))
	rec, ok := recOf("gpt-a", "seven_day")
	if !ok || !rec.attributed || !rec.until.Equal(r7) {
		t.Fatalf("record = %+v, want attributed until the named reset %v", rec, r7)
	}
	// The window is closed until r7; the credits it could draw are not in
	// this route's pay.
	if c, _ := lan.routeCheck("r-gpt", "gpt-a", "gpt-5.6", *now); !c.no() || c.Reason != "pay" || !c.Until.Time().Equal(r7) {
		t.Fatalf("check = %s, want no(pay) until %v", c, r7)
	}
	reread("gpt-a", now.Add(time.Minute), setWindow("seven_day", 0, epoch(now.Add(7*24*time.Hour))))
	if _, ok := recOf("gpt-a", "seven_day"); ok {
		t.Fatal("the early reset (a new resets_at, use 0) did not end the record")
	}
	if c, _ := lan.routeCheck("r-gpt", "gpt-a", "gpt-5.6", *now); !c.included() {
		t.Fatalf("check after the early reset = %s, want included", c)
	}
}

// A refusal that names no reset ends by reading (62 m6): a reading older than
// the refusal, one of the same period at the same or higher use, one with a
// null reset, or one with a passed reset does not end it; the first newer
// reading's reset becomes its end; a lower use ends it.
func TestRecordEndSamePeriodSameOrHigherUseDoesNotEnd(t *testing.T) {
	now, r5, _ := recWorld(t)
	refuse(t, memKimi, "kimi", 403, nil, kimi403FiveHour)
	rec, ok := recOf("kimi", "five_hour")
	if !ok || !rec.attributed || !rec.until.IsZero() {
		t.Fatalf("record = %+v, want attributed, ending by reading", rec)
	}
	if c, _ := lan.routeCheck("r-kimi", "kimi", "kimi-k3", *now); !c.no() || c.Reason != "spent" {
		t.Fatalf("check = %s, want no(spent): the refusal outranks the 80 %% reading", c)
	}
	steps := []struct {
		name string
		at   time.Duration
		edit func(*acctStanding)
	}{
		{"an older reading at lower use", -30 * time.Second, setWindow("five_hour", 10, epoch(r5))},
		{"the same period at the same use", 5 * time.Minute, setWindow("five_hour", 80, epoch(r5))},
		{"the same period at higher use (the reset restated within slack)", 10 * time.Minute, setWindow("five_hour", 95, epoch(r5.Add(30*time.Second)))},
		{"a null reset", 15 * time.Minute, setWindow("five_hour", 80, nil)},
		{"a passed reset", 20 * time.Minute, setWindow("five_hour", 80, epoch(now.Add(-time.Hour)))},
	}
	for _, s := range steps {
		reread("kimi", now.Add(s.at), s.edit)
		if _, ok := recOf("kimi", "five_hour"); !ok {
			t.Fatalf("%s ended the record", s.name)
		}
	}
	if rec, _ := recOf("kimi", "five_hour"); !rec.until.Equal(time.Unix(0, int64(unixSeconds(r5)*1e9))) {
		t.Fatalf("until = %v, want the first newer reading's reset %v", rec.until, r5)
	}
	reread("kimi", now.Add(25*time.Minute), setWindow("five_hour", 60, epoch(r5)))
	if _, ok := recOf("kimi", "five_hour"); ok {
		t.Fatal("a newer reading at a lower use did not end the record")
	}
}

// A new period in a newer reading (a reset other than the one seen, still
// ahead) ends the record even at a high use.
func TestRecordEndNewPeriod(t *testing.T) {
	now, r5, _ := recWorld(t)
	refuse(t, memKimi, "kimi", 403, nil, kimi403FiveHour)
	reread("kimi", now.Add(time.Minute), setWindow("five_hour", 90, epoch(r5.Add(5*time.Hour))))
	if _, ok := recOf("kimi", "five_hour"); ok {
		t.Fatal("a new period did not end the record")
	}
	// At 100 the new period is closed: a reading of it ends nothing.
	refuse(t, memKimi, "kimi", 403, nil, kimi403FiveHour)
	reread("kimi", now.Add(2*time.Minute), setWindow("five_hour", 100, epoch(r5.Add(10*time.Hour))))
	if _, ok := recOf("kimi", "five_hour"); !ok {
		t.Fatal("a reading showing the limit closed ended the record")
	}
}

// Unattributed records keep the clamp: a Kimi model-level 403 holds 6 h, an
// Anthropic unpaid with no balance reading holds 30 min, the billing-cycle
// pool 30 min, and a named reset past the cap holds the cap. No reading ends
// them.
func TestRecordEndUnattributedClamp(t *testing.T) {
	now, _, _ := recWorld(t)
	start := *now
	refuse(t, memKimi, "kimi", 403, nil, kimiModel403)
	refuse(t, memAnth, "a", 402, nil, anthBilling402)
	refuse(t, memKimi, "kimi", 429, map[string]string{"Retry-After": "86400"}, kimi403Cycle)
	for _, c := range []struct {
		limit, account, kind string
		until                time.Time
	}{
		{modelLimit("kimi-k3"), "kimi", recNotEntitled, start.Add(quotaCooldownCap)},
		{potCredits, "a", recUnpaid, start.Add(quotaCooldown)},
		{"total_quota", "kimi", recSpent, start.Add(quotaCooldownCap)},
	} {
		rec, ok := recOf(c.account, c.limit)
		if !ok || rec.attributed || rec.kind != c.kind || !rec.until.Equal(c.until) {
			t.Fatalf("%s/%s = %+v, want unattributed %s until %v", c.account, c.limit, rec, c.kind, c.until)
		}
	}
	// The model record binds that model only.
	if c, _ := lan.routeCheck("r-kimi", "kimi", "kimi-k3-max", *now); !c.no() {
		t.Fatalf("kimi-k3-max = %s: the pool record closes the account", c)
	}
	// The Anthropic credits pot reads closed for as long as the record holds.
	if st := lan.standingFor("a", *now); st.Money[potCredits].Open == nil || *st.Money[potCredits].Open {
		t.Fatal("an unpaid record did not close its pot")
	}
	open := true
	reread("a", now.Add(time.Minute), func(st *acctStanding) { st.Money[potCredits] = moneyPot{Open: &open, Balance: ptr(5)} })
	reread("kimi", now.Add(time.Minute), setWindow("five_hour", 0, epoch(now.Add(5*time.Hour))))
	if _, ok := recOf("a", potCredits); !ok {
		t.Fatal("a reading ended an unattributed record")
	}
	if _, ok := recOf("kimi", modelLimit("kimi-k3")); !ok {
		t.Fatal("a reading ended a model record")
	}
	*now = start.Add(quotaCooldown + time.Second)
	if _, ok := recOf("a", potCredits); ok {
		t.Fatal("the unpaid record outlived its 30 min")
	}
	*now = start.Add(quotaCooldownCap - time.Second)
	if _, ok := recOf("kimi", modelLimit("kimi-k3")); !ok {
		t.Fatal("the model-level 403 ended before 6 h")
	}
	*now = start.Add(quotaCooldownCap + time.Second)
	if _, ok := recOf("kimi", modelLimit("kimi-k3")); ok {
		t.Fatal("the model-level 403 outlived 6 h")
	}
}

// The owner's re-check (recheck_epoch, POST /recheck on the brain) ends every
// record on the account older than it, attributed or not; a refusal after it
// stands. The epoch survives the merge of an older standing.
func TestRecordEndRecheckEpoch(t *testing.T) {
	now, _, r7 := recWorld(t)
	refuse(t, memKimi, "kimi", 403, nil, kimiModel403)
	refuse(t, memKimi, "kimi", 403, nil, kimi403FiveHour)
	refuse(t, memAnth, "a", 402, nil, anthBilling402)
	refuse(t, memGpt, "gpt-a", 429, nil, s9(r7))
	epochAt := now.Add(time.Second)
	*now = now.Add(2 * time.Second)
	refuse(t, memGpt, "gpt-a", 429, nil, codexCreditExhausted429) // after the gpt-a epoch below
	for _, a := range []string{"kimi", "a"} {
		reread(a, now.Add(-time.Hour), func(st *acctStanding) { st.RecheckEpoch = epoch(epochAt) })
	}
	reread("gpt-a", now.Add(-time.Hour), func(st *acctStanding) { st.RecheckEpoch = epoch(epochAt) })
	for _, k := range []recKey{{"kimi", modelLimit("kimi-k3")}, {"kimi", "five_hour"}, {"a", potCredits}, {"gpt-a", "seven_day"}} {
		if _, ok := recOf(k.account, k.limit); ok {
			t.Fatalf("the re-check did not end %s/%s", k.account, k.limit)
		}
	}
	if _, ok := recOf("gpt-a", potCredits); !ok {
		t.Fatal("the re-check ended a refusal newer than it")
	}
	prev := map[string]acctStanding{"kimi": {AsOf: epoch(*now)}}
	next := map[string]acctStanding{"kimi": {AsOf: epoch(now.Add(-time.Hour)), RecheckEpoch: epoch(*now)}}
	if m := mergeStandings(prev, next); m["kimi"].RecheckEpoch == nil || *m["kimi"].RecheckEpoch != unixSeconds(*now) {
		t.Fatal("the merge kept the older standing's re-check")
	}
}

// A codex billing refusal is attributed to the credits gate the codex reading
// carries: the pot reads closed until a newer reading shows it open with more
// money (or newly open), not on a restatement of the balance it had.
func TestRecordEndCodexCreditsByReading(t *testing.T) {
	now, _, _ := recWorld(t)
	refuse(t, memGpt, "gpt-a", 429, nil, codexCreditExhausted429)
	rec, ok := recOf("gpt-a", potCredits)
	if !ok || !rec.attributed || !rec.until.IsZero() {
		t.Fatalf("record = %+v, want attributed to the credits gate, ending by reading", rec)
	}
	if st := lan.standingFor("gpt-a", *now); *st.Money[potCredits].Open {
		t.Fatal("the unpaid record did not close the credits pot")
	}
	open := true
	reread("gpt-a", now.Add(time.Minute), func(st *acctStanding) { st.Money[potCredits] = moneyPot{Open: &open, Balance: ptr(10)} })
	if _, ok := recOf("gpt-a", potCredits); !ok {
		t.Fatal("the same balance ended the record")
	}
	*now = now.Add(24 * time.Hour) // no clamp: still in force a day on
	if _, ok := recOf("gpt-a", potCredits); !ok {
		t.Fatal("an attributed money record expired by time")
	}
	reread("gpt-a", now.Add(time.Minute), func(st *acctStanding) { st.Money[potCredits] = moneyPot{Open: &open, Balance: ptr(50)} })
	if _, ok := recOf("gpt-a", potCredits); ok {
		t.Fatal("a reading with more money did not end the record")
	}
}

// A pot whose open the reading at the refusal did not know is judged by the
// balance that reading did carry: the same balance read open later is no
// change (it was open, unknown to us, when the provider said no money), and
// only more money ends the record.
func TestRecordEndPotWithUnknownOpenAtTheRefusal(t *testing.T) {
	now, _, _ := recWorld(t)
	reread("gpt-a", now.Add(-30*time.Second), func(st *acctStanding) {
		st.Money[potCredits] = moneyPot{Open: nil, Balance: ptr(10)}
	})
	refuse(t, memGpt, "gpt-a", 429, nil, codexCreditExhausted429)
	if rec, ok := recOf("gpt-a", potCredits); !ok || rec.seen.open != nil || rec.seen.balance == nil {
		t.Fatalf("record = %+v, want the refusal's reading: open unknown, balance 10", rec)
	}
	open := true
	reread("gpt-a", now.Add(time.Minute), func(st *acctStanding) { st.Money[potCredits] = moneyPot{Open: &open, Balance: ptr(10)} })
	if _, ok := recOf("gpt-a", potCredits); !ok {
		t.Fatal("the same balance read open ended a record whose open was unknown at the refusal")
	}
	reread("gpt-a", now.Add(2*time.Minute), func(st *acctStanding) { st.Money[potCredits] = moneyPot{Open: &open, Balance: ptr(40)} })
	if _, ok := recOf("gpt-a", potCredits); ok {
		t.Fatal("a reading with more money did not end the record")
	}
}

// Kimi's "no access to Kimi Code" is not_entitled on the plan: the account's
// allowance reads closed on every route until a newer reading shows the
// entitlement active, or a 2xx on the allowance disproves it.
func TestRecordEndKimiEntitlement(t *testing.T) {
	now, _, _ := recWorld(t)
	refuse(t, memKimi, "kimi", 403, nil, kimiNotEntitled403)
	if rec, ok := recOf("kimi", limitEntitlement); !ok || !rec.attributed || rec.kind != recNotEntitled {
		t.Fatalf("record = %+v, want attributed not_entitled on the plan", rec)
	}
	if c, _ := lan.routeCheck("r-kimi", "kimi", "kimi-k3", *now); !c.no() {
		t.Fatalf("check = %s, want no", c)
	}
	reread("kimi", now.Add(-time.Second), nil)
	if _, ok := recOf("kimi", limitEntitlement); !ok {
		t.Fatal("an older reading ended the entitlement record")
	}
	reread("kimi", now.Add(time.Minute), nil)
	if _, ok := recOf("kimi", limitEntitlement); ok {
		t.Fatal("a newer reading with the plan active did not end the record")
	}
	refuse(t, memKimi, "kimi", 403, nil, kimiNotEntitled403)
	refuse(t, memKimi, "kimi", 403, nil, kimiModel403)
	lan.noteServed("kimi", "", "kimi-k3", now.Add(time.Second))
	if _, ok := recOf("kimi", limitEntitlement); ok {
		t.Fatal("a 2xx on the allowance did not end the entitlement record")
	}
	if _, ok := recOf("kimi", modelLimit("kimi-k3")); ok {
		t.Fatal("a 2xx on the model did not end its record")
	}
}

// A 2xx a pot paid for ends that pot's unpaid record, and nothing else.
func TestRecordEndPaidServed(t *testing.T) {
	now, _, r7 := recWorld(t)
	refuse(t, memGpt, "gpt-a", 429, nil, codexCreditExhausted429)
	refuse(t, memGpt, "gpt-a", 429, nil, s9(r7))
	lan.notePaidServed("gpt-a", potCredits, now.Add(time.Second))
	if _, ok := recOf("gpt-a", potCredits); ok {
		t.Fatal("a paid 2xx did not end the pot's record")
	}
	if _, ok := recOf("gpt-a", "seven_day"); !ok {
		t.Fatal("a paid 2xx ended the window's record")
	}
	st := lan.recordsStatus(*now)
	got, _ := st["gpt-a/seven_day"].(map[string]any)
	if got["attributed"] != true || got["until"] == nil {
		t.Fatalf("status = %v, want attributed with its until", got)
	}
}
