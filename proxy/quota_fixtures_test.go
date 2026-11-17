package main

// Fixtures for the quota model's /preferred blocks (C2): accounts{}, routes{},
// pots{}, pay_order — served by the e2e fake brain, or seeded straight into
// pref by unit tests — and the reset of the one record store.

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// resetQuotaState empties the quota view, the record store, our ledger
// entries and the reports' queue, returning the restore. Both are package globals: a prior test's standing or refusal must
// not steer this one's routing.
func resetQuotaState() func() {
	lan.pref.mu.Lock()
	oa, or, op, oo, os, ol := lan.pref.accounts, lan.pref.routes, lan.pref.pots, lan.pref.payOrder, lan.pref.staleMax, lan.pref.ledger
	lan.pref.accounts, lan.pref.routes, lan.pref.pots, lan.pref.payOrder, lan.pref.ledger = nil, nil, nil, nil, nil
	lan.pref.staleMax = 1800 // the brain's default STALE_MAX
	lan.pref.mu.Unlock()
	lan.ledgerMu.Lock()
	oown, oerr := lan.ledgerOwn, lan.ledgerLastErr
	lan.ledgerOwn, lan.ledgerLastErr = nil, ""
	lan.ledgerMu.Unlock()
	lan.recMu.Lock()
	orec, oread := lan.records, lan.readings
	lan.records, lan.readings = map[recKey]refusalRecord{}, map[string]headerReading{}
	lan.recMu.Unlock()
	restoreReports := resetReports()
	return func() {
		lan.pref.mu.Lock()
		lan.pref.accounts, lan.pref.routes, lan.pref.pots, lan.pref.payOrder, lan.pref.staleMax, lan.pref.ledger = oa, or, op, oo, os, ol
		lan.pref.mu.Unlock()
		lan.ledgerMu.Lock()
		lan.ledgerOwn, lan.ledgerLastErr = oown, oerr
		lan.ledgerMu.Unlock()
		lan.recMu.Lock()
		lan.records, lan.readings = orec, oread
		lan.recMu.Unlock()
		restoreReports()
	}
}

// e2eLanes are the bare lanes every fake brain serves a route for.
var e2eLanes = []string{"opus", "sonnet", "haiku", "fable", "default"}

// withRoute is a class member as the brain serves it: with its route id.
func withRoute(class string, m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m)+1)
	for k, v := range m {
		out[k] = v
	}
	if _, has := out["route"]; !has {
		id, _ := m["id"].(string)
		out["route"] = class + "/" + id
	}
	return out
}

func withRoutes(class string, ms []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(ms))
	for _, m := range ms {
		out = append(out, withRoute(class, m))
	}
	return out
}

// e2eStanding is the default fake standing of an account: measured at now, a
// seven_day window at pct (nil: no window), no money.
func e2eStanding(provider string, pct *float64, now float64) map[string]any {
	windows := map[string]any{}
	if pct != nil {
		windows["seven_day"] = map[string]any{"used_pct": *pct, "resets_at": now + 3*86400, "period": "7d", "scope": nil, "as_of": now}
	}
	return map[string]any{"provider": provider, "as_of": now, "next_poll_at": now + 300,
		"allowance": map[string]any{"open": nil, "windows": windows}, "money": map[string]any{}}
}

// memberAccounts is the accounts a fake class member's route is backed by:
// every subscription account for an anthropic member (anth); the pool view's
// for a provider member that has one, else the provider's one account.
func memberAccounts(m map[string]any, anth []string) []string {
	if m["kind"] == "anthropic" {
		return anth
	}
	if t, ok := m["targets"].(map[string]any); ok && len(t) > 0 {
		out := make([]string, 0, len(t))
		for a := range t {
			out = append(out, a)
		}
		sort.Strings(out)
		return out
	}
	p, _ := m["provider"].(string)
	return []string{p}
}

// quotaBlocks adds the quota model's blocks to a fake /preferred payload.
// Caller holds brainMu.
func (env *e2eEnv) quotaBlocks(payload map[string]any) {
	// Default standings were measured a while ago (fresh, but older than any
	// view a test serves, so the proxy's merge — the later as_of wins —
	// takes the view the test set).
	now := float64(time.Now().Add(-15*time.Minute).UnixNano()) / 1e9
	scopes := map[string]any{}
	for _, r := range payload["lane_rules"].([]map[string]any) {
		scopes[r["lane"].(string)] = r["scope"]
	}
	routes := map[string]any{}
	accounts := map[string]any{}
	// Every Anthropic account backs every Anthropic route, as the brain does:
	// both fixture accounts plus any account a view adds (a third account in
	// a scenario).
	anth := []string{native, other}
	for a, tv := range env.views {
		if v, _ := tv.(map[string]any); v["kind"] == "anthropic_account" && a != native && a != other {
			anth = append(anth, a)
		}
	}
	sort.Strings(anth[2:])
	route := func(id string, r map[string]any) {
		if pol, ok := env.routePolicy[id]; ok {
			for k, v := range pol {
				r[k] = v
			}
		}
		if _, has := r["pay"]; !has {
			r["pay"] = []string{}
		}
		// As the brain serves it: the id is the key, never a field.
		routes[id] = r
	}
	lanes := map[string]bool{}
	for _, l := range e2eLanes {
		lanes[l] = true
	}
	for l := range env.lanes {
		lanes[l] = true
	}
	for l := range lanes {
		route("lane:"+l, map[string]any{"kind": "lane", "lane": l, "provider": "anthropic",
			"scope": scopes[l], "accounts": anth})
	}
	weekly, _ := payload["weekly"].(map[string]float64)
	for _, a := range []string{native, other} {
		var pct *float64
		if v, ok := weekly[a]; ok {
			pct = &v
		}
		accounts[a] = e2eStanding("anthropic", pct, now)
	}
	served := map[string][]map[string]any{}
	if env.classMembers != nil && !env.classesGone.Load() {
		class := env.className
		if class == "" {
			class = "frontier"
		}
		served[class] = env.classMembers
	}
	image := map[string]bool{}
	if env.imageMembers != nil {
		served["images"] = env.imageMembers
		image["images"] = true
		for name, ms := range env.imageClasses {
			served[name], image[name] = ms, true
		}
	}
	for class, members := range served {
		for _, m := range withRoutes(class, members) {
			id, _ := m["id"].(string)
			accts := memberAccounts(m, anth)
			if u, has := m["usable"]; has && u == false {
				// The fixtures' "usable": false (no_target, no_quota_data,
				// stale) is a member the brain can stand behind no account
				// for: its route lists none, so its check is no (unknown).
				// The proxy reads only the route; usable itself is display.
				accts = []string{}
			}
			kind := "chat"
			if image[class] {
				kind = "image"
			}
			r := map[string]any{"kind": "member", "class": class, "member": id,
				"lane": m["lane"], "provider": m["provider"], "accounts": accts, "request_kind": kind}
			if m["kind"] == "anthropic" {
				r["provider"] = "anthropic"
				if l, ok := m["lane"].(string); ok {
					r["scope"] = scopes[l]
				}
			}
			route(m["route"].(string), r)
			if m["kind"] != "anthropic" {
				for _, a := range accts {
					if _, has := accounts[a]; !has {
						accounts[a] = e2eStanding(m["provider"].(string), nil, now)
					}
				}
			}
		}
	}
	for a, tv := range env.views {
		if v, _ := tv.(map[string]any); v != nil {
			accounts[a] = viewStanding(v, accounts[a])
		}
	}
	for a, st := range env.standings {
		accounts[a] = st
	}
	payload["accounts"] = accounts
	payload["routes"] = routes
	payload["settings"] = map[string]any{"stale_max": 1800}
	stampMemberIncumbents(payload)
	if env.pots != nil {
		payload["pots"] = env.pots
	}
	if env.payOrder != nil {
		payload["pay_order"] = env.payOrder
	}
	payload["ledger"] = env.ledger
	if env.ledger == nil {
		payload["ledger"] = e2eLedger(time.Now(), 0, nil)
	}
}

// e2eLedger is a fake brain's /preferred ledger block read at at: this
// period, seq entries, the given pots' spend ({pot: {unit, period_spend,
// routes}}), the codex credit rates the brain publishes, and the tables each
// unit's charges need (rate_tables: Runway's need none).
func e2eLedger(at time.Time, seq int64, pots map[string]any) map[string]any {
	start, end := budgetPeriod(unixSeconds(at))
	if pots == nil {
		pots = map[string]any{}
	}
	return map[string]any{"period": map[string]any{"start": start, "end": end},
		"as_of": unixSeconds(at), "seq": seq, "pots": pots, "unreadable_lines": 0,
		"rates": map[string]any{"codex_credit": map[string]any{"per_tokens": 1000000,
			"models": map[string]any{
				"gpt-6-astra": map[string]any{"input": 250.0, "cached": 25.0, "output": 1250.0},
				"gpt-6.1-sol": map[string]any{"input": 50.0, "cached": 2.5, "output": 250.0}},
			"images": map[string]any{"gpt-image-2": map[string]any{
				"image": map[string]any{"input": 200.0, "cached": 50.0, "output": 750.0},
				"text":  map[string]any{"input": 125.0, "cached": 31.25, "output": 250.0}}},
			"speed_weights": map[string]any{"standard": 1.0, "fast": 2.0, "ultrafast": 6.0}}},
		"rate_tables": map[string]any{"codex_credit": []string{"per_tokens", "models", "speed_weights", "images"},
			"runway_credit": []string{}}}
}

// stampMemberIncumbents gives each served anthropic member that names none
// the incumbent the brain gives it when the member's own checks agree with
// its lane: the lane's account, else the top-level pick. A test that needs a
// member off its lane's account sets "incumbent" on the member itself.
func stampMemberIncumbents(payload map[string]any) {
	classes, _ := payload["classes"].(map[string]any)
	lanes, _ := payload["lanes"].(map[string]any)
	for _, c := range classes {
		cm, _ := c.(map[string]any)
		ms, _ := cm["members"].([]map[string]any)
		for _, m := range ms {
			if m["kind"] != "anthropic" {
				continue
			}
			if _, has := m["incumbent"]; has {
				continue
			}
			inc := payload["account"]
			if lp, ok := lanes[fmt.Sprint(m["lane"])].(map[string]any); ok && lp["account"] != nil && lp["account"] != "" {
				inc = lp["account"]
			}
			m["incumbent"] = inc
		}
	}
}

// ---- unit-test seeding (no brain) ----

func ptr(f float64) *float64 { return &f }

func epoch(t time.Time) *float64 { return ptr(unixSeconds(t)) }

// standingAt is a measured standing: the windows given, credits per credits
// ("" none, "open", "closed"), measured at asOf.
func standingAt(provider string, asOf time.Time, windows map[string]checkWindow, credits string) acctStanding {
	st := acctStanding{Provider: provider, AsOf: epoch(asOf), NextPollAt: epoch(asOf.Add(5 * time.Minute))}
	// Every window read with the standing, as a brain poll reads them; a test
	// that ages one window sets its AsOf itself.
	for n, w := range windows {
		if w.AsOf == nil {
			w.AsOf = st.AsOf
			windows[n] = w
		}
	}
	st.Allowance.Windows = windows
	if st.Allowance.Windows == nil {
		st.Allowance.Windows = map[string]checkWindow{}
	}
	st.Money = map[string]moneyPot{}
	switch credits {
	case "open":
		o := true
		st.Money[potCredits] = moneyPot{Open: &o, Balance: ptr(10)}
	case "closed":
		c := false
		st.Money[potCredits] = moneyPot{Open: &c, Balance: ptr(0)}
	}
	return st
}

// week is an unscoped seven_day window at pct, resetting at reset.
func week(pct float64, reset time.Time) checkWindow {
	return checkWindow{UsedPct: ptr(pct), ResetsAt: epoch(reset), Period: "7d"}
}

// fiveHour is an unscoped five_hour window.
func fiveHour(pct float64, reset time.Time) checkWindow {
	return checkWindow{UsedPct: ptr(pct), ResetsAt: epoch(reset), Period: "5h"}
}

// scoped is a scoped weekly cap.
func scoped(scope string, pct float64, reset time.Time) checkWindow {
	return checkWindow{UsedPct: ptr(pct), ResetsAt: epoch(reset), Period: "7d", Scope: &scope}
}

// seedQuota installs a quota view straight into pref and empties the record
// store; t.Cleanup restores both.
func seedQuota(t interface{ Cleanup(func()) }, accounts map[string]acctStanding, routes map[string]routePref,
	pots map[string]potSettings, payOrder []string) {
	restore := resetQuotaState()
	t.Cleanup(restore)
	lan.pref.mu.Lock()
	lan.pref.accounts, lan.pref.routes, lan.pref.pots, lan.pref.payOrder = accounts, routes, pots, payOrder
	lan.pref.mu.Unlock()
}

// laneRoute is a bare lane's route over accounts.
func laneRoute(lane, scope string, accounts ...string) routePref {
	r := routePref{Kind: "lane", Lane: lane, Provider: "anthropic", Accounts: accounts}
	r.ID, r.Scope, r.Pay = "lane:"+lane, scope, []string{}
	return r
}

// memberRoute is a class member's route over accounts.
func memberRoute(id, class, member, lane, provider string, accounts ...string) routePref {
	r := routePref{Kind: "member", Class: class, Member: member, Lane: lane, Provider: provider, Accounts: accounts}
	r.ID, r.Pay = id, []string{}
	return r
}

// ---- unit members (health and ranker suites) ----

// uAccount is the one account a unit member's route is backed by, so a record
// on it speaks for that member alone.
func uAccount(memberID string) string { return "u-" + memberID }

// uRoute registers route "u/<id>" over uAccount(id), measured and with nothing
// binding (check included, basis unknown) until a record says otherwise. It
// adds to whatever quota view is installed: unit members carry unique ids, so
// nothing collides, and a test that needs a clean view seeds one first.
func uRoute(memberID string) string {
	id, acct := "u/"+memberID, uAccount(memberID)
	asOf := 1.0
	lan.pref.mu.Lock()
	if lan.pref.routes == nil {
		lan.pref.routes = map[string]routePref{}
	}
	if lan.pref.accounts == nil {
		lan.pref.accounts = map[string]acctStanding{}
	}
	lan.pref.routes[id] = memberRoute(id, "u", memberID, "", "anthropic", acct)
	st := acctStanding{Provider: "anthropic", AsOf: &asOf}
	st.Allowance.Windows = map[string]checkWindow{}
	lan.pref.accounts[acct] = st
	lan.pref.mu.Unlock()
	return id
}

// seedSpent seeds the record store with an unattributed spent record (its
// clamp), the record recordRefusal writes for a limit no reading carries (a
// Kimi billing-cycle pool), for a test whose subject is what a spent account
// does to routing rather than how its refusal was classified.
func seedSpent(account, limit string, status int, detail string, resets time.Time) refusalRecord {
	return lan.writeRecord(account, limit, refusalRecord{kind: recSpent, limKind: limPool,
		refusedAt: nowFn(), resets: resets, status: status, detail: detail})
}

// uSpent records the unit member's account spent: its check is `no (spent)`
// and PR11 never re-admits it.
func uSpent(m classMemberPref) {
	seedSpent(uAccount(m.ID), "seven_day", 429, "usage_limit_reached", time.Time{})
}

// uModelSpent records an unattributed refusal on the member's model only
// (§2.6): its check reads `no (spent)`, and PR11 may re-admit it.
func uModelSpent(m classMemberPref) {
	seedSpent(uAccount(m.ID), modelLimit(m.Model), 429, "usage_limit_reached", time.Time{})
}

// uUnroutable is a member the brain served no route for: `no (unknown)`.
func uUnroutable(m classMemberPref) classMemberPref {
	m.Route = "u/none/" + m.ID
	return m
}

// classMemberDialable is memberDialable for a member of a class the proxy
// holds, by id ("" = dialable).
func classMemberDialable(t interface{ Fatalf(string, ...any) }, class, id string) string {
	lan.pref.mu.RLock()
	cp := lan.pref.classes[class]
	lan.pref.mu.RUnlock()
	for _, m := range cp.Members {
		if m.ID == id {
			return lan.memberDialable(m, nowFn())
		}
	}
	t.Fatalf("class %s has no member %s", class, id)
	return ""
}

// laneNo reports whether acct's check on the bare lane's route is `no` now.
func laneNo(acct, lane string) bool {
	c, _ := lan.routeCheck(laneRouteID(lane), acct, "", nowFn())
	return c.no()
}

// recordsOn lists the live records on acct as "<kind>:<limit>", sorted.
func recordsOn(acct string) []string {
	now := nowFn()
	lan.recMu.Lock()
	defer lan.recMu.Unlock()
	var out []string
	for k, r := range lan.records {
		if k.account == acct && r.live(now) {
			out = append(out, r.kind+":"+k.limit)
		}
	}
	sort.Strings(out)
	return out
}

// throttledOn reports a live throttle on acct.
func throttledOn(acct string) bool { return !lan.throttledUntil(acct, nowFn()).IsZero() }

// liveRecords counts every live record in the store.
func liveRecords() int {
	now := nowFn()
	lan.recMu.Lock()
	defer lan.recMu.Unlock()
	n := 0
	for _, r := range lan.records {
		if r.live(now) {
			n++
		}
	}
	return n
}

// viewStanding is a fake brain's standing for an account its view describes,
// read the way the brain reads the same poll: as_of the last good poll (none:
// never measured), one window per view window, a scoped window keyed
// scoped:<Scope> with its aliases. prev supplies the provider when the view
// names none.
func viewStanding(v map[string]any, prev any) map[string]any {
	provider := "anthropic"
	if v["kind"] == "provider" {
		provider = ""
		if p, _ := prev.(map[string]any); p != nil {
			provider, _ = p["provider"].(string)
		}
	}
	windows := map[string]any{}
	ws, _ := v["windows"].(map[string]any)
	for n, wv := range ws {
		w, _ := wv.(map[string]any)
		out := map[string]any{"used_pct": w["percent"], "resets_at": w["resets_at"], "period": "7d", "scope": nil,
			"as_of": v["good_ts"]}
		if n == "five_hour" {
			out["period"] = "5h"
		}
		if sc, ok := strings.CutPrefix(n, "scoped:"); ok {
			out["scope"], out["aliases"] = sc, w["aliases"]
		}
		windows[n] = out
	}
	asOf := v["good_ts"]
	next := any(nil)
	if f, ok := asOf.(float64); ok {
		next = f + 300
	}
	return map[string]any{"provider": provider, "as_of": asOf, "next_poll_at": next,
		"allowance": map[string]any{"open": nil, "windows": windows}, "money": map[string]any{}}
}
