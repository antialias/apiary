package main

// The brain and the proxy evaluate the same dial check (quota-model 50 §1.6,
// §5.1). dialcheck_golden.json pins the function; this pins the wire: the
// proxy decodes what the brain really serves (testdata/preferred_c2.json,
// produced by the brain itself — gen_preferred_c2.py) and, with no record of
// its own, must reach the brain's check on every (route, account) the brain
// lists, the brain's stale limit included.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"testing"
	"time"
)

func TestProxyReadsTheBrainsPreferredAndAgreesOnEveryCheck(t *testing.T) {
	raw, err := os.ReadFile("testdata/preferred_c2.json")
	if err != nil {
		t.Fatal(err)
	}
	var fx struct {
		Now       float64         `json:"now"`
		Preferred json.RawMessage `json:"preferred"`
	}
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatal(err)
	}
	var brain struct {
		Routes map[string]struct {
			Checks map[string]dialCheck `json:"checks"`
		} `json:"routes"`
		Classes map[string]struct {
			Members []struct {
				ID    string `json:"id"`
				Route string `json:"route"`
			} `json:"members"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(fx.Preferred, &brain); err != nil {
		t.Fatal(err)
	}

	now := time.Unix(0, int64(fx.Now*1e9))
	old := nowFn
	nowFn = func() time.Time { return now }
	t.Cleanup(func() { nowFn = old })
	t.Cleanup(resetQuotaState()) // the record store; the view goes with pref below
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != tpath("/preferred") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(fx.Preferred)
	}))
	t.Cleanup(srv.Close)
	// fetchPreferred writes the whole decision and the class rows: run it on a
	// fresh decision and put both back after.
	oldBrain, oldPref := brainBase, lan.pref
	brainBase, lan.pref = srv.URL, &decision{}
	lan.classRowsMu.Lock()
	oldRows, oldSource := lan.classRows, lan.classRowsSource
	lan.classRowsMu.Unlock()
	t.Cleanup(func() {
		prewarmWG.Wait()
		brainBase, lan.pref = oldBrain, oldPref
		lan.classRowsMu.Lock()
		lan.classRows, lan.classRowsSource = oldRows, oldSource
		lan.classRowsMu.Unlock()
	})
	fetchPreferred()

	ids := make([]string, 0, len(brain.Routes))
	for id := range brain.Routes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	type seen struct{ id, acct string }
	got := map[seen]dialCheck{}
	for _, id := range ids {
		for acct, want := range brain.Routes[id].Checks {
			c, _ := lan.routeCheck(id, acct, "", now)
			want.route = id // the route the check was made for, which the wire does not carry
			if c != want {
				t.Errorf("%s on %s: proxy %s, brain %s", id, acct, c, want)
			}
			got[seen{id, acct}] = c
		}
	}
	// The brain serves a route's id only as its key, and the unavailable body
	// (§2.8) names each route and reads its accounts' checks by that id: from
	// what the brain serves, every route's body is the brain's checks.
	for _, id := range ids {
		r, _, _, _ := lan.routeOf(id)
		if r.ID != id {
			t.Errorf("route %s decoded with id %q", id, r.ID)
		}
		u := lan.routesUnavailable([]unavailRoute{{policy: r.routePolicy, accounts: r.Accounts}}, nil, now)
		if u == nil {
			continue // no account: configuration, the caller's typed 503
		}
		if m := u.Members[0]; m.Route != id {
			t.Errorf("route %s: the body names route %q", id, m.Route)
		}
		for _, ua := range u.Members[0].Accounts {
			want := brain.Routes[id].Checks[ua.Account]
			why := ""
			if ua.Why != nil {
				why = *ua.Why
			}
			if ua.Check != checkLabel(want) || why != want.Reason || ua.Basis != want.Basis {
				t.Errorf("%s on %s: the body says %s (%s, %s), brain %s", id, ua.Account, ua.Check, why, ua.Basis, want)
			}
		}
	}
	if s := lan.staleMax(); s != 1800 {
		t.Errorf("stale_max = %v, want the brain's published 1800", s)
	}
	// The fixture is only worth its name while it holds every kind of answer
	// the two evaluators could disagree on (gen_preferred_c2.py's scenario).
	const a, b = "acct-a@example.com", "acct-b@example.com"
	for _, w := range []struct {
		what string
		at   seen
		ok   func(dialCheck) bool
	}{
		{"included", seen{"lane:default", a}, dialCheck.included},
		{"paid on the opus member's credits", seen{"frontier/lane:opus", a},
			func(c dialCheck) bool { return c.paid() && c.Pot == a+"/credits" }},
		{"stale with money at risk", seen{"lane:default", b},
			func(c dialCheck) bool { return c.no() && c.Reason == "unknown" && c.Basis == "stale" }},
		{"stop_at", seen{"lane:sonnet", a}, func(c dialCheck) bool { return c.no() && c.Reason == "stop_at" }},
		{"pay [] refuses a spent allowance", seen{"lane:opus", a}, func(c dialCheck) bool { return c.no() && c.Reason == "pay" }},
		{"codex gate closed under 100, pay []", seen{"frontier/gpt/gpt-5.6", "gpt"},
			func(c dialCheck) bool { return c.no() && c.Reason == "pay" && c.Until.known() }},
		{"the image route paying its credits on the same codex account", seen{"image/gpt/gpt-image-2", "gpt"},
			func(c dialCheck) bool { return c.paid() && c.Pot == "gpt/credits" }},
	} {
		if c, ok := got[w.at]; !ok || !w.ok(c) {
			t.Errorf("the fixture lacks its %s case at %v (got %s); regenerate it with gen_preferred_c2.py", w.what, w.at, c)
		}
	}

	// The image door's class decodes as one (C4): its row carries the image
	// modality, so the chat door refuses it and no picker lists it.
	if e, ok := lan.matchRegistryClassName("image"); !ok || e.Modality != modalityImage {
		t.Errorf("class image decoded as %+v, want an image row", e)
	}

	// The ledger the budget gate reads and ledger.go charges by, as the brain
	// publishes it (C6): the counted pot's unit and spend, and its rates.
	lan.pref.mu.RLock()
	l := lan.pref.ledger
	lan.pref.mu.RUnlock()
	if l == nil || l.AsOf == nil || l.Period.Start == 0 || l.Seq < 1 {
		t.Fatalf("ledger = %+v, want the brain's", l)
	}
	if lp := l.Pots["gpt/credits"]; lp.Unit == nil || *lp.Unit != "codex_credit" || lp.PeriodSpend != 12.5 ||
		lp.Routes["frontier/gpt/gpt-5.6"] != 12.5 {
		t.Errorf("ledger.pots[gpt/credits] = %+v, want the fixture's counted 12.5 credits", lp)
	}
	if r, ok := l.Rates["codex_credit"]; !ok || r.PerTokens != 1e6 || r.Models["gpt-6.1-sol"].Output != 250 ||
		r.SpeedWeights["standard"] != 1 {
		t.Errorf("ledger.rates = %+v", l.Rates)
	}
	// The image door's entries charge by the image model's own rates (§1.7).
	if ir := l.Rates["codex_credit"].Images["gpt-image-2"]; ir.Image.Output != 750 || ir.Text.Input != 125 {
		t.Errorf("ledger.rates.codex_credit.images[gpt-image-2] = %+v, want 11 §3's", ir)
	}
	if h := lan.armLedger(context.Background(), dialCheck{Check: checkPaid, Pot: "gpt/credits", route: "frontier/gpt/gpt-5.6"}, "gpt-6.1-sol"); h == nil {
		t.Error("a paid dial on gpt/credits arms no ledger entry on the brain's ledger")
	}
	// The scope map is the accounts{} windows' aliases (C5 moved off targets).
	sm := lan.currentScopes()
	if len(sm.lane) == 0 {
		t.Error("no lane joins decoded from the brain's lane_rules")
	}

	// Each class member names a route the proxy holds, so its dials are checked
	// against that route's policy, not refused as unknown.
	lan.pref.mu.RLock()
	classes := lan.pref.classes
	lan.pref.mu.RUnlock()
	for name, c := range brain.Classes {
		for i, m := range c.Members {
			if m.Route == "" {
				t.Errorf("%s/%s: the brain served no route", name, m.ID)
				continue
			}
			if got := classes[name].Members[i].Route; got != m.Route {
				t.Errorf("%s/%s: decoded route %q, served %q", name, m.ID, got, m.Route)
			}
			if _, _, _, ok := lan.routeOf(m.Route); !ok {
				t.Errorf("%s/%s: route %q is not in routes{}", name, m.ID, m.Route)
			}
		}
	}
}
