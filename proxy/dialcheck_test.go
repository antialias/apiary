package main

import (
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"testing"
)

// The dial check's golden fixtures (quota-model 50 §5.1). The brain's
// test_exporter.py DialCheckGoldenTest runs the same file through its own
// evaluator, so the two agree by test (§2.12 item 8). Regenerate the file, never
// hand-edit one side.
type goldenDoc struct {
	Now       float64 `json:"now"`
	StaleMaxS float64 `json:"stale_max_s"`
	Cases     []struct {
		Name     string                 `json:"name"`
		Route    routePolicy            `json:"route"`
		Account  string                 `json:"account"`
		Standing *acctStanding          `json:"standing"`
		Pots     map[string]potSettings `json:"pots"`
		Ledger   *checkLedger           `json:"ledger"`
		Request  *checkRequest          `json:"request"` // null: the brain's own check
		Want     dialCheck              `json:"want"`
	} `json:"cases"`
	Verdicts []struct {
		Name   string      `json:"name"`
		Checks []dialCheck `json:"checks"`
		Want   int         `json:"want"`
	} `json:"verdicts"`
	PaidOrders []struct {
		Name       string   `json:"name"`
		PayOrder   []string `json:"pay_order"`
		Candidates []struct {
			Pot      string `json:"pot"`
			Priority int    `json:"priority"`
		} `json:"candidates"`
		Want []string `json:"want"`
	} `json:"paid_orders"`
	Unavailable []struct {
		Name   string `json:"name"`
		Routes []struct {
			routePolicy
			Accounts []string `json:"accounts"`
		} `json:"routes"`
		Standings map[string]*acctStanding `json:"standings"`
		Pots      map[string]potSettings   `json:"pots"`
		Ledger    *checkLedger             `json:"ledger"`
		Request   *checkRequest            `json:"request"`
		// Given is a check neither evaluator makes from a standing (this
		// proxy's no (blocked), from a not_entitled record), by route and
		// account, used in place of the evaluator's own.
		Given map[string]map[string]dialCheck `json:"given"`
		Want  json.RawMessage                 `json:"want"`
	} `json:"unavailable"`
}

// orZero is a fixture's request, the zero request (the brain's own check)
// for null.
func (r *checkRequest) orZero() checkRequest {
	if r == nil {
		return checkRequest{}
	}
	return *r
}

func loadGolden(t *testing.T) goldenDoc {
	t.Helper()
	b, err := os.ReadFile("testdata/dialcheck_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var d goldenDoc
	if err := json.Unmarshal(b, &d); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestDialCheckGolden(t *testing.T) {
	d := loadGolden(t)
	if len(d.Cases) < 40 {
		t.Fatalf("only %d golden cases loaded", len(d.Cases))
	}
	for _, c := range d.Cases {
		t.Run(c.Name, func(t *testing.T) {
			got := evalDialCheck(c.Route, c.Account, c.Standing, c.Pots, c.Ledger, d.StaleMaxS, d.Now, c.Request.orZero())
			if got != c.Want {
				t.Fatalf("got %+v (%s), want %+v (%s)", got, got, c.Want, c.Want)
			}
		})
	}
}

func TestDialCheckGoldenVerdicts(t *testing.T) {
	d := loadGolden(t)
	for _, v := range d.Verdicts {
		t.Run(v.Name, func(t *testing.T) {
			// A member's verdict is its best account's check: the first that
			// no other beats under checkLess, the order every refusal sorts by.
			best := -1
			for i, c := range v.Checks {
				if best < 0 || checkLess(c, v.Checks[best]) {
					best = i
				}
			}
			if best != v.Want {
				t.Fatalf("checkLess picks %d, want %d", best, v.Want)
			}
		})
	}
}

func TestDialCheckGoldenPaidOrder(t *testing.T) {
	d := loadGolden(t)
	for _, o := range d.PaidOrders {
		t.Run(o.Name, func(t *testing.T) {
			cs := o.Candidates
			sort.SliceStable(cs, func(i, j int) bool {
				return payOrderLess(cs[i].Pot, cs[i].Priority, cs[j].Pot, cs[j].Priority, o.PayOrder)
			})
			var got []string
			for _, c := range cs {
				got = append(got, c.Pot)
			}
			if len(got) != len(o.Want) {
				t.Fatalf("got %v, want %v", got, o.Want)
			}
			for i := range got {
				if got[i] != o.Want[i] {
					t.Fatalf("got %v, want %v", got, o.Want)
				}
			}
		})
	}
}

// The until round-trips in the fixture's three forms.
func TestCheckUntilJSON(t *testing.T) {
	for _, in := range []string{`1800000000`, `"until_paid"`, `null`} {
		var u checkUntil
		if err := json.Unmarshal([]byte(in), &u); err != nil {
			t.Fatal(err)
		}
		out, _ := json.Marshal(u)
		if string(out) != in {
			t.Fatalf("%s round-tripped to %s", in, out)
		}
	}
}

// The unavailable reply's body (§2.8, C3): each evaluator runs its own dial
// check per (route, account) and builds the body from those checks, so the
// brain's 409 and this proxy's walk-time reply say the same thing.
func TestDialCheckGoldenUnavailable(t *testing.T) {
	d := loadGolden(t)
	if len(d.Unavailable) < 10 {
		t.Fatalf("only %d unavailable cases loaded", len(d.Unavailable))
	}
	for _, c := range d.Unavailable {
		t.Run(c.Name, func(t *testing.T) {
			var routes []unavailRoute
			checks := map[string]map[string]dialCheck{}
			standings := map[string]map[string]*acctStanding{}
			for _, r := range c.Routes {
				routes = append(routes, unavailRoute{policy: r.routePolicy, accounts: r.Accounts, req: c.Request.orZero()})
				checks[r.ID], standings[r.ID] = map[string]dialCheck{}, map[string]*acctStanding{}
				for _, a := range r.Accounts {
					checks[r.ID][a] = evalDialCheck(r.routePolicy, a, c.Standings[a], c.Pots, c.Ledger, d.StaleMaxS, d.Now, c.Request.orZero())
					if g, ok := c.Given[r.ID][a]; ok {
						checks[r.ID][a] = g
					}
					standings[r.ID][a] = c.Standings[a]
				}
			}
			b, err := json.Marshal(unavailableBody(routes, checks, standings, d.Now, c.Pots, c.Ledger))
			if err != nil {
				t.Fatal(err)
			}
			var got, want any
			_ = json.Unmarshal(b, &got)
			_ = json.Unmarshal(c.Want, &want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got  %s\nwant %s", b, c.Want)
			}
		})
	}
}

// An anthropic-api key has no quota to read, so its reading opens an
// unmetered allowance beside a shut money pot. Like the brain's dial_check,
// that open allowance is the whole answer: included, with no pay on the
// route. Only an api account whose allowance is not open walks apiCheck.
func TestDialCheckAPIKeyWithOpenAllowanceIsIncluded(t *testing.T) {
	now := 1_800_000_000.0
	open, shut := true, false
	st := acctStanding{Provider: "anthropic-api", Kind: "api", AsOf: &now}
	st.Allowance.Open = &open
	st.Money = map[string]moneyPot{"balance": {Open: &shut}}
	route := routePolicy{ID: "lane:sonnet", Scope: "Sonnet"}
	if c := evalDialCheck(route, "key-a", &st, nil, nil, 1800, now, checkRequest{}); c.Check != checkIncluded {
		t.Fatalf("open allowance: %+v, want included", c)
	}
	st.Allowance.Open = nil
	if c := evalDialCheck(route, "key-a", &st, nil, nil, 1800, now, checkRequest{}); c.Check != checkNo || c.Reason != "pay" {
		t.Fatalf("no allowance: %+v, want no (pay)", c)
	}
}
