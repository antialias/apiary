package main

// Session facts' reset_now (quota-model 69 §6.3): the session's selection's
// first wise or probably wise event by rank, while the selection is out now,
// from the brain's own /preferred (testdata/preferred_c2.json) as edited per
// case; null whenever an input is missing or says the verdict cannot be
// trusted.

import (
	"reflect"
	"testing"
)

// sfResetEvents is a selection's events as the brain sorts them, by (account,
// id), so the list order is not the rank order: acct-c's unsized event ranks
// first, acct-b's probably wise one second, acct-a's wise one third.
func sfResetEvents() []any {
	return []any{
		map[string]any{"id": "e:full", "account": "acct-a@example.com", "label": "One full limit reset",
			"trigger": "owner", "verdict": "wise", "wise_share": 0.97, "rank": 3, "step": 2},
		map[string]any{"id": "e:fable", "account": "acct-b@example.com", "label": "Fable usage reset",
			"trigger": "owner", "verdict": "probably_wise", "wise_share": 0.84, "rank": 2, "step": 1},
		map[string]any{"id": "e:late", "account": "acct-b@example.com", "label": "A later reset",
			"trigger": "owner", "verdict": "probably_wasteful", "wise_share": 0.3, "rank": 4, "step": nil},
		map[string]any{"id": "e:unsized", "account": "acct-c@example.com", "label": "Monthly reset",
			"trigger": "owner", "verdict": "unsized", "rank": 1, "step": nil},
	}
}

// sfOutNow makes frontier out now with a learned, fresh forecast and the
// events given.
func sfOutNow(p map[string]any, events any) {
	sfSel(p, "frontier", map[string]any{"room": 0.0, "events": events,
		"forecast": map[string]any{"basis": "learned", "stale": false, "computed_at": 1799999000,
			"horizon_at": 1800604800, "samples": 400}})
}

func sfResetNow(t *testing.T, m map[string]any) any {
	t.Helper()
	r, has := m["reset_now"]
	if !has {
		t.Fatalf("the reply carries no reset_now: %v", m)
	}
	return r
}

// The first wise or probably wise event by rank, not by list order and not
// the first event by rank whatever its verdict; the seen and the unseen
// replies both carry their selection's.
func TestSessionFactsResetNowIsTheFirstPlannedByRank(t *testing.T) {
	sfBrainDoc(t, func(p map[string]any) { sfOutNow(p, sfResetEvents()) })
	sfServeFrontier()
	sfServeOpus()
	// Another selection's events are not this one's: lane:opus is out in the
	// fixture too, with events null.
	if got := sfResetNow(t, sfGet(t, "sess-lane", "claude-opus-5-5")); got != nil {
		t.Fatalf("lane:opus reset_now = %v, want null", got)
	}
	want := map[string]any{"account": "acct-b", "label": "Fable usage reset"}
	for _, m := range []map[string]any{sfGet(t, "sess-class", "claude-frontier"), sfGet(t, "unseen", "claude-frontier")} {
		if got := sfResetNow(t, m); !reflect.DeepEqual(got, want) {
			t.Fatalf("reset_now = %v, want %v", got, want)
		}
	}
}

// A wise event alone, ranked behind nothing planned, is the one.
func TestSessionFactsResetNowWise(t *testing.T) {
	sfBrainDoc(t, func(p map[string]any) { sfOutNow(p, sfResetEvents()[:1]) })
	sfServeFrontier()
	want := map[string]any{"account": "acct-a", "label": "One full limit reset"}
	if got := sfResetNow(t, sfGet(t, "sess-class", "claude-frontier")); !reflect.DeepEqual(got, want) {
		t.Fatalf("reset_now = %v, want %v", got, want)
	}
}

// The account is named as the asks name it: "an account" when the only name
// is an address (here its local part is shared with another account).
func TestSessionFactsResetNowNamesNoAddress(t *testing.T) {
	sfBrainDoc(t, func(p map[string]any) {
		sfOutNow(p, []any{map[string]any{"id": "e:x", "account": "acct-a@example.test",
			"label": "One full limit reset", "verdict": "wise", "rank": 1}})
	})
	sfServeFrontier()
	want := map[string]any{"account": "an account", "label": "One full limit reset"}
	if got := sfResetNow(t, sfGet(t, "sess-class", "claude-frontier")); !reflect.DeepEqual(got, want) {
		t.Fatalf("reset_now = %v, want %v", got, want)
	}
}

// Null for every case where there is none or it is unknown.
func TestSessionFactsResetNowNull(t *testing.T) {
	forecast := func(p map[string]any, f map[string]any) { sfSel(p, "frontier", map[string]any{"forecast": f}) }
	for _, c := range []struct {
		name  string
		edit  func(p map[string]any)
		after func()
	}{
		{name: "the row has room", edit: func(p map[string]any) { sfSel(p, "frontier", map[string]any{"room": 12.5}) }},
		{name: "room null", edit: func(p map[string]any) { sfSel(p, "frontier", map[string]any{"room": nil}) }},
		{name: "room absent", edit: func(p map[string]any) {
			delete(sfFleetSelections(p)["frontier"].(map[string]any), "room")
		}},
		{name: "brain unreachable", after: func() { lan.pref.mu.Lock(); lan.pref.ok = false; lan.pref.mu.Unlock() }},
		{name: "no fleet", edit: func(p map[string]any) { delete(p, "fleet") }},
		{name: "unreadable fleet", edit: func(p map[string]any) {
			sfSel(p, "frontier", map[string]any{"more_at": 1800000000.5})
		}},
		{name: "no entry", edit: func(p map[string]any) { delete(sfFleetSelections(p), "frontier") }},
		{name: "events null", edit: func(p map[string]any) { sfSel(p, "frontier", map[string]any{"events": nil}) }},
		{name: "events absent", edit: func(p map[string]any) {
			delete(sfFleetSelections(p)["frontier"].(map[string]any), "events")
		}},
		{name: "events empty", edit: func(p map[string]any) { sfSel(p, "frontier", map[string]any{"events": []any{}}) }},
		{name: "no verdicts", edit: func(p map[string]any) {
			// An older brain: events without a valuation.
			sfSel(p, "frontier", map[string]any{"events": []any{map[string]any{"id": "e:full",
				"account": "acct-a@example.com", "label": "One full limit reset", "trigger": "owner"}}})
		}},
		{name: "nothing planned", edit: func(p map[string]any) {
			sfSel(p, "frontier", map[string]any{"events": sfResetEvents()[2:]})
		}},
		{name: "planned with no rank", edit: func(p map[string]any) {
			sfSel(p, "frontier", map[string]any{"events": []any{map[string]any{"account": "acct-a@example.com",
				"label": "One full limit reset", "verdict": "wise", "rank": nil}}})
		}},
		{name: "a small room is not out", edit: func(p map[string]any) { sfSel(p, "frontier", map[string]any{"room": 0.5}) }},
		{name: "the first planned has no label", edit: func(p map[string]any) {
			sfSel(p, "frontier", map[string]any{"events": []any{map[string]any{"account": "acct-a@example.com",
				"label": nil, "verdict": "wise", "rank": 1}, sfResetEvents()[1]}})
		}},
		{name: "the first planned has no account", edit: func(p map[string]any) {
			sfSel(p, "frontier", map[string]any{"events": []any{map[string]any{"account": "",
				"label": "One full limit reset", "verdict": "wise", "rank": 1}, sfResetEvents()[1]}})
		}},
		{name: "no history", edit: func(p map[string]any) {
			forecast(p, map[string]any{"basis": "no history", "stale": false})
		}},
		{name: "not yet computed", edit: func(p map[string]any) {
			forecast(p, map[string]any{"basis": "not yet computed", "stale": false})
		}},
		{name: "stale", edit: func(p map[string]any) { forecast(p, map[string]any{"basis": "learned", "stale": true}) }},
		{name: "stale absent", edit: func(p map[string]any) { forecast(p, map[string]any{"basis": "learned"}) }},
		{name: "no forecast", edit: func(p map[string]any) {
			delete(sfFleetSelections(p)["frontier"].(map[string]any), "forecast")
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			sfBrainDoc(t, func(p map[string]any) {
				sfOutNow(p, sfResetEvents())
				if c.edit != nil {
					c.edit(p)
				}
			})
			sfServeFrontier()
			if c.after != nil {
				c.after()
			}
			for _, m := range []map[string]any{sfGet(t, "sess-class", "claude-frontier"), sfGet(t, "unseen", "claude-frontier")} {
				if got := sfResetNow(t, m); got != nil {
					t.Fatalf("reset_now = %v, want null", got)
				}
			}
		})
	}
}

// A session with no selection (a refused id) has none, and no problem.
func TestSessionFactsResetNowNoSelection(t *testing.T) {
	sfBrainDoc(t, func(p map[string]any) { sfOutNow(p, sfResetEvents()) })
	if got := sfResetNow(t, sfGet(t, "unseen", "claude-nosuchclass")); got != nil {
		t.Fatalf("reset_now = %v, want null", got)
	}
}

// Events or a forecast the proxy cannot read cost reset_now alone: the room,
// the asks and the problems are what they would be with none sent.
func TestSessionFactsResetNowUnreadableKeepsTheFigure(t *testing.T) {
	planned := func(rank any) map[string]any {
		return map[string]any{"id": "e:full", "account": "acct-a@example.com", "label": "One full limit reset",
			"verdict": "wise", "rank": rank}
	}
	for _, c := range []struct {
		name string
		edit func(p map[string]any)
	}{
		{name: "events not a list", edit: func(p map[string]any) { sfSel(p, "frontier", map[string]any{"events": "soon"}) }},
		{name: "an event not an object", edit: func(p map[string]any) {
			sfSel(p, "frontier", map[string]any{"events": []any{planned(1), 7}})
		}},
		{name: "a null event among good ones", edit: func(p map[string]any) {
			sfSel(p, "frontier", map[string]any{"events": []any{planned(1), nil}})
		}},
		{name: "a rank of the wrong type", edit: func(p map[string]any) {
			sfSel(p, "frontier", map[string]any{"events": []any{planned("first")}})
		}},
		{name: "a fractional rank", edit: func(p map[string]any) {
			sfSel(p, "frontier", map[string]any{"events": []any{planned(1.5)}})
		}},
		{name: "a verdict of the wrong type", edit: func(p map[string]any) {
			e := planned(1)
			e["verdict"] = true
			sfSel(p, "frontier", map[string]any{"events": []any{e}})
		}},
		{name: "forecast not an object", edit: func(p map[string]any) {
			sfSel(p, "frontier", map[string]any{"forecast": []any{"learned"}})
		}},
		{name: "stale of the wrong type", edit: func(p map[string]any) {
			sfSel(p, "frontier", map[string]any{"forecast": map[string]any{"basis": "learned", "stale": "no"}})
		}},
		{name: "basis of the wrong type", edit: func(p map[string]any) {
			sfSel(p, "frontier", map[string]any{"forecast": map[string]any{"basis": 1, "stale": false}})
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			// The same document with the reset fields taken out: what the rest
			// of the reply must stay.
			load, _ := sfBrainDoc(t, func(p map[string]any) {
				sfOutNow(p, sfResetEvents())
				sel := sfFleetSelections(p)["frontier"].(map[string]any)
				delete(sel, "events")
				delete(sel, "forecast")
			})
			sfServeFrontier()
			want := []map[string]any{sfGet(t, "sess-class", "claude-frontier"), sfGet(t, "unseen", "claude-frontier")}
			load(func(p map[string]any) { sfOutNow(p, sfResetEvents()); c.edit(p) })
			for i, m := range []map[string]any{sfGet(t, "sess-class", "claude-frontier"), sfGet(t, "unseen", "claude-frontier")} {
				if got := sfResetNow(t, m); got != nil {
					t.Fatalf("reset_now = %v, want null", got)
				}
				if m["room"] == nil || !reflect.DeepEqual(m, want[i]) {
					t.Fatalf("reply = %v\nwant %v", m, want[i])
				}
			}
		})
	}
}
