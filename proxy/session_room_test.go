package main

// Session facts' room (quota-model 65 §6, §8 step 4): the selection's figure
// from the brain's `fleet`, passed on unchanged, from the brain's own
// /preferred (testdata/preferred_c2.json) as the proxy decodes it.

import (
	"fmt"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

// sfClock makes nowFn a clock the test sets, at start; restored after the
// test. Atomic, so a stream's reader may read it while the test moves it.
func sfClock(t *testing.T, start time.Time) (set func(time.Time)) {
	t.Helper()
	var at atomic.Int64
	at.Store(start.UnixNano())
	oldNow := nowFn
	nowFn = func() time.Time { return time.Unix(0, at.Load()).UTC() }
	t.Cleanup(func() { nowFn = oldNow })
	return func(v time.Time) { at.Store(v.UnixNano()) }
}

// sfBrainDoc loads the brain's document, after edit, into a fresh decision
// through the proxy's own decode, with an empty session table, the clock at
// sfNow. Returns a loader for a later document and the clock's setter.
func sfBrainDoc(t *testing.T, edit func(p map[string]any)) (load func(edit func(p map[string]any)), setNow func(time.Time)) {
	t.Helper()
	setNow = sfClock(t, sfNow)
	set := brainServing(t)
	load = func(edit func(p map[string]any)) {
		p := brainPreferred(t)
		if edit != nil {
			edit(p)
		}
		set(p)
		fetchPreferred()
		lan.pref.mu.RLock()
		ok := lan.pref.ok
		lan.pref.mu.RUnlock()
		if !ok {
			t.Fatal("the brain's document did not decode")
		}
	}
	load(edit)
	lan.sessFactsMu.Lock()
	oldFacts := lan.sessFacts
	lan.sessFacts = map[string]sessionFact{}
	lan.sessFactsMu.Unlock()
	t.Cleanup(func() {
		lan.sessFactsMu.Lock()
		lan.sessFacts = oldFacts
		lan.sessFactsMu.Unlock()
	})
	return load, setNow
}

// sfFleetSelections is the document's fleet.selections, to edit.
func sfFleetSelections(p map[string]any) map[string]any {
	return p["fleet"].(map[string]any)["selections"].(map[string]any)
}

// sfServeFrontier and sfServeOpus are a session on the frontier class and one
// naming opus directly (the bare lane), each served on a made-up account.
func sfServeFrontier() {
	sfServe("sess-class", "claude-frontier", sessionFact{class: "frontier", member: "lane:fable",
		account: "acct-a@example.com", model: "claude-fable-5-1", route: "frontier/lane:fable",
		tier: "included", provider: "anthropic", lane: "fable"})
}

func sfServeOpus() {
	sfServe("sess-lane", "claude-opus-5-5", sessionFact{account: "acct-b@example.com",
		model: "claude-opus-5-5", route: "lane:opus", tier: "included", provider: "anthropic", lane: "opus"})
}

// sfNullRoom fails unless the reply carries room, as null.
func sfNullRoom(t *testing.T, m map[string]any, what string) {
	t.Helper()
	if r, has := m["room"]; !has || r != nil {
		t.Fatalf("%s: room = %v (present %v), want null", what, r, has)
	}
}

func TestSessionFactsRoomOfAClass(t *testing.T) {
	sfBrainDoc(t, func(p map[string]any) {
		// Made-up numbers, so a pass-through that recomputed or rounded shows.
		f := sfFleetSelections(p)["frontier"].(map[string]any)
		f["capacity"], f["room"], f["left_pct"] = 2780.13, 2021.07, 72.7
	})
	sfServeFrontier()
	m := sfGet(t, "sess-class", "claude-frontier")
	// The counts and times are the fixture's own, passed on: the times as ISO.
	want := map[string]any{"selection": "frontier", "unit": "api_usd", "capacity": 2780.13,
		"room": 2021.07, "left_pct": 72.7, "heard_at": isoTime(sfNow),
		"coverage": map[string]any{"counted": 0.0, "learning": 4.0, "empty": 2.0, "unread": 1.0, "stale": 0.0,
			"not_quota": 0.0},
		"more_at": nil, "back_at": isoTime(time.Unix(1800604800, 0))}
	if !reflect.DeepEqual(m["room"], want) {
		t.Fatalf("room = %v, want %v", m["room"], want)
	}
}

func TestSessionFactsRoomOfABareLane(t *testing.T) {
	sfBrainDoc(t, nil)
	sfServeOpus()
	m := sfGet(t, "sess-lane", "claude-opus-5-5")
	// The brain's own entry: nothing sized, so capacity 0 and left_pct null.
	want := map[string]any{"selection": "lane:opus", "unit": "api_usd", "capacity": 0.0,
		"room": 0.0, "left_pct": nil, "heard_at": isoTime(sfNow),
		"coverage": map[string]any{"counted": 0.0, "learning": 2.0, "empty": 1.0, "unread": 1.0, "stale": 0.0,
			"not_quota": 0.0},
		"more_at": nil, "back_at": isoTime(time.Unix(4089225600, 0))}
	if !reflect.DeepEqual(m["room"], want) {
		t.Fatalf("room = %v, want %v", m["room"], want)
	}
	// The class session's selection is its own, not the lane's.
	sfServeFrontier()
	if r := sfGet(t, "sess-class", "claude-frontier")["room"].(map[string]any); r["selection"] != "frontier" {
		t.Fatalf("class room = %v", r)
	}
}

func TestSessionFactsRoomOfASelectionTheBrainLacks(t *testing.T) {
	sfBrainDoc(t, func(p map[string]any) { delete(sfFleetSelections(p), "frontier") })
	sfServeFrontier()
	sfServeOpus()
	sfNullRoom(t, sfGet(t, "sess-class", "claude-frontier"), "sess-class")
	// The other selections still show theirs.
	if r, _ := sfGet(t, "sess-lane", "claude-opus-5-5")["room"].(map[string]any); r == nil {
		t.Fatal("lane:opus lost its room")
	}
}

func TestSessionFactsRoomWithoutFleet(t *testing.T) {
	load, _ := sfBrainDoc(t, nil)
	sfServeFrontier()
	if sfGet(t, "sess-class", "claude-frontier")["room"] == nil {
		t.Fatal("no room from a document with fleet")
	}
	// A later document without fleet (a brain older than 67 push 2): the
	// last figure is not kept.
	load(func(p map[string]any) { delete(p, "fleet") })
	sfServeOpus()
	sfNullRoom(t, sfGet(t, "sess-class", "claude-frontier"), "sess-class")
	sfNullRoom(t, sfGet(t, "sess-lane", "claude-opus-5-5"), "sess-lane")
}

// A fleet without a unit: the figure's unit is null, never "".
func TestSessionFactsRoomWithoutUnit(t *testing.T) {
	sfBrainDoc(t, func(p map[string]any) { delete(p["fleet"].(map[string]any), "unit") })
	sfServeFrontier()
	r, _ := sfGet(t, "sess-class", "claude-frontier")["room"].(map[string]any)
	if r == nil {
		t.Fatal("no room")
	}
	if u, has := r["unit"]; !has || u != nil {
		t.Fatalf("unit = %#v (present %v), want null", u, has)
	}
}

// A fleet the proxy cannot read costs the sessions their room and nothing
// else: the document's routing applies, and the last good figure goes.
func TestSessionFactsRoomUnreadableFleetStillApplies(t *testing.T) {
	probes := map[string]func(p map[string]any){
		"unit is a number": func(p map[string]any) { p["fleet"].(map[string]any)["unit"] = 5 },
		"a selection is a list": func(p map[string]any) {
			sfFleetSelections(p)["frontier"] = []any{1, 2}
		},
	}
	for name, probe := range probes {
		t.Run(name, func(t *testing.T) {
			load, _ := sfBrainDoc(t, nil)
			sfServeFrontier()
			if sfGet(t, "sess-class", "claude-frontier")["room"] == nil {
				t.Fatal("no room from a readable fleet")
			}
			load(func(p map[string]any) {
				p["account"] = "after-probe"
				probe(p)
			})
			lan.pref.mu.RLock()
			ok, account, routes, ledger, fleet := lan.pref.ok, lan.pref.account, len(lan.pref.routes), lan.pref.ledger, lan.pref.fleet
			lan.pref.mu.RUnlock()
			if !ok || account != "after-probe" || routes == 0 || ledger == nil {
				t.Fatalf("routing state not applied: ok=%v account=%q routes=%d ledger=%v", ok, account, routes, ledger)
			}
			if fleet != nil {
				t.Fatalf("fleet = %+v, want nil", fleet)
			}
			m := sfGet(t, "sess-class", "claude-frontier")
			sfNullRoom(t, m, "sess-class")
			// Reachable, so only the missing figure is said (68 §4.5).
			if p, _ := m["problems"].([]any); len(p) != 1 || p[0] != problemNoFleet {
				t.Fatalf("problems = %v, want only %q", p, problemNoFleet)
			}
		})
	}
}

// The figure's heard_at is when the document carrying it was applied:
// keepalives on the stream do not move it, nor does a document the proxy
// rejects (which leaves the last figure). The next good document does.
func TestSessionFactsRoomStampIsTheAppliedDocuments(t *testing.T) {
	t1, t2 := sfNow, sfNow.Add(10*time.Minute)
	setNow := sfClock(t, t1)
	step := make(chan struct{})
	streamBrain(t, time.Minute, 10*time.Millisecond, func(w http.ResponseWriter, r *http.Request, n int) {
		sseOpen(w)
		ssePreferred(t, w, 1, brainPreferred(t))
		wait := func() bool {
			select {
			case <-step:
				return true
			case <-r.Context().Done():
				return false
			}
		}
		if !wait() {
			return
		}
		for i := 0; i < 3; i++ {
			fmt.Fprint(w, ": keepalive\n")
			w.(http.Flusher).Flush()
			time.Sleep(5 * time.Millisecond)
		}
		if !wait() {
			return
		}
		bad := brainPreferred(t)
		delete(bad, "routes")
		sfFleetSelections(bad)["frontier"].(map[string]any)["room"] = 999.0
		ssePreferred(t, w, 2, bad)
		if !wait() {
			return
		}
		good := brainPreferred(t)
		sfFleetSelections(good)["frontier"].(map[string]any)["room"] = 7.0
		ssePreferred(t, w, 3, good)
		<-r.Context().Done()
	})
	stamp := func() (heard, room string) {
		t.Helper()
		r, _, _ := lan.roomFor("frontier")
		if r == nil || r.HeardAt == nil {
			t.Fatalf("room = %+v", r)
		}
		return *r.HeardAt, string(r.Room)
	}
	waitUntil(t, "the first document", func() bool { ok, _, _ := prefState(); return ok })
	if h, _ := stamp(); h != isoTime(t1) {
		t.Fatalf("heard_at = %s, want the applied %s", h, isoTime(t1))
	}
	setNow(t2)

	_, _, before := prefState()
	step <- struct{}{}
	waitUntil(t, "a keepalive heard", func() bool { _, _, h := prefState(); return h.After(before) })
	if h, _ := stamp(); h != isoTime(t1) {
		t.Fatalf("after keepalives heard_at = %s, want still %s", h, isoTime(t1))
	}

	step <- struct{}{}
	waitUntil(t, "the rejected document", func() bool { ok, _, _ := prefState(); return !ok })
	// The view is not ok, so no figure is served (68 §4.5); what is held is
	// still the last good document's, with its stamp.
	if r, _, _ := lan.roomFor("frontier"); r != nil {
		t.Fatalf("room while the brain's view is not ok = %+v, want none", r)
	}
	lan.pref.mu.RLock()
	heldAt, held := lan.pref.fleetAt, lan.pref.fleet
	lan.pref.mu.RUnlock()
	if !heldAt.Equal(t1) || held == nil || string(held.Selections["frontier"].Room) == "999" {
		t.Fatalf("after a rejected document the held figure is stamped %s, want the last good one at %s", heldAt, t1)
	}

	step <- struct{}{}
	waitUntil(t, "the next good document", func() bool { ok, _, _ := prefState(); return ok })
	if h, room := stamp(); h != isoTime(t2) || room != "7" {
		t.Fatalf("after the next document heard_at = %s room = %s, want %s and 7", h, room, isoTime(t2))
	}
}

// ---- through the real serve path ----

// sfE2EFleet is a fleet whose every figure differs, so a session shown the
// wrong selection's figure shows.
func sfE2EFleet() map[string]any {
	sel := func(capacity, room float64) map[string]any {
		return map[string]any{"capacity": capacity, "room": room, "left_pct": 100 * room / capacity,
			"accounts": []any{}, "stale": []any{}, "resting": []any{}, "listed": []any{}}
	}
	return map[string]any{"unit": "api_usd", "selections": map[string]any{
		"frontier":     sel(1000, 400),
		"lane:fable":   sel(800, 200),
		"lane:haiku":   sel(300, 150),
		"lane:default": sel(500, 50),
	}}
}

// sfE2ERoom is the room a running proxy shows for one session and model.
func sfE2ERoom(t *testing.T, base, id, model string) any {
	t.Helper()
	m := sfGetVia(t, base, id, model)
	if m["seen"] != true {
		t.Fatalf("%s %s not seen: %v", id, model, m)
	}
	r, has := m["room"]
	if !has {
		t.Fatalf("%s %s: no room key", id, model)
	}
	return r
}

// sfAppliedAt is the stamp of the document the proxy applied last, as a reply
// states it.
func sfAppliedAt() string {
	lan.pref.mu.RLock()
	defer lan.pref.mu.RUnlock()
	return isoTime(lan.pref.fleetAt)
}

// A class session, bare-lane sessions (a named lane and the default lane),
// each recorded by the real serve path, show their own selection's figure.
func TestE2ESessionFactsRoomThroughTheServePath(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	h.brainMu.Lock()
	h.fleet = sfE2EFleet()
	h.brainMu.Unlock()
	fetchPreferredSync()
	lan.sessFactsMu.Lock()
	oldFacts := lan.sessFacts
	lan.sessFacts = map[string]sessionFact{}
	lan.sessFactsMu.Unlock()
	t.Cleanup(func() { lan.sessFactsMu.Lock(); lan.sessFacts = oldFacts; lan.sessFactsMu.Unlock() })

	for _, c := range []struct{ session, model string }{
		{"sess-room-class", "claude-frontier"},
		{"sess-room-haiku", "claude-haiku-4-5"},
		{"sess-room-default", "claude-sonnet-4-5"},
	} {
		resp := h.postSess(t, c.model, "CLIENT", c.session)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%s: %d", c.model, resp.StatusCode)
		}
		h.hit(t)
	}
	at := sfAppliedAt()
	for _, c := range []struct {
		session, model, selection string
		capacity, room            float64
	}{
		{"sess-room-class", "claude-frontier", "frontier", 1000, 400},
		{"sess-room-haiku", "claude-haiku-4-5", "lane:haiku", 300, 150},
		{"sess-room-default", "claude-sonnet-4-5", "lane:default", 500, 50},
	} {
		// sfE2EFleet sends no counts or times: null, never made up.
		want := map[string]any{"selection": c.selection, "unit": "api_usd", "capacity": c.capacity,
			"room": c.room, "left_pct": 100 * c.room / c.capacity, "heard_at": at,
			"coverage": nil, "more_at": nil, "back_at": nil}
		if got := sfE2ERoom(t, h.proxy.URL, c.session, c.model); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: room = %v, want %v", c.session, got, want)
		}
	}
}
