package main

// Session facts' asks, counts and times (quota-model 68 §4): the asks'
// words and the golden Scry shares, the unseen reply's selection by the
// request path's order, the missing-figure cases, and paying.pinned.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// ---- the wording golden shared with Scry (68 §4.2, §9) ----

// asksWordingPath is Scry's copy of the proxy's wording table, under Scry's
// directory (scryCopy). The proxy's test writes and checks it, so a stale copy
// fails the proxy's build.
const asksWordingPath = "lib/fixtures/asks-wording.json"

// asksWordingAccounts is the roster the short names are worked out over
// (Scry's rule, lib/targetNames.ts): acct-d's local part is shared, so it has
// no short name and reads "an account". Made-up names.
var asksWordingAccounts = []string{"acct-c@example.com", "acct-d@example.com", "acct-d@example.org",
	"max-b@example.com", "acct-nine@example.com", "gpt", "kimi"}

type asksWordingCase struct {
	Note  string   `json:"note"`
	Ask   fleetAsk `json:"ask"`
	Words string   `json:"words"`
}

type asksWording struct {
	Comment  string            `json:"_comment"`
	Accounts []string          `json:"accounts"`
	Cases    []asksWordingCase `json:"cases"`
}

func asksWordingCases() []asksWordingCase {
	i := func(v int64) *int64 { return &v }
	oauth, key := str("oauth"), str("key")
	return []asksWordingCase{
		{Note: "auth, oauth", Ask: fleetAsk{Account: "acct-c@example.com", Kind: "auth", SignIn: oauth}},
		{Note: "auth, oauth, on codex", Ask: fleetAsk{Account: "gpt", Kind: "auth", SignIn: oauth}},
		{Note: "auth, key: kimi is a subscription with a static key", Ask: fleetAsk{Account: "kimi", Kind: "auth", SignIn: key}},
		{Note: "not_entitled with a model, through plainModel", Ask: fleetAsk{Account: "max-b@example.com",
			Kind: "not_entitled", SignIn: oauth, Model: str("claude-fable-5-1"), Until: i(1800021600)}},
		{Note: "an address-only name", Ask: fleetAsk{Account: "acct-d@example.com", Kind: "auth", SignIn: oauth}},
		{Note: "a nine-character short name, auth oauth", Ask: fleetAsk{Account: "acct-nine@example.com", Kind: "auth", SignIn: oauth}},
		{Note: "a nine-character short name, auth key", Ask: fleetAsk{Account: "acct-nine@example.com", Kind: "auth", SignIn: key}},
		{Note: "a nine-character short name, not_entitled: exactly 40 columns", Ask: fleetAsk{Account: "acct-nine@example.com",
			Kind: "not_entitled", SignIn: oauth, Model: str("claude-fable-5-1"), Until: i(1800021600)}},
		{Note: "not_entitled on a pool limit: no model (as built)", Ask: fleetAsk{Account: "max-b@example.com",
			Kind: "not_entitled", SignIn: oauth, Until: i(1800021600)}},
	}
}

// withRoster sets the accounts the brain lists to names, for short names.
func withRoster(t *testing.T, names []string) {
	t.Helper()
	old := lan.pref
	accts := map[string]acctStanding{}
	for _, n := range names {
		accts[n] = acctStanding{}
	}
	lan.pref = &decision{accounts: accts, ok: true}
	t.Cleanup(func() { lan.pref = old })
}

func TestAsksWordingGoldenIsCurrent(t *testing.T) {
	withRoster(t, asksWordingAccounts)
	g := asksWording{Comment: "Written by the switch proxy's TestAsksWordingGoldenIsCurrent (session_asks_test.go) " +
		"from its wording table (quota-model 68 §4.2); regenerate with UPDATE_ASKS_WORDING=1. Each ask is one " +
		"brain ask as /preferred carries it; words is the session-facts reply's string, short names worked " +
		"out over accounts.",
		Accounts: asksWordingAccounts}
	for _, c := range asksWordingCases() {
		c.Words = lan.askWords(c.Ask)
		g.Cases = append(g.Cases, c)
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(g); err != nil {
		t.Fatal(err)
	}
	path := scryCopy(t, asksWordingPath)
	if os.Getenv("UPDATE_ASKS_WORDING") == "1" {
		if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	have, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v: regenerate with UPDATE_ASKS_WORDING=1", err)
	}
	if !bytes.Equal(have, buf.Bytes()) {
		t.Fatalf("%s is stale against the proxy's wording table; regenerate with UPDATE_ASKS_WORDING=1\nwant:\n%s", path, buf.Bytes())
	}
}

// The wording table itself, pinned here so the golden cannot be regenerated
// into new words unnoticed.
func TestAsksWording(t *testing.T) {
	withRoster(t, asksWordingAccounts)
	want := []string{
		"acct-c: login expired, log in again",
		"gpt: login expired, log in again",
		"kimi: key refused, replace the key",
		"max-b: fable 5.1 needs a plan change",
		"an account: login expired, log in again",
		"acct-nine: login expired, log in again",
		"acct-nine: key refused, replace the key",
		"acct-nine: fable 5.1 needs a plan change",
		"max-b: needs a plan change",
	}
	for i, c := range asksWordingCases() {
		got := lan.askWords(c.Ask)
		if got != want[i] {
			t.Errorf("%s: %q, want %q", c.Note, got, want[i])
		}
		if strings.Contains(got, "@") {
			t.Errorf("%s: carries an address: %q", c.Note, got)
		}
		if c.Ask.Kind == "auth" && strings.Contains(c.Note, "nine-character") && utf8.RuneCountInString(got) > 40 {
			t.Errorf("%s: %d columns, over 40", c.Note, utf8.RuneCountInString(got))
		}
	}
	// Keyed on the sign-in, never on the account's kind: the same kimi
	// account, listed as a subscription, reads "key refused".
	lan.pref.accounts["kimi"] = acctStanding{Kind: "subscription"}
	if got := lan.askWords(fleetAsk{Account: "kimi", Kind: "auth", SignIn: str("key")}); got != "kimi: key refused, replace the key" {
		t.Errorf("kimi as a subscription: %q", got)
	}
}

// ---- asks, counts and times in a reply ----

// sfRaw answers one session for one model, as the bytes the handler writes.
func sfRaw(t *testing.T, id, model string) []byte {
	t.Helper()
	rec := httptest.NewRecorder()
	sessionHandler(rec, lanRequest("GET", "/_switch/session?id="+url.QueryEscape(id)+"&model="+url.QueryEscape(model), nil))
	if rec.Code != 200 {
		t.Fatalf("GET session %s: %d %s", id, rec.Code, rec.Body)
	}
	sfNoAddress(t, rec.Body.String())
	return rec.Body.Bytes()
}

func TestSessionFactsAsksOfTheSelection(t *testing.T) {
	sfBrainDoc(t, nil)
	sfServeFrontier()
	sfServeOpus()
	// The fixture's frontier asks for acct-c's sign-in; lane:fable for that
	// and acct-b's fable entitlement too, in the brain's order.
	if got := sfGet(t, "sess-class", "claude-frontier")["asks"]; !reflect.DeepEqual(got, []any{"acct-c: login expired, log in again"}) {
		t.Fatalf("frontier asks = %v", got)
	}
	sfServe("sess-fable", "claude-fable-5-1", sessionFact{account: "acct-a@example.com", model: "claude-fable-5-1",
		route: "lane:fable", tier: "included", provider: "anthropic", lane: "fable"})
	want := []any{"acct-c: login expired, log in again", "acct-b: fable 5.1 needs a plan change"}
	if got := sfGet(t, "sess-fable", "claude-fable-5-1")["asks"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("lane:fable asks = %v, want %v", got, want)
	}
}

// A brain that sends a selection no asks (older than 68 step 1) leaves them
// unknown: null, not an empty list; and an empty list is passed as one.
func TestSessionFactsAsksUnknownAndNone(t *testing.T) {
	load, _ := sfBrainDoc(t, func(p map[string]any) {
		delete(sfFleetSelections(p)["frontier"].(map[string]any), "asks")
	})
	sfServeFrontier()
	m := sfGet(t, "sess-class", "claude-frontier")
	if a, has := m["asks"]; !has || a != nil {
		t.Fatalf("asks = %#v (present %v), want null", a, has)
	}
	if m["room"] == nil {
		t.Fatal("the figure went with the asks")
	}
	load(func(p map[string]any) { sfFleetSelections(p)["frontier"].(map[string]any)["asks"] = []any{} })
	if a := sfGet(t, "sess-class", "claude-frontier")["asks"]; !reflect.DeepEqual(a, []any{}) {
		t.Fatalf("asks = %#v, want []", a)
	}
}

// A time the brain sends as a fraction is not its contract (integer seconds,
// 68 §3.1 as built): the fleet is unreadable, so the figure goes and says so.
func TestSessionFactsFractionalTimeIsUnreadable(t *testing.T) {
	sfBrainDoc(t, func(p map[string]any) { sfFleetSelections(p)["frontier"].(map[string]any)["more_at"] = 1800000000.5 })
	sfServeFrontier()
	m := sfGet(t, "sess-class", "claude-frontier")
	sfNullRoom(t, m, "sess-class")
	if p := m["problems"].([]any); len(p) != 1 || p[0] != problemNoFleet {
		t.Fatalf("problems = %v", p)
	}
}

// ---- the missing-figure cases (68 §4.5) ----

func TestSessionFactsMissingFigureProblems(t *testing.T) {
	t.Run("no fleet", func(t *testing.T) {
		sfBrainDoc(t, func(p map[string]any) { delete(p, "fleet") })
		sfServeFrontier()
		for _, m := range []map[string]any{sfGet(t, "sess-class", "claude-frontier"), sfGet(t, "unseen", "claude-frontier")} {
			sfNullRoom(t, m, "no fleet")
			if !reflect.DeepEqual(m["problems"], []any{problemNoFleet}) || m["asks"] != nil {
				t.Fatalf("problems = %v asks = %v", m["problems"], m["asks"])
			}
		}
	})
	t.Run("no entry", func(t *testing.T) {
		sfBrainDoc(t, func(p map[string]any) { delete(sfFleetSelections(p), "frontier") })
		sfServeFrontier()
		for _, m := range []map[string]any{sfGet(t, "sess-class", "claude-frontier"), sfGet(t, "unseen", "claude-frontier")} {
			sfNullRoom(t, m, "no entry")
			if !reflect.DeepEqual(m["problems"], []any{"the brain has no figure for frontier"}) || m["asks"] != nil {
				t.Fatalf("problems = %v asks = %v", m["problems"], m["asks"])
			}
		}
	})
	t.Run("unreachable", func(t *testing.T) {
		sfBrainDoc(t, nil)
		sfServeFrontier()
		lan.pref.mu.Lock()
		lan.pref.ok = false // as brainFail leaves it: the last fleet still held
		held := lan.pref.fleet != nil
		lan.pref.mu.Unlock()
		if !held {
			t.Fatal("the last fleet is not held")
		}
		for _, m := range []map[string]any{sfGet(t, "sess-class", "claude-frontier"), sfGet(t, "unseen", "claude-frontier")} {
			sfNullRoom(t, m, "unreachable")
			p := m["problems"].([]any)
			if len(p) != 1 || !strings.HasPrefix(p[0].(string), "the proxy cannot reach the brain") || m["asks"] != nil {
				t.Fatalf("problems = %v asks = %v", p, m["asks"])
			}
		}
	})
}

// ---- the unseen reply (68 §4.3) ----

func TestSessionFactsUnseenRoomByTheRequestPath(t *testing.T) {
	sfBrainDoc(t, func(p map[string]any) {
		// Every figure differs, so the wrong selection's shows.
		for name, sel := range sfFleetSelections(p) {
			sel.(map[string]any)["room"] = float64(len(name))
		}
	})
	for _, c := range []struct{ model, selection string }{
		{"claude-frontier", "frontier"},
		{"claude-frontier[1m]", ""}, // the fixture's frontier is not marked [1m]: class-shaped, refused
		{"claude-opus-5-5", "lane:opus"},
		{"claude-opus-5-5[1m]", "lane:opus"},
		{"claude-fable-5-1", "lane:fable"},
		{"claude-mystery-9", "lane:default"},
		{"claude-kimi-k3", ""},            // a hosted provider's deleted direct id: refused
		{"claude-gpt-6.1-sol[1m]", ""},    // likewise
		{"claude-local-qwen-test:7b", ""}, // the local runtime's deleted direct id (#135): refused
		{"claude-nosuchclass", ""},        // class-shaped: refused
	} {
		m := sfGet(t, "never-seen", c.model)
		if m["seen"] != false {
			t.Fatalf("%s: seen", c.model)
		}
		if c.selection == "" {
			sfNullRoom(t, m, c.model)
			if !reflect.DeepEqual(m["asks"], []any{}) || len(m["problems"].([]any)) != 0 {
				t.Errorf("%s: asks = %v problems = %v, want none", c.model, m["asks"], m["problems"])
			}
			continue
		}
		r, _ := m["room"].(map[string]any)
		if r == nil || r["selection"] != c.selection || r["room"] != float64(len(c.selection)) {
			t.Errorf("%s: room = %v, want %s's", c.model, m["room"], c.selection)
		}
		if _, has := m["model"]; has {
			t.Errorf("%s: an unseen reply names a model", c.model)
		}
		if a, ok := m["asks"].([]any); !ok || len(a) == 0 {
			t.Errorf("%s: asks = %v, want the selection's", c.model, m["asks"])
		}
	}
}

// ---- paying.pinned (68 §4.4) ----

func TestSessionFactsPayingPinned(t *testing.T) {
	sfFixture(t)
	paid := sessionFact{class: "capable", member: "gpt/gpt-6.1-sol", account: "gpt", model: "gpt-6.1-sol",
		route: "capable/gpt/gpt-6.1-sol", tier: "paid:gpt/credits", provider: "gpt"}
	pinned := paid
	pinned.pinned = true
	sfServe("sess-pinned", "claude-capable", pinned)
	sfServe("sess-seated", "claude-capable", paid)
	if p := sfGet(t, "sess-pinned", "claude-capable")["paying"].(map[string]any); p["pinned"] != true {
		t.Fatalf("pinned paying = %v", p)
	}
	if p := sfGet(t, "sess-seated", "claude-capable")["paying"].(map[string]any); p["pinned"] != false {
		t.Fatalf("seated paying = %v", p)
	}
	// Not paying: no paying object to carry it.
	inc := pinned
	inc.tier = "included"
	sfServe("sess-pinned-inc", "claude-capable", inc)
	if p := sfGet(t, "sess-pinned-inc", "claude-capable")["paying"]; p != nil {
		t.Fatalf("included paying = %v", p)
	}
}

// Through the real serve path: a pinned class's hop records the pin; a
// seated (bound, not pinned) one does not.
func TestE2ESessionFactsPinnedThroughTheServePath(t *testing.T) {
	h, restore := newE2EClassOn(t, "kimi/kimi-k3", 1.0)
	defer restore()
	h.nearlySpent("kimi", "kimi", 100, true)
	h.payFrom("frontier/kimi/kimi-k3", "kimi/credits")
	fetchPreferredSync()
	// No health store: a pin is the brain's binding fact, not health's.
	armHealth(t, nil)
	lan.sessFactsMu.Lock()
	oldFacts := lan.sessFacts
	lan.sessFacts = map[string]sessionFact{}
	lan.sessFactsMu.Unlock()
	t.Cleanup(func() { lan.sessFactsMu.Lock(); lan.sessFacts = oldFacts; lan.sessFactsMu.Unlock() })

	post := func(session string) {
		t.Helper()
		resp := h.postSess(t, "claude-frontier", "CLIENT", session)
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d", session, resp.StatusCode)
		}
		if hit := <-h.hosted; hit.model != "kimi-k3" {
			t.Fatalf("served %q, want kimi-k3", hit.model)
		}
	}
	post("sess-seated")
	h.bindReason.Store("bind: pinned by operator")
	post("sess-pinned")
	for session, want := range map[string]bool{"sess-seated": false, "sess-pinned": true} {
		f, ok := sfFactOf(session, "claude-frontier")
		if !ok || f.pinned != want {
			t.Errorf("%s: recorded %v pinned=%v, want pinned=%v", session, ok, f.pinned, want)
		}
		m := sfGetVia(t, h.proxy.URL, session, "claude-frontier")
		p, _ := m["paying"].(map[string]any)
		if p == nil || p["pinned"] != want {
			t.Errorf("%s: paying = %v, want pinned=%v", session, m["paying"], want)
		}
	}
}

// ---- the replies golden (68 §9), which the status line's test reads a copy of ----

const sessionFactsGoldenPath = "testdata/session_facts_golden.json"

// sfFixtureNow is testdata/preferred_c2.json's `now`.
var sfFixtureNow = time.Unix(1800000000, 0).UTC()

type sfGoldenCase struct {
	Name  string          `json:"name"`
	Model string          `json:"model"`
	Reply json.RawMessage `json:"reply"`
}

// sfSel sets one selection's figure, counts and times in a document.
func sfSel(p map[string]any, name string, fields map[string]any) {
	sel := sfFleetSelections(p)[name].(map[string]any)
	for k, v := range fields {
		sel[k] = v
	}
}

func sfCoverage(counted, learning, empty, unread, stale, notQuota int) map[string]any {
	return map[string]any{"counted": counted, "learning": learning, "empty": empty, "unread": unread,
		"stale": stale, "not_quota": notQuota}
}

func TestSessionFactsGoldenIsCurrent(t *testing.T) {
	frontierOn := func(session string) {
		sfServe(session, "claude-frontier", sessionFact{class: "frontier", member: "lane:opus",
			account: "acct-b@example.com", model: "claude-opus-5-5", route: "frontier/lane:opus",
			tier: "included", provider: "anthropic", lane: "opus"})
	}
	noAsks := func(p map[string]any) { sfSel(p, "frontier", map[string]any{"asks": []any{}}) }
	cases := []struct {
		name, session, model string
		edit                 func(p map[string]any)
		serve                func()
		after                func()
	}{
		{name: "plenty", session: "sess-plenty", model: "claude-frontier", serve: func() { frontierOn("sess-plenty") },
			edit: func(p map[string]any) {
				noAsks(p)
				sfSel(p, "frontier", map[string]any{"capacity": 646.6, "room": 517.28, "left_pct": 80.0,
					"coverage": sfCoverage(3, 0, 0, 0, 0, 0), "more_at": 1800007200, "back_at": nil})
			}},
		{name: "partial", session: "sess-partial", model: "claude-frontier", serve: func() { frontierOn("sess-partial") },
			edit: func(p map[string]any) {
				noAsks(p)
				sfSel(p, "frontier", map[string]any{"capacity": 646.6, "room": 316.83, "left_pct": 49.0,
					"coverage": sfCoverage(1, 3, 2, 0, 0, 0), "more_at": 1800086400, "back_at": 1800043200})
			}},
		{name: "low", session: "sess-low", model: "claude-frontier", serve: func() { frontierOn("sess-low") },
			edit: func(p map[string]any) {
				noAsks(p)
				sfSel(p, "frontier", map[string]any{"capacity": 646.6, "room": 77.59, "left_pct": 12.0,
					"coverage": sfCoverage(3, 0, 0, 0, 1, 0), "more_at": 1800003600, "back_at": nil})
			}},
		{name: "an ask", session: "sess-ask", model: "claude-frontier", serve: func() { frontierOn("sess-ask") }},
		{name: "paying pinned", session: "sess-paying", model: "claude-frontier",
			serve: func() {
				sfServe("sess-paying", "claude-frontier", sessionFact{class: "frontier", member: "lane:opus",
					account: "acct-a@example.com", model: "claude-opus-5-5", route: "frontier/lane:opus",
					tier: "paid:acct-a@example.com/credits", provider: "anthropic", lane: "opus", pinned: true})
			}},
		{name: "unavailable", session: "sess-unavailable", model: "claude-frontier",
			serve: func() {
				frontierOn("sess-unavailable")
				lan.noteSessionUnavailable("frontier", "sess-unavailable", sfFixtureNow.Add(3*time.Hour))
			}},
		{name: "unseen", session: "sess-unseen", model: "claude-frontier"},
		{name: "no fleet", session: "sess-nofleet", model: "claude-frontier", serve: func() { frontierOn("sess-nofleet") },
			edit: func(p map[string]any) { delete(p, "fleet") }},
		{name: "no entry", session: "sess-noentry", model: "claude-frontier", serve: func() { frontierOn("sess-noentry") },
			edit: func(p map[string]any) { delete(sfFleetSelections(p), "frontier") }},
		{name: "brain unreachable", session: "sess-unreachable", model: "claude-frontier", serve: func() { frontierOn("sess-unreachable") },
			after: func() { lan.pref.mu.Lock(); lan.pref.ok = false; lan.pref.mu.Unlock() }},
	}
	var got []sfGoldenCase
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// At the fixture's own clock, so its readings are as old as it says.
			load, setNow := sfBrainDoc(t, c.edit)
			setNow(sfFixtureNow)
			load(c.edit)
			if c.serve != nil {
				c.serve()
			}
			if c.after != nil {
				c.after()
			}
			got = append(got, sfGoldenCase{Name: c.name, Model: c.model, Reply: bytes.TrimSpace(sfRaw(t, c.session, c.model))})
		})
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(map[string]any{
		"_comment": "Written by the switch proxy's TestSessionFactsGoldenIsCurrent (session_asks_test.go): " +
			"GET /_switch/session replies for quota-model 68 §9's cases, from testdata/preferred_c2.json as edited " +
			"per case. The status line's test reads a copy. Regenerate with UPDATE_SESSION_FACTS_GOLDEN=1.",
		"cases": got,
	}); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("UPDATE_SESSION_FACTS_GOLDEN") == "1" {
		if err := os.WriteFile(sessionFactsGoldenPath, buf.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	have, err := os.ReadFile(sessionFactsGoldenPath)
	if err != nil {
		t.Fatalf("%v: regenerate with UPDATE_SESSION_FACTS_GOLDEN=1", err)
	}
	if !bytes.Equal(have, buf.Bytes()) {
		t.Fatalf("%s is stale; regenerate with UPDATE_SESSION_FACTS_GOLDEN=1\nwant:\n%s", sessionFactsGoldenPath, buf.Bytes())
	}
}

// Counts are the brain's JSON (including a future count), not a proxy
// recomputation; both times pass as the same integer second, expressed in ISO.
func TestSessionFactsCountsAndTimesAreTheBrains(t *testing.T) {
	coverage := map[string]any{"counted": 2, "learning": 3, "empty": 1, "unread": 4,
		"stale": 1, "not_quota": 5, "future_count": 6}
	sfBrainDoc(t, func(p map[string]any) {
		sfSel(p, "frontier", map[string]any{"coverage": coverage, "more_at": 1800000001, "back_at": 1800000002})
	})
	sfServeFrontier()
	for _, m := range []map[string]any{sfGet(t, "sess-class", "claude-frontier"), sfGet(t, "unseen", "claude-frontier")} {
		r := m["room"].(map[string]any)
		b, _ := json.Marshal(coverage)
		var want any
		_ = json.Unmarshal(b, &want)
		if !reflect.DeepEqual(r["coverage"], want) || r["more_at"] != "2027-01-15T08:00:01Z" || r["back_at"] != "2027-01-15T08:00:02Z" {
			t.Fatalf("counts and times = %v", r)
		}
	}
}

func TestSessionFactsUnreadableAskCostsTheFigure(t *testing.T) {
	for _, a := range []map[string]any{
		{"account": "kimi", "kind": "auth", "sign_in": nil, "model": nil, "until": nil},
		{"account": "kimi", "kind": "auth", "sign_in": "new-kind", "model": nil, "until": nil},
		{"account": "kimi", "kind": "new-kind", "sign_in": "key", "model": nil, "until": nil},
	} {
		t.Run(fmt.Sprint(a["kind"], "/", a["sign_in"]), func(t *testing.T) {
			sfBrainDoc(t, func(p map[string]any) { sfSel(p, "frontier", map[string]any{"asks": []any{a}}) })
			sfServeFrontier()
			m := sfGet(t, "sess-class", "claude-frontier")
			sfNullRoom(t, m, "unreadable ask")
			if m["asks"] != nil || !reflect.DeepEqual(m["problems"], []any{problemNoFleet}) {
				t.Fatalf("asks = %v, problems = %v", m["asks"], m["problems"])
			}
		})
	}
}

// Health-off routing must keep its pre-session-facts behavior: a pin is
// recorded as a fact but does not prevent a throttled bound member deranking.
func TestE2ESessionFactsHealthOffPinDeranksThrottle(t *testing.T) {
	h, restore := newE2EClassOn(t, "kimi/kimi-k3", 1.0)
	defer restore()
	armHealth(t, nil)
	h.bindReason.Store("bind: pinned by operator")
	lan.recordThrottle("kimi", 429, "busy", nil)
	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-pin-throttle")
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	f, ok := sfFactOf("sess-pin-throttle", "claude-frontier")
	if !ok || f.member != "lane:fable" {
		t.Fatalf("served = %q (recorded %v), want healthy lane:fable", f.member, ok)
	}
	h.hit(t)
}

// Even with a pin in the binding, a cross-member failover must not record
// the final member as pinned: that member is not the one the pin named.
func TestE2ESessionFactsPinDoesNotFollowFailover(t *testing.T) {
	h, restore := newE2EClassOn(t, "kimi/kimi-k3", 1.0)
	defer restore()
	armHealth(t, nil)
	h.bindReason.Store("bind: pinned by operator")
	h.deny.Store(true)
	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-pin-cross")
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	h.hostedHit(t)
	h.hit(t)
	f, ok := sfFactOf("sess-pin-cross", "claude-frontier")
	if !ok || f.member != "lane:fable" || f.pinned {
		t.Fatalf("member = %q, pinned = %v (recorded %v), want lane:fable not pinned", f.member, f.pinned, ok)
	}
}

// A pinned recovering member can serve a probe rather than a pinned hop;
// its serving record still says pinned when it served the bound member.
func TestE2ESessionFactsPinnedProbe(t *testing.T) {
	c := testClock(t)
	h, restore := newE2EClassOn(t, "kimi/kimi-k3", 1.0)
	defer restore()
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	armEnforce(t)
	h.bindReason.Store("bind: pinned by operator")
	halfOpenK(t, hs, c, "kimi/kimi-k3")
	h.nearlySpent("kimi", "kimi", 100, true)
	h.payFrom("frontier/kimi/kimi-k3", "kimi/credits")
	fetchPreferredSync()
	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-pin-probe")
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	h.hostedHit(t)
	// The decision header identifies the real probe; do not let a plain
	// pinned serve make this test vacuously pass.
	if got := resp.Header.Get("X-Switch-Health"); !strings.Contains(got, "probe") {
		t.Fatalf("health header = %q, want probe", got)
	}
	f, ok := sfFactOf("sess-pin-probe", "claude-frontier")
	if !ok || f.member != "kimi/kimi-k3" || !f.pinned {
		t.Fatalf("member = %q, pinned = %v (recorded %v), want bound kimi pinned", f.member, f.pinned, ok)
	}
	m := sfGetVia(t, h.proxy.URL, "sess-pin-probe", "claude-frontier")
	p, _ := m["paying"].(map[string]any)
	if p == nil || p["pinned"] != true {
		t.Fatalf("probe paying = %v, want pinned", p)
	}
}
