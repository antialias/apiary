package main

// GET /_switch/session (docs/session-facts.md): the facts a status line draws
// for one session, from what this proxy holds.

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

var sfNow = time.Date(2026, 11, 1, 18, 40, 0, 0, time.UTC)

func str(v string) *string { return &v }

func sfEpoch(t time.Time) *float64 { return f64(unixSeconds(t)) }

// sfWindow is a window read 108 s before sfNow.
func sfWindow(period string, scope *string, pct float64, resets time.Time) checkWindow {
	return checkWindow{Period: period, Scope: scope, UsedPct: f64(pct), ResetsAt: sfEpoch(resets),
		AsOf: sfEpoch(sfNow.Add(-108 * time.Second))}
}

// withSessionFacts installs a brain view and an empty session table at sfNow.
func withSessionFacts(t *testing.T, accounts map[string]acctStanding, routes map[string]routePref) {
	t.Helper()
	oldPref, oldBrain, oldNow := lan.pref, brainBase, nowFn
	classRowsMu.Lock()
	oldRows := classRowsSet
	// frontier's window is marked [1m]; capable's and deep's are not.
	classRowsSet = []registryEntry{
		{Name: "frontier", Type: "class", Class: "frontier", ContextWindow: oneMegaMarkFloor},
		{Name: "capable", Type: "class", Class: "capable", ContextWindow: 200000},
		{Name: "deep", Type: "class", Class: "deep", ContextWindow: 200000},
	}
	classRowsMu.Unlock()
	lan.sessFactsMu.Lock()
	oldFacts := lan.sessFacts
	lan.sessFacts = map[string]sessionFact{}
	lan.sessFactsMu.Unlock()
	lan.pref = &decision{accounts: accounts, routes: routes, ok: true, fleet: sfQuietFleet(), fleetAt: sfNow}
	brainBase = "http://brain.test"
	nowFn = func() time.Time { return sfNow }
	t.Cleanup(func() {
		lan.pref, brainBase, nowFn = oldPref, oldBrain, oldNow
		classRowsMu.Lock()
		classRowsSet = oldRows
		classRowsMu.Unlock()
		lan.sessFactsMu.Lock()
		lan.sessFacts = oldFacts
		lan.sessFactsMu.Unlock()
	})
}

// sfQuietFleet is a brain fleet with a figure, and nothing to ask, for every
// selection the session-facts tests route under, so a test about something
// else sees no missing-figure problem.
func sfQuietFleet() *brainFleet {
	sel := fleetSelection{Capacity: json.RawMessage("100"), Room: json.RawMessage("50"),
		LeftPct: json.RawMessage("50"), Asks: []fleetAsk{}}
	fl := &brainFleet{Unit: "api_usd", Selections: map[string]fleetSelection{}}
	for _, s := range []string{"frontier", "capable", "deep", "lane:default", "lane:opus", "lane:fable",
		"lane:haiku", "lane:sonnet"} {
		fl.Selections[s] = sel
	}
	return fl
}

// sfServe records a hop that served session for the model the client asked
// for, keyed as noteServedSession keys it.
func sfServe(session, asked string, f sessionFact) {
	lan.noteSessionServed(session, sessionModelKey(asked), f)
}

// sfFixture is the two sessions the contract is shown with: an Anthropic
// session on fable whose scoped window is over pace, and a GPT session whose
// weekly limit is spent and which pays from codex credits.
func sfFixture(t *testing.T) {
	t.Helper()
	wkReset := sfNow.Add(time.Duration(0.31 * float64(7*24*time.Hour)))
	anth := acctStanding{Provider: "anthropic", Kind: "subscription", AsOf: sfEpoch(sfNow.Add(-108 * time.Second))}
	anth.Allowance.Windows = map[string]checkWindow{
		"five_hour":    sfWindow("5h", nil, 22, sfNow.Add(2*time.Hour)),
		"seven_day":    sfWindow("7d", nil, 64, wkReset),
		"scoped:Fable": sfWindow("7d", str("Fable"), 88, wkReset),
		"scoped:Opus":  sfWindow("7d", str("Opus"), 40, wkReset),
	}
	anth.Money = map[string]moneyPot{"credits": {Open: new(bool)}}
	gpt := acctStanding{Provider: "gpt", Kind: "subscription", AsOf: sfEpoch(sfNow.Add(-60 * time.Second))}
	gpt.Allowance.Windows = map[string]checkWindow{
		"five_hour": sfWindow("5h", nil, 30, sfNow.Add(3*time.Hour)),
		"seven_day": sfWindow("7d", nil, 100, sfNow.Add(73*time.Hour)),
		"scoped:GPT-5.3-Codex-Spark:7d": sfWindow("7d", str("GPT-5.3-Codex-Spark"), 10,
			sfNow.Add(73*time.Hour)),
	}
	gpt.Money = map[string]moneyPot{"credits": {Open: new(bool), Balance: f64(439), Unit: str("codex_credit")}}
	withSessionFacts(t, map[string]acctStanding{
		"max-a@example.com": {Provider: "anthropic", Kind: "subscription"},
		"max-b@example.com": anth,
		"gpt":               gpt,
	}, map[string]routePref{
		"frontier/lane:fable":     {routePolicy: routePolicy{ID: "frontier/lane:fable", Scope: "Fable"}},
		"capable/gpt/gpt-6.1-sol": {routePolicy: routePolicy{ID: "capable/gpt/gpt-6.1-sol", Scope: "gpt-6.1-sol"}},
	})
	sfServe("sess-fable", "claude-frontier[1m]", sessionFact{class: "frontier", member: "lane:fable",
		account: "max-b@example.com", model: "claude-fable-5-1", route: "frontier/lane:fable",
		tier: "included", provider: "anthropic", lane: "fable"})
	sfServe("sess-gpt", "claude-capable", sessionFact{class: "capable", member: "gpt/gpt-6.1-sol",
		account: "gpt", model: "gpt-6.1-sol", route: "capable/gpt/gpt-6.1-sol", tier: "paid:gpt/credits",
		provider: "gpt"})
}

// sfGet answers one session id for one model as JSON, through the handler.
func sfGet(t *testing.T, id, model string) map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	sessionHandler(rec, lanRequest("GET", "/_switch/session?id="+id+"&model="+url.QueryEscape(model), nil))
	if rec.Code != 200 {
		t.Fatalf("GET session %s: %d %s", id, rec.Code, rec.Body)
	}
	sfNoAddress(t, rec.Body.String())
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// sfNoAddress: no reply ever carries an email address, in any field.
func sfNoAddress(t *testing.T, body string) {
	t.Helper()
	if strings.Contains(body, "@") {
		t.Fatalf("the reply carries an address: %s", body)
	}
}

func TestSessionFactsServedClassSession(t *testing.T) {
	sfFixture(t)
	m := sfGet(t, "sess-fable", "claude-frontier[1m]")
	if b, err := json.MarshalIndent(m, "", "  "); err == nil {
		t.Logf("sess-fable:\n%s", b)
	}
	if m["v"] != 2.0 || m["seen"] != true || m["class"] != "claude-frontier" {
		t.Fatalf("head = %v %v %v", m["v"], m["seen"], m["class"])
	}
	if got := m["model"].(map[string]any)["name"]; got != "fable 5.1" {
		t.Fatalf("model.name = %v, want fable 5.1", got)
	}
	want := map[string]any{"name": "max-b", "provider": "anthropic", "kind": "subscription"}
	if !reflect.DeepEqual(m["account"], want) {
		t.Fatalf("account = %v, want %v", m["account"], want)
	}
	// v 2: one account's windows are no longer the session's to draw (68 §4.6).
	for _, k := range []string{"limits", "usage_reported"} {
		if _, has := m[k]; has {
			t.Fatalf("%s is still sent: %v", k, m[k])
		}
	}
	if m["paying"] != nil || m["unavailable_until"] != nil {
		t.Fatalf("paying/unavailable = %v %v", m["paying"], m["unavailable_until"])
	}
	if p := m["problems"].([]any); len(p) != 0 {
		t.Fatalf("problems = %v", p)
	}
}

func TestSessionFactsGPTSpentPayingCredits(t *testing.T) {
	sfFixture(t)
	m := sfGet(t, "sess-gpt", "claude-capable")
	if b, err := json.MarshalIndent(m, "", "  "); err == nil {
		t.Logf("sess-gpt:\n%s", b)
	}
	want := map[string]any{"name": "gpt", "provider": "openai", "kind": "subscription"}
	if !reflect.DeepEqual(m["account"], want) {
		t.Fatalf("account = %v, want %v", m["account"], want)
	}
	if m["model"].(map[string]any)["name"] != "gpt-6.1-sol" {
		t.Fatalf("model = %v", m["model"])
	}
	wantPay := map[string]any{"from": "codex credits", "left": 439.0, "unit": "credits", "pinned": false}
	if !reflect.DeepEqual(m["paying"], wantPay) {
		t.Fatalf("paying = %v, want %v", m["paying"], wantPay)
	}
}

func TestSessionFactsPayingAPIUnknownBalance(t *testing.T) {
	api := acctStanding{Provider: "anthropic", Kind: "api", AsOf: sfEpoch(sfNow)}
	api.Money = map[string]moneyPot{"balance": {Open: new(bool), Unit: str("usd")}}
	withSessionFacts(t, map[string]acctStanding{"anthropic-api": api}, nil)
	sfServe("sess-api", "claude-frontier", sessionFact{class: "frontier", member: "lane:opus",
		account: "anthropic-api", model: "claude-opus-5-5", tier: "paid?:anthropic-api/balance", provider: "anthropic"})
	m := sfGet(t, "sess-api", "claude-frontier")
	want := map[string]any{"from": "Anthropic API", "left": nil, "unit": "usd", "pinned": false}
	if !reflect.DeepEqual(m["paying"], want) {
		t.Fatalf("paying = %v, want %v", m["paying"], want)
	}
	if a := m["account"].(map[string]any); a["kind"] != "api" || a["name"] != "anthropic-api" {
		t.Fatalf("account = %v", a)
	}
}

func TestSessionFactsUnseen(t *testing.T) {
	sfFixture(t)
	rec := httptest.NewRecorder()
	sessionHandler(rec, lanRequest("GET", "/_switch/session?id=never-seen&model=claude-frontier", nil))
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	if len(m) != 7 || m["seen"] != false || m["v"] != 2.0 || m["as_of"] != "2026-11-01T18:40:00Z" ||
		m["class"] != "claude-frontier" || m["problems"] == nil {
		t.Fatalf("unseen = %s", rec.Body)
	}
	// No id is a caller's mistake, not an unseen session.
	rec = httptest.NewRecorder()
	sessionHandler(rec, lanRequest("GET", "/_switch/session?model=claude-frontier", nil))
	if rec.Code != 400 {
		t.Fatalf("no id: %d", rec.Code)
	}
	// Nor is no model: the reply is the prompt's model's, so it must be named.
	for _, q := range []string{"?id=sess-fable", "?id=sess-fable&model="} {
		rec = httptest.NewRecorder()
		sessionHandler(rec, lanRequest("GET", "/_switch/session"+q, nil))
		if rec.Code != 400 || rec.Body.String() != `{"error":"model is required: the model the session's prompt is set to"}` {
			t.Fatalf("no model (%s): %d %s", q, rec.Code, rec.Body)
		}
	}
}

// An unseen session's answer names the class of a class model, matched as a
// request is, and no class for anything else: a status line tells a class from
// a model named directly before the class first serves the session.
func TestSessionFactsUnseenNamesTheClassAsked(t *testing.T) {
	sfFixture(t)
	classRowsMu.Lock()
	classRowsSet = append(classRowsSet, registryEntry{Name: "gone", Type: "class", Class: "gone",
		ContextWindow: 200000, disabledReason: "removed"})
	classRowsMu.Unlock()
	for _, c := range []struct{ model, class string }{
		{"claude-frontier[1m]", "claude-frontier"},
		{"claude-frontier", "claude-frontier"}, // the bare form of a marked row
		{"claude-capable", "claude-capable"},
		{"claude-capable[1m]", ""}, // not marked: the [1m] form is no class
		{"claude-opus-5-5", ""},    // a model named directly
		{"claude-gone", ""},        // a removed row is no class
	} {
		m := sfGet(t, "never-seen", c.model)
		got, has := m["class"]
		if m["seen"] != false || has != (c.class != "") || (has && got != c.class) {
			t.Errorf("%s: %v", c.model, m)
		}
	}
}

func TestSessionFactsProviderWithoutUsage(t *testing.T) {
	// A provider whose reading carries no windows, and one the brain does not
	// read at all.
	kimi := acctStanding{Provider: "kimi", Kind: "subscription", AsOf: sfEpoch(sfNow)}
	withSessionFacts(t, map[string]acctStanding{"kimi": kimi}, nil)
	sfServe("sess-kimi", "claude-frontier", sessionFact{class: "frontier", member: "kimi/kimi-k3",
		account: "kimi", model: "kimi-k3", tier: "included", provider: "kimi"})
	sfServe("sess-unread", "claude-frontier", sessionFact{class: "frontier", member: "lane:opus",
		account: "max-z@example.com", model: "claude-opus-5-5", tier: "included", provider: "anthropic"})
	for _, id := range []string{"sess-kimi", "sess-unread"} {
		m := sfGet(t, id, "claude-frontier")
		if len(m["problems"].([]any)) != 0 {
			t.Errorf("%s: problems = %v", id, m["problems"])
		}
	}
	// Read but never measured is unknown, not unreported: said as a problem.
	withSessionFacts(t, map[string]acctStanding{"max-c@example.com": {Provider: "anthropic", Kind: "subscription"}}, nil)
	sfServe("sess-new", "claude-opus-5-5", sessionFact{account: "max-c@example.com", model: "claude-opus-5-5", tier: "included", provider: "anthropic"})
	m := sfGet(t, "sess-new", "claude-opus-5-5")
	if p := m["problems"].([]any); len(p) != 1 || p[0] != "the usage of max-c has not been read yet" {
		t.Fatalf("never measured: %v", p)
	}
	if _, has := m["class"]; has {
		t.Fatalf("a model named directly has no class: %v", m["class"])
	}
	if m["model"].(map[string]any)["name"] != "opus 5.5" {
		t.Fatalf("model = %v", m["model"])
	}
}

func TestSessionFactsUnavailableUntil(t *testing.T) {
	sfFixture(t)
	until := sfNow.Add(5 * time.Hour)
	lan.noteSessionUnavailable("capable", "sess-gpt", until)
	m := sfGet(t, "sess-gpt", "claude-capable")
	if m["unavailable_until"] != isoTime(until) {
		t.Fatalf("unavailable_until = %v", m["unavailable_until"])
	}
	if m["paying"] != nil {
		t.Fatalf("a refused session pays nothing: %v", m["paying"])
	}
	if m["model"].(map[string]any)["name"] != "gpt-6.1-sol" {
		t.Fatalf("the member that last served it stays: %v", m["model"])
	}
	// No reading says when: null, and said in plain words.
	lan.noteSessionUnavailable("capable", "sess-gpt", time.Time{})
	m = sfGet(t, "sess-gpt", "claude-capable")
	if m["unavailable_until"] != nil || len(m["problems"].([]any)) != 1 {
		t.Fatalf("unknown until: %v %v", m["unavailable_until"], m["problems"])
	}
	// A class that refused a session on its first request served nothing.
	lan.noteSessionUnavailable("deep", "sess-first", until)
	m = sfGet(t, "sess-first", "claude-deep")
	if m["seen"] != true || m["model"] != nil || m["account"] != nil || m["class"] != "claude-deep" {
		t.Fatalf("first-request refusal: %v", m)
	}
	// A later serve ends it.
	sfServe("sess-gpt", "claude-capable", sessionFact{class: "capable", account: "gpt", model: "gpt-6.1-sol", tier: "included", provider: "gpt"})
	if m := sfGet(t, "sess-gpt", "claude-capable"); m["unavailable_until"] != nil {
		t.Fatalf("served again: %v", m["unavailable_until"])
	}
}

// One session served for three models (its prompt's class, a subagent's
// class on another account, Claude Code's own haiku calls on a lane): each
// model answers only its own hop, whichever spoke last.
func TestSessionFactsPerModel(t *testing.T) {
	sfFixture(t)
	const sess = "sess-many"
	frontier := sessionFact{class: "frontier", member: "lane:fable", account: "max-b@example.com",
		model: "claude-fable-5-1", route: "frontier/lane:fable", tier: "included",
		provider: "anthropic", lane: "fable"}
	capable := sessionFact{class: "capable", member: "gpt/gpt-6.1-sol", account: "gpt", model: "gpt-6.1-sol",
		route: "capable/gpt/gpt-6.1-sol", tier: "paid:gpt/credits", provider: "gpt"}
	haiku := sessionFact{account: "max-a@example.com", model: "claude-haiku-4-5", tier: "included",
		provider: "anthropic", lane: "haiku"}
	check := func(when string) {
		t.Helper()
		for _, c := range []struct{ model, class, name, acct string }{
			{"claude-frontier[1m]", "claude-frontier", "fable 5.1", "max-b"},
			{"claude-frontier", "claude-frontier", "fable 5.1", "max-b"}, // classIDMatches: the bare form of a marked row
			{"claude-capable", "claude-capable", "gpt-6.1-sol", "gpt"},
			{"claude-haiku-4-5", "", "haiku 4.5", "max-a"},
			{"claude-haiku-4-5[1m]", "", "haiku 4.5", "max-a"},
		} {
			m := sfGet(t, sess, c.model)
			if m["seen"] != true || m["model"].(map[string]any)["name"] != c.name ||
				m["account"].(map[string]any)["name"] != c.acct {
				t.Errorf("%s: %s = %v", when, c.model, m)
			}
			if got, _ := m["class"].(string); got != c.class {
				t.Errorf("%s: %s class = %q, want %q", when, c.model, got, c.class)
			}
		}
	}
	// Each order: whichever is last never answers for the others.
	sfServe(sess, "claude-frontier[1m]", frontier)
	sfServe(sess, "claude-capable", capable)
	sfServe(sess, "claude-haiku-4-5", haiku)
	check("haiku last")
	sfServe(sess, "claude-frontier", frontier)
	check("frontier last")
	sfServe(sess, "claude-capable", capable)
	check("capable last")

	// capable's row is not marked [1m], so its marked form is no class: a
	// model named directly that this session never asked for.
	if m := sfGet(t, sess, "claude-capable[1m]"); m["seen"] != false {
		t.Errorf("claude-capable[1m] = %v", m)
	}
	// A model never served in the session, as right after the owner switched.
	if m := sfGet(t, sess, "claude-opus-5-5"); m["seen"] != false || len(m) != 6 {
		t.Errorf("never served = %v", m)
	}
	// A direct model's [1m] form finds its bare hops.
	sfServe(sess, "claude-opus-5-5", sessionFact{account: "max-a@example.com", model: "claude-opus-5-5",
		tier: "included", provider: "anthropic", lane: "opus"})
	if m := sfGet(t, sess, "claude-opus-5-5[1m]"); m["seen"] != true || m["model"].(map[string]any)["name"] != "opus 5.5" {
		t.Errorf("claude-opus-5-5[1m] = %v", m)
	}

	// A class's unavailable reply shows only under that class.
	until := sfNow.Add(2 * time.Hour)
	lan.noteSessionUnavailable("capable", sess, until)
	if m := sfGet(t, sess, "claude-capable"); m["unavailable_until"] != isoTime(until) || m["paying"] != nil {
		t.Errorf("capable after its refusal = %v", m)
	}
	lan.noteSessionUnavailable("capable", sess, time.Time{}) // and no reading says until when
	if b, _ := json.Marshal(sfGet(t, sess, "claude-capable")["problems"]); !strings.Contains(string(b), "claude-capable cannot serve") {
		t.Errorf("capable with no until: problems = %s", b)
	}
	for _, model := range []string{"claude-frontier[1m]", "claude-haiku-4-5", "claude-opus-5-5"} {
		m := sfGet(t, sess, model)
		if b, _ := json.Marshal(m["problems"]); m["unavailable_until"] != nil || strings.Contains(string(b), "cannot serve") {
			t.Errorf("%s after capable's refusal = %v", model, m)
		}
	}
	// And a class that refused a model the session never served is seen
	// there only, with nothing served.
	lan.noteSessionUnavailable("deep", sess, until)
	if m := sfGet(t, sess, "claude-deep"); m["seen"] != true || m["model"] != nil || m["class"] != "claude-deep" {
		t.Errorf("deep refusal = %v", m)
	}
	if m := sfGet(t, sess, "claude-frontier"); m["class"] != "claude-frontier" || m["unavailable_until"] != nil {
		t.Errorf("frontier after deep's refusal = %v", m)
	}
}

// The one key rule, as recording and reading share it.
func TestSessionModelKey(t *testing.T) {
	withSessionFacts(t, nil, nil)
	for _, c := range []struct{ a, b string }{
		{"claude-frontier", "claude-frontier[1m]"},
		{"claude-opus-5-5", "claude-opus-5-5[1m]"},
	} {
		if sessionModelKey(c.a) != sessionModelKey(c.b) {
			t.Errorf("%s and %s key apart", c.a, c.b)
		}
	}
	if sessionModelKey("claude-frontier") != classModelKey("frontier") {
		t.Errorf("a class model keys by its class")
	}
	for _, c := range []struct{ a, b string }{
		{"claude-capable", "claude-capable[1m]"}, // not marked: the [1m] form is no class
		{"claude-frontier", "claude-capable"},
		{"claude-opus-5-5", "claude-fable-5-1"},
	} {
		if sessionModelKey(c.a) == sessionModelKey(c.b) {
			t.Errorf("%s and %s share a key", c.a, c.b)
		}
	}
	if sessionModelKey("") != "" {
		t.Errorf("no model has no key")
	}
}

func TestSessionFactsBrainUnreachableIsAProblem(t *testing.T) {
	sfFixture(t)
	lan.pref.ok = false
	const words = "the proxy cannot reach the brain, so its facts may be old"
	m := sfGet(t, "sess-fable", "claude-frontier[1m]")
	if p := m["problems"].([]any); len(p) != 1 || p[0] != words {
		t.Fatalf("problems = %v", p)
	}
	if m := sfGet(t, "nobody", "claude-frontier"); len(m["problems"].([]any)) != 1 || m["problems"].([]any)[0] != words {
		t.Fatalf("unseen problems = %v", m["problems"])
	}
}

func TestSessionFactsShortNames(t *testing.T) {
	got := shortenTargets([]string{"user@example.com", "user+second@example.com", "gpt", "a@example.net", "a@example.org"})
	want := map[string]string{"user@example.com": "user", "user+second@example.com": "user+second",
		"gpt": "gpt", "a@example.net": "a@example.net", "a@example.org": "a@example.org"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("shortenTargets = %v", got)
	}
	// An address whose local part is shared has no short name: null, said.
	withSessionFacts(t, map[string]acctStanding{"a@example.net": {}, "a@example.org": {}}, nil)
	sfServe("s", "claude-opus-5-5", sessionFact{account: "a@example.net", model: "claude-opus-5-5", provider: "anthropic"})
	m := sfGet(t, "s", "claude-opus-5-5")
	if m["account"].(map[string]any)["name"] != nil || len(m["problems"].([]any)) != 2 {
		t.Fatalf("shared local part: %v %v", m["account"], m["problems"])
	}
}

func TestSessionFactsRefusedWithoutSecret(t *testing.T) {
	sfFixture(t)
	srv := httptest.NewServer(clientGate("lan-secret", http.HandlerFunc(switchHandler)))
	defer srv.Close()
	r, err := http.Get(srv.URL + "/_switch/session?id=sess-fable&model=claude-frontier")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if r.StatusCode != http.StatusUnauthorized || strings.Contains(string(b), "max-b") {
		t.Fatalf("without the secret: %d %s", r.StatusCode, b)
	}
	req, _ := http.NewRequest("GET", srv.URL+"/_switch/session?id=sess-fable&model=claude-frontier", nil)
	req.Header.Set("Authorization", "Bearer lan-secret")
	r, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != 200 {
		t.Fatalf("with the secret: %d", r.StatusCode)
	}
}

// End to end: a class request and a model named directly are each recorded
// by the hop that served them, and the endpoint reads them back.
func TestE2ESessionFactsRecordsServedHop(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	lan.sessFactsMu.Lock()
	oldFacts := lan.sessFacts
	lan.sessFacts = map[string]sessionFact{}
	lan.sessFactsMu.Unlock()
	t.Cleanup(func() { lan.sessFactsMu.Lock(); lan.sessFacts = oldFacts; lan.sessFactsMu.Unlock() })

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-e2e-class")
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("class request: %d", resp.StatusCode)
	}
	h.hit(t)
	resp = h.postSess(t, "claude-haiku-4-5", "CLIENT", "sess-e2e-direct")
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("direct request: %d", resp.StatusCode)
	}
	h.hit(t)
	// Claude Code's own haiku call under the class session's id speaks last;
	// the class's facts stay the class's.
	resp = h.postSess(t, "claude-haiku-4-5", "CLIENT", "sess-e2e-class")
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("direct request in the class session: %d", resp.StatusCode)
	}
	h.hit(t)

	get := func(id, model string) map[string]any { return sfGetVia(t, h.proxy.URL, id, model) }
	m := get("sess-e2e-class", "claude-frontier")
	if m["seen"] != true || m["class"] != "claude-frontier" || m["model"].(map[string]any)["name"] != "fable 5" {
		t.Fatalf("class session = %v", m)
	}
	if a := m["account"].(map[string]any); a["name"] != "user+second" || a["provider"] != "anthropic" {
		t.Fatalf("class session account = %v", a)
	}
	m = get("sess-e2e-direct", "claude-haiku-4-5")
	if _, has := m["class"]; has || m["seen"] != true || m["model"].(map[string]any)["name"] != "haiku 4.5" {
		t.Fatalf("direct session = %v", m)
	}
	if m := get("sess-e2e-none", "claude-frontier"); m["seen"] != false {
		t.Fatalf("unseen = %v", m)
	}
	// Each is kept under the model it asked for, not the other's.
	if m := get("sess-e2e-direct", "claude-frontier"); m["seen"] != false {
		t.Fatalf("direct session read for frontier = %v", m)
	}
	if m := get("sess-e2e-class", "claude-haiku-4-5"); m["seen"] != true || m["model"].(map[string]any)["name"] != "haiku 4.5" {
		t.Fatalf("the class session's haiku call = %v", m)
	}
}

// An account's address never leaks: not while the brain has not read the
// account, not through a pot's tier, not through a provider the hop did not
// name, not through a malformed tier.
func TestSessionFactsNeverCarryAnAddress(t *testing.T) {
	withSessionFacts(t, map[string]acctStanding{}, nil) // the brain lists nothing yet
	cases := map[string]sessionFact{
		"leak-credits":   {account: "x@example.org", model: "claude-opus-5-5", tier: "paid:x@example.org/credits", provider: "anthropic"},
		"leak-unknown":   {account: "x@example.org", model: "claude-opus-5-5", tier: "paid?:x@example.org/credits"},
		"leak-malformed": {account: "x@example.org", model: "claude-opus-5-5", tier: "paid:x@example.org", provider: "anthropic"},
		"leak-odd-pot":   {account: "x@example.org", model: "claude-opus-5-5", tier: "paid:x@example.org/x@example.org", provider: "anthropic"},
		"leak-class":     {class: "frontier", account: "x@example.org", model: "claude-opus-5-5", tier: "included", provider: "anthropic"},
	}
	asked := func(f sessionFact) string {
		if f.class != "" {
			return classPickerID(f.class)
		}
		return f.model
	}
	for id, f := range cases {
		sfServe(id, asked(f), f)
	}
	lan.noteSessionUnavailable("frontier", "leak-class", time.Time{})
	for id, f := range cases {
		m := sfGet(t, id, asked(f)) // sfGet fails on any "@" in the reply
		if a := m["account"].(map[string]any); a["name"] != "x" || a["kind"] != nil {
			t.Errorf("%s: account = %v", id, a)
		}
	}
	if m := sfGet(t, "leak-credits", "claude-opus-5-5"); m["paying"].(map[string]any)["from"] != "Anthropic extra usage" ||
		m["account"].(map[string]any)["provider"] != "anthropic" {
		t.Errorf("leak-credits = %v", m)
	}
	if m := sfGet(t, "leak-unknown", "claude-opus-5-5"); m["paying"].(map[string]any)["from"] != "paid usage" ||
		m["account"].(map[string]any)["provider"] != nil {
		t.Errorf("an unknown provider is null and its pot generic: %v", m)
	}
	if m := sfGet(t, "leak-malformed", "claude-opus-5-5"); m["paying"].(map[string]any)["from"] != "Anthropic paid usage" {
		t.Errorf("a malformed tier: %v", m["paying"])
	}
	// And every reply the fixture scenarios make.
	sfFixture(t)
	lan.noteSessionUnavailable("deep", "sess-first", sfNow.Add(time.Hour))
	for id, model := range map[string]string{"sess-fable": "claude-frontier[1m]", "sess-gpt": "claude-capable",
		"sess-first": "claude-deep", "nobody": "claude-frontier"} {
		sfGet(t, id, model)
	}
}

// A session idle past the binding lifetime is unseen when read, whether or
// not anything has been written since to prune it.
func TestSessionFactsIdleSessionIsUnseenAtReadTime(t *testing.T) {
	sfFixture(t)
	nowFn = func() time.Time { return sfNow.Add(fundingIdle) }
	if m := sfGet(t, "sess-fable", "claude-frontier[1m]"); m["seen"] != true {
		t.Fatalf("at the lifetime: %v", m["seen"])
	}
	nowFn = func() time.Time { return sfNow.Add(30 * time.Hour) }
	if m := sfGet(t, "sess-fable", "claude-frontier[1m]"); m["seen"] != false || len(m) != 7 || m["class"] != "claude-frontier" {
		t.Fatalf("30h idle: %v", m)
	}
}

// sfGetVia reads one session for one model through a running proxy.
func sfGetVia(t *testing.T, base, id, model string) map[string]any {
	t.Helper()
	r, err := http.Get(base + "/_switch/session?id=" + url.QueryEscape(id) + "&model=" + url.QueryEscape(model))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	sfNoAddress(t, string(b))
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("%v: %s", err, b)
	}
	return m
}

// sfFactOf is the fact held for session under the model the client asked for.
func sfFactOf(session, model string) (sessionFact, bool) {
	lan.sessFactsMu.Lock()
	defer lan.sessFactsMu.Unlock()
	f, ok := lan.sessFacts[sessionFactKey(sessionModelKey(model), session)]
	return f, ok
}

// Through the real hooks: a mid-request cross records the member and lane
// that finally served, not the one bound.
func TestE2ESessionFactsFailoverRecordsTheFinalHop(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	h.bindTo.Store("kimi/kimi-k3")
	h.deny.Store(true) // the bound kimi member 429s; the walk crosses to lane:fable

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-sf-cross")
	resp.Body.Close()
	h.hostedHit(t)
	h.hit(t)
	if resp.StatusCode != 200 {
		t.Fatalf("crossed request: %d", resp.StatusCode)
	}
	f, ok := sfFactOf("sess-sf-cross", "claude-frontier")
	if !ok || f.member != "lane:fable" || f.lane != "fable" || f.provider != "anthropic" || f.account != other || f.class != "frontier" {
		t.Fatalf("fact = %+v, want the final hop: lane:fable on the fable lane's account", f)
	}
	m := sfGetVia(t, h.proxy.URL, "sess-sf-cross", "claude-frontier")
	if a := m["account"].(map[string]any); a["provider"] != "anthropic" || a["name"] != "user+second" {
		t.Fatalf("account = %v", a)
	}
	if m["model"].(map[string]any)["name"] != "fable 5" {
		t.Fatalf("model = %v", m["model"])
	}
}

// Through the real hooks: a provider member records its provider; count_tokens
// writes nothing; an unavailable reply records through unavailableResponse;
// a later serve clears it.
func TestE2ESessionFactsLifecycleThroughTheHooks(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	oldFresh := bindFresh
	bindFresh = 0
	defer func() { bindFresh = oldFresh }()
	post := func(path, session string) {
		t.Helper()
		req, _ := http.NewRequest("POST", h.proxy.URL+path,
			strings.NewReader(`{"model":"claude-frontier","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`))
		req.Header.Set("Authorization", "Bearer CLIENT")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-claude-code-session-id", session)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	// count_tokens alone: nothing recorded.
	post("/v1/messages/count_tokens", "sess-sf-ct")
	h.hit(t)
	if _, ok := sfFactOf("sess-sf-ct", "claude-frontier"); ok {
		t.Fatal("count_tokens recorded a session fact")
	}
	if m := sfGetVia(t, h.proxy.URL, "sess-sf-ct", "claude-frontier"); m["seen"] != false {
		t.Fatalf("count_tokens session = %v", m)
	}

	post("/v1/messages", "sess-sf-life")
	h.hit(t)
	served, ok := sfFactOf("sess-sf-life", "claude-frontier")
	if !ok || served.unavailable || served.member != "lane:fable" {
		t.Fatalf("served = %+v", served)
	}

	d := loadGolden(t)
	var body map[string]any
	_ = json.Unmarshal(d.Unavailable[0].Want, &body)
	body["error"], body["class"] = "class_unavailable", "frontier"
	b, _ := json.Marshal(body)
	h.bind409.Store(string(b))
	post("/v1/messages", "sess-sf-life")
	f, _ := sfFactOf("sess-sf-life", "claude-frontier")
	if !f.unavailable || f.class != "frontier" || f.member != "lane:fable" {
		t.Fatalf("after the unavailable reply: %+v", f)
	}
	m := sfGetVia(t, h.proxy.URL, "sess-sf-life", "claude-frontier")
	if m["paying"] != nil || (m["unavailable_until"] == nil && len(m["problems"].([]any)) == 0) {
		t.Fatalf("unavailable session = %v", m)
	}
	h.bind409.Store("")

	post("/v1/messages/count_tokens", "sess-sf-life")
	h.hit(t)
	if g, _ := sfFactOf("sess-sf-life", "claude-frontier"); !g.unavailable {
		t.Fatal("count_tokens cleared the unavailable record")
	}

	post("/v1/messages", "sess-sf-life")
	h.hit(t)
	if g, _ := sfFactOf("sess-sf-life", "claude-frontier"); g.unavailable {
		t.Fatalf("a later serve left it unavailable: %+v", g)
	}
	if m := sfGetVia(t, h.proxy.URL, "sess-sf-life", "claude-frontier"); m["unavailable_until"] != nil {
		t.Fatalf("served again = %v", m)
	}

	// A provider member's hop records its provider, named for its maker.
	h.serveMember(t, "kimi/kimi-k3", 0.61)
	post("/v1/messages", "sess-sf-kimi")
	h.hostedHit(t)
	if g, _ := sfFactOf("sess-sf-kimi", "claude-frontier"); g.provider != "kimi" || g.lane != "" || g.member != "kimi/kimi-k3" {
		t.Fatalf("kimi fact = %+v", g)
	}
}
