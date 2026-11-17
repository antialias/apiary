package main

// ---- session facts: GET /_switch/session?id=<session id>&model=<model> ----
//
// What this proxy can say about one Claude Code session's prompt, for a status
// line (docs/session-facts.md is the contract). Facts are kept per session and
// model (sessionModelKey): a session's subagents, a workflow's agents and
// Claude Code's own small calls carry the parent's session id, so the hop that
// last served the session at all is often not what its prompt talks to. Every
// fact is one this proxy already holds: the hop that last served the session
// for that model (noteServedHop), the class's last unavailable reply for it
// (noteSessionFunding), and the
// account's standing, the brain's /preferred reading with this proxy's newer
// response readings and live records over it (standingFor). Nothing here asks
// the brain or a provider anything: a render never waits on the network.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// sessionFact is what this proxy last saw of one session for one model: the
// hop that last served it, and whether its class has since refused it as
// unavailable.
type sessionFact struct {
	class   string // brain class name; "" when the session named a model directly
	member  string
	account string // the account the serving hop drew on
	model   string // the upstream model the serving hop sent
	route   string // the route its dial check was made for ("frontier/lane:fable", "lane:opus")
	tier    string // the funding tier the serving hop drew on (dialCheck.funding)
	// provider is the serving hop's provider id as the hop knew it: anthropic
	// for an Anthropic lane, a class member's provider ("gpt", "kimi"), a
	// direct provider's name ("local"); "" when the hop did not say.
	provider string
	lane     string // the Anthropic lane that served, "" for a provider hop
	// pinned: the serving hop was held to its member by a pin (the class
	// route's pin, and the hop served that member). A seat is not a pin.
	pinned bool
	served time.Time

	// The class's last unavailable reply, when it is newer than served.
	unavailable bool
	until       time.Time // zero = no reading says when
	at          time.Time // the newest write
}

// Per tenant: tenant.sessFactsMu, tenant.sessFacts (by sessionFactKey).

// sessionModelKey is the model a session's facts are kept under, from a model
// as the client names it: a class model by its class, matched as requests are
// (matchRegistryClass, so "claude-frontier" and "claude-frontier[1m]" are one
// class where classIDMatches says so); any other model by its id without a
// trailing "[1m]". Recording and reading both key through here, so the two
// cannot disagree. "" for no model.
func sessionModelKey(model string) string {
	if e, ok := matchRegistryClass(model); ok {
		return classModelKey(e.Class)
	}
	return strings.TrimSuffix(model, "[1m]")
}

// classModelKey is a brain class's key in sessionModelKey's terms. No model id
// holds a NUL, so a class can never share a key with a model named directly.
func classModelKey(class string) string { return "class\x00" + class }

// sessionFactKey is one session's facts for one model key, as bindKey is the
// brain's per class and session.
func sessionFactKey(key, session string) string { return key + "\x00" + session }

// noteSessionServed records the hop that served a session's request under
// key (sessionModelKey). A count_tokens call is free and says nothing of what
// serves the session's requests, so it writes nothing (as
// noteSessionFunding).
func (tn *tenant) noteSessionServed(session, key string, f sessionFact) {
	if session == "" || key == "" {
		return
	}
	now := nowFn()
	f.served, f.at = now, now
	tn.sessFactsMu.Lock()
	tn.sessFacts[sessionFactKey(key, session)] = f
	tn.pruneSessionFactsLocked(now)
	tn.sessFactsMu.Unlock()
}

// noteSessionUnavailable records the class's unavailable reply to a session,
// under the class. What served it there before stays: a status line shows
// what it was on, and that it cannot be served until.
func (tn *tenant) noteSessionUnavailable(class, session string, until time.Time) {
	if session == "" || class == "" {
		return
	}
	now := nowFn()
	k := sessionFactKey(classModelKey(class), session)
	tn.sessFactsMu.Lock()
	f := tn.sessFacts[k]
	f.class, f.unavailable, f.until, f.at = class, true, until, now
	tn.sessFacts[k] = f
	tn.pruneSessionFactsLocked(now)
	tn.sessFactsMu.Unlock()
}

// pruneSessionFactsLocked drops each session's facts for a model once idle
// past the brain's binding lifetime, as noteSessionFunding does.
func (tn *tenant) pruneSessionFactsLocked(now time.Time) {
	for k, o := range tn.sessFacts {
		if now.Sub(o.at) > fundingIdle {
			delete(tn.sessFacts, k)
		}
	}
}

// ---- the reply (docs/session-facts.md, v1) ----

type sfModel struct {
	Name string `json:"name"`
}

type sfAccount struct {
	Name     *string `json:"name"`
	Provider *string `json:"provider"`
	Kind     *string `json:"kind"`
}

type sfPaying struct {
	From   string   `json:"from"`
	Left   *float64 `json:"left"`
	Unit   *string  `json:"unit"`
	Pinned bool     `json:"pinned"`
}

type sfSeen struct {
	V                int        `json:"v"`
	AsOf             string     `json:"as_of"`
	Seen             bool       `json:"seen"`
	Class            string     `json:"class,omitempty"`
	Model            *sfModel   `json:"model"`
	Account          *sfAccount `json:"account"`
	Paying           *sfPaying  `json:"paying"`
	UnavailableUntil *string    `json:"unavailable_until"`
	Room             *sfRoom    `json:"room"`
	Asks             []string   `json:"asks"`
	Problems         []string   `json:"problems"`
}

// brainFleet is the part of the brain's `fleet` section (quota-model 67 §8)
// session facts read: its unit and each selection's figure. A selection's
// accounts are not read: they name accounts, and a reply never carries an
// account's address.
type brainFleet struct {
	Unit       string                    `json:"unit"`
	Selections map[string]fleetSelection `json:"selections"`
}

// fleetSelection is one selection's figure as the brain publishes it, kept as
// the brain's own JSON so it passes on unchanged (65 §6), with its counts, its
// two times (integer seconds, quota-model 68 §3.1 as built) and its asks. A
// field the brain does not send stays nil, and the reply says null.
type fleetSelection struct {
	Capacity json.RawMessage `json:"capacity"`
	Room     json.RawMessage `json:"room"`
	LeftPct  json.RawMessage `json:"left_pct"`
	Coverage json.RawMessage `json:"coverage"`
	MoreAt   *int64          `json:"more_at"`
	BackAt   *int64          `json:"back_at"`
	// Asks is nil when the brain sends none (a brain older than 68 step 1):
	// unknown, not "nothing to ask".
	Asks []fleetAsk `json:"asks"`
}

// fleetAsk is one thing only the owner can fix (68 §3.2). Its account is read
// for the short name only, never passed on.
type fleetAsk struct {
	Account string  `json:"account"`
	Kind    string  `json:"kind"`
	SignIn  *string `json:"sign_in"`
	Model   *string `json:"model"`
	Until   *int64  `json:"until"`
}

// decodeFleet reads the brain's `fleet` section on its own: nil for none, and
// nil with the error for one the proxy cannot read, which only costs the
// sessions their room (applyPreferred logs it and applies the rest).
func decodeFleet(raw json.RawMessage) (*brainFleet, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var fl *brainFleet
	if err := json.Unmarshal(raw, &fl); err != nil {
		return nil, err
	}
	if fl != nil {
		for selection, sel := range fl.Selections {
			for _, a := range sel.Asks {
				if a.Kind != "not_entitled" && (a.Kind != "auth" || a.SignIn == nil ||
					(*a.SignIn != "oauth" && *a.SignIn != "key")) {
					return nil, fmt.Errorf("fleet selection %q carries an ask the proxy cannot read", selection)
				}
			}
		}
	}
	return fl, nil
}

// sfRoom is the session's selection's figure: the brain's numbers unchanged,
// the selection named as the brain keys it, its unit (null when the brain
// gives none), when the proxy applied the document that carried it, and the
// selection's counts and times as the brain gives them.
type sfRoom struct {
	Selection string          `json:"selection"`
	Unit      *string         `json:"unit"`
	Capacity  json.RawMessage `json:"capacity"`
	Room      json.RawMessage `json:"room"`
	LeftPct   json.RawMessage `json:"left_pct"`
	HeardAt   *string         `json:"heard_at"`
	Coverage  json.RawMessage `json:"coverage"`
	MoreAt    *string         `json:"more_at"`
	BackAt    *string         `json:"back_at"`
}

// selectionOf is the selection the session's request was routed under, as the
// brain's `fleet.selections` keys it (67 §7): a class by its brain name, a bare
// Anthropic lane by its route id ("lane:opus"). "" for a hop of neither: a
// lane is an Anthropic account's, so only an
// Anthropic hop names one here, whatever the recorded fact carries.
func selectionOf(f sessionFact) string {
	switch {
	case f.class != "":
		return f.class
	case f.lane != "" && f.provider == "anthropic":
		return laneRouteID(f.lane)
	}
	return ""
}

// unseenSelection is the selection a session's first request for model would
// be routed under, by the request path's own order (main.go's model switch):
// a class row, by its class; a removed class, a class-shaped id and a
// provider's deleted direct id are refused, so none; anything else rides the Anthropic lane laneForModel gives it, its
// "[1m]" picker mark stripped as the request path strips it. laneForModel
// alone answers "default" for anything, so it is asked last.
func (tn *tenant) unseenSelection(model string) string {
	if e, ok := matchRegistryClass(model); ok {
		return e.Class
	}
	if _, ok := matchClassTombstone(model); ok {
		return ""
	}
	if _, _, ok := classShapedRefusalReason(model); ok {
		return ""
	}
	if _, ok := hostedProviderNamespace(model); ok {
		return ""
	}
	routed := model
	if base, cut := strings.CutSuffix(model, "[1m]"); cut && strings.ContainsAny(base, "0123456789") {
		routed = base
	}
	tn.pref.mu.RLock()
	rules := tn.pref.laneRules
	tn.pref.mu.RUnlock()
	return laneRouteID(laneForModel(routed, rules))
}

// Words for the missing-figure cases (quota-model 68 §4.5).
const problemNoFleet = "the brain publishes no room figure"

func (tn *tenant) problemNoFigure(selection string) string {
	return "the brain has no figure for " + selection
}

// roomFor is the selection's figure and asks from the brain's last document,
// never made up. No selection (a model the proxy refuses): no room, no asks, nothing wrong. Otherwise room and asks are nil
// (null: unknown) while the brain is unreachable, whose problem proxyProblems
// says, and when the document has no readable `fleet` or no entry for the
// selection, with the problem that says which.
func (tn *tenant) roomFor(selection string) (room *sfRoom, asks []string, problem string) {
	if selection == "" {
		return nil, []string{}, ""
	}
	tn.pref.mu.RLock()
	ok, fl, at := tn.pref.ok, tn.pref.fleet, tn.pref.fleetAt
	tn.pref.mu.RUnlock()
	switch {
	case !ok:
		return nil, nil, ""
	case fl == nil:
		return nil, nil, problemNoFleet
	}
	sel, has := fl.Selections[selection]
	if !has {
		return nil, nil, tn.problemNoFigure(selection)
	}
	r := &sfRoom{Selection: selection, Capacity: sel.Capacity, Room: sel.Room, LeftPct: sel.LeftPct,
		Coverage: sel.Coverage, MoreAt: isoSeconds(sel.MoreAt), BackAt: isoSeconds(sel.BackAt)}
	if fl.Unit != "" {
		r.Unit = sfPtr(fl.Unit)
	}
	if !at.IsZero() {
		r.HeardAt = sfPtr(isoTime(at))
	}
	if sel.Asks != nil {
		asks = make([]string, 0, len(sel.Asks))
		for _, a := range sel.Asks {
			asks = append(asks, tn.askWords(a))
		}
	}
	return r, asks, ""
}

// isoSeconds is a brain time in integer seconds as ISO; nil for none.
func isoSeconds(s *int64) *string {
	if s == nil {
		return nil
	}
	return sfPtr(isoTime(time.Unix(*s, 0)))
}

// askWords is one ask in the owner's words (quota-model 68 §4.2), keyed on
// the provider's sign-in kind, never on the account's kind. The account is
// named by its short name, "an account" when the only name is an address.
func (tn *tenant) askWords(a fleetAsk) string {
	name := "an account"
	if n := tn.accountShortName(a.Account); n != nil {
		name = *n
	}
	signIn := ""
	if a.SignIn != nil {
		signIn = *a.SignIn
	}
	switch {
	case a.Kind == "auth" && signIn == "oauth":
		return name + ": login expired, log in again"
	case a.Kind == "auth" && signIn == "key":
		return name + ": key refused, replace the key"
	case a.Kind == "not_entitled" && a.Model != nil && *a.Model != "":
		return name + ": " + plainModel(*a.Model) + " needs a plan change"
	case a.Kind == "not_entitled":
		// A pool's limit names no model (68 §3.2 as built).
		return name + ": needs a plan change"
	}
	// decodeFleet rejects a kind or sign-in the proxy has no words for.
	return ""
}

// sessionFactsV is the contract's version. 2 since limits and usage_reported
// left the reply (quota-model 68 §4.6): the status line draws the selection's
// room, not one account's windows.
const sessionFactsV = 2

type sfUnseen struct {
	V        int      `json:"v"`
	AsOf     string   `json:"as_of"`
	Seen     bool     `json:"seen"`
	Class    string   `json:"class,omitempty"`
	Room     *sfRoom  `json:"room"`
	Asks     []string `json:"asks"`
	Problems []string `json:"problems"`
}

func isoTime(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// sessionFactsFor is the reply for one session id and the model its prompt is
// set to, at now: only what served the session for that model.
func (tn *tenant) sessionFactsFor(session, model string, now time.Time) any {
	problems := tn.proxyProblems()
	key := sessionModelKey(model)
	tn.sessFactsMu.Lock()
	f, ok := tn.sessFacts[sessionFactKey(key, session)]
	tn.sessFactsMu.Unlock()
	// Pruning runs on writes; a quiet proxy must not keep answering for a
	// session idle past the binding lifetime.
	if !ok || session == "" || key == "" || now.Sub(f.at) > fundingIdle {
		// A class model's class is the registry's, served or not: a status line
		// tells a class from a model named directly from its first answer.
		out := sfUnseen{V: sessionFactsV, AsOf: isoTime(now), Seen: false}
		if e, ok := matchRegistryClass(model); ok {
			out.Class = classPickerID(e.Class)
		}
		var why string
		out.Room, out.Asks, why = tn.roomFor(tn.unseenSelection(model))
		if why != "" {
			problems = append(problems, why)
		}
		out.Problems = problems
		return out
	}
	out := sfSeen{V: sessionFactsV, AsOf: isoTime(now), Seen: true}

	if f.class != "" {
		out.Class = classPickerID(f.class)
	}
	var why string
	out.Room, out.Asks, why = tn.roomFor(selectionOf(f))
	if why != "" {
		problems = append(problems, why)
	}
	// What served the session for this model, unless the class refused it
	// before it ever served it.
	served := !f.served.IsZero()
	if f.unavailable {
		if f.until.IsZero() {
			problems = append(problems, out.Class+" cannot serve this session, and no reading says until when")
		} else {
			out.UnavailableUntil = sfPtr(isoTime(f.until))
		}
	}
	if !served {
		out.Problems = problems
		return out
	}
	out.Model = &sfModel{Name: plainModel(f.model)}
	if f.account == "" {
		// No account named: nothing to read or pay.
		out.Problems = problems
		return out
	}
	st := tn.standingFor(f.account, now)
	acct := &sfAccount{Name: tn.accountShortName(f.account), Provider: contractProvider(f.provider)}
	if st != nil && st.Kind != "" {
		acct.Kind = sfPtr(st.Kind)
	}
	out.Account = acct
	if acct.Name == nil {
		problems = append(problems, "the proxy holds no short name for this account")
	}
	if st != nil && st.AsOf == nil {
		// The brain holds the account but has never measured it.
		problems = append(problems, "the usage of "+displayName(acct)+" has not been read yet")
	}
	if !f.unavailable {
		if out.Paying = payingFrom(f.tier, f.provider, st); out.Paying != nil {
			out.Paying.Pinned = f.pinned
		}
	}
	out.Problems = problems
	return out
}

func sfPtr[T any](v T) *T { return &v }

func displayName(a *sfAccount) string {
	if a.Name != nil {
		return *a.Name
	}
	return "the account"
}

// proxyProblems is the proxy-wide trouble worth the owner's attention.
func (tn *tenant) proxyProblems() []string {
	out := []string{}
	tn.pref.mu.RLock()
	ok := tn.pref.ok
	tn.pref.mu.RUnlock()
	if tn.brainURL() != "" && !ok {
		out = append(out, "the proxy cannot reach the brain, so its facts may be old")
	}
	return out
}

// classPickerID is a brain class as the client names it: its registry row's
// rendered id without the window mark ("frontier" -> "claude-frontier").
func classPickerID(class string) string {
	if e, ok := matchRegistryClassName(class); ok {
		return "claude-" + e.Name
	}
	return "claude-" + class
}

// plainModel is a model id in plain words: an Anthropic id as family and
// version ("claude-fable-5-1" -> "fable 5.1"), any other as it is.
func plainModel(id string) string {
	base := strings.TrimSuffix(id, "[1m]")
	if name, ok := strings.CutPrefix(base, "claude-"); ok {
		if fam, ver, ok := anthropicNameParts(name); ok {
			return fam + " " + ver
		}
		return name
	}
	return base
}

// contractProvider is a provider id the serving hop recorded, in the
// contract's words: codex's "gpt" named for its maker. Never derived from an
// account name; nil when the hop did not say.
func contractProvider(p string) *string {
	switch p {
	case "":
		return nil
	case "gpt":
		return sfPtr("openai")
	}
	return sfPtr(p)
}

// accountShortName is the name the owner reads an account by on Scry's
// quota and routing panels (routing-observability lib/targetNames.ts): an
// address's local part when no other account the brain serves shares it,
// any other name as it is. nil when the only name is a whole address.
func (tn *tenant) accountShortName(account string) *string {
	if account == "" {
		return nil
	}
	tn.pref.mu.RLock()
	names := make([]string, 0, len(tn.pref.accounts)+1)
	for a := range tn.pref.accounts {
		names = append(names, a)
	}
	tn.pref.mu.RUnlock()
	names = append(names, account)
	short := shortenTargets(names)[account]
	if strings.Contains(short, "@") {
		return nil
	}
	return &short
}

// shortenTargets is Scry's rule (lib/targetNames.ts), kept in step with it:
// shorten per target, and only when the short form is still unique.
func shortenTargets(targets []string) map[string]string {
	local := func(t string) string {
		if at := strings.LastIndex(t, "@"); at > 0 {
			return t[:at]
		}
		return t
	}
	seen := map[string]bool{}
	claims := map[string]int{}
	for _, t := range targets {
		if !seen[t] {
			seen[t] = true
			claims[local(t)]++
		}
	}
	out := map[string]string{}
	for t := range seen {
		if s := local(t); claims[s] == 1 {
			out[t] = s
		} else {
			out[t] = t
		}
	}
	return out
}

// payingFrom is the money pot a served funding tier ("paid:<account>/<pot>",
// "paid?:<account>/<pot>") draws on, in plain words from the provider the
// serving hop recorded and the pot's id, with its balance as this proxy reads
// it now (st: the serving account's standing). nil for an included tier. No
// part of the tier is echoed: an account name may be an address.
func payingFrom(tier, provider string, st *acctStanding) *sfPaying {
	pot, ok := strings.CutPrefix(tier, "paid:")
	if !ok {
		if pot, ok = strings.CutPrefix(tier, "paid?:"); !ok {
			return nil
		}
	}
	_, potID, _ := strings.Cut(pot, "/")
	p := &sfPaying{From: potWords(provider, potID)}
	if st != nil && potID != "" {
		if mp, has := st.Money[potID]; has {
			p.Left = mp.Balance
			if mp.Unit != nil {
				p.Unit = sfPtr(unitWords(*mp.Unit))
			}
		}
	}
	return p
}

var providerTitles = map[string]string{"anthropic": "Anthropic", "gpt": "OpenAI", "openai": "OpenAI", "kimi": "Kimi", "runway": "Runway"}

// providerTitle is a provider id as a name; "" for one this proxy has no name
// for (an unknown id is not echoed).
func providerTitle(p string) string {
	return providerTitles[p]
}

// potWords names a money pot in plain words: a subscription's credits are
// codex credits on OpenAI and the provider's extra usage elsewhere; an api
// account's balance is "<Provider> API"; anything else, or a provider not
// known, is said generically.
func potWords(provider, potID string) string {
	title := providerTitle(provider)
	switch {
	case title == "":
		return "paid usage"
	case potID == potCredits && title == "OpenAI":
		return "codex credits"
	case potID == potCredits:
		return title + " extra usage"
	case potID == "balance":
		return title + " API"
	}
	return title + " paid usage"
}

// unitWords is a pot's unit in plain words: a provider's credit is credits.
func unitWords(u string) string {
	if strings.HasSuffix(u, "_credit") {
		return "credits"
	}
	return u
}

// sessionHandler serves GET /_switch/session?id=&model=. Behind the client
// gate, like every /_switch path but healthz.
func sessionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"GET only"}`, http.StatusMethodNotAllowed)
		return
	}
	id := r.URL.Query().Get("id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"id is required: the Claude Code session id"}`))
		return
	}
	model := r.URL.Query().Get("model")
	if model == "" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"model is required: the model the session's prompt is set to"}`))
		return
	}
	// The facts are the entry's tenant's: a session is read from the state
	// of the tenant whose login asked (MEMBERS-PLAN 1b).
	tn := mustTenant(w, r)
	if tn == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(tn.sessionFactsFor(id, model, nowFn()))
}
