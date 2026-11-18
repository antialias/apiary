// claude-switch-proxy — a reverse proxy in front of the Anthropic API for
// Claude Code (via ANTHROPIC_BASE_URL) that transparently routes each request to
// whichever of its Claude accounts has quota headroom, and fires a LOUD notification
// the moment routing actually switches. Since #35 it can run as a network service
// (clients authenticate with a minted secret via ANTHROPIC_AUTH_TOKEN — see
// SWITCH_CLIENT_SECRET below); a loopback deployment is the
// secret-unset degenerate case.
//
// Design:
//   - The "brain" (claude-usage-exporter) decides which account is preferred
//     (GET /preferred, sticky hysteresis) and dispenses each account's credential,
//     secret-guarded, to the actuator of the account's kind (actuator.go). The brain
//     is the SOLE credential refresher, so there's no refresh-token rotation race.
//   - This proxy subscribes to the brain's GET /events (brain_stream.go), whose
//     `preferred` events carry the /preferred document, and on each /v1 request ALWAYS swaps the
//     Authorization bearer to the chosen account's fresh token — every account is
//     equal, none is privileged, and the client's own login is never forwarded onto
//     an account lane (anthropic-beta is always forwarded verbatim; stripping it -> 401).
//   - OBSERVABLE, NO SILENT FALLBACK. Each account request is one of three typed states:
//     ROUTED (fresh brain decision, token injected), STALE (brain unreachable but riding
//     the last-good decision on a still-valid cached token — requests keep flowing), or
//     FAILED (no usable token for any account -> a loud 503, never the ambient login).
//     The prior "degrade to passthrough" behavior silently rode whatever account you had
//     last /login'd into — the exact bug this replaces.
//   - When the account actually used for a request changes, it POSTs an alert to the
//     alert receiver (SWITCH_BRIDGE_URL). TIERED (see loudWeeklyPct): a routine switchover goes to its chat channel
//     only, because the proxy handling it is the system working and there is nothing
//     for you to do. It escalates to a DND-bypassing critical push only on the
//     LAST HOP — when the account we just switched TO is itself >= 80% weekly, i.e.
//     there is nowhere left to go.
//   - CLASS ROWS (#27, class.go): a registry `"type":"class"` id (claude-frontier)
//     is bound per SESSION by the brain (/class-bind) to a member — an Anthropic
//     lane or a direct provider — and actuated here: model rewritten to the
//     member's real id, member's own auth injected. 429s ladder locally (account
//     swap, then ONE cross-provider member hop); status keys class_bindings /
//     refusal_records / state_by_lane["class:<name>"].
//
// Env:
//
//	SWITCH_BRAIN_URL        e.g. http://192.0.2.10:9115 (enables brain mode)
//	SWITCH_KEY / _FILE      shared secret for the brain's credential endpoints
//	SWITCH_BRIDGE_URL       e.g. http://192.0.2.10:9116/alert (switchover alert)
//	SWITCH_429_COOLDOWN     seconds a 429'd account is skipped (default 300)
//	SWITCH_LOUD_WEEKLY_PCT  last-hop threshold for a loud alert (default 80)
//	SWITCH_ALERT_ALWAYS_LOUD=1  every switchover pages critical (pre-tiering behaviour)
//	SWITCH_ALERT_CRIT_COOLDOWN  seconds before a lane may page critical again (default 21600)
//	SWITCH_ALERT_REPEAT_WINDOW  seconds an identical alert (lane + from->to) is suppressed (default 3600)
//	SWITCH_INJECT_TOKEN_FILE  manual override: force this token on /v1 (test hook)
//	SWITCH_LOCAL_UPSTREAM   a llama-server, provider "local" (#135; unset or "off": no provider)
//	SWITCH_CLIENT_MODELS_CACHE  picker cache path override ("off" disables; default ~/.claude/cache/gateway-models.json)
//	SWITCH_CLIENT_BASE_URL  baseUrl written into the picker cache (default http://<listen>)
//	SWITCH_CLIENT_SECRET / _FILE  client gate (#35): when set, EVERY request except
//	                        GET /_switch/healthz must carry Authorization: Bearer <secret>
//	                        (Claude Code: export ANTHROPIC_AUTH_TOKEN=<secret> — it lands
//	                        there and DISPLACES the ambient OAuth login, so the network only
//	                        ever carries this dumb secret). The header is validated then
//	                        STRIPPED — it must never reach an upstream. Unset = no gate
//	                        (the pre-#35 loopback deployment).
//
// Hosted providers — non-Anthropic upstreams that serve workload-class provider
// members (class.go). Kimi speaks the Anthropic Messages format natively (its key is
// injected, enabled only when set); the gpt provider is translated in-proxy to the
// codex wire shape (codex.go). Neither has a picker id of its own: their direct,
// policy-free ids were deleted in quota-model C2 (§2.2 row 10). Keys come from the
// environment — NEVER the repo.
//
//	SWITCH_KIMI_UPSTREAM    Anthropic-format Kimi endpoint (default https://api.kimi.com/coding for
//	                        sk-kimi-* Kimi-for-Coding keys; Moonshot platform keys use https://api.moonshot.ai/anthropic)
//	An edition's providers and doors document their own env where they read
//	it (providerSpecs.fromEnv, entryHooks.fromEnv).
//	-listen                 default 127.0.0.1:8787
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// product is this proxy's name in the messages it writes to a client.
const product = "apiary-proxy"

var upstream = "https://api.anthropic.com"

var (
	// brainBase is SWITCH_BRAIN_URL: the brain's root. A tenant's routes are
	// under brainBase + "/t/" + id (tenant.brainURL); the index routes
	// (/tenants) and nothing else are at the root.
	brainBase      string
	bridgeURL      string
	switchKey      string
	injectFile     string
	forcedCooldown = 300 * time.Second // how long a 429'd account is skipped

	// Switchover alert tiering. A routine switchover is the system WORKING — the
	// proxy routed around a hot account and nothing is required of you. Paging for
	// it would put "handled it, nothing to do" in the same DND-bypassing
	// channel as the real "you are about to lose Claude" page, and you would learn
	// to swipe both away. So a switchover is only LOUD when it is the LAST HOP:
	// we're moving TO an account that is itself already >= loudWeeklyPct, i.e. the
	// fallback we just landed on is nearly spent too.
	//
	// Failing QUIET is safe here: the actual emergency page does not come from this
	// proxy at all, it comes from separate monitoring
	// (an all-accounts-exhausted rule). So if the brain is unreachable and we can't
	// tell how hot the destination is, staying quiet loses nothing — the page still
	// fires from there, which doesn't depend on this process being alive.
	loudWeeklyPct = 80.0 // switching TO an account at/above this % weekly = loud
	alwaysLoud    = false

	// Local model runtime (llama-server's Anthropic-compatible API,
	// #135). A STATIC provider like kimi: reached only as a member of a workload
	// class (the `local` class, claude-local), never by an id of its own. No
	// bearer: the wire strips the client's credentials. SWITCH_LOCAL_UPSTREAM
	// sets it; unset, "off" or "0" leaves it out of the table.
	localUpstream = ""

	// Hosted direct providers (see the direct-providers section). Set from env in
	// main(). No key lives here: each tenant's is its brain's (bearerWire).
	// kimiUpstream is Kimi-for-Coding's (sk-kimi-*) unless SWITCH_KIMI_UPSTREAM
	// says otherwise (Moonshot platform keys: https://api.moonshot.ai/anthropic);
	// package default "" keeps tests hermetic.
	kimiUpstream string
	// The gpt provider is the codex subscription backend (#33) — translated
	// in-proxy, bearer dispensed by the brain. See codex.go for the upstream/auth
	// model; the old LiteLLM + dev-key path (SWITCH_GPT_KEY/SWITCH_GPT_UPSTREAM)
	// is gone. Which gpt model a request names is the class member's `model`.

	// Claude Code's /model picker reads gateway rows ONLY from this client-side
	// cache file, and on subscription OAuth logins the client never populates it
	// itself (see the client-cache section below) — so the proxy maintains it.
	// Set in main(): ~/.claude/cache/gateway-models.json, and the baseUrl string
	// the client must see in it (must equal the shell's ANTHROPIC_BASE_URL).
	clientCachePath string
	clientBaseURL   string

	// clientSecret gates every request except GET /_switch/healthz (#35):
	// clients present it as Authorization: Bearer via
	// ANTHROPIC_AUTH_TOKEN. "" disables the gate (loopback deployment).
	clientSecret string
)

// ---- direct providers ----
//
// A direct provider is a non-Anthropic upstream: the request's model id is rewritten
// to the upstream's own id, and the provider's WIRE (#41, wire.go) does the rest —
// strip Anthropic auth for an open runtime like llama-server, inject the provider's own key
// for a hosted one, or translate both directions for an upstream that doesn't speak
// Anthropic Messages at all. The core routes on the provider and never branches on
// which protocol that is.
//
// No provider claims an id of its own: every one (local, kimi, gpt, runway) is
// reached only as a workload-class member (class.go), behind the brain's route
// and pay list. Their former direct ids are refused typed
// (hostedProviderNamespace).
type advertisedModel struct {
	pickerID   string // id shown in the picker + matched against the request model
	upstreamID string // model id rewritten into the body for the upstream
	display    string // picker display_name

	// kind / effort are launch-sets §15.2: each door serves its own model
	// list, and this one (/v1/models on the chat door) is the ONLY list of
	// chat models any service may take, so a row has to say what it is and
	// whether Claude Code's effort setting actually reaches it through this
	// proxy. Claude Code itself reads neither (id + display_name only), so
	// they are additive. Image classes are listed on the image door's own
	// /v1/models (images_classes.go) and are kept out of this one, hidden from
	// the picker.
	//
	// kind is one of the kind* constants below. effort is the honest answer for
	// THIS proxy, not the model's own capability:
	//
	//   - anthropic rows ride the lane path with the client's body untouched,
	//     so output_config.effort arrives exactly as sent ⇒ true.
	//   - a class row inherits the answer from the member it currently targets
	//     (classEffortReaches): an anthropic member is a lane hop, a provider
	//     member is one of the two provider cases.
	kind   string
	effort bool

	// family / version say WHAT an anthropic row is, so no consumer has to
	// parse an id to rank it (§15.3: "latest Opus" is the family-opus row with
	// the highest version, then the largest contextWindow). Derived from the
	// registry entry's name by anthropicNameParts, in that one place; empty on
	// every other kind, which has no such fact to state.
	family  string
	version string
	// contextWindow is the EFFECTIVE window — the same number renderedID
	// decides the "[1m]" mark on (registryEntry.effectiveWindow), so a row's
	// mark and its number can never disagree. 0 = not declared (an image
	// class has no token window), where the registry's "declared, never
	// inferred" rule leaves us nothing honest to say.
	contextWindow int
}

// The kinds a /v1/models row can carry (launch-sets §15.2). Wire values: a
// consumer groups rows by these strings.
const (
	kindAnthropic = "anthropic" // a real Anthropic id, served on the account lane
	kindClass     = "class"     // a brain-owned workload class, resolved per session
)

type provider struct {
	name   string   // "local" | "kimi" | "gpt" | "runway" — the class member's provider
	target *url.URL // parsed upstream
	wire   wire     // how this upstream is spoken to (#41: auth + any translation)
}

// providers is the direct-provider table, built in buildHandler from env (nil ⇒ none).
var providers []*provider

// providerSpec is a hosted provider an edition adds (its init appends to
// providerSpecs): fromEnv reads its env in main, and build makes its row on
// every buildHandlers, nil when it is not configured.
type providerSpec struct {
	name    string
	fromEnv func()
	build   func() *provider
}

// providerSpecs is every edition provider, in table order after local and kimi.
var providerSpecs []providerSpec

// hostedProviderNames are the providers buildHandler constructs itself;
// providerSpecs name the rest.
var hostedProviderNames = []string{"local", "kimi", "gpt"}

// hostedProviderNamespace reports whether model sits in a hosted provider's
// former picker namespace, "claude-<provider>-…" (claude-kimi-k3,
// claude-gpt-6.1-sol[1m], or any id a SWITCH_*_MODELS list once added), and
// which provider. Those ids were the policy-free direct path quota-model C2
// deleted (§2.2 row 10). Client picker caches have no TTL, so they still
// arrive; they carry digits, so the class-shaped refusal does not catch them,
// and laneForModel would hand them to api.anthropic.com under an account
// bearer. The namespace is config-independent on purpose: a provider whose key
// is unset today must refuse its old ids just as loudly. No Anthropic model id
// begins with either prefix.
func hostedProviderNamespace(model string) (string, bool) {
	for _, name := range hostedProviderNames {
		if strings.HasPrefix(model, "claude-"+name+"-") {
			return name, true
		}
	}
	for _, ps := range providerSpecs {
		if strings.HasPrefix(model, "claude-"+ps.name+"-") {
			return ps.name, true
		}
	}
	return "", false
}

// advertisedModels is the registry's advertisement-only Anthropic rows (#96 —
// listed so gateway discovery stops substituting them, routed by lane like any
// unadvertised id) and the active class rows derived from the brain's /preferred
// (#93; last-good snapshot when the brain is down — see classrows.go). No
// provider adds a row of its own.
func advertisedModels() []advertisedModel {
	return append(registryAnthropicRows(), registryClassRows()...)
}

// directProvidersStatus summarizes the provider table for /_switch/status. A
// provider advertises no id of its own, so its `models` is always empty.
func directProvidersStatus() []map[string]any {
	out := make([]map[string]any, 0, len(providers))
	for _, p := range providers {
		out = append(out, map[string]any{
			"name": p.name, "upstream": p.target.String(), "auth": wireOf(p).authLabel(), "models": []string{},
		})
	}
	return out
}

// providerEffortReaches follows provider identity, just like startup's wire
// selection: gpt uses codexWire, which translates the client's effort setting;
// kimi uses bearerWire and local uses localWire, neither of which applies it.
func providerEffortReaches(name string) bool {
	return name == "gpt"
}

// parseDirectTarget parses a provider upstream URL, logging and disabling on a bad one.
func parseDirectTarget(name, raw string) *url.URL {
	if raw == "" {
		return nil
	}
	if u, err := url.Parse(raw); err == nil && u.Host != "" {
		return u
	}
	log.Printf("bad %s upstream %q — provider disabled", name, raw)
	return nil
}

// brainClient is a short-timeout client for the brain's RPCs (/token, /class-bind …) (NOT the streaming
// transport used for proxied inference).
var brainClient = &http.Client{Timeout: 8 * time.Second}

// ---- per-model routing lanes ----
// The brain classifies models into "lanes" (fable/opus/…/default) and decides an
// account PER LANE, so e.g. fable traffic can ride one account while everything
// else stays on another — concurrently. The proxy is a dumb executor: it extracts
// the request's top-level "model", walks laneRules first-match, and routes that
// request on its lane's account. When the brain payload has no lanes (old brain),
// everything falls back to the single legacy account — exactly the old behavior.
type laneRule struct {
	Lane  string   `json:"lane"`
	Match []string `json:"match"` // lowercased substrings of the model id; empty on the default rule
	// Scope is the lane's scoped cap in the provider's words ("Fable"): the
	// brain's window "scoped:<Scope>" binds this lane. Empty for a lane with no
	// cap. Required of the brain: this proxy deploys after one that sends it (C5).
	Scope string `json:"scope"`
}

type lanePref struct {
	Account string `json:"account"`
	Reason  string `json:"reason"`
	// Effective = the lane's binding utilization per account: max(overall weekly,
	// the lane's scoped model cap). Drives per-lane alert tiering.
	Effective map[string]float64 `json:"effective"`
}

// ---- brain-cached preferred decision ----
type decision struct {
	mu        sync.RWMutex
	account   string
	reason    string
	weekly    map[string]float64
	laneRules []laneRule           // ordered, default last; nil => single-lane fallback
	lanes     map[string]lanePref  // per-lane account decisions
	classes   map[string]classPref // #27: full class view (members in priority order)
	// hardPct = the brain's SWITCH_HARD_PCT ("forces a switch to any lower
	// account"). The proxy's ONE local use is classCandidates: a member whose
	// known effective is at/over it is EXHAUSTED for the mid-request cross and
	// goes last. Zero (old brain / not yet fetched) => defaultHardPct.
	hardPct float64
	// The dial check's inputs (quota-model 50 §2.7): every account's standing
	// (its scoped windows' aliases are the scope map, C5), every route's
	// policy, the money pots' settings and pay_order, and the ledger's spend
	// this budget period (§1.7, C6).
	accounts map[string]acctStanding
	routes   map[string]routePref
	pots     map[string]potSettings
	payOrder []string
	ledger   *checkLedger
	// staleMax is the brain's STALE_MAX (settings.stale_max): a limit read
	// longer ago than this is stale, on both sides of the wire.
	staleMax float64
	// fleet is the brain's `fleet` section (quota-model 67 §8) as far as
	// session facts pass it on: nil when the last applied document carried
	// none, or one the proxy could not read. fleetAt is when that document
	// was applied (nowFn, session facts' clock): the figure is the brain's as
	// of then, however long the stream keeps talking without a new document.
	fleet   *brainFleet
	fleetAt time.Time
	// fleetErr is why the last document's fleet could not be read ("" when
	// it could), so a brain that keeps sending the same bad fleet is logged
	// once, not on every document.
	fleetErr string
	ok       bool
	// heardAt is when the brain was last heard from: a document applied, or
	// any bytes on the /events stream (a keepalive included).
	heardAt time.Time
}

// defaultHardPct mirrors the brain's SWITCH_HARD_PCT default; used only until a
// /preferred that carries thresholds has been decoded.
const defaultHardPct = 95.0

// laneParseFailures counts /v1 POSTs whose body yielded no top-level "model" (they
// route on the default lane = old behavior, but a Claude Code payload change must be
// visible, not silent). Exposed in /_switch/status.
var laneParseFailures int64

// ---- effective-account tracking (drives switch detection), PER LANE ----
//
// Per tenant: tenant.effMu, tenant.lastEffective (lane -> account that last
// served it) and tenant.lastState (lane -> most recent routeState, for
// /_switch/status).

// noteState records a lane's most recent routing outcome so /_switch/status (and the
// status-line script that reads it) can show whether a lane is ROUTED / STALE / FAILED.
func (tn *tenant) noteState(lane string, s routeState) {
	tn.effMu.Lock()
	tn.lastState[lane] = s
	tn.effMu.Unlock()
}

// ---- stats for /_switch/status ----
type stats struct {
	mu         sync.Mutex
	started    time.Time
	requests   int64
	switches   int64
	lastMethod string
	lastPath   string
	lastStatus int
	lastBearer string
	lastEff    string
	lastAt     time.Time
}

func (s *stats) record(method, path string, status int, bearerFP, eff string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests++
	s.lastMethod, s.lastPath, s.lastStatus, s.lastBearer, s.lastEff, s.lastAt =
		method, path, status, bearerFP, eff, time.Now()
}

var st = &stats{started: time.Now()}

type ctxKey int

const infoKey ctxKey = 0

// reqStartKey carries requestHandler's own `start` down to the ReverseProxy
// ErrorHandler, which is otherwise the one place that reports a failure with no
// idea how long the client waited for it. Stamped before any other ctx value so
// the class ladder's request rebuilds inherit it; the elapsed it yields is
// therefore since the INBOUND request, hops included.
//
// Its own unexported type, like classify.go's ctxKeyProxyDeadline and NOT a
// third `ctxKey` integer: ctxKey constants are handed out across files with no
// central registry (infoKey=0 here, wireKey=1 in wire_openai.go), so the next
// number is a guess. Guessing 1 here silently overwrote the OpenAI-wire marker
// with a time.Time — three e2e wire tests caught it. A struct key cannot
// collide with anything.
type reqStartKeyT struct{}

var reqStartKey reqStartKeyT

// routeState is the observable outcome of routing one account-lane request. There is
// no silent fallback: every account request is either injected with a chosen account's
// token or refused. The status line and /_switch/status surface this so a degraded
// route is always visible, never a mystery (the whole point of the redesign).
type routeState int

const (
	stateRouted routeState = iota // fresh brain decision; chosen account's token injected
	stateStale                    // brain unreachable, but riding the last-good decision on a still-valid cached token
	stateFailed                   // no usable token for any account — fail loud (503), never ride the client's ambient login
)

func (s routeState) String() string {
	switch s {
	case stateStale:
		return "STALE"
	case stateFailed:
		return "FAILED"
	default:
		return "ROUTED"
	}
}

// reqInfo travels on the request context from requestHandler through Rewrite (which
// injects .token) into the failover Transport (which needs .eff to pick an alternative).
// For an account lane .token is ALWAYS set (ROUTED/STALE) — the proxy never forwards the
// client's own bearer; a FAILED route is short-circuited to 503 before it gets here.
// .body is the request body buffered ONCE in requestHandler — the transport replays it
// on a 429 retry instead of re-reading (bodies can be multi-MB with images).
type reqInfo struct {
	tenant   *tenant     // whose request: every per-tenant read below the entry goes through it
	token    string      // bearer to inject (account lanes; also a wire's per-request credential)
	eff      string      // effective account label this request was routed to
	state    routeState  // ROUTED / STALE (account lanes; bypass leaves it zero-value)
	lane     string      // model lane this request classified into
	body     []byte      // buffered request body (nil when the request had none)
	bypass   bool        // route to a direct provider: no lanes, no 429 dance, no failover
	upstream *url.URL    // direct-provider upstream (bypass only)
	wire     wire        // #41: the provider's wire (bypass only) — owns auth + any translation
	classCtx *classRoute // #27: set on class-routed requests; enables the member 429 ladder
	// asked is the model the client named, from its bytes before any rewrite,
	// on a request that names a model directly: the model its session facts
	// are kept under (sessionModelKey). A class request's are kept under its
	// class.
	asked string

	// Filled by wire.prepare on a TRANSLATED wire (#41), meaningless otherwise:
	// body holds the translated request, token/account are the per-request
	// credential pair, and clientStream is the CLIENT's stream wish (the
	// upstream call itself may be stream-only regardless).
	account      string
	clientStream bool

	// poolAccount is the provider POOL member serving this hop (#71): the class
	// actuation sets the request ("" = follow the brain's pool incumbent) and
	// the wire's prepare writes back the name the brain actually resolved.
	// Distinct from account, which on codex is the ChatGPT workspace UUID.
	poolAccount string

	// ratio is the served provider's anthropic_token_ratio (#44), stamped at
	// actuation time (direct, class attempt 1, and every mid-request rebuild).
	// 0 or 1 = no scaling: Anthropic hops never set it, and legacy mode has no
	// declared ratios. The transport divides the response's input-side usage by
	// it so the client's compaction numerator is always in Anthropic units.
	ratio float64

	// scale is the usage-scaling provenance record (#76), created wherever ratio
	// is stamped and SHARED across mid-request rebuilds (classRoute.scale is the
	// same pointer), so whichever hop finally serves resolves the one record the
	// response tap reads at terminal publish. nil on account-lane requests.
	scale *scaleMark

	// check is the dial check that admitted this hop's account (§5.2: every
	// dial logs it).
	check dialCheck
}

func fp(auth string) string {
	tok := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
	if tok == "" {
		return "(none)"
	}
	h := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(h[:])[:8]
}

func readToken(path string) string {
	if path == "" {
		return ""
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// ---- brain client -----------------------------------------------------------

// brainFail flips pref.ok false, logging only the TRANSITION into failure (a
// client away from the brain's network is a normal, designed state — logging every failed
// reconnect there would be noise; logging nothing cost a diagnosis once).
func (tn *tenant) brainFail(why string, err error) {
	tn.pref.mu.Lock()
	was := tn.pref.ok
	tn.pref.ok = false
	tn.pref.mu.Unlock()
	if was {
		log.Printf("brain unreachable for %s (%s): %v — serving STALE (last-good decision on cached tokens) until it returns", tn.id, why, err)
	}
}

// prewarmWG tracks the fire-and-forget token prewarms below. PRODUCTION NEVER
// WAITS on it — not stalling the caller is the entire point of the prewarm, and
// its production callers are the brain stream and the prewarm timers (armPrewarm). Tests wait (fetchPreferredSync),
// because a helper that returns while unbounded goroutines are still reading
// package globals makes every later global write a data race. CI caught exactly
// that once: three e2e tests swap brainURL to a dead server to assert a
// refusal, and an in-flight prewarm was still reading it. The race was latent
// from #33 and surfaced only under -race on a loaded runner, which
// is the failure mode worth designing against — a green local run proved nothing.
var prewarmWG sync.WaitGroup

// applyPreferred decodes one /preferred document (the data of a `preferred`
// event on the tenant's /t/<tenant>/events stream) and, when it holds
// everything the proxy reads, makes it the tenant's decision. A document that
// does not is a decode failure: the view goes not-ok (brainFail) and the last
// good state stays. The class rows (/_switch/classes, /v1/models) are global
// and derive from the lan tenant's document alone.
func (tn *tenant) applyPreferred(body io.Reader) {
	pref := tn.pref
	var d struct {
		Account   string              `json:"account"`
		Reason    string              `json:"reason"`
		Weekly    map[string]float64  `json:"weekly"`
		LaneRules []laneRule          `json:"lane_rules"` // absent on an old brain -> single-lane fallback
		Lanes     map[string]lanePref `json:"lanes"`
		// Classes is a POINTER so key absence is distinguishable from an empty
		// object (#93): an old brain that omits the key says nothing about
		// classes (keep the last-good rows), while `"classes": {}` is a brain
		// positively stating there are none (tombstone everything).
		Classes *map[string]classPref `json:"classes"`
		// Consumers (#95) is read for ONE thing here: request_shape decides whether
		// a consumer's classes belong in the Claude Code picker. Absent on an old
		// brain -> empty map -> every class stays advertised, the pre-#95 behavior.
		Consumers []struct {
			ID           string `json:"id"`
			RequestShape string `json:"request_shape"`
		} `json:"consumers"`
		Thresholds struct {
			Hard float64 `json:"hard"`
		} `json:"thresholds"` // brain's SWITCH_*_PCT; only hard is consumed here (classCandidates)
		// The dial check's inputs (§2.7). Required, like Classes a pointer so
		// absence is seen: a brain that omits them (one older than C2) would
		// leave every route without policy, so its payload is a decode
		// failure and the last good state stays.
		Accounts *map[string]acctStanding `json:"accounts"`
		Routes   *map[string]routePref    `json:"routes"`
		Pots     map[string]potSettings   `json:"pots"`
		PayOrder []string                 `json:"pay_order"`
		// Ledger is required too (C6): the budget gate reads it, and a brain
		// older than C6 publishes none.
		Ledger   *checkLedger `json:"ledger"`
		Settings struct {
			StaleMax *float64 `json:"stale_max"`
		} `json:"settings"`
		// Fleet (quota-model 67 §8) is read only for session facts' room
		// figure, so it is decoded on its own (decodeFleet): display only, it
		// never decides whether the document's routing applies. Optional: a
		// brain older than 67 push 2 publishes none, and then no session
		// shows a room.
		Fleet json.RawMessage `json:"fleet"`
	}
	err := json.NewDecoder(body).Decode(&d)
	switch {
	case err != nil:
	// No top-level account is a brain whose account loop names no pick: it has
	// no Claude-plan account, or none passes. That document still routes: an
	// API-key brain's lanes are served by their routes' accounts (decideOn).
	case d.Routes == nil || d.Accounts == nil:
		err = fmt.Errorf("payload lacks routes or accounts (a brain older than C2): keeping the last good state")
	case d.Ledger == nil:
		err = fmt.Errorf("payload lacks ledger (a brain older than C6): keeping the last good state")
	case d.Settings.StaleMax == nil || *d.Settings.StaleMax <= 0:
		err = fmt.Errorf("payload lacks settings.stale_max: keeping the last good state")
	}
	if err != nil {
		tn.brainFail("decode", err)
		return
	}
	var classes map[string]classPref
	if d.Classes != nil {
		classes = *d.Classes
	}
	// The brain serves a route's id only as its key; the policy carries it
	// because the unavailable body (§2.8) names routes and reads their
	// accounts' checks by it.
	routes := make(map[string]routePref, len(*d.Routes))
	for id, r := range *d.Routes {
		r.ID = id
		routes[id] = r
	}
	fleet, fleetErr := decodeFleet(d.Fleet)
	pref.mu.Lock()
	wasOK := pref.ok
	left := departedAccounts(pref.accounts, *d.Accounts)
	pref.account, pref.reason, pref.weekly = d.Account, d.Reason, d.Weekly
	pref.laneRules, pref.lanes, pref.classes = d.LaneRules, d.Lanes, classes
	pref.hardPct = d.Thresholds.Hard // 0 when absent => defaultHardPct at the read site
	pref.accounts = mergeStandings(pref.accounts, *d.Accounts)
	pref.routes, pref.pots, pref.payOrder, pref.ledger = routes, d.Pots, d.PayOrder, d.Ledger
	pref.staleMax = *d.Settings.StaleMax
	pref.fleet, pref.fleetAt = fleet, nowFn()
	logFleet := fleetErr != nil && fleetErr.Error() != pref.fleetErr
	pref.fleetErr = ""
	if fleetErr != nil {
		pref.fleetErr = fleetErr.Error()
	}
	accounts := pref.accounts
	pref.ok, pref.heardAt = true, time.Now()
	pref.mu.Unlock()
	if logFleet {
		log.Printf("brain fleet unreadable, sessions show no room until a readable one: %v", fleetErr)
	}
	tn.dropDeparted(left)
	tn.pruneRecords(accounts)
	// The ledger just pulled covers what it covers; what it does not waits in
	// the reporter's queue (reports.go), which this wakes. Nothing is posted
	// here: this runs on the /events reader, and a post that waited on a slow
	// brain would stop it, and its silence watchdog would call a healthy
	// brain unreachable.
	tn.ledgerPulled(d.Ledger, unixSeconds(time.Now()))
	tn.reportWake()
	if !wasOK {
		log.Printf("brain reachable for %s: preferred=%s lanes=%d", tn.id, d.Account, len(d.LaneRules))
	}
	// #93 (B2): the classes payload is the source of the advertised class picker
	// rows — derive, tombstone what vanished, persist the last-good snapshot.
	// The rows are one registry for every tenant, read from the lan tenant's
	// document (the classes document is the operator's, one for all).
	if d.Classes != nil {
		if tn.lan {
			shapes := make(map[string]string, len(d.Consumers))
			for _, c := range d.Consumers {
				if c.ID != "" {
					shapes[c.ID] = c.RequestShape
				}
			}
			updateClassRowsFromPreferred(classes, shapes)
		}
		if tn.health != nil {
			// #115: freeze health keys that left the registry, unfreeze the
			// returning ones — the union across classes is the present set.
			present := map[string]bool{}
			for _, cp := range classes {
				for _, m := range cp.Members {
					present[m.ID] = true
				}
			}
			tn.health.syncRegistry(present)
		}
	}
	// Every account any lane routes to (not just the default): each actuator
	// pre-warms its own kind's credentials, so a freshly-flipped lane's first
	// request doesn't stall on a fetch.
	warm := map[string]bool{d.Account: true}
	for _, lp := range d.Lanes {
		if lp.Account != "" {
			warm[lp.Account] = true
		}
	}
	tn.warmAccounts(warm)
}

// laneDecision is one Anthropic dial's account (§2.2 rows 11, 16): the
// account, its bearer and its check — or, when no account of the route may be
// dialled, the refusal and every account's check (none).
type laneDecision struct {
	tok, acct, reason string
	state             routeState
	check             dialCheck
	none              bool       // every account's check is `no`: refuse, dial nobody
	refused           []acctPick // the accounts left out, soonest back first
	refusedRoute      string     // with none: the route refused, and the model it was asked for
	model             string
	// unactuated is, on a FAILED decision, the kind of the route's account
	// that this build has no actuator for (writeNoActuator); "" otherwise.
	unactuated string
	// notTenant is, on a FAILED decision, an account the route named that
	// the tenant's document does not list, when the route named no other
	// (writeNotTenantAccount: the typed 503 of MEMBERS-PLAN 1b); "" otherwise.
	notTenant string
}

// laneRouteID is a bare lane's route id.
func laneRouteID(lane string) string { return "lane:" + lane }

// decideRoute picks the account an Anthropic route dials: the brain's lane
// pick when its check is not `no`, else the route's other accounts as the swap
// ranks them (rankRouteAccounts: included before paid, a throttle deranked).
// When every account's check is `no` it refuses (none) — there is no probe of
// a rejected account and no riding one: its record ends when §2.6 says, and
// only then is it dialled again.
//
// State: ROUTED when the brain is reachable; STALE when it isn't and the
// cached standing still admits an account; FAILED when there is no brain
// decision at all or no admitted account's token can be had (the caller 503s
// rather than ride the client's ambient login).
//
// entitlementOnly is count_tokens (§2.2 row 20): it is free, so no capacity,
// pay or owner limit applies; any account backing the route answers it, the
// pick first.
//
// The manual inject file passes the check of the account whose token it holds
// (a token no account of this proxy holds has no reading: no (unknown)) and
// never grants pay: it serves only while that check is included.
func (tn *tenant) decideRoute(routeID, lane, model string, entitlementOnly bool) laneDecision {
	tn.pref.mu.RLock()
	p, r := tn.pref.account, tn.pref.reason
	if lp, has := tn.pref.lanes[lane]; has && lp.Account != "" {
		p, r = lp.Account, lp.Reason
	}
	tn.pref.mu.RUnlock()
	return tn.decideOn(routeID, lane, model, p, r, entitlementOnly)
}

// decideOn is decideRoute from the brain's pick p (why: r) for the route: the
// lane's pick for a bare lane, the member's incumbent for a class member.
func (tn *tenant) decideOn(routeID, lane, model, p, r string, entitlementOnly bool) laneDecision {
	tn.pref.mu.RLock()
	ok := tn.pref.ok
	tn.pref.mu.RUnlock()
	state := stateRouted
	if !ok {
		state = stateStale // brain down: the last-good decision, on cached standing and tokens
	}
	now := nowFn()
	// The operator's manual override / test hook is the household's alone: a
	// member's request is never served on a token the operator put on the host.
	if injectFile != "" && tn.lan {
		if t := readToken(injectFile); t != "" {
			eff := "manual:" + fp("Bearer "+t)
			a := tn.accountOfCredential(t)
			c, _ := tn.routeCheck(routeID, a, model, now)
			if entitlementOnly || c.included() {
				return laneDecision{tok: t, acct: eff, reason: "manual inject file", state: stateRouted, check: c}
			}
			who := a
			if who == "" {
				who = "a token no account holds"
			}
			return laneDecision{acct: eff, reason: "manual inject file: " + who + " is " + c.String(), state: stateRouted,
				check: c, none: true, refused: []acctPick{{account: a, check: c}}, refusedRoute: routeID, model: model}
		}
	}
	if p == "" {
		// The brain names no pick only where none of the route's accounts
		// passes; the ranking below says why, account by account. A route it
		// does not serve has no decision at all.
		if _, _, _, known := tn.routeOf(routeID); !known {
			return laneDecision{reason: "no brain decision yet", state: stateFailed} // nothing to route to
		}
	}
	// The credential boundary, before any check: an account the tenant's
	// document does not list is not the tenant's to dial, whatever the route
	// (or a wrong-tenant bug) names. Nothing is fetched or ranked for it.
	accts, notTenant := tn.routeAccounts(routeID), ""
	own := make([]string, 0, len(accts))
	for _, a := range accts {
		if _, _, err := tn.actuatorFor(a); err != nil {
			log.Printf("route %s: %v", routeID, err)
			if notTenant == "" {
				notTenant = a
			}
			continue
		}
		own = append(own, a)
	}
	if len(own) == 0 && notTenant != "" {
		return laneDecision{acct: p, reason: "account " + notTenant + " is not tenant " + tn.id + "'s",
			state: stateFailed, notTenant: notTenant}
	}
	cands, refused := tn.rankRouteAccounts(routeID, model, checkRequest{}, p, own, tn.lanePct(lane), now)
	if entitlementOnly {
		cands = append(cands, refused...)
		sort.SliceStable(cands, func(i, j int) bool { return cands[i].account == p && cands[j].account != p })
		refused = nil
	}
	if len(cands) == 0 {
		return laneDecision{acct: p, reason: "no account of " + routeID + " may be dialled: " + renderPicks(refused),
			state: state, none: true, refused: refused, refusedRoute: routeID, model: model}
	}
	unactuated := ""
	for _, c := range cands {
		act, kind, err := tn.actuatorFor(c.account)
		if err != nil {
			// Not the tenant's account (the document named one it does not
			// list): never dialled, as if absent from the route.
			log.Printf("route %s: %v", routeID, err)
			continue
		}
		if act == nil {
			// A kind this build cannot sign for: never dialled, and named.
			if unactuated == "" {
				unactuated = kind
			}
			continue
		}
		tok := act.credential(tn, c.account)
		if tok == "" {
			continue
		}
		why := r
		switch {
		case p == "":
			why = "the brain named no pick; best of " + routeID
		case c.account != p:
			why = "avoiding " + p + " (" + pickOf(p, cands, refused) + ")"
		}
		return laneDecision{tok: tok, acct: c.account, reason: why, state: state, check: c.check, refused: refused}
	}
	if unactuated != "" {
		return laneDecision{acct: p, reason: "no actuator for " + unactuated + " accounts in this build",
			state: stateFailed, unactuated: unactuated}
	}
	return laneDecision{acct: p, reason: "token unavailable for " + p, state: stateFailed}
}

// pickOf renders account's check among the judged picks.
func pickOf(account string, lists ...[]acctPick) string {
	for _, l := range lists {
		for _, p := range l {
			if p.account == account {
				s := p.check.String()
				if p.throttled {
					s += ", throttled"
				}
				return s
			}
		}
	}
	return "not an account of the route"
}

// lanePct is the use the swap orders an Anthropic route's accounts by: the
// lane's effective, else the account's weekly; nil when neither is known.
func (tn *tenant) lanePct(lane string) func(string) *float64 {
	tn.pref.mu.RLock()
	weekly := tn.pref.weekly
	var laneEff map[string]float64
	if lp, has := tn.pref.lanes[lane]; has {
		laneEff = lp.Effective
	}
	tn.pref.mu.RUnlock()
	return func(a string) *float64 {
		if v, ok := laneEff[a]; ok {
			return &v
		}
		if v, ok := weekly[a]; ok {
			return &v
		}
		return nil
	}
}

// failoverRoute is the same-lane account swap after `not` refused (§2.2 row
// 8): the next account rankRouteAccounts admits, never `not`, with a fetchable
// token. ok=false when there is none (the caller surfaces the refusal).
func (tn *tenant) failoverRoute(routeID, lane, model, not string) (string, string, dialCheck, bool) {
	cands, _ := tn.rankRouteAccounts(routeID, model, checkRequest{}, "", tn.routeAccounts(routeID), tn.lanePct(lane), nowFn())
	for _, c := range cands {
		if c.account == not {
			continue
		}
		if t := tn.credentialFor(c.account); t != "" {
			return t, c.account, c.check, true
		}
	}
	return "", "", dialCheck{}, false
}

// forceEffective sets a lane's effective account WITHOUT firing a switch alert — used
// after the Transport already fired one for a 429 failover, so the next request doesn't
// fire a duplicate when noteEffective sees the same change.
func (tn *tenant) forceEffective(lane, a string) {
	tn.effMu.Lock()
	tn.lastEffective[lane] = a
	tn.effMu.Unlock()
}

// noteEffective records which account served a lane's request and fires the switch
// alert on a real change (never on the lane's first request, never to/from an empty
// account). Detection is at REQUEST time: an idle lane reports its flip on the first
// request after it — the alert text says so.
func (tn *tenant) noteEffective(lane, effective, reason string) {
	tn.effMu.Lock()
	prev := tn.lastEffective[lane]
	tn.lastEffective[lane] = effective
	tn.effMu.Unlock()
	if prev != "" && effective != "" && prev != effective {
		st.mu.Lock()
		st.switches++
		st.mu.Unlock()
		tn.fireSwitchAlert(lane, prev, effective, reason)
	}
}

// ---- switchover alert coalescing ----
// When the shared overall-weekly component crosses the threshold, EVERY lane flips
// within seconds of each other (as their next requests arrive) — that must reach the
// bridge as ONE message, not a thundering herd of five. fireSwitchAlert queues; a
// short timer flushes the batch in a single bridge POST. Every enqueue funnels
// through gateAndEnqueue (alert_history.go), which gates dispatch on the recent
// alert history: identical-alert repeat window + lane-scoped critical cooldown.
var (
	alertMu    sync.Mutex
	alertQueue []queuedAlert
	alertTimer *time.Timer
	// bridgeURL is startup-written in prod, but flushSwitchAlerts reads it on a
	// timer goroutine that Stop() cannot un-fire — so tests swapping worlds must
	// write it under alertMu (swapBridgeURL) and cross-goroutine reads use
	// bridgeTarget(). alertMu is a leaf lock, never held across I/O.
	alertCoalesce = 2 * time.Second // var so tests can shrink it
)

const (
	switchTitleRoutine = "🔀 Claude account switchover"
	switchTitleLastHop = "🚨 Claude switched to its LAST account"
)

func (tn *tenant) fireSwitchAlert(lane, from, to, reason string) {
	log.Printf("🔀 SWITCHOVER[%s %s] %s -> %s (%s)", tn.id, lane, from, to, reason)
	pref := tn.pref
	// The hub's only publisher (#59). Deliberately here: above the bridgeURL return so
	// it fires with no bridge configured, and above pref.mu.RLock() so hub.mu never
	// nests inside pref.mu. A few flips a day, off the hot path — the request-path taps
	// are #60/#61.
	publish(tn.id, "switchover", map[string]any{"lane": lane, "from": from, "to": to, "reason": reason})
	if bridgeTarget() == "" {
		log.Printf("   (no SWITCH_BRIDGE_URL set — switch alert skipped)")
		return
	}
	pref.mu.RLock()
	weekly := pref.weekly
	lanePct, laneKnown := 0.0, false
	if lp, ok := pref.lanes[lane]; ok && lp.Effective != nil {
		lanePct, laneKnown = lp.Effective[to]
	}
	pref.mu.RUnlock()

	desc := fmt.Sprintf("Claude Code %s traffic moved from %s to %s. %s.", lane, from, to, reason)
	if weekly != nil {
		desc += fmt.Sprintf(" Weekly usage: %s=%.0f%%, %s=%.0f%%.", from, weekly[from], to, weekly[to])
	}
	if laneKnown {
		desc += fmt.Sprintf(" %s-effective usage on %s: %.0f%%.", lane, to, lanePct)
	}
	desc += fmt.Sprintf(" (Observed at the first %s request since the flip.)", lane)

	// Last hop? Tier by the LANE-EFFECTIVE utilization of the destination (its
	// overall weekly OR its scoped model cap, whichever binds — a fable move onto an
	// account at overall 41% but fable-cap 85% IS a last hop for fable). Fall back to
	// overall weekly when the brain payload predates lanes; unknown => quiet, because
	// external monitoring owns the emergency page and doesn't depend on us.
	toPct, known := 0.0, false
	if laneKnown {
		toPct, known = lanePct, true
	} else if weekly != nil {
		toPct, known = weekly[to]
	}
	loud := alwaysLoud || (known && toPct >= loudWeeklyPct)

	delivery, title := "telegram", switchTitleRoutine
	if loud {
		delivery = "critical"
		title = switchTitleLastHop
		desc += fmt.Sprintf(" This was the last hop for %s: %s is already at %.0f%%"+
			" %s-effective, so there is nowhere left to switch. Downshift the model"+
			" (Sonnet/Haiku burn a fraction of Opus/Fable) or buy extra-usage credits.",
			lane, to, toPct, lane)
	}
	log.Printf("   tier=%s (lane=%s to=%s pct=%.0f%% known=%v laneKnown=%v threshold=%.0f%% alwaysLoud=%v)",
		delivery, lane, to, toPct, known, laneKnown, loudWeeklyPct, alwaysLoud)

	entry := map[string]any{
		"status": "firing",
		"labels": map[string]string{
			"alertname": "ClaudeAccountSwitchover", "alertroute": "claude-usage",
			// delivery=critical => the receiver's critical push (bypasses DND) + its chat channel.
			// anything else => its chat channel only.
			"delivery": delivery, "lane": lane, "from_account": from, "to_account": to,
		},
		"annotations": map[string]string{
			// `title` drives the critical push title (when loud); `summary` is the body.
			"title":       title,
			"summary":     fmt.Sprintf("Claude Code %s traffic now using %s (was %s)", lane, to, from),
			"description": desc,
		},
	}
	rec := &alertRecord{alertname: "ClaudeAccountSwitchover", tenant: tn.id, lane: lane, from: from, to: to, tier: delivery}
	// Demote closure for gate 1 (lane-scoped critical cooldown): map writes
	// only — it runs under alertMu inside gateAndEnqueue.
	demote := func(ago time.Duration) {
		labels, _ := entry["labels"].(map[string]string)
		ann, _ := entry["annotations"].(map[string]string)
		labels["delivery"] = "telegram"
		ann["title"] = switchTitleRoutine
		ann["description"] += fmt.Sprintf(" (Demoted from critical: a critical alert for %s fired %s ago; cooldown %s.)",
			lane, ago.Round(time.Second), alertCritCooldown)
	}
	// Tiering evidence for the dispatch event — the same numbers the tier=
	// log line above prints, so the feed can answer "why this tier" without
	// the logs. to_pct is omitted (not zeroed) when unknown.
	tierInputs := map[string]any{"known": known, "loud_pct": loudWeeklyPct, "always_loud": alwaysLoud}
	if known {
		tierInputs["to_pct"] = toPct
	}
	out := gateAndEnqueue(rec, entry, reason, tierInputs, demote)
	if !out.enqueued {
		// Suppressed candidates never reach the flush, so their dispatch is
		// published here — enqueued ones publish from flushSwitchAlerts (see
		// alert_history.go's ordering contract).
		publish(tn.id, "alert_dispatch", out.dispatch)
		log.Printf("   suppressed: identical alert (lane=%s %s->%s) fired within %s", lane, from, to, alertRepeatWindow)
		return
	}
	log.Printf("   enqueued tier=%s demoted=%v", rec.tier, out.demoted)
}

// bridgeTarget reads bridgeURL under alertMu; swapBridgeURL replaces it and
// returns the old value. flushSwitchAlerts runs on a timer goroutine that
// Stop() cannot un-fire, so tests swapping worlds must write through
// swapBridgeURL and concurrent readers through bridgeTarget — a bare read of
// bridgeURL is only safe before serving starts (startup wiring, log lines).
func bridgeTarget() string {
	alertMu.Lock()
	defer alertMu.Unlock()
	return bridgeURL
}

func swapBridgeURL(url string) string {
	alertMu.Lock()
	defer alertMu.Unlock()
	old := bridgeURL
	bridgeURL = url
	return old
}

// alertFlushInFlight counts running flushSwitchAlerts goroutines. Test seam only
// (same reasoning as respTapSettle): a coalesce timer that already fired cannot be
// un-fired, so a test swapping alert worlds must be able to WAIT for the stale
// flush to finish — its publish() lands in whatever ring is armed at publish time,
// which under the race scheduler is the NEXT test's.
var alertFlushInFlight int64

func flushSwitchAlerts() {
	atomic.AddInt64(&alertFlushInFlight, 1)
	defer atomic.AddInt64(&alertFlushInFlight, -1)
	alertMu.Lock()
	batch := alertQueue
	bridge := bridgeURL
	alertQueue, alertTimer = nil, nil
	alertMu.Unlock()
	if len(batch) == 0 {
		return
	}
	// Publish the batch's dispatch events HERE, not at the fire sites: the
	// flush timer arms inside gateAndEnqueue under alertMu, so this goroutine
	// is the only place that can guarantee every dispatch frame precedes the
	// batch's delivery frame on the wire (alert_history.go's ordering
	// contract). Suppressed candidates never enqueue and publish at their
	// caller instead.
	ids := make([]string, 0, len(batch))
	for _, qa := range batch {
		publish(qa.rec.tenant, "alert_dispatch", qa.dispatch)
		ids = append(ids, qa.rec.id)
	}
	// delivery emits the batch's single alert_delivery event — every exit path
	// below reports one, success or not, so the feed never has a dangling
	// dispatched-but-unaccounted batch. bridgeStatus nil = no HTTP exchange.
	// One batch may hold several tenants' flips: the delivery event is the
	// proxy's, with no tenant.
	delivery := func(bridgeStatus any, delivered, rolledBack bool, channels map[string]any) {
		publish("", "alert_delivery", map[string]any{
			"ids": ids, "at": time.Now().UnixMilli(), "bridge_status": bridgeStatus,
			"delivered": delivered, "rolled_back": rolledBack, "channels": channels,
		})
	}
	// rollback returns the batch's history records to oblivion: the bridge never
	// durably saw these alerts, so they must not gate the next flip. A fire
	// landing between the drain above and the prune here is suppressed by
	// about-to-be-pruned records — at most one lost fire per failed batch,
	// self-healing; do NOT "fix" by moving the prune under the drain lock.
	rollback := func(why string) {
		recs := make([]*alertRecord, 0, len(batch))
		for _, qa := range batch {
			recs = append(recs, qa.rec)
		}
		pruneAlertRecords(recs...)
		log.Printf("   switchover alert history rolled back (%s) — the next flip will retry", why)
	}
	if bridge == "" {
		rollback("bridge URL unset")
		delivery(nil, false, true, nil)
		return
	}
	entries := make([]map[string]any, 0, len(batch))
	for _, qa := range batch {
		entries = append(entries, qa.entry)
	}
	// The bridge takes the push title from the FIRST alert in the payload — when the
	// batch is mixed, a critical entry must lead or the loud push gets a quiet title.
	sort.SliceStable(entries, func(i, j int) bool {
		li, _ := entries[i]["labels"].(map[string]string)
		lj, _ := entries[j]["labels"].(map[string]string)
		return li["delivery"] == "critical" && lj["delivery"] != "critical"
	})
	payload := map[string]any{"status": "firing", "alerts": entries}
	b, _ := json.Marshal(payload)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", bridge, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("   switchover alert POST failed: %v", err)
		rollback("POST error")
		delivery(nil, false, true, nil)
		return
	}
	// The bridge's JSON body carries per-channel booleans either way — a 502
	// still says WHICH channels failed, so read before branching on status.
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	channels := parseBridgeChannels(respBody)
	// the reference receiver returns 502 on a total delivery failure (chat channel AND
	// critical push both down) precisely so this rollback fires — a bridge 200 now means
	// at least one channel reached the operator (partial success counts).
	if resp.StatusCode >= 400 {
		rollback(fmt.Sprintf("bridge HTTP %d", resp.StatusCode))
		delivery(resp.StatusCode, false, true, channels)
		return
	}
	delivery(resp.StatusCode, true, false, channels)
	log.Printf("   %d switch alert(s) sent (bridge HTTP %d)", len(entries), resp.StatusCode)
}

// parseBridgeChannels lifts the per-channel booleans off the bridge's JSON
// reply ({"telegram": bool, "ha": bool|null, ...}). ha is tri-state on purpose:
// null means no critical was in
// the batch so the critical push was never attempted. A nil return means the body
// was unparsable or predates the channel fields — UNKNOWN, which the feed must
// render as absence, never as failure.
func parseBridgeChannels(body []byte) map[string]any {
	var r struct {
		Telegram *bool `json:"telegram"`
		Ha       *bool `json:"ha"`
	}
	if err := json.Unmarshal(body, &r); err != nil || r.Telegram == nil {
		return nil
	}
	ch := map[string]any{"telegram": *r.Telegram, "ha": nil}
	if r.Ha != nil {
		ch["ha"] = *r.Ha
	}
	return ch
}

// ---- HTTP plumbing ----------------------------------------------------------

type recorder struct {
	decision *decisionTrace
	http.ResponseWriter
	status int
	// mark is the request's ledgerMark: a reply written after one of its
	// dials entered a ledger entry carries x-should-retry: false.
	mark *ledgerMark
}

func (r *recorder) WriteHeader(code int) {
	r.decision.headers(r.Header())
	if r.mark.charged() {
		ledgerNoRetry(r.Header())
	}
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Flush runs on a SEPARATE goroutine: with FlushInterval -1, ReverseProxy's
// copyResponse arms an immediate-fire time.AfterFunc whose delayedFlush calls this
// from a timer goroutine, serialized against Write only by maxLatencyWriter's mutex.
// Anything added here that touches shared state — the response tap included — is a
// genuine race. Keep Flush and WriteHeader free of tap symbols (a test enforces it).
func (r *recorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// tapRecorder tees response bytes into the response tap (#61). A SEPARATE type, not a
// Write method on recorder, so SWITCH_TAP=off serves through the plain recorder with
// no Write override in existence — the tee is absent, not disarmed.
type tapRecorder struct {
	*recorder // promotes WriteHeader (status capture) and Flush
	tap       *respTap
}

// Write tees b[:n] — only the bytes the client-side writer actually accepted — so a
// short write can never make the aggregate claim bytes the client never got. (n, err)
// is preserved verbatim in every branch, including n>0 && err!=nil. All calls happen
// on the request goroutine inside maxLatencyWriter.Write, between its buffered write
// and its flush; feed's cost there is one bounded copy and one non-blocking send.
func (t *tapRecorder) Write(b []byte) (int, error) {
	n, err := t.recorder.ResponseWriter.Write(b)
	if n > 0 {
		t.tap.feed(b[:n], t.recorder.status, t.recorder.Header())
	}
	return n, err
}

// maxBodyBytes caps every buffered /v1/* request body (SWITCH_MAX_BODY_BYTES to
// tune). The buffer is retained up to ~3x per request (r.Body replay, reqInfo.body
// for the 429 failover, the transcript tap), so an uncapped ReadAll is a memory-
// amplification lever for anyone holding the client secret. 32 MiB clears the
// largest observed Claude Code payloads (multi-MB image turns) with a wide margin.
var maxBodyBytes int64 = 32 << 20

// bufferBody reads and restores r.Body so the model can be extracted here and the
// SAME buffer replayed by the failover transport via reqInfo.body — the body is
// buffered exactly once (payloads can be multi-MB with images). Also pins
// ContentLength, which matters for chunked bodies. A body over maxBodyBytes
// returns (nil, true) and leaves r.Body consumed — the caller answers 413 and
// must not forward the request.
func bufferBody(r *http.Request) ([]byte, bool) {
	if r.Body == nil {
		return nil, false
	}
	b, _ := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	r.Body.Close()
	if int64(len(b)) > maxBodyBytes {
		return nil, true
	}
	r.Body = io.NopCloser(bytes.NewReader(b))
	r.ContentLength = int64(len(b))
	return b, false
}

// extractModel returns the string value of the TOP-LEVEL "model" key of a JSON body,
// or "" if absent/unparseable. It walks tokens at depth 1 only and skips every other
// value wholesale, so a "model" key nested inside messages/tools/metadata can never
// match, and a 5MB body with model in the first few bytes costs almost nothing.
func extractModel(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	t, err := dec.Token()
	if err != nil {
		return ""
	}
	if d, ok := t.(json.Delim); !ok || d != '{' {
		return ""
	}
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return ""
		}
		key, _ := kt.(string)
		if key == "model" {
			vt, err := dec.Token()
			if err != nil {
				return ""
			}
			s, _ := vt.(string)
			return s
		}
		var skip json.RawMessage // consumes the value, however nested
		if err := dec.Decode(&skip); err != nil {
			return ""
		}
	}
	return ""
}

// laneForModel walks the brain-served rules in order, first match wins (the brain
// always ships the catch-all "default" rule last). No rules (old brain), no model,
// or no match => "default", which routes on the legacy single account.
func laneForModel(model string, rules []laneRule) string {
	m := strings.ToLower(model)
	if m == "" || len(rules) == 0 {
		return "default"
	}
	for _, r := range rules {
		for _, sub := range r.Match {
			if sub != "" && strings.Contains(m, strings.ToLower(sub)) {
				return r.Lane
			}
		}
	}
	return "default"
}

// ratelimitDetail renders every Anthropic-Ratelimit-* response header on the 429
// log line. The error body is generic ("would exceed your account's rate limit" —
// no window named), so these headers are the only upstream
// signal that can say WHICH limit tripped. Logged raw, on the lane and the class
// paths alike, so a token no scope claims yet (quota-model 10 U4) is on record
// for the brain's alias table.
func ratelimitDetail(h http.Header) string {
	var keys []string
	for k := range h {
		if strings.HasPrefix(k, "Anthropic-Ratelimit-") {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		short := strings.ToLower(strings.TrimPrefix(k, "Anthropic-Ratelimit-"))
		parts = append(parts, short+"="+h.Get(k))
	}
	return strings.Join(parts, " ")
}

// ---- gateway model discovery ----
// Claude Code with CLAUDE_CODE_ENABLE_GATEWAY_MODEL_DISCOVERY=1 calls GET /v1/models
// at startup (3s timeout, redirects = failure) and adds the returned ids to the
// /model picker as "From gateway" rows. The class rows ride it, and so do
// Anthropic's own ids since #96: gateway discovery treats /v1/models as
// the whole world and silently substitutes another model for a subagent asking
// for an id it does not list, so the registry's advertisement-only anthropic
// rows ride this payload. They still route by lane, exactly like an id nobody
// advertised. The picker therefore shows a gateway row beside the client's
// built-in row for the same model — a harmless duplicate, and the price of
// discovery honoring the id.

// modelsHandler serves gateway model discovery. Shape per the gateway protocol:
// the client reads data[].id and the optional data[].display_name, nothing else.
//
// kind + effort (launch-sets §15.2) ride along for the OTHER consumers: this
// endpoint is the only chat model list in the system (the image door lists
// its own classes), so a service rendering a launch picker needs to know what
// each row is and whether an effort setting would do anything on it. Claude
// Code ignores both extra keys (its bootstrap rows and its picker cache are
// separate surfaces, and writeClientModelsCache deliberately keeps its
// id/display_name-only shape). effort is always present, never omitempty:
// `false` is the answer for most rows and an absent key would read as
// "unknown".
func modelsHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	type entry struct {
		Type        string `json:"type"`
		ID          string `json:"id"`
		DisplayName string `json:"display_name"`
		Kind        string `json:"kind"`
		Effort      bool   `json:"effort"`
		// family/version are anthropic-only facts (§15.3) and omitted
		// elsewhere: an empty string would read as "this row has a family and
		// it is blank". context_window is always present — 0 says "not
		// declared", which is a different statement from absent.
		Family        string `json:"family,omitempty"`
		Version       string `json:"version,omitempty"`
		ContextWindow int    `json:"context_window"`
	}
	ams := advertisedModels()
	data := make([]entry, 0, len(ams))
	for _, m := range ams {
		data = append(data, entry{Type: "model", ID: m.pickerID, DisplayName: m.display,
			Kind: m.kind, Effort: m.effort,
			Family: m.family, Version: m.version, ContextWindow: m.contextWindow})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"data": data, "has_more": false})
	st.record(r.Method, r.URL.Path, http.StatusOK, fp(r.Header.Get("Authorization")), "")
	log.Printf("%-4s %-28s -> 200  model discovery: advertised %d direct model(s)  %s",
		r.Method, r.URL.Path, len(data), time.Since(start).Round(time.Millisecond))
}

// ---- /model picker rows via the client's cache file ----
//
// Discovery has two halves: GET /v1/models (served above) and the client actually
// CALLING it. On a subscription OAuth login the client never calls: its fetcher insists
// on ANTHROPIC_AUTH_TOKEN or an API key just to build an auth header for the GET and
// silently bails with neither — and a subscription login has neither (verified in the
// 2.1.207 binary; faking either env var would hijack real request auth). The /model
// picker itself reads ONLY the cache file the fetcher would have written:
// ~/.claude/cache/gateway-models.json — no TTL, no re-fetch, just a baseUrl string
// match against the shell's ANTHROPIC_BASE_URL. We run on the same machine and ARE
// the gateway, so we keep that cache in sync ourselves, in exactly the shape the
// client writes ({baseUrl, fetchedAt, models:[{id, display_name}]}, mode 0600). If a
// future client fixes the OAuth fetch path it will simply overwrite the file with the
// same rows — fetched from us anyway.

type clientModelEntry struct {
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
}

type clientModelsFile struct {
	BaseURL   string             `json:"baseUrl"`
	FetchedAt int64              `json:"fetchedAt"`
	Models    []clientModelEntry `json:"models"`
}

// writeClientModelsCache syncs the picker cache at path to the advertised models,
// returning whether it wrote. An empty set never clobbers an existing file (all
// providers down ≠ models gone — keep the last-good rows, same philosophy as localTags
// for /v1/models).
func writeClientModelsCache(path, baseURL string, models []advertisedModel) (bool, error) {
	if path == "" || len(models) == 0 {
		return false, nil
	}
	next := clientModelsFile{BaseURL: baseURL, FetchedAt: time.Now().UnixMilli()}
	for _, m := range models {
		next.Models = append(next.Models, clientModelEntry{ID: m.pickerID, DisplayName: m.display})
	}
	if prev, err := os.ReadFile(path); err == nil {
		var cur clientModelsFile
		if json.Unmarshal(prev, &cur) == nil && cur.BaseURL == baseURL && slices.Equal(cur.Models, next.Models) {
			return false, nil
		}
	}
	buf, err := json.Marshal(next)
	if err != nil {
		return false, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return false, err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o600); err != nil {
		return false, err
	}
	return true, os.Rename(tmp, path)
}

// removeClientModelsCache deletes the picker cache iff it is ours (baseUrl matches):
// with local models disabled those rows would route to Anthropic and 404, so leaving
// them in the picker is worse than removing the file. Foreign baseUrls are not ours
// to delete.
func removeClientModelsCache(path, baseURL string) bool {
	buf, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var cur clientModelsFile
	if json.Unmarshal(buf, &cur) != nil || cur.BaseURL != baseURL {
		return false
	}
	return os.Remove(path) == nil
}

// rewriteModel replaces the TOP-LEVEL "model" value in body with newModel by splicing
// bytes: no re-marshal (bodies run to multi-MB and a round-trip would also reorder
// keys), and the same depth-1 token walk as extractModel, so a nested "model" key can
// never be touched. Returns nil when there is no top-level string "model" to replace.
func rewriteModel(body []byte, newModel string) []byte {
	if len(body) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	t, err := dec.Token()
	if err != nil {
		return nil
	}
	if d, ok := t.(json.Delim); !ok || d != '{' {
		return nil
	}
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return nil
		}
		key, _ := kt.(string)
		if key == "model" {
			keyEnd := dec.InputOffset() // just past the key's closing quote
			vt, err := dec.Token()
			if err != nil {
				return nil
			}
			if _, ok := vt.(string); !ok {
				return nil
			}
			valEnd := dec.InputOffset() // just past the value's closing quote
			nv, _ := json.Marshal(newModel)
			out := make([]byte, 0, len(body)-int(valEnd-keyEnd)+len(nv)+1)
			out = append(out, body[:keyEnd]...)
			out = append(out, ':')
			out = append(out, nv...)
			out = append(out, body[valEnd:]...)
			return out
		}
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			return nil
		}
	}
	return nil
}

// failoverTransport wraps the base transport and, on a 429 to a /v1 request, retries the
// same request once against the other account (Stage 4 — reactive load failover). It runs
// below ReverseProxy so all of the proxy's streaming/header handling is preserved: the
// caller sees only the winning (retried) response.
type failoverTransport struct {
	base      http.RoundTripper
	anthropic *url.URL // the Anthropic target, for cross-member class retries (#27)
}

func (t *failoverTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	info, _ := req.Context().Value(infoKey).(reqInfo)
	if info.tenant == nil {
		info.tenant = tenantFrom(req.Context())
	}
	tn := info.tenant
	if tn == nil {
		// No tenant on the hop: nothing below may read anyone's state. A
		// request that reached the transport without one is a bug in an
		// entry, answered as the typed 503, never on an account.
		return noTenantResponse(req), nil
	}
	// Drop the client's Accept-Encoding on EVERY upstream hop and let the
	// transport own gzip (it decodes transparently when it negotiated it), so
	// every body this proxy inspects — usage rewrites, 429 classification, the
	// ⚠️ log captures, the tap — arrives plain. This was once scoped to scaled
	// hops; a 429 showed why it can't be: Claude Code offers br, Anthropic
	// brotli'd its 429 bodies, and classify429 silently failed on the compressed
	// bytes — so the "ambiguous ⇒ cool the whole account" arm was the only arm
	// that ever ran, and a capped fable lane starved opus onto an exhausted
	// account. rebuildForMember still does its own Del for a mid-request cross.
	req.Header.Del("Accept-Encoding")
	trace := decisionFrom(req.Context())
	if info.classCtx == nil {
		trace.head(info)
	}
	// sent is before any hop of this request reached an upstream: what serves
	// is evidence only against the records written before it (noteServedHop).
	sent := nowFn()
	resp, err := t.dispatch(req, info)
	if err != nil || resp == nil {
		return resp, err
	}
	// Whichever hop finally served — direct, class attempt 1, or a mid-request
	// cross INTO a translated member — if it was a translating wire's upstream,
	// the client must get an Anthropic-shaped response back. resp.Request
	// survives the ladder's rebuilds, so it names the final hop.
	hop := resp.Request
	if hop == nil {
		hop = req
	}
	// Use the SERVED hop's own info when it has one. A mid-request cross
	// rebuilds the request for a different member and stamps that member's info
	// on the new context; the outer info still names attempt 1, so using it
	// would mislabel the error envelope. Falls back to the outer info for every
	// hop the ladder didn't rebuild (the direct path, and class attempt 1 —
	// both of which already carry the right one).
	if hi, ok := hop.Context().Value(infoKey).(reqInfo); ok {
		info = hi
	}
	// §5.4 property 2 (#123): a synthetic health 529 flows out of RoundTrip
	// untouched — translate would replace its header map (destroying
	// Retry-After / X-Switch-Health) and the scaler would misread the body.
	// The quota twin rides the same exemption for the same reasons, and it
	// matters more there: X-Switch-Quota and Retry-After ARE the refusal.
	if cc := info.classCtx; cc != nil && (cc.healthExhaust || cc.unavail != nil) {
		trace.headers(resp.Header)
		return resp, nil
	}
	// X-Switch-Quota is ours alone: it is what tells the HTTPS-proxy entry's D7
	// stripper that the unified rate-limit headers on a response are the ones
	// WE minted for the class. An upstream that sent it would switch that off
	// for its own pooled-account numbers.
	resp.Header.Del("X-Switch-Quota")
	tn.noteServedHop(hop, info, resp, t.anthropic.Host, sent)
	if cc := info.classCtx; cc != nil && cc.image != nil {
		// An image hop (§2.9): the door reads the upstream's own reply through
		// the wire's image side, so nothing here may translate, scale or
		// annotate it. Its readings and records are taken above, like any hop's;
		// its head was scored at the head site, and the door settles a 200's
		// health row once it has read the image (hpImagesScore).
		trace.headers(resp.Header)
		return resp, nil
	}
	// #124: route evidence is frozen before translate can start its goroutine.
	// Healthy bound replies take no body filter or aggregate buffer.
	if cc := info.classCtx; cc != nil && resp.StatusCode == http.StatusOK {
		cc.note = prepareResponseNote(cc)
	}
	if info.classCtx == nil {
		trace.head(info)
	}
	if w := wireFor(hop); w != nil {
		// upstreamStatus before translate: the non-200 branch REWRITES the body
		// but keeps the status, so post-translate it no longer discriminates.
		upstreamStatus := resp.StatusCode
		out := w.translate(resp, info)
		// #122 codex NON-STREAM producer (§3 rows 3/5/20/21): the aggregate path
		// collapses the upstream SSE into one JSON reply inside translate, so
		// the tap never sees it — score off the translated status here. Head
		// rows (non-200 upstream) were already recorded at the head site.
		if info.classCtx != nil && info.classCtx.tenant.health != nil && upstreamStatus == 200 {
			if cw, ok := w.(codexWire); ok && !cw.clientStream(info) {
				cc := info.classCtx
				skey := cc.served.snapshot().member
				hs := &hopScore{ctx: hop.Context(), key: skey, cc: cc, lease: cc.leaseFor(skey)}
				o := hopOutcome{phase: "post_head", final: true}
				// Rule 0 with its exception (§3): a 200 or the incomplete 500
				// means response.completed/incomplete arrived — the hop
				// finished and a cancel after it changes nothing. A 502 does
				// not say whether the upstream failed or our cancel cut the
				// read, so under a client cancel it is row 1, no sample.
				canceled := errors.Is(hop.Context().Err(), context.Canceled)
				switch {
				case out.StatusCode == 200:
					o.row, o.sig, o.evidence = "5", sigHealthy, evOK
				case out.StatusCode == 500:
					// The aggregate's incomplete refusal (#132) — the only 500 it makes.
					o.row, o.sig, o.cause, o.ladder, o.evidence = "21", sigClient, "upstream_incomplete", ladderReturn, evNone
				case canceled:
					o.row, o.sig, o.ladder, o.evidence = "1", sigCancelled, ladderStop, evNone
				case out.StatusCode == 502:
					o.row, o.sig, o.cause, o.ladder, o.evidence = "3", sigDegraded, "aggregate_502", ladderReturn, evDeg
				default:
					o.row, o.sig, o.ladder, o.evidence = "20", sigUnknown, ladderReturn, evNone
				}
				hs.settle(o)
			}
		}
		// Translated streams append inside response.completed, never in the
		// translator's synthetic truncation terminal. JSON appends here.
		if cc := info.classCtx; cc != nil && !wantsStream(cc.origBody) {
			appendResponseNote(out, cc.note)
		}
		trace.headers(out.Header)
		return out, nil
	}
	// A pass-through wire may still reshape a refusal into the Anthropic
	// contract (local: llama-server's over-window 400, wire.go); bearer and
	// runway return resp as is. Only when the served hop IS that provider's: a
	// provider→anthropic cross does not re-stamp info (rebuildForMember).
	if info.wire != nil && info.upstream != nil && hop.URL != nil && hop.URL.Host == info.upstream.Host {
		resp = info.wire.translate(resp, info)
	}
	// The served event's tokens (served.go), read off the upstream's own bytes
	// before the health tap or the #44 scaler wraps the body.
	tapServed(hop, resp)
	// #122 passthrough producer: a provider hop with NO wire (kimi et al) and
	// an anthropic-wire hop both deliver the upstream body as-is — tap it for
	// §3 shape evidence (rows 1/2/3/4a/4b/5/20). Installed BEFORE the #44
	// scaler wraps the body so the tap reads the upstream's own bytes.
	if info.classCtx != nil && info.classCtx.tenant.health != nil && resp.StatusCode == 200 {
		cc := info.classCtx
		resp.Body = newHealthTap(resp.Body, hop.Context(), cc, cc.served.snapshot().member, wantsStream(cc.origBody))
	}
	// #44: a pass-through provider hop (kimi) speaks the Anthropic shape but
	// reports usage in its OWN tokenizer's units — ~0.61 of Anthropic's for the
	// same bytes — which coasts the client past its name-inferred window before
	// compaction fires. Divide the input-side numbers by the member's declared
	// ratio. The Host guard keeps a provider→anthropic mid-request cross honest:
	// that rebuild deliberately does not re-stamp reqInfo (see rebuildForMember),
	// so the outer info still carries attempt 1's ratio while the served hop is
	// Anthropic and already speaks Anthropic units.
	if r := info.ratio; r > 0 && r != 1 && hop.URL.Host != t.anthropic.Host {
		scaleProviderUsage(resp, r, info.scale)
	} else {
		// #76: resolve "nothing to scale" for an Anthropic/unscaled hop.
		info.scale.resolveNone()
	}
	if cc := info.classCtx; cc != nil && cc.note != nil {
		// A failed JSON scaling read restores the raw bytes but consumes its
		// read error. Do not annotate that partial reply, even if it parses.
		if mark, _ := info.scale.view(); mark.reason != "stream_truncated" {
			appendResponseNote(resp, cc.note)
		}
	}
	trace.headers(resp.Header)
	return resp, nil
}

// noteServedHop reads the final hop's response into the record store. Every
// response's limit headers feed the overlay (§1.6: the newest partial reading
// per account). A 2xx is the freshest evidence there is that its account has
// headroom on what it served, when the hop's check drew on that allowance
// (included, or a PR11 re-admission): it ends that account's throttle, its
// unscoped windows' records, the served route's scoped window and the model's
// record (noteServed). A paid 2xx says only that money served; the records
// that keep routes which may not pay off the account stand. Refusals were
// recorded where the ladder met them.
//
// The 2xx ends only records written before sent, when the request left. A
// record written later may be a sibling dial's refusal on the same account
// that the upstream answered after this success, and nothing here can tell
// which came first; one this request's own walk met stands too, and ends by
// reading or by time. Once an image batch's success, read just after
// a sibling's 429, cleared that 429's spent record, and the batch answered the
// raw 429 instead of the class's unavailable reply.
func (tn *tenant) noteServedHop(hop *http.Request, info reqInfo, resp *http.Response, anthropicHost string, sent time.Time) {
	if hop.URL == nil {
		return
	}
	now := nowFn()
	var account, scope, model, provider string
	switch {
	case hop.URL.Host == anthropicHost:
		account = tn.accountOfHop(hop.Header)
		scope = tn.laneScope(servedLane(hop, info))
		model = extractModel(info.body)
		provider = "anthropic"
	case info.classCtx != nil:
		// A provider hop of a class: the account the served-hop record names
		// (the pool account, or the provider's one account).
		sh := info.classCtx.served.snapshot()
		account = sh.account
		provider = servedMemberProvider(info.classCtx, sh.member)
	}
	if cc := info.classCtx; cc != nil {
		model = cc.served.snapshot().model
	}
	if account == "" {
		return
	}
	// The headers are parsed once, at the instant the head arrived: the served
	// event's readings and the overlay's, and so the reading reports, are the
	// same parse (quota-model 66 §1).
	hr, hasReading := tn.parseHeaderReading(resp.Header, now)
	noteServedCap(hop, info, resp, anthropicHost, account, model, now, hr)
	if hasReading {
		tn.keepHeaderReading(account, hr)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		chk := servedCheck(hop, info)
		switch {
		case chk.onAllowance():
			tn.noteServed(account, scope, model, sent)
		case chk.paid():
			// The pot paid: an unpaid record on it is disproved (§2.6).
			tn.notePaidServed(account, strings.TrimPrefix(chk.Pot, account+"/"), sent)
		}
		tn.noteSessionFunding(info.classCtx, sessionFunding{Tier: chk.funding()})
		tn.noteServedSession(hop, info, account, model, provider, chk)
	}
}

// servedMemberProvider is the provider id of the class member that served
// ("anthropic" for an anthropic member), from the class's member snapshot;
// "" when the snapshot does not name it.
func servedMemberProvider(cc *classRoute, memberID string) string {
	for _, m := range append([]classMemberPref{cc.member}, cc.members...) {
		if m.ID != memberID {
			continue
		}
		if m.Kind == "anthropic" {
			return "anthropic"
		}
		return m.Provider
	}
	return ""
}

// noteServedSession records what served a session's request for
// GET /_switch/session (session_facts.go): a class request by its session
// under its class, a model named directly by the client's session header
// under the model the client asked for (not the upstream model the hop
// sent). count_tokens is free and says nothing of what serves the session,
// so it writes nothing.
func (tn *tenant) noteServedSession(hop *http.Request, info reqInfo, account, model, provider string, chk dialCheck) {
	f := sessionFact{account: account, model: model, route: chk.route, tier: chk.funding(),
		provider: provider, lane: servedLane(hop, info)}
	session := hop.Header.Get("x-claude-code-session-id")
	key := sessionModelKey(info.asked)
	if cc := info.classCtx; cc != nil {
		if strings.HasSuffix(cc.inPath, "/count_tokens") {
			return
		}
		sh := cc.served.snapshot()
		session, f.class, f.member = cc.session, cc.class, sh.member
		// Held to its member by a pin only when the hop served the member
		// the pin names; a hop that left it (a failover) was not.
		f.pinned = cc.pinFact && sh.member == cc.boundMember
		key = classModelKey(cc.class)
		if sh.lane != "" {
			f.lane = sh.lane
		}
	} else if strings.HasSuffix(hop.URL.Path, "/count_tokens") {
		return
	}
	if provider != "anthropic" {
		f.lane = "" // a lane is an Anthropic account's; a provider hop has none
	}
	tn.noteSessionServed(session, key, f)
}

// servedCheckKey carries the check that admitted a hop a swap or the class
// walk re-issued on another account without re-stamping infoKey.
type servedCheckKey struct{}

// anthDialKey carries, beside servedCheckKey, the Anthropic account a swap or
// the class walk dials and the body it sends (an anthDial): on those hops
// infoKey still names the request's first account and body. A first hop
// carries none, and anthDialOf reads its reqInfo.
type anthDialKey struct{}

type anthDial struct {
	tenant *tenant // whose account: the reauth asks its brain routes, signs by its actuator
	acct   string
	body   []byte
}

// withAnthDial stamps the account and body a hop dials, for the request's
// tenant (tenantFrom ctx).
func withAnthDial(ctx context.Context, acct string, body []byte) context.Context {
	return context.WithValue(ctx, anthDialKey{}, anthDial{tenant: tenantFrom(ctx), acct: acct, body: body})
}

// anthDialOf is the account and body an Anthropic hop dials: the swap's or
// walk's stamp, else a first hop's reqInfo when the bearer on the hop is the
// one Rewrite injected from it.
func anthDialOf(req *http.Request) (anthDial, bool) {
	if d, ok := req.Context().Value(anthDialKey{}).(anthDial); ok {
		return d, true
	}
	info, ok := req.Context().Value(infoKey).(reqInfo)
	tn := tenantFrom(req.Context())
	if !ok || tn == nil || info.token == "" || tn.signedCredential(req.Header, info.eff) != info.token {
		return anthDial{}, false
	}
	return anthDial{tenant: tn, acct: info.eff, body: info.body}, true
}

// servedCheck is the check that admitted the hop that served: the swap's or
// walk's stamp, else the hop's own info.
func servedCheck(hop *http.Request, info reqInfo) dialCheck {
	if c, ok := hop.Context().Value(servedCheckKey{}).(dialCheck); ok {
		return c
	}
	return info.check
}

func (t *failoverTransport) dispatch(req *http.Request, info reqInfo) (*http.Response, error) {
	if info.classCtx != nil {
		// Class-routed (#27): its own bounded 429 ladder. Dispatched BEFORE the
		// path check — a provider-actuated attempt's outbound path embeds the
		// upstream base path (kimi: /coding/v1/…) and would fail the /v1/ guard.
		return t.classRoundTrip(req, info)
	}
	if wireFor(req) != nil {
		// A translated wire (#41, today: codex): its outbound path is the
		// upstream's own (/backend-api/…), so this must come before the /v1/
		// guard. No account failover — but one 401 retry on a freshly acquired
		// credential (the chain may have rotated under us).
		return t.tripWithReauth(req)
	}
	if !strings.HasPrefix(req.URL.Path, "/v1/") {
		return t.base.RoundTrip(req) // only inference calls are eligible for retry
	}
	if info.bypass {
		// Provider hop (local, kimi, runway): there is no other account to
		// fail over to, its 429 isn't an Anthropic quota event, and cooling an account
		// over it would be nonsense. Surface the provider's response verbatim.
		return t.base.RoundTrip(req)
	}
	if info.lane == "" {
		info.lane = "default" // request bypassed requestHandler (tests) — behave legacy
	}
	tn := info.tenant

	// requestHandler already buffered the body (info.body) — reuse it for the replay.
	// Read it here only when the request bypassed the handler.
	body := info.body
	if body == nil && req.Body != nil {
		body, _ = io.ReadAll(req.Body)
		req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.ContentLength = int64(len(body))
		if info.token != "" && tn.signedCredential(req.Header, info.eff) == info.token {
			req = req.WithContext(withAnthDial(req.Context(), info.eff, body)) // the reauth replays this body
		}
	}

	// A 401 on a rotated copy is re-fetched and retried here, before the
	// refusal below is classified: a 429 on the retry still walks.
	resp, err := t.tripWithReauth(req)
	if err != nil || resp == nil || resp.StatusCode < 400 {
		return resp, err
	}

	// The adapter classifies the refusal (refusal.go, §2.6); only the classes
	// that walk swap the account. Anything else goes back as it came.
	model := extractModel(body)
	ref := tn.anthropicRefusal(resp, model, nowFn())
	if !ref.walks() {
		return resp, err
	}
	if ref.class == refWrongMember {
		// #90: Anthropic refused this CLIENT's request shape (subscription bearer
		// + a non-Claude-Code system prompt), not the account's quota. Cooling or
		// flipping the account would punish a healthy lane for a client that can
		// never be served on it. Surface the 429 intact.
		log.Printf("⚠️  429 on %s lane=%s is a client-shape rejection (message \"Error\", no ratelimit headers) — not capacity; no cooldown, no failover",
			info.eff, info.lane)
		return resp, err
	}
	if strings.HasSuffix(req.URL.Path, "/count_tokens") {
		// §2.2 row 20: count_tokens is free and asks entitlement only. Its
		// refusal writes no record and swaps nothing; it goes back as it came.
		return resp, err
	}
	// The refusal is recorded on what it names (records.go): a rejected window
	// closes that window on this account for every route it binds; a 429 that
	// rejects nothing is a throttle, which only deranks.
	kind, what := tn.recordRefusal(info.eff, ref, resp)
	snippet := string(ref.body)
	if len(snippet) > 300 {
		snippet = snippet[:300] + "…"
	}
	log.Printf("⚠️  %d on %s lane=%s %s=%s ratelimit[%s] body=%q",
		resp.StatusCode, info.eff, info.lane, kind, what, ratelimitDetail(resp.Header), snippet)

	route := laneRouteID(info.lane)
	ftok, facct, fc, ok := tn.failoverRoute(route, info.lane, model, info.eff)
	if !ok {
		return resp, err // no other account may be dialled — surface the refusal
	}
	if kind == recThrottled && fc.paid() {
		// §2.3: a throttle is not exhaustion. Money is not spent to dodge it;
		// the client's own retry is the answer.
		log.Printf("⚠️  %d on %s is a throttle and the only other account is paid (%s) — surfacing it", resp.StatusCode, info.eff, fc)
		return resp, err
	}
	resp.Body.Close()
	log.Printf("⚠️  %d on %s — auto-failing over to %s check=%s and retrying", resp.StatusCode, info.eff, facct, fc)
	tn.forceEffective(info.lane, facct)
	st.mu.Lock()
	st.switches++
	st.mu.Unlock()
	tn.fireSwitchAlert(info.lane, info.eff, facct, "hit a usage limit ("+ref.class+") — auto-flipped mid-request")

	req2 := req.Clone(withAnthDial(context.WithValue(req.Context(), servedCheckKey{}, fc), facct, body))
	req2.Body = io.NopCloser(bytes.NewReader(body))
	req2.ContentLength = int64(len(body))
	tn.signAccount(req2.Header, facct, ftok) // always inject the alt account's credential
	resp2, err2 := t.tripWithReauth(req2)
	if err2 == nil && resp2 != nil && resp2.StatusCode >= 400 {
		// One swap keeps the ladder bounded; the second refusal is recorded so
		// the next request's decide sees it, and goes back to the client.
		if kind, what := tn.recordRefusal(facct, tn.anthropicRefusal(resp2, model, nowFn()), resp2); kind != "" {
			log.Printf("⚠️  %d on %s lane=%s (account swap) %s=%s ratelimit[%s]",
				resp2.StatusCode, facct, info.lane, kind, what, ratelimitDetail(resp2.Header))
		}
	}
	return resp2, err2
}

// tripWithReauth executes one hop, and retries it ONCE on a 401 with a freshly
// acquired credential: a 401 means the credential rotated or was revoked under
// us and the issuer may already hold the successor. A translating wire's hop
// re-acquires through its wire; an Anthropic hop on an account's injected copy
// through the account's actuator (accountRefused). Every other hop passes through
// untouched, so the ladders can call this unconditionally. A hop whose wire is
// not one HTTP exchange (Runway's task, runway_wire.go) is the wire's own trip.
func (t *failoverTransport) tripWithReauth(req *http.Request) (*http.Response, error) {
	if info, ok := req.Context().Value(infoKey).(reqInfo); ok {
		if tw, ok := info.wire.(tripWire); ok && info.wire.owns(req) {
			return tw.trip(req, info)
		}
	}
	resp, err := t.base.RoundTrip(req)
	if err != nil || resp == nil || resp.StatusCode != 401 {
		return resp, err
	}
	w := wireFor(req)
	if w == nil {
		return t.accountRefused(req, resp)
	}
	// Clone before reauth so the wire stamps the fresh credential straight onto
	// the retry. reauth drops the cached credential FIRST, so even a bail-out
	// below still forces the next request to re-acquire.
	req2 := req.Clone(req.Context())
	if !w.reauth(req2) || req.GetBody == nil {
		return resp, err // nothing fresher to offer — surface the 401 (translated upstack)
	}
	body, berr := req.GetBody()
	if berr != nil {
		return resp, err
	}
	log.Printf("%s: upstream 401 — retrying once on a re-acquired credential", req.URL.Host)
	resp.Body.Close()
	req2.Body = body
	return t.base.RoundTrip(req2)
}

// accountRefused hands Anthropic's 401 on an account's injected credential to
// that account's actuator. A hop on no account of ours (another upstream, the
// manual inject file) and a kind whose actuator has no answer return the 401
// as it came.
func (t *failoverTransport) accountRefused(req *http.Request, resp *http.Response) (*http.Response, error) {
	if t.anthropic == nil || req.URL.Host != t.anthropic.Host {
		return resp, nil
	}
	d, ok := anthDialOf(req)
	if !ok || d.acct == "" || strings.HasPrefix(d.acct, "manual:") {
		return resp, nil
	}
	if a, _, err := d.tenant.actuatorFor(d.acct); err == nil && a != nil && a.refused != nil {
		return a.refused(t, req, resp)
	}
	return resp, nil
}

func isLoopbackListenAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func main() {
	listen := flag.String("listen", "127.0.0.1:8787", "listen address")
	flag.Parse()

	brainBase = strings.TrimRight(os.Getenv("SWITCH_BRAIN_URL"), "/")
	bridgeURL = os.Getenv("SWITCH_BRIDGE_URL")
	switchKey = os.Getenv("SWITCH_KEY")
	if switchKey == "" {
		switchKey = readToken(os.Getenv("SWITCH_KEY_FILE"))
	}
	injectFile = os.Getenv("SWITCH_INJECT_TOKEN_FILE")
	if v := os.Getenv("SWITCH_429_COOLDOWN"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			forcedCooldown = time.Duration(n) * time.Second
		}
	}
	// Escape hatch: SWITCH_ALERT_ALWAYS_LOUD=1 restores "every switchover pages
	// critical" with no rebuild — just set it and restart.
	if v := os.Getenv("SWITCH_ALERT_ALWAYS_LOUD"); v == "1" || strings.EqualFold(v, "true") {
		alwaysLoud = true
	}
	if v := os.Getenv("SWITCH_MAX_BODY_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			maxBodyBytes = n
		}
	}
	if v := os.Getenv("SWITCH_LOUD_WEEKLY_PCT"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 && f <= 100 {
			loudWeeklyPct = f
		}
	}
	if v := os.Getenv("SWITCH_ALERT_CRIT_COOLDOWN"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			alertCritCooldown = time.Duration(n) * time.Second
		}
	}
	if v := os.Getenv("SWITCH_ALERT_REPEAT_WINDOW"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			alertRepeatWindow = time.Duration(n) * time.Second
		}
	}
	if v := strings.TrimRight(os.Getenv("ANTHROPIC_UPSTREAM"), "/"); v != "" {
		upstream = v // override for tests (mock upstream); defaults to api.anthropic.com
	}
	if v := strings.TrimRight(os.Getenv("SWITCH_LOCAL_UPSTREAM"), "/"); v != "" {
		if strings.EqualFold(v, "off") || v == "0" {
			localUpstream = ""
		} else {
			localUpstream = v
		}
	}
	// Hosted direct providers (Kimi/GPT). Neither holds a key: each tenant's
	// credential is its brain's. "off" disables a provider.
	wireInitFromEnv()
	kimiUpstream = "https://api.kimi.com/coding"
	if v := strings.TrimRight(os.Getenv("SWITCH_KIMI_UPSTREAM"), "/"); v != "" {
		if strings.EqualFold(v, "off") || v == "0" {
			kimiUpstream = ""
		} else {
			kimiUpstream = v
		}
	}
	// The gpt provider (#33): the codex subscription backend, translated in-proxy.
	// Resolved here ONLY (package default "" keeps tests hermetic); "off"
	// disables the provider. Requires the brain — the bearer is dispensed there.
	codexUpstream = "https://chatgpt.com"
	if v := strings.TrimRight(os.Getenv("SWITCH_CODEX_UPSTREAM"), "/"); v != "" {
		if strings.EqualFold(v, "off") || v == "0" {
			codexUpstream = ""
		} else {
			codexUpstream = v
		}
	}
	// An edition's providers and its second front door read their own env.
	for _, ps := range providerSpecs {
		ps.fromEnv()
	}
	for _, h := range entryHooks {
		if h.fromEnv != nil {
			h.fromEnv()
		}
	}
	// Picker cache (see the client-cache section): baseUrl must STRING-equal the
	// shell's ANTHROPIC_BASE_URL or the client ignores the file — the default
	// matches the documented `export ANTHROPIC_BASE_URL=http://<listen>` exactly.
	clientBaseURL = "http://" + *listen
	if v := os.Getenv("SWITCH_CLIENT_BASE_URL"); v != "" {
		clientBaseURL = v
	}
	if home, err := os.UserHomeDir(); err == nil {
		clientCachePath = filepath.Join(home, ".claude", "cache", "gateway-models.json")
	}
	if v := os.Getenv("SWITCH_CLIENT_MODELS_CACHE"); v != "" {
		if strings.EqualFold(v, "off") || v == "0" {
			clientCachePath = ""
		} else {
			clientCachePath = v
		}
	}
	// Client gate (#35). Same secret/file precedence as SWITCH_KEY.
	clientSecret = os.Getenv("SWITCH_CLIENT_SECRET")
	if clientSecret == "" {
		clientSecret = readToken(os.Getenv("SWITCH_CLIENT_SECRET_FILE"))
	}
	if clientSecret == "" && !isLoopbackListenAddr(*listen) {
		log.Fatalf("refusing ungated non-loopback listener %q: configure SWITCH_CLIENT_SECRET or bind an explicit loopback address", *listen)
	}
	// Per-model registry (#36). Resolved here ONLY — the package default "" keeps
	// tests (which never run main) in hermetic legacy mode, never reading a
	// production models.json.
	if home, err := os.UserHomeDir(); err == nil {
		modelsFilePath = filepath.Join(home, ".claude-switch-proxy", "models.json")
	}
	if v := os.Getenv("SWITCH_MODELS_FILE"); v != "" {
		if strings.EqualFold(v, "off") || v == "0" {
			modelsFilePath = ""
		} else {
			modelsFilePath = v
		}
	}
	// Effective-window floor for the renderer-owned "[1m]" suffix (#120). Resolved
	// here ONLY, like modelsFilePath: the package default 1M keeps every test on
	// the literal pre-existing rule.
	if v := os.Getenv("SWITCH_1M_MARK_FLOOR"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			oneMegaMarkFloor = n
		} else {
			log.Printf("registry: ignoring SWITCH_1M_MARK_FLOOR=%q — want a positive integer", v)
		}
	}
	// Class-row last-good state path (#93). Resolved here ONLY — the package
	// default "" keeps tests hermetic (no persistence), exactly like
	// modelsFilePath. The LOAD happens after buildHandler below.
	if home, err := os.UserHomeDir(); err == nil {
		classStateFilePath = filepath.Join(home, ".claude-switch-proxy", "class-rows.json")
	}
	if v := os.Getenv("SWITCH_CLASS_STATE_FILE"); v != "" {
		if strings.EqualFold(v, "off") || v == "0" {
			classStateFilePath = ""
		} else {
			classStateFilePath = v
		}
	}
	// Response tap kill switch (#61). Resolved here ONLY; default on. Off means the
	// tapRecorder wrapper is never installed — no Write override exists at all.
	if v := os.Getenv("SWITCH_TAP"); strings.EqualFold(v, "off") || v == "0" {
		respTapEnabled = false
	}
	// Force the Anthropic 1-hour prompt-cache TTL on every served request (default ON),
	// so no client needs the ENABLE_PROMPT_CACHING_1H env var (see cache1h.go). Kill
	// switch: SWITCH_FORCE_1H_CACHE=off (or 0).
	forceCache1h = true
	if v := os.Getenv("SWITCH_FORCE_1H_CACHE"); strings.EqualFold(v, "off") || v == "0" {
		forceCache1h = false
	}
	// Degraded-provider failover mode (docs/degraded-failover/60-spec-v2.md §8).
	healthModeV, healthEnforceFor = parseHealthMode(os.Getenv("SWITCH_HEALTH"))
	healthAllDegradedEnabled = strings.EqualFold(strings.TrimSpace(os.Getenv("SWITCH_HEALTH_ALL_DEGRADED")), "on")
	healthNotesEnabled = !strings.EqualFold(os.Getenv("SWITCH_HEALTH_NOTES"), "off")
	firstEventBudget = parseFirstEventBudget(os.Getenv("SWITCH_FIRST_EVENT_S")) // first_event.go
	if tag := budgetTag(firstEventBudget); tag != "" {
		log.Printf("first-event budget: %s", tag)
	}
	if healthModeV != healthOff {
		// Observe records evidence; enforce also consumes it for class routing.
		// One store per tenant, each persisted beside the configured file
		// (healthStatePath); tenant.start makes them.
		healthStateBase = "/var/lib/switch/health.json"
		if v := os.Getenv("SWITCH_HEALTH_STATE_FILE"); v != "" {
			healthStateBase = v
		}
		if v := os.Getenv("SWITCH_HEALTH_LARGE_PROMPT_TOK"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				healthLargePromptTok = n
			}
		}
		// Recovery probes run on a separate connection pool; recovery never
		// occupies a customer's hop.
		healthRecoveryOn = strings.EqualFold(strings.TrimSpace(os.Getenv("SWITCH_HEALTH_RECOVERY")), "on")
		log.Printf("🩺 SWITCH_HEALTH=%v (enforced classes: %v, notes enabled: %t)", healthModeV, healthEnforceClasses(), healthNotesEnabled)
	}
	gated, core := buildHandlers()
	// Boot the class-row store (#93) AFTER buildHandler and BEFORE the brain
	// stream goroutine and the listener start: a live /preferred must replace
	// the snapshot (never the reverse), and "advertising must work brain-down"
	// means the rows are in place for the first request. Loading never touches
	// the network, so startup still never blocks on the brain.
	loadClassRowsState()

	// The tenants (MEMBERS-PLAN 1b): the last index from beside the class-rows
	// snapshot first, so a brain outage at boot does not blind the household;
	// then the brain's own, kept by its /tenants/events. Each tenant's start
	// opens its own /t/<tenant>/events stream and reporter.
	//
	// No brain configured -> direct-providers-only: account lanes have nothing to route
	// to and FAIL loud (there is no ambient passthrough anymore). Production always sets
	// SWITCH_BRAIN_URL, so this is a tests/misconfig path, not a supported production mode.
	if classStateFilePath != "" {
		tenantsStateFilePath = filepath.Join(filepath.Dir(classStateFilePath), "tenants.json")
	}
	loadTenantIndexState()
	mode := "direct (no brain — account lanes will FAIL)"
	if brainBase != "" {
		mode = "brain"
		go func() {
			if err := fetchTenantIndex(context.Background()); err != nil {
				log.Printf("tenants: GET /tenants at boot: %v — serving the last-good index (%d tenants) until /tenants/events delivers one", err, len(allTenants()))
			}
			runTenantEvents(context.Background())
		}()
	}

	srv := &http.Server{Addr: *listen, Handler: gated, ReadHeaderTimeout: 10 * time.Second}
	// An edition's second front door dispatches into the same core chain. Its
	// start NEVER stops the process — :8787 must keep serving.
	for _, h := range entryHooks {
		if h.start != nil {
			h.start(core)
		}
	}
	log.Printf(product+" (%s) on %s -> %s", mode, *listen, upstream)
	if clientSecret != "" {
		log.Printf("client gate: ON (fp=%s) — everything except GET /_switch/healthz requires the client secret", fp("Bearer "+clientSecret))
	} else {
		log.Printf("client gate: off — no SWITCH_CLIENT_SECRET; run this way only on a loopback listener")
	}
	log.Printf("brain=%q (index %s/tenants, one stream per tenant at %s/t/<tenant>/events, silent after %s) bridge=%v 429-cooldown=%s", brainBase, brainBase, brainBase, brainSilence, bridgeURL != "", forcedCooldown)
	log.Printf("events: GET /_switch/events (SSE, epoch=%s) — ring %d frames / %d MiB, %d subscribers max, keepalive %s",
		evEpoch, evRingSize, evRingMaxBytes>>20, evMaxSubs, evKeepalive)
	if alwaysLoud {
		log.Printf("switch-alerts: ALWAYS LOUD (every switchover -> critical push; damped: repeats within crit-cooldown demote to Telegram) crit-cooldown=%s repeat-window=%s",
			alertCritCooldown, alertRepeatWindow)
	} else {
		log.Printf("switch-alerts: tiered — Telegram, escalating to a critical push only on the last hop (to-account >= %.0f%% weekly); crit-cooldown=%s repeat-window=%s",
			loudWeeklyPct, alertCritCooldown, alertRepeatWindow)
	}
	if injectFile != "" {
		log.Printf("MANUAL INJECT override active (fp=%s)", fp("Bearer "+readToken(injectFile)))
	}
	// The provider table + registry are built by buildHandler (above), so the picker-cache
	// decision comes after them: sync while anything is advertisable BY CONFIG, remove our
	// file when nothing is. Config-shaped on purpose — a momentarily-empty advertised set
	// (the brain down at a tick) must never delete the client's picker cache.
	// An anthropic-only registry (#96) counts as advertisable and DOES sync: the removal
	// rationale below ("rows would 404 at Anthropic") is about provider-owned picker ids
	// that only this proxy can serve. Anthropic rows carry real Anthropic ids, so a cached
	// row still resolves even if the proxy is out of the path entirely.
	if advertisableConfigured() {
		names := make([]string, 0, len(providers))
		for _, p := range providers {
			names = append(names, fmt.Sprintf("%s->%s (%s)", p.name, p.target, wireOf(p).authLabel()))
		}
		log.Printf("direct providers: %s — picker via gateway discovery (client needs CLAUDE_CODE_ENABLE_GATEWAY_MODEL_DISCOVERY=1)", strings.Join(names, ", "))
		if clientCachePath != "" {
			log.Printf("client model cache: syncing %s (baseUrl=%s)", clientCachePath, clientBaseURL)
			go func() {
				for {
					models := advertisedModels()
					if wrote, err := writeClientModelsCache(clientCachePath, clientBaseURL, models); err != nil {
						log.Printf("client model cache: %v", err)
					} else if wrote {
						log.Printf("client model cache: wrote %d direct model(s)", len(models))
					}
					time.Sleep(60 * time.Second)
				}
			}()
		}
	} else {
		log.Printf("direct providers: none")
		if clientCachePath != "" && removeClientModelsCache(clientCachePath, clientBaseURL) {
			log.Printf("client model cache: removed %s (no direct providers — rows would 404 at Anthropic)", clientCachePath)
		}
	}
	// Shutdown (#126 D5). With no second entry there is no drain, as before:
	// the process ends at once on SIGTERM, but only after the reports' one
	// bounded post (reportsFlush), so waiting entries are not lost to it. An
	// edition's entry owns the whole sequence (entryHook.shutdown).
	// ErrServerClosed is handled either way (revision R10) — a graceful
	// shutdown must not print a spurious fatal line that a deploy
	// check reading the log tail would then surface on unrelated failures.
	sigCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()
	shutdownDone := make(chan struct{})
	go func() {
		<-sigCtx.Done()
		shutdown(srv)
		close(shutdownDone)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	// srv.Shutdown closes the listener as its FIRST act, so ListenAndServe
	// above returns ErrServerClosed while the drain is still running.
	// Without this wait main() falls off the end and the runtime exits the
	// process mid-drain: hpDrain, hp.finish(), the reports' flush and the
	// "shutdown: done" line never ran, and every restart cut in-flight routed
	// requests. D5 and R10 are only real because of this line.
	<-shutdownDone
}

// buildHandler assembles the full proxy handler from the current globals. Split out
// of main() so e2e tests can run the real thing under httptest.NewServer.
// upstreamHeaderTimeout caps how long an upstream may take to send response
// HEADERS before the hop is abandoned. A var, not a const, so tests can shrink
// it: the failure it guards is a two-minute silence, which is not something a
// test can afford to wait out (see TestUpstreamStallReturns504).
var upstreamHeaderTimeout = 120 * time.Second

// upstreamErrorResponse turns a transport failure into the status + envelope the
// CLIENT sees. Its predecessor collapsed every error into one flat 502
// product + ": upstream failure", and that cost a
// diagnosis twice over: the status hid that the hop had TIMED
// OUT rather than failed, and the body named neither the member nor how long we
// waited. The truth was in OUR log and nowhere on the wire, so the only thing
// left describing the failure downstream was the agent gateway's failover classifier —
// a status table (TRANSIENT_HTTP_ERROR_CODES) that buckets every 5xx except 529
// as "timeout" — and the failure notice was that one word.
//
// The error TYPE is picked for what the far end DOES with it, not for novelty.
// The agent gateway reads `server_error`/`upstream_error` bodies as a server_error reason
// and infers "timeout" from the status alone otherwise. So the timeout arm
// deliberately uses a type that matches neither: the "timeout" the agent gateway then
// infers is CORRECT, and the detail it cannot model rides the message, which is
// what reaches its rawError= log line and providerErrorMessagePreview.
func upstreamErrorResponse(r *http.Request, err error) (status int, errType, msg string) {
	// Who we were talking to. The class member is the useful name (kimi/kimi-k3);
	// .eff is the account-lane fallback; "upstream" only if neither is stamped.
	// On the HTTPS-proxy entry the reader of this body is a client machine,
	// not us, and .eff is a POOL ACCOUNT's email — which has no business in a
	// client-visible error. Class member ids (kimi/kimi-k3) stay: they are a
	// bounded set and name a vendor, not a person. Base-URL output is
	// byte-identical, because entryFrom is the zero value there.
	redact := entryFrom(r.Context()).kind != ""
	who := "upstream"
	if info, ok := r.Context().Value(infoKey).(reqInfo); ok {
		switch {
		case info.classCtx != nil && info.classCtx.member.ID != "":
			who = info.classCtx.member.ID
		case info.eff != "" && redact:
			who = "the routed account"
		case info.eff != "":
			who = info.eff
		}
	}
	waited := ""
	if t0, ok := r.Context().Value(reqStartKey).(time.Time); ok {
		waited = " after " + time.Since(t0).Round(time.Second).String()
	}

	// Rule 0, as in classify.go: Canceled can only have come from OUTSIDE our own
	// deadlines, i.e. the client hung up. Nothing will read this body — the point
	// is that the log line and the status counter stop calling it an upstream
	// failure. 499 is nginx's convention and matches §3 row 1's "cancelled".
	if errors.Is(err, context.Canceled) {
		return 499, "cancelled", product + ": client cancelled the request" + waited
	}
	// A proxy-owned deadline (wire_openai's WithTimeout) or a transport timeout —
	// ResponseHeaderTimeout surfaces as a net.Error with Timeout() true, both on
	// h2 ("http2: timeout awaiting response headers") and h1.
	var ne net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
		return http.StatusGatewayTimeout, "timeout_error",
			product + ": upstream timeout — no response headers from " + who + waited
	}
	return http.StatusBadGateway, "upstream_error",
		product + ": upstream failure on " + who + waited + ": " + err.Error()
}

// newUpstreamTransport builds the ONE transport every upstream hop rides.
// Extracted from buildHandler so its timeout/liveness settings are assertable
// (TestTransportHasHTTP2Liveness) rather than being a literal only reachable by
// reading the code — the whole point of the h2 ping knobs is that nothing else
// notices when they are missing until a request hangs for two minutes.
func newUpstreamTransport() *http.Transport {
	return &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: upstreamHeaderTimeout,
		// HTTP/2 liveness. Without a health check, a connection that died
		// SILENTLY — no FIN, no GOAWAY, which is what a spent kimi rung does —
		// stays in the idle pool and swallows the next request whole: the stream
		// is written into the void and nothing fires until ResponseHeaderTimeout,
		// two minutes later. That is not a hypothetical: every 502 this proxy
		// once served on one class was exactly
		// that shape (`http2: timeout awaiting response headers`, elapsed
		// 120.0xx s, always a REUSED conn — the first request of each session,
		// on a fresh conn, answered in seconds; so did every genuine refusal, in
		// 1-2 s). It cost a scheduled job two runs and reported itself as
		// the single word "timeout".
		//
		// SendPingTimeout is the idle silence after which we PING; PingTimeout is
		// how long the answer may take before the conn is closed and its streams
		// error out. 30s/10s means a corpse is reaped while it is still idle —
		// the NEXT request then dials fresh instead of inheriting the grave — and
		// a request already in flight on one fails in ~40s instead of 120s.
		// Stdlib since Go 1.24 (http.Transport.HTTP2), so this stays a
		// zero-dependency module: no golang.org/x/net/http2 import.
		HTTP2: &http.HTTP2Config{
			SendPingTimeout: 30 * time.Second,
			PingTimeout:     10 * time.Second,
		},
	}
}

// buildHandler is buildHandlers' gated half — the :8787 handler, assembled
// exactly as before #126 split the function. Every existing caller (main and
// the whole e2e suite) keeps using it.
func buildHandler() http.Handler {
	gated, _ := buildHandlers()
	return gated
}

// buildHandlers assembles the handler stack once and returns both halves:
//
//	gated — clientGate(clientSecret, mux), i.e. the :8787 handler, unchanged.
//	core  — requestHandler(proxy), the routing chain BELOW the gate and below
//	        the /_switch mux, which the HTTPS-proxy entry (#126) dispatches two
//	        literal (method, path) pairs into.
//
// Returning core rather than re-building it is the whole point: both entries
// must share one failoverTransport, one registry, one inject queue and one set
// of lane metrics, so a second entry can never become a second routing path.
func buildHandlers() (gated, core http.Handler) {
	target, err := url.Parse(upstream)
	if err != nil {
		log.Fatalf("bad upstream: %v", err)
	}

	transport := newUpstreamTransport()

	// Build the direct-provider table from env (rebuilt on every buildHandler so tests
	// that flip the globals get a clean table). Each is enabled only when configured:
	// local by its upstream, kimi by its key, gpt by its upstream, an edition's
	// (providerSpecs) by its own.
	providers = nil
	if t := parseDirectTarget("SWITCH_LOCAL_UPSTREAM", localUpstream); t != nil {
		providers = append(providers, &provider{name: "local", target: t, wire: localWire{}})
	}
	if t := parseDirectTarget("SWITCH_KIMI_UPSTREAM", kimiUpstream); t != nil {
		providers = append(providers, &provider{name: "kimi", target: t, wire: bearerWire{account: "kimi"}})
	}
	if codexUpstream != "" {
		if t := parseDirectTarget("SWITCH_CODEX_UPSTREAM", codexUpstream); t != nil {
			providers = append(providers, &provider{name: "gpt", target: t, wire: codexWire{}})
		}
	}
	for _, ps := range providerSpecs {
		if p := ps.build(); p != nil {
			providers = append(providers, p)
		}
	}
	for _, h := range entryHooks {
		if h.build != nil {
			h.build()
		}
	}

	// Per-model registry (#36): the advertisement-only Anthropic rows (#96).
	registry, registrySource, registryErr = loadRegistry(modelsFilePath)

	// One failover transport for every routed request: the ReverseProxy's,
	// and the image door's (images_api.go), which dials one image at a time
	// through it.
	classTransport = &failoverTransport{base: transport, anthropic: target}
	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			if info, ok := pr.In.Context().Value(infoKey).(reqInfo); ok && info.bypass && info.upstream != nil {
				pr.SetURL(info.upstream)
				pr.Out.Host = info.upstream.Host
				// The wire owns everything provider-specific from here (#41):
				// auth, and for a translated wire the backend path, a fresh
				// header set, and a replayable body for the 401 retry.
				if info.wire != nil {
					info.wire.apply(pr.Out, info)
				} else {
					// Unreachable: every bypass route carries a wire. Strip rather
					// than leak the client's bearer to a provider.
					pr.Out.Header.Del("Authorization")
					pr.Out.Header.Del("X-Api-Key")
				}
				return
			}
			pr.SetURL(target)
			pr.Out.Host = target.Host
			// anthropic-beta / anthropic-version forwarded verbatim; the account's
			// actuator signs (actuator.go) and adds what its kind requires.
			if info, ok := pr.In.Context().Value(infoKey).(reqInfo); ok && info.token != "" && info.tenant != nil {
				info.tenant.signAccount(pr.Out.Header, info.eff, info.token)
				// The body's ttl:"1h" rewrite happens at the Anthropic actuation sites
				// (replay-safe); the matching beta capability goes on here. This branch is
				// Anthropic-only, so kimi/codex never receive it.
				if forceCache1h {
					ensureExtendedCacheBeta(pr.Out.Header)
				}
			}
		},
		Transport:     classTransport,
		FlushInterval: -1, // stream SSE immediately
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			status, errType, msg := upstreamErrorResponse(r, err)
			logErr := err
			if entryFrom(r.Context()).kind != "" {
				// *url.Error stringifies the FULL url, query included. A
				// base-URL request's query is ours; a request routed in from the
				// HTTPS-proxy entry carries whatever the client machine put
				// there. Unwrap on that path only — the base-URL line stays
				// byte-identical, which the existing suite pins.
				logErr = hpUnwrapURLError(err)
			}
			log.Printf("upstream error %s %s -> %d: %v", r.Method, r.URL.Path, status, logErr)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"type":  "error",
				"error": map[string]any{"type": errType, "message": msg},
			})
		},
	}

	mux := http.NewServeMux()
	// Live event stream (#59). A literal pattern outranks the "/_switch/" subtree below
	// (Go 1.22 precedence: it matches a strict subset), so switchHandler keeps its
	// unconditional application/json without it ever reaching this stream. Gated for
	// free by clientGate, like everything except /_switch/healthz.
	mux.HandleFunc("GET /_switch/events", eventsHandler)
	mux.HandleFunc("POST /_switch/inject", injectHandler)
	mux.HandleFunc("/_switch/", switchHandler)
	// Gateway model discovery. Served directly (never proxied upstream): the
	// client's 3s timeout + no-redirect rule mean this must answer fast and in
	// place. Anthropic's built-in models stay in the picker regardless.
	// Unconditional since #36: an empty advertised set answers {"data":[]} —
	// strictly better than the old zero-provider fallthrough, which lane-
	// classified with model="" and forwarded with an injected account bearer.
	mux.HandleFunc("GET /v1/models", modelsHandler)
	core = requestHandler(proxy)
	// #96 OpenAI inbound wire: POST /v1/chat/completions is normalized to an
	// Anthropic-shaped request and re-dispatched through the SAME core chain
	// under a context marker — never a parallel routing path. Any other method
	// on the path falls through to "/" and dies at allowedV1Path like any
	// unknown /v1 path. clientGate stays in front of both.
	mux.Handle("POST /v1/chat/completions", openaiWireHandler(core))
	mux.Handle("/", core)
	return clientGate(clientSecret, mux), core
}

// imageClassDoor names, after a comma, the door an image class is served
// through, for the chat door's refusal of one; an edition with an image door
// sets it, and "" leaves the refusal at the class's modality.
var imageClassDoor string

// shutdown is the signal's whole shutdown: an edition entry's sequence when
// it has one (its drain included), else the reports' one bounded post and
// the listener closed at once.
func shutdown(srv *http.Server) {
	for _, h := range entryHooks {
		if h.shutdown != nil {
			h.shutdown(srv)
			return
		}
	}
	log.Printf("shutdown: signal received (no second entry: no drain)")
	reportsFlushAll()
	if srv != nil {
		_ = srv.Close()
	}
	log.Printf("shutdown: done")
}

// ---- client gate (#35) ----
//
// When the listener is network-reachable, every request must prove it came
// from a client: Authorization: Bearer <SWITCH_CLIENT_SECRET> (Claude Code puts
// it there via ANTHROPIC_AUTH_TOKEN). DEFAULT-DENY: the only open path is
// GET /_switch/healthz (container/uptime probes). /v1/models is gated (the
// client's discovery fetch carries the token) and so is /_switch/status (it
// exposes account emails and session ids). The gate is also where the inbound
// Authorization header DIES: everything downstream either injects its own auth
// (account bearer / provider key) or must send none — before this gate existed,
// an unmatched path would have relayed the client's header to api.anthropic.com
// verbatim.

// clientGate wraps next with the client-secret check; secret "" = no gate (the
// loopback deployment, and every pre-#35 test).
//
// It is also the base-URL entry's tenant stamp (MEMBERS-PLAN 1b): every
// request on :8787 is the lan tenant's, and this is the one place that says
// so — withTenant here, read by tenantFrom below, which maps nothing else to
// the household. With no lan tenant in the index the stamp is nil and the
// routed paths answer the typed 503. The stamp precedes the gate so the
// ungated deployment gets it too.
func clientGate(secret string, next http.Handler) http.Handler {
	stamped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(withTenant(r.Context(), lanTenant())))
	})
	if secret == "" {
		return stamped
	}
	want := sha256.Sum256([]byte(secret))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_switch/healthz" {
			stamped.ServeHTTP(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		got := sha256.Sum256([]byte(strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))))
		if subtle.ConstantTimeCompare(want[:], got[:]) != 1 {
			log.Printf("%-4s %-28s -> 401  client gate: bad or missing secret (presented fp=%s)",
				r.Method, r.URL.Path, fp(auth))
			writeClientAuthFailed(w)
			return
		}
		// Validated. Strip it: the secret must never ride an upstream request, and
		// no ambient client credential may survive past this point either.
		r.Header.Del("Authorization")
		r.Header.Del("X-Api-Key")
		stamped.ServeHTTP(w, r)
	})
}

// writeClientAuthFailed answers a request that failed the client gate with an
// Anthropic-shaped authentication_error so Claude Code parses and surfaces it.
func writeClientAuthFailed(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type": "error",
		"error": map[string]any{
			"type": "authentication_error",
			"message": product + ": missing or wrong client secret — " +
				"export ANTHROPIC_AUTH_TOKEN to this proxy's client secret",
		},
	})
}

// writeModelUnroutable answers a request for a QUARANTINED id (#36): the id is
// recognized — a removed class, or a hosted provider's deleted direct id — but
// deliberately not routable. Typed 503 instead of the silent alternative: leaking the id
// onto the Anthropic account path, where api.anthropic.com 404s it.
// writeModelUnroutable answers a typed 503. retrySecs > 0 means the condition
// is transient and the caller should try again — it sets Retry-After, which is
// the only part of this an automated client can act on without reading prose.
// A permanent refusal deliberately sends NO Retry-After: a client that retries
// an unknown model id just burns the same error on a timer.
func writeModelUnroutable(w http.ResponseWriter, tn *tenant, model, reason string, retrySecs int) {
	w.Header().Set("Content-Type", "application/json")
	if retrySecs > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retrySecs))
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type": "error",
		"error": map[string]any{
			"type": "api_error",
			"message": product + ": model " + model + " is not routable — " + reason +
				". " + checkHint(tn, "Check"),
		},
	})
}

// statusHint renders the /_switch/status URL for error-message hints from the
// client-facing base URL (where the client can actually reach us — on a remote
// deployment "localhost" would point at the wrong machine). It is the
// household's address: only the lan tenant is answered with it. Anyone else —
// a member through the public door, or a request no entry stamped — gets ""
// and is pointed at apiary doctor instead, never at an address that is the
// household's and that a member cannot reach.
func statusHint(tn *tenant) string {
	if tn == nil || !tn.lan {
		return ""
	}
	base := clientBaseURL
	if base == "" {
		base = "http://localhost:8787"
	}
	return base + "/_switch/status"
}

// checkHint is a refusal's closing advice: for the household, lead + ": curl"
// + the status URL; for anyone else, apiary doctor, the member client's end to
// end check.
func checkHint(tn *tenant, lead string) string {
	if u := statusHint(tn); u != "" {
		return lead + ": curl " + u
	}
	return "Check: apiary doctor"
}

// writeRouteFailed answers a FAILED account-lane route: HTTP 503 with an Anthropic-shaped
// error envelope (so Claude Code parses and surfaces it) instead of silently forwarding
// the client's ambient login onto whatever account it happens to be logged into.
func writeRouteFailed(w http.ResponseWriter, tn *tenant, eff string) {
	msg := product + ": no account has a usable token"
	if eff != "" {
		msg += " (last tried " + eff + ")"
	}
	msg += " — refusing to forward your ambient login. " + checkHint(tn, "Check the brain")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":  "error",
		"error": map[string]any{"type": "api_error", "message": msg},
	})
}

// allowedV1Path lists the ONLY /v1/* paths that may enter the routing chain.
// The chain's final rung is laneForModel + decide() — a minted subscription
// bearer riding to api.anthropic.com — so an unknown /v1/* path must be denied
// BEFORE extractModel, not forwarded by default (Phase 0 of the OpenAI-wire
// plan, spikes/openai-inbound-wire/PLAN.md). GET /v1/models has its own mux
// route and never reaches requestHandler (the Go 1.22 mux answers other
// methods on that path with 405 itself). Live traffic uses exactly these two
// paths.
func allowedV1Path(p string) bool {
	return p == "/v1/messages" || p == "/v1/messages/count_tokens"
}

// denyEarly answers a request that never reaches the routing chain (unknown
// /v1/* path, oversized body) with an Anthropic-shaped error envelope, keeping
// the same status-counter and log-line bookkeeping as routed requests.
func denyEarly(rec *recorder, r *http.Request, start time.Time, inFP, tag string, status int, errType, msg string) {
	rec.Header().Set("Content-Type", "application/json")
	rec.WriteHeader(status)
	_ = json.NewEncoder(rec).Encode(map[string]any{
		"type":  "error",
		"error": map[string]any{"type": errType, "message": msg},
	})
	if d := decisionFrom(r.Context()); d != nil {
		d.finish(r.Context(), status)
		tag += " d=" + d.record.DecisionID
	}
	st.record(r.Method, r.URL.Path, rec.status, inFP, "")
	log.Printf("%-4s %-28s -> %d  bearer=%s%s  %s",
		r.Method, r.URL.Path, rec.status, inFP, tag, time.Since(start).Round(time.Millisecond))
}

func requestHandler(proxy http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		// Request-path metrics (reqmetrics.go). Registered first and retired by
		// the FIRST defer, so it runs LAST: a handler blocked anywhere after
		// this line — the final log line included — stays in flight.
		fbw := &firstByteWriter{ResponseWriter: w}
		metricLane := "other" // set by the routing branch that claims the request
		metricTenant := ""    // the request's tenant, once known
		rec := &recorder{ResponseWriter: fbw, status: http.StatusOK}
		reqID := reqMetrics.begin(start)
		defer func() {
			reqMetrics.end(reqID, metricLane, metricTenant, rec.status, start, fbw.first.Load(), time.Now())
		}()
		inFP := fp(r.Header.Get("Authorization"))
		// The request's tenant, stamped by its entry (MEMBERS-PLAN 1b). Every
		// per-tenant read below goes through tn; a request without one is
		// refused here, before any state is read. (Pass-through paths never
		// reach this handler: the door serves them on its own.)
		tn := tenantFrom(r.Context())
		if tn == nil {
			writeNoTenant(w, entryFrom(r.Context()).tenant)
			st.record(r.Method, r.URL.Path, http.StatusServiceUnavailable, inFP, "")
			log.Printf("%-4s %-28s -> 503  bearer=%s NO-TENANT[%s]", r.Method, r.URL.Path, inFP, entryFrom(r.Context()).tenant)
			return
		}
		metricTenant = tn.id
		ctx := context.WithValue(r.Context(), reqStartKey, start)
		ctx, rec.mark = withLedgerMark(ctx)
		ctx, served := withServedCap(ctx)
		// A broken stream ends in ReverseProxy's ErrAbortHandler panic, past
		// the log line below: the served event still goes, with its own line
		// naming the account and the seq. On every other path finish already
		// ran and this is a no-op.
		defer func() {
			if seq, first := served.finish(r.Context().Err() != nil); first && seq > 0 {
				log.Printf("%-4s %-28s -> %d  bearer=%s ABORTED%s  %s",
					r.Method, r.URL.Path, rec.status, inFP, served.logTag(), time.Since(start).Round(time.Millisecond))
			}
		}()
		decision := newDecision(r)
		rec.decision = decision
		if decision != nil {
			ctx = context.WithValue(ctx, decisionContextKey{}, decision)
			decision.headers(rec.Header())
			// ReverseProxy may panic with ErrAbortHandler after a broken stream.
			// Publish the terminal decision on that path too; finish is idempotent.
			defer func() {
				if tag := decision.finish(ctx, rec.status); tag != "" {
					log.Printf("request ended%s", tag)
				}
			}()
		}
		// Unconditional: reqStartKey has to reach the ErrorHandler whether or not
		// a decision is being traced — it is the only thing that can say how long
		// we waited before the upstream gave up on us.
		r = r.WithContext(ctx)
		wireOpenAI := wireFromContext(ctx) == "openai"
		// #126: which front door served this. "" for base-URL, so every log line
		// below is byte-identical to what :8787 printed before the second entry
		// existed; " via=hp:<machine>" when the HTTPS-proxy entry dispatched it.
		// Kept OUT of `tag`: every routing branch below assigns tag wholesale,
		// so an entry stored there would be overwritten by the first route
		// that claimed the request. It goes straight onto the log line.
		entryTag := entryFrom(r.Context()).logTag()
		tag, eff := "", ""
		failed := false               // account-lane route with no usable token -> 503, never ride ambient
		failedKind := ""              // with failed: the account kind this build has no actuator for
		failedNotTenant := ""         // with failed: the account the route named that is not the tenant's
		unroutable, unModel := "", "" // quarantined registry id -> typed 503, never the Anthropic path
		unRetry := 0                  // >0 => the refusal is transient; tell the client how long to wait
		var laneRefusal *laneDecision // bare lane whose every account's check is `no` -> 429, dial nobody
		var injection *injectReservation
		var ident tapRespIdent
		var requestBody []byte
		var facts reqFacts
		if strings.HasPrefix(r.URL.Path, "/v1/") {
			if !allowedV1Path(r.URL.Path) {
				denyEarly(rec, r, start, inFP, entryTag+" DENIED (unknown /v1 path)",
					http.StatusNotFound, "not_found_error", product+": no such endpoint: "+r.URL.Path)
				return
			}
			// Buffer the body ONCE (also replayed by the 429 failover via reqInfo),
			// classify the request into a lane by its top-level "model", and route
			// on that lane's account.
			var body []byte
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				var tooLarge bool
				body, tooLarge = bufferBody(r)
				if tooLarge {
					denyEarly(rec, r, start, inFP, entryTag+" DENIED (body too large)",
						http.StatusRequestEntityTooLarge, "request_too_large",
						fmt.Sprintf(product+": request body exceeds %d bytes (SWITCH_MAX_BODY_BYTES)", maxBodyBytes))
					return
				}
			}
			// D9: wire traffic never carries injected turns — the queue speaks
			// Claude Code's transcript dialect, and wire sessions are never
			// observed by the tap anyway.
			if r.Method == http.MethodPost && r.URL.Path == "/v1/messages" && !wireOpenAI {
				if injected, reservation := tn.injectRequest(body, r.Header.Get("x-claude-code-session-id")); reservation != nil {
					body, injection = injected, reservation
					r.Body = io.NopCloser(bytes.NewReader(body))
					r.ContentLength = int64(len(body))
				}
			}
			model := extractModel(body)
			if decision != nil {
				decision.record.RequestedModel = model
				decision.record.EstInput = estInputTokens(body)
			}
			// The transcript tap (#60) reads the body the CLIENT sent: the provider
			// and class branches below repoint `body` at a model-rewritten copy.
			requestBody = body
			// Client-side facts the upstream never echoes (1M window claim, effort);
			// read from the CLIENT's bytes before any lane/class rewrite touches them.
			facts = parseReqFacts(r.Header, body)
			// Which harness sent it, worked out once and carried on ctx so the
			// turn event and the route_decision say the same thing; and the
			// session's folder, kept on the request path so it never depends
			// on the tap being armed.
			harness := recognizeHarness(r.Header, facts.FirstSystem)
			ctx = withHarness(ctx, harness)
			if decision != nil {
				decision.record.Harness = harness
			}
			// Not on the OpenAI wire: like the tap and the inject queue, the
			// labels read Claude Code's dialect only.
			if r.Method == http.MethodPost && r.URL.Path == "/v1/messages" && !wireOpenAI {
				tn.sessionLabelsObserve(r.Header.Get("x-claude-code-session-id"), facts, time.Now())
			}
			served.request(model, facts, r.Method == http.MethodPost && r.URL.Path == "/v1/messages")
			ceClass, isClass := matchRegistryClass(model)
			if wireOpenAI && !isClass {
				// D8: the OpenAI wire is default-deny — workload classes ONLY.
				// A lane id or garbage must never reach laneForModel from
				// this wire (subscription
				// bearers stay structurally out of reach). Normalization
				// already 404s non-classes; this arm is belt-and-suspenders.
				unroutable, unModel = "the openai wire routes workload classes only", model
				eff = model
				tag = " UNROUTABLE[" + model + "] (openai wire: not a class)"
			} else if ce, ok := ceClass, isClass; ok && ce.Modality != "" {
				// An image class (§2.9) is the image door's: its members make
				// images, not messages.
				unroutable, unModel = "class "+ce.Class+" serves "+ce.Modality+" requests"+imageClassDoor, model
				eff = ce.renderedID()
				tag = " CLASS[" + ce.Class + "] UNROUTABLE (" + ce.Modality + " class)"
			} else if ok {
				// A class row (#27): the brain binds this SESSION to a member
				// (#26, sticky); the proxy actuates whatever kind came back.
				// Refusals and dead-ends answer a typed 503 — never the lane path.
				metricLane = "class:" + ce.Class
				sess := r.Header.Get("x-claude-code-session-id")
				if sess == "" {
					sess = "no-session" // curl etc. — one shared sticky binding
				}
				// §5.5(5): take back our own trailing note before the history goes
				// upstream. Gated on one bytes.Contains, so a body without the
				// sentinel costs a linear scan and nothing else. Runs in EVERY mode,
				// off included — a note written under enforce must still come off
				// after a rollback, or it rides into the prompt-cache prefix. This
				// replaces `body`, so it is what routeClass fingerprints, forwards and
				// replays; requestBody above deliberately keeps the client's own bytes,
				// sentinel included, because that is what the transcript tap should show.
				if nb := stripSwitchNote(ce.Class, body); nb != nil {
					body = nb
					r.Body = io.NopCloser(bytes.NewReader(body))
					r.ContentLength = int64(len(body))
				}
				var requestID []string
				if decision != nil {
					decision.record.Class = nullable(ce.Class)
					decision.record.HopKind = "bound"
					decision.record.Reason = "bind:pointer"
					requestID = []string{decision.record.DecisionID}
				}
				out := tn.routeClass(ce, sess, classReq{origBody: body, inPath: r.URL.Path, wireOpenAI: wireOpenAI, ctx: r.Context()}, r.Header, requestID...)
				if out.info.classCtx != nil {
					out.info.classCtx.trace = decision
				}
				eff, tag = out.eff, out.tag
				if out.refuse != "" {
					unroutable, unModel = out.refuse, model
				} else {
					if out.info.body != nil {
						r.Body = io.NopCloser(bytes.NewReader(out.info.body))
						r.ContentLength = int64(len(out.info.body))
					}
					ctx = context.WithValue(ctx, infoKey, out.info)
				}
			} else if re, ok := matchClassTombstone(model); ok {
				// A removed class (#36, #93 rule 2): recognized, deliberately not
				// routable — answered below with a typed 503, never the lane path.
				unroutable, unModel = re.disabledReason, model
				eff = re.renderedID()
				tag = " UNROUTABLE[" + model + "] (" + re.disabledReason + ")"
			} else if reason, retrySecs, ok := classShapedRefusalReason(tn, model); ok {
				// The workload-class namespace is fail-closed even when a row is
				// unknown (typo), removed while proxy state was lost, malformed,
				// or carrying the wrong [1m] suffix. Generalizes the old
				// claude-delegate-* reserved prefix (#93 rule 3): a class-shaped
				// id must never reach laneForModel and ride to api.anthropic.com.
				unroutable, unModel, unRetry = reason, model, retrySecs
				eff = model
				tag = " UNROUTABLE[" + model + "] (" + reason + ")"
			} else if prov, ok := hostedProviderNamespace(model); ok {
				// A hosted provider's deleted direct id (quota-model C2, §2.2 row
				// 10), from a picker cache that has no TTL. Permanent, so no
				// Retry-After; the message names the edit.
				unroutable, unModel = "direct "+prov+" model ids were removed: "+prov+" is reached only as a "+
					"member of a workload class, where its route and pay list apply. Retrying will not help — "+
					"pick a class ("+classesHint(tn)+")", model
				eff = model
				tag = " UNROUTABLE[" + model + "] (deleted " + prov + " direct id)"
			} else {
				// #96: "[1m]" is RENDERER-OWNED picker metadata — registry.go appends
				// it iff context_window >= 1M, purely so the client's bare regex reads
				// a 1M window (spikes/model-registry/FINDINGS.md:152-155). Anthropic
				// has never seen the suffix and never will. Class rows shed it via
				// routeClass; THIS passthrough arm is the
				// only one that would hand a picker id to an account lane verbatim,
				// where "claude-fable-5[1m]" 404s. Strip it here, before lane inference
				// and before the body leaves — `body` feeds both the first send and the
				// 429 replay. The digit guard keeps the class-shaped (digit-free)
				// namespace out: those ids are refused above and must never be quietly
				// repaired into a lane route.
				routed := model
				if base, cut := strings.CutSuffix(model, "[1m]"); cut && strings.ContainsAny(base, "0123456789") {
					routed = base
					if nb := rewriteModel(body, routed); nb != nil {
						body = nb
						r.Body = io.NopCloser(bytes.NewReader(body))
						r.ContentLength = int64(len(body))
					}
				}
				tn.pref.mu.RLock()
				rules := tn.pref.laneRules
				tn.pref.mu.RUnlock()
				lane := laneForModel(routed, rules)
				metricLane = lane // brain lane names (opus/sonnet/haiku/fable) or "default"
				if r.Method == http.MethodPost && len(body) > 0 && model == "" {
					atomic.AddInt64(&laneParseFailures, 1)
				}
				d := tn.decideRoute(laneRouteID(lane), lane, routed, strings.HasSuffix(r.URL.Path, "/count_tokens"))
				tok, effective, reason, state := d.tok, d.acct, d.reason, d.state
				eff = effective
				tn.noteState(lane, state)
				if d.none {
					// §2.2 row 11: the lane's route admits no account. Refused
					// here, in the client's usage-limit terms, before any dial.
					laneRefusal = &d
					tag = " REFUSED [" + effective + "] lane=" + lane + " (" + reason + ")"
				} else if state == stateFailed {
					// No account has a usable token. Fail LOUD — never forward the
					// client's ambient login (that silent misroute is the whole bug we
					// deleted). The request is answered 503 below, before any upstream call.
					failed, failedKind, failedNotTenant = true, d.unactuated, d.notTenant
					tag = " FAILED [" + effective + "] lane=" + lane + " (" + reason + ")"
				} else {
					tn.noteEffective(lane, effective, reason)
					// Anthropic lane (wire==nil): optionally force the 1h prompt-cache TTL.
					// Rewrite `body` so BOTH the first send (r.Body) and the 429 replay
					// (reqInfo.body) carry it; the matching beta goes on in Rewrite.
					if forceCache1h {
						if nb := force1hCacheControl(body); len(nb) != len(body) {
							body = nb
							r.Body = io.NopCloser(bytes.NewReader(body))
							r.ContentLength = int64(len(body))
						}
					}
					ctx = context.WithValue(ctx, infoKey, reqInfo{
						tenant: tn,
						token:  tok,
						eff:    effective,
						state:  state,
						lane:   lane,
						body:   body,
						check:  d.check,
						asked:  model,
					})
					tag = " " + state.String() + "->" + fp("Bearer "+tok) + " [" + effective + "] lane=" + lane + " check=" + d.check.String()
				}
			}
			// #60 transcript tap. Placed AFTER the routing chain and BEFORE the
			// upstream call: it reads the pre-rewrite body (requestBody) yet carries the
			// decision that served it, so a turn joins to its route. Emitting before
			// ServeHTTP is the point — a streamed answer can take a minute and the
			// transcript is meant to be live. No lock is held here: tn.pref.mu was
			// released inside the branch above and st.mu is not taken until st.record
			// below, which is what keeps hub.mu a leaf.
			if wireOpenAI {
				// The transcript tap and inject queue speak Claude Code's
				// dialect; wire turns are invisible to both (D9). A zero ident
				// also keeps the response tap uninstalled below.
				tag += " WIRE[openai]"
			} else {
				ident = tn.tapTurn(ctx, r, requestBody, model, tag, eff, inFP)
			}
		}
		decision.correlate(ident)
		decision.headers(rec.Header())
		if failed && failedNotTenant != "" {
			writeNotTenantAccount(rec, tn, failedNotTenant)
		} else if failed && failedKind != "" {
			writeNoActuator(rec, tn, eff, failedKind)
		} else if failed {
			writeRouteFailed(rec, tn, eff)
		} else if laneRefusal != nil {
			tn.writeLaneRefused(rec, laneRefusal, r.Header, nowFn())
		} else if unroutable != "" {
			writeModelUnroutable(rec, tn, unModel, unroutable, unRetry)
		} else {
			// #61 response tap: installed only when the request tap produced an
			// identity (armed hub, parseable /v1/messages body) AND the operator
			// hasn't killed it. finish() is the ONLY closer of the tap's channel,
			// and it runs on this goroutine after every Write has returned.
			var sink http.ResponseWriter = rec
			if respTapEnabled && ident.ok {
				rt := newRespTap(ident)
				defer rt.finish()
				sink = &tapRecorder{recorder: rec, tap: rt}
			}
			proxy.ServeHTTP(sink, r.WithContext(ctx))
		}
		// The response has ended: the served event, if an account answered.
		served.finish(r.Context().Err() != nil)
		tag += decision.finish(ctx, rec.status)
		tn.finishInject(injection, rec.status >= 200 && rec.status < 300)
		st.record(r.Method, r.URL.Path, rec.status, inFP, eff)
		if requestBody != nil {
			tag += facts.logTag()
		}
		tag += served.logTag()
		log.Printf("%-4s %-28s -> %d  bearer=%s%s%s  %s",
			r.Method, r.URL.Path, rec.status, inFP, entryTag, tag, time.Since(start).Round(time.Millisecond))
	})
}

// statusExtras add an edition's keys to /_switch/status. Each runs before
// st.mu is taken and may take only its own leaf mutex.
var statusExtras []func(out map[string]any)

func switchHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/_switch/healthz":
		_, _ = w.Write([]byte(`{"ok":true,"service":"claude-switch-proxy"}`))
	case "/_switch/classes":
		classesHandler(w, r)
	case "/_switch/metrics":
		// §6.2(1). Behind the client gate like everything but healthz; the
		// scraper passes the secret as a Bearer.
		// Request-path series first and in every mode: they take only
		// reqMetrics.mu, which no request holds across I/O or logging.
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = io.WriteString(w, reqMetrics.render(time.Now()))
		// A second entry's own families, deliberately NOT folded into
		// reqMetrics: a 14-minute pass-through event stream counted there would
		// pin switch_proxy_oldest_in_flight_seconds. Each takes only its
		// entry's own mutex, a leaf.
		for _, h := range entryHooks {
			if h.metrics != nil {
				_, _ = io.WriteString(w, h.metrics())
			}
		}
		// Mode off: no stores, so no switch_health_* series. Every tenant's
		// store renders into one buffer, each under its tenant label.
		m := newMetricsBuf()
		for _, tn := range allTenants() {
			if tn.health != nil {
				tn.health.renderMetrics(m, tn)
			}
		}
		_, _ = io.WriteString(w, m.String())
	case "/_switch/quota-drill":
		// Read-only rendering of the refusal a class would be served right now
		// (see quotaDrillHandler). Behind the client gate like everything but
		// healthz; it holds no locks the serving path needs and mutates nothing.
		quotaDrillHandler(w, r)
	case "/_switch/session":
		// What a status line draws for one session (docs/session-facts.md).
		sessionHandler(w, r)
	case "/_switch/status":
		// The per-tenant keys nest under members.<tenant> (MEMBERS-PLAN 1b),
		// the household's under members[lan_tenant]; the proxy-wide keys stay
		// at the top. Each tenant's snapshot takes only that tenant's leaf
		// mutexes, before st.mu.
		members := map[string]any{}
		for _, tn := range allTenants() {
			members[tn.id] = tn.statusSnapshot()
		}
		lan := ""
		if tn := lanTenant(); tn != nil {
			lan = tn.id
		}
		// An edition's keys (statusExtras), each taking only its own leaf
		// mutex, before st.mu.
		extras := map[string]any{}
		for _, f := range statusExtras {
			f(extras)
		}
		st.mu.Lock()
		snap := map[string]any{
			"service":             "claude-switch-proxy",
			"mode":                map[bool]string{true: "brain", false: "direct"}[brainBase != ""],
			"lan_tenant":          lan,
			"tenants":             tenantsStatus(),
			"members":             members,
			"lane_parse_failures": atomic.LoadInt64(&laneParseFailures),
			"direct_providers":    directProvidersStatus(),
			"registry":            registryStatus(),
			// eventsStatus takes hub.mu, which is a leaf — it must never reach for
			// st.mu, which is held across this whole literal.
			"events":       eventsStatus(),
			"response_tap": respTapStatus(),
			// usageScalingStatus takes scaleStatsMu — a leaf, same rule as
			// eventsStatus above. #77: display only, no alerting hooks.
			"usage_scaling":       usageScalingStatus(),
			"client_models_cache": clientCachePath,
			"bridge_configured":   bridgeTarget() != "",
			"requests":            st.requests,
			"switches":            st.switches,
			"uptime_seconds":      int(time.Since(st.started).Seconds()),
		}
		st.mu.Unlock()
		for k, v := range extras {
			snap[k] = v
		}
		_ = json.NewEncoder(w).Encode(snap)
	default:
		http.NotFound(w, r)
	}
}

// classTransport is the routing chain's failover transport — the one the
// core ReverseProxy dials through, set by buildHandlers. An image dial goes
// through it, so it gets what every other route gets: the class walk, the
// one 401 retry on a fresh bearer, the refusal records and the served hop's
// readings (an image's x-codex-* headers are the account's, chat's too).
var classTransport *failoverTransport

// hpUnwrapURLError reduces a *url.Error to its cause. url.Error's Error()
// prints the full URL, query included, and a pass-through query is the client's
// business, not ours to log.
func hpUnwrapURLError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) && ue.Err != nil {
		return ue.Err
	}
	return err
}

// statusSnapshot is one tenant's /_switch/status keys: what the top level
// carried before the proxy was keyed per tenant, under members.<tenant> now.
// Takes only the tenant's own leaf mutexes, one at a time.
func (tn *tenant) statusSnapshot() map[string]any {
	pref := tn.pref
	pref.mu.RLock()
	prefAcct, prefReason, prefOK := pref.account, pref.reason, pref.ok
	laneMode := "single-fallback"
	if len(pref.laneRules) > 0 {
		laneMode = "lanes"
	}
	rules := make([]map[string]any, 0, len(pref.laneRules))
	for _, lr := range pref.laneRules {
		rules = append(rules, map[string]any{"lane": lr.Lane, "match": lr.Match, "scope": lr.Scope})
	}
	lanes := map[string]any{}
	for ln, lp := range pref.lanes {
		lanes[ln] = map[string]any{"preferred": lp.Account, "reason": lp.Reason, "effective": lp.Effective}
	}
	pref.mu.RUnlock()
	tn.effMu.Lock()
	effByLane := make(map[string]string, len(tn.lastEffective))
	for ln, a := range tn.lastEffective {
		effByLane[ln] = a
	}
	stateByLane := make(map[string]string, len(tn.lastState))
	for ln, s := range tn.lastState {
		stateByLane[ln] = s.String()
	}
	tn.effMu.Unlock()
	tn.tokMu.Lock()
	cached := make(map[string]string, len(tn.tokens))
	for a, e := range tn.tokens {
		cached[a] = time.Until(e.exp).Round(time.Minute).String()
	}
	anthTokens := tn.anthropicTokensStatus()
	tn.tokMu.Unlock()
	tn.bindMu.Lock()
	classBindings := map[string]map[string]any{}
	for k, cb := range tn.bindCache {
		cls, sess, ok := strings.Cut(k, "\x00")
		if !ok {
			continue
		}
		if classBindings[cls] == nil {
			classBindings[cls] = map[string]any{}
		}
		classBindings[cls][fpSess(sess)] = map[string]any{
			"member": cb.member.ID, "reason": cb.reason,
			// The bound member's r_p as the brain served it (1.0 on an
			// anthropic member): what this proxy scales that hop's usage by.
			"anthropic_token_ratio": cb.member.AnthropicTokenRatio,
			"served_member":         nullable(cb.servedMember), "served_reason": nullable(cb.servedReason),
			"age_seconds": int(time.Since(cb.fetchedAt).Seconds()),
		}
	}
	classFunding := map[string]map[string]any{}
	for k, f := range tn.sessionFundings {
		cls, sess, ok := strings.Cut(k, "\x00")
		if !ok {
			continue
		}
		if classFunding[cls] == nil {
			classFunding[cls] = map[string]any{}
		}
		classFunding[cls][fpSess(sess)] = f.json()
	}
	tn.bindMu.Unlock()
	tn.codexTokMu.Lock()
	codexBearers := map[string]string{}
	for acct, e := range tn.codexToks {
		if acct == "" {
			acct = "(incumbent)"
		}
		codexBearers[acct] = time.Until(e.exp).Round(time.Minute).String()
	}
	tn.codexTokMu.Unlock()
	return map[string]any{
		"tenant":            tn.id,
		"name":              tn.name,
		"lan":               tn.lan,
		"tap":               tn.tap,
		"preferred_account": prefAcct,
		"preferred_reason":  prefReason,
		"brain_reachable":   prefOK,
		// legacy mirror: the default lane's last effective account
		"effective_account": effByLane["default"],
		"effective_by_lane": effByLane,
		"state_by_lane":     stateByLane,
		"lane_mode":         laneMode,
		"lane_rules":        rules,
		"lanes":             lanes,
		"injection":         tn.injectStatus(),
		"tokens_cached_ttl": cached,
		// Per account: the cached copy (fp, fetched_at, expires_at), the
		// last refusal of an injected copy (last_refused: at, fp, msg) and
		// the reauths refusals caused. tokens_cached_ttl stays as it is:
		// Scry reads it as record<string,string>.
		"anthropic_tokens": anthTokens,
		"class_bindings":   classBindings,
		// C6 (§2.11's statusline): per class and session, the tier its last
		// request was served on, or the class unavailable until a time.
		"class_funding": classFunding,
		// The one record store (records.go): "<account>/<limit>" -> the
		// refusal and when it ends. recordsStatus takes recMu — a leaf.
		"refusal_records": tn.recordsStatus(nowFn()),
		// C6: the ledger entries of ours the brain has not counted yet,
		// and the spend of the ledger last pulled (ledger.go).
		"ledger": tn.ledgerStatus(),
		// 64 §5: the events waiting for the brain's POST /reports, and the
		// last post's failure (reports.go).
		"reports": tn.reportsStatus(),
		// #71: the per-account codex bearer cache ("" = the incumbent-follow slot).
		"codex_bearers_ttl": codexBearers,
		// C8: each api account's dispensed key, its state and, with none,
		// why (apikey.go; never the key). apiKeyStatus takes apiKeyMu — a leaf.
		"api_keys": tn.apiKeyStatus(),
		// healthStatus takes the store's own mutex — a leaf, same rule as
		// eventsStatus above. Minimal until T6 (#125).
		"member_health": tn.healthStatus(),
	}
}
