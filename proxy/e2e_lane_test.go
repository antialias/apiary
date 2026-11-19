package main

// End-to-end lane tests: the REAL handler stack (buildHandler -> ReverseProxy ->
// failoverTransport) against an httptest brain, upstream, and bridge. These prove
// the headline behavior: one proxy instance routing two model classes to two
// accounts in the same second, per-lane switch alerts, and correctly-scoped 429
// cooldowns.

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type e2eEnv struct {
	proxy *httptest.Server
	// core is the routing chain under the gate (#126): the handler the HTTPS-
	// proxy entry dispatches its two routed paths into.
	core     http.Handler
	upstream *httptest.Server
	bridge   chan []byte
	seen     chan e2eHit // one entry per upstream arrival
	brainMu  sync.Mutex
	lanes    map[string]any // mutable: tests flip lane accounts then re-fetchPreferred
	// fleet, when set, is served as /preferred's `fleet` (quota-model 67 §8),
	// the section session facts' room reads. Under brainMu.
	fleet map[string]any
	// upstream behavior: return 429 with this body while set (checked per request)
	deny429 atomic.Value // string: bearer (or api key) to 429; body429: the payload
	body429 atomic.Value // string
	// denyStatus, when set, is the status deny429 answers with instead of
	// 429: Anthropic's money refusals are a 400 and a 402 (A-D6).
	denyStatus atomic.Int64
	deny500    atomic.Value // string: bearer the fake Anthropic upstream 500s ("*" = all) — #123's head-failure knob
	// deny429Enc compresses the 429 body per the arriving Accept-Encoding, the
	// way live Anthropic encodes error bodies for a client that offers it (the
	// brotli lesson: those bodies came back brotli'd and the 429 classifier
	// was parsing the compressed bytes).
	deny429Enc atomic.Bool

	// #61 response-tap knobs: when anthSSE is a non-empty SSE script the fake
	// Anthropic upstream serves it as text/event-stream, one frame per Flush
	// (split on \n\n) — the suite's first incrementally-flushing upstream —
	// sleeping anthGap between frames. "" = the JSON default (existing tests).
	// anthGzip serves the script gzip-compressed with Content-Encoding: gzip,
	// the way live Anthropic answers a client that offers compression (which
	// every real CLI does).
	anthSSE  atomic.Value // string
	anthGap  atomic.Value // time.Duration
	anthGzip atomic.Bool
	// anthAbort cuts the connection once the anthSSE script is written, a
	// stream the upstream never finished.
	anthAbort atomic.Bool
	// anthJSON, when a non-empty string, is the JSON reply the fake Anthropic
	// upstream serves in place of its default.
	anthJSON atomic.Value
	// anthHeaders (#126 C3b): response headers the fake Anthropic upstream sets
	// on every reply, so a test can serve the anthropic-ratelimit-unified-*
	// set a pooled account's answer really carries. map[string]string.
	anthHeaders atomic.Value
	// anthHeadersFor: per-account response headers, keyed by the bearer the
	// fake Anthropic upstream sees ("tok-<account>"), applied after
	// anthHeaders — so each pooled account can carry its OWN rejected windows.
	// map[string]map[string]string.
	anthHeadersFor atomic.Value
	// denyModel: 429 (body429) any request whose model contains .model, with
	// .headers — the same bearer then serves every other model. Lets one
	// account refuse one lane and serve another.
	denyModel atomic.Value
	// views, when non-nil, is a test's shorthand for an account's standing:
	// its last good poll (good_ts) and its windows by name (percent,
	// resets_at, a scoped window's aliases). quotaBlocks serves each as the
	// account's accounts{} entry (viewStanding). Under brainMu.
	views map[string]any

	// The quota model's /preferred blocks (C2), under brainMu. standings
	// overrides an account's accounts{} entry (default: e2eStanding);
	// routePolicy merges into a route's routes{} entry (pay, stop_at); pots
	// and payOrder are served as given. Routes are derived: one per lane, one
	// per class member (fakeRoutes). ledger is served as given (C6); nil
	// serves an empty one read now (e2eLedger).
	standings   map[string]any
	routePolicy map[string]map[string]any
	pots        map[string]any
	payOrder    []string
	ledger      map[string]any
	// ledgerPosts is every `ledger` event POSTed to /reports, in order; the
	// fake answers seq = their count. reports is every other event, in order.
	// The fake applies every event. Under brainMu.
	ledgerPosts []map[string]any
	reports     []map[string]any

	// class knobs (#27): when classMembers is non-nil the fake brain serves a
	// "frontier" class on /preferred and a sticky per-session /class-bind.
	// classesGone (#93) makes /preferred serve `"classes": {}` — the key PRESENT
	// but empty, i.e. a brain positively stating the class was removed — which is
	// what tombstones derive from (an ABSENT key keeps the last-good rows).
	classesGone  atomic.Bool
	classMembers []map[string]any  // priority-ordered member objects (under brainMu)
	className    string            // empty means frontier (under brainMu)
	classDisplay string            // empty means "Frontier (auto)" (under brainMu)
	classWindow  int               // the class's context_window; 0 means 200000 (under brainMu)
	binds        map[string]string // session -> member id, sticky (under brainMu)
	bindTo       atomic.Value      // string: member id NEW binds land on ("" = first member)
	bind404      atomic.Bool       // force /class-bind 404 (unknown class)
	bind409      atomic.Value      // string: force /class-bind 409 class_unavailable with this body
	bind503      atomic.Value      // string: force /class-bind 503 (no member has an account) with this body
	bindCalls    atomic.Int64      // how many /class-bind requests arrived
	bindReason   atomic.Value      // string: the /class-bind `reason` ("" = "bind: pointer")
	// imageMembers (C4): when non-nil the fake brain also serves the image
	// door's class, "images" (request_kind image), beside the chat class:
	// on /preferred, with its routes, and on /class-bind, sticky per session
	// like the chat class, NEW binds landing on its first member. Under brainMu.
	imageMembers []map[string]any
	// imageClasses are more image classes beside "images", by name
	// (images-runway, images-chatgpt): served like it on /preferred, with
	// their routes, and on /class-bind. Under brainMu.
	imageClasses map[string][]map[string]any
	// brainHard > 0 makes /preferred carry `thresholds.hard` (the real brain
	// always does; the default fake omits it so the proxy's mirrored default is
	// what most tests exercise). Under brainMu.
	brainHard float64
	// apiKeys (C8) is what /api-key dispenses, account -> key; an account
	// absent is the brain's named 404 (no key placed). apiKeyCalls counts the
	// requests; apiKeyHang makes /api-key answer nothing until its caller
	// gives up, or, with apiKeyHangFor set, a 503 after that long (a brain
	// that answers no one in time). Under brainMu.
	apiKeys       map[string]string
	apiKeyCalls   int
	apiKeyHang    bool
	apiKeyHangFor time.Duration

	// codex knobs (#33): the fake brain dispenses "codex-tok-<n>" per
	// /openai-token call, so tests can assert WHICH dispense served a hop.
	codexDispenses atomic.Int64
	// #71 pool mode: comma-separated pool account names ("" = legacy single
	// chain). When set, /openai-token serves per-account bearers
	// ("codex-tok-<name>") with the response `name` key; no ?account= param
	// resolves to the FIRST name (the fake pool's incumbent); an unknown name
	// is a 404 — exactly the real brain's contract.
	codexPool atomic.Value

	// Sign-in rotation knobs (a reauth). tokGen is each
	// account's token generation, so /token dispenses e2eTokenGen(a, gen);
	// revoked is the bearers the fake Anthropic upstream answers with a 401
	// authentication_error (revokedHits counts those answers). rotate revokes
	// an account's current copy and moves the brain to the next generation —
	// unless tokStuck, when the brain keeps dispensing the revoked one (a
	// dead grant). tokenCalls counts /token requests, refusedCalls those
	// carrying refused_fp, lastRefusedFP the last such value; tokGate, when
	// set, holds every refused_fp /token open until it is closed; tokFail
	// answers every refused_fp /token with a 502 (a brain mid-restart). tokGen,
	// revoked and tokGate are under brainMu.
	tokGen        map[string]int
	revoked       map[string]bool
	revokedHits   atomic.Int64
	tokStuck      atomic.Bool
	tokenCalls    atomic.Int64
	refusedCalls  atomic.Int64
	lastRefusedFP atomic.Value // string
	tokGate       chan struct{}
	tokFail       atomic.Bool
}

// e2eRevoked401 is Anthropic's answer to a revoked OAuth access token.
const e2eRevoked401 = `{"type":"error","error":{"type":"authentication_error","message":"OAuth access token has been revoked."}}`

// e2eTokenGen is the fake brain's copy of account's token at generation gen
// (0 is e2eToken, which every other test expects).
func e2eTokenGen(account string, gen int) string {
	if gen == 0 {
		return e2eToken(account)
	}
	return fmt.Sprintf("%s-g%d", e2eToken(account), gen)
}

// revokeCopy revokes the brain's current copy of account at the upstream.
func (env *e2eEnv) revokeCopy(account string) {
	env.brainMu.Lock()
	defer env.brainMu.Unlock()
	env.revoked[e2eTokenGen(account, env.tokGen[account])] = true
}

// rotate is a sign-in elsewhere refreshing account early: the copy the brain
// handed out is revoked and the brain holds the next one (or, tokStuck, not).
func (env *e2eEnv) rotate(account string) {
	env.revokeCopy(account)
	if !env.tokStuck.Load() {
		env.brainMu.Lock()
		env.tokGen[account]++
		env.brainMu.Unlock()
	}
}

// brainCopy is the token the fake brain dispenses for account now.
func (env *e2eEnv) brainCopy(account string) string {
	env.brainMu.Lock()
	defer env.brainMu.Unlock()
	return e2eTokenGen(account, env.tokGen[account])
}

// acct/ua capture codex-header hygiene: an Anthropic-bound hop must never
// carry ChatGPT-Account-Id or the codex UA (cross-member rebuilds rebase
// headers on the inbound snapshot).
type e2eHit struct {
	auth, model, beta, acct, ua string
	apiKey                      string // X-Api-Key as the upstream saw it
	ae                          string // Accept-Encoding as the upstream saw it
	body                        []byte
}

func e2eToken(account string) string { return "tok-" + account }

// e2eServeSSE writes an SSE script frame-by-frame — split on \n\n, Flush after each,
// optional gap between frames — so tests can prove incremental client-side arrival.
// Rejoining with \n\n keeps the client's bytes identical to the script.
func e2eServeSSE(w http.ResponseWriter, script string, gap time.Duration, gzipped bool) {
	w.Header().Set("Content-Type", "text/event-stream")
	var (
		out io.Writer = w
		zw  *gzip.Writer
	)
	if gzipped {
		w.Header().Set("Content-Encoding", "gzip")
		zw = gzip.NewWriter(w)
		out = zw
	}
	w.WriteHeader(200)
	fl, _ := w.(http.Flusher)
	first := true
	for _, f := range strings.Split(script, "\n\n") {
		if f == "" {
			continue
		}
		if !first && gap > 0 {
			time.Sleep(gap)
		}
		first = false
		_, _ = io.WriteString(out, f+"\n\n")
		if zw != nil {
			_ = zw.Flush() // per-frame, like a live edge that compresses SSE without buffering it
		}
		if fl != nil {
			fl.Flush()
		}
	}
	if zw != nil {
		_ = zw.Close()
	}
}

// fetchPreferredSync is fetchPreferred for tests: it also waits for the token
// prewarm to drain, so no goroutine outlives the call.
//
// Every test call site uses this, not the bare fetchPreferred. The prewarm is
// deliberately fire-and-forget in production, which means a test that returns
// from setup with warming goroutines still running has handed itself a package
// full of globals that are unsafe to write — brainURL today, whichever one the
// next test reaches for tomorrow. Three tests already swap brainURL to a dead
// server to assert a refusal, and CI caught the race under -race
// on a loaded runner. Waiting here fixes the class of bug rather than the three
// instances of it.
func fetchPreferredSync() {
	fetchPreferred()
	prewarmWG.Wait()
}

func newE2E(t *testing.T) (*e2eEnv, func()) {
	t.Helper()
	env := &e2eEnv{
		bridge: make(chan []byte, 8),
		seen:   make(chan e2eHit, 32),
		lanes: map[string]any{
			"fable":   map[string]any{"account": other, "reason": "fable cap hot", "effective": map[string]float64{native: 85, other: 70}},
			"haiku":   map[string]any{"account": native, "reason": "incumbent", "effective": map[string]float64{native: 41, other: 30}},
			"default": map[string]any{"account": native, "reason": "incumbent", "effective": map[string]float64{native: 41, other: 30}},
		},
	}
	env.deny429.Store("")
	env.body429.Store("")
	env.deny500.Store("")
	env.deny429Enc.Store(false)
	env.bindTo.Store("")
	env.codexPool.Store("")
	env.anthSSE.Store("")
	env.anthGap.Store(time.Duration(0))
	env.binds = map[string]string{}
	env.tokGen, env.revoked = map[string]int{}, map[string]bool{}
	env.lastRefusedFP.Store("")

	env.upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		auth := r.Header.Get("Authorization")
		ae := r.Header.Get("Accept-Encoding")
		env.seen <- e2eHit{auth: auth, model: extractModel(body), beta: r.Header.Get("anthropic-beta"),
			acct: r.Header.Get("ChatGPT-Account-Id"), apiKey: r.Header.Get("X-Api-Key"), ua: r.Header.Get("User-Agent"), ae: ae, body: body}
		env.brainMu.Lock()
		dead := env.revoked[strings.TrimPrefix(auth, "Bearer ")]
		env.brainMu.Unlock()
		if dead {
			env.revokedHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(401)
			_, _ = w.Write([]byte(e2eRevoked401))
			return
		}
		if hs, _ := env.anthHeaders.Load().(map[string]string); len(hs) > 0 {
			for k, v := range hs {
				w.Header().Set(k, v)
			}
		}
		if by, _ := env.anthHeadersFor.Load().(map[string]map[string]string); len(by) > 0 {
			for k, v := range by[strings.TrimPrefix(auth, "Bearer ")] {
				w.Header().Set(k, v)
			}
		}
		if dm, _ := env.denyModel.Load().(e2eDenyModel); dm.model != "" && strings.Contains(extractModel(body), dm.model) {
			for k, v := range dm.headers {
				w.Header().Set(k, v)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(429)
			_, _ = w.Write([]byte(env.body429.Load().(string)))
			return
		}
		if deny := env.deny500.Load().(string); deny != "" && (deny == "*" || auth == "Bearer "+deny) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"api_error","message":"internal server error"}}`))
			return
		}
		if deny := env.deny429.Load().(string); deny != "" && (deny == "*" || auth == "Bearer "+deny || r.Header.Get("X-Api-Key") == deny) {
			w.Header().Set("Content-Type", "application/json")
			payload := []byte(env.body429.Load().(string))
			if env.deny429Enc.Load() && strings.Contains(ae, "gzip") {
				var buf bytes.Buffer
				zw := gzip.NewWriter(&buf)
				_, _ = zw.Write(payload)
				_ = zw.Close()
				w.Header().Set("Content-Encoding", "gzip")
				payload = buf.Bytes()
			}
			status := 429
			if s := env.denyStatus.Load(); s != 0 {
				status = int(s)
			}
			w.WriteHeader(status)
			_, _ = w.Write(payload)
			return
		}
		if sse := env.anthSSE.Load().(string); sse != "" {
			e2eServeSSE(w, sse, env.anthGap.Load().(time.Duration), env.anthGzip.Load())
			if env.anthAbort.Load() {
				panic(http.ErrAbortHandler)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if j, _ := env.anthJSON.Load().(string); j != "" {
			_, _ = w.Write([]byte(j))
			return
		}
		_, _ = w.Write([]byte(`{"type":"message","content":[{"type":"text","text":"ok"}]}`))
	}))

	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, tpath("/preferred")):
			env.brainMu.Lock()
			payload := map[string]any{
				"account": native, "reason": "incumbent", "since": 0, "last_switch": 0,
				"pinned": nil,
				"weekly": map[string]float64{native: 41, other: 30},
				"lane_rules": []map[string]any{
					{"lane": "fable", "match": []string{"fable"}, "scope": "Fable"},
					{"lane": "haiku", "match": []string{"haiku"}, "scope": "Haiku"},
					{"lane": "default", "match": []string{}, "scope": nil},
				},
				"lanes": env.lanes,
			}
			if env.classesGone.Load() {
				payload["classes"] = map[string]any{} // present-but-empty: the class was REMOVED (#93)
			} else if env.classMembers != nil {
				name := env.className
				if name == "" {
					name = "frontier"
				}
				display, window := env.classDisplay, env.classWindow
				if display == "" {
					display = "Frontier (auto)"
				}
				if window == 0 {
					window = 200000
				}
				payload["classes"] = map[string]any{name: map[string]any{
					"display": display, "context_window": window,
					"pointer": nil, "pinned": nil, "sessions": map[string]any{},
					"members": withRoutes(name, env.classMembers),
				}}
			}
			if env.imageMembers != nil {
				classes, _ := payload["classes"].(map[string]any)
				if classes == nil {
					classes = map[string]any{}
					payload["classes"] = classes
				}
				classes["images"] = map[string]any{
					"display": "Images", "context_window": nil, "request_kind": "image",
					"pointer": nil, "pinned": nil, "sessions": map[string]any{},
					"members": withRoutes("images", env.imageMembers),
				}
				for name, ms := range env.imageClasses {
					classes[name] = map[string]any{
						"display": name, "context_window": nil, "request_kind": "image",
						"pointer": nil, "pinned": nil, "sessions": map[string]any{},
						"members": withRoutes(name, ms),
					}
				}
			}
			if env.brainHard > 0 {
				payload["thresholds"] = map[string]any{"high": 80.0, "margin": 15.0,
					"hard": env.brainHard, "dwell_s": 900}
			}
			if env.fleet != nil {
				payload["fleet"] = env.fleet
			}
			env.quotaBlocks(payload) // last: it reads the lane rules and weekly above
			env.brainMu.Unlock()
			_ = json.NewEncoder(w).Encode(payload)
		case strings.HasPrefix(r.URL.Path, tpath("/class-bind")):
			env.bindCalls.Add(1)
			if r.Header.Get("X-Switch-Key") != "e2e-key" {
				w.WriteHeader(401)
				return
			}
			q := r.URL.Query()
			cname, sid := q.Get("class"), q.Get("session")
			if cname == "" || sid == "" {
				w.WriteHeader(400)
				_, _ = w.Write([]byte(`{"error":"class and session are required"}`))
				return
			}
			env.brainMu.Lock()
			members := env.classMembers
			name := env.className
			if name == "" {
				name = "frontier"
			}
			bindKey, bindTo := sid, env.bindTo.Load().(string)
			if cname == "images" && env.imageMembers != nil {
				members, name, bindKey, bindTo = env.imageMembers, "images", "images|"+sid, ""
			}
			if ms, ok := env.imageClasses[cname]; ok && env.imageMembers != nil {
				members, name, bindKey, bindTo = ms, cname, cname+"|"+sid, ""
			}
			env.brainMu.Unlock()
			if env.bind404.Load() || cname != name || members == nil {
				w.WriteHeader(404)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "unknown class '" + cname + "'"})
				return
			}
			if b, _ := env.bind409.Load().(string); b != "" {
				w.WriteHeader(409)
				_, _ = w.Write([]byte(b))
				return
			}
			if b, _ := env.bind503.Load().(string); b != "" {
				w.WriteHeader(503)
				_, _ = w.Write([]byte(b))
				return
			}
			env.brainMu.Lock()
			id, created := env.binds[bindKey], false
			if id == "" {
				id = bindTo
				if id == "" && len(members) > 0 {
					id, _ = members[0]["id"].(string)
				}
				env.binds[bindKey] = id
				created = true
			}
			var member map[string]any
			for _, m := range members {
				if m["id"] == id {
					member = m
					break
				}
			}
			env.brainMu.Unlock()
			if member == nil {
				w.WriteHeader(409)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": "class_unavailable", "class": cname,
					"members": []any{}, "available_at": nil, "available_basis": nil, "retry_after_s": nil,
					"retryable": false, "options": []any{}})
				return
			}
			reason := "bind: pointer"
			if s, _ := env.bindReason.Load().(string); s != "" {
				reason = s
			}
			// The real brain's shape (_class_bind): the configured member,
			// its route beside it, never inside it; the live view (route,
			// incumbent, checks) is /preferred's.
			_ = json.NewEncoder(w).Encode(map[string]any{
				"class": cname, "session": sid, "member": member,
				"route":  withRoute(name, member)["route"],
				"reason": reason, "since": 0, "created": created,
			})
		case r.URL.Path == tpath("/reports") && r.Method == http.MethodPost:
			if r.Header.Get("X-Switch-Key") != "e2e-key" {
				w.WriteHeader(401)
				return
			}
			var body struct {
				Reporter string           `json:"reporter"`
				Events   []map[string]any `json:"events"`
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil || body.Reporter == "" || len(body.Events) > 500 {
				w.WriteHeader(400)
				return
			}
			results := make([]map[string]any, len(body.Events))
			env.brainMu.Lock()
			for i, e := range body.Events {
				result := "applied"
				if e["kind"] == "ledger" {
					l := make(map[string]any, len(e))
					for k, v := range e {
						if k != "kind" {
							l[k] = v
						}
					}
					dup := false
					for _, p := range env.ledgerPosts {
						dup = dup || p["id"] == l["id"]
					}
					if dup {
						result = "ignored" // the brain dedupes ledger ids
					} else {
						env.ledgerPosts = append(env.ledgerPosts, l)
					}
				} else {
					env.reports = append(env.reports, e)
				}
				results[i] = map[string]any{"id": e["id"], "result": result, "why": nil}
			}
			seq := len(env.ledgerPosts)
			env.brainMu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"results": results, "seq": seq})
		case r.URL.Path == tpath("/api-key"):
			if r.Header.Get("X-Switch-Key") != "e2e-key" {
				w.WriteHeader(401)
				return
			}
			acct := r.URL.Query().Get("account")
			env.brainMu.Lock()
			env.apiKeyCalls++
			key, ok := env.apiKeys[acct]
			hang, hangFor := env.apiKeyHang, env.apiKeyHangFor
			env.brainMu.Unlock()
			if hang && hangFor > 0 {
				select {
				case <-r.Context().Done():
				case <-time.After(hangFor):
					w.WriteHeader(http.StatusServiceUnavailable)
				}
				return
			}
			if hang {
				<-r.Context().Done()
				return
			}
			if !ok {
				w.WriteHeader(404)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error": "the brain has no key for api account '" + acct + "'", "reason": "no_key"})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"account": acct, "api_key": key})
		case strings.HasPrefix(r.URL.Path, tpath("/openai-token")):
			if r.Header.Get("X-Switch-Key") != "e2e-key" {
				w.WriteHeader(401)
				return
			}
			if pool := env.codexPool.Load().(string); pool != "" {
				names := strings.Split(pool, ",")
				name := r.URL.Query().Get("account")
				if name == "" {
					name = names[0] // the fake pool's incumbent
				}
				known := false
				for _, n := range names {
					known = known || n == name
				}
				if !known {
					w.WriteHeader(404)
					_ = json.NewEncoder(w).Encode(map[string]any{
						"error": "unknown codex account '" + name + "'"})
					return
				}
				env.codexDispenses.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"account_id":   "uuid-" + name,
					"access_token": "codex-tok-" + name,
					"expires_at":   float64(time.Now().Add(240 * time.Hour).Unix()),
					"name":         name,
				})
				return
			}
			n := env.codexDispenses.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"account_id":   "acct-e2e",
				"access_token": fmt.Sprintf("codex-tok-%d", n),
				"expires_at":   float64(time.Now().Add(240 * time.Hour).Unix()),
			})
		case strings.HasPrefix(r.URL.Path, tpath("/token")):
			env.tokenCalls.Add(1)
			a := r.URL.Query().Get("account")
			if rf := r.URL.Query().Get("refused_fp"); rf != "" {
				env.refusedCalls.Add(1)
				env.lastRefusedFP.Store(rf)
				env.brainMu.Lock()
				gate := env.tokGate
				env.brainMu.Unlock()
				if gate != nil {
					select {
					case <-gate:
					case <-r.Context().Done():
					}
				}
				if env.tokFail.Load() {
					http.Error(w, "brain restarting", http.StatusBadGateway)
					return
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"account": a, "access_token": env.brainCopy(a),
				"expires_at": float64(time.Now().Add(time.Hour).Unix()),
			})
		default:
			http.NotFound(w, r)
		}
	}))

	bridgeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		env.bridge <- b
		w.WriteHeader(200)
	}))

	// install globals; snapshot the old world
	oldUpstream, oldBrain := upstream, brainBase
	oldKey, oldInject, oldCoalesce := switchKey, injectFile, alertCoalesce
	upstream, brainBase = env.upstream.URL, brain.URL
	oldBridge := swapBridgeURL(bridgeSrv.URL)
	switchKey, injectFile, alertCoalesce = "e2e-key", "", 10*time.Millisecond

	lan.pref.mu.Lock()
	oldAcct, oldReason, oldWeekly := lan.pref.account, lan.pref.reason, lan.pref.weekly
	oldRules, oldLanes, oldOK := lan.pref.laneRules, lan.pref.lanes, lan.pref.ok
	oldClasses, oldHard := lan.pref.classes, lan.pref.hardPct
	lan.pref.account, lan.pref.reason, lan.pref.weekly, lan.pref.laneRules, lan.pref.lanes, lan.pref.ok =
		"", "", nil, nil, nil, false
	lan.pref.classes, lan.pref.hardPct = nil, 0
	lan.pref.mu.Unlock()
	// The quota model's view and the record store are global: a prior test's
	// standing or refusal must not steer THIS test's routing.
	restoreQuota := resetQuotaState()
	lan.tokMu.Lock()
	oldTokens := lan.tokens
	lan.tokens = map[string]tokEntry{}
	lan.tokMu.Unlock()
	lan.effMu.Lock()
	oldEff := lan.lastEffective
	lan.lastEffective = map[string]string{}
	lan.effMu.Unlock()
	lan.bindMu.Lock()
	oldBinds, oldFundings := lan.bindCache, lan.sessionFundings
	lan.bindCache, lan.sessionFundings = map[string]classBind{}, map[string]sessionFunding{}
	lan.bindMu.Unlock()
	// derived class-row store (#93): global — a prior test's derived rows or
	// tombstones must not advertise (or refuse) into THIS test's picker
	lan.classRowsMu.Lock()
	oldClassRows, oldClassSrc := lan.classRows, lan.classRowsSource
	lan.classRows, lan.classRowsSource = nil, ""
	lan.classRowsMu.Unlock()
	// codex bearer cache (#33/#71): global — a prior test's dispensed bearer
	// must not serve THIS test's first request (dispense counting depends on it)
	lan.codexTokMu.Lock()
	oldCodexToks := lan.codexToks
	lan.codexToks = map[string]codexTokEntry{}
	lan.codexTokMu.Unlock()
	// the coalescer queue is global: a prior test's queued flips must not flush
	// into THIS test's bridge (and vice versa on restore). Both async publishers
	// are joined too, as evTest does: a flush already running and a response
	// aggregator still settling publish into whatever ring is armed, which after
	// evReset is the next test's.
	drainAlertQueue := func() {
		alertQuiesce(t)
		respTapSettle()
		alertMu.Lock()
		alertHistory = nil // gate history is global too — same bleed hazard
		alertMu.Unlock()
	}
	drainAlertQueue()
	// the event hub (#59) and live injection queue (#66) are package-level singletons:
	// prior tests must not leak subscribers, replay frames, or queued work into this one.
	evReset()
	lan.injectReset()

	// The reporter runs beside the proxy, as in production (reports.go),
	// posting to this fake brain; restore stops it before the globals go.
	switchKey = "e2e-key"
	lan.startReporter()

	fetchPreferredSync()
	// Both halves (#126 C0): `gated` is :8787 exactly as before, and `core` is
	// the SAME routing chain the HTTPS-proxy entry dispatches into — which is
	// what lets a test prove the two entries share one chain rather than
	// comparing two lookalikes.
	gated, core := buildHandlers()
	env.core = core
	env.proxy = httptest.NewServer(gated)

	restore := func() {
		drainAlertQueue()
		evReset()
		lan.injectReset()
		lan.stopReporterNow(context.Background())
		env.proxy.Close()
		env.upstream.Close()
		brain.Close()
		bridgeSrv.Close()
		upstream, brainBase = oldUpstream, oldBrain
		swapBridgeURL(oldBridge)
		switchKey, injectFile, alertCoalesce = oldKey, oldInject, oldCoalesce
		lan.pref.mu.Lock()
		lan.pref.account, lan.pref.reason, lan.pref.weekly = oldAcct, oldReason, oldWeekly
		lan.pref.laneRules, lan.pref.lanes, lan.pref.ok = oldRules, oldLanes, oldOK
		lan.pref.classes, lan.pref.hardPct = oldClasses, oldHard
		lan.pref.mu.Unlock()
		lan.tokMu.Lock()
		lan.tokens = oldTokens
		lan.tokMu.Unlock()
		restoreQuota()
		lan.effMu.Lock()
		lan.lastEffective = oldEff
		lan.effMu.Unlock()
		lan.bindMu.Lock()
		lan.bindCache, lan.sessionFundings = oldBinds, oldFundings
		lan.bindMu.Unlock()
		lan.classRowsMu.Lock()
		lan.classRows, lan.classRowsSource = oldClassRows, oldClassSrc
		lan.classRowsMu.Unlock()
		lan.codexTokMu.Lock()
		lan.codexToks = oldCodexToks
		lan.codexTokMu.Unlock()
	}
	return env, restore
}

func (env *e2eEnv) post(t *testing.T, model, clientBearer string) *http.Response {
	t.Helper()
	return env.postSess(t, model, clientBearer, "")
}

// postSess is post with an x-claude-code-session-id header (#27 class routing
// binds per session; "" sends no header = the proxy's "no-session" fallback).
func (env *e2eEnv) postSess(t *testing.T, model, clientBearer, session string) *http.Response {
	t.Helper()
	body := fmt.Sprintf(`{"model":%q,"max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`, model)
	req, _ := http.NewRequest("POST", env.proxy.URL+"/v1/messages", bytes.NewReader([]byte(body)))
	req.Header.Set("Authorization", "Bearer "+clientBearer)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("anthropic-beta", "oauth-2025-04-20")
	if session != "" {
		req.Header.Set("x-claude-code-session-id", session)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request failed: %v", err)
	}
	return resp
}

func (env *e2eEnv) hit(t *testing.T) e2eHit {
	t.Helper()
	select {
	case h := <-env.seen:
		return h
	case <-time.After(3 * time.Second):
		t.Fatal("upstream never saw the request")
		return e2eHit{}
	}
}

// The headline: fable INJECTs one account's bearer while haiku INJECTs the other's —
// same proxy instance, same second, no restarts, no global switch. Neither rides the
// client's ambient login (that "CLIENT" bearer must never reach upstream).
func TestE2ESplitRoutingSameInstant(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	resp := env.post(t, "claude-fable-5", "CLIENT")
	resp.Body.Close()
	if h := env.hit(t); h.auth != "Bearer "+e2eToken(other) || h.model != "claude-fable-5" {
		t.Errorf("fable request must arrive on %s's bearer, got auth=%q model=%q", other, h.auth, h.model)
	}

	resp = env.post(t, "claude-haiku-4-5", "CLIENT")
	resp.Body.Close()
	if h := env.hit(t); h.auth != "Bearer "+e2eToken(native) {
		t.Errorf("haiku request must INJECT %s's bearer (never the client's ambient login), got %q", native, h.auth)
	}

	// and the replayed body reached upstream intact (buffering didn't eat it)
	resp = env.post(t, "claude-fable-5", "CLIENT")
	defer resp.Body.Close()
	if h := env.hit(t); h.model != "claude-fable-5" {
		t.Errorf("upstream got a mangled body, model=%q", h.model)
	}
	if resp.StatusCode != 200 {
		t.Errorf("client must see 200, got %d", resp.StatusCode)
	}
}

// #96: advertising a real Anthropic id changes nothing about how it routes.
// claude-fable-5 appears in /v1/models AND still classifies into the fable lane,
// arriving on that lane's injected bearer with its model id unrewritten — no
// provider capture, no registry refusal, no class binding in the path.
func TestE2EAdvertisedAnthropicIDStillRoutesByLane(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	oldLocal := localUpstream
	localUpstream = ""
	t.Cleanup(func() { localUpstream = oldLocal; providers = nil })
	withModelsFile(t, `[{"name":"fable-5","display":"Claude Fable 5","type":"anthropic","context_window":200000}]`)
	rebuildProxy(env)

	if ids := getModels(t, env.proxy.URL); len(ids) != 1 || ids[0] != "claude-fable-5" {
		t.Fatalf("advertised %v, want exactly [claude-fable-5]", ids)
	}
	resp := env.post(t, "claude-fable-5", "CLIENT")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200 — an advertised anthropic row must not be refused", resp.StatusCode)
	}
	if h := env.hit(t); h.auth != "Bearer "+e2eToken(other) || h.model != "claude-fable-5" {
		t.Errorf("fable request must reach the upstream on %s's bearer with the id unrewritten, got auth=%q model=%q",
			other, h.auth, h.model)
	}
}

// #96: the "[1m]" suffix is renderer-owned picker metadata and must NEVER reach
// the wire. Anthropic has never seen it, so "claude-fable-5[1m]" forwarded
// verbatim onto an account lane 404s (spikes/model-registry/FINDINGS.md:152-155).
// Named rows shed it via upstreamID and class rows via routeClass; the lane
// passthrough is the arm that has to strip it itself — while still classifying
// into the same lane.
func TestE2EOneMSuffixStrippedBeforeAnthropicLane(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	// the suffix is invisible to lane inference: both ids land on fable
	rules := []laneRule{{Lane: "fable", Match: []string{"fable"}}, {Lane: "default"}}
	if lane := laneForModel("claude-fable-5[1m]", rules); lane != "fable" {
		t.Fatalf("laneForModel(claude-fable-5[1m]) = %q, want fable", lane)
	}

	resp := env.post(t, "claude-fable-5[1m]", "CLIENT")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	h := env.hit(t)
	if h.model != "claude-fable-5" {
		t.Errorf("upstream saw model=%q, want the suffix-free claude-fable-5 — Anthropic 404s the picker id", h.model)
	}
	if h.auth != "Bearer "+e2eToken(other) {
		t.Errorf("stripping the suffix must not move the lane: got auth=%q, want %s's bearer", h.auth, other)
	}
}

// A brain-side lane flip surfaces as ONE per-lane alert at the lane's next request.
func TestE2ELaneFlipFiresLaneAlert(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	// start fable on `native` so there's a previous effective account
	env.brainMu.Lock()
	env.lanes["fable"] = map[string]any{"account": native, "reason": "incumbent",
		"effective": map[string]float64{native: 41, other: 30}}
	env.brainMu.Unlock()
	fetchPreferredSync()
	env.post(t, "claude-fable-5", "CLIENT").Body.Close()
	env.hit(t)

	// brain flips fable to the 40x account
	env.brainMu.Lock()
	env.lanes["fable"] = map[string]any{"account": other, "reason": "fable cap hot",
		"effective": map[string]float64{native: 85, other: 70}}
	env.brainMu.Unlock()
	fetchPreferredSync()
	env.post(t, "claude-fable-5", "CLIENT").Body.Close()
	env.hit(t)

	select {
	case b := <-env.bridge:
		var p struct {
			Alerts []bridgeAlert `json:"alerts"`
		}
		if err := json.Unmarshal(b, &p); err != nil || len(p.Alerts) != 1 {
			t.Fatalf("want 1 alert, got err=%v n=%d", err, len(p.Alerts))
		}
		a := p.Alerts[0]
		if a.Labels["lane"] != "fable" || a.Labels["to_account"] != other {
			t.Errorf("alert must say fable->%s, got lane=%q to=%q", other, a.Labels["lane"], a.Labels["to_account"])
		}
		if a.Labels["delivery"] != "telegram" {
			t.Errorf("fable-effective 70%% on the destination is routine — want telegram, got %q",
				a.Labels["delivery"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("lane flip never produced a bridge alert")
	}

	// haiku never moved: no second alert
	select {
	case b := <-env.bridge:
		t.Fatalf("unexpected extra alert: %s", b)
	case <-time.After(150 * time.Millisecond):
	}
}

// A 429 that rejects no window (no unified headers, generic words) is a
// throttle (§2.3): the turn fails over, the account is deranked on every lane,
// and no lane's check closes.
func TestE2E429UnclassifiedThrottlesAccount(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	env.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"You have exceeded your weekly usage limit."}}`)
	env.deny429.Store(e2eToken(other))

	resp := env.post(t, "claude-fable-5", "CLIENT")
	defer resp.Body.Close()
	if h := env.hit(t); h.auth != "Bearer "+e2eToken(other) {
		t.Fatalf("first attempt must go to %s, got %q", other, h.auth)
	}
	if h := env.hit(t); h.auth != "Bearer "+e2eToken(native) {
		t.Fatalf("429 retry must fail over by INJECTING %s's bearer (never the client's login), got %q", native, h.auth)
	}
	if resp.StatusCode != 200 {
		t.Errorf("client must see the retried 200, got %d", resp.StatusCode)
	}
	if got := recordsOn(other); len(got) != 1 || got[0] != "throttled:throttle" {
		t.Errorf("records on %s = %v, want one throttle", other, got)
	}
	if laneNo(other, "fable") || laneNo(other, "haiku") || laneNo(other, "default") {
		t.Error("a throttle deranks; it must not close any lane")
	}
}

// A 429 whose headers reject the Fable scope (a captured 7d_oi)
// closes ONLY the fable route — haiku can still ride the same account. The body
// names no model; the brain's aliases say what 7d_oi is.
func TestE2E429ClassifiedCoolsLaneOnly(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	env.capturedFable429(t, other)
	env.deny429.Store(e2eToken(other))

	resp := env.post(t, "claude-fable-5", "CLIENT")
	resp.Body.Close()
	env.hit(t) // 429'd attempt on other
	env.hit(t) // retried on native

	if !laneNo(other, "fable") {
		t.Error("model-cap 429 must close its own scope's route")
	}
	if laneNo(other, "haiku") || laneNo(other, "default") {
		t.Error("model-cap 429 must NOT close other lanes on the account")
	}
}

// When there is no failover left, the client must still receive the ORIGINAL 429
// body — classification reads it, but must hand it back intact.
func TestE2E429BodySurvivesClassification(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	const errBody = `{"type":"error","error":{"type":"rate_limit_error","message":"You have exceeded your weekly usage limit."}}`
	env.body429.Store(errBody)
	env.deny429.Store(e2eToken(other))
	seedSpent(native, "seven_day", 429, "weekly limit", time.Time{}) // no alternative: the walk must decline

	resp := env.post(t, "claude-fable-5", "CLIENT")
	defer resp.Body.Close()
	env.hit(t)
	if resp.StatusCode != 429 {
		t.Fatalf("with no failover the client must see the 429, got %d", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	if string(b) != errBody {
		t.Errorf("429 body must survive classification intact, got %q", b)
	}
}

// The brotli incident, replayed: the client offers an encoding the proxy
// cannot read (Claude Code offers br; Anthropic brotli'd its 429 bodies), and a
// model-cap 429 comes back on the wire. The client's Accept-Encoding must never
// reach the upstream hop — the transport owns negotiation, so the body arrives
// plain for every body-reading path (the shape gate, quota phrases). The
// cooldown stays scoped to the capped lane instead of blacklisting the whole
// account (which is what starved opus onto an exhausted sibling account).
func TestE2E429EncodedWireStillClassifies(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	env.capturedFable429(t, other)
	env.deny429.Store(e2eToken(other))
	env.deny429Enc.Store(true)

	body := `{"model":"claude-fable-5","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`
	req, _ := http.NewRequest("POST", env.proxy.URL+"/v1/messages", bytes.NewReader([]byte(body)))
	req.Header.Set("Authorization", "Bearer CLIENT")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("anthropic-beta", "oauth-2025-04-20")
	req.Header.Set("Accept-Encoding", "br") // what the real CLI offers; the proxy can't decode it
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	h := env.hit(t) // 429'd attempt on other
	if strings.Contains(h.ae, "br") {
		t.Fatalf("the client's Accept-Encoding must never reach the upstream hop, upstream saw %q", h.ae)
	}
	env.hit(t) // retried on native

	if resp.StatusCode != 200 {
		t.Errorf("client must see the retried 200, got %d", resp.StatusCode)
	}
	if !laneNo(other, "fable") {
		t.Error("model-cap 429 must close its own scope even on an encoding-negotiating wire")
	}
	if laneNo(other, "haiku") || laneNo(other, "default") {
		t.Error("an encoded wire must not degrade a model-cap 429 into an account-wide record")
	}
}

// Unparseable bodies route on the default lane (the legacy policy) and are counted.
func TestE2EParseFailureCountsAndDefaults(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	before := atomic.LoadInt64(&laneParseFailures)
	req, _ := http.NewRequest("POST", env.proxy.URL+"/v1/messages", bytes.NewReader([]byte("not json at all")))
	req.Header.Set("Authorization", "Bearer CLIENT")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if h := env.hit(t); h.auth != "Bearer "+e2eToken(native) {
		t.Errorf("unparseable body must route on the default lane (INJECT %s, not the client's login), got %q", native, h.auth)
	}
	if got := atomic.LoadInt64(&laneParseFailures); got != before+1 {
		t.Errorf("laneParseFailures must increment, got %d -> %d", before, got)
	}
}

// /_switch/status must expose the lane world: mode, rules, per-lane routing.
func TestE2EStatusShape(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	env.post(t, "claude-fable-5", "CLIENT").Body.Close()
	env.hit(t)

	// The lane world is the household's: under members.<lan_tenant>.
	var s map[string]any
	if err := json.Unmarshal(statusMine(t, env.proxy.URL), &s); err != nil {
		t.Fatalf("status not JSON: %v", err)
	}
	if s["lane_mode"] != "lanes" {
		t.Errorf("lane_mode must be \"lanes\" with a lane-serving brain, got %v", s["lane_mode"])
	}
	lanes, _ := s["lanes"].(map[string]any)
	fable, _ := lanes["fable"].(map[string]any)
	if fable == nil || fable["preferred"] != other {
		t.Errorf("status lanes.fable.preferred must be %s, got %v", other, fable)
	}
	effs, _ := s["effective_by_lane"].(map[string]any)
	if effs["fable"] != other {
		t.Errorf("effective_by_lane.fable must be %s after a fable request, got %v", other, effs["fable"])
	}
	// lane_parse_failures is the proxy's one counter: at the top, beside
	// lan_tenant, not under a member.
	top := statusTop(t, env.proxy.URL)
	if _, ok := top["lane_parse_failures"]; !ok {
		t.Error("status must expose lane_parse_failures at the top")
	}
	if top["lan_tenant"] != testLanTenant {
		t.Errorf("status lan_tenant = %v, want the household", top["lan_tenant"])
	}
	if s["brain_reachable"] != true {
		t.Errorf("brain_reachable must be true, got %v", s["brain_reachable"])
	}
}

// (#90) On the direct lane path a client-shape 429 must pass through untouched:
// no account cooldown, no failover to the sibling account, no switchover alert.
// The account is healthy; the client's request shape is what Anthropic refused.
func TestE2E429ShapeRejectionPassesThroughUncooled(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	env.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"Error"},"request_id":"req_x"}`)
	env.deny429.Store(e2eToken(other))

	resp := env.post(t, "claude-fable-5", "CLIENT")
	defer resp.Body.Close()
	if h := env.hit(t); h.auth != "Bearer "+e2eToken(other) {
		t.Fatalf("first attempt must go to %s, got %q", other, h.auth)
	}
	select {
	case extra := <-env.seen:
		t.Fatalf("shape rejection must not fail over to the sibling account, saw %+v", extra)
	case <-time.After(150 * time.Millisecond):
	}
	if resp.StatusCode != 429 {
		t.Fatalf("client must see the 429 intact, got %d", resp.StatusCode)
	}
	if got := recordsOn(other); len(got) != 0 {
		t.Fatalf("shape rejection must write no record, got %v", got)
	}
	select {
	case b := <-env.bridge:
		t.Fatalf("shape rejection must not fire a switchover alert, got %s", b)
	case <-time.After(150 * time.Millisecond):
	}
}

// capturedFable429 arms the fake upstream with a captured Fable 429 for
// account's bearer (scopes_test.go's capture2110, its reset moved two days on)
// and the brain with that account's scoped:Fable window and its aliases, the
// way the C5 brain publishes them.
func (env *e2eEnv) capturedFable429(t *testing.T, account string) time.Time {
	t.Helper()
	reset := time.Now().Add(48 * time.Hour).Truncate(time.Second)
	hs := map[string]string{}
	for k, vs := range capturedFable429(capture2110, reset) {
		hs[k] = vs[0]
	}
	env.anthHeadersFor.Store(map[string]map[string]string{e2eToken(account): hs})
	env.body429.Store(fable429Body)
	env.brainMu.Lock()
	env.views = map[string]any{account: map[string]any{"kind": "anthropic_account",
		"good_ts": float64(time.Now().Add(-time.Minute).Unix()),
		"windows": map[string]any{
			"seven_day": map[string]any{"percent": 89.0, "resets_at": reset.Unix()},
			"scoped:Fable": map[string]any{"percent": 99.0, "resets_at": reset.Unix(),
				"aliases": map[string]any{"header": []string{"7d_oi"}, "claim": []string{"seven_day_overage_included"}}},
		}}}
	env.brainMu.Unlock()
	fetchPreferredSync()
	return reset
}

// e2eDenyModel is env.denyModel's value.
type e2eDenyModel struct {
	model   string
	headers map[string]string
}

// syncBuf is captureLog's buffer with a mutex around it.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func captureLogSync(t *testing.T) *syncBuf {
	t.Helper()
	buf := &syncBuf{}
	old := log.Writer()
	log.SetOutput(buf)
	t.Cleanup(func() { log.SetOutput(old) })
	return buf
}
