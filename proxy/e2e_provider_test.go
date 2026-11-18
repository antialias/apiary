package main

// e2e tests for hosted providers (Kimi via Moonshot's native Anthropic endpoint,
// GPT via the codex subscription backend, #33) through the REAL handler stack.
// Since quota-model C2 a hosted provider is reached only as a member of a
// workload class, so these drive a class session bound to the member. They prove
// the contract that differs from an Anthropic lane: the provider's OWN auth is
// attached (the client's Anthropic bearer never leaks), the class picker id is
// rewritten to the member's model id, and the upstream base path is preserved.
// A hosted request and an Anthropic account-lane request share one proxy
// instance. Codex-specific behavior (wire translation, dispensed bearer, SSE
// re-framing) lives in e2e_codex_test.go on this same harness.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type e2eProvHit struct {
	auth, model, path string
	// beta is the anthropic-beta the provider SAW. kimi's bearerWire forwards
	// client headers verbatim, so this is the one lane where a client-side beta
	// flag actually reaches a third party (#126 test 3).
	beta string
	// header is the FULL header set the provider saw. The kimi lane's
	// bearerWire forwards client headers verbatim, so this is what proves no
	// client credential reached a third party (#126 review S11).
	header http.Header
	body   []byte
}

// codexHit captures everything the codex contract pins on the outbound hop:
// the dispensed bearer, the mandatory account-id/originator/UA trio, the fixed
// backend path, the translated body, the session_id the prompt cache keys its
// affinity on, and (hygiene) that no Anthropic client header rode along.
type codexHit struct {
	auth, acct, originator, ua, path, beta, session string
	accept                                          string
	body                                            []byte
}

// defaultCodexSSE is the canned happy-path codex stream: two text deltas
// assembling "codex ok", then a completed response with usage.
const defaultCodexSSE = `event: response.created
data: {"type":"response.created","response":{"id":"resp_1","model":"gpt-5.6"}}

event: response.output_item.added
data: {"type":"response.output_item.added","item":{"type":"message"}}

event: response.output_text.delta
data: {"type":"response.output_text.delta","delta":"codex "}

event: response.output_text.delta
data: {"type":"response.output_text.delta","delta":"ok"}

event: response.output_item.done
data: {"type":"response.output_item.done","item":{"type":"message","content":[{"type":"output_text","text":"codex ok"}]}}

event: response.completed
data: {"type":"response.completed","response":{"id":"resp_1","model":"gpt-5.6","status":"completed","usage":{"input_tokens":11,"output_tokens":5}}}

`

type e2eHosted struct {
	*e2eEnv
	up        *httptest.Server
	codexUp   *httptest.Server
	hosted    chan e2eProvHit // requests that reached the hosted upstream (≠ embedded .seen = Anthropic)
	codexSeen chan codexHit   // requests that reached the codex upstream
	deny      atomic.Bool     // when set, the hosted upstream returns 429
	kimi401   atomic.Bool     // when set, the hosted upstream returns a 401 authentication_error
	hosted500 atomic.Bool     // controlled head failure for the health rollout gate
	deny403   atomic.Bool     // when set, the hosted upstream returns kimi's quota 403 (#89)
	stall     atomic.Bool     // when set, the hosted upstream accepts and then NEVER answers (a wedged rung)
	codexDeny atomic.Bool     // when set, the codex upstream returns 429
	// codexQuota: the codex backend's EXPLICIT budget refusal, as distinct from
	// codexDeny's ambiguous "rate limited".
	codexQuota atomic.Bool
	codex401   atomic.Int64 // the next N codex requests 401 (bearer-retry drill)
	codexSSE   atomic.Value // string: SSE script served on 200 ("" = defaultCodexSSE)
	codexHold  atomic.Bool  // after the script, send nothing until the proxy hangs up (a stall)
	// codexStall: the next N codex requests hang until the proxy hangs up —
	// before the head when codexStallHead, else after a bare 200 head.
	codexStall     atomic.Int64
	codexStallHead atomic.Bool
	codexFirstMS   atomic.Int64 // the 200 head goes out, then this many ms before the first byte
	kimiSSE        atomic.Value // string: #61 — hosted upstream serves this as SSE ("" = JSON)
	// kimiHold: after the kimiSSE script, send nothing more and keep the
	// connection open until the proxy hangs up (or 10 s pass); kimiHoldCut
	// is set when it was the proxy's hang-up that ended the hold.
	kimiHold    atomic.Bool
	kimiHoldCut atomic.Bool
	kimi403     atomic.Value // string: when set, the hosted upstream returns this 403 body (C7's wordings)
	kimiJSON    atomic.Value // string: #96 — hosted upstream serves this as the 200 JSON body ("" = default)
	// #71: 429 only requests whose Authorization contains this substring —
	// lets one pool account be exhausted while its sibling still serves.
	codexDenyBearer atomic.Value
	// codexHook (C4), when set, answers a codex request the knobs above let
	// through (401, quota, deny, denyBearer and stall still come first): it
	// gets the request body and the call's 1-based count, and returns whether
	// it wrote the reply. The image tests' upstream: image streams, per-call
	// refusals, x-codex-* headers.
	// func(w http.ResponseWriter, r *http.Request, body []byte, n int64) bool.
	codexHook  atomic.Value
	codexCalls atomic.Int64
}

// newE2EHosted enables Kimi (fake Anthropic-format upstream, its key injected) and the
// codex-backed gpt lane (fake codex upstream, bearer dispensed by the fake brain),
// disables the local runtime, and rebuilds the proxy. kimiPathSuffix is appended to the
// fake base URL to exercise the ReverseProxy path-join (Moonshot serves under /anthropic).
func newE2EHosted(t *testing.T, kimiPathSuffix string) (*e2eHosted, func()) {
	t.Helper()
	env, restoreBase := newE2E(t)
	h := &e2eHosted{e2eEnv: env, hosted: make(chan e2eProvHit, 32), codexSeen: make(chan codexHit, 32)}
	h.codexSSE.Store("")
	h.kimiSSE.Store("")
	h.kimi403.Store("")
	h.kimiJSON.Store("")
	h.codexDenyBearer.Store("")

	h.up = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/v1/messages") {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		h.hosted <- e2eProvHit{auth: r.Header.Get("Authorization"), model: extractModel(body),
			path: r.URL.Path, beta: r.Header.Get("anthropic-beta"),
			header: r.Header.Clone(), body: body}
		if strings.HasSuffix(r.URL.Path, "/count_tokens") {
			// The hosted provider counts with its OWN tokenizer (#44) — the
			// canned 1000 is in upstream units for the scaling tests.
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"input_tokens":1000}`))
			return
		}
		if h.kimi401.Load() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(401)
			_, _ = w.Write([]byte(e2eKimi401))
			return
		}
		if h.deny.Load() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"rate_limit_error","message":"provider busy"}}`))
			return
		}
		if h.hosted500.Load() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"api_error","message":"acceptance upstream failure"}}`))
			return
		}
		if h.stall.Load() {
			// The half-dead rung: headers never arrive, the client never learns
			// why. Hold until the proxy gives up and the test tears us down.
			<-r.Context().Done()
			return
		}
		if h.deny403.Load() {
			// Verbatim shape of Moonshot's cycle cap: a 403, not a 429.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(403)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"permission_error","message":"You've reached your usage limit for this billing cycle. Your quota will be refreshed in the next cycle. To continue now, purchase extra usage or upgrade your plan: https://www.kimi.com/code/#pricing"}}`))
			return
		}
		if b := h.kimi403.Load().(string); b != "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(403)
			_, _ = w.Write([]byte(b))
			return
		}
		if sse := h.kimiSSE.Load().(string); sse != "" {
			e2eServeSSE(w, sse, 0, false)
			if h.kimiHold.Load() {
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
					h.kimiHoldCut.Store(true)
				case <-time.After(10 * time.Second):
				}
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if j := h.kimiJSON.Load().(string); j != "" {
			_, _ = w.Write([]byte(j))
			return
		}
		_, _ = w.Write([]byte(`{"type":"message","content":[{"type":"text","text":"hosted ok"}]}`))
	}))

	h.codexUp = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		h.codexSeen <- codexHit{auth: r.Header.Get("Authorization"),
			acct: r.Header.Get("ChatGPT-Account-Id"), originator: r.Header.Get("originator"),
			ua: r.Header.Get("User-Agent"), path: r.URL.Path,
			beta: r.Header.Get("anthropic-beta"), session: r.Header.Get("session_id"),
			accept: r.Header.Get("Accept"), body: body}
		if h.codexStall.Add(-1) >= 0 {
			if !h.codexStallHead.Load() {
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("X-Request-Id", "req-stall-1")
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
			}
			<-r.Context().Done()
			return
		}
		h.codexStall.Store(0)
		n := h.codexCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if h.codex401.Load() > 0 {
			h.codex401.Add(-1)
			w.WriteHeader(401)
			_, _ = w.Write([]byte(`{"detail":"token expired"}`))
			return
		}
		if h.codexQuota.Load() {
			// usage_limit_reached + a resets_at two days out: gpt's weekly
			// bucket, the shape that can leave a class memberless.
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"detail":{"type":"usage_limit_reached","plan_type":"pro","resets_at":` +
				strconv.FormatInt(time.Now().Add(52*time.Hour).Unix(), 10) + `}}`))
			return
		}
		if h.codexDeny.Load() {
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"detail":"rate limited"}`))
			return
		}
		if db := h.codexDenyBearer.Load().(string); db != "" &&
			strings.Contains(r.Header.Get("Authorization"), db) {
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"detail":"rate limited"}`))
			return
		}
		if hook, _ := h.codexHook.Load().(func(http.ResponseWriter, *http.Request, []byte, int64) bool); hook != nil && hook(w, r, body, n) {
			return
		}
		sse := h.codexSSE.Load().(string)
		if sse == "" {
			sse = defaultCodexSSE
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		if ms := h.codexFirstMS.Load(); ms > 0 {
			w.(http.Flusher).Flush()
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
		_, _ = io.WriteString(w, sse)
		if h.codexHold.Load() {
			w.(http.Flusher).Flush()
			select {
			case <-r.Context().Done():
			case <-time.After(10 * time.Second):
			}
		}
	}))

	// snapshot + install provider globals
	oldLocal := localUpstream
	oldKU := kimiUpstream
	oldCU := codexUpstream
	localUpstream = "" // isolate: table = kimi + gpt only
	// Kimi's key is the tenant's, dispensed by its brain's /api-key (bearerWire).
	kimiUpstream = h.up.URL + kimiPathSuffix
	env.brainMu.Lock()
	if env.apiKeys == nil {
		env.apiKeys = map[string]string{}
	}
	env.apiKeys["kimi"] = "kimi-secret"
	env.brainMu.Unlock()
	codexUpstream = h.codexUp.URL

	env.proxy.Close()
	env.proxy = httptest.NewServer(buildHandler())

	restore := func() {
		h.up.Close()
		h.codexUp.Close()
		localUpstream = oldLocal
		kimiUpstream = oldKU
		codexUpstream = oldCU
		providers = nil
		restoreBase()
	}
	return h, restore
}

func (h *e2eHosted) codexHitOrFail(t *testing.T) codexHit {
	t.Helper()
	select {
	case hit := <-h.codexSeen:
		return hit
	case <-time.After(3 * time.Second):
		t.Fatal("codex upstream never saw the request")
		return codexHit{}
	}
}

func (h *e2eHosted) noCodexLeak(t *testing.T) {
	t.Helper()
	select {
	case hit := <-h.codexSeen:
		t.Fatalf("request leaked to the codex upstream: %+v", hit)
	default:
	}
}

func (h *e2eHosted) hostedHit(t *testing.T) e2eProvHit {
	t.Helper()
	select {
	case hit := <-h.hosted:
		return hit
	case <-time.After(3 * time.Second):
		t.Fatal("hosted upstream never saw the request")
		return e2eProvHit{}
	}
}

// noAnthropicLeak asserts nothing reached the Anthropic upstream (the embedded .seen).
func (h *e2eHosted) noAnthropicLeak(t *testing.T) {
	t.Helper()
	select {
	case leaked := <-h.seen:
		t.Fatalf("request leaked to the Anthropic upstream: %+v", leaked)
	default:
	}
}

func TestE2EKimiInjectsKeyRewritesModelPreservesPath(t *testing.T) {
	h, restore := newE2EClassOn(t, "kimi/kimi-k3", 0.61)
	defer restore()

	resp := h.postSess(t, "claude-frontier", "client-oauth-bearer", "sess-kimi-key")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "hosted ok") {
		t.Fatalf("response did not come from the hosted provider: %s", b)
	}

	hit := h.hostedHit(t)
	// MUTATION GUARD: the provider's own key must be INJECTED — not the client's
	// Anthropic bearer (leak), and not stripped (llama-server semantics). Flipping the code
	// to Del(Authorization) or leaving the client bearer fails right here.
	if hit.auth != "Bearer kimi-secret" {
		t.Fatalf("Authorization = %q, want the injected Moonshot key (not the client bearer, not stripped)", hit.auth)
	}
	if hit.model != "kimi-k3" {
		t.Fatalf("upstream saw model %q, want the class picker id rewritten to kimi-k3", hit.model)
	}
	// path-join: the provider's /coding base + /v1/messages
	if hit.path != "/coding/v1/messages" {
		t.Fatalf("upstream path = %q, want /coding/v1/messages", hit.path)
	}
	h.noAnthropicLeak(t)
}

func TestE2EHostedAndAccountLaneSameInstance(t *testing.T) {
	h, restore := newE2EClassOn(t, "kimi/kimi-k3", 0.61)
	defer restore()

	// a Kimi-bound class main loop and an Anthropic haiku background call, same proxy
	resp := h.postSess(t, "claude-frontier", "client-bearer", "sess-kimi-main")
	resp.Body.Close()
	resp = h.post(t, "claude-haiku-4-5", "client-bearer")
	resp.Body.Close()

	kimi := h.hostedHit(t)
	if kimi.model != "kimi-k3" || kimi.auth != "Bearer kimi-secret" {
		t.Fatalf("kimi hit = %+v", kimi)
	}
	haiku := h.hit(t) // Anthropic upstream
	if haiku.model != "claude-haiku-4-5" {
		t.Fatalf("anthropic upstream saw %q", haiku.model)
	}
	if haiku.auth == "" || haiku.auth == "Bearer kimi-secret" {
		t.Fatalf("haiku lane auth = %q, want its Anthropic account bearer", haiku.auth)
	}
}
