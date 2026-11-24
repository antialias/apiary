package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

// ---- provider wires (#41) ----
//
// A direct provider is reached over a WIRE: the request/response shape its
// upstream speaks, plus how that upstream is authenticated. Kimi and the local
// llama-server speak Anthropic Messages natively — their wire only swaps the
// auth header, and local also reshapes its over-window refusal. The gpt lane
// speaks codex/Responses and is translated in both directions (wire_codex.go).
//
// Everything protocol-specific lives behind this interface, so the core routes
// on the PROVIDER and never asks which protocol it is. Adding a provider is a
// new wire plus a row in the table at buildHandler — no branch anywhere else.
type wire interface {
	// authLabel is this wire's "auth" mode in /_switch/status. The value is a
	// deploy contract: a deploy check gates on gpt reporting "codex-bearer".
	authLabel() string

	// name is the wire's family, which the health rows and the refusal
	// adapters discriminate on (memberWireName).
	name() string

	// owns reports whether an OUTBOUND request is aimed at this wire's upstream.
	// It is the hop discriminator: the transport uses it to decide response
	// translation and retry gating for the hop that ACTUALLY served, which the
	// class ladder can change mid-request. Pass-through wires own nothing —
	// there is nothing to translate and nothing to re-auth.
	owns(req *http.Request) bool

	// prepare validates one inbound Anthropic request for this wire and fills
	// the wire-owned fields of info (body, and for a translated wire the
	// per-request credential and the client's stream wish). A non-empty return
	// is a refusal reason: the direct path turns it into a typed 503, the class
	// ladder falls through to the next candidate. Pass-through wires never refuse.
	prepare(info *reqInfo, body []byte, inPath string) string

	// apply stamps info onto an OUTBOUND request: auth, and — for a translated
	// wire — the fixed backend path, a complete fresh header set, and a
	// replayable body for the 401 retry. Runs LAST, after the URL is set.
	apply(out *http.Request, info reqInfo)

	// translate converts an upstream response back into the Anthropic contract.
	translate(resp *http.Response, info reqInfo) *http.Response

	// reauth drops this wire's cached credential, acquires a fresh one and
	// stamps it on out. false = nothing fresher to offer, surface the 401.
	reauth(out *http.Request) bool
}

// translatingWires is every wire that can own a hop. Immutable after init and
// stateless (all mutable credential state lives behind its own mutex), so it is
// safe to read from any handler or transport goroutine.
var translatingWires = []wire{codexWire{}}

// refusingWire is a wire with its own refusal adapter (refusal.go,
// rungRefusal), for a provider whose refusals no core adapter reads.
type refusingWire interface {
	refusal(resp *http.Response, model string) refusal
}

// wireFor reports the wire an OUTBOUND request is aimed at, or nil for a plain
// Anthropic-shaped hop. This is the same discriminator the codex lane has used
// since #33 — derived from the request that was actually sent, so it survives
// the class ladder's mid-request rebuilds.
func wireFor(req *http.Request) wire {
	if req == nil {
		return nil
	}
	for _, w := range translatingWires {
		if w.owns(req) {
			return w
		}
	}
	return nil
}

// wireOf is the DISPLAY-safe read of a provider's wire, for the paths that walk
// whatever is in the providers table (status, the startup log). buildHandler
// always sets a wire; a hand-built table needn't, and neither of those paths may
// panic — /_switch/status in particular is what the deploy health gate polls.
// Defaults to no-auth, never to leaking a bearer. The REQUEST path deliberately
// reads p.wire directly: a provider with no wire is a config bug, and failing
// loudly beats silently sending an unauthenticated request to a paid upstream.
func wireOf(p *provider) wire {
	if p == nil || p.wire == nil {
		return localWire{}
	}
	return p.wire
}

// ---- pass-through wires ----

// bearerWire is a hosted provider that speaks Anthropic natively and
// authenticates with a static key (Kimi). The key is the asking tenant's own
// account's, the one the class walk dialled (reqInfo.poolAccount: the
// provider member's pool pick, one of the tenant's accounts of the
// provider), dispensed by its brain's GET /api-key?account=<account>
// (apikey.go) and cached per tenant like an api account's; the proxy holds
// none of its own. It replaces the client's bearer.
type bearerWire struct{}

func (bearerWire) authLabel() string                                       { return "inject" }
func (bearerWire) name() string                                            { return "bearer" }
func (bearerWire) owns(*http.Request) bool                                 { return false }
func (bearerWire) translate(resp *http.Response, _ reqInfo) *http.Response { return resp }
func (bearerWire) reauth(*http.Request) bool                               { return false }

// wireNamesAccount: the provider's wire signs with the key of the account
// its dial picked (bearerWire), so the dial names that account even for a
// member the brain gave no pool view.
func wireNamesAccount(provider string) bool {
	p := providerByName(provider)
	if p == nil {
		return false
	}
	_, ok := p.wire.(bearerWire)
	return ok
}

// prepare fetches the key of the account the dial picked: an account the
// tenant's document does not list is refused before anything is fetched
// (actuatorFor), and a tenant with no key is refused rather than signed with
// anyone else's. A hop the dial named no account for is refused: there is no
// account to sign it as.
func (w bearerWire) prepare(info *reqInfo, body []byte, _ string) string {
	info.body = body
	account := info.poolAccount
	if account == "" {
		return "no account was dialled for this member"
	}
	if _, _, err := info.tenant.actuatorFor(account); err != nil {
		return err.Error()
	}
	key, err := info.tenant.getAPIKey(nil, account)
	if err != nil {
		return "no " + account + " key: " + err.Error()
	}
	info.token = key
	return ""
}

func (w bearerWire) apply(out *http.Request, info reqInfo) {
	// Anthropic convention: the provider's key as a bearer. Never the client's.
	out.Header.Del("X-Api-Key")
	if info.token == "" {
		out.Header.Del("Authorization")
		return
	}
	out.Header.Set("Authorization", "Bearer "+info.token)
}

// localWire is a llama-server (#135): an open runtime with no auth
// at all. The client's bearer has no business there and must never leak.
//
// Its one translation is the over-window refusal. llama-server refuses a prompt
// longer than the slot's context with its own envelope and wording, and Claude
// Code compacts and retries only on Anthropic's: a 400 invalid_request_error
// whose message reads "prompt is too long". translate rewrites that refusal and
// passes everything else through as sent.
type localWire struct{}

func (localWire) authLabel() string         { return "strip" }
func (localWire) name() string              { return "local" }
func (localWire) owns(*http.Request) bool   { return false }
func (localWire) reauth(*http.Request) bool { return false }

func (localWire) prepare(info *reqInfo, body []byte, _ string) string {
	info.body = body
	return ""
}

func (localWire) apply(out *http.Request, _ reqInfo) {
	out.Header.Del("Authorization")
	out.Header.Del("X-Api-Key")
}

// llama-server b12110's two over-window messages (tools/server/server-context.cpp,
// update_slots): the first when the prompt cannot be truncated, the second when
// it can but still does not fit. Both carry type exceed_context_size_error.
var llamaOverWindow = []*regexp.Regexp{
	regexp.MustCompile(`^request \((\d+) tokens\) exceeds the available context size \((\d+) tokens\), try increasing it$`),
	regexp.MustCompile(`^input \((\d+) tokens\) is larger than the max context size \((\d+) tokens\)\. skipping$`),
}

// translate turns llama-server's over-window 400 into Anthropic's own:
//
//	{"error":{"code":400,"message":"request (N tokens) exceeds ...","type":"exceed_context_size_error","n_prompt_tokens":N,"n_ctx":M}}
//	→ {"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: N tokens > M maximum"}}
//
// llama-server sends it as a plain JSON 400 whether or not the client asked to
// stream (the first result of a stream is answered unstreamed when it is an
// error, and the context check runs before any token), and the router passes
// the child's status and body through. Anything else is returned untouched.
func (localWire) translate(resp *http.Response, _ reqInfo) *http.Response {
	if resp == nil || resp.StatusCode != http.StatusBadRequest || resp.Body == nil {
		return resp
	}
	original := resp.Body
	const limit = 64 << 10
	raw, err := io.ReadAll(io.LimitReader(original, limit+1))
	if err != nil {
		resp.Body = &noteReplayBody{Reader: bytes.NewReader(raw), Closer: original, err: err}
		return resp
	}
	if len(raw) > limit {
		resp.Body = hpRestoreBody(raw, original)
		return resp
	}
	n, m, ok := llamaOverWindowCounts(raw)
	if !ok {
		resp.Body = &noteReplayBody{Reader: bytes.NewReader(raw), Closer: original}
		return resp
	}
	_ = original.Close()
	body, _ := json.Marshal(map[string]any{"type": "error", "error": map[string]any{
		"type":    "invalid_request_error",
		"message": fmt.Sprintf("prompt is too long: %d tokens > %d maximum", n, m),
	}})
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	resp.Header.Set("Content-Type", "application/json")
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
	resp.Header.Del("Content-Encoding")
	resp.TransferEncoding = nil
	return resp
}

// llamaOverWindowCounts takes both counts from the two pinned messages only.
func llamaOverWindowCounts(raw []byte) (n, m int, ok bool) {
	var env struct {
		Error struct{ Type, Message string }
	}
	if json.Unmarshal(raw, &env) != nil || env.Error.Type != "exceed_context_size_error" {
		return 0, 0, false
	}
	for _, re := range llamaOverWindow {
		if g := re.FindStringSubmatch(env.Error.Message); g != nil {
			n, en := strconv.Atoi(g[1])
			m, em := strconv.Atoi(g[2])
			return n, m, en == nil && em == nil && n > 0 && m > 0
		}
	}
	return 0, 0, false
}

// hpRestoreBody puts the bytes already read back in front of whatever is left,
// so a bail-out is indistinguishable from never having looked. Close still
// closes the real body.
func hpRestoreBody(raw []byte, rest io.ReadCloser) io.ReadCloser {
	return &hpRestoredBody{r: io.MultiReader(bytes.NewReader(raw), rest), c: rest}
}

type hpRestoredBody struct {
	r io.Reader
	c io.Closer
}

func (b *hpRestoredBody) Read(p []byte) (int, error) { return b.r.Read(p) }
func (b *hpRestoredBody) Close() error               { return b.c.Close() }

// tripWire is a wire whose upstream is not one HTTP exchange: the transport
// hands it the built hop, and it answers with the response the router reads.
type tripWire interface {
	trip(req *http.Request, info reqInfo) (*http.Response, error)
}

// hopBounded is a wire whose hop, for some requests, has a deadline of its
// own besides its request's (0: none). It runs from the hop's send to the
// close of its reply's body, so one slow member of a walk cannot spend the
// time the members after it would draw in.
type hopBounded interface {
	hopDeadline(info reqInfo) time.Duration
}

// cancelOnClose ends a hop's own deadline when its reply's body is closed.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}
