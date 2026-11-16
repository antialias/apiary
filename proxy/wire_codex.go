package main

import (
	"bytes"
	"io"
	"net/http"
)

// codexWire is the gpt lane (#33): the ChatGPT-subscription "codex" backend.
// It speaks the Responses shape, is stream-only, and authenticates with a
// bearer the brain dispenses (the proxy never refreshes the token chain).
// codex.go owns both translation directions and the bearer cache; this type is
// the entire surface the core sees.
type codexWire struct{}

func (codexWire) authLabel() string { return "codex-bearer" }
func (codexWire) name() string      { return "codex" }

// owns: the backend path is the discriminator. Only codex hops ever carry it,
// and reading it off the request that was actually sent is what makes it
// survive the class ladder's mid-request rebuilds.
func (codexWire) owns(req *http.Request) bool {
	return req != nil && req.URL.Path == codexResponsesPath
}

func (codexWire) prepare(info *reqInfo, body []byte, inPath string) string {
	if inPath != "/v1/messages" {
		return "the codex gpt lane serves /v1/messages only (got " + inPath + ")"
	}
	cbody, cs, _, err := translateAnthropicToCodex(body)
	if err != nil {
		return "codex translation failed: " + err.Error()
	}
	// info.poolAccount is the caller's pool-account choice (#71): the class
	// actuation sets it from the member's pool view; the direct path leaves it
	// "" (incumbent-follow — the brain resolves its pool's current pick). The
	// brain's resolved name is written back so the served hop is labelled with
	// the account that ACTUALLY answered, not the one merely asked for.
	tok, acct, name, ok := getCodexToken(info.poolAccount)
	if !ok {
		return "no codex bearer (brain /openai-token unreachable and no cached bearer)"
	}
	info.body = cbody
	info.token, info.account = tok, acct
	if name != "" {
		info.poolAccount = name
	}
	info.clientStream = cs
	return ""
}

// apply: fixed backend path, a complete fresh header set (never inherit the
// client's Anthropic headers), and a replayable body for the transport's 401
// retry. The body was translated in prepare and installed by the caller.
//
// The session is read off out.Header before it is replaced: both callers hand
// apply the inbound header set (Rewrite's clone of the client request, and the
// class ladder's rebuild from origHeader), so the client's session id is still
// there at this point and nowhere after it.
func (codexWire) apply(out *http.Request, info reqInfo) {
	out.URL.Path, out.URL.RawPath = codexResponsesPath, ""
	sess := codexSession(out.Header.Get("x-claude-code-session-id"), info.body)
	out.Header = codexHeaders(info.token, info.account, sess)
	cb := info.body
	out.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(cb)), nil
	}
}

// info.eff is the member label for the SERVED hop: the direct route sets
// "<provider>:<upstream-model>", both class paths set the member id.
//
// The wire != nil guard is a SAFETY interlock, not a formality. eff is
// overloaded — on an Anthropic hop it holds the ACCOUNT, which is a real email
// address. If the served hop's info ever fails to reach us (the ladder clones
// attempt 1's context, so the previous member's info is what's there by
// default), an unguarded eff would put that address in a client-visible error
// envelope. Only a provider hop carries a wire, so gating on it means the worst
// case is an unlabelled error instead of a leaked account.
func (w codexWire) translate(resp *http.Response, info reqInfo) *http.Response {
	member := ""
	if info.wire != nil {
		member = info.eff
	}
	// The stream producer's hopScore (#122): armed only for class routes with
	// health on. key = the SERVED member's endpoint key (snapshot, not eff —
	// eff is overloaded on anthropic hops).
	var hs *hopScore
	if health != nil && info.classCtx != nil {
		cc := info.classCtx
		skey := cc.served.snapshot().member
		hs = &hopScore{ctx: resp.Request.Context(), key: skey, cc: cc, lease: cc.leaseFor(skey)}
	}
	// How the served 200's events arrive rides the decision to the log line.
	var up *upstreamTrace
	if resp.StatusCode == http.StatusOK && resp.Request != nil {
		if d := decisionFrom(resp.Request.Context()); d != nil {
			up = newUpstreamTrace(resp.Request.Context(), hopHeadAt(resp.Request.Context())) // a budgeted hop held its head (first_event.go)
			d.upstream(up)
		}
	}
	// A 200 a paid check sent to a counted pot is entered in the ledger when
	// its response ends (ledger.go). A refusal bills nothing.
	var led *ledgerHop
	if resp.StatusCode == http.StatusOK && resp.Request != nil {
		model := ""
		if info.classCtx != nil {
			model = info.classCtx.served.snapshot().model
		}
		led = armLedger(resp.Request.Context(), servedCheck(resp.Request, info), model)
	}
	// The served event's tokens (served.go): the upstream's own usage, which
	// the translator reads before it scales the input and drops reasoning.
	var srv *servedCap
	if resp.StatusCode == http.StatusOK && resp.Request != nil {
		if c := servedCapOf(resp.Request.Context()); c.armedAs(servedCodex) {
			srv = c
		}
	}
	return translateCodexResponse(resp, w.clientStream(info), member, info.ratio, info.scale, hs, up, led, srv)
}

// clientStream resolves the CLIENT's stream wish for a codex-served request:
// class routes read it off the original Anthropic body (a mid-request cross can
// land on codex even when attempt 1 was anthropic, so prepare never ran for
// this hop); direct routes carried it on reqInfo from translation time.
func (codexWire) clientStream(info reqInfo) bool {
	if info.classCtx != nil {
		return wantsStream(info.classCtx.origBody)
	}
	return info.clientStream
}

// reauth: a codex 401 means the token chain rotated or was revoked under us and
// the brain may already hold the successor. Dropping the cache before anything
// else is deliberate — even a bail-out should force the next request to refetch.
// The pool account rides the request context (every provider hop stamps
// infoKey), so the retry re-acquires the SAME pool member's chain — a 401 on
// gpt-b must never be "fixed" by silently swapping to the incumbent's bearer.
func (codexWire) reauth(out *http.Request) bool {
	account := ""
	if info, ok := out.Context().Value(infoKey).(reqInfo); ok {
		account = info.poolAccount
	}
	dropCodexToken(account)
	if account != "" {
		// The incumbent-follow slot ("") may alias this same chain (the direct
		// path fetches under "" and only then learns the name) — drop it too, or
		// it keeps serving the revoked bearer for up to codexTokenFresh.
		dropCodexToken("")
	}
	tok, acct, _, ok := getCodexToken(account)
	if !ok {
		return false
	}
	out.Header.Set("Authorization", "Bearer "+tok)
	out.Header.Set("ChatGPT-Account-Id", acct)
	return true
}
