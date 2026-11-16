package main

// Entry identity (#126 C0).
//
// Requests reach the routing chain through more than one front door now: the
// base-URL listener (:8787, behind clientGate) and — when it is switched on —
// the HTTPS-proxy entry point (:8788, behind a per-machine proxy login). The
// routing chain itself must not care which, so the only thing that travels is
// this value, on the request context.
//
// The contract that makes the refactor safe: base-URL NEVER sets it, so
// entryFrom returns the zero value and every rendered field stays empty or
// omitted — the log line, the tap turn and the route_decision are byte-
// identical to what :8787 produced before this file existed.

import (
	"context"
	"net/http"
)

// entryHook is a second front door beside :8787, an edition's (core has
// none): it reads its env in main, sets up on every buildHandlers, starts
// with the core routing chain to dispatch into, renders its own metrics
// families, and owns the signal's whole shutdown, :8787's drain included.
// Any field may be nil.
type entryHook struct {
	fromEnv  func()
	build    func()
	start    func(core http.Handler)
	metrics  func() string
	shutdown func(srv *http.Server)
}

// entryHooks is every second front door, appended by an edition's init().
var entryHooks []entryHook

// entryHTTPSProxy is the kind for the decrypted HTTPS-proxy origin.
const entryHTTPSProxy = "https_proxy"

// entryInfo names the front door a request came through and, where the door
// authenticates per machine, which machine. Both fields are display/attribution
// only: nothing in routing, classification or quota reads them.
type entryInfo struct {
	kind   string // "" = base-URL (the default); entryHTTPSProxy
	client string // proxy login name, "" when the door has no per-machine identity
	// #128 P2: set only when the login was a Scry device token. device is the
	// token's `sub` ("dev_…"), user its `usr` ("usr_…") — Scry's opaque id for
	// the approving person, NEVER an email. Both empty for a users-file login,
	// which keeps every rendered field omitted on that path.
	device string
	user   string
	// anon marks a CONNECT that carried NO Proxy-Authorization at all and was
	// accepted anyway, under SWITCH_HTTPS_PROXY_ANON_PASSTHROUGH and only for
	// api.anthropic.com:443. It has no client, no device and no user: it may
	// talk to Anthropic with its own login and nothing else (the dispatcher
	// refuses the routed paths and whoami). A credential that was PRESENT and
	// failed never produces this — it 407s — with ONE exception: a device
	// token of ours that has only expired (expired below).
	anon bool
	// expired is set on an anonymous connection whose CONNECT carried one of
	// our device tokens that had ONLY expired (hpEntry.expiredAsAnon): the
	// device's signed name, for logs and the routed-path refusal. It grants
	// nothing anon does not; device and user stay empty, so the connection is
	// never registered against the device.
	expired string
	// unattributed marks a decrypted connection the entry cannot account for:
	// the internal server accepted it but no identity was recorded against it.
	// Unreachable by construction (decrypt() stores the identity before it
	// pushes), and it FAILS CLOSED anyway — the dispatcher answers 403 to every
	// request on such a connection rather than routing or forwarding one whose
	// login nobody can name.
	unattributed bool
}

// hpAnonClient is the client label for an anonymous connection, in logs and
// in the connect metric. Parenthesised so it cannot collide with a
// users-file name (hpNameRe permits no brackets).
const hpAnonClient = "(anonymous)"

type entryCtxKey struct{}

// withEntry attaches e to ctx. Callers: the HTTPS-proxy dispatcher only.
func withEntry(ctx context.Context, e entryInfo) context.Context {
	return context.WithValue(ctx, entryCtxKey{}, e)
}

// entryFrom reads the entry identity. The zero value means base-URL.
func entryFrom(ctx context.Context) entryInfo {
	if ctx == nil {
		return entryInfo{}
	}
	e, _ := ctx.Value(entryCtxKey{}).(entryInfo)
	return e
}

// logName is what a log line or a metric label calls this identity: the login
// name, or hpAnonClient for an anonymous connection. Never empty for an
// anonymous one — `client=` with nothing after it would read as a bug.
func (e entryInfo) logName() string {
	if e.anon {
		if e.expired != "" {
			// Parenthesised like hpAnonClient, so it cannot collide with a
			// users-file name (hpNameRe permits no brackets).
			return "(expired:" + e.expired + ")"
		}
		return hpAnonClient
	}
	return e.client
}

// logTag renders the " via=hp:<client>" suffix for requestHandler's log line.
// Empty for base-URL, which is what keeps that line unchanged.
func (e entryInfo) logTag() string {
	if e.kind == "" {
		return ""
	}
	short := e.kind
	if e.kind == entryHTTPSProxy {
		short = "hp"
	}
	if e.client == "" {
		return " via=" + short
	}
	return " via=" + short + ":" + e.client
}
