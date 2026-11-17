package main

// Tests for upstreamErrorResponse + the ReverseProxy ErrorHandler it feeds —
// an incident post-mortem.
//
// What went wrong: class helpdesk was out of quota on BOTH of its
// members, the gateway's retry re-bound to a kimi rung that accepted the
// connection and then went silent, and 120 s later the proxy answered a flat
// 502 {"type":"proxy_error","message":"apiary-proxy: upstream failure"}.
// Downstream, the agent gateway's failover classifier is a status table that buckets
// every 5xx except 529 as "timeout", so the only thing the operator ever saw was
// that one word — no member, no elapsed, no hint that quota was the real cause.
//
// These pin the two things that were missing ON THE WIRE, not in a log: the
// STATUS separates a timeout (504) from a dead connection (502) from a client
// hang-up (499), and the MESSAGE names the member and how long we waited.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeTimeoutErr stands in for what a transport hands back on
// ResponseHeaderTimeout: a net.Error whose Timeout() is true. The real one is
// x/net/http2's unexported errTimeout behind the string "http2: timeout
// awaiting response headers" — exactly what the proxy logged then — so the
// interface, not the text, is what the classification may key on.
type fakeTimeoutErr struct{ msg string }

func (e fakeTimeoutErr) Error() string   { return e.msg }
func (e fakeTimeoutErr) Timeout() bool   { return true }
func (e fakeTimeoutErr) Temporary() bool { return true }

func TestUpstreamErrorResponseClassification(t *testing.T) {
	var _ net.Error = fakeTimeoutErr{} // the arm under test keys on this interface

	// A request carrying what a real class hop would: the member to name, and a
	// start stamp 90 s in the past to prove the elapsed is reported, not invented.
	withCtx := func() *http.Request {
		r := httptest.NewRequest("POST", "/v1/messages", nil)
		ctx := context.WithValue(r.Context(), reqStartKey, time.Now().Add(-90*time.Second))
		ctx = context.WithValue(ctx, infoKey, reqInfo{
			classCtx: &classRoute{tenant: lan, class: "helpdesk", member: classMemberPref{ID: "kimi/kimi-k3"}},
		})
		return r.WithContext(ctx)
	}

	for _, tc := range []struct {
		name       string
		err        error
		wantStatus int
		wantType   string
		wantInMsg  []string
	}{{
		// The headline regression: this used to be a 502, which the agent gateway reads
		// as a generic server error and reports as the bare word "timeout".
		name:       "an h2 header timeout is a gateway TIMEOUT, not a bad gateway",
		err:        fakeTimeoutErr{msg: "http2: timeout awaiting response headers"},
		wantStatus: http.StatusGatewayTimeout,
		wantType:   "timeout_error",
		wantInMsg:  []string{"upstream timeout", "kimi/kimi-k3", "1m30s"},
	}, {
		name:       "a proxy-owned deadline lands in the same arm",
		err:        context.DeadlineExceeded,
		wantStatus: http.StatusGatewayTimeout,
		wantType:   "timeout_error",
		wantInMsg:  []string{"kimi/kimi-k3", "1m30s"},
	}, {
		name:       "a dead connection stays a 502, but says which member died",
		err:        errors.New("connection reset by peer"),
		wantStatus: http.StatusBadGateway,
		wantType:   "upstream_error",
		wantInMsg:  []string{"kimi/kimi-k3", "connection reset by peer", "1m30s"},
	}, {
		// classify.go's Rule 0: Canceled can only come from outside our own
		// deadlines. Billing the client's hang-up to the upstream is the kind of
		// lie that costs an hour on the next incident.
		name:       "a client hang-up is not blamed on the upstream",
		err:        context.Canceled,
		wantStatus: 499,
		wantType:   "cancelled",
		wantInMsg:  []string{"client cancelled"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			status, errType, msg := upstreamErrorResponse(withCtx(), tc.err)
			if status != tc.wantStatus {
				t.Errorf("status = %d, want %d (msg %q)", status, tc.wantStatus, msg)
			}
			if errType != tc.wantType {
				t.Errorf("type = %q, want %q", errType, tc.wantType)
			}
			for _, want := range tc.wantInMsg {
				if !strings.Contains(msg, want) {
					t.Errorf("message %q missing %q", msg, want)
				}
			}
		})
	}
}

// A hop with neither a class member nor an account still has to answer, must
// not panic reaching for a name, and must not invent an elapsed it never measured.
func TestUpstreamErrorResponseWithoutContext(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/messages", nil)
	status, errType, msg := upstreamErrorResponse(r, errors.New("dial tcp: refused"))
	if status != http.StatusBadGateway || errType != "upstream_error" {
		t.Fatalf("got %d/%s, want 502/upstream_error", status, errType)
	}
	if !strings.Contains(msg, "upstream") || !strings.Contains(msg, "dial tcp: refused") {
		t.Fatalf("message %q should name the generic upstream and the cause", msg)
	}
	if strings.Contains(msg, "after") {
		t.Fatalf("message %q claims an elapsed it cannot know", msg)
	}
}

// The account-lane path has no class member; .eff is the name worth printing.
func TestUpstreamErrorResponseNamesAccountLane(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/messages", nil)
	r = r.WithContext(context.WithValue(r.Context(), infoKey,
		reqInfo{eff: "acct-a@example.com", lane: "opus"}))
	_, _, msg := upstreamErrorResponse(r, fakeTimeoutErr{msg: "timeout awaiting response headers"})
	if !strings.Contains(msg, "acct-a@example.com") {
		t.Fatalf("message %q should name the effective account", msg)
	}
}

// End to end through the REAL handler stack: a class member that accepts the
// request and then never sends headers must surface as a 504 naming that member,
// not as the anonymous 502 that made the failure unreadable.
func TestUpstreamStallReturns504(t *testing.T) {
	// Shrink the header timeout BEFORE the harness builds the proxy — the
	// transport reads it once, at construction. A two-minute silence is not
	// something a test can afford to wait out.
	oldTimeout := upstreamHeaderTimeout
	upstreamHeaderTimeout = 400 * time.Millisecond
	defer func() { upstreamHeaderTimeout = oldTimeout }()

	h, restore := newE2EClass(t)
	defer restore()
	h.bindTo.Store("kimi/kimi-k3")
	h.stall.Store(true)

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-stall")
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504; body: %s", resp.StatusCode, raw)
	}
	var env struct {
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("body is not an error envelope (%v): %s", err, raw)
	}
	if env.Error.Type != "timeout_error" {
		t.Errorf("error type = %q, want timeout_error; body: %s", env.Error.Type, raw)
	}
	// The whole point: the member is on the WIRE, not just in our log.
	if !strings.Contains(env.Error.Message, "kimi/kimi-k3") {
		t.Errorf("message %q does not name the stalled member", env.Error.Message)
	}
	if !strings.Contains(env.Error.Message, "after") {
		t.Errorf("message %q does not report how long we waited", env.Error.Message)
	}
}

// The h2 liveness knobs are the actual FIX for the stall; the error shape above
// only makes it legible. Without a health check a silently-dead pooled
// connection swallows the next request for the full header timeout — every 502
// of that incident was that shape, at
// elapsed 120.0xx s on a reused conn.
func TestTransportHasHTTP2Liveness(t *testing.T) {
	tr := newUpstreamTransport()
	if !tr.ForceAttemptHTTP2 {
		t.Fatal("ForceAttemptHTTP2 is off, so HTTP2Config would never apply")
	}
	if tr.HTTP2 == nil {
		t.Fatal("no HTTP2Config: a silently-dead h2 conn is undetectable and will eat a full ResponseHeaderTimeout")
	}
	if tr.HTTP2.SendPingTimeout <= 0 {
		t.Error("SendPingTimeout must be > 0 — zero means no health check at all")
	}
	if tr.HTTP2.PingTimeout <= 0 {
		t.Error("PingTimeout must be > 0 — zero means the ping answer is never waited on")
	}
	// A ping that can only fire after the timeout it exists to pre-empt is
	// decoration. Keep the ordering, not the specific numbers.
	if slack := tr.ResponseHeaderTimeout - (tr.HTTP2.SendPingTimeout + tr.HTTP2.PingTimeout); slack <= 0 {
		t.Errorf("ping budget %s+%s does not fit inside ResponseHeaderTimeout %s",
			tr.HTTP2.SendPingTimeout, tr.HTTP2.PingTimeout, tr.ResponseHeaderTimeout)
	}
	if tr.ResponseHeaderTimeout != upstreamHeaderTimeout {
		t.Errorf("ResponseHeaderTimeout = %s, want the tunable upstreamHeaderTimeout (%s)",
			tr.ResponseHeaderTimeout, upstreamHeaderTimeout)
	}
}
