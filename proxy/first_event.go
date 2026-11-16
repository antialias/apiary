package main

// First-event budget (SWITCH_FIRST_EVENT_S): on the classes it names, a codex
// hop that has not produced its first upstream byte within the budget is
// abandoned and sent ONCE more to the same member. A 30 s non-streaming
// client with no retry once saw four of its requests
// stall (no head, or a head and then nothing) between successes on the same
// member, and a request sent the instant one was abandoned answered normally.
// Nothing reaches the client before the first event (the aggregate reply is
// built after RoundTrip, a stream's message_start waits for the first event),
// so the resend is invisible to it. The abandoned hop gets one log line saying
// how it failed, because which failure it was is the open question.

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptrace"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// errFirstEventBudget is the hop context's cancel cause when the budget fires;
// classify reads it as row 22.
var errFirstEventBudget = errors.New("no upstream byte within the first-event budget")

// errResendRefused: the stalled hop's account may not be dialled again — its
// check is `no` on the newest overlay (§2.2 row 18). The class ladder walks.
var errResendRefused = errors.New("first-event resend refused by its dial check")

// firstEventBudget is class -> budget; startup-only. Empty (the default) is off.
var firstEventBudget map[string]time.Duration

// parseFirstEventBudget reads "class:seconds[,class:seconds…]". A malformed or
// non-positive entry is skipped with a warning, never guessed at.
func parseFirstEventBudget(v string) map[string]time.Duration {
	m := map[string]time.Duration{}
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		class, secs, ok := strings.Cut(part, ":")
		s, err := strconv.ParseFloat(strings.TrimSpace(secs), 64)
		if !ok || err != nil || s <= 0 || strings.TrimSpace(class) == "" {
			log.Printf("⚠️  SWITCH_FIRST_EVENT_S entry %q not understood (class:seconds) — ignored", part)
			continue
		}
		m[strings.TrimSpace(class)] = time.Duration(s * float64(time.Second))
	}
	return m
}

// hopConn is what httptrace's GotConn said about the hop's connection — the
// observed 502s were all dead REUSED h2 connections, so a stall line
// has to say whether this one rode a reused or idle one.
type hopConn struct {
	mu   sync.Mutex
	got  bool
	info httptrace.GotConnInfo
}

func (c *hopConn) trace() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{GotConn: func(i httptrace.GotConnInfo) {
		c.mu.Lock()
		c.got, c.info = true, i
		c.mu.Unlock()
	}}
}

// tag renders the connection for the stall line: conn=none when the hop never
// got one (the dial or the TLS handshake is where it hung).
func (c *hopConn) tag() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.got || c.info.Conn == nil {
		return "conn=none"
	}
	proto := "http/1.1"
	if tc, ok := c.info.Conn.(*tls.Conn); ok && tc.ConnectionState().NegotiatedProtocol != "" {
		proto = tc.ConnectionState().NegotiatedProtocol
	}
	return fmt.Sprintf("reused=%t was_idle=%t idle=%s proto=%s remote=%s",
		c.info.Reused, c.info.WasIdle, c.info.IdleTime.Round(time.Millisecond), proto, addrOf(c.info.Conn))
}

func addrOf(c net.Conn) string {
	if a := c.RemoteAddr(); a != nil {
		return a.String()
	}
	return ""
}

// upstreamIDs lists the head's request-id headers (and cf-ray), so the stall
// can be quoted to the upstream; "" when there are none.
func upstreamIDs(h http.Header) string {
	var ids []string
	for k, v := range h {
		lk := strings.ToLower(k)
		if len(v) > 0 && (strings.HasSuffix(lk, "request-id") || lk == "cf-ray") {
			ids = append(ids, lk+"="+v[0])
		}
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}

// hopHeadKey carries the instant a budgeted hop's head arrived. The hop holds
// its head until the first byte, so translate's own clock would start late and
// read the wait as head time; it starts the upstream trace from this instead.
type hopHeadKeyT struct{}

var hopHeadKey hopHeadKeyT

// hopHeadAt is when the hop's head arrived: the recorded instant on a budgeted
// hop, now on every other.
func hopHeadAt(ctx context.Context) time.Time {
	if p, ok := ctx.Value(hopHeadKey).(*time.Time); ok && !p.IsZero() {
		return *p
	}
	return time.Now()
}

// tripFirstEvent is tripWithReauth under the class's first-event budget. With
// no budget (the default) it IS tripWithReauth. Otherwise the hop rides a
// cancellable child context; a 200 head is held until its first body byte
// (bufio Peek, re-wrapped so the translator reads that byte again), and a hop
// that has neither when the budget fires is abandoned: one stall line, one
// row-22 sample, and one fresh send of the same request to the same member on
// the parent context, with no budget — never a third — while that account's
// check still admits it (else errResendRefused, and the ladder walks). A non-200 head stops
// the clock and takes its normal path.
func (t *failoverTransport) tripFirstEvent(req *http.Request, info reqInfo) (*http.Response, error) {
	cc := info.classCtx
	budget := firstEventBudget[cc.class]
	if budget <= 0 || memberWireName(cc.member) != "codex" || req.GetBody == nil {
		return t.tripWithReauth(req)
	}
	ctx, cancel := context.WithCancelCause(req.Context())
	timer := time.AfterFunc(budget, func() { cancel(errFirstEventBudget) })
	var conn hopConn
	var headTime time.Time
	ctx = context.WithValue(httptrace.WithClientTrace(ctx, conn.trace()), hopHeadKey, &headTime)
	start := time.Now()
	resp, err := t.tripWithReauth(req.Clone(ctx))
	headTime = time.Now()
	buffered := 0
	if err == nil && resp.StatusCode == http.StatusOK {
		br := bufio.NewReader(resp.Body)
		_, _ = br.Peek(1) // a read error stays in br for the translator to meet
		buffered = br.Buffered()
		resp.Body = struct {
			io.Reader
			io.Closer
		}{br, resp.Body}
	}
	if !timer.Stop() {
		<-ctx.Done() // the budget fired: its cancel may still be on its way
	}
	if context.Cause(ctx) != errFirstEventBudget {
		return resp, err // in budget, or the client left first (Rule 0, as today)
	}
	// shape is what the hop had when the budget fired. Only the first two are
	// the upstream's stall; the other two lost a race with the timer by a hair
	// and are resent (their body is cancelled) but not scored.
	shape, head := "no_head", "head=none status=none ids=none"
	if err == nil && resp != nil {
		switch {
		case resp.StatusCode != http.StatusOK:
			shape = "head_status_at_budget"
		case buffered > 0:
			shape = "first_byte_at_budget"
		default:
			shape = "head_no_event"
		}
		ids := upstreamIDs(resp.Header)
		if ids == "" {
			ids = "none"
		}
		head = fmt.Sprintf("head=%s status=%d ids=%s", headTime.Sub(start).Round(time.Millisecond), resp.StatusCode, ids)
		resp.Body.Close()
	}
	log.Printf("⏱️  first-event stall on class %s member %s d=%s shape=%s %s bytes=%d elapsed=%s budget=%s %s — resending once to the same member",
		cc.class, cc.member.ID, cc.reqID, shape, head, buffered, time.Since(start).Round(time.Millisecond), budget, conn.tag())
	if shape == "no_head" || shape == "head_no_event" {
		out := classify(cc.member, ctx, nil, context.Cause(ctx))
		out.lease = cc.leaseFor(cc.member.ID)
		recordHopHead(cc, cc.member, out)
	}
	// §2.2 row 18: the resend is a dial, re-checked on the newest overlay; a
	// `no` hands the request to the walk instead of resending.
	sv := cc.served.snapshot() // same member, same account: only attempts moves
	// §1.7: the abandoned hop was sent, may be billed, and returned no usage.
	// On a counted pot it is entered uncounted.
	if info, ok := req.Context().Value(infoKey).(reqInfo); ok {
		armLedger(req.Context(), servedCheck(req, info), sv.model).abandon("abandoned by the first-event resend (" + shape + ")")
	}
	want := ""
	if len(cc.member.Targets) > 0 {
		want = sv.account
	}
	_, chk, ok := providerMemberDial(cc.member, want, false, false, nowFn())
	if !ok {
		log.Printf("⏱️  first-event resend on class %s member %s account %s refused by its check — walking",
			cc.class, cc.member.ID, sv.account)
		return nil, errResendRefused
	}
	log.Printf("⏱️  first-event resend on class %s member %s account %s check=%s", cc.class, cc.member.ID, sv.account, chk)
	cc.walked = "retry"
	body, berr := req.GetBody()
	if berr != nil {
		return nil, berr
	}
	// The resend is admitted by its own check: that is the check its 2xx
	// speaks for (noteServedHop) and the one its ledger entry draws on.
	resend := req.Clone(context.WithValue(req.Context(), servedCheckKey{}, chk))
	resend.Body = body
	cc.served.selectHop(cc.member, sv.account, sv.lane, sv.translated, sv.ratio)
	return t.tripWithReauth(resend)
}

// budgetTag renders the armed budgets for the startup line, "" when off.
func budgetTag(m map[string]time.Duration) string {
	parts := make([]string, 0, len(m))
	for c, d := range m {
		parts = append(parts, c+"="+d.String())
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}
