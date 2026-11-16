package main

// #122 evidence-producer suite: the §3 row tables (classify, classifyCodexExit,
// healthTap), the producer glue (hopScore.settle → healthRecord → store), E15
// (codex cancel re-arms the probe token) and E27's producer-path variant (a
// double settle through hopScore is inert). The e2e observe test lives with
// the E0 harness in e2e_health_observe_test.go.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ---- fixtures ----

// withTestProviders installs a static gpt (codex wire) and kimi (bearer wire)
// so memberWireName resolves without a full buildHandler.
func withTestProviders(t *testing.T) {
	t.Helper()
	old := providers
	providers = append([]*provider{
		{name: "gpt", wire: codexWire{}},
		{name: "kimi", wire: bearerWire{}},
	}, old...)
	t.Cleanup(func() { providers = old })
}

// armHealth points the global producer sink at a fresh store; producers are
// nil-gated, so this is the test's mode switch.
func armHealth(t *testing.T, h *healthStore) {
	t.Helper()
	old := health
	health = h
	t.Cleanup(func() { health = old })
}

var (
	memAnth = classMemberPref{ID: "lane:fable", Kind: "anthropic", Lane: "fable", Model: "claude-fable-5"}
	memGpt  = classMemberPref{ID: "gpt/gpt-5.6", Kind: "provider", Provider: "gpt", Model: "gpt-5.6"}
	memKimi = classMemberPref{ID: "kimi/kimi-k3", Kind: "provider", Provider: "kimi", Model: "kimi-k3"}
)

func hResp(status int, hdr map[string]string, body string) *http.Response {
	h := http.Header{}
	for k, v := range hdr {
		h.Set(k, v)
	}
	return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(body))}
}

func canceledCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

const quota403Body = `{"type":"error","error":{"type":"permission_error","message":"usage limit reached for this billing cycle"}}`
const shape429Body = `{"type":"error","error":{"type":"rate_limit_error","message":"Error"}}`

// ---- the head-site row table ----

func TestClassifyHeadRowTable(t *testing.T) {
	withTestProviders(t)
	bg := context.Background()
	cases := []struct {
		name     string
		m        classMemberPref
		ctx      context.Context
		resp     *http.Response
		err      error
		row, sig string
		cause    string
		ladder   string
		ev       string
		final    bool
	}{
		{"row1 err canceled", memAnth, bg, nil, context.Canceled, "1", sigCancelled, "", ladderStop, evNone, true},
		{"row1 ctx canceled other err", memAnth, canceledCtx(), nil, errors.New("broken pipe"), "1", sigCancelled, "", ladderStop, evNone, true},
		{"row18 proxy deadline", memGpt, bg, nil, context.DeadlineExceeded, "18", sigDegraded, "deadline", ladderReturn, evDeg, true},
		{"row17 transport err", memKimi, bg, nil, io.ErrUnexpectedEOF, "17", sigDegraded, "transport", ladderDetour, evDeg, true},
		{"row17 nil resp defensive", memAnth, bg, nil, nil, "17", sigDegraded, "transport", ladderDetour, evDeg, true},
		{"row5 head half not final", memAnth, bg, hResp(200, nil, ""), nil, "5", sigHealthy, "", "", "", false},
		{"row6 codex 429", memGpt, bg, hResp(429, nil, `{}`), nil, "6", sigQuota, "", ladderWalkQuota, evNone, true},
		// C7: a codex 403 is auth whatever its words say (the wire re-auths it).
		{"row9 codex 403 with quota words", memGpt, bg, hResp(403, nil, quota403Body), nil, "9", sigAuth, "", ladderStop, evNone, true},
		{"row10 kimi quota 403", memKimi, bg, hResp(403, nil, quota403Body), nil, "10", sigQuota, "", ladderWalkQuota, evNone, true},
		{"row13 anthropic capacity 429", memAnth, bg, hResp(429, map[string]string{"Anthropic-Ratelimit-Unified-Status": "allowed_warning"}, `{}`), nil, "13", sigQuota, "", ladderWalkQuota, evNone, true},
		{"row14 shape-rejected 429 still walks", memAnth, bg, hResp(429, nil, shape429Body), nil, "14", sigClient, "", ladderWalkQuota, evNone, true},
		{"row12 kimi 429", memKimi, bg, hResp(429, nil, `{}`), nil, "12", sigQuota, "", ladderWalkQuota, evNone, true},
		// C7: the classes walk, whatever the status (§2.6).
		{"row10 kimi not entitled 403", memKimi, bg, hResp(403, nil, kimiNotEntitled403), nil, "10", sigQuota, "", ladderWalkQuota, evNone, true},
		{"row13 anthropic 400 credit balance too low", memAnth, bg, hResp(400, nil, anthCreditTooLow400), nil, "13", sigQuota, "", ladderWalkQuota, evNone, true},
		{"row13 anthropic 402 billing", memAnth, bg, hResp(402, nil, anthBilling402), nil, "13", sigQuota, "", ladderWalkQuota, evNone, true},
		{"row6 codex billing code", memGpt, bg, hResp(429, nil, codexCreditExhausted429), nil, "6", sigQuota, "", ladderWalkQuota, evNone, true},
		{"row20 anthropic invalid 400", memAnth, bg, hResp(400, nil, anthInvalid400), nil, "20", sigUnknown, "", ladderReturn, evNone, true},
		{"row20 kimi invalid 400", memKimi, bg, hResp(400, nil, `{"error":{"type":"invalid_request_error","message":"bad"}}`), nil, "20", sigUnknown, "", ladderReturn, evNone, true},
		{"row9 codex 401", memGpt, bg, hResp(401, nil, ""), nil, "9", sigAuth, "", ladderStop, evNone, true},
		{"row9 codex non-quota 403", memGpt, bg, hResp(403, nil, `{"error":"forbidden"}`), nil, "9", sigAuth, "", ladderStop, evNone, true},
		{"row7 codex 502", memGpt, bg, hResp(502, nil, ""), nil, "7", sigDegraded, "http_5xx", ladderDetour, evDeg, true},
		{"row8 codex 400", memGpt, bg, hResp(400, nil, ""), nil, "8", sigClient, "", ladderStop, evNone, true},
		{"row11 kimi 500", memKimi, bg, hResp(500, nil, ""), nil, "11", sigDegraded, "http_5xx", ladderDetour, evDeg, true},
		{"row15 anthropic 529", memAnth, bg, hResp(529, nil, ""), nil, "15", sigDegraded, "upstream_529", ladderDetour, evDeg, true},
		{"row16 anthropic 500", memAnth, bg, hResp(500, nil, ""), nil, "16", sigDegraded, "http_5xx", ladderDetour, evDeg, true},
		{"row20 anthropic 404 residual", memAnth, bg, hResp(404, nil, ""), nil, "20", sigUnknown, "", ladderReturn, evNone, true},
		{"row20 kimi non-quota 403 residual", memKimi, bg, hResp(403, nil, `{"error":"forbidden"}`), nil, "20", sigUnknown, "", ladderReturn, evNone, true},
	}
	walkRows := map[string]bool{"6": true, "10": true, "12": true, "13": true, "14": true}
	seen := map[string]bool{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := classify(tc.m, tc.ctx, tc.resp, tc.err)
			if out.row != tc.row || out.sig != tc.sig || out.cause != tc.cause ||
				out.ladder != tc.ladder || out.evidence != tc.ev || out.final != tc.final {
				t.Fatalf("got (row=%s sig=%s cause=%q ladder=%s ev=%s final=%v), want (%s %s %q %s %s %v)",
					out.row, out.sig, out.cause, out.ladder, out.evidence, out.final,
					tc.row, tc.sig, tc.cause, tc.ladder, tc.ev, tc.final)
			}
			if out.phase != "head" {
				t.Fatalf("phase = %q, want head", out.phase)
			}
			if out.key != tc.m.ID {
				t.Fatalf("key = %q, want %q", out.key, tc.m.ID)
			}
			// walk_quota ⟺ the adapter's class walks (§2.6, C7), and only
			// on the walk rows.
			if (out.ladder == ladderWalkQuota) != walkRows[out.row] ||
				(out.ladder == ladderWalkQuota) != out.refusal.walks() {
				t.Fatalf("row %s: ladder %s, class %q violates the walk_quota ⟺ walks invariant",
					out.row, out.ladder, out.refusal.class)
			}
		})
		seen[tc.row] = true
	}
	// Completeness: every head-site row of §3's table is exercised.
	for _, r := range []string{"1", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "20"} {
		if !seen[r] {
			t.Fatalf("head row %s has no test case", r)
		}
	}
}

// ---- the codex exit-site table ----

func TestClassifyCodexExitTable(t *testing.T) {
	bg := context.Background()
	cases := []struct {
		name      string
		a         anthStream
		err       error
		ctx       context.Context
		row, sig  string
		cause, ev string
		ladder    string
	}{
		{"row1 ctx canceled", anthStream{}, nil, canceledCtx(), "1", sigCancelled, "", evNone, ladderStop},
		{"row1 err canceled", anthStream{}, context.Canceled, bg, "1", sigCancelled, "", evNone, ladderStop},
		{"row1 pipe read half closed", anthStream{writeErr: io.ErrClosedPipe}, nil, bg, "1", sigCancelled, "", evNone, ladderStop},
		{"writeErr after ended is not a cancel", anthStream{writeErr: io.ErrClosedPipe, ended: true}, nil, bg, "5", sigHealthy, "", evOK, ""},
		{"row2 response.failed", anthStream{failed: true, ended: true}, nil, bg, "2", sigDegraded, "upstream_failed", evDeg, ladderReturn},
		{"row21 incomplete refusal", anthStream{refused: true, ended: true, started: true}, nil, bg, "21", sigClient, "upstream_incomplete", evNone, ladderReturn},
		{"row5 completed", anthStream{ended: true}, nil, bg, "5", sigHealthy, "", evOK, ""},
		{"row4b truncated after content", anthStream{blocksOpened: 2}, io.ErrUnexpectedEOF, bg, "4b", sigUnknown, "stream_truncated_late", evNone, ladderReturn},
		{"row4a truncated before content", anthStream{started: true}, nil, bg, "4a", sigDegraded, "stream_truncated", evDeg, ladderReturn},
		{"row4a never started", anthStream{}, io.ErrUnexpectedEOF, bg, "4a", sigDegraded, "stream_truncated", evDeg, ladderReturn},
		// Rule 0's exception: a terminal event in hand finishes the hop, and a
		// cancel after it is not row 1.
		{"completed then ctx canceled", anthStream{ended: true}, nil, canceledCtx(), "5", sigHealthy, "", evOK, ""},
		{"completed then err canceled and pipe closed", anthStream{ended: true, writeErr: io.ErrClosedPipe}, context.Canceled, canceledCtx(), "5", sigHealthy, "", evOK, ""},
		{"failed then ctx canceled", anthStream{failed: true, ended: true}, nil, canceledCtx(), "2", sigDegraded, "upstream_failed", evDeg, ladderReturn},
		{"refused then ctx canceled", anthStream{refused: true, ended: true, started: true}, nil, canceledCtx(), "21", sigClient, "upstream_incomplete", evNone, ladderReturn},
		{"row1 canceled mid-reply after content", anthStream{started: true, blocksOpened: 1}, context.Canceled, canceledCtx(), "1", sigCancelled, "", evNone, ladderStop},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := classifyCodexExit(&tc.a, tc.err, tc.ctx)
			if out.row != tc.row || out.sig != tc.sig || out.cause != tc.cause ||
				out.evidence != tc.ev || out.ladder != tc.ladder || !out.final || out.phase != "post_head" {
				t.Fatalf("got (row=%s sig=%s cause=%q ladder=%s ev=%s final=%v phase=%s), want (%s %s %q %s %s true post_head)",
					out.row, out.sig, out.cause, out.ladder, out.evidence, out.final, out.phase,
					tc.row, tc.sig, tc.cause, tc.ladder, tc.ev)
			}
		})
	}
}

// ---- the healthTap row table ----

// tapFeed drives chunks through scan the way Read would, then classifies.
func tapFeed(stream bool, ctx context.Context, chunks []string, readErr error) hopOutcome {
	tap := &healthTap{ctx: ctx, stream: stream}
	for _, c := range chunks {
		tap.scan([]byte(c))
	}
	return tap.outcome(readErr)
}

func TestHealthTapRowTable(t *testing.T) {
	bg := context.Background()
	okScript := "event: message_start\ndata: {}\n\nevent: content_block_start\ndata: {}\n\nevent: message_stop\ndata: {}\n\n"
	cases := []struct {
		name      string
		stream    bool
		ctx       context.Context
		chunks    []string
		readErr   error
		row, sig  string
		cause, ev string
	}{
		{"stream row5 message_stop", true, bg, []string{okScript}, io.EOF, "5", sigHealthy, "", evOK},
		{"stream row5 marker split across chunks", true, bg,
			[]string{"event: message_start\ndata: {}\n\nevent: messa", "ge_stop\ndata: {}\n\n"}, io.EOF, "5", sigHealthy, "", evOK},
		{"stream row2 upstream error event", true, bg,
			[]string{"event: message_start\ndata: {}\n\nevent: error\ndata: {}\n\n"}, io.EOF, "2", sigDegraded, "upstream_failed", evDeg},
		{"stream row4b truncated after a block", true, bg,
			[]string{"event: message_start\ndata: {}\n\nevent: content_block_start\ndata: {}\n\n"}, io.ErrUnexpectedEOF, "4b", sigUnknown, "stream_truncated_late", evNone},
		{"stream row4b block marker split across chunks", true, bg,
			[]string{"event: message_start\ndata: {}\n\nevent: content_bl", "ock_start\ndata: {}\n\n"}, io.ErrUnexpectedEOF, "4b", sigUnknown, "stream_truncated_late", evNone},
		{"stream row4a truncated before content", true, bg,
			[]string{"event: message_start\ndata: {}\n\n"}, io.ErrUnexpectedEOF, "4a", sigDegraded, "stream_truncated", evDeg},
		{"stream row4a non-SSE reply to a stream ask", true, bg,
			[]string{`{"type":"message"}`}, io.EOF, "4a", sigDegraded, "stream_truncated", evDeg},
		{"stream row1 canceled", true, canceledCtx(), []string{"event: message_start\n"}, nil, "1", sigCancelled, "", evNone},
		{"nonstream row5 json body", false, bg, []string{`  {"type":"message"}`}, io.EOF, "5", sigHealthy, "", evOK},
		{"nonstream row3 empty body", false, bg, nil, io.EOF, "3", sigDegraded, "bad_body", evDeg},
		{"nonstream row20 non-json residual", false, bg, []string{"<html>bad gateway</html>"}, io.EOF, "20", sigUnknown, "", evNone},
		{"nonstream row20 read error mid-body", false, bg, []string{`{"type":`}, io.ErrUnexpectedEOF, "20", sigUnknown, "", evNone},
		{"nonstream row1 canceled", false, canceledCtx(), []string{`{"x":1}`}, nil, "1", sigCancelled, "", evNone},
		// Rule 0's exception: the reply had finished before the cancel.
		{"stream row5 message_stop then canceled", true, canceledCtx(), []string{okScript}, nil, "5", sigHealthy, "", evOK},
		{"stream row5 message_stop then canceled read", true, canceledCtx(), []string{okScript}, context.Canceled, "5", sigHealthy, "", evOK},
		{"stream row1 canceled after a block", true, canceledCtx(),
			[]string{"event: message_start\ndata: {}\n\nevent: content_block_start\ndata: {}\n\n"}, context.Canceled, "1", sigCancelled, "", evNone},
		{"stream row1 error literal without message_stop", true, canceledCtx(),
			[]string{"event: message_start\ndata: {}\n\nevent: error\ndata: {}\n\n"}, context.Canceled, "1", sigCancelled, "", evNone},
		{"stream row2 error and message_stop then canceled", true, canceledCtx(),
			[]string{"event: message_start\ndata: {}\n\nevent: error\ndata: {}\n\nevent: message_stop\n"}, nil, "2", sigDegraded, "upstream_failed", evDeg},
		{"nonstream row5 body at EOF then canceled", false, canceledCtx(), []string{`{"type":"message"}`}, io.EOF, "5", sigHealthy, "", evOK},
		{"nonstream row1 canceled read mid-body", false, canceledCtx(), []string{`{"type":`}, context.Canceled, "1", sigCancelled, "", evNone},
		{"nonstream row1 empty body at EOF canceled", false, canceledCtx(), nil, io.EOF, "1", sigCancelled, "", evNone},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := tapFeed(tc.stream, tc.ctx, tc.chunks, tc.readErr)
			if out.row != tc.row || out.sig != tc.sig || out.cause != tc.cause || out.evidence != tc.ev {
				t.Fatalf("got (row=%s sig=%s cause=%q ev=%s), want (%s %s %q %s)",
					out.row, out.sig, out.cause, out.evidence, tc.row, tc.sig, tc.cause, tc.ev)
			}
		})
	}
}

// The tail window survives any chunking: a marker split byte-by-byte still latches.
func TestHealthTapMarkerByteAtATime(t *testing.T) {
	script := "event: message_start\ndata: {}\n\nevent: message_stop\ndata: {}\n\n"
	tap := &healthTap{ctx: context.Background(), stream: true}
	for i := 0; i < len(script); i++ {
		tap.scan([]byte{script[i]})
	}
	if out := tap.outcome(io.EOF); out.row != "5" {
		t.Fatalf("byte-at-a-time stream classified row %s, want 5", out.row)
	}
}

// Through the real reader path: record lands in the store exactly once, keyed
// by the served member, and Close after EOF does not double-record.
func TestHealthTapRecordsThroughStore(t *testing.T) {
	testClock(t)
	h := newHS()
	armHealth(t, h)
	cc := &classRoute{class: "helpdesk", reqID: "r-tap", classReq: classReq{origBody: []byte(`{"model":"claude-frontier"}`)}}
	body := io.NopCloser(strings.NewReader(`{"type":"message"}`))
	tap := newHealthTap(body, context.Background(), cc, tKey, false)
	if _, err := io.ReadAll(tap); err != nil {
		t.Fatalf("read: %v", err)
	}
	tap.Close()
	if n := ringLen(h, tKey); n != 1 {
		t.Fatalf("ring holds %d samples, want exactly 1 (EOF records, Close must not double)", n)
	}
	st := snap(t, h, tKey)
	s := st.ring[0]
	if !s.ok || s.class != "helpdesk" || s.reqID != "r-tap" || !s.forced {
		t.Fatalf("sample = %+v, want ok helpdesk r-tap forced (no other admissible member)", s)
	}
}

// A degraded stream writes a deg sample carrying the REQUEST body's fp (§4.4).
func TestHealthTapDegSampleCarriesRequestFP(t *testing.T) {
	testClock(t)
	h := newHS()
	armHealth(t, h)
	reqBody := []byte(`{"model":"claude-frontier","messages":[]}`)
	cc := &classRoute{class: "helpdesk", reqID: "r-deg", classReq: classReq{origBody: reqBody}}
	body := io.NopCloser(strings.NewReader("event: message_start\ndata: {}\n\nevent: error\ndata: {}\n\n"))
	tap := newHealthTap(body, context.Background(), cc, tKey, true)
	io.Copy(io.Discard, tap)
	tap.Close()
	st := snap(t, h, tKey)
	if len(st.ring) != 1 {
		t.Fatalf("ring holds %d samples, want 1", len(st.ring))
	}
	s := st.ring[0]
	if s.ok || s.cause != "upstream_failed" || s.fp != healthFP(reqBody) {
		t.Fatalf("sample = %+v, want deg upstream_failed with the request-body fp %d", s, healthFP(reqBody))
	}
}

// ---- E15: a canceled probe hop re-arms the token via probe_abandoned ----

func TestE15CancelSettlesProbeAbandoned(t *testing.T) {
	c := testClock(t)
	h := newHS()
	armHealth(t, h)
	toHalfOpen(t, h, c)
	l := h.claimProbe(tKey, "helpdesk", "gpt-5.6-sol", "probe-e15", 1000)
	if l == nil {
		t.Fatal("claimProbe returned nil")
	}
	hs := &hopScore{ctx: context.Background(), key: tKey, cc: &classRoute{class: "helpdesk", classReq: classReq{origBody: []byte(`{}`)}}, lease: l}
	// The codex cancel: pipe read half closed, stream never ended → row 1.
	hs.settle(classifyCodexExit(&anthStream{writeErr: io.ErrClosedPipe}, nil, context.Background()))
	st := snap(t, h, tKey)
	if st.state != hsHalfOpen || !st.probeTok || st.probeAbandons != 1 || st.lease != nil {
		t.Fatalf("after abandoned settle: state=%s tok=%v abandons=%d lease=%v — want half_open, token re-armed, 1 abandon, no lease",
			st.state, st.probeTok, st.probeAbandons, st.lease)
	}
	if n := ringLen(h, tKey); n != 4 { // trip's 4 samples only — the cancel wrote none
		t.Fatalf("ring holds %d samples, want 4 (row 1 is evidence-none)", n)
	}
}

// ---- E27 (producer path): a second settle of the same lease is inert ----

func TestE27DoubleSettleThroughProducerIsInert(t *testing.T) {
	c := testClock(t)
	h := newHS()
	armHealth(t, h)
	toHalfOpen(t, h, c)
	l := h.claimProbe(tKey, "helpdesk", "gpt-5.6-sol", "probe-e27", 1000)
	if l == nil {
		t.Fatal("claimProbe returned nil")
	}
	hs := &hopScore{ctx: context.Background(), key: tKey, cc: &classRoute{class: "helpdesk", classReq: classReq{origBody: []byte(`{}`)}}, lease: l}
	ok := hopOutcome{row: "5", sig: sigHealthy, evidence: evOK, phase: "post_head", final: true}
	hs.settle(ok)
	before := snap(t, h, tKey)
	hs.settle(ok) // the stale duplicate — CAS must make it a no-op
	after := snap(t, h, tKey)
	if after.state != before.state || after.probeOK != before.probeOK || len(after.ring) != len(before.ring) {
		t.Fatalf("double settle mutated state: before=(%s ok=%d ring=%d) after=(%s ok=%d ring=%d)",
			before.state, before.probeOK, len(before.ring), after.state, after.probeOK, len(after.ring))
	}
	h.mu.Lock()
	ds := h.doubleSettles[tKey]
	h.mu.Unlock()
	if ds != 1 {
		t.Fatalf("doubleSettles = %d, want 1", ds)
	}
}

// ---- producer gating: mode off records nothing anywhere ----

func TestProducersInertWhenHealthNil(t *testing.T) {
	armHealth(t, nil)
	if id := healthReqID(); id != "" {
		t.Fatalf("healthReqID = %q under off, want empty", id)
	}
	// A settle with no store must be a no-op, not a panic.
	hs := &hopScore{ctx: context.Background(), key: tKey, cc: &classRoute{class: "helpdesk"}}
	hs.settle(hopOutcome{row: "5", sig: sigHealthy, evidence: evOK, final: true})
	// The tap constructor is never even reached under off (main.go gates on
	// health != nil), but a nil-store settle path is what recordHopHead hits.
	recordHopHead(&classRoute{class: "helpdesk"}, memAnth, hopOutcome{row: "16", final: true, evidence: evDeg})
}

// ---- the decision record's out= under a late cancel ----

// A shape site's settled row stands over a cancel that came after it; a cancel
// with no settled row (mid-reply, or only a head row in hand) is cancelled.
func TestDecisionFinishLateCancel(t *testing.T) {
	armHealth(t, newHS())
	armObserve(t)
	cases := []struct {
		name string
		out  hopOutcome
		ctx  context.Context
		want string
	}{
		{"settled ok then cancel", hopOutcome{phase: "post_head", final: true, row: "5", sig: sigHealthy}, canceledCtx(), "ok"},
		{"settled deg then cancel", hopOutcome{phase: "post_head", final: true, row: "4a", sig: sigDegraded, cause: "stream_truncated"}, canceledCtx(), "deg"},
		{"settled row1 stays cancelled", hopOutcome{phase: "post_head", final: true, row: "1", sig: sigCancelled}, canceledCtx(), "cancelled"},
		{"no shape row yet", hopOutcome{phase: "head", row: "5", sig: sigHealthy}, canceledCtx(), "cancelled"},
		{"head-final row then cancel", hopOutcome{phase: "head", final: true, row: "13", sig: sigQuota}, canceledCtx(), "cancelled"},
		{"nothing at all", hopOutcome{}, canceledCtx(), "cancelled"},
		{"settled ok, no cancel", hopOutcome{phase: "post_head", final: true, row: "5", sig: sigHealthy}, context.Background(), "ok"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newDecision(httptest.NewRequest("POST", "/v1/messages", nil))
			d.outcome(tc.out)
			d.finish(tc.ctx, 200)
			if d.record.Outcome != tc.want {
				t.Fatalf("outcome = %q, want %q", d.record.Outcome, tc.want)
			}
		})
	}
}
