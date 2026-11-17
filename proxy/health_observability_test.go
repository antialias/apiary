package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func awaitDecision(t *testing.T, id string) routeDecision {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, f := range evRingFrames() {
			p := evParseFrame(t, f.b)
			if p.event != "route_decision" {
				continue
			}
			b, _ := json.Marshal(p.env.Data)
			var d routeDecision
			if err := json.Unmarshal(b, &d); err != nil {
				t.Fatal(err)
			}
			if d.DecisionID == id {
				return d
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no final decision for %s", id)
	return routeDecision{}
}

func TestDecisionHeadAndTerminalThroughHTTP(t *testing.T) {
	for _, truncated := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy", true: "truncated"}[truncated], func(t *testing.T) {
			h, restore := newE2EClass(t)
			defer restore()
			armHealth(t, newHealthStore(defaultHealthCfg()))
			armObserve(t)
			evTest(t)
			evArm()
			script := "event: message_start\ndata: {\"type\":\"message_start\"}\n\n" + "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"hi\"}}\n\n"
			if !truncated {
				script += "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
			}
			h.anthSSE.Store(script)
			resp := h.postClassBody(t, "test-decisions", `{"model":"claude-frontier","stream":true,"max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`)
			b, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			h.hit(t)
			if err != nil || string(b) != script {
				t.Fatalf("response changed: %q %v", b, err)
			}
			if got := resp.Header.Values("X-Switch-Health"); len(got) != 1 || got[0] != "none" {
				t.Fatalf("head: %v", got)
			}
			id := resp.Header.Get("X-Switch-Decision")
			if len(resp.Header.Values("X-Switch-Decision")) != 1 || !strings.HasPrefix(id, "r-"+evEpoch+"-") {
				t.Fatalf("identity: %v", resp.Header)
			}
			d := awaitDecision(t, id)
			want := "ok"
			if truncated {
				want = "deg"
			}
			if d.Outcome != want || d.Noted || d.NoteKind != nil || d.Attempts != 1 || valueOrEmpty(d.ServedMember) != "lane:fable" {
				t.Fatalf("decision: %+v", d)
			}
			if truncated && valueOrEmpty(d.Cause) != "stream_truncated" {
				t.Fatalf("missing terminal cause: %+v", d)
			}
			if d.ConversationKey == "route:"+id {
				t.Fatal("decision did not join the tapped turn")
			}
		})
	}
}

func TestDecisionSurvivesResponseAbortOnce(t *testing.T) {
	armObserve(t)
	evTest(t)
	evArm()
	handler := requestHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d := decisionFrom(r.Context())
		d.outcome(hopOutcome{final: true, sig: sigDegraded, cause: "stream_truncated"})
		w.WriteHeader(200)
		panic(http.ErrAbortHandler)
	}))
	rec := httptest.NewRecorder()
	func() {
		defer func() {
			if recover() != http.ErrAbortHandler {
				t.Error("abort was swallowed")
			}
		}()
		handler.ServeHTTP(rec, lanRequest("GET", "/", nil))
	}()
	d := awaitDecision(t, rec.Header().Get("X-Switch-Decision"))
	if d.Outcome != "deg" || valueOrEmpty(d.Cause) != "stream_truncated" {
		t.Fatalf("decision: %+v", d)
	}
	n := 0
	for _, f := range evRingFrames() {
		if evParseFrame(t, f.b).event == "route_decision" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d terminal decisions", n)
	}
}

func TestDecisionDirectEarlyDenyAndPinContract(t *testing.T) {
	armObserve(t)
	evTest(t)
	evArm()
	rec := httptest.NewRecorder()
	requestHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("denial forwarded") })).ServeHTTP(rec, lanRequest("POST", "/v1/not-an-api", nil))
	d := awaitDecision(t, rec.Header().Get("X-Switch-Decision"))
	if d.HopKind != "direct" || d.HealthState != "n/a" || d.Outcome != "client_error" || d.Attempts != 0 || d.Class != nil {
		t.Fatalf("denial: %+v", d)
	}
	if rec.Header().Get("X-Switch-Health") != "" {
		t.Fatal("direct health header")
	}
	old := lan.bindCache
	lan.bindCache = map[string]classBind{}
	defer func() { lan.bindCache = old }()
	no := false
	lan.bindCache["c\x00s"] = classBind{pinned: &no}
	if pin, source := lan.pinForBinding("c", "s", "bind: pinned to x"); pin || source != "bind_payload" {
		t.Fatal("explicit false lost to stale reason")
	}
	if pin, source := lan.pinForBinding("c", "legacy", "bind: pinned to x"); !pin || source != "reason_prefix" {
		t.Fatal("legacy pin missing")
	}
}

func TestHealthWireFixture(t *testing.T) {
	// Generated from the Go payload types and FSM, then validated by Scry's Zod schemas.
	c := testClock(t)
	h := newHealthStore(defaultHealthCfg())
	armHealth(t, h)
	armObserve(t)
	evTest(t)
	evArm()
	h.record("lane:fable", okS("fixture-ok"))
	c.advance(6 * time.Second)
	for i := 0; i < 3; i++ {
		s := degS(string(rune('a'+i)), uint64(301+i))
		s.cause = "http_5xx"
		h.record("lane:fable", s)
		c.advance(6 * time.Second)
	}
	h.noteBinding("frontier", "lane:fable", false)
	h.noteBinding("frontier", "lane:fable", false)
	h.noteAuthFailure("lane:fable")
	metrics := renderHealthMetrics(h)
	for _, s := range []string{`switch_health_auth_failures_total{key="lane:fable"} 1`, `switch_health_pointer_fighting_health{class="frontier"} 1`, `switch_health_ramp_pending{class="frontier"} 0`} {
		if !strings.Contains(metrics, s) {
			t.Errorf("missing %s", s)
		}
	}
	var transition any
	for _, f := range evRingFrames() {
		p := evParseFrame(t, f.b)
		if p.event == "member_health" {
			transition = p.env.Data
		}
	}
	if transition == nil {
		t.Fatal("no transition")
	}
	h.mu.Lock()
	snapshot := h.snapshotLocked(nil)
	h.mu.Unlock()
	d := newDecision(httptest.NewRequest("POST", "/v1/messages", nil))
	d.record.DecisionID = "r-fixture-1"
	d.record.ConversationKey = "fixture-turn"
	d.record.RequestedModel = "claude-frontier"
	d.record.TS = c.now().UTC().Format(time.RFC3339Nano)
	d.record.Class = nullable("frontier")
	d.record.HopKind = "bound"
	d.record.Reason = "bind:pointer"
	d.record.HealthState = "healthy"
	d.record.ServedMember = nullable("lane:fable")
	d.record.BoundMember = nullable("lane:fable")
	d.record.ServedAccount = nullable("test-account")
	d.record.Attempts = 1
	d.outcome(hopOutcome{final: true, sig: sigDegraded, cause: "stream_truncated"})
	d.finish(context.Background(), 200)
	d.record.ElapsedMS = 12
	status := lan.healthStatus()
	// Process-global note counters depend on earlier tests, unlike this store's state.
	status["notes"].(map[string]any)["emitted_total"] = int64(0)
	status["notes"].(map[string]any)["stripped_total"] = int64(0)
	wire := map[string]any{"route_decision": d.record, "member_health": transition, "member_health_snapshot": snapshot, "status": status}
	b, err := json.MarshalIndent(wire, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	b = append(b, '\n')
	path := scryCopy(t, "lib/testdata/health-wire.json")
	if os.Getenv("UPDATE_HEALTH_FIXTURE") == "1" {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, b, 0644); err != nil {
			t.Fatal(err)
		}
	}
	old, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(old) != string(b) {
		t.Fatal("Go health contract differs from Scry fixture; review and regenerate with UPDATE_HEALTH_FIXTURE=1")
	}
}

func TestPinnedClassDoesNotFightHealth(t *testing.T) {
	c := testClock(t)
	h := newHealthStore(defaultHealthCfg())
	tripK(t, h, c, "lane:fable")
	h.noteBinding("frontier", "lane:fable", false)
	h.noteBinding("frontier", "lane:fable", false)
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.fightingLocked("frontier") {
		t.Fatal("repeated open bindings not detected")
	}
	h.pins = map[string]classPinHealth{"frontier": {member: "lane:fable", source: "bind_payload"}}
	if h.fightingLocked("frontier") {
		t.Fatal("an explicit pin is intentional, not pointer contention")
	}
}
