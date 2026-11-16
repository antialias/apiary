package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func recoveryFixture(t *testing.T, handler http.HandlerFunc) (*recoveryRunner, *fakeClock, string) {
	t.Helper()
	c := testClock(t)
	h := newHS()
	armHealth(t, h)
	oldMode, oldFor := healthModeV, healthEnforceFor
	healthModeV, healthEnforceFor = healthEnforce, map[string]bool{recoveryClass: true}
	t.Cleanup(func() { healthModeV, healthEnforceFor = oldMode, oldFor })
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL + "/coding")
	oldProviders := providers
	providers = []*provider{{name: "recovery-test", target: u, wire: bearerWire{key: "test-only-key"}}}
	t.Cleanup(func() { providers = oldProviders })
	id := uniq("recovery")
	m := classMemberPref{ID: id, Kind: "provider", Provider: "recovery-test", Model: "test-model", Route: uRoute(id)}
	pref.mu.Lock()
	oldClasses := pref.classes
	pref.classes = map[string]classPref{recoveryClass: {Members: []classMemberPref{m}}}
	pref.mu.Unlock()
	t.Cleanup(func() { pref.mu.Lock(); pref.classes = oldClasses; pref.mu.Unlock() })
	halfOpenK(t, h, c, m.ID)
	return &recoveryRunner{transport: &failoverTransport{base: srv.Client().Transport}, deadline: time.Second}, c, m.ID
}

func TestRecoveryBackgroundClosesWithoutCustomerTraffic(t *testing.T) {
	var calls atomic.Int32
	r, c, key := recoveryFixture(t, func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(req.Body)
		if req.URL.Path != "/coding/v1/messages" || req.Header.Get("Authorization") != "Bearer test-only-key" || !strings.Contains(string(body), `"model":"test-model"`) || strings.Contains(string(body), "customer-secret") {
			t.Errorf("incorrect member actuation: path=%s body=%s", req.URL.Path, body)
		}
		io.WriteString(w, `{"type":"message","stop_reason":"end_turn","content":[{"type":"text","text":"OK"}]}`)
	})
	r.pass(context.Background())
	mustState(t, health, key, hsHalfOpen)
	r.pass(context.Background())
	if calls.Load() != 1 {
		t.Fatal("rate limit failed")
	}
	c.advance(time.Minute)
	r.pass(context.Background())
	mustState(t, health, key, hsConvalescent)
	if calls.Load() != 2 || r.totals["ok"] != 2 || snap(t, health, key).probeID != "" {
		t.Fatal("probe accounting/settlement failed")
	}
}

func TestRecoveryDeadlineIncludesStreamingBodyAndDoesNotFallback(t *testing.T) {
	var calls atomic.Int32
	entered := make(chan struct{})
	r, _, key := recoveryFixture(t, func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		close(entered)
		<-req.Context().Done()
	})
	r.deadline = 100 * time.Millisecond
	done := make(chan struct{})
	start := time.Now()
	go func() { r.pass(context.Background()); close(done) }()
	<-entered
	if health.claimProbe(key, recoveryClass, key, "customer", 1) != nil {
		t.Fatal("background lease did not exclude customer probe")
	}
	r.pass(context.Background())
	<-done
	if time.Since(start) > time.Second || calls.Load() != 1 || r.totals["deadline"] != 1 {
		t.Fatalf("unbounded or repeated attempt: %+v", r.snapshot())
	}
	mustState(t, health, key, hsOpen)
	if snap(t, health, key).probeID != "" {
		t.Fatal("leaked probe lease")
	}
}

func TestRecoveryGatesAndInvalidResults(t *testing.T) {
	for _, tc := range []struct {
		name, mode, body, result string
		status                   int
	}{
		{"observe", "observe", "", "", 200},
		{"other-class", "enforce:frontier", "", "", 200},
		{"off", "off", "", "", 200},
		{"truncated", "enforce:fast", `{"type":"message","stop_reason":"max_tokens","content":[{"type":"text","text":"OK"}]}`, "invalid_response", 200},
		{"wrong-answer", "enforce:fast", `{"type":"message","stop_reason":"end_turn","content":[{"type":"text","text":"not OK"}]}`, "invalid_response", 200},
		{"too-large", "enforce:fast", strings.Repeat("x", recoveryMaxBytes+1), "invalid_response", 200},
		{"quota", "enforce:fast", `{"error":{"type":"rate_limit_error","message":"rate limit"}}`, "abandoned", 429},
		{"server", "enforce:fast", `{"error":"unavailable"}`, "degraded", 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			r, _, key := recoveryFixture(t, func(w http.ResponseWriter, req *http.Request) {
				calls.Add(1)
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			})
			healthModeV, healthEnforceFor = parseHealthMode(tc.mode)
			r.pass(context.Background())
			if tc.result == "" {
				if calls.Load() != 0 {
					t.Fatal("outbound probe in disabled mode")
				}
			} else if calls.Load() != 1 || r.totals[tc.result] != 1 || snap(t, health, key).probeID != "" {
				t.Fatalf("bad outcome: %+v", r.snapshot())
			}
			if tc.result != "degraded" {
				mustState(t, health, key, hsHalfOpen)
			}
		})
	}
}

func TestRecoveryCancellationAbandonsLease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r, _, key := recoveryFixture(t, func(w http.ResponseWriter, req *http.Request) {
		io.Copy(io.Discard, req.Body)
		cancel()
		<-req.Context().Done()
	})
	r.pass(ctx)
	mustState(t, health, key, hsHalfOpen)
	if r.totals["abandoned"] != 1 || snap(t, health, key).probeID != "" {
		t.Fatal("cancellation scored or leaked lease")
	}
}

// §2.2 row 13: the prober reads the check itself. It probes an included
// member whatever its tier (a HALF_OPEN member over its hard number is still
// inside its allowance), and never a paid one or a `no` one: a probe never
// spends money.
func TestRecoveryProbesOnlyAnIncludedCheck(t *testing.T) {
	for _, tc := range []struct {
		name  string
		set   func(key string, now time.Time)
		probe bool
		check string
	}{
		{"included", func(string, time.Time) {}, true, checkIncluded},
		{"included-over-hard", func(key string, _ time.Time) {
			pref.mu.Lock()
			m := pref.classes[recoveryClass].Members[0]
			m.Effective = ptr(100)
			pref.classes[recoveryClass] = classPref{Members: []classMemberPref{m}}
			pref.mu.Unlock()
		}, true, checkIncluded},
		{"paid", func(key string, now time.Time) {
			acct := uAccount(key)
			pref.mu.Lock()
			r := pref.routes["u/"+key]
			r.Pay = []string{acct + "/credits"}
			pref.routes["u/"+key] = r
			pref.accounts[acct] = standingAt("anthropic", now, map[string]checkWindow{
				"seven_day": week(100, now.Add(48*time.Hour))}, "open")
			pref.mu.Unlock()
		}, false, checkPaid},
		{"no-pay", func(key string, now time.Time) {
			pref.mu.Lock()
			pref.accounts[uAccount(key)] = standingAt("anthropic", now, map[string]checkWindow{
				"seven_day": week(100, now.Add(48*time.Hour))}, "open")
			pref.mu.Unlock()
		}, false, checkNo},
		{"spent-record", func(key string, _ time.Time) {
			seedSpent(uAccount(key), "seven_day", 429, "usage_limit_reached", time.Time{})
		}, false, checkNo},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(resetQuotaState())
			var calls atomic.Int32
			r, _, key := recoveryFixture(t, func(w http.ResponseWriter, req *http.Request) {
				calls.Add(1)
				io.WriteString(w, `{"type":"message","stop_reason":"end_turn","content":[{"type":"text","text":"OK"}]}`)
			})
			tc.set(key, nowFn())
			pref.mu.RLock()
			m := pref.classes[recoveryClass].Members[0]
			pref.mu.RUnlock()
			c, _ := routeCheck(m.Route, uAccount(key), m.Model, nowFn())
			if c.Check != tc.check {
				t.Fatalf("fixture: check %s, want %s", c, tc.check)
			}
			r.pass(context.Background())
			if got := calls.Load() == 1; got != tc.probe {
				t.Fatalf("check %s: probed=%v, want %v", c, got, tc.probe)
			}
		})
	}
}

// The probe itself dials only on the check rebuildForMember returns at dial
// time, not on the one ranking saw: the standing can move between the two, and
// a probe never spends money.
func TestRecoveryProbeUsesTheDialTimeCheck(t *testing.T) {
	t.Cleanup(resetQuotaState())
	var calls atomic.Int32
	r, _, key := recoveryFixture(t, func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		io.WriteString(w, `{"type":"message","stop_reason":"end_turn","content":[{"type":"text","text":"OK"}]}`)
	})
	acct, now := uAccount(key), nowFn()
	pref.mu.Lock()
	rt := pref.routes["u/"+key]
	rt.Pay = []string{acct + "/credits"}
	pref.routes["u/"+key] = rt
	pref.accounts[acct] = standingAt("anthropic", now, map[string]checkWindow{
		"seven_day": week(100, now.Add(48*time.Hour))}, "open")
	m := pref.classes[recoveryClass].Members[0]
	pref.mu.Unlock()
	if c, _ := routeCheck(m.Route, acct, m.Model, now); !c.paid() {
		t.Fatalf("fixture: check %s, want paid", c)
	}
	if _, result := r.probe(context.Background(), m); result != "unavailable" || calls.Load() != 0 {
		t.Fatalf("probe on a paid dial: result %q, %d upstream calls; want unavailable and none", result, calls.Load())
	}
}
