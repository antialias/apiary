package main

// Request-path metrics (reqmetrics.go). The case that matters is the
// incident shape: the whole answer was streamed, then the handler
// never returned. It must read as in flight, with a growing oldest age.

import (
	"bufio"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func swapReqMetrics(t *testing.T) *reqMetricsT {
	t.Helper()
	old := reqMetrics
	reqMetrics = newReqMetrics()
	t.Cleanup(func() { reqMetrics = old })
	return reqMetrics
}

// stuckAfterStream answers a full SSE body, flushes it, then blocks until
// release closes — the handler-never-returns shape.
func stuckAfterStream(release <-chan struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "event: message_start\ndata: {}\n\nevent: message_stop\ndata: {}\n\n")
		w.(http.Flusher).Flush()
		<-release
	})
}

func TestReqMetricsBlockedHandlerStaysInFlight(t *testing.T) {
	rm := swapReqMetrics(t)
	release := make(chan struct{})
	srv := httptest.NewServer(clientGate("", requestHandler(stuckAfterStream(release))))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/not-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	// Read the whole streamed answer: the client has everything but EOF.
	br := bufio.NewReader(resp.Body)
	for {
		ln, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("stream ended early: %v", err)
		}
		if strings.HasPrefix(ln, "event: message_stop") {
			break
		}
	}
	time.Sleep(50 * time.Millisecond)

	out := rm.render(time.Now())
	if v := mustMetric(t, out, "switch_proxy_requests_in_flight "); v != "1" {
		t.Fatalf("in flight = %s, want 1 while the handler is blocked:\n%s", v, out)
	}
	age, _ := strconv.ParseFloat(mustMetric(t, out, "switch_proxy_oldest_in_flight_seconds "), 64)
	if age < 0.04 {
		t.Fatalf("oldest in-flight age = %v, want >= the time blocked", age)
	}
	if _, ok := metricLine(t, out, "switch_proxy_requests_total{"); ok {
		t.Fatalf("a blocked request was counted as completed:\n%s", out)
	}
	// Ages from the request's start, not from the scrape.
	later, _ := strconv.ParseFloat(mustMetric(t, rm.render(time.Now().Add(time.Minute)), "oldest_in_flight_seconds "), 64)
	if later < 60 {
		t.Fatalf("oldest age a minute later = %v, want >= 60", later)
	}

	close(release)
	_, _ = io.Copy(io.Discard, br)
	out = rm.render(time.Now())
	if v := mustMetric(t, out, "switch_proxy_requests_in_flight "); v != "0" {
		t.Fatalf("in flight after release = %s", v)
	}
	if v := mustMetric(t, out, "switch_proxy_oldest_in_flight_seconds "); v != "0" {
		t.Fatalf("oldest age with nothing in flight = %s, want 0", v)
	}
	if v := mustMetric(t, out, `switch_proxy_requests_total{lane="other",tenant="`+testLanTenant+`",code="200"}`); v != "1" {
		t.Fatalf("requests_total = %s:\n%s", v, out)
	}
	if v := mustMetric(t, out, `switch_proxy_request_duration_seconds_count{lane="other"}`); v != "1" {
		t.Fatalf("duration count = %s", v)
	}
	dur, _ := strconv.ParseFloat(mustMetric(t, out, `switch_proxy_request_duration_seconds_sum{lane="other"}`), 64)
	ttfb, _ := strconv.ParseFloat(mustMetric(t, out, `switch_proxy_request_ttfb_seconds_sum{lane="other"}`), 64)
	if !(ttfb >= 0 && ttfb < dur && dur >= 0.04) {
		t.Fatalf("ttfb %v must precede full duration %v (which includes the blocked tail)", ttfb, dur)
	}
}

func TestReqMetricsLaneAndStatus(t *testing.T) {
	rm := swapReqMetrics(t)
	srv := httptest.NewServer(clientGate("", requestHandler(http.NotFoundHandler())))
	defer srv.Close()
	// An unknown /v1 path is denied before routing: lane other, code 404.
	resp, err := http.Post(srv.URL+"/v1/nope", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	out := rm.render(time.Now())
	if v := mustMetric(t, out, `requests_total{lane="other",tenant="`+testLanTenant+`",code="404"}`); v != "1" {
		t.Fatalf("denied request not counted under other/404:\n%s", out)
	}
	// Denials write a body, so TTFB is observed for them too.
	if v := mustMetric(t, out, `request_ttfb_seconds_count{lane="other"}`); v != "1" {
		t.Fatalf("ttfb count = %s", v)
	}
}

func TestReqMetricsHistogramAndExposition(t *testing.T) {
	rm := newReqMetrics()
	t0 := time.Unix(1000, 0)
	for _, d := range []time.Duration{30 * time.Millisecond, 700 * time.Millisecond, 45 * time.Second, 20 * time.Minute} {
		id := rm.begin(t0)
		rm.end(id, "opus", "", 200, t0, t0.Add(d/2).UnixNano(), t0.Add(d))
	}
	id := rm.begin(t0)
	rm.end(id, "class:frontier", "", 529, t0, 0, t0.Add(time.Second)) // no body byte: no TTFB sample
	out := rm.render(t0.Add(time.Hour))

	// Same shape rules as the health exposition: one HELP/TYPE per family,
	// every family has series, every series has a family.
	types := map[string]string{}
	series := map[string]int{}
	for _, ln := range strings.Split(out, "\n") {
		switch {
		case strings.HasPrefix(ln, "# TYPE "):
			f := strings.Fields(ln)
			if _, dup := types[f[2]]; dup {
				t.Errorf("family %s declared twice", f[2])
			}
			types[f[2]] = f[3]
		case ln == "" || strings.HasPrefix(ln, "#"):
		default:
			name := ln[:strings.IndexAny(ln, "{ ")]
			if _, err := strconv.ParseFloat(ln[strings.LastIndex(ln, " ")+1:], 64); err != nil {
				t.Errorf("unparseable value in %q", ln)
			}
			for _, suf := range []string{"_bucket", "_sum", "_count"} {
				if base := strings.TrimSuffix(name, suf); base != name && types[base] == "histogram" {
					name = base
				}
			}
			series[name]++
		}
	}
	for name := range types {
		if series[name] == 0 {
			t.Errorf("family %s declared but has no series", name)
		}
	}
	for name := range series {
		if types[name] == "" {
			t.Errorf("series %s has no HELP/TYPE", name)
		}
	}

	// Buckets are cumulative, inclusive, and end at +Inf == _count.
	want := map[string]string{"0.05": "1", "1": "2", "60": "3", "600": "3", "+Inf": "4"}
	for le, n := range want {
		if v := mustMetric(t, out, `request_duration_seconds_bucket{lane="opus",le="`+le+`"}`); v != n {
			t.Errorf("duration bucket le=%s = %s, want %s", le, v, n)
		}
	}
	if v := mustMetric(t, out, `request_duration_seconds_count{lane="opus"}`); v != "4" {
		t.Errorf("duration count = %s", v)
	}
	if v := mustMetric(t, out, `request_ttfb_seconds_bucket{lane="opus",le="0.05"}`); v != "1" {
		t.Errorf("ttfb le=0.05 = %s, want 1 (15ms)", v)
	}
	if _, ok := metricLine(t, out, `request_ttfb_seconds_count{lane="class:frontier"}`); ok {
		t.Error("a request with no body byte produced a TTFB sample")
	}
	if v := mustMetric(t, out, `requests_total{lane="class:frontier",tenant="",code="529"}`); v != "1" {
		t.Errorf("class 529 count = %s", v)
	}
	if v := mustMetric(t, out, "switch_proxy_process_start_time_seconds "); v == "0" {
		t.Error("process start time unset")
	}
	if v := mustMetric(t, out, "switch_proxy_goroutines "); v == "0" {
		t.Error("goroutines = 0")
	}
}

// Under SWITCH_HEALTH=off there is no store, but the request path still
// speaks: the endpoint serves the switch_proxy_* series alone.
func TestReqMetricsServedWithoutHealthStore(t *testing.T) {
	swapReqMetrics(t)
	if lan.health != nil {
		t.Skip("a health store is armed by an earlier test")
	}
	w := httptest.NewRecorder()
	switchHandler(w, httptest.NewRequest("GET", "/_switch/metrics", nil))
	out := w.Body.String()
	if !strings.HasPrefix(w.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("Content-Type = %q", w.Header().Get("Content-Type"))
	}
	mustMetric(t, out, "switch_proxy_requests_in_flight ")
	if strings.Contains(out, "switch_health_") {
		t.Fatalf("health series without a store:\n%s", out)
	}
}
