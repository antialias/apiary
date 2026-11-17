package main

// §6.2(1) exposition tests. The point of these counters is that they survive
// the questions the ring cannot answer, so the tests are mostly about what is
// present rather than what is pretty.

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// metricLine returns the value of the first series whose text matches sub,
// and whether it was found at all.
func metricLine(t *testing.T, out, sub string) (string, bool) {
	t.Helper()
	for _, ln := range strings.Split(out, "\n") {
		if strings.HasPrefix(ln, "#") || !strings.Contains(ln, sub) {
			continue
		}
		i := strings.LastIndex(ln, " ")
		return ln[i+1:], true
	}
	return "", false
}

func mustMetric(t *testing.T, out, sub string) string {
	t.Helper()
	v, ok := metricLine(t, out, sub)
	if !ok {
		t.Fatalf("no series matching %q in:\n%s", sub, out)
	}
	return v
}

func TestMetricsExpositionShape(t *testing.T) {
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	hs.record("lane:m1", sample{ok: true, class: "c1", reqID: "r1"})

	out := renderHealthMetrics(hs)

	// Every family declares HELP and TYPE exactly once, and every declared
	// family has at least one series — a bare HELP is a lie about coverage.
	types := map[string]int{}
	series := map[string]int{}
	for _, ln := range strings.Split(out, "\n") {
		switch {
		case strings.HasPrefix(ln, "# TYPE "):
			types[strings.Fields(ln)[2]]++
		case ln == "" || strings.HasPrefix(ln, "#"):
		default:
			name := ln
			if i := strings.IndexAny(name, "{ "); i >= 0 {
				name = name[:i]
			}
			series[name]++
		}
	}
	for name, n := range types {
		if n != 1 {
			t.Errorf("family %s declared %d times, want 1", name, n)
		}
		if series[name] == 0 {
			t.Errorf("family %s declared but has no series", name)
		}
	}
	for name := range series {
		if types[name] == 0 {
			t.Errorf("series %s has no HELP/TYPE", name)
		}
	}
}

func TestMetricsStateGaugeSumsToOne(t *testing.T) {
	c := testClock(t)
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	tripK(t, hs, c, "lane:m1")

	out := renderHealthMetrics(hs)
	sum := 0
	for _, st := range []string{hsHealthy, hsOpen, hsHalfOpen, hsConvalescent} {
		if v := mustMetric(t, out, `state{key="lane:m1",state="`+st+`"}`); v == "1" {
			sum++
		}
	}
	if sum != 1 {
		t.Fatalf("state gauge sums to %d, want exactly 1:\n%s", sum, out)
	}
	if v := mustMetric(t, out, `state{key="lane:m1",state="`+hsOpen+`"}`); v != "1" {
		t.Fatalf("tripped key is not open:\n%s", out)
	}
	// The trip is attributable to its key, not just pooled.
	if v := mustMetric(t, out, `transitions_total{key="lane:m1",from="healthy",to="open"}`); v != "1" {
		t.Fatalf("transitions_total = %s, want 1", v)
	}
	if v := mustMetric(t, out, `episodes_total{key="lane:m1"}`); v != "1" {
		t.Fatalf("episodes_total = %s, want 1", v)
	}
}

// The series the observe stage exists to read: it must distinguish "scored
// clean traffic" from "never ran", and name the cause when it degrades.
func TestMetricsSamplesTotalSeparatesCleanFromSilent(t *testing.T) {
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)

	if out := renderHealthMetrics(hs); strings.Contains(out, "samples_total") {
		t.Fatal("samples_total exists before anything was scored — silence must read as absence")
	}
	base := time.Now()
	for i := 0; i < 3; i++ {
		hs.record("lane:m1", sample{t: base.Add(time.Duration(i) * 6 * time.Second),
			ok: true, class: "c1", reqID: "ok" + string(rune('a'+i))})
	}
	hs.record("lane:m1", sample{t: base.Add(18 * time.Second),
		ok: false, cause: "http_5xx", class: "c1", reqID: "d1", fp: 7})

	out := renderHealthMetrics(hs)
	if v := mustMetric(t, out, `samples_total{key="lane:m1",class="c1",outcome="ok",cause="",forced="false"}`); v != "3" {
		t.Fatalf("ok samples = %s, want 3", v)
	}
	if v := mustMetric(t, out, `samples_total{key="lane:m1",class="c1",outcome="deg",cause="http_5xx",forced="false"}`); v != "1" {
		t.Fatalf("deg samples = %s, want 1", v)
	}
	// evidence_rate is the same number the trip arm reads.
	if v := mustMetric(t, out, `evidence_rate{key="lane:m1"}`); v != "0.25" {
		t.Fatalf("evidence_rate = %s, want 0.25", v)
	}
	if v := mustMetric(t, out, `evidence_samples{key="lane:m1"}`); v != "4" {
		t.Fatalf("evidence_samples = %s, want 4", v)
	}
}

// samples_total counts ring-ADMITTED evidence, so §4.1 rule 1's one-per-
// (class, 5 s bucket) dedup applies to it too. It measures what the trip arm
// sees, not raw hop throughput — a distinction anyone reading the panel needs.
func TestMetricsSamplesFollowBucketDedup(t *testing.T) {
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	base := time.Now()
	for i := 0; i < 5; i++ {
		hs.record("lane:m1", sample{t: base, ok: true, class: "c1", reqID: "r" + string(rune('a'+i))})
	}
	out := renderHealthMetrics(hs)
	if v := mustMetric(t, out, `outcome="ok"`); v != "1" {
		t.Fatalf("five same-bucket hops counted %s samples, want 1", v)
	}
}

// A deg that supersedes an ok for the same request is evidence arriving, so it
// counts; the superseded ok already counted. The pair is the audit trail.
func TestMetricsSupersededOkStillCounts(t *testing.T) {
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	hs.record("lane:m1", sample{ok: true, class: "c1", reqID: "same"})
	hs.record("lane:m1", sample{ok: false, cause: "transport", class: "c1", reqID: "same"})

	out := renderHealthMetrics(hs)
	if v := mustMetric(t, out, `outcome="ok"`); v != "1" {
		t.Fatalf("ok = %s, want 1", v)
	}
	if v := mustMetric(t, out, `cause="transport"`); v != "1" {
		t.Fatalf("deg = %s, want 1", v)
	}
	if n := len(snap(t, hs, "lane:m1").ring); n != 1 {
		t.Fatalf("ring holds %d samples, want 1 — the counter must not change dedup", n)
	}
}

// This one is an assertion, not a measurement: a missing series would be
// indistinguishable from a healthy one.
func TestMetricsDoubleSettleAlwaysPresent(t *testing.T) {
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	out := renderHealthMetrics(hs)
	if v := mustMetric(t, out, "lease_double_settle_total"); v != "0" {
		t.Fatalf("lease_double_settle_total = %s, want 0", v)
	}
}

func TestMetricsModeAndDecisions(t *testing.T) {
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	armObserve(t)
	hs.noteRouteDecision("frontier", "bound", "")
	hs.noteRouteDecision("frontier", "", "")
	hs.noteExhausted("frontier", true)

	out := renderHealthMetrics(hs)
	if v := mustMetric(t, out, `mode{mode="observe"}`); v != "1" {
		t.Fatalf("observe mode gauge = %s", v)
	}
	if v := mustMetric(t, out, `mode{mode="enforce"}`); v != "0" {
		t.Fatalf("enforce mode gauge = %s, want 0", v)
	}
	if v := mustMetric(t, out, `route_decisions_total{class="frontier",hop_kind="bound",reason=""}`); v != "1" {
		t.Fatalf("bound decisions = %s, want 1", v)
	}
	// An unset hop kind is still a decision — it must not vanish into "".
	if v := mustMetric(t, out, `hop_kind="none"`); v != "1" {
		t.Fatalf("none decisions = %s, want 1", v)
	}
	if v := mustMetric(t, out, `exhausted_total{class="frontier",pinned="true"}`); v != "1" {
		t.Fatalf("exhausted = %s, want 1", v)
	}
}

func TestMetricsEscapesLabelValues(t *testing.T) {
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	hs.noteRouteDecision(`we"ird\class`, "bound", "r")
	out := renderHealthMetrics(hs)
	if !strings.Contains(out, `class="we\"ird\\class"`) {
		t.Fatalf("label not escaped:\n%s", out)
	}
}

// The chain that matters: a real request through the handler, then a scrape
// off the live endpoint, with the client gate in front of it.
func TestE2EMetricsEndpointServesScoredTraffic(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	armObserve(t)

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-metrics")
	defer resp.Body.Close()
	h.hit(t)
	io.Copy(io.Discard, resp.Body)
	waitRing(t, hs, "lane:fable", 1)

	scrape, err := http.Get(h.proxy.URL + "/_switch/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer scrape.Body.Close()
	if scrape.StatusCode != 200 {
		t.Fatalf("scrape status = %d", scrape.StatusCode)
	}
	if ct := scrape.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("Content-Type = %q, want text/plain exposition", ct)
	}
	b, _ := io.ReadAll(scrape.Body)
	out := string(b)
	// Every health series wears the tenant label first (the household's here).
	tl := `tenant="` + testLanTenant + `",`
	if !strings.Contains(out, `samples_total{`+tl+`key="lane:fable",class="frontier",outcome="ok"`) {
		t.Fatalf("the served hop did not reach samples_total:\n%s", out)
	}
	if !strings.Contains(out, `route_decisions_total{`+tl+`class="frontier"`) {
		t.Fatalf("no route decision recorded:\n%s", out)
	}
	if !strings.Contains(out, `switch_proxy_requests_total{lane="class:frontier",tenant="`+testLanTenant+`",code="200"}`) {
		t.Fatalf("requests_total has no tenant label:\n%s", out)
	}
	if v := mustMetric(t, out, `state{`+tl+`key="lane:fable",state="healthy"}`); v != "1" {
		t.Fatalf("served key not healthy: %s", v)
	}
}

var _ = time.Second
