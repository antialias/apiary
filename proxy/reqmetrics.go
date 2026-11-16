package main

// Request-path metrics: the series that see the request path itself.
//
// Why these exist: every request through the proxy once hung for
// hours after streaming its answer — the handler blocked on its final log line
// because the container's stdout pipe stopped draining. /_switch/healthz said
// ok and the scrape's up stayed 1 the whole time, because neither
// touches the request path. These series do, without being specific to that
// cause: a handler stuck anywhere after it started is still in flight, and
// the age of the OLDEST in-flight request climbs without bound.
//
// Scope: every request that reaches requestHandler — /v1/messages,
// /v1/messages/count_tokens, the OpenAI wire (/v1/chat/completions, which
// re-dispatches through the same chain), and denied paths (unknown /v1/*,
// non-/v1, oversized bodies), which land under lane="other". Not counted:
// GET /v1/models and /_switch/* (own mux routes, never reach requestHandler)
// and requests the client gate refuses with 401 before routing.
//
// Locking: reqMetrics.mu guards maps only and is never held across I/O,
// logging, or another lock, so a request stuck in a write or a log call can
// never hold it. The render path takes only this lock. First-byte stamps are
// atomics, written by the response writer without any lock.

import (
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const reqMetricPrefix = "switch_proxy_"

// Bucket upper bounds in seconds. Duration covers multi-minute streamed
// answers; TTFB is dominated by upstream prefill.
var (
	reqDurationBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600}
	reqTTFBBuckets     = []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 30, 60}
)

type histo struct {
	counts []uint64 // per bucket, non-cumulative; len(bounds)+1, last is +Inf
	sum    float64
	n      uint64
}

func (h *histo) observe(bounds []float64, v float64) {
	if h.counts == nil {
		h.counts = make([]uint64, len(bounds)+1)
	}
	i := sort.SearchFloat64s(bounds, v) // first bound >= v: le is inclusive
	h.counts[i]++
	h.sum += v
	h.n++
}

type reqCode struct{ lane, code string }

type reqMetricsT struct {
	mu       sync.Mutex
	nextID   uint64
	inflight map[uint64]time.Time
	totals   map[reqCode]int64
	dur      map[string]*histo
	ttfb     map[string]*histo
}

func newReqMetrics() *reqMetricsT {
	return &reqMetricsT{
		inflight: map[uint64]time.Time{},
		totals:   map[reqCode]int64{},
		dur:      map[string]*histo{},
		ttfb:     map[string]*histo{},
	}
}

var (
	reqMetrics       = newReqMetrics()
	processStartTime = time.Now()
)

// begin registers a request as in flight from start.
func (rm *reqMetricsT) begin(start time.Time) uint64 {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.nextID++
	rm.inflight[rm.nextID] = start
	return rm.nextID
}

// inFlightCount is what the graceful-shutdown drain waits on (#126 D5): zero
// means no ROUTED request on either entry is still being served. Pass-through
// streams and tunnels are deliberately not counted here — they reconnect.
func (rm *reqMetricsT) inFlightCount() int {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return len(rm.inflight)
}

// end retires a request: it counts the request, observes its full duration
// and (if any byte reached the client) its time to first byte.
func (rm *reqMetricsT) end(id uint64, lane string, status int, start time.Time, firstByte int64, now time.Time) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	delete(rm.inflight, id)
	rm.totals[reqCode{lane, strconv.Itoa(status)}]++
	d := rm.dur[lane]
	if d == nil {
		d = &histo{}
		rm.dur[lane] = d
	}
	d.observe(reqDurationBuckets, now.Sub(start).Seconds())
	if firstByte != 0 {
		t := rm.ttfb[lane]
		if t == nil {
			t = &histo{}
			rm.ttfb[lane] = t
		}
		t.observe(reqTTFBBuckets, time.Duration(firstByte-start.UnixNano()).Seconds())
	}
}

// render writes the request-path exposition. Copies under the lock, formats
// after it.
func (rm *reqMetricsT) render(now time.Time) string {
	type hcopy struct {
		lane string
		h    histo
	}
	copyH := func(src map[string]*histo) []hcopy {
		out := make([]hcopy, 0, len(src))
		for lane, h := range src {
			c := *h
			c.counts = append([]uint64(nil), h.counts...)
			out = append(out, hcopy{lane, c})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].lane < out[j].lane })
		return out
	}
	rm.mu.Lock()
	inflight := len(rm.inflight)
	var oldest time.Time
	for _, t := range rm.inflight {
		if oldest.IsZero() || t.Before(oldest) {
			oldest = t
		}
	}
	codes := make([]reqCode, 0, len(rm.totals))
	totals := make(map[reqCode]int64, len(rm.totals))
	for k, v := range rm.totals {
		codes = append(codes, k)
		totals[k] = v
	}
	dur, ttfb := copyH(rm.dur), copyH(rm.ttfb)
	rm.mu.Unlock()

	sort.Slice(codes, func(i, j int) bool {
		if codes[i].lane != codes[j].lane {
			return codes[i].lane < codes[j].lane
		}
		return codes[i].code < codes[j].code
	})
	m := newMetricsBufPrefix(reqMetricPrefix)
	m.help("requests_in_flight", "gauge", "Requests inside requestHandler right now, including any blocked after streaming.")
	m.add("requests_in_flight", float64(inflight))
	age := 0.0
	if !oldest.IsZero() {
		age = now.Sub(oldest).Seconds()
	}
	m.help("oldest_in_flight_seconds", "gauge", "Age of the oldest in-flight request; 0 when none.")
	m.add("oldest_in_flight_seconds", age)
	m.help("requests_total", "counter", "Completed requests by lane and HTTP status code.")
	for _, c := range codes {
		m.add("requests_total", float64(totals[c]), "lane", c.lane, "code", c.code)
	}
	writeHist := func(name, help string, bounds []float64, hs []hcopy) {
		m.help(name, "histogram", help)
		for _, hc := range hs {
			var cum uint64
			for i, b := range bounds {
				cum += hc.h.counts[i]
				m.addSuffixed(name, "_bucket", float64(cum), "lane", hc.lane, "le", strconv.FormatFloat(b, 'f', -1, 64))
			}
			m.addSuffixed(name, "_bucket", float64(hc.h.n), "lane", hc.lane, "le", "+Inf")
			m.addSuffixed(name, "_sum", hc.h.sum, "lane", hc.lane)
			m.addSuffixed(name, "_count", float64(hc.h.n), "lane", hc.lane)
		}
	}
	writeHist("request_duration_seconds", "Full handler time per request, including the streamed body.", reqDurationBuckets, dur)
	writeHist("request_ttfb_seconds", "Time until the first response body byte was written to the client.", reqTTFBBuckets, ttfb)
	m.help("process_start_time_seconds", "gauge", "Unix time this process started; a change is a restart.")
	m.add("process_start_time_seconds", float64(processStartTime.UnixNano())/1e9)
	m.help("goroutines", "gauge", "Goroutines in this process.")
	m.add("goroutines", float64(runtime.NumGoroutine()))
	return m.b.String()
}

// firstByteWriter sits UNDER recorder (recorder itself must never grow a
// Write method — resptap_test.go) and stamps the first body byte handed to
// the client. Both the plain recorder's promoted Write and tapRecorder.Write
// reach it. Flush is pass-through and stateless: it runs on a timer
// goroutine.
type firstByteWriter struct {
	http.ResponseWriter
	first atomic.Int64 // UnixNano of the first Write with n > 0; 0 = none yet
}

func (f *firstByteWriter) Write(b []byte) (int, error) {
	n, err := f.ResponseWriter.Write(b)
	if n > 0 && f.first.Load() == 0 {
		f.first.CompareAndSwap(0, time.Now().UnixNano())
	}
	return n, err
}

func (f *firstByteWriter) Flush() {
	if fl, ok := f.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}

func (f *firstByteWriter) Unwrap() http.ResponseWriter { return f.ResponseWriter }
