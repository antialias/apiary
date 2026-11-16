package main

// #122 e2e: SWITCH_HEALTH=observe through the real handler stack. The armed
// store must ACCUMULATE evidence keyed by served member while the client
// bytes stay byte-identical to the E0 goldens — observe is a tap, never a
// participant. (The E0 suite itself runs with health nil and pins `off`.)

import (
	"io"
	"strings"
	"testing"
	"time"
)

// waitRing polls for a sample count: the tap records on the proxy side of the
// body copy, a scheduling hair before the client sees EOF.
func waitRing(t *testing.T, h *healthStore, key string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if n := ringLen(h, key); n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ring for %s never reached %d samples (have %d)", key, want, ringLen(h, key))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestE2EObserveHealthyHopRecordsAndStaysByteIdentical(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	armObserve(t)

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-obs-healthy")
	defer resp.Body.Close()
	h.hit(t)

	// The E0-1 golden, unchanged with the tap installed.
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	wantIn := `{"type":"message","content":[{"type":"text","text":"ok"}]}`
	if string(b) != wantIn {
		t.Fatalf("client bytes drifted under observe:\n got %q\nwant %q", b, wantIn)
	}
	if got := clientHeaderNames(resp.Header); got != "Content-Length,Content-Type,Date,X-Switch-Decision,X-Switch-Health" {
		t.Fatalf("client header name set drifted under observe: %s", got)
	}

	waitRing(t, hs, "lane:fable", 1)
	st := snap(t, hs, "lane:fable")
	s := st.ring[0]
	if !s.ok || s.class != "frontier" || !strings.HasPrefix(s.reqID, "r-") {
		t.Fatalf("sample = %+v, want an ok sample for class frontier with a minted reqID", s)
	}
	mustState(t, hs, "lane:fable", hsHealthy)
}

func TestE2EObserveQuotaWalkWritesOnlyTheServingSample(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	armObserve(t)
	h.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"You have exceeded your weekly usage limit."}}`)
	h.deny429.Store("*")

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-obs-walk")
	defer resp.Body.Close()
	h.hit(t)       // fable incumbent: 429
	h.hit(t)       // swapped account: 429
	h.hostedHit(t) // kimi serves

	if resp.StatusCode != 200 {
		t.Fatalf("client status = %d, want kimi's 200", resp.StatusCode)
	}
	io.Copy(io.Discard, resp.Body)

	// Quota is evidence-NONE (§3 rows 13/12): the refused rungs write nothing.
	waitRing(t, hs, "kimi/kimi-k3", 1)
	if n := ringLen(hs, "lane:fable"); n != 0 {
		t.Fatalf("lane:fable holds %d samples — a 429 must not score as degradation", n)
	}
	s := snap(t, hs, "kimi/kimi-k3").ring[0]
	if !s.ok || s.class != "frontier" {
		t.Fatalf("kimi sample = %+v, want ok/frontier", s)
	}

	classAlertsN(t, h.e2eEnv, 2)
}
