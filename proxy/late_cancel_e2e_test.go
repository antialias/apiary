package main

// Rule 0's exception (§3) end to end, with the timing forced rather than
// hoped for: the passthrough upstream sends its whole reply and then holds the
// connection open, so the proxy cannot see the upstream's EOF before the
// client closes. The client reads to message_stop and hangs up; only the
// hang-up (the proxy cancelling the hop) ends the upstream's hold. A finished
// reply is scored by what it was; a hang-up mid-reply stays row 1.

import (
	"bufio"
	"strings"
	"testing"
	"time"
)

const lateCancelSSE = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"content\":[],\"usage\":{\"input_tokens\":3}}}\n\n" +
	"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
	"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"done\"}}\n\n" +
	"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
	"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n" +
	"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"

// lateCancelRun posts a streaming class request to the kimi member, reads the
// reply until a line contains until, closes, and waits for the proxy's
// request-ended line. It fails unless the upstream's hold was ended by the
// proxy hanging up — i.e. the tap could only classify after the cancel.
func lateCancelRun(t *testing.T, script, until, sess string) (*e2eHosted, *healthStore, string) {
	t.Helper()
	h, restore := newE2EClassOn(t, "kimi/kimi-k3", 1.0)
	t.Cleanup(restore)
	hs := newHealthStore(defaultHealthCfg())
	armHealth(t, hs)
	armObserve(t)
	logs := captureLogSync(t)
	h.kimiSSE.Store(script)
	h.kimiHold.Store(true)

	resp := h.postStreamSess(t, "claude-frontier", true, sess)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	br := bufio.NewReader(resp.Body)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("reply ended before %q: %v", until, err)
		}
		if strings.Contains(line, until) {
			break
		}
	}
	resp.Body.Close() // the client is done: the proxy sees the hang-up
	h.hostedHit(t)

	line := waitLog(t, logs, " out=")
	deadline := time.Now().Add(2 * time.Second)
	for !h.kimiHoldCut.Load() {
		if time.Now().After(deadline) {
			t.Fatal("the upstream's hold was not ended by the proxy's hang-up; the timing was not forced")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return h, hs, line
}

// A CI flake, forced: the reply finished (message_stop handed
// to the client), then the client closed. Row 5, out=ok, a healthy sample.
func TestE2ELateCancelAfterMessageStopScoresOK(t *testing.T) {
	_, hs, line := lateCancelRun(t, lateCancelSSE, `"type":"message_stop"`, "sess-late-cancel-ok")
	if !strings.Contains(line, " out=ok") {
		t.Fatalf("finished reply then hang-up logged:\n%s", line)
	}
	waitRing(t, hs, "kimi/kimi-k3", 1)
	if s := snap(t, hs, "kimi/kimi-k3").ring[0]; !s.ok {
		t.Fatalf("sample = %+v, want the healthy reading", s)
	}
}

// The control: the client hangs up after content but before message_stop.
// Rule 0 stands — out=cancelled, no sample.
func TestE2ELateCancelMidReplyStaysCancelled(t *testing.T) {
	script := lateCancelSSE[:strings.Index(lateCancelSSE, "event: message_delta")]
	_, hs, line := lateCancelRun(t, script, `"type":"content_block_stop"`, "sess-late-cancel-mid")
	if !strings.Contains(line, " out=cancelled") {
		t.Fatalf("mid-reply hang-up logged:\n%s", line)
	}
	time.Sleep(50 * time.Millisecond)
	if n := ringLen(hs, "kimi/kimi-k3"); n != 0 {
		t.Fatalf("a mid-reply cancel wrote %d samples, want 0", n)
	}
}
