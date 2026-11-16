package main

// e2e for the inbound strip (§5.5(5)) through the REAL handler stack.
//
// The point of these is the one property no unit test can show: the strip runs on
// the class branch, in EVERY mode including off, and what it produces is what the
// upstream actually receives — the same bytes that get fingerprinted, forwarded
// and replayed.

import (
	"bytes"
	"net/http"
	"testing"
)

// postClassBody posts an arbitrary body at a class id, so a test can put a
// previous assistant turn in the history.
func (env *e2eEnv) postClassBody(t *testing.T, session, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest("POST", env.proxy.URL+"/v1/messages", bytes.NewReader([]byte(body)))
	req.Header.Set("Authorization", "Bearer CLIENT")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("anthropic-beta", "oauth-2025-04-20")
	req.Header.Set("x-claude-code-session-id", session)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request failed: %v", err)
	}
	return resp
}

// notedHistory is a Claude Code-shaped body whose previous assistant turn carries
// the note as its own trailing block — what a block-faithful client hands back.
func notedHistory(model string) (noted, clean string) {
	head := `{"model":"` + model + `","max_tokens":16,"messages":[` +
		`{"role":"user","content":"why is it slow?"},` +
		`{"role":"assistant","content":[{"type":"text","text":"The upstream was degraded."}`
	tail := `]},{"role":"user","content":"and now?"}]}`
	note := `,{"type":"text","text":"\n\n` + noteSentinel +
		` answered by kimi/kimi-k3 — gpt/gpt-5.6-sol is degraded (open 4 min, 7/9 failed); traffic returns on its own once it passes a probe"}`
	return head + note + tail, head + tail
}

// The strip runs under SWITCH_HEALTH=off — which is what the e2e env is. This is
// the deliberate deviation from E0's "off is byte-identical": a note written
// under enforce must still come off after a rollback, or the first request after
// the flip carries a [switch-proxy] line into the upstream and into the
// prompt-cache prefix, where it stays.
func TestE2EStripRunsUnderHealthOff(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	if healthModeV != healthOff {
		t.Fatalf("fixture precondition: SWITCH_HEALTH is %v, want off", healthModeV)
	}

	_, strippedBefore := noteStripTotals()
	noted, clean := notedHistory("claude-frontier")
	resp := h.postClassBody(t, "sess-strip-off", noted)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}

	hit := h.hit(t) // the anthropic member (lane:fable, P1)
	if bytes.Contains(hit.body, noteSentinelBytes) {
		t.Fatalf("the note reached the upstream:\n%s", hit.body)
	}
	// The upstream body differs from `clean` only by the model rewrite the class
	// branch already does, so re-running that rewrite on `clean` must reproduce it
	// byte for byte: the strip cost exactly the note's bytes and nothing else.
	want := rewriteModel([]byte(clean), "claude-fable-5")
	if want == nil {
		t.Fatal("fixture: model rewrite failed")
	}
	if !bytes.Equal(hit.body, want) {
		t.Fatalf("upstream body is not the un-annotated history:\n got %s\nwant %s", hit.body, want)
	}
	// §6.2(3): the removal is counted against the class it happened on.
	_, strippedAfter := noteStripTotals()
	if got := strippedAfter["frontier"] - strippedBefore["frontier"]; got != 1 {
		t.Fatalf("notes_stripped_total{class=\"frontier\"} moved by %d, want 1", got)
	}
}

// A class request with no sentinel is untouched: the gate is one scan and the
// token walk never runs (§0.8).
func TestE2ENoSentinelIsUntouched(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()

	before, _ := noteStripTotals()
	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-strip-clean")
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	h.hit(t)
	after, _ := noteStripTotals()
	if after["frontier"] != before["frontier"] {
		t.Fatalf("token walk ran %d time(s) on a class request with no sentinel", after["frontier"]-before["frontier"])
	}
}

// The strip is scoped to the class branch. An account-lane id routes outside
// it, where no note is ever emitted, so nothing there is ours to take back — a
// sentinel on that path is the user's text and rides through. (Before
// quota-model C2 a named provider id was this test's vehicle; the lane is the
// non-class path that remains.)
func TestE2EStripIsScopedToClassRequests(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()

	noted, _ := notedHistory("claude-fable-5")
	resp := h.postClassBody(t, "sess-strip-lane", noted)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	hit := h.hit(t)
	if !bytes.Contains(hit.body, noteSentinelBytes) {
		t.Fatalf("lane body was stripped; it is not a class request:\n%s", hit.body)
	}
}
