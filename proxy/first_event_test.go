package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestParseFirstEventBudget(t *testing.T) {
	got := parseFirstEventBudget(" vision-app:10, fast:0.3, bad:x, :5, zero:0,nocolon")
	if len(got) != 2 || got["vision-app"] != 10*time.Second || got["fast"] != 300*time.Millisecond {
		t.Fatalf("parse = %v", got)
	}
	if len(parseFirstEventBudget("")) != 0 {
		t.Fatal("empty must be off")
	}
}

// Row 22 when our budget cancelled the hop; row 1 when the client's cancel
// reached it first, even if the budget fires after.
func TestClassifyFirstEventBudget(t *testing.T) {
	m := classMemberPref{ID: "gpt/gpt-6.1-sol"}
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errFirstEventBudget)
	out := classify(m, ctx, nil, context.Cause(ctx))
	if out.row != "22" || out.sig != sigDegraded || out.cause != "first_event_timeout" || out.evidence != evDeg || !out.final {
		t.Fatalf("budget cancel = %+v", out)
	}

	parent, pcancel := context.WithCancel(context.Background())
	hop, hcancel := context.WithCancelCause(parent)
	pcancel()
	hcancel(errFirstEventBudget)
	out = classify(m, hop, nil, context.Cause(hop))
	if out.row != "1" || out.sig != sigCancelled || out.evidence != evNone {
		t.Fatalf("client cancel first = %+v", out)
	}
}

type captureRT struct{ got *http.Request }

func (c *captureRT) RoundTrip(r *http.Request) (*http.Response, error) {
	c.got = r
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
}

// No budget for the class: the hop is the caller's request itself, unwrapped.
func TestTripFirstEventUnsetIsPassthrough(t *testing.T) {
	old := firstEventBudget
	firstEventBudget = map[string]time.Duration{"other": time.Second}
	t.Cleanup(func() { firstEventBudget = old })
	rt := &captureRT{}
	req, _ := http.NewRequest("POST", "http://codex.invalid/backend-api/codex/responses", strings.NewReader("{}"))
	req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("{}")), nil }
	cc := &classRoute{class: "vision-app", member: classMemberPref{ID: "gpt/gpt-6.1-sol"}}
	resp, err := (&failoverTransport{base: rt}).tripFirstEvent(req, reqInfo{classCtx: cc})
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("trip = %v, %v", resp, err)
	}
	if rt.got != req {
		t.Fatal("an unbudgeted class must send the request itself, not a clone")
	}
}

func armFirstEvent(t *testing.T, m map[string]time.Duration) {
	t.Helper()
	old := firstEventBudget
	firstEventBudget = m
	t.Cleanup(func() { firstEventBudget = old })
}

func firstEventSamples() int64 {
	health.mu.Lock()
	defer health.mu.Unlock()
	var n int64
	for k, v := range health.samples {
		if k.cause == "first_event_timeout" {
			n += v
		}
	}
	return n
}

func codexText(t *testing.T, resp *http.Response) string {
	t.Helper()
	var msg map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&msg); err != nil {
		t.Fatal(err)
	}
	c, _ := msg["content"].([]any)
	if len(c) == 0 {
		t.Fatalf("no content in %v", msg)
	}
	s, _ := c[0].(map[string]any)["text"].(string)
	return s
}

// Both stall shapes on the non-streaming path (a scheduled job): the first
// hop is abandoned at the budget, the stall line says how it failed, and the
// one resend to the same member answers the client.
func TestE2EFirstEventStallResends(t *testing.T) {
	for _, tc := range []struct {
		name   string
		noHead bool
		want   []string
	}{
		{"no_head", true, []string{"shape=no_head head=none status=none ids=none bytes=0",
			"reused=false was_idle=false idle=0s proto=http/1.1 remote=127.0.0.1:"}},
		{"head_no_event", false, []string{"shape=head_no_event head=", "status=200 ids=x-request-id=req-stall-1 bytes=0",
			"reused=false was_idle=false idle=0s proto=http/1.1 remote=127.0.0.1:"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, restore := classWithUsableGpt(t)
			defer restore()
			armHealth(t, newHealthStore(defaultHealthCfg()))
			armObserve(t)
			armFirstEvent(t, map[string]time.Duration{"frontier": 300 * time.Millisecond})
			logs := captureLogSync(t)
			h.codexStall.Store(1)
			h.codexStallHead.Store(tc.noHead)

			start := time.Now()
			resp := h.postStreamSess(t, "claude-frontier", false, "sess-stall-"+tc.name)
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				b, _ := io.ReadAll(resp.Body)
				t.Fatalf("status %d: %s", resp.StatusCode, b)
			}
			if got := codexText(t, resp); got != "codex ok" {
				t.Fatalf("resent reply = %q", got)
			}
			if el := time.Since(start); el < 300*time.Millisecond {
				t.Fatalf("answered in %s, before the budget could fire", el)
			}
			if got := resp.Header.Get("X-Switch-Health"); got != "walked" {
				t.Fatalf("X-Switch-Health = %q, want walked", got)
			}
			first, second := h.codexHitOrFail(t), h.codexHitOrFail(t)
			h.noCodexLeak(t)
			if first.auth != second.auth || first.acct != second.acct || first.session != second.session ||
				first.session == "" || !bytes.Equal(first.body, second.body) {
				t.Fatalf("the resend is not the same request:\n%+v\n%+v", first, second)
			}

			stall := waitLog(t, logs, "first-event stall")
			t.Log(stall)
			for _, w := range append(tc.want, "on class frontier member gpt/gpt-5.6 d=", "budget=300ms", "resending once") {
				if !strings.Contains(stall, w) {
					t.Fatalf("stall line lacks %q:\n%s", w, stall)
				}
			}
			served := waitLog(t, logs, "walked=retry")
			t.Log(served)
			d := strings.Fields(stall[strings.Index(stall, " d="):])[0]
			if !strings.Contains(served, "out=ok") || !strings.Contains(served, d+" ") {
				t.Fatalf("served line:\n%s", served)
			}
			if n := firstEventSamples(); n != 1 {
				t.Fatalf("first_event_timeout samples = %d, want 1", n)
			}
		})
	}
}

// The client gives up inside the budget: Rule 0 as today — cancelled, no
// stall line, no sample, no resend.
func TestE2EFirstEventClientCancelWhileWaiting(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()
	armHealth(t, newHealthStore(defaultHealthCfg()))
	armObserve(t)
	armFirstEvent(t, map[string]time.Duration{"frontier": 2 * time.Second})
	logs := captureLogSync(t)
	h.codexStall.Store(1)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	body := `{"model":"claude-frontier","max_tokens":16,"stream":false,"messages":[{"role":"user","content":"hi"}]}`
	req, _ := http.NewRequestWithContext(ctx, "POST", h.proxy.URL+"/v1/messages", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer CLIENT")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("anthropic-beta", "oauth-2025-04-20")
	req.Header.Set("x-claude-code-session-id", "sess-stall-cancel")
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
		t.Fatalf("client got a reply (%d) after cancelling", resp.StatusCode)
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("client error = %v", err)
	}
	h.codexHitOrFail(t)

	line := waitLog(t, logs, "out=cancelled")
	if !strings.Contains(line, "walked=none") {
		t.Fatalf("cancelled line:\n%s", line)
	}
	time.Sleep(100 * time.Millisecond)
	if strings.Contains(logs.String(), "first-event stall") {
		t.Fatalf("a client cancel logged a stall:\n%s", logs.String())
	}
	h.noCodexLeak(t)
	if n := firstEventSamples(); n != 0 {
		t.Fatalf("first_event_timeout samples = %d, want 0", n)
	}
}

// Budget unset: a stalled hop is waited on as today — no resend, no stall line.
func TestE2EFirstEventUnsetUnchanged(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()
	armHealth(t, newHealthStore(defaultHealthCfg()))
	armObserve(t)
	armFirstEvent(t, nil)
	logs := captureLogSync(t)

	resp := h.postStreamSess(t, "claude-frontier", false, "sess-stall-unset")
	if got := codexText(t, resp); got != "codex ok" {
		t.Fatalf("reply = %q", got)
	}
	resp.Body.Close()
	h.codexHitOrFail(t)
	line := waitLog(t, logs, "out=ok")
	if !strings.Contains(line, "walked=none") || strings.Contains(logs.String(), "first-event stall") {
		t.Fatalf("unset budget changed the request:\n%s", logs.String())
	}
	h.noCodexLeak(t)
	if n := firstEventSamples(); n != 0 {
		t.Fatalf("first_event_timeout samples = %d, want 0", n)
	}
}

func postClass(t *testing.T, h *e2eHosted, stream bool, sess string) string {
	t.Helper()
	resp := h.postStreamSess(t, "claude-frontier", stream, sess)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	return string(b)
}

var upHeadFirst = regexp.MustCompile(` up=head:([^,]+),first:([^,]+),gap:([^,]+),`)

// The in-budget path every vision-app request takes: the head is held until
// its first byte, the peeked byte is read again, and the reply, the served
// line and its up= record match the knob off — head is the head, first is the
// wait for the first event (here 300 ms after the head).
func TestE2EFirstEventInBudgetMatchesOff(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, on := range []bool{false, true} {
			name := map[bool]string{false: "non-stream", true: "stream"}[stream] + map[bool]string{false: "/off", true: "/on"}[on]
			t.Run(name, func(t *testing.T) {
				h, restore := classWithUsableGpt(t)
				defer restore()
				armHealth(t, newHealthStore(defaultHealthCfg()))
				armObserve(t)
				if on {
					armFirstEvent(t, map[string]time.Duration{"frontier": 2 * time.Second})
				} else {
					armFirstEvent(t, nil)
				}
				logs := captureLogSync(t)
				h.codexFirstMS.Store(300)

				body := postClass(t, h, stream, "sess-inbudget")
				if !strings.Contains(body, "codex") {
					t.Fatalf("reply lost the peeked byte:\n%s", body)
				}
				if stream && !strings.Contains(body, "message_stop") {
					t.Fatalf("stream reply incomplete:\n%s", body)
				}
				h.codexHitOrFail(t)
				h.noCodexLeak(t)
				line := waitLog(t, logs, "out=ok up=head:")
				t.Log(line)
				if !strings.Contains(line, "walked=none") || strings.Contains(logs.String(), "first-event stall") {
					t.Fatalf("in-budget hop was not served as-is:\n%s", logs.String())
				}
				m := upHeadFirst.FindStringSubmatch(line)
				if m == nil {
					t.Fatalf("no up= record:\n%s", line)
				}
				head, _ := time.ParseDuration(m[1])
				first, _ := time.ParseDuration(m[2])
				if head >= 250*time.Millisecond || first < 250*time.Millisecond {
					t.Fatalf("up= is not truthful (head %s, first %s; want head < 250ms, first >= 250ms):\n%s", head, first, line)
				}
			})
		}
	}
}

// A non-200 head inside the budget is no stall: it takes its normal path (here
// the quota walk), with no stall line, no sample and no resend.
func TestE2EFirstEventNon200HeadTakesNormalPath(t *testing.T) {
	h, restore := classWithUsableGpt(t)
	defer restore()
	armHealth(t, newHealthStore(defaultHealthCfg()))
	armObserve(t)
	armFirstEvent(t, map[string]time.Duration{"frontier": 2 * time.Second})
	logs := captureLogSync(t)
	h.codexQuota.Store(true)

	resp := h.postStreamSess(t, "claude-frontier", false, "sess-inbudget-429")
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	h.codexHitOrFail(t)
	h.noCodexLeak(t)
	line := waitLog(t, logs, " d=")
	if strings.Contains(logs.String(), "first-event stall") || strings.Contains(line, "walked=retry") {
		t.Fatalf("a 429 head was treated as a stall:\n%s", logs.String())
	}
	if n := firstEventSamples(); n != 0 {
		t.Fatalf("first_event_timeout samples = %d, want 0", n)
	}
}
