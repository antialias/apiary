package main

// The served event (quota-model 65 §7): its encoder against the golden cases
// the brain tests too, its queue in reports.go, the codex cache-write rule,
// and each lane end to end through the real handler stack.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- the encoder against the golden ----

// servedFromGolden builds the servedEvent a golden case's event is the wire of.
func servedFromGolden(t *testing.T, m map[string]any) servedEvent {
	t.Helper()
	str := func(k string) *string {
		if v, ok := m[k].(string); ok {
			return &v
		}
		return nil
	}
	num := func(v any) *float64 {
		if f, ok := v.(float64); ok {
			return &f
		}
		return nil
	}
	rd := map[string]servedReading{}
	for name, v := range m["readings"].(map[string]any) {
		w := v.(map[string]any)
		rd[name] = servedReading{usedPct: num(w["used_pct"]), resetsAt: num(w["resets_at"])}
	}
	tk := m["tokens"].(map[string]any)
	n := func(k string) *int {
		if v, ok := tk[k].(float64); ok {
			i := int(v)
			return &i
		}
		return nil
	}
	return servedEvent{
		id: m["id"].(string), account: m["account"].(string), readAt: m["read_at"].(float64),
		headAt: m["head_at"].(float64), readings: rd,
		boot: m["boot"].(string), seq: int64(m["seq"].(float64)),
		consumer: str("consumer"), selection: str("selection"), member: str("member"),
		model: str("model"), speed: str("speed"), paid: m["paid"].(bool),
		tokens: servedTokens{input: n("input"), cacheWrite5m: n("cache_write_5m"),
			cacheWrite1h: n("cache_write_1h"), cacheWrite30m: n("cache_write_30m"),
			cacheWrite12h: n("cache_write_12h"), cacheWrite: n("cache_write"),
			cacheRead: n("cache_read"), output: n("output")},
		complete: m["complete"].(bool), why: str("why"), outputMax: goldenCount(m["output_max"]),
	}
}

// goldenCount is a golden number as an int, nil for null.
func goldenCount(v any) *int {
	if f, ok := v.(float64); ok {
		i := int(f)
		return &i
	}
	return nil
}

// Every applied case's event is exactly what wire() sends for it: the same
// keys, every one present, null where unknown, and the same values; head_at
// and readings (66 §1) on every case, the Anthropic, codex and Kimi ones and
// the incomplete ones alike.
func TestServedEncoderMatchesTheGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/served_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var g struct {
		Cases []struct {
			Event  map[string]any `json:"event"`
			Result string         `json:"result"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatal(err)
	}
	applied := 0
	for _, c := range g.Cases {
		if c.Result != "applied" {
			continue
		}
		applied++
		ev := servedFromGolden(t, c.Event)
		b, err := json.Marshal(ev.wire())
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		_ = json.Unmarshal(b, &got)
		if !reflect.DeepEqual(got, c.Event) {
			t.Errorf("%s:\n got %s\nwant %v", c.Event["id"], b, c.Event)
		}
	}
	if applied < 10 {
		t.Fatalf("only %d applied cases read", applied)
	}
}

// ---- the queue ----

// servedQueue is a copy of the served events waiting, oldest first.
func servedQueue() []servedEvent {
	lan.repMu.Lock()
	defer lan.repMu.Unlock()
	out := make([]servedEvent, len(lan.repServed))
	for i, q := range lan.repServed {
		out[i] = q.ev
	}
	return out
}

func servedOf(account string) servedEvent {
	return servedEvent{account: account, readAt: 1, complete: true}
}

// Events take seq from 1 in the order they end, under one boot; past the
// bound the oldest go and are counted; they follow readings and refusals in a
// batch; a refused one is dropped and counted, a retry stays.
func TestServedQueueOrderBoundAndBoot(t *testing.T) {
	t.Cleanup(resetQuotaState())
	reportServedMax = 3
	for i := 1; i <= 5; i++ {
		if seq := lan.reportServedEvent(servedOf(fmt.Sprint("a", i))); seq != int64(i) {
			t.Fatalf("event %d took seq %d", i, seq)
		}
	}
	q := servedQueue()
	if len(q) != 3 {
		t.Fatalf("%d queued, want the bound of 3", len(q))
	}
	ids := map[string]bool{}
	for i, ev := range q {
		if ev.seq != int64(i+3) || ev.account != fmt.Sprint("a", i+3) {
			t.Fatalf("queue[%d] = seq %d of %s, want the newest three in order", i, ev.seq, ev.account)
		}
		if ev.boot == "" || ev.boot != repBoot || ids[ev.id] || ev.id == "" {
			t.Fatalf("queue[%d] boot %q id %q, want this process's boot and a fresh id", i, ev.boot, ev.id)
		}
		ids[ev.id] = true
	}
	st := lan.reportsStatus()["served"].(map[string]any)
	if st["dropped"] != int64(2) || st["pending"] != 3 || st["seq"] != int64(5) || st["boot"] != repBoot {
		t.Fatalf("status served = %v, want 2 dropped, 3 pending, seq 5", st)
	}

	lan.reportHeaderReading("a", reading(time.Now(), map[string][2]*float64{"five_hour": {ptr(50), nil}}))
	b := lan.reportBatch()
	if len(b) != 4 || b[0].srv != nil {
		t.Fatalf("batch of %d, first served %v: want the reading first", len(b), b[0].srv != nil)
	}
	for i, s := range b[1:] {
		if s.srv == nil || s.srv.ev.seq != int64(i+3) {
			t.Fatalf("batch[%d] is not served event seq %d", i+1, i+3)
		}
	}
	url, posts := fakeReportsBrain(t, func(es []map[string]any) (int, any) {
		rs := make([]map[string]any, len(es))
		for i, e := range es {
			res := "applied"
			switch e["seq"] {
			case float64(3):
				res = "refused"
			case float64(4):
				res = "retry"
			}
			rs[i] = map[string]any{"id": e["id"], "result": res, "why": "because"}
		}
		return 200, map[string]any{"results": rs, "seq": 1}
	})
	if ok, more := lan.reportPost(context.Background(), tbase(url), "k"); ok || !more {
		t.Fatalf("ok %v more %v, want the retry left", ok, more)
	}
	if q := servedQueue(); len(q) != 1 || q[0].seq != 4 {
		t.Fatalf("queue = %+v, want only the retry", q)
	}
	st = lan.reportsStatus()["served"].(map[string]any)
	if st["refused"] != int64(1) || st["pending"] != 1 {
		t.Fatalf("status served = %v, want 1 refused, 1 pending", st)
	}
	p := posts()
	if len(p) != 1 || p[0][1]["kind"] != "served" || p[0][1]["boot"] != repBoot {
		t.Fatalf("posted %v", p)
	}
}

// ---- codex cache writes ----

func codexResp(details *codexInputDetails, reasoning int) *codexRespObj {
	u := &codexUsageObj{InputTokens: 10000, OutputTokens: 500, InputTokensDetails: details}
	if reasoning > 0 {
		u.OutputTokensDetails = &struct {
			ReasoningTokens int `json:"reasoning_tokens"`
		}{reasoning}
	}
	return &codexRespObj{Model: "gpt-6.1-sol", Usage: u}
}

func intp(i int) *int { return &i }

func fmtp(p *int) string {
	if p == nil {
		return "null"
	}
	return fmt.Sprint(*p)
}

// A codex cache write goes by lifetime: as the details give it, else all of
// it at 30 minutes (the proxy never asks for another), else unknown. Output
// keeps its reasoning; input is the uncached, unwritten rest.
func TestServedCodexCacheWrites(t *testing.T) {
	cases := []struct {
		name                           string
		details                        *codexInputDetails
		in, read, w30, w12, cw, output string
	}{
		{"by lifetime", &codexInputDetails{CachedTokens: 5000, CacheWriteTokens: intp(3000),
			CacheWriteTokensDetails: &codexCacheWriteSplit{TTL30m: intp(2000), TTL12h: intp(1000)}},
			"2000", "5000", "2000", "1000", "null", "500"},
		{"total only", &codexInputDetails{CachedTokens: 5000, CacheWriteTokens: intp(3000)},
			"2000", "5000", "3000", "0", "null", "500"},
		{"a reported zero", &codexInputDetails{CachedTokens: 5000, CacheWriteTokens: intp(0)},
			"5000", "5000", "0", "0", "null", "500"},
		{"no cache write reported", &codexInputDetails{CachedTokens: 5000},
			"5000", "5000", "null", "null", "null", "500"},
		{"no details", nil, "10000", "null", "null", "null", "null", "500"},
		// Clamped to the prompt, in order: input + read + 30m + 12h never
		// exceed input_tokens (10000).
		{"lifetimes past the prompt", &codexInputDetails{CachedTokens: 5000, CacheWriteTokens: intp(8000),
			CacheWriteTokensDetails: &codexCacheWriteSplit{TTL30m: intp(4000), TTL12h: intp(4000)}},
			"0", "5000", "4000", "1000", "null", "500"},
		{"total past the prompt", &codexInputDetails{CachedTokens: 9000, CacheWriteTokens: intp(3000)},
			"0", "9000", "1000", "0", "null", "500"},
		{"one lifetime given", &codexInputDetails{CachedTokens: 0, CacheWriteTokens: intp(3000),
			CacheWriteTokensDetails: &codexCacheWriteSplit{TTL12h: intp(3000)}},
			"7000", "0", "null", "3000", "null", "500"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, cp := withServedCap(context.Background())
			cp.request("gpt-6.1-sol", reqFacts{}, true)
			cp.hop("codex-1", 200, func(string) servedHopFacts { return servedHopFacts{kind: servedCodex} })
			cp.noteCodex(codexResp(c.details, 120))
			cp.mu.Lock()
			ev := cp.eventLocked()
			cp.mu.Unlock()
			tk := ev.tokens
			got := []string{fmtp(tk.input), fmtp(tk.cacheRead), fmtp(tk.cacheWrite30m), fmtp(tk.cacheWrite12h), fmtp(tk.cacheWrite), fmtp(tk.output)}
			want := []string{c.in, c.read, c.w30, c.w12, c.cw, c.output}
			if !reflect.DeepEqual(got, want) || tk.cacheWrite5m != nil || tk.cacheWrite1h != nil || !ev.complete {
				t.Fatalf("in/read/30m/12h/total/out = %v, want %v (complete %v)", got, want, ev.complete)
			}
		})
	}
}

// ---- end to end ----

// servedPosted is every served event the fake brain has been sent, as sent.
func servedPosted(env *e2eEnv) []map[string]any {
	env.brainMu.Lock()
	defer env.brainMu.Unlock()
	var out []map[string]any
	for _, e := range env.reports {
		if e["kind"] == reportServed {
			out = append(out, e)
		}
	}
	return out
}

// waitServed waits up to 3 s for the running sender to have posted n served
// events to the fake brain, and returns them as they went on the wire.
func waitServed(t *testing.T, env *e2eEnv, n int) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		got := servedPosted(env)
		if len(got) >= n || time.Now().After(deadline) {
			if len(got) != n {
				t.Fatalf("%d served events posted, want %d: %v", len(got), n, got)
			}
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// noServed checks, a moment after the response, that nothing was queued or
// posted.
func noServed(t *testing.T, env *e2eEnv) {
	t.Helper()
	time.Sleep(100 * time.Millisecond)
	reportsSettle()
	if q, p := servedQueue(), servedPosted(env); len(q) != 0 || len(p) != 0 {
		t.Fatalf("served events queued %v, posted %v; want none", q, p)
	}
}

// wireTokens is a posted event's tokens, every key in order.
func wireTokens(ev map[string]any) string {
	tk, _ := ev["tokens"].(map[string]any)
	if len(tk) != 8 {
		return fmt.Sprintf("tokens with %d keys: %v", len(tk), tk)
	}
	v := func(k string) string {
		if tk[k] == nil {
			return "null"
		}
		return fmt.Sprint(tk[k])
	}
	return fmt.Sprintf("in=%s 5m=%s 1h=%s 30m=%s 12h=%s cw=%s read=%s out=%s",
		v("input"), v("cache_write_5m"), v("cache_write_1h"), v("cache_write_30m"),
		v("cache_write_12h"), v("cache_write"), v("cache_read"), v("output"))
}

// wireHas checks a posted event's top-level fields; nil wants null.
func wireHas(t *testing.T, ev map[string]any, want map[string]any) {
	t.Helper()
	if len(ev) != 18 {
		t.Fatalf("event has %d keys, want 18: %v", len(ev), ev)
	}
	for k, w := range want {
		if fmt.Sprint(ev[k]) != fmt.Sprint(w) {
			t.Fatalf("%s = %v, want %v, in %v", k, ev[k], w, ev)
		}
	}
}

// postBody posts a raw /v1/messages body as the lane tests' client does.
func (env *e2eEnv) postBody(t *testing.T, path, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest("POST", env.proxy.URL+path, bytes.NewReader([]byte(body)))
	req.Header.Set("Authorization", "Bearer CLIENT")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("anthropic-beta", "oauth-2025-04-20")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request failed: %v", err)
	}
	return resp
}

const servedAnthStart = "event: message_start\n" +
	`data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-fable-5-20261101",` +
	`"usage":{"input_tokens":1200,"cache_creation_input_tokens":30500,"cache_read_input_tokens":250000,` +
	`"cache_creation":{"ephemeral_5m_input_tokens":500,"ephemeral_1h_input_tokens":30000},"output_tokens":1}}}` + "\n\n" +
	"event: content_block_delta\n" +
	`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}` + "\n\n"

const servedAnthEnd = "event: message_delta\n" +
	`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":900}}` + "\n\n" +
	"event: message_stop\n" + `data: {"type":"message_stop"}` + "\n\n"

// An Anthropic lane's stream: its usage by the 5m/1h split, the final output
// from message_delta, the model it answered with, the speed asked; the log
// line names the event and the account.
func TestE2EServedAnthropicStream(t *testing.T) {
	t.Cleanup(resetQuotaState())
	env, restore := newE2E(t)
	defer restore()
	env.anthSSE.Store(servedAnthStart + servedAnthEnd)
	logs := captureLogSync(t)
	resp := env.postBody(t, "/v1/messages",
		`{"model":"claude-fable-5[1m]","max_tokens":16,"stream":true,"speed":"fast","messages":[{"role":"user","content":"hi"}]}`)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	account := native
	if env.hit(t).auth == "Bearer "+e2eToken(other) {
		account = other
	}
	ev := waitServed(t, env, 1)[0]
	wireHas(t, ev, map[string]any{"account": account, "selection": "claude-fable-5", "member": "lane:fable",
		"model": "claude-fable-5-20261101", "speed": "fast", "paid": false, "seq": 1, "boot": repBoot,
		"consumer": nil, "complete": true, "why": nil})
	if got := wireTokens(ev); got != "in=1200 5m=500 1h=30000 30m=null 12h=null cw=null read=250000 out=900" {
		t.Fatalf("tokens %s", got)
	}
	line := waitLog(t, logs, "lane=fable")
	if !strings.Contains(line, " srv=1 acct="+account+" ") {
		t.Fatalf("log line:\n%s", line)
	}
	// The stopgap rate-limit tags went in 66 §11's push 3.
	for _, gone := range []string{" rid=", " rlat=", " rl="} {
		if strings.Contains(line, gone) {
			t.Fatalf("log line still carries %q:\n%s", gone, line)
		}
	}
}

// A stream the upstream cut, and one that ended cleanly without its final
// usage: each is counted, complete false, with no output, and carries its
// head's readings: the head arrived though the final usage did not.
func TestE2EServedCutStream(t *testing.T) {
	for _, abort := range []bool{true, false} {
		t.Run(map[bool]string{true: "cut", false: "ended early"}[abort], func(t *testing.T) {
			t.Cleanup(resetQuotaState())
			env, restore := newE2E(t)
			defer restore()
			env.anthSSE.Store(servedAnthStart)
			env.anthAbort.Store(abort)
			env.anthHeaders.Store(map[string]string{
				"anthropic-ratelimit-unified-7d-status":      "allowed",
				"anthropic-ratelimit-unified-7d-utilization": "0.12",
				"anthropic-ratelimit-unified-7d-reset":       "1794326400",
			})
			sent := time.Now()
			logs := captureLogSync(t)
			resp := env.postBody(t, "/v1/messages",
				`{"model":"claude-fable-5","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			ev := waitServed(t, env, 1)[0]
			// The request's max_tokens bounds what it drew (66 §1).
			wireHas(t, ev, map[string]any{"complete": false, "speed": "standard", "output_max": 16})
			headAgrees(t, ev, sent)
			want := map[string]any{"seven_day": map[string]any{"used_pct": 12.0, "resets_at": 1794326400.0}}
			if !reflect.DeepEqual(ev["readings"], want) {
				t.Fatalf("readings %v, want %v", ev["readings"], want)
			}
			if why, _ := ev["why"].(string); why == "" {
				t.Fatalf("why = %v, want the reason", ev["why"])
			}
			if got := wireTokens(ev); got != "in=1200 5m=500 1h=30000 30m=null 12h=null cw=null read=250000 out=null" {
				t.Fatalf("tokens %s", got)
			}
			if abort {
				// The proxy's copy failed: the line still names the event.
				if line := waitLog(t, logs, " ABORTED"); !strings.Contains(line, " ABORTED srv=1 acct=") {
					t.Fatalf("log line:\n%s", line)
				}
			}
		})
	}
}

// The bound comes from the client's own body (66 §1): its max_tokens, unless
// it names a server-side tool, whose results the provider adds to the input
// after the answer's start; the body reaches the upstream unchanged.
func TestE2EServedBoundFromTheBody(t *testing.T) {
	cases := []struct {
		name, extra string
		want        any
	}{
		{"client tools", `,"max_tokens":64000,"tools":[{"name":"Read","input_schema":{}}]`, 64000},
		{"web search", `,"max_tokens":64000,"tools":[{"type":"web_search_20250305","name":"web_search"}]`, nil},
		{"mcp servers", `,"max_tokens":64000,"mcp_servers":[{"type":"url","url":"https://x","name":"x"}]`, nil},
		{"no max_tokens", ``, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Cleanup(resetQuotaState())
			env, restore := newE2E(t)
			defer restore()
			env.anthSSE.Store(servedAnthStart)
			env.anthAbort.Store(true)
			body := `{"model":"claude-fable-5","stream":true` + c.extra + `,"messages":[{"role":"user","content":"hi"}]}`
			resp := env.postBody(t, "/v1/messages", body)
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			ev := waitServed(t, env, 1)[0]
			wireHas(t, ev, map[string]any{"complete": false, "output_max": c.want})
			if got := string(env.hit(t).body); !strings.Contains(got, c.extra) {
				t.Fatalf("the upstream got %s, want the client's %s", got, c.extra)
			}
		})
	}
}

// A client that leaves mid-answer: the event's why is its leaving, and the
// request's max_tokens bounds what it drew (66 §1).
func TestE2EServedBoundClientLeft(t *testing.T) {
	t.Cleanup(resetQuotaState())
	env, restore := newE2E(t)
	defer restore()
	env.anthSSE.Store(servedAnthStart + servedAnthEnd)
	env.anthGap.Store(300 * time.Millisecond)
	resp := env.postBody(t, "/v1/messages",
		`{"model":"claude-fable-5","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	buf := make([]byte, 64<<10)
	var got strings.Builder
	for !strings.Contains(got.String(), "message_start") {
		n, err := resp.Body.Read(buf)
		got.Write(buf[:n])
		if err != nil {
			t.Fatalf("stream ended before its start: %v\n%s", err, got.String())
		}
	}
	resp.Body.Close() // the client leaves after the start
	ev := waitServed(t, env, 1)[0]
	wireHas(t, ev, map[string]any{"complete": false, "why": servedWhyLeft, "output_max": 16})
	if got := wireTokens(ev); got != "in=1200 5m=500 1h=30000 30m=null 12h=null cw=null read=250000 out=null" {
		t.Fatalf("tokens %s", got)
	}
}

// Kimi speaks Anthropic's wire, but what its stream's start states is not
// known: a cut Kimi stream carries no bound (66 §1).
func TestE2EServedKimiNoBound(t *testing.T) {
	t.Cleanup(resetQuotaState())
	h, restore := newE2EClassOn(t, "kimi/kimi-k3", 1.0)
	defer restore()
	h.kimiSSE.Store(sseFixtureScaled[:strings.Index(sseFixtureScaled, "event: message_delta")])
	resp := h.postClassBody(t, "sess-kimi-cut",
		`{"model":"claude-frontier","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	ev := waitServed(t, h.e2eEnv, 1)[0]
	wireHas(t, ev, map[string]any{"account": "kimi", "complete": false, "why": servedWhyCut, "output_max": nil})
	if in, _ := ev["tokens"].(map[string]any)["input"].(float64); in == 0 {
		t.Fatalf("the cut stream's start was not read: %v", ev["tokens"])
	}
}

// Through a class, the body the member's lane sends has its model rewritten
// and keeps the client's max_tokens, so the bound is what the upstream was
// asked for (66 §1).
func TestE2EServedBoundThroughAClass(t *testing.T) {
	t.Cleanup(resetQuotaState())
	h, restore := newE2EClassOn(t, "lane:fable", 1.0)
	defer restore()
	h.anthSSE.Store(servedAnthStart)
	h.anthAbort.Store(true)
	resp := h.postClassBody(t, "sess-bound-1",
		`{"model":"claude-frontier","max_tokens":48000,"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	hit := h.hit(t)
	if hit.model != "claude-fable-5" || !strings.Contains(string(hit.body), `"max_tokens":48000`) {
		t.Fatalf("the upstream got model %q and body %s", hit.model, hit.body)
	}
	ev := waitServed(t, h.e2eEnv, 1)[0]
	wireHas(t, ev, map[string]any{"complete": false, "selection": "claude-frontier", "member": "lane:fable",
		"output_max": 48000})
}

// The bound's rule at the event (66 §1): only a request Anthropic served whose
// client hung up or whose stream was cut, with its input stated and no output
// count, carries output_max; anything else, null.
func TestServedBoundRule(t *testing.T) {
	in, out := 1200, 900
	cases := []struct {
		name string
		mut  func(c *servedCap)
		want bool
	}{
		{"the client left", func(c *servedCap) { c.why = servedWhyLeft }, true},
		{"the stream was cut", func(c *servedCap) {}, true},
		{"complete", func(c *servedCap) { c.final, c.usage.OutputTokens = true, &out }, false},
		{"an in-band error", func(c *servedCap) { c.errWhy = "overloaded_error" }, false},
		{"an in-band error after the final usage", func(c *servedCap) {
			c.final, c.usage.OutputTokens, c.errWhy = true, &out, "overloaded_error"
		}, false},
		{"no usage", func(c *servedCap) { c.why = servedWhyNoUsage }, false},
		{"encoded", func(c *servedCap) { c.why = servedWhyEncoded }, false},
		{"a server tool", func(c *servedCap) { c.serverTools = true }, false},
		{"no max_tokens", func(c *servedCap) { c.outputMax = nil }, false},
		{"no input count", func(c *servedCap) { c.usage.InputTokens = nil }, false},
		{"lossy", func(c *servedCap) { c.why = servedWhyLossy }, false},
		{"a usage line too long", func(c *servedCap) { c.why = servedWhyTooLarge }, false},
		{"another provider on Anthropic's wire (Kimi)", func(c *servedCap) { c.anthropic = false }, false},
		{"codex", func(c *servedCap) { c.kind = servedCodex }, false},
		{"an image", func(c *servedCap) { c.kind = servedImage }, false},
		{"another wire", func(c *servedCap) { c.kind = servedOtherWire }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			max := 32000
			c := &servedCap{armed: true, kind: servedPassthrough, anthropic: true, account: native, outputMax: &max}
			c.usage.InputTokens = &in
			tc.mut(c)
			ev := c.eventLocked()
			switch {
			case tc.want && (ev.outputMax == nil || *ev.outputMax != 32000):
				t.Fatalf("output_max = %v, want 32000 (why %v)", ev.outputMax, ev.why)
			case !tc.want && ev.outputMax != nil:
				t.Fatalf("output_max = %d, want null (why %v)", *ev.outputMax, ev.why)
			}
			if ev.outputMax != nil {
				max = 1 // the event holds its own copy
				if *ev.outputMax != 32000 {
					t.Fatal("output_max shares the capture's int")
				}
			}
		})
	}
}

// A refusal draws nothing, and count_tokens is not inference: no event.
func TestE2EServedNoneForRefusalOrCount(t *testing.T) {
	t.Cleanup(resetQuotaState())
	env, restore := newE2E(t)
	defer restore()
	resp := env.postBody(t, "/v1/messages/count_tokens",
		`{"model":"claude-fable-5","messages":[{"role":"user","content":"hi"}]}`)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("count_tokens status %d", resp.StatusCode)
	}
	noServed(t, env)
	env.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"rate limited"}}`)
	env.deny429.Store("*")
	resp = env.postBody(t, "/v1/messages",
		`{"model":"claude-fable-5","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode < 400 {
		t.Fatalf("status %d, want the refusal", resp.StatusCode)
	}
	noServed(t, env)
}

// Codex through a class member at ratio 0.5, aggregated and streamed: the
// tokens are the upstream's, unscaled, output with its reasoning, the cache
// write by lifetime.
func TestE2EServedCodexUnscaled(t *testing.T) {
	t.Cleanup(resetQuotaState())
	h, restore := newE2EClassOn(t, "gpt/gpt-5.6", 0.5)
	defer restore()
	h.codexSSE.Store(strings.Replace(defaultCodexSSE,
		`"usage":{"input_tokens":11,"output_tokens":5}`,
		`"usage":{"input_tokens":9000,"output_tokens":700,"input_tokens_details":{"cached_tokens":5000,`+
			`"cache_write_tokens":3000,"cache_write_tokens_details":{"ttl_30m_tokens":2000,"ttl_12h_tokens":1000}},`+
			`"output_tokens_details":{"reasoning_tokens":300}}`, 1))
	for _, stream := range []bool{false, true} {
		resp := h.postStreamSess(t, "claude-frontier", stream, "sess-served-codex")
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("stream %v: status %d", stream, resp.StatusCode)
		}
	}
	for _, ev := range waitServed(t, h.e2eEnv, 2) {
		wireHas(t, ev, map[string]any{"account": "gpt", "member": "gpt/gpt-5.6", "selection": "claude-frontier",
			"model": "gpt-5.6", "speed": "standard", "complete": true, "why": nil})
		if got := wireTokens(ev); got != "in=1000 5m=null 1h=null 30m=2000 12h=1000 cw=null read=5000 out=700" {
			t.Fatalf("tokens %s", got)
		}
	}
}

// Kimi through a class member at ratio 0.5: what it reported, unscaled, and
// null for what it did not (no 5m/1h split, so its cache write is a total).
func TestE2EServedKimiUnscaled(t *testing.T) {
	t.Cleanup(resetQuotaState())
	h, restore := newE2EClassOn(t, "kimi/kimi-k3", 0.5)
	defer restore()
	h.kimiSSE.Store(sseFixtureScaled)
	body := `{"model":"claude-frontier","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}`
	req, _ := http.NewRequest("POST", h.proxy.URL+"/v1/messages", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer client-bearer")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(raw), `"input_tokens":2000`) {
		t.Fatalf("the client was not given scaled usage:\n%s", raw)
	}
	ev := waitServed(t, h.e2eEnv, 1)[0]
	wireHas(t, ev, map[string]any{"account": "kimi", "member": "kimi/kimi-k3", "model": "kimi-k3",
		"speed": "standard", "complete": true})
	if got := wireTokens(ev); got != "in=1000 5m=null 1h=null 30m=null 12h=null cw=200 read=400 out=50" {
		t.Fatalf("tokens %s", got)
	}
}

// ---- in-band errors, aborts, failover, JSON, concurrency ----

// codexFailedSSE is a codex stream that fails in-band with code, after a
// text delta when withOutput, its failed response carrying usage.
func codexFailedSSE(code string, withOutput bool) string {
	s := "event: response.created\n" +
		`data: {"type":"response.created","response":{"id":"resp_1","model":"gpt-5.6"}}` + "\n\n"
	if withOutput {
		s += "event: response.output_item.added\n" +
			`data: {"type":"response.output_item.added","item":{"type":"message"}}` + "\n\n" +
			"event: response.output_text.delta\n" +
			`data: {"type":"response.output_text.delta","delta":"partial "}` + "\n\n"
	}
	return s + "event: response.failed\n" +
		`data: {"type":"response.failed","response":{"id":"resp_1","model":"gpt-5.6","status":"failed",` +
		`"error":{"code":"` + code + `","message":"it failed"},"usage":{"input_tokens":900,"output_tokens":40}}}` + "\n\n"
}

// A codex failure inside its 2xx: a 4xx before any output is a refusal or a
// request error (no event); a server error, before or after output, goes
// incomplete with the usage it reported and the error named.
func TestE2EServedCodexInBandErrors(t *testing.T) {
	cases := []struct {
		name, code string
		output     bool
		stream     bool
		event      bool
	}{
		{"usage_limit_reached streamed", "usage_limit_reached", false, true, false},
		{"usage_limit_reached aggregated", "usage_limit_reached", false, false, false},
		{"context_length_exceeded aggregated", "context_length_exceeded", false, false, false},
		{"context_length_exceeded streamed", "context_length_exceeded", false, true, false},
		{"server_error before output", "server_error", false, false, true},
		{"server_error before output streamed", "server_error", false, true, true},
		{"server_error after output", "server_error", true, true, true},
		{"usage_limit_reached after output", "usage_limit_reached", true, true, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Cleanup(resetQuotaState())
			h, restore := newE2EClassOn(t, "gpt/gpt-5.6", 1.0)
			defer restore()
			h.codexSSE.Store(codexFailedSSE(c.code, c.output))
			resp := h.postStreamSess(t, "claude-frontier", c.stream, "sess-served-inband")
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if !c.event {
				noServed(t, h.e2eEnv)
				return
			}
			ev := waitServed(t, h.e2eEnv, 1)[0]
			wireHas(t, ev, map[string]any{"complete": false, "why": "error in-band: " + c.code})
			if got := wireTokens(ev); got != "in=900 5m=null 1h=null 30m=null 12h=null cw=null read=null out=40" {
				t.Fatalf("tokens %s", got)
			}
		})
	}
}

// An Anthropic error mid-stream: overloaded after output goes incomplete
// with the error named; a rate limit before any output sends nothing.
func TestE2EServedAnthropicInBandErrors(t *testing.T) {
	errLine := func(typ string) string {
		return "event: error\n" + `data: {"type":"error","error":{"type":"` + typ + `","message":"x"}}` + "\n\n"
	}
	startOnly := servedAnthStart[:strings.Index(servedAnthStart, "event: content_block_delta")]
	for _, c := range []struct {
		name, script string
		event        bool
	}{
		{"overloaded after output", servedAnthStart + errLine("overloaded_error"), true},
		{"overloaded before output", startOnly + errLine("overloaded_error"), true},
		{"rate limited before output", startOnly + errLine("rate_limit_error"), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Cleanup(resetQuotaState())
			env, restore := newE2E(t)
			defer restore()
			env.anthSSE.Store(c.script)
			resp := env.postBody(t, "/v1/messages",
				`{"model":"claude-fable-5","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if !c.event {
				noServed(t, env)
				return
			}
			ev := waitServed(t, env, 1)[0]
			wireHas(t, ev, map[string]any{"complete": false, "why": "error in-band: overloaded_error"})
			if got := wireTokens(ev); got != "in=1200 5m=500 1h=30000 30m=null 12h=null cw=null read=250000 out=null" {
				t.Fatalf("tokens %s", got)
			}
		})
	}
}

// A client that leaves a codex stream: the event says it left, though the
// translator, on its own goroutine, says so after the door has finished.
func TestE2EServedCodexClientLeft(t *testing.T) {
	t.Cleanup(resetQuotaState())
	h, restore := newE2EClassOn(t, "gpt/gpt-5.6", 1.0)
	defer restore()
	h.codexSSE.Store(defaultCodexSSE[:strings.Index(defaultCodexSSE, "event: response.output_item.done")])
	h.codexHold.Store(true)
	resp := h.postStreamSess(t, "claude-frontier", true, "sess-served-left")
	buf := make([]byte, 64<<10)
	var got strings.Builder
	for !strings.Contains(got.String(), "codex ") {
		n, err := resp.Body.Read(buf)
		got.Write(buf[:n])
		if err != nil {
			t.Fatalf("stream ended before its text: %v\n%s", err, got.String())
		}
	}
	resp.Body.Close() // the client leaves mid-stream
	ev := waitServed(t, h.e2eEnv, 1)[0]
	wireHas(t, ev, map[string]any{"complete": false, "why": servedWhyLeft})
}

// A non-streamed Anthropic reply: complete, with its split and its output.
func TestE2EServedAnthropicJSON(t *testing.T) {
	t.Cleanup(resetQuotaState())
	env, restore := newE2E(t)
	defer restore()
	env.anthJSON.Store(`{"id":"msg_1","type":"message","role":"assistant","model":"claude-fable-5-20261101",` +
		`"content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":300,` +
		`"cache_creation_input_tokens":4100,"cache_read_input_tokens":9000,` +
		`"cache_creation":{"ephemeral_5m_input_tokens":100,"ephemeral_1h_input_tokens":4000},"output_tokens":77}}`)
	resp := env.post(t, "claude-fable-5", "CLIENT")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	ev := waitServed(t, env, 1)[0]
	wireHas(t, ev, map[string]any{"complete": true, "why": nil, "model": "claude-fable-5-20261101"})
	if got := wireTokens(ev); got != "in=300 5m=100 1h=4000 30m=null 12h=null cw=null read=9000 out=77" {
		t.Fatalf("tokens %s", got)
	}
}

// Failover then serve: the refused hop sends nothing, and the one event is
// the answering account's, its readings that account's head's: native's own
// 7d, not the refused hop's 7d at 89 % or its rejected 7d_oi.
func TestE2EServedFailoverOneEvent(t *testing.T) {
	t.Cleanup(resetQuotaState())
	env, restore := newE2E(t)
	defer restore()
	reset := env.capturedFable429(t, other)
	by := map[string]map[string]string{e2eToken(native): {
		"anthropic-ratelimit-unified-status":         "allowed",
		"anthropic-ratelimit-unified-7d-status":      "allowed",
		"anthropic-ratelimit-unified-7d-utilization": "0.42",
		"anthropic-ratelimit-unified-7d-reset":       strconv.FormatInt(reset.Unix(), 10),
	}}
	for k, v := range env.anthHeadersFor.Load().(map[string]map[string]string) {
		by[k] = v
	}
	env.anthHeadersFor.Store(by)
	env.deny429.Store(e2eToken(other))
	env.anthSSE.Store(servedAnthStart + servedAnthEnd)
	resp := env.postBody(t, "/v1/messages",
		`{"model":"claude-fable-5","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if first, second := env.hit(t), env.hit(t); first.auth != "Bearer "+e2eToken(other) || second.auth != "Bearer "+e2eToken(native) {
		t.Fatalf("hops %q then %q, want the refusal then the serve", first.auth, second.auth)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want the failover's 200", resp.StatusCode)
	}
	ev := waitServed(t, env, 1)[0]
	wireHas(t, ev, map[string]any{"account": native, "complete": true, "seq": 1})
	want := map[string]any{"seven_day": map[string]any{"used_pct": 42.0, "resets_at": float64(reset.Unix())}}
	if !reflect.DeepEqual(ev["readings"], want) {
		t.Fatalf("readings %v, want the serving hop's %v", ev["readings"], want)
	}
	time.Sleep(100 * time.Millisecond)
	if n := len(servedPosted(env)); n != 1 {
		t.Fatalf("%d served events, want 1", n)
	}
}

// Concurrent requests: each one's log line names the seq its event went
// under. The line is told apart by its client bearer, the event by the
// model its request named.
func TestE2EServedLogSeqUnderConcurrency(t *testing.T) {
	t.Cleanup(resetQuotaState())
	env, restore := newE2E(t)
	defer restore()
	logs := captureLogSync(t)
	const n = 16
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := fmt.Sprintf(`{"model":"claude-fable-5-c%d","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`, i)
			req, _ := http.NewRequest("POST", env.proxy.URL+"/v1/messages", strings.NewReader(body))
			req.Header.Set("Authorization", fmt.Sprintf("Bearer CLIENT-%d", i))
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Error(err)
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()
	}
	wg.Wait()
	for range n {
		env.hit(t)
	}
	seqOf := map[string]string{} // model -> seq
	for _, ev := range waitServed(t, env, n) {
		seqOf[fmt.Sprint(ev["model"])] = fmt.Sprint(ev["seq"])
	}
	for i := range n {
		line := waitLog(t, logs, "bearer="+fp(fmt.Sprintf("Bearer CLIENT-%d", i)))
		want := seqOf[fmt.Sprintf("claude-fable-5-c%d", i)]
		if want == "" || !strings.Contains(line, " srv="+want+" ") {
			t.Fatalf("request %d: event seq %q, line:\n%s", i, want, line)
		}
	}
}

// An event this side cannot encode is dropped and counted apart from the
// brain's refusals.
func TestServedUnencodableCountedApart(t *testing.T) {
	t.Cleanup(resetQuotaState())
	ev := servedOf("a")
	ev.readAt = math.NaN()
	lan.reportServedEvent(ev)
	url, _ := fakeReportsBrain(t, func(es []map[string]any) (int, any) { return 200, resultsAll(es, "applied", 1) })
	lan.reportPost(context.Background(), tbase(url), "k")
	st := lan.reportsStatus()["served"].(map[string]any)
	if st["unencodable"] != int64(1) || st["refused"] != int64(0) || st["pending"] != 0 {
		t.Fatalf("status served = %v, want 1 unencodable, none refused or pending", st)
	}
}

// ---- head_at and readings (quota-model 66 §1, §12) ----

// readingReportsAt is the reading reports posted for account whose read_at is
// at, by window: what the overlay kept of the served head (its as_of is the
// head's instant), each {used_pct, resets_at}.
func readingReportsAt(env *e2eEnv, account string, at float64) map[string]any {
	env.brainMu.Lock()
	defer env.brainMu.Unlock()
	out := map[string]any{}
	for _, e := range env.reports {
		if e["kind"] != reportReading || e["account"] != account || e["read_at"] != at || e["pot"] != nil {
			continue
		}
		out[e["window"].(string)] = map[string]any{"used_pct": e["used_pct"], "resets_at": e["resets_at"]}
	}
	return out
}

// headAgrees checks a posted event's head_at against the request's span:
// after the request left, before its response ended (read_at).
func headAgrees(t *testing.T, ev map[string]any, sent time.Time) float64 {
	t.Helper()
	head, ok := ev["head_at"].(float64)
	readAt, _ := ev["read_at"].(float64)
	if !ok || head < unixSeconds(sent) || head > readAt {
		t.Fatalf("head_at %v, want between the request's send %v and read_at %v", ev["head_at"], unixSeconds(sent), readAt)
	}
	return head
}

// For one set of Anthropic headers the served event's readings are the
// windows of the reading reports that carry a use: the same ids, use and
// reset. 7d_oi is scoped:Fable through the published alias; a window no alias
// names (7d_xx) is in neither. A window with a reset and no use (5h) is
// reported, the brain publishing it, but is not in the event: sizing needs a
// use for a head.
func TestE2EServedReadingsAgreeAnthropic(t *testing.T) {
	t.Cleanup(resetQuotaState())
	env, restore := newE2E(t)
	defer restore()
	reset := time.Now().Add(72 * time.Hour).Truncate(time.Second)
	rs := strconv.FormatInt(reset.Unix(), 10)
	reset5h := time.Now().Add(3 * time.Hour).Truncate(time.Second)
	env.anthHeaders.Store(map[string]string{
		"anthropic-ratelimit-unified-status":            "allowed",
		"anthropic-ratelimit-unified-5h-status":         "allowed",
		"anthropic-ratelimit-unified-5h-reset":          strconv.FormatInt(reset5h.Unix(), 10),
		"anthropic-ratelimit-unified-7d-status":         "allowed",
		"anthropic-ratelimit-unified-7d-utilization":    "0.15",
		"anthropic-ratelimit-unified-7d-reset":          rs,
		"anthropic-ratelimit-unified-7d_oi-status":      "allowed",
		"anthropic-ratelimit-unified-7d_oi-utilization": "0.08",
		"anthropic-ratelimit-unified-7d_oi-reset":       rs,
		"anthropic-ratelimit-unified-7d_xx-status":      "allowed",
		"anthropic-ratelimit-unified-7d_xx-utilization": "0.5",
		"anthropic-ratelimit-unified-7d_xx-reset":       rs,
	})
	view := func() map[string]any {
		return map[string]any{"kind": "anthropic_account", "good_ts": float64(time.Now().Add(-time.Minute).Unix()),
			"windows": map[string]any{
				"five_hour": map[string]any{"percent": 3.0, "resets_at": time.Now().Add(time.Hour).Unix()},
				"seven_day": map[string]any{"percent": 89.0, "resets_at": reset.Unix()},
				"scoped:Fable": map[string]any{"percent": 60.0, "resets_at": reset.Unix(),
					"aliases": map[string]any{"header": []string{"7d_oi"}, "claim": []string{"seven_day_overage_included"}}},
			}}
	}
	env.brainMu.Lock()
	env.views = map[string]any{native: view(), other: view()}
	env.brainMu.Unlock()
	fetchPreferredSync()
	env.anthSSE.Store(servedAnthStart + servedAnthEnd)
	env.anthGap.Store(100 * time.Millisecond)

	sent := time.Now()
	resp := env.postBody(t, "/v1/messages",
		`{"model":"claude-fable-5","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	ev := waitServed(t, env, 1)[0]
	head := headAgrees(t, ev, sent)
	if readAt := ev["read_at"].(float64); readAt-head < 0.2 {
		t.Fatalf("head_at %v is %.3f s before read_at %v; the stream took three 100 ms gaps after its head", head, readAt-head, readAt)
	}
	reportsSettle()
	want := map[string]any{
		"seven_day":    map[string]any{"used_pct": 15.0, "resets_at": float64(reset.Unix())},
		"scoped:Fable": map[string]any{"used_pct": 8.0, "resets_at": float64(reset.Unix())},
	}
	if !reflect.DeepEqual(ev["readings"], want) {
		t.Fatalf("served readings %v, want %v", ev["readings"], want)
	}
	got := readingReportsAt(env, ev["account"].(string), head)
	if five := got["five_hour"]; !reflect.DeepEqual(five, map[string]any{"used_pct": nil, "resets_at": float64(reset5h.Unix())}) {
		t.Fatalf("five_hour reported %v, want its reset alone: the case this test is for", five)
	}
	delete(got, "five_hour")
	if !reflect.DeepEqual(got, ev["readings"]) {
		t.Fatalf("reading reports with a use at head_at %v, served readings %v: they disagree", got, ev["readings"])
	}
}

// For one set of codex headers the reading reports and the served event's
// readings agree. Windows are named by length, not slot: a primary of 10080
// minutes is seven_day, a secondary of 1440 window:1d. A reset given as
// reset-after-seconds is that many seconds after head_at; the credits pot is
// not a window and is not in the readings.
func TestE2EServedReadingsAgreeCodex(t *testing.T) {
	t.Cleanup(resetQuotaState())
	h, restore := newE2EClassOn(t, "gpt/gpt-5.6", 1)
	defer restore()
	resetAt := time.Now().Add(20 * time.Hour).Truncate(time.Second)
	h.codexHook.Store(func(w http.ResponseWriter, _ *http.Request, _ []byte, _ int64) bool {
		w.Header().Set("X-Codex-Primary-Used-Percent", "41")
		w.Header().Set("X-Codex-Primary-Window-Minutes", "10080")
		w.Header().Set("X-Codex-Primary-Reset-After-Seconds", "3600")
		w.Header().Set("X-Codex-Secondary-Used-Percent", "7")
		w.Header().Set("X-Codex-Secondary-Window-Minutes", "1440")
		w.Header().Set("X-Codex-Secondary-Reset-At", strconv.FormatInt(resetAt.Unix(), 10))
		w.Header().Set("X-Codex-Credits-Has-Credits", "True")
		w.Header().Set("X-Codex-Credits-Balance", "7.5")
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, defaultCodexSSE)
		return true
	})
	sent := time.Now()
	resp := h.postStreamSess(t, "claude-frontier", true, "sess-served-readings")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	ev := waitServed(t, h.e2eEnv, 1)[0]
	head := headAgrees(t, ev, sent)
	reportsSettle()
	want := map[string]any{
		"seven_day": map[string]any{"used_pct": 41.0, "resets_at": head + 3600},
		"window:1d": map[string]any{"used_pct": 7.0, "resets_at": float64(resetAt.Unix())},
	}
	if !reflect.DeepEqual(ev["readings"], want) {
		t.Fatalf("served readings %v, want %v", ev["readings"], want)
	}
	if got := readingReportsAt(h.e2eEnv, "gpt", head); !reflect.DeepEqual(got, ev["readings"]) {
		t.Fatalf("reading reports at head_at %v, served readings %v: they disagree", got, ev["readings"])
	}
}

// Kimi carries no rate-limit headers: its event's readings are {}, and it
// still has its head.
func TestE2EServedKimiNoReadings(t *testing.T) {
	t.Cleanup(resetQuotaState())
	h, restore := newE2EClassOn(t, "kimi/kimi-k3", 1)
	defer restore()
	h.kimiSSE.Store(sseFixtureScaled)
	sent := time.Now()
	resp := h.postStreamSess(t, "claude-frontier", true, "sess-served-kimi")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	ev := waitServed(t, h.e2eEnv, 1)[0]
	wireHas(t, ev, map[string]any{"account": "kimi", "complete": true})
	headAgrees(t, ev, sent)
	if rd, ok := ev["readings"].(map[string]any); !ok || len(rd) != 0 {
		t.Fatalf("readings %v (%T), want {}", ev["readings"], ev["readings"])
	}
}

// read_at is head_at plus the time since the head on the monotonic clock
// (servedReadAt), so a wall-clock step mid-response can't put the head after
// the end. On a real clock the difference is the monotonic elapsed time, and
// the capture keeps the head's monotonic reading; a fixed fake clock, with
// none, falls back to the wall difference.
func TestServedReadAtIsMonotonicFromTheHead(t *testing.T) {
	head := time.Now()
	if !strings.Contains(head.String(), " m=") {
		t.Fatalf("time.Now() %v carries no monotonic reading", head)
	}
	time.Sleep(20 * time.Millisecond)
	end := time.Now()
	if got, want := servedReadAt(head, end)-unixSeconds(head), end.Sub(head).Seconds(); math.Abs(got-want) > 1e-6 || want < 0.02 {
		t.Fatalf("read_at - head_at = %v, want the monotonic elapsed %v", got, want)
	}

	c := &servedCap{inference: true}
	c.hop("a", 200, func(string) servedHopFacts {
		return servedHopFacts{kind: servedPassthrough, head: head}
	})
	if !strings.Contains(c.headAt.String(), " m=") {
		t.Fatalf("the capture's head %v lost its monotonic reading", c.headAt)
	}
	before := time.Now()
	ev := c.eventLocked()
	after := time.Now()
	if ev.headAt != unixSeconds(head) {
		t.Fatalf("head_at %v, want the head's own wall time %v", ev.headAt, unixSeconds(head))
	}
	d := ev.readAt - ev.headAt
	if lo, hi := before.Sub(head).Seconds(), after.Sub(head).Seconds(); d < lo-1e-6 || d > hi+1e-6 {
		t.Fatalf("read_at - head_at = %v, want the monotonic elapsed, within [%v, %v]", d, lo, hi)
	}

	// A fake clock: wall times only.
	t0 := time.Unix(1793740000, 500_000_000)
	old := nowFn
	nowFn = func() time.Time { return t0 }
	t.Cleanup(func() { nowFn = old })
	c = &servedCap{inference: true}
	c.hop("a", 200, func(string) servedHopFacts {
		return servedHopFacts{kind: servedPassthrough, head: t0.Add(-3 * time.Second)}
	})
	if ev := c.eventLocked(); ev.readAt != unixSeconds(t0) || ev.headAt != unixSeconds(t0)-3 {
		t.Fatalf("fake clock: head_at %v read_at %v, want %v and %v", ev.headAt, ev.readAt, unixSeconds(t0)-3, unixSeconds(t0))
	}
}

// The response-side live events name the speed the served hop ran at, the one
// its served event is priced at, on every hop kind: an Anthropic hop reads the
// request's speed field (null on the served event, "unknown" live, for one it
// cannot tell); kimi's pass-through hop and gpt's codex hop run at standard
// whatever the request asked. The request-side turn event names no speed. A
// cross within one request names the hop that served: both Anthropic accounts
// 429 a fast-asking request and gpt's codex hop serves it, so its events say
// standard, never the fast the 429'd hops were asked for.
func TestE2ELiveEventsCarryTheServedSpeed(t *testing.T) {
	for i, tc := range []struct {
		label, member, asked, servedBy, served, live string
		cross                                        bool
	}{
		{"anthropic fast", "lane:fable", `,"speed":"fast"`, "lane:fable", "fast", "fast", false},
		{"anthropic none", "lane:fable", ``, "lane:fable", "standard", "standard", false},
		{"anthropic turbo", "lane:fable", `,"speed":"turbo"`, "lane:fable", "<nil>", "unknown", false},
		{"kimi fast", "kimi/kimi-k3", `,"speed":"fast"`, "kimi/kimi-k3", "standard", "standard", false},
		{"codex fast", "gpt/gpt-5.6", `,"speed":"fast"`, "gpt/gpt-5.6", "standard", "standard", false},
		{"anthropic 429 crossed to codex", "lane:fable", `,"speed":"fast"`, "gpt/gpt-5.6", "standard", "standard", true},
	} {
		t.Run(tc.label, func(t *testing.T) {
			tapTest(t)
			t.Cleanup(resetQuotaState())
			h, restore := respClassHarness(t)
			defer restore()
			drain := tapListen(t)
			h.anthSSE.Store(servedAnthStart + servedAnthEnd)
			h.kimiSSE.Store(respSSEFixture())
			h.bindTo.Store(tc.member)
			if tc.cross {
				h.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"You have exceeded your weekly usage limit."}}`)
				h.deny429.Store("*") // every Anthropic account 429s
			}
			body := `{"model":"claude-frontier","max_tokens":16,"stream":true` + tc.asked +
				`,"messages":[{"role":"user","content":"hi"}]}`
			req, _ := http.NewRequest("POST", h.proxy.URL+"/v1/messages", strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer CLIENT")
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("anthropic-beta", "oauth-2025-04-20")
			req.Header.Set("x-claude-code-session-id", fmt.Sprintf("sess-speed-%d", i))
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("proxy request failed: %v", err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Fatalf("status %d", resp.StatusCode)
			}
			ev := waitServed(t, h.e2eEnv, 1)[0]
			if ev["member"] != tc.servedBy {
				t.Fatalf("served member = %v, want %s", ev["member"], tc.servedBy)
			}
			if got := fmt.Sprint(ev["speed"]); got != tc.served {
				t.Fatalf("served speed = %s, want %s", got, tc.served)
			}
			respTapSettle()
			kinds := map[string]bool{}
			for _, f := range drain() {
				switch f.kind {
				case "turn":
					kinds[f.kind] = true
					if strings.Contains(string(f.raw), `"speed"`) {
						t.Fatalf("the turn event names a speed: %s", f.raw)
					}
				case "turn_started", "assistant_message", "turn_ended":
					kinds[f.kind] = true
					if got := respSpeedOf(t, f); got != tc.live {
						t.Fatalf("%s speed = %q, want %q", f.kind, got, tc.live)
					}
				}
			}
			if !kinds["turn"] || !kinds["turn_started"] || !kinds["assistant_message"] {
				t.Fatalf("events seen %v, want the turn, turn_started and assistant_message", kinds)
			}
		})
	}
}

// assistantUsage drains the tap and returns the one assistant_message's usage
// block as raw keys.
func assistantUsage(t *testing.T, drain func() []tapFrame) map[string]json.RawMessage {
	t.Helper()
	respTapSettle()
	var usage map[string]json.RawMessage
	n := 0
	for _, f := range drain() {
		if f.kind != "assistant_message" {
			continue
		}
		n++
		var ev struct {
			Usage map[string]json.RawMessage `json:"usage"`
		}
		if err := json.Unmarshal(f.raw, &ev); err != nil {
			t.Fatalf("assistant_message does not decode: %v", err)
		}
		usage = ev.Usage
	}
	if n != 1 || usage == nil {
		t.Fatalf("%d assistant_message events, usage %v; want one with usage", n, usage)
	}
	return usage
}

// A codex turn's assistant_message carries raw_output_tokens, the upstream's
// whole output count, which is what its served event bills as output; its
// output_tokens stays the client's, reasoning taken off. With nothing to take
// off the two are equal, and raw_output_tokens is still there.
func TestE2ELiveUsageCarriesCodexRawOutput(t *testing.T) {
	for _, tc := range []struct {
		label, usage     string
		client, upstream string
	}{
		{"reasoning", `"usage":{"input_tokens":9000,"output_tokens":700,"output_tokens_details":{"reasoning_tokens":300}}`, "400", "700"},
		{"no reasoning", `"usage":{"input_tokens":9000,"output_tokens":700}`, "700", "700"},
	} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s stream=%v", tc.label, stream), func(t *testing.T) {
				tapTest(t)
				t.Cleanup(resetQuotaState())
				h, restore := newE2EClassOn(t, "gpt/gpt-5.6", 1.0)
				defer restore()
				drain := tapListen(t)
				h.codexSSE.Store(strings.Replace(defaultCodexSSE,
					`"usage":{"input_tokens":11,"output_tokens":5}`, tc.usage, 1))
				resp := h.postStreamSess(t, "claude-frontier", stream, "sess-raw-out")
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 {
					t.Fatalf("status %d", resp.StatusCode)
				}
				ev := waitServed(t, h.e2eEnv, 1)[0]
				tokens := ev["tokens"].(map[string]any)
				if got := fmt.Sprint(tokens["output"]); got != tc.upstream {
					t.Fatalf("served output = %s, want %s", got, tc.upstream)
				}
				u := assistantUsage(t, drain)
				if got := string(u["output_tokens"]); got != tc.client {
					t.Fatalf("output_tokens = %s, want the client's %s", got, tc.client)
				}
				if got := string(u["raw_output_tokens"]); got != fmt.Sprint(tokens["output"]) {
					t.Fatalf("raw_output_tokens = %q, want the served event's output %v", got, lan.tokens["output"])
				}
			})
		}
	}
}

// An Anthropic turn and a pass-through provider's turn reach the client with
// the upstream's own output count, so their usage names no raw_output_tokens.
func TestE2ELiveUsageHasNoRawOutputOffCodex(t *testing.T) {
	for _, member := range []string{"lane:fable", "kimi/kimi-k3"} {
		t.Run(member, func(t *testing.T) {
			tapTest(t)
			t.Cleanup(resetQuotaState())
			h, restore := respClassHarness(t)
			defer restore()
			drain := tapListen(t)
			h.anthSSE.Store(servedAnthStart + servedAnthEnd)
			h.kimiSSE.Store(respSSEFixture())
			h.bindTo.Store(member)
			resp := h.postStreamSess(t, "claude-frontier", true, "sess-raw-none")
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Fatalf("status %d", resp.StatusCode)
			}
			if ev := waitServed(t, h.e2eEnv, 1)[0]; ev["member"] != member {
				t.Fatalf("served member = %v, want %s", ev["member"], member)
			}
			u := assistantUsage(t, drain)
			if _, ok := u["output_tokens"]; !ok {
				t.Fatalf("usage has no output_tokens: %v", u)
			}
			if raw, ok := u["raw_output_tokens"]; ok {
				t.Fatalf("usage names raw_output_tokens %s off a codex hop", raw)
			}
		})
	}
}
