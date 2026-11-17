package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func noteE2EBody(stream bool) string {
	return fmt.Sprintf(`{"model":"claude-frontier","max_tokens":16,"stream":%t,"messages":[{"role":"user","content":"hello"}]}`, stream)
}

func noteE2EEnableCodex(t *testing.T, h *e2eHosted) {
	t.Helper()
	h.brainMu.Lock()
	h.classMembers[1]["usable"], h.classMembers[1]["status"], h.classMembers[1]["effective"] = true, "ok", 1.0
	h.brainMu.Unlock()
	fetchPreferredSync()
}

func TestE2ENoteWalkJSONAndHistoryRoundtrip(t *testing.T) {
	h, done := newE2EClass(t)
	defer done()
	armHealth(t, newHealthStore(defaultHealthCfg()))
	armEnforce(t)
	h.deny500.Store("*")
	h.kimiJSON.Store(noteJSONFixture)
	before := noteEmissionTotals()[[2]string{"frontier", "walk_health"}]
	resp := h.postClassBody(t, "note-json", noteE2EBody(false))
	body := wireBody(t, resp)
	h.hit(t)
	h.hostedHit(t)
	var reply struct{ Content []json.RawMessage }
	if resp.StatusCode != 200 || json.Unmarshal(body, &reply) != nil || len(reply.Content) != 3 {
		t.Fatalf("missing separate note block: %d %s", resp.StatusCode, body)
	}
	var block struct{ Type, Text string }
	_ = json.Unmarshal(reply.Content[2], &block)
	if block.Type != "text" || !strings.Contains(block.Text, "lane:fable failed at the door (500)") || !strings.Contains(block.Text, "kimi/kimi-k3") {
		t.Fatalf("note lost served/failed identities: %+v", block)
	}
	assertNoteStrips(t, strings.TrimPrefix(block.Text, "\n\n[switch-proxy] "))
	if noteEmissionTotals()[[2]string{"frontier", "walk_health"}]-before != 1 {
		t.Fatal("handler did not count exactly one note")
	}
	// Echo the actual response blocks, then prove the forwarded bytes match
	// the same history with only the proxy's block removed.
	all, _ := json.Marshal(reply.Content)
	model, _ := json.Marshal(reply.Content[:2])
	encode := func(content []byte) string {
		return `{"model":"claude-frontier","max_tokens":16,"messages":[{"role":"user","content":"hello"},{"role":"assistant","content":` + string(content) + `},{"role":"user","content":"continue"}]}`
	}
	resp = h.postClassBody(t, "note-json", encode(all))
	_ = wireBody(t, resp)
	h.hit(t)
	hit := h.hostedHit(t)
	want := rewriteModel([]byte(encode(model)), "kimi-k3")
	if !bytes.Equal(hit.body, want) {
		t.Fatalf("actual note leaked into replay/cache prefix:\n got %s\nwant %s", hit.body, want)
	}
}

func TestE2ENotePassthroughStreamAndGuards(t *testing.T) {
	for _, stop := range []string{"end_turn", "tool_use", "truncated"} {
		t.Run(stop, func(t *testing.T) {
			h, done := newE2EClass(t)
			defer done()
			armHealth(t, newHealthStore(defaultHealthCfg()))
			armEnforce(t)
			h.deny500.Store("*")
			script := noteSSEFixture(stop, "answer")
			if stop == "truncated" {
				script = script[:strings.Index(script, "event: message_delta")]
			}
			h.kimiSSE.Store(script)
			resp := h.postClassBody(t, "note-stream", noteE2EBody(true))
			body := wireBody(t, resp)
			h.hit(t)
			h.hostedHit(t)
			want := 0
			if stop == "end_turn" {
				want = 1
			}
			if resp.StatusCode != 200 || bytes.Count(body, noteSentinelBytes) != want {
				t.Fatalf("unexpected note count: %d %s", resp.StatusCode, body)
			}
			assertSSEFrames(t, string(body))
		})
	}
}

func TestE2ENoteCrossIntoCodexBothShapes(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			h, done := newE2EClass(t)
			defer done()
			noteE2EEnableCodex(t, h)
			armHealth(t, newHealthStore(defaultHealthCfg()))
			armEnforce(t)
			evTest(t)
			evArm()
			h.deny500.Store("*")
			resp := h.postClassBody(t, "note-codex", noteE2EBody(stream))
			body := wireBody(t, resp)
			h.hit(t)
			h.codexHitOrFail(t)
			d := awaitDecision(t, resp.Header.Get("X-Switch-Decision"))
			if !d.Noted || valueOrEmpty(d.NoteKind) != "walk_health" || d.Walked != "health" || d.Outcome != "ok" || d.Attempts != 2 || valueOrEmpty(d.ServedMember) != "gpt/gpt-5.6" {
				t.Fatalf("codex final decision: %+v", d)
			}
			if resp.StatusCode != 200 || bytes.Count(body, noteSentinelBytes) != 1 || !bytes.Contains(body, []byte("answered by gpt/gpt-5.6")) {
				t.Fatalf("codex cross lost its note: %d %s", resp.StatusCode, body)
			}
			if stream {
				assertSSEFrames(t, string(body))
			}
		})
	}
}

func TestE2ENoteOpenAIFlatHistoryRoundtrip(t *testing.T) {
	h, done := newE2EClass(t)
	defer done()
	resetWireBuckets()
	noteE2EEnableCodex(t, h)
	armHealth(t, newHealthStore(defaultHealthCfg()))
	armEnforce(t)
	h.bindTo.Store("kimi/kimi-k3")
	h.deny.Store(true)
	resp := wirePost(t, h.e2eEnv, `{"model":"claude-frontier","messages":[{"role":"user","content":"hello"}]}`, "note-openai")
	body := wireBody(t, resp)
	h.hostedHit(t)
	h.codexHitOrFail(t)
	var reply struct {
		Choices []struct{ Message struct{ Content string } }
	}
	if resp.StatusCode != 200 || json.Unmarshal(body, &reply) != nil || len(reply.Choices) != 1 || !strings.Contains(reply.Choices[0].Message.Content, noteSentinel) {
		t.Fatalf("OpenAI wire lost its note: %d %s", resp.StatusCode, body)
	}
	content, _ := json.Marshal(reply.Choices[0].Message.Content)
	resp = wirePost(t, h.e2eEnv, `{"model":"claude-frontier","messages":[{"role":"user","content":"hello"},{"role":"assistant","content":`+string(content)+`},{"role":"user","content":"continue"}]}`, "note-openai")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	hit := h.codexHitOrFail(t)
	if bytes.Contains(hit.body, noteSentinelBytes) {
		t.Fatalf("flattened OpenAI note reached codex: %s", hit.body)
	}
}

func TestE2ENoteEmissionOffAndLane(t *testing.T) {
	for _, mode := range []string{"off", "observe", "disabled", "unlisted", "lane"} {
		t.Run(mode, func(t *testing.T) {
			h, done := newE2EClass(t)
			defer done()
			armHealth(t, newHealthStore(defaultHealthCfg()))
			armEnforce(t)
			oldNotes := healthNotesEnabled
			t.Cleanup(func() { healthNotesEnabled = oldNotes })
			h.bindTo.Store("kimi/kimi-k3")
			h.kimiJSON.Store(noteJSONFixture)
			// A pinned half-open ride meets the predicate without walking or
			// changing the class binding, even when enforcement is disabled.
			h.bindReason.Store("bind: pinned by operator")
			lan.health.mu.Lock()
			lan.health.keys["kimi/kimi-k3"] = &epState{state: hsHalfOpen}
			lan.health.mu.Unlock()
			request := noteE2EBody(false)
			switch mode {
			case "off":
				healthModeV = healthOff
			case "observe":
				healthModeV = healthObserve
			case "disabled":
				healthNotesEnabled = false
			case "unlisted":
				healthEnforceFor = map[string]bool{"elsewhere": true}
			case "lane": // a non-class request never carries a note
				request = strings.Replace(request, "claude-frontier", "claude-fable-5", 1)
			}
			resp := h.postClassBody(t, "note-dark", request)
			body := wireBody(t, resp)
			if mode == "lane" {
				h.hit(t)
			} else {
				h.hostedHit(t)
			}
			if resp.StatusCode != 200 || bytes.Contains(body, noteSentinelBytes) {
				t.Fatalf("emitted note in %s: %d %s", mode, resp.StatusCode, body)
			}
		})
	}
}

func TestE2ENotePartialScaledJSONFailsOpen(t *testing.T) {
	h, done := newE2EClass(t)
	defer done()
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "99999") // a complete-looking JSON prefix, broken HTTP body
		_, _ = io.WriteString(w, noteJSONFixture)
	}))
	defer broken.Close()
	kimiUpstream = broken.URL
	rebuildProxy(h.e2eEnv)
	armHealth(t, newHealthStore(defaultHealthCfg()))
	armEnforce(t)
	h.deny500.Store("*")
	resp := h.postClassBody(t, "note-partial-json", noteE2EBody(false))
	body := wireBody(t, resp)
	h.hit(t)
	if string(body) != noteJSONFixture {
		t.Fatalf("annotated or changed a partial upstream response: %s", body)
	}
}

func TestE2ENoteHalfOpenPinWithAndWithoutLease(t *testing.T) {
	for _, leased := range []bool{false, true} {
		t.Run(fmt.Sprint(leased), func(t *testing.T) {
			h, done := newE2EClass(t)
			defer done()
			hs := newHealthStore(defaultHealthCfg())
			armHealth(t, hs)
			armEnforce(t)
			h.bindTo.Store("kimi/kimi-k3")
			h.bindReason.Store("bind: pinned by operator")
			h.kimiJSON.Store(noteJSONFixture)
			hs.keys["kimi/kimi-k3"] = &epState{key: "kimi/kimi-k3", state: hsHalfOpen,
				ep: fmt.Sprintf("kimi/kimi-k3@%d", nowFn().Add(-4*time.Minute).Unix()), probeTok: leased}
			resp := h.postClassBody(t, "note-pin-probe", noteE2EBody(false))
			body := wireBody(t, resp)
			h.hostedHit(t)
			if resp.StatusCode != 200 || bytes.Count(body, noteSentinelBytes) != 1 {
				t.Fatalf("missing half-open note: %d %s", resp.StatusCode, body)
			}
			if leased {
				if !bytes.Contains(body, []byte("this request probed kimi/kimi-k3")) || !bytes.Contains(body, []byte("one more good probe")) {
					t.Fatalf("adopted probe mislabelled: %s", body)
				}
			} else if !bytes.Contains(body, []byte("recovering (a probe is in flight)")) || bytes.Contains(body, []byte("this request probed")) {
				t.Fatalf("non-probe claimed a lease or degradation: %s", body)
			}
		})
	}
}
