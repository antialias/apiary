package main

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func fixtureNoteDecision(kind string) noteDecision {
	d := noteDecision{class: "helpdesk", bound: "gpt/gpt-5.6-sol", served: "kimi/kimi-k3", hop: kind,
		boundState:  noteKeyState{state: hsOpen, age: 4 * time.Minute, failed: 7, total: 9},
		servedState: noteKeyState{state: hsHealthy}}
	switch kind {
	case "ranked":
		d.servedState = d.boundState
	case "forced/only", "bound_ride", "pinned_ride":
		d.served, d.servedState = d.bound, d.boundState
		d.hop = "bound"
		if kind == "forced/only" {
			d.hop = "forced"
		}
		d.pinned = kind == "pinned_ride"
	case "probe":
		d.served, d.probe = d.bound, true
		d.servedState = noteKeyState{state: hsHalfOpen, episodeStart: time.Date(2026, 9, 4, 20, 2, 0, 0, time.Local), probesRemaining: 1}
	case "ramp":
		d.reason, d.boundState.dwell = "health:ramp", 3*time.Minute
	case "walk_health", "walk_health/same-member", "walk_health/both":
		d.walked, d.failure = "health", &noteWalkCause{member: d.bound, status: 502}
		if kind == "walk_health/same-member" {
			d.served = d.bound
		}
		if kind == "walk_health/both" {
			d.walked, d.failure.member, d.served = "both", d.served, "claude/opus"
			d.quota = &noteWalkCause{member: d.bound, reset: time.Date(2026, 9, 4, 14, 5, 0, 0, time.Local)}
		}
	case "walk_quota":
		d.walked = "quota"
		d.quota = &noteWalkCause{member: d.bound, reset: time.Date(2026, 9, 4, 14, 5, 0, 0, time.Local)}
	}
	return d
}

func assertNoteStrips(t *testing.T, line string) {
	t.Helper()
	if utf8.RuneCountInString(line) > noteMaxRunes || strings.ContainsAny(line, "\r\n") {
		t.Fatalf("invalid note line: %q", line)
	}
	if got, ok := splitTailNote("answer\n\n" + noteSentinel + " " + line); !ok || got != "answer" {
		t.Fatalf("renderer escaped the deployed strip grammar: %q -> %q, %t", line, got, ok)
	}
}

// The renderer, not just hand-written strings, must meet U17's grammar.
func TestNoteRendererAllTemplates(t *testing.T) {
	for name, want := range noteTemplateFixtures {
		t.Run(name, func(t *testing.T) {
			kind, got := renderResponseNote(fixtureNoteDecision(name))
			if got != want || kind != strings.Split(name, "/")[0] {
				t.Fatalf("%s: %q, want %q", kind, got, want)
			}
			assertNoteStrips(t, got)
		})
	}
}

func TestNoteRendererRecoveryAndLabelEdges(t *testing.T) {
	for _, kind := range []string{"bound_ride", "pinned_ride", "forced/only"} {
		d := fixtureNoteDecision(kind)
		d.boundState.state, d.servedState.state = hsHalfOpen, hsHalfOpen
		gotKind, line := renderResponseNote(d)
		if gotKind == "probe" || strings.Contains(line, "degraded") || !strings.Contains(line, "recovering (a probe is in flight)") {
			t.Fatalf("unleased half-open reply misdescribed: %s %q", gotKind, line)
		}
		assertNoteStrips(t, line)
		d.probe = true
		if gotKind, _ = renderResponseNote(d); gotKind != "probe" {
			t.Fatalf("an adopted probe was not described as a probe: %s", gotKind)
		}
	}
	for name := range noteTemplateFixtures {
		for _, label := range []string{"lane:fable", "p/" + strings.Repeat("雪", 190), "bad\nlabel\r\t"} {
			d := fixtureNoteDecision(name)
			d.bound, d.served, d.class = label, label+"2", label
			if d.quota != nil {
				d.quota.member = label
			}
			if d.failure != nil {
				d.failure.member = label
			}
			_, line := renderResponseNote(d)
			assertNoteStrips(t, line)
		}
	}
	d := fixtureNoteDecision("walk_quota")
	d.quota.reset = time.Time{}
	_, line := renderResponseNote(d)
	if strings.Contains(line, "until") {
		t.Fatal("invented an unknown quota reset")
	}
	d.quota.shapeRejected = true
	_, line = renderResponseNote(d)
	if strings.Contains(line, "out of quota") || !strings.Contains(line, "rejected this request (429)") {
		t.Fatalf("shape rejection mislabelled: %q", line)
	}
	assertNoteStrips(t, line)
}

func TestNoteCandidateModeAndSnapshot(t *testing.T) {
	h := newHealthStore(defaultHealthCfg())
	armHealth(t, h)
	armEnforce(t)
	oldEnabled := healthNotesEnabled
	t.Cleanup(func() { healthNotesEnabled = oldEnabled })
	cc := &classRoute{class: "test-note", boundMember: "gpt/a", member: classMemberPref{ID: "gpt/a"},
		served: &servedHop{member: "kimi/b"}, hopKind: "detour"}
	h.keys["gpt/a"] = &epState{state: hsOpen, since: nowFn().Add(-4 * time.Minute)}
	n := prepareResponseNote(cc)
	if n == nil || !strings.Contains(n.text, "gpt/a is degraded (open 4 min)") {
		t.Fatalf("missing original binding evidence: %+v", n)
	}
	h.keys["gpt/a"].state = hsHealthy
	if !strings.Contains(n.text, "is degraded") {
		t.Fatal("route snapshot mutated after the head")
	}
	for _, mode := range []healthMode{healthOff, healthObserve} {
		healthModeV = mode
		if prepareResponseNote(cc) != nil {
			t.Fatalf("candidate under %v", mode)
		}
	}
	healthModeV, healthNotesEnabled = healthEnforce, false
	if prepareResponseNote(cc) != nil {
		t.Fatal("kill switch ignored")
	}
	healthNotesEnabled, healthEnforceFor = true, map[string]bool{"another-class": true}
	if prepareResponseNote(cc) != nil {
		t.Fatal("enforce class allowlist ignored")
	}
	healthEnforceFor = nil
	cc.hopKind = "bound"
	if prepareResponseNote(cc) != nil {
		t.Fatal("healthy bound path installed a candidate")
	}
	if prepareResponseNote(nil) != nil {
		t.Fatal("direct path installed a candidate")
	}
	h.keys["kimi/b"] = &epState{state: hsHalfOpen}
	if prepareResponseNote(cc) == nil {
		t.Fatal("half-open bound ride lost its note")
	}
}

const noteJSONFixture = ` { "id":"msg_1", "type":"message", "content" : [{"type":"thinking","thinking":"private"}, {"type":"text","text":"answer"}], "stop_reason":"end_turn", "usage":{"output_tokens":9007199254740993}, "extra":[1,2] } `

func testResponseNote() *responseNote {
	return &responseNote{class: "note-test", kind: "walk_health", text: "\n\n[switch-proxy] answered by kimi/kimi-k3 — lane:fable failed at the door (500)"}
}

func TestNoteJSONSplicesOnlyNewBlock(t *testing.T) {
	n := testResponseNote()
	out := insertJSONNote([]byte(noteJSONFixture), n.text)
	if out == nil {
		t.Fatal("no note appended")
	}
	start := bytes.Index(out, []byte(`,{"type":"text","text":"\n\n[switch-proxy]`))
	end := bytes.Index(out[start:], []byte(`}]`)) + start + 1
	if start < 0 || !bytes.Equal(append(append([]byte{}, out[:start]...), out[end:]...), []byte(noteJSONFixture)) {
		t.Fatalf("modified model blocks, usage, or formatting: %s", out)
	}
}

func TestNoteJSONGuards(t *testing.T) {
	for name, raw := range map[string]string{
		"tool":         strings.Replace(noteJSONFixture, "end_turn", "tool_use", 1),
		"empty text":   strings.Replace(noteJSONFixture, `"text":"answer"`, `"text":""`, 1),
		"no text":      `{"content":[{"type":"thinking","thinking":"x"}],"stop_reason":"end_turn"}`,
		"missing stop": `{"content":[{"type":"text","text":"x"}]}`,
		"null stop":    strings.Replace(noteJSONFixture, `"end_turn"`, "null", 1),
		"invalid":      noteJSONFixture[:len(noteJSONFixture)-3],
		"trailing":     noteJSONFixture + ` {}`,
		"duplicate":    `{"content":[],"content":[{"type":"text","text":"x"}],"stop_reason":"end_turn"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if out := insertJSONNote([]byte(raw), testResponseNote().text); out != nil {
				t.Fatalf("annotated an ineligible response: %s", out)
			}
		})
	}
	if insertJSONNote([]byte(strings.Replace(noteJSONFixture, "end_turn", "max_tokens", 1)), testResponseNote().text) == nil {
		t.Fatal("a completed max_tokens response is eligible")
	}
}

type noteTestBody struct {
	io.Reader
	err    error
	closed bool
}

func (b *noteTestBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	if err == io.EOF && b.err != nil {
		err = b.err
	}
	return n, err
}

func (b *noteTestBody) Close() error { b.closed = true; return nil }

func TestNoteJSONBodyFailureAndOversize(t *testing.T) {
	broken := errors.New("broken upstream")
	for _, tc := range []struct {
		name  string
		limit int64
		err   error
	}{
		{"oversize", 25, nil}, {"read error", noteJSONLimit, broken},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orig := &noteTestBody{Reader: strings.NewReader(noteJSONFixture), err: tc.err}
			resp := &http.Response{StatusCode: 200, Body: orig, ContentLength: -1, Header: http.Header{"Content-Type": {"application/json"}, "X-Original": {"yes"}}}
			head := resp.Header.Clone()
			n := testResponseNote()
			appendJSONNote(resp, n, tc.limit)
			got, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if string(got) != noteJSONFixture || !errors.Is(err, tc.err) || !orig.closed || n.noted.Load() || resp.ContentLength != -1 || !reflect.DeepEqual(head, resp.Header) {
				t.Fatalf("fail-open changed the response: %q err=%v headers=%v", got, err, resp.Header)
			}
		})
	}
}

func TestNoteResponseGuardsAndCounters(t *testing.T) {
	for _, status := range []int{200, 400, 500, 529} {
		n := testResponseNote()
		before := noteEmissionTotals()[[2]string{n.class, n.kind}]
		resp := &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(noteJSONFixture)), Header: http.Header{"Content-Type": {"application/json"}}}
		appendResponseNote(resp, n)
		appendResponseNote(resp, n) // idempotent if a future wrapper also calls it
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		want := int64(0)
		if status == 200 {
			want = 1
			if resp.ContentLength != int64(len(b)) || !strings.Contains(string(b), noteSentinel) {
				t.Fatal("JSON note length/body mismatch")
			}
		} else if string(b) != noteJSONFixture {
			t.Fatal("error response changed")
		}
		if delta := noteEmissionTotals()[[2]string{n.class, n.kind}] - before; delta != want {
			t.Fatalf("status %d counted %d emissions, want %d", status, delta, want)
		}
	}
	metrics := newHealthStore(defaultHealthCfg()).renderMetrics()
	if !strings.Contains(metrics, `switch_health_notes_total{class="note-test",note_kind="walk_health"}`) {
		t.Fatal("no class/kind emission metric")
	}
}
