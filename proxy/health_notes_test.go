package main

// U17 — the inbound strip (§5.5(5)).
//
// Two things are being pinned here. The GRAMMAR: what counts as a tail note this
// proxy wrote, and — at least as important — what does not, because the user's own
// words and the model's own quotation are the history and must survive untouched.
// The SPLICE: that removing one costs exactly the bytes of the note and nothing
// else, so a block-faithful client's upstream body is byte-identical to the one it
// would have sent for the un-annotated history, and the prompt-cache prefix holds.

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

// noteTemplateFixtures is every rendering in §5.5(2)'s table, verbatim — all nine
// note_kind values and the alternate forms three of them carry. The renderer's
// output is checked against these in TestNoteRendererAllTemplates. A head not in
// noteTemplateHeads (or is not a member id followed by one of noteMemberHeads)
// fails here, which is the point: a new template cannot silently escape the strip.
var noteTemplateFixtures = map[string]string{
	"detour":                  "answered by kimi/kimi-k3 — gpt/gpt-5.6-sol is degraded (open 4 min, 7/9 failed); traffic returns on its own once it passes a probe",
	"ranked":                  "answered by kimi/kimi-k3, which is also degraded — it was the best available; gpt/gpt-5.6-sol is degraded (open 4 min)",
	"forced":                  "answered by kimi/kimi-k3 — it is the only other member of class helpdesk; gpt/gpt-5.6-sol is degraded (open 4 min, 7/9 failed)",
	"forced/only":             "served by gpt/gpt-5.6-sol although it is degraded (open 4 min) — it is the only member of class helpdesk",
	"bound_ride":              "served by gpt/gpt-5.6-sol although it is degraded (open 4 min) — the brain just re-bound class helpdesk to it",
	"pinned_ride":             "served by gpt/gpt-5.6-sol although it is degraded (open 4 min) — it is pinned for class helpdesk; unpin: GET :9115/class-pin?class=helpdesk&member=auto",
	"probe":                   "this request probed gpt/gpt-5.6-sol, degraded since 20:02 — it answered; one more good probe and it is back",
	"ramp":                    "still on kimi/kimi-k3 while gpt/gpt-5.6-sol recovers — this session moves back in ~3 min",
	"walk_health":             "gpt/gpt-5.6-sol failed at the door (502); answered by kimi/kimi-k3",
	"walk_health/same-member": "gpt/gpt-5.6-sol failed at the door on one account (502); answered by it on another",
	"walk_health/both":        "gpt/gpt-5.6-sol is out of quota until 14:05; kimi/kimi-k3 failed at the door (502); answered by claude/opus",
	"walk_quota":              "gpt/gpt-5.6-sol is out of quota until 14:05; answered by kimi/kimi-k3",
}

// U17 (a) — every template the proxy can render is recognized and removed, and
// every one of them fits the length bound the strip enforces. A template that
// outgrew 160 characters would be emitted and then never stripped again.
func TestU17EveryTemplateStrips(t *testing.T) {
	for kind, line := range noteTemplateFixtures {
		if n := utf8.RuneCountInString(line); n > noteMaxRunes {
			t.Errorf("%s: template is %d chars, over the %d bound — it would be emitted but never stripped", kind, n, noteMaxRunes)
		}
		got, ok := splitTailNote("The answer.\n\n" + noteSentinel + " " + line)
		if !ok {
			t.Errorf("%s: not recognized as a tail note: %q", kind, line)
			continue
		}
		if got != "The answer." {
			t.Errorf("%s: stripped to %q, want %q", kind, got, "The answer.")
		}
	}
}

// U17 (b) — the grammar is the note's own and nothing looser. Each of these
// carries the sentinel and must survive: they are the history.
func TestU17LeavesNonNotesAlone(t *testing.T) {
	cases := []struct{ name, s string }{
		{"mid-line mention", "The proxy prefixes its notes with " + noteSentinel + " so you can spot them."},
		{"not the last line", "x\n" + noteSentinel + " answered by kimi/kimi-k3\nand then I continued."},
		{"two spaces after the sentinel", "x\n\n" + noteSentinel + "  answered by kimi/kimi-k3"},
		{"no space after the sentinel", "x\n\n" + noteSentinel + "answered by kimi/kimi-k3"},
		{"unknown head", "x\n\n" + noteSentinel + " something else entirely happened"},
		{"member id, wrong verb", "x\n\n" + noteSentinel + " kimi/kimi-k3 exploded at the door"},
		{"head-shaped but not a member id", "x\n\n" + noteSentinel + " kimi-k3 failed at the door"},
		{"over the length bound", "x\n\n" + noteSentinel + " answered by kimi/kimi-k3 " + strings.Repeat("y", noteMaxRunes)},
		{"sentinel only", "x\n\n" + noteSentinel},
		{"no sentinel at all", "just an ordinary answer"},
	}
	for _, c := range cases {
		if _, ok := splitTailNote(c.s); ok {
			t.Errorf("%s: stripped %q — the history must survive", c.name, c.s)
		}
	}
}

// U17 (c) — the blank line that separates the note from the model's last
// paragraph goes with it, and a single newline is handled too.
func TestU17RemovesTheSeparatingBlankLine(t *testing.T) {
	note := noteSentinel + " answered by kimi/kimi-k3"
	for _, c := range []struct{ in, want string }{
		{"Answer.\n\n" + note, "Answer."},
		{"Answer.\n" + note, "Answer."},
		{"Para one.\n\nPara two.\n\n" + note, "Para one.\n\nPara two."},
		{"\n\n" + note, ""},
		{note, ""},
	} {
		got, ok := splitTailNote(c.in)
		if !ok {
			t.Fatalf("%q: not recognized", c.in)
		}
		if got != c.want {
			t.Errorf("%q -> %q, want %q", c.in, got, c.want)
		}
	}
}

// noteBody renders a request body around one assistant turn, so the annotated and
// un-annotated forms differ ONLY by the note.
func noteBody(assistant string) string {
	return `{"model":"claude-frontier","max_tokens":16,"messages":[` +
		`{"role":"user","content":"hi"},` +
		`{"role":"assistant","content":[` + assistant + `],` + `"x":1},` +
		`{"role":"user","content":"and then?"}]}`
}

// E21 (a) — the block-faithful case, which is the PRIMARY one: every placement in
// §5.5(3) emits the note as its own trailing block, so the strip drops a whole
// block object and the result is byte-identical to what the same client would have
// sent for the un-annotated history.
func TestE21BlockFaithfulStripIsByteIdentical(t *testing.T) {
	clean := noteBody(`{"type":"text","text":"The answer."}`)
	noted := noteBody(`{"type":"text","text":"The answer."},{"type":"text","text":"\n\n` + noteSentinel + ` answered by kimi/kimi-k3"}`)

	got := stripSwitchNote("test", []byte(noted))
	if got == nil {
		t.Fatal("no strip happened")
	}
	if string(got) != clean {
		t.Fatalf("not byte-identical to the un-annotated body:\n got %s\nwant %s", got, clean)
	}
}

// E21 (b) — the flattened case: a client that stores the reply as one string
// hands the note back merged into the answer's own text. The upstream body must
// parse equal to the un-annotated history, and every byte outside the one touched
// value must be identical.
func TestE21FlattenedStringStripParsesEqual(t *testing.T) {
	clean := `{"model":"claude-frontier","messages":[{"role":"assistant","content":"The answer."},{"role":"user","content":"go on"}]}`
	noted := `{"model":"claude-frontier","messages":[{"role":"assistant","content":"The answer.\n\n` + noteSentinel + ` answered by kimi/kimi-k3"},{"role":"user","content":"go on"}]}`

	got := stripSwitchNote("test", []byte(noted))
	if got == nil {
		t.Fatal("no strip happened")
	}
	if string(got) != clean {
		t.Fatalf("value splice changed bytes outside the value:\n got %s\nwant %s", got, clean)
	}
	assertParsesEqual(t, got, []byte(clean))
}

// E21 (c) — a body with no sentinel is byte-identical (stripSwitchNote returns nil
// so the caller keeps its own bytes) and the token walk never ran. That second
// half is §0.8's cost claim: the only unconditional health work is one scan.
func TestE21NoSentinelNeverWalks(t *testing.T) {
	before, _ := noteStripTotals()
	body := noteBody(`{"type":"text","text":"a perfectly ordinary answer"}`)
	if got := stripSwitchNote("test", []byte(body)); got != nil {
		t.Fatalf("body without the sentinel was rewritten: %s", got)
	}
	after, _ := noteStripTotals()
	if after["test"] != before["test"] {
		t.Fatalf("token walk ran %d time(s) on a body with no sentinel", after["test"]-before["test"])
	}
}

// The user's own words and a tool's output are never touched, whatever they say —
// including a user who pastes a real note back to ask about it.
func TestStripLeavesNonAssistantRolesAlone(t *testing.T) {
	note := noteSentinel + " answered by kimi/kimi-k3"
	body := `{"model":"claude-frontier","system":"you may see ` + note + `","messages":[` +
		`{"role":"user","content":"why did I get this?\n\n` + note + `"},` +
		`{"role":"user","content":[{"type":"tool_result","content":"log line: ` + note + `"}]},` +
		`{"role":"assistant","content":[{"type":"text","text":"Because the class was rerouted."}]}]}`
	if got := stripSwitchNote("test", []byte(body)); got != nil {
		t.Fatalf("touched a non-assistant turn:\n%s", got)
	}
}

// A note in an assistant turn that is not the last message still comes off: the
// history is replayed whole on every turn, so an old note is just as poisonous to
// the cache prefix as a fresh one.
func TestStripCoversEarlierAssistantTurns(t *testing.T) {
	note := noteSentinel + " still on kimi/kimi-k3 while gpt/gpt-5.6-sol recovers — this session moves back in ~3 min"
	clean := `{"messages":[{"role":"assistant","content":[{"type":"text","text":"First."}]},` +
		`{"role":"user","content":"more"},` +
		`{"role":"assistant","content":[{"type":"text","text":"Second."}]}]}`
	noted := `{"messages":[{"role":"assistant","content":[{"type":"text","text":"First."},{"type":"text","text":"\n\n` + note + `"}]},` +
		`{"role":"user","content":"more"},` +
		`{"role":"assistant","content":[{"type":"text","text":"Second."},{"type":"text","text":"\n\n` + note + `"}]}]}`
	got := stripSwitchNote("test", []byte(noted))
	if got == nil {
		t.Fatal("no strip happened")
	}
	if string(got) != clean {
		t.Fatalf("\n got %s\nwant %s", got, clean)
	}
}

// A thinking or tool_use block sitting between the model's text and the note is
// stepped over, and the note block still leaves with its own separator.
func TestStripSkipsNonTextBlocks(t *testing.T) {
	clean := `{"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"hm"},{"type":"text","text":"Answer."}]}]}`
	noted := `{"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"hm"},{"type":"text","text":"Answer."},{"type":"text","text":"\n\n` + noteSentinel + ` served by gpt/gpt-5.6-sol although it is degraded (open 4 min) — the brain just re-bound class helpdesk to it"}]}]}`
	got := stripSwitchNote("test", []byte(noted))
	if got == nil {
		t.Fatal("no strip happened")
	}
	if string(got) != clean {
		t.Fatalf("\n got %s\nwant %s", got, clean)
	}
}

// Encoder invariance. The sentinel is ASCII precisely so the gate is exact
// whatever the client encoded with: Go escapes < > &, Python's ensure_ascii
// escapes non-ASCII (the note's own em-dash becomes —), JSON.stringify
// escapes neither. All three must strip to something that parses equal to the
// un-annotated history.
func TestStripIsEncoderInvariant(t *testing.T) {
	const note = noteSentinel + ` answered by kimi/kimi-k3 — it is fine`
	cases := []struct{ name, answer, noted string }{
		{
			name:   "js/literal",
			answer: `Answer with an em-dash — here.`,
			noted:  `Answer with an em-dash — here.\n\n` + note,
		},
		{
			name:   "python/ensure_ascii",
			answer: `Answer with an em-dash — here.`,
			noted:  `Answer with an em-dash — here.\n\n` + noteSentinel + ` answered by kimi/kimi-k3 — it is fine`,
		},
		{
			name:   "go/SetEscapeHTML",
			answer: `Answer with <b> markup.`,
			noted:  `Answer with <b> markup.\n\n` + note,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			clean := `{"messages":[{"role":"assistant","content":"` + c.answer + `"}]}`
			noted := `{"messages":[{"role":"assistant","content":"` + c.noted + `"}]}`
			got := stripSwitchNote("test", []byte(noted))
			if got == nil {
				t.Fatalf("no strip happened for %s", noted)
			}
			assertParsesEqual(t, got, []byte(clean))
			if bytes.Contains(got, noteSentinelBytes) {
				t.Fatalf("sentinel survived: %s", got)
			}
		})
	}
}

// Defensive (§5.5(5)): a value that is nothing BUT the note. The append guard
// (§5.5(1)) makes this unreachable for a note we wrote — a noted reply always
// carries model text beside it — so these are hand-built. Both forms drop the
// whole message rather than leave content:"" or content:[], which the API rejects.
func TestStripDropsANoteOnlyMessage(t *testing.T) {
	note := noteSentinel + " answered by kimi/kimi-k3"
	for _, c := range []struct{ name, noted, clean string }{
		{
			"string form",
			`{"messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"` + note + `"},{"role":"user","content":"go on"}]}`,
			`{"messages":[{"role":"user","content":"hi"},{"role":"user","content":"go on"}]}`,
		},
		{
			"array form, sole block",
			`{"messages":[{"role":"user","content":"hi"},{"role":"assistant","content":[{"type":"text","text":"` + note + `"}]},{"role":"user","content":"go on"}]}`,
			`{"messages":[{"role":"user","content":"hi"},{"role":"user","content":"go on"}]}`,
		},
		{
			"array form, sole block, first message",
			`{"messages":[{"role":"assistant","content":[{"type":"text","text":"` + note + `"}]},{"role":"user","content":"go on"}]}`,
			`{"messages":[{"role":"user","content":"go on"}]}`,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := stripSwitchNote("test", []byte(c.noted))
			if got == nil {
				t.Fatal("no strip happened")
			}
			if string(got) != c.clean {
				t.Fatalf("\n got %s\nwant %s", got, c.clean)
			}
			var v any
			if err := json.Unmarshal(got, &v); err != nil {
				t.Fatalf("result is not valid JSON: %v\n%s", err, got)
			}
		})
	}
}

// Whitespace between tokens is the client's, and the splice must not disturb it.
func TestStripPreservesSurroundingWhitespace(t *testing.T) {
	note := noteSentinel + " answered by kimi/kimi-k3"
	noted := "{\n  \"messages\" : [\n    {\"role\": \"assistant\", \"content\": [\n      {\"type\": \"text\", \"text\": \"Answer.\"} ,\n      {\"type\": \"text\", \"text\": \"\\n\\n" + note + "\"}\n    ]}\n  ]\n}"
	clean := "{\n  \"messages\" : [\n    {\"role\": \"assistant\", \"content\": [\n      {\"type\": \"text\", \"text\": \"Answer.\"}\n    ]}\n  ]\n}"
	got := stripSwitchNote("test", []byte(noted))
	if got == nil {
		t.Fatal("no strip happened")
	}
	if string(got) != clean {
		t.Fatalf("\n got %q\nwant %q", got, clean)
	}
}

// A body the walk cannot make sense of goes upstream unchanged. A note that
// survives is cosmetic; a corrupted body is not.
func TestStripLeavesUnparseableBodiesAlone(t *testing.T) {
	note := noteSentinel + " answered by kimi/kimi-k3"
	for _, s := range []string{
		`{"messages":[{"role":"assistant","content":"` + note + `"}`, // truncated
		`[{"role":"assistant","content":"` + note + `"}]`,            // not an object
		`{"messages":{"role":"assistant","content":"` + note + `"}}`, // messages not an array
		`{"messages":[{"role":"assistant","content":42}],"x":"` + note + `"}`,
		`not json at all ` + note,
	} {
		if got := stripSwitchNote("test", []byte(s)); got != nil {
			t.Errorf("rewrote a body it could not parse:\n in  %s\n out %s", s, got)
		}
	}
}

// The counter behind §6.2(3)'s notes_stripped_total moves once per note removed.
func TestStripCountsNotesRemoved(t *testing.T) {
	note := noteSentinel + " answered by kimi/kimi-k3"
	_, before := noteStripTotals()
	body := `{"messages":[` +
		`{"role":"assistant","content":[{"type":"text","text":"One."},{"type":"text","text":"\n\n` + note + `"}]},` +
		`{"role":"assistant","content":[{"type":"text","text":"Two."},{"type":"text","text":"\n\n` + note + `"}]}]}`
	if stripSwitchNote("test", []byte(body)) == nil {
		t.Fatal("no strip happened")
	}
	_, after := noteStripTotals()
	if got := after["test"] - before["test"]; got != 2 {
		t.Fatalf("counted %d notes stripped, want 2", got)
	}
}

func assertParsesEqual(t *testing.T, got, want []byte) {
	t.Helper()
	var a, b any
	if err := json.Unmarshal(got, &a); err != nil {
		t.Fatalf("stripped body is not valid JSON: %v\n%s", err, got)
	}
	if err := json.Unmarshal(want, &b); err != nil {
		t.Fatalf("reference body is not valid JSON: %v", err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("stripped body does not parse equal to the un-annotated history:\n got %s\nwant %s", got, want)
	}
}
