package main

// Tests for the transcript tap (#60).
//
// The load-bearing ones are TestTapColdPathNoAlloc (the tap must cost NOTHING when
// nobody is listening — the whole basis of #59's gate), TestTapNeverPanics (this runs in
// the live inference path for every client, so a malformed body must degrade, never
// fail a request) and TestTapSubAgentIsKeyedSeparately (the keying the ticket calls out
// as most likely to be wrong).
//
// Most of these drive tapTurn directly, because what is being pinned down is the cursor
// arithmetic and the parse, not HTTP. The one thing that genuinely needs the real
// handler stack — that an Anthropic, a kimi and a gpt request produce the SAME event
// shape — is TestE2ETapIdenticalShapeAcrossProviders, which posts through it.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ---- fixtures ----

// tapTest resets the hub AND the tap's conversation state, and restores every tunable
// afterwards. Both stores are package-level singletons, so a prior test's cursors would
// otherwise decide this test's `from`.
func tapTest(t *testing.T) {
	t.Helper()
	evTest(t)
	msgs, blocks, text := tapMaxMessages, tapMaxBlocks, tapMaxText
	budget, keyText, depth := tapTextBudget, tapKeyText, tapMaxDepth
	convs, ttl, reserve, anchors := tapMaxConvs, tapConvTTL, tapReserve, tapMaxAnchors
	tapClear()
	t.Cleanup(func() {
		// E2e tests here drive the full handler, which spawns respTap aggregators
		// that read tapReserve; join them before restoring it.
		respTapSettle()
		tapMaxMessages, tapMaxBlocks, tapMaxText = msgs, blocks, text
		tapTextBudget, tapKeyText, tapMaxDepth = budget, keyText, depth
		tapMaxConvs, tapConvTTL, tapReserve, tapMaxAnchors = convs, ttl, reserve, anchors
		tapClear()
	})
}

func tapClear() {
	lan.tapMu.Lock()
	lan.tapConvs = map[string]*tapConv{}
	lan.tapAnchors = map[string]tapAnchor{}
	lan.tapMu.Unlock()
}

// tapFrame is one captured turn: the decoded payload plus its raw JSON, because the
// "identical shape" criterion is about the KEY SET, which a decode into a struct would
// silently normalize away.
type tapFrame struct {
	kind string
	raw  json.RawMessage
	ev   tapTurnEvent
}

// tapListen attaches a real subscriber (which is also what arms the hub) and returns a
// drain. Going through the hub rather than stubbing publish is deliberate: it proves
// the payload survives renderFrame, including the oversize backstop.
func tapListen(t *testing.T) func() []tapFrame {
	t.Helper()
	sub, _, _ := hub.attach("tap-test", "")
	t.Cleanup(func() { hub.detach(sub) })
	return func() []tapFrame {
		var out []tapFrame
		for {
			select {
			case f := <-sub.ch:
				out = append(out, tapParseFrame(t, f.b))
			default:
				return out
			}
		}
	}
}

func tapParseFrame(t *testing.T, b []byte) tapFrame {
	t.Helper()
	if len(b) > evMaxEventBytes+256 { // +256: the id/event/data line prefixes
		t.Fatalf("frame is %d bytes, over the %d ceiling — the hub would have stubbed it", len(b), evMaxEventBytes)
	}
	_, rest, ok := strings.Cut(string(b), "\ndata: ")
	if !ok {
		t.Fatalf("frame has no data line: %q", b)
	}
	line, _, _ := strings.Cut(rest, "\n")
	var env struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(line), &env); err != nil {
		t.Fatalf("frame data is not JSON (%v): %q", err, line)
	}
	f := tapFrame{kind: env.Type, raw: env.Data}
	if env.Type == "turn" {
		if err := json.Unmarshal(env.Data, &f.ev); err != nil {
			t.Fatalf("turn payload does not decode: %v", err)
		}
	}
	return f
}

// tapPost drives one request through the tap as an Anthropic lane route would.
func tapPost(session, body string) {
	tapPostCtx(tapLaneCtx("default"), session, body, "claude-fable-1", " ROUTED [acct] lane=default")
}

func tapPostCtx(ctx context.Context, session, body, model, tag string) {
	r := httptest.NewRequest("POST", "/v1/messages", nil)
	if session != "" {
		r.Header.Set("x-claude-code-session-id", session)
	}
	lan.tapTurn(ctx, r, []byte(body), model, tag, "acct", "deadbeef")
}

func tapLaneCtx(lane string) context.Context {
	return context.WithValue(context.Background(), infoKey,
		reqInfo{lane: lane, eff: "acct", state: stateRouted})
}

// tapConvo renders a messages array of n turns: [user, assistant, user, ...].
func tapConvo(first string, n int) string {
	var b strings.Builder
	b.WriteString(`{"model":"claude-fable-1","system":"you are a proxy","messages":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		role, text := "user", fmt.Sprintf("turn %d", i)
		if i == 0 {
			text = first
		}
		if i%2 == 1 {
			role = "assistant"
		}
		fmt.Fprintf(&b, `{"role":%q,"content":[{"type":"text","text":%q}]}`, role, text)
	}
	b.WriteString(`]}`)
	return b.String()
}

func tapOnly(t *testing.T, frames []tapFrame) tapTurnEvent {
	t.Helper()
	var turns []tapFrame
	for _, f := range frames {
		if f.kind == "turn" {
			turns = append(turns, f)
		}
	}
	if len(turns) != 1 {
		t.Fatalf("got %d turn events, want exactly 1", len(turns))
	}
	return turns[0].ev
}

// ---- the acceptance criteria ----

// The literal criterion, and the reason the tap exists: a growing conversation sends its
// whole history every request, and the tap must emit only what is new.
func TestTapThreeTurnsEmitOnlyTheTail(t *testing.T) {
	tapTest(t)
	drain := tapListen(t)

	tapPost("sess-A", tapConvo("hello", 1))
	first := tapOnly(t, drain())
	if first.From != 0 || first.To != 0 || first.New != 1 || first.Total != 1 {
		t.Fatalf("turn 1: from=%d to=%d new=%d total=%d, want 0/0/1/1", first.From, first.To, first.New, first.Total)
	}

	tapPost("sess-A", tapConvo("hello", 3))
	second := tapOnly(t, drain())
	if second.From != 1 || second.To != 2 || second.New != 2 || second.Total != 3 {
		t.Fatalf("turn 2: from=%d to=%d new=%d total=%d, want 1/2/2/3", second.From, second.To, second.New, second.Total)
	}

	tapPost("sess-A", tapConvo("hello", 5))
	third := tapOnly(t, drain())
	if third.From != 3 || third.To != 4 || third.New != 2 {
		t.Fatalf("turn 3: from=%d to=%d new=%d, want 3/4/2", third.From, third.To, third.New)
	}

	// Not just the counts: the indices must be contiguous and never repeat, which is
	// what "never a re-send of history" actually means to a consumer stitching them.
	want := 0
	for _, ev := range []tapTurnEvent{first, second, third} {
		for _, m := range ev.Messages {
			if m.Index != want {
				t.Fatalf("message index %d out of order, want %d", m.Index, want)
			}
			want++
		}
	}
	if want != 5 {
		t.Fatalf("saw %d messages across three turns, want 5", want)
	}

	// A fourth request that adds nothing must produce nothing — an identical array is
	// a retry, not a turn.
	tapPost("sess-A", tapConvo("hello", 5))
	if got := drain(); len(got) != 0 {
		t.Fatalf("an unchanged array emitted %d events, want 0", len(got))
	}
	if ev := third; ev.Turn != 3 {
		t.Fatalf("turn counter = %d after three emitted turns, want 3", ev.Turn)
	}
}

// The risk the ticket calls out. A sub-agent issues its OWN /v1/messages under (most
// likely) the parent's session id; keying on the session alone would interleave its
// turns into the parent transcript and make the parent's cursor jump.
func TestTapSubAgentIsKeyedSeparately(t *testing.T) {
	tapTest(t)
	drain := tapListen(t)

	tapPost("sess-A", tapConvo("parent conversation", 3))
	parent := tapOnly(t, drain())

	tapPost("sess-A", tapConvo("sub-agent task brief", 1))
	sub := tapOnly(t, drain())

	if parent.ConversationKey == sub.ConversationKey {
		t.Fatalf("parent and sub-agent share conversation_key %q — the sub-agent's turns interleave into the parent transcript", parent.ConversationKey)
	}
	if parent.Session != sub.Session {
		t.Fatalf("sessions differ (%q vs %q) — the fixture is wrong; the whole point is the SAME session id", parent.Session, sub.Session)
	}
	if sub.From != 0 || sub.New != 1 {
		t.Fatalf("sub-agent: from=%d new=%d, want 0/1 — its cursor must be its own", sub.From, sub.New)
	}

	// And the parent's cursor must be untouched by the detour.
	tapPost("sess-A", tapConvo("parent conversation", 5))
	back := tapOnly(t, drain())
	if back.From != 3 || back.ConversationKey != parent.ConversationKey {
		t.Fatalf("parent resumed at %d on key %q, want 3 on %q", back.From, back.ConversationKey, parent.ConversationKey)
	}
}

// Two sessions whose ids share a prefix are still two conversations. tapTurn keys the
// internal cursor on the FULL session id "so two sessions never merge"; emitting an
// 8-char prefix as conversation_key would have handed scry a key that merges exactly
// what the proxy kept apart.
func TestTapSessionPrefixCollisionKeepsKeysDistinct(t *testing.T) {
	tapTest(t)
	drain := tapListen(t)

	// Same leading 8 chars AND identical content, so the full session id is the only
	// thing left that can tell them apart.
	a := "abcd1234-0000-4000-8000-000000000001"
	b := "abcd1234-9999-4999-8999-999999999999"

	tapPost(a, tapConvo("same opening line", 3))
	evA := tapOnly(t, drain())
	tapPost(b, tapConvo("same opening line", 3))
	evB := tapOnly(t, drain())

	if evA.Session != evB.Session {
		t.Fatalf("fixture is wrong: Session %q vs %q — they must share the displayed prefix", evA.Session, evB.Session)
	}
	if evA.ConversationKey == evB.ConversationKey {
		t.Fatalf("prefix-colliding sessions share conversation_key %q — scry interleaves two transcripts", evA.ConversationKey)
	}
	if evB.From != 0 || evB.New != 3 {
		t.Fatalf("second session: from=%d new=%d, want 0/3 — it must carry its own cursor", evB.From, evB.New)
	}
}

// The frame budget has to cover the WHOLE event. ModelRequested is the client's `model`
// string verbatim and Route embeds it, so both are sized by the caller: measuring only
// the messages let a huge model name push the frame past evMaxEventBytes and get the
// entire thing stubbed by renderFrame — losing the envelope, and with it the record that
// a turn happened at all.
func TestTapHostileModelCannotBlowTheFrame(t *testing.T) {
	tapTest(t)
	drain := tapListen(t)

	huge := strings.Repeat("m", 40<<10)
	// UNROUTABLE is the branch that echoes the model into the tag verbatim.
	tapPostCtx(tapLaneCtx("default"), "sess-A", tapConvo("hello", 3), huge, " UNROUTABLE "+huge)

	// tapParseFrame already fails if the frame exceeded the ceiling, so reaching here at
	// all is half the assertion.
	ev := tapOnly(t, drain())

	if len(ev.ModelRequested) > tapMaxIdent || len(ev.Route) > tapMaxIdent {
		t.Fatalf("unclamped metadata: model_requested=%d route=%d, want <= %d each",
			len(ev.ModelRequested), len(ev.Route), tapMaxIdent)
	}
	if ev.ConversationKey == "" || ev.Total != 3 {
		t.Fatalf("envelope lost: conversation_key=%q total=%d — clamping exists so this survives", ev.ConversationKey, ev.Total)
	}
	if len(ev.Messages) == 0 {
		t.Fatalf("messages dropped — clamping the metadata should have left room for them")
	}
}

// /compact replaces the array. A shorter array is the designed shape of that, so it must
// open an epoch rather than read as a corrupt or truncated transcript.
func TestTapCompactionOpensNewEpoch(t *testing.T) {
	tapTest(t)
	drain := tapListen(t)

	tapPost("sess-A", tapConvo("hello", 5))
	before := tapOnly(t, drain())
	if before.Epoch != 1 || before.Compacted {
		t.Fatalf("epoch=%d compacted=%v before any compaction, want 1/false", before.Epoch, before.Compacted)
	}

	// Microcompaction: the head survives (so the key is stable), the middle is gone.
	tapPost("sess-A", tapConvo("hello", 2))
	after := tapOnly(t, drain())
	if !after.Compacted {
		t.Fatal("a shorter array did not open a new epoch — the transcript would read as truncated")
	}
	if after.Epoch != 2 || after.From != 0 || after.Total != 2 || after.New != 2 {
		t.Fatalf("epoch=%d from=%d total=%d new=%d, want 2/0/2/2", after.Epoch, after.From, after.Total, after.New)
	}
	if after.ConversationKey != before.ConversationKey {
		t.Fatal("compaction changed the conversation key — an epoch and a new conversation must be distinguishable")
	}

	// And the new epoch's cursor picks up from the SHORTER array, not the old length.
	tapPost("sess-A", tapConvo("hello", 3))
	next := tapOnly(t, drain())
	if next.Epoch != 2 || next.From != 2 || next.New != 1 {
		t.Fatalf("post-compaction: epoch=%d from=%d new=%d, want 2/2/1", next.Epoch, next.From, next.New)
	}
}

// The falsified assumption. Keying on the RAW bytes of system + messages[0] is the
// obvious implementation and it is wrong: Claude Code moves its cache_control
// breakpoints between requests, so those bytes are not stable across a conversation.
// The key must survive the drift or every request re-sends the whole history.
func TestTapCacheControlDriftKeepsTheKey(t *testing.T) {
	tapTest(t)
	drain := tapListen(t)

	withBreakpoint := `{"model":"m","system":[{"type":"text","text":"sys","cache_control":{"type":"ephemeral"}}],` +
		`"messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}]}`
	moved := `{"model":"m","system":[{"type":"text","text":"sys"}],` +
		`"messages":[{"role":"user","content":[{"type":"text","text":"hello"}]},` +
		`{"role":"assistant","content":[{"type":"text","text":"hi","cache_control":{"type":"ephemeral"}}]}]}`

	tapPost("sess-A", withBreakpoint)
	one := tapOnly(t, drain())
	tapPost("sess-A", moved)
	two := tapOnly(t, drain())

	if one.ConversationKey != two.ConversationKey {
		t.Fatalf("cache_control drift re-keyed the conversation (%q -> %q) — every request would re-send history",
			one.ConversationKey, two.ConversationKey)
	}
	if two.From != 1 || two.New != 1 {
		t.Fatalf("second request: from=%d new=%d, want 1/1", two.From, two.New)
	}
}

// Behind the HTTPS-proxy entry Claude Code believes it talks to Anthropic directly and
// writes per-request fields into the billing-header block that opens `system`. Keying on
// that block gave nearly every request its own key (nearly every turn in a journal read as
// a first turn): the block must not reach the key, and nothing else may
// leave it.
func TestTapBillingHeaderDriftKeepsTheKey(t *testing.T) {
	tapTest(t)
	drain := tapListen(t)

	body := func(header, sys string, n int) string {
		msgs := []string{`{"role":"user","content":[{"type":"text","text":"hello"}]}`}
		for i := 1; i < n; i++ {
			role := map[bool]string{true: "assistant", false: "user"}[i%2 == 1]
			msgs = append(msgs, fmt.Sprintf(`{"role":%q,"content":[{"type":"text","text":"m%d"}]}`, role, i))
		}
		return `{"model":"m","system":[{"type":"text","text":` + fmt.Sprintf("%q", header) + `},` +
			`{"type":"text","text":` + fmt.Sprintf("%q", sys) + `,"cache_control":{"type":"ephemeral"}}],` +
			`"messages":[` + strings.Join(msgs, ",") + `]}`
	}
	h1 := "x-anthropic-billing-header: cc_version=2.1.322; cc_entrypoint=cli; cc_prev_req=req_011aaa; cc_prompt_index=1; cc_turn_index=1;"
	h2 := "x-anthropic-billing-header: cc_version=2.1.322; cc_entrypoint=cli; cc_prev_req=req_011bbb; cc_prompt_index=1; cc_turn_index=2;"

	tapPost("sess-A", body(h1, "you are Claude Code", 1))
	one := tapOnly(t, drain())
	tapPost("sess-A", body(h2, "you are Claude Code", 3))
	two := tapOnly(t, drain())

	if one.ConversationKey != two.ConversationKey || one.ContentFP != two.ContentFP {
		t.Fatalf("a billing-header change re-keyed the conversation (%q -> %q)", one.ConversationKey, two.ConversationKey)
	}
	if two.From != 1 || two.New != 2 || two.Turn != 2 {
		t.Fatalf("second request: from=%d new=%d turn=%d, want 1/2/2", two.From, two.New, two.Turn)
	}

	// The rest of the system prompt still keys: another prompt is another conversation.
	tapPost("sess-A", body(h2, "you are a sub-agent", 1))
	other := tapOnly(t, drain())
	if other.ConversationKey == one.ConversationKey {
		t.Fatalf("a different system prompt kept the key %q", one.ConversationKey)
	}
}

// Claude Code also sends role:"system" messages inside `messages`, re-rendered per
// request. One ahead of the first user message must not key the conversation; the user
// message must.
func TestTapSystemMessageAheadOfThePromptKeepsTheKey(t *testing.T) {
	tapTest(t)
	drain := tapListen(t)

	first := `{"model":"m","system":"sys","messages":[{"role":"system","content":"context as of 10:01"},` +
		`{"role":"user","content":[{"type":"text","text":"hello"}]}]}`
	later := `{"model":"m","system":"sys","messages":[{"role":"system","content":[{"type":"text","text":"context as of 10:02"}]},` +
		`{"role":"user","content":[{"type":"text","text":"hello"}]},{"role":"assistant","content":[{"type":"text","text":"hi"}]},` +
		`{"role":"user","content":[{"type":"text","text":"more"}]}]}`
	otherPrompt := `{"model":"m","system":"sys","messages":[{"role":"system","content":"context as of 10:02"},` +
		`{"role":"user","content":[{"type":"text","text":"a sub-agent's brief"}]}]}`

	tapPost("sess-A", first)
	one := tapOnly(t, drain())
	tapPost("sess-A", later)
	two := tapOnly(t, drain())
	tapPost("sess-A", otherPrompt)
	three := tapOnly(t, drain())

	if one.ConversationKey != two.ConversationKey {
		t.Fatalf("a re-rendered system message re-keyed the conversation (%q -> %q)", one.ConversationKey, two.ConversationKey)
	}
	if three.ConversationKey == one.ConversationKey {
		t.Fatalf("a different first user message kept the key %q", one.ConversationKey)
	}
}

// ---- Claude Code's thread protocol ----

const tapThreadBilling = "x-anthropic-billing-header: cc_version=2.1.322; cc_entrypoint=cli; cc_turn_index=1;"

// tapThreadBody renders a request as Claude Code 2.1.322 sends one. A create carries
// the full system and tools; a continue carries only the uncached system blocks (the
// billing header and one constant paragraph), no tools, and only the messages after
// the reply it names.
func tapThreadBody(thread string, msgs ...string) string {
	sys := `[{"type":"text","text":"` + tapThreadBilling + `"},{"type":"text","text":"Respond in the user's language."}]`
	extra := ""
	if !strings.Contains(thread, `"continue"`) {
		sys = `[{"type":"text","text":"` + tapThreadBilling + `"},` +
			`{"type":"text","text":"You are Claude Code, a long cached system prompt.","cache_control":{"type":"ephemeral"}},` +
			`{"type":"text","text":"Respond in the user's language."}]`
		extra = `"tools":[{"name":"Bash","input_schema":{"type":"object"}}],`
	}
	if thread != "" {
		extra += `"thread":` + thread + `,`
	}
	return `{"model":"claude-fable-1","system":` + sys + `,` + extra + `"stream":true,"messages":[` + strings.Join(msgs, ",") + `]}`
}

func tapUserMsg(text string) string {
	return fmt.Sprintf(`{"role":"user","content":[{"type":"text","text":%q}]}`, text)
}

func tapToolResultMsg(id, text string) string {
	return fmt.Sprintf(`{"role":"user","content":[{"type":"tool_result","tool_use_id":%q,"content":%q}]}`, id, text)
}

func tapContinue(prev string) string {
	return `{"type":"continue","previous_message_id":"` + prev + `"}`
}

const tapCreate = `{"type":"create"}`

// tapTurnIdent drives one request through the tap and returns the identity the response
// tap would be handed.
func tapTurnIdent(session, body string) tapRespIdent {
	r := httptest.NewRequest("POST", "/v1/messages", nil)
	r.Header.Set("x-claude-code-session-id", session)
	return lan.tapTurn(tapLaneCtx("default"), r, []byte(body), "claude-fable-1", " ROUTED [acct] lane=default", "acct", "deadbeef")
}

// tapReply runs the reply to id through the real response tap, its message_start
// carrying msgID, and returns the assistant_message it published.
func tapReply(t *testing.T, id tapRespIdent, msgID string) respMessageEvent {
	t.Helper()
	sse := strings.Replace(respSSEFixture(), `"id":"msg_01"`, `"id":"`+msgID+`"`, 1)
	frames := respDrive(t, id, 200, "text/event-stream", sse, 61)
	return respDecodeMsg(t, respOne(t, frames, "assistant_message"))
}

func tapIndices(ev tapTurnEvent) []int {
	var out []int
	for _, m := range ev.Messages {
		out = append(out, m.Index)
	}
	return out
}

func tapRawHas(t *testing.T, frames []tapFrame, field string) bool {
	t.Helper()
	for _, f := range frames {
		if f.kind == "turn" {
			return strings.Contains(string(f.raw), `"`+field+`"`)
		}
	}
	t.Fatalf("no turn frame")
	return false
}

// A continue names the reply it follows and carries only what came after it, under a
// cut-down system prompt. It must land in the create's conversation, numbered as if the
// whole array had been sent, with no epoch bump: a sliced array is not a compaction.
func TestTapThreadContinueKeepsTheKey(t *testing.T) {
	respTest(t)
	drain := tapListen(t)

	id1 := tapTurnIdent("sess-T", tapThreadBody(tapCreate, tapUserMsg("hello")))
	create := tapOnly(t, drain())
	if create.From != 0 || create.Total != 1 || id1.index != 1 {
		t.Fatalf("create: from=%d total=%d reply index=%d, want 0/1/1", create.From, create.Total, id1.index)
	}
	reply := tapReply(t, id1, "msg_X")
	drain()
	if reply.ConversationKey != create.ConversationKey || reply.Index != 1 {
		t.Fatalf("reply: key=%q index=%d, want %q/1", reply.ConversationKey, reply.Index, create.ConversationKey)
	}

	id2 := tapTurnIdent("sess-T", tapThreadBody(tapContinue("msg_X"), tapToolResultMsg("toolu_01", "file.txt")))
	frames := drain()
	cont := tapOnly(t, frames)
	if cont.ConversationKey != create.ConversationKey || cont.ContentFP != create.ContentFP {
		t.Fatalf("a continue re-keyed the conversation (%q -> %q)", create.ConversationKey, cont.ConversationKey)
	}
	if cont.Turn != 2 || cont.Epoch != 1 || cont.Compacted {
		t.Fatalf("continue: turn=%d epoch=%d compacted=%v, want 2/1/false", cont.Turn, cont.Epoch, cont.Compacted)
	}
	// Full conversation: hello (0), the anchor reply (1), the tool_result (2).
	if cont.From != 2 || cont.To != 2 || cont.Total != 3 || cont.New != 1 || cont.ElidedHead != 0 || cont.Truncated {
		t.Fatalf("continue: from=%d to=%d total=%d new=%d elided=%d truncated=%v, want 2/2/3/1/0/false",
			cont.From, cont.To, cont.Total, cont.New, cont.ElidedHead, cont.Truncated)
	}
	if got := tapIndices(cont); len(got) != 1 || got[0] != 2 {
		t.Fatalf("continue message indices %v, want [2]", got)
	}
	if cont.ThreadOrphan || tapRawHas(t, frames, "thread_orphan") {
		t.Fatalf("a followed continue is marked orphan")
	}

	// The response side lands in the same conversation, after the delta.
	if id2.index != 3 {
		t.Fatalf("continue reply index %d, want 3", id2.index)
	}
	r2 := tapReply(t, id2, "msg_Y")
	if r2.ConversationKey != create.ConversationKey || r2.Index != 3 || r2.Epoch != 1 || r2.Turn != 2 {
		t.Fatalf("continue reply: key=%q index=%d epoch=%d turn=%d, want %q/3/1/2",
			r2.ConversationKey, r2.Index, r2.Epoch, r2.Turn, create.ConversationKey)
	}
}

// Every continue names the reply before it; the conversation stays one key and one
// numbering all the way down the chain.
func TestTapThreadContinueChain(t *testing.T) {
	respTest(t)
	drain := tapListen(t)

	id := tapTurnIdent("sess-C", tapThreadBody(tapCreate, tapUserMsg("hello")))
	create := tapOnly(t, drain())
	tapReply(t, id, "msg_1") // index 1
	drain()

	steps := []struct {
		prev, reply string
		delta       []string
		from, total int
		indices     []int
		replyIndex  int
	}{
		{"msg_1", "msg_2", []string{tapToolResultMsg("toolu_1", "a"), tapUserMsg("and then")}, 2, 4, []int{2, 3}, 4},
		{"msg_2", "msg_3", []string{tapToolResultMsg("toolu_2", "b")}, 5, 6, []int{5}, 6},
		{"msg_3", "msg_4", []string{tapToolResultMsg("toolu_3", "c"), tapUserMsg("x"), tapUserMsg("y")}, 7, 10, []int{7, 8, 9}, 10},
	}
	for i, st := range steps {
		id = tapTurnIdent("sess-C", tapThreadBody(tapContinue(st.prev), st.delta...))
		ev := tapOnly(t, drain())
		if ev.ConversationKey != create.ConversationKey || ev.Epoch != 1 || ev.Compacted || ev.ThreadOrphan {
			t.Fatalf("step %d: key=%q epoch=%d compacted=%v orphan=%v, want %q/1/false/false",
				i, ev.ConversationKey, ev.Epoch, ev.Compacted, ev.ThreadOrphan, create.ConversationKey)
		}
		if ev.Turn != int64(i+2) || ev.From != st.from || ev.Total != st.total || ev.ElidedHead != 0 {
			t.Fatalf("step %d: turn=%d from=%d total=%d elided=%d, want %d/%d/%d/0",
				i, ev.Turn, ev.From, ev.Total, ev.ElidedHead, i+2, st.from, st.total)
		}
		if got := tapIndices(ev); fmt.Sprint(got) != fmt.Sprint(st.indices) {
			t.Fatalf("step %d: indices %v, want %v", i, got, st.indices)
		}
		r := tapReply(t, id, st.reply)
		drain()
		if r.ConversationKey != create.ConversationKey || r.Index != st.replyIndex {
			t.Fatalf("step %d reply: key=%q index=%d, want %q/%d", i, r.ConversationKey, r.Index, create.ConversationKey, st.replyIndex)
		}
	}

	// A retried continue (same anchor, same delta) is not a new turn.
	tapTurnIdent("sess-C", tapThreadBody(tapContinue("msg_3"), steps[2].delta...))
	for _, f := range drain() {
		if f.kind == "turn" {
			t.Fatalf("a retried continue emitted a turn: from=%d total=%d", f.ev.From, f.ev.Total)
		}
	}
}

// A continue naming a reply the tap never saw (a proxy restart, or another process
// served the create) keeps the key its own content gives it, as every request had before
// continues were followed, and says so. It is never folded into a conversation it might
// belong to — not even the session's live one whose last delta it repeats — and a reply
// from another session is not an anchor.
func TestTapThreadOrphanContinue(t *testing.T) {
	respTest(t)
	drain := tapListen(t)

	id := tapTurnIdent("sess-O", tapThreadBody(tapCreate, tapUserMsg("hello")))
	live := tapOnly(t, drain())
	tapReply(t, id, "msg_live")
	drain()
	delta := tapToolResultMsg("toolu_01", "file.txt")
	tapTurnIdent("sess-O", tapThreadBody(tapContinue("msg_live"), delta))
	if ev := tapOnly(t, drain()); ev.ConversationKey != live.ConversationKey || ev.Total != 3 {
		t.Fatalf("setup: the live continue did not follow its anchor")
	}

	orphanBody := tapThreadBody(tapContinue("msg_unknown"), delta)
	tapTurnIdent("sess-O", orphanBody)
	frames := drain()
	orphan := tapOnly(t, frames)
	if !orphan.ThreadOrphan || !tapRawHas(t, frames, "thread_orphan") {
		t.Fatalf("an unknown previous_message_id is not marked thread_orphan")
	}
	if orphan.ConversationKey == live.ConversationKey {
		t.Fatalf("an orphan continue was merged into the live conversation %q", live.ConversationKey)
	}
	if orphan.From != 0 || orphan.Total != 1 || orphan.Epoch != 1 || orphan.Turn != 1 {
		t.Fatalf("orphan: from=%d total=%d epoch=%d turn=%d, want 0/1/1/1", orphan.From, orphan.Total, orphan.Epoch, orphan.Turn)
	}
	// Today's content key: what the same body gets with no thread at all.
	plain := tapTurnIdent("sess-O", strings.Replace(orphanBody, `"thread":`+tapContinue("msg_unknown")+`,`, "", 1))
	if plain.convKey != orphan.ConversationKey || plain.contentFP != orphan.ContentFP {
		t.Fatalf("orphan key %q is not the content key %q", orphan.ConversationKey, plain.convKey)
	}

	// The live conversation's cursor did not move: its next continue still follows on.
	id = tapTurnIdent("sess-O", tapThreadBody(tapContinue("msg_live"), delta, tapUserMsg("more")))
	if ev := tapOnly(t, drain()); ev.ConversationKey != live.ConversationKey || ev.From != 3 || ev.Total != 4 {
		t.Fatalf("live conversation after the orphan: key=%q from=%d total=%d, want %q/3/4", ev.ConversationKey, ev.From, ev.Total, live.ConversationKey)
	}

	// Another session naming this session's reply is an orphan too.
	tapTurnIdent("sess-other", tapThreadBody(tapContinue("msg_live"), delta))
	if ev := tapOnly(t, drain()); !ev.ThreadOrphan || ev.ConversationKey == live.ConversationKey {
		t.Fatalf("a continue from another session followed this session's anchor")
	}
}

// The reply store is bounded like the conversation store.
func TestTapThreadAnchorsAreBounded(t *testing.T) {
	tapTest(t)
	evArm()
	tapMaxAnchors = 8
	id := tapTurnIdent("sess-B", tapThreadBody(tapCreate, tapUserMsg("hello")))
	for i := 0; i < 50; i++ {
		lan.tapRememberReply(fmt.Sprintf("msg_%d", i), &id, time.Now())
	}
	lan.tapMu.Lock()
	n := len(lan.tapAnchors)
	lan.tapMu.Unlock()
	if n > tapMaxAnchors {
		t.Fatalf("remembering %d replies with a cap of %d", n, tapMaxAnchors)
	}
}

// The gate. Everything expensive in this file sits behind eventsArmed(), so an idle
// proxy — which is the normal state — must pay nothing at all.
func TestTapColdPathNoAlloc(t *testing.T) {
	tapTest(t)
	evDisarm()
	body := []byte(tapConvo("hello", 6))
	ctx := tapLaneCtx("default")
	r := httptest.NewRequest("POST", "/v1/messages", nil)
	r.Header.Set("x-claude-code-session-id", "sess-cold")

	n := testing.AllocsPerRun(1000, func() {
		lan.tapTurn(ctx, r, body, "claude-fable-1", " ROUTED [acct] lane=default", "acct", "deadbeef")
	})
	if n != 0 {
		t.Fatalf("cold tap allocated %.1f times per request — the inference path must pay nothing when nobody is listening", n)
	}
	lan.tapMu.Lock()
	tracked := len(lan.tapConvs)
	lan.tapMu.Unlock()
	if tracked != 0 {
		t.Fatalf("cold tap recorded %d conversations — it must not even reach the cursor store", tracked)
	}
}

// count_tokens carries the same conversation but is a pricing probe, not a turn.
// Emitting it would double every event and advance the cursor ahead of the real request.
func TestTapIgnoresCountTokensAndNonMessages(t *testing.T) {
	tapTest(t)
	drain := tapListen(t)
	body := []byte(tapConvo("hello", 2))
	ctx := tapLaneCtx("default")

	for _, path := range []string{"/v1/messages/count_tokens", "/v1/models", "/v1/messages/", "/_switch/status"} {
		r := httptest.NewRequest("POST", path, nil)
		lan.tapTurn(ctx, r, body, "m", "", "acct", "fp")
	}
	if got := drain(); len(got) != 0 {
		t.Fatalf("%d events from non-/v1/messages paths, want 0", len(got))
	}
	// ...and the real request that follows must still be the FIRST turn.
	tapPost("sess-A", tapConvo("hello", 2))
	if ev := tapOnly(t, drain()); ev.From != 0 || ev.New != 2 {
		t.Fatalf("from=%d new=%d after count_tokens, want 0/2 — the probe advanced the cursor", ev.From, ev.New)
	}
}

// ---- panic safety on hostile bodies ----

// This runs in front of every client including the operator's own session, so the bar is
// not "handles bad JSON" but "cannot take the request down". Every case here must either
// produce nothing or produce a well-formed frame; none may panic.
func TestTapNeverPanicsOnHostileBodies(t *testing.T) {
	tapTest(t)
	drain := tapListen(t)

	deep := `{"messages":[{"role":"user","content":` + strings.Repeat("[", 20000) + strings.Repeat("]", 20000) + `}]}`
	wide := `{"messages":[` + strings.Repeat(`{"role":"user","content":"x"},`, 500) + `{"role":"user","content":"y"}]}`

	cases := []struct{ name, body string }{
		{"empty", ""},
		{"not json", "\x00\x01\x02 not json at all"},
		{"top-level array", `[{"role":"user"}]`},
		{"top-level string", `"just a string"`},
		{"truncated object", `{`},
		{"truncated key", `{"messages"`},
		{"truncated array", `{"messages":[{"role":"user","content":"hi"}`},
		{"messages null", `{"messages":null}`},
		{"messages string", `{"messages":"nope"}`},
		{"messages object", `{"messages":{"role":"user"}}`},
		{"messages empty", `{"messages":[]}`},
		{"elements not objects", `{"messages":[null,1,"x",[],true]}`},
		{"wrong field types", `{"messages":[{"role":7,"content":9}],"system":42,"stream":"yes"}`},
		{"block wrong types", `{"messages":[{"role":"user","content":[{"type":5,"text":[],"name":{},"is_error":"maybe"}]}]}`},
		{"tool_result nested", `{"messages":[{"role":"user","content":[{"type":"tool_result","content":{"a":{"b":{"c":{"d":{"e":1}}}}}}]}]}`},
		{"image without source", `{"messages":[{"role":"user","content":[{"type":"image"}]}]}`},
		{"duplicate messages", `{"messages":[],"messages":[{"role":"user","content":"x"}]}`},
		{"system after messages", `{"messages":[{"role":"user","content":"x"}],"system":"late"}`},
		{"lone surrogate", `{"messages":[{"role":"user","content":"\ud800"}]}`},
		{"huge number", `{"messages":[{"role":"user","content":1e999}]}`},
		{"deep nesting", deep},
		{"many messages", wide},
		{"trailing garbage", `{"messages":[{"role":"user","content":"x"}]}}}}`},
		{"nul in string", "{\"messages\":[{\"role\":\"user\",\"content\":\"a\\u0000b\"}]}"},
	}

	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("tapTurn panicked on %s: %v", c.name, p)
				}
			}()
			// A distinct session per case so no case is silently skipped by another
			// case's cursor.
			tapPost(fmt.Sprintf("hostile-%d", i), c.body)
			for _, f := range drain() {
				if f.kind != "turn" {
					continue
				}
				// Whatever came out must be coherent enough to render.
				if f.ev.Total < f.ev.New {
					t.Fatalf("%s: new=%d exceeds total=%d", c.name, f.ev.New, f.ev.Total)
				}
			}
		})
	}
}

// ---- frame-size discipline ----

// A single tool_result can carry a 200 KB file read. The tap clips it itself rather than
// letting renderFrame stub the whole frame: the cursor has already advanced, so a stub
// would be permanent loss with no conversation key to hang it on.
func TestTapClipsFatToolResultsAndKeepsTheEnvelope(t *testing.T) {
	tapTest(t)
	drain := tapListen(t)

	fat := strings.Repeat("A", 200_000)
	body := `{"model":"m","messages":[{"role":"user","content":"read it"},` +
		`{"role":"assistant","content":[{"type":"tool_use","id":"tu_1","name":"Read","input":{"file_path":"/etc/hosts"}}]},` +
		`{"role":"user","content":[{"type":"tool_result","tool_use_id":"tu_1","content":"` + fat + `"}]}]}`

	tapPost("sess-fat", body)
	ev := tapOnly(t, drain())
	if !ev.Truncated {
		t.Fatal("a 200 KB tool_result was not reported as truncated")
	}
	if len(ev.Messages) != 3 {
		t.Fatalf("carried %d messages, want 3 — the envelope and the tail must survive a clip", len(ev.Messages))
	}
	last := ev.Messages[2].Blocks[0]
	if last.Bytes < 200_000 {
		t.Fatalf("tool_result bytes=%d, want the ORIGINAL size so the clip is measurable", last.Bytes)
	}
	if !last.Clipped || len(last.Text) > tapMaxText {
		t.Fatalf("tool_result text is %d bytes (clipped=%v), want <= %d", len(last.Text), last.Clipped, tapMaxText)
	}
	// The tool_use input is the interesting half of a tool call — it must survive.
	if in := ev.Messages[1].Blocks[0]; !strings.Contains(in.Text, "/etc/hosts") || in.Name != "Read" {
		t.Fatalf("tool_use rendered as %+v, want name=Read and the input path", in)
	}
}

// Base64 image data is the single biggest thing in a real body and worthless as
// transcript. It must never be copied into an event.
func TestTapNeverEmitsImageData(t *testing.T) {
	tapTest(t)
	drain := tapListen(t)

	b64 := strings.Repeat("QUJD", 20_000)
	body := `{"model":"m","messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + b64 + `"}}]}]}`
	tapPost("sess-img", body)

	ev := tapOnly(t, drain())
	blk := ev.Messages[0].Blocks[0]
	if blk.Type != "image" || blk.Text != "" {
		t.Fatalf("image block rendered as %+v, want type=image with no text", blk)
	}
	if blk.Bytes < len(b64) {
		t.Fatalf("image bytes=%d, want at least the source size %d", blk.Bytes, len(b64))
	}
}

// The hard guarantee behind the clipping: whatever the payload turns out to be once
// escaped, the ENVELOPE survives. Shrinking the ceiling forces the fallback that a real
// body would only reach pathologically.
func TestTapOversizePayloadKeepsTheEnvelope(t *testing.T) {
	tapTest(t)
	drain := tapListen(t)
	evMaxEventBytes = tapReserve + 64 // leaves ~64 bytes for messages: nothing fits

	tapPost("sess-big", tapConvo("hello", 4))
	ev := tapOnly(t, drain())
	if len(ev.Messages) != 0 || !ev.Truncated || ev.MessagesElided != 4 {
		t.Fatalf("messages=%d truncated=%v elided=%d, want 0/true/4", len(ev.Messages), ev.Truncated, ev.MessagesElided)
	}
	if ev.ConversationKey == "" || ev.Total != 4 || ev.From != 0 {
		t.Fatalf("the envelope did not survive: %+v", ev)
	}
}

// Only the tail is retained, so a cursor further behind than that must say so rather
// than imply continuity it cannot deliver.
func TestTapElidesHeadBeyondRetention(t *testing.T) {
	tapTest(t)
	drain := tapListen(t)
	tapMaxMessages = 3

	tapPost("sess-long", tapConvo("hello", 10))
	ev := tapOnly(t, drain())
	if ev.ElidedHead != 7 || ev.From != 7 || ev.New != 3 || ev.Total != 10 {
		t.Fatalf("elided_head=%d from=%d new=%d total=%d, want 7/7/3/10",
			ev.ElidedHead, ev.From, ev.New, ev.Total)
	}
	if ev.Messages[0].Index != 7 || ev.Messages[2].Index != 9 {
		t.Fatalf("retained indices %d..%d, want 7..9", ev.Messages[0].Index, ev.Messages[2].Index)
	}
}

// ---- bookkeeping ----

// tapConvs grows with every session, sub-agent and one-shot title call. Unbounded, it is
// a slow leak in a process that is meant to run for months.
func TestTapConversationStoreIsBounded(t *testing.T) {
	tapTest(t)
	evArm() // armed with no subscriber: the ring absorbs the frames, nothing evicts
	tapMaxConvs = 8

	for i := 0; i < 50; i++ {
		tapPost(fmt.Sprintf("sess-%d", i), tapConvo(fmt.Sprintf("conversation %d", i), 1))
	}
	lan.tapMu.Lock()
	n := len(lan.tapConvs)
	lan.tapMu.Unlock()
	if n > tapMaxConvs { // the sweep runs before the insert, so the cap is the steady state
		t.Fatalf("tracking %d conversations with a cap of %d", n, tapMaxConvs)
	}
}

func TestTapEvictionPrefersExpiredThenOldest(t *testing.T) {
	tapTest(t)
	tapMaxConvs = 3
	tapConvTTL = time.Minute
	now := time.Now()

	lan.tapMu.Lock()
	lan.tapConvs["stale"] = &tapConv{seen: now.Add(-2 * time.Hour)}
	lan.tapConvs["old"] = &tapConv{seen: now.Add(-30 * time.Second)}
	lan.tapConvs["fresh"] = &tapConv{seen: now}
	lan.tapEvictLocked(now)
	_, hasStale := lan.tapConvs["stale"]
	_, hasOld := lan.tapConvs["old"]
	_, hasFresh := lan.tapConvs["fresh"]
	lan.tapMu.Unlock()

	if hasStale {
		t.Fatal("an expired conversation survived the sweep")
	}
	if !hasOld || !hasFresh {
		t.Fatalf("the sweep alone cleared the pressure, so nothing else should go (old=%v fresh=%v)", hasOld, hasFresh)
	}
}

// requestHandler is concurrent, so the cursor store is hit from many goroutines at once
// — including, when a client retries, several on the SAME conversation. Under -race
// this is what catches a torn cursor; the count assertion catches a lost one.
func TestTapConcurrentRequestsShareOneCursor(t *testing.T) {
	tapTest(t)
	evArm() // no subscriber: the ring absorbs everything, nothing evicts under load

	const goroutines, each = 8, 25
	done := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer func() { done <- struct{}{} }()
			for i := 1; i <= each; i++ {
				tapPost("sess-shared", tapConvo("hello", i))             // contended
				tapPost(fmt.Sprintf("sess-%d", g), tapConvo("hello", i)) // per-goroutine
			}
		}(g)
	}
	for g := 0; g < goroutines; g++ {
		<-done
	}

	lan.tapMu.Lock()
	shared := lan.tapConvs["sess-shared\x00"+tapFP("you are a proxy", "hello")]
	lan.tapMu.Unlock()
	if shared == nil {
		t.Fatal("the contended conversation was never recorded")
	}
	// Every goroutine walked the array 1..each, so the cursor must land on `each` —
	// a lost update would leave it short and re-send history on the next request.
	if shared.cursor != each {
		t.Fatalf("contended cursor = %d, want %d", shared.cursor, each)
	}
}

// The routing join: a turn is only useful if it can be attached to the decision that
// served it, including the decisions that never reached an upstream.
func TestTapCarriesTheRoutingDecision(t *testing.T) {
	tapTest(t)
	drain := tapListen(t)

	class := context.WithValue(context.Background(), infoKey, reqInfo{
		lane: "class:frontier", eff: "opus-native@a@example.org", state: stateStale,
		classCtx: &classRoute{tenant: lan, class: "frontier", member: classMemberPref{ID: "opus-native", Model: "claude-opus-4-6"}},
	})
	tapPostCtx(class, "sess-class", tapConvo("hello", 1), "claude-frontier", " CLASS[frontier→opus-native] STALE")
	ev := tapOnly(t, drain())
	if ev.Class != "frontier" || ev.Member != "opus-native" || ev.ModelUpstream != "claude-opus-4-6" {
		t.Fatalf("class route: class=%q member=%q upstream=%q", ev.Class, ev.Member, ev.ModelUpstream)
	}
	if ev.ModelRequested != "claude-frontier" || ev.State != "STALE" || ev.Lane != "class:frontier" {
		t.Fatalf("class route: requested=%q state=%q lane=%q", ev.ModelRequested, ev.State, ev.Lane)
	}
	if !strings.Contains(ev.Route, "CLASS[frontier") {
		t.Fatalf("route tag = %q, want the tag requestHandler computed", ev.Route)
	}

	// A 503'd route never puts a reqInfo on the context. The turn still happened, and
	// the tag is the only thing that says what became of it.
	tapPostCtx(context.Background(), "sess-refused", tapConvo("hello", 1), "claude-nope",
		" UNROUTABLE[claude-nope] (the local provider is disabled)")
	ref := tapOnly(t, drain())
	if ref.State != "REFUSED" || !strings.Contains(ref.Route, "UNROUTABLE") {
		t.Fatalf("refused route: state=%q route=%q", ref.State, ref.Route)
	}
}

// ---- cost ----

// The ticket asks for a measured, stated number on a 5 MB body. The walk is O(body) and
// unavoidable (the cursor is an index, so the count must be known), so what this pins is
// that it stays LINEAR — an accidental quadratic would blow straight past the bound.
//
// The assertion is a RATIO between two body sizes, not a wall-clock ceiling: CI is a
// shared VM, and an absolute bound there measures the neighbours' load (one CI run read
// 2.6s for a walk that takes 18ms locally). Contention scales both measurements
// together, so 5x-the-body-in-~5x-the-time survives it while a quadratic (~25x) does not.
func TestTapFiveMegabyteBodyLatency(t *testing.T) {
	tapTest(t)

	// buildBody returns a request body of at least n bytes, same shape at every size.
	buildBody := func(n int) []byte {
		var b strings.Builder
		b.WriteString(`{"model":"claude-fable-1","system":"you are a proxy","messages":[`)
		chunk := strings.Repeat("x", 4096)
		for i := 0; b.Len() < n; i++ {
			if i > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, `{"role":"user","content":[{"type":"text","text":"%s"}]}`, chunk)
		}
		b.WriteString(`]}`)
		return []byte(b.String())
	}

	ctx := tapLaneCtx("default")
	// best-of-N: the fastest run is the one least interrupted by the scheduler, which is
	// what makes the two sizes comparable on a noisy box.
	walk := func(body []byte, tag string) time.Duration {
		best := time.Hour
		for i := 0; i < 5; i++ {
			r := httptest.NewRequest("POST", "/v1/messages", nil)
			r.Header.Set("x-claude-code-session-id", fmt.Sprintf("sess-perf-%s-%d", tag, i))
			start := time.Now()
			lan.tapTurn(ctx, r, body, "claude-fable-1", " ROUTED", "acct", "fp")
			if d := time.Since(start); d < best {
				best = d
			}
		}
		return best
	}

	small, large := buildBody(1<<20), buildBody(5<<20)
	evArm()
	smallT, largeT := walk(small, "1mb"), walk(large, "5mb")
	// The cold number is the one that matters for the 99.99% of the proxy's life when
	// nobody is subscribed, so measure it alongside rather than asserting it in prose.
	evDisarm()
	cold := walk(large, "cold")

	sizeRatio := float64(len(large)) / float64(len(small))
	timeRatio := float64(largeT) / float64(smallT)
	t.Logf("%.1f MB body (%d messages): armed %v, cold %v (vs %.1f MB armed %v — %.1fx bytes in %.1fx time)",
		float64(len(large))/(1<<20), len(large)/4200, largeT, cold,
		float64(len(small))/(1<<20), smallT, sizeRatio, timeRatio)

	// Deliberately loose — this is a floor under a regression, not a benchmark. Linear is
	// ~5x, quadratic ~25x; 3x the linear cost is the alarm.
	if timeRatio > 3*sizeRatio {
		t.Fatalf("5 MB walk took %v vs %v for 1 MB (%.1fx bytes, %.1fx time) — that is not linear any more",
			largeT, smallT, sizeRatio, timeRatio)
	}
}

// ---- through the real handler stack ----

// The criterion that only the real stack can prove: the tap sits above every provider
// branch, so class requests bound to the anthropic, kimi and gpt members must produce
// the same event SHAPE — same keys, differing only in the values that describe the
// route. (Provider hops exist only on the class route since quota-model C2, and a
// class turn carries its class/member keys, so the anthropic leg is a class one too.)
func TestE2ETapIdenticalShapeAcrossProviders(t *testing.T) {
	tapTest(t)
	h, restore := respClassHarness(t)
	defer restore()
	drain := tapListen(t)

	labelsTest(t)

	shapes := map[string][]string{}
	for i, tc := range []struct{ label, model, member string }{
		{"anthropic", "claude-frontier", "lane:fable"},
		{"kimi", "claude-frontier", "kimi/kimi-k3"},
		{"gpt", "claude-frontier", "gpt/gpt-5.6"},
	} {
		h.bindTo.Store(tc.member)
		// A Claude Code create whose turn follows a /rename, so the harness,
		// folder, platform and name keys are all present and pinned with the
		// rest of the shape.
		sess := fmt.Sprintf("3f2a9c1e-7b4d-4e8a-9c2f-1a2b3c4d5e6%d", i)
		body := strings.Replace(labelBody(labelCreate, labelBilling,
			[]string{labelEnv("/Users/someone/projects/home-infra", "darwin")}, labelRenameTurn("Session renamed to: scry names", "hi")),
			`"claude-fable-1"`, strconv.Quote(tc.model), 1)
		req, _ := http.NewRequest("POST", h.proxy.URL+"/v1/messages", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer client-bearer")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-claude-code-session-id", sess)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", tc.label, err)
		}
		_ = resp.Body.Close()

		respTapSettle() // the response-side events are published by the aggregator
		var got *tapTurnEvent
		respSide := map[string]bool{}
		for _, f := range drain() {
			switch f.kind {
			case "turn":
				ev := f.ev
				got = &ev
				shapes[tc.label] = tapKeysOf(t, f.raw)
			case "turn_started", "assistant_message", "turn_ended":
				// The speed the served hop ran at rides the response side only.
				if !slices.Contains(tapKeysOf(t, f.raw), "speed") {
					t.Fatalf("%s: %s has no speed key: %s", tc.label, f.kind, f.raw)
				}
				respSide[f.kind] = true
			}
		}
		if !respSide["turn_started"] || !respSide["assistant_message"] {
			t.Fatalf("%s: response-side events %v, want turn_started and assistant_message", tc.label, respSide)
		}
		if got == nil {
			t.Fatalf("%s: no turn event — the tap must fire above the provider branch", tc.label)
		}
		if got.ModelRequested != tc.model || got.Total != 1 || got.New != 1 {
			t.Fatalf("%s: requested=%q total=%d new=%d", tc.label, got.ModelRequested, got.Total, got.New)
		}
		if len(got.Messages) != 1 || got.Messages[0].Role != "user" {
			t.Fatalf("%s: messages=%+v, want one user message — the tap must read the PRE-translation body",
				tc.label, got.Messages)
		}
		if got.Harness != harnessClaudeCode || got.Folder != "home-infra" || got.Platform != "darwin" || got.Name != "scry names" {
			t.Fatalf("%s: harness=%q folder=%q platform=%q name=%q", tc.label, got.Harness, got.Folder, got.Platform, got.Name)
		}
	}
	for _, k := range []string{"harness", "folder", "platform", "name"} {
		if !slices.Contains(shapes["anthropic"], k) {
			t.Fatalf("turn event has no %q key: %v", k, shapes["anthropic"])
		}
	}
	if slices.Contains(shapes["anthropic"], "speed") {
		t.Fatalf("the turn event names a speed, which only the served hop knows: %v", shapes["anthropic"])
	}

	base := shapes["anthropic"]
	for _, label := range []string{"kimi", "gpt"} {
		if strings.Join(shapes[label], ",") != strings.Join(base, ",") {
			t.Fatalf("%s event shape differs from anthropic:\n  %v\n  %v", label, shapes[label], base)
		}
	}
	if len(base) == 0 {
		t.Fatal("no keys captured")
	}
}

func tapKeysOf(t *testing.T, raw json.RawMessage) []string {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("payload is not an object: %v", err)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// A tool_use carries its id and a tool_result its tool_use_id, so a reader can
// pair each result with the tool that produced it. Ids only: nothing else of
// either block is added by this.
func TestTapToolBlocksCarryTheirIDs(t *testing.T) {
	tapTest(t)
	drain := tapListen(t)

	body := `{"model":"m","messages":[{"role":"user","content":"list it"},` +
		`{"role":"assistant","content":[{"type":"text","text":"ok"},{"type":"tool_use","id":"toolu_abc","name":"Bash","input":{"command":"ls"}}]},` +
		`{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_abc","content":"a b"}]}]}`
	tapPost("sess-ids", body)

	ev := tapOnly(t, drain())
	if len(ev.Messages) != 3 {
		t.Fatalf("carried %d messages, want 3", len(ev.Messages))
	}
	if b := ev.Messages[1].Blocks[1]; b.Type != "tool_use" || b.ID != "toolu_abc" || b.Name != "Bash" || b.ToolUseID != "" {
		t.Fatalf("tool_use rendered as %+v, want id=toolu_abc name=Bash", b)
	}
	if b := ev.Messages[1].Blocks[0]; b.ID != "" {
		t.Fatalf("text block carries id %q", b.ID)
	}
	if b := ev.Messages[2].Blocks[0]; b.Type != "tool_result" || b.ToolUseID != "toolu_abc" || b.ID != "" {
		t.Fatalf("tool_result rendered as %+v, want tool_use_id=toolu_abc", b)
	}
	raw, err := json.Marshal(ev.Messages[1].Blocks[1])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"id":"toolu_abc"`) {
		t.Fatalf("wire block %s has no id", raw)
	}
}
