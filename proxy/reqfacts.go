package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// oneMegaBetaMarker is the substring of the anthropic-beta flag a Claude Code
// client sends when it believes its model id carries a 1M context window (the
// `[1m]` picker suffix). The proxy forwards the header verbatim (README §
// "1M rendering") and never adds or strips this flag; it is logged because it is
// the client's WINDOW ASSUMPTION made visible — a session compacting at ~167k
// instead of ~267k is explained entirely by this flag being absent, and nothing
// else on the wire says so.
const oneMegaBetaMarker = "context-1m"

// reqFacts are the client-side knobs on a /v1/messages request that the proxy
// forwards untouched and that no other log field exposes: the 1M window claim
// and the reasoning effort. Both matter when attributing a compaction ceiling or
// a runaway thinking budget to the request that carried it (every
// Agent-tool child once arrived without the beta and at effort xhigh, invisible until
// the bodies were captured client-side).
type reqFacts struct {
	OneMega bool   // anthropic-beta carries oneMegaBetaMarker
	Effort  string // output_config.effort, else legacy top-level effort; "" if absent
	Think   string // thinking.type, ":budget" appended when declared; "" if absent
	Speed   string // top-level speed ("fast" asks for fast mode); "" if absent (not logged)

	// The bound of a request the client hangs up on (quota-model 66 §1), not
	// logged: MaxTokens is its max_tokens, nil unless an integer >= 0;
	// ServerTools says it names a server-side tool, whose results the
	// provider adds to the input mid-answer.
	MaxTokens   *int
	ServerTools bool

	// What names the harness and the session's working folder (harness.go,
	// session_labels.go), none of it logged. FirstSystem is the first `system`
	// block's text, at most reqFirstSystemMax bytes: Claude Code's billing
	// header when it is Claude Code. Thread is the top-level thread.type
	// ("create", "continue", "" when absent), read as the tap reads it
	// (tapThread). Env is the folder and platform read from the request's
	// environment block, zero when it carries none; EnvInSystem says it came
	// from text the client writes (`system`, or a system-role message in
	// `messages`) rather than from a user message. Rename is the name the
	// newest `/rename` in this turn's input gave the session, read from Claude
	// Code's output for the command (messagesRename), "" when there is none.
	FirstSystem string
	Thread      string
	Env         sessionEnv
	EnvInSystem bool
	Rename      string
}

// reqFirstSystemMax bounds FirstSystem: the billing header is one short line.
const reqFirstSystemMax = 1024

// parseReqFacts reads the facts from the CLIENT's body and header. It is
// deliberately tolerant: a non-JSON or non-messages body yields zero facts, and an
// effort that is an integer (the client's agent frontmatter allows one) renders as
// its digits rather than being dropped.
//
// It also reads what names the harness and the session (FirstSystem, Thread,
// Env, Rename), in the same pass. A continue decodes no messages for the
// environment, and messages are read for it only when `system` named none
// (messagesEnv); a rename costs one byte search unless the body holds one
// (messagesRename).
func parseReqFacts(h http.Header, body []byte) reqFacts {
	return parseReqFactsOpt(h, body, true)
}

// reqFactsBase is the part of the body every caller of the facts reads.
type reqFactsBase struct {
	Effort       json.RawMessage `json:"effort"`
	OutputConfig struct {
		Effort json.RawMessage `json:"effort"`
	} `json:"output_config"`
	Thinking struct {
		Type         string `json:"type"`
		BudgetTokens int    `json:"budget_tokens"`
	} `json:"thinking"`
	Speed      json.RawMessage `json:"speed"`
	MaxTokens  json.RawMessage `json:"max_tokens"`
	Tools      json.RawMessage `json:"tools"`
	MCPServers json.RawMessage `json:"mcp_servers"`
}

// parseReqFactsOpt is parseReqFacts; labels=false leaves out FirstSystem,
// Thread, Env and Rename, for the tap, which renders only the base facts and takes the
// harness from the request's context.
func parseReqFactsOpt(h http.Header, body []byte, labels bool) reqFacts {
	var f reqFacts
	if strings.Contains(h.Get("anthropic-beta"), oneMegaBetaMarker) {
		f.OneMega = true
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return f
	}
	var req reqFactsBase
	if !labels {
		if err := json.Unmarshal(body, &req); err != nil {
			return f
		}
	} else {
		// Read in the same pass; neither custom type ever fails the decode, so
		// an odd system or messages costs none of the base facts.
		var full struct {
			reqFactsBase
			System   reqSystem       `json:"system"`
			Thread   json.RawMessage `json:"thread"`
			Messages reqRawMessages  `json:"messages"`
		}
		if err := json.Unmarshal(body, &full); err != nil {
			return f
		}
		req = full.reqFactsBase
		f.FirstSystem = full.System.first
		if !rawAbsent(full.Thread) {
			var th tapThread
			if json.Unmarshal(full.Thread, &th) == nil {
				f.Thread = tapShort(th.Type, 16)
			}
		}
		f.Env = full.System.env
		f.EnvInSystem = f.Env.Folder != ""
		if f.Thread != "continue" && f.Env.Folder == "" {
			f.Env, f.EnvInSystem = messagesEnv(full.Messages)
		}
		f.Rename = messagesRename(full.Messages)
	}
	f.MaxTokens = rawCount(req.MaxTokens)
	f.ServerTools = !rawAbsent(req.MCPServers) || toolsTyped(req.Tools)
	f.Speed = rawScalar(req.Speed)
	f.Effort = rawScalar(req.OutputConfig.Effort)
	if f.Effort == "" {
		f.Effort = rawScalar(req.Effort)
	}
	if req.Thinking.Type != "" {
		f.Think = req.Thinking.Type
		if req.Thinking.BudgetTokens > 0 {
			f.Think += ":" + strconv.Itoa(req.Thinking.BudgetTokens)
		}
	}
	return f
}

// rawScalar renders a JSON string or number as bare text; anything else
// (null, object, array, absent) is "".
func rawScalar(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		return n.String()
	}
	return ""
}

func rawAbsent(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) == 0 || bytes.Equal(raw, []byte("null"))
}

// rawCount reads a JSON integer >= 0; anything else (absent, null, a
// fraction, a negative, a string) is nil.
func rawCount(raw json.RawMessage) *int {
	if rawAbsent(raw) {
		return nil
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err != nil || n < 0 {
		return nil
	}
	v := int(n)
	return &v
}

// toolsTyped says a tools list names a tool the provider defines. A list
// that can't be read as one errs the same way, toward no bound, and is read
// apart from the other facts so it costs none of them.
func toolsTyped(raw json.RawMessage) bool {
	if rawAbsent(raw) {
		return false
	}
	var tools []struct {
		Type json.RawMessage `json:"type"`
	}
	if json.Unmarshal(raw, &tools) != nil {
		return true
	}
	for _, tl := range tools {
		if toolTyped(tl.Type) {
			return true
		}
	}
	return false
}

// toolTyped says a tools entry's type names a tool the provider defines:
// anything but none or "custom". Server-side tools (web search, web fetch,
// code execution) carry one, and so do Anthropic-defined client tools
// (bash_…, text_editor_…): the bound's rule errs toward no bound (66 §1).
func toolTyped(raw json.RawMessage) bool {
	if rawAbsent(raw) {
		return false
	}
	var s string
	if json.Unmarshal(raw, &s) == nil && (s == "" || s == "custom") {
		return false
	}
	return true
}

// logTag renders the facts for the per-request stdout line. Always the same
// three keys so a grep for `1m=n` or `effort=xhigh` works across every request;
// "-" marks an absent field so columns stay aligned in the eye.
func (f reqFacts) logTag() string {
	one := "n"
	if f.OneMega {
		one = "y"
	}
	return " 1m=" + one + " effort=" + orDash(f.Effort) + " think=" + orDash(f.Think)
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// subagent says the request came from a Claude Code subagent: its billing
// header says so. Claude Code 2.1.322 writes exactly " cc_is_subagent=true;"
// into the header for any thread that is not the main session.
func (f reqFacts) subagent() bool {
	return strings.HasPrefix(f.FirstSystem, tapBillingHeader) && strings.Contains(f.FirstSystem, " cc_is_subagent=true;")
}

// reqTextBlock is a content block as far as the facts need one: its type and,
// for a text block, its text. Any other field (a tool_result's content among
// them) is skipped unread.
type reqTextBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// reqSystem reads `system`: the first block's text for recognizeHarness and
// the environment from its text blocks. A bare string is one text block.
// UnmarshalJSON never fails: a system it cannot read yields nothing.
type reqSystem struct {
	first string
	env   sessionEnv
}

func (s *reqSystem) UnmarshalJSON(b []byte) error {
	var str string
	if json.Unmarshal(b, &str) == nil {
		s.first = clipBytes(str, reqFirstSystemMax)
		s.env = envFromText(str)
		return nil
	}
	var blocks []json.RawMessage
	if json.Unmarshal(b, &blocks) != nil {
		return nil
	}
	for i, raw := range blocks {
		var blk reqTextBlock
		if json.Unmarshal(raw, &blk) != nil || blk.Type != "text" {
			continue
		}
		if i == 0 {
			s.first = clipBytes(blk.Text, reqFirstSystemMax)
		}
		if s.env.Folder == "" {
			s.env = envFromText(blk.Text)
		}
	}
	return nil
}

// reqRawMessages holds the `messages` value as a slice of the body, uncopied
// and undecoded: whether it is read at all is decided after the whole body is
// (a continue, or a system that named the environment, never reads it). The
// slice aliases the body, which is never written after it is buffered.
type reqRawMessages []byte

func (m *reqRawMessages) UnmarshalJSON(b []byte) error {
	*m = b
	return nil
}

// messagesEnv reads the environment from `messages`, for a request whose
// `system` names none. It reads text blocks only, never a tool_result's content
// and never the assistant's text, in two places:
//
//   - A system-role message, anywhere in the conversation. Claude Code 2.1.319
//     renders its attachments there (the environment beside the model and the
//     instructions) and sends the environment whole again after a compaction,
//     so it can follow assistant messages. The client writes these, never the
//     person, so one counts as `system` does (fromClient). The first whose
//     environment section names a folder wins, whole.
//   - A user message ahead of the first assistant message: the meta-message form
//     other versions use, where a section a person pasted could also sit. It is
//     taken only when no system-role message names a folder, and only ever
//     fills an unknown entry (sessionLabelsObserve).
//
// Messages that hold no working-directory line at all cost one byte search.
func messagesEnv(raw []byte) (env sessionEnv, fromClient bool) {
	if !bytes.Contains(raw, []byte(envFolderMark)) {
		return sessionEnv{}, false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if t, err := dec.Token(); err != nil || t != json.Delim('[') {
		return sessionEnv{}, false
	}
	var lead sessionEnv
	leading := true
	for dec.More() {
		var msg struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		}
		if dec.Decode(&msg) != nil {
			break
		}
		switch msg.Role {
		case "system":
			if e := contentEnv(msg.Content); e.Folder != "" {
				return e, true
			}
		case "assistant":
			leading = false
		case "user":
			if leading && lead.Folder == "" {
				lead = contentEnv(msg.Content)
			}
		}
	}
	return lead, false
}

// contentEnv is the environment a message's content names: a string, or its
// text blocks in order (never a tool_result's content). The first block whose
// environment section names a folder wins, whole.
func contentEnv(content json.RawMessage) sessionEnv {
	var str string
	if json.Unmarshal(content, &str) == nil {
		return envFromText(str)
	}
	var blocks []json.RawMessage
	if json.Unmarshal(content, &blocks) != nil {
		return sessionEnv{}
	}
	for _, raw := range blocks {
		var blk reqTextBlock
		if json.Unmarshal(raw, &blk) == nil && blk.Type == "text" {
			if e := envFromText(blk.Text); e.Folder != "" {
				return e
			}
		}
	}
	return sessionEnv{}
}

// renameOutputMark opens every output of Claude Code's /rename that names
// something (renameFromOutput), as it sits in the raw body: Claude Code's
// encoder writes it unescaped, so a body without it holds no rename.
var renameOutputMark = []byte(renameOpen + "Session ")

// messagesRename is the name the newest /rename in this turn's input gave the
// session, "" when there is none. This turn's input is what follows the last
// assistant message in `messages`: the user message Claude Code merges the
// command's output into, with the prompt that follows it, and (2.1.319 on) the
// system-role message it renders that turn's attachments into, after it. A
// continue's delta holds no more than that. A create sends the whole
// conversation, and a rename before its last assistant message is not read
// again, so after a restart a session's name is known only from its next
// rename. A body without
// the output's opening costs one byte search. One with it is walked back from
// its end (eachMessageBack): each message of this turn is read for its role
// (messageRole), and only one that holds the opening is decoded, for its text.
func messagesRename(raw []byte) string {
	if !bytes.Contains(raw, renameOutputMark) {
		return ""
	}
	name := ""
	eachMessageBack(raw, func(m []byte) bool {
		if !bytes.Contains(m, renameOutputMark) {
			return messageRole(m) != "assistant"
		}
		var msg struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		}
		if json.Unmarshal(m, &msg) != nil || msg.Role == "assistant" {
			return false
		}
		if msg.Role == "user" {
			name = contentRename(msg.Content)
		}
		return name == ""
	})
	return name
}

// messageRole is a message's role, read from its first key, where Claude Code
// writes it, without decoding the rest; decoded when it is elsewhere.
func messageRole(m []byte) string {
	if rest, ok := bytes.CutPrefix(m, []byte(`{"role":"`)); ok {
		if k := bytes.IndexByte(rest, '"'); k >= 0 && bytes.IndexByte(rest[:k], '\\') < 0 {
			return string(rest[:k])
		}
	}
	var r struct {
		Role string `json:"role"`
	}
	_ = json.Unmarshal(m, &r)
	return r.Role
}

// contentRename is the name the last /rename output among a user message's
// text blocks gives (renameFromOutput), "" when none does.
func contentRename(content json.RawMessage) string {
	var str string
	if json.Unmarshal(content, &str) == nil {
		return renameFromOutput(str)
	}
	var blocks []reqTextBlock
	if json.Unmarshal(content, &blocks) != nil {
		return ""
	}
	name := ""
	for _, blk := range blocks {
		if blk.Type != "text" {
			continue
		}
		if n := renameFromOutput(blk.Text); n != "" {
			name = n
		}
	}
	return name
}

// eachMessageBack calls fn with each element of a JSON array, from the last
// one back, as its raw bytes, until fn returns false. raw must be valid JSON
// (the request's decode has checked it), and its elements objects, as
// `messages`' are. It reads bytes, not tokens: an element costs a scan of its
// own bytes, and those before where fn stopped are never read.
func eachMessageBack(raw []byte, fn func(msg []byte) bool) {
	i := bytes.LastIndexByte(raw, ']')
	for i > 0 {
		j := i - 1
		for j >= 0 && (raw[j] == ',' || raw[j] == ' ' || raw[j] == '\n' || raw[j] == '\r' || raw[j] == '\t') {
			j--
		}
		if j < 0 || raw[j] != '}' {
			return // the array's '[': no element is left
		}
		end, depth := j+1, 0
	scan:
		for ; j >= 0; j-- {
			switch raw[j] {
			case '"':
				// Outside a string, a quote closes one: skip back to the quote
				// that opens it, the nearest one before it that is not escaped.
				k := j
				for {
					if k = bytes.LastIndexByte(raw[:k], '"'); k < 0 {
						return
					}
					if !jsonEscapedAt(raw, k) {
						break
					}
				}
				j = k
			case '}', ']':
				depth++
			case '{', '[':
				if depth--; depth == 0 {
					break scan
				}
			}
		}
		if j < 0 || !fn(raw[j:end]) {
			return
		}
		i = j
	}
}

// jsonEscapedAt says whether the quote at b[i] is escaped: an odd number of
// backslashes runs up to it.
func jsonEscapedAt(b []byte, i int) bool {
	n := 0
	for i--; i >= 0 && b[i] == '\\'; i-- {
		n++
	}
	return n%2 == 1
}

// clipBytes cuts s to at most n bytes.
func clipBytes(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
