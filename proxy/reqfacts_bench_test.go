package main

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// The hot path's worst case for the facts decode: a thread continue whose delta
// is one 2 MiB tool_result. A continue must decode no messages.
func BenchmarkParseReqFactsContinueBigToolResult(b *testing.B) {
	big := strings.Repeat("0123456789abcdef", 2<<20/16)
	msg := fmt.Sprintf(`{"role":"user","content":[{"type":"tool_result","tool_use_id":"t","content":%q}]}`, big)
	body := []byte(`{"model":"claude-fable-1","system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.322; cc_entrypoint=cli; cc_turn_index=1;"},{"type":"text","text":"You are Claude Code."}],` +
		`"thread":{"type":"continue","previous_message_id":"msg_1"},"stream":true,"messages":[` + msg + `]}`)
	h := http.Header{}
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = parseReqFacts(h, body)
	}
}

// benchRenameTurn is the user message after a /rename, as Claude Code writes
// it: the command, its output, the note for a typed name and the prompt.
const benchRenameTurn = `{"role":"user","content":[{"type":"text","text":"<command-name>/rename</command-name>\n            <command-message>rename</command-message>\n            <command-args>scry names</command-args>"},` +
	`{"type":"text","text":"<local-command-stdout>Session renamed to: scry names</local-command-stdout>"},` +
	`{"type":"text","text":"<system-reminder>\nThe user named this session \"scry names\". This may indicate the session's focus or intent.\n</system-reminder>"},` +
	`{"type":"text","text":"next"}]}`

// benchAttachments is a turn's system-role message of attachments, about 20 KiB.
func benchAttachments() string {
	return fmt.Sprintf(`{"role":"system","content":[{"type":"text","text":%q}]}`, "<system-reminder>\n"+strings.Repeat("Contents of CLAUDE.md: a line of rules. ", 512)+"</system-reminder>")
}

// A create of a session renamed long ago: about 1 MiB of history whose first
// turn holds the old rename, so the body holds the output's opening and the
// walk reads this turn's messages back to the last assistant one.
func BenchmarkParseReqFactsCreateRenamedHistory(b *testing.B) {
	msgs := []string{benchRenameTurn}
	text := strings.Repeat("words of a reply ", 150)
	for i := 0; i < 200; i++ {
		msgs = append(msgs,
			fmt.Sprintf(`{"role":"assistant","content":[{"type":"text","text":%q}]}`, text),
			fmt.Sprintf(`{"role":"user","content":[{"type":"text","text":%q}]}`, text))
	}
	msgs = append(msgs, benchAttachments())
	body := []byte(`{"model":"claude-fable-1","system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.322; cc_entrypoint=cli; cc_turn_index=1;"},{"type":"text","text":"You are Claude Code."}],` +
		`"thread":{"type":"create"},"stream":true,"messages":[` + strings.Join(msgs, ",") + `]}`)
	h := http.Header{}
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = parseReqFacts(h, body)
	}
}

// The request right after a /rename: a continue whose delta is the rename's
// turn and its attachments, the one request whose name is decoded.
func BenchmarkParseReqFactsContinueRename(b *testing.B) {
	body := []byte(`{"model":"claude-fable-1","system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.322; cc_entrypoint=cli; cc_turn_index=1;"},{"type":"text","text":"You are Claude Code."}],` +
		`"thread":{"type":"continue","previous_message_id":"msg_1"},"stream":true,"messages":[` + benchRenameTurn + `,` + benchAttachments() + `]}`)
	h := http.Header{}
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = parseReqFacts(h, body)
	}
}
