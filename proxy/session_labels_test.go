package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

const (
	labelBilling         = "x-anthropic-billing-header: cc_version=2.1.322; cc_entrypoint=cli; cc_turn_index=1;"
	labelBillingSubagent = "x-anthropic-billing-header: cc_version=2.1.322; cc_entrypoint=cli; cc_is_subagent=true; cc_turn_index=1;"
)

// labelEnv is Claude Code's environment block for a working directory.
func labelEnv(cwd, platform string) string {
	return "# Environment\nYou have been invoked in the following environment: \n" +
		" - Primary working directory: " + cwd + "\n - Is a git repository: true\n - Platform: " + platform + "\n - Shell: zsh\n"
}

// labelBody renders a Claude Code request: the billing block first in
// `system`, then the given system texts, then the messages.
func labelBody(thread, billing string, system []string, msgs ...string) string {
	blocks := []map[string]any{{"type": "text", "text": billing}}
	for _, s := range system {
		blocks = append(blocks, map[string]any{"type": "text", "text": s})
	}
	sys, _ := json.Marshal(blocks)
	extra := ""
	if thread != "" {
		extra = `"thread":` + thread + `,`
	}
	return `{"model":"claude-fable-1","system":` + string(sys) + `,` + extra + `"stream":true,"messages":[` + strings.Join(msgs, ",") + `]}`
}

func labelsTest(t *testing.T) {
	t.Helper()
	reset := func() {
		sessionLabels.Lock()
		sessionLabels.m = map[string]*sessionLabel{}
		sessionLabels.Unlock()
	}
	reset()
	t.Cleanup(reset)
}

func labelObserve(sess, body string, now time.Time) {
	sessionLabelsObserve(sess, parseReqFacts(http.Header{}, []byte(body)), now)
}

func wantLabel(t *testing.T, sess string, now time.Time, folder, platform string) {
	t.Helper()
	got := sessionLabelsLookup(sess, now)
	if got.Folder != folder || got.Platform != platform {
		t.Fatalf("label = %+v, want folder %q platform %q", got, folder, platform)
	}
}

const labelCreate = `{"type":"create"}`

func TestSessionLabelFromSystem(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{"You are Claude Code.", labelEnv("/Users/someone/projects/home-infra", "darwin")}, tapUserMsg("hi")), now)
	wantLabel(t, "s1", now, "home-infra", "darwin")
}

func TestSessionLabelFromMetaUserMessage(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	meta := fmt.Sprintf(`{"role":"user","content":[{"type":"text","text":%q},{"type":"text","text":"hi"}]}`,
		"<system-reminder>\n"+labelEnv("/home/dev/src/proxy", "linux")+"</system-reminder>")
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{"You are Claude Code."}, meta), now)
	wantLabel(t, "s1", now, "proxy", "linux")
}

func TestSessionLabelOlderFormat(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	env := "Here is useful information about the environment you are running in:\n<env>\nWorking directory: C:\\Users\\dev\\notes\nIs directory a git repo: No\nPlatform: win32\n</env>"
	// An older client sends no thread at all; its body is the whole conversation.
	labelObserve("s1", labelBody("", labelBilling, []string{env}, tapUserMsg("hi")), now)
	wantLabel(t, "s1", now, "notes", "win32")
}

func TestSessionLabelSubagentDoesNotRename(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{labelEnv("/Users/someone/projects/app", "darwin")}, tapUserMsg("hi")), now)
	labelObserve("s1", labelBody(labelCreate, labelBillingSubagent, []string{labelEnv("/Users/someone/worktrees/app-feature", "darwin")}, tapUserMsg("task")), now)
	wantLabel(t, "s1", now, "app", "darwin")
}

func TestSessionLabelSubagentFillsUnknown(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	labelObserve("s1", labelBody(labelCreate, labelBillingSubagent, []string{labelEnv("/Users/someone/worktrees/app-feature", "darwin")}, tapUserMsg("task")), now)
	wantLabel(t, "s1", now, "app-feature", "darwin")
	// A second subagent does not rename what the first filled.
	labelObserve("s1", labelBody(labelCreate, labelBillingSubagent, []string{labelEnv("/tmp/other", "darwin")}, tapUserMsg("task")), now)
	wantLabel(t, "s1", now, "app-feature", "darwin")
	// The main thread's create wins over a subagent's.
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{labelEnv("/Users/someone/projects/app", "darwin")}, tapUserMsg("hi")), now)
	wantLabel(t, "s1", now, "app", "darwin")
}

func TestSessionLabelContinueDoesNotRename(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{labelEnv("/Users/someone/projects/app", "darwin")}, tapUserMsg("hi")), now)
	labelObserve("s1", labelBody(tapContinue("msg_1"), labelBilling, []string{labelEnv("/Users/someone/projects/elsewhere", "linux")}, tapUserMsg("next")), now)
	wantLabel(t, "s1", now, "app", "darwin")
	// And a continue never creates an entry.
	labelObserve("s2", labelBody(tapContinue("msg_2"), labelBilling, []string{labelEnv("/Users/someone/projects/elsewhere", "linux")}, tapUserMsg("next")), now)
	wantLabel(t, "s2", now, "", "")
}

func TestSessionLabelEnvironmentUpdateIgnored(t *testing.T) {
	for name, update := range map[string]string{
		// As Claude Code 2.1.322 writes it: "(was …)" only when the old
		// directory is known, so the line can lack it.
		"no was":   "# Environment update\nPrimary working directory: /Users/someone/projects/moved\n - Platform: linux",
		"with was": "# Environment update\nPrimary working directory: /Users/someone/projects/moved (was /Users/someone/projects/app)\n - Platform: linux",
		// Shaped like the create's section in every line but the heading: only
		// the exact "# Environment" heading opens one.
		"preamble": "# Environment update\n" + envPreamble + " \n - Primary working directory: /Users/someone/projects/moved\n - Platform: linux",
	} {
		t.Run(name, func(t *testing.T) {
			labelsTest(t)
			now := time.Now()
			labelObserve("s1", labelBody(labelCreate, labelBilling, []string{labelEnv("/Users/someone/projects/app", "darwin")}, tapUserMsg("hi")), now)
			meta := fmt.Sprintf(`{"role":"user","content":[{"type":"text","text":%q}]}`, update)
			labelObserve("s1", labelBody(labelCreate, labelBilling, []string{"You are Claude Code."}, meta, tapUserMsg("hi")), now)
			wantLabel(t, "s1", now, "app", "darwin")
			// On its own, in system or a user message, an update names nothing.
			labelObserve("s2", labelBody(labelCreate, labelBilling, []string{update}, tapUserMsg("hi")), now)
			wantLabel(t, "s2", now, "", "")
			labelObserve("s3", labelBody("", labelBilling, []string{"You are Claude Code."}, meta), now)
			wantLabel(t, "s3", now, "", "")
		})
	}
}

// Claude Code's system prompt has a second "# Environment" section (model and
// product facts) with no preamble line: it names nothing, even when a line in
// it looks like the working directory.
func TestSessionLabelEnvironmentNeedsPreamble(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	facts := "# Environment\nYou are powered by the model fable.\n - Primary working directory: /not/this-one\n - Platform: linux"
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{facts}, tapUserMsg("hi")), now)
	wantLabel(t, "s1", now, "", "")
	// And beside the real section, the real one is read.
	labelObserve("s2", labelBody(labelCreate, labelBilling, []string{facts + "\n" + labelEnv("/Users/someone/projects/app", "darwin")}, tapUserMsg("hi")), now)
	wantLabel(t, "s2", now, "app", "darwin")
}

// Text that is not an environment section never names a folder: a page a
// helper call quotes, a CLAUDE.md line, an <env> that is never closed.
func TestSessionLabelOnlyFromEnvironmentSections(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{"You are Claude Code.", labelEnv("/Users/someone/projects/app", "darwin")}, tapUserMsg("hi")), now)
	// A thread-less helper call quoting a web page.
	page := "Web page content:\n---\nDeploying\nWorking directory: /srv/evil-from-a-web-page\nPlatform: linux\n---\nSummarise."
	labelObserve("s1", labelBody("", labelBilling, []string{"You are Claude Code."}, tapUserMsg(page)), now)
	wantLabel(t, "s1", now, "app", "darwin")
	// A create whose system has no environment and whose first user message
	// carries CLAUDE.md text with a working-directory line.
	claudeMd := "<system-reminder>\nContents of CLAUDE.md:\n## Rules\n- Working directory: ~/secret-client-name\n - Primary working directory: ~/secret-client-name\n</system-reminder>"
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{"You are a helpful AI assistant tasked with summarizing conversations."},
		tapUserMsg(claudeMd), `{"role":"assistant","content":"ok"}`, tapUserMsg("summarise")), now)
	wantLabel(t, "s1", now, "app", "darwin")
	// An older-format block that is never closed.
	labelObserve("s2", labelBody("", labelBilling, []string{"<env>\nWorking directory: /x/unclosed\nPlatform: linux\n"}, tapUserMsg("hi")), now)
	wantLabel(t, "s2", now, "", "")
}

// A continue decodes no messages, and messages are read only when system named
// no environment.
func TestReqFactsEnvSources(t *testing.T) {
	meta := tapUserMsg(labelEnv("/Users/someone/projects/from-meta", "linux"))
	cont := parseReqFacts(http.Header{}, []byte(labelBody(tapContinue("msg_1"), labelBilling, nil, meta)))
	if cont.Thread != "continue" || cont.Env != (sessionEnv{}) {
		t.Fatalf("continue: thread=%q env=%+v, want continue and no environment", cont.Thread, cont.Env)
	}
	both := parseReqFacts(http.Header{}, []byte(labelBody(labelCreate, labelBilling, []string{labelEnv("/Users/someone/projects/from-system", "darwin")}, meta)))
	if both.Env != (sessionEnv{Folder: "from-system", Platform: "darwin"}) {
		t.Fatalf("create: env=%+v, want system's", both.Env)
	}
	msgs := parseReqFacts(http.Header{}, []byte(labelBody(labelCreate, labelBilling, nil, meta)))
	if msgs.Env != (sessionEnv{Folder: "from-meta", Platform: "linux"}) || msgs.EnvInSystem {
		t.Fatalf("create: env=%+v fromClient=%v, want the meta message's, not as the client's", msgs.Env, msgs.EnvInSystem)
	}
	sysMsg := parseReqFacts(http.Header{}, []byte(labelBody(labelCreate, labelBilling, nil, tapUserMsg("hi"), labelSystemMsg(labelEnv("/Users/someone/projects/from-system-msg", "darwin")))))
	if sysMsg.Env != (sessionEnv{Folder: "from-system-msg", Platform: "darwin"}) || !sysMsg.EnvInSystem {
		t.Fatalf("create: env=%+v fromClient=%v, want the system-role message's, as the client's", sysMsg.Env, sysMsg.EnvInSystem)
	}
}

// labelSystemMsg is a system-role message as Claude Code 2.1.319 sends its
// attachments: one text block holding several of them, then tool additions.
func labelSystemMsg(text string) string {
	return fmt.Sprintf(`{"role":"system","content":[{"type":"text","text":%q},{"type":"tool_addition","name":"Monitor"}]}`, text)
}

// Claude Code 2.1.319 renders its environment, with its other attachments,
// into a system-role message in `messages`, and after a compaction sends it
// whole again at the end, after the kept assistant turns. This is the shape of
// such a create (from a real session's, its texts replaced).
func TestSessionLabelFromSystemRoleMessage(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	attachments := "Contents of CLAUDE.md:\n## Rules\n- Working directory: ~/not-this-one\n\n" +
		labelEnv("/Users/someone/projects/home-infra", "darwin") +
		"You are powered by the model named Fable.\n"
	body := labelBody(labelCreate, labelBilling, []string{"You are Claude Code."},
		tapUserMsg("This session is being continued from a previous conversation."),
		`{"role":"system","content":"Conversation compacted."}`,
		`{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"Bash","input":{}}]}`,
		tapToolResultMsg("toolu_1", "ok"),
		labelSystemMsg(attachments))
	labelObserve("s1", body, now)
	wantLabel(t, "s1", now, "home-infra", "darwin")
}

// A system-role message is the client's own text, as `system` is: the main
// thread's create from one renames a session, a subagent's does not, and it is
// read before a section in a user message.
func TestSessionLabelSystemRoleMessageIsTheClients(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{labelEnv("/Users/someone/projects/app", "darwin")}, tapUserMsg("hi")), now)
	labelObserve("s1", labelBody(labelCreate, labelBillingSubagent, []string{"You are an agent."}, tapUserMsg("task"),
		labelSystemMsg(labelEnv("/Users/someone/worktrees/app-feature", "darwin"))), now)
	wantLabel(t, "s1", now, "app", "darwin")
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{"You are Claude Code."}, tapUserMsg("hi"),
		labelSystemMsg(labelEnv("/home/cloud/work/app-moved", "linux"))), now)
	wantLabel(t, "s1", now, "app-moved", "linux")
	pasted := tapUserMsg(labelEnv("/Users/x/pasted-name", "linux"))
	labelObserve("s2", labelBody(labelCreate, labelBilling, []string{"You are Claude Code."}, pasted,
		labelSystemMsg(labelEnv("/Users/someone/projects/app", "darwin"))), now)
	wantLabel(t, "s2", now, "app", "darwin")
}

// An "# Environment update" in a system-role message names nothing, so a `cd`
// renames no session, and a continue never reads its messages at all.
func TestSessionLabelSystemRoleUpdateAndContinue(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{"You are Claude Code."}, tapUserMsg("hi"),
		labelSystemMsg(labelEnv("/Users/someone/projects/app", "darwin"))), now)
	update := "# Environment update\n - Primary working directory: /Users/someone/projects/app/services/api (was /Users/someone/projects/app)"
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{"You are Claude Code."}, tapUserMsg("hi"), labelSystemMsg(update)), now)
	wantLabel(t, "s1", now, "app", "darwin")
	// In one create, the whole section is read and the update after it is not.
	labelObserve("s3", labelBody(labelCreate, labelBilling, []string{"You are Claude Code."}, tapUserMsg("hi"),
		labelSystemMsg(labelEnv("/Users/someone/projects/app", "darwin")), `{"role":"assistant","content":"ok"}`, labelSystemMsg(update)), now)
	wantLabel(t, "s3", now, "app", "darwin")
	labelObserve("s1", labelBody(tapContinue("msg_1"), labelBilling, nil, tapUserMsg("next"),
		labelSystemMsg(labelEnv("/Users/someone/projects/elsewhere", "linux"))), now)
	wantLabel(t, "s1", now, "app", "darwin")
	labelObserve("s2", labelBody(tapContinue("msg_2"), labelBilling, nil, labelSystemMsg(labelEnv("/Users/someone/projects/elsewhere", "linux"))), now)
	wantLabel(t, "s2", now, "", "")
}

func TestSessionLabelNewMainCreateRenames(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{labelEnv("/Users/someone/projects/app", "darwin")}, tapUserMsg("hi")), now)
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{labelEnv("/home/cloud/work/app-teleported", "linux")}, tapUserMsg("hi")), now.Add(time.Minute))
	wantLabel(t, "s1", now.Add(time.Minute), "app-teleported", "linux")
}

func TestSessionLabelToolResultIgnored(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	body := labelBody(labelCreate, labelBilling, []string{"You are Claude Code."},
		tapUserMsg("read the file"),
		tapToolResultMsg("toolu_1", labelEnv("/Users/someone/projects/from-a-file", "linux")))
	labelObserve("s1", body, now)
	wantLabel(t, "s1", now, "", "")
	// Nor one whose content is a list of text blocks.
	nested := fmt.Sprintf(`{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_2","content":[{"type":"text","text":%q}]}]}`,
		labelEnv("/Users/someone/projects/from-a-file", "linux"))
	labelObserve("s3", labelBody(labelCreate, labelBilling, []string{"You are Claude Code."}, tapUserMsg("read it"), nested), now)
	wantLabel(t, "s3", now, "", "")
	// Nor the assistant's text, nor a user message after it.
	body = labelBody(labelCreate, labelBilling, []string{"You are Claude Code."},
		tapUserMsg("hi"),
		fmt.Sprintf(`{"role":"assistant","content":[{"type":"text","text":%q}]}`, labelEnv("/x/assistant-said", "linux")),
		tapUserMsg(labelEnv("/x/user-pasted-later", "linux")))
	labelObserve("s2", body, now)
	wantLabel(t, "s2", now, "", "")
}

func TestSessionLabelSanitised(t *testing.T) {
	for _, tc := range []struct{ name, cwd, platform, folder, wantPlatform string }{
		{"format character", "/a/b/na\u200bme", "darwin", "name", "darwin"},
		{"bidi override", "/a/b/ab\u202ecd", "darwin", "abcd", "darwin"},
		{"control character", "/a/b/pro\x07j", "linux", "proj", "linux"},
		{"length", "/a/" + strings.Repeat("y", 100), "win32", strings.Repeat("y", sessionLabelFolderMax), "win32"},
		{"windows path, trailing separator", `C:\Users\dev\notes\`, "win32", "notes", "win32"},
		{"unknown platform", "/a/app", "plan9", "app", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			labelsTest(t)
			now := time.Now()
			labelObserve("s1", labelBody(labelCreate, labelBilling, []string{labelEnv(tc.cwd, tc.platform)}, tapUserMsg("hi")), now)
			wantLabel(t, "s1", now, tc.folder, tc.wantPlatform)
		})
	}
}

func TestSessionLabelPrune(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{labelEnv("/p/app", "darwin")}, tapUserMsg("hi")), now)
	// A continue keeps it alive.
	labelObserve("s1", labelBody(tapContinue("msg_1"), labelBilling, nil, tapUserMsg("next")), now.Add(5*time.Hour))
	// Idle overnight, and for days, it keeps its label.
	wantLabel(t, "s1", now.Add(5*time.Hour+6*time.Hour+time.Second), "app", "darwin")
	wantLabel(t, "s1", now.Add(5*time.Hour+sessionLabelIdle), "app", "darwin")
	// A new session's sweep keeps it too while it is inside the week.
	labelObserve("s2", labelBody(labelCreate, labelBilling, []string{labelEnv("/p/other", "linux")}, tapUserMsg("hi")), now.Add(5*time.Hour+6*24*time.Hour))
	wantLabel(t, "s1", now.Add(5*time.Hour+6*24*time.Hour), "app", "darwin")
	// Past the week it is gone.
	wantLabel(t, "s1", now.Add(5*time.Hour+sessionLabelIdle+time.Second), "", "")
	// The next new session sweeps it out.
	labelObserve("s3", labelBody(labelCreate, labelBilling, []string{labelEnv("/p/third", "linux")}, tapUserMsg("hi")), now.Add(5*time.Hour+sessionLabelIdle+time.Second))
	sessionLabels.Lock()
	_, kept := sessionLabels.m["s1"]
	_, keptS2 := sessionLabels.m["s2"]
	sessionLabels.Unlock()
	if kept {
		t.Fatal("a session idle past sessionLabelIdle survived the sweep")
	}
	if !keptS2 {
		t.Fatal("the sweep dropped a session idle less than a week")
	}
}

func TestSessionLabelCap(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	f := parseReqFacts(http.Header{}, []byte(labelBody(labelCreate, labelBilling, []string{labelEnv("/p/app", "darwin")}, tapUserMsg("hi"))))
	for i := 0; i < sessionLabelCap; i++ {
		sessionLabelsObserve(fmt.Sprintf("s%d", i), f, now.Add(time.Duration(i)*time.Millisecond))
	}
	// s0 is the least recently seen until it is touched; touch it so s1 is.
	sessionLabelsObserve("s0", reqFacts{Thread: "continue"}, now.Add(time.Hour))
	sessionLabelsObserve("new", f, now.Add(time.Hour))
	sessionLabels.Lock()
	n := len(sessionLabels.m)
	_, s0 := sessionLabels.m["s0"]
	_, s1 := sessionLabels.m["s1"]
	sessionLabels.Unlock()
	if n != sessionLabelCap || !s0 || s1 {
		t.Fatalf("entries=%d s0=%v s1=%v, want %d with the least recently seen (s1) evicted", n, s0, s1, sessionLabelCap)
	}
}

// The labels are written on the request path, so they are known while nobody
// is subscribed to the events hub, and no folder or session name reaches the
// log.
func TestSessionLabelWrittenWithTapDisarmed(t *testing.T) {
	labelsTest(t)
	env, restore := newE2E(t)
	defer restore()
	if eventsArmed() {
		t.Fatal("the events hub is armed; this test needs it cold")
	}
	logBuf := captureLogSync(t)

	body := labelBody(labelCreate, labelBilling, []string{labelEnv("/Users/someone/projects/secret-folder-name", "darwin")},
		labelRenameTurn("Session renamed to: secret-session-name", "hi"), labelAttachments())
	body = strings.Replace(body, `"claude-fable-1"`, `"claude-haiku-4-5"`, 1)
	req, _ := http.NewRequest("POST", env.proxy.URL+"/v1/messages", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+clientSecret)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-claude-code-session-id", ccTestSession)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	env.hit(t)

	wantLabel(t, ccTestSession, time.Now(), "secret-folder-name", "darwin")
	wantName(t, ccTestSession, time.Now(), "secret-session-name")
	// The access line is written after the response; wait for it, so the
	// check covers everything the request logged.
	deadline := time.Now().Add(3 * time.Second)
	for !strings.Contains(logBuf.String(), "/v1/messages") {
		if time.Now().After(deadline) {
			t.Fatalf("no access line was logged:\n%s", logBuf.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, secret := range []string{"secret-folder-name", "secret-session-name"} {
		if strings.Contains(logBuf.String(), secret) {
			t.Fatalf("%s reached the log:\n%s", secret, logBuf.String())
		}
	}
}

// The OpenAI wire speaks no Claude Code dialect: like the tap and the inject
// queue, the label store never reads it.
func TestSessionLabelSkipsOpenAIWire(t *testing.T) {
	labelsTest(t)
	h, done := newE2EClass(t)
	defer done()
	resetWireBuckets()
	h.bindTo.Store("kimi/kimi-k3")
	body, _ := json.Marshal(map[string]any{"model": "frontier", "max_tokens": 16,
		"messages": []any{map[string]any{"role": "user", "content": labelEnv("/Users/someone/projects/over-the-wire", "darwin")}}})
	resp := wirePost(t, h.e2eEnv, string(body), ccTestSession)
	wireBody(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("wire status = %d", resp.StatusCode)
	}
	h.hostedHit(t)
	wantLabel(t, ccTestSession, time.Now(), "", "")
}

// An environment found in a user message (a pasted system prompt, say) only
// fills an unknown entry: only one in `system` replaces a known folder.
func TestSessionLabelMessageEnvOnlyFillsUnknown(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{"You are Claude Code.", labelEnv("/Users/someone/src/app", "darwin")}, tapUserMsg("hi")), now)
	pasted := fmt.Sprintf(`{"role":"user","content":[{"type":"text","text":%q}]}`, "what does this mean?\n"+labelEnv("/Users/x/pasted-name", "linux"))
	labelObserve("s1", labelBody("", labelBilling, []string{"You are Claude Code."}, pasted), now)
	wantLabel(t, "s1", now, "app", "darwin")
	labelObserve("s2", labelBody("", labelBilling, []string{"You are Claude Code."}, pasted), now)
	wantLabel(t, "s2", now, "pasted-name", "linux")
}

// The environment section ends at the next "# " heading: a platform line
// after it belongs to another section.
func TestSessionLabelSectionEndsAtNextHeading(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	sys := "# Environment\nYou have been invoked in the following environment: \n - Primary working directory: /Users/someone/src/app\n# Other\n - Platform: linux\n"
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{sys}, tapUserMsg("hi")), now)
	wantLabel(t, "s1", now, "app", "")
}

// renameOut is the user text block Claude Code writes for a command's output.
func renameOut(msg string) string {
	return "<local-command-stdout>" + msg + "</local-command-stdout>"
}

// labelUserBlocks is a user message of text blocks as Claude Code's
// JSON.stringify writes it: role first, `<` and `>` unescaped.
func labelUserBlocks(texts ...string) string {
	blocks := make([]map[string]string, 0, len(texts))
	for _, s := range texts {
		blocks = append(blocks, map[string]string{"type": "text", "text": s})
	}
	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(blocks); err != nil {
		panic(err)
	}
	return `{"role":"user","content":` + strings.TrimSuffix(sb.String(), "\n") + `}`
}

// labelRenameTurn is the user message of the request after a /rename: the
// command, its output, the note Claude Code adds for a typed name, and the
// prompt that followed, merged into one.
func labelRenameTurn(output, prompt string) string {
	return labelUserBlocks(
		"<command-name>/rename</command-name>\n            <command-message>rename</command-message>\n            <command-args>x</command-args>",
		renameOut(output),
		"<system-reminder>\nThe user named this session \"x\". This may indicate the session's focus or intent.\n</system-reminder>",
		prompt)
}

// labelAttachments is the system-role message Claude Code 2.1.319 renders a
// turn's attachments into, after the turn's user message.
func labelAttachments() string {
	return labelSystemMsg("<system-reminder>\nToday's date is 2026-11-12.\n</system-reminder>")
}

func TestRenameFromOutput(t *testing.T) {
	const (
		held     = ` is held by another live session on this machine)`
		noTrans  = ". The new name may not last: the session transcript could not be updated"
		noReg    = ". Other sessions may still show the old name: the session registry could not be updated"
		debugFor = " (run with --debug for the cause)"
	)
	for _, tc := range []struct{ text, want string }{
		{renameOut("Session renamed to: scry names"), "scry names"},
		{renameOut("Session renamed to: scry names") + "\n", "scry names"},
		{renameOut(`Session renamed to: scry names 2 ("scry names"` + held), "scry names 2"},
		{renameOut("Session is named: newer one (a newer rename landed first)"), "newer one"},
		{renameOut("Session renamed to: notes" + noTrans + ". Run /rename notes to try again" + debugFor), "notes"},
		{renameOut("Session renamed to: notes" + noReg + debugFor), "notes"},
		{renameOut("Session renamed to: notes" + noTrans + noReg + ". Run /rename notes to try again" + debugFor), "notes"},
		{renameOut(`Session renamed to: notes 2 ("notes"` + held + noTrans + ". Run /rename notes to try again" + debugFor), "notes 2"},
		// Names are the owner's words, quotes and brackets included.
		{renameOut(`Session renamed to: fix "auth" (v2) {draft}`), `fix "auth" (v2) {draft}`},
		{renameOut("Session renamed to: odd" + debugFor), "odd" + debugFor},
		// Shown as a name: no format characters, and a bounded length.
		{renameOut("Session renamed to: a​b‮c"), "abc"},
		{renameOut("Session renamed to: " + strings.Repeat("n", 100)), strings.Repeat("n", sessionLabelNameMax)},
		// Outputs that name nothing.
		{renameOut("Session renamed to:   "), ""},
		{renameOut("Session is named: x"), ""},
		{renameOut("Cannot rename: This session is a teammate. Teammate names are set by the team leader."), ""},
		{renameOut("Could not generate a name: no conversation context yet. Usage: /rename <name>"), ""},
		{renameOut("That name is empty once invisible characters are removed. Usage: /rename <name>"), ""},
		{renameOut("Set model to Fable"), ""},
		// Only a whole block is the command's output.
		{"see " + renameOut("Session renamed to: quoted"), ""},
		{"<local-command-stdout>Session renamed to: unclosed", ""},
		{"Session renamed to: bare", ""},
	} {
		if got := renameFromOutput(tc.text); got != tc.want {
			t.Errorf("renameFromOutput(%q) = %q, want %q", tc.text, got, tc.want)
		}
	}
}

// The name is read from this turn's input only: the messages after the last
// assistant message, whatever system-role messages follow the user's.
func TestReqFactsRename(t *testing.T) {
	renamed := func(name string) string { return renameOut("Session renamed to: " + name) }
	assistant := `{"role":"assistant","content":[{"type":"text","text":"ok"}]}`
	for _, tc := range []struct {
		label  string
		thread string
		msgs   []string
		want   string
	}{
		{"a continue, attachments after it", tapContinue("msg_1"),
			[]string{labelRenameTurn("Session renamed to: scry names", "next"), labelAttachments()}, "scry names"},
		{"a continue, nothing after it", tapContinue("msg_1"),
			[]string{labelRenameTurn("Session renamed to: scry names", "next")}, "scry names"},
		{"a create's last turn", labelCreate,
			[]string{tapUserMsg("hi"), assistant, labelRenameTurn("Session renamed to: scry names", "next"), labelAttachments()}, "scry names"},
		{"a create's earlier turn", labelCreate,
			[]string{labelRenameTurn("Session renamed to: old name", "hi"), assistant, tapUserMsg("next"), labelAttachments()}, ""},
		{"two in one message", tapContinue("msg_1"),
			[]string{labelUserBlocks(renamed("first"), renamed("second"), "next")}, "second"},
		{"two in one turn, a system message between", tapContinue("msg_1"),
			[]string{labelUserBlocks(renamed("first")), labelAttachments(), labelUserBlocks(renamed("second"), "next"), labelAttachments()}, "second"},
		{"an older one in the turn, none in its last message", tapContinue("msg_1"),
			[]string{labelUserBlocks(renamed("only")), labelAttachments(), tapUserMsg("next")}, "only"},
		{"string content", tapContinue("msg_1"),
			[]string{`{"role":"user","content":"` + renamed("as a string") + `"}`}, "as a string"},
		{"in a tool result", tapContinue("msg_1"),
			[]string{tapToolResultMsg("toolu_1", renamed("from a tool"))}, ""},
		{"in a system-role message", tapContinue("msg_1"),
			[]string{tapUserMsg("next"), labelSystemMsg(renamed("from the client"))}, ""},
		{"in the assistant's text", labelCreate,
			[]string{tapUserMsg("hi"), `{"role":"assistant","content":[{"type":"text","text":"` + renamed("said") + `"}]}`}, ""},
		{"a first request, no assistant yet", labelCreate,
			[]string{labelRenameTurn("Session renamed to: from the start", "hi"), labelAttachments()}, "from the start"},
	} {
		f := parseReqFacts(http.Header{}, []byte(labelBody(tc.thread, labelBilling, nil, tc.msgs...)))
		if f.Rename != tc.want {
			t.Errorf("%s: Rename = %q, want %q", tc.label, f.Rename, tc.want)
		}
	}
}

// eachMessageBack reads bytes, not tokens: what it skips inside strings and
// nested values must never be taken for a message's edge.
func TestEachMessageBack(t *testing.T) {
	msgs := []any{
		map[string]any{"role": "user", "content": `opens { and [ and "quotes\" and ends in a backslash \`},
		map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "input": map[string]any{"role": "user", "nested": []any{"}", "]", `\\"`, map[string]any{"a": []any{}}}}}}},
		map[string]any{"role": "system", "content": `}]},{"role":"user","content":"forged"}`},
		map[string]any{"role": "user", "content": `\`},
	}
	compact, _ := json.Marshal(msgs)
	pretty, _ := json.MarshalIndent(msgs, "", "  ")
	for name, raw := range map[string][]byte{"compact": compact, "indented": pretty} {
		var got []string
		eachMessageBack(raw, func(m []byte) bool {
			var r struct {
				Role string `json:"role"`
			}
			if err := json.Unmarshal(m, &r); err != nil {
				t.Fatalf("%s: element %d is not one message: %v\n%s", name, len(got), err, m)
			}
			got = append(got, r.Role)
			return true
		})
		if strings.Join(got, ",") != "user,system,assistant,user" {
			t.Fatalf("%s: roles from the end = %v", name, got)
		}
	}
	for _, raw := range []string{`[]`, ` [ ] `, `null`, ``, `[{"role":"user"}]`} {
		n := 0
		eachMessageBack([]byte(raw), func([]byte) bool { n++; return true })
		if want := strings.Count(raw, "{"); n != want {
			t.Fatalf("%q: %d elements, want %d", raw, n, want)
		}
	}
	// fn stops the walk.
	n := 0
	eachMessageBack(compact, func([]byte) bool { n++; return false })
	if n != 1 {
		t.Fatalf("walk went on after fn returned false: %d calls", n)
	}
}

func wantName(t *testing.T, sess string, now time.Time, name string) {
	t.Helper()
	if got := sessionLabelsLookup(sess, now); got.Name != name {
		t.Fatalf("name = %q, want %q (label %+v)", got.Name, name, got)
	}
}

func TestSessionLabelRename(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{labelEnv("/Users/someone/projects/app", "darwin")}, tapUserMsg("hi")), now)
	wantName(t, "s1", now, "")
	labelObserve("s1", labelBody(tapContinue("msg_1"), labelBilling, nil, labelRenameTurn("Session renamed to: scry names", "next"), labelAttachments()), now)
	wantName(t, "s1", now, "scry names")
	wantLabel(t, "s1", now, "app", "darwin")
	// A turn without one keeps it, and so does a new main-thread create.
	labelObserve("s1", labelBody(tapContinue("msg_2"), labelBilling, nil, tapUserMsg("more")), now)
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{labelEnv("/Users/someone/projects/app", "darwin")}, tapUserMsg("hi")), now)
	wantName(t, "s1", now, "scry names")
	// A subagent's request never names the session.
	labelObserve("s1", labelBody(labelCreate, labelBillingSubagent, []string{"You are an agent."}, labelRenameTurn("Session renamed to: agent's", "task")), now)
	wantName(t, "s1", now, "scry names")
	// A newer rename wins.
	labelObserve("s1", labelBody(tapContinue("msg_3"), labelBilling, nil, labelRenameTurn("Session is named: newer (a newer rename landed first)", "next")), now)
	wantName(t, "s1", now, "newer")
	// Kept overnight, and gone with the rest of the label once the session
	// idles out.
	wantName(t, "s1", now.Add(6*time.Hour+time.Second), "newer")
	wantName(t, "s1", now.Add(sessionLabelIdle+time.Second), "")
}

// A rename can come before this process has seen the session's create (it
// restarted): the entry holds the name alone until a create fills its folder,
// a subagent's included, as for any entry that knows no folder.
func TestSessionLabelRenameBeforeCreate(t *testing.T) {
	labelsTest(t)
	now := time.Now()
	labelObserve("s1", labelBody(tapContinue("msg_1"), labelBilling, nil, labelRenameTurn("Session renamed to: early", "next")), now)
	wantName(t, "s1", now, "early")
	wantLabel(t, "s1", now, "", "")
	labelObserve("s1", labelBody(labelCreate, labelBillingSubagent, []string{labelEnv("/Users/someone/worktrees/app-feature", "darwin")}, tapUserMsg("task")), now)
	wantLabel(t, "s1", now, "app-feature", "darwin")
	labelObserve("s1", labelBody(labelCreate, labelBilling, []string{labelEnv("/Users/someone/projects/app", "darwin")}, tapUserMsg("hi")), now)
	wantLabel(t, "s1", now, "app", "darwin")
	wantName(t, "s1", now, "early")
}

// messageRole reads Claude Code's role-first layout without a decode, and any
// other layout by decoding it.
func TestMessageRole(t *testing.T) {
	for _, tc := range []struct{ msg, want string }{
		{`{"role":"user","content":"x"}`, "user"},
		{`{"content":"x","role":"assistant"}`, "assistant"},
		{`{"role":"user"}`, "user"},
		{`{ "role" : "system" }`, "system"},
		{`{"content":"x"}`, ""},
	} {
		if got := messageRole([]byte(tc.msg)); got != tc.want {
			t.Errorf("messageRole(%s) = %q, want %q", tc.msg, got, tc.want)
		}
	}
}
