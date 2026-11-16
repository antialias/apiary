package main

import (
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

// Session labels: the working folder and platform of each live session, and
// the name a /rename gave it, by its full x-claude-code-session-id, so the turn
// event can say what a session is (the console's "Coding harness" card names a
// session by its name, else its folder).
//
// Memory only, and never logged at any level: a folder or a session name is
// the user's, and it leaves this process only on the turn event. Written on the
// request path, not in the tap, because the tap runs only while Scry is
// subscribed and the labels must be known when it subscribes.
//
// A name is read from the output Claude Code writes for /rename, which reaches
// the request that follows the command (messagesRename); only the main thread's
// requests name a session. A name set any other way (the app's rename menu,
// `claude -n`, an automatic title) never reaches a request, so it is not known
// here, and a restart forgets every name until the session's next /rename.
//
// Only a request that carries the whole conversation writes: Claude Code's
// thread protocol sends its environment block only on a create, and a continue
// carries the billing header, one constant paragraph and the delta. The block
// is in `system`, or (Claude Code 2.1.319) in a system-role message among the
// attachments it renders into `messages`, or in a meta user message ahead of
// the first assistant message (messagesEnv). The main thread's create wins; a
// subagent's create fills an entry only when nothing is known for the session,
// so an agent working in a worktree never renames its parent. A later
// main-thread create whose environment is in text the client writes (`system`
// or a system-role message) replaces the entry (a session moved to another
// folder); one found in a user message (Claude Code's meta-message form, or a
// section a person pasted) only ever fills an unknown entry.

// sessionEnv is what a request's environment block says.
type sessionEnv struct {
	Folder   string // last part of the working directory; "" when unknown
	Platform string // darwin, linux or win32; "" when unknown or anything else
}

const (
	sessionLabelFolderMax = 64
	sessionLabelNameMax   = 64
	sessionLabelCap       = 2048
)

// sessionLabelIdle is how long an idle session keeps its label: a week, so a
// session left overnight keeps the name its /rename gave it (Claude Code sends
// a rename once, and compaction drops it from later requests).
const sessionLabelIdle = 7 * 24 * time.Hour

// The environment formats, pinned to what Claude Code writes (2.1.322's
// bundle). The current one is
//
//	["# Environment", "You have been invoked in the following environment: ",
//	 " - Primary working directory: <path>", " - Is a git repository: …",
//	 " - Platform: darwin", …].join("\n")
//
// and runs to the next line starting "# ". The system prompt has a second
// "# Environment" section (model and product facts) without the preamble line;
// it names nothing. Older clients wrote "Working directory: <path>" and
// "Platform: <os>" between a "<env>" line and a "</env>" line.
//
// "# Environment update" (["# Environment update", "Primary working directory:
// <new>" + " (was <old>)" only when the old one is known].join("\n")) is sent on
// a `cd`. Its heading is not "# Environment", so it is never read, whatever
// follows, and a `cd` never renames a session.
const (
	envHeading       = "# Environment"
	envPreamble      = "You have been invoked in the following environment:"
	envCwdLine       = " - Primary working directory: "
	envPlatformLine  = " - Platform: "
	envOldOpen       = "<env>"
	envOldClose      = "</env>"
	envOldCwdLine    = "Working directory: "
	envOldPlatLine   = "Platform: "
	envSectionPrefix = "# "
	// envFolderMark is in every folder line of both formats, and JSON never
	// escapes it, so text (or a raw body) without it names no folder.
	envFolderMark = "orking directory: "
)

// envFromText reads the folder and platform from the first environment
// section of one text block that names a folder, as a whole: zero when none
// does. Text outside such a section (a pasted page, a CLAUDE.md line) is
// never read.
func envFromText(s string) sessionEnv {
	if !strings.Contains(s, envFolderMark) {
		return sessionEnv{}
	}
	lines := strings.Split(s, "\n")
	for i := 0; i < len(lines); i++ {
		var e sessionEnv
		switch {
		case lines[i] == envHeading && i+1 < len(lines) && strings.HasPrefix(lines[i+1], envPreamble):
			for j := i + 2; j < len(lines) && !strings.HasPrefix(lines[j], envSectionPrefix); j++ {
				e.take(lines[j], envCwdLine, envPlatformLine)
			}
		case lines[i] == envOldOpen:
			closed := false
			for j := i + 1; j < len(lines); j++ {
				if lines[j] == envOldClose {
					closed = true
					break
				}
				e.take(lines[j], envOldCwdLine, envOldPlatLine)
			}
			if !closed {
				e = sessionEnv{}
			}
		}
		if e.Folder != "" {
			return e
		}
	}
	return sessionEnv{}
}

// take reads one line of an environment section: the first folder and the
// first platform it names.
func (e *sessionEnv) take(line, cwdPrefix, platformPrefix string) {
	if v, ok := strings.CutPrefix(line, cwdPrefix); ok && e.Folder == "" {
		e.Folder = folderOf(v)
	} else if v, ok := strings.CutPrefix(line, platformPrefix); ok && e.Platform == "" {
		e.Platform = platformOf(v)
	}
}

// folderOf is the last part of a working directory, POSIX or Windows, without
// control or format characters and at most sessionLabelFolderMax characters.
func folderOf(p string) string {
	p = strings.TrimRight(strings.TrimSpace(p), `/\`)
	if i := strings.LastIndexAny(p, `/\`); i >= 0 {
		p = p[i+1:]
	}
	return labelText(p, sessionLabelFolderMax)
}

// labelText is s as the console may show it: without control or format
// characters, at most max characters, trimmed. Format characters too
// (zero-width, bidi overrides): a label is shown as a name, and these only
// ever disguise one.
func labelText(s string, max int) string {
	var b strings.Builder
	n := 0
	for _, r := range s {
		if r == utf8.RuneError || unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			continue
		}
		if n == max {
			break
		}
		b.WriteRune(r)
		n++
	}
	return strings.TrimSpace(b.String())
}

// The output of Claude Code's /rename, pinned to what it writes (2.1.323's
// bundle, unchanged since 2.1.318). The command's output reaches the next
// request as one user text block,
//
//	"<local-command-stdout>" + message + "</local-command-stdout>"
//
// whose message is one of
//
//	"Session renamed to: <name>"
//	"Session renamed to: <name> (\"<asked>\" is held by another live session on this machine)"
//	"Session is named: <name> (a newer rename landed first)"
//
// followed, when Claude Code could not write the session's transcript or its
// registry, by ". " and the failures, ". Run /rename <name> to try again" when
// the transcript was one, and " (run with --debug for the cause)". The name
// stays the session's in that process either way. Every other output (a
// rename that failed, a teammate) begins otherwise and names nothing. Claude
// Code has already trimmed the name, stripped its control characters and cut it
// to 200 characters; it escapes nothing in the message.
const (
	renameOpen         = "<local-command-stdout>"
	renameClose        = "</local-command-stdout>"
	renameRenamed      = "Session renamed to: "
	renameNamed        = "Session is named: "
	renameSuperseded   = " (a newer rename landed first)"
	renameHeldOpen     = ` ("`
	renameHeldClose    = `" is held by another live session on this machine)`
	renameNoTranscript = ". The new name may not last: the session transcript could not be updated"
	renameNoRegistry   = ". Other sessions may still show the old name: the session registry could not be updated"
	renameDebug        = " (run with --debug for the cause)"
)

// renameFromOutput is the name one text block of a user message gives the
// session, "" unless the block is, as a whole, the output of a /rename that
// named it. A name that itself holds one of the message's own phrases (` ("`,
// the failure sentences) is cut where the phrase begins.
func renameFromOutput(text string) string {
	body, ok := strings.CutPrefix(strings.TrimRight(text, "\n"), renameOpen)
	if !ok {
		return ""
	}
	if body, ok = strings.CutSuffix(body, renameClose); !ok {
		return ""
	}
	if b, ok := strings.CutSuffix(body, renameDebug); ok {
		if i := strings.Index(b, renameNoTranscript); i >= 0 {
			body = b[:i]
		} else if b, ok := strings.CutSuffix(b, renameNoRegistry); ok {
			body = b
		}
	}
	var name string
	if rest, ok := strings.CutPrefix(body, renameNamed); ok {
		if name, ok = strings.CutSuffix(rest, renameSuperseded); !ok {
			return ""
		}
	} else if rest, ok := strings.CutPrefix(body, renameRenamed); ok {
		name = rest
		if strings.HasSuffix(rest, renameHeldClose) {
			if i := strings.Index(rest, renameHeldOpen); i >= 0 {
				name = rest[:i]
			}
		}
	} else {
		return ""
	}
	return labelText(name, sessionLabelNameMax)
}

func platformOf(v string) string {
	switch v = strings.TrimSpace(v); v {
	case "darwin", "linux", "win32":
		return v
	}
	return ""
}

type sessionLabel struct {
	env  sessionEnv
	name string
	seen time.Time
}

// sessionKnown is what is known of a session: the environment it works in and
// the name a /rename gave it ("" when none has since this process started).
type sessionKnown struct {
	sessionEnv
	Name string
}

var sessionLabels = struct {
	sync.Mutex
	m map[string]*sessionLabel
}{m: map[string]*sessionLabel{}}

// sessionLabelsObserve is called for every POST /v1/messages on the request
// path. It marks the session seen, takes the name a main-thread request's
// /rename output gives it, and, on a request that carries the whole
// conversation (a thread create, or a client that sends no thread), applies
// the environment it names.
func sessionLabelsObserve(sess string, f reqFacts, now time.Time) {
	if sess == "" {
		return
	}
	sessionLabels.Lock()
	defer sessionLabels.Unlock()
	cur := sessionLabels.m[sess]
	if cur != nil {
		cur.seen = now
	}
	if f.Rename != "" && !f.subagent() {
		if cur == nil {
			sessionLabelsMakeRoomLocked(now)
			cur = &sessionLabel{seen: now}
			sessionLabels.m[sess] = cur
		}
		cur.name = f.Rename
	}
	// Only a whole environment replaces another: a section that names a
	// folder, with its own platform (or none), never a partial one.
	if f.Thread == "continue" || f.Env.Folder == "" {
		return
	}
	switch {
	case cur == nil:
		sessionLabelsMakeRoomLocked(now)
		sessionLabels.m[sess] = &sessionLabel{env: f.Env, seen: now}
	case cur.env.Folder == "": // known so far only by its name
		cur.env = f.Env
	case !f.subagent() && f.EnvInSystem:
		cur.env = f.Env
	}
}

// sessionLabelsLookup is what is known of a session, zero when nothing is or
// the session has been idle past sessionLabelIdle.
func sessionLabelsLookup(sess string, now time.Time) sessionKnown {
	if sess == "" {
		return sessionKnown{}
	}
	sessionLabels.Lock()
	defer sessionLabels.Unlock()
	cur := sessionLabels.m[sess]
	if cur == nil || now.Sub(cur.seen) > sessionLabelIdle {
		return sessionKnown{}
	}
	return sessionKnown{sessionEnv: cur.env, Name: cur.name}
}

// sessionLabelsMakeRoomLocked drops sessions idle past sessionLabelIdle (a
// week) and then, at the cap, the least recently seen, so one more fits.
// It runs only when a new session is added.
func sessionLabelsMakeRoomLocked(now time.Time) {
	for k, l := range sessionLabels.m {
		if now.Sub(l.seen) > sessionLabelIdle {
			delete(sessionLabels.m, k)
		}
	}
	for len(sessionLabels.m) >= sessionLabelCap {
		oldest, at := "", time.Time{}
		for k, l := range sessionLabels.m {
			if oldest == "" || l.seen.Before(at) {
				oldest, at = k, l.seen
			}
		}
		delete(sessionLabels.m, oldest)
	}
}
