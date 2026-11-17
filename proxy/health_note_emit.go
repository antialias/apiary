package main

// #124 / §5.5: a note describes the route, never the model's words or an
// upstream error string. Freeze the evidence at the head; the body writer only
// decides whether a completed, readable reply is eligible to carry it.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"
)

var healthNotesEnabled = true // startup-only; SWITCH_HEALTH_NOTES=off disables emission, never stripping

type noteWalkCause struct {
	member        string
	status        int // 0 = transport failure; never render the upstream error text
	reset         time.Time
	shapeRejected bool
}

func captureNoteWalk(member string, resp *http.Response) *noteWalkCause {
	c := &noteWalkCause{member: member}
	if resp != nil {
		c.status = resp.StatusCode
		// This is an upstream reset instant, unlike our 300s local cooldown.
		if epoch, err := strconv.ParseInt(resp.Header.Get("Anthropic-Ratelimit-Unified-Reset"), 10, 64); err == nil && epoch > 0 {
			c.reset = time.Unix(epoch, 0)
		}
	}
	return c
}

type noteKeyState struct {
	state           string
	age             time.Duration
	failed, total   int
	episodeStart    time.Time
	probesRemaining int // after this request succeeds
	dwell           time.Duration
}

type noteDecision struct {
	class, bound, served, hop, reason, walked string
	pinned, probe                             bool
	boundState, servedState                   noteKeyState
	quota, failure                            *noteWalkCause
}

type responseNote struct {
	class, kind, text string
	noted             atomic.Bool
}

var noteEmitCounters = struct {
	sync.Mutex
	totals map[[2]string]int64
}{totals: map[[2]string]int64{}}

func (n *responseNote) emitted() {
	if n != nil && n.noted.CompareAndSwap(false, true) {
		noteEmitCounters.Lock()
		noteEmitCounters.totals[[2]string{n.class, n.kind}]++
		noteEmitCounters.Unlock()
	}
}

func noteEmissionTotals() map[[2]string]int64 {
	noteEmitCounters.Lock()
	defer noteEmitCounters.Unlock()
	out := make(map[[2]string]int64, len(noteEmitCounters.totals))
	for k, v := range noteEmitCounters.totals {
		out[k] = v
	}
	return out
}

func prepareResponseNote(cc *classRoute) *responseNote {
	if cc == nil || !healthNotesEnabled || !healthEnforcedClass(cc.class) || cc.healthExhaust {
		return nil
	}
	served := cc.served.snapshot().member
	if served == "" {
		return nil
	}
	// No ranking, health snapshot allocation, or body reads on a healthy bound hop.
	switch cc.hopKind {
	case "detour", "ranked", "forced", "probe":
	default:
		if (cc.walked == "" || cc.walked == "retry") && cc.routeReason != "health:ramp" { // a same-member resend is no walk to note
			s := cc.tenant.health.stateOf(served)
			if s == "" || s == hsHealthy || s == hsConvalescent {
				return nil
			}
		}
	}
	bound := cc.boundMember
	if bound == "" {
		bound = cc.member.ID
	}
	d := noteDecision{class: cc.class, bound: bound, served: served, hop: cc.hopKind,
		reason: cc.routeReason, walked: cc.walked, pinned: cc.pinned,
		probe: cc.leaseFor(served) != nil, quota: cc.quotaNote, failure: cc.healthNote}
	health := cc.tenant.health
	health.mu.Lock()
	now := nowFn()
	d.boundState = snapshotNoteKey(health, bound, cc.class, cc.session, now)
	d.servedState = snapshotNoteKey(health, served, cc.class, cc.session, now)
	health.mu.Unlock()
	kind, line := renderResponseNote(d)
	return &responseNote{class: cc.class, kind: kind, text: "\n\n" + noteSentinel + " " + line}
}

// Caller holds h.mu. No references to the mutable store escape.
func snapshotNoteKey(h *healthStore, id, class, session string, now time.Time) noteKeyState {
	k := noteKeyState{state: hsHealthy}
	if st := h.keys[id]; st != nil {
		k.state, k.age = st.state, max(time.Duration(0), now.Sub(st.since))
		for _, s := range st.ring {
			k.total++
			if !s.ok {
				k.failed++
			}
		}
		k.episodeStart = epOpenedAtOf(st.ep)
		k.probesRemaining = max(0, h.cfg.probeOKN-st.probeOK-1)
		if ce := st.perClass[class]; ce != nil {
			if sr := ce.sessions[session]; sr != nil {
				k.dwell = max(time.Duration(0), sr.dwellUntil.Sub(now))
			}
		}
	}
	return k
}

func noteDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%d sec", max(0, int(d/time.Second)))
	}
	return fmt.Sprintf("%d min", int(d/time.Minute))
}

func (k noteKeyState) phrase(counts bool) string {
	switch k.state {
	case hsOpen:
		s := "degraded (open " + noteDuration(k.age)
		if counts && k.total > 0 {
			s += fmt.Sprintf(", %d/%d failed", k.failed, k.total)
		}
		return s + ")"
	case hsHalfOpen:
		return "recovering (a probe is in flight)"
	default:
		return "available again"
	}
}

func noteLabel(s string) string {
	// Labels are configured member/class identifiers, never account identities.
	// Keep malformed configuration from injecting lines or terminal controls.
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || r == '\u2028' || r == '\u2029' {
			return '?'
		}
		return r
	}, s)
}

func (c *noteWalkCause) quotaPhrase() string {
	if c == nil {
		return "quota unavailable"
	}
	if c.shapeRejected {
		return noteLabel(c.member) + " rejected this request (429)"
	}
	s := noteLabel(c.member) + " is out of quota"
	if !c.reset.IsZero() {
		s += " until " + c.reset.Local().Format("15:04")
	}
	return s
}

func (c *noteWalkCause) healthPhrase(same bool) string {
	if c == nil {
		return "an upstream failed at the door"
	}
	s := noteLabel(c.member) + " failed at the door"
	if same {
		s += " on one account"
	}
	if c.status > 0 {
		s += fmt.Sprintf(" (%d)", c.status)
	} else {
		s += " (connection failed)"
	}
	return s
}

func renderResponseNote(d noteDecision) (kind, line string) {
	b, s, c := noteLabel(d.bound), noteLabel(d.served), noteLabel(d.class)
	switch {
	case d.walked == "health" || d.walked == "both":
		kind = "walk_health"
		same := d.failure != nil && d.failure.member == d.served
		line = d.failure.healthPhrase(same) + "; answered by " + s
		if same {
			line = d.failure.healthPhrase(true) + "; answered by it on another"
		}
		if d.walked == "both" && d.quota != nil {
			line = d.quota.quotaPhrase() + "; " + line
		}
	case d.walked == "quota":
		kind, line = "walk_quota", d.quota.quotaPhrase()+"; answered by "+s
		if d.quota != nil && d.quota.member == d.served && !d.quota.shapeRejected {
			line = "answered by " + s + " on another account — one account is out of quota"
		}
	case d.probe:
		kind, line = "probe", "this request probed "+s
		if !d.servedState.episodeStart.IsZero() {
			line += ", degraded since " + d.servedState.episodeStart.Local().Format("15:04")
		}
		line += " — it answered; "
		switch n := d.servedState.probesRemaining; n {
		case 0:
			line += "the recovery ramp can begin"
		case 1:
			line += "one more good probe and it is back"
		default:
			line += fmt.Sprintf("%d more good probes and it is back", n)
		}
	case d.reason == "health:ramp":
		kind, line = "ramp", "still on "+s+" while "+b+" recovers — this session moves back in ~"+noteDuration(d.boundState.dwell)
	case d.pinned:
		kind, line = "pinned_ride", "served by "+s+" although it is "+d.servedState.phrase(false)+" — it is pinned for class "+c+"; unpin: GET :9115/class-pin?class="+c+"&member=auto"
	case d.hop == "forced":
		kind = "forced"
		if d.served == d.bound {
			line = "served by " + s + " although it is " + d.servedState.phrase(false) + " — it is the only member of class " + c
		} else {
			line = "answered by " + s + " — it is the only other member of class " + c + "; " + b + " is " + d.boundState.phrase(true)
		}
	case d.hop == "bound" || d.hop == "pinned":
		kind, line = "bound_ride", "served by "+s+" although it is "+d.servedState.phrase(false)+" — the brain just re-bound class "+c+" to it"
	default:
		kind = "detour"
		if d.hop == "ranked" {
			kind = "ranked"
		}
		line = "answered by " + s + " — " + b + " is " + d.boundState.phrase(true) + "; traffic returns on its own once it passes a probe"
		if d.servedState.state == hsOpen || d.servedState.state == hsHalfOpen {
			state := "degraded"
			if d.servedState.state == hsHalfOpen {
				state = d.servedState.phrase(false)
			}
			line = "answered by " + s + ", which is also " + state + " — it was the best available; " + b + " is " + d.boundState.phrase(false)
		} else if d.reason == "health:fallback" || d.boundState.state == hsHealthy || d.boundState.state == hsConvalescent {
			line = "answered by " + s + " — " + b + " was unavailable to this request"
		}
	}
	// Real Anthropic identifiers are lane:<name>, not provider/model. Keep
	// the strip's deployed grammar closed: these variants use its generic head.
	if !noteHeadOK(line) {
		line = "answered by " + s + " — " + line
	}
	if utf8.RuneCountInString(line) > noteMaxRunes {
		// Compact templates preserve the cause and real IDs for ordinary labels.
		// Pathological long configuration labels are abbreviated only here.
		short := func(v string) string {
			r := []rune(v)
			if len(r) > 36 {
				return string(r[:35]) + "…"
			}
			return v
		}
		s, b = short(s), short(b)
		switch kind {
		case "walk_health":
			why := d.failure.healthPhrase(false)
			if d.failure != nil {
				why = strings.Replace(why, noteLabel(d.failure.member), short(noteLabel(d.failure.member)), 1)
			}
			line = "answered by " + s + " — " + why
			if d.walked == "both" && d.quota != nil {
				cause := " quota refusal"
				if d.quota.shapeRejected {
					cause = " request rejected"
				}
				line += "; " + short(noteLabel(d.quota.member)) + cause
			}
		case "walk_quota":
			why := d.quota.quotaPhrase()
			if d.quota != nil {
				why = strings.Replace(why, noteLabel(d.quota.member), short(noteLabel(d.quota.member)), 1)
			}
			line = "answered by " + s + " — " + why
		case "probe":
			line = "this request probed " + s + " — it answered; recovery follows successful probes"
		case "ramp":
			line = "still on " + s + " while " + b + " recovers; return in ~" + noteDuration(d.boundState.dwell)
		case "pinned_ride":
			line = "served by " + s + " — " + d.servedState.phrase(false) + "; pinned for class " + short(c)
		case "bound_ride":
			line = "served by " + s + " — " + d.servedState.phrase(false) + "; the brain just re-bound this class"
		case "forced":
			line = "served by " + s + " — the only admissible member; " + b + " is " + d.boundState.phrase(false)
		default:
			line = "answered by " + s + " — " + b + " is " + d.boundState.phrase(false)
		}
		if r := []rune(line); len(r) > noteMaxRunes {
			line = string(r[:noteMaxRunes-1]) + "…"
		}
	}
	return kind, line
}

const noteJSONLimit int64 = 32 << 20

func appendResponseNote(resp *http.Response, n *responseNote) {
	if n == nil || resp == nil || resp.StatusCode != http.StatusOK || resp.Body == nil || n.noted.Load() {
		return
	}
	// Unexpected encodings must pass through unchanged.
	if enc := resp.Header.Get("Content-Encoding"); enc != "" && enc != "identity" {
		return
	}
	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	if strings.HasPrefix(ct, "text/event-stream") {
		resp.Body = newNoteSSE(resp.Body, n)
		resp.Header.Del("Content-Length")
		resp.ContentLength = -1
		return
	}
	if !strings.HasPrefix(ct, "application/json") {
		return
	}
	appendJSONNote(resp, n, noteJSONLimit)
}

func appendJSONNote(resp *http.Response, n *responseNote, limit int64) {
	orig := resp.Body
	raw, err := io.ReadAll(io.LimitReader(orig, limit+1))
	if int64(len(raw)) > limit {
		resp.Body = struct {
			io.Reader
			io.Closer
		}{io.MultiReader(bytes.NewReader(raw), orig), orig}
		return
	}
	// Preserve both the original error and headers on a partial read. Turning
	// a broken reply into a clean EOF would incorrectly report completion.
	resp.Body = &noteReplayBody{Reader: bytes.NewReader(raw), Closer: orig, err: err}
	if err != nil {
		return
	}
	if out := insertJSONNote(raw, n.text); out != nil {
		resp.Body = &noteReplayBody{Reader: bytes.NewReader(out), Closer: orig}
		resp.ContentLength = int64(len(out))
		resp.Header.Set("Content-Length", strconv.Itoa(len(out)))
		n.emitted()
	}
}

type noteReplayBody struct {
	*bytes.Reader
	io.Closer
	err error
}

func (r *noteReplayBody) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF && r.err != nil {
		err = r.err
	}
	return n, err
}

// Token offsets permit a single insertion: untouched content blocks, usage
// numbers (including >2^53), unknown fields and formatting remain byte-exact.
func insertJSONNote(raw []byte, text string) []byte {
	d := json.NewDecoder(bytes.NewReader(raw))
	tok, err := d.Token()
	if err != nil || tok != json.Delim('{') {
		return nil
	}
	seen := map[string]bool{}
	stop, readable, contentEnd := "", false, int64(-1)
	for d.More() {
		tok, err = d.Token()
		key, ok := tok.(string)
		if err != nil || !ok || seen[key] {
			return nil
		}
		seen[key] = true
		switch key {
		case "stop_reason":
			if d.Decode(&stop) != nil {
				return nil
			}
		case "content":
			if tok, err = d.Token(); err != nil || tok != json.Delim('[') {
				return nil
			}
			for d.More() {
				var block struct{ Type, Text string }
				if d.Decode(&block) != nil {
					return nil
				}
				readable = readable || block.Type == "text" && block.Text != ""
			}
			if tok, err = d.Token(); err != nil || tok != json.Delim(']') {
				return nil
			}
			contentEnd = d.InputOffset() - 1
		default:
			var skip json.RawMessage
			if d.Decode(&skip) != nil {
				return nil
			}
		}
	}
	if tok, err = d.Token(); err != nil || tok != json.Delim('}') {
		return nil
	}
	if _, err = d.Token(); err != io.EOF || !readable || contentEnd < 0 || stop == "" || stop == "tool_use" {
		return nil
	}
	block, _ := json.Marshal(struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}{"text", text})
	return applyNoteEdits(raw, []noteEdit{{start: contentEnd, end: contentEnd, repl: append([]byte{','}, block...)}})
}
