package main

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
)

// §5.5(5) — the inbound strip.
//
// Under enforce the proxy appends one trailing "[switch-proxy] …" text block to a
// class reply (§5.5(2)) so the human reading it learns it was rerouted. The client
// hands that block straight back in the next request's history. This file removes
// it again, on the way in.
//
// Two properties make the strip safe to run unconditionally:
//
//   - The gate is one bytes.Contains over the buffered body. The sentinel is ASCII
//     and no JSON encoder in use escapes those bytes — Go's SetEscapeHTML touches
//     only < > &, Python's ensure_ascii only non-ASCII, JSON.stringify only control
//     characters and quotes — so its raw bytes are present iff the sentinel is
//     present in some string of the body, whatever the client encoded with. No hit
//     means no walk, no allocation, ~50 µs/MB. That is the only unconditional
//     health work in the proxy (§0.8).
//
//   - On a hit, the walk splices bytes; it never re-marshals the body. Exactly one
//     string value (or one whole block/message object) changes and every other byte
//     is copied verbatim, so a block-faithful client's upstream body stays
//     byte-identical to the one it would have sent for the un-annotated history —
//     which is what keeps the upstream prompt-cache prefix intact.
//
// It runs in EVERY mode, off included: a note written under enforce must still be
// stripped after a rollback, or the first request after the flip carries the line
// into the upstream and into the cache prefix. That is the one deliberate deviation
// from §12 E0's "off is byte-identical".
//
// What is NOT a tail note is left alone, and that is a rule rather than an
// omission: a sentinel mid-line, mid-string, in a paragraph that merely mentions
// the convention, or anywhere in user / tool_result / system text is the history,
// and the model seeing it is correct.

// noteSentinel is ASCII on purpose — see the gate note above.
const noteSentinel = "[switch-proxy]"

// noteMaxRunes bounds the rendered line (§5.5(2)). A longer last line that happens
// to open with the sentinel is not one of ours and is not touched.
const noteMaxRunes = 160

// noteTemplateHeads is the closed set of §5.5(2) opening phrases that do not begin
// with a member id. The two that do — "<member id> failed at the door" and
// "<member id> is out of quota" — are matched lexically by noteHeadOK, because a
// member id is data. U17 asserts every rendered template opens with one of these,
// so a template added later cannot silently escape the strip.
var noteTemplateHeads = []string{
	"answered by ",
	"served by ",
	"this request probed ",
	"still on ",
}

// noteMemberHeads is the tail half of the two member-id-leading templates.
var noteMemberHeads = []string{
	"failed at the door",
	"is out of quota",
}

var noteSentinelBytes = []byte(noteSentinel)

// Strip counters, per class (§6.2(3)). They live here rather than on healthStore
// because the strip runs under SWITCH_HEALTH=off too, where there is no store.
//
// Two counters, not one, because they answer different questions. `walks` is
// bodies that passed the gate and were token-walked; E21(c) asserts it does not
// move for a body with no sentinel, which is §0.8's cost claim. `stripped` is
// notes actually removed. A walk with no strip is a client quoting the convention
// back at us — exactly what the grammar exists to leave alone — so the gap
// between them is the signal that the grammar is doing its job.
var (
	noteStripMu     sync.Mutex
	noteStripWalks  = map[string]int64{}
	noteStripsTaken = map[string]int64{}
)

// noteStripTotals snapshots both counters for the exposition and for tests.
func noteStripTotals() (walks, stripped map[string]int64) {
	noteStripMu.Lock()
	defer noteStripMu.Unlock()
	walks, stripped = make(map[string]int64, len(noteStripWalks)), make(map[string]int64, len(noteStripsTaken))
	for k, v := range noteStripWalks {
		walks[k] = v
	}
	for k, v := range noteStripsTaken {
		stripped[k] = v
	}
	return walks, stripped
}

// stripSwitchNote removes every tail note from the assistant turns of an
// Anthropic-shaped request body. It returns nil when nothing changed — the caller
// keeps its own bytes, so the no-note path allocates nothing beyond the gate.
func stripSwitchNote(class string, body []byte) []byte {
	if len(body) == 0 || !bytes.Contains(body, noteSentinelBytes) {
		return nil
	}
	noteStripMu.Lock()
	noteStripWalks[class]++
	noteStripMu.Unlock()

	edits, hits := collectNoteEdits(body)
	if len(edits) == 0 {
		return nil
	}
	out := applyNoteEdits(body, edits)
	if out == nil {
		return nil // overlapping or out-of-range edits: leave the body alone
	}
	noteStripMu.Lock()
	noteStripsTaken[class] += int64(hits)
	noteStripMu.Unlock()
	return out
}

// splitTailNote decides whether s ends in a note this proxy wrote and, if so,
// returns s without it.
//
// The grammar is the note's own and nothing looser: the LAST line of the string,
// with nothing after it; that line begins with the sentinel; the sentinel is
// followed by exactly one space; the remainder is <= 160 characters and opens with
// one of §5.5(2)'s template heads. The "\n\n" that separates the note from the
// model's last paragraph goes with it.
func splitTailNote(s string) (string, bool) {
	nl := strings.LastIndexByte(s, '\n')
	line := s[nl+1:] // nl == -1 => the whole string is the last line
	if !strings.HasPrefix(line, noteSentinel+" ") {
		return "", false
	}
	rest := line[len(noteSentinel)+1:]
	if utf8.RuneCountInString(rest) > noteMaxRunes || !noteHeadOK(rest) {
		return "", false
	}
	if nl < 0 {
		return "", true // the note was the entire value
	}
	cut := nl
	if cut > 0 && s[cut-1] == '\n' {
		cut-- // the leading blank line is part of the note
	}
	return s[:cut], true
}

// noteHeadOK reports whether rest opens with one of §5.5(2)'s template heads.
func noteHeadOK(rest string) bool {
	for _, h := range noteTemplateHeads {
		if strings.HasPrefix(rest, h) {
			return true
		}
	}
	// "<member id> failed at the door" / "<member id> is out of quota".
	sp := strings.IndexByte(rest, ' ')
	if sp <= 0 {
		return false
	}
	if !isMemberID(rest[:sp]) {
		return false
	}
	for _, h := range noteMemberHeads {
		if strings.HasPrefix(rest[sp+1:], h) {
			return true
		}
	}
	return false
}

// isMemberID matches the lexical form of classMemberPref.ID: <provider>/<model>,
// exactly one slash, both halves non-empty, no whitespace.
func isMemberID(s string) bool {
	slash := strings.IndexByte(s, '/')
	if slash <= 0 || slash >= len(s)-1 || strings.IndexByte(s[slash+1:], '/') >= 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '/', c == '-', c == '_', c == '.', c == ':':
		default:
			return false
		}
	}
	return true
}

// noteEdit is one byte range of the original body to replace. repl == nil deletes.
type noteEdit struct {
	start, end int64
	repl       []byte
}

// collectNoteEdits token-walks the body and records the splices, in the shape of
// rewriteModel (Token + InputOffset, splice bytes, never re-marshal) with the two
// additions that walk needs: a value's START offset, and the offset before an
// array element so a dropped element takes its separator with it.
//
// Any structural surprise returns no edits — the body then goes upstream unchanged,
// which is the safe direction: a note that survives is cosmetic, a corrupted body
// is not.
func collectNoteEdits(body []byte) ([]noteEdit, int) {
	dec := json.NewDecoder(bytes.NewReader(body))
	t, err := dec.Token()
	if err != nil {
		return nil, 0
	}
	if d, ok := t.(json.Delim); !ok || d != '{' {
		return nil, 0
	}
	var edits []noteEdit
	hits := 0
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return nil, 0
		}
		key, _ := kt.(string)
		if key != "messages" {
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return nil, 0
			}
			continue
		}
		at, err := dec.Token()
		if err != nil {
			return nil, 0
		}
		if d, ok := at.(json.Delim); !ok || d != '[' {
			return nil, 0
		}
		for idx := 0; dec.More(); idx++ {
			prevEnd := dec.InputOffset()
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return nil, 0
			}
			end := dec.InputOffset()
			start := skipToElement(body, prevEnd)
			if start < 0 {
				return nil, 0
			}
			me, n := messageNoteEdits(body, start, end, idx == 0)
			edits = append(edits, me...)
			hits += n
		}
		if _, err := dec.Token(); err != nil { // ']'
			return nil, 0
		}
	}
	return edits, hits
}

// messageNoteEdits handles one element of "messages". Only assistant turns are
// looked at; every other role is the user's own words or a tool's output, and the
// model seeing a sentinel there is correct.
func messageNoteEdits(body []byte, start, end int64, first bool) ([]noteEdit, int) {
	seg := body[start:end]
	var probe struct {
		Role string `json:"role"`
	}
	if err := json.Unmarshal(seg, &probe); err != nil || probe.Role != "assistant" {
		return nil, 0
	}
	dec := json.NewDecoder(bytes.NewReader(seg))
	t, err := dec.Token()
	if err != nil {
		return nil, 0
	}
	if d, ok := t.(json.Delim); !ok || d != '{' {
		return nil, 0
	}
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return nil, 0
		}
		key, _ := kt.(string)
		if key != "content" {
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return nil, 0
			}
			continue
		}
		valStart := skipPastColon(seg, dec.InputOffset())
		if valStart < 0 {
			return nil, 0
		}
		vt, err := dec.Token()
		if err != nil {
			return nil, 0
		}
		switch v := vt.(type) {
		case string:
			// String form. A note-only value would leave content:"" — the API
			// rejects it, so the message goes. Defensive: §5.5(1)'s append guard
			// means a noted reply always carries model text beside the note.
			stripped, ok := splitTailNote(v)
			if !ok {
				return nil, 0
			}
			if stripped == "" {
				return []noteEdit{dropElement(body, start, end, first)}, 1
			}
			enc := encodeJSONString(stripped)
			if enc == nil {
				return nil, 0
			}
			return []noteEdit{{start + valStart, start + dec.InputOffset(), enc}}, 1
		case json.Delim:
			if v != '[' {
				return nil, 0
			}
			return blockNoteEdits(body, seg, start, end, first, dec)
		}
		return nil, 0
	}
	return nil, 0
}

// blockNoteEdits walks the content array of an assistant turn. dec is positioned
// just past the opening '[' of that array, and seg is the message object's bytes
// starting at absolute offset base.
func blockNoteEdits(body, seg []byte, base, msgEnd int64, msgFirst bool, dec *json.Decoder) ([]noteEdit, int) {
	type hit struct {
		start, end       int64 // the block object, within seg
		valStart, valEnd int64 // its "text" value, within seg
		stripped         string
		first            bool
	}
	var hits []hit
	total := 0
	for ; dec.More(); total++ {
		prevEnd := dec.InputOffset()
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return nil, 0
		}
		end := dec.InputOffset()
		start := skipToElement(seg, prevEnd)
		if start < 0 {
			return nil, 0
		}
		valStart, valEnd, text, ok := textBlockValue(seg[start:end])
		if !ok {
			continue
		}
		stripped, isNote := splitTailNote(text)
		if !isNote {
			continue
		}
		hits = append(hits, hit{start, end, start + valStart, start + valEnd, stripped, total == 0})
	}
	if len(hits) == 0 {
		return nil, 0
	}
	// A drop that would empty the array takes the message instead: content:[] is
	// rejected upstream. Defensive for the same reason as the string form.
	drops := 0
	for _, h := range hits {
		if h.stripped == "" {
			drops++
		}
	}
	if drops == total {
		return []noteEdit{dropElement(body, base, msgEnd, msgFirst)}, len(hits)
	}
	edits := make([]noteEdit, 0, len(hits))
	for _, h := range hits {
		if h.stripped == "" {
			e := dropElement(seg, h.start, h.end, h.first)
			edits = append(edits, noteEdit{base + e.start, base + e.end, nil})
			continue
		}
		enc := encodeJSONString(h.stripped)
		if enc == nil {
			return nil, 0
		}
		edits = append(edits, noteEdit{base + h.valStart, base + h.valEnd, enc})
	}
	return edits, len(hits)
}

// textBlockValue returns the offsets of a {"type":"text"} block's text value
// within blk, and the decoded string. ok=false for any other block kind.
func textBlockValue(blk []byte) (int64, int64, string, bool) {
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(blk, &probe); err != nil || probe.Type != "text" {
		return 0, 0, "", false
	}
	dec := json.NewDecoder(bytes.NewReader(blk))
	t, err := dec.Token()
	if err != nil {
		return 0, 0, "", false
	}
	if d, ok := t.(json.Delim); !ok || d != '{' {
		return 0, 0, "", false
	}
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return 0, 0, "", false
		}
		key, _ := kt.(string)
		if key != "text" {
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return 0, 0, "", false
			}
			continue
		}
		valStart := skipPastColon(blk, dec.InputOffset())
		if valStart < 0 {
			return 0, 0, "", false
		}
		vt, err := dec.Token()
		if err != nil {
			return 0, 0, "", false
		}
		s, ok := vt.(string)
		if !ok {
			return 0, 0, "", false
		}
		return valStart, dec.InputOffset(), s, true
	}
	return 0, 0, "", false
}

// dropElement is the range that removes one array element cleanly. Normally that
// is the element plus the separator BEFORE it — which is what a trailing note
// block always has. A first element has none, so it takes the separator after it
// instead; without that the array would be left opening with a comma.
//
// The separator is found by scanning the bytes rather than by trusting the
// decoder's offset: json.Decoder.InputOffset may sit either side of the comma
// depending on how far its scanner ran, and the difference would show up as a
// stray byte in an otherwise byte-identical body.
func dropElement(b []byte, start, end int64, first bool) noteEdit {
	if !first {
		i := start
		for i > 0 && isJSONSpace(b[i-1]) {
			i--
		}
		if i > 0 && b[i-1] == ',' {
			i--
			for i > 0 && isJSONSpace(b[i-1]) {
				i--
			}
			return noteEdit{i, end, nil}
		}
		return noteEdit{start, end, nil}
	}
	j := end
	for j < int64(len(b)) && isJSONSpace(b[j]) {
		j++
	}
	if j < int64(len(b)) && b[j] == ',' {
		return noteEdit{start, j + 1, nil}
	}
	return noteEdit{start, end, nil}
}

// applyNoteEdits splices the recorded ranges. Every byte outside them is copied
// verbatim — that is the byte-identity property E21(a) asserts.
func applyNoteEdits(body []byte, edits []noteEdit) []byte {
	sort.Slice(edits, func(i, j int) bool { return edits[i].start < edits[j].start })
	out := make([]byte, 0, len(body))
	var pos int64
	for _, e := range edits {
		if e.start < pos || e.start > e.end || e.end > int64(len(body)) {
			return nil
		}
		out = append(out, body[pos:e.start]...)
		out = append(out, e.repl...)
		pos = e.end
	}
	return append(out, body[pos:]...)
}

// encodeJSONString renders one JSON string value with HTML escaping off, so a
// stripped value re-encodes the way the rest of the body is written.
func encodeJSONString(s string) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		return nil
	}
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// skipToElement walks from just past the previous token to the first byte of the
// next array element, stepping over whitespace and the separator.
func skipToElement(b []byte, from int64) int64 {
	i := from
	for i < int64(len(b)) && isJSONSpace(b[i]) {
		i++
	}
	if i < int64(len(b)) && b[i] == ',' {
		i++
		for i < int64(len(b)) && isJSONSpace(b[i]) {
			i++
		}
	}
	if i >= int64(len(b)) {
		return -1
	}
	return i
}

// skipPastColon walks from just past a key's closing quote to the first byte of
// its value. rewriteModel never needs this — it reconstructs the ':' — but a
// value REPLACEMENT has to know where the value actually starts.
func skipPastColon(b []byte, from int64) int64 {
	i := from
	for i < int64(len(b)) && isJSONSpace(b[i]) {
		i++
	}
	if i >= int64(len(b)) || b[i] != ':' {
		return -1
	}
	i++
	for i < int64(len(b)) && isJSONSpace(b[i]) {
		i++
	}
	if i >= int64(len(b)) {
		return -1
	}
	return i
}

func isJSONSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}
