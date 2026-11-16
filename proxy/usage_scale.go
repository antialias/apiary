package main

// ---- anthropic_token_ratio numerator scaling (#44 Phase 4) ----
//
// A pass-through provider (kimi) speaks the Anthropic wire shape but counts
// with its own tokenizer — C1 measured ~0.61 tokens per Anthropic token for
// identical bodies. The client can only express its window as 200k/1M from the
// model NAME, so un-scaled usage makes a session look 39% smaller than it is
// and compaction fires long after the window is really gone (the
// frontier incident). These rewriters divide the INPUT-side usage numbers by
// the member's declared ratio; output_tokens is transcript-visible and stays
// in upstream units (PLAN §1 — the transcript re-enters the next turn's input,
// where the divided input count covers it).
//
// Constraints (PLAN §7/§212, pre-committed when C1 read r_kimi < 0.97):
//   - per-RESPONSE basis, decided before the first byte — never a mixed response
//   - the SSE reader is line-based and UNBOUNDED (a single input_json_delta
//     frame can exceed any fixed scanner buffer), holding at most one line
//   - fail open everywhere: on any parse or size surprise the original bytes
//     pass verbatim (unscaled = over-counted = early compaction, the safe side)
//   - /v1/messages and /v1/messages/count_tokens only, 200s only

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
)

// scaleTokensByRatio converts an upstream token count to Anthropic units:
// round(n / r), per PLAN §1. r <= 0 or 1 means no declared scaling.
func scaleTokensByRatio(n int, r float64) int {
	if n <= 0 || r <= 0 || r == 1 {
		return n
	}
	return int(math.Round(float64(n) / r))
}

// usageInputKeys are the fields Claude Code sums into its compaction numerator
// (input + cache_read + cache_creation) — the input side of a usage block.
var usageInputKeys = [...]string{"input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"}

// scaleUsageMap divides the input-side counts of one usage object in place.
// Returns false when u carries none of them (nothing to do).
func scaleUsageMap(u map[string]any, r float64) bool {
	touched := false
	for _, k := range usageInputKeys {
		if f, ok := u[k].(float64); ok {
			u[k] = float64(scaleTokensByRatio(int(f), r))
			touched = true
		}
	}
	return touched
}

// scaleProviderUsage rewrites a pass-through provider response's usage into
// Anthropic units. Dispatch is by outbound path suffix (the hop's URL embeds
// the provider's base path — kimi serves under /coding) and Content-Type.
// Anything unexpected leaves the response untouched — and resolves mark (#76)
// so the pass-through is a recorded fail-open, not a silent one. A non-200
// leaves the mark pending: an error envelope carries no usage, so there is
// no scaling claim to make either way.
func scaleProviderUsage(resp *http.Response, r float64, mark *scaleMark) {
	if resp.StatusCode != http.StatusOK || resp.Request == nil {
		return
	}
	if ce := resp.Header.Get("Content-Encoding"); ce != "" {
		// The outbound hop had Accept-Encoding stripped, so a still-encoded body
		// means the upstream forced it. Don't guess — pass through unscaled.
		log.Printf("usage-scale: %s response Content-Encoding=%q — passing through unscaled", resp.Request.URL.Host, ce)
		mark.failOpen("content_encoding")
		return
	}
	path := resp.Request.URL.Path
	ct := resp.Header.Get("Content-Type")
	switch {
	case strings.HasSuffix(path, "/count_tokens"):
		scaleJSONBody(resp, mark, "count_tokens", 1<<20, func(m map[string]any) bool {
			// count_tokens answers {"input_tokens": N} at the top level.
			mark.noteRaw(m)
			return scaleUsageMap(m, r)
		})
	case strings.HasSuffix(path, "/messages"):
		if strings.HasPrefix(ct, "text/event-stream") {
			resp.Body = newSSEUsageScaler(resp.Body, r, mark)
			resp.ContentLength = -1
			resp.Header.Del("Content-Length")
			return
		}
		if strings.HasPrefix(ct, "application/json") {
			scaleJSONBody(resp, mark, "json", 32<<20, func(m map[string]any) bool {
				u, ok := m["usage"].(map[string]any)
				if !ok {
					return false
				}
				mark.noteRaw(u)
				return scaleUsageMap(u, r)
			})
		}
	}
}

// scaleJSONBody buffers a (bounded) JSON body, applies rewrite, and swaps the
// body when it actually changed something. Any surprise — over limit,
// unparseable, rewrite found nothing — restores the original bytes and
// resolves mark with the matching fail-open reason (#76).
func scaleJSONBody(resp *http.Response, mark *scaleMark, mode string, limit int64, rewrite func(map[string]any) bool) {
	orig := resp.Body
	raw, err := io.ReadAll(io.LimitReader(orig, limit+1))
	if int64(len(raw)) > limit {
		// Over the cap: splice what was read back in FRONT of the unread
		// remainder, leaving length headers untouched — the client streams the
		// original bytes. (The old restore() here truncated at the cap.)
		resp.Body = struct {
			io.Reader
			io.Closer
		}{io.MultiReader(bytes.NewReader(raw), orig), orig}
		mark.failOpen("oversize")
		return
	}
	orig.Close()
	restore := func() {
		resp.Body = io.NopCloser(bytes.NewReader(raw))
		resp.ContentLength = int64(len(raw))
		resp.Header.Set("Content-Length", strconv.Itoa(len(raw)))
	}
	if err != nil {
		restore()
		mark.failOpen("stream_truncated")
		return
	}
	var m map[string]any
	if json.Unmarshal(raw, &m) != nil {
		restore()
		mark.failOpen("unparseable")
		return
	}
	if !rewrite(m) {
		restore()
		mark.failOpen("no_usage")
		return
	}
	b, err := json.Marshal(m)
	if err != nil {
		restore()
		mark.failOpen("unparseable")
		return
	}
	resp.Body = io.NopCloser(bytes.NewReader(b))
	resp.ContentLength = int64(len(b))
	resp.Header.Set("Content-Length", strconv.Itoa(len(b)))
	mark.resolveOK(mode)
}

// sseUsageScaler streams an Anthropic-shaped SSE body, dividing the input-side
// usage numbers on message_start / message_delta frames. Every other line —
// event: lines, deltas, pings, anything that fails to parse — passes verbatim.
// It holds at most the one line it is assembling (ReadBytes grows without a
// fixed cap, so a multi-MiB input_json_delta frame cannot break framing).
type sseUsageScaler struct {
	src   io.ReadCloser
	br    *bufio.Reader
	ratio float64
	mark  *scaleMark // #76: resolved ok on the first scaled frame, fail-open otherwise
	out   bytes.Buffer
	err   error
}

func newSSEUsageScaler(src io.ReadCloser, r float64, mark *scaleMark) *sseUsageScaler {
	return &sseUsageScaler{src: src, br: bufio.NewReaderSize(src, 64<<10), ratio: r, mark: mark}
}

func (s *sseUsageScaler) Read(p []byte) (int, error) {
	for s.out.Len() == 0 && s.err == nil {
		line, err := s.br.ReadBytes('\n')
		if len(line) > 0 {
			s.out.Write(s.rewriteLine(line))
		}
		s.err = err
	}
	if s.out.Len() > 0 {
		n, _ := s.out.Read(p)
		return n, nil
	}
	// Stream over with the mark still pending: no input-side usage was ever
	// converted. A clean EOF means the stream simply never carried one; anything
	// else died mid-flight. Resolutions latch, so the EOF-drain repeats no-op.
	if s.err == io.EOF {
		s.mark.failOpen("no_usage")
	} else {
		s.mark.failOpen("stream_truncated")
	}
	return 0, s.err
}

func (s *sseUsageScaler) Close() error {
	// The client walked away (or the copy loop errored) before any usage frame:
	// pending resolves truncated; an already-resolved mark is untouched.
	s.mark.failOpen("stream_truncated")
	return s.src.Close()
}

// rewriteLine scales one SSE line's usage numbers, or returns it verbatim.
// The cheap gates (data: prefix, a "usage" substring) keep the JSON round-trip
// off the hot text-delta path.
func (s *sseUsageScaler) rewriteLine(line []byte) []byte {
	rest, ok := bytes.CutPrefix(line, []byte("data:"))
	if !ok || !bytes.Contains(rest, []byte(`"usage"`)) {
		return line
	}
	// Preserve the exact framing: leading space after the colon, trailing \r\n or \n.
	payload, ending := rest, ""
	if bytes.HasSuffix(payload, []byte("\r\n")) {
		payload, ending = payload[:len(payload)-2], "\r\n"
	} else if bytes.HasSuffix(payload, []byte("\n")) {
		payload, ending = payload[:len(payload)-1], "\n"
	}
	sp := len(payload) > 0 && payload[0] == ' '
	if sp {
		payload = payload[1:]
	}
	var m map[string]any
	if json.Unmarshal(payload, &m) != nil {
		// A usage-gated line that isn't JSON is a corrupt stream, not a benign
		// frame type — the verbatim pass-through is a fail-open, so say so.
		s.mark.failOpen("unparseable")
		return line
	}
	var u map[string]any
	switch m["type"] {
	case "message_start":
		msg, _ := m["message"].(map[string]any)
		u, _ = msg["usage"].(map[string]any)
	case "message_delta":
		u, _ = m["usage"].(map[string]any)
	}
	if u == nil {
		return line // not a usage frame after all (a delta whose text says "usage") — no claim
	}
	s.mark.noteRaw(u)
	if !scaleUsageMap(u, s.ratio) {
		return line
	}
	b, err := json.Marshal(m)
	if err != nil {
		s.mark.failOpen("unparseable")
		return line
	}
	s.mark.resolveOK("sse")
	var out bytes.Buffer
	out.Grow(len(b) + 8)
	out.WriteString("data:")
	if sp {
		out.WriteByte(' ')
	}
	out.Write(b)
	out.WriteString(ending)
	return out.Bytes()
}
