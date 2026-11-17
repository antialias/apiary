package main

// The brain's decisions reach the proxy as events (quota-model 64 §3): one
// long-lived GET <brain>/events, whose `preferred` events each carry the whole
// /preferred document. Each document goes through applyPreferred, the decode
// the pull used, so the contract with the brain is unchanged.
//
// Liveness. The brain is reachable while the stream is open and something
// (an event or a `: keepalive` comment, which the brain sends after 15 s of
// nothing) arrived in the last brainSilence. When brainSilence passes with
// nothing, the proxy closes the stream. A stream that drops, closes or falls
// silent makes the router view stale (brainFail): routes serve STALE, the
// router-wide clause goes silent, /_switch/status says brain_reachable false
// and session facts say the proxy cannot reach the brain. There is no
// fallback to polling /preferred.
//
// Reconnect at once after a drop of a connection that held: one that delivered
// a `preferred` event and stayed open for at least brainSilence. Every other
// end (a refused or failed connect, or a stream that closed sooner) backs off
// brainBackoffMin, doubling, up to brainBackoffMax. A brain that sends one
// document and closes is therefore redialled at most once per brainBackoffMax,
// not in a loop; a brain restart after a long healthy stream is redialled at
// once.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// Variables, not constants, so tests can shrink them; production never sets them.
var (
	brainSilence    = 30 * time.Second // two of the brain's keepalive periods
	brainBackoffMin = 1 * time.Second
	brainBackoffMax = 30 * time.Second
)

// brainStreamClient has no overall timeout: the stream lives for as long as the
// brain does. The silence watchdog bounds the connect and every read instead.
var brainStreamClient = &http.Client{}

// errBrainSilent is the watchdog's reason for closing the stream.
var errBrainSilent = errors.New("nothing from the brain's /events")

// nextBackoff is the wait before the next connect: none after a connection that
// held (heldConnection), else brainBackoffMin, doubling, capped at brainBackoffMax.
func nextBackoff(prev time.Duration, held bool) time.Duration {
	if held {
		return 0
	}
	if prev <= 0 {
		return brainBackoffMin
	}
	return min(2*prev, brainBackoffMax)
}

// heldConnection says whether a connection that ended counts as healthy for
// the backoff: it delivered a `preferred` event and lasted at least
// brainSilence, the span the watchdog allows between two signs of life. Event
// alone is not enough: a brain that answers each connect with one document
// and a close would then be redialled with no wait, the whole document
// (about 80 KB) each time.
func heldConnection(delivered bool, lasted time.Duration) bool {
	return delivered && lasted >= brainSilence
}

// runBrainStream holds the subscription until ctx ends.
func (tn *tenant) runBrainStream(ctx context.Context) {
	var wait time.Duration
	for {
		start := time.Now()
		good, err := tn.brainStreamOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		tn.brainFail("stream", err)
		wait = nextBackoff(wait, heldConnection(good, time.Since(start)))
		if wait == 0 {
			continue
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// brainStreamOnce holds one connection to the brain's /events until it drops,
// closes or falls silent; it always returns a non-nil error saying which. good
// is whether it delivered at least one `preferred` event. Its first event
// queues the reports' resync (reportResync).
func (tn *tenant) brainStreamOnce(parent context.Context) (good bool, err error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var silent atomic.Bool
	watch := time.AfterFunc(brainSilence, func() { silent.Store(true); cancel() })
	defer watch.Stop()
	defer func() {
		if silent.Load() {
			err = fmt.Errorf("%w for %s", errBrainSilent, brainSilence)
		}
	}()

	req, _ := http.NewRequestWithContext(ctx, "GET", tn.brainURL()+"/events", nil)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := brainStreamClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("GET /events: %s", resp.Status)
	}
	body := &heardReader{r: resp.Body, heard: func() {
		watch.Reset(brainSilence)
		tn.pref.mu.Lock()
		tn.pref.heardAt = time.Now()
		tn.pref.mu.Unlock()
	}}
	err = readSSE(body, func(event, data string) {
		if event != "preferred" {
			return
		}
		first := !good
		good = true
		tn.applyPreferred(strings.NewReader(data))
		if first {
			// A brain that restarted, or missed a post, catches up: the
			// connection's first event queues every live record the brain
			// takes and the latest reading of each window (64 §5, 67 §9). Queued, not sent:
			// nothing slow runs on the reader.
			tn.reportResync()
		}
	})
	if err == nil {
		err = errors.New("the brain closed /events")
	}
	return good, err
}

// heardReader calls heard whenever bytes arrive.
type heardReader struct {
	r     io.Reader
	heard func()
}

func (h *heardReader) Read(p []byte) (int, error) {
	n, err := h.r.Read(p)
	if n > 0 {
		h.heard()
	}
	return n, err
}

// readSSE parses a text/event-stream (the WHATWG rules): lines end in "\r\n",
// "\n" or "\r"; a line starting with ':' is a comment; "field: value" drops one
// space after the colon; data lines join with "\n"; a blank line dispatches the
// event, unless it has no data. event defaults to "message". id and retry are
// read and ignored: the brain replays nothing, and the reconnect policy is the
// proxy's own. An event cut off by the end of the stream is discarded. It
// returns nil at the end of the stream, else the read error.
func readSSE(r io.Reader, dispatch func(event, data string)) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 64<<20) // a /preferred document is ~80 KB on one line
	sc.Split(sseLines)
	var event string
	var data strings.Builder
	hasData := false
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			if hasData {
				ev := event
				if ev == "" {
					ev = "message"
				}
				dispatch(ev, data.String())
			}
			event, hasData = "", false
			data.Reset()
			continue
		}
		if line[0] == ':' {
			continue
		}
		field, value, _ := bytes.Cut(line, []byte(":"))
		value = bytes.TrimPrefix(value, []byte(" "))
		switch string(field) {
		case "event":
			event = string(value)
		case "data":
			if hasData {
				data.WriteByte('\n')
			}
			data.Write(value)
			hasData = true
		}
	}
	return sc.Err()
}

// sseLines splits on "\r\n", "\n" or a lone "\r". A "\r" at the end of what has
// arrived waits for the next byte, which says whether a "\n" belongs to it.
func sseLines(data []byte, atEOF bool) (int, []byte, error) {
	for i, b := range data {
		switch b {
		case '\n':
			return i + 1, data[:i], nil
		case '\r':
			if i+1 < len(data) {
				if data[i+1] == '\n' {
					return i + 2, data[:i], nil
				}
				return i + 1, data[:i], nil
			}
			if atEOF {
				return i + 1, data[:i], nil
			}
			return 0, nil, nil
		}
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}
