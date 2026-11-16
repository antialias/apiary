package main

// upstreamTrace records how a codex stream arrived, for the access log line:
// the upstream's events by type, how long the first one took, the longest
// silence, and how the read ended. The translator forwards nothing for event
// types the client has no shape for (reasoning), so without this record a
// stream GPT stopped sending and one GPT was still reasoning on look the same:
// a 200 the client gave up on.

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"
)

type upstreamTrace struct {
	mu     sync.Mutex
	head   time.Duration // the proxy got the request → the upstream's 200 head; -1 unknown
	start  time.Time     // the upstream's 200 head
	first  time.Duration // start → the first event; -1 until one arrives
	last   time.Time     // the latest event, or start
	maxGap time.Duration // the longest silence between start and the latest event
	types  []string      // event types in first-seen order
	counts map[string]int
	lastTy string
	end    string // how the read ended; "" while it goes on
	endAt  time.Time
}

// newUpstreamTrace starts the record at the upstream's 200 head. ctx is the
// hop's; requestHandler's start rides it as reqStartKey.
func newUpstreamTrace(ctx context.Context, now time.Time) *upstreamTrace {
	u := &upstreamTrace{head: -1, start: now, first: -1, last: now, counts: map[string]int{}}
	if t0, ok := ctx.Value(reqStartKey).(time.Time); ok {
		u.head = now.Sub(t0)
	}
	return u
}

// event records one upstream event. typ is its type, "" for a frame that did
// not parse.
func (u *upstreamTrace) event(typ string, at time.Time) {
	if u == nil {
		return
	}
	typ = strings.TrimPrefix(typ, "response.")
	if typ == "" {
		typ = "unparsed"
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.first < 0 {
		u.first = at.Sub(u.start)
	}
	if g := at.Sub(u.last); g > u.maxGap {
		u.maxGap = g
	}
	u.last = at
	if u.counts[typ] == 0 {
		u.types = append(u.types, typ)
	}
	u.counts[typ]++
	u.lastTy = typ
}

// ended records why the read stopped: a terminal event (done), the upstream
// closed without one (eof), the client went away while we waited (canceled) or
// while we wrote to it (client_gone), or the read failed (read_error).
func (u *upstreamTrace) ended(terminal bool, writeErr, readErr error, at time.Time) {
	if u == nil {
		return
	}
	how := "eof"
	switch {
	case terminal:
		how = "done"
	case writeErr != nil:
		how = "client_gone"
	case errors.Is(readErr, context.Canceled) || errors.Is(readErr, context.DeadlineExceeded):
		how = "canceled"
	case readErr != nil:
		how = "read_error"
	}
	u.mu.Lock()
	u.end, u.endAt = how, at
	u.mu.Unlock()
}

// logTag renders the record for the access log line. A read still going at
// now is measured to now and ends "reading".
func (u *upstreamTrace) logTag(now time.Time) string {
	if u == nil {
		return ""
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	end, at := u.end, u.endAt
	if end == "" {
		end, at = "reading", now
	}
	gap := u.maxGap
	if g := at.Sub(u.last); g > gap {
		gap = g
	}
	last := u.lastTy
	if last == "" {
		last = "none"
	}
	var b strings.Builder
	b.WriteString(" up=head:" + traceDur(u.head) + ",first:" + traceDur(u.first) + ",gap:" + traceDur(gap) +
		",last:" + last + ",end:" + end + " upev=")
	if len(u.types) == 0 {
		b.WriteString("none")
	}
	for i, t := range u.types {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(t + ":" + strconv.Itoa(u.counts[t]))
	}
	return b.String()
}

func traceDur(d time.Duration) string {
	if d < 0 {
		return "none"
	}
	return d.Round(10 * time.Millisecond).String()
}
