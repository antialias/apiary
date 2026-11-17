package main

// Reading what an upstream's refusal says about WHEN — the reset a 429 or a
// 403 names, in a header or somewhere in its body. What a refusal MEANS (its
// class, and whether the walk moves on) is the adapter's (refusal.go, §2.6);
// this file only finds the time it names, and builds the unavailable reply
// the walk ends on when nothing is left to dial.
//
// Everything here parses OTHER PEOPLE'S error bodies, which is to say it parses
// shapes nobody promised us and which will change without notice. So it is
// deliberately tolerant rather than precise: headers before bodies (those are
// standardized), a depth-agnostic scan for a small set of key names, and both
// epoch and RFC3339 accepted for each. Anything it cannot read yields a zero
// reset, which the record store reads as "the refusal named none" — never an
// error, never a panic, never a refusal that depends on the parse succeeding.

import (
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// resetKeys are the JSON keys an upstream might use to say WHEN. Scanned at any
// depth: the codex backend nests under "detail", Anthropic-shaped bodies under
// "error", and neither promises to keep doing so.
var resetKeys = map[string]bool{
	"resets_at": true, "reset_at": true, "resetsat": true,
	"resets_in_seconds": true, "reset_in_seconds": true,
	"resets_in": true, "retry_after": true, "retry_after_seconds": true,
}

// quotaResetAt digs the reset instant out of a refusal. Headers win — they are
// standardized and the body is not. Returns the zero time when the upstream
// said nothing readable, which the caller treats as "use the floor".
func quotaResetAt(h http.Header, body []byte, now time.Time) time.Time {
	if h != nil {
		// Retry-After is RFC 7231: either delta-seconds or an HTTP-date.
		if v := strings.TrimSpace(h.Get("Retry-After")); v != "" {
			if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
				return now.Add(time.Duration(secs) * time.Second)
			}
			if t, err := http.ParseTime(v); err == nil {
				return t
			}
		}
		// Provider reset headers, in the order we are likeliest to see them.
		for _, k := range []string{
			"anthropic-ratelimit-unified-reset",
			"anthropic-ratelimit-requests-reset",
			"x-ratelimit-reset-requests",
			"x-ratelimit-reset",
		} {
			if t := parseResetValue(strings.TrimSpace(h.Get(k)), now); !t.IsZero() {
				return t
			}
		}
	}
	return scanResetInJSON(body, now)
}

// scanResetInJSON walks the decoded body looking for any resetKeys entry, at any
// depth. Depth-agnostic on purpose: the nesting is the part most likely to move.
func scanResetInJSON(body []byte, now time.Time) time.Time {
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		return time.Time{}
	}
	var found time.Time
	var walk func(any)
	walk = func(n any) {
		if !found.IsZero() {
			return
		}
		switch t := n.(type) {
		case map[string]any:
			for k, val := range t {
				if !found.IsZero() {
					return
				}
				if resetKeys[strings.ToLower(k)] {
					if got := coerceReset(val, now); !got.IsZero() {
						found = got
						return
					}
				}
				walk(val)
			}
		case []any:
			for _, val := range t {
				walk(val)
			}
		}
	}
	walk(v)
	return found
}

// coerceReset accepts the two shapes a reset ever arrives in: an absolute
// instant (epoch seconds, epoch millis, or RFC3339) or a relative duration in
// seconds. They are told apart by magnitude — a value below ~1e8 cannot be a
// plausible epoch and is read as seconds-from-now.
func coerceReset(v any, now time.Time) time.Time {
	switch t := v.(type) {
	case float64:
		return numericReset(t, now)
	case json.Number:
		if f, err := t.Float64(); err == nil {
			return numericReset(f, now)
		}
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return time.Time{}
		}
		if f, ok := parseFiniteFloat(s); ok {
			return numericReset(f, now)
		}
		for _, layout := range []string{time.RFC3339, time.RFC3339Nano, "2006-01-02T15:04:05Z0700", "2006-01-02 15:04:05"} {
			if ts, err := time.Parse(layout, s); err == nil {
				return ts
			}
		}
	}
	return time.Time{}
}

// epochFloor is the smallest value we will read as an absolute epoch: 1973-03-03.
// Anything smaller is a duration — a real reset is never 40 years ago, and a
// relative wait is never 50 million seconds.
const epochFloor = 1e8

func numericReset(f float64, now time.Time) time.Time {
	switch {
	case f <= 0 || math.IsNaN(f) || math.IsInf(f, 0):
		return time.Time{}
	case f >= epochFloor*1000: // epoch milliseconds
		return time.UnixMilli(int64(f))
	case f >= epochFloor: // epoch seconds
		return time.Unix(int64(f), 0)
	default: // seconds from now
		return now.Add(time.Duration(f) * time.Second)
	}
}

// parseFiniteFloat is strconv.ParseFloat for a value read off the wire,
// refusing what ParseFloat takes but no reading can be: NaN and ±Inf. One of
// them in a reading would also make it unencodable as JSON.
func parseFiniteFloat(s string) (float64, bool) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return f, true
}

// parseResetValue is coerceReset for a raw header string.
func parseResetValue(s string, now time.Time) time.Time {
	if s == "" {
		return time.Time{}
	}
	return coerceReset(s, now)
}

// ---- the router-wide clause ------------------------------------------------

// routerViewStale is the brain's liveness rule (brain_stream.go) read at now:
// the view may be spoken for only while the brain was heard from (a document
// or a keepalive) within brainSilence. The stream flips pref.ok when that
// lapses, so this only catches the case ok cannot: a wedged stream goroutine
// sitting on an ancient snapshot that still claims to be good.
func routerViewStale(heard time.Time, now time.Time) bool {
	return heard.IsZero() || now.Sub(heard) > brainSilence
}

// routerDry reports whether EVERY class on this router is currently out of
// candidates, and whether we are entitled to an opinion at all.
//
// Note what this deliberately does NOT claim. The router cannot see an
// upstream's wallet: kimi once sat at effective 57 against a hard line
// of 95 and still answered 403 "5-hour usage limit", because the brain's number
// tracks the weekly window and the thing that actually refused was a rolling
// five-hour one. A clause asserting "no budget anywhere" would have been
// confidently wrong then, which is the one failure mode this whole change
// exists to stop.
//
// So the claim is about THIS ROUTER'S OWN BEHAVIOUR, which it does know exactly:
// would a request for any other model find a candidate? That is decided by
// classCandidates — the same function real routing uses — so the sentence is
// true by construction rather than inferred from what happened last. It answers
// the only question the refused consumer has ("would my own failover help?")
// without asserting anything we cannot see.
//
// known is false when the brain view is unreachable, stale, or empty: we are
// serving a last-good snapshot and are not entitled to speak for the fleet. The
// caller then says nothing at all. The only wrong answer here is a confident one.
//
// No wire or kind scoping. A consumer's own failover may arrive on a different
// wire than the request being refused, so a member this request could not have
// crossed to may still be one the NEXT request reaches. Counting every member
// can only produce silence, never a false "nothing is routable".
func (tn *tenant) routerDry() (dry, known bool) {
	tn.pref.mu.RLock()
	ok, heard := tn.pref.ok, tn.pref.heardAt
	classes := make(map[string][]classMemberPref, len(tn.pref.classes))
	for name, cp := range tn.pref.classes {
		classes[name] = cp.Members
	}
	tn.pref.mu.RUnlock()

	if !ok || routerViewStale(heard, nowFn()) || len(classes) == 0 {
		return false, false
	}
	// pref.mu is released BEFORE the checks are read: classCandidates takes
	// recMu and re-takes pref.mu, and routeClass already orders them this way.
	// Keeping that the only order means they are never nested.
	for _, members := range classes {
		if len(tn.classCandidates(nil, members, "")) > 0 {
			return false, true
		}
	}
	return true, true
}

// routerDryClause is the one sentence appended to a quota refusal.
//
// Negative only, on purpose. "Something else is routable" would be a claim with
// a shelf life measured in seconds and no action attached — the consumer learns
// nothing it could not discover by simply trying. "Nothing is routable" is the
// one that changes behaviour: it says a failover round-trip is wasted effort.
func (tn *tenant) routerDryClause() string {
	if dry, known := tn.routerDry(); known && dry {
		return "; no other model on this router is routable right now"
	}
	return ""
}

// classExhaustAfterWalk is the walk's terminal-reply switch (§2.8 item 7):
// when the walk ran out of rungs on a refusal the taxonomy walks (§2.6) and
// no member this wire can use now has an account whose check admits a dial,
// the class is unavailable and the reply says so from the checks, for every
// client. The anthropic member's 429s once carried their windows in
// headers, not quota phrases; the records the walk wrote from them are what
// the checks now read. An Anthropic-wire client once heard a spent class's 429 as
// "retry the same model in 60 s"; the 529 it gets instead is one backoff,
// then its own failover.
//
// The last refusal is classified by the member that gave it. A wrong-member
// refusal (#90) is not a usage fact about anyone, and a throttle is not
// exhaustion (a throttled account's check still admits it): both are
// surfaced intact.
func classExhaustAfterWalk(cc *classRoute, last *http.Response) *http.Response {
	if cc == nil || cc.unavail != nil || last == nil {
		return nil
	}
	m, ok := memberByID(cc.members, cc.served.snapshot().member)
	if !ok {
		return nil
	}
	tn := cc.tenant
	if ref := tn.rungRefusal(m, last); !ref.walks() || ref.class == refWrongMember {
		return nil
	}
	u := tn.classUnavailable(cc.members, cc.classReq, nowFn())
	if u == nil {
		return nil
	}
	cc.unavail = u
	return tn.unavailableResponse(cc)
}

// ---- the drill endpoint -----------------------------------------------------

// quotaDrillHandler renders the unavailable reply a class WOULD be served
// right now, on each wire, built by the same functions that serve the real
// one.
//
// Strictly read-only. It dials nobody, writes no record, changes no routing
// decision and logs no outage — the only thing it can affect is its own
// response. That is the whole design constraint: a diagnostic for a refusal path
// must not be able to cause the refusal.
//
// It exists because of the one gap the test suite cannot close: whether the
// checks and routerDry() read PRODUCTION'S actual class shapes correctly — real
// member kinds, real standings, a real brain snapshot. This answers that against
// live state, on the deployed binary. Behind the client gate like everything
// but healthz.
//
// The body is built over every member whether or not the class is unavailable
// (class_unavailable says which), so a class that serves still shows what its
// reply would hold; routerDryClause reads live state either way.
func quotaDrillHandler(w http.ResponseWriter, r *http.Request) {
	class := r.URL.Query().Get("class")
	if class == "" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"class query parameter is required"}`))
		return
	}
	// The drill reads the entry's tenant: what ITS class would be served.
	tn := mustTenant(w, r)
	if tn == nil {
		return
	}
	tn.pref.mu.RLock()
	cp, found := tn.pref.classes[class]
	members := cp.Members
	known := make([]string, 0, len(tn.pref.classes))
	for name := range tn.pref.classes {
		known = append(known, name)
	}
	tn.pref.mu.RUnlock()
	sort.Strings(known)

	if !found {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": "no such class", "class": class, "known_classes": known,
		})
		return
	}

	now := nowFn()
	unavailableNow := tn.classUnavailable(members, classReq{ctx: r.Context()}, now) != nil
	var routes []unavailRoute
	for _, m := range members {
		if why := tn.memberUnroutable(r.Context(), m); why != "" {
			routes = append(routes, unavailRoute{policy: routePolicy{ID: m.Route}, unroutable: why})
			continue
		}
		rp, _, _, _ := tn.routeOf(m.Route)
		routes = append(routes, unavailRoute{policy: rp.routePolicy, accounts: rp.Accounts})
	}
	u := tn.routesUnavailable(routes, modelsOf(members), now)
	dry, dryKnown := tn.routerDry()
	// No member's route has an account: there is no unavailable reply, only
	// the typed 503 (configuration), so would_serve is null.
	var serve map[string]any
	if u != nil {
		serve = map[string]any{}
		for _, rw := range []replyWire{replyClaudeCode, replyAnthropic, replyOpenAI} {
			status, hd, body, msg := renderUnavailable(u, "class "+class, class, "", rw, now)
			serve[rw.String()] = map[string]any{
				"status": status, "headers": hd, "message": msg, "body": json.RawMessage(body),
			}
		}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"class":             class,
		"class_unavailable": unavailableNow,
		"router_dry":        dry,
		"router_dry_known":  dryKnown,
		"clause":            tn.routerDryClause(),
		"known_classes":     known,
		"would_serve":       serve,
	})
}
