package main

// The subscription actuator: an Anthropic account signed in with a
// subscription login. The brain is the sole refresher of each account's
// grant and dispenses the access token (GET /token?account=…, secret-guarded);
// this proxy caches each copy, pre-warms it before it expires, injects it as
// a Bearer with the oauth beta flag Anthropic requires beside it, and answers
// Anthropic's 401 on a copy by asking the brain once more (anthropicReauth).
// Moved out of main.go whole; core reaches it only through actuators.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

func init() {
	actuators[subscriptionKind] = &actuator{
		credential: (*tenant).getToken,
		sign: func(h http.Header, tok string) {
			h.Set("Authorization", "Bearer "+tok)
			// anthropic-beta / anthropic-version are forwarded verbatim — EXCEPT
			// that an injected OAuth bearer must carry the oauth beta flag: a
			// client in ANTHROPIC_AUTH_TOKEN mode (the gated deployment) believes
			// it is on API-key auth and omits it, and Anthropic 401s OAuth
			// bearers without it.
			ensureOAuthBeta(h)
		},
		signed: func(h http.Header) string {
			tok, _ := strings.CutPrefix(h.Get("Authorization"), "Bearer ")
			return tok
		},
		accountOf: accountByBearer,
		refused:   (*failoverTransport).anthropicReauth,
		warm:      (*tenant).warmSubscriptions,
	}
}

// warmSubscriptions pre-warms the token cache for EVERY subscription account
// any lane routes to (not just the default), so a freshly-flipped lane's
// first request doesn't stall on a fetch. Every routed account is warmed —
// none is privileged, all get injected.
func (tn *tenant) warmSubscriptions(warm map[string]bool) {
	tn.setWarmSet(warm)
	for a := range warm {
		if a != "" {
			prewarmWG.Add(1)
			go func(a string) { defer prewarmWG.Done(); tn.getToken(a) }(a)
		}
	}
}

// ---- token cache (per account) ----
type tokEntry struct {
	token string
	exp   time.Time
	// fetchedAt is when this copy came from the brain (storeToken).
	fetchedAt time.Time
	// refused is the last time Anthropic refused this account's injected copy
	// (a 401 authentication_error), and reauths how many times a refusal made
	// this proxy ask the brain again (reauthToken). Both outlive the copy they
	// name: /_switch/status shows them as anthropic_tokens.
	refused refusedCopy
	reauths int
	// quietUntil: a reauth that got nothing fresher (the same token, a failed
	// fetch, or a fresh copy refused too) holds the brain unasked for this
	// account until then (reauthQuiet; reauthFailQuiet after a failed fetch).
	// quietFor is which of the two it was, for the log line. A new copy clears
	// it.
	quietUntil time.Time
	quietFor   time.Duration
}

// refusedCopy is one refusal of an injected copy: when, the copy's
// fingerprint and Anthropic's words, and the copy's own fetch and expiry.
type refusedCopy struct {
	at             time.Time
	fp, msg        string
	fetchedAt, exp time.Time
}

// The cache is per tenant: tenant.tokMu, tenant.tokens.

// fetchToken asks the brain for account's access token. refusedFP, when set,
// is the fingerprint (fp) of the copy Anthropic just refused: the brain records
// the refusal and checks its own copy (an old brain ignores the parameter).
func (tn *tenant) fetchToken(account, refusedFP string) (string, time.Time, error) {
	q := "/token?account=" + url.QueryEscape(account)
	if refusedFP != "" {
		q += "&refused_fp=" + url.QueryEscape(refusedFP)
	}
	req, _ := http.NewRequest("GET", tn.brainURL()+q, nil)
	req.Header.Set("X-Switch-Key", switchKey)
	resp, err := brainClient.Do(req)
	if err != nil {
		return "", time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", time.Time{}, fmt.Errorf("brain /token %d", resp.StatusCode)
	}
	var d struct {
		AccessToken string  `json:"access_token"`
		ExpiresAt   float64 `json:"expires_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil || d.AccessToken == "" {
		return "", time.Time{}, fmt.Errorf("bad /token body")
	}
	return d.AccessToken, time.Unix(int64(d.ExpiresAt), 0), nil
}

// getToken returns a currently-valid access token for account, or "" if none can be
// obtained (caller then routes FAILED — a loud 503, never the client's ambient bearer).
// A still-valid cached token is returned when a live fetch fails (this is what makes a
// brain-down route STALE rather than FAILED), unless a drop happened since it was read:
// a dropped token is never served. The network fetch runs without the lock
// held so concurrent requests don't serialize behind it.
func (tn *tenant) getToken(account string) string {
	// Read before the cache: a drop after it moves the count, so neither a
	// fetch's answer nor the copy read here is kept or served stale.
	gen := tn.credDrops.Load()
	tn.tokMu.Lock()
	e := tn.tokens[account]
	tn.tokMu.Unlock()
	if e.token != "" && time.Until(e.exp) > tokenRefreshMargin {
		return e.token
	}
	tok, exp, err := tn.fetchToken(account, "")
	if err != nil {
		if e.token != "" && time.Now().Before(e.exp) && tn.credDrops.Load() == gen {
			return e.token // still-valid cached token (keeps a brain-down route STALE, not FAILED)
		}
		log.Printf("token fetch for %s failed: %v (no cached token — route will FAIL loud)", account, err)
		return ""
	}
	tn.storeToken(account, tok, exp, gen)
	return tok
}

// storeToken caches a freshly fetched token and arms its prewarm timer. The
// account's refusal history stays; a new copy lifts the reauth quiet. gen is
// credDrops as the fetch began: a drop since then caches nothing.
func (tn *tenant) storeToken(account, tok string, exp time.Time, gen uint64) {
	tn.tokMu.Lock()
	if tn.credDrops.Load() != gen {
		tn.tokMu.Unlock()
		return
	}
	e := tn.tokens[account]
	if e.token != tok {
		e.quietUntil = time.Time{}
	}
	e.token, e.exp, e.fetchedAt = tok, exp, time.Now()
	tn.tokens[account] = e
	tn.tokMu.Unlock()
	tn.armPrewarm(account, exp)
}

// reauthQuiet is how long a reauth that got nothing fresher keeps this proxy
// from asking the brain again for that account: a dead grant answers every
// request with the proxy's 403 at once instead of a brain round trip each.
var reauthQuiet = 30 * time.Second

// reauthFailQuiet is the shorter quiet after a brain fetch that failed: the
// brain may answer a moment later (a restart, a slow usage check), so a failure
// only spaces the asks out instead of holding the account to the 403 for
// reauthQuiet. The per-account lock already makes a burst one fetch.
var reauthFailQuiet = 3 * time.Second

// reauthLocks serializes the reauths of one account, so a burst of 401s on a
// rotated copy makes one brain fetch: the first holder fetches, the rest find
// the copy already replaced.
// Per tenant: tenant.reauthLocksMu, tenant.reauthLocks.

func (tn *tenant) reauthLock(account string) *sync.Mutex {
	tn.reauthLocksMu.Lock()
	defer tn.reauthLocksMu.Unlock()
	m := tn.reauthLocks[account]
	if m == nil {
		m = &sync.Mutex{}
		tn.reauthLocks[account] = m
	}
	return m
}

// reauthResult is what reauthToken did, for its caller's one log line.
type reauthResult struct {
	tok string // the copy to retry with; "" = nothing fresher
	// how: "fetched" (asked the brain, got a new copy), "replaced" (another
	// request already had), "same" (the brain handed back the refused copy),
	// "failed" (the fetch failed: err), "quiet" (within reauthQuiet of a
	// reauth that got nothing fresher; the brain was not asked).
	how      string
	err      error
	quietFor time.Duration // how: "quiet" — the quiet in force
	old      refusedCopy   // the refused copy (fetchedAt zero when not known)
}

// reauthToken answers Anthropic refusing account's injected copy refused (a
// 401 authentication_error saying msg): the cached copy when it is no longer
// the refused one, else a fresh one from the brain, told which copy was
// refused. It never empties the cache: accountByBearer and getToken's
// brain-down fallback read it, and storeToken re-arms the prewarm itself.
func (tn *tenant) reauthToken(account, refused, msg string) reauthResult {
	mu := tn.reauthLock(account)
	mu.Lock()
	defer mu.Unlock()
	now := time.Now()
	rfp := fp("Bearer " + refused)
	tn.tokMu.Lock()
	e := tn.tokens[account]
	if e.token != "" && e.token != refused {
		old := refusedCopy{fp: rfp}
		if e.refused.fp == rfp {
			old = e.refused
		}
		tn.tokMu.Unlock()
		return reauthResult{tok: e.token, how: "replaced", old: old}
	}
	old := refusedCopy{at: now, fp: rfp, msg: msg, fetchedAt: e.fetchedAt, exp: e.exp}
	quiet := now.Before(e.quietUntil)
	if e.token != "" {
		e.refused = old
		if !quiet {
			e.reauths++
		}
		tn.tokens[account] = e
	}
	tn.tokMu.Unlock()
	if quiet {
		return reauthResult{how: "quiet", old: old, quietFor: e.quietFor}
	}
	gen := tn.credDrops.Load()
	tok, exp, err := tn.fetchToken(account, rfp)
	switch {
	case err != nil:
		tn.quietAccount(account, refused, reauthFailQuiet)
		return reauthResult{how: "failed", err: err, old: old}
	case tok == refused:
		tn.quietAccount(account, refused, reauthQuiet)
		return reauthResult{how: "same", old: old}
	}
	tn.storeToken(account, tok, exp, gen)
	return reauthResult{tok: tok, how: "fetched", old: old}
}

// quietAccount starts account's reauth quiet, for d, while its cached copy is
// still tok: a copy that moved on meanwhile is not held to it.
func (tn *tenant) quietAccount(account, tok string, d time.Duration) {
	tn.tokMu.Lock()
	defer tn.tokMu.Unlock()
	if e := tn.tokens[account]; e.token == tok {
		e.quietUntil = time.Now().Add(d)
		e.quietFor = d
		tn.tokens[account] = e
	}
}

// noteRetryRefused records Anthropic refusing the fresh copy a reauth retried
// on too, and quiets the account: a copy straight from the brain refused at
// once is a grant the brain cannot mend by being asked again.
func (tn *tenant) noteRetryRefused(account, tok, msg string) {
	tn.tokMu.Lock()
	if e := tn.tokens[account]; e.token == tok {
		e.refused = refusedCopy{at: time.Now(), fp: fp("Bearer " + tok), msg: msg, fetchedAt: e.fetchedAt, exp: e.exp}
		tn.tokens[account] = e
	}
	tn.tokMu.Unlock()
	tn.quietAccount(account, tok, reauthQuiet)
}

// anthropicTokensStatus is /_switch/status's anthropic_tokens: per account,
// the cached copy's fingerprint, when it was fetched and expires, the last
// refusal Anthropic gave an injected copy, and how many reauths refusals
// caused. tokMu is held by the caller.
func (tn *tenant) anthropicTokensStatus() map[string]any {
	out := make(map[string]any, len(tn.tokens))
	for a, e := range tn.tokens {
		if e.token == "" {
			continue
		}
		var last any
		if !e.refused.at.IsZero() {
			last = map[string]any{"at": e.refused.at.Format(time.RFC3339), "fp": e.refused.fp, "msg": e.refused.msg}
		}
		out[a] = map[string]any{
			"fp":           fp("Bearer " + e.token),
			"fetched_at":   rfc3339OrNil(e.fetchedAt),
			"expires_at":   rfc3339OrNil(e.exp),
			"last_refused": last,
			"reauths":      e.reauths,
		}
	}
	return out
}

func rfc3339OrNil(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.Format(time.RFC3339)
}

// tokenRefreshMargin is how close to its expiry a cached token may get before
// getToken fetches a fresh one, and when the prewarm timer refreshes it.
const tokenRefreshMargin = 10 * time.Minute

// The prewarm keeps every routed account's cached token more than
// tokenRefreshMargin from expiry without waiting for a request or a document:
// a token's own clock says when. warmSet is the routed accounts the last
// applied document named (applyPreferred); warmTimers holds one timer per
// account, armed when getToken stores a token, firing at exp - margin.
// Per tenant: tenant.warmMu, tenant.warmSet, tenant.warmTimers.

// A timed refresh that fails is tried again while the cached token lasts:
// after prewarmRetryMin, doubling up to prewarmRetryMax, and never later than
// halfway to the token's expiry, so a retry lands before it expires. Once the
// wait that leaves would be under prewarmRetryMin (under twice that left on
// the token), the timer gives up and the request path and the next document
// cover the account, as before. Variables so tests can shrink them.
var (
	prewarmRetryMin = 30 * time.Second
	prewarmRetryMax = 2 * time.Minute
)

// nextPrewarmRetry is the wait before retrying a failed timed refresh: prev is
// the wait before the attempt that failed (0 for the timer at the margin), and
// left the cached token's remaining life. 0 means no retry.
func nextPrewarmRetry(prev, left time.Duration) time.Duration {
	w := prewarmRetryMin
	if prev > 0 {
		w = min(2*prev, prewarmRetryMax)
	}
	w = min(w, left/2)
	if w < prewarmRetryMin {
		return 0
	}
	return w
}

// setWarmSet replaces the routed accounts and stops the timers of those that
// left them. A routed account with no timer gets one from its cached token: an
// account that rejoins the set with a still-fresh token would otherwise have
// none, since getToken's cache hit arms nothing. tokMu is taken inside warmMu
// here; nothing takes warmMu while holding tokMu.
func (tn *tenant) setWarmSet(w map[string]bool) {
	tn.warmMu.Lock()
	defer tn.warmMu.Unlock()
	tn.warmSet = w
	for a, t := range tn.warmTimers {
		if !w[a] {
			t.Stop()
			delete(tn.warmTimers, a)
		}
	}
	for a := range w {
		if a == "" || tn.warmTimers[a] != nil {
			continue
		}
		tn.tokMu.Lock()
		e := tn.tokens[a]
		tn.tokMu.Unlock()
		if e.token != "" {
			tn.armPrewarmLocked(a, e.exp)
		}
	}
}

// armPrewarm (re)arms account's timer for exp - tokenRefreshMargin. A token
// already inside the margin arms nothing (no tight loop: the request path and
// the next document cover it), nor does an account outside the warm set. A
// timed refresh that fails re-arms itself (prewarmRefresh); a failed fetch on
// any other path arms nothing.
func (tn *tenant) armPrewarm(account string, exp time.Time) {
	tn.warmMu.Lock()
	defer tn.warmMu.Unlock()
	tn.armPrewarmLocked(account, exp)
}

// armPrewarmLocked is armPrewarm with warmMu held.
func (tn *tenant) armPrewarmLocked(account string, exp time.Time) {
	left := time.Until(exp) - tokenRefreshMargin
	if !tn.warmSet[account] || left <= 0 {
		return
	}
	tn.armWarmTimerLocked(account, left, 0)
}

// armWarmTimerLocked (re)arms account's one timer to run prewarmRefresh after
// d; wait is the wait it was armed with when it is a retry, 0 at the margin.
func (tn *tenant) armWarmTimerLocked(account string, d, wait time.Duration) {
	if t := tn.warmTimers[account]; t != nil {
		t.Stop()
	}
	var t *time.Timer
	t = time.AfterFunc(d, func() {
		tn.warmMu.Lock()
		live := tn.warmTimers[account] == t
		if live {
			delete(tn.warmTimers, account)
			prewarmWG.Add(1)
		}
		tn.warmMu.Unlock()
		if live {
			defer prewarmWG.Done()
			tn.prewarmRefresh(account, wait)
		}
	})
	tn.warmTimers[account] = t
}

// prewarmRefresh is a timer's fetch. On success storeToken arms the next one
// from the new expiry. On failure the cached token still serves (STALE when
// the brain is down) and the refresh is armed again (nextPrewarmRetry),
// unless another path armed a timer meanwhile or the account left the warm
// set. The first failure of a run is logged; the retries are not.
func (tn *tenant) prewarmRefresh(account string, wait time.Duration) {
	gen := tn.credDrops.Load()
	tok, exp, err := tn.fetchToken(account, "")
	if err == nil {
		tn.storeToken(account, tok, exp, gen)
		return
	}
	tn.tokMu.Lock()
	e := tn.tokens[account]
	tn.tokMu.Unlock()
	next := nextPrewarmRetry(wait, time.Until(e.exp))
	if wait == 0 {
		if next > 0 {
			log.Printf("timed token refresh for %s failed: %v — retrying in %s; the cached token expires %s", account, err, next, e.exp.Format(time.RFC3339))
		} else {
			log.Printf("timed token refresh for %s failed: %v — too near expiry to retry", account, err)
		}
	}
	if next == 0 {
		return
	}
	tn.warmMu.Lock()
	defer tn.warmMu.Unlock()
	if tn.warmSet[account] && tn.warmTimers[account] == nil {
		tn.armWarmTimerLocked(account, next, next)
	}
}

// anthropicReauth answers Anthropic's 401 on an account's injected copy. The
// brain may already hold a newer copy: a sign-in elsewhere (a cloud session's
// /harness-login) refreshes the grant early, which revokes every copy handed
// out before, while this proxy keeps its copy until 10 minutes before it
// expires (35 minutes of "OAuth token revoked · Please run
// /login"). So it re-fetches (reauthToken) and replays the dialled body once
// on the fresh copy. When there is none, or Anthropic refuses that too, the
// client gets the proxy's own 403 (reauthRefusedResponse), not a 401 its own
// re-auth would answer. Only a hop on an Anthropic account's copy qualifies:
// kimi's hops have no wire either, and the manual inject file has no brain
// copy to fetch. resp is returned as it came otherwise.
func (t *failoverTransport) anthropicReauth(req *http.Request, resp *http.Response) (*http.Response, error) {
	if t.anthropic == nil || req.URL.Host != t.anthropic.Host {
		return resp, nil
	}
	d, ok := anthDialOf(req)
	if !ok || d.acct == "" || strings.HasPrefix(d.acct, "manual:") {
		return resp, nil
	}
	typ, msg := errBodyTypeMsg(bufferResp(resp))
	if typ != "authentication_error" {
		return resp, nil
	}
	if len(msg) > 300 {
		msg = msg[:300] + "…"
	}
	refused := strings.TrimSpace(strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer "))
	tn := d.tenant
	r := tn.reauthToken(d.acct, refused, msg)
	fetched, left := "?", "? left"
	if !r.old.fetchedAt.IsZero() {
		fetched = r.old.fetchedAt.Format("15:04")
	}
	if !r.old.exp.IsZero() {
		left = "expired"
		if l := time.Until(r.old.exp); l > 0 {
			left = shortDur(l) + " left"
		}
	}
	head := fmt.Sprintf("upstream 401 %s: %s (fp %s, fetched %s, %s)", d.acct, tn.logProviderText("", typ, msg), r.old.fp, fetched, left)
	if r.tok == "" || r.tok == refused {
		switch r.how {
		case "failed":
			log.Printf("%s — re-fetch failed (%v); answered 403", head, r.err)
		case "quiet":
			log.Printf("%s — not re-fetched within %s of a reauth that got nothing fresher; answered 403", head, r.quietFor)
		default:
			log.Printf("%s — brain returned the same token; answered 403", head)
		}
		resp.Body.Close()
		return reauthRefusedResponse(req, d.acct, msg), nil
	}
	if r.how == "replaced" {
		log.Printf("%s — already re-fetched fp %s, retrying", head, fp("Bearer "+r.tok))
	} else {
		log.Printf("%s — re-fetched fp %s, retrying", head, fp("Bearer "+r.tok))
	}
	resp.Body.Close()
	req2 := req.Clone(req.Context())
	tn.signAccount(req2.Header, d.acct, r.tok)
	req2.Body = io.NopCloser(bytes.NewReader(d.body))
	req2.ContentLength = int64(len(d.body))
	req2.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(d.body)), nil }
	resp2, err2 := t.base.RoundTrip(req2)
	if err2 != nil || resp2 == nil || resp2.StatusCode != 401 {
		return resp2, err2 // the retry's answer, a 429 included, is the hop's
	}
	_, msg2 := errBodyTypeMsg(bufferResp(resp2))
	if msg2 == "" {
		msg2 = msg
	}
	if len(msg2) > 300 {
		msg2 = msg2[:300] + "…"
	}
	resp2.Body.Close()
	tn.noteRetryRefused(d.acct, r.tok, msg2)
	log.Printf("upstream 401 %s on the retry: %s (fp %s, fetched just now) — answered 403", d.acct, tn.logProviderText("", typ, msg2), fp("Bearer "+r.tok))
	return reauthRefusedResponse(req2, d.acct, msg2), nil
}

// reauthRefusedResponse is the proxy's own words when Anthropic refuses an
// account's sign-in and no fresher copy serves: a transport-level 403
// permission_error, minted like unavailableResponse. Not a 401, which Claude
// Code reads as its OWN login being bad (precedent: the anonymous-routed 403,
// https_proxy_tls.go). The message's "switch proxy: " prefix marks it as the
// proxy's own words, which a client's probe matches.
// reauthSeeWhere ends that message: how the owner re-signs the account. The
// shared edition's is the brain image's accounts command (the brain serves no
// page); an edition with its own accounts page names it from its init().
var reauthSeeWhere = defaultReauthSeeWhere

const defaultReauthSeeWhere = "Sign it in again: docker compose exec brain accounts sign-in claude <account>."

func reauthRefusedResponse(req *http.Request, acct, msg string) *http.Response {
	if msg == "" {
		msg = "authentication_error"
	}
	text := "switch proxy: Anthropic refused the proxy's sign-in for " + acct + " (\"" + msg + "\"). " +
		"Your laptop login is not used here; /login cannot fix it. " + reauthSeeWhere
	body, _ := json.Marshal(map[string]any{
		"type":  "error",
		"error": map[string]any{"type": "permission_error", "message": text},
	})
	hd := http.Header{}
	hd.Set("Content-Type", "application/json")
	return &http.Response{
		StatusCode: http.StatusForbidden, Status: "403 Forbidden",
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: hd, Body: io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)), Request: req,
	}
}

// shortDur renders a token's time left as the log reads it: 49m, 1h05m.
func shortDur(d time.Duration) string {
	d = d.Round(time.Minute)
	h, m := int(d/time.Hour), int(d%time.Hour/time.Minute)
	if h > 0 {
		return fmt.Sprintf("%dh%02dm", h, m)
	}
	return fmt.Sprintf("%dm", m)
}

// oauthBetaFlag is the anthropic-beta capability Anthropic requires alongside an
// OAuth (subscription-login) bearer. Claude Code normally sends it, but a client
// running in ANTHROPIC_AUTH_TOKEN mode drops it — it believes it is on API-key
// auth — so every site that injects an account OAuth token re-adds it.
const oauthBetaFlag = "oauth-2025-04-20"

// ensureOAuthBeta makes h's anthropic-beta header include oauthBetaFlag,
// preserving the client's other flags. Idempotent.
func ensureOAuthBeta(h http.Header) {
	cur := h.Get("anthropic-beta")
	if cur == "" {
		h.Set("anthropic-beta", oauthBetaFlag)
		return
	}
	for _, f := range strings.Split(cur, ",") {
		if strings.TrimSpace(f) == oauthBetaFlag {
			return
		}
	}
	h.Set("anthropic-beta", cur+","+oauthBetaFlag)
}
