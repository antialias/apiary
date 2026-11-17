package main

// A sign-in rotation, end to end: a sign-in elsewhere refreshes
// an account early, Anthropic revokes the copy the proxy holds, and the next
// request on it must recover inside itself — one re-fetch from the brain (told
// which copy was refused), one replay of the body it dialled — at every site
// that dials an Anthropic account. When the brain has nothing fresher the
// client gets the proxy's own 403, never a 401 its /login would answer.

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

const revokedMsg = "OAuth access token has been revoked."

// e2eKimi401 is a kimi refusal in the same authentication_error shape: only
// the hop's host tells it from an Anthropic account's.
const e2eKimi401 = `{"type":"error","error":{"type":"authentication_error","message":"Invalid Authentication"}}`

// reauthLine is the start of the one log line a refusal of account's copy tok
// writes.
func reauthLine(account, tok string) string {
	return `upstream 401 ` + account + `: "` + revokedMsg + `" (fp ` + fp("Bearer "+tok) + `, fetched `
}

// wantRefetch asserts the brain was asked once, told the refused copy.
func wantRefetch(t *testing.T, env *e2eEnv, refused string) {
	t.Helper()
	if n := env.refusedCalls.Load(); n != 1 {
		t.Fatalf("/token calls carrying refused_fp = %d, want 1", n)
	}
	if got, want := env.lastRefusedFP.Load().(string), fp("Bearer "+refused); got != want {
		t.Fatalf("refused_fp = %q, want %q (the refused copy's fp)", got, want)
	}
}

// statusTokens reads /_switch/status's anthropic_tokens and checks that
// tokens_cached_ttl kept its record<string,string> shape.
func statusTokens(t *testing.T, env *e2eEnv) map[string]map[string]any {
	t.Helper()
	var st struct {
		Cached map[string]string         `json:"tokens_cached_ttl"`
		Tokens map[string]map[string]any `json:"anthropic_tokens"`
	}
	if err := json.Unmarshal(statusMine(t, env.proxy.URL), &st); err != nil {
		t.Fatalf("status: %v (tokens_cached_ttl must stay record<string,string>)", err)
	}
	if len(st.Cached) == 0 {
		t.Fatal("tokens_cached_ttl is empty")
	}
	return st.Tokens
}

func readAll(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return b
}

// The headline: a rotated copy on the direct lane's first hop. One re-fetch
// with refused_fp, the identical body replayed on the fresh copy, a 200 to the
// client, the line in the log, the refusal on /_switch/status — and the next
// request rides the fresh copy without asking again.
func TestE2EReauthDirectFirstHop(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	logs := captureLogSync(t)
	old := e2eToken(other)
	env.rotate(other)
	fresh := env.brainCopy(other)

	resp := env.post(t, "claude-fable-5", "CLIENT")
	if b := readAll(t, resp); resp.StatusCode != 200 {
		t.Fatalf("status %d: %s, want the retried 200", resp.StatusCode, b)
	}
	first, second := env.hit(t), env.hit(t)
	if first.auth != "Bearer "+old || second.auth != "Bearer "+fresh {
		t.Fatalf("bearers = %q -> %q, want the revoked copy then the fresh one", first.auth, second.auth)
	}
	if string(first.body) != string(second.body) {
		t.Fatal("the retry must replay the identical body")
	}
	wantRefetch(t, env, old)
	line := reauthLine(other, old)
	if l := logs.String(); !strings.Contains(l, line) || !strings.Contains(l, "— re-fetched fp "+fp("Bearer "+fresh)+", retrying") {
		t.Fatalf("log lacks the reauth line %q…re-fetched:\n%s", line, l)
	}

	row := statusTokens(t, env)[other]
	last, _ := row["last_refused"].(map[string]any)
	if row["fp"] != fp("Bearer "+fresh) || row["reauths"] != float64(1) || last == nil ||
		last["fp"] != fp("Bearer "+old) || last["msg"] != revokedMsg {
		t.Fatalf("anthropic_tokens[%s] = %v", other, row)
	}
	for _, k := range []string{"fetched_at", "expires_at"} {
		if _, err := time.Parse(time.RFC3339, row[k].(string)); err != nil {
			t.Fatalf("%s = %v: %v", k, row[k], err)
		}
	}
	if _, err := time.Parse(time.RFC3339, last["at"].(string)); err != nil {
		t.Fatalf("last_refused.at = %v: %v", last["at"], err)
	}
	if statusTokens(t, env)[native]["last_refused"] != nil {
		t.Fatalf("%s was never refused", native)
	}

	resp = env.post(t, "claude-fable-5", "CLIENT")
	readAll(t, resp)
	if h := env.hit(t); resp.StatusCode != 200 || h.auth != "Bearer "+fresh {
		t.Fatalf("next request = %d on %q, want 200 on the fresh copy", resp.StatusCode, h.auth)
	}
	if n := env.refusedCalls.Load(); n != 1 {
		t.Fatalf("refused_fp calls = %d after a clean request, want still 1", n)
	}
}

// The direct 429 swap dials the OTHER account while reqInfo still names the
// first: the 401 there must re-fetch the swapped account's copy, and the
// retry's answer is the request's.
func TestE2EReauthDirect429Swap(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	logs := captureLogSync(t)
	env.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"You have exceeded your weekly usage limit."}}`)
	env.deny429.Store(e2eToken(other))
	env.rotate(native)

	resp := env.post(t, "claude-fable-5", "CLIENT")
	if b := readAll(t, resp); resp.StatusCode != 200 {
		t.Fatalf("status %d: %s, want the swapped account's retried 200", resp.StatusCode, b)
	}
	hits := []e2eHit{env.hit(t), env.hit(t), env.hit(t)}
	want := []string{e2eToken(other), e2eToken(native), env.brainCopy(native)}
	for i, h := range hits {
		if h.auth != "Bearer "+want[i] {
			t.Fatalf("hit %d on %q, want %q (429, then the swap's 401, then its retry)", i, h.auth, want[i])
		}
	}
	if string(hits[1].body) != string(hits[2].body) {
		t.Fatal("the retry must replay the swap's body")
	}
	wantRefetch(t, env, e2eToken(native))
	if !strings.Contains(logs.String(), reauthLine(native, e2eToken(native))) {
		t.Fatalf("the reauth line must name the swapped account:\n%s", logs.String())
	}
	if statusTokens(t, env)[other]["last_refused"] != nil {
		t.Fatalf("%s 429'd; it was never refused a sign-in", other)
	}
}

// A 429 on the reauth's retry is the hop's answer, so the direct path still
// walks it to the other account.
func TestE2EReauthRetry429StillWalks(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	env.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"You have exceeded your weekly usage limit."}}`)
	env.rotate(other)
	env.deny429.Store(env.brainCopy(other))

	resp := env.post(t, "claude-fable-5", "CLIENT")
	if b := readAll(t, resp); resp.StatusCode != 200 {
		t.Fatalf("status %d: %s, want the walk's 200", resp.StatusCode, b)
	}
	hits := []e2eHit{env.hit(t), env.hit(t), env.hit(t)}
	want := []string{e2eToken(other), env.brainCopy(other), e2eToken(native)}
	for i, h := range hits {
		if h.auth != "Bearer "+want[i] {
			t.Fatalf("hit %d on %q, want %q (401, the retry's 429, the swap)", i, h.auth, want[i])
		}
	}
}

// A class request's first hop on its anthropic member.
func TestE2EReauthClassFirstHop(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	logs := captureLogSync(t)
	h.rotate(other)

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-reauth-first")
	if b := readAll(t, resp); resp.StatusCode != 200 {
		t.Fatalf("status %d: %s, want the retried 200", resp.StatusCode, b)
	}
	first, second := h.hit(t), h.hit(t)
	if first.auth != "Bearer "+e2eToken(other) || second.auth != "Bearer "+h.brainCopy(other) ||
		second.model != "claude-fable-5" || string(first.body) != string(second.body) {
		t.Fatalf("hits = %q %s -> %q %s, want the member's body replayed on the fresh copy",
			first.auth, first.model, second.auth, second.model)
	}
	wantRefetch(t, h.e2eEnv, e2eToken(other))
	if !strings.Contains(logs.String(), reauthLine(other, e2eToken(other))) {
		t.Fatalf("log lacks the reauth line:\n%s", logs.String())
	}
	select {
	case hit := <-h.hosted:
		t.Fatalf("a reauth is not a member cross: %+v", hit)
	default:
	}
}

// The class same-member swap dials the member's other account.
func TestE2EReauthClassSameMemberSwap(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	logs := captureLogSync(t)
	h.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"You have exceeded your weekly usage limit."}}`)
	h.deny429.Store(e2eToken(other))
	h.rotate(native)

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-reauth-swap")
	if b := readAll(t, resp); resp.StatusCode != 200 {
		t.Fatalf("status %d: %s, want the swapped account's retried 200", resp.StatusCode, b)
	}
	hits := []e2eHit{h.hit(t), h.hit(t), h.hit(t)}
	want := []string{e2eToken(other), e2eToken(native), h.brainCopy(native)}
	for i, hit := range hits {
		if hit.auth != "Bearer "+want[i] || hit.model != "claude-fable-5" {
			t.Fatalf("hit %d = %q %s, want %q on the member", i, hit.auth, hit.model, want[i])
		}
	}
	wantRefetch(t, h.e2eEnv, e2eToken(native))
	if !strings.Contains(logs.String(), reauthLine(native, e2eToken(native))) {
		t.Fatalf("the reauth line must name the swapped account:\n%s", logs.String())
	}
	select {
	case hit := <-h.hosted:
		t.Fatalf("the swap's retry served; nothing may cross: %+v", hit)
	default:
	}
}

// The cross from a provider member onto the anthropic member: reqInfo is
// kimi's there, so only the walk's stamp names the account and body.
func TestE2EReauthClassCross(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	logs := captureLogSync(t)
	h.bindTo.Store("kimi/kimi-k3")
	h.deny.Store(true)
	h.rotate(other)

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-reauth-cross")
	if b := readAll(t, resp); resp.StatusCode != 200 {
		t.Fatalf("status %d: %s, want the crossed, retried 200", resp.StatusCode, b)
	}
	if first := h.hostedHit(t); first.model != "kimi-k3" {
		t.Fatalf("attempt 1 = %+v, want kimi", first)
	}
	cross, retry := h.hit(t), h.hit(t)
	if cross.auth != "Bearer "+e2eToken(other) || retry.auth != "Bearer "+h.brainCopy(other) ||
		retry.model != "claude-fable-5" || string(cross.body) != string(retry.body) {
		t.Fatalf("cross %q %s -> retry %q %s, want the member's body replayed on the fresh copy",
			cross.auth, cross.model, retry.auth, retry.model)
	}
	wantRefetch(t, h.e2eEnv, e2eToken(other))
	if !strings.Contains(logs.String(), reauthLine(other, e2eToken(other))) {
		t.Fatalf("log lacks the reauth line:\n%s", logs.String())
	}
}

// A burst of 401s on the one revoked copy makes ONE brain fetch: the first
// holds the account's reauth while the brain is slow, the rest find the copy
// already replaced and retry on it.
func TestE2EReauthBurstFetchesOnce(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	logs := captureLogSync(t)
	gate := make(chan struct{})
	env.brainMu.Lock()
	env.tokGate = gate
	env.brainMu.Unlock()
	env.rotate(other)

	stop := make(chan struct{})
	var drained sync.WaitGroup
	drained.Add(1)
	go func() { // 40 arrivals overflow seen's buffer
		defer drained.Done()
		for {
			select {
			case <-env.seen:
			case <-stop:
				return
			}
		}
	}()
	const n = 20
	codes := make(chan int, n)
	var wg sync.WaitGroup
	body := `{"model":"claude-fable-5","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`
	for range n {
		wg.Add(1)
		go func() { // no t.Fatal off the test goroutine: a failure is a 0 code
			defer wg.Done()
			req, _ := http.NewRequest("POST", env.proxy.URL+"/v1/messages", strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer CLIENT")
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("anthropic-beta", "oauth-2025-04-20")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				codes <- 0
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			codes <- resp.StatusCode
		}()
	}
	deadline := time.Now().Add(10 * time.Second)
	for env.revokedHits.Load() < n || env.refusedCalls.Load() < 1 {
		if time.Now().After(deadline) {
			close(gate)
			t.Fatalf("401s = %d, refused_fp calls = %d before the gate opened", env.revokedHits.Load(), env.refusedCalls.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(gate)
	wg.Wait()
	close(stop)
	drained.Wait()
	close(codes)
	for c := range codes {
		if c != 200 {
			t.Fatalf("a burst request got %d, want every one retried to 200", c)
		}
	}
	if got := env.refusedCalls.Load(); got != 1 {
		t.Fatalf("refused_fp /token calls = %d for %d concurrent 401s, want 1", got, n)
	}
	l := logs.String()
	if a, b := strings.Count(l, "— re-fetched fp "), strings.Count(l, "— already re-fetched fp "); a != 1 || b != n-1 {
		t.Fatalf("log: %d re-fetched + %d already re-fetched lines, want 1 + %d:\n%s", a, b, n-1, l)
	}
	if r := statusTokens(t, env)[other]["reauths"]; r != float64(1) {
		t.Fatalf("reauths = %v, want 1", r)
	}
}

// A brain that hands back the refused copy (the grant itself is dead): no
// retry loop, the proxy's own 403 naming the account, the brain not asked
// again for 30s — and the cache never emptied.
func TestE2EReauthStuckBrainAnswers403AndQuiets(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	logs := captureLogSync(t)
	env.tokStuck.Store(true)
	env.rotate(other)
	dead := e2eToken(other)

	want := "switch proxy: Anthropic refused the proxy's sign-in for " + other + ` ("` + revokedMsg + `"). ` +
		"Your laptop login is not used here; /login cannot fix it. " + reauthSeeWhere
	post := func() {
		t.Helper()
		resp := env.post(t, "claude-fable-5", "CLIENT")
		b := readAll(t, resp)
		var e struct {
			Type  string `json:"type"`
			Error struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if resp.StatusCode != 403 || json.Unmarshal(b, &e) != nil || e.Type != "error" ||
			e.Error.Type != "permission_error" || e.Error.Message != want {
			t.Fatalf("reply = %d %s, want the proxy's 403 permission_error %q", resp.StatusCode, b, want)
		}
		if h := env.hit(t); h.auth != "Bearer "+dead {
			t.Fatalf("hit on %q", h.auth)
		}
		select {
		case h := <-env.seen:
			t.Fatalf("a same-token reauth must not retry: %+v", h)
		case <-time.After(50 * time.Millisecond):
		}
	}

	post()
	wantRefetch(t, env, dead)
	if l := logs.String(); !strings.Contains(l, reauthLine(other, dead)) ||
		!strings.Contains(l, "— brain returned the same token; answered 403") {
		t.Fatalf("log lacks the same-token line:\n%s", l)
	}

	post() // inside the 30s: the brain is not asked
	if n := env.refusedCalls.Load(); n != 1 {
		t.Fatalf("refused_fp calls = %d inside the quiet, want 1", n)
	}
	if !strings.Contains(logs.String(), "not re-fetched within 30s") {
		t.Fatalf("log lacks the quiet line:\n%s", logs.String())
	}

	lan.tokMu.Lock()
	e := lan.tokens[other]
	if e.token != dead {
		lan.tokMu.Unlock()
		t.Fatalf("cached copy = %q, want the refused one kept (never emptied)", e.token)
	}
	e.quietUntil = time.Now().Add(-time.Second) // the 30s are up
	lan.tokens[other] = e
	lan.tokMu.Unlock()
	post()
	if n := env.refusedCalls.Load(); n != 2 {
		t.Fatalf("refused_fp calls = %d after the quiet, want 2", n)
	}
	row := statusTokens(t, env)[other]
	if last, _ := row["last_refused"].(map[string]any); row["reauths"] != float64(2) || last["fp"] != fp("Bearer "+dead) {
		t.Fatalf("anthropic_tokens[%s] = %v", other, row)
	}
}

// A brain that fails the re-fetch (a restart, a slow usage check) quiets the
// account only for reauthFailQuiet, not reauthQuiet: once the brain answers
// again, the next request after that short quiet recovers on the fresh copy.
func TestE2EReauthFailedFetchQuietsBriefly(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	logs := captureLogSync(t)
	old := e2eToken(other)
	env.rotate(other)
	env.tokFail.Store(true)

	resp := env.post(t, "claude-fable-5", "CLIENT")
	if b := readAll(t, resp); resp.StatusCode != 403 {
		t.Fatalf("status %d: %s, want the proxy's 403 while the brain fails", resp.StatusCode, b)
	}
	env.hit(t)
	if !strings.Contains(logs.String(), reauthLine(other, old)) || !strings.Contains(logs.String(), "— re-fetch failed (") {
		t.Fatalf("log lacks the failed re-fetch line:\n%s", logs.String())
	}
	lan.tokMu.Lock()
	e := lan.tokens[other]
	lan.tokMu.Unlock()
	if left := time.Until(e.quietUntil); e.quietFor != reauthFailQuiet || left <= 0 || left > reauthFailQuiet {
		t.Fatalf("quiet = %v (%v left), want reauthFailQuiet (%v), not reauthQuiet", e.quietFor, left, reauthFailQuiet)
	}

	env.tokFail.Store(false) // the brain is back
	lan.tokMu.Lock()
	e = lan.tokens[other]
	e.quietUntil = time.Now().Add(-time.Millisecond) // the short quiet is up
	lan.tokens[other] = e
	lan.tokMu.Unlock()
	resp = env.post(t, "claude-fable-5", "CLIENT")
	if b := readAll(t, resp); resp.StatusCode != 200 {
		t.Fatalf("status %d: %s, want 200 once the brain answers again", resp.StatusCode, b)
	}
	if first, second := env.hit(t), env.hit(t); first.auth != "Bearer "+old || second.auth != "Bearer "+env.brainCopy(other) {
		t.Fatalf("bearers = %q -> %q, want the revoked copy then the fresh one", first.auth, second.auth)
	}
	if n := env.refusedCalls.Load(); n != 2 {
		t.Fatalf("refused_fp calls = %d, want 2 (the failed one and the recovery)", n)
	}
}

// A fresh copy refused at once: the 403, and the account quiet, so the next
// request does not ask again.
func TestE2EReauthFreshCopyAlsoRefused(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	logs := captureLogSync(t)
	env.rotate(other)
	env.revokeCopy(other) // the brain's new copy is dead too
	fresh := env.brainCopy(other)

	resp := env.post(t, "claude-fable-5", "CLIENT")
	b := readAll(t, resp)
	if resp.StatusCode != 403 || !strings.Contains(string(b), `"message":"switch proxy: Anthropic refused the proxy's sign-in for `+other) {
		t.Fatalf("reply = %d %s, want the proxy's 403", resp.StatusCode, b)
	}
	if a, b := env.hit(t), env.hit(t); a.auth != "Bearer "+e2eToken(other) || b.auth != "Bearer "+fresh {
		t.Fatalf("hits %q -> %q, want one retry on the fresh copy", a.auth, b.auth)
	}
	if !strings.Contains(logs.String(), "upstream 401 "+other+` on the retry: "`+revokedMsg+`" (fp `+fp("Bearer "+fresh)) {
		t.Fatalf("log lacks the retry's line:\n%s", logs.String())
	}

	resp = env.post(t, "claude-fable-5", "CLIENT")
	readAll(t, resp)
	env.hit(t)
	if resp.StatusCode != 403 || env.refusedCalls.Load() != 1 {
		t.Fatalf("next = %d with %d refused_fp calls, want 403 and still 1", resp.StatusCode, env.refusedCalls.Load())
	}
}

// A kimi 401 is kimi's: its hop has no wire either, but it is not an
// Anthropic account's copy. It goes back as it came, and the brain is not asked.
func TestE2EReauthKimi401PassesThrough(t *testing.T) {
	h, restore := newE2EClassOn(t, "kimi/kimi-k3", 1.0)
	defer restore()
	h.kimi401.Store(true)

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-reauth-kimi")
	b := readAll(t, resp)
	if resp.StatusCode != 401 || string(b) != e2eKimi401 {
		t.Fatalf("reply = %d %s, want kimi's 401 unchanged", resp.StatusCode, b)
	}
	h.hostedHit(t)
	if n := h.refusedCalls.Load(); n != 0 {
		t.Fatalf("a kimi 401 asked the brain for an Anthropic copy (%d refused_fp calls)", n)
	}
	select {
	case hit := <-h.seen:
		t.Fatalf("a kimi 401 reached Anthropic: %+v", hit)
	default:
	}
	select {
	case hit := <-h.hosted:
		t.Fatalf("a kimi 401 must not retry: %+v", hit)
	default:
	}
}

// The manual inject file's token has no brain copy to fetch: its 401 goes
// back as Anthropic sent it.
func TestE2EReauthManualInject401PassesThrough(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	injectToken(t, e2eToken(native))
	env.revokeCopy(native)

	resp := env.post(t, "claude-haiku-4-5", "CLIENT")
	b := readAll(t, resp)
	if resp.StatusCode != 401 || string(b) != e2eRevoked401 {
		t.Fatalf("reply = %d %s, want Anthropic's 401 unchanged", resp.StatusCode, b)
	}
	if h := env.hit(t); h.auth != "Bearer "+e2eToken(native) {
		t.Fatalf("hit on %q, want the inject file's token", h.auth)
	}
	if n := env.refusedCalls.Load(); n != 0 {
		t.Fatalf("a manual: 401 asked the brain (%d refused_fp calls)", n)
	}
	select {
	case h := <-env.seen:
		t.Fatalf("a manual: 401 must not retry: %+v", h)
	case <-time.After(50 * time.Millisecond):
	}
}

// rt401 answers every hop with body as a 401 and counts the hops.
type rt401 struct {
	body string
	hops int
}

func (r *rt401) RoundTrip(req *http.Request) (*http.Response, error) {
	r.hops++
	return &http.Response{StatusCode: 401, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(r.body)), Request: req}, nil
}

// The host guard on its own: a hop off the Anthropic host is never re-fetched,
// even one carrying an account's dial stamp. No e2e path stamps one there
// today; this keeps a future one from asking the brain for an Anthropic copy
// over another upstream's 401.
func TestAnthropicReauthOnlyOnTheAnthropicHost(t *testing.T) {
	anth, _ := url.Parse("https://api.anthropic.com")
	rt := &rt401{body: e2eKimi401}
	tr := &failoverTransport{base: rt, anthropic: anth}
	req, _ := http.NewRequest("POST", "https://kimi.invalid/coding/v1/messages", strings.NewReader("{}"))
	req = req.WithContext(withAnthDial(req.Context(), native, []byte("{}")))
	req.Header.Set("Authorization", "Bearer kimi-secret")
	resp, err := tr.tripWithReauth(req)
	if err != nil {
		t.Fatal(err)
	}
	b := readAll(t, resp)
	if resp.StatusCode != 401 || string(b) != e2eKimi401 || rt.hops != 1 {
		t.Fatalf("reply = %d %s after %d hops, want the 401 as it came after 1", resp.StatusCode, b, rt.hops)
	}
}
