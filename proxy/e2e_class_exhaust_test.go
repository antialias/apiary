package main

// e2e for the honest class-exhaustion reply, an incident
// replayed through the real handler stack.
//
// The class `capable` was lane:opus -> gpt-6.1-sol -> kimi-k3. Every
// Opus account had a rejected window (one 5h, two 7d), Sol had
// said usage_limit_reached for the week, and kimi was not routable. The walk
// surfaced one pooled account's bare 429; the CLI read "not your usage limit",
// and nobody learned when the class would be back. Here the fixture's fable lane
// stands in for opus, gpt-5.6 for Sol.

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

const ghost = "ghost@example.com"

type exhaustScenario struct {
	h          *e2eHosted
	t1, t2, t3 time.Time // other 5h, native 7d, ghost 7d (brain only)
}

// incidentShape builds the incident's class: an anthropic member whose three
// accounts are each spent on a different window, a provider member that will
// refuse on an explicit weekly quota, and a provider member the brain has never
// measured (its check: no, unknown).
func incidentShape(t *testing.T, h *e2eHosted) exhaustScenario {
	t.Helper()
	now := time.Now().Truncate(time.Second)
	sc := exhaustScenario{h: h, t1: now.Add(4 * time.Hour), t2: now.Add(48 * time.Hour), t3: now.Add(72 * time.Hour)}
	h.brainMu.Lock()
	fable, gpt, kimi := h.classMembers[0], h.classMembers[1], h.classMembers[2]
	gpt["usable"], gpt["effective"] = true, 20.0
	h.classMembers = []map[string]any{fable, gpt, kimi}
	// The brain still points the lane at other (its effective ignores the
	// 5h window); native has headroom on paper; ghost is at 100% weekly.
	h.lanes["fable"] = map[string]any{"account": other, "reason": "headroom",
		"effective": map[string]float64{other: 82, native: 60, ghost: 100}}
	old := float64(now.Add(-10 * time.Minute).Unix())
	h.views = map[string]any{
		ghost: map[string]any{"kind": "anthropic_account", "good_ts": old,
			"serves": []string{"fable", "haiku", "default"},
			"windows": map[string]any{
				"five_hour": map[string]any{"percent": 0.0, "resets_at": nil},
				"seven_day": map[string]any{"percent": 100.0, "resets_at": sc.t3.Unix()},
			}},
		"kimi": map[string]any{"kind": "provider", "good_ts": nil, "windows": map[string]any{}},
	}
	h.brainMu.Unlock()
	h.deny429.Store("*")
	h.body429.Store(`{"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later."}}`)
	h.anthHeadersFor.Store(map[string]map[string]string{
		e2eToken(other): {
			"anthropic-ratelimit-unified-status":               "rejected",
			"anthropic-ratelimit-unified-reset":                strconv.FormatInt(sc.t1.Unix(), 10),
			"anthropic-ratelimit-unified-representative-claim": "five_hour",
			"anthropic-ratelimit-unified-5h-status":            "rejected",
			"anthropic-ratelimit-unified-5h-reset":             strconv.FormatInt(sc.t1.Unix(), 10),
			"anthropic-ratelimit-unified-5h-utilization":       "1.0",
			"anthropic-ratelimit-unified-7d-status":            "allowed",
			"anthropic-ratelimit-unified-7d-utilization":       "0.82",
			"anthropic-ratelimit-unified-7d-reset":             strconv.FormatInt(now.Add(100*time.Hour).Unix(), 10),
		},
		e2eToken(native): {
			"anthropic-ratelimit-unified-status":               "rejected",
			"anthropic-ratelimit-unified-reset":                strconv.FormatInt(sc.t2.Unix(), 10),
			"anthropic-ratelimit-unified-representative-claim": "seven_day",
			"anthropic-ratelimit-unified-7d-status":            "rejected",
			"anthropic-ratelimit-unified-7d-reset":             strconv.FormatInt(sc.t2.Unix(), 10),
			"anthropic-ratelimit-unified-7d-utilization":       "1.0",
		},
	})
	h.codexQuota.Store(true) // gpt: 429 usage_limit_reached, resets_at 52 h out
	// kimi has never been measured. The proxy merges standings by as_of, so a
	// reading it holds from the harness's first poll would survive the brain's
	// null: start it from nothing, as on a proxy that never held one.
	lan.pref.mu.Lock()
	delete(lan.pref.accounts, "kimi")
	lan.pref.mu.Unlock()
	fetchPreferredSync()
	return sc
}

// postCC sends a class request the way Claude Code does — its UA is what
// selects the honest reply.
func postCC(t *testing.T, env *e2eEnv, model, session string) *http.Response {
	t.Helper()
	body := `{"model":"` + model + `","max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`
	req, _ := http.NewRequest("POST", env.proxy.URL+"/v1/messages", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer CLIENT")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "claude-cli/2.1.308 (external, cli)")
	req.Header.Set("x-claude-code-session-id", session)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request failed: %v", err)
	}
	return resp
}

func drainAnthropicHits(env *e2eEnv) []e2eHit {
	var out []e2eHit
	for {
		select {
		case h := <-env.seen:
			out = append(out, h)
		default:
			return out
		}
	}
}

func drainCodexHits(h *e2eHosted) int {
	n := 0
	for {
		select {
		case <-h.codexSeen:
			n++
		default:
			return n
		}
	}
}

// assertHonest checks the whole honest reply and returns its message.
func assertHonest(t *testing.T, resp *http.Response, reset time.Time, claim string) string {
	t.Helper()
	return assertHonestAt(t, resp, reset, claim, time.Until(reset).Seconds())
}

// assertHonestAt is assertHonest with the Retry-After expected (seconds, ±10).
func assertHonestAt(t *testing.T, resp *http.Response, reset time.Time, claim string, retry float64) string {
	t.Helper()
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 429 {
		t.Fatalf("status = %d, want 429 (a usage limit, not an overload): %s", resp.StatusCode, raw)
	}
	want := map[string]string{
		"Anthropic-Ratelimit-Unified-Status":               "rejected",
		"Anthropic-Ratelimit-Unified-Reset":                strconv.FormatInt(reset.Unix(), 10),
		"Anthropic-Ratelimit-Unified-Representative-Claim": claim,
		"X-Switch-Quota":                                   "exhausted",
		"X-Switch-Available-At":                            reset.UTC().Format(time.RFC3339),
		"X-Switch-Class":                                   "frontier",
	}
	for k, v := range want {
		if got := resp.Header.Get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	// The CLI latches state off these; none may be minted.
	for k := range resp.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "anthropic-ratelimit-unified-overage") ||
			strings.HasPrefix(lk, "anthropic-ratelimit-unified-fallback") || lk == "x-should-retry" {
			t.Errorf("minted %s; the CLI would persist or latch it", k)
		}
	}
	ra, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
	if float64(ra) < retry-10 || float64(ra) > retry+10 {
		t.Errorf("Retry-After = %d, want ~%.0f (seconds to the class's reset)", ra, retry)
	}
	var env struct {
		Error       struct{ Type, Message string } `json:"error"`
		Unavailable unavailable                    `json:"unavailable"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("body is not an error envelope: %s", raw)
	}
	// The §2.8 body rides the reply, and its time is the headers' time.
	if u := env.Unavailable; u.AvailableAt == nil || int64(*u.AvailableAt) != reset.Unix() ||
		u.RetryAfterS == nil || *u.RetryAfterS != ra || len(u.Members) == 0 {
		t.Errorf("unavailable body = %s, want available_at %d and retry_after_s %d", raw, reset.Unix(), ra)
	}
	if env.Error.Type != "rate_limit_error" {
		t.Errorf("error.type = %q, want rate_limit_error (overloaded_error re-enables CLI retries)", env.Error.Type)
	}
	return env.Error.Message
}

// (1) The incident: the walk that discovers the class is spent answers with the
// class's own reset, naming every member's state; the next request inside the
// window dials nobody.
func TestE2EClassExhaustionHonestReplyForClaudeCode(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	sc := incidentShape(t, h)

	resp := postCC(t, h.e2eEnv, "claude-frontier", "sess-1720")
	msg := assertHonest(t, resp, sc.t1, "five_hour")
	now := time.Now()
	for _, want := range []string{
		"class frontier is unavailable. Back at " + fmtAvail(sc.t1, now) + " (exact).",
		other + " spent (5h) until " + fmtAvail(sc.t1, now),
		native + " spent (7d) until " + fmtAvail(sc.t2, now),
		ghost + " spent (7d) until " + fmtAvail(sc.t3, now),
		"frontier/gpt/gpt-5.6: gpt ",
		"frontier/kimi/kimi-k3: kimi unknown",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("message missing %q:\n%s", want, msg)
		}
	}
	t.Logf("17:20 reply: %s", msg)
	// The statusline's class row (§2.11): unavailable until the earliest return.
	if f := statusFunding(t, h.proxy.URL, "frontier", "sess-1720"); f["unavailable"] != true ||
		f["until"] != sc.t1.UTC().Format(time.RFC3339) {
		t.Fatalf("class_funding = %v, want unavailable until %s", f, sc.t1.UTC().Format(time.RFC3339))
	}

	hits := drainAnthropicHits(h.e2eEnv)
	if len(hits) != 2 || hits[0].auth != "Bearer "+e2eToken(other) || hits[1].auth != "Bearer "+e2eToken(native) {
		t.Fatalf("anthropic hits = %+v, want other then the one same-lane swap to native (ghost is at the hard line)", hits)
	}
	if n := drainCodexHits(h); n != 1 {
		t.Fatalf("codex hits = %d, want the one cross to gpt", n)
	}
	h.noHostedHit(t) // kimi is not routable
	drainClassAlerts(t, h.e2eEnv)

	// --- the churn: a second request inside the window dials nobody ---------
	resp2 := postCC(t, h.e2eEnv, "claude-frontier", "sess-1720")
	assertHonest(t, resp2, sc.t1, "five_hour")
	if hits := drainAnthropicHits(h.e2eEnv); len(hits) != 0 {
		t.Fatalf("second request re-dialed a rejected account: %+v", hits)
	}
	if n := drainCodexHits(h); n != 0 {
		t.Fatalf("second request re-dialed the spent gpt member")
	}
	h.noHostedHit(t)
}

// (2) No probe (§2.2 row 16): a refused account is dialled again only when its
// record ends AND its reading no longer closes it. Past every record here, the
// Anthropic accounts' own 429 headers still read their windows closed until
// their resets, so nothing dials them; gpt's refusal named no window
// (unattributed, keyed on its model), so live traffic re-checks it once.
func TestE2EClassExhaustionRedialsOnlyWhenTheRecordEnds(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	sc := incidentShape(t, h)
	assertHonest(t, postCC(t, h.e2eEnv, "claude-frontier", "sess-probe"), sc.t1, "five_hour")
	drainAnthropicHits(h.e2eEnv)
	drainCodexHits(h)
	drainClassAlerts(t, h.e2eEnv)

	lan.recMu.Lock()
	for k, r := range lan.records {
		r.until = time.Now().Add(-time.Second)
		lan.records[k] = r
	}
	lan.recMu.Unlock()

	assertHonest(t, postCC(t, h.e2eEnv, "claude-frontier", "sess-probe"), sc.t1, "five_hour")
	if hits := drainAnthropicHits(h.e2eEnv); len(hits) != 0 {
		t.Fatalf("re-dialled an account its own reading still closes: %+v", hits)
	}
	if n := drainCodexHits(h); n != 1 {
		t.Fatalf("codex hits = %d, want one re-check of gpt's expired model record", n)
	}
	// The re-check refused again: the record is back, and the next request
	// inside it dials nobody.
	assertHonest(t, postCC(t, h.e2eEnv, "claude-frontier", "sess-probe-2"), sc.t1, "five_hour")
	if hits, n := drainAnthropicHits(h.e2eEnv), drainCodexHits(h); len(hits) != 0 || n != 0 {
		t.Fatalf("a request inside the records dialled: anthropic %+v codex %d", hits, n)
	}
}

// (3) Early recovery, the brain half: the human buys usage, and the next poll —
// fresher than our 429 — shows headroom. The record is cleared and the account
// serves at once, without waiting for its reset.
func TestE2EClassExhaustionBrainRecoveryClearsRecord(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	sc := incidentShape(t, h)
	assertHonest(t, postCC(t, h.e2eEnv, "claude-frontier", "sess-bought"), sc.t1, "five_hour")
	drainAnthropicHits(h.e2eEnv)
	drainCodexHits(h)
	drainClassAlerts(t, h.e2eEnv)

	h.brainMu.Lock()
	h.views[other] = map[string]any{"kind": "anthropic_account",
		"good_ts": float64(time.Now().Add(2 * time.Second).Unix()),
		"serves":  []string{"fable", "haiku", "default"},
		"windows": map[string]any{
			"five_hour": map[string]any{"percent": 4.0, "resets_at": sc.t1.Unix()},
			"seven_day": map[string]any{"percent": 82.0, "resets_at": time.Now().Add(100 * time.Hour).Unix()},
		}}
	h.brainMu.Unlock()
	h.deny429.Store(e2eToken(native)) // other serves again
	fetchPreferredSync()

	if laneNo(other, "fable") {
		t.Fatalf("the brain poll showed headroom but the account's record still stands: %v", recordsOn(other))
	}
	resp := postCC(t, h.e2eEnv, "claude-frontier", "sess-bought")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d after recovery, want 200: %s", resp.StatusCode, b)
	}
	if hit := h.hit(t); hit.auth != "Bearer "+e2eToken(other) {
		t.Fatalf("served on %q, want the recovered other", hit.auth)
	}
}

// (4) The agent gateway contract is a status code: a client that is not Claude Code
// gets the class 529 it already knows — no minted unified headers — in the
// same scenario.
func TestE2EClassExhaustionNonClaudeCodeUnchanged(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	incidentShape(t, h)

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-gateway")
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	// Every member's check is `no` — kimi's for want of any reading (§2.2
	// row 5: never route blind) — so this client gets the class 529, the
	// status it already knows, naming each member's state.
	if resp.StatusCode != 529 || resp.Header.Get("X-Switch-Quota") != "exhausted" {
		t.Fatalf("status %d X-Switch-Quota %q, want the class 529: %s",
			resp.StatusCode, resp.Header.Get("X-Switch-Quota"), raw)
	}
	if !strings.Contains(string(raw), "kimi/kimi-k3: kimi unknown") {
		t.Fatalf("the 529 does not name kimi's state: %s", raw)
	}
	if got := resp.Header.Get("Anthropic-Ratelimit-Unified-Representative-Claim"); got != "" {
		t.Fatalf("minted a usage-limit claim for a non-Claude-Code client: %q", got)
	}
}

// (5) The same class on the OpenAI wire: anthropic members are off this wire
// (D2), so the universe is gpt + kimi. The reply is OpenAI-shaped and carries
// the class's time.
func TestE2EClassExhaustionOpenAIWire(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	resetWireBuckets()
	incidentShape(t, h)
	h.bindTo.Store("gpt/gpt-5.6")

	resp := wirePost(t, h.e2eEnv, `{"model":"frontier","messages":[{"role":"user","content":"hi"}],"max_tokens":16}`, "wire-1720")
	body := wireBody(t, resp)
	if resp.StatusCode != 429 {
		t.Fatalf("status = %d, want 429: %s", resp.StatusCode, body)
	}
	var e struct {
		Error struct{ Message, Type, Code string } `json:"error"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("not OpenAI-shaped: %s", body)
	}
	// A time exists (gpt's reset): the OpenAI wire's retryable reply, not the
	// money one (§2.8 item 4: insufficient_quota reads as "do not retry").
	if e.Error.Type != "requests" || e.Error.Code != "rate_limit_exceeded" {
		t.Errorf("error = %+v, want requests / rate_limit_exceeded", e.Error)
	}
	if resp.Header.Get("x-should-retry") != "" {
		t.Errorf("a retryable reply says x-should-retry %q", resp.Header.Get("x-should-retry"))
	}
	ra, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
	if d := time.Until(time.Now().Add(52 * time.Hour)).Seconds(); float64(ra) < d-30 || float64(ra) > d+30 {
		t.Errorf("Retry-After = %d, want ~%.0f (gpt's reset)", ra, d)
	}
	for _, want := range []string{"frontier/gpt/gpt-5.6: gpt spent (model:gpt-5.6) until", "429 usage_limit_reached",
		"frontier/kimi/kimi-k3: kimi unknown"} {
		if !strings.Contains(e.Error.Message, want) {
			t.Errorf("message missing %q: %s", want, e.Error.Message)
		}
	}
	if strings.Contains(e.Error.Message, "lane:fable") {
		t.Errorf("the wire's message names an anthropic member it can never use: %s", e.Error.Message)
	}
	if got := resp.Header.Get("X-Switch-Quota"); got != "exhausted" {
		t.Errorf("X-Switch-Quota = %q", got)
	}
	at, err := time.Parse(time.RFC3339, resp.Header.Get("X-Switch-Available-At"))
	if err != nil || at.Sub(time.Now()) < 51*time.Hour {
		t.Errorf("X-Switch-Available-At = %q, want gpt's reset ~52h out", resp.Header.Get("X-Switch-Available-At"))
	}
	h.noAnthropicLeak(t)
	drainCodexHits(h)
}

// Re-review: during a class walk, a 2xx is credited to the lane of the hop that
// SERVED, not attempt 1's. The fable member 429s on account A with its Fable cap
// rejected (the captured 7d_oi set); the walk crosses to the haiku member, which
// routes to the same account and serves. That success must not lift the Fable
// cap, and the Fable 429 must not have cooled haiku (C5: scoped by its header).
func TestE2EClassWalkCreditsServedLane(t *testing.T) {
	h, restore := newE2EClass(t)
	defer restore()
	now := time.Now().Truncate(time.Second)
	reset := now.Add(48 * time.Hour)
	h.brainMu.Lock()
	h.classMembers = []map[string]any{
		{"id": "lane:fable", "priority": 1, "kind": "anthropic", "lane": "fable",
			"model": "claude-fable-5", "context_window": 200000, "effective": 20.0},
		{"id": "lane:haiku", "priority": 2, "kind": "anthropic", "lane": "haiku",
			"model": "claude-haiku-5", "context_window": 200000, "effective": 20.0},
	}
	// The other account is spent, so neither member can swap accounts: both
	// lanes land on native. native's poll publishes its Fable window with the
	// aliases the headers will use.
	h.lanes["fable"] = map[string]any{"account": native, "reason": "headroom",
		"effective": map[string]float64{native: 20, other: 100}}
	h.views = map[string]any{
		other: map[string]any{"kind": "anthropic_account",
			"good_ts": float64(now.Add(-time.Minute).Unix()),
			"windows": map[string]any{"seven_day": map[string]any{"percent": 100.0, "resets_at": now.Add(72 * time.Hour).Unix()}}},
		native: map[string]any{"kind": "anthropic_account",
			"good_ts": float64(now.Add(-time.Minute).Unix()),
			"windows": map[string]any{
				"seven_day": map[string]any{"percent": 89.0, "resets_at": reset.Unix()},
				"scoped:Fable": map[string]any{"percent": 99.0, "resets_at": reset.Unix(),
					"aliases": map[string]any{"header": []string{"7d_oi"}, "claim": []string{"seven_day_overage_included"}}}}},
	}
	h.brainMu.Unlock()
	h.body429.Store(fable429Body)
	hs := map[string]string{}
	for k, vs := range capturedFable429(capture2239, reset) {
		hs[k] = vs[0]
	}
	h.denyModel.Store(e2eDenyModel{model: "fable", headers: hs})
	fetchPreferredSync()

	resp := h.postSess(t, "claude-frontier", "CLIENT", "sess-xlane")
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want the haiku member's 200: %s", resp.StatusCode, raw)
	}
	hits := drainAnthropicHits(h.e2eEnv)
	if len(hits) != 2 || hits[0].model != "claude-fable-5" || hits[1].model != "claude-haiku-5" ||
		hits[0].auth != "Bearer "+e2eToken(native) || hits[1].auth != "Bearer "+e2eToken(native) {
		t.Fatalf("hits = %+v, want fable then haiku, both on native", hits)
	}
	if !laneNo(native, "fable") || laneNo(native, "haiku") || laneNo(native, "default") {
		t.Fatal("the Fable 429 must close native's Fable scope and nothing else")
	}
	later := time.Now().Add(quotaCooldown + time.Second) // past the record floor
	if c, _ := lan.routeCheck("lane:fable", native, "claude-fable-5", later); !c.no() || !c.Until.Time().Equal(reset) {
		t.Fatalf("fable on native after a haiku 2xx = %s, want still refused by its Fable cap until %v", c, reset)
	}
	if c, _ := lan.routeCheck("lane:haiku", native, "claude-haiku-5", later); c.no() {
		t.Fatalf("haiku on native = %s, want admitted", c)
	}
}
