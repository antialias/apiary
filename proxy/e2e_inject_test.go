package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func enqueueE2E(t *testing.T, env *e2eEnv, bearer, sessionKey, text string) *http.Response {
	t.Helper()
	b, _ := json.Marshal(map[string]string{"tenant": testLanTenant, "session_key": sessionKey, "text": text})
	req, _ := http.NewRequest("POST", env.proxy.URL+"/_switch/inject", bytes.NewReader(b))
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("enqueue request failed: %v", err)
	}
	return resp
}

func TestE2EInjectIsGatedAndRejectsUnobservedSessions(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	withClientSecret(t, env, "sekrit")

	key := injectSessionKey("never-observed")
	resp := enqueueE2E(t, env, "", key, "hello")
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated enqueue got %d, want 401", resp.StatusCode)
	}
	resp = enqueueE2E(t, env, "sekrit", key, "hello")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unobserved session enqueue got %d, want 404", resp.StatusCode)
	}
	var result struct {
		Error injectError `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&result)
	if result.Error.Type != "unknown_session" {
		t.Fatalf("typed error = %#v", result.Error)
	}
}

func TestE2EInjectQueuesThenChangesOnlyTheTargetSessionsNextTurn(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	withClientSecret(t, env, "sekrit")

	// A real subscriber arms tapTurn. The first request creates the browser-safe session
	// handle exactly as production does; there is no test-only registration path.
	_, frames, closeStream := evStream(t, env, "sekrit", "")
	defer closeStream()
	evHello(t, frames)

	const target = "target-session"
	resp := env.postSess(t, "claude-fable-5", "sekrit", target)
	resp.Body.Close()
	firstHit := env.hit(t)
	turn := evAwait(t, frames, "turn", 3*time.Second)
	data, _ := turn.env.Data.(map[string]any)
	key, _ := data["session_key"].(string)
	if key != injectSessionKey(target) || len(key) != 64 {
		t.Fatalf("turn session_key = %q, want opaque full key", key)
	}
	if strings.Contains(string(turn.raw), target) {
		t.Fatal("raw session id leaked into the browser event")
	}

	queued := enqueueE2E(t, env, "sekrit", key, "inspect the deployment state")
	defer queued.Body.Close()
	if queued.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(queued.Body)
		t.Fatalf("enqueue got %d: %s", queued.StatusCode, b)
	}
	qev := evAwait(t, frames, "instruction_queued", 3*time.Second)
	qdata, _ := qev.env.Data.(map[string]any)
	if qdata["session_key"] != key || qdata["depth"] != float64(1) {
		t.Fatalf("queue event = %v", qdata)
	}

	// Another session remains byte-identical at the upstream. This also exercises the
	// empty-queue fast path through the complete handler stack.
	const otherSession = "other-session"
	resp = env.postSess(t, "claude-fable-5", "sekrit", otherSession)
	resp.Body.Close()
	otherHit := env.hit(t)
	if !bytes.Equal(otherHit.body, firstHit.body) {
		t.Fatalf("untargeted session body changed:\n got %s\nwant %s", otherHit.body, firstHit.body)
	}

	resp = env.postSess(t, "claude-fable-5", "sekrit", target)
	resp.Body.Close()
	targetHit := env.hit(t)
	var request struct {
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(targetHit.body, &request); err != nil {
		t.Fatal(err)
	}
	if len(request.Messages) != 2 || request.Messages[1].Role != "user" ||
		!bytes.Contains(request.Messages[1].Content, []byte("inspect the deployment state")) {
		t.Fatalf("upstream did not receive one appended user turn: %s", targetHit.body)
	}
	dev := evAwait(t, frames, "instruction_delivered", 3*time.Second)
	dd, _ := dev.env.Data.(map[string]any)
	if dd["session_key"] != key || dd["items"] != float64(1) {
		t.Fatalf("delivery event = %v", dd)
	}

	// Drained once: the following request is the exact fixture body again.
	resp = env.postSess(t, "claude-fable-5", "sekrit", target)
	resp.Body.Close()
	after := env.hit(t)
	if !bytes.Equal(after.body, firstHit.body) {
		t.Fatalf("drained queue changed a later request:\n got %s\nwant %s", after.body, firstHit.body)
	}
}

func TestE2EInjectWorksForPrintModeShapeWithoutInteractiveTools(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	const session = "print-session"
	key := lan.injectObserve(session, time.Now())
	if _, _, problem := lan.injectEnqueue(key, "continue headlessly", time.Now()); problem != nil {
		t.Fatal(problem)
	}
	body := `{"model":"claude-fable-5","max_tokens":16,"tools":[],"messages":[{"role":"user","content":"print"}]}`
	req, _ := http.NewRequest("POST", env.proxy.URL+"/v1/messages", strings.NewReader(body))
	req.Header.Set("x-claude-code-session-id", session)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	hit := env.hit(t)
	if !bytes.Contains(hit.body, []byte("continue headlessly")) {
		t.Fatalf("print-shaped request was not injected: %s", hit.body)
	}
}
