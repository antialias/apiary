package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func withInjectWorld(t *testing.T) {
	t.Helper()
	injectMu.Lock()
	oldSessions := injectSessions
	injectSessions = map[string]*injectSession{}
	injectMu.Unlock()
	t.Cleanup(func() {
		injectMu.Lock()
		injectSessions = oldSessions
		injectMu.Unlock()
	})
}

func TestInjectEmptyQueueIsByteIdenticalPassthrough(t *testing.T) {
	withInjectWorld(t)
	body := []byte(" {\n  \"model\": \"claude-fable-5\", \"messages\" : [ {\"role\":\"user\",\"content\":\"hi\"} ]\n}\n")
	before := append([]byte(nil), body...)
	got, reservation := injectRequest(body, "session-a")
	if reservation != nil {
		t.Fatal("empty queue created a reservation")
	}
	if !bytes.Equal(got, before) {
		t.Fatalf("empty queue changed the request:\n got %q\nwant %q", got, before)
	}
	if len(got) > 0 && &got[0] != &body[0] {
		t.Fatal("empty queue copied the body; cold path must return the original slice")
	}
}

func TestInjectAppendsOneVisibleUserTurnInFIFOOrder(t *testing.T) {
	withInjectWorld(t)
	const session = "session-a"
	key := injectObserve(session, time.Now())
	first, _, problem := injectEnqueue(key, "first instruction", time.Now())
	if problem != nil {
		t.Fatal(problem)
	}
	second, _, problem := injectEnqueue(key, "second instruction", time.Now())
	if problem != nil {
		t.Fatal(problem)
	}
	body := []byte(`{"model":"claude-fable-5","messages":[{"role":"user","content":"original"}],"stream":true}`)
	got, reservation := injectRequest(body, session)
	if reservation == nil || len(reservation.items) != 2 || reservation.items[0].ID != first.ID || reservation.items[1].ID != second.ID {
		t.Fatalf("reserved items = %#v, want FIFO %s, %s", reservation, first.ID, second.ID)
	}
	var request struct {
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(got, &request); err != nil {
		t.Fatalf("injected body is invalid JSON: %v\n%s", err, got)
	}
	if len(request.Messages) != 2 || request.Messages[1].Role != "user" {
		t.Fatalf("messages = %#v, want original + one synthetic user turn", request.Messages)
	}
	var blocks []struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(request.Messages[1].Content, &blocks); err != nil || len(blocks) != 1 {
		t.Fatalf("synthetic content = %s, err=%v", request.Messages[1].Content, err)
	}
	text := blocks[0].Text
	if !strings.HasPrefix(text, injectPreamble) {
		t.Fatalf("synthetic turn does not announce itself: %q", text)
	}
	if strings.Index(text, "first instruction") > strings.Index(text, "second instruction") {
		t.Fatalf("instructions are not FIFO: %q", text)
	}
	if concurrent, secondReservation := injectRequest(body, session); secondReservation != nil || !bytes.Equal(concurrent, body) {
		t.Fatal("a concurrent request stole an in-flight reservation")
	}
	finishInject(reservation, true)
	if again, laterReservation := injectRequest(body, session); laterReservation != nil || !bytes.Equal(again, body) {
		t.Fatal("queue was not committed exactly once")
	}
}

func TestInjectMalformedRequestDoesNotDrainQueue(t *testing.T) {
	withInjectWorld(t)
	const session = "session-a"
	key := injectObserve(session, time.Now())
	if _, _, problem := injectEnqueue(key, "keep me", time.Now()); problem != nil {
		t.Fatal(problem)
	}
	bad := []byte(`{"model":"claude-fable-5","messages":"not-an-array"}`)
	got, reservation := injectRequest(bad, session)
	if reservation != nil || !bytes.Equal(got, bad) {
		t.Fatal("malformed request changed or reserved the queue")
	}
	good := []byte(`{"model":"claude-fable-5","messages":[]}`)
	_, reservation = injectRequest(good, session)
	if reservation == nil || len(reservation.items) != 1 || reservation.items[0].Text != "keep me" {
		t.Fatalf("queued item was lost after malformed request: %#v", reservation)
	}
	finishInject(reservation, false)
	_, retry := injectRequest(good, session)
	if retry == nil || len(retry.items) != 1 {
		t.Fatal("failed response did not release the reservation for retry")
	}
}

func TestInjectQueueCapsAreTypedAndNeverDrop(t *testing.T) {
	withInjectWorld(t)
	oldItems, oldItemBytes, oldSessionBytes := injectMaxItems, injectMaxItemBytes, injectMaxSessionBytes
	injectMaxItems, injectMaxItemBytes, injectMaxSessionBytes = 2, 8, 12
	t.Cleanup(func() {
		injectMaxItems, injectMaxItemBytes, injectMaxSessionBytes = oldItems, oldItemBytes, oldSessionBytes
	})
	key := injectObserve("session-a", time.Now())
	if _, _, p := injectEnqueue(key, "123456789", time.Now()); p == nil || p.Type != "instruction_too_large" {
		t.Fatalf("oversize problem = %#v", p)
	}
	if _, _, p := injectEnqueue(key, "12345678", time.Now()); p != nil {
		t.Fatal(p)
	}
	if _, _, p := injectEnqueue(key, "12345", time.Now()); p == nil || p.Type != "queue_full" {
		t.Fatalf("byte-cap problem = %#v", p)
	}
	if _, _, p := injectEnqueue(key, "four", time.Now()); p != nil {
		t.Fatal(p)
	}
	if _, depth, p := injectEnqueue(key, "x", time.Now()); p == nil || p.Type != "queue_full" || depth != 2 {
		t.Fatalf("item-cap result depth=%d problem=%#v", depth, p)
	}
	_, reservation := injectRequest([]byte(`{"messages":[]}`), "session-a")
	if reservation == nil || len(reservation.items) != 2 || reservation.items[0].Text != "12345678" || reservation.items[1].Text != "four" {
		t.Fatalf("cap silently dropped/reordered accepted items: %#v", reservation)
	}
}

func TestInjectRejectsUnknownSession(t *testing.T) {
	withInjectWorld(t)
	_, _, problem := injectEnqueue(strings.Repeat("a", 64), "hello", time.Now())
	if problem == nil || problem.Type != "unknown_session" || problem.Status != 404 {
		t.Fatalf("problem = %#v", problem)
	}
}

// Core's own words for what a client reads name no edition's tools; an
// edition that has them names them from its init().
func TestCoreClientWordsAreNeutral(t *testing.T) {
	if !strings.HasPrefix(defaultInjectPreamble, "[Queued]\n") {
		t.Errorf("defaultInjectPreamble = %q, want it to open with [Queued]", defaultInjectPreamble)
	}
	for name, s := range map[string]string{"defaultInjectPreamble": defaultInjectPreamble, "defaultReauthSeeWhere": defaultReauthSeeWhere} {
		if strings.Contains(s, "Scry") {
			t.Errorf("%s = %q names Scry", name, s)
		}
	}
}
