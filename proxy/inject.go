package main

// ---- queued instruction injection (#66) ----
//
// Scry can queue text for a live Claude Code session. The proxy appends the queued
// items as ONE synthetic user message at the end of that session's next /v1/messages
// body. Nothing is installed beside the client, and nothing is persisted: a restart
// intentionally loses this live-only state just like the transcript ring.
//
// The request path identifies a session exclusively from x-claude-code-session-id.
// Scry never receives that raw id. A turn event carries injectSessionKey(id), a full
// SHA-256 opaque handle; the enqueue endpoint accepts that handle only after the proxy
// has observed it on a tapped request. This avoids both exposing the client id and
// addressing writes with fpSess's collision-prone eight-character display prefix.
//
// Empty-queue requests return the original []byte without parsing or copying. This is
// load-bearing: the path carries every client.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

var (
	injectMaxSessions     = 512
	injectMaxItems        = 16
	injectMaxItemBytes    = 8 << 10
	injectMaxSessionBytes = 32 << 10
	injectSessionTTL      = 6 * time.Hour
)

type queuedInstruction struct {
	ID     string    `json:"id"`
	Text   string    `json:"-"`
	Bytes  int       `json:"bytes"`
	Queued time.Time `json:"queued"`
}

type injectSession struct {
	items    []queuedInstruction
	bytes    int
	seen     time.Time
	inFlight int64 // one request may reserve the queue; 0 means available
}

var (
	injectMu       sync.Mutex
	injectSessions = map[string]*injectSession{}
	injectSeq      int64
)

type injectError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Status  int    `json:"-"`
}

func (e *injectError) Error() string { return e.Message }

type injectQueuedEvent struct {
	SessionKey string `json:"session_key"`
	Session    string `json:"session"`
	ID         string `json:"id"`
	Bytes      int    `json:"bytes"`
	Depth      int    `json:"depth"`
	Queued     string `json:"queued"`
}

type injectDeliveredEvent struct {
	SessionKey string   `json:"session_key"`
	Session    string   `json:"session"`
	IDs        []string `json:"ids"`
	Items      int      `json:"items"`
	Bytes      int      `json:"bytes"`
}

type injectReservation struct {
	key   string
	token int64
	items []queuedInstruction
}

func injectSessionKey(session string) string {
	sum := sha256.Sum256([]byte(session))
	return hex.EncodeToString(sum[:])
}

// injectObserve makes an opaque session key addressable by Scry. It is called only
// while the event hub is armed, from tapTurn, so an idle proxy does not grow this map.
func injectObserve(session string, now time.Time) string {
	key := injectSessionKey(session)
	injectMu.Lock()
	defer injectMu.Unlock()
	if s := injectSessions[key]; s != nil {
		s.seen = now
		return key
	}
	injectEvictLocked(now)
	if len(injectSessions) < injectMaxSessions {
		injectSessions[key] = &injectSession{seen: now}
	}
	return key
}

func injectEvictLocked(now time.Time) {
	for key, s := range injectSessions {
		if len(s.items) == 0 && now.Sub(s.seen) > injectSessionTTL {
			delete(injectSessions, key)
		}
	}
	for len(injectSessions) >= injectMaxSessions {
		oldest, at := "", time.Time{}
		for key, s := range injectSessions {
			if len(s.items) != 0 {
				continue
			}
			if at.IsZero() || s.seen.Before(at) {
				oldest, at = key, s.seen
			}
		}
		if oldest == "" {
			return // every tracked session has queued work; reject the new observation
		}
		delete(injectSessions, oldest)
	}
}

func injectEnqueue(sessionKey, text string, now time.Time) (queuedInstruction, int, *injectError) {
	text = strings.TrimSpace(text)
	if text == "" {
		return queuedInstruction{}, 0, &injectError{
			Type: "empty_instruction", Message: "instruction must not be empty", Status: http.StatusBadRequest,
		}
	}
	if !utf8.ValidString(text) {
		return queuedInstruction{}, 0, &injectError{
			Type: "invalid_instruction", Message: "instruction must be valid UTF-8", Status: http.StatusBadRequest,
		}
	}
	if len(text) > injectMaxItemBytes {
		return queuedInstruction{}, 0, &injectError{
			Type: "instruction_too_large", Message: "instruction exceeds the per-item byte cap", Status: http.StatusRequestEntityTooLarge,
		}
	}

	injectMu.Lock()
	s := injectSessions[sessionKey]
	if s == nil {
		injectMu.Unlock()
		return queuedInstruction{}, 0, &injectError{
			Type: "unknown_session", Message: "session is not live in this proxy process", Status: http.StatusNotFound,
		}
	}
	if len(s.items) >= injectMaxItems || s.bytes+len(text) > injectMaxSessionBytes {
		injectMu.Unlock()
		return queuedInstruction{}, len(s.items), &injectError{
			Type: "queue_full", Message: "session instruction queue is full", Status: http.StatusConflict,
		}
	}
	id := evEpoch + "-q-" + decimal(atomic.AddInt64(&injectSeq, 1))
	item := queuedInstruction{ID: id, Text: text, Bytes: len(text), Queued: now.UTC()}
	s.items = append(s.items, item)
	s.bytes += item.Bytes
	s.seen = now
	depth := len(s.items)
	injectMu.Unlock()

	publish("instruction_queued", injectQueuedEvent{
		SessionKey: sessionKey, Session: sessionKey[:8], ID: id,
		Bytes: item.Bytes, Depth: depth, Queued: item.Queued.Format(time.RFC3339Nano),
	})
	return item, depth, nil
}

func decimal(n int64) string { return strconv.FormatInt(n, 10) }

// injectRequest appends all currently queued work and reserves it for this request. A
// malformed body changes nothing. A second concurrent request sees the reservation and
// passes through unchanged; finishInject releases or commits it after the response.
func injectRequest(body []byte, session string) ([]byte, *injectReservation) {
	if len(body) == 0 || session == "" {
		return body, nil
	}
	key := injectSessionKey(session)
	injectMu.Lock()
	s := injectSessions[key]
	if s == nil || len(s.items) == 0 || s.inFlight != 0 {
		injectMu.Unlock()
		return body, nil
	}
	items := append([]queuedInstruction(nil), s.items...)
	message, err := injectMessage(items)
	if err != nil {
		injectMu.Unlock()
		return body, nil
	}
	scan, ok := tapScanBody(body)
	if !ok || scan.messagesClose < 0 || scan.messagesClose > len(body) {
		injectMu.Unlock()
		return body, nil
	}
	joint := []byte(nil)
	if scan.count > 0 {
		joint = []byte(",")
	}
	out := make([]byte, 0, len(body)+len(joint)+len(message))
	out = append(out, body[:scan.messagesClose]...)
	out = append(out, joint...)
	out = append(out, message...)
	out = append(out, body[scan.messagesClose:]...)
	token := atomic.AddInt64(&injectSeq, 1)
	s.inFlight = token
	s.seen = time.Now()
	injectMu.Unlock()
	return out, &injectReservation{key: key, token: token, items: items}
}

func finishInject(res *injectReservation, delivered bool) {
	if res == nil {
		return
	}
	injectMu.Lock()
	s := injectSessions[res.key]
	if s == nil || s.inFlight != res.token {
		injectMu.Unlock()
		return
	}
	s.inFlight = 0
	if !delivered {
		injectMu.Unlock()
		return
	}
	// Items enqueued while the request was in flight remain behind the delivered prefix.
	if len(s.items) >= len(res.items) {
		s.items = s.items[len(res.items):]
	}
	s.bytes = 0
	for _, item := range s.items {
		s.bytes += item.Bytes
	}
	s.seen = time.Now()
	injectMu.Unlock()

	ids := make([]string, len(res.items))
	bytesN := 0
	for i, item := range res.items {
		ids[i] = item.ID
		bytesN += item.Bytes
	}
	publish("instruction_delivered", injectDeliveredEvent{
		SessionKey: res.key, Session: res.key[:8], IDs: ids, Items: len(res.items), Bytes: bytesN,
	})
}

// injectPreamble opens the synthetic turn a queued instruction rides in: a
// heading the model can see it by, and what to do with it. An edition whose
// queue has a name of its own says so from its init().
var injectPreamble = defaultInjectPreamble

const defaultInjectPreamble = "[Queued]\n" +
	"The operator queued the following while this session was running. " +
	"Acknowledge in your response that the queued instruction was received, then follow it.\n"

func injectMessage(items []queuedInstruction) ([]byte, error) {
	var text strings.Builder
	text.WriteString(injectPreamble)
	for i, item := range items {
		if len(items) > 1 {
			text.WriteString("\n")
			text.WriteString(decimal(int64(i + 1)))
			text.WriteString(". ")
		}
		text.WriteString(item.Text)
	}
	return json.Marshal(map[string]any{
		"role":    "user",
		"content": []map[string]string{{"type": "text", "text": text.String()}},
	})
}

func injectReset() {
	injectMu.Lock()
	injectSessions = map[string]*injectSession{}
	injectMu.Unlock()
}

func injectStatus() map[string]any {
	injectMu.Lock()
	defer injectMu.Unlock()
	queuedSessions, queuedItems, queuedBytes := 0, 0, 0
	for _, s := range injectSessions {
		if len(s.items) > 0 {
			queuedSessions++
			queuedItems += len(s.items)
			queuedBytes += s.bytes
		}
	}
	return map[string]any{
		"known_sessions": len(injectSessions), "queued_sessions": queuedSessions,
		"queued_items": queuedItems, "queued_bytes": queuedBytes,
		"max_items_per_session": injectMaxItems, "max_item_bytes": injectMaxItemBytes,
		"max_session_bytes": injectMaxSessionBytes,
	}
}

type injectPostBody struct {
	SessionKey string `json:"session_key"`
	Text       string `json:"text"`
}

func injectHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	r.Body = http.MaxBytesReader(w, r.Body, int64(injectMaxItemBytes+1024))
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var in injectPostBody
	if err := dec.Decode(&in); err != nil {
		writeInjectError(w, &injectError{Type: "invalid_request", Message: "invalid enqueue request", Status: http.StatusBadRequest})
		return
	}
	if len(in.SessionKey) != sha256.Size*2 {
		writeInjectError(w, &injectError{Type: "invalid_session", Message: "session_key must be an opaque session key from a turn event", Status: http.StatusBadRequest})
		return
	}
	if _, err := hex.DecodeString(in.SessionKey); err != nil {
		writeInjectError(w, &injectError{Type: "invalid_session", Message: "session_key must be an opaque session key from a turn event", Status: http.StatusBadRequest})
		return
	}
	item, depth, problem := injectEnqueue(in.SessionKey, in.Text, time.Now())
	if problem != nil {
		writeInjectError(w, problem)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok": true, "state": "queued", "id": item.ID, "bytes": item.Bytes,
		"depth": depth, "queued": item.Queued.Format(time.RFC3339Nano),
	})
}

func writeInjectError(w http.ResponseWriter, problem *injectError) {
	w.WriteHeader(problem.Status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok": false, "error": problem,
	})
}
