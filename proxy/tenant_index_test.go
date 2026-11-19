package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The members index (MEMBERS-PLAN 1b): filled from GET /tenants, kept by
// GET /tenants/events, persisted beside the class state and reloaded at
// boot as the last-good rule the class rows follow. A tenant that leaves
// the index is stopped and its state dropped; the lan tenant is the one
// row marked lan.
func TestTenantIndexFetchEventsAndPersistence(t *testing.T) {
	const b = "ten_memberbbbbbbbbbbbbbbbbbbbb"
	rowA := tenantRow{Tenant: testLanTenant, Name: "household", Lan: true, Tap: tapContent}
	rowB := tenantRow{Tenant: b, Name: "member b", Tap: tapMetadata}

	var index atomic.Value // []tenantRow the brain serves now
	index.Store([]tenantRow{rowA, rowB})
	events := make(chan []tenantRow, 4)
	var keyed atomic.Int64
	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Switch-Key") != "idx-key" {
			w.WriteHeader(401)
			return
		}
		keyed.Add(1)
		switch r.URL.Path {
		case "/tenants":
			_ = json.NewEncoder(w).Encode(map[string]any{"tenants": index.Load()})
		case "/tenants/events":
			w.Header().Set("Content-Type", "text/event-stream")
			fl := w.(http.Flusher)
			fl.Flush()
			for {
				select {
				case rows := <-events:
					d, _ := json.Marshal(map[string]any{"tenants": rows})
					fmt.Fprintf(w, "event: tenants\ndata: %s\n\n", d)
					fl.Flush()
				case <-r.Context().Done():
					return
				}
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer brain.Close()

	oldBrain, oldKey, oldPath := brainBase, switchKey, tenantsStateFilePath
	brainBase, switchKey = brain.URL, "idx-key"
	tenantsStateFilePath = filepath.Join(t.TempDir(), "state", "tenants.json")
	// The tenants this test starts are stopped (resetTenants) before the
	// globals their goroutines read are put back.
	defer func() { resetTenants(); brainBase, switchKey, tenantsStateFilePath = oldBrain, oldKey, oldPath }()

	// Empty index, then the fetch: both rows, the household the lan tenant.
	installTenants()
	if lanTenant() != nil {
		t.Fatal("an empty index has a lan tenant")
	}
	if err := fetchTenantIndex(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := lanTenantID(); got != testLanTenant {
		t.Fatalf("lan tenant = %q, want the household", got)
	}
	tb := memberOf(b)
	if tb == nil || tb.tap != tapMetadata || tb.name != "member b" || tb.lan {
		t.Fatalf("member b = %+v", tb)
	}
	if memberOf("ten_nobodyzzzzzzzzzzzzzzzzzzzz") != nil {
		t.Fatal("an unlisted id resolves")
	}
	if tb.brainURL() != brain.URL+"/t/"+b {
		t.Fatalf("B's brain URL = %q", tb.brainURL())
	}

	// Persisted beside the class state: the file names both rows.
	raw, err := os.ReadFile(tenantsStateFilePath)
	if err != nil {
		t.Fatal(err)
	}
	var f tenantsStateFile
	if err := json.Unmarshal(raw, &f); err != nil || len(f.Tenants) != 2 {
		t.Fatalf("state file %s: %v", raw, err)
	}

	// The events stream: B leaves, then a new C arrives with B gone; the
	// same tenant object stands for the household throughout.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { runTenantEvents(ctx); close(done) }()
	hh := lanTenant()
	events <- []tenantRow{rowA}
	evWaitFor(t, "B to leave the index", 3*time.Second, func() bool { return memberOf(b) == nil })
	if lanTenant() != hh {
		t.Fatal("the household's tenant object was replaced by an event that kept it")
	}
	const c = "ten_membercccccccccccccccccccc"
	events <- []tenantRow{rowA, {Tenant: c, Name: "member c", Tap: tapMetadata}}
	evWaitFor(t, "C to join the index", 3*time.Second, func() bool { return memberOf(c) != nil })
	if memberOf(b) != nil {
		t.Fatal("B came back with C")
	}
	// An index the proxy refuses (two lan rows) leaves the one in force.
	events <- []tenantRow{rowA, {Tenant: c, Name: "member c", Lan: true, Tap: tapMetadata}}
	time.Sleep(100 * time.Millisecond)
	if memberOf(c) == nil || lanTenantID() != testLanTenant {
		t.Fatal("a refused index changed the one in force")
	}
	cancel()
	<-done

	// The last-good file reloads at boot, with the index's source named.
	installTenants()
	loadTenantIndexState()
	if memberOf(c) == nil || memberOf(b) != nil || lanTenantID() != testLanTenant {
		t.Fatalf("reload: lan=%q b=%v c=%v", lanTenantID(), memberOf(b) != nil, memberOf(c) != nil)
	}
	if st := tenantsStatus(); st["source"] != "last-good" || st["count"] != 2 {
		t.Fatalf("tenants status after reload = %v", st)
	}
	if keyed.Load() == 0 {
		t.Fatal("the brain never saw the switch key")
	}
}

// The request's tenant is only what an entry stamped: a bare context, a
// Background one and a door connection before its stamp all have none (the
// typed 503), never the household. :8787's stamp is clientGate's, and with
// no lan tenant in the index it stamps none.
func TestTenantFromContext(t *testing.T) {
	defer resetTenants()
	if tn := tenantFrom(context.Background()); tn != nil {
		t.Fatalf("a bare context = %v, want none: nothing maps an unstamped context to the household", tn)
	}
	if tn := tenantFrom(nil); tn != nil {
		t.Fatalf("a nil context = %v, want none", tn)
	}
	ctx := withEntry(context.Background(), entryInfo{kind: entryHTTPSProxy, client: "x"})
	if tn := tenantFrom(ctx); tn != nil {
		t.Fatalf("a door connection without a tenant = %v, want none", tn)
	}
	b := addTenant(tenantRow{Tenant: "ten_memberbbbbbbbbbbbbbbbbbbbb", Name: "b", Tap: tapMetadata})
	if tn := tenantFrom(withTenant(ctx, b)); tn != b {
		t.Fatalf("a stamped context = %v, want b", tn)
	}
	// ctxOf: a helper on b carries b whatever it was handed.
	if tn := tenantFrom(b.ctxOf(nil)); tn != b {
		t.Fatalf("b.ctxOf(nil) carries %v, want b", tn)
	}
	if tn := tenantFrom(b.ctxOf(withTenant(ctx, lan))); tn != b {
		t.Fatalf("b.ctxOf(a context stamped lan) carries %v, want b", tn)
	}
	// The :8787 entry stamps the lan tenant on every request, gated or not.
	var seen *tenant
	gate := clientGate("", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { seen = tenantFrom(r.Context()) }))
	gate.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/_switch/status", nil))
	if seen != lan {
		t.Fatalf(":8787 ungated stamped %v, want the lan tenant", seen)
	}
	seen = nil
	gated := clientGate("s3cret", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { seen = tenantFrom(r.Context()) }))
	req := httptest.NewRequest("POST", "/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer s3cret")
	gated.ServeHTTP(httptest.NewRecorder(), req)
	if seen != lan {
		t.Fatalf(":8787 gated stamped %v, want the lan tenant", seen)
	}
	installTenants()
	seen = lan
	gate.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/_switch/status", nil))
	if seen != nil {
		t.Fatalf("no lan tenant: :8787 stamped %v, want none", seen)
	}
	if tn := tenantFrom(context.Background()); tn != nil {
		t.Fatalf("no lan tenant: bare context = %v, want none", tn)
	}
	rec := httptest.NewRecorder()
	writeNoTenant(rec, "ten_nobodyzzzzzzzzzzzzzzzzzzzz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("no tenant = %d, want 503", rec.Code)
	}
	var e struct {
		Error struct{ Type, Message string } `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil || e.Error.Type == "" {
		t.Fatalf("no-tenant body %s", rec.Body.String())
	}
}

// A request that reaches the routing chain with no stamped tenant (a bare
// request below clientGate, a door helper run before its stamp) is the typed
// 503 before any state is read; the lan tenant's stamp serves it.
func TestRoutedHelperWithoutTenantIs503(t *testing.T) {
	defer resetTenants()
	forwarded := false
	h := requestHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forwarded = true; w.WriteHeader(200) }))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/messages", nil))
	if rec.Code != http.StatusServiceUnavailable || forwarded {
		t.Fatalf("bare request: %d forwarded=%v %s, want the typed 503 and nothing forwarded", rec.Code, forwarded, rec.Body)
	}
	var e struct {
		Error struct{ Type, Message string } `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil || e.Error.Type != "api_error" || !strings.Contains(e.Error.Message, "no tenant") {
		t.Fatalf("refusal body: %s", rec.Body)
	}
	// A door connection before its stamp is the same 503.
	rec = httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/messages", nil)
	req = req.WithContext(withEntry(req.Context(), entryInfo{kind: entryHTTPSProxy, client: "x", tenant: testLanTenant}))
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || forwarded {
		t.Fatalf("door request before its stamp: %d forwarded=%v, want the typed 503", rec.Code, forwarded)
	}
	// The transport, the last hop before an account: the same answer.
	tr := &failoverTransport{base: http.DefaultTransport}
	hop, _ := http.NewRequest("POST", "http://upstream.invalid/v1/messages", nil)
	resp, err := tr.RoundTrip(hop)
	if err != nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("transport hop with no tenant: %v %v, want the typed 503", resp, err)
	}
}

// An index is valid only with exactly one lan row: an empty list, one with
// no lan row and one with two are refused whole — the index in force stays
// (the household's state with it), the refusal is shown in status and is
// never persisted as last-good; the next good index clears it. Over the
// fetch and the events stream alike.
func TestTenantIndexRefusedWithoutOneLanRow(t *testing.T) {
	const b = "ten_memberbbbbbbbbbbbbbbbbbbbb"
	rowA := tenantRow{Tenant: testLanTenant, Name: "household", Lan: true, Tap: tapContent}
	rowB := tenantRow{Tenant: b, Name: "member b", Tap: tapMetadata}
	good := []tenantRow{rowA, rowB}

	var index atomic.Value
	index.Store(good)
	events := make(chan []tenantRow, 4)
	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/tenants":
			_ = json.NewEncoder(w).Encode(map[string]any{"tenants": index.Load()})
		case "/tenants/events":
			w.Header().Set("Content-Type", "text/event-stream")
			fl := w.(http.Flusher)
			fl.Flush()
			for {
				select {
				case rows := <-events:
					d, _ := json.Marshal(map[string]any{"tenants": rows})
					fmt.Fprintf(w, "event: tenants\ndata: %s\n\n", d)
					fl.Flush()
				case <-r.Context().Done():
					return
				}
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer brain.Close()
	oldBrain, oldKey, oldPath := brainBase, switchKey, tenantsStateFilePath
	brainBase, switchKey = brain.URL, "idx-key"
	tenantsStateFilePath = filepath.Join(t.TempDir(), "tenants.json")
	defer func() { resetTenants(); brainBase, switchKey, tenantsStateFilePath = oldBrain, oldKey, oldPath }()

	installTenants()
	if err := fetchTenantIndex(context.Background()); err != nil {
		t.Fatal(err)
	}
	hh := lanTenant()
	if hh == nil || memberOf(b) == nil {
		t.Fatal("the good index did not apply")
	}
	goodFile, err := os.ReadFile(tenantsStateFilePath)
	if err != nil {
		t.Fatal(err)
	}
	// Give the household state a refused index would drop (a cached token:
	// nothing the running stream rewrites).
	hh.tokMu.Lock()
	hh.tokens["user@example.com"] = tokEntry{token: "cached"}
	hh.tokMu.Unlock()

	bad := map[string][]tenantRow{
		"empty":   {},
		"no lan":  {rowB, {Tenant: testLanTenant, Name: "household", Tap: tapContent}},
		"two lan": {rowA, {Tenant: b, Name: "member b", Lan: true, Tap: tapMetadata}},
	}
	for name, rows := range bad {
		index.Store(rows)
		err := fetchTenantIndex(context.Background())
		if err == nil {
			t.Fatalf("%s index: applied, want refused", name)
		}
		if lanTenant() != hh || memberOf(b) == nil || lanTenantID() != testLanTenant {
			t.Fatalf("%s index: the index in force changed (lan=%q)", name, lanTenantID())
		}
		hh.tokMu.Lock()
		_, kept := hh.tokens["user@example.com"]
		hh.tokMu.Unlock()
		if !kept {
			t.Fatalf("%s index: the household's state was dropped", name)
		}
		st := tenantsStatus()
		ref, has := st["refused"].(tenantRefusal)
		if !has || ref.Reason != err.Error() || ref.Source != "brain" || ref.Rows != len(rows) || ref.At == "" {
			t.Fatalf("%s index: status refused = %#v, want the refusal %q", name, st["refused"], err)
		}
		if st["count"] != 2 {
			t.Fatalf("%s index: status count = %v, want the index in force", name, st["count"])
		}
		if now, _ := os.ReadFile(tenantsStateFilePath); string(now) != string(goodFile) {
			t.Fatalf("%s index: the last-good file was rewritten: %s", name, now)
		}
		t.Logf("%s index refused: %v", name, err)
	}

	// The events stream: a refused event is shown, the next good one clears it.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { runTenantEvents(ctx); close(done) }()
	events <- []tenantRow{}
	evWaitFor(t, "the empty event to be refused", 3*time.Second, func() bool {
		_, has := tenantsStatus()["refused"]
		return has && tenantsStatus()["refused"].(tenantRefusal).Rows == 0
	})
	if lanTenant() != hh || memberOf(b) == nil {
		t.Fatal("an empty tenants event changed the index in force")
	}
	events <- []tenantRow{rowA}
	evWaitFor(t, "B to leave on a good event", 3*time.Second, func() bool { return memberOf(b) == nil })
	if _, has := tenantsStatus()["refused"]; has {
		t.Fatalf("a good index left the refusal in status: %v", tenantsStatus()["refused"])
	}
	if lanTenant() != hh {
		t.Fatal("the household's tenant object was replaced")
	}
	cancel()
	<-done

	// A last-good file with no lan row is refused at boot: no tenants, the
	// refusal shown.
	if err := os.WriteFile(tenantsStateFilePath, []byte(`{"saved_at":1,"tenants":[{"tenant":"`+b+`","name":"b","tap":"metadata"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	installTenants()
	loadTenantIndexState()
	if lanTenant() != nil || memberOf(b) != nil {
		t.Fatal("a last-good file without a lan row was applied")
	}
	if ref, has := tenantsStatus()["refused"].(tenantRefusal); !has || ref.Source != "last-good" {
		t.Fatalf("status refused after a bad last-good file = %#v", tenantsStatus()["refused"])
	}
}

// stop ends the tenant's goroutines and waits for them: after it the brain
// stream's connection is closed and the sender has returned, so nothing of
// the tenant's reads the globals (brainBase) after the index forgets it.
func TestTenantStopWaitsForItsGoroutines(t *testing.T) {
	connected := make(chan struct{}, 1)
	dropped := make(chan struct{}, 1)
	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/events") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		select {
		case connected <- struct{}{}:
		default:
		}
		<-r.Context().Done()
		select {
		case dropped <- struct{}{}:
		default:
		}
	}))
	defer brain.Close()
	oldBrain := brainBase
	brainBase = brain.URL
	defer func() { brainBase = oldBrain }()

	tn := newTenant(tenantRow{Tenant: "ten_memberbbbbbbbbbbbbbbbbbbbb", Name: "b", Tap: tapMetadata})
	tn.start()
	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("the tenant's stream never connected")
	}
	tn.repMu.Lock()
	run := tn.repRunner
	tn.repMu.Unlock()
	if run == nil {
		t.Fatal("the tenant's reporter did not start")
	}
	stopped := make(chan struct{})
	go func() { tn.stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(tenantStopWait + time.Second):
		t.Fatal("stop did not return")
	}
	select {
	case <-dropped:
	case <-time.After(time.Second): // the server sees the close a moment after the client's
		t.Fatal("stop returned with the brain stream's connection still open")
	}
	select {
	case <-run.done:
	default:
		t.Fatal("stop returned with the sender still running")
	}
	waited := make(chan struct{})
	go func() { tn.bgWG.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(time.Second):
		t.Fatal("stop returned with a background goroutine still counted")
	}
}

// A member is metadata whatever its index row says: a row not marked lan
// that asks tap content is refused that mode, loads as metadata, and says so
// in the log, on its first load and on every index that names it again. The
// household keeps the content tap its row names.
func TestMemberAskingContentLoadsAsMetadata(t *testing.T) {
	const b = "ten_memberbbbbbbbbbbbbbbbbbbbb"
	oldPath := tenantsStateFilePath
	tenantsStateFilePath = ""
	defer func() { resetTenants(); tenantsStateFilePath = oldPath }()
	logs := captureLog(t)

	rows := []tenantRow{
		{Tenant: testLanTenant, Name: "household", Lan: true, Tap: tapContent},
		{Tenant: b, Name: "member b", Tap: tapContent},
	}
	installTenants()
	for _, pass := range []string{"new", "again"} {
		logs.Reset()
		if err := applyTenantIndex(rows, "test"); err != nil {
			t.Fatalf("%s: index refused: %v", pass, err)
		}
		mb := memberOf(b)
		if mb == nil || !mb.metadataOnly() || mb.tap != tapMetadata {
			t.Fatalf("%s: member asking content loaded with tap %q, want metadata", pass, mb.tap)
		}
		if hh := lanTenant(); hh == nil || hh.metadataOnly() {
			t.Fatalf("%s: the household lost its content tap", pass)
		}
		if !strings.Contains(logs.String(), b+" is a member but its index row asks tap \"content\": refused") {
			t.Fatalf("%s: no loud refusal in the log: %q", pass, logs.String())
		}
	}
}

// Every log line that shows a provider's words about a request goes through
// logProviderText: a member's (and a request with no tenant) shows only the
// error type, the household's the clipped body.
func TestLogProviderTextHidesAMembersWords(t *testing.T) {
	member := addTenant(tenantRow{Tenant: "ten_" + strings.Repeat("m", 26), Name: "member", Tap: tapMetadata})
	defer removeTenant(member.id)
	body := []byte(`{"type":"error","error":{"type":"not_found_error","message":"secret words"}}`)
	for name, tn := range map[string]*tenant{"member": member, "no tenant": nil} {
		if got := tn.logBody(body); got != `error_type="not_found_error"` {
			t.Fatalf("%s: logBody = %s, want the error type alone", name, got)
		}
		if got := tn.logProviderText("", "authentication_error", "secret words"); got != `error_type="authentication_error"` {
			t.Fatalf("%s: logProviderText = %s, want the error type alone", name, got)
		}
		if got := tn.logBody([]byte("<html>secret words</html>")); got != `error_type="unknown"` {
			t.Fatalf("%s: logBody of a non-JSON body = %s", name, got)
		}
	}
	if got := lan.logBody(body); !strings.HasPrefix(got, "body=") || !strings.Contains(got, "secret words") {
		t.Fatalf("household: logBody = %s, want the body snippet", got)
	}
	long := strings.Repeat("x", 400)
	if got := lan.logProviderText("", "", long); got != strconv.Quote(long[:300]+"…") {
		t.Fatalf("household: logProviderText not clipped to 300: %d bytes", len(got))
	}
}
