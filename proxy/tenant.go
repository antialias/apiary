package main

// Tenants (Apiary members, MEMBERS-PLAN step 1b).
//
// One brain serves every tenant at /t/<tenant>/…, and this proxy keeps one
// `tenant` per row of the brain's index (GET /tenants). Everything that was
// package-level state about "the" brain document, its accounts and its
// sessions lives on that struct: the decision, the credential caches, the
// refusal records, the class binds, the health store, the ledger, the
// reporter, the session facts and labels, the transcript tap's cursors, the
// inject queues, the OpenAI wire's buckets and the brain stream reader.
//
// A request learns its tenant once, at the entry (entry.go: withTenant), and
// every per-tenant read below that goes through the *tenant it carries. A
// request with no tenant on its context is refused with a typed 503, never
// served from another tenant's state. The one credential boundary is
// actuatorFor: an account absent from the tenant's own document is never
// dialled, whatever named it.
//
// The household is tenant #1: the index row marked `lan: true`. Users-file
// logins and :8787 clients belong to it (lanTenant); a device token names its
// tenant in its `mem` claim. Nothing in this file maps "no tenant" to the
// household.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// tenantIDRe is a tenant id's grammar: ten_ + 26 base32, like usr_ and dev_.
var tenantIDRe = regexp.MustCompile(`^ten_[a-z2-7]{26}$`)

// Tap modes (the index's `tap`): what the transcript tap emits for the
// tenant's turns.
const (
	tapContent  = "content"  // the household: messages, system blocks, replies
	tapMetadata = "metadata" // members: numbers, class, lane, ids; never content
)

// tenantRow is one row of the brain's index: GET /tenants, and each
// `tenants` event on GET /tenants/events, carry a list of these.
type tenantRow struct {
	Tenant string `json:"tenant"`
	Name   string `json:"name"`
	Lan    bool   `json:"lan"`
	Tap    string `json:"tap"`
}

// tenant is everything this proxy holds for one member.
type tenant struct {
	id   string
	name string
	lan  bool
	tap  string

	// credDrops counts dropDeparted's drops of cached credentials. A brain
	// fetch reads it before it starts and caches its answer only when it has
	// not moved since, so a fetch begun before a drop never re-caches what the
	// drop forgot.
	credDrops atomic.Uint64

	// The brain's document and the per-lane routing state (main.go).
	pref          *decision
	effMu         sync.Mutex
	lastEffective map[string]string     // lane -> account that last served it
	lastState     map[string]routeState // lane -> most recent routeState

	// The subscription actuator's token cache and prewarm (subscription.go).
	tokMu         sync.Mutex
	tokens        map[string]tokEntry
	reauthLocksMu sync.Mutex
	reauthLocks   map[string]*sync.Mutex
	warmMu        sync.Mutex
	warmSet       map[string]bool
	warmTimers    map[string]*time.Timer

	// The codex dispensed-bearer cache (codex.go).
	codexTokMu sync.Mutex
	codexToks  map[string]codexTokEntry

	// The api-kind dispensed keys (apikey.go).
	apiKeyMu     sync.Mutex
	apiKeys      map[string]apiKeyEntry
	apiKeyStates map[string]string

	// The refusal records and readings (records.go).
	recMu    sync.Mutex
	records  map[recKey]refusalRecord
	readings map[string]headerReading

	// Class binds and session fundings (class.go).
	bindMu          sync.Mutex
	bindCache       map[string]classBind
	sessionFundings map[string]sessionFunding

	// The degraded-member health store and its recovery runner (health.go);
	// nil under SWITCH_HEALTH=off.
	health         *healthStore
	healthRecovery *recoveryRunner

	// The ledger's own entries and holds (ledger.go).
	ledgerMu      sync.Mutex
	ledgerOwn     []*ownEntry
	ledgerLastErr string
	ledgerHolds   map[*ledgerHold]struct{}

	// The reporter's queue and runner (reports.go).
	repMu                sync.Mutex
	repQueue             map[reportKey]queuedReport
	repGen               uint64
	repLast              map[reportKey]reportEvent
	repServed            []*servedQueued
	repServedSeq         int64
	repServedDropped     int64
	repServedRefused     int64
	repServedUnencodable int64
	repLastErr           string
	repWake              chan struct{}
	repSlot              chan struct{}
	repRunner            *reporterRun

	// Session facts for the status line (session_facts.go) and the labels a
	// session's create and /rename gave it (session_labels.go).
	sessFactsMu sync.Mutex
	sessFacts   map[string]sessionFact
	labelsMu    sync.Mutex
	labels      map[string]*sessionLabel

	// The transcript tap's cursors and reply anchors (tap.go).
	tapMu      sync.Mutex
	tapConvs   map[string]*tapConv
	tapAnchors map[string]tapAnchor

	// Queued instruction injection (inject.go).
	injectMu       sync.Mutex
	injectSessions map[string]*injectSession

	// The OpenAI wire's per-class rate buckets (wire_openai.go).
	wireMu      sync.Mutex
	wireBuckets map[string]*wireBucket

	// The tenant's background work (start): the brain stream reader, the
	// health tick loop and the recovery prober run under bg, and bgWG
	// counts them, so stop can cancel them and wait for them to end.
	bg     context.Context
	bgStop context.CancelFunc
	bgWG   sync.WaitGroup
}

// newTenant is a tenant with fresh, empty state.
func newTenant(row tenantRow) *tenant {
	return &tenant{
		id: row.Tenant, name: row.Name, lan: row.Lan, tap: row.Tap,
		pref:            &decision{},
		lastEffective:   map[string]string{},
		lastState:       map[string]routeState{},
		tokens:          map[string]tokEntry{},
		reauthLocks:     map[string]*sync.Mutex{},
		warmSet:         map[string]bool{},
		warmTimers:      map[string]*time.Timer{},
		codexToks:       map[string]codexTokEntry{},
		apiKeys:         map[string]apiKeyEntry{},
		apiKeyStates:    map[string]string{},
		records:         map[recKey]refusalRecord{},
		readings:        map[string]headerReading{},
		bindCache:       map[string]classBind{},
		sessionFundings: map[string]sessionFunding{},
		ledgerHolds:     map[*ledgerHold]struct{}{},
		repQueue:        map[reportKey]queuedReport{},
		repLast:         map[reportKey]reportEvent{},
		repWake:         make(chan struct{}, 1),
		repSlot:         make(chan struct{}, 1),
		sessFacts:       map[string]sessionFact{},
		labels:          map[string]*sessionLabel{},
		tapConvs:        map[string]*tapConv{},
		tapAnchors:      map[string]tapAnchor{},
		injectSessions:  map[string]*injectSession{},
		wireBuckets:     map[string]*wireBucket{},
	}
}

// brainURL is the tenant's base on the brain: SWITCH_BRAIN_URL + "/t/" + id,
// "" when no brain is configured (direct mode: account lanes FAIL loud).
func (tn *tenant) brainURL() string {
	if brainBase == "" {
		return ""
	}
	return brainBase + "/t/" + tn.id
}

// metadataOnly reports whether the tenant's tap withholds content.
func (tn *tenant) metadataOnly() bool { return tn.tap == tapMetadata }

// ---- the index ----

var (
	membersMu sync.RWMutex
	// members is every tenant the brain's index names, by id.
	members = map[string]*tenant{}
	// lanID is the id of the index row marked lan, "" when none.
	lanID string
	// membersSource says where the index in force came from: "" (none yet),
	// "last-good" (the file at boot) or "brain" (GET /tenants or its events).
	membersSource string
	// tenantsStateFilePath is where the last index is persisted, beside
	// SWITCH_CLASS_STATE_FILE; "" keeps nothing (tests).
	tenantsStateFilePath string
	// membersRefusal is the last index refused since the index in force was
	// applied (nil: none): what /_switch/status shows as tenants.refused.
	membersRefusal *tenantRefusal
)

// tenantRefusal is one refused index: why, from where, when, how many rows.
type tenantRefusal struct {
	Reason string `json:"reason"`
	Source string `json:"source"`
	At     string `json:"at"`
	Rows   int    `json:"rows"`
}

// tenantIndexInForce reports whether any index has been applied (the
// last-good file or the brain's): before one, "not in the index" says
// nothing about a tenant.
func tenantIndexInForce() bool {
	membersMu.RLock()
	defer membersMu.RUnlock()
	return membersSource != ""
}

// memberOf is the tenant with id, nil when the index does not name it.
func memberOf(id string) *tenant {
	membersMu.RLock()
	defer membersMu.RUnlock()
	return members[id]
}

// lanTenant is the household: the index row marked lan. nil when the index
// names none — then users-file logins and :8787 have no tenant and their
// routed requests are refused.
func lanTenant() *tenant {
	membersMu.RLock()
	defer membersMu.RUnlock()
	if lanID == "" {
		return nil
	}
	return members[lanID]
}

// lanTenantID is the lan tenant's id, "" when the index names none: what an
// entry stamps on a users-file login and on :8787.
func lanTenantID() string {
	membersMu.RLock()
	defer membersMu.RUnlock()
	return lanID
}

// allTenants is every tenant, by id.
func allTenants() []*tenant {
	membersMu.RLock()
	out := make([]*tenant, 0, len(members))
	for _, tn := range members {
		out = append(out, tn)
	}
	membersMu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

// validTenantRow says whether a row may enter the index.
func validTenantRow(r tenantRow) error {
	if !tenantIDRe.MatchString(r.Tenant) {
		return fmt.Errorf("tenant id %q is not ten_ + 26 base32", r.Tenant)
	}
	switch r.Tap {
	case tapContent, tapMetadata:
	default:
		return fmt.Errorf("tenant %s: tap %q is neither content nor metadata", r.Tenant, r.Tap)
	}
	return nil
}

// validTenantIndex says whether rows can be the index: every row valid, no
// id twice, and exactly one lan row — the household is always a tenant, so
// an empty list or one without a lan row is not an index but a broken
// answer, and applying it would drop the household's state. The lan row's
// id is returned.
func validTenantIndex(rows []tenantRow) (string, error) {
	seen := map[string]bool{}
	lan := ""
	for _, r := range rows {
		if err := validTenantRow(r); err != nil {
			return "", err
		}
		if seen[r.Tenant] {
			return "", fmt.Errorf("tenant %s listed twice", r.Tenant)
		}
		seen[r.Tenant] = true
		if r.Lan {
			if lan != "" {
				return "", fmt.Errorf("two lan tenants: %s and %s", lan, r.Tenant)
			}
			lan = r.Tenant
		}
	}
	if len(rows) == 0 {
		return "", errors.New("empty index: the household is always a tenant")
	}
	if lan == "" {
		return "", fmt.Errorf("no lan tenant among %d rows: the household is always a tenant", len(rows))
	}
	return lan, nil
}

// applyTenantIndex makes rows the index. A tenant already present keeps its
// state and takes the row's name, lan and tap; a new one is started; one the
// rows no longer name is stopped and dropped. source is logged. A list that
// cannot be the index (validTenantIndex: a bad row, an id twice, no lan
// row, two lan rows, no rows) is refused whole: the index in force stays,
// the refusal is logged by the caller and shown in /_switch/status
// (tenants.refused) until an index is applied, and it is never persisted.
func applyTenantIndex(rows []tenantRow, source string) error {
	lan, err := validTenantIndex(rows)
	if err != nil {
		membersMu.Lock()
		membersRefusal = &tenantRefusal{Reason: err.Error(), Source: source, At: time.Now().UTC().Format(time.RFC3339), Rows: len(rows)}
		membersMu.Unlock()
		return err
	}
	seen := map[string]bool{}
	for _, r := range rows {
		seen[r.Tenant] = true
	}
	var started, stopped []*tenant
	membersMu.Lock()
	for _, r := range rows {
		tn := members[r.Tenant]
		if tn == nil {
			tn = newTenant(r)
			members[r.Tenant] = tn
			started = append(started, tn)
			continue
		}
		tn.name, tn.lan, tn.tap = r.Name, r.Lan, r.Tap
	}
	for id, tn := range members {
		if !seen[id] {
			delete(members, id)
			stopped = append(stopped, tn)
		}
	}
	lanID, membersSource, membersRefusal = lan, source, nil
	membersMu.Unlock()
	for _, tn := range stopped {
		tn.stop()
		log.Printf("tenants: %s left the index (%s); its state is dropped", tn.id, source)
	}
	for _, tn := range started {
		tn.start()
	}
	if len(started) > 0 || len(stopped) > 0 || source == "last-good" {
		log.Printf("tenants: %d in the index (%s), lan=%q", len(rows), source, lan)
	}
	return nil
}

// start begins the tenant's background work: its health store, its reporter
// and its brain stream. Nothing here waits on the brain.
func (tn *tenant) start() {
	tn.bg, tn.bgStop = context.WithCancel(withTenant(context.Background(), tn))
	if healthModeV != healthOff && tn.health == nil {
		cfg := defaultHealthCfg()
		cfg.stateFile = healthStatePath(tn.id)
		tn.health = loadHealthStore(cfg)
		tn.health.tenantID = tn.id
		tn.spawn(tn.healthTickLoop)
		if healthRecoveryOn && healthEnforcedClass(recoveryClass) {
			tn.healthRecovery = &recoveryRunner{tenant: tn,
				transport: &failoverTransport{base: http.DefaultTransport.(*http.Transport).Clone()}, deadline: recoveryDeadline}
			tn.spawn(tn.healthRecovery.run)
		}
	}
	if brainBase == "" {
		return
	}
	tn.startReporter()
	tn.spawn(tn.runBrainStream)
}

// spawn runs f under the tenant's background context, counted for stop.
func (tn *tenant) spawn(f func(context.Context)) {
	tn.bgWG.Add(1)
	go func() {
		defer tn.bgWG.Done()
		f(tn.bg)
	}()
}

// tenantStopWait bounds stop's wait for the tenant's goroutines: each ends
// as soon as its context does (a brain connection in flight is cut).
const tenantStopWait = 5 * time.Second

// stop ends the tenant's background work and its timers and waits for the
// goroutines to end, bounded by tenantStopWait: after it nothing of the
// tenant's runs, so the index may forget it. Its maps are left to the
// collector with it.
func (tn *tenant) stop() {
	if tn.bgStop != nil {
		tn.bgStop()
	}
	bound, cancel := context.WithTimeout(context.Background(), tenantStopWait)
	defer cancel()
	tn.stopReporterNow(bound)
	done := make(chan struct{})
	go func() { tn.bgWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-bound.Done():
		log.Printf("⚠️  tenants: %s: background work still running %s after its stop", tn.id, tenantStopWait)
	}
	tn.warmMu.Lock()
	for a, t := range tn.warmTimers {
		t.Stop()
		delete(tn.warmTimers, a)
	}
	tn.warmMu.Unlock()
}

// healthStatePath is the tenant's health snapshot: the configured file's
// directory, one file per tenant.
func healthStatePath(id string) string {
	if healthStateBase == "" {
		return ""
	}
	dir, name := filepath.Split(healthStateBase)
	ext := filepath.Ext(name)
	return filepath.Join(dir, strings.TrimSuffix(name, ext)+"."+id+ext)
}

// healthStateBase is SWITCH_HEALTH_STATE_FILE, the per-tenant files' pattern;
// healthRecoveryOn is SWITCH_HEALTH_RECOVERY=on. Both are written once in
// main() and then only read.
var (
	healthStateBase  string
	healthRecoveryOn bool
)

// ---- the index on the wire and on disk ----

// tenantIndexBody decodes the brain's index document, {"tenants": [...]}: the
// body of GET /tenants and the data of each /tenants/events event.
func tenantIndexBody(raw []byte) ([]tenantRow, error) {
	raw = []byte(strings.TrimSpace(string(raw)))
	if len(raw) == 0 {
		return nil, errors.New("empty index")
	}
	var d struct {
		Tenants *[]tenantRow `json:"tenants"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	if d.Tenants == nil {
		return nil, errors.New("index lacks tenants")
	}
	return *d.Tenants, nil
}

// fetchTenantIndex asks the brain for GET /tenants and applies it.
func fetchTenantIndex(ctx context.Context) error {
	if brainBase == "" {
		return errors.New("no brain configured")
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", brainBase+"/tenants", nil)
	req.Header.Set("X-Switch-Key", switchKey)
	resp, err := brainClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("brain /tenants %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	rows, err := tenantIndexBody(raw)
	if err != nil {
		return fmt.Errorf("brain /tenants: %w", err)
	}
	if err := applyTenantIndex(rows, "brain"); err != nil {
		return err
	}
	persistTenantIndex(rows)
	return nil
}

// runTenantEvents keeps the index by the brain's GET /tenants/events: every
// `tenants` event carries the whole list. A drop re-fetches GET /tenants on
// reconnect, with the brain stream's backoff.
func runTenantEvents(ctx context.Context) {
	var wait time.Duration
	for {
		start := time.Now()
		good, err := tenantEventsOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("tenants: /tenants/events ended: %v — the index in force stays until it returns", err)
		}
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

func tenantEventsOnce(parent context.Context) (good bool, err error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	watch := time.AfterFunc(brainSilence, cancel)
	defer watch.Stop()
	req, _ := http.NewRequestWithContext(ctx, "GET", brainBase+"/tenants/events", nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("X-Switch-Key", switchKey)
	resp, err := brainStreamClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("GET /tenants/events: %s", resp.Status)
	}
	body := &heardReader{r: resp.Body, heard: func() { watch.Reset(brainSilence) }}
	err = readSSE(body, func(event, data string) {
		if event != "tenants" {
			return
		}
		rows, derr := tenantIndexBody([]byte(data))
		if derr != nil {
			log.Printf("tenants: bad tenants event: %v — ignored", derr)
			return
		}
		if aerr := applyTenantIndex(rows, "brain"); aerr != nil {
			log.Printf("tenants: refused index from the brain: %v — the index in force stays", aerr)
			return
		}
		good = true
		persistTenantIndex(rows)
	})
	if err == nil {
		err = errors.New("the brain closed /tenants/events")
	}
	return good, err
}

type tenantsStateFile struct {
	SavedAt int64       `json:"saved_at"`
	Tenants []tenantRow `json:"tenants"`
}

// persistTenantIndex writes the index beside the class-rows snapshot, the
// same tmp+rename. A write failure is logged: durability, not correctness.
func persistTenantIndex(rows []tenantRow) {
	if tenantsStateFilePath == "" {
		return
	}
	buf, err := json.Marshal(tenantsStateFile{SavedAt: time.Now().UnixMilli(), Tenants: rows})
	if err != nil {
		log.Printf("tenants: cannot marshal state: %v", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(tenantsStateFilePath), 0o700); err != nil {
		log.Printf("tenants: cannot create state dir: %v", err)
		return
	}
	tmp := tenantsStateFilePath + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o600); err != nil {
		log.Printf("tenants: cannot write state: %v", err)
		return
	}
	if err := os.Rename(tmp, tenantsStateFilePath); err != nil {
		log.Printf("tenants: cannot install state: %v", err)
	}
}

// loadTenantIndexState seeds the index from the last-good file at boot, so a
// brain outage at boot does not blind the household: the proxy serves its
// tenants on cached state (STALE) until the brain's index arrives. Absent
// file: a first boot, nothing to do.
func loadTenantIndexState() {
	if tenantsStateFilePath == "" {
		return
	}
	raw, err := os.ReadFile(tenantsStateFilePath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("tenants: cannot read last-good state %s: %v — starting with no tenants", tenantsStateFilePath, err)
		}
		return
	}
	var f tenantsStateFile
	if err := json.Unmarshal(raw, &f); err != nil {
		log.Printf("tenants: bad JSON in %s: %v — starting with no tenants", tenantsStateFilePath, err)
		return
	}
	if err := applyTenantIndex(f.Tenants, "last-good"); err != nil {
		log.Printf("tenants: last-good state %s refused: %v — starting with no tenants", tenantsStateFilePath, err)
	}
}

// tenantsStatus is /_switch/status's view of the index.
func tenantsStatus() map[string]any {
	membersMu.RLock()
	defer membersMu.RUnlock()
	rows := make([]map[string]any, 0, len(members))
	for _, tn := range members {
		rows = append(rows, map[string]any{"tenant": tn.id, "name": tn.name, "lan": tn.lan, "tap": tn.tap})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i]["tenant"].(string) < rows[j]["tenant"].(string) })
	out := map[string]any{"source": membersSource, "count": len(members), "rows": rows}
	if membersRefusal != nil {
		r := *membersRefusal
		out["refused"] = r
	}
	return out
}

// ---- the request's tenant ----

type tenantCtxKey struct{}

// withTenant attaches the request's tenant. Callers: the entries only.
func withTenant(ctx context.Context, tn *tenant) context.Context {
	return context.WithValue(ctx, tenantCtxKey{}, tn)
}

// tenantFrom is the request's tenant: ONLY the one its entry stamped with
// withTenant — the base-URL entry (:8787, clientGate) stamps the lan tenant,
// the HTTPS-proxy door stamps the login's (its device token's mem claim, or
// the lan tenant for a users-file login). nil for a context nothing stamped
// (a helper reached with a bare or Background context, a door request before
// its stamp), for a door login the index does not name, and when the index
// names no lan tenant: the typed 503, never anyone's accounts. Nothing here
// maps "no stamp" to the household: a member decision made on an unstamped
// context would otherwise be judged against the household's state.
func tenantFrom(ctx context.Context) *tenant {
	if ctx == nil {
		return nil
	}
	tn, _ := ctx.Value(tenantCtxKey{}).(*tenant)
	return tn
}

// ctxOf is ctx carrying tn, for a helper on tn that hands the context on to
// something that reads tenantFrom (a keyed wire's dispense, a hop): the
// receiver is the request's tenant, so the judgment stays its own. A nil ctx
// (a caller with no request: the prober, the status page) becomes Background
// stamped with tn; one already carrying tn is returned as it is.
func (tn *tenant) ctxOf(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if tenantFrom(ctx) == tn {
		return ctx
	}
	return withTenant(ctx, tn)
}

// mustTenant is the request's tenant, or nil after answering the typed 503
// that says a request with no tenant is served from nobody's accounts.
func mustTenant(w http.ResponseWriter, r *http.Request) *tenant {
	if tn := tenantFrom(r.Context()); tn != nil {
		return tn
	}
	writeNoTenant(w, entryFrom(r.Context()).tenant)
	return nil
}

// writeNoTenant answers a routed request whose tenant the index does not
// name: HTTP 503 in the Anthropic error envelope, naming the tenant, so a
// token whose member was removed fails closed, never on another's accounts.
func writeNoTenant(w http.ResponseWriter, id string) {
	msg := product + ": no tenant for this request — refusing to serve it from anyone's accounts"
	if id != "" {
		msg = product + ": tenant " + id + " is unknown to this proxy — refusing to serve it from anyone's accounts"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":  "error",
		"error": map[string]any{"type": "api_error", "message": msg},
	})
}

// noTenantResponse is the transport's answer to a hop with no tenant on its
// context: the same typed 503 writeNoTenant sends, as an upstream reply.
func noTenantResponse(req *http.Request) *http.Response {
	body, _ := json.Marshal(map[string]any{
		"type": "error",
		"error": map[string]any{"type": "api_error",
			"message": product + ": no tenant for this request — refusing to serve it from anyone's accounts"},
	})
	hd := http.Header{"Content-Type": []string{"application/json"}}
	return &http.Response{
		StatusCode: http.StatusServiceUnavailable, Status: "503 Service Unavailable",
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: hd, Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body)), Request: req,
	}
}

// closureKey scopes a door's lane closure (video_api: a lane, or lane/model)
// to the tenant whose account refused: the lan tenant's keys stay bare, so
// the household's status reads as before; another tenant's carry its id.
func (tn *tenant) closureKey(key string) string {
	if tn == nil || tn.lan {
		return key
	}
	return tn.id + "/" + key
}

// departedAccounts is every account the last applied document listed and next
// does not: an account disconnected on the brain (POST /auth/<pid>/remove).
// The first document departs nothing.
func departedAccounts(prev, next map[string]acctStanding) []string {
	var left []string
	for a, was := range prev {
		now, ok := next[a]
		// A re-added account is a new identity: the brain sets its
		// recheck_epoch to the enrolment, so a remove and an add between two
		// documents still drops what was cached for the old credential.
		if !ok || epochAdvanced(was.RecheckEpoch, now.RecheckEpoch) {
			left = append(left, a)
		}
	}
	sort.Strings(left)
	return left
}

// epochAdvanced: the next document's recheck_epoch is newer than the last's.
func epochAdvanced(prev, next *float64) bool {
	return next != nil && (prev == nil || *next > *prev)
}

// dropDeparted forgets every credential this proxy holds for accounts that
// left the tenant's document: the subscription token and its prewarm timer,
// the codex bearer (cached under its pool name, or under "" for the
// incumbent the brain named), and the api key. Without it a cached copy would
// outlive its account by up to its expiry. The refusal records go through
// pruneRecords as before.
func (tn *tenant) dropDeparted(left []string) {
	if len(left) == 0 {
		return
	}
	gone := make(map[string]bool, len(left))
	for _, a := range left {
		gone[a] = true
	}
	// Before the deletes: a fetch that read the count earlier and stores after
	// them finds it moved and caches nothing.
	tn.credDrops.Add(1)
	tn.warmMu.Lock()
	for a := range gone {
		if t := tn.warmTimers[a]; t != nil {
			t.Stop()
			delete(tn.warmTimers, a)
		}
	}
	tn.warmMu.Unlock()
	tn.tokMu.Lock()
	for a := range gone {
		delete(tn.tokens, a)
	}
	tn.tokMu.Unlock()
	tn.codexTokMu.Lock()
	for k, e := range tn.codexToks {
		if gone[k] || (e.name != "" && gone[e.name]) {
			delete(tn.codexToks, k)
		}
	}
	tn.codexTokMu.Unlock()
	tn.apiKeyMu.Lock()
	for a := range gone {
		delete(tn.apiKeys, a)
		delete(tn.apiKeyStates, a)
	}
	tn.apiKeyMu.Unlock()
	log.Printf("tenant %s: dropped the cached credentials of %s (left the brain's document, or re-added)",
		tn.id, strings.Join(left, ", "))
}
