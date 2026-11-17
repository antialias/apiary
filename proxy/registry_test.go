package main

// Tests for the per-model registry (#36): validation, [1m] rendering, the empty
// legacy registry, the always-registered /v1/models route, the advertisement-only
// Anthropic rows (#96), and the typed 503 for recognized-but-unroutable ids.
// The registry carries no provider row: the "named" type was deleted in
// quota-model C2 (§2.2 row 10); e2e_named_deleted_test.go pins its ids' refusal.

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// withModelsFile installs a registry config file for the duration of a test and
// snapshots every registry global.
func withModelsFile(t *testing.T, content string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "models.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	oldPath, oldReg, oldSrc, oldErr := modelsFilePath, registry, registrySource, registryErr
	modelsFilePath = path
	t.Cleanup(func() {
		modelsFilePath, registry, registrySource, registryErr = oldPath, oldReg, oldSrc, oldErr
	})
}

func TestRegistryRenderedID(t *testing.T) {
	e := registryEntry{Name: "opus-5", Type: "anthropic", ContextWindow: 200000}
	if got := e.renderedID(); got != "claude-opus-5" {
		t.Fatalf("renderedID = %q", got)
	}
	e.ContextWindow = 1_000_000
	if got := e.renderedID(); got != "claude-opus-5[1m]" {
		t.Fatalf("renderedID at 1M = %q — the suffix is the client's only window signal", got)
	}
	e.ContextWindow = 999_999
	if got := e.renderedID(); got != "claude-opus-5" {
		t.Fatalf("renderedID just under 1M = %q, must not overclaim", got)
	}
}

// The deleted "named" type is just an unknown type now: each of the four rows
// the deployed models.json carried before C2, verbatim, is dropped with the
// reason logged, so a stale file can neither advertise nor route one.
func TestRegistryNamedTypeIsDropped(t *testing.T) {
	logs := captureLog(t)
	var stale []registryEntry
	if err := json.Unmarshal([]byte(`[
	  {"name": "kimi-k3", "display": "kimi-k3 (kimi)", "type": "named", "provider": "kimi",
	   "upstream_model": "kimi-k3", "context_window": 256000, "anthropic_token_ratio": 0.61,
	   "calibrated_at": "2026-08-12", "calibration_ref": "spikes/context-window-abstraction/FINDINGS.md (C1; #44)"},
	  {"name": "gpt-6.1-sol", "type": "named", "provider": "gpt", "upstream_model": "gpt-6.1-sol",
	   "context_window": 400000, "anthropic_token_ratio": 0.52},
	  {"name": "gpt-6-luna", "type": "named", "provider": "gpt", "upstream_model": "gpt-6-luna",
	   "context_window": 200000, "anthropic_token_ratio": 0.52},
	  {"name": "gpt-6-astra", "type": "named", "provider": "gpt", "upstream_model": "gpt-6-astra",
	   "context_window": 200000, "anthropic_token_ratio": 0.52}
	]`), &stale); err != nil {
		t.Fatal(err)
	}
	if got := validateRegistry(stale); len(got) != 0 {
		t.Fatalf("named rows survived validation: %+v", got)
	}
	for _, name := range []string{"kimi-k3", "gpt-6.1-sol", "gpt-6-luna", "gpt-6-astra"} {
		if !strings.Contains(logs.String(), `dropping entry "`+name+`": type must be class|anthropic`) {
			t.Fatalf("no logged drop reason for %s: %s", name, logs.String())
		}
	}
}

func TestRegistryValidationDrops(t *testing.T) {
	cases := []struct {
		why string
		e   registryEntry
	}{
		{"missing name", registryEntry{Type: "anthropic", ContextWindow: 200000}},
		{"authored [1m]", registryEntry{Name: "fable-5[1m]", Type: "anthropic", ContextWindow: 1_000_000}},
		{"authored [1M] case-insensitive", registryEntry{Name: "fable-5[1M]", Type: "anthropic", ContextWindow: 1_000_000}},
		{"bad type", registryEntry{Name: "x", Type: "lane", ContextWindow: 256000}},
		{"named type", registryEntry{Name: "kimi-k3", Type: "named", Provider: "kimi", UpstreamModel: "kimi-k3",
			ContextWindow: 256000, AnthropicTokenRatio: 0.61}},
		{"anthropic missing window", registryEntry{Name: "fable-5", Type: "anthropic"}},
		{"anthropic digit-free name", registryEntry{Name: "fable", Type: "anthropic", ContextWindow: 200000}},
		// #96: an anthropic row declares NO routing, so every routable field is a
		// hard reject — declared, never inferred.
		{"anthropic with provider", registryEntry{Name: "fable-5", Type: "anthropic", ContextWindow: 200000, Provider: "kimi"}},
		{"anthropic with upstream_model", registryEntry{Name: "fable-5", Type: "anthropic", ContextWindow: 200000, UpstreamModel: "fable-5"}},
		{"anthropic with class", registryEntry{Name: "fable-5", Type: "anthropic", ContextWindow: 200000, Class: "frontier"}},
		{"anthropic with anthropic_token_ratio", registryEntry{Name: "fable-5", Type: "anthropic", ContextWindow: 200000, AnthropicTokenRatio: 1}},
	}
	for _, c := range cases {
		if got := validateRegistry([]registryEntry{c.e}); len(got) != 0 {
			t.Fatalf("%s: entry survived validation: %+v", c.why, got)
		}
	}
}

func TestRegistryCollisionFirstWins(t *testing.T) {
	got := validateRegistry([]registryEntry{
		{Name: "fable-5", Display: "first", Type: "anthropic", ContextWindow: 200000},
		{Name: "fable-5", Display: "second", Type: "anthropic", ContextWindow: 200000},
	})
	if len(got) != 1 || got[0].Display != "first" {
		t.Fatalf("collision: got %+v, want first entry only", got)
	}
}

// captureLog redirects the package logger for one test. validateRegistry
// returns only the survivors, so a drop REASON is only assertable from the log.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(old) })
	return &buf
}

// #96: anthropic rows are advertisement only — name + window, nothing routable,
// so they can never be quarantined. The two fable rows differ ONLY in window:
// the renderer's [1m] suffix is what separates their ids.
func TestRegistryAnthropicRows(t *testing.T) {
	got := validateRegistry([]registryEntry{
		{Name: "fable-5", Display: "Claude Fable 5", Type: "anthropic", ContextWindow: 200000},
		{Name: "fable-5", Display: "Claude Fable 5 (1M)", Type: "anthropic", ContextWindow: 1_000_000},
		{Name: "opus-5", Type: "anthropic", ContextWindow: 200000},
	})
	if len(got) != 3 {
		t.Fatalf("validated %+v, want all three anthropic rows", got)
	}
	if got[0].renderedID() != "claude-fable-5" || got[1].renderedID() != "claude-fable-5[1m]" {
		t.Fatalf("rendered ids = %q/%q, want the window to separate the two fable rows",
			got[0].renderedID(), got[1].renderedID())
	}
	if got[2].Display != "claude-opus-5" {
		t.Fatalf("anthropic display default = %q, want the rendered id", got[2].Display)
	}
	for i := range got {
		if got[i].disabledReason != "" {
			t.Fatalf("anthropic row quarantined (%+v) — it declares nothing that could be unconfigured", got[i])
		}
	}
}

// The digit rule is load-bearing, so the refusal has to SAY why: a digit-free
// rendered id sits in the class-shaped namespace, where rule 3 answers a typed
// 503 — advertising one would advertise an id this proxy refuses to route.
func TestRegistryAnthropicDigitFreeNameRejected(t *testing.T) {
	logs := captureLog(t)
	got := validateRegistry([]registryEntry{
		{Name: "fable", Display: "Claude Fable", Type: "anthropic", ContextWindow: 200000},
	})
	if len(got) != 0 {
		t.Fatalf("digit-free anthropic row survived validation: %+v", got)
	}
	if !strings.Contains(logs.String(), "class-shaped refusal namespace") {
		t.Fatalf("drop reason must name the namespace it would collide with, got: %s", logs.String())
	}
	// and the rule is exactly the namespace boundary the refusal uses
	if !classShapedModel("claude-fable") || classShapedModel("claude-fable-5") {
		t.Fatal("the digit rule and classShapedModel must agree on where the class namespace ends")
	}
}

// A stray routable field on an anthropic row is REFUSED, not ignored (#96).
// Advertisement-only means the row can carry no authority: the id routes by
// lane, so a provider never claims it, an upstream_model never rewrites, a
// class never binds, and a ratio never meters. Silently dropping the field
// would advertise a row whose config lies about how it is served.
func TestRegistryAnthropicRoutableFieldsRejected(t *testing.T) {
	base := registryEntry{Name: "fable-5", Display: "Claude Fable 5", Type: "anthropic", ContextWindow: 200000}
	for _, c := range []struct {
		field string
		e     registryEntry
	}{
		{"provider", func() registryEntry { e := base; e.Provider = "kimi"; return e }()},
		{"upstream_model", func() registryEntry { e := base; e.UpstreamModel = "fable-5"; return e }()},
		{"class", func() registryEntry { e := base; e.Class = "frontier"; return e }()},
		{"anthropic_token_ratio", func() registryEntry { e := base; e.AnthropicTokenRatio = 1; return e }()},
	} {
		logs := captureLog(t)
		if got := validateRegistry([]registryEntry{c.e}); len(got) != 0 {
			t.Fatalf("anthropic row carrying %s survived validation: %+v", c.field, got)
		}
		if !strings.Contains(logs.String(), "advertisement only") {
			t.Fatalf("%s: drop reason must say the type carries no routing, got: %s", c.field, logs.String())
		}
	}
	// the clean row is unaffected — the strictness is about the stray fields only
	if got := validateRegistry([]registryEntry{base}); len(got) != 1 {
		t.Fatalf("clean anthropic row dropped: %+v", got)
	}
}

// An advertised anthropic id must stay INVISIBLE to every refusal: it routes
// by lane, like an id nobody advertised (#96).
func TestAnthropicRowNeitherRefusedNorClaimed(t *testing.T) {
	oldReg := registry
	registry = validateRegistry([]registryEntry{
		{Name: "fable-5", Display: "Claude Fable 5", Type: "anthropic", ContextWindow: 200000},
	})
	t.Cleanup(func() { registry = oldReg })
	withClassRows(t, "")
	if e, ok := matchClassTombstone("claude-fable-5"); ok {
		t.Fatalf("advertised anthropic id quarantined: %+v", e)
	}
	if _, ok := matchRegistryClass("claude-fable-5"); ok {
		t.Fatal("anthropic row must not match as a class row")
	}
	if _, _, ok := classShapedRefusalReason("claude-fable-5"); ok {
		t.Fatal("anthropic id must not fall into the class-shaped refusal")
	}
	if p, ok := hostedProviderNamespace("claude-fable-5"); ok {
		t.Fatalf("anthropic id fell into the %s direct-id refusal", p)
	}
}

// #93 grace: a not-yet-migrated models.json still carrying class entries has
// them logged and IGNORED (class rows derive from /preferred now) — never
// registered, never advertised alongside a derived row for the same class.
func TestRegistryClassEntriesIgnoredWithGrace(t *testing.T) {
	got := validateRegistry([]registryEntry{
		{Name: "fable-5", Type: "anthropic", ContextWindow: 200000},
		{Name: "frontier", Type: "class", Class: "frontier", ContextWindow: 200000},
		{Name: "fast", Type: "class", ContextWindow: 128000}, // even malformed class shapes are just ignored
	})
	if len(got) != 1 || got[0].Name != "fable-5" {
		t.Fatalf("class entries must be ignored, got %+v", got)
	}
}

// No file is the empty registry: only the class rows advertise. Nothing is synthesized from the provider table any more.
func TestLegacyRegistryIsEmpty(t *testing.T) {
	entries, src, errStr := loadRegistry("")
	if src != "legacy" || errStr != "" || len(entries) != 0 {
		t.Fatalf("no-file load = (%+v, %s, %q), want a clean empty legacy registry", entries, src, errStr)
	}
}

func TestLoadRegistryFileErrors(t *testing.T) {
	if entries, src, errStr := loadRegistry(filepath.Join(t.TempDir(), "absent.json")); src != "legacy" || errStr != "" || len(entries) != 0 {
		t.Fatalf("absent file = (%+v, %s, %q), want clean empty legacy (a normal mode)", entries, src, errStr)
	}
	bad := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(bad, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if entries, src, errStr := loadRegistry(bad); src != "legacy" || errStr == "" || len(entries) != 0 {
		t.Fatalf("malformed file = (%+v, %s, %q), want empty legacy WITH the error surfaced", entries, src, errStr)
	}
}

func TestAdvertisableConfigured(t *testing.T) {
	oldProv, oldReg, oldBrain := providers, registry, brainBase
	t.Cleanup(func() { providers, registry, brainBase = oldProv, oldReg, oldBrain })
	withClassRows(t, "")

	providers, registry, brainBase = nil, nil, ""
	if advertisableConfigured() {
		t.Fatal("nothing configured must not be advertisable")
	}
	// A provider advertises nothing of its own (quota-model C2; local since #135).
	for _, p := range []*provider{
		{name: "local", target: mustURL(t, "http://127.0.0.1:11434"), wire: localWire{}},
		{name: "kimi", target: mustURL(t, "http://x"), wire: bearerWire{key: "k"}},
	} {
		providers = []*provider{p}
		if advertisableConfigured() {
			t.Fatalf("provider %s alone must not be advertisable — it has no picker rows", p.name)
		}
	}
	providers = nil
	registry = []registryEntry{{Name: "fable-5", Type: "anthropic", ContextWindow: 200000}}
	if !advertisableConfigured() {
		t.Fatal("an enabled registry entry is advertisable")
	}
	// #93: class rows are config-shaped advertisable too — a configured brain
	// will serve them, and a boot-loaded last-good active row already does.
	registry = nil
	brainBase = "http://brain:9115"
	if !advertisableConfigured() {
		t.Fatal("a configured brain must count (class rows derive from it)")
	}
	brainBase = ""
	withClassRows(t, "last-good", registryEntry{Name: "frontier", Type: "class", Class: "frontier", ContextWindow: 200000})
	if !advertisableConfigured() {
		t.Fatal("a boot-loaded active class row must count")
	}
	tomb := registryEntry{Name: "frontier", Type: "class", Class: "frontier", ContextWindow: 200000}
	tomb.disabledReason = classRemovedReason
	withClassRows(t, "last-good", tomb)
	if advertisableConfigured() {
		t.Fatal("only-tombstoned class rows must not be advertisable")
	}
}

// withClassRows swaps the derived class-row store for the duration of a test.
func withClassRows(t *testing.T, source string, rows ...registryEntry) {
	t.Helper()
	classRowsMu.Lock()
	oldRows, oldSrc := classRowsSet, classRowsSource
	classRowsSet, classRowsSource = rows, source
	classRowsMu.Unlock()
	t.Cleanup(func() {
		classRowsMu.Lock()
		classRowsSet, classRowsSource = oldRows, oldSrc
		classRowsMu.Unlock()
	})
}

func TestMatchClassTombstone(t *testing.T) {
	tomb := registryEntry{Name: "helpdesk", Type: "class", Class: "helpdesk", ContextWindow: 200000}
	tomb.disabledReason = classRemovedReason
	withClassRows(t, "preferred",
		registryEntry{Name: "frontier", Type: "class", Class: "frontier", ContextWindow: 200000}, tomb)

	if _, ok := matchClassTombstone("claude-frontier"); ok {
		t.Fatal("active class row must not match as unroutable")
	}
	if e, ok := matchClassTombstone("claude-helpdesk"); !ok || e.disabledReason != classRemovedReason {
		t.Fatalf("tombstoned class row = (%v, %+v), want the removed quarantine (#93 rule 2)", ok, e)
	}
	if e, ok := matchRegistryClass("claude-frontier"); !ok || e.Class != "frontier" {
		t.Fatalf("class match = (%v, %+v)", ok, e)
	}
	if _, ok := matchRegistryClass("claude-helpdesk"); ok {
		t.Fatal("tombstoned class row must not match as an active class")
	}
	if _, ok := matchClassTombstone(""); ok {
		t.Fatal("empty model must not match")
	}
	if _, ok := matchRegistryClass(""); ok {
		t.Fatal("empty model must not match as class")
	}
}

// ---- through the real handler stack ----

// rebuildProxy swaps the env's proxy server for one built from the current globals.
func rebuildProxy(env *e2eEnv) {
	env.proxy.Close()
	// Keep env.core in step with the handler actually serving (#126): a test
	// that wires the HTTPS-proxy entry to env.core must get the chain this
	// rebuild just produced, not the one it replaced.
	gated, core := buildHandlers()
	env.core = core
	env.proxy = httptest.NewServer(gated)
}

func getModels(t *testing.T, base string) []string {
	t.Helper()
	resp, err := http.Get(base + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("/v1/models status %d", resp.StatusCode)
	}
	var d struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for _, m := range d.Data {
		ids = append(ids, m.ID)
	}
	return ids
}

func TestE2EModelsRouteAlwaysRegistered(t *testing.T) {
	// Zero providers, no registry file: /v1/models must answer {"data":[]} locally —
	// never fall through to the account path (bearer-injected, brain-dependent).
	env, restore := newE2E(t)
	defer restore()
	oldLocal := localUpstream
	localUpstream = ""
	t.Cleanup(func() { localUpstream = oldLocal; providers = nil })
	rebuildProxy(env)

	ids := getModels(t, env.proxy.URL)
	if len(ids) != 0 {
		t.Fatalf("advertised %v, want none", ids)
	}
	select {
	case leaked := <-env.seen:
		t.Fatalf("/v1/models leaked upstream: %+v", leaked)
	default:
	}
}

func TestE2EReservedDelegationUnknownTyped503(t *testing.T) {
	// The entire workload-class namespace is fail-closed (#93 rule 3 generalizes
	// the old claude-delegate-* reserved prefix). Typos, stale picker rows,
	// removed classes, and suffix mismatches fail locally instead of being
	// inferred as an Anthropic lane.
	env, restore := newE2E(t)
	defer restore()
	oldLocal := localUpstream
	localUpstream = ""
	t.Cleanup(func() { localUpstream = oldLocal; providers = nil })
	rebuildProxy(env)

	for _, model := range []string{
		"claude-fast",
		"claude-delegate-capabel",
		"claude-capable[1m]",
		"claude-helpdesk", // no reserved prefix needed anymore: any class-shaped id refuses
	} {
		resp := env.post(t, model, "client-bearer")
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("%s: status %d, want 503", model, resp.StatusCode)
		}
		if !strings.Contains(string(b), "not routable") {
			t.Fatalf("%s: typed 503 lacks reserved reason: %s", model, b)
		}
		if resp.Header.Get("Retry-After") != "" {
			t.Fatalf("%s: a permanent refusal must not advertise a retry", model)
		}
	}
	select {
	case leaked := <-env.seen:
		t.Fatalf("reserved delegation id leaked to the Anthropic upstream: %+v", leaked)
	default:
	}
}

func TestE2EReservedDelegationMalformedRowTyped503(t *testing.T) {
	// A class row still sitting in models.json is ignored at load (#93 grace),
	// and its id is caught by the namespace guard rather than becoming fail-open.
	env, restore := newE2E(t)
	defer restore()
	oldLocal := localUpstream
	localUpstream = ""
	t.Cleanup(func() { localUpstream = oldLocal; providers = nil })
	withModelsFile(t, `[{"name":"fast","type":"class","context_window":200000}]`)
	rebuildProxy(env)

	resp := env.post(t, "claude-fast", "client-bearer")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", resp.StatusCode)
	}
	select {
	case leaked := <-env.seen:
		t.Fatalf("dropped delegation row leaked to the Anthropic upstream: %+v", leaked)
	default:
	}
}

func TestE2EStaleNamedRowsTyped503(t *testing.T) {
	// A deployment whose models.json was not redeployed still carries the deleted named
	// rows. They are dropped at load — not advertised, not quarantined, no
	// routing authority — and their ids get the deleted-id refusal, never the
	// Anthropic account path, even with the hosted providers configured.
	withModelsFile(t, `[{"name":"kimi-k3","type":"named","provider":"kimi",
		"upstream_model":"kimi-k3","context_window":256000,"anthropic_token_ratio":0.61},
		{"name":"fable-5","display":"Claude Fable 5","type":"anthropic","context_window":200000}]`)
	h, restore := newE2EHosted(t, "")
	defer restore()

	if ids := getModels(t, h.proxy.URL); !reflect.DeepEqual(ids, []string{"claude-fable-5"}) {
		t.Fatalf("advertised %v, want only the anthropic row — the named row must be gone", ids)
	}
	if len(registry) != 1 || registry[0].Name != "fable-5" {
		t.Fatalf("registry = %+v, want the named row dropped, not quarantined", registry)
	}
	wantDeletedIDRefusal(t, "claude-kimi-k3", h.post(t, "claude-kimi-k3", "client-bearer"))
	h.noAnthropicLeak(t)
	h.noHostedHit(t)
}

func TestE2EAnthropicRowsAdvertisedAlongsideClass(t *testing.T) {
	// #96: gateway discovery treats /v1/models as the whole world and silently
	// substitutes another model for a subagent that asks for a missing id, so the
	// real Anthropic ids ride the same payload as the class rows — including the
	// [1m] variant the renderer derives from the declared window.
	withModelsFile(t, `[{"name":"fable-5","display":"Claude Fable 5","type":"anthropic","context_window":200000},
		{"name":"fable-5","display":"Claude Fable 5 (1M)","type":"anthropic","context_window":1000000}]`)
	h, restore := newE2EHosted(t, "")
	defer restore()
	// seed AFTER the harness: newE2E resets the class-row store on setup
	withClassRows(t, "last-good",
		registryEntry{Name: "frontier", Display: "Frontier (auto)", Type: "class", Class: "frontier", ContextWindow: 200000})

	// MEMBERSHIP, not order: the client keys the picker by id, so which source
	// emits first is an implementation detail this test must not freeze.
	ids := getModels(t, h.proxy.URL)
	want := map[string]bool{"claude-fable-5": true, "claude-fable-5[1m]": true, "claude-frontier": true}
	got := map[string]bool{}
	for _, id := range ids {
		got[id] = true
	}
	if len(ids) != len(got) {
		t.Fatalf("advertised %v contains a duplicate id", ids)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("advertised %v, want exactly the anthropic (both windows) and class rows %v", ids, want)
	}
}

func TestE2ERegistryStatusKey(t *testing.T) {
	// The file carries anthropic rows; the class row rides in the DERIVED store
	// (#93) and the status view folds both into one entries list, tombstones
	// included, with class_source saying where the class rows came from. No
	// entry carries routing or calibration: those were the named rows' (C2).
	withModelsFile(t, `[{"name":"fable-5","display":"Claude Fable 5","type":"anthropic","context_window":200000}]`)
	h, restore := newE2EHosted(t, "")
	defer restore()
	// seed AFTER the harness: newE2E resets the class-row store on setup
	tomb := registryEntry{Name: "helpdesk", Type: "class", Class: "helpdesk", ContextWindow: 200000}
	tomb.disabledReason = classRemovedReason
	withClassRows(t, "last-good",
		registryEntry{Name: "frontier", Type: "class", Class: "frontier", Display: "Frontier (auto)", ContextWindow: 200000},
		tomb)

	resp, err := http.Get(h.proxy.URL + "/_switch/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var d struct {
		Registry struct {
			Source      string           `json:"source"`
			Error       string           `json:"error"`
			ClassSource string           `json:"class_source"`
			Entries     []map[string]any `json:"entries"`
		} `json:"registry"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		t.Fatal(err)
	}
	if d.Registry.Source != "file" || d.Registry.Error != "" {
		t.Fatalf("registry source/error = %q/%q", d.Registry.Source, d.Registry.Error)
	}
	if d.Registry.ClassSource != "last-good" {
		t.Fatalf("class_source = %q, want last-good", d.Registry.ClassSource)
	}
	if len(d.Registry.Entries) != 3 {
		t.Fatalf("entries = %+v", d.Registry.Entries)
	}
	for _, e := range d.Registry.Entries {
		for _, gone := range []string{"provider", "upstream_model", "metering_key",
			"anthropic_token_ratio", "calibrated_at", "calibration_ref"} {
			if _, ok := e[gone]; ok {
				t.Fatalf("entry %v still carries %q — ratio and calibration are the brain's now", e["id"], gone)
			}
		}
	}
	fable, class, dead := d.Registry.Entries[0], d.Registry.Entries[1], d.Registry.Entries[2]
	if fable["id"] != "claude-fable-5" || fable["type"] != "anthropic" || fable["enabled"] != true ||
		fable["context_window"] != float64(200000) {
		t.Fatalf("anthropic entry = %+v", fable)
	}
	if class["id"] != "claude-frontier" || class["enabled"] != true {
		t.Fatalf("class entry = %+v", class)
	}
	if dead["id"] != "claude-helpdesk" || dead["enabled"] != false || dead["disabled_reason"] != classRemovedReason {
		t.Fatalf("tombstone entry = %+v (want disabled with reason %q)", dead, classRemovedReason)
	}
}

// #120: the "[1m]" floor is a knob, and what it compares against is the window
// in CLIENT units. Anthropic rows share the client's tokenizer; a class row's
// window is the brain's, already native/r_p and min across rungs.
func TestRegistryOneMegaMarkFloor(t *testing.T) {
	defer func(prev int) { oneMegaMarkFloor = prev }(oneMegaMarkFloor)

	small := registryEntry{Name: "haiku-4-5", Type: "anthropic", ContextWindow: 200000}
	big := registryEntry{Name: "fable-5", Type: "anthropic", ContextWindow: 400000}
	class := registryEntry{Name: "capable", Type: "class", Class: "capable", ContextWindow: 400000}

	// Default floor = the literal pre-#120 rule: nothing below 1M is marked.
	for _, e := range []registryEntry{small, big, class} {
		if strings.HasSuffix(e.renderedID(), "[1m]") {
			t.Fatalf("%s marked at the default floor — the 1M rule must be unchanged until the floor moves", e.Name)
		}
	}

	oneMegaMarkFloor = 300000
	if got := big.renderedID(); got != "claude-fable-5[1m]" {
		t.Fatalf("400k anthropic row rendered %q at a 300k floor, want the suffix", got)
	}
	if got := small.renderedID(); got != "claude-haiku-4-5" {
		t.Fatalf("200k anthropic row rendered %q at a 300k floor, want it unmarked", got)
	}
	// The brain already derives each member's effective window as native/r_p and
	// publishes MIN across the rungs (claude_usage_exporter.py _normalize_classes),
	// so a class row's number is already the safe bound in client units.
	if got := class.renderedID(); got != "claude-capable[1m]" {
		t.Fatalf("class row rendered %q — a 400k brain-declared window clears a 300k floor", got)
	}

	// The floor is a floor, not a window: a real window still has to clear it.
	oneMegaMarkFloor = 500000
	if got := big.renderedID(); got != "claude-fable-5" {
		t.Fatalf("400k row rendered %q at a 500k floor", got)
	}
}
