package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Model discovery's kind + effort contract (launch-sets §15.2).
//
// /v1/models is the only model list in the system: the session manager renders
// its launch picker from it and nothing keeps a copy. A row therefore has to
// say what it IS (kind) and whether Claude Code's effort setting actually
// reaches it through this proxy (effort). effort is a statement about the
// PROXY's handling, not the model's own capability — see advertisedModel.

type modelsRow struct {
	Type        string `json:"type"`
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	Kind        string `json:"kind"`
	Effort      bool   `json:"effort"`

	Family        string `json:"family"`
	Version       string `json:"version"`
	ContextWindow int    `json:"context_window"`
}

func modelsRows(t *testing.T) map[string]modelsRow {
	t.Helper()
	rec := httptest.NewRecorder()
	modelsHandler(rec, httptest.NewRequest("GET", "/v1/models", nil))
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	var out struct {
		Data []modelsRow `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("bad json: %v (%s)", err, rec.Body.String())
	}
	rows := map[string]modelsRow{}
	for _, r := range out.Data {
		rows[r.ID] = r
	}
	return rows
}

// anthropicMember / providerMember are the two member kinds a class can point
// at, and the only thing that decides a class row's effort.
func anthropicMember(id string) classMemberPref {
	return classMemberPref{ID: id, Kind: kindAnthropic, Lane: "default"}
}

func providerMember(id, provider, model string) classMemberPref {
	return classMemberPref{ID: id, Kind: "provider", Provider: provider, Model: model}
}

// One row of every kind in a single answer, each with its own effort verdict.
func TestModelsHandlerKindAndEffortPerKind(t *testing.T) {
	// The providers are configured but add no row (quota-model C2; local
	// since #135): a provider is reached only as a class member.
	local := &provider{name: "local", target: mustURL(t, "http://127.0.0.1:11434"), wire: localWire{}}
	kimi := &provider{name: "kimi", target: mustURL(t, "https://kimi.example/anthropic"),
		wire: bearerWire{key: "test-only-key"}}
	gpt := &provider{name: "gpt", target: mustURL(t, "https://codex.example"), wire: codexWire{}}
	withProviders(t, local, kimi, gpt)

	oldReg := registry
	registry = []registryEntry{
		{Name: "opus-5-5", Display: "Claude Opus 5.5", Type: "anthropic", ContextWindow: 200000},
	}
	t.Cleanup(func() { registry = oldReg })

	withClassRows(t, "preferred",
		registryEntry{Name: "frontier", Display: "Frontier", Type: "class", Class: "frontier",
			ContextWindow: 200000},
		registryEntry{Name: "local", Display: "Local (LAN PC)", Type: "class", Class: "local",
			ContextWindow: 65536})
	withRouterView(t, true, time.Now(), map[string]classPref{
		"frontier": {Members: []classMemberPref{anthropicMember("opus-5-5")}},
		"local":    {Members: []classMemberPref{providerMember("local/local-gemma4-12b", "local", "local-gemma4-12b")}},
	})

	rows := modelsRows(t)
	if len(rows) != 3 {
		t.Fatalf("/v1/models = %v, want exactly one anthropic and two class rows", rows)
	}
	for id, want := range map[string]modelsRow{
		// family/version are anthropic-only; contextWindow is the declared
		// window, a class's below 200k included (#135).
		"claude-local": {Kind: kindClass, Effort: false, ContextWindow: 65536},
		"claude-opus-5-5": {Kind: kindAnthropic, Effort: true,
			Family: "opus", Version: "5.5", ContextWindow: 200000},
		"claude-frontier": {Kind: kindClass, Effort: true, ContextWindow: 200000},
	} {
		got, ok := rows[id]
		if !ok {
			t.Fatalf("%q missing from /v1/models; got %v", id, rows)
		}
		if got.Kind != want.Kind || got.Effort != want.Effort {
			t.Errorf("%q: kind=%q effort=%v, want kind=%q effort=%v",
				id, got.Kind, got.Effort, want.Kind, want.Effort)
		}
		if got.Family != want.Family || got.Version != want.Version {
			t.Errorf("%q: family=%q version=%q, want %q/%q", id, got.Family, got.Version,
				want.Family, want.Version)
		}
		if got.ContextWindow != want.ContextWindow {
			t.Errorf("%q: context_window=%d, want %d", id, got.ContextWindow, want.ContextWindow)
		}
		// The additions are additive: the three original fields stay.
		if got.Type != "model" || got.ID == "" || got.DisplayName == "" {
			t.Errorf("%q lost a pre-existing field: %+v", id, got)
		}
	}
}

// A class is only as effort-capable as the member it currently points at, and
// that pointer moves while the proxy runs.
func TestClassRowEffortFollowsItsTarget(t *testing.T) {
	withProviders(t) // no providers: a class row needs none to be advertised
	oldReg := registry
	registry = nil
	t.Cleanup(func() { registry = oldReg })
	withClassRows(t, "preferred",
		registryEntry{Name: "frontier", Display: "Frontier", Type: "class", Class: "frontier",
			ContextWindow: 200000})

	cases := []struct {
		name    string
		classes map[string]classPref
		want    bool
	}{
		{"anthropic target", map[string]classPref{
			"frontier": {Members: []classMemberPref{anthropicMember("opus-5-5")}}}, true},
		{"gpt provider target", map[string]classPref{
			"frontier": {Members: []classMemberPref{providerMember("gpt-6.1-sol", "gpt", "gpt-6.1-sol")}}}, true},
		{"kimi provider target", map[string]classPref{
			"frontier": {Members: []classMemberPref{providerMember("kimi-k3", "kimi", "kimi-k3")}}}, false},
		// The brain's pointer wins over priority order in both directions, so a
		// class whose top member is anthropic but whose pointer sits on a
		// Kimi provider must report false (the request goes where the pointer says).
		{"pointer on the provider member", map[string]classPref{
			"frontier": {Pointer: ptrTo("kimi-k3"), Members: []classMemberPref{
				anthropicMember("opus-5-5"), providerMember("kimi-k3", "kimi", "kimi-k3")}}}, false},
		{"pointer on the gpt member", map[string]classPref{
			"frontier": {Pointer: ptrTo("gpt-6.1-sol"), Members: []classMemberPref{
				providerMember("kimi-k3", "kimi", "kimi-k3"), providerMember("gpt-6.1-sol", "gpt", "gpt-6.1-sol")}}}, true},
		{"pointer on the anthropic member", map[string]classPref{
			"frontier": {Pointer: ptrTo("opus-5-5"), Members: []classMemberPref{
				providerMember("kimi-k3", "kimi", "kimi-k3"), anthropicMember("opus-5-5")}}}, true},
		// Unknown target (brain-down cold, or a class the last /preferred did
		// not carry): the row still advertises from last-good, but we cannot
		// claim the setting lands.
		{"no member view", map[string]classPref{"frontier": {}}, false},
		{"class absent from /preferred", map[string]classPref{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			withRouterView(t, true, time.Now(), c.classes)
			rows := modelsRows(t)
			got, ok := rows["claude-frontier"]
			if !ok {
				t.Fatalf("class row missing from /v1/models: %v", rows)
			}
			if got.Kind != kindClass {
				t.Fatalf("kind = %q, want %q", got.Kind, kindClass)
			}
			if got.Effort != c.want {
				t.Fatalf("effort = %v, want %v", got.Effort, c.want)
			}
		})
	}
}

func ptrTo(s string) *string { return &s }

// The picker cache Claude Code actually reads keeps EXACTLY its old shape: the
// client writes {id, display_name} rows and compares the file against what it
// would have fetched. An extra key here would be a difference the client did
// not put there.
func TestClientModelsCacheShapeUnchangedByKinds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway-models.json")
	models := []advertisedModel{
		{pickerID: "claude-opus-5-5", display: "Claude Opus 5.5", kind: kindAnthropic, effort: true},
		{pickerID: "claude-frontier", display: "Frontier", kind: kindClass, effort: true},
	}
	if wrote, err := writeClientModelsCache(path, "http://127.0.0.1:8787", models); err != nil || !wrote {
		t.Fatalf("writeClientModelsCache: wrote=%v err=%v", wrote, err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		BaseURL   string                       `json:"baseUrl"`
		FetchedAt int64                        `json:"fetchedAt"`
		Models    []map[string]json.RawMessage `json:"models"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&file); err != nil {
		t.Fatalf("cache file grew a key the client does not know: %v (%s)", err, raw)
	}
	if len(file.Models) != 2 {
		t.Fatalf("models = %v", file.Models)
	}
	for _, m := range file.Models {
		if len(m) != 2 || m["id"] == nil || m["display_name"] == nil {
			t.Fatalf("row keys changed: %v (want exactly id + display_name)", m)
		}
	}
}

// ---- what an Anthropic row is (launch-sets §15.3) ----

func TestAnthropicNameParts(t *testing.T) {
	cases := []struct {
		name            string
		family, version string
		ok              bool
	}{
		{"opus-5", "opus", "5", true},
		{"sonnet-5", "sonnet", "5", true},
		{"fable-5-1", "fable", "5.1", true},
		// A snapshot date is not part of the version: two snapshots of 4.5 are
		// the same version, and a consumer ranking "4.5" against "4.520251001"
		// would compare a date as a version number.
		{"haiku-4-5-20251001", "haiku", "4.5", true},
		{"opus-5-1-20260501", "opus", "5.1", true},
		// Not rankable, and the proxy must say so rather than guess.
		{"frontier", "", "", false},    // no version at all
		{"opus", "", "", false},        // family alone
		{"k2-pro-1", "", "", false},    // family outside the closed set
		{"gpt-5.6-sol", "", "", false}, // a provider name, not an Anthropic one
		{"opus-latest", "", "", false}, // no numeric version
		{"Opus-5", "", "", false},      // the registry's names are lowercase
	}
	for _, c := range cases {
		family, version, ok := anthropicNameParts(c.name)
		if ok != c.ok || family != c.family || version != c.version {
			t.Errorf("anthropicNameParts(%q) = (%q, %q, %v), want (%q, %q, %v)",
				c.name, family, version, ok, c.family, c.version, c.ok)
		}
	}
}

// The deployed models.json, not a fixture: a new Anthropic entry whose name
// does not yield a family and a version ships a row no consumer can rank
// (§15.3's "latest Opus" is family+version+context_window), so it must fail
// here rather than in a launch picker.
func TestRealModelsJSONAnthropicRowsAreRankable(t *testing.T) {
	raw, err := os.ReadFile("models.json")
	if err != nil {
		t.Fatal(err)
	}
	var entries []registryEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("models.json does not parse: %v", err)
	}
	oldReg := registry
	registry = validateRegistry(entries)
	t.Cleanup(func() { registry = oldReg })
	// Every row of the deployed file survives validation: a dropped row is a
	// model gateway discovery would silently substitute.
	if len(registry) != len(entries) {
		t.Fatalf("models.json: %d of %d entries survive validation (see the drop log)", len(registry), len(entries))
	}

	rows := registryAnthropicRows()
	if len(rows) == 0 {
		t.Fatal("models.json advertises no anthropic rows — this test would pass vacuously")
	}
	for _, m := range rows {
		if m.kind != kindAnthropic || !m.effort {
			t.Errorf("%s: kind=%q effort=%v, want %q/true", m.pickerID, m.kind, m.effort, kindAnthropic)
		}
		if m.family == "" || m.version == "" {
			t.Errorf("%s: family=%q version=%q — an anthropic row must yield both; name it "+
				"<family>-<major>[-<minor>][-<date>] with family in opus|sonnet|fable|haiku",
				m.pickerID, m.family, m.version)
		}
		if m.contextWindow <= 0 {
			t.Errorf("%s: context_window=%d, want the declared effective window", m.pickerID, m.contextWindow)
		}
	}
	// The window a row reports and the mark on its id are one number, so they
	// cannot drift apart.
	for _, m := range rows {
		marked := strings.HasSuffix(m.pickerID, "[1m]")
		if marked != (m.contextWindow >= oneMegaMarkFloor) {
			t.Errorf("%s: [1m]=%v but context_window=%d (floor %d) — the mark and the number disagree",
				m.pickerID, marked, m.contextWindow, oneMegaMarkFloor)
		}
	}
}
