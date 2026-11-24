package main

// Tests for the brain-derived class picker rows (#93 / B2): derivation from a
// /preferred classes payload, the tombstone lifecycle, last-good persistence
// (boot-from-file with the brain down), and the generalized class-shape refusal
// heuristic — including the edge the false-positive cost makes sacred: every
// real Anthropic model id must pass through untouched.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// withClassStateFile points the last-good persistence at a test-owned path:
// the configured file, and the household's (the LAN tenant's own) with it.
func withClassStateFile(t *testing.T, path string) {
	t.Helper()
	old := classStateFilePath
	classStateFilePath = path
	t.Cleanup(func() { classStateFilePath = old })
}

func TestDeriveClassRowsBasics(t *testing.T) {
	classes := map[string]classPref{
		"frontier":  {Display: "Frontier (auto)", ContextWindow: 200000},
		"helpdesk":  {ContextWindow: 256000},  // no display -> defaults to the id
		"bigbrain":  {ContextWindow: 1500000}, // >= 1M -> "[1m]" renders
		"tinyclass": {ContextWindow: 65536},   // below 200k -> advertised as declared (#135)
	}
	rows := deriveClassRows(classes, nil, nil)
	if len(rows) != 4 {
		t.Fatalf("derived %d rows, want 4: %+v", len(rows), rows)
	}
	// sorted by name: bigbrain, frontier, helpdesk, tinyclass
	if rows[0].renderedID() != "claude-bigbrain[1m]" {
		t.Fatalf("1M class rendered %q, want the [1m] suffix (the client's only window signal)", rows[0].renderedID())
	}
	if rows[1].renderedID() != "claude-frontier" || rows[1].Display != "Frontier (auto)" || rows[1].ContextWindow != 200000 {
		t.Fatalf("frontier row = %+v", rows[1])
	}
	if rows[2].Display != "helpdesk" {
		t.Fatalf("missing display must default to the class id, got %q", rows[2].Display)
	}
	if rows[3].renderedID() != "claude-tinyclass" || rows[3].ContextWindow != 65536 {
		t.Fatalf("sub-200k row = %s %d, want claude-tinyclass advertised as declared, 65536",
			rows[3].renderedID(), rows[3].ContextWindow)
	}
	for _, r := range rows {
		if r.Type != "class" || r.Class != r.Name || r.disabledReason != "" {
			t.Fatalf("derived row malformed: %+v", r)
		}
	}
}

func TestDeriveClassRowsTombstoneLifecycle(t *testing.T) {
	// active
	rows := deriveClassRows(map[string]classPref{"frontier": {Display: "Frontier", ContextWindow: 200000}}, nil, nil)
	if len(rows) != 1 || rows[0].disabledReason != "" {
		t.Fatalf("active derive = %+v", rows)
	}
	// vanished -> tombstone, window and display preserved
	rows = deriveClassRows(map[string]classPref{}, rows, nil)
	if len(rows) != 1 || rows[0].disabledReason != classRemovedReason {
		t.Fatalf("vanish derive = %+v, want one tombstone", rows)
	}
	if rows[0].ContextWindow != 200000 || rows[0].Display != "Frontier" {
		t.Fatalf("tombstone must preserve the row it replaces: %+v", rows[0])
	}
	// still vanished -> tombstone carries FORWARD across refreshes
	rows = deriveClassRows(map[string]classPref{"other": {ContextWindow: 200000}}, rows, nil)
	if len(rows) != 2 {
		t.Fatalf("carry-forward derive = %+v", rows)
	}
	var tomb *registryEntry
	for i := range rows {
		if rows[i].Name == "frontier" {
			tomb = &rows[i]
		}
	}
	if tomb == nil || tomb.disabledReason != classRemovedReason {
		t.Fatalf("tombstone dropped on an unrelated refresh: %+v", rows)
	}
	// re-created -> active again, tombstone dissolves
	rows = deriveClassRows(map[string]classPref{"frontier": {ContextWindow: 256000}}, rows, nil)
	if len(rows) != 2 {
		t.Fatalf("re-create derive = %+v", rows)
	}
	for _, r := range rows {
		if r.Name == "frontier" && (r.disabledReason != "" || r.ContextWindow != 256000) {
			t.Fatalf("re-created class must be active with fresh facts: %+v", r)
		}
		if r.Name == "other" && r.disabledReason != classRemovedReason {
			t.Fatalf("the class that vanished this refresh must tombstone: %+v", r)
		}
	}
}

func TestDeriveClassRowsSkipsCollisionsAndJunk(t *testing.T) {
	rows := deriveClassRows(map[string]classPref{
		"":            {ContextWindow: 200000}, // empty id
		"has[1m]junk": {ContextWindow: 200000}, // "[1m]" is renderer-owned
		"frontier":    {ContextWindow: 200000},
	}, nil, nil)
	if len(rows) != 1 || rows[0].Name != "frontier" {
		t.Fatalf("derived %+v, want only the clean frontier row", rows)
	}
}

// The advertise-set must be a SUBSET of the refusable-set: the brain's class-id
// charset is wider than the proxy's class shape (^[a-z][a-z0-9-]*$ allows
// digits and hyphen runs), and a brain-valid id the rule-3 shape could not
// refuse must never be advertised — once cached by a no-TTL picker, removed,
// and the snapshot lost, it would be indistinguishable from a real Anthropic
// model id and ride the lane path to api.anthropic.com.
func TestDeriveClassRowsNeverAdvertisesUnrefusable(t *testing.T) {
	rows := deriveClassRows(map[string]classPref{
		"k2":       {ContextWindow: 200000}, // digit: valid brain class id, unrefusable shape
		"gpt5-sol": {ContextWindow: 200000}, // digit inside a word
		"a--b":     {ContextWindow: 200000}, // hyphen run: brain-valid, outside the shape
		"frontier": {ContextWindow: 200000},
	}, nil, nil)
	if len(rows) != 1 || rows[0].Name != "frontier" {
		t.Fatalf("derived %+v, want only frontier — every other id is one the rule-3 refusal could never catch", rows)
	}
	for i := range rows {
		if id := rows[i].renderedID(); !classShapedModel(id) {
			t.Fatalf("derived row advertises %q, which classShapedModel would not refuse after a state loss", id)
		}
	}
}

// An image class may be named for its model: no picker lists an image row, so
// the shape rule has nothing to protect, and the image door reaches it by its
// bare name. A chat class of the same name is still refused. The last-good
// loader admits it the same way.
func TestImageRowsMayCarryDigits(t *testing.T) {
	rows := deriveClassRows(map[string]classPref{
		"gpt-image-2": {RequestKind: "image"},
		"k2":          {ContextWindow: 200000},
	}, nil, nil)
	if len(rows) != 1 || rows[0].Name != "gpt-image-2" || rows[0].Modality != modalityImage || !rows[0].pickerHidden {
		t.Fatalf("derived %+v, want only the hidden image row gpt-image-2", rows)
	}
	withClassRows(t, "")
	path := filepath.Join(t.TempDir(), "class-rows.json")
	withClassStateFile(t, path)
	lan.persistClassRows(rows)
	lan.loadClassRowsState()
	got, _ := lan.classRowsSnapshot()
	if len(got) != 1 || got[0].Name != "gpt-image-2" || got[0].Modality != modalityImage {
		t.Fatalf("reloaded %+v, want the image row kept", got)
	}
}

// The last-good loader applies the same admission rule (a snapshot is state,
// not an authority): unrefusable shapes and renderer-owned "[1m]" names.
func TestLoadClassRowsStateAppliesRowGuard(t *testing.T) {
	withClassRows(t, "")
	path := filepath.Join(t.TempDir(), "class-rows.json")
	withClassStateFile(t, path)
	if err := os.WriteFile(path, []byte(`{"savedAt":1,"rows":[
		{"name":"k2","class":"k2","context_window":200000},
		{"name":"has[1m]junk","class":"has[1m]junk","context_window":200000},
		{"name":"helpdesk","class":"helpdesk","context_window":200000,"removed":true},
		{"name":"frontier","class":"frontier","context_window":200000}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	lan.loadClassRowsState()
	got, src := lan.classRowsSnapshot()
	if src != "last-good" {
		t.Fatalf("boot source = %q, want last-good", src)
	}
	if len(got) != 2 || got[0].Name != "frontier" || got[1].Name != "helpdesk" {
		t.Fatalf("loaded %+v, want only the clean frontier row and the helpdesk tombstone", got)
	}
	if got[1].disabledReason != classRemovedReason {
		t.Fatalf("tombstone lost its quarantine: %+v", got[1])
	}
}

// The last-good snapshot keeps each class's direct stand-in, so a proxy that
// boots brain-down still serves it on /_switch/classes.
func TestClassRowsStateKeepsDirectModel(t *testing.T) {
	withClassRows(t, "")
	path := filepath.Join(t.TempDir(), "class-rows.json")
	withClassStateFile(t, path)
	lan.persistClassRows([]registryEntry{{Name: "capable", Type: "class", Class: "capable", ContextWindow: 200000,
		FirstChoice: "Claude Opus 5.5", DirectModel: "claude-opus-5-5"}})
	lan.loadClassRowsState()
	got, _ := lan.classRowsSnapshot()
	if len(got) != 1 || got[0].DirectModel != "claude-opus-5-5" {
		t.Fatalf("reloaded %+v, want the direct model kept", got)
	}
}

// The generalized class-shape heuristic (#93 rule 3): digit-free claude-* ids
// are workload classes; every real Anthropic id carries a digit and must pass.
func TestClassShapedModel(t *testing.T) {
	for _, model := range []string{
		// every class id in play today, plus shapes of the trap cases
		"claude-frontier",
		"claude-helpdesk",
		"claude-fast",
		"claude-capable",
		"claude-capable[1m]", // stale [1m]-suffixed picker row
		"claude-frontier[1m]",
		"claude-delegate-typo",
		"claude-local",         // the local class (#135)
		"claude-local-mistral", // a digit-free tag of the deleted local rows: refused as no class
	} {
		if !classShapedModel(model) {
			t.Fatalf("classShapedModel(%q) = false, want true (must refuse, never forward)", model)
		}
		if reason, _, ok := classShapedRefusalReason(nil, model); !ok || reason == "" {
			t.Fatalf("classShapedRefusalReason(%q) = (%q, %v), want a typed refusal", model, reason, ok)
		}
	}
	for _, model := range []string{
		// real Anthropic model ids — the false-positive cost is severe, so every
		// current and historical shape must pass: they all carry a digit
		"claude-opus-5",
		"claude-fable-5",
		"claude-haiku-4-5-20251001",
		"claude-3-5-sonnet-20241022",
		"claude-3-5-sonnet-latest",
		"claude-sonnet-4-20250514",
		"claude-opus-4-1-20250805",
		"claude-3-haiku-20240307",
		"claude-2.1",
		"claude-instant-1.2",
		"claude-opus-5[1m]",
		// non-namespace shapes
		"",
		"claude",
		"fast",                   // no claude- prefix
		"Claude-Frontier",        // case-sensitive: not the namespace
		"claude-frontier-2",      // digit => never refused by the heuristic
		"claude-frontier extra",  // whitespace/garbage: not the namespace
		"anthropic-claude-fable", // wrong prefix
	} {
		if classShapedModel(model) {
			t.Fatalf("classShapedModel(%q) = true — a non-class id must NEVER be refused", model)
		}
	}
}

func TestClassRowsPersistRoundTrip(t *testing.T) {
	withClassRows(t, "")
	path := filepath.Join(t.TempDir(), "state", "class-rows.json")
	withClassStateFile(t, path)

	tomb := registryEntry{Name: "helpdesk", Type: "class", Class: "helpdesk", Display: "Helpdesk", ContextWindow: 200000}
	tomb.disabledReason = classRemovedReason
	rows := []registryEntry{
		{Name: "frontier", Type: "class", Class: "frontier", Display: "Frontier (auto)", ContextWindow: 1000000},
		tomb,
	}
	lan.setClassRows(rows, "preferred")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("live-derived rows must persist: %v", err)
	}

	// wipe the in-memory store, then boot from the file
	lan.classRowsMu.Lock()
	lan.classRows, lan.classRowsSource = nil, ""
	lan.classRowsMu.Unlock()
	lan.loadClassRowsState()
	got, src := lan.classRowsSnapshot()
	if src != "last-good" {
		t.Fatalf("boot source = %q, want last-good", src)
	}
	if len(got) != 2 {
		t.Fatalf("boot rows = %+v", got)
	}
	if got[0].renderedID() != "claude-frontier[1m]" || got[0].disabledReason != "" {
		t.Fatalf("active row lost in round trip: %+v", got[0])
	}
	if got[1].Name != "helpdesk" || got[1].disabledReason != classRemovedReason {
		t.Fatalf("tombstone lost in round trip: %+v", got[1])
	}

	// a boot-time load must never re-persist what it just read
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	lan.setClassRows(got, "last-good")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("source last-good must not write the state file")
	}
	// the first live refresh after boot serves the SAME rows: no change, no
	// write — the 30s poll must not grind the disk re-writing identical state
	lan.setClassRows(got, "preferred")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("an unchanged refresh must not rewrite the state file")
	}
	// a CHANGED live refresh persists again
	changed := []registryEntry{
		{Name: "frontier", Type: "class", Class: "frontier", Display: "Frontier (auto)", ContextWindow: 200000},
	}
	lan.setClassRows(changed, "preferred")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("a changed refresh must persist: %v", err)
	}
}

func TestLoadClassRowsStateTolerant(t *testing.T) {
	withClassRows(t, "")
	dir := t.TempDir()

	// absent file: a normal first boot
	withClassStateFile(t, filepath.Join(dir, "absent.json"))
	lan.loadClassRowsState()
	if got, src := lan.classRowsSnapshot(); len(got) != 0 || src != "" {
		t.Fatalf("absent file must leave the store empty, got %+v / %q", got, src)
	}

	// corrupt file: logged, tolerated, never fatal (a restart would only fail again)
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	withClassStateFile(t, bad)
	lan.loadClassRowsState()
	if got, _ := lan.classRowsSnapshot(); len(got) != 0 {
		t.Fatalf("corrupt file must leave the store empty, got %+v", got)
	}

	// junk rows are skipped, sane ones load
	mixed := filepath.Join(dir, "mixed.json")
	if err := os.WriteFile(mixed, []byte(`{"savedAt":1,"rows":[
		{"name":"","class":"","context_window":200000},
		{"name":"frontier","class":"frontier","context_window":100000}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	withClassStateFile(t, mixed)
	lan.loadClassRowsState()
	got, _ := lan.classRowsSnapshot()
	if len(got) != 1 || got[0].Name != "frontier" || got[0].ContextWindow != 100000 {
		t.Fatalf("mixed file load = %+v, want the one sane row with its window as saved", got)
	}
}

// Boot-from-last-good with the brain DOWN (#93 rule 1): the advertised set must
// include the class rows before any /preferred has ever answered — "advertising
// must work brain-down" — and a request routes to the brain's typed refusal
// path (here: unreachable + cold bind cache => typed 503), never upstream.
func TestE2EClassBootFromLastGoodBrainDown(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	oldLocal := localUpstream
	localUpstream = ""
	t.Cleanup(func() { localUpstream = oldLocal; providers = nil })

	path := filepath.Join(t.TempDir(), "class-rows.json")
	tomb := registryEntry{Name: "helpdesk", Type: "class", Class: "helpdesk", ContextWindow: 200000}
	tomb.disabledReason = classRemovedReason
	withClassStateFile(t, path)
	lan.persistClassRows([]registryEntry{
		{Name: "frontier", Type: "class", Class: "frontier", Display: "Frontier (auto)", ContextWindow: 200000},
		tomb,
	})

	// kill the brain BEFORE the boot load: nothing may depend on a live fetch
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()
	oldBrain := brainBase
	brainBase = deadURL
	t.Cleanup(func() { brainBase = oldBrain })

	// production boot order: registry first (buildHandler), THEN the snapshot —
	// loadClassRowsState's named-collision guard consults the built registry.
	rebuildProxy(env)
	lan.loadClassRowsState()

	ids := getModels(t, env.proxy.URL)
	if len(ids) != 1 || ids[0] != "claude-frontier" {
		t.Fatalf("brain-down advertised %v, want exactly [claude-frontier] (tombstones never advertise)", ids)
	}

	// active row, brain down, cold bind cache: typed 503 from the class path
	resp := env.post(t, "claude-frontier", "CLIENT")
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", resp.StatusCode)
	}
	if !strings.Contains(string(b), "brain unreachable and no cached class binding") {
		t.Fatalf("503 body lacks the cold-cache reason: %s", b)
	}

	// tombstoned row: the typed removed-quarantine, not an upstream 404
	resp = env.post(t, "claude-helpdesk", "CLIENT")
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(b), classRemovedReason) {
		t.Fatalf("tombstone answer = %d %s, want a typed 503 naming %q", resp.StatusCode, b, classRemovedReason)
	}
	select {
	case leaked := <-env.seen:
		t.Fatalf("brain-down class request leaked upstream: %+v", leaked)
	default:
	}
}

// #120: a marked class row must still answer to the id its API consumers send.
// An agent gateway or a web app has no picker and no knowledge of "[1m]"; matching
// on the rendered id alone would answer them model_not_found.
func TestClassMarkedRowAnswersBareID(t *testing.T) {
	defer func(p int) { oneMegaMarkFloor = p }(oneMegaMarkFloor)
	oneMegaMarkFloor = 300000

	marked := registryEntry{Name: "tutor", Type: "class", Class: "tutor", ContextWindow: 419672}
	plain := registryEntry{Name: "frontier", Type: "class", Class: "frontier", ContextWindow: 200000}
	lan.classRowsMu.Lock()
	oldRows := lan.classRows
	lan.classRows = []registryEntry{marked, plain}
	lan.classRowsMu.Unlock()
	t.Cleanup(func() {
		lan.classRowsMu.Lock()
		lan.classRows = oldRows
		lan.classRowsMu.Unlock()
	})

	if got := marked.renderedID(); got != "claude-tutor[1m]" {
		t.Fatalf("marked.renderedID() = %q", got)
	}
	for _, id := range []string{"claude-tutor[1m]", "claude-tutor"} {
		if _, ok := lan.matchRegistryClass(id); !ok {
			t.Fatalf("%q did not resolve — a marked class must answer to both its rendered and bare id", id)
		}
	}
	if _, ok := lan.matchRegistryClass("claude-frontier"); !ok {
		t.Fatalf("an unmarked class must still answer to its own id")
	}
	// The reverse is NOT an alias: rule 3 still refuses a suffix the row never had.
	if _, ok := lan.matchRegistryClass("claude-frontier[1m]"); ok {
		t.Fatalf("an unmarked class answered to a suffixed id — that must stay a typed refusal")
	}
}

// #95/#120: only classes whose consumer is a Claude Code client belong in the
// picker. Another app's class stays fully routable but is neither advertised nor
// marked — its id is a routing key that app hard-codes, and the marker would
// move under it whenever the brain re-derives the window.
func TestClassRowsHideForeignConsumers(t *testing.T) {
	defer func(p int) { oneMegaMarkFloor = p }(oneMegaMarkFloor)
	oneMegaMarkFloor = 300000

	shapes := map[string]string{"coding-harness": "claude-code", "delegation": "claude-code",
		"agent-gw": "other", "tutor": "other"}
	classes := map[string]classPref{
		"frontier": {ContextWindow: 200000, Consumer: "coding-harness"},
		"tutor":    {ContextWindow: 419672, Consumer: "tutor"},
		"unowned":  {ContextWindow: 419672},
	}
	rows := deriveClassRows(classes, nil, shapes)

	byName := map[string]*registryEntry{}
	for i := range rows {
		byName[rows[i].Name] = &rows[i]
	}
	if got := byName["tutor"].renderedID(); got != "claude-tutor" {
		t.Fatalf("foreign-consumer row rendered %q — it must never carry the picker's marker", got)
	}
	if !byName["tutor"].pickerHidden {
		t.Fatalf("tutor is owned by a non-claude-code consumer and must be hidden from the picker")
	}
	// Unowned is not the same as owned-by-something-else: it stays advertised,
	// and at 419672 it clears the floor like any other advertised row.
	if got := byName["unowned"].renderedID(); got != "claude-unowned[1m]" {
		t.Fatalf("unowned row rendered %q, want the marker", got)
	}
	if byName["frontier"].pickerHidden {
		t.Fatalf("a claude-code consumer's class must stay advertised")
	}

	lan.classRowsMu.Lock()
	old := lan.classRows
	lan.classRows = rows
	lan.classRowsMu.Unlock()
	t.Cleanup(func() {
		lan.classRowsMu.Lock()
		lan.classRows = old
		lan.classRowsMu.Unlock()
	})

	for _, ad := range lan.registryClassRows() {
		if ad.pickerID == "claude-tutor" {
			t.Fatalf("hidden row reached /v1/models: %+v", ad)
		}
	}
	// Hidden does NOT mean unroutable: API consumers without a picker must still reach it.
	if _, ok := lan.matchRegistryClass("claude-tutor"); !ok {
		t.Fatalf("hidden class stopped routing — that would be an outage for its consumer")
	}
}

// A class-shaped id can be unknown for three different reasons, and answering
// all of them with one sentence sent a real caller chasing a phantom "removed
// class" for a session. Each case must name its own remediation, and only the
// transient one may advertise a retry.
func TestClassShapedRefusalDistinguishesItsThreeCases(t *testing.T) {
	oldRows, oldSrc := lan.classRows, lan.classRowsSource
	t.Cleanup(func() { lan.classRows, lan.classRowsSource = oldRows, oldSrc })

	lan.pref.mu.Lock()
	oldOK := lan.pref.ok
	lan.pref.ok = false
	lan.pref.mu.Unlock()
	t.Cleanup(func() { lan.pref.mu.Lock(); lan.pref.ok = oldOK; lan.pref.mu.Unlock() })

	// (1) nothing loaded and the brain has never answered — every class id is
	// unknown right now and may well resolve on its own. The ONLY retryable case.
	lan.classRows, lan.classRowsSource = nil, ""
	reason, retry, ok := classShapedRefusalReason(lan, "claude-frontier")
	if !ok || retry <= 0 {
		t.Fatalf("pre-load refusal = (%q, %d), want a retryable refusal", reason, retry)
	}
	if !strings.Contains(reason, "has not loaded yet") || !strings.Contains(reason, "EVERY class id") {
		t.Fatalf("pre-load refusal does not say the registry is the problem: %q", reason)
	}

	// (1b) the brain ANSWERED and declared no classes. Same empty registry,
	// opposite advice — waiting cannot conjure a class that does not exist.
	lan.pref.mu.Lock()
	lan.pref.ok = true
	lan.pref.mu.Unlock()
	reason, retry, ok = classShapedRefusalReason(lan, "claude-frontier")
	if !ok || retry != 0 {
		t.Fatalf("no-classes refusal = (%q, %d), want a permanent refusal", reason, retry)
	}
	if !strings.Contains(reason, "no workload classes at all") {
		t.Fatalf("no-classes refusal does not say so: %q", reason)
	}

	// (2) the class exists; the caller asked for a 1M variant that is not
	// advertised. Retrying is useless — the message must say the edit.
	lan.classRows = []registryEntry{{Name: "frontier", Type: "class", Class: "frontier", ContextWindow: 200000}}
	lan.classRowsSource = "preferred"
	reason, retry, ok = classShapedRefusalReason(lan, "claude-frontier[1m]")
	if !ok {
		t.Fatal("a [1m] form of a 200k class must still be refused")
	}
	if retry != 0 {
		t.Fatalf("retry = %d, want 0 — retrying a suffix mismatch just burns the same error", retry)
	}
	for _, want := range []string{"claude-frontier exists", "1M context window", "without the suffix"} {
		if !strings.Contains(reason, want) {
			t.Fatalf("suffix refusal missing %q:\n%s", want, reason)
		}
	}

	// (3) genuinely unknown — no retry, and it points at the list.
	reason, retry, ok = classShapedRefusalReason(lan, "claude-nope")
	if !ok || retry != 0 {
		t.Fatalf("unknown-class refusal = (%q, %d), want a permanent refusal", reason, retry)
	}
	if !strings.Contains(reason, "not registered") {
		t.Fatalf("unknown-class refusal changed shape: %q", reason)
	}
}
