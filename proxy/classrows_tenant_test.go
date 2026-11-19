package main

// Class rows per tenant (C15): each tenant's rows, last-good file and class
// lookups come from its own /preferred, never another tenant's. Tenant A is
// the LAN tenant with classes private and shared; tenant B is a member with
// classes mine and shared, where B's shared targets a provider and A's an
// Anthropic lane. The door's half (routing, its status, its class list) is in
// classrows_tenant_household_test.go.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

var (
	classTenantA = "ten_classa" + strings.Repeat("a", 20)
	classTenantB = "ten_classb" + strings.Repeat("b", 20)
)

// classDocA and classDocB are the two tenants' /preferred classes.
func classDocA() map[string]classPref {
	return map[string]classPref{
		"private": {Display: "Private", ContextWindow: 200000,
			Members: []classMemberPref{anthropicMember("lane:default")}},
		"shared": {Display: "Shared", ContextWindow: 200000,
			Members: []classMemberPref{anthropicMember("lane:default")}},
	}
}

func classDocB() map[string]classPref {
	return map[string]classPref{
		"mine": {Display: "Mine", ContextWindow: 200000,
			Members: []classMemberPref{providerMember("gpt/gpt-6", "gpt", "gpt-6")}},
		"shared": {Display: "Shared", ContextWindow: 200000,
			Members: []classMemberPref{providerMember("kimi/kimi-k3", "kimi", "kimi-k3")}},
	}
}

// withClassDoc gives tn classes as its /preferred would: the document the
// class walk reads, and the rows derived from it.
func withClassDoc(tn *tenant, classes map[string]classPref) {
	tn.pref.mu.Lock()
	tn.pref.classes, tn.pref.ok = classes, true
	tn.pref.mu.Unlock()
	tn.updateClassRowsFromPreferred(classes, nil)
}

// twoClassTenants installs A (the LAN tenant) and B, each with its own
// classes document, and puts the household back afterwards.
func twoClassTenants(t *testing.T) (a, b *tenant) {
	t.Helper()
	a = installTenants(
		tenantRow{Tenant: classTenantA, Name: "a", Lan: true, Tap: tapContent},
		tenantRow{Tenant: classTenantB, Name: "b", Tap: tapMetadata})
	b = memberOf(classTenantB)
	t.Cleanup(resetTenants)
	withClassDoc(a, classDocA())
	withClassDoc(b, classDocB())
	return a, b
}

// (1) A class resolves only for the tenant that declares it; for the other it
// is an unknown class-shaped id, refused typed.
func TestClassRowsResolvePerTenant(t *testing.T) {
	a, b := twoClassTenants(t)
	for _, c := range []struct {
		tn         *tenant
		mine, them string
	}{{a, "claude-private", "claude-mine"}, {b, "claude-mine", "claude-private"}} {
		if _, ok := c.tn.matchRegistryClass(c.mine); !ok {
			t.Fatalf("%s: its own %s does not resolve", c.tn.id, c.mine)
		}
		if _, ok := c.tn.matchRegistryClass(c.them); ok {
			t.Fatalf("%s: the other tenant's %s resolves", c.tn.id, c.them)
		}
		reason, retry, ok := classShapedRefusalReason(c.tn, c.them)
		if !ok || retry != 0 || !strings.Contains(reason, "not registered") {
			t.Fatalf("%s: %s refusal = %q retry=%d ok=%v, want the unknown-class refusal", c.tn.id, c.them, reason, retry, ok)
		}
		if _, ok := c.tn.matchRegistryClass("claude-shared"); !ok {
			t.Fatalf("%s: its own shared does not resolve", c.tn.id)
		}
	}
	var nobody *tenant
	if _, ok := nobody.matchRegistryClass("claude-shared"); ok {
		t.Fatal("a nil tenant resolves a class")
	}
}

func modelsIDsFor(t *testing.T, tn *tenant) map[string]bool {
	t.Helper()
	rec := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/v1/models", nil)
	if tn != nil {
		r = r.WithContext(withTenant(r.Context(), tn))
	}
	modelsHandler(rec, r)
	var out struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("bad json: %v (%s)", err, rec.Body.String())
	}
	ids := map[string]bool{}
	for _, d := range out.Data {
		ids[d.ID] = true
	}
	return ids
}

func classesListed(t *testing.T, serve func(http.ResponseWriter)) map[string]bool {
	t.Helper()
	rec := httptest.NewRecorder()
	serve(rec)
	var out struct {
		Classes []classGuidanceRow `json:"classes"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("bad json: %v (%s)", err, rec.Body.String())
	}
	got := map[string]bool{}
	for _, c := range out.Classes {
		got[c.Class] = true
	}
	return got
}

// (2) /v1/models and /_switch/classes list the request tenant's own rows.
func TestModelsAndClassesListTheTenantsOwnRows(t *testing.T) {
	a, b := twoClassTenants(t)
	if got := modelsIDsFor(t, a); !got["claude-private"] || !got["claude-shared"] || got["claude-mine"] {
		t.Fatalf("A's /v1/models = %v, want private and shared only", got)
	}
	if got := modelsIDsFor(t, b); !got["claude-mine"] || !got["claude-shared"] || got["claude-private"] {
		t.Fatalf("B's /v1/models = %v, want mine and shared only", got)
	}
	for id := range modelsIDsFor(t, nil) {
		if classShapedModel(id) {
			t.Fatalf("an unstamped /v1/models lists class %s", id)
		}
	}
	req := httptest.NewRequest("GET", "/_switch/classes", nil)
	if got := classesListed(t, func(w http.ResponseWriter) { classesHandler(w, req) }); !got["private"] || got["mine"] {
		t.Fatalf(":8787's classes = %v, want the LAN tenant's", got)
	}
	if got := classesListed(t, func(w http.ResponseWriter) { classesHandlerFor(w, req, b) }); !got["mine"] || !got["shared"] || got["private"] {
		t.Fatalf("B's classes = %v, want mine and shared only", got)
	}
}

// (3) A class B's document drops tombstones for B; A's class of the same id
// stays live.
func TestClassRemovedTombstonesForItsTenantOnly(t *testing.T) {
	a, b := twoClassTenants(t)
	doc := classDocB()
	delete(doc, "shared")
	withClassDoc(b, doc)
	if _, ok := b.matchClassTombstone("claude-shared"); !ok {
		t.Fatal("B's removed shared is not tombstoned for B")
	}
	if _, ok := b.matchRegistryClass("claude-shared"); ok {
		t.Fatal("B's removed shared still resolves for B")
	}
	if _, ok := a.matchRegistryClass("claude-shared"); !ok {
		t.Fatal("A's shared stopped resolving when B removed its own")
	}
	if _, ok := a.matchClassTombstone("claude-shared"); ok {
		t.Fatal("A's shared is tombstoned by B's removal")
	}
}

// (4) Each tenant's last-good file round-trips: the LAN tenant's is the
// configured file in its {savedAt, rows} shape, a member's sits beside it,
// each tenant serves its rows from its file at start before any brain, and a
// tenant that leaves the index takes its file with it.
func TestClassRowsLastGoodPerTenant(t *testing.T) {
	dir := t.TempDir()
	lanFile := filepath.Join(dir, "class-rows.json")
	memberFile := filepath.Join(dir, "class-rows."+classTenantB+".json")
	old := classStateFilePath
	classStateFilePath = lanFile
	t.Cleanup(func() { classStateFilePath = old })
	if got := classStatePath(classTenantB, false); got != memberFile {
		t.Fatalf("member's file = %s, want %s", got, memberFile)
	}

	rows := []tenantRow{
		{Tenant: classTenantA, Name: "a", Lan: true, Tap: tapContent},
		{Tenant: classTenantB, Name: "b", Tap: tapMetadata},
	}
	installTenants()
	t.Cleanup(resetTenants)
	if err := applyTenantIndex(rows, "test"); err != nil {
		t.Fatal(err)
	}
	a, b := memberOf(classTenantA), memberOf(classTenantB)
	withClassDoc(a, classDocA())
	withClassDoc(b, classDocB())

	// The LAN tenant's file is today's shape, exactly: savedAt and rows.
	raw, err := os.ReadFile(lanFile)
	if err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(top))
	for k := range top {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if !slices.Equal(keys, []string{"rows", "savedAt"}) {
		t.Fatalf("LAN file keys = %v, want rows and savedAt", keys)
	}
	var f classStateFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if names := stateRowNames(f); !slices.Equal(names, []string{"private", "shared"}) {
		t.Fatalf("LAN file rows = %v, want private and shared", names)
	}
	raw, err = os.ReadFile(memberFile)
	if err != nil {
		t.Fatalf("B's file: %v", err)
	}
	f = classStateFile{}
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if names := stateRowNames(f); !slices.Equal(names, []string{"mine", "shared"}) {
		t.Fatalf("B's file rows = %v, want mine and shared", names)
	}

	// A fresh start of both, no brain: each serves its own last-good rows.
	installTenants()
	if err := applyTenantIndex(rows, "test"); err != nil {
		t.Fatal(err)
	}
	a, b = memberOf(classTenantA), memberOf(classTenantB)
	for _, c := range []struct {
		tn         *tenant
		mine, them string
	}{{a, "claude-private", "claude-mine"}, {b, "claude-mine", "claude-private"}} {
		if _, src := c.tn.classRowsSnapshot(); src != "last-good" {
			t.Fatalf("%s: rows source = %q, want last-good", c.tn.id, src)
		}
		if _, ok := c.tn.matchRegistryClass(c.mine); !ok {
			t.Fatalf("%s: %s not served from its last-good file", c.tn.id, c.mine)
		}
		if _, ok := c.tn.matchRegistryClass(c.them); ok {
			t.Fatalf("%s: serves the other tenant's %s", c.tn.id, c.them)
		}
	}

	// B leaves the index: its rows and its file go; A's file stays.
	if err := applyTenantIndex(rows[:1], "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(memberFile); !os.IsNotExist(err) {
		t.Fatalf("B's file survives its removal: %v", err)
	}
	if _, err := os.Stat(lanFile); err != nil {
		t.Fatalf("A's file: %v", err)
	}
}

// A last-good file as an older proxy wrote it (one shared registry) is the
// LAN tenant's, read unchanged.
func TestClassRowsLanFileReadsTodaysShape(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "class-rows.json")
	old := classStateFilePath
	classStateFilePath = path
	t.Cleanup(func() { classStateFilePath = old })
	today := `{"savedAt":1700000000000,"rows":[` +
		`{"name":"frontier","display":"Frontier","class":"frontier","context_window":200000,"guidance":"g","first_choice":"Claude Fable 5"},` +
		`{"name":"gone","display":"Gone","class":"gone","context_window":200000,"removed":true}]}`
	if err := os.WriteFile(path, []byte(today), 0o600); err != nil {
		t.Fatal(err)
	}
	installTenants()
	t.Cleanup(resetTenants)
	if err := applyTenantIndex([]tenantRow{{Tenant: classTenantA, Name: "a", Lan: true, Tap: tapContent}}, "test"); err != nil {
		t.Fatal(err)
	}
	a := memberOf(classTenantA)
	if e, ok := a.matchRegistryClass("claude-frontier"); !ok || e.Guidance != "g" || e.FirstChoice != "Claude Fable 5" {
		t.Fatalf("frontier from today's file = %+v %v", e, ok)
	}
	if _, ok := a.matchClassTombstone("claude-gone"); !ok {
		t.Fatal("today's tombstone is not one")
	}
}

func stateRowNames(f classStateFile) []string {
	var out []string
	for _, r := range f.Rows {
		out = append(out, r.Name)
	}
	sort.Strings(out)
	return out
}

// (5) The member view, effort and class-by-name answer from the request
// tenant's document: the same class id targets different members.
func TestClassMemberViewIsTheTenantsOwn(t *testing.T) {
	a, b := twoClassTenants(t)
	if m, ok := a.classTargetMember("shared"); !ok || m.Kind != kindAnthropic {
		t.Fatalf("A's shared targets %+v %v, want its lane", m, ok)
	}
	if m, ok := b.classTargetMember("shared"); !ok || m.Provider != "kimi" {
		t.Fatalf("B's shared targets %+v %v, want kimi", m, ok)
	}
	if !a.classEffortReaches("shared") || b.classEffortReaches("shared") {
		t.Fatalf("effort: A's shared %v (want true), B's %v (want false)",
			a.classEffortReaches("shared"), b.classEffortReaches("shared"))
	}
	if _, ok := b.classTargetMember("private"); ok {
		t.Fatal("B answers for A's private")
	}
	if _, ok := b.matchRegistryClassName("mine"); !ok {
		t.Fatal("B's mine not found by name")
	}
	if _, ok := a.matchRegistryClassName("mine"); ok {
		t.Fatal("A finds B's mine by name")
	}
}

// (6) /_switch/status keeps the LAN tenant's rows under registry, as the
// deploy gate reads them, and every tenant's own under members.<t>.classes.
func TestStatusRegistryIsTheLanTenantsAndMembersCarryTheirOwn(t *testing.T) {
	a, b := twoClassTenants(t)
	rec := httptest.NewRecorder()
	switchHandler(rec, httptest.NewRequest("GET", "/_switch/status", nil))
	var st struct {
		Registry struct {
			ClassSource string `json:"class_source"`
			Entries     []struct {
				ID      string `json:"id"`
				Enabled bool   `json:"enabled"`
			} `json:"entries"`
		} `json:"registry"`
		Members map[string]struct {
			Classes struct {
				Source  string `json:"source"`
				Entries []struct {
					ID      string `json:"id"`
					Enabled bool   `json:"enabled"`
				} `json:"entries"`
			} `json:"classes"`
		} `json:"members"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatalf("%v %s", err, rec.Body.String())
	}
	reg := map[string]bool{}
	for _, e := range st.Registry.Entries {
		reg[e.ID] = e.Enabled
	}
	if !reg["claude-private"] || !reg["claude-shared"] || reg["claude-mine"] || st.Registry.ClassSource != "preferred" {
		t.Fatalf("registry = %v (%s), want the LAN tenant's rows", reg, st.Registry.ClassSource)
	}
	for tn, want := range map[*tenant][]string{a: {"claude-private", "claude-shared"}, b: {"claude-mine", "claude-shared"}} {
		var got []string
		for _, e := range st.Members[tn.id].Classes.Entries {
			got = append(got, e.ID)
		}
		sort.Strings(got)
		if !slices.Equal(got, want) || st.Members[tn.id].Classes.Source != "preferred" {
			t.Fatalf("members[%s].classes = %v (%s), want %v", tn.id, got, st.Members[tn.id].Classes.Source, want)
		}
	}
}

// (7) The lan_only filter is gone from the proxy: its rows are per tenant,
// so there is nothing left for it to filter.
func TestTheLANFilterIsGone(t *testing.T) {
	gone := regexp.MustCompile(`Lan` + `Only|lan` + `Only|visible` + `To\b`)
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if loc := gone.FindIndex(raw); loc != nil {
			t.Fatalf("%s still names %s", f, raw[loc[0]:loc[1]])
		}
	}
}

// withClassStateDir points the last-good files at a fresh directory and
// returns the household's (configured) file and a member's.
func withClassStateDir(t *testing.T) (lanFile string, memberFile func(id string) string) {
	t.Helper()
	dir := t.TempDir()
	lanFile = filepath.Join(dir, "class-rows.json")
	old := classStateFilePath
	classStateFilePath = lanFile
	t.Cleanup(func() { classStateFilePath = old })
	return lanFile, func(id string) string { return filepath.Join(dir, "class-rows."+id+".json") }
}

func readStateNames(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	var f classStateFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	return stateRowNames(f)
}

// The lan flag moving between two tenants that both stay: each tenant's rows
// follow it to the file its new flag names, and the new household's old
// member file goes.
func TestClassRowsFileFollowsTheLanFlag(t *testing.T) {
	lanFile, memberFile := withClassStateDir(t)
	installTenants()
	t.Cleanup(resetTenants)
	rows := []tenantRow{
		{Tenant: classTenantA, Name: "a", Lan: true, Tap: tapContent},
		{Tenant: classTenantB, Name: "b", Tap: tapMetadata},
	}
	if err := applyTenantIndex(rows, "test"); err != nil {
		t.Fatal(err)
	}
	a, b := memberOf(classTenantA), memberOf(classTenantB)
	withClassDoc(a, classDocA())
	withClassDoc(b, classDocB())

	flipped := []tenantRow{
		{Tenant: classTenantA, Name: "a", Tap: tapMetadata},
		{Tenant: classTenantB, Name: "b", Lan: true, Tap: tapContent},
	}
	if err := applyTenantIndex(flipped, "test"); err != nil {
		t.Fatal(err)
	}
	if got := readStateNames(t, lanFile); !slices.Equal(got, []string{"mine", "shared"}) {
		t.Fatalf("the household's file holds %v, want B's rows", got)
	}
	if got := readStateNames(t, memberFile(classTenantA)); !slices.Equal(got, []string{"private", "shared"}) {
		t.Fatalf("A's member file holds %v, want A's rows", got)
	}
	if _, err := os.Stat(memberFile(classTenantB)); !os.IsNotExist(err) {
		t.Fatalf("B's old member file survives its move to the household's: %v", err)
	}
	// The next change of each lands in its new file.
	doc := classDocA()
	delete(doc, "shared")
	withClassDoc(a, doc)
	raw, err := os.ReadFile(memberFile(classTenantA))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"removed":true`) {
		t.Fatalf("A's change (shared removed) did not reach its member file: %s", raw)
	}
	if got := readStateNames(t, lanFile); !slices.Equal(got, []string{"mine", "shared"}) {
		t.Fatalf("A's change reached the household's file: %v", got)
	}
}

// The household's id changing: the old household stops before the new one
// starts, the configured file is never removed, and the new household starts
// from it.
func TestClassRowsFileSurvivesTheHouseholdsNewID(t *testing.T) {
	lanFile, _ := withClassStateDir(t)
	installTenants()
	t.Cleanup(resetTenants)
	if err := applyTenantIndex([]tenantRow{{Tenant: classTenantA, Name: "a", Lan: true, Tap: tapContent}}, "test"); err != nil {
		t.Fatal(err)
	}
	withClassDoc(memberOf(classTenantA), classDocA())
	if err := applyTenantIndex([]tenantRow{{Tenant: classTenantB, Name: "b", Lan: true, Tap: tapContent}}, "test"); err != nil {
		t.Fatal(err)
	}
	if got := readStateNames(t, lanFile); !slices.Equal(got, []string{"private", "shared"}) {
		t.Fatalf("the household's file = %v after its id changed", got)
	}
	b := memberOf(classTenantB)
	if _, src := b.classRowsSnapshot(); src != "last-good" {
		t.Fatalf("the new household's rows source = %q, want last-good", src)
	}
	if _, ok := b.matchRegistryClass("claude-private"); !ok {
		t.Fatal("the new household does not serve the household's last-good rows")
	}
}

// A write that arrives after the tenant's stop (a /preferred in flight when
// it left the index) writes nothing: its removed file stays removed.
func TestClassRowsNoWriteAfterStop(t *testing.T) {
	_, memberFile := withClassStateDir(t)
	installTenants()
	t.Cleanup(resetTenants)
	rows := []tenantRow{
		{Tenant: classTenantA, Name: "a", Lan: true, Tap: tapContent},
		{Tenant: classTenantB, Name: "b", Tap: tapMetadata},
	}
	if err := applyTenantIndex(rows, "test"); err != nil {
		t.Fatal(err)
	}
	b := memberOf(classTenantB)
	withClassDoc(b, classDocB())
	if _, err := os.Stat(memberFile(classTenantB)); err != nil {
		t.Fatalf("B's file: %v", err)
	}
	if err := applyTenantIndex(rows[:1], "test"); err != nil {
		t.Fatal(err)
	}
	doc := classDocB()
	doc["late"] = classPref{Display: "Late", ContextWindow: 200000}
	b.updateClassRowsFromPreferred(doc, nil)
	if _, err := os.Stat(memberFile(classTenantB)); !os.IsNotExist(err) {
		t.Fatalf("a write after the stop brought B's file back: %v", err)
	}
}

// A tenant whose brain stream is configured but has not answered serves its
// last-good rows from its start.
func TestClassRowsServedBeforeTheBrainAnswers(t *testing.T) {
	lanFile, _ := withClassStateDir(t)
	today := `{"savedAt":1700000000000,"rows":[{"name":"private","display":"Private","class":"private","context_window":200000}]}`
	if err := os.WriteFile(lanFile, []byte(today), 0o600); err != nil {
		t.Fatal(err)
	}
	asked := make(chan string, 16)
	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case asked <- r.URL.Path:
		default:
		}
		if strings.HasSuffix(r.URL.Path, "/events") {
			<-r.Context().Done() // never answers
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(brain.Close)
	oldBrain := brainBase
	brainBase = brain.URL
	installTenants()
	t.Cleanup(func() { resetTenants(); brainBase = oldBrain })
	if err := applyTenantIndex([]tenantRow{{Tenant: classTenantA, Name: "a", Lan: true, Tap: tapContent}}, "test"); err != nil {
		t.Fatal(err)
	}
	a := memberOf(classTenantA)
	// The stream is open and silent.
	for p := range asked {
		if p == "/t/"+classTenantA+"/events" {
			break
		}
	}
	if a.brainAnswered() {
		t.Fatal("the brain answered; this test needs it silent")
	}
	if _, ok := a.matchRegistryClass("claude-private"); !ok {
		t.Fatal("the last-good row is not served while the brain is silent")
	}
	if got := modelsIDsFor(t, a); !got["claude-private"] {
		t.Fatalf("/v1/models = %v, want the last-good row", got)
	}
}

// A request on the OpenAI wire with no tenant is the typed no-tenant 503,
// never a model_not_found that blames its model.
func TestOpenAIWireWithNoTenantIsTheTyped503(t *testing.T) {
	core := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("a request with no tenant reached the core")
	})
	rec := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"claude-private","messages":[{"role":"user","content":"hi"}]}`))
	openaiWireHandler(core).ServeHTTP(rec, r)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "no tenant") {
		t.Fatalf("no tenant: %d %s, want the typed 503", rec.Code, rec.Body.String())
	}
}
