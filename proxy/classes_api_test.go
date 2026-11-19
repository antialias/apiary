package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

func TestClassMemberDisplay(t *testing.T) {
	for _, tc := range []struct{ kind, model, want string }{
		{kindAnthropic, "claude-sonnet-5-5", "Claude Sonnet 5.5"},
		{kindAnthropic, "claude-haiku-4-5-20251001", "Claude Haiku 4.5"},
		{kindAnthropic, "claude-fable-5-1", "Claude Fable 5.1"},
		{kindAnthropic, "claude-opus-5", "Claude Opus 5"},
		{kindAnthropic, "claude-opus-5-20251001", "Claude Opus 5"},
		{kindAnthropic, "claude-opus-5-12", "Claude Opus 5.12"},
		{"provider", "gpt-6.1-sol", "gpt-6.1-sol"},
		{"provider", "gpt-6-astra", "gpt-6-astra"},
		{"provider", "kimi-k3", "kimi-k3"},
		{"provider", "claude-opus-5-5", "claude-opus-5-5"},
		{kindAnthropic, "claude-3-5-sonnet-20241022", "claude-3-5-sonnet-20241022"},
		{kindAnthropic, "claude-opus-5-123", "claude-opus-5-123"},
		{kindAnthropic, "claude-opus-5-5-extra", "claude-opus-5-5-extra"},
		{kindAnthropic, "", ""},
	} {
		t.Run(tc.kind+"/"+tc.model, func(t *testing.T) {
			if got := classMemberDisplay(classMemberPref{Kind: tc.kind, Model: tc.model}); got != tc.want {
				t.Fatalf("display = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDeriveClassRowsGuidanceAndFirstChoice(t *testing.T) {
	pointer := "serving"
	members := []classMemberPref{
		{ID: "serving", Priority: 9, Kind: "provider", Model: "kimi-k3"},
		{ID: "first", Priority: 1, Kind: kindAnthropic, Model: "claude-opus-5-5"},
		{ID: "middle", Priority: 4, Kind: "provider", Model: "gpt-6.1-sol"},
	}
	for range 3 {
		before := []string{members[0].ID, members[1].ID, members[2].ID}
		rows := deriveClassRows(map[string]classPref{"capable": {
			Guidance: "Main tier. Settled work.", Members: members, Pointer: &pointer,
		}}, nil, nil)
		if len(rows) != 1 || rows[0].Guidance != "Main tier. Settled work." || rows[0].FirstChoice != "Claude Opus 5.5" ||
			rows[0].DirectModel != "claude-opus-5-5" {
			t.Fatalf("derived guidance = %+v", rows)
		}
		if !slices.Equal(before, []string{members[0].ID, members[1].ID, members[2].ID}) {
			t.Fatal("derive mutated payload member order")
		}
		tombs := deriveClassRows(nil, rows, nil)
		if tombs[0].Guidance != rows[0].Guidance || tombs[0].FirstChoice != rows[0].FirstChoice || tombs[0].DirectModel != rows[0].DirectModel {
			t.Fatalf("tombstone lost guidance: %+v", tombs)
		}
		members = append(members[1:], members[0])
	}
	// A provider first choice still has a direct stand-in when an Anthropic member follows it.
	rows := deriveClassRows(map[string]classPref{"frontier": {Members: []classMemberPref{
		{ID: "gpt", Priority: 1, Kind: "provider", Model: "gpt-6-astra"},
		{ID: "fable", Priority: 2, Kind: kindAnthropic, Model: "claude-fable-5-1"},
	}}, "kimi": {Members: []classMemberPref{{ID: "kimi", Priority: 1, Kind: "provider", Model: "kimi-k3"}}}}, nil, nil)
	if rows[0].Name != "frontier" || rows[0].FirstChoice != "gpt-6-astra" || rows[0].DirectModel != "claude-fable-5-1" {
		t.Fatalf("provider-first class = %+v", rows[0])
	}
	if rows[1].Name != "kimi" || rows[1].DirectModel != "" {
		t.Fatalf("provider-only class = %+v", rows[1])
	}
	rows = deriveClassRows(map[string]classPref{"empty": {}}, nil, nil)
	if rows[0].FirstChoice != "" || rows[0].Guidance != "" || rows[0].DirectModel != "" {
		t.Fatalf("memberless class = %+v", rows)
	}
}

func TestE2EClassesEndpoint(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	withClientSecret(t, env, "classes-test-secret")
	rows := []registryEntry{
		{Name: "zeta", Class: "zeta", Type: "class", Display: "Zeta", ContextWindow: 200000},
		{Name: "capable", Class: "capable", Type: "class", Display: "Delegation: capable", ContextWindow: 1000000, Guidance: "Main tier.", FirstChoice: "Claude Opus 5.5", DirectModel: "claude-opus-5-5"},
		{Name: "removed", Class: "removed", Type: "class", disabledReason: classRemovedReason},
		{Name: "hidden", Class: "hidden", Type: "class", pickerHidden: true},
	}
	withClassRows(t, "preferred", rows...)
	request := func(method, path string, auth bool) (int, []byte) {
		t.Helper()
		req, _ := http.NewRequest(method, env.proxy.URL+path, nil)
		if auth {
			req.Header.Set("Authorization", "Bearer classes-test-secret")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(resp.Header.Get("Content-Type"), "application/json") {
			t.Fatalf("not JSON: %s", body)
		}
		return resp.StatusCode, body
	}
	if status, _ := request("GET", "/_switch/classes", false); status != 401 {
		t.Fatalf("ungated status = %d", status)
	}
	if status, _ := request("POST", "/_switch/classes", true); status != 405 {
		t.Fatalf("POST status = %d", status)
	}
	type response struct {
		Revision string           `json:"revision"`
		Source   string           `json:"source"`
		Classes  []map[string]any `json:"classes"`
	}
	get := func() response {
		t.Helper()
		status, body := request("GET", "/_switch/classes", true)
		if status != 200 {
			t.Fatalf("GET status = %d: %s", status, body)
		}
		var got response
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	got := get()
	if got.Source != "preferred" || !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(got.Revision) || len(got.Classes) != 2 {
		t.Fatalf("response = %+v", got)
	}
	want := map[string]any{"class": "capable", "id": "claude-capable[1m]", "display": "Delegation: capable", "context_window": float64(1000000), "guidance": "Main tier.", "first_choice": "Claude Opus 5.5", "direct_model": "claude-opus-5-5"}
	wantJSON, _ := json.Marshal(want)
	gotJSON, _ := json.Marshal(got.Classes[0])
	if string(gotJSON) != string(wantJSON) {
		t.Fatalf("row = %s, want %s", gotJSON, wantJSON)
	}
	if value, ok := got.Classes[1]["guidance"]; !ok || value != nil || got.Classes[1]["class"] != "zeta" {
		t.Fatalf("absent guidance / ordering = %+v", got.Classes[1])
	}
	// Verify the hash against the exact served array bytes, not a map re-encoding.
	_, body := request("GET", "/_switch/classes", true)
	var raw struct {
		Classes json.RawMessage `json:"classes"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw.Classes)
	if got.Revision != hex.EncodeToString(digest[:])[:16] {
		t.Fatalf("revision does not hash served array: %s", got.Revision)
	}
	status, modelsBody := request("GET", "/v1/models", true)
	var models struct {
		Data []struct {
			ID   string `json:"id"`
			Kind string `json:"kind"`
		} `json:"data"`
	}
	if status != 200 {
		t.Fatalf("models status = %d", status)
	}
	if err := json.Unmarshal(modelsBody, &models); err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, m := range models.Data {
		if m.Kind == kindClass {
			ids[m.ID] = true
		}
	}
	if len(ids) != len(got.Classes) {
		t.Fatalf("discovery class set = %v", ids)
	}
	for _, row := range got.Classes {
		if !ids[row["id"].(string)] {
			t.Fatalf("id not advertised: %v", row)
		}
	}
	if again := get(); again.Revision != got.Revision {
		t.Fatal("same content changed revision")
	}
	lan.setClassRows(slices.Clone(rows), "last-good")
	if again := get(); again.Revision != got.Revision || again.Source != "last-good" {
		t.Fatalf("restart/source changed revision: %+v", again)
	}
	changed := slices.Clone(rows)
	changed[1].Guidance = "Updated purpose."
	lan.setClassRows(changed, "preferred")
	if again := get(); again.Revision == got.Revision {
		t.Fatal("guidance change did not change revision")
	}
	lan.setClassRows(nil, "preferred")
	if empty := get(); len(empty.Classes) != 0 {
		t.Fatalf("empty load = %+v", empty)
	}
	withClassRows(t, "")
	if status, body := request("GET", "/_switch/classes", true); status != 503 || !strings.Contains(string(body), "registry has not loaded yet") || !strings.Contains(string(body), "brain is unreachable") {
		t.Fatalf("cold status = %d: %s", status, body)
	}
}

func TestClassGuidancePersistence(t *testing.T) {
	withClassRows(t, "")
	path := filepath.Join(t.TempDir(), "classes.json")
	withClassStateFile(t, path)
	rows := []registryEntry{{Name: "fast", Class: "fast", Type: "class", Display: "Fast", ContextWindow: 200000, Guidance: "Small, fast tier.", FirstChoice: "gpt-6-astra"}}
	lan.persistClassRows(rows)
	lan.loadClassRowsState()
	loaded, source := lan.classRowsSnapshot()
	if source != "last-good" || !slices.Equal(loaded, rows) {
		t.Fatalf("round trip = %+v / %s", loaded, source)
	}
	if err := os.WriteFile(path, []byte(`{"savedAt":1,"rows":[{"name":"fast","class":"fast","display":"Fast","context_window":200000}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	lan.loadClassRowsState()
	loaded, source = lan.classRowsSnapshot()
	if source != "last-good" || len(loaded) != 1 || loaded[0].Guidance != "" || loaded[0].FirstChoice != "" {
		t.Fatalf("old snapshot = %+v / %s", loaded, source)
	}
}
