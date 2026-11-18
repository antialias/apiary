package main

// A class the brain marks lan_only is the LAN tenant's alone: its row rides
// the class rows (they come from the LAN tenant's document) and every list a
// tenant is shown filters it for any tenant but the LAN one.

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

// lanOnlyRows installs one lan_only class row and one every tenant sees.
func lanOnlyRows(t *testing.T) {
	t.Helper()
	classRowsMu.Lock()
	oldRows, oldSource := classRowsSet, classRowsSource
	classRowsSet = deriveClassRows(map[string]classPref{
		"private": {Display: "Private", ContextWindow: 200000, LanOnly: true,
			Members: []classMemberPref{anthropicMember("lane:default")}},
		"shared": {Display: "Shared", ContextWindow: 200000,
			Members: []classMemberPref{anthropicMember("lane:default")}},
	}, nil, nil)
	classRowsSource = "brain"
	classRowsMu.Unlock()
	t.Cleanup(func() {
		classRowsMu.Lock()
		classRowsSet, classRowsSource = oldRows, oldSource
		classRowsMu.Unlock()
	})
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

func TestLanOnlyRowFromPreferred(t *testing.T) {
	var p classPref
	if err := json.Unmarshal([]byte(`{"display":"P","context_window":200000,"lan_only":true}`), &p); err != nil || !p.LanOnly {
		t.Fatalf("lan_only not decoded: %+v %v", p, err)
	}
	rows := deriveClassRows(map[string]classPref{"p": p}, nil, nil)
	if len(rows) != 1 || !rows[0].LanOnly {
		t.Fatalf("row does not carry lan_only: %+v", rows)
	}
	// Persisted with the last-good rows, so a boot without the brain keeps it.
	b, _ := json.Marshal(rows[0])
	var back registryEntry
	if err := json.Unmarshal(b, &back); err != nil || !back.LanOnly {
		t.Fatalf("lan_only lost on the last-good round trip: %s", b)
	}
}

func TestModelsOmitLanOnlyClassForAMember(t *testing.T) {
	lanOnlyRows(t)
	lan := &tenant{id: "ten_lan", lan: true}
	member := &tenant{id: "ten_member"}
	if got := modelsIDsFor(t, lan); !got["claude-private"] || !got["claude-shared"] {
		t.Fatalf("the LAN tenant's /v1/models = %v, want both classes", got)
	}
	for name, tn := range map[string]*tenant{"member": member, "unstamped": nil} {
		got := modelsIDsFor(t, tn)
		if got["claude-private"] || !got["claude-shared"] {
			t.Fatalf("%s /v1/models = %v, want shared and not private", name, got)
		}
	}
}

func TestClassesForOmitLanOnlyClassForAMember(t *testing.T) {
	lanOnlyRows(t)
	list := func(serve func(*httptest.ResponseRecorder)) map[string]bool {
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
	req := httptest.NewRequest("GET", "/_switch/classes", nil)
	all := list(func(w *httptest.ResponseRecorder) { classesHandler(w, req) })
	lan := list(func(w *httptest.ResponseRecorder) { classesHandlerFor(w, req, &tenant{id: "ten_lan", lan: true}) })
	mem := list(func(w *httptest.ResponseRecorder) { classesHandlerFor(w, req, &tenant{id: "ten_member"}) })
	if !all["private"] || !lan["private"] || !lan["shared"] {
		t.Fatalf("the LAN lists = %v / %v, want private and shared", all, lan)
	}
	if mem["private"] || !mem["shared"] {
		t.Fatalf("the member's list = %v, want shared and not private", mem)
	}
}
