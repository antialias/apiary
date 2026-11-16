package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
)

// classGuidanceRow is the client-facing class contract. Guidance is explicitly
// null when absent, including last-good snapshots written by an older proxy.
type classGuidanceRow struct {
	Class         string  `json:"class"`
	ID            string  `json:"id"`
	Display       string  `json:"display"`
	ContextWindow int     `json:"context_window"`
	Guidance      *string `json:"guidance"`
	FirstChoice   string  `json:"first_choice"`
	// DirectModel is the Anthropic model that stands in for the class in a
	// session that bypasses the proxy; "" when the class has no Anthropic member.
	DirectModel string `json:"direct_model"`
}

func classesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "GET required"})
		return
	}
	if !classRowsLoaded() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "the workload-class registry has not loaded yet — this proxy has not had a successful read of the brain's /preferred (it just started, or the brain is unreachable). Retry shortly"})
		return
	}
	rows, source := classRowsSnapshot()
	classes := make([]classGuidanceRow, 0, len(rows))
	for _, e := range rows {
		// Keep the same advertise-set as registryClassRows (/v1/models).
		if e.disabledReason != "" || e.pickerHidden {
			continue
		}
		var guidance *string
		if e.Guidance != "" {
			g := e.Guidance
			guidance = &g
		}
		classes = append(classes, classGuidanceRow{
			Class: e.Class, ID: e.renderedID(), Display: e.Display,
			ContextWindow: e.ContextWindow, Guidance: guidance, FirstChoice: e.FirstChoice,
			DirectModel: e.DirectModel,
		})
	}
	sort.Slice(classes, func(i, j int) bool { return classes[i].Class < classes[j].Class })
	// Hash only the served array: source and process lifetime are not content.
	encoded, _ := json.Marshal(classes)
	digest := sha256.Sum256(encoded)
	_ = json.NewEncoder(w).Encode(struct {
		Revision string             `json:"revision"`
		Source   string             `json:"source"`
		Classes  []classGuidanceRow `json:"classes"`
	}{hex.EncodeToString(digest[:])[:16], source, classes})
}
