package main

// Unit tests for the model rewrite and the client picker cache: the
// rewriteModel byte splice every class hop uses, and the gateway-models.json
// file the proxy keeps for Claude Code's /model picker.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRewriteModel(t *testing.T) {
	cases := []struct {
		name, body, want string // want = "" means expect nil (no rewrite)
	}{
		{"model first key",
			`{"model":"claude-local","max_tokens":16,"messages":[]}`, "local-qwen3-coder-30b"},
		{"model last key",
			`{"messages":[{"role":"user","content":"hi"}],"model":"claude-fast"}`, "x"},
		{"model mid with whitespace",
			`{"a":1, "model" :  "claude-capable" , "b":2}`, "t"},
		{"nested model key must not be touched",
			`{"messages":[{"model":"claude-frontier","content":"x"}],"max_tokens":1}`, ""},
		{"no model key", `{"messages":[]}`, ""},
		{"model not a string", `{"model":{"id":"x"}}`, ""},
		{"not an object", `["model","x"]`, ""},
		{"empty body", ``, ""},
		{"truncated json", `{"model":"cl`, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := rewriteModel([]byte(c.body), c.want)
			if c.want == "" {
				if out != nil {
					t.Fatalf("expected nil, got %q", out)
				}
				return
			}
			if out == nil {
				t.Fatalf("expected a rewrite, got nil")
			}
			if !json.Valid(out) {
				t.Fatalf("rewrite produced invalid JSON: %q", out)
			}
			if got := extractModel(out); got != c.want {
				t.Fatalf("top-level model after rewrite = %q, want %q", got, c.want)
			}
			// nothing else may change: unmarshal both and compare non-model keys
			var before, after map[string]json.RawMessage
			_ = json.Unmarshal([]byte(c.body), &before)
			_ = json.Unmarshal(out, &after)
			for k, v := range before {
				if k == "model" {
					continue
				}
				if string(after[k]) != string(v) {
					t.Fatalf("key %q changed: %q -> %q", k, v, after[k])
				}
			}
		})
	}
}

func TestRewriteModelPreservesNestedOnRealShape(t *testing.T) {
	// A realistic Claude Code body: nested "model" strings inside messages/tools
	// must survive verbatim while the top-level one is swapped.
	body := `{"model":"claude-local","messages":[{"role":"user","content":"the model field"},{"role":"assistant","content":[{"type":"tool_use","name":"x","input":{"model":"claude-fable-5"}}]}],"tools":[{"name":"t","description":"pass model=claude-opus-4-8"}]}`
	out := rewriteModel([]byte(body), "local-qwen3-coder-30b")
	if out == nil {
		t.Fatal("expected rewrite")
	}
	s := string(out)
	if !strings.Contains(s, `"model":"claude-fable-5"`) {
		t.Fatalf("nested tool_use model was damaged: %s", s)
	}
	if !strings.Contains(s, "model=claude-opus-4-8") {
		t.Fatalf("tool description was damaged: %s", s)
	}
	if extractModel(out) != "local-qwen3-coder-30b" {
		t.Fatalf("top-level model = %q", extractModel(out))
	}
}

// ---- client picker cache (~/.claude/cache/gateway-models.json) ----

// classAdv builds advertised class rows for the given picker ids — the input
// shape writeClientModelsCache takes. The first id gets the display name the
// shape test checks.
func classAdv(ids ...string) []advertisedModel {
	out := make([]advertisedModel, 0, len(ids))
	for i, id := range ids {
		display := id
		if i == 0 && id == "claude-frontier" {
			display = "Frontier (auto)"
		}
		out = append(out, advertisedModel{pickerID: id, display: display, kind: kindClass})
	}
	return out
}

func TestClientModelsCacheWritesClientShape(t *testing.T) {
	// The client's zod schema: {baseUrl: string, fetchedAt: number, models:
	// [{id: string, display_name?: string}]}, and it only serves the rows when
	// baseUrl string-equals ANTHROPIC_BASE_URL. Mode 0600 matches the client's own
	// writes (mode:384).
	path := filepath.Join(t.TempDir(), "cache", "gateway-models.json")
	wrote, err := writeClientModelsCache(path, "http://127.0.0.1:8787", classAdv("claude-frontier", "claude-fast"))
	if err != nil || !wrote {
		t.Fatalf("wrote=%v err=%v", wrote, err)
	}
	buf, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got clientModelsFile
	if err := json.Unmarshal(buf, &got); err != nil {
		t.Fatal(err)
	}
	if got.BaseURL != "http://127.0.0.1:8787" || got.FetchedAt <= 0 {
		t.Fatalf("bad envelope: %+v", got)
	}
	if len(got.Models) != 2 || got.Models[0].ID != "claude-frontier" || got.Models[0].DisplayName != "Frontier (auto)" {
		t.Fatalf("bad models: %+v", got.Models)
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v, want 0600", fi.Mode().Perm())
	}
}

func TestClientModelsCacheEmptyRowsNeverClobbers(t *testing.T) {
	// The brain down at proxy start must not wipe the picker rows.
	path := filepath.Join(t.TempDir(), "gateway-models.json")
	if _, err := writeClientModelsCache(path, "http://x", classAdv("claude-a")); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	wrote, err := writeClientModelsCache(path, "http://x", nil)
	if err != nil || wrote {
		t.Fatalf("empty rows: wrote=%v err=%v", wrote, err)
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatalf("file changed on an empty write")
	}
}

func TestClientModelsCacheIdempotentAndChangeAware(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway-models.json")
	if wrote, _ := writeClientModelsCache(path, "http://x", classAdv("claude-a")); !wrote {
		t.Fatal("first write should write")
	}
	if wrote, _ := writeClientModelsCache(path, "http://x", classAdv("claude-a")); wrote {
		t.Fatal("identical write should be a no-op (no mtime churn every 60s)")
	}
	if wrote, _ := writeClientModelsCache(path, "http://x", classAdv("claude-a", "claude-b")); !wrote {
		t.Fatal("a new row should rewrite")
	}
	buf, _ := os.ReadFile(path)
	if !strings.Contains(string(buf), "claude-b") {
		t.Fatalf("missing new row: %s", buf)
	}
	if wrote, _ := writeClientModelsCache(path, "http://y", classAdv("claude-a", "claude-b")); !wrote {
		t.Fatal("baseUrl change should rewrite")
	}
}

func TestRemoveClientModelsCacheOnlyOurs(t *testing.T) {
	dir := t.TempDir()
	ours := filepath.Join(dir, "ours.json")
	foreign := filepath.Join(dir, "foreign.json")
	_, _ = writeClientModelsCache(ours, "http://127.0.0.1:8787", classAdv("claude-a"))
	_, _ = writeClientModelsCache(foreign, "http://other-gateway:9999", classAdv("claude-a"))

	if removeClientModelsCache(foreign, "http://127.0.0.1:8787") {
		t.Fatal("removed a cache belonging to a different gateway")
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Fatal("foreign cache should still exist")
	}
	if !removeClientModelsCache(ours, "http://127.0.0.1:8787") {
		t.Fatal("failed to remove our own cache")
	}
	if _, err := os.Stat(ours); !os.IsNotExist(err) {
		t.Fatal("our cache should be gone")
	}
	if removeClientModelsCache(filepath.Join(dir, "absent.json"), "http://x") {
		t.Fatal("removing a missing file should report false")
	}
}
