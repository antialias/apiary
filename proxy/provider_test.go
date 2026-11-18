package main

// Unit tests for the direct-provider table: no provider (local, kimi, gpt)
// advertises an id of its own, the deleted direct ids' namespace, and the
// advertised-model union.
// Routing through the real handler stack lives in e2e_provider_test.go.

import (
	"net/url"
	"testing"
)

// withProviders installs a provider table for the duration of a test.
func withProviders(t *testing.T, ps ...*provider) {
	t.Helper()
	old := providers
	providers = ps
	t.Cleanup(func() { providers = old })
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("bad url %q: %v", raw, err)
	}
	return u
}

// hostedProviderNamespace is what keeps a stale picker id off the lane path:
// every id of the deleted direct rows, in either window spelling, and nothing an
// Anthropic, class or local id can look like.
func TestHostedProviderNamespace(t *testing.T) {
	for model, want := range map[string]string{
		"claude-kimi-k3":         "kimi",
		"claude-kimi-k3[1m]":     "kimi",
		"claude-gpt-6.1-sol":     "gpt",
		"claude-gpt-6.1-sol[1m]": "gpt",
		"claude-gpt-6-luna":      "gpt",
		"claude-gpt-6-astra":     "gpt",
		"claude-gpt-5.6":         "gpt", // a SWITCH_GPT_MODELS id from before the registry
		// The local runtime's deleted claude-local-<tag> rows (#135).
		"claude-local-qwen3.6:35b-a3b": "local",
		"claude-local-gemma4:12b":      "local",
	} {
		if got, ok := hostedProviderNamespace(model); !ok || got != want {
			t.Fatalf("hostedProviderNamespace(%q) = (%q, %v), want (%q, true)", model, got, ok, want)
		}
	}
	for _, model := range []string{
		"claude-fable-5", "claude-opus-5-5[1m]", "claude-haiku-4-5-20251001",
		"claude-frontier",
		"claude-local",              // the local CLASS's id (#135), never the deleted namespace
		"claude-kimi", "claude-gpt", // no "-" after the provider: not the namespace
		"kimi-k3", "gpt-6.1-sol", "",
	} {
		if got, ok := hostedProviderNamespace(model); ok {
			t.Fatalf("hostedProviderNamespace(%q) claimed it for %q", model, got)
		}
	}
}

// The providers are back-ends for class members only (quota-model C2, §2.2 row
// 10; local since #135): none adds a picker row. With no registry and no class
// row, the union is empty whatever the provider table holds.
func TestAdvertisedModelsNoProviderRows(t *testing.T) {
	local := &provider{name: "local", target: mustURL(t, "http://127.0.0.1:11434"), wire: localWire{}}
	kimi := &provider{name: "kimi", target: mustURL(t, "http://x"), wire: bearerWire{account: "kimi"}}
	gpt := &provider{name: "gpt", target: mustURL(t, "http://y"), wire: codexWire{}}
	withProviders(t, local, kimi, gpt)
	oldReg := registry
	registry = nil
	t.Cleanup(func() { registry = oldReg })
	withClassRows(t, "preferred")

	if rows := advertisedModels(); len(rows) != 0 {
		t.Fatalf("advertised = %+v, want none: no provider advertises an id of its own", rows)
	}
}

// #96: the union also carries the registry's advertisement-only Anthropic rows.
// No provider claims them and no class row derives them — they exist purely so
// gateway discovery finds the id it was asked for.
func TestAdvertisedModelsUnionIncludesAnthropicRows(t *testing.T) {
	kimi := &provider{name: "kimi", target: mustURL(t, "http://x"), wire: bearerWire{account: "kimi"}}
	withProviders(t, kimi)

	oldReg := registry
	registry = []registryEntry{
		{Name: "fable-5", Display: "Claude Fable 5", Type: "anthropic", ContextWindow: 200000},
		{Name: "fable-5", Display: "Claude Fable 5 (1M)", Type: "anthropic", ContextWindow: 1_000_000},
	}
	t.Cleanup(func() { registry = oldReg })
	withClassRows(t, "preferred",
		registryEntry{Name: "frontier", Display: "Frontier (auto)", Type: "class", Class: "frontier", ContextWindow: 200000})

	rows := map[string]string{}
	for _, m := range advertisedModels() {
		rows[m.pickerID] = m.display
	}
	if len(rows) != 3 {
		t.Fatalf("advertised union = %v, want exactly the two anthropic rows and the class row", rows)
	}
	for id, display := range map[string]string{
		"claude-fable-5":     "Claude Fable 5",
		"claude-fable-5[1m]": "Claude Fable 5 (1M)",
		"claude-frontier":    "Frontier (auto)",
	} {
		if rows[id] != display {
			t.Fatalf("advertised union: %q display = %q, want %q; got %v", id, rows[id], display, rows)
		}
	}
}
