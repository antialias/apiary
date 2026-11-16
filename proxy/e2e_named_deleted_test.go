package main

// The four named model ids (kimi-k3, gpt-6.1-sol, gpt-6-luna, gpt-6-astra) and
// their policy-free direct path were deleted in quota-model C2 (§2.2 row 10,
// agreed decision 4). Client picker caches have no TTL, so their picker ids keep
// arriving. Each one carries digits, so the class-shaped refusal does not catch
// it, and laneForModel would otherwise put it on the default lane and forward it
// to api.anthropic.com under an account bearer. These tests pin the typed
// refusal through the real handler stack, with the deployed models.json.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
)

// formerNamedIDs is every picker spelling the deleted rows were reachable by:
// the rendered id, and the base id Claude Code sends after stripping "[1m]".
var formerNamedIDs = []string{
	"claude-kimi-k3", "claude-kimi-k3[1m]",
	"claude-gpt-6.1-sol", "claude-gpt-6.1-sol[1m]",
	"claude-gpt-6-luna", "claude-gpt-6-luna[1m]",
	"claude-gpt-6-astra", "claude-gpt-6-astra[1m]",
}

// wantDeletedIDRefusal asserts the typed, permanent refusal: a 503 in the
// Anthropic error envelope (what Claude Code parses), naming the edit, and no
// Retry-After — retrying an id that no longer exists only burns the same error.
func wantDeletedIDRefusal(t *testing.T, what string, resp *http.Response) {
	t.Helper()
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("%s: status %d (%s), want the typed 503", what, resp.StatusCode, raw)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "" {
		t.Fatalf("%s: Retry-After %q on a permanent refusal", what, ra)
	}
	var envl struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &envl); err != nil || envl.Type != "error" || envl.Error.Type != "api_error" {
		t.Fatalf("%s: body %s is not the Anthropic error envelope", what, raw)
	}
	if !strings.Contains(envl.Error.Message, "model ids were removed") ||
		!strings.Contains(envl.Error.Message, "member of a workload class") {
		t.Fatalf("%s: message %q does not say the id is gone and what to use instead", what, envl.Error.Message)
	}
}

func deployedModelsFile(t *testing.T) {
	t.Helper()
	raw, err := os.ReadFile("models.json")
	if err != nil {
		t.Fatal(err)
	}
	withModelsFile(t, string(raw))
}

// With both hosted providers configured as in production, no former id
// reaches any upstream: not kimi, not codex, and above all not Anthropic.
// Streaming and count_tokens take the same arm.
func TestE2EFormerNamedIDsAreRefusedTyped(t *testing.T) {
	deployedModelsFile(t)
	h, restore := newE2EHosted(t, "")
	defer restore()

	for _, id := range formerNamedIDs {
		for _, stream := range []bool{false, true} {
			wantDeletedIDRefusal(t, fmt.Sprintf("%s stream=%v", id, stream), h.postStream(t, id, stream))
		}
		body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}]}`, id)
		wantDeletedIDRefusal(t, id+" count_tokens", h.postPath(t, "/v1/messages/count_tokens", body))
	}
	h.noAnthropicLeak(t)
	h.noHostedHit(t)
	h.noCodexLeak(t)
}

// The refusal does not depend on the provider being configured: on a proxy
// with no hosted provider at all (and no models.json), a stale picker id is
// still refused rather than ridden onto the default lane.
func TestE2EFormerNamedIDsAreRefusedWithNoHostedProvider(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()

	for _, id := range formerNamedIDs {
		wantDeletedIDRefusal(t, id, env.post(t, id, "client-oauth-bearer"))
	}
	env.noUpstream(t)
}

// Nothing advertises a former id any more: not /v1/models, and so not the
// client picker cache that is written from the same list.
func TestE2EFormerNamedIDsAreNotAdvertised(t *testing.T) {
	deployedModelsFile(t)
	h, restore := newE2EHosted(t, "")
	defer restore()

	resp, err := http.Get(h.proxy.URL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var list struct {
		Data []struct {
			ID   string `json:"id"`
			Kind string `json:"kind"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &list); err != nil || len(list.Data) == 0 {
		t.Fatalf("/v1/models: %v %s", err, raw)
	}
	for _, m := range list.Data {
		if _, ok := hostedProviderNamespace(m.ID); ok || m.Kind == "provider" {
			t.Fatalf("/v1/models still advertises %q (kind %q)", m.ID, m.Kind)
		}
	}
}
