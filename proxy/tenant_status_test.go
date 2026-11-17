package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// The household golden (MEMBERS-PLAN 1b): /_switch/status for the household
// is what it was, under the new nesting. Every key the top level had before
// the cut is either at the top still (the proxy's own) or in
// members[lan_tenant] (the household's), never both and never gone; the
// member block adds its row (tenant, name, lan, tap) and the top adds
// lan_tenant, members and tenants.
func TestStatusHouseholdGolden(t *testing.T) {
	env, restore := newE2E(t)
	defer restore()
	env.post(t, "claude-haiku-4-5", "CLIENT").Body.Close()
	env.hit(t)

	perTenant := strings.Fields(`anthropic_tokens api_keys brain_reachable class_bindings class_funding
		codex_bearers_ttl effective_account effective_by_lane injection lane_mode lane_rules lanes ledger
		member_health preferred_account preferred_reason refusal_records reports state_by_lane tokens_cached_ttl`)
	global := strings.Fields(`bridge_configured client_models_cache direct_providers events lane_parse_failures
		mode registry requests response_tap service switches uptime_seconds usage_scaling`)

	top := statusTop(t, env.proxy.URL)
	var mine map[string]any
	if err := json.Unmarshal(statusMine(t, env.proxy.URL), &mine); err != nil {
		t.Fatal(err)
	}
	for _, k := range perTenant {
		if _, ok := mine[k]; !ok {
			t.Errorf("members[lan_tenant] lacks %q, which status had before the cut", k)
		}
		if _, ok := top[k]; ok {
			t.Errorf("%q is at the top as well as under the member", k)
		}
	}
	for _, k := range global {
		if _, ok := top[k]; !ok {
			t.Errorf("status lacks %q at the top, where it was before the cut", k)
		}
		if _, ok := mine[k]; ok {
			t.Errorf("%q is under the member as well as at the top", k)
		}
	}
	for k, want := range map[string]any{"tenant": testLanTenant, "name": "household", "lan": true, "tap": tapContent} {
		if mine[k] != want {
			t.Errorf("members[lan_tenant].%s = %v, want %v", k, mine[k], want)
		}
	}
	for _, k := range []string{"lan_tenant", "members", "tenants"} {
		if _, ok := top[k]; !ok {
			t.Errorf("status lacks %q at the top", k)
		}
	}
	members, _ := top["members"].(map[string]any)
	if len(members) != 1 {
		t.Errorf("members = %d tenants, want the household alone", len(members))
	}
}
