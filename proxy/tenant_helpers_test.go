package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The tests' tenant (MEMBERS-PLAN 1b): the household, installed as the lan
// tenant before any test runs, with no background work (no stream, no
// reporter, no health loop — each test that wants one starts it). Every
// per-tenant read in a test goes through lan; a test of several tenants
// installs its own index (installTenants).

// testLanTenant is the tests' lan tenant id. The core names no tenant of
// ours, so in the shared edition it is any well-formed id; in ours,
// tenant_household_test.go sets it to the household's (householdTenant)
// before any init runs, so the household's tests see the real id.
var testLanTenant = "ten_testhousehold" + "aaaaaaaaaaaaa"

// lan is the household: the lan tenant of the tests' index.
var lan *tenant

func init() {
	lan = installTenants(tenantRow{Tenant: testLanTenant, Name: "household", Lan: true, Tap: tapContent})
}

// installTenants replaces the index with rows, started nowhere, and returns
// the lan row's tenant (nil when rows mark none). The tenants it drops are
// stopped first, as applyTenantIndex stops one that leaves, so nothing of a
// test's index runs into the next test. Tests that replace the index
// restore the household with resetTenants.
func installTenants(rows ...tenantRow) *tenant {
	membersMu.Lock()
	old := members
	membersMu.Unlock()
	for _, tn := range old {
		tn.stop()
	}
	membersMu.Lock()
	defer membersMu.Unlock()
	members = map[string]*tenant{}
	lanID = ""
	membersSource = "test"
	membersRefusal = nil
	for _, row := range rows {
		tn := newTenant(row)
		members[tn.id] = tn
		if row.Lan {
			lanID = tn.id
		}
	}
	if lanID == "" {
		return nil
	}
	return members[lanID]
}

// resetTenants puts a fresh household back as the only tenant and points
// lan at it.
func resetTenants() {
	lan = installTenants(tenantRow{Tenant: testLanTenant, Name: "household", Lan: true, Tap: tapContent})
}

// tpath is the household's path on the brain for p: what a fake brain
// serves /preferred, /token, /events, /reports, /class-bind, /api-key and
// /openai-token under (the route table of MEMBERS-PLAN 1b).
func tpath(p string) string { return "/t/" + testLanTenant + p }

// renderHealthMetrics is one store's exposition on its own, without a
// tenant label: the store's series as these tests have always read them.
// /_switch/metrics renders each tenant's store under its tenant label
// (TestE2EMetricsEndpointServesScoredTraffic, the tenant tests).
func renderHealthMetrics(hs *healthStore) string {
	m := newMetricsBuf()
	hs.renderMetrics(m, nil)
	return m.String()
}

// statusMine is the household's block of /_switch/status at proxyURL:
// members[lan_tenant], where every per-tenant key lives (MEMBERS-PLAN 1b).
// The proxy-wide keys stay at the top; decode the whole body for those.
func statusMine(t *testing.T, proxyURL string) json.RawMessage {
	t.Helper()
	resp, err := http.Get(proxyURL + "/_switch/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var top struct {
		LanTenant string                     `json:"lan_tenant"`
		Members   map[string]json.RawMessage `json:"members"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&top); err != nil {
		t.Fatalf("status not JSON: %v", err)
	}
	if top.LanTenant != testLanTenant {
		t.Fatalf("status lan_tenant = %q, want %q", top.LanTenant, testLanTenant)
	}
	mine := top.Members[top.LanTenant]
	if len(mine) == 0 {
		t.Fatalf("status has no members[%s] block", top.LanTenant)
	}
	return mine
}

// statusTop is the whole of /_switch/status at proxyURL: the proxy-wide keys
// (registry, events, lane_parse_failures, lan_tenant, members...).
func statusTop(t *testing.T, proxyURL string) map[string]any {
	t.Helper()
	resp, err := http.Get(proxyURL + "/_switch/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var top map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&top); err != nil {
		t.Fatalf("status not JSON: %v", err)
	}
	return top
}

// addTenant puts row beside the index's tenants, started nowhere, and
// returns it. removeTenant takes it out again.
func addTenant(row tenantRow) *tenant {
	membersMu.Lock()
	defer membersMu.Unlock()
	tn := newTenant(row)
	members[tn.id] = tn
	return tn
}

func removeTenant(id string) {
	membersMu.Lock()
	defer membersMu.Unlock()
	delete(members, id)
}

// fetchPreferredFor is fetchPreferred for any tenant: one read of its
// /preferred, through applyPreferred, with its prewarm joined.
func fetchPreferredFor(tn *tenant) {
	req, _ := http.NewRequest("GET", tn.brainURL()+"/preferred", nil)
	resp, err := brainClient.Do(req)
	if err != nil {
		tn.brainFail("fetch", err)
		return
	}
	defer resp.Body.Close()
	tn.applyPreferred(resp.Body)
	prewarmWG.Wait()
}

// tbase is the household's base on a fake brain at root: what reportPost
// and the per-tenant brain calls take (tenant.brainURL()).
func tbase(root string) string { return root + "/t/" + testLanTenant }

// lanCtx is a context stamped with the lan tenant, as the :8787 entry
// (clientGate) stamps every request's: for a test that calls a helper below
// the entry directly. Nothing below the entry maps a bare context to the
// household.
func lanCtx() context.Context { return withTenant(context.Background(), lan) }

// lanRequest is httptest.NewRequest with the lan tenant stamped, for a test
// that serves a handler below clientGate directly.
func lanRequest(method, target string, body io.Reader) *http.Request {
	r := httptest.NewRequest(method, target, body)
	return r.WithContext(withTenant(r.Context(), lan))
}
