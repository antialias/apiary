package main

// Fast-class recovery must not borrow a customer's latency budget. This first
// rollout probes only static-bearer Anthropic-wire members (currently Kimi).
// Token-refreshing wires retain traffic-driven recovery until their credential
// acquisition can share this deadline. No user body, fallback, or account swap
// is involved, and the ordinary FSM lease remains the single-flight authority.
import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// The shared fast capability is also the health-recovery canary.
const recoveryClass = "fast"
const recoveryInterval = time.Minute
const recoveryDeadline = 20 * time.Second
const recoveryMaxBytes = 64 << 10

// One runner per tenant (tenant.healthRecovery): it probes the tenant's own
// fast members on the tenant's own credentials.
type recoveryRunner struct {
	tenant    *tenant
	transport *failoverTransport
	deadline  time.Duration
	mu        sync.Mutex
	busy      bool
	next      time.Time
	lastKey   string
	lastAt    time.Time
	duration  time.Duration
	result    string
	totals    map[string]int64
}

func (r *recoveryRunner) snapshot() map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	totals := map[string]int64{}
	for k, v := range r.totals {
		totals[k] = v
	}
	return map[string]any{"scope": "fast/static-bearer", "active": healthEnforcedClass(recoveryClass),
		"deadline_s": r.deadline.Seconds(), "interval_s": recoveryInterval.Seconds(), "max_response_bytes": recoveryMaxBytes,
		"in_flight": r.busy, "next_check_at": timeValue(r.next), "last_key": r.lastKey,
		"last_started_at": timeValue(r.lastAt), "last_duration_s": r.duration.Seconds(), "last_result": r.result, "totals": totals}
}

func (r *recoveryRunner) run(ctx context.Context) {
	ticker := time.NewTicker(recoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.pass(ctx)
		}
	}
}

func (r *recoveryRunner) pass(ctx context.Context) {
	tn := r.tenant
	if ctx.Err() != nil || tn.health == nil || !healthEnforcedClass(recoveryClass) {
		return
	}
	r.mu.Lock()
	if r.busy || nowFn().Before(r.next) {
		r.mu.Unlock()
		return
	}
	r.busy, r.next = true, nowFn().Add(recoveryInterval)
	last := r.lastKey
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.busy = false; r.mu.Unlock() }()
	tn.pref.mu.RLock()
	members := append([]classMemberPref(nil), tn.pref.classes[recoveryClass].Members...)
	tn.pref.mu.RUnlock()
	// Round robin prevents a repeatedly abandoned member starving another.
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
	cut := sort.Search(len(members), func(i int) bool { return members[i].ID > last })
	members = append(members[cut:], members[:cut]...)
	// A probe never spends money (§2.2 row 13): only a member ranked on an
	// included check is chosen, read from the check itself so no tier
	// renumbering can open a paid probe; the probe dials only on its own
	// dial-time check (probe).
	for _, rc := range tn.rankCandidates(members, "", rankOpt{est: 32, health: true, ctx: ctx}) {
		p := providerByName(rc.m.Provider)
		if rc.m.Kind != "provider" || p == nil || rc.health != hsHalfOpen || !rc.check.included() {
			continue
		}
		if _, ok := p.wire.(bearerWire); !ok {
			continue
		}
		id := "recovery-" + nextDecisionID()
		lease := tn.health.claimProbe(rc.m.ID, recoveryClass, rc.m.ID, id, 32)
		if lease == nil {
			continue
		}
		started := time.Now()
		r.mu.Lock()
		r.lastKey, r.lastAt, r.result = rc.m.ID, nowFn(), "running"
		r.mu.Unlock()
		out, result := r.probe(ctx, rc.m)
		out.key, out.lease = rc.m.ID, lease
		healthRecord(tn.health, out, recoveryClass, id, 0, false)
		r.mu.Lock()
		r.result, r.duration = result, time.Since(started)
		if r.totals == nil {
			r.totals = map[string]int64{}
		}
		r.totals[result]++
		r.mu.Unlock()
		return
	}
}

func (r *recoveryRunner) probe(parent context.Context, member classMemberPref) (hopOutcome, string) {
	ctx, cancel := context.WithTimeout(withTenant(parent, r.tenant), r.deadline)
	defer cancel()
	body := []byte(`{"model":"claude-fast","max_tokens":256,"stream":false,"messages":[{"role":"user","content":"Health check. Reply with exactly OK, no explanation."}]}`)
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://recovery.invalid/v1/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Anthropic-Version", "2023-06-01")
	cc := &classRoute{classReq: classReq{origBody: body, inPath: "/v1/messages", ctx: req.Context()}, tenant: r.tenant, origHeader: req.Header, scale: newScaleMark("", 1)}
	// The dial-time check decides (§2.2 row 13): the standing may have moved
	// since ranking chose this member, and a probe never spends money.
	req, _, c, ok := r.transport.rebuildForMember(req, cc, member, "", false)
	if !ok || !c.included() {
		return hopOutcome{evidence: evNone}, "unavailable"
	}
	// Bypass the request ladder: success must come from this exact member.
	resp, err := r.transport.base.RoundTrip(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		out := classify(member, ctx, nil, err)
		return out, recoveryResult(out)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, recoveryMaxBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		out := classify(member, ctx, nil, err)
		return out, recoveryResult(out)
	}
	if len(data) > recoveryMaxBytes {
		return hopOutcome{evidence: evNone}, "invalid_response"
	}
	resp.Body = io.NopCloser(bytes.NewReader(data))
	if resp.StatusCode != http.StatusOK {
		out := classify(member, ctx, resp, nil)
		return out, recoveryResult(out)
	}
	var msg struct {
		Type    string `json:"type"`
		Stop    string `json:"stop_reason"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if json.Unmarshal(data, &msg) != nil || msg.Type != "message" || msg.Stop != "end_turn" {
		return hopOutcome{evidence: evNone}, "invalid_response"
	}
	var answer strings.Builder
	for _, block := range msg.Content {
		if block.Type == "text" {
			answer.WriteString(block.Text)
		}
	}
	if strings.TrimSpace(answer.String()) != "OK" {
		return hopOutcome{evidence: evNone}, "invalid_response"
	}
	return hopOutcome{evidence: evOK, final: true}, "ok"
}

func recoveryResult(out hopOutcome) string {
	if out.evidence == evDeg {
		if out.cause == "deadline" {
			return "deadline"
		}
		return "degraded"
	}
	return "abandoned"
}
