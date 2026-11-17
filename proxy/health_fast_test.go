package main

import (
	"context"
	"testing"
	"time"
)

func TestFastRouteKeepsHealthyCapacityOffRecoveryProbes(t *testing.T) {
	for _, boundState := range []string{hsHealthy, hsOpen, hsHalfOpen} {
		t.Run(boundState, func(t *testing.T) {
			c := testClock(t)
			h := newHS()
			armHealth(t, h)
			a, b, ready := uMember(uniq("bound")), uMember(uniq("probe")), uMember(uniq("ready"))
			halfOpenK(t, h, c, b.ID)
			switch boundState {
			case hsOpen:
				tripK(t, h, c, a.ID)
			case hsHalfOpen:
				halfOpenK(t, h, c, a.ID)
			}
			// Even a freshly re-pointed binding or an old half-open election
			// cannot spend healthy fast capacity on recovery.
			sess := uniq("fast-session")
			lan.bindMu.Lock()
			lan.bindCache[bindKey("fast", sess)] = classBind{member: a, fetchedAt: time.Now(), memberSince: time.Now()}
			lan.bindMu.Unlock()
			t.Cleanup(func() { lan.bindMu.Lock(); delete(lan.bindCache, bindKey("fast", sess)); lan.bindMu.Unlock() })
			h.elect(a.ID, "fast", b.ID)
			lead := lan.decideHealthRoute(context.Background(), "fast", sess, a, []classMemberPref{a, b, ready}, []byte(`{}`), false, uniq("request"), false)
			want := ready.ID
			if boundState == hsHealthy {
				want = a.ID
			}
			if lead.m.ID != want || lead.lease != nil || lead.hopKind == "probe" {
				t.Fatalf("got %+v, want healthy %s without a probe", lead, want)
			}
			if !snap(t, h, b.ID).probeTok {
				t.Fatal("fast request consumed the recovery token")
			}
		})
	}
}

func TestFastRouteHonorsPinsAndProbesWithoutHealthyCapacity(t *testing.T) {
	for _, condition := range []string{"pinned", "unusable", "cooling", "spent", "sole"} {
		t.Run(condition, func(t *testing.T) {
			c := testClock(t)
			h := newHS()
			armHealth(t, h)
			a, b := uMember(uniq("probe")), uMember(uniq("other"))
			halfOpenK(t, h, c, a.ID)
			members := []classMemberPref{a, b}
			switch condition {
			case "unusable":
				members[1] = uUnroutable(members[1])
			case "cooling":
				uSpent(b)
			case "spent":
				v := 100.0
				members[1].Effective = &v
			case "sole":
				members = members[:1]
			}
			lead := lan.decideHealthRoute(context.Background(), "fast", uniq("s"), a, members, []byte(`{}`), false, uniq("request"), condition == "pinned")
			if lead.m.ID != a.ID || lead.lease == nil {
				t.Fatalf("got %+v, want original probe when %s", lead, condition)
			}
		})
	}
}
