package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// A class in priority order is walked in priority order across funding: a
// run of neighbours of one funding ranks among itself by tier (a throttled
// member yields to a healthy neighbour), never past a member of the other
// funding.
func TestPriorityOrderKeepsTheOwnersOrderAcrossFunding(t *testing.T) {
	inc, paid := dialCheck{Check: checkIncluded}, dialCheck{Check: checkPaid}
	r := func(id string, prio int, c dialCheck, tier int) ranked {
		return ranked{m: classMemberPref{ID: id, Priority: prio}, check: c, tier: tier}
	}
	// As the funding ranker leaves them: included first (c busy), then paid.
	in := []ranked{r("a", 1, inc, 0), r("d", 4, inc, 0), r("c", 3, inc, tierOpenBase), r("b", 2, paid, tierPaid)}
	var got []string
	for _, rc := range priorityOrder(in) {
		got = append(got, rc.m.ID)
	}
	if strings.Join(got, ",") != "a,b,d,c" {
		t.Fatalf("order %v, want a (included), b (paid), then d before the throttled c", got)
	}
	if in[1].m.ID != "d" {
		t.Fatal("the input was reordered in place")
	}
}

// /preferred's `order: "priority"` marks every member of that class, and no
// other class's.
func TestStampOrderMarksTheClassesInPriorityOrder(t *testing.T) {
	var classes map[string]classPref
	if err := json.Unmarshal([]byte(`{
		"p": {"order": "priority", "members": [{"id": "x", "priority": 1}, {"id": "y", "priority": 2}]},
		"f": {"members": [{"id": "z", "priority": 1}]}}`), &classes); err != nil {
		t.Fatal(err)
	}
	stampOrder(classes)
	if !inPriorityOrder(classes["p"].Members) || !classes["p"].Members[1].priorityOrder {
		t.Fatalf("p not in priority order: %+v", classes["p"].Members)
	}
	if inPriorityOrder(classes["f"].Members) {
		t.Fatal("a class with no order is in priority order")
	}
}
