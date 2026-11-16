package main

import "testing"

// Stage 0 (#120): SWITCH_HEALTH parses per spec §8 and defaults off; nothing
// else may read the mode yet, so parsing is the whole observable contract.
func TestParseHealthMode(t *testing.T) {
	cases := []struct {
		in      string
		mode    healthMode
		classes string // healthEnforceClasses rendering of the allowlist
	}{
		{"", healthOff, "all"},
		{"off", healthOff, "all"},
		{"OFF", healthOff, "all"},
		{" observe ", healthObserve, "all"},
		{"enforce", healthEnforce, "all"},
		{"enforce:fast", healthEnforce, "fast"},
		{"Enforce:frontier, delegate-fast", healthEnforce, "delegate-fast,frontier"},
		{"enforce:", healthOff, "all"},    // malformed: no classes after the colon
		{"enforce:, ,", healthOff, "all"}, // malformed: only empty class names
		{"bogus", healthOff, "all"},
	}
	for _, c := range cases {
		mode, classes := parseHealthMode(c.in)
		if mode != c.mode {
			t.Errorf("parseHealthMode(%q) mode = %v, want %v", c.in, mode, c.mode)
		}
		oldFor := healthEnforceFor
		healthEnforceFor = classes
		if got := healthEnforceClasses(); got != c.classes {
			t.Errorf("parseHealthMode(%q) classes = %q, want %q", c.in, got, c.classes)
		}
		healthEnforceFor = oldFor
	}
}
