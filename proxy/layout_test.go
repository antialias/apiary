package main

import (
	"os"
	"path/filepath"
	"testing"
)

// The proxy's tests run in two layouts: llm-routing's trunk, where the proxy is
// services/claude-switch-proxy beside the brain (claude-usage-exporter) and
// Scry (routing-observability), and the Apiary export, where it is proxy/
// beside brain/ and there is no Scry. Which one is decided by the brain's
// directory, never by what happens to be missing: a trunk without Scry fails.

func isDir(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.IsDir()
}

// scryCopy is the path of Scry's copy of a golden the proxy writes and checks
// (rel is under Scry's directory). In the trunk Scry must be there; in the
// Apiary export, which has no Scry, the check is skipped, saying so. Neither
// layout fails the test.
func scryCopy(t *testing.T, rel string) string {
	t.Helper()
	switch {
	case isDir("../claude-usage-exporter"):
		if !isDir("../routing-observability") {
			t.Fatalf("trunk layout, but no ../routing-observability: Scry's copy %s must be checked here", rel)
		}
		return filepath.Join("..", "routing-observability", rel)
	case isDir("../brain"):
		t.Skipf("Apiary export: no Scry beside the proxy, so its copy %s is checked in the trunk only", rel)
	default:
		t.Fatalf("unknown layout: neither ../claude-usage-exporter (trunk) nor ../brain (Apiary) is beside the proxy")
	}
	return ""
}
