package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func cli(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := run(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestUsage(t *testing.T) {
	if code, _, _ := cli(t); code != exitUsage {
		t.Fatalf("no args: %d", code)
	}
	if code, out, _ := cli(t, "version"); code != 0 || !strings.HasPrefix(out, "centric-fetch ") {
		t.Fatalf("version: %d %q", code, out)
	}
	if code, _, errOut := cli(t, "fetch", "--config", "/nonexistent.json"); code != exitUsage || !strings.Contains(errOut, "config") {
		t.Fatalf("missing config: %d %q", code, errOut)
	}
	if code, _, _ := cli(t, "fetch", "--allow-mass-delete"); code != exitUsage {
		t.Fatalf("mass delete without full: %d", code)
	}
}

func TestCheckAndStatusWithoutDatabase(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.json")
	os.WriteFile(cfg, []byte(`{"database": "centric.db", "endpoints": ["styles"]}`), 0o600)

	code, out, _ := cli(t, "check", "--config", cfg, "--json")
	var h struct {
		Healthy  bool
		Problems []string
	}
	if code != exitFailed || json.Unmarshal([]byte(out), &h) != nil || h.Healthy || !strings.HasPrefix(h.Problems[0], "database not created yet") {
		t.Fatalf("check: %d %s", code, out)
	}
	if code, out, _ := cli(t, "status", "--config", cfg); code != 0 || !strings.Contains(out, "not created yet") {
		t.Fatalf("status: %d %s", code, out)
	}
	// A fetch without credentials stops before touching anything.
	t.Setenv("CENTRIC_BASE_URL", "")
	if code, _, errOut := cli(t, "fetch", "--config", cfg); code != exitUsage || !strings.Contains(errOut, "CENTRIC_BASE_URL") {
		t.Fatalf("fetch: %d %s", code, errOut)
	}
	if _, err := os.Stat(filepath.Join(dir, "centric.db")); err == nil {
		t.Fatal("database created without credentials")
	}
}
