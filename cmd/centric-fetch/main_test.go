package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/eemax/centric-fetch/internal/store"
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

func TestHeldLockSkipsWithExit75(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.json")
	os.WriteFile(cfg, []byte(`{"database": "centric.db", "endpoints": ["styles"]}`), 0o600)
	t.Setenv("CENTRIC_BASE_URL", "http://127.0.0.1:1")
	t.Setenv("CENTRIC_USERNAME", "u")
	t.Setenv("CENTRIC_PASSWORD", "p")
	lock, err := store.AcquireLock(filepath.Join(dir, "centric.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()

	for _, args := range [][]string{{}, {"--wait", "1", "--full"}} {
		code, out, _ := cli(t, append([]string{"fetch", "--config", cfg}, args...)...)
		var sum struct {
			Event, Status, Mode, Error string
			Totals                     map[string]int
			Endpoints                  []any
		}
		if code != exitBusy || json.Unmarshal([]byte(out), &sum) != nil || sum.Event != "run" || sum.Status != "skipped" ||
			!strings.Contains(sum.Error, "holds the lock") || sum.Totals["fetched"] != 0 || sum.Endpoints == nil {
			t.Fatalf("%v: %d %s", args, code, out)
		}
		if len(args) > 0 && (sum.Mode != "full" || !strings.Contains(sum.Error, "waited 1")) {
			t.Fatalf("with --wait: %s", out)
		}
	}
	if code, _, _ := cli(t, "fetch", "--config", cfg, "--wait", "soon"); code != exitUsage {
		t.Fatalf("bad --wait: %d", code)
	}
}
