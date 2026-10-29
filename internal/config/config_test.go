package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func write(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoad(t *testing.T) {
	path := write(t, "config.json", `{
		"database": "data/centric.db",
		"env_file": "/etc/x.env",
		"endpoints": ["styles", {"name": "boms", "limit": 200}, "bom_lines"],
		"http": {"timeout_seconds": 30}
	}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database != filepath.Join(filepath.Dir(path), "data/centric.db") || cfg.EnvFile != "/etc/x.env" {
		t.Fatalf("paths %s %s", cfg.Database, cfg.EnvFile)
	}
	if len(cfg.Endpoints) != 3 || cfg.Endpoints[0].Limit != 50 || cfg.Endpoints[1].Limit != 200 ||
		cfg.Endpoints[1].Path != "apparel_bom_revisions" || cfg.Endpoints[2].Limit != 500 {
		t.Fatalf("endpoints %+v", cfg.Endpoints)
	}
	if cfg.HTTP.Timeout != 30*time.Second || cfg.HTTP.Attempts != 6 || cfg.Concurrency != 3 || cfg.Overlap != 10*time.Minute {
		t.Fatalf("defaults %+v", cfg)
	}
	if eps, err := cfg.Select([]string{"bom_lines", "styles"}); err != nil || len(eps) != 2 || eps[0].Name != "styles" {
		t.Fatalf("select %v %v", eps, err)
	}
	if _, err := cfg.Select([]string{"nope"}); err == nil {
		t.Fatal("unknown endpoint selected")
	}
}

func TestLoadRejects(t *testing.T) {
	for name, content := range map[string]string{
		"no database":  `{}`,
		"unknown key":  `{"database": "x", "databse": "y"}`,
		"unknown ep":   `{"database": "x", "endpoints": ["stylez"]}`,
		"twice":        `{"database": "x", "endpoints": ["styles", "styles"]}`,
		"ep field":     `{"database": "x", "endpoints": [{"name": "styles", "limt": 5}]}`,
		"bad http":     `{"database": "x", "http": {"attempts": 0}}`,
		"bad parallel": `{"database": "x", "concurrency": 0}`,
	} {
		if _, err := Load(write(t, "c.json", content)); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestCredentials(t *testing.T) {
	env := write(t, "c.env", "# comment\nexport CENTRIC_BASE_URL=acme\nCENTRIC_USERNAME='user'\nCENTRIC_PASSWORD=\"p=w\"\n")
	cfg := &Config{EnvFile: env}
	// The caller's own credentials must neither leak in nor show in a failure message.
	t.Setenv("CENTRIC_BASE_URL", "")
	t.Setenv("CENTRIC_PASSWORD", "")
	t.Setenv("CENTRIC_USERNAME", "from-env")
	creds, err := cfg.LoadCredentials()
	if err != nil {
		t.Fatal(err)
	}
	if creds.BaseURL != "acme" || creds.Username != "from-env" || creds.Password != "p=w" {
		t.Fatalf("base %q user %q, password as in the file: %v", creds.BaseURL, creds.Username, creds.Password == "p=w")
	}
	cfg.EnvFile = write(t, "d.env", "CENTRIC_BASE_URL=x\n")
	t.Setenv("CENTRIC_USERNAME", "")
	if _, err := cfg.LoadCredentials(); err == nil || !strings.Contains(err.Error(), "CENTRIC_USERNAME, CENTRIC_PASSWORD") {
		t.Fatalf("missing: %v", err)
	}
}
