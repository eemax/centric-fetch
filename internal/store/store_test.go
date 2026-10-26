package store

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestOpenPathWithSpaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a dir", "my db.sqlite")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("database not at %s: %v", path, err)
	}
	ro, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	ro.Close()
}

func TestOpenRefusesForeignAndNewerDatabases(t *testing.T) {
	dir := t.TempDir()
	foreign := filepath.Join(dir, "foreign.db")
	raw, _ := sql.Open("sqlite", foreign)
	raw.Exec(`CREATE TABLE other (x)`)
	raw.Close()
	if _, err := Open(foreign); err == nil || !strings.Contains(err.Error(), "other tables") {
		t.Fatalf("foreign: %v", err)
	}

	newer := filepath.Join(dir, "newer.db")
	db, err := Open(newer)
	if err != nil {
		t.Fatal(err)
	}
	db.Exec(`UPDATE meta_info SET value = '2' WHERE key = 'schema_version'`)
	db.Close()
	if _, err := Open(newer); err == nil || !strings.Contains(err.Error(), "schema version 2") {
		t.Fatalf("newer: %v", err)
	}
	if _, err := OpenReadOnly(filepath.Join(dir, "missing.db")); err == nil {
		t.Fatal("missing database opened")
	}
}

func TestCompareModified(t *testing.T) {
	v := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	for _, c := range []struct {
		a, b sql.NullString
		want int
	}{
		{v("2026-01-01T00:00:00.000Z"), v("2026-01-01T01:00:00+01:00"), 0},
		{v("2026-01-01T00:00:00Z"), v("2026-01-01T00:00:00.001Z"), -1},
		{sql.NullString{}, v("x"), -1},
		{v("x"), sql.NullString{}, 1},
		{v("b"), v("a"), 1},
	} {
		if got := CompareModified(c.a, c.b); got != c.want {
			t.Errorf("%v vs %v: %d, want %d", c.a, c.b, got, c.want)
		}
	}
}
