package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/eemax/centric-fetch/internal/pyjson"
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

func TestAcquireLockWait(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.db")
	held, err := AcquireLock(path)
	if err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	if _, err := AcquireLockWait(context.Background(), path, 1200*time.Millisecond); !errors.Is(err, ErrLocked) || time.Since(began) < time.Second {
		t.Fatalf("wait ran out: %v after %s", err, time.Since(began))
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errors.New("SIGTERM"))
	if _, err := AcquireLockWait(ctx, path, time.Minute); err == nil || err.Error() != "SIGTERM" {
		t.Fatalf("cancelled: %v", err)
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		held.Release()
	}()
	l, err := AcquireLockWait(context.Background(), path, 10*time.Second)
	if err != nil {
		t.Fatalf("released while waiting: %v", err)
	}
	l.Release()
}

func TestChangedFields(t *testing.T) {
	obj := func(s string) *pyjson.Object {
		v, err := pyjson.Parse([]byte(s))
		if err != nil {
			t.Fatal(err)
		}
		return v.(*pyjson.Object)
	}
	got := ChangedFields(obj(`{"id":"A","a":1,"b":[1,2],"gone":true}`), obj(`{"id":"A","b":[2,1],"a":1.0,"new":null}`))
	if strings.Join(got, ",") != "b,a,new,gone" {
		t.Fatalf("%v", got)
	}
	if got := ChangedFields(nil, obj(`{"id":"A"}`)); strings.Join(got, ",") != "id" {
		t.Fatalf("against nothing: %v", got)
	}
}

func TestSilentEdgeCases(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "c.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	apply := func(run string, sample int, raws ...string) PageCounts {
		t.Helper()
		var items []pyjson.Value
		for _, raw := range raws {
			v, err := pyjson.Parse([]byte(raw))
			if err != nil {
				t.Fatal(err)
			}
			items = append(items, v)
		}
		var c PageCounts
		if err := db.Tx(func(tx *sql.Tx) (err error) {
			c, err = ApplyPage(tx, Page{Endpoint: "e", Items: items, RunID: run, Now: "2026-01-02T00:00:00.000Z", ChangeSeq: 1, SilentSample: sample})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return c
	}
	m := `"_modified_at":"2026-01-01T00:00:00.000Z"`
	apply("r1", 10, `{"id":"A",`+m+`,"x":1}`, `{"id":"B","x":1}`, `{"id":"C",`+m+`,"x":1}`, `{"id":"D",`+m+`,"x":1}`)

	// Without _modified_at a change is silent too; a sample of 1 compares only the first.
	c := apply("r2", 1, `{"id":"A",`+m+`,"x":2}`, `{"id":"B","x":2}`)
	if c.Silent != 2 || c.SilentSampled != 1 || c.SilentFields["x"] != 1 {
		t.Fatalf("sampled: %+v", c)
	}
	// A restored record that also changed silently counts as both.
	if _, err := db.Exec(`UPDATE bronze_records SET deleted = 1 WHERE id = 'C'`); err != nil {
		t.Fatal(err)
	}
	if c := apply("r3", 10, `{"id":"C",`+m+`,"x":3}`); c.Silent != 1 || c.Restored != 1 || c.SilentFields["x"] != 1 {
		t.Fatalf("restored: %+v", c)
	}
	// A stored payload that doesn't parse is counted, but says nothing about fields.
	if _, err := db.Exec(`UPDATE bronze_records SET payload = 'not json' WHERE id = 'D'`); err != nil {
		t.Fatal(err)
	}
	if c := apply("r4", 10, `{"id":"D",`+m+`,"x":4}`); c.Silent != 1 || c.SilentSampled != 0 || len(c.SilentFields) != 0 {
		t.Fatalf("unparseable: %+v", c)
	}
}
