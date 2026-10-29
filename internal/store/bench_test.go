package store

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/eemax/centric-fetch/internal/pyjson"
)

// A page of 500 style-sized records (~3 KB, 60 fields), as a full fetch sees them.
func benchPage(b *testing.B, start int, modified, name string) []pyjson.Value {
	b.Helper()
	items := make([]pyjson.Value, 500)
	for i := range items {
		raw := fmt.Sprintf(`{"id":"C%07d","_modified_at":"%s","node_name":"%s"`, start+i, modified, name)
		for f := range 60 {
			raw += fmt.Sprintf(`,"field_%02d":"value %d of a record that is about as long as a real one"`, f, f)
		}
		v, err := pyjson.Parse([]byte(raw + "}"))
		if err != nil {
			b.Fatal(err)
		}
		items[i] = v
	}
	return items
}

func benchApply(b *testing.B, second func(i int) []pyjson.Value, first []pyjson.Value) {
	db, err := Open(filepath.Join(b.TempDir(), "b.db"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	apply := func(items []pyjson.Value, run string) {
		if err := db.Tx(func(tx *sql.Tx) error {
			_, err := ApplyPage(tx, Page{Endpoint: "styles", Items: items, RunID: run, Now: "2026-01-02T00:00:00.000Z", ChangeSeq: 1})
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
	apply(first, "r0")
	b.ResetTimer()
	for i := range b.N {
		apply(second(i), fmt.Sprintf("r%d", i+1))
	}
	b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N*500), "µs/record")
}

// Re-reading unchanged records: the bulk of a full fetch.
func BenchmarkApplyUnchanged(b *testing.B) {
	page := benchPage(b, 0, "2026-01-01T00:00:00.000Z", "a")
	benchApply(b, func(int) []pyjson.Value { return page }, page)
}

// Records changed silently: same _modified_at, new content.
func BenchmarkApplySilent(b *testing.B) {
	pages := [2][]pyjson.Value{benchPage(b, 0, "2026-01-01T00:00:00.000Z", "a"), benchPage(b, 0, "2026-01-01T00:00:00.000Z", "b")}
	benchApply(b, func(i int) []pyjson.Value { return pages[(i+1)%2] }, pages[0])
}

// New records.
func BenchmarkApplyInsert(b *testing.B) {
	benchApply(b, func(i int) []pyjson.Value { return benchPage(b, (i+1)*500, "2026-01-01T00:00:00.000Z", "a") }, benchPage(b, 0, "2026-01-01T00:00:00.000Z", "a"))
}
