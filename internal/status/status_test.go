package status

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/eemax/centric-fetch/internal/config"
	"github.com/eemax/centric-fetch/internal/pyjson"
	"github.com/eemax/centric-fetch/internal/store"
)

var now = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func setup(t *testing.T) (*config.Config, *store.DB) {
	t.Helper()
	cfg := &config.Config{
		Database:  filepath.Join(t.TempDir(), "c.db"),
		Endpoints: []config.Endpoint{{Name: "styles"}, {Name: "seasons"}},
	}
	db, err := store.Open(cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return cfg, db
}

func exec(t *testing.T, db *store.DB, fn func(tx *sql.Tx) error) {
	t.Helper()
	if err := db.Tx(fn); err != nil {
		t.Fatal(err)
	}
}

// A finished run: styles ok an hour ago (one of three records deleted), seasons failed.
func record(t *testing.T, db *store.DB, runStatus string) {
	exec(t, db, func(tx *sql.Tx) error {
		var items []pyjson.Value
		for _, raw := range []string{`{"id":"A"}`, `{"id":"B"}`, `{"id":"C"}`} {
			v, _ := pyjson.Parse([]byte(raw))
			items = append(items, v)
		}
		if _, err := store.ApplyPage(tx, store.Page{Endpoint: "styles", Items: items, RunID: "r1", Now: store.Iso(now), ChangeSeq: 1}); err != nil {
			return err
		}
		if _, err := store.MarkDeleted(tx, "styles", []string{"C"}, store.Iso(now), 2); err != nil {
			return err
		}
		hourAgo := store.Iso(now.Add(-time.Hour))
		for _, q := range []struct {
			sql  string
			args []any
		}{
			{`INSERT INTO meta_runs VALUES ('r1', 'fetch', ?, ?, ?, NULL)`, []any{store.Iso(now.Add(-2 * time.Hour)), hourAgo, runStatus}},
			{`INSERT INTO meta_run_endpoints (run_id, endpoint, mode, status, fetched, inserted, started_at, finished_at)
				VALUES ('r1', 'styles', 'delta', 'ok', 3, 3, ?, ?)`, []any{store.Iso(now.Add(-2 * time.Hour)), hourAgo}},
			{`INSERT INTO meta_endpoint_state (endpoint, last_success_finished_at, last_success_started_at, last_attempt_status)
				VALUES ('styles', ?, ?, 'ok')`, []any{hourAgo, hourAgo}},
			{`INSERT INTO meta_endpoint_state (endpoint, last_attempt_status, last_attempt_error)
				VALUES ('seasons', 'failed', 'timed out')`, nil},
			{`INSERT INTO meta_windows (endpoint, mode, run_id, pass, started_at, next_skip, fetched)
				VALUES ('seasons', 'delta', 'r1', 1, ?, 4, 4)`, []any{hourAgo}},
		} {
			if _, err := tx.Exec(q.sql, q.args...); err != nil {
				return err
			}
		}
		return nil
	})
}

func TestCollect(t *testing.T) {
	cfg, db := setup(t)
	record(t, db, "partial")
	st, err := Collect(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Exists || st.Running || st.LastRun == nil || st.LastRun.Status != "partial" {
		t.Fatalf("%+v", st)
	}
	if st.LastRun.Totals["inserted"] != 3 || *st.LastRun.Mode != "delta" || *st.LastRun.DurationSeconds != 3600 {
		t.Fatalf("last run %+v", st.LastRun)
	}
	styles, seasons := st.Endpoints[0], st.Endpoints[1]
	if styles.ActiveRecords != 2 || styles.DeletedRecords != 1 || seasons.ActiveRecords != 0 || !seasons.PendingResume {
		t.Fatalf("endpoints %+v %+v", styles, seasons)
	}
}

func TestCheck(t *testing.T) {
	cfg, db := setup(t)
	h, err := Check(cfg, Ages{}, now)
	if err != nil {
		t.Fatal(err)
	}
	if h.Healthy || h.Problems[0] != "no fetch has run yet" {
		t.Fatalf("empty: %+v", h.Problems)
	}

	record(t, db, "partial")
	h, _ = Check(cfg, Ages{Any: 26 * time.Hour}, now)
	want := []string{
		"last fetch partial (started 2026-03-01T10:00:00.000Z)",
		"seasons: never fetched successfully",
		"seasons: last attempt failed: timed out",
	}
	if h.Healthy || len(h.Problems) != len(want) {
		t.Fatalf("problems %q", h.Problems)
	}
	for i := range want {
		if h.Problems[i] != want[i] {
			t.Fatalf("problem %d: %q, want %q", i, h.Problems[i], want[i])
		}
	}
	if !h.Endpoints[0].Healthy || *h.Endpoints[0].AgeSeconds != 3600 {
		t.Fatalf("styles %+v", h.Endpoints[0])
	}

	cfg.Endpoints = cfg.Endpoints[:1]
	exec(t, db, func(tx *sql.Tx) error { _, err := tx.Exec(`UPDATE meta_runs SET status = 'ok'`); return err })
	if h, _ := Check(cfg, Ages{Any: 26 * time.Hour}, now); !h.Healthy {
		t.Fatalf("styles only: %q", h.Problems)
	}
	if h, _ := Check(cfg, Ages{Any: 30 * time.Minute}, now); h.Healthy || h.Problems[0] != "styles: last success 2026-03-01T11:00:00.000Z is older than 30m" {
		t.Fatalf("stale: %q", h.Problems)
	}
}

func TestCheckSeesUnfinishedRun(t *testing.T) {
	cfg, db := setup(t)
	record(t, db, "running")
	h, _ := Check(cfg, Ages{}, now)
	if h.Problems[0] != "fetch r1 never finished (started 2026-03-01T10:00:00.000Z)" {
		t.Fatalf("%q", h.Problems)
	}
	lock, err := store.AcquireLock(cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	if h, _ := Check(cfg, Ages{}, now); !h.Running || h.Problems[0] == "fetch r1 never finished (started 2026-03-01T10:00:00.000Z)" {
		t.Fatalf("running: %v %q", h.Running, h.Problems)
	}
}

func TestParseDuration(t *testing.T) {
	for in, want := range map[string]time.Duration{"90": 90 * time.Second, "45m": 45 * time.Minute, "26h": 26 * time.Hour, "2d": 48 * time.Hour} {
		if got, err := ParseDuration(in); err != nil || got != want {
			t.Errorf("%s: %v %v", in, got, err)
		}
	}
	for _, in := range []string{"", "x", "-5m", "0", "0d", "-1d"} {
		if _, err := ParseDuration(in); err == nil {
			t.Errorf("%q: no error", in)
		}
	}
}

// A database as the first release left it: no meta_windows, none of the later columns, and an
// unfinished window in meta_endpoint_state's pending_* columns.
func firstReleaseDatabase(t *testing.T, db *store.DB) {
	t.Helper()
	record(t, db, "ok")
	for _, q := range []string{
		`DROP TABLE meta_windows`,
		`ALTER TABLE meta_endpoint_state DROP COLUMN last_incremental_success_at`,
		`ALTER TABLE meta_run_endpoints DROP COLUMN silent`,
		`ALTER TABLE meta_run_endpoints DROP COLUMN silent_fields`,
		`UPDATE meta_endpoint_state SET pending_run_id = 'r1', pending_mode = 'delta', pending_pass = 1,
			pending_started_at = '2026-03-01T10:00:00.000Z', pending_next_skip = 4, pending_fetched = 4
			WHERE endpoint = 'seasons'`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

func TestReadsAndMigratesAFirstReleaseDatabase(t *testing.T) {
	cfg, db := setup(t)
	firstReleaseDatabase(t, db)
	db.Close()

	st, err := Collect(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Endpoints[1].PendingResume || st.Endpoints[0].LastIncrementalSuccessAt != nil || st.LastRun.Totals["silent"] != 0 {
		t.Fatalf("before migrating: %+v %+v", st.Endpoints, st.LastRun.Totals)
	}
	if _, err := Check(cfg, Ages{Full: time.Hour}, now); err != nil {
		t.Fatal(err)
	}

	db, err = store.Open(cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var mode string
	var skip int
	var oldPending sql.NullString
	if err := db.QueryRow(`SELECT mode, next_skip FROM meta_windows WHERE endpoint = 'seasons'`).Scan(&mode, &skip); err != nil || mode != "delta" || skip != 4 {
		t.Fatalf("window moved: %s %d %v", mode, skip, err)
	}
	db.QueryRow(`SELECT pending_run_id FROM meta_endpoint_state WHERE endpoint = 'seasons'`).Scan(&oldPending)
	if oldPending.Valid {
		t.Fatal("old pending columns still set")
	}
	if st, err := Collect(cfg); err != nil || !st.Endpoints[1].PendingResume {
		t.Fatalf("after migrating: %v %+v", err, st)
	}
	// An older version ran again (a rollback) and left a newer position in the old columns:
	// that one wins.
	if _, err := db.Exec(`UPDATE meta_endpoint_state SET pending_run_id = 'r2', pending_mode = 'delta',
		pending_pass = 1, pending_started_at = '2026-03-01T11:00:00.000Z', pending_next_skip = 8,
		pending_fetched = 8 WHERE endpoint = 'seasons'`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if db, err = store.Open(cfg.Database); err != nil {
		t.Fatal(err)
	}
	var run string
	if err := db.QueryRow(`SELECT run_id, next_skip FROM meta_windows WHERE endpoint = 'seasons' AND mode = 'delta'`).Scan(&run, &skip); err != nil || run != "r2" || skip != 8 {
		t.Fatalf("after a rollback: %s %d %v", run, skip, err)
	}
}

func TestCheckFullAge(t *testing.T) {
	cfg, db := setup(t)
	record(t, db, "ok")
	cfg.Endpoints = cfg.Endpoints[:1]
	if h, _ := Check(cfg, Ages{Full: 2 * 24 * time.Hour}, now); h.Healthy || h.Problems[0] != "styles: never fetched in full" {
		t.Fatalf("never full: %q", h.Problems)
	}
	exec(t, db, func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE meta_endpoint_state SET last_full_success_at = ? WHERE endpoint = 'styles'`, store.Iso(now.Add(-3*24*time.Hour)))
		return err
	})
	h, _ := Check(cfg, Ages{Full: 2 * 24 * time.Hour}, now)
	if h.Healthy || h.Problems[0] != "styles: last full success 2026-02-26T12:00:00.000Z is older than 2d" || *h.Endpoints[0].FullAgeSeconds != 3*86400 {
		t.Fatalf("stale full: %q", h.Problems)
	}
	if h, _ := Check(cfg, Ages{Any: 26 * time.Hour, Full: 4 * 24 * time.Hour}, now); !h.Healthy {
		t.Fatalf("fresh enough: %q", h.Problems)
	}
}
