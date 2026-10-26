// Package store is the SQLite database: raw Centric records (bronze_records) and fetch
// bookkeeping (meta_*).
//
// The layout is schema version 1. Readers depend on it: add tables or indexes, never change
// existing columns.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

const schemaVersion = "1"

const schema = `
CREATE TABLE IF NOT EXISTS meta_info (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS meta_runs (
    run_id TEXT PRIMARY KEY,
    command TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    error TEXT
);
CREATE TABLE IF NOT EXISTS meta_run_endpoints (
    run_id TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    mode TEXT NOT NULL,
    delta_floor TEXT,
    resumed INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    expected INTEGER,
    fetched INTEGER NOT NULL DEFAULT 0,
    inserted INTEGER NOT NULL DEFAULT 0,
    updated INTEGER NOT NULL DEFAULT 0,
    unchanged INTEGER NOT NULL DEFAULT 0,
    restored INTEGER NOT NULL DEFAULT 0,
    deleted INTEGER NOT NULL DEFAULT 0,
    missing INTEGER NOT NULL DEFAULT 0,
    passes INTEGER NOT NULL DEFAULT 1,
    complete INTEGER NOT NULL DEFAULT 0,
    note TEXT,
    warning TEXT,
    error TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    PRIMARY KEY (run_id, endpoint)
);
CREATE TABLE IF NOT EXISTS meta_endpoint_state (
    endpoint TEXT PRIMARY KEY,
    last_success_started_at TEXT,
    last_success_finished_at TEXT,
    last_success_mode TEXT,
    last_full_success_at TEXT,
    last_attempt_started_at TEXT,
    last_attempt_status TEXT,
    last_attempt_error TEXT,
    pending_run_id TEXT,
    pending_mode TEXT,
    pending_pass INTEGER,
    pending_started_at TEXT,
    pending_delta_floor TEXT,
    pending_next_skip INTEGER,
    pending_fetched INTEGER
);
CREATE TABLE IF NOT EXISTS bronze_records (
    endpoint TEXT NOT NULL,
    id TEXT NOT NULL,
    modified_at TEXT,
    payload TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    deleted INTEGER NOT NULL DEFAULT 0,
    deleted_at TEXT,
    first_seen_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_seen_run TEXT NOT NULL,
    change_seq INTEGER NOT NULL,
    PRIMARY KEY (endpoint, id)
);
CREATE INDEX IF NOT EXISTS bronze_records_seen ON bronze_records (endpoint, last_seen_run);
CREATE INDEX IF NOT EXISTS bronze_records_changes ON bronze_records (endpoint, change_seq);
`

// Counting active/deleted records per endpoint from this index never touches the payloads
// (in the table they'd be read just to get past them to the deleted column).
const liveIndex = `CREATE INDEX IF NOT EXISTS bronze_records_live ON bronze_records (endpoint, deleted)`

// DB is an open database. Writers hold it alone (one connection, see Lock).
type DB struct {
	*sql.DB
	Path string
}

// Open opens the database for writing, creating it and its schema on first use.
func Open(path string) (*DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	db, err := open(path, "_txlock=immediate")
	if err != nil {
		return nil, err
	}
	exists, err := db.checkSchema()
	if err != nil {
		db.Close()
		return nil, err
	}
	ctx := context.Background()
	if !exists {
		if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
			db.Close()
			return nil, err
		}
	}
	err = db.Tx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(schema + liveIndex); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO meta_info (key, value) VALUES ('schema_version', ?), ('change_seq', '0')
			ON CONFLICT (key) DO NOTHING`, schemaVersion)
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	return db, nil
}

// OpenReadOnly opens an existing database for reading. It never blocks a running fetch.
func OpenReadOnly(path string) (*DB, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("database not found: %s", path)
	}
	db, err := open(path, "mode=ro")
	if err != nil {
		return nil, err
	}
	exists, err := db.checkSchema()
	if err == nil && !exists {
		err = fmt.Errorf("%s is not a centric-fetch database", path)
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func open(path, extra string) (*DB, error) {
	dsn := "file:" + (&url.URL{Path: path}).EscapedPath() +
		"?_pragma=busy_timeout(30000)&_pragma=synchronous(NORMAL)&" + extra
	sqldb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	sqldb.SetMaxOpenConns(1)
	return &DB{DB: sqldb, Path: path}, nil
}

// checkSchema reports whether the schema exists; it refuses other applications' databases
// and other schema versions.
func (db *DB) checkSchema() (bool, error) {
	var tables int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'`).Scan(&tables); err != nil {
		return false, fmt.Errorf("open %s: %w", db.Path, err)
	}
	var version string
	err := db.QueryRow(`SELECT value FROM meta_info WHERE key = 'schema_version'`).Scan(&version)
	switch {
	case err != nil && tables == 0:
		return false, nil
	case err != nil && strings.Contains(err.Error(), "no such table"):
		return false, fmt.Errorf("%s holds other tables and is not a centric-fetch database", db.Path)
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	case version != schemaVersion:
		return false, fmt.Errorf("%s has schema version %s; centric-fetch supports %s", db.Path, version, schemaVersion)
	}
	return true, nil
}

// Tx runs fn in an IMMEDIATE transaction. It deliberately ignores cancellation: a page that
// has been received is either stored whole or not at all, never cut off by a signal.
func (db *DB) Tx(fn func(*sql.Tx) error) error {
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// NextChangeSeq allocates the next change sequence number.
func NextChangeSeq(tx *sql.Tx) (int64, error) {
	var v string
	if err := tx.QueryRow(`SELECT value FROM meta_info WHERE key = 'change_seq'`).Scan(&v); err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, err
	}
	n++
	_, err = tx.Exec(`UPDATE meta_info SET value = ? WHERE key = 'change_seq'`, strconv.FormatInt(n, 10))
	return n, err
}

// StoredEndpoints lists endpoints that have records, hopping along the primary key.
func (db *DB) StoredEndpoints() ([]string, error) {
	rows, err := db.Query(`
		WITH RECURSIVE names(name) AS (
			SELECT MIN(endpoint) FROM bronze_records
			UNION ALL
			SELECT (SELECT MIN(endpoint) FROM bronze_records WHERE endpoint > names.name)
			FROM names WHERE names.name IS NOT NULL
		)
		SELECT name FROM names WHERE name IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// LatestChangeSeq is the highest change_seq on any record (0 if none). It moves only when a
// record really changed.
func (db *DB) LatestChangeSeq() (int64, error) {
	names, err := db.StoredEndpoints()
	if err != nil {
		return 0, err
	}
	var best int64
	for _, n := range names {
		var v sql.NullInt64
		if err := db.QueryRow(`SELECT MAX(change_seq) FROM bronze_records WHERE endpoint = ?`, n).Scan(&v); err != nil {
			return 0, err
		}
		best = max(best, v.Int64)
	}
	return best, nil
}

// Lock is the single-writer lock: an flock on <database>.lock. The kernel drops it when the
// process exits, however it exits, so there is nothing to go stale.
type Lock struct{ f *os.File }

// ErrLocked means another fetch holds the lock.
var ErrLocked = errors.New("another centric-fetch run holds the lock")

// AcquireLock takes the writer lock without waiting.
func AcquireLock(dbPath string) (*Lock, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(dbPath+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrLocked
		}
		return nil, err
	}
	f.Truncate(0)
	fmt.Fprintf(f, "%d\n", os.Getpid())
	return &Lock{f}, nil
}

// Release drops the lock.
func (l *Lock) Release() {
	unix.Flock(int(l.f.Fd()), unix.LOCK_UN)
	l.f.Close()
}

// Locked reports whether a fetch holds the lock right now.
func Locked(dbPath string) bool {
	f, err := os.Open(dbPath + ".lock")
	if err != nil {
		return false
	}
	defer f.Close()
	if err := unix.Flock(int(f.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
		return errors.Is(err, unix.EWOULDBLOCK)
	}
	unix.Flock(int(f.Fd()), unix.LOCK_UN)
	return false
}

// Iso formats t as stored: UTC, milliseconds, Z.
func Iso(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

// ParseTime reads the ISO 8601 forms Centric and this database use. A value without a zone
// is UTC.
func ParseTime(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04:05.999999999Z0700",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}
