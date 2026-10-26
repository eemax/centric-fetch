// Package status reports what the database holds and whether the fetch is healthy. Both
// only read the small bookkeeping tables and an index, never the payloads, so they're cheap
// enough for a monitor to call every few minutes.
package status

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/eemax/centric-fetch/internal/config"
	"github.com/eemax/centric-fetch/internal/store"
)

// Status is `centric-fetch status`.
type Status struct {
	Database  string     `json:"database"`
	Exists    bool       `json:"exists"`
	SizeBytes int64      `json:"size_bytes"`
	Running   bool       `json:"running"`
	LastRun   *Run       `json:"last_run"`
	Endpoints []Endpoint `json:"endpoints"`
}

// Endpoint is one endpoint's stored state.
type Endpoint struct {
	Endpoint          string  `json:"endpoint"`
	ActiveRecords     int     `json:"active_records"`
	DeletedRecords    int     `json:"deleted_records"`
	LastSuccessAt     *string `json:"last_success_at"`
	IncrementalCursor *string `json:"incremental_cursor"`
	LastSuccessMode   *string `json:"last_success_mode"`
	LastFullSuccessAt *string `json:"last_full_success_at"`
	LastAttemptStatus *string `json:"last_attempt_status"`
	LastAttemptError  *string `json:"last_attempt_error"`
	PendingResume     bool    `json:"pending_resume"`
}

// Run is a recorded run with what it did per endpoint and in total.
type Run struct {
	RunID           string         `json:"run_id"`
	Command         string         `json:"command"`
	StartedAt       string         `json:"started_at"`
	FinishedAt      *string        `json:"finished_at"`
	Status          string         `json:"status"`
	Error           *string        `json:"error"`
	DurationSeconds *float64       `json:"duration_seconds"`
	Mode            *string        `json:"mode"`
	Totals          map[string]int `json:"totals"`
	Endpoints       []RunEndpoint  `json:"endpoints"`
}

// RunEndpoint is one endpoint's part of a run.
type RunEndpoint struct {
	Endpoint  string   `json:"endpoint"`
	Mode      string   `json:"mode"`
	Status    string   `json:"status"`
	Fetched   int      `json:"fetched"`
	Inserted  int      `json:"inserted"`
	Updated   int      `json:"updated"`
	Unchanged int      `json:"unchanged"`
	Restored  int      `json:"restored"`
	Deleted   int      `json:"deleted"`
	Missing   int      `json:"missing"`
	Expected  *int     `json:"expected"`
	Complete  bool     `json:"complete"`
	Warning   *string  `json:"warning"`
	Error     *string  `json:"error"`
	Seconds   *float64 `json:"seconds"`
}

var runCounts = []string{"fetched", "inserted", "updated", "unchanged", "restored", "deleted", "missing"}

// Collect reads the status.
func Collect(cfg *config.Config) (*Status, error) {
	st := &Status{Database: cfg.Database, Running: store.Locked(cfg.Database)}
	fi, err := os.Stat(cfg.Database)
	if err != nil {
		for _, ep := range cfg.Endpoints {
			st.Endpoints = append(st.Endpoints, Endpoint{Endpoint: ep.Name})
		}
		return st, nil
	}
	st.Exists, st.SizeBytes = true, fi.Size()
	db, err := store.OpenReadOnly(cfg.Database)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	type counts struct{ active, deleted int }
	byEndpoint := map[string]*counts{}
	rows, err := db.Query(`SELECT endpoint, deleted, COUNT(*) FROM bronze_records GROUP BY endpoint, deleted`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		var deleted bool
		var n int
		if err := rows.Scan(&name, &deleted, &n); err != nil {
			rows.Close()
			return nil, err
		}
		c := byEndpoint[name]
		if c == nil {
			c = &counts{}
			byEndpoint[name] = c
		}
		if deleted {
			c.deleted += n
		} else {
			c.active += n
		}
	}
	rows.Close()

	for _, ep := range cfg.Endpoints {
		e := Endpoint{Endpoint: ep.Name}
		if c := byEndpoint[ep.Name]; c != nil {
			e.ActiveRecords, e.DeletedRecords = c.active, c.deleted
		}
		var pending sql.NullString
		err := db.QueryRow(`SELECT last_success_finished_at, last_success_started_at, last_success_mode,
			last_full_success_at, last_attempt_status, last_attempt_error, pending_run_id
			FROM meta_endpoint_state WHERE endpoint = ?`, ep.Name).Scan(
			&e.LastSuccessAt, &e.IncrementalCursor, &e.LastSuccessMode, &e.LastFullSuccessAt,
			&e.LastAttemptStatus, &e.LastAttemptError, &pending)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		e.PendingResume = pending.Valid
		st.Endpoints = append(st.Endpoints, e)
	}
	if st.LastRun, err = lastRun(db, ""); err != nil {
		return nil, err
	}
	return st, nil
}

func lastRun(db *store.DB, filter string) (*Run, error) {
	r := &Run{}
	err := db.QueryRow(`SELECT run_id, command, started_at, finished_at, status, error FROM meta_runs `+filter+
		` ORDER BY started_at DESC LIMIT 1`).Scan(&r.RunID, &r.Command, &r.StartedAt, &r.FinishedAt, &r.Status, &r.Error)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r.DurationSeconds = secondsBetween(&r.StartedAt, r.FinishedAt)
	rows, err := db.Query(`SELECT endpoint, mode, status, fetched, inserted, updated, unchanged, restored,
		deleted, missing, expected, complete, warning, error, started_at, finished_at
		FROM meta_run_endpoints WHERE run_id = ? ORDER BY started_at, endpoint`, r.RunID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	r.Totals = map[string]int{}
	for _, k := range runCounts {
		r.Totals[k] = 0
	}
	modes := map[string]bool{}
	r.Endpoints = []RunEndpoint{}
	for rows.Next() {
		var e RunEndpoint
		var started string
		var finished *string
		if err := rows.Scan(&e.Endpoint, &e.Mode, &e.Status, &e.Fetched, &e.Inserted, &e.Updated, &e.Unchanged,
			&e.Restored, &e.Deleted, &e.Missing, &e.Expected, &e.Complete, &e.Warning, &e.Error, &started, &finished); err != nil {
			return nil, err
		}
		e.Seconds = secondsBetween(&started, finished)
		for k, v := range map[string]int{"fetched": e.Fetched, "inserted": e.Inserted, "updated": e.Updated,
			"unchanged": e.Unchanged, "restored": e.Restored, "deleted": e.Deleted, "missing": e.Missing} {
			r.Totals[k] += v
		}
		modes[e.Mode] = true
		r.Endpoints = append(r.Endpoints, e)
	}
	switch len(modes) {
	case 0:
	case 1:
		for m := range modes {
			r.Mode = &m
		}
	default:
		mixed := "mixed"
		r.Mode = &mixed
	}
	return r, rows.Err()
}

// Health is `centric-fetch check`.
type Health struct {
	Healthy       bool           `json:"healthy"`
	CheckedAt     string         `json:"checked_at"`
	MaxAgeSeconds *int64         `json:"max_age_seconds"`
	Problems      []string       `json:"problems"`
	LastFetch     *Run           `json:"last_fetch"`
	Running       bool           `json:"running"`
	Endpoints     []HealthDetail `json:"endpoints"`
}

// HealthDetail is one endpoint's health.
type HealthDetail struct {
	Endpoint          string   `json:"endpoint"`
	Healthy           bool     `json:"healthy"`
	LastSuccessAt     *string  `json:"last_success_at"`
	AgeSeconds        *int64   `json:"age_seconds"`
	LastAttemptStatus *string  `json:"last_attempt_status"`
	Problems          []string `json:"problems"`
}

// Check says whether the data is fresh and the last fetch good: the database exists, the
// latest fetch didn't fail (one in progress is fine), and every endpoint has a success, its
// last attempt didn't fail and, with maxAge, that success is recent enough.
func Check(cfg *config.Config, maxAge time.Duration, now time.Time) (*Health, error) {
	h := &Health{CheckedAt: store.Iso(now), Problems: []string{}, Endpoints: []HealthDetail{}, Running: store.Locked(cfg.Database)}
	if maxAge > 0 {
		s := int64(maxAge.Seconds())
		h.MaxAgeSeconds = &s
	}
	if _, err := os.Stat(cfg.Database); err != nil {
		h.Problems = append(h.Problems, "database not created yet: "+cfg.Database)
		return h, nil
	}
	db, err := store.OpenReadOnly(cfg.Database)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if h.LastFetch, err = lastRun(db, "WHERE command LIKE 'fetch%'"); err != nil {
		return nil, err
	}
	switch lf := h.LastFetch; {
	case lf == nil:
		h.Problems = append(h.Problems, "no fetch has run yet")
	case lf.Status == "running":
		if !h.Running {
			h.Problems = append(h.Problems, fmt.Sprintf("fetch %s never finished (started %s)", lf.RunID, lf.StartedAt))
		}
	case lf.Status != "ok" && lf.Status != "warn":
		p := fmt.Sprintf("last fetch %s (started %s)", lf.Status, lf.StartedAt)
		if lf.Error != nil {
			p += ": " + *lf.Error
		}
		h.Problems = append(h.Problems, p)
	}
	for _, ep := range cfg.Endpoints {
		d := HealthDetail{Endpoint: ep.Name, Problems: []string{}}
		var lastError *string
		err := db.QueryRow(`SELECT last_success_finished_at, last_attempt_status, last_attempt_error
			FROM meta_endpoint_state WHERE endpoint = ?`, ep.Name).Scan(&d.LastSuccessAt, &d.LastAttemptStatus, &lastError)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		if d.LastSuccessAt == nil {
			d.Problems = append(d.Problems, "never fetched successfully")
		} else if t, ok := store.ParseTime(*d.LastSuccessAt); ok {
			age := int64(now.Sub(t).Seconds())
			d.AgeSeconds = &age
			if maxAge > 0 && now.Sub(t) > maxAge {
				d.Problems = append(d.Problems, fmt.Sprintf("last success %s is older than %s", *d.LastSuccessAt, human(maxAge)))
			}
		}
		if d.LastAttemptStatus != nil && *d.LastAttemptStatus == "failed" {
			p := "last attempt failed"
			if lastError != nil {
				p += ": " + *lastError
			}
			d.Problems = append(d.Problems, p)
		}
		d.Healthy = len(d.Problems) == 0
		for _, p := range d.Problems {
			h.Problems = append(h.Problems, ep.Name+": "+p)
		}
		h.Endpoints = append(h.Endpoints, d)
	}
	h.Healthy = len(h.Problems) == 0
	return h, nil
}

// ParseDuration reads "90" (seconds), "45m", "26h" or "2d".
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "d") {
		d, err := time.ParseDuration(strings.TrimSuffix(s, "d") + "h")
		return d * 24, err
	}
	if s != "" && strings.Trim(s, "0123456789.") == "" {
		s += "s"
	}
	d, err := time.ParseDuration(s)
	if err == nil && d <= 0 {
		err = fmt.Errorf("duration must be positive")
	}
	return d, err
}

func human(d time.Duration) string {
	s := int64(d.Seconds())
	for _, u := range []struct {
		unit string
		size int64
	}{{"d", 86400}, {"h", 3600}, {"m", 60}} {
		if s >= u.size && s%u.size == 0 {
			return fmt.Sprintf("%d%s", s/u.size, u.unit)
		}
	}
	return fmt.Sprintf("%ds", s)
}

func secondsBetween(start, end *string) *float64 {
	if start == nil || end == nil {
		return nil
	}
	a, ok1 := store.ParseTime(*start)
	b, ok2 := store.ParseTime(*end)
	if !ok1 || !ok2 {
		return nil
	}
	s := float64(int64(b.Sub(a).Seconds()*10+0.5)) / 10
	return &s
}
