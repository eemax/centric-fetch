package fetch

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/eemax/centric-fetch/internal/store"
)

func (e *env) state(endpoint string) (cursor, pendingRun sql.NullString, skip sql.NullInt64, attempt sql.NullString) {
	e.t.Helper()
	err := e.db.QueryRow(`SELECT last_success_started_at, pending_run_id, pending_next_skip, last_attempt_status
		FROM meta_endpoint_state WHERE endpoint = ?`, endpoint).Scan(&cursor, &pendingRun, &skip, &attempt)
	if err != nil {
		e.t.Fatal(err)
	}
	return
}

func (e *env) active(endpoint string) int {
	var n int
	e.db.QueryRow(`SELECT COUNT(*) FROM bronze_records WHERE endpoint = ? AND deleted = 0`, endpoint).Scan(&n)
	return n
}

func TestFirstFetchThenIncremental(t *testing.T) {
	e := newEnv(t)
	e.fake.add("styles", styles(5, t0)...)
	e.fake.add("seasons", record{ID: "SE1", Modified: t0, Name: "SS26"})

	sum := e.run(Options{})
	if sum.Status != "ok" || sum.Mode != "incremental" {
		t.Fatalf("status %s mode %s: %+v", sum.Status, sum.Mode, sum.Endpoints)
	}
	st := sum.endpoint("styles")
	if st.Mode != "full" || st.Inserted != 5 || st.Fetched != 5 || !st.Complete {
		t.Fatalf("first fetch of styles: %+v", st)
	}
	if sum.RecordsChanged != 6 || sum.ChangeSeqAfter <= sum.ChangeSeqBefore {
		t.Fatalf("records changed %d, seq %d -> %d", sum.RecordsChanged, sum.ChangeSeqBefore, sum.ChangeSeqAfter)
	}
	if cursor, pending, _, attempt := e.state("styles"); cursor.String != store.Iso(e.now) || pending.Valid || attempt.String != "ok" {
		t.Fatalf("state after first fetch: cursor %v pending %v attempt %v", cursor, pending, attempt)
	}

	// An hour later: one style changed, one new.
	e.now = e.now.Add(time.Hour)
	e.fake.add("styles", record{ID: "S001", Modified: e.now.Add(-30 * time.Minute), Name: "renamed"},
		record{ID: "S100", Modified: e.now.Add(-30 * time.Minute), Name: "new"})
	sum = e.run(Options{})
	st = sum.endpoint("styles")
	if st.Mode != "delta" || st.Fetched != 2 || st.Inserted != 1 || st.Updated != 1 || sum.Status != "ok" {
		t.Fatalf("incremental: status %s %+v", sum.Status, st)
	}
	if want := store.Iso(t0.Add(24*time.Hour - 10*time.Minute)); st.DeltaFloor != want {
		t.Fatalf("delta floor %s, want %s", st.DeltaFloor, want)
	}
	var payload string
	e.db.QueryRow(`SELECT payload FROM bronze_records WHERE endpoint = 'styles' AND id = 'S001'`).Scan(&payload)
	if !strings.Contains(payload, `"node_name":"renamed"`) {
		t.Fatalf("payload not updated: %s", payload)
	}
	if e.active("styles") != 6 {
		t.Fatalf("active styles %d", e.active("styles"))
	}
}

func TestOlderPayloadNeverRollsBack(t *testing.T) {
	e := newEnv(t)
	e.fake.add("styles", record{ID: "S1", Modified: t0.Add(time.Hour), Name: "new"})
	e.run(Options{})
	e.fake.add("styles", record{ID: "S1", Modified: t0, Name: "old"})
	sum := e.run(Options{Full: true})
	if o := sum.endpoint("styles"); o.Updated != 0 || o.Unchanged != 1 {
		t.Fatalf("older payload: %+v", o)
	}
}

func TestFullFetchFlagsConfirmedDeletions(t *testing.T) {
	e := newEnv(t)
	e.fake.add("styles", styles(12, t0)...)
	e.run(Options{})

	e.fake.remove("styles", "S003")
	hidden := styles(12, t0)[5]
	hidden.hidden = true // not listed, but still in Centric
	e.fake.add("styles", hidden)
	e.now = e.now.Add(time.Hour)
	sum := e.run(Options{Full: true})
	o := sum.endpoint("styles")
	if o.Deleted != 1 || o.Missing != 0 || o.Status != "warn" || e.fake.lookups == 0 {
		t.Fatalf("full fetch: %+v (lookups %d)", o, e.fake.lookups)
	}
	if !strings.Contains(strings.Join(o.Warnings, ";"), "still exist in Centric") {
		t.Fatalf("warnings: %v", o.Warnings)
	}
	var deleted int
	e.db.QueryRow(`SELECT deleted FROM bronze_records WHERE endpoint = 'styles' AND id = 'S003'`).Scan(&deleted)
	if deleted != 1 || e.active("styles") != 11 {
		t.Fatalf("S003 deleted=%d, active %d", deleted, e.active("styles"))
	}

	// It comes back: restored.
	e.fake.add("styles", styles(12, t0)[3])
	e.now = e.now.Add(time.Hour)
	if o := e.run(Options{Full: true}).endpoint("styles"); o.Restored != 1 {
		t.Fatalf("restore: %+v", o)
	}
}

func TestMassDeletionNeedsConfirmation(t *testing.T) {
	e := newEnv(t)
	e.fake.add("styles", styles(40, t0)...)
	e.run(Options{})
	for _, r := range styles(40, t0)[:25] {
		e.fake.remove("styles", r.ID)
	}
	e.now = e.now.Add(time.Hour)
	o := e.run(Options{Full: true}).endpoint("styles")
	if o.Deleted != 0 || o.Missing != 25 || o.Status != "warn" || e.fake.lookups != 0 {
		t.Fatalf("guarded: %+v (lookups %d)", o, e.fake.lookups)
	}
	e.now = e.now.Add(time.Hour)
	if o := e.run(Options{Full: true, AllowMassDelete: true}).endpoint("styles"); o.Deleted != 25 {
		t.Fatalf("allowed: %+v", o)
	}
}

func TestTransientErrorsAreRetried(t *testing.T) {
	e := newEnv(t)
	e.fake.add("styles", styles(3, t0)...)
	e.fake.statuses = []int{503, 429}
	sum := e.run(Options{Endpoints: []string{"styles"}})
	o := sum.endpoint("styles")
	if sum.Status != "ok" || o.Retries != 2 || o.Inserted != 3 {
		t.Fatalf("status %s: %+v", sum.Status, o)
	}
	// Retry-After: 7 wins over the shorter backoff.
	for _, d := range e.sleeps {
		if d < 7*time.Second {
			t.Fatalf("slept %s, want at least Retry-After", d)
		}
	}
}

func TestBackoffIsBoundedAndJittered(t *testing.T) {
	e := newEnv(t)
	e.fake.add("styles", styles(1, t0)...)
	e.fake.failFrom = 1
	e.run(Options{Endpoints: []string{"styles"}})
	if len(e.sleeps) != 2 {
		t.Fatalf("sleeps %v", e.sleeps)
	}
	if e.sleeps[0] < 500*time.Millisecond || e.sleeps[0] > time.Second || e.sleeps[1] < time.Second || e.sleeps[1] > 2*time.Second {
		t.Fatalf("backoff %v outside base 1s doubling with equal jitter", e.sleeps)
	}
}

func TestExpiredSessionLogsInAgain(t *testing.T) {
	e := newEnv(t)
	e.fake.add("styles", styles(3, t0)...)
	e.run(Options{Endpoints: []string{"styles"}})
	e.fake.expire401 = true
	e.now = e.now.Add(time.Hour)
	if sum := e.run(Options{Endpoints: []string{"styles"}}); sum.Status != "ok" || e.fake.logins != 2 {
		t.Fatalf("status %s, logins %d", sum.Status, e.fake.logins)
	}
}

func TestLoginFailureStopsTheRun(t *testing.T) {
	e := newEnv(t)
	e.fake.add("styles", styles(3, t0)...)
	e.fake.loginFail = 401
	sum := e.run(Options{})
	if sum.Status != "failed" || !strings.Contains(sum.Error, "authentication failed") {
		t.Fatalf("status %s error %q", sum.Status, sum.Error)
	}
}

func TestFailedEndpointResumesNextRun(t *testing.T) {
	e := newEnv(t)
	e.fake.add("styles", styles(5, t0)...)
	e.fake.add("seasons", record{ID: "SE1", Modified: t0})
	e.fake.failFrom = 3 // two pages of styles (or seasons) arrive, then Centric is down
	e.cfg.Concurrency = 1
	sum := e.run(Options{})
	o := sum.endpoint("styles")
	if sum.Status != "partial" && sum.Status != "failed" || o.Status != "failed" || !strings.Contains(o.Error, "after 3 attempts") {
		t.Fatalf("status %s: %+v", sum.Status, o)
	}
	_, pending, skip, attempt := e.state("styles")
	if !pending.Valid || skip.Int64 != 4 || attempt.String != "failed" {
		t.Fatalf("pending %v skip %v attempt %v", pending, skip, attempt)
	}

	e.fake.failFrom = 0
	e.now = e.now.Add(time.Hour)
	sum = e.run(Options{})
	o = sum.endpoint("styles")
	// Resumed where it stopped; a first fetch's full window isn't re-read for that.
	if sum.Status != "ok" || !o.Resumed || o.Mode != "full" || o.Inserted != 1 || !o.Complete || e.active("styles") != 5 {
		t.Fatalf("resume: status %s %+v", sum.Status, o)
	}
}

func TestFailedEndpointGetsASecondTryInTheRun(t *testing.T) {
	e := newEnv(t)
	e.cfg.EndpointRetryAfter = time.Millisecond
	e.fake.add("styles", styles(5, t0)...)
	e.fake.statuses = []int{500, 500, 500} // all three attempts of one request
	sum := e.run(Options{Endpoints: []string{"styles"}})
	o := sum.endpoint("styles")
	if sum.Status != "ok" || o.Attempts != 2 || o.Inserted != 5 || len(o.Notes) == 0 || !strings.HasPrefix(o.Notes[0], "first attempt failed") {
		t.Fatalf("status %s: %+v", sum.Status, o)
	}
	var status string
	var inserted int
	e.db.QueryRow(`SELECT status, inserted FROM meta_run_endpoints WHERE run_id = ? AND endpoint = 'styles'`, sum.RunID).Scan(&status, &inserted)
	if status != "ok" || inserted != 5 {
		t.Fatalf("recorded %s %d", status, inserted)
	}
}

func TestServerIgnoringSkipFails(t *testing.T) {
	e := newEnv(t)
	e.fake.add("styles", styles(5, t0)...)
	e.fake.ignoreSkip = true
	o := e.run(Options{Endpoints: []string{"styles"}}).endpoint("styles")
	if o.Status != "failed" || !strings.Contains(o.Error, "same page twice") {
		t.Fatalf("%+v", o)
	}
	if _, pending, _, _ := e.state("styles"); pending.Valid {
		t.Fatal("an integrity failure should drop the window")
	}
}

func TestChangeDuringDeltaIsVerified(t *testing.T) {
	e := newEnv(t)
	e.fake.add("styles", styles(3, t0)...)
	e.run(Options{Endpoints: []string{"styles"}})

	e.now = e.now.Add(time.Hour)
	e.fake.add("styles", styles(6, e.now.Add(-30*time.Minute))[3:]...)
	once := false
	e.fake.pageHook = func(path string, skip int) {
		if path == "styles" && skip == 2 && !once {
			once = true
			// Modified while paging: sorts to the end of the window.
			e.fake.add("styles", record{ID: "S003", Modified: e.now, Name: "edited"})
		}
	}
	o := e.run(Options{Endpoints: []string{"styles"}}).endpoint("styles")
	if o.Passes != 2 || !o.Complete || o.Status != "ok" {
		t.Fatalf("%+v", o)
	}
}

func TestStopKeepsTheWindow(t *testing.T) {
	e := newEnv(t)
	e.fake.add("styles", styles(6, t0)...)
	ctx, cancel := context.WithCancelCause(context.Background())
	e.fake.pageHook = func(path string, skip int) {
		if skip == 4 {
			cancel(errors.New("SIGTERM"))
		}
	}
	sum, err := e.runErr(ctx, Options{Endpoints: []string{"styles"}})
	if !errors.Is(err, ErrStopped) || sum.Status != "failed" || !strings.Contains(sum.Error, "SIGTERM") {
		t.Fatalf("err %v status %s error %q", err, sum.Status, sum.Error)
	}
	if _, pending, skip, _ := e.state("styles"); !pending.Valid || skip.Int64 != 4 {
		t.Fatalf("pending %v skip %v", pending, skip)
	}
	var runStatus string
	e.db.QueryRow(`SELECT status FROM meta_runs WHERE run_id = ?`, sum.RunID).Scan(&runStatus)
	if runStatus != "failed" {
		t.Fatalf("run recorded as %s", runStatus)
	}
}

func TestEndpointsRunInParallel(t *testing.T) {
	e := newEnv(t)
	e.fake.add("styles", styles(4, t0)...)
	e.fake.add("seasons", record{ID: "SE1", Modified: t0})
	started := make(chan string, 2)
	release := make(chan struct{})
	e.fake.pageHook = func(path string, skip int) {
		if skip == 0 {
			started <- path
			<-release
		}
	}
	done := make(chan *Summary)
	go func() { done <- e.run(Options{}) }()
	seen := map[string]bool{<-started: true, <-started: true}
	close(release)
	sum := <-done
	if !seen["styles"] || !seen["seasons"] || sum.Status != "ok" {
		t.Fatalf("seen %v status %s", seen, sum.Status)
	}
}

func TestLock(t *testing.T) {
	path := t.TempDir() + "/c.db"
	l, err := store.AcquireLock(path)
	if err != nil {
		t.Fatal(err)
	}
	if !store.Locked(path) {
		t.Fatal("not reported locked")
	}
	if _, err := store.AcquireLock(path); !errors.Is(err, store.ErrLocked) {
		t.Fatalf("second lock: %v", err)
	}
	l.Release()
	if store.Locked(path) {
		t.Fatal("still locked")
	}
}

func TestPhantomCountIsANoteOnFullFetches(t *testing.T) {
	e := newEnv(t)
	e.fake.add("styles", styles(5, t0)...)
	e.run(Options{Endpoints: []string{"styles"}})
	cursor, _, _, _ := e.state("styles")

	e.fake.countExtra = 2
	e.now = e.now.Add(time.Hour)
	o := e.run(Options{Full: true, Endpoints: []string{"styles"}}).endpoint("styles")
	if o.Status != "ok" || o.Complete || len(o.Warnings) != 0 || len(o.Notes) != 1 || !strings.Contains(o.Notes[0], "fetched 5 of the 7") {
		t.Fatalf("full: %+v", o)
	}
	if now, _, _, _ := e.state("styles"); now != cursor {
		t.Fatalf("cursor moved %v -> %v", cursor, now)
	}
	// The next incremental run proves its window (a second pass agrees) and moves the cursor.
	e.now = e.now.Add(time.Hour)
	o = e.run(Options{Endpoints: []string{"styles"}}).endpoint("styles")
	if o.Status != "ok" || !o.Complete || o.Passes != 2 {
		t.Fatalf("incremental: %+v", o)
	}
	if now, _, _, _ := e.state("styles"); now.String != store.Iso(e.now) {
		t.Fatalf("cursor %v, want %s", now, store.Iso(e.now))
	}
}
