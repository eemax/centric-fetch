// Package fetch runs a fetch: every selected endpoint, incremental by default, into the
// database.
//
// Each endpoint reads one "window": everything (full) or everything modified since the last
// successful start less an overlap (delta). Its position is saved with every page, so a run
// that stops for any reason is resumed by the next one. A window counts as complete only
// when it can be proven: no record was seen twice and at least Centric's count arrived.
// Otherwise a delta window is read again, up to maxPasses times, and if it still can't be
// proven its data is kept but the cursor doesn't move, so the next run reads that span again.
package fetch

import (
	"cmp"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eemax/centric-fetch/internal/centric"
	"github.com/eemax/centric-fetch/internal/config"
	"github.com/eemax/centric-fetch/internal/pyjson"
	"github.com/eemax/centric-fetch/internal/store"
)

const (
	countAPIVersion = "v2"
	modifiedField   = "_modified_at"
	lookupBatch     = 50
	maxPasses       = 3
	// A full fetch flags at most this share of an endpoint's records deleted (and at least
	// massDeleteFloor) unless AllowMassDelete: more looks like a Centric problem, not deletions.
	massDeleteShare = 0.10
	massDeleteFloor = 10
	// An outcome lists at most this many of the fields that changed silently, counted over at
	// most silentSample records per endpoint and run (comparing costs a parse of each stored
	// payload; one event in Centric can change tens of thousands of records silently).
	silentFieldsShown = 10
	silentSample      = 1000
)

// Options for one run.
type Options struct {
	Full            bool
	Restart         bool // drop unfinished windows and start over
	AllowMassDelete bool
	Endpoints       []string
	Log             func(format string, args ...any) // progress; nil for none
	Now             func() time.Time                 // nil: time.Now
}

// Outcome is what one endpoint's fetch did.
type Outcome struct {
	Endpoint   string `json:"endpoint"`
	Mode       string `json:"mode"`
	Status     string `json:"status"` // ok, warn, failed
	Resumed    bool   `json:"resumed,omitempty"`
	DeltaFloor string `json:"delta_floor,omitempty"`
	Expected   *int   `json:"expected,omitempty"`
	Fetched    int    `json:"fetched"`
	Inserted   int    `json:"inserted"`
	Updated    int    `json:"updated"`
	Unchanged  int    `json:"unchanged"`
	Restored   int    `json:"restored"`
	Deleted    int    `json:"deleted"`
	Missing    int    `json:"missing"`
	// Updated records whose _modified_at didn't move, and the fields that changed most in the
	// first SilentSampled of them.
	Silent        int            `json:"silent"`
	SilentSampled int            `json:"silent_sampled,omitempty"`
	SilentFields  map[string]int `json:"silent_fields,omitempty"`
	Passes        int            `json:"passes"`
	Complete      bool           `json:"complete"`
	Retries       int            `json:"retries"`
	Attempts      int            `json:"attempts"`
	Seconds       float64        `json:"seconds"`
	Notes         []string       `json:"notes,omitempty"`
	Warnings      []string       `json:"warnings,omitempty"`
	Error         string         `json:"error,omitempty"`

	counts    store.PageCounts
	startedAt string
	finished  string
	retryable bool
}

// Summary is what the run did.
type Summary struct {
	RunID           string     `json:"run_id"`
	Status          string     `json:"status"` // ok, warn, partial, failed
	Mode            string     `json:"mode"`
	StartedAt       string     `json:"started_at"`
	FinishedAt      string     `json:"finished_at"`
	Seconds         float64    `json:"seconds"`
	Error           string     `json:"error,omitempty"`
	ChangeSeqBefore int64      `json:"change_seq_before"`
	ChangeSeqAfter  int64      `json:"change_seq_after"`
	RecordsChanged  int        `json:"records_changed"`
	Totals          Totals     `json:"totals"`
	Endpoints       []*Outcome `json:"endpoints"`
}

// Totals sums the endpoints.
type Totals struct {
	Fetched, Inserted, Updated, Unchanged, Restored, Deleted, Missing, Silent int
}

// MarshalJSON keeps the keys lower case like the rest of the summary.
func (t Totals) MarshalJSON() ([]byte, error) {
	return fmt.Appendf(nil, `{"fetched":%d,"inserted":%d,"updated":%d,"unchanged":%d,"restored":%d,"deleted":%d,"missing":%d,"silent":%d}`,
		t.Fetched, t.Inserted, t.Updated, t.Unchanged, t.Restored, t.Deleted, t.Missing, t.Silent), nil
}

// ErrStopped wraps the run's error when it was cancelled (a signal).
var ErrStopped = errors.New("stopped")

type runner struct {
	db     *store.DB
	client *centric.Client
	cfg    *config.Config
	opts   Options
	runID  string
	log    func(string, ...any)
	now    func() time.Time
}

// Run fetches. The caller holds the writer lock. A returned error means the run couldn't
// record anything (database trouble); fetch problems are in the summary.
func Run(ctx context.Context, db *store.DB, client *centric.Client, cfg *config.Config, opts Options) (*Summary, error) {
	selected, err := cfg.Select(opts.Endpoints)
	if err != nil {
		return nil, err
	}
	r := &runner{db: db, client: client, cfg: cfg, opts: opts, runID: newID(), log: opts.Log, now: opts.Now}
	if r.log == nil {
		r.log = func(string, ...any) {}
	}
	if r.now == nil {
		r.now = time.Now
	}
	started := r.now()
	seqBefore, err := db.LatestChangeSeq()
	if err != nil {
		return nil, err
	}
	mode := "incremental"
	if opts.Full {
		mode = "full"
	}
	sum := &Summary{RunID: r.runID, Mode: mode, StartedAt: store.Iso(started), ChangeSeqBefore: seqBefore}
	if err := db.Tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO meta_runs (run_id, command, started_at, status) VALUES (?, ?, ?, 'running')`,
			r.runID, command(opts, len(selected) < len(cfg.Endpoints), selected), sum.StartedAt)
		return err
	}); err != nil {
		return nil, err
	}

	// --restart drops unfinished windows once, here: the in-run retry below resumes where the
	// first try stopped.
	if opts.Restart {
		if err := db.Tx(func(tx *sql.Tx) error {
			for _, ep := range selected {
				if err := dropWindows(tx, ep.Name, ""); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	// An authentication failure stops everything: no endpoint can work.
	runCtx, abort := context.WithCancelCause(ctx)
	defer abort(nil)
	outcomes := make([]*Outcome, len(selected))
	r.parallel(runCtx, selected, func(i int, ep config.Endpoint) {
		outcomes[i] = r.endpoint(runCtx, ep, abort)
	})

	// One more try for endpoints that failed on transient errors (Centric timing out), after
	// a pause. The window resumes where the first try stopped.
	var again []int
	for i, o := range outcomes {
		if o != nil && o.Status == "failed" && o.retryable {
			again = append(again, i)
		}
	}
	if len(again) > 0 && cfg.EndpointRetryAfter > 0 && runCtx.Err() == nil {
		r.log("retrying %d endpoint(s) in %s", len(again), cfg.EndpointRetryAfter)
		if sleep(runCtx, cfg.EndpointRetryAfter) == nil {
			eps := make([]config.Endpoint, len(again))
			for n, i := range again {
				eps[n] = selected[i]
			}
			r.parallel(runCtx, eps, func(n int, ep config.Endpoint) {
				first := outcomes[again[n]]
				second := r.endpoint(runCtx, ep, abort)
				outcomes[again[n]] = merge(first, second)
				if err := r.recordOutcome(outcomes[again[n]]); err != nil {
					r.log("%s: recording outcome: %v", ep.Name, err)
				}
			})
		}
	}

	for _, o := range outcomes {
		if o != nil {
			sum.Endpoints = append(sum.Endpoints, o)
		}
	}
	var fatal string
	if cause := context.Cause(runCtx); cause != nil {
		fatal = cause.Error()
		if ctx.Err() != nil {
			fatal = "stopped: " + context.Cause(ctx).Error()
		}
	}
	sum.Status = runStatus(sum.Endpoints, len(selected), fatal)
	sum.Error = fatal
	for _, o := range sum.Endpoints {
		sum.Totals.Fetched += o.Fetched
		sum.Totals.Inserted += o.Inserted
		sum.Totals.Updated += o.Updated
		sum.Totals.Unchanged += o.Unchanged
		sum.Totals.Restored += o.Restored
		sum.Totals.Deleted += o.Deleted
		sum.Totals.Missing += o.Missing
		sum.Totals.Silent += o.Silent
	}
	sum.RecordsChanged = sum.Totals.Inserted + sum.Totals.Updated + sum.Totals.Restored + sum.Totals.Deleted
	if sum.ChangeSeqAfter, err = db.LatestChangeSeq(); err != nil {
		return nil, err
	}
	finished := r.now()
	sum.FinishedAt = store.Iso(finished)
	sum.Seconds = round1(finished.Sub(started).Seconds())
	if err := r.finishRun(sum); err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return sum, fmt.Errorf("%w: %v", ErrStopped, context.Cause(ctx))
	}
	return sum, nil
}

// finishRun records the run's final status. Separate so maintenance can amend it.
func (r *runner) finishRun(sum *Summary) error {
	return r.db.Tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE meta_runs SET finished_at = ?, status = ?, error = ? WHERE run_id = ?`,
			sum.FinishedAt, sum.Status, nullString(sum.Error), sum.RunID)
		return err
	})
}

// MarkFailed amends a finished run's status (a failed integrity check after the fetch).
func MarkFailed(db *store.DB, sum *Summary, reason string) error {
	sum.Status = "failed"
	sum.Error = reason
	return db.Tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE meta_runs SET status = 'failed', error = ? WHERE run_id = ?`, reason, sum.RunID)
		return err
	})
}

func (r *runner) parallel(ctx context.Context, eps []config.Endpoint, fn func(int, config.Endpoint)) {
	sem := make(chan struct{}, r.cfg.Concurrency)
	var wg sync.WaitGroup
	for i, ep := range eps {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			fn(i, ep)
		}()
	}
	wg.Wait()
}

type window struct {
	runID, mode, startedAt string
	deltaFloor             string // "" for a full window
	pass, skip, fetched    int
	resumed                bool
	// Records modified at or after this changed while the window was read: the window start
	// less the overlap, so a Centric clock running behind can't hide a change.
	changedSince string
}

type endpointState struct {
	lastSuccessStarted sql.NullString
	pending            map[string]*window // unfinished windows by mode
}

// endpoint fetches one endpoint and records its outcome. It never returns an error: what
// went wrong is in the outcome. An authentication failure aborts the whole run.
func (r *runner) endpoint(ctx context.Context, ep config.Endpoint, abort context.CancelCauseFunc) *Outcome {
	began := time.Now()
	now := r.now()
	o := &Outcome{Endpoint: ep.Name, Status: "ok", Passes: 1, Attempts: 1, startedAt: store.Iso(now)}
	fail := func(err error) *Outcome {
		var auth *centric.AuthError
		var ce *centric.Error
		var bad *store.BadRecord
		var integrity *integrityError
		keepPending := true
		switch {
		case errors.As(err, &auth):
			o.Error = "authentication failed: " + auth.Msg
			abort(errors.New(o.Error))
		case ctx.Err() != nil:
			o.Error = "stopped: " + context.Cause(ctx).Error()
		case errors.As(err, &integrity):
			o.Error = err.Error()
			keepPending = false // the server misbehaved while paging: start over next time
		case errors.As(err, &ce):
			o.Error = err.Error()
			o.retryable = ce.Transient
		case errors.As(err, &bad):
			o.Error = err.Error()
		default:
			o.Error = err.Error()
		}
		o.Status = "failed"
		o.Seconds = round1(time.Since(began).Seconds())
		o.finished = store.Iso(r.now())
		r.log("%s: FAILED %s", ep.Name, o.Error)
		if err := r.recordFailure(ep.Name, o.Mode, o.Error, keepPending); err != nil {
			r.log("%s: recording failure: %v", ep.Name, err)
		}
		if err := r.recordOutcome(o); err != nil {
			r.log("%s: recording outcome: %v", ep.Name, err)
		}
		return o
	}

	state, err := r.state(ep.Name)
	if err != nil {
		return fail(err)
	}
	w, clockWarning := r.openWindow(state, now)
	o.Mode, o.DeltaFloor, o.Resumed = w.mode, w.deltaFloor, w.resumed
	if clockWarning != "" {
		o.Warnings = append(o.Warnings, clockWarning)
	}
	span := "all records"
	if w.deltaFloor != "" {
		span = "since " + w.deltaFloor
	}
	if w.resumed {
		span += fmt.Sprintf(", resuming pass %d at %d", w.pass, w.skip)
	}
	r.log("%s: %s (%s)", ep.Name, w.mode, span)
	if err := r.db.Tx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`UPDATE meta_endpoint_state SET last_attempt_started_at = ?,
			last_attempt_status = 'running', last_attempt_error = NULL WHERE endpoint = ?`, store.Iso(now), ep.Name); err != nil {
			return err
		}
		return savePosition(tx, ep.Name, w)
	}); err != nil {
		return fail(err)
	}

	params := url.Values{"decoded": {"true"}}
	if w.deltaFloor != "" {
		params.Set(modifiedField, "ge"+w.deltaFloor)
	}
	call := &caller{r: r, ep: ep, o: o}
	firstFetch := !state.lastSuccessStarted.Valid
	// Re-reading is how an unproven window gets settled. Delta windows are small, so they're
	// always re-read; a full window only on an endpoint's first fetch, when there's no
	// earlier cursor to fall back to.
	verify := w.mode == "delta" || firstFetch
	complete, err := r.readWindow(ctx, call, w, params, verify)
	if err != nil {
		return fail(err)
	}
	var found []pyjson.Value
	var missing []string
	if w.mode == "full" {
		if found, missing, err = r.confirmUnseen(ctx, call, w); err != nil {
			return fail(err)
		}
	}
	// An unproven window may have skipped a record: keep its data but leave the cursor, so
	// the next incremental run reads that span again. A first fetch has no earlier cursor,
	// so it always moves forward.
	advance := complete || firstFetch
	if !complete && !advance && len(o.Warnings) > 0 {
		o.Warnings = append(o.Warnings, "the incremental cursor was not moved forward")
	}
	if err := r.finishWindow(ep, w, o, found, missing, advance); err != nil {
		return fail(err)
	}
	o.Complete = complete
	if len(o.Warnings) > 0 {
		o.Status = "warn"
	}
	o.Seconds = round1(time.Since(began).Seconds())
	o.finished = store.Iso(r.now())
	if err := r.recordOutcome(o); err != nil {
		r.log("%s: recording outcome: %v", ep.Name, err)
	}
	r.log("%s: %s fetched=%d new=%d changed=%d deleted=%d missing=%d passes=%d %.1fs",
		ep.Name, o.Status, o.Fetched, o.Inserted, o.Updated, o.Deleted, o.Missing, o.Passes, o.Seconds)
	return o
}

// openWindow resumes the endpoint's unfinished window, or opens a new one. Each mode keeps
// its own unfinished window; an unfinished full window also serves an incremental request,
// since it covers more.
func (r *runner) openWindow(s endpointState, now time.Time) (*window, string) {
	requested := "delta"
	if r.opts.Full {
		requested = "full"
	}
	for _, mode := range []string{"full", requested} {
		if w := s.pending[mode]; w != nil {
			w.resumed = true
			w.changedSince = r.changedSince(w.startedAt)
			// A verification pass starts from the top: it has to see the whole window.
			if w.pass > 1 {
				w.skip, w.fetched = 0, 0
			}
			return w, ""
		}
	}
	floor, warning := "", ""
	if requested == "delta" && s.lastSuccessStarted.Valid {
		last, ok := store.ParseTime(s.lastSuccessStarted.String)
		switch {
		case !ok:
			warning = fmt.Sprintf("the last successful start %q can't be read, so a full fetch was taken instead", s.lastSuccessStarted.String)
		case last.After(now):
			warning = fmt.Sprintf("the last successful fetch started at %s, which is in the future; the system clock moved back, so a full fetch was taken instead", s.lastSuccessStarted.String)
		default:
			floor = store.Iso(last.Add(-r.cfg.Overlap))
		}
	}
	mode := requested
	if floor == "" {
		mode = "full" // without a usable cursor take a complete snapshot
	}
	started := store.Iso(now)
	return &window{runID: r.runID, mode: mode, startedAt: started, deltaFloor: floor, pass: 1,
		changedSince: r.changedSince(started)}, warning
}

func (r *runner) changedSince(startedAt string) string {
	t, _ := store.ParseTime(startedAt)
	return store.Iso(t.Add(-r.cfg.Overlap))
}

type integrityError struct{ msg string }

func (e *integrityError) Error() string { return e.msg }

type passResult struct{ lateFinds, duplicates, distinct, concurrent int }

// readWindow reads the window and reports whether it is proven complete.
//
// Pass 1 is proven when every record is distinct and at least the count arrived (extras were
// created while paging). Otherwise a record may have been skipped, and with verify the
// window is read again. A later pass proves it when it finds nothing the earlier passes
// missed, or is itself distinct and complete.
func (r *runner) readWindow(ctx context.Context, call *caller, w *window, params url.Values, verify bool) (bool, error) {
	o := call.o
	expected, err := call.count(ctx, params)
	if err != nil {
		return false, err
	}
	o.Expected = &expected
	var reason string
	if w.pass == 1 {
		first, err := r.readPass(ctx, call, w, params, expected)
		if err != nil {
			return false, err
		}
		// Counted in the database, so a pass resumed by a later run is judged whole.
		var unique int
		if err := r.db.Tx(func(tx *sql.Tx) (err error) {
			unique, err = store.SeenCount(tx, call.ep.Name, w.runID)
			return err
		}); err != nil {
			return false, err
		}
		// A record deleted mid-fetch makes the pages skip one; one created at the same time
		// hides that in the totals. The created one shows as a concurrent change, so a pass
		// that saw one is checked by a second pass.
		changed := first.concurrent > 0 || w.resumed
		if unique == w.fetched && unique >= expected {
			if !changed {
				return true, nil
			}
			if w.mode == "full" && !verify {
				o.Notes = append(o.Notes, "records changed during the full fetch, so the incremental cursor stays "+
					"where it was and the next incremental run re-reads from there")
				return false, nil
			}
			if w.mode == "full" {
				return true, nil // first fetch: re-reading everything isn't worth it for this alone
			}
		}
		switch {
		case unique < w.fetched:
			reason = fmt.Sprintf("%d records appeared twice (records moved while paging)", w.fetched-unique)
		case unique < expected:
			reason = fmt.Sprintf("fetched %d of the %d records Centric counted", unique, expected)
		case w.resumed:
			reason = "the window was resumed from an earlier run"
		default:
			reason = fmt.Sprintf("%d records changed while the window was being read", first.concurrent)
		}
		if !verify {
			// Only a full window that isn't an endpoint's first gets here. Nothing is lost by
			// leaving it unproven: the cursor stays, the next incremental run re-reads (and
			// proves) everything modified since, and stored records the listing skipped are
			// looked up by id. Centric counting records it never lists ends up here every time,
			// which is why this is a note, not a warning.
			o.Notes = append(o.Notes, reason+"; the window can't be proven complete, so the incremental cursor "+
				"stays where it was and the next incremental run re-reads from there")
			return false, nil
		}
		w.pass, w.skip, w.fetched = 2, 0, 0
	} else {
		reason = "the previous run's verification pass was interrupted"
	}
	o.Notes = append(o.Notes, reason+"; reading the window again")
	for ; w.pass <= maxPasses; w.pass++ {
		o.Passes = w.pass
		if err := r.db.Tx(func(tx *sql.Tx) error { return savePosition(tx, call.ep.Name, w) }); err != nil {
			return false, err
		}
		res, err := r.readPass(ctx, call, w, params, expected)
		if err != nil {
			return false, err
		}
		found := "found nothing the earlier passes missed"
		if res.lateFinds > 0 {
			found = fmt.Sprintf("found %d records the earlier passes missed", res.lateFinds)
		}
		if res.lateFinds == 0 || (res.duplicates == 0 && res.distinct >= expected) {
			o.Notes = append(o.Notes, fmt.Sprintf("pass %d %s and proved the window complete", w.pass, found))
			return true, nil
		}
		o.Notes = append(o.Notes, fmt.Sprintf("pass %d %s", w.pass, found))
		w.skip, w.fetched = 0, 0
	}
	o.Warnings = append(o.Warnings, fmt.Sprintf("the window could not be proven complete after %d passes because records kept changing", maxPasses))
	return false, nil
}

// readPass pages through the window until Centric returns an empty page. The offset moves
// by the records actually received, so a short page never leaves a gap, and records created
// while paging are collected rather than treated as an error.
func (r *runner) readPass(ctx context.Context, call *caller, w *window, params url.Values, expected int) (passResult, error) {
	o, ep := call.o, call.ep
	runaway := 3*expected + 100*ep.Limit
	var res passResult
	seen := map[string]struct{}{}
	var previous []string
	for {
		items, err := call.page(ctx, params, w.skip)
		if err != nil {
			return res, err
		}
		if len(items) == 0 {
			res.distinct = len(seen)
			return res, nil
		}
		ids := make([]string, len(items))
		for i, item := range items {
			ids[i], _ = store.RecordID(item, ep.Name)
		}
		if slices.Equal(ids, previous) {
			return res, &integrityError{fmt.Sprintf("%s: Centric returned the same page twice in a row; it isn't honouring 'skip', so the endpoint can't be paged", ep.Name)}
		}
		previous = ids
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				res.duplicates++
			}
			seen[id] = struct{}{}
		}
		var counts store.PageCounts
		err = r.db.Tx(func(tx *sql.Tx) error {
			seq, err := store.NextChangeSeq(tx)
			if err != nil {
				return err
			}
			page := store.Page{Endpoint: ep.Name, Items: items, RunID: w.runID, Now: store.Iso(r.now()),
				ChangeSeq: seq, ChangedSince: w.changedSince, SilentSample: silentSample - o.counts.SilentSampled}
			if w.pass > 1 {
				page.WindowStartedAt = w.startedAt
			}
			if counts, err = store.ApplyPage(tx, page); err != nil {
				return err
			}
			next := *w
			next.skip += len(items)
			next.fetched += len(items)
			return savePosition(tx, ep.Name, &next)
		})
		if err != nil {
			return res, err
		}
		w.skip += len(items)
		w.fetched += len(items)
		res.lateFinds += counts.LateFinds
		res.concurrent += counts.Concurrent
		o.addCounts(counts)
		// Pass 1 counts the whole window, including pages read before a resume.
		if w.pass == 1 {
			o.Fetched = w.fetched
		} else {
			o.Fetched += len(items)
		}
		r.log("%s pass %d: %d/%d", ep.Name, w.pass, w.fetched, expected)
		if w.fetched > runaway {
			return res, &integrityError{fmt.Sprintf("%s: received %d records for a count of %d without reaching the end; stopping", ep.Name, w.fetched, expected)}
		}
	}
}

// confirmUnseen looks up the stored records a full fetch didn't return and splits them into
// still there (found) and gone (missing).
func (r *runner) confirmUnseen(ctx context.Context, call *caller, w *window) ([]pyjson.Value, []string, error) {
	o, ep := call.o, call.ep
	var unseen []string
	if err := r.db.Tx(func(tx *sql.Tx) (err error) {
		unseen, err = store.UnseenIDs(tx, ep.Name, w.runID)
		return err
	}); err != nil || len(unseen) == 0 {
		return nil, nil, err
	}
	if o.Fetched == 0 {
		o.Warnings = append(o.Warnings, fmt.Sprintf("Centric returned no records but %d are stored; nothing was flagged deleted", len(unseen)))
		return nil, nil, nil
	}
	// More missing than real deletions look like: Centric's listing is probably wrong, so
	// don't spend a lookup per 50 records only to flag nothing.
	if !r.opts.AllowMassDelete {
		var active int
		if err := r.db.Tx(func(tx *sql.Tx) (err error) {
			active, err = store.ActiveCount(tx, ep.Name)
			return err
		}); err != nil {
			return nil, nil, err
		}
		if limit := max(massDeleteFloor, int(float64(active)*massDeleteShare)); len(unseen) > limit {
			o.Missing = len(unseen)
			o.Warnings = append(o.Warnings, fmt.Sprintf("%d of %d stored records weren't in the full listing, more than "+
				"look like real deletions; none were looked up or flagged deleted (fetch --full --allow-mass-delete does)", len(unseen), active))
			return nil, nil, nil
		}
	}
	found, err := call.lookup(ctx, unseen)
	if err != nil {
		return nil, nil, err
	}
	if len(found) > 0 {
		o.Warnings = append(o.Warnings, fmt.Sprintf("%d records missing from the full fetch still exist in Centric "+
			"(skipped while paging, or outside this endpoint's filters); they were stored", len(found)))
	}
	have := map[string]bool{}
	for _, item := range found {
		id, _ := store.RecordID(item, ep.Name)
		have[id] = true
	}
	var missing []string
	for _, id := range unseen {
		if !have[id] {
			missing = append(missing, id)
		}
	}
	return found, missing, nil
}

func (r *runner) finishWindow(ep config.Endpoint, w *window, o *Outcome, found []pyjson.Value, missing []string, advance bool) error {
	return r.db.Tx(func(tx *sql.Tx) error {
		now := store.Iso(r.now())
		if len(found) > 0 {
			seq, err := store.NextChangeSeq(tx)
			if err != nil {
				return err
			}
			counts, err := store.ApplyPage(tx, store.Page{Endpoint: ep.Name, Items: found, RunID: w.runID, Now: now, ChangeSeq: seq})
			if err != nil {
				return err
			}
			o.addCounts(counts)
		}
		if len(missing) > 0 { // confirmed gone, and within the mass-deletion guard
			seq, err := store.NextChangeSeq(tx)
			if err != nil {
				return err
			}
			if o.Deleted, err = store.MarkDeleted(tx, ep.Name, missing, now, seq); err != nil {
				return err
			}
		}
		if advance {
			if _, err := tx.Exec(`UPDATE meta_endpoint_state SET last_success_started_at = ? WHERE endpoint = ?`, w.startedAt, ep.Name); err != nil {
				return err
			}
		}
		// A finished full window also ends an unfinished incremental one. Resuming that would
		// misjudge it: the full re-stamped its records with another run's id, so they'd look
		// like duplicates and late finds. Nothing is lost, since an unfinished window never moved
		// the cursor: the next incremental run opens one from the same floor.
		drop := w.mode
		if w.mode == "full" {
			drop = ""
		}
		if err := dropWindows(tx, ep.Name, drop); err != nil {
			return err
		}
		status := "ok"
		if len(o.Warnings) > 0 {
			status = "warn"
		}
		_, err := tx.Exec(`UPDATE meta_endpoint_state
			SET last_success_finished_at = ?, last_success_mode = ?,
			    last_full_success_at = CASE WHEN ? = 'full' THEN ? ELSE last_full_success_at END,
			    last_incremental_success_at = CASE WHEN ? = 'delta' THEN ? ELSE last_incremental_success_at END,
			    last_attempt_status = ?, last_attempt_error = NULL
			WHERE endpoint = ?`, now, w.mode, w.mode, now, w.mode, now, status, ep.Name)
		return err
	})
}

func savePosition(tx *sql.Tx, endpoint string, w *window) error {
	_, err := tx.Exec(`INSERT OR REPLACE INTO meta_windows (endpoint, mode, run_id, pass, started_at,
		delta_floor, next_skip, fetched) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		endpoint, w.mode, w.runID, w.pass, w.startedAt, nullString(w.deltaFloor), w.skip, w.fetched)
	return err
}

// dropWindows forgets an endpoint's unfinished window of mode, or all of them for "".
func dropWindows(tx *sql.Tx, endpoint, mode string) error {
	_, err := tx.Exec(`DELETE FROM meta_windows WHERE endpoint = ? AND (? = '' OR mode = ?)`, endpoint, mode, mode)
	return err
}

func (r *runner) state(endpoint string) (endpointState, error) {
	s := endpointState{pending: map[string]*window{}}
	err := r.db.Tx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO meta_endpoint_state (endpoint) VALUES (?) ON CONFLICT DO NOTHING`, endpoint); err != nil {
			return err
		}
		if err := tx.QueryRow(`SELECT last_success_started_at FROM meta_endpoint_state WHERE endpoint = ?`,
			endpoint).Scan(&s.lastSuccessStarted); err != nil {
			return err
		}
		rows, err := tx.Query(`SELECT mode, run_id, pass, started_at, delta_floor, next_skip, fetched
			FROM meta_windows WHERE endpoint = ?`, endpoint)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			w := &window{}
			var floor sql.NullString
			if err := rows.Scan(&w.mode, &w.runID, &w.pass, &w.startedAt, &floor, &w.skip, &w.fetched); err != nil {
				return err
			}
			w.deltaFloor, w.pass = floor.String, max(w.pass, 1)
			s.pending[w.mode] = w
		}
		return rows.Err()
	})
	return s, err
}

func (r *runner) recordFailure(endpoint, mode, msg string, keepPending bool) error {
	return r.db.Tx(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`UPDATE meta_endpoint_state SET last_attempt_status = 'failed', last_attempt_error = ?
			WHERE endpoint = ?`, msg, endpoint); err != nil {
			return err
		}
		if keepPending || mode == "" {
			return nil
		}
		return dropWindows(tx, endpoint, mode)
	})
}

func (r *runner) recordOutcome(o *Outcome) error {
	var expected any
	if o.Expected != nil {
		expected = *o.Expected
	}
	o.SilentFields = topFields(o.counts.SilentFields, silentFieldsShown)
	var silentFields any
	if len(o.SilentFields) > 0 {
		b, _ := json.Marshal(o.SilentFields)
		silentFields = string(b)
	}
	return r.db.Tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT OR REPLACE INTO meta_run_endpoints (run_id, endpoint, mode, delta_floor,
			resumed, status, expected, fetched, inserted, updated, unchanged, restored, deleted, missing,
			silent, silent_fields, passes, complete, note, warning, error, started_at, finished_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			r.runID, o.Endpoint, o.Mode, nullString(o.DeltaFloor), o.Resumed, o.Status, expected, o.Fetched,
			o.Inserted, o.Updated, o.Unchanged, o.Restored, o.Deleted, o.Missing, o.Silent, silentFields,
			o.Passes, o.Complete, nullString(strings.Join(o.Notes, "; ")), nullString(strings.Join(o.Warnings, "; ")),
			nullString(o.Error), o.startedAt, nullString(o.finished))
		return err
	})
}

func (o *Outcome) addCounts(c store.PageCounts) {
	o.counts.Add(c)
	o.syncCounts()
}

// syncCounts copies the summed page counts into the outcome's fields. The silent fields are
// ranked once, when the outcome is recorded.
func (o *Outcome) syncCounts() {
	c := o.counts
	o.Inserted, o.Updated, o.Unchanged, o.Restored = c.Inserted, c.Updated, c.Unchanged, c.Restored
	o.Silent, o.SilentSampled = c.Silent, c.SilentSampled
}

// topFields keeps the n most frequent fields (ties by name).
func topFields(counts map[string]int, n int) map[string]int {
	if len(counts) == 0 {
		return nil
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b string) int {
		return cmp.Or(cmp.Compare(counts[b], counts[a]), cmp.Compare(a, b))
	})
	out := map[string]int{}
	for _, k := range keys[:min(n, len(keys))] {
		out[k] = counts[k]
	}
	return out
}

// merge folds a retried endpoint's two attempts into one outcome.
func merge(first, second *Outcome) *Outcome {
	m := *second
	m.counts = first.counts
	m.counts.SilentFields = maps.Clone(first.counts.SilentFields)
	m.counts.Add(second.counts)
	m.syncCounts()
	m.Deleted += first.Deleted
	m.Retries += first.Retries
	m.Attempts = first.Attempts + second.Attempts
	m.Seconds = round1(first.Seconds + second.Seconds)
	m.startedAt = first.startedAt
	m.Notes = append([]string{"first attempt failed (" + first.Error + "); retried"}, second.Notes...)
	return &m
}

func runStatus(outcomes []*Outcome, selected int, fatal string) string {
	failed, warned := 0, false
	for _, o := range outcomes {
		switch o.Status {
		case "failed":
			failed++
		case "warn":
			warned = true
		}
	}
	switch {
	case fatal != "" || (selected > 0 && failed == selected):
		return "failed"
	case failed > 0 || len(outcomes) < selected:
		return "partial"
	case warned:
		return "warn"
	}
	return "ok"
}

// caller makes one endpoint's requests and counts its retries.
type caller struct {
	r  *runner
	ep config.Endpoint
	o  *Outcome
}

func (c *caller) get(ctx context.Context, apiVersion, path string, q url.Values) (pyjson.Value, error) {
	u := c.r.client.URL(apiVersion, path)
	body, err := c.r.client.GetJSON(ctx, u, q, func(msg string) {
		c.o.Retries++
		c.r.log("%s: %s", c.ep.Name, msg)
	})
	if err != nil {
		return nil, err
	}
	v, err := pyjson.Parse(body)
	if err != nil {
		return nil, &centric.Error{Msg: fmt.Sprintf("invalid JSON from %s: %v", u, err)}
	}
	return v, nil
}

func (c *caller) count(ctx context.Context, params url.Values) (int, error) {
	v, err := c.get(ctx, countAPIVersion, c.ep.CountPath, params)
	var ce *centric.Error
	if errors.As(err, &ce) && ce.Status == 404 {
		return 0, &centric.Error{Status: 404, Msg: fmt.Sprintf("%s: count path %q doesn't exist on this Centric instance (HTTP 404)", c.ep.Name, c.ep.CountPath)}
	}
	if err != nil {
		return 0, err
	}
	obj, _ := v.(*pyjson.Object)
	if obj != nil {
		if n, ok := obj.Get("count"); ok {
			if num, ok := n.(pyjson.Number); ok {
				if f, err := strconv.ParseFloat(string(num), 64); err == nil && f >= 0 && f == float64(int(f)) {
					return int(f), nil
				}
			}
		}
	}
	return 0, &centric.Error{Msg: fmt.Sprintf("%s: count endpoint returned no valid 'count'", c.ep.Name)}
}

func (c *caller) page(ctx context.Context, params url.Values, skip int) ([]pyjson.Value, error) {
	q := cloneValues(params)
	q.Set("sort", modifiedField)
	q.Set("skip", strconv.Itoa(skip))
	q.Set("limit", strconv.Itoa(c.ep.Limit))
	v, err := c.get(ctx, c.ep.APIVersion, c.ep.Path, q)
	if err != nil {
		return nil, err
	}
	list, ok := v.([]pyjson.Value)
	if !ok {
		return nil, &centric.Error{Msg: fmt.Sprintf("%s: expected a JSON array of records", c.ep.Name)}
	}
	return list, nil
}

// lookup asks Centric directly for records by id. Endpoint filters are deliberately not
// applied: a record only counts as gone when it no longer exists at all. An endpoint that
// ignores the id filter can't confirm anything, so that fails rather than delete.
func (c *caller) lookup(ctx context.Context, ids []string) ([]pyjson.Value, error) {
	var found []pyjson.Value
	for start := 0; start < len(ids); start += lookupBatch {
		batch := ids[start:min(start+lookupBatch, len(ids))]
		q := url.Values{"id": batch, "decoded": {"true"}, "limit": {strconv.Itoa(len(batch))}}
		v, err := c.get(ctx, c.ep.APIVersion, c.ep.Path, q)
		if err != nil {
			return nil, err
		}
		list, ok := v.([]pyjson.Value)
		if !ok {
			return nil, &centric.Error{Msg: fmt.Sprintf("%s: expected a JSON array when looking up records by id", c.ep.Name)}
		}
		want := map[string]bool{}
		for _, id := range batch {
			want[id] = true
		}
		for _, item := range list {
			id, err := store.RecordID(item, c.ep.Name)
			if err != nil {
				return nil, err
			}
			if !want[id] {
				return nil, &centric.Error{Msg: fmt.Sprintf("%s: looking up records by id returned other records (e.g. %q); "+
					"this endpoint doesn't support id lookups, so missing records can't be confirmed", c.ep.Name, id)}
			}
		}
		found = append(found, list...)
	}
	return found, nil
}

func command(o Options, subset bool, eps []config.Endpoint) string {
	parts := []string{"fetch"}
	if o.Full {
		parts = append(parts, "--full")
	}
	if o.Restart {
		parts = append(parts, "--restart")
	}
	if subset {
		for _, e := range eps {
			parts = append(parts, "-e", e.Name)
		}
	}
	return strings.Join(parts, " ")
}

func cloneValues(v url.Values) url.Values {
	out := make(url.Values, len(v)+3)
	for k, vs := range v {
		out[k] = slices.Clone(vs)
	}
	return out
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func newID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func round1(f float64) float64 { return float64(int64(f*10+0.5)) / 10 }

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
