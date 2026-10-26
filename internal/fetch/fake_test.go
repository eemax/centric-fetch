package fetch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eemax/centric-fetch/internal/centric"
	"github.com/eemax/centric-fetch/internal/config"
	"github.com/eemax/centric-fetch/internal/store"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type record struct {
	ID       string
	Modified time.Time
	Name     string
	hidden   bool // left out of listings, still found by id
}

func (r record) json() map[string]any {
	return map[string]any{"id": r.ID, "_modified_at": store.Iso(r.Modified), "node_name": r.Name}
}

// fakeCentric is an in-memory Centric: login, counts, sorted and filtered paging, id lookup,
// and knobs for failures.
type fakeCentric struct {
	mu         sync.Mutex
	records    map[string][]record // by path
	counts     map[string]string   // count path -> path
	logins     int
	token      string
	lookups    int
	requests   int
	expire401  bool  // the next authorized request gets a 401
	loginFail  int   // login answers this status (0: works)
	statuses   []int // the next listing requests answer these statuses, in order
	failFrom   int   // listing pages from this one on (1-based) answer 500; 0: never
	pages      int
	ignoreSkip bool
	countExtra int                         // records Centric counts but never lists
	pageHook   func(path string, skip int) // runs before a page is served (lock released)
}

func newFake() *fakeCentric {
	return &fakeCentric{
		records: map[string][]record{"styles": nil, "seasons": nil},
		counts:  map[string]string{"count/Style": "styles", "count/Season": "seasons"},
	}
}

func (f *fakeCentric) add(path string, rs ...record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range rs {
		list := f.records[path][:0:0]
		for _, old := range f.records[path] {
			if old.ID != r.ID {
				list = append(list, old)
			}
		}
		f.records[path] = append(list, r)
	}
}

func (f *fakeCentric) remove(path, id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	list := f.records[path][:0:0]
	for _, r := range f.records[path] {
		if r.ID != id {
			list = append(list, r)
		}
	}
	f.records[path] = list
}

func (f *fakeCentric) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	f.mu.Lock()
	f.requests++
	path := strings.TrimPrefix(req.URL.Path, "/api/")
	_, path, _ = strings.Cut(path, "/") // drop the API version
	if path == "session" {
		f.logins++
		if f.loginFail != 0 {
			f.mu.Unlock()
			http.Error(w, `{"message":"bad credentials"}`, f.loginFail)
			return
		}
		f.token = fmt.Sprintf("tok%d", f.logins)
		f.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]string{"token": "SecurityTokenURL=" + f.token})
		return
	}
	if req.Header.Get("Authorization") != "Bearer "+f.token || f.expire401 {
		f.expire401 = false
		f.mu.Unlock()
		http.Error(w, "unauthorized", 401)
		return
	}
	q := req.URL.Query()
	floor := strings.TrimPrefix(q.Get("_modified_at"), "ge")
	matches := func(list []record) []record {
		var out []record
		for _, r := range list {
			if floor == "" || store.Iso(r.Modified) >= floor {
				out = append(out, r)
			}
		}
		return out
	}
	if target, ok := f.counts[path]; ok {
		n := len(matches(f.records[target])) + f.countExtra
		f.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]int{"count": n})
		return
	}
	list, ok := f.records[path]
	if !ok {
		f.mu.Unlock()
		http.NotFound(w, req)
		return
	}
	if ids := q["id"]; len(ids) > 0 {
		f.lookups++
		want := map[string]bool{}
		for _, id := range ids {
			want[id] = true
		}
		out := []map[string]any{}
		for _, r := range list {
			if want[r.ID] {
				out = append(out, r.json())
			}
		}
		f.mu.Unlock()
		json.NewEncoder(w).Encode(out)
		return
	}
	f.pages++
	if len(f.statuses) > 0 {
		status := f.statuses[0]
		f.statuses = f.statuses[1:]
		f.mu.Unlock()
		w.Header().Set("Retry-After", "7")
		http.Error(w, `{"error":"busy"}`, status)
		return
	}
	if f.failFrom > 0 && f.pages >= f.failFrom {
		f.mu.Unlock()
		http.Error(w, "down", 500)
		return
	}
	hook := f.pageHook
	skip, _ := strconv.Atoi(q.Get("skip"))
	f.mu.Unlock()
	if hook != nil {
		hook(path, skip)
	}
	f.mu.Lock()
	var visible []record
	for _, r := range matches(f.records[path]) {
		if !r.hidden {
			visible = append(visible, r)
		}
	}
	f.mu.Unlock()
	sort.Slice(visible, func(i, j int) bool {
		if !visible[i].Modified.Equal(visible[j].Modified) {
			return visible[i].Modified.Before(visible[j].Modified)
		}
		return visible[i].ID < visible[j].ID
	})
	if f.ignoreSkip {
		skip = 0
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	out := []map[string]any{}
	for i := skip; i < len(visible) && i < skip+limit; i++ {
		out = append(out, visible[i].json())
	}
	json.NewEncoder(w).Encode(out)
}

// env is a test setup: fake Centric, config, database, clock.
type env struct {
	t      *testing.T
	fake   *fakeCentric
	cfg    *config.Config
	client *centric.Client
	db     *store.DB
	now    time.Time
	sleeps []time.Duration
}

func newEnv(t *testing.T) *env {
	t.Helper()
	fake := newFake()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	styles := config.Endpoint{Name: "styles", Path: "styles", APIVersion: "v2", CountPath: "count/Style", Limit: 2}
	seasons := config.Endpoint{Name: "seasons", Path: "seasons", APIVersion: "v2", CountPath: "count/Season", Limit: 2}
	cfg := &config.Config{
		Database:    filepath.Join(t.TempDir(), "centric.db"),
		Endpoints:   []config.Endpoint{styles, seasons},
		Concurrency: 2,
		Overlap:     10 * time.Minute,
		HTTP:        config.HTTP{Timeout: 5 * time.Second, Attempts: 3, BackoffBase: time.Second, BackoffMax: 4 * time.Second},
	}
	e := &env{t: t, fake: fake, cfg: cfg, now: t0.Add(24 * time.Hour)}
	e.client = centric.New(config.Credentials{BaseURL: srv.URL, Username: "u", Password: "p"}, cfg.HTTP, nil)
	e.client.SetSleep(func(ctx context.Context, d time.Duration) error {
		e.sleeps = append(e.sleeps, d)
		return ctx.Err()
	})
	db, err := store.Open(cfg.Database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	e.db = db
	return e
}

func (e *env) run(opts Options) *Summary {
	e.t.Helper()
	sum, err := e.runErr(context.Background(), opts)
	if err != nil {
		e.t.Fatal(err)
	}
	return sum
}

func (e *env) runErr(ctx context.Context, opts Options) (*Summary, error) {
	opts.Now = func() time.Time { return e.now }
	return Run(ctx, e.db, e.client, e.cfg, opts)
}

func (s *Summary) endpoint(name string) *Outcome {
	for _, o := range s.Endpoints {
		if o.Endpoint == name {
			return o
		}
	}
	return nil
}

func styles(n int, modified time.Time) []record {
	out := make([]record, n)
	for i := range out {
		out[i] = record{ID: fmt.Sprintf("S%03d", i), Modified: modified.Add(time.Duration(i) * time.Second), Name: fmt.Sprintf("style %d", i)}
	}
	return out
}
