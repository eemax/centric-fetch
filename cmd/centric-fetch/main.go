// centric-fetch fetches Centric PLM records into a local SQLite database. It is built for
// running unattended on a server: silent while it works, one JSON summary on stdout when it
// finishes, and an exit code that says how it went.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/eemax/centric-fetch/internal/centric"
	"github.com/eemax/centric-fetch/internal/config"
	"github.com/eemax/centric-fetch/internal/fetch"
	"github.com/eemax/centric-fetch/internal/status"
	"github.com/eemax/centric-fetch/internal/store"
)

// Set by the build: -ldflags "-X main.version=…".
var version = "dev"

const usage = `usage: centric-fetch <command> [flags]

commands:
  fetch    fetch from Centric (incremental; --full for everything plus deletions)
  status   what the database holds and what the last run did
  check    exit 0 when the last fetch was good and the data is fresh, else 1
  version  print the version

Every command takes --config PATH (default $CENTRIC_FETCH_CONFIG, then /etc/centric-fetch/config.json).
Run 'centric-fetch <command> -h' for its flags.
`

// Exit codes.
const (
	exitOK     = 0
	exitFailed = 1  // the run failed or was partial; check: unhealthy
	exitUsage  = 2  // bad flags, config or credentials
	exitBusy   = 75 // another fetch holds the lock (sysexits' EX_TEMPFAIL): skipped, try later
	exitSignal = 128
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return exitUsage
	}
	switch args[0] {
	case "fetch":
		return cmdFetch(args[1:], stdout, stderr)
	case "status":
		return cmdStatus(args[1:], stdout, stderr)
	case "check":
		return cmdCheck(args[1:], stdout, stderr)
	case "version", "--version", "-V":
		fmt.Fprintln(stdout, "centric-fetch", version)
		return exitOK
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return exitOK
	}
	fmt.Fprintf(stderr, "centric-fetch: unknown command %q\n\n%s", args[0], usage)
	return exitUsage
}

type endpointList []string

func (e *endpointList) String() string { return strings.Join(*e, ",") }
func (e *endpointList) Set(v string) error {
	for _, name := range strings.Split(v, ",") {
		if name = strings.TrimSpace(name); name != "" {
			*e = append(*e, name)
		}
	}
	return nil
}

func cmdFetch(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("fetch", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "config file")
	full := fs.Bool("full", false, "fetch everything, and flag records deleted in Centric")
	restart := fs.Bool("restart", false, "drop unfinished windows instead of resuming them")
	massDelete := fs.Bool("allow-mass-delete", false, "with --full: flag deletions even when more than 10% of an endpoint is gone")
	maintain := fs.Bool("maintain", false, "after the fetch: integrity check, VACUUM, optimize, WAL checkpoint")
	verbose := fs.Bool("v", false, "progress on stderr")
	waitFor := fs.String("wait", "", "when another fetch holds the lock, wait up to this long for it (e.g. 30m) instead of skipping")
	var endpoints endpointList
	fs.Var(&endpoints, "e", "only this endpoint (repeat, or comma-separated)")
	if fs.Parse(args) != nil {
		return exitUsage
	}
	if *massDelete && !*full {
		fmt.Fprintln(stderr, "centric-fetch: --allow-mass-delete only applies to --full")
		return exitUsage
	}
	var wait time.Duration
	if *waitFor != "" {
		var err error
		if wait, err = status.ParseDuration(*waitFor); err != nil {
			return fail(stderr, exitUsage, fmt.Errorf("--wait %q: %w", *waitFor, err))
		}
	}
	cfg, err := config.Load(config.Resolve(*configPath))
	if err != nil {
		return fail(stderr, exitUsage, err)
	}
	if _, err := cfg.Select(endpoints); err != nil {
		return fail(stderr, exitUsage, err)
	}
	creds, err := cfg.LoadCredentials()
	if err != nil {
		return fail(stderr, exitUsage, err)
	}

	// SIGTERM (systemd stop), SIGINT, SIGHUP: stop requesting, keep what's stored, exit
	// 128+signal. The next run resumes where this one stopped.
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	var caught os.Signal
	go func() {
		caught = <-signals
		// A second signal ends the process at once.
		signal.Reset(syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
		cancel(fmt.Errorf("%s", strings.ToUpper(signalName(caught))))
	}()
	signalExit := func() int {
		if s, ok := caught.(syscall.Signal); ok {
			return exitSignal + int(s)
		}
		return exitSignal + int(syscall.SIGTERM)
	}

	began := time.Now()
	lock, err := store.AcquireLockWait(ctx, cfg.Database, wait)
	if errors.Is(err, store.ErrLocked) || (err != nil && ctx.Err() != nil) {
		msg := fmt.Sprintf("%v (%s.lock)", store.ErrLocked, cfg.Database)
		if wait > 0 {
			msg += "; waited " + *waitFor
		}
		if ctx.Err() != nil {
			msg = "stopped while waiting for the lock: " + context.Cause(ctx).Error()
		}
		printSkipped(stdout, *full, began, msg)
		fmt.Fprintln(stderr, "centric-fetch:", msg)
		if ctx.Err() != nil {
			return signalExit()
		}
		return exitBusy
	} else if err != nil {
		return fail(stderr, exitFailed, err)
	}
	defer lock.Release()
	db, err := store.Open(cfg.Database)
	if err != nil {
		return fail(stderr, exitFailed, err)
	}
	defer db.Close()

	logf := func(string, ...any) {}
	if *verbose {
		logf = func(format string, a ...any) {
			fmt.Fprintf(stderr, time.Now().UTC().Format("15:04:05 ")+format+"\n", a...)
		}
	}
	client := centric.New(creds, cfg.HTTP, nil)
	sum, err := fetch.Run(ctx, db, client, cfg, fetch.Options{
		Full: *full, Restart: *restart, AllowMassDelete: *massDelete, Endpoints: endpoints, Log: logf,
	})
	if sum == nil {
		return fail(stderr, exitFailed, err)
	}
	out := struct {
		Event string `json:"event"`
		*fetch.Summary
		Maintenance *store.MaintainResult `json:"maintenance,omitempty"`
	}{Event: "run", Summary: sum}
	if *maintain && err == nil && sum.Status != "failed" {
		logf("maintenance")
		res, merr := db.Maintain()
		out.Maintenance = res
		if merr != nil {
			if ferr := fetch.MarkFailed(db, sum, merr.Error()); ferr != nil {
				fmt.Fprintln(stderr, "centric-fetch:", ferr)
			}
		}
	}
	line, _ := json.Marshal(out)
	fmt.Fprintln(stdout, string(line))
	if errors.Is(err, fetch.ErrStopped) {
		return signalExit()
	}
	if sum.Status == "failed" || sum.Status == "partial" {
		return exitFailed
	}
	return exitOK
}

// printSkipped prints the summary of a fetch that didn't run because the lock was held: the
// usual shape, with nothing fetched.
func printSkipped(stdout io.Writer, full bool, began time.Time, msg string) {
	sum := &fetch.Summary{Status: "skipped", Mode: "incremental", StartedAt: store.Iso(began), Error: msg,
		Endpoints: []*fetch.Outcome{}}
	if full {
		sum.Mode = "full"
	}
	now := time.Now()
	sum.FinishedAt = store.Iso(now)
	sum.Seconds = float64(now.Sub(began).Milliseconds()/100) / 10
	line, _ := json.Marshal(struct {
		Event string `json:"event"`
		*fetch.Summary
	}{"run", sum})
	fmt.Fprintln(stdout, string(line))
}

func cmdStatus(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "config file")
	asJSON := fs.Bool("json", false, "print JSON")
	if fs.Parse(args) != nil {
		return exitUsage
	}
	cfg, err := config.Load(config.Resolve(*configPath))
	if err != nil {
		return fail(stderr, exitUsage, err)
	}
	st, err := status.Collect(cfg)
	if err != nil {
		return fail(stderr, exitFailed, err)
	}
	if *asJSON {
		return printJSON(stdout, st)
	}
	fmt.Fprintf(stdout, "database  %s (%s)\n", st.Database, map[bool]string{true: humanBytes(st.SizeBytes), false: "not created yet"}[st.Exists])
	if st.Running {
		fmt.Fprintln(stdout, "running   a fetch is running now")
	}
	if r := st.LastRun; r != nil {
		fmt.Fprintf(stdout, "last run  %s %s, %s", r.StartedAt, r.Command, r.Status)
		if r.DurationSeconds != nil {
			fmt.Fprintf(stdout, " in %.0fs", *r.DurationSeconds)
		}
		fmt.Fprintf(stdout, ": %d fetched, %d new, %d changed, %d deleted\n",
			r.Totals["fetched"], r.Totals["inserted"], r.Totals["updated"], r.Totals["deleted"])
		if r.Error != nil {
			fmt.Fprintf(stdout, "          error: %s\n", *r.Error)
		}
	}
	fmt.Fprintf(stdout, "\n%-16s %9s %8s  %-24s %-24s %s\n", "endpoint", "records", "deleted", "last success", "last full", "last attempt")
	for _, e := range st.Endpoints {
		attempt := deref(e.LastAttemptStatus)
		if e.PendingResume {
			attempt += " (resumes)"
		}
		if e.LastAttemptError != nil {
			attempt += ": " + *e.LastAttemptError
		}
		fmt.Fprintf(stdout, "%-16s %9d %8d  %-24s %-24s %s\n", e.Endpoint, e.ActiveRecords, e.DeletedRecords,
			deref(e.LastSuccessAt), deref(e.LastFullSuccessAt), attempt)
	}
	return exitOK
}

func cmdCheck(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "config file")
	maxAge := fs.String("max-age", "", "also require each endpoint's last success to be this recent (e.g. 26h)")
	maxFullAge := fs.String("max-full-age", "", "also require each endpoint's last full success to be this recent (e.g. 15d)")
	asJSON := fs.Bool("json", false, "print JSON")
	if fs.Parse(args) != nil {
		return exitUsage
	}
	var ages status.Ages
	for _, a := range []struct {
		flag, value string
		into        *time.Duration
	}{{"--max-age", *maxAge, &ages.Any}, {"--max-full-age", *maxFullAge, &ages.Full}} {
		if a.value == "" {
			continue
		}
		var err error
		if *a.into, err = status.ParseDuration(a.value); err != nil {
			return fail(stderr, exitUsage, fmt.Errorf("%s %q: %w", a.flag, a.value, err))
		}
	}
	cfg, err := config.Load(config.Resolve(*configPath))
	if err != nil {
		return fail(stderr, exitUsage, err)
	}
	h, err := status.Check(cfg, ages, time.Now())
	if err != nil {
		return fail(stderr, exitFailed, err)
	}
	if *asJSON {
		printJSON(stdout, h)
	} else if h.Healthy {
		fmt.Fprintln(stdout, "healthy")
	} else {
		for _, p := range h.Problems {
			fmt.Fprintln(stdout, "unhealthy:", p)
		}
	}
	if !h.Healthy {
		return exitFailed
	}
	return exitOK
}

func printJSON(w io.Writer, v any) int {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return exitFailed
	}
	return exitOK
}

func fail(stderr io.Writer, code int, err error) int {
	fmt.Fprintln(stderr, "centric-fetch:", err)
	return code
}

func deref(s *string) string {
	if s == nil {
		return "-"
	}
	return *s
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func signalName(s os.Signal) string {
	switch s {
	case syscall.SIGTERM:
		return "sigterm"
	case syscall.SIGINT:
		return "sigint"
	case syscall.SIGHUP:
		return "sighup"
	}
	return s.String()
}
