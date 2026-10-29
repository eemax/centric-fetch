# centric-fetch

Fetches [Centric PLM](https://www.centricsoftware.com/) records into a local SQLite database.
Built to run unattended on a server: one static binary (pure-Go SQLite, no cgo, no runtime
dependencies), one JSON config file, silent while it works, one JSON summary when it's done,
and an exit code that says how it went.

Every record is stored as the JSON payload Centric returned, one row per record. Fetches are
incremental, resume where they stopped, prove they're complete before moving on, and flag
records deleted in Centric instead of removing them.

## Build

Go 1.26 or newer:

```sh
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(git describe --always)" -o centric-fetch ./cmd/centric-fetch
```

The binary runs on any Linux or macOS of that architecture (`GOOS`/`GOARCH` to cross-compile).

## Configure

A JSON file, found via `--config`, then `$CENTRIC_FETCH_CONFIG`, then
`/etc/centric-fetch/config.json`. Relative paths resolve against its folder; unknown keys are
an error. See [examples/config.json](examples/config.json):

```json
{
  "database": "/var/lib/centric-fetch/centric.db",
  "env_file": "/etc/centric-fetch/centric.env",
  "endpoints": ["styles", "seasons", {"name": "boms", "limit": 200}],
  "concurrency": 3,
  "overlap_minutes": 10,
  "endpoint_retry_after_seconds": 60,
  "http": {"timeout_seconds": 90, "attempts": 6, "backoff_base_seconds": 10, "backoff_max_seconds": 120}
}
```

Only `database` is required; the other values shown are the defaults. `endpoints` defaults to
the whole catalog in [internal/config/catalog.go](internal/config/catalog.go); an entry is a
catalog name, or an object that changes its page size (`limit`).

Credentials come from `CENTRIC_BASE_URL`, `CENTRIC_USERNAME` and `CENTRIC_PASSWORD`, in the
environment or in `env_file` (`KEY=value` lines; the environment wins). `CENTRIC_BASE_URL` may
be just `acme` or `acme.centricsoftware.com`.

## Use

```sh
centric-fetch fetch                       # incremental: what changed since the last run
centric-fetch fetch --full                # everything, then flag records deleted in Centric
centric-fetch fetch --full --maintain     # ...then integrity check, VACUUM, optimize, checkpoint
centric-fetch fetch -e styles -v          # one endpoint, progress on stderr
centric-fetch fetch --wait 30m            # if another fetch is running, wait for it instead of skipping
centric-fetch status [--json]             # what's stored, what the last run did
centric-fetch check [--max-age 26h] [--max-full-age 8d] [--json]   # exit 0 healthy, 1 not
```

`fetch` exits **0** when the run is ok (or ok with warnings), **1** when it's partial or
failed, **2** on bad flags, config or credentials, **75** when it was skipped because another
fetch holds the lock (after `--wait`, if given), and **128+n** when stopped by signal n (143 for SIGTERM; a
second signal exits at once). Its stdout is one line of JSON: run id, status (`ok`, `warn`,
`partial`, `failed`, or `skipped`: same shape, nothing fetched, the reason in `error`), the
change sequence before and after, totals, and per endpoint the mode, counts (fetched,
inserted, updated, unchanged, restored, deleted, missing, silent), the fields that changed
silently, retries, attempts, notes, warnings and error.

`check` is for monitors: healthy means the latest fetch didn't fail (one in progress is
fine), and every configured endpoint has a successful fetch, its last attempt didn't fail
and, with `--max-age`, that success is recent enough, and with `--max-full-age`, so is its
last full one. It and `status` read only bookkeeping
tables and an index, so they take milliseconds on a database of any size and never block a
running fetch.

Running it on a schedule: [examples/systemd/](examples/systemd/) has units for a nightly
incremental fetch and a weekly full one.

## How a fetch works

- **Parallel endpoints.** Up to `concurrency` endpoints are fetched at once. Each stores
  every page in its own transaction; writes go through one SQLite connection.
- **Windows.** An incremental window is everything modified since the last successful
  start, less `overlap_minutes`; a full window is everything. A window counts as complete
  only when that can be proven: no record seen twice and at least Centric's count arrived.
  An unproven incremental window is read again, up to 3 passes, until a pass finds nothing
  the earlier ones missed. If it still can't be proven, its data is kept but the cursor stays,
  so the next run reads that span again.
- **Resume.** The position is saved with every page. A run that fails or is stopped leaves
  its windows pending, and the next run continues them. Each endpoint keeps one unfinished
  window per mode, so a full fetch and the incremental ones between its attempts don't undo
  each other's progress. An unfinished full window also serves an incremental run (it covers
  more), and a full window that finishes ends the unfinished incremental one (which never
  moved the cursor, so the next incremental run reads that span again).
- **Retries.** Each request is retried on timeouts, 408, 429 and 5xx with exponential
  backoff and jitter, honouring `Retry-After`. A 401 logs in again. An endpoint that still
  fails on such errors gets one more go at the end of the run, after
  `endpoint_retry_after_seconds`, resuming where it stopped.
- **Deletions.** A full fetch looks up, by id, the stored records the listing didn't return.
  Those Centric no longer has are flagged `deleted`: kept, not removed. If more than 10% of
  an endpoint's records (and more than 10) are missing from the listing, nothing is looked up
  or flagged and the run warns instead. `--allow-mass-delete` confirms.
- **No rollbacks.** A stored payload is only replaced by one with the same or a newer
  `_modified_at`, and only when its content differs.
- **Silent changes.** A record whose content changed while its `_modified_at` didn't (or
  that has none) is counted as `silent`. For the first 1,000 per endpoint and run
  (`silent_sampled`) the stored payload is compared field by field, and `silent_fields` has
  the ten top-level fields that differed most. Incremental fetches can't see such changes;
  full fetches find them, and the counts show where they happen.
- **One writer.** An `flock` on `<database>.lock`, released by the kernel however the
  process ends.

## The database

| Table | Holds |
|---|---|
| `bronze_records` | One row per record: `endpoint`, `id`, `modified_at`, `payload` (JSON), `payload_sha256`, `deleted`, `deleted_at`, `first_seen_at`, `updated_at`, `last_seen_at`, `last_seen_run`, `change_seq` |
| `meta_endpoint_state` | Per endpoint: cursor, last success (any, full, incremental) and attempt. Its `pending_*` columns are no longer written |
| `meta_windows` | Unfinished windows, one per endpoint and mode, with their position |
| `meta_runs`, `meta_run_endpoints` | Every run and what it did per endpoint |
| `meta_info` | `schema_version` (1) and the `change_seq` counter |

New tables and columns are added when a fetch opens the database; `schema_version` changes
only when existing ones would, which hasn't happened.

Read live records with `deleted = 0`. `change_seq` grows with every real change (insert,
update, restore, deletion), so a consumer that remembers the highest value it processed can
select what changed since. The database uses WAL, so readers never block a fetch.

Payloads are stored in one canonical form: compact JSON, non-ASCII characters as-is, Centric's
key order kept, numbers normalized. That is exactly what Python's
`json.dumps(value, separators=(",", ":"), ensure_ascii=False)` produces, so other tools can
re-create payloads and hashes byte for byte. `payload_sha256` is the SHA-256 of the same form
with keys sorted (`sort_keys=True`), so it changes only when the content does.

## Develop

```sh
go test -race ./...
```

To check payload encoding against an existing database (every payload re-encoded and
re-hashed):

```sh
CENTRIC_FETCH_COMPAT_DB=/path/to/centric.db go test ./internal/pyjson -run Compat -v
```

## License

MIT
