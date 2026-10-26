# Working on centric-fetch

README.md says what it does and how a fetch works. Notes for changing it:

- `go test -race ./...` before every commit. The fetch tests run against an in-memory fake
  Centric (internal/fetch/fake_test.go); add a scenario there for new fetch behaviour.
- The database is a contract with its readers. Don't change existing columns or
  `schema_version`; adding tables or indexes is fine.
- Payload bytes and hashes must keep matching Python's `json.dumps(…, separators=(",", ":"),
  ensure_ascii=False)` (internal/pyjson). After touching that package, run its tests and, if
  you have one, the compat test against a real database (README, Develop). New edge cases go
  in `testdata/gen_python_cases.py`; regenerate `python_cases.json` with Python.
- Database writes happen only inside `store.DB.Tx`, never across a network call: the one
  connection is shared by all endpoints.
- Endpoints come from `internal/config/catalog.go`. A new one needs its list path, API
  version and count path.
- No output while fetching unless `-v`; the run's result is the one JSON line on stdout.
