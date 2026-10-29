# Design: scoped fetches

Status: **milestone 1 implemented** (silent changes, windows per mode, `--wait` and exit 75,
`check --max-full-age`); scopes themselves (milestones 2–4) are design only.

## Why

centric-fetch has two kinds of fetch:

- **incremental** reads what changed since the last run, by `_modified_at`. It's cheap, so it
  can run often, but it can't see two things: records deleted in Centric, and records changed
  without `_modified_at` moving (integrations and bulk updates do this).
- **full** reads everything and flags what's gone. It sees both, but on a large instance it
  reads hundreds of thousands of records, so it runs rarely.

For production use that leaves a gap: data that must be fresh within the hour, and silent
changes that shouldn't wait a week. A **scoped** fetch fills it: a nightly re-read of only
the records that matter (current revisions, active styles in live seasons), defined per
company, so it costs a fraction of a full fetch. A typical schedule:

| Fetch | When | Reads | Sees silent changes | Flags deletions | Moves the incremental cursor |
|---|---|---|---|---|---|
| incremental | hourly | changed since the last run | no | no | yes |
| scoped | nightly | the scope, in full | in the scope | no | no |
| full | weekly or fortnightly | everything | yes | yes | yes |

Scopes are optional and off by default: without a `scopes` block, `fetch --scoped` is a config
error and nothing else changes. What's "relevant" differs between companies, so centric-fetch
ships the mechanism and the generic Centric relationships, never a company's choices.

## Principles

1. **A scope is an optimisation, not a boundary of correctness.** Incremental fetches keep
   everything fresh and full fetches keep everything complete; a scoped fetch only re-reads
   part of the data sooner. So a scope may be wider than intended (wasted requests) or
   narrower (the next full fetch catches up), and neither loses data. Every rule below that
   has to choose between the two picks **wider**.
2. **A scoped fetch never flags deletions and never moves the incremental cursor.** It sees
   only part of an endpoint, so "not seen" means nothing, and it says nothing about records
   outside the scope. Deletions remain the full fetch's job.
3. **Records are filtered in Centric, never after fetching.** Throwing away fetched records
   saves nothing: requests are the cost, not storage. Stored data may decide *what to ask*
   (sets, below), but a rule that can't become a query parameter isn't a scope rule.
4. **Never slower than plain.** Before reading, the planner counts what each strategy would
   read (counts are cheap) and takes the cheapest, including "no scope". A scope can't make a
   fetch cost noticeably more than reading the endpoint without one: the planning counts are
   in the estimate, and they're reused as the read's proof counts, not sent twice.
5. **Provable like everything else.** Every query, and every batch of one, is paged and proven
   against Centric's count with exactly the same parameters, as windows are today.
6. **Generic links only.** The relationships centric-fetch knows are Centric's own fields
   (below). Customer fields (`x_*`, company prefixes) aren't built in; a company may still use
   any field in its own config, at its own risk.

## Configuration

```json
{
  "endpoints": ["seasons", "styles", "colorways", "bom_masters", "boms", "bom_lines"],
  "sets": {
    "live_seasons": {"endpoint": "seasons", "where": {"status": {"not_in": ["Archived", "Cancelled"]}}}
  },
  "scopes": {
    "seasons":     "all",
    "styles":      {"where": {"active": true, "parent_season": {"in": {"set": "live_seasons"}}}},
    "colorways":   {"where": {"parent_season": {"in": {"set": "live_seasons"}}}},
    "bom_masters": {"where": {"parent": {"in": {"set": "styles"}}}},
    "boms":        {"where": {"is_current": true}}
  }
}
```

- **`scopes`** maps an endpoint to `"all"` (re-read everything, flag nothing) or to a `where`.
  An endpoint without a scope is fetched incrementally in a scoped run, so a scoped run also
  keeps it fresh. Here `bom_lines` has none: line items are never scoped (see "What stays
  out").
- **`where`** is a set of conditions on the endpoint's own top-level fields, all of which must
  hold. A condition is a literal (`true`, `"DRAFT"`, `42`: equality) or an object with one
  operator:

  | Operator | Meaning | Sent to Centric as |
  |---|---|---|
  | `{"ne": v}` | not equal | `field=!v` |
  | `{"in": [a, b]}` | one of | `field=a&field=b` |
  | `{"not_in": [a, b]}` | none of | `field=!a&field=!b` |
  | `{"ge": v}`, `gt`, `le`, `lt` | compare | `field=gev` … |
  | `{"in": {"set": "name"}}` | the id of a record in the set | resolved, then `in` or `not_in` |
  | `{"in": {"set": "name", "field": "f"}}` | a value of field `f` (an id or an array of ids) of a record in the set | resolved, then `in` or `not_in` |

- **`sets`** name groups of stored records. A set's `where` uses the same language but is
  evaluated **locally**, on the stored records of its endpoint (sets are small: seasons, or
  the styles already read). A set can use another set; cycles are a config error. Sets exist
  so a scope can say "styles in a live season" without listing seasons by hand, and so one
  definition serves several scopes. **Every scope is also a set** named after its endpoint
  (`{"set": "styles"}` above: the stored styles its scope selects), so a child can follow its
  parent's scope without repeating it.

`in` against a set is the only operator that needs planning: the set is resolved to ids, then
sent as whichever is cheaper, the ids in it (`in`) or the stored ids not in it (`not_in`).
Both are correct under principle 1: a parent created since its endpoint was last read is in
neither list, so `not_in` includes its children (wider) and `in` leaves them to the next
incremental fetch.

Long lists are cut into batches (see "How a scoped fetch runs"), and only `in` can be: each
`in` batch reads a disjoint part, but a `not_in` batch excludes only its own values, so
batches would together read everything, repeatedly. `not_in`, and any literal list, must fit
in one query or it isn't a candidate. For the same reason at most one condition of a scope may
need batching; a second one is a config error, reported by `plan` and at config load where the
lists are known.

## Generic relationships

Found by matching payload values to record ids across all 42 endpoints of a production
instance, keeping only Centric's own fields. The ones a scope can use, grouped by how they
point:

**The child points to its parent** (filterable in Centric: cheap):

| Child | Field | Parent |
|---|---|---|
| styles, colorways, collections, category1s, category2s | `parent_season` | seasons |
| materials | `material_original_season` | seasons |
| colorways | `style` | styles |
| bom_masters | `parent` | styles |
| care_and_compositions | `bom` | bom_masters |
| supplier_quotes | `master` | supplier_quote_masters |
| category2s, collections, styles, colorways | `category_1`, `category_2`, `collection` | category1s, category2s, collections |

**The parent lists its children** (only resolvable as ids, `id=a&id=b`: cheap only for small sets):

| Parent | Field | Children |
|---|---|---|
| styles | `data_sheets[]` | bom_masters, size_charts, care_and_compositions, routings |
| styles | `product_colors[]`, `production_colorways[]` | colorways |
| masters (bom_masters, size_charts, supplier_quote_masters, documents, routings, care_and_compositions) | `current_revision`, `latest_revision`, `revisions[]` | their revisions |
| boms | `items[]` | bom_lines |
| size_chart_revisions | `items[]` | size_chart_dimensions |
| routing_revisions | `items[]` | routing_items |

**The record's own state** (a plain `where`): `is_current` on every revision endpoint (boms,
supplier_quotes, size_chart_revisions, document_revisions, care_and_composition_revisions,
routing_revisions); `active` on styles and materials; `state` on revisions; `status` on seasons
(its values are each company's own).

## What a scope buys

From the same instance, the share of each endpoint a typical scope keeps (current revisions;
active styles in non-archived seasons and what hangs off them):

| Endpoint | Records | Kept | Rule |
|---|---|---|---|
| styles (active) | 20k | 77% | `parent_season` in live seasons |
| colorways | 66k | 70% | `parent_season` in live seasons |
| size_chart_revisions | 37k | 43% | `is_current` |
| document_revisions | 107k | 47% | `is_current` |
| supplier_quotes | 39k | 74% | `is_current` |
| boms | 17k | 95% | `is_current` |
| bom_lines | 270k | 93% under current revisions, 64% under active styles | none: see below |
| size_chart_dimensions | 610k | 42% under current revisions | none: see below |

Two lessons. `is_current` is the best rule there is: generic, one parameter, and on some
endpoints it halves the read. And the scope of a big child is only worth having if the child
can be filtered in Centric.

## What stays out

**Line items (`bom_lines`, `size_chart_dimensions`, `routing_items`) are never scoped.** No
generic field on a line points to its revision (the parent lists the lines), so a scope would
mean looking lines up by id: 174k lines at ~200 ids per request is ~870 requests, against
~540 pages to read all of `bom_lines` at 500 per page. Lines are kept fresh by incremental fetches
and complete by full fetches. Silent-change tracking (milestone 1) will show whether that's
enough; if lines turn out to change silently under unchanged revisions, the fix is a targeted
re-read of the lines of revisions that changed, not a scope.

**Small reference data** (users, countries, sizes, lookup items, …) is cheapest read whole:
`"all"`, or no scope so it's fetched incrementally.

## How a scoped fetch runs

`centric-fetch fetch --scoped`:

1. **Order.** Endpoints run in dependency order: an endpoint whose scope uses a set waits for
   the set's endpoint (and its sets' endpoints) to finish in this run; independent endpoints
   run in parallel as today (`concurrency`). If a set's endpoint fails, its stored records are
   used when they're at most `max_set_age` old (default 26h); otherwise the condition is
   dropped (wider) and the outcome says so.
2. **Resolve.** Each scope's set conditions become id lists from the stored records, now
   refreshed. An empty `in` list means nothing is in scope: the endpoint is skipped with a
   note. An empty `not_in` list means no filter.
3. **Batch.** Value lists are cut into batches that keep the query string under a byte budget
   (`max_query_bytes`, default 6000; Centric ids are short, so ~200 values per batch), never a
   fixed count.
4. **Plan.** For every candidate (the scope with `in`, with `not_in` if it fits one query, the
   endpoint's literal conditions only, and no scope at all) the planner sends the count requests and estimates
   requests as `Σ ceil(count / limit) + batches`. The cheapest wins. A plan over
   `max_requests_per_endpoint` (default 2000) also falls back to the next cheapest, with a
   warning. The chosen plan (strategy, batches, counts) is stored with the endpoint's window.
5. **Read.** Each batch is a window: paged by `_modified_at`, proven against its own count,
   re-read up to 3 passes when records move, as delta windows are today. Its position is saved
   with every page, so a stopped scoped fetch resumes on the same batch at the same offset, with
   the same stored plan (a season changing status overnight doesn't reshuffle a half-read
   scope).
6. **Finish.** No deletions, no cursor. The endpoint records its last scoped success; the
   outcome has the plan, per-batch counts, and the usual counters.

### Windows and the lock

- Each endpoint keeps one unfinished window **per kind** (incremental, scoped, full) instead of
  one in all (done in milestone 1). Today an unfinished full window also serves an incremental
  fetch, because it covers more. With hourly fetches that would stall freshness: a resumed full
  window can't be proven, so it never moves the cursor, and a large one could take several
  runs. Milestone 2 therefore lets each kind read only its own window, which needs one more
  change first: records are stamped with the run that last saw them (`last_seen_run`), and
  that stamp is shared, so a window read in between hides records from a full window's
  deletion check (they'd be looked up by id, safely, but needlessly) and confuses a resumed
  window's proof. The stamp has to become per kind (a column per kind, or a seen table per
  open window) before kinds can interleave freely.
- `fetch --wait <duration>` waits for the lock instead of giving up, so an hourly fetch that
  starts during a nightly one runs straight after it. When the wait runs out (or without
  `--wait`), the fetch exits **75** ("busy", sysexits' `EX_TEMPFAIL`) with a summary of status
  `skipped`, instead of 2. A scheduler can count that as success (systemd:
  `SuccessExitStatus=75`).

## Visibility

- **Silent changes**, in every fetch: records whose payload changed while `_modified_at`
  didn't (in practice found by full and scoped fetches, which re-read unchanged records). The outcome gets `silent` (a count) and `silent_fields` (the
  top-level fields that changed most, with counts). This is what tells a company which
  endpoints need a scope, and it's useful on its own as a data-quality signal.
- **`centric-fetch plan [--scoped|--full] [-e name] [--json]`** makes no changes and takes no
  lock. It resolves sets from stored records, sends only count requests, and prints per
  endpoint the chosen strategy, the alternatives with their estimated requests, and the
  batches. It also checks Centric reads the filters as intended: for a set condition it
  compares Centric's count with the number of stored records that match locally, and warns on
  a large gap (for example a `field=!a&field=!b` that Centric reads as "or").
- **`status`** shows, per endpoint, the last incremental, scoped and full success. **`check`**
  takes an age per kind: `--max-age 2h --max-scoped-age 26h --max-full-age 15d` (each only
  when given; `--max-scoped-age` comes with milestone 2).

## Compatibility

- Without `scopes`, nothing changes: same requests and summary. One exit code changes: a held
  lock becomes 75 instead of 2 (2 stays for bad flags, config and credentials), in the release
  that adds `--wait`, noted in its release notes.
- The database changes are additive (new columns, a table for per-kind windows), created on
  open like today's tables. `bronze_records` doesn't change, so `schema_version` stays `1` and
  readers keep working.
- The summary gains fields (`silent`, `silent_fields`, `plan`); none change meaning.

## Milestones

1. **Groundwork** (done, v0.2.0). Silent-change tracking; per-kind windows and last
   successes; `--wait` and exit 75; `check --max-full-age`. Useful without scopes, and the silent-change
   numbers decide the scopes.
2. **Scopes with literal conditions.** Seen stamps per kind, then each kind resuming only its
   own window (see "Windows and the lock"). `scopes` with `where` of literals and operators,
   `"all"`, `fetch --scoped`, `plan`. Covers `is_current` and `active`.
3. **Sets.** Local set evaluation, `in`/`not_in` resolution, byte-budget batching, the
   cost-based planner, dependency order, `max_set_age`, the count check in `plan`.
4. **Only if milestone 1 shows a need:** id lists from a set's field (`{"set", "field"}`, e.g.
   size charts from active styles' `data_sheets`), and re-reading the line items of revisions
   that changed.

Each milestone ships with tests against the fake Centric (filters, batches, resume mid-batch,
plan choice, wider-on-failure fallbacks) and a release.

## Open questions

- Does Centric combine repeated not-equal parameters as "and" (the reading this design
  assumes)? `plan`'s count check catches it either way, but it decides whether `not_in` is
  usable.
- Do Centric's not-equal and `in` filters match records where the field is empty? It changes
  whether `not_in` includes children with no parent (it should: wider).
- The real query-string limit in front of Centric, to set `max_query_bytes`.
