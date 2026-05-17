# Advanced Differentiators Plan

This document captures the planned design for the feature group that makes
CSharpDB feel like a SQLite, Firebase, and SQL Server hybrid: archive-backed
time travel, change tracking feeds, reactive queries, and full-text search
integration.

## Goal And Positioning

The goal is to make CSharpDB useful not only as an embedded SQL database, but as
an application data platform that can answer operational questions:

- What did this table look like at a point in time?
- What changed since my worker last checked?
- Can my UI react to database changes without polling?
- Can users search business text with database-native relevance?

The first implementation should lean on existing CSharpDB strengths. Native
table archives already provide timestamped table snapshots. WAL snapshot
readers already provide point-in-time read mechanics for active sessions.
Full-text search already has storage, maintenance, tokenization, and ranking
infrastructure. This plan connects those pieces into a clear developer-facing
surface.

## Current Foundation

CSharpDB already has several foundations for this feature group:

- Full-text indexes with tokenization, index maintenance, and ranked search
  hits.
- Native `.csdbtable` table archives with a manifest `CreatedUtc` timestamp.
- Admin Import / Export and SQL external table registration over archive files.
- WAL-backed snapshot readers for consistent point-in-time reads while writers
  continue.
- Roadmap research for replication and change feeds.

The missing work is durable historical query routing, a retained change log,
subscription delivery, and SQL ergonomics around the existing full-text engine.

## V1 SQL Contract

Archive-backed time travel:

```sql
SELECT * FROM Customers AS OF TIMESTAMP '2026-05-10T08:15:00Z';
```

Change tracking feed:

```sql
SELECT * FROM CHANGES(Customers);
```

Reactive query subscription:

```sql
SUBSCRIBE TO SELECT * FROM Orders WHERE Status = 'Open';
```

Full-text search SQL ergonomics:

```sql
SELECT * FROM Customers
WHERE MATCH(Name, Notes) AGAINST ('priority support');
```

Default semantics:

- Time travel v1 is archive-backed. `AS OF TIMESTAMP` resolves to the newest
  registered table archive at or before the requested timestamp.
- Archive manifests use the existing `CreatedUtc` timestamp. V1 does not add
  row-version chains.
- Change feed v1 is commit-log backed and begins when change tracking is
  enabled for a table.
- `CHANGES(table)` returns commit timestamp, operation, primary key or rowid,
  changed columns, and current values.
- Reactive queries are exposed first through `CSharpDB.Client` and Admin
  streaming APIs using SQL text as the subscription definition.
- `SUBSCRIBE TO SELECT ...` is the SQL contract, but v1 does not force ordinary
  `ExecuteAsync` query cursors to become long-running streams.
- Full-text search builds on existing full-text indexes and adds SQL-level
  `MATCH(...) AGAINST (...)` parsing and planning.

## Time Travel Via Timestamped Table Archives

The first time-travel implementation should use native table archives rather
than row-versioned storage. This keeps the feature aligned with the existing
export system and avoids changing the main table format before the use cases are
proven.

Recommended behavior:

- Register timestamped archives in internal metadata, keyed by source table and
  archive `CreatedUtc`.
- Resolve `AS OF TIMESTAMP` by selecting the newest archive at or before the
  requested timestamp.
- Execute the query against the resolved archive using the existing external
  table scan and archive primary-key lookup paths.
- Return a clear error when no archive exists for the requested timestamp.
- Reject `AS OF` on mutating statements in v1.

This is not true row-level temporal storage yet. It is point-in-time querying
over retained snapshots.

## Change Tracking Feed

Change tracking should use a retained commit log, not snapshot diffing. Tracking
starts only after it is enabled for a table, so v1 does not need to reconstruct
history that was never captured.

Recommended SQL shape for enabling tracking:

```sql
ALTER TABLE Customers ENABLE CHANGE TRACKING;
```

`CHANGES(Customers)` should expose a table-valued feed with at least:

| Column | Meaning |
| --- | --- |
| `commit_timestamp` | UTC commit time for the change. |
| `commit_sequence` | Monotonic sequence for stable ordering. |
| `operation` | `INSERT`, `UPDATE`, or `DELETE`. |
| `table_name` | Source table. |
| `row_id` | Internal rowid when available. |
| `primary_key` | Primary-key value when available. |
| `changed_columns` | Column names changed by the operation. |
| `current_values` | Current row values for insert/update changes. |

Deletes should include enough identity information to let consumers invalidate
or remove cached rows. Full before/after row images are future work.

## Reactive Query Subscriptions

Reactive queries should initially be a client and Admin streaming feature backed
by the same SQL text users already understand.

Recommended behavior:

- A subscription runs an initial query and emits the current result.
- Subsequent writes to referenced tables enqueue invalidation events.
- The subscription re-runs or incrementally refreshes the query based on the
  supported plan shape.
- Backpressure, cancellation, and disconnects are explicit parts of the client
  API contract.
- Admin can use the stream for live query tabs and dashboards without polling.

The SQL statement documents intent:

```sql
SUBSCRIBE TO SELECT * FROM Orders WHERE Status = 'Open';
```

In v1, this statement should map to a subscription API rather than returning a
normal finite `QueryResult` from ordinary SQL execution.

## Full-Text Search Integration

Full-text search is already a shipped storage/indexing capability. The future
work here is SQL ergonomics and integration, not rebuilding the full-text index.

Recommended behavior:

- Parse `MATCH(column, ...) AGAINST ('query')` in `WHERE`.
- Resolve the expression to an existing compatible full-text index.
- Use the existing full-text reader for candidate rowids and scores.
- Expose score ordering through a system expression or projected score alias in
  a later phase.
- Keep full-text index creation and maintenance on the existing infrastructure.

## Metadata And Execution Model

Time-travel archive metadata should be stored in internal tables and exposed via
system catalog views. The metadata should connect source table, archive path,
archive timestamp, row count, and schema fingerprint so query routing can fail
clearly when an archive no longer matches the requested table shape.

Change tracking should use append-only internal storage with retention policy
metadata. A global commit sequence gives deterministic ordering across tables,
while per-table filtering keeps `CHANGES(table)` cheap.

Reactive subscriptions should use table dependency analysis from parsed query
plans. V1 can invalidate and re-run the full query for broad correctness, then
add incremental delivery for simple primary-key and predicate shapes later.

Full-text SQL planning should be a thin adapter from SQL syntax to the existing
full-text index reader and row fetch path.

## Non-Goals

- No row-version chain storage in v1 time travel.
- No `AS OF` support for writes in v1.
- No snapshot diffing as the primary change-feed mechanism.
- No full before/after image feed for every update/delete in v1.
- No requirement that ordinary `ExecuteAsync` query results become infinite
  subscription cursors.
- No rebuild of the existing full-text indexing engine.
- No cross-database replication protocol in v1.

## Phased Implementation Plan

Phase 1: archive-backed time travel.

- Add archive history metadata and system catalog exposure.
- Add `AS OF TIMESTAMP` parser and planner support for table references.
- Route eligible reads to the selected table archive.
- Add Admin and CLI affordances for listing and registering archive snapshots.

Phase 2: commit-log change feed.

- Add table-level change tracking enablement.
- Write append-only change records during committed inserts, updates, and
  deletes for tracked tables.
- Add `CHANGES(table)` as a table-valued query source.
- Add retention and pruning controls.

Phase 3: reactive subscriptions.

- Add `CSharpDB.Client` subscription APIs and Admin live-query surfaces.
- Use parsed query dependency analysis to subscribe to referenced table changes.
- Deliver initial results, invalidation/update events, cancellation, reconnect,
  and backpressure handling.
- Keep `SUBSCRIBE TO SELECT ...` as the SQL-facing contract for subscription
  definitions.

Phase 4: full-text SQL ergonomics.

- Add parser and planner support for `MATCH(...) AGAINST (...)`.
- Resolve compatible full-text indexes and use existing full-text search hits.
- Add docs and examples that combine full-text search with reactive queries and
  change feeds.

## Future Test Plan

- `AS OF TIMESTAMP` archive selection before, at, between, and after registered
  archive timestamps.
- Missing archive behavior and schema mismatch handling.
- `CHANGES(table)` after insert, update, delete, checkpoint, reopen, and
  retention pruning.
- Change feed ordering by commit timestamp and sequence.
- Subscription initial result delivery, later change notifications,
  cancellation, reconnect, and backpressure.
- `MATCH(...) AGAINST (...)` parser coverage and equivalence to existing
  full-text search APIs.
- Client and Admin streaming API tests for reactive query delivery.
- CLI and Admin docs examples for archive registration, change feed inspection,
  and subscription demos.
