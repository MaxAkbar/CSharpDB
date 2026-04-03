# Transparent Materialized Views v1

This document describes a practical first design for native materialized views in CSharpDB.
It is intentionally scoped to fit the current engine shape:

- materialized views are backed by persisted hidden tables
- the optimizer can transparently rewrite eligible queries to those stored rows
- v1 only targets projection joins
- rewrite is allowed only when the materialized view is known to be fresh

## Summary

Build materialized views as a logical metadata object on top of a hidden backing table plus normal SQL indexes.
Do not treat them as enhanced normal views.

This keeps the design aligned with what already exists in the repo:

- views currently store SQL text and are expanded at planning time
- tables and indexes already have durable storage, row maintenance, and snapshot-safe reads
- hidden underscore-prefixed tables are already compatible with the current user-visible schema surfaces

The first version should support transparent auto-rewrite for a narrow class of repeated joined-table reads.
If a materialized view is fresh, the planner may rewrite an exact matching query to use the stored backing table.
If it is stale, building, or invalid, the planner must fall back to the base tables.

## Public Interfaces

Add these SQL statements:

- `CREATE MATERIALIZED VIEW name AS SELECT ...`
- `REFRESH MATERIALIZED VIEW name`
- `DROP MATERIALIZED VIEW name`

Add a dedicated materialized-view catalog entry with at least these fields:

- logical materialized view name
- defining SQL
- normalized query fingerprint
- hidden backing table name
- dependency table names
- refresh state: `Fresh`, `Stale`, `Building`, `Invalid`

Add `sys.materialized_views`.

Extend `sys.objects` with object type `MATERIALIZED VIEW`.

Allow `CREATE INDEX` and `DROP INDEX` using the logical materialized view name.
Internally, those operations resolve to the hidden backing table, but user-facing metadata should continue to show the logical materialized view name rather than the hidden table name.

Direct reads from `SELECT ... FROM mv_name` should read the stored backing-table rows even if the materialized view is stale.
Only optimizer-driven transparent rewrite should require `Fresh`.

## Implementation Changes

Add a new materialized-view catalog instead of overloading the current view catalog.
Normal views currently only store SQL text and do not have ownership, dependency, or refresh-state metadata.

Represent each materialized view as:

- one logical metadata record
- one hidden backing table such as `_mv_<name>`
- zero or more normal SQL indexes on that backing table

On `CREATE MATERIALIZED VIEW`:

- validate that the query is inside the supported v1 subset
- infer the output schema from the query result
- create the hidden backing table
- populate it from the defining query
- record dependencies
- mark the materialized view `Fresh`

On `REFRESH MATERIALIZED VIEW`:

- rebuild the same hidden backing table inside one write transaction
- preserve existing indexes on the backing table
- mark the materialized view `Building` while refresh is in progress
- mark it `Fresh` only after successful completion
- preserve the previously committed state on rollback or refresh failure

The planner should gain two materialized-view behaviors:

- resolve `FROM mv_name` to the backing table
- optionally rewrite eligible base-table queries to the backing table when the materialized view is `Fresh`

The rewrite matcher should be strict in v1.
Only exact normalized query-shape matches should be considered eligible.
Do not attempt semantic subsumption, partial predicate matching, join reordering equivalence, or partial projection matching in the first version.

Track dependencies explicitly in the materialized-view catalog.
Base-table writes should not try to maintain materialized views incrementally in v1.
Instead, statement finalization should mark dependent materialized views `Stale` once per completed write statement.

Use the existing write finalization hooks rather than row-level trigger execution as the primary invalidation path.
That keeps invalidation cheap and avoids coupling correctness to per-row trigger behavior.

Add DDL guardrails:

- block `DROP TABLE`, destructive `ALTER TABLE`, and table rename when dependent materialized views exist
- mark dependent materialized views `Invalid` if unsupported schema drift is detected during open or planning
- disable transparent rewrite for any materialized view not in the `Fresh` state

### Supported v1 Query Shape

Transparent rewrite in v1 should only support projection joins:

- base-table joins only
- deterministic projections
- deterministic filters
- no `GROUP BY`
- no aggregates
- no `DISTINCT`
- no `ORDER BY`
- no `LIMIT` or `OFFSET`
- no subqueries
- no CTEs
- no nested views

This is the best fit for the current joined-table performance goal and keeps the first implementation tractable.

## Test Plan

Add parser and catalog tests for:

- create materialized view
- refresh materialized view
- drop materialized view
- `sys.materialized_views`
- `sys.objects` showing `MATERIALIZED VIEW`

Add correctness tests that verify:

- `SELECT ... FROM mv_name` returns stored backing-table rows
- refresh rebuilds the stored rows correctly
- writes to dependent base tables mark the materialized view `Stale`
- refresh transitions `Stale` back to `Fresh`

Add rewrite tests that verify:

- exact matching eligible queries rewrite to the materialized view when it is `Fresh`
- the same queries fall back to base tables when the materialized view is `Stale`
- `Building` and `Invalid` states also block rewrite

Add index tests that verify:

- indexes can be created through the logical materialized view name
- those indexes are used for direct reads from the materialized view
- those indexes survive refresh

Add failure and safety tests for:

- refresh rollback
- invalidation after dependent table writes
- unsupported query-shape rejection at create time
- blocking dependent-table DDL
- reopen behavior when metadata or dependencies no longer match

## Assumptions

- v1 is focused on joined-table acceleration, not grouped summary materialization
- transparent rewrite is exact-match only in the first version
- freshness in v1 comes from explicit invalidation plus `REFRESH MATERIALIZED VIEW`
- incremental maintenance on every write is deferred to a later phase
- the hidden backing table remains a real table so it can reuse the current table, index, and snapshot infrastructure
