# SQL Performance Plan

## Summary

SQL performance work should be treated as a separate track from advanced collection storage.

Collection storage is still constrained by UTF-8 JSON serialization and JSON deserialization on the hot path. SQL row storage is not. SQL rows already use a compact binary record format, selective column decoding, pre-decode filtering, and several point-lookup fast paths.

That means the collection `v3` plan is expected to materially improve document collection performance, but it should not be treated as the primary lever for SQL performance.

## Current State

Today, SQL already has several structural advantages:

- Table rows are stored in a compact typed binary row format rather than JSON text.
- The record codec can decode only selected columns instead of materializing the full row.
- The execution layer can evaluate some simple predicates before full row decode.
- Primary-key equality lookups already have dedicated fast paths.
- Projection pushdown is already present for some operators.

This is an important architectural distinction:

- Collections are still paying a JSON tax.
- SQL is mostly paying execution, allocation, index-coverage, and row-materialization costs.

## Problem Statement

If SQL is slower than desired, the bottleneck is usually not row serialization format. The more likely causes are:

- Full or partial `DbValue[]` materialization in scans, filters, joins, and aggregates.
- Reading table rows when an index already contains enough information to answer the query.
- Limited predicate pushdown into encoded row payloads.
- Generic row-by-row iterator overhead in hot loops.
- Join, sort, and aggregate paths that still materialize more data than necessary.

## Recommended Direction

The SQL plan should focus on reducing work above the row codec rather than replacing the row codec itself.

### 1. Expand covering-index execution

This is likely the highest-value SQL optimization after existing point-lookup paths.

When a query can be answered from an index alone, the engine should avoid reading the base table row entirely. That means:

- extend index planning to detect covering projections
- store or expose enough indexed column data to satisfy projection and filter requirements
- prefer index-only execution for selective lookup, range, and ordered scan workloads

Expected impact:

- large gains on lookup-heavy and read-mostly workloads
- potentially order-of-magnitude improvements when table-row fetches are eliminated

### 2. Push more predicates into encoded-row evaluation

The engine already supports simple pre-decode filters. That should be widened carefully.

Useful next steps:

- more comparison shapes on primitive columns
- more compound predicate pushdown where correctness is easy to prove
- more direct numeric and text comparisons without constructing full `DbValue` objects

Expected impact:

- moderate gains on scans
- better cache behavior and fewer temporary allocations

### 3. Reduce `DbValue[]` materialization

A large share of SQL overhead is likely in creating row arrays and `DbValue` instances that are only needed transiently.

The next step is to introduce lighter-weight row access for internal execution paths:

- row views or accessors over encoded payloads
- delayed materialization of projected columns
- specialized operator paths for common query shapes

Expected impact:

- moderate to large gains on scans, filters, and aggregates
- noticeably lower allocation pressure and GC activity

### 4. Specialize aggregates and joins

Once row access is lighter, aggregate and join operators should stop behaving like generic fully-materialized row processors when the query shape is simple.

Examples:

- integer-only SUM/COUNT/MIN/MAX fast paths
- tighter hash-join row layouts
- projection-aware join execution that does not materialize unused columns

Expected impact:

- moderate to large gains on analytical and mixed workloads

### 5. Keep storage-level work separate

Features like `mmap`, page-level compression, and at-rest encryption affect both SQL and collections, but they are lower-level storage concerns.

They should not be confused with SQL executor improvements:

- `mmap` may reduce copy and allocation overhead on cache-miss reads
- compression may reduce I/O on cold scans and checkpoints
- neither replaces the need to reduce SQL execution overhead above storage

## Non-Goals

This plan does not recommend:

- replacing SQL row storage with a document-style binary payload format
- tying SQL performance strategy to the collection `v3` breaking change
- introducing a SQL storage-format breaking change without evidence that the current binary row format is the bottleneck

## Proposed Phasing

### Phase 1

- expand covering-index planning
- widen safe pre-decode predicate pushdown
- add benchmarks that isolate row decode, predicate evaluation, and index-only reads

### Phase 2

- introduce lighter-weight internal row access
- reduce `DbValue[]` and `DbValue` creation in hot scan and lookup paths
- specialize common aggregate and lookup operators

### Phase 3

- evaluate vectorized or batch-oriented execution for scan-heavy operators
- revisit join and aggregate implementations after materialization costs are reduced
- re-measure whether storage-level changes are still the highest remaining bottleneck

The concrete design for that phase should follow the internal batch-transport model described in [SQL Batched Row Transport Design](../sql-batched-row-transport/README.md).

## Expected Performance Shape

Roughly:

- covering indexes: often the biggest SQL upside
- broader predicate pushdown: moderate gains on scan-heavy queries
- lower row materialization overhead: moderate to large gains across general SQL workloads
- collection `v3` storage changes: minimal direct benefit for normal SQL tables
- `mmap` and compression: shared storage wins, but secondary to executor improvements for hot SQL paths

## Recommended Position

The right plan is to keep collection storage and SQL performance as two separate initiatives.

- The collection plan should focus on removing JSON from collection persistence and indexing.
- The SQL plan should focus on reducing execution and materialization costs on top of the existing binary row format.

If a future benchmark shows that SQL row encoding itself is the bottleneck, that should be treated as a separate evidence-driven design change rather than assumed upfront.
