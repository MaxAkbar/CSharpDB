# What's New

## v1.5.0 (2026-03-07)

### Insert Performance and Batching
- Added a reusable engine batch API via `Database.PrepareInsertBatch()` and `InsertBatch` for low-allocation transactional inserts.
- Added simple SQL insert fast paths for `INSERT INTO ... VALUES (...)`, including multi-row value lists.
- Deferred table and index root persistence inside explicit transactions so batched inserts do not pay that work per row.
- Added a prepared ADO.NET simple-insert path so compatible `CSharpDbCommand` executions can bypass the full SQL parser and planner.

### Storage Engine Allocation Reductions
- Added zero-copy internal lookup paths for hot read callers to avoid unnecessary payload copies on primary-key and index hits.
- Reworked slotted-page defragmentation and related B+tree rebalance paths to use span-based copies instead of per-cell heap allocations.
- Tightened split-buffer sizing during leaf splits to reduce temporary pooled-buffer pressure.

### Benchmark Highlights
- SQL batched insert throughput improved to about **605K rows/sec** in the macro benchmark median-of-3 run, up from the previously published **~294K rows/sec**.
- SQL single-row insert throughput now measures about **22.2K ops/sec** in the same macro run.
- SQL concurrent reader throughput at 8 readers now measures about **250K ops/sec**.
- Prepared ADO.NET insert batches reduce managed allocation by about **43%** for `Batch100` while improving throughput by about **11%**.

### Validation and Coverage
- Added parser, integration, and ADO.NET tests for prepared inserts, reusable insert batches, schema invalidation, and reopen persistence.
- Expanded micro and macro benchmark coverage for prepared ADO.NET insert execution and engine-level prepared insert batches.

## v1.4.0 (2026-03-06)

### SQL Completeness and Schema Metadata
- Added first-class `IDENTITY` and `AUTOINCREMENT` column modifiers for `INTEGER` primary key columns.
- Identity metadata is now persisted and surfaced across SQL/system metadata and tooling.
- `sys.columns` now includes `is_identity` (0/1).
- REST API, CLI, and MCP schema/table introspection now expose identity flags.
- Backward compatibility preserved: legacy schemas without identity flags still surface `INTEGER PRIMARY KEY` columns as identity.

### Query and Insert Path Improvements
- Expanded integer ordered-index fast path for range predicates (`<`, `<=`, `>`, `>=`) and `BETWEEN`.
- `next rowid` high-water mark is now stored in table schema metadata and reused across sessions.
- Insert rowid allocation still accepts explicit values and advances the high-water mark when explicit values are higher.

### Storage Engine
- Implemented B+tree delete rebalancing with sibling borrow/merge for underflowed leaf pages.
- Added interior-child collapse handling after delete rebalancing to keep tree structure valid.

### Saved Queries and Admin UX
- Added internal saved query catalog table (`__saved_queries`) with unique query names and UTC audit timestamps.
- Added system virtual catalog projection for saved queries via `sys.saved_queries` (and `sys_saved_queries` alias).
- Added Admin Object Explorer grouping for `System (Virtual)` items including `sys.saved_queries`.
- Added Admin Query tab `Save`/`Load` UX for named SQL snippets with refreshable saved-query list.
- Saved query catalog remains hidden from regular user table listings and table endpoints.

### Validation and Test Coverage
- Added parser/tokenizer coverage for `IDENTITY`/`AUTOINCREMENT` syntax and validation rules.
- Added integration tests for identity insert behavior and persisted next-rowid continuity.
- Added schema serializer compatibility tests for legacy metadata payloads.
- Added B+tree delete rebalancing tests to validate merge/borrow behavior.
- Added service/system/API tests for saved-query catalog behavior and virtual catalog projection.
