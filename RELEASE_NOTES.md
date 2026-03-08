# What's New

## v1.6.0 (2026-03-08)

### In-Memory Database Mode
- Added first-class in-memory engine APIs via `Database.OpenInMemoryAsync()`, `Database.LoadIntoMemoryAsync()`, and `Database.SaveToFileAsync()`.
- Added memory-backed storage and WAL implementations so SQL tables and collection data can run entirely in memory while preserving the normal on-disk database format for explicit save/load.
- Added disk-to-memory load support that imports both the main database file and committed WAL state before recovery.

### Shared In-Memory ADO.NET
- Added ADO.NET in-memory connection targets: `Data Source=:memory:` for private databases and `Data Source=:memory:<name>` for named shared databases.
- Added `Load From=...` seeding and `CSharpDbConnection.SaveToFileAsync(...)` for explicit persistence from provider-managed in-memory sessions.
- Added a shared-memory host/session layer so multiple live connections can share one named in-memory database with a single explicit transaction owner and committed-snapshot reads for other sessions.

### Collection Indexing and Storage Path Optimizations
- Reworked collections to use a direct UTF-8 payload path instead of the older `DbValue[]` row shim in the default serializer path.
- Preserved backward compatibility for legacy collection payloads and kept SQL reads over internal `_col_*` tables compatible.
- Added collection-native secondary indexes with `EnsureIndexAsync(...)` and `FindByIndexAsync(...)`, including index maintenance on `PutAsync()` and `DeleteAsync()`.
- Narrowed auto-commit collection root persistence to the touched collection and added a no-op root-persistence optimization for non-indexed steady-state writes.

### Concurrent Reader Throughput
- Reworked `ReaderSession` to reuse its snapshot pager and `QueryPlanner` across multiple reads instead of rebuilding both on every query.
- Fixed WAL snapshot synchronization so reader snapshots are taken safely under concurrent load.
- Added scaling benchmarks that separate per-query snapshot sessions from reused reader-session bursts.

### Benchmark and Documentation Coverage
- Expanded the benchmark suite with in-memory SQL, in-memory collections, in-memory ADO.NET, cold/cache-pressured lookup, rotating in-memory batch, collection payload, collection schema-breadth, and reader-scaling coverage.
- Updated the benchmark README master comparison table to separate file-backed vs in-memory results and to distinguish cold lookups from hot-cache micro numbers.
- Added a storage tutorial track under `docs/tutorials/storage` plus a runnable `StorageStudyExamples` project.

### Benchmark Highlights
- SQL in-memory single insert now measures about **~307K ops/sec**, with rotating in-memory batch insert at **~1.65M rows/sec**.
- Collection in-memory single put now measures about **~494K ops/sec**, with rotating in-memory batch put at **~1.01M docs/sec**.
- Cold/cache-pressured point lookups improved from about **~31K ops/sec** file-backed to **~400K ops/sec** in-memory for SQL, and from **~29K ops/sec** to **~375K ops/sec** for collections.
- File-backed collection single put improved to about **29.7K ops/sec** and batch put to about **~433K docs/sec** after the collection storage-path and root-persistence work.
- SQL concurrent readers at 8 readers now measure about **~236K ops/sec** with per-query snapshot sessions and **~3.69M ops/sec** when reusing reader sessions for burst reads.

### Validation and Coverage
- Added engine and ADO.NET tests for in-memory open/load/save flows, shared-memory connection behavior, collection payload compatibility, collection indexes, and WAL snapshot correctness.
- Expanded benchmark coverage for in-memory persistence, shared-memory contention, collection encode/decode cost, cache-pressured lookups, and concurrent reader scaling.

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
