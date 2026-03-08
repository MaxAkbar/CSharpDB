# What's New

## v1.7.0 (2026-03-08)

### Unified Client and Host Migration
- Added `CSharpDB.Client` as the authoritative public database API with transport-selecting `CSharpDbClientOptions`, `ICSharpDbClient`, and DI registration helpers.
- Added direct engine-backed client execution as the default behavior, with explicit transport selection for `Direct`, `Http`, `Grpc`, `Tcp`, and named pipes; non-direct network transports are recognized for future expansion.
- Migrated the CLI, Web API, Admin UI, MCP host, and umbrella `CSharpDB` package to use the new client surface.
- Reduced `CSharpDB.Service` to a compatibility facade so hosts no longer need a separate authoritative service API.

### SQL Ownership and Tooling Consistency
- Moved SQL script splitting, statement completeness checks, and read/write classification into `CSharpDB.Sql`.
- Updated the CLI REPL and direct client execution paths to share the same trigger-aware SQL parsing behavior.
- Added transport-neutral client result shaping for schema, browsing, saved queries, procedures, diagnostics, and metadata flows used by host applications.

### Documentation and Packaging
- Updated architecture, API, MCP, service, and package documentation to reflect the client-first design.
- Added a dedicated `CSharpDB.Client` README and included the client in the `CSharpDB` metapackage.
- Documented the API Scalar UI and MCP client-target configuration for local testing and integration.
- Added `CSharpDB.Native`, a Node client package scaffold, and native FFI tutorials for JavaScript and Python consumers.

### Validation and Coverage
- Added CLI integration coverage for startup, `.info`, positional database paths, and `.read` script execution.
- Added SQL-layer tests for script splitting, trigger-aware statement handling, and statement classification.
- Added direct-client regression coverage for multi-statement execution and CLI/client file-handle reuse.

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
