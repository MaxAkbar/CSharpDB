# What's New

## v1.8.0 (2026-03-10)

### Database Maintenance and Admin Workflows
- Added first-class maintenance APIs via `GetMaintenanceReportAsync()`, `ReindexAsync(...)`, and `VacuumAsync()` on `ICSharpDbClient` and `CSharpDbClient`.
- Added CLI maintenance commands and REPL meta commands for maintenance report, reindex, and vacuum operations.
- Added Web API maintenance endpoints and Admin storage-tab support for maintenance report, reindex, and vacuum flows.
- Reindex now supports full-database, table-scoped, index-scoped, and collection-index rebuild paths.
- Vacuum rewrites the database while preserving catalog objects, saved queries, procedures, and legacy next-row-id cases.

### Daemon and gRPC Transport
- Added the standalone `CSharpDB.Daemon` host as the dedicated gRPC surface for remote clients.
- Implemented explicit gRPC RPCs and model/value mapping for SQL execution, schema browsing, collections, procedures, maintenance, and storage inspection.
- Added gRPC client coverage for primitive rows, nested collection documents, procedure execution, and explicit RPC contract exposure.
- Added startup scripts and documentation for running the Admin UI either directly or against the daemon.

### Collection and Storage Performance
- Refined collection secondary-index reads so integer-key lookups trust the index key and text-key lookups verify raw payloads before materializing documents.
- Shared row-id payload encoding and maintenance between collection and SQL index paths through `RowIdPayloadCodec`.
- Added `UseLookupOptimizedPreset()` and `UseWriteOptimizedPreset()` for opt-in file-backed read-heavy and write-heavy storage tuning.
- Reworked WAL auto-checkpointing to support background sliced execution and incremental checkpoint progress, reducing foreground commit disruption without changing the default checkpoint policy.
- Expanded benchmark coverage with collection-index microbenchmarks, storage tuning sweeps, durable-write diagnostics, and refreshed reproducible macro capture defaults.

### Benchmark Highlights
- File-backed SQL single insert now measures about **~30.0K ops/sec**, with batched insert at **~740K rows/sec**.
- File-backed collection single put now measures about **34.6K ops/sec**, with indexed equality lookup at **~742K ops/sec** and single put with one secondary index at **~27.9K ops/sec**.
- The best measured write-heavy auto-checkpoint preset currently is `UseWriteOptimizedPreset()` at about **~31.96K ops/sec**, while keeping checkpoint work off the triggering commit.
- Cold/cache-pressured point lookups currently measure about **~36.2K ops/sec** for SQL file-backed, **~35.0K ops/sec** for collections file-backed, and about **~582K / ~554K ops/sec** in-memory for SQL and collections respectively.

### Validation and Coverage
- Added maintenance regression coverage, collection-index tests, row-id payload codec tests, WAL/background-checkpoint tests, storage extensibility tests, and daemon gRPC tests.
- Refreshed the benchmark README and reproducible baseline workflow around `--macro --repeat 3 --repro`.
