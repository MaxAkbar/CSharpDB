# CSharpDB Roadmap

This document outlines the planned direction for CSharpDB, organized by timeframe and priority. Items are roughly ordered by expected impact within each tier, and statuses are intended to reflect the current `v2.4.0` state of the repo.

---

## Near-Term

Recently completed improvements to query performance, storage/runtime behavior, maintenance workflows, and developer ergonomics.

| Feature | Description | Status |
|---------|-------------|--------|
| **`DISTINCT` keyword** | Deduplicate rows in SELECT output | Done |
| **Composite indexes** | Multi-column indexes for covering more query patterns | Done |
| **Index range scans** | Use indexes for `<`, `>`, `<=`, `>=`, `BETWEEN` — not just equality | Done |
| **Prepared statement cache** | Cache parsed ASTs and query plans to avoid re-parsing identical SQL | Done |
| **Cached max rowid** | Avoid repeated O(n) scans when generating row IDs on insert (in-memory + persisted high-water mark) | Done |
| **B+tree delete rebalancing** | Merge underflowed pages on delete to reclaim space | Done |
| **In-memory database mode** | Open a database fully in memory, load a disk database into memory, and save a committed snapshot back to disk | Done |
| **Shared in-memory ADO.NET mode** | Support `Data Source=:memory:` and named shared in-memory databases with explicit save/load | Done |
| **Collection field indexes** | Equality-based secondary indexes for `Collection<T>` via `EnsureIndexAsync` / `FindByIndexAsync` | Done |
| **Reader session reuse** | Reuse snapshot pager and query planner inside `ReaderSession` for burst concurrent reads | Done |
| **Architecture enforcement** | `CSharpDB.Client` is now the main caller-facing interaction layer across local and remote scenarios, with direct engine-backed transport intentionally retained for in-process access | Done |
| **Database administration** | Maintenance report, reindex (database/table/index/collection), VACUUM/compact, fragmentation analysis, database size report | Done |
| **Dedicated gRPC daemon** | `CSharpDB.Daemon` host plus `CSharpDB.Client` gRPC coverage for SQL, schema, procedures, collections, and maintenance | Done |
| **Storage tuning presets** | `UseLookupOptimizedPreset()` and `UseWriteOptimizedPreset()` for file-backed workloads | Done |
| **Memory-mapped main-file reads** | Opt-in mapped clean-page reads plus copy-on-write materialization for mutable access on file-backed databases | Done |
| **Background WAL checkpointing** | Incremental/sliced auto-checkpointing to move work off the triggering commit | Done |
| **SQL executor/read-path fast paths** | Compact scan and indexed-range projections, broader join lookup/covered paths, grouped/composite index aggregates, correlated subquery filter fast paths, and lower row materialization overhead | Done |
| **Table/index statistics** | ANALYZE command with persisted row counts, column NDV/min/max, stale tracking, and initial stats-guided index selection in the query planner | Done |
| **Collection binary payloads** | Binary direct-payload format with faster hydration, direct field/path extraction, and richer path-based indexing | Done |
| **Collection path indexes** | Nested scalar, array-element, nested array-object, Guid, temporal, and ordered text path indexes with `FindByPathAsync` / `FindByPathRangeAsync` | Done |
| **Hybrid storage mode** | Lazy-resident durable storage with gRPC tunable file-cache configuration | Done |
| **Client backup/restore** | `BackupAsync` / `RestoreAsync` as first-class `ICSharpDbClient` operations across direct, HTTP, gRPC, CLI, and Admin | Done |

---

## Mid-Term

SQL feature parity, provider/tooling compatibility, and ecosystem expansion.

| Feature | Description | Status |
|---------|-------------|--------|
| **User-defined functions** | Broader built-in scalar function registry (UPPER, ABS, COALESCE, etc.), user-registered C# functions, native plugin extensions | Planned |
| **Subqueries** | Scalar subqueries, `IN (SELECT ...)`, `EXISTS (SELECT ...)`, including correlated evaluation in `WHERE`, non-aggregate projection, and `UPDATE`/`DELETE` expressions | Done |
| **`UNION` / `INTERSECT` / `EXCEPT`** | Set operations across SELECT results, including use in top-level queries, views, and CTE query bodies | Done |
| **Window functions** | `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LEAD()`, `LAG()` | Planned |
| **`DEFAULT` column values** | Allow default expressions in column definitions | Planned |
| **`CHECK` constraints** | Arbitrary expression-based constraints per column or per table | Planned |
| **Foreign key constraints** | `REFERENCES` with optional `ON DELETE CASCADE` | Planned |
| **Remote host consolidation** | Fold the current `CSharpDB.Api` REST/HTTP surface into `CSharpDB.Daemon` so one long-running server host can serve REST, gRPC, and future local transports from a shared warm `Database` instance | Planned |
| **Remote host security** | Add built-in authentication, authorization, and transport-security options for remote HTTP and gRPC access, including API keys, protected admin endpoints, and TLS/mTLS deployment support | Planned |
| **Daemon service packaging** | Package the existing `CSharpDB.Daemon` host as a persistent background service across systemd, Windows Service, and launchd | Planned |
| **Cross-platform deployment** | dotnet tool, self-contained binaries, Docker, Homebrew, winget, install scripts | Planned |
| **NuGet package** | Publish and maintain `CSharpDB.Engine`, `CSharpDB.Data`, `CSharpDB.Client`, and `CSharpDB.Primitives` as the primary NuGet packages | Done |
| **Connection pooling** | Pool `Database` instances behind `CSharpDbConnection` to amortize open/close cost | Done |
| **Admin dashboard improvements** | Richer SQL editor UX, query history, and deeper diagnostics beyond the current schema/procedure/storage tooling | Done |
| **Visual query designer** | Classic Admin query builder with source canvas, join editing, design grid, SQL preview, and saved designer layouts | Done |
| **ETL pipelines** | Built-in package-driven pipeline runtime with validation, dry-run, execute/resume flows, API/CLI/client coverage, run history, and Admin visual designer support | Done |
| **VS Code extension** | Schema explorer, SQL editor with IntelliSense, data browser, table designer, storage diagnostics | Done |
| **ADO.NET `GetSchema` collections** | Implement `DbConnection.GetSchema()` for standard metadata collections (MetaDataCollections, Tables, Columns, Indexes, Views) to support ORMs and tooling that discover schema through ADO.NET | Done |
| **Multilingual text support** | In progress: `BINARY` / `NOCASE` collation now works across SQL schema/query semantics, metadata surfaces, and collection path indexes; locale-aware ICU-style collation and ordered SQL text index optimization remain planned | In Progress |

---

## Long-Term

Advanced features and fundamental architecture enhancements.

| Feature | Description | Status |
|---------|-------------|--------|
| **Full-text search** | Inverted index with tokenization, stemming, and relevance ranking | Done |
| **JSON path querying** | Query into JSON document fields in the Collection API (e.g., `$.address.city`) via `FindByPathAsync` / `FindByPathRangeAsync` | Done |
| **Advanced collection storage path** | Binary direct-payload format with direct binary hydration, path-based field extraction, and richer expression/path indexes | Done |
| **Source-generated collection fast path** | Add a no-reflection, trim-safe typed collection API backed by generated codecs, field descriptors, and index bindings while preserving current collection payload compatibility | Planned |
| **SQL batched row transport** | Introduce true internal row-batch transport between operators as the foundation for batch-oriented/vectorized scan-heavy execution | Planned |
| **Page-level compression** | Compress cell content within pages to reduce I/O and storage | Planned |
| **At-rest encryption** | Encrypt database and WAL files with passphrase-based key management and explicit plaintext/encrypted migration/export paths | Research |
| **Advanced cost-based query optimizer** | In progress: phase-1 statistics-guided costing is already in place for non-unique lookup selection, hash-vs-lookup join choice, hash build-side choice, and limited greedy inner-join reordering for selective equality/range/`IN`/same-column `OR` filters; advanced histograms, skew/correlation modeling, and adaptive re-optimization remain future work | In Progress |
| **Async I/O batching** | In progress: WAL frame-chunk writes, chunked checkpoint page copies, and staged multi-page `AppendFrameAsync` commits now reduce syscall count on the main storage write paths; remaining batch/export paths still need auditing | In Progress |
| **Low-latency durable writes** | Reduce file-backed durable auto-commit overhead by deferring advisory planner-stat persistence and separating exact versus estimated row-count semantics, while preserving exact per-commit WAL durability for committed user data | Planned |
| **Group commit / deferred WAL flush** | Buffer committed WAL writes across transactions before flushing to improve auto-commit throughput | Planned |
| **Multi-writer support** | Allow concurrent write transactions (conflict detection + retry) | Research |
| **Replication / change feed** | Stream committed changes for read replicas or event-driven architectures | Research |
| **WebAssembly sandboxed UDFs** | Execute untrusted user-submitted functions in a WASM sandbox with resource limits (fuel, memory caps) via Wasmtime | Research |

---

## Current Limitations

These are known simplifications in the current implementation:

| Area | Limitation |
|------|-----------|
| **Functions** | Very limited scalar function surface today: built-in `TEXT(expr)` plus aggregate functions; no broader built-in function library or user-defined functions yet |
| **Query** | Scalar/`IN`/`EXISTS` subqueries are supported, including correlated cases in `WHERE`, non-aggregate projection, and `UPDATE`/`DELETE` expressions; correlated subqueries are not yet supported in `JOIN ON`, `GROUP BY`, `HAVING`, `ORDER BY`, or aggregate projections |
| **Query** | `UNION`, `INTERSECT`, and `EXCEPT` are supported; `UNION ALL` is not implemented yet |
| **Query** | No window functions |
| **Schema** | No SQL `DEFAULT` column values, `CHECK` constraints, or foreign keys |
| **Indexes** | Equality lookups support current `INTEGER`/`TEXT` indexes, but ordered range-scan pushdown is still limited to single-column `INTEGER` index paths |
| **RowId** | Legacy table schemas without persisted high-water metadata may pay a one-time key scan on first insert |
| **Collections** | `FindByIndexAsync` supports declared field-equality lookups; `FindByPathAsync` and `FindByPathRangeAsync` support path-based queries on indexed paths; `FindAsync` remains a full scan for unindexed predicates |
| **Networking** | The current shipping model still splits remote access between `CSharpDB.Api` for HTTP and `CSharpDB.Daemon` for gRPC; host consolidation plus named pipes remain planned and are not implemented yet |
| **Security** | Remote HTTP and gRPC deployment still rely on external network controls or front-end TLS termination; built-in authentication, authorization, and TLS/mTLS support are still planned |
| **Text / Multilingual** | Text is stored as UTF-8 and supports all Unicode languages; default semantics remain ordinal, but opt-in `BINARY` / `NOCASE` collation is now implemented for SQL and collection indexes. Locale-aware ICU-style collation and accent-insensitive matching remain planned |
| **Concurrency** | Single writer only (no multi-writer) |
| **Storage** | No page-level compression |
| **Storage** | No at-rest encryption for database/WAL files; on-disk storage is plaintext only |
| **Storage** | Memory-mapped reads are opt-in and currently apply only to clean main-file pages; WAL-backed reads still rely on the WAL/cache path |
| **Storage** | Durable auto-commit single-row writes still pay a physical WAL flush per commit; current write-heavy presets mainly reduce checkpoint variance rather than the flush cost itself |
| **Query** | Phase-1 cost-based planning is in place: `ANALYZE`, `sys.table_stats`, and `sys.column_stats` now feed join/access-path costing, but there are still no histograms, no adaptive re-optimization, and complex skew/correlation cases can still fall back to heuristics |

---

## Completed Milestones

Major features already implemented:

- Single-file database with 4 KB page-oriented storage
- B+tree-backed tables and secondary indexes
- Write-Ahead Log with crash recovery and auto-checkpoint
- Concurrent snapshot-isolated readers via WAL-based MVCC
- Full SQL pipeline: tokenizer, parser, query planner, operator tree
- JOINs (INNER, LEFT, RIGHT, CROSS), aggregates, GROUP BY, HAVING, CTEs
- Set operations: `UNION`, `INTERSECT`, `EXCEPT`
- `SELECT DISTINCT` and DISTINCT aggregates
- Scalar subqueries, `IN (SELECT ...)`, and `EXISTS (SELECT ...)`, including correlated evaluation in filters, non-aggregate projections, and `UPDATE`/`DELETE` expressions
- Scalar `TEXT(expr)` for filter-friendly text coercion
- Composite (multi-column) indexes
- Ordered integer index range scans (`<`, `<=`, `>`, `>=`, `BETWEEN`) in the fast lookup path
- `ANALYZE`, persisted `sys.table_stats` / `sys.column_stats`, and stale-aware column-stat refresh
- Phase-1 cost-based query planning: statistics-guided access-path selection, join method choice, hash build-side choice, and limited greedy inner-join reordering for selective filters
- SQL statement and SELECT plan caching
- First-class `IDENTITY` / `AUTOINCREMENT` support for `INTEGER PRIMARY KEY` columns
- Persisted table `NextRowId` high-water mark with compatibility fallback for legacy metadata
- Views and triggers (BEFORE/AFTER on INSERT/UPDATE/DELETE)
- ADO.NET provider (DbConnection, DbCommand, DbDataReader, DbTransaction)
- ADO.NET `GetSchema()` metadata collections for `MetaDataCollections`, `Tables`, `Columns`, `Indexes`, and `Views`
- ADO.NET connection pooling with `ClearPool` / `ClearAllPools`
- In-memory database mode with explicit load-from-disk and save-to-disk APIs
- Shared/private in-memory ADO.NET connections with named shared-memory hosts
- Document Collection API (NoSQL) with typed Put/Get/Delete/Scan/Find
- Collection UTF-8 payload fast path with compatibility for legacy backing rows
- Collection secondary field indexes via `EnsureIndexAsync` / `FindByIndexAsync`
- Maintenance report, `REINDEX`, and `VACUUM` flows across client, CLI, API, and Admin UI
- Dedicated `CSharpDB.Daemon` gRPC host for remote `CSharpDB.Client` access
- Storage tuning presets, bounded WAL read caching, memory-mapped main-file reads, and sliced background WAL auto-checkpointing
- SQL executor/read-path fast paths for compact projections, broader join/index coverage, grouped aggregates, and correlated subquery filters
- Interactive CLI with meta-commands and file execution
- REST API with 34 endpoints and OpenAPI/Scalar documentation
- Blazor Server admin dashboard
- B+tree delete rebalancing with underflow handling (borrow/merge + interior collapse path)
- Reusable snapshot reader sessions for higher concurrent-read throughput
- Comprehensive benchmark suite (micro, macro, stress, scaling, in-memory, shared-memory)
- Binary direct-payload collection storage with direct hydration and field/path extraction
- Collection path indexes: nested scalar, array-element, nested array-object, Guid, temporal, ordered text
- Collection path query APIs: `FindByPathAsync` and `FindByPathRangeAsync`
- Full-text search with tokenization, stemming, and relevance ranking
- Hybrid storage mode with lazy-resident durable storage and gRPC tunable file-cache
- Client-wide `BackupAsync` / `RestoreAsync` across direct, HTTP, gRPC, CLI, and Admin
- `ReplaceAsync` for index stores
- Package-driven ETL pipelines with validation, dry-run, execute/resume, persisted run history, and Admin visual designer support

---

## See Also

- [Architecture Guide](architecture.md) — How the engine is structured
- [Internals & Contributing](internals.md) — How to extend the engine
- [Deployment & Installation Plan](deployment/README.md) — Cross-platform distribution via dotnet tool, Docker, Homebrew, winget, and install scripts
- [SQL Batched Row Transport Design](sql-batched-row-transport/README.md) — Internal batch transport proposal for scan-heavy SQL execution, staged operator migration, and future vectorized work
- [Low-Latency Durable Writes Plan](low-latency-durable-writes/README.md) — Exact-durability auto-commit write optimization plan, advisory stats deferral, and benchmark gates
- [Multilingual Text Support Plan](collation-support/README.md) — Build on existing Unicode text storage with case-insensitive matching, locale-aware sorting, and `COLLATE` clause support for queries and index definitions
- [Database Encryption Plan](database-encryption/README.md) — Encrypted storage format, key management, migration, and managed-surface rollout
- [Storage Engine Guide](storage/README.md) — CSharpDB.Storage API reference: device, pager, B+tree, WAL, indexing, serialization, and catalog
- [Native FFI Tutorials](tutorials/native-ffi/README.md) — Python and Node.js examples using the NativeAOT shared library
- [User-Defined Functions Plan](user-defined-functions/README.md) — C# library functions callable by the database, native plugin extensions, and WASM sandboxing
- [Pub/Sub Change Events Plan](pub-sub-events/README.md) — Engine-level change events with channel-based delivery for real-time data subscriptions
- [Benchmark Suite](../tests/CSharpDB.Benchmarks/README.md) — Performance data informing optimization priorities
