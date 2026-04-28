# CSharpDB Roadmap

This document outlines the planned direction for CSharpDB, organized by timeframe and priority. Items are roughly ordered by expected impact within each tier, and statuses are intended to reflect the current `v3.4.0` state of the repo.

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
| **Architecture enforcement** | `CSharpDB.Client` is now the main caller-facing interaction layer across local and remote scenarios; ADO.NET now routes ordinary direct and daemon-backed access through that layer, with only named shared in-memory provider state still retaining an internal engine dependency | Done |
| **Database administration** | Maintenance report, reindex (database/table/index/collection), VACUUM/compact, fragmentation analysis, database size report | Done |
| **Dedicated gRPC daemon** | `CSharpDB.Daemon` host plus `CSharpDB.Client` gRPC coverage for SQL, schema, procedures, collections, and maintenance | Done |
| **Storage tuning presets** | `UseLookupOptimizedPreset()` and `UseWriteOptimizedPreset()` for file-backed workloads | Done |
| **Memory-mapped main-file reads** | Opt-in mapped clean-page reads plus copy-on-write materialization for mutable access on file-backed databases | Done |
| **Background WAL checkpointing** | Incremental/sliced auto-checkpointing to move work off the triggering commit | Done |
| **SQL executor/read-path fast paths** | Compact scan and indexed-range projections, broader join lookup/covered paths, grouped/composite index aggregates, correlated subquery filter fast paths, and lower row materialization overhead | Done |
| **Table/index statistics** | ANALYZE command with persisted row counts, column NDV/min/max, stale tracking, and initial stats-guided index selection in the query planner | Done |
| **Collection binary payloads** | Binary direct-payload format with faster hydration, direct field/path extraction, and richer path-based indexing | Done |
| **Collection path indexes** | Nested scalar, array-element, nested array-object, Guid, temporal, and ordered text path indexes with `FindByPathAsync` / `FindByPathRangeAsync` | Done |
| **Hybrid storage mode** | Lazy-resident durable storage with gRPC tunable file-cache configuration; Admin direct local hosting keeps a warm in-process database instance and uses hybrid incremental-durable options by default | Done |
| **Client backup/restore** | `BackupAsync` / `RestoreAsync` as first-class `ICSharpDbClient` operations across direct, HTTP, gRPC, CLI, and Admin | Done |
| **Older DB foreign-key retrofit migration** | Validate/apply maintenance workflow that rewrites existing child tables with persisted FK metadata across direct, HTTP, gRPC, CLI, and Admin | Done |

---

## Mid-Term

SQL feature parity, provider/tooling compatibility, and ecosystem expansion.

| Feature | Description | Status |
|---------|-------------|--------|
| **User-defined functions and commands** | Trusted in-process C# scalar functions are implemented for SQL, triggers/procedures, direct clients, Admin Forms/Reports, and pipelines; trusted commands now back Admin Forms event bindings, command-button clicks, Admin Reports render lifecycle events, and pipeline run hooks; broader built-in scalar functions, native plugin extensions, aggregate/table-valued UDFs, macro action scripts, broader control events, and sandboxed UDFs remain future work | Partial |
| **Subqueries** | Scalar subqueries, `IN (SELECT ...)`, `EXISTS (SELECT ...)`, including correlated evaluation in `WHERE`, non-aggregate projection, and `UPDATE`/`DELETE` expressions | Done |
| **`UNION` / `INTERSECT` / `EXCEPT`** | Set operations across SELECT results, including use in top-level queries, views, and CTE query bodies | Done |
| **Window functions** | `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LEAD()`, `LAG()` | Planned |
| **`DEFAULT` column values** | Allow default expressions in column definitions | Planned |
| **`CHECK` constraints** | Arbitrary expression-based constraints per column or per table | Planned |
| **Foreign key constraints** | v1 support for single-column, column-level `REFERENCES` with optional `ON DELETE CASCADE`, plus `sys.foreign_keys` and metadata/tooling surfaces | Done |
| **Remote host consolidation** | `CSharpDB.Daemon` now hosts the existing REST/HTTP `/api` surface and gRPC from one long-running process backed by the same warm daemon-hosted client; standalone `CSharpDB.Api` remains supported for REST-only hosting | Done |
| **Remote host security** | Add built-in authentication, authorization, and transport-security options for remote HTTP and gRPC access, including API keys, protected admin endpoints, and TLS/mTLS deployment support | Planned |
| **Daemon service packaging** | Package the existing `CSharpDB.Daemon` host as a persistent background service across systemd, Windows Service, and launchd | Done |
| **Cross-platform deployment** | Self-contained daemon archives and install scripts ship for Windows, Linux, and macOS; dotnet tool, Docker, Homebrew, and winget distribution remain future work | In Progress |
| **NuGet package** | Publish and maintain `CSharpDB.Engine`, `CSharpDB.Data`, `CSharpDB.Client`, and `CSharpDB.Primitives` as the primary NuGet packages | Done |
| **Connection pooling** | Pool underlying direct embedded sessions behind `CSharpDbConnection` to amortize open/close cost | Done |
| **Admin dashboard improvements** | Richer SQL editor UX, query history, deeper diagnostics, and integrated Forms/Reports tooling beyond the core schema/procedure/storage surface | Done |
| **Admin Forms Access parity** | Close the highest-impact Access-style form gaps: runtime responsive layouts, full inferred validation enforcement, richer record-source/filter/sort models, Layout View, form modes, broader action/event coverage, and broader control coverage; trusted command-backed form events and command buttons are now started | Partial |
| **Admin Reports Access parity** | Close the highest-impact Access-style report gaps: bounded saved-query previews, full report rendering/export, parameter/filter prompts, richer grouping/totals options, Layout View, conditional formatting, subreports, and broader report controls; trusted command-backed report preview lifecycle events are now started | Partial |
| **Visual query designer** | Classic Admin query builder with source canvas, join editing, design grid, SQL preview, and saved designer layouts | Done |
| **ETL pipelines** | Built-in package-driven pipeline runtime with validation, dry-run, execute/resume flows, API/CLI/client coverage, run history, and Admin visual designer support | Done |
| **VS Code extension** | Schema explorer, SQL editor with IntelliSense, data browser, table designer, storage diagnostics | Done |
| **ADO.NET `GetSchema` collections** | Implement `DbConnection.GetSchema()` for standard metadata collections (MetaDataCollections, Tables, Columns, Indexes, Views, ForeignKeys) to support ORMs and tooling that discover schema through ADO.NET | Done |
| **Multilingual text support** | `BINARY`, `NOCASE`, `NOCASE_AI`, and built-in `ICU:<locale>` collation now work across SQL schema/query semantics, metadata surfaces, and collection path indexes; dedicated ordered SQL text index optimization remains planned | Done |

---

## Long-Term

Advanced features and fundamental architecture enhancements.

| Feature | Description | Status |
|---------|-------------|--------|
| **Full-text search** | Inverted index with tokenization, stemming, and relevance ranking | Done |
| **JSON path querying** | Query into JSON document fields in the Collection API (e.g., `$.address.city`) via `FindByPathAsync` / `FindByPathRangeAsync` | Done |
| **Advanced collection storage path** | Binary direct-payload format with direct binary hydration, path-based field extraction, and richer expression/path indexes | Done |
| **SQL batched row transport** | Internal row-batch transport serves as the batch-first SQL execution foundation across batch-capable result boundaries, scans, joins, and generic aggregates | Done |
| **Source-generated collection fast path** | In progress: `GetGeneratedCollectionAsync<T>(...)`, generated field descriptors/index bindings, analyzer-packaged collection model/codecs, trim/NativeAOT smoke coverage, and a dedicated sample are now in place while broader package ergonomics and remaining generator coverage continue | In Progress |
| **Page-level compression** | Compress cell content within pages to reduce I/O and storage | Planned |
| **At-rest encryption** | Encrypt database and WAL files with passphrase-based key management and explicit plaintext/encrypted migration/export paths | Research |
| **Advanced cost-based query optimizer** | In progress: phase-2 stats-guided costing is now in place through internal equi-depth histograms, heavy hitters, composite-index prefix distinct-count summaries, skew-aware lookup/filter estimates, correlation-aware composite equality filters/joins, and bounded DP reordering for small inner-join chains; adaptive re-optimization and public histogram inspection remain future work | In Progress |
| **Async I/O batching** | In progress: WAL frame-chunk writes, chunked checkpoint page copies, shared snapshot/export batching, and reusable B-tree copy utilities now cover the main storage and maintenance write paths; remaining auditing is outside the WAL hot path | In Progress |
| **Low-latency durable writes** | Done in `v2.9.0`: advisory planner-stat persistence can stay deferred without weakening committed-row durability, and `sys.table_stats.row_count_is_exact` now makes exact versus estimated row-count semantics explicit to planner and `COUNT(*)` fast paths | Done |
| **Group commit / deferred WAL flush** | Done in `v2.9.0`: opt-in `UseDurableCommitBatchWindow(...)` batches durable WAL flushes across contending in-process transactions and remains an expert measure-first knob rather than default behavior | Done |
| **Initial multi-writer support** | Explicit `WriteTransaction` conflict-detected retry flow, shared auto-commit non-insert isolation, and opt-in `ConcurrentWriteTransactions` for shared implicit inserts | Done |
| **Broader multi-writer insert optimization** | Improve hot insert fan-in, row-id reservation, and other high-contention patterns beyond the current initial multi-writer path | Research |
| **Replication / change feed** | Stream committed changes for read replicas or event-driven architectures | Research |
| **WebAssembly sandboxed UDFs** | Execute untrusted user-submitted functions in a WASM sandbox with resource limits (fuel, memory caps) via Wasmtime | Research |

---

## Current Limitations

These are known simplifications in the current implementation:

| Area | Limitation |
|------|-----------|
| **Functions and automation** | Trusted in-process C# scalar functions are supported when registered by the host; Admin Forms, Admin Reports, and pipelines can invoke trusted host commands from supported event/hook surfaces; broader built-in scalar functions, aggregate/table-valued UDFs, stored C# modules, remote delegate serialization, broader control-level form events, macro action scripts, and sandboxed UDFs are not implemented |
| **Query** | Scalar/`IN`/`EXISTS` subqueries are supported, including correlated cases in `WHERE`, non-aggregate projection, and `UPDATE`/`DELETE` expressions; correlated subqueries are not yet supported in `JOIN ON`, `GROUP BY`, `HAVING`, `ORDER BY`, or aggregate projections |
| **Query** | `UNION`, `INTERSECT`, and `EXCEPT` are supported; `UNION ALL` is not implemented yet |
| **Query** | No window functions |
| **Schema** | No SQL `DEFAULT` column values or `CHECK` constraints yet. Foreign keys are currently v1 only: single-column, column-level `REFERENCES` with optional `ON DELETE CASCADE`; table-level/composite/deferred foreign keys and `ON UPDATE` actions are not implemented |
| **Indexes** | Equality lookups support current `INTEGER`/`TEXT` indexes, but ordered range-scan pushdown is still limited to single-column `INTEGER` index paths |
| **RowId** | Legacy table schemas without persisted high-water metadata may pay a one-time key scan on first insert |
| **Collections** | `FindByIndexAsync` supports declared field-equality lookups; `FindByPathAsync` and `FindByPathRangeAsync` support path-based queries on indexed paths; `FindAsync` remains a full scan for unindexed predicates |
| **Networking** | `CSharpDB.Daemon` now hosts both REST and gRPC from one process; named pipes remain reserved but are not implemented end to end today |
| **Security** | Remote HTTP and gRPC deployment still rely on external network controls or front-end TLS termination; built-in authentication, authorization, and TLS/mTLS support are still planned |
| **Admin Forms** | The Forms designer/runtime supports the core generated-form and data-entry path plus initial trusted command-backed automation, but still needs Access-parity work for responsive runtime rendering, complete inferred validation, richer form modes, a broader action model and control events, advanced filtering/sorting, and broader controls |
| **Admin Reports** | The Reports designer/runtime supports the core banded preview path plus trusted command-backed preview lifecycle events, but still needs Access-parity work for bounded saved-query previews, full report output/export, parameters, richer grouping and totals semantics, conditional formatting, subreports, and broader controls |
| **Text / Multilingual** | Text is stored as UTF-8 and supports all Unicode languages; default semantics remain ordinal, but opt-in `BINARY`, `NOCASE`, `NOCASE_AI`, and `ICU:<locale>` collation are implemented for SQL and collection indexes. Dedicated ordered SQL text index optimization remains planned |
| **Concurrency** | The physical WAL commit path is still serialized at the storage boundary. Initial multi-writer support is shipped, but observed gains still depend on conflict shape and whether shared auto-commit `INSERT` is left on the default serialized path |
| **Storage** | No page-level compression |
| **Storage** | No at-rest encryption for database/WAL files; on-disk storage is plaintext only |
| **Storage** | Memory-mapped reads are opt-in and currently apply only to clean main-file pages; WAL-backed reads still rely on the WAL/cache path |
| **Storage** | By default, durable auto-commit single-row writes still pay a physical WAL flush per commit; opt-in `UseDurableCommitBatchWindow(...)` can trade some commit latency for higher throughput across contending in-process writers, but default behavior remains per-commit durable |
| **Query** | Phase-2 cost-based planning is largely in place: `ANALYZE`, `sys.table_stats`, `sys.column_stats`, internal histograms/heavy hitters/prefix stats, and bounded small-chain join reordering now feed join/access-path costing; remaining work is adaptive re-optimization and public histogram/diagnostic surfacing rather than missing core stats-guided costing |
| **Query** | Internal row-batch transport is now the default scan-heavy execution foundation across batch-capable scans, joins, aggregates, and result boundaries; remaining work is broader kernel specialization and optional SIMD-style tuning rather than missing core batch coverage |

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
- Phase-2 cost-based query planning: statistics-guided access-path selection, join method choice, hash build-side choice, histogram/heavy-hitter/cardinality estimation, composite-prefix correlation modeling, and bounded small-chain inner-join reordering
- SQL statement and SELECT plan caching
- First-class `IDENTITY` / `AUTOINCREMENT` support for `INTEGER PRIMARY KEY` columns
- Persisted table `NextRowId` high-water mark with compatibility fallback for legacy metadata
- Views and triggers (BEFORE/AFTER on INSERT/UPDATE/DELETE)
- Foreign key constraints: single-column, column-level `REFERENCES` with optional `ON DELETE CASCADE`
- Older-database foreign-key retrofit migration across direct, HTTP, gRPC, CLI, and Admin
- ADO.NET provider (DbConnection, DbCommand, DbDataReader, DbTransaction)
- ADO.NET `GetSchema()` metadata collections for `MetaDataCollections`, `Tables`, `Columns`, `Indexes`, `Views`, and `ForeignKeys`
- ADO.NET connection pooling with `ClearPool` / `ClearAllPools`
- In-memory database mode with explicit load-from-disk and save-to-disk APIs
- Shared/private in-memory ADO.NET connections with named shared-memory hosts
- Document Collection API (NoSQL) with typed Put/Get/Delete/Scan/Find
- Collection UTF-8 payload fast path with compatibility for legacy backing rows
- Collection secondary field indexes via `EnsureIndexAsync` / `FindByIndexAsync`
- Maintenance report, `REINDEX`, and `VACUUM` flows across client, CLI, API, and Admin UI
- Dedicated `CSharpDB.Daemon` gRPC host for remote `CSharpDB.Client` access
- Remote host consolidation in `CSharpDB.Daemon`, with REST `/api` and gRPC sharing the same warm hosted database client
- Storage tuning presets, bounded WAL read caching, memory-mapped main-file reads, and sliced background WAL auto-checkpointing
- SQL executor/read-path fast paths for compact projections, broader join/index coverage, grouped aggregates, and correlated subquery filters
- Batch-first SQL row-batch execution foundation with batch-aware scan/index/join roots, shared predicate/projection kernels, and batch-native generic aggregate paths
- Interactive CLI with meta-commands and file execution
- REST API with 34 endpoints and OpenAPI/Scalar documentation
- Blazor Server admin dashboard
- Integrated Admin Forms and Reports designers with runtime preview/entry, database-backed metadata persistence, and print-ready report output
- B+tree delete rebalancing with underflow handling (borrow/merge + interior collapse path)
- Reusable snapshot reader sessions for higher concurrent-read throughput
- Comprehensive benchmark suite (micro, macro, stress, scaling, in-memory, shared-memory)
- Binary direct-payload collection storage with direct hydration and field/path extraction
- Collection path indexes: nested scalar, array-element, nested array-object, Guid, temporal, ordered text
- Collection path query APIs: `FindByPathAsync` and `FindByPathRangeAsync`
- Source-generated typed collection fast path foundations: generated collection models/codecs/field descriptors, trim-safe `GetGeneratedCollectionAsync<T>(...)`, generator diagnostics, NativeAOT trim-smoke validation, and a dedicated sample
- Full-text search with tokenization, stemming, and relevance ranking
- Hybrid storage mode with lazy-resident durable storage and gRPC tunable file-cache
- Client-wide `BackupAsync` / `RestoreAsync` across direct, HTTP, gRPC, CLI, and Admin
- `ReplaceAsync` for index stores
- Package-driven ETL pipelines with validation, dry-run, execute/resume, persisted run history, and Admin visual designer support

---

## See Also

- [Architecture Guide](architecture.md) — How the engine is structured
- [Internals & Contributing](https://csharpdb.com/docs/internals.html) — How to extend the engine
- [Deployment & Installation Plan](deployment/README.md) — Cross-platform distribution via dotnet tool, Docker, Homebrew, winget, and install scripts
- [Multi-Writer Follow-Up Plan](multi-writer-follow-up-plan.md) — Post-initial multi-writer roadmap, insert-path gaps, and release criteria for broader completion
- [Query And Durable Write Performance Plan](query-and-durable-write-performance/README.md) — Combined optimizer phase-2 plus durable-write completion plan, shipped state, and remaining benchmark/future-work boundaries
- [Multilingual Text Support Plan](https://csharpdb.com/docs/collation-support.html) — Build on existing Unicode text storage with case-insensitive matching, locale-aware sorting, and `COLLATE` clause support for queries and index definitions
- [Database Encryption Plan](database-encryption/README.md) — Encrypted storage format, key management, migration, and managed-surface rollout
- [Storage Engine Guide](storage/README.md) — CSharpDB.Storage API reference: device, pager, B+tree, WAL, indexing, serialization, and catalog
- [Native FFI Tutorials](tutorials/native-ffi/README.md) — Python and Node.js examples using the NativeAOT shared library
- [User-Defined Functions Plan](user-defined-functions/README.md) — C# library functions callable by the database, native plugin extensions, and WASM sandboxing
- [Pub/Sub Change Events Plan](pub-sub-events/README.md) — Engine-level change events with channel-based delivery for real-time data subscriptions
- [Admin Forms Access Parity Plan](admin-forms-access-parity/README.md) — Microsoft Access parity review findings and forms roadmap
- [Admin Reports Access Parity Plan](admin-reports-access-parity/README.md) — Microsoft Access parity review findings and reports roadmap
- [Benchmark Suite](../tests/CSharpDB.Benchmarks/README.md) — Performance data informing optimization priorities
