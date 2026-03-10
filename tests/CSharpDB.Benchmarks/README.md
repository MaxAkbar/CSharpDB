# CSharpDB Benchmark Suite

Performance benchmarks for the CSharpDB embedded database engine.

The current snapshot in this README mixes the captured artifacts below with isolated macro reruns from March 10, 2026:

- `Isolated reruns on March 10, 2026: SustainedWriteBenchmark, ReaderScalingBenchmark, CollectionBenchmark, InMemoryBatchBenchmark`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/macro-20260308-012442.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/macro-20260308-015601.csv`
- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/macro-batch-memory-20260308-005708.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InMemorySqlBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InMemoryCollectionBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InMemoryAdoNetBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InMemoryPersistenceBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.CollectionPayloadBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.CollectionSchemaBreadthBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ColdLookupBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.CollectionIndexBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.StorageTuningBenchmarks-report.csv`

## Test Environment

| Component | Details |
|-----------|---------|
| CPU | Intel Core i9-11900K @ 3.50GHz, 8 cores / 16 threads |
| OS | Windows 11 (10.0.26300) |
| Runtime | .NET 10.0.3, X64 RyuJIT AVX-512F |
| Disk | NVMe SSD |
| Page Size | 4,096 bytes |
| WAL Mode | Enabled (redo-log with auto-checkpoint at 1,000 frames) |
| Page Cache | LRU page cache (in-memory) |
| WAL Index | Hash map (O(1) page lookup) |
| Benchmark Mode | Latest README snapshot mixes targeted macro captures with BenchmarkDotNet micro suites |

## Running Benchmarks

```bash
# Micro-benchmarks (BenchmarkDotNet)
dotnet run -c Release -- --micro

# Filter to a specific micro suite
dotnet run -c Release -- --micro --filter *InsertBenchmarks*
dotnet run -c Release -- --micro --filter *InMemory*

# Stable macro snapshot (median-of-3, reproducible mode)
dotnet run -c Release -- --macro --repeat 3 --repro

# Durable write variance diagnostics
dotnet run -c Release -- --write-diagnostics --repeat 3 --repro

# In-memory rotating batch throughput
dotnet run -c Release -- --macro-batch-memory

# Stress and scaling suites
dotnet run -c Release -- --stress
dotnet run -c Release -- --scaling
```

Results are written to `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/` and `BenchmarkDotNet.Artifacts/results/`.

### New In-Memory Suites

- `InMemorySqlBenchmarks`: file-backed vs in-memory SQL point lookups and inserts
- `InMemoryCollectionBenchmarks`: file-backed vs in-memory collection `GetAsync` and `PutAsync`
- `InMemoryAdoNetBenchmarks`: ADO.NET private `:memory:` vs named shared `:memory:name`
- `InMemoryPersistenceBenchmarks`: `LoadIntoMemoryAsync` and `SaveToFileAsync`
- `ColdLookupBenchmarks`: file-backed vs in-memory point lookups under cache pressure
- `CollectionIndexBenchmarks`: focused collection secondary-index lookup and indexed write-maintenance costs
- `StorageTuningBenchmarks`: cache-size, index-provider, and reader-session matrix for file-backed indexed lookups
- `DurableWriteDiagnosticsBenchmark`: file-backed single-row write diagnostics across frame-count and WAL-size checkpoint policies, including background sliced auto-checkpoint scheduling
- `InMemoryBatchBenchmark`: rotating x100 batch throughput for in-memory SQL and collections
- `InMemoryWorkloadBenchmark`: macro mixed workloads for SQL and collections in memory vs file-backed
- `SharedMemoryAdoNetBenchmark`: named shared-memory reader/writer contention through the provider host layer
- `InMemoryPersistenceBenchmark`: macro load/save latency and output-size reporting

### Baselines and Guardrails

```bash
# Capture a fresh baseline snapshot
pwsh ./tests/CSharpDB.Benchmarks/scripts/Capture-Baseline.ps1

# Run the current guardrail subset
pwsh ./tests/CSharpDB.Benchmarks/scripts/Run-Perf-Guardrails.ps1
```

Defaults:

- Baseline snapshot: `tests/CSharpDB.Benchmarks/baselines/20260302-001757`
- Threshold config: `tests/CSharpDB.Benchmarks/perf-thresholds.json`
- Last guardrail report: `tests/CSharpDB.Benchmarks/results/perf-guardrails-last.md`
- `Capture-Baseline.ps1` runs non-micro suites in reproducible mode by default and captures macro results as `--macro --repeat 3`.

## Current Performance Snapshot

### SQL API (latest macro snapshot)

| Metric | Current Result | Notes |
|--------|----------------|-------|
| Single INSERT | 27.49K ops/sec | Auto-commit durable write |
| Batch 100 rows/tx | ~704K rows/sec | 7,040.9 tx/sec x 100 rows |
| Point lookup (10K rows) | 1.33M ops/sec | `Comparison_SQL_PointLookup_10k` |
| Mixed workload reads | 56.4K ops/sec | 80/20 read/write mix |
| Mixed workload writes | 14.1K ops/sec | 80/20 read/write mix |
| Reader throughput (8 readers) | 266K ops/sec | Median of 3 isolated per-query reader-session runs, concurrent with writer |
| Writer throughput under 8 readers | 1.36K ops/sec | Same median-of-3 per-query reader-session runs as above |
| Checkpoint time (1,000 WAL frames) | 3.64 ms | Manual checkpoint |

### Collection API (latest macro snapshot)

| Metric | Current Result | Notes |
|--------|----------------|-------|
| Single Put | 32.60K ops/sec | Auto-commit durable document write |
| Batch 100 docs/tx | ~451K docs/sec | 4,505.4 tx/sec x 100 docs |
| Point Get (10K docs) | 1.58M ops/sec | Direct collection lookup |
| Mixed workload reads | 83.7K ops/sec | 80/20 read/write mix |
| Mixed workload writes | 20.9K ops/sec | 80/20 read/write mix |
| Full Scan (1K docs) | 3,058 scans/sec | Full collection scan |
| Filtered Find (1K docs, 20% match) | 2,992 scans/sec | Predicate evaluation path |
| Indexed equality lookup (10K docs) | 780.51K ops/sec | `Collection_FindByIndex_Value_10k_15s` |
| Single Put (with 1 secondary index) | 26.72K ops/sec | `Collection_Put_Single_WithIndex_15s` |

### Collection Path Micro Spot Checks

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Collection encode (direct payload) | 208 ns | 248 B | Current collection payload path |
| Collection encode (legacy row format) | 284 ns | 304 B | Prior `DbValue[]` + record serializer path |
| Collection decode (direct payload) | 356 ns | 328 B | Current collection payload path |
| Collection decode (legacy row format) | 439 ns | 600 B | Prior `DbValue[]` + record serializer path |
| Collection put (minimal schema, in-memory) | 2.28 us | 764 B | Auto-commit write with only the target collection loaded |
| Collection put (48 extra tables + 48 extra collections, in-memory) | 2.26 us | 756 B | Unrelated schema breadth no longer adds measurable write tax |

### Query Micro Spot Checks

| Metric | Mean | Allocated |
|--------|------|-----------|
| SQL PK lookup (10K rows) | 771 ns | 697 B |
| SQL PK lookup (100K rows) | 1.03 us | 696 B |
| SQL indexed lookup (100K rows) | 871 ns | 458 B |
| SQL point miss (100K rows) | 217 ns | 184 B |

### In-Memory Spot Checks

| Metric | Current Result | Notes |
|--------|----------------|-------|
| SQL insert (private engine in-memory) | 3.01 us | `Database.OpenInMemoryAsync` micro |
| Collection put (private engine in-memory) | 2.00 us | Direct payload collection write |
| SQL batch insert x100 (rotating in-memory) | ~1.70M rows/sec | Dedicated 10s run, resets the in-memory DB every 100K inserted rows |
| Collection batch put x100 (rotating in-memory) | ~1.05M docs/sec | Dedicated 10s run, resets the in-memory DB every 100K inserted docs |
| ADO.NET ExecuteScalar (`:memory:`) | 138 ns | Private connection-local in-memory DB |
| ADO.NET ExecuteScalar (`:memory:name`) | 226 ns | Named shared in-memory DB |
| ADO.NET insert (`:memory:`) | 2.16 us | Private connection-local in-memory DB |
| ADO.NET insert (`:memory:name`) | 2.18 us | Named shared in-memory DB |
| Load SQL DB + WAL into memory | 0.834 ms | `Database.LoadIntoMemoryAsync` |
| Load collection DB + WAL into memory | 1.28 ms | `Database.LoadIntoMemoryAsync` |
| Save in-memory SQL snapshot to disk | 2.84 ms | `Database.SaveToFileAsync` |
| Save in-memory collection snapshot to disk | 2.89 ms | `Database.SaveToFileAsync` |

### Cold / Cache-Pressured Lookup Spot Checks

These runs use a 200K-row working set with `MaxCachedPages = 16` and randomized lookup probes so the storage path is exercised instead of a single warmed page.

| Metric | Current Result | Notes |
|--------|----------------|-------|
| SQL cold lookup (file-backed) | 28.94 us | Cache-pressured primary-key lookup |
| SQL cold lookup (in-memory) | 2.13 us | Same workload after `LoadIntoMemoryAsync` |
| Collection cold get (file-backed) | 30.31 us | Cache-pressured direct collection lookup |
| Collection cold get (in-memory) | 2.16 us | Same workload after `LoadIntoMemoryAsync` |

### Indexed Lookup / Tuning Spot Checks

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Collection `FindByIndex` int equality (1 match) | 1.43 us | 1.21 KB | Direct integer-key index probe |
| Collection `FindByIndex` text equality (1 match) | 1.68 us | 1.27 KB | Raw-payload text verification before document materialization |
| Collection `PutAsync` with secondary indexes (insert) | 5.42 us | 25.00 KB | Transaction + rollback micro for write maintenance |
| Collection `PutAsync` with secondary indexes (update) | 16.10 us | 32.05 KB | Includes old-entry removal plus reinsert |
| Collection `DeleteAsync` with secondary indexes | 13.23 us | 24.62 KB | Transaction + rollback micro for delete-side cleanup |

### File-Backed Lookup Tuning Takeaways

- `MaxCachedPages = 2048` was the best collection setting in the new tuning matrix: indexed collection lookup fell from `117.15 us` at 16 pages to `36.90 us` at 2048 pages.
- `UseCachingBTreeIndexes` was neutral-to-negative on these lookup workloads. The worst regressions showed up on SQL reader-session paths, so it is not the recommended default tuning knob.
- Reusing a `ReaderSession` matters more than cache sizing for repeated SQL reads. At `MaxCachedPages = 2048`, per-query reader sessions measured `202.66 us`, while a reused session measured `73.64 us`.
- Recommended file-backed read-heavy preset: `builder.UseLookupOptimizedPreset()` and reuse a `ReaderSession` for bursts of related SQL reads.

### File-Backed Durable Write Tuning Takeaways

- In the March 10, 2026 `write-diagnostics` median-of-3 run after removing the retained-suffix WAL rescan, `FrameCount(4096)+Background(64 pages/step)` was the best measured write-heavy variant at `33.30K ops/sec`. `Background(256)` followed at `33.24K`, foreground `FrameCount(4096)` measured `33.13K`, and `WalSize(8 MiB)` measured `31.13K`.
- Background auto-checkpointing still does not make checkpoints cheaper. It moves them off the triggering commit. In the same median run, foreground `FrameCount(4096)` had `246` commits that paid checkpoint cost, while the `64`-page and `256`-page background variants had `0`.
- Smaller slices still reduce per-checkpoint task time but are no longer the throughput winner. At `16` pages/step, average checkpoint time fell to about `1.49 ms`, but throughput landed slightly lower at `32.99K ops/sec` because more background steps were required.
- The non-checkpoint commit path is now much cleaner than the earlier checkpoint phase. The two changes that mattered were deferring DB flushes until checkpoint completion and rebuilding retained WAL index state from the copied bytes instead of rescanning the retained suffix after compaction.
- Higher frame-count thresholds still help by making checkpoints less frequent, and background sliced scheduling helps by keeping almost all of that work off the write call that triggered it.
- Recommended file-backed write-heavy preset: `builder.UseWriteOptimizedPreset()`. This is opt-in and does not change the engine default checkpoint policy.

## Competitor Comparison

The master table below now separates CSharpDB file-backed runs from in-memory runs.

- File-backed single-write and batched-write numbers were refreshed on March 10, 2026 from isolated `SustainedWriteBenchmark` runs.
- File-backed concurrent-reader numbers were refreshed on March 10, 2026 from isolated `ReaderScalingBenchmark` runs, reported as the median of 3 reruns.
- CSharpDB SQL concurrent reads are shown as `per-query sessions / reused reader sessions (x32 reads per snapshot)` because those patterns measure materially different setup costs.
- In-memory batched-write numbers were refreshed on March 10, 2026 from isolated `InMemoryBatchBenchmark` runs and use a rotating reset-after-100K-rows harness to keep the working set bounded.
- Point-lookup numbers in the master table are cold/cache-pressured lookups from `ColdLookupBenchmarks-report.csv`.
- Hot-cache lookup numbers are still useful, but they are reported in the micro sections above instead of the master table because they collapse the storage difference once pages are warmed.
- In-memory single-write and point-lookup numbers were refreshed on March 10, 2026 from the `InMemory*Benchmarks` micro suites.
- In-memory concurrent-reader cells are left as `N/A` where an apples-to-apples dedicated benchmark has not been added yet.
- Competitor figures are still approximate ranges from published third-party sources on comparable hardware.

### Master Comparison Table

| Database | Language | Type | Single INSERT | Batched INSERT | Point Lookup | Concurrent Reads |
|----------|----------|------|---------------|----------------|--------------|------------------|
| **CSharpDB SQL (file-backed)** | **C#** | **Relational SQL** | **27.5K ops/sec** | **~704K rows/sec** | **~34.6K ops/sec** | **~266K / ~3.63M ops/sec (8r, per-query / reused x32)** |
| **CSharpDB SQL (in-memory)** | **C#** | **Relational SQL** | **~333K ops/sec** | **~1.70M rows/sec** | **~470K ops/sec** | **N/A** |
| **CSharpDB Collection (file-backed)** | **C#** | **Document (NoSQL)** | **32.6K ops/sec** | **~451K docs/sec** | **~33.0K ops/sec** | **-** |
| **CSharpDB Collection (in-memory)** | **C#** | **Document (NoSQL)** | **~500K ops/sec** | **~1.05M docs/sec** | **~463K ops/sec** | **-** |
| SQLite | C | Relational SQL | ~1-4K ops/sec | ~80-114K rows/sec | N/A | WAL lock limited |
| LiteDB | C# | Document (NoSQL) | ~1K ops/sec | ~16-21K rows/sec | N/A | N/A |
| Realm | C++ | Object DB | ~9-76K obj/sec | N/A | N/A | Multi-reader |
| UnQLite | C | Doc + KV store | ~41K (KV) / ~28K (doc) | N/A | N/A | N/A |
| H2 | Java | Relational SQL | ~3-8K ops/sec | ~2-6.5K rows/sec | N/A | Multi-threaded |
| NeDB | JavaScript | Document (JSON) | ~325 (persistent) | N/A | N/A | N/A |
| LowDB | JavaScript | JSON file | ~5-50 ops/sec | N/A | N/A | N/A |
| RocksDB | C++ | KV (LSM-tree) | ~17K ops/sec | ~1M+ rows/sec | N/A | ~713K (32 threads) |
| DuckDB | C++ | Columnar SQL (OLAP) | ~1-8K ops/sec | ~163K-1.2M rows/sec | N/A | N/A (OLAP) |
| PouchDB | JavaScript | Document + sync | ~4-6K (bulk) | ~4-6K docs/sec | N/A | N/A |
| TinyDB | Python | Document (JSON) | ~1-5K ops/sec | ~26K batch | N/A | N/A |

Hot-cache point-lookup reference for CSharpDB:

| Database | Point Lookup (hot cache) |
|----------|---------------------------|
| CSharpDB SQL (file-backed) | ~3.62M ops/sec |
| CSharpDB SQL (in-memory) | ~3.19M ops/sec |
| CSharpDB Collection (file-backed) | ~2.73M ops/sec |
| CSharpDB Collection (in-memory) | ~2.55M ops/sec |

### Sources for Competitor Numbers

| Database | Sources |
|----------|---------|
| **SQLite** | [SQLite Optimizations for Ultra High-Performance (PowerSync)](https://www.powersync.com/blog/sqlite-optimizations-for-ultra-high-performance) · [Evaluating SQLite Performance by Testing All Parameters (Eric Draken)](https://ericdraken.com/sqlite-performance-testing/) · [SQLite Performance Tuning (phiresky)](https://phiresky.github.io/blog/2020/sqlite-performance-tuning/) · [How fast is SQLite? (marending.dev)](https://marending.dev/notes/sqlite-benchmarks/) |
| **LiteDB** | [LiteDB-Benchmark (official)](https://github.com/mbdavid/LiteDB-Benchmark) · [LiteDB-Perf: INSERT/BULK compare (official)](https://github.com/mbdavid/LiteDB-Perf) · [SoloDB vs LiteDB Deep Dive (unconcurrent.com)](https://unconcurrent.com/articles/SoloDBvsLiteDB.html) |
| **Realm** | [Realm API Optimized for Performance (Realm Academy)](https://academy.realm.io/posts/realm-api-optimized-for-performance-and-low-memory-use/) · [realm-java-benchmarks (official)](https://github.com/realm/realm-java-benchmarks) · [SwiftData vs Realm Performance (Emerge Tools)](https://www.emergetools.com/blog/posts/swiftdata-vs-realm-performance-comparison) |
| **UnQLite** | [Un-scientific Benchmarks of Embedded Databases (Charles Leifer)](https://charlesleifer.com/blog/completely-un-scientific-benchmarks-of-some-embedded-databases-with-python/) · [ioarena: Embedded Storage Benchmarking Tool](https://github.com/pmwkaa/ioarena) |
| **H2** | [H2 Database Performance (official)](http://www.h2database.com/html/performance.html) · [H2 JPA Benchmark (jpab.org)](https://www.jpab.org/H2.html) · [Evaluating H2 as a Production Database (Baeldung)](https://www.baeldung.com/h2-production-database-features-limitations) |
| **RocksDB** | [Performance Benchmarks (RocksDB Wiki)](https://github.com/facebook/rocksdb/wiki/Performance-Benchmarks) · [RocksDB Benchmarks 2025 (Small Datum)](https://smalldatum.blogspot.com/2025/12/performance-for-rocksdb-98-through-1010.html) · [Benchmarking Tools - db_bench (RocksDB Wiki)](https://github.com/facebook/rocksdb/wiki/Benchmarking-tools/) |
| **DuckDB** | [Benchmarks (DuckDB Docs)](https://duckdb.org/docs/stable/guides/performance/benchmarks) · [DuckDB v1.4 LTS Benchmark Results](https://duckdb.org/2025/10/09/benchmark-results-14-lts) · [Database-like Ops Benchmark (DuckDB Labs)](https://duckdblabs.github.io/db-benchmark/) |
| **NeDB / LowDB / PouchDB / TinyDB** | [PouchDB vs NeDB comparison (GitHub)](https://github.com/pouchdb/pouchdb/issues/4031) · [npm trends: lowdb vs nedb vs pouchdb](https://npmtrends.com/lowdb-vs-nedb-vs-pouchdb) · [Flat File Database roundup (medevel.com)](https://medevel.com/flatfile-database-1721/) |

> Note: competitor figures are directional, not lab-identical. If the decision matters, run the same workload on your own hardware.

## See Also

- [Project README](../../README.md)
- [Architecture Guide](../../docs/architecture.md)
- [Roadmap](../../docs/roadmap.md)
