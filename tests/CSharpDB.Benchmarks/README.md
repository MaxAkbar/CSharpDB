# CSharpDB Benchmark Suite

Performance benchmarks for the CSharpDB embedded database engine.

The current snapshot in this README is based on:

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

# Stable macro snapshot (median-of-3)
dotnet run -c Release -- --macro --repeat 3

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

## Current Performance Snapshot

### SQL API (latest macro snapshot)

| Metric | Current Result | Notes |
|--------|----------------|-------|
| Single INSERT | 23.97K ops/sec | Auto-commit durable write |
| Batch 100 rows/tx | ~648K rows/sec | 6,481.6 tx/sec x 100 rows |
| Point lookup (10K rows) | 1.20M ops/sec | `Comparison_SQL_PointLookup_10k` |
| Mixed workload reads | 56.4K ops/sec | 80/20 read/write mix |
| Mixed workload writes | 14.1K ops/sec | 80/20 read/write mix |
| Reader throughput (8 readers) | 289K ops/sec | Concurrent with writer |
| Writer throughput under 8 readers | 9.6K ops/sec | Same run as above |
| Checkpoint time (1,000 WAL frames) | 3.64 ms | Manual checkpoint |

### Collection API (latest macro snapshot)

| Metric | Current Result | Notes |
|--------|----------------|-------|
| Single Put | 29.74K ops/sec | Auto-commit durable document write |
| Batch 100 docs/tx | ~433K docs/sec | 4,330.8 tx/sec x 100 docs |
| Point Get (10K docs) | 1.58M ops/sec | Direct collection lookup |
| Mixed workload reads | 77.2K ops/sec | 80/20 read/write mix |
| Mixed workload writes | 19.3K ops/sec | 80/20 read/write mix |
| Full Scan (1K docs) | 2,944 scans/sec | Full collection scan |
| Filtered Find (1K docs, 20% match) | 2,901 scans/sec | Predicate evaluation path |
| Indexed equality lookup (10K docs) | 15.53K ops/sec | `Collection_FindByIndex_Value_10k_15s` |
| Single Put (with 1 secondary index) | 23.25K ops/sec | `Collection_Put_Single_WithIndex_15s` |

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
| SQL insert (private engine in-memory) | 3.25 us | `Database.OpenInMemoryAsync` micro |
| Collection put (private engine in-memory) | 2.02 us | Direct payload collection write |
| SQL batch insert x100 (rotating in-memory) | ~1.65M rows/sec | Dedicated 10s run, resets the in-memory DB every 100K inserted rows |
| Collection batch put x100 (rotating in-memory) | ~1.01M docs/sec | Dedicated 10s run, resets the in-memory DB every 100K inserted docs |
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
| SQL cold lookup (file-backed) | 32.23 us | Cache-pressured primary-key lookup |
| SQL cold lookup (in-memory) | 2.50 us | Same workload after `LoadIntoMemoryAsync` |
| Collection cold get (file-backed) | 34.06 us | Cache-pressured direct collection lookup |
| Collection cold get (in-memory) | 2.67 us | Same workload after `LoadIntoMemoryAsync` |

## Competitor Comparison

The master table below now separates CSharpDB file-backed runs from in-memory runs.

- File-backed single-write and batched-write numbers come from `macro-20260308-012442.csv`.
- File-backed concurrent-reader numbers come from `macro-20260308-015601.csv`.
- CSharpDB SQL concurrent reads are shown as `per-query sessions / reused reader sessions (x32 reads per snapshot)` because those patterns measure materially different setup costs.
- In-memory batched-write numbers come from `macro-batch-memory-20260308-005708.csv` and use a rotating reset-after-100K-rows harness to keep the working set bounded.
- Point-lookup numbers in the master table are cold/cache-pressured lookups from `ColdLookupBenchmarks-report.csv`.
- Hot-cache lookup numbers are still useful, but they are reported in the micro sections above instead of the master table because they collapse the storage difference once pages are warmed.
- In-memory single-write and point-lookup numbers come from the new `InMemory*Benchmarks` micro suites run on March 7, 2026.
- In-memory concurrent-reader cells are left as `N/A` where an apples-to-apples dedicated benchmark has not been added yet.
- Competitor figures are still approximate ranges from published third-party sources on comparable hardware.

### Master Comparison Table

| Database | Language | Type | Single INSERT | Batched INSERT | Point Lookup | Concurrent Reads |
|----------|----------|------|---------------|----------------|--------------|------------------|
| **CSharpDB SQL (file-backed)** | **C#** | **Relational SQL** | **24.0K ops/sec** | **~648K rows/sec** | **~31.0K ops/sec** | **~236K / ~3.69M ops/sec (8r, per-query / reused x32)** |
| **CSharpDB SQL (in-memory)** | **C#** | **Relational SQL** | **~307K ops/sec** | **~1.65M rows/sec** | **~400K ops/sec** | **N/A** |
| **CSharpDB Collection (file-backed)** | **C#** | **Document (NoSQL)** | **29.7K ops/sec** | **~433K docs/sec** | **~29.4K ops/sec** | **-** |
| **CSharpDB Collection (in-memory)** | **C#** | **Document (NoSQL)** | **~494K ops/sec** | **~1.01M docs/sec** | **~375K ops/sec** | **-** |
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
