# CSharpDB Benchmark Suite

Performance benchmarks for the CSharpDB embedded database engine.

The current snapshot in this README is based on:

- `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/macro-20260307-171340-median-of-3.csv`
- `tests/CSharpDB.Benchmarks/BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv`

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
| Benchmark Mode | `--repro --cpu-threads 8 --repeat 3` |

## Running Benchmarks

```bash
# Micro-benchmarks (BenchmarkDotNet)
dotnet run -c Release -- --micro

# Filter to a specific micro suite
dotnet run -c Release -- --micro --filter *InsertBenchmarks*

# Stable macro snapshot (median-of-3)
dotnet run -c Release -- --macro --repeat 3

# Stress and scaling suites
dotnet run -c Release -- --stress
dotnet run -c Release -- --scaling
```

Results are written to `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/` and `tests/CSharpDB.Benchmarks/BenchmarkDotNet.Artifacts/results/`.

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

### SQL API (macro median-of-3)

| Metric | Current Result | Notes |
|--------|----------------|-------|
| Single INSERT | 22.17K ops/sec | Auto-commit durable write |
| Batch 100 rows/tx | ~605K rows/sec | 6,049.8 tx/sec x 100 rows |
| Point lookup (10K rows) | 1.15M ops/sec | `Comparison_SQL_PointLookup_10k` |
| Mixed workload reads | 48.3K ops/sec | 80/20 read/write mix |
| Mixed workload writes | 12.1K ops/sec | 80/20 read/write mix |
| Reader throughput (8 readers) | 250K ops/sec | Concurrent with writer |
| Writer throughput under 8 readers | 7.7K ops/sec | Same run as above |
| Checkpoint time (1,000 WAL frames) | 3.69 ms | Manual checkpoint |

### Collection API (macro median-of-3)

| Metric | Current Result | Notes |
|--------|----------------|-------|
| Single Put | 25.29K ops/sec | Auto-commit durable document write |
| Batch 100 docs/tx | ~348K docs/sec | 3,482.8 tx/sec x 100 docs |
| Point Get (10K docs) | 1.35M ops/sec | Direct collection lookup |
| Mixed workload reads | 65.9K ops/sec | 80/20 read/write mix |
| Mixed workload writes | 16.5K ops/sec | 80/20 read/write mix |
| Full Scan (1K docs) | 2,358 scans/sec | Full collection scan |
| Filtered Find (1K docs, 20% match) | 2,302 scans/sec | Predicate evaluation path |

### Query Micro Spot Checks

| Metric | Mean | Allocated |
|--------|------|-----------|
| SQL PK lookup (10K rows) | 658 ns | 697 B |
| SQL PK lookup (100K rows) | 750 ns | 696 B |
| SQL indexed lookup (100K rows) | 772 ns | 458 B |
| SQL point miss (100K rows) | 244 ns | 184 B |

## Competitor Comparison

All CSharpDB numbers below are from the current benchmark snapshot above with WAL durability enabled. Competitor figures are approximate ranges from published third-party sources on comparable hardware.

### Master Comparison Table

| Database | Language | Type | Single INSERT | Batched INSERT | Point Lookup | Concurrent Reads |
|----------|----------|------|---------------|----------------|--------------|------------------|
| **CSharpDB (SQL)** | **C#** | **Relational SQL** | **22.2K ops/sec** | **~605K rows/sec** | **~1.15M ops/sec** | **250K ops/sec (8r)** |
| **CSharpDB (Collection)** | **C#** | **Document (NoSQL)** | **25.3K ops/sec** | **~348K docs/sec** | **~1.35M ops/sec** | **-** |
| SQLite | C | Relational SQL | ~1-4K ops/sec | ~80-114K rows/sec | ~275-484K ops/sec | WAL lock limited |
| LiteDB | C# | Document (NoSQL) | ~1K ops/sec | ~16-21K rows/sec | ~24K ops/sec | N/A |
| Realm | C++ | Object DB | ~9-76K obj/sec | N/A | Near-instant (zero-copy) | Multi-reader |
| UnQLite | C | Doc + KV store | ~41K (KV) / ~28K (doc) | N/A | ~60K (KV) / ~47K (doc) | N/A |
| H2 | Java | Relational SQL | ~3-8K ops/sec | ~2-6.5K rows/sec | ~50-150K ops/sec | Multi-threaded |
| NeDB | JavaScript | Document (JSON) | ~325 (persistent) | N/A | ~43K (in-memory) | N/A |
| LowDB | JavaScript | JSON file | ~5-50 ops/sec | N/A | Instant (in-memory) | N/A |
| RocksDB | C++ | KV (LSM-tree) | ~17K ops/sec | ~1M+ rows/sec | ~15-189K ops/sec | ~713K (32 threads) |
| DuckDB | C++ | Columnar SQL (OLAP) | ~1-8K ops/sec | ~163K-1.2M rows/sec | Not optimized | N/A (OLAP) |
| PouchDB | JavaScript | Document + sync | ~4-6K (bulk) | ~4-6K docs/sec | ~385 ops/sec | N/A |
| TinyDB | Python | Document (JSON) | ~1-5K ops/sec | ~26K batch | Degrades past 10K | N/A |

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
