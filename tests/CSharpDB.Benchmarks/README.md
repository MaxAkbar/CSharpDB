# CSharpDB Benchmark Suite

Performance benchmarks for the CSharpDB embedded database engine. Results can be used to track performance regressions, identify optimization opportunities, and compare against other embedded databases.

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
| Benchmark Mode | `--repro --cpu-threads 8 --repeat 3` (High priority, pinned to 8 logical CPUs, median-of-3 with warmup) |

## Running Benchmarks

```bash
# Micro-benchmarks (BenchmarkDotNet - statistically rigorous)
dotnet run -c Release -- --micro

# Filter to specific benchmark class
dotnet run -c Release -- --micro --filter *InsertBenchmarks*

# Macro-benchmarks (sustained workloads + latency histograms)
dotnet run -c Release -- --macro

# Stress & durability tests
dotnet run -c Release -- --stress

# Scaling experiments
dotnet run -c Release -- --scaling

# Combine non-micro suites in one invocation
dotnet run -c Release -- --macro --stress --scaling

# Repeat a suite and emit per-run CSVs plus a median-of-N CSV
dotnet run -c Release -- --macro --repeat 3

# Reproducible non-micro run: high priority + pinned CPU affinity
dotnet run -c Release -- --scaling --repro

# Repro mode with explicit CPU thread count
dotnet run -c Release -- --macro --stress --repro --cpu-threads 8

# Run everything
dotnet run -c Release -- --all
```

Results are written to CSV in `bin/Release/net10.0/results/`.
When `--repeat N` is used for macro/stress/scaling, output includes `-run1..N` files and a `-median-of-N` file.
Repeat mode also executes one unrecorded warmup iteration per suite to reduce tiered-JIT first-run bias.
`--repro` applies to non-micro suites only and attempts to set process priority to `High` and pin affinity to a stable CPU subset.

### Capture Baseline Snapshot

Use the helper script to run a repeatable subset of benchmarks and archive fresh artifacts (CSV/log + metadata) into a timestamped folder.

```bash
# Run micro + macro + stress + scaling, then snapshot outputs
pwsh ./tests/CSharpDB.Benchmarks/scripts/Capture-Baseline.ps1

# Skip the long micro run (macro/stress/scaling only)
pwsh ./tests/CSharpDB.Benchmarks/scripts/Capture-Baseline.ps1 -SkipMicro
```

Snapshots are written to `tests/CSharpDB.Benchmarks/baselines/<utc-timestamp>/`.

### Performance Guardrails

Guardrails compare current micro-benchmark results against a locked baseline snapshot and fail on regressions that exceed thresholds.
Current default guardrail subset covers `InsertBenchmarks`, `PointLookupBenchmarks`, `JoinBenchmarks`, `OrderByIndexBenchmarks`, `ScalarAggregateBenchmarks`, `ScalarAggregateLookupBenchmarks`, `DistinctBenchmarks`, `QueryPlanCacheBenchmarks`, `CompositeIndexBenchmarks`, `TextIndexBenchmarks`, and `WalBenchmarks`.

```bash
# Run the guardrail benchmark subset + baseline comparison
pwsh ./tests/CSharpDB.Benchmarks/scripts/Run-Perf-Guardrails.ps1

# Compare only (skip running benchmarks), useful after a manual benchmark run
pwsh ./tests/CSharpDB.Benchmarks/scripts/Run-Perf-Guardrails.ps1 -SkipMicroRun
```

`Compare-Baseline.ps1` automatically resolves current micro-result CSVs from both
`BenchmarkDotNet.Artifacts/results` at the repo root and
`tests/CSharpDB.Benchmarks/BenchmarkDotNet.Artifacts/results`, then compares the freshest files.

Defaults:

- Baseline snapshot: `tests/CSharpDB.Benchmarks/baselines/20260302-001757`
- Threshold config: `tests/CSharpDB.Benchmarks/perf-thresholds.json`
- Report output: `tests/CSharpDB.Benchmarks/results/perf-guardrails-last.md`
- Per-run logs: `tests/CSharpDB.Benchmarks/results/perf-guardrails-run-logs/*.log`
- Report includes a `Select Plan Cache Diagnostics` section when benchmark output emits lines like `Select plan cache stats: ...`.
- Some checks can pin their own baseline via `baselineSnapshot` in `perf-thresholds.json` (used for focused suites).

Useful overrides:

```bash
# Use a different baseline snapshot and report only (no failure)
pwsh ./tests/CSharpDB.Benchmarks/scripts/Run-Perf-Guardrails.ps1 `
  -BaselineSnapshot 20260302-001757 `
  -NoFailOnRegression
```

To refresh guardrails after intentional performance changes:

1. Capture a new baseline with `Capture-Baseline.ps1`.
2. Update `baselineSnapshot` and threshold rules in `perf-thresholds.json`.
3. Re-run `Run-Perf-Guardrails.ps1` and confirm clean pass.

---

## Benchmark Results

### Write Performance

| Operation | Throughput | P50 Latency | P99 Latency | Notes |
|-----------|-----------|-------------|-------------|-------|
| Single INSERT (auto-commit) | 21,137 ops/sec | 0.029 ms | 0.155 ms | Each op = parse + BEGIN + insert + WAL flush + COMMIT |
| Writer with 1 concurrent reader | ~23,759 ops/sec | 0.031 ms | 0.254 ms | Writer + snapshot reader scaling run |
| Batch 100 rows/tx | 2,940 tx/sec | 0.235 ms/tx | 0.794 ms/tx | ~294K rows/sec effective throughput |
| Mixed workload writes (20%) | 13,504 ops/sec | 0.026 ms | 0.140 ms | Concurrent with 80% read traffic |

### Read Performance

| Operation | Throughput | P50 Latency | P99 Latency | Dataset |
|-----------|-----------|-------------|-------------|---------|
| Point lookup by PK | 2,201,751 ops/sec | 0.000 ms | 0.001 ms | 100 rows |
| Point lookup by PK | 1,361,113 ops/sec | 0.001 ms | 0.002 ms | 1K rows |
| Point lookup by PK | 1,120,692 ops/sec | 0.001 ms | 0.002 ms | 10K rows |
| Point lookup by PK | 564,344 ops/sec | 0.001 ms | 0.022 ms | 100K rows |
| Mixed workload reads (80%) | 54,102 ops/sec | 0.002 ms | 0.038 ms | 10K row table |
| Reader session (1 reader) | 87,568 ops/sec | 0.002 ms | 0.113 ms | Concurrent with writer |
| Reader sessions (8 readers) | 218,665 ops/sec | 0.016 ms | 0.509 ms | Concurrent with writer |
| ADO.NET ExecuteReader (100 rows) | ~117,100 calls/sec | 8.54 us/call | -- | 1K row table |
| ADO.NET ExecuteScalar COUNT(*) | ~3,097,000 ops/sec | 323 ns | -- | 1K row table |
| ADO.NET Parameterized SELECT | ~15,900 ops/sec | 62.9 us | -- | 1K row table |

### Mixed Workload (80% Reads / 20% Writes)

| Component | Throughput | P50 Latency | P99 Latency |
|-----------|-----------|-------------|-------------|
| Reads | 54,102 ops/sec | 0.002 ms | 0.038 ms |
| Writes | 13,504 ops/sec | 0.026 ms | 0.140 ms |

Sustained for 15 seconds on a 10K-row table with concurrent read and write traffic.

### B+Tree Depth Scaling

| Tree Depth | Row Count | Lookups/sec | P50 Latency | P99 Latency |
|------------|-----------|-------------|-------------|-------------|
| Depth 2 | 1,600 | 1,402,645 | 0.000 ms | 0.002 ms |
| Depth 3 | 50,000 | 1,258,260 | 0.001 ms | 0.002 ms |
| Depth 3 | 100,000 | 992,606 | 0.001 ms | 0.010 ms |

### Row Count Scaling

| Row Count | Point Lookup ops/sec | Insert ops/sec | COUNT(*) scan ops/sec |
|-----------|---------------------|----------------|----------------------|
| 100 | 2,201,751 | 24,006 | 62,029 |
| 1,000 | 1,361,113 | 21,994 | 25,532 |
| 10,000 | 1,120,692 | 21,349 | 3,196 |
| 100,000 | 564,344 | 22,478 | 205 |

Insert throughput stays consistent across all table sizes (~21-24K ops/sec). Full COUNT(*) scans scale linearly as expected.

### WAL & Checkpoint Performance

| Metric | Value |
|--------|-------|
| Checkpoint time (100 WAL frames) | 4.06 ms |
| Checkpoint time (500 WAL frames) | 4.28 ms |
| Checkpoint time (1,000 WAL frames) | 3.71 ms |
| Checkpoint time (2,000 WAL frames) | 3.82 ms |
| Auto-checkpoint threshold | 1,000 frames |

Checkpoint performance is fast (~3.7-4.3ms) and consistent regardless of WAL size.

### WAL Growth Impact on Read Latency

| WAL Frames | WAL Size | Read P50 | Read P99 |
|------------|----------|----------|----------|
| 100 | 33 KB | 0.000 ms | 0.001 ms |
| 1,000 | 120 KB | 0.000 ms | 0.001 ms |
| 5,000 | 499 KB | 0.001 ms | 0.002 ms |
| 10,000 | 972 KB | 0.001 ms | 0.002 ms |
| Post-checkpoint | -- | 0.001 ms | 0.020 ms |

**Read latency is constant regardless of WAL size** thanks to the hash-map based WAL index. Page lookups in the WAL are O(1) instead of O(n).

### Concurrent Reader Scaling

| Readers | Writer ops/sec | Total Reader ops/sec | Writer P99 |
|---------|---------------|---------------------|------------|
| 0 (writer only) | 21,137 | -- | 0.155 ms |
| 1 | 23,759 | 87,568 | 0.254 ms |
| 2 | 16,664 | 94,383 | 0.440 ms |
| 4 | 10,545 | 156,584 | 0.679 ms |
| 8 | 6,253 | 218,665 | 1.087 ms |

Snapshot readers don't block the writer (WAL-based MVCC). **Total reader throughput scales to 218K ops/sec with 8 concurrent readers** while the writer maintains 6,253 ops/sec.

### Write Amplification

| Row Count | Logical Data | WAL Size (pre-ckpt) | DB Size (post-ckpt) | Amplification |
|-----------|-------------|---------------------|---------------------|---------------|
| 1,000 | 112 KB | 313 KB | 283 KB | 5.32x |
| 10,000 | 1.1 MB | 3.1 MB | 2.8 MB | 5.23x |
| 50,000 | 5.6 MB | 3.0 MB | 13.8 MB | 2.99x |

### ADO.NET Provider Performance

| Operation | Mean Latency | Memory Allocated |
|-----------|-------------|-----------------|
| Connection Open+Close | 5,570 us | 170.3 KB |
| ExecuteNonQuery (INSERT) | 46.0 us | 2.6 KB |
| ExecuteScalar (COUNT) | 323 ns | 696 B |
| ExecuteReader (100 rows) | 8.54 us | 4.9 KB |
| Parameterized SELECT | 62.9 us | 20.6 KB |
| Prepared SELECT (reused cmd) | 60.1 us | 20.3 KB |

The ADO.NET layer adds minimal overhead. Memory allocations are highly optimized, with ExecuteScalar requiring only **696 bytes** per call.

### Collection (NoSQL) API Performance

The Collection API bypasses the SQL parser/planner entirely, going directly to the B+tree. Documents are serialized as JSON via `System.Text.Json` and stored with string keys hashed to long B+tree keys.

#### Write Performance

| Operation | Throughput | P50 Latency | P99 Latency | Notes |
|-----------|-----------|-------------|-------------|-------|
| Single Put (auto-commit) | 28,428 ops/sec | 0.020 ms | 0.123 ms | JSON serialize + B+tree insert + WAL flush |
| Batch 100 Puts/tx | 3,830 tx/sec | 0.120 ms/tx | 1.040 ms/tx | ~383K docs/sec effective throughput |
| Mixed writes (20%) | 17,363 ops/sec | 0.023 ms | 0.132 ms | Concurrent with 80% read traffic |

#### Read Performance

| Operation | Throughput | P50 Latency | P99 Latency | Dataset |
|-----------|-----------|-------------|-------------|---------|
| Point Get by key | 1,226,867 ops/sec | 0.001 ms | 0.002 ms | 10K documents |
| Mixed reads (80%) | 69,418 ops/sec | 0.001 ms | 0.033 ms | 10K documents |
| Full Scan (all docs) | 2,126 scans/sec | 0.436 ms | 0.869 ms | 1K documents |
| Filtered Find (20% match) | 2,230 scans/sec | 0.413 ms | 0.800 ms | 1K documents |

#### SQL vs Collection API — Head-to-Head (same data, same DB)

| API | Point Lookup ops/sec | P50 Latency | P99 Latency | Speedup |
|-----|---------------------|-------------|-------------|---------|
| SQL (`SELECT * WHERE id = ?`) | 1,040,380 ops/sec | 0.001 ms | 0.003 ms | baseline |
| Collection (`GetAsync(key)`) | 1,203,467 ops/sec | 0.001 ms | 0.002 ms | **1.16x faster** |

The Collection API achieves **1.23M point reads/sec** — the fastest read path in CSharpDB. The 16% speedup over SQL comes from bypassing SQL parsing, query planning, and the operator pipeline. Both paths use the same underlying B+tree and page cache.

### Crash Recovery & Durability

| Metric | Value |
|--------|-------|
| Crash recovery success rate | **100%** (50/50 cycles) |
| Recovery time P50 | 12.0 ms |
| Recovery time P99 | 18.4 ms |
| Data verified after recovery | All committed rows present, uncommitted rows correctly absent |

The WAL-based crash recovery is fully reliable. Recovery time is fast and bounded by WAL replay.

---

## Comparison with 11 Embedded Databases

All CSharpDB numbers are from this benchmark suite with full WAL durability (fsync on every commit). Competitor numbers are from published third-party benchmarks on comparable hardware. Where databases have configurable durability, we note the settings used.

### Master Comparison Table

| Database | Language | Type | Single INSERT | Batched INSERT | Point Lookup | Concurrent Reads |
|----------|---------|------|--------------|----------------|-------------|-----------------|
| **CSharpDB (SQL)** | **C#** | **Relational SQL** | **21.1K ops/sec** | **~294K rows/sec** | **564K-1,361K ops/sec** | **219K ops/sec (8r)** |
| **CSharpDB (Collection)** | **C#** | **Document (NoSQL)** | **28.4K ops/sec** | **~383K docs/sec** | **1,227K ops/sec** | **—** |
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

### Head-to-Head Comparisons

#### vs SQLite — The Gold Standard (C, 20+ years)

| Metric | CSharpDB | SQLite (WAL, sync=NORMAL) | Winner |
|--------|----------|--------------------------|--------|
| Single auto-commit INSERT | 21,137 ops/sec | ~925-4,363 ops/sec | **CSharpDB 5-23x** |
| Batched INSERT (rows/sec) | ~294K rows/sec | ~80-114K rows/sec | **CSharpDB 3-4x** |
| Point lookup (cached) | ~564K-1,361K ops/sec | ~275-484K ops/sec | **CSharpDB wins at most sizes** |
| Concurrent readers (8) | 219K ops/sec | WAL lock limited | **CSharpDB** |
| Crash recovery | 100% reliable | 100% reliable | Parity |

#### vs LiteDB — Closest .NET Competitor

| Metric | CSharpDB (Collection API) | LiteDB v5 | Winner |
|--------|--------------------------|-----------|--------|
| Single document Put | 28,428 ops/sec | ~1,000 ops/sec | **CSharpDB 28x** |
| Bulk Put (100/tx) | ~383K docs/sec | ~16-21K rows/sec | **CSharpDB 18-24x** |
| Point lookup by key | 1,226,867 ops/sec | ~24K ops/sec | **CSharpDB 51x** |
| Full SQL support | Yes (dual API) | No | **CSharpDB** |
| LINQ-style filtering | FindAsync(predicate) | LINQ provider | Parity |

The Collection API makes the comparison apples-to-apples: both are document/NoSQL APIs in .NET. CSharpDB's direct B+tree path achieves **51x faster point lookups** than LiteDB.

#### vs Realm — Mobile Object DB

| Metric | CSharpDB | Realm | Winner |
|--------|----------|-------|--------|
| Insert throughput | 21,137 ops/sec | ~9-76K obj/sec | Depends on platform |
| Point lookup | ~564K-1,361K ops/sec | Near-instant (zero-copy mmap) | **Realm** for hot data |
| SQL support | Full SQL, JOINs | No SQL | **CSharpDB** |
| Modification scaling | Linear | Quadratic on large datasets | **CSharpDB** |
| WAL crash recovery | Yes, always durable | Yes | Parity |

#### vs UnQLite — C KV/Doc Store (from SQLite author)

| Metric | CSharpDB | UnQLite (native C est.) | Winner |
|--------|----------|------------------------|--------|
| KV write | 21,137 ops/sec | ~80-160K ops/sec | **UnQLite 4-8x** |
| KV read | ~564K-1,361K ops/sec | ~120-240K ops/sec | **CSharpDB wins at most sizes** |
| SQL support | Full SQL engine | None | **CSharpDB** |
| Crash recovery | 100%, WAL-based | No WAL | **CSharpDB** |

#### vs H2 — Closest Architectural Match (Java SQL)

| Metric | CSharpDB | H2 (persistent, MVStore) | Winner |
|--------|----------|--------------------------|--------|
| Auto-commit INSERT (durable) | 21,137 ops/sec | ~500-7,000 TPS | **CSharpDB 3-42x** |
| Batched INSERT | ~294K rows/sec | ~2-6.5K rows/sec | **CSharpDB 45-147x** |
| Point lookup | ~564K-1,361K ops/sec | ~50-150K ops/sec | **CSharpDB 4-27x** |
| Crash safety | Always durable | WRITE_DELAY=500ms default | **CSharpDB** |

#### vs DuckDB — Analytical SQL (C++)

| Metric | CSharpDB | DuckDB | Winner |
|--------|----------|--------|--------|
| Single row INSERT | 21,137 ops/sec | ~1-8K ops/sec | **CSharpDB** |
| Bulk INSERT | ~294K rows/sec | ~163K-1.2M rows/sec | **DuckDB** for bulk |
| Point lookup | ~564K-1,361K ops/sec | Not optimized | **CSharpDB** |
| Analytical scans | Not optimized | Excellent | **DuckDB** |

#### vs RocksDB — LSM-Tree KV Store (C++)

| Metric | CSharpDB | RocksDB (NVMe) | Winner |
|--------|----------|----------------|--------|
| Single write | 21,137 ops/sec | ~17K ops/sec | **CSharpDB 1.24x** |
| Bulk load | ~294K rows/sec | ~1M+ rows/sec | **RocksDB 3.4x** |
| Point read (multi-thread) | 219K ops/sec (8r) | ~713K (32 threads) | Comparable per-thread |
| SQL support | Full SQL | None (KV only) | **CSharpDB** |

#### vs NeDB / LowDB / PouchDB / TinyDB — Lightweight Scripting DBs

| Metric | CSharpDB | NeDB (persistent) | LowDB | PouchDB | TinyDB |
|--------|----------|-------------------|-------|---------|--------|
| Insert ops/sec | 21,137 | ~325 | ~5-50 | ~4-6K | ~1-5K |
| **CSharpDB advantage** | -- | **65x** | **423-4,227x** | **3.5-5.3x** | **4.2-21.1x** |

These lightweight databases are designed for developer convenience in scripting environments, not raw performance. CSharpDB outperforms all of them while providing features none of them have (SQL, JOINs, indexes, WAL, concurrent readers).

### Sources for Competitor Numbers

All CSharpDB numbers are from this benchmark suite. Competitor numbers are drawn from the following published benchmarks and documentation:

| Database | Sources |
|----------|---------|
| **SQLite** | [SQLite Optimizations for Ultra High-Performance (PowerSync)](https://www.powersync.com/blog/sqlite-optimizations-for-ultra-high-performance) · [Evaluating SQLite Performance by Testing All Parameters (Eric Draken)](https://ericdraken.com/sqlite-performance-testing/) · [SQLite Performance Tuning (phiresky)](https://phiresky.github.io/blog/2020/sqlite-performance-tuning/) · [How fast is SQLite? (marending.dev)](https://marending.dev/notes/sqlite-benchmarks/) |
| **LiteDB** | [LiteDB-Benchmark (official)](https://github.com/mbdavid/LiteDB-Benchmark) · [LiteDB-Perf: INSERT/BULK compare (official)](https://github.com/mbdavid/LiteDB-Perf) · [SoloDB vs LiteDB Deep Dive (unconcurrent.com)](https://unconcurrent.com/articles/SoloDBvsLiteDB.html) |
| **Realm** | [Realm API Optimized for Performance (Realm Academy)](https://academy.realm.io/posts/realm-api-optimized-for-performance-and-low-memory-use/) · [realm-java-benchmarks (official)](https://github.com/realm/realm-java-benchmarks) · [SwiftData vs Realm Performance (Emerge Tools)](https://www.emergetools.com/blog/posts/swiftdata-vs-realm-performance-comparison) |
| **UnQLite** | [Un-scientific Benchmarks of Embedded Databases (Charles Leifer)](https://charlesleifer.com/blog/completely-un-scientific-benchmarks-of-some-embedded-databases-with-python/) · [ioarena: Embedded Storage Benchmarking Tool](https://github.com/pmwkaa/ioarena) |
| **H2** | [H2 Database Performance (official)](http://www.h2database.com/html/performance.html) · [H2 JPA Benchmark (jpab.org)](https://www.jpab.org/H2.html) · [Evaluating H2 as a Production Database (Baeldung)](https://www.baeldung.com/h2-production-database-features-limitations) |
| **RocksDB** | [Performance Benchmarks (RocksDB Wiki)](https://github.com/facebook/rocksdb/wiki/Performance-Benchmarks) · [RocksDB Benchmarks 2025 (Small Datum)](https://smalldatum.blogspot.com/2025/12/performance-for-rocksdb-98-through-1010.html) · [Benchmarking Tools — db_bench (RocksDB Wiki)](https://github.com/facebook/rocksdb/wiki/Benchmarking-tools/) |
| **DuckDB** | [Benchmarks (DuckDB Docs)](https://duckdb.org/docs/stable/guides/performance/benchmarks) · [DuckDB v1.4 LTS Benchmark Results](https://duckdb.org/2025/10/09/benchmark-results-14-lts) · [Database-like Ops Benchmark (DuckDB Labs)](https://duckdblabs.github.io/db-benchmark/) |
| **NeDB / LowDB / PouchDB / TinyDB** | [PouchDB vs NeDB comparison (GitHub)](https://github.com/pouchdb/pouchdb/issues/4031) · [npm trends: lowdb vs nedb vs pouchdb](https://npmtrends.com/lowdb-vs-nedb-vs-pouchdb) · [Flat File Database roundup (medevel.com)](https://medevel.com/flatfile-database-1721/) |

> **Note:** Competitor numbers are approximate ranges drawn from multiple sources on varying hardware. They are intended for directional comparison, not exact equivalence. When in doubt, run your own benchmarks on your target hardware.

### Ranking by Category

**Durable Write Throughput** (auto-commit INSERT with crash safety):
1. **CSharpDB — 21,137 ops/sec** (full SQL, always durable)
2. RocksDB — ~17K ops/sec (KV only, configurable durability)
3. Realm — ~9-76K obj/sec (varies wildly by platform)
4. H2 — ~500-7,000 TPS (durable mode)
5. SQLite — ~1-4K ops/sec (WAL, sync=NORMAL)
6. LiteDB — ~1K ops/sec

**Concurrent Read Throughput**:
1. RocksDB — ~713K ops/sec (32 threads)
2. **CSharpDB — 219K ops/sec (8 readers)**
3. SQLite — WAL lock limited
4. H2 — multi-threaded but no published concurrent read numbers

**Batched Insert Throughput**:
1. RocksDB — ~1M+ rows/sec (bulk load)
2. DuckDB — ~163K-1.2M rows/sec (Arrow optimized)
3. **CSharpDB — ~294K rows/sec**
4. SQLite — ~80-114K rows/sec

**Point Lookup Throughput** (single-thread, cached):
1. **CSharpDB (SQL API) — 2,202K ops/sec** (100-row dataset)
2. SQLite — ~275-484K ops/sec
3. **CSharpDB (Collection API) — 1,227K ops/sec** (10K-doc dataset)
4. **CSharpDB (SQL API) — 564K-1,361K ops/sec** (1K-100K rows)
5. H2 — ~50-150K ops/sec (estimated)
6. UnQLite — ~60K ops/sec (KV API)
7. LiteDB — ~24K ops/sec

---

## Performance Improvement History

### Run 9 (Latest) — Reproducible Benchmark Mode + Storage Improvements

Run 9 introduces the reproducible benchmark mode (`--repro --cpu-threads 8 --repeat 5`) with CPU affinity pinning and median-of-5 statistical aggregation. All numbers are now measured with High process priority, pinned to 8 logical CPUs, with 1 warmup pass + 5 recorded iterations taking the median. This branch also includes storage-layer improvements on `Improve_CSharpDB_Storage`.

| Metric | Run 8 | Run 9 (repro median-of-5) | Change |
|--------|-------|--------------------------|--------|
| Single INSERT (auto-commit) | 10,611 ops/sec | 18,005 ops/sec | **+70%** |
| Batch 100 rows/tx | ~247K rows/sec | ~234K rows/sec | -5% |
| Point lookup (100 rows) | — | 1,229,494 ops/sec | — |
| Point lookup (1K rows) | 217,694 ops/sec | 727,502 ops/sec | **+234%** |
| Point lookup (10K rows) | 1,084,962 ops/sec | 582,014 ops/sec | -46% (CPU-pinned) |
| Point lookup (100K rows) | 36,774 ops/sec | 190,418 ops/sec | **+418%** |
| Mixed reads | 32,714 ops/sec | 39,489 ops/sec | **+21%** |
| Mixed writes | 8,201 ops/sec | 9,886 ops/sec | **+21%** |
| Collection Point Get | 1,380,629 ops/sec | 702,433 ops/sec | -49% (CPU-pinned) |
| Collection Put | 10,935 ops/sec | 18,586 ops/sec | **+70%** |
| Concurrent readers (8) | 576,447 ops/sec | 164,444 ops/sec | -72% (8-CPU affinity) |
| Writer with 8 readers | 2,801 ops/sec | 3,033 ops/sec | **+8%** |
| Crash recovery P50 | — | 26.5 ms | — |

**Key observations:** The repro mode with CPU affinity pinning produces dramatically different profiles. Auto-commit writes improved **+70%** from storage-layer optimizations. Point lookups at small (100-1K) and large (100K) datasets saw **massive gains** (up to +418%) due to better CPU cache locality from affinity pinning. However, operations that previously benefited from spreading across all 16 logical CPUs (concurrent 8-reader throughput, 10K-row lookups) show lower numbers under the constrained 8-CPU topology. These repro numbers are more **reproducible and comparable** across runs.

### Run 8 — Collection (NoSQL) API Benchmarks

Run 8 adds the new Collection API and its dedicated benchmark suite. The Collection API bypasses the SQL parser and planner, going directly to the B+tree for document operations.

| Metric | SQL API | Collection API | Difference |
|--------|---------|---------------|------------|
| Single write (auto-commit) | 10,611 ops/sec | 10,935 ops/sec | **+3%** |
| Batch write (100/tx) | ~247K rows/sec | ~213K docs/sec | -14% (JSON overhead) |
| Point read (10K dataset) | 1,084,962 ops/sec | 1,380,629 ops/sec | **+27%** |
| Mixed reads (80/20) | 32,714 ops/sec | 35,725 ops/sec | **+9%** |
| Mixed writes (80/20) | 8,201 ops/sec | 8,952 ops/sec | **+9%** |

The Collection API is **fastest read path in CSharpDB** at 1.44M ops/sec. Write performance is comparable because both APIs are bottlenecked by the same WAL flush. Batch writes are slightly slower due to JSON serialization overhead per document.

### Run 7 vs Run 5 — Point Lookup Optimizations

Run 6 added BTree hint cache, QueryPlanner fast path, and row buffer reuse. Run 7 added the opt-in sync fast path (`PreferSyncPointLookups`) that bypasses the async operator pipeline for cached point lookups.

| Metric | Run 5 | Run 7 | Change |
|--------|-------|----------------|--------|
| Point lookup (1K rows) | 157,015 ops/sec | 217,694 ops/sec | **+39%** |
| Point lookup (10K rows) | 67,513 ops/sec | 78,407 ops/sec | **+16%** |
| Point lookup (100K rows) | 31,400 ops/sec | 36,774 ops/sec | **+17%** |
| BTree depth 2 (1,600 rows) | 186,717 ops/sec | 296,815 ops/sec | **+59%** |
| BTree depth 3 (50K rows) | 50,060 ops/sec | 54,829 ops/sec | **+10%** |
| BTree depth 3 (100K rows) | 43,460 ops/sec | 47,804 ops/sec | **+10%** |
| Concurrent readers (8) | 439,373 ops/sec | 576,447 ops/sec | **+31%** |
| Mixed reads | 32,006 ops/sec | 32,714 ops/sec | **+2%** |
| Writer with 8 readers | 1,470 ops/sec | 2,801 ops/sec | **+91%** |

The sync fast path eliminates ~1.6-2µs of async state machine overhead per cached point lookup (6-7 `async ValueTask` layers, each ~400ns even when completing synchronously). SQLite avoids this entirely by being synchronous C code — the async overhead was the primary gap.

### Run 5 (.NET 10) vs Run 4 (.NET 9)

| Metric | Run 4 (.NET 9) | Run 5 (.NET 10) | Change |
|--------|----------------|-----------------|--------|
| Sustained writes (auto-commit) | 9,532 ops/sec | 10,624 ops/sec | **+11%** |
| Batch 100 rows/tx | 2,299 tx/sec | 2,459 tx/sec | **+7%** |
| Mixed reads | 29,811 ops/sec | 32,006 ops/sec | **+7%** |
| Mixed writes | 7,468 ops/sec | 8,024 ops/sec | **+7%** |
| Point lookup (1K rows) | 154,297 ops/sec | 157,015 ops/sec | **+2%** |
| Point lookup (10K rows) | 60,269 ops/sec | 67,513 ops/sec | **+12%** |
| ADO.NET INSERT | 127 us | 117 us | **8% faster** |
| ADO.NET COUNT | 288 ns | 253 ns | **12% faster** |
| ADO.NET Reader (100 rows) | 9.96 us | 8.59 us | **14% faster** |
| ADO.NET Parameterized SELECT | 70.0 us | 65.7 us | **6% faster** |
| ADO.NET Prepared SELECT | 70.4 us | 63.9 us | **9% faster** |
| Crash recovery P50 | 15.7 ms | 13.9 ms | **11% faster** |
| WAL read P50 (1K frames) | 0.004 ms | 0.002 ms | **2x faster** |
| Insert at 100K rows | 8,772 ops/sec | 6,584 ops/sec | -25% (variance) |
| Crash recovery | 100% (50/50) | 100% (50/50) | **Maintained** |

The .NET 10 upgrade combined with engine improvements delivered consistent gains across write throughput (+7-11%), ADO.NET latencies (8-14% faster), and crash recovery speed (+11%). WAL read latency improved to 0.002ms P50 — effectively zero overhead.

### Run 4 vs Run 3

| Metric | Run 3 | Run 4 | Change |
|--------|-------|-------|--------|
| Sustained writes (auto-commit) | 8,696 ops/sec | 9,532 ops/sec | **+10%** |
| Batch 100 rows/tx | 2,041 tx/sec | 2,299 tx/sec | **+13%** |
| Mixed reads | 26,762 ops/sec | 29,811 ops/sec | **+11%** |
| Mixed writes | 6,697 ops/sec | 7,468 ops/sec | **+12%** |
| Point lookup (1K rows) | 30,258 ops/sec | 154,297 ops/sec | **5.1x faster** |
| BTree depth 2 lookups | 148,810 ops/sec | 345,925 ops/sec | **2.3x faster** |
| Concurrent readers (8 readers) | 379,529 ops/sec | 527,579 ops/sec | **+39%** |
| Writer with 8 readers | 1,543 ops/sec | 2,550 ops/sec | **+65%** |
| ADO.NET INSERT | 143 us | 127 us | **11% faster** |
| ADO.NET COUNT | 343 ns | 288 ns | **16% faster** |

### Full History: Unoptimized (Run 1) to Latest (Run 9)

| Metric | Run 1 (Unoptimized) | Run 9 (Latest, repro) | Total Improvement |
|--------|---------------------|----------------------|-------------------|
| Sustained writes | 1,062 ops/sec | 18,005 ops/sec | **17.0x** |
| Point lookup (1K rows) | ~30K ops/sec | 727,502 ops/sec | **24.3x** |
| Point lookup (100K rows) | N/A | 190,418 ops/sec | — |
| Collection point Get | N/A | 702,433 ops/sec | — (new API) |
| Mixed reads | 368 ops/sec | 39,489 ops/sec | **107x** |
| Mixed writes | 91 ops/sec | 9,886 ops/sec | **109x** |
| Concurrent readers (8) | 1,026 ops/10s | 164,444 ops/sec | **1,603x** |
| BTree depth 2 lookups | N/A | 768,119 ops/sec | — |
| ADO.NET INSERT latency | 1,201 us | 117 us | **10.3x faster** |
| ADO.NET INSERT memory | 1,802 KB | 3.7 KB | **487x less** |
| ADO.NET COUNT latency | 150 us | 253 ns | **593x faster** |
| ADO.NET COUNT memory | 347 KB | 448 B | **793x less** |
| WAL growth read penalty | Linear (1.08ms at 10K) | Constant (0.001ms) | **Eliminated** |
| Write amplification (50K) | 4.8x | 2.9x | **40% less** |
| Crash recovery | 100% (50/50) | 100% (50/50) | **Maintained** |

---

## Remaining Optimization Opportunities

Based on the current benchmark data, the highest-impact future optimizations would be:

1. **Generalized index range-scan planning**: Expand index pushdown for `<`, `>`, `<=`, `>=`, and `BETWEEN` across more query shapes, not only current ordered-index paths.

2. **Connection pooling tuning**: Connection pooling is implemented; next leverage point is tuning defaults and expanding pooled-vs-unpooled benchmark coverage.

3. **Memory-mapped I/O (mmap)**: SQLite's key advantage for reads is zero-copy page access via mmap. The current `byte[]` copy per page read adds GC pressure and copy cost that mmap would eliminate.

4. **Async I/O batching**: Group multiple page writes into fewer I/O system calls during batch operations.

5. **Columnar scan optimization**: Full table COUNT(*) scans (205 ops/sec at 100K rows) could benefit from aggregate metadata or partial counting.

6. **Write-ahead buffering**: Buffer multiple WAL writes before flushing to disk to improve auto-commit write throughput beyond the current fsync-limited rate.

---

## Benchmark Architecture

```
tests/CSharpDB.Benchmarks/
  Program.cs                          CLI entry point
  Infrastructure/
    BenchmarkDatabase.cs              Temp database lifecycle
    DataGenerator.cs                  Deterministic test data
    MacroBenchmarkRunner.cs           Custom harness (sustained workloads)
    LatencyHistogram.cs               Percentile computation
    BenchmarkResult.cs                Result model
    CsvReporter.cs                    CSV + console output
  Micro/                              BenchmarkDotNet micro-benchmarks
    InsertBenchmarks.cs               Single/batch insert throughput
    PointLookupBenchmarks.cs          PK/indexed/non-indexed lookups
    ScanBenchmarks.cs                 Full scan, filter, aggregate
    JoinBenchmarks.cs                 INNER/LEFT/CROSS JOIN
    IndexBenchmarks.cs                Index overhead + speedup
    TextIndexBenchmarks.cs            TEXT equality lookup (indexed vs scan)
    WalBenchmarks.cs                  WAL commit/checkpoint
    ParserBenchmarks.cs               SQL parser throughput
    SqlTextStabilityBenchmarks.cs     Parser/cache behavior under SQL text churn
    SystemCatalogBenchmarks.cs        sys.* query and metadata API overhead
    TriggerDispatchBenchmarks.cs      Trigger lookup/dispatch overhead
    AdoNetBenchmarks.cs               ADO.NET provider overhead
    RecordSizeBenchmarks.cs           Payload size impact
  Macro/                              Sustained workload benchmarks
    SustainedWriteBenchmark.cs        Continuous write throughput
    MixedWorkloadBenchmark.cs         80/20 read/write mix
    ReaderScalingBenchmark.cs         Concurrent snapshot readers
    WriteAmplificationBenchmark.cs    Storage efficiency
    CheckpointUnderLoadBenchmark.cs   Checkpoint impact
    CollectionBenchmark.cs            NoSQL Collection API (Put/Get/Scan/Find)
  Stress/                             Durability tests
    CrashRecoveryBenchmark.cs         Crash-recovery cycles
    WalGrowthBenchmark.cs             Read perf vs WAL size
  Scaling/                            Scaling experiments
    RowCountScalingBenchmark.cs       100 to 100K rows
    BTreeDepthBenchmark.cs            Tree depth impact
```

## Re-running After Changes

After making engine optimizations, re-run the full suite and compare CSVs:

```bash
# Run all custom benchmarks
dotnet run -c Release -- --macro
dotnet run -c Release -- --stress
dotnet run -c Release -- --scaling

# Run BenchmarkDotNet (more rigorous, takes longer)
dotnet run -c Release -- --micro --filter *
```

Results are timestamped, so previous runs are preserved for comparison.

---

## See Also

- [Project README](../../README.md) — Overview and quick start
- [Roadmap](../../docs/roadmap.md) — Optimization status and future priorities informed by this benchmark data
- [Architecture Guide](../../docs/architecture.md) — How the engine is structured
