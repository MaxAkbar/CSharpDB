# CSharpDB Benchmark Suite

Performance benchmarks for the CSharpDB embedded database engine. Results can be used to track performance regressions, identify optimization opportunities, and compare against other embedded databases (SQLite, LiteDB, DuckDB, etc.).

## Test Environment

| Component | Details |
|-----------|---------|
| CPU | Intel Core i9-11900K @ 3.50GHz, 8 cores / 16 threads |
| OS | Windows 11 (10.0.26300) |
| Runtime | .NET 9.0.12, X64 RyuJIT AVX-512F |
| Disk | NVMe SSD |
| Page Size | 4,096 bytes |
| WAL Mode | Enabled (redo-log with auto-checkpoint at 1,000 frames) |

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

# Run everything
dotnet run -c Release -- --all
```

Results are written to CSV in `bin/Release/net9.0/results/`.

---

## Benchmark Results

### Write Performance

| Operation | Throughput | P50 Latency | P99 Latency | Notes |
|-----------|-----------|-------------|-------------|-------|
| Single INSERT (auto-commit) | 1,062 ops/sec | 0.76 ms | 2.74 ms | Each op = parse + BEGIN + insert + WAL flush + COMMIT |
| INSERT in explicit transaction | ~8,800 ops/sec | 0.09 ms | 0.40 ms | Amortized WAL flush across batch |
| Batch 100 rows/tx | 1,200 rows/sec | 79.7 ms/batch | 167.2 ms/batch | 12 tx/sec x 100 rows |
| Batch 1000 rows/tx (in-tx insert) | ~6,600 ops/sec | 0.12 ms | 0.42 ms | Best sustained throughput |

**Key insight**: Batching inside explicit transactions yields a **8x throughput improvement** over auto-commit, because the WAL fsync cost is amortized. Applications should batch writes for best performance.

### Read Performance

| Operation | Throughput | P50 Latency | P99 Latency | Dataset |
|-----------|-----------|-------------|-------------|---------|
| Point lookup by PK | 3,005 ops/sec | 0.14 ms | 0.59 ms | 100 rows |
| Point lookup by PK | 1,728 ops/sec | 0.51 ms | 1.15 ms | 1K rows |
| Point lookup by PK | 402 ops/sec | 2.38 ms | 4.38 ms | 10K rows |
| Point lookup by PK | 51 ops/sec | 19.2 ms | 29.3 ms | 100K rows |
| ADO.NET ExecuteReader (100 rows) | ~52,000 row-reads/sec | 19.2 us/call | -- | 1K row table |
| ADO.NET ExecuteScalar COUNT(*) | ~6,660 ops/sec | 150 us | -- | 1K row table |
| ADO.NET Parameterized SELECT | ~5,320 ops/sec | 188 us | -- | 1K row table |

### Mixed Workload (80% Reads / 20% Writes)

| Component | Throughput | P50 Latency | P99 Latency |
|-----------|-----------|-------------|-------------|
| Reads | 368 ops/sec | 2.24 ms | 4.95 ms |
| Writes | 91 ops/sec | 1.06 ms | 2.61 ms |

Sustained for 15 seconds on a 10K-row table with concurrent read and write traffic.

### B+Tree Depth Scaling

| Tree Depth | Row Count | Lookups/sec | P50 Latency | P99 Latency |
|------------|-----------|-------------|-------------|-------------|
| Depth 2 | 1,600 | 5,917 | 0.16 ms | 0.32 ms |
| Depth 3 | 50,000 | 181 | 5.43 ms | 6.79 ms |
| Depth 3 | 100,000 | 64 | 13.3 ms | 33.7 ms |

Lookup performance scales with tree depth as expected for B+tree (logarithmic page reads), but the per-query SQL pipeline overhead (parse, plan, execute, materialize) dominates at all depths.

### WAL & Checkpoint Performance

| Metric | Value |
|--------|-------|
| Checkpoint time (100 WAL frames) | 7.3 ms |
| Checkpoint time (500 WAL frames) | 7.8 ms |
| Checkpoint time (1,000 WAL frames) | 10.0 ms |
| Checkpoint time (2,000 WAL frames) | 9.2 ms |
| Auto-checkpoint threshold | 1,000 frames |

Checkpoint is fast and scales sub-linearly with WAL size.

### WAL Growth Impact on Read Latency

| WAL Frames | WAL Size | Read P50 | Read P99 |
|------------|----------|----------|----------|
| 100 | 28 KB | 0.016 ms | 0.052 ms |
| 1,000 | 185 KB | 0.165 ms | 0.491 ms |
| 5,000 | 878 KB | 0.633 ms | 1.287 ms |
| 10,000 | 1.7 MB | 1.080 ms | 1.791 ms |
| Post-checkpoint | -- | 1.012 ms | 2.036 ms |

Read latency grows linearly with WAL size because the WAL index must be scanned for each page read. Regular checkpoints keep this in check.

### Concurrent Reader Scaling

| Readers | Writer ops/sec | Total Reader ops (10s) | Writer P99 |
|---------|---------------|----------------------|------------|
| 0 (writer only) | ~8,800 | -- | 0.40 ms |
| 1 | 777 | 590 | 12.4 ms |
| 2 | 459 | 835 | 17.5 ms |
| 4 | 235 | 914 | 23.8 ms |
| 8 | 101 | 1,026 | 43.0 ms |

Snapshot readers don't block the writer (WAL-based MVCC), but total throughput is shared across all sessions due to single-threaded I/O on the underlying file.

### Write Amplification

| Row Count | Logical Data | WAL Size (pre-ckpt) | DB Size (post-ckpt) | Amplification |
|-----------|-------------|---------------------|---------------------|---------------|
| 1,000 | 112 KB | 532 KB | 512 KB | 9.3x |
| 10,000 | 1.1 MB | 1.1 MB | 5.1 MB | 5.5x |
| 50,000 | 5.6 MB | 1.1 MB | 25.6 MB | 4.8x |

Write amplification decreases as the dataset grows because page utilization improves. The 4KB page overhead per write dominates at small scales.

### ADO.NET Provider Overhead

| Operation | Mean Latency | Memory Allocated |
|-----------|-------------|-----------------|
| ExecuteNonQuery (INSERT) | 1,201 us | 1,802 KB |
| ExecuteScalar (COUNT) | 150 us | 347 KB |
| ExecuteReader (100 rows) | 19.2 us | 36.6 KB |
| Parameterized SELECT | 188 us | 280 KB |

The ADO.NET layer adds minimal overhead on top of the raw engine. Memory allocation per operation is an area for future optimization.

### Crash Recovery & Durability

| Metric | Value |
|--------|-------|
| Crash recovery success rate | **100%** (50/50 cycles) |
| Recovery time P50 | 16.9 ms |
| Recovery time P99 | 26.0 ms |
| Data verified after recovery | All committed rows present, uncommitted rows correctly absent |

The WAL-based crash recovery is fully reliable. Recovery time is fast and bounded by WAL replay.

---

## Comparison Reference Points

These are rough reference points from published benchmarks for context. Actual numbers vary by hardware, workload, and configuration.

| Metric | CSharpDB | SQLite (WAL) | LiteDB | DuckDB |
|--------|----------|-------------|--------|--------|
| Single auto-commit INSERT | ~1K ops/sec | ~50-100K ops/sec | ~10-30K ops/sec | ~100K+ ops/sec |
| Batched INSERT (per row in tx) | ~8.8K ops/sec | ~200-400K ops/sec | ~50-100K ops/sec | ~500K+ ops/sec |
| Point lookup (1K rows) | ~1.7K ops/sec | ~100-500K ops/sec | ~50-100K ops/sec | ~200K+ ops/sec |
| Crash recovery | 100% reliable | 100% reliable | 100% reliable | 100% reliable |

CSharpDB is a learning/educational engine built from scratch in C#. It implements a complete SQL pipeline (parser, planner, B+tree storage, WAL, ADO.NET provider) and prioritizes correctness over speed. The performance gap vs production databases is expected and represents optimization opportunities.

---

## Key Optimization Opportunities

Based on the benchmark data, the highest-impact optimizations would be:

1. **Page cache**: Currently pages may be re-read from disk on each access. A proper LRU page cache would dramatically improve read performance, especially for point lookups.

2. **Prepared statement cache**: Each query re-parses SQL and re-plans. Caching parsed ASTs or compiled plans would eliminate repeated parsing overhead.

3. **Reduce allocations**: The ADO.NET INSERT allocates 1.8 MB per call. Pooling byte arrays, reducing string allocations, and using `Span<T>` in hot paths would reduce GC pressure.

4. **Async I/O batching**: Group multiple page reads into fewer I/O system calls where possible.

5. **WAL index lookup optimization**: Use a hash map instead of linear WAL frame scan to improve read performance when WAL is large.

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
    WalBenchmarks.cs                  WAL commit/checkpoint
    ParserBenchmarks.cs               SQL parser throughput
    AdoNetBenchmarks.cs               ADO.NET provider overhead
    RecordSizeBenchmarks.cs           Payload size impact
  Macro/                              Sustained workload benchmarks
    SustainedWriteBenchmark.cs        Continuous write throughput
    MixedWorkloadBenchmark.cs         80/20 read/write mix
    ReaderScalingBenchmark.cs         Concurrent snapshot readers
    WriteAmplificationBenchmark.cs    Storage efficiency
    CheckpointUnderLoadBenchmark.cs   Checkpoint impact
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
