# CSharpDB Benchmark Suite

Performance benchmarks for the CSharpDB embedded database engine.

The current snapshot in this README mixes the March 14, 2026 balanced macro capture, the March 14, 2026 full baseline capture, the latest focused reruns still present in `BenchmarkDotNet.Artifacts/results`, and a smaller set of archived March 12 validation numbers called out inline below.

- `Balanced macro capture on March 14, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/macro-20260314-214358.csv`
- `Full sequential baseline capture on March 14, 2026: tests/CSharpDB.Benchmarks/baselines/20260314-173320`
- `Latest focused reruns on March 15, 2026: InsertBenchmarks, PointLookupBenchmarks, ReaderSessionBenchmarks, MemoryMappedReadBenchmarks, WalReadCacheBenchmarks, BTreeCursorBenchmarks, OrderByIndexBenchmarks, ScanBenchmarks, ScanProjectionBenchmarks, JoinBenchmarks, CompositeGroupedIndexBenchmarks, ColdLookupBenchmarks`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ReaderSessionBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.MemoryMappedReadBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.WalReadCacheBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.BTreeCursorBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ScanBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ScanProjectionBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ColdLookupBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InMemorySqlBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InMemoryCollectionBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InMemoryAdoNetBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InMemoryPersistenceBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.CollectionPayloadBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.CollectionSchemaBreadthBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.CollectionIndexBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.StorageTuningBenchmarks-report.csv`

## Test Environment

| Component | Details |
|-----------|---------|
| CPU | Intel Core i9-11900K @ 3.50GHz, 8 cores / 16 threads |
| OS | Windows 11 (10.0.26300) |
| Runtime | .NET 10.0.4, X64 RyuJIT AVX-512F |
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
dotnet run -c Release -- --micro --filter *InMemory*
dotnet run -c Release -- --micro --filter *InsertBenchmarks*
dotnet run -c Release -- --micro --filter *PointLookupBenchmarks*
dotnet run -c Release -- --micro --filter *ReaderSessionBenchmarks*
dotnet run -c Release -- --micro --filter *SqlMaterializationBenchmarks*
dotnet run -c Release -- --micro --filter *CollectionAccessBenchmarks*
dotnet run -c Release -- --micro --filter *MemoryMappedReadBenchmarks*
dotnet run -c Release -- --micro --filter *WalReadCacheBenchmarks*
dotnet run -c Release -- --micro --filter *BTreeCursorBenchmarks*
dotnet run -c Release -- --micro --filter *CoveringIndexBenchmarks*
dotnet run -c Release -- --micro --filter *CompositeIndexBenchmarks*
dotnet run -c Release -- --micro --filter *CollectionFieldExtractionBenchmarks*
dotnet run -c Release -- --micro --filter *IndexProjectionBenchmarks*
dotnet run -c Release -- --micro --filter *OrderByIndexBenchmarks*
dotnet run -c Release -- --micro --filter *IndexAggregateBenchmarks*
dotnet run -c Release -- --micro --filter *PrimaryKeyAggregateBenchmarks*
dotnet run -c Release -- --micro --filter *DistinctAggregateBenchmarks*
dotnet run -c Release -- --micro --filter *GroupedIndexAggregateBenchmarks*
dotnet run -c Release -- --micro --filter *CompositeGroupedIndexBenchmarks*
dotnet run -c Release -- --micro --filter *PredicatePushdownBenchmarks*
dotnet run -c Release -- --micro --filter *ScanBenchmarks*
dotnet run -c Release -- --micro --filter *ScanProjectionBenchmarks*
dotnet run -c Release -- --micro --filter *JoinBenchmarks*
dotnet run -c Release -- --micro --filter *CollectionLookupFallbackBenchmarks*

# Run the focused phase-1 baseline set
pwsh ./tests/CSharpDB.Benchmarks/scripts/Run-Phase1-Baselines.ps1

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
- `MemoryMappedReadBenchmarks`: file-backed SQL and collection cold lookups with `UseMemoryMappedReads` toggled on and off
- `WalReadCacheBenchmarks`: file-backed SQL cold lookups where the latest table pages stay in WAL and `MaxCachedWalReadPages` is toggled
- `BTreeCursorBenchmarks`: raw storage-layer sequential B+tree cursor scans and seek+window traversals without SQL executor overhead
- `CollectionIndexBenchmarks`: focused collection secondary-index lookup and indexed write-maintenance costs
- `StorageTuningBenchmarks`: cache-size, index-provider, and reader-session matrix for file-backed indexed lookups
- `DurableWriteDiagnosticsBenchmark`: file-backed single-row write diagnostics across frame-count and WAL-size checkpoint policies, including background sliced auto-checkpoint scheduling
- `InMemoryBatchBenchmark`: rotating x100 batch throughput for in-memory SQL and collections
- `InMemoryWorkloadBenchmark`: macro mixed workloads for SQL and collections in memory vs file-backed
- `SharedMemoryAdoNetBenchmark`: named shared-memory reader/writer contention through the provider host layer
- `InMemoryPersistenceBenchmark`: macro load/save latency and output-size reporting

### Phase 1 Baseline Suites

- `SqlMaterializationBenchmarks`: isolates full-row decode, selected-column decode, single-column access, and payload-level text/numeric checks
- `CollectionAccessBenchmarks`: isolates full document hydration, key-only access, key matching, and indexed-field reads over collection payloads
- `MemoryMappedReadBenchmarks`: isolates file-backed SQL and collection cold lookups so the opt-in `mmap` read path can be compared directly against copy-based reads
- `WalReadCacheBenchmarks`: isolates file-backed SQL cold lookups where the latest table pages remain WAL-backed so the dedicated WAL read cache can be compared directly against fresh WAL stream reads
- `BTreeCursorBenchmarks`: isolates raw forward cursor traversal and seek+window scans over a B+tree so sequential leaf-scan changes can be measured without the SQL executor on top
- `CoveringIndexBenchmarks`: isolates unique-index lookup shapes that could become index-only from shapes that still need the wide base-row payload
- `IndexProjectionBenchmarks`: isolates non-unique secondary-index lookups where `SELECT id` or `SELECT indexed_col` can now avoid base-row fetches
- `OrderByIndexBenchmarks`: isolates indexed `ORDER BY`, covered integer range scans, and compact non-covered range projection shapes where indexed filtering still avoids full row materialization
- `IndexAggregateBenchmarks`: isolates scalar `SUM` / `COUNT` / `MIN` / `MAX` queries and range aggregates that can now execute directly from integer index keys
- `PrimaryKeyAggregateBenchmarks`: isolates scalar and ranged aggregates that can now execute directly from the `INTEGER PRIMARY KEY` table B-tree key stream
- `GroupedIndexAggregateBenchmarks`: isolates `GROUP BY` on a duplicate-heavy integer key so grouped aggregates can be compared against the new direct index-grouped fast path
- `CompositeGroupedIndexBenchmarks`: isolates `GROUP BY` on composite indexed keys and leftmost-prefix grouped scans so composite grouped fast paths can be compared against generic grouped scans
- `CollectionFieldExtractionBenchmarks`: isolates early/middle/late extraction cost, nested-path access, miss cost, and full document hydration comparison for collection payload scans
- `CollectionLookupFallbackBenchmarks`: isolates collection equality lookups on unindexed fields to measure the direct-payload compare fallback before full document hydration
- `Run-Phase1-Baselines.ps1`: runs the focused phase-1 benchmark set without the larger macro, stress, or scaling suites

### Baselines and Guardrails

```bash
# Capture a fresh baseline snapshot
pwsh ./tests/CSharpDB.Benchmarks/scripts/Capture-Baseline.ps1

# Run the current guardrail subset
pwsh ./tests/CSharpDB.Benchmarks/scripts/Run-Perf-Guardrails.ps1
```

Defaults:

- Baseline snapshot: `tests/CSharpDB.Benchmarks/baselines/20260314-173320`
- Threshold config: `tests/CSharpDB.Benchmarks/perf-thresholds.json`
- Last guardrail report: `tests/CSharpDB.Benchmarks/results/perf-guardrails-last.md`
- `Capture-Baseline.ps1` runs non-micro suites in reproducible mode by default and captures macro results as `--macro --repeat 3 --repro`.

## Current Performance Snapshot

### SQL API (latest balanced macro snapshot)

| Metric | Current Result | Notes |
|--------|----------------|-------|
| Single INSERT | 26.98K ops/sec | Auto-commit durable write |
| Batch 100 rows/tx | ~695K rows/sec | 6,945.0 tx/sec x 100 rows |
| Point lookup (10K rows) | 1.42M ops/sec | `Comparison_SQL_PointLookup_10k` |
| Mixed workload reads | 65.7K ops/sec | 80/20 read/write mix |
| Mixed workload writes | 16.4K ops/sec | 80/20 read/write mix |
| Reader throughput (8 readers, per-query sessions) | 911.96K ops/sec | Total `COUNT(*)` queries/sec across 8 readers |
| Reader throughput (8 readers, reused snapshots x32) | 10.70M ops/sec | `ReaderScalingBurst32_8readers_Readers` |
| Writer throughput under 8 readers | 21.24K ops/sec | Same 8-reader scaling run |
| Checkpoint time (1,000 WAL frames) | 3.72 ms | Manual checkpoint |

### Collection API (latest balanced macro snapshot)

| Metric | Current Result | Notes |
|--------|----------------|-------|
| Single Put | 31.96K ops/sec | Auto-commit durable document write |
| Batch 100 docs/tx | ~425K docs/sec | 4,252.3 tx/sec x 100 docs |
| Point Get (10K docs) | 1.50M ops/sec | Direct collection lookup |
| Mixed workload reads | 80.6K ops/sec | 80/20 read/write mix |
| Mixed workload writes | 20.1K ops/sec | 80/20 read/write mix |
| Full Scan (1K docs) | 2,589 scans/sec | Full collection scan |
| Filtered Find (1K docs, 20% match) | 2,501 scans/sec | Predicate evaluation path |
| Indexed equality lookup (10K docs) | 617.07K ops/sec | `Collection_FindByIndex_Value_10k_15s` |
| Single Put (with 1 secondary index) | 21.75K ops/sec | `Collection_Put_Single_WithIndex_15s` |

### Collection Path Micro Spot Checks

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Collection encode (direct payload) | 189 ns | 216 B | Current collection payload path |
| Collection encode (legacy row format) | 275 ns | 304 B | Prior `DbValue[]` + record serializer path |
| Collection decode (direct payload) | 356 ns | 328 B | Current collection payload path |
| Collection decode (legacy row format) | 433 ns | 600 B | Prior `DbValue[]` + record serializer path |
| Collection put (minimal schema, in-memory) | 2.45 us | 702 B | Auto-commit write with only the target collection loaded |
| Collection put (48 extra tables + 48 extra collections, in-memory) | 2.51 us | 693 B | Unrelated schema breadth no longer adds measurable write tax |

### Query Micro Spot Checks

| Metric | Mean | Allocated |
|--------|------|-----------|
| SQL PK lookup (10K rows) | 519 ns | 728 B |
| SQL PK lookup (100K rows) | 729 ns | 728 B |
| SQL indexed lookup (100K rows) | 677 ns | 490 B |
| SQL point miss (100K rows) | 330 ns | 424 B |

### Reader Session Spot Checks (March 14, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `COUNT(*)` with per-query reader sessions | 168.80 ns | 464 B | Full reader-session create/execute/dispose path |
| `COUNT(*)` with reused reader session | 85.28 ns | 242 B | Same query with a reused snapshot session |
| Point lookup with per-query reader sessions | 649.83 ns | 735 B | Reader-session setup is now close to direct execute cost |
| Point lookup with reused reader session | 617.39 ns | 513 B | Small remaining gap versus direct execution |
| Point lookup with direct `ExecuteAsync` | 582.32 ns | 504 B | Lower bound for the same simple PK read path |

### Memory-Mapped Read Spot Checks (March 14, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| SQL cold lookup, copy-based read path | 27.04 us | 9.50 KB | File-backed cache-pressured lookup with `UseMemoryMappedReads = false` |
| SQL cold lookup, mmap read path | 1.30 us | 648 B | Same workload with clean main-file pages served from mapped read views |
| Collection cold get, copy-based read path | 27.38 us | 9.35 KB | File-backed cache-pressured collection lookup with `UseMemoryMappedReads = false` |
| Collection cold get, mmap read path | 1.23 us | 418 B | Same workload with mapped main-file reads and copy-on-write only on mutable access |

### WAL Read Cache Spot Checks (March 14, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| SQL cold lookup, WAL-backed, no WAL cache | 25.64 us | 8.68 KB | File-backed cache-pressured lookup where the latest table pages are still read from WAL frames |
| SQL cold lookup, WAL-backed, 128-page WAL cache | 17.80 us | 6.09 KB | Same workload with `MaxCachedWalReadPages = 128` so immutable WAL frame images can be reused between reads |

### B-Tree Cursor Spot Checks (March 14, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| B-tree cursor full scan (10K rows, read-ahead off) | 9.12 ms | 3.26 MB | File-backed raw forward scan with `EnableSequentialLeafReadAhead = false` |
| B-tree cursor full scan (10K rows, read-ahead on) | 7.86 ms | 3.18 MB | Same scan with speculative next-leaf reads enabled |
| B-tree cursor seek + 1024-row window (10K rows, read-ahead off) | 928.10 us | 350.67 KB | Mid-tree seek followed by sequential leaf traversal |
| B-tree cursor seek + 1024-row window (10K rows, read-ahead on) | 798.90 us | 337.41 KB | Same seek-window path with speculative next-leaf reads |
| B-tree cursor full scan (100K rows, read-ahead off) | 88.88 ms | 32.60 MB | File-backed forward scan across a deeper leaf chain |
| B-tree cursor full scan (100K rows, read-ahead on) | 80.58 ms | 31.83 MB | Same scan with speculative next-leaf reads enabled |
| B-tree cursor seek + 1024-row window (100K rows, read-ahead off) | 899.50 us | 350.68 KB | Mid-tree seek followed by a bounded sequential window |
| B-tree cursor seek + 1024-row window (100K rows, read-ahead on) | 797.40 us | 337.39 KB | Seek-window path with speculative next-leaf reads; latency stays roughly flat as the tree grows |

### SQL Covered Read-Path Spot Checks (March 14, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Unique index lookup `SELECT *` (100K rows) | 6.88 us | 2.95 KB | Baseline unique secondary-index lookup |
| Unique index lookup `SELECT id` (100K rows) | 5.78 us | 1.08 KB | Covered projection from index payload |
| Unique index lookup `SELECT lookup_key` (100K rows) | 5.78 us | 1.11 KB | Covered projection from index payload |
| Non-unique index lookup `SELECT *` (100K rows) | 388.88 us | 71.04 KB | Baseline duplicate-key secondary-index lookup |
| Non-unique index lookup `SELECT id` (100K rows) | 378.87 us | 33.59 KB | Covered projection drops most row materialization cost |
| `ORDER BY value` no index (100K rows) | 155.35 ms | 48.64 MB | Full sort baseline from the latest indexed-order rerun |
| `ORDER BY value` covered index-order scan (100K rows) | 28.06 ms | 14.56 MB | `SELECT id, value` stays on index data |
| `ORDER BY value LIMIT 100` index-order scan (100K rows) | 37.47 us | 34.37 KB | Index order avoids sort, still fetches base rows |
| `ORDER BY value LIMIT 100` covered index-order scan (100K rows) | 19.99 us | 16.35 KB | Index-only top-N path |
| `WHERE value BETWEEN ...` row fetch (100K rows) | 50.48 ms | 16.43 MB | Integer range scan with base-row fetch |
| `WHERE value BETWEEN ...` covered projection (100K rows) | 15.88 ms | 7.30 MB | Integer range scan that stays on index data |
| `WHERE value BETWEEN ... SELECT id, category` compact projection (100K rows) | 36.79 ms | 7.28 MB | Non-covered indexed range scan decodes only projected payload columns instead of wide rows |
| `WHERE value BETWEEN ... SELECT id, value + id` compact expression projection (100K rows) | 42.45 ms | 11.47 MB | Indexed range scan keeps the compact payload decode path even when projection includes an expression |

### SQL Composite Equality Lookup Spot Checks (March 14, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `WHERE a = ... AND b = ...` no index (100K rows) | 175.17 ms | 69.43 MB | Full scan over wide rows |
| `WHERE a = ... AND b = ...` single-column index (100K rows) | 23.92 us | 4.27 KB | Uses `a` only, then filters `b` after row fetch |
| `WHERE a = ... AND b = ... SELECT *` composite index (100K rows) | 1.17 us | 1.83 KB | Direct composite equality lookup over hashed secondary index |
| `WHERE a = ... AND b = ... SELECT id, a, b` composite covered projection (100K rows) | 1.40 us | 2.03 KB | Index-only projection now uses no-copy hashed-payload matching on the covered path |
| `WHERE a = ... AND b = ... SELECT id, a, b` unique composite covered projection (100K rows) | 1.46 us | 2.03 KB | Same covered path on a unique composite index |

### SQL Indexed Aggregate Spot Checks (March 14, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `SUM(value)` no index (100K rows) | 5.75 ms | 482 B | Full table aggregate over decoded rows |
| `SUM(value)` direct index aggregate (100K rows) | 3.57 ms | 553 B | Walks integer index keys without base-row fetch |
| `COUNT(value)` no index (100K rows) | 4.02 ms | 490 B | Full table aggregate |
| `COUNT(value)` direct index aggregate (100K rows) | 2.87 ms | 561 B | Counts row-id payloads per integer key |
| `MIN(value)` no index (100K rows) | 5.09 ms | 482 B | Full scan baseline |
| `MIN(value)` direct index aggregate (100K rows) | 387 ns | 552 B | First-key fast path on ordered integer index |
| `MAX(value)` no index (100K rows) | 5.50 ms | 482 B | Full scan baseline |
| `MAX(value)` direct index aggregate (100K rows) | 404 ns | 552 B | Rightmost-key fast path on ordered integer index |
| `COUNT(*) WHERE value BETWEEN ...` no index (100K rows) | 9.74 ms | 2.45 KB | Scan + predicate baseline |
| `COUNT(*) WHERE value BETWEEN ...` direct index aggregate (100K rows) | 1.80 ms | 1.05 KB | Range aggregate from integer index keys |
| `SUM(value) WHERE value BETWEEN ...` no index (100K rows) | 12.53 ms | 2.74 KB | Scan + predicate baseline |
| `SUM(value) WHERE value BETWEEN ...` direct index aggregate (100K rows) | 1.84 ms | 984 B | Range aggregate stays on index data |

### SQL Primary-Key Aggregate Spot Checks (March 14, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `MIN(id)` via table key aggregate (100K rows) | 330 ns | 504 B | First-row fast path on the table B-tree key |
| `MAX(id)` via table key aggregate (100K rows) | 341 ns | 552 B | Rightmost-key fast path on the table B-tree key |
| `COUNT(id)` via table key aggregate (100K rows) | 200 ns | 344 B | Reuses cached table row count; same semantics as `COUNT(*)` on integer PK |
| `SUM(id)` via table key aggregate (100K rows) | 3.47 ms | 505 B | Sums row keys without row payload decode |
| `COUNT(*) WHERE id BETWEEN ...` via table key aggregate (100K rows) | 1.42 ms | 744 B | Range aggregate stays on the table key stream |
| `SUM(id) WHERE id BETWEEN ...` via table key aggregate (100K rows) | 1.42 ms | 800 B | PK range aggregate without row fetch |

### SQL DISTINCT Aggregate Spot Checks (March 14, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `COUNT(DISTINCT value)` no index (100K rows) | 3.39 ms | 200.01 KB | Duplicate-heavy integer column with 1,024 distinct keys |
| `COUNT(DISTINCT value)` direct index aggregate (100K rows) | 41.07 us | 576 B | Counts unique integer index keys without row decode |
| `SUM(DISTINCT value)` no index (100K rows) | 3.47 ms | 200.01 KB | Full table distinct-set baseline |
| `SUM(DISTINCT value)` direct index aggregate (100K rows) | 38.69 us | 576 B | Sums unique integer index keys directly |
| `AVG(DISTINCT value)` no index (100K rows) | 4.11 ms | 200.01 KB | Full table distinct-set baseline |
| `AVG(DISTINCT value)` direct index aggregate (100K rows) | 38.85 us | 576 B | Computes distinct sum/count from index keys only |

### SQL Grouped Aggregate Spot Checks (March 14, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `GROUP BY group_id SELECT group_id, COUNT(*)` no index (100K rows) | 33.34 ms | 11.45 MB | Generic grouped hash aggregate over a duplicate-heavy integer key |
| `GROUP BY group_id SELECT group_id, COUNT(*)` direct index aggregate (100K rows) | 146.74 us | 102.98 KB | Streams distinct integer index keys and row-id payload counts without row decode |
| `GROUP BY group_id SELECT group_id, COUNT(*), SUM(group_id), AVG(group_id)` no index (100K rows) | 35.30 ms | 11.71 MB | Generic grouped hash aggregate with multiple scalar states per group |
| `GROUP BY group_id SELECT group_id, COUNT(*), SUM(group_id), AVG(group_id)` direct index aggregate (100K rows) | 160.43 us | 165.84 KB | Same grouped result computed directly from ordered index keys |
| `GROUP BY group_id WHERE group_id BETWEEN ... SELECT group_id, COUNT(*)` no index (100K rows) | 33.36 ms | 11.14 MB | Generic grouped aggregate still scans and groups the filtered input |
| `GROUP BY group_id WHERE group_id BETWEEN ... SELECT group_id, COUNT(*)` direct index aggregate (100K rows) | 76.36 us | 52.18 KB | Range-restricted grouped aggregate stays on the ordered integer index |
| `GROUP BY group_id ORDER BY group_id LIMIT 100 SELECT group_id, COUNT(*)` no index (100K rows) | 33.20 ms | 11.44 MB | Generic grouped aggregate still materializes, sorts, and then trims |
| `GROUP BY group_id ORDER BY group_id LIMIT 100 SELECT group_id, COUNT(*)` direct index aggregate (100K rows) | 15.96 us | 10.30 KB | Natural key order from the index lets the grouped path stop after the first 100 groups |
| `GROUP BY group_id WHERE group_id = ... HAVING COUNT(*) >= ... SELECT group_id, COUNT(*)` no index (100K rows) | 28.07 ms | 10.76 MB | Equality filter still scans the table, groups one key, and applies HAVING in the generic path |
| `GROUP BY group_id WHERE group_id = ... HAVING COUNT(*) >= ... SELECT group_id, COUNT(*)` direct index aggregate (100K rows) | 1.04 us | 1.41 KB | Equality-restricted grouped fast path now applies `HAVING COUNT(*)` directly from the index payload count |

### SQL Composite Grouped Aggregate Spot Checks (March 14, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `GROUP BY a, b SELECT a, b, COUNT(*)` no index (100K rows) | 45.17 ms | 22.64 MB | Generic grouped aggregate over the full composite key builds hash state from decoded rows |
| `GROUP BY a, b SELECT a, b, COUNT(*)` composite index aggregate (100K rows) | 3.01 ms | 3.65 MB | Streams hashed composite index payloads and aggregates directly from grouped key buckets |
| `GROUP BY a SELECT a, COUNT(*)` no index (100K rows) | 35.53 ms | 11.46 MB | Generic grouped aggregate over the leftmost composite key prefix |
| `GROUP BY a SELECT a, COUNT(*)` composite index prefix aggregate (100K rows) | 951.0 us | 2.09 MB | Leftmost-prefix grouping stays on the `(a, b)` index and avoids base-row decode |

### SQL Predicate Pushdown Spot Checks (March 14, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `WHERE value < 200000` (100K rows) | 15.16 ms | 6.06 MB | Single simple pre-decode predicate with about 20% selectivity |
| `WHERE value >= 10000 AND value < 20000` (100K rows) | 7.36 ms | 1.04 MB | Compound same-column range now pushes both bounds into pre-decode filtering |
| `WHERE category = 'Alpha' AND value < 200000` (100K rows) | 7.57 ms | 1.81 MB | Compound mixed text + integer predicate now pushes both conjuncts before row decode |

### SQL Scan Projection Spot Checks (March 15, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Filtered scan + column projection (10K rows, 20% selectivity) | 738.0 us | 252.56 KB | Generic projection batching now reaches the projection boundary too; small `10K` gain is modest, but the column path stays flat while the batch transport survives longer |
| Filtered scan + expression projection (10K rows, 20% selectivity) | 711.5 us | 426.74 KB | `FilterProjectionOperator` now keeps generic expression projections batch-backed instead of dropping to row transport immediately |
| Filtered scan + column projection (100K rows, 20% selectivity) | 57.09 ms | 24.24 MB | Large filtered scan improves once the generic `ProjectionOperator` preserves batch transport on the non-compact path |
| Filtered scan + expression projection (100K rows, 20% selectivity) | 57.76 ms | 24.95 MB | Same scan shape with batch transport carried through `FilterProjectionOperator`, cutting broad expression-projection overhead |

### SQL Batched Scan Root Spot Checks (March 15, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `SELECT *` full scan (100K rows) | 111.10 ms | 48.64 MB | First broader non-compact batch-backed root: plain `TableScanOperator` now feeds `QueryResult` through row batches instead of only row-by-row materialization |
| `SELECT * WHERE value < 200000` (100K rows) | 77.50 ms | 27.36 MB | Plain filtered scan now stays batch-backed through `FilterOperator` on the simple scan path |
| `SELECT * WHERE value < 10000` (100K rows) | 57.85 ms | 21.93 MB | Same batched scan root with a low-selectivity predicate |
| `SELECT * LIMIT 100` (100K rows) | 119.36 us | 97.50 KB | `LimitOperator` now preserves the scan batch root instead of forcing row-by-row materialization at the result boundary |

### SQL Join Projection Spot Checks (March 15, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Wide late-projection hash join (`1K x 1K`) | 372.8 us | 329.92 KB | Hash join trims both sides to join keys plus projected tail columns |
| Wide late-projection forced nested-loop join (`1K x 1K`) | 45.70 ms | 492.52 KB | Nested-loop path now trims decode too, but still remains far slower than hash join |
| Composite join `SELECT l.label, r.amount` lookup join (`1K x 1K`) | 514.3 us | 411.48 KB | New right-side composite/text lookup join path over hashed secondary indexes; still fetches base rows for non-covered right columns |
| Composite join `SELECT l.label, r.amount` forced hash (`1K x 1K`) | 478.9 us | 646.49 KB | Same join shape forced back to hash join; slightly faster here, but with materially higher allocation |
| Composite join `SELECT l.label, r.id, r.a, r.b` covered lookup (`1K x 1K`) | 432.6 us | 474.33 KB | Covered composite join now stays on index payloads for right PK and indexed key columns |
| Composite join `SELECT l.label, r.id, r.a, r.b` covered forced hash (`1K x 1K`) | 533.8 us | 709.36 KB | Same covered projection forced to hash join; slower and more allocation-heavy than the new covered lookup path |
| Join `SELECT l.id, r.amount + l.id` (`1K x 1K`) | 322.9 us | 568.09 KB | Generic expression-projection batching now reaches the join projection boundary too; latency improved slightly once `ProjectionOperator` became batch-capable |
| Join `SELECT l.id, r.amount + l.id WHERE r.amount > 2500` (`1K x 1K`) | 302.7 us | 517.52 KB | Same generic batching path with a residual join filter, now flowing through batch-capable `FilterProjectionOperator` |

### Focused Query-Engine Validation (March 12, 2026)

| Metric | Current Result | Notes |
|--------|----------------|-------|
| Parser simple SELECT | 120.4 ns | `ParserBenchmarks` |
| Parser complex SELECT | 2.50 us | JOIN + GROUP BY + HAVING + ORDER BY |
| Parser CTE (WITH clause) | 1.09 us | `ParserBenchmarks` |
| Query-plan cache stable SQL | 1.013 ms | Statement + plan cache hits |
| Query-plan cache pre-parsed | 1.003 ms | Plan cache hit |
| Query-plan cache varying SQL | 974 us | Limited plan reuse |
| Correlated scalar subquery filter (1K / 10K) | 3.061 ms / 29.292 ms | `COUNT(*)` guardrail |
| Correlated IN subquery filter (1K / 10K) | 1.440 ms / 16.344 ms | `COUNT(*)` guardrail, simple bound-probe fast path |
| Correlated NOT IN subquery filter (1K / 10K) | 1.515 ms / 17.848 ms | `COUNT(*)` guardrail, null-aware anti-semi bound-probe fast path |
| Correlated EXISTS subquery filter (1K / 10K) | 1.693 ms / 19.511 ms | `COUNT(*)` guardrail, simple bound-probe fast path |

### In-Memory Spot Checks

| Metric | Current Result | Notes |
|--------|----------------|-------|
| SQL insert (private engine in-memory) | 2.78 us | `Database.OpenInMemoryAsync` micro |
| Collection put (private engine in-memory) | 2.45 us | Direct payload collection write |
| SQL batch insert x100 (rotating in-memory) | ~1.67M rows/sec | Dedicated 10s run, resets the in-memory DB every 100K inserted rows |
| Collection batch put x100 (rotating in-memory) | ~1.07M docs/sec | Dedicated 10s run, resets the in-memory DB every 100K inserted docs |
| ADO.NET ExecuteScalar (`:memory:`) | 138 ns | Private connection-local in-memory DB |
| ADO.NET ExecuteScalar (`:memory:name`) | 226 ns | Named shared in-memory DB |
| ADO.NET insert (`:memory:`) | 2.16 us | Private connection-local in-memory DB |
| ADO.NET insert (`:memory:name`) | 2.18 us | Named shared in-memory DB |
| Load SQL DB + WAL into memory | 1.22 ms | `Database.LoadIntoMemoryAsync` |
| Load collection DB + WAL into memory | 1.39 ms | `Database.LoadIntoMemoryAsync` |
| Save in-memory SQL snapshot to disk | 2.72 ms | `Database.SaveToFileAsync` |
| Save in-memory collection snapshot to disk | 3.19 ms | `Database.SaveToFileAsync` |

### Cold / Cache-Pressured Lookup Spot Checks

These runs use a 200K-row working set with `MaxCachedPages = 16` and randomized lookup probes so the storage path is exercised instead of a single warmed page.

| Metric | Current Result | Notes |
|--------|----------------|-------|
| SQL cold lookup (file-backed) | 482 ns | Cache-pressured primary-key lookup |
| SQL cold lookup (in-memory) | 455 ns | Same workload after `LoadIntoMemoryAsync` |
| Collection cold get (file-backed) | 745 ns | Cache-pressured direct collection lookup |
| Collection cold get (in-memory) | 475 ns | Same workload after `LoadIntoMemoryAsync` |

### Indexed Lookup / Tuning Spot Checks

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Collection `FindByIndex` int equality (1 match) | 1.51 us | 1.21 KB | Direct integer-key index probe |
| Collection `FindByIndex` text equality (1 match) | 1.77 us | 1.27 KB | Raw-payload text verification before document materialization |
| Collection `PutAsync` with secondary indexes (insert) | 5.17 us | 25.00 KB | Transaction + rollback micro for write maintenance |
| Collection `PutAsync` with secondary indexes (update) | 15.22 us | 32.06 KB | Includes old-entry removal plus reinsert |
| Collection `DeleteAsync` with secondary indexes | 14.27 us | 24.62 KB | Transaction + rollback micro for delete-side cleanup |

### File-Backed Lookup Tuning Takeaways

- `MaxCachedPages = 2048` was the best collection setting in the current tuning matrix: indexed collection lookup fell from `66.65 us` at 16 pages to `24.58 us` at 2048 pages.
- `UseCachingBTreeIndexes` was neutral-to-negative on these lookup workloads. The worst regressions showed up on SQL reader-session paths, so it is not the recommended default tuning knob.
- The older reader-session setup penalty has largely been removed. In the latest dedicated `ReaderSessionBenchmarks`, simple point lookups measured `649.83 ns` with per-query sessions, `617.39 ns` with a reused session, and `582.32 ns` with direct `ExecuteAsync`.
- A small dedicated WAL read cache also helps once the hottest pages live in the WAL instead of the main file: the WAL-backed SQL cold-lookup micro dropped from `25.64 us` to `17.80 us` when `MaxCachedWalReadPages = 128`.
- Raw storage traversal benefits from speculative next-leaf reads too: on the direct file-backed `BTreeCursor` micro, full scans improved from `9.12 ms` to `7.86 ms` at `10K` rows and from `88.88 ms` to `80.58 ms` at `100K`, while the `1024`-row seek window improved from about `928 us` to `799 us` at `10K` and from `900 us` to `797 us` at `100K`.
- Recommended file-backed read-heavy preset: `builder.UseLookupOptimizedPreset()` now combines the `2048`-page cache setting with a bounded WAL read cache (`MaxCachedWalReadPages = 256`) and opt-in memory-mapped reads for clean main-file pages; still reuse a `ReaderSession` for bursts of related SQL reads.

### File-Backed Durable Write Tuning Takeaways

- In the March 10, 2026 `write-diagnostics` median-of-3 run after the final revert and checkpoint cleanup, `FrameCount(4096)+Background(64 pages/step)` was the best measured write-heavy variant at `31.96K ops/sec`. `Background(256)` followed at `31.95K`, foreground `FrameCount(4096)` measured `31.73K`, and `WalSize(8 MiB)` measured `30.28K`.
- Background auto-checkpointing still does not make checkpoints cheaper. It moves them off the triggering commit. In the same median run, foreground `FrameCount(4096)` had `236` commits that paid checkpoint cost, while the `64`-page and `256`-page background variants had `0`.
- Smaller slices still reduce per-checkpoint task time but are no longer the throughput winner. At `16` pages/step, average checkpoint time fell to about `2.09 ms`, but throughput landed lower at `29.24K ops/sec` because more background steps were required.
- The non-checkpoint commit path is now much cleaner than the earlier checkpoint phase. The two changes that mattered were deferring DB flushes until checkpoint completion and rebuilding retained WAL index state from the copied bytes instead of rescanning the retained suffix after compaction.
- Higher frame-count thresholds still help by making checkpoints less frequent, and background sliced scheduling helps by keeping almost all of that work off the write call that triggered it.
- Recommended file-backed write-heavy preset: `builder.UseWriteOptimizedPreset()`. This is opt-in and does not change the engine default checkpoint policy.

## Competitor Comparison

The master table below now separates CSharpDB file-backed runs from in-memory runs.

- File-backed single-write, batched-write, and concurrent-reader numbers were refreshed on March 14, 2026 from the balanced `macro-20260314-214358.csv` capture.
- CSharpDB SQL concurrent reads are shown as `per-query sessions / reused reader sessions (x32 reads per snapshot)` because those patterns measure materially different setup costs.
- In-memory batched-write numbers were refreshed on March 12, 2026 from isolated `InMemoryBatchBenchmark` runs and use a rotating reset-after-100K-rows harness to keep the working set bounded.
- Point-lookup numbers in the master table were refreshed on March 14, 2026 from `ColdLookupBenchmarks-report.csv`.
- Hot-cache lookup numbers are still useful, but they are reported in the micro sections above instead of the master table because they collapse the storage difference once pages are warmed.
- Ordered/range covered-scan numbers were refreshed on March 14, 2026 from `OrderByIndexBenchmarks`, but they stay in the micro sections because the master table tracks durable writes, cold point lookups, and concurrent-read throughput rather than scan-shape throughput.
- Indexed aggregate numbers were refreshed on March 14, 2026 from `IndexAggregateBenchmarks`, but they stay in the micro sections because the master table does not currently have an aggregate column.
- Primary-key aggregate numbers were refreshed on March 14, 2026 from `PrimaryKeyAggregateBenchmarks`, and they also stay in the micro sections for the same reason.
- In-memory single-write numbers were refreshed on March 10, 2026 from the `InMemory*Benchmarks` micro suites, and in-memory point-lookup numbers were refreshed on March 14, 2026 from `ColdLookupBenchmarks-report.csv`.
- In-memory concurrent-reader cells are left as `N/A` where an apples-to-apples dedicated benchmark has not been added yet.
- Competitor figures are still approximate ranges from published third-party sources on comparable hardware.

### Master Comparison Table

| Database | Language | Type | Single INSERT | Batched INSERT | Point Lookup | Concurrent Reads |
|----------|----------|------|---------------|----------------|--------------|------------------|
| **CSharpDB SQL (file-backed)** | **C#** | **Relational SQL** | **27.0K ops/sec** | **~695K rows/sec** | **~2.07M ops/sec** | **~912K / ~10.70M COUNT(*) ops/sec (8r, per-query / reused x32)** |
| **CSharpDB SQL (in-memory)** | **C#** | **Relational SQL** | **~360K ops/sec** | **~1.67M rows/sec** | **~2.20M ops/sec** | **N/A** |
| **CSharpDB Collection (file-backed)** | **C#** | **Document (NoSQL)** | **32.0K ops/sec** | **~425K docs/sec** | **~1.34M ops/sec** | **-** |
| **CSharpDB Collection (in-memory)** | **C#** | **Document (NoSQL)** | **~408K ops/sec** | **~1.07M docs/sec** | **~2.11M ops/sec** | **-** |
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
| CSharpDB SQL (file-backed) | ~4.16M ops/sec |
| CSharpDB SQL (in-memory) | ~4.21M ops/sec |
| CSharpDB Collection (file-backed) | ~2.63M ops/sec |
| CSharpDB Collection (in-memory) | ~2.79M ops/sec |

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
