# CSharpDB Benchmark Suite

Performance benchmarks for the CSharpDB embedded database engine.

The current top-level snapshot in this README centers on the March 30, 2026 `main` validation refresh: one full `--all` sweep, one same-day focused baseline snapshot, and same-day `median-of-3` reruns for the durable write guardrails. A release validation rerun was completed on April 2, 2026 against that March 30 focused baseline and finished clean at `PASS=184, FAIL=0`. Archived competitor ranges and older spot-check notes are still called out inline where they remain useful.

## Latest Validation Snapshot

The latest release guardrail rerun completed on April 2, 2026 with a clean compare report:

```powershell
pwsh -NoProfile .\tests\CSharpDB.Benchmarks\scripts\Compare-Baseline.ps1 `
  -ThresholdsPath .\tests\CSharpDB.Benchmarks\perf-thresholds.json `
  -ReportPath .\tests\CSharpDB.Benchmarks\results\perf-guardrails-last.md
```

| Item | Result |
|------|--------|
| Release guardrail report | `tests/CSharpDB.Benchmarks/results/perf-guardrails-last.md` |
| Release guardrail result | `Compared 184 rows against baseline. PASS=184, WARN=0, SKIP=0, FAIL=0` |
| Baseline used by release rerun | `tests/CSharpDB.Benchmarks/baselines/focused-validation/20260330-122507` |
| Release rerun note | `BenchmarkDotNet class refreshes were run sequentially before compare to avoid shared job-directory collisions between concurrent filtered runs.` |

The current focused validation baseline was refreshed on March 30, 2026 with:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --all
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --write-diagnostics --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --durable-sql-batching --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --concurrent-write-diagnostics --repeat 3 --repro
```

| Item | Result |
|------|--------|
| Baseline snapshot | `tests/CSharpDB.Benchmarks/baselines/focused-validation/20260330-122507` |
| PR baseline snapshot | `tests/CSharpDB.Benchmarks/baselines/pr-validation/20260330-233433` |
| Broad main sweep | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/macro-20260330-081102.csv` |
| Direct transport sweep | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/direct-file-cache-transport-20260330-081709.csv` |
| Hybrid storage sweep | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-storage-mode-20260330-082134.csv` |
| Hybrid cold-open sweep | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-cold-open-20260330-082420.csv` |
| Hybrid hot-set sweep | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-hot-set-read-20260330-082434.csv` |
| Hybrid post-checkpoint sweep | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-post-checkpoint-20260330-082443.csv` |
| Durable batching artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-20260330-122006-median-of-3.csv` |
| Durable write artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/write-diagnostics-20260330-121058-median-of-3.csv` |
| Concurrent durable write artifact | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/concurrent-write-diagnostics-20260330-122507-median-of-3.csv` |
| Broad micro sweep | `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.*-report.csv` |

High-level takeaways from the March 30 refresh:

- The focused validation baseline now points to a single coherent March 30 snapshot built on `main`.
- The PR lane now has its own checked-in March 30 snapshot under `baselines/pr-validation/20260330-233433`.
- The top-level SQL, collection, storage-mode, and master comparison tables below now use March 30 durable artifacts from the same run family.
- The durable write guardrails now anchor on same-day `median-of-3` captures for `write-diagnostics`, `durable-sql-batching`, and `concurrent-write-diagnostics`.
- The broad micro sweep in `BenchmarkDotNet.Artifacts/results` was also refreshed on March 30, 2026.

- `Focused validation baseline refresh on March 30, 2026: tests/CSharpDB.Benchmarks/baselines/focused-validation/20260330-122507`
- `Broad main sweep on March 30, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/macro-20260330-081102.csv`
- `Direct client transport sweep on March 30, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/direct-file-cache-transport-20260330-081709.csv`
- `Hybrid storage-mode sweep on March 30, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-storage-mode-20260330-082134.csv`
- `Hybrid cold-open sweep on March 30, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-cold-open-20260330-082420.csv`
- `Hybrid hot-set sweep on March 30, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-hot-set-read-20260330-082434.csv`
- `Hybrid post-checkpoint sweep on March 30, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-post-checkpoint-20260330-082443.csv`
- `Durable SQL batching median-of-3 capture on March 30, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-20260330-122006-median-of-3.csv`
- `Durable write median-of-3 capture on March 30, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/write-diagnostics-20260330-121058-median-of-3.csv`
- `Concurrent durable write median-of-3 capture on March 30, 2026: tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/concurrent-write-diagnostics-20260330-122507-median-of-3.csv`
- `Broad micro sweep on March 30, 2026: BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.*-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InsertBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.PointLookupBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ReaderSessionBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.MemoryMappedReadBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.WalReadCacheBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.BTreeCursorBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.OrderByIndexBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ScanBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ScanProjectionBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.DistinctBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ColdLookupBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.CollectionFieldExtractionBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.CollectionAccessBenchmarks-report.csv`
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
| Runtime | .NET 10.0.5, X64 RyuJIT AVX-512F |
| Disk | NVMe SSD |
| Page Size | 4,096 bytes |
| WAL Mode | Enabled (redo-log with auto-checkpoint at 1,000 frames) |
| Page Cache | LRU page cache (in-memory) |
| WAL Index | Hash map (O(1) page lookup) |
| Benchmark Mode | March 30 top-level snapshot uses a broad `main` sweep plus same-day focused durable reruns |

## Running Benchmarks

```bash
# Fast PR guardrail pass (dedicated guardrail classes only)
dotnet run -c Release -- --pr

# Focused release/guardrail pass (recommended for normal validation)
dotnet run -c Release -- --release

# Micro-benchmarks (BenchmarkDotNet, original full micro surface)
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
dotnet run -c Release -- --micro --filter *CollationIndexBenchmarks*
dotnet run -c Release -- --micro --filter *IndexAggregateBenchmarks*
dotnet run -c Release -- --micro --filter *PrimaryKeyAggregateBenchmarks*
dotnet run -c Release -- --micro --filter *DistinctAggregateBenchmarks*
dotnet run -c Release -- --micro --filter *GroupedIndexAggregateBenchmarks*
dotnet run -c Release -- --micro --filter *CompositeGroupedIndexBenchmarks*
dotnet run -c Release -- --micro --filter *PredicatePushdownBenchmarks*
dotnet run -c Release -- --micro --filter *ScanBenchmarks*
dotnet run -c Release -- --micro --filter *ScanProjectionBenchmarks*
dotnet run -c Release -- --micro --filter *ScalarAggregateBenchmarks*
dotnet run -c Release -- --micro --filter *DistinctBenchmarks*
dotnet run -c Release -- --micro --filter *JoinBenchmarks*
dotnet run -c Release -- --micro --filter *CollectionLookupFallbackBenchmarks*

# Run the focused phase-1 baseline set
pwsh ./tests/CSharpDB.Benchmarks/scripts/Run-Phase1-Baselines.ps1

# Stable macro snapshot (median-of-3, reproducible mode)
dotnet run -c Release -- --macro --repeat 3 --repro

# Durable write variance diagnostics
dotnet run -c Release -- --write-diagnostics --repeat 3 --repro

# Durable SQL batching diagnostics
dotnet run -c Release -- --durable-sql-batching --repeat 3 --repro

# Concurrent durable write diagnostics
dotnet run -c Release -- --concurrent-write-diagnostics --repeat 3 --repro

# Focused direct hybrid transport comparison (no gRPC)
dotnet run -c Release -- --direct-file-cache-transport --repeat 3 --repro

# Focused hot steady-state comparison for file-backed vs in-memory vs lazy-resident incremental-durable hybrid
dotnet run -c Release -- --hybrid-storage-mode --repeat 3 --repro

# Focused engine-cold open + first read comparison for file-backed vs in-memory vs lazy-resident incremental-durable hybrid
dotnet run -c Release -- --hybrid-cold-open --repeat 3 --repro

# Focused post-open hot-set read comparison including hybrid warm-set mode
dotnet run -c Release -- --hybrid-hot-set-read --repeat 3 --repro

# Focused post-checkpoint hot reread comparison for file-backed vs in-memory vs lazy-resident incremental-durable hybrid
dotnet run -c Release -- --hybrid-post-checkpoint --repeat 3 --repro

# In-memory rotating batch throughput
dotnet run -c Release -- --macro-batch-memory

# Stress and scaling suites
dotnet run -c Release -- --stress
dotnet run -c Release -- --scaling

# Exhaustive full sweep (includes the original full micro suite set; very slow)
dotnet run -c Release -- --all
```

Results are written to `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/` and `BenchmarkDotNet.Artifacts/results/`.

### Concurrent Durable Write Methodology

- `ConcurrentDurableWriteBenchmark` is an in-process contention benchmark, not a multi-process transport benchmark.
- Each scenario launches `4` or `8` parallel writer `Task`s against one shared `Database` instance and one shared WAL/pager.
- All writers are released together through a start gate, then each task loops on auto-commit `INSERT` statements for the timed phase.
- `Ops/sec` in the concurrent durability CSVs is total successful commits per second across all writers combined, not per-writer throughput.
- The high shared-engine throughput rows therefore belong to the `8`-writer scenarios in `concurrent-write-diagnostics-*.csv`, while the much lower single-writer rows belong to the separate `write-diagnostics-*.csv` and `durable-sql-batching-*.csv` harnesses.

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
- `DurableSqlBatchingBenchmark`: file-backed durable SQL ingest sweep covering auto-commit single-row inserts, analyzed-table single-row inserts, and explicit transaction batches of `10`, `100`, and `1000` rows with commit-path stage diagnostics
- `ConcurrentDurableWriteBenchmark`: shared-database multi-writer auto-commit sweep where `4` or `8` in-process writer tasks hit one shared `Database` instance for durable batch-window tuning under actual writer contention
- `InMemoryBatchBenchmark`: rotating x100 batch throughput for in-memory SQL and collections
- `InMemoryWorkloadBenchmark`: macro mixed workloads for SQL and collections in memory vs file-backed
- `SharedMemoryAdoNetBenchmark`: named shared-memory reader/writer contention through the provider host layer
- `InMemoryPersistenceBenchmark`: macro load/save latency and output-size reporting
- `DirectFileCacheTransportBenchmark`: macro-style direct-client comparison on the same larger file-backed dataset so direct pager/file-cache tuning can be measured without gRPC transport overhead
- `HybridStorageModeBenchmark`: macro-style hot steady-state comparison between file-backed, pure in-memory, and the lazy-resident hybrid mode with incremental durable commits
- `HybridColdOpenBenchmark`: engine-cold open + first SQL lookup / collection get comparison across file-backed, pure in-memory load, and lazy-resident incremental-durable hybrid on a larger seeded database
- `HybridHotSetReadBenchmark`: post-open hot-burst SQL lookups and collection gets that isolate the hybrid warm-set hint from generic lazy hybrid behavior while excluding open cost from the throughput denominator
- `HybridPostCheckpointBenchmark`: identical post-checkpoint hot reread comparison across file-backed, pure in-memory, and lazy-resident hybrid after one auto-checkpointed write per burst

### Phase 1 Baseline Suites

- `SqlMaterializationBenchmarks`: isolates full-row decode, selected-column decode, single-column access, and payload-level text/numeric checks
- `CollectionAccessBenchmarks`: isolates full document hydration, key-only access, key matching, and indexed-field reads over collection payloads
- `MemoryMappedReadBenchmarks`: isolates file-backed SQL and collection cold lookups so the opt-in `mmap` read path can be compared directly against copy-based reads
- `WalReadCacheBenchmarks`: isolates file-backed SQL cold lookups where the latest table pages remain WAL-backed so the dedicated WAL read cache can be compared directly against fresh WAL stream reads
- `BTreeCursorBenchmarks`: isolates raw forward cursor traversal and seek+window scans over a B+tree so sequential leaf-scan changes can be measured without the SQL executor on top
- `CoveringIndexBenchmarks`: isolates unique-index lookup shapes that could become index-only from shapes that still need the wide base-row payload
- `IndexProjectionBenchmarks`: isolates non-unique secondary-index lookups where `SELECT id` or `SELECT indexed_col` can now avoid base-row fetches
- `OrderByIndexBenchmarks`: isolates indexed `ORDER BY`, covered integer range scans, and compact non-covered range projection shapes, including residual-filter batch-plan variants where indexed filtering still avoids full row materialization
- `CollationIndexBenchmarks`: isolates ordered SQL text-index equality lookup, range scan, top-N `ORDER BY`, and indexed write-maintenance cost under `BINARY`, `NOCASE`, `NOCASE_AI`, and `ICU:<locale>`
- `ScanProjectionBenchmarks`: isolates compact table scans and LIMIT-forced generic scans where scan-heavy filter/projection shapes now stay on the internal row-batch transport path
- `IndexAggregateBenchmarks`: isolates scalar `SUM` / `COUNT` / `MIN` / `MAX` queries and range aggregates that can now execute directly from integer index keys
- `PrimaryKeyAggregateBenchmarks`: isolates scalar and ranged aggregates that can now execute directly from the `INTEGER PRIMARY KEY` table B-tree key stream
- `GroupedIndexAggregateBenchmarks`: isolates `GROUP BY` on a duplicate-heavy integer key so grouped aggregates can be compared against the new direct index-grouped fast path
- `CompositeGroupedIndexBenchmarks`: isolates `GROUP BY` on composite indexed keys and leftmost-prefix grouped scans so composite grouped fast paths can be compared against generic grouped scans
- `CollectionFieldExtractionBenchmarks`: isolates early/middle/late extraction cost, nested-path access, miss cost, and full document hydration comparison for collection payload scans
- `CollectionLookupFallbackBenchmarks`: isolates collection equality lookups on unindexed fields to measure the direct-payload compare fallback before full document hydration
- `Run-Phase1-Baselines.ps1`: runs the focused phase-1 benchmark set without the larger macro, stress, or scaling suites
- `CollectionFieldExtractionBenchmarks`, `CollectionPayloadBenchmarks`, and `CollectionAccessBenchmarks` use an in-process BenchmarkDotNet toolchain on this machine because that was the smallest stable fix for local child-job generation/runtime issues

### Baselines and Guardrails

```bash
# Capture a fresh baseline snapshot
pwsh ./tests/CSharpDB.Benchmarks/scripts/Capture-Baseline.ps1

# Run the fast PR guardrails
pwsh ./tests/CSharpDB.Benchmarks/scripts/Run-Perf-Guardrails.ps1 -Mode pr

# Run the fuller release guardrails
pwsh ./tests/CSharpDB.Benchmarks/scripts/Run-Perf-Guardrails.ps1 -Mode release
```

Defaults:

- Baseline snapshot: `tests/CSharpDB.Benchmarks/baselines/focused-validation/20260330-122507`
- PR baseline snapshot: `tests/CSharpDB.Benchmarks/baselines/pr-validation/20260330-233433`
- Threshold config: `tests/CSharpDB.Benchmarks/perf-thresholds.json`
- PR threshold config: `tests/CSharpDB.Benchmarks/perf-thresholds-pr.json`
- Last guardrail report: `tests/CSharpDB.Benchmarks/results/perf-guardrails-last.md`
- `Capture-Baseline.ps1` runs non-micro suites in reproducible mode by default and captures macro results as `--macro --repeat 3 --repro`.
- The focused guardrail set now stages stable durability CSVs from `--write-diagnostics --repeat 3 --repro`, `--durable-sql-batching --repeat 3 --repro`, and `--concurrent-write-diagnostics --repeat 3 --repro` into `macro-stress-scaling/write-diagnostics-median-of-3.csv`, `macro-stress-scaling/durable-sql-batching-median-of-3.csv`, and `macro-stress-scaling/concurrent-write-diagnostics-median-of-3.csv`.
- `--pr` reads `perf-thresholds-pr.json` and runs only the dedicated PR guardrail classes. It skips the repeat-3 durability suites and the specialized read suites that stay release-only.
- `--release` reads `perf-thresholds.json` and runs the existing tracked micro filters plus the tracked non-micro guardrail suites, sequentially.
- `--micro` and `--all` keep the original full micro benchmark surface; they do not automatically add the PR guardrail duplicates unless you explicitly filter for `*GuardrailBenchmarks*`.
- The focused validation baseline snapshot under `tests/CSharpDB.Benchmarks/baselines/focused-validation/20260330-122507` is checked in and now carries the tracked micro guardrail CSVs plus the staged durable median-of-3 CSVs, so fresh clones can run the current guardrail set without first rebuilding older focused baseline snapshots.
- The checked-in PR snapshot under `tests/CSharpDB.Benchmarks/baselines/pr-validation/20260330-233433` carries only the dedicated PR guardrail CSVs so PR validation stays lane-specific and fast.
- That focused validation snapshot also tracks `CSharpDB.Benchmarks.Micro.CollationIndexBenchmarks-report.csv` so ordered-text collation regressions can be checked alongside the existing micro suites.
- Baseline snapshots now include a `machine.json` fingerprint sidecar. `Run-Perf-Guardrails.ps1` stays strict on a matching perf runner or same-machine fingerprint, downgrades regressions to warnings on compatible hardware/runtime, and skips regression enforcement on materially different machines.
- Set `CSHARPDB_PERF_RUNNER_ID` on the canonical perf runner before capturing a baseline if you want strict regression failures to be limited to that designated machine.
- The focused durability checks compare `Mean` against the stable median-of-3 durable rows only. Allocation comparison is intentionally skipped for those checks because the diagnostics CSVs emit raw millisecond values, not BenchmarkDotNet `Allocated` columns.

## Current Performance Snapshot

The API snapshot, hot steady-state tables, and master comparison table below use the March 30, 2026 `main` artifacts listed above. CSharpDB values are durable-only unless a note says otherwise.

### SQL API (March 30, 2026 broad main sweep)

| Metric | Value | Notes |
|--------|-------|-------|
| Single INSERT | 280.7 ops/sec | Auto-commit write |
| Batch 100 rows/tx | ~26.32K rows/sec | 263.2 tx/sec x 100 rows |
| Point lookup (10K rows) | 1.41M ops/sec | `Comparison_SQL_PointLookup_10k` |
| Mixed workload reads | 1,079.1 ops/sec | 80/20 read/write mix |
| Mixed workload writes | 276.1 ops/sec | 80/20 read/write mix |
| Reader throughput (8 readers, per-query sessions) | 526.89K ops/sec | Total `COUNT(*)` queries/sec across 8 readers |
| Reader throughput (8 readers, reused snapshots x32) | 9.70M ops/sec | `ReaderScalingBurst32_8readers_Readers` |
| Writer throughput under 8 readers | 266.0 ops/sec | `ReaderScaling_8readers_Writer` |
| Checkpoint time (1,000 WAL frames) | 8.38 ms | `CheckpointLoad_1000frames_CheckpointTime` |

### Collection API (March 30, 2026 broad main sweep)

| Metric | Value | Notes |
|--------|-------|-------|
| Single Put | 285.0 ops/sec | Auto-commit document write |
| Batch 100 docs/tx | ~26.15K docs/sec | 261.5 tx/sec x 100 docs |
| Point Get (10K docs) | 1.93M ops/sec | Direct collection lookup |
| Mixed workload reads | 1,071.6 ops/sec | 80/20 read/write mix |
| Mixed workload writes | 274.2 ops/sec | 80/20 read/write mix |
| Full Scan (1K docs) | 4,515.0 scans/sec | Full collection scan |
| Filtered Find (1K docs, 20% match) | 4,358.1 scans/sec | Predicate evaluation path |
| Indexed equality lookup (10K docs) | 659.79K ops/sec | `Collection_FindByIndex_Value_10k_15s` |
| Single Put (with 1 secondary index) | 270.0 ops/sec | `Collection_Put_Single_WithIndex_15s` |

### Collection Path Micro Spot Checks

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Collection encode (direct payload) | 198.7 ns | 552 B | Current versioned binary direct-payload path |
| Collection encode (legacy row format) | 280.7 ns | 304 B | Prior `DbValue[]` + record serializer path |
| Collection decode (direct payload) | 210.1 ns | 328 B | Current binary direct-payload path with direct header reuse |
| Collection decode (legacy row format) | 444.6 ns | 600 B | Prior `DbValue[]` + record serializer path |
| Collection put (minimal schema, in-memory) | 2.733 us | 1.28 KB | Auto-commit write with only the target collection loaded |
| Collection put (48 extra tables + 48 extra collections, in-memory) | 2.760 us | 1.25 KB | Unrelated schema breadth still does not add measurable write tax |

### WAL Core Micro Spot Checks (March 26, 2026)

Source: `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.WalCoreBenchmarks-report.csv`

| Metric | 100 frames before checkpoint | 500 frames before checkpoint | 1000 frames before checkpoint | Notes |
|--------|-----------------------------:|-----------------------------:|------------------------------:|-------|
| WAL core: 100-frame batch commit | 10.803 ms | 10.301 ms | 10.613 ms | Direct `AppendFramesAndCommitAsync(...)` path |
| WAL core: 100-frame staged commit | 10.584 ms | 10.717 ms | 10.477 ms | Repeated `AppendFrameAsync(...)` calls staged and emitted at `CommitAsync(...)` |
| WAL core: single-frame commit | 4.202 ms | 4.297 ms | 4.450 ms | One page + one durable commit |
| WAL core: manual checkpoint after N frames | 20.449 ms | 44.536 ms | 85.726 ms | Commit N single-page appends, then checkpoint |

The important result for async I/O batching is still the staged-vs-direct comparison: the repeated-`AppendFrameAsync(...)` path stays close to the direct batched WAL path instead of collapsing into one durable write per page before commit, while manual checkpoint cost rises sharply as the frame threshold grows.

### Collection Path Index Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Collection `FindByIndex` nested path equality (`$.address.city`) | 579.9 ns | 1080 B | Public string-path index lookup over a nested scalar path with many matches |
| Collection `FindByPath` nested path equality (`$.address.city`) | 568.9 ns | 1080 B | Query-facing path API running on the same nested scalar index |
| Collection `FindByIndex` array path equality (`$.tags[]`) | 481.4 ns | 912 B | Multi-value array element lookup over a public string-path collection index |
| Collection `FindByPath` array path equality (`$.tags[]`) | 474.8 ns | 912 B | Query-facing path API running on the same array index |
| Collection `FindByPath` nested array path equality (`$.orders[].sku`) | 599.3 ns | 1160 B | Query-facing path API over an index on scalar fields inside array elements |
| Collection `FindByPath` integer range (`Value`, 1024 matches) | 552.79 us | 314548 B | Ordered integer path range over the collection index path/query surface |
| Collection `FindByPath` text range (`Tag`, 1000 matches) | 545.69 us | 460571 B | Ordered text path range over prefix-bucket text indexes with exact in-bucket filtering |
| Collection `FindByPath` Guid equality (`SessionId`) | 595.3 ns | 1123 B | Canonical `Guid` path lookup over the ordered text collection index path/query surface |
| Collection `FindByPath` DateOnly range (`EventDate`, 1000 matches) | 264.79 us | 207888 B | Canonical `DateOnly` range over ordered text collection indexes using fixed-width ISO keys |

### Collection Extraction Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Collection field read (early field) | 65.40 ns | 112 B | Direct binary payload scan near the front of the document |
| Collection field read (middle field) | 76.72 ns | 64 B | Unbound middle-field integer read |
| Collection field read (late field) | 121.01 ns | 64 B | Unbound late-field integer read |
| Collection field compare (late text field, bound accessor) | 127.38 ns | 0 B | Bound accessor text compare stays allocation-free |
| Collection field read (middle field, bound accessor) | 56.72 ns | 0 B | Bound accessor integer extraction on the binary payload path |
| Collection field read (nested path, bound accessor) | 214.10 ns | 40 B | Nested document walk without hydrating `T` |
| Collection hydrate document (comparison) | 517.23 ns | 912 B | Full typed hydration on the new binary direct-payload path |

### Query Micro Spot Checks

| Metric | Mean | Allocated |
|--------|------|-----------|
| SQL PK lookup (10K rows) | 548.0 ns | 768 B |
| SQL PK lookup (100K rows) | 802.9 ns | 768 B |
| SQL indexed lookup (100K rows) | 2,450.7 ns | 17185 B |
| SQL point miss (100K rows) | 366.3 ns | 464 B |

### Reader Session Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `COUNT(*)` with per-query reader sessions | 1,035.5 ns | 7528 B | Full reader-session create/execute/dispose path |
| `COUNT(*)` with reused reader session | 105.7 ns | 321 B | Same query with a reused snapshot session |
| Point lookup with per-query reader sessions | 1,703.7 ns | 7799 B | Reader-session setup is materially higher than direct execute on the current path |
| Point lookup with reused reader session | 680.9 ns | 592 B | Small remaining gap versus direct execution |
| Point lookup with direct `ExecuteAsync` | 663.6 ns | 640 B | Lower bound for the same simple PK read path |

### Memory-Mapped Read Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| SQL cold lookup, copy-based read path | 28.704 us | 9707 B | File-backed cache-pressured lookup with `UseMemoryMappedReads = false` |
| SQL cold lookup, mmap read path | 1.608 us | 784 B | Same workload with clean main-file pages served from mapped read views |
| Collection cold get, copy-based read path | 29.286 us | 9373 B | File-backed cache-pressured collection lookup with `UseMemoryMappedReads = false` |
| Collection cold get, mmap read path | 1.252 us | 409 B | Same workload with mapped main-file reads and copy-on-write only on mutable access |

### WAL Read Cache Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| SQL cold lookup, WAL-backed, no WAL cache | 28.74 us | 8.68 KB | File-backed cache-pressured lookup where the latest table pages are still read from WAL frames |
| SQL cold lookup, WAL-backed, 128-page WAL cache | 19.83 us | 6.27 KB | Same workload with `MaxCachedWalReadPages = 128` so immutable WAL frame images can be reused between reads |

### B-Tree Cursor Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| B-tree cursor full scan (10K rows, read-ahead off) | 9.25 ms | 3.34 MB | File-backed raw forward scan with `EnableSequentialLeafReadAhead = false` |
| B-tree cursor full scan (10K rows, read-ahead on) | 8.51 ms | 3.26 MB | Same scan with speculative next-leaf reads enabled |
| B-tree cursor seek + 1024-row window (10K rows, read-ahead off) | 919.2 us | 359.98 KB | Mid-tree seek followed by sequential leaf traversal |
| B-tree cursor seek + 1024-row window (10K rows, read-ahead on) | 822.2 us | 346.56 KB | Same seek-window path with speculative next-leaf reads |
| B-tree cursor full scan (100K rows, read-ahead off) | 92.74 ms | 33.46 MB | File-backed forward scan across a deeper leaf chain |
| B-tree cursor full scan (100K rows, read-ahead on) | 87.41 ms | 32.69 MB | Same scan with speculative next-leaf reads enabled |
| B-tree cursor seek + 1024-row window (100K rows, read-ahead off) | 945.7 us | 359.98 KB | Mid-tree seek followed by a bounded sequential window |
| B-tree cursor seek + 1024-row window (100K rows, read-ahead on) | 824.7 us | 346.54 KB | Seek-window path with speculative next-leaf reads; latency stays roughly flat as the tree grows |

### SQL Covered Read-Path Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Unique index lookup `SELECT *` (100K rows) | 5.963 us | 19.47 KB | Baseline unique secondary-index lookup |
| Unique index lookup `SELECT id` (100K rows) | 2.374 us | 9.92 KB | Covered projection from index payload |
| Unique index lookup `SELECT lookup_key` (100K rows) | 2.870 us | 9.88 KB | Covered projection from index payload |
| Non-unique index lookup `SELECT *` (100K rows) | 148.913 us | 83.00 KB | Baseline duplicate-key secondary-index lookup |
| Non-unique index lookup `SELECT id` (100K rows) | 19.163 us | 34.61 KB | Covered projection drops most row materialization cost |
| `ORDER BY value` no index (100K rows) | 160.914 ms | 63.53 MB | Full sort baseline from the latest indexed-order rerun |
| `ORDER BY value` covered index-order scan (100K rows) | 33.637 ms | 14.58 MB | `SELECT id, value` stays on index data |
| `ORDER BY value LIMIT 100` index-order scan (100K rows) | 63.45 us | 83.22 KB | Index order avoids sort, still fetches base rows |
| `ORDER BY value LIMIT 100` covered index-order scan (100K rows) | 21.78 us | 40.75 KB | Index-only top-N path |
| `WHERE value BETWEEN ...` row fetch (100K rows) | 52.728 ms | 16.45 MB | Integer range scan with base-row fetch |
| `WHERE value BETWEEN ...` covered projection (100K rows) | 16.619 ms | 7.30 MB | Integer range scan that stays on index data |
| `WHERE value BETWEEN ... SELECT id, category` compact projection (100K rows) | 38.913 ms | 7.30 MB | Non-covered indexed range scan decodes only projected payload columns instead of wide rows |
| `WHERE value BETWEEN ... SELECT id, value + id` compact expression projection (100K rows) | 39.049 ms | 7.30 MB | Indexed range scan keeps the compact payload decode path even when projection includes an expression |

### SQL Composite Equality Lookup Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `WHERE a = ... AND b = ...` no index (100K rows) | 182.879 ms | 69.44 MB | Full scan over wide rows |
| `WHERE a = ... AND b = ...` single-column index (100K rows) | 32.360 us | 20.54 KB | Uses `a` only, then filters `b` after row fetch |
| `WHERE a = ... AND b = ... SELECT *` composite index (100K rows) | 3.427 us | 18.19 KB | Direct composite equality lookup over hashed secondary index |
| `WHERE a = ... AND b = ... SELECT id, a, b` composite covered projection (100K rows) | 2.523 us | 10.33 KB | Index-only projection now uses no-copy hashed-payload matching on the covered path |
| `WHERE a = ... AND b = ... SELECT id, a, b` unique composite covered projection (100K rows) | 2.575 us | 10.33 KB | Same covered path on a unique composite index |

### SQL Indexed Aggregate Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `SUM(value)` no index (100K rows) | 56.782 ms | 20.79 MB | Full table aggregate over decoded rows |
| `SUM(value)` direct index aggregate (100K rows) | 14.491 ms | 4.46 MB | Walks integer index keys without base-row fetch |
| `COUNT(value)` no index (100K rows) | 56.479 ms | 20.79 MB | Full table aggregate |
| `COUNT(value)` direct index aggregate (100K rows) | 14.519 ms | 4.47 MB | Counts row-id payloads per integer key |
| `MIN(value)` no index (100K rows) | 57.152 ms | 20.78 MB | Full scan baseline |
| `MIN(value)` direct index aggregate (100K rows) | 7.608 us | 5674 B | First-key fast path on ordered integer index |
| `MAX(value)` no index (100K rows) | 57.626 ms | 20.78 MB | Full scan baseline |
| `MAX(value)` direct index aggregate (100K rows) | 519.6 ns | 824 B | Rightmost-key fast path on ordered integer index |
| `COUNT(*) WHERE value BETWEEN ...` no index (100K rows) | 56.392 ms | 20.79 MB | Scan + predicate baseline |
| `COUNT(*) WHERE value BETWEEN ...` direct index aggregate (100K rows) | 7.304 ms | 2.20 MB | Range aggregate from integer index keys |
| `SUM(value) WHERE value BETWEEN ...` no index (100K rows) | 57.880 ms | 20.79 MB | Scan + predicate baseline |
| `SUM(value) WHERE value BETWEEN ...` direct index aggregate (100K rows) | 7.284 ms | 2.20 MB | Range aggregate stays on index data |

### SQL Primary-Key Aggregate Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `MIN(id)` via table key aggregate (100K rows) | 7.052 us | 5526 B | First-row fast path on the table B-tree key |
| `MAX(id)` via table key aggregate (100K rows) | 409.1 ns | 728 B | Rightmost-key fast path on the table B-tree key |
| `COUNT(id)` via table key aggregate (100K rows) | 278.5 ns | 520 B | Reuses cached table row count; same semantics as `COUNT(*)` on integer PK |
| `SUM(id)` via table key aggregate (100K rows) | 55.462 ms | 20.79 MB | Sums row keys without row payload decode |
| `COUNT(*) WHERE id BETWEEN ...` via table key aggregate (100K rows) | 27.535 ms | 10.67 MB | Range aggregate stays on the table key stream |
| `SUM(id) WHERE id BETWEEN ...` via table key aggregate (100K rows) | 27.166 ms | 10.67 MB | PK range aggregate without row fetch |

### SQL DISTINCT Aggregate Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `COUNT(DISTINCT value)` no index (100K rows) | 29.054 ms | 10.95 MB | Duplicate-heavy integer column with 1,024 distinct keys |
| `COUNT(DISTINCT value)` direct index aggregate (100K rows) | 81.74 us | 904 B | Counts unique integer index keys without row decode |
| `SUM(DISTINCT value)` no index (100K rows) | 29.739 ms | 10.94 MB | Full table distinct-set baseline |
| `SUM(DISTINCT value)` direct index aggregate (100K rows) | 83.80 us | 904 B | Sums unique integer index keys directly |
| `AVG(DISTINCT value)` no index (100K rows) | 29.635 ms | 10.95 MB | Full table distinct-set baseline |
| `AVG(DISTINCT value)` direct index aggregate (100K rows) | 83.26 us | 904 B | Computes distinct sum/count from index keys only |

### SQL Grouped Aggregate Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `GROUP BY group_id SELECT group_id, COUNT(*)` no index (100K rows) | 34.742 ms | 11.51 MB | Generic grouped hash aggregate over a duplicate-heavy integer key |
| `GROUP BY group_id SELECT group_id, COUNT(*)` direct index aggregate (100K rows) | 153.156 us | 103.10 KB | Streams distinct integer index keys and row-id payload counts without row decode |
| `GROUP BY group_id SELECT group_id, COUNT(*), SUM(group_id), AVG(group_id)` no index (100K rows) | 37.018 ms | 11.93 MB | Generic grouped hash aggregate with multiple scalar states per group |
| `GROUP BY group_id SELECT group_id, COUNT(*), SUM(group_id), AVG(group_id)` direct index aggregate (100K rows) | 170.621 us | 165.96 KB | Same grouped result computed directly from ordered index keys |
| `GROUP BY group_id WHERE group_id BETWEEN ... SELECT group_id, COUNT(*)` no index (100K rows) | 35.477 ms | 11.24 MB | Generic grouped aggregate still scans and groups the filtered input |
| `GROUP BY group_id WHERE group_id BETWEEN ... SELECT group_id, COUNT(*)` direct index aggregate (100K rows) | 87.671 us | 52.30 KB | Range-restricted grouped aggregate stays on the ordered integer index |
| `GROUP BY group_id ORDER BY group_id LIMIT 100 SELECT group_id, COUNT(*)` no index (100K rows) | 35.231 ms | 11.52 MB | Generic grouped aggregate still materializes, sorts, and then trims |
| `GROUP BY group_id ORDER BY group_id LIMIT 100 SELECT group_id, COUNT(*)` direct index aggregate (100K rows) | 17.615 us | 18.55 KB | Natural key order from the index lets the grouped path stop after the first 100 groups |
| `GROUP BY group_id WHERE group_id = ... HAVING COUNT(*) >= ... SELECT group_id, COUNT(*)` no index (100K rows) | 32.516 ms | 10.76 MB | Equality filter still scans the table, groups one key, and applies HAVING in the generic path |
| `GROUP BY group_id WHERE group_id = ... HAVING COUNT(*) >= ... SELECT group_id, COUNT(*)` direct index aggregate (100K rows) | 1.155 us | 1.53 KB | Equality-restricted grouped fast path now applies `HAVING COUNT(*)` directly from the index payload count |

### SQL Composite Grouped Aggregate Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `GROUP BY a, b SELECT a, b, COUNT(*)` no index (100K rows) | 44.156 ms | 22.57 MB | Generic grouped aggregate over the full composite key builds hash state from decoded rows |
| `GROUP BY a, b SELECT a, b, COUNT(*)` composite index aggregate (100K rows) | 3.571 ms | 3.66 MB | Streams hashed composite index payloads and aggregates directly from grouped key buckets |
| `GROUP BY a SELECT a, COUNT(*)` no index (100K rows) | 35.172 ms | 11.41 MB | Generic grouped aggregate over the leftmost composite key prefix |
| `GROUP BY a SELECT a, COUNT(*)` composite index prefix aggregate (100K rows) | 927.8 us | 2.09 MB | Leftmost-prefix grouping stays on the `(a, b)` index and avoids base-row decode |

### SQL Predicate Pushdown Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `WHERE value < 200000` (100K rows) | 67.312 ms | 26.89 MB | Single simple pre-decode predicate with about 20% selectivity |
| `WHERE value >= 10000 AND value < 20000` (100K rows) | 54.542 ms | 21.84 MB | Compound same-column range now pushes both bounds into pre-decode filtering |
| `WHERE category = 'Alpha' AND value < 200000` (100K rows) | 56.748 ms | 22.61 MB | Compound mixed text + integer predicate now pushes both conjuncts before row decode |

### SQL Scan Projection Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Compact scan batch plan: residual column projection (10K rows) | 1.172 ms | 780.69 KB | Compact scan path keeps residual filtering and column projection on the internal row-batch transport |
| Compact scan batch plan: expression projection (10K rows, 20% selectivity) | 856.8 us | 266.12 KB | Compact scan path keeps expression projection batch-backed instead of dropping to row transport immediately |
| Compact scan batch plan: residual column projection (100K rows) | 71.956 ms | 28.64 MB | Large filtered compact scan stays on the internal row-batch path end to end |
| Generic scan batch plan: expression projection + LIMIT (100K rows, 20% selectivity) | 56.532 ms | 23.41 MB | Generic scan path still reaches the projection boundary without losing the batch transport |

### SQL Batched Scan Root Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `SELECT *` full scan (100K rows) | 123.052 ms | 48.32 MB | Plain `TableScanOperator` now feeds `QueryResult` through row batches instead of only row-by-row materialization |
| `SELECT * WHERE value < 200000` (100K rows) | 68.517 ms | 26.89 MB | Plain filtered scan stays batch-backed through `FilterOperator` on the simple scan path |
| `SELECT * WHERE value < 10000` (100K rows) | 54.625 ms | 21.83 MB | Same batched scan root with a low-selectivity predicate |
| `SELECT * LIMIT 100` (100K rows) | 110.620 us | 98.49 KB | `LimitOperator` preserves the scan batch root instead of forcing row-by-row materialization at the result boundary |

### SQL Batched Sort / Distinct Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `SELECT DISTINCT value` (10K rows) | 2.004 ms | 2.21 MB | `DistinctOperator` ingests batch sources directly and can act as a batch-backed root rather than pulling one row at a time through the result boundary |
| `SELECT DISTINCT value ORDER BY value LIMIT 100` (10K rows) | 2.660 ms | 974.79 KB | `Distinct` feeding `Sort` stays batch-aware on both operators before final row materialization |
| `SELECT * ORDER BY value` (100K rows) | 146.535 ms | 63.56 MB | `SortOperator` materializes input from batch sources in `OpenAsync` and can emit sorted output in row batches |
| `SELECT * ORDER BY value + id` (100K rows) | 161.375 ms | 63.56 MB | Same full-sort path with an expression key; batch-aware input still helps when sort keys are computed |
| `SELECT * ORDER BY value LIMIT 100` (100K rows) | 52.767 ms | 20.89 MB | Top-N sort root stays batch-backed at the output boundary too |
| `SELECT * ORDER BY value + id LIMIT 100` (100K rows) | 52.864 ms | 20.93 MB | Expression top-N path over the batch-aware sort root |

### SQL Batched Aggregate Consumer Spot Checks (March 25, 2026)

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `Scalar SUM(value)` (100K rows) | 53.467 ms | 20.79 MB | `ScalarAggregateOperator` ingests batch sources directly, removing row-by-row executor calls on the generic scan-fed aggregate path |
| `Scalar COUNT(value)` (100K rows) | 53.435 ms | 20.79 MB | Same generic scalar aggregate consumer path with batch-fed row accumulation |
| `Scalar MIN(value)` (100K rows) | 53.660 ms | 20.78 MB | Generic aggregate consumer stays on batched scan input even when the aggregate itself is non-additive |
| `Hash SUM(value) via GROUP BY 1` (100K rows) | 53.400 ms | 20.79 MB | `HashAggregateOperator` reads batches directly before materializing grouped output rows |
| `GROUP BY with COUNT + AVG` (100K rows) | 57.704 ms | 20.98 MB | Generic grouped aggregate path over the batch-fed scan root; specialized index-grouped paths remain much faster when they apply |

### SQL Join Spot Checks (March 26, 2026)

Source: `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.JoinBenchmarks-report.csv`

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| `INNER JOIN 5Kx200x10` reorder chain with outer mixed union filter | 2.071 ms | 792.46 KB | Focused planner/executor stress case for reordered multi-join pipelines under an outer mixed union filter |

The latest targeted join rerun is narrower than the older March 25 projection matrix: this refresh is a focused reorder-chain validation case rather than a full hash-vs-nested-loop sweep.

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

The single-op, ADO.NET, and dedicated rotating batch rows below were refreshed on March 25, 2026. The load/save rows were refreshed on March 26, 2026 from `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.InMemoryPersistenceBenchmarks-report.csv`.

| Metric | Current Result | Notes |
|--------|----------------|-------|
| SQL insert (private engine in-memory) | 3.23 us | `InMemorySqlBenchmarks` |
| Collection put (private engine in-memory) | 3.60 us | `InMemoryCollectionBenchmarks` on the binary payload path |
| SQL batch insert x100 (rotating in-memory) | ~1.32M rows/sec | Dedicated 10s run, resets the in-memory DB every 100K inserted rows |
| Collection batch put x100 (rotating in-memory) | ~868.16K docs/sec | Dedicated 10s run, resets the in-memory DB every 100K inserted docs |
| ADO.NET ExecuteScalar (`:memory:`) | 238.7 ns | Private connection-local in-memory DB |
| ADO.NET ExecuteScalar (`:memory:name`) | 347.1 ns | Named shared in-memory DB |
| ADO.NET insert (`:memory:`) | 2.722 us | Private connection-local in-memory DB |
| ADO.NET insert (`:memory:name`) | 2.903 us | Named shared in-memory DB |
| Load SQL DB + WAL into memory | 1.812 ms | `Database.LoadIntoMemoryAsync` |
| Load collection DB + WAL into memory | 2.493 ms | `Database.LoadIntoMemoryAsync` |
| Save in-memory SQL snapshot to disk | 4.143 ms | `Database.SaveToFileAsync` |
| Save in-memory collection snapshot to disk | 4.448 ms | `Database.SaveToFileAsync` |

### Cold / Cache-Pressured Lookup Spot Checks

These runs use a 200K-row working set with `MaxCachedPages = 16` and randomized lookup probes so the storage path is exercised instead of a single warmed page.

| Metric | Current Result | Notes |
|--------|----------------|-------|
| SQL cold lookup (file-backed) | 29.608 us | Cache-pressured primary-key lookup |
| SQL cold lookup (in-memory) | 2.507 us | Same workload after `LoadIntoMemoryAsync` |
| Collection cold get (file-backed) | 30.146 us | Cache-pressured direct collection lookup |
| Collection cold get (in-memory) | 2.178 us | Same workload after `LoadIntoMemoryAsync` |

### Indexed Lookup / Tuning Spot Checks

| Metric | Mean | Allocated | Notes |
|--------|------|-----------|-------|
| Collection `FindByIndex` int equality (1 match) | 1.407 us | 1546 B | Direct integer-key index probe |
| Collection `FindByIndex` text equality (1 match) | 2.152 us | 3993 B | Ordered text bucket probe with exact string match before document materialization |
| Collection `FindByIndex` nested path equality | 579.9 ns | 1080 B | Indexed nested scalar path probe |
| Collection `FindByPath` nested path equality | 568.9 ns | 1080 B | Query-facing path API on the same indexed nested path |
| Collection `FindByIndex` array path equality | 481.4 ns | 912 B | Indexed terminal-array contains lookup |
| Collection `FindByPath` array path equality | 474.8 ns | 912 B | Query-facing path API on the same indexed array path |
| Collection `FindByPath` nested array path equality | 599.3 ns | 1160 B | Indexed scalar lookup through array-of-object elements |
| Collection `FindByPath` integer range (1024 matches) | 552.79 us | 314548 B | Ordered integer path range query over the public collection path surface |
| Collection `FindByPath` text range (1000 matches) | 545.69 us | 460571 B | Ordered text path range query over prefix-bucket collection indexes |
| Collection `PutAsync` with secondary indexes (insert) | 12.453 us | 43205 B | Transaction + rollback micro for write maintenance |
| Collection `PutAsync` with secondary indexes (update) | 29.079 us | 74375 B | Includes old-entry removal plus reinsert |
| Collection `DeleteAsync` with secondary indexes | 47.314 us | 209424 B | Transaction + rollback micro for delete-side cleanup |

### File-Backed Lookup Tuning Takeaways

- `MaxCachedPages = 2048` was still the best collection setting in the current tuning matrix: indexed collection lookup fell from `54.15 us` at 16 pages to `20.57 us` at 2048 pages with `UseCachingIndexes = false`.
- `UseCachingIndexes` stayed neutral-to-negative on these lookup workloads. At `256` pages, SQL indexed lookup worsened from `91.55 us` to `273.44 us`, and the reused reader-session path worsened from `41.36 us` to `212.94 us`; collection lookup was roughly flat at `32.35 us` vs `34.06 us`.
- The reader-session setup penalty is visible again in the latest dedicated `ReaderSessionBenchmarks`: simple point lookups measured `1,703.7 ns` with per-query sessions, `680.9 ns` with a reused session, and `663.6 ns` with direct `ExecuteAsync`.
- A small dedicated WAL read cache still helps once the hottest pages live in the WAL instead of the main file: the WAL-backed SQL cold-lookup micro dropped from `28.74 us` to `19.83 us` when `MaxCachedWalReadPages = 128`.
- Raw storage traversal still benefits from speculative next-leaf reads: on the direct file-backed `BTreeCursor` micro, full scans improved from `9.25 ms` to `8.51 ms` at `10K` rows and from `92.74 ms` to `87.41 ms` at `100K`, while the `1024`-row seek window improved from `919.2 us` to `822.2 us` at `10K` and from `945.7 us` to `824.7 us` at `100K`.
- Recommended direct file-backed read preset for hot local workloads: `builder.UseDirectLookupOptimizedPreset()` keeps the existing page-cache shape and read path.
- Recommended direct cold-file preset for cache-pressured local reads: `builder.UseDirectColdFileLookupPreset()` keeps the existing cache shape but enables memory-mapped reads for clean main-file pages.

### File-Backed Durable Write Tuning Takeaways

- Do not compare the durable write harnesses directly. `durable-sql-batching` answers explicit-transaction and analyzed-table SQL questions, `write-diagnostics` answers single-row checkpoint/WAL policy questions, and `concurrent-write-diagnostics` answers shared-engine in-process writer contention questions.
- The March 30, 2026 `durable-sql-batching-20260330-122006-median-of-3.csv` run again showed that application-level batching is the largest lever. Auto-commit single-row SQL stayed around `273.0 ops/sec`, but explicit transactions scaled to about `2700 rows/sec` at `10` rows/commit, `25700 rows/sec` at `100` rows/commit, and `205000 rows/sec` at `1000` rows/commit.
- The same batching run showed that analyzed `UseLowLatencyDurableWritePreset()` edged out the analyzed `UseWriteOptimizedPreset()` row on this runner, but only slightly: `278 ops/sec` vs `275 ops/sec`. Treat the low-latency preset as measure-first rather than a blanket default change.
- Commit-path diagnostics from the batching run still pointed at the durable flush as the dominant fixed cost for single-row durable SQL. WAL append, publish-batch, finalize, and checkpoint-decision stages remained comparatively small.
- The latest dedicated single-writer `write-diagnostics-20260330-121058-median-of-3.csv` rerun stayed in a tight `270-280 ops/sec` band. `FrameCount(2048)` and `FrameCount(4096)` both landed at `280 ops/sec`, `WalSize(8 MiB)` and `FrameCount(1000)` both reached `279`, and the background / preallocation / `BatchWindow(250us)` variants clustered just behind them.
- Background auto-checkpointing still matters because it keeps checkpoint work off the triggering commit. In the March 30 rerun, the background rows again recorded `0` commits that paid checkpoint cost while staying close to the top single-writer rows.
- `BatchWindow(250us)` remained a non-win on the single-writer path at `272 ops/sec`, and `BatchWindow(1ms)` remained clearly bad at `67 ops/sec`. Single-writer durable commits still do not have enough contention to amortize an intentional wait.
- The stable concurrent median-of-3 rerun in `concurrent-write-diagnostics-20260330-122507-median-of-3.csv` restored the expected `8`-writer range. `W8_Batch0_Prealloc1MiB` led at `1114 commits/sec`, followed closely by `W8_Batch250us_Prealloc1MiB` at `1104`, `W8_Batch500us` at `1102`, and `W8_Batch0` at `1093`.
- In that same concurrent rerun, the best `4`-writer row was `W4_Batch250us` at `551 commits/sec`, with both `W4_Batch0` and `W4_Batch500us` landing at `541`.
- Recommended interpretation after the latest reruns: keep defaults at `0`, start with `builder.UseWriteOptimizedPreset()`, and benchmark explicit transaction batching before batch-window or preallocation tuning. For shared-instance `8`-writer contention on this runner, benchmark `UseWalPreallocationChunkBytes(1 * 1024 * 1024)` with `BatchWindow(0)` first and `BatchWindow(250us)` second. For `4` writers, benchmark `BatchWindow(250us)` first.
- Recommended file-backed write-heavy preset: `builder.UseWriteOptimizedPreset()`. This is opt-in and does not change the engine default checkpoint policy.

## CSharpDB Storage Mode Comparison

These tables isolate the embedded CSharpDB storage modes relevant to the current hybrid design:

- file-backed
- hybrid incremental-durable
- hybrid hot-set incremental-durable
- in-memory

The tables below come from different focused harnesses and should not be mixed:

- resident hot-set read source: `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-hot-set-read-20260330-082434.csv`
- post-checkpoint hot reread source: `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-post-checkpoint-20260330-082443.csv`
- hot steady-state source: `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-storage-mode-20260330-082134.csv`
- cold open source: `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/hybrid-cold-open-20260330-082420.csv`

Use the resident hot-set table as the canonical "pinned/resident hot object"
comparison. That harness opens fresh instances and times only the immediate hot
read burst. Use the cold-open tables to see the up-front cost of that preload,
and use the post-checkpoint table to see the baseline lazy-hybrid checkpoint
residency behavior.

### Canonical Resident Hot-Set Comparison

Each iteration opens a fresh database instance, then measures the same `256`-item
SQL or collection hot burst. Open cost is excluded from throughput here so the
table shows read speed after the mode is ready to serve.

| Mode | SQL Hot Burst | SQL P50 | Collection Hot Burst | Collection P50 |
|------|---------------|---------|----------------------|----------------|
| **File-backed** | **38.32K ops/sec** | **0.0239 ms** | **41.65K ops/sec** | **0.0225 ms** |
| **Hybrid incremental-durable** | **35.33K ops/sec** | **0.0274 ms** | **42.21K ops/sec** | **0.0224 ms** |
| **Hybrid hot-set incremental-durable** | **384.32K ops/sec** | **0.0013 ms** | **886.55K ops/sec** | **0.0011 ms** |
| **In-memory** | **93.18K ops/sec** | **0.0025 ms** | **400.78K ops/sec** | **0.0022 ms** |

### Hybrid Open Cost

This is the cost you pay to get the resident hot-set behavior above.

| Mode | SQL Open Only | SQL P50 | Collection Open Only | Collection P50 |
|------|---------------|---------|----------------------|----------------|
| **File-backed** | **74.5 ops/sec** | **13.2234 ms** | **73.1 ops/sec** | **13.1025 ms** |
| **Hybrid incremental-durable** | **70.2 ops/sec** | **13.2952 ms** | **74.2 ops/sec** | **12.9680 ms** |
| **Hybrid hot-set incremental-durable** | **10.6 ops/sec** | **90.7188 ms** | **7.7 ops/sec** | **127.3652 ms** |
| **In-memory** | **67.8 ops/sec** | **14.0901 ms** | **45.8 ops/sec** | **20.2809 ms** |

### Cold Open + First Read

| Mode | SQL Open + First Lookup | SQL P50 | Collection Open + First Get | Collection P50 |
|------|--------------------------|---------|------------------------------|----------------|
| **File-backed** | **73.9 ops/sec** | **12.8874 ms** | **74.8 ops/sec** | **13.1145 ms** |
| **Hybrid incremental-durable** | **73.5 ops/sec** | **13.1080 ms** | **75.2 ops/sec** | **12.9058 ms** |
| **Hybrid hot-set incremental-durable** | **11.1 ops/sec** | **87.7034 ms** | **7.8 ops/sec** | **124.6584 ms** |
| **In-memory** | **68.7 ops/sec** | **14.0499 ms** | **47.0 ops/sec** | **20.7994 ms** |

### Post-Checkpoint Hot Reread

Each measured burst forces one auto-checkpointed write and then rereads the same
`256`-item hot set. This is the baseline lazy-hybrid checkpoint-residency
harness.

| Mode | SQL Rereads/sec | SQL P50 | Collection Rereads/sec | Collection P50 |
|------|-----------------|---------|------------------------|----------------|
| **File-backed** | **18.61K ops/sec** | **0.0125 ms** | **17.75K ops/sec** | **0.0140 ms** |
| **Hybrid incremental-durable** | **23.27K ops/sec** | **0.0010 ms** | **23.08K ops/sec** | **0.0007 ms** |
| **Hybrid hot-set incremental-durable** | **21.78K ops/sec** | **0.0011 ms** | **22.71K ops/sec** | **0.0008 ms** |
| **In-memory** | **595.35K ops/sec** | **0.0014 ms** | **864.30K ops/sec** | **0.0009 ms** |

### Hot Steady-State SQL

| Mode | Single INSERT | Batched INSERT | Point Lookup | Concurrent Reads |
|------|---------------|----------------|--------------|------------------|
| **File-backed** | **272.8 ops/sec** | **~26.22K rows/sec** | **~1.07M ops/sec** | **~619.66K / ~11.80M COUNT(*) ops/sec (8r, per-query / reused x32)** |
| **Hybrid incremental-durable** | **272.3 ops/sec** | **~26.78K rows/sec** | **~1.08M ops/sec** | **~614.95K / ~12.38M COUNT(*) ops/sec (8r, per-query / reused x32)** |
| **In-memory** | **~256.28K ops/sec** | **~760.80K rows/sec** | **~1.09M ops/sec** | **~622.95K / ~12.33M COUNT(*) ops/sec (8r, per-query / reused x32)** |

`Hybrid hot-set incremental-durable` is intentionally not shown in the generic
steady-state table. Once the workload itself has already touched the hot pages,
the dedicated warm-set hint stops being the differentiator. Use the resident
hot-set table above for that feature.

### Hot Steady-State Collection

| Mode | Single Put | Batched Put | Point Get |
|------|------------|-------------|-----------|
| **File-backed** | **278.3 ops/sec** | **~26.96K docs/sec** | **~1.77M ops/sec** |
| **Hybrid incremental-durable** | **281.4 ops/sec** | **~26.27K docs/sec** | **~1.80M ops/sec** |
| **In-memory** | **~244.76K ops/sec** | **~654.34K docs/sec** | **~1.81M ops/sec** |

## Competitor Comparison

The master table below separates embedded engine runs from client/hosted runs so the interface cost is visible.

- Embedded engine durable SQL/collection rows were refreshed on March 30, 2026 from `hybrid-storage-mode-20260330-082134.csv` using the focused hot storage-mode harness.
- CSharpDB SQL concurrent reads are shown as `per-query sessions / reused reader sessions (x32 reads per snapshot)` because those patterns measure materially different setup costs.
- The direct local SQL client row was refreshed on March 30, 2026 from `direct-file-cache-transport-20260330-081709.csv`.
- The top SQL/collection API snapshot tables above use the macro harness in durable mode (`macro-20260330-081102.csv`).
- Cold / cache-pressured lookup numbers were also refreshed on March 15, 2026 from `ColdLookupBenchmarks-report.csv`, but they stay in the dedicated spot-check section rather than the master table.
- Ordered/range covered-scan numbers were refreshed on March 14, 2026 from `OrderByIndexBenchmarks`, but they stay in the micro sections because the master table tracks durable writes, cold point lookups, and concurrent-read throughput rather than scan-shape throughput.
- Indexed aggregate numbers were refreshed on March 14, 2026 from `IndexAggregateBenchmarks`, but they stay in the micro sections because the master table does not currently have an aggregate column.
- Primary-key aggregate numbers were refreshed on March 14, 2026 from `PrimaryKeyAggregateBenchmarks`, and they also stay in the micro sections for the same reason.
- Embedded engine rows and client rows are different surfaces: the direct client row includes public client API overhead on top of the embedded engine itself.
- Competitor figures are still approximate ranges from published third-party sources on comparable hardware.

### Master Comparison Table

CSharpDB rows below are durable March 30, 2026 `main` captures. Competitor rows remain single published directional ranges.

| Database | Language | Type | Single INSERT | Batched INSERT | Point Lookup | Concurrent Reads |
|----------|----------|------|---------------|----------------|--------------|------------------|
| **CSharpDB SQL (embedded engine, file-backed)** | **C#** | **Relational SQL** | **272.8 ops/sec** | **~26.22K rows/sec** | **~1.07M ops/sec** | **~619.66K / ~11.80M COUNT(*) ops/sec (8r, per-query / reused x32)** |
| **CSharpDB SQL (embedded engine, incremental-durable hybrid)** | **C#** | **Relational SQL** | **272.3 ops/sec** | **~26.78K rows/sec** | **~1.08M ops/sec** | **~614.95K / ~12.38M COUNT(*) ops/sec (8r, per-query / reused x32)** |
| **CSharpDB SQL (direct client, local process)** | **C#** | **Relational SQL** | **277.3 ops/sec** | **~3.26K rows/sec** | **~530.73K ops/sec** | **~510.33K COUNT(*) ops/sec (8r)** |
| **CSharpDB SQL (embedded engine, in-memory)** | **C#** | **Relational SQL** | **~256.28K ops/sec** | **~760.80K rows/sec** | **~1.09M ops/sec** | **~622.95K / ~12.33M COUNT(*) ops/sec (8r, per-query / reused x32)** |
| **CSharpDB Collection (embedded engine, file-backed)** | **C#** | **Document (NoSQL)** | **278.3 ops/sec** | **~26.96K docs/sec** | **~1.77M ops/sec** | **-** |
| **CSharpDB Collection (embedded engine, incremental-durable hybrid)** | **C#** | **Document (NoSQL)** | **281.4 ops/sec** | **~26.27K docs/sec** | **~1.80M ops/sec** | **-** |
| **CSharpDB Collection (embedded engine, in-memory)** | **C#** | **Document (NoSQL)** | **~244.76K ops/sec** | **~654.34K docs/sec** | **~1.81M ops/sec** | **-** |
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
