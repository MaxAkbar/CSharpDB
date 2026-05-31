# CSharpDB SQLite Comparison

This page is the focused same-runner comparison between CSharpDB and local SQLite. The main [README.md](README.md) remains the release performance scorecard; this file explains the SQLite rows in more detail.

## Comparison Contract

- CSharpDB rows are durable and file-backed unless a row says otherwise.
- SQLite rows use `Microsoft.Data.Sqlite/10.0.7`, `journal_mode=WAL`, `synchronous=FULL`, private cache, and pooling disabled.
- Ratios are `CSharpDB throughput / SQLite throughput`; values above `1.00x` mean CSharpDB is faster for that row.
- Published comparison rows come only from approved release artifacts. Failed or diagnostic runs stay in [HISTORY.md](HISTORY.md).
- This is a local same-machine comparison, not a universal database ranking.

Current snapshot:

| Field | Value |
|---|---|
| Promotion state | Promoted with the main benchmark README after `PASS=187, WARN=0, SKIP=0, FAIL=0`; the May 31, 2026 rerun was not promoted |
| Runner | Intel i9-11900K, 16 logical cores, Windows 10.0.26300, .NET SDK 10.0.203, .NET runtime 10.0.7 |
| Repro mode | priority=High, affinity=0xFF when captured with `--repro` |
| Commit | `47a700950a150669ce404294c594dd845550f460` |

## Headline Results

| Workload | CSharpDB | SQLite `WAL+FULL` | Ratio | Notes |
|---|---:|---:|---:|---|
| Single durable SQL insert | 267.1 ops/sec | 242.7 ops/sec | 1.10x | Single-row auto-commit SQL |
| Batch x100 durable SQL insert | 25.56K rows/sec | 22.21K rows/sec | 1.15x | 100-row transaction throughput |
| Durable ingest B1000 | 211.99K rows/sec | 155.66K rows/sec | 1.36x | CSharpDB `InsertBatch`; SQLite prepared 4-column insert |
| Durable ingest B10000 | 801.02K rows/sec | 388.17K rows/sec | 2.06x | Large explicit durable batch |
| SQL point lookup | 1.48M ops/sec | 93.91K ops/sec | 15.72x | Warm single-connection primary-key lookup |
| 8-reader burst count | 9.68M ops/sec | 79.08K ops/sec | 122.45x | Reused read sessions/connections; tiny `COUNT(*)` read shape |

## Latency Detail

| Workload | Engine | P50 | P99 |
|---|---|---:|---:|
| Single durable SQL insert | CSharpDB | 3.6088 ms | 6.1934 ms |
| Single durable SQL insert | SQLite `WAL+FULL` | 3.8820 ms | 7.9947 ms |
| Batch x100 durable SQL insert | CSharpDB | 3.7121 ms | 5.5989 ms |
| Batch x100 durable SQL insert | SQLite `WAL+FULL` | 4.2511 ms | 8.4682 ms |
| Durable ingest B1000 | CSharpDB | 4.0197 ms | 8.0891 ms |
| Durable ingest B1000 | SQLite `WAL+FULL` | 5.9735 ms | 22.9219 ms |
| Durable ingest B10000 | CSharpDB | 8.7654 ms | 118.3244 ms |
| Durable ingest B10000 | SQLite `WAL+FULL` | 22.9309 ms | 58.9035 ms |
| SQL point lookup | CSharpDB | 0.0005 ms | 0.0018 ms |
| SQL point lookup | SQLite `WAL+FULL` | 0.0088 ms | 0.0282 ms |
| 8-reader burst count | CSharpDB | 0.0001 ms | 0.0008 ms |
| 8-reader burst count | SQLite `WAL+FULL` | 1.3252 ms | 16.9242 ms |

## Scenario Map

| Comparison row | CSharpDB source row | SQLite source row |
|---|---|---|
| Single durable SQL insert | `MasterComparison_Sql_FileBacked_SingleInsert` | `SQLite_WalFull_Sql_SingleInsert_5s` |
| Batch x100 durable SQL insert | `MasterComparison_Sql_FileBacked_BatchInsertRows` | `SQLite_WalFull_Sql_Batch100_5s` |
| Durable ingest B1000 | `DurableSqlBatching_BatchSweep_InsertBatch_B1000_Baseline_PkOnly_Monotonic_10s` | `SQLite_WalFull_Sql_PreparedBulk4Col_B1000_5s` |
| Durable ingest B10000 | `DurableSqlBatching_BatchSweep_InsertBatch_B10000_Baseline_PkOnly_Monotonic_10s` | `SQLite_WalFull_Sql_PreparedBulk4Col_B10000_5s` |
| SQL point lookup | `MasterComparison_Sql_FileBacked_PointLookup` | `SQLite_WalFull_Sql_PointLookup_20000` |
| 8-reader burst count | `MasterComparison_Sql_FileBacked_ConcurrentReadsBurst32` | `SQLite_WalFull_Sql_ConcurrentReadsBurst32_8readers` |

Approved source artifacts:

| Artifact | Command | Source CSV |
|---|---|---|
| CSharpDB top-line SQL | `--master-table --repeat 3 --repro` | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/master-table-20260506-024609-median-of-3.csv` |
| CSharpDB durable batching | `--durable-sql-batching --repeat 3 --repro` | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-20260506-030458-median-of-3.csv` |
| SQLite comparison | `--sqlite-compare --repeat 3 --repro` | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/sqlite-compare-20260506-035128-median-of-3.csv` |

## How To Refresh

For a full promoted refresh, run the balanced core suite and guardrails from [README.md](README.md). That keeps the SQLite comparison tied to the same promotion rules as the main scorecard.

For an isolated SQLite comparison rerun:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --sqlite-compare --repeat 3 --repro
```

For a complete comparison refresh, rerun the CSharpDB source suites too:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --master-table --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --durable-sql-batching --repeat 3 --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --sqlite-compare --repeat 3 --repro
```

Only promote the results after the release guardrail comparison is clean.

## Caveats

- The B1000 and B10000 ingest rows compare durable batched inserts, but the API shapes are not identical: CSharpDB uses `InsertBatch`; SQLite uses prepared ADO.NET statements.
- The burst read row is intentionally a tiny hot read/concurrency shape. It should not be read as a broad analytical-query comparison.
- SQLite can be configured many ways. This page publishes the durable local configuration used by the benchmark harness, not the fastest possible SQLite mode.
