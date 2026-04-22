# CSharpDB SQLite Comparison

This page is the focused same-runner comparison between CSharpDB and local SQLite. The main [README.md](README.md) remains the release performance scorecard; this file explains the SQLite rows in more detail.

## Comparison Contract

- CSharpDB rows are durable and file-backed unless a row says otherwise.
- SQLite rows use `Microsoft.Data.Sqlite/10.0.6`, `journal_mode=WAL`, `synchronous=FULL`, private cache, and pooling disabled.
- Ratios are `CSharpDB throughput / SQLite throughput`; values above `1.00x` mean CSharpDB is faster for that row.
- Published comparison rows come only from approved release artifacts. Failed or diagnostic runs stay in [HISTORY.md](HISTORY.md).
- This is a local same-machine comparison, not a universal database ranking.

Current snapshot:

| Field | Value |
|---|---|
| Promotion state | Promoted with the main benchmark README after `PASS=185, WARN=0, SKIP=0, FAIL=0` |
| Runner | Intel i9-11900K, 16 logical cores, Windows 10.0.26300, .NET SDK 10.0.202, .NET runtime 10.0.6 |
| Repro mode | priority=High, affinity=0xFF when captured with `--repro` |
| Commit | `4e12bbe6eed58ffa7b7cf85371093008c292ec13` plus uncommitted release-prep fixes |

## Headline Results

| Workload | CSharpDB | SQLite `WAL+FULL` | Ratio | Notes |
|---|---:|---:|---:|---|
| Single durable SQL insert | 279.4 ops/sec | 281.7 ops/sec | 0.99x | Single-row auto-commit SQL |
| Batch x100 durable SQL insert | 26.71K rows/sec | 25.47K rows/sec | 1.05x | 100-row transaction throughput |
| Durable ingest B1000 | 204.03K rows/sec | 192.06K rows/sec | 1.06x | CSharpDB `InsertBatch`; SQLite prepared 4-column insert |
| Durable ingest B10000 | 798.25K rows/sec | 539.56K rows/sec | 1.48x | Large explicit durable batch |
| SQL point lookup | 1.33M ops/sec | 138.33K ops/sec | 9.63x | Warm single-connection primary-key lookup |
| 8-reader burst count | 10.77M ops/sec | 109.99K ops/sec | 97.95x | Reused read sessions/connections; tiny `COUNT(*)` read shape |

## Latency Detail

| Workload | Engine | P50 | P99 |
|---|---|---:|---:|
| Single durable SQL insert | CSharpDB | 3.4014 ms | 7.8048 ms |
| Single durable SQL insert | SQLite `WAL+FULL` | 3.3914 ms | 7.9140 ms |
| Batch x100 durable SQL insert | CSharpDB | 3.5461 ms | 8.0026 ms |
| Batch x100 durable SQL insert | SQLite `WAL+FULL` | 3.7841 ms | 7.9544 ms |
| Durable ingest B1000 | CSharpDB | 4.1034 ms | 9.5599 ms |
| Durable ingest B1000 | SQLite `WAL+FULL` | 4.7479 ms | 18.1078 ms |
| Durable ingest B10000 | CSharpDB | 8.7019 ms | 119.0664 ms |
| Durable ingest B10000 | SQLite `WAL+FULL` | 16.6275 ms | 43.3034 ms |
| SQL point lookup | CSharpDB | 0.0005 ms | 0.0021 ms |
| SQL point lookup | SQLite `WAL+FULL` | 0.0058 ms | 0.0188 ms |
| 8-reader burst count | CSharpDB | 0.0002 ms | 0.0010 ms |
| 8-reader burst count | SQLite `WAL+FULL` | 1.1546 ms | 11.9707 ms |

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
| CSharpDB top-line SQL | `--master-table --repeat 3 --repro` | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/master-table-20260421-212338-median-of-3.csv` |
| CSharpDB durable batching | `--durable-sql-batching --repeat 3 --repro` | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/durable-sql-batching-20260421-214227-median-of-3.csv` |
| SQLite comparison | `--sqlite-compare --repeat 3 --repro` | `tests/CSharpDB.Benchmarks/bin/Release/net10.0/results/sqlite-compare-20260421-222824-median-of-3.csv` |

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
