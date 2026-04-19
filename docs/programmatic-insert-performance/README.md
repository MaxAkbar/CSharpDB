# Programmatic Insert Performance Plans

This folder splits the earlier programmatic insert investigation into five
independent plans aligned to the current public API and the benchmark harnesses
already checked into the repo.

The split is intentional:

- Plan 1 measures the fastest single-writer bulk path.
- Plan 2 measures what changes when durability or residency changes.
- Plan 3 measures when concurrent disjoint-key writers help.
- Plan 4 measures what remains of the hot right-edge conflict-recovery problem.
- Plan 5 measures the remaining raw rows/sec gap against a matched SQLite
  baseline.

Do not compare raw numbers across these plans as if they are one ranking. Each
plan holds a different workload family constant.

## Execution Order

1. Start with [01-single-writer-bulk-path.md](01-single-writer-bulk-path.md).
   That establishes the canonical embedded bulk insert path before any storage
   or concurrency trade-offs are introduced.
2. Move to [02-durability-and-residency-tradeoffs.md](02-durability-and-residency-tradeoffs.md).
   That reuses the same bulk path and isolates what changes when durability or
   residency semantics change.
3. Finish with [03-concurrent-disjoint-key-writers.md](03-concurrent-disjoint-key-writers.md).
   That answers the separate multi-writer question without pretending it is the
   same workload as the single-writer bulk path.
4. If the remaining problem is still hot right-edge structural recovery, move
   to [04-hot-right-edge-conflict-recovery.md](04-hot-right-edge-conflict-recovery.md).
   That is the branch for `MissingTraversal`, `SplitFallbackShape`, and related
   committed-split recovery gaps.
5. If the goal is the remaining durable throughput gap versus SQLite, move to
   [05-raw-rows-per-sec-vs-sqlite.md](05-raw-rows-per-sec-vs-sqlite.md). That
   is the branch for row encoding, index maintenance, locality, and flush
   amortization.

## Shared Scope

- Embedded engine path only: `Database`, `InsertBatch`, `WriteTransaction`, and
  storage options.
- `CSharpDB.Client` remote transports are out of scope unless called out as a
  separate wrapper-overhead comparison.
- ADO.NET and EF Core remain out of scope.
- Every plan should keep one stable schema, one stable row shape, and one
  clearly named baseline row before adding secondary variants.
- Every plan should report `P50`, `P95`, and `P99` in addition to throughput.

## Current Repo Surface

The plans below are written against the current public surface, not an older
draft API map:

- `Database.PrepareInsertBatch(...)`
- `Database.ExecuteAsync(...)`
- `Database.BeginTransactionAsync()`, `CommitAsync()`, and `RollbackAsync()`
- `Database.BeginWriteTransactionAsync(...)`
- `Database.RunWriteTransactionAsync(...)`
- `Database.OpenInMemoryAsync(...)`
- `Database.LoadIntoMemoryAsync(...)`
- `Database.OpenHybridAsync(...)` with `HybridPersistenceMode`
- `ImplicitInsertExecutionMode.ConcurrentWriteTransactions`

## Existing Harnesses To Reuse

These plans assume the next version extends or reuses the current benchmark
code instead of creating a parallel benchmark universe with new naming:

- `tests/CSharpDB.Benchmarks/Macro/DurableSqlBatchingBenchmark.cs`
- `tests/CSharpDB.Benchmarks/Macro/HybridStorageModeBenchmark.cs`
- `tests/CSharpDB.Benchmarks/Macro/InsertFanInDiagnosticsBenchmark.cs`
- `tests/CSharpDB.Benchmarks/Macro/ConcurrentDurableWriteBenchmark.cs`
- `tests/CSharpDB.Benchmarks/Macro/WriteTransactionDiagnosticsBenchmark.cs`
- `tests/CSharpDB.Benchmarks/Macro/SqliteComparisonBenchmark.cs`
- `tests/CSharpDB.Benchmarks/Macro/MasterComparisonBenchmark.cs`
- `tests/CSharpDB.Benchmarks/README.md`

## Plans

- [01-single-writer-bulk-path.md](01-single-writer-bulk-path.md)
- [02-durability-and-residency-tradeoffs.md](02-durability-and-residency-tradeoffs.md)
- [03-concurrent-disjoint-key-writers.md](03-concurrent-disjoint-key-writers.md)
- [04-hot-right-edge-conflict-recovery.md](04-hot-right-edge-conflict-recovery.md)
- [05-raw-rows-per-sec-vs-sqlite.md](05-raw-rows-per-sec-vs-sqlite.md)
