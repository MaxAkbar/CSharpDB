# CSharpDB Benchmark Catalog

This file lists the available benchmark harnesses and how they are used. The main [README.md](README.md) publishes only the core performance contract.

## Classification

| Class | Meaning |
|---|---|
| `core` | Feeds the generated README scorecard and current core result tables. |
| `release guardrail` | Used by `Run-Perf-Guardrails.ps1 -Mode release` or PR guardrails. |
| `diagnostic` | Kept for investigations, tuning, comparisons, and one-off validation. |

## Core Release Suites

| Command | Class | Purpose |
|---|---|---|
| `--master-table --repeat 3 --repro` | `core` | Durable SQL/collection top-line writes, reads, and concurrent reads. |
| `--durable-sql-batching --repeat 3 --repro` | `core` | Single-writer durable ingest and batch-size behavior. |
| `--concurrent-write-diagnostics --repeat 3 --repro` | `core` | Shared-engine concurrent durable auto-commit insert throughput. |
| `--hybrid-storage-mode --repeat 3 --repro` | `core` | File-backed, hybrid, and in-memory hot steady-state storage mode tradeoffs. |
| `--hybrid-hot-set-read --repeat 3 --repro` | `core` | Resident hot-set read behavior after open. |
| `--hybrid-cold-open --repeat 3 --repro` | `core` | Engine-cold open and first-read costs. |
| `--sqlite-compare --repeat 3 --repro` | `core` | Local SQLite WAL+FULL comparison rows. |

Use `--release-core --repeat 3 --repro` to run the core suites in one command.

## Release Guardrails

| Harness / CSV | Class | Purpose |
|---|---|---|
| `CollationIndexBenchmarks` | `release guardrail` | Ordered/collated text-index lookup and indexed write maintenance. |
| `InsertBenchmarks` | `release guardrail` | SQL insert micro paths, including batch insert rows. |
| `PointLookupBenchmarks` | `release guardrail` | SQL primary-key lookup rows. |
| `CollectionIndexBenchmarks` | `release guardrail` | Collection index lookup and write maintenance. |
| `JoinBenchmarks` | `release guardrail` | Join planner/executor guardrail rows. |
| `ScanProjectionBenchmarks` | `release guardrail` | Batch-backed scan/projection guardrails. |
| `CompositeIndexBenchmarks` | `release guardrail` | Composite index lookup guardrails. |
| `IndexAggregateBenchmarks` | `release guardrail` | Direct index aggregate guardrails. |
| `QueryPlanCacheBenchmarks` / `QueryPlanCacheGuardrailBenchmarks` | `release guardrail` | Query-plan cache stability. |
| `WalBenchmarks` / `WalGuardrailBenchmarks` | `release guardrail` | WAL write and checkpoint micro guardrails. |
| `ScalarAggregateLookupBenchmarks` | `release guardrail` | Scalar aggregate lookup fast-path guardrails. |
| `CommitFanInDiagnosticsBenchmark` | `release guardrail` | Shared non-insert commit fan-in behavior. |
| `InsertFanInDiagnosticsBenchmark` | `release guardrail` | Insert-side fan-in boundaries and correctness. |
| `DurableWriteDiagnosticsBenchmark` | `release guardrail` | Single-row durable write policy diagnostics. |
| `DurableSqlBatchingBenchmark` | `release guardrail` | Staged durable SQL batching rows. |
| `WriteTransactionDiagnosticsBenchmark` | `release guardrail` | Explicit `WriteTransaction` rows staged into release baselines. |

## Diagnostic Macro Suites

These harnesses stay available, but they do not feed the main README unless they are promoted into the core contract.

| Command | Class | Purpose |
|---|---|---|
| `--macro` | `diagnostic` | Broad sustained write, mixed workload, reader scaling, write amplification, collections, and in-memory macro sweep. |
| `--macro-batch-memory` | `diagnostic` | In-memory rotating x100 batch throughput. |
| `--write-diagnostics` | `diagnostic` | Pager/WAL single-row durable-write policy detail. |
| `--write-transaction-diagnostics` | `diagnostic` | Explicit transaction hot-insert and disjoint-update behavior. |
| `--commit-fan-in-diagnostics` | `diagnostic` | Shared auto-commit vs explicit transaction fan-in detail. |
| `--insert-fan-in-diagnostics` | `diagnostic` | Disjoint key, hot right-edge, and auto-id insert fan-in detail. |
| `--checkpoint-retention-diagnostics` | `diagnostic` | Background checkpoint progress while writers hold transactions. |
| `--concurrent-sqlite-capi-compare` | `diagnostic` | CSharpDB/SQLite C API concurrent insert comparisons. |
| `--concurrent-adonet-compare` | `diagnostic` | CSharpDB/SQLite ADO.NET concurrent insert comparisons. |
| `--direct-file-cache-transport` | `diagnostic` | Direct client and tuned file-cache comparison. |
| `--strict-insert-compare` | `diagnostic` | Strict raw/prepared insert comparison. |
| `--native-aot-insert-compare` | `diagnostic` | ADO.NET, NativeAOT FFI, and SQLite insert comparison. |
| `--efcore-compare` | `diagnostic` | EF Core steady-state insert comparisons. |
| `--efcore-compare-auto-open-close` | `diagnostic` | EF-managed open/close insert comparison. |
| `--efcore-compare-hybrid-shared-connection` | `diagnostic` | EF Core with externally-owned hybrid shared connection. |
| `--hybrid-post-checkpoint` | `diagnostic` | Post-checkpoint hot reread behavior. |
| `--stress` | `diagnostic` | Crash recovery, logical conflicts, and WAL growth. |
| `--scaling` | `diagnostic` | Row-count and B+tree depth scaling experiments. |

Scenario-specific commands such as `--durable-sql-batching-scenario`, `--concurrent-write-scenario`, `--commit-fan-in-scenario`, and `--insert-fan-in-scenario` are diagnostic isolation tools.

## Diagnostic Micro Suites

| Harness | Class |
|---|---|
| `AdoNetBenchmarks` | `diagnostic` |
| `BatchEvaluationBenchmarks` | `diagnostic` |
| `BTreeCursorBenchmarks` | `diagnostic` |
| `ColdLookupBenchmarks` | `diagnostic` |
| `CollectionAccessBenchmarks` | `diagnostic` |
| `CollectionFieldExtractionBenchmarks` | `diagnostic` |
| `CollectionLookupFallbackBenchmarks` | `diagnostic` |
| `CollectionPayloadBenchmarks` | `diagnostic` |
| `CollectionSchemaBreadthBenchmarks` | `diagnostic` |
| `CompositeGroupedIndexBenchmarks` | `diagnostic` |
| `CoveringIndexBenchmarks` | `diagnostic` |
| `DistinctBenchmarks` | `diagnostic` |
| `DistinctAggregateBenchmarks` | `diagnostic` |
| `FullTextSearchBenchmarks` / `FullTextIndexBuildBenchmarks` | `diagnostic` |
| `GroupedDistinctAggregateBenchmarks` | `diagnostic` |
| `GroupedIndexAggregateBenchmarks` | `diagnostic` |
| `IndexBenchmarks` | `diagnostic` |
| `IndexProjectionBenchmarks` | `diagnostic` |
| `InMemorySqlBenchmarks` / `InMemoryCollectionBenchmarks` / `InMemoryAdoNetBenchmarks` | `diagnostic` |
| `InMemorySqlBatchBenchmarks` / `InMemoryCollectionBatchBenchmarks` | `diagnostic` |
| `InMemoryPersistenceBenchmarks` | `diagnostic` |
| `LayerComparisonLookupBenchmarks` / `LayerComparisonInsertBenchmarks` | `diagnostic` |
| `OrderByIndexBenchmarks` | `diagnostic` |
| `ParserBenchmarks` | `diagnostic` |
| `PayloadRangeAggregateBenchmarks` | `diagnostic` |
| `PredicatePushdownBenchmarks` | `diagnostic` |
| `PrimaryKeyAggregateBenchmarks` | `diagnostic` |
| `RecordSizeBenchmarks` | `diagnostic` |
| `ScalarAggregateBenchmarks` | `diagnostic` |
| `ScanBenchmarks` | `diagnostic` |
| `SelectiveDistinctAvgPayloadAggregateBenchmarks` | `diagnostic` |
| `SelectivePayloadRangeAggregateBenchmarks` | `diagnostic` |
| `SingleInsertPathBenchmarks` | `diagnostic` |
| `SortStabilityBenchmarks` | `diagnostic` |
| `SqlMaterializationBenchmarks` | `diagnostic` |
| `SqlTextStabilityBenchmarks` | `diagnostic` |
| `StorageTuningBenchmarks` | `diagnostic` |
| `SubqueryBenchmarks` | `diagnostic` |
| `SystemCatalogBenchmarks` | `diagnostic` |
| `TextIndexBenchmarks` | `diagnostic` |
| `TriggerDispatchBenchmarks` | `diagnostic` |
| `WalCoreBenchmarks` / `WalReadCacheBenchmarks` | `diagnostic` |
| `WideRowSortBenchmarks` | `diagnostic` |

## Adding A New Benchmark

- Add the harness normally, but classify it here before publishing numbers.
- Default to `diagnostic`.
- Promote to `core` only if it answers a product-level performance question in the main README.
- Promote to `release guardrail` only if it should block releases through the threshold files.
