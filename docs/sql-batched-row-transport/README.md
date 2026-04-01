# SQL Batched Row Transport Status

This document records the shipped state of the SQL row-batch transport in `CSharpDB.Execution`.

The roadmap item is no longer "future batch transport."
As of March 31, 2026, internal row-batch transport is the default scan-heavy SQL execution foundation in the current tree, and the remaining work is follow-on optimization rather than missing core architecture.

## Roadmap Status

`SQL batched row transport` can now be treated as `Done`.

That status is justified by the current engine behavior:

- batch-capable scan, index-scan, and join roots can stay on batch-aware result boundaries
- generic filter, projection, and filter+projection paths preserve batch form through shared batch kernels
- generic scalar and grouped aggregates now process batch input without the earlier row-buffer-heavy hot paths
- join condition and residual evaluation can run directly over `(left span, right span)` inputs in the main join paths
- batch-plan pushdown feeds back into predecode-capable sources for comparisons, ranges, and direct-column positive `IN` predicates

## What Shipped

### Batch-first execution foundation

The current tree now has all of the pieces needed for the roadmap item itself:

- `RowBatch`, `IBatchOperator`, and batch-aware result boundaries
- batch-preserving scan, filter, projection, filter+projection, distinct, limit, sort, and top-N paths
- batch-capable hash join, nested-loop join, and index nested-loop join variants
- batch-native generic scalar and grouped aggregate execution over batch sources
- shared batch predicate/projection infrastructure via `RowSelection`, `BatchPlanCompiler`, and span-based expression evaluation

### Shared kernel cleanup

The completion work also converged the execution layer onto one coherent batch-oriented kernel surface:

- shared span evaluators for row spans and join spans
- shared batch predicate/projection plans instead of scattered operator-local loops
- shared predecode filter handling for scan and lookup sources
- shared pushdown plumbing from specialized batch plans back into encoded payload sources

That is enough to treat the batch transport as the generalized execution foundation rather than a partial optimization tier.

## Validation

Validation for closing the roadmap item included both correctness and focused performance checks.

### Test validation

On March 31, 2026, all six test projects passed:

- `tests/CSharpDB.Tests/CSharpDB.Tests.csproj`
- `tests/CSharpDB.Api.Tests/CSharpDB.Api.Tests.csproj`
- `tests/CSharpDB.Cli.Tests/CSharpDB.Cli.Tests.csproj`
- `tests/CSharpDB.Data.Tests/CSharpDB.Data.Tests.csproj`
- `tests/CSharpDB.Daemon.Tests/CSharpDB.Daemon.Tests.csproj`
- `tests/CSharpDB.Pipelines.Tests/CSharpDB.Pipelines.Tests.csproj`

### Benchmark validation

Focused validation was run against the baseline snapshot in `tests/CSharpDB.Benchmarks/baselines/focused-validation/20260330-122507`.

Current reports:

- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ScanProjectionBenchmarks-report.csv`
- `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.ScalarAggregateBenchmarks-report.csv`

Representative results from the final focused reruns:

- `Generic scan batch plan: IN expression projection + LIMIT` improved from `1458.4 us` to `1350.9 us` at `10k` rows (`-7.37%`)
- `Generic scan batch plan: IN expression projection + LIMIT` improved from `67967.9 us` to `65240.4 us` at `100k` rows (`-4.01%`)
- `Generic scan batch plan: residual column projection + LIMIT` improved from `1563.6 us` to `1513.7 us` at `10k` rows (`-3.19%`)
- `Generic scan batch plan: residual column projection + LIMIT` improved from `76769.0 us` to `69985.5 us` at `100k` rows (`-8.84%`)
- `Scalar COUNT(DISTINCT value)` improved from `775.12 us` to `737.91 us` at `10k` rows (`-4.80%`)
- `Scalar SUM(DISTINCT value)` improved from `794.75 us` to `763.70 us` at `10k` rows (`-3.91%`)

Earlier focused validation in the same completion pass also showed strong positive movement in join-root and lookup-aggregate buckets, including limit-sensitive join cases and indexed scalar aggregates.

## What Still Remains

There is still useful performance work left, but it is no longer roadmap-blocking for this item.

The remaining work is now in the "keep optimizing the foundation" category:

- broader specialized kernels for hot numeric and text shapes
- optional SIMD-friendly execution where measurement justifies the added complexity
- wider benchmark guardrails and PR automation for batch-preserving plans
- additional planner and operator micro-optimizations on top of the batch-first base

Those are follow-on optimization tracks, not reasons to keep the core roadmap item open.
