# SQL Batched Row Transport Design

## Summary

CSharpDB's executor still fundamentally transports one logical row at a time through `IOperator`.

That is true even after the recent compact scan, compact payload, aggregate, lookup, and join optimizations. Some operators now batch internal work, but the public executor contract is still:

- `MoveNextAsync()`
- one `DbValue[] Current`
- optional row-buffer reuse via `ReusesCurrentRowBuffer`

That contract keeps the engine simple, but it also leaves performance on the table for scan-heavy, projection-heavy, and join-heavy workloads. The next larger executor step is to move from operator-local batching toward true batched row transport between operators.

## Why This Exists

Recent work proved three things:

- compact table-scan and compact payload projection paths benefit from local batching on expression-heavy work
- generic projection and filter/projection operators can also benefit from batched expression shaping
- the current gains are bounded because every operator boundary still collapses back to single-row transport

In other words, the engine now has multiple local batching islands, but not a batched execution model.

This design is about replacing that fragmentation with a consistent internal row-batch transport.

## Problem Statement

The current `IOperator` shape in `src/CSharpDB.Execution/IOperator.cs` creates several costs:

- every operator boundary requires row-by-row control flow
- expressions are still invoked one row at a time even when upstream work is scan-oriented
- filters and projections cannot share a stable batch buffer across multiple operators
- joins and sorts still frequently re-materialize rows into per-row arrays
- any future vectorized work has to be implemented as operator-local special cases instead of using a shared transport model

That is manageable for lookup-heavy workloads, but it is increasingly limiting for:

- large filtered scans
- projection-heavy scans
- grouped or aggregate pipelines over large row sets
- join pipelines that touch many rows even after planner specialization

## Goals

- introduce a true internal batch transport between operators
- reduce per-row control-flow overhead in scan-heavy execution
- reduce `DbValue[]` churn across operator boundaries
- make expression, filter, and projection execution batch-aware
- give joins, aggregates, and sorts a common batch-shaped input model
- preserve existing SQL semantics, ordering behavior, and null semantics

## Non-Goals

- changing the on-disk row format
- replacing `DbValue`
- adding SIMD-specific logic in the first phase
- forcing every operator to become batch-native immediately
- breaking the public query surface

## Current Constraints

Today the executor is centered on:

- `IOperator.Current : DbValue[]`
- `IOperator.MoveNextAsync()`
- `IRowBufferReuseController`

That contract is deeply embedded in:

- planner output in `QueryPlanner`
- scan, filter, projection, join, sort, and aggregate operators
- `QueryResult`
- benchmark and test expectations

So the migration cannot be a big-bang replacement. It needs compatibility layers.

## Proposed Direction

### 1. Add a batch-native internal contract

Add a new internal transport alongside `IOperator`, rather than replacing `IOperator` immediately.

Recommended shape:

- `IBatchOperator`
- `RowBatch`
- `BatchExecutionResult` or equivalent move-next-batch status

The important point is that operators should be able to exchange multiple rows per call without flattening back to one `DbValue[]` at every boundary.

### 2. Use a flat row-major batch first

The first batch transport should be practical, not idealized.

Recommended first representation:

- one flat `DbValue[]` storage buffer sized `capacity * columnCount`
- `Count`
- `ColumnCount`
- optional active row mask or row count after filtering

Why row-major first:

- it maps cleanly to the existing `DbValue[]`-based expression compiler
- it minimizes migration risk versus a fully columnar rewrite
- joins, sorts, and hash structures can adopt it incrementally

This is not the final word on columnar/vectorized execution. It is the best bridge from the current executor to a real batch model.

### 3. Make ownership explicit

The batch contract must define:

- whether the producer can reuse the current batch buffer
- when a consumer must copy
- whether filtered rows compact in place or use an active-row mask

Recommended rule:

- batches are reusable by default
- consumers that retain rows beyond the next `MoveNextBatchAsync()` must copy
- phase one should compact accepted rows in place instead of introducing a mask everywhere

That keeps the first implementation easier to reason about.

## Proposed Types

### `RowBatch`

Suggested responsibilities:

- own the flat `DbValue[]` storage
- expose `Count` and `ColumnCount`
- provide `GetRowSpan(int rowIndex)` for operator-local work
- provide `CopyRowTo(int rowIndex, DbValue[] destination)` for adapters
- support reset/reuse between batch fills

Suggested fields:

- `DbValue[] Values`
- `int Count`
- `int Capacity`
- `int ColumnCount`

### `IBatchOperator`

Suggested shape:

- `ColumnDefinition[] OutputSchema`
- `bool ReusesCurrentBatch`
- `RowBatch CurrentBatch`
- `ValueTask OpenAsync(...)`
- `ValueTask<bool> MoveNextBatchAsync(...)`

This mirrors `IOperator`, but at batch granularity.

### Adapters

Two adapters are needed early:

- `RowToBatchAdapter`
  Wrap an existing row-based operator so downstream batch-native operators can consume it.
- `BatchToRowAdapter`
  Wrap a batch-native operator so existing row-based operators and `QueryResult` can still consume it.

These adapters are what make phased migration possible.

## Migration Strategy

### Phase 1: Infrastructure

Deliverables:

- `RowBatch`
- `IBatchOperator`
- row/batch adapters
- batch-aware benchmark coverage

No planner changes yet beyond targeted experiments.

### Phase 2: Batch-native scan sources

First native producers should be:

- table scan
- ordered index scan
- hashed/range index scan where payload iteration is already scan-like

Why these first:

- they are the most natural batch producers
- they dominate many scan-heavy pipelines
- they already feed the compact projection work

### Phase 3: Batch-native filter and projection

Next operators:

- filter
- projection
- fused filter/projection
- compact scan/payload projection variants

This is where the current operator-local batching can be replaced with shared transport.

### Phase 4: Batch-native aggregates

Next targets:

- grouped scans
- hash aggregate build/update
- simple scalar aggregates over batched input

This is likely the highest-return follow-on after scan/projection transport is stable.

### Phase 5: Batch-native joins

Next targets:

- hash join probe/build
- nested-loop join output shaping where row counts are moderate
- lookup joins where the outer side can batch probes

This phase should only happen after the batch contract is stable, because joins have the highest semantic and memory complexity.

### Phase 6: Optional columnar specialization

Only after row-major batch transport is established should the engine consider:

- columnar batch views for specific operators
- SIMD-friendly numeric loops
- type-specialized aggregate kernels

That is a second design layer, not the starting point.

## Operator Priority

Best first operator set:

1. `TableScanOperator`
2. `IndexOrderedScanOperator`
3. `IndexScanOperator`
4. generic `ProjectionOperator`
5. generic `FilterProjectionOperator`
6. compact projection operators

Avoid first:

- sort
- DISTINCT
- hash join
- generic grouped aggregate

Those should consume batches only after the producer and projection path is stable.

## QueryResult Compatibility

`QueryResult` should remain row-oriented at the public edge in the first implementation.

Recommended approach:

- keep external enumeration unchanged
- add a `BatchToRowAdapter` before `QueryResult` when the root operator is batch-native

That preserves API compatibility while allowing the executor interior to evolve.

## Expression Execution

The current expression compiler works against `DbValue[]` rows.

The first batch design should not replace that compiler. Instead:

- expose row spans or temporary row views over `RowBatch`
- let the compiler continue evaluating one logical row at a time inside a batched outer loop

This still reduces:

- operator boundary overhead
- repeated buffer allocation
- repeated virtual dispatch per row

Later work can specialize expression kernels if the benchmarks justify it.

## Memory Model

The first implementation should prefer:

- reusable fixed-capacity batches
- predictable buffer lifetimes
- low-copy flow between producer and consumer

Batch size should be configurable internally, but the first default should be conservative, for example:

- `64` rows for projection-heavy operators
- possibly `128` or `256` for simpler scan/filter paths after measurement

The first design should not overfit batch size up front.

## Benchmark Plan

New benchmark coverage should be added for:

- full scan `SELECT *`
- filtered scan `SELECT *`
- filtered scan with expression projection
- indexed range scan with expression projection
- join with expression projection
- grouped aggregate over scan-heavy input

Success criteria should be measured against current row transport:

- lower allocation
- fewer operator-local buffers
- lower mean latency on `10K` and `100K` scan-heavy cases

## Risks

- batch adapters may erase early gains if they are inserted too often
- row-major batches may still leave some SIMD/vector opportunities unrealized
- sort/join operators may need their own retained-batch copy strategy
- debugging correctness regressions gets harder when a bug affects multiple rows per call

These are acceptable if the rollout stays phased.

## Recommended Starting Point

The first implementation pass should do only this:

1. add `RowBatch`
2. add `IBatchOperator`
3. add adapters
4. make table scan and generic projection/filter-projection batch-native
5. benchmark scan and join expression-projection shapes against the current row-based path

That is enough to prove whether true batch transport is worth rolling further into aggregates and joins.

## Recommended Position

True batched row transport should be treated as the next major SQL executor design, not as a collection of operator-local micro-optimizations.

The right path is:

- batch-native transport first
- operator migration second
- columnar/vectorized specialization later

That keeps the design compatible with the current engine while creating a real foundation for the next tier of SQL performance work.
