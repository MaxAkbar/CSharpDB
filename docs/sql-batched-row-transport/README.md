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

## Current Status

As of March 15, 2026, the branch has a partial retained implementation:

- `RowBatch` and `IBatchOperator` exist in `src/CSharpDB.Execution/IOperator.cs`
- `RowSelection`, `IFilterProjectionBatchPlan`, and a delegate-backed compatibility plan now exist in `src/CSharpDB.Execution/BatchEvaluation.cs`
- a narrow `BatchPlanCompiler` and `SpecializedFilterProjectionBatchPlan` now exist for simple integer predicates and simple integer-or-column projections, but they are still internal opt-in infrastructure rather than planner-driven behavior
- several scan/filter/projection/sort/distinct operators can already produce or consume batches internally
- `QueryResult` in `src/CSharpDB.Execution/QueryResult.cs` now supports storing batch-native roots directly instead of always wrapping them in `BatchToRowOperatorAdapter`
- partial-consumption behavior is covered: a caller can `MoveNextAsync()` on a batch-backed result and then call `ToListAsync()` without losing the remainder of the current batch

The first retained benchmark signal for that specialized plan is positive on the internal operator path in `tests/CSharpDB.Benchmarks/Micro/BatchEvaluationBenchmarks.cs`:

- projection-only batch plan: about `378.8 us` down to `345.5 us` over `16,384` rows
- filter + projection batch plan: about `221.4 us` down to `208.8 us` over `16,384` rows

That is enough to justify keeping the specialized-plan direction, but not enough yet to route production queries through it blindly.

What is not solved yet:

- the generic expression/filter/projection boundary still pays too much per-row cost
- batch transport is still not cheap enough once the engine falls back to `Func<DbValue[], DbValue>`-style evaluation
- adapter-heavy transport broadening has not produced stable wins
- the new batch-plan contract is only wired as an internal opt-in path so far; it is not planner-driven yet

That means the next step is no longer "add more adapters" or "add a thin row-view wrapper". The next step is changing the evaluation contract inside the executor.

## Pause Decision

As of March 15, 2026, this workstream should be treated as paused.

Reason:

- the retained groundwork is useful and should stay
- the small and medium follow-on experiments have been exhausted
- repeated production rollouts did not produce stable enough wins to justify more local iteration
- the next plausible gain is a deeper evaluator rewrite, not another incremental patch

That means the project should not come back here for:

- more adapter experiments
- more alternate delegate shapes
- more narrow planner hooks over the current evaluator model
- more compact-operator-specific special cases

If none of the deeper rewrite prerequisites are available, the right decision is to move to another performance area instead of spending more time here.

## Revisit Gate

Only reopen this work when all of the following are true:

1. there is explicit willingness to do a deeper SQL executor rewrite rather than another bounded experiment
2. the rewrite target is a new evaluator contract, not another wrapper over `Func<DbValue[], DbValue>`
3. benchmark success criteria are agreed up front for scan-heavy production shapes, not just internal microbenchmarks
4. the rollout starts behind a narrow gate and is measured against the current stable executor baseline

If those conditions are not met, this document should be read as historical design context plus retained infrastructure notes, not as an active next task.

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

In practice, adapter use needs to stay narrow. Recent branch work showed that broadening adapter use too aggressively can erase the gains from batched transport, especially on join-expression and scan-projection workloads.

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
- allow `QueryResult` to hold either an `IOperator` or an `IBatchOperator`
- materialize rows only at the public boundary when callers enumerate rows or call `ToListAsync()`

This is now the retained branch direction. Earlier versions used `BatchToRowAdapter` at the boundary, but the current implementation keeps batch-native roots intact for longer while preserving the public API.

## Tried And Backed Out

The following approaches were already implemented and benchmarked on this branch, then reverted because the results were not good enough.

### 1. Broad `RowToBatch` adapter expansion

Tried:

- adding a `RowToBatch` adapter and wiring generic batch-capable consumers to request "a batch source" unconditionally
- pushing that through scan/filter/projection consumers, `SortOperator`, `DistinctOperator`, aggregate consumers, and join-side batch build/probe paths

Result:

- scan-projection results became mixed to worse
- join-expression rows regressed materially

Conclusion:

- do not broaden batching further by layering adapters over row-mode operators
- adapters are acceptable as narrow compatibility seams, not as the main strategy

### 2. Batch-aware expression evaluator seam over the current compiler

Tried:

- adding a batch-row evaluation seam around the existing `ExpressionCompiler`
- wiring generic filter/projection operators to evaluate over that seam

Result:

- join-expression benchmarks regressed enough that the work was backed out

Conclusion:

- the next evaluator step cannot be just another wrapper around `Func<DbValue[], DbValue>`
- the evaluator contract itself needs to change

### 3. Parallel compiled batch-expression trees over the current compiler model

Tried:

- compiling a second cached evaluator path for generic `ProjectionOperator` and `FilterProjectionOperator`
- evaluating batch rows through precompiled batch-expression objects instead of the normal per-row `Func<DbValue[], DbValue>` delegates

Result:

- filtered scan projection improved slightly
- join expression projection rows regressed enough to erase the scan win
- the change was backed out

Conclusion:

- a parallel evaluator object tree layered on the current expression compiler model is still too expensive on important join shapes
- the next rewrite needs a new internal evaluator contract, not a second cached evaluator stack over the existing `DbValue[]` compiler model

### 4. Additional operator-local transport experiments

Tried and reverted:

- batch-side `TopNSortOperator` consumption
- batch output from hash join for projected join consumers
- direct materialized-row fast paths in `BatchTransport`

Result:

- no stable broad win
- too much risk of making important query shapes worse

Conclusion:

- stop spending time on isolated operator-local transport tweaks
- move to a deeper shared execution contract instead

### 5. `ReadOnlySpan<DbValue>` compiled delegate evaluators

Tried:

- compiling a second cached evaluator path over `ReadOnlySpan<DbValue>`
- wiring generic `ProjectionOperator` and `FilterProjectionOperator` batch paths to use those span-based delegates instead of copying rows into temporary `DbValue[]` buffers

Result:

- scan-projection benchmarks regressed versus the current stable batch baseline
- join-expression rows regressed more materially
- the change was backed out

Conclusion:

- even a narrower span-based delegate path is not enough if it still mirrors the current expression-compiler model
- the next retained step needs a deeper evaluator contract, not another alternate delegate shape layered beside `Func<DbValue[], DbValue>`

### 6. Narrow bytecode evaluator plus selection-vector filtering

Tried:

- compiling a compact bytecode form for common literals, column refs, arithmetic, comparisons, boolean operators, and `IS NULL`
- wiring generic `ProjectionOperator` and `FilterProjectionOperator` batch paths to use that bytecode when available
- adding a simple selected-row index buffer for batched filter/projection instead of projecting every candidate row immediately

Result:

- scan projection regressed clearly versus the retained baseline
- filtered scan + column projection moved from about `738.0 us` to `816.8 us` at `10K`, and from `57.09 ms` to `62.04 ms` at `100K`
- filtered scan + expression projection moved from about `711.5 us` to `929.1 us` at `10K`, and from `57.76 ms` to `62.83 ms` at `100K`
- the change was backed out before spending more time validating join shapes

Conclusion:

- a bytecode layer is not enough if the executor still pays the current generic row/evaluator costs around it
- selection-vector filtering by itself does not recover the lost time
- the next retained step still needs a deeper shared evaluator contract, not another execution engine bolted onto the current expression boundary

### 7. Planner-routed delegate-backed batch plan for generic expression projection

Tried:

- keeping the new `RowSelection` and `IFilterProjectionBatchPlan` scaffolding
- routing generic expression projection and filter+expression projection through `DelegateFilterProjectionBatchPlan` whenever the source was already batch-capable

Result:

- expression-heavy scan rows improved, but not enough to justify the regressions elsewhere
- filtered scan + expression projection improved to about `742.1 us` at `10K` and `59.27 ms` at `100K`
- join expression projection regressed to about `352.4 us`
- join filter + expression projection regressed to about `344.0 us`
- the planner hook was backed out, but the shared batch-plan infrastructure was retained

Conclusion:

- the contract scaffolding is worth keeping
- the delegate-backed compatibility implementation is not good enough as a planner-routed production path
- the next retained use needs a more specialized evaluator on top of the new contract, not direct routing of the compatibility layer

### 8. Planner-routed specialized batch plan for generic expression projection

Tried:

- keeping the new `BatchPlanCompiler` and `SpecializedFilterProjectionBatchPlan`
- routing the generic planner expression path through that specialized plan whenever the source was already batch-capable and the SQL shape was supported

Result:

- the retained internal contract benchmark stayed positive
- broad production routing was not good enough:
  - join with expression projection landed around `323.5 us`, which was effectively flat
  - join with filter + expression projection regressed to about `340.6 us`
  - scan expression rows were not a clear enough win to justify keeping the planner route
- the planner hook was backed out

Conclusion:

- the specialized plan implementation is worth keeping as infrastructure
- it is not yet good enough as a generic planner-routed production path
- the next use needs a narrower production target or a deeper evaluator specialization before planner rollout

### 9. Narrow specialized-plan rollout in compact scan / compact payload expression operators

Tried:

- routing the specialized plan only through `CompactTableScanProjectionOperator` and `CompactPayloadProjectionOperator`
- keeping the broad generic join path untouched

Result:

- the scan-heavy production signal was still not good enough to keep:
  - filtered scan + expression projection landed around `734.9 us` at `10K` and `57.28 ms` at `100K`
  - compact indexed range expression projection landed around `45.63 ms` at `100K`
- that did not clearly beat the retained baseline enough to justify the extra runtime path
- the planner and compact-operator rollout was backed out

Conclusion:

- even the compact-operator production rollout is not yet compelling
- the specialized plan/compiler is still useful retained infrastructure
- the next promotion target must be narrower again, or the plan itself needs deeper specialization before production use

### 10. Direct row-specialized compact fast-path rollout

Tried:

- keeping the compact single-table fast path in place
- adding a narrower row-specialized use of `SpecializedFilterProjectionBatchPlan` inside `CompactTableScanProjectionOperator`
- avoiding the earlier double-buffered source-batch-to-destination-batch path and instead decoding once into the compact row buffer, then letting the specialized plan filter/project directly into the output row or batch

Result:

- the code was functionally correct, and focused integration tests stayed green
- the production benchmark signal was still not good enough to keep:
  - filtered scan + expression projection landed around `761.4 us` at `10K` and `59.81 ms` at `100K`
  - filtered scan + column projection landed around `750.5 us` at `10K` and `58.94 ms` at `100K`
- compared to the retained stable baseline, the expression path was still slower on the important scan-heavy shape
- the rollout was backed out and the branch was returned to the prior stable executor baseline

Conclusion:

- a direct row-specialized hook inside the compact fast path is still not enough by itself
- the remaining cost is deeper than the current compact scan wrapper and plan dispatch
- the next meaningful gain needs a more fundamental evaluator contract change, not another specialized hook bolted onto the current compact operator

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

## If Revisited

If this area is reopened later, the first pass should not be another adapter pass.

It should do this:

1. keep the retained direct-batch `QueryResult` boundary
2. design a new internal evaluator contract that is not centered on `Func<DbValue[], DbValue>`
3. make generic `ProjectionOperator` and `FilterProjectionOperator` consume that contract first
4. benchmark scan and join expression-projection shapes again
5. only after that, extend the same evaluator path to compact scans, aggregates, and joins

That is the smallest plausible restart point that does not immediately repeat the already-rejected adapter-heavy and row-accessor-wrapper approaches.

## Recommended Position

True batched row transport should be treated as the next major SQL executor design, not as a collection of operator-local micro-optimizations.

The right path is:

- batch-native transport first
- operator migration second
- columnar/vectorized specialization later

Today that remains strategically true, but tactically this track is paused. The retained code and this document should be used as a checkpoint if the deeper rewrite is funded later; otherwise the team should move to a different performance area.
