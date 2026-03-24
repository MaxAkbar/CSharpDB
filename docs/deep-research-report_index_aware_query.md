# Index-Aware Prepared Access Plans For CSharpDB

This document replaces the earlier research-style draft with a CSharpDB-specific recommendation.
It is intentionally pragmatic:

- anchor the design in code that already exists in this repo
- favor the highest-ROI read-path improvements first
- give only ballpark performance estimates
- keep more speculative ideas separate from the core plan

## Executive Summary

CSharpDB already has most of the machinery needed for index-aware compiled execution:

- fast-path classification for PK and indexed lookups in `QueryPlanner`
- a select plan cache
- prepared command plumbing in the ADO.NET layer
- cache-only B-tree lookup APIs
- index-aware covered and compact projection paths
- index nested-loop join planning

Because of that, the best next step is not "build a full query compiler."
The best next step is to add a parameter-native prepared access-plan layer that:

1. reuses the chosen access path across executions
2. binds directly to B-tree and index primitives
3. reuses compiled predicates and projections
4. falls back cleanly to the current operator pipeline when assumptions stop holding

If implemented well, this should improve repeated in-memory OLTP-style reads by roughly 10% to 25% overall, with some narrow query shapes reaching 30%+.
The larger wins in this codebase are still likely to come from broader covering/index-only execution, fewer allocations, and better locality tricks rather than from deeper runtime code generation alone.

## Where The Engine Already Is

The current codebase is already closer to "compiled access paths" than a typical generic SQL engine.

### Planner And Fast Paths

- `QueryPlanner` already classifies SELECT shapes and caches the result.
- Simple PK equality and indexed lookup shapes already bypass much of the generic planner path.
- The planner already pushes simple filters into pre-decode checks and can trim decode work to only the referenced columns.
- Covered index and compact projection paths already exist for several lookup and range cases.
- Index nested-loop joins already exist when the join shape matches an indexable inner side.

### Storage Primitives

- `BTree.TryFindCachedMemory` gives a cache-only hot lookup path.
- `BTree.FindMemoryAsync` gives the general point-read path.
- `BTreeCursor.SeekAsync` gives the right primitive for compiled range probes.
- Index stores already expose cache-aware lookup behavior and B-tree-backed cursor scans.

### Prepared Statements

Prepared command support already exists in the ADO.NET layer.
That is useful, but it is not the same thing as a reusable prepared access plan.

Today the prepared path still binds parameters into a statement shape before executing it.
That means the next optimization step is to compile against the query template and parameter slots directly, not just against each bound statement instance.

## Current Performance Snapshot

The most useful benchmark signals already in the repo:

| Metric | Current Result | Notes |
|--------|----------------|-------|
| SQL PK lookup (10K rows) | 519 ns | Hot point-read path is already very fast |
| SQL PK lookup (100K rows) | 729 ns | Still sub-microsecond |
| SQL indexed lookup (100K rows) | 677 ns | Secondary equality is also already fast |
| Query-plan cache stable SQL | 1.013 ms | Plan reuse exists, but not at the prepared access-plan level |
| Query-plan cache pre-parsed | 1.003 ms | Pre-parsed still looks close to stable SQL |
| Unique index lookup `SELECT *` | 6.88 us | Secondary lookup plus base-row fetch |
| Unique index lookup `SELECT id` | 5.78 us | Covered projection already helps |
| Non-unique index lookup `SELECT *` | 388.88 us | Duplicate-key row fetch dominates |
| Non-unique index lookup `SELECT id` | 378.87 us | Covered projection helps allocations more than latency |
| `ORDER BY value LIMIT 100` index-order scan | 37.47 us | Ordered index path, still fetching rows |
| `ORDER BY value LIMIT 100` covered index-order scan | 19.99 us | Strong signal that index-only work is high leverage |
| `WHERE value BETWEEN ...` row fetch | 50.48 ms | Base-row fetch dominates |
| `WHERE value BETWEEN ...` covered projection | 15.88 ms | About 3x faster on the covered path |
| Composite join covered lookup | 440.8 us | Covered index lookup join already beats forced hash here |

The main read on these numbers:

- hot point reads are already very optimized
- wider scan/range shapes benefit more from staying on index data
- non-unique secondary lookups still pay a lot for row fetch and materialization
- the next big improvement is probably not "more planner intelligence"
- the next big improvement is likely "reuse more work" and "touch less row data"

## Recommendation Change

The earlier draft leaned toward "compiled access kernels" as the headline.
That is still directionally right, but the priority order should change.

### Old Framing

- prepared plan abstraction
- compiled PK and secondary probes
- compiled join probes
- optional IL emit
- optional source generation

### Better Framing For This Repo

1. Parameter-native prepared access plans
2. Allocation reduction on hot lookup paths
3. Broader covering/index-only execution
4. Compiled probe kernels for repeated PK, unique secondary, non-unique secondary, and range shapes
5. Compiled join probes
6. Optional IL emit or source generation only if the simpler plan leaves measurable CPU overhead

This matters because CSharpDB already has strong handwritten fast paths.
The first thing to optimize now is reuse and materialization cost, not just control-flow overhead.

## Recommended Architecture

Add two concepts:

### 1. `BoundAccessPath`

This is a planner product.
It should describe the chosen lookup strategy without yet being tied to specific runtime parameter values.

Suggested contents:

- target table schema
- chosen index schema, if any
- key columns and key-building strategy
- projection coverage information
- residual predicate placement
- range/order metadata
- join inner-side probe metadata, when applicable

### 2. `PreparedAccessPlan`

This is the reusable executable object.
It should be cached by normalized query shape plus parameter type signature rather than by bound statement object identity.

Suggested contents:

- plan fingerprint
- schema version stamp
- bound trees and index stores
- compiled key builder delegates
- compiled predicate/projection delegates
- executor delegate or executor object
- counters for cache hits, fallbacks, and selectivity
- optional locality hints such as last-leaf or last-range metadata

### Execution Flow

```text
SQL template / prepared command
  -> parse once
  -> bind schema and choose BoundAccessPath
  -> build PreparedAccessPlan
  -> execute with parameter values
  -> if assumptions fail, fall back to current operator pipeline
```

## Phased Integration Plan

### Phase 1 - Add Prepared Access Plan Infrastructure

Goal:
Create a prepared access-plan cache parallel to the current statement cache and select-plan cache.

Work:

- extract planner logic that identifies PK, secondary equality, range, and join lookup shapes
- produce `BoundAccessPath` objects from that logic
- cache `PreparedAccessPlan` by normalized query shape plus parameter type signature
- invalidate by schema version

Why first:
This is the lowest-risk way to turn existing fast-path logic into something reusable.

Ballpark gain:
`5% to 15%` on repeated prepared reads before any deeper specialization.

### Phase 2 - Parameter-Native PK And Unique Secondary Plans

Goal:
Compile the simplest and most common repeated point-read shapes.

Work:

- PK equality: cache-only lookup first, async fallback second
- unique secondary equality: direct index probe, then one row fetch or covered projection
- reuse compiled predicates and projection builders
- specialize scalar, single-row, and single-column result creation to cut allocations

Why second:
This is the cleanest place to turn prepared statements into true prepared access plans.

Ballpark gain:
`10% to 30%` on prepared hot point reads, mostly from less binding, less planning, and less allocation churn.

### Phase 3 - Non-Unique Equality, Range, And Ordered Scans

Goal:
Handle the shapes where row fetch and repeated decode work still dominate.

Work:

- compiled non-unique secondary probes
- compiled `SeekAsync` plus bounded iteration for integer ranges
- prepared compact-projection and covered-projection reuse
- optional rowid batching by likely page locality

Why third:
This is where broader read-path throughput can improve materially.

Ballpark gain:
`15% to 35%` on repeated non-covered range or duplicate-key index workloads.
Covered/index-only range shapes can still do much better than that when row fetch disappears.

### Phase 4 - Compiled Join Probe Plans

Goal:
Reuse the inner probe shape of index nested-loop joins.

Work:

- compile the inner key extraction and probe sequence
- reuse residual predicate placement
- preserve current fallback to hash join or generic join execution

Why fourth:
The current join planner is already doing useful work.
The gain here comes from not repeating inner-side probe setup for each execution, not from inventing a new join algorithm.

Ballpark gain:
`10% to 25%` on repeated lookup-join workloads.

### Phase 5 - Optional IL Emit Or Source Generation

Goal:
Only pursue if earlier phases still show real CPU overhead on hot paths.

Work:

- optional `DynamicMethod` fusion of probe + decode + predicate + projection
- optional source-generated stored query/procedure shapes for AOT-friendly environments

Why later:
This is harder to debug, harder to maintain, and less likely to beat the lower-risk wins above until the simpler work is already done.

Ballpark gain:
Usually incremental, not transformational, on top of the earlier phases.

## Better-Than-The-Original Ideas

These are the ideas I would seriously consider in addition to the prepared access-plan work.

### 1. Kill Allocations Before Chasing More Compilation

The hot lookup path is already fast, but it still allocates.
Special-case result containers for:

- scalar aggregate results
- single-row single-column lookups
- single-row fixed-width projections

This is a very plausible "better than expected" win because it cuts both CPU and GC overhead.

Expected upside:
`15% to 35%` on the hottest read paths.

### 2. Broader Covering Index Support

This is likely the highest-ROI feature after prepared access plans.

Possible options:

- richer covered projection recognition
- secondary index payloads that keep more projected values
- optional `INCLUDE`-style non-key payload columns

The benchmark signals already show that index-only execution can be a much bigger win than planner overhead reduction.

Expected upside:
`1.5x to 3x` on affected lookup, range, and top-N queries.

### 3. Sticky Leaf Hints Per Prepared Plan

Store the last successful leaf range or likely page neighborhood and try it before a full descent.
This is a lightweight locality assist and fits the current B-tree design better than a full learned-index experiment.

Expected upside:
Usually small but real on hot skewed workloads, maybe `5% to 15%`.

### 4. Tiny Exact-Result Cache Per Prepared Plan

For highly skewed lookup workloads, cache the exact projected result for the last few parameter tuples and invalidate on the table write epoch.

This is not a general SQL cache.
It is a narrow hot-key accelerator.

Expected upside:
Very workload-dependent.
Could be negligible on random access and dramatic on skewed repeated reads.

### 5. RowId Locality Batching For Non-Unique Index Hits

Duplicate-key lookups currently spend a lot of time chasing many rowids into base-row fetches.
If those rowids can be grouped by likely page locality, some non-unique lookups may improve more than they would from compile-time work alone.

Expected upside:
`10% to 25%` on duplicate-heavy workloads.

### 6. Expand The String-Level Fast Front Door

The engine already special-cases simple insert and simple PK lookup text before the full planner path.
Extending that idea to simple indexed equality and simple integer range SQL would help workloads that never prepare commands.

Expected upside:
Helpful for literal-heavy apps, probably modest overall but cheap to ship.

### 7. Add Batched Point-Probe APIs

An `IN (...)` or multi-get style batched point probe may outperform many micro-optimizations because it amortizes planner, snapshot, and page-touch overhead across several lookups.

Expected upside:
Can be large for apps that naturally issue bursts of point reads.

## Ballpark Performance Outlook

These are intentionally rough estimates:

| Change | Likely Gain |
|--------|-------------|
| Parameter-native prepared access plans | `10% to 25%` overall on repeated prepared reads |
| Hot path allocation reduction | `15% to 35%` on hot point reads and scalar/small result shapes |
| Broader covering/index-only execution | `1.5x to 3x` on affected queries |
| Sticky leaf hints / locality aids | `5% to 15%` on hot skewed workloads |
| Rowid locality batching | `10% to 25%` on duplicate-heavy secondary lookups |
| IL-emitted fused kernels | usually incremental after the above |
| Cold file-backed reads | `0% to 5%` from these changes alone |

If I had to state one realistic headline:

For repeated in-memory OLTP-style reads, the combined package of prepared access plans plus allocation reduction plus broader covering support has a decent chance of producing a visible gain.
For truly hot narrow workloads it may feel much better than the average.
For cold I/O-bound reads it will not move the needle much.

## Benchmark Plan

Use the existing benchmark suite and add focused prepared-plan cases.

### Add

- prepared PK equality with reused command and varying parameter values
- prepared unique secondary equality with reused command
- prepared non-unique equality on duplicate-heavy data
- prepared integer range with compact projection
- prepared covered integer range
- prepared lookup join with repeated executions
- allocation-focused small-result benchmarks
- skewed workload benchmark for tiny exact-result cache and leaf hints

### Keep Watching

- mean latency
- allocations
- p95 where relevant
- cache-only hit rate
- fallback rate
- rowids fetched per probe
- row payload bytes decoded per result row

## Risks And Guardrails

- Do not cache page-backed payload memory across writes.
- Treat compiled access plans as an optimization, not as a new correctness boundary.
- Keep schema-version invalidation strict.
- Always allow fallback to the current operator pipeline.
- Keep IL emit optional and isolated.
- Do not let speculative locality hints become correctness-critical.

## Final Recommendation

If CSharpDB wants the next practical read-performance step, the roadmap should be:

1. implement parameter-native prepared access plans
2. reduce allocations on hot lookup/result paths
3. broaden covering/index-only execution
4. add compiled reusable probe kernels for repeated equality and range shapes
5. add a few small locality tricks such as sticky leaf hints and rowid batching
6. only then evaluate IL emit or source generation

That ordering is more consistent with the current codebase and with the benchmark signals already in the repo.
It also gives the best chance of landing measurable wins without over-engineering the first version.
