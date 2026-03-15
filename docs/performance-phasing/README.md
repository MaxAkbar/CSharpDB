# Performance Phasing Plan

## Summary

Collection storage and SQL execution should be improved as two related but separate tracks.

They overlap in a few important areas:

- both benefit from lighter-weight access over encoded bytes
- both benefit from delaying materialization until it is actually needed
- both benefit from pushing filters and index logic closer to the encoded payload
- both need better benchmark coverage around decode, scan, lookup, and index-only work

The right approach is to build the shared foundation first, then branch into SQL-specific and collection-specific phases.

## Why Phase This Work

Phasing matters for two reasons:

- the highest-value SQL work does not require a collection storage breaking change
- the collection `v3` migration is valuable, but it is also the riskiest and most disruptive step

This sequencing lets the project ship performance improvements earlier while reducing the chance of mixing too many architectural changes at once.

## Shared Areas

These are the main areas touched by both plans:

### Encoded payload access

Both systems need better access to encoded data without immediately expanding it into higher-level objects.

- collections need direct field/path reads from document payloads
- SQL needs direct column reads from row payloads

### Delayed materialization

Both systems want to avoid constructing full in-memory representations too early.

- collections want to avoid full `T` hydration
- SQL wants to avoid full `DbValue[]` row materialization

### Pushdown

Both systems benefit when more work happens before full decode:

- collections for index extraction and document field checks
- SQL for predicate evaluation and projection-aware reads

### Index coverage

Both systems need better logic for answering queries from encoded or indexed data alone.

- collections need richer path and expression indexes
- SQL needs more covering-index and index-only execution

### Benchmarking and profiling

Both plans need the same evidence loop:

- isolate decode and hydration cost
- measure scan and lookup behavior
- compare full-materialization vs lightweight-access paths

## Recommended Phases

### Phase 1: Shared benchmarks and profiling

Build a common measurement baseline before changing architecture.

Deliverables:

- benchmarks for collection hydration, SQL row decode, scan cost, lookup cost, and index-only opportunities
- allocation-focused measurement for collection reads and SQL scans
- representative workloads for point lookup, filtered scan, aggregate, and document access

Outcome:

- a defensible baseline for prioritizing later phases

Current phase `1` benchmark set:

- `SqlMaterializationBenchmarks`
  Measures full-row decode, selected-column decode, single-column access, and payload-level text/numeric checks.
- `CoveringIndexBenchmarks`
  Measures the gap between unique-index lookup shapes that could become index-only and lookup shapes that still need the base-row payload.
- `CollectionAccessBenchmarks`
  Measures full document hydration, key-only access, payload key matching, and direct indexed-field reads.
- `CollectionFieldExtractionBenchmarks`
  Measures early-field, middle-field, late-field, and miss-path extraction cost for the current collection JSON token scan.

Recommended phase `1` exit criteria:

- capture stable benchmark results for all four suites
- identify the largest SQL gap between row materialization and lighter-weight payload access
- identify the largest collection gap between full document hydration and direct field access
- confirm whether index-only SQL planning or collection field-reader work is the better phase `2` priority
- record allocation baselines so later phases can prove they reduced temporary object creation

### Phase 2: Shared encoded-access primitives

Add low-level readers and helpers that operate directly over encoded payloads.

Deliverables:

- lightweight row access helpers for SQL
- lightweight document field/path access helpers for collections
- shared rules for "read only what is needed"

Outcome:

- a common technical foundation without yet changing the storage format

### Phase 3: Shared non-breaking read-path wins

Use the new primitives to improve both systems without introducing a breaking format change.

Deliverables:

- broader predicate and key extraction directly from encoded payloads
- better projection-aware reads
- lower unnecessary materialization on hot paths

Outcome:

- early performance gains with lower migration risk

### Phase 4: SQL-specific execution improvements

Treat SQL as its own optimization track after the shared foundation is in place.

Deliverables:

- more covering-index and index-only planning
- reduced `DbValue[]` and `DbValue` materialization in hot operators
- more specialized aggregate, join, and lookup execution paths
- an internal batch-transport foundation for scan-heavy operators, as outlined in [SQL Batched Row Transport Design](../sql-batched-row-transport/README.md)

Outcome:

- meaningful SQL gains without waiting on collection storage redesign

### Phase 5: Collection `v3` storage redesign

Treat the collection redesign as a dedicated breaking-change phase.

Deliverables:

- native binary collection payload format
- direct binary hydration
- richer path-aware index support
- explicit migration tooling for existing JSON-backed collection data

Outcome:

- removal of the JSON tax on collection persistence and indexing

### Phase 6: Shared storage-layer enhancements

Apply lower-level storage improvements after the higher-level execution and materialization work has stabilized.

Deliverables:

- evaluate `mmap` read paths
- evaluate page-level compression
- measure storage wins against the new upper-layer baselines

Outcome:

- shared storage improvements applied after the bigger execution bottlenecks are better understood

## Recommended Ordering

The safest and most effective order is:

1. Phase 1
2. Phase 2
3. Phase 3
4. Phase 4
5. Phase 5
6. Phase 6

This order is deliberate:

- it gets measurable wins early
- it keeps SQL progress independent of the collection `v3` migration
- it reserves the breaking collection format change for a later, more controlled step

## What This Means In Practice

Near-term work should be biased toward:

- benchmark coverage
- low-level encoded-access helpers
- non-breaking read-path improvements
- SQL index-only and materialization reductions

Later work should be biased toward:

- collection binary payload migration
- storage-layer features that help both SQL and collections

## Recommended Position

The project should not try to solve SQL and collection performance with one storage-format change.

The shared work is real, but it sits mostly in access patterns, pushdown, and materialization control. The actual collection `v3` payload redesign should remain a dedicated later phase, while SQL performance should move sooner through executor and planner improvements.
