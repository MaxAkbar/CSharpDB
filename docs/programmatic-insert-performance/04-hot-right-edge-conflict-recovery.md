# Plan 4: Hot Right-Edge Conflict Recovery

## Goal

Answer one question cleanly:

How much of the remaining hot right-edge insert limit is still structural
conflict-recovery work, and how much is now just the durable commit ceiling?

This plan is a continuation of the Plan 3 failure-boundary rows. It is not the
SQLite throughput race.

## Fixed Controls

- Shared `Database` instance.
- File-backed durable mode.
- One row per commit.
- Hot right-edge inserts only.
- Primary rows use explicit IDs.
- Primary comparison keeps:
  - writer counts `4` and `8`
  - durable group-commit windows `0` and `250us`
- Compare shared auto-commit concurrent inserts against explicit
  `WriteTransaction` inserts under the same workload.

## In Scope

- committed-leaf split fallback
- dirty-parent committed-split recovery
- traversal capture and traversal retargeting
- `MissingTraversal` split-fallback preconditions
- `SplitFallbackShape` structural coverage
- pager and B-tree correctness regressions exposed by committed right-edge split
  retries

## Out Of Scope

- SQLite comparison tables
- `InsertBatch` or single-writer bulk API comparisons
- row-encoding or serialization micro-optimization
- buffered or in-memory durability variants
- disjoint explicit-key success-case analysis already covered by Plan 3

## Current Evidence

- The checked-in hot right-edge concurrency harness is:
  `tests/CSharpDB.Benchmarks/Macro/InsertFanInDiagnosticsBenchmark.cs`.
- Commit-path reporting already exposes the right recovery counters through:
  `leafRebases`, `leafRejectReasons`, `leafSplitPreconditions`,
  `dirtyParentRecoveries`, `interiorRebases`, and `maxPendingCommits`.
- The recent committed-split recovery fixes eliminated the earlier
  dirty-parent-recovery failures in the focused hot rows, but they did not move
  the steady-state throughput ceiling.
- Latest local hot-right-edge spot checks still landed around `243-245
  commits/sec` with `commitsPerFlush = 1.00` and `dirtyParentRecoveries = 0`.
- The remaining reject tail is now mostly split-fallback preconditions and
  shape coverage, especially `MissingTraversal` and `SplitFallbackShape`.

Relevant code:

- `src/CSharpDB.Storage/Paging/Pager.cs`
- `src/CSharpDB.Storage/Paging/LeafInsertRebaseHelper.cs`
- `src/CSharpDB.Storage/Paging/InteriorInsertRebaseHelper.cs`
- `src/CSharpDB.Storage/BTree/BTree.cs`
- `tests/CSharpDB.Tests/BTreeCursorTests.cs`
- `tests/CSharpDB.Benchmarks/Macro/InsertFanInDiagnosticsBenchmark.cs`

## Experiment Matrix

### 1. Missing Traversal Cleanup

Drive down the `leafSplitPreconditions` bucket for missing traversal state in
the hot `W4/W8` rows:

- traversal capture on right-edge fast-path inserts
- traversal preservation across commit retries
- traversal retargeting after committed split recovery

Expected result:

- `MissingTraversal` approaches zero on the focused hot rows.

### 2. Split Fallback Shape Coverage

Extend structural recovery for the remaining committed split shapes that still
land in `leafRejectReasons` as `SplitFallbackShape`.

Expected result:

- structural rejects approach zero for the focused hot rows.

### 3. Recovery-Path Audit

Audit every successful recovery path to ensure the post-recovery traversal still
matches the committed tree shape:

- second committed right-edge split
- three-leaf repartition cases
- parent pivot rebasing
- interior insert rebasing

Expected result:

- no hidden "successful but stale traversal" paths remain.

### 4. Exit Check

After the structural tail is reduced, rerun the same hot rows and ask one
question:

does `commitsPerFlush` rise above `1.00`?

If not, the remaining limiter is no longer structural recovery. Move the work
to Plan 5.

## Reporting Rules

For every row, report:

- commits/sec
- rows/sec
- `P50`, `P95`, `P99`
- `commitsPerFlush`
- `leafRebases`
- `leafRejectReasons`
- `leafSplitPreconditions`
- `dirtyParentRecoveries`
- `interiorRebases`
- `maxPendingCommits`

This plan should be read as a conflict-recovery diagnostics table first and a
throughput table second.

## Deliverables

- one focused hot right-edge table that shows which structural reject families
  are still live
- one regression suite for committed-split recovery edge cases
- one short decision note that says whether the remaining hot right-edge limit
  is still structural or now purely throughput-bound

## Next Version Tasks

- [x] Add committed-right second-split fallback coverage.
- [x] Add committed three-leaf repartition fallback coverage.
- [x] Record traversal on successful right-edge fast-path inserts.
- [x] Retarget traversal after committed split recovery paths.
- [x] Add regression coverage for repeated committed right-edge split cases.
- [ ] Drive `MissingTraversal` close to zero on the hot `W8` rows.
- [ ] Drive `SplitFallbackShape` close to zero on the hot `W8` rows.
- [ ] Stop and move to Plan 5 if throughput remains pinned near one commit per
      flush after the structural tail is cleaned up.
