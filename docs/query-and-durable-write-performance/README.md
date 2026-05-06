# Query And Durable Write Performance

This note tracks the combined optimizer phase-2 and durable-write completion work that used to be split across roadmap bullets and an older low-latency write stub.

## Shipped In This Round

- `sys.table_stats` now exposes `row_count_is_exact` so exact `COUNT(*)` fast paths can stay exact-only while planner costing can still consume estimated row counts when necessary.
- Deferred advisory-stat persistence no longer blurs committed-row durability versus planner-stat freshness. Committed user rows remain WAL-durable per commit; advisory stats can reopen as stale and non-exact.
- `ANALYZE` now refreshes internal planner stats beyond NDV/min/max:
  - per-column equi-depth histograms
  - top-N frequent values for skewed equality / `IN` / same-column `OR`
  - composite-index prefix distinct-count summaries for multi-column correlation and join-domain estimates
- Cardinality estimation now prefers:
  - heavy hitters for equality and `IN`
  - histograms for numeric range interpolation
  - composite-prefix stats for correlated multi-column equality filters and joins
  - independence heuristics only after the richer stats are unavailable
- Inner-join reordering now uses bounded dynamic programming for small reorderable chains and falls back to the earlier greedy chooser above the cap or when estimates are missing.
- Raw page-copy batching is shared for snapshot/export-style paths, and logical table-copy loops now share one reusable B-tree copy utility instead of each maintenance path carrying its own copy loop.
- Opt-in durable group commit remains exposed through `UseDurableGroupCommit(...)`; this round keeps it expert-only and documentation-led rather than changing defaults.
- Shared auto-commit non-insert SQL writes on one `Database` can now use the same isolated commit path as explicit `WriteTransaction` work, so low-conflict `UPDATE` / `DELETE` contention can build real pending WAL commit fan-in instead of stalling above the queue.
- The current advanced-optimizer phase is closed: heavy-hitter equality, histogram range, composite-prefix correlation, non-unique lookup costing, hash build-side choice, and bounded small-chain join reordering are covered by implementation, tests, and diagnostic close-out benchmarks.
- The current async I/O batching phase is closed: WAL, checkpoint, snapshot/export, backup/restore staging, logical rewrite, and inspector scan paths have been audited and covered by diagnostic close-out benchmarks.

## Public Surface

- `sys.table_stats.row_count` keeps its existing numeric meaning.
- `sys.table_stats.row_count_is_exact` is the new explicit exactness bit.
- `sys.planner_histograms`, `sys.planner_heavy_hitters`, and `sys.planner_index_prefix_stats` expose stable SQL projections over the internal planner statistics.
- `EXPLAIN ESTIMATE FOR <query>` returns a bounded diagnostic rowset showing estimate sources, stale/missing-stat fallbacks, lookup decisions, join estimates, and join-reorder choices without executing the target query. The practical debugging guide is in [Debugging Slow Queries With EXPLAIN ESTIMATE](../query-execution-pipeline.md#debugging-slow-queries-with-explain-estimate).
- Adaptive re-optimization remains a separate future roadmap item, not a hidden requirement for the current optimizer phase.
- `UseWriteOptimizedPreset()` remains the default recommendation for durable file-backed workloads.
- `UseLowLatencyDurableWritePreset()` and `UseDurableGroupCommit(...)` remain opt-in measure-first knobs.
- Shared-`Database` implicit auto-commit is now split by workload shape:
  - non-insert SQL writes can queue behind the WAL pending-commit path and benefit from `UseDurableGroupCommit(...)`
  - one-row insert loops can opt into `ImplicitInsertExecutionMode.ConcurrentWriteTransactions`; hot right-edge and auto-ID rows now use shared row-id reservation plus pending leaf-page rebases to build WAL fan-in

## Close-Out Validation

The current advanced-optimizer and async I/O batching phases are backed by diagnostic benchmark suites rather than new public API:

```powershell
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --optimizer-closeout --repro
dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --async-io-closeout --repro
```

The May 5, 2026 optimizer close-out run showed `ANALYZE`-driven plans improving the targeted shapes by `1.06x-1.89x` on the local runner. The async I/O close-out run classified save/backup/restore as already batched, vacuum/FK migration as intentionally row-logical through `BTreeCopyUtility`, and inspector/WAL scans as specialized diagnostics.

## Future Work

- Adaptive runtime re-optimization is still future work.
- Raw histogram/prefix storage payloads remain internal; future diagnostics should extend stable SQL projections or add typed DTOs deliberately rather than exposing storage encodings.
- Durable group-commit guidance should keep following benchmark evidence, especially:
  - single-writer no-regression checks
  - 4-writer and 8-writer shared-`Database` contention runs
  - queue-depth, commits-per-flush, and latency percentile diagnostics
- The remaining phase-4 write-path question is now narrower than "shared auto-commit in general":
  - non-insert shared auto-commit fan-in is working
  - hot insert auto-commit now has an opt-in concurrent path, but bulk ingest guidance still starts with application-level batching
- Async I/O batching is done for the current phase; future work should be limited to specialized diagnostics or maintenance-path tuning when benchmark data justifies it. See [Async I/O Batching Follow-Up](async-io-batching-follow-up.md).

## Phase 4 Status

The phase-3 checkpoint work is no longer the main limiter. The April 11, 2026 phase-4 fan-in work added a focused `commit-fan-in-diagnostics` harness, proved where shared auto-commit had been serializing, and then moved shared non-insert auto-commit SQL onto isolated `WriteTransaction` state.

Current measured status:

1. Shared auto-commit disjoint updates now coalesce.
   - In `commit-fan-in-diagnostics-20260411-141949.csv`, shared auto-commit `W4` reached about `525 commits/sec` with `commitsPerFlush = 1.99`.
   - The same rerun showed shared auto-commit `W8` at about `743 commits/sec` with `commitsPerFlush = 3.37`.
   - That is now effectively in the same commit-fan-in band as the explicit `WriteTransaction` disjoint-update rows on the same runner.

2. Insert fan-in is now opt-in and shape-specific.
   - `ConcurrentWriteTransactions` keeps serialized inserts as the default but lets shared auto-commit insert loops use explicit write-transaction state.
   - The shared row-id reservation path reserves monotonic in-memory ranges, publishes only committed high-water metadata, and tolerates rollback/retry gaps without duplicates.
   - Pending leaf-page rebases let hot right-edge writers merge insert-only deltas against staged WAL images instead of waiting for the earlier commit to publish.
   - A May 5, 2026 spot-check of `AutoCommitConcurrent_AutoId_W8_Batch250us` reached about `1,441 commits/sec` with `commitsPerFlush = 3.32`.
   - The same spot-check put `AutoCommitConcurrent_ExplicitId_W8_Batch250us` at about `1,433 commits/sec` with `commitsPerFlush = 3.29`, and `AutoCommitConcurrent_ExplicitIdDisjoint_W8_Batch250us` at about `1,754 commits/sec` with `commitsPerFlush = 3.99`.

3. Defaults and presets should still stay where they are for now.
   - The engine now has measured wins for shared non-insert contention and opt-in concurrent one-row insert commits.
   - This still does not replace `InsertBatch` or explicit transaction batching for bulk ingest.
   - `UseDurableGroupCommit(...)` and `ConcurrentWriteTransactions` should remain opt-in measured knobs.

Next clean steps:

1. Keep serialized inserts as the default until the broader release guardrails continue to show no single-writer or batch-ingest regression.
2. Keep the compact validation matrix small:
   - single-writer no-regression
   - shared non-insert auto-commit `W4` / `W8`
   - explicit `WriteTransaction` disjoint updates
   - hot insert auto-commit contention
3. Keep measuring disjoint explicit keys, hot explicit right-edge rows, and hot auto-generated IDs as separate shapes.
4. Do not change default batch windows or preset recommendations based on the shared-writer case alone.
