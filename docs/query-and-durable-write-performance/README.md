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

## Public Surface

- `sys.table_stats.row_count` keeps its existing numeric meaning.
- `sys.table_stats.row_count_is_exact` is the new explicit exactness bit.
- Histogram and prefix stats remain internal in this round; there are still no public histogram system tables.
- `UseWriteOptimizedPreset()` remains the default recommendation for durable file-backed workloads.
- `UseLowLatencyDurableWritePreset()` and `UseDurableGroupCommit(...)` remain opt-in measure-first knobs.
- Shared-`Database` implicit auto-commit is now split by workload shape:
  - non-insert SQL writes can queue behind the WAL pending-commit path and benefit from `UseDurableGroupCommit(...)`
  - hot insert loops still use the legacy serialized path today, so they should still be benchmarked as insert-specific workloads rather than assumed to coalesce

## Remaining Work

- Adaptive runtime re-optimization is still future work.
- Histogram inspection remains internal; there is no SQL surface for planner histogram dumps yet.
- Durable group-commit guidance should keep following benchmark evidence, especially:
  - single-writer no-regression checks
  - 4-writer and 8-writer shared-`Database` contention runs
  - queue-depth, commits-per-flush, and latency percentile diagnostics
- The remaining phase-4 write-path question is now narrower than "shared auto-commit in general":
  - non-insert shared auto-commit fan-in is working
  - hot insert auto-commit still needs a dedicated design decision if we want it to coalesce without reopening structural conflict costs
- Async I/O batching still has room for more auditing outside the WAL hot path, but the main write-path batching pieces are already in place.

## Phase 4 Status

The phase-3 checkpoint work is no longer the main limiter. The April 11, 2026 phase-4 fan-in work added a focused `commit-fan-in-diagnostics` harness, proved where shared auto-commit had been serializing, and then moved shared non-insert auto-commit SQL onto isolated `WriteTransaction` state.

Current measured status:

1. Shared auto-commit disjoint updates now coalesce.
   - In `commit-fan-in-diagnostics-20260411-141949.csv`, shared auto-commit `W4` reached about `525 commits/sec` with `commitsPerFlush = 1.99`.
   - The same rerun showed shared auto-commit `W8` at about `743 commits/sec` with `commitsPerFlush = 3.37`.
   - That is now effectively in the same commit-fan-in band as the explicit `WriteTransaction` disjoint-update rows on the same runner.

2. The gain is intentionally scoped.
   - Hot auto-commit inserts still use the legacy serialized path.
   - The targeted `ConcurrentDurableWrite_W8_Batch250us` insert rerun in `concurrent-write-scenario-W8_Batch250us-20260411-142023.csv` still measured about `428 commits/sec` with `commitsPerFlush = 1.00`.
   - The current phase-4 result should therefore be read as "shared non-insert auto-commit fan-in works" rather than "every auto-commit workload now coalesces."

3. Defaults and presets should still stay where they are for now.
   - The engine now has a real measured win for shared non-insert contention.
   - It does not yet have a blanket insert-path win that would justify changing default batch-window guidance.
   - `UseDurableGroupCommit(...)` should remain an opt-in measured knob until the insert-side question is settled.

Next clean steps:

1. Decide whether hot auto-commit inserts should stay explicitly transaction-first or gain their own isolated implicit path.
2. Keep the compact validation matrix small:
   - single-writer no-regression
   - shared non-insert auto-commit `W4` / `W8`
   - explicit `WriteTransaction` disjoint updates
   - hot insert auto-commit contention
3. Do not change default batch windows or preset recommendations until the insert-side behavior is intentionally resolved.
