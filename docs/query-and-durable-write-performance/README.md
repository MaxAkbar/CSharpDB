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

## Public Surface

- `sys.table_stats.row_count` keeps its existing numeric meaning.
- `sys.table_stats.row_count_is_exact` is the new explicit exactness bit.
- Histogram and prefix stats remain internal in this round; there are still no public histogram system tables.
- `UseWriteOptimizedPreset()` remains the default recommendation for durable file-backed workloads.
- `UseLowLatencyDurableWritePreset()` and `UseDurableGroupCommit(...)` remain opt-in measure-first knobs.
- Shared-`Database` implicit auto-commit still serializes through the engine write gate, so `UseDurableGroupCommit(...)` is currently more relevant to overlapping explicit `WriteTransaction` commits than to `Database.ExecuteAsync(...)` auto-commit loops.

## Remaining Work

- Adaptive runtime re-optimization is still future work.
- Histogram inspection remains internal; there is no SQL surface for planner histogram dumps yet.
- Durable group-commit guidance should keep following benchmark evidence, especially:
  - single-writer no-regression checks
  - 4-writer and 8-writer shared-`Database` contention runs
  - queue-depth, commits-per-flush, and latency percentile diagnostics
- Async I/O batching still has room for more auditing outside the WAL hot path, but the main write-path batching pieces are already in place.

## Phase 4 Outline

The next performance phase should not keep tuning checkpoint behavior. The phase-3 work already moved checkpoint copy forward and left only a low-single-digit millisecond post-release checkpoint tail in the focused retention benchmark. The remaining fixed cost is still durable flush plus the shared auto-commit path above the WAL pending-commit queue.

Recommended execution order:

1. Measure shared auto-commit commit fan-in directly.
   - Add focused diagnostics that distinguish:
     - concurrent `Database.ExecuteAsync(...)` auto-commit on one shared `Database`
     - overlapping explicit `WriteTransaction` commits on one shared `Database`
   - Record `maxPendingCommits`, `commitsPerFlush`, `flushes/sec`, and tail latency in both modes.
   - Exit criterion: prove exactly where the shared auto-commit path still serializes before the WAL queue.

2. Reduce engine-side serialization above the WAL pending-commit queue.
   - Audit the shared write gate and `PagerCommitResult` completion path for work that can move after logical commit publication.
   - Treat this as a commit-fan-in project, not a checkpoint project.
   - Preserve correctness invariants first:
     - commit order remains stable
     - durable flush acknowledgment still gates commit completion
     - conflict visibility stays monotonic

3. Re-run focused write diagnostics after each fan-in change.
   - Keep the benchmark matrix small:
     - single-writer no-regression
     - shared auto-commit `W4` and `W8`
     - explicit `WriteTransaction` disjoint-update scenario
   - Exit criterion: `commitsPerFlush` rises above `1.00` in the shared auto-commit harness without a single-writer regression that erases the gain.

4. Only then revisit defaults and presets.
   - Do not change the default batch window or preset guidance until the shared auto-commit path actually coalesces commits on the measured runner.
   - If the fan-in work does not materially improve `commitsPerFlush`, keep `UseDurableGroupCommit(...)` as an expert knob and leave the current defaults alone.
