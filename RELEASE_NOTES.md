# What's New

## v2.8.1

### Admin Query and View Responsiveness

- Removed the blocking upfront `COUNT(*)` requirement for ad-hoc SQL and view browsing in `CSharpDB.Admin`, so first-page results can render immediately.
- Added unknown-total paging state for query and view grids, including improved row-range status when the full result count is not yet known.
- Added a forward-only direct query cursor for view paging so sequential next-page navigation can continue without re-running the full view query.
- Improved filtered view behavior by pushing simple outer predicates into the underlying view source instead of evaluating them only after the expanded join/view result.

### SQL Planner and Execution Improvements

- Added simple-view outer predicate rewrite and broader inner-join leaf predicate pushdown so selective filters can reach the correct base-table source earlier.
- Extended these pushdown rules across qualifying inner-join chains instead of limiting the optimization to a fixed join count.
- Normalized identity arithmetic join predicates such as `x + 0 = y` so they use normal join planning instead of falling back to slow nested-loop behavior.
- Extended compact scan fast paths to cover simple projected `LIMIT/OFFSET` queries, including `SELECT *` single-table scans.
- Extended fast indexed lookup paths so `LIMIT/OFFSET` is preserved on star, covered-projection, compact-payload, and generic projection indexed plans.
- Tightened SQL parsing so `JOIN` syntax without the required `ON` clause is rejected as invalid instead of reaching execution.

### Benchmark and Validation Updates

- Reduced benchmark baseline storage to a single focused validation snapshot and updated guardrail configuration to use that single baseline consistently.
- Renamed stale benchmark labels and baseline rows so guardrail comparisons match the current benchmark surface after the join and scan fast-path changes.
- Refreshed the benchmark validation pass and confirmed the current release guardrail comparison is clean: `PASS=184, WARN=0, SKIP=0, FAIL=0`.
