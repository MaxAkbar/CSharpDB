# What's New

## v2.5.0

### Durable WAL Recovery, Concurrency, and Tuning

- Refactored the file-backed WAL commit path to batch staged frame appends at commit time, reduce unnecessary writer-lock hold time, and harden recovery and repair behavior around checkpoint and WAL edge cases.
- Exposed durable-write tuning through `DatabaseOptions.ConfigureStorageEngine(...)`, including `UseWriteOptimizedPreset()`, `UseDurableCommitBatchWindow(...)`, and `UseWalPreallocationChunkBytes(...)` for advanced file-backed write workloads.
- Added process-crash durability coverage, concurrent durable-write diagnostics and benchmarks, and machine-aware performance guardrails with checked-in focused baselines to make release validation more reproducible.
- Fixed a collection write-gate leak on cached collection opens that could stall later collection count and browse requests through the HTTP API and gRPC daemon after earlier collection writes.

### Phase-1 Cost-Based Join Planning

- Added the first stats-driven cardinality estimation phase for the SQL planner, using `ANALYZE` data to improve non-unique lookup selection, join method choice, and hash build-side choice.
- Added limited greedy inner-join reordering so selective predicates can move earlier in supported inner-join chains when statistics are available.
- Expanded selectivity heuristics and planner coverage for range predicates, `IN` lists, nullable disjunctions, and mixed `UNION` join cases, plus new join microbenchmarks for those shapes.

### Documentation and Release Guidance

- Refreshed the root, engine, execution, and storage READMEs with updated performance snapshots, thread-safety guidance, current write-tuning recommendations, and documentation of the current cost-based optimizer phase.
- Updated benchmark docs and scripts so durability diagnostics, guardrail capture, and machine compatibility handling are documented and easier to run on a fresh clone.