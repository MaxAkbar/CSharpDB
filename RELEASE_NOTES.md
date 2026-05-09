# What's New

## version3.7.0

version3.7.0 focuses on query-planner observability, opt-in adaptive join
reoptimization, faster paged view browsing, and the benchmark/documentation
close-out work around the current optimizer and async I/O roadmap phases. It
also carries smaller but important polish for SQL result metadata, fulfillment
sample lookup indexes, DataGen direct-load throughput, and benchmark regression
analysis.

### Planner Observability

- Added SQL-first planner diagnostics through `EXPLAIN ESTIMATE FOR SELECT`,
  `EXPLAIN ESTIMATE FOR WITH`, and compound query estimate support.
- Added public `sys.planner_*` virtual catalogs for planner histograms, heavy
  hitters, and composite index prefix statistics.
- Added bounded estimate diagnostics for stats freshness, lookup and filter
  estimates, index choices, hash build-side selection, and join reordering.
- Added an Admin Query tab Estimate action and Plan tab rendering for planner
  diagnostic rowsets.
- Documented how to read plan output, debug missing or stale stats, and spot
  common query-planning red flags.

### Adaptive Join Reoptimization

- Added opt-in phase-one adaptive query reoptimization through
  `DatabaseOptions.AdaptiveQueryReoptimization` and
  `EnableAdaptiveQueryReoptimization(...)`.
- Added ADO.NET direct embedded connection-string support for
  `Adaptive Query Reoptimization=true`; remote endpoint connections reject the
  key so hosts enable the feature server-side.
- Added adaptive join wrappers that can switch eligible index nested-loop joins
  to hash joins before rows are emitted when observed outer cardinality
  diverges.
- Added adaptive hash join build-side flipping for eligible inner joins when
  the planned build side is materially larger than estimated.
- Added internal diagnostics for eligible queries, attempts, successful
  switches, rejected switches, divergence events, buffered rows, and fail-closed
  fallback reasons.
- Kept default query behavior unchanged and suppressed adaptation for risky
  shapes such as compound query children, correlated subqueries, cross/right
  joins, and `SELECT *` cases where visible column order could change.

### View And Lookup Planning

- Taught row-goal planning to reorder eligible simple view join chains before
  building the view operator tree, so bounded `LIMIT`/`OFFSET` view queries can
  use the same streaming lookup plans as equivalent inline SQL.
- Updated the Admin DataGrid view path to page views with bounded
  `LIMIT`/`OFFSET` instead of opening unbounded forward-only view cursors.
- Fixed the Query tab grid layout so the row grid is the only scroll container
  and the pagination bar stays fixed below the rows.
- Improved lookup-join planning by using indexed local predicate estimates when
  cardinality stats are unavailable or weaker.
- Preserved right-side local predicates as residual join filters for lookup
  joins and passed estimated row counts into index scans as capacity hints.
- Added an `orders(customer_id)` lookup index to the fulfillment sample schema
  for customer-filtered order views.

### SQL Result Metadata

- Propagated query column types through engine transport, HTTP API/client DTOs,
  and the Admin DataGrid.
- View/query result metadata now preserves `ColumnTypes` across local and
  remote SQL execution paths.
- Updated client SQL execution coverage so column names and column types are
  both asserted for query results.
- Updated the API package reference for `Scalar.AspNetCore` from `2.14.10` to
  `2.14.11`.

### DataGen And Collection Fast Path Close-Out

- Closed the current generated collection fast-path roadmap phase and split
  package ergonomics and broader generator coverage into future roadmap items.
- Refreshed benchmark-facing docs with the current release-core snapshot and
  generated collection codec diagnostics.
- Moved CSharpDB.DataGen direct loads onto the write-optimized storage preset.
- Reused SQL row buffers before `InsertBatch` copies.
- Documented the DataGen fast-path choices and capped direct collection
  document target sizes to stay within the current inline collection payload
  envelope.

### Benchmarks And Roadmap Close-Out

- Marked the current advanced cost-based query optimizer and async I/O batching
  roadmap phases as completed current-phase work.
- Kept adaptive reoptimization and public histogram inspection documented as
  separate planned/follow-up items where appropriate.
- Added optimizer close-out diagnostics for heavy-hitter equality, histogram
  range estimates, composite-prefix correlation, and bounded join reordering.
- Added async I/O close-out diagnostics for save/backup/restore, vacuum and FK
  logical rewrites, database inspector scans, and live WAL inspector scans.
- Refreshed roadmap, query/durable-write docs, async I/O audit notes, benchmark
  catalog, README performance tables, and release-core manifest metadata with
  the May 6 benchmark baseline.
- Added a BenchmarkDotNet WAL point-read benchmark for primary-key reads across
  WAL-backed and checkpointed states at 100, 1k, 5k, and 10k target frames.
- Updated `Compare-Baseline.ps1` so performance checks can compare a
  configurable time metric such as `P95` through `metricColumn` while keeping
  `Mean` as the default.

### Tests And Benchmarks

- Added parser, catalog, planner, client, HTTP, and benchmark coverage for
  planner diagnostics and `EXPLAIN ESTIMATE`.
- Added adaptive reoptimization engine/operator tests, ADO.NET option tests,
  and the `AdaptiveReoptimizationBenchmark` diagnostic suite.
- Added regression coverage for bounded simple-view row-goal planning, late
  unindexed detail joins, purchase-order style join chains, and Admin DataGrid
  view paging SQL generation.
- Added tests for right-side join predicates, unique text-filter lookup joins,
  and SQL result column type metadata.
- Added benchmark suites for optimizer close-out, async I/O close-out, planner
  catalog diagnostics, and WAL point-read regression analysis.

### Validation

- `dotnet test .\CSharpDB.slnx -c Release`
- `dotnet build .\CSharpDB.slnx -c Release --no-restore`
- `dotnet test .\CSharpDB.slnx -c Release --no-build`
- `dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --optimizer-closeout --repro`
- `dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --async-io-closeout --repro`
- `pwsh -NoProfile .\tests\CSharpDB.Benchmarks\scripts\Run-Perf-Guardrails.ps1 -Mode release`
  - Reported `PASS=187, WARN=0, SKIP=0, FAIL=0`.
- `dotnet build .\src\CSharpDB.Admin\CSharpDB.Admin.csproj -c Release`
- `dotnet test .\tests\CSharpDB.Admin.Forms.Tests\CSharpDB.Admin.Forms.Tests.csproj -c Release --filter FullyQualifiedName~DataGridTests`
- `dotnet test .\tests\CSharpDB.Tests\CSharpDB.Tests.csproj -c Release --filter FullyQualifiedName~SimpleViewLateUnindexedDetailJoinWithLimit|FullyQualifiedName~JoinChainWithLimit|FullyQualifiedName~PurchaseOrderLineJoinChainWithLimit`
- Targeted generated collection tests, trim smoke publish, DataGen Release
  build, relational direct-load smoke, and document direct-load smoke passed.
- `dotnet build tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -c Release --no-restore`
- `dotnet run -c Release --project tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --micro --filter *WalPointReadBenchmarks* --job Dry`
- Focused lookup-join and SQL column type tests passed for:
  `Join_WithWhereOnRightPrimaryKeyLookupSide_AppliesPredicate`,
  `Join_WithUniqueTextFilterAndIndexedDependentSide_UsesLookupJoins`, and
  `ExecuteSqlAsync_ReturnsQueryColumnTypes`.

### Review Notes

- Adaptive query reoptimization is opt-in and intentionally leaves default
  planning behavior unchanged.
- `EXPLAIN ESTIMATE` and `sys.planner_*` are diagnostic surfaces; normal select
  planning should not depend on the diagnostic rowset materialization path.
- Simple view row-goal reordering is scoped to eligible join chains and bounded
  paging shapes.
- The SQL result DTO column type addition is additive for API/client callers.
- The new WAL point-read benchmark provides a stable current signal, but it
  needs a captured historical baseline before it can be used as a release
  regression gate.
