# What's New

## v3.3.0

This release starts from `bf954f788cc4a4c5915af2a9a68b66bc7c06f126`
(`Checkpoint hot-right-edge recovery and add plans 4/5`) and is current
through `8d11653c81e690849f462cb376872d901eabfeb6`
(`Refresh benchmark release docs and guardrails`).

v3.3.0 focuses on durable write performance, storage tuning for embedded
ADO.NET and EF Core usage, same-runner SQLite comparisons, and a cleaner
release benchmark process. The main benchmark story is now promoted from the
April 21, 2026 release-core run with guardrails passing at
`PASS=185, WARN=0, SKIP=0, FAIL=0`.

### Durable Write and Indexing Performance

- Added append-optimized index storage paths for row-id chains and hashed
  payloads, including appendable payload codecs, overflow-store improvements,
  and focused tests for the new insert-maintenance paths.
- Optimized indexed insert maintenance for monotonic and append-heavy
  workloads, including hot right-edge recovery, insert sequence context, and
  expanded B-tree/index diagnostics for commit-path investigation.
- Added trailing-integer support for composite grouped aggregate planning while
  tightening SQL index metadata defaults so multi-column indexes no longer
  receive trailing-integer hash options unless explicitly requested.
- Expanded record encoding and serialization support used by the optimized
  storage paths and added compatibility coverage for hashed index payloads,
  append-only row-id chains, record encoding, collation metadata, and SQL index
  behavior.

### Embedded ADO.NET and EF Core Storage Tuning

- Added storage tuning presets for embedded ADO.NET and EF Core users,
  including `CSharpDbStoragePreset`, embedded open modes, connection-string
  builder support, and configuration resolution for shared file-backed usage.
- Updated the EF Core provider option validation and relational connection
  setup so storage mode and connection behavior are explicit for embedded
  workloads.
- Added comparative ADO.NET and EF Core smoke coverage plus embedded storage
  tuning tests for the new configuration surface.
- Updated package references across the CLI, EF Core sample/provider, tests,
  and benchmark projects.

### Benchmarks and SQLite Comparisons

- Added durable SQLite comparison coverage, including SQLite C API helpers,
  concurrent SQLite C API benchmarks, concurrent ADO.NET comparison
  benchmarks, strict insert comparison rows, and EF Core comparison benchmarks.
- Added CSharpDB-versus-SQLite performance guidance and blog content under
  `docs/query-and-durable-write-performance`, with website and sitemap updates
  for the new comparison material.
- Replaced the older programmatic insert planning docs with the current
  ADO.NET/EF storage tuning guide and release-core benchmark story.

### Release Benchmark Process

- Redesigned `tests/CSharpDB.Benchmarks/README.md` into a compact,
  user-facing benchmark contract with a generated core scorecard, current
  results, benchmark map, and run/promote instructions.
- Added `BENCHMARK_CATALOG.md`, `HISTORY.md`, `SQLITE_COMPARISON.md`,
  `release-core-manifest.json`, and
  `scripts/Update-BenchmarkReadme.ps1` so published numbers come from an
  explicit manifest and only the generated region is rewritten.
- Added the `--release-core` benchmark command to run the balanced core suite:
  master-table, durable SQL batching, concurrent durable writes, hybrid storage
  mode, resident hot-set reads, cold open, and SQLite comparison.
- Updated release guardrail comparison logic to support structural `ExtraInfo`
  checks and row-specific tolerances for known volatile microbenchmark rows.

### Docs, Package READMEs, and Website

- Refreshed the root README with the current promoted performance numbers:
  `1.67M` collection gets/sec, `10.77M` concurrent reader-burst reads/sec,
  `798.25K` durable InsertBatch B10000 rows/sec, and `1.04K` concurrent
  durable commits/sec.
- Added or refreshed package-local READMEs for Admin, Forms, Reports, CLI,
  MCP, Native, API, Data, Engine, EF Core, and the aggregate package surface.
- Updated documentation routes, titles, sitemap entries, and website pages,
  including moving public docs under the current `www/docs` structure and
  removing unused legacy JavaScript files.

### Validation

- `dotnet build .\CSharpDB.slnx -c Release --no-restore` completed
  successfully.
- `dotnet build .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -c Release --no-restore`
  completed successfully during release prep.
- `dotnet run -c Release --project .\tests\CSharpDB.Benchmarks\CSharpDB.Benchmarks.csproj -- --release-core --repeat 3 --repro`
  completed and produced the promoted April 21, 2026 release-core artifacts.
- Final release guardrail comparison passed with
  `PASS=185, WARN=0, SKIP=0, FAIL=0`.
- The benchmark README generator was run twice and verified stable on the
  second dry run.
