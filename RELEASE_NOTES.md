# What's New

## v3.2.0

This release carries forward the `version3.2.0` branch from the April 15, 2026
`v3.1.2` base through the current head, centering on the first embedded EF Core
provider foundation, scalar aggregate lookup planning improvements, refreshed
benchmark guardrails, and new guidance for programmatic insert performance.

### Entity Framework Core and ADO.NET Foundation

- Added `CSharpDB.EntityFrameworkCore`, an embedded-only EF Core 10 provider
  built on top of `CSharpDB.Data`, including `UseCSharpDb(...)`, provider
  options, design-time services, migrations/history/locking infrastructure,
  relational connection services, query SQL generation, type mappings, and
  update batching.
- Added provider docs and a runnable `samples/efcore-provider` sample covering
  `EnsureCreatedAsync()`, design-time context creation, migrations, navigation
  loading, and the supported embedded runtime shapes.
- Added package-local NuGet README and release/CI pack wiring so
  `CSharpDB.EntityFrameworkCore` ships through the same package flow as the
  existing provider/runtime packages.
- Expanded the underlying ADO.NET command, parameter binding, and
  prepared-statement plumbing needed by the provider foundation and added
  focused data/provider tests.

### Query Planning and Runtime Work

- Optimized scalar aggregate lookup planning so simple indexed and
  primary-key-backed `COUNT(...)` / `SUM(...)` lookup shapes can reuse
  lightweight lookup plans instead of first materializing fuller operator
  pipelines, reducing allocations and improving targeted microbenchmark rows.
- Extended parser, tokenizer, query-result, and execution-planning support so
  the embedded engine can handle the SQL and query shapes emitted by the new EF
  Core surface.
- Simplified WAL flush policy abstractions by removing
  `FlushBufferedWritesAsync` from `IWalFlushPolicy` and aligning WAL test
  assertions around the current commit/flush contract.

### Benchmarks, Guardrails, and Guidance

- Refreshed the focused `ScalarAggregateLookupBenchmarks` baseline and perf
  thresholds using the April 18, 2026 validation capture checked into
  `tests/CSharpDB.Benchmarks/baselines/focused-validation/20260418-185724`.
- Stabilized the benchmark guardrail workflow and scenario entry points,
  including durable SQL batching coverage and the release compare script used
  for targeted performance verification.
- Added a benchmark-driven CSharpDB-versus-SQLite comparison guide to the
  website and updated the sitemap to publish it.

### Docs, Samples, and Planning

- Added the new `docs/programmatic-insert-performance` plan set, splitting the
  earlier insert investigation into separate single-writer,
  durability/residency, and concurrent disjoint-key writer plans aligned to the
  current public API and benchmark harnesses.
- Refreshed benchmark documentation, sample index content, and README badges to
  reflect the current provider, benchmarking, and documentation surface.

### Validation

- Added EF Core foundation coverage across `tests/CSharpDB.Data.Tests`,
  `tests/CSharpDB.EntityFrameworkCore.Tests`, `tests/CSharpDB.Tests`, and the
  focused benchmark baseline/guardrail docs used by this branch.
- Checked in the focused scalar aggregate benchmark baseline capture used by the
  current perf threshold update.
