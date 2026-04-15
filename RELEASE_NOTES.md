# What's New

## v3.1.0

This release carries forward the full `version3.1.0` branch from the April 8, 2026
phase-1 multi-writer base through the current head, combining substantial
durable-write/runtime work with new samples, tutorials, and refreshed docs.

### Multi-Writer and Durable Write Progress

- Built out the post-phase-1 multi-writer engine work from the initial `WriteTransaction` base, including stronger explicit transaction handling, richer logical conflict tracking, range and disjunctive predicate conflict validation, and broader concurrency coverage.
- Added phase-3 snapshot checkpoint retention tuning and phase-4 shared auto-commit fan-in work so overlapping durable writes can build a real pending commit queue in more shapes than the original single-writer path allowed.
- Added the opt-in `ImplicitInsertExecutionMode = ConcurrentWriteTransactions` path for shared auto-commit inserts, plus focused insert fan-in diagnostics, split-aware conflict recovery, and right-edge/leaf rebase hardening around hot insert workloads.
- Added commit-path and insert-fan-in diagnostics so durable-write behavior is easier to measure, explain, and regress-test.

### Insert, Collection, and Query Performance

- Optimized insert commit allocation paths, single-insert hot-path work, and related WAL/commit plumbing to reduce fixed per-commit cost on hot ingest paths.
- Improved collection secondary-index maintenance and related collection/runtime bookkeeping.
- Added a fast path for reader-session `COUNT(*)` behavior.
- Refreshed focused benchmarks, baselines, and benchmark documentation around concurrent durable writes, insert fan-in, checkpoint retention, and storage-mode behavior.

### Transport and Hosting

- Routed the ADO.NET provider through `CSharpDB.Client`, adding the supporting remote session and connection plumbing needed for the newer transport model.
- Defaulted the daemon host to hybrid multi-writer mode and updated daemon configuration/docs to match the current runtime recommendations.

### Samples, Tutorials, and Docs

- Added the Atlas Platform Showcase sample with schema, workbook queries, procedures, and a runnable sample app.
- Added the CSV bulk import sample and tutorial showing the current best-practice relational bulk-ingest path on the public API: `UseWriteOptimizedPreset()`, `PrepareInsertBatch(...)`, explicit transaction batching, and post-load index creation.
- Added the multi-writer follow-up plan document and linked it from the roadmap.
- Refreshed the README, architecture/configuration/FAQ/internals docs, ecosystem docs, benchmark docs, and website pages to reflect the current multi-writer, transport, and tooling story.

### Validation

- Validated the CSV bulk import sample with `dotnet run --project samples/csv-bulk-import/CsvBulkImportSample.csproj -- --database-path artifacts/sample-verification/csv-import-demo.db`.
- Validated sample smoke coverage with `dotnet test tests/CSharpDB.Tests/CSharpDB.Tests.csproj --filter "FullyQualifiedName~SampleSmokeTests" -nologo`.
