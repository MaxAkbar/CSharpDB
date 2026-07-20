# What's New

## version4.2.0

version4.2.0 advances `CSharpDB.EntityFrameworkCore` from a credible baseline to a proof-backed, production-ready provider for its documented embedded Tier 1 surface. Support remains intentionally bounded: every partial or unsupported area is called out in the generated compatibility matrix rather than implied by the overall designation.

### Migrations and Model Fidelity

- Added repeatable idempotent migration scripts and expanded practical schema evolution, including bounded column, key, foreign-key, check-constraint, index, default, nullability, numeric-type, and collation changes with transactional failure behavior.
- Expanded literal defaults, standalone named checks, and metadata round trips while retaining early validation for unsupported shapes; computed columns and `DefaultValueSql` remain explicitly unsupported.
- Added first-class exact `decimal` mapping for the qualified precision and scale envelope, including round trips, single-facet parameters, comparisons, ordering, ordinary indexes, overflow handling, and durable reopen tests. Computed decimal expressions and precision/scale-changing migrations remain outside the supported contract.

### Concurrency and Queries

- Added database-generated `byte[]` rowversion support with generated-value refresh, stale update/delete detection, raw SQL and trigger advancement, reopen behavior, and clear scope limitations.
- Broadened Tier 1 LINQ translation across string predicates, grouped aggregates, direct inner and left joins, ordinal string operations, bounded `LIKE`, and set operations.
- Added stable provider diagnostics for unsupported LINQ and model shapes so known limitations fail before malformed SQL reaches the engine.

### Application and Package Qualification

- Qualified explicit transaction commit and rollback behavior and documented that savepoints are unsupported.
- Added warm embedded-engine connection pooling for EF-created file connections, including logical-session cleanup, streamed committed-snapshot readers for data-only concurrency, serialized schema changes, poisoned-engine eviction after failed resets, bounded concurrent leases, explicit opt-out, and physical checkpoint/WAL cleanup through the pool-clear APIs. The ADO.NET hot path now also caches prepared absolute-file open plans, checks out existing pools without registry-wide serialization, and skips temporary-state cleanup for sessions proven clean.
- Optimized direct embedded `CSharpDB.Client` transaction sessions to reuse an exclusively owned non-hybrid engine handle after a logical-session reset. In the matched diagnostic, median begin/rollback latency fell from 9.016 ms to 70.429 us (99.22%) and begin/commit fell from 20.871 ms to 3.452 ms (83.46%). Independent transaction completions now perform commit/rollback and disposal outside the client-wide coordination lock. Failed or overlapping transactions, competing handles, and hybrid persistence keep their physical open/close boundaries; raw `Database.OpenAsync(...)` and storage-factory opens remain physical and should be retained by their owners.
- Against the pooling-disabled physical-reopen baseline, the repeat-three EF-managed auto-open/close comparison reduced CSharpDB median single-insert latency from 31.41 ms to 3.58 ms and batch-100 latency from 33.12 ms to 4.07 ms. Final pooled throughput reached 269 single inserts/sec and 23,349 batch rows/sec, placing the tested workloads within about 7% of SQLite.
- Added an eight-case BenchmarkDotNet lifecycle comparison covering CSharpDB and `Microsoft.Data.Sqlite`, explicit pooling on/off, and reused versus short-lived connection objects. Pooled CSharpDB measured 0.228 us and 128 B of managed allocation for a reused connection and 0.508 us and 256 B for construct/open/close/dispose, versus SQLite at 0.045 us/0 B and 0.170 us/168 B. In a cross-run diagnostic comparison, the earlier CSharpDB pooled short-lived result was 2.499 us and 3.08 KB; the new path is about 4.9x faster with about 92% less managed allocation on this runner.
- Added a bounded ASP.NET Core Identity readiness profile for schema version 1 with integer user and role keys, including the seven-table store workflow, relationships, claims, logins, tokens, concurrency stamps, cancellation, rollback, cascade cleanup, and reopen persistence.
- Added a runnable ASP.NET Core minimal API sample and an in-process HTTP compatibility test that proves CRUD and persistence across host restarts.
- Added a package-only consumer gate that restores freshly packed NuGet artifacts and exercises CRUD, reopen, `dotnet ef migrations add`, script generation, and database update without project references.

### Compatibility and Adoption

- Published a generated EF Core compatibility matrix with status, support tier, exact contract, and execution-proof identifiers for every claim.
- Added shared EF Core patch-version qualification across the provider, tests, samples, benchmarks, Identity integration, and package smoke fixture.
- Expanded the provider guide, package README, website, sample catalog, migration deployment guidance, query cookbook, troubleshooting, upgrade notes, and production-readiness checklist from the same bounded support contract.
