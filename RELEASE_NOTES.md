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
- Added a bounded ASP.NET Core Identity readiness profile for schema version 1 with integer user and role keys, including the seven-table store workflow, relationships, claims, logins, tokens, concurrency stamps, cancellation, rollback, cascade cleanup, and reopen persistence.
- Added a runnable ASP.NET Core minimal API sample and an in-process HTTP compatibility test that proves CRUD and persistence across host restarts.
- Added a package-only consumer gate that restores freshly packed NuGet artifacts and exercises CRUD, reopen, `dotnet ef migrations add`, script generation, and database update without project references.

### Compatibility and Adoption

- Published a generated EF Core compatibility matrix with status, support tier, exact contract, and execution-proof identifiers for every claim.
- Added shared EF Core patch-version qualification across the provider, tests, samples, benchmarks, Identity integration, and package smoke fixture.
- Expanded the provider guide, package README, website, sample catalog, migration deployment guidance, query cookbook, troubleshooting, upgrade notes, and production-readiness checklist from the same bounded support contract.

## version4.1.0

version4.1.0 establishes CSharpDB's proof-backed SQL compatibility contract and expands the dialect across constraints, composite relationships, transactional schema changes, compound queries, windows, metadata, and generated SQL. It also hardens ALTER and window execution, pager shutdown, and immutable compatibility release gates.

### SQL and Schema Compatibility

- Added quoted identifiers, SQL three-valued logic, literal and `NULL` defaults, explicit `DEFAULT` insert forms, and deterministic row-local column/table `CHECK` constraints.
- Added persisted logical primary and unique keys, including composite tuples, plus table-level and composite foreign keys with immediate `RESTRICT` and `CASCADE` behavior for deletes and key updates.
- Added transactional shadow-root rewrites for unconstrained `DROP COLUMN` and populated literal-default `ADD COLUMN`, together with default, nullability, named-check, table/column rename, and foreign-key constraint alterations.
- ALTER now rejects stored-view, trigger-body, and validation-rule dependencies before mutation, and failed writes make explicit transactions rollback-only.

### Queries and Diagnostics

- Added lazy duplicate-preserving `UNION ALL` execution and an in-memory first-tier window-function slice for ranking and common aggregates.
- Star projections can be mixed with expressions and windows without leaking hidden window columns; cancellation and cleanup are enforced throughout window execution.
- `EXPLAIN ESTIMATE FOR` now uses the shared read-only routing rules, including reader sessions, and prepared statements discover and rebind parameters in the explained target.
- Built-in function metadata is centralized and exposed through `sys.functions`.

### Providers, Metadata, and Documentation

- Expanded schema metadata across SQL catalogs, ADO.NET, REST, gRPC, archives, Admin, DevOps, and the EF Core provider for defaults, checks, ordered keys, and ordered foreign-key pairs.
- Expanded EF Core SQL and migration generation for literal defaults, checks, `DEFAULT VALUES`, composite primary keys, indexes, and related supported schema operations.
- Added a machine-readable compatibility manifest, JSON Schema, deterministic HTML matrix, public roadmap, proof-backed tests, and CI/Pages documentation checks.

### Compatibility and Release Qualification

- Published the immutable 4.1.0 SQL compatibility snapshot required by tagged-release qualification, and aligned package, manifest, roadmap, and generated-matrix metadata with the release.
- Added tooling and release CI gates for immutable per-version compatibility snapshots, strict semantic-version progression, verified commit ancestry, manifest/schema parity, tracked artifacts, and prior-snapshot immutability.
- Legacy schema fixtures remain readable, and newly added schema metadata is serialized additively and covered by reopen, archive, transport, and tooling tests.
- Pager shutdown now atomically stops and drains background checkpoints before final WAL cleanup, preventing a late checkpoint from truncating a WAL while the next daemon host recovers it.
- Broader ALTER type/collation/key rewrites, spill-backed windows, physical `EXPLAIN ANALYZE`, and the remaining performance, replay, upgrade, and crash-qualification lanes remain planned rather than advertised as supported.
- Release validation completed with a zero-warning solution build, 2,306 passing tests across the solution, 43 documented compatibility features backed by 141 proof IDs, positive plus tamper-rejection release-gate fixtures, and 10 fresh-process passes of the complete daemon suite.
