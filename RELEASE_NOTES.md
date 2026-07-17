# What's New

## version4.1.0

version4.1.0 establishes CSharpDB's proof-backed SQL compatibility contract and expands the dialect across constraints, composite relationships, transactional schema changes, compound queries, windows, metadata, and generated SQL. It also hardens ALTER and window execution and adds immutable compatibility release gates.

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

- Added tooling and release CI gates for immutable per-version compatibility snapshots, strict semantic-version progression, verified commit ancestry, manifest/schema parity, tracked artifacts, and prior-snapshot immutability.
- Legacy schema fixtures remain readable, and newly added schema metadata is serialized additively and covered by reopen, archive, transport, and tooling tests.
- Broader ALTER type/collation/key rewrites, spill-backed windows, physical `EXPLAIN ANALYZE`, and the remaining performance, replay, upgrade, and crash-qualification lanes remain planned rather than advertised as supported.
- Release validation completed with a zero-warning solution build, 2,302 passing tests across the solution, 43 documented compatibility features backed by 141 proof IDs, and positive plus tamper-rejection release-gate fixtures.
