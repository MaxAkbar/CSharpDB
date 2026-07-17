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

## version4.0.4

version4.0.4 adds automatic numeric relationship join acceleration for eligible `INNER JOIN` queries, restores inline B-tree read performance after the 4.0.3 overflow-page work, reduces reader-session overhead, and resolves the outstanding NU1903 dependency advisories.

### Performance

- Broad key-only `INNER JOIN` queries over a declared `INTEGER` primary-key-to-foreign-key relationship can now automatically merge-scan the existing foreign-key support index with the parent primary-key tree.
- The supported path is selected conservatively for sufficiently large, unfiltered base-table joins. Text keys, payload projections, predicates or residual conditions, outer and reversed joins, point shapes, and `LIMIT`/`OFFSET` retain the existing plan.
- No separate join index or additional payload columns are stored. Inserts, updates, and deletes continue to maintain the existing primary-key and foreign-key indexes.
- Focused SQL comparisons measured `1.84x`, `6.77x`, and `11.11x` faster reads at child fanouts of 1, 10, and 100, with allocations reduced by `53.2%` to `94.9%`.
- Checkpoint reader tracking now maintains the minimum retained WAL offset incrementally, while no-WAL snapshots reuse an immutable empty page map. Per-query reader-session allocation dropped by 80 bytes in focused testing.

### Fixed

- Inline B-tree values now obtain their payload and overflow flag in one leaf-cell parse.
- Point and cursor reads return inline values directly and invoke overflow resolution only for actual overflow references.
- This removes the scan and point-read regression introduced with large-value overflow support while preserving overflow-backed value behavior.

### Security and Dependencies

- Pinned `Microsoft.OpenApi` to `2.7.5`.
- Pinned the benchmark and comparative-test SQLite bundle to `SQLitePCLRaw.bundle_e_sqlite3` `2.1.12`.
- The solution vulnerability audit now reports no vulnerable packages and Release builds complete without NU1903 warnings.

### Compatibility and Validation

- This release does not introduce a new database format. Existing format-v1 and format-v2 databases retain the compatibility behavior documented for 4.0.3.
- Queries outside the numeric relationship eligibility gates continue through the existing join operators with unchanged SQL semantics.
- Full solution validation passed 2,152 tests with zero failures.
- The complete post-fix release-core run produced all 125 expected rows. Against the published May artifacts, 71 improved by at least 8%, two noisy in-memory hot-set rows regressed by at least 8%, and 52 stayed within the band.
- The full compatible-runner guardrail reported `PASS=186, WARN=1, SKIP=0, FAIL=0`. A same-session v4.0.3 control reproduced the warned durable-flush slowdown and was 3.2% to 6.4% slower than 4.0.4, isolating the warning to runner/storage drift rather than this release's code.
