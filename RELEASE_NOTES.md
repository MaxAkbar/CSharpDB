# What's New

## version4.4.0

version4.4.0 completes the bounded SQL implementation and makes its release claims reproducible. The public reference, provider behavior, migration capability contracts, recovery tests, and release workflow now describe the same supported surface; publishing remains gated on the recorded qualification passes described below.

### SQL and Schema Coverage

- Completed the bounded window-function slice with explicit `ROWS` frames, named windows, compatible shared ordering, navigation/value functions, cancellation, prepared execution, and explicit in-memory resource limits.
- Added the full immediate foreign-key action matrix for deletes and referenced-key updates, including composite relationships and transactional nested cascades.
- Added transactional shadow-root `ALTER COLUMN` rewrites for the supported numeric, UTF-8 text/BLOB, and collation shapes, including atomic rebuilding of eligible indexes, plus validated transactional catalog updates for literal-default and nullability changes.
- Added logical composite `INTEGER`/`TEXT` keys and bounded populated single-`INTEGER` primary-key rekeying with relational and complete ready full-text-owned storage rebuilding.
- Added stable physical `EXPLAIN` and `EXPLAIN ANALYZE` rowsets with estimated and actual values, operator/access-path metadata, redacted predicates, partial safe diagnostics, and direct/ADO.NET/HTTP/gRPC access.
- Unsupported SQL forms now fail with stable diagnostics instead of being silently accepted; documentation tests execute every concrete public SQL example from its published source, parser-check the parameterized example, and classify schematic grammar templates separately.

### Provider and Compatibility Qualification

- Added a replayable EF Core generated-SQL corpus covering defaults, checks, composite and named relationships, referential actions, key changes, column rewrites, table/column/index rename chains, stable diagnostics for unsupported database sequence operations, rollback, reopen, upgrade/downgrade, runtime CRUD, and ADO.NET schema inspection.
- Added immutable database fixtures produced by supported historical releases, with recorded commits and checksums, then qualified current write, checkpoint, recovery, and reopen behavior against each fixture.
- Added one canonical typed workload across the direct engine, embedded ADO.NET, HTTP, and gRPC paths. This qualification found and fixed HTTP BLOB results being returned as base64 text rather than bytes.
- Added deterministic bounded property coverage for parser expressions, constraint graphs, and type conversions, plus fault-injected WAL commit recovery for schema rewrites and multi-level cascades.

### Release Gate

- Added a reusable release qualification workflow that runs the full solution and provider/tooling checks twice from clean environments on Windows, Linux, and macOS before publishing can begin. It supports manual dispatch after registration on the default branch and non-release `qualification-*` tags for qualifying an exact pre-merge candidate commit.
- Added two clean previous-release master-table qualifications that run a hash-recorded candidate harness against both engines on the same runner. Each pass receives one unrecorded warmup and three recorded runs per revision; the gate recomputes the median from retained raw evidence, requires a strict stable majority within both revisions, and reverses revision order in pass two. A stable candidate fails above the 15% throughput limit or when P99 exceeds both 25% and 0.05 ms; malformed or unstable evidence fails closed. Broader suites remain available through the manual comparison runner, while the existing scheduled guardrail workflow provides report-only diagnostics without multiplying the release gate.
- Added diagnostic-only exact hybrid-row controls with five pairs per order, an optional directly executed same-revision DLL whose hash is verified around every sample, separate revision build-input identities, build-server shutdown plus a fixed wait, and a fail-closed 30-second plus 10,000-retained-sample floor with a 120-second measurement cap. These controls diagnose benchmark stability and do not satisfy the two-revision release gate.
- Preserved the immutable 4.3.0 migration capability catalog and added a separately digested 4.4.0 catalog so older migration plans remain independently replayable.

## version4.3.0

version4.3.0 adds a first-party, review-first workflow for moving schemas and rows from other data sources into a new CSharpDB database. Migration is deliberately staged: capture a source, review the conversion plan and target DDL, apply to a new target, resume from committed target receipts if interrupted, and activate only after validation passes.

### Migration Source Lanes

- Added retained, digest-pinned migration packages for strict CSV, JSON/NDJSON, SQLite, LiteDB 5, MySQL 8.0/8.4, and the bounded Microsoft Access and on-premises SQL Server 2019/2022/2025 candidate lanes.
- Added coherent SQLite online backup and offline/quiesced LiteDB capture, with bounded source profiles and explicit diagnostics for unsupported source objects.
- Added a Windows-only ACE worker for local unencrypted `.mdb`/`.accdb` tables. Access capture is evaluation-only until the declared Windows/ACE/file-format/bitness VM matrix passes.
- Added isolated SQL Server and MySQL workers for bounded schema and row capture. Connection strings are accepted only through named environment variables and are needed only during capture.
- Added deterministic scalar encodings, row ordering, catalog/package bindings, package size ceilings, and fail-closed diagnostics for the implemented SQL Server and MySQL relational subsets.
- SQL Server now has a strict positive non-`sysadmin` metadata-visibility proof that passes a SQL Server 2019 Express LocalDB fixture and fails closed on object-level `DENY`. Retained capture remains evaluation-only until the supported-edition, published-runtime, platform, authentication, and differential matrix passes. Do not elevate a migration account to bypass a failed proof.

### Review, Recovery, and Validation

- Added explicit `inspect`, `plan`, `preview`, `apply`, and `validate` commands plus an `apply --resume` recovery mode. Exact target DDL and isolated scratch-schema evidence are available before target creation.
- Added `type-map` reports using the planner's exact conversion policy and bounded `query-check` reports for CSharpDB and isolated T-SQL parse-level evidence. MySQL, SQLite, and Access query dialects fail closed as `Unknown`.
- Apply writes a new staged target and commits rows with target receipts. Resume reopens and requalifies the exact package binding and skips only batches with matching target receipts.
- Checksum validation compares normalized schema, 64-bit row counts, and partitioned canonical SHA-256 evidence. Failed or inconclusive validation withholds activation.
- Added `CSharpDB.Migration.DualRun`, a packaged read-only SDK for typed query-pack comparison between explicitly identified, coherent source and target snapshots. Writable generic source connections, structurally inconsistent or tampered pass reports, endpoint errors, and exhausted limits fail closed.
- Retained packages are treated as plaintext-sensitive source data. Expected package digests must be stored in an independently trusted record.

### CSV and JSON Export

- Added explicit offline retained CSharpDB snapshot capture with a canonical snapshot identity.
- Added resumable, lossless CSV, JSON, and NDJSON publication with source-bound manifests and create-new/no-overwrite behavior.
- The hardened resumable publisher is currently limited to trusted local Windows directories; other platforms return `MIG-EXPORT-PLATFORM-001` instead of weakening the publication contract.

### Packaging and Documentation

- Added combined, framework-dependent migration CLI archives for Windows x64, Linux x64, and macOS Apple silicon. Archives require the .NET 10 runtime and retain the applicable fixed optional adapters, notices, licenses, and installers; the GitHub release includes `MIGRATION-SHA256SUMS.txt` for all three archives.
- Published the provider-neutral `CSharpDB.Migration` and `CSharpDB.Migration.DualRun` packages so the durable migration contracts and dual-run SDK are usable without a source checkout.
- Added safe user-directory installers that refuse overwrite by default, do not request administrator access, do not create services, and do not change `PATH`.
- Added the public [database migration guide](https://csharpdb.com/docs/database-migration.html) and [migration release article](https://csharpdb.com/blog/migrating-existing-data-to-csharpdb.html), including source recipes, compatibility tools, export, dual-run, recovery, validation, security, and troubleshooting.

### Qualification Boundary

- The retained package, process-isolation, deterministic replay, and offline validation paths are reviewed and covered by automated tests.
- Broad live Access/ACE, SQL Server/MySQL server, authentication, TLS, restricted-account, published-runtime, platform, and disposable-Windows-VM qualification remains deferred. This release does not claim that every live provider configuration is shipping-qualified.
