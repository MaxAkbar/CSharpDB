# What's New

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
