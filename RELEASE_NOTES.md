# What's New

## version4.3.0

version4.3.0 adds a first-party, review-first workflow for moving schemas and rows from other data sources into a new CSharpDB database. Migration is deliberately staged: capture a source, review the conversion plan and target DDL, apply to a new target, resume from committed target receipts if interrupted, and activate only after validation passes.

### Migration Source Lanes

- Added retained, digest-pinned migration packages for strict CSV, JSON/NDJSON, SQLite, LiteDB 5, MySQL 8.0/8.4, and the bounded on-premises SQL Server 2019/2022/2025 candidate lanes.
- Added coherent SQLite online backup and offline/quiesced LiteDB capture, with bounded source profiles and explicit diagnostics for unsupported source objects.
- Added isolated SQL Server and MySQL workers for bounded schema and row capture. Connection strings are accepted only through named environment variables and are needed only during capture.
- Added deterministic scalar encodings, row ordering, catalog/package bindings, package size ceilings, and fail-closed diagnostics for the implemented SQL Server and MySQL relational subsets.
- SQL Server retained capture remains an evaluation lane, not an apply-ready least-privilege workflow: complete metadata visibility is currently proven only for `sysadmin`, while a clean restricted account remains `Unknown` and blocks planning. Do not elevate a migration account to bypass that guard.

### Review, Recovery, and Validation

- Added explicit `inspect`, `plan`, `preview`, `apply`, and `validate` commands plus an `apply --resume` recovery mode. Exact target DDL and isolated scratch-schema evidence are available before target creation.
- Apply writes a new staged target and commits rows with target receipts. Resume reopens and requalifies the exact package binding and skips only batches with matching target receipts.
- Checksum validation compares normalized schema, 64-bit row counts, and partitioned canonical SHA-256 evidence. Failed or inconclusive validation withholds activation.
- Retained packages are treated as plaintext-sensitive source data. Expected package digests must be stored in an independently trusted record.

### Packaging and Documentation

- Added combined, framework-dependent migration CLI archives for Windows x64, Linux x64, and macOS Apple silicon. Archives require the .NET 10 runtime and retain the fixed SQL Server and MySQL adapter layout, notices, licenses, and installers; `SHA256SUMS.txt` is produced alongside the archives.
- Added safe user-directory installers that refuse overwrite by default, do not request administrator access, do not create services, and do not change `PATH`.
- Added the public [database migration guide](https://csharpdb.com/docs/database-migration.html), including source recipes, recovery, validation, security, troubleshooting, and the distinction from the older library-rename migration guide.

### Qualification Boundary

- The retained package, process-isolation, deterministic replay, and offline validation paths are reviewed and covered by automated tests.
- Broad live SQL Server/MySQL server, authentication, TLS, restricted-account, published-runtime, and disposable-Windows-VM qualification remains deferred. This release does not claim that every live provider configuration is shipping-qualified.