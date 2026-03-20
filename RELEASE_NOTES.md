# What's New

## v2.2.0 (Unreleased)

### SQL Read-Path Performance

- Added storage read-path optimizations for pager and WAL hot paths.
- Narrowed reader and write-path regressions introduced during earlier tuning.
- Reduced insert metadata churn and front-door overhead.
- Expanded composite and grouped index fast paths.
- Added composite grouped index aggregates for single-pass grouped queries.
- Broadened join lookup and covered join paths to reduce intermediate row copies.
- Optimized correlated `EXISTS` and `IN` filter evaluation.
- Optimized correlated `NOT IN` filters.
- Fused scan filter and projection paths to avoid unnecessary row materialization.
- Added filtered scalar aggregate fast path for single-pass aggregate queries.
- Added compact scan projection path and compact indexed range projection paths.
- Batched compact scan expression projections.
- Batched generic expression projection operators.
- Extended batch transport through generic projections.
- Batched aggregate and join consumers.
- Checkpoint SQL batched transport groundwork (internal executor-level batching; the full public batch transport is forward-looking design work).

### Collection Storage and Indexing

- Added binary collection payload read path and faster binary collection hydration.
- Added path-based collection index APIs (`EnsureIndexAsync`, `FindByPathAsync`, `FindByPathRangeAsync`).
- Added multi-value array collection indexes for terminal array-element indexing.
- Added collection path query API with string-path forms such as `FindByPathAsync("$.address.city", ...)` and `FindByPathAsync("$.tags[]", ...)`.
- Added collection path range queries for integer and text paths.
- Added nested array path collection indexes for `$.orders[].sku`-style paths.
- Added ordered text collection indexes for index-backed text equality and text range queries.
- Added Guid and temporal (DateTime) collection path indexing.

### Hybrid Storage Mode

- Redesigned hybrid mode around lazy-resident durable storage.
- Added gRPC tunable file-cache hybrid mode documentation and configuration options.

### Client-Wide Backup and Restore

- `ICSharpDbClient` now exposes `BackupAsync` and `RestoreAsync` as first-class operations.
- Backup and restore work across Direct, HTTP, gRPC, CLI, and Admin flows.
- `CSharpDB.Api` exposes `/api/maintenance/backup` and `/api/maintenance/restore`.
- CLI `.backup` / `.restore` now route through `ICSharpDbClient` instead of calling engine helpers directly.

### Index Store

- Implemented `ReplaceAsync` method for index stores and updated related logic across the codebase.

### Documentation and Maintenance

- Added SQL batched row transport design document (forward-looking design for next phase).
- Refreshed architecture docs and published storage advanced examples.
- Expanded v2.2.0 collection release notes and broadened v2.2.0 release notes.
- Added benchmark results for CSharpDB v2.2 and v2.0 comparison results.
- Cleaned up trim warnings and normalized line endings.
- Updated `.gitignore` to include `tmp/` directory.
- Deleted old benchmarks.