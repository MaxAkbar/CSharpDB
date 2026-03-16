# gRPC Tunable File-Cache Hybrid Mode

## Summary
- Treat hybrid mode for gRPC as a supported file-backed warm-cache mode, not a new full-RAM storage engine.
- Reuse the daemon’s existing long-lived `Database` instance and pager cache, then make cache residency explicit, bounded, and configurable for remote workloads.
- Keep the database file and WAL as the source of truth; no change to gRPC RPC contracts, backup/restore semantics, or single-writer rules.

## Key Changes
- Add a direct-only engine configuration hook to `CSharpDbClientOptions`:
  - New property: `DirectDatabaseOptions : DatabaseOptions?`
  - It applies only when `Transport = Direct`; reject it for `Http`, `Grpc`, and `NamedPipes` to avoid ambiguous remote config.
- Thread `DirectDatabaseOptions` through direct transport creation and reopen paths:
  - `ClientTransportResolver` passes it into `EngineTransportClient`
  - `EngineTransportClient` stores it and uses it for the initial open and all later reopens after restore, vacuum, reindex, or cached-handle release
- Add daemon config for the hybrid mode:
  - New section: `CSharpDB:Storage`
  - New enum: `Mode = Default | TunedFileCache`
  - Optional overrides: `MaxCachedPages`, `MaxCachedWalReadPages`, `UseMemoryMappedReads`, `EnableSequentialLeafReadAhead`
- Define `TunedFileCache` precisely:
  - Base it on `UseLookupOptimizedPreset()`
  - Default values are therefore the existing preset values: `MaxCachedPages = 2048`, `MaxCachedWalReadPages = 256`, `UseMemoryMappedReads = true`
  - If overrides are provided, apply them on top of that preset and leave unspecified fields at preset defaults
- Keep `Default` mode behavior-compatible with today’s daemon:
  - Same direct open path
  - No forced bounded cache unless the new mode is selected
- Add daemon startup logging that prints resolved storage mode and effective cache settings so operators can verify the daemon is in hybrid mode without adding new RPC fields.
- Update daemon docs and sample config to show “portion in memory” means pager page cache plus WAL read cache, not `LoadIntoMemoryAsync` full-database mirroring.

## Public API / Config
- `CSharpDbClientOptions.DirectDatabaseOptions`
- `CSharpDB:Storage:Mode`
- `CSharpDB:Storage:MaxCachedPages`
- `CSharpDB:Storage:MaxCachedWalReadPages`
- `CSharpDB:Storage:UseMemoryMappedReads`
- `CSharpDB:Storage:EnableSequentialLeafReadAhead`

## Test Plan
- Add unit tests proving direct transport honors `DirectDatabaseOptions` on first open and after forced reopen flows.
- Add config-binding tests for daemon storage settings, including invalid values and `Default` vs `TunedFileCache` resolution.
- Extend existing gRPC daemon integration tests to run once with `Mode = TunedFileCache` and confirm CRUD, collections, transactions, backup, restore, and maintenance still behave the same.
- Add a focused transport benchmark or perf harness that compares default daemon mode vs tuned-file-cache mode for repeated gRPC point lookups and collection gets on a larger database; use it to confirm the new mode is worth documenting as the recommended gRPC hybrid setup.

## Assumptions
- V1 does not implement full in-memory mirroring or selective table/collection pinning.
- V1 does not change protobuf/RPC models or `DatabaseInfo`; observability is via config and startup logs.
- The recommended operator path for gRPC hybrid mode is explicit opt-in `TunedFileCache`, not a silent change to existing daemon defaults.
- Memory budgeting is documented in page terms: cached main-file pages and WAL pages are 4 KB each, plus normal query/materialization overhead.
