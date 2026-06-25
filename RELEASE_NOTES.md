# What's New

## version4.0.0

version4.0.0 introduces API-level sharding for CSharpDB. It adds explicit route-key routing, master-catalog-backed shard maps, Admin route selection, shard administration APIs, manifest-driven resharding workflows, diagnostic read-only fan-out, and metadata for future replica and failover workflows. It also completes the NuGet package dependency closure and moves release publishing to NuGet Trusted Publishing.

### API-Level Sharding

- Added route contexts, stable virtual-bucket shard maps, exact route-key pins, route-bound clients, shard status helpers, and shard-prefixed transaction ids.
- Added `CSharpDbShardedClient` and helpers for resolving route keys to shard-specific direct, HTTP, gRPC, daemon, and ADO.NET client operations.
- Added master-catalog mode so the master database owns active sharding metadata while application data remains in shard databases.
- Added route propagation through REST, gRPC, daemon registration, and connection-string based clients.
- Added an e-commerce order-history sample that demonstrates month-based route keys, exact route access, and application-controlled cross-month page fill.
- Defined the v4 sharding boundary clearly: routing is explicit and API-level. CSharpDB does not infer routes from SQL predicates, and v4.0.0 does not add distributed joins, distributed writes, or distributed transactions.

### Shard Administration and Catalog Management

- Added `ICSharpDbShardAdminClient` for shard map snapshots, route resolution previews, shard status checks, and explicit all-shards operations.
- Exposed shard administration through direct clients, REST endpoints, gRPC services, and daemon-hosted workflows.
- Added catalog-backed shard map validation and apply flows, including pending map changes that are activated by recreating or restarting route-bound clients.
- Added shard catalog history and migration history so operators can inspect route moves and catalog changes.
- Added read-only all-shards SQL fan-out with mutating SQL rejection, per-shard result reporting, and per-shard error reporting.

### Resharding and Migration Workflows

- Added manifest-driven exact route-key migration for moving individual route keys between shards.
- Added bucket-range migration for moving virtual bucket ranges between shards.
- Added write fencing, route-owned object manifests, copy and verification steps, counts, checksums, pending catalog updates, and recovery metadata for operator-managed moves.
- Added shard directory workflows and metadata so master-catalog deployments can track shards, roles, and route ownership consistently.
- Added guidance for moving an existing database into a sharded deployment. v4.0.0 does not automatically split a monolithic database or infer a shard key from existing tables.

### Admin Sharding Experience

- Added the Admin Sharding workspace for topology, status, catalog inspection, catalog draft validation and apply, history, and route simulation.
- Added per-tab route context and route selection for sharded Admin workflows, including query, table data, collection data, and shared grid operations.
- Added route-bound Admin client leases for direct and remote transports.
- Preserved unsharded Admin behavior for databases that do not enable sharding.

### Replica Metadata

- Added primary and replica shard role metadata, primary links, promotion eligibility, and operator-reported lag fields.
- Exposed replica metadata through shard snapshots and status APIs.
- v4.0.0 stores replica metadata only. Automated replication, catch-up, promotion, and failover are not included in this release.

### NuGet and Release Publishing

- Added NuGet packages for the dependency closure required by the published artifacts, including `CSharpDB.ImportExport`, `CSharpDB.CodeModules`, and `CSharpDB.Generators`.
- Added a package closure verification script and workflow integration so published packages do not depend on unpublished CSharpDB packages.
- Switched the release workflow from a long-lived NuGet API key to NuGet Trusted Publishing.

### Documentation, Samples, and Tests

- Added public API-level sharding documentation and updated the docs index and sample pages.
- Added sharding migration, master-catalog, roadmap, and remaining-work guidance for operators and maintainers.
- Added the `samples/api-level-sharding` sample project and README.
- Added and expanded tests for sharded clients, daemon and gRPC routing, ADO.NET route connections, shard admin workflows, catalog validation, migration workflows, and Admin sharding services.

### Review Notes and Known Limits

- Applications must provide a route key, or Admin users must choose a route context. SQL predicates do not choose the route automatically.
- The master database stores sharding metadata and catalog state. It is not used as a data shard or implicit fan-out participant.
- Catalog changes and migration workflows write pending map changes; active clients must be recreated or restarted to pick them up.
- Read-only fan-out is a diagnostic and operator helper, not a distributed query planner.
- Existing monolithic database splitting remains a manual backfill and cutover process.
