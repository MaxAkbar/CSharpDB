# Sharding Master Catalog Plan

## Goal

Move production sharding metadata out of application JSON and into the CSharpDB-backed master database opened by the host. Application configuration should identify only the database to open through the normal `ConnectionStrings:CSharpDB` path; it should not carry a shard map, catalog locator, bucket ranges, directory entries, migration checkpoints, or failover history.

The master catalog is the only catalog implementation for this feature. JSON file catalog compatibility is intentionally not carried forward because the feature has not shipped.

## Current Baseline

- Admin, daemon, and API hosts open `ConnectionStrings:CSharpDB` as the master database and discover sharding from its catalog tables.
- Directory workflows, migration checkpoints, migration progress, and migration history already write through the existing catalog store when catalog writes are enabled.
- Admin can operate sharded clients once the opened master database has an active shard map.
- Admin can create sharding metadata for a new local direct master DB, but it
  does not split an existing monolithic database into shards.

## Target Shape

The production path should look like this:

```json
{
  "ConnectionStrings": {
    "CSharpDB": "Data Source=master.db"
  }
}
```

Only the normal database locator remains in config. The opened master database owns the active map and operational history.

## Work Plan

### 1. Catalog Discovery

- Use the host's opened `CSharpDbClientOptions` as the CSharpDB master catalog input.
- Treat an active map in the opened master database as the sharding signal.
- Keep the catalog implementation master-DB only.
- Keep direct shard-map construction internal to tests/tools; public app code uses `SeedMasterCatalogAsync` or catalog-update APIs to write the master DB, then uses `TryCreateFromMasterCatalogAsync`.

### 2. Catalog Store Boundary

- Split catalog resolution from sharded-client construction so startup can read the master DB before opening shard clients.
- Add async catalog resolution for master-catalog I/O.
- Keep the store backed only by the CSharpDB master catalog.

### 3. CSharpDB Master Catalog Provider

- Store the active shard map in a master catalog table as JSON.
- Store catalog apply history in a separate table.
- Store migration history and migration checkpoints in dedicated tables.
- Initialize missing catalog tables automatically when writes are allowed.
- Read the active map from the opened master database during async sharded client creation.

### 4. Admin And Daemon Startup

- Make Admin, daemon, and API startup attempt sharded-client creation from the opened master database.
- Keep unsharded behavior unchanged.
- Add a small Admin-visible catalog source label so operators can tell when the map came from the master DB.

### 5. Operational Writes

- Route catalog updates, directory mutations, migration checkpoints, migration history, and future failover history through the master catalog store.
- Ensure catalog writes are disabled when `AllowWrites=false`.
- Keep active-map replacement atomic from the caller perspective.
- Keep restart/recreate semantics for map activation until live map reload is added.

### 6. Tests And Acceptance Gates

- CSharpDB catalog tests create a master DB, seed an active map, and create a sharded client without sharding config.
- Catalog update tests verify the active map and history are persisted in the master catalog DB.
- Migration checkpoint/progress tests run against the master catalog.
- Admin/daemon tests verify the opened master DB activates sharding without sharding config.
- Full solution build passes after the provider slice.

## Assumptions

- "No configuration" means no sharding configuration in appsettings; the normal database connection string points at the master DB.
- The master catalog is not a data shard and must not be part of fan-out operations.
- Existing monolithic databases require a split/backfill/verify/cutover workflow
  before the master catalog is seeded. See
  `docs/sharding-existing-database-migration.md`.
- JSON file catalog compatibility is not supported.
- Automatic distributed SQL planning is still out of scope.
- Automatic failover remains a later milestone; this plan only creates the catalog foundation it will use.
