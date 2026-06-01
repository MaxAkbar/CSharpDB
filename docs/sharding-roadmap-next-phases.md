# Sharding Roadmap Next Phases

Internal planning note for the post-V1 sharding roadmap. This document is kept
under the top-level `docs/` folder so the team can revisit the plan without
digging through chat history. It is not linked from the public `www/docs` site.

## Summary

API-Level Sharding V1 introduced explicit route-key based routing above the
storage engine. The next phases should make that usable in Admin first, then add
operator catalog management, then controlled resharding. Replication, failover,
and cross-shard query support come later and should not block Admin route-aware
V1.

Default product decisions:

- Admin route context is per tab, not global.
- Phase 1 focuses on route-aware single-shard Admin workflows.
- Phase 2 introduces shard-admin APIs and a read-only catalog view.
- Phase 3 introduces operator-managed catalog updates.
- Controlled resharding comes before replication or cross-shard SQL.
- Config-only sharding remains supported after catalog mode exists.
- A global shard index, also called a shard directory, is a catalog feature for
  resolving alternate lookup keys to route context. It is not a normal in-shard
  table index and should not imply automatic distributed SQL.

## Phase 1: Admin Route-Aware Single-Shard UX

Goal: make Admin usable against a sharded keyspace without pretending CSharpDB
supports distributed SQL.

Key work:

- Detect sharded connections during Admin startup.
- Preserve current unsharded behavior exactly.
- Add per-tab route context to Admin tab state.
- Add a route selector for keyspace and shard key.
- Show resolved shard id, bucket, token, and map version for the selected route.
- Make each table/query/collection/form/report tab keep its own route snapshot.
- Add an Admin routing service that returns the correct route-bound
  `ICSharpDbClient` for the current tab.
- For direct sharded Admin, use `CSharpDbShardedClient.ForRoute(...)`.
- For remote HTTP/gRPC Admin, create or cache route-bound clients with
  `CSharpDbClientOptions.RouteContext`.
- Disable route-required tabs until a route is selected.
- Label routed tabs clearly as operating on one route.
- Keep Desktop Admin default behavior unchanged: it still opens one local direct
  database unless explicitly configured for sharding.

Important limits:

- `GetInfoAsync()` may remain aggregate/cluster-aware.
- Table browse, SQL, collections, procedures, forms, reports, pipelines, and
  query designer run against only the selected route.
- No cross-shard query inference from SQL.
- No automatic tenant discovery in this phase.

## Phase 2: Shard Admin APIs And Read-Only Catalog

Goal: expose shard topology and operational status through explicit admin
interfaces instead of overloading normal `ICSharpDbClient` data operations.

First implementation slice:

- Added `ICSharpDbShardAdminClient`.
- Added read-only map snapshot models, route resolution preview, shard status,
  and explicit execute-on-all-shards SQL.
- Exposed the shard-admin surface through direct sharded clients, REST endpoints,
  and gRPC RPCs.
- Added read-only shard-directory model types as placeholders for future global
  lookup indexes. Static config still returns an empty directory list.
- Admin UI workspace work remains for the next slice.

Key work:

- Add a shard-admin client surface such as `ICSharpDbShardAdminClient`. (started)
- Expose shard map snapshot, route resolution preview, shard health/status, and
  execute-schema-on-all-shards operations.
- Add REST and gRPC shard-admin endpoints so Admin can manage remote sharded
  daemons without direct shard-file access.
- Add an Admin Sharding workspace with read-only views for:
  - shard definitions;
  - bucket ranges;
  - exact-key pins;
  - map version;
  - status and errors;
  - route simulation.
- Keep static config as the source of truth in this phase.
- Add a catalog abstraction so Phase 3 can introduce persistent catalog-backed
  management without rewriting Admin.
- Add read-only shard-directory model types so Admin can preview future global
  lookup indexes such as `order_number -> order_month -> shard`.
- Add route lookup simulation for alternate keys, but keep catalog data static
  and read-only in this phase.

## Phase 3: Operator-Managed Catalog And Map Changes

Goal: make shard maps inspectable and editable through an operator workflow
while preserving config-only deployments.

First implementation slice:

- Added catalog-backed mode with `CSharpDB:Sharding:Catalog`.
- Added JSON catalog file loading at sharded-client startup.
- Added catalog state, validation, and apply models.
- Added REST and gRPC APIs to inspect the live/pending catalog state, validate a
  proposed map, and persist an applied map.
- Applying a catalog update writes a pending map and returns
  `RequiresRestart = true`; it does not mutate live routing in-process.
- Map version checks and migration-required validation prevent silent bucket or
  exact-pin ownership changes unless an operator explicitly acknowledges a
  metadata-only change.
- Directory definitions and directory entries validate against route ownership,
  giving the future global shard-directory index a persisted metadata home.
- Admin UI draft/apply screens remain for a later slice.

Key work:

- Add catalog-backed mode behind configuration. (started)
- Store keyspace, map versions, shard definitions, bucket ranges, exact-key
  pins, disabled/read-only flags, and change history. (started)
- Let Admin draft, validate, preview, and apply catalog changes. (backend API started)
- Add operator-managed shard-directory entries for alternate lookup keys that
  must resolve to a route context before data can be queried.
- Support directory entry states such as `Reserved`, `Active`, `Moving`,
  `Disabled`, and `Deleted` so write and migration workflows can reason about
  partial progress.
- Require an explicit consistency policy for directory-backed writes. Examples:
  reserve directory entry before writing a write-once row, or write the row first
  and repair/upsert the directory entry through an idempotent background process.
- Require explicit confirmation for metadata-only changes.
- Do not silently remap existing data. Bucket ownership changes require either
  a completed migration record or an operator acknowledgement that data has
  already been moved.

Validation requirements:

- Reject duplicate shard ids.
- Reject uncovered buckets.
- Reject overlapping bucket ranges.
- Reject bucket ownership by unknown shards.
- Reject enabled maps that leave all shards disabled.
- Reject exact-key pins to unknown shards.
- Reject shard-directory entries that point to unknown keyspaces, invalid route
  keys, unknown shards, or stale map versions unless an operator explicitly marks
  them as historical/deleted.
- Warn or reject disabled shard ownership depending on operation type.
- Require monotonically increasing map versions for applied catalog changes.

## Experimental Track: Global Shard Directory Index

Goal: support scenarios where the caller has an alternate key and needs to find
the route key or shard before querying the real data. This acts like an outside
index on top of the shards.

Example use cases:

- `order_number -> order_month -> shard`
- `invoice_number -> route key -> shard`
- `customer_id -> home shard`
- `email -> user shard`
- `legacy_id -> canonical route key`

Possible entry shape:

```csharp
public sealed class CSharpDbShardDirectoryEntry
{
    public required string DirectoryName { get; init; } // "orders_by_id"
    public required string LookupKey { get; init; }     // "SO-202605-2044"
    public required string TargetKeyspace { get; init; } // "orders_by_month"
    public required string RouteKey { get; init; }      // "2026-05"
    public required string ShardId { get; init; }       // "shard-1"
    public int MapVersion { get; init; }
    public required string State { get; init; }         // Reserved, Active, Moving
}
```

Lookup flow:

```text
entry = directory.resolve("orders_by_id", "SO-202605-2044")

route = {
  keyspace: entry.TargetKeyspace,
  key: entry.RouteKey
}

client = sharded.ForRoute(route)
order = client.query(
  "SELECT * FROM orders WHERE order_number = @order_number",
  order_number = "SO-202605-2044")
```

Write-once flow for orders:

```text
1. Reserve directory entry for order_number.
2. Resolve or assign the target route key, such as order_month.
3. Write the order row to the resolved shard.
4. Mark the directory entry Active.
5. Repair or delete Reserved entries that never completed.
```

Alternative write-first flow:

```text
1. Write row to the route chosen by application rules.
2. Upsert directory entry with route key, shard id, and map version.
3. Run a repair job that scans recent writes and fixes missing directory entries.
```

Important design rules:

- The directory resolves keys; it does not move data by itself.
- Directory entries must include the route key, not only the shard id, because
  bucket ownership can change across map versions.
- The shard id is useful for diagnostics and stale-entry detection, but route
  context remains the normal routing input.
- Directory-backed uniqueness is only as strong as the reservation/write policy.
- Mutable alternate keys require versioned or fenced updates to avoid pointing
  old keys at deleted or moved rows.
- Broad predicates such as `WHERE amount > 100` still require explicit fan-out
  or a purpose-built aggregate/index; the directory is for known lookup keys.

## Phase 4: Controlled Resharding

Goal: move route-key owned data safely and deliberately. Resharding is an
operator workflow, not automatic dynamic membership.

Implemented first slice:

- Added exact route-key migration through `ICSharpDbShardAdminClient`.
- Added REST `POST /api/sharding/migrations/exact-route-key`.
- Added gRPC `MigrateExactRouteKey`.
- Migration manifests can name route-owned tables by route-key column and
  primary-key column.
- Migration manifests can name route-owned collections by a top-level route-key
  JSON property.
- The sharded client fences writes for the affected route key while migration is
  active.
- The migration copies matching rows/documents to the destination shard, verifies
  counts and checksums, and writes a pending exact-key pin to the catalog.
- The live map remains unchanged until the sharded client or daemon is
  restarted.
- Verification failure leaves the active map unchanged.
- Missing manifest items are rejected.

Key work:

- Start with exact route-key migration. (first slice implemented)
- Add bucket-range migration after exact-key movement is stable.
- Require a migration manifest that identifies key-owned data per table and
  collection. (first slice implemented)
- Do not infer ownership from arbitrary SQL clauses.
- Fence writes for affected route keys while migration is active. (first slice
  implemented)
- Copy data from source shard to destination shard. (first slice implemented)
- Verify counts and checksums. (first slice implemented)
- Update exact-key pins or bucket ownership only after verification. (exact-key
  pins implemented)
- Record migration history and final status. (result status and catalog history
  comment implemented; durable migration history remains)
- Leave the old map active when migration verification fails. (first slice
  implemented)

Remaining work:

- Add durable migration history as a first-class catalog section, not only the
  apply history comment.
- Add resumable/retryable migration states for partial copy failures.
- Add shard-directory repair or stale marking when directory entries point at
  the old shard.
- Add bucket-range migration after exact-key movement is stable.
- Add Admin UX for migration preview, progress, verification, and confirmation.

Admin workflow:

- Show source shard, destination shard, affected route keys or buckets, required
  manifest, progress, verification, and stopping points.
- Surface failures as recoverable operator states.
- Require confirmation before metadata-only recovery.

## Phase 5: Read-Only Cross-Shard Operations

Goal: provide explicit fan-out read tooling for diagnostics and Admin without
claiming general distributed SQL support.

Key work:

- Add explicit scatter/gather read APIs for Admin and diagnostics.
- Support read-only fan-out SQL execution with per-shard results and per-shard
  errors.
- Present Admin output as per-shard result sets by default.
- Add aggregate helpers only when they are explicit API operations.

Out of scope:

- Cross-shard writes.
- Cross-shard joins.
- Cross-shard transactions.
- Automatic query planner fan-out from arbitrary SQL.

## Phase 6: Replication And Failover

Goal: add high-availability primitives after catalog and resharding workflows are
stable.

Key work:

- Build replication from retained change feeds or WAL-derived replication
  primitives, not router retry behavior.
- Add catalog metadata for shard roles, replicas, health, lag, and promotion
  eligibility.
- Start with manual/operator-confirmed failover.
- Add automatic failover policy only after manual failover is proven.
- Add Admin views for replica health, lag, promote/demote actions, and failover
  audit history.

## Public API And Interface Additions

Likely additions:

- `ICSharpDbShardAdminClient`
- `CSharpDbShardMapSnapshot`
- `CSharpDbShardAdminOptions`
- `ICSharpDbShardDirectoryClient`
- `CSharpDbShardDirectoryEntry`
- `CSharpDbShardDirectoryResolution`
- `CSharpDbShardDirectoryWritePolicy`
- Catalog validation result models
- Per-tab Admin route state service
- Admin route-bound client factory
- Route validation helpers

Later migration additions:

- `CSharpDbShardMigrationPlan`
- `CSharpDbShardMigrationManifest`
- `CSharpDbShardMigrationStatus`
- Catalog update request and result models

## Test Plan

Phase 1 Admin tests:

- Sharded startup succeeds.
- Unsharded startup and existing Admin behavior remain unchanged.
- Route-required tabs are blocked until route context is selected.
- Per-tab route isolation works when two tabs use different route keys.
- Direct sharded Admin routes through `ForRoute(...)`.
- Remote HTTP/gRPC Admin sends route context through `CSharpDbClientOptions`.

Phase 2 shard-admin tests:

- Map snapshot returns configured shards, bucket ranges, exact pins, and map
  version.
- Route simulation resolves expected shard, bucket, token, and map version.
- Shard status reports healthy, disabled, and failing shards.
- Execute-schema-on-all-shards reports per-shard success and failure.
- Read-only shard-directory lookup previews return route context and resolved
  shard metadata without executing data queries.
- Existing API-key behavior remains unchanged.

Phase 3 catalog tests:

- Config-backed catalog matches existing static configuration behavior.
- Catalog validation rejects malformed maps.
- Applied map versions are recorded in history.
- Exact-key pin edits validate and apply.
- Shard-directory entries validate target keyspace, route key, shard id, map
  version, and state transitions.
- Directory reservation and activation flows are idempotent for write-once
  workloads.
- Bucket range edits require safe migration metadata or explicit
  metadata-only acknowledgement.

Phase 4 resharding tests:

- Exact-key migration copies data to the destination shard.
- Write fencing blocks conflicting writes during migration.
- Verification failure leaves the old route active.
- Successful migration updates pins or bucket ownership.
- Successful migration updates affected shard-directory entries or marks stale
  entries for repair before exposing the new route.
- Missing migration manifests are rejected.

Phase 5 cross-shard read tests:

- Fan-out read returns one result per shard.
- Partial shard failure is reported without hiding successful shard results.
- Cancellation and timeout behavior are deterministic.
- Admin labels results by shard.

Phase 6 replication tests:

- Replica metadata loads from catalog.
- Lag and health are reported.
- Manual promotion updates catalog state.
- Failed promotion leaves prior primary ownership intact.

Release verification:

- Run existing sharding, daemon, ADO.NET, Admin Forms, Admin Reports, and full
  solution build on the `version4.0.0` branch.

## Assumptions

- Phase 1 is the next implementation target.
- Admin route context is per-tab.
- Config-only sharding remains supported after catalog mode exists.
- Global shard directory indexes are optional. They help locate a route from an
  alternate key, but applications that already know the route key should keep
  using direct route-key routing.
- Resharding is operator-controlled and manifest-driven.
- Cross-shard SQL, replication, and failover are future phases and do not block
  Admin route-aware V1.
- This document is internal planning material and should not be linked from the
  public website unless explicitly requested.
