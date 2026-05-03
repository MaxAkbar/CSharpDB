# API-Level Sharding Plan

This is a research and design proposal. API-level sharding is not a shipped
CSharpDB feature today.

## Goal

The first target for sharding is write throughput. CSharpDB's durable commit
path is isolated to a database file and its WAL, so multiple independent
database files can spread write pressure across separate commit paths while the
API decides where each request belongs.

The recommended v1 shape is a routing layer above the existing client and
daemon surfaces, not a distributed pager or storage-engine rewrite.

## Recommended V1 Design

- Run one warm `ICSharpDbClient` per shard.
- Treat each shard as a normal CSharpDB database file with its own `.db` file,
  WAL, checkpoints, maintenance lifecycle, and storage options.
- Add a shard catalog that maps a stable shard key such as `tenantId` or
  `accountId` to a shard identity and database path or endpoint.
- Require a shard key for writes and point reads.
- Keep write transactions scoped to one shard.
- Do not support cross-shard transactions in v1.
- Add read-only fan-out later for admin, diagnostics, and reporting workloads
  that explicitly need all shards.

Conceptually:

```text
HTTP/gRPC/API request
        |
        v
Shard key extraction
        |
        v
IShardRouter
        |
        +--> shard-a.db / ICSharpDbClient
        +--> shard-b.db / ICSharpDbClient
        +--> shard-c.db / ICSharpDbClient
```

## Proposed Interfaces

These names describe the intended public shape. They are not implemented yet.

```csharp
public interface IShardRouter
{
    ValueTask<ShardRoute> ResolveAsync(
        string shardKey,
        CancellationToken cancellationToken = default);
}

public sealed record ShardRoute(
    string ShardId,
    string DataSource,
    ICSharpDbClient Client);

public interface IShardedCSharpDbClient
{
    ValueTask<ICSharpDbClient> GetClientAsync(
        string shardKey,
        CancellationToken cancellationToken = default);
}
```

Two request shapes are worth considering:

- Header-based routing: `X-CSharpDB-Shard-Key: tenant-0042`
- Route-based routing: `/api/{tenantId}/tables/...`

Header-based routing preserves the existing URL layout. Route-based routing is
more visible and easier to test manually. Either way, the API should fail fast
when a sharded operation does not include a shard key.

## Shard Key Guidance

Choose the shard key before implementation. Changing it later usually requires
moving most or all data.

Good shard keys are:

- Stable and immutable.
- High-cardinality, such as tenant, account, organization, or workspace IDs.
- Present on the dominant write and point-read requests.
- Shared by related tables and collections so normal application workflows stay
  on a single shard.

Avoid shard keys that are:

- Auto-incrementing IDs.
- Sequential timestamps.
- Booleans or low-cardinality enums.
- Values users commonly edit.
- Attributes that do not appear in normal request filters.

For multitenant data, prefer colocating related tenant data in the same shard.
That means tenant-owned tables and collections should include the tenant key,
and API calls should route by that same key.

## Query And Transaction Rules

V1 should make single-shard behavior explicit:

- `INSERT`, `UPDATE`, `DELETE`, document writes, point reads, and explicit
  transactions require one shard key and execute on one shard.
- Raw SQL execution requires a shard key unless the endpoint is explicitly
  marked as an all-shards read-only operation.
- Cross-shard joins are out of scope for v1.
- Cross-shard transactions are out of scope for v1.
- Global reads should use an explicit fan-out API that returns per-shard
  results and partial-failure metadata.

This keeps the write-throughput feature honest: each shard can commit
independently without a distributed transaction coordinator.

## Shard Catalog

Start with a small catalog that can be loaded at daemon/API startup:

```json
{
  "strategy": "lookup",
  "shards": [
    { "id": "shard-a", "dataSource": "data/shard-a.db" },
    { "id": "shard-b", "dataSource": "data/shard-b.db" }
  ],
  "routes": [
    { "key": "tenant-0001", "shardId": "shard-a" },
    { "key": "tenant-0002", "shardId": "shard-b" }
  ]
}
```

A lookup strategy is the best first step because it supports deliberate tenant
placement and later tenant movement. A later version can introduce virtual
shards:

```text
tenantId -> virtual shard -> physical shard
```

Virtual shards make rebalancing less disruptive because a new physical shard can
take ownership of some virtual shards without changing application routing code.

## Operations

Sharding multiplies operational work. Each operation should report per-shard
status instead of hiding failures behind one aggregate result.

- Backup: back up every shard independently and record the shard id, database
  path, timestamp, and result.
- Restore: restore one shard at a time unless an explicit all-shards restore
  workflow is added.
- Reindex, vacuum, checkpoint, and inspect: support one shard or all shards with
  per-shard output.
- Schema changes: apply DDL to all shards through an orchestrated migration
  command and capture success/failure per shard.
- Monitoring: aggregate size, WAL, checkpoint, error, latency, and throughput
  metrics by shard.
- Rebalancing: move one tenant or virtual shard at a time, block or redirect
  writes during the move, validate row/document counts, update the catalog, and
  refresh router caches.

## Open Questions

- Should the first catalog live in JSON configuration, a control CSharpDB
  database, or both?
- Should API routing prefer headers, route prefixes, or support both?
- Should raw SQL require a shard key only, or should the API also validate that
  the SQL filters by the shard key?
- Should fan-out reads return one combined result set, per-shard result sets, or
  both?
- How should Admin UI expose shard selection and all-shards maintenance tasks?

## Suggested Rollout

1. Add configuration models for shard definitions and routing strategy.
2. Add an `IShardRouter` implementation backed by the configured catalog.
3. Add a sharded client wrapper that opens and owns one warm `ICSharpDbClient`
   per shard.
4. Add shard-key extraction to API/daemon endpoints for writes and point reads.
5. Add one-shard maintenance operations, then all-shards maintenance with
   per-shard results.
6. Add benchmarks comparing one database file versus multiple routed shards for
   independent tenant write workloads.

