# CSharpDB.Client

`CSharpDB.Client` is the authoritative database API for CSharpDB.

It owns the public client contract used to talk to a database, while transport and lower-level implementation details stay behind that boundary.

## Current Direction

- `CSharpDB.Client` is now the real implementation layer for database access.
- `Direct`, `Http`, and `Grpc` are implemented transports today.
- `NamedPipes` remains the only future transport target.

## Current Transport Model

Create the client with `CSharpDbClientOptions`:

```csharp
var client = CSharpDbClient.Create(new CSharpDbClientOptions
{
    Transport = CSharpDbTransport.Direct,
    DataSource = "csharpdb.db",
    HybridDatabaseOptions = new HybridDatabaseOptions
    {
        PersistenceMode = HybridPersistenceMode.IncrementalDurable,
        HotTableNames = ["users"],
        HotCollectionNames = ["session_cache"]
    },
    DirectDatabaseOptions = new DatabaseOptions()
});
```

The transport can be selected explicitly with `Transport`. If it is omitted, the client infers it from `Endpoint` and otherwise defaults to direct.

Direct resolution currently accepts:

- `Endpoint` as a file path
- `Endpoint` as `file://...`
- `DataSource`
- `ConnectionString` containing `Data Source=...`
- optional `HybridDatabaseOptions` for the lazy-resident hybrid direct mode
- optional `DirectDatabaseOptions` for direct transport engine/pager tuning

Resolution rules:

- direct is the default when transport cannot be inferred from a network endpoint
- supplied direct inputs must resolve to the same target
- `HybridDatabaseOptions` is supported only for direct transport and is rejected for `Http`, `Grpc`, and `NamedPipes`
- `DirectDatabaseOptions` is supported only for direct transport and is rejected for `Http`, `Grpc`, and `NamedPipes`
- `http://` and `https://` infer `Http` unless `Transport = CSharpDbTransport.Grpc` is set explicitly
- `pipe://` and `npipe://` infer `NamedPipes`
- `Grpc` uses `http://` or `https://` endpoints and talks to `CSharpDB.Daemon`
- `Http` uses `http://` or `https://` endpoints and talks to `CSharpDB.Api`
- `NamedPipes` still validates its endpoint shape and then fails with a not-implemented error
- `HttpClient` is supported for both `Http` and `Grpc`
- `ApiKey` and `ApiKeyHeaderName` are supported for both `Http` and `Grpc`

Use `HybridDatabaseOptions` when the direct client should run with a lazy
resident page cache while persisting committed state back to the resolved file
path. Typical patterns are:
- default `IncrementalDurable` for durable hybrid direct usage with on-demand page warming
- `IncrementalDurable` plus `HotTableNames` / `HotCollectionNames` when selected read-mostly objects should be preloaded into the hybrid cache at open
- `Snapshot` plus `Dispose` when the process wants explicit full-image export behavior on close
- `Snapshot` plus `None` when the process will call `SaveToFileAsync(...)` manually

Hot-set warming is a hybrid-only runtime hint. In v1 it:
- warms SQL table B+trees plus SQL secondary indexes
- warms collection backing tables only
- is supported only for `IncrementalDurable`
- requires the default unbounded pager cache and is rejected for bounded/custom cache setups

Use `DirectDatabaseOptions` when the in-process engine should open with explicit
storage tuning. Typical patterns are:
- `UseDirectLookupOptimizedPreset()` for hot local direct workloads
- `UseDirectColdFileLookupPreset()` for cache-pressured direct file reads
- `UseHybridFileCachePreset()` only for explicit bounded file-cache experiments

Remote transports do not accept either direct-only property because those
settings must be configured on the host process instead.

Example HTTP selection:

```csharp
var client = CSharpDbClient.Create(new CSharpDbClientOptions
{
    Transport = CSharpDbTransport.Http,
    Endpoint = "http://localhost:61818"
});
```

This resolves to the dedicated `CSharpDB.Api` REST host.

Example gRPC selection:

```csharp
var client = CSharpDbClient.Create(new CSharpDbClientOptions
{
    Transport = CSharpDbTransport.Grpc,
    Endpoint = "https://localhost:5001"
});
```

This resolves to the dedicated `CSharpDB.Daemon` gRPC host.

When the remote host is configured with API-key mode, set `ApiKey` once on the
client options. The HTTP transport sends it as a request header, and the gRPC
transport sends it as call metadata.

```csharp
var client = CSharpDbClient.Create(new CSharpDbClientOptions
{
    Transport = CSharpDbTransport.Grpc,
    Endpoint = "https://db-host:5821",
    ApiKey = "replace-with-a-secret",
    ApiKeyHeaderName = "X-CSharpDB-Api-Key"
});
```

API-key mode is shared-secret authentication only. It does not provide JWT,
RBAC, mTLS, or TLS termination.

## API-Level Sharding

`CSharpDB.Client` can route requests across multiple ordinary CSharpDB database
files with `CSharpDbShardedClient`. Sharding is an API/daemon feature: each
shard is still a standalone database file with its own WAL and commit path.

V1 uses an explicit route context instead of SQL inference. For an e-commerce
order-history workload, the route key could be the order month (`yyyy-MM`):

```csharp
await using var sharded = await CSharpDbShardedClient.CreateAsync(new CSharpDbShardingOptions
{
    Enabled = true,
    Keyspace = "orders_by_month",
    MapVersion = 1,
    VirtualBucketCount = 4096,
    Shards =
    [
        new CSharpDbShardDefinition { ShardId = "s0", DataSource = "orders-s0.db" },
        new CSharpDbShardDefinition { ShardId = "s1", DataSource = "orders-s1.db" },
    ],
    BucketRanges =
    [
        new CSharpDbShardBucketRange { StartBucketInclusive = 0, EndBucketExclusive = 2048, ShardId = "s0" },
        new CSharpDbShardBucketRange { StartBucketInclusive = 2048, EndBucketExclusive = 4096, ShardId = "s1" },
    ],
});

ICSharpDbClient juneOrders = sharded.ForRoute(new CSharpDbRouteContext
{
    Keyspace = "orders_by_month",
    Key = "2026-06",
});

await juneOrders.ExecuteSqlAsync("""
    SELECT order_number, order_date, amount
    FROM orders
    WHERE order_month = '2026-06'
    ORDER BY order_date DESC
    LIMIT 25 OFFSET 0;
    """);
```

Remote clients pass the same route through headers/metadata by setting
`CSharpDbClientOptions.RouteContext`. REST uses `X-CSharpDB-Keyspace` and
`X-CSharpDB-Shard-Key`; gRPC sends the same names as lowercase metadata.

The route key gets the request to the right database file. Queries should still
filter on the route-key column because several route keys can share one physical
shard. If a view spans multiple months, the caller explicitly runs multiple
routed requests and combines the results.

For paged history that crosses route keys, fill the page in application code:
query the newest route first, append its rows, then continue to older route keys
until the requested page size is satisfied. For later pages, skip whole routes by
count before applying a route-local `OFFSET`.

Other application patterns are valid when the UI has a bounded route window. A
recent-orders view can query the current and previous month, merge by
`order_date DESC, id DESC`, and take the requested page size even if that reads a
few extra rows. A date-range filter can compute the month route keys in the
range, query each with the same date predicate, and merge/limit the result. An
infinite-scroll API can avoid global counts by returning a continuation token
that records the remaining route keys and per-route cursor state.

Phase 2 adds an explicit shard-admin surface for topology and operational views:

```csharp
await using ICSharpDbShardAdminClient shardAdmin =
    CSharpDbClient.CreateShardAdmin(new CSharpDbClientOptions
    {
        Transport = CSharpDbTransport.Grpc,
        Endpoint = "https://db-host:5821",
    });

CSharpDbShardMapSnapshot map = await shardAdmin.GetShardMapAsync();
CSharpDbShardResolution preview = await shardAdmin.ResolveRouteAsync(new CSharpDbRouteContext
{
    Keyspace = "orders_by_month",
    Key = "2026-06",
});
IReadOnlyList<CSharpDbShardStatus> status = await shardAdmin.GetShardStatusAsync();
```

The shard-admin surface is separate from normal `ICSharpDbClient` data
operations. It exposes the map snapshot, route simulation, per-shard health, and
explicit execute-on-all-shards SQL for schema setup. It does not add automatic
cross-shard query planning.

V1 intentionally supports single-shard operations only. Cross-shard joins,
cross-shard transactions, automatic resharding, replication, and failover remain
out of scope. Changing bucket ownership requires an operator-controlled data
migration before the map is changed.

## Supported Surface

The current `ICSharpDbClient` includes:

- database info and data source metadata
- tables, schemas, row counts, browse, and primary-key lookup
- row insert, update, and delete
- table and column DDL
- indexes, views, and triggers
- saved queries
- procedures and procedure execution
- SQL execution with multi-statement splitting
- client-managed transaction sessions
- document collections
- maintenance: checkpoint, backup/restore, reindex, vacuum, and foreign-key retrofit migration
- storage diagnostics

## Foreign-Key Retrofit Migration

Older databases do not automatically gain foreign-key metadata just because they are opened on a newer engine. Use `MigrateForeignKeysAsync(...)` when you want to validate and then persist FK metadata onto existing tables:

```csharp
using CSharpDB.Client;
using CSharpDB.Client.Models;

await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
{
    DataSource = "mydata.db"
});

var spec = new[]
{
    new ForeignKeyMigrationConstraintSpec
    {
        TableName = "orders",
        ColumnName = "customer_id",
        ReferencedTableName = "customers",
        ReferencedColumnName = "id",
        OnDelete = ForeignKeyOnDeleteAction.Cascade,
    },
};

var preview = await client.MigrateForeignKeysAsync(new ForeignKeyMigrationRequest
{
    ValidateOnly = true,
    ViolationSampleLimit = 100,
    Constraints = spec,
});

if (preview.Succeeded)
{
    await client.MigrateForeignKeysAsync(new ForeignKeyMigrationRequest
    {
        BackupDestinationPath = "pre-fk.backup.db",
        Constraints = spec,
    });
}
```

The same request/response contract flows through the direct, HTTP, and gRPC transports.

## Implementation Notes

- The direct client depends on `CSharpDB.Engine`, `CSharpDB.Sql`, and `CSharpDB.Storage.Diagnostics`.
- `CSharpDB.Client` does not reference `CSharpDB.Data`.
- The HTTP transport runs against `CSharpDB.Api` and now covers the same public `ICSharpDbClient` surface as the direct client.
- The gRPC transport uses generated protobuf RPC methods, not a generic JSON tunnel.
- Dynamic values such as row cells, procedure args, and collection documents are carried through a recursive protobuf value contract that preserves blobs and nested objects.
- The direct transport talks to the engine in-process, the HTTP transport uses JSON endpoints, and the gRPC transport uses the dedicated daemon host.
- Internal tables such as `__procedures`, `__saved_queries`, and collection backing tables are hidden from normal table listing.

## Dependency Injection

```csharp
services.AddCSharpDbClient(new CSharpDbClientOptions
{
    DataSource = "csharpdb.db"
});
```

or

```csharp
services.AddCSharpDbClient(sp => new CSharpDbClientOptions
{
    ConnectionString = "Data Source=csharpdb.db"
});
```

## Design Rule

New database-facing functionality should be added here first.

Host-specific concerns should not create a second authoritative API beside `CSharpDB.Client`.
