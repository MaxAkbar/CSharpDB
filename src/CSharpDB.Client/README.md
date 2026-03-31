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
