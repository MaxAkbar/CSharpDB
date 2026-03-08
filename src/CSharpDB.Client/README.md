# CSharpDB.Client

`CSharpDB.Client` is the authoritative database API for CSharpDB.

It owns the public client contract used to talk to a database, while transport and lower-level implementation details stay behind that boundary.

## Current Direction

- `CSharpDB.Client` is now the real implementation layer for database access.
- `CSharpDB.Service` is a compatibility facade over the client while the repo retires direct service usage.
- The current transport implementation is direct/local.
- `Http`, `Grpc`, `Tcp`, and `NamedPipes` are part of the public client transport model, but only `Direct` is implemented today.

## Current Transport Model

Create the client with `CSharpDbClientOptions`:

```csharp
var client = CSharpDbClient.Create(new CSharpDbClientOptions
{
    Transport = CSharpDbTransport.Direct,
    DataSource = "csharpdb.db"
});
```

The transport can be selected explicitly with `Transport`. If it is omitted, the client infers it from `Endpoint` and otherwise defaults to direct.

Direct resolution currently accepts:

- `Endpoint` as a file path
- `Endpoint` as `file://...`
- `DataSource`
- `ConnectionString` containing `Data Source=...`

Resolution rules:

- direct is the default when transport cannot be inferred from a network endpoint
- supplied direct inputs must resolve to the same target
- `http://` and `https://` infer `Http` unless `Transport = CSharpDbTransport.Grpc` is set explicitly
- `tcp://`, `pipe://`, and `npipe://` infer their corresponding future transport
- `Http`, `Grpc`, `Tcp`, and `NamedPipes` validate their endpoint shape and then fail with a not-implemented error for now
- `HttpClient` is reserved for future `Http` and `Grpc` transports

Example gRPC selection:

```csharp
var client = CSharpDbClient.Create(new CSharpDbClientOptions
{
    Transport = CSharpDbTransport.Grpc,
    Endpoint = "https://localhost:5001"
});
```

This currently fails with a transport-not-implemented error because the repo does not yet expose a gRPC server transport.

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
- checkpoint
- storage diagnostics

## Implementation Notes

- The direct client depends on `CSharpDB.Engine`, `CSharpDB.Sql`, and `CSharpDB.Storage.Diagnostics`.
- `CSharpDB.Client` does not reference `CSharpDB.Data`.
- Schema, data, procedure, and saved-query operations all run through direct engine access.
- Collection access and client-managed transaction sessions use direct engine instances.
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
