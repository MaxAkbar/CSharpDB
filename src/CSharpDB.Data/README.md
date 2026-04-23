# CSharpDB.Data

ADO.NET provider for the [CSharpDB](https://github.com/MaxAkbar/CSharpDB) embedded database engine. Standard `DbConnection`, `DbCommand`, and `DbDataReader` with parameterized queries and transactions.

[![NuGet](https://img.shields.io/nuget/v/CSharpDB.Data)](https://www.nuget.org/packages/CSharpDB.Data)
[![.NET 10](https://img.shields.io/badge/.NET-10-512bd4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Release](https://img.shields.io/github/v/release/MaxAkbar/CSharpDB?display_name=tag&label=Release)](https://github.com/MaxAkbar/CSharpDB/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE)

## Overview

`CSharpDB.Data` provides a standard `System.Data.Common` (ADO.NET) data provider for CSharpDB. Use familiar `DbConnection`/`DbCommand`/`DbDataReader` patterns to query and modify your database. Supports parameterized queries, transactions, prepared statements, prepared-template caching, schema introspection, embedded file-backed and in-memory connection modes, and daemon-backed remote connections over the `CSharpDB.Daemon` gRPC host.

## Key Types

| Type | Description |
|------|-------------|
| `CSharpDbConnection` | `DbConnection` for file-backed databases, private `:memory:` databases, named shared `:memory:name` databases, and daemon-backed remote connections |
| `CSharpDbCommand` | `DbCommand` with prepared statement support, template caching, and parameter binding |
| `CSharpDbDataReader` | `DbDataReader` with async iteration, typed getters, and `HasRows` |
| `CSharpDbTransaction` | `DbTransaction` with auto-rollback on dispose |
| `CSharpDbFactory` | Singleton `DbProviderFactory` for creating connections and commands |
| `CSharpDbParameter` | Parameter support with `AddWithValue` convenience method |

## Usage

```csharp
using CSharpDB.Data;

// Open a connection
await using var connection = new CSharpDbConnection("Data Source=myapp.db");
await connection.OpenAsync();

// Create a table
await using var cmd = connection.CreateCommand();
cmd.CommandText = """
    CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        name TEXT,
        price REAL
    )
    """;
await cmd.ExecuteNonQueryAsync();

// Insert with parameters
cmd.CommandText = "INSERT INTO products VALUES (@id, @name, @price)";
cmd.Parameters.AddWithValue("@id", 1);
cmd.Parameters.AddWithValue("@name", "Widget");
cmd.Parameters.AddWithValue("@price", 9.99);
await cmd.ExecuteNonQueryAsync();

// Query with a data reader
cmd.CommandText = "SELECT name, price FROM products WHERE price < @max";
cmd.Parameters.Clear();
cmd.Parameters.AddWithValue("@max", 50.0);

await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    Console.WriteLine($"{reader.GetString(0)}: ${reader.GetDouble(1):F2}");
}

// Transactions
await using var tx = await connection.BeginTransactionAsync();
cmd.Transaction = (CSharpDbTransaction)tx;
cmd.CommandText = "INSERT INTO products VALUES (2, 'Gadget', 19.99)";
await cmd.ExecuteNonQueryAsync();
await tx.CommitAsync();

// Save an in-memory connection back to disk
await connection.SaveToFileAsync("products.db");

// Schema introspection
var csConn = (CSharpDbConnection)connection;
var tables = csConn.GetTableNames();
var schema = csConn.GetTableSchema("products");
```

### Using DbProviderFactory

```csharp
var factory = CSharpDbFactory.Instance;
await using var conn = factory.CreateConnection();
conn.ConnectionString = "Data Source=myapp.db";
await conn.OpenAsync();
```

## Daemon-Backed Connections

Use a remote transport connection string when you want ADO.NET to talk to a running `CSharpDB.Daemon` host instead of opening the database file directly:

```csharp
await using var connection = new CSharpDbConnection(
    "Transport=Grpc;Endpoint=http://localhost:5000");
await connection.OpenAsync();

await using var command = connection.CreateCommand();
command.CommandText = "SELECT COUNT(*) FROM products";
var count = (long)(await command.ExecuteScalarAsync() ?? 0L);
```

Connection-string rules:

- Embedded/direct mode: use `Data Source=...`
- Daemon-backed mode: use `Transport=Grpc;Endpoint=http://...`
- `Data Source`, `Load From`, and connection pooling are embedded-only options
- `Endpoint` requires an explicit `Transport`

For a daemon host, `Grpc` is the primary transport today:

```text
Transport=Grpc;Endpoint=http://localhost:5000
```

That gives you standard ADO.NET commands and transactions against the daemon-managed database while keeping the existing embedded connection-string shape unchanged.

Transport guidance:

- `Direct` with `Data Source=...` is still faster than any network hop when the caller can open the database locally
- `Grpc` is the fastest supported network transport in the current CSharpDB stack
- `NamedPipes` is not implemented end to end today, so do not target it yet for daemon-backed ADO.NET
- for daemon-backed workloads, reuse open connections instead of reconnecting for each command

## In-Memory Connection Strings

```text
Data Source=:memory:
```

Creates a private in-memory database scoped to a single connection.

```text
Data Source=:memory:shared-cache
```

Creates or attaches to a named shared in-memory database within the current process.

```text
Data Source=:memory:shared-cache;Load From=seed.db
```

Seeds an in-memory database from `seed.db` on first open. For named shared memory, later opens must either omit `Load From` or use the same source path.

Named shared in-memory connections allow multiple live connections at once. One connection may own an explicit transaction at a time; other connections can still run reads against the last committed snapshot while that transaction is active.

## Embedded Storage Tuning

You can now push engine-level embedded tuning through the ADO.NET surface
without dropping down to `Database.OpenAsync(...)`.

Direct engine options:

```csharp
using CSharpDB.Data;
using CSharpDB.Engine;

var directOptions = new DatabaseOptions()
    .ConfigureStorageEngine(builder => builder.UseWriteOptimizedPreset());

await using var connection = new CSharpDbConnection("Data Source=ingest.db", directOptions);
await connection.OpenAsync();
```

Hybrid embedded mode:

```csharp
await using var connection = new CSharpDbConnection("Data Source=app.db")
{
    DirectDatabaseOptions = new DatabaseOptions()
        .ConfigureStorageEngine(builder => builder.UseDirectLookupOptimizedPreset()),
    HybridDatabaseOptions = new HybridDatabaseOptions
    {
        PersistenceMode = HybridPersistenceMode.IncrementalDurable,
    },
};

await connection.OpenAsync();
```

Convenience connection-string keywords:

```text
Data Source=app.db;Storage Preset=WriteOptimized
Data Source=app.db;Storage Preset=DirectColdFileLookup;Embedded Open Mode=Direct
Data Source=app.db;Storage Preset=WriteOptimized;Embedded Open Mode=HybridIncrementalDurable
```

Supported `Storage Preset` values:

- `DirectLookupOptimized`
- `DirectColdFileLookup`
- `HybridFileCache`
- `WriteOptimized`
- `LowLatencyDurableWrite`

Supported `Embedded Open Mode` values:

- `Direct`
- `HybridIncrementalDurable`
- `HybridSnapshot`

Configuration rules:

- explicit `DirectDatabaseOptions` override `Storage Preset`
- explicit `HybridDatabaseOptions` override `Embedded Open Mode`
- connection-string keywords fill gaps only when the corresponding explicit
  options object is absent
- remote transports and named shared-memory databases reject embedded tuning
- private `:memory:` supports direct tuning, but not hybrid open modes

Pooling note:

- file-backed pooling is now options-aware
- distinct explicit options object instances do not share a pool in v1
- `ClearPool(connectionString)` clears all pooled entries for the normalized
  file-backed target

## Connection Pooling (Opt-In)

Connection pooling is disabled by default. Enable it explicitly in the connection string:

```bash
Data Source=myapp.db;Pooling=true;Max Pool Size=16
```

To force-release pooled physical connections (for example before deleting database files):

```csharp
CSharpDbConnection.ClearPool("Data Source=myapp.db;Pooling=true;Max Pool Size=16");
CSharpDbConnection.ClearAllPools();
```

`ClearPool` and `ClearAllPools` also clear named shared in-memory hosts.

## Installation

```
dotnet add package CSharpDB.Data
```

For the recommended all-in-one package:

```
dotnet add package CSharpDB
```

## Dependencies

- `CSharpDB.Engine` - embedded database engine

## Related Packages

| Package | Description |
|---------|-------------|
| [CSharpDB](https://www.nuget.org/packages/CSharpDB) | All-in-one package for application development |
| [CSharpDB.Engine](https://www.nuget.org/packages/CSharpDB.Engine) | Underlying embedded database engine |
| [CSharpDB.Client](https://www.nuget.org/packages/CSharpDB.Client) | Authoritative client SDK for direct and daemon-backed database access |
| [CSharpDB.EntityFrameworkCore](https://www.nuget.org/packages/CSharpDB.EntityFrameworkCore) | EF Core provider built on top of `CSharpDB.Data` |

## License

MIT - see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE) for details.
