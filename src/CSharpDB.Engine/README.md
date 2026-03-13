# CSharpDB.Engine

Lightweight embedded SQL database engine for .NET with single-file storage, WAL durability, concurrent readers, and a typed `Collection<T>` NoSQL API.

[![NuGet](https://img.shields.io/nuget/v/CSharpDB.Engine)](https://www.nuget.org/packages/CSharpDB.Engine)
[![.NET 10](https://img.shields.io/badge/.NET-10-512bd4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Release](https://img.shields.io/github/v/release/MaxAkbar/CSharpDB?display_name=tag&label=Release)](https://github.com/MaxAkbar/CSharpDB/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE)

## Overview

`CSharpDB.Engine` is the main entry point for embedding CSharpDB in your .NET application. It combines the SQL parser, query planner, and B+tree storage engine into a single `Database` class with two access paths: a full SQL engine and a zero-SQL `Collection<T>` document API. You can run against a normal on-disk database file or open the engine fully in memory and explicitly save/load snapshots when needed.

## Features

- **SQL engine**: DDL, DML, JOINs, aggregates, GROUP BY, HAVING, CTEs, `UNION` / `INTERSECT` / `EXCEPT`, scalar subqueries, `IN (SELECT ...)`, `EXISTS (SELECT ...)`, views, triggers, indexes, `ANALYZE`, and `sys.*` catalogs including `sys.table_stats` and `sys.column_stats`
- **NoSQL Collection API**: Typed `Collection<T>` with `Put`/`Get`/`Delete`/`Scan`/`Find`
- **Single-file storage**: All data in one `.db` file with 4 KB B+tree pages
- **In-memory mode**: Open empty in memory, load an existing `.db` + `.wal` into memory, then save back to disk
- **WAL durability**: Write-ahead log with crash recovery
- **Concurrent readers**: Snapshot-isolated readers alongside a single writer
- **Statement + plan caching**: bounded caches for parsed SQL statements and SELECT plan reuse
- **Fast-path lookups**: Direct B+tree access for `SELECT ... WHERE pk = value`
- **Persisted statistics**: Exact row counts maintained on write, `ANALYZE`-refreshed column distinct/min/max stats, stale tracking after writes, and reuse of fresh stats for `COUNT(*)` and selective lookup planning
- **Async-first**: All APIs are `async`/`await` from top to bottom

Current boundary:
- Correlated subqueries are supported in `WHERE`, non-aggregate projection expressions, and `UPDATE` / `DELETE` expressions.
- Correlated subqueries in `JOIN ON`, `GROUP BY`, `HAVING`, `ORDER BY`, and aggregate projections remain unsupported.
- `UNION ALL` remains planned.

## Usage

### SQL API

```csharp
using CSharpDB.Engine;

// Open or create a database
await using var db = await Database.OpenAsync("myapp.db");

// Create a table
await db.ExecuteAsync("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT,
        email TEXT
    )
    """);

// Insert data
await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')");

// Query data
var result = await db.ExecuteAsync("SELECT name, email FROM users WHERE id = 1");
while (await result.MoveNextAsync())
{
    Console.WriteLine($"{result.Current[0].AsText} - {result.Current[1].AsText}");
}

// Transactions
await db.BeginTransactionAsync();
await db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')");
await db.CommitAsync();
```

### In-Memory Open, Load, and Save

```csharp
using CSharpDB.Engine;

// Start with an empty in-memory database
await using var db = await Database.OpenInMemoryAsync();
await db.ExecuteAsync("CREATE TABLE cache (id INTEGER PRIMARY KEY, value TEXT)");
await db.ExecuteAsync("INSERT INTO cache VALUES (1, 'hot data')");

// Persist the current committed state to disk
await db.SaveToFileAsync("cache.db");

// Load an existing on-disk database into memory, including committed WAL state
await using var imported = await Database.LoadIntoMemoryAsync("cache.db");
```

### NoSQL Collection API

```csharp
// Get a typed collection
var users = await db.GetCollectionAsync<User>("users");

// Put a document
await users.PutAsync("alice", new User { Name = "Alice", Age = 30 });

// Get a document
var alice = await users.GetAsync("alice");

// Scan all documents
await foreach (var user in users.ScanAsync())
{
    Console.WriteLine(user.Name);
}

// Find with predicate
var adults = await users.FindAsync(u => u.Age >= 18);
```

### Concurrent Readers

```csharp
// Create a snapshot-isolated reader session
using var reader = db.CreateReaderSession();
var result = await reader.ExecuteReadAsync("SELECT * FROM users");
// Reads from a consistent snapshot while the writer continues
```

Reuse the same `ReaderSession` for a burst of related reads when possible. The current file-backed tuning benchmarks show that reusing a snapshot is materially cheaper than creating a new reader session for every single query.

## Installation

```
dotnet add package CSharpDB.Engine
```

For the recommended all-in-one package:

```
dotnet add package CSharpDB
```

## Dependencies

- `CSharpDB.Primitives` - shared type system
- `CSharpDB.Sql` - SQL parser
- `CSharpDB.Storage` - B+tree storage engine
- `CSharpDB.Execution` - query planner and operators

## Related Packages

| Package | Description |
|---------|-------------|
| [CSharpDB.Data](https://www.nuget.org/packages/CSharpDB.Data) | ADO.NET provider built on this engine |
| [CSharpDB.Client](https://www.nuget.org/packages/CSharpDB.Client) | Authoritative client SDK over direct and remote transports |
| [CSharpDB.Storage.Diagnostics](https://www.nuget.org/packages/CSharpDB.Storage.Diagnostics) | Storage inspection and integrity checking |

## License

MIT - see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE) for details.
