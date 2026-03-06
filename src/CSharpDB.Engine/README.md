# CSharpDB.Engine

Lightweight embedded SQL database engine for .NET with single-file storage, WAL durability, concurrent readers, and a typed `Collection<T>` NoSQL API.

[![NuGet](https://img.shields.io/nuget/v/CSharpDB.Engine)](https://www.nuget.org/packages/CSharpDB.Engine)
[![.NET 10](https://img.shields.io/badge/.NET-10-512bd4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Release](https://img.shields.io/github/v/release/MaxAkbar/CSharpDB?display_name=tag&label=Release)](https://github.com/MaxAkbar/CSharpDB/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE)

## Overview

`CSharpDB.Engine` is the main entry point for embedding CSharpDB in your .NET application. It combines the SQL parser, query planner, and B+tree storage engine into a single `Database` class with two access paths: a full SQL engine and a zero-SQL `Collection<T>` document API. No external dependencies, no server process, just a single `.db` file on disk.

## Features

- **SQL engine**: DDL, DML, JOINs, aggregates, GROUP BY, HAVING, CTEs, views, triggers, indexes
- **NoSQL Collection API**: Typed `Collection<T>` with `Put`/`Get`/`Delete`/`Scan`/`Find`
- **Single-file storage**: All data in one `.db` file with 4 KB B+tree pages
- **WAL durability**: Write-ahead log with crash recovery
- **Concurrent readers**: Snapshot-isolated readers alongside a single writer
- **Statement cache**: LRU cache (512 capacity) for parsed and planned queries
- **Fast-path lookups**: Direct B+tree access for `SELECT ... WHERE pk = value`
- **Async-first**: All APIs are `async`/`await` from top to bottom

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
var result = await reader.ExecuteAsync("SELECT * FROM users");
// Reads from a consistent snapshot while the writer continues
```

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
| [CSharpDB.Service](https://www.nuget.org/packages/CSharpDB.Service) | Thread-safe service layer for web apps |
| [CSharpDB.Storage.Diagnostics](https://www.nuget.org/packages/CSharpDB.Storage.Diagnostics) | Storage inspection and integrity checking |

## License

MIT - see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE) for details.
