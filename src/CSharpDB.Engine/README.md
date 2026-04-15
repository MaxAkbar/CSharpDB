# CSharpDB.Engine

Lightweight embedded SQL database engine for .NET with single-file storage, WAL durability, concurrent readers, and a typed `Collection<T>` NoSQL API.

[![NuGet](https://img.shields.io/nuget/v/CSharpDB.Engine)](https://www.nuget.org/packages/CSharpDB.Engine)
[![.NET 10](https://img.shields.io/badge/.NET-10-512bd4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Release](https://img.shields.io/github/v/release/MaxAkbar/CSharpDB?display_name=tag&label=Release)](https://github.com/MaxAkbar/CSharpDB/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE)

## Overview

`CSharpDB.Engine` is the main entry point for embedding CSharpDB in your .NET application. It combines the SQL parser, query planner, and B+tree storage engine into a single `Database` class with two access paths: a full SQL engine and a zero-SQL `Collection<T>` document API. You can run against a normal on-disk database file, open the engine fully in memory and explicitly save/load snapshots when needed, or use the hybrid lazy-resident mode that warms pages into memory on demand and keeps committed state durable on disk.

## Features

- **SQL engine**: DDL, DML, JOINs, aggregates, GROUP BY, HAVING, CTEs, `UNION` / `INTERSECT` / `EXCEPT`, scalar subqueries, `IN (SELECT ...)`, `EXISTS (SELECT ...)`, views, triggers, indexes, `ANALYZE`, and `sys.*` catalogs including `sys.table_stats` and `sys.column_stats`
- **NoSQL Collection API**: Typed `Collection<T>` with `Put`/`Get`/`Delete`/`Scan`/`Find`
- **Single-file storage**: All data in one `.db` file with 4 KB B+tree pages
- **In-memory mode**: Open empty in memory, load an existing `.db` + `.wal` into memory, then save back to disk
- **Hybrid mode**: Open lazily from disk, keep touched pages resident in process memory, and persist commits durably through the backing-file WAL
- **WAL durability**: Write-ahead log with crash recovery
- **Concurrent readers**: Snapshot-isolated readers alongside a single writer
- **Statement + plan caching**: bounded caches for parsed SQL statements and SELECT plan reuse
- **Fast-path lookups**: Direct B+tree access for `SELECT ... WHERE pk = value`
- **Persisted statistics**: Exact row counts maintained on write, explicit `row_count_is_exact` semantics in `sys.table_stats`, `ANALYZE`-refreshed column distinct/min/max plus internal histograms/heavy hitters/composite-prefix stats, stale tracking after writes, and reuse of fresh stats for `COUNT(*)`, selective lookup planning, join method choice, and bounded small-chain inner-join reordering
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

`BeginTransactionAsync()` / `CommitAsync()` is still the legacy single-owner
explicit transaction API. Use it when one caller owns the whole transaction on
that `Database` handle. If you want overlapping task-per-writer work on one
shared `Database`, use the concurrent writer patterns below instead of starting
one of these legacy explicit transactions per task.

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

### Hybrid Memory-Resident Mode

```csharp
using CSharpDB.Engine;

// Start with an empty in-memory database and create a table
await using var db = await Database.OpenInMemoryAsync();
await db.ExecuteAsync("CREATE TABLE cache (id INTEGER PRIMARY KEY, value TEXT)");

// Persist the current committed state to disk
await db.SaveToFileAsync("cache.db");

await using var cacheDb = await Database.OpenHybridAsync(
    "cache.db",
    new DatabaseOptions(),
    new HybridDatabaseOptions
    {
        PersistenceMode = HybridPersistenceMode.IncrementalDurable,
        HotTableNames = ["cache"]
    });

await cacheDb.ExecuteAsync("INSERT INTO cache VALUES (1, 'hot data')");
```

`OpenHybridAsync(...)` opens from the backing file lazily, keeps owned pages
resident in the pager cache after they are first touched, writes committed
changes durably through the backing-file WAL, and checkpoints those committed
pages into the base file over time.

Use `HotTableNames` and `HotCollectionNames` when a long-lived hybrid process
should preload selected read-mostly objects into the shared pager cache at
open. In v1:

- hot SQL tables warm the primary table B+tree plus SQL secondary indexes
- hot collections warm the backing `_col_...` table only
- hot-set warming is supported only for `IncrementalDurable`
- hot-set warming requires the default unbounded pager cache shape and is rejected for snapshot mode, bounded caches, and custom page-cache factories

If you want the older full-image export behavior, opt into snapshot mode:

```csharp
await using var snapshotHybrid = await Database.OpenHybridAsync(
    "cache.db",
    new DatabaseOptions(),
    new HybridDatabaseOptions
    {
        PersistenceMode = HybridPersistenceMode.Snapshot,
        PersistenceTriggers = HybridPersistenceTriggers.Dispose
    });
```

### NoSQL Collection API

```csharp
using CSharpDB.Engine;

await using var db = await Database.OpenAsync("myapp.db");

// Opens the existing "users" collection, or creates it if it doesn't exist yet.
var users = await db.GetCollectionAsync<User>("users");

// Put a document
await users.PutAsync("alice", new User
{
    Name = "Alice",
    Email = "alice@example.com",
    Age = 30
});

// Get a document
var alice = await users.GetAsync("alice");
if (alice is not null)
{
    Console.WriteLine($"{alice.Name} <{alice.Email}>");
}

// Scan all documents
await foreach (var entry in users.ScanAsync())
{
    Console.WriteLine($"{entry.Key}: {entry.Value.Name}");
}

// Find with predicate
await foreach (var entry in users.FindAsync(u => u.Age >= 18))
{
    Console.WriteLine($"Adult: {entry.Key} ({entry.Value.Name})");
}

public sealed class User
{
    public string Name { get; set; } = "";
    public string Email { get; set; } = "";
    public int Age { get; set; }
}
```

`GetCollectionAsync<T>("users")` is the create/open operation for collections. If
the collection does not exist yet, CSharpDB creates its backing storage
automatically the first time you call it.

### Generated Collection API

For trim-safe typed collections, add the `CSharpDB.Generators` package and pair
your document type with a `System.Text.Json` source-generated context:

```csharp
using System.Text.Json.Serialization;
using CSharpDB.Engine;

[CollectionModel(typeof(UserJsonContext))]
public sealed partial record User(string Email, int Age);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(User))]
internal sealed partial class UserJsonContext : JsonSerializerContext;

await using var db = await Database.OpenAsync("myapp.db");
var users = await db.GetGeneratedCollectionAsync<User>("users");

await users.PutAsync("alice", new User("alice@example.com", 30));
await users.EnsureIndexAsync(User.Collection.Email);

await foreach (var entry in users.FindByIndexAsync(User.Collection.Email, "alice@example.com"))
{
    Console.WriteLine(entry.Value.Email);
}
```

Generated collection descriptors follow CLR member names, including flattened
nested paths such as `User.Collection.Address_City` for `Address.City` and
array-element paths such as `User.Collection.Tags` for `Tags[]` or
`User.Collection.Orders_Sku` for `Orders[].Sku`. JSON payload names can differ
through `JsonPropertyName` without changing the public descriptor names.

`GetGeneratedCollectionAsync<T>(...)` requires a generated or manually
registered collection model and exposes only the descriptor-based collection
surface. That keeps the call path off the reflection-based collection APIs.

Generated collections also require existing collection indexes on that
collection to resolve through registered generated descriptors. If a collection
already has a reflection-only path index such as `Next.Name` that your
generated model does not expose, open it through `GetCollectionAsync<T>(...)`
or add generated descriptor coverage before switching to
`GetGeneratedCollectionAsync<T>(...)`.

### Concurrent Readers

```csharp
using CSharpDB.Engine;

await using var db = await Database.OpenAsync("myapp.db");
await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice')");

// Take a snapshot-isolated reader session.
using var reader = db.CreateReaderSession();

// Writer continues changing the live database after the snapshot is created.
await db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob')");

// The reader still sees the earlier snapshot.
await using (var snapshotResult = await reader.ExecuteReadAsync(
    "SELECT id, name FROM users ORDER BY id"))
{
    while (await snapshotResult.MoveNextAsync())
    {
        Console.WriteLine(
            $"{snapshotResult.Current[0].AsInteger}: {snapshotResult.Current[1].AsText}");
    }
}

// The main database sees the latest committed state.
await using var liveResult = await db.ExecuteAsync("SELECT COUNT(*) FROM users");
await liveResult.MoveNextAsync();
Console.WriteLine($"Live row count: {liveResult.Current[0].AsInteger}");
```

`ReaderSession` gives you a stable snapshot from the moment it is created, even
while the writer keeps committing changes. Dispose each `QueryResult` before
executing the next query on the same reader session.

Reuse the same `ReaderSession` for a burst of related reads when possible. The
current file-backed tuning benchmarks show that reusing a snapshot is
materially cheaper than creating a new reader session for every single query.

### Snapshot Reads Inside Write Transactions

When you need a long-lived explicit `WriteTransaction` but some analytical
reads inside it should not participate in logical read-conflict tracking, use
`ExecuteSnapshotReadAsync(...)` for those specific queries:

```csharp
await using var tx = await db.BeginWriteTransactionAsync();

await using (var snapshotRead = await tx.ExecuteSnapshotReadAsync(
    "SELECT COUNT(*) FROM orders WHERE status = 'Open'"))
{
    await snapshotRead.ToListAsync();
}

await tx.ExecuteAsync("UPDATE orders SET status = 'Processing' WHERE id = 42");
await tx.CommitAsync();
```

This is an opt-in weaker isolation path for the selected query only. It does
not disable normal conflict tracking for later writes, foreign-key checks, or
uniqueness validation inside the same transaction.

### Concurrent Writer Patterns

`Task.Run(...)` by itself does not opt you into multi-writer behavior. The write
API you call determines whether work can overlap on one shared `Database`.

- Shared auto-commit `UPDATE`, `DELETE`, and DDL statements already run through
  isolated write-transaction state internally.
- Shared auto-commit `INSERT` statements stay on the legacy serialized path by
  default.
- Multi-statement per-writer units should use `RunWriteTransactionAsync(...)`
  or `BeginWriteTransactionAsync(...)`.

#### Shared Auto-Commit Non-Insert Work

If each task is issuing its own auto-commit `UPDATE` / `DELETE` statements, you
can share one `Database` instance directly:

```csharp
using CSharpDB.Engine;

await using var db = await Database.OpenAsync("workers.db");

await db.ExecuteAsync("CREATE TABLE IF NOT EXISTS worker_stats (worker_id INTEGER PRIMARY KEY, commits INTEGER)");
await db.ExecuteAsync("INSERT INTO worker_stats VALUES (0, 0)");
await db.ExecuteAsync("INSERT INTO worker_stats VALUES (1, 0)");
await db.ExecuteAsync("INSERT INTO worker_stats VALUES (2, 0)");
await db.ExecuteAsync("INSERT INTO worker_stats VALUES (3, 0)");

Task[] workers = new Task[4];

for (int workerId = 0; workerId < workers.Length; workerId++)
{
    int localWorkerId = workerId;
    workers[workerId] = Task.Run(async () =>
    {
        for (int i = 0; i < 100; i++)
        {
            await db.ExecuteAsync(
                $"UPDATE worker_stats SET commits = commits + 1 WHERE worker_id = {localWorkerId}");
        }
    });
}

await Task.WhenAll(workers);
```

#### Shared Auto-Commit Inserts

If you want shared auto-commit `INSERT` statements to use isolated write
transactions, opt in through `ImplicitInsertExecutionMode`:

```csharp
using System.Threading;
using CSharpDB.Engine;

var options = new DatabaseOptions
{
    ImplicitInsertExecutionMode = ImplicitInsertExecutionMode.ConcurrentWriteTransactions,
};

await using var db = await Database.OpenAsync("ingest.db", options);
await db.ExecuteAsync("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, worker_id INTEGER, payload TEXT)");

int nextId = 0;
Task[] writers = new Task[4];

for (int writerId = 0; writerId < writers.Length; writerId++)
{
    int localWriterId = writerId;
    writers[writerId] = Task.Run(async () =>
    {
        for (int i = 0; i < 1_000; i++)
        {
            int id = Interlocked.Increment(ref nextId);
            await db.ExecuteAsync(
                $"INSERT INTO events (id, worker_id, payload) VALUES ({id}, {localWriterId}, 'queued')");
        }
    });
}

await Task.WhenAll(writers);
```

Keep the default `Serialized` mode for hot right-edge insert loops unless your
own benchmark shows that `ConcurrentWriteTransactions` helps your key pattern.

#### Explicit Multi-Statement Writers

If each task needs multiple writes to commit atomically, use
`RunWriteTransactionAsync(...)`. It creates an isolated `WriteTransaction` per
attempt and retries on transaction conflicts using the supplied
`WriteTransactionOptions`:

```csharp
using System.Threading;
using CSharpDB.Engine;

var options = new DatabaseOptions
{
    ImplicitInsertExecutionMode = ImplicitInsertExecutionMode.ConcurrentWriteTransactions,
};

await using var db = await Database.OpenAsync("orders.db", options);
await db.ExecuteAsync("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, worker_id INTEGER, status TEXT)");
await db.ExecuteAsync("CREATE TABLE IF NOT EXISTS worker_stats (worker_id INTEGER PRIMARY KEY, commits INTEGER)");
await db.ExecuteAsync("INSERT INTO worker_stats VALUES (0, 0)");
await db.ExecuteAsync("INSERT INTO worker_stats VALUES (1, 0)");
await db.ExecuteAsync("INSERT INTO worker_stats VALUES (2, 0)");
await db.ExecuteAsync("INSERT INTO worker_stats VALUES (3, 0)");

var txOptions = new WriteTransactionOptions
{
    MaxRetries = 10,
    InitialBackoff = TimeSpan.FromMilliseconds(0.25),
    MaxBackoff = TimeSpan.FromMilliseconds(20),
};

int nextId = 0;
Task[] writers = new Task[4];

for (int writerId = 0; writerId < writers.Length; writerId++)
{
    int localWriterId = writerId;
    writers[writerId] = Task.Run(async () =>
    {
        for (int batch = 0; batch < 250; batch++)
        {
            await db.RunWriteTransactionAsync(
                async (tx, ct) =>
                {
                    int id = Interlocked.Increment(ref nextId);
                    await tx.ExecuteAsync(
                        $"INSERT INTO orders (id, worker_id, status) VALUES ({id}, {localWriterId}, 'pending')",
                        ct);
                    await tx.ExecuteAsync(
                        $"UPDATE worker_stats SET commits = commits + 1 WHERE worker_id = {localWriterId}",
                        ct);
                },
                txOptions);
        }
    });
}

await Task.WhenAll(writers);
```

Use `BeginWriteTransactionAsync()` directly only when you need manual lifetime
control inside one task. Do not share one `WriteTransaction` across tasks.

## Thread Safety

The supported threading model for `Database` is:

- Auto-commit SQL `UPDATE`, `DELETE`, and DDL statements can be issued concurrently against the same `Database` and now run through isolated `WriteTransaction` commits internally.
- Shared auto-commit SQL `INSERT` statements use the legacy serialized path by default. Set `ImplicitInsertExecutionMode` to `ConcurrentWriteTransactions` if you want those inserts routed through isolated `WriteTransaction` commits instead.
- `Collection<T>` writes can be issued concurrently, but they still serialize behind the collection/database write gate today.
- Only one explicit transaction can be active per `Database`. Do not share one explicit transaction concurrently across multiple tasks.
- Use one `ReaderSession` per concurrent SQL reader when you want snapshot-isolated reads alongside writes.
- A single `ReaderSession` is not re-entrant and supports only one active query at a time.
- The collection API does not yet expose its own snapshot-reader abstraction. For repeatable concurrent read isolation during writes, prefer SQL reads through `ReaderSession`.

## Installation

```
dotnet add package CSharpDB.Engine
```

For generated collection models:

```
dotnet add package CSharpDB.Generators
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
