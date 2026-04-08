# CSharpDB

A zero-dependency embedded database engine for .NET with full SQL support, a typed NoSQL Collection API, built-in full-text search, ETL pipelines, and an ADO.NET provider — all in a single NuGet package.

[![NuGet](https://img.shields.io/nuget/v/CSharpDB)](https://www.nuget.org/packages/CSharpDB)
[![.NET 10](https://img.shields.io/badge/.NET-10-512bd4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Release](https://img.shields.io/github/v/release/MaxAkbar/CSharpDB?display_name=tag&label=Release)](https://github.com/MaxAkbar/CSharpDB/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE)

## Installation

```bash
dotnet add package CSharpDB
```

## Quick Start

```csharp
await using var db = await Database.OpenAsync("myapp.db");

// SQL
await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");
await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')");
await using var result = await db.ExecuteAsync("SELECT * FROM users WHERE name = 'Alice'");

// NoSQL Collections
var customers = await db.GetCollectionAsync<Customer>("customers");
await customers.PutAsync("c1", new Customer("Alice", "alice@example.com"));
var alice = await customers.GetAsync("c1");

// Full-Text Search
await db.EnsureFullTextIndexAsync("fts_users", "users", ["name", "email"]);
var hits = await db.SearchAsync("fts_users", "alice");
```

## Features

### Database Modes

CSharpDB supports three storage modes to fit different use cases:

```csharp
// File-based — persistent storage with WAL
await using var db = await Database.OpenAsync("app.db");

// In-memory — fast, ephemeral, ideal for testing
await using var db = await Database.OpenInMemoryAsync();

// Hybrid — lazy-resident with file-backed persistence
await using var db = await Database.OpenHybridAsync("app.db");
```

| Mode | Persistence | Use Case |
|------|------------|----------|
| File-based | Full WAL + checkpoint | Production workloads |
| In-memory | None | Unit tests, caches, temp data |
| Hybrid | Lazy-resident with backing file | Large datasets with selective caching |

### SQL Support

A full SQL engine with DDL, DML, JOINs, aggregates, CTEs, subqueries, views, triggers, and stored procedures.

**DDL**
```csharp
await db.ExecuteAsync("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, total REAL)");
await db.ExecuteAsync("CREATE UNIQUE INDEX idx_email ON users (email)");
await db.ExecuteAsync("CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1");
await db.ExecuteAsync("ALTER TABLE users ADD COLUMN active INTEGER NOT NULL DEFAULT 1");
```

**DML & Queries**
```csharp
await db.ExecuteAsync("INSERT INTO orders VALUES (1, 1, 99.50)");
await db.ExecuteAsync("UPDATE orders SET total = 109.50 WHERE id = 1");
await db.ExecuteAsync("DELETE FROM orders WHERE id = 1");

// JOINs, aggregates, CTEs, subqueries
await using var result = await db.ExecuteAsync(@"
    WITH top_customers AS (
        SELECT customer_id, SUM(total) AS spend
        FROM orders
        GROUP BY customer_id
        HAVING spend > 500
    )
    SELECT u.name, tc.spend
    FROM users u
    INNER JOIN top_customers tc ON u.id = tc.customer_id
    ORDER BY tc.spend DESC
    LIMIT 10");
```

**Supported SQL Features**
- Column types: `INTEGER`, `REAL`, `TEXT`, `BLOB`
- Constraints: `PRIMARY KEY`, `IDENTITY`, `NOT NULL`, `UNIQUE`
- JOINs: `INNER`, `LEFT OUTER`, `RIGHT OUTER`, `CROSS`
- Set operations: `UNION`, `INTERSECT`, `EXCEPT`
- Expressions: `LIKE`, `IN`, `BETWEEN`, `IS NULL`, `EXISTS`
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- Pagination: `LIMIT` / `OFFSET`
- Common Table Expressions (`WITH` clause)
- Subqueries (scalar, `IN`, `EXISTS`)
- Views, Triggers, Stored Procedures
- `ANALYZE` for statistics collection

### Collection API (NoSQL)

A typed document API that serializes C# objects to JSON and stores them in B+trees with secondary indexing.

```csharp
public record Customer(string Name, string Email, Address Address, string[] Tags);
public record Address(string City, string State, string Zip);

var customers = await db.GetCollectionAsync<Customer>("customers");

// CRUD
await customers.PutAsync("c1", new Customer("Alice", "alice@example.com",
    new("Portland", "OR", "97201"), ["premium"]));
var alice = await customers.GetAsync("c1");
await customers.DeleteAsync("c1");
var count = await customers.CountAsync();

// Secondary indexes on any field path
await customers.EnsureIndexAsync(c => c.Email);
var results = await customers.FindByIndexAsync(c => c.Email, "alice@example.com");

// Nested path indexes
await customers.EnsureIndexAsync(c => c.Address.City);
var portlanders = await customers.FindByIndexAsync(c => c.Address.City, "Portland");
```

### Full-Text Search

Built-in full-text search with Unicode-aware tokenization, automatic index maintenance on INSERT/UPDATE/DELETE, and term-intersection queries — all stored in the same database file with zero external dependencies.

```csharp
// Create a table and index
await db.ExecuteAsync("CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT, body TEXT)");
await db.ExecuteAsync("INSERT INTO articles VALUES (1, 'Getting Started', 'Learn to build fast apps')");

await db.EnsureFullTextIndexAsync("fts_articles", "articles", ["title", "body"]);

// Search (AND semantics — all terms must match)
var hits = await db.SearchAsync("fts_articles", "fast apps");

foreach (var hit in hits)
    Console.WriteLine($"Row {hit.RowId}, Score {hit.Score:F2}");

// Index options
await db.EnsureFullTextIndexAsync("fts_articles", "articles", ["title", "body"],
    new FullTextIndexOptions
    {
        Normalization = NormalizationForm.FormKC,
        LowercaseInvariant = true,
        StorePositions = true,
    });

// Drop with standard SQL
await db.ExecuteAsync("DROP INDEX fts_articles");
```

### Transactions

ACID transactions with WAL-based durability and single-writer concurrency.

```csharp
await db.BeginTransactionAsync();
try
{
    await db.ExecuteAsync("INSERT INTO orders VALUES (1, 1, 50.00)");
    await db.ExecuteAsync("UPDATE users SET balance = balance - 50 WHERE id = 1");
    await db.CommitAsync();
}
catch
{
    await db.RollbackAsync();
    throw;
}
```

### ETL Pipelines

Built-in pipeline runtime with CSV/JSON connectors, transforms, validation, checkpoint/resume, and run history.

```csharp
var orchestrator = new PipelineOrchestrator(
    new DefaultPipelineComponentFactory(),
    NullPipelineCheckpointStore.Instance,
    NullPipelineRunLogger.Instance);

await orchestrator.ExecuteAsync(new PipelineRunRequest
{
    Package = new PipelinePackageDefinition
    {
        Name = "import-customers",
        Source = new PipelineSourceDefinition
        {
            Kind = PipelineSourceKind.CsvFile,
            Path = "customers.csv",
            HasHeaderRow = true,
        },
        Destination = new PipelineDestinationDefinition
        {
            Kind = PipelineDestinationKind.JsonFile,
            Path = "output.json",
        },
        Options = new PipelineExecutionOptions
        {
            ErrorMode = PipelineErrorMode.SkipBadRows,
            MaxRejects = 100,
        },
    },
    Mode = PipelineExecutionMode.Run,
});
```

### ADO.NET Provider

Standard `System.Data.Common` implementation for compatibility with ORMs, Dapper, and existing data access code.

```csharp
await using var connection = new CSharpDbConnection("Data Source=myapp.db");
await connection.OpenAsync();

await using var command = connection.CreateCommand();
command.CommandText = "SELECT name, email FROM users WHERE id = @id";
command.Parameters.Add(new CSharpDbParameter("@id", 1));

await using var reader = await command.ExecuteReaderAsync();
while (await reader.ReadAsync())
    Console.WriteLine($"{reader.GetString(0)} — {reader.GetString(1)}");
```

### Indexes

Multiple index types for optimal query performance:

| Index Type | Created Via | Use Case |
|-----------|------------|----------|
| B+tree (SQL) | `CREATE INDEX` | WHERE, JOIN, ORDER BY optimization |
| Unique | `CREATE UNIQUE INDEX` | Enforce uniqueness + fast lookup |
| Collection | `EnsureIndexAsync()` | Document field equality queries |
| Full-Text | `EnsureFullTextIndexAsync()` | Term-based text search |

### Maintenance & Diagnostics

```csharp
// WAL checkpoint
await db.CheckpointAsync();

// Export / backup
await db.SaveToFileAsync("backup.db");

// Storage inspection (via Client SDK)
var report = await client.InspectStorageAsync("myapp.db");
var walReport = await client.CheckWalAsync("myapp.db");
var indexReport = await client.CheckIndexesAsync("myapp.db");

// Vacuum
await client.VacuumAsync();
```

## Ecosystem

CSharpDB ships with a full set of tools beyond the core library:

| Tool | Package | Description |
|------|---------|-------------|
| CLI Shell | `CSharpDB.Cli` | Interactive REPL with SQL execution and meta-commands |
| REST API | `CSharpDB.Api` | HTTP endpoints with OpenAPI documentation |
| gRPC Server | `CSharpDB.Daemon` | High-performance RPC service |
| MCP Server | `CSharpDB.Mcp` | Model Context Protocol integration for AI tools |
| Admin UI | `CSharpDB.Admin` | Web-based database administration |

## Architecture

```
┌──────────────────────────────────────────────────┐
│                  CSharpDB (meta-package)         │
├──────────────────────────────────────────────────┤
│  Client SDK  │  ADO.NET Provider  │  Diagnostics │
├──────────────────────────────────────────────────┤
│                 Engine (Database API)            │
│   SQL · Collections · Full-Text Search · Pipes   │
├──────────────────────────────────────────────────┤
│          Execution (Query Planner & Ops)         │
├──────────────────────────────────────────────────┤
│       SQL Parser (Lexer · Parser · AST)          │
├──────────────────────────────────────────────────┤
│              Storage Engine                      │
│   B+Tree · Pager · WAL · Checkpointing · Index   │
├──────────────────────────────────────────────────┤
│           Primitives (DbType · DbValue · Schema) │
└──────────────────────────────────────────────────┘
```

## Client Transports

The Client SDK supports multiple transport modes for different deployment scenarios:

```csharp
// Direct (in-process)
var client = CSharpDbClient.Create(new CSharpDbClientOptions
{
    Transport = CSharpDbTransport.Direct,
    DataSource = "myapp.db",
});

// HTTP (REST API)
var client = CSharpDbClient.Create(new CSharpDbClientOptions
{
    Transport = CSharpDbTransport.Http,
    Endpoint = "https://localhost:5001",
});

// gRPC
var client = CSharpDbClient.Create(new CSharpDbClientOptions
{
    Transport = CSharpDbTransport.Grpc,
    Endpoint = "https://localhost:5002",
});
```

## Included Packages

The `CSharpDB` meta-package pulls in the full library set:

| Package | Purpose |
|---------|---------|
| `CSharpDB.Primitives` | Core types — `DbType`, `DbValue`, `TableSchema`, `IndexSchema` |
| `CSharpDB.Sql` | SQL lexer, parser, and AST |
| `CSharpDB.Storage` | B+tree, pager, WAL, checkpointing, serialization |
| `CSharpDB.Execution` | Query planner, operator pipeline |
| `CSharpDB.Engine` | Top-level `Database` API — SQL, Collections, FTS, Pipelines |
| `CSharpDB.Client` | Unified client SDK (Direct, HTTP, gRPC, Named Pipes) |
| `CSharpDB.Data` | ADO.NET provider (`DbConnection`, `DbCommand`, `DbDataReader`) |
| `CSharpDB.Storage.Diagnostics` | Storage inspection, WAL analysis, index validation |

## License

MIT — see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE).
