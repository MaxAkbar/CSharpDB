# CSharpDB

A lightweight, embedded SQL database engine written from scratch in C#. Single-file storage, WAL-based crash recovery, concurrent readers, async-first API, zero external dependencies.

[![.NET 10](https://img.shields.io/badge/.NET-10-512bd4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/release/MaxAkbar/CSharpDB?display_name=tag&label=Release)](https://github.com/MaxAkbar/CSharpDB/releases/latest)

---

## Why CSharpDB?

CSharpDB is a fully self-contained database engine that runs inside your .NET application — no server process, no external dependencies, just a single `.db` file on disk. It provides two data-access paths: a **full SQL engine** with JOINs, aggregates, views, triggers, and indexes, and a **NoSQL Collection API** that bypasses SQL entirely for sub-microsecond key-value reads.

**Use cases:**

- Embedded databases for desktop, CLI, or IoT applications
- Prototyping and local development without setting up a database server
- Educational reference for understanding database internals (storage engines, B+trees, WAL, query planning)
- Cross-language interoperability via the built-in REST API

## Features

| Category | Details |
|----------|---------|
| **Storage** | Single `.db` file, 4 KB page-oriented, B+tree-backed tables and indexes |
| **Durability** | Write-Ahead Log (WAL) with fsync-on-commit, automatic crash recovery |
| **Concurrency** | Single writer + concurrent snapshot-isolated readers via WAL-based MVCC |
| **SQL** | DDL, DML, JOINs, aggregates, GROUP BY, HAVING, CTEs, views, triggers, indexes, and `sys.*` catalog queries |
| **NoSQL** | Typed `Collection<T>` with Put/Get/Delete/Scan/Find — 1.44M reads/sec |
| **ADO.NET** | Standard `DbConnection`/`DbCommand`/`DbDataReader` provider |
| **REST API** | ASP.NET Core Minimal API with 34 endpoints, OpenAPI/Scalar UI |
| **MCP Server** | Model Context Protocol server — let AI assistants query and modify your database |
| **Admin UI** | Blazor Server dashboard for browsing tables, views, indexes, triggers |
| **CLI** | Interactive REPL with meta-commands, file execution, snapshot mode |
| **Dependencies** | Zero — pure .NET 10, nothing else |

## Admin UI Preview

See the product first, then dive into the API and internals:

| Querying Metadata | Table Data View | Table Schema View |
|---|---|---|
| ![Admin query tab showing system table query results](docs/images/QuerySytemTable.png) | ![Admin table details tab showing row data](docs/images/TableDetails.png) | ![Admin schema tab showing table structure](docs/images/TableSchema.png) |

## Quick Start

### Engine API

```csharp
using CSharpDB.Engine;

await using var db = await Database.OpenAsync("mydata.db");

await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice', 30)");
await db.ExecuteAsync("INSERT INTO users VALUES (2, 'Bob', 25)");

await using var result = await db.ExecuteAsync(
    "SELECT * FROM users WHERE age > 26 ORDER BY name ASC");

await foreach (var row in result.GetRowsAsync())
    Console.WriteLine($"{row[0].AsInteger}: {row[1].AsText}, age {row[2].AsInteger}");
// 1: Alice, age 30
```

### Collection API (NoSQL)

```csharp
var users = await db.GetCollectionAsync<User>("users");

await users.PutAsync("user:1", new User("Alice", 30, "alice@example.com"));
var alice = await users.GetAsync("user:1");

await foreach (var kvp in users.FindAsync(u => u.Age > 25))
    Console.WriteLine(kvp.Value.Name);

record User(string Name, int Age, string Email);
```

### ADO.NET Provider

```csharp
using CSharpDB.Data;

await using var conn = new CSharpDbConnection("Data Source=myapp.db");
await conn.OpenAsync();

using var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT * FROM users WHERE age > @age";
cmd.Parameters.AddWithValue("@age", 25);

await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
    Console.WriteLine($"{reader.GetInt64(0)}: {reader.GetString(1)}");
```

### REST API

```bash
# Start the API server
dotnet run --project src/CSharpDB.Api

# Create a table
curl -X POST http://localhost:61818/api/sql/execute \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"}'

# Insert a row
curl -X POST http://localhost:61818/api/tables/users/rows \
  -H "Content-Type: application/json" \
  -d '{"values": {"id": 1, "name": "Alice"}}'

# Browse rows
curl http://localhost:61818/api/tables/users/rows
```

### CLI

```bash
dotnet run --project src/CSharpDB.Cli -- mydata.db

csdb> CREATE TABLE demo (id INTEGER PRIMARY KEY, name TEXT);
csdb> INSERT INTO demo VALUES (1, 'Hello');
csdb> SELECT * FROM demo;
csdb> .tables
csdb> .quit
```

### MCP Server (AI Integration)

Connect AI assistants like Claude Desktop, Cursor, OpenAI Codex, LM Studio, or VS Code Copilot to your database:

```bash
dotnet run --project src/CSharpDB.Mcp -- --database mydata.db
```

Add to `claude_desktop_config.json` (or `.mcp.json` for Claude Code / Cursor):

```json
{
  "mcpServers": {
    "csharpdb": {
      "command": "dotnet",
      "args": ["run", "--project", "path/to/src/CSharpDB.Mcp", "--", "--database", "mydata.db"]
    }
  }
}
```

Or for OpenAI Codex CLI (`~/.codex/config.toml`):

```toml
[mcp_servers.csharpdb]
command = "dotnet"
args = ["run", "--project", "path/to/src/CSharpDB.Mcp", "--", "--database", "mydata.db"]
```

The MCP server exposes 15 tools for schema inspection, data browsing, row mutations, and SQL execution. See the [MCP Server Reference](docs/mcp-server.md) for the full tool list and configuration options for all supported clients.

## Architecture

```
  SQL string              Collection<T> API
      │                        │
  [Tokenizer]   ── Sql     [JSON serialize]  ── Engine
      │                        │
  [Parser → AST]          (bypassed)
      │                        │
  [Query Planner]  ── Execution   (bypassed)
      │                        │
  [Operator Tree]              │
      │                        │
  [B+Tree]  ───────────────  [B+Tree]  ── Storage
      │
  [Pager + WAL]        (page cache, dirty tracking, write-ahead log)
      │
  [File I/O]           (4 KB pages, slotted page layout)
      │
  mydata.db + mydata.db.wal
```

The SQL path goes through tokenizer → parser → planner → operators → B+tree. The Collection API goes directly to the B+tree, bypassing SQL entirely for maximum throughput.

## Project Structure

```
CSharpDB.slnx
├── src/
│   ├── CSharpDB.Core/        Core types: DbValue, Schema, ErrorCodes
│   ├── CSharpDB.Storage/     Pager, B+tree, WAL, file I/O
│   ├── CSharpDB.Sql/         Tokenizer, parser, AST
│   ├── CSharpDB.Execution/   Query planner, operators, expression evaluator
│   ├── CSharpDB.Engine/      Top-level Database API + Collection<T> (NoSQL)
│   ├── CSharpDB.Data/        ADO.NET provider (DbConnection, DbCommand, DbDataReader)
│   ├── CSharpDB.Cli/         Interactive REPL
│   ├── CSharpDB.Service/     Shared service layer for Admin and API
│   ├── CSharpDB.Admin/       Blazor Server admin dashboard
│   ├── CSharpDB.Api/         REST API (ASP.NET Core Minimal API)
│   └── CSharpDB.Mcp/         MCP server for AI assistant integration
├── tests/
│   ├── CSharpDB.Tests/       Engine unit + integration tests
│   ├── CSharpDB.Data.Tests/  ADO.NET provider tests
│   ├── CSharpDB.Cli.Tests/   CLI smoke tests
│   └── CSharpDB.Benchmarks/  Performance benchmarks (BenchmarkDotNet + custom)
├── samples/                   Sample SQL datasets
└── docs/                      Documentation
```

## Supported SQL

| Category | Syntax |
|----------|--------|
| **DDL** | `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE` (ADD/DROP/RENAME COLUMN, RENAME TO) |
| **DML** | `INSERT INTO ... VALUES`, `SELECT`, `UPDATE ... SET`, `DELETE FROM` |
| **Indexes** | `CREATE [UNIQUE] INDEX`, `DROP INDEX` |
| **Views** | `CREATE VIEW ... AS`, `DROP VIEW` |
| **Triggers** | `CREATE TRIGGER ... BEFORE/AFTER INSERT/UPDATE/DELETE ... BEGIN ... END` |
| **CTEs** | `WITH name AS (select) SELECT ...` |
| **JOINs** | `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `CROSS JOIN` |
| **Aggregates** | `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`, `SUM`, `AVG`, `MIN`, `MAX` |
| **Clauses** | `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET` |
| **Expressions** | `=`, `<>`, `<`, `>`, `AND`, `OR`, `NOT`, `LIKE`, `IN`, `BETWEEN`, `IS NULL` |
| **Types** | `INTEGER` (i64), `REAL` (f64), `TEXT` (UTF-8), `BLOB` (byte[]) |

### System Catalog Queries

Use SQL to inspect schema metadata:

```sql
SELECT * FROM sys.tables ORDER BY table_name;
SELECT * FROM sys.columns WHERE table_name = 'users' ORDER BY ordinal_position;
SELECT * FROM sys.indexes WHERE table_name = 'users';
SELECT * FROM sys.views;
SELECT * FROM sys.triggers;
```

Underscored aliases are also supported: `sys_tables`, `sys_columns`, `sys_indexes`, `sys_views`, `sys_triggers`.

## Building and Testing

Requires [.NET 10 SDK](https://dotnet.microsoft.com/download/dotnet/10.0).

```bash
dotnet build
dotnet run --project tests/CSharpDB.Tests/CSharpDB.Tests.csproj --
dotnet run --project tests/CSharpDB.Data.Tests/CSharpDB.Data.Tests.csproj --
dotnet run --project tests/CSharpDB.Cli.Tests/CSharpDB.Cli.Tests.csproj --
```

## Performance Highlights

Benchmarks run on Intel i9-11900K, .NET 10, Windows 11. Full results in [tests/CSharpDB.Benchmarks/README.md](tests/CSharpDB.Benchmarks/README.md).

| Metric | Result |
|--------|--------|
| Single INSERT (auto-commit, durable) | 27,842 ops/sec |
| Batched INSERT (100 rows/tx) | ~370K rows/sec |
| Point lookup by PK (1K rows) | 786,596 ops/sec |
| Collection `GetAsync` (10K docs) | 1,371,530 ops/sec |
| Concurrent readers (8 sessions) | 256,088 ops/sec |
| ADO.NET `ExecuteScalar` | 323 ns / 696 bytes |
| Crash recovery | 100% reliable (50/50 cycles), P50 = 11.5 ms |

## Samples

The [`samples/`](samples/) directory contains ready-to-run SQL scripts for three realistic scenarios:

- **[ecommerce-store.sql](samples/ecommerce-store.sql)** — Northwind Electronics (customers, products, orders, reviews)
- **[medical-clinic.sql](samples/medical-clinic.sql)** — Riverside Health Center (patients, doctors, appointments, billing)
- **[school-district.sql](samples/school-district.sql)** — Maplewood School District (students, courses, enrollments, attendance)

Each script creates 7 tables with sample data, indexes, views, and triggers. See the [samples README](samples/README.md) for execution instructions.

## Roadmap

See [docs/roadmap.md](docs/roadmap.md) for the full roadmap. Highlights:

**Near-term**
- `DISTINCT` keyword
- Composite (multi-column) indexes
- Index range scans (`<`, `>`, `BETWEEN`)
- Prepared statement cache

**Mid-term**
- Subqueries and `EXISTS`
- `UNION` / `INTERSECT` / `EXCEPT`
- Window functions (`ROW_NUMBER`, `RANK`)
- NuGet package publishing

**Long-term**
- Memory-mapped I/O (mmap) read path
- Full-text search
- JSON path querying for Collection API
- Secondary indexes for Collection API

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started Tutorial](docs/getting-started.md) | Step-by-step walkthrough from opening a database to transactions |
| [Architecture Guide](docs/architecture.md) | Layer-by-layer deep dive into the engine design |
| [Internals & Contributing](docs/internals.md) | How to extend the engine, testing strategy, project layout |
| [REST API Reference](docs/rest-api.md) | All 34 API endpoints with request/response examples |
| [MCP Server Reference](docs/mcp-server.md) | AI assistant integration via Model Context Protocol |
| [CLI Reference](docs/cli.md) | Interactive REPL commands and meta-commands |
| [Storage Inspector](docs/storage-inspector.md) | Read-only DB/WAL integrity diagnostics and page-level inspection |
| [FAQ](docs/faq.md) | Common setup, SQL, Admin UI, and troubleshooting questions |
| [Roadmap](docs/roadmap.md) | Near-term, mid-term, and long-term project goals |
| [Benchmark Suite](tests/CSharpDB.Benchmarks/README.md) | Full benchmark results and comparison with 11 other databases |
| [Sample Datasets](samples/README.md) | Ready-to-run SQL scripts for testing |

## License

[MIT](LICENSE)
