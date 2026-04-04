<p align="center">
  <img src="docs/images/icon3.png" alt="CSharpDB" width="120">
</p>

<h1 align="center">CSharpDB</h1>

<p align="center">
  <strong>The embedded database engine built for .NET</strong><br>
  Zero dependencies. Full SQL. ACID storage. Single file. One NuGet package.
</p>

<p align="center">
  <a href="https://www.nuget.org/packages/CSharpDB"><img src="https://img.shields.io/nuget/v/CSharpDB" alt="NuGet"></a>
  <a href="https://dotnet.microsoft.com/download/dotnet/10.0"><img src="https://img.shields.io/badge/.NET-10-512bd4" alt=".NET 10"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>
  <a href="https://github.com/MaxAkbar/CSharpDB/releases/latest"><img src="https://img.shields.io/github/v/release/MaxAkbar/CSharpDB?display_name=tag&label=Release" alt="Release"></a>
</p>

<p align="center">
  <a href="https://csharpdb.com/getting-started.html">Getting Started</a> &middot;
  <a href="https://csharpdb.com/docs/index.html">Docs</a> &middot;
  <a href="https://csharpdb.com/benchmarks.html">Benchmarks</a> &middot;
  <a href="https://csharpdb.com/roadmap.html">Roadmap</a> &middot;
  <a href="https://csharpdb.com">Website</a>
</p>

---

## Performance at a Glance

| 1.99M gets/sec | 9.47M reads/sec | 587K inserts/sec | 238.7 ns |
|:-:|:-:|:-:|:-:|
| Collection point reads | Concurrent snapshot readers (8x) | Batched SQL inserts | ADO.NET ExecuteScalar |

<sub>Intel i9-11900K, .NET 10, Windows 11. Full results in the <a href="tests/CSharpDB.Benchmarks/README.md">benchmark suite</a>.</sub>

---

## Quick Start

```bash
dotnet add package CSharpDB
```

```csharp
using CSharpDB.Engine;

await using var db = await Database.OpenAsync("mydata.db");

await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice', 30)");

await using var result = await db.ExecuteAsync("SELECT * FROM users WHERE age > 26");
await foreach (var row in result.GetRowsAsync())
    Console.WriteLine($"{row[0].AsInteger}: {row[1].AsText}, age {row[2].AsInteger}");
```

---

## Why CSharpDB?

- **No moving parts** -- single `.db` file, no server process, no native binaries, no external dependencies
- **SQL + NoSQL in one engine** -- full SQL with JOINs, CTEs, subqueries, views, and triggers *plus* a typed `Collection<T>` API that bypasses SQL entirely for sub-microsecond reads
- **ACID by default** -- WAL-based crash recovery with fsync-on-commit and concurrent snapshot-isolated readers
- **Ships with tooling** -- Admin UI, VS Code extension, CLI REPL, REST API, gRPC daemon, and MCP server for AI agents
- **Use from any language** -- NativeAOT compiles to a standalone C library; call from Python, Node.js, Go, Rust, Swift, Kotlin, Dart, Android, and iOS

---

## Admin UI

| Querying | Table Data | Schema |
|:-:|:-:|:-:|
| ![Query tab](docs/images/QuerySytemTable.png) | ![Data browser](docs/images/TableDetails.png) | ![Schema view](docs/images/TableSchema.png) |

Blazor Server dashboard with query execution, visual [Query Designer](https://csharpdb.com/docs/admin-ui.html#query-editor), data browser CRUD, schema editing, integrated forms and reports designers, stored procedures, and storage diagnostics.

---

## Ecosystem

| | | | |
|:-:|:-:|:-:|:-:|
| **Engine API** | **Collection API** | **ADO.NET Provider** | **Client SDK** |
| Direct async SQL | Typed NoSQL key-value | Standard DbConnection | Unified API with pluggable transports |
| **REST API** | **gRPC Daemon** | **CLI REPL** | **MCP Server** |
| 33 HTTP endpoints | Remote binary protocol | Interactive shell | AI assistant integration |
| **VS Code Extension** | **Native FFI** | **Admin UI** | **Node.js Package** |
| Schema + query + CRUD | NativeAOT C library | Blazor dashboard | TypeScript/JavaScript |

---

## Use from Any Language

**Node.js:**
```javascript
import { Database } from 'csharpdb';

const db = new Database('mydata.db');
db.execute("INSERT INTO demo VALUES (1, 'Alice')");
for (const row of db.query('SELECT * FROM demo')) console.log(row);
db.close();
```

**Python:**
```python
from csharpdb import Database

with Database("mydata.db") as db:
    db.execute("INSERT INTO demo VALUES (1, 'Alice')")
    for row in db.query("SELECT * FROM demo"):
        print(row)
```

The native library exports 20 C functions. See the [Native Library Reference](src/CSharpDB.Native/README.md) for Go, Rust, Swift, Kotlin, Dart, Android, and iOS examples.

---

## How CSharpDB Compares

| Feature | CSharpDB | SQLite | LiteDB | RocksDB |
|---------|:--------:|:------:|:------:|:-------:|
| Pure .NET / no native binaries | :white_check_mark: | -- | :white_check_mark: | -- |
| Full SQL (JOINs, CTEs, subqueries) | :white_check_mark: | :white_check_mark: | -- | -- |
| NoSQL Collection API | :white_check_mark: | -- | :white_check_mark: | -- |
| ACID transactions | :white_check_mark: | :white_check_mark: | :white_check_mark: | :white_check_mark: |
| REST API / gRPC | :white_check_mark: | -- | -- | -- |
| Admin UI | :white_check_mark: | -- | -- | -- |
| MCP server (AI agents) | :white_check_mark: | -- | -- | -- |
| VS Code extension | :white_check_mark: | -- | -- | -- |
| Multi-language SDKs | :white_check_mark: | :white_check_mark: | -- | :white_check_mark: |
| Mature ecosystem / battle-tested | -- | :white_check_mark: | :white_check_mark: | :white_check_mark: |

---

## Architecture

```
  SQL string              Collection<T> API
      |                        |
  [Tokenizer]            [JSON serialize]
      |                        |
  [Parser -> AST]         (bypassed)
      |                        |
  [Query Planner]              |
      |                        |
  [Operator Tree]              |
      |                        |
  [B+Tree]  ---------------  [B+Tree]
      |
  [Pager + WAL]        (page cache, write-ahead log)
      |
  [File I/O]           (4 KB pages, slotted layout)
      |
  mydata.db + mydata.db.wal
```

---

## Documentation

| | |
|---|---|
| [Getting Started](docs/getting-started.md) | Step-by-step walkthrough |
| [Architecture Guide](docs/architecture.md) | Engine design deep dive |
| [CSharpDB.Client](src/CSharpDB.Client/README.md) | Unified client API and transports |
| [Native FFI](src/CSharpDB.Native/README.md) | C library API and cross-language examples |
| [REST API Reference](docs/rest-api.md) | All 33 endpoints |
| [MCP Server](docs/mcp-server.md) | AI assistant integration |
| [CLI Reference](docs/cli.md) | REPL commands |
| [VS Code Extension](vscode-extension/README.md) | Local NativeAOT-backed extension |
| [Benchmark Suite](tests/CSharpDB.Benchmarks/README.md) | Full results and comparisons |
| [SQL Reference](https://csharpdb.com/docs/sql.html) | Supported SQL syntax |
| [Internals & Contributing](docs/internals.md) | Project structure and concurrency model |
| [FAQ](docs/faq.md) | Common questions |
| [Roadmap](docs/roadmap.md) | Project goals |

---

## License

[MIT](LICENSE)
