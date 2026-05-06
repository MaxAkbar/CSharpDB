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
  <a href="https://www.nuget.org/packages/CSharpDB"><img src="https://img.shields.io/nuget/dt/CSharpDB?label=Downloads" alt="NuGet downloads"></a>
  <a href="https://github.com/MaxAkbar/CSharpDB/actions/workflows/ci.yml"><img src="https://github.com/MaxAkbar/CSharpDB/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/MaxAkbar/CSharpDB/stargazers"><img src="https://img.shields.io/github/stars/MaxAkbar/CSharpDB?label=Stars" alt="GitHub stars"></a>
  <a href="https://dotnet.microsoft.com/download/dotnet/10.0"><img src="https://img.shields.io/badge/.NET-10-512bd4" alt=".NET 10"></a>
  <a href="https://github.com/MaxAkbar/CSharpDB"><img src="https://img.shields.io/badge/Platform-Windows%20%7C%20Linux%20%7C%20macOS-0f766e" alt="Platform: Windows, Linux, macOS"></a>
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

| 1.60M gets/sec | 9.97M COUNTs/sec | 618.81K rows/sec | 891 commits/sec |
|:-:|:-:|:-:|:-:|
| Collection point reads | Concurrent reader burst (8x reused) | Durable `InsertBatch` B10000 | Concurrent durable writes |

<sub>Intel i9-11900K, 16 logical cores, Windows 10.0.26300, .NET SDK 10.0.203, .NET runtime 10.0.7. Snapshot promoted from the April 26, 2026 release-core suite; latest release guardrail compare passed May 6, 2026 with PASS=187, WARN=0, SKIP=0, FAIL=0. Full results live in the <a href="tests/CSharpDB.Benchmarks/README.md">benchmark suite</a>.</sub>

---

## Durable API Top Lines

Default CSharpDB file-backed benchmarks are fully durable: WAL fsync-on-commit unless a row explicitly says otherwise. In-memory rows show the same API paths without disk durability.

| Surface | Single write | Batch x100 | Point read | Concurrent read |
|---|---:|---:|---:|---:|
| SQL file-backed | 450.4 ops/sec | 41.88K rows/sec | 1.27M ops/sec | 9.97M COUNTs/sec |
| SQL hybrid incremental-durable | 449.3 ops/sec | 41.77K rows/sec | 1.29M ops/sec | 10.27M COUNTs/sec |
| SQL in-memory | 194.98K ops/sec | 708.75K rows/sec | 1.24M ops/sec | 10.09M COUNTs/sec |
| Collection file-backed | 447.3 ops/sec | 42.28K docs/sec | 1.60M ops/sec | - |
| Collection hybrid incremental-durable | 450.8 ops/sec | 42.34K docs/sec | 1.66M ops/sec | - |
| Collection in-memory | 205.25K ops/sec | 872.89K docs/sec | 1.59M ops/sec | - |

<sub>Source: `master-table-20260426-215529-median-of-3.csv` from the April 26, 2026 release-core snapshot. Full methodology and storage-mode detail live in the <a href="tests/CSharpDB.Benchmarks/README.md">benchmark suite README</a>.</sub>

---

## Concurrent Durable Writes

The current release-core concurrent write rows measure the intended shared-insert shape: one shared `Database`, disjoint explicit key ranges, auto-commit SQL inserts, and `ConcurrentWriteTransactions` enabled. The numbers below are total durable commits/sec across all writers combined.

| Workload | Writers | Commit window | Durable Commits/sec | Commits/flush | Notes |
|----------|--------:|--------------:|--------------------:|--------------:|-------|
| Shared auto-commit `INSERT` | 4 | `0` | 231.1 | 1.00 | One durable flush per commit |
| Shared auto-commit `INSERT` | 4 | `250us` | 449.6 | 1.99 | Group commit roughly doubles throughput |
| Shared auto-commit `INSERT` | 8 | `0` | 240.5 | 1.00 | Still flush-bound with no commit window |
| Shared auto-commit `INSERT` | 8 | `250us` | 891.0 | 3.92 | Current release-core headline row |

<sub>Source: `concurrent-write-diagnostics-20260426-223659-median-of-3.csv` from the April 26, 2026 release-core run. A later focused insert fan-in validation reaches 1,456.6 commits/sec for hot auto-ID inserts and 1,464.6 commits/sec for hot explicit right-edge inserts with `W8 + 250us`; those rows remain diagnostic until the next release-core promotion. Full methodology and tuning notes live in the <a href="tests/CSharpDB.Benchmarks/README.md">benchmark suite README</a>.</sub>

---

## Local SQLite Reference

Same-runner SQLite rows use Microsoft.Data.Sqlite 10.0.7 with WAL + `synchronous=FULL`. They are comparison points, not universal claims.

| Workload | CSharpDB | SQLite WAL+FULL |
|---|---:|---:|
| Durable prepared bulk insert B1000 | 233.06K rows/sec | 203.30K rows/sec |
| SQL point lookup | 1.27M ops/sec | 70.21K ops/sec |

<sub>Source: `sqlite-compare-20260426-230045-median-of-3.csv` from the April 26, 2026 release-core snapshot.</sub>

---

## Generated Collection Fast Path

The source-generated collection path is opt-in through `GetGeneratedCollectionAsync<T>(...)`. It mainly improves collection payload CPU, direct field extraction, and index-reader paths; one-row durable writes can still be WAL-flush-bound.

| Path | Source-gen JSON | Generated binary | Gain | Allocation |
|---|---:|---:|---:|---|
| Encode payload | 600.1 ns | 306.2 ns | 1.96x | 552 B to 136 B |
| Decode payload | 2,277.9 ns | 371.9 ns | 6.12x | 1,240 B to 480 B |
| Indexed int field read | 187.23 ns | 29.74 ns | 6.30x | 0 B to 0 B |
| Text field UTF-8 read | 185.82 ns | 27.26 ns | 6.82x | 56 B to 0 B |

<sub>Source: `BenchmarkDotNet.Artifacts/results/CSharpDB.Benchmarks.Micro.GeneratedCollection*Benchmarks-report.csv`. These rows are diagnostic microbenchmarks, not release-core scorecard rows.</sub>

---

## Quick Start

```bash
dotnet add package CSharpDB
```

```csharp
using CSharpDB.Engine;

// If the file exists, delete it to start fresh
if (File.Exists("mydata.db"))
    File.Delete("mydata.db");

await using var db = await Database.OpenAsync("mydata.db");

await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
await db.ExecuteAsync("INSERT INTO users VALUES (1, 'Alice', 30)");

await using var result = await db.ExecuteAsync("SELECT * FROM users WHERE age > 26");
await foreach (var row in result.GetRowsAsync())
    Console.WriteLine($"{row[0].AsInteger}: {row[1].AsText}, age {row[2].AsInteger}");
```

---

## Why CSharpDB?

- **No moving parts** — single `.db` file, no server process, no native binaries, no external dependencies
- **SQL + NoSQL in one engine** — full SQL with JOINs, CTEs, subqueries, views, and triggers *plus* a typed `Collection<T>` API that bypasses SQL entirely for sub-microsecond reads
- **ACID by default** — WAL-based crash recovery with fsync-on-commit and concurrent snapshot-isolated readers
- **Ships with tooling** — Admin UI, VS Code extension, CLI REPL, REST API, gRPC daemon, pipeline tooling, integrated forms and reports designers, and MCP server for AI agents
- **Use from any language** — NativeAOT compiles to a standalone C library; call from Python, Node.js, Go, Rust, Swift, Kotlin, Dart, Android, and iOS

---

## Admin UI

| Querying | Table Data | Schema |
|:-:|:-:|:-:|
| ![Query tab](docs/images/QuerySytemTable.png) | ![Data browser](docs/images/TableDetails.png) | ![Schema view](docs/images/TableSchema.png) |

Blazor Server dashboard with query execution, visual [Query Designer](https://csharpdb.com/docs/admin-ui.html#query-editor), data browser CRUD, schema editing, stored procedures, visual pipeline design, integrated forms and reports designers, backup and maintenance flows, and storage diagnostics.

---

## Ecosystem

CSharpDB is more than an embedded SQL engine. The same database can be used through in-process APIs, remote service hosts, AI tooling, visual designers, and cross-language bindings.

| Surface | Primary use | Highlights |
|---|---|---|
| **Engine API** | Embedded in-process access | Direct async SQL, transactions, views, triggers, procedures, and query stats |
| **Collection API** | Typed document and key-value access | `Collection<T>`, nested path indexes, point reads, scans, and path/range queries |
| **ADO.NET Provider** | Standard .NET data access | `DbConnection`, `DbCommand`, `DbDataReader`, and `DbTransaction` support |
| **Client SDK** | One C# API across transports | Direct, HTTP, and gRPC transports plus maintenance and diagnostics |
| **REST API** | HTTP integration and automation | 30+ endpoints with OpenAPI and Scalar for SQL, schema, data, collections, and maintenance |
| **gRPC Daemon** | Long-running remote host | Strongly typed RPC surface for SQL, schema, procedures, collections, and maintenance |
| **CLI REPL** | Terminal-first workflows | Interactive SQL shell, schema inspection, backup/restore, and migration commands |
| **MCP Server** | AI assistant integration | Tool-based schema inspection, query execution, and row operations for MCP-compatible clients |
| **Admin UI** | Browser-based database studio | Query editor, visual query designer, CRUD, schema editing, procedures, and storage diagnostics |
| **Forms + Reports** | Internal app workflows and printable output | Database-backed forms designer/runtime plus banded reports with grouping, expressions, preview, and print |
| **Pipelines** | ETL and automation | Package-based runtime, visual pipeline designer, transforms, dry-run, checkpoints, and run history |
| **VS Code Extension** | IDE integration | Schema explorer, `.csql` support, query results, CRUD, and storage diagnostics |
| **Native FFI** | Polyglot embedding | NativeAOT C library for Python, Go, Rust, Swift, Kotlin, Dart, Android, and iOS |
| **Node.js Package** | JavaScript and TypeScript access | Local embedded wrapper over the native library for Node.js apps and tooling |

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

The native library exports 20 C functions. See the [Native Library Reference](https://csharpdb.com/docs/tutorials/native-ffi.html) for Go, Rust, Swift, Kotlin, Dart, Android, and iOS examples.

---

## How CSharpDB Compares

| Feature | CSharpDB | SQLite | LiteDB | RocksDB |
|---------|:--------:|:------:|:------:|:-------:|
| Pure .NET / no native binaries | ✅ | ❌ | ✅ | ❌ |
| Full SQL (JOINs, CTEs, subqueries) | ✅ | ✅ | ❌ | ❌ |
| NoSQL Collection API | ✅ | ❌ | ✅ | ❌ |
| ACID transactions | ✅ | ✅ | ✅ | ✅ |
| REST API / gRPC | ✅ | ❌ | ❌ | ❌ |
| Admin UI | ✅ | ❌ | ❌ | ❌ |
| MCP server (AI agents) | ✅ | ❌ | ❌ | ❌ |
| VS Code extension | ✅ | ❌ | ❌ | ❌ |
| Multi-language SDKs | ✅ | ✅ | ❌ | ✅ |
| Mature ecosystem / battle-tested | ❌ | ✅ | ✅ | ✅ |

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
  [Pager + WAL]              (page cache, write-ahead log)
      |
  [File I/O]                 (4 KB pages, slotted layout)
      |
  mydata.db + mydata.db.wal
```

---

## Documentation

| | |
|---|---|
| [Getting Started](https://csharpdb.com/docs/getting-started.html) | Step-by-step walkthrough |
| [Architecture Guide](https://csharpdb.com/architecture.html) | Engine design deep dive |
| [Tools & Ecosystem](https://csharpdb.com/docs/ecosystem.html) | APIs, hosts, designers, and integrations |
| [EF Core Provider](https://csharpdb.com/docs/entity-framework-core.html) | Embedded EF Core 10 provider guide |
| [Trusted C# Callbacks](docs/trusted-csharp-functions/README.md) | Register in-process C# functions, commands, and validation rules for SQL, forms, reports, and pipelines |
| [Trusted C# Host Sample](samples/trusted-csharp-host/README.md) | VS Code-ready C# host project for trusted functions, commands, validation rules, and form actions |
| [Admin UI Guide](https://csharpdb.com/docs/admin-ui.html) | Querying, schema, pipelines, forms, reports, and storage |
| [CSharpDB.Client](src/CSharpDB.Client/README.md) | Unified client API and transports |
| [Pipelines](https://csharpdb.com/docs/pipelines.html) | ETL package model and visual designer |
| [Reports](https://csharpdb.com/docs/reports.html) | Visual banded report designer and preview |
| [Native FFI](https://csharpdb.com/docs/tutorials/native-ffi.html) | C library API and cross-language examples |
| [REST API Reference](https://csharpdb.com/docs/rest-api.html) | HTTP API, schema/data CRUD, and maintenance |
| [MCP Server](https://csharpdb.com/docs/mcp-server.html) | AI assistant integration |
| [CLI Reference](https://csharpdb.com/docs/cli.html) | REPL commands |
| [VS Code Extension](vscode-extension/README.md) | Local NativeAOT-backed extension |
| [Benchmark Suite](tests/CSharpDB.Benchmarks/README.md) | Full results and comparisons |
| [CSharpDB and SharpCoreDB](docs/comparisons/sharpcoredb.md) | Evidence-based comparison and fit guidance |
| [SQL Reference](https://csharpdb.com/docs/sql.html) | Supported SQL syntax |
| [Internals & Contributing](https://csharpdb.com/docs/internals.html) | Project structure and concurrency model |
| [FAQ](https://csharpdb.com/docs/faq.html) | Common questions |
| [Roadmap](https://csharpdb.com/roadmap.html) | Project goals |

---

## License

[MIT](LICENSE)
