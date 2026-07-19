<p align="center">
  <img src="docs/images/icon3.png" alt="CSharpDB" width="120">
</p>

<h1 align="center">CSharpDB</h1>

<p align="center">
  <strong>The embedded database engine built for .NET</strong><br>
  Zero dependencies. CSharpDB SQL. ACID storage. Single file. One NuGet package.
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

| 1.99M gets/sec | 9.68M COUNTs/sec | 799.29K rows/sec | 890 commits/sec |
|:-:|:-:|:-:|:-:|
| Collection point reads | Concurrent reader burst (8x reused) | Durable `InsertBatch` B10000 | Concurrent durable writes |

<sub>Intel i9-11900K, 16 logical cores, Windows 10.0.26300, .NET SDK 10.0.203, .NET runtime 10.0.7. Snapshot promoted from the May 6, 2026 release-core suite; latest release guardrail compare passed May 6, 2026 with PASS=187, WARN=0, SKIP=0, FAIL=0. Full results live in the <a href="tests/CSharpDB.Benchmarks/README.md">benchmark suite</a>.</sub>

---

## Durable API Top Lines

Default CSharpDB file-backed benchmarks are fully durable: WAL fsync-on-commit unless a row explicitly says otherwise. In-memory rows show the same API paths without disk durability.

| Surface | Single write | Batch x100 | Point read | Concurrent read |
|---|---:|---:|---:|---:|
| SQL file-backed | 267.1 ops/sec | 25.56K rows/sec | 1.48M ops/sec | 9.68M COUNTs/sec |
| SQL hybrid incremental-durable | 276.1 ops/sec | 26.55K rows/sec | 1.47M ops/sec | 10.04M COUNTs/sec |
| SQL in-memory | 259.48K ops/sec | 934.22K rows/sec | 1.49M ops/sec | 10.26M COUNTs/sec |
| Collection file-backed | 265.7 ops/sec | 24.53K docs/sec | 1.99M ops/sec | - |
| Collection hybrid incremental-durable | 276.9 ops/sec | 25.75K docs/sec | 2.02M ops/sec | - |
| Collection in-memory | 262.14K ops/sec | 969.55K docs/sec | 2.02M ops/sec | - |

<sub>Source: `master-table-20260506-024609-median-of-3.csv` from the May 6, 2026 release-core snapshot. Full methodology and storage-mode detail live in the <a href="tests/CSharpDB.Benchmarks/README.md">benchmark suite README</a>.</sub>

---

## Concurrent Durable Writes

The current release-core concurrent write rows measure the intended shared-insert shape: one shared `Database`, disjoint explicit key ranges, auto-commit SQL inserts, and `ConcurrentWriteTransactions` enabled. The numbers below are total durable commits/sec across all writers combined.

| Workload | Writers | Commit window | Durable Commits/sec | Commits/flush | Notes |
|----------|--------:|--------------:|--------------------:|--------------:|-------|
| Shared auto-commit `INSERT` | 4 | `0` | 247.0 | 1.00 | One durable flush per commit |
| Shared auto-commit `INSERT` | 4 | `250us` | 463.4 | 1.99 | Group commit roughly doubles throughput |
| Shared auto-commit `INSERT` | 8 | `0` | 239.2 | 1.00 | Still flush-bound with no commit window |
| Shared auto-commit `INSERT` | 8 | `250us` | 890.1 | 3.94 | Current release-core headline row |

Focused hot insert fan-in diagnostics cover the newer right-edge and auto-ID shapes that are not part of the release-core scorecard yet:

| Insert shape | Writers/window | Durable Commits/sec | Commits/flush |
|---|---:|---:|---:|
| Serialized explicit hot right-edge | `W8 + 250us` | 278.4 | 1.00 |
| Concurrent explicit hot right-edge | `W8 + 250us` | 910.3 | 3.33 |
| Concurrent auto-ID hot right-edge | `W8 + 250us` | 913.1 | 3.34 |
| Concurrent explicit disjoint ranges | `W8 + 250us` | 1,049.6 | 3.96 |

<sub>Sources: release-core `concurrent-write-diagnostics-20260506-032735-median-of-3.csv`; focused insert fan-in diagnostic `insert-fan-in-diagnostics-20260505-233424.csv`. Focused rows remain diagnostic until the release-core suite includes those shapes directly. Full methodology and tuning notes live in the <a href="tests/CSharpDB.Benchmarks/README.md">benchmark suite README</a>.</sub>

---

## Local SQLite Reference

Same-runner SQLite rows use Microsoft.Data.Sqlite 10.0.7 with WAL + `synchronous=FULL`. They are comparison points, not universal claims.

| Workload | CSharpDB | SQLite WAL+FULL |
|---|---:|---:|
| Durable prepared bulk insert B1000 | 211.99K rows/sec | 155.66K rows/sec |
| SQL point lookup | 1.48M ops/sec | 93.91K ops/sec |

<sub>Source: `sqlite-compare-20260506-035128-median-of-3.csv` from the May 6, 2026 release-core snapshot.</sub>

---

## Generated Collection Fast Path

The source-generated collection path is opt-in through `GetGeneratedCollectionAsync<T>(...)`. It mainly improves collection payload CPU, direct field extraction, and index-reader paths; one-row durable writes can still be WAL-flush-bound.

| Path | Source-gen JSON | Generated binary | Gain | Allocation |
|---|---:|---:|---:|---|
| Encode payload | 600.1 ns | 306.2 ns | 1.96x | 552 B to 136 B |
| Decode payload | 2,277.9 ns | 371.9 ns | 6.12x | 1,240 B to 480 B |
| Indexed int field read | 187.23 ns | 29.74 ns | 6.30x | 0 B to 0 B |
| Text field UTF-8 read | 185.82 ns | 27.26 ns | 6.82x | 56 B to 0 B |
| Key match | 21.48 ns | 19.91 ns | 1.08x | 0 B to 0 B |

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
- **SQL + NoSQL in one engine** — the documented CSharpDB SQL subset includes JOINs, CTEs, subqueries, views, and triggers, alongside a typed `Collection<T>` API that bypasses SQL entirely for sub-microsecond reads
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

## EF Core 10 Provider

CSharpDB includes a first-party embedded EF Core 10 provider for file-backed and private in-memory databases. Its qualified LINQ surface includes ordinary filtering, ordering, pagination, projections, selected string/temporal/math translations, bounded scalar numeric aggregates, bounded direct inner joins, and two deliberately constrained aggregate extensions:

- One explicit `Join` between direct entity roots over a nonnullable `int`, `long`, or `int`/`long`-backed enum key, with qualified scalar or entity result projection and post-join filtering, ordering, and pagination.
- Direct single-table `GroupBy` over mapped scalar or anonymous-type/`ValueTuple` composite keys, with optional pre-filtering, qualified bare numeric aggregates, basic `HAVING`, and ordering by directly projected keys or aggregates.
- `Where` (optional) → selection of one directly mapped nonnullable `int` column → `Distinct` → `Count`, `LongCount`, `Sum`, `Min`, or `Max`.

Distinct `Average`, nullable or non-`int` columns, value-converted columns, predicates after `Distinct`, and transformed or derived distinct shapes are intentionally rejected. In particular, nullable distinct `Count`/`LongCount` cannot preserve LINQ's rule that a distinct `null` is counted once because SQL `COUNT(DISTINCT ...)` ignores it.

Grouped keys may be direct mapped Boolean, integral, enum, default-`BINARY` string, or nullable values; Boolean columns must contain canonical provider-written `0`/`1` storage. Configured key converters and `double` keys are outside the qualified surface. Grouped projections may contain the direct key plus bare `Count`/`LongCount`, qualified non-distinct `Sum`/`Average`/`Min`/`Max`, and the same nonnullable-`int` distinct variants listed above. Transformed keys or results, group materialization, post-projection filters or projections, raw group transforms, nested or joined grouping, predicate aggregates, and unsupported aggregate types or value converters fail before command dispatch with provider diagnostics. See the [EF Core provider guide](https://csharpdb.com/docs/entity-framework-core.html#linq-translation) and [generated compatibility manifest](docs/ef-core-compatibility.md) for the complete boundary.

For direct joins, the inner source must be unfiltered; filtered inner roots, prior ordering or row limits, source shapes that remain projected or derived after EF normalization, nullable/text/decimal/transformed/composite keys, chained joins, `GroupJoin`, and outer/cross joins remain explicitly unsupported.

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

| Feature | CSharpDB | SQLite | LiteDB | RocksDB | Microsoft Access |
|---------|:--------:|:------:|:------:|:-------:|:----------------:|
| Pure .NET / no native binaries | ✅ | ❌ | ✅ | ❌ | ❌ |
| SQL JOINs, CTEs, and subqueries | ✅ | ✅ | ❌ | ❌ | Partial |
| NoSQL Collection API | ✅ | ❌ | ✅ | ❌ | ❌ |
| ACID transactions | ✅ | ✅ | ✅ | ✅ | ✅ |
| REST API / gRPC | ✅ | ❌ | ❌ | ❌ | ❌ |
| Admin UI | ✅ | ❌ | ❌ | ❌ | ✅ |
| MCP server (AI agents) | ✅ | ❌ | ❌ | ❌ | ❌ |
| VS Code extension | ✅ | ❌ | ❌ | ❌ | ❌ |
| Multi-language SDKs | ✅ | ✅ | ❌ | ✅ | Partial |
| Mature ecosystem / battle-tested | ❌ | ✅ | ✅ | ✅ | ✅ |

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
| [Trusted C# Callbacks](https://csharpdb.com/docs/trusted-csharp-functions.html) | Register in-process C# functions, commands, and validation rules for SQL, forms, reports, and pipelines |
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
| [SQL Reference](https://csharpdb.com/docs/sql.html) | Supported SQL syntax |
| [SQL Compatibility Matrix](https://csharpdb.com/docs/sql-compatibility.html) | Feature-level availability, limitations, roadmap state, and test evidence |
| [SQL Compatibility Roadmap](https://csharpdb.com/docs/sql-compatibility-roadmap.html) | The 11 staged SQL implementation and qualification milestones |
| [Internals & Contributing](https://csharpdb.com/docs/internals.html) | Project structure and concurrency model |
| [FAQ](https://csharpdb.com/docs/faq.html) | Common questions |
| [Roadmap](https://csharpdb.com/roadmap.html) | Project goals |

---

## License

[MIT](LICENSE)
