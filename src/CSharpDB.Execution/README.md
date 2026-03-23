# CSharpDB.Execution

Query planner, operator tree, and expression evaluator for the [CSharpDB](https://github.com/MaxAkbar/CSharpDB) embedded database engine.

[![NuGet](https://img.shields.io/nuget/v/CSharpDB.Execution)](https://www.nuget.org/packages/CSharpDB.Execution)
[![.NET 10](https://img.shields.io/badge/.NET-10-512bd4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Release](https://img.shields.io/github/v/release/MaxAkbar/CSharpDB?display_name=tag&label=Release)](https://github.com/MaxAkbar/CSharpDB/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE)

## Overview

`CSharpDB.Execution` transforms parsed SQL (AST from `CSharpDB.Sql`) into executable query plans. It implements the classic iterator model with physical operators for scans, joins, aggregation, sorting, filtering, projection, compound queries, and materialized subquery execution. Expression evaluation is handled by both an interpreter (for one-off use) and a compiler (for hot-path evaluation with column binding at compile time).

## Key Components

### Query Planner
- Translates AST statements into physical operator trees
- Dispatches to type-specific handlers for all DDL and DML statements
- System catalog virtual tables: `sys.tables`, `sys.columns`, `sys.indexes`, `sys.views`, `sys.triggers`, `sys.objects`, `sys.saved_queries`, `sys.table_stats`, `sys.column_stats`
- Compound query execution for `UNION`, `INTERSECT`, and `EXCEPT`
- Subquery lowering for uncorrelated forms plus a correlated fallback path for supported contexts
- Compiled expression cache (up to 4096 entries) for repeated queries
- Trigger body caching with schema-sensitive invalidation
- Sync point-lookup fast path for `SELECT ... WHERE pk = value`
- Persisted row-count reuse for `COUNT(*)`, planner cardinality estimates, and fresh-column-stat-guided non-unique lookup selection

### Operator Tree (Iterator Model)
- `IOperator` interface: `OpenAsync`, `MoveNextAsync`, `Current`, `OutputSchema`
- `TableScanOperator` with pre-decode filtering, projection pushdown, and row count estimation
- Join operators, filter, sort, aggregate, limit/offset, projection, and more
- Internal optimization interfaces: `IPreDecodeFilterSupport`, `IProjectionPushdownTarget`, `IMaterializedRowsProvider`

### Expression Evaluation
- **`ExpressionEvaluator`** - Static interpreter for simple/infrequent evaluations
- **`ExpressionCompiler`** - Compiles expression ASTs into `Func<DbValue[], DbValue>` delegates with column indices bound at compile time, eliminating per-row schema lookups

### Query Result
- Wraps operator output (SELECT) or rows-affected counts (DML/DDL)
- Sync fast-path for point lookups via `FromSyncLookup`
- `ToListAsync` for materializing full result sets

## Usage

```csharp
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Storage.StorageEngine;
using CSharpDB.Sql;

var factory = new DefaultStorageEngineFactory();
var context = await factory.OpenAsync("sample.db", new StorageEngineOptions());
await using var pager = context.Pager;

var planner = new QueryPlanner(context.Pager, context.Catalog, context.RecordSerializer);

// Execution package is low-level: wrap writes in explicit Pager transactions.
await pager.BeginTransactionAsync();
try
{
    await using (await planner.ExecuteAsync(Parser.Parse(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")))
    { }

    await using (await planner.ExecuteAsync(Parser.Parse(
        "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 19)")))
    { }

    await pager.CommitAsync();
}
catch
{
    await pager.RollbackAsync();
    throw;
}

var statement = Parser.Parse("SELECT name, age FROM users WHERE age > 21");

// Plan and execute (typically called through CSharpDB.Engine)
await using var result = await planner.ExecuteAsync(statement);

// Iterate results
while (await result.MoveNextAsync())
{
    DbValue[] row = result.Current;
    Console.WriteLine($"{row[0].AsText}, {row[1].AsInteger}");
}
```

## Installation

```
dotnet add package CSharpDB.Execution
```

For the recommended all-in-one package:

```
dotnet add package CSharpDB
```

## Dependencies

- `CSharpDB.Primitives` - shared type system
- `CSharpDB.Sql` - SQL parser and AST
- `CSharpDB.Storage` - B+tree storage engine

## Related Packages

| Package | Description |
|---------|-------------|
| [CSharpDB.Engine](https://www.nuget.org/packages/CSharpDB.Engine) | Embedded database engine that wraps this execution layer |
| [CSharpDB.Sql](https://www.nuget.org/packages/CSharpDB.Sql) | SQL parser producing the AST this package consumes |
| [CSharpDB.Storage](https://www.nuget.org/packages/CSharpDB.Storage) | Storage layer for physical I/O |

## License

MIT - see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE) for details.
