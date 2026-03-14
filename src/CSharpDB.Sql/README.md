# CSharpDB.Sql

SQL tokenizer, recursive-descent parser, and abstract syntax tree (AST) for the [CSharpDB](https://github.com/MaxAkbar/CSharpDB) embedded database engine.

[![NuGet](https://img.shields.io/nuget/v/CSharpDB.Sql)](https://www.nuget.org/packages/CSharpDB.Sql)
[![.NET 10](https://img.shields.io/badge/.NET-10-512bd4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Release](https://img.shields.io/github/v/release/MaxAkbar/CSharpDB?display_name=tag&label=Release)](https://github.com/MaxAkbar/CSharpDB/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE)

## Overview

`CSharpDB.Sql` provides a complete SQL front-end: a single-pass tokenizer, a recursive-descent parser, and a rich AST hierarchy. It transforms SQL text into strongly-typed statement and expression objects that downstream components (query planner, execution engine) consume. Zero external dependencies.

## Features

- **100+ token types** covering SQL keywords, literals, identifiers, `@parameters`, operators, and punctuation
- **Full DDL/DML parsing**: CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW, CREATE/DROP TRIGGER, `ANALYZE`
- **Identity syntax support**: `IDENTITY` and `AUTOINCREMENT` modifiers on `INTEGER PRIMARY KEY` columns
- **Rich query support**: SELECT with JOINs (INNER, LEFT, RIGHT, CROSS), WHERE, GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET, CTEs (`WITH`), set operations (`UNION`, `INTERSECT`, `EXCEPT`), and subquery syntax
- **Expression tree**: binary/unary operators, LIKE, IN, BETWEEN, IS NULL, aggregate functions (COUNT, SUM, AVG, MIN, MAX with DISTINCT), scalar subqueries, `IN (SELECT ...)`, and `EXISTS (...)`
- **Fast-path optimizers**: `TryParseSimpleSelect`, `TryParseSimplePrimaryKeyLookup`, and `TryParseSimpleInsert` detect common patterns and skip full AST construction

## Usage

```csharp
using CSharpDB.Sql;

// Parse a SQL statement
var statement = Parser.Parse("SELECT id, name FROM users WHERE age > 21");

// The result is a strongly-typed AST node
if (statement is SelectStatement select)
{
    Console.WriteLine($"Table: {select.From}");
    Console.WriteLine($"Columns: {select.Columns.Count}");
    Console.WriteLine($"Has WHERE: {select.Where != null}");
}

// Parse DDL
var ddl = Parser.Parse("""
    CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        total REAL
    )
    """);

// Fast-path detection for simple queries
string sql = "SELECT name FROM users WHERE id = 1";
if (Parser.TryParseSimplePrimaryKeyLookup(sql, out var lookup))
{
    // Skip full parsing for SELECT ... WHERE pk = value
}
```

## JOIN Detection and Multi-Table Queries

The parser builds a recursive `TableRef` tree from the `FROM` clause. Each JOIN keyword produces a `JoinTableRef` node whose `Left` and `Right` children are themselves `TableRef` nodes, so chained joins form a left-deep binary tree:

```
SELECT o.id, c.name, p.title
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
LEFT JOIN products p ON p.id = o.product_id
```

Parses into:

```
JoinTableRef (LEFT JOIN, ON p.id = o.product_id)
├── Left:  JoinTableRef (INNER JOIN, ON c.id = o.customer_id)
│   ├── Left:  SimpleTableRef("orders", alias: "o")
│   └── Right: SimpleTableRef("customers", alias: "c")
└── Right: SimpleTableRef("products", alias: "p")
```

Walking the tree to collect every table involved is a simple recursive visit:

```csharp
using CSharpDB.Sql;

var stmt = Parser.Parse("""
    SELECT o.id, c.name, p.title
    FROM orders o
    INNER JOIN customers c ON c.id = o.customer_id
    LEFT JOIN products p ON p.id = o.product_id
    """) as SelectStatement;

// Recursively collect all tables from the FROM tree
static IEnumerable<SimpleTableRef> CollectTables(TableRef tableRef) => tableRef switch
{
    SimpleTableRef simple => [simple],
    JoinTableRef join     => CollectTables(join.Left).Concat(CollectTables(join.Right)),
    _                     => [],
};

foreach (var table in CollectTables(stmt!.From))
    Console.WriteLine($"Table: {table.TableName}, Alias: {table.Alias}");

// Output:
//   Table: orders, Alias: o
//   Table: customers, Alias: c
//   Table: products, Alias: p
```

The execution layer uses the same recursive pattern — `BuildFromOperator(TableRef)` walks the tree, creates a scan operator for each `SimpleTableRef`, and combines them with join operators (hash join, index nested-loop join, or nested-loop fallback) at each `JoinTableRef` node. Column references like `o.id` are resolved through `QualifiedMappings` on the composite schema.

## AST Hierarchy

**Statements**: `CreateTableStatement`, `DropTableStatement`, `InsertStatement`, `SelectStatement`, `CompoundSelectStatement`, `UpdateStatement`, `DeleteStatement`, `AlterTableStatement`, `CreateIndexStatement`, `DropIndexStatement`, `CreateViewStatement`, `DropViewStatement`, `CreateTriggerStatement`, `DropTriggerStatement`, `AnalyzeStatement`, `WithStatement`

**Expressions**: `LiteralExpression`, `ParameterExpression`, `ColumnRefExpression`, `BinaryExpression`, `UnaryExpression`, `LikeExpression`, `InExpression`, `InSubqueryExpression`, `ScalarSubqueryExpression`, `ExistsExpression`, `BetweenExpression`, `IsNullExpression`, `FunctionCallExpression`

**Table References**: `SimpleTableRef`, `JoinTableRef` (Inner, LeftOuter, RightOuter, Cross)

## Installation

```
dotnet add package CSharpDB.Sql
```

For the recommended all-in-one package:

```
dotnet add package CSharpDB
```

## Dependencies

- `CSharpDB.Primitives` - shared type system and schema definitions

## Related Packages

| Package | Description |
|---------|-------------|
| [CSharpDB.Execution](https://www.nuget.org/packages/CSharpDB.Execution) | Query planner and operator tree that consumes this AST |
| [CSharpDB.Engine](https://www.nuget.org/packages/CSharpDB.Engine) | Embedded database engine |

## License

MIT - see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE) for details.
