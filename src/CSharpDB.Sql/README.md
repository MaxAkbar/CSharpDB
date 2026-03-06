# CSharpDB.Sql

SQL tokenizer, recursive-descent parser, and abstract syntax tree (AST) for the [CSharpDB](https://github.com/MaxAkbar/CSharpDB) embedded database engine.

[![NuGet](https://img.shields.io/nuget/v/CSharpDB.Sql)](https://www.nuget.org/packages/CSharpDB.Sql)
[![.NET 10](https://img.shields.io/badge/.NET-10-512bd4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Release](https://img.shields.io/github/v/release/MaxAkbar/CSharpDB?display_name=tag&label=Release)](https://github.com/MaxAkbar/CSharpDB/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE)

## Overview

`CSharpDB.Sql` provides a complete SQL front-end: a single-pass tokenizer, a recursive-descent parser, and a rich AST hierarchy. It transforms SQL text into strongly-typed statement and expression objects that downstream components (query planner, execution engine) consume. Zero external dependencies.

## Features

- **108 token types** covering SQL keywords, literals, identifiers, `@parameters`, operators, and punctuation
- **Full DDL/DML parsing**: CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX, CREATE/DROP VIEW, CREATE/DROP TRIGGER
- **Rich query support**: SELECT with JOINs (INNER, LEFT, RIGHT, CROSS), WHERE, GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET, CTEs (WITH)
- **Expression tree**: binary/unary operators, LIKE, IN, BETWEEN, IS NULL, aggregate functions (COUNT, SUM, AVG, MIN, MAX with DISTINCT)
- **Fast-path optimizers**: `TryParseSimpleSelect` and `TryParseSimplePrimaryKeyLookup` detect common patterns and skip full AST construction

## Usage

```csharp
using CSharpDB.Sql;

// Parse a SQL statement
var statements = Parser.Parse("SELECT id, name FROM users WHERE age > 21");

// The result is a list of strongly-typed AST nodes
if (statements[0] is SelectStatement select)
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
if (Parser.TryParseSimplePrimaryKeyLookup(sql, out var lookup))
{
    // Skip full parsing for SELECT ... WHERE pk = value
}
```

## AST Hierarchy

**Statements**: `CreateTableStatement`, `DropTableStatement`, `InsertStatement`, `SelectStatement`, `UpdateStatement`, `DeleteStatement`, `AlterTableStatement`, `CreateIndexStatement`, `DropIndexStatement`, `CreateViewStatement`, `DropViewStatement`, `CreateTriggerStatement`, `DropTriggerStatement`, `WithStatement`

**Expressions**: `LiteralExpression`, `ParameterExpression`, `ColumnRefExpression`, `BinaryExpression`, `UnaryExpression`, `LikeExpression`, `InExpression`, `BetweenExpression`, `IsNullExpression`, `FunctionCallExpression`

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
