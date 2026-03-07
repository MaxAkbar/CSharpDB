# CSharpDB.Primitives

Shared types, schema definitions, and error codes for the [CSharpDB](https://github.com/MaxAkbar/CSharpDB) embedded database engine.

[![NuGet](https://img.shields.io/nuget/v/CSharpDB.Primitives)](https://www.nuget.org/packages/CSharpDB.Primitives)
[![.NET 10](https://img.shields.io/badge/.NET-10-512bd4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Release](https://img.shields.io/github/v/release/MaxAkbar/CSharpDB?display_name=tag&label=Release)](https://github.com/MaxAkbar/CSharpDB/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE)

## Overview

`CSharpDB.Primitives` is the low-level foundation package for CSharpDB. It defines the database type system, schema model, and error codes used by higher-level CSharpDB packages.

If you are building an application, prefer the top-level package:

```
dotnet add package CSharpDB
```

## Key Types

| Type | Description |
|------|-------------|
| `DbType` | Enum of supported database types: `Null`, `Integer`, `Real`, `Text`, `Blob` |
| `DbValue` | Discriminated-union struct representing a single database value with comparison, equality, and truthiness semantics |
| `TableSchema` | Table structure definition including columns, primary key, and qualified mappings for JOINs |
| `ColumnDefinition` | Column metadata: name, type, nullability, primary key flag, and identity flag |
| `IndexSchema` | Index metadata: name, table, columns, uniqueness |
| `TriggerSchema` | Trigger metadata: name, table, timing, event, and body SQL |
| `CSharpDbException` | Typed exception with `ErrorCode` covering 15+ error conditions |

## Usage

```csharp
using CSharpDB.Core;

// Create typed values
var id = DbValue.FromInteger(42);
var name = DbValue.FromText("Alice");
var balance = DbValue.FromReal(100.50);
var empty = DbValue.Null;

// Compare values
bool isPositive = balance > DbValue.FromReal(0);

// Define a schema
var schema = new TableSchema
{
    TableName = "users",
    Columns =
    [
        new ColumnDefinition { Name = "id", Type = DbType.Integer, IsPrimaryKey = true },
        new ColumnDefinition { Name = "name", Type = DbType.Text },
        new ColumnDefinition { Name = "email", Type = DbType.Text, IsNullable = true }
    ]
};
```

## Installation

```
dotnet add package CSharpDB.Primitives
```

## Related Packages

| Package | Description |
|---------|-------------|
| [CSharpDB](https://www.nuget.org/packages/CSharpDB) | All-in-one package for application developers |
| [CSharpDB.Engine](https://www.nuget.org/packages/CSharpDB.Engine) | Embedded database engine with SQL and NoSQL APIs |
| [CSharpDB.Data](https://www.nuget.org/packages/CSharpDB.Data) | ADO.NET provider |
| [CSharpDB.Storage](https://www.nuget.org/packages/CSharpDB.Storage) | B+tree storage engine |

## License

MIT - see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE) for details.
