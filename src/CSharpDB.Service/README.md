# CSharpDB.Service

Thread-safe service layer for hosting [CSharpDB](https://github.com/MaxAkbar/CSharpDB) in ASP.NET Core, Blazor, or MCP server applications.

[![NuGet](https://img.shields.io/nuget/v/CSharpDB.Service)](https://www.nuget.org/packages/CSharpDB.Service)
[![.NET 10](https://img.shields.io/badge/.NET-10-512bd4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Release](https://img.shields.io/github/v/release/MaxAkbar/CSharpDB?display_name=tag&label=Release)](https://github.com/MaxAkbar/CSharpDB/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE)

## Overview

`CSharpDB.Service` wraps the CSharpDB ADO.NET provider in a thread-safe service class designed for dependency injection in web applications. It provides a complete API surface for schema operations, CRUD, DDL, SQL console execution, and storage diagnostics. Reads the `CSharpDB` connection string from `IConfiguration`.

## Features

- **Thread-safe**: All operations are serialized via `SemaphoreSlim`
- **DI-ready**: Constructor takes `IConfiguration`, reads the `"CSharpDB"` connection string
- **Full schema operations**: Tables, indexes, views, triggers (create, update, drop)
- **CRUD**: Browse (paginated), get by PK, insert, update, delete
- **Procedure catalog**: Table-backed procedure definitions with strict typed parameter validation and transactional execution
- **SQL console**: `ExecuteSqlAsync` with multi-statement splitting and timing
- **Storage diagnostics**: Database inspection, WAL checking, page inspection, index verification
- **Events**: `TablesChanged`, `SchemaChanged`, and `ProceduresChanged` for UI refresh

## Usage

### ASP.NET Core Registration

```csharp
// appsettings.json
{
    "ConnectionStrings": {
        "CSharpDB": "Data Source=myapp.db"
    }
}

// Program.cs
builder.Services.AddSingleton<CSharpDbService>();
```

### Service API

```csharp
using CSharpDB.Service;

public class MyController(CSharpDbService db)
{
    // Browse a table with pagination
    public async Task<TableBrowseResult> GetUsers(int page = 1)
        => await db.BrowseTableAsync("users", page, pageSize: 25);

    // Get a single row by primary key
    public async Task<Dictionary<string, object?>> GetUser(string pk)
        => await db.GetRowByPkAsync("users", pk);

    // Insert a row
    public async Task CreateUser(Dictionary<string, string?> values)
        => await db.InsertRowAsync("users", values);

    // Execute arbitrary SQL
    public async Task<SqlExecutionResult> RunSql(string sql)
        => await db.ExecuteSqlAsync(sql);

    // Schema introspection
    public async Task<List<string>> GetTables()
        => await db.GetTableNamesAsync();

    // Storage diagnostics
    public async Task<object> InspectDatabase()
        => await db.InspectStorageAsync();
}
```

### Schema Management

```csharp
// DDL operations
await db.CreateIndexAsync("users", "idx_email", "email", isUnique: true);
await db.CreateViewAsync("active_users", "SELECT * FROM users WHERE active = 1");
await db.RenameTableAsync("users", "app_users");
await db.AddColumnAsync("app_users", "created_at", "TEXT");
await db.DropColumnAsync("app_users", "legacy_field");
```

### Storage Diagnostics

```csharp
// Inspect the database file
var report = await db.InspectStorageAsync();

// Check WAL integrity
var walReport = await db.CheckWalAsync();

// Inspect a specific page
var pageInfo = await db.InspectPageAsync(pageId: 5);

// Verify all indexes
var indexReport = await db.CheckIndexesAsync();
```

### Procedure Catalog

```csharp
using CSharpDB.Core;
using CSharpDB.Service.Models;

await db.CreateProcedureAsync(new ProcedureDefinition
{
    Name = "GetUserById",
    BodySql = "SELECT * FROM users WHERE id = @id;",
    Parameters =
    [
        new ProcedureParameterDefinition
        {
            Name = "id",
            Type = DbType.Integer,
            Required = true,
        }
    ],
    IsEnabled = true,
    CreatedUtc = DateTime.UtcNow,
    UpdatedUtc = DateTime.UtcNow,
});

var execution = await db.ExecuteProcedureAsync("GetUserById", new Dictionary<string, object?>
{
    ["id"] = 1L,
});
```

## Result Models

| Model | Description |
|-------|-------------|
| `SqlExecutionResult` | Query/DML result with `IsQuery`, `ColumnNames`, `Rows`, `RowsAffected`, `Error`, `Elapsed` |
| `TableBrowseResult` | Paginated table data with `Schema`, `Rows`, `TotalRows`, `Page`, `PageSize`, `TotalPages` |
| `ViewBrowseResult` | View query results |
| `ViewDefinition` | View name and defining SQL |
| `ProcedureDefinition` | Procedure metadata: name, body SQL, params, enabled flag, timestamps |
| `ProcedureExecutionResult` | Procedure execution summary with per-statement results and rollback-safe error info |

## Installation

```
dotnet add package CSharpDB.Service
```

For the recommended all-in-one package:

```
dotnet add package CSharpDB
```

## Dependencies

- `CSharpDB.Data` - ADO.NET provider
- `CSharpDB.Storage.Diagnostics` - storage inspection toolkit
- `Microsoft.Extensions.Configuration.Abstractions` - configuration binding

## Related Packages

| Package | Description |
|---------|-------------|
| [CSharpDB.Data](https://www.nuget.org/packages/CSharpDB.Data) | Underlying ADO.NET provider |
| [CSharpDB.Engine](https://www.nuget.org/packages/CSharpDB.Engine) | Embedded database engine |

## License

MIT - see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE) for details.
