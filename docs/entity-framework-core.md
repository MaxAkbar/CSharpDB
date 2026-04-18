# Entity Framework Core 10 Provider

`CSharpDB.EntityFrameworkCore` adds an embedded-only EF Core 10 provider on top of the existing ADO.NET layer in `CSharpDB.Data`.

## Install

For an app that consumes the package directly:

```bash
dotnet add package CSharpDB.EntityFrameworkCore
dotnet add package Microsoft.EntityFrameworkCore.Design
```

Then configure your context with `UseCSharpDb(...)`:

```csharp
using CSharpDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

public sealed class BloggingContext(string databasePath) : DbContext
{
    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseCSharpDb($"Data Source={databasePath}");
}
```

You can also pass an existing `CSharpDbConnection`:

```csharp
await using var connection = new CSharpDbConnection("Data Source=:memory:");
await connection.OpenAsync();

var options = new DbContextOptionsBuilder<BloggingContext>()
    .UseCSharpDb(connection)
    .Options;
```

Keep that connection open for the full lifetime of a private `:memory:` database.

## Migrations

For file-backed databases, the provider supports the normal EF Core design-time flow:

```bash
dotnet ef migrations add InitialCreate
dotnet ef database update
dotnet ef migrations script
```

`Database.Migrate()` and `EnsureCreated()` are both supported. Migrations use the standard `__EFMigrationsHistory` table plus a simple `__EFMigrationsLock` row to serialize concurrent migration runs across processes.

## Supported Feature Matrix

| Area | Supported in v1 | Notes |
|------|------------------|-------|
| Runtime provider | Yes | Embedded only |
| File-backed databases | Yes | Primary supported mode |
| Private `:memory:` runtime | Yes | Supply an open `CSharpDbConnection` |
| `Database.Migrate()` | Yes | File-backed only |
| `dotnet ef migrations add` | Yes | No separate design package required |
| `dotnet ef database update` | Yes | File-backed only |
| `dotnet ef migrations script` | Yes | Non-idempotent scripts |
| `EnsureCreated()` | Yes | File-backed and private `:memory:` |
| CRUD + change tracking | Yes | Includes affected-row concurrency checks |
| Integer identity propagation | Yes | Single-column integer PKs |
| Basic query subset | Yes | `Where`, ordering, pagination, scalar projections, `First`/`Single`, `Any`, `Count`, null checks, `Contains`, and simple navigation-loading joins |
| Supported CLR types | Yes | `bool`, integral types, enums, `double`, `float`, `string`, `Guid`, `DateTime`, `DateTimeOffset`, `DateOnly`, `TimeOnly`, `byte[]` |
| `decimal` | No | Add an explicit converter |
| Schemas | No | Unsupported in runtime and migrations |
| Defaults / computed / checks | No | Rejected early |
| Rowversion | No | Unsupported |
| Pooling | No | Provider rejects pooled connections |
| Named shared-memory | No | Provider rejects `:memory:<name>` |
| Endpoint / daemon transports | No | Embedded-only provider in v1 |
| Reverse engineering / scaffolding | No | Deferred |
| Standalone FK alteration migrations | No | Only inline FKs in `CreateTable` |
| Broad table-rebuild emulation | No | Unsupported operations fail explicitly |
| Idempotent migration scripts | No | `dotnet ef migrations script --idempotent` is not supported in v1 |

## DDL Surface

The provider’s migrations SQL generator currently supports:

- `CreateTable`
- `DropTable`
- `RenameTable`
- `AddColumn`
- `RenameColumn`
- `DropColumn`
- `CreateIndex`
- `DropIndex`

Foreign keys are supported only when emitted inline during `CreateTable`, and only for the current single-column shape supported by the engine.

## Sample

See [samples/efcore-provider](../samples/efcore-provider/README.md) for a minimal runnable app and design-time context factory.
