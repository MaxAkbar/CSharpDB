# CSharpDB.EntityFrameworkCore

Entity Framework Core 10 provider for the
[CSharpDB](https://github.com/MaxAkbar/CSharpDB) embedded database engine.
Use standard EF Core `DbContext`, migrations, and LINQ patterns against
embedded file-backed or private in-memory CSharpDB databases.

[![NuGet](https://img.shields.io/nuget/v/CSharpDB.EntityFrameworkCore)](https://www.nuget.org/packages/CSharpDB.EntityFrameworkCore)
[![.NET 10](https://img.shields.io/badge/.NET-10-512bd4)](https://dotnet.microsoft.com/download/dotnet/10.0)
[![Release](https://img.shields.io/github/v/release/MaxAkbar/CSharpDB?display_name=tag&label=Release)](https://github.com/MaxAkbar/CSharpDB/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE)

## Overview

`CSharpDB.EntityFrameworkCore` adds an embedded-only EF Core provider on top of
`CSharpDB.Data`. It supports the current CSharpDB relational runtime with:

- `UseCSharpDb(...)` provider configuration
- file-backed runtime and migrations
- private `:memory:` runtime when you keep a `CSharpDbConnection` open
- `EnsureCreated()`, `Database.Migrate()`, and standard `dotnet ef` flows
- CRUD, change tracking, concurrency row-count checks, and a focused LINQ subset

This package is intentionally scoped as a v1 embedded provider. It does not
target daemon/client transports, pooled connections, or broad schema-rebuild
emulation.

## Installation

```bash
dotnet add package CSharpDB.EntityFrameworkCore
dotnet add package Microsoft.EntityFrameworkCore.Design
```

`Microsoft.EntityFrameworkCore.Design` is still recommended in the application
project so `dotnet ef` can run design-time commands cleanly.

## Usage

```csharp
using CSharpDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

public sealed class BloggingContext : DbContext
{
    private readonly string? _connectionString;

    public BloggingContext(string databasePath)
        => _connectionString = $"Data Source={databasePath}";

    public BloggingContext(DbContextOptions<BloggingContext> options)
        : base(options)
    {
    }

    public DbSet<Blog> Blogs => Set<Blog>();
    public DbSet<Post> Posts => Set<Post>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        if (!optionsBuilder.IsConfigured && _connectionString is not null)
            optionsBuilder.UseCSharpDb(_connectionString);
    }
}

public sealed class Blog
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public List<Post> Posts { get; set; } = [];
}

public sealed class Post
{
    public int Id { get; set; }
    public int BlogId { get; set; }
    public string Title { get; set; } = string.Empty;
    public Blog Blog { get; set; } = null!;
}
```

Then use EF Core as usual:

```csharp
await using var db = new BloggingContext("blogging.db");
await db.Database.EnsureCreatedAsync();

db.Blogs.Add(new Blog
{
    Name = "Engineering",
    Posts = [new Post { Title = "Hello from CSharpDB EF Core" }]
});

await db.SaveChangesAsync();

var blogs = await db.Blogs
    .Include(b => b.Posts)
    .OrderBy(b => b.Name)
    .ToListAsync();
```

## Using an Existing Connection

For a private in-memory database, open and keep the `CSharpDbConnection` alive
for the entire `DbContext` lifetime:

```csharp
using CSharpDB.Data;
using CSharpDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

await using var connection = new CSharpDbConnection("Data Source=:memory:");
await connection.OpenAsync();

var options = new DbContextOptionsBuilder<BloggingContext>()
    .UseCSharpDb(connection)
    .Options;

await using var db = new BloggingContext(options);
await db.Database.EnsureCreatedAsync();
```

## Embedded Storage Tuning

The EF Core provider can now push the embedded engine tuning surface down into
the `CSharpDbConnection` it creates.

Use named presets and embedded open mode:

```csharp
using CSharpDB.Data;
using CSharpDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

var options = new DbContextOptionsBuilder<BloggingContext>()
    .UseCSharpDb(
        "Data Source=blogging.db",
        csharpdb =>
        {
            csharpdb.UseStoragePreset(CSharpDbStoragePreset.WriteOptimized);
            csharpdb.UseEmbeddedOpenMode(CSharpDbEmbeddedOpenMode.HybridIncrementalDurable);
        })
    .Options;
```

Use full direct or hybrid options when you want exact engine composition:

```csharp
using CSharpDB.Engine;

var directOptions = new DatabaseOptions()
    .ConfigureStorageEngine(builder => builder.UseWriteOptimizedPreset());

var options = new DbContextOptionsBuilder<BloggingContext>()
    .UseCSharpDb(
        "Data Source=blogging.db",
        csharpdb => csharpdb.UseDirectDatabaseOptions(directOptions))
    .Options;
```

Provider builder methods:

- `UseDirectDatabaseOptions(DatabaseOptions)`
- `UseHybridDatabaseOptions(HybridDatabaseOptions)`
- `UseStoragePreset(CSharpDbStoragePreset)`
- `UseEmbeddedOpenMode(CSharpDbEmbeddedOpenMode)`

Precedence rules:

- explicit `DirectDatabaseOptions` override `Storage Preset`
- explicit `HybridDatabaseOptions` override `Embedded Open Mode`
- provider builder tuning is validated, not applied mutably, when EF Core is
  given an existing `CSharpDbConnection`

## Migrations

For file-backed databases, the normal EF Core workflow is supported:

```bash
dotnet ef migrations add InitialCreate
dotnet ef database update
dotnet ef migrations script
```

`Database.Migrate()` is supported for file-backed databases. Migrations use the
standard `__EFMigrationsHistory` table plus a simple `__EFMigrationsLock` row
to serialize concurrent migration runs across processes.

## Supported v1 Surface

| Area | Supported | Notes |
|------|-----------|-------|
| Embedded runtime provider | Yes | No daemon or remote transports |
| File-backed databases | Yes | Primary supported runtime and migration mode |
| Private `:memory:` runtime | Yes | Requires an open `CSharpDbConnection` |
| `EnsureCreated()` | Yes | File-backed and private in-memory |
| `Database.Migrate()` | Yes | File-backed only |
| `dotnet ef migrations add` | Yes | Use the app project with `Microsoft.EntityFrameworkCore.Design` |
| `dotnet ef database update` | Yes | File-backed only |
| `dotnet ef migrations script` | Yes | Non-idempotent scripts only |
| CRUD + change tracking | Yes | Includes affected-row concurrency checks |
| Integer identity propagation | Yes | Single-column integer primary keys |
| Basic LINQ/query subset | Yes | `Where`, ordering, pagination, scalar projections, `First`/`Single`, `Any`, `Count`, null checks, `Contains`, and simple navigation-loading joins |
| Supported CLR types | Yes | `bool`, integral types, enums, `double`, `float`, `string`, `Guid`, `DateTime`, `DateTimeOffset`, `DateOnly`, `TimeOnly`, `byte[]` |

## Current Limitations

- `decimal` requires an explicit value converter
- schemas are unsupported in runtime and migrations
- defaults, computed columns, check constraints, and rowversion are unsupported
- pooled connections are rejected
- named shared-memory databases (`:memory:<name>`) are rejected
- standalone foreign-key alteration migrations are unsupported
- idempotent migration scripts are unsupported in v1

## Dependencies

The provider depends on:

- [CSharpDB.Data](https://www.nuget.org/packages/CSharpDB.Data) for the ADO.NET
  connection and command layer
- `Microsoft.EntityFrameworkCore.Relational`

## Related Packages

| Package | Description |
|---------|-------------|
| [CSharpDB](https://www.nuget.org/packages/CSharpDB) | All-in-one package for core application development |
| [CSharpDB.Data](https://www.nuget.org/packages/CSharpDB.Data) | Underlying ADO.NET provider used by the EF Core provider |
| [CSharpDB.Engine](https://www.nuget.org/packages/CSharpDB.Engine) | Embedded database engine below the relational/provider layers |

## Docs and Samples

- [EF Core provider guide](https://csharpdb.com/docs/entity-framework-core.html)
- [EF Core provider sample](../../samples/efcore-provider/README.md)
- [ADO.NET and EF storage tuning notes](../../docs/ado-ef-storage-tuning/README.md)

## License

MIT - see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE) for
details.
