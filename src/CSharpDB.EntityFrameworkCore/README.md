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
dotnet ef migrations script --idempotent
```

`Database.Migrate()` is supported for file-backed databases. Migrations use the
standard `__EFMigrationsHistory` table plus a simple `__EFMigrationsLock` row
to serialize concurrent migration runs across processes. Idempotent scripts
guard migration commands with history-table checks, so one script can be
applied to empty, partially migrated, or current databases.

Provider versions before 4.2.0 emitted create-time foreign keys inline, so the
engine assigned generated constraint names. Before dropping one of those
legacy constraints, query `sys.foreign_keys` for its stored name and use that
name in a one-time migration. New 4.2.0 schemas preserve EF constraint names.

Standalone primary-key migrations have a bounded support path:

- named single-`TEXT` and composite `INTEGER`/`TEXT` logical primary keys can be
  added and dropped
- a single physical `INTEGER` primary key can be added to populated data after
  every value passes non-NULL and uniqueness validation; those values become
  the physical row IDs
- ready ordinary/unique SQL, constraint-owned, and foreign-key-support indexes
  are rebuilt atomically with that physical rekey; full-text, collection, and
  non-ready indexes reject the operation before mutation
- EF `DropPrimaryKey` migrations emit `DROP CONSTRAINT` with the exact
  constraint name; a mismatched name does not drop another key
- `ALTER TABLE ... DROP PRIMARY KEY` is the explicit compatibility path for an
  older unnamed primary key
- dropping a primary key preserves `NOT NULL`; dropping a physical `INTEGER`
  key also ends its identity role, and EF can emit separate follow-up column
  changes when needed

Existing nulls or duplicates reject a primary-key add without leaving key or
index metadata behind. A primary key cannot be dropped while an inbound foreign
key depends on it unless another ordered, collation-compatible unique candidate
remains.

`AlterColumn` also has a bounded shadow-rewrite path:

- `INTEGER` to `REAL` is accepted when every integer is within the exactly
  representable `±2^53` range
- `REAL` to `INTEGER` is accepted when every value is finite, integral, and in
  the signed 64-bit range
- a `TEXT` column can change among supported collations or return to the
  default `BINARY` collation
- ready ordinary and unique SQL indexes that inherit the column collation are
  rebuilt atomically with the table; explicit-collation indexes and indexes on
  other columns retain their roots
- physical row IDs are preserved, and checks plus affected uniqueness are
  revalidated against the rewritten rows
- key constraints, foreign keys, full-text/collection dependencies, views on
  the table, table-owned triggers, cross-table triggers that reference the
  column, and applicable validation rules still block the rewrite

The provider orders compound changes as drop old default, rewrite type, restore
the target default, change collation, then change nullability. `Database.Migrate()`,
`dotnet ef database update`, and normal generated migration scripts supply the
surrounding transaction. If those commands are extracted into a custom deployment
script, keep them in one transaction so a failed later facet also restores the
original table and index roots.

## Exact Decimal Foundation

`decimal` and nullable `decimal` properties no longer require an application
value converter for the bounded exact mapping. The provider stores values as
signed scaled `INTEGER`s, so round trips, parameters used with one facet
mapping, equality/range comparisons, ordering, and ordinary indexes remain
exact:

```csharp
modelBuilder.Entity<Invoice>()
    .Property(invoice => invoice.Amount)
    .HasPrecision(18, 4);
```

The default is `decimal(18, 2)`. Precision must be between 1 and 18, and scale
must be between 0 and precision. A write with more fractional digits than the
configured scale is rejected instead of rounded, and a value outside the
configured precision is rejected as overflow. Raw SQL sees the scaled
`INTEGER` representation; for example, `12.3400` at scale 4 is stored as
`123400`.

This first slice deliberately rejects decimal keys, database defaults,
generated values, precision/scale-changing migrations, and computed decimal
expressions including arithmetic, numeric casts, conditionals/coalescing, and
`Sum`/`Average`/`Min`/`Max`. Decimal collection or subquery `Contains` and
reusing one captured parameter across different decimal facets are also
rejected, as are comparisons with application-converter decimal mappings and
model-mapped functions with decimal parameters or returns. Unsafe query
expressions fail before command dispatch with
`CDBEF1006`. Configure precision and scale with
`HasPrecision(precision, scale)`; custom decimal store-type declarations are
not accepted for the provider-owned mapping. Applications that call
`IMigrationsSqlGenerator` directly with a hand-authored `AddPrimaryKeyOperation`
must pass the target model; with `model: null`, that low-level operation does
not carry enough column metadata to identify a decimal mapping.

## LINQ Translation

The provider qualifies a deliberately bounded server-side LINQ surface.
Alongside `Where`, ordering, `Skip`/`Take`, scalar projections, `Single`,
`Any`, `Count`, non-decimal constant/parameter collection `Contains`, and
simple `Include`, these CLR members and methods translate to CSharpDB SQL:

- `string.Length`
- parameterless `ToLower()`, `ToLowerInvariant()`, `ToUpper()`, and
  `ToUpperInvariant()`
- parameterless `Trim()`, `TrimStart()`, and `TrimEnd()`
- `Replace(string, string)`
- `Substring(start)` and `Substring(start, length)`
- `DateTime.Year`, `Month`, `Day`, `Hour`, `Minute`, and `Second`
- `DateOnly.Year`, `Month`, and `Day`
- `TimeOnly.Hour`, `Minute`, and `Second`
- finite `double` overloads of `Math.Abs`, `Math.Round`, `Math.Floor`,
  `Math.Ceiling`, `Math.Truncate`, and `Math.Sign`

Both culture-sensitive and invariant CLR casing methods map to CSharpDB
`LOWER`/`UPPER`. They therefore use invariant server semantics, not the
application's `CurrentCulture`.

The qualified scalar aggregate slice covers `Count`, `LongCount`, simple and
bounded-shape `Any`, `Sum` over `int`, `double`, and nullable `double`,
`Average` over `double` and nullable `double`, and `Min`/`Max` over `int`,
`double`, and nullable `double`. Filtered, empty, and all-NULL cases are
cross-checked against SQLite. `Math.Round(double)` uses midpoint-to-even
semantics, and translated math functions propagate SQL NULL.

The bounded distinct-aggregate shape is an optional `Where`, selection of one
directly mapped nonnullable `int` column, `Distinct`, then `Count`,
`LongCount`, `Sum`, `Min`, or `Max`. Distinct `Average`, nullable or non-`int`
columns, configured value converters, predicates after `Distinct`, ordering,
limits, intervening operators, computed/composite selectors, casts, and
derived sources are rejected with `CDBEF1004` before command dispatch.

Direct single-table `GroupBy` supports an optional pre-filter and direct mapped
Boolean, integral, enum, default-`BINARY` string, or nullable keys. Composite
keys must use C# anonymous types or `ValueTuple`, and Boolean key columns must
contain canonical provider-written `0`/`1` storage. A single grouped projection
may
contain direct keys plus bare `Count`/`LongCount`, `Sum` over
`int`/`double`/nullable `double`, `Average` over `double`/nullable `double`,
`Min`/`Max` over `int`/`double`/nullable `double`, and the nonnullable-`int`
distinct variants above. Basic `HAVING`, including aggregate `IS NULL`, and
ordering by a directly projected key or aggregate are supported. `double`,
transformed, non-`BINARY`-collated, or configured-converter keys; aggregate
value converters; element/result selector overloads; group materialization;
raw group transforms; post-projection filtering/projection/distinct/limits/set
operations; predicate aggregates; casts; and broader shapes are rejected with
`CDBEF1005`.

String `StartsWith`, `EndsWith`, instance `Contains`, and
`StringComparison` overloads are not translated yet. CSharpDB's current
pattern-search functions are case-insensitive, so translating those methods
would silently change ordinary .NET string semantics. `DateTimeOffset`
components, integral/decimal/`MathF` math overloads, two-argument or
midpoint-mode rounding, long- and float-valued `Sum`/`Average`/`Min`/`Max`
variants and other unqualified aggregate variants,
broader distinct/grouped aggregate variants, set-operation, and correlated
query shapes also remain outside the qualified surface.

Unsupported expressions retain EF Core's `InvalidOperationException` and add
provider guidance:

| Code | Meaning |
|------|---------|
| `CDBEF1001` | Unsupported CLR method |
| `CDBEF1002` | Unsupported CLR member |
| `CDBEF1003` | Recognized unsupported query operator: `TakeWhile`, `SkipWhile`, `Concat`, `Union`, `Except`, `Intersect`, or `ExecuteUpdate` |
| `CDBEF1004` | Unsupported distinct aggregate shape |
| `CDBEF1005` | Unsupported grouped aggregate shape |
| `CDBEF1006` | Unsupported decimal operation outside the exact scaled-integer foundation |

When client evaluation is intentional, apply selective supported filters
first, then call `AsEnumerable()` explicitly before the unsupported portion.
This keeps the server/client boundary visible and avoids accidentally loading
an entire table.

## Supported Surface

| Area | Supported | Notes |
|------|-----------|-------|
| Embedded runtime provider | Yes | No daemon or remote transports |
| File-backed databases | Yes | Primary supported runtime and migration mode |
| Private `:memory:` runtime | Yes | Requires an open `CSharpDbConnection` |
| `EnsureCreated()` | Yes | File-backed and private in-memory |
| `Database.Migrate()` | Yes | File-backed only |
| `dotnet ef migrations add` | Yes | Use the app project with `Microsoft.EntityFrameworkCore.Design` |
| `dotnet ef database update` | Yes | File-backed only |
| `dotnet ef migrations script` | Yes | Includes idempotent scripts guarded by `__EFMigrationsHistory` |
| CRUD + change tracking | Yes | Includes affected-row concurrency checks |
| Integer identity propagation | Yes | Single-column integer primary keys |
| Composite primary keys and indexes | Yes | Composite primary keys are emitted as table constraints; composite unique and non-unique indexes preserve declared column order |
| Index migrations | Yes | Create, drop, and root-preserving rename operations |
| Alternate keys and unique constraints | Yes | Named create-table constraints plus standalone add/drop migrations |
| Standalone primary-key migrations | Partial | Named logical keys add/drop; physical `INTEGER` adds can rekey validated populated rows and supported relational indexes atomically; EF drops match the exact constraint name |
| Foreign keys | Partial | Named scalar/composite create/add/drop, primary or alternate-key targets, and cascade/restrict behavior |
| Literal column defaults | Partial | `HasDefaultValue(...)` values that map to INTEGER, REAL, TEXT, BLOB, or NULL; computed/default SQL expressions remain unsupported |
| Check constraints | Partial | Create-table and standalone add/drop migrations for deterministic row-local expressions accepted by the engine |
| `AlterColumn` | Partial | Literal default/nullability changes, exact dependency-free `INTEGER`/`REAL` rewrites, and `TEXT` collation changes with inherited ordinary/unique SQL-index rebuilding |
| Exact decimal mapping | Partial | Provider-owned scaled `INTEGER` storage for precision 1–18; exact round trips, parameters, comparisons, and ordering |
| Bounded LINQ/query subset | Partial | Basic operators plus the string, temporal, finite-double math, scalar numeric aggregate, direct-column integer-distinct aggregate, and direct single-table grouped aggregate translations listed above; unsupported methods, members, operators, and aggregate shapes receive provider diagnostics |
| Supported CLR types | Yes | `bool`, integral types, enums, bounded exact `decimal`, `double`, `float`, `string`, `Guid`, `DateTime`, `DateTimeOffset`, `DateOnly`, `TimeOnly`, `byte[]` |

## Current Limitations

- provider-owned decimal mapping does not yet support keys, defaults, generated
  values, computed decimal expressions, or precision/scale-changing migrations
- complex properties are rejected until their flattened column mappings are
  formally qualified
- `ExecuteUpdate` is rejected until assignment conversions and decimal facets
  are formally qualified
- schemas are unsupported in runtime and migrations
- computed columns, `DefaultValueSql`, and rowversion are unsupported
- string `StartsWith`, `EndsWith`, instance `Contains`, `StringComparison`
  overloads, and `DateTimeOffset` component translation are unsupported
- integral, decimal, `MathF`, precision-argument, midpoint-mode, and
  transcendental math overloads are outside the qualified translation surface
- long- and float-valued `Sum`/`Average`/`Min`/`Max` variants, integer
  non-distinct `Average`, text `Min`/`Max`, distinct `Average`, non-`int`
  distinct aggregates, and broader `GroupBy` variants remain outside the
  qualified surface
- physical `INTEGER` primary-key rekeying supports ready ordinary/unique SQL,
  constraint-owned, and foreign-key-support indexes; full-text, collection, and
  non-ready indexes are rejected
- pooled connections are rejected
- named shared-memory databases (`:memory:<name>`) are rejected
- `TEXT`/`BLOB` type conversions, lossy numeric conversions, indexed numeric
  changes, and collation changes involving key, foreign-key, full-text, or
  collection dependencies still require broader rewrite support

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
- [Versioned EF Core compatibility matrix](../../docs/ef-core-compatibility.md)
- [EF Core provider sample](../../samples/efcore-provider/README.md)
- [ADO.NET and EF storage tuning notes](https://csharpdb.com/docs/ado-ef-storage-tuning.html)

## License

MIT - see [LICENSE](https://github.com/MaxAkbar/CSharpDB/blob/main/LICENSE) for
details.
