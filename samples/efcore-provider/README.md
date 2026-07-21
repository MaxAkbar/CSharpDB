# EF Core Provider Sample

This sample shows the embedded EF Core 10 provider running against a file-backed CSharpDB database with:

- `UseCSharpDb(...)`
- `EnsureCreatedAsync()`
- insert/query/update-friendly entity mapping
- exact `decimal(18, 2)` mapping through provider-owned scaled-integer storage
- collection navigation loading with `Include(...)`
- ordinal string predicates with plain `Contains(string)` and literal
  `StringComparison.Ordinal` for `StartsWith`, `EndsWith`, and `Contains`
- bounded `EF.Functions.Like(...)` with a captured wildcard pattern
- one terminal `Except(...)` over direct integer-column projections
- a direct two-entity `Join(...)` over nonnullable integer keys
- a direct two-entity `LeftJoin(...)` that preserves a blog without posts
- documented guidance for the conventional optional-relationship `ClientSetNull` pattern
- database-generated `[Timestamp]`/rowversion concurrency, including raw SQL updates
- design-time context creation for `dotnet ef`

## Run The Sample

```bash
dotnet run --project samples/efcore-provider/EfCoreProviderSample.csproj -- --database-path artifacts/sample-verification/efcore-provider.db
```

Expected output looks like:

```text
Database: C:\...\efcore-provider.db
Blogs: 3
Posts: 3
JoinedPosts: 3
LeftJoinedRows: 4
BlogsWithoutPosts: 1
StringPredicateMatches: 3
LikePredicateMatches: 1
TerminalExceptBlogsWithoutPosts: 1
RowVersionBytes: 8
RowVersionAdvancedAfterRawSql: True
Engineering|2
Operations|1
Research|0
```

## First Migration

Run these commands from `samples/efcore-provider`:

```bash
dotnet ef migrations add InitialCreate
dotnet ef database update
dotnet ef migrations script
```

The sample includes `BloggingContextFactory`, so `dotnet ef` can create the context without a separate startup project or design package.

## Idempotent Deployment Script

Generate one script that can be applied to empty, partially migrated, or
current databases:

```bash
dotnet ef migrations script --idempotent --output artifacts/deploy.sql
```

CSharpDB guards each migration block with `__EFMigrationsHistory`. Validate the
generated artifact in CI, keep compound migration commands inside their
surrounding transaction, and back up the database plus WAL before applying a
production schema change.

## Defaults, Checks, and Computed Values

Literal defaults and deterministic row-local checks can be modeled normally:

```csharp
modelBuilder.Entity<Post>(post =>
{
    post.Property(item => item.Title)
        .HasDefaultValue("Draft");

    post.ToTable(table =>
        table.HasCheckConstraint(
            "CK_Posts_Title",
            "\"Title\" <> ''"));
});
```

`HasDefaultValueSql(...)` and computed columns remain unsupported. Calculate a
derived value in application code, expose it as a query projection when the
expression is inside the documented LINQ surface, or persist it explicitly.

## Financial Decimals

The sample's `MonthlyBudget` uses the provider-owned exact mapping:

```csharp
blog.Property(item => item.MonthlyBudget)
    .HasPrecision(18, 2);
```

Precision 1–18 and scale 0–precision are supported for exact round trips,
parameters, comparisons, ordering, and ordinary indexes. Excess fractional
digits and overflow fail instead of rounding. Arithmetic and aggregates over
this mapping are not currently supported in server queries.

## Optimistic Concurrency and Conflict Resolution

`Blog.RowVersion` is an opaque per-row token. A stale write throws
`DbUpdateConcurrencyException`; reload database values, apply the
application's merge policy, and retry deliberately:

```csharp
try
{
    await db.SaveChangesAsync();
}
catch (DbUpdateConcurrencyException conflict)
{
    foreach (var entry in conflict.Entries)
    {
        var databaseValues = await entry.GetDatabaseValuesAsync();
        if (databaseValues is null)
            throw; // The row was deleted.

        entry.OriginalValues.SetValues(databaseValues);
        // Decide which proposed values to keep before retrying.
    }

    await db.SaveChangesAsync();
}
```

The token is eight bytes and advances after every successful update. It is not
SQL Server's database-wide rowversion counter.

## Composite Relationships

Composite primary and foreign keys are supported when every key component has
a supported scalar mapping:

```csharp
modelBuilder.Entity<OrderLine>()
    .HasKey(line => new { line.OrderId, line.LineNumber });

modelBuilder.Entity<ShipmentLine>()
    .HasOne(item => item.OrderLine)
    .WithMany()
    .HasForeignKey(item => new { item.OrderId, item.LineNumber });
```

Use the provider guide for the exact `ClientSetNull`, cascade, alternate-key,
and migration boundaries.

## Query Cookbook and Troubleshooting

Start with direct entity roots and compose only operators documented in the
[EF Core provider guide](../../src/CSharpDB.EntityFrameworkCore/README.md#linq-translation).
Provider diagnostics fail before command dispatch:

- `CDBEF1001`/`CDBEF1002` identify unsupported CLR methods and members.
- `CDBEF1003` identifies unsupported query operators.
- `CDBEF1004`–`CDBEF1009` identify bounded aggregate, decimal, join, and set
  operation shapes.

When client evaluation is intentional, keep selective supported predicates on
the server and make the boundary explicit:

```csharp
var candidates = await db.Blogs
    .Where(blog => blog.MonthlyBudget >= 500m)
    .AsNoTracking()
    .ToListAsync();

var result = candidates
    .Where(blog => MyApplicationOnlyRule(blog))
    .ToList();
```

Do not call `AsEnumerable()` before a selective server-side predicate; doing so
can load the entire table.

## Optional Relationships

Use a nullable foreign-key property and leave the delete behavior at EF Core's
default:

```csharp
modelBuilder.Entity<Post>()
    .HasOne(post => post.Editor)
    .WithMany(editor => editor.Posts)
    .HasForeignKey(post => post.EditorId);

public sealed class Post
{
    // Other post properties...
    public int? EditorId { get; set; }
    public Editor? Editor { get; set; }
}

public sealed class Editor
{
    public int Id { get; set; }
    public List<Post> Posts { get; set; } = [];
}
```

The default is `DeleteBehavior.ClientSetNull`. When the editor and posts are
tracked, EF updates `EditorId` to `null` before deleting the editor. When a
dependent post is not tracked, CSharpDB's restrictive foreign key blocks the
delete. Do not configure `DeleteBehavior.SetNull`; database-side `ON DELETE SET
NULL` is not supported.

## Scope

- Runtime + migrations: file-backed databases
- Runtime only: private `:memory:` when you supply and keep open a `CSharpDbConnection`
- Provider-created file connections use pooling by default; specify
  `Pooling=false` to request a physical close after each EF operation.
- Unsupported in v1: named shared-memory databases,
  endpoint/daemon transports, schemas, computed/default SQL columns,
  decimal keys/arithmetic/aggregates, and precision/scale-changing migrations.
  Rowversion supports one nonnullable `byte[]` property configured with
  `[Timestamp]` or `IsRowVersion()` when it is created with the table; standalone
  add/alter rowversion migrations remain unsupported. Tokens are opaque,
  eight-byte, big-endian per-row revisions rather than SQL Server's
  database-wide counter. Inner joins are limited to one direct `Join` over
  nonnullable `int`, `long`, or `int`/`long`-backed enum keys; filtered inner
  sources and composite/chained joins remain unsupported. The same bounded
  key and source rules apply to one direct `LeftJoin`, with nullable
  unmatched-side projections; other outer joins and cross joins remain
  unsupported. Plain `Contains(string)` is ordinal. `StartsWith`, `EndsWith`,
  and the two-argument `Contains` require a literal
  `StringComparison.Ordinal`; all other string-search overloads remain
  unsupported, including default culture-sensitive `StartsWith`/`EndsWith`,
  the Boolean/`CultureInfo` forms, ignore-case or culture comparison modes,
  captured comparison modes, and character overloads. `EF.Functions.Like`
  supports one direct converter-free `TEXT` match property and a constant or
  captured pattern. `%` and `_` are wildcards under invariant
  case-insensitive CSharpDB semantics; the escape overload requires a literal
  one-UTF-16-code-unit escape other than `%`. Transformed or converted
  matches, row-derived patterns, and captured or invalid escapes are rejected
  before dispatch. Exactly one terminal `Concat`, `Union`, `Intersect`, or
  `Except` is supported when both branches are direct mapped tables with
  optional filtering and each projects one compatible, converter-free
  `INTEGER`-backed `int`, `long`, or nullable equivalent. Branch ordering,
  limits, distinct/grouped/derived sources, entity or transformed projections,
  nested operations, comparer overloads, and server-side composition after the
  set operation remain unsupported. Materialize first, then order or transform
  the results in memory.

For the full provider guide and supported-feature matrix, see the [EF Core Provider guide](https://csharpdb.com/docs/entity-framework-core.html).
