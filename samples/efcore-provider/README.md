# EF Core Provider Sample

This sample shows the embedded EF Core 10 provider running against a file-backed CSharpDB database with:

- `UseCSharpDb(...)`
- `EnsureCreatedAsync()`
- insert/query/update-friendly entity mapping
- exact `decimal(18, 2)` mapping through provider-owned scaled-integer storage
- collection navigation loading with `Include(...)`
- ordinal string predicates with plain `Contains(string)` and literal
  `StringComparison.Ordinal` for `StartsWith`, `EndsWith`, and `Contains`
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
- Unsupported in v1: pooled connections, named shared-memory databases,
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
  captured comparison modes, and character overloads.

For the full provider guide and supported-feature matrix, see the [EF Core Provider guide](https://csharpdb.com/docs/entity-framework-core.html).
