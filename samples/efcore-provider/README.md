# EF Core Provider Sample

This sample shows the embedded EF Core 10 provider running against a file-backed CSharpDB database with:

- `UseCSharpDb(...)`
- `EnsureCreatedAsync()`
- insert/query/update-friendly entity mapping
- collection navigation loading with `Include(...)`
- design-time context creation for `dotnet ef`

## Run The Sample

```bash
dotnet run --project samples/efcore-provider/EfCoreProviderSample.csproj -- --database-path artifacts/sample-verification/efcore-provider.db
```

Expected output looks like:

```text
Database: C:\...\efcore-provider.db
Blogs: 2
Posts: 3
Engineering|2
Operations|1
```

## First Migration

Run these commands from `samples/efcore-provider`:

```bash
dotnet ef migrations add InitialCreate
dotnet ef database update
dotnet ef migrations script
```

The sample includes `BloggingContextFactory`, so `dotnet ef` can create the context without a separate startup project or design package.

## Scope

- Runtime + migrations: file-backed databases
- Runtime only: private `:memory:` when you supply and keep open a `CSharpDbConnection`
- Unsupported in v1: pooled connections, named shared-memory databases, endpoint/daemon transports, schemas, defaults/checks/computed columns, standalone FK alterations, and `decimal` without an explicit converter

For the full provider guide and supported-feature matrix, see the [EF Core Provider guide](https://csharpdb.com/docs/entity-framework-core.html).
