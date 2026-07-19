# Samples & Tutorials

This folder contains SQL dataset samples, runnable C# sample projects, and hands-on tutorials for the current CSharpDB surface area.

The SQL dataset samples use a conventional layout with `schema.sql` for setup, `procedures.json` for API/admin procedure import, and `queries.sql` for optional read-only workbook queries. Runnable C# samples include a `.csproj`, `Program.cs`, and a focused `README.md`. Tutorials provide guided walkthroughs with runnable code.

## Available Samples

| Folder | Focus | Includes |
|--------|-------|----------|
| `ecommerce-store/` | Online retail | `schema.sql`, `procedures.json` |
| `medical-clinic/` | Healthcare scheduling + billing | `schema.sql`, `procedures.json` |
| `school-district/` | Education + attendance | `schema.sql`, `procedures.json` |
| `procurement-analytics/` | Query expansion + planner stats workbook | `schema.sql`, `procedures.json`, `queries.sql` |
| `feature-tour/` | Northstar Field Services | `schema.sql`, `procedures.json`, `queries.sql` |
| `platform-showcase/` | Broad relational feature tour + optional API demo | `schema.sql`, `procedures.json`, `queries.sql`, `.csproj`, `Program.cs` |
| `fulfillment-hub/` | Full-stack operations showcase with forms, reports, pipelines, collections, and full-text search | `schema.sql`, `procedures.json`, `saved-queries.json`, `queries.sql`, `pipelines/`, `imports/`, `.csproj`, `Program.cs`, `README.md` |
| `csv-bulk-import/` | Runnable CSV-to-table bulk ingest walkthrough | `.csproj`, `Program.cs`, `README.md`, `events.csv` |
| `api-level-sharding/` | Runnable e-commerce order-history sharding walkthrough | `.csproj`, `Program.cs`, `README.md` |
| `collection-indexing/` | Runnable `Collection<T>` indexing walkthrough | `.csproj`, `Program.cs`, `README.md` |
| `compression-sdk/` | Runnable application-level payload compression helper and benchmark sample | `.csproj`, `Program.cs`, `PayloadCompression.cs`, `Benchmarks/`, `README.md` |
| `generated-collections/` | Runnable source-generated collection fast-path walkthrough | `.csproj`, `Program.cs`, `README.md` |
| `efcore-provider/` | Runnable EF Core 10 embedded-provider sample | `.csproj`, `Program.cs`, `README.md` |
| `efcore-minimal-api/` | Runnable ASP.NET Core 10 HTTP CRUD API using a scoped CSharpDB EF Core context | `.csproj`, `Program.cs`, `appsettings.json`, `sample.http`, `README.md` |
| `aspnet-core-identity/` | Runnable ASP.NET Core 10 auth sample with a custom ADO.NET-backed user store, cookie + JWT auth, and role/policy authorization | `.csproj`, `Program.cs`, `UserStore.cs`, `AppUser.cs`, `Seed.cs`, `sample.http`, `README.md` |
| `trusted-csharp-host/` | Runnable trusted callback host sample | scalar functions, commands, validation rules, form automation metadata, `.csproj`, `Program.cs`, `README.md` |

## Tutorials

| Folder | Focus | Contents |
|--------|-------|----------|
| `storage-tutorials/` | Storage engine internals | Architecture guide, extensibility patterns, runnable examples (GraphDB, SpatialIndex, TimeSeries, VirtualFS, and more) |
| `native-ffi/` | Cross-language FFI via NativeAOT | Python (ctypes) and JavaScript (koffi/Node.js) wrappers with CRUD and transaction examples |

Root-level helpers:

- `run-sample.csx`: import helper for SQL + procedures through the REST API
- `mcp.json`: example MCP configuration for pointing assistants at a sample-backed database

## Sample Highlights

### Northwind Electronics

- SQL: [schema.sql](ecommerce-store/schema.sql)
- Procedures: [procedures.json](ecommerce-store/procedures.json)
- Domain: customers, products, orders, reviews, and shipping addresses
- Good for: joins, inventory-style triggers, order rollups

### Riverside Health Center

- SQL: [schema.sql](medical-clinic/schema.sql)
- Procedures: [procedures.json](medical-clinic/procedures.json)
- Domain: departments, doctors, patients, appointments, prescriptions, and billing
- Good for: views over operational data and procedure-driven updates

### Maplewood Unified School District

- SQL: [schema.sql](school-district/schema.sql)
- Procedures: [procedures.json](school-district/procedures.json)
- Domain: teachers, students, courses, schedules, enrollments, and attendance
- Good for: joins, defaulted procedure parameters, trigger-generated child rows

### Northstar Field Services

- SQL: [schema.sql](feature-tour/schema.sql)
- Procedures: [procedures.json](feature-tour/procedures.json)
- Queries: [queries.sql](feature-tour/queries.sql)
- Domain: multi-region field service operations for grocery, logistics, healthcare, and campus customers
- Good for: customer/site hierarchies, contract coverage, dispatch workflows, inventory positions, billing snapshots, procedure execution, system catalog inspection, and `TEXT(...)` search over numeric IDs

### Procurement Analytics Lab

- SQL: [schema.sql](procurement-analytics/schema.sql)
- Procedures: [procedures.json](procurement-analytics/procedures.json)
- Queries: [queries.sql](procurement-analytics/queries.sql)
- Domain: suppliers, warehouses, products, purchase orders, and quality incidents
- Good for: `UNION` / `INTERSECT` / `EXCEPT`, scalar subqueries, `IN (SELECT ...)`, `EXISTS (SELECT ...)`, `ANALYZE`, and `sys.table_stats` / `sys.column_stats`

### Atlas Platform Showcase

- SQL: [schema.sql](platform-showcase/schema.sql)
- Procedures: [procedures.json](platform-showcase/procedures.json)
- Queries: [queries.sql](platform-showcase/queries.sql)
- Demo: [PlatformShowcaseSample.csproj](platform-showcase/PlatformShowcaseSample.csproj)
- Domain: subscriptions, orders, support operations, inventory, knowledge articles, and dashboard presets
- Good for: foreign keys, collations, unique + composite indexes, views, triggers, `IDENTITY` audit rows, joins, CTEs, subqueries, set operations, `TEXT(...)`, `ANALYZE`, full-text search, and `Collection<T>`

### Fulfillment Hub

- SQL: [schema.sql](fulfillment-hub/schema.sql)
- Procedures: [procedures.json](fulfillment-hub/procedures.json)
- Saved Queries: [saved-queries.json](fulfillment-hub/saved-queries.json)
- Queries: [queries.sql](fulfillment-hub/queries.sql)
- Pipelines: [pipelines/](fulfillment-hub/pipelines)
- Demo: [FulfillmentHubSample.csproj](fulfillment-hub/FulfillmentHubSample.csproj)
- Docs: [README.md](fulfillment-hub/README.md)
- Domain: local-first warehouse, order fulfillment, receiving, shipment, and returns operations
- Good for: end-to-end admin workflows, stored procedures, saved queries, reports, forms, CSV/JSON pipelines, typed collections with path indexes, and full-text search

### Bulk Import / CSV To Table

- Project: [CsvBulkImportSample.csproj](csv-bulk-import/CsvBulkImportSample.csproj)
- Code: [Program.cs](csv-bulk-import/Program.cs)
- Input: [events.csv](csv-bulk-import/events.csv)
- Docs: [CSV Bulk Import Tutorial](https://csharpdb.com/docs/tutorials/csv-bulk-import.html)
- Domain: fixed-schema operational event rows loaded into a relational table
- Good for: `UseWriteOptimizedPreset()`, `PrepareInsertBatch(...)`, explicit transaction batching, header validation, row conversion into `DbValue[]`, and post-load secondary-index creation

### API-Level Sharding

- Project: [ApiLevelShardingSample.csproj](api-level-sharding/ApiLevelShardingSample.csproj)
- Code: [Program.cs](api-level-sharding/Program.cs)
- Docs: [README.md](api-level-sharding/README.md)
- Domain: e-commerce order history routed by `yyyy-MM` order month across four local shard files
- Good for: `CSharpDbShardedClient`, `ICSharpDbShardAdminClient`, `CSharpDbRouteContext`, virtual bucket maps, month route keys, exact route-key pins, route-bound clients, shard-admin map/status/route preview, execute-on-all-shards schema setup, paged recent/older order history, precise page fill across route keys, bounded over-fetch alternatives, shard-prefixed transaction ids, and missing-route failure behavior

### Collection Indexing Walkthrough

- Project: [CollectionIndexingSample.csproj](collection-indexing/CollectionIndexingSample.csproj)
- Code: [Program.cs](collection-indexing/Program.cs)
- Docs: [Collection Indexing Guide](https://csharpdb.com/docs/collection-indexing.html)
- Domain: typed user documents with nested address, tags, and orders
- Good for: `GetCollectionAsync<T>()`, `EnsureIndexAsync(...)`, `FindByIndexAsync(...)`, `FindByPathAsync(...)`, and `FindByPathRangeAsync(...)`

### Compression SDK Sample

- Project: [CompressionSdkSample.csproj](compression-sdk/CompressionSdkSample.csproj)
- Code: [Program.cs](compression-sdk/Program.cs)
- Helper: [PayloadCompression.cs](compression-sdk/PayloadCompression.cs)
- Benchmarks: [Benchmarks](compression-sdk/Benchmarks)
- Good for: opt-in application-level compression of large payload columns, codec metadata storage, and before/after benchmarking without changing the CSharpDB storage engine

### Generated Collections Walkthrough

- Project: [GeneratedCollectionsSample.csproj](generated-collections/GeneratedCollectionsSample.csproj)
- Code: [Program.cs](generated-collections/Program.cs)
- Docs: [README.md](generated-collections/README.md)
- Domain: generated customer documents with nested address, tags, and orders
- Good for: `GetGeneratedCollectionAsync<T>()`, generated `CollectionField<,>` descriptors, `JsonPropertyName` payload compatibility, and trim/AOT-friendly collection access

### EF Core Embedded Provider

- Project: [EfCoreProviderSample.csproj](efcore-provider/EfCoreProviderSample.csproj)
- Code: [Program.cs](efcore-provider/Program.cs)
- Docs: [README.md](efcore-provider/README.md)
- Domain: simple blog/posts model over the embedded EF Core provider
- Good for: `UseCSharpDb(...)`, `EnsureCreatedAsync()`, `Include(...)`, and `dotnet ef` design-time flows

### EF Core Minimal API

- Project: [EfCoreMinimalApiSample.csproj](efcore-minimal-api/EfCoreMinimalApiSample.csproj)
- Code: [Program.cs](efcore-minimal-api/Program.cs)
- Requests: [sample.http](efcore-minimal-api/sample.http)
- Docs: [README.md](efcore-minimal-api/README.md)
- Domain: ASP.NET Core 10 Todo HTTP API with a scoped file-backed EF Core context
- Good for: `AddDbContext(...)`, `UseCSharpDb(...)`, startup database creation,
  cancellation-aware CRUD endpoints, generated keys, and persistence across
  host restarts

### ASP.NET Core Authentication & Authorization

- Project: [AspNetCoreIdentitySample.csproj](aspnet-core-identity/AspNetCoreIdentitySample.csproj)
- Code: [Program.cs](aspnet-core-identity/Program.cs)
- Store: [UserStore.cs](aspnet-core-identity/UserStore.cs)
- Seed: [Seed.cs](aspnet-core-identity/Seed.cs)
- Docs: [README.md](aspnet-core-identity/README.md)
- Domain: ASP.NET Core 10 web app with users, roles, claims, lockout, and password hashing
- Good for: cookie + JWT bearer authentication, role-based and policy-based authorization, `PasswordHasher<T>`, and an ADO.NET (`CSharpDB.Data`) custom user/role/claim store with single-column primary keys
- EF note: this custom-store sample is separate from the provider's bounded
  [EF Core Identity profile](../src/CSharpDB.EntityFrameworkCore/README.md#supported-surface),
  which qualifies Identity schema v1 with integer user and role keys.

## Running a Sample

### Option 1: Import through the REST API

Start the API, then import the sample SQL and its companion procedures:

```bash
# 1. Start the API
dotnet run --project src/CSharpDB.Api

# 2. Import schema/data + procedures
# Requires dotnet-script: dotnet tool install -g dotnet-script
dotnet script samples/run-sample.csx -- samples/ecommerce-store
```

The runner targets `http://localhost:61818` by default. Point it at either a sample folder or a specific `schema.sql` file, and it will import the matching `procedures.json` file using create-or-update behavior.

Override the API URL:

```bash
CSHARPDB_API_BASEURL=http://localhost:5000 dotnet script samples/run-sample.csx -- samples/feature-tour
```

Override the procedure file explicitly:

```bash
dotnet script samples/run-sample.csx -- samples/feature-tour/schema.sql samples/feature-tour/procedures.json
```

### Option 2: Load through the CLI

```bash
dotnet run --project src/CSharpDB.Cli -- sample.db

csdb> .read samples/feature-tour/schema.sql
csdb> .read samples/feature-tour/queries.sql
```

This path is ideal for browsing the schema, running the workbook queries, and exploring the system catalogs. Procedure catalogs are still imported through the API/admin workflow.

### Option 3: Load through `CSharpDB.Client`

Use the current recommended client API plus the built-in SQL script splitter:

```csharp
using CSharpDB.Client;
using CSharpDB.Sql;

await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
{
    DataSource = "sample.db",
});

var script = File.ReadAllText("samples/feature-tour/schema.sql");
foreach (var statement in SqlScriptSplitter.SplitExecutableStatements(script))
    await client.ExecuteSqlAsync(statement);
```

You can then run workbook queries the same way:

```csharp
var workbook = File.ReadAllText("samples/feature-tour/queries.sql");
foreach (var statement in SqlScriptSplitter.SplitExecutableStatements(workbook))
    await client.ExecuteSqlAsync(statement);
```

### Option 4: Run the Collection Indexing Sample

```bash
dotnet run --project samples/collection-indexing/CollectionIndexingSample.csproj
```

This sample is the quickest way to see the collection indexing APIs with real seed data and console output.

### Option 5: Run the Generated Collections Sample

```bash
dotnet run --project samples/generated-collections/GeneratedCollectionsSample.csproj
```

This sample is the quickest way to see the source-generated collection API, generated descriptors, and trim/AOT-friendly collection access with real seed data and console output.

### Option 6: Run the CSV Bulk Import Sample

```bash
dotnet run --project samples/csv-bulk-import/CsvBulkImportSample.csproj
```

This sample demonstrates the current best-practice SQL bulk-ingest path on the public API: `UseWriteOptimizedPreset()`, `PrepareInsertBatch(...)`, explicit transaction batching, and index creation after the load. Inspect the generated database with `CSharpDB.Cli` after the import completes.

### Option 7: Run the API-Level Sharding Sample

```bash
dotnet run --project samples/api-level-sharding/ApiLevelShardingSample.csproj
```

This sample creates four shard database files, applies a shared order schema to every shard through the shard-admin surface, prints the shard-admin map/status snapshot, routes recent and older order-history pages by explicit month route key, demonstrates filling a 10-row page from two month routes, demonstrates exact-key month pins, and commits a transaction using only the shard-prefixed transaction id.

### Option 8: Run the Platform Showcase Demo

```bash
dotnet run --project samples/platform-showcase/PlatformShowcaseSample.csproj
```

This demo loads the broad SQL showcase schema, adds a full-text index over the knowledge-base articles, seeds a typed collection of dashboard filters, and prints a few representative queries.

### Option 9: Run The EF Core Provider Sample

```bash
dotnet run --project samples/efcore-provider/EfCoreProviderSample.csproj
```

This sample is the quickest way to validate the embedded EF Core provider, `UseCSharpDb(...)`, navigation loading, and the design-time context setup used by `dotnet ef`.

### Option 10: Run The EF Core Minimal API Sample

```bash
dotnet run --project samples/efcore-minimal-api/EfCoreMinimalApiSample.csproj
```

This ASP.NET Core 10 sample registers the CSharpDB EF Core provider through
dependency injection and exposes cancellation-aware Todo CRUD endpoints. Its
companion `sample.http` file walks through the HTTP flow, and the compatibility
suite verifies persistence across a host restart.

### Option 11: Run The ASP.NET Core Identity Sample

```bash
dotnet run --project samples/aspnet-core-identity/AspNetCoreIdentitySample.csproj
```

This sample brings up an ASP.NET Core 10 web app with a custom `CSharpDB.Data` user store. On first start it seeds an admin (`admin@example.com` / `ChangeMe!2026`) and exposes minimal-API endpoints for cookie login, JWT issuance, role-based authorization, and policy-based authorization. The companion `sample.http` file walks through the full flow.

### Option 12: Run the Fulfillment Hub Sample

```bash
dotnet run --project samples/fulfillment-hub/FulfillmentHubSample.csproj
```

This is the broadest runnable operations sample in the repo. It rebuilds a fresh database, seeds the relational schema and snapshot data, stores procedures and saved queries, persists admin forms and reports, saves and runs pipelines, seeds collections, and creates a full-text index over operational playbooks.

## v2.2.0 API Examples

The SQL samples above cover the relational surface. The following snippets show v2.2.0 features that are accessed through the `CSharpDB.Client` and `CSharpDB.Engine` C# APIs.

### Collection Path Indexing and Queries

Collection path indexes and queries are engine-level APIs on `Database` and `Collection<T>`:

For a complete walkthrough with seed data, index creation, and equality/range query examples, see the [Collection Indexing Guide](https://csharpdb.com/docs/collection-indexing.html).

```csharp
using CSharpDB.Engine;

await using var db = await Database.OpenAsync("sample.db");
var users = await db.GetCollectionAsync<User>("users");

// Create path indexes
await users.EnsureIndexAsync("Email");
await users.EnsureIndexAsync("$.address.city");
await users.EnsureIndexAsync("$.tags[]");
await users.EnsureIndexAsync("$.orders[].sku");

// Query by scalar path
await foreach (var kv in users.FindByPathAsync<string>("$.address.city", "Seattle"))
    Console.WriteLine($"{kv.Key}: {kv.Value}");

// Query by array element path
await foreach (var kv in users.FindByPathAsync<string>("$.tags[]", "premium"))
    Console.WriteLine($"{kv.Key}: {kv.Value}");

// Range query on an ordered text index
await foreach (var kv in users.FindByPathRangeAsync<string>("Email", "a@", "m@"))
    Console.WriteLine($"{kv.Key}: {kv.Value}");
```

### Backup and Restore

Backup and restore are first-class `ICSharpDbClient` operations across Direct, HTTP, gRPC, and CLI:

```csharp
using CSharpDB.Client;
using CSharpDB.Client.Models;

await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
{
    DataSource = "production.db",
});

// Backup
var backupResult = await client.BackupAsync(new BackupRequest
{
    DestinationPath = "backups/production-2026-03-19.db",
});

// Restore
var restoreResult = await client.RestoreAsync(new RestoreRequest
{
    SourcePath = "backups/production-2026-03-19.db",
});
```

## Notes

- The SQL samples are setup scripts, not migrations. Re-running them against the same database will fail on existing objects.
- The API importer is idempotent for procedures (`POST`, then `PUT` on conflict).
- `feature-tour/queries.sql` is intentionally read-only. The `EXEC ...` examples in that file are commented so you can copy them into the Admin Query tab as needed.
- `platform-showcase/queries.sql` follows the same read-only pattern, while `PlatformShowcaseSample.csproj` covers API-only features like full-text search and collections.
- The API uses `Data Source=csharpdb.db` by default (`src/CSharpDB.Api/appsettings.json`).

## Storage Engine Tutorials

The `storage-tutorials/` folder contains a guided learning track for the `CSharpDB.Storage` layer:

- **[architecture.md](storage-tutorials/architecture.md)** — Mental models for Pager, WAL, B+tree, SchemaCatalog
- **[extensibility.md](storage-tutorials/extensibility.md)** — Configuration and extension points
- **[examples/](storage-tutorials/examples/)** — Runnable C# projects covering study examples (VirtualDrive, ConfigStore, EventLog, TaskQueue, GraphStore, StorageInternals) and advanced standalone projects (GraphDB, SpatialIndex, TimeSeries, VirtualFS)

## Native FFI Tutorials

The `native-ffi/` folder contains wrappers and examples for calling CSharpDB from other languages via the NativeAOT C library:

- **[python/](native-ffi/python/)** — ctypes-based wrapper with CRUD and transaction examples
- **[javascript/](native-ffi/javascript/)** — koffi-based Node.js wrapper with CRUD and transaction examples

## See Also

- [Getting Started Tutorial](https://csharpdb.com/docs/getting-started.html)
- [CSharpDB.Client README](../src/CSharpDB.Client/README.md)
- [REST API Reference](https://csharpdb.com/docs/rest-api.html)
- [CLI Reference](https://csharpdb.com/docs/cli.html)
