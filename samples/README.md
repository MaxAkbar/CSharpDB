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
| `collection-indexing/` | Runnable `Collection<T>` indexing walkthrough | `.csproj`, `Program.cs`, `README.md` |
| `generated-collections/` | Runnable source-generated collection fast-path walkthrough | `.csproj`, `Program.cs`, `README.md` |

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

### Collection Indexing Walkthrough

- Project: [CollectionIndexingSample.csproj](collection-indexing/CollectionIndexingSample.csproj)
- Code: [Program.cs](collection-indexing/Program.cs)
- Docs: [Collection Indexing Guide](https://csharpdb.com/docs/collection-indexing.html)
- Domain: typed user documents with nested address, tags, and orders
- Good for: `GetCollectionAsync<T>()`, `EnsureIndexAsync(...)`, `FindByIndexAsync(...)`, `FindByPathAsync(...)`, and `FindByPathRangeAsync(...)`

### Generated Collections Walkthrough

- Project: [GeneratedCollectionsSample.csproj](generated-collections/GeneratedCollectionsSample.csproj)
- Code: [Program.cs](generated-collections/Program.cs)
- Docs: [README.md](generated-collections/README.md)
- Domain: generated customer documents with nested address, tags, and orders
- Good for: `GetGeneratedCollectionAsync<T>()`, generated `CollectionField<,>` descriptors, `JsonPropertyName` payload compatibility, and trim/AOT-friendly collection access

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

### Option 6: Run the Platform Showcase Demo

```bash
dotnet run --project samples/platform-showcase/PlatformShowcaseSample.csproj
```

This demo loads the broad SQL showcase schema, adds a full-text index over the knowledge-base articles, seeds a typed collection of dashboard filters, and prints a few representative queries.

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

- [Getting Started Tutorial](../docs/getting-started.md)
- [CSharpDB.Client README](../src/CSharpDB.Client/README.md)
- [REST API Reference](../docs/rest-api.md)
- [CLI Reference](../docs/cli.md)
