# Samples

This folder contains realistic datasets, a focused query/statistics workbook sample, and a full-fidelity fictitious company example for the current CSharpDB surface area. Each sample now lives in its own folder with a conventional layout: `schema.sql` for setup, `procedures.json` for API/admin procedure import, and `queries.sql` for optional read-only workbook queries.

Each sample folder also includes a small `README.md` with domain notes, key features, and suggested starting points.

## Available Samples

| Sample Folder | Focus | Includes |
|---------------|-------|----------|
| `ecommerce-store/` | Online retail | `schema.sql`, `procedures.json` |
| `medical-clinic/` | Healthcare scheduling + billing | `schema.sql`, `procedures.json` |
| `school-district/` | Education + attendance | `schema.sql`, `procedures.json` |
| `procurement-analytics/` | Query expansion + planner stats workbook | `schema.sql`, `procedures.json`, `queries.sql` |
| `feature-tour/` | Northstar Field Services | `schema.sql`, `procedures.json`, `queries.sql` |

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

## Notes

- The SQL samples are setup scripts, not migrations. Re-running them against the same database will fail on existing objects.
- The API importer is idempotent for procedures (`POST`, then `PUT` on conflict).
- `feature-tour/queries.sql` is intentionally read-only. The `EXEC ...` examples in that file are commented so you can copy them into the Admin Query tab as needed.
- The API uses `Data Source=csharpdb.db` by default (`src/CSharpDB.Api/appsettings.json`).

## See Also

- [Getting Started Tutorial](../docs/getting-started.md)
- [CSharpDB.Client README](../src/CSharpDB.Client/README.md)
- [REST API Reference](../docs/rest-api.md)
- [CLI Reference](../docs/cli.md)
