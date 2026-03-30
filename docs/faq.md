# CSharpDB FAQ

## General

### What is CSharpDB?

CSharpDB is an embedded database engine written in C# for .NET. It runs in-process, stores data in a single `.db` file, and uses a WAL file for durability and crash recovery.

### Does CSharpDB require a server process?

No. You open the database file directly from your application using the Engine API or ADO.NET provider.

### Which .NET version is supported?

Current projects target `.NET 10` (`net10.0`).

## SQL and Schema

### Do SQL scripts need `GO` separators?

No. Use semicolon-terminated SQL statements (`;`). `GO` is not required.

### Can I query system metadata using SQL?

Yes. Query virtual catalog sources:

- `sys.tables`
- `sys.columns`
- `sys.indexes`
- `sys.foreign_keys`
- `sys.views`
- `sys.triggers`
- `sys.objects`

Underscored aliases are also supported (`sys_tables`, `sys_columns`, etc.).

### Do `sys.*` objects appear as normal tables in the Admin sidebar?

No. They are virtual system catalog sources, so use the Query tab to query them.

### Why does index creation fail on some column types?

Current SQL secondary indexes support `INTEGER` and `TEXT` columns. `COLLATE` is only valid on `TEXT` index columns.

### Are foreign keys enforced?

Yes. CSharpDB currently supports single-column, column-level `REFERENCES` constraints with optional `ON DELETE CASCADE`.

Current v1 boundary:

- Constraints are enforced immediately.
- `NULL` child values are allowed.
- Table-level/composite foreign keys are not implemented yet.
- `ON UPDATE`, `SET NULL`, `SET DEFAULT`, and deferred constraints are not implemented yet.

## Admin, CLI, and API

### How do I run a sample SQL file quickly?

Use the CLI:

```bash
dotnet run --project src/CSharpDB.Cli -- mydata.db
```

Then:

```text
csdb> .read samples/ecommerce-store/schema.sql
```

### Can the Admin Query tab run multi-statement SQL?

Yes. Multiple semicolon-delimited statements are supported.

### How do I report bugs or request features?

Use GitHub issue templates:

- Bug report
- Feature request
- Documentation
- Question

Security issues should be reported privately through GitHub Security Advisories.

## Development

### How do I build the repo?

```bash
dotnet build CSharpDB.slnx
```

### How do I run tests?

This repository uses executable xUnit projects. Run tests with:

```bash
dotnet run --project tests/CSharpDB.Tests/CSharpDB.Tests.csproj --
dotnet run --project tests/CSharpDB.Data.Tests/CSharpDB.Data.Tests.csproj --
dotnet run --project tests/CSharpDB.Cli.Tests/CSharpDB.Cli.Tests.csproj --
```

### Why do I see a `.wal` file next to my database?

That is expected. The WAL file stores recent committed changes before checkpointing into the main `.db` file.

Lifecycle details:

- While the database is open, the `.wal` file exists.
- A checkpoint copies committed pages back into the `.db` file and resets/truncates the WAL, but does not delete it while the DB is still open.
- On a clean close/dispose, CSharpDB performs a final checkpoint (when needed) and deletes the `.wal` file.
- If the process crashes or is terminated, the `.wal` file can remain on disk. On next open, CSharpDB recovers from it and continues normally.
