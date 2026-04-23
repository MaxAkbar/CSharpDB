# CSharpDB.Cli

Command-line shell and maintenance tool for CSharpDB.

The built executable is named `csharpdb`. It can open a local database directly
or connect to a configured CSharpDB host through `CSharpDB.Client`.

## Usage

Interactive shell with the default local database:

```powershell
dotnet run --project src/CSharpDB.Cli/CSharpDB.Cli.csproj
```

Interactive shell for an explicit database path:

```powershell
dotnet run --project src/CSharpDB.Cli/CSharpDB.Cli.csproj -- C:\data\app.db
```

Remote gRPC daemon:

```powershell
dotnet run --project src/CSharpDB.Cli/CSharpDB.Cli.csproj -- --transport grpc --endpoint http://localhost:5820
```

Supported shell target syntax:

```text
csharpdb [database-path] [--endpoint <uri>] [--transport <direct|http|grpc|namedpipes>]
```

`NamedPipes` is parsed for consistency with the shared client transport enum,
but it is not implemented end to end.

## Interactive Commands

SQL statements are entered directly and terminated with `;`. Multi-line input,
multi-statement lines, and trigger bodies are supported.

Built-in meta commands:

| Command | Description |
| --- | --- |
| `.help` | Show shell help. |
| `.info` | Show database and runtime status. |
| `.tables [PATTERN|--all]` | List tables. |
| `.schema [TABLE|--all]` | Show table DDL. |
| `.indexes [TABLE]` | List indexes. |
| `.views` / `.view <NAME>` | List views or show one view definition. |
| `.triggers [TABLE]` / `.trigger <NAME>` | List triggers or show one trigger definition. |
| `.collections` | List document collections. |
| `.begin`, `.commit`, `.rollback` | Manage an explicit transaction. |
| `.checkpoint` | Flush WAL pages to the main database file. |
| `.backup <FILE> [--with-manifest]` | Write a committed snapshot backup. |
| `.restore <FILE> [--validate-only]` | Validate or restore a database snapshot. |
| `.migrate-fks <SPEC.json> [--validate-only] [--backup <FILE>]` | Validate or retrofit foreign keys. |
| `.reindex [--all|--table <name>|--index <name>] [--force-corrupt-rebuild]` | Rebuild indexes. |
| `.vacuum` | Rewrite the database file to reclaim free pages. |
| `.snapshot [on|off|status]` | Toggle read-only snapshot mode for SELECT. |
| `.syncpoint [on|off|status]` | Toggle sync point lookup fast path. |
| `.timing [on|off|status]` | Toggle query timing output. |
| `.read <FILE>` | Execute SQL statements from a script file. |

## Non-Interactive Commands

Storage inspection:

```powershell
csharpdb inspect <dbfile> [--json] [--out <file>] [--include-pages]
csharpdb inspect-page <dbfile> <pageId> [--json] [--hex]
csharpdb check-wal <dbfile> [--json]
csharpdb check-indexes <dbfile> [--index <name>] [--sample <n>] [--json]
```

Maintenance:

```powershell
csharpdb maintenance-report <dbfile> [--json]
csharpdb migrate-foreign-keys <dbfile> --spec <json-file> [--validate-only] [--backup <file>] [--json]
csharpdb reindex <dbfile> [--all|--table <name>|--index <name>] [--force-corrupt-rebuild] [--json]
csharpdb vacuum <dbfile> [--json]
```

ETL pipelines:

```powershell
csharpdb etl <validate|dry-run|run> <dbfile> <packagefile> [--json]
csharpdb etl list <dbfile> [--json]
csharpdb etl <status|run-package|rejects|resume> <dbfile> <runId> [--json]
csharpdb etl <pipelines|revisions|import|export|export-revision|delete|run-stored> ...
```

## Project Layout

- `Program.cs` - command dispatch and shell startup
- `CliShellOptions.cs` - target and transport parsing
- `Repl.cs` - interactive SQL shell
- `MetaCommands.cs` - dot-command implementation
- `InspectorCommandRunner.cs` - storage inspection commands
- `MaintenanceCommandRunner.cs` - maintenance commands
- `PipelineCommandRunner.cs` - ETL package and catalog commands
- `CliConsole.cs` and `TableFormatter.cs` - terminal formatting helpers

## Build And Test

```powershell
dotnet build src/CSharpDB.Cli/CSharpDB.Cli.csproj
dotnet test tests/CSharpDB.Cli.Tests/CSharpDB.Cli.Tests.csproj
```

## Dependencies

- `CSharpDB.Client`
- `CSharpDB.Engine`
- `CSharpDB.Sql`
- `CSharpDB.Storage.Diagnostics`
- `Spectre.Console`
