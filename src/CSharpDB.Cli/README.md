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

Database DevOps:

```powershell
csharpdb compare schema <source> <target> [--json] [--script-out <file>]
csharpdb compare data <source> <target> --table <name> [--key <columns>] [--json] [--script-out <file>] [--max-preview <n>]
csharpdb drift <dbfile> --baseline <archive-or-dbfile> [--table <name>] [--key <columns>] [--json]
```

The compare commands accept existing database files and `.csdbtable` archives.
Schema compare reports source-to-target structural differences. Data compare
uses a primary key by default or an explicit `--key` list for tables without a
stable primary key. Script output is preview-only and should be reviewed before
execution. The drift command returns a warning exit code when differences are
found so CI can fail on drift.

ETL pipelines:

```powershell
csharpdb etl <validate|dry-run|run> <dbfile> <packagefile> [--json]
csharpdb etl list <dbfile> [--json]
csharpdb etl <status|run-package|rejects|resume> <dbfile> <runId> [--json]
csharpdb etl <pipelines|revisions|import|export|export-revision|delete|run-stored> ...
```

Migration planning proof surface:

```powershell
csharpdb migrate inspect --source synthetic --out <catalog.json>
csharpdb migrate inspect --source csv --input <source.csv> --package <source.csdbcsv> --out <catalog.json> [--delimiter auto|comma|semicolon|tab|pipe|<character>] [--no-header]
csharpdb migrate plan <catalog.json> --out <plan.json> [--profile preserve|queryable] [--accept-exclusions all|<id,...>] [--accept-diagnostics <id,...>]
csharpdb migrate preview <plan.json> --catalog <catalog.json> [--format text|json]
csharpdb migrate apply <plan.json> --catalog <catalog.json> --source-package <source.csdbcsv> --expected-manifest-digest <sha256:...> --target <staged.csdb> --out <run.json> [--resume] [--format text|json]
csharpdb migrate validate <plan.json> --catalog <catalog.json> --source-package <source.csdbcsv> --expected-manifest-digest <sha256:...> --target <staged.csdb> --out <validation.json> [--level schema|count|checksum] [--spill-dir <directory>] [--format text|json]
```

Inspection supports both the immutable synthetic qualification source and a
strict CSV source. CSV inspection freezes the raw bytes and complete reader and
inference policy into one no-overwrite `.csdbcsv` package, writes the normal
catalog artifact, and prints `manifestDigest=sha256:...`. Retain that digest in
an independently trusted change record or CI parameter; CSV apply, resume, and
validation require it through `--expected-manifest-digest`. The original CSV
path is not retained and is never reopened after inspection. Common delimiter
detection is automatic; `--delimiter` supplies the only candidate when an
explicit convention is required. CSV defaults are strict UTF-8 with BOM
detection, a header row, invariant culture, and no null token.
The package parent and any explicit workspace must already exist and remain
caller-controlled and cannot themselves be links, junctions, reparse points,
or devices. CSV collision checks resolve link aliases in ancestor components
before comparing input, package, catalog, plan, target, and report roles.

These commands produce digested deterministic planning artifacts and apply an
explicitly approved plan to a new staged database. Apply never overwrites or
activates an existing target. Before target creation, CSV execution verifies
the exact package-manifest digest and reconstructs the catalog, source fingerprint,
snapshot identity, parser policy, and inference recipe. Rows and receipts
commit together; `--resume` replays the same source snapshot and skips only
batches whose identities and digests match exactly.
Successful execution stops at `awaitingValidation` and writes a derived run
report that contains no source values or resume cursors. Phase 2 uses the
versioned `csharpdb-migration-fail-fast/v1` contract: the first invalid value
stops the load before its batch reaches the target, and the failure report
contains only its stable code plus object, batch, row, and column coordinates.
Plans requesting durable skip-and-record rejects are refused before a staged
target is created.

`migrate validate` compares normalized schema, 64-bit counts, and—by
default—partitioned canonical SHA-256 evidence. It writes a deterministic,
self-digesting JSON audit report and prints either a compact text summary or
the JSON report. Validation uses bounded temporary spill space. Only an
established, passing result whose report is successfully published can activate
the staged database; differences, errors, or unavailable consistency leave it
unactivated. Repeating the same validation/report path is idempotent, while a
different existing report is never overwritten.

## Project Layout

- `Program.cs` - command dispatch and shell startup
- `CliShellOptions.cs` - target and transport parsing
- `Repl.cs` - interactive SQL shell
- `MetaCommands.cs` - dot-command implementation
- `InspectorCommandRunner.cs` - storage inspection commands
- `MaintenanceCommandRunner.cs` - maintenance commands
- `DevOpsCommandRunner.cs` - schema compare commands
- `PipelineCommandRunner.cs` - ETL package and catalog commands
- `MigrationCommandRunner.cs` - migration inspect, plan, preview, apply, resume, and validate commands
- `CliConsole.cs` and `TableFormatter.cs` - terminal formatting helpers

## Build And Test

```powershell
dotnet build src/CSharpDB.Cli/CSharpDB.Cli.csproj
dotnet test tests/CSharpDB.Cli.Tests/CSharpDB.Cli.Tests.csproj
```

## Dependencies

- `CSharpDB.Client`
- `CSharpDB.DevOps`
- `CSharpDB.Engine`
- `CSharpDB.Migration.Files`
- `CSharpDB.Sql`
- `CSharpDB.Storage.Diagnostics`
- `Spectre.Console`
