# CSharpDB EF Core Migration Analyzer

`csharpdb-ef` inspects a compiled EF Core migration chain with the real
CSharpDB provider services. It is a separate .NET tool so EF Core design-time
dependencies and application loading do not become part of the base
`csharpdb` command.

## Install

Install the tool globally:

```powershell
dotnet tool install --global CSharpDB.EntityFrameworkCore.Tools
```

You can instead add the same package to a repository tool manifest. Both
install forms expose the `dotnet csharpdb-ef` subcommand.

## Analyze a project

Restore the project first, then run:

```powershell
dotnet csharpdb-ef analyze `
  --project .\MyApp.csproj `
  --context MyApp.Data.AppDbContext
```

Use `--format json` for a deterministic machine-readable report.

The initial contract accepts one restored, single-target `net10.0` project and
builds its Debug output with `--no-restore`. The context may be specified by
its fully qualified name or by a unique simple name.

## Trust boundary

Treat the selected project as executable code. Building it and using EF Core's
normal design-time context creation can execute MSBuild targets, module
initializers, an `IDesignTimeDbContextFactory`, application host setup,
constructors, and migration `Up` or `Down` methods.

Application loading and migration inspection run in bounded child processes.
The tool suppresses application console output, enforces time and output
limits, terminates the child process tree on cancellation or timeout, and
applies a process-memory limit on Windows. It never accepts a connection
string and never asks the provider to open, migrate, or otherwise modify the
configured database. Application code can still perform its own side effects,
so analyze only projects you trust.

## Evidence in the first release

The analyzer:

- verifies that the design-time context uses `CSharpDB.EntityFrameworkCore`;
- creates compiled migrations with that provider active, including
  provider-conditional branches;
- inspects ordered `Up` and `Down` operations and destructive flags;
- applies a bounded schema-operation allowlist;
- invokes CSharpDB's actual `IMigrationsSqlGenerator`;
- sends raw DDL operations through the bounded CSharpDB DDL checker;
- emits only fixed rule IDs, bounded metadata, and digests—never generated SQL
  or raw exception text.

This tier is generation-only. A successfully generated chain is
`Conditional` with `Bound` evidence and exits with code `1`; it is not reported
as fully compatible until a later tier executes and compares every migration
prefix in an isolated scratch database.

Exit codes:

- `1`: a valid generation-only report that still requires scratch proof;
- `2`: unsupported, unknown, failed, timed out, or invalid analysis;
- `64`: command usage error;
- `130`: user cancellation.
