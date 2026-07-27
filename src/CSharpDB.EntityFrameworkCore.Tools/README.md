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

The current contract accepts one restored, single-target `net10.0` project and
builds its Debug output with `--no-restore`. The context may be specified by
its fully qualified name or by a unique simple name.

### Empty-database scratch proof

Generation analysis remains the default. Add `--scratch` to request the
explicit empty-database execution tier:

```powershell
dotnet csharpdb-ef analyze `
  --project .\MyApp.csproj `
  --context MyApp.Data.AppDbContext `
  --scratch `
  --format json
```

Scratch mode first runs the same generation preflight. A supported non-empty
chain is then executed only against tool-owned private-memory CSharpDB
databases. The tool materializes the expected target model for every prefix,
applies each prefix, compares normalized schema and migration-history digests,
migrates one step down and back up, and twice applies an analyzer-owned guarded
replay built from the retained `Up` command payloads. A complete match reports
`Compatible` with `ScratchExecuted` evidence and exits with code `0`. Its
distinct JSON envelope is
`csharpdb-ef-migration-scratch-analysis/v1`; the embedded generation report
keeps its existing `csharpdb-ef-migration-analysis/v1` format. The nested
scratch proof uses `csharpdb-ef-scratch-chain/v1` and the fixed
`csharpdb-ef-empty-chain/v1` algorithm.

## Trust boundary

Treat the selected project as executable code. Building it and using EF Core's
normal design-time context creation can execute MSBuild targets, module
initializers, an `IDesignTimeDbContextFactory`, application host setup,
constructors, and migration `Up` or `Down` methods.

Application loading and migration inspection run in bounded child processes.
The tool suppresses application console output, enforces time and output limits
plus a 384 MiB managed-heap ceiling across platforms, terminates the child
process tree on cancellation or timeout, and adds a process/job memory limit on
Windows. It never accepts a connection string and never asks the provider to
open, migrate, or otherwise modify the configured database. Scratch mode
executes only retained, preflighted migration SQL against
`Data Source=:memory:;Pooling=false`; it does not use the selected context's
configured database. Application code can still perform its own side effects,
so analyze only projects you trust.

## Evidence tiers

The analyzer:

- verifies that the design-time context uses `CSharpDB.EntityFrameworkCore`;
- rejects contexts that replace EF/provider services, supply a custom internal
  service provider, or add nonstandard options extensions, because their
  configured migration path is outside this proof;
- creates compiled migrations with that provider active, including
  provider-conditional branches;
- inspects ordered `Up` and `Down` operations and destructive flags;
- applies a bounded schema-operation allowlist;
- invokes CSharpDB's actual `IMigrationsSqlGenerator`;
- sends raw DDL operations through the bounded CSharpDB DDL checker;
- emits only fixed rule IDs, bounded metadata, and digests—never generated SQL
  or raw exception text.

Without `--scratch`, a successfully generated chain remains `Conditional` with
`Bound` evidence and exits with code `1`. This default is unchanged and makes
no execution claim.

The opt-in scratch tier is deliberately an **empty-database proof**.
`dataPreflightCompleted` is always `false`: no existing rows are present, so it
cannot prove that conversions, required-column additions, or other data-shaped
changes are safe for production data. Private-memory execution also does not
prove file or WAL persistence, recovery, or durability. It does not invoke the
configured `IMigrator` or `IMigrator.GenerateScript`, and its guarded replay
does not qualify EF-generated idempotent scripts, configured migration-history
behavior, migration locks, command or connection interceptors, or
application-specific migration or deployment orchestration. Validate those
surfaces separately before production rollout.

Exit codes:

- `0`: the requested scratch chain passed empty-database execution proof;
- `1`: a `Conditional` result—either generation-only evidence or a scratch
  request blocked after an otherwise `Conditional` generation preflight;
- `2`: unsupported or unknown analysis, failed scratch execution, a scratch
  request blocked by an unsupported or unknown preflight, timeout, or invalid
  analysis;
- `64`: command usage error;
- `130`: user cancellation.
