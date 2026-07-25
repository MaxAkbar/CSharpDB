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

Migration and retained table-export surface:

```powershell
csharpdb migrate inspect --source synthetic --out <catalog.json>
csharpdb migrate inspect --source csv --input <source.csv> --package <source.csdbcsv> --out <catalog.json> [--delimiter auto|comma|semicolon|tab|pipe|<character>] [--no-header]
csharpdb migrate inspect --source json --input <source.json|source.ndjson> --package <source.csdbjson> --out <catalog.json> [--framing root-array|ndjson] [--table <name>] [--sample-rows <count>] [--source-id <label>] [--workspace <directory>] [--max-source-bytes <count>]
csharpdb migrate inspect --source json --input <source.json|source.ndjson> --typed-intent <source.csdbjson-intent.json> --expected-intent-manifest-digest <sha256:...> --package <source.csdbjson> --out <catalog.json> [--framing root-array|ndjson] [--table <name>] [--sample-rows <count>] [--source-id <label>] [--workspace <directory>] [--max-source-bytes <count>]
csharpdb migrate inspect --source sqlite --input <source.db> --package <source.csdbsqlite> --out <catalog.json> [--profile-sample-size <count>] [--max-source-bytes <count>]
csharpdb migrate inspect --source sqlserver --connection-env <name> --out <catalog.json>
csharpdb migrate plan <catalog.json> --out <plan.json> [--profile preserve|queryable] [--accept-exclusions all|<id,...>] [--accept-diagnostics <id,...>] [--reject-mode fail-fast|deterministic --reject-rules all|<id,...> --max-rejected-rows-per-batch <count> --max-rejected-rows-per-run <count> --max-reject-evidence-value-bytes <count> --max-reject-evidence-bytes-per-batch <count> --max-reject-evidence-bytes-per-run <count> --max-reject-artifact-bytes <count>]
csharpdb migrate preview <plan.json> --catalog <catalog.json> [--ddl|--scratch] [--format text|json]
csharpdb migrate apply <plan.json> --catalog <catalog.json> --source-package <source.csdbcsv|source.csdbjson|source.csdbsqlite> --expected-manifest-digest <sha256:...> --target <staged.csdb> --out <run.json> [--resume] [--allow-deterministic-rejects --reject-artifact <absolute-normalized-rejects.jsonl>] [--format text|json]
csharpdb migrate validate <plan.json> --catalog <catalog.json> --source-package <source.csdbcsv|source.csdbjson|source.csdbsqlite> --expected-manifest-digest <sha256:...> --target <staged.csdb> --out <validation.json> [--level schema|count|checksum] [--spill-dir <directory>] [--allow-deterministic-rejects --reject-artifact <absolute-normalized-rejects.jsonl>] [--format text|json]
csharpdb migrate export <retained-snapshot.db> --format csv --table <physical-table> --out <table.csv> --manifest <table.manifest.json> --expected-snapshot-identity <csharpdb-retained-snapshot/v1:<bytes>:sha256:<64-lowercase-hex>> [--profile lossless-v1|spreadsheet-safe-lossy-v1] [--max-data-bytes <positive-int64>] [--max-decoded-blob-bytes <positive-int32>] [--checkpoint-row-interval <positive-int64>] [--json]
csharpdb migrate export <retained-snapshot.db> --format json|ndjson --table <physical-table> --out <table.json|table.ndjson> --manifest <table.manifest.json> --expected-snapshot-identity <csharpdb-retained-snapshot/v1:<bytes>:sha256:<64-lowercase-hex>> [--profile lossless-v1] [--max-data-bytes <positive-int64>] [--max-decoded-blob-bytes <positive-int32>] [--checkpoint-row-interval <positive-int64>] [--json]
```

Inspection supports the immutable synthetic qualification source, strict CSV,
untyped retained JSON package v1, explicitly selected typed JSON package v2,
SQLite, and schema-only SQL Server readiness analysis. CSV inspection freezes
the raw bytes and complete reader and
inference policy into one no-overwrite `.csdbcsv` package. JSON inspection does
the same for root-array JSON or NDJSON-compatible whitespace-separated
top-level values in one no-overwrite `.csdbjson` package. Both JSON package
versions use that extension; the catalog's versioned schema facets select the
reopen API, never the filename or package contents. SQLite inspection creates a
coherent online backup in one no-overwrite `.csdbsqlite` package and binds its
native catalog plus sampled storage-class profile to the package SHA-256.
SQLite backup creation is incrementally cancellable and defaults to a 1 TiB
retained-snapshot ceiling; `--max-source-bytes` can select a smaller bound.

SQL Server inspection accepts only the name of an environment variable through
`--connection-env`; a raw connection string is never a command-line argument.
The resolved value is held in memory, is not written to the catalog or command
output, and should identify a dedicated least-privilege, read-only metadata
login. The catalog destination uses create-new publication and is never
overwritten. This checkpoint inventories and analyzes schema only: it neither
reads application rows nor writes to SQL Server or a CSharpDB target.
For migration commands, the first Ctrl+C requests cooperative cancellation and
the second retains the console's normal force-termination behavior if a bounded
provider operation has not returned.

Pass both `--typed-intent` and
`--expected-intent-manifest-digest` to select v2. The canonical sidecar must
already exist and must be bound to the exact input bytes, framing, source ID,
and reader defaults used by the command. The CLI does not author or
automatically discover typed sidecars. Typed inspection embeds the exact
verified sidecar in package v2, so the original JSON and standalone sidecar can
both be removed after successful publication.

Inspection writes the normal catalog artifact and prints
`manifestDigest=sha256:...`; typed inspection also reports the embedded
`intentManifestDigest=sha256:...`. Retain package and sidecar pins in an
independently trusted change record or CI parameter. Apply, resume, and
validation require the package pin through `--expected-manifest-digest`.
The original CSV, JSON, or live SQLite path is not retained and is never
reopened after inspection.

Planning accepts a strict UTF-8 catalog artifact of at most 64 MiB. Before a
plan is published, the CLI renders the selected CSharpDB schema actions once
under the production action and SQL-size ceilings and stores their lowercase
SHA-256 digest in the plan. Rendered SQL is not stored. A cancellation, input
limit, render limit, or sealing failure occurs before plan publication and
does not replace an existing output. The serialized plan is also capped at
64 MiB so every published plan remains consumable by the explicit preview
path. Legacy plans without this optional binding remain readable, but every
plan authored by this CLI includes it.

SQLite Tier 1 replays visible scalar columns from ordinary UTF-8 rowid tables in
signed rowid order. It preserves null, integer, finite real, UTF-8 text, and
BLOB values exactly, validates every emitted resume boundary, and supports
fail-fast plans only. Views, triggers, generated columns, virtual/internal
tables, `WITHOUT ROWID`, non-rowid primary keys, partial or expression indexes,
unresolved or violating foreign keys, and other unsupported semantics remain
visible as nonoverrideable catalog diagnostics or exclusions. Apply and
validation copy the pinned package into an owner-private workspace, reconstruct
the exact catalog before target access, and never read the deleted or changed
live source path.

Supported migration-source matrix:

| Source | Retained consistency boundary | Schema/data route | Reject policy |
| --- | --- | --- | --- |
| CSV | Private byte-for-byte snapshot in `.csdbcsv` | Strict tabular inference and streaming | Fail-fast or the fixed CSV deterministic registry |
| JSON/NDJSON v1 | Private byte-for-byte snapshot in `.csdbjson` | Untyped table inference and streaming | Fail-fast or the fixed untyped-JSON deterministic registry |
| Typed JSON v2 | Source- and intent-pinned `.csdbjson` | Explicit typed table contract | Fail-fast |
| SQLite v1 | Coherent online backup in `.csdbsqlite` | Tier 1 native catalog and rowid streaming | Fail-fast |
| SQL Server readiness | Live best-effort, schema-only metadata inspection | Inventory, compatibility, planning, and target DDL assurance only | No data route |

SQLite catalogs record the adapter, Microsoft.Data.Sqlite assembly, native
SQLite engine, compile-option digest, database text encoding, profile coverage,
and content fingerprint used for the run. This repository qualifies the
provider version selected by `CSharpDbQualifiedEfCoreVersion` together with the
pinned SQLitePCLRaw bundle; a catalog produced by a different build remains
explicitly versioned evidence, not an implied compatibility claim.

Example SQLite workflow:

```powershell
csharpdb migrate inspect --source sqlite --input .\source.db --package .\source.csdbsqlite --out .\catalog.json
# Record the printed manifestDigest in trusted change control.
csharpdb migrate plan .\catalog.json --out .\plan.json --accept-exclusions all
csharpdb migrate preview .\plan.json --catalog .\catalog.json
csharpdb migrate apply .\plan.json --catalog .\catalog.json --source-package .\source.csdbsqlite --expected-manifest-digest <recorded-sha256> --target .\staged.csdb --out .\run.json
csharpdb migrate validate .\plan.json --catalog .\catalog.json --source-package .\source.csdbsqlite --expected-manifest-digest <recorded-sha256> --target .\staged.csdb --out .\validation.json --level checksum
```

Example SQL Server schema-readiness workflow:

```powershell
# Populate CSHARPDB_SQLSERVER_SOURCE through your secret manager or protected
# process environment; do not put the connection string in scripts.
csharpdb migrate inspect --source sqlserver --connection-env CSHARPDB_SQLSERVER_SOURCE --out .\catalog.json
csharpdb migrate plan .\catalog.json --out .\plan.json
csharpdb migrate preview .\plan.json --catalog .\catalog.json
csharpdb migrate preview .\plan.json --catalog .\catalog.json --ddl
csharpdb migrate preview .\plan.json --catalog .\catalog.json --scratch --format json
```

The default preview remains the compact
`csharpdb-migration-preview/v1` planning summary. `--ddl` is the only mode that
prints exact target SQL and typed collection actions. `--scratch` instead
prints the sanitized `csharpdb-ddl-scratch-validation/v1` report containing
digests, stable rule/action identifiers, counts, and evidence level, but no SQL
or object names. The modes are mutually exclusive. A successful scratch run
does not approve exclusions, clear diagnostics, establish source semantic
equivalence, or make a blocked SQL Server plan ready.
Each catalog and plan consumed by the explicit `--ddl` or `--scratch` path is
limited to 64 MiB before contract validation; rendered action count,
per-action SQL, and aggregate SQL bytes have separate fixed production
ceilings. The regenerated DDL digest must match the binding in a sealed plan.

The SQL Server project reference currently belongs to this non-packable proof
CLI. Optional adapter/package isolation and published-runtime qualification
remain required before this surface can be treated as a shipping connector.

Common CSV delimiter detection is automatic; `--delimiter` supplies the only
candidate when an explicit convention is required. CSV defaults are strict
UTF-8 with BOM detection, a header row, invariant culture, and no null token.
JSON defaults are `root-array` framing, table `json_data`, 1,000 type-profile
sample rows, and the retained JSON snapshot's default source-size ceiling.
Select `--framing ndjson` for that multiple-value mode; line breaks are
conventional, not required by the reader. Untyped package v1 supports fail-fast
and source-aware deterministic-reject plans. Typed package v2 currently
supports fail-fast CLI plans only; its typed deterministic-reject registry is a
separate qualification slice.

The package parent and any explicit workspace must already exist and remain
caller-controlled and cannot themselves be links, junctions, reparse points,
or devices. Source-package collision checks resolve link aliases in ancestor
components before comparing input, package, catalog, plan, target, and report
roles.

CSV export accepts only an already retained CSharpDB snapshot. Capture
and pin its canonical identity independently before invoking the command; the
CLI does not turn a live database path into a retained source or derive a trust
decision from the path alone. The default profile is `lossless-v1`.
`spreadsheet-safe-lossy-v1` is an explicit value-changing mitigation profile.
The output CSV and manifest must be distinct siblings in one trusted,
caller-controlled local Windows directory. UNC or mapped-network output paths,
output links, junctions, reparse points, and devices, and non-Windows
publication fail closed.

The exact same export command is also the resume command. A rerun requalifies
the retained snapshot identity and private checkpoint journal, resumes only at
a verified complete-row boundary, recovers an exact CSV-only publication, or
reuses an exact completed CSV/manifest pair. Different or unsafe existing
files are never overwritten or repaired. Text output reports the final paths,
row and byte counts, content digests, and whether the CSV or manifest was
reused; `--json` emits the same result as structured JSON.

JSON and NDJSON export use the same independently pinned retained-snapshot
boundary and durable resume workflow. `--format json` writes one compact root
array and `--format ndjson` writes one compact object plus LF per row. The
exact same command is the resume command: a rerun requalifies the retained
snapshot identity, physical table schema, private prepared data, and checkpoint
journal, resumes only at a verified complete-row boundary, recovers an exact
data-only publication, or reuses an exact completed data/manifest pair.
A same-binding exact pair created by the restart-only route is
source-requalified and bootstrapped into durable prepared/checkpoint authority
on its first resumable rerun. Reader/source-version binding changes fail closed
rather than adopting an older manifest implicitly. Different or unsafe
existing files are never overwritten or repaired.

`--checkpoint-row-interval` defaults to 10,000 rows and must be a positive
64-bit integer. Successful JSON/NDJSON exports preserve current-owner-only
`.csharpdb-json-export-*` prepared-data and checkpoint siblings so a later
process can requalify and resume them. The local Windows publisher requires the
same protected ACL on reusable final files. UNC and mapped-network paths,
non-Windows publication, links, aliases, special files, unsafe directory
chains, manifest-only states, and mismatched pairs fail closed. The private
prefix cannot be selected for a source, final data, or manifest leaf. The
`spreadsheet-safe-lossy-v1` profile remains CSV-only. File content and
checkpoints are flushed to durable storage as supported, with no directory-fsync
or hard-power guarantee. The structured-status `--json` flag remains valid
with both JSON data formats.

Fail-fast is the default: omitting `--reject-mode` produces the established
`csharpdb-migration-fail-fast/v1` plan JSON and digest. Retained CSV and
untyped retained JSON package v1 can opt into `--reject-mode deterministic`;
typed JSON v2 and SQLite currently remain fail-fast only.
That mode requires `--reject-rules` plus all six positive, base-10 limits shown
above. `--reject-rules all` expands to a source-specific fixed set. CSV uses
`MIG-CSV-DATA-MISSING-001`, `MIG-CSV-DATA-NULL-001`, and
`MIG-CSV-DATA-TYPE-001`. Untyped JSON v1 uses
`MIG-JSON-DATA-MISSING-001`, `MIG-JSON-DATA-NULL-001`,
`MIG-JSON-DATA-ROW-001`, and `MIG-JSON-DATA-TYPE-001`. An explicit
comma-separated value may select a nonempty subset of the selected source's
registry. The expanded rules and all six limits are stored in the plan and
therefore change its digest. They cannot be supplied to a fail-fast plan.

Selecting a reject rule does not waive catalog readiness. Known JSON
structural defects remain blocking inspection diagnostics; the retained-v1 CLI
path is end-to-end qualified for sampled schemas whose later rows produce
row-local type mismatches. Typed JSON package v2 is available through the
fail-fast route, while its deterministic registry and
`MIG-JSON-DATA-TYPED-001` remain outside the CLI reject route.

Apply, `apply --resume`, and validate require a second explicit opt-in for a
deterministic plan: both `--allow-deterministic-rejects` and
`--reject-artifact <absolute-normalized-rejects.jsonl>`. They are rejected for
a fail-fast plan. The artifact parent must already exist, be stable and
caller-controlled, and contain no link, junction, reparse-point, device, or
traversal aliases. Publication is owner-private, atomic, and no-overwrite. An
existing artifact is reused only when every byte matches the newly projected
canonical artifact; a different existing file is preserved and the operation
fails.

These commands produce digested deterministic planning artifacts and apply an
explicitly approved plan to a new staged database. Apply never overwrites or
activates an existing target. Before target creation, retained CSV, JSON, or
SQLite execution verifies the exact package digest and reconstructs the
catalog, source fingerprint, snapshot identity, and inspection recipe. Rows and
receipts commit together; `--resume` replays the same source snapshot and skips
only batches whose identities and digests match exactly.
Successful execution stops at `awaitingValidation` and writes a derived run
report that contains no source values or resume cursors. In fail-fast mode the
first invalid value stops the load before its batch reaches the target, and the
failure report contains only its stable code plus object, batch, row, and column
coordinates. In deterministic mode accepted rows, rejects, and each v2 receipt
commit atomically. Apply materializes and verifies the required reject artifact
before publishing the successful run report. The report and console expose only
safe aggregates and bindings—such as reject count, artifact digest, byte count,
and exact-reuse status—never rejected values. A successful apply or validation
that contains rejects returns the warning exit code so automation cannot mistake
skip-and-record outcomes for a clean strict run.

`migrate validate` compares normalized schema, 64-bit counts, and—by
default—partitioned canonical SHA-256 evidence. It writes a deterministic,
self-digesting JSON audit report and prints either a compact text summary or
the JSON report. Validation uses bounded temporary spill space. For a
deterministic plan it first compares the complete source outcome replay with the
target receipt and reject-ledger snapshot. After a passing validation report is
durably published, the CLI re-materializes or exactly reuses the reject artifact
and verifies its plan, target, and target-snapshot report bindings plus its
digest, byte, and count invariants;
only then may activation occur. Any report, artifact, or requalification failure
withholds activation. Repeating the same validation/report and reject-artifact
paths is idempotent, while different existing files are never overwritten.

Reject artifacts are sensitive because they can contain decoded source values.
Their protected storage, access, retention, and deletion are operator
responsibilities. The target-owned ledger and transactional receipts remain the
resume authority: the artifact is an operator-facing projection, never a
checkpoint, and is not consulted to decide which batches `--resume` skips.

## Project Layout

- `Program.cs` - command dispatch and shell startup
- `CliShellOptions.cs` - target and transport parsing
- `Repl.cs` - interactive SQL shell
- `MetaCommands.cs` - dot-command implementation
- `InspectorCommandRunner.cs` - storage inspection commands
- `MaintenanceCommandRunner.cs` - maintenance commands
- `DevOpsCommandRunner.cs` - schema compare commands
- `PipelineCommandRunner.cs` - ETL package and catalog commands
- `MigrationCommandRunner.cs` - migration inspect, plan, bounded DDL/scratch preview, apply, resume, validate, retained CSV/JSON/SQLite, schema-only SQL Server analysis, and CSV/JSON/NDJSON export commands
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
