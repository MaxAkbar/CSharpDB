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
csharpdb migrate inspect --source litedb --input <source.db> --package <source.csdblitedb> --out <catalog.json> [--profile-sample-size <count>] [--max-source-bytes <count>]
csharpdb migrate inspect --source access --input <source.mdb|source.accdb> --package <source.csdbaccess> --out <catalog.json> [--provider ace16|ace12] [--allow-ace12-fallback] [--command-timeout-seconds <1..3600>] [--max-source-bytes <count>] [--max-package-bytes <count>]
csharpdb migrate inspect --source sqlserver --connection-env <name> --out <catalog.json> [--package <source.csdbsqlserver> --max-source-bytes <count> --table-timeout-seconds <1..86400>]
csharpdb migrate inspect --source mysql --connection-env <name> --out <catalog.json> [--package <source.csdbmysql> --max-source-bytes <count> --table-timeout-seconds <1..86400>]
csharpdb migrate ddl-check <file.sql> --dialect csharpdb|tsql [--format text|json]
csharpdb migrate type-map <catalog.json> --out <report> [--profile preserve|queryable|custom --custom-map <map.json>] [--format text|json]
csharpdb migrate query-check <query.sql> --dialect csharpdb|tsql|mysql|sqlite|access --out <report> [--query-id <id>] [--compatibility-level 150|160|170] [--format text|json]
csharpdb migrate plan <catalog.json> --out <plan.json> [--profile preserve|queryable] [--accept-exclusions all|<id,...>] [--accept-diagnostics <id,...>] [--reject-mode fail-fast|deterministic --reject-rules all|<id,...> --max-rejected-rows-per-batch <count> --max-rejected-rows-per-run <count> --max-reject-evidence-value-bytes <count> --max-reject-evidence-bytes-per-batch <count> --max-reject-evidence-bytes-per-run <count> --max-reject-artifact-bytes <count>]
csharpdb migrate preview <plan.json> --catalog <catalog.json> [--ddl|--scratch] [--format text|json]
csharpdb migrate apply <plan.json> --catalog <catalog.json> --source-package <source.csdbcsv|source.csdbjson|source.csdbsqlite|source.csdblitedb|source.csdbaccess|source.csdbsqlserver|source.csdbmysql> --expected-manifest-digest <sha256:...> [--workspace <directory>] [--max-source-bytes <count>] --target <staged.csdb> --out <run.json> [--resume] [--allow-deterministic-rejects --reject-artifact <absolute-normalized-rejects.jsonl>] [--format text|json]
csharpdb migrate validate <plan.json> --catalog <catalog.json> --source-package <source.csdbcsv|source.csdbjson|source.csdbsqlite|source.csdblitedb|source.csdbaccess|source.csdbsqlserver|source.csdbmysql> --expected-manifest-digest <sha256:...> [--workspace <directory>] [--max-source-bytes <count>] --target <staged.csdb> --out <validation.json> [--level schema|count|checksum] [--spill-dir <directory>] [--allow-deterministic-rejects --reject-artifact <absolute-normalized-rejects.jsonl>] [--format text|json]
csharpdb migrate snapshot <source.csdb> --out <retained-snapshot.db> --offline [--workspace <directory>] [--max-database-bytes <positive-int64>] [--max-wal-bytes <positive-int64>] [--max-snapshot-bytes <positive-int64>] [--json]
csharpdb migrate export <retained-snapshot.db> --format csv --table <physical-table> --out <table.csv> --manifest <table.manifest.json> --expected-snapshot-identity <csharpdb-retained-snapshot/v1:<bytes>:sha256:<64-lowercase-hex>> [--profile lossless-v1|spreadsheet-safe-lossy-v1] [--max-data-bytes <positive-int64>] [--max-decoded-blob-bytes <positive-int32>] [--checkpoint-row-interval <positive-int64>] [--json]
csharpdb migrate export <retained-snapshot.db> --format json|ndjson --table <physical-table> --out <table.json|table.ndjson> --manifest <table.manifest.json> --expected-snapshot-identity <csharpdb-retained-snapshot/v1:<bytes>:sha256:<64-lowercase-hex>> [--profile lossless-v1] [--max-data-bytes <positive-int64>] [--max-decoded-blob-bytes <positive-int32>] [--checkpoint-row-interval <positive-int64>] [--json]
```

Inspection supports the immutable synthetic qualification source, strict CSV,
untyped retained JSON package v1, explicitly selected typed JSON package v2,
SQLite, LiteDB, Microsoft Access, SQL Server, and MySQL. Access inspection
creates a digest-pinned `.csdbaccess` retained capture. SQL Server and MySQL
inspection without `--package` remains a schema-only readiness route; supplying
`--package` instead creates a digest-pinned `.csdbsqlserver` or `.csdbmysql`
retained capture. MySQL can proceed to offline apply, resume, and validation. SQL Server
remains an evaluation capture lane: the restricted-account metadata proof
passes the SQL Server 2019 Express LocalDB fixture, but a separate
non-overrideable diagnostic blocks planning until the supported-edition,
published-runtime, platform, authentication, and differential matrix is
complete. Microsoft Access is also evaluation-only until its Windows/ACE/file
format/bitness VM matrix passes. CSV
inspection freezes the raw bytes and complete reader and inference policy into
one no-overwrite `.csdbcsv` package. JSON inspection does
the same for root-array JSON or NDJSON-compatible whitespace-separated
top-level values in one no-overwrite `.csdbjson` package. Both JSON package
versions use that extension; the catalog's versioned schema facets select the
reopen API, never the filename or package contents. SQLite inspection creates a
coherent online backup in one no-overwrite `.csdbsqlite` package and binds its
native catalog plus sampled storage-class profile to the package SHA-256.
SQLite backup creation is incrementally cancellable and defaults to a 1 TiB
retained-snapshot ceiling; `--max-source-bytes` can select a smaller bound.
LiteDB inspection requires an offline/quiesced, unencrypted LiteDB 5 source and
creates a byte-for-byte retained snapshot in one no-overwrite `.csdblitedb`
package. Capture holds a read handle that denies writers and deletion while
copying because LiteDB has no coherent online-backup API. Close all LiteDB
writers before running the command. The package and catalog are bound to the
printed SHA-256 content digest.

SQL Server and MySQL inspection accept only the name of an environment variable
through `--connection-env`; a raw connection string is never a command-line
argument.
The generic host first resolves its fixed sibling worker and passes only the
safe environment-variable name across the process boundary. The worker alone
resolves the value from the inherited environment; the host code never reads
or materializes it. The value is not written to the catalog, process arguments,
process protocol, or command output, and should identify a dedicated
least-privilege, read-only login with the required metadata visibility; retained
SQL Server capture additionally needs `SELECT` access to the tables being
captured. Complete SQL Server metadata visibility can be proven for a strict
non-`sysadmin` account when the audited database permissions and all per-object
visibility evidence are complete; an object-level `DENY`, missing evidence, or
permission drift fails closed. Do not elevate an account to bypass that guard.
Retained MySQL v1 uses a
dedicated read-only account with direct
schema-level `SELECT` on the selected database's ordinary base tables; it does
not require `TRIGGER`, `EXECUTE`, or `SHOW VIEW`. Each provider uses a separate
fixed worker directory and process
protocol. Catalog and retained-package destinations use create-new publication
and are never overwritten. SQL Server and MySQL inspection without `--package`
inventory and analyze schema only, without reading application rows. Inspection
with `--package` reads rows into the provider's retained package. Neither route
writes to SQL Server, MySQL, or a CSharpDB target. Cancellation terminates the
worker process tree. For SQL Server, this also contains a ScriptDom parse call
that cannot observe cooperative cancellation.

Access capture resolves only the fixed optional Windows worker under
`adapters/access`; the base CLI does not load ACE or `System.Data.OleDb`.
Capture accepts local unencrypted `.mdb` and `.accdb` files, defaults to ACE 16,
never uses Jet, and holds a read handle that denies writes and deletion for the
complete retained capture. The catalog's
`MIG-ACCESS-LIVE-QUALIFICATION-PENDING-001` diagnostic is non-overrideable and
keeps plan/apply unavailable until the release VM matrix is complete.

`migrate type-map` reports the exact planner mapping selected for every typed
catalog object without changing the catalog. `migrate query-check` is a bounded,
read-only static checker. CSharpDB analysis remains in the provider-neutral base
tool; T-SQL parsing runs only in the optional SQL Server worker. MySQL, SQLite,
and Access query dialects return `Unknown` until a qualified parser exists.
Parse-level `Conditional` evidence does not claim schema binding, scratch
execution, or semantic equivalence, and rewrite candidates are never applied.
For `type-map --profile custom`, the custom-map file is one strict JSON object
keyed by exact catalog object ID. Values are case-insensitive `integer`, `real`,
`decimal`, `text`, or `blob`; these select the persistent physical carrier,
while reports expose the retained logical declaration as `targetSqlType`.
Unknown IDs, duplicate properties, comments, trailing commas, and invalid types
are rejected.

Pass both `--typed-intent` and
`--expected-intent-manifest-digest` to select v2. The canonical sidecar must
already exist and must be bound to the exact input bytes, framing, source ID,
and reader defaults used by the command. The CLI does not author or
automatically discover typed sidecars. Typed inspection embeds the exact
verified sidecar in package v2, so the original JSON and standalone sidecar can
both be removed after successful publication.

Retained inspection writes the normal catalog artifact and prints
`manifestDigest=sha256:...`; typed JSON inspection also reports the embedded
`intentManifestDigest=sha256:...`. Retain package and sidecar pins in an
independently trusted change record or CI parameter. Apply, resume, and
validation require the package pin through `--expected-manifest-digest`.
The original CSV, JSON, live SQLite, live LiteDB, SQL Server, or MySQL source is
not reopened after retained inspection.

The DDL proof command reads one strict, bounded UTF-8 script and supports the
`csharpdb` and `tsql` dialects. The CSharpDB route uses source grammar
`csharpdb-sql/v1` and allowlists persistent `CREATE TABLE` plus simple
`CREATE INDEX`. The T-SQL route uses the fixed `tsql160` standalone ScriptDom
grammar with `QUOTED_IDENTIFIER` on; there is no grammar or SQL Server version
switch.

The T-SQL allowlist is intentionally narrow: ordinary two-part, exact-lowercase
`dbo` table creation, supported built-in scalar column types and facets,
explicit nullability for ordinary non-key columns, primary and unique keys over
SQL Server signed-integer columns, source-ordered foreign keys, and later simple
ascending indexes over the same source type family. SQL Server `rowversion` and
`timestamp` declarations lower to generated, implicitly non-nullable CSharpDB
`ROWVERSION`; they may omit nullability or state `NOT NULL`, while explicit
`NULL` is rejected. Explicit constraint names share the schema-object namespace
with tables; index names are table-scoped and cannot collide with a named key's
backing index on that table. Defaults, identity, computed or other generated
columns, checks, non-`dbo` or one-part names, `ALTER`, `DROP`, views, sequences,
triggers, routines,
derived/temporal/graph/storage features,
filtered/included/descending/clustered indexes, and other statement or
physical-option shapes fail closed. Any unsupported statement prevents
scratch proof for the complete script; a supported prefix is never proved by
itself.

The T-SQL parser ceilings are 4,194,304 UTF-16 code units, 16 MiB of UTF-8,
4,096 statements, 1,048,576 code units per statement, 250,000 tokens, nesting
depth 128, 250,000 AST nodes, 64 lexer errors, 64 parser errors, and 100,000
lowered catalog objects. A conservative lexical-unit preflight rejects
definite token-budget overruns before ScriptDom token allocation, followed by
the authoritative ScriptDom token-count check. Any lexer or parser error
rejects the script; crossing an error-count or other limit returns `Unknown`.
The downstream CSharpDB render, parser, action-count, SQL-size, and scratch
ceilings still apply. An ambiguous reference or failed target capability proof
also returns sanitized fail-closed evidence rather than a partial result.

Both routes lower the complete supported script through the migration model,
render candidate target DDL, and compare the resulting normalized schema in a
new, isolated in-memory scratch database. T-SQL always requires a rewrite, so
a passing non-text script reports `CompatibleWithRewrite`, not `Compatible`.
SQL Server text collation semantics remain unresolved: a text-bearing script
can attain `ScratchExecuted` evidence and equal normalized schemas while its
overall status remains `Conditional` and `ProvenStatementCount` remains zero.
`CompatibleWithRewrite` and `Conditional` return warning exit code 1;
`Unsupported` and `Unknown` return error exit code 2. T-SQL cannot return the
success exit code 0 because it always requires a rewrite.

The command writes only deterministic, sanitized text or JSON evidence to
standard output. Reports contain `SourceGrammar` (`sourceGrammar` in JSON),
digests, counts, source spans, stable rule identifiers, compatibility status,
and the attained evidence level, but not the script text, input path, object
names, or raw parser and engine messages. It never opens an existing target,
creates a target file, applies a rewrite, or promotes a migration plan.
Generated DDL remains review-only; other source dialects remain deferred.

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

LiteDB v1 replays every collection as a CSharpDB document collection in
ascending built-in `_id` index order. `_key` is a collision-proof typed source
key and `_doc` is the complete tagged canonical BSON document, including `_id`.
The bridge preserves all LiteDB BSON types and supports fail-fast plans only.
The built-in `_id` index is subsumed by `_key`; other simple, expression, and
unique indexes are retained in inventory but excluded from automatic target
creation because BSON-path, uniqueness, and collation semantics are not
translated. Review and accept those exclusions, then recreate required indexes
manually using reviewed CSharpDB semantics.

Supported migration-source matrix:

| Source | Retained consistency boundary | Schema/data route | Reject policy |
| --- | --- | --- | --- |
| CSV | Private byte-for-byte snapshot in `.csdbcsv` | Strict tabular inference and streaming | Fail-fast or the fixed CSV deterministic registry |
| JSON/NDJSON v1 | Private byte-for-byte snapshot in `.csdbjson` | Untyped table inference and streaming | Fail-fast or the fixed untyped-JSON deterministic registry |
| Typed JSON v2 | Source- and intent-pinned `.csdbjson` | Explicit typed table contract | Fail-fast |
| SQLite v1 | Coherent online backup in `.csdbsqlite` | Tier 1 native catalog and rowid streaming | Fail-fast |
| LiteDB v1 | Offline/quiesced byte-for-byte snapshot in `.csdblitedb` | Tagged canonical BSON document-collection streaming | Fail-fast |
| SQL Server retained v1 | Snapshot-isolated candidate capture in `.csdbsqlserver` | Bounded relational catalog and ordered row streaming | Evaluation only; least-privilege metadata proof blocks planning |
| SQL Server readiness (without `--package`) | Live best-effort, schema-only metadata inspection | Inventory, compatibility, planning, and target DDL assurance only | No data route |
| MySQL retained v1 | Content- and catalog-bound capture in `.csdbmysql` | Bounded relational catalog and ordered row streaming | Fail-fast |
| MySQL readiness (without `--package`) | Live best-effort, schema-only metadata inspection | Inventory, compatibility, planning, and target DDL assurance only | No data route |

SQLite catalogs record the adapter, Microsoft.Data.Sqlite assembly, native
SQLite engine, compile-option digest, database text encoding, profile coverage,
and content fingerprint used for the run. This repository qualifies the
provider version selected by `CSharpDbQualifiedEfCoreVersion` together with the
pinned SQLitePCLRaw bundle; a catalog produced by a different build remains
explicitly versioned evidence, not an implied compatibility claim.

LiteDB catalogs record the adapter and LiteDB provider versions, collection
name comparison, collation, collection/index/document counts, profile coverage,
and snapshot content fingerprint. Apply and validation copy the package into a
unique owner-private child of `--workspace`, verify the trusted digest,
reconstruct the exact catalog, and read only that private copy. The child is
removed when the command closes. If `--workspace` is omitted, the system
temporary directory is used; an explicit existing caller-controlled local
directory is recommended.

Example SQLite workflow:

```powershell
csharpdb migrate inspect --source sqlite --input .\source.db --package .\source.csdbsqlite --out .\catalog.json
# Record the printed manifestDigest in trusted change control.
csharpdb migrate plan .\catalog.json --out .\plan.json --accept-exclusions all
csharpdb migrate preview .\plan.json --catalog .\catalog.json
csharpdb migrate apply .\plan.json --catalog .\catalog.json --source-package .\source.csdbsqlite --expected-manifest-digest <recorded-sha256> --target .\staged.csdb --out .\run.json
csharpdb migrate validate .\plan.json --catalog .\catalog.json --source-package .\source.csdbsqlite --expected-manifest-digest <recorded-sha256> --target .\staged.csdb --out .\validation.json --level checksum
```

Example LiteDB workflow:

```powershell
# Close all LiteDB writers first. The v1 route accepts unencrypted LiteDB 5.
New-Item -ItemType Directory -Force .\migration-work, .\migration-spill
csharpdb migrate inspect --source litedb --input .\source.db --package .\source.csdblitedb --out .\catalog.json --max-source-bytes 1073741824
# Record the printed manifestDigest in trusted change control.

# Index inventory is excluded from automatic target creation and requires
# explicit review/approval. Use specific object IDs instead of "all" when
# change control requires narrower approval.
csharpdb migrate plan .\catalog.json --out .\plan.json --accept-exclusions all
csharpdb migrate preview .\plan.json --catalog .\catalog.json
csharpdb migrate preview .\plan.json --catalog .\catalog.json --ddl
csharpdb migrate preview .\plan.json --catalog .\catalog.json --scratch

csharpdb migrate apply .\plan.json --catalog .\catalog.json --source-package .\source.csdblitedb --expected-manifest-digest <recorded-sha256> --workspace .\migration-work --max-source-bytes 1073741824 --target .\staged.csdb --out .\run.json

# If apply was interrupted after the staged target was created, repeat the
# exact binding and add --resume.
csharpdb migrate apply .\plan.json --catalog .\catalog.json --source-package .\source.csdblitedb --expected-manifest-digest <recorded-sha256> --workspace .\migration-work --max-source-bytes 1073741824 --target .\staged.csdb --out .\run.json --resume

csharpdb migrate validate .\plan.json --catalog .\catalog.json --source-package .\source.csdblitedb --expected-manifest-digest <recorded-sha256> --workspace .\migration-work --max-source-bytes 1073741824 --target .\staged.csdb --out .\validation.json --level checksum --spill-dir .\migration-spill
```

Apply creates a new staged target and stops at `awaitingValidation`; it does not
overwrite or activate an existing target. Rows and receipts commit together.
`--resume` reopens and requalifies the package, creates a fresh private
workspace copy, and skips only exact matching batch receipts. The target
receipts—not the workspace copy or run report—are the recovery authority.
Checksum validation replays the same snapshot and compares normalized schema,
64-bit counts, and partitioned canonical SHA-256 evidence. Passing validation
activates the staged target; failure or inconclusive evidence withholds
activation.

Example SQL Server schema-readiness workflow:

```powershell
# Use the optional SQL Server migration bundle. A base-only csharpdb
# distribution reports MIG-SQLSERVER-CLI-ADAPTER-001 for this source.
# Populate CSHARPDB_SQLSERVER_SOURCE through your secret manager or protected
# process environment; do not put the connection string in scripts.
csharpdb migrate inspect --source sqlserver --connection-env CSHARPDB_SQLSERVER_SOURCE --out .\catalog.json
csharpdb migrate plan .\catalog.json --out .\plan.json
csharpdb migrate preview .\plan.json --catalog .\catalog.json
csharpdb migrate preview .\plan.json --catalog .\catalog.json --ddl
csharpdb migrate preview .\plan.json --catalog .\catalog.json --scratch --format json
```

Add `--package` to inspect when rows must be retained for an offline data
migration:

```powershell
csharpdb migrate inspect --source sqlserver --connection-env CSHARPDB_SQLSERVER_SOURCE --package .\source.csdbsqlserver --out .\catalog.json --max-source-bytes 274877906944 --table-timeout-seconds 1800
# Copy the exact manifestDigest=sha256:... value from the successful status.
$digest = 'sha256:<64-lowercase-hex>'

csharpdb migrate plan .\catalog.json --out .\plan.json --accept-exclusions all
csharpdb migrate apply .\plan.json --catalog .\catalog.json --source-package .\source.csdbsqlserver --expected-manifest-digest $digest --workspace . --target .\staged.csdb --out .\run.json
csharpdb migrate apply .\plan.json --catalog .\catalog.json --source-package .\source.csdbsqlserver --expected-manifest-digest $digest --workspace . --target .\staged.csdb --out .\resume.json --resume
csharpdb migrate validate .\plan.json --catalog .\catalog.json --source-package .\source.csdbsqlserver --expected-manifest-digest $digest --workspace . --target .\staged.csdb --out .\validation.json --level checksum --spill-dir .
```

The connection environment value is needed only by capture. Plan, preview,
apply, resume, and validation reopen the digest-pinned retained package and do
not reconnect to SQL Server. The default per-table row command timeout is
1,800 seconds; `--table-timeout-seconds` accepts 1 through 86,400. The package
ceiling defaults to 256 GiB and can only be lowered. A catalog produced without
`--package` remains the existing schema-only v6 planning route and is rejected
early by apply and data validation with instructions to inspect again.

Example MySQL schema-readiness workflow:

```powershell
# Use the optional MySQL migration bundle. A base-only csharpdb
# distribution reports MIG-MYSQL-CLI-ADAPTER-001 for this source.
# Populate CSHARPDB_MYSQL_SOURCE through your secret manager or protected
# process environment; do not put the connection string in scripts.
csharpdb migrate inspect --source mysql --connection-env CSHARPDB_MYSQL_SOURCE --out .\catalog.json
csharpdb migrate plan .\catalog.json --out .\plan.json
csharpdb migrate preview .\plan.json --catalog .\catalog.json
csharpdb migrate preview .\plan.json --catalog .\catalog.json --ddl
csharpdb migrate preview .\plan.json --catalog .\catalog.json --scratch --format json
```

Add `--package` to inspect when rows must be retained for an offline data
migration:

```powershell
csharpdb migrate inspect --source mysql --connection-env CSHARPDB_MYSQL_SOURCE --package .\source.csdbmysql --out .\catalog.json --max-source-bytes 274877906944 --table-timeout-seconds 1800
# Store the exact manifestDigest=sha256:... value in trusted change control,
# separately from the package.
$digest = 'sha256:<64-lowercase-hex>'

csharpdb migrate plan .\catalog.json --out .\plan.json --accept-exclusions all
csharpdb migrate apply .\plan.json --catalog .\catalog.json --source-package .\source.csdbmysql --expected-manifest-digest $digest --workspace . --target .\staged.csdb --out .\run.json
csharpdb migrate apply .\plan.json --catalog .\catalog.json --source-package .\source.csdbmysql --expected-manifest-digest $digest --workspace . --target .\staged.csdb --out .\resume.json --resume
csharpdb migrate validate .\plan.json --catalog .\catalog.json --source-package .\source.csdbmysql --expected-manifest-digest $digest --workspace . --target .\staged.csdb --out .\validation.json --level checksum --spill-dir .
```

Only capture needs the connection environment value. Plan, preview, apply,
resume, and validation reopen the digest-pinned `.csdbmysql` package without
reconnecting to MySQL. The package is plaintext-sensitive source data: protect
it with source-equivalent access controls, retention, and deletion policy.
Keep its expected digest in an independently trusted record so an attacker
cannot replace both the package and its pin. Credentials are accepted only
through the named environment variable, never as command text. Use a dedicated
read-only account with direct schema-level `SELECT` on the selected database's
ordinary base tables; do not grant `TRIGGER`, `EXECUTE`, or `SHOW VIEW` for
retained v1. Programmable objects remain outside this retained migration scope.
The default per-table timeout is 1,800 seconds, the accepted range is 1 through
86,400, and the 256 GiB package ceiling can only be lowered. A catalog produced
without `--package` is schema-only and is rejected before apply or data
validation can create a target.

The default preview remains the compact
`csharpdb-migration-preview/v1` planning summary. `--ddl` is the only mode that
prints exact target SQL and typed collection actions. `--scratch` instead
prints the sanitized `csharpdb-ddl-scratch-validation/v1` report containing
digests, stable rule/action identifiers, counts, and evidence level, but no SQL
or object names. The modes are mutually exclusive. A successful scratch run
does not approve exclusions, clear diagnostics, establish source semantic
equivalence, or make a blocked Access or SQL Server plan ready.
Each catalog and plan consumed by the explicit `--ddl` or `--scratch` path is
limited to 64 MiB before contract validation; rendered action count,
per-action SQL, and aggregate SQL bytes have separate fixed production
ceilings. The regenerated DDL digest must match the binding in a sealed plan.

The base CLI has no SQL Server project reference and its output contains no
SqlClient, ScriptDom, SNI, or related authentication assets. The non-packable
SQL Server bundle places that dependency closure only beneath
`adapters/sqlserver`, with a fixed companion executable. Schema inspection
uses `csharpdb-sqlserver-worker/v1`; standalone T-SQL DDL proof uses the
separate `csharpdb-sqlserver-ddl-worker/v1` protocol. Retained row capture is
an explicit third route using the exact
`csharpdb-sqlserver-capture-worker/v1` protocol. The host passes only the
connection environment variable name, current target version, one fixed
immediate-child private output path, package byte ceiling, and table timeout.
The worker reads and immediately clears its inherited copy of the named
connection value. Its bounded receipt contains only package/catalog digests,
snapshot identity, byte count, table count, and row count. The CLI independently
reopens and verifies the provider-neutral package before publication, removes
the private workspace before publishing the catalog or reporting success, and
never relays worker standard error. If catalog publication later fails, the
already verified package remains published for diagnosis.

The DDL host passes only
the protocol and current target version as arguments and streams the strict,
at-most-16-MiB script through redirected standard input, never an argument or
temporary file. A leading UTF-8 BOM is removed before transmission and digest
binding. The host accepts only a digest- and target-bound, `tsql160` report
under the 8 MiB output ceiling, drains bounded standard error without relaying
it, clears transient byte buffers, and terminates the worker process tree on
cancellation or protocol failure. On Windows, a kill-on-close job also limits
the worker to 512 MiB of process memory.

A missing or incompatible inspection worker fails only that route with
`MIG-SQLSERVER-CLI-ADAPTER-001`. A missing, incompatible, overclaiming, or
contract-invalid DDL worker fails the T-SQL route with
`MIG-TSQL-CLI-ADAPTER-001`; a safe worker analysis failure uses
`MIG-TSQL-CLI-DDL-CHECK-001`. Other migration commands remain
provider-independent. Capture reports stable, sanitized connection, limit,
package-verification, capture, cleanup, and catalog-publication error codes;
credentials, SQL text, source values, and worker diagnostics are never copied
into those messages. This packaging boundary does not qualify a live server,
runtime identifier, authentication mode, or shipping connector.

The base CLI also has no MySQL project reference and its output contains no
MySqlConnector assets. The non-packable MySQL bundle places that dependency
closure only beneath `adapters/mysql`, with a fixed companion executable.
Schema inspection uses `csharpdb-mysql-worker/v1`; retained row capture uses
the separate `csharpdb-mysql-capture-worker/v1` protocol. The host sends only
the safe environment-variable name, target version, private output path,
package byte ceiling, and table timeout. It independently reopens and verifies
the provider-neutral package before publishing it, removes the private capture
workspace, and never relays worker diagnostics. A missing or incompatible
worker fails only the MySQL route with `MIG-MYSQL-CLI-ADAPTER-001`; other
migration commands remain provider-independent.

These retained-package routes prove package, catalog, snapshot, and row
binding; they do not claim broad live-provider qualification. The Access
Windows/ACE/file-format/bitness matrix and broader SQL Server and MySQL server,
runtime, authentication, TLS, restricted-account, platform, and differential
qualification remain deferred.

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

Retained SQL Server and MySQL capture additionally require both package and
catalog parent paths to be exact normalized directories on a supported local
filesystem. The CLI holds and rechecks their filesystem identities through
capture and publication. A parent owned or writable by an untrusted identity,
a shared non-sticky writable ancestor, an extended ACL, a link, junction,
reparse point, or remote/unsupported filesystem fails closed. This directory
lease is a local publication-safety boundary; it does not qualify a live
database, authentication mode, or deployment environment.

LiteDB inspect publishes the no-overwrite `.csdblitedb` package before its
no-overwrite catalog. If later catalog publication fails, the package is
preserved for diagnosis instead of being silently deleted or repaired. Apply
and validation normally remove their unique owner-private workspace child on
close. A process crash can leave an orphan; after confirming that no migration
process is using it, inspect and remove it manually. Orphaned workspace data is
not resumable state and must never be substituted for the retained package.

CSV and JSON export begin with an explicit offline snapshot of the source
CSharpDB database. Close every writer before capture; `--offline` confirms that
operator-controlled write freeze. The command recovers only a bounded private
copy, publishes one clean retained snapshot with create-new semantics, and
prints the canonical identity required by export:

```powershell
New-Item -ItemType Directory -Force .\snapshot-work | Out-Null

$capture = csharpdb migrate snapshot .\app.csdb `
  --out .\app.export-snapshot.db `
  --offline `
  --workspace .\snapshot-work `
  --json | ConvertFrom-Json

csharpdb migrate export .\app.export-snapshot.db `
  --format csv `
  --table users `
  --out .\users.csv `
  --manifest .\users.manifest.json `
  --expected-snapshot-identity $capture.snapshotIdentity
```

The source database, retained destination, and optional workspace parent must
be different caller-controlled paths. The destination parent and optional
workspace parent must already exist and be link-free. Capture refuses to
replace a retained snapshot or its WAL namespace. Defaults cap the source
database and clean retained snapshot at 1 TiB and the WAL at 4 GiB; the three
`--max-*-bytes` options can select smaller positive bounds. Capture is portable,
but the current resumable CSV/JSON output publisher remains Windows-only.

The default CSV profile is `lossless-v1`.
`spreadsheet-safe-lossy-v1` is an explicit value-changing mitigation profile.
The output CSV and manifest must be distinct siblings in one trusted,
caller-controlled local Windows directory. UNC or mapped-network output paths,
output links, junctions, reparse points, and devices, and non-Windows
publication fail closed with `MIG-EXPORT-PLATFORM-001`. The Windows-only
boundary is intentional: the current qualified publisher depends on
handle-bound parent-directory validation and atomic no-replace pair
publication. An equivalent Unix publication substrate is not yet qualified.

The exact same export command is also the resume command. A rerun requalifies
the retained snapshot identity and private checkpoint journal, resumes only at
a verified complete-row boundary, recovers an exact CSV-only publication, or
reuses an exact completed CSV/manifest pair. Different or unsafe existing
files are never overwritten or repaired. Text output reports the final paths,
row and byte counts, content digests, and whether the CSV or manifest was
reused; `--json` emits the same result as structured JSON.

JSON and NDJSON export use the same CLI-created, identity-pinned retained
snapshot and durable resume workflow. `--format json` writes one compact root
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
typed JSON v2, SQLite, and LiteDB remain fail-fast only.
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
activates an existing target. Before target creation, retained CSV, JSON,
SQLite, or LiteDB execution verifies the exact package digest and reconstructs the
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
- `MigrationCommandRunner.cs` - migration inspect, plan, bounded DDL/scratch
  preview, apply, resume, validate, retained
  CSV/JSON/SQLite/LiteDB/MySQL/Access/SQL Server capture, schema analysis,
  type mapping, DDL/query/EF compatibility checks, retained snapshots, and
  CSV/JSON/NDJSON export commands
- `AccessWorkerClient.cs` - bounded fixed-path protocol client for the optional
  Windows Access capture worker
- `SqlServerWorkerClient.cs` - bounded fixed-path protocol client for the
  optional SQL Server inspection, retained capture, T-SQL query analysis, and
  standalone T-SQL DDL worker routes
- `MySqlWorkerClient.cs` - bounded fixed-path protocol client for the optional
  MySQL schema-inspection and retained-capture worker routes
- `CliConsole.cs` and `TableFormatter.cs` - terminal formatting helpers

## Build And Test

```powershell
dotnet build src/CSharpDB.Cli/CSharpDB.Cli.csproj
dotnet test tests/CSharpDB.Cli.Tests/CSharpDB.Cli.Tests.csproj
dotnet test tests/CSharpDB.Migration.Access.Tests/CSharpDB.Migration.Access.Tests.csproj
dotnet test tests/CSharpDB.Migration.Compatibility.Tests/CSharpDB.Migration.Compatibility.Tests.csproj
dotnet test tests/CSharpDB.Migration.DualRun.Tests/CSharpDB.Migration.DualRun.Tests.csproj
dotnet test tests/CSharpDB.Migration.MySql.Tests/CSharpDB.Migration.MySql.Tests.csproj
dotnet test tests/CSharpDB.Migration.SqlServer.Tests/CSharpDB.Migration.SqlServer.Tests.csproj
.\scripts\Test-AccessMigrationIsolation.ps1 -Configuration Release
.\scripts\Test-MySqlMigrationIsolation.ps1 -Configuration Release
.\scripts\Test-SqlServerMigrationIsolation.ps1 -Configuration Release
```

## Dependencies

- `CSharpDB.Client`
- `CSharpDB.DevOps`
- `CSharpDB.Engine`
- `CSharpDB.Migration`
- `CSharpDB.Migration.CSharpDb`
- `CSharpDB.Migration.Compatibility`
- `CSharpDB.Migration.Files`
- `CSharpDB.Migration.LiteDb`
- `CSharpDB.Migration.Retained`
- `CSharpDB.Migration.Sqlite`
- `CSharpDB.Sql`
- `CSharpDB.Storage.Diagnostics`
- `Spectre.Console`

The base CLI deliberately references none of
`CSharpDB.Migration.Access`, `CSharpDB.Migration.MySql`, or
`CSharpDB.Migration.SqlServer`. Optional workers own those provider references
together with System.Data.OleDb, MySqlConnector, or
Microsoft.Data.SqlClient/ScriptDom, respectively.
