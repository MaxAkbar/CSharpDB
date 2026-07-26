# CSharpDB MySQL migration analyzer

This optional, non-packable project provides the bounded MySQL schema analyzer
and retained row capture for the CSharpDB migration tooling. It opens one
selected database, reads fixed server-variable and `INFORMATION_SCHEMA`
projections, retains bounded `SHOW CREATE TABLE` evidence, and builds the
source snapshot used by the migration planner. Its retained v1 path also
captures a deliberately narrow, deterministic subset of application rows into
the provider-neutral retained migration package. The generic CLI does not
reference MySqlConnector directly; the non-packable MySQL distribution runs
this project in a fixed companion worker.

The checkpoint inventories tables, columns and column-default evidence,
primary and unique keys, foreign keys, checks, index variants, views and their
output columns, triggers, stored procedures and functions, and routine
parameters and function return rows. Programmable definitions are inventory
evidence only. They are not parsed, bound, lowered, scratch-executed, or
promoted to target SQL. Retained v1 never captures or migrates those objects.
No path executes caller-supplied SQL, writes to the source, or creates a
target.

## Qualified scope

The intended qualification target is Oracle MySQL 8.0 and 8.4 with InnoDB.
MySqlConnector can connect to MariaDB, Aurora MySQL, and other compatible
servers, but driver connectivity is not qualification. The analyzer records
server version and version-comment evidence so non-Oracle variants can be
reported as unqualified instead of being treated as equivalent.

The provider has deterministic reader, catalog, retained-package,
process-protocol, and adversarial lifecycle tests. It also has an opt-in live
least-privilege evidence test, described below, but no completed live-server
qualification claim. MySQL 8.0 and 8.4 server tags, TLS modes, platforms, and
published-runtime smoke tests must still be qualified before this adapter
becomes packable or shipping software.

### Opt-in least-privilege live evidence

`MySqlLeastPrivilegeLiveQualificationTests` is disabled unless
`CSHARPDB_MYSQL_LIVE_ADMIN_CONNECTION` contains an administrative connection
string. The connection must select one Oracle MySQL 8.0 or 8.4 server, use
`SslMode=Required` or a stronger certificate-verifying mode for TCP, and allow
the test to create and drop a database, users, a role, and schema grants. Run:

```powershell
$env:CSHARPDB_MYSQL_LIVE_ADMIN_CONNECTION = '<secure admin connection string>'
dotnet test tests/CSharpDB.Migration.MySql.Tests/CSharpDB.Migration.MySql.Tests.csproj --filter FullyQualifiedName~MySqlLeastPrivilegeLiveQualificationTests
```

The test creates unique disposable objects and removes them afterward. It
proves that an account with the analyzer's exact four direct schema grants
(`SELECT`, `SHOW VIEW`, `TRIGGER`, and `EXECUTE`) can inventory a table, view,
trigger, and routines, removes the metadata-completeness diagnostic, and can
capture retained rows. It also proves that the analyzer remains fail-closed on
the separate live-qualification diagnostic and that equivalent privileges
received only through an active role do not satisfy the direct-grantee proof
or publish a retained package.

These are the minimum direct grants used by the current full-analyzer proof,
not a claim that the MySQL account is strictly read-only. In MySQL,
`TRIGGER` and `EXECUTE` carry capabilities beyond metadata reads. The adapter
does not exercise those capabilities, but a strictly read-only route for
complete programmable-object metadata remains a qualification and design gap.
Retained v1 avoids that wider grant set and requires only direct `SELECT`.

This opt-in test is one evidence-acquisition lane, not the qualification
matrix. A successful local run does not remove
`MIG-MYSQL-LIVE-QUALIFICATION-PENDING-001` or
`MIG-MYSQL-RETAINED-LIVE-QUALIFICATION-DEFERRED-001`, and it does not certify
other server tags, authentication modes, TLS modes, operating systems, or the
published worker bundle. Without the environment variable, the test reports
as skipped.

## CLI and worker boundary

The optional bundle places the fixed
`csharpdb-migration-mysql-worker` companion and its complete MySqlConnector
dependency closure beneath `adapters/mysql`. The generic host resolves only
that sibling path and communicates through `csharpdb-mysql-worker/v1`. The
base CLI output and dependency graph remain free of this project and
MySqlConnector.

The CLI accepts only the safe name of an inherited environment variable
through `--connection-env`; it never accepts, reads, logs, or sends a raw
connection string. The worker alone resolves the value and returns a bounded
catalog through the fixed protocol. Public errors are stable and sanitized,
standard error is bounded and not relayed, catalog output is limited to
64 MiB, and cancellation terminates the worker process tree.

Schema inspection remains distinct from retained capture. The schema analyzer
continues to emit fail-closed inventory and live-qualification diagnostics.
Retained capture replaces only the blockers that its bounded row package and
scoped metadata proof resolve; it preserves unrelated analyzer diagnostics.
A preview, a package capture, or a successful scratch comparison does not by
itself make the MySQL adapter shipping-qualified.

## Connection policy

The caller must select exactly one server and one non-empty database in the
connection string. Comma-delimited multi-host configurations are rejected so
one inspection cannot silently move between servers. The reader opens one
connection with pooling, local-infile requests, user variables, automatic
transaction enlistment, and persisted security information disabled.
`TreatTinyAsBoolean=false`, `AllowZeroDateTime=true`,
`ConvertZeroDateTime=false`, `DateTimeKind=Unspecified`, `GuidFormat=None`,
and `IgnoreCommandTransaction=false` are forced so provider conversions and
transaction attachment cannot silently change retained values. Connection,
command, and cancellation timeouts are bounded.

TCP connections reject `SslMode=None` and `SslMode=Preferred`; they must use
`Required` or a stronger certificate-verifying mode. Local Unix sockets,
named pipes, and shared-memory transports may explicitly opt out of TLS.
Caller-selected certificate and authentication settings are otherwise
preserved, and the adapter never relaxes them.

Only the configured database name is used as a parameter to fixed metadata
queries. The sole dynamic command is one `SHOW CREATE TABLE` per table name
already returned by the bounded catalog scan. Schema and table identifiers are
quoted separately with backticks, and embedded backticks are doubled. No
caller-supplied SQL fragment is executed.

Raw connection strings, credentials, account names, server names, socket
paths, and provider exception messages are not migration artifacts or public
errors. The durable endpoint identity is a domain-separated SHA-256 digest;
the selected database remains visible because it is required schema metadata.

## Retained v1 row capture

`MySqlRetainedCapture.CaptureAsync` writes
`csharpdb-mysql-retained-data/v1` content through the generic retained package
writer. Capture uses one non-pooled connection and one read-only
consistent-snapshot transaction. Every catalog and row command is explicitly
bound to that transaction. The analyzer catalog is read before row streaming
and read and digested again before final publication. Any difference fails the
capture, and the generic writer publishes no output file until all row,
catalog, manifest, and package checks have succeeded. Rollback, transaction
disposal, and connection disposal are all attempted even when another cleanup
step reports a provider failure.

A table is data-available only when all of these conditions hold:

- it is an ordinary, nonpartitioned InnoDB base table;
- every selected column has a retained v1 scalar codec and is neither
  generated, invisible, nor ZEROFILL; and
- it has a complete nonnullable primary key, or otherwise a complete
  nonnullable unique key, whose exact visible ascending unique BTREE backing
  index contains only signed or unsigned integer columns.

Before materializing any row, capture runs a same-snapshot, length-only
preflight for every projected scalar. The row query then projects
`OCTET_LENGTH(projected-value)` plus a server-side `CASE` that returns the
column only when it remains within the configured value limit; oversized
values therefore never enter the connector's current-row buffer. Text length
uses the exact UTF-8 result representation through
`OCTET_LENGTH(CONVERT(column USING utf8mb4))`. All schema, table, column, and
ordering identifiers are separately quoted. The selected database and exact
table identity are verified inside the same transaction before each scan.
Rows are ordered by every chosen key member in ascending order. Text or
collation order is never used as retained ordering evidence.

Retained scalar v1 supports signed and unsigned MySQL integers, including the
full unsigned `BIGINT` range. `TINYINT(1)` remains an integer rather than being
coerced to Boolean. `DECIMAL` and `NUMERIC` use MySqlConnector's
`MySqlDecimal` representation and a canonical text path that supports MySQL's
65-digit precision and 30-digit scale. Finite `FLOAT` and `DOUBLE` values use
round-trip text; binary32 values are widened exactly before encoding. Text is
strict Unicode-to-UTF-8, binary values remain byte-exact, and valid `DATE` and
`DATETIME` values use the shared CSharpDB wall-clock formats. Zero or partial
dates fail capture rather than being converted.

`TIME`, `TIMESTAMP`, `JSON`, `BIT`, `YEAR`, `ENUM`, `SET`, spatial values,
generated columns, invisible columns, and ZEROFILL columns are unavailable in
retained v1. A table containing any unavailable column is not row-captured.
Caller-selected limits bound tables, columns per table, rows per table, total
rows, value bytes, row bytes, package bytes, and per-table command time. Fixed
implementation ceilings cannot be raised. Impossible retained envelope minima
and exceeded capture bounds use the typed
`MySqlRetainedCaptureLimitException`.

The retained catalog records `mysqlRetainedMetadataScope` as
`ordinary-base-tables`, uses generic `migrationDataAvailable` and
`migrationDataUnavailableReason` table facets, and binds every available
table to the integer-key ordering and scalar codec contracts. Views, triggers,
routines, events, and non-table objects are explicitly outside this retained
scope. The retained catalog therefore includes a conditional scope warning
and a separate deferred-live-qualification warning; neither is a claim of
live qualification.

## Metadata visibility proof

The analyzer reads `CURRENT_USER()` and direct schema grants from
`INFORMATION_SCHEMA.SCHEMA_PRIVILEGES` through the same transaction-bound
catalog context. A grant counts only when its `GRANTEE` exactly matches the
authenticated account format derived from `CURRENT_USER()`. Global grants,
activated-role grants, different grantees, and ambiguous account formats do
not count.

For the full schema analyzer, direct schema-level `SELECT`, `SHOW VIEW`,
`TRIGGER`, and `EXECUTE` facts are all required to remove the broad metadata
completeness blocker. Those facts only establish bounded metadata visibility;
the analyzer does not invoke routines or mutate triggers. Routine bodies can
still remain unavailable and diagnosed.

Retained v1 has a narrower read-only proof. It requires only an exact-grantee,
direct schema-level `SELECT` grant because its scope is ordinary base-table
metadata and rows. The binding checks this proof before any table row is read
or any package output is started. Missing, ambiguous, role-only, or
different-grantee evidence fails capture. Retained capture does not require
`SHOW VIEW`, `TRIGGER`, or `EXECUTE`, and it does not claim complete
programmable-object inventory.

## Metadata scope

The server snapshot records:

- server version and version comment;
- session SQL mode and time zone;
- server character set and collation;
- system time zone and `lower_case_table_names`;
- generated invisible primary-key visibility when the qualified server exposes
  that session setting;
- `sql_quote_show_create` session behavior;
- `explicit_defaults_for_timestamp` session behavior;
- the selected database;
- ordinary `INFORMATION_SCHEMA.TABLES` metadata;
- ordered `INFORMATION_SCHEMA.COLUMNS` metadata, including bounded
  `COLUMN_TYPE` byte-count and digest evidence;
- primary and unique constraints with ordered column membership;
- foreign-key rules and ordered child/referenced column pairs;
- enforced and unenforced check constraints;
- index type, visibility, uniqueness, prefix length, sort direction, and
  functional-key-part evidence;
- one bounded `SHOW CREATE TABLE` result for every inventoried table;
- bounded column-default evidence, including expression-default and automatic
  `ON UPDATE` markers;
- views, ordered view output columns, and bounded view definitions;
- table-owned triggers, their event, timing, action order, creation settings,
  and bounded action statements; and
- stored procedures and functions, ordered parameters and function return
  rows, execution characteristics, creation settings, and bounded routine
  definitions.

Every result set is consumed under fixed object and text-byte ceilings.
Exceeding a ceiling fails the inspection instead of silently publishing an
incomplete catalog.

Raw `COLUMN_TYPE`, check clauses, functional-index expressions, and
`SHOW CREATE TABLE` text are bounded and used only during analysis. The same
rule applies to column defaults and view, trigger, and routine definitions.
Durable catalog facets retain byte counts, lengths, and domain-separated
digests, never the raw SQL text, definer account, comments, or creation and
alteration timestamps. Privilege-filtered view and routine metadata remains
inventoried with explicit unavailable evidence instead of being treated as a
complete definition. MySQL reports `COLUMN_DEFAULT` as SQL `NULL` for both an
explicit `DEFAULT NULL` and no `DEFAULT` clause, so that case remains
explicitly ambiguous rather than being guessed from text. Non-null defaults,
expression defaults, and `ON UPDATE` behavior remain source expressions and
are excluded by planning until a safe MySQL expression pipeline exists.
On affected Oracle versions, `DEFAULT_GENERATED` can coexist with unavailable
`COLUMN_DEFAULT` evidence; the analyzer inventories and excludes that shape
instead of guessing its expression or aborting the inspection.

Views and triggers remain excluded from target planning because their bodies
have not reached parse, bind, scratch, or differential evidence. Routines have
no automatic CSharpDB lowering contract. No `targetSql`, generic
`deterministic`, or generic `rowLocal` facet is inferred from MySQL declarations
or raw text. `VIEW_TABLE_USAGE` and `VIEW_ROUTINE_USAGE` are
privilege-filtered and do not establish a complete dependency graph, so
dependency proof remains deferred. Functional, prefix, descending, invisible,
non-BTREE, and ambiguous foreign-key support indexes remain explicit blockers
instead of being simplified silently.

Detailed partition metadata, Event Scheduler definitions and schedules,
complete programmable dependency proof, unsupported row families, and live
restricted-account qualification remain follow-on work. Their absence is
represented by explicit availability facts and diagnostics; a retained
catalog is not blanket migration approval. Live MySQL 8.0/8.4, Docker,
published-runtime, restricted-account matrix, and TLS-mode qualification
remain deferred. The opt-in restricted-account test above is one evidence
lane and does not complete that matrix. The wider migration roadmap also
defers Access and disposable-Windows-VM qualification.

MySQL `BIT` remains unsupported because its width and bit-string conversion
semantics are not equivalent to a generic binary mapping. MySQL `TIME` also
remains unsupported because it is a signed duration, not the shared
time-of-day semantic type.

## Dependencies

MySqlConnector 2.6.1 is pinned directly and is managed-only. The reviewed
`net10.0` worker runtime package closure is recorded in
`THIRD-PARTY-NOTICES.md` and accompanies the optional worker distribution.
This project deliberately does not reference Oracle's `MySql.Data` package or
optional MariaDB authentication extensions.
