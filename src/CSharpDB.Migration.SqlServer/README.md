# CSharpDB.Migration.SqlServer

This optional project is the bounded SQL Server readiness analyzer for the
CSharpDB migration tooling. The generic CLI does not reference it; the
non-packable SQL Server distribution runs it in a fixed companion worker. It
inspects server and database facts plus schemas,
ordinary user tables, columns, defaults, identity and computed-column metadata,
primary and unique keys, foreign keys, check constraints, table indexes, and
sequences. It also inventories user views, indexed-view backing indexes,
database and object triggers, procedures and functions, routine parameters,
bounded SQL module facts, trigger events, declared SQL expression dependencies,
full-text catalogs and index configuration, data spaces, partition
functions/schemes/destinations, and per-heap/index partition compression facts.
The offline index inventory also retains XML-index subtype and reference facts,
bounded selective-XML path metadata, spatial tessellation configuration,
memory-optimized hash bucket counts, columnstore ordering and SQL Server 2025
data-clustering ordinals, and SQL Server 2025 JSON-index options and bounded
path metadata.
It does not copy SQL Server rows or write to either the source or a CSharpDB
target. Available default, computed-column, check, filtered-index, and SQL
module definitions receive bounded, syntax-only ScriptDom analysis. Parsing
does not imply that an expression or module has been bound, lowered, or
accepted by CSharpDB.

The reader requires product major version 15 or later. It rejects pre-2019
engines immediately after the server/database preflight, before running any
structural catalog query. The intended qualification boundary is on-premises
SQL Server 2019, 2022, and 2025 at their default compatibility levels 150, 160,
and 170. These lanes remain provisional until exact-tag live fixtures pass; the
analyzer records that pending proof explicitly. Later or otherwise unqualified
major versions, compatibility levels, and engine variants that pass the version
gate remain visible in the catalog but receive a stable unqualified-source
diagnostic. Azure SQL Database, Azure SQL Managed Instance, Synapse, Fabric,
and other compatible services are not silently treated as equivalent to the
intended on-premises products.

This checkpoint is intentionally non-shipping. Available SQL definitions are
parsed only for bounded syntax and expected-root evidence; they are not bound,
rewritten, or differentially validated. The proven ordinary relational subset
now has an offline integration proof through the generic migration planner,
the exact CSharpDB DDL renderer, and a transactionally isolated in-memory
scratch catalog comparison. That proof does not promote the partial SQL Server
inventory from `Blocked`, establish source semantics, or write a
caller-supplied, existing, or durable target.
The non-packable proof CLI exposes schema-only inspection through a named
environment variable and the isolated worker, generic planning sealed to the
bounded target DDL digest, bounded exact CSharpDB DDL preview, and sanitized
in-memory scratch evidence. The worker resolves the connection value itself;
the value is never placed on the command line or process protocol. The surface
does not accept a raw connection string argument, copy rows, apply DDL, open
an existing target, or promote the analyzer's blocked readiness. The offline
physical inventory deliberately omits raw partition
boundary values, full-text stopwords and registered property definitions,
database file names and paths, allocation details, physical sizes, row
estimates, crawl state, and other volatile operational facts. Subtype-specific
XML and JSON path text is read only under fixed byte ceilings and retained in
artifacts as domain-separated digests and lengths, not raw path text. The
subtype inventory does not query columnstore segments or row groups, dynamic
management views, allocation details, or row counts. The base CLI output and
dependency graph now exclude this adapter, SqlClient, ScriptDom, SNI, and
their authentication closure; those assets live only beneath the optional
worker directory. Live server and published-runtime qualification remain later
Phase 7A work. Disposable-VM and restricted-login qualification remain
deferred; no offline fixture is described as live evidence. A SQL Server data
importer is a separately approved follow-on.

## Phase 6B.2: standalone T-SQL DDL proof

The optional adapter now provides a connection-free proof route for one
standalone T-SQL DDL script:

```powershell
csharpdb migrate ddl-check .\schema.sql --dialect tsql [--format text|json]
```

This route is separate from live SQL Server inspection: the source script is
parsed and lowered but never executed on SQL Server. It always uses
`TSql160Parser` with `QUOTED_IDENTIFIER` on and
`SqlEngineType.Standalone`, reported as source grammar `tsql160`, and proves
only the current embedded CSharpDB target. The CLI does not accept a grammar,
compatibility-level, engine, or server-version option. `GO` batches are
accepted and flattened by that fixed grammar, but every contained statement
must fit the following whole-script additive DDL allowlist:

- `CREATE TABLE` must name an ordinary persistent table as exactly
  `dbo.<name>`, declare at least one column, and omit graph, temporal,
  filetable, derived-table, storage, partition, inline-index, and table-option
  features. Identifiers are at most 128 UTF-16 code units and a table has at
  most 1,024 columns.
- Accepted no-parameter built-in shapes are `bigint`, `int`, `smallint`,
  `tinyint`, `bit`, `real`, `money`, `smallmoney`, `datetime`,
  `smalldatetime`, `text`, `ntext`, `image`, `uniqueidentifier`, and `date`.
  User-defined, alias, qualified, `rowversion`/`timestamp`, and every unlisted
  type fail closed.
- `decimal`/`numeric` default to `(18,0)`, with precision 1 through 38 and
  scale 0 through the selected precision. `float` has optional precision 1
  through 53 and defaults to 53. `char`, `varchar`, `binary`, and `varbinary`
  default to length 1 and accept 1 through 8,000; only `varchar` and
  `varbinary` also accept `max`. `nchar` and `nvarchar` default to length 1
  and accept 1 through 4,000; only `nvarchar` also accepts `max`. `time`,
  `datetime2`, and `datetimeoffset` accept fractional precision 0 through 7
  and default to 7.
- Every non-primary-key column must state exactly one `NULL` or `NOT NULL`;
  primary-key columns may omit nullability but cannot be nullable. Defaults,
  identity, computed or generated values, column indexes, encryption, masking,
  hidden/row-guid/persisted flags, and storage options are outside the
  allowlist. Explicit `COLLATE` is accepted only for a text-mapped type, and
  remains conditional rather than proving SQL Server collation semantics.
- Only primary keys, unique constraints, and foreign keys are lowered.
  Primary and unique key members must be distinct, ascending, non-null columns
  of the SQL Server `bigint`, `int`, `smallint`, or `tinyint` source family,
  with at most 32 members, at most one primary key per table, and no clustered
  or physical index options. Explicit constraint names share the `dbo`
  schema-object namespace with table names and are checked case-insensitively;
  omitted source names remain unnamed and receive only a deterministic target
  identity. Foreign keys must resolve in source order to exactly one earlier
  or self-referenced ordered primary/unique key with exact identifier
  spelling, native type, facets, target mapping, classification, and codec.
  Update actions are restricted to no action; delete permits no action or
  cascade. `SET NULL`, `SET DEFAULT`, unenforced, and
  `NOT FOR REPLICATION` shapes fail closed.
- A separate `CREATE [UNIQUE] [NONCLUSTERED] INDEX` must follow its table, use
  a case-insensitively unique name within that table, avoid the backing-index
  name of an explicitly named primary/unique key, and use distinct ascending
  columns from that same signed-integer source family, with at most 32
  members. The same standalone index name may occur on different tables.
  Clustered, descending, included, filtered, filegroup/partition, and other
  index options are omitted. Unique index members must also be non-null.

`ALTER`, `DROP`, checks, views, sequences, triggers, routines, and every other
statement kind are outside this standalone proof. Duplicate detection is
case-insensitive, while `dbo` and every reference must exactly match the
declared spelling so the analyzer does not infer a database collation. If
any statement, feature, reference, type, or target capability is unsupported,
the analyzer does not prove a supported prefix or silently omit it; the whole
script stops before scratch execution and returns sanitized span- and
rule-based evidence.

The parser hard ceilings are 4,194,304 UTF-16 code units, 16 MiB of strict
UTF-8, 4,096 statements, 1,048,576 code units per statement, 250,000 tokens,
nesting depth 128, 250,000 AST nodes, 64 lexer errors, 64 parser errors, and
100,000 lowered catalog objects. A conservative lexical-unit preflight rejects
definite token-budget overruns before ScriptDom allocates a token stream; the
ScriptDom count remains the authoritative second check. Any lexer or parser
error rejects the script; exceeding either error ceiling returns `Unknown`.
Lower caller-selected limits are allowed, but these production maxima cannot
be raised. The CSharpDB target proof independently caps candidate actions at
4,096 and aggregate candidate SQL at 16 MiB; its defaults also cap each action
at 1,048,576 UTF-16 code units and 4 MiB of UTF-8. Crossing a ceiling fails
closed without partial proof.

A completely lowered script is evaluated against the current CSharpDB
capability catalog, rendered as candidate CSharpDB DDL, parsed again by
`CSharpDB.Sql`, executed in a new in-memory scratch database, and compared with
the intended normalized schema. Because this route necessarily rewrites
T-SQL, a passing non-text script reports `CompatibleWithRewrite`, never
`Compatible`. SQL Server text collation semantics are not yet proven: any
text-bearing script retains `tsql.ddl.collation.unresolved` and overall
`Conditional` status even when `HighestEvidence` is `ScratchExecuted` and the
normalized schema digests match. In that conditional case
`ProvenStatementCount` remains zero and every statement remains
`Conditional`/`ScratchExecuted`: scratch proves the rewritten target shape,
not SQL Server collation equivalence.

The shared `csharpdb-ddl-compatibility/v1` report includes `SourceGrammar`
(`sourceGrammar` in JSON), `Dialect`, the current target version, a
domain-separated source digest, sanitized statement spans, stable rules,
counts, status, and evidence. The T-SQL digest is lowercase SHA-256 over UTF-8
`tsql-ddl-input/v1`, one NUL byte, and the exact strict UTF-8 source bytes.
Reports omit SQL text, paths, identifiers, ASTs, parser messages, and engine
messages. This proof creates no durable database, opens no existing target,
applies no rewrite, and changes no plan readiness.

The base CLI reaches this code only through the optional fixed sibling worker
under `adapters/sqlserver`, using the separate
`csharpdb-sqlserver-ddl-worker/v1` protocol. The host supplies exactly the
protocol and current target version as
`--protocol csharpdb-sqlserver-ddl-worker/v1 --target-version <current>`.
The CLI removes an optional leading UTF-8 BOM, then streams the at-most-16-MiB
strict UTF-8 script through redirected standard input; SQL is never placed in
arguments or a temporary file. Success output is the exact header
`csharpdb-sqlserver-ddl-worker/v1\n` followed by one compact camelCase,
string-enum report capped at 8 MiB. Worker exit 0 means success, 10 means
incompatible input or invocation, 12 means analysis failed, and 13 means an
internal failure; exit 11 is not used by this DDL protocol. Standard error is
bounded and never relayed, transient byte buffers are cleared, and
cancellation or a protocol limit terminates the worker process tree. On
Windows, the host also attaches the worker to a kill-on-close job with a
512-MiB per-process memory ceiling.

The host accepts only a report bound to the expected protocol, format, `tsql`
dialect, `tsql160` grammar, current target, and source digest. A missing,
incompatible, overclaiming, or malformed worker fails only this route with a
sanitized adapter error.

## Read-only and security boundary

The production reader:

- uses only hard-coded, parameter-free `SELECT` statements over
  `SERVERPROPERTY` and `sys.*` catalog views;
- caps the connection timeout at 30 seconds, fixes the command timeout at
  30 seconds, and disables connection retries, pooling, and multiple active
  result sets;
- requests `ApplicationIntent=ReadOnly`; and
- leaves the caller's encryption and certificate-validation choices intact.

`ApplicationIntent=ReadOnly` is routing intent, not a database permission
boundary. Run the analyzer with a dedicated least-privilege login that can
connect to the selected database and view its complete definitions but has no
data- or schema-write grants. Database-level `db_owner`, `CONTROL`, and
`VIEW DEFINITION` evidence is not treated as proof of completeness by itself
because an object- or schema-level `DENY` can still narrow visibility. The
reader captures effective `sys.user_token` membership and applicable explicit
denials before and after the structural inventory, probes `VIEW DEFINITION` on
each visible schema, table, view, and routine, and probes
`VIEW SECURITY DEFINITION` on SQL Server 2022 and later. Trigger metadata
visibility follows its parent database, table, or view. A detected denial,
missing definition access, or changed permission snapshot blocks completeness.
Only sysadmin membership currently promotes visibility to complete; a clean
least-privilege result remains `Unknown` until the live restricted-account
qualification lane passes.

In the optional CLI distribution, the caller supplies connection material in
the named worker environment variable at runtime, and the base CLI passes only
that variable's safe name. Direct SDK callers instead pass the connection
string in memory to `SqlServerMigrationSourceInspector`. Raw connection
strings, account names, access material, and endpoints are not placed on worker
arguments or written to migration artifacts. The selected database name
remains visible as required catalog metadata, while the durable source identity
uses a one-way digest of the normalized endpoint and database scope. Public
adapter and worker errors are generic; provider exceptions and worker standard
error are not relayed because their text can contain connection material.

`Microsoft.Data.SqlClient` 7.0.2 keeps encryption mandatory by default. This
adapter never enables `TrustServerCertificate` or weakens a stricter caller
setting. Authentication modes that require
`Microsoft.Data.SqlClient.Extensions.Azure` are not bundled by this checkpoint.

## Runtime and bitness status

The project targets `net10.0` and does not bundle a SQL Server engine or other
server-side native runtime. Microsoft.Data.SqlClient brings its own managed and
platform transport dependencies, including transitive SNI runtime assets where
the package requires them. The generic CLI has no SQL Server project or package
reference. Bundle publication places this complete provider closure, its
resolved dependency inventory, and the applicable third-party license notices
only in `adapters/sqlserver` with the fixed worker.

This checkpoint has provider-absent unit, process-protocol, dependency-graph,
and published-output isolation coverage. It is not live runtime qualification.
No Windows x86/Arm64, Linux, macOS, container, integrated-authentication,
Kerberos, client-certificate, or platform-specific SNI lane is publicly
qualified yet. Before the adapter is packable, every advertised RID/bitness
and authentication/certificate mode must pass a published-output smoke test
plus a live least-privilege SQL Server fixture. A successful compile, bundle
smoke test, or TCP connection is not qualification evidence.

## Consistency and bounds

Catalog queries share one connection, but SQL Server metadata is not claimed
to be a transactionally coherent snapshot. The catalog therefore reports
`BestEffort` consistency. Permission-token or denial drift during the read is
detected, but concurrent DDL is not excluded. Later live qualification must
prove repeatable output with a restricted account.

Fixed ceilings currently allow at most 4,096 schemas, 10,000 tables, and
20,000 columns, with independent and aggregate ceilings for keys, indexes,
index columns, foreign keys, foreign-key columns, checks, sequences, effective
tokens, denials, views, triggers, trigger events, routines, parameters, modules,
expression dependencies, full-text catalogs/stoplists/property lists/indexes
and columns, data spaces, partition functions/parameters/ranges/schemes and
destinations, heap/index partitions, XML indexes and promoted paths, spatial
indexes and tessellations, hash indexes, and SQL Server 2025 JSON indexes and
paths. Additional ceilings cover names, individual index paths, individual and
aggregate expressions, partition-boundary bytes, and total retained metadata.
Crossing a reader or retained-metadata ceiling fails the inspection rather than
returning a truncated catalog. Default, computed, check, index-filter, and SQL
module text, XML/JSON index paths, plus partition-boundary bytes are read and
hashed only in memory; ScriptDom analysis is additionally bounded by fixed
input, token, nesting, AST-node, statement, and parse-error ceilings. A
per-definition parser ceiling becomes an explicit compatibility blocker;
crossing an aggregate parser ceiling fails the inspection. Durable facets
retain only fixed parser status, dialect, root, count, error-number,
source-position, and digest facts rather than raw SQL, token text, identifier
text, literals, comments, parser messages, or raw partition values. A
syntactically parsed definition remains blocked until later binding, lowering,
and target proof. Unresolved, external, ambiguous, or cyclic dependency shapes
remain explicit blockers rather than invented executable ordering.

ScriptDom does not expose a cancellation hook inside its parse call.
Cancellation is checked during input reading and bounded AST traversal, and
before and after parsing. The optional CLI distribution now runs parsing in a
killable companion worker; cancellation or a process-protocol limit terminates
that process tree. This closes the non-cooperative parse gap for the CLI path,
but direct SDK use remains in-process and hostile-source parsing still requires
the deferred published-runtime and live-server qualification before any
shipping claim.
Sequence fingerprints retain only static definition facts and exclude volatile
current, last-used, and exhaustion values.

## Retained row capture

`SqlServerRetainedCapture.CaptureAsync` creates a provider-neutral retained
migration package from the deliberately narrow SQL Server subset that can be
read and replayed deterministically. Capture uses one non-pooled connection
with read-only application intent and one `SNAPSHOT` transaction for both the
catalog inventory and every retained row. The source database must report
`snapshot_isolation_state` as `ON`; capture otherwise fails closed. The
content digest becomes the retained catalog fingerprint, and the versioned
snapshot identity is `sqlserver-retained:` followed by that digest.

Only ordinary disk-based tables whose complete projected column set excludes
identity, defaulted, computed, rowversion, user-defined, XML, generated,
encrypted, masked, sparse, hidden, FILESTREAM, and other special column shapes
are retained. Supported scalar projections are bounded, lossless integers,
booleans, precision-38 decimals, binary32/binary64 floating point with explicit
`binaryWidth`, strict Unicode text, binary, GUID, date, time, datetime, and
datetime-offset values. A non-null integer primary key is preferred for the
full `ORDER BY`; a safe non-null integer unique constraint is the only
fallback. Heaps and tables without such an ordering key are cataloged with
`migrationDataAvailable=false` and a stable
`migrationDataUnavailableReason`; they are never scanned unordered.
The table inventory also records enabled row-level-security `FILTER`
predicates and whether the caller has complete security-policy metadata
visibility. A table with an enabled filter, or one for which that inventory
cannot be proven complete, is cataloged as retained-data unavailable and is
never scanned. The corresponding analyzer evidence and diagnostics remain in
the retained catalog.

Caller-selectable ceilings bound tables, columns, rows per table, total rows,
scalar bytes, row bytes, and package bytes. Per-table query timeout defaults
to 1,800 seconds and is bounded from 1 through 86,400 seconds; cancellation is
the primary operator stop. The retained envelope requires a row-byte bound of
at least 5 and a package-byte bound of at least 13. Positive configured bounds
below those minima, and configured or fixed capture-limit failures, use
`SqlServerRetainedCaptureLimitException`. Cleanup always attempts rollback,
transaction disposal, and connection disposal; provider-only cleanup failures
do not replace a completed publication or an earlier capture failure. Public
error text never includes connection material, SQL text, identifiers, or
source values.

The retained catalog removes only the analyzer's schema-only
`MIG-SQLSERVER-INVENTORY-PARTIAL-001` and
`MIG-SQLSERVER-LIVE-QUALIFICATION-PENDING-001` diagnostics, preserves every
other object and diagnostic, and adds one bound warning that live platform,
authentication, least-privilege, and differential qualification is still
deferred. Offline tests cover admissibility, ordering, identifier quoting,
scalar canonicalization, limits, catalog transformation, source disposal,
package verification, and replay. They do not constitute live SQL Server
qualification.

## Dependencies

The resolved worker package closure is inventoried in
`THIRD-PARTY-NOTICES.md`. Its MIT-licensed packages include
Microsoft.Data.SqlClient and Microsoft.SqlServer.TransactSql.ScriptDom.
Microsoft.Data.SqlClient.SNI.runtime 6.0.2 is not MIT; its separate Microsoft
Software License Terms accompany the worker under `licenses/`. This remains a
non-packable, non-shipping distribution pending the deferred runtime, live
server, and legal qualification lanes.
