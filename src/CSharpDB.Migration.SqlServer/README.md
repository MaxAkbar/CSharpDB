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

## Dependencies

The resolved worker package closure is inventoried in
`THIRD-PARTY-NOTICES.md`. Its MIT-licensed packages include
Microsoft.Data.SqlClient and Microsoft.SqlServer.TransactSql.ScriptDom.
Microsoft.Data.SqlClient.SNI.runtime 6.0.2 is not MIT; its separate Microsoft
Software License Terms accompany the worker under `licenses/`. This remains a
non-packable, non-shipping distribution pending the deferred runtime, live
server, and legal qualification lanes.
