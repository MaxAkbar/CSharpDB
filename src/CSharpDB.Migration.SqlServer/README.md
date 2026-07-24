# CSharpDB.Migration.SqlServer

This optional project is the bounded SQL Server readiness analyzer for the
CSharpDB migration tooling. It inspects server and database facts plus schemas,
ordinary user tables, columns, defaults, identity and computed-column metadata,
primary and unique keys, foreign keys, check constraints, table indexes, and
sequences. It also inventories user views, database and object triggers,
procedures and functions, routine parameters, bounded SQL module facts, trigger
events, and declared SQL expression dependencies. It does not copy SQL Server
rows or write to either the source or a CSharpDB target. Available default,
computed-column, check, filtered-index, and SQL module definitions receive
bounded, syntax-only ScriptDom analysis. Parsing does not imply that an
expression or module has been bound, lowered, or accepted by CSharpDB.

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
rewritten, scratch-executed, or differentially validated. Indexed-view physical
details, full-text and physical partition inventory, CSharpDB DDL previews, CLI
integration, and live server qualification are later Phase 7A checkpoints. A
SQL Server data importer is a separately approved follow-on.

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

The caller supplies connection material at runtime. Raw connection strings,
account names, access material, and endpoints are not written to migration
artifacts. The selected database name remains visible as required catalog
metadata, while the durable source identity uses a one-way digest of the
normalized endpoint and database scope. Public adapter errors are generic;
provider exceptions are not retained because their text can contain connection
material.

`Microsoft.Data.SqlClient` 7.0.2 keeps encryption mandatory by default. This
adapter never enables `TrustServerCertificate` or weakens a stricter caller
setting. Authentication modes that require
`Microsoft.Data.SqlClient.Extensions.Azure` are not bundled by this checkpoint.

## Runtime and bitness status

The project targets `net10.0` and does not bundle a SQL Server engine or other
server-side native runtime. Microsoft.Data.SqlClient brings its own managed and
platform transport dependencies, including transitive SNI runtime assets where
the package requires them. Those assets remain isolated in this optional
adapter.

This checkpoint has build and provider-absent unit coverage on the current
Windows x64 development lane only. No Windows x86/Arm64, Linux, macOS,
container, integrated-authentication, Kerberos, client-certificate, or
platform-specific SNI lane is publicly qualified yet. Before the adapter is
packable, every advertised RID/bitness and authentication/certificate mode
must pass a published-output smoke test plus a live least-privilege SQL Server
fixture. A successful compile or TCP connection is not qualification evidence.

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
and expression dependencies. Additional ceilings cover names, individual and
aggregate expressions, and total retained metadata. Crossing a reader or
retained-metadata ceiling fails the inspection rather than returning a
truncated catalog. Default, computed, check, index-filter, and SQL module text
is read and hashed only in memory; ScriptDom analysis is additionally bounded
by fixed input, token, nesting, AST-node, statement, and parse-error ceilings.
A per-definition parser ceiling becomes an explicit compatibility blocker;
crossing an aggregate parser ceiling fails the inspection. Durable facets
retain only fixed parser status, dialect, root, count, error-number,
source-position, and digest facts rather than raw SQL, token text, identifier
text, literals, comments, or parser messages. A syntactically parsed definition
remains blocked until later binding, lowering, and target proof. Unresolved,
external, ambiguous, or cyclic dependency shapes remain explicit blockers
rather than invented executable ordering.

ScriptDom does not expose a cancellation hook inside its parse call.
Cancellation is checked during input reading and bounded AST traversal, and
before and after parsing. Isolation in a killable worker remains a prerequisite
before qualifying hostile-source parsing for shipping use.
Sequence fingerprints retain only static definition facts and exclude volatile
current, last-used, and exhaustion values.

## Dependency

Microsoft.Data.SqlClient and Microsoft.SqlServer.TransactSql.ScriptDom are used
under the MIT License. See `THIRD-PARTY-NOTICES.md`.
