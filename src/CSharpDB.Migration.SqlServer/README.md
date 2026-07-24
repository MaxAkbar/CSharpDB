# CSharpDB.Migration.SqlServer

This optional project is the first SQL Server readiness checkpoint for the
CSharpDB migration tooling. It inspects server and database facts plus schemas,
ordinary user tables, columns, defaults, identity columns, and computed-column
metadata. It does not copy SQL Server rows or write to either the source or a
CSharpDB target.

The intended qualification boundary is on-premises SQL Server 2019, 2022, and
2025 at their default compatibility levels 150, 160, and 170. These lanes
remain provisional until exact-tag live fixtures pass; the analyzer records
that pending proof explicitly. Other engine variants, major versions, and
compatibility levels remain visible in the catalog but receive a stable
unqualified-source diagnostic. Azure SQL Database, Azure SQL Managed Instance,
Synapse, Fabric, and other compatible services are not silently treated as
equivalent to the intended on-premises products.

This checkpoint is intentionally non-shipping. Keys, foreign keys, checks,
indexes, sequences, views, triggers, routines, module/dependency analysis,
ScriptDom analysis, CSharpDB DDL previews, CLI integration, and live server
qualification are later Phase 7A checkpoints. A SQL Server data importer is a
separately approved follow-on.

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
`VIEW DEFINITION` evidence is not treated as proof of completeness because an
object- or schema-level `DENY` can still narrow visibility. Until an effective
per-object permission scan is implemented, only sysadmin membership proves
complete visibility; other evidence is reported as unknown or incomplete and
blocks planning.

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
`BestEffort` consistency. Later live qualification must detect concurrent DDL
and prove repeatable output with a restricted account.

Fixed ceilings currently allow at most 4,096 schemas, 10,000 tables, and
20,000 columns. Additional ceilings cover names; individual and aggregate
default/computed expressions; and total retained metadata. Crossing a ceiling
fails the inspection rather than returning a truncated catalog. Default and
computed SQL text is read and hashed only in memory; parsing remains deferred,
and durable facets retain bounded facts and digests rather than the raw
expression.

## Dependency

Microsoft.Data.SqlClient 7.0.2 is used under the MIT License. See
`THIRD-PARTY-NOTICES.md`.
