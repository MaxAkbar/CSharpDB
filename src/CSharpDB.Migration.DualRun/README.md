# CSharpDB migration dual-run validation

This project compares a declared read-only query against a source database and
CSharpDB. It is cutover evidence, not a replication mechanism and not a
substitute for row-count/checksum validation.

## Install

```powershell
dotnet add package CSharpDB.Migration.DualRun --version 4.3.0
```

Install the ADO.NET provider for each external source separately. For the
SQLite example below, add `Microsoft.Data.Sqlite`.

The validator:

- runs source and target concurrently through read-only executor contracts;
- binds typed parameters rather than interpolating values into SQL;
- canonicalizes schema and values with `csharpdb-canon-v1`;
- compares row order when `Ordered` is selected, or a duplicate-preserving
  multiset when `Unordered` is selected;
- enforces row, column, cell, total-byte, timeout, and mismatch-detail bounds;
- treats provider errors, timeouts, safety rejections, and limit exhaustion as
  `Inconclusive`, never as a pass;
- emits a digest-verified stable JSON report containing no SQL, parameter
  values, or row values.

## SQLite source to CSharpDB example

This example uses two independent safety controls for the source:

1. the query must exactly match a reviewed query pack; and
2. SQLite is opened in read-only mode.

The allowlist is deliberately byte-for-byte. A comment, whitespace change,
second statement, or other edit requires review and a new query-pack version.

```csharp
using System.Data.Common;
using CSharpDB.Migration.Canonicalization;
using CSharpDB.Migration.DualRun;
using Microsoft.Data.Sqlite;

const string sql =
    "SELECT id, name FROM customers WHERE id >= @minimum ORDER BY id";
// These are immutable, independently captured copies. Their capture receipts
// produced the identities below; do not substitute a changing live database.
const string sourceSnapshotPath = "legacy.dualrun-snapshot.sqlite";
const string targetSnapshotPath = "migrated.dualrun-snapshot.csdb";
const string sourceSnapshotIdentity =
    "sqlite-backup:sha256:<digest-from-retained-snapshot>";
const string targetSnapshotIdentity =
    "csharpdb-retained-snapshot/v1:<bytes>:sha256:<digest>";
CancellationToken cancellationToken = CancellationToken.None;

var approvedQueries = new ExactSqlAllowListReadOnlyValidator(
    "customer-cutover-query-pack/v1",
    [sql]);

var source = new DbConnectionDualRunQueryExecutor(
    providerId: "sqlite",
    snapshotIdentity: sourceSnapshotIdentity,
    connectionFactory: async cancellationToken =>
    {
        DbConnection connection = new SqliteConnection(
            $"Data Source={sourceSnapshotPath};Mode=ReadOnly;Pooling=False");
        await connection.OpenAsync(cancellationToken);
        await using DbCommand readOnly = connection.CreateCommand();
        readOnly.CommandText = "PRAGMA query_only = ON;";
        await readOnly.ExecuteNonQueryAsync(cancellationToken);
        readOnly.CommandText = "PRAGMA query_only;";
        if (Convert.ToInt32(
                await readOnly.ExecuteScalarAsync(cancellationToken),
                System.Globalization.CultureInfo.InvariantCulture) != 1)
        {
            throw new InvalidOperationException("SQLite query_only was not established.");
        }
        return connection;
    },
    statementValidator: approvedQueries,
    connectionIsReadOnly: true,
    providerErrorCodeFactory: exception =>
        exception is SqliteException sqlite
            ? $"SQLITE_{sqlite.SqliteErrorCode}"
            : "SQLITE_DB_ERROR");

var target = new CSharpDbDualRunQueryExecutor(
    $"Data Source={targetSnapshotPath};Pooling=false",
    targetSnapshotIdentity);

var queryCase = new DualRunQueryCase
{
    CaseId = "customers-by-id",
    SourceSnapshotIdentity = sourceSnapshotIdentity,
    TargetSnapshotIdentity = targetSnapshotIdentity,
    SourceSql = sql,
    TargetSql = sql,
    Ordering = DualRunOrdering.Ordered,
    Parameters =
    [
        new DualRunParameter
        {
            Name = "@minimum",
            Type = CanonicalType.Int64,
            Value = 1L,
        },
    ],
    Columns =
    [
        new DualRunColumnContract
        {
            Name = "id",
            Type = CanonicalType.Int64,
        },
        new DualRunColumnContract
        {
            Name = "name",
            Type = CanonicalType.Text,
        },
    ],
    Limits = new DualRunLimits
    {
        MaxRows = 100_000,
        MaxColumns = 16,
        MaxCellBytes = 1024 * 1024,
        MaxTotalCanonicalBytesPerEndpoint = 256L * 1024 * 1024,
        MaxMismatchDetails = 100,
        TimeoutPerEndpoint = TimeSpan.FromMinutes(2),
    },
};

DualRunReport report = await new DualRunValidator().ValidateAsync(
    queryCase,
    source,
    target,
    cancellationToken);

string json = DualRunReportSerializer.Serialize(report);
await File.WriteAllTextAsync("customers.dual-run.json", json, cancellationToken);

if (report.Status != DualRunValidationStatus.Passed)
    throw new InvalidOperationException("Dual-run evidence does not permit cutover.");
```

`Microsoft.Data.Sqlite` is supplied by the caller; the dual-run library depends
only on the ADO.NET abstractions for external sources.

The generic ADO.NET executor refuses `connectionIsReadOnly: false`. The exact
SQL allowlist proves that the reviewed bytes have not changed, but membership
in an allowlist is not a dialect-level proof that a statement is read-only.
The independently enforced provider connection or credential is therefore
mandatory. For SQLite, use both `Mode=ReadOnly` and `PRAGMA query_only = ON`.

## Result contracts

Declare `Columns` when the source and CSharpDB intentionally use different
physical representations for one logical value. For example, a source `DATE`
can be compared with an ISO date stored as CSharpDB text by declaring
`CanonicalType.Date`. Conversion is strict and fail-closed.

If `Columns` is omitted, the canonical schema inferred by each provider must
match. Column names are Unicode-normalized but remain case-sensitive.

Use `Ordered` only when both query texts establish the same deterministic order.
For queries without a meaningful order, use `Unordered`; duplicate rows still
count and are not collapsed.

## Qualification boundary

The generic source executor is a provider adapter, not proof that any specific
database, driver, authentication mode, TLS configuration, collation, or
snapshot-isolation strategy has been qualified. Use a read-only source identity
or provider mode, a reviewed query pack, and a consistent source
snapshot/watermark.

Every case and executor must carry the same non-secret, content-pinned snapshot
identity for its endpoint. The validator refuses an identity mismatch before
opening either connection and records both identities in the report. The
report also records the installed canonicalization ID and contract hash, and
the invocation digest binds the queries, typed parameters, limits, executors,
snapshot identities, and canonicalization contract. These strings are
contracts supplied by the caller; obtain them from retained-snapshot capture
receipts rather than inventing labels for live databases. Live provider
qualification remains a separate release gate.

The v1 comparer retains one digest per accepted row and, for unordered
comparison, a bounded multiplicity map; it does not spill comparison state to
disk. Set `MaxRows` and `MaxTotalCanonicalBytesPerEndpoint` for the available
memory. Generic ADO.NET providers may materialize a field inside `GetValue`
before the validator can reject it against `MaxCellBytes`, so do not treat the
SDK as an isolation boundary for an untrusted provider or unrestricted query.
A future provider-specific streaming-cell adapter and spill-backed comparer are
separate qualification work.
