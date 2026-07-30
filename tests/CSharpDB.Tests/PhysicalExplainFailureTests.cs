using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class PhysicalExplainFailureTests
{
    private const string PhysicalProfileDiagnosticsDataKey =
        "CSharpDB.PhysicalProfileDiagnostics";
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ExplainAnalyze_CancellationDuringExecutionPropagates()
    {
        const string secret = "phase7-cancel-secret-4d18";
        using var executionCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(Ct);
        int invocationCount = 0;
        var options = new DatabaseOptions().ConfigureFunctions(functions =>
            functions.AddScalar(
                "Phase7Cancel",
                1,
                new DbScalarFunctionOptions(
                    ReturnType: DbType.Integer,
                    IsDeterministic: false),
                (_, arguments) =>
                {
                    if (Interlocked.Increment(ref invocationCount) == 1)
                        executionCancellation.Cancel();

                    return arguments[0];
                }));

        await using Database db = await Database.OpenInMemoryAsync(options, Ct);
        await ExecuteNonQueryAsync(
            db,
            """
            CREATE TABLE phase7_cancel_source (
                id INTEGER PRIMARY KEY,
                payload TEXT
            )
            """);
        await ExecuteNonQueryAsync(
            db,
            """
            INSERT INTO phase7_cancel_source VALUES
                (1, 'one'),
                (2, 'two'),
                (3, 'three')
            """);

        OperationCanceledException cancellation =
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () =>
                {
                    await using QueryResult result = await db.ExecuteAsync(
                        $"""
                        EXPLAIN ANALYZE
                        SELECT Phase7Cancel(id)
                        FROM phase7_cancel_source
                        WHERE payload <> '{secret}'
                        """,
                        executionCancellation.Token);
                    _ = await result.ToListAsync(executionCancellation.Token);
                });

        Assert.True(Volatile.Read(ref invocationCount) >= 1);
        Assert.Equal(
            executionCancellation.Token,
            cancellation.CancellationToken);
        AssertFailureDiagnostics(
            cancellation,
            expectedStatus: "cancelled",
            secret);
    }

    [Fact]
    public async Task ExplainAnalyze_ExecutionErrorPropagatesInsteadOfReturningPlanRows()
    {
        const string secret = "phase7-error-secret-923c";
        int invocationCount = 0;
        var options = new DatabaseOptions().ConfigureFunctions(functions =>
            functions.AddScalar(
                "Phase7Fail",
                1,
                new DbScalarFunctionOptions(
                    ReturnType: DbType.Integer,
                    IsDeterministic: false),
                (_, _) =>
                {
                    Interlocked.Increment(ref invocationCount);
                    throw new Phase7ExecutionException(
                        "phase 7 execution failure");
                }));

        await using Database db = await Database.OpenInMemoryAsync(options, Ct);
        await ExecuteNonQueryAsync(
            db,
            "CREATE TABLE phase7_error_source (id INTEGER PRIMARY KEY)");
        await ExecuteNonQueryAsync(
            db,
            "INSERT INTO phase7_error_source VALUES (1), (2)");

        CSharpDbException error =
            await Assert.ThrowsAsync<CSharpDbException>(
                async () =>
                {
                    await using QueryResult result = await db.ExecuteAsync(
                        """
                        EXPLAIN ANALYZE
                        SELECT Phase7Fail(id)
                        FROM phase7_error_source
                        WHERE id > 0 AND 'phase7-error-secret-923c' = 'phase7-error-secret-923c'
                        """,
                        Ct);
                    _ = await result.ToListAsync(Ct);
                });

        Assert.Equal(ErrorCode.Unknown, error.Code);
        Assert.Contains("phase 7 execution failure", error.Message);
        Phase7ExecutionException inner =
            Assert.IsType<Phase7ExecutionException>(error.InnerException);
        Assert.Equal("phase 7 execution failure", inner.Message);
        Assert.Equal(1, Volatile.Read(ref invocationCount));
        AssertFailureDiagnostics(
            error,
            expectedStatus: "error",
            secret);
    }

    [Fact]
    public async Task ExplainAnalyze_UnexpectedStreamFailureAttachesSafePartialDiagnostics()
    {
        const string secret = "phase7-stream-secret-e104";
        string dbDirectory = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_phase7_profile_failure_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dbDirectory);

        try
        {
            string dbPath = Path.Combine(dbDirectory, "profile_failure.db");
            string archivePath = Path.Combine(
                dbDirectory,
                "profile_failure.csdbtable");

            await using (Database db = await Database.OpenAsync(dbPath, Ct))
            {
                await ExecuteNonQueryAsync(
                    db,
                    "CREATE TABLE phase7_archive_source (id INTEGER, payload TEXT)");
                await ExecuteNonQueryAsync(
                    db,
                    "INSERT INTO phase7_archive_source VALUES (1, 'one')");

                TableSchema schema =
                    Assert.IsType<TableSchema>(
                        db.GetTableSchema("phase7_archive_source"));
                await using (QueryResult source = await db.ExecuteAsync(
                    "SELECT * FROM phase7_archive_source",
                    Ct))
                {
                    List<DbValue[]> rows = await source.ToListAsync(Ct);
                    await TableArchiveWriter.WriteAsync(
                        archivePath,
                        schema,
                        TableArchiveWriter.ToAsyncRows(rows, Ct),
                        Ct);
                }

                await ExecuteNonQueryAsync(
                    db,
                    """
                    CREATE EXTERNAL TABLE phase7_broken_archive
                    FROM 'profile_failure.csdbtable'
                    """);

                await using (var archive = new FileStream(
                    archivePath,
                    FileMode.Open,
                    FileAccess.ReadWrite,
                    FileShare.Read))
                {
                    var header = new byte[76];
                    await archive.ReadExactlyAsync(header, Ct);
                    long manifestOffset =
                        BinaryPrimitives.ReadInt64LittleEndian(
                            header.AsSpan(24));
                    int manifestLength =
                        BinaryPrimitives.ReadInt32LittleEndian(
                            header.AsSpan(32));
                    long rowsOffset =
                        BinaryPrimitives.ReadInt64LittleEndian(
                            header.AsSpan(36));
                    int rowsLength = checked((int)
                        BinaryPrimitives.ReadInt64LittleEndian(
                            header.AsSpan(44)));

                    archive.Position = rowsOffset;
                    await archive.WriteAsync(new byte[sizeof(int)], Ct);

                    var rows = new byte[rowsLength];
                    archive.Position = rowsOffset;
                    await archive.ReadExactlyAsync(rows, Ct);
                    string replacementDigest = Convert.ToHexString(
                            SHA256.HashData(rows))
                        .ToLowerInvariant();

                    var manifestBytes = new byte[manifestLength];
                    archive.Position = manifestOffset;
                    await archive.ReadExactlyAsync(manifestBytes, Ct);
                    using JsonDocument manifest =
                        JsonDocument.Parse(manifestBytes);
                    string originalDigest = Assert.IsType<string>(
                        manifest.RootElement
                            .GetProperty("digests")
                            .GetProperty("rows")
                            .GetString());
                    byte[] originalDigestBytes =
                        Encoding.UTF8.GetBytes(originalDigest);
                    byte[] replacementDigestBytes =
                        Encoding.UTF8.GetBytes(replacementDigest);
                    int digestOffset = manifestBytes.AsSpan().IndexOf(
                        originalDigestBytes);
                    Assert.True(digestOffset >= 0);
                    replacementDigestBytes.CopyTo(
                        manifestBytes.AsSpan(digestOffset));

                    archive.Position = manifestOffset;
                    await archive.WriteAsync(manifestBytes, Ct);
                }

                InvalidDataException error =
                    await Assert.ThrowsAsync<InvalidDataException>(
                        async () =>
                        {
                            await using QueryResult result =
                                await db.ExecuteAsync(
                                    $"""
                                    EXPLAIN ANALYZE
                                    SELECT *
                                    FROM phase7_broken_archive
                                    WHERE payload <> '{secret}'
                                    """,
                                    Ct);
                            _ = await result.ToListAsync(Ct);
                        });

                Assert.IsNotType<CSharpDbException>(error);
                string diagnostics = Assert.IsType<string>(
                    error.Data[PhysicalProfileDiagnosticsDataKey]);
                Assert.Contains(
                    "Physical profile diagnostics (partial):",
                    diagnostics);
                Assert.Contains("status=error", diagnostics);
                Assert.Contains("/rows=", diagnostics);
                Assert.Contains("/loops=", diagnostics);
                Assert.True(diagnostics.Length <= 2048);
                Assert.DoesNotContain(
                    secret,
                    diagnostics,
                    StringComparison.Ordinal);
            }
        }
        finally
        {
            if (Directory.Exists(dbDirectory))
                Directory.Delete(dbDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task ExplainAnalyze_LimitReportsSafelyStoppedChildAsPartial()
    {
        await using Database db = await Database.OpenInMemoryAsync(Ct);
        await ExecuteNonQueryAsync(
            db,
            """
            CREATE TABLE phase7_limit_source (
                id INTEGER PRIMARY KEY,
                payload TEXT
            )
            """);
        await ExecuteNonQueryAsync(
            db,
            """
            INSERT INTO phase7_limit_source VALUES
                (1, 'one'),
                (2, 'two'),
                (3, 'three')
            """);

        CapturedPlan plan = await ExecutePlanAsync(
            db,
            "EXPLAIN ANALYZE SELECT * FROM phase7_limit_source LIMIT 1");

        DbValue[] limit = Assert.Single(
            plan.Rows,
            row => plan.Text(row, "operator_type") == "limit");
        Assert.Equal(1, plan.Integer(limit, "actual_rows"));
        Assert.Equal("completed", plan.Text(limit, "status"));
        Assert.Null(plan.Text(limit, "diagnostic_code"));

        DbValue[] partialScan = Assert.Single(
            plan.Rows,
            row =>
                plan.Text(row, "operator_type") is "table_scan" or "compact_table_scan" &&
                plan.Text(row, "status") == "partial");
        Assert.Equal(1, plan.Integer(partialScan, "actual_rows"));
        Assert.Equal(
            "execution_stopped_early",
            plan.Text(partialScan, "diagnostic_code"));
    }

    private static async Task ExecuteNonQueryAsync(Database db, string sql)
    {
        await using QueryResult result = await db.ExecuteAsync(sql, Ct);
        Assert.False(result.IsQuery);
    }

    private static void AssertFailureDiagnostics(
        Exception error,
        string expectedStatus,
        string secret)
    {
        string diagnostics = Assert.IsType<string>(
            error.Data[PhysicalProfileDiagnosticsDataKey]);
        Assert.Contains("Physical profile diagnostics (partial):", diagnostics);
        Assert.Contains($"status={expectedStatus}", diagnostics);
        Assert.Contains("/rows=", diagnostics);
        Assert.Contains("/loops=", diagnostics);
        Assert.True(diagnostics.Length <= 2048);
        Assert.Contains(diagnostics, error.Message);
        Assert.DoesNotContain(secret, diagnostics, StringComparison.Ordinal);
        Assert.DoesNotContain(secret, error.Message, StringComparison.Ordinal);
    }

    private static async Task<CapturedPlan> ExecutePlanAsync(
        Database db,
        string sql)
    {
        await using QueryResult result = await db.ExecuteAsync(sql, Ct);
        Assert.True(result.IsQuery);
        ColumnDefinition[] schema = result.Schema.ToArray();
        List<DbValue[]> rows = await result.ToListAsync(Ct);
        return new CapturedPlan(schema, rows);
    }

    private sealed class CapturedPlan(
        ColumnDefinition[] schema,
        List<DbValue[]> rows)
    {
        private readonly Dictionary<string, int> _ordinals = schema
            .Select((column, ordinal) => (column.Name, ordinal))
            .ToDictionary(
                static item => item.Name,
                static item => item.ordinal,
                StringComparer.OrdinalIgnoreCase);

        internal List<DbValue[]> Rows { get; } = rows;

        internal string? Text(DbValue[] row, string columnName)
        {
            DbValue value = row[Ordinal(columnName)];
            return value.IsNull ? null : value.AsText;
        }

        internal long? Integer(DbValue[] row, string columnName)
        {
            DbValue value = row[Ordinal(columnName)];
            return value.IsNull ? null : value.AsInteger;
        }

        private int Ordinal(string columnName)
            => _ordinals.TryGetValue(columnName, out int ordinal)
                ? ordinal
                : throw new Xunit.Sdk.XunitException(
                    $"Expected plan column '{columnName}' was not present.");
    }

    private sealed class Phase7ExecutionException(string message)
        : Exception(message);
}
