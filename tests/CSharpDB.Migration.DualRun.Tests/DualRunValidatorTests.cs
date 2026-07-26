using System.Runtime.CompilerServices;
using CSharpDB.Data;
using CSharpDB.Migration.Canonicalization;
using Microsoft.Data.Sqlite;

namespace CSharpDB.Migration.DualRun.Tests;

public sealed class DualRunValidatorTests
{
    private const string TestSnapshotIdentity =
        "test-snapshot:sha256:0000000000000000000000000000000000000000000000000000000000000000";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task OrderedEquivalentResults_PassAndSerializeWithoutQueryData()
    {
        var source = FakeExecutor.Success(
            "sqlite-source",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "alpha"], [2L, "private-row-value"]));
        var target = FakeExecutor.Success(
            "csharpdb-fake",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1, "alpha"], [2L, "private-row-value"]));
        DualRunQueryCase queryCase = Case(
            parameters:
            [
                new DualRunParameter
                {
                    Name = "@tenant",
                    Type = CanonicalType.Text,
                    Value = "private-parameter-value",
                },
            ]);

        DualRunReport report = await new DualRunValidator().ValidateAsync(
            queryCase,
            source,
            target,
            Ct);

        Assert.Equal(DualRunValidationStatus.Passed, report.Status);
        Assert.Equal(report.Source.SchemaDigest, report.Target.SchemaDigest);
        Assert.Equal(report.Source.ResultDigest, report.Target.ResultDigest);
        Assert.Equal(TestSnapshotIdentity, report.Source.SnapshotIdentity);
        Assert.Equal(TestSnapshotIdentity, report.Target.SnapshotIdentity);
        Assert.Equal(
            DualRunReportFormats.CanonicalizationId,
            report.CanonicalizationId);
        Assert.Equal(
            DualRunReportFormats.CanonicalizationContractHash,
            report.CanonicalizationContractHash);
        Assert.Empty(report.Differences);
        Assert.NotNull(source.LastRequest);
        Assert.Single(source.LastRequest!.Parameters);
        Assert.Equal(TestSnapshotIdentity, source.LastRequest.SnapshotIdentity);
        Assert.Equal(
            DualRunReportFormats.CanonicalizationId,
            source.LastRequest.CanonicalizationId);
        Assert.Equal(
            DualRunReportFormats.CanonicalizationContractHash,
            source.LastRequest.CanonicalizationContractHash);

        string first = DualRunReportSerializer.Serialize(report);
        string second = DualRunReportSerializer.Serialize(report);
        Assert.Equal(first, second);
        Assert.DoesNotContain("private-parameter-value", first, StringComparison.Ordinal);
        Assert.DoesNotContain("private-row-value", first, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT id", first, StringComparison.Ordinal);

        DualRunReport roundTrip = DualRunReportSerializer.Deserialize(first);
        Assert.Equal(first, DualRunReportSerializer.Serialize(roundTrip));
    }

    [Fact]
    public async Task OrderedDifference_ReportsOnlyBoundedOrdinalAndDigest()
    {
        var source = FakeExecutor.Success(
            "source",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "same"], [2L, "secret-source-value"]));
        var target = FakeExecutor.Success(
            "target",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "same"], [2L, "secret-target-value"]));

        DualRunReport report = await new DualRunValidator().ValidateAsync(
            Case(),
            source,
            target,
            Ct);

        Assert.Equal(DualRunValidationStatus.Different, report.Status);
        DualRunDifference difference = Assert.Single(
            report.Differences,
            static item => item.Code == DualRunDifferenceCodes.OrderedRowMismatch);
        Assert.Equal(1, difference.RowOrdinal);
        Assert.Equal(64, difference.RowDigest!.Length);
        string json = DualRunReportSerializer.Serialize(report, writeIndented: false);
        Assert.DoesNotContain("secret-source-value", json, StringComparison.Ordinal);
        Assert.DoesNotContain("secret-target-value", json, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ZeroMismatchDetailLimit_IsRejectedBeforeExecution()
    {
        var source = FakeExecutor.Success(
            "source",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "source"]));
        var target = FakeExecutor.Success(
            "target",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "target"]));
        DualRunQueryCase queryCase = Case(
            limits: new DualRunLimits
            {
                MaxRows = 10,
                MaxColumns = 2,
                MaxCellBytes = 1024,
                MaxTotalCanonicalBytesPerEndpoint = 4096,
                MaxMismatchDetails = 0,
                TimeoutPerEndpoint = TimeSpan.FromSeconds(5),
            });

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await new DualRunValidator().ValidateAsync(
                queryCase,
                source,
                target,
                Ct));

        Assert.Null(source.LastRequest);
        Assert.Null(target.LastRequest);
    }

    [Theory]
    [InlineData(DualRunOrdering.Ordered)]
    [InlineData(DualRunOrdering.Unordered)]
    public async Task CappedMismatchDetails_CannotTurnDifferentResultsIntoPass(
        DualRunOrdering ordering)
    {
        var source = FakeExecutor.Success(
            "source",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "source-one"], [2L, "source-two"]));
        var target = FakeExecutor.Success(
            "target",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "target-one"], [2L, "target-two"]));
        DualRunQueryCase queryCase = Case(
            ordering: ordering,
            limits: new DualRunLimits
            {
                MaxRows = 10,
                MaxColumns = 2,
                MaxCellBytes = 1024,
                MaxTotalCanonicalBytesPerEndpoint = 4096,
                MaxMismatchDetails = 1,
                TimeoutPerEndpoint = TimeSpan.FromSeconds(5),
            });

        DualRunReport report = await new DualRunValidator().ValidateAsync(
            queryCase,
            source,
            target,
            Ct);

        Assert.Equal(DualRunValidationStatus.Different, report.Status);
        Assert.NotEqual(report.Source.ResultDigest, report.Target.ResultDigest);
        Assert.Single(
            report.Differences,
            static item => item.Code is
                DualRunDifferenceCodes.OrderedRowMismatch or
                DualRunDifferenceCodes.UnorderedRowMultiplicityMismatch);
    }

    [Fact]
    public async Task UnorderedComparison_PreservesDuplicateMultiplicity()
    {
        var source = FakeExecutor.Success(
            "source",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "one"], [2L, "two"], [2L, "two"]));
        var reordered = FakeExecutor.Success(
            "target",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([2L, "two"], [1L, "one"], [2L, "two"]));
        DualRunQueryCase queryCase = Case(ordering: DualRunOrdering.Unordered);

        DualRunReport passing = await new DualRunValidator().ValidateAsync(
            queryCase,
            source,
            reordered,
            Ct);

        Assert.Equal(DualRunValidationStatus.Passed, passing.Status);
        Assert.Equal(passing.Source.ResultDigest, passing.Target.ResultDigest);

        var missingDuplicate = FakeExecutor.Success(
            "target",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([2L, "two"], [1L, "one"]));
        DualRunReport different = await new DualRunValidator().ValidateAsync(
            queryCase,
            source,
            missingDuplicate,
            Ct);

        Assert.Equal(DualRunValidationStatus.Different, different.Status);
        Assert.Contains(
            different.Differences,
            static item =>
                item.Code == DualRunDifferenceCodes.UnorderedRowMultiplicityMismatch &&
                item.SourceCount == 2 &&
                item.TargetCount == 1);
    }

    [Fact]
    public async Task DeclaredSchemaMismatch_IsDifferentEvenWhenRowsMatch()
    {
        var source = FakeExecutor.Success(
            "source",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "one"]));
        var target = FakeExecutor.Success(
            "target",
            Columns(("ID", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "one"]));

        DualRunReport report = await new DualRunValidator().ValidateAsync(
            Case(),
            source,
            target,
            Ct);

        Assert.Equal(DualRunValidationStatus.Different, report.Status);
        Assert.Contains(
            report.Differences,
            static item =>
                item.Code == DualRunDifferenceCodes.SchemaMismatch &&
                item.Endpoint == "target");
    }

    [Fact]
    public async Task RowLimitFailure_IsInconclusiveAndFailClosed()
    {
        var source = FakeExecutor.Success(
            "source",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "one"], [2L, "two"]));
        var target = FakeExecutor.Success(
            "target",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "one"]));
        DualRunQueryCase queryCase = Case(
            limits: new DualRunLimits
            {
                MaxRows = 1,
                MaxColumns = 2,
                MaxCellBytes = 1024,
                MaxTotalCanonicalBytesPerEndpoint = 4096,
                TimeoutPerEndpoint = TimeSpan.FromSeconds(5),
            });

        DualRunReport report = await new DualRunValidator().ValidateAsync(
            queryCase,
            source,
            target,
            Ct);

        Assert.Equal(DualRunValidationStatus.Inconclusive, report.Status);
        Assert.Equal(DualRunEndpointStatus.Failed, report.Source.Status);
        Assert.Equal(DualRunErrorKind.LimitExceeded, report.Source.Error!.Kind);
        Assert.Equal("DUALRUN_ROW_LIMIT_EXCEEDED", report.Source.Error.Code);
    }

    [Fact]
    public async Task CellLimitFailure_IsInconclusiveAndDoesNotEmitValue()
    {
        string oversized = new('x', 1_024);
        var source = FakeExecutor.Success(
            "source",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, oversized]));
        var target = FakeExecutor.Success(
            "target",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "small"]));
        DualRunQueryCase queryCase = Case(
            limits: new DualRunLimits
            {
                MaxRows = 10,
                MaxColumns = 2,
                MaxCellBytes = 128,
                MaxTotalCanonicalBytesPerEndpoint = 4096,
                TimeoutPerEndpoint = TimeSpan.FromSeconds(5),
            });

        DualRunReport report = await new DualRunValidator().ValidateAsync(
            queryCase,
            source,
            target,
            Ct);

        Assert.Equal(DualRunValidationStatus.Inconclusive, report.Status);
        Assert.Equal("DUALRUN_CELL_LIMIT_EXCEEDED", report.Source.Error!.Code);
        Assert.DoesNotContain(
            oversized,
            DualRunReportSerializer.Serialize(report),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task MatchingProviderErrors_AreStillInconclusive()
    {
        var source = FakeExecutor.Failure(
            "source",
            new DualRunExecutionException(
                DualRunErrorKind.ProviderError,
                "SOURCE_UNAVAILABLE"));
        var target = FakeExecutor.Failure(
            "target",
            new DualRunExecutionException(
                DualRunErrorKind.ProviderError,
                "SOURCE_UNAVAILABLE"));

        DualRunReport report = await new DualRunValidator().ValidateAsync(
            Case(),
            source,
            target,
            Ct);

        Assert.Equal(DualRunValidationStatus.Inconclusive, report.Status);
        Assert.Equal(DualRunEndpointStatus.Failed, report.Source.Status);
        Assert.Equal(DualRunEndpointStatus.Failed, report.Target.Status);
        Assert.Equal(2, report.Differences.Count);
    }

    [Fact]
    public async Task EndpointTimeout_IsCanonicalizedWithoutCancellingCaller()
    {
        var source = FakeExecutor.Success(
            "source",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "one"]),
            executeDelay: TimeSpan.FromSeconds(5));
        var target = FakeExecutor.Success(
            "target",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "one"]));
        DualRunQueryCase queryCase = Case(
            limits: new DualRunLimits
            {
                MaxRows = 10,
                MaxColumns = 2,
                MaxCellBytes = 1024,
                MaxTotalCanonicalBytesPerEndpoint = 4096,
                TimeoutPerEndpoint = TimeSpan.FromMilliseconds(25),
            });

        DualRunReport report = await new DualRunValidator().ValidateAsync(
            queryCase,
            source,
            target,
            Ct);

        Assert.Equal(DualRunValidationStatus.Inconclusive, report.Status);
        Assert.Equal(DualRunErrorKind.TimedOut, report.Source.Error!.Kind);
        Assert.Equal(DualRunEndpointStatus.Succeeded, report.Target.Status);
    }

    [Fact]
    public async Task CallerCancellation_Propagates()
    {
        var source = FakeExecutor.Success(
            "source",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "one"]),
            executeDelay: TimeSpan.FromSeconds(5));
        var target = FakeExecutor.Success(
            "target",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "one"]),
            executeDelay: TimeSpan.FromSeconds(5));
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(25));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await new DualRunValidator().ValidateAsync(
                Case(),
                source,
                target,
                cancellation.Token));
    }

    [Fact]
    public async Task CSharpDbTarget_ExecutesParameterizedReadOnlyQuery()
    {
        string databasePath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_dual_run_{Guid.NewGuid():N}.db");
        string connectionString = $"Data Source={databasePath};Pooling=false";

        try
        {
            await using (var connection = new CSharpDbConnection(connectionString))
            {
                await connection.OpenAsync(Ct);
                await ExecuteNonQueryAsync(
                    connection,
                    "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);");
                await ExecuteNonQueryAsync(
                    connection,
                    "INSERT INTO items (id, name) VALUES (1, 'alpha');");
                await ExecuteNonQueryAsync(
                    connection,
                    "INSERT INTO items (id, name) VALUES (2, 'beta');");
            }

            var source = FakeExecutor.Success(
                "qualified-source-fake",
                Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
                Rows([1L, "alpha"], [2L, "beta"]));
            var target = new CSharpDbDualRunQueryExecutor(
                connectionString,
                TestSnapshotIdentity);
            DualRunQueryCase queryCase = Case(
                sourceSql: "SELECT id, name FROM items WHERE id >= @minimum ORDER BY id",
                targetSql: "SELECT id, name FROM items WHERE id >= @minimum ORDER BY id",
                parameters:
                [
                    new DualRunParameter
                    {
                        Name = "@minimum",
                        Type = CanonicalType.Int64,
                        Value = 1L,
                    },
                ]);

            DualRunReport report = await new DualRunValidator().ValidateAsync(
                queryCase,
                source,
                target,
                Ct);

            Assert.Equal(DualRunValidationStatus.Passed, report.Status);
            Assert.Equal("csharpdb", report.Target.ProviderId);
            Assert.Equal(
                DualRunReadOnlyEnforcement.StatementValidated,
                report.Target.ReadOnlyEnforcement);
        }
        finally
        {
            DeleteIfExists(databasePath);
            DeleteIfExists(databasePath + ".wal");
        }
    }

    [Fact]
    public async Task DbConnectionSource_ValidatesReadOnlySqliteAgainstCSharpDb()
    {
        string sqlitePath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_dual_run_source_{Guid.NewGuid():N}.sqlite");
        string csharpDbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_dual_run_target_{Guid.NewGuid():N}.db");
        const string query = "SELECT id, name FROM items WHERE id >= @minimum ORDER BY id";

        try
        {
            await using (var source = new SqliteConnection(
                             $"Data Source={sqlitePath};Mode=ReadWriteCreate;Pooling=False"))
            {
                await source.OpenAsync(Ct);
                await ExecuteNonQueryAsync(
                    source,
                    "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);");
                await ExecuteNonQueryAsync(
                    source,
                    "INSERT INTO items (id, name) VALUES (1, 'alpha'), (2, 'beta');");
            }

            await using (var target = new CSharpDbConnection(
                             $"Data Source={csharpDbPath};Pooling=false"))
            {
                await target.OpenAsync(Ct);
                await ExecuteNonQueryAsync(
                    target,
                    "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);");
                await ExecuteNonQueryAsync(
                    target,
                    "INSERT INTO items (id, name) VALUES (1, 'alpha');");
                await ExecuteNonQueryAsync(
                    target,
                    "INSERT INTO items (id, name) VALUES (2, 'beta');");
            }

            var guard = new ExactSqlAllowListReadOnlyValidator(
                "sqlite-cutover-query-pack/v1",
                [query]);
            var sourceExecutor = new DbConnectionDualRunQueryExecutor(
                "sqlite",
                TestSnapshotIdentity,
                async cancellationToken =>
                {
                    var connection = new SqliteConnection(
                        $"Data Source={sqlitePath};Mode=ReadOnly;Pooling=False");
                    await connection.OpenAsync(cancellationToken);
                    return connection;
                },
                guard,
                connectionIsReadOnly: true,
                static exception => exception is SqliteException sqlite
                    ? $"SQLITE_{sqlite.SqliteErrorCode}"
                    : "SQLITE_DB_ERROR");
            var targetExecutor = new CSharpDbDualRunQueryExecutor(
                $"Data Source={csharpDbPath};Pooling=false",
                TestSnapshotIdentity);
            DualRunQueryCase queryCase = Case(
                sourceSql: query,
                targetSql: query,
                parameters:
                [
                    new DualRunParameter
                    {
                        Name = "@minimum",
                        Type = CanonicalType.Int64,
                        Value = 1L,
                    },
                ]);

            DualRunReport report = await new DualRunValidator().ValidateAsync(
                queryCase,
                sourceExecutor,
                targetExecutor,
                Ct);

            Assert.Equal(DualRunValidationStatus.Passed, report.Status);
            Assert.Equal(
                DualRunReadOnlyEnforcement.StatementValidatedAndReadOnlyConnection,
                report.Source.ReadOnlyEnforcement);
            Assert.Equal("sqlite-cutover-query-pack/v1", report.Source.ReadOnlyValidatorId);
        }
        finally
        {
            DeleteSqliteFiles(sqlitePath);
            DeleteIfExists(csharpDbPath);
            DeleteIfExists(csharpDbPath + ".wal");
        }
    }

    [Fact]
    public async Task CSharpDbTarget_RejectsMutationBeforeOpeningConnection()
    {
        int factoryCalls = 0;
        var target = new CSharpDbDualRunQueryExecutor(
            TestSnapshotIdentity,
            _ =>
            {
                Interlocked.Increment(ref factoryCalls);
                throw new InvalidOperationException("The connection factory must not run.");
            });
        var source = FakeExecutor.Success(
            "source",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows());
        DualRunQueryCase queryCase = Case(
            targetSql: "DELETE FROM items",
            columns: []);

        DualRunReport report = await new DualRunValidator().ValidateAsync(
            queryCase,
            source,
            target,
            Ct);

        Assert.Equal(0, factoryCalls);
        Assert.Equal(DualRunValidationStatus.Inconclusive, report.Status);
        Assert.Equal(DualRunErrorKind.SafetyRejected, report.Target.Error!.Kind);
        Assert.Equal("DUALRUN_CSHARPDB_READ_ONLY_REQUIRED", report.Target.Error.Code);
    }

    [Fact]
    public async Task DbConnectionSource_RejectsUnapprovedSqlBeforeOpeningConnection()
    {
        int factoryCalls = 0;
        var guard = new ExactSqlAllowListReadOnlyValidator(
            "approved-source-queries/v1",
            ["SELECT id, name FROM items ORDER BY id"]);
        var source = new DbConnectionDualRunQueryExecutor(
            "external-source",
            TestSnapshotIdentity,
            _ =>
            {
                Interlocked.Increment(ref factoryCalls);
                throw new InvalidOperationException("The connection factory must not run.");
            },
            guard,
            connectionIsReadOnly: true);
        var target = FakeExecutor.Success(
            "target",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows());
        DualRunQueryCase queryCase = Case(sourceSql: "DELETE FROM items");

        DualRunReport report = await new DualRunValidator().ValidateAsync(
            queryCase,
            source,
            target,
            Ct);

        Assert.Equal(0, factoryCalls);
        Assert.Equal(DualRunValidationStatus.Inconclusive, report.Status);
        Assert.Equal(DualRunErrorKind.SafetyRejected, report.Source.Error!.Kind);
        Assert.Equal(
            "DUALRUN_SQL_NOT_IN_APPROVED_READ_ONLY_SET",
            report.Source.Error.Code);
    }

    [Fact]
    public void DbConnectionSource_RejectsWritableConnectionContract()
    {
        int factoryCalls = 0;
        var guard = new ExactSqlAllowListReadOnlyValidator(
            "approved-source-queries/v1",
            ["DELETE FROM items"]);

        ArgumentException exception = Assert.Throws<ArgumentException>(
            () => new DbConnectionDualRunQueryExecutor(
                "external-source",
                TestSnapshotIdentity,
                _ =>
                {
                    Interlocked.Increment(ref factoryCalls);
                    throw new InvalidOperationException("The connection factory must not run.");
                },
                guard,
                connectionIsReadOnly: false));

        Assert.Equal("connectionIsReadOnly", exception.ParamName);
        Assert.Equal(0, factoryCalls);
    }

    [Fact]
    public async Task SnapshotIdentityMismatch_IsRejectedBeforeExecution()
    {
        var source = FakeExecutor.Success(
            "source",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows());
        var target = FakeExecutor.Success(
            "target",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows());

        await Assert.ThrowsAsync<ArgumentException>(
            async () => await new DualRunValidator().ValidateAsync(
                Case(targetSnapshotIdentity: "different-snapshot:sha256:" +
                    new string('1', 64)),
                source,
                target,
                Ct));

        Assert.Null(source.LastRequest);
        Assert.Null(target.LastRequest);
    }

    [Fact]
    public async Task CanonicalizationContractMismatch_IsRejectedBeforeExecution()
    {
        var source = FakeExecutor.Success(
            "source",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows());
        var target = FakeExecutor.Success(
            "target",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows());

        await Assert.ThrowsAsync<ArgumentException>(
            async () => await new DualRunValidator().ValidateAsync(
                Case() with { CanonicalizationContractHash = new string('f', 64) },
                source,
                target,
                Ct));

        Assert.Null(source.LastRequest);
        Assert.Null(target.LastRequest);
    }

    [Fact]
    public async Task TamperedReport_IsRejected()
    {
        var executor = FakeExecutor.Success(
            "source",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "one"]));
        DualRunReport report = await new DualRunValidator().ValidateAsync(
            Case(),
            executor,
            executor,
            Ct);
        string json = DualRunReportSerializer.Serialize(report, writeIndented: false);
        string tampered = json.Replace(
            "\"status\":\"Passed\"",
            "\"status\":\"Different\"",
            StringComparison.Ordinal);

        Assert.Throws<InvalidDataException>(() => DualRunReportSerializer.Deserialize(tampered));
    }

    [Fact]
    public async Task ReserializedFalsePass_IsRejected()
    {
        var source = FakeExecutor.Success(
            "source",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "source"]));
        var target = FakeExecutor.Success(
            "target",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "target"]));
        DualRunReport different = await new DualRunValidator().ValidateAsync(
            Case(),
            source,
            target,
            Ct);
        Assert.Equal(DualRunValidationStatus.Different, different.Status);

        DualRunReport forged = different with
        {
            Status = DualRunValidationStatus.Passed,
            Differences = [],
        };

        Assert.Throws<InvalidDataException>(
            () => DualRunReportSerializer.Serialize(forged));
    }

    [Fact]
    public async Task MissingCanonicalizationBinding_IsRejected()
    {
        var executor = FakeExecutor.Success(
            "source",
            Columns(("id", CanonicalType.Int64), ("name", CanonicalType.Text)),
            Rows([1L, "one"]));
        DualRunReport report = await new DualRunValidator().ValidateAsync(
            Case(),
            executor,
            executor,
            Ct);
        string json = DualRunReportSerializer.Serialize(report, writeIndented: false);
        string missingBinding = json.Replace(
            $"\"canonicalizationId\":\"{DualRunReportFormats.CanonicalizationId}\",",
            string.Empty,
            StringComparison.Ordinal);

        Assert.NotEqual(json, missingBinding);
        Assert.Throws<InvalidDataException>(
            () => DualRunReportSerializer.Deserialize(missingBinding));
    }

    private static DualRunQueryCase Case(
        string sourceSql = "SELECT id, name FROM items ORDER BY id",
        string targetSql = "SELECT id, name FROM items ORDER BY id",
        DualRunOrdering ordering = DualRunOrdering.Ordered,
        IReadOnlyList<DualRunParameter>? parameters = null,
        IReadOnlyList<DualRunColumnContract>? columns = null,
        DualRunLimits? limits = null,
        string sourceSnapshotIdentity = TestSnapshotIdentity,
        string targetSnapshotIdentity = TestSnapshotIdentity) =>
        new()
        {
            CaseId = "items-by-id",
            SourceSnapshotIdentity = sourceSnapshotIdentity,
            TargetSnapshotIdentity = targetSnapshotIdentity,
            SourceSql = sourceSql,
            TargetSql = targetSql,
            Ordering = ordering,
            Parameters = parameters ?? [],
            Columns = columns ??
            [
                new DualRunColumnContract { Name = "id", Type = CanonicalType.Int64 },
                new DualRunColumnContract { Name = "name", Type = CanonicalType.Text },
            ],
            Limits = limits ?? new DualRunLimits(),
        };

    private static IReadOnlyList<DualRunResultColumn> Columns(
        params (string Name, CanonicalType Type)[] columns) =>
        columns.Select(static item => new DualRunResultColumn
        {
            Name = item.Name,
            InferredType = item.Type,
        }).ToArray();

    private static IReadOnlyList<IReadOnlyList<object?>> Rows(params object?[][] rows) => rows;

    private static async Task ExecuteNonQueryAsync(CSharpDbConnection connection, string sql)
    {
        await using CSharpDbCommand command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(Ct);
    }

    private static async Task ExecuteNonQueryAsync(
        System.Data.Common.DbConnection connection,
        string sql)
    {
        await using System.Data.Common.DbCommand command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(Ct);
    }

    private static void DeleteSqliteFiles(string databasePath)
    {
        DeleteIfExists(databasePath);
        DeleteIfExists(databasePath + "-wal");
        DeleteIfExists(databasePath + "-shm");
        DeleteIfExists(databasePath + "-journal");
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private sealed class FakeExecutor : IDualRunQueryExecutor
    {
        private readonly IReadOnlyList<DualRunResultColumn> _columns;
        private readonly IReadOnlyList<IReadOnlyList<object?>> _rows;
        private readonly Exception? _exception;
        private readonly TimeSpan _executeDelay;

        private FakeExecutor(
            string providerId,
            IReadOnlyList<DualRunResultColumn> columns,
            IReadOnlyList<IReadOnlyList<object?>> rows,
            Exception? exception,
            TimeSpan executeDelay)
        {
            ProviderId = providerId;
            _columns = columns;
            _rows = rows;
            _exception = exception;
            _executeDelay = executeDelay;
        }

        public string ProviderId { get; }

        public string SnapshotIdentity => TestSnapshotIdentity;

        public DualRunReadOnlyEnforcement ReadOnlyEnforcement =>
            DualRunReadOnlyEnforcement.StatementValidated;

        public string ReadOnlyValidatorId => "fake-read-only-validator/v1";

        public DualRunExecutionRequest? LastRequest { get; private set; }

        internal static FakeExecutor Success(
            string providerId,
            IReadOnlyList<DualRunResultColumn> columns,
            IReadOnlyList<IReadOnlyList<object?>> rows,
            TimeSpan executeDelay = default) =>
            new(providerId, columns, rows, exception: null, executeDelay);

        internal static FakeExecutor Failure(string providerId, Exception exception) =>
            new(providerId, [], [], exception, executeDelay: default);

        public async ValueTask<IDualRunQueryExecution> ExecuteReadOnlyAsync(
            DualRunExecutionRequest request,
            CancellationToken cancellationToken = default)
        {
            LastRequest = request;
            if (_executeDelay > TimeSpan.Zero)
                await Task.Delay(_executeDelay, cancellationToken);
            if (_exception is not null)
                throw _exception;
            return new FakeExecution(_columns, _rows);
        }
    }

    private sealed class FakeExecution(
        IReadOnlyList<DualRunResultColumn> columns,
        IReadOnlyList<IReadOnlyList<object?>> rows) : IDualRunQueryExecution
    {
        public IReadOnlyList<DualRunResultColumn> Columns { get; } = columns;

        public async IAsyncEnumerable<IReadOnlyList<object?>> ReadRowsAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            foreach (IReadOnlyList<object?> row in rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return row;
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
