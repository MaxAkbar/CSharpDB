using System.Collections.Concurrent;
using CSharpDB.Data;
using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Data.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class AdoQueryDetailRuntimeDiagnosticsTests : IAsyncLifetime
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
        => await CSharpDbConnection.ClearAllPoolsAsync();

    public async ValueTask DisposeAsync()
        => await CSharpDbConnection.ClearAllPoolsAsync();

    [Theory]
    [InlineData(SqlTextCaptureMode.None)]
    [InlineData(SqlTextCaptureMode.Normalized)]
    [InlineData(SqlTextCaptureMode.Raw)]
    public void PredispatchLedgerRetainsConfiguredDetailWithoutListeners(
        SqlTextCaptureMode captureMode)
    {
        const string sql = "SELECT  @missing /* ado-detail */";
        CSharpDbObservabilityOptions options = CreateObservability(captureMode);
        using IDisposable runtimeState =
            AdoCommandObservation.CreateRuntimeDiagnosticsStateForTest(options);
        AdoCommandObservation observation = Assert.IsType<AdoCommandObservation>(
            AdoCommandObservation.TryStartForTest(
                options,
                runtimeState,
                sql,
                OpaqueDiagnosticsId.Create()));
        OpaqueDiagnosticsId operationId = Assert.Single(
            AdoCommandObservation
                .CaptureActiveQueriesForTest(runtimeState)
                .Records!).OperationId;

        QueryDetailSnapshot? active =
            AdoCommandObservation.CaptureQueryDetailForTest(
                runtimeState,
                operationId);
        AssertDetail(captureMode, sql, active);

        observation.FailBeforeDispatch(new InvalidOperationException("binding failed"));
        observation.Dispose();
        QueryDetailSnapshot? recent =
            AdoCommandObservation.CaptureQueryDetailForTest(
                runtimeState,
                operationId);
        AssertDetail(captureMode, sql, recent);
    }

    [Fact]
    public async Task SuccessfulAndFailedCommandsShareExactAdoEngineDetailHistory()
    {
        CSharpDbObservabilityOptions observability =
            CreateObservability(SqlTextCaptureMode.Raw);
        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false",
            new DatabaseOptions { ObservabilityOptions = observability });
        await connection.OpenAsync(Ct);
        object runtimeState = Assert.IsAssignableFrom<object>(
            AdoCommandObservation.GetRuntimeDiagnosticsStateForTest(
                connection.GetSession()));

        const string successfulSql =
            "CREATE TABLE ado_detail_rows (id INTEGER PRIMARY KEY)";
        await using (CSharpDbCommand command =
                     (CSharpDbCommand)connection.CreateCommand())
        {
            command.CommandText = successfulSql;
            Assert.Equal(0, await command.ExecuteNonQueryAsync(Ct));
        }

        const string failedSql = "SELECT @missing AS secret_value";
        await using (CSharpDbCommand command =
                     (CSharpDbCommand)connection.CreateCommand())
        {
            command.CommandText = failedSql;
            await Assert.ThrowsAnyAsync<Exception>(
                async () => await command.ExecuteNonQueryAsync(Ct));
        }

        IReadOnlyList<RecentQuerySnapshot> recent = Assert.IsAssignableFrom<
            IReadOnlyList<RecentQuerySnapshot>>(
            AdoCommandObservation
                .CaptureRecentQueriesForTest(runtimeState, maximumRecords: 32)
                .Records);
        RecentQuerySnapshot successful = Assert.Single(
            recent,
            record => record.Fingerprint ==
                      SqlQueryFingerprintProvider.Instance.CreateFingerprint(successfulSql, Ct));
        RecentQuerySnapshot failed = Assert.Single(
            recent,
            record => record.Fingerprint ==
                      SqlQueryFingerprintProvider.Instance.CreateFingerprint(failedSql, Ct));

        Assert.Equal(
            successfulSql,
            AdoCommandObservation.CaptureQueryDetailForTest(
                runtimeState,
                successful.OperationId)!.CapturedSqlText);
        Assert.Equal(
            failedSql,
            AdoCommandObservation.CaptureQueryDetailForTest(
                runtimeState,
                failed.OperationId)!.CapturedSqlText);
        Assert.Equal(CSharpDbOperationOutcome.Succeeded, successful.Outcome);
        Assert.Equal(CSharpDbOperationOutcome.Failed, failed.Outcome);
    }

    [Fact]
    public void PredispatchEventKeepsFullRawTextWhileRetainedDetailIsCapped()
    {
        string sql = "SELECT '" +
                     new string(
                         'r',
                         QueryDetailSnapshot.MaximumCapturedSqlTextLength + 512) +
                     "'";
        var received = new ConcurrentQueue<object>();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            new EventObserver(received),
            static name => name == CSharpDbLogEvents.QueryFailed.Name);
        CSharpDbObservabilityOptions options = CreateObservability(
            SqlTextCaptureMode.Raw,
            loggingEnabled: true,
            queryEvents: true);
        using IDisposable runtimeState =
            AdoCommandObservation.CreateRuntimeDiagnosticsStateForTest(options);
        AdoCommandObservation observation = Assert.IsType<AdoCommandObservation>(
            AdoCommandObservation.TryStartForTest(
                options,
                runtimeState,
                sql,
                OpaqueDiagnosticsId.Create()));
        OpaqueDiagnosticsId operationId = Assert.Single(
            AdoCommandObservation
                .CaptureActiveQueriesForTest(runtimeState)
                .Records!).OperationId;

        QueryDetailSnapshot active = Assert.IsType<QueryDetailSnapshot>(
            AdoCommandObservation.CaptureQueryDetailForTest(
                runtimeState,
                operationId));
        Assert.Equal(QueryDetailSnapshot.MaximumCapturedSqlTextLength, active.CapturedSqlText!.Length);
        Assert.True(active.Metadata.FieldsTruncated);

        observation.FailBeforeDispatch(new InvalidOperationException("binding failed"));
        observation.Dispose();

        CSharpDbQueryFailedEvent terminal = Assert.Single(
            received.OfType<CSharpDbQueryFailedEvent>());
        Assert.Equal(sql, terminal.CapturedSqlText);
        QueryDetailSnapshot recent = Assert.IsType<QueryDetailSnapshot>(
            AdoCommandObservation.CaptureQueryDetailForTest(
                runtimeState,
                operationId));
        Assert.Equal(active.CapturedSqlText, recent.CapturedSqlText);
        Assert.True(recent.Metadata.FieldsTruncated);
    }

    private static void AssertDetail(
        SqlTextCaptureMode captureMode,
        string sql,
        QueryDetailSnapshot? detail)
    {
        if (captureMode == SqlTextCaptureMode.None)
        {
            Assert.Null(detail);
            return;
        }

        QueryDetailSnapshot captured = Assert.IsType<QueryDetailSnapshot>(detail);
        Assert.Equal(captureMode, captured.CaptureMode);
        Assert.Equal(
            captureMode == SqlTextCaptureMode.Raw
                ? sql
                : SqlQueryFingerprintProvider.Instance
                    .NormalizeAndFingerprint(sql, Ct)
                    .NormalizedText,
            captured.CapturedSqlText);
        Assert.Equal(
            SqlQueryFingerprintProvider.Instance.CreateFingerprint(sql, Ct),
            captured.Fingerprint);
    }

    private static CSharpDbObservabilityOptions CreateObservability(
        SqlTextCaptureMode captureMode,
        bool loggingEnabled = false,
        bool queryEvents = false)
        => new()
        {
            Enabled = true,
            DatabaseAlias = "ado-query-detail-tests",
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = loggingEnabled,
                Queries = queryEvents,
                SlowQueries = false,
                SqlText = captureMode,
            },
            History = new CSharpDbHistoryOptions
            {
                ActiveQueryCapacity = 16,
                RecentQueryCapacity = 16,
                RecentOperationCapacity = 8,
                Retention = TimeSpan.FromMinutes(5),
            },
        };

    private sealed class EventObserver(ConcurrentQueue<object> received)
        : IObserver<KeyValuePair<string, object?>>
    {
        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Value is not null)
                received.Enqueue(value.Value);
        }
    }
}
