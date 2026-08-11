using System.Collections.Concurrent;
using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Observability;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class QueryDetailRuntimeDiagnosticsTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(SqlTextCaptureMode.None)]
    [InlineData(SqlTextCaptureMode.Normalized)]
    [InlineData(SqlTextCaptureMode.Raw)]
    public void DirectEngineCapture_IsListenerIndependentAndAvailableWhileActiveAndRecent(
        SqlTextCaptureMode captureMode)
    {
        const string sql = "SELECT  42 /* retained-detail */";
        using var state = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(captureMode, loggingEnabled: false));
        using var diagnostics = new QueryObservability(
            state,
            startLongRunningSweepTimer: false);

        QueryOperation operation = Assert.IsType<QueryOperation>(diagnostics.Start(sql));
        ActiveQuerySnapshot active = Assert.Single(
            diagnostics.GetActiveCollectionSnapshot(10).Records!);

        QueryDetailSnapshot? activeDetail =
            diagnostics.GetQueryDetailSnapshot(active.OperationId);
        if (captureMode == SqlTextCaptureMode.None)
        {
            Assert.Null(activeDetail);
        }
        else
        {
            QueryDetailSnapshot detail = Assert.IsType<QueryDetailSnapshot>(activeDetail);
            Assert.Equal(captureMode, detail.CaptureMode);
            Assert.Equal(
                captureMode == SqlTextCaptureMode.Raw
                    ? sql
                    : SqlQueryFingerprintProvider.Instance
                        .NormalizeAndFingerprint(sql, Ct)
                        .NormalizedText,
                detail.CapturedSqlText);
            Assert.Equal(
                SqlQueryFingerprintProvider.Instance.CreateFingerprint(sql, Ct),
                detail.Fingerprint);
            Assert.False(detail.Metadata.FieldsTruncated);
        }

        operation.Observe(new QueryResult(rowsAffected: 1));
        RecentQuerySnapshot recent = Assert.Single(
            diagnostics.GetRecentCollectionSnapshot(10).Records!);
        QueryDetailSnapshot? recentDetail =
            diagnostics.GetQueryDetailSnapshot(recent.OperationId);
        Assert.Equal(captureMode != SqlTextCaptureMode.None, recentDetail is not null);
        if (activeDetail is not null)
        {
            Assert.Equal(activeDetail.OperationId, recentDetail!.OperationId);
            Assert.Equal(activeDetail.Fingerprint, recentDetail.Fingerprint);
            Assert.Equal(activeDetail.CaptureMode, recentDetail.CaptureMode);
            Assert.Equal(activeDetail.CapturedSqlText, recentDetail.CapturedSqlText);
            Assert.Equal(
                activeDetail.Metadata.FieldsTruncated,
                recentDetail.Metadata.FieldsTruncated);
        }
    }

    [Fact]
    public void RetainedDetail_IsCappedButTerminalEventKeepsItsExistingFullPayload()
    {
        const string prefix = "SELECT '";
        string sql = prefix +
                     new string(
                         's',
                         QueryDetailSnapshot.MaximumCapturedSqlTextLength - prefix.Length - 1) +
                     "\U0001F600-tail'";
        var received = new ConcurrentQueue<object>();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            new EventObserver(received),
            static name => name == CSharpDbLogEvents.QueryCompleted.Name);
        using var state = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(
                SqlTextCaptureMode.Raw,
                loggingEnabled: true,
                queryEvents: true));
        using var diagnostics = new QueryObservability(
            state,
            startLongRunningSweepTimer: false);

        QueryOperation operation = Assert.IsType<QueryOperation>(diagnostics.Start(sql));
        OpaqueDiagnosticsId operationId = Assert.Single(
            diagnostics.GetActiveCollectionSnapshot(10).Records!).OperationId;
        QueryDetailSnapshot activeDetail = Assert.IsType<QueryDetailSnapshot>(
            diagnostics.GetQueryDetailSnapshot(operationId));
        Assert.Equal(
            QueryDetailSnapshot.MaximumCapturedSqlTextLength - 1,
            activeDetail.CapturedSqlText!.Length);
        Assert.Equal(
            sql[..(QueryDetailSnapshot.MaximumCapturedSqlTextLength - 1)],
            activeDetail.CapturedSqlText);
        Assert.False(char.IsHighSurrogate(activeDetail.CapturedSqlText[^1]));
        Assert.True(activeDetail.Metadata.FieldsTruncated);
        Assert.Equal(
            SqlQueryFingerprintProvider.Instance.CreateFingerprint(sql, Ct),
            activeDetail.Fingerprint);

        operation.Observe(new QueryResult(rowsAffected: 1));

        CSharpDbQueryCompletedEvent terminal = Assert.Single(
            received.OfType<CSharpDbQueryCompletedEvent>());
        Assert.Equal(sql, terminal.CapturedSqlText);
        Assert.Equal(sql.Length, terminal.CapturedSqlText!.Length);
        QueryDetailSnapshot recentDetail = Assert.IsType<QueryDetailSnapshot>(
            diagnostics.GetQueryDetailSnapshot(operationId));
        Assert.Equal(activeDetail.CapturedSqlText, recentDetail.CapturedSqlText);
        Assert.True(recentDetail.Metadata.FieldsTruncated);

        string json = JsonSerializer.Serialize(
            recentDetail,
            CSharpDbObservabilityJsonContext.Default.QueryDetailSnapshot);
        QueryDetailSnapshot roundTripped = Assert.IsType<QueryDetailSnapshot>(
            JsonSerializer.Deserialize(
                json,
                CSharpDbObservabilityJsonContext.Default.QueryDetailSnapshot));
        Assert.Equal(recentDetail, roundTripped);
    }

    [Fact]
    public void OversizedSourceThatCannotBeNormalizedIsTruthfullyUnavailable()
    {
        string sql = "SELECT " + string.Join(
            ", ",
            Enumerable.Range(0, 8_000)
                .Select(static index => $"column_{index:D5}"));
        using var state = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(SqlTextCaptureMode.Normalized));
        using var diagnostics = new QueryObservability(
            state,
            startLongRunningSweepTimer: false);

        QueryOperation operation = Assert.IsType<QueryOperation>(diagnostics.Start(sql));
        OpaqueDiagnosticsId operationId = Assert.Single(
            diagnostics.GetActiveCollectionSnapshot(10).Records!).OperationId;
        Assert.Null(diagnostics.GetQueryDetailSnapshot(operationId));
        operation.Observe(new QueryResult(rowsAffected: 1));
        Assert.Null(diagnostics.GetQueryDetailSnapshot(operationId));
    }

    [Fact]
    public void DetailSharesRecentEvictionRetentionAndRuntimeFamilyDisposal()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 11, 9, 0, 0, TimeSpan.Zero));
        using var state = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(
                SqlTextCaptureMode.Raw,
                recentCapacity: 1,
                retention: TimeSpan.FromSeconds(1)),
            clock);
        QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
            state,
            startSweepTimer: false);

        OpaqueDiagnosticsId firstId = CompleteRaw(registry, clock, "SELECT 'first-secret'");
        Assert.NotNull(registry.GetQueryDetailSnapshot(firstId));

        clock.Advance(TimeSpan.FromMilliseconds(100));
        OpaqueDiagnosticsId secondId = CompleteRaw(registry, clock, "SELECT 'second-secret'");
        Assert.Null(registry.GetQueryDetailSnapshot(firstId));
        Assert.NotNull(registry.GetQueryDetailSnapshot(secondId));

        clock.Advance(TimeSpan.FromSeconds(2));
        Assert.Null(registry.GetQueryDetailSnapshot(secondId));

        OpaqueDiagnosticsId thirdId = CompleteRaw(registry, clock, "SELECT 'third-secret'");
        Assert.NotNull(registry.GetQueryDetailSnapshot(thirdId));
        state.Dispose();
        Assert.Null(registry.GetQueryDetailSnapshot(thirdId));
    }

    [Fact]
    public void RebindMovesDetailToTheExactTargetFamilyAndHonorsItsCapturePolicy()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 11, 10, 0, 0, TimeSpan.Zero));
        using var sourceState = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(SqlTextCaptureMode.Raw, alias: "source-family"),
            clock);
        using var targetState = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(SqlTextCaptureMode.Raw, alias: "target-family"),
            clock);
        using var noCaptureState = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(SqlTextCaptureMode.None, alias: "private-family"),
            clock);
        QueryRuntimeDiagnostics source = QueryRuntimeDiagnostics.GetOrCreate(
            sourceState,
            startSweepTimer: false);
        QueryRuntimeDiagnostics target = QueryRuntimeDiagnostics.GetOrCreate(
            targetState,
            startSweepTimer: false);
        QueryRuntimeDiagnostics noCapture = QueryRuntimeDiagnostics.GetOrCreate(
            noCaptureState,
            startSweepTimer: false);

        CSharpDbOperationContext context = CreateContext(clock, "source-family", "SELECT 'move-me'");
        QueryRuntimeDiagnostics.QueryRuntimeOperation sourceLease =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                source.TryStart(
                    context,
                    QueryExecutionPhase.Queued,
                    SqlTextCaptureMode.Raw,
                    "SELECT 'move-me'"));
        Assert.NotNull(source.GetQueryDetailSnapshot(context.OperationId));

        QueryRuntimeDiagnostics.QueryRuntimeOperation targetLease =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                sourceLease.RebindTo(target, QueryExecutionPhase.Executing));
        Assert.Null(source.GetQueryDetailSnapshot(context.OperationId));
        QueryDetailSnapshot targetDetail = Assert.IsType<QueryDetailSnapshot>(
            target.GetQueryDetailSnapshot(context.OperationId));
        Assert.Equal(targetState.ServerInstanceId, targetDetail.Metadata.ServerInstanceId);
        Assert.Equal("target-family", targetDetail.Metadata.DatabaseAlias);

        QueryRuntimeDiagnostics.QueryRuntimeOperation privateLease =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                targetLease.RebindTo(noCapture, QueryExecutionPhase.Streaming));
        Assert.Null(target.GetQueryDetailSnapshot(context.OperationId));
        Assert.Null(noCapture.GetQueryDetailSnapshot(context.OperationId));
        privateLease.Complete(
            CSharpDbOperationOutcome.Succeeded,
            clock.GetUtcNow(),
            TimeSpan.Zero,
            timeToFirstResult: null,
            rowsProduced: 0,
            rowsAffected: 1,
            error: null,
            isSlow: false);
        Assert.Null(noCapture.GetQueryDetailSnapshot(context.OperationId));
    }

    [Fact]
    public async Task PreparsedStatementHasNoSourceTextAndReportsUnavailableDetail()
    {
        var options = new DatabaseOptions
        {
            ObservabilityOptions = CreateOptions(SqlTextCaptureMode.Raw),
        };
        await using Database database = await Database.OpenInMemoryAsync(options, Ct);
        Statement statement = Parser.Parse("SELECT 1");

        await using (QueryResult result = await database.ExecuteAsync(statement, Ct))
        {
            _ = await result.ToListAsync(Ct);
        }

        RecentQuerySnapshot recent = Assert.Single(
            database.GetRecentQueryDiagnosticsCollection(10)!.Records!);
        Assert.Null(database.GetQueryDetailDiagnosticsSnapshot(recent.OperationId));
    }

    [Fact]
    public void OrdinarySnapshotsNeverSerializeCapturedTextButExplicitDetailDoes()
    {
        const string secret = "SELECT 'do-not-leak-through-ordinary-diagnostics'";
        using var state = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(SqlTextCaptureMode.Raw, loggingEnabled: false));
        using var diagnostics = new QueryObservability(
            state,
            startLongRunningSweepTimer: false);
        QueryOperation operation = Assert.IsType<QueryOperation>(diagnostics.Start(secret));
        DiagnosticsCollectionSnapshot<ActiveQuerySnapshot> active =
            diagnostics.GetActiveCollectionSnapshot(10);
        OpaqueDiagnosticsId operationId = Assert.Single(active.Records!).OperationId;
        QueryDetailSnapshot detail = Assert.IsType<QueryDetailSnapshot>(
            diagnostics.GetQueryDetailSnapshot(operationId));

        string activeJson = JsonSerializer.Serialize(
            active,
            typeof(DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>),
            CSharpDbObservabilityJsonContext.Default);
        string summaryJson = JsonSerializer.Serialize(
            diagnostics.GetSummary(),
            typeof(QueryDiagnosticsSummary),
            CSharpDbObservabilityJsonContext.Default);
        Assert.DoesNotContain(secret, activeJson, StringComparison.Ordinal);
        Assert.DoesNotContain("capturedSqlText", activeJson, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(secret, summaryJson, StringComparison.Ordinal);

        operation.Observe(new QueryResult(rowsAffected: 1));
        string recentJson = JsonSerializer.Serialize(
            diagnostics.GetRecentCollectionSnapshot(10),
            typeof(DiagnosticsCollectionSnapshot<RecentQuerySnapshot>),
            CSharpDbObservabilityJsonContext.Default);
        string detailJson = JsonSerializer.Serialize(
            detail,
            typeof(QueryDetailSnapshot),
            CSharpDbObservabilityJsonContext.Default);
        Assert.DoesNotContain(secret, recentJson, StringComparison.Ordinal);
        Assert.DoesNotContain("capturedSqlText", recentJson, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("capturedSqlText", detailJson, StringComparison.OrdinalIgnoreCase);
        QueryDetailSnapshot roundTripped = Assert.IsType<QueryDetailSnapshot>(
            JsonSerializer.Deserialize(
                detailJson,
                CSharpDbObservabilityJsonContext.Default.QueryDetailSnapshot));
        Assert.Equal(secret, roundTripped.CapturedSqlText);
    }

    [Fact]
    public async Task LookupCompletionAndDisposalRaceIsNoThrowAndReleasesDetail()
    {
        var clock = new SwitchableThrowingTimeProvider(
            new DateTimeOffset(2026, 8, 11, 11, 0, 0, TimeSpan.Zero));
        using var state = new CSharpDbRuntimeDiagnosticsState(
            CreateOptions(SqlTextCaptureMode.Raw),
            clock);
        QueryRuntimeDiagnostics registry = QueryRuntimeDiagnostics.GetOrCreate(
            state,
            startSweepTimer: false);
        CSharpDbOperationContext context = CreateContext(
            clock,
            "query-detail-tests",
            "SELECT 'race-secret'");
        QueryRuntimeDiagnostics.QueryRuntimeOperation lease =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                registry.TryStart(
                    context,
                    QueryExecutionPhase.Executing,
                    SqlTextCaptureMode.Raw,
                    "SELECT 'race-secret'"));
        clock.Throw = true;
        Assert.NotNull(registry.GetQueryDetailSnapshot(context.OperationId));

        var failures = new ConcurrentQueue<Exception>();
        Task[] readers = Enumerable.Range(0, 8)
            .Select(_ => Task.Run(() =>
            {
                for (int index = 0; index < 500; index++)
                {
                    try
                    {
                        QueryDetailSnapshot? snapshot =
                            registry.GetQueryDetailSnapshot(context.OperationId);
                        if (snapshot is not null)
                        {
                            Assert.Equal(SqlTextCaptureMode.Raw, snapshot.CaptureMode);
                            Assert.Equal("SELECT 'race-secret'", snapshot.CapturedSqlText);
                        }
                    }
                    catch (Exception exception)
                    {
                        failures.Enqueue(exception);
                    }
                }
            }, Ct))
            .ToArray();

        lease.Complete(
            CSharpDbOperationOutcome.Succeeded,
            context.StartedAtUtc,
            TimeSpan.Zero,
            timeToFirstResult: null,
            rowsProduced: 0,
            rowsAffected: 1,
            error: null,
            isSlow: false);
        state.Dispose();
        await Task.WhenAll(readers);

        Assert.Empty(failures);
        Assert.Null(registry.GetQueryDetailSnapshot(context.OperationId));
    }

    private static OpaqueDiagnosticsId CompleteRaw(
        QueryRuntimeDiagnostics registry,
        TimeProvider clock,
        string sql)
    {
        CSharpDbOperationContext context = CreateContext(
            clock,
            "query-detail-tests",
            sql);
        QueryRuntimeDiagnostics.QueryRuntimeOperation lease =
            Assert.IsType<QueryRuntimeDiagnostics.QueryRuntimeOperation>(
                registry.TryStart(
                    context,
                    QueryExecutionPhase.Executing,
                    SqlTextCaptureMode.Raw,
                    sql));
        lease.Complete(
            CSharpDbOperationOutcome.Succeeded,
            context.GetUtcNow(),
            context.GetElapsedTime(),
            timeToFirstResult: null,
            rowsProduced: 0,
            rowsAffected: 1,
            error: null,
            isSlow: false);
        return context.OperationId;
    }

    private static CSharpDbOperationContext CreateContext(
        TimeProvider clock,
        string alias,
        string sql)
        => CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Embedded,
            alias,
            queryFingerprint: SqlQueryFingerprintProvider.Instance.CreateFingerprint(sql, Ct),
            timeProvider: clock);

    private static CSharpDbObservabilityOptions CreateOptions(
        SqlTextCaptureMode captureMode,
        bool loggingEnabled = false,
        bool queryEvents = false,
        int recentCapacity = 8,
        TimeSpan? retention = null,
        string alias = "query-detail-tests")
        => new()
        {
            Enabled = true,
            DatabaseAlias = alias,
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = loggingEnabled,
                Queries = queryEvents,
                SlowQueries = false,
                SqlText = captureMode,
            },
            History = new CSharpDbHistoryOptions
            {
                ActiveQueryCapacity = 8,
                RecentQueryCapacity = recentCapacity,
                RecentOperationCapacity = 8,
                Retention = retention ?? TimeSpan.FromMinutes(5),
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

    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private long _timestamp;
        private long _utcTicks = utcNow.UtcTicks;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => Interlocked.Read(ref _timestamp);
        public override DateTimeOffset GetUtcNow() =>
            new(Interlocked.Read(ref _utcTicks), TimeSpan.Zero);

        internal void Advance(TimeSpan duration)
        {
            Interlocked.Add(ref _timestamp, duration.Ticks);
            Interlocked.Add(ref _utcTicks, duration.Ticks);
        }
    }

    private sealed class SwitchableThrowingTimeProvider(DateTimeOffset utcNow)
        : TimeProvider
    {
        private int _throw;

        internal bool Throw
        {
            set => Volatile.Write(ref _throw, value ? 1 : 0);
        }

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp()
            => Volatile.Read(ref _throw) == 0
                ? 0
                : throw new InvalidOperationException("timestamp unavailable");

        public override DateTimeOffset GetUtcNow()
            => Volatile.Read(ref _throw) == 0
                ? utcNow
                : throw new InvalidOperationException("clock unavailable");
    }
}
