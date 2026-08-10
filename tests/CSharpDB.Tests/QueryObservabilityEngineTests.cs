using System.Diagnostics;
using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class QueryObservabilityEngineTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task SharedDmlResults_EmitOneCompletionPerOperationWithoutAttachingState()
    {
        using var events = new QueryEventRecorder();
        await using Database database = await OpenObservedDatabaseAsync();
        await ExecuteNonQueryAsync(database, "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)");
        events.Clear();

        QueryResult oneA = await database.ExecuteAsync("INSERT INTO items VALUES (1, 10)", Ct);
        QueryResult oneB = await database.ExecuteAsync("INSERT INTO items VALUES (2, 20)", Ct);

        Assert.Same(oneA, oneB);
        CSharpDbQueryCompletedEvent[] inserts = events.Events<CSharpDbQueryCompletedEvent>();
        Assert.Equal(2, inserts.Length);
        Assert.All(inserts, item => Assert.Equal(1, item.RowsAffected));

        events.Clear();
        QueryResult zeroA = await database.ExecuteAsync("UPDATE items SET value = 30 WHERE id = 99", Ct);
        QueryResult zeroB = await database.ExecuteAsync("DELETE FROM items WHERE id = 100", Ct);

        CSharpDbQueryCompletedEvent[] misses = events.Events<CSharpDbQueryCompletedEvent>();
        Assert.Equal(2, misses.Length);
        Assert.All(misses, item => Assert.Equal(0, item.RowsAffected));

        await oneA.DisposeAsync();
        await oneB.DisposeAsync();
        await zeroA.DisposeAsync();
        await zeroB.DisposeAsync();
        Assert.Equal(2, events.Events<CSharpDbQueryCompletedEvent>().Length);
    }

    [Fact]
    public async Task TriggerBody_RemainsInternalToTheCausingStatement()
    {
        OpaqueDiagnosticsId? triggerOperationId = null;
        DatabaseOptions options = CreateDatabaseOptions().ConfigureFunctions(functions =>
        {
            functions.AddScalar(
                "CaptureTriggerQueryContext",
                1,
                new DbScalarFunctionOptions(DbType.Text, IsDeterministic: false),
                (_, arguments) =>
                {
                    triggerOperationId = CSharpDbOperationScope.Current?.OperationId;
                    return arguments[0];
                });
        });
        using var events = new QueryEventRecorder();
        await using Database database = await OpenObservedDatabaseAsync(options);
        await ExecuteNonQueryAsync(database, "CREATE TABLE trigger_items (id INTEGER PRIMARY KEY, value TEXT)");
        await ExecuteNonQueryAsync(database, "CREATE TABLE trigger_audit (id INTEGER PRIMARY KEY, value TEXT)");
        await ExecuteNonQueryAsync(
            database,
            "CREATE TRIGGER trg_phase1 AFTER INSERT ON trigger_items " +
            "BEGIN INSERT INTO trigger_audit VALUES " +
            "(NEW.id, CaptureTriggerQueryContext(NEW.value)); END");
        events.Clear();

        await ExecuteNonQueryAsync(database, "INSERT INTO trigger_items VALUES (1, 'trigger-canary')");

        CSharpDbQueryCompletedEvent completed = Assert.Single(
            events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Equal(CSharpDbOperationRole.Root, completed.Context.Role);
        Assert.Equal(1, completed.RowsAffected);
        Assert.Equal(completed.Context.OperationId, triggerOperationId);
        Assert.Empty(events.Events<CSharpDbLifecycleCompletedEvent>());

        events.Clear();
        await ExecuteQueryToExhaustionAsync(database, "SELECT * FROM trigger_audit WHERE id = 1");
        CSharpDbQueryCompletedEvent verification = Assert.Single(
            events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Equal(1, verification.RowsProduced);
    }

    [Fact]
    public async Task FastLookupAndScalar_CompleteOnConsumptionExactlyOnce()
    {
        using var events = new QueryEventRecorder();
        await using Database database = await CreatePopulatedDatabaseAsync();
        events.Clear();

        await using (QueryResult lookup = await database.ExecuteAsync(
                         "SELECT * FROM items WHERE id = 1",
                         Ct))
        {
            Assert.Empty(events.Events<CSharpDbQueryCompletedEvent>());
            Assert.Single(await lookup.ToListAsync(Ct));
        }

        CSharpDbQueryCompletedEvent lookupEvent = Assert.Single(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Equal(1, lookupEvent.RowsProduced);
        Assert.NotNull(lookupEvent.TimeToFirstResult);

        events.Clear();
        await using (QueryResult scalar = await database.ExecuteAsync("SELECT COUNT(*) FROM items", Ct))
        {
            Assert.Empty(events.Events<CSharpDbQueryCompletedEvent>());
            List<DbValue[]> rows = await scalar.ToListAsync(Ct);
            Assert.Equal(3, rows[0][0].AsInteger);
        }

        CSharpDbQueryCompletedEvent scalarEvent = Assert.Single(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Equal(1, scalarEvent.RowsProduced);
    }

    [Fact]
    public async Task StreamedExhaustion_EmitsOnlyAfterFinalMoveNext()
    {
        using var events = new QueryEventRecorder();
        await using Database database = await CreatePopulatedDatabaseAsync();
        events.Clear();

        await using QueryResult result = await database.ExecuteAsync(
            "SELECT id, value FROM items ORDER BY id",
            Ct);
        Assert.True(await result.MoveNextAsync(Ct));
        Assert.True(await result.MoveNextAsync(Ct));
        Assert.True(await result.MoveNextAsync(Ct));
        Assert.Empty(events.Events<CSharpDbQueryCompletedEvent>());

        Assert.False(await result.MoveNextAsync(Ct));
        Assert.False(await result.MoveNextAsync(Ct));

        CSharpDbQueryCompletedEvent completed = Assert.Single(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Equal(3, completed.RowsProduced);
    }

    [Fact]
    public async Task EarlyAndNeverOpenedDisposal_EmitOneTerminalOutcome()
    {
        using var events = new QueryEventRecorder();
        await using Database database = await CreatePopulatedDatabaseAsync();
        events.Clear();

        QueryResult early = await database.ExecuteAsync("SELECT * FROM items ORDER BY id", Ct);
        Assert.True(await early.MoveNextAsync(Ct));
        Assert.Empty(events.Events<CSharpDbQueryCompletedEvent>());
        await early.DisposeAsync();
        await early.DisposeAsync();

        CSharpDbQueryCompletedEvent earlyEvent = Assert.Single(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Equal(1, earlyEvent.RowsProduced);

        events.Clear();
        QueryResult unopened = await database.ExecuteAsync("SELECT * FROM items ORDER BY id", Ct);
        await unopened.DisposeAsync();
        await unopened.DisposeAsync();

        CSharpDbQueryCompletedEvent unopenedEvent = Assert.Single(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Equal(0, unopenedEvent.RowsProduced);
    }

    [Fact]
    public async Task ParseAndPlannerFailures_EmitOneSafeFailureEach()
    {
        const string secret = "engine-parse-secret-91f4";
        using var events = new QueryEventRecorder();
        await using Database database = await OpenObservedDatabaseAsync();

        await Assert.ThrowsAnyAsync<Exception>(
            () => database.ExecuteAsync($"NOT_SQL '{secret}'", Ct).AsTask());

        CSharpDbQueryFailedEvent parseFailure = Assert.Single(events.Events<CSharpDbQueryFailedEvent>());
        Assert.Null(parseFailure.CapturedSqlText);
        string serialized = JsonSerializer.Serialize(
            parseFailure,
            CSharpDbObservabilityJsonContext.Default.CSharpDbQueryFailedEvent);
        Assert.DoesNotContain(secret, serialized, StringComparison.Ordinal);

        events.Clear();
        await Assert.ThrowsAnyAsync<Exception>(
            () => database.ExecuteAsync("SELECT * FROM missing_engine_table", Ct).AsTask());

        CSharpDbQueryFailedEvent plannerFailure = Assert.Single(events.Events<CSharpDbQueryFailedEvent>());
        Assert.NotNull(plannerFailure.Error);
        Assert.Null(plannerFailure.CapturedSqlText);
    }

    [Fact]
    public async Task StreamFailureAndCancellation_EmitOneMatchingTerminalOutcome()
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        int cancellationInvocationCount = 0;
        DatabaseOptions options = CreateDatabaseOptions().ConfigureFunctions(functions =>
        {
            functions.AddScalar(
                "EnginePhase1Fail",
                1,
                new DbScalarFunctionOptions(DbType.Integer, IsDeterministic: false),
                static (_, _) => throw new InvalidOperationException("engine-stream-secret"));
            functions.AddScalar(
                "EnginePhase1Cancel",
                1,
                new DbScalarFunctionOptions(DbType.Integer, IsDeterministic: false),
                (_, arguments) =>
                {
                    if (Interlocked.Increment(ref cancellationInvocationCount) == 1)
                        cancellation.Cancel();

                    return arguments[0];
                });
        });
        using var events = new QueryEventRecorder();
        await using Database database = await CreatePopulatedDatabaseAsync(options);
        events.Clear();

        await using (QueryResult failing = await database.ExecuteAsync(
                         "SELECT EnginePhase1Fail(id) FROM items ORDER BY id",
                         Ct))
        {
            Assert.Empty(events.Events<CSharpDbQueryFailedEvent>());
            await Assert.ThrowsAsync<CSharpDbException>(() => failing.ToListAsync(Ct).AsTask());
        }

        CSharpDbQueryFailedEvent failure = Assert.Single(events.Events<CSharpDbQueryFailedEvent>());
        Assert.DoesNotContain("secret", failure.Error!.PublicDetail, StringComparison.OrdinalIgnoreCase);

        events.Clear();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () =>
            {
                await using QueryResult canceled = await database.ExecuteAsync(
                    "EXPLAIN ANALYZE SELECT EnginePhase1Cancel(id) FROM items ORDER BY id",
                    cancellation.Token);
                _ = await canceled.ToListAsync(cancellation.Token);
            });

        Assert.True(Volatile.Read(ref cancellationInvocationCount) >= 1);
        CSharpDbQueryCanceledEvent canceledEvent = Assert.Single(events.Events<CSharpDbQueryCanceledEvent>());
        Assert.Equal(CSharpDbOperationOutcome.Canceled, canceledEvent.Outcome);
    }

    [Fact]
    public async Task SlowQuery_IsClassifiedOnlyAtTerminalUsingOperationClock()
    {
        var clock = new ManualTimeProvider(
            new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero));
        CSharpDbOperationContext context = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Embedded,
            "engine-tests",
            timeProvider: clock);
        using var events = new QueryEventRecorder();
        await using Database database = await OpenObservedDatabaseAsync(
            CreateDatabaseOptions(slowQueries: true, slowThreshold: TimeSpan.FromSeconds(1)));
        using IDisposable operationScope = CSharpDbOperationScope.Enter(context);

        QueryResult result = await database.ExecuteAsync("SELECT 1", Ct);
        clock.Advance(TimeSpan.FromSeconds(2));
        Assert.Empty(events.Events<CSharpDbSlowQueryEvent>());

        await result.DisposeAsync();

        CSharpDbSlowQueryEvent slow = Assert.Single(events.Events<CSharpDbSlowQueryEvent>());
        CSharpDbQueryCompletedEvent completed = Assert.Single(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Equal(TimeSpan.FromSeconds(2), slow.TotalDuration);
        Assert.Equal(clock.GetUtcNow(), slow.CompletedAtUtc);
        Assert.Equal(slow.CompletedAtUtc, completed.CompletedAtUtc);
        Assert.Equal(context.OperationId, slow.Context.OperationId);
    }

    [Fact]
    public async Task DefaultAndNormalizedCapture_RedactLiteralsAndPaths()
    {
        const string secret = "engine-redaction-secret-1d8f";
        const string path = @"C:\private\engine.db";
        using var events = new QueryEventRecorder();
        await using (Database database = await OpenObservedDatabaseAsync())
        {
            await using QueryResult result = await database.ExecuteAsync(
                $"SELECT '{secret} {path}' AS payload",
                Ct);
            _ = await result.ToListAsync(Ct);
        }

        CSharpDbQueryCompletedEvent safeDefault = Assert.Single(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Equal(SqlTextCaptureMode.None, safeDefault.SqlTextCaptureMode);
        Assert.Null(safeDefault.CapturedSqlText);
        Assert.NotNull(safeDefault.Context.QueryFingerprint);

        events.Clear();
        await using (Database database = await OpenObservedDatabaseAsync(
                         CreateDatabaseOptions(SqlTextCaptureMode.Normalized)))
        {
            await using QueryResult result = await database.ExecuteAsync(
                $"SELECT '{secret} {path}' AS payload",
                Ct);
            _ = await result.ToListAsync(Ct);
        }

        CSharpDbQueryCompletedEvent normalized = Assert.Single(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Equal(SqlTextCaptureMode.Normalized, normalized.SqlTextCaptureMode);
        Assert.NotNull(normalized.CapturedSqlText);
        Assert.DoesNotContain(secret, normalized.CapturedSqlText, StringComparison.Ordinal);
        Assert.DoesNotContain(path, normalized.CapturedSqlText, StringComparison.Ordinal);
        Assert.NotNull(normalized.Context.QueryFingerprint);
    }

    [Fact]
    public async Task PreParsedWriteTransactionAndReaderSession_AreObservedExactlyOnce()
    {
        using var events = new QueryEventRecorder();
        await using Database database = await CreatePopulatedDatabaseAsync();
        events.Clear();

        Statement prepared = Parser.Parse("SELECT id FROM items ORDER BY id");
        await using (QueryResult result = await database.ExecuteAsync(prepared, Ct))
            Assert.Equal(3, (await result.ToListAsync(Ct)).Count);

        CSharpDbQueryCompletedEvent preparedEvent = Assert.Single(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Equal(3, preparedEvent.RowsProduced);

        events.Clear();
        await using (WriteTransaction transaction = await database.BeginWriteTransactionAsync(Ct))
        {
            await using QueryResult inserted = await transaction.ExecuteAsync(
                "INSERT INTO items VALUES (4, 40)",
                Ct);
            await using QueryResult selected = await transaction.ExecuteAsync(
                "SELECT * FROM items WHERE id = 4",
                Ct);
            Assert.Single(await selected.ToListAsync(Ct));
            await transaction.CommitAsync(Ct);
        }

        CSharpDbQueryCompletedEvent[] transactionEvents = events.Events<CSharpDbQueryCompletedEvent>();
        Assert.Equal(2, transactionEvents.Length);
        Assert.Contains(transactionEvents, item => item.RowsAffected == 1);
        Assert.Contains(transactionEvents, item => item.RowsProduced == 1);

        events.Clear();
        using (Database.ReaderSession reader = database.CreateReaderSession())
        {
            await using (QueryResult count = await reader.ExecuteReadAsync("SELECT COUNT(*) FROM items", Ct))
                Assert.Equal(4, (await count.ToListAsync(Ct))[0][0].AsInteger);

            Statement readerPrepared = Parser.Parse("SELECT * FROM items WHERE id = 1");
            await using (QueryResult lookup = await reader.ExecuteReadAsync(readerPrepared, Ct))
                Assert.Single(await lookup.ToListAsync(Ct));
        }

        CSharpDbQueryCompletedEvent[] readerEvents = events.Events<CSharpDbQueryCompletedEvent>();
        Assert.Equal(2, readerEvents.Length);
        Assert.All(readerEvents, item => Assert.Equal(1, item.RowsProduced));
    }

    [Fact]
    public async Task SuppressionScopes_NestRestoreAndSkipEngineQueryEvents()
    {
        using var events = new QueryEventRecorder();
        await using Database database = await OpenObservedDatabaseAsync();

        using (CSharpDbOperationScope.SuppressDiagnostics())
        {
            Assert.True(CSharpDbOperationScope.IsDiagnosticsSuppressed);
            await ExecuteQueryToExhaustionAsync(database, "SELECT 1");

            using (CSharpDbOperationScope.SuppressDiagnostics())
            {
                Assert.True(CSharpDbOperationScope.IsDiagnosticsSuppressed);
                await ExecuteQueryToExhaustionAsync(database, "SELECT 2");
            }

            Assert.True(CSharpDbOperationScope.IsDiagnosticsSuppressed);
            await ExecuteQueryToExhaustionAsync(database, "SELECT 3");
        }

        Assert.False(CSharpDbOperationScope.IsDiagnosticsSuppressed);
        Assert.Empty(events.Events<CSharpDbQueryCompletedEvent>());

        await ExecuteQueryToExhaustionAsync(database, "SELECT 4");
        Assert.Single(events.Events<CSharpDbQueryCompletedEvent>());
    }

    [Fact]
    public async Task ThrowingSubscriber_DoesNotChangeResultsOrDurability()
    {
        var observer = new ThrowingEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));
        await using Database database = await OpenObservedDatabaseAsync();

        await ExecuteNonQueryAsync(database, "CREATE TABLE durable_items (id INTEGER PRIMARY KEY)");
        await ExecuteNonQueryAsync(database, "INSERT INTO durable_items VALUES (1)");
        await using QueryResult result = await database.ExecuteAsync(
            "SELECT * FROM durable_items WHERE id = 1",
            Ct);

        Assert.Single(await result.ToListAsync(Ct));
        Assert.True(observer.AttemptCount >= 3);
    }

    private static DatabaseOptions CreateDatabaseOptions(
        SqlTextCaptureMode captureMode = SqlTextCaptureMode.None,
        bool slowQueries = false,
        TimeSpan? slowThreshold = null)
        => new()
        {
            ObservabilityOptions = new CSharpDbObservabilityOptions
            {
                Enabled = true,
                DatabaseAlias = "engine-tests",
                Logging = new CSharpDbLoggingOptions
                {
                    Enabled = true,
                    Queries = true,
                    SlowQueries = slowQueries,
                    SlowQueryThreshold = slowThreshold ?? TimeSpan.FromSeconds(30),
                    SqlText = captureMode,
                },
            },
        };

    private static async ValueTask<Database> OpenObservedDatabaseAsync(DatabaseOptions? options = null)
        => await Database.OpenInMemoryAsync(options ?? CreateDatabaseOptions(), Ct);

    private static async ValueTask<Database> CreatePopulatedDatabaseAsync(DatabaseOptions? options = null)
    {
        Database database = await OpenObservedDatabaseAsync(options);
        try
        {
            await ExecuteNonQueryAsync(database, "CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)");
            await ExecuteNonQueryAsync(database, "INSERT INTO items VALUES (1, 10), (2, 20), (3, 30)");
            return database;
        }
        catch
        {
            await database.DisposeAsync();
            throw;
        }
    }

    private static async ValueTask ExecuteNonQueryAsync(Database database, string sql)
    {
        await using QueryResult result = await database.ExecuteAsync(sql, Ct);
    }

    private static async ValueTask ExecuteQueryToExhaustionAsync(Database database, string sql)
    {
        await using QueryResult result = await database.ExecuteAsync(sql, Ct);
        _ = await result.ToListAsync(Ct);
    }

    private sealed class QueryEventRecorder : IObserver<KeyValuePair<string, object?>>, IDisposable
    {
        private readonly object _gate = new();
        private readonly List<object> _events = [];
        private readonly IDisposable _subscription;

        internal QueryEventRecorder()
        {
            _subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
                this,
                static name =>
                    name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal) ||
                    name == CSharpDbLogEvents.TransactionCompleted.Name);
        }

        internal T[] Events<T>()
        {
            lock (_gate)
                return _events.OfType<T>().ToArray();
        }

        internal void Clear()
        {
            lock (_gate)
                _events.Clear();
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Value is null)
                return;

            lock (_gate)
                _events.Add(value.Value);
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void Dispose() => _subscription.Dispose();
    }

    private sealed class ThrowingEventObserver : IObserver<KeyValuePair<string, object?>>
    {
        private int _attemptCount;

        internal int AttemptCount => Volatile.Read(ref _attemptCount);

        public void OnNext(KeyValuePair<string, object?> value)
        {
            Interlocked.Increment(ref _attemptCount);
            throw new InvalidOperationException("throwing-subscriber-secret");
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }
    }

    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset _utcNow = utcNow;
        private long _timestamp;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public override long GetTimestamp() => Interlocked.Read(ref _timestamp);

        internal void Advance(TimeSpan duration)
        {
            _utcNow = _utcNow.Add(duration);
            Interlocked.Add(ref _timestamp, duration.Ticks);
        }
    }
}

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ObservabilityDiagnosticsCollection
{
    public const string Name = "ObservabilityDiagnostics";
}
