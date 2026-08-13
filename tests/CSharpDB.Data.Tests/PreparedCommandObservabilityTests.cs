using System.Data.Common;
using System.Text.Json;
using CSharpDB.Data;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Data.Tests;

[Collection(ObservabilityDiagnosticsCollection.Name)]
public sealed class PreparedCommandObservabilityTests : IAsyncLifetime
{
    private readonly string _databasePath = Path.Combine(
        Path.GetTempPath(),
        $"csharpdb_prepared_observability_{Guid.NewGuid():N}.db");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
        DeleteIfExists(_databasePath);
        DeleteIfExists(_databasePath + ".wal");
    }

    [Fact]
    public async Task DirectStringExecution_UsesOriginalSourceForNormalizedCapture()
    {
        const string secret = "direct-bound-secret-7b22";
        const string sourceSql = "SELECT @payload AS payload";
        string executionSql = $"SELECT '{secret}' AS payload";
        using var events = new QueryEventRecorder();
        Database database = await Database.OpenInMemoryAsync(
            CreateOptions(SqlTextCaptureMode.Normalized),
            Ct);
        await using var session = new DirectDatabaseSession(database);

        await using (QueryResult result = await session.ExecuteAsync(
                         executionSql,
                         sourceSql,
                         Ct))
        {
            Assert.Single(await result.ToListAsync(Ct));
        }

        CSharpDbQueryCompletedEvent completed =
            Assert.Single(events.Events<CSharpDbQueryCompletedEvent>());
        QueryFingerprintResult expected =
            SqlQueryFingerprintProvider.Instance.NormalizeAndFingerprint(sourceSql, Ct);
        Assert.Equal(expected.Fingerprint, completed.Context.QueryFingerprint);
        Assert.Equal(expected.NormalizedText, completed.CapturedSqlText);
        Assert.DoesNotContain(secret, completed.CapturedSqlText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PreparedSimpleInsert_EmitsSourceFingerprintWithoutBoundSecret()
    {
        const string secret = "prepared-insert-secret-4f19";
        const string sourceSql =
            "INSERT INTO prepared_items (id, payload) VALUES (@id, @payload)";
        using var events = new QueryEventRecorder();
        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false",
            CreateOptions(SqlTextCaptureMode.Normalized));
        await connection.OpenAsync(Ct);
        await ExecuteNonQueryAsync(
            connection,
            "CREATE TABLE prepared_items (id INTEGER PRIMARY KEY, payload TEXT)");
        events.Clear();

        await using CSharpDbCommand command = (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = sourceSql;
        command.Parameters.AddWithValue("@id", 1);
        command.Parameters.AddWithValue("@payload", secret);
        command.Prepare();

        Assert.Equal(1, await command.ExecuteNonQueryAsync(Ct));

        CSharpDbQueryCompletedEvent completed =
            Assert.Single(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Equal(
            SqlQueryFingerprintProvider.Instance.CreateFingerprint(sourceSql, Ct),
            completed.Context.QueryFingerprint);
        Assert.Equal(1, completed.RowsAffected);
        Assert.Equal(CSharpDbTransport.Direct, completed.Context.Transport);
        Assert.NotNull(completed.Context.SessionId);
        Assert.Equal(TimeSpan.Zero, completed.QueueDuration);
        Assert.Equal(SqlTextCaptureMode.Normalized, completed.SqlTextCaptureMode);
        string serialized = JsonSerializer.Serialize(
            completed,
            CSharpDbObservabilityJsonContext.Default.CSharpDbQueryCompletedEvent);
        Assert.DoesNotContain(secret, serialized, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PooledPreparedReader_EmitsOneTerminalEventOnReaderDisposal()
    {
        const string secret = "pooled-reader-secret-b891";
        const string sourceSql =
            "SELECT @payload AS payload FROM prepared_items ORDER BY id";
        using var events = new QueryEventRecorder();
        string connectionString =
            $"Data Source={_databasePath};Pooling=true;Max Pool Size=1";
        await using var connection = new CSharpDbConnection(
            connectionString,
            CreateOptions(SqlTextCaptureMode.None));
        await connection.OpenAsync(Ct);
        await ExecuteNonQueryAsync(
            connection,
            "CREATE TABLE prepared_items (id INTEGER PRIMARY KEY)");
        await ExecuteNonQueryAsync(
            connection,
            "INSERT INTO prepared_items VALUES (1), (2)");
        events.Clear();

        await using CSharpDbCommand command = (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = sourceSql;
        command.Parameters.AddWithValue("@payload", secret);
        command.Prepare();
        DbDataReader reader = await command.ExecuteReaderAsync(Ct);

        Assert.True(await reader.ReadAsync(Ct));
        Assert.Equal(secret, reader.GetString(0));
        Assert.Empty(events.Events<CSharpDbQueryCompletedEvent>());

        await reader.DisposeAsync();
        await reader.DisposeAsync();

        CSharpDbQueryCompletedEvent completed =
            Assert.Single(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Equal(
            SqlQueryFingerprintProvider.Instance.CreateFingerprint(sourceSql, Ct),
            completed.Context.QueryFingerprint);
        Assert.Equal(1, completed.RowsProduced);
        Assert.Equal(CSharpDbTransport.Direct, completed.Context.Transport);
        Assert.NotNull(completed.Context.SessionId);
        Assert.Equal(SqlTextCaptureMode.None, completed.SqlTextCaptureMode);
        Assert.Null(completed.CapturedSqlText);
        string serialized = JsonSerializer.Serialize(
            completed,
            CSharpDbObservabilityJsonContext.Default.CSharpDbQueryCompletedEvent);
        Assert.DoesNotContain(secret, serialized, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DisabledObservability_EmitsNoPreparedCommandEvents()
    {
        using var events = new QueryEventRecorder();
        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false",
            CreateOptions(SqlTextCaptureMode.None, enabled: false));
        await connection.OpenAsync(Ct);
        await ExecuteNonQueryAsync(
            connection,
            "CREATE TABLE prepared_items (id INTEGER PRIMARY KEY)");

        await using CSharpDbCommand command = (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = "INSERT INTO prepared_items VALUES (@id)";
        command.Parameters.AddWithValue("@id", 1);
        command.Prepare();

        Assert.Equal(1, await command.ExecuteNonQueryAsync(Ct));
        Assert.Empty(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Empty(events.Events<CSharpDbQueryFailedEvent>());
        Assert.Empty(events.Events<CSharpDbQueryCanceledEvent>());
    }

    [Fact]
    public async Task MissingPreparedParameter_EmitsOneSafePreDispatchFailure()
    {
        const string sourceSql = "SELECT @payload AS payload";
        using var events = new QueryEventRecorder();
        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false",
            CreateOptions(SqlTextCaptureMode.Normalized));
        await connection.OpenAsync(Ct);
        events.Clear();

        await using CSharpDbCommand command = (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = sourceSql;
        command.Prepare();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => command.ExecuteScalarAsync(Ct));

        CSharpDbQueryFailedEvent failed =
            Assert.Single(events.Events<CSharpDbQueryFailedEvent>());
        Assert.Empty(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Empty(events.Events<CSharpDbQueryCanceledEvent>());
        Assert.Equal(CSharpDbTransport.Direct, failed.Context.Transport);
        Assert.NotNull(failed.Context.SessionId);
        Assert.Equal(
            SqlQueryFingerprintProvider.Instance.CreateFingerprint(sourceSql, Ct),
            failed.Context.QueryFingerprint);
        Assert.Equal(TimeSpan.Zero, failed.QueueDuration);
        Assert.Equal(failed.TotalDuration, failed.ExecutionAndConsumptionDuration);
        Assert.Equal("invalid_argument", failed.Error!.Code);

        string serialized = JsonSerializer.Serialize(
            failed,
            CSharpDbObservabilityJsonContext.Default.CSharpDbQueryFailedEvent);
        Assert.DoesNotContain(_databasePath, serialized, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("was not supplied", serialized, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task PooledGateCancellation_EmitsOneCanceledEventWithMeasuredQueue()
    {
        using var gateEntered = new ManualResetEventSlim();
        using var gateRelease = new ManualResetEventSlim();
        DatabaseOptions options = CreateOptions(SqlTextCaptureMode.None);
        options.ObservabilityOptions!.Logging.SlowQueries = true;
        options.ObservabilityOptions.Logging.SlowQueryThreshold =
            TimeSpan.FromMilliseconds(10);
        options = options
            .ConfigureFunctions(functions => functions.AddScalar(
                "HoldAdoPoolGate",
                0,
                new DbScalarFunctionOptions(DbType.Integer, IsDeterministic: false),
                (_, _) =>
                {
                    gateEntered.Set();
                    gateRelease.Wait();
                    return DbValue.FromInteger(1);
                }));
        string connectionString =
            $"Data Source={_databasePath};Pooling=true;Max Pool Size=2";
        using var events = new QueryEventRecorder();
        await using var blockerConnection = new CSharpDbConnection(connectionString, options);
        await using var queuedConnection = new CSharpDbConnection(connectionString, options);
        await blockerConnection.OpenAsync(Ct);
        await queuedConnection.OpenAsync(Ct);
        await ExecuteNonQueryAsync(
            blockerConnection,
            "CREATE TABLE gate_items (id INTEGER PRIMARY KEY)");
        events.Clear();

        await using CSharpDbCommand blockingCommand =
            (CSharpDbCommand)blockerConnection.CreateCommand();
        blockingCommand.CommandText =
            "INSERT INTO gate_items VALUES (HoldAdoPoolGate())";
        Task<int> blockingTask = Task.Run(
            () => blockingCommand.ExecuteNonQueryAsync(Ct));

        try
        {
            Assert.True(await Task.Run(
                () => gateEntered.Wait(TimeSpan.FromSeconds(5))));

            const string queuedSql = "SELECT COUNT(*) FROM gate_items";
            await using CSharpDbCommand queuedCommand =
                (CSharpDbCommand)queuedConnection.CreateCommand();
            queuedCommand.CommandText = queuedSql;
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            cancellation.CancelAfter(TimeSpan.FromMilliseconds(150));

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => queuedCommand.ExecuteScalarAsync(cancellation.Token));

            CSharpDbQueryCanceledEvent canceled =
                Assert.Single(events.Events<CSharpDbQueryCanceledEvent>());
            Assert.Empty(events.Events<CSharpDbQueryFailedEvent>());
            Assert.Equal(CSharpDbTransport.Direct, canceled.Context.Transport);
            Assert.NotNull(canceled.Context.SessionId);
            Assert.Equal(
                SqlQueryFingerprintProvider.Instance.CreateFingerprint(queuedSql, Ct),
                canceled.Context.QueryFingerprint);
            Assert.InRange(
                canceled.QueueDuration,
                TimeSpan.Zero,
                canceled.TotalDuration);
            Assert.Equal(
                canceled.TotalDuration - canceled.QueueDuration,
                canceled.ExecutionAndConsumptionDuration);
            Assert.True(canceled.QueueDuration >= TimeSpan.FromMilliseconds(50));

            CSharpDbSlowQueryEvent slow =
                Assert.Single(events.Events<CSharpDbSlowQueryEvent>());
            Assert.Equal(canceled.Context.OperationId, slow.Context.OperationId);
            Assert.Equal(CSharpDbOperationOutcome.Canceled, slow.Outcome);
            Assert.Equal(canceled.QueueDuration, slow.QueueDuration);
            Assert.Equal(
                slow.TotalDuration - slow.QueueDuration,
                slow.ExecutionAndConsumptionDuration);
        }
        finally
        {
            gateRelease.Set();
            await blockingTask;
        }
    }

    [Fact]
    public async Task DirectSharedAndPooledCommands_EmitExactlyOneTerminalEvent()
    {
        string sharedName = Guid.NewGuid().ToString("N");
        string[] connectionStrings =
        [
            "Data Source=:memory:;Pooling=false",
            $"Data Source=:memory:{sharedName}",
            $"Data Source={_databasePath};Pooling=true;Max Pool Size=1",
        ];

        using var events = new QueryEventRecorder();
        var operationIds = new HashSet<OpaqueDiagnosticsId>();
        foreach (string connectionString in connectionStrings)
        {
            DatabaseOptions options = CreateOptions(SqlTextCaptureMode.None);
            await using var connection = new CSharpDbConnection(connectionString, options);
            await connection.OpenAsync(Ct);
            events.Clear();

            await using CSharpDbCommand command =
                (CSharpDbCommand)connection.CreateCommand();
            command.CommandText = "SELECT 42";
            Assert.Equal(
                42L,
                Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));

            CSharpDbQueryCompletedEvent completed =
                Assert.Single(events.Events<CSharpDbQueryCompletedEvent>());
            Assert.Empty(events.Events<CSharpDbQueryFailedEvent>());
            Assert.Empty(events.Events<CSharpDbQueryCanceledEvent>());
            Assert.Equal(CSharpDbTransport.Direct, completed.Context.Transport);
            Assert.NotNull(completed.Context.SessionId);
            Assert.Equal(
                SqlQueryFingerprintProvider.Instance.CreateFingerprint("SELECT 42", Ct),
                completed.Context.QueryFingerprint);
            Assert.True(operationIds.Add(completed.Context.OperationId));
        }
    }

    [Fact]
    public async Task NamedSharedMemory_FirstExplicitOptionsEstablishImmutableHostConfiguration()
    {
        string connectionString = $"Data Source=:memory:{Guid.NewGuid():N}";
        DatabaseOptions options = CreateOptions(SqlTextCaptureMode.None);
        await using var first = new CSharpDbConnection(connectionString, options);
        await using var compatible = new CSharpDbConnection(connectionString, options);
        await first.OpenAsync(Ct);
        await compatible.OpenAsync(Ct);

        await ExecuteNonQueryAsync(
            first,
            "CREATE TABLE shared_observed (id INTEGER PRIMARY KEY)");
        Assert.Equal(
            0L,
            await ExecuteScalarAsync(
                compatible,
                "SELECT COUNT(*) FROM shared_observed"));

        await using var incompatible = new CSharpDbConnection(
            connectionString,
            CreateOptions(SqlTextCaptureMode.None));
        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => incompatible.OpenAsync(Ct));
        Assert.Contains("different DatabaseOptions", exception.Message, StringComparison.Ordinal);

        options.ObservabilityOptions!.DatabaseAlias = "mutated-shared-host";
        await using var mutatedOriginal = new CSharpDbConnection(
            connectionString,
            options);
        InvalidOperationException mutationException =
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => mutatedOriginal.OpenAsync(Ct));
        Assert.Contains(
            "different DatabaseOptions",
            mutationException.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void RemoteCommandBoundary_DoesNotCreateAnAdoQueryObservation()
    {
        using var events = new QueryEventRecorder();
        using var connection = new CSharpDbConnection(
            "Endpoint=http://127.0.0.1:7777;Transport=Rest;Pooling=false",
            CreateOptions(SqlTextCaptureMode.None));

        Assert.Null(connection.StartCommandObservation("SELECT 1"));
        Assert.Empty(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Empty(events.Events<CSharpDbQueryFailedEvent>());
        Assert.Empty(events.Events<CSharpDbQueryCanceledEvent>());
    }

    [Fact]
    public async Task PooledClassificationFailure_EmitsOneSafePreDispatchFailure()
    {
        const string secret = "classification_secret_81d4";
        using var events = new QueryEventRecorder();
        await using var connection = new CSharpDbConnection(
            $"Data Source={_databasePath};Pooling=true;Max Pool Size=1",
            CreateOptions(SqlTextCaptureMode.None));
        await connection.OpenAsync(Ct);
        events.Clear();

        await using CSharpDbCommand command = (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = $"SELEC {secret}";
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => command.ExecuteScalarAsync(Ct));

        CSharpDbQueryFailedEvent failed =
            Assert.Single(events.Events<CSharpDbQueryFailedEvent>());
        Assert.Empty(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Empty(events.Events<CSharpDbQueryCanceledEvent>());
        Assert.Equal("csharpdb.syntax", failed.Error!.Code);
        Assert.Equal(TimeSpan.Zero, failed.QueueDuration);
        Assert.Equal(failed.TotalDuration, failed.ExecutionAndConsumptionDuration);
        Assert.Null(failed.CapturedSqlText);
        string serialized = JsonSerializer.Serialize(
            failed,
            CSharpDbObservabilityJsonContext.Default.CSharpDbQueryFailedEvent);
        Assert.DoesNotContain(secret, serialized, StringComparison.Ordinal);
        Assert.DoesNotContain(_databasePath, serialized, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task SuppressedDiagnostics_EmitNoAdoSuccessOrPreDispatchFailure()
    {
        using var events = new QueryEventRecorder();
        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false",
            CreateOptions(SqlTextCaptureMode.None));
        await connection.OpenAsync(Ct);
        events.Clear();

        using (CSharpDbOperationScope.SuppressDiagnostics())
        {
            await using CSharpDbCommand success =
                (CSharpDbCommand)connection.CreateCommand();
            success.CommandText = "SELECT 1";
            Assert.NotNull(await success.ExecuteScalarAsync(Ct));

            await using CSharpDbCommand failure =
                (CSharpDbCommand)connection.CreateCommand();
            failure.CommandText = "SELECT @missing";
            failure.Prepare();
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => failure.ExecuteScalarAsync(Ct));
        }

        Assert.Empty(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Empty(events.Events<CSharpDbQueryFailedEvent>());
        Assert.Empty(events.Events<CSharpDbQueryCanceledEvent>());
        Assert.Empty(events.Events<CSharpDbSlowQueryEvent>());
    }

    [Fact]
    public async Task OpenConnection_SnapshotsAdoObservabilityOptions()
    {
        DatabaseOptions options = CreateOptions(SqlTextCaptureMode.None);
        using var events = new QueryEventRecorder();
        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false",
            options);
        await connection.OpenAsync(Ct);
        events.Clear();

        options.ObservabilityOptions!.Enabled = false;
        options.ObservabilityOptions.DatabaseAlias = "mutated-after-open";
        await using CSharpDbCommand command = (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = "SELECT 1";
        _ = await command.ExecuteScalarAsync(Ct);

        CSharpDbQueryCompletedEvent completed =
            Assert.Single(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Equal("data-prepared-tests", completed.Context.DatabaseAlias);
    }

    [Fact]
    public async Task PooledRuntimeSnapshot_RemainsEnabledAfterCallerDisablesOptions()
    {
        string connectionString =
            $"Data Source={_databasePath};Pooling=true;Max Pool Size=1";
        DatabaseOptions options = CreateOptions(SqlTextCaptureMode.None);
        using var events = new QueryEventRecorder();
        await using (var first = new CSharpDbConnection(connectionString, options))
        {
            await first.OpenAsync(Ct);
            await first.CloseAsync();
        }
        events.Clear();

        options.ObservabilityOptions!.Enabled = false;
        options.ObservabilityOptions.DatabaseAlias = "mutated-disabled-pool";
        await using var second = new CSharpDbConnection(connectionString, options);
        await second.OpenAsync(Ct);
        await using CSharpDbCommand command = (CSharpDbCommand)second.CreateCommand();
        command.CommandText = "SELECT 1";
        _ = await command.ExecuteScalarAsync(Ct);

        CSharpDbQueryCompletedEvent completed =
            Assert.Single(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Equal("data-prepared-tests", completed.Context.DatabaseAlias);
    }

    [Fact]
    public async Task PooledRuntimeSnapshot_RemainsDisabledAfterCallerEnablesOptions()
    {
        string connectionString =
            $"Data Source={_databasePath};Pooling=true;Max Pool Size=1";
        DatabaseOptions options = CreateOptions(
            SqlTextCaptureMode.None,
            enabled: false);
        using var events = new QueryEventRecorder();
        await using (var first = new CSharpDbConnection(connectionString, options))
        {
            await first.OpenAsync(Ct);
            await first.CloseAsync();
        }

        options.ObservabilityOptions!.Enabled = true;
        options.ObservabilityOptions.DatabaseAlias = "mutated-enabled-pool";
        await using var second = new CSharpDbConnection(connectionString, options);
        await second.OpenAsync(Ct);
        await using CSharpDbCommand command = (CSharpDbCommand)second.CreateCommand();
        command.CommandText = "SELECT 1";
        _ = await command.ExecuteScalarAsync(Ct);

        Assert.Empty(events.Events<CSharpDbQueryCompletedEvent>());
        Assert.Empty(events.Events<CSharpDbQueryFailedEvent>());
        Assert.Empty(events.Events<CSharpDbQueryCanceledEvent>());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task PooledDeferredOpen_UsesFrozenPlanWhenCallerMutatesOptions(
        bool initiallyEnabled)
    {
        string connectionString =
            $"Data Source={_databasePath};Pooling=true;Max Pool Size=1";
        DatabaseOptions options = CreateOptions(
            SqlTextCaptureMode.None,
            enabled: initiallyEnabled);
        using var events = new QueryEventRecorder();
        CSharpDbConnectionPool.BeforeFirstPhysicalOpenForTest = () =>
            options.ObservabilityOptions!.Enabled = !initiallyEnabled;

        try
        {
            await using var connection = new CSharpDbConnection(connectionString, options);
            await connection.OpenAsync(Ct);
            events.Clear();
            await using CSharpDbCommand command = (CSharpDbCommand)connection.CreateCommand();
            command.CommandText = "SELECT 1";
            _ = await command.ExecuteScalarAsync(Ct);

            CSharpDbQueryCompletedEvent[] completed =
                events.Events<CSharpDbQueryCompletedEvent>();
            Assert.Equal(initiallyEnabled ? 1 : 0, completed.Length);
        }
        finally
        {
            CSharpDbConnectionPool.BeforeFirstPhysicalOpenForTest = null;
        }
    }

    [Fact]
    public async Task PooledQueuedTerminal_ListenerCanSynchronouslyReenterAfterGateRelease()
    {
        using var gateEntered = new ManualResetEventSlim();
        using var gateRelease = new ManualResetEventSlim();
        using var queueWaitStarted = new ManualResetEventSlim();
        DatabaseOptions options = CreateOptions(SqlTextCaptureMode.None)
            .ConfigureFunctions(functions => functions.AddScalar(
                "HoldAdoReentrantGate",
                0,
                new DbScalarFunctionOptions(DbType.Integer, IsDeterministic: false),
                (_, _) =>
                {
                    gateEntered.Set();
                    gateRelease.Wait();
                    return DbValue.FromInteger(1);
                }));
        string connectionString =
            $"Data Source={_databasePath};Pooling=true;Max Pool Size=2";
        await using var blockerConnection = new CSharpDbConnection(connectionString, options);
        await using var queuedConnection = new CSharpDbConnection(connectionString, options);
        await blockerConnection.OpenAsync(Ct);
        await queuedConnection.OpenAsync(Ct);

        await using CSharpDbCommand blockingCommand =
            (CSharpDbCommand)blockerConnection.CreateCommand();
        blockingCommand.CommandText = "SELECT HoldAdoReentrantGate()";
        Task<object?> blockingTask = Task.Run(
            () => blockingCommand.ExecuteScalarAsync(Ct));

        try
        {
            Assert.True(await Task.Run(
                () => gateEntered.Wait(TimeSpan.FromSeconds(5))));

            using var sentinel = new QueryEventRecorder();
            using var reentrant = new ReentrantQueryRecorder(
                async cancellationToken =>
                {
                    await using CSharpDbCommand command =
                        (CSharpDbCommand)queuedConnection.CreateCommand();
                    command.CommandText = "SELECT 7";
                    Assert.Equal(
                        7L,
                        Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken)));
                });
            AdoCommandObservation.QueueWaitStartingForTest = queueWaitStarted.Set;
            await using CSharpDbCommand queuedCommand =
                (CSharpDbCommand)queuedConnection.CreateCommand();
            queuedCommand.CommandText = "SELECT 2";
            Task<object?> queuedTask = Task.Run(
                () => queuedCommand.ExecuteScalarAsync(Ct));
            Assert.True(await Task.Run(
                () => queueWaitStarted.Wait(TimeSpan.FromSeconds(5))));
            AdoCommandObservation.QueueWaitStartingForTest = null;

            gateRelease.Set();
            Assert.Equal(1L, Convert.ToInt64(await blockingTask));
            Assert.Equal(2L, Convert.ToInt64(await queuedTask));
            await reentrant.WaitForReentryAsync(Ct);

            Assert.Equal(2, sentinel.Events<CSharpDbQueryCompletedEvent>().Length);
            Assert.Equal(2, reentrant.Events.Length);
            Assert.All(
                reentrant.Events,
                static completed =>
                {
                    Assert.Equal(CSharpDbTransport.Direct, completed.Context.Transport);
                    Assert.NotNull(completed.Context.SessionId);
                });
        }
        finally
        {
            AdoCommandObservation.QueueWaitStartingForTest = null;
            gateRelease.Set();
            await blockingTask;
        }
    }

    [Fact]
    public async Task PooledQueuedCommand_MidWaitFirstSubscriberDoesNotObserveInFlightTerminal()
    {
        using var gateEntered = new ManualResetEventSlim();
        using var gateRelease = new ManualResetEventSlim();
        using var queueWaitStarted = new ManualResetEventSlim();
        DatabaseOptions options = CreateOptions(SqlTextCaptureMode.None)
            .ConfigureFunctions(functions => functions.AddScalar(
                "HoldAdoLateSubscriberGate",
                0,
                new DbScalarFunctionOptions(DbType.Integer, IsDeterministic: false),
                (_, _) =>
                {
                    gateEntered.Set();
                    gateRelease.Wait();
                    return DbValue.FromInteger(1);
                }));
        string connectionString =
            $"Data Source={_databasePath};Pooling=true;Max Pool Size=2";
        await using var blockerConnection = new CSharpDbConnection(connectionString, options);
        await using var queuedConnection = new CSharpDbConnection(connectionString, options);
        await blockerConnection.OpenAsync(Ct);
        await queuedConnection.OpenAsync(Ct);
        await using CSharpDbCommand blockingCommand =
            (CSharpDbCommand)blockerConnection.CreateCommand();
        blockingCommand.CommandText = "SELECT HoldAdoLateSubscriberGate()";
        Task<object?> blockingTask = Task.Run(
            () => blockingCommand.ExecuteScalarAsync(Ct));

        try
        {
            Assert.True(await Task.Run(
                () => gateEntered.Wait(TimeSpan.FromSeconds(5))));
            AdoCommandObservation.QueueWaitStartingForTest = queueWaitStarted.Set;
            await using CSharpDbCommand queuedCommand =
                (CSharpDbCommand)queuedConnection.CreateCommand();
            queuedCommand.CommandText = "SELECT 2";
            Task<object?> queuedTask = Task.Run(
                () => queuedCommand.ExecuteScalarAsync(Ct));
            Assert.True(await Task.Run(
                () => queueWaitStarted.Wait(TimeSpan.FromSeconds(5))));
            AdoCommandObservation.QueueWaitStartingForTest = null;

            using var lateSubscriber = new QueryEventRecorder();
            gateRelease.Set();
            _ = await blockingTask;
            _ = await queuedTask;

            Assert.Empty(lateSubscriber.Events<CSharpDbQueryCompletedEvent>());
            Assert.Empty(lateSubscriber.Events<CSharpDbQueryFailedEvent>());
            Assert.Empty(lateSubscriber.Events<CSharpDbQueryCanceledEvent>());
        }
        finally
        {
            AdoCommandObservation.QueueWaitStartingForTest = null;
            gateRelease.Set();
            await blockingTask;
        }
    }

    private static DatabaseOptions CreateOptions(
        SqlTextCaptureMode captureMode,
        bool enabled = true)
        => new()
        {
            ObservabilityOptions = new CSharpDbObservabilityOptions
            {
                Enabled = enabled,
                DatabaseAlias = "data-prepared-tests",
                Logging = new CSharpDbLoggingOptions
                {
                    Enabled = true,
                    Queries = true,
                    SlowQueries = false,
                    SqlText = captureMode,
                },
            },
        };

    private static async ValueTask ExecuteNonQueryAsync(
        CSharpDbConnection connection,
        string sql)
    {
        await using CSharpDbCommand command = (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = sql;
        _ = await command.ExecuteNonQueryAsync(Ct);
    }

    private static async ValueTask<object?> ExecuteScalarAsync(
        CSharpDbConnection connection,
        string sql)
    {
        await using CSharpDbCommand command = (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync(Ct);
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
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
                static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));
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

    private sealed class ReentrantQueryRecorder :
        IObserver<KeyValuePair<string, object?>>,
        IDisposable
    {
        private static readonly TimeSpan ReentryTimeout = TimeSpan.FromSeconds(5);
        private readonly object _gate = new();
        private readonly List<CSharpDbQueryCompletedEvent> _events = [];
        private readonly Func<CancellationToken, Task> _reentry;
        private readonly TaskCompletionSource _reentryCompleted =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly IDisposable _subscription;
        private Exception? _reentryError;
        private int _reentryStarted;

        internal ReentrantQueryRecorder(Func<CancellationToken, Task> reentry)
        {
            _reentry = reentry;
            _subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
                this,
                static name => name == CSharpDbLogEvents.QueryCompleted.Name);
        }

        internal CSharpDbQueryCompletedEvent[] Events
        {
            get
            {
                lock (_gate)
                    return _events.ToArray();
            }
        }

        internal async Task WaitForReentryAsync(CancellationToken cancellationToken)
        {
            await _reentryCompleted.Task.WaitAsync(
                TimeSpan.FromSeconds(10),
                cancellationToken);
            if (_reentryError is not null)
                System.Runtime.ExceptionServices.ExceptionDispatchInfo
                    .Capture(_reentryError)
                    .Throw();
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Value is not CSharpDbQueryCompletedEvent completed)
                return;

            lock (_gate)
                _events.Add(completed);
            if (Interlocked.Exchange(ref _reentryStarted, 1) != 0)
                return;

            try
            {
                using var timeout = new CancellationTokenSource(ReentryTimeout);
                _reentry(timeout.Token)
                    .WaitAsync(ReentryTimeout)
                    .GetAwaiter()
                    .GetResult();
            }
            catch (Exception exception)
            {
                _reentryError = exception;
            }
            finally
            {
                _reentryCompleted.TrySetResult();
            }
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void Dispose() => _subscription.Dispose();
    }
}

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ObservabilityDiagnosticsCollection
{
    public const string Name = "ObservabilityDiagnostics";
}
