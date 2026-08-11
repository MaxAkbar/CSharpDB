using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Engine;
using CSharpDB.Observability;
using ClientTransport = CSharpDB.Client.CSharpDbTransport;

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class CSharpDbConnectionObservabilityCapabilityTests : IAsyncLifetime
{
    private readonly List<string> _paths = [];
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
        => await CSharpDbConnection.ClearAllPoolsAsync();

    public async ValueTask DisposeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();
        foreach (string path in _paths)
        {
            DeleteIfExists(path);
            DeleteIfExists(path + ".wal");
        }
    }

    [Fact]
    public async Task DirectCapability_ProjectsExactRuntimeQueriesPlanDetailAndSession()
    {
        const string secretSql = "SELECT 'capability_secret_literal'";
        DatabaseOptions options = CreateOptions(
            "ado_capability_direct",
            SqlTextCaptureMode.Normalized);
        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false",
            options);
        await connection.OpenAsync(Ct);
        ICSharpDbObservabilityClient diagnostics =
            Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(connection);

        Assert.Equal("capability_secret_literal", await ExecuteScalarAsync(
            connection,
            secretSql));

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> runtime =
            await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
        Assert.Equal(DiagnosticsScope.Instance, runtime.Metadata.Scope);
        Assert.Equal(DiagnosticsAvailability.Available, runtime.Metadata.Availability);
        Assert.Equal("ado_capability_direct", runtime.Metadata.DatabaseAlias);
        Assert.Null(runtime.Shards);
        Assert.Null(runtime.RuntimeFamilies);
        QueryDiagnosticsSummary querySummary = Assert.IsType<QueryDiagnosticsSummary>(
            runtime.Aggregate.Queries.Value);
        Assert.True(querySummary.RequestCount >= 1);
        ConnectionDiagnosticsSnapshot connectionSummary =
            Assert.IsType<ConnectionDiagnosticsSnapshot>(
                runtime.Aggregate.Connections.Value);
        Assert.Equal(1, connectionSummary.ActiveLogicalSessions);

        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>
            recent = await diagnostics.GetRecentQueriesAsync(16, Ct);
        RecentQuerySnapshot completed = Assert.Single(
            Assert.IsAssignableFrom<IReadOnlyList<RecentQuerySnapshot>>(
                recent.Aggregate.Records));
        Assert.Equal(CSharpDbOperationOutcome.Succeeded, completed.Outcome);
        Assert.NotNull(completed.Fingerprint);

        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>
            plan = await diagnostics.GetQueryPlanDiagnosticsAsync(
                completed.OperationId,
                Ct);
        QueryPlanDiagnosticsSnapshot planValue =
            Assert.IsType<QueryPlanDiagnosticsSnapshot>(plan.Aggregate.Value);
        Assert.Equal(completed.OperationId, planValue.OperationId);

        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>
            detail = await diagnostics.GetQueryDetailAsync(completed.OperationId, Ct);
        QueryDetailSnapshot detailValue =
            Assert.IsType<QueryDetailSnapshot>(detail.Aggregate.Value);
        Assert.Equal(SqlTextCaptureMode.Normalized, detailValue.CaptureMode);
        Assert.False(string.IsNullOrWhiteSpace(detailValue.CapturedSqlText));

        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>
            sessions = await diagnostics.GetSessionsAsync(4, Ct);
        SessionDiagnosticsSnapshot session = Assert.Single(
            Assert.IsAssignableFrom<IReadOnlyList<SessionDiagnosticsSnapshot>>(
                sessions.Aggregate.Records));
        Assert.Equal(completed.SessionId, session.SessionId);
        Assert.Equal(CSharpDB.Observability.CSharpDbTransport.Direct, session.Transport);

        string ordinaryJson = JsonSerializer.Serialize(new
        {
            runtime,
            recent,
            sessions,
        });
        Assert.DoesNotContain("capability_secret_literal", ordinaryJson, StringComparison.Ordinal);
        Assert.DoesNotContain(":memory:", ordinaryJson, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DisabledCapability_IsLazyStableAndAllocatesNoRuntimeStateOrSidecar()
    {
        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false");
        await connection.OpenAsync(Ct);
        DirectDatabaseSession session = Assert.IsType<DirectDatabaseSession>(
            connection.GetSession());
        Assert.Null(session.RuntimeDiagnosticsState);
        Assert.False(session.HasDiagnosticsSidecarForTest);
        Assert.DoesNotContain(
            typeof(CSharpDbConnection).GetFields(
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic),
            field => field.Name.Contains(
                "runtimeDiagnostics",
                StringComparison.OrdinalIgnoreCase));

        var diagnostics = (ICSharpDbObservabilityClient)connection;
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> first =
            await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> second =
            await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
        Assert.Equal(DiagnosticsAvailability.Disabled, first.Metadata.Availability);
        Assert.Equal(first.Metadata.ServerInstanceId, second.Metadata.ServerInstanceId);
        Assert.Equal(first.Metadata.CounterEpoch, second.Metadata.CounterEpoch);
        Assert.Equal(DiagnosticsAvailability.Disabled, first.Aggregate.Queries.Availability);
        Assert.Equal(DiagnosticsAvailability.Disabled, first.Aggregate.Connections.Availability);
        Assert.Null(session.RuntimeDiagnosticsState);
        Assert.False(session.HasDiagnosticsSidecarForTest);

        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>> active =
            await diagnostics.GetActiveQueriesAsync(1, Ct);
        Assert.Equal(DiagnosticsAvailability.Disabled, active.Metadata.Availability);
        Assert.Null(active.Aggregate.Records);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>
            sessions = await diagnostics.GetSessionsAsync(1, Ct);
        Assert.Equal(DiagnosticsAvailability.Disabled, sessions.Metadata.Availability);
        Assert.Null(sessions.Aggregate.Records);
    }

    [Theory]
    [InlineData(SqlTextCaptureMode.Raw, DiagnosticsAvailability.Available)]
    [InlineData(SqlTextCaptureMode.None, DiagnosticsAvailability.Unavailable)]
    public async Task QueryDetail_RespectsExactRawAndNoneCapturePolicy(
        SqlTextCaptureMode captureMode,
        DiagnosticsAvailability expectedAvailability)
    {
        const string sql = "SELECT 'raw_detail_secret'";
        await using var connection = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false",
            CreateOptions($"detail_{captureMode.ToString().ToLowerInvariant()}", captureMode));
        await connection.OpenAsync(Ct);
        var diagnostics = (ICSharpDbObservabilityClient)connection;
        Assert.Equal("raw_detail_secret", await ExecuteScalarAsync(connection, sql));
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>
            recent = await diagnostics.GetRecentQueriesAsync(4, Ct);
        RecentQuerySnapshot completed = Assert.Single(
            Assert.IsAssignableFrom<IReadOnlyList<RecentQuerySnapshot>>(
                recent.Aggregate.Records));

        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>> detail =
            await diagnostics.GetQueryDetailAsync(completed.OperationId, Ct);
        Assert.Equal(expectedAvailability, detail.Metadata.Availability);
        if (captureMode == SqlTextCaptureMode.Raw)
        {
            QueryDetailSnapshot value = Assert.IsType<QueryDetailSnapshot>(
                detail.Aggregate.Value);
            Assert.Equal(SqlTextCaptureMode.Raw, value.CaptureMode);
            Assert.Equal(sql, value.CapturedSqlText);
        }
        else
        {
            Assert.Null(detail.Aggregate.Value);
        }
    }

    [Fact]
    public async Task DisabledIdentity_IsSharedOnlyByTheSamePhysicalPoolOrHost()
    {
        string firstPath = CreatePath("disabled-owner-a");
        string secondPath = CreatePath("disabled-owner-b");
        string firstPool = $"Data Source={firstPath};Pooling=true;Max Pool Size=2";
        string secondPool = $"Data Source={secondPath};Pooling=true;Max Pool Size=2";
        await using var poolA1 = new CSharpDbConnection(firstPool);
        await using var poolA2 = new CSharpDbConnection(firstPool);
        await using var poolB = new CSharpDbConnection(secondPool);
        await poolA1.OpenAsync(Ct);
        await poolA2.OpenAsync(Ct);
        await poolB.OpenAsync(Ct);

        string poolA1Identity = (await ((ICSharpDbObservabilityClient)poolA1)
            .GetRuntimeDiagnosticsAsync(Ct)).Metadata.ServerInstanceId;
        string poolA2Identity = (await ((ICSharpDbObservabilityClient)poolA2)
            .GetRuntimeDiagnosticsAsync(Ct)).Metadata.ServerInstanceId;
        string poolBIdentity = (await ((ICSharpDbObservabilityClient)poolB)
            .GetRuntimeDiagnosticsAsync(Ct)).Metadata.ServerInstanceId;
        Assert.Equal(poolA1Identity, poolA2Identity);
        Assert.NotEqual(poolA1Identity, poolBIdentity);

        string sharedName = $"disabled_shared_{Guid.NewGuid():N}";
        string sharedConnectionString =
            $"Data Source=:memory:{sharedName};Pooling=false";
        await using var shared1 = new CSharpDbConnection(sharedConnectionString);
        await using var shared2 = new CSharpDbConnection(sharedConnectionString);
        await shared1.OpenAsync(Ct);
        await shared2.OpenAsync(Ct);
        string shared1Identity = (await ((ICSharpDbObservabilityClient)shared1)
            .GetRuntimeDiagnosticsAsync(Ct)).Metadata.ServerInstanceId;
        string shared2Identity = (await ((ICSharpDbObservabilityClient)shared2)
            .GetRuntimeDiagnosticsAsync(Ct)).Metadata.ServerInstanceId;
        Assert.Equal(shared1Identity, shared2Identity);
        Assert.NotEqual(poolA1Identity, shared1Identity);

        await using var direct1 = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false");
        await using var direct2 = new CSharpDbConnection(
            "Data Source=:memory:;Pooling=false");
        await direct1.OpenAsync(Ct);
        await direct2.OpenAsync(Ct);
        string direct1Identity = (await ((ICSharpDbObservabilityClient)direct1)
            .GetRuntimeDiagnosticsAsync(Ct)).Metadata.ServerInstanceId;
        string direct2Identity = (await ((ICSharpDbObservabilityClient)direct2)
            .GetRuntimeDiagnosticsAsync(Ct)).Metadata.ServerInstanceId;
        Assert.NotEqual(direct1Identity, direct2Identity);
    }

    [Fact]
    public async Task EnabledOwnerWithoutExactRuntimeState_ReturnsUnavailableWithoutInventingOne()
    {
        CSharpDbObservabilityOptions observability = CreateObservability(
            "missing_exact_state");
        Database database = await Database.OpenInMemoryAsync(ct: Ct);
        var session = new DirectDatabaseSession(
            database,
            observabilityOptionsSnapshot: observability);
        await using CSharpDbConnection connection = CreateOpenedConnection(session);
        var diagnostics = (ICSharpDbObservabilityClient)connection;

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> runtime =
            await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
        Assert.Equal(DiagnosticsAvailability.Unavailable, runtime.Metadata.Availability);
        Assert.Equal(DiagnosticsAvailability.Unavailable, runtime.Aggregate.Queries.Availability);
        Assert.Equal(DiagnosticsAvailability.Unavailable, runtime.Aggregate.Connections.Availability);
        Assert.Null(session.RuntimeDiagnosticsState);

        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>
            sessions = await diagnostics.GetSessionsAsync(4, Ct);
        Assert.Equal(DiagnosticsAvailability.Unavailable, sessions.Metadata.Availability);
        Assert.Null(sessions.Aggregate.Records);
        Assert.Null(session.RuntimeDiagnosticsState);
    }

    [Fact]
    public async Task PooledCapability_UsesOnlyItsPhysicalOwnerAndPrioritizesActiveSessionAtCap()
    {
        string firstPath = CreatePath("capability-pool-a");
        string secondPath = CreatePath("capability-pool-b");
        DatabaseOptions options = CreateOptions("same_safe_alias");
        string firstConnectionString =
            $"Data Source={firstPath};Pooling=true;Max Pool Size=2";
        string secondConnectionString =
            $"Data Source={secondPath};Pooling=true;Max Pool Size=2";
        await using var first = new CSharpDbConnection(firstConnectionString, options);
        await using var sameOwner = new CSharpDbConnection(firstConnectionString, options);
        await using var unrelated = new CSharpDbConnection(secondConnectionString, options);
        await first.OpenAsync(Ct);
        await sameOwner.OpenAsync(Ct);
        await unrelated.OpenAsync(Ct);

        var firstDiagnostics = (ICSharpDbObservabilityClient)first;
        var sameOwnerDiagnostics = (ICSharpDbObservabilityClient)sameOwner;
        var unrelatedDiagnostics = (ICSharpDbObservabilityClient)unrelated;
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> firstRuntime =
            await firstDiagnostics.GetRuntimeDiagnosticsAsync(Ct);
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> sameRuntime =
            await sameOwnerDiagnostics.GetRuntimeDiagnosticsAsync(Ct);
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> unrelatedRuntime =
            await unrelatedDiagnostics.GetRuntimeDiagnosticsAsync(Ct);
        Assert.Equal(firstRuntime.Metadata.ServerInstanceId, sameRuntime.Metadata.ServerInstanceId);
        Assert.NotEqual(firstRuntime.Metadata.ServerInstanceId, unrelatedRuntime.Metadata.ServerInstanceId);

        await ExecuteNonQueryAsync(
            sameOwner,
            "CREATE TABLE owner_rows (id INTEGER PRIMARY KEY)");
        await ExecuteNonQueryAsync(sameOwner, "INSERT INTO owner_rows VALUES (1)");
        await using CSharpDbCommand command =
            (CSharpDbCommand)sameOwner.CreateCommand();
        command.CommandText = "SELECT id FROM owner_rows";
        await using DbDataReader reader = await command.ExecuteReaderAsync(Ct);

        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>
            cappedSessions = await firstDiagnostics.GetSessionsAsync(1, Ct);
        SessionDiagnosticsSnapshot selected = Assert.Single(
            Assert.IsAssignableFrom<IReadOnlyList<SessionDiagnosticsSnapshot>>(
                cappedSessions.Aggregate.Records));
        Assert.True(selected.HasActiveReader);
        Assert.True(cappedSessions.Aggregate.IsTruncated);
        Assert.True(cappedSessions.Aggregate.Metadata.RecordsTruncated);
        Assert.Equal(1, cappedSessions.Aggregate.Capacity);
        Assert.Equal(0, cappedSessions.Aggregate.DroppedCount);

        await reader.DisposeAsync();
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>
            ownerRecent = await firstDiagnostics.GetRecentQueriesAsync(16, Ct);
        Assert.NotEmpty(Assert.IsAssignableFrom<IReadOnlyList<RecentQuerySnapshot>>(
            ownerRecent.Aggregate.Records));
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>
            unrelatedRecent = await unrelatedDiagnostics.GetRecentQueriesAsync(16, Ct);
        Assert.Empty(Assert.IsAssignableFrom<IReadOnlyList<RecentQuerySnapshot>>(
            unrelatedRecent.Aggregate.Records));

        string serialized = JsonSerializer.Serialize(new
        {
            firstRuntime,
            cappedSessions,
            ownerRecent,
        });
        Assert.DoesNotContain(firstPath, serialized, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(firstConnectionString, serialized, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task SharedMemoryCapability_ReusesOnlyTheOwningHostIdentity()
    {
        string sharedName = $"capability_shared_{Guid.NewGuid():N}";
        DatabaseOptions options = CreateOptions("shared_capability");
        string connectionString = $"Data Source=:memory:{sharedName};Pooling=false";
        await using var first = new CSharpDbConnection(connectionString, options);
        await using var second = new CSharpDbConnection(connectionString, options);
        await first.OpenAsync(Ct);
        await second.OpenAsync(Ct);

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> firstRuntime =
            await ((ICSharpDbObservabilityClient)first)
                .GetRuntimeDiagnosticsAsync(Ct);
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> secondRuntime =
            await ((ICSharpDbObservabilityClient)second)
                .GetRuntimeDiagnosticsAsync(Ct);
        Assert.Equal(firstRuntime.Metadata.ServerInstanceId, secondRuntime.Metadata.ServerInstanceId);
        Assert.Equal(firstRuntime.Metadata.CounterEpoch, secondRuntime.Metadata.CounterEpoch);
        ConnectionDiagnosticsSnapshot connections =
            Assert.IsType<ConnectionDiagnosticsSnapshot>(
                firstRuntime.Aggregate.Connections.Value);
        Assert.Equal(2, connections.ActiveLogicalSessions);

        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>
            sessions = await ((ICSharpDbObservabilityClient)first)
                .GetSessionsAsync(4, Ct);
        Assert.Equal(2, sessions.Aggregate.Records?.Count);
    }

    [Fact]
    public async Task RemoteSession_DelegatesCapabilityAndUnsupportedClientUsesSafeException()
    {
        ICSharpDbClient remoteClient = CSharpDbClient.Create(new CSharpDbClientOptions
        {
            Transport = ClientTransport.Direct,
            DataSource = ":memory:",
            DirectDatabaseOptions = CreateOptions("remote_delegate"),
        });
        var supportedSession = new RemoteDatabaseSession(remoteClient);
        await using (CSharpDbConnection supported = CreateOpenedConnection(supportedSession))
        {
            var directCapability = (ICSharpDbObservabilityClient)remoteClient;
            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> expected =
                await directCapability.GetRuntimeDiagnosticsAsync(Ct);
            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> actual =
                await ((ICSharpDbObservabilityClient)supported)
                    .GetRuntimeDiagnosticsAsync(Ct);
            Assert.Equal(expected.Metadata.ServerInstanceId, actual.Metadata.ServerInstanceId);
            Assert.Equal(expected.Metadata.CounterEpoch, actual.Metadata.CounterEpoch);
            Assert.Equal(expected.Metadata.Source, actual.Metadata.Source);
        }

        ICSharpDbClient plainClient =
            DispatchProxy.Create<ICSharpDbClient, PlainClientProxy>();
        var unsupportedSession = new RemoteDatabaseSession(plainClient);
        await using CSharpDbConnection unsupported =
            CreateOpenedConnection(unsupportedSession);
        CSharpDbObservabilityNotSupportedException exception =
            await Assert.ThrowsAsync<CSharpDbObservabilityNotSupportedException>(
                () => ((ICSharpDbObservabilityClient)unsupported)
                    .GetRuntimeDiagnosticsAsync(Ct));
        Assert.Equal(CSharpDbObservabilityNotSupportedException.SafeMessage, exception.Message);
    }

    [Fact]
    public async Task ValidationAndCloseRace_HaveNoOpenSideEffectsOrDisposedLeaks()
    {
        string unopenedPath = CreatePath("capability-unopened");
        var unopened = new CSharpDbConnection(
            $"Data Source={unopenedPath};Pooling=false",
            CreateOptions("unopened_capability"));
        var unopenedDiagnostics = (ICSharpDbObservabilityClient)unopened;
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => unopenedDiagnostics.GetActiveQueriesAsync(0, Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => unopenedDiagnostics.GetSessionsAsync(
                CSharpDbObservabilityOptions.MaximumHistoryCapacity + 1,
                Ct));
        using (var canceled = new CancellationTokenSource())
        {
            canceled.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => unopenedDiagnostics.GetRuntimeDiagnosticsAsync(canceled.Token));
        }
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => unopenedDiagnostics.GetRuntimeDiagnosticsAsync(Ct));
        Assert.False(File.Exists(unopenedPath));

        string racePath = CreatePath("capability-close-race");
        var connection = new CSharpDbConnection(
            $"Data Source={racePath};Pooling=false",
            CreateOptions("close_race_capability"));
        await connection.OpenAsync(Ct);
        var diagnostics = (ICSharpDbObservabilityClient)connection;
        Task<Exception?>[] captures = Enumerable.Range(0, 64)
            .Select(_ => Task.Run(async () =>
            {
                try
                {
                    await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
                    await diagnostics.GetSessionsAsync(2, Ct);
                    return null;
                }
                catch (Exception exception)
                {
                    return exception;
                }
            }, Ct))
            .ToArray();
        await Task.Yield();
        await connection.CloseAsync();
        Exception?[] observed = await Task.WhenAll(captures);
        Assert.DoesNotContain(observed, exception => exception is ObjectDisposedException);
        Assert.All(
            observed.Where(static exception => exception is not null),
            exception =>
            {
                Assert.IsType<InvalidOperationException>(exception);
                Assert.DoesNotContain(racePath, exception!.ToString(), StringComparison.OrdinalIgnoreCase);
            });
        await connection.DisposeAsync();
    }

    private string CreatePath(string marker)
    {
        string path = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb-{marker}-{Guid.NewGuid():N}.db");
        _paths.Add(path);
        return path;
    }

    private static CSharpDbConnection CreateOpenedConnection(ICSharpDbSession session)
    {
        var connection = new CSharpDbConnection();
        typeof(CSharpDbConnection).GetField(
                "_session",
                BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(connection, session);
        typeof(CSharpDbConnection).GetField(
                "_state",
                BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(connection, ConnectionState.Open);
        return connection;
    }

    private static DatabaseOptions CreateOptions(
        string alias,
        SqlTextCaptureMode sqlTextCaptureMode = SqlTextCaptureMode.None)
        => new()
        {
            ObservabilityOptions = CreateObservability(alias, sqlTextCaptureMode),
        };

    private static CSharpDbObservabilityOptions CreateObservability(
        string alias,
        SqlTextCaptureMode sqlTextCaptureMode = SqlTextCaptureMode.None)
        => new()
        {
            Enabled = true,
            DatabaseAlias = alias,
            Logging = new CSharpDbLoggingOptions
            {
                Enabled = false,
                Queries = true,
                SqlText = sqlTextCaptureMode,
            },
            History = new CSharpDbHistoryOptions
            {
                ActiveQueryCapacity = 32,
                RecentQueryCapacity = 32,
                RecentOperationCapacity = 16,
                Retention = TimeSpan.FromMinutes(5),
            },
        };

    private static async Task<object?> ExecuteScalarAsync(
        CSharpDbConnection connection,
        string sql)
    {
        await using CSharpDbCommand command =
            (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync(Ct);
    }

    private static async Task<int> ExecuteNonQueryAsync(
        CSharpDbConnection connection,
        string sql)
    {
        await using CSharpDbCommand command =
            (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteNonQueryAsync(Ct);
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    public class PlainClientProxy : DispatchProxy
    {
        protected override object? Invoke(
            MethodInfo? targetMethod,
            object?[]? args)
        {
            if (targetMethod?.Name == nameof(IAsyncDisposable.DisposeAsync))
                return ValueTask.CompletedTask;

            throw new NotSupportedException();
        }
    }
}
