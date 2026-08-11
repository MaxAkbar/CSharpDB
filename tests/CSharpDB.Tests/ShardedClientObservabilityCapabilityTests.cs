using System.Reflection;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Observability;
using CSharpDB.Engine;

namespace CSharpDB.Tests;

public sealed class ShardedClientObservabilityCapabilityTests
{
    private static readonly CancellationToken Ct = TestContext.Current.CancellationToken;

    [Fact]
    public async Task RuntimeAggregate_PreservesExactShardIdentityWithoutSummingPhysicalCounters()
    {
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> bravo =
            RuntimeTopology("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", counterEpoch: 7, requestCount: 20);
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> alpha =
            RuntimeTopology("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", counterEpoch: 3, requestCount: 10);
        (IShardTestClient alphaClient, RecordingProxy alphaRecording) =
            CreateObservedClient();
        (IShardTestClient bravoClient, RecordingProxy bravoRecording) =
            CreateObservedClient();
        alphaRecording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
            Task.FromResult(alpha));
        bravoRecording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
            Task.FromResult(bravo));

        await using var client = new CSharpDbShardedClient(
            CreateOptions("bravo", "alpha"),
            new Dictionary<string, ICSharpDbClient>(StringComparer.OrdinalIgnoreCase)
            {
                ["bravo"] = bravoClient,
                ["alpha"] = alphaClient,
            });

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> result =
            await client.GetRuntimeDiagnosticsAsync(Ct);

        Assert.Equal(DiagnosticsScope.Aggregate, result.Metadata.Scope);
        Assert.Equal(DiagnosticsSource.Client, result.Metadata.Source);
        Assert.Equal(DiagnosticsAvailability.Available, result.Aggregate.Queries.Availability);
        QueryDiagnosticsSummary coordinator = Assert.IsType<QueryDiagnosticsSummary>(
            result.Aggregate.Queries.Value);
        Assert.Equal(0, coordinator.RequestCount);
        Assert.Equal(0, coordinator.StatementExecutionCount);
        Assert.Equal(["alpha", "bravo"], result.Shards!.Select(static item => item.ShardAlias));
        Assert.Equal(
            ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"],
            result.Shards!.Select(static item => item.Value!.Metadata.ServerInstanceId));
        Assert.Equal([3L, 7L], result.Shards!.Select(static item => item.Value!.Metadata.CounterEpoch));
        Assert.All(result.Shards!, static item =>
        {
            Assert.Equal(DiagnosticsAvailability.Available, item.Availability);
            Assert.Equal(DiagnosticsScope.Shard, item.Value!.Metadata.Scope);
            Assert.Equal(item.ShardAlias, item.Value.Metadata.DatabaseAlias);
        });
        Assert.Single(alphaRecording.Invocations, static item =>
            item.MethodName == nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync));
        Assert.Single(bravoRecording.Invocations, static item =>
            item.MethodName == nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync));
    }

    [Fact]
    public async Task DisabledCoordinatorState_IsLazyStableAndDoesNotCreateRuntimeComponents()
    {
        CSharpDbShardingOptions options = CreateOptions("alpha");
        options.DirectDatabaseOptions!.ObservabilityOptions!.Enabled = false;
        await using var client = new CSharpDbShardedClient(
            options,
            new Dictionary<string, ICSharpDbClient>());
        FieldInfo stateField = Assert.IsAssignableFrom<FieldInfo>(
            typeof(CSharpDbShardedClient).GetField(
                "_coordinatorRuntimeState",
                BindingFlags.Instance | BindingFlags.NonPublic));

        Assert.Null(stateField.GetValue(client));
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> first =
            await client.GetRuntimeDiagnosticsAsync(Ct);
        CSharpDbRuntimeDiagnosticsState state = Assert.IsType<CSharpDbRuntimeDiagnosticsState>(
            stateField.GetValue(client));
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> second =
            await client.GetRuntimeDiagnosticsAsync(Ct);

        Assert.Equal(DiagnosticsAvailability.Disabled, first.Metadata.Availability);
        Assert.Equal(first.Metadata.ServerInstanceId, second.Metadata.ServerInstanceId);
        Assert.Equal(first.Metadata.CounterEpoch, second.Metadata.CounterEpoch);
        FieldInfo componentsField = Assert.IsAssignableFrom<FieldInfo>(
            typeof(CSharpDbRuntimeDiagnosticsState).GetField(
                "_components",
                BindingFlags.Instance | BindingFlags.NonPublic));
        Assert.Null(componentsField.GetValue(state));
    }

    [Fact]
    public async Task DirectShardChildren_RoundTripDistinctPhysicalIdentities()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_sharded_diagnostics_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            CSharpDbShardingOptions options = CreateOptions("alpha", "bravo");
            options.Shards[0].DataSource = Path.Combine(directory, "alpha.db");
            options.Shards[1].DataSource = Path.Combine(directory, "bravo.db");
            await using CSharpDbShardedClient client =
                await CSharpDbShardedClient.CreateAsync(options, ct: Ct);

            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> result =
                await client.GetRuntimeDiagnosticsAsync(Ct);

            Assert.Equal(DiagnosticsScope.Aggregate, result.Metadata.Scope);
            ShardDiagnosticsSection<RuntimeDiagnosticsSnapshot>[] shards =
                result.Shards!.ToArray();
            Assert.Equal(2, shards.Length);
            Assert.All(shards, static shard =>
            {
                Assert.Equal(DiagnosticsAvailability.Available, shard.Availability);
                Assert.Equal(DiagnosticsScope.Shard, shard.Value!.Metadata.Scope);
                Assert.Equal(shard.ShardAlias, shard.Value.Metadata.DatabaseAlias);
                Assert.Equal(DiagnosticsAvailability.Available, shard.Value.Metadata.Availability);
            });
            Assert.Equal(
                2,
                shards.Select(static shard => shard.Value!.Metadata.ServerInstanceId)
                    .Distinct(StringComparer.Ordinal)
                    .Count());
            Assert.DoesNotContain(
                shards,
                shard => shard.Value!.Metadata.ServerInstanceId == result.Metadata.ServerInstanceId);
        }
        finally
        {
            try
            {
                Directory.Delete(directory, recursive: true);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // Ignore transient cleanup locks on Windows test hosts.
            }
        }
    }

    [Fact]
    public async Task MixedChildren_ProjectExplicitAvailabilityWithoutLeakingFailureText()
    {
        const string sensitiveFailure =
            "SELECT secret FROM C:\\private\\customer.db at https://internal.invalid";
        (IShardTestClient available, RecordingProxy availableRecording) = CreateObservedClient();
        availableRecording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
            Task.FromResult(RuntimeTopology(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                counterEpoch: 0,
                requestCount: 1)));
        ICSharpDbClient unsupported = CreateLegacyClient();
        (IShardTestClient failing, RecordingProxy failingRecording) = CreateObservedClient();
        failingRecording.SetSynchronousException(
            nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
            new InvalidOperationException(sensitiveFailure));

        await using var client = new CSharpDbShardedClient(
            CreateOptions("available", "unsupported", "failed"),
            new Dictionary<string, ICSharpDbClient>(StringComparer.OrdinalIgnoreCase)
            {
                ["available"] = available,
                ["unsupported"] = unsupported,
                ["failed"] = failing,
            });

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> result =
            await client.GetRuntimeDiagnosticsAsync(Ct);

        Assert.Equal(
            DiagnosticsAvailability.Available,
            Assert.Single(result.Shards!, static item => item.ShardAlias == "available").Availability);
        Assert.Equal(
            DiagnosticsAvailability.Unsupported,
            Assert.Single(result.Shards!, static item => item.ShardAlias == "unsupported").Availability);
        Assert.Equal(
            DiagnosticsAvailability.Unavailable,
            Assert.Single(result.Shards!, static item => item.ShardAlias == "failed").Availability);
        string json = JsonSerializer.Serialize(
            result,
            CSharpDbObservabilityJsonContext.Default
                .DiagnosticsTopologySnapshotRuntimeDiagnosticsSnapshot);
        Assert.DoesNotContain(sensitiveFailure, json, StringComparison.Ordinal);
        Assert.DoesNotContain("customer.db", json, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ActiveCollections_KeepCoordinatorLogicalAndShardPhysicalRecordsSeparate()
    {
        DateTimeOffset started = new(2026, 8, 10, 12, 0, 0, TimeSpan.Zero);
        (IShardTestClient alpha, RecordingProxy alphaRecording) = CreateObservedClient();
        (IShardTestClient bravo, RecordingProxy bravoRecording) = CreateObservedClient();
        alphaRecording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetActiveQueriesAsync),
            Task.FromResult(ActiveTopology(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "11111111111111111111111111111111",
                started,
                long.MaxValue)));
        bravoRecording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetActiveQueriesAsync),
            Task.FromResult(ActiveTopology(
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "22222222222222222222222222222222",
                started,
                1)));

        await using var client = new CSharpDbShardedClient(
            CreateOptions("bravo", "alpha"),
            new Dictionary<string, ICSharpDbClient>(StringComparer.OrdinalIgnoreCase)
            {
                ["bravo"] = bravo,
                ["alpha"] = alpha,
            });

        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>> result =
            await client.GetActiveQueriesAsync(2, Ct);

        Assert.Empty(result.Aggregate.Records!);
        Assert.Equal(0, result.Aggregate.DroppedCount);
        Assert.False(result.Aggregate.IsTruncated);
        Assert.False(result.Metadata.RecordsTruncated);
        Assert.Equal(["alpha", "bravo"], result.Shards!.Select(static item => item.ShardAlias));
        ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>> alphaSection =
            result.Shards![0];
        ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>> bravoSection =
            result.Shards[1];
        Assert.Equal(long.MaxValue, alphaSection.Value!.DroppedCount);
        Assert.Equal(1, bravoSection.Value!.DroppedCount);
        Assert.Equal(
            "11111111111111111111111111111111",
            Assert.Single(alphaSection.Value.Records!).OperationId.Value);
        Assert.Equal(
            "22222222222222222222222222222222",
            Assert.Single(bravoSection.Value.Records!).OperationId.Value);
        Assert.All(result.Shards, static item =>
            Assert.Equal(DiagnosticsScope.Shard, item.Value!.Metadata.Scope));
        Assert.Equal(
            1,
            Assert.IsType<int>(Assert.Single(
                alphaRecording.Invocations,
                static item => item.MethodName == nameof(ICSharpDbObservabilityClient.GetActiveQueriesAsync))
                .Arguments[0]));
        Assert.Equal(
            1,
            Assert.IsType<int>(Assert.Single(
                bravoRecording.Invocations,
                static item => item.MethodName == nameof(ICSharpDbObservabilityClient.GetActiveQueriesAsync))
                .Arguments[0]));
    }

    [Fact]
    public async Task ActiveShardPayloads_UseDeterministicGlobalRecordBudget()
    {
        string[] shardIds = ["delta", "charlie", "bravo", "alpha"];
        var children = new Dictionary<string, ICSharpDbClient>(StringComparer.OrdinalIgnoreCase);
        var recordings = new List<RecordingProxy>();
        for (int index = 0; index < shardIds.Length; index++)
        {
            (IShardTestClient child, RecordingProxy recording) = CreateObservedClient();
            recording.SetResult(
                nameof(ICSharpDbObservabilityClient.GetActiveQueriesAsync),
                Task.FromResult(ActiveTopologyWithRecordCount(
                    $"{index + 1:x32}",
                    recordCount: 30)));
            children.Add(shardIds[index], child);
            recordings.Add(recording);
        }

        await using var client = new CSharpDbShardedClient(
            CreateOptions(shardIds),
            children);

        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>> result =
            await client.GetActiveQueriesAsync(100, Ct);

        Assert.Empty(result.Aggregate.Records!);
        Assert.Equal(100, result.Shards!.Sum(static shard => shard.Value!.Records!.Count));
        Assert.All(result.Shards!, static shard =>
        {
            Assert.Equal(25, shard.Value!.Records!.Count);
            Assert.True(shard.Value.IsTruncated);
            Assert.True(shard.Value.Metadata.RecordsTruncated);
        });
        Assert.All(recordings, static recording =>
        {
            Invocation invocation = Assert.Single(
                recording.Invocations,
                static item => item.MethodName == nameof(ICSharpDbObservabilityClient.GetActiveQueriesAsync));
            Assert.Equal(25, Assert.IsType<int>(invocation.Arguments[0]));
        });

        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>> tiny =
            await client.GetActiveQueriesAsync(1, Ct);

        Assert.Empty(tiny.Aggregate.Records!);
        IReadOnlyList<ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>
            tinyShards = Assert.IsAssignableFrom<IReadOnlyList<
                ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>>(
                    tiny.Shards);
        Assert.Equal(1, tinyShards.Sum(static shard => shard.Value!.Records!.Count));
        Assert.Single(tinyShards[0].Value!.Records!);
        Assert.All(tinyShards.Skip(1), static shard => Assert.Empty(shard.Value!.Records!));
        Assert.All(recordings, static recording =>
        {
            Invocation invocation = recording.Invocations
                .Where(static item => item.MethodName == nameof(ICSharpDbObservabilityClient.GetActiveQueriesAsync))
                .Last();
            Assert.Equal(1, Assert.IsType<int>(invocation.Arguments[0]));
        });
    }

    [Fact]
    public async Task FanOutExecution_RecordsOneLogicalCoordinatorQueryWithoutSummingShardCounters()
    {
        const string sql = "SELECT 42";
        (IShardTestClient child, RecordingProxy recording) = CreateObservedClient();
        var executionStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var executionCompletion = new TaskCompletionSource<SqlExecutionResult>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        recording.SetHandler(
            nameof(ICSharpDbClient.ExecuteSqlAsync),
            _ =>
            {
                executionStarted.TrySetResult();
                return executionCompletion.Task;
            });
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetActiveQueriesAsync),
            Task.FromResult(ActiveTopology(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "22222222222222222222222222222222",
                new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero),
                droppedCount: 0)));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRecentQueriesAsync),
            Task.FromResult(EmptyCollectionTopology<RecentQuerySnapshot>(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
            Task.FromResult(RuntimeTopology(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                counterEpoch: 11,
                requestCount: 99)));

        await using var client = new CSharpDbShardedClient(
            CreateOptions("alpha"),
            new Dictionary<string, ICSharpDbClient> { ["alpha"] = child });

        Task<IReadOnlyList<CSharpDbShardSqlExecutionResult>> pending =
            client.ExecuteSqlOnAllShardsAsync(sql, Ct);
        await executionStarted.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>> active =
            await client.GetActiveQueriesAsync(1, Ct);
        ActiveQuerySnapshot coordinatorActive = Assert.Single(active.Aggregate.Records!);
        Assert.Equal(CSharpDbOperationRole.Root, coordinatorActive.Role);
        Assert.Equal(CSharpDB.Observability.CSharpDbTransport.Sharded, coordinatorActive.Transport);
        Assert.Equal(DiagnosticsScope.Aggregate, coordinatorActive.Metadata.Scope);
        Assert.Empty(Assert.Single(active.Shards!).Value!.Records!);

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> whileActive =
            await client.GetRuntimeDiagnosticsAsync(Ct);
        QueryDiagnosticsSummary whileActiveCoordinator = Assert.IsType<QueryDiagnosticsSummary>(
            whileActive.Aggregate.Queries.Value);
        Assert.Equal(0, whileActiveCoordinator.RequestCount);
        Assert.Equal(1, whileActiveCoordinator.ActiveCount);
        Assert.Equal(
            99,
            Assert.IsType<QueryDiagnosticsSummary>(
                Assert.Single(whileActive.Shards!).Value!.Queries.Value).RequestCount);

        executionCompletion.SetResult(new SqlExecutionResult
        {
            IsQuery = true,
            Rows = [[42]],
        });
        await pending;

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> completed =
            await client.GetRuntimeDiagnosticsAsync(Ct);
        QueryDiagnosticsSummary coordinator = Assert.IsType<QueryDiagnosticsSummary>(
            completed.Aggregate.Queries.Value);
        Assert.Equal(1, coordinator.RequestCount);
        Assert.Equal(1, coordinator.StatementExecutionCount);
        Assert.Equal(1, coordinator.SucceededCount);
        Assert.Equal(1, coordinator.RowsProduced);
        Assert.Equal(0, coordinator.ActiveCount);
        Assert.Equal(
            99,
            Assert.IsType<QueryDiagnosticsSummary>(
                Assert.Single(completed.Shards!).Value!.Queries.Value).RequestCount);

        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>> recent =
            await client.GetRecentQueriesAsync(10, Ct);
        RecentQuerySnapshot coordinatorRecent = Assert.Single(recent.Aggregate.Records!);
        Assert.Equal(coordinatorActive.OperationId, coordinatorRecent.OperationId);
        Assert.Empty(Assert.Single(recent.Shards!).Value!.Records!);
    }

    [Fact]
    public async Task CoordinatorRecentCap_UsesTruncationWithoutInflatingCumulativeDropCount()
    {
        (IShardTestClient child, RecordingProxy recording) = CreateObservedClient();
        recording.SetResult(
            nameof(ICSharpDbClient.ExecuteSqlAsync),
            Task.FromResult(new SqlExecutionResult
            {
                IsQuery = true,
                Rows = [],
            }));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRecentQueriesAsync),
            Task.FromResult(EmptyCollectionTopology<RecentQuerySnapshot>(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")));

        await using var client = new CSharpDbShardedClient(
            CreateOptions("alpha"),
            new Dictionary<string, ICSharpDbClient> { ["alpha"] = child });

        await client.ExecuteSqlOnAllShardsAsync("SELECT 1", Ct);
        await client.ExecuteSqlOnAllShardsAsync("SELECT 2", Ct);

        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>> result =
            await client.GetRecentQueriesAsync(1, Ct);

        Assert.Single(result.Aggregate.Records!);
        Assert.True(result.Aggregate.IsTruncated);
        Assert.True(result.Aggregate.Metadata.RecordsTruncated);
        Assert.Equal(0, result.Aggregate.DroppedCount);
    }

    [Fact]
    public async Task CoordinatorLedgerAndLogging_UseOneCustomClockContext()
    {
        DateTimeOffset startedAt = new(2032, 4, 5, 6, 7, 8, TimeSpan.Zero);
        var clock = new ManualTimeProvider(startedAt);
        (IShardTestClient child, RecordingProxy recording) = CreateObservedClient();
        var executionStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var executionCompletion = new TaskCompletionSource<SqlExecutionResult>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        recording.SetHandler(
            nameof(ICSharpDbClient.ExecuteSqlAsync),
            _ =>
            {
                executionStarted.TrySetResult();
                return executionCompletion.Task;
            });
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetActiveQueriesAsync),
            Task.FromResult(EmptyCollectionTopology<ActiveQuerySnapshot>(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRecentQueriesAsync),
            Task.FromResult(EmptyCollectionTopology<RecentQuerySnapshot>(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")));
        CSharpDbShardingOptions options = CreateOptions("alpha");
        options.DirectDatabaseOptions!.ObservabilityOptions!.Logging.Queries = true;
        options.DirectDatabaseOptions.ObservabilityOptions.Logging.SlowQueries = false;
        var observer = new QueryEventObserver();
        using IDisposable subscription = CSharpDbDiagnostics.DiagnosticListener.Subscribe(
            observer,
            static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal));
        await using var client = new CSharpDbShardedClient(
            options,
            new Dictionary<string, ICSharpDbClient> { ["alpha"] = child },
            clock);

        Task<IReadOnlyList<CSharpDbShardSqlExecutionResult>> pending =
            client.ExecuteSqlOnAllShardsAsync("SELECT 1", Ct);
        await executionStarted.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>> active =
            await client.GetActiveQueriesAsync(10, Ct);
        ActiveQuerySnapshot activeRoot = Assert.Single(active.Aggregate.Records!);
        Assert.Equal(startedAt, activeRoot.StartedAtUtc);
        Assert.Equal(startedAt, active.Metadata.CapturedAtUtc);

        clock.Advance(TimeSpan.FromSeconds(3));
        executionCompletion.SetResult(new SqlExecutionResult { IsQuery = true, Rows = [] });
        await pending;
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>> recent =
            await client.GetRecentQueriesAsync(10, Ct);
        RecentQuerySnapshot recentRoot = Assert.Single(recent.Aggregate.Records!);
        CSharpDbQueryTerminalEvent loggedRoot = Assert.Single(
            observer.TerminalEvents,
            static item => item.Context.Role == CSharpDbOperationRole.Root);

        Assert.Equal(activeRoot.OperationId, recentRoot.OperationId);
        Assert.Equal(activeRoot.OperationId, loggedRoot.Context.OperationId);
        Assert.Equal(startedAt.AddSeconds(3), recentRoot.CompletedAtUtc);
        Assert.Equal(TimeSpan.FromSeconds(3), recentRoot.Duration);
        Assert.Equal(recentRoot.CompletedAtUtc, loggedRoot.CompletedAtUtc);
        Assert.Equal(recentRoot.Duration, loggedRoot.TotalDuration);
        Assert.Equal(startedAt.AddSeconds(3), recent.Metadata.CapturedAtUtc);
    }

    [Fact]
    public async Task AmbientParentAndCoordinatorLedger_UseTheCoordinatorClockDomain()
    {
        DateTimeOffset coordinatorStartedAt = new(2033, 1, 2, 3, 4, 5, TimeSpan.Zero);
        var coordinatorClock = new ManualTimeProvider(coordinatorStartedAt);
        var parentClock = new ManualTimeProvider(
            new DateTimeOffset(2044, 5, 6, 7, 8, 9, TimeSpan.Zero));
        (IShardTestClient child, RecordingProxy recording) = CreateObservedClient();
        recording.SetHandler(
            nameof(ICSharpDbClient.ExecuteSqlAsync),
            _ =>
            {
                coordinatorClock.Advance(TimeSpan.FromSeconds(4));
                return Task.FromResult(new SqlExecutionResult { IsQuery = true, Rows = [] });
            });
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRecentQueriesAsync),
            Task.FromResult(EmptyCollectionTopology<RecentQuerySnapshot>(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")));
        await using var client = new CSharpDbShardedClient(
            CreateOptions("alpha"),
            new Dictionary<string, ICSharpDbClient> { ["alpha"] = child },
            coordinatorClock);
        CSharpDbOperationContext parent = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDB.Observability.CSharpDbTransport.Http,
            "host",
            timeProvider: parentClock);

        using (CSharpDbOperationScope.Enter(parent))
        {
            await client.ExecuteSqlOnAllShardsAsync("SELECT 1", Ct);
        }
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>> recent =
            await client.GetRecentQueriesAsync(10, Ct);
        RecentQuerySnapshot coordinator = Assert.Single(recent.Aggregate.Records!);

        Assert.Equal(parent.OperationId, coordinator.ParentOperationId);
        Assert.Equal(coordinatorStartedAt, coordinator.StartedAtUtc);
        Assert.Equal(coordinatorStartedAt.AddSeconds(4), coordinator.CompletedAtUtc);
        Assert.Equal(TimeSpan.FromSeconds(4), coordinator.Duration);
    }

    [Fact]
    public async Task DirectShardRuntimeHistory_IsIndependentOfQueryListenerInterest()
    {
        string directory = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_sharded_listener_parity_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        try
        {
            CSharpDbShardingOptions options = CreateOptions("alpha", "bravo");
            options.Shards[0].DataSource = Path.Combine(directory, "alpha.db");
            options.Shards[1].DataSource = Path.Combine(directory, "bravo.db");
            options.DirectDatabaseOptions!.ObservabilityOptions!.Logging.Queries = true;
            options.DirectDatabaseOptions.ObservabilityOptions.Logging.SlowQueries = false;
            await using CSharpDbShardedClient client =
                await CSharpDbShardedClient.CreateAsync(options, ct: Ct);

            await client.ExecuteSqlOnAllShardsAsync("SELECT 1", Ct);
            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>> before =
                await client.GetRecentQueriesAsync(100, Ct);
            var observer = new QueryEventObserver();
            using (CSharpDbDiagnostics.DiagnosticListener.Subscribe(
                       observer,
                       static name => name.StartsWith("CSharpDB.Query.", StringComparison.Ordinal)))
            {
                await client.ExecuteSqlOnAllShardsAsync("SELECT 2", Ct);
            }

            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>> after =
                await client.GetRecentQueriesAsync(100, Ct);

            Assert.Single(before.Aggregate.Records!);
            Assert.Equal(2, after.Aggregate.Records!.Count);
            Assert.All(before.Shards!, static shard => Assert.Single(shard.Value!.Records!));
            Assert.All(after.Shards!, static shard => Assert.Equal(2, shard.Value!.Records!.Count));
            Assert.Equal(3, observer.TerminalEvents.Count);
        }
        finally
        {
            try
            {
                Directory.Delete(directory, recursive: true);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // Ignore transient cleanup locks on Windows test hosts.
            }
        }
    }

    [Fact]
    public async Task TopologyCapsAndOrdersMoreThanMaximumConfiguredShards()
    {
        string[] shardIds = Enumerable.Range(0, 66)
            .Select(static index => $"s{index:D2}")
            .Reverse()
            .ToArray();
        await using var client = new CSharpDbShardedClient(
            CreateOptions(shardIds),
            new Dictionary<string, ICSharpDbClient>());

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> result =
            await client.GetRuntimeDiagnosticsAsync(Ct);

        Assert.Equal(CSharpDbDiagnostics.MaximumConfiguredDatabaseAliases, result.ShardCapacity);
        Assert.Equal(2, result.DroppedShardCount);
        Assert.True(result.ShardsTruncated);
        Assert.Equal(
            Enumerable.Range(0, 64).Select(static index => $"s{index:D2}"),
            result.Shards!.Select(static item => item.ShardAlias));
        Assert.All(result.Shards!, static item =>
            Assert.Equal(DiagnosticsAvailability.Unavailable, item.Availability));
    }

    [Fact]
    public async Task LongShardIds_RemainValidForRoutingWhenObservabilityIsDisabled()
    {
        string longShardId = $"tenant_{new string('x', 80)}";
        CSharpDbShardingOptions options = CreateOptions(longShardId);
        options.DirectDatabaseOptions!.ObservabilityOptions!.Enabled = false;
        ICSharpDbClient child = CreateLegacyClient();
        await using var client = new CSharpDbShardedClient(
            options,
            new Dictionary<string, ICSharpDbClient> { [longShardId] = child });

        Assert.Same(child, client.ForShardId(longShardId));
        Assert.Equal(longShardId, CSharpDbShardedClient.CreateShardMapSnapshot(options).Shards[0].ShardId);
    }

    [Fact]
    public async Task LongShardIds_UseDistinctBoundedOpaqueDiagnosticsAliases()
    {
        string sharedPrefix = new('p', 80);
        string firstShardId = $"{sharedPrefix}x";
        string secondShardId = $"{sharedPrefix}y";
        (IShardTestClient first, RecordingProxy firstRecording) = CreateObservedClient();
        (IShardTestClient second, RecordingProxy secondRecording) = CreateObservedClient();
        firstRecording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
            Task.FromResult(RuntimeTopology(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                counterEpoch: 1,
                requestCount: 1)));
        secondRecording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
            Task.FromResult(RuntimeTopology(
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                counterEpoch: 2,
                requestCount: 2)));
        await using var client = new CSharpDbShardedClient(
            CreateOptions(firstShardId, secondShardId),
            new Dictionary<string, ICSharpDbClient>
            {
                [firstShardId] = first,
                [secondShardId] = second,
            });

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> result =
            await client.GetRuntimeDiagnosticsAsync(Ct);
        ShardDiagnosticsSection<RuntimeDiagnosticsSnapshot>[] shards =
            result.Shards!.ToArray();

        Assert.Equal(2, shards.Length);
        Assert.All(shards, static shard =>
        {
            Assert.Equal(CSharpDbDiagnostics.MaximumDatabaseAliasLength, shard.ShardAlias.Length);
            Assert.True(CSharpDbObservabilityOptions.IsValidDatabaseAlias(shard.ShardAlias));
            Assert.Equal(shard.ShardAlias, shard.Value!.Metadata.DatabaseAlias);
        });
        Assert.Equal(2, shards.Select(static shard => shard.ShardAlias).Distinct().Count());
        Assert.Equal(
            ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"],
            shards.Select(static shard => shard.Value!.Metadata.ServerInstanceId));
        string json = JsonSerializer.Serialize(
            result,
            CSharpDbObservabilityJsonContext.Default
                .DiagnosticsTopologySnapshotRuntimeDiagnosticsSnapshot);
        Assert.DoesNotContain(firstShardId, json, StringComparison.Ordinal);
        Assert.DoesNotContain(secondShardId, json, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RouteBoundCapability_DelegatesToExactlyOneResolvedChild()
    {
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> alphaTopology =
            RuntimeTopology("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 0, 1);
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> bravoTopology =
            RuntimeTopology("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 0, 1);
        (IShardTestClient alpha, RecordingProxy alphaRecording) = CreateObservedClient();
        (IShardTestClient bravo, RecordingProxy bravoRecording) = CreateObservedClient();
        alphaRecording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
            Task.FromResult(alphaTopology));
        bravoRecording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
            Task.FromResult(bravoTopology));
        CSharpDbShardingOptions options = CreateOptions("alpha", "bravo");
        options.ExactKeyPins["tenant-b"] = "bravo";

        await using var client = new CSharpDbShardedClient(
            options,
            new Dictionary<string, ICSharpDbClient>(StringComparer.OrdinalIgnoreCase)
            {
                ["alpha"] = alpha,
                ["bravo"] = bravo,
            });
        var routed = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(
            client.ForRoute(new CSharpDbRouteContext
            {
                Keyspace = "observed",
                Key = "tenant-b",
            }));

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> result =
            await routed.GetRuntimeDiagnosticsAsync(Ct);

        Assert.Same(bravoTopology, result);
        Assert.DoesNotContain(
            alphaRecording.Invocations,
            static item => item.MethodName == nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync));
        Assert.Single(
            bravoRecording.Invocations,
            static item => item.MethodName == nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync));
    }

    [Fact]
    public async Task CancellationStopsBoundedFanOutAndPropagatesToCaller()
    {
        string[] shardIds = Enumerable.Range(0, 16)
            .Select(static index => $"s{index:D2}")
            .ToArray();
        var tracker = new BlockingCaptureTracker();
        var children = new Dictionary<string, ICSharpDbClient>(StringComparer.OrdinalIgnoreCase);
        foreach (string shardId in shardIds)
        {
            (IShardTestClient child, RecordingProxy recording) = CreateObservedClient();
            recording.SetHandler(
                nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
                args => tracker.CaptureAsync((CancellationToken)args[0]!));
            children.Add(shardId, child);
        }
        await using var client = new CSharpDbShardedClient(CreateOptions(shardIds), children);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);

        Task<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>> pending =
            client.GetRuntimeDiagnosticsAsync(cts.Token);
        await tracker.AtConcurrencyLimit.Task.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        Assert.Equal(8, tracker.MaximumConcurrency);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        Assert.InRange(tracker.MaximumConcurrency, 1, 8);
    }

    [Fact]
    public async Task QueryDetail_IsOnlyMaterializedByTheSeparateDetailCall()
    {
        const string capturedSql = "SELECT 'detail-only-secret-42'";
        var operationId = new OpaqueDiagnosticsId("11111111111111111111111111111111");
        (IShardTestClient child, RecordingProxy recording) = CreateObservedClient();
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
            Task.FromResult(RuntimeTopology(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                counterEpoch: 0,
                requestCount: 1)));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetQueryDetailAsync),
            Task.FromResult(QueryDetailTopology(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                operationId,
                capturedSql)));

        await using var client = new CSharpDbShardedClient(
            CreateOptions("alpha"),
            new Dictionary<string, ICSharpDbClient> { ["alpha"] = child });

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> runtime =
            await client.GetRuntimeDiagnosticsAsync(Ct);
        string runtimeJson = JsonSerializer.Serialize(
            runtime,
            CSharpDbObservabilityJsonContext.Default
                .DiagnosticsTopologySnapshotRuntimeDiagnosticsSnapshot);
        Assert.DoesNotContain(capturedSql, runtimeJson, StringComparison.Ordinal);
        Assert.DoesNotContain(
            recording.Invocations,
            static item => item.MethodName == nameof(ICSharpDbObservabilityClient.GetQueryDetailAsync));

        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>> detail =
            await client.GetQueryDetailAsync(operationId, Ct);

        Assert.Equal(DiagnosticsAvailability.Unavailable, detail.Aggregate.Metadata.Availability);
        Assert.Null(detail.Aggregate.Value);
        Assert.Equal(
            capturedSql,
            Assert.Single(detail.Shards!).Value!.Value!.CapturedSqlText);
        Assert.Single(
            recording.Invocations,
            static item => item.MethodName == nameof(ICSharpDbObservabilityClient.GetQueryDetailAsync));
    }

    [Fact]
    public async Task CoordinatorQueryDetail_IsRetainedOnlyWhenExplicitRawCaptureIsEnabled()
    {
        const string capturedSql = "SELECT 'coordinator-detail-secret-42'";
        (IShardTestClient child, RecordingProxy recording) = CreateObservedClient();
        recording.SetResult(
            nameof(ICSharpDbClient.ExecuteSqlAsync),
            Task.FromResult(new SqlExecutionResult
            {
                IsQuery = true,
                Rows = [],
            }));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRecentQueriesAsync),
            Task.FromResult(EmptyCollectionTopology<RecentQuerySnapshot>(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetQueryDetailAsync),
            Task.FromResult(UnavailableQueryDetailTopology(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")));
        CSharpDbShardingOptions options = CreateOptions("alpha");
        options.DirectDatabaseOptions!.ObservabilityOptions!.Logging.SqlText =
            SqlTextCaptureMode.Raw;

        await using var client = new CSharpDbShardedClient(
            options,
            new Dictionary<string, ICSharpDbClient> { ["alpha"] = child });

        await client.ExecuteSqlOnAllShardsAsync(capturedSql, Ct);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>> recent =
            await client.GetRecentQueriesAsync(10, Ct);
        string recentJson = JsonSerializer.Serialize(
            recent,
            CSharpDbObservabilityJsonContext.Default
                .DiagnosticsTopologySnapshotDiagnosticsCollectionSnapshotRecentQuerySnapshot);
        Assert.DoesNotContain(capturedSql, recentJson, StringComparison.Ordinal);
        OpaqueDiagnosticsId operationId = Assert.Single(recent.Aggregate.Records!).OperationId;

        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>> detail =
            await client.GetQueryDetailAsync(operationId, Ct);

        Assert.Equal(DiagnosticsAvailability.Available, detail.Aggregate.Metadata.Availability);
        Assert.Equal(capturedSql, detail.Aggregate.Value!.CapturedSqlText);
        Assert.Equal(DiagnosticsScope.Aggregate, detail.Aggregate.Value.Metadata.Scope);
        Assert.Equal(
            DiagnosticsAvailability.Unavailable,
            Assert.Single(detail.Shards!).Value!.Metadata.Availability);
        Assert.Single(
            recording.Invocations,
            static item => item.MethodName == nameof(ICSharpDbObservabilityClient.GetQueryDetailAsync));
    }

    [Fact]
    public async Task DedicatedStorageAndWal_DeepProjectExactShardMetadata()
    {
        const string serverId = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        (IShardTestClient child, RecordingProxy recording) = CreateObservedClient();
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetStorageDiagnosticsAsync),
            Task.FromResult(StorageTopology(serverId)));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetWalDiagnosticsAsync),
            Task.FromResult(WalTopology(serverId)));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
            Task.FromResult(RuntimeTopologyWithWalDetails(serverId)));

        await using var client = new CSharpDbShardedClient(
            CreateOptions("alpha"),
            new Dictionary<string, ICSharpDbClient> { ["alpha"] = child });

        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<
            StorageRuntimeDiagnosticsSnapshot>> storage =
                await client.GetStorageDiagnosticsAsync(Ct);
        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<
            WalRuntimeDiagnosticsSnapshot>> wal =
                await client.GetWalDiagnosticsAsync(Ct);
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> runtime =
            await client.GetRuntimeDiagnosticsAsync(Ct);

        Assert.Equal(
            DiagnosticsAvailability.Unavailable,
            storage.Aggregate.Metadata.Availability);
        StorageRuntimeDiagnosticsSnapshot storageValue = Assert.IsType<
            StorageRuntimeDiagnosticsSnapshot>(Assert.Single(storage.Shards!).Value!.Value);
        Assert.Equal(DiagnosticsScope.Shard, storageValue.Metadata.Scope);
        Assert.Equal("alpha", storageValue.Metadata.DatabaseAlias);
        Assert.Equal(storageValue.Metadata, storageValue.Cache.Value!.Metadata);
        Assert.Equal(
            storageValue.Metadata,
            storageValue.PhysicalIo.Value!.Metadata);

        StorageRuntimeDiagnosticsSnapshot summaryStorage = Assert.IsType<
            StorageRuntimeDiagnosticsSnapshot>(Assert.Single(runtime.Shards!)
                .Value!.Storage.Value);
        Assert.Equal(summaryStorage.Metadata, summaryStorage.Cache.Value!.Metadata);
        Assert.Equal(
            summaryStorage.Metadata,
            summaryStorage.PhysicalIo.Value!.Metadata);

        WalRuntimeDiagnosticsSnapshot dedicatedWal = Assert.IsType<
            WalRuntimeDiagnosticsSnapshot>(Assert.Single(wal.Shards!).Value!.Value);
        AssertProjectedWalMetadata(dedicatedWal, "alpha");
        WalRuntimeDiagnosticsSnapshot summaryWal = Assert.IsType<
            WalRuntimeDiagnosticsSnapshot>(Assert.Single(runtime.Shards!).Value!.Wal.Value);
        AssertProjectedWalMetadata(summaryWal, "alpha");
    }

    [Fact]
    public void DetailProjectionFailure_DegradesOnlyThatDetailToUnavailable()
    {
        DiagnosticsSnapshotMetadata metadata = Metadata(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            counterEpoch: 1,
            DiagnosticsAvailability.Available);
        DiagnosticsSection<StorageCacheDiagnosticsSnapshot> section =
            DiagnosticsSection<StorageCacheDiagnosticsSnapshot>.Available(
                new StorageCacheDiagnosticsSnapshot(
                    metadata,
                    sharedResidentPages: 1,
                    sharedCapacityPages: null,
                    walResidentPages: 0,
                    walCapacityPages: 0));
        MethodInfo method = typeof(CSharpDbShardedClient).GetMethod(
            "ProjectDetailSection",
            BindingFlags.Static | BindingFlags.NonPublic)!
            .MakeGenericMethod(typeof(StorageCacheDiagnosticsSnapshot));
        Func<StorageCacheDiagnosticsSnapshot, DiagnosticsSnapshotMetadata,
            StorageCacheDiagnosticsSnapshot> failingProjector =
                static (_, _) => throw new InvalidOperationException(
                    "Synthetic nested projection failure.");

        var projected = Assert.IsType<
            DiagnosticsSection<StorageCacheDiagnosticsSnapshot>>(
                method.Invoke(
                    null,
                    [section, metadata, failingProjector]));

        Assert.Equal(DiagnosticsAvailability.Unavailable, projected.Availability);
        Assert.Null(projected.Value);
    }

    [Fact]
    public async Task MaintenanceCollections_SplitOneGlobalBudgetAcrossShards()
    {
        (IShardTestClient alpha, RecordingProxy alphaRecording) =
            CreateObservedClient();
        (IShardTestClient bravo, RecordingProxy bravoRecording) =
            CreateObservedClient();
        foreach (RecordingProxy recording in new[] { alphaRecording, bravoRecording })
        {
            recording.SetResult(
                nameof(ICSharpDbObservabilityClient.GetActiveMaintenanceOperationsAsync),
                Task.FromResult(MaintenanceTopology(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    recordCount: 2,
                    terminal: false)));
            recording.SetResult(
                nameof(ICSharpDbObservabilityClient.GetRecentMaintenanceOperationsAsync),
                Task.FromResult(MaintenanceTopology(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    recordCount: 2,
                    terminal: true)));
        }

        CSharpDbShardingOptions options = CreateOptions("bravo", "alpha");
        int configuredCapacity = options.DirectDatabaseOptions!
            .ObservabilityOptions!.History.RecentOperationCapacity;
        TimeSpan configuredRetention = options.DirectDatabaseOptions
            .ObservabilityOptions.History.Retention;
        Assert.NotEqual(1, configuredCapacity);
        await using var client = new CSharpDbShardedClient(
            options,
            new Dictionary<string, ICSharpDbClient>(StringComparer.OrdinalIgnoreCase)
            {
                ["alpha"] = alpha,
                ["bravo"] = bravo,
            });

        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
            MaintenanceOperationSnapshot>> active =
                await client.GetActiveMaintenanceOperationsAsync(1, Ct);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
            MaintenanceOperationSnapshot>> recent =
                await client.GetRecentMaintenanceOperationsAsync(2, Ct);

        Assert.Empty(active.Aggregate.Records!);
        Assert.Equal(configuredCapacity, active.Aggregate.Capacity);
        Assert.Null(active.Aggregate.Retention);
        Assert.Equal(configuredCapacity, recent.Aggregate.Capacity);
        Assert.Equal(configuredRetention, recent.Aggregate.Retention);
        Assert.Equal(
            1,
            active.Shards!.Sum(static shard => shard.Value!.Records!.Count));
        Assert.Equal(
            2,
            recent.Shards!.Sum(static shard => shard.Value!.Records!.Count));
        Assert.All(
            new[] { alphaRecording, bravoRecording },
            static recording =>
            {
                Assert.Equal(
                    1,
                    Assert.IsType<int>(Assert.Single(
                        recording.Invocations,
                        static item => item.MethodName == nameof(
                            ICSharpDbObservabilityClient.GetActiveMaintenanceOperationsAsync))
                        .Arguments[0]));
                Assert.Equal(
                    1,
                    Assert.IsType<int>(Assert.Single(
                        recording.Invocations,
                        static item => item.MethodName == nameof(
                            ICSharpDbObservabilityClient.GetRecentMaintenanceOperationsAsync))
                        .Arguments[0]));
            });
    }

    [Fact]
    public async Task NewShardCapabilityUnsupported_IsProjectedWithoutFailureText()
    {
        (IShardTestClient child, RecordingProxy recording) = CreateObservedClient();
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetStorageDiagnosticsAsync),
            Task.FromException<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<
                StorageRuntimeDiagnosticsSnapshot>>>(
                    new CSharpDbObservabilityNotSupportedException()));
        await using var client = new CSharpDbShardedClient(
            CreateOptions("alpha"),
            new Dictionary<string, ICSharpDbClient> { ["alpha"] = child });

        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<
            StorageRuntimeDiagnosticsSnapshot>> result =
                await client.GetStorageDiagnosticsAsync(Ct);

        Assert.Equal(
            DiagnosticsAvailability.Unsupported,
            Assert.Single(result.Shards!).Availability);
    }

    private static CSharpDbShardingOptions CreateOptions(params string[] shardIds)
        => new()
        {
            Keyspace = "observed",
            MapVersion = 1,
            VirtualBucketCount = 1,
            Shards = shardIds.Select(static shardId => new CSharpDbShardDefinition
            {
                ShardId = shardId,
                DataSource = $"{shardId}.db",
            }).ToArray(),
            BucketRanges =
            [
                new CSharpDbShardBucketRange
                {
                    StartBucketInclusive = 0,
                    EndBucketExclusive = 1,
                    ShardId = shardIds[0],
                },
            ],
            ExactKeyPins = new Dictionary<string, string>(StringComparer.Ordinal),
            DirectDatabaseOptions = new DatabaseOptions
            {
                ObservabilityOptions = new CSharpDbObservabilityOptions
                {
                    Enabled = true,
                    DatabaseAlias = "coordinator",
                },
            },
        };

    private static DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> RuntimeTopology(
        string serverInstanceId,
        long counterEpoch,
        long requestCount)
    {
        DiagnosticsSnapshotMetadata metadata = Metadata(
            serverInstanceId,
            counterEpoch,
            DiagnosticsAvailability.Available);
        var summary = new QueryDiagnosticsSummary(
            metadata,
            RequestCount: requestCount,
            StatementExecutionCount: requestCount,
            SucceededCount: requestCount,
            FailedCount: 0,
            CanceledCount: 0,
            SlowCount: 0,
            RowsProduced: 0,
            RowsAffected: 0,
            ActiveCount: 0);
        var snapshot = new RuntimeDiagnosticsSnapshot(
            metadata,
            DiagnosticsSection<QueryDiagnosticsSummary>.Available(summary),
            DiagnosticsSection<ConnectionDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<MaintenanceOperationSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<HealthDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable));
        return InstanceTopology(snapshot);
    }

    private static DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>
        RuntimeTopologyWithWalDetails(string serverInstanceId)
    {
        DiagnosticsSnapshotMetadata metadata = Metadata(
            serverInstanceId,
            counterEpoch: 3,
            DiagnosticsAvailability.Available);
        var summary = new QueryDiagnosticsSummary(
            metadata,
            RequestCount: 1,
            StatementExecutionCount: 1,
            SucceededCount: 1,
            FailedCount: 0,
            CanceledCount: 0,
            SlowCount: 0,
            RowsProduced: 0,
            RowsAffected: 0,
            ActiveCount: 0);
        var snapshot = new RuntimeDiagnosticsSnapshot(
            metadata,
            DiagnosticsSection<QueryDiagnosticsSummary>.Available(summary),
            DiagnosticsSection<ConnectionDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.Available(
                StorageSnapshot(metadata)),
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.Available(
                WalSnapshot(metadata)),
            DiagnosticsSection<MaintenanceOperationSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable),
            DiagnosticsSection<HealthDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Unavailable));
        return InstanceTopology(snapshot);
    }

    private static DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<
        StorageRuntimeDiagnosticsSnapshot>> StorageTopology(
            string serverInstanceId)
    {
        DiagnosticsSnapshotMetadata metadata = Metadata(
            serverInstanceId,
            counterEpoch: 3,
            DiagnosticsAvailability.Available);
        StorageRuntimeDiagnosticsSnapshot storage = StorageSnapshot(metadata);
        return InstanceTopology(
            new DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>(
                metadata,
                storage));
    }

    private static StorageRuntimeDiagnosticsSnapshot StorageSnapshot(
        DiagnosticsSnapshotMetadata metadata)
        => new StorageRuntimeDiagnosticsSnapshot(
            metadata,
            LogicalDatabaseBytes: 4096,
            AllocatedDatabaseBytes: 8192,
            PageCount: 1,
            PageReads: null,
            PageWrites: null,
            BytesRead: null,
            BytesWritten: null,
            CacheHits: null,
            CacheMisses: null,
            DirtyPages: 0,
            ActiveReaders: 0,
            ActiveWriters: 0,
            CommitCount: 1,
            ConflictCount: null)
        {
            Cache = DiagnosticsSection<StorageCacheDiagnosticsSnapshot>.Available(
                new StorageCacheDiagnosticsSnapshot(
                    metadata,
                    sharedResidentPages: 1,
                    sharedCapacityPages: 16,
                    walResidentPages: 2,
                    walCapacityPages: 8)),
            PhysicalIo = DiagnosticsSection<StorageDeviceIoDiagnosticsSnapshot>
                .Available(new StorageDeviceIoDiagnosticsSnapshot(
                    metadata,
                    readCount: 3,
                    bytesRead: 12_288,
                    writeCount: 1,
                    bytesWritten: 4_096,
                    flushCount: 1,
                    resizeCount: 0,
                    sequentialReadCount: 2,
                    sequentialBytesRead: 8_192,
                    memoryMappedPageExposureCount: 1,
                    memoryMappedBytesExposed: 4_096)),
        };

    private static DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<
        WalRuntimeDiagnosticsSnapshot>> WalTopology(string serverInstanceId)
    {
        DiagnosticsSnapshotMetadata metadata = Metadata(
            serverInstanceId,
            counterEpoch: 3,
            DiagnosticsAvailability.Available);
        return InstanceTopology(
            new DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>(
                metadata,
                WalSnapshot(metadata)));
    }

    private static WalRuntimeDiagnosticsSnapshot WalSnapshot(
        DiagnosticsSnapshotMetadata metadata)
    {
        DateTimeOffset now = metadata.CapturedAtUtc;
        var recovery = new WalRecoveryDiagnosticsSnapshot(
            metadata,
            new OpaqueDiagnosticsId("11111111111111111111111111111111"),
            WalRecoveryPhase.Completed,
            now.AddSeconds(-2),
            now.AddSeconds(-1),
            TimeSpan.FromSeconds(1),
            CSharpDbOperationOutcome.Succeeded,
            scannedFrameCount: 1,
            scannedBytes: 100,
            recoveredFrameCount: 1,
            recoveredBytes: 100,
            discardedFrameCount: 0,
            discardedBytes: 0,
            WalRecoveryTruncationReason.None,
            attemptCount: 1,
            retryCount: 0,
            lastRetryError: null,
            error: null);
        var checkpoint = new CheckpointDiagnosticsSnapshot(
            metadata,
            new OpaqueDiagnosticsId("22222222222222222222222222222222"),
            CheckpointPhase.Copying,
            CheckpointOrigin.BackgroundAuto,
            now.AddSeconds(-1),
            TimeSpan.FromSeconds(1),
            completedPageCount: 0,
            totalPageCount: 1,
            CheckpointRetentionReason.None,
            lastStartedAtUtc: now.AddSeconds(-1),
            lastSuccessfulAtUtc: null,
            lastFailedAtUtc: null,
            lastElapsed: TimeSpan.FromSeconds(1),
            activeCount: 1,
            attemptCount: 1,
            successCount: 0,
            failureCount: 0,
            canceledCount: 0,
            lastError: null);
        return new WalRuntimeDiagnosticsSnapshot(
            metadata,
            LogicalBytes: 100,
            AllocatedBytes: 128,
            CommittedFrameBytes: 100,
            RetainedBytes: 0,
            FrameCount: 1,
            FlushCount: 1,
            BytesWritten: 100,
            PendingCommitCount: 0,
            CheckpointPhase.Copying,
            LastSuccessfulFlushAtUtc: now.AddSeconds(-1),
            LastSuccessfulCheckpointAtUtc: null,
            LastError: null)
        {
            FlushedCommitCount = 3,
            DurableFlushCount = 1,
            LastSuccessfulDurableFlushAtUtc = now.AddSeconds(-1),
            GroupCommitBatchCount = 1,
            GroupCommitCount = 2,
            LastSuccessfulGroupCommitAtUtc = now.AddSeconds(-1),
            Recovery = DiagnosticsSection<WalRecoveryDiagnosticsSnapshot>.Available(
                recovery),
            Checkpoint = DiagnosticsSection<CheckpointDiagnosticsSnapshot>.Available(
                checkpoint),
        };
    }

    private static void AssertProjectedWalMetadata(
        WalRuntimeDiagnosticsSnapshot wal,
        string shardAlias)
    {
        Assert.Equal(DiagnosticsScope.Shard, wal.Metadata.Scope);
        Assert.Equal(shardAlias, wal.Metadata.DatabaseAlias);
        Assert.Equal(wal.Metadata, wal.Recovery.Value!.Metadata);
        Assert.Equal(wal.Metadata, wal.Checkpoint.Value!.Metadata);
        Assert.Equal(3, wal.FlushedCommitCount);
        Assert.Equal(1, wal.DurableFlushCount);
        Assert.Equal(
            wal.Metadata.CapturedAtUtc.AddSeconds(-1),
            wal.LastSuccessfulDurableFlushAtUtc);
        Assert.Equal(1, wal.GroupCommitBatchCount);
        Assert.Equal(2, wal.GroupCommitCount);
        Assert.Equal(
            wal.Metadata.CapturedAtUtc.AddSeconds(-1),
            wal.LastSuccessfulGroupCommitAtUtc);
    }

    private static DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
        MaintenanceOperationSnapshot>> MaintenanceTopology(
            string serverInstanceId,
            int recordCount,
            bool terminal)
    {
        DiagnosticsSnapshotMetadata metadata = Metadata(
            serverInstanceId,
            counterEpoch: 0,
            DiagnosticsAvailability.Available);
        MaintenanceOperationSnapshot[] records = Enumerable.Range(1, recordCount)
            .Select(index => new MaintenanceOperationSnapshot(
                metadata,
                new OpaqueDiagnosticsId($"{index:x32}"),
                MaintenanceOperationKind.Backup,
                terminal
                    ? MaintenanceOperationPhase.Completed
                    : MaintenanceOperationPhase.Copying,
                metadata.CapturedAtUtc.AddTicks(index),
                TimeSpan.FromSeconds(1),
                CompletedUnits: null,
                TotalUnits: null,
                terminal
                    ? CSharpDbOperationOutcome.Succeeded
                    : CSharpDbOperationOutcome.Unknown,
                WarningCount: 0,
                ErrorCount: 0,
                Error: null))
            .ToArray();
        return InstanceTopology(new DiagnosticsCollectionSnapshot<
            MaintenanceOperationSnapshot>(
                metadata,
                records,
                capacity: Math.Max(1, recordCount),
                retention: terminal ? TimeSpan.FromMinutes(1) : null,
                droppedCount: 0,
                isTruncated: false));
    }

    private static DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>
        ActiveTopology(
            string serverInstanceId,
            string operationId,
            DateTimeOffset startedAtUtc,
            long droppedCount)
    {
        DiagnosticsSnapshotMetadata metadata = Metadata(
            serverInstanceId,
            counterEpoch: 0,
            DiagnosticsAvailability.Available,
            recordsTruncated: droppedCount > 0);
        var active = new ActiveQuerySnapshot(
            metadata,
            new OpaqueDiagnosticsId(operationId),
            ParentOperationId: null,
            CSharpDbOperationClass.Query,
            CSharpDbOperationRole.Root,
            QueryExecutionPhase.Executing,
            startedAtUtc,
            TimeSpan.FromSeconds(1),
            Fingerprint: null,
            CSharpDB.Observability.CSharpDbTransport.Direct,
            TraceId: null,
            SessionId: null);
        var collection = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            metadata,
            [active],
            capacity: 10,
            retention: null,
            droppedCount,
            isTruncated: droppedCount > 0);
        return InstanceTopology(collection);
    }

    private static DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>
        ActiveTopologyWithRecordCount(
            string serverInstanceId,
            int recordCount)
    {
        DiagnosticsSnapshotMetadata metadata = Metadata(
            serverInstanceId,
            counterEpoch: 0,
            DiagnosticsAvailability.Available);
        ActiveQuerySnapshot[] records = Enumerable.Range(1, recordCount)
            .Select(index => new ActiveQuerySnapshot(
                metadata,
                new OpaqueDiagnosticsId($"{index:x32}"),
                ParentOperationId: null,
                CSharpDbOperationClass.Query,
                CSharpDbOperationRole.Internal,
                QueryExecutionPhase.Executing,
                new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero)
                    .AddTicks(index),
                TimeSpan.FromSeconds(1),
                Fingerprint: null,
                CSharpDB.Observability.CSharpDbTransport.Direct,
                TraceId: null,
                SessionId: null))
            .ToArray();
        var collection = new DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>(
            metadata,
            records,
            capacity: Math.Max(1, recordCount),
            retention: null,
            droppedCount: 0,
            isTruncated: false);
        return InstanceTopology(collection);
    }

    private static DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<T>>
        EmptyCollectionTopology<T>(string serverInstanceId)
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        DiagnosticsSnapshotMetadata metadata = Metadata(
            serverInstanceId,
            counterEpoch: 0,
            DiagnosticsAvailability.Available);
        return InstanceTopology(new DiagnosticsCollectionSnapshot<T>(
            metadata,
            records: [],
            capacity: 10,
            retention: null,
            droppedCount: 0,
            isTruncated: false));
    }

    private static DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>
        QueryDetailTopology(
            string serverInstanceId,
            OpaqueDiagnosticsId operationId,
            string capturedSql)
    {
        DiagnosticsSnapshotMetadata metadata = Metadata(
            serverInstanceId,
            counterEpoch: 0,
            DiagnosticsAvailability.Available);
        var detail = new QueryDetailSnapshot(
            metadata,
            operationId,
            Fingerprint: null,
            SqlTextCaptureMode.Raw,
            capturedSql);
        return InstanceTopology(new DiagnosticsValueSnapshot<QueryDetailSnapshot>(
            metadata,
            detail));
    }

    private static DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>
        UnavailableQueryDetailTopology(string serverInstanceId)
    {
        DiagnosticsSnapshotMetadata metadata = Metadata(
            serverInstanceId,
            counterEpoch: 0,
            DiagnosticsAvailability.Unavailable);
        return InstanceTopology(new DiagnosticsValueSnapshot<QueryDetailSnapshot>(
            metadata,
            value: null));
    }

    private static DiagnosticsTopologySnapshot<T> InstanceTopology<T>(T aggregate)
        where T : class, IRuntimeDiagnosticsSnapshot
        => new(
            aggregate,
            shards: null,
            shardCapacity: null,
            droppedShardCount: null,
            shardsTruncated: null);

    private static DiagnosticsSnapshotMetadata Metadata(
        string serverInstanceId,
        long counterEpoch,
        DiagnosticsAvailability availability,
        bool recordsTruncated = false)
        => new(
            CSharpDbDiagnostics.SchemaVersion,
            new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero),
            serverInstanceId,
            counterEpoch,
            DiagnosticsScope.Instance,
            availability,
            DiagnosticsSource.Engine,
            "physical",
            recordsTruncated,
            fieldsTruncated: false);

    private static (IShardTestClient Client, RecordingProxy Recording)
        CreateObservedClient()
    {
        IShardTestClient client = DispatchProxy.Create<IShardTestClient, RecordingProxy>();
        return (client, (RecordingProxy)(object)client);
    }

    private static ICSharpDbClient CreateLegacyClient()
        => DispatchProxy.Create<ICSharpDbClient, RecordingProxy>();

    public interface IShardTestClient : ICSharpDbClient, ICSharpDbObservabilityClient;

    public sealed record Invocation(string MethodName, object?[] Arguments);

    public class RecordingProxy : DispatchProxy
    {
        private readonly Dictionary<string, Func<object?[], object>> _handlers =
            new(StringComparer.Ordinal);
        private readonly List<Invocation> _invocations = [];

        public IReadOnlyList<Invocation> Invocations
        {
            get
            {
                lock (_invocations)
                    return _invocations.ToArray();
            }
        }

        public void SetResult(string methodName, object result)
            => SetHandler(methodName, _ => result);

        public void SetHandler(string methodName, Func<object?[], object> handler)
            => _handlers[methodName] = handler;

        public void SetSynchronousException(string methodName, Exception exception)
            => SetHandler(methodName, _ => throw exception);

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            ArgumentNullException.ThrowIfNull(targetMethod);
            if (targetMethod.Name == "get_DataSource")
                return "fake";
            if (targetMethod.Name == nameof(IAsyncDisposable.DisposeAsync))
                return ValueTask.CompletedTask;

            object?[] arguments = args?.ToArray() ?? [];
            lock (_invocations)
                _invocations.Add(new Invocation(targetMethod.Name, arguments));
            if (!_handlers.TryGetValue(targetMethod.Name, out Func<object?[], object>? handler))
                throw new InvalidOperationException($"No result configured for {targetMethod.Name}.");
            return handler(arguments);
        }
    }

    private sealed class BlockingCaptureTracker
    {
        private int _active;
        private int _maximum;

        public TaskCompletionSource AtConcurrencyLimit { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int MaximumConcurrency => Volatile.Read(ref _maximum);

        public async Task<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>
            CaptureAsync(CancellationToken ct)
        {
            int active = Interlocked.Increment(ref _active);
            int observed;
            do
            {
                observed = Volatile.Read(ref _maximum);
                if (observed >= active)
                    break;
            }
            while (Interlocked.CompareExchange(ref _maximum, active, observed) != observed);
            if (active == 8)
                AtConcurrencyLimit.TrySetResult();

            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                throw new InvalidOperationException("The infinite cancellation wait completed unexpectedly.");
            }
            finally
            {
                Interlocked.Decrement(ref _active);
            }
        }
    }

    private sealed class QueryEventObserver : IObserver<KeyValuePair<string, object?>>
    {
        private readonly List<CSharpDbQueryTerminalEvent> _terminalEvents = [];

        public IReadOnlyList<CSharpDbQueryTerminalEvent> TerminalEvents
        {
            get
            {
                lock (_terminalEvents)
                    return _terminalEvents.ToArray();
            }
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Value is not CSharpDbQueryTerminalEvent terminal)
                return;
            lock (_terminalEvents)
                _terminalEvents.Add(terminal);
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
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

        public void Advance(TimeSpan duration)
        {
            _utcNow = _utcNow.Add(duration);
            Interlocked.Add(ref _timestamp, duration.Ticks);
        }
    }
}
