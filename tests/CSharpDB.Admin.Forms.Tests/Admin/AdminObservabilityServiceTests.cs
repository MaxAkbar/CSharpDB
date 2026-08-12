using System.Reflection;
using CSharpDB.Admin.Configuration;
using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.Observability;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Tests.Admin;

public sealed class AdminObservabilityServiceTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly DateTimeOffset Start =
        new(2026, 8, 11, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Options_ValidateAllBoundaries()
    {
        new AdminObservabilityOptions
        {
            RefreshInterval = AdminObservabilityOptions.MinimumRefreshInterval,
            MaximumRecords = CSharpDbObservabilityOptions.MaximumHistoryCapacity,
            SampleCapacity = 2,
            StaleAfter = AdminObservabilityOptions.MinimumRefreshInterval,
        }.Validate();

        Assert.Throws<InvalidOperationException>(() => new AdminObservabilityOptions
        {
            RefreshInterval = TimeSpan.FromMilliseconds(249),
        }.Validate());
        Assert.Throws<InvalidOperationException>(() => new AdminObservabilityOptions
        {
            MaximumRecords = CSharpDbObservabilityOptions.MaximumHistoryCapacity + 1,
        }.Validate());
        Assert.Throws<InvalidOperationException>(() => new AdminObservabilityOptions
        {
            SampleCapacity = 1,
        }.Validate());
        Assert.Throws<InvalidOperationException>(() => new AdminObservabilityOptions
        {
            RefreshInterval = TimeSpan.FromSeconds(2),
            StaleAfter = TimeSpan.FromSeconds(1),
        }.Validate());
    }

    [Fact]
    public async Task RuntimeRefreshInterval_CannotExceedConfiguredStaleThreshold()
    {
        var clock = new ManualTimeProvider(Start);
        var client = new FakeObservabilityClient(clock);
        await using var service = CreatePausedService(
            client,
            clock,
            staleAfter: TimeSpan.FromSeconds(10));
        service.SetRefreshInterval(TimeSpan.FromSeconds(10));
        Assert.Equal(TimeSpan.FromSeconds(10), service.Current.MaximumRefreshInterval);
        Assert.Equal(TimeSpan.FromSeconds(10), service.Current.RefreshInterval);
        Assert.Throws<ArgumentOutOfRangeException>(
            () => service.SetRefreshInterval(TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task ManualRefreshWhilePaused_UsesSixCallsAndDoesNotFabricateFirstRates()
    {
        var clock = new ManualTimeProvider(Start);
        var client = new FakeObservabilityClient(clock);
        await using var service = CreatePausedService(client, clock, sampleCapacity: 4);

        await service.SetActiveAsync(true);
        Assert.False(service.Current.IsLoading);
        Assert.Equal("Paused", service.Current.StatusText);
        await service.RefreshAsync(Ct);

        Assert.Equal(6, client.PollCallCount);
        Assert.Equal(100, client.MaximumRecordsSeen);
        Assert.Equal(0, client.PlanCallCount);
        Assert.Equal(0, client.DetailCallCount);
        AdminObservabilityMetricSample sample = Assert.Single(service.Current.Samples);
        Assert.Null(sample.QueryRatePerSecond);
        Assert.Null(sample.ErrorRatePerSecond);
        Assert.Null(sample.AverageLatencyMilliseconds);
        Assert.Null(sample.WalGrowthBytesPerSecond);
        Assert.Equal(Start, service.Current.SnapshotCapturedAtUtc);
        Assert.Equal(TimeSpan.Zero, service.Current.SnapshotAge);
    }

    [Fact]
    public async Task Samples_AreBounded_ResetOnCounterRegression_AndPreserveWalShrink()
    {
        var clock = new ManualTimeProvider(Start);
        var client = new FakeObservabilityClient(clock)
        {
            RequestCount = 10,
            FailedCount = 2,
            WalBytes = 1_000,
        };
        await using var service = CreatePausedService(client, clock, sampleCapacity: 2);
        await service.SetActiveAsync(true);
        await service.RefreshAsync(Ct);

        clock.Advance(TimeSpan.FromSeconds(2));
        client.RequestCount = 14;
        client.FailedCount = 4;
        client.WalBytes = 600;
        await service.RefreshAsync(Ct);

        AdminObservabilityMetricSample second = service.Current.Samples[^1];
        Assert.Equal(2d, second.QueryRatePerSecond);
        Assert.Equal(1d, second.ErrorRatePerSecond);
        Assert.Equal(-200d, second.WalGrowthBytesPerSecond);
        Assert.Equal(2, service.Current.Samples.Count);

        clock.Advance(TimeSpan.FromSeconds(1));
        client.RequestCount = 1;
        client.FailedCount = 0;
        await service.RefreshAsync(Ct);

        AdminObservabilityMetricSample reset = Assert.Single(service.Current.Samples);
        Assert.Null(reset.QueryRatePerSecond);
        Assert.Null(reset.ErrorRatePerSecond);
    }

    [Fact]
    public async Task Samples_ResetWhenServerInstanceOrCounterEpochChanges()
    {
        var clock = new ManualTimeProvider(Start);
        var client = new FakeObservabilityClient(clock) { RequestCount = 10 };
        await using var service = CreatePausedService(client, clock, sampleCapacity: 4);
        await service.SetActiveAsync(true);
        await service.RefreshAsync(Ct);

        clock.Advance(TimeSpan.FromSeconds(1));
        client.RequestCount = 12;
        client.ServerInstanceId = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        await service.RefreshAsync(Ct);

        AdminObservabilityMetricSample instanceReset = Assert.Single(service.Current.Samples);
        Assert.Null(instanceReset.QueryRatePerSecond);

        clock.Advance(TimeSpan.FromSeconds(1));
        client.RequestCount = 14;
        client.CounterEpoch = 1;
        await service.RefreshAsync(Ct);

        AdminObservabilityMetricSample epochReset = Assert.Single(service.Current.Samples);
        Assert.Null(epochReset.QueryRatePerSecond);
    }

    [Fact]
    public async Task PartialFailures_AreClassifiedWithoutLeakingMessages()
    {
        var clock = new ManualTimeProvider(Start);
        var client = new FakeObservabilityClient(clock)
        {
            ActiveQueriesFailure = new CSharpDbObservabilityAccessDeniedException(),
            RecentQueriesFailure = new CSharpDbObservabilityNotSupportedException(),
            SessionsFailure = new InvalidOperationException("secret path C:\\private\\db"),
            ActiveMaintenanceAvailability = DiagnosticsAvailability.Disabled,
            RecentMaintenanceAvailability = DiagnosticsAvailability.NotApplicable,
        };
        await using var service = CreatePausedService(client, clock);
        await service.SetActiveAsync(true);
        await service.RefreshAsync(Ct);

        Assert.Equal(DiagnosticsAvailability.Denied, service.Current.ActiveQueries.Availability);
        Assert.Equal(DiagnosticsAvailability.Unsupported, service.Current.RecentQueries.Availability);
        Assert.Equal(DiagnosticsAvailability.Unavailable, service.Current.Sessions.Availability);
        Assert.Equal(DiagnosticsAvailability.Disabled, service.Current.ActiveMaintenance.Availability);
        Assert.Equal(DiagnosticsAvailability.NotApplicable, service.Current.RecentMaintenance.Availability);
        Assert.DoesNotContain("private", service.Current.StatusText, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("secret", service.Current.Sessions.StatusText, StringComparison.OrdinalIgnoreCase);
        Assert.True(service.Current.HasUnavailableSections);
    }

    [Fact]
    public async Task RuntimeDenial_IsInheritedByStorageAndWalWithoutReadingDetails()
    {
        var clock = new ManualTimeProvider(Start);
        var client = new FakeObservabilityClient(clock)
        {
            RuntimeOverride = _ => Task.FromException<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>(
                new CSharpDbObservabilityAccessDeniedException()),
        };
        await using var service = CreatePausedService(client, clock);
        await service.SetActiveAsync(true);

        await service.RefreshAsync(Ct);

        Assert.Equal(DiagnosticsAvailability.Denied, service.Current.Runtime.Availability);
        Assert.Equal(DiagnosticsAvailability.Denied, service.Current.Storage.Availability);
        Assert.Equal(DiagnosticsAvailability.Denied, service.Current.Wal.Availability);
        Assert.True(service.Current.HasUnavailableSections);
    }

    [Fact]
    public async Task Refreshes_DoNotOverlap_AndLateCompletionAfterDisposeCannotPublish()
    {
        var clock = new ManualTimeProvider(Start);
        var client = new FakeObservabilityClient(clock);
        var blocked = new TaskCompletionSource<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        client.RuntimeOverride = _ => blocked.Task;
        var service = CreatePausedService(client, clock);
        await service.SetActiveAsync(true);

        Task first = service.RefreshAsync(Ct);
        await client.RuntimeCalled.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        await service.RefreshAsync(Ct);
        Assert.Equal(1, client.RuntimeCallCount);

        int notifications = 0;
        service.StateChanged += () => notifications++;
        await service.DisposeAsync();
        int afterDispose = notifications;
        blocked.SetResult(client.CreateRuntime());
        await first;

        Assert.Equal(afterDispose, notifications);
        Assert.Equal("Disposed", service.Current.StatusText);
    }

    [Fact]
    public async Task LatestSensitiveRequestWins_AndHideCancelsLateSqlPublication()
    {
        var clock = new ManualTimeProvider(Start);
        var client = new FakeObservabilityClient(clock);
        await using var service = CreatePausedService(client, clock);
        await service.SetActiveAsync(true);
        await service.RefreshAsync(Ct);

        var first = new TaskCompletionSource<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var second = new TaskCompletionSource<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        client.DetailResponses.Enqueue(first.Task);
        client.DetailResponses.Enqueue(second.Task);
        var firstId = new OpaqueDiagnosticsId("11111111111111111111111111111111");
        var secondId = new OpaqueDiagnosticsId("22222222222222222222222222222222");

        Task firstLoad = service.RevealQueryDetailAsync(firstId, Ct);
        Task secondLoad = service.RevealQueryDetailAsync(secondId, Ct);
        second.SetResult(client.CreateDetail(secondId, "select 'new'"));
        await secondLoad;
        first.SetResult(client.CreateDetail(firstId, "select 'old'"));
        await firstLoad;
        Assert.Equal("select 'new'", service.Current.RevealedDetail.Value?.CapturedSqlText);

        var late = new TaskCompletionSource<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        client.DetailResponses.Enqueue(late.Task);
        Task lateLoad = service.RevealQueryDetailAsync(firstId, Ct);
        await service.SetActiveAsync(false);
        late.SetResult(client.CreateDetail(firstId, "select 'leaked'"));
        await lateLoad;
        Assert.False(service.Current.HasDetailRequest);
        Assert.Null(service.Current.RevealedDetail.Value);
    }

    [Fact]
    public async Task TabSwitchImmediatelyStopsCallsAndClearsSensitiveDetail()
    {
        var clock = new ManualTimeProvider(Start);
        var client = new FakeObservabilityClient(clock);
        var tabs = new TabManagerService();
        tabs.OpenObservabilityTab();
        await using var service = CreatePausedService(client, clock, tabManager: tabs);
        await service.SetActiveAsync(true);
        await service.RefreshAsync(Ct);
        var id = new OpaqueDiagnosticsId("33333333333333333333333333333333");
        client.DetailResponses.Enqueue(Task.FromResult(client.CreateDetail(id, "select 3")));
        await service.RevealQueryDetailAsync(id, Ct);
        Assert.NotNull(service.Current.RevealedDetail.Value);

        tabs.ActivateTab("welcome");
        int before = client.PollCallCount;
        await service.RefreshAsync(Ct);

        Assert.Equal(before, client.PollCallCount);
        Assert.False(service.Current.HasDetailRequest);
        Assert.Null(service.Current.RevealedDetail.Value);
        Assert.Equal("Inactive", service.Current.StatusText);
    }

    [Fact]
    public async Task TabDeactivationClosesAdmissionBeforeReturning()
    {
        var clock = new ManualTimeProvider(Start);
        using var runtimeGate = new ManualResetEventSlim(false);
        var client = new FakeObservabilityClient(clock)
        {
            RuntimeSynchronousGate = runtimeGate,
        };
        var tabs = new TabManagerService();
        tabs.OpenObservabilityTab();
        await using var service = CreatePausedService(client, clock, tabManager: tabs);
        await service.SetActiveAsync(true);

        Task refresh = Task.Run(() => service.RefreshAsync(Ct), Ct);
        await client.RuntimeSynchronousEntered.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        var hideStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task hide = Task.Run(() =>
        {
            hideStarted.TrySetResult();
            tabs.ActivateTab("welcome");
        }, Ct);
        await hideStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        Assert.False(hide.IsCompleted);

        runtimeGate.Set();
        await hide;
        await refresh;
        int callsAfterHideReturned = client.PollCallCount;

        await service.RefreshAsync(Ct);
        Assert.Equal(callsAfterHideReturned, client.PollCallCount);
    }

    [Fact]
    public async Task DatabaseChangeCancelsOldGenerationBeforeDrain_AndLateResultCannotMix()
    {
        var clock = new ManualTimeProvider(Start);
        var firstData = new FakeObservabilityClient(clock)
        {
            ServerInstanceId = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        };
        var secondData = new FakeObservabilityClient(clock)
        {
            ServerInstanceId = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        };
        (DatabaseClientHolderObservabilityTests.IObservabilityTestClient first,
            DatabaseClientHolderObservabilityTests.RecordingClientProxy firstRecording) =
            CreateHolderProxy();
        (DatabaseClientHolderObservabilityTests.IObservabilityTestClient second,
            DatabaseClientHolderObservabilityTests.RecordingClientProxy secondRecording) =
            CreateHolderProxy();
        ConfigureHolderObservability(firstRecording, firstData);
        ConfigureHolderObservability(secondRecording, secondData);

        await using var holder = new DatabaseClientHolder(
            first,
            shardAdmin: null,
            baseClientOptions: null,
            hostDatabaseOptions: new AdminHostDatabaseOptions(),
            functions: DbFunctionRegistry.Create(_ => { }));
        var tabs = new TabManagerService();
        tabs.OpenObservabilityTab();
        var options = CreateOptions();
        await using var service = new AdminObservabilityService(holder, options, tabs);
        service.SetPaused(true);
        await service.SetActiveAsync(true);
        await service.RefreshAsync(Ct);

        var detailId = new OpaqueDiagnosticsId("44444444444444444444444444444444");
        firstRecording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetQueryDetailAsync),
            Task.FromResult(firstData.CreateDetail(detailId, "select 'old database'")));
        await service.RevealQueryDetailAsync(detailId, Ct);
        Assert.NotNull(service.Current.RevealedDetail.Value);

        var blockedRuntime = new TaskCompletionSource<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        firstRecording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
            blockedRuntime.Task);
        Task oldRefresh = service.RefreshAsync(Ct);
        await WaitUntilAsync(
            () => firstRecording.Invocations.Count(invocation =>
                invocation.MethodName == nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync)) >= 2,
            Ct);

        Task replacement = holder.ReplaceClientAsync(
            second,
            newShardAdmin: null,
            newBaseClientOptions: null);

        Assert.False(replacement.IsCompleted);
        Assert.Null(service.Current.Runtime.Value);
        Assert.Null(service.Current.SnapshotCapturedAtUtc);
        Assert.Null(service.Current.RevealedDetail.Value);
        Assert.False(service.Current.HasDetailRequest);
        Assert.Null(service.Current.SelectedScope);
        Assert.False(service.Current.IsLoading);
        Assert.Equal(
            "Paused; refresh manually to load the new database",
            service.Current.StatusText);
        await ((ICSharpDbObservabilityClient)holder).GetRuntimeDiagnosticsAsync(Ct);
        Assert.Contains(
            secondRecording.Invocations,
            invocation => invocation.MethodName == nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync));

        blockedRuntime.SetResult(firstData.CreateRuntime());
        await oldRefresh;
        await replacement;
        Assert.Null(service.Current.Runtime.Value);
        Assert.Null(service.Current.SnapshotCapturedAtUtc);

        await service.RefreshAsync(Ct);
        Assert.Equal(
            secondData.ServerInstanceId,
            service.Current.Runtime.Value?.Metadata.ServerInstanceId);
    }

    [Fact]
    public async Task ShardTopologyDisclosesBounds_AndMissingSelectionResetsHonestly()
    {
        var clock = new ManualTimeProvider(Start);
        var client = new FakeObservabilityClient(clock);
        client.RuntimeTopologyOverride = () => clientTopology(includeShard: true);
        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> clientTopology(bool includeShard)
            => client.CreateShardedRuntime(includeShard, shardsTruncated: true, droppedShards: 2);

        await using var service = CreatePausedService(client, clock);
        await service.SetActiveAsync(true);
        await service.RefreshAsync(Ct);
        Assert.True(service.Current.ShardsTruncated);
        Assert.Equal(2, service.Current.DroppedShardCount);
        Assert.Equal(4, service.Current.ShardCapacity);
        Assert.Contains(service.Current.ScopeOptions, option => option.Value == "alpha");

        service.SetScope("alpha");
        Assert.Null(service.Current.LastSuccessfulRefreshUtc);
        Assert.Null(service.Current.SnapshotCapturedAtUtc);
        Assert.Null(service.Current.SnapshotAge);
        Assert.False(service.Current.IsStale);
        Assert.False(service.Current.IsLoading);
        Assert.Equal(
            "Paused; refresh manually to load the selected scope",
            service.Current.StatusText);
        await service.RefreshAsync(Ct);
        Assert.Equal("alpha", service.Current.SelectedScope);
        Assert.Equal(DiagnosticsScope.Shard, service.Current.Runtime.Value?.Metadata.Scope);

        client.RuntimeTopologyOverride = () => client.CreateShardedRuntime(
            includeShard: false,
            shardsTruncated: true,
            droppedShards: 2);
        await service.RefreshAsync(Ct);
        Assert.Equal("alpha", service.Current.SelectedScope);
        Assert.Null(service.Current.Runtime.Value);
        Assert.Equal(DiagnosticsAvailability.Unavailable, service.Current.Runtime.Availability);
        Assert.Contains(
            service.Current.ScopeOptions,
            option => option.Value == "alpha" &&
                      option.Availability == DiagnosticsAvailability.Unavailable);
        Assert.Equal(
            "Shard 'alpha' is outside this bounded response; selection retained and diagnostics are unavailable.",
            service.Current.ScopeNotice);

        client.RuntimeTopologyOverride = () => client.CreateShardedRuntime(
            includeShard: false,
            shardsTruncated: false,
            droppedShards: 0);
        await service.RefreshAsync(Ct);
        Assert.Null(service.Current.SelectedScope);
        Assert.Equal(
            "Shard 'alpha' is no longer present; showing aggregate diagnostics.",
            service.Current.ScopeNotice);
        Assert.Equal(DiagnosticsScope.Aggregate, service.Current.Runtime.Value?.Metadata.Scope);
    }

    [Fact]
    public async Task PausedSnapshotPublishesStaleBoundaryWithoutPolling_AndEventsAreIsolated()
    {
        var clock = new ManualTimeProvider(Start);
        var client = new FakeObservabilityClient(clock);
        await using var service = CreatePausedService(client, clock, staleAfter: TimeSpan.FromSeconds(2));
        int healthyNotifications = 0;
        service.StateChanged += () => throw new InvalidOperationException("subscriber failure");
        service.StateChanged += () => healthyNotifications++;
        await service.SetActiveAsync(true);
        await service.RefreshAsync(Ct);
        int calls = client.PollCallCount;
        int beforeBoundary = healthyNotifications;
        await clock.TimerCreated.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct);

        clock.Advance(TimeSpan.FromSeconds(2));
        await WaitUntilAsync(() => healthyNotifications > beforeBoundary, Ct);

        Assert.Equal(calls, client.PollCallCount);
        Assert.True(service.Current.IsStale);
        Assert.True(healthyNotifications > beforeBoundary);
    }

    [Fact]
    public async Task ActiveSnapshotPublishesStaleWhileNextRefreshHangsWithoutExtraCall()
    {
        var clock = new ManualTimeProvider(Start);
        var client = new FakeObservabilityClient(clock);
        await using var service = CreatePausedService(
            client,
            clock,
            staleAfter: TimeSpan.FromSeconds(2));
        int notifications = 0;
        service.StateChanged += () => notifications++;
        await service.SetActiveAsync(true);
        await service.RefreshAsync(Ct);

        var blocked = new TaskCompletionSource<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        client.RuntimeOverride = _ => blocked.Task;
        int beforeHungRefresh = client.RuntimeCallCount;
        int callsBeforeHungRefresh = client.PollCallCount;
        service.SetPaused(false);
        await WaitUntilAsync(() => client.RuntimeCallCount > beforeHungRefresh, Ct);
        await WaitUntilAsync(() => client.PollCallCount >= callsBeforeHungRefresh + 6, Ct);
        int callsWhileHung = client.PollCallCount;
        int notificationsBeforeBoundary = notifications;

        clock.Advance(TimeSpan.FromSeconds(2));
        await WaitUntilAsync(() => notifications > notificationsBeforeBoundary, Ct);

        Assert.True(service.Current.IsStale);
        Assert.Equal(callsWhileHung, client.PollCallCount);
        blocked.SetResult(client.CreateRuntime());
        await WaitUntilAsync(() => !service.Current.IsRefreshing, Ct);
    }

    private static AdminObservabilityService CreatePausedService(
        FakeObservabilityClient client,
        ManualTimeProvider clock,
        int sampleCapacity = 4,
        TimeSpan? staleAfter = null,
        TabManagerService? tabManager = null)
    {
        AdminObservabilityOptions options = CreateOptions(sampleCapacity, staleAfter);
        var service = new AdminObservabilityService(
            client,
            options,
            clock,
            databaseClientHolder: null,
            tabManager: tabManager);
        service.SetPaused(true);
        return service;
    }

    private static AdminObservabilityOptions CreateOptions(
        int sampleCapacity = 4,
        TimeSpan? staleAfter = null)
        => new()
        {
            RefreshInterval = TimeSpan.FromSeconds(1),
            MaximumRecords = 100,
            SampleCapacity = sampleCapacity,
            StaleAfter = staleAfter ?? TimeSpan.FromSeconds(10),
        };

    private static (
        DatabaseClientHolderObservabilityTests.IObservabilityTestClient Client,
        DatabaseClientHolderObservabilityTests.RecordingClientProxy Recording)
        CreateHolderProxy()
    {
        DatabaseClientHolderObservabilityTests.IObservabilityTestClient client =
            DispatchProxy.Create<
                DatabaseClientHolderObservabilityTests.IObservabilityTestClient,
                DatabaseClientHolderObservabilityTests.RecordingClientProxy>();
        return (
            client,
            (DatabaseClientHolderObservabilityTests.RecordingClientProxy)(object)client);
    }

    private static void ConfigureHolderObservability(
        DatabaseClientHolderObservabilityTests.RecordingClientProxy recording,
        FakeObservabilityClient data)
    {
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRuntimeDiagnosticsAsync),
            Task.FromResult(data.CreateRuntime()));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetActiveQueriesAsync),
            Task.FromResult(data.CreateEmptyCollection<ActiveQuerySnapshot>()));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRecentQueriesAsync),
            Task.FromResult(data.CreateEmptyCollection<RecentQuerySnapshot>()));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetSessionsAsync),
            Task.FromResult(data.CreateEmptyCollection<SessionDiagnosticsSnapshot>()));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetActiveMaintenanceOperationsAsync),
            Task.FromResult(data.CreateEmptyCollection<MaintenanceOperationSnapshot>()));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetRecentMaintenanceOperationsAsync),
            Task.FromResult(data.CreateEmptyCollection<MaintenanceOperationSnapshot>()));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetQueryPlanDiagnosticsAsync),
            Task.FromResult(data.CreateUnavailableValue<QueryPlanDiagnosticsSnapshot>()));
        recording.SetResult(
            nameof(ICSharpDbObservabilityClient.GetQueryDetailAsync),
            Task.FromResult(data.CreateUnavailableValue<QueryDetailSnapshot>()));
    }

    private static async Task WaitUntilAsync(
        Func<bool> condition,
        CancellationToken cancellationToken = default)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            timeout.Token,
            cancellationToken);
        while (!condition())
            await Task.Delay(10, linked.Token);
    }

    private sealed class FakeObservabilityClient(ManualTimeProvider clock) : ICSharpDbObservabilityClient
    {
        private const string ServerId = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        private int _pollCallCount;
        private int _runtimeCallCount;

        public long RequestCount { get; set; }
        public long FailedCount { get; set; }
        public long? WalBytes { get; set; }
        public long CounterEpoch { get; set; }
        public string ServerInstanceId { get; set; } = ServerId;
        public int PollCallCount => Volatile.Read(ref _pollCallCount);
        public int RuntimeCallCount => Volatile.Read(ref _runtimeCallCount);
        public int MaximumRecordsSeen { get; private set; }
        public int PlanCallCount { get; private set; }
        public int DetailCallCount { get; private set; }
        public TaskCompletionSource RuntimeCalled { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource RuntimeSynchronousEntered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        public ManualResetEventSlim? RuntimeSynchronousGate { get; set; }
        public Func<CancellationToken, Task<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>>? RuntimeOverride { get; set; }
        public Func<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>? RuntimeTopologyOverride { get; set; }
        public Exception? ActiveQueriesFailure { get; set; }
        public Exception? RecentQueriesFailure { get; set; }
        public Exception? SessionsFailure { get; set; }
        public DiagnosticsAvailability ActiveMaintenanceAvailability { get; set; } = DiagnosticsAvailability.Available;
        public DiagnosticsAvailability RecentMaintenanceAvailability { get; set; } = DiagnosticsAvailability.Available;
        public Queue<Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>>> DetailResponses { get; } = new();

        public Task<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>> GetRuntimeDiagnosticsAsync(CancellationToken ct = default)
        {
            Interlocked.Increment(ref _pollCallCount);
            Interlocked.Increment(ref _runtimeCallCount);
            RuntimeCalled.TrySetResult();
            if (RuntimeSynchronousGate is { } gate)
            {
                RuntimeSynchronousEntered.TrySetResult();
                gate.Wait(ct);
            }
            if (RuntimeOverride is not null)
                return RuntimeOverride(ct);
            return Task.FromResult(RuntimeTopologyOverride?.Invoke() ?? CreateRuntime());
        }

        public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>>> GetStorageDiagnosticsAsync(CancellationToken ct = default)
            => throw new Xunit.Sdk.XunitException("Storage detail must not be polled.");

        public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>>> GetWalDiagnosticsAsync(CancellationToken ct = default)
            => throw new Xunit.Sdk.XunitException("WAL detail must not be polled.");

        public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>> GetActiveQueriesAsync(int maximumRecords, CancellationToken ct = default)
        {
            RecordPoll(maximumRecords);
            return ActiveQueriesFailure is null
                ? Task.FromResult(CreateEmptyCollection<ActiveQuerySnapshot>())
                : Task.FromException<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>(ActiveQueriesFailure);
        }

        public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>> GetRecentQueriesAsync(int maximumRecords, CancellationToken ct = default)
        {
            RecordPoll(maximumRecords);
            return RecentQueriesFailure is null
                ? Task.FromResult(CreateEmptyCollection<RecentQuerySnapshot>())
                : Task.FromException<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>>(RecentQueriesFailure);
        }

        public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>> GetQueryPlanDiagnosticsAsync(OpaqueDiagnosticsId operationId, CancellationToken ct = default)
        {
            PlanCallCount++;
            return Task.FromResult(CreateUnavailableValue<QueryPlanDiagnosticsSnapshot>());
        }

        public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>> GetSessionsAsync(int maximumRecords, CancellationToken ct = default)
        {
            RecordPoll(maximumRecords);
            return SessionsFailure is null
                ? Task.FromResult(CreateEmptyCollection<SessionDiagnosticsSnapshot>())
                : Task.FromException<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>(SessionsFailure);
        }

        public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>> GetActiveMaintenanceOperationsAsync(int maximumRecords, CancellationToken ct = default)
        {
            RecordPoll(maximumRecords);
            return Task.FromResult(CollectionWithAvailability<MaintenanceOperationSnapshot>(ActiveMaintenanceAvailability));
        }

        public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>> GetRecentMaintenanceOperationsAsync(int maximumRecords, CancellationToken ct = default)
        {
            RecordPoll(maximumRecords);
            return Task.FromResult(CollectionWithAvailability<MaintenanceOperationSnapshot>(RecentMaintenanceAvailability));
        }

        public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>> GetQueryDetailAsync(OpaqueDiagnosticsId operationId, CancellationToken ct = default)
        {
            DetailCallCount++;
            return DetailResponses.Count == 0
                ? Task.FromResult(CreateDetail(operationId, "select 1"))
                : DetailResponses.Dequeue();
        }

        public DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> CreateRuntime()
            => InstanceTopology(CreateRuntimeSnapshot(DiagnosticsScope.Instance, "physical"));

        public DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> CreateShardedRuntime(
            bool includeShard,
            bool shardsTruncated,
            long droppedShards)
        {
            RuntimeDiagnosticsSnapshot aggregate = CreateRuntimeSnapshot(
                DiagnosticsScope.Aggregate,
                "coordinator");
            IReadOnlyList<ShardDiagnosticsSection<RuntimeDiagnosticsSnapshot>> shards =
                includeShard
                    ? [new("alpha", DiagnosticsAvailability.Available,
                        CreateRuntimeSnapshot(DiagnosticsScope.Shard, "alpha"))]
                    : [];
            return new DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>(
                aggregate,
                shards,
                shardCapacity: 4,
                droppedShardCount: droppedShards,
                shardsTruncated,
                runtimeFamilies: null,
                runtimeFamilyCapacity: null,
                droppedRuntimeFamilyCount: null,
                runtimeFamiliesTruncated: null);
        }

        public DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>> CreateDetail(
            OpaqueDiagnosticsId operationId,
            string sql)
        {
            DiagnosticsSnapshotMetadata metadata = Metadata(
                DiagnosticsScope.Instance,
                "physical",
                DiagnosticsAvailability.Available);
            var detail = new QueryDetailSnapshot(
                metadata,
                operationId,
                Fingerprint: null,
                SqlTextCaptureMode.Raw,
                sql);
            return InstanceTopology(new DiagnosticsValueSnapshot<QueryDetailSnapshot>(metadata, detail));
        }

        private RuntimeDiagnosticsSnapshot CreateRuntimeSnapshot(DiagnosticsScope scope, string alias)
        {
            DiagnosticsSnapshotMetadata metadata = Metadata(scope, alias, DiagnosticsAvailability.Available);
            var query = new QueryDiagnosticsSummary(
                metadata,
                RequestCount,
                RequestCount,
                Math.Max(0, RequestCount - FailedCount),
                FailedCount,
                CanceledCount: 0,
                SlowCount: 0,
                RowsProduced: 0,
                RowsAffected: 0,
                ActiveCount: 0);
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot> wal = WalBytes is null
                ? DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.WithoutValue(DiagnosticsAvailability.Unavailable)
                : DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.Available(new WalRuntimeDiagnosticsSnapshot(
                    metadata,
                    LogicalBytes: WalBytes,
                    AllocatedBytes: WalBytes,
                    CommittedFrameBytes: WalBytes,
                    RetainedBytes: 0,
                    FrameCount: 0,
                    FlushCount: 0,
                    BytesWritten: WalBytes,
                    PendingCommitCount: 0,
                    CheckpointPhase.Idle,
                    LastSuccessfulFlushAtUtc: null,
                    LastSuccessfulCheckpointAtUtc: null,
                    LastError: null));
            return new RuntimeDiagnosticsSnapshot(
                metadata,
                DiagnosticsSection<QueryDiagnosticsSummary>.Available(query),
                DiagnosticsSection<ConnectionDiagnosticsSnapshot>.WithoutValue(DiagnosticsAvailability.Unavailable),
                DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.WithoutValue(DiagnosticsAvailability.Unavailable),
                wal,
                DiagnosticsSection<MaintenanceOperationSnapshot>.WithoutValue(DiagnosticsAvailability.Unavailable),
                DiagnosticsSection<HealthDiagnosticsSnapshot>.WithoutValue(DiagnosticsAvailability.Unavailable));
        }

        public DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<T>> CreateEmptyCollection<T>()
            where T : class, IRuntimeDiagnosticsSnapshot
            => CollectionWithAvailability<T>(DiagnosticsAvailability.Available);

        private DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<T>> CollectionWithAvailability<T>(DiagnosticsAvailability availability)
            where T : class, IRuntimeDiagnosticsSnapshot
        {
            DiagnosticsSnapshotMetadata metadata = Metadata(
                DiagnosticsScope.Instance,
                "physical",
                availability);
            var collection = availability == DiagnosticsAvailability.Available
                ? new DiagnosticsCollectionSnapshot<T>(metadata, [], 100, null, 0, false)
                : new DiagnosticsCollectionSnapshot<T>(metadata, null, null, null, null, null);
            return InstanceTopology(collection);
        }

        public DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<T>> CreateUnavailableValue<T>()
            where T : class, IRuntimeDiagnosticsSnapshot
        {
            DiagnosticsSnapshotMetadata metadata = Metadata(
                DiagnosticsScope.Instance,
                "physical",
                DiagnosticsAvailability.Unavailable);
            return InstanceTopology(new DiagnosticsValueSnapshot<T>(metadata, null));
        }

        private DiagnosticsSnapshotMetadata Metadata(
            DiagnosticsScope scope,
            string alias,
            DiagnosticsAvailability availability)
            => new(
                CSharpDbDiagnostics.SchemaVersion,
                clock.GetUtcNow(),
                ServerInstanceId,
                CounterEpoch,
                scope,
                availability,
                DiagnosticsSource.Engine,
                alias,
                recordsTruncated: false,
                fieldsTruncated: false);

        private void RecordPoll(int maximumRecords)
        {
            Interlocked.Increment(ref _pollCallCount);
            MaximumRecordsSeen = maximumRecords;
        }

        private static DiagnosticsTopologySnapshot<T> InstanceTopology<T>(T aggregate)
            where T : class, IRuntimeDiagnosticsSnapshot
            => new(aggregate, null, null, null, null);
    }

    private sealed class ManualTimeProvider(DateTimeOffset start) : TimeProvider
    {
        private readonly object _sync = new();
        private readonly List<ManualTimer> _timers = [];
        private DateTimeOffset _utcNow = start;

        public TaskCompletionSource TimerCreated { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override DateTimeOffset GetUtcNow()
        {
            lock (_sync)
                return _utcNow;
        }

        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period)
        {
            ManualTimer timer;
            lock (_sync)
            {
                timer = new ManualTimer(this, callback, state, _utcNow + dueTime, period);
                _timers.Add(timer);
            }
            TimerCreated.TrySetResult();
            return timer;
        }

        public void Advance(TimeSpan elapsed)
        {
            ManualTimer[] due;
            lock (_sync)
            {
                _utcNow += elapsed;
                due = _timers.Where(timer => timer.IsDue(_utcNow)).ToArray();
            }
            foreach (ManualTimer timer in due)
                timer.Fire();
        }

        private sealed class ManualTimer(
            ManualTimeProvider owner,
            TimerCallback callback,
            object? state,
            DateTimeOffset dueAt,
            TimeSpan period) : ITimer
        {
            private bool _disposed;
            private DateTimeOffset _dueAt = dueAt;
            private TimeSpan _period = period;

            public bool IsDue(DateTimeOffset now) => !_disposed && now >= _dueAt;

            public void Fire()
            {
                lock (owner._sync)
                {
                    if (_disposed || owner._utcNow < _dueAt)
                        return;
                    if (_period == Timeout.InfiniteTimeSpan)
                        _disposed = true;
                    else
                        _dueAt = owner._utcNow + _period;
                }
                callback(state);
            }

            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                lock (owner._sync)
                {
                    if (_disposed)
                        return false;
                    _dueAt = owner._utcNow + dueTime;
                    _period = period;
                    return true;
                }
            }

            public void Dispose()
            {
                lock (owner._sync)
                    _disposed = true;
            }

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }
        }
    }
}
