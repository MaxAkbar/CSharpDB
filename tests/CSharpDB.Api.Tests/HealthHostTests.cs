using System.Net;
using System.Net.Http.Json;
using System.Reflection;
using System.Text.Json;
using CSharpDB.Api.Security;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Observability;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace CSharpDB.Api.Tests;

public sealed class HealthHostTests
{
    private const string ApiKey = "health-test-key";

    [Fact]
    public void Coordinator_UsesAuthoritativeStateAndNestedReadinessLeases()
    {
        var state = new CSharpDbHostState(
            TimeProvider.System,
            static _ => { });
        var coordinator = new CSharpDbHostReadinessCoordinator(
            state,
            DisabledObservabilityOptions());

        Assert.Same(state.Snapshot, coordinator.Snapshot);
        coordinator.MarkRecovering();
        coordinator.MarkReady();
        Assert.True(coordinator.IsReady);

        using IDisposable exclusive = coordinator.EnterNotReady(
            CSharpDbReadinessReason.ExclusiveMaintenance);
        using IDisposable restore = coordinator.EnterNotReady(
            CSharpDbReadinessReason.RestoreInProgress);
        Assert.Same(state.Snapshot, coordinator.Snapshot);
        Assert.Equal(
            CSharpDbReadinessReason.RestoreInProgress,
            coordinator.Snapshot.ReadinessReason);

        restore.Dispose();
        Assert.Equal(
            CSharpDbReadinessReason.ExclusiveMaintenance,
            coordinator.Snapshot.ReadinessReason);
        exclusive.Dispose();
        Assert.True(coordinator.IsReady);

        coordinator.MarkFailed(new InvalidOperationException("startup failed"));
        Assert.True(coordinator.Snapshot.IsLive);
        Assert.False(coordinator.IsReady);
        coordinator.MarkRecovering();
        coordinator.MarkReady();
        Assert.True(coordinator.IsReady);
    }

    [Fact]
    public void Coordinator_TransitionObserverCanReenterFromAnotherThread()
    {
        CSharpDbHostReadinessCoordinator? coordinator = null;
        int observerEntered = 0;
        bool reentrantCallCompleted = false;
        var state = new CSharpDbHostState(
            TimeProvider.System,
            snapshot =>
            {
                if (snapshot.LifecyclePhase !=
                        CSharpDbHostLifecyclePhase.Running ||
                    snapshot.ReadinessReason != CSharpDbReadinessReason.None ||
                    Interlocked.Exchange(ref observerEntered, 1) != 0)
                {
                    return;
                }

                Task reentrant = Task.Run(() => coordinator!.RequestRecovery(
                    CSharpDbReadinessReason.ReopenPending));
                reentrantCallCompleted = reentrant.Wait(TimeSpan.FromSeconds(1));
            });
        coordinator = new CSharpDbHostReadinessCoordinator(
            state,
            DisabledObservabilityOptions());

        coordinator.MarkRecovering();
        coordinator.MarkReady();

        Assert.True(reentrantCallCompleted);
        Assert.False(coordinator.IsReady);
        Assert.Equal(
            CSharpDbReadinessReason.ReopenPending,
            coordinator.Snapshot.ReadinessReason);
    }

    [Fact]
    public async Task HealthGets_ReadOnlyCachedStateAndRecoverAfterFailure()
    {
        ICSharpDbClient client = DispatchProxy.Create<
            ICSharpDbClient,
            HealthClientProxy>();
        var proxy = (HealthClientProxy)client;
        var first = NewInfoCompletion();
        var second = NewInfoCompletion();
        proxy.GetInfoHandler = (_, call) => call switch
        {
            1 => first.Task,
            2 => second.Task,
            _ => Task.FromResult(CreateInfo()),
        };

        await using WebApplication app = await StartHealthAppAsync(client);
        using HttpClient http = app.GetTestClient();
        CSharpDbHostReadinessCoordinator coordinator = app.Services
            .GetRequiredService<CSharpDbHostReadinessCoordinator>();

        await WaitUntilAsync(() => proxy.GetInfoCallCount == 1);
        first.SetException(new InvalidOperationException("database unavailable"));
        await WaitUntilAsync(() =>
            coordinator.Snapshot.LifecyclePhase ==
                CSharpDbHostLifecyclePhase.Failed);

        int beforeHealthGets = proxy.GetInfoCallCount;
        using HttpResponseMessage live = await http.GetAsync("/health/live", Ct);
        using HttpResponseMessage ready = await http.GetAsync("/health/ready", Ct);
        Assert.Equal(HttpStatusCode.OK, live.StatusCode);
        Assert.Equal(HttpStatusCode.ServiceUnavailable, ready.StatusCode);
        await AssertMinimalHealthBodyAsync(live, "healthy");
        await AssertMinimalHealthBodyAsync(ready, "unhealthy");
        Assert.Equal(beforeHealthGets, proxy.GetInfoCallCount);

        await WaitUntilAsync(() => proxy.GetInfoCallCount == 2);
        second.SetResult(CreateInfo());
        await WaitUntilAsync(() => coordinator.IsReady);

        beforeHealthGets = proxy.GetInfoCallCount;
        using HttpResponseMessage recovered = await http.GetAsync(
            "/health/ready",
            Ct);
        Assert.Equal(HttpStatusCode.OK, recovered.StatusCode);
        await AssertMinimalHealthBodyAsync(recovered, "healthy");
        Assert.Equal(beforeHealthGets, proxy.GetInfoCallCount);
    }

    [Fact]
    public async Task Initializer_ResolvesClientOnlyAfterApplicationStarted()
    {
        ICSharpDbClient client = DispatchProxy.Create<
            ICSharpDbClient,
            HealthClientProxy>();
        var proxy = (HealthClientProxy)client;
        bool resolvedBeforeStarted = false;

        WebApplicationBuilder builder = CreateBuilder();
        builder.Services.AddSingleton(DisabledObservabilityOptions());
        builder.Services.AddSingleton<ICSharpDbClient>(services =>
        {
            if (!services.GetRequiredService<IHostApplicationLifetime>()
                    .ApplicationStarted.IsCancellationRequested)
            {
                resolvedBeforeStarted = true;
            }

            return client;
        });
        builder.Services.AddCSharpDbHealth(DiagnosticsSource.Api);

        await using WebApplication app = builder.Build();
        app.MapCSharpDbHealthEndpoints();
        await app.StartAsync(Ct);
        await WaitUntilAsync(() => proxy.GetInfoCallCount > 0);

        Assert.False(resolvedBeforeStarted);
        Assert.True(app.Services
            .GetRequiredService<CSharpDbHostReadinessCoordinator>()
            .IsReady);
    }

    [Fact]
    public async Task Initializer_NonCooperativeTimeoutIsBoundedWithoutRetryStorm()
    {
        ICSharpDbClient client = DispatchProxy.Create<
            ICSharpDbClient,
            HealthClientProxy>();
        var proxy = (HealthClientProxy)client;
        var neverCompletes = NewInfoCompletion();
        proxy.GetInfoHandler = (_, _) => neverCompletes.Task;

        CSharpDbObservabilityOptions options = DisabledObservabilityOptions();
        options.Health.ReadinessTimeout = TimeSpan.FromMilliseconds(75);
        WebApplicationBuilder builder = CreateBuilder();
        builder.Services.AddSingleton(options);
        builder.Services.AddSingleton(client);
        builder.Services.AddCSharpDbHealth();

        await using WebApplication app = builder.Build();
        app.MapCSharpDbHealthEndpoints();
        await app.StartAsync(Ct);
        CSharpDbHostReadinessCoordinator coordinator = app.Services
            .GetRequiredService<CSharpDbHostReadinessCoordinator>();

        await WaitUntilAsync(() =>
            coordinator.Snapshot.LifecyclePhase ==
                CSharpDbHostLifecyclePhase.Failed);
        DateTimeOffset failedBy = DateTimeOffset.UtcNow;
        Assert.True(coordinator.Snapshot.IsLive);
        Assert.False(coordinator.IsReady);

        await WaitUntilAsync(() => proxy.GetInfoCallCount == 2);
        await Task.Delay(TimeSpan.FromMilliseconds(400), Ct);
        Assert.Equal(2, proxy.GetInfoCallCount);
        Assert.True(
            DateTimeOffset.UtcNow - failedBy < TimeSpan.FromSeconds(2),
            "Readiness did not enter Failed at the configured bounded deadline.");

        await app.StopAsync(Ct).WaitAsync(TimeSpan.FromSeconds(1), Ct);
        Assert.Equal(
            CSharpDbHostLifecyclePhase.Stopped,
            coordinator.Snapshot.LifecyclePhase);
    }

    [Fact]
    public async Task Initializer_RetainsScopedClientUntilTimedOutCallCompletes()
    {
        var clients = new List<HealthClientProxy>();
        var neverCompletes = NewInfoCompletion();
        CSharpDbObservabilityOptions options = DisabledObservabilityOptions();
        options.Health.ReadinessTimeout = TimeSpan.FromMilliseconds(75);
        WebApplicationBuilder builder = CreateBuilder();
        builder.Services.AddSingleton(options);
        builder.Services.AddScoped<ICSharpDbClient>(_ =>
        {
            ICSharpDbClient client = DispatchProxy.Create<
                ICSharpDbClient,
                HealthClientProxy>();
            var proxy = (HealthClientProxy)client;
            proxy.GetInfoHandler = (_, _) => neverCompletes.Task;
            lock (clients)
                clients.Add(proxy);
            return client;
        });
        builder.Services.AddCSharpDbHealth();

        await using WebApplication app = builder.Build();
        app.MapCSharpDbHealthEndpoints();
        await app.StartAsync(Ct);
        CSharpDbHostReadinessCoordinator coordinator = app.Services
            .GetRequiredService<CSharpDbHostReadinessCoordinator>();

        await WaitUntilAsync(() =>
            coordinator.Snapshot.LifecyclePhase ==
                CSharpDbHostLifecyclePhase.Failed);
        HealthClientProxy first;
        lock (clients)
        {
            Assert.NotEmpty(clients);
            first = clients[0];
        }
        Assert.Equal(0, first.DisposeCallCount);

        neverCompletes.SetResult(CreateInfo());
        await WaitUntilAsync(() => first.DisposeCallCount == 1);
    }

    [Fact]
    public async Task Shutdown_MarksNotReadyBeforeApplicationStoppingObservers()
    {
        ICSharpDbClient client = DispatchProxy.Create<
            ICSharpDbClient,
            HealthClientProxy>();
        WebApplicationBuilder builder = CreateBuilder();
        builder.Services.AddSingleton(DisabledObservabilityOptions());
        builder.Services.AddSingleton(client);
        builder.Services.AddCSharpDbHealth();

        await using WebApplication app = builder.Build();
        app.MapCSharpDbHealthEndpoints();
        CSharpDbHostReadinessCoordinator coordinator = app.Services
            .GetRequiredService<CSharpDbHostReadinessCoordinator>();
        CSharpDbHostStateSnapshot? observedDuringStopping = null;
        using IDisposable registration = app.Lifetime.ApplicationStopping.Register(
            () => observedDuringStopping = coordinator.Snapshot);

        await app.StartAsync(Ct);
        await WaitUntilAsync(() => coordinator.IsReady);
        await app.StopAsync(Ct);

        Assert.NotNull(observedDuringStopping);
        Assert.Equal(
            CSharpDbHostLifecyclePhase.Stopping,
            observedDuringStopping.LifecyclePhase);
        Assert.True(observedDuringStopping.IsLive);
        Assert.False(observedDuringStopping.IsReady);
        Assert.Equal(
            CSharpDbHostLifecyclePhase.Stopped,
            coordinator.Snapshot.LifecyclePhase);
        Assert.False(coordinator.Snapshot.IsLive);
    }

    [Fact]
    public async Task RecoveryRequestedDuringProbe_RequiresANewerSuccessfulProbe()
    {
        ICSharpDbClient client = DispatchProxy.Create<
            ICSharpDbClient,
            HealthClientProxy>();
        var proxy = (HealthClientProxy)client;
        var staleProbe = NewInfoCompletion();
        var recoveryProbe = NewInfoCompletion();
        proxy.GetInfoHandler = (_, call) => call switch
        {
            1 => staleProbe.Task,
            2 => recoveryProbe.Task,
            _ => Task.FromResult(CreateInfo()),
        };

        await using WebApplication app = await StartHealthAppAsync(client);
        CSharpDbHostReadinessCoordinator coordinator = app.Services
            .GetRequiredService<CSharpDbHostReadinessCoordinator>();
        await WaitUntilAsync(() => proxy.GetInfoCallCount == 1);

        coordinator.RequestRecovery(CSharpDbReadinessReason.ReopenPending);
        staleProbe.SetResult(CreateInfo());
        await WaitUntilAsync(() => proxy.GetInfoCallCount == 2);

        Assert.False(coordinator.IsReady);
        recoveryProbe.SetResult(CreateInfo());
        await WaitUntilAsync(() => coordinator.IsReady);
        Assert.Equal(2, proxy.GetInfoCallCount);
    }

    [Fact]
    public async Task AuthenticatedDetailedHealth_DoesNotResolveOrCallDatabase()
    {
        ICSharpDbClient client = DispatchProxy.Create<
            ICSharpDbClient,
            HealthClientProxy>();
        var proxy = (HealthClientProxy)client;
        var initialization = NewInfoCompletion();
        proxy.GetInfoHandler = (_, _) => initialization.Task;

        WebApplicationBuilder builder = CreateBuilder();
        builder.Services.AddSingleton(DisabledObservabilityOptions());
        builder.Services.AddSingleton(client);
        builder.Services.AddCSharpDbHealth(DiagnosticsSource.Api);
        builder.Services.AddCSharpDbRestApi(security =>
        {
            security.Mode = CSharpDbRemoteSecurityMode.ApiKey;
            security.ApiKey = ApiKey;
        });

        await using WebApplication app = builder.Build();
        app.MapCSharpDbRestApi(options =>
            options.MapDevelopmentOpenApi = false);
        app.MapCSharpDbHealthEndpoints();
        await app.StartAsync(Ct);
        await WaitUntilAsync(() => proxy.GetInfoCallCount == 1);
        using HttpClient http = app.GetTestClient();

        using HttpResponseMessage missing = await http.GetAsync(
            "/api/diagnostics/health",
            Ct);
        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            "/api/diagnostics/health");
        request.Headers.TryAddWithoutValidation(
            CSharpDbApiSecurityOptions.DefaultApiKeyHeaderName,
            ApiKey);
        using HttpResponseMessage authenticated = await http.SendAsync(request, Ct);

        Assert.Equal(HttpStatusCode.Unauthorized, missing.StatusCode);
        Assert.Equal(HttpStatusCode.OK, authenticated.StatusCode);
        string json = await authenticated.Content.ReadAsStringAsync(Ct);
        HealthDiagnosticsSnapshot? snapshot = JsonSerializer.Deserialize(
            json,
            CSharpDbObservabilityJsonContext.Default.HealthDiagnosticsSnapshot);
        Assert.NotNull(snapshot);
        Assert.Equal(DiagnosticsSource.Api, snapshot.Metadata.Source);
        Assert.Equal(CSharpDbHealthStatus.Healthy, snapshot.Liveness);
        Assert.Equal(CSharpDbHealthStatus.Unhealthy, snapshot.Readiness);
        Assert.Equal(1, proxy.GetInfoCallCount);

        initialization.SetResult(CreateInfo());
        await WaitUntilAsync(() => app.Services
            .GetRequiredService<CSharpDbHostReadinessCoordinator>()
            .IsReady);
    }

    [Fact]
    public async Task ExclusiveMaintenance_UsesReadinessLeasesOnlyForMutatingWork()
    {
        ICSharpDbClient client = DispatchProxy.Create<
            ICSharpDbClient,
            HealthClientProxy>();
        var proxy = (HealthClientProxy)client;
        var observations = new List<MaintenanceObservation>();
        proxy.MaintenanceObserved = (method, validateOnly) =>
        {
            CSharpDbHostReadinessCoordinator coordinator =
                Assert.IsType<CSharpDbHostReadinessCoordinator>(
                    proxy.Coordinator);
            lock (observations)
            {
                observations.Add(new MaintenanceObservation(
                    method,
                    validateOnly,
                    coordinator.IsReady,
                    coordinator.Snapshot.ReadinessReason));
            }
        };

        WebApplicationBuilder builder = CreateBuilder();
        builder.Services.AddSingleton(DisabledObservabilityOptions());
        builder.Services.AddSingleton(client);
        builder.Services.AddCSharpDbHealth();
        builder.Services.AddCSharpDbRestApi();

        await using WebApplication app = builder.Build();
        app.MapCSharpDbRestApi(options =>
            options.MapDevelopmentOpenApi = false);
        app.MapCSharpDbHealthEndpoints();
        proxy.Coordinator = app.Services
            .GetRequiredService<CSharpDbHostReadinessCoordinator>();
        await app.StartAsync(Ct);
        await WaitUntilAsync(() => proxy.Coordinator.IsReady);
        using HttpClient http = app.GetTestClient();

        AssertSuccess(await http.PostAsync("/api/maintenance/checkpoint", null, Ct));
        AssertSuccess(await http.PostAsJsonAsync(
            "/api/maintenance/backup",
            new BackupRequest { DestinationPath = "backup.db" },
            Ct));
        AssertSuccess(await http.PostAsJsonAsync(
            "/api/maintenance/restore",
            new RestoreRequest
            {
                SourcePath = "backup.db",
                ValidateOnly = true,
            },
            Ct));
        AssertSuccess(await http.PostAsJsonAsync(
            "/api/maintenance/restore",
            new RestoreRequest { SourcePath = "backup.db" },
            Ct));
        AssertSuccess(await http.PostAsJsonAsync(
            "/api/maintenance/migrate-foreign-keys",
            new ForeignKeyMigrationRequest { ValidateOnly = true },
            Ct));
        AssertSuccess(await http.PostAsJsonAsync(
            "/api/maintenance/migrate-foreign-keys",
            new ForeignKeyMigrationRequest(),
            Ct));
        AssertSuccess(await http.PostAsJsonAsync(
            "/api/maintenance/reindex",
            new ReindexRequest(),
            Ct));
        AssertSuccess(await http.PostAsync("/api/maintenance/vacuum", null, Ct));

        AssertObservation(
            observations,
            "CheckpointAsync",
            validateOnly: false,
            expectedReady: true,
            CSharpDbReadinessReason.None);
        AssertObservation(
            observations,
            "BackupAsync",
            validateOnly: false,
            expectedReady: true,
            CSharpDbReadinessReason.None);
        AssertObservation(
            observations,
            "RestoreAsync",
            validateOnly: true,
            expectedReady: true,
            CSharpDbReadinessReason.None);
        AssertObservation(
            observations,
            "RestoreAsync",
            validateOnly: false,
            expectedReady: false,
            CSharpDbReadinessReason.RestoreInProgress);
        AssertObservation(
            observations,
            "MigrateForeignKeysAsync",
            validateOnly: true,
            expectedReady: true,
            CSharpDbReadinessReason.None);
        AssertObservation(
            observations,
            "MigrateForeignKeysAsync",
            validateOnly: false,
            expectedReady: false,
            CSharpDbReadinessReason.ExclusiveMaintenance);
        AssertObservation(
            observations,
            "ReindexAsync",
            validateOnly: false,
            expectedReady: false,
            CSharpDbReadinessReason.ExclusiveMaintenance);
        AssertObservation(
            observations,
            "VacuumAsync",
            validateOnly: false,
            expectedReady: false,
            CSharpDbReadinessReason.ExclusiveMaintenance);
        Assert.True(proxy.Coordinator.IsReady);
    }

    [Fact]
    public async Task HealthRoutes_RejectCollisionsInEitherMappingOrder()
    {
        CSharpDbObservabilityOptions options = DisabledObservabilityOptions();
        options.Health.LivenessPath = "/api/info";

        await using WebApplication restFirst = CreateUnstartedApp(options);
        restFirst.MapCSharpDbRestApi(host =>
            host.MapDevelopmentOpenApi = false);
        Assert.Throws<InvalidOperationException>(
            restFirst.MapCSharpDbHealthEndpoints);

        await using WebApplication healthFirst = CreateUnstartedApp(options);
        healthFirst.MapCSharpDbHealthEndpoints();
        Assert.Throws<InvalidOperationException>(() =>
            healthFirst.MapCSharpDbRestApi(host =>
                host.MapDevelopmentOpenApi = false));
    }

    [Fact]
    public async Task HealthMapping_RejectsPostRegistrationTimeoutMutation()
    {
        var options = new CSharpDbObservabilityOptions();
        WebApplicationBuilder builder = CreateBuilder();
        builder.Services.AddSingleton(options);
        builder.Services.AddCSharpDbObservability(
            new ConfigurationBuilder().Build());
        builder.Services.AddCSharpDbHealth();
        options.Health.ReadinessTimeout = TimeSpan.FromSeconds(3);

        await using WebApplication app = builder.Build();
        InvalidOperationException error = Assert.Throws<InvalidOperationException>(
            app.MapCSharpDbHealthEndpoints);

        Assert.Contains(
            "must not be replaced or mutated",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    private static WebApplication CreateUnstartedApp(
        CSharpDbObservabilityOptions options)
    {
        WebApplicationBuilder builder = CreateBuilder();
        builder.Services.AddSingleton(options);
        builder.Services.AddCSharpDbHealth();
        builder.Services.AddCSharpDbRestApi();
        return builder.Build();
    }

    private static async Task<WebApplication> StartHealthAppAsync(
        ICSharpDbClient client)
    {
        WebApplicationBuilder builder = CreateBuilder();
        builder.Services.AddSingleton(DisabledObservabilityOptions());
        builder.Services.AddSingleton(client);
        builder.Services.AddCSharpDbHealth(DiagnosticsSource.Api);
        WebApplication app = builder.Build();
        app.MapCSharpDbHealthEndpoints();
        await app.StartAsync(Ct);
        return app;
    }

    private static WebApplicationBuilder CreateBuilder()
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder(
            new WebApplicationOptions { EnvironmentName = "Testing" });
        builder.WebHost.UseTestServer();
        return builder;
    }

    private static CSharpDbObservabilityOptions DisabledObservabilityOptions()
        => new()
        {
            Enabled = false,
            Health = new CSharpDbHealthOptions
            {
                Enabled = true,
                ReadinessTimeout = TimeSpan.FromSeconds(1),
            },
        };

    private static TaskCompletionSource<DatabaseInfo> NewInfoCompletion()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static DatabaseInfo CreateInfo()
        => new() { DataSource = "health-test" };

    private static async Task AssertMinimalHealthBodyAsync(
        HttpResponseMessage response,
        string expectedStatus)
    {
        string json = await response.Content.ReadAsStringAsync(Ct);
        using JsonDocument document = JsonDocument.Parse(json);
        JsonProperty property = Assert.Single(
            document.RootElement.EnumerateObject());
        Assert.Equal("status", property.Name);
        Assert.Equal(expectedStatus, property.Value.GetString());
    }

    private static void AssertSuccess(HttpResponseMessage response)
    {
        using (response)
            Assert.True(response.IsSuccessStatusCode, response.StatusCode.ToString());
    }

    private static void AssertObservation(
        IEnumerable<MaintenanceObservation> observations,
        string method,
        bool validateOnly,
        bool expectedReady,
        CSharpDbReadinessReason expectedReason)
    {
        MaintenanceObservation observation = Assert.Single(
            observations,
            candidate => candidate.Method == method &&
                         candidate.ValidateOnly == validateOnly);
        Assert.Equal(expectedReady, observation.IsReady);
        Assert.Equal(expectedReason, observation.Reason);
    }

    private static async Task WaitUntilAsync(Func<bool> predicate)
    {
        DateTimeOffset deadline = DateTimeOffset.UtcNow.AddSeconds(5);
        while (!predicate() && DateTimeOffset.UtcNow < deadline)
            await Task.Delay(TimeSpan.FromMilliseconds(10), Ct);
        Assert.True(predicate(), "The expected asynchronous state was not reached.");
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private sealed record MaintenanceObservation(
        string Method,
        bool ValidateOnly,
        bool IsReady,
        CSharpDbReadinessReason Reason);

    public class HealthClientProxy : DispatchProxy
    {
        private int _getInfoCallCount;
        private int _disposeCallCount;

        public Func<CancellationToken, int, Task<DatabaseInfo>>? GetInfoHandler
        {
            get;
            set;
        }

        public Action<string, bool>? MaintenanceObserved { get; set; }

        public CSharpDbHostReadinessCoordinator? Coordinator { get; set; }

        public int GetInfoCallCount => Volatile.Read(ref _getInfoCallCount);
        public int DisposeCallCount => Volatile.Read(ref _disposeCallCount);

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            string method = targetMethod?.Name ?? string.Empty;
            return method switch
            {
                "get_DataSource" => "health-test",
                "GetInfoAsync" => GetInfo((CancellationToken)args![0]!),
                "CheckpointAsync" => ObserveCheckpoint(
                    (CancellationToken)args![0]!),
                "BackupAsync" => ObserveBackup(
                    (CancellationToken)args![1]!),
                "RestoreAsync" => ObserveRestore(
                    (RestoreRequest)args![0]!,
                    (CancellationToken)args[1]!),
                "MigrateForeignKeysAsync" => ObserveForeignKeys(
                    (ForeignKeyMigrationRequest)args![0]!,
                    (CancellationToken)args[1]!),
                "ReindexAsync" => ObserveReindex(
                    (CancellationToken)args![1]!),
                "VacuumAsync" => ObserveVacuum(
                    (CancellationToken)args![0]!),
                "DisposeAsync" => DisposeClient(),
                _ => throw new NotSupportedException(method),
            };
        }

        private Task<DatabaseInfo> GetInfo(CancellationToken cancellationToken)
        {
            int call = Interlocked.Increment(ref _getInfoCallCount);
            return GetInfoHandler?.Invoke(cancellationToken, call) ??
                Task.FromResult(CreateInfo());
        }

        private Task ObserveCheckpoint(CancellationToken cancellationToken)
        {
            MaintenanceObserved?.Invoke("CheckpointAsync", false);
            return Task.CompletedTask;
        }

        private Task<BackupResult> ObserveBackup(CancellationToken cancellationToken)
        {
            MaintenanceObserved?.Invoke("BackupAsync", false);
            return Task.FromResult(new BackupResult
            {
                SourcePath = "database.db",
                DestinationPath = "backup.db",
                Sha256 = string.Empty,
            });
        }

        private Task<RestoreResult> ObserveRestore(
            RestoreRequest request,
            CancellationToken cancellationToken)
        {
            MaintenanceObserved?.Invoke("RestoreAsync", request.ValidateOnly);
            return Task.FromResult(new RestoreResult
            {
                SourcePath = request.SourcePath,
                ValidateOnly = request.ValidateOnly,
            });
        }

        private Task<ForeignKeyMigrationResult> ObserveForeignKeys(
            ForeignKeyMigrationRequest request,
            CancellationToken cancellationToken)
        {
            MaintenanceObserved?.Invoke(
                "MigrateForeignKeysAsync",
                request.ValidateOnly);
            return Task.FromResult(new ForeignKeyMigrationResult
            {
                ValidateOnly = request.ValidateOnly,
                Succeeded = true,
            });
        }

        private Task<ReindexResult> ObserveReindex(
            CancellationToken cancellationToken)
        {
            MaintenanceObserved?.Invoke("ReindexAsync", false);
            return Task.FromResult(new ReindexResult
            {
                Scope = ReindexScope.All,
            });
        }

        private Task<VacuumResult> ObserveVacuum(
            CancellationToken cancellationToken)
        {
            MaintenanceObserved?.Invoke("VacuumAsync", false);
            return Task.FromResult(new VacuumResult());
        }

        private ValueTask DisposeClient()
        {
            Interlocked.Increment(ref _disposeCallCount);
            return ValueTask.CompletedTask;
        }
    }
}
