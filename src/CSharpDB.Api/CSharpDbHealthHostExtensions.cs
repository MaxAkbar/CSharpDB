using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Observability;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace CSharpDB.Api;

public static class CSharpDbHealthHostExtensions
{
    public const string DatabaseGrpcServiceName = "csharpdb.database";

    public static IServiceCollection AddCSharpDbHealth(
        this IServiceCollection services)
        => services.AddCSharpDbHealth(DiagnosticsSource.Api);

    public static IServiceCollection AddCSharpDbHealth(
        this IServiceCollection services,
        DiagnosticsSource diagnosticsSource)
    {
        ArgumentNullException.ThrowIfNull(services);
        ValidateDiagnosticsSource(diagnosticsSource);

        services.TryAddSingleton<CSharpDbObservabilityOptions>();
        return AddHealthCore(services, diagnosticsSource);
    }

    public static IServiceCollection AddCSharpDbHealth(
        this IServiceCollection services,
        IConfiguration configuration,
        DiagnosticsSource diagnosticsSource = DiagnosticsSource.Api)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        ValidateDiagnosticsSource(diagnosticsSource);

        if (!services.Any(static descriptor =>
                !descriptor.IsKeyedService &&
                descriptor.ServiceType == typeof(CSharpDbObservabilityOptions)))
        {
            CSharpDbObservabilityOptions options = configuration
                .GetSection(CSharpDbObservabilityOptions.ConfigurationSectionName)
                .Get<CSharpDbObservabilityOptions>()
                ?? new CSharpDbObservabilityOptions();
            options.Validate();
            services.AddSingleton(options);
        }

        return AddHealthCore(services, diagnosticsSource);
    }

    public static WebApplication MapCSharpDbHealthEndpoints(
        this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);
        CSharpDbObservabilityOptions options = app.Services
            .GetRequiredService<CSharpDbObservabilityOptions>();
        options.Validate();
        app.Services.GetService<CSharpDbObservabilityRegistrationMarker>()
            ?.ValidateEffectiveOptions(options);
        if (!options.Health.Enabled)
            return app;

        CSharpDbHostRouteRegistry registry = app.Services
            .GetRequiredService<CSharpDbHostRouteRegistry>();
        ReserveHealthPath(app, registry, options.Health.LivenessPath, "liveness");
        ReserveHealthPath(app, registry, options.Health.ReadinessPath, "readiness");

        app.MapGet(
            options.Health.LivenessPath,
            static (CSharpDbHostReadinessCoordinator coordinator) =>
                MinimalHealthResult(coordinator.Snapshot.IsLive));
        app.MapGet(
            options.Health.ReadinessPath,
            static (CSharpDbHostReadinessCoordinator coordinator) =>
                MinimalHealthResult(coordinator.Snapshot.IsReady));
        return app;
    }

    private static IServiceCollection AddHealthCore(
        IServiceCollection services,
        DiagnosticsSource diagnosticsSource)
    {
        services.TryAddSingleton<CSharpDbHostRouteRegistry>();
        services.TryAddSingleton<CSharpDbHostState>();
        services.TryAddSingleton(
            new CSharpDbHealthHostRegistration(diagnosticsSource));
        services.TryAddSingleton(static serviceProvider =>
        {
            var registration = serviceProvider.GetRequiredService<
                CSharpDbHealthHostRegistration>();
            return new CSharpDbHostReadinessCoordinator(
                serviceProvider.GetRequiredService<CSharpDbHostState>(),
                serviceProvider.GetRequiredService<CSharpDbObservabilityOptions>(),
                registration.DiagnosticsSource,
                serviceProvider.GetService<TimeProvider>());
        });
        services.TryAddEnumerable(ServiceDescriptor.Singleton<
            IHostedService,
            CSharpDbHostInitializer>());
        return services;
    }

    private static IResult MinimalHealthResult(bool healthy)
        => Results.Json(
            new CSharpDbMinimalHealthResponse(
                healthy ? "healthy" : "unhealthy"),
            statusCode: healthy
                ? StatusCodes.Status200OK
                : StatusCodes.Status503ServiceUnavailable);

    private static void ReserveHealthPath(
        WebApplication app,
        CSharpDbHostRouteRegistry registry,
        string path,
        string kind)
    {
        registry.ThrowIfCollides(path, $"CSharpDB {kind} health endpoint");
        ThrowIfEndpointAlreadyUsesPath(app, path, kind);
        registry.ReserveExact(path, $"CSharpDB {kind} health endpoint");
    }

    private static void ThrowIfEndpointAlreadyUsesPath(
        WebApplication app,
        string path,
        string kind)
    {
        string normalized = path.TrimEnd('/');
        foreach (EndpointDataSource source in
                 ((IEndpointRouteBuilder)app).DataSources)
        {
            foreach (RouteEndpoint endpoint in source.Endpoints
                         .OfType<RouteEndpoint>())
            {
                string? rawText = endpoint.RoutePattern.RawText;
                if (rawText is null)
                    continue;
                string existing = rawText.StartsWith('/')
                    ? rawText.TrimEnd('/')
                    : "/" + rawText.TrimEnd('/');
                if (string.Equals(
                        normalized,
                        existing,
                        StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException(
                        $"The CSharpDB {kind} health path collides with an existing endpoint route.");
                }
            }
        }
    }

    private static void ValidateDiagnosticsSource(
        DiagnosticsSource diagnosticsSource)
    {
        if (diagnosticsSource is DiagnosticsSource.Unknown ||
            !Enum.IsDefined(diagnosticsSource))
        {
            throw new ArgumentOutOfRangeException(nameof(diagnosticsSource));
        }
    }

    private sealed record CSharpDbMinimalHealthResponse(string Status);
}

internal sealed record CSharpDbHealthHostRegistration(
    DiagnosticsSource DiagnosticsSource);

internal sealed class CSharpDbHostInitializer(
    IServiceProvider services,
    CSharpDbHostReadinessCoordinator coordinator,
    CSharpDbObservabilityOptions options,
    IHostApplicationLifetime applicationLifetime) : BackgroundService
{
    private const int MaximumConcurrentInitializationAttempts = 2;
    private static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(250);
    private IDisposable? _stoppingRegistration;

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _stoppingRegistration = applicationLifetime.ApplicationStopping.Register(
            coordinator.MarkStopping);
        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var timedOutAttempts = new List<TrackedInitializationAttempt>();
        try
        {
            await WaitForApplicationStartedAsync(
                    applicationLifetime,
                    stoppingToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return;
        }

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await WaitForAttemptSlotAsync(
                            timedOutAttempts,
                            stoppingToken)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (
                    stoppingToken.IsCancellationRequested)
                {
                    break;
                }

                long recoveryVersion = coordinator.BeginRecoveryAttempt();
                if (await TryInitializeAsync(timedOutAttempts, stoppingToken)
                        .ConfigureAwait(false))
                {
                    if (!coordinator.TryMarkReady(recoveryVersion))
                        continue;

                    try
                    {
                        await coordinator.WaitForRecoveryRequestAsync(stoppingToken)
                            .ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (
                        stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }

                    continue;
                }

                try
                {
                    await Task.Delay(RetryDelay, stoppingToken)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (
                    stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
        finally
        {
            foreach (TrackedInitializationAttempt attempt in timedOutAttempts)
            {
                await attempt.DisposeScopeAsync().ConfigureAwait(false);
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        coordinator.MarkStopping();
        try
        {
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            coordinator.MarkStopped();
            _stoppingRegistration?.Dispose();
        }
    }

    private async Task<bool> TryInitializeAsync(
        List<TrackedInitializationAttempt> timedOutAttempts,
        CancellationToken stoppingToken)
    {
        AsyncServiceScope scope = services.CreateAsyncScope();
        TrackedInitializationAttempt? trackedAttempt = null;
        bool scopeTransferred = false;
        try
        {
            ICSharpDbClient client = scope.ServiceProvider
                .GetRequiredService<ICSharpDbClient>();
            using var attemptCancellation = CancellationTokenSource
                .CreateLinkedTokenSource(stoppingToken);
            attemptCancellation.CancelAfter(options.Health.ReadinessTimeout);

            Task<DatabaseInfo> attempt = client.GetInfoAsync(
                attemptCancellation.Token);
            trackedAttempt = new TrackedInitializationAttempt(attempt, scope);
            scopeTransferred = true;
            try
            {
                _ = await attempt.WaitAsync(
                        options.Health.ReadinessTimeout,
                        stoppingToken)
                    .ConfigureAwait(false);
                return true;
            }
            catch (TimeoutException exception)
            {
                attemptCancellation.Cancel();
                coordinator.MarkFailed(new TimeoutException(
                    "CSharpDB database initialization exceeded the configured readiness timeout.",
                    exception));
                TrackTimedOutAttempt(trackedAttempt, timedOutAttempts);
                return false;
            }
            catch (OperationCanceledException) when (
                !stoppingToken.IsCancellationRequested &&
                attemptCancellation.IsCancellationRequested)
            {
                coordinator.MarkFailed(new TimeoutException(
                    "CSharpDB database initialization exceeded the configured readiness timeout."));
                TrackTimedOutAttempt(trackedAttempt, timedOutAttempts);
                return false;
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return false;
        }
        catch (Exception exception)
        {
            coordinator.MarkFailed(exception);
            return false;
        }
        finally
        {
            if (!scopeTransferred)
                await scope.DisposeAsync().ConfigureAwait(false);
            else if (trackedAttempt is not null &&
                     trackedAttempt.Attempt.IsCompleted)
                await trackedAttempt.DisposeScopeAsync().ConfigureAwait(false);
        }
    }

    private static void TrackTimedOutAttempt(
        TrackedInitializationAttempt attempt,
        List<TrackedInitializationAttempt> timedOutAttempts)
    {
        if (!attempt.Completion.IsCompleted)
            timedOutAttempts.Add(attempt);
    }

    private static async Task WaitForAttemptSlotAsync(
        List<TrackedInitializationAttempt> timedOutAttempts,
        CancellationToken stoppingToken)
    {
        timedOutAttempts.RemoveAll(static attempt => attempt.Completion.IsCompleted);
        if (timedOutAttempts.Count < MaximumConcurrentInitializationAttempts)
            return;

        // A non-cooperative client cannot be forcibly stopped. Bound the
        // damage to two in-flight probes: readiness changes at the configured
        // deadline, terminal faults are observed, and another retry begins as
        // soon as either timed-out call completes.
        _ = await Task.WhenAny(timedOutAttempts.Select(static item => item.Completion))
            .WaitAsync(stoppingToken)
            .ConfigureAwait(false);
        timedOutAttempts.RemoveAll(static attempt => attempt.Completion.IsCompleted);
    }

    private static async Task WaitForApplicationStartedAsync(
        IHostApplicationLifetime applicationLifetime,
        CancellationToken stoppingToken)
    {
        if (applicationLifetime.ApplicationStarted.IsCancellationRequested)
            return;

        var started = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using CancellationTokenRegistration registration =
            applicationLifetime.ApplicationStarted.Register(
                static state => ((TaskCompletionSource)state!).TrySetResult(),
                started);
        await started.Task.WaitAsync(stoppingToken).ConfigureAwait(false);
    }

    private sealed class TrackedInitializationAttempt
    {
        private readonly object _gate = new();
        private AsyncServiceScope? _scope;

        internal TrackedInitializationAttempt(
            Task<DatabaseInfo> attempt,
            AsyncServiceScope scope)
        {
            Attempt = attempt;
            _scope = scope;
            Completion = ObserveAndDisposeAsync();
        }

        internal Task<DatabaseInfo> Attempt { get; }
        internal Task Completion { get; }

        internal async ValueTask DisposeScopeAsync()
        {
            AsyncServiceScope? scope;
            lock (_gate)
            {
                scope = _scope;
                _scope = null;
            }

            if (scope.HasValue)
                await scope.Value.DisposeAsync().ConfigureAwait(false);
        }

        private async Task ObserveAndDisposeAsync()
        {
            try
            {
                _ = await Attempt.ConfigureAwait(false);
            }
            catch
            {
                // Initialization state reports the bounded public failure.
                // Observe the terminal task exception after a timeout.
            }
            finally
            {
                await DisposeScopeAsync().ConfigureAwait(false);
            }
        }
    }
}
