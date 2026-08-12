using CSharpDB.Client;
using CSharpDB.Observability;

namespace CSharpDB.Admin.Services;

/// <summary>
/// Initializes the Admin database after the web listener has started and owns
/// the cached database-readiness state. Health requests never execute database
/// work or acquire the client lock.
/// </summary>
public sealed class AdminHostReadinessService(
    IServiceProvider services,
    CSharpDbHostState hostState,
    CSharpDbHealthOptions healthOptions,
    IHostApplicationLifetime applicationLifetime) : BackgroundService
{
    private static readonly TimeSpan RetryDelay =
        TimeSpan.FromMilliseconds(250);
    private IDisposable? _stoppingRegistration;
    private readonly object _readinessGate = new();
    private int _databaseSwitchLeaseCount;
    private long _readinessVersion;

    public CSharpDbHostStateSnapshot Snapshot => hostState.Snapshot;

    /// <summary>
    /// Marks the Admin host not ready while its shared database client is
    /// replaced. The lease is nestable and never performs database work.
    /// </summary>
    internal IDisposable EnterDatabaseSwitch()
    {
        lock (_readinessGate)
        {
            if (_databaseSwitchLeaseCount < int.MaxValue)
                _databaseSwitchLeaseCount++;
            _readinessVersion = unchecked(_readinessVersion + 1);
        }

        ConvergeRunningReadiness();

        return new DatabaseSwitchLease(this);
    }

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _stoppingRegistration = applicationLifetime.ApplicationStopping.Register(
            MarkStopping);
        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await WaitForApplicationStartedAsync(stoppingToken).ConfigureAwait(false);

        ICSharpDbClient client = services.GetRequiredService<ICSharpDbClient>();
        while (!stoppingToken.IsCancellationRequested)
        {
            TryMarkRecovering();
            Task? attempt = null;
            try
            {
                using var attemptCancellation =
                    CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                attemptCancellation.CancelAfter(
                    healthOptions.ReadinessTimeout);
                attempt = client.GetInfoAsync(attemptCancellation.Token);
                await attempt.WaitAsync(
                        healthOptions.ReadinessTimeout,
                        stoppingToken)
                    .ConfigureAwait(false);
                MarkInitialized();
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (TimeoutException exception)
            {
                TryMarkFailed(new TimeoutException(
                    "CSharpDB Admin database initialization exceeded the configured readiness timeout.",
                    exception));
                if (attempt is not null &&
                    !await ObserveAttemptBeforeRetryAsync(attempt, stoppingToken)
                        .ConfigureAwait(false))
                {
                    return;
                }
            }
            catch (OperationCanceledException exception)
            {
                TryMarkFailed(new TimeoutException(
                    "CSharpDB Admin database initialization exceeded the configured readiness timeout.",
                    exception));
                if (attempt is not null &&
                    !await ObserveAttemptBeforeRetryAsync(attempt, stoppingToken)
                        .ConfigureAwait(false))
                {
                    return;
                }
            }
            catch (Exception exception)
            {
                TryMarkFailed(exception);
            }

            try
            {
                await Task.Delay(RetryDelay, stoppingToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        MarkStopping();
        try
        {
            await base.StopAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            try
            {
                if (hostState.Snapshot.LifecyclePhase ==
                    CSharpDbHostLifecyclePhase.Stopping)
                {
                    hostState.MarkStopped();
                }
            }
            catch (InvalidOperationException)
            {
                // A concurrent host-shutdown callback won the transition.
            }

            _stoppingRegistration?.Dispose();
        }
    }

    private async Task WaitForApplicationStartedAsync(
        CancellationToken cancellationToken)
    {
        if (applicationLifetime.ApplicationStarted.IsCancellationRequested)
            return;

        var started = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using CancellationTokenRegistration startedRegistration =
            applicationLifetime.ApplicationStarted.Register(
                static state => ((TaskCompletionSource)state!).TrySetResult(),
                started);
        await started.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private void TryMarkRecovering()
    {
        try
        {
            if (hostState.Snapshot.LifecyclePhase is
                CSharpDbHostLifecyclePhase.Starting or
                CSharpDbHostLifecyclePhase.Failed)
            {
                hostState.MarkRecovering();
            }
        }
        catch (InvalidOperationException)
        {
            // Shutdown won the transition.
        }
    }

    private void TryMarkFailed(Exception exception)
    {
        try
        {
            if (hostState.Snapshot.LifecyclePhase is
                CSharpDbHostLifecyclePhase.Starting or
                CSharpDbHostLifecyclePhase.Recovering)
            {
                hostState.MarkFailed(SafeErrorProjector.Project(exception));
            }
        }
        catch (InvalidOperationException)
        {
            // Shutdown won the transition.
        }
    }

    private void MarkInitialized()
    {
        ConvergeRunningReadiness(allowEnterRunning: true);
    }

    private void ExitDatabaseSwitch()
    {
        lock (_readinessGate)
        {
            if (_databaseSwitchLeaseCount > 0 &&
                _databaseSwitchLeaseCount < int.MaxValue)
            {
                _databaseSwitchLeaseCount--;
            }
            _readinessVersion = unchecked(_readinessVersion + 1);
        }

        ConvergeRunningReadiness();
    }

    private void ConvergeRunningReadiness(bool allowEnterRunning = false)
    {
        while (true)
        {
            long version;
            CSharpDbReadinessReason reason;
            lock (_readinessGate)
            {
                version = _readinessVersion;
                reason = _databaseSwitchLeaseCount == 0
                    ? CSharpDbReadinessReason.None
                    : CSharpDbReadinessReason.ReopenPending;
            }

            CSharpDbHostLifecyclePhase phase =
                hostState.Snapshot.LifecyclePhase;
            if (phase != CSharpDbHostLifecyclePhase.Running &&
                !(allowEnterRunning && phase is
                    CSharpDbHostLifecyclePhase.Starting or
                    CSharpDbHostLifecyclePhase.Recovering))
            {
                return;
            }

            try
            {
                // HostState invokes external transition listeners. Never hold
                // the Admin lease gate across that publication boundary.
                hostState.MarkRunning(reason);
            }
            catch (InvalidOperationException)
            {
                // Shutdown or a concurrent failed transition won.
                return;
            }

            lock (_readinessGate)
            {
                if (_readinessVersion == version)
                    return;
            }

            allowEnterRunning = false;
        }
    }

    private static async Task<bool> ObserveAttemptBeforeRetryAsync(
        Task attempt,
        CancellationToken stoppingToken)
    {
        try
        {
            await attempt.WaitAsync(stoppingToken).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return false;
        }
        catch
        {
            // The readiness timeout is authoritative. Observe the one
            // in-flight attempt before retrying so a non-cooperative client
            // cannot create unbounded concurrent initialization calls.
            return true;
        }
    }

    private void MarkStopping()
    {
        try
        {
            CSharpDbHostLifecyclePhase phase = hostState.Snapshot.LifecyclePhase;
            if (phase is not (
                CSharpDbHostLifecyclePhase.Stopping or
                CSharpDbHostLifecyclePhase.Stopped))
            {
                hostState.MarkStopping();
            }
        }
        catch (InvalidOperationException)
        {
            // Shutdown is idempotent under concurrent lifecycle callbacks.
        }
    }

    private sealed class DatabaseSwitchLease(
        AdminHostReadinessService owner) : IDisposable
    {
        private AdminHostReadinessService? _owner = owner;

        public void Dispose()
            => Interlocked.Exchange(ref _owner, null)?.ExitDatabaseSwitch();
    }
}
