using CSharpDB.Observability;

namespace CSharpDB.Api;

/// <summary>
/// Coordinates initialization, shutdown, and temporary not-ready leases around
/// the authoritative cached host state. Reading this object never resolves,
/// opens, queries, or locks a database.
/// </summary>
public sealed class CSharpDbHostReadinessCoordinator : IDisposable
{
    private static readonly CSharpDbReadinessReason[] s_reasonPrecedence =
    [
        CSharpDbReadinessReason.ReopenPending,
        CSharpDbReadinessReason.RestoreInProgress,
        CSharpDbReadinessReason.ExclusiveMaintenance,
        CSharpDbReadinessReason.ReadOnly,
        CSharpDbReadinessReason.TimedOut,
        CSharpDbReadinessReason.Unavailable,
    ];

    private readonly object _gate = new();
    private readonly CSharpDbHostState _hostState;
    private readonly TimeProvider _timeProvider;
    private readonly DiagnosticsSource _diagnosticsSource;
    private readonly string _databaseAlias;
    private readonly string _serverInstanceId =
        CSharpDbDiagnostics.CreateServerInstanceId();
    private readonly Dictionary<CSharpDbReadinessReason, int> _leases = [];
    private readonly CSharpDbHealthMetricSource? _metricSource;
    private TaskCompletionSource _recoveryRequested = CreateSignal();
    private CSharpDbReadinessReason? _persistentReason;
    private long _recoveryVersion;
    private bool _stoppingRequested;
    private int _disposed;

    public CSharpDbHostReadinessCoordinator(
        CSharpDbHostState hostState,
        CSharpDbObservabilityOptions options,
        TimeProvider? timeProvider = null)
        : this(
            hostState,
            options,
            DiagnosticsSource.Api,
            timeProvider)
    {
    }

    internal CSharpDbHostReadinessCoordinator(
        CSharpDbHostState hostState,
        CSharpDbObservabilityOptions options,
        DiagnosticsSource diagnosticsSource,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(hostState);
        ArgumentNullException.ThrowIfNull(options);
        if (diagnosticsSource is DiagnosticsSource.Unknown ||
            !Enum.IsDefined(diagnosticsSource))
        {
            throw new ArgumentOutOfRangeException(nameof(diagnosticsSource));
        }

        _hostState = hostState;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _diagnosticsSource = diagnosticsSource;
        _databaseAlias = CSharpDbObservabilityOptions.IsValidDatabaseAlias(
            options.DatabaseAlias)
                ? options.DatabaseAlias
                : "default";
        if (options.Enabled &&
            (options.OpenTelemetry.Enabled || options.Prometheus.Enabled))
        {
            _metricSource = CSharpDbHealthMetricSource.TryCreate(
                hostState,
                _databaseAlias);
        }
    }

    public CSharpDbHostStateSnapshot Snapshot => _hostState.Snapshot;

    public bool IsReady => Snapshot.IsReady;

    public IDisposable EnterNotReady(CSharpDbReadinessReason reason)
    {
        ValidateRuntimeReason(reason);
        lock (_gate)
        {
            ThrowIfDisposed();
            _leases.TryGetValue(reason, out int count);
            _leases[reason] = count == int.MaxValue ? int.MaxValue : count + 1;
        }

        ConvergeRunningReason();

        return new ReadinessLease(this, reason);
    }

    /// <summary>
    /// Keeps readiness false and asks the background initializer to verify the
    /// database before returning the host to Ready.
    /// </summary>
    public void RequestRecovery(CSharpDbReadinessReason reason)
    {
        ValidateRuntimeReason(reason);
        TaskCompletionSource recoverySignal;
        lock (_gate)
        {
            if (_stoppingRequested)
                return;

            _persistentReason = HigherPrecedenceReason(
                _persistentReason,
                reason);
            _recoveryVersion = unchecked(_recoveryVersion + 1);
            recoverySignal = _recoveryRequested;
        }

        ConvergeRunningReason();
        recoverySignal.TrySetResult();
    }

    public HealthDiagnosticsSnapshot CaptureDiagnostics()
    {
        CSharpDbHostStateSnapshot snapshot = Snapshot;
        DiagnosticsSnapshotMetadata metadata = DiagnosticsSnapshotMetadata.Create(
            _serverInstanceId,
            counterEpoch: 0,
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            _diagnosticsSource,
            _databaseAlias,
            timeProvider: _timeProvider);
        return new HealthDiagnosticsSnapshot(
            metadata,
            snapshot.LifecyclePhase,
            snapshot.IsLive
                ? CSharpDbHealthStatus.Healthy
                : CSharpDbHealthStatus.Unhealthy,
            snapshot.IsReady
                ? CSharpDbHealthStatus.Healthy
                : CSharpDbHealthStatus.Unhealthy,
            snapshot.ReadinessReason,
            snapshot.ChangedAtUtc,
            snapshot.Error);
    }

    internal long BeginRecoveryAttempt()
    {
        long recoveryVersion;
        lock (_gate)
        {
            if (_stoppingRequested)
                return _recoveryVersion;
            recoveryVersion = _recoveryVersion;
        }

        CSharpDbHostStateSnapshot snapshot = Snapshot;
        if (snapshot.LifecyclePhase is CSharpDbHostLifecyclePhase.Starting or
            CSharpDbHostLifecyclePhase.Failed)
        {
            TryTransition(
                static state => state.MarkRecovering(),
                allowedAfterRace: static phase => phase is
                    CSharpDbHostLifecyclePhase.Recovering or
                    CSharpDbHostLifecyclePhase.Running or
                    CSharpDbHostLifecyclePhase.Stopping or
                    CSharpDbHostLifecyclePhase.Stopped);
        }

        return recoveryVersion;
    }

    internal void MarkRecovering()
        => _ = BeginRecoveryAttempt();

    internal bool TryMarkReady(long recoveryVersion)
    {
        lock (_gate)
        {
            if (_stoppingRequested)
                return false;

            // A restore or reopen request that arrived after this probe began
            // must be verified by a later probe. Never let an older successful
            // GetInfo result clear a newer not-ready condition.
            if (_recoveryVersion != recoveryVersion)
                return false;

            _persistentReason = null;
            _recoveryRequested = CreateSignal();
        }

        ConvergeRunningReason(allowEnterRunning: true);
        lock (_gate)
            return !_stoppingRequested &&
                   _recoveryVersion == recoveryVersion &&
                   Snapshot.LifecyclePhase ==
                       CSharpDbHostLifecyclePhase.Running;
    }

    internal void MarkReady()
        => _ = TryMarkReady(CurrentRecoveryVersion());

    internal void MarkFailed(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        lock (_gate)
        {
            if (_stoppingRequested)
                return;
        }

        SafeErrorProjection error = SafeErrorProjector.Project(exception);
        TryTransition(
            state => state.MarkFailed(error),
            allowedAfterRace: static phase => phase is
                CSharpDbHostLifecyclePhase.Failed or
                CSharpDbHostLifecyclePhase.Stopping or
                CSharpDbHostLifecyclePhase.Stopped);
    }

    internal Task WaitForRecoveryRequestAsync(CancellationToken cancellationToken)
    {
        Task task;
        lock (_gate)
            task = _recoveryRequested.Task;
        return task.WaitAsync(cancellationToken);
    }

    internal void MarkStopping()
    {
        TaskCompletionSource recoverySignal;
        lock (_gate)
        {
            if (_stoppingRequested)
                return;
            _stoppingRequested = true;
            recoverySignal = _recoveryRequested;
        }

        recoverySignal.TrySetResult();
        TryTransition(
            static state => state.MarkStopping(),
            allowedAfterRace: static phase => phase is
                CSharpDbHostLifecyclePhase.Stopping or
                CSharpDbHostLifecyclePhase.Stopped);
    }

    internal void MarkStopped()
    {
        MarkStopping();
        TryTransition(
            static state => state.MarkStopped(),
            allowedAfterRace: static phase =>
                phase == CSharpDbHostLifecyclePhase.Stopped);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        _metricSource?.Dispose();
    }

    private void ExitNotReady(CSharpDbReadinessReason reason)
    {
        lock (_gate)
        {
            if (!_leases.TryGetValue(reason, out int count))
                return;
            if (count > 1)
                _leases[reason] = count - 1;
            else
                _leases.Remove(reason);
        }

        ConvergeRunningReason();
    }

    private void ConvergeRunningReason(bool allowEnterRunning = false)
    {
        while (true)
        {
            CSharpDbReadinessReason reason;
            long recoveryVersion;
            lock (_gate)
            {
                if (_stoppingRequested)
                    return;
                reason = GetEffectiveReasonLocked();
                recoveryVersion = _recoveryVersion;
            }

            CSharpDbHostLifecyclePhase phase = Snapshot.LifecyclePhase;
            if (phase != CSharpDbHostLifecyclePhase.Running &&
                (!allowEnterRunning || phase is not (
                    CSharpDbHostLifecyclePhase.Starting or
                    CSharpDbHostLifecyclePhase.Recovering)))
            {
                return;
            }

            TryTransition(
                state => state.MarkRunning(reason),
                allowedAfterRace: static current => current is
                    CSharpDbHostLifecyclePhase.Running or
                    CSharpDbHostLifecyclePhase.Failed or
                    CSharpDbHostLifecyclePhase.Stopping or
                    CSharpDbHostLifecyclePhase.Stopped);

            lock (_gate)
            {
                if (_stoppingRequested ||
                    (_recoveryVersion == recoveryVersion &&
                     GetEffectiveReasonLocked() == reason))
                {
                    return;
                }
            }
        }
    }

    private CSharpDbReadinessReason GetEffectiveReasonLocked()
    {
        foreach (CSharpDbReadinessReason reason in s_reasonPrecedence)
        {
            if (_persistentReason == reason || _leases.ContainsKey(reason))
                return reason;
        }

        return CSharpDbReadinessReason.None;
    }

    private long CurrentRecoveryVersion()
    {
        lock (_gate)
            return _recoveryVersion;
    }

    private static CSharpDbReadinessReason HigherPrecedenceReason(
        CSharpDbReadinessReason? current,
        CSharpDbReadinessReason requested)
    {
        if (current is null)
            return requested;

        foreach (CSharpDbReadinessReason reason in s_reasonPrecedence)
        {
            if (reason == current || reason == requested)
                return reason;
        }

        return requested;
    }

    private void TryTransition(
        Func<CSharpDbHostState, CSharpDbHostStateSnapshot> transition,
        Func<CSharpDbHostLifecyclePhase, bool> allowedAfterRace)
    {
        try
        {
            _ = transition(_hostState);
        }
        catch (InvalidOperationException) when (
            allowedAfterRace(Snapshot.LifecyclePhase))
        {
            // Another lifecycle transition won the race. Its authoritative
            // cached state is already at, or beyond, the requested state.
        }
    }

    private void ThrowIfDisposed()
        => ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _disposed) != 0,
            this);

    private static void ValidateRuntimeReason(CSharpDbReadinessReason reason)
    {
        if (reason is CSharpDbReadinessReason.Unknown or
            CSharpDbReadinessReason.None or
            CSharpDbReadinessReason.Starting or
            CSharpDbReadinessReason.Recovering or
            CSharpDbReadinessReason.InitializationFailed or
            CSharpDbReadinessReason.Stopping ||
            !Enum.IsDefined(reason))
        {
            throw new ArgumentOutOfRangeException(nameof(reason));
        }
    }

    private static TaskCompletionSource CreateSignal()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class ReadinessLease(
        CSharpDbHostReadinessCoordinator owner,
        CSharpDbReadinessReason reason) : IDisposable
    {
        private CSharpDbHostReadinessCoordinator? _owner = owner;

        public void Dispose()
            => Interlocked.Exchange(ref _owner, null)?.ExitNotReady(reason);
    }
}
