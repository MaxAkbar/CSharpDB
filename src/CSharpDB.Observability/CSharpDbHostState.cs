using System.Text.Json.Serialization;

namespace CSharpDB.Observability;

public sealed record CSharpDbHostStateSnapshot
{
    [JsonConstructor]
    public CSharpDbHostStateSnapshot(
        CSharpDbHostLifecyclePhase lifecyclePhase,
        bool isLive,
        bool isReady,
        CSharpDbReadinessReason readinessReason,
        DateTimeOffset changedAtUtc,
        SafeErrorProjection? error)
    {
        ValidateCombination(lifecyclePhase, readinessReason, error);
        bool expectedLive = lifecyclePhase != CSharpDbHostLifecyclePhase.Stopped;
        bool expectedReady = lifecyclePhase == CSharpDbHostLifecyclePhase.Running &&
                             readinessReason == CSharpDbReadinessReason.None;
        if (isLive != expectedLive || isReady != expectedReady)
        {
            throw new ArgumentException(
                "The host lifecycle phase, liveness, and readiness flags are inconsistent.");
        }
        if (changedAtUtc.Offset != TimeSpan.Zero)
            throw new ArgumentException("The host state timestamp must be UTC.", nameof(changedAtUtc));

        LifecyclePhase = lifecyclePhase;
        IsLive = isLive;
        IsReady = isReady;
        ReadinessReason = readinessReason;
        ChangedAtUtc = changedAtUtc;
        Error = error;
    }

    public CSharpDbHostLifecyclePhase LifecyclePhase { get; }
    public bool IsLive { get; }
    public bool IsReady { get; }
    public CSharpDbReadinessReason ReadinessReason { get; }
    public DateTimeOffset ChangedAtUtc { get; }
    public SafeErrorProjection? Error { get; }

    internal static void ValidateCombination(
        CSharpDbHostLifecyclePhase phase,
        CSharpDbReadinessReason reason,
        SafeErrorProjection? error)
    {
        bool valid = phase switch
        {
            CSharpDbHostLifecyclePhase.Starting =>
                reason == CSharpDbReadinessReason.Starting && error is null,
            CSharpDbHostLifecyclePhase.Recovering =>
                reason == CSharpDbReadinessReason.Recovering && error is null,
            CSharpDbHostLifecyclePhase.Running =>
                reason is not (
                    CSharpDbReadinessReason.Unknown or
                    CSharpDbReadinessReason.Starting or
                    CSharpDbReadinessReason.Recovering or
                    CSharpDbReadinessReason.InitializationFailed or
                    CSharpDbReadinessReason.Stopping) &&
                Enum.IsDefined(reason) &&
                error is null,
            CSharpDbHostLifecyclePhase.Failed =>
                reason == CSharpDbReadinessReason.InitializationFailed && error is not null,
            CSharpDbHostLifecyclePhase.Stopping or CSharpDbHostLifecyclePhase.Stopped =>
                reason == CSharpDbReadinessReason.Stopping && error is null,
            _ => false,
        };

        if (!valid)
            throw new ArgumentException("The host lifecycle phase, readiness reason, and error are inconsistent.");
    }
}

/// <summary>
/// Thread-safe host lifecycle state that exists before a Database instance.
/// Database failure does not make the running process non-live.
/// </summary>
public sealed class CSharpDbHostState
{
    private readonly object _gate = new();
    private readonly TimeProvider _timeProvider;
    private readonly Action<CSharpDbHostStateSnapshot> _transitionObserver;
    private readonly Queue<CSharpDbHostStateSnapshot> _pendingNotifications = new();
    private CSharpDbHostStateSnapshot _snapshot;
    private bool _notificationDrainActive;

    public CSharpDbHostState(TimeProvider? timeProvider = null)
        : this(timeProvider, PublishHealthTransition)
    {
    }

    /// <summary>
    /// Creates host state with a best-effort transition observer. The observer
    /// receives the initial <see cref="CSharpDbHostLifecyclePhase.Starting"/>
    /// state and each distinct later transition in commit order. Observer
    /// failures are isolated from lifecycle state changes.
    /// </summary>
    public CSharpDbHostState(
        TimeProvider? timeProvider,
        Action<CSharpDbHostStateSnapshot> transitionObserver)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _transitionObserver = transitionObserver ??
            throw new ArgumentNullException(nameof(transitionObserver));
        _snapshot = CreateSnapshot(
            CSharpDbHostLifecyclePhase.Starting,
            CSharpDbReadinessReason.Starting,
            error: null);
        _pendingNotifications.Enqueue(_snapshot);
        _notificationDrainActive = true;
        DrainNotifications();
    }

    public CSharpDbHostStateSnapshot Snapshot
    {
        get
        {
            lock (_gate)
                return _snapshot;
        }
    }

    public CSharpDbHostStateSnapshot MarkRecovering()
        => Transition(
            CSharpDbHostLifecyclePhase.Recovering,
            CSharpDbReadinessReason.Recovering,
            error: null);

    public CSharpDbHostStateSnapshot MarkReady()
        => MarkRunning();

    /// <summary>
    /// Enters the running phase with either ready state or one reviewed
    /// runtime not-ready reason. This allows initialization to publish its
    /// first running state atomically without an intermediate ready transition.
    /// </summary>
    public CSharpDbHostStateSnapshot MarkRunning(
        CSharpDbReadinessReason reason = CSharpDbReadinessReason.None)
    {
        ValidateRunningReason(reason, nameof(reason));
        return Transition(
            CSharpDbHostLifecyclePhase.Running,
            reason,
            error: null);
    }

    public CSharpDbHostStateSnapshot MarkNotReady(CSharpDbReadinessReason reason)
    {
        ValidateRunningReason(reason, nameof(reason));
        if (reason == CSharpDbReadinessReason.None)
            throw new ArgumentOutOfRangeException(nameof(reason));

        bool drainNotifications;
        CSharpDbHostStateSnapshot snapshot;
        lock (_gate)
        {
            if (_snapshot.LifecyclePhase != CSharpDbHostLifecyclePhase.Running)
            {
                throw new InvalidOperationException(
                    "Only a running host can transition to a runtime not-ready reason.");
            }

            (snapshot, drainNotifications) = SetSnapshotLocked(
                CSharpDbHostLifecyclePhase.Running,
                reason,
                error: null);
        }

        if (drainNotifications)
            DrainNotifications();
        return snapshot;
    }

    private static void ValidateRunningReason(
        CSharpDbReadinessReason reason,
        string parameterName)
    {
        if (reason is CSharpDbReadinessReason.Unknown or
            CSharpDbReadinessReason.Starting or
            CSharpDbReadinessReason.Recovering or
            CSharpDbReadinessReason.InitializationFailed or
            CSharpDbReadinessReason.Stopping ||
            !Enum.IsDefined(reason))
        {
            throw new ArgumentOutOfRangeException(parameterName);
        }
    }

    public CSharpDbHostStateSnapshot MarkFailed(SafeErrorProjection error)
    {
        ArgumentNullException.ThrowIfNull(error);
        return Transition(
            CSharpDbHostLifecyclePhase.Failed,
            CSharpDbReadinessReason.InitializationFailed,
            error);
    }

    public CSharpDbHostStateSnapshot MarkStopping()
        => Transition(
            CSharpDbHostLifecyclePhase.Stopping,
            CSharpDbReadinessReason.Stopping,
            error: null);

    public CSharpDbHostStateSnapshot MarkStopped()
    {
        bool drainNotifications;
        CSharpDbHostStateSnapshot snapshot;
        lock (_gate)
        {
            if (_snapshot.LifecyclePhase != CSharpDbHostLifecyclePhase.Stopping)
            {
                throw new InvalidOperationException(
                    "The host must enter Stopping before it can enter Stopped.");
            }

            (snapshot, drainNotifications) = SetSnapshotLocked(
                CSharpDbHostLifecyclePhase.Stopped,
                CSharpDbReadinessReason.Stopping,
                error: null);
        }

        if (drainNotifications)
            DrainNotifications();
        return snapshot;
    }

    private CSharpDbHostStateSnapshot Transition(
        CSharpDbHostLifecyclePhase phase,
        CSharpDbReadinessReason reason,
        SafeErrorProjection? error)
    {
        bool drainNotifications;
        CSharpDbHostStateSnapshot snapshot;
        lock (_gate)
        {
            if (!CanTransition(_snapshot.LifecyclePhase, phase))
            {
                throw new InvalidOperationException(
                    $"The host cannot transition from {_snapshot.LifecyclePhase} to {phase}.");
            }

            (snapshot, drainNotifications) = SetSnapshotLocked(
                phase,
                reason,
                error);
        }

        if (drainNotifications)
            DrainNotifications();
        return snapshot;
    }

    private (CSharpDbHostStateSnapshot Snapshot, bool DrainNotifications)
        SetSnapshotLocked(
            CSharpDbHostLifecyclePhase phase,
            CSharpDbReadinessReason reason,
            SafeErrorProjection? error)
    {
        if (_snapshot.LifecyclePhase == phase &&
            _snapshot.ReadinessReason == reason &&
            Equals(_snapshot.Error, error))
        {
            return (_snapshot, false);
        }

        _snapshot = CreateSnapshot(phase, reason, error);
        _pendingNotifications.Enqueue(_snapshot);
        if (_notificationDrainActive)
            return (_snapshot, false);

        _notificationDrainActive = true;
        return (_snapshot, true);
    }

    private void DrainNotifications()
    {
        while (true)
        {
            CSharpDbHostStateSnapshot notification;
            lock (_gate)
            {
                if (_pendingNotifications.Count == 0)
                {
                    _notificationDrainActive = false;
                    return;
                }

                notification = _pendingNotifications.Dequeue();
            }

            try
            {
                _transitionObserver(notification);
            }
            catch
            {
                // Health publication is best effort and cannot change host
                // lifecycle, readiness, or shutdown behavior.
            }
        }
    }

    private static void PublishHealthTransition(
        CSharpDbHostStateSnapshot snapshot)
        => CSharpDbDiagnostics.EventPublisher.Publish(
            CSharpDbLogEvents.HealthTransition,
            snapshot,
            static state => new CSharpDbHealthTransitionEvent(state));

    private CSharpDbHostStateSnapshot CreateSnapshot(
        CSharpDbHostLifecyclePhase phase,
        CSharpDbReadinessReason reason,
        SafeErrorProjection? error)
    {
        return new CSharpDbHostStateSnapshot(
            phase,
            isLive: phase != CSharpDbHostLifecyclePhase.Stopped,
            isReady: phase == CSharpDbHostLifecyclePhase.Running &&
                     reason == CSharpDbReadinessReason.None,
            reason,
            _timeProvider.GetUtcNow(),
            error);
    }

    private static bool CanTransition(
        CSharpDbHostLifecyclePhase current,
        CSharpDbHostLifecyclePhase next)
        => current switch
        {
            CSharpDbHostLifecyclePhase.Starting => next is
                CSharpDbHostLifecyclePhase.Recovering or
                CSharpDbHostLifecyclePhase.Running or
                CSharpDbHostLifecyclePhase.Failed or
                CSharpDbHostLifecyclePhase.Stopping,
            CSharpDbHostLifecyclePhase.Recovering => next is
                CSharpDbHostLifecyclePhase.Running or
                CSharpDbHostLifecyclePhase.Failed or
                CSharpDbHostLifecyclePhase.Stopping,
            CSharpDbHostLifecyclePhase.Running => next is
                CSharpDbHostLifecyclePhase.Running or
                CSharpDbHostLifecyclePhase.Failed or
                CSharpDbHostLifecyclePhase.Stopping,
            CSharpDbHostLifecyclePhase.Failed =>
                next is CSharpDbHostLifecyclePhase.Recovering or
                    CSharpDbHostLifecyclePhase.Stopping,
            CSharpDbHostLifecyclePhase.Stopping =>
                next == CSharpDbHostLifecyclePhase.Stopped,
            _ => false,
        };

}
