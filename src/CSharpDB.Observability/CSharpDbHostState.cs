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
    private CSharpDbHostStateSnapshot _snapshot;

    public CSharpDbHostState(TimeProvider? timeProvider = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _snapshot = CreateSnapshot(
            CSharpDbHostLifecyclePhase.Starting,
            CSharpDbReadinessReason.Starting,
            error: null);
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
        => Transition(
            CSharpDbHostLifecyclePhase.Running,
            CSharpDbReadinessReason.None,
            error: null);

    public CSharpDbHostStateSnapshot MarkNotReady(CSharpDbReadinessReason reason)
    {
        if (reason is CSharpDbReadinessReason.Unknown or
            CSharpDbReadinessReason.None or
            CSharpDbReadinessReason.Starting or
            CSharpDbReadinessReason.Recovering or
            CSharpDbReadinessReason.InitializationFailed or
            CSharpDbReadinessReason.Stopping)
        {
            throw new ArgumentOutOfRangeException(nameof(reason));
        }

        lock (_gate)
        {
            if (_snapshot.LifecyclePhase != CSharpDbHostLifecyclePhase.Running)
            {
                throw new InvalidOperationException(
                    "Only a running host can transition to a runtime not-ready reason.");
            }

            _snapshot = CreateSnapshot(CSharpDbHostLifecyclePhase.Running, reason, error: null);
            return _snapshot;
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
        lock (_gate)
        {
            if (_snapshot.LifecyclePhase != CSharpDbHostLifecyclePhase.Stopping)
            {
                throw new InvalidOperationException(
                    "The host must enter Stopping before it can enter Stopped.");
            }

            _snapshot = CreateSnapshot(
                CSharpDbHostLifecyclePhase.Stopped,
                CSharpDbReadinessReason.Stopping,
                error: null);
            return _snapshot;
        }
    }

    private CSharpDbHostStateSnapshot Transition(
        CSharpDbHostLifecyclePhase phase,
        CSharpDbReadinessReason reason,
        SafeErrorProjection? error)
    {
        lock (_gate)
        {
            if (!CanTransition(_snapshot.LifecyclePhase, phase))
            {
                throw new InvalidOperationException(
                    $"The host cannot transition from {_snapshot.LifecyclePhase} to {phase}.");
            }

            _snapshot = CreateSnapshot(phase, reason, error);
            return _snapshot;
        }
    }

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
                next == CSharpDbHostLifecyclePhase.Stopping,
            CSharpDbHostLifecyclePhase.Stopping =>
                next == CSharpDbHostLifecyclePhase.Stopped,
            _ => false,
        };

}
