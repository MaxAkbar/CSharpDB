using CSharpDB.Storage.Wal;

namespace CSharpDB.Storage.Checkpointing;

/// <summary>
/// Coordinates auto-checkpoint policy decisions and reader-aware checkpoint locking.
/// </summary>
internal sealed class CheckpointCoordinator : IDisposable
{
    private readonly SemaphoreSlim _checkpointLock = new(1, 1);
    private readonly object _backgroundCheckpointGate = new();
    private readonly Dictionary<WalSnapshot, byte> _activeSnapshots = new();
    private int _activeReaderCount;
    private int _deferredCheckpointRequested;
    private long _minimumRetainedWalOffset = long.MaxValue;
    private Task? _backgroundCheckpointTask;

    public int ActiveReaderCount => Volatile.Read(ref _activeReaderCount);
    public bool HasPendingCheckpointRequest => Volatile.Read(ref _deferredCheckpointRequested) != 0;

    public WalSnapshot AcquireReaderSnapshot(WalIndex index, long? minimumWalOffset = null)
    {
        _checkpointLock.Wait();
        try
        {
            WalSnapshot snapshot = index.TakeSnapshot(minimumWalOffset);
            _activeSnapshots[snapshot] = 0;
            Volatile.Write(ref _activeReaderCount, _activeSnapshots.Count);
            RecomputeMinimumRetainedWalOffset_NoLock();
            return snapshot;
        }
        finally
        {
            _checkpointLock.Release();
        }
    }

    public bool ReleaseReaderSnapshot(WalSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);

        _checkpointLock.Wait();
        try
        {
            if (!_activeSnapshots.Remove(snapshot))
                return false;

            int activeCount = _activeSnapshots.Count;
            Volatile.Write(ref _activeReaderCount, activeCount);
            RecomputeMinimumRetainedWalOffset_NoLock();
            return activeCount == 0;
        }
        finally
        {
            _checkpointLock.Release();
        }
    }

    public bool ShouldCheckpoint(
        ICheckpointPolicy policy,
        int committedFrameCount,
        int legacyThreshold,
        long estimatedWalBytes)
    {
        bool readersRequireWalRetention = TryGetMinimumRetainedWalOffset(out _);
        var context = new PagerCheckpointContext(
            committedFrameCount,
            readersRequireWalRetention ? ActiveReaderCount : 0,
            estimatedWalBytes);

        bool shouldCheckpoint = policy is FrameCountCheckpointPolicy
            ? context.CommittedFrameCount >= legacyThreshold &&
              context.ActiveReaderCount == 0
            : policy.ShouldCheckpoint(context);

        if (!shouldCheckpoint &&
            context.ActiveReaderCount > 0 &&
            policy is FrameCountCheckpointPolicy &&
            context.CommittedFrameCount >= legacyThreshold)
        {
            Volatile.Write(ref _deferredCheckpointRequested, 1);
        }

        return shouldCheckpoint;
    }

    public void RequestDeferredCheckpoint()
    {
        Volatile.Write(ref _deferredCheckpointRequested, 1);
    }

    public bool TryConsumeDeferredCheckpointRequest()
    {
        if (TryGetMinimumRetainedWalOffset(out _))
            return false;

        return Interlocked.Exchange(ref _deferredCheckpointRequested, 0) == 1;
    }

    public void ClearDeferredCheckpointRequest()
    {
        Interlocked.Exchange(ref _deferredCheckpointRequested, 0);
    }

    public bool TryGetMinimumRetainedWalOffset(out long walOffset)
    {
        walOffset = Volatile.Read(ref _minimumRetainedWalOffset);
        return walOffset != long.MaxValue;
    }

    public async ValueTask RunCheckpointAsync(
        int committedFrameCount,
        Func<CancellationToken, ValueTask> checkpointAction,
        CancellationToken ct = default)
    {
        if (committedFrameCount == 0)
            return;

        await _checkpointLock.WaitAsync(ct);
        try
        {
            if (TryGetMinimumRetainedWalOffset(out _))
                Volatile.Write(ref _deferredCheckpointRequested, 1);

            await checkpointAction(ct);
        }
        finally
        {
            _checkpointLock.Release();
        }
    }

    public bool TryStartBackgroundCheckpoint(Func<CancellationToken, ValueTask> checkpointAction)
    {
        if (!HasPendingCheckpointRequest)
            return false;

        lock (_backgroundCheckpointGate)
        {
            if (_backgroundCheckpointTask is { IsCompleted: false })
                return false;

            _backgroundCheckpointTask = Task.Run(
                async () => await checkpointAction(CancellationToken.None));
            return true;
        }
    }

    public async ValueTask WaitForBackgroundCheckpointAsync(CancellationToken ct = default)
    {
        Task? backgroundCheckpointTask;
        lock (_backgroundCheckpointGate)
        {
            backgroundCheckpointTask = _backgroundCheckpointTask;
        }

        if (backgroundCheckpointTask is null)
            return;

        try
        {
            await backgroundCheckpointTask.WaitAsync(ct);
        }
        finally
        {
            lock (_backgroundCheckpointGate)
            {
                if (ReferenceEquals(_backgroundCheckpointTask, backgroundCheckpointTask) &&
                    backgroundCheckpointTask.IsCompleted)
                {
                    _backgroundCheckpointTask = null;
                }
            }
        }
    }

    public void Dispose() => _checkpointLock.Dispose();

    private void RecomputeMinimumRetainedWalOffset_NoLock()
    {
        long minimumRetainedWalOffset = long.MaxValue;
        foreach (WalSnapshot snapshot in _activeSnapshots.Keys)
        {
            if (!snapshot.HasWalFrames)
                continue;

            if (snapshot.MinimumWalOffset < minimumRetainedWalOffset)
                minimumRetainedWalOffset = snapshot.MinimumWalOffset;
        }

        Volatile.Write(ref _minimumRetainedWalOffset, minimumRetainedWalOffset);
    }
}
