namespace CSharpDB.Storage.Checkpointing;

/// <summary>
/// Coordinates auto-checkpoint policy decisions and reader-aware checkpoint locking.
/// </summary>
internal sealed class CheckpointCoordinator : IDisposable
{
    private readonly SemaphoreSlim _checkpointLock = new(1, 1);
    private readonly object _backgroundCheckpointGate = new();
    private int _activeReaderCount;
    private int _deferredCheckpointRequested;
    private Task? _backgroundCheckpointTask;

    public int ActiveReaderCount => Volatile.Read(ref _activeReaderCount);
    public bool HasPendingCheckpointRequest => Volatile.Read(ref _deferredCheckpointRequested) != 0;

    public WalSnapshot AcquireReaderSnapshot(WalIndex index)
    {
        _checkpointLock.Wait();
        try
        {
            Interlocked.Increment(ref _activeReaderCount);
            return index.TakeSnapshot();
        }
        finally
        {
            _checkpointLock.Release();
        }
    }

    public bool ReleaseReaderSnapshot()
    {
        int activeCount = Interlocked.Decrement(ref _activeReaderCount);
        if (activeCount < 0)
        {
            Interlocked.Exchange(ref _activeReaderCount, 0);
            return false;
        }

        return activeCount == 0;
    }

    public bool ShouldCheckpoint(
        ICheckpointPolicy policy,
        int committedFrameCount,
        int legacyThreshold,
        long estimatedWalBytes)
    {
        var context = new PagerCheckpointContext(
            committedFrameCount,
            ActiveReaderCount,
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
        if (ActiveReaderCount != 0)
            return false;

        return Interlocked.Exchange(ref _deferredCheckpointRequested, 0) == 1;
    }

    public void ClearDeferredCheckpointRequest()
    {
        Interlocked.Exchange(ref _deferredCheckpointRequested, 0);
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
            if (ActiveReaderCount > 0)
            {
                Volatile.Write(ref _deferredCheckpointRequested, 1);
                return;
            }

            await checkpointAction(ct);
        }
        finally
        {
            _checkpointLock.Release();
        }
    }

    public bool TryStartBackgroundCheckpoint(Func<CancellationToken, ValueTask> checkpointAction)
    {
        if (ActiveReaderCount > 0 || !HasPendingCheckpointRequest)
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
}
