namespace CSharpDB.Storage.Checkpointing;

/// <summary>
/// Coordinates auto-checkpoint policy decisions and reader-aware checkpoint locking.
/// </summary>
internal sealed class CheckpointCoordinator : IDisposable
{
    private readonly SemaphoreSlim _checkpointLock = new(1, 1);
    private int _activeReaderCount;
    private int _deferredCheckpointRequested;

    public int ActiveReaderCount => Volatile.Read(ref _activeReaderCount);

    public WalSnapshot AcquireReaderSnapshot(WalIndex index)
    {
        Interlocked.Increment(ref _activeReaderCount);
        return index.TakeSnapshot();
    }

    public void ReleaseReaderSnapshot()
    {
        int activeCount = Interlocked.Decrement(ref _activeReaderCount);
        if (activeCount < 0)
        {
            Interlocked.Exchange(ref _activeReaderCount, 0);
            return;
        }
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

        if (ActiveReaderCount > 0)
        {
            Volatile.Write(ref _deferredCheckpointRequested, 1);
            return;
        }

        await _checkpointLock.WaitAsync(ct);
        try
        {
            await checkpointAction(ct);
            Interlocked.Exchange(ref _deferredCheckpointRequested, 0);
        }
        finally
        {
            _checkpointLock.Release();
        }
    }

    public void Dispose() => _checkpointLock.Dispose();
}
