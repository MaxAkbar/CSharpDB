namespace CSharpDB.Storage.Checkpointing;

/// <summary>
/// Coordinates auto-checkpoint policy decisions and reader-aware checkpoint locking.
/// </summary>
internal sealed class CheckpointCoordinator : IDisposable
{
    private readonly SemaphoreSlim _checkpointLock = new(1, 1);
    private int _activeReaderCount;

    public int ActiveReaderCount => Volatile.Read(ref _activeReaderCount);

    public WalSnapshot AcquireReaderSnapshot(WalIndex index)
    {
        Interlocked.Increment(ref _activeReaderCount);
        return index.TakeSnapshot();
    }

    public void ReleaseReaderSnapshot()
    {
        Interlocked.Decrement(ref _activeReaderCount);
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

        return policy is FrameCountCheckpointPolicy
            ? context.CommittedFrameCount >= legacyThreshold &&
              context.ActiveReaderCount == 0
            : policy.ShouldCheckpoint(context);
    }

    public async ValueTask RunCheckpointAsync(
        int committedFrameCount,
        Func<CancellationToken, ValueTask> checkpointAction,
        CancellationToken ct = default)
    {
        if (committedFrameCount == 0 || ActiveReaderCount > 0)
            return;

        await _checkpointLock.WaitAsync(ct);
        try
        {
            await checkpointAction(ct);
        }
        finally
        {
            _checkpointLock.Release();
        }
    }

    public void Dispose() => _checkpointLock.Dispose();
}
