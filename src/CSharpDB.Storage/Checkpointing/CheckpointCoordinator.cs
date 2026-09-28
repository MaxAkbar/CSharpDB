using CSharpDB.Storage.Diagnostics;
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
    private int _runtimePhase = (int)StorageCheckpointPhaseRaw.Idle;
    private long _minimumRetainedWalOffset = long.MaxValue;
    private Task? _backgroundCheckpointTask;
    private bool _backgroundCheckpointStartsStopped;

    public int ActiveReaderCount => Volatile.Read(ref _activeReaderCount);
    public bool HasPendingCheckpointRequest => Volatile.Read(ref _deferredCheckpointRequested) != 0;
    internal StorageCheckpointPhaseRaw RuntimePhase =>
        (StorageCheckpointPhaseRaw)Volatile.Read(ref _runtimePhase);

    internal void SetRuntimePhase(StorageCheckpointPhaseRaw phase)
    {
        Volatile.Write(ref _runtimePhase, (int)phase);
    }

    internal void SetRuntimePhaseUnlessFaulted(StorageCheckpointPhaseRaw phase)
    {
        while (true)
        {
            int current = Volatile.Read(ref _runtimePhase);
            if (current == (int)StorageCheckpointPhaseRaw.Faulted ||
                current == (int)phase)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref _runtimePhase, (int)phase, current) == current)
                return;
        }
    }

    public WalSnapshot AcquireReaderSnapshot(
        WalIndex index,
        long? minimumWalOffset = null,
        IWriteAheadLog? checkpointWal = null)
    {
        // Allocation can wait for the GC. Do it before taking the checkpoint lock
        // so that an allocating reader cannot block every other reader's admission.
        var snapshot = new WalSnapshot();
        _checkpointLock.Wait();
        try
        {
            long? floor = checkpointWal is null
                ? minimumWalOffset
                : checkpointWal.IsCheckpointCopyComplete &&
                  checkpointWal.TryGetCheckpointRetainedWalStartOffset(out long retainedStart)
                    ? retainedStart
                    : null;
            if (index.TryInitializeCachedSnapshot(snapshot, floor))
            {
                RegisterSnapshot_NoLock(snapshot);
                return snapshot;
            }
        }
        finally
        {
            _checkpointLock.Release();
        }

        return AcquireReaderSnapshotSlow(index, snapshot, minimumWalOffset, checkpointWal);
    }

    // Keep segmented scratch storage and preparation out of the cached admission frame.
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private WalSnapshot AcquireReaderSnapshotSlow(
        WalIndex index, WalSnapshot snapshot, long? minimumWalOffset, IWriteAheadLog? checkpointWal)
    {
        WalSnapshotPreparation? preparation = null;
        Span<int> requiredCapacities = stackalloc int[WalSnapshotMap.SegmentCount];
        while (true)
        {
            int segmentCount;
            int keyCapacity;
            _checkpointLock.Wait();
            try
            {
                // A checkpoint can finish while map storage is being allocated.
                // Read its current floor only after admission, on every attempt.
                long? floor = checkpointWal is null
                    ? minimumWalOffset
                    : checkpointWal.IsCheckpointCopyComplete &&
                      checkpointWal.TryGetCheckpointRetainedWalStartOffset(out long retainedStart)
                        ? retainedStart
                        : null;

                if (index.TryInitializeSnapshot(snapshot, preparation, floor, requiredCapacities, out segmentCount, out keyCapacity))
                {
                    RegisterSnapshot_NoLock(snapshot);
                    return snapshot;
                }
            }
            finally
            {
                _checkpointLock.Release();
            }

            // A failed attempt captured and registered nothing. Grow only this
            // reader's unpublished buffer, with neither storage gate held.
            if (preparation is null || preparation.Buffers.Length != segmentCount)
                preparation = new WalSnapshotPreparation(segmentCount);
            preparation.EnsureCapacity(requiredCapacities, keyCapacity);
        }
    }

    private void RegisterSnapshot_NoLock(WalSnapshot snapshot)
    {
        _activeSnapshots[snapshot] = 0;
        Volatile.Write(ref _activeReaderCount, _activeSnapshots.Count);
        if (snapshot.HasWalFrames && snapshot.MinimumWalOffset < _minimumRetainedWalOffset)
            Volatile.Write(ref _minimumRetainedWalOffset, snapshot.MinimumWalOffset);
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
            if (snapshot.HasWalFrames &&
                snapshot.MinimumWalOffset == _minimumRetainedWalOffset)
            {
                RecomputeMinimumRetainedWalOffset_NoLock();
            }
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
            RequestDeferredCheckpoint();
        }
        else if (shouldCheckpoint)
        {
            MarkRuntimePhaseRequested();
        }

        return shouldCheckpoint;
    }

    public void RequestDeferredCheckpoint()
    {
        Volatile.Write(ref _deferredCheckpointRequested, 1);
        MarkRuntimePhaseRequested();
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
        Interlocked.CompareExchange(
            ref _runtimePhase,
            (int)StorageCheckpointPhaseRaw.Idle,
            (int)StorageCheckpointPhaseRaw.Requested);
    }

    private void MarkRuntimePhaseRequested()
    {
        while (true)
        {
            int current = Volatile.Read(ref _runtimePhase);
            var phase = (StorageCheckpointPhaseRaw)current;
            if (phase is StorageCheckpointPhaseRaw.Requested or
                StorageCheckpointPhaseRaw.Copying or
                StorageCheckpointPhaseRaw.CopyCompleteAwaitingReaders or
                StorageCheckpointPhaseRaw.Finalizing or
                StorageCheckpointPhaseRaw.Faulted)
            {
                return;
            }

            if (Interlocked.CompareExchange(
                    ref _runtimePhase,
                    (int)StorageCheckpointPhaseRaw.Requested,
                    current) == current)
            {
                return;
            }
        }
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
                RequestDeferredCheckpoint();

            await checkpointAction(ct);
        }
        finally
        {
            _checkpointLock.Release();
        }
    }

    public bool TryStartBackgroundCheckpoint(Func<CancellationToken, ValueTask> checkpointAction)
    {
        lock (_backgroundCheckpointGate)
        {
            if (_backgroundCheckpointStartsStopped ||
                !HasPendingCheckpointRequest ||
                _backgroundCheckpointTask is { IsCompleted: false })
            {
                return false;
            }

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

    internal async ValueTask<bool> WaitForBackgroundCheckpointWithProgressAsync(
        IPagerSaveToFileProgressObserver observer,
        CancellationToken ct = default)
    {
        Task? backgroundCheckpointTask;
        lock (_backgroundCheckpointGate)
        {
            backgroundCheckpointTask = _backgroundCheckpointTask;
        }

        if (backgroundCheckpointTask is null)
            return false;

        bool reportedCheckpointing = !backgroundCheckpointTask.IsCompleted;
        if (reportedCheckpointing)
        {
            // Never invoke diagnostics while holding the coordinator gate.
            observer.TryReportPhase(PagerSaveToFilePhase.Checkpointing);
        }

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

        return reportedCheckpointing;
    }

    public async ValueTask StopAndWaitForBackgroundCheckpointAsync()
    {
        Task? backgroundCheckpointTask;
        lock (_backgroundCheckpointGate)
        {
            _backgroundCheckpointStartsStopped = true;
            backgroundCheckpointTask = _backgroundCheckpointTask;
        }

        if (backgroundCheckpointTask is null)
            return;

        try
        {
            await backgroundCheckpointTask;
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
