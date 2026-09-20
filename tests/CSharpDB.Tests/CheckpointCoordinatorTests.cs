using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Device;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class CheckpointCoordinatorTests
{
    [Fact]
    public void MapPreparation_RechecksTheCheckpointFloorBeforeCapturingAndRegistering()
    {
        using var coordinator = new CheckpointCoordinator();
        var index = CreateIndexWithTwoWalOffsets();
        int floorReads = 0;
        var wal = new SnapshotFloorWal(() =>
        {
            Assert.Equal(0, coordinator.ActiveReaderCount);
            if (++floorReads == 1)
                return true;

            // The previous checkpoint finalized and a new generation uses lower
            // offsets. The first attempt's floor must not exclude these frames.
            index.OverwriteCommittedState(new Dictionary<uint, long> { [1] = 20, [2] = 40 }, 2, 8);
            return false;
        });

        WalSnapshot snapshot = coordinator.AcquireReaderSnapshot(index, checkpointWal: wal);

        Assert.InRange(floorReads, 2, 3); // Cached admission may add an initial miss.
        Assert.Equal(8, snapshot.CommitCounter);
        Assert.True(snapshot.TryGet(1, out long first));
        Assert.True(snapshot.TryGet(2, out long second));
        Assert.Equal(20, first);
        Assert.Equal(40, second);
        Assert.Equal(1, coordinator.ActiveReaderCount);
        AssertMinimumRetainedWalOffset(coordinator, 20);
        Assert.True(coordinator.ReleaseReaderSnapshot(snapshot));
    }

    [Fact]
    public async Task CheckpointCannotRunBetweenFloorReadAndReaderRegistration()
    {
        using var coordinator = new CheckpointCoordinator();
        var index = CreateIndexWithTwoWalOffsets();
        index.TakeSnapshot(200); // Exercise a cache hit without a capacity retry.
        Task? checkpoint = null;
        var wal = new SnapshotFloorWal(() =>
        {
            checkpoint = coordinator.RunCheckpointAsync(2, _ =>
            {
                Assert.Equal(1, coordinator.ActiveReaderCount);
                AssertMinimumRetainedWalOffset(coordinator, 200);
                return ValueTask.CompletedTask;
            }).AsTask();
            Assert.False(checkpoint.IsCompleted);
            return true;
        });

        var snapshot = coordinator.AcquireReaderSnapshot(index, checkpointWal: wal);
        Assert.NotNull(checkpoint);
        await checkpoint.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.True(coordinator.ReleaseReaderSnapshot(snapshot));
    }

    [Theory]
    [InlineData(null, 100L)]
    [InlineData(100L, 100L)]
    [InlineData(200L, 200L)]
    public void SameStateSnapshots_ReleaseIndependentlyAndRetainWalUntilLastReader(
        long? minimumWalOffset, long expectedMinimum)
    {
        using var coordinator = new CheckpointCoordinator();
        WalIndex index = CreateIndexWithTwoWalOffsets();

        WalSnapshot first = coordinator.AcquireReaderSnapshot(index, minimumWalOffset);
        WalSnapshot second = coordinator.AcquireReaderSnapshot(index, minimumWalOffset);

        Assert.NotSame(first, second);
        Assert.Equal(first.CommitCounter, second.CommitCounter);
        Assert.Equal(2, coordinator.ActiveReaderCount);
        AssertMinimumRetainedWalOffset(coordinator, expectedMinimum);

        coordinator.RequestDeferredCheckpoint();
        Assert.False(coordinator.TryConsumeDeferredCheckpointRequest());

        Assert.False(coordinator.ReleaseReaderSnapshot(first));
        Assert.Equal(1, coordinator.ActiveReaderCount);
        AssertMinimumRetainedWalOffset(coordinator, expectedMinimum);
        Assert.False(coordinator.TryConsumeDeferredCheckpointRequest());

        // Releasing the same handle twice must not unregister the other reader.
        Assert.False(coordinator.ReleaseReaderSnapshot(first));
        Assert.Equal(1, coordinator.ActiveReaderCount);
        AssertMinimumRetainedWalOffset(coordinator, expectedMinimum);
        Assert.True(second.TryGet(2, out long secondOffset));
        Assert.Equal(200L, secondOffset);

        Assert.True(coordinator.ReleaseReaderSnapshot(second));
        Assert.Equal(0, coordinator.ActiveReaderCount);
        Assert.False(coordinator.TryGetMinimumRetainedWalOffset(out _));
        Assert.True(coordinator.TryConsumeDeferredCheckpointRequest());
        Assert.False(coordinator.ReleaseReaderSnapshot(second));
        Assert.Equal(0, coordinator.ActiveReaderCount);
    }

    [Theory]
    [InlineData(201L)]
    [InlineData(long.MaxValue)]
    public void SameStateFloorFilteredEmptySnapshots_HaveIndependentLifetimesWithoutRetainingWal(
        long minimumWalOffset)
    {
        using var coordinator = new CheckpointCoordinator();
        WalIndex index = CreateIndexWithTwoWalOffsets();

        WalSnapshot retaining = coordinator.AcquireReaderSnapshot(index);
        WalSnapshot firstEmpty = coordinator.AcquireReaderSnapshot(index, minimumWalOffset);
        WalSnapshot secondEmpty = coordinator.AcquireReaderSnapshot(index, minimumWalOffset);

        Assert.NotSame(firstEmpty, secondEmpty);
        Assert.False(firstEmpty.HasWalFrames);
        Assert.False(secondEmpty.HasWalFrames);
        Assert.False(firstEmpty.TryGet(1, out _));
        Assert.False(secondEmpty.TryGet(2, out _));
        Assert.Equal(3, coordinator.ActiveReaderCount);
        AssertMinimumRetainedWalOffset(coordinator, 100);

        coordinator.RequestDeferredCheckpoint();
        Assert.False(coordinator.TryConsumeDeferredCheckpointRequest());
        Assert.False(coordinator.ReleaseReaderSnapshot(retaining));
        Assert.Equal(2, coordinator.ActiveReaderCount);
        Assert.False(coordinator.TryGetMinimumRetainedWalOffset(out _));
        // Readers whose floor excludes every frame must not defer finalization.
        Assert.True(coordinator.TryConsumeDeferredCheckpointRequest());

        Assert.False(coordinator.ReleaseReaderSnapshot(firstEmpty));
        Assert.False(coordinator.ReleaseReaderSnapshot(firstEmpty));
        Assert.Equal(1, coordinator.ActiveReaderCount);
        Assert.False(coordinator.TryGetMinimumRetainedWalOffset(out _));
        Assert.True(coordinator.ReleaseReaderSnapshot(secondEmpty));
        Assert.Equal(0, coordinator.ActiveReaderCount);
        Assert.False(coordinator.TryGetMinimumRetainedWalOffset(out _));
    }

    [Fact]
    public void NoWalSnapshot_DoesNotCreateOrPreserveRetentionFloor()
    {
        using var coordinator = new CheckpointCoordinator();
        var index = new WalIndex();

        WalSnapshot emptySnapshot = coordinator.AcquireReaderSnapshot(index);
        Assert.False(coordinator.TryGetMinimumRetainedWalOffset(out _));

        index.AddCommittedFrame(pageId: 1, walFileOffset: 100);
        WalSnapshot retainingSnapshot = coordinator.AcquireReaderSnapshot(index);
        AssertMinimumRetainedWalOffset(coordinator, 100);

        Assert.False(coordinator.ReleaseReaderSnapshot(retainingSnapshot));
        Assert.Equal(1, coordinator.ActiveReaderCount);
        Assert.False(coordinator.TryGetMinimumRetainedWalOffset(out _));

        Assert.True(coordinator.ReleaseReaderSnapshot(emptySnapshot));
        Assert.Equal(0, coordinator.ActiveReaderCount);
    }

    [Fact]
    public void ReleasingNonMinimumSnapshot_PreservesMinimum()
    {
        using var coordinator = new CheckpointCoordinator();
        WalIndex index = CreateIndexWithTwoWalOffsets();

        WalSnapshot minimum = coordinator.AcquireReaderSnapshot(index);
        WalSnapshot later = coordinator.AcquireReaderSnapshot(index, minimumWalOffset: 200);
        AssertMinimumRetainedWalOffset(coordinator, 100);

        Assert.False(coordinator.ReleaseReaderSnapshot(later));
        AssertMinimumRetainedWalOffset(coordinator, 100);
        Assert.True(coordinator.ReleaseReaderSnapshot(minimum));
        Assert.False(coordinator.TryGetMinimumRetainedWalOffset(out _));
    }

    [Fact]
    public void ReleasingMinimumSnapshot_AdvancesToNextMinimum()
    {
        using var coordinator = new CheckpointCoordinator();
        WalIndex index = CreateIndexWithTwoWalOffsets();

        WalSnapshot minimum = coordinator.AcquireReaderSnapshot(index);
        WalSnapshot later = coordinator.AcquireReaderSnapshot(index, minimumWalOffset: 200);

        Assert.False(coordinator.ReleaseReaderSnapshot(minimum));
        AssertMinimumRetainedWalOffset(coordinator, 200);
        Assert.True(coordinator.ReleaseReaderSnapshot(later));
        Assert.False(coordinator.TryGetMinimumRetainedWalOffset(out _));
    }

    [Fact]
    public void ReleasingOneOfDuplicateMinimumSnapshots_PreservesMinimumUntilLastDuplicateReleases()
    {
        using var coordinator = new CheckpointCoordinator();
        WalIndex index = CreateIndexWithTwoWalOffsets();

        WalSnapshot firstMinimum = coordinator.AcquireReaderSnapshot(index);
        WalSnapshot secondMinimum = coordinator.AcquireReaderSnapshot(index);
        WalSnapshot later = coordinator.AcquireReaderSnapshot(index, minimumWalOffset: 200);

        Assert.False(coordinator.ReleaseReaderSnapshot(firstMinimum));
        AssertMinimumRetainedWalOffset(coordinator, 100);

        Assert.False(coordinator.ReleaseReaderSnapshot(secondMinimum));
        AssertMinimumRetainedWalOffset(coordinator, 200);

        Assert.True(coordinator.ReleaseReaderSnapshot(later));
        Assert.False(coordinator.TryGetMinimumRetainedWalOffset(out _));
    }

    [Fact]
    public async Task StopAndWaitForBackgroundCheckpointAsync_WaitsForRunningCheckpointAndRejectsFutureStarts()
    {
        using var coordinator = new CheckpointCoordinator();
        var checkpointStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCheckpointToFinish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int checkpointCount = 0;
        CancellationToken ct = TestContext.Current.CancellationToken;

        coordinator.RequestDeferredCheckpoint();
        Assert.True(coordinator.TryStartBackgroundCheckpoint(async _ =>
        {
            checkpointStarted.SetResult();
            await allowCheckpointToFinish.Task;
            Interlocked.Increment(ref checkpointCount);
        }));

        await checkpointStarted.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);

        Task stopTask = coordinator.StopAndWaitForBackgroundCheckpointAsync().AsTask();
        Assert.False(stopTask.IsCompleted);

        coordinator.RequestDeferredCheckpoint();
        Assert.False(coordinator.TryStartBackgroundCheckpoint(_ => ValueTask.CompletedTask));

        allowCheckpointToFinish.SetResult();
        await stopTask.WaitAsync(TimeSpan.FromSeconds(10), ct);

        Assert.Equal(1, Volatile.Read(ref checkpointCount));
        Assert.False(coordinator.TryStartBackgroundCheckpoint(_ => ValueTask.CompletedTask));
    }

    [Fact]
    public async Task StopAndWaitForBackgroundCheckpointAsync_WithNoRunningCheckpointRejectsFutureStarts()
    {
        using var coordinator = new CheckpointCoordinator();

        await coordinator.StopAndWaitForBackgroundCheckpointAsync();

        coordinator.RequestDeferredCheckpoint();
        Assert.False(coordinator.TryStartBackgroundCheckpoint(_ => ValueTask.CompletedTask));
    }

    [Fact]
    public async Task StopAndWaitForBackgroundCheckpointAsync_ConcurrentCallersWaitForSameCheckpoint()
    {
        using var coordinator = new CheckpointCoordinator();
        var checkpointStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCheckpointToFinish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        CancellationToken ct = TestContext.Current.CancellationToken;

        coordinator.RequestDeferredCheckpoint();
        Assert.True(coordinator.TryStartBackgroundCheckpoint(async _ =>
        {
            checkpointStarted.SetResult();
            await allowCheckpointToFinish.Task;
        }));

        await checkpointStarted.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);

        Task firstStopTask = coordinator.StopAndWaitForBackgroundCheckpointAsync().AsTask();
        Task secondStopTask = coordinator.StopAndWaitForBackgroundCheckpointAsync().AsTask();
        Assert.False(firstStopTask.IsCompleted);
        Assert.False(secondStopTask.IsCompleted);

        allowCheckpointToFinish.SetResult();
        await Task.WhenAll(firstStopTask, secondStopTask)
            .WaitAsync(TimeSpan.FromSeconds(10), ct);
    }

    [Fact]
    public async Task StopAndWaitForBackgroundCheckpointAsync_PropagatesFailureAndStillRejectsFutureStarts()
    {
        using var coordinator = new CheckpointCoordinator();
        var expected = new InvalidOperationException("checkpoint failed");

        coordinator.RequestDeferredCheckpoint();
        Assert.True(coordinator.TryStartBackgroundCheckpoint(
            _ => ValueTask.FromException(expected)));

        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => coordinator.StopAndWaitForBackgroundCheckpointAsync().AsTask());

        Assert.Same(expected, actual);
        coordinator.RequestDeferredCheckpoint();
        Assert.False(coordinator.TryStartBackgroundCheckpoint(_ => ValueTask.CompletedTask));
    }

    private sealed class SnapshotFloorWal(Func<bool> isCopyComplete) : IWriteAheadLog
    {
        public bool IsCheckpointCopyComplete => isCopyComplete();
        public bool HasPendingCheckpoint => true;
        public bool HasPendingCommitWork => false;
        public bool IsOpen => true;
        public bool TryGetCheckpointRetainedWalStartOffset(out long walOffset) { walOffset = 200; return true; }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public ValueTask OpenAsync(uint currentDbPageCount, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public void BeginTransaction() => throw new NotSupportedException();
        public ValueTask AppendFrameAsync(uint pageId, ReadOnlyMemory<byte> pageData, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask AppendFramesAsync(ReadOnlyMemory<WalFrameWrite> frames, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask<WalCommitResult> AppendFramesAndCommitAsync(ReadOnlyMemory<WalFrameWrite> frames, uint newDbPageCount, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask<WalCommitResult> CommitAsync(uint newDbPageCount, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask RollbackAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask<byte[]> ReadPageAsync(long walFrameOffset, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask ReadPageIntoAsync(long walFrameOffset, Memory<byte> destination, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask<bool> CheckpointStepAsync(IStorageDevice device, uint pageCount, int maxPages, CancellationToken cancellationToken = default, bool allowFinalize = true) => throw new NotSupportedException();
        public ValueTask CheckpointAsync(IStorageDevice device, uint pageCount, CancellationToken cancellationToken = default, bool allowFinalize = true) => throw new NotSupportedException();
        public ValueTask CloseAndDeleteAsync() => throw new NotSupportedException();
    }

    private static WalIndex CreateIndexWithTwoWalOffsets()
    {
        var index = new WalIndex();
        index.AddCommittedFrame(pageId: 1, walFileOffset: 100);
        index.AddCommittedFrame(pageId: 2, walFileOffset: 200);
        return index;
    }

    private static void AssertMinimumRetainedWalOffset(
        CheckpointCoordinator coordinator,
        long expected)
    {
        Assert.True(coordinator.TryGetMinimumRetainedWalOffset(out long actual));
        Assert.Equal(expected, actual);
    }
}
