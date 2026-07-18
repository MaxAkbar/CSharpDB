using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class CheckpointCoordinatorTests
{
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
