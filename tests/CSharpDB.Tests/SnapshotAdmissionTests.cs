using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class SnapshotAdmissionTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(1024)]
    public void CachedAdmissions_CaptureCounterOnlyChangesAndReleaseIndependently(int pages)
    {
        using var coordinator = new CheckpointCoordinator();
        var index = new WalIndex();
        for (uint page = 1; page <= pages; page++) index.AddCommittedFrame(page, page * 100L);
        index.AdvanceCommit();
        var first = coordinator.AcquireReaderSnapshot(index);
        index.AdvanceCommit();
        var second = coordinator.AcquireReaderSnapshot(index);
        Assert.NotSame(first, second);
        Assert.Equal(1, first.CommitCounter);
        Assert.Equal(2, second.CommitCounter);
        Assert.Equal(2, coordinator.ActiveReaderCount);
        Assert.Equal(pages != 0, second.HasWalFrames);
        if (pages != 0)
        {
            Assert.True(second.TryGet((uint)pages, out var offset));
            Assert.Equal(pages * 100L, offset);
        }
        Assert.False(coordinator.ReleaseReaderSnapshot(first));
        Assert.False(coordinator.ReleaseReaderSnapshot(first));
        Assert.Equal(1, coordinator.ActiveReaderCount);
        Assert.True(coordinator.ReleaseReaderSnapshot(second));
        Assert.False(coordinator.TryGetMinimumRetainedWalOffset(out _));
    }

    [Theory]
    [InlineData(3)]
    [InlineData(1024)]
    public void DirtyMapAndFloorChange_PreserveOldViewsAndRetention(int pages)
    {
        using var coordinator = new CheckpointCoordinator();
        var index = new WalIndex();
        for (uint page = 1; page <= pages; page++) index.AddCommittedFrame(page, page * 100L);
        var original = coordinator.AcquireReaderSnapshot(index);
        var filtered = coordinator.AcquireReaderSnapshot(index, 200);
        var cached = coordinator.AcquireReaderSnapshot(index, 200);
        index.AddCommittedFrame(1, 250);
        var updated = coordinator.AcquireReaderSnapshot(index, 200);
        Assert.True(original.TryGet(1, out var oldOffset)); Assert.Equal(100, oldOffset);
        Assert.False(filtered.TryGet(1, out _)); Assert.False(cached.TryGet(1, out _));
        Assert.True(updated.TryGet(1, out var newOffset)); Assert.Equal(250, newOffset);
        Assert.True(coordinator.TryGetMinimumRetainedWalOffset(out var minimum)); Assert.Equal(100, minimum);
        Assert.False(coordinator.ReleaseReaderSnapshot(original));
        Assert.True(coordinator.TryGetMinimumRetainedWalOffset(out minimum)); Assert.Equal(200, minimum);
        Assert.False(coordinator.ReleaseReaderSnapshot(filtered));
        Assert.False(coordinator.ReleaseReaderSnapshot(cached));
        Assert.True(coordinator.ReleaseReaderSnapshot(updated));
    }

    [Fact]
    public void ResetAndRepopulation_DoNotReuseStaleEmptyOrPopulatedMaps()
    {
        using var coordinator = new CheckpointCoordinator();
        var index = new WalIndex();
        var empty = coordinator.AcquireReaderSnapshot(index);
        Assert.True(coordinator.ReleaseReaderSnapshot(empty));
        index.AddCommittedFrame(1, 100);
        var populated = coordinator.AcquireReaderSnapshot(index);
        Assert.True(coordinator.ReleaseReaderSnapshot(populated));
        index.Reset(); // No active readers; previously returned maps remain immutable.
        var reset = coordinator.AcquireReaderSnapshot(index);
        Assert.True(coordinator.ReleaseReaderSnapshot(reset));
        index.AddCommittedFrame(2, 20);
        var repopulated = coordinator.AcquireReaderSnapshot(index);
        Assert.False(empty.HasWalFrames); Assert.False(reset.HasWalFrames);
        Assert.True(populated.TryGet(1, out var offset)); Assert.Equal(100, offset);
        Assert.False(repopulated.TryGet(1, out _));
        Assert.True(repopulated.TryGet(2, out offset)); Assert.Equal(20, offset);
        Assert.True(coordinator.ReleaseReaderSnapshot(repopulated));
    }
}
