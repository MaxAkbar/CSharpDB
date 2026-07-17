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
