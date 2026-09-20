using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class WalIndexBulkGuardTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ConcentratedBatch_DoesNotNeedMoreIndexAllocationThanConsecutiveBatch(bool contiguous)
    {
        var consecutive = Locations(1000, 1);
        var concentrated = Locations(1000, 64);
        var consecutiveFrames = Frames(consecutive);
        var concentratedFrames = Frames(concentrated);
        _ = Measure(consecutive, consecutiveFrames);
        _ = Measure(concentrated, concentratedFrames);

        Assert.Equal(Measure(consecutive, consecutiveFrames), Measure(concentrated, concentratedFrames));

        long Measure((uint PageId, long WalOffset)[] locations, WalFrameWrite[] frames)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            var index = new WalIndex();
            if (contiguous) index.PublishCommittedFrames(frames, 32);
            else index.PublishCommittedFrames(locations);
            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(1000, index.GetAllCommittedPages().Count);
            return allocated;
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void CheckpointCopy_PreservesConsecutiveTraversalAndIndependentState(bool contiguous)
    {
        var locations = Locations(1024, 1);
        var index = new WalIndex();
        if (contiguous) index.PublishCommittedFrames(Frames(locations), 32);
        else index.PublishCommittedFrames(locations);
        var state = index.GetCommittedStateSnapshot();
        Assert.Equal(locations.Select(x => x.PageId), state.LatestPageMap.Keys);
        Assert.Equal(1024, state.FrameCount);
        Assert.Equal(1, state.CommitCounter);
        index.PublishCommittedFrames(new (uint, long)[] { (1, 900), (1025, 1000) });
        index.Reset();
        Assert.Equal(locations.Select(x => x.WalOffset), state.LatestPageMap.Values);
        Assert.Equal(locations.Select(x => x.PageId), state.LatestPageMap.Keys);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ResetAndRepeatedKeys_RebuildOnlyCurrentSegmentMembership(bool contiguous)
    {
        var index = new WalIndex();
        var oldLocations = Locations(1000, 64);
        index.PublishCommittedFrames(oldLocations);
        var oldSnapshot = index.TakeSnapshot();
        index.Reset();
        var locations = Locations(130, 1).Reverse().Concat(Locations(130, 1)).ToArray();
        if (contiguous) index.PublishCommittedFrames(Frames(locations), 32);
        else index.PublishCommittedFrames(locations);
        var before = index.TakeSnapshot();
        var expected = index.GetAllCommittedPages().ToDictionary();
        index.PublishCommittedFrames(new (uint, long)[] { (1, 99_000_000), (65, 88_000_000), (1, 77_000_000) });
        expected[1] = 77_000_000; expected[65] = 88_000_000;
        var after = index.TakeSnapshot();
        foreach (var entry in expected)
        {
            Assert.True(after.TryGet(entry.Key, out long actual));
            Assert.Equal(entry.Value, actual);
        }
        Assert.False(after.TryGet(oldLocations[^1].PageId, out _));
        Assert.True(before.TryGet(1, out long prior));
        Assert.NotEqual(77_000_000, prior);
        foreach (var entry in oldLocations)
        {
            Assert.True(oldSnapshot.TryGet(entry.PageId, out long actual));
            Assert.Equal(entry.WalOffset, actual);
        }
        Assert.Equal(130, index.GetAllCommittedPages().Count);
    }

    [Fact]
    public void ConcentratedSegment_PreparedCopyAfterLinkGrowthAllocatesNothing()
    {
        var index = new WalIndex();
        for (uint i = 0; i < 1000; i++) index.AddCommittedFrame(1 + 64 * i, 32 + i);
        var old = index.TakeSnapshot();
        index.AddCommittedFrame(1, 90_000);
        var pending = new WalSnapshot();
        Span<int> capacities = stackalloc int[64];
        Assert.False(index.TryInitializeSnapshot(pending, null, null, capacities, out int segments, out _));
        Assert.Equal(64, segments);
        Assert.Equal(1000, capacities[1]);
        Assert.Equal(1000, capacities.ToArray().Sum());
        var preparation = new WalSnapshotPreparation(segments);
        preparation.EnsureCapacity(capacities);
        long start = GC.GetAllocatedBytesForCurrentThread();
        bool captured = index.TryInitializeSnapshot(pending, preparation, null, capacities, out _, out _);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - start;
        Assert.True(captured);
        Assert.Equal(0, allocated);
        Assert.True(old.TryGet(1, out long oldOffset)); Assert.Equal(32, oldOffset);
        Assert.True(pending.TryGet(1, out long updated)); Assert.Equal(90_000, updated);
        Assert.Equal(33, pending.MinimumWalOffset);
        Assert.True(pending.TryGet(1 + 64 * 999, out long last)); Assert.Equal(1031, last);
    }

    [Fact]
    public void IncrementalGrowthAndReplacement_KeepLinkedSegmentSnapshotsComplete()
    {
        var index = new WalIndex();
        var expected = new Dictionary<uint, long>();
        var retained = new List<(WalSnapshot Snapshot, Dictionary<uint, long> Expected)>();
        for (uint i = 0; i < 400; i++)
        {
            uint key = i % 2 == 0 ? i * 64 : i;
            expected[key] = 100 + i;
            index.PublishCommittedFrames(new (uint, long)[] { (key, 100 + i) });
            if (i % 17 == 0) retained.Add((index.TakeSnapshot(), new(expected)));
        }
        var replacement = expected.Where(e => e.Key % 2 != 0).ToDictionary();
        index.ReplaceCommittedState(replacement, replacement.Count, 1);
        var replaced = index.TakeSnapshot();
        index.AddCommittedFrame(0, 999_999);
        var latest = index.TakeSnapshot();
        foreach (var entry in replacement)
        {
            Assert.True(latest.TryGet(entry.Key, out long actual)); Assert.Equal(entry.Value, actual);
        }
        Assert.False(replaced.TryGet(0, out _));
        foreach (var item in retained)
        {
            foreach (var entry in item.Expected)
            {
                Assert.True(item.Snapshot.TryGet(entry.Key, out long actual)); Assert.Equal(entry.Value, actual);
            }
            Assert.Equal(item.Expected.Values.Min(), item.Snapshot.MinimumWalOffset);
        }
    }

    private static (uint PageId, long WalOffset)[] Locations(int count, uint stride)
        => Enumerable.Range(0, count).Select(i => (1 + (uint)i * stride, 32L + (long)i * PageConstants.WalFrameSize)).ToArray();

    private static WalFrameWrite[] Frames((uint PageId, long WalOffset)[] locations)
        => locations.Select(x => new WalFrameWrite(x.PageId, ReadOnlyMemory<byte>.Empty)).ToArray();

    [Fact]
    public void ColdKeyDirectory_CapacityRetryLeavesBuffersUntouchedAfterGrowth()
    {
        var index = new WalIndex();
        index.PublishCommittedFrames(Locations(128, 1));
        var pending = new WalSnapshot();
        Span<int> required = stackalloc int[64];
        long coldStart = GC.GetAllocatedBytesForCurrentThread();
        bool coldCaptured = index.TryInitializeSnapshot(pending, null, null, required, out int segments, out int keyCapacity);
        long coldAllocated = GC.GetAllocatedBytesForCurrentThread() - coldStart;
        Assert.False(coldCaptured); Assert.Equal(0, coldAllocated);
        Assert.Equal(128, keyCapacity);
        var prepared = new WalSnapshotPreparation(segments);
        prepared.EnsureCapacity(required, keyCapacity);
        index.AddCommittedFrame(129, 900_000);
        long start = GC.GetAllocatedBytesForCurrentThread();
        bool captured = index.TryInitializeSnapshot(pending, prepared, null, required, out _, out keyCapacity);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - start;
        Assert.False(captured); Assert.Equal(0, allocated); Assert.Equal(129, keyCapacity);
        Assert.All(prepared.Buffers, buffer => Assert.Equal(0, buffer!.Count));
        Assert.Equal(0, prepared.Keys!.Count);
        prepared.EnsureCapacity(required, keyCapacity);
        start = GC.GetAllocatedBytesForCurrentThread();
        captured = index.TryInitializeSnapshot(pending, prepared, null, required, out _, out _);
        allocated = GC.GetAllocatedBytesForCurrentThread() - start;
        Assert.True(captured); Assert.Equal(0, allocated);
        Assert.Equal(129, prepared.Keys!.Count);
        for (uint i = 1; i <= 128; i++)
        {
            Assert.True(pending.TryGet(i, out long offset)); Assert.Equal(32L + (i - 1) * PageConstants.WalFrameSize, offset);
        }
        Assert.True(pending.TryGet(129, out long added)); Assert.Equal(900_000, added);
    }

    [Fact]
    public void KeyDirectory_ReusesMembershipForOverwritesAndRebuildsForNewKeys()
    {
        var index = new WalIndex();
        index.PublishCommittedFrames(Locations(128, 1));
        var old = index.TakeSnapshot();
        index.AddCommittedFrame(1, 999_999);
        Span<int> required = stackalloc int[64];
        Assert.False(index.TryInitializeSnapshot(new WalSnapshot(), null, null, required, out _, out int keyCapacity));
        Assert.Equal(0, keyCapacity);
        Assert.Equal(2, required.ToArray().Sum());
        index.AddCommittedFrame(129, 888_888);
        var pending = new WalSnapshot();
        Assert.False(index.TryInitializeSnapshot(pending, null, null, required, out int segments, out keyCapacity));
        Assert.Equal(129, keyCapacity);
        Assert.Equal(3, required.ToArray().Sum());
        var prepared = new WalSnapshotPreparation(segments);
        prepared.EnsureCapacity(required, keyCapacity);
        Assert.True(index.TryInitializeSnapshot(pending, prepared, null, required, out _, out _));
        for (uint i = 2; i <= 128; i++)
        {
            Assert.True(pending.TryGet(i, out long offset)); Assert.Equal(32L + (i - 1) * PageConstants.WalFrameSize, offset);
        }
        Assert.True(pending.TryGet(1, out long changed)); Assert.Equal(999_999, changed);
        Assert.True(pending.TryGet(129, out long added)); Assert.Equal(888_888, added);
        Assert.True(old.TryGet(1, out long prior)); Assert.Equal(32, prior);
        Assert.False(old.TryGet(129, out _));
    }

    [Fact]
    public void SameSizedReplacement_RebuildsKeyDirectoryInsteadOfReusingOldKeys()
    {
        var index = new WalIndex();
        index.PublishCommittedFrames(Locations(128, 64));
        var old = index.TakeSnapshot();
        var replacement = Locations(128, 1).ToDictionary(x => x.PageId, x => x.WalOffset);
        index.OverwriteCommittedState(replacement, 128, 7);
        var current = index.TakeSnapshot();
        index.AddCommittedFrame(2, 500_000);
        var updated = index.TakeSnapshot();
        foreach (var entry in replacement)
        {
            Assert.True(current.TryGet(entry.Key, out long offset)); Assert.Equal(entry.Value, offset);
            Assert.True(updated.TryGet(entry.Key, out offset)); Assert.Equal(entry.Key == 2 ? 500_000 : entry.Value, offset);
        }
        Assert.False(updated.TryGet(1 + 127 * 64, out _));
        Assert.True(old.TryGet(1 + 127 * 64, out _));
    }
}
