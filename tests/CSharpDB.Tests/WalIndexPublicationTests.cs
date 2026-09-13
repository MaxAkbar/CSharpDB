using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class WalIndexPublicationTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Publication_PreservesOldSnapshotAndCountsRepeatedPageWrites(bool contiguous)
    {
        var index = new WalIndex();
        index.PublishCommittedFrames(new (uint, long)[] { (1, 100), (2, 200) });
        var before = index.TakeSnapshot();
        long lastOffset = 1000 + 2L * PageConstants.WalFrameSize;
        if (contiguous)
        {
            WalFrameWrite[] frames = [new(1, new byte[PageConstants.PageSize]), new(3, new byte[PageConstants.PageSize]), new(1, new byte[PageConstants.PageSize])];
            index.PublishCommittedFrames(frames, 1000);
        }
        else
        {
            index.PublishCommittedFrames(new (uint, long)[] { (1, 1000), (3, 1000 + PageConstants.WalFrameSize), (1, lastOffset) });
        }

        Assert.True(before.TryGet(1, out long oldOffset));
        Assert.Equal(100, oldOffset);
        Assert.False(before.TryGet(3, out _));
        Assert.Equal(1, before.CommitCounter);
        var after = index.TakeSnapshot();
        Assert.Equal(2, after.CommitCounter);
        Assert.True(after.TryGet(1, out long newOffset));
        Assert.Equal(lastOffset, newOffset);
        Assert.True(after.TryGet(2, out long unchangedOffset));
        Assert.Equal(200, unchangedOffset);
        Assert.Equal((5, 2L, 5L), index.GetRuntimeStateSnapshot());

        index.Reset();
        Assert.Equal((0, 2L, 5L), index.GetRuntimeStateSnapshot());
        index.AddCommittedFrame(4, 400);
        index.AdvanceRecoveredCommit();
        Assert.Equal((1, 2L, 5L), index.GetRuntimeStateSnapshot());
        Assert.Equal(3, index.CommitCounter);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConcurrentSnapshots_ObserveCompleteCommitGenerations(bool contiguous)
    {
        const int pageCount = 64;
        const int commitCount = 1000;
        var index = new WalIndex();
        var frames = Enumerable.Range(0, pageCount)
            .Select(i => new WalFrameWrite((uint)i, new byte[PageConstants.PageSize])).ToArray();
        var locations = new (uint PageId, long WalOffset)[pageCount];
        using var start = new ManualResetEventSlim();
        var writer = Task.Run(() =>
        {
            start.Wait();
            for (int commit = 1; commit <= commitCount; commit++)
            {
                long firstOffset = (long)commit * pageCount * PageConstants.WalFrameSize;
                if (contiguous)
                    index.PublishCommittedFrames(frames, firstOffset);
                else
                {
                    for (int i = 0; i < pageCount; i++)
                        locations[i] = ((uint)i, firstOffset + (long)i * PageConstants.WalFrameSize);
                    index.PublishCommittedFrames(locations);
                }
                Thread.Yield();
            }
        }, TestContext.Current.CancellationToken);

        start.Set();
        for (int read = 0; read < commitCount; read++)
        {
            var snapshot = index.TakeSnapshot();
            for (uint page = 0; page < pageCount; page++)
            {
                bool found = snapshot.TryGet(page, out long offset);
                Assert.Equal(snapshot.CommitCounter != 0, found);
                if (found)
                    Assert.Equal((snapshot.CommitCounter * pageCount + page) * PageConstants.WalFrameSize, offset);
            }
        }
        await writer;
        Assert.Equal((pageCount * commitCount, (long)commitCount, (long)pageCount * commitCount), index.GetRuntimeStateSnapshot());
    }
}
