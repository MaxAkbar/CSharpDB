using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class WalIndexPublicationTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void FreshAndResetPublication_ResumeSnapshotInvalidationAndPreserveLifetimeCounts(bool contiguous)
    {
        var index = new WalIndex();
        var retained = new List<(WalSnapshot Snapshot, Dictionary<uint, long> Expected)>();
        uint[] initialPages = Enumerable.Range(0, 128).Select(i => (uint)i).Append(63u).ToArray();
        var payload = new byte[PageConstants.PageSize];

        for (int cycle = 0; cycle < 2; cycle++)
        {
            if (cycle != 0)
            {
                index.Reset();
                Assert.Equal((0, 2L, 132L), index.GetRuntimeStateSnapshot());
                Assert.Equal(2, index.CommitCounter);
            }

            long initialOffset = (cycle + 1) * 1_000_000L;
            Publish(initialPages, initialOffset);
            var expected = Enumerable.Range(0, 128).ToDictionary(
                i => (uint)i, i => initialOffset + (long)i * PageConstants.WalFrameSize);
            expected[63] = initialOffset + 128L * PageConstants.WalFrameSize;
            var before = index.TakeSnapshot();
            Assert.Equal(2L * cycle + 1, before.CommitCounter);
            Assert.Equal((129, 2L * cycle + 1, 132L * cycle + 129), index.GetRuntimeStateSnapshot());
            retained.Add((before, new(expected)));

            // Both keys belong to segment 63. This capture must notice writes
            // after the preceding snapshot has cleared the dirty state.
            long updateOffset = initialOffset + 900_000;
            Publish([63, 127, 63], updateOffset);
            expected[63] = updateOffset + 2L * PageConstants.WalFrameSize;
            expected[127] = updateOffset + PageConstants.WalFrameSize;
            var after = index.TakeSnapshot();
            Assert.Equal(2L * cycle + 2, after.CommitCounter);
            Assert.Equal((132, 2L * cycle + 2, 132L * (cycle + 1)), index.GetRuntimeStateSnapshot());
            Assert.Equal(128, index.GetAllCommittedPages().Count);
            retained.Add((after, expected));

            foreach (var view in retained)
            {
                Assert.Equal(view.Expected.Values.Min(), view.Snapshot.MinimumWalOffset);
                foreach (var entry in view.Expected)
                {
                    Assert.True(view.Snapshot.TryGet(entry.Key, out long actual));
                    Assert.Equal(entry.Value, actual);
                }
                Assert.False(view.Snapshot.TryGet(128, out _));
            }
        }

        void Publish(uint[] pages, long firstOffset)
        {
            if (contiguous)
                index.PublishCommittedFrames(pages.Select(page => new WalFrameWrite(page, payload)).ToArray(), firstOffset);
            else
                index.PublishCommittedFrames(pages.Select((page, i) =>
                    (page, firstOffset + (long)i * PageConstants.WalFrameSize)).ToArray());
        }
    }

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
    [InlineData(false, 64)]
    [InlineData(true, 64)]
    [InlineData(false, 128)]
    [InlineData(true, 128)]
    public async Task ConcurrentSnapshots_ObserveCompleteCommitGenerations(bool contiguous, int pageCount)
    {
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
