using System.Reflection;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class WalIndexSnapshotTests
{
    private static readonly FieldInfo s_snapshotPageMapField = typeof(WalSnapshot)
        .GetField("_pageMap", BindingFlags.Instance | BindingFlags.NonPublic)!;

    [Theory]
    [InlineData(null)]
    [InlineData(200L)]
    public void RepeatedSnapshots_ShareMapButKeepSeparateReaderIdentities(long? floor)
    {
        var index = CreateIndex();

        var first = index.TakeSnapshot(floor);
        var second = index.TakeSnapshot(floor);

        Assert.NotSame(first, second);
        Assert.Same(GetPageMap(first), GetPageMap(second));
        Assert.Equal(1, first.CommitCounter);
        Assert.Equal(first.CommitCounter, second.CommitCounter);
        if (floor.HasValue)
        {
            AssertSnapshot(first, (2, 200), (3, 300));
            AssertSnapshot(second, (2, 200), (3, 300));
        }
        else
        {
            AssertSnapshot(first, (1, 100), (2, 200), (3, 300));
            AssertSnapshot(second, (1, 100), (2, 200), (3, 300));
        }
    }

    [Fact]
    public void EmptySnapshots_StayEmptyAfterTheIndexIsPopulatedAndReset()
    {
        var index = new WalIndex();
        var empty = index.TakeSnapshot();
        var anotherEmpty = index.TakeSnapshot();
        Assert.NotSame(empty, anotherEmpty);
        Assert.Same(GetPageMap(empty), GetPageMap(anotherEmpty));

        index.PublishCommittedFrames(new (uint, long)[] { (1, 100) });
        var populated = index.TakeSnapshot();
        var filteredEmpty = index.TakeSnapshot(101);
        var anotherFilteredEmpty = index.TakeSnapshot(101);
        Assert.NotSame(filteredEmpty, anotherFilteredEmpty);
        Assert.Same(GetPageMap(filteredEmpty), GetPageMap(anotherFilteredEmpty));

        index.Reset();
        index.AddCommittedFrame(2, 200);

        AssertSnapshot(empty);
        AssertSnapshot(anotherEmpty);
        AssertSnapshot(filteredEmpty);
        AssertSnapshot(anotherFilteredEmpty);
        AssertSnapshot(populated, (1, 100));
        AssertSnapshot(index.TakeSnapshot(), (2, 200));
        Assert.Equal(0, empty.CommitCounter);
        Assert.Equal(1, filteredEmpty.CommitCounter);
    }

    [Fact]
    public void AddCommittedFrame_InvalidatesMapWithoutAdvancingCommitCounter()
    {
        var index = CreateIndex();
        var before = index.TakeSnapshot();
        var sameStateReader = index.TakeSnapshot();
        Assert.Same(GetPageMap(before), GetPageMap(sameStateReader));

        index.AddCommittedFrame(1, 400);
        var after = index.TakeSnapshot();

        Assert.Equal(before.CommitCounter, after.CommitCounter);
        Assert.NotSame(GetPageMap(before), GetPageMap(after));
        Assert.Same(GetPageMap(after), GetPageMap(index.TakeSnapshot()));
        AssertSnapshot(before, (1, 100), (2, 200), (3, 300));
        AssertSnapshot(sameStateReader, (1, 100), (2, 200), (3, 300));
        AssertSnapshot(after, (1, 400), (2, 200), (3, 300));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void PublishCommittedFrames_InvalidatesSharedMapForBothOverloads(bool contiguous)
    {
        var index = CreateIndex();
        var before = index.TakeSnapshot();
        var sameStateReader = index.TakeSnapshot();
        Assert.Same(GetPageMap(before), GetPageMap(sameStateReader));

        if (contiguous)
        {
            WalFrameWrite[] frames =
            [
                new(1, new byte[PageConstants.PageSize]),
                new(4, new byte[PageConstants.PageSize]),
            ];
            index.PublishCommittedFrames(frames, 1000);
        }
        else
        {
            index.PublishCommittedFrames(new (uint, long)[]
            {
                (1, 1000), (4, 1000L + PageConstants.WalFrameSize),
            });
        }

        var after = index.TakeSnapshot();
        Assert.Equal(before.CommitCounter + 1, after.CommitCounter);
        Assert.NotSame(GetPageMap(before), GetPageMap(after));
        Assert.Same(GetPageMap(after), GetPageMap(index.TakeSnapshot()));
        AssertSnapshot(before, (1, 100), (2, 200), (3, 300));
        AssertSnapshot(sameStateReader, (1, 100), (2, 200), (3, 300));
        AssertSnapshot(after, (1, 1000), (2, 200), (3, 300), (4, 1000L + PageConstants.WalFrameSize));
    }

    [Fact]
    public void Reset_DropsCachedStateEvenThoughCommitCounterIsUnchanged()
    {
        var index = CreateIndex();
        var before = index.TakeSnapshot();

        index.Reset();
        var reset = index.TakeSnapshot();
        Assert.Equal(before.CommitCounter, reset.CommitCounter);
        AssertSnapshot(reset);
        Assert.NotSame(GetPageMap(before), GetPageMap(reset));

        index.AddCommittedFrame(9, 900);
        var repopulated = index.TakeSnapshot();
        Assert.Equal(before.CommitCounter, repopulated.CommitCounter);
        AssertSnapshot(repopulated, (9, 900));
        AssertSnapshot(reset);
        AssertSnapshot(before, (1, 100), (2, 200), (3, 300));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ReplacingState_InvalidatesMapWithTheSameCommitCounter_AndCopiesCallerInput(bool overwrite)
    {
        var index = CreateIndex();
        var before = index.TakeSnapshot();
        var replacement = new Dictionary<uint, long> { [2] = 50, [7] = 700 };

        if (overwrite)
            index.OverwriteCommittedState(replacement, frameCount: 2, commitCounter: before.CommitCounter);
        else
            index.ReplaceCommittedState(replacement, frameCount: 2, commitAdvanceCount: 0);

        var after = index.TakeSnapshot();
        Assert.Equal(before.CommitCounter, after.CommitCounter);
        Assert.NotSame(GetPageMap(before), GetPageMap(after));
        Assert.Same(GetPageMap(after), GetPageMap(index.TakeSnapshot()));

        replacement[2] = 999;
        replacement[8] = 800;
        AssertSnapshot(after, (2, 50), (7, 700));
        AssertSnapshot(index.TakeSnapshot(), (2, 50), (7, 700));
        AssertSnapshot(before, (1, 100), (2, 200), (3, 300));
    }

    [Theory]
    [InlineData("live")]
    [InlineData("live-with-frame-count")]
    [InlineData("recovered")]
    [InlineData("empty-locations")]
    [InlineData("empty-contiguous")]
    public void CounterOnlyChanges_ReuseMapButCaptureTheNewCounter(string operation)
    {
        var index = CreateIndex();
        var before = index.TakeSnapshot(200);

        switch (operation)
        {
            case "live":
                index.AdvanceCommit();
                break;
            case "live-with-frame-count":
                index.AdvanceCommit(committedFrameCount: 3);
                break;
            case "recovered":
                index.AdvanceRecoveredCommit();
                break;
            case "empty-locations":
                index.PublishCommittedFrames(Array.Empty<(uint PageId, long WalOffset)>());
                break;
            case "empty-contiguous":
                index.PublishCommittedFrames(Array.Empty<WalFrameWrite>(), 1000);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(operation));
        }

        var after = index.TakeSnapshot(200);
        Assert.NotSame(before, after);
        Assert.Same(GetPageMap(before), GetPageMap(after));
        Assert.Equal(1, before.CommitCounter);
        Assert.Equal(2, after.CommitCounter);
        AssertSnapshot(before, (2, 200), (3, 300));
        AssertSnapshot(after, (2, 200), (3, 300));
    }

    [Theory]
    [InlineData(0L, 100L, 3)]
    [InlineData(100L, 100L, 3)]
    [InlineData(101L, 200L, 2)]
    [InlineData(200L, 200L, 2)]
    [InlineData(201L, 300L, 1)]
    [InlineData(300L, 300L, 1)]
    [InlineData(301L, long.MaxValue, 0)]
    public void MinimumOffsetFloor_IsInclusiveAndMinimumDescribesRetainedPages(long floor, long expectedMinimum, int expectedCount)
    {
        var index = CreateIndex();
        var snapshot = index.TakeSnapshot(floor);
        Assert.Equal(expectedMinimum, snapshot.MinimumWalOffset);
        Assert.Equal(expectedCount != 0, snapshot.HasWalFrames);
        Assert.Equal(expectedCount, GetPageMap(snapshot).Count);
        Assert.Same(GetPageMap(snapshot), GetPageMap(index.TakeSnapshot(floor)));

        for (uint page = 1; page <= 3; page++)
        {
            bool found = snapshot.TryGet(page, out long offset);
            Assert.Equal(page * 100L >= floor, found);
            if (found)
                Assert.Equal(page * 100L, offset);
        }
    }

    [Fact]
    public void CacheKey_UsesExactNullableFloor_AndKeepsOnlyTheCurrentVariant()
    {
        var index = CreateIndex();
        var unfiltered = index.TakeSnapshot();
        var zeroFloor = index.TakeSnapshot(0);
        var hundredFloor = index.TakeSnapshot(100);
        var zeroFloorAgain = index.TakeSnapshot(0);
        var unfilteredAgain = index.TakeSnapshot();

        // These floors select identical pages, but only the latest exact key is cached.
        Assert.NotSame(GetPageMap(unfiltered), GetPageMap(zeroFloor));
        Assert.NotSame(GetPageMap(zeroFloor), GetPageMap(hundredFloor));
        Assert.NotSame(GetPageMap(zeroFloor), GetPageMap(zeroFloorAgain));
        Assert.NotSame(GetPageMap(unfiltered), GetPageMap(unfilteredAgain));
        Assert.Same(GetPageMap(unfilteredAgain), GetPageMap(index.TakeSnapshot()));
        foreach (var snapshot in new[] { unfiltered, zeroFloor, hundredFloor, zeroFloorAgain, unfilteredAgain })
            AssertSnapshot(snapshot, (1, 100), (2, 200), (3, 300));
    }

    [Fact]
    public void FilteredMap_IsInvalidatedWhenPagesMoveAcrossTheFloor()
    {
        var index = CreateIndex();
        var before = index.TakeSnapshot(200);

        index.AddCommittedFrame(1, 400);
        var added = index.TakeSnapshot(200);
        index.AddCommittedFrame(2, 150);
        var removed = index.TakeSnapshot(200);

        Assert.NotSame(GetPageMap(before), GetPageMap(added));
        Assert.NotSame(GetPageMap(added), GetPageMap(removed));
        AssertSnapshot(before, (2, 200), (3, 300));
        AssertSnapshot(added, (1, 400), (2, 200), (3, 300));
        AssertSnapshot(removed, (1, 400), (3, 300));
        Assert.Equal(before.CommitCounter, removed.CommitCounter);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConcurrentReaders_ReuseCompleteGenerations_WhileSharedOldMapsStayUnchanged(bool contiguous)
    {
        const int pageCount = 16;
        const int commitCount = 128;
        var index = new WalIndex();
        var frames = Enumerable.Range(0, pageCount)
            .Select(i => new WalFrameWrite((uint)i, new byte[PageConstants.PageSize])).ToArray();
        index.PublishCommittedFrames(frames, pageCount * (long)PageConstants.WalFrameSize);
        var oldFirst = index.TakeSnapshot();
        var oldSecond = index.TakeSnapshot();
        Assert.Same(GetPageMap(oldFirst), GetPageMap(oldSecond));
        using var start = new ManualResetEventSlim();
        var ct = TestContext.Current.CancellationToken;

        var writer = Task.Run(() =>
        {
            start.Wait(ct);
            var locations = new (uint PageId, long WalOffset)[pageCount];
            for (int commit = 2; commit <= commitCount; commit++)
            {
                long firstOffset = (long)commit * pageCount * PageConstants.WalFrameSize;
                if (contiguous)
                    index.PublishCommittedFrames(frames, firstOffset);
                else
                {
                    for (int page = 0; page < pageCount; page++)
                        locations[page] = ((uint)page, firstOffset + (long)page * PageConstants.WalFrameSize);
                    index.PublishCommittedFrames(locations);
                }
                Thread.Yield();
            }
        }, ct);

        var readers = Enumerable.Range(0, 2).Select(_ => Task.Run(() =>
        {
            start.Wait(ct);
            for (int read = 0; read < commitCount * 2; read++)
            {
                var first = index.TakeSnapshot();
                Thread.Yield();
                var second = index.TakeSnapshot();
                Assert.NotSame(first, second);
                if (first.CommitCounter == second.CommitCounter)
                    Assert.Same(GetPageMap(first), GetPageMap(second));

                AssertGeneration(first, pageCount);
                AssertGeneration(second, pageCount);
                AssertGeneration(oldFirst, pageCount);
                AssertGeneration(oldSecond, pageCount);
                Assert.Equal(1, oldFirst.CommitCounter);
                Assert.Equal(1, oldSecond.CommitCounter);
            }
        }, ct)).ToArray();

        start.Set();
        await Task.WhenAll(readers.Append(writer));
        Assert.Equal(commitCount, index.CommitCounter);
        AssertGeneration(index.TakeSnapshot(), pageCount);
    }

    private static WalIndex CreateIndex()
    {
        var index = new WalIndex();
        index.PublishCommittedFrames(new (uint, long)[] { (3, 300), (1, 100), (2, 200) });
        return index;
    }

    // Identity is the allocation regression guard; production does not expose the map.
    private static Dictionary<uint, long> GetPageMap(WalSnapshot snapshot)
        => Assert.IsType<Dictionary<uint, long>>(s_snapshotPageMapField.GetValue(snapshot));

    private static void AssertSnapshot(WalSnapshot snapshot, params (uint PageId, long Offset)[] expected)
    {
        Assert.Equal(expected.Length, GetPageMap(snapshot).Count);
        Assert.Equal(expected.Length != 0, snapshot.HasWalFrames);
        Assert.Equal(expected.Length == 0 ? long.MaxValue : expected.Min(page => page.Offset), snapshot.MinimumWalOffset);
        foreach (var page in expected)
        {
            Assert.True(snapshot.TryGet(page.PageId, out long offset));
            Assert.Equal(page.Offset, offset);
        }
    }

    private static void AssertGeneration(WalSnapshot snapshot, int pageCount)
    {
        Assert.Equal(pageCount, GetPageMap(snapshot).Count);
        Assert.True(snapshot.HasWalFrames);
        Assert.Equal(snapshot.CommitCounter * pageCount * PageConstants.WalFrameSize, snapshot.MinimumWalOffset);
        for (uint page = 0; page < pageCount; page++)
        {
            Assert.True(snapshot.TryGet(page, out long offset));
            Assert.Equal((snapshot.CommitCounter * pageCount + page) * PageConstants.WalFrameSize, offset);
        }
    }
}
