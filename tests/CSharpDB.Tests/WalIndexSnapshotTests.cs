using System.Reflection;
using CSharpDB.Storage.Paging;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class WalIndexSnapshotTests
{
    private static readonly FieldInfo s_snapshotPageMapField = typeof(WalSnapshot)
        .GetField("_pageMap", BindingFlags.Instance | BindingFlags.NonPublic)!;

    [Fact]
    public void CapacityMiss_DoesNotInitializeSnapshotOrAllocateMapStorage()
    {
        var index = CreateIndex();
        var pending = new WalSnapshot();
        Span<int> required = stackalloc int[WalSnapshotMap.SegmentCount];
        index.TryInitializeSnapshot(new WalSnapshot(), null, null, required, out _, out _);
        long before = GC.GetAllocatedBytesForCurrentThread();
        bool initialized = index.TryInitializeSnapshot(pending, null, null, required, out _, out _);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.False(initialized);
        Assert.Equal(3, required[0]);
        Assert.Equal(0, allocated);
        Assert.Null(s_snapshotPageMapField.GetValue(pending));
    }

    [Theory]
    [InlineData(null, 200L)]
    [InlineData(250L, 300L)]
    [InlineData(1000L, long.MaxValue)]
    public void PreparedCacheMiss_CopiesAndFindsMinimumWithoutAllocating(long? floor, long minimum)
    {
        var index = CreateIndex();
        var previous = index.TakeSnapshot(floor);
        index.AddCommittedFrame(1, 400);
        var pending = new WalSnapshot();
        Span<int> required = stackalloc int[WalSnapshotMap.SegmentCount];
        Assert.False(index.TryInitializeSnapshot(pending, null, floor, required, out _, out _));
        var prepared = new WalSnapshotPreparation(1);
        prepared.EnsureCapacity(required);

        long before = GC.GetAllocatedBytesForCurrentThread();
        bool initialized = index.TryInitializeSnapshot(pending, prepared, floor, required, out _, out _);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(initialized);
        Assert.True(prepared.Published);
        Assert.Equal(0, allocated);
        Assert.Equal(minimum, pending.MinimumWalOffset);
        Assert.Equal(1, pending.CommitCounter);
        Assert.Same(GetPageMap(pending), GetPageMap(index.TakeSnapshot(floor)));
        if (floor is null)
        {
            Assert.Same(prepared.Map, GetPageMap(pending));
            AssertSnapshot(pending, (1, 400), (2, 200), (3, 300));
            AssertSnapshot(previous, (1, 100), (2, 200), (3, 300));
        }
        else if (floor == 250)
        {
            Assert.Same(prepared.Map, GetPageMap(pending));
            AssertSnapshot(pending, (1, 400), (3, 300));
            AssertSnapshot(previous, (3, 300));
        }
        else
        {
            AssertSnapshot(pending);
            AssertSnapshot(previous);
        }
    }

    [Fact]
    public void CapacityRetry_CapturesLatestCompleteStateAfterGrowth()
    {
        var index = CreateIndex();
        var pending = new WalSnapshot();
        Span<int> required = stackalloc int[WalSnapshotMap.SegmentCount];
        Assert.False(index.TryInitializeSnapshot(pending, null, null, required, out _, out _));
        var prepared = new WalSnapshotPreparation(1);
        prepared.EnsureCapacity(required);
        int capacity = prepared.Buffers[0]!.Capacity;
        var growth = Enumerable.Range(1, capacity + 1)
            .Select(i => ((uint)(1 + i * WalSnapshotMap.SegmentCount), i * 1000L)).ToArray();
        index.PublishCommittedFrames(growth);

        Assert.False(index.TryInitializeSnapshot(pending, prepared, null, required, out _, out _));
        Assert.True(required[0] > capacity);
        Assert.All(prepared.Buffers.Where(x => x is not null), x => Assert.Equal(0, x!.Count));
        Assert.Null(s_snapshotPageMapField.GetValue(pending));
        prepared.EnsureCapacity(required);
        index.PublishCommittedFrames(new (uint, long)[] { (1, 9000) });
        Assert.True(index.TryInitializeSnapshot(pending, prepared, null, required, out _, out _));
        Assert.Equal(3, pending.CommitCounter);
        Assert.Equal(3 + growth.Length, GetPageMap(pending).Count);
        Assert.True(pending.TryGet(1, out long offset));
        Assert.Equal(9000, offset);
        Assert.Equal(200, pending.MinimumWalOffset);
    }

    [Fact]
    public void PreparedBuffer_AfterResetAndRemap_DoesNotPublishTheOldState()
    {
        var index = CreateIndex();
        var pending = new WalSnapshot();
        Span<int> required = stackalloc int[WalSnapshotMap.SegmentCount];
        Assert.False(index.TryInitializeSnapshot(pending, null, null, required, out _, out _));
        var prepared = new WalSnapshotPreparation(1);
        prepared.EnsureCapacity(required);
        index.Reset();
        index.OverwriteCommittedState(new Dictionary<uint, long> { [7] = 50, [9] = 70 }, 2, 8);

        Assert.True(index.TryInitializeSnapshot(pending, prepared, 60, required, out _, out _));
        Assert.Equal(8, pending.CommitCounter);
        AssertSnapshot(pending, (9, 70));
    }

    [Fact]
    public void CompetingPreparations_ReusePublishedMapAndLeaveLosingBufferPrivate()
    {
        var index = CreateIndex();
        var first = new WalSnapshot();
        var second = new WalSnapshot();
        Span<int> required = stackalloc int[WalSnapshotMap.SegmentCount];
        Assert.False(index.TryInitializeSnapshot(first, null, null, required, out _, out _));
        var firstBuffer = new WalSnapshotPreparation(1);
        firstBuffer.EnsureCapacity(required);
        Assert.False(index.TryInitializeSnapshot(second, null, null, required, out _, out _));
        var secondBuffer = new WalSnapshotPreparation(1);
        secondBuffer.EnsureCapacity(required);
        Assert.True(index.TryInitializeSnapshot(first, firstBuffer, null, required, out _, out _));
        Assert.True(index.TryInitializeSnapshot(second, secondBuffer, null, required, out _, out _));
        Assert.NotSame(first, second);
        Assert.Same(firstBuffer.Map, GetPageMap(first));
        Assert.Same(GetPageMap(first), GetPageMap(second));
        Assert.False(secondBuffer.Published);
        secondBuffer.Buffers[0]!.AddUnique(1, 9000);
        AssertSnapshot(first, (1, 100), (2, 200), (3, 300));
        AssertSnapshot(second, (1, 100), (2, 200), (3, 300));
    }

    [Fact]
    public void PreparedBuffer_MustBeEmptyBeforeCopying()
    {
        var index = CreateIndex();
        var pending = new WalSnapshot();
        var required = new int[WalSnapshotMap.SegmentCount];
        Assert.False(index.TryInitializeSnapshot(pending, null, null, required, out _, out _));
        var prepared = new WalSnapshotPreparation(1);
        prepared.EnsureCapacity(required);
        prepared.Buffers[0]!.AddUnique(1, 9000);
        Assert.Throws<ArgumentException>(() => index.TryInitializeSnapshot(pending, prepared, null, required, out _, out _));
        Assert.Equal(1, prepared.Buffers[0]!.Count);
        Assert.Null(s_snapshotPageMapField.GetValue(pending));
    }

    [Fact]
    public void OnePageCommit_CopiesOnlyItsSegmentAndSharesAllOthers()
    {
        var index = new WalIndex();
        index.PublishCommittedFrames(Enumerable.Range(0, 1024).Select(i => ((uint)i, 100L + i)).ToArray());
        var old = index.TakeSnapshot();
        index.PublishCommittedFrames(new (uint, long)[] { (65, 9000) });
        var pending = new WalSnapshot();
        Span<int> required = stackalloc int[WalSnapshotMap.SegmentCount];
        Assert.False(index.TryInitializeSnapshot(pending, null, null, required, out _, out _));
        Assert.Equal(16, required.ToArray().Sum());
        Assert.Equal(16, required[1]);
        var prepared = new WalSnapshotPreparation(WalSnapshotMap.SegmentCount);
        prepared.EnsureCapacity(required);
        long before = GC.GetAllocatedBytesForCurrentThread();
        Assert.True(index.TryInitializeSnapshot(pending, prepared, null, required, out _, out _));
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(0, allocated);
        Assert.Equal(1024, GetPageMap(pending).Count);
        for (int i = 0; i < WalSnapshotMap.SegmentCount; i++)
        {
            if (i == 1)
                Assert.NotSame(GetPageMap(old).Segments[i], GetPageMap(pending).Segments[i]);
            else
                Assert.Same(GetPageMap(old).Segments[i], GetPageMap(pending).Segments[i]);
        }
        Assert.True(old.TryGet(65, out long oldOffset));
        Assert.Equal(165, oldOffset);
        Assert.True(pending.TryGet(65, out long newOffset));
        Assert.Equal(9000, newOffset);
        Assert.Equal(100, pending.MinimumWalOffset);
    }

    [Fact]
    public void Preparation_RechecksCompactToSegmentedTransitionAndPreservesBothViews()
    {
        var index = new WalIndex();
        index.PublishCommittedFrames(Enumerable.Range(0, 64).Select(i => ((uint)i, 100L + i)).ToArray());
        var old = index.TakeSnapshot();
        Assert.Single(GetPageMap(old).Segments);
        index.AddCommittedFrame(0, 200);
        var pending = new WalSnapshot();
        Span<int> required = stackalloc int[WalSnapshotMap.SegmentCount];
        Assert.False(index.TryInitializeSnapshot(pending, null, null, required, out int segmentCount, out _));
        Assert.Equal(1, segmentCount);
        var compact = new WalSnapshotPreparation(segmentCount);
        compact.EnsureCapacity(required);
        index.PublishCommittedFrames(new (uint, long)[] { (64, 500) });
        Assert.False(index.TryInitializeSnapshot(pending, compact, null, required, out segmentCount, out int keyCapacity));
        Assert.Equal(64, segmentCount);
        Assert.False(compact.Published);
        Assert.Equal(0, compact.Buffers[0]!.Count);
        var segmented = new WalSnapshotPreparation(segmentCount);
        segmented.EnsureCapacity(required, keyCapacity);
        Assert.True(index.TryInitializeSnapshot(pending, segmented, null, required, out _, out _));
        Assert.Equal(65, GetPageMap(pending).Count);
        Assert.Equal(64, GetPageMap(pending).Segments.Length);
        Assert.True(old.TryGet(0, out long original));
        Assert.Equal(100, original);
        Assert.False(old.TryGet(64, out _));
        index.OverwriteCommittedState(new Dictionary<uint, long> { [0] = 10, [uint.MaxValue] = 20 }, 2, 8);
        var smallAgain = index.TakeSnapshot(15);
        Assert.Single(GetPageMap(smallAgain).Segments);
        AssertSnapshot(smallAgain, (uint.MaxValue, 20));
        Assert.True(pending.TryGet(0, out long updated));
        Assert.Equal(200, updated);
        Assert.True(pending.TryGet(64, out long added));
        Assert.Equal(500, added);
    }

    [Fact]
    public void Preparation_RechecksNewDirtySegmentsBeforeCopyingAnyEntries()
    {
        var index = new WalIndex();
        index.PublishCommittedFrames(Enumerable.Range(0, 128).Select(i => ((uint)i, 100L + i)).ToArray());
        var old = index.TakeSnapshot();
        index.PublishCommittedFrames(new (uint, long)[] { (1, 9000) });
        var pending = new WalSnapshot();
        Span<int> required = stackalloc int[WalSnapshotMap.SegmentCount];
        Assert.False(index.TryInitializeSnapshot(pending, null, null, required, out int segmentCount, out _));
        var prepared = new WalSnapshotPreparation(segmentCount);
        prepared.EnsureCapacity(required);
        Assert.Null(prepared.Buffers[2]);
        index.PublishCommittedFrames(new (uint, long)[] { (2, 10000) });
        Assert.False(index.TryInitializeSnapshot(pending, prepared, null, required, out _, out _));
        Assert.Equal(0, prepared.Buffers[1]!.Count);
        Assert.Equal(2, required[2]);
        prepared.EnsureCapacity(required);
        Assert.True(index.TryInitializeSnapshot(pending, prepared, null, required, out _, out _));
        Assert.Equal(3, pending.CommitCounter);
        Assert.True(pending.TryGet(1, out long first));
        Assert.True(pending.TryGet(2, out long second));
        Assert.Equal(9000, first);
        Assert.Equal(10000, second);
        Assert.True(old.TryGet(1, out first));
        Assert.Equal(101, first);
    }

    [Fact]
    public void EmptyFilteredLargeMap_RetainsSegmentSharingWhenAFrameCrossesTheFloor()
    {
        var index = new WalIndex();
        index.PublishCommittedFrames(Enumerable.Range(0, 128).Select(i => ((uint)i, 100L + i)).ToArray());
        var empty = index.TakeSnapshot(1000);
        Assert.False(empty.HasWalFrames);
        Assert.Equal(64, GetPageMap(empty).Segments.Length);
        index.PublishCommittedFrames(new (uint, long)[] { (2, 1100) });
        var pending = new WalSnapshot();
        Span<int> required = stackalloc int[WalSnapshotMap.SegmentCount];
        Assert.False(index.TryInitializeSnapshot(pending, null, 1000, required, out int segmentCount, out _));
        Assert.Equal(2, required.ToArray().Sum());
        var prepared = new WalSnapshotPreparation(segmentCount);
        prepared.EnsureCapacity(required);
        Assert.True(index.TryInitializeSnapshot(pending, prepared, 1000, required, out _, out _));
        AssertSnapshot(pending, (2, 1100));
        AssertSnapshot(empty);
    }

    [Fact]
    public void RepeatedOverwrites_RecomputeMinimumWithoutKeepingHistoricalValues()
    {
        var index = new WalIndex();
        index.PublishCommittedFrames(new (uint, long)[] { (0, 10), (64, 20), (1, 30), (uint.MaxValue, 40) });
        var oldest = index.TakeSnapshot();
        index.PublishCommittedFrames(new (uint, long)[] { (0, 100), (64, 200) });
        var next = index.TakeSnapshot();
        Assert.Equal(30, next.MinimumWalOffset);
        index.PublishCommittedFrames(new (uint, long)[] { (1, 300), (uint.MaxValue, 400) });
        Assert.Equal(100, index.TakeSnapshot().MinimumWalOffset);
        AssertSnapshot(oldest, (0, 10), (64, 20), (1, 30), (uint.MaxValue, 40));
        AssertSnapshot(next, (0, 100), (64, 200), (1, 30), (uint.MaxValue, 40));
        Assert.False(next.TryGet(uint.MaxValue - 64, out long missing));
        Assert.Equal(0, missing);
    }

    [Fact]
    public void RandomizedMutationAndFloorChanges_MatchIndependentDictionarySnapshots()
    {
        var random = new Random(74565);
        var index = new WalIndex();
        var expected = new Dictionary<uint, long>();
        var retained = new List<(WalSnapshot Snapshot, Dictionary<uint, long> Expected)>();
        uint[] keys = [0, 1, 64, 128, 192, 255, 1024, uint.MaxValue, uint.MaxValue - 64];
        for (int step = 0; step < 300; step++)
        {
            if (step % 47 == 0)
            {
                expected.Clear();
                index.Reset();
            }
            else if (step % 31 == 0)
            {
                expected.Remove(keys[random.Next(keys.Length)]);
                index.OverwriteCommittedState(expected, expected.Count, index.CommitCounter);
            }
            else
            {
                var frames = new (uint, long)[random.Next(1, 6)];
                for (int i = 0; i < frames.Length; i++)
                {
                    uint key = keys[random.Next(keys.Length)];
                    long value = random.Next(-2, 500);
                    frames[i] = (key, value);
                    expected[key] = value;
                }
                index.PublishCommittedFrames(frames);
            }
            long? floor = step % 4 == 0 ? null : random.Next(-2, 550);
            var filtered = expected.Where(x => !floor.HasValue || x.Value >= floor.Value).ToDictionary();
            var snapshot = index.TakeSnapshot(floor);
            retained.Add((snapshot, filtered));
            foreach (var item in retained)
            {
                Assert.Equal(item.Expected.Count, GetPageMap(item.Snapshot).Count);
                Assert.Equal(item.Expected.Count == 0 ? long.MaxValue : item.Expected.Values.Min(), item.Snapshot.MinimumWalOffset);
                foreach (uint key in keys)
                {
                    Assert.Equal(item.Expected.TryGetValue(key, out long wanted), item.Snapshot.TryGet(key, out long actual));
                    Assert.Equal(wanted, actual);
                }
            }
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData(500L)]
    [InlineData(1000L)]
    public void PreallocatedWrapper_CapturesStateAtInitialization(long? floor)
    {
        var index = CreateIndex();
        var previous = index.TakeSnapshot();
        var pending = new WalSnapshot();

        // A checkpoint and a commit may finish while this reader awaits admission.
        index.Reset();
        index.PublishCommittedFrames(new (uint, long)[] { (2, 400), (4, 500) });
        index.InitializeSnapshot(pending, floor);
        index.PublishCommittedFrames(new (uint, long)[] { (4, 900) });

        Assert.Equal(2, pending.CommitCounter);
        if (floor is null)
            AssertSnapshot(pending, (2, 400), (4, 500));
        else if (floor == 500)
            AssertSnapshot(pending, (4, 500));
        else
            AssertSnapshot(pending);
        Assert.Equal(1, previous.CommitCounter);
        AssertSnapshot(previous, (1, 100), (2, 200), (3, 300));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void PublishedSnapshot_CannotBeInitializedAgain(bool empty)
    {
        var index = CreateIndex();
        var published = index.TakeSnapshot(empty ? 1000L : null);
        var map = GetPageMap(published);
        index.PublishCommittedFrames(new (uint, long)[] { (1, 1100) });

        Assert.Throws<InvalidOperationException>(() => index.InitializeSnapshot(published));

        Assert.Same(map, GetPageMap(published));
        Assert.Equal(1, published.CommitCounter);
        if (empty)
            AssertSnapshot(published);
        else
            AssertSnapshot(published, (1, 100), (2, 200), (3, 300));
    }

    [Fact]
    public void InitializingPreallocatedWrappers_WithCachedMap_DoesNotAllocate()
    {
        var index = CreateIndex();
        var warm = index.TakeSnapshot();
        var pending = new WalSnapshot[128];
        for (int i = 0; i < pending.Length; i++)
            pending[i] = new WalSnapshot();

        long before = GC.GetAllocatedBytesForCurrentThread();
        foreach (var snapshot in pending)
            index.InitializeSnapshot(snapshot);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(0, allocated);
        Assert.All(pending, snapshot =>
        {
            Assert.Same(GetPageMap(warm), GetPageMap(snapshot));
            Assert.Equal(1, snapshot.CommitCounter);
        });
    }

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
    private static WalSnapshotMap GetPageMap(WalSnapshot snapshot)
        => Assert.IsType<WalSnapshotMap>(s_snapshotPageMapField.GetValue(snapshot));

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
