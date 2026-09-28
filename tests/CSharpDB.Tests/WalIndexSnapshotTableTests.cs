using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class WalIndexSnapshotTableTests
{
    [Fact]
    public void PartialCapacityRetry_ClearsRequirementsForSegmentsPublishedByAnotherCapture()
    {
        var index = new WalIndex();
        index.PublishCommittedFrames(Enumerable.Range(0, 128).Select(i => ((uint)i, 100L + i)).ToArray());
        var retained = index.TakeSnapshot();
        index.AddCommittedFrame(1, 9000);
        var pending = new WalSnapshot();
        var required = Enumerable.Repeat(-1, WalSnapshotMap.SegmentCount).ToArray();
        Assert.False(index.TryInitializeSnapshot(pending, null, null, required, out int segments, out int keyCapacity));
        Assert.Equal(WalSnapshotMap.SegmentCount, segments);
        Assert.Equal(0, keyCapacity);
        for (int i = 0; i < required.Length; i++)
            Assert.Equal(i == 1 ? 2 : 0, required[i]);
        var prepared = new WalSnapshotPreparation(segments);
        prepared.EnsureCapacity(required, keyCapacity);

        var competing = index.TakeSnapshot();
        index.AddCommittedFrame(63, 10000);
        Array.Fill(required, -1);
        Assert.False(index.TryInitializeSnapshot(pending, prepared, null, required, out _, out keyCapacity));
        Assert.Equal(0, keyCapacity);
        for (int i = 0; i < required.Length; i++)
            Assert.Equal(i == 63 ? 2 : 0, required[i]);
        Assert.False(prepared.Published);
        Assert.Equal(0, prepared.Buffers[1]!.Count);
        Assert.All(prepared.Map.Segments, segment => Assert.Null(segment));
        prepared.EnsureCapacity(required, keyCapacity);
        Assert.True(index.TryInitializeSnapshot(pending, prepared, null, required, out _, out _));
        Assert.Equal(128, prepared.Map.Count);
        Assert.True(pending.TryGet(1, out long offset));
        Assert.Equal(9000, offset);
        Assert.True(pending.TryGet(63, out offset));
        Assert.Equal(10000, offset);
        Assert.True(competing.TryGet(63, out offset));
        Assert.Equal(163, offset);
        Assert.True(retained.TryGet(1, out offset));
        Assert.Equal(101, offset);
    }

    [Fact]
    public void PartialFilteredCopy_RemovesADirtySegmentWhenEveryPageFallsBelowTheFloor()
    {
        var index = new WalIndex();
        index.PublishCommittedFrames(Enumerable.Range(0, 128).Select(i => ((uint)i, 2000L + i)).ToArray());
        var retained = index.TakeSnapshot(1000);
        index.PublishCommittedFrames(new (uint, long)[] { (63, 100), (127, 200) });
        var pending = new WalSnapshot();
        var required = new int[WalSnapshotMap.SegmentCount];
        Assert.False(index.TryInitializeSnapshot(pending, null, 1000, required, out int segments, out int keyCapacity));
        Assert.Equal(2, required.Sum());
        Assert.Equal(2, required[63]);
        var prepared = new WalSnapshotPreparation(segments);
        prepared.EnsureCapacity(required, keyCapacity);
        Assert.True(index.TryInitializeSnapshot(pending, prepared, 1000, required, out _, out _));

        Assert.Null(prepared.Map.Segments[63]);
        Assert.Equal(126, prepared.Map.Count);
        Assert.Equal(2000, pending.MinimumWalOffset);
        for (uint page = 0; page < 128; page++)
        {
            Assert.Equal(page is not (63 or 127), pending.TryGet(page, out long offset));
            Assert.Equal(page is 63 or 127 ? 0 : 2000L + page, offset);
            Assert.True(retained.TryGet(page, out offset));
            Assert.Equal(2000L + page, offset);
        }
    }

    [Fact]
    public void PartialPreparation_FloorChangeRequiresAllSegmentsBeforeCopying()
    {
        var index = new WalIndex();
        index.PublishCommittedFrames(Enumerable.Range(0, 128).Select(i => ((uint)i, 900L + i)).ToArray());
        var retained = index.TakeSnapshot(1000);
        index.AddCommittedFrame(1, 2000);
        var pending = new WalSnapshot();
        var required = new int[WalSnapshotMap.SegmentCount];
        Assert.False(index.TryInitializeSnapshot(pending, null, 1000, required, out int segments, out int keyCapacity));
        Assert.Equal(2, required.Sum());
        var prepared = new WalSnapshotPreparation(segments);
        prepared.EnsureCapacity(required, keyCapacity);

        // Only segment 1 was written, but changing the floor also changes clean views.
        Assert.False(index.TryInitializeSnapshot(pending, prepared, 950, required, out _, out keyCapacity));
        Assert.Equal(0, keyCapacity);
        Assert.All(required, capacity => Assert.Equal(2, capacity));
        Assert.False(prepared.Published);
        Assert.Equal(0, prepared.Buffers[1]!.Count);
        Assert.All(prepared.Map.Segments, segment => Assert.Null(segment));
        prepared.EnsureCapacity(required, keyCapacity);
        Assert.True(index.TryInitializeSnapshot(pending, prepared, 950, required, out _, out _));
        Assert.Equal(79, prepared.Map.Count);
        Assert.Equal(950, pending.MinimumWalOffset);

        // A second floor change has no writer dirty bits to drive the rebuild.
        var higherFloor = index.TakeSnapshot(1100);
        Assert.Equal(2000, higherFloor.MinimumWalOffset);
        for (uint page = 0; page < 128; page++)
        {
            Assert.Equal(page >= 100, retained.TryGet(page, out long offset));
            if (page >= 100) Assert.Equal(900L + page, offset);
            Assert.Equal(page == 1 || page >= 50, pending.TryGet(page, out offset));
            if (page == 1 || page >= 50) Assert.Equal(page == 1 ? 2000 : 900L + page, offset);
            Assert.Equal(page == 1, higherFloor.TryGet(page, out offset));
            if (page == 1) Assert.Equal(2000, offset);
        }
    }

    [Theory]
    [InlineData(64)]
    [InlineData(65)]
    public void OffsetFloorChanges_PreserveExtremeOffsetsAcrossLayouts(int count)
    {
        var index = new WalIndex();
        var expected = new Dictionary<uint, long>
        {
            [0] = long.MinValue,
            [uint.MaxValue] = long.MaxValue,
        };
        for (uint key = 1; expected.Count < count; key++)
            expected.Add(key, (long)key - 32);
        index.PublishCommittedFrames(expected.Select(e => (e.Key, e.Value)).ToArray());

        long?[] floors = [null, long.MinValue, -1, 0, long.MaxValue];
        var retained = floors.Select(floor => index.TakeSnapshot(floor)).ToArray();
        index.PublishCommittedFrames(new (uint, long)[] { (0, 100), (uint.MaxValue, 200) });
        var latest = index.TakeSnapshot();

        for (int i = 0; i < floors.Length; i++)
        {
            long floor = floors[i].GetValueOrDefault(long.MinValue);
            foreach (var entry in expected)
            {
                bool included = entry.Value >= floor;
                Assert.Equal(included, retained[i].TryGet(entry.Key, out long offset));
                if (included)
                    Assert.Equal(entry.Value, offset);
            }
            Assert.Equal(expected.Values.Where(offset => offset >= floor).Min(), retained[i].MinimumWalOffset);
        }
        Assert.True(latest.TryGet(0, out long first));
        Assert.Equal(100, first);
        Assert.True(latest.TryGet(uint.MaxValue, out long last));
        Assert.Equal(200, last);
    }

    [Fact]
    public void CollidingKeys_PreserveEveryOffsetAndTerminateMissingLookups()
    {
        // These distinct IDs collide in a 16-bucket snapshot table. Include a
        // missing member of the same chain to check unsuccessful lookup too.
        uint[] keys = Enumerable.Range(0, 17).Select(i => unchecked((uint)i * 340573321u)).ToArray();
        var index = new WalIndex();
        index.PublishCommittedFrames(keys[..16].Select((key, i) => (key, (long)i - 8)).ToArray());
        var old = index.TakeSnapshot();
        var filtered = index.TakeSnapshot(0);
        index.PublishCommittedFrames(new (uint, long)[] { (keys[0], 999), (keys[7], 555), (keys[0], 888) });
        var latest = index.TakeSnapshot();

        for (int i = 0; i < 16; i++)
        {
            Assert.True(old.TryGet(keys[i], out long value));
            Assert.Equal((long)i - 8, value);
            Assert.Equal(i >= 8, filtered.TryGet(keys[i], out value));
            if (i >= 8) Assert.Equal((long)i - 8, value);
            Assert.True(latest.TryGet(keys[i], out value));
            Assert.Equal(i == 0 ? 888L : i == 7 ? 555L : (long)i - 8, value);
        }
        Assert.False(old.TryGet(keys[16], out long missing));
        Assert.Equal(0, missing);
        Assert.Equal(-8, old.MinimumWalOffset);
        Assert.Equal(0, filtered.MinimumWalOffset);
    }

    [Theory]
    [InlineData(16)]
    [InlineData(64)]
    [InlineData(65)]
    [InlineData(1000)]
    [InlineData(10000)]
    public void FullAndNarrowRebuilds_PreserveBoundaryKeysAndRetainedGenerations(int count)
    {
        var index = new WalIndex();
        var expected = new Dictionary<uint, long> { [0] = -100, [uint.MaxValue] = 0 };
        // Mix concentrated keys with distinct keys spanning the entire uint range.
        uint state = 0x9E3779B9;
        while (expected.Count < count)
        {
            state ^= state << 13; state ^= state >> 17; state ^= state << 5;
            uint key = expected.Count % 2 == 0 ? (uint)expected.Count * 64 : state;
            expected.TryAdd(key, expected.Count * 100L);
        }
        index.PublishCommittedFrames(expected.Select(e => (e.Key, e.Value)).ToArray());
        var oldest = index.TakeSnapshot();
        var fullValues = expected.ToDictionary(e => e.Key, e => e.Value + 1_000_000);
        index.PublishCommittedFrames(fullValues.Reverse().Select(e => (e.Key, e.Value)).ToArray());
        var full = index.TakeSnapshot();
        index.PublishCommittedFrames(new (uint, long)[] { (0, 8_000_000), (uint.MaxValue, 9_000_000) });
        var narrow = index.TakeSnapshot();
        var filtered = index.TakeSnapshot(8_000_000);
        index.Reset();
        index.AddCommittedFrame(0, 1);

        foreach (var entry in expected)
        {
            Assert.True(oldest.TryGet(entry.Key, out long original));
            Assert.Equal(entry.Value, original);
            Assert.True(full.TryGet(entry.Key, out long broad));
            Assert.Equal(fullValues[entry.Key], broad);
            Assert.True(narrow.TryGet(entry.Key, out long latest));
            Assert.Equal(entry.Key == 0 ? 8_000_000 : entry.Key == uint.MaxValue ? 9_000_000 : fullValues[entry.Key], latest);
            Assert.Equal(entry.Key is 0 or uint.MaxValue, filtered.TryGet(entry.Key, out _));
        }
        Assert.Equal(-100, oldest.MinimumWalOffset);
        Assert.Equal(999_900, full.MinimumWalOffset);
        Assert.Equal(8_000_000, filtered.MinimumWalOffset);
    }
}
