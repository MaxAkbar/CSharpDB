using CSharpDB.Storage.Wal;

namespace CSharpDB.Tests;

public sealed class WalSnapshotSegmentStorageTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(15)]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(63)]
    [InlineData(64)]
    [InlineData(65)]
    [InlineData(156)]
    [InlineData(157)]
    [InlineData(10000)]
    public void ExactCapacity_PreservesBucketChainsAndEntryOffsets(int capacity)
    {
        var segment = new WalSnapshotSegment(capacity);
        var expected = new Dictionary<uint, long>();
        Assert.Equal(capacity, segment.Capacity);
        Assert.Equal(0, segment.Count);
        Assert.Equal(long.MaxValue, segment.MinimumWalOffset);

        for (int i = 0; i < capacity; i++)
        {
            // Odd multiplication keeps distinct IDs and creates long chains for
            // small bucket counts. Include both boundary IDs and signed offsets.
            uint key = i == 0 ? 0 : i == 1 ? uint.MaxValue : unchecked((uint)(i - 1) * 340573321u);
            long value = i == 0 ? long.MinValue : i == 1 ? long.MaxValue : -(long)i * 0x1_0000_0001;
            expected.Add(key, value);
            segment.AddUnique(key, value);
            Assert.True(segment.TryGetValue(key, out long actual));
            Assert.Equal(value, actual);
        }

        Assert.Equal(capacity, segment.Count);
        Assert.Equal(long.MinValue, segment.MinimumWalOffset);
        uint missing = unchecked((uint)(capacity + 1) * 340573321u);
        Assert.False(expected.ContainsKey(missing));
        Assert.False(segment.TryGetValue(missing, out long absent));
        Assert.Equal(0, absent);

        // Combined backing storage must not expose bucket words as extra entries.
        Assert.Throws<IndexOutOfRangeException>(() => segment.AddUnique(missing, 123));
        Assert.Equal(capacity, segment.Count);
        Assert.Equal(capacity, segment.Capacity);
        foreach (var entry in expected)
        {
            Assert.True(segment.TryGetValue(entry.Key, out long actual));
            Assert.Equal(entry.Value, actual);
        }
        Assert.False(segment.TryGetValue(missing, out _));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    public void NonpositiveCapacity_IsRejected(int capacity)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new WalSnapshotSegment(capacity));
    }

    [Fact]
    public void OverflowingCapacity_IsRejectedBeforeBackingAllocation()
    {
        Assert.Throws<OverflowException>(() => new WalSnapshotSegment(int.MaxValue));
    }

    [Fact]
    public void SeparateSegments_OwnIndependentEntryAndBucketStorage()
    {
        var retained = new WalSnapshotSegment(2);
        retained.AddUnique(0, long.MinValue);
        retained.AddUnique(uint.MaxValue, long.MaxValue);
        var replacement = new WalSnapshotSegment(2);
        replacement.AddUnique(uint.MaxValue, -17);
        replacement.AddUnique(0, 29);

        Assert.True(retained.TryGetValue(0, out long first));
        Assert.True(retained.TryGetValue(uint.MaxValue, out long last));
        Assert.Equal(long.MinValue, first);
        Assert.Equal(long.MaxValue, last);
        Assert.True(replacement.TryGetValue(0, out first));
        Assert.True(replacement.TryGetValue(uint.MaxValue, out last));
        Assert.Equal(29, first);
        Assert.Equal(-17, last);
        Assert.Equal(long.MinValue, retained.MinimumWalOffset);
        Assert.Equal(-17, replacement.MinimumWalOffset);
    }
}
