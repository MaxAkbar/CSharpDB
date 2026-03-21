using CSharpDB.Storage.Indexing;

namespace CSharpDB.Tests;

public sealed class RowIdPayloadCodecTests
{
    [Fact]
    public void TryInsertSorted_InsertsInOrder_AndPreventsDuplicates()
    {
        byte[] initial = RowIdPayloadCodec.CreateSingle(20);
        Assert.True(RowIdPayloadCodec.TryInsertSorted(initial, 10, out byte[] withTen));
        Assert.True(RowIdPayloadCodec.TryInsertSorted(withTen, 30, out byte[] ordered));
        Assert.False(RowIdPayloadCodec.TryInsertSorted(ordered, 20, out _));

        Assert.Equal(3, RowIdPayloadCodec.GetCount(ordered));
        Assert.Equal(10, RowIdPayloadCodec.ReadAt(ordered, 0));
        Assert.Equal(20, RowIdPayloadCodec.ReadAt(ordered, 1));
        Assert.Equal(30, RowIdPayloadCodec.ReadAt(ordered, 2));
    }

    [Fact]
    public void TryRemoveSorted_RemovesEntries_AndDeletesWhenLastRowRemains()
    {
        byte[] payload = RowIdPayloadCodec.CreateSingle(20);
        Assert.True(RowIdPayloadCodec.TryInsertSorted(payload, 10, out byte[] withTen));
        Assert.True(RowIdPayloadCodec.TryInsertSorted(withTen, 30, out byte[] ordered));

        Assert.True(RowIdPayloadCodec.TryRemoveSorted(ordered, 20, out byte[]? withoutMiddle));
        Assert.NotNull(withoutMiddle);
        Assert.Equal(2, RowIdPayloadCodec.GetCount(withoutMiddle));
        Assert.Equal(10, RowIdPayloadCodec.ReadAt(withoutMiddle, 0));
        Assert.Equal(30, RowIdPayloadCodec.ReadAt(withoutMiddle, 1));

        Assert.True(RowIdPayloadCodec.TryRemoveSorted(RowIdPayloadCodec.CreateSingle(99), 99, out byte[]? deleteEntry));
        Assert.Null(deleteEntry);
    }

    [Fact]
    public void TryInsert_PreservesLegacyUnsortedPayloads_AndPreventsDuplicates()
    {
        byte[] legacy = RowIdPayloadCodec.CreateFromSorted(new long[] { 10, 30 });
        byte[] unsorted = new byte[legacy.Length];
        legacy.AsSpan(8, 8).CopyTo(unsorted.AsSpan(0, 8));
        legacy.AsSpan(0, 8).CopyTo(unsorted.AsSpan(8, 8));

        Assert.True(RowIdPayloadCodec.TryInsert(unsorted, 20, out byte[] appended));
        Assert.Equal(3, RowIdPayloadCodec.GetCount(appended));
        Assert.Equal(30, RowIdPayloadCodec.ReadAt(appended, 0));
        Assert.Equal(10, RowIdPayloadCodec.ReadAt(appended, 1));
        Assert.Equal(20, RowIdPayloadCodec.ReadAt(appended, 2));

        Assert.False(RowIdPayloadCodec.TryInsert(appended, 10, out _));
    }

    [Fact]
    public void TryRemove_RemovesEntries_FromLegacyUnsortedPayloads()
    {
        byte[] payload = RowIdPayloadCodec.CreateFromSorted(new long[] { 30, 10, 20 });

        Assert.True(RowIdPayloadCodec.TryRemove(payload, 10, out byte[]? trimmed));
        Assert.NotNull(trimmed);
        Assert.Equal(2, RowIdPayloadCodec.GetCount(trimmed));
        Assert.Equal(30, RowIdPayloadCodec.ReadAt(trimmed, 0));
        Assert.Equal(20, RowIdPayloadCodec.ReadAt(trimmed, 1));

        Assert.True(RowIdPayloadCodec.TryRemove(RowIdPayloadCodec.CreateSingle(5), 5, out byte[]? deleted));
        Assert.Null(deleted);
    }
}
