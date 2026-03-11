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
}
