using CSharpDB.Storage.Paging;

namespace CSharpDB.Tests;

public sealed class WalReadCacheTests
{
    [Fact]
    public void ReplacementRemainsReadOnlyAndTouchesUsageOrder()
    {
        var cache = new WalReadCache(capacity: 2);
        byte[] replacement = [3, 4];
        cache.Set(100, PageReadBuffer.FromOwnedBuffer([1, 2]));
        cache.Set(200, PageReadBuffer.FromOwnedBuffer([5, 6]));
        cache.Set(100, PageReadBuffer.FromOwnedBuffer(replacement));
        cache.Set(300, PageReadBuffer.FromReadOnlyMemory(new byte[] { 7, 8 }));

        Assert.Equal(2, cache.Count);
        Assert.False(cache.TryGet(200, out _));
        Assert.True(cache.TryGet(100, out var page));
        Assert.Equal(replacement, page.Memory.ToArray());
        Assert.False(page.TryGetOwnedBuffer(out _));
        byte[] mutable = page.MaterializeOwnedBuffer();
        mutable[0] = 99;
        Assert.Equal(3, page.Memory.Span[0]);
        Assert.Equal(3, replacement[0]);

        cache.Clear();
        Assert.Equal(0, cache.Count);
        Assert.False(cache.TryGet(100, out _));
        cache.Set(400, PageReadBuffer.FromOwnedBuffer(replacement));
        Assert.True(cache.TryGet(400, out _));
    }
}
