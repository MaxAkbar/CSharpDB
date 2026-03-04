using CSharpDB.Storage.Caching;

namespace CSharpDB.Tests;

public sealed class PageCacheTests
{
    [Fact]
    public void DictionaryPageCache_SupportsSetGetContainsRemoveAndClear()
    {
        var cache = new DictionaryPageCache();
        var page1 = new byte[] { 1, 2, 3 };
        var page2 = new byte[] { 4, 5, 6 };

        cache.Set(1, page1);
        cache.Set(2, page2);

        Assert.True(cache.Contains(1));
        Assert.True(cache.Contains(2));
        Assert.True(cache.TryGet(1, out var cached1));
        Assert.Equal(page1, cached1);

        Assert.True(cache.Remove(1));
        Assert.False(cache.Remove(1));
        Assert.False(cache.Contains(1));
        Assert.True(cache.Contains(2));

        cache.Clear();
        Assert.False(cache.Contains(2));
        Assert.False(cache.TryGet(2, out _));
    }

    [Fact]
    public void LruPageCache_RejectsNonPositiveCapacity()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new LruPageCache(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new LruPageCache(-1));
    }

    [Fact]
    public void LruPageCache_EvictsLeastRecentlyUsedPage()
    {
        var cache = new LruPageCache(capacity: 2);

        cache.Set(1, new byte[] { 1 });
        cache.Set(2, new byte[] { 2 });
        cache.Set(3, new byte[] { 3 });

        Assert.False(cache.Contains(1));
        Assert.True(cache.Contains(2));
        Assert.True(cache.Contains(3));
    }

    [Fact]
    public void LruPageCache_TryGetMovesPageToMostRecentlyUsed()
    {
        var cache = new LruPageCache(capacity: 2);

        cache.Set(1, new byte[] { 1 });
        cache.Set(2, new byte[] { 2 });

        Assert.True(cache.TryGet(1, out _));

        cache.Set(3, new byte[] { 3 });

        Assert.True(cache.Contains(1));
        Assert.False(cache.Contains(2));
        Assert.True(cache.Contains(3));
    }

    [Fact]
    public void LruPageCache_SetExistingPageUpdatesPayloadAndUsageOrder()
    {
        var cache = new LruPageCache(capacity: 2);

        cache.Set(1, new byte[] { 1 });
        cache.Set(2, new byte[] { 2 });
        cache.Set(1, new byte[] { 9 });
        cache.Set(3, new byte[] { 3 });

        Assert.True(cache.Contains(1));
        Assert.False(cache.Contains(2));
        Assert.True(cache.Contains(3));

        Assert.True(cache.TryGet(1, out var updated));
        Assert.Equal(new byte[] { 9 }, updated);
    }

    [Fact]
    public void LruPageCache_RemoveAndClearWorkAsExpected()
    {
        var cache = new LruPageCache(capacity: 3);

        cache.Set(1, new byte[] { 1 });
        cache.Set(2, new byte[] { 2 });
        cache.Set(3, new byte[] { 3 });

        Assert.True(cache.Remove(2));
        Assert.False(cache.Remove(2));
        Assert.False(cache.Contains(2));

        cache.Clear();
        Assert.False(cache.Contains(1));
        Assert.False(cache.Contains(3));
        Assert.False(cache.TryGet(1, out _));
    }
}
