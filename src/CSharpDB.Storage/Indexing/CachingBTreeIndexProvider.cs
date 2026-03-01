namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Index provider that composes BTree-backed stores with a bounded find-result cache.
/// </summary>
public sealed class CachingBTreeIndexProvider : IIndexProvider
{
    private readonly int _findCacheCapacity;

    public CachingBTreeIndexProvider(int findCacheCapacity = 2048)
    {
        if (findCacheCapacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(findCacheCapacity), "Cache capacity must be greater than zero.");

        _findCacheCapacity = findCacheCapacity;
    }

    public IIndexStore CreateIndexStore(Pager pager, uint rootPageId)
    {
        var btreeStore = new BTreeIndexStore(new BTree(pager, rootPageId));
        return new CachingIndexStore(btreeStore, _findCacheCapacity);
    }
}
