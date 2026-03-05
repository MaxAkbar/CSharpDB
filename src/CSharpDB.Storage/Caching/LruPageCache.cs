namespace CSharpDB.Storage.Caching;

/// <summary>
/// Bounded page cache with LRU eviction semantics.
/// </summary>
public sealed class LruPageCache : IPageCache, IPageCacheEvictionEvents
{
    private readonly int _capacity;
    private readonly Dictionary<uint, CacheEntry> _entries;
    private readonly LinkedList<uint> _usageOrder = new();
    public event Action<uint, byte[]>? PageEvicted;

    private sealed class CacheEntry
    {
        public required byte[] Page { get; init; }
        public required LinkedListNode<uint> Node { get; init; }
    }

    public LruPageCache(int capacity)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be greater than zero.");

        _capacity = capacity;
        _entries = new Dictionary<uint, CacheEntry>(capacity);
    }

    public bool TryGet(uint pageId, out byte[] page)
    {
        if (_entries.TryGetValue(pageId, out var entry))
        {
            Touch(entry.Node);
            page = entry.Page;
            return true;
        }

        page = null!;
        return false;
    }

    public void Set(uint pageId, byte[] page)
    {
        if (_entries.TryGetValue(pageId, out var existing))
        {
            if (!ReferenceEquals(existing.Page, page))
                PageEvicted?.Invoke(pageId, existing.Page);

            _entries[pageId] = new CacheEntry
            {
                Page = page,
                Node = existing.Node,
            };
            Touch(existing.Node);
            return;
        }

        if (_entries.Count >= _capacity)
            EvictLeastRecentlyUsed();

        var node = _usageOrder.AddLast(pageId);
        _entries[pageId] = new CacheEntry
        {
            Page = page,
            Node = node,
        };
    }

    public bool Contains(uint pageId) => _entries.ContainsKey(pageId);

    public bool Remove(uint pageId)
    {
        if (!_entries.TryGetValue(pageId, out var entry))
            return false;

        _usageOrder.Remove(entry.Node);
        _entries.Remove(pageId);
        PageEvicted?.Invoke(pageId, entry.Page);
        return true;
    }

    public void Clear()
    {
        if (PageEvicted != null)
        {
            foreach (var entry in _entries)
                PageEvicted(entry.Key, entry.Value.Page);
        }

        _usageOrder.Clear();
        _entries.Clear();
    }

    private void EvictLeastRecentlyUsed()
    {
        var first = _usageOrder.First;
        if (first == null)
            return;

        _usageOrder.RemoveFirst();
        uint pageId = first.Value;
        if (_entries.Remove(pageId, out var entry))
            PageEvicted?.Invoke(pageId, entry.Page);
    }

    private void Touch(LinkedListNode<uint> node)
    {
        if (ReferenceEquals(node.List?.Last, node))
            return;

        _usageOrder.Remove(node);
        _usageOrder.AddLast(node);
    }
}
