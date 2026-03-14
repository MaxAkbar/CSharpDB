namespace CSharpDB.Storage.Paging;

/// <summary>
/// Small bounded cache of immutable WAL frame page images keyed by frame offset.
/// Stored pages are always exposed as read-only buffers so mutable access goes
/// through normal page materialization/copy-on-write paths.
/// </summary>
internal sealed class WalReadCache
{
    private readonly int _capacity;
    private readonly Dictionary<long, CacheEntry> _entries;
    private readonly LinkedList<long> _usageOrder = new();

    private sealed class CacheEntry
    {
        public required PageReadBuffer Page { get; init; }
        public required LinkedListNode<long> Node { get; init; }
    }

    public WalReadCache(int capacity)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be greater than zero.");

        _capacity = capacity;
        _entries = new Dictionary<long, CacheEntry>(capacity);
    }

    public bool TryGet(long walOffset, out PageReadBuffer page)
    {
        if (_entries.TryGetValue(walOffset, out var entry))
        {
            Touch(entry.Node);
            page = entry.Page;
            return true;
        }

        page = default;
        return false;
    }

    public void Set(long walOffset, PageReadBuffer page)
    {
        if (page.TryGetOwnedBuffer(out var ownedPage) && ownedPage is not null)
            page = PageReadBuffer.FromReadOnlyMemory(ownedPage);

        if (_entries.TryGetValue(walOffset, out var existing))
        {
            _entries[walOffset] = new CacheEntry
            {
                Page = page,
                Node = existing.Node,
            };
            Touch(existing.Node);
            return;
        }

        if (_entries.Count >= _capacity)
            EvictLeastRecentlyUsed();

        var node = _usageOrder.AddLast(walOffset);
        _entries[walOffset] = new CacheEntry
        {
            Page = page,
            Node = node,
        };
    }

    public void Clear()
    {
        _usageOrder.Clear();
        _entries.Clear();
    }

    private void EvictLeastRecentlyUsed()
    {
        var first = _usageOrder.First;
        if (first == null)
            return;

        _usageOrder.RemoveFirst();
        _entries.Remove(first.Value);
    }

    private void Touch(LinkedListNode<long> node)
    {
        if (ReferenceEquals(node.List?.Last, node))
            return;

        _usageOrder.Remove(node);
        _usageOrder.AddLast(node);
    }
}
