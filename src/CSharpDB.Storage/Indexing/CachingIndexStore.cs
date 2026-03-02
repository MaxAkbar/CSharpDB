namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Decorates an index store with a bounded in-memory cache for key lookups.
/// Write operations remain delegated to the inner store.
/// </summary>
public sealed class CachingIndexStore : IIndexStore, ICacheAwareIndexStore
{
    private readonly IIndexStore _inner;
    private readonly int _capacity;
    private readonly Dictionary<long, CacheEntry> _entries;
    private readonly LinkedList<long> _usageOrder = new();
    private readonly object _gate = new();

    private sealed class CacheEntry
    {
        public required byte[]? Payload { get; init; }
        public required LinkedListNode<long> Node { get; init; }
    }

    public CachingIndexStore(IIndexStore inner, int capacity)
    {
        ArgumentNullException.ThrowIfNull(inner);
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be greater than zero.");

        _inner = inner;
        _capacity = capacity;
        _entries = new Dictionary<long, CacheEntry>(capacity);
    }

    public uint RootPageId => _inner.RootPageId;

    public async ValueTask<byte[]?> FindAsync(long key, CancellationToken ct = default)
    {
        if (TryGetCached(key, out var cached))
            return cached;

        var value = await _inner.FindAsync(key, ct);
        Cache(key, value);
        return value;
    }

    public async ValueTask InsertAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        await _inner.InsertAsync(key, payload, ct);
        Cache(key, payload.ToArray());
    }

    public async ValueTask<bool> DeleteAsync(long key, CancellationToken ct = default)
    {
        bool removed = await _inner.DeleteAsync(key, ct);
        Remove(key);
        return removed;
    }

    public IIndexCursor CreateCursor(IndexScanRange range) => _inner.CreateCursor(range);

    public bool TryFindCached(long key, out byte[]? payload)
    {
        if (TryGetCached(key, out payload))
            return true;

        if (_inner is ICacheAwareIndexStore innerCache && innerCache.TryFindCached(key, out payload))
        {
            Cache(key, payload);
            return true;
        }

        payload = null;
        return false;
    }

    private bool TryGetCached(long key, out byte[]? payload)
    {
        lock (_gate)
        {
            if (_entries.TryGetValue(key, out var entry))
            {
                Touch(entry.Node);
                payload = entry.Payload;
                return true;
            }
        }

        payload = null;
        return false;
    }

    private void Cache(long key, byte[]? payload)
    {
        lock (_gate)
        {
            if (_entries.TryGetValue(key, out var existing))
            {
                _entries[key] = new CacheEntry
                {
                    Payload = payload,
                    Node = existing.Node,
                };
                Touch(existing.Node);
                return;
            }

            if (_entries.Count >= _capacity)
                EvictLeastRecentlyUsed();

            var node = _usageOrder.AddLast(key);
            _entries[key] = new CacheEntry
            {
                Payload = payload,
                Node = node,
            };
        }
    }

    private void Remove(long key)
    {
        lock (_gate)
        {
            if (!_entries.TryGetValue(key, out var entry))
                return;

            _usageOrder.Remove(entry.Node);
            _entries.Remove(key);
        }
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
