namespace CSharpDB.Data;

internal sealed class PreparedStatementCache
{
    private readonly int _capacity;
    private readonly Dictionary<string, LinkedListNode<CacheEntry>> _map = new(StringComparer.Ordinal);
    private readonly LinkedList<CacheEntry> _lru = new();
    private readonly object _gate = new();

    internal PreparedStatementCache(int capacity)
    {
        _capacity = capacity > 0 ? capacity : 0;
    }

    internal PreparedStatementTemplate GetOrAdd(string sql, Func<string, PreparedStatementTemplate> factory)
    {
        if (_capacity == 0)
            return factory(sql);

        lock (_gate)
        {
            if (_map.TryGetValue(sql, out var hitNode))
            {
                _lru.Remove(hitNode);
                _lru.AddFirst(hitNode);
                return hitNode.Value.Template;
            }
        }

        var created = factory(sql);

        lock (_gate)
        {
            if (_map.TryGetValue(sql, out var existingNode))
            {
                _lru.Remove(existingNode);
                _lru.AddFirst(existingNode);
                return existingNode.Value.Template;
            }

            var entry = new CacheEntry(sql, created);
            var node = _lru.AddFirst(entry);
            _map[sql] = node;

            if (_map.Count > _capacity)
            {
                var tail = _lru.Last;
                if (tail != null)
                {
                    _lru.RemoveLast();
                    _map.Remove(tail.Value.Sql);
                }
            }
        }

        return created;
    }

    private readonly record struct CacheEntry(string Sql, PreparedStatementTemplate Template);
}
