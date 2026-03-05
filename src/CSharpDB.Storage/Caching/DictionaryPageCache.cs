namespace CSharpDB.Storage.Caching;

/// <summary>
/// Default in-memory page cache backed by a dictionary.
/// Maintains current behavior (unbounded, no eviction).
/// </summary>
public sealed class DictionaryPageCache : IPageCache, IPageCacheEvictionEvents
{
    private readonly Dictionary<uint, byte[]> _pages = new();
    public event Action<uint, byte[]>? PageEvicted;

    public bool TryGet(uint pageId, out byte[] page) =>
        _pages.TryGetValue(pageId, out page!);

    public void Set(uint pageId, byte[] page)
    {
        if (_pages.TryGetValue(pageId, out var existing) && !ReferenceEquals(existing, page))
            PageEvicted?.Invoke(pageId, existing);

        _pages[pageId] = page;
    }

    public bool Contains(uint pageId) => _pages.ContainsKey(pageId);

    public bool Remove(uint pageId)
    {
        if (!_pages.Remove(pageId, out var page))
            return false;

        PageEvicted?.Invoke(pageId, page);
        return true;
    }

    public void Clear()
    {
        if (PageEvicted != null)
        {
            foreach (var entry in _pages)
                PageEvicted(entry.Key, entry.Value);
        }

        _pages.Clear();
    }
}
