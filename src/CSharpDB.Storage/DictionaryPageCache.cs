namespace CSharpDB.Storage;

/// <summary>
/// Default in-memory page cache backed by a dictionary.
/// Maintains current behavior (unbounded, no eviction).
/// </summary>
public sealed class DictionaryPageCache : IPageCache
{
    private readonly Dictionary<uint, byte[]> _pages = new();

    public bool TryGet(uint pageId, out byte[] page) =>
        _pages.TryGetValue(pageId, out page!);

    public void Set(uint pageId, byte[] page)
    {
        _pages[pageId] = page;
    }

    public bool Contains(uint pageId) => _pages.ContainsKey(pageId);

    public bool Remove(uint pageId) => _pages.Remove(pageId);

    public void Clear() => _pages.Clear();
}
