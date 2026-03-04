namespace CSharpDB.Storage.Caching;

/// <summary>
/// Abstraction for pager page-cache behavior.
/// </summary>
public interface IPageCache
{
    bool TryGet(uint pageId, out byte[] page);
    void Set(uint pageId, byte[] page);
    bool Contains(uint pageId);
    bool Remove(uint pageId);
    void Clear();
}
