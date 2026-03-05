namespace CSharpDB.Storage.Caching;

/// <summary>
/// Optional event surface for caches that can report page removals/evictions.
/// </summary>
public interface IPageCacheEvictionEvents
{
    event Action<uint, byte[]>? PageEvicted;
}
