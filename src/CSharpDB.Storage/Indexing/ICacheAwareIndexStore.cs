namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Optional index-store capability for cache-only lookups.
/// Returns true when a definitive cached answer is available.
/// </summary>
public interface ICacheAwareIndexStore
{
    bool TryFindCached(long key, out byte[]? payload);
}

