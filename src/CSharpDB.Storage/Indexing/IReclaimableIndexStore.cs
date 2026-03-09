namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Optional capability for index stores that can reclaim all owned pages.
/// </summary>
public interface IReclaimableIndexStore
{
    ValueTask ReclaimAsync(CancellationToken ct = default);
}
