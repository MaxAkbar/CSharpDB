namespace CSharpDB.Storage.Paging;

/// <summary>
/// Manages page allocation and free-list reuse.
/// </summary>
public interface IPageAllocator
{
    ValueTask<uint> AllocatePageAsync(CancellationToken ct = default);
    ValueTask FreePageAsync(uint pageId, CancellationToken ct = default);
}
