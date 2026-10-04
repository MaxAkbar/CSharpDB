namespace CSharpDB.Storage.Paging;

/// <summary>
/// Rejects snapshot writes without retaining a pager or allocation delegates.
/// </summary>
internal sealed class ReadOnlyPageAllocator : IPageAllocator
{
    internal static ReadOnlyPageAllocator Instance { get; } = new();

    private ReadOnlyPageAllocator() { }

    public ValueTask<uint> AllocatePageAsync(CancellationToken ct = default)
        => ValueTask.FromException<uint>(
            new InvalidOperationException("Cannot allocate pages on a read-only snapshot pager."));

    public ValueTask FreePageAsync(uint pageId, CancellationToken ct = default)
        => ValueTask.FromException(
            new InvalidOperationException("Cannot free pages on a read-only snapshot pager."));
}
