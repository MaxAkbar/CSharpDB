namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Abstraction over index storage operations.
/// </summary>
public interface IIndexStore
{
    uint RootPageId { get; }

    ValueTask<byte[]?> FindAsync(long key, CancellationToken ct = default);
    ValueTask InsertAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default);
    ValueTask<bool> ReplaceAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default);
    ValueTask<bool> DeleteAsync(long key, CancellationToken ct = default);
    IIndexCursor CreateCursor(IndexScanRange range);
}
