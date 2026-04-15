namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Abstraction over index storage operations.
/// </summary>
public interface IIndexStore
{
    string LogicalName { get; }

    uint RootPageId { get; }

    void RecordPointRead(long key);
    void RecordRangeRead(IndexScanRange range);
    ValueTask<byte[]?> FindAsync(long key, CancellationToken ct = default);
    ValueTask<long?> FindMaxKeyAsync(IndexScanRange range, CancellationToken ct = default);
    ValueTask InsertAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default);
    ValueTask<bool> ReplaceAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default);
    ValueTask<bool> DeleteAsync(long key, CancellationToken ct = default);
    IIndexCursor CreateCursor(IndexScanRange range);
}
