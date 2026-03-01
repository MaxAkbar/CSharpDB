namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Cursor abstraction for ordered iteration over index entries.
/// </summary>
public interface IIndexCursor
{
    long CurrentKey { get; }
    ReadOnlyMemory<byte> CurrentValue { get; }
    ValueTask<bool> MoveNextAsync(CancellationToken ct = default);
}
