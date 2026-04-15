namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Cursor that never yields rows.
/// </summary>
public sealed class EmptyIndexCursor : IIndexCursor
{
    public static EmptyIndexCursor Instance { get; } = new();

    private EmptyIndexCursor()
    {
    }

    public long CurrentKey => default;

    public ReadOnlyMemory<byte> CurrentValue => ReadOnlyMemory<byte>.Empty;

    public ValueTask<bool> MoveNextAsync(CancellationToken ct = default) =>
        ValueTask.FromResult(false);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
