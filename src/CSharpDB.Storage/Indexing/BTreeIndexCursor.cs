namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Adapts a BTreeCursor to the index cursor abstraction.
/// </summary>
public sealed class BTreeIndexCursor : IIndexCursor
{
    private readonly BTreeCursor _cursor;
    private readonly long? _seekStart;
    private bool _initialized;

    public BTreeIndexCursor(BTreeCursor cursor, long? seekStart = null)
    {
        ArgumentNullException.ThrowIfNull(cursor);
        _cursor = cursor;
        _seekStart = seekStart;
        _initialized = false;
    }

    public long CurrentKey => _cursor.CurrentKey;

    public ReadOnlyMemory<byte> CurrentValue => _cursor.CurrentValue;

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_initialized)
            return await _cursor.MoveNextAsync(ct);

        _initialized = true;
        if (_seekStart.HasValue)
            return await _cursor.SeekAsync(_seekStart.Value, ct);

        return await _cursor.MoveNextAsync(ct);
    }
}
