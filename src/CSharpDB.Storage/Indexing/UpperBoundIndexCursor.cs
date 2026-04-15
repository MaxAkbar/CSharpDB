namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Stops iteration when keys pass an inclusive upper bound.
/// </summary>
public sealed class UpperBoundIndexCursor : IIndexCursor
{
    private readonly IIndexCursor _inner;
    private readonly long _upperBoundInclusive;
    private bool _eof;

    public UpperBoundIndexCursor(IIndexCursor inner, long upperBoundInclusive)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
        _upperBoundInclusive = upperBoundInclusive;
    }

    public long CurrentKey => _inner.CurrentKey;

    public ReadOnlyMemory<byte> CurrentValue => _inner.CurrentValue;

    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_eof)
            return false;

        if (!await _inner.MoveNextAsync(ct))
        {
            _eof = true;
            return false;
        }

        if (_inner.CurrentKey > _upperBoundInclusive)
        {
            _eof = true;
            return false;
        }

        return true;
    }

    public ValueTask DisposeAsync() => _inner.DisposeAsync();
}
