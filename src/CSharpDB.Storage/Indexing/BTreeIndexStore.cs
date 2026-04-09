namespace CSharpDB.Storage.Indexing;

/// <summary>
/// IIndexStore adapter backed by BTree.
/// </summary>
public sealed class BTreeIndexStore : IIndexStore, ICacheAwareIndexStore, IReclaimableIndexStore
{
    private readonly BTree _tree;
    private readonly string _logicalName;

    public BTreeIndexStore(BTree tree, string logicalName)
    {
        _tree = tree;
        _logicalName = logicalName;
    }

    public string LogicalName => _logicalName;

    public uint RootPageId => _tree.RootPageId;

    public void RecordPointRead(long key)
        => _tree.RecordLogicalIndexRead(_logicalName, key);

    public void RecordRangeRead(IndexScanRange range)
        => _tree.RecordLogicalIndexRangeRead(_logicalName, range);

    public ValueTask<byte[]?> FindAsync(long key, CancellationToken ct = default)
    {
        RecordPointRead(key);
        return _tree.FindAsync(key, ct);
    }

    public bool TryFindCached(long key, out byte[]? payload)
    {
        bool found = _tree.TryFindCached(key, out payload);
        if (found)
            RecordPointRead(key);

        return found;
    }

    public ValueTask<long?> FindMaxKeyAsync(IndexScanRange range, CancellationToken ct = default)
    {
        RecordRangeRead(range);
        return _tree.FindMaxKeyAsync(range, ct);
    }

    public ValueTask InsertAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default) =>
        _tree.InsertAsync(key, payload, ct);

    public ValueTask<bool> ReplaceAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default) =>
        _tree.ReplaceAsync(key, payload, ct);

    public ValueTask<bool> DeleteAsync(long key, CancellationToken ct = default) =>
        _tree.DeleteAsync(key, ct);

    public IIndexCursor CreateCursor(IndexScanRange range)
    {
        if (!TryNormalizeRange(range, out long? startKeyInclusive, out long? upperBoundInclusive))
            return EmptyIndexCursor.Instance;

        RecordRangeRead(range);

        IIndexCursor cursor = new BTreeIndexCursor(_tree.CreateCursor(), startKeyInclusive);
        if (upperBoundInclusive.HasValue)
            cursor = new UpperBoundIndexCursor(cursor, upperBoundInclusive.Value);

        return cursor;
    }

    public ValueTask ReclaimAsync(CancellationToken ct = default) => _tree.ReclaimAsync(ct);

    private static bool TryNormalizeRange(
        IndexScanRange range,
        out long? startKeyInclusive,
        out long? upperBoundInclusive)
    {
        startKeyInclusive = range.LowerBound;
        upperBoundInclusive = range.UpperBound;

        if (startKeyInclusive.HasValue && !range.LowerInclusive)
        {
            if (startKeyInclusive.Value == long.MaxValue)
                return false;
            startKeyInclusive = startKeyInclusive.Value + 1;
        }

        if (upperBoundInclusive.HasValue && !range.UpperInclusive)
        {
            if (upperBoundInclusive.Value == long.MinValue)
                return false;
            upperBoundInclusive = upperBoundInclusive.Value - 1;
        }

        if (startKeyInclusive.HasValue &&
            upperBoundInclusive.HasValue &&
            startKeyInclusive.Value > upperBoundInclusive.Value)
        {
            return false;
        }

        return true;
    }
}
