using CSharpDB.Storage.Paging;

namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Transparently spills oversized index payloads into overflow-page chains so
/// duplicate-heavy secondary-index buckets do not need to fit inside one B-tree leaf cell.
/// </summary>
public sealed class OverflowingIndexStore : IIndexStore, ICacheAwareIndexStore, IReclaimableIndexStore
{
    private readonly IIndexStore _inner;
    private readonly Pager _pager;

    public OverflowingIndexStore(IIndexStore inner, Pager pager)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(pager);

        _inner = inner;
        _pager = pager;
    }

    public uint RootPageId => _inner.RootPageId;

    public string LogicalName => _inner.LogicalName;

    public void RecordPointRead(long key)
        => _inner.RecordPointRead(key);

    public void RecordRangeRead(IndexScanRange range)
        => _inner.RecordRangeRead(range);

    public async ValueTask<byte[]?> FindAsync(long key, CancellationToken ct = default)
    {
        byte[]? storedPayload = await _inner.FindAsync(key, ct);
        if (storedPayload == null || !IndexOverflowReferenceCodec.IsEncoded(storedPayload))
            return storedPayload;

        return await IndexOverflowPageStore.ReadAsync(_pager, storedPayload, ct);
    }

    public ValueTask<long?> FindMaxKeyAsync(IndexScanRange range, CancellationToken ct = default)
        => _inner.FindMaxKeyAsync(range, ct);

    public async ValueTask InsertAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        byte[]? overflowReference = null;
        try
        {
            ReadOnlyMemory<byte> storedPayload = payload;
            if (IndexOverflowPageStore.RequiresOverflow(payload.Length))
            {
                overflowReference = await IndexOverflowPageStore.WriteAsync(_pager, payload, ct);
                storedPayload = overflowReference;
            }

            await _inner.InsertAsync(key, storedPayload, ct);
        }
        catch
        {
            if (overflowReference != null)
                await SafeReclaimOverflowAsync(overflowReference, ct);
            throw;
        }
    }

    public async ValueTask<bool> ReplaceAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        byte[]? existingStoredPayload = await _inner.FindAsync(key, ct);
        if (existingStoredPayload == null)
            return false;

        byte[]? newOverflowReference = null;
        try
        {
            ReadOnlyMemory<byte> storedPayload = payload;
            if (IndexOverflowPageStore.RequiresOverflow(payload.Length))
            {
                newOverflowReference = await IndexOverflowPageStore.WriteAsync(_pager, payload, ct);
                storedPayload = newOverflowReference;
            }

            bool replaced = await _inner.ReplaceAsync(key, storedPayload, ct);
            if (!replaced)
            {
                if (newOverflowReference != null)
                    await SafeReclaimOverflowAsync(newOverflowReference, ct);
                return false;
            }

            if (IndexOverflowReferenceCodec.IsEncoded(existingStoredPayload))
                await IndexOverflowPageStore.ReclaimAsync(_pager, existingStoredPayload, ct);

            return true;
        }
        catch
        {
            if (newOverflowReference != null)
                await SafeReclaimOverflowAsync(newOverflowReference, ct);
            throw;
        }
    }

    public async ValueTask<bool> DeleteAsync(long key, CancellationToken ct = default)
    {
        byte[]? existingStoredPayload = await _inner.FindAsync(key, ct);
        if (existingStoredPayload == null)
            return false;

        bool deleted = await _inner.DeleteAsync(key, ct);
        if (!deleted)
            return false;

        if (IndexOverflowReferenceCodec.IsEncoded(existingStoredPayload))
            await IndexOverflowPageStore.ReclaimAsync(_pager, existingStoredPayload, ct);

        return true;
    }

    public IIndexCursor CreateCursor(IndexScanRange range)
        => new OverflowingIndexCursor(_inner.CreateCursor(range), _pager);

    public async ValueTask ReclaimAsync(CancellationToken ct = default)
    {
        await using var cursor = _inner.CreateCursor(IndexScanRange.All);
        while (await cursor.MoveNextAsync(ct))
        {
            if (!IndexOverflowReferenceCodec.IsEncoded(cursor.CurrentValue.Span))
                continue;

            await IndexOverflowPageStore.ReclaimAsync(_pager, cursor.CurrentValue, ct);
        }

        if (_inner is IReclaimableIndexStore reclaimable)
            await reclaimable.ReclaimAsync(ct);
    }

    public bool TryFindCached(long key, out byte[]? payload)
    {
        payload = null;
        if (_inner is not ICacheAwareIndexStore cacheAware ||
            !cacheAware.TryFindCached(key, out byte[]? storedPayload))
        {
            return false;
        }

        if (storedPayload == null)
        {
            payload = null;
            return true;
        }

        if (IndexOverflowReferenceCodec.IsEncoded(storedPayload))
            return false;

        RecordPointRead(key);
        payload = storedPayload;
        return true;
    }

    private async ValueTask SafeReclaimOverflowAsync(ReadOnlyMemory<byte> overflowReference, CancellationToken ct)
    {
        try
        {
            await IndexOverflowPageStore.ReclaimAsync(_pager, overflowReference, ct);
        }
        catch
        {
            // Preserve the original storage failure.
        }
    }

    private sealed class OverflowingIndexCursor : IIndexCursor
    {
        private readonly IIndexCursor _inner;
        private readonly Pager _pager;

        public OverflowingIndexCursor(IIndexCursor inner, Pager pager)
        {
            _inner = inner;
            _pager = pager;
        }

        public long CurrentKey => _inner.CurrentKey;

        public ReadOnlyMemory<byte> CurrentValue { get; private set; }

        public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
        {
            if (!await _inner.MoveNextAsync(ct))
            {
                CurrentValue = ReadOnlyMemory<byte>.Empty;
                return false;
            }

            ReadOnlyMemory<byte> storedPayload = _inner.CurrentValue;
            if (!IndexOverflowReferenceCodec.IsEncoded(storedPayload.Span))
            {
                CurrentValue = storedPayload;
                return true;
            }

            CurrentValue = await IndexOverflowPageStore.ReadAsync(_pager, storedPayload, ct);
            return true;
        }

        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }
}
