using CSharpDB.Primitives;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Storage.Indexing;

/// <summary>
/// Transparently spills oversized index payloads into overflow-page chains so
/// duplicate-heavy secondary-index buckets do not need to fit inside one B-tree leaf cell.
/// </summary>
public sealed class OverflowingIndexStore : IIndexStore, ICacheAwareIndexStore, IReclaimableIndexStore, IAppendOptimizedIndexStore
{
    private const int AppendableHashedPromotionRowIdBytes = 1024;

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
        return storedPayload == null
            ? null
            : await DecodeStoredPayloadAsync(storedPayload, ct);
    }

    public ValueTask<long?> FindMaxKeyAsync(IndexScanRange range, CancellationToken ct = default)
        => _inner.FindMaxKeyAsync(range, ct);

    public async ValueTask InsertAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        byte[]? overflowReference = null;
        try
        {
            ReadOnlyMemory<byte> storedPayload = payload;
            if (ShouldStoreInGenericOverflow(payload.Span))
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
            if (ShouldStoreInGenericOverflow(payload.Span))
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

            await ReclaimStoredPayloadAsync(existingStoredPayload, ct);

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

        await ReclaimStoredPayloadAsync(existingStoredPayload, ct);

        return true;
    }

    public IIndexCursor CreateCursor(IndexScanRange range)
        => new OverflowingIndexCursor(_inner.CreateCursor(range), _pager);

    public async ValueTask<AppendRowIdResult> TryAppendHashedRowIdAsync(
        long key,
        DbValue[] keyComponents,
        long rowId,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(keyComponents);

        byte[]? storedPayload = await _inner.FindAsync(key, ct);
        if (storedPayload == null)
            return AppendRowIdResult.Missing;

        if (AppendableHashedIndexPayloadCodec.IsEncoded(storedPayload))
        {
            if (!AppendableHashedIndexPayloadCodec.TryDecode(storedPayload, out AppendableHashedIndexPayload appendable) ||
                !ComponentsEqual(appendable.KeyComponents, keyComponents))
            {
                return AppendRowIdResult.NotApplicable;
            }

            if (appendable.IsSortedAscending)
            {
                if (rowId == appendable.LastRowId)
                    return AppendRowIdResult.AlreadyExists;

                if (rowId > appendable.LastRowId)
                {
                    uint newLastPageId = await AppendOnlyRowIdChainStore.AppendAsync(_pager, appendable.LastPageId, rowId, ct);
                    byte[] updatedPayload = AppendableHashedIndexPayloadCodec.Encode(
                        appendable.KeyComponents,
                        appendable.FirstPageId,
                        newLastPageId,
                        appendable.RowCount + 1,
                        rowId,
                        isSortedAscending: true);
                    if (!await _inner.ReplaceAsync(key, updatedPayload, ct))
                        throw new InvalidOperationException($"Failed to update appendable hashed payload for index key {key}.");

                    return AppendRowIdResult.Appended;
                }
            }

            if (await AppendOnlyRowIdChainStore.ContainsAsync(_pager, appendable.FirstPageId, appendable.RowCount, rowId, ct))
                return AppendRowIdResult.AlreadyExists;

            uint appendedLastPageId = await AppendOnlyRowIdChainStore.AppendAsync(_pager, appendable.LastPageId, rowId, ct);
            byte[] appendedPayload = AppendableHashedIndexPayloadCodec.Encode(
                appendable.KeyComponents,
                appendable.FirstPageId,
                appendedLastPageId,
                appendable.RowCount + 1,
                rowId,
                isSortedAscending: false);
            if (!await _inner.ReplaceAsync(key, appendedPayload, ct))
                throw new InvalidOperationException($"Failed to update appendable hashed payload for index key {key}.");

            return AppendRowIdResult.Appended;
        }

        bool storedAsGenericOverflow = IndexOverflowReferenceCodec.IsEncoded(storedPayload);
        byte[] logicalPayload = storedAsGenericOverflow
            ? await IndexOverflowPageStore.ReadAsync(_pager, storedPayload, ct)
            : storedPayload;

        if (!HashedIndexPayloadCodec.TryDecodeSingleGroup(logicalPayload, out DbValue[]? decodedKeyComponents, out byte[]? rowIdPayload) ||
            !ComponentsEqual(decodedKeyComponents, keyComponents) ||
            rowIdPayload == null ||
            rowIdPayload.Length < AppendableHashedPromotionRowIdBytes)
        {
            return AppendRowIdResult.NotApplicable;
        }

        int rowCount = RowIdPayloadCodec.GetCount(rowIdPayload);
        bool isSortedAscending = RowIdPayloadCodec.IsSortedAscending(rowIdPayload);
        long lastRowId = RowIdPayloadCodec.ReadAt(rowIdPayload, rowCount - 1);

        if (isSortedAscending)
        {
            if (rowId == lastRowId)
                return AppendRowIdResult.AlreadyExists;
        }
        else
        {
            for (int i = 0; i < rowCount; i++)
            {
                if (RowIdPayloadCodec.ReadAt(rowIdPayload, i) == rowId)
                    return AppendRowIdResult.AlreadyExists;
            }
        }

        if (isSortedAscending && rowId < lastRowId)
        {
            for (int i = 0; i < rowCount; i++)
            {
                if (RowIdPayloadCodec.ReadAt(rowIdPayload, i) == rowId)
                    return AppendRowIdResult.AlreadyExists;
            }

            isSortedAscending = false;
        }

        (uint firstPageId, uint lastPageId) = await AppendOnlyRowIdChainStore.WriteAsync(_pager, rowIdPayload, ct);
        try
        {
            uint appendedLastPageId = await AppendOnlyRowIdChainStore.AppendAsync(_pager, lastPageId, rowId, ct);
            byte[] appendablePayload = AppendableHashedIndexPayloadCodec.Encode(
                decodedKeyComponents!,
                firstPageId,
                appendedLastPageId,
                rowCount + 1,
                rowId,
                isSortedAscending);

            if (!await _inner.ReplaceAsync(key, appendablePayload, ct))
            {
                await AppendOnlyRowIdChainStore.ReclaimAsync(_pager, firstPageId, ct);
                return AppendRowIdResult.Missing;
            }

            if (storedAsGenericOverflow)
                await IndexOverflowPageStore.ReclaimAsync(_pager, storedPayload, ct);

            return AppendRowIdResult.Appended;
        }
        catch
        {
            await SafeReclaimAppendableHashedPayloadAsync(firstPageId, ct);
            throw;
        }
    }

    public async ValueTask ReclaimAsync(CancellationToken ct = default)
    {
        await using var cursor = _inner.CreateCursor(IndexScanRange.All);
        while (await cursor.MoveNextAsync(ct))
        {
            if (IndexOverflowReferenceCodec.IsEncoded(cursor.CurrentValue.Span))
            {
                await IndexOverflowPageStore.ReclaimAsync(_pager, cursor.CurrentValue, ct);
                continue;
            }

            if (AppendableHashedIndexPayloadCodec.IsEncoded(cursor.CurrentValue.Span))
                await ReclaimStoredPayloadAsync(cursor.CurrentValue, ct);
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

        if (IndexOverflowReferenceCodec.IsEncoded(storedPayload) ||
            AppendableHashedIndexPayloadCodec.IsEncoded(storedPayload))
        {
            return false;
        }

        RecordPointRead(key);
        payload = storedPayload;
        return true;
    }

    private async ValueTask<byte[]> DecodeStoredPayloadAsync(byte[] storedPayload, CancellationToken ct)
    {
        if (AppendableHashedIndexPayloadCodec.IsEncoded(storedPayload))
        {
            if (!AppendableHashedIndexPayloadCodec.TryDecode(storedPayload, out AppendableHashedIndexPayload appendable))
                throw new InvalidOperationException("Stored appendable hashed payload is invalid.");

            byte[] rowIdPayload = await AppendOnlyRowIdChainStore.ReadAsync(_pager, appendable.FirstPageId, appendable.RowCount, ct);
            return HashedIndexPayloadCodec.CreateSingleGroup(appendable.KeyComponents, rowIdPayload);
        }

        if (IndexOverflowReferenceCodec.IsEncoded(storedPayload))
            return await IndexOverflowPageStore.ReadAsync(_pager, storedPayload, ct);

        return storedPayload;
    }

    private async ValueTask ReclaimStoredPayloadAsync(ReadOnlyMemory<byte> storedPayload, CancellationToken ct)
    {
        if (IndexOverflowReferenceCodec.IsEncoded(storedPayload.Span))
        {
            await IndexOverflowPageStore.ReclaimAsync(_pager, storedPayload, ct);
            return;
        }

        if (!AppendableHashedIndexPayloadCodec.IsEncoded(storedPayload.Span))
            return;

        if (!AppendableHashedIndexPayloadCodec.TryDecode(storedPayload.Span, out AppendableHashedIndexPayload appendable))
            throw new InvalidOperationException("Stored appendable hashed payload is invalid.");

        await AppendOnlyRowIdChainStore.ReclaimAsync(_pager, appendable.FirstPageId, ct);
    }

    private static bool ShouldStoreInGenericOverflow(ReadOnlySpan<byte> payload)
        => !AppendableHashedIndexPayloadCodec.IsEncoded(payload) &&
           IndexOverflowPageStore.RequiresOverflow(payload.Length);

    private static bool ComponentsEqual(ReadOnlySpan<DbValue> left, ReadOnlySpan<DbValue> right)
    {
        if (left.Length != right.Length)
            return false;

        for (int i = 0; i < left.Length; i++)
        {
            if (DbValue.Compare(left[i], right[i]) != 0)
                return false;
        }

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

    private async ValueTask SafeReclaimAppendableHashedPayloadAsync(uint firstPageId, CancellationToken ct)
    {
        try
        {
            await AppendOnlyRowIdChainStore.ReclaimAsync(_pager, firstPageId, ct);
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
            if (!IndexOverflowReferenceCodec.IsEncoded(storedPayload.Span) &&
                !AppendableHashedIndexPayloadCodec.IsEncoded(storedPayload.Span))
            {
                CurrentValue = storedPayload;
                return true;
            }

            if (AppendableHashedIndexPayloadCodec.IsEncoded(storedPayload.Span))
            {
                if (!AppendableHashedIndexPayloadCodec.TryDecode(storedPayload.Span, out AppendableHashedIndexPayload appendable))
                    throw new InvalidOperationException("Stored appendable hashed payload is invalid.");

                byte[] rowIdPayload = await AppendOnlyRowIdChainStore.ReadAsync(_pager, appendable.FirstPageId, appendable.RowCount, ct);
                CurrentValue = HashedIndexPayloadCodec.CreateSingleGroup(appendable.KeyComponents, rowIdPayload);
                return true;
            }

            CurrentValue = await IndexOverflowPageStore.ReadAsync(_pager, storedPayload, ct);
            return true;
        }

        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }
}
