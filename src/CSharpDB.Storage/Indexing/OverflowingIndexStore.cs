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
        AppendOptimizedIndexMutationContext? context = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(keyComponents);

        byte[]? storedPayload;
        if (_inner is ICacheAwareIndexStore cacheAware &&
            cacheAware.TryFindCached(key, out byte[]? cachedPayload))
        {
            storedPayload = cachedPayload;
        }
        else
        {
            storedPayload = await _inner.FindAsync(key, ct);
        }

        AppendOptimizedIndexMutationContext? matchedContext = null;
        if (context is { HasCapturedState: true } activeContext)
        {
            if (storedPayload is not null &&
                activeContext.Matches(key, keyComponents, storedPayload))
            {
                matchedContext = activeContext;
                _pager.RecordHashedIndexAppendContextHit();
            }
            else
            {
                _pager.RecordHashedIndexAppendContextMiss();
                if (activeContext.HasPendingExternalAppends)
                    await FlushPendingHashedRowIdsAsync(activeContext, ct);
            }
        }

        if (storedPayload == null)
        {
            context?.Clear();
            return AppendRowIdResult.Missing;
        }

        if (matchedContext is not null)
        {
            return await TryAppendKnownAppendablePayloadAsync(
                key,
                storedPayload,
                keyComponents,
                rowId,
                matchedContext.Metadata,
                matchedContext,
                ct);
        }

        if (AppendableHashedIndexPayloadCodec.IsEncoded(storedPayload))
        {
            if (!AppendableHashedIndexPayloadCodec.TryDecodeMetadata(
                    storedPayload,
                    out AppendableHashedIndexPayloadMetadata metadata) ||
                !AppendableHashedIndexPayloadCodec.EncodedKeyComponentsEqual(
                    storedPayload.AsSpan(metadata.KeyComponentsOffset),
                    keyComponents))
            {
                _pager.RecordHashedIndexAppendNotApplicable();
                return AppendRowIdResult.NotApplicable;
            }

            metadata = await PopulateAppendableMetadataAsync(metadata, ct);
            return await TryAppendKnownAppendablePayloadAsync(
                key,
                storedPayload,
                keyComponents,
                rowId,
                metadata,
                context,
                ct);
        }

        bool storedAsGenericOverflow = IndexOverflowReferenceCodec.IsEncoded(storedPayload);
        byte[] logicalPayload = storedAsGenericOverflow
            ? await IndexOverflowPageStore.ReadAsync(_pager, storedPayload, ct)
            : storedPayload;

        if (!HashedIndexPayloadCodec.TryDecodeSingleGroup(logicalPayload, out DbValue[]? decodedKeyComponents, out byte[]? rowIdPayload) ||
            !HashedIndexPayloadCodec.KeyComponentsEqualStored(decodedKeyComponents, keyComponents) ||
            rowIdPayload == null ||
            rowIdPayload.Length < AppendableHashedPromotionRowIdBytes)
        {
            _pager.RecordHashedIndexAppendNotApplicable();
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

        (uint firstPageId, AppendOnlyRowIdChainStore.AppendableChainMetadata appendableChainMetadata) =
            await AppendOnlyRowIdChainStore.WriteAppendableAsync(
                _pager,
                rowIdPayload,
                isSortedAscending,
                lastRowId,
                ct);
        try
        {
            AppendOnlyRowIdChainStore.AppendableChainMetadata updatedChainMetadata =
                await AppendOnlyRowIdChainStore.AppendAsync(
                    _pager,
                    firstPageId,
                    appendableChainMetadata,
                    rowId,
                    isSortedAscending,
                    ct);
            byte[] appendablePayload = AppendableHashedIndexPayloadCodec.EncodeExternal(
                decodedKeyComponents!,
                firstPageId);

            if (!await _inner.ReplaceAsync(key, appendablePayload, ct))
            {
                await AppendOnlyRowIdChainStore.ReclaimAsync(_pager, firstPageId, ct);
                return AppendRowIdResult.Missing;
            }

            if (storedAsGenericOverflow)
                await IndexOverflowPageStore.ReclaimAsync(_pager, storedPayload, ct);

            _pager.RecordHashedIndexAppendPromotion();

            if (context is not null &&
                AppendableHashedIndexPayloadCodec.TryDecodeMetadata(appendablePayload, out AppendableHashedIndexPayloadMetadata metadata))
            {
                context.Capture(
                    key,
                    keyComponents,
                    appendablePayload,
                    CombineAppendableMetadata(metadata, updatedChainMetadata));
            }

            return AppendRowIdResult.Appended;
        }
        catch
        {
            await SafeReclaimAppendableHashedPayloadAsync(firstPageId, ct);
            throw;
        }
    }

    public async ValueTask FlushPendingHashedRowIdsAsync(
        AppendOptimizedIndexMutationContext? context,
        CancellationToken ct = default)
    {
        if (context is not { HasPendingExternalAppends: true })
            return;

        AppendableHashedIndexPayloadMetadata flushedMetadata = context.FlushedMetadata;
        if (flushedMetadata.Format != AppendableHashedIndexPayloadFormat.ExternalChainState)
        {
            throw new InvalidOperationException(
                "Deferred append flushing is only supported for external appendable hashed payloads.");
        }

        AppendOnlyRowIdChainStore.AppendableChainMetadata updated =
            await AppendOnlyRowIdChainStore.AppendBatchAsync(
                _pager,
                flushedMetadata.FirstPageId,
                CreateAppendableChainMetadata(flushedMetadata),
                context.PendingExternalRowIds,
                context.Metadata.IsSortedAscending,
                ct);
        _pager.RecordHashedIndexDeferredFlush();
        context.CompleteDeferredExternalFlush(CombineAppendableMetadata(flushedMetadata, updated));
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
            if (!AppendableHashedIndexPayloadCodec.TryDecodeReference(
                    storedPayload,
                    out AppendableHashedIndexPayloadReference appendableReference))
            {
                throw new InvalidOperationException("Stored appendable hashed payload is invalid.");
            }

            AppendableHashedIndexPayloadMetadata metadata =
                await PopulateAppendableMetadataAsync(appendableReference.Metadata, ct);
            byte[] rowIdPayload = metadata.Format == AppendableHashedIndexPayloadFormat.ExternalChainState
                ? await AppendOnlyRowIdChainStore.ReadAsync(
                    _pager,
                    metadata.FirstPageId,
                    CreateAppendableChainMetadata(metadata),
                    ct)
                : await AppendOnlyRowIdChainStore.ReadAsync(_pager, metadata.FirstPageId, metadata.RowCount, ct);
            return HashedIndexPayloadCodec.CreateSingleGroup(appendableReference.KeyComponents, rowIdPayload);
        }

        if (IndexOverflowReferenceCodec.IsEncoded(storedPayload))
            return await IndexOverflowPageStore.ReadAsync(_pager, storedPayload, ct);

        return storedPayload;
    }

    private async ValueTask<AppendableHashedIndexPayloadMetadata> PopulateAppendableMetadataAsync(
        AppendableHashedIndexPayloadMetadata metadata,
        CancellationToken ct)
    {
        if (metadata.Format != AppendableHashedIndexPayloadFormat.ExternalChainState)
            return metadata;

        _pager.RecordHashedIndexAppendExternalMetadataRead();
        AppendOnlyRowIdChainStore.AppendableChainMetadata chainMetadata =
            await AppendOnlyRowIdChainStore.ReadAppendableMetadataAsync(_pager, metadata.FirstPageId, ct);
        return CombineAppendableMetadata(metadata, chainMetadata);
    }

    private async ValueTask<AppendRowIdResult> TryAppendKnownAppendablePayloadAsync(
        long key,
        byte[] storedPayload,
        DbValue[] keyComponents,
        long rowId,
        AppendableHashedIndexPayloadMetadata appendable,
        AppendOptimizedIndexMutationContext? context,
        CancellationToken ct)
    {
        switch (appendable.Format)
        {
            case AppendableHashedIndexPayloadFormat.InlineMutableState:
                return await TryAppendInlineAppendablePayloadAsync(
                    key,
                    storedPayload,
                    keyComponents,
                    rowId,
                    appendable,
                    context,
                    ct);

            case AppendableHashedIndexPayloadFormat.ExternalChainState:
                return await TryAppendExternalAppendablePayloadAsync(
                    key,
                    storedPayload,
                    keyComponents,
                    rowId,
                    appendable,
                    context,
                    ct);

            default:
                throw new InvalidOperationException($"Unknown appendable payload format '{appendable.Format}'.");
        }
    }

    private async ValueTask<AppendRowIdResult> TryAppendInlineAppendablePayloadAsync(
        long key,
        byte[] storedPayload,
        DbValue[] keyComponents,
        long rowId,
        AppendableHashedIndexPayloadMetadata appendable,
        AppendOptimizedIndexMutationContext? context,
        CancellationToken ct)
    {
        if (appendable.IsSortedAscending)
        {
            if (rowId == appendable.LastRowId)
            {
                context?.Capture(key, keyComponents, storedPayload, appendable);
                return AppendRowIdResult.AlreadyExists;
            }

            if (rowId > appendable.LastRowId)
            {
                uint newLastPageId = await AppendOnlyRowIdChainStore.AppendAsync(_pager, appendable.LastPageId, rowId, ct);
                byte[] updatedPayload = AppendableHashedIndexPayloadCodec.Encode(
                    storedPayload.AsSpan(appendable.KeyComponentsOffset),
                    appendable.FirstPageId,
                    newLastPageId,
                    appendable.RowCount + 1,
                    rowId,
                    isSortedAscending: true);
                if (!await _inner.ReplaceAsync(key, updatedPayload, ct))
                    throw new InvalidOperationException($"Failed to update appendable hashed payload for index key {key}.");

                context?.Capture(
                    key,
                    keyComponents,
                    updatedPayload,
                    appendable with
                    {
                        LastPageId = newLastPageId,
                        RowCount = appendable.RowCount + 1,
                        LastRowId = rowId,
                    });
                return AppendRowIdResult.Appended;
            }
        }

        if (await AppendOnlyRowIdChainStore.ContainsAsync(_pager, appendable.FirstPageId, appendable.RowCount, rowId, ct))
        {
            context?.Capture(key, keyComponents, storedPayload, appendable);
            return AppendRowIdResult.AlreadyExists;
        }

        uint appendedLastPageId = await AppendOnlyRowIdChainStore.AppendAsync(_pager, appendable.LastPageId, rowId, ct);
        byte[] appendedPayload = AppendableHashedIndexPayloadCodec.Encode(
            storedPayload.AsSpan(appendable.KeyComponentsOffset),
            appendable.FirstPageId,
            appendedLastPageId,
            appendable.RowCount + 1,
            rowId,
            isSortedAscending: false);
        if (!await _inner.ReplaceAsync(key, appendedPayload, ct))
            throw new InvalidOperationException($"Failed to update appendable hashed payload for index key {key}.");

        context?.Capture(
            key,
            keyComponents,
            appendedPayload,
            appendable with
            {
                LastPageId = appendedLastPageId,
                RowCount = appendable.RowCount + 1,
                LastRowId = rowId,
                IsSortedAscending = false,
            });
        return AppendRowIdResult.Appended;
    }

    private async ValueTask<AppendRowIdResult> TryAppendExternalAppendablePayloadAsync(
        long key,
        byte[] storedPayload,
        DbValue[] keyComponents,
        long rowId,
        AppendableHashedIndexPayloadMetadata appendable,
        AppendOptimizedIndexMutationContext? context,
        CancellationToken ct)
    {
        if (context is { HasPendingExternalAppends: true } pendingContext)
        {
            bool canContinueDeferredAppend =
                pendingContext.AllowDeferredExternalAppends &&
                appendable.IsSortedAscending &&
                rowId > appendable.LastRowId;
            if (!canContinueDeferredAppend)
            {
                await FlushPendingHashedRowIdsAsync(pendingContext, ct);
                appendable = pendingContext.Metadata;
            }
        }

        AppendOnlyRowIdChainStore.AppendableChainMetadata chainMetadata =
            CreateAppendableChainMetadata(appendable);
        if (appendable.IsSortedAscending)
        {
            if (rowId == appendable.LastRowId)
            {
                if (context is not { HasPendingExternalAppends: true })
                    context?.Capture(key, keyComponents, storedPayload, appendable);
                return AppendRowIdResult.AlreadyExists;
            }

            if (rowId > appendable.LastRowId)
            {
                if (context?.AllowDeferredExternalAppends == true)
                {
                    if (!context.Matches(key, keyComponents, storedPayload) ||
                        context.Metadata.Format != AppendableHashedIndexPayloadFormat.ExternalChainState)
                    {
                        context.Capture(key, keyComponents, storedPayload, appendable);
                    }

                    _pager.RecordHashedIndexDeferredAppend();
                    context.StageDeferredExternalAppend(
                        rowId,
                        appendable with
                        {
                            RowCount = appendable.RowCount + 1,
                            LastRowId = rowId,
                            IsSortedAscending = true,
                        });
                    return AppendRowIdResult.Appended;
                }

                AppendOnlyRowIdChainStore.AppendableChainMetadata updated =
                    await AppendOnlyRowIdChainStore.AppendAsync(
                        _pager,
                        appendable.FirstPageId,
                        chainMetadata,
                        rowId,
                        isSortedAscending: true,
                        ct);
                context?.Capture(key, keyComponents, storedPayload, CombineAppendableMetadata(appendable, updated));
                return AppendRowIdResult.Appended;
            }
        }

        if (appendable.ChainEncoding == AppendableChainEncoding.DeltaVarint)
        {
            _pager.RecordHashedIndexAppendNotApplicable();
            return AppendRowIdResult.NotApplicable;
        }

        if (await AppendOnlyRowIdChainStore.ContainsAsync(_pager, appendable.FirstPageId, chainMetadata, rowId, ct))
        {
            context?.Capture(key, keyComponents, storedPayload, appendable);
            return AppendRowIdResult.AlreadyExists;
        }

        AppendOnlyRowIdChainStore.AppendableChainMetadata appended =
            await AppendOnlyRowIdChainStore.AppendAsync(
                _pager,
                appendable.FirstPageId,
                chainMetadata,
                rowId,
                isSortedAscending: false,
                ct);
        context?.Capture(key, keyComponents, storedPayload, CombineAppendableMetadata(appendable, appended));
        return AppendRowIdResult.Appended;
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

        if (!AppendableHashedIndexPayloadCodec.TryDecodeMetadata(
                storedPayload.Span,
                out AppendableHashedIndexPayloadMetadata appendable))
        {
            throw new InvalidOperationException("Stored appendable hashed payload is invalid.");
        }

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

    private static AppendOnlyRowIdChainStore.AppendableChainMetadata CreateAppendableChainMetadata(
        AppendableHashedIndexPayloadMetadata metadata)
    {
        return new AppendOnlyRowIdChainStore.AppendableChainMetadata(
            metadata.LastPageId,
            metadata.RowCount,
            metadata.LastRowId,
            metadata.IsSortedAscending,
            metadata.ChainEncoding);
    }

    private static AppendableHashedIndexPayloadMetadata CombineAppendableMetadata(
        AppendableHashedIndexPayloadMetadata metadata,
        AppendOnlyRowIdChainStore.AppendableChainMetadata chainMetadata)
    {
        return metadata with
        {
            LastPageId = chainMetadata.LastPageId,
            RowCount = chainMetadata.RowCount,
            LastRowId = chainMetadata.LastRowId,
            IsSortedAscending = chainMetadata.IsSortedAscending,
            ChainEncoding = chainMetadata.Encoding,
        };
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
                if (!AppendableHashedIndexPayloadCodec.TryDecodeReference(
                        storedPayload.Span,
                        out AppendableHashedIndexPayloadReference appendableReference))
                {
                    throw new InvalidOperationException("Stored appendable hashed payload is invalid.");
                }

                AppendableHashedIndexPayloadMetadata metadata =
                    appendableReference.Metadata.Format == AppendableHashedIndexPayloadFormat.ExternalChainState
                        ? CombineAppendableMetadata(
                            appendableReference.Metadata,
                            await AppendOnlyRowIdChainStore.ReadAppendableMetadataAsync(_pager, appendableReference.Metadata.FirstPageId, ct))
                        : appendableReference.Metadata;
                byte[] rowIdPayload = metadata.Format == AppendableHashedIndexPayloadFormat.ExternalChainState
                    ? await AppendOnlyRowIdChainStore.ReadAsync(
                        _pager,
                        metadata.FirstPageId,
                        CreateAppendableChainMetadata(metadata),
                        ct)
                    : await AppendOnlyRowIdChainStore.ReadAsync(_pager, metadata.FirstPageId, metadata.RowCount, ct);
                CurrentValue = HashedIndexPayloadCodec.CreateSingleGroup(appendableReference.KeyComponents, rowIdPayload);
                return true;
            }

            CurrentValue = await IndexOverflowPageStore.ReadAsync(_pager, storedPayload, ct);
            return true;
        }

        private static AppendOnlyRowIdChainStore.AppendableChainMetadata CreateAppendableChainMetadata(
            AppendableHashedIndexPayloadMetadata metadata)
        {
            return new AppendOnlyRowIdChainStore.AppendableChainMetadata(
                metadata.LastPageId,
                metadata.RowCount,
                metadata.LastRowId,
                metadata.IsSortedAscending,
                metadata.ChainEncoding);
        }

        private static AppendableHashedIndexPayloadMetadata CombineAppendableMetadata(
            AppendableHashedIndexPayloadMetadata metadata,
            AppendOnlyRowIdChainStore.AppendableChainMetadata chainMetadata)
        {
            return metadata with
            {
                LastPageId = chainMetadata.LastPageId,
                RowCount = chainMetadata.RowCount,
                LastRowId = chainMetadata.LastRowId,
                IsSortedAscending = chainMetadata.IsSortedAscending,
                ChainEncoding = chainMetadata.Encoding,
            };
        }

        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }
}
