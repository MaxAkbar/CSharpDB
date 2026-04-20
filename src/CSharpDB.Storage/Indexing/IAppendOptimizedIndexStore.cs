using CSharpDB.Primitives;

namespace CSharpDB.Storage.Indexing;

public sealed class AppendOptimizedIndexMutationContext
{
    private long[]? _pendingExternalRowIds;
    private int _pendingExternalRowCount;
    private bool _optimisticInsertEnabled;
    private bool _allowOptimisticInsert;
    private bool _hasLastOptimisticInsertKey;
    private long _lastOptimisticInsertKey;

    internal long Key { get; private set; }

    internal DbValue[]? KeyComponents { get; private set; }

    internal byte[]? StoredPayload { get; private set; }

    internal AppendableHashedIndexPayloadMetadata FlushedMetadata { get; private set; }

    internal AppendableHashedIndexPayloadMetadata Metadata { get; private set; }

    public bool AllowDeferredExternalAppends { get; set; }

    internal bool HasCapturedState => StoredPayload is not null;

    internal bool HasPendingExternalAppends => _pendingExternalRowCount > 0;

    internal ReadOnlyMemory<long> PendingExternalRowIds =>
        _pendingExternalRowIds is null || _pendingExternalRowCount == 0
            ? ReadOnlyMemory<long>.Empty
            : _pendingExternalRowIds.AsMemory(0, _pendingExternalRowCount);

    internal bool Matches(long key, ReadOnlySpan<DbValue> keyComponents, ReadOnlySpan<byte> storedPayload)
    {
        return Key == key &&
               StoredPayload is not null &&
               storedPayload.SequenceEqual(StoredPayload) &&
               ComponentsEqual(KeyComponents, keyComponents);
    }

    internal void Capture(
        long key,
        ReadOnlySpan<DbValue> keyComponents,
        byte[] storedPayload,
        AppendableHashedIndexPayloadMetadata metadata)
    {
        Key = key;
        if (KeyComponents is null || KeyComponents.Length != keyComponents.Length)
            KeyComponents = new DbValue[keyComponents.Length];

        for (int i = 0; i < keyComponents.Length; i++)
            KeyComponents[i] = keyComponents[i];

        StoredPayload = storedPayload;
        FlushedMetadata = metadata;
        Metadata = metadata;
        _pendingExternalRowCount = 0;
    }

    internal void StageDeferredExternalAppend(long rowId, AppendableHashedIndexPayloadMetadata visibleMetadata)
    {
        if (Metadata.Format != AppendableHashedIndexPayloadFormat.ExternalChainState)
        {
            throw new InvalidOperationException(
                "Deferred append staging is only supported for external appendable hashed payloads.");
        }

        if (_pendingExternalRowCount == 0)
            FlushedMetadata = Metadata;

        int required = _pendingExternalRowCount + 1;
        if (_pendingExternalRowIds is null)
        {
            _pendingExternalRowIds = new long[Math.Max(4, required)];
        }
        else if (required > _pendingExternalRowIds.Length)
        {
            int newCapacity = _pendingExternalRowIds.Length;
            while (newCapacity < required)
                newCapacity = checked(newCapacity * 2);

            Array.Resize(ref _pendingExternalRowIds, newCapacity);
        }

        _pendingExternalRowIds[_pendingExternalRowCount++] = rowId;
        Metadata = visibleMetadata;
    }

    internal void CompleteDeferredExternalFlush(AppendableHashedIndexPayloadMetadata flushedMetadata)
    {
        FlushedMetadata = flushedMetadata;
        Metadata = flushedMetadata;
        _pendingExternalRowCount = 0;
    }

    internal void ResetOptimisticInsertState(bool enabled)
    {
        _optimisticInsertEnabled = enabled;
        _allowOptimisticInsert = enabled;
        _hasLastOptimisticInsertKey = false;
        _lastOptimisticInsertKey = 0;
    }

    internal bool TryBeginOptimisticInsert(long key)
    {
        if (!_optimisticInsertEnabled || !_allowOptimisticInsert)
            return false;

        if (_hasLastOptimisticInsertKey && key <= _lastOptimisticInsertKey)
        {
            _allowOptimisticInsert = false;
            return false;
        }

        return true;
    }

    internal void RecordOptimisticInsertSuccess(long key)
    {
        _hasLastOptimisticInsertKey = true;
        _lastOptimisticInsertKey = key;
    }

    internal void RecordOptimisticInsertFallback(long key)
    {
        _hasLastOptimisticInsertKey = true;
        _lastOptimisticInsertKey = key;
        _allowOptimisticInsert = false;
    }

    internal void Clear()
    {
        Key = 0;
        StoredPayload = null;
        FlushedMetadata = default;
        Metadata = default;
        _pendingExternalRowCount = 0;
        if (KeyComponents is not null)
            Array.Clear(KeyComponents, 0, KeyComponents.Length);
    }

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
}

public enum AppendRowIdResult
{
    Missing = 0,
    Appended = 1,
    AlreadyExists = 2,
    NotApplicable = 3,
}

/// <summary>
/// Optional index-store capability for appending rowids into large duplicate buckets
/// without rewriting the full logical payload each time.
/// </summary>
public interface IAppendOptimizedIndexStore
{
    ValueTask<AppendRowIdResult> TryAppendHashedRowIdAsync(
        long key,
        DbValue[] keyComponents,
        long rowId,
        AppendOptimizedIndexMutationContext? context = null,
        CancellationToken ct = default);

    ValueTask FlushPendingHashedRowIdsAsync(
        AppendOptimizedIndexMutationContext? context,
        CancellationToken ct = default);
}
