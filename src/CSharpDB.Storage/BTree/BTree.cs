using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using CSharpDB.Core;

namespace CSharpDB.Storage.BTrees;

/// <summary>
/// B+tree keyed by long rowid. Leaf pages store (key, payload). Interior pages store routing keys and child pointers.
///
/// Leaf cell format:   [totalSize:varint] [key:8 bytes] [payload bytes...]
/// Interior cell format: [totalSize:varint] [leftChild:4 bytes] [key:8 bytes]
///
/// Interior pages also have a "rightmost child" stored in the page header.
/// Leaf pages have a "next leaf" pointer for sequential scans.
/// </summary>
public sealed class BTree
{
    private const int InteriorCellHeaderBytes = 1; // varint(12)
    private const int InteriorCellLeftChildOffset = InteriorCellHeaderBytes;
    private const int InteriorCellKeyOffset = InteriorCellHeaderBytes + 4;
    private const int InteriorCellSize = 13;
    private const int MaxStackLeafCellBytes = 256;
    private const int MinLeafCells = 1;
    private const int MinInteriorCells = 1;

    private readonly Pager _pager;
    private uint _rootPageId;
    private long? _cachedEntryCount;

    // Leaf page hint cache: skip interior traversal when the target key falls within the cached leaf's key range
    private uint _hintLeafPageId;
    private long _hintLeafMinKey;
    private long _hintLeafMaxKey;
    private bool _hintValid;

    public uint RootPageId => _rootPageId;

    public BTree(Pager pager, uint rootPageId)
    {
        _pager = pager;
        _rootPageId = rootPageId;
    }

    /// <summary>
    /// Create a new empty B+tree and return its root page ID.
    /// </summary>
    public static async ValueTask<uint> CreateNewAsync(Pager pager, CancellationToken ct = default)
    {
        uint pageId = await pager.AllocatePageAsync(ct);
        var page = await pager.GetPageAsync(pageId, ct);
        var sp = new SlottedPage(page, pageId);
        sp.Initialize(PageConstants.PageTypeLeaf);
        await pager.MarkDirtyAsync(pageId, ct);
        return pageId;
    }

    /// <summary>
    /// Look up a single key. Returns the payload or null if not found.
    /// </summary>
    public async ValueTask<byte[]?> FindAsync(long key, CancellationToken ct = default)
    {
        var payload = await FindMemoryAsync(key, ct);
        return payload.HasValue ? payload.Value.ToArray() : null;
    }

    /// <summary>
    /// Look up a single key. Returns a view over the page-backed payload or null if not found.
    /// Callers should consume the returned memory immediately and not retain it across writes.
    /// </summary>
    public async ValueTask<ReadOnlyMemory<byte>?> FindMemoryAsync(long key, CancellationToken ct = default)
    {
        // Try leaf hint cache: if the key falls within the cached leaf's range, go directly to that leaf
        if (_hintValid && key >= _hintLeafMinKey && key <= _hintLeafMaxKey)
        {
            var hintPage = await _pager.GetPageAsync(_hintLeafPageId, ct);
            var hintSp = new SlottedPage(hintPage, _hintLeafPageId);
            if (hintSp.PageType == PageConstants.PageTypeLeaf && hintSp.CellCount > 0)
            {
                // Validate the hint is still accurate (page may have been split by a concurrent write)
                long actualMin = ReadLeafKey(hintSp, 0);
                long actualMax = ReadLeafKey(hintSp, hintSp.CellCount - 1);
                if (key >= actualMin && key <= actualMax)
                {
                    int idx = FindKeyInLeaf(hintSp, key);
                    if (idx < 0) return null;
                    return ReadLeafPayloadMemory(hintSp, idx);
                }
            }
            // Hint is stale, clear it and fall through to normal traversal
            _hintValid = false;
        }

        uint pageId = _rootPageId;

        while (true)
        {
            var page = await _pager.GetPageAsync(pageId, ct);
            var sp = new SlottedPage(page, pageId);

            if (sp.PageType == PageConstants.PageTypeLeaf)
            {
                // Populate hint cache for next lookup
                if (sp.CellCount > 0)
                {
                    _hintLeafPageId = pageId;
                    _hintLeafMinKey = ReadLeafKey(sp, 0);
                    _hintLeafMaxKey = ReadLeafKey(sp, sp.CellCount - 1);
                    _hintValid = true;
                }

                int idx = FindKeyInLeaf(sp, key);
                if (idx < 0) return null;
                return ReadLeafPayloadMemory(sp, idx);
            }
            else
            {
                pageId = FindChildPage(sp, key);
            }
        }
    }

    /// <summary>
    /// Synchronous cache-only lookup. Returns true if all pages were in the Pager cache
    /// (payload is the definitive result — null means key not found, non-null is the record bytes).
    /// Returns false if any page was a cache miss (caller should fall back to async FindAsync).
    /// </summary>
    public bool TryFindCached(long key, out byte[]? payload)
    {
        if (TryFindCachedMemory(key, out var cachedPayload))
        {
            payload = cachedPayload.HasValue ? cachedPayload.Value.ToArray() : null;
            return true;
        }

        payload = null;
        return false;
    }

    /// <summary>
    /// Synchronous cache-only lookup that returns a page-backed payload view on cache hits.
    /// Callers should consume the returned memory immediately and not retain it across writes.
    /// </summary>
    public bool TryFindCachedMemory(long key, out ReadOnlyMemory<byte>? payload)
    {
        payload = null;

        // Try leaf hint cache first
        if (_hintValid && key >= _hintLeafMinKey && key <= _hintLeafMaxKey)
        {
            var hintPage = _pager.TryGetCachedPage(_hintLeafPageId);
            if (hintPage == null) return false; // cache miss → fallback

            var hintSp = new SlottedPage(hintPage, _hintLeafPageId);
            if (hintSp.PageType == PageConstants.PageTypeLeaf && hintSp.CellCount > 0)
            {
                long actualMin = ReadLeafKey(hintSp, 0);
                long actualMax = ReadLeafKey(hintSp, hintSp.CellCount - 1);
                if (key >= actualMin && key <= actualMax)
                {
                    int idx = FindKeyInLeaf(hintSp, key);
                    payload = idx >= 0 ? ReadLeafPayloadMemory(hintSp, idx) : (ReadOnlyMemory<byte>?)null;
                    return true; // cache hit, definitive answer
                }
            }
            _hintValid = false;
        }

        // Full traversal using only cached pages
        uint pageId = _rootPageId;
        while (true)
        {
            var page = _pager.TryGetCachedPage(pageId);
            if (page == null) return false; // cache miss → fallback

            var sp = new SlottedPage(page, pageId);

            if (sp.PageType == PageConstants.PageTypeLeaf)
            {
                // Populate hint cache for next lookup
                if (sp.CellCount > 0)
                {
                    _hintLeafPageId = pageId;
                    _hintLeafMinKey = ReadLeafKey(sp, 0);
                    _hintLeafMaxKey = ReadLeafKey(sp, sp.CellCount - 1);
                    _hintValid = true;
                }

                int idx = FindKeyInLeaf(sp, key);
                payload = idx >= 0 ? ReadLeafPayloadMemory(sp, idx) : (ReadOnlyMemory<byte>?)null;
                return true; // cache hit, definitive answer
            }
            else
            {
                pageId = FindChildPage(sp, key);
            }
        }
    }

    /// <summary>
    /// Insert a key/value pair. If the key already exists, it's an error.
    /// </summary>
    public async ValueTask InsertAsync(long key, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        var result = await InsertRecursiveAsync(_rootPageId, key, payload, ct);

        if (result.Split)
        {
            // Root was split — create a new root
            uint newRootId = await _pager.AllocatePageAsync(ct);
            var newRoot = await _pager.GetPageAsync(newRootId, ct);
            var sp = new SlottedPage(newRoot, newRootId);
            sp.Initialize(PageConstants.PageTypeInterior);
            sp.RightChildOrNextLeaf = result.NewPageId;

            // Insert single interior cell pointing to old root.
            // Interior cells are fixed-size (13 bytes), so avoid heap allocation here.
            Span<byte> rootCell = stackalloc byte[13];
            WriteInteriorCell(rootCell, _rootPageId, result.SplitKey);
            sp.InsertCell(0, rootCell);
            await _pager.MarkDirtyAsync(newRootId, ct);

            _rootPageId = newRootId;
        }

        _cachedEntryCount = null;
        _hintValid = false;
    }

    /// <summary>
    /// Delete a key. Returns true if the key was found and deleted.
    /// </summary>
    public async ValueTask<bool> DeleteAsync(long key, CancellationToken ct = default)
    {
        var result = await DeleteRecursiveAsync(_rootPageId, key, isRoot: true, ct);
        bool deleted = result.Deleted;
        if (deleted)
        {
            await CollapseRootIfNeededAsync(ct);
            _cachedEntryCount = null;
            _hintValid = false;
        }
        return deleted;
    }

    /// <summary>
    /// Create a cursor positioned before the first entry.
    /// </summary>
    public BTreeCursor CreateCursor()
    {
        return new BTreeCursor(this, _pager);
    }

    /// <summary>
    /// Count entries by walking leaf pages and summing cell counts.
    /// </summary>
    public async ValueTask<long> CountEntriesAsync(CancellationToken ct = default)
    {
        if (_cachedEntryCount.HasValue)
            return _cachedEntryCount.Value;

        long count = 0;
        uint leafPageId = await FindLeftmostLeafAsync(ct);

        while (leafPageId != PageConstants.NullPageId)
        {
            var page = await _pager.GetPageAsync(leafPageId, ct);
            var sp = new SlottedPage(page, leafPageId);
            count += sp.CellCount;
            leafPageId = sp.RightChildOrNextLeaf;
        }

        _cachedEntryCount = count;
        return count;
    }

    /// <summary>
    /// Returns a cached entry count when available without touching storage.
    /// </summary>
    public bool TryGetCachedEntryCount(out long count)
    {
        if (_cachedEntryCount.HasValue)
        {
            count = _cachedEntryCount.Value;
            return true;
        }

        count = 0;
        return false;
    }

    #region Internal Insert

    private struct InsertResult
    {
        public bool Split;
        public long SplitKey;
        public uint NewPageId; // the new right sibling
    }

    private async ValueTask<InsertResult> InsertRecursiveAsync(uint pageId, long key, ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        var page = await _pager.GetPageAsync(pageId, ct);
        var sp = new SlottedPage(page, pageId);

        if (sp.PageType == PageConstants.PageTypeLeaf)
        {
            return await InsertIntoLeafAsync(pageId, page, sp, key, payload, ct);
        }
        else
        {
            // Find child to descend into
            uint childPageId = FindChildPageWithIndex(sp, key, out int childIdx);

            var childResult = await InsertRecursiveAsync(childPageId, key, payload, ct);
            if (!childResult.Split)
                return childResult;

            // Child was split — insert the new separator key into this interior page
            return await InsertIntoInteriorAsync(pageId, page, sp, childResult.SplitKey, childResult.NewPageId, childIdx, ct);
        }
    }

    private async ValueTask<InsertResult> InsertIntoLeafAsync(uint pageId, byte[] page, SlottedPage sp, long key, ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        // Find insertion point (sorted order)
        int insertIdx = FindInsertPosition(sp, key);

        // Check for duplicate
        if (insertIdx < sp.CellCount)
        {
            long existingKey = ReadLeafKey(sp, insertIdx);
            if (existingKey == key)
                throw new CSharpDbException(ErrorCode.DuplicateKey, $"Duplicate key: {key}");
        }

        int leafCellLength = GetLeafCellLength(payload.Length);
        if (leafCellLength <= MaxStackLeafCellBytes)
        {
            Span<byte> stackCell = stackalloc byte[leafCellLength];
            WriteLeafCell(stackCell, key, payload.Span);

            if (sp.InsertCell(insertIdx, stackCell))
            {
                await _pager.MarkDirtyAsync(pageId, ct);
                return new InsertResult { Split = false };
            }

            // Preserve the stack-built cell for split handling.
            byte[] splitCell = GC.AllocateUninitializedArray<byte>(leafCellLength);
            stackCell.CopyTo(splitCell);
            return await SplitLeafAsync(pageId, page, sp, insertIdx, splitCell, splitCell.Length, ct);
        }

        // For larger payloads, rent a temporary buffer instead of allocating per insert.
        byte[] pooledCell = ArrayPool<byte>.Shared.Rent(leafCellLength);
        try
        {
            var pooledCellSpan = pooledCell.AsSpan(0, leafCellLength);
            WriteLeafCell(pooledCellSpan, key, payload.Span);

            if (sp.InsertCell(insertIdx, pooledCellSpan))
            {
                await _pager.MarkDirtyAsync(pageId, ct);
                return new InsertResult { Split = false };
            }

            // Page is full — split
            return await SplitLeafAsync(pageId, page, sp, insertIdx, pooledCell, leafCellLength, ct);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(pooledCell, clearArray: false);
        }
    }

    private async ValueTask<InsertResult> SplitLeafAsync(
        uint pageId,
        byte[] page,
        SlottedPage sp,
        int insertIdx,
        byte[] newCell,
        int newCellLength,
        CancellationToken ct)
    {
        int existingCellCount = sp.CellCount;
        int totalCellCount = existingCellCount + 1;
        int[] cellOffsets = ArrayPool<int>.Shared.Rent(totalCellCount + 1);
        byte[]? splitCellBuffer = null;
        try
        {
            splitCellBuffer = BuildSplitCellBuffer(
                page,
                sp,
                insertIdx,
                newCell.AsSpan(0, newCellLength),
                cellOffsets,
                out int totalCellBytes);
            cellOffsets[totalCellCount] = totalCellBytes;
            int mid = totalCellCount / 2;

            // Allocate new right sibling
            uint newPageId = await _pager.AllocatePageAsync(ct);
            var newPage = await _pager.GetPageAsync(newPageId, ct);
            var newSp = new SlottedPage(newPage, newPageId);
            newSp.Initialize(PageConstants.PageTypeLeaf);

            // Link leaves
            newSp.RightChildOrNextLeaf = sp.RightChildOrNextLeaf;
            sp.Initialize(PageConstants.PageTypeLeaf);
            sp.RightChildOrNextLeaf = newPageId;

            // Distribute cells from split buffer
            for (int i = 0; i < mid; i++)
            {
                int offset = cellOffsets[i];
                int length = cellOffsets[i + 1] - offset;
                sp.InsertCell(i, splitCellBuffer.AsSpan(offset, length));
            }

            for (int i = mid; i < totalCellCount; i++)
            {
                int offset = cellOffsets[i];
                int length = cellOffsets[i + 1] - offset;
                newSp.InsertCell(i - mid, splitCellBuffer.AsSpan(offset, length));
            }

            await _pager.MarkDirtyAsync(pageId, ct);
            await _pager.MarkDirtyAsync(newPageId, ct);

            // The split key is the first key of the right page.
            int splitOffset = cellOffsets[mid];
            int splitLength = cellOffsets[mid + 1] - splitOffset;
            long splitKey = ReadLeafCellKey(splitCellBuffer.AsSpan(splitOffset, splitLength));
            return new InsertResult { Split = true, SplitKey = splitKey, NewPageId = newPageId };
        }
        finally
        {
            ArrayPool<int>.Shared.Return(cellOffsets, clearArray: false);
            if (splitCellBuffer != null)
                ArrayPool<byte>.Shared.Return(splitCellBuffer, clearArray: false);
        }
    }

    private async ValueTask<InsertResult> InsertIntoInteriorAsync(uint pageId, byte[] page, SlottedPage sp, long key, uint newChildPageId, int afterChildIdx, CancellationToken ct)
    {
        // Get the original child pointer that was at afterChildIdx
        uint originalChild = FindChildAtIndex(sp, afterChildIdx);

        // The new cell's left child is the original (left half stays).
        // Interior cells are fixed-size (13 bytes), so use stackalloc on the no-split fast path.
        Span<byte> cell = stackalloc byte[13];
        WriteInteriorCell(cell, originalChild, key);

        // Update the pointer at afterChildIdx to point to the new right sibling
        if (afterChildIdx >= sp.CellCount)
        {
            sp.RightChildOrNextLeaf = newChildPageId;
        }
        else
        {
            int offset = sp.GetCellOffset(afterChildIdx);
            BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(offset + InteriorCellLeftChildOffset), newChildPageId);
        }

        if (sp.InsertCell(afterChildIdx, cell))
        {
            await _pager.MarkDirtyAsync(pageId, ct);
            return new InsertResult { Split = false };
        }

        // Interior page full — split it
        return await SplitInteriorAsync(pageId, page, sp, afterChildIdx, originalChild, key, ct);
    }

    private async ValueTask<InsertResult> SplitInteriorAsync(
        uint pageId,
        byte[] page,
        SlottedPage sp,
        int insertIdx,
        uint insertedLeftChild,
        long insertedKey,
        CancellationToken ct)
    {
        int existingCellCount = sp.CellCount;
        int totalCellCount = existingCellCount + 1;
        const int interiorCellSize = 13;
        int totalBytes = checked(totalCellCount * interiorCellSize);
        byte[]? splitCellBuffer = null;
        try
        {
            splitCellBuffer = ArrayPool<byte>.Shared.Rent(totalBytes);
            int writeOffset = 0;
            for (int i = 0; i < totalCellCount; i++)
            {
                if (i == insertIdx)
                {
                    WriteInteriorCell(splitCellBuffer.AsSpan(writeOffset, interiorCellSize), insertedLeftChild, insertedKey);
                }
                else
                {
                    int sourceIndex = i < insertIdx ? i : i - 1;
                    ushort sourceOffset = sp.GetCellOffset(sourceIndex);
                    page.AsSpan(sourceOffset, interiorCellSize).CopyTo(splitCellBuffer.AsSpan(writeOffset, interiorCellSize));
                }

                writeOffset += interiorCellSize;
            }

            int mid = totalCellCount / 2;
            ReadOnlySpan<byte> promotedCell = splitCellBuffer.AsSpan(mid * interiorCellSize, interiorCellSize);
            long promotedKey = ReadInteriorCellKey(promotedCell);
            uint rightChildOfLeft = ReadInteriorCellLeftChild(promotedCell);

            uint newPageId = await _pager.AllocatePageAsync(ct);
            var newPage = await _pager.GetPageAsync(newPageId, ct);
            var newSp = new SlottedPage(newPage, newPageId);
            newSp.Initialize(PageConstants.PageTypeInterior);

            newSp.RightChildOrNextLeaf = sp.RightChildOrNextLeaf;
            sp.Initialize(PageConstants.PageTypeInterior);
            sp.RightChildOrNextLeaf = rightChildOfLeft;

            for (int i = 0; i < mid; i++)
                sp.InsertCell(i, splitCellBuffer.AsSpan(i * interiorCellSize, interiorCellSize));

            for (int i = mid + 1; i < totalCellCount; i++)
                newSp.InsertCell(i - mid - 1, splitCellBuffer.AsSpan(i * interiorCellSize, interiorCellSize));

            await _pager.MarkDirtyAsync(pageId, ct);
            await _pager.MarkDirtyAsync(newPageId, ct);

            return new InsertResult { Split = true, SplitKey = promotedKey, NewPageId = newPageId };
        }
        finally
        {
            if (splitCellBuffer != null)
                ArrayPool<byte>.Shared.Return(splitCellBuffer, clearArray: false);
        }
    }

    #endregion

    #region Internal Delete

    private readonly struct DeleteResult
    {
        public DeleteResult(bool deleted, bool underflow)
        {
            Deleted = deleted;
            Underflow = underflow;
        }

        public bool Deleted { get; }
        public bool Underflow { get; }
    }

    private async ValueTask<DeleteResult> DeleteRecursiveAsync(
        uint pageId,
        long key,
        bool isRoot,
        CancellationToken ct)
    {
        var page = await _pager.GetPageAsync(pageId, ct);
        var sp = new SlottedPage(page, pageId);
        if (sp.PageType == PageConstants.PageTypeLeaf)
        {
            int idx = FindKeyInLeaf(sp, key);
            if (idx < 0)
                return default;

            sp.DeleteCell(idx);
            sp.Defragment();
            await _pager.MarkDirtyAsync(pageId, ct);
            bool underflow = !isRoot && sp.CellCount < MinLeafCells;
            return new DeleteResult(deleted: true, underflow);
        }

        uint childPageId = FindChildPageWithIndex(sp, key, out int childIndex);
        var childResult = await DeleteRecursiveAsync(childPageId, key, isRoot: false, ct);
        if (!childResult.Deleted)
            return default;

        if (childResult.Underflow)
            await RebalanceUnderflowedChildAsync(pageId, sp, childIndex, childPageId, ct);

        bool interiorUnderflow = !isRoot && sp.CellCount < MinInteriorCells;
        return new DeleteResult(deleted: true, interiorUnderflow);
    }

    private async ValueTask RebalanceUnderflowedChildAsync(
        uint parentPageId,
        SlottedPage parentSp,
        int childIndex,
        uint childPageId,
        CancellationToken ct)
    {
        var childPage = await _pager.GetPageAsync(childPageId, ct);
        var childSp = new SlottedPage(childPage, childPageId);

        if (childSp.PageType == PageConstants.PageTypeLeaf)
        {
            await RebalanceUnderflowedLeafChildAsync(
                parentPageId,
                parentSp,
                childIndex,
                childPageId,
                childSp,
                ct);
            return;
        }

        if (childSp.PageType == PageConstants.PageTypeInterior)
        {
            await CollapseUnderflowedInteriorChildAsync(
                parentPageId,
                parentSp,
                childIndex,
                childPageId,
                childSp,
                ct);
            return;
        }

        throw new CSharpDbException(
            ErrorCode.CorruptDatabase,
            $"Invalid child page type 0x{childSp.PageType:X2} during delete rebalance.");
    }

    private async ValueTask RebalanceUnderflowedLeafChildAsync(
        uint parentPageId,
        SlottedPage parentSp,
        int childIndex,
        uint childPageId,
        SlottedPage childSp,
        CancellationToken ct)
    {
        if (childSp.CellCount >= MinLeafCells)
            return;

        if (childIndex > 0)
        {
            uint leftPageId = FindChildAtIndex(parentSp, childIndex - 1);
            var leftPage = await _pager.GetPageAsync(leftPageId, ct);
            var leftSp = new SlottedPage(leftPage, leftPageId);

            if (leftSp.PageType != PageConstants.PageTypeLeaf)
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Expected leaf sibling page {leftPageId}, found type 0x{leftSp.PageType:X2}.");
            }

            if (leftSp.CellCount > MinLeafCells)
            {
                ReadOnlyMemory<byte> borrowedCell = leftSp.GetCellMemory(leftSp.CellCount - 1);
                InsertLeafCellAt(childSp, 0, borrowedCell.Span);
                leftSp.DeleteCell(leftSp.CellCount - 1);
                leftSp.Defragment();
                WriteInteriorKey(parentSp, childIndex - 1, ReadLeafKey(childSp, 0));

                await _pager.MarkDirtyAsync(leftPageId, ct);
                await _pager.MarkDirtyAsync(childPageId, ct);
                await _pager.MarkDirtyAsync(parentPageId, ct);
                return;
            }
        }

        if (childIndex < parentSp.CellCount)
        {
            uint rightPageId = FindChildAtIndex(parentSp, childIndex + 1);
            var rightPage = await _pager.GetPageAsync(rightPageId, ct);
            var rightSp = new SlottedPage(rightPage, rightPageId);

            if (rightSp.PageType != PageConstants.PageTypeLeaf)
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Expected leaf sibling page {rightPageId}, found type 0x{rightSp.PageType:X2}.");
            }

            if (rightSp.CellCount > MinLeafCells)
            {
                ReadOnlyMemory<byte> borrowedCell = rightSp.GetCellMemory(0);
                InsertLeafCellAt(childSp, childSp.CellCount, borrowedCell.Span);
                rightSp.DeleteCell(0);
                rightSp.Defragment();
                WriteInteriorKey(parentSp, childIndex, ReadLeafKey(rightSp, 0));

                await _pager.MarkDirtyAsync(childPageId, ct);
                await _pager.MarkDirtyAsync(rightPageId, ct);
                await _pager.MarkDirtyAsync(parentPageId, ct);
                return;
            }
        }

        if (childIndex > 0)
        {
            uint leftPageId = FindChildAtIndex(parentSp, childIndex - 1);
            var leftPage = await _pager.GetPageAsync(leftPageId, ct);
            var leftSp = new SlottedPage(leftPage, leftPageId);

            if (leftSp.PageType != PageConstants.PageTypeLeaf)
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Expected leaf sibling page {leftPageId}, found type 0x{leftSp.PageType:X2}.");
            }

            AppendLeafCells(leftSp, childSp);
            leftSp.RightChildOrNextLeaf = childSp.RightChildOrNextLeaf;
            RemoveInteriorKeyAndChild(parentSp, keyIndexToRemove: childIndex - 1, childIndexToRemove: childIndex);

            await _pager.MarkDirtyAsync(leftPageId, ct);
            await _pager.MarkDirtyAsync(parentPageId, ct);
            await _pager.FreePageAsync(childPageId, ct);
            return;
        }

        if (childIndex < parentSp.CellCount)
        {
            uint rightPageId = FindChildAtIndex(parentSp, childIndex + 1);
            var rightPage = await _pager.GetPageAsync(rightPageId, ct);
            var rightSp = new SlottedPage(rightPage, rightPageId);

            if (rightSp.PageType != PageConstants.PageTypeLeaf)
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    $"Expected leaf sibling page {rightPageId}, found type 0x{rightSp.PageType:X2}.");
            }

            AppendLeafCells(childSp, rightSp);
            childSp.RightChildOrNextLeaf = rightSp.RightChildOrNextLeaf;
            RemoveInteriorKeyAndChild(parentSp, keyIndexToRemove: childIndex, childIndexToRemove: childIndex + 1);

            await _pager.MarkDirtyAsync(childPageId, ct);
            await _pager.MarkDirtyAsync(parentPageId, ct);
            await _pager.FreePageAsync(rightPageId, ct);
        }
    }

    private async ValueTask CollapseUnderflowedInteriorChildAsync(
        uint parentPageId,
        SlottedPage parentSp,
        int childIndex,
        uint childPageId,
        SlottedPage childSp,
        CancellationToken ct)
    {
        if (childSp.CellCount >= MinInteriorCells)
            return;

        uint replacementChildPageId = childSp.RightChildOrNextLeaf;
        if (replacementChildPageId == PageConstants.NullPageId)
        {
            int keyIndexToRemove = childIndex == 0 ? 0 : childIndex - 1;
            RemoveInteriorKeyAndChild(parentSp, keyIndexToRemove, childIndex);
        }
        else
        {
            ReplaceInteriorChildPointer(parentSp, childIndex, replacementChildPageId);
        }

        await _pager.MarkDirtyAsync(parentPageId, ct);
        await _pager.FreePageAsync(childPageId, ct);
    }

    private async ValueTask CollapseRootIfNeededAsync(CancellationToken ct)
    {
        while (true)
        {
            var rootPage = await _pager.GetPageAsync(_rootPageId, ct);
            var rootSp = new SlottedPage(rootPage, _rootPageId);

            if (rootSp.PageType == PageConstants.PageTypeLeaf)
                return;

            if (rootSp.CellCount > 0)
                return;

            uint promotedRootPageId = rootSp.RightChildOrNextLeaf;
            if (promotedRootPageId == PageConstants.NullPageId)
            {
                rootSp.Initialize(PageConstants.PageTypeLeaf);
                await _pager.MarkDirtyAsync(_rootPageId, ct);
                return;
            }

            uint previousRootPageId = _rootPageId;
            _rootPageId = promotedRootPageId;
            await _pager.FreePageAsync(previousRootPageId, ct);
        }
    }

    #endregion

    #region Cell Format Helpers

    internal static byte[] BuildLeafCell(long key, ReadOnlySpan<byte> payload)
    {
        byte[] cell = GC.AllocateUninitializedArray<byte>(GetLeafCellLength(payload.Length));
        WriteLeafCell(cell, key, payload);
        return cell;
    }

    private static int GetLeafCellLength(int payloadLength)
    {
        int payloadPart = 8 + payloadLength;
        return checked(Varint.SizeOf((ulong)payloadPart) + payloadPart);
    }

    private static void WriteLeafCell(Span<byte> destination, long key, ReadOnlySpan<byte> payload)
    {
        // Cell: [totalSize:varint] [key:8] [payload]
        int payloadPart = 8 + payload.Length;
        int pos;
        if (payloadPart <= 0x7F)
        {
            destination[0] = (byte)payloadPart;
            pos = 1;
        }
        else
        {
            pos = Varint.Write(destination, (ulong)payloadPart);
        }

        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(pos, 8), key);
        payload.CopyTo(destination[(pos + 8)..]);
    }

    internal static long ReadLeafKey(SlottedPage sp, int index)
    {
        byte[] page = sp.Buffer;
        int offset = sp.GetCellOffset(index);
        ReadVarintFast(page.AsSpan(offset), out int headerBytes);
        return BinaryPrimitives.ReadInt64LittleEndian(page.AsSpan(offset + headerBytes));
    }

    internal static byte[] ReadLeafPayload(SlottedPage sp, int index)
    {
        return ReadLeafPayloadMemory(sp, index).ToArray();
    }

    internal static ReadOnlyMemory<byte> ReadLeafPayloadMemory(SlottedPage sp, int index)
    {
        byte[] page = sp.Buffer;
        int offset = sp.GetCellOffset(index);
        ulong payloadSize = ReadVarintFast(page.AsSpan(offset), out int headerBytes);
        int payloadLength = (int)payloadSize - 8;
        return page.AsMemory(offset + headerBytes + 8, payloadLength);
    }

    private static long ReadInteriorCellKey(ReadOnlySpan<byte> cell)
    {
        return BinaryPrimitives.ReadInt64LittleEndian(cell[InteriorCellKeyOffset..]);
    }

    private static uint ReadInteriorCellLeftChild(ReadOnlySpan<byte> cell)
    {
        return BinaryPrimitives.ReadUInt32LittleEndian(cell[InteriorCellLeftChildOffset..]);
    }

    private static long ReadLeafCellKey(ReadOnlySpan<byte> cell)
    {
        ReadVarintFast(cell, out int headerBytes);
        return BinaryPrimitives.ReadInt64LittleEndian(cell.Slice(headerBytes, 8));
    }

    private static byte[] BuildSplitCellBuffer(
        byte[] page,
        SlottedPage sp,
        int insertIdx,
        ReadOnlySpan<byte> newCell,
        int[] cellOffsets,
        out int totalCellBytes)
    {
        int existingCellCount = sp.CellCount;
        int totalCellCount = existingCellCount + 1;
        totalCellBytes = newCell.Length;
        for (int i = 0; i < existingCellCount; i++)
            totalCellBytes += GetCellTotalSize(page, sp.GetCellOffset(i));

        byte[] splitCellBuffer = ArrayPool<byte>.Shared.Rent(totalCellBytes);
        int writeOffset = 0;
        for (int i = 0; i < totalCellCount; i++)
        {
            if (i == insertIdx)
            {
                cellOffsets[i] = writeOffset;
                newCell.CopyTo(splitCellBuffer.AsSpan(writeOffset, newCell.Length));
                writeOffset += newCell.Length;
                continue;
            }

            int sourceIndex = i < insertIdx ? i : i - 1;
            ushort sourceOffset = sp.GetCellOffset(sourceIndex);
            int sourceLength = GetCellTotalSize(page, sourceOffset);
            cellOffsets[i] = writeOffset;
            page.AsSpan(sourceOffset, sourceLength).CopyTo(splitCellBuffer.AsSpan(writeOffset, sourceLength));
            writeOffset += sourceLength;
        }

        return splitCellBuffer;
    }

    private static int GetCellTotalSize(byte[] page, ushort offset)
    {
        ulong payloadSize = ReadVarintFast(page.AsSpan(offset), out int headerBytes);
        return checked(headerBytes + (int)payloadSize);
    }

    private static void WriteInteriorCell(Span<byte> destination, uint leftChild, long key)
    {
        // Interior payload size is always 12, encoded as a one-byte varint.
        destination[0] = 12;
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(1, 4), leftChild);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(5, 8), key);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ReadVarintFast(ReadOnlySpan<byte> source, out int bytesRead)
    {
        byte first = source[0];
        if ((first & 0x80) == 0)
        {
            bytesRead = 1;
            return first;
        }

        return Varint.Read(source, out bytesRead);
    }

    private static long ReadInteriorKey(SlottedPage sp, int index)
    {
        byte[] page = sp.Buffer;
        int offset = sp.GetCellOffset(index);
        return BinaryPrimitives.ReadInt64LittleEndian(page.AsSpan(offset + InteriorCellKeyOffset));
    }

    private static uint ReadInteriorLeftChild(SlottedPage sp, int index)
    {
        byte[] page = sp.Buffer;
        int offset = sp.GetCellOffset(index);
        return BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(offset + InteriorCellLeftChildOffset));
    }

    private static void WriteInteriorKey(SlottedPage sp, int index, long key)
    {
        byte[] page = sp.Buffer;
        int offset = sp.GetCellOffset(index);
        BinaryPrimitives.WriteInt64LittleEndian(page.AsSpan(offset + InteriorCellKeyOffset), key);
    }

    private static void InsertLeafCellAt(SlottedPage leaf, int index, ReadOnlySpan<byte> cell)
    {
        if (leaf.InsertCell(index, cell))
            return;

        leaf.Defragment();
        if (leaf.InsertCell(index, cell))
            return;

        throw new CSharpDbException(
            ErrorCode.CorruptDatabase,
            "Leaf page rebalance failed: could not insert moved cell.");
    }

    private static void AppendLeafCells(SlottedPage destination, SlottedPage source)
    {
        int sourceCount = source.CellCount;
        for (int i = 0; i < sourceCount; i++)
            InsertLeafCellAt(destination, destination.CellCount, source.GetCellMemory(i).Span);
    }

    private static void RemoveInteriorKeyAndChild(SlottedPage interior, int keyIndexToRemove, int childIndexToRemove)
    {
        var keys = new List<long>(interior.CellCount);
        var children = new List<uint>(interior.CellCount + 1);
        ReadInteriorNode(interior, keys, children);

        if ((uint)keyIndexToRemove >= (uint)keys.Count || (uint)childIndexToRemove >= (uint)children.Count)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                "Interior rebalance failed: invalid key/child index removal.");
        }

        keys.RemoveAt(keyIndexToRemove);
        children.RemoveAt(childIndexToRemove);
        WriteInteriorNode(interior, keys, children);
    }

    private static void ReplaceInteriorChildPointer(SlottedPage interior, int childIndex, uint replacementChildPageId)
    {
        var keys = new List<long>(interior.CellCount);
        var children = new List<uint>(interior.CellCount + 1);
        ReadInteriorNode(interior, keys, children);

        if ((uint)childIndex >= (uint)children.Count)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                "Interior rebalance failed: child index is out of range.");
        }

        children[childIndex] = replacementChildPageId;
        WriteInteriorNode(interior, keys, children);
    }

    private static void ReadInteriorNode(SlottedPage interior, List<long> keys, List<uint> children)
    {
        keys.Clear();
        children.Clear();

        int keyCount = interior.CellCount;
        for (int i = 0; i < keyCount; i++)
        {
            keys.Add(ReadInteriorKey(interior, i));
            children.Add(ReadInteriorLeftChild(interior, i));
        }

        children.Add(interior.RightChildOrNextLeaf);
    }

    private static void WriteInteriorNode(SlottedPage interior, IReadOnlyList<long> keys, IReadOnlyList<uint> children)
    {
        if (children.Count != keys.Count + 1)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                "Interior rebalance failed: children count must be keys count + 1.");
        }

        interior.Initialize(PageConstants.PageTypeInterior);
        interior.RightChildOrNextLeaf = children[^1];

        Span<byte> cell = stackalloc byte[InteriorCellSize];
        for (int i = 0; i < keys.Count; i++)
        {
            WriteInteriorCell(cell, children[i], keys[i]);
            if (interior.InsertCell(i, cell))
                continue;

            interior.Defragment();
            if (!interior.InsertCell(i, cell))
            {
                throw new CSharpDbException(
                    ErrorCode.CorruptDatabase,
                    "Interior rebalance failed: could not insert rebuilt cell.");
            }
        }
    }

    #endregion

    #region Navigation Helpers

    private int FindInsertPosition(SlottedPage sp, long key) => LowerBoundLeaf(sp, key);

    private int FindKeyInLeaf(SlottedPage sp, long key)
    {
        int idx = LowerBoundLeaf(sp, key);
        return idx < sp.CellCount && ReadLeafKey(sp, idx) == key ? idx : -1;
    }

    private uint FindChildPage(SlottedPage sp, long key)
    {
        int childIndex = UpperBoundInterior(sp, key);
        return childIndex < sp.CellCount
            ? ReadInteriorLeftChild(sp, childIndex)
            : sp.RightChildOrNextLeaf;
    }

    private uint FindChildPageWithIndex(SlottedPage sp, long key, out int childIndex)
    {
        childIndex = UpperBoundInterior(sp, key);
        return childIndex < sp.CellCount
            ? ReadInteriorLeftChild(sp, childIndex)
            : sp.RightChildOrNextLeaf;
    }

    private static int LowerBoundLeaf(SlottedPage sp, long key)
    {
        int lo = 0;
        int hi = sp.CellCount;
        while (lo < hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            if (ReadLeafKey(sp, mid) < key)
                lo = mid + 1;
            else
                hi = mid;
        }
        return lo;
    }

    private static int UpperBoundInterior(SlottedPage sp, long key)
    {
        int lo = 0;
        int hi = sp.CellCount;
        while (lo < hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            if (key < ReadInteriorKey(sp, mid))
                hi = mid;
            else
                lo = mid + 1;
        }
        return lo;
    }

    private uint FindChildAtIndex(SlottedPage sp, int index)
    {
        if (index < sp.CellCount)
            return ReadInteriorLeftChild(sp, index);
        return sp.RightChildOrNextLeaf;
    }

    /// <summary>
    /// Find the leftmost leaf page.
    /// </summary>
    internal async ValueTask<uint> FindLeftmostLeafAsync(CancellationToken ct = default)
    {
        uint pageId = _rootPageId;
        while (true)
        {
            var page = await _pager.GetPageAsync(pageId, ct);
            var sp = new SlottedPage(page, pageId);
            if (sp.PageType == PageConstants.PageTypeLeaf)
                return pageId;
            if (sp.CellCount == 0)
                return sp.RightChildOrNextLeaf;
            pageId = ReadInteriorLeftChild(sp, 0);
        }
    }

    #endregion
}
