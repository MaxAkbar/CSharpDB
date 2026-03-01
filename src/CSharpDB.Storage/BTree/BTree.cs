using System.Buffers.Binary;
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
                    return ReadLeafPayload(hintSp, idx);
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
                return ReadLeafPayload(sp, idx);
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
                    payload = idx >= 0 ? ReadLeafPayload(hintSp, idx) : null;
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
                payload = idx >= 0 ? ReadLeafPayload(sp, idx) : null;
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

            // Insert single interior cell pointing to old root
            var cell = BuildInteriorCell(_rootPageId, result.SplitKey);
            sp.InsertCell(0, cell);
            await _pager.MarkDirtyAsync(newRootId, ct);

            _rootPageId = newRootId;
        }

        _cachedEntryCount = null;
        _hintValid = false;
    }

    /// <summary>
    /// Delete a key. Returns true if the key was found and deleted.
    /// Note: This is a simplified delete that doesn't rebalance/merge underflowed pages.
    /// </summary>
    public async ValueTask<bool> DeleteAsync(long key, CancellationToken ct = default)
    {
        bool deleted = await DeleteRecursiveAsync(_rootPageId, key, ct);
        if (deleted)
        {
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

        var cell = BuildLeafCell(key, payload.Span);

        if (sp.InsertCell(insertIdx, cell))
        {
            await _pager.MarkDirtyAsync(pageId, ct);
            return new InsertResult { Split = false };
        }

        // Page is full — split
        return await SplitLeafAsync(pageId, page, sp, insertIdx, cell, ct);
    }

    private async ValueTask<InsertResult> SplitLeafAsync(uint pageId, byte[] page, SlottedPage sp, int insertIdx, byte[] newCell, CancellationToken ct)
    {
        // Collect all cells + the new one
        int count = sp.CellCount;
        var allCells = new byte[count + 1][];
        for (int i = 0; i < insertIdx; i++)
            allCells[i] = CopyCellBytes(page, sp.GetCellOffset(i));
        allCells[insertIdx] = newCell;
        for (int i = insertIdx; i < count; i++)
            allCells[i + 1] = CopyCellBytes(page, sp.GetCellOffset(i));

        int mid = allCells.Length / 2;

        // Allocate new right sibling
        uint newPageId = await _pager.AllocatePageAsync(ct);
        var newPage = await _pager.GetPageAsync(newPageId, ct);
        var newSp = new SlottedPage(newPage, newPageId);
        newSp.Initialize(PageConstants.PageTypeLeaf);

        // Link leaves
        newSp.RightChildOrNextLeaf = sp.RightChildOrNextLeaf;
        sp.Initialize(PageConstants.PageTypeLeaf);
        sp.RightChildOrNextLeaf = newPageId;

        // Distribute cells
        for (int i = 0; i < mid; i++)
            sp.InsertCell(i, allCells[i]);
        for (int i = mid; i < allCells.Length; i++)
            newSp.InsertCell(i - mid, allCells[i]);

        await _pager.MarkDirtyAsync(pageId, ct);
        await _pager.MarkDirtyAsync(newPageId, ct);

        // The split key is the first key of the right page
        long splitKey = ReadLeafKey(newSp, 0);

        return new InsertResult { Split = true, SplitKey = splitKey, NewPageId = newPageId };
    }

    private async ValueTask<InsertResult> InsertIntoInteriorAsync(uint pageId, byte[] page, SlottedPage sp, long key, uint newChildPageId, int afterChildIdx, CancellationToken ct)
    {
        // Get the original child pointer that was at afterChildIdx
        uint originalChild = FindChildAtIndex(sp, afterChildIdx);

        // The new cell's left child is the original (left half stays)
        var cell = BuildInteriorCell(originalChild, key);

        // Update the pointer at afterChildIdx to point to the new right sibling
        UpdateChildPointerDirect(page, pageId, afterChildIdx, newChildPageId);

        if (sp.InsertCell(afterChildIdx, cell))
        {
            await _pager.MarkDirtyAsync(pageId, ct);
            return new InsertResult { Split = false };
        }

        // Interior page full — split it
        return await SplitInteriorAsync(pageId, page, sp, afterChildIdx, cell, ct);
    }

    private async ValueTask<InsertResult> SplitInteriorAsync(uint pageId, byte[] page, SlottedPage sp, int insertIdx, byte[] newCell, CancellationToken ct)
    {
        int count = sp.CellCount;
        var allCells = new byte[count + 1][];
        for (int i = 0; i < insertIdx; i++)
            allCells[i] = CopyCellBytes(page, sp.GetCellOffset(i));
        allCells[insertIdx] = newCell;
        for (int i = insertIdx; i < count; i++)
            allCells[i + 1] = CopyCellBytes(page, sp.GetCellOffset(i));

        int mid = allCells.Length / 2;
        long promotedKey = ReadInteriorCellKey(allCells[mid]);

        uint newPageId = await _pager.AllocatePageAsync(ct);
        var newPage = await _pager.GetPageAsync(newPageId, ct);
        var newSp = new SlottedPage(newPage, newPageId);
        newSp.Initialize(PageConstants.PageTypeInterior);

        newSp.RightChildOrNextLeaf = sp.RightChildOrNextLeaf;

        uint rightChildOfLeft = ReadInteriorCellLeftChild(allCells[mid]);
        sp.Initialize(PageConstants.PageTypeInterior);
        sp.RightChildOrNextLeaf = rightChildOfLeft;

        for (int i = 0; i < mid; i++)
            sp.InsertCell(i, allCells[i]);

        for (int i = mid + 1; i < allCells.Length; i++)
            newSp.InsertCell(i - mid - 1, allCells[i]);

        await _pager.MarkDirtyAsync(pageId, ct);
        await _pager.MarkDirtyAsync(newPageId, ct);

        return new InsertResult { Split = true, SplitKey = promotedKey, NewPageId = newPageId };
    }

    #endregion

    #region Internal Delete

    private async ValueTask<bool> DeleteRecursiveAsync(uint pageId, long key, CancellationToken ct)
    {
        var page = await _pager.GetPageAsync(pageId, ct);
        var sp = new SlottedPage(page, pageId);

        if (sp.PageType == PageConstants.PageTypeLeaf)
        {
            int idx = FindKeyInLeaf(sp, key);
            if (idx < 0) return false;

            sp.DeleteCell(idx);
            await _pager.MarkDirtyAsync(pageId, ct);
            return true;
        }
        else
        {
            uint childPageId = FindChildPage(sp, key);
            return await DeleteRecursiveAsync(childPageId, key, ct);
        }
    }

    #endregion

    #region Cell Format Helpers

    internal static byte[] BuildLeafCell(long key, ReadOnlySpan<byte> payload)
    {
        // Cell: [totalSize:varint] [key:8] [payload]
        int payloadPart = 8 + payload.Length;
        int sizeBytes = Varint.SizeOf((ulong)payloadPart);
        var cell = new byte[sizeBytes + payloadPart];
        int pos = Varint.Write(cell, (ulong)payloadPart);
        BinaryPrimitives.WriteInt64LittleEndian(cell.AsSpan(pos), key);
        pos += 8;
        payload.CopyTo(cell.AsSpan(pos));
        return cell;
    }

    private static byte[] BuildInteriorCell(uint leftChild, long key)
    {
        // Cell: [totalSize:varint] [leftChild:4] [key:8]
        int payloadPart = 4 + 8;
        int sizeBytes = Varint.SizeOf((ulong)payloadPart);
        var cell = new byte[sizeBytes + payloadPart];
        int pos = Varint.Write(cell, (ulong)payloadPart);
        BinaryPrimitives.WriteUInt32LittleEndian(cell.AsSpan(pos), leftChild);
        pos += 4;
        BinaryPrimitives.WriteInt64LittleEndian(cell.AsSpan(pos), key);
        return cell;
    }

    internal static long ReadLeafKey(SlottedPage sp, int index)
    {
        byte[] page = sp.Buffer;
        int offset = sp.GetCellOffset(index);
        int headerBytes = ReadVarintHeaderLength(page, offset);
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
        ulong payloadSize = Varint.Read(page.AsSpan(offset), out int headerBytes);
        int payloadLength = (int)payloadSize - 8;
        return page.AsMemory(offset + headerBytes + 8, payloadLength);
    }

    private static byte[] CopyCellBytes(byte[] page, ushort offset)
    {
        var cellSpan = page.AsSpan(offset);
        ulong payloadSize = Varint.Read(cellSpan, out int headerBytes);
        int totalSize = headerBytes + (int)payloadSize;
        return cellSpan[..totalSize].ToArray();
    }

    private static long ReadInteriorCellKey(byte[] cell)
    {
        return BinaryPrimitives.ReadInt64LittleEndian(cell.AsSpan(InteriorCellKeyOffset));
    }

    private static uint ReadInteriorCellLeftChild(byte[] cell)
    {
        return BinaryPrimitives.ReadUInt32LittleEndian(cell.AsSpan(InteriorCellLeftChildOffset));
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

    internal void UpdateChildPointerDirect(byte[] pageData, uint pageId, int index, uint newChildId)
    {
        var sp = new SlottedPage(pageData, pageId);
        if (index >= sp.CellCount)
        {
            sp.RightChildOrNextLeaf = newChildId;
        }
        else
        {
            int offset = sp.GetCellOffset(index);
            BinaryPrimitives.WriteUInt32LittleEndian(
                pageData.AsSpan(offset + InteriorCellLeftChildOffset), newChildId);
        }
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

    private static int ReadVarintHeaderLength(byte[] page, int offset)
    {
        int len = 1;
        while ((page[offset + len - 1] & 0x80) != 0 && len < 10)
            len++;
        return len;
    }
}
