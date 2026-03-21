namespace CSharpDB.Storage.BTrees;

/// <summary>
/// A forward-only cursor over a B+tree. Iterates leaf pages in order via next-leaf pointers.
/// </summary>
public sealed class BTreeCursor
{
    private const int InteriorCellHeaderBytes = 1; // varint(12)
    private const int InteriorCellLeftChildOffset = InteriorCellHeaderBytes;
    private const int InteriorCellKeyOffset = InteriorCellHeaderBytes + 4;

    private readonly BTree _tree;
    private readonly Pager _pager;
    private uint _currentPageId;
    private int _currentIndex;
    private PageReadBuffer _currentLeafPage;
    private ReadOnlySlottedPage _currentLeaf;
    private uint _prefetchedLeafPageId;
    private Task<PageReadBuffer>? _prefetchedLeafTask;
    private bool _currentLeafLoaded;
    private bool _initialized;
    private bool _eof;

    internal BTreeCursor(BTree tree, Pager pager)
    {
        _tree = tree;
        _pager = pager;
        _currentPageId = 0;
        _currentIndex = -1;
        _currentLeafPage = default;
        _currentLeaf = default;
        _prefetchedLeafPageId = PageConstants.NullPageId;
        _prefetchedLeafTask = null;
        _currentLeafLoaded = false;
        _initialized = false;
        _eof = false;
    }

    public long CurrentKey { get; private set; }
    public ReadOnlyMemory<byte> CurrentValue { get; private set; } = ReadOnlyMemory<byte>.Empty;

    /// <summary>
    /// Move to the next entry. Returns false when there are no more entries.
    /// On first call, positions at the first entry.
    /// </summary>
    public async ValueTask<bool> MoveNextAsync(CancellationToken ct = default)
    {
        if (_eof) return false;

        if (!_initialized)
        {
            _currentPageId = await _tree.FindLeftmostLeafAsync(ct);
            _currentIndex = -1;
            ResetCurrentLeaf(clearPrefetch: true);
            _initialized = true;
        }

        _currentIndex++;

        while (true)
        {
            if (_currentPageId == PageConstants.NullPageId)
            {
                _eof = true;
                return false;
            }

            var sp = await GetCurrentLeafAsync(ct);

            if (_currentIndex < sp.CellCount)
            {
                CurrentKey = BTree.ReadLeafKey(sp, _currentIndex);
                CurrentValue = BTree.ReadLeafPayloadMemory(sp, _currentIndex);
                return true;
            }

            // Move to next leaf
            _currentPageId = sp.RightChildOrNextLeaf;
            _currentIndex = 0;
            ResetCurrentLeaf(clearPrefetch: false);
        }
    }

    /// <summary>
    /// Seek to the first entry with key >= targetKey.
    /// Returns true if positioned at a valid entry.
    /// </summary>
    public async ValueTask<bool> SeekAsync(long targetKey, CancellationToken ct = default)
    {
        _initialized = true;
        _eof = false;
        ResetCurrentLeaf(clearPrefetch: true);

        uint pageId = _tree.RootPageId;

        while (true)
        {
            var page = await _pager.GetPageReadAsync(pageId, ct);
            var sp = new ReadOnlySlottedPage(page.Memory, pageId);

            if (sp.PageType == PageConstants.PageTypeLeaf)
            {
                int i = LowerBoundLeaf(sp, targetKey);
                if (i < sp.CellCount)
                {
                    long key = BTree.ReadLeafKey(sp, i);
                    _currentPageId = pageId;
                    _currentIndex = i;
                    _currentLeafPage = page;
                    _currentLeaf = sp;
                    _currentLeafLoaded = true;
                    ScheduleLeafPrefetch(sp.RightChildOrNextLeaf);
                    CurrentKey = key;
                    CurrentValue = BTree.ReadLeafPayloadMemory(sp, i);
                    return true;
                }

                uint nextLeaf = sp.RightChildOrNextLeaf;
                if (nextLeaf == PageConstants.NullPageId)
                {
                    _eof = true;
                    return false;
                }

                _currentPageId = nextLeaf;
                _currentIndex = -1;
                ResetCurrentLeaf(clearPrefetch: true);
                return await MoveNextAsync(ct);
            }
            else
            {
                int childIdx = UpperBoundInterior(sp, targetKey);
                pageId = childIdx < sp.CellCount
                    ? ReadInteriorLeftChild(sp, childIdx)
                    : sp.RightChildOrNextLeaf;
            }
        }
    }

    private static int LowerBoundLeaf(ReadOnlySlottedPage sp, long key)
    {
        int lo = 0;
        int hi = sp.CellCount;
        while (lo < hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            if (BTree.ReadLeafKey(sp, mid) < key)
                lo = mid + 1;
            else
                hi = mid;
        }
        return lo;
    }

    private static int UpperBoundInterior(ReadOnlySlottedPage sp, long key)
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

    private static long ReadInteriorKey(ReadOnlySlottedPage sp, int index)
    {
        ReadOnlyMemory<byte> page = sp.Buffer;
        int offset = sp.GetCellOffset(index);
        return System.Buffers.Binary.BinaryPrimitives.ReadInt64LittleEndian(
            page.Span[(offset + InteriorCellKeyOffset)..]);
    }

    private static uint ReadInteriorLeftChild(ReadOnlySlottedPage sp, int index)
    {
        ReadOnlyMemory<byte> page = sp.Buffer;
        int offset = sp.GetCellOffset(index);
        return System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(
            page.Span[(offset + InteriorCellLeftChildOffset)..]);
    }

    private async ValueTask<ReadOnlySlottedPage> GetCurrentLeafAsync(CancellationToken ct)
    {
        if (_currentLeafLoaded)
            return _currentLeaf;

        if (_prefetchedLeafTask is not null && _prefetchedLeafPageId == _currentPageId)
        {
            try
            {
                _currentLeafPage = await _prefetchedLeafTask.WaitAsync(ct);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch
            {
                _currentLeafPage = await _pager.GetPageReadAsync(_currentPageId, ct);
            }
            finally
            {
                ClearPrefetchedLeaf();
            }
        }
        else
        {
            _currentLeafPage = await _pager.GetPageReadAsync(_currentPageId, ct);
        }

        _currentLeaf = new ReadOnlySlottedPage(_currentLeafPage.Memory, _currentPageId);
        _currentLeafLoaded = true;
        ScheduleLeafPrefetch(_currentLeaf.RightChildOrNextLeaf);
        return _currentLeaf;
    }

    private void ResetCurrentLeaf(bool clearPrefetch)
    {
        _currentLeafPage = default;
        _currentLeaf = default;
        _currentLeafLoaded = false;
        if (clearPrefetch)
            ClearPrefetchedLeaf();
    }

    private void ScheduleLeafPrefetch(uint nextLeafPageId)
    {
        if (!_pager.CanSpeculativePageReads || nextLeafPageId == PageConstants.NullPageId)
        {
            ClearPrefetchedLeaf();
            return;
        }

        if (_prefetchedLeafTask is not null && _prefetchedLeafPageId == nextLeafPageId)
            return;

        if (_pager.TryGetCachedPageReadBuffer(nextLeafPageId, out _))
        {
            ClearPrefetchedLeaf();
            return;
        }

        _prefetchedLeafPageId = nextLeafPageId;
        _prefetchedLeafTask = _pager.ReadPageUncachedAsync(nextLeafPageId, CancellationToken.None).AsTask();
    }

    private void ClearPrefetchedLeaf()
    {
        _prefetchedLeafPageId = PageConstants.NullPageId;
        _prefetchedLeafTask = null;
    }
}
