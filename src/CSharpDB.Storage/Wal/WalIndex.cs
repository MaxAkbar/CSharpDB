namespace CSharpDB.Storage.Wal;

/// <summary>
/// In-memory index mapping pageId to the file offset of its most recent
/// committed frame in the WAL file. Supports snapshot isolation for concurrent readers.
/// </summary>
public sealed class WalIndex
{
    private readonly object _gate = new();

    // Maps pageId → WAL file offset of the latest committed frame for that page.
    private readonly Dictionary<uint, long> _pageMap = new();

    // Number of committed frames currently in WAL.
    private int _frameCount;

    // Monotonically increasing commit counter.
    private long _commitCounter;

    /// <summary>Total number of committed frames.</summary>
    public int FrameCount => _frameCount;

    /// <summary>Total number of committed publish cycles.</summary>
    public long CommitCounter
    {
        get
        {
            lock (_gate)
            {
                return _commitCounter;
            }
        }
    }

    /// <summary>
    /// Ensure internal page map capacity before bulk frame publication.
    /// </summary>
    public void EnsurePageCapacity(int additionalEntries)
    {
        if (additionalEntries <= 0)
            return;

        lock (_gate)
        {
            _pageMap.EnsureCapacity(_pageMap.Count + additionalEntries);
        }
    }

    /// <summary>
    /// Record a committed frame. Called by WriteAheadLog after writing
    /// each frame that belongs to a committed transaction.
    /// </summary>
    public void AddCommittedFrame(uint pageId, long walFileOffset)
    {
        lock (_gate)
        {
            _pageMap[pageId] = walFileOffset;
            _frameCount++;
        }
    }

    /// <summary>
    /// Advance the commit counter. Called once per commit, after all
    /// frames for that commit have been added.
    /// </summary>
    public void AdvanceCommit()
    {
        lock (_gate)
        {
            _commitCounter++;
        }
    }

    /// <summary>
    /// Try to find the WAL offset for a page in the current (latest) state.
    /// Returns true if the page is in the WAL, false if it should be read from the DB file.
    /// </summary>
    public bool TryGetLatest(uint pageId, out long walOffset)
    {
        lock (_gate)
        {
            return _pageMap.TryGetValue(pageId, out walOffset);
        }
    }

    /// <summary>
    /// Take a snapshot of the current WAL index state. The snapshot
    /// remembers the current page map so that subsequent WAL appends
    /// by the writer do not affect what this reader sees.
    /// </summary>
    public WalSnapshot TakeSnapshot(long? minimumWalOffset = null)
    {
        lock (_gate)
        {
            var snapshot = minimumWalOffset is long walOffsetFloor
                ? FilterPageMap(_pageMap, walOffsetFloor)
                : new Dictionary<uint, long>(_pageMap);
            return new WalSnapshot(snapshot, _commitCounter);
        }
    }

    /// <summary>
    /// Get all committed (pageId, walOffset) pairs for checkpointing.
    /// </summary>
    public IReadOnlyDictionary<uint, long> GetAllCommittedPages()
    {
        lock (_gate)
        {
            return new Dictionary<uint, long>(_pageMap);
        }
    }

    /// <summary>
    /// Internal fast-path access for checkpointing without interface enumeration overhead.
    /// </summary>
    internal Dictionary<uint, long> GetCommittedPages()
    {
        lock (_gate)
        {
            return new Dictionary<uint, long>(_pageMap);
        }
    }

    internal (Dictionary<uint, long> LatestPageMap, int FrameCount, long CommitCounter) GetCommittedStateSnapshot()
    {
        lock (_gate)
        {
            return (new Dictionary<uint, long>(_pageMap), _frameCount, _commitCounter);
        }
    }

    /// <summary>
    /// Reset after a successful checkpoint. Clears all entries.
    /// Must only be called when no readers hold snapshots.
    /// </summary>
    public void Reset()
    {
        lock (_gate)
        {
            _pageMap.Clear();
            _frameCount = 0;
        }
    }

    /// <summary>
    /// Replace the committed WAL state after an in-place compaction that already
    /// preserved the surviving committed frames on disk.
    /// </summary>
    internal void ReplaceCommittedState(
        Dictionary<uint, long> latestPageMap,
        int frameCount,
        int commitAdvanceCount)
    {
        ArgumentNullException.ThrowIfNull(latestPageMap);

        if (frameCount < 0)
            throw new ArgumentOutOfRangeException(nameof(frameCount));
        if (commitAdvanceCount < 0)
            throw new ArgumentOutOfRangeException(nameof(commitAdvanceCount));

        lock (_gate)
        {
            _pageMap.Clear();
            _pageMap.EnsureCapacity(latestPageMap.Count);

            foreach (var entry in latestPageMap)
                _pageMap[entry.Key] = entry.Value;

            _frameCount = frameCount;
            _commitCounter += commitAdvanceCount;
        }
    }

    internal void OverwriteCommittedState(
        Dictionary<uint, long> latestPageMap,
        int frameCount,
        long commitCounter)
    {
        ArgumentNullException.ThrowIfNull(latestPageMap);

        if (frameCount < 0)
            throw new ArgumentOutOfRangeException(nameof(frameCount));
        if (commitCounter < 0)
            throw new ArgumentOutOfRangeException(nameof(commitCounter));

        lock (_gate)
        {
            _pageMap.Clear();
            _pageMap.EnsureCapacity(latestPageMap.Count);

            foreach (var entry in latestPageMap)
                _pageMap[entry.Key] = entry.Value;

            _frameCount = frameCount;
            _commitCounter = commitCounter;
        }
    }

    private static Dictionary<uint, long> FilterPageMap(Dictionary<uint, long> pageMap, long minimumWalOffset)
    {
        if (pageMap.Count == 0)
            return new Dictionary<uint, long>();

        var filtered = new Dictionary<uint, long>(pageMap.Count);
        foreach ((uint pageId, long walOffset) in pageMap)
        {
            if (walOffset >= minimumWalOffset)
                filtered[pageId] = walOffset;
        }

        return filtered;
    }
}

/// <summary>
/// An immutable snapshot of the WAL index at a point in time.
/// Readers use this to resolve pages without seeing uncommitted
/// or future-committed data.
/// </summary>
public sealed class WalSnapshot
{
    private readonly Dictionary<uint, long> _pageMap;
    private readonly long _commitCounter;
    private long _minimumWalOffset;

    internal WalSnapshot(Dictionary<uint, long> pageMap, long commitCounter)
    {
        _pageMap = pageMap;
        _commitCounter = commitCounter;
        _minimumWalOffset = ComputeMinimumWalOffset(pageMap);
    }

    public long CommitCounter => _commitCounter;
    public bool HasWalFrames => _minimumWalOffset != long.MaxValue;
    public long MinimumWalOffset => _minimumWalOffset;

    /// <summary>
    /// Look up a page in this snapshot's WAL state.
    /// </summary>
    public bool TryGet(uint pageId, out long walOffset)
    {
        return _pageMap.TryGetValue(pageId, out walOffset);
    }

    internal void RemapRetainedWalOffsets(long retainedWalStartOffset, long destinationStartOffset)
    {
        if (_pageMap.Count == 0 || retainedWalStartOffset <= destinationStartOffset)
            return;

        long shift = retainedWalStartOffset - destinationStartOffset;
        var keys = _pageMap.Keys.ToArray();
        for (int i = 0; i < keys.Length; i++)
        {
            uint pageId = keys[i];
            long walOffset = _pageMap[pageId];
            if (walOffset >= retainedWalStartOffset)
                _pageMap[pageId] = walOffset - shift;
        }

        _minimumWalOffset = ComputeMinimumWalOffset(_pageMap);
    }

    private static long ComputeMinimumWalOffset(Dictionary<uint, long> pageMap)
    {
        if (pageMap.Count == 0)
            return long.MaxValue;

        long minimumWalOffset = long.MaxValue;
        foreach (long walOffset in pageMap.Values)
        {
            if (walOffset < minimumWalOffset)
                minimumWalOffset = walOffset;
        }

        return minimumWalOffset;
    }

    private static Dictionary<uint, long> FilterPageMap(Dictionary<uint, long> pageMap, long minimumWalOffset)
    {
        if (pageMap.Count == 0)
            return new Dictionary<uint, long>();

        var filtered = new Dictionary<uint, long>(pageMap.Count);
        foreach ((uint pageId, long walOffset) in pageMap)
        {
            if (walOffset >= minimumWalOffset)
                filtered[pageId] = walOffset;
        }

        return filtered;
    }
}
