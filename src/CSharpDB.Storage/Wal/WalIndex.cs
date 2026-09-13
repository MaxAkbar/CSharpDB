namespace CSharpDB.Storage.Wal;

/// <summary>
/// In-memory index mapping pageId to the file offset of its most recent
/// committed frame in the WAL file. Supports snapshot isolation for concurrent readers.
/// </summary>
public sealed class WalIndex
{
    // Snapshot maps are privately owned copies and are never mutated after publication.
    private static readonly Dictionary<uint, long> s_emptyPageMap = new();

    private readonly object _gate = new();

    // Maps pageId → WAL file offset of the latest committed frame for that page.
    private readonly Dictionary<uint, long> _pageMap = new();

    // Keep only the most recently requested floor for the current map state.
    // Every map content change invalidates this copy under _gate; existing readers
    // retain their old copy independently of the current cache entry.
    private Dictionary<uint, long>? _snapshotPageMap;
    private long? _snapshotWalOffsetFloor;
    private long _snapshotMinimumWalOffset;

    // Number of committed frames currently in WAL.
    private int _frameCount;

    // Monotonically increasing commit counter.
    private long _commitCounter;

    // Sticky-saturating logical commits published through the normal commit path.
    // Recovery, checkpoint remapping, and index reset intentionally do not alter it.
    private long _logicalCommitCount;

    // Sticky-saturating committed page images published through the normal live
    // commit path. Recovery and checkpoint index maintenance do not alter it.
    private long _logicalPageWriteCount;

    /// <summary>Total number of committed frames.</summary>
    public int FrameCount => _frameCount;

    internal (
        int FrameCount,
        long LogicalCommitCount,
        long LogicalPageWriteCount) GetRuntimeStateSnapshot()
    {
        lock (_gate)
        {
            return (
                _frameCount,
                _logicalCommitCount,
                _logicalPageWriteCount);
        }
    }

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
            _snapshotPageMap = null;
            _pageMap[pageId] = walFileOffset;
            _frameCount++;
        }
    }

    /// <summary>
    /// Publish a complete live commit under one lock so snapshots observe its
    /// page locations and counters together. The caller has already flushed it.
    /// </summary>
    internal void PublishCommittedFrames(ReadOnlySpan<(uint PageId, long WalOffset)> frames)
    {
        lock (_gate)
        {
            if (!frames.IsEmpty)
            {
                _snapshotPageMap = null;
                _pageMap.EnsureCapacity(_pageMap.Count + frames.Length);
            }

            foreach (var frame in frames)
                _pageMap[frame.PageId] = frame.WalOffset;

            _frameCount += frames.Length;
            AdvanceCommitCore(frames.Length);
        }
    }

    /// <summary>
    /// Publish contiguous frame locations without materializing a location array.
    /// </summary>
    internal void PublishCommittedFrames(ReadOnlySpan<WalFrameWrite> frames, long firstFrameOffset)
    {
        lock (_gate)
        {
            if (!frames.IsEmpty)
            {
                _snapshotPageMap = null;
                _pageMap.EnsureCapacity(_pageMap.Count + frames.Length);
            }

            for (int i = 0; i < frames.Length; i++)
                _pageMap[frames[i].PageId] = firstFrameOffset + (long)i * PageConstants.WalFrameSize;

            _frameCount += frames.Length;
            AdvanceCommitCore(frames.Length);
        }
    }

    /// <summary>
    /// Advance the commit counter. Called once per commit, after all
    /// frames for that commit have been added.
    /// </summary>
    public void AdvanceCommit()
        => AdvanceCommit(committedFrameCount: 0);

    /// <summary>
    /// Advance the live commit counters after publishing the exact number of
    /// frames made visible by the commit.
    /// </summary>
    internal void AdvanceCommit(int committedFrameCount)
    {
        if (committedFrameCount < 0)
            throw new ArgumentOutOfRangeException(nameof(committedFrameCount));

        lock (_gate)
        {
            AdvanceCommitCore(committedFrameCount);
        }
    }

    private void AdvanceCommitCore(int committedFrameCount)
    {
        _commitCounter++;
        if (_logicalCommitCount != long.MaxValue)
            _logicalCommitCount++;
        _logicalPageWriteCount = _logicalPageWriteCount >=
            long.MaxValue - committedFrameCount
                ? long.MaxValue
                : _logicalPageWriteCount + committedFrameCount;
    }

    /// <summary>
    /// Reconstruct the snapshot generation while scanning an existing WAL.
    /// Recovered commits predate this live runtime epoch and therefore do not
    /// contribute to its logical lifetime commit count.
    /// </summary>
    internal void AdvanceRecoveredCommit()
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
            if (_snapshotPageMap is null || _snapshotWalOffsetFloor != minimumWalOffset)
            {
                Dictionary<uint, long> snapshot = _pageMap.Count == 0
                    ? s_emptyPageMap
                    : minimumWalOffset is long walOffsetFloor
                        ? FilterPageMap(_pageMap, walOffsetFloor)
                        : new Dictionary<uint, long>(_pageMap);

                _snapshotMinimumWalOffset = ComputeMinimumWalOffset(snapshot);
                _snapshotWalOffsetFloor = minimumWalOffset;
                _snapshotPageMap = snapshot;
            }

            // Reader registration uses wrapper identity, even when its map is shared.
            // Read the counter afresh: counter-only advances can reuse the same map.
            return new WalSnapshot(_snapshotPageMap, _commitCounter, _snapshotMinimumWalOffset);
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
            _snapshotPageMap = null;
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
            _snapshotPageMap = null;
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
            _snapshotPageMap = null;
            _pageMap.Clear();
            _pageMap.EnsureCapacity(latestPageMap.Count);

            foreach (var entry in latestPageMap)
                _pageMap[entry.Key] = entry.Value;

            _frameCount = frameCount;
            _commitCounter = commitCounter;
        }
    }

    private static long ComputeMinimumWalOffset(Dictionary<uint, long> pageMap)
    {
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
            return s_emptyPageMap;

        Dictionary<uint, long>? filtered = null;
        foreach ((uint pageId, long walOffset) in pageMap)
        {
            if (walOffset >= minimumWalOffset)
            {
                filtered ??= new Dictionary<uint, long>(pageMap.Count);
                filtered[pageId] = walOffset;
            }
        }

        return filtered ?? s_emptyPageMap;
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
    private readonly long _minimumWalOffset;

    internal WalSnapshot(Dictionary<uint, long> pageMap, long commitCounter, long minimumWalOffset)
    {
        _pageMap = pageMap;
        _commitCounter = commitCounter;
        _minimumWalOffset = minimumWalOffset;
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
}
