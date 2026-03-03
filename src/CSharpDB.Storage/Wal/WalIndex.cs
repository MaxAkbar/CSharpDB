namespace CSharpDB.Storage.Wal;

/// <summary>
/// In-memory index mapping pageId to the file offset of its most recent
/// committed frame in the WAL file. Supports snapshot isolation for concurrent readers.
/// </summary>
public sealed class WalIndex
{
    // Maps pageId → WAL file offset of the latest committed frame for that page.
    private readonly Dictionary<uint, long> _pageMap = new();

    // Number of committed frames currently in WAL.
    private int _frameCount;

    // Monotonically increasing commit counter.
    private long _commitCounter;

    /// <summary>Total number of committed frames.</summary>
    public int FrameCount => _frameCount;

    /// <summary>
    /// Ensure internal page map capacity before bulk frame publication.
    /// </summary>
    public void EnsurePageCapacity(int additionalEntries)
    {
        if (additionalEntries <= 0)
            return;

        _pageMap.EnsureCapacity(_pageMap.Count + additionalEntries);
    }

    /// <summary>
    /// Record a committed frame. Called by WriteAheadLog after writing
    /// each frame that belongs to a committed transaction.
    /// </summary>
    public void AddCommittedFrame(uint pageId, long walFileOffset)
    {
        _pageMap[pageId] = walFileOffset;
        _frameCount++;
    }

    /// <summary>
    /// Advance the commit counter. Called once per commit, after all
    /// frames for that commit have been added.
    /// </summary>
    public void AdvanceCommit()
    {
        _commitCounter++;
    }

    /// <summary>
    /// Try to find the WAL offset for a page in the current (latest) state.
    /// Returns true if the page is in the WAL, false if it should be read from the DB file.
    /// </summary>
    public bool TryGetLatest(uint pageId, out long walOffset)
    {
        return _pageMap.TryGetValue(pageId, out walOffset);
    }

    /// <summary>
    /// Take a snapshot of the current WAL index state. The snapshot
    /// remembers the current page map so that subsequent WAL appends
    /// by the writer do not affect what this reader sees.
    /// </summary>
    public WalSnapshot TakeSnapshot()
    {
        var snapshot = new Dictionary<uint, long>(_pageMap);
        return new WalSnapshot(snapshot, _commitCounter);
    }

    /// <summary>
    /// Get all committed (pageId, walOffset) pairs for checkpointing.
    /// </summary>
    public IReadOnlyDictionary<uint, long> GetAllCommittedPages()
    {
        return _pageMap;
    }

    /// <summary>
    /// Reset after a successful checkpoint. Clears all entries.
    /// Must only be called when no readers hold snapshots.
    /// </summary>
    public void Reset()
    {
        _pageMap.Clear();
        _frameCount = 0;
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

    internal WalSnapshot(Dictionary<uint, long> pageMap, long commitCounter)
    {
        _pageMap = pageMap;
        _commitCounter = commitCounter;
    }

    public long CommitCounter => _commitCounter;

    /// <summary>
    /// Look up a page in this snapshot's WAL state.
    /// </summary>
    public bool TryGet(uint pageId, out long walOffset)
    {
        return _pageMap.TryGetValue(pageId, out walOffset);
    }
}
