using System.Numerics;
using System.Runtime.CompilerServices;

namespace CSharpDB.Storage.Wal;

/// <summary>
/// In-memory index mapping pageId to the file offset of its most recent
/// committed frame in the WAL file. Supports snapshot isolation for concurrent readers.
/// </summary>
public sealed class WalIndex
{
    static WalIndex()
    {
        // Initialize shared map storage before the first index is constructed,
        // rather than allowing its first allocation during reader admission.
        _ = WalSnapshotMap.Empty;
    }

    private readonly object _gate = new();

    // One mutable map keeps bulk publication and checkpoint copies independent of
    // page-ID distribution. Readers build the segment-key directory lazily, so
    // writes and recovery without snapshots do not maintain a second index.
    private readonly Dictionary<uint, long> _pageMap = new();
    private WalSnapshotKeys? _snapshotKeys;
    private ulong _dirtySegments = ulong.MaxValue;

    // Keep only the most recently requested floor for the current map state.
    // Mutations mark segments dirty under _gate. Unchanged immutable segments are
    // shared with the next directory; old readers retain their complete directory.
    private WalSnapshotMap? _snapshotPageMap;
    private long? _snapshotWalOffsetFloor;

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
            EnsurePageCapacityCore(additionalEntries);
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
            SetPageCore(pageId, walFileOffset);
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
                EnsurePageCapacityCore(frames.Length);
            }

            ulong dirty = _dirtySegments;
            foreach (var frame in frames)
            {
                _pageMap[frame.PageId] = frame.WalOffset;
                dirty |= 1UL << WalSnapshotMap.GetSegmentIndex(frame.PageId);
            }
            _dirtySegments = dirty;

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
                EnsurePageCapacityCore(frames.Length);
            }

            ulong dirty = _dirtySegments;
            for (int i = 0; i < frames.Length; i++)
            {
                uint pageId = frames[i].PageId;
                _pageMap[pageId] = firstFrameOffset + (long)i * PageConstants.WalFrameSize;
                dirty |= 1UL << WalSnapshotMap.GetSegmentIndex(pageId);
            }
            _dirtySegments = dirty;

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
        var snapshot = new WalSnapshot();
        InitializeSnapshot(snapshot, minimumWalOffset);
        return snapshot;
    }

    /// <summary>
    /// Capture the current generation into an exclusively owned, unpublished wrapper.
    /// The caller must not hold checkpoint protection: cache misses prepare map storage
    /// between attempts. Checkpoint admission uses TryInitializeSnapshot instead.
    /// </summary>
    internal void InitializeSnapshot(WalSnapshot snapshot, long? minimumWalOffset = null)
    {
        WalSnapshotPreparation? preparation = null;
        Span<int> requiredCapacities = stackalloc int[WalSnapshotMap.SegmentCount];
        while (!TryInitializeSnapshot(snapshot, preparation, minimumWalOffset, requiredCapacities, out int segmentCount, out int keyCapacity))
        {
            if (preparation is null || preparation.Buffers.Length != segmentCount)
                preparation = new WalSnapshotPreparation(segmentCount);
            preparation.EnsureCapacity(requiredCapacities, keyCapacity);
        }
    }

    /// <summary>
    /// Capture a reusable immutable map without preparing segmented storage.
    /// The caller still holds checkpoint protection through reader registration.
    /// A miss leaves the wrapper unpublished and requires the normal preparation path.
    /// </summary>
    internal bool TryInitializeCachedSnapshot(WalSnapshot snapshot, long? minimumWalOffset)
    {
        lock (_gate)
        {
            if (_snapshotPageMap is null || _dirtySegments != 0 || _snapshotWalOffsetFloor != minimumWalOffset)
                return false;

            snapshot.Initialize(_snapshotPageMap, _commitCounter, _snapshotPageMap.MinimumWalOffset);
            return true;
        }
    }

    /// <summary>
    /// Capture one complete generation without allocating snapshot-map storage under
    /// the publication gate. On a capacity miss, report the segment capacities and
    /// leave the wrapper and preparation unpublished. Only dirty segments are copied;
    /// unchanged immutable segments are shared. Success consumes the preparation.
    /// </summary>
    internal bool TryInitializeSnapshot(
        WalSnapshot snapshot,
        WalSnapshotPreparation? preparation,
        long? minimumWalOffset,
        Span<int> requiredCapacities,
        out int segmentCount,
        out int keyCapacity)
    {
        lock (_gate)
        {
            segmentCount = _pageMap.Count <= WalSnapshotMap.CompactPageLimit ? 1 : WalSnapshotMap.SegmentCount;
            keyCapacity = 0;
            if (_snapshotPageMap is null || _dirtySegments != 0 || _snapshotWalOffsetFloor != minimumWalOffset)
            {
                WalSnapshotMap pageMap = WalSnapshotMap.Empty;
                if (_pageMap.Count != 0)
                {
                    bool compact = segmentCount == 1;
                    bool rebuildKeys = !compact && (_snapshotKeys is null || _snapshotKeys.Count != _pageMap.Count);
                    if (rebuildKeys)
                    {
                        keyCapacity = _pageMap.Count;
                        requiredCapacities.Clear();
                        foreach (var entry in _pageMap)
                            requiredCapacities[WalSnapshotMap.GetSegmentIndex(entry.Key)]++;
                    }
                    ulong dirty = compact || _snapshotPageMap is null ||
                        _snapshotPageMap.Segments.Length != segmentCount || _snapshotWalOffsetFloor != minimumWalOffset
                        ? ulong.MaxValue : _dirtySegments;
                    bool ready = preparation is not null && preparation.Buffers.Length == segmentCount;
                    if (rebuildKeys && (preparation?.Keys is null || preparation.Keys.PageIds.Length < keyCapacity))
                        ready = false;
                    if (preparation?.Published == true)
                        throw new InvalidOperationException("The snapshot preparation was already published.");

                    // Validate every capacity before filling anything: writers may
                    // have changed a different segment while this reader allocated.
                    int copiedPageCount = 0;
                    for (int i = 0; i < segmentCount; i++)
                    {
                        int count = compact ? _pageMap.Count : (dirty & (1UL << i)) == 0 ? 0 :
                            rebuildKeys ? requiredCapacities[i] : _snapshotKeys!.Offsets[i + 1] - _snapshotKeys.Offsets[i];
                        requiredCapacities[i] = count;
                        copiedPageCount += count;
                        if (count == 0)
                            continue;
                        var buffer = preparation is not null && preparation.Buffers.Length == segmentCount
                            ? preparation.Buffers[i] : null;
                        if (buffer is null || buffer.Capacity < count)
                            ready = false;
                        else if (buffer.Count != 0)
                            throw new ArgumentException("Snapshot buffers must be empty and unpublished.", nameof(preparation));
                    }
                    if (!ready)
                        return false;

                    if (rebuildKeys)
                    {
                        preparation!.Keys!.Initialize(_pageMap);
                        _snapshotKeys = preparation.Keys;
                    }
                    pageMap = preparation!.Map;
                    long floor = minimumWalOffset.GetValueOrDefault(long.MinValue);
                    if (copiedPageCount == _pageMap.Count)
                    {
                        CopyFullSnapshotPages(preparation, compact, floor);
                        for (int i = 0; i < segmentCount; i++)
                        {
                            if (compact || (dirty & (1UL << i)) != 0)
                            {
                                var buffer = preparation.Buffers[i];
                                if (buffer is not null && buffer.Count != 0)
                                    pageMap.Segments[i] = buffer;
                            }
                        }
                    }
                    else
                    {
                        // Share immutable references in bulk, then copy and
                        // replace only dirty segments, including empty views.
                        _snapshotPageMap!.Segments.CopyTo(pageMap.Segments, 0);
                        var keys = _snapshotKeys!;
                        for (ulong remaining = dirty; remaining != 0; remaining &= remaining - 1)
                        {
                            int i = BitOperations.TrailingZeroCount(remaining);
                            for (int key = keys.Offsets[i]; key < keys.Offsets[i + 1]; key++)
                            {
                                uint pageId = keys.PageIds[key];
                                long offset = _pageMap[pageId];
                                if (offset >= floor)
                                    CopySnapshotPage(preparation, i, pageId, offset);
                            }
                            var buffer = preparation.Buffers[i];
                            pageMap.Segments[i] = buffer is not null && buffer.Count != 0 ? buffer : null;
                        }
                    }
                    pageMap.Complete();
                    preparation.Published = true;
                    // An empty large view still caches its segment layout, so a
                    // later update need not rebuild unrelated filtered segments.
                    if (pageMap.Count == 0 && segmentCount == 1)
                        pageMap = WalSnapshotMap.Empty;
                }

                _snapshotWalOffsetFloor = minimumWalOffset;
                _snapshotPageMap = pageMap;
                _dirtySegments = 0;
            }

            // Capture one complete generation, including counter-only advances.
            snapshot.Initialize(_snapshotPageMap, _commitCounter, _snapshotPageMap.MinimumWalOffset);
            return true;
        }
    }

    /// <summary>
    /// Get all committed (pageId, walOffset) pairs for checkpointing.
    /// </summary>
    public IReadOnlyDictionary<uint, long> GetAllCommittedPages()
    {
        lock (_gate)
        {
            return CopyCommittedPagesCore();
        }
    }

    /// <summary>
    /// Internal fast-path access for checkpointing without interface enumeration overhead.
    /// </summary>
    internal Dictionary<uint, long> GetCommittedPages()
    {
        lock (_gate)
        {
            return CopyCommittedPagesCore();
        }
    }

    internal (Dictionary<uint, long> LatestPageMap, int FrameCount, long CommitCounter) GetCommittedStateSnapshot()
    {
        lock (_gate)
        {
            return (CopyCommittedPagesCore(), _frameCount, _commitCounter);
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
            ClearPagesCore();
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
            ClearPagesCore();
            EnsurePageCapacityCore(latestPageMap.Count);

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
            ClearPagesCore();
            EnsurePageCapacityCore(latestPageMap.Count);

            foreach (var entry in latestPageMap)
                _pageMap[entry.Key] = entry.Value;

            _frameCount = frameCount;
            _commitCounter = commitCounter;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SetPageCore(uint pageId, long offset)
    {
        _pageMap[pageId] = offset;
        _dirtySegments |= 1UL << WalSnapshotMap.GetSegmentIndex(pageId);
    }

    private void EnsurePageCapacityCore(int additionalEntries)
    {
        _pageMap.EnsureCapacity(_pageMap.Count + additionalEntries);
    }

    private void ClearPagesCore()
    {
        _snapshotPageMap = null;
        _dirtySegments = ulong.MaxValue;
        _pageMap.Clear();
        _snapshotKeys = null;
    }

    private Dictionary<uint, long> CopyCommittedPagesCore()
    {
        return new Dictionary<uint, long>(_pageMap);
    }

    // Called under _gate after validating every destination capacity. Keep the
    // full-copy loops separate from the capture path used by narrow updates.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void CopyFullSnapshotPages(WalSnapshotPreparation preparation, bool compact, long floor)
    {
        // Stream the map once, choosing the layout and filter before copying.
        if (compact)
        {
            var buffer = preparation.Buffers[0]!;
            if (floor == long.MinValue)
            {
                foreach (var entry in _pageMap)
                    buffer.AddUnique(entry.Key, entry.Value);
            }
            else
            {
                foreach (var entry in _pageMap)
                {
                    if (entry.Value >= floor)
                        buffer.AddUnique(entry.Key, entry.Value);
                }
            }
        }
        else if (floor == long.MinValue)
        {
            foreach (var entry in _pageMap)
                CopySnapshotPage(preparation, WalSnapshotMap.GetSegmentIndex(entry.Key), entry.Key, entry.Value);
        }
        else
        {
            foreach (var entry in _pageMap)
            {
                if (entry.Value >= floor)
                    CopySnapshotPage(preparation, WalSnapshotMap.GetSegmentIndex(entry.Key), entry.Key, entry.Value);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void CopySnapshotPage(WalSnapshotPreparation preparation, int segment, uint pageId, long offset)
    {
        var buffer = preparation.Buffers[segment]!;
        buffer.AddUnique(pageId, offset);
    }
}

/// <summary>
/// An immutable snapshot of the WAL index at a point in time.
/// Readers use this to resolve pages without seeing uncommitted
/// or future-committed data.
/// </summary>
public sealed class WalSnapshot
{
    // Written once while this wrapper is exclusively owned and unpublished.
    // A non-null map marks initialization, including for an empty snapshot.
    private WalSnapshotMap _pageMap = null!;
    private long _commitCounter;
    private long _minimumWalOffset;

    internal WalSnapshot()
    {
    }

    internal void Initialize(WalSnapshotMap pageMap, long commitCounter, long minimumWalOffset)
    {
        if (_pageMap is not null)
            throw new InvalidOperationException("The WAL snapshot is already initialized.");

        ArgumentNullException.ThrowIfNull(pageMap);
        _commitCounter = commitCounter;
        _minimumWalOffset = minimumWalOffset;
        _pageMap = pageMap;
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
