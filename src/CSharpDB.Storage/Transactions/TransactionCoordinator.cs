using System.Buffers;
using CSharpDB.Primitives;
using CSharpDB.Storage.Wal;

namespace CSharpDB.Storage.Transactions;

/// <summary>
/// Coordinates legacy single-writer transactions alongside explicit multi-writer
/// begin/commit metadata, checkpoint barriers, and page-version validation state.
/// </summary>
internal sealed class TransactionCoordinator : IDisposable
{
    private readonly object _stateGate = new();
    private readonly object _pageVersionGate = new();
    private readonly SemaphoreSlim _beginBarrier = new(1, 1);
    private readonly SemaphoreSlim _writerLock = new(1, 1);
    private readonly SemaphoreSlim _commitLock = new(1, 1);
    private readonly Dictionary<uint, long> _pageLastWriteVersion = new();
    private readonly Dictionary<LogicalConflictKey, long> _logicalLastWriteVersion = new();
    private readonly Dictionary<long, long> _activeExplicitTransactions = new();
    private long _currentTransactionId;
    private long _nextTransactionId;
    private long _commitVersion;
    private long _nextReservedCommitVersion;
    private long _nextReservedPageId;
    private long _schemaExclusiveOwnerTransactionId;
    private long _schemaExclusiveWaiterTransactionId;
    private int _inTransactionFlag;
    private bool _inTransaction;
    private bool _writerLockReleased;
    private int _pendingCommitWindowCount;
    private bool _pendingCommitBarrierHeld;
    private TaskCompletionSource<bool> _schemaStateChanged = CreateStateChangedSource();
    private TaskCompletionSource<bool> _commitStateChanged = CreateStateChangedSource();

    public bool InTransaction => Volatile.Read(ref _inTransactionFlag) != 0;

    public long CurrentCommitVersion => Volatile.Read(ref _commitVersion);

    public void EnsureNextReservedPageIdAtLeast(uint pageCount)
    {
        while (true)
        {
            long current = Volatile.Read(ref _nextReservedPageId);
            if (current >= pageCount)
                return;

            if (Interlocked.CompareExchange(ref _nextReservedPageId, pageCount, current) == current)
                return;
        }
    }

    public long CreateTransactionId() => Interlocked.Increment(ref _nextTransactionId);

    public void RegisterExplicitTransaction(long transactionId, long startVersion)
    {
        if (transactionId == 0)
            return;

        lock (_stateGate)
        {
            _activeExplicitTransactions[transactionId] = startVersion;
            SignalSchemaStateChanged_NoLock();
        }
    }

    public void UnregisterExplicitTransaction(long transactionId)
    {
        if (transactionId == 0)
            return;

        long retentionFloor;
        lock (_stateGate)
        {
            _activeExplicitTransactions.Remove(transactionId);
            if (_schemaExclusiveOwnerTransactionId == transactionId)
                _schemaExclusiveOwnerTransactionId = 0;
            if (_schemaExclusiveWaiterTransactionId == transactionId)
                _schemaExclusiveWaiterTransactionId = 0;
            retentionFloor = GetConflictRetentionFloor_NoLock();
            SignalSchemaStateChanged_NoLock();
        }

        PruneConflictVersionsUpTo(retentionFloor);
    }

    public uint ReserveNewPageId()
    {
        long reserved = Interlocked.Increment(ref _nextReservedPageId) - 1;
        if (reserved < 0 || reserved > uint.MaxValue)
            throw new CSharpDbException(ErrorCode.Unknown, "Reserved page id exceeded supported range.");

        return (uint)reserved;
    }

    public async ValueTask BeginAsync(
        IWriteAheadLog wal,
        TimeSpan writerLockTimeout,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(wal);

        lock (_stateGate)
        {
            if (_inTransaction)
                throw new CSharpDbException(ErrorCode.Unknown, "Nested transactions are not supported.");
        }

        await _beginBarrier.WaitAsync(ct);
        try
        {
            if (!await _writerLock.WaitAsync(writerLockTimeout, ct))
                throw new CSharpDbException(ErrorCode.Busy, "Could not acquire write lock (database is busy).");

            try
            {
                wal.BeginTransaction();
                lock (_stateGate)
                {
                    _currentTransactionId = Interlocked.Increment(ref _nextTransactionId);
                    _inTransaction = true;
                    Volatile.Write(ref _inTransactionFlag, 1);
                    _writerLockReleased = false;
                }
            }
            catch
            {
                _writerLock.Release();
                throw;
            }
        }
        finally
        {
            _beginBarrier.Release();
        }
    }

    public long ReleaseWriterAfterCommitAppend()
    {
        long transactionId;
        bool releaseWriter = false;
        lock (_stateGate)
        {
            transactionId = _currentTransactionId;
            _inTransaction = false;
            Volatile.Write(ref _inTransactionFlag, 0);
            if (_writerLockReleased)
                return transactionId;

            _writerLockReleased = true;
            releaseWriter = true;
        }

        if (releaseWriter)
            _writerLock.Release();

        return transactionId;
    }

    public void CompleteCommit(long transactionId)
    {
        bool releaseWriter = false;
        lock (_stateGate)
        {
            if (_currentTransactionId != transactionId || _writerLockReleased)
                return;

            _inTransaction = false;
            Volatile.Write(ref _inTransactionFlag, 0);
            _writerLockReleased = true;
            releaseWriter = true;
        }

        if (releaseWriter)
            _writerLock.Release();
    }

    public bool TryBeginRollback()
    {
        lock (_stateGate)
        {
            return _inTransaction;
        }
    }

    public void CompleteRollback()
    {
        bool releaseWriter = false;
        lock (_stateGate)
        {
            _inTransaction = false;
            Volatile.Write(ref _inTransactionFlag, 0);
            if (_writerLockReleased)
                return;

            _writerLockReleased = true;
            releaseWriter = true;
        }

        if (!releaseWriter)
            return;

        try
        {
            _writerLock.Release();
        }
        catch (SemaphoreFullException)
        {
        }
    }

    public async ValueTask<IDisposable> AcquireBeginBarrierAsync(CancellationToken ct = default)
    {
        await _beginBarrier.WaitAsync(ct);
        return new SemaphoreReservation(_beginBarrier);
    }

    public async ValueTask WaitForSchemaBeginAsync(CancellationToken ct = default)
    {
        while (true)
        {
            Task waitTask;
            lock (_stateGate)
            {
                if (_schemaExclusiveOwnerTransactionId == 0 && _schemaExclusiveWaiterTransactionId == 0)
                    return;

                waitTask = _schemaStateChanged.Task;
            }

            await waitTask.WaitAsync(ct);
        }
    }

    public async ValueTask AcquireSchemaExclusiveAsync(long transactionId, CancellationToken ct = default)
    {
        if (transactionId == 0)
            return;

        Task waitTask;
        lock (_stateGate)
        {
            if (_schemaExclusiveOwnerTransactionId == transactionId)
                return;

            if (_schemaExclusiveOwnerTransactionId != 0 &&
                _schemaExclusiveOwnerTransactionId != transactionId)
            {
                throw new CSharpDbConflictException(
                    "A concurrent schema change is already active. The transaction must be retried.");
            }

            if (_schemaExclusiveWaiterTransactionId != 0 &&
                _schemaExclusiveWaiterTransactionId != transactionId)
            {
                throw new CSharpDbConflictException(
                    "A concurrent schema change is already waiting for exclusive access. The transaction must be retried.");
            }

            _schemaExclusiveWaiterTransactionId = transactionId;
            SignalSchemaStateChanged_NoLock();
            waitTask = _schemaStateChanged.Task;
        }

        try
        {
            while (true)
            {
                lock (_stateGate)
                {
                    if (_schemaExclusiveOwnerTransactionId == transactionId)
                        return;

                    if (_schemaExclusiveOwnerTransactionId == 0 &&
                        _schemaExclusiveWaiterTransactionId == transactionId &&
                        _activeExplicitTransactions.Count == 1 &&
                        _activeExplicitTransactions.ContainsKey(transactionId))
                    {
                        _schemaExclusiveOwnerTransactionId = transactionId;
                        _schemaExclusiveWaiterTransactionId = 0;
                        SignalSchemaStateChanged_NoLock();
                        return;
                    }

                    waitTask = _schemaStateChanged.Task;
                }

                await waitTask.WaitAsync(ct);
            }
        }
        catch
        {
            lock (_stateGate)
            {
                if (_schemaExclusiveWaiterTransactionId == transactionId)
                {
                    _schemaExclusiveWaiterTransactionId = 0;
                    SignalSchemaStateChanged_NoLock();
                }
            }

            throw;
        }
    }

    public async ValueTask<IDisposable> AcquireCheckpointBarrierAsync(CancellationToken ct = default)
    {
        await _beginBarrier.WaitAsync(ct);
        try
        {
            await _writerLock.WaitAsync(ct);
            return new CompositeReservation(_writerLock, _beginBarrier);
        }
        catch
        {
            _beginBarrier.Release();
            throw;
        }
    }

    public async ValueTask<IDisposable> AcquireCommitLockAsync(CancellationToken ct = default)
    {
        await _commitLock.WaitAsync(ct);
        return new SemaphoreReservation(_commitLock);
    }

    public async ValueTask<IDisposable> EnterPendingCommitWindowAsync(CancellationToken ct = default)
    {
        bool acquireBarrier;
        lock (_stateGate)
        {
            acquireBarrier = _pendingCommitWindowCount == 0;
            _pendingCommitWindowCount++;
        }

        if (acquireBarrier)
        {
            try
            {
                await _beginBarrier.WaitAsync(ct);
                lock (_stateGate)
                {
                    _pendingCommitBarrierHeld = true;
                }
            }
            catch
            {
                lock (_stateGate)
                {
                    _pendingCommitWindowCount--;
                }

                throw;
            }
        }

        return new PendingCommitWindowReservation(this);
    }

    public bool HasWriteConflict(IEnumerable<uint> pageIds, long startVersion, out uint conflictPageId)
    {
        ArgumentNullException.ThrowIfNull(pageIds);

        lock (_pageVersionGate)
        {
            foreach (uint pageId in pageIds)
            {
                if (!_pageLastWriteVersion.TryGetValue(pageId, out long lastWriteVersion))
                    continue;

                if (lastWriteVersion > startVersion)
                {
                    conflictPageId = pageId;
                    return true;
                }
            }
        }

        conflictPageId = PageConstants.NullPageId;
        return false;
    }

    public bool TryGetPageLastWriteVersion(uint pageId, out long lastWriteVersion)
    {
        lock (_pageVersionGate)
            return _pageLastWriteVersion.TryGetValue(pageId, out lastWriteVersion);
    }

    public async ValueTask WaitForCommitStateChangeAsync(CancellationToken ct = default)
    {
        Task waitTask;
        lock (_pageVersionGate)
        {
            waitTask = _commitStateChanged.Task;
        }

        await waitTask.WaitAsync(ct);
    }

    public bool HasLogicalConflict(
        IEnumerable<LogicalConflictKey> logicalReadKeys,
        long startVersion,
        out LogicalConflictKey conflictKey)
    {
        ArgumentNullException.ThrowIfNull(logicalReadKeys);

        lock (_pageVersionGate)
        {
            foreach (LogicalConflictKey logicalReadKey in logicalReadKeys)
            {
                if (!_logicalLastWriteVersion.TryGetValue(logicalReadKey, out long lastWriteVersion))
                    continue;

                if (lastWriteVersion > startVersion)
                {
                    conflictKey = logicalReadKey;
                    return true;
                }
            }
        }

        conflictKey = default;
        return false;
    }

    public bool HasLogicalRangeConflict(
        IEnumerable<LogicalConflictRange> logicalReadRanges,
        long startVersion,
        out LogicalConflictKey conflictKey)
    {
        ArgumentNullException.ThrowIfNull(logicalReadRanges);

        lock (_pageVersionGate)
        {
            foreach (LogicalConflictRange logicalReadRange in logicalReadRanges)
            {
                foreach ((LogicalConflictKey writtenKey, long lastWriteVersion) in _logicalLastWriteVersion)
                {
                    if (lastWriteVersion <= startVersion)
                        continue;

                    if (!logicalReadRange.Contains(writtenKey))
                        continue;

                    conflictKey = writtenKey;
                    return true;
                }
            }
        }

        conflictKey = default;
        return false;
    }

    public PendingCommitReservation ReservePendingCommit(
        IEnumerable<uint> pageIds,
        IEnumerable<LogicalConflictKey> logicalWriteKeys)
    {
        ArgumentNullException.ThrowIfNull(pageIds);
        ArgumentNullException.ThrowIfNull(logicalWriteKeys);

        (PendingPageVersion[] previousVersions, int previousVersionCount) = BuildPendingPageVersionBuffer(pageIds);
        (PendingLogicalVersion[] previousLogicalVersions, int previousLogicalVersionCount) = BuildPendingLogicalVersionBuffer(logicalWriteKeys);
        return ReservePendingCommitCore(previousVersions, previousVersionCount, previousLogicalVersions, previousLogicalVersionCount);
    }

    internal PendingCommitReservation ReservePendingCommit(
        uint[] pageIds,
        int pageCount,
        HashSet<LogicalConflictKey> logicalWriteKeys)
    {
        ArgumentNullException.ThrowIfNull(pageIds);
        ArgumentNullException.ThrowIfNull(logicalWriteKeys);
        ArgumentOutOfRangeException.ThrowIfNegative(pageCount);
        if (pageCount > pageIds.Length)
            throw new ArgumentOutOfRangeException(nameof(pageCount));

        (PendingPageVersion[] previousVersions, int previousVersionCount) = BuildPendingPageVersionBuffer(pageIds, pageCount);
        (PendingLogicalVersion[] previousLogicalVersions, int previousLogicalVersionCount) = BuildPendingLogicalVersionBuffer(logicalWriteKeys);
        return ReservePendingCommitCore(previousVersions, previousVersionCount, previousLogicalVersions, previousLogicalVersionCount);
    }

    private PendingCommitReservation ReservePendingCommitCore(
        PendingPageVersion[] previousVersions,
        int previousVersionCount,
        PendingLogicalVersion[] previousLogicalVersions,
        int previousLogicalVersionCount)
    {
        long commitVersion;
        lock (_pageVersionGate)
        {
            commitVersion = Interlocked.Increment(ref _nextReservedCommitVersion);
            for (int i = 0; i < previousVersionCount; i++)
            {
                uint pageId = previousVersions[i].PageId;
                previousVersions[i] = new PendingPageVersion(
                    pageId,
                    _pageLastWriteVersion.TryGetValue(pageId, out long previousVersion)
                    ? previousVersion
                    : null);
                _pageLastWriteVersion[pageId] = commitVersion;
            }

            for (int i = 0; i < previousLogicalVersionCount; i++)
            {
                LogicalConflictKey logicalWriteKey = previousLogicalVersions[i].LogicalWriteKey;
                previousLogicalVersions[i] = new PendingLogicalVersion(
                    logicalWriteKey,
                    _logicalLastWriteVersion.TryGetValue(logicalWriteKey, out long previousVersion)
                        ? previousVersion
                        : null);
                _logicalLastWriteVersion[logicalWriteKey] = commitVersion;
            }
        }

        return new PendingCommitReservation(
            commitVersion,
            previousVersions,
            previousVersionCount,
            previousLogicalVersions,
            previousLogicalVersionCount);
    }

    public void PublishPendingCommit(PendingCommitReservation reservation)
    {
        ArgumentNullException.ThrowIfNull(reservation);

        try
        {
            bool published = false;
            while (true)
            {
                long current = Volatile.Read(ref _commitVersion);
                if (current >= reservation.CommitVersion)
                    break;

                if (Interlocked.CompareExchange(ref _commitVersion, reservation.CommitVersion, current) == current)
                {
                    published = true;
                    break;
                }
            }

            if (published)
                SignalCommitStateChanged();

            PruneConflictVersionsUpTo(GetConflictRetentionFloor());
        }
        finally
        {
            reservation.ReleaseBuffers();
        }
    }

    public void RevertPendingCommit(PendingCommitReservation reservation)
    {
        ArgumentNullException.ThrowIfNull(reservation);

        try
        {
            lock (_pageVersionGate)
            {
                PendingPageVersion[] previousPageVersions = reservation.PreviousPageVersions;
                for (int i = 0; i < reservation.PreviousPageVersionCount; i++)
                {
                    PendingPageVersion previous = previousPageVersions[i];
                    uint pageId = previous.PageId;
                    if (!_pageLastWriteVersion.TryGetValue(pageId, out long currentVersion) ||
                        currentVersion != reservation.CommitVersion)
                    {
                        continue;
                    }

                    long? previousVersion = previous.PreviousVersion;
                    if (previousVersion.HasValue)
                        _pageLastWriteVersion[pageId] = previousVersion.Value;
                    else
                        _pageLastWriteVersion.Remove(pageId);
                }

                PendingLogicalVersion[] previousLogicalVersions = reservation.PreviousLogicalVersions;
                for (int i = 0; i < reservation.PreviousLogicalVersionCount; i++)
                {
                    PendingLogicalVersion previous = previousLogicalVersions[i];
                    LogicalConflictKey logicalWriteKey = previous.LogicalWriteKey;
                    if (!_logicalLastWriteVersion.TryGetValue(logicalWriteKey, out long currentVersion) ||
                        currentVersion != reservation.CommitVersion)
                    {
                        continue;
                    }

                    long? previousVersion = previous.PreviousVersion;
                    if (previousVersion.HasValue)
                        _logicalLastWriteVersion[logicalWriteKey] = previousVersion.Value;
                    else
                        _logicalLastWriteVersion.Remove(logicalWriteKey);
                }
            }
        }
        finally
        {
            reservation.ReleaseBuffers();
        }

        SignalCommitStateChanged();
        PruneConflictVersionsUpTo(GetConflictRetentionFloor());
    }

    internal (int PageVersionCount, int LogicalVersionCount) GetTrackedConflictVersionCounts()
    {
        lock (_pageVersionGate)
            return (_pageLastWriteVersion.Count, _logicalLastWriteVersion.Count);
    }

    public void Dispose()
    {
        _beginBarrier.Dispose();
        _writerLock.Dispose();
        _commitLock.Dispose();
    }

    private void ReleasePendingCommitWindow()
    {
        bool releaseBarrier = false;
        lock (_stateGate)
        {
            if (_pendingCommitWindowCount <= 0)
                return;

            _pendingCommitWindowCount--;
            if (_pendingCommitWindowCount == 0 && _pendingCommitBarrierHeld)
            {
                _pendingCommitBarrierHeld = false;
                releaseBarrier = true;
            }
        }

        if (releaseBarrier)
            _beginBarrier.Release();
    }

    private static (PendingPageVersion[] Buffer, int Count) BuildPendingPageVersionBuffer(IEnumerable<uint> pageIds)
    {
        if (pageIds.TryGetNonEnumeratedCount(out int count))
        {
            if (count == 0)
                return (Array.Empty<PendingPageVersion>(), 0);

            PendingPageVersion[] previousVersions = ArrayPool<PendingPageVersion>.Shared.Rent(count);
            int index = 0;
            foreach (uint pageId in pageIds)
                previousVersions[index++] = new PendingPageVersion(pageId, PreviousVersion: null);

            return (previousVersions, index);
        }

        var previousVersionsList = new List<PendingPageVersion>();
        foreach (uint pageId in pageIds)
            previousVersionsList.Add(new PendingPageVersion(pageId, PreviousVersion: null));

        if (previousVersionsList.Count == 0)
            return (Array.Empty<PendingPageVersion>(), 0);

        PendingPageVersion[] previousVersionsBuffer = ArrayPool<PendingPageVersion>.Shared.Rent(previousVersionsList.Count);
        previousVersionsList.CopyTo(previousVersionsBuffer, 0);
        return (previousVersionsBuffer, previousVersionsList.Count);
    }

    private static (PendingPageVersion[] Buffer, int Count) BuildPendingPageVersionBuffer(uint[] pageIds, int count)
    {
        if (count == 0)
            return (Array.Empty<PendingPageVersion>(), 0);

        PendingPageVersion[] previousVersions = ArrayPool<PendingPageVersion>.Shared.Rent(count);
        for (int i = 0; i < count; i++)
            previousVersions[i] = new PendingPageVersion(pageIds[i], PreviousVersion: null);

        return (previousVersions, count);
    }

    private static (PendingLogicalVersion[] Buffer, int Count) BuildPendingLogicalVersionBuffer(IEnumerable<LogicalConflictKey> logicalWriteKeys)
    {
        if (logicalWriteKeys.TryGetNonEnumeratedCount(out int count))
        {
            if (count == 0)
                return (Array.Empty<PendingLogicalVersion>(), 0);

            PendingLogicalVersion[] previousVersions = ArrayPool<PendingLogicalVersion>.Shared.Rent(count);
            int index = 0;
            foreach (LogicalConflictKey logicalWriteKey in logicalWriteKeys)
                previousVersions[index++] = new PendingLogicalVersion(logicalWriteKey, PreviousVersion: null);

            return (previousVersions, index);
        }

        var previousVersionsList = new List<PendingLogicalVersion>();
        foreach (LogicalConflictKey logicalWriteKey in logicalWriteKeys)
            previousVersionsList.Add(new PendingLogicalVersion(logicalWriteKey, PreviousVersion: null));

        if (previousVersionsList.Count == 0)
            return (Array.Empty<PendingLogicalVersion>(), 0);

        PendingLogicalVersion[] previousVersionsBuffer = ArrayPool<PendingLogicalVersion>.Shared.Rent(previousVersionsList.Count);
        previousVersionsList.CopyTo(previousVersionsBuffer, 0);
        return (previousVersionsBuffer, previousVersionsList.Count);
    }

    private static (PendingLogicalVersion[] Buffer, int Count) BuildPendingLogicalVersionBuffer(HashSet<LogicalConflictKey> logicalWriteKeys)
    {
        if (logicalWriteKeys.Count == 0)
            return (Array.Empty<PendingLogicalVersion>(), 0);

        PendingLogicalVersion[] previousVersions = ArrayPool<PendingLogicalVersion>.Shared.Rent(logicalWriteKeys.Count);
        int index = 0;
        foreach (LogicalConflictKey logicalWriteKey in logicalWriteKeys)
            previousVersions[index++] = new PendingLogicalVersion(logicalWriteKey, PreviousVersion: null);

        return (previousVersions, index);
    }

    private long GetConflictRetentionFloor()
    {
        lock (_stateGate)
            return GetConflictRetentionFloor_NoLock();
    }

    private long GetConflictRetentionFloor_NoLock()
    {
        if (_activeExplicitTransactions.Count == 0)
            return Volatile.Read(ref _commitVersion);

        long oldestStartVersion = long.MaxValue;
        foreach (long startVersion in _activeExplicitTransactions.Values)
            oldestStartVersion = Math.Min(oldestStartVersion, startVersion);

        return oldestStartVersion == long.MaxValue
            ? Volatile.Read(ref _commitVersion)
            : oldestStartVersion;
    }

    private void PruneConflictVersionsUpTo(long retentionFloor)
    {
        uint[]? stalePageIds = null;
        int stalePageCount = 0;
        LogicalConflictKey[]? staleLogicalKeys = null;
        int staleLogicalCount = 0;
        lock (_pageVersionGate)
        {
            foreach ((uint pageId, long version) in _pageLastWriteVersion)
            {
                if (version > retentionFloor)
                    continue;

                stalePageIds ??= ArrayPool<uint>.Shared.Rent(_pageLastWriteVersion.Count);
                stalePageIds[stalePageCount++] = pageId;
            }

            foreach ((LogicalConflictKey logicalWriteKey, long version) in _logicalLastWriteVersion)
            {
                if (version > retentionFloor)
                    continue;

                staleLogicalKeys ??= ArrayPool<LogicalConflictKey>.Shared.Rent(_logicalLastWriteVersion.Count);
                staleLogicalKeys[staleLogicalCount++] = logicalWriteKey;
            }

            if (stalePageIds is not null)
            {
                for (int i = 0; i < stalePageCount; i++)
                    _pageLastWriteVersion.Remove(stalePageIds[i]);
            }

            if (staleLogicalKeys is not null)
            {
                for (int i = 0; i < staleLogicalCount; i++)
                    _logicalLastWriteVersion.Remove(staleLogicalKeys[i]);
            }
        }

        if (stalePageIds is not null)
            ArrayPool<uint>.Shared.Return(stalePageIds, clearArray: false);

        if (staleLogicalKeys is not null)
            ArrayPool<LogicalConflictKey>.Shared.Return(staleLogicalKeys, clearArray: true);
    }

    private void SignalSchemaStateChanged_NoLock()
    {
        TaskCompletionSource<bool> completed = _schemaStateChanged;
        _schemaStateChanged = CreateStateChangedSource();
        completed.TrySetResult(true);
    }

    private void SignalCommitStateChanged()
    {
        TaskCompletionSource<bool> completed;
        lock (_pageVersionGate)
        {
            completed = _commitStateChanged;
            _commitStateChanged = CreateStateChangedSource();
        }

        completed.TrySetResult(true);
    }

    private static TaskCompletionSource<bool> CreateStateChangedSource()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class SemaphoreReservation : IDisposable
    {
        private SemaphoreSlim? _semaphore;

        public SemaphoreReservation(SemaphoreSlim semaphore)
        {
            _semaphore = semaphore;
        }

        public void Dispose()
        {
            SemaphoreSlim? semaphore = Interlocked.Exchange(ref _semaphore, null);
            semaphore?.Release();
        }
    }

    private sealed class CompositeReservation : IDisposable
    {
        private SemaphoreSlim? _writerLock;
        private SemaphoreSlim? _beginBarrier;

        public CompositeReservation(SemaphoreSlim writerLock, SemaphoreSlim beginBarrier)
        {
            _writerLock = writerLock;
            _beginBarrier = beginBarrier;
        }

        public void Dispose()
        {
            SemaphoreSlim? writerLock = Interlocked.Exchange(ref _writerLock, null);
            SemaphoreSlim? beginBarrier = Interlocked.Exchange(ref _beginBarrier, null);

            try
            {
                writerLock?.Release();
            }
            finally
            {
                beginBarrier?.Release();
            }
        }
    }

    private sealed class PendingCommitWindowReservation : IDisposable
    {
        private TransactionCoordinator? _coordinator;

        public PendingCommitWindowReservation(TransactionCoordinator coordinator)
        {
            _coordinator = coordinator;
        }

        public void Dispose()
        {
            TransactionCoordinator? coordinator = Interlocked.Exchange(ref _coordinator, null);
            coordinator?.ReleasePendingCommitWindow();
        }
    }

    public sealed class PendingCommitReservation
    {
        private PendingPageVersion[]? _previousPageVersions;
        private PendingLogicalVersion[]? _previousLogicalVersions;

        internal PendingCommitReservation(
            long commitVersion,
            PendingPageVersion[] previousPageVersions,
            int previousPageVersionCount,
            PendingLogicalVersion[] previousLogicalVersions,
            int previousLogicalVersionCount)
        {
            CommitVersion = commitVersion;
            _previousPageVersions = previousPageVersions;
            _previousLogicalVersions = previousLogicalVersions;
            PreviousPageVersionCount = previousPageVersionCount;
            PreviousLogicalVersionCount = previousLogicalVersionCount;
        }

        public long CommitVersion { get; }

        internal int PreviousPageVersionCount { get; }

        internal int PreviousLogicalVersionCount { get; }

        internal PendingPageVersion[] PreviousPageVersions => _previousPageVersions ?? Array.Empty<PendingPageVersion>();

        internal PendingLogicalVersion[] PreviousLogicalVersions => _previousLogicalVersions ?? Array.Empty<PendingLogicalVersion>();

        internal void ReleaseBuffers()
        {
            PendingPageVersion[]? previousPageVersions = Interlocked.Exchange(ref _previousPageVersions, null);
            if (previousPageVersions is not null && previousPageVersions.Length > 0)
                ArrayPool<PendingPageVersion>.Shared.Return(previousPageVersions, clearArray: true);

            PendingLogicalVersion[]? previousLogicalVersions = Interlocked.Exchange(ref _previousLogicalVersions, null);
            if (previousLogicalVersions is not null && previousLogicalVersions.Length > 0)
                ArrayPool<PendingLogicalVersion>.Shared.Return(previousLogicalVersions, clearArray: true);
        }
    }

    internal readonly record struct PendingPageVersion(uint PageId, long? PreviousVersion);

    internal readonly record struct PendingLogicalVersion(LogicalConflictKey LogicalWriteKey, long? PreviousVersion);
}
