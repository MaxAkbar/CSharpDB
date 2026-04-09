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
    private readonly HashSet<long> _activeExplicitTransactions = [];
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
    private TaskCompletionSource<bool> _schemaStateChanged = CreateSchemaStateChangedSource();

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

    public void RegisterExplicitTransaction(long transactionId)
    {
        if (transactionId == 0)
            return;

        lock (_stateGate)
        {
            _activeExplicitTransactions.Add(transactionId);
            SignalSchemaStateChanged_NoLock();
        }
    }

    public void UnregisterExplicitTransaction(long transactionId)
    {
        if (transactionId == 0)
            return;

        lock (_stateGate)
        {
            _activeExplicitTransactions.Remove(transactionId);
            if (_schemaExclusiveOwnerTransactionId == transactionId)
                _schemaExclusiveOwnerTransactionId = 0;
            if (_schemaExclusiveWaiterTransactionId == transactionId)
                _schemaExclusiveWaiterTransactionId = 0;
            SignalSchemaStateChanged_NoLock();
        }
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
                        _activeExplicitTransactions.Contains(transactionId))
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

        Dictionary<uint, long?> previousVersions = new();
        Dictionary<LogicalConflictKey, long?> previousLogicalVersions = new();
        long commitVersion;
        lock (_pageVersionGate)
        {
            commitVersion = Interlocked.Increment(ref _nextReservedCommitVersion);
            foreach (uint pageId in pageIds)
            {
                previousVersions[pageId] = _pageLastWriteVersion.TryGetValue(pageId, out long previousVersion)
                    ? previousVersion
                    : null;
                _pageLastWriteVersion[pageId] = commitVersion;
            }

            foreach (LogicalConflictKey logicalWriteKey in logicalWriteKeys)
            {
                previousLogicalVersions[logicalWriteKey] =
                    _logicalLastWriteVersion.TryGetValue(logicalWriteKey, out long previousVersion)
                        ? previousVersion
                        : null;
                _logicalLastWriteVersion[logicalWriteKey] = commitVersion;
            }
        }

        return new PendingCommitReservation(commitVersion, previousVersions, previousLogicalVersions);
    }

    public void PublishPendingCommit(PendingCommitReservation reservation)
    {
        ArgumentNullException.ThrowIfNull(reservation);

        while (true)
        {
            long current = Volatile.Read(ref _commitVersion);
            if (current >= reservation.CommitVersion)
                return;

            if (Interlocked.CompareExchange(ref _commitVersion, reservation.CommitVersion, current) == current)
                return;
        }
    }

    public void RevertPendingCommit(PendingCommitReservation reservation)
    {
        ArgumentNullException.ThrowIfNull(reservation);

        lock (_pageVersionGate)
        {
            foreach ((uint pageId, long? previousVersion) in reservation.PreviousPageVersions)
            {
                if (!_pageLastWriteVersion.TryGetValue(pageId, out long currentVersion) ||
                    currentVersion != reservation.CommitVersion)
                {
                    continue;
                }

                if (previousVersion.HasValue)
                    _pageLastWriteVersion[pageId] = previousVersion.Value;
                else
                    _pageLastWriteVersion.Remove(pageId);
            }

            foreach ((LogicalConflictKey logicalWriteKey, long? previousVersion) in reservation.PreviousLogicalVersions)
            {
                if (!_logicalLastWriteVersion.TryGetValue(logicalWriteKey, out long currentVersion) ||
                    currentVersion != reservation.CommitVersion)
                {
                    continue;
                }

                if (previousVersion.HasValue)
                    _logicalLastWriteVersion[logicalWriteKey] = previousVersion.Value;
                else
                    _logicalLastWriteVersion.Remove(logicalWriteKey);
            }
        }
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

    private void SignalSchemaStateChanged_NoLock()
    {
        TaskCompletionSource<bool> completed = _schemaStateChanged;
        _schemaStateChanged = CreateSchemaStateChangedSource();
        completed.TrySetResult(true);
    }

    private static TaskCompletionSource<bool> CreateSchemaStateChangedSource()
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
        internal PendingCommitReservation(
            long commitVersion,
            IReadOnlyDictionary<uint, long?> previousPageVersions,
            IReadOnlyDictionary<LogicalConflictKey, long?> previousLogicalVersions)
        {
            CommitVersion = commitVersion;
            PreviousPageVersions = previousPageVersions;
            PreviousLogicalVersions = previousLogicalVersions;
        }

        public long CommitVersion { get; }

        internal IReadOnlyDictionary<uint, long?> PreviousPageVersions { get; }

        internal IReadOnlyDictionary<LogicalConflictKey, long?> PreviousLogicalVersions { get; }
    }
}
