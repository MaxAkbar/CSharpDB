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
    private long _currentTransactionId;
    private long _nextTransactionId;
    private long _commitVersion;
    private long _nextReservedPageId;
    private int _inTransactionFlag;
    private bool _inTransaction;
    private bool _writerLockReleased;

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

    public long PublishCommit(IEnumerable<uint> pageIds)
    {
        ArgumentNullException.ThrowIfNull(pageIds);

        long commitVersion = Interlocked.Increment(ref _commitVersion);
        lock (_pageVersionGate)
        {
            foreach (uint pageId in pageIds)
                _pageLastWriteVersion[pageId] = commitVersion;
        }

        return commitVersion;
    }

    public void Dispose()
    {
        _beginBarrier.Dispose();
        _writerLock.Dispose();
        _commitLock.Dispose();
    }

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
}
