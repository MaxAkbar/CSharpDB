using CSharpDB.Primitives;

namespace CSharpDB.Storage.Transactions;

/// <summary>
/// Coordinates single-writer transaction state and write-lock lifecycle.
/// </summary>
internal sealed class TransactionCoordinator : IDisposable
{
    private readonly object _stateGate = new();
    private readonly SemaphoreSlim _writerLock = new(1, 1);
    private long _currentTransactionId;
    private long _nextTransactionId;
    private int _inTransactionFlag;
    private bool _inTransaction;
    private bool _writerLockReleased;

    public bool InTransaction => Volatile.Read(ref _inTransactionFlag) != 0;

    public async ValueTask BeginAsync(IWriteAheadLog wal, TimeSpan writerLockTimeout, CancellationToken ct = default)
    {
        lock (_stateGate)
        {
            if (_inTransaction)
                throw new CSharpDbException(ErrorCode.Unknown, "Nested transactions are not supported.");
        }

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

    public async ValueTask<IDisposable> AcquireCheckpointBarrierAsync(CancellationToken ct = default)
    {
        await _writerLock.WaitAsync(ct);
        return new WriterLockReservation(_writerLock);
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

        try { _writerLock.Release(); } catch (SemaphoreFullException) { }
    }

    public void Dispose() => _writerLock.Dispose();

    private sealed class WriterLockReservation : IDisposable
    {
        private SemaphoreSlim? _writerLock;

        public WriterLockReservation(SemaphoreSlim writerLock)
        {
            _writerLock = writerLock;
        }

        public void Dispose()
        {
            SemaphoreSlim? writerLock = Interlocked.Exchange(ref _writerLock, null);
            writerLock?.Release();
        }
    }
}
