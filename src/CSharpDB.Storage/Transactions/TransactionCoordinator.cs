using CSharpDB.Primitives;

namespace CSharpDB.Storage.Transactions;

/// <summary>
/// Coordinates single-writer transaction state and write-lock lifecycle.
/// </summary>
internal sealed class TransactionCoordinator : IDisposable
{
    private readonly SemaphoreSlim _writerLock = new(1, 1);
    private bool _inTransaction;

    public bool InTransaction => _inTransaction;

    public async ValueTask BeginAsync(IWriteAheadLog wal, TimeSpan writerLockTimeout, CancellationToken ct = default)
    {
        if (_inTransaction)
            throw new CSharpDbException(ErrorCode.Unknown, "Nested transactions are not supported.");

        if (!await _writerLock.WaitAsync(writerLockTimeout, ct))
            throw new CSharpDbException(ErrorCode.Busy, "Could not acquire write lock (database is busy).");

        wal.BeginTransaction();
        _inTransaction = true;
    }

    public void CompleteCommit()
    {
        _inTransaction = false;
        _writerLock.Release();
    }

    public bool TryBeginRollback()
    {
        return _inTransaction;
    }

    public void CompleteRollback()
    {
        _inTransaction = false;
        try { _writerLock.Release(); } catch (SemaphoreFullException) { }
    }

    public void Dispose() => _writerLock.Dispose();
}
