using System.Data;
using System.Data.Common;

namespace CSharpDB.Data;

public sealed class CSharpDbTransaction : DbTransaction
{
    private readonly CSharpDbConnection _connection;
    private bool _completed;

    public override IsolationLevel IsolationLevel { get; }
    protected override DbConnection DbConnection => _connection;

    internal CSharpDbTransaction(CSharpDbConnection connection, IsolationLevel isolationLevel)
    {
        _connection = connection;
        IsolationLevel = isolationLevel;
    }

    public override void Commit()
        => CommitAsync(CancellationToken.None).GetAwaiter().GetResult();

    public override async Task CommitAsync(CancellationToken cancellationToken)
    {
        if (_completed) throw new InvalidOperationException("Transaction already completed.");
        try
        {
            await _connection.GetSession().CommitAsync(cancellationToken);
        }
        catch (CSharpDB.Core.CSharpDbException ex)
        {
            throw new CSharpDbDataException(ex);
        }

        _completed = true;
        _connection.ClearTransaction();
    }

    public override void Rollback()
        => RollbackAsync(CancellationToken.None).GetAwaiter().GetResult();

    public override async Task RollbackAsync(CancellationToken cancellationToken)
    {
        if (_completed) throw new InvalidOperationException("Transaction already completed.");
        try
        {
            await _connection.GetSession().RollbackAsync(cancellationToken);
        }
        catch (CSharpDB.Core.CSharpDbException ex)
        {
            throw new CSharpDbDataException(ex);
        }

        _completed = true;
        _connection.ClearTransaction();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_completed)
        {
            try { Rollback(); } catch { /* best-effort rollback on dispose */ }
        }
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        if (!_completed)
        {
            try { await RollbackAsync(CancellationToken.None); } catch { }
        }
    }
}
