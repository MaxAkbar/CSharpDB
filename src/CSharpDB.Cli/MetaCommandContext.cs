using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Core;
using CSharpDB.Engine;
using CSharpDB.Execution;

namespace CSharpDB.Cli;

internal sealed class MetaCommandContext : IDisposable
{
    private readonly Func<string, CancellationToken, ValueTask<bool>> _executeSql;
    private Database.ReaderSession? _snapshot;
    private string? _transactionId;

    public MetaCommandContext(
        ICSharpDbClient client,
        Database? localDatabase,
        string databasePath,
        Func<string, CancellationToken, ValueTask<bool>> executeSql)
    {
        Client = client;
        LocalDatabase = localDatabase;
        DatabasePath = databasePath;
        _executeSql = executeSql;
    }

    public ICSharpDbClient Client { get; }
    public Database? LocalDatabase { get; }
    public string DatabasePath { get; }
    public bool ShowTiming { get; set; } = true;
    public bool InExplicitTransaction => _transactionId is not null;
    public bool SnapshotEnabled => _snapshot is not null;
    public bool SupportsLocalDirectFeatures => LocalDatabase is not null;

    public bool PreferSyncPointLookups
    {
        get => LocalDatabase?.PreferSyncPointLookups ?? false;
        set
        {
            if (LocalDatabase is null)
                throw new InvalidOperationException("Sync point mode requires direct local access.");

            LocalDatabase.PreferSyncPointLookups = value;
        }
    }

    public async ValueTask<bool> ExecuteSqlAsync(string sql, CancellationToken ct = default)
        => await _executeSql(sql, ct);

    public async ValueTask<SqlExecutionResult> ExecuteDbSqlAsync(string sql, CancellationToken ct = default)
        => _transactionId is null
            ? await Client.ExecuteSqlAsync(sql, ct)
            : await Client.ExecuteInTransactionAsync(_transactionId, sql, ct);

    public async ValueTask<QueryResult> ExecuteReadSnapshotAsync(string sql, CancellationToken ct = default)
    {
        if (_snapshot is null)
            throw new CSharpDbException(ErrorCode.Unknown, "Snapshot mode is not enabled.");
        return await _snapshot.ExecuteReadAsync(sql, ct);
    }

    public async ValueTask BeginTransactionAsync(CancellationToken ct = default)
    {
        if (_transactionId is not null)
            throw new InvalidOperationException("An explicit transaction is already active.");

        var tx = await Client.BeginTransactionAsync(ct);
        _transactionId = tx.TransactionId;
    }

    public async ValueTask CommitAsync(CancellationToken ct = default)
    {
        if (_transactionId is null)
            throw new InvalidOperationException("No explicit transaction is active.");

        try
        {
            await Client.CommitTransactionAsync(_transactionId, ct);
        }
        finally
        {
            _transactionId = null;
        }
    }

    public async ValueTask RollbackAsync(CancellationToken ct = default)
    {
        if (_transactionId is null)
            throw new InvalidOperationException("No explicit transaction is active.");

        try
        {
            await Client.RollbackTransactionAsync(_transactionId, ct);
        }
        finally
        {
            _transactionId = null;
        }
    }

    public async ValueTask CheckpointAsync(CancellationToken ct = default)
    {
        await Client.CheckpointAsync(ct);
    }

    public void EnableSnapshot()
    {
        if (LocalDatabase is null)
            throw new InvalidOperationException("Snapshot mode requires direct local access.");

        if (_snapshot is not null)
            throw new CSharpDbException(ErrorCode.Unknown, "Snapshot mode is already enabled.");

        _snapshot = LocalDatabase.CreateReaderSession();
    }

    public void DisableSnapshot()
    {
        _snapshot?.Dispose();
        _snapshot = null;
    }

    public void Dispose()
    {
        DisableSnapshot();
    }
}
