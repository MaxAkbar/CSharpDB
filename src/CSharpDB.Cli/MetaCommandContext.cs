using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Core;
using CSharpDB.Engine;
using CSharpDB.Execution;

namespace CSharpDB.Cli;

internal sealed class MetaCommandContext : IDisposable
{
    private readonly Func<string, CancellationToken, ValueTask<bool>> _executeSql;
    private readonly IEngineBackedClient? _engineBackedClient;
    private Database? _localDatabase;
    private Database.ReaderSession? _snapshot;
    private string? _transactionId;

    public MetaCommandContext(
        ICSharpDbClient client,
        Database? localDatabase,
        string databasePath,
        Func<string, CancellationToken, ValueTask<bool>> executeSql)
    {
        Client = client;
        _engineBackedClient = client as IEngineBackedClient;
        _localDatabase = localDatabase;
        DatabasePath = databasePath;
        _executeSql = executeSql;
    }

    public ICSharpDbClient Client { get; }
    public Database? LocalDatabase => _localDatabase;
    public string DatabasePath { get; }
    public bool ShowTiming { get; set; } = true;
    public bool InExplicitTransaction => _transactionId is not null;
    public bool SnapshotEnabled => _snapshot is not null;
    public bool SupportsLocalDirectFeatures => _localDatabase is not null;

    public bool PreferSyncPointLookups
    {
        get => _localDatabase?.PreferSyncPointLookups ?? false;
        set
        {
            if (_localDatabase is null)
                throw new InvalidOperationException("Sync point mode requires direct local access.");

            _localDatabase.PreferSyncPointLookups = value;
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

    public async ValueTask<ReindexResult> ReindexAsync(ReindexRequest request, CancellationToken ct = default)
    {
        DisableSnapshot();
        var result = await Client.ReindexAsync(request, ct);
        await RefreshLocalDatabaseAsync(ct);
        return result;
    }

    public async ValueTask<VacuumResult> VacuumAsync(CancellationToken ct = default)
    {
        DisableSnapshot();
        var result = await Client.VacuumAsync(ct);
        await RefreshLocalDatabaseAsync(ct);
        return result;
    }

    public async ValueTask RefreshLocalDatabaseAsync(CancellationToken ct = default)
    {
        if (_engineBackedClient is null)
            return;

        _localDatabase = await _engineBackedClient.TryGetDatabaseAsync(ct);
    }

    public void EnableSnapshot()
    {
        if (_localDatabase is null)
            throw new InvalidOperationException("Snapshot mode requires direct local access.");

        if (_snapshot is not null)
            throw new CSharpDbException(ErrorCode.Unknown, "Snapshot mode is already enabled.");

        _snapshot = _localDatabase.CreateReaderSession();
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
