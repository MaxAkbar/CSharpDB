using CSharpDB.Core;
using CSharpDB.Engine;
using CSharpDB.Execution;

namespace CSharpDB.Cli;

internal sealed class MetaCommandContext : IDisposable
{
    private readonly Func<string, CancellationToken, ValueTask<bool>> _executeSql;
    private Database.ReaderSession? _snapshot;

    public MetaCommandContext(
        Database database,
        string databasePath,
        Func<string, CancellationToken, ValueTask<bool>> executeSql)
    {
        Database = database;
        DatabasePath = databasePath;
        _executeSql = executeSql;
    }

    public Database Database { get; }
    public string DatabasePath { get; }
    public bool ShowTiming { get; set; } = true;
    public bool InExplicitTransaction { get; private set; }
    public bool SnapshotEnabled => _snapshot is not null;

    public bool PreferSyncPointLookups
    {
        get => Database.PreferSyncPointLookups;
        set => Database.PreferSyncPointLookups = value;
    }

    public async ValueTask<bool> ExecuteSqlAsync(string sql, CancellationToken ct = default)
        => await _executeSql(sql, ct);

    public async ValueTask<QueryResult> ExecuteReadSnapshotAsync(string sql, CancellationToken ct = default)
    {
        if (_snapshot is null)
            throw new CSharpDbException(ErrorCode.Unknown, "Snapshot mode is not enabled.");
        return await _snapshot.ExecuteReadAsync(sql, ct);
    }

    public async ValueTask BeginTransactionAsync(CancellationToken ct = default)
    {
        await Database.BeginTransactionAsync(ct);
        InExplicitTransaction = true;
    }

    public async ValueTask CommitAsync(CancellationToken ct = default)
    {
        await Database.CommitAsync(ct);
        InExplicitTransaction = false;
    }

    public async ValueTask RollbackAsync(CancellationToken ct = default)
    {
        await Database.RollbackAsync(ct);
        InExplicitTransaction = false;
    }

    public async ValueTask CheckpointAsync(CancellationToken ct = default)
    {
        await Database.CheckpointAsync(ct);
    }

    public void EnableSnapshot()
    {
        if (_snapshot is not null)
            throw new CSharpDbException(ErrorCode.Unknown, "Snapshot mode is already enabled.");

        _snapshot = Database.CreateReaderSession();
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
